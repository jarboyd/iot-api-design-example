from pika.channel import Channel
from pika import BasicProperties, DeliveryMode
from iot_design_example.infra import Exchange
from iot_design_example.producers import RoutingKey
import json

from iot_design_example.producers import (
    SensorMessage,
    SensorType,
    AnomalyLevel,
    AnomalyMessage
)

class AnomalyDetector:
    TEMPERATUR_WARNING = 80.0
    TEMPERATURE_CRITICAL = 100.0

    PRESSURE_WARNING = 1020.0
    PRESSURE_CRITICAL = 1040.0

    @staticmethod
    def detect(message: SensorMessage) -> AnomalyLevel | None:
        if message.TYPE == SensorType.temperature:
            warning = AnomalyDetector.TEMPERATUR_WARNING
            critical = AnomalyDetector.TEMPERATURE_CRITICAL

        elif message.TYPE == SensorType.pressure:
            warning = AnomalyDetector.PRESSURE_WARNING
            critical = AnomalyDetector.PRESSURE_CRITICAL

        else: return None

        if message.VALUE > critical:
            return AnomalyLevel.critical

        if message.VALUE > warning:
            return AnomalyLevel.warning

        return None

class AnomalyConsumer:
    def __init__(self, channel: Channel, routing_key: RoutingKey):
        self.channel = channel
        self.routing_key = routing_key

    def callback(self, channel, method, properties, body):
        try:
            message = SensorMessage.from_dict(json.loads(body))
            level = AnomalyDetector.detect(message)

            if level is not None:
                print(f"Anomaly detected: {message.SENSOR_ID}, Level: {level}")

                alarm = AnomalyMessage(
                    sensor_id=message.SENSOR_ID,
                    timestamp=message.TIMESTAMP,
                    sensor_type=message.TYPE,
                    value=message.VALUE,
                    level=level
                )

                if level == AnomalyLevel.critical:
                    priority = 2
                else:
                    priority = 1


                channel.basic_publish(exchange=Exchange.anomalies,
                                      routing_key="",
                                      body=str(alarm),
                                      properties=BasicProperties(
                                          delivery_mode=DeliveryMode.Persistent,
                                          priority=priority
                                      ),
                                      mandatory=True)

            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f"Message error: {e}")

            channel.basic_nack(delivery_tag=method.delivery_tag,
                               requeue=False)


    def start_consuming(self):
        if self.routing_key == RoutingKey.sensor_temperature:
            queue_name = "anomaly_temperature"

        elif self.routing_key == RoutingKey.sensor_pressure:
            queue_name = "anomaly_pressure"

        self.channel.queue_declare(queue=queue_name,
                                   durable=True,
                                   arguments={"x-message-ttl": 60000}
                                   )

        self.channel.queue_bind(exchange=Exchange.sensor_values,
                                queue=queue_name,
                                routing_key=self.routing_key)

        self.channel.confirm_delivery()

        self.channel.basic_consume(queue=queue_name,
                                   on_message_callback=self.callback,
                                   auto_ack=False)

        print(f"Waitig for message on {queue_name}")

        self.channel.start_consuming()

        