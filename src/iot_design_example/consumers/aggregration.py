
from enum import Enum
import json
import sys
from typing import Any

from pika import DeliveryMode
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from iot_design_example.infra import (
    Exchange,
)

from iot_design_example.producers import (
    RoutingKey,
    SensorMessage
)

class AggregationType(int, Enum):
    max_temperature = 1

class AggregationMessage:
    aggr_type : AggregationType
    value: Any

    def __init__(self, aggr_type: AggregationType, value):
        self.aggr_type = aggr_type
        self.value = value

    def __str__(self):
        return json.dumps(
            {
                "aggr_type": self.aggr_type,
                "value": self.value
            }
        )

    @staticmethod
    def from_dict(data: dict) -> "AggregationMessage":
        return AggregationMessage(
            aggr_type=AggregationType(data["aggr_type"]),
            value=data["value"]
        )


class SensorAggregationMaxTemperaturConsumer:
    """
    Consumer that aggregates the maximum temperature value 
    from sensor messages and publishes it to the aggr_values 
    exchange every 10 seconds.

    A fixed time-window of 10 seconds is used to aggregate the maximum temperature value.
    After each time-window, the maximum value is published to the aggr_values exchange.
    """
    TIME_INTERVALL_SECONDS = 10

    _channel: Channel

    current_max_value: float
    produced_by_sensor_id: str

    def __init__(self, channel: Channel):
        self._channel = channel
        self.current_max_value = sys.float_info.min
        self.produced_by_sensor_id = None

    def start_consuming(self):
        result = self._channel.queue_declare(queue="", exclusive=True)
        queue_name = result.method.queue
        self._channel.queue_bind(
            exchange=Exchange.sensor_values,
            queue=queue_name,
            routing_key=RoutingKey.sensor_temperature,
        ) 
        self._channel.basic_consume(
            queue=queue_name, auto_ack=False, on_message_callback=self._callback
        )
        self._channel.connection.call_later(
            self.TIME_INTERVALL_SECONDS, self._publish_max_value_and_re_schedule
        )
        self._channel.start_consuming()

    def _callback(self, channel: Channel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
        try:
            sensor_message = SensorMessage.from_dict(json.loads(body.decode()))
            if sensor_message.VALUE > self.current_max_value:
                self.current_max_value = sensor_message.VALUE
                self.produced_by_sensor_id = sensor_message.SENSOR_ID
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"[SensorAggregationMaxTemperaturConsumer] Error processing message: {e}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            

    def _publish_max_value_and_re_schedule(self):
        self._channel.basic_publish(
            exchange=Exchange.aggr_values,
            routing_key="max",
            body=str(AggregationMessage(AggregationType.max_temperature, (self.produced_by_sensor_id, self.current_max_value))),
            properties=BasicProperties(delivery_mode=DeliveryMode.Persistent),
        )

        self.produced_by_sensor_id = None
        self.current_max_value = sys.float_info.min

        self._channel.connection.call_later(
            self.TIME_INTERVALL_SECONDS, self._publish_max_value_and_re_schedule
        )