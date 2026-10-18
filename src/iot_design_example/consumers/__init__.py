import json

from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from iot_design_example.infra import Exchange
from iot_design_example.producers import RoutingKey, SensorMessage


class MonitorConsumer:
    def __init__(self, channel: Channel):
        self.CHANNEL = channel

    def start_consuming(self):
        result = self.CHANNEL.queue_declare(queue="", exclusive=True)
        queue_name = result.method.queue
        # Bind all exchanges, ergo all messages
        for exchange in Exchange:
            self.CHANNEL.queue_bind(
                exchange=exchange,
                queue=queue_name,
                routing_key=RoutingKey.wildcard,
            )
        self.CHANNEL.basic_consume(
            queue=queue_name, auto_ack=True, on_message_callback=self.callback
        )
        self.CHANNEL.start_consuming()

    def callback(
        self,
        ch: Channel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        # TODO: validate message
        message = json.loads(body.decode())
        print(f"Received sensor message from {message["sensor_id"]}")
        print(f"\ttimestamp: {message["timestamp"]}")
        print(f"\tsensor_type: {message["type"]}")
        print(f"\tvalue: {message["value"]}")
