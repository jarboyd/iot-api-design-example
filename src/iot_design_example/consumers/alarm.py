from pika.channel import Channel
from iot_design_example.producers import AnomalyMessage
from iot_design_example.infra import Connection, declare_exchanges, declare_alarm_queue

import json
import sys


class AlarmConsumer:

    def __init__(self, channel: Channel):
        self.channel = channel

    def callback(self, channel, method, properties, body):
        try:
            alarm = AnomalyMessage.from_dict(json.loads(body))

            print(f"Alarm received: {alarm.SENSOR_ID}, Level: {alarm.LEVEL}")

            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as error:
            print(f"[AlarmConsumer] Error: {error}",
                  file=sys.stderr)

            channel.basic_nack(delivery_tag=method.delivery_tag,
                               requeue=False)


    def start_consuming(self):
        self.channel.basic_consume(queue="alarm_queue",
                                   on_message_callback=self.callback,
                                   auto_ack=False)

        print("Waiting for alarms on alarm_queue")

        self.channel.start_consuming()


if __name__ == "__main__":

    with Connection() as connection:
        channel = connection.channel()
        declare_exchanges(channel)
        declare_alarm_queue(channel)

        consumer = AlarmConsumer(channel)
        consumer.start_consuming()
