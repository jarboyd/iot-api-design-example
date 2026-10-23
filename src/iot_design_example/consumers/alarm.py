from pika.channel import Channel
from iot_design_example.producers import AnomalyMessage

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
