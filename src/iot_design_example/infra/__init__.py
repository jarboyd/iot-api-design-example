from enum import Enum
from pika import BlockingConnection, ConnectionParameters
from pika.channel import Channel
from pika.exchange_type import ExchangeType


class Exchange(str, Enum):
    sensor_values = "sensor_values"
    aggr_values = "aggr_values"
    anomalies = "anomalies"
    dead_letters = "dead_letters"


DEAD_LETTER_QUEUE = "dead_letters"


def declare_exchanges(channel: Channel):
    # Declare exchanges
    channel.exchange_declare(
        exchange=Exchange.sensor_values, exchange_type=ExchangeType.topic
    )
    channel.exchange_declare(
        exchange=Exchange.aggr_values, exchange_type=ExchangeType.topic
    )
    channel.exchange_declare(
        exchange=Exchange.anomalies, exchange_type=ExchangeType.fanout
    )
    channel.exchange_declare(
        exchange=Exchange.dead_letters,
        exchange_type=ExchangeType.fanout,
        durable=True,
    )
    channel.queue_declare(queue=DEAD_LETTER_QUEUE, durable=True)
    channel.queue_bind(
        exchange=Exchange.dead_letters,
        queue=DEAD_LETTER_QUEUE,
    )

def declare_alarm_queue(channel: Channel):
    channel.queue_declare(queue="alarm_queue",
                          durable=True,
                          arguments={
                              "x-max-priority": 2,
                              "x-message-ttl": 60000,
                              "x-dead-letter-exchange": Exchange.dead_letters
                          })
    channel.queue_bind(exchange=Exchange.anomalies,
                       queue="alarm_queue")


class Connection:
    def __enter__(self) -> BlockingConnection:
        self.CONNECTION = BlockingConnection(ConnectionParameters("localhost"))
        return self.CONNECTION

    def __exit__(self, exc_type, exc, tb):
        self.CONNECTION.close()
