from enum import Enum
from pika.channel import Channel
from pika.exchange_type import ExchangeType


class Exchange(str, Enum):
    sensor_values = "sensor_values"
    aggr_values = "aggr_values"
    anomalies = "anomalies"


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
