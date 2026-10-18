from multiprocessing import Process

from iot_design_example.consumers import MonitorConsumer
from iot_design_example.infra import Connection, declare_exchanges
from iot_design_example.producers import (
    MockHumidityProducer,
    MockPressureProducer,
    MockTemperatureProducer,
)


def run_mock_temperature_sensor():
    with Connection() as connection:
        channel = connection.channel()
        declare_exchanges(channel)

        producer = MockTemperatureProducer(channel)
        producer.start_producing()


def run_mock_pressure_sensor():
    with Connection() as connection:
        channel = connection.channel()
        declare_exchanges(channel)

        producer = MockPressureProducer(channel)
        producer.start_producing()


def run_mock_humidity_sensor():
    with Connection() as connection:
        channel = connection.channel()
        declare_exchanges(channel)

        producer = MockHumidityProducer(channel)
        producer.start_producing()


def run_monitor_consumer():
    with Connection() as connection:
        channel = connection.channel()
        declare_exchanges(channel)

        consumer = MonitorConsumer(channel)
        consumer.start_consuming()


def main() -> None:
    consumers = [Process(target=run_monitor_consumer)]
    producers = [
        Process(target=run_mock_temperature_sensor),
        Process(target=run_mock_pressure_sensor),
        Process(target=run_mock_humidity_sensor),
    ]

    print("Starting consumers")
    for p in consumers:
        p.start()
    print("Starting producers")
    for p in producers:
        p.start()
    print("Consumers and producers running")
    for p in producers + consumers:
        p.join()
    print("All processes have finished.")
