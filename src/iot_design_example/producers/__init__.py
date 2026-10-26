import threading
import time
from enum import Enum
import json
from pika.channel import Channel
from pika import BasicProperties, DeliveryMode
import random

from retry import retry

from iot_design_example.infra import Exchange


class SensorType(int, Enum):
    temperature = 0
    pressure = 1
    humidity = 2

class AnomalyLevel(int, Enum):
    warning = 0
    critical = 1

class RoutingKey(str, Enum):
    wildcard = "#"
    sensor_temperature = "sensor.temperature"
    sensor_pressure = "sensor.pressure"
    sensor_humidity = "sensor.humidity"


class SensorMessage:
    def __init__(self, sensor_id: str, timestamp: int, sensor_type: SensorType, value: float):
        self.SENSOR_ID = sensor_id
        self.TIMESTAMP = timestamp
        self.TYPE = sensor_type
        self.VALUE = value

    def __str__(self) -> str:
        return json.dumps(
            {
                "sensor_id": self.SENSOR_ID,
                "timestamp": self.TIMESTAMP,
                "type": self.TYPE,
                "value": self.VALUE,
            }
        )

    @staticmethod
    def from_dict(data: dict) -> "SensorMessage":
        return SensorMessage(
            sensor_id=data["sensor_id"],
            timestamp=data["timestamp"],
            sensor_type=SensorType(data["type"]),
            value=data["value"],
        )

class AnomalyMessage:
    def __init__(self, sensor_id: str, timestamp: int, sensor_type: SensorType, value: float, level: AnomalyLevel):
        self.SENSOR_ID = sensor_id
        self.TIMESTAMP = timestamp
        self.TYPE = sensor_type
        self.VALUE = value
        self.LEVEL = level

    def __str__(self) -> str:
        return json.dumps(
            {
                "sensor_id": self.SENSOR_ID,
                "timestamp": self.TIMESTAMP,
                "type": self.TYPE,
                "value": self.VALUE,
                "level": self.LEVEL,
            }
        )

    @staticmethod
    def from_dict(data: dict) -> "AnomalyMessage":
        return AnomalyMessage(
            sensor_id=data["sensor_id"],
            timestamp=data["timestamp"],
            sensor_type=SensorType(data["type"]),
            value=data["value"],
            level=AnomalyLevel(data["level"])
        )


class Producer:
    def __init__(self, channel: Channel, routing_key: str):
        self.CHANNEL = channel
        self.ROUTING_KEY = routing_key

    @retry(tries=5, delay=1, backoff=2, max_delay=10)
    def __publish_with_retry(self, message: SensorMessage):
        self.CHANNEL.basic_publish(
            exchange=Exchange.sensor_values,
            routing_key=self.ROUTING_KEY,
            body=str(message),
            properties=BasicProperties(delivery_mode=DeliveryMode.Persistent),
        )

    def __publish_with_thread(self, message: SensorMessage):
        thread = threading.Thread(target=self.__publish_with_retry, args=(message,))
        thread.start()

    def _publish(self, message: SensorMessage):
        self.__publish_with_thread(message)


class TemperatureProducer(Producer):
    def __init__(self, channel: Channel):
        super().__init__(channel, RoutingKey.sensor_temperature)

    def start_producing(self):
        raise NotImplementedError()


class MockTemperatureProducer(TemperatureProducer):
    ID_COUNTER = 0

    MIN_TEMP = -20.0
    MAX_TEMP = 90.0

    def __init__(self, channel: Channel):
        super().__init__(channel)
        self.SENSOR_ID = f"temperature_{MockTemperatureProducer.ID_COUNTER}"
        MockTemperatureProducer.ID_COUNTER += 1

    def start_producing(self):
        try:
            while True:
                self._publish(
                    SensorMessage(
                        sensor_id=self.SENSOR_ID,
                        timestamp=int(time.time()),
                        sensor_type=SensorType.temperature,
                        value=random.uniform(MockTemperatureProducer.MIN_TEMP, MockTemperatureProducer.MAX_TEMP),
                    )
                )
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass


class PressureProducer(Producer):
    def __init__(self, channel: Channel):
        super().__init__(channel, RoutingKey.sensor_pressure)

    def start_producing(self):
        raise NotImplementedError()


class MockPressureProducer(PressureProducer):
    ID_COUNTER = 0

    MIN_PRESSURE = 950.0
    MAX_PRESSURE = 1050.0

    def __init__(self, channel: Channel):
        super().__init__(channel)
        self.SENSOR_ID = f"pressure_{MockPressureProducer.ID_COUNTER}"
        MockPressureProducer.ID_COUNTER += 1

    def start_producing(self):
        try:
            while True:
                self._publish(
                    SensorMessage(
                        sensor_id=self.SENSOR_ID,
                        timestamp=int(time.time()),
                        sensor_type=SensorType.pressure,
                        value=random.uniform(MockPressureProducer.MIN_PRESSURE, MockPressureProducer.MAX_PRESSURE),
                    )
                )
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass


class HumidityProducer(Producer):
    def __init__(self, channel: Channel):
        super().__init__(channel, RoutingKey.sensor_humidity)

    def start_producing(self):
        raise NotImplementedError()


class MockHumidityProducer(HumidityProducer):
    ID_COUNTER = 0

    MIN_HUMIDITY = 30.0
    MAX_HUMIDITY = 70.0

    def __init__(self, channel: Channel):
        super().__init__(channel)
        self.SENSOR_ID = f"humidity_{MockHumidityProducer.ID_COUNTER}"
        MockHumidityProducer.ID_COUNTER += 1

    def start_producing(self):
        try:
            while True:
                self._publish(
                    SensorMessage(
                        sensor_id=self.SENSOR_ID,
                        timestamp=int(time.time()),
                        sensor_type=SensorType.humidity,
                        value=random.uniform(MockHumidityProducer.MIN_HUMIDITY, MockHumidityProducer.MAX_HUMIDITY),
                    )
                )
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
