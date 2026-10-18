import time
from enum import Enum
import json
from pika.channel import Channel
from pika import BasicProperties, DeliveryMode

from iot_design_example.infra import Exchange


class SensorType(int, Enum):
    temperature = 0
    pressure = 1
    humidity = 2


class RoutingKey(str, Enum):
    wildcard = "#"
    sensor_temperature = "sensor.temperature"
    sensor_pressure = "sensor.pressure"
    sensor_humidity = "sensor.humidity"


class SensorMessage:
    def __init__(self, sensor_id: str, timestamp: int, type: SensorType, value: float):
        self.SENSOR_ID = sensor_id
        self.TIMESTAMP = timestamp
        self.TYPE = type
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


class Producer:
    def __init__(self, channel: Channel, routing_key: str):
        self.CHANNEL = channel
        self.ROUTING_KEY = routing_key

    def _publish(self, message: SensorMessage):
        self.CHANNEL.basic_publish(
            exchange=Exchange.sensor_values,
            routing_key=self.ROUTING_KEY,
            body=str(message),
            properties=BasicProperties(delivery_mode=DeliveryMode.Persistent),
        )


class TemperatureProducer(Producer):
    def __init__(self, channel: Channel):
        super().__init__(channel, RoutingKey.sensor_temperature)

    def start_producing(self):
        raise NotImplementedError()


class MockTemperatureProducer(TemperatureProducer):
    ID_COUNTER = 0

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
                        timestamp=0,
                        type=SensorType.temperature,
                        value=0.0,
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
                        timestamp=0,
                        type=SensorType.pressure,
                        value=0.0,
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
                        timestamp=0,
                        type=SensorType.humidity,
                        value=0.0,
                    )
                )
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
