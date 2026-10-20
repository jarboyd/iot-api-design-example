import json
import sys

from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from iot_design_example.infra import Exchange
from iot_design_example.producers import (
    SensorMessage, 
    AnomalyMessage,    
)
from iot_design_example.consumers.aggregration import (
    AggregationMessage,
    AggregationType
)


class MonitorConsumer:
    SUMMARY_INTERVAL_SECONDS = 10
    _channel: Channel

    _processed_messages: int
    _sensor_messages_by_type: dict[str, int]
    _anomalies: int
    _failed_messages: int
    _processing_time_ms: float
    _processing_attempts: int

    _message_type_processing_funcs: dict[str, callable[[bytes], None]]

    _further_infos: dict[str, str]

    def __init__(self, channel: Channel):
        self._channel = channel
        self._processed_messages = 0
        self._failed_messages = 0
        self._anomalies = 0
        self._processing_time_ms = 0.0
        self._processing_attempts = 0
        self._sensor_messages_by_type = {}
        self._further_infos = {}
        self._message_type_processing_funcs = {
            Exchange.sensor_values: self._process_sensor_message,
            Exchange.aggr_values: self._process_aggregation_message,
            Exchange.anomalies: self._process_anomaly_message,
        }

    def start_consuming(self):
        result = self._channel.queue_declare(queue="", exclusive=True)
        queue_name = result.method.queue
        for exchange in (Exchange.sensor_values, Exchange.aggr_values, Exchange.anomalies):
            self._channel.queue_bind(
                exchange=exchange,
                queue=queue_name,
                routing_key="#",
            )
        self._channel.basic_consume(
            queue=queue_name, auto_ack=False, on_message_callback=self.callback
        )
        self.print_summary()
        self._channel.connection.call_later(
            self.SUMMARY_INTERVAL_SECONDS, self._print_and_schedule
        )
        self._channel.start_consuming()

    def _print_and_schedule(self):
        self.print_summary()
        self._channel.connection.call_later(
            self.SUMMARY_INTERVAL_SECONDS, self._print_and_schedule
        )

    def print_summary(self):
        print("==================== Monitoring Summary ====================")
        print("General statistics:")
        print(" > sensor_messages_received:     ", sum(self._sensor_messages_by_type.values()))
        print(" > processed:                    ", self._processed_messages)
        print(" > failed:                       ", self._failed_messages)
        print(" > anomalies/alarms:             ", self._anomalies)
        print(" > average_processing_time_ms:   ", "<TODO>")
        
        print(f"Sensor messages by type: ({len(self._sensor_messages_by_type)})")
        for sensor_type, count in self._sensor_messages_by_type.items():
            sensor_type = f"{sensor_type}:"
            print(f" - {sensor_type:<31}{count}")

        print(f"Further infos: ({len(self._further_infos)})")
        for key, value in self._further_infos.items():
            key = f"{key}:"
            print(f" > {key:<31}{value}")

        print()
        print()

    def _process_sensor_message(self, body: bytes):
        sensor_message: SensorMessage = SensorMessage.from_dict(json.loads(body.decode()))
        self._sensor_messages_by_type[sensor_message.TYPE] = (
            self._sensor_messages_by_type.get(sensor_message.TYPE, 0) + 1
        )

    def _process_aggregation_message(self, body: bytes):
        aggregation_message: AggregationMessage = AggregationMessage.from_dict(json.loads(body.decode()))
        if aggregation_message.aggr_type == AggregationType.max_temperature:
            value = aggregation_message.value
            self._further_infos["max_temperature"] = f"{value[1]:.2f}°C <{value[0]}>"

    def _process_anomaly_message(self, body: bytes):
        anomaly_message: AnomalyMessage = AnomalyMessage.from_dict(json.loads(body.decode()))
        self._anomalies += 1

    def _process_unknown_message(self, body: bytes):   
        print(f"[Monitor] Received unknown message: {body.decode()}", file=sys.stderr)

    def _process_error_message(self, body: bytes, error: Exception):
        print(f"[Monitor] Error processing message: {error}", file=sys.stderr)

    def callback(
        self,
        ch: Channel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        try:
            self._message_type_processing_funcs.get(method.exchange, self._process_unknown_message)(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as error:
            self._process_error_message(body, error)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)