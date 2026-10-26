import time

import pika

MESSAGES = [
    ("kein JSON", 'das ist kein json{'),
    ("unbekannter SensorType", '{"sensor_id": "t_inv", "timestamp": 0, "type": 7, "value": 20}'),
    ("value ist kein float", '{"sensor_id": "t_inv", "timestamp": 0, "type": 0, "value": "heiss"}'),
    ("Feld value fehlt", '{"sensor_id": "t_inv", "timestamp": 0, "type": 0}'),
    ("gueltig, kritisch (Kontrolle)", '{"sensor_id": "t_crit", "timestamp": 0, "type": 0, "value": 150}'),
]

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

dlq_before = channel.queue_declare(queue="dead_letters", passive=True).method.message_count

for label, body in MESSAGES:
    print(f"Sende {label}: {body}")
    channel.basic_publish(exchange="sensor_values", routing_key="sensor.temperature", body=body)

time.sleep(2)
dlq_after = channel.queue_declare(queue="dead_letters", passive=True).method.message_count
print(f"\nNachrichten in dead_letters: vorher {dlq_before}, nachher {dlq_after}")

connection.close()
