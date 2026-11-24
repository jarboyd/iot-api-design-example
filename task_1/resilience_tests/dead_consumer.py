import base64
import json
import time
import urllib.parse
import urllib.request

import pika

API = "http://localhost:15672/api"
HEADERS = {"Authorization": "Basic " + base64.b64encode(b"guest:guest").decode()}


def api(method: str, path: str):
    request = urllib.request.Request(API + path, method=method, headers=HEADERS)
    with urllib.request.urlopen(request) as response:
        content = response.read()
        return json.loads(content) if content else None


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()


def show_queues(label: str):
    alarm_queue = channel.queue_declare(queue="alarm_queue", passive=True).method
    dead_letters = channel.queue_declare(queue="dead_letters", passive=True).method
    print(
        f"{label:>6}  alarm_queue: {alarm_queue.message_count:3} wartend, "
        f"{alarm_queue.consumer_count} Consumer | dead_letters: {dead_letters.message_count:3}"
    )


consumers = api("GET", "/queues/%2F/alarm_queue")["consumer_details"]
alarm_connections = {consumer["channel_details"]["connection_name"] for consumer in consumers}

for name in alarm_connections:
    print(f"\nBeende AlarmConsumer (Verbindung {name})")
    api("DELETE", "/connections/" + urllib.parse.quote(name, safe=""))

start = time.time()
for seconds in range(0, 80, 10):
    time.sleep(max(0, seconds - (time.time() - start)))
    show_queues(f"t+{seconds}s")

connection.close()
