from multiprocessing import Process
import pika


def run_sender():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    try:
        channel = connection.channel()

        channel.queue_declare(
            queue="logs", durable=True, arguments={"x-queue-type": "quorum"}
        )

        for i in range(5):
            channel.basic_publish(
                exchange="",
                routing_key="logs",
                body=f"Log Entry {i}",
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
    finally:
        connection.close()


def run_receiver():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    try:
        channel = connection.channel()

        channel.queue_declare(
            queue="logs", durable=True, arguments={"x-queue-type": "quorum"}
        )

        def callback(ch, method, properties, body):
            print(body)

        channel.basic_consume(queue="logs", auto_ack=True, on_message_callback=callback)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()


def main() -> None:
    sender = Process(target=run_sender)
    receiver = Process(target=run_receiver)

    processes = [sender, receiver]
    print("Starting sender and receiver")
    for p in processes:
        p.start()
    print("Sender and receiver running")
    for p in processes:
        p.join()
    print("All processes have finished.")
