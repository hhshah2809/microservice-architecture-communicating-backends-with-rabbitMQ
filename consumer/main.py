import pika
import json
import time

QUEUE = "task_queue"


def connect():

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="localhost"
        )
    )

    channel = connection.channel()

    channel.queue_declare(
        queue=QUEUE,
        durable=True
    )

    return connection, channel


def process_message(ch, method, properties, body):

    try:

        message = json.loads(body.decode())

        print("\n==============================")
        print("MESSAGE RECEIVED")
        print("==============================")

        print(json.dumps(
            message,
            indent=4
        ))


        # Simulating processing
        print("\nProcessing message...")

        time.sleep(2)

        print("Message processed successfully")


        # Manual acknowledgement
        ch.basic_ack(
            delivery_tag=method.delivery_tag
        )


    except Exception as error:

        print("Error processing message:", error)

        # Reject message
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )


def start_consumer():

    while True:

        try:

            connection, channel = connect()


            # Process one message at a time
            channel.basic_qos(
                prefetch_count=1
            )


            channel.basic_consume(
                queue=QUEUE,
                on_message_callback=process_message
            )


            print("Python Consumer is waiting for messages...")
            print("Press CTRL + C to stop\n")


            channel.start_consuming()


        except pika.exceptions.AMQPConnectionError:

            print("RabbitMQ is not available. Retrying...")

            time.sleep(5)


        except KeyboardInterrupt:

            print("\nConsumer stopped")

            break


if __name__ == "__main__":

    start_consumer()