import os
import ssl
import uuid
import pika
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_BROKER_ID = os.environ.get("RABBITMQ_BROKER_ID")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD")
RABBITMQ_REGION = os.environ.get("RABBITMQ_REGION")

ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')

connection_url = f"amqps://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_BROKER_ID}.mq.{RABBITMQ_REGION}.amazonaws.com:5671"

connection_parameters = pika.URLParameters(connection_url)
connection_parameters.ssl_options = pika.SSLOptions(context=ssl_context)

def on_message_received(ch, method, properties, body):
    print(f"Received a reply: {body}")


connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel() # default

reply_queue = channel.queue_declare(queue='', exclusive=True)

channel.basic_consume(queue=reply_queue.method.queue, auto_ack=True, on_message_callback=on_message_received)

channel.queue_declare(queue='request_queue')

message = "Requesting a reply -->"

correlation_id = uuid.uuid4().hex

print(f"Sending request {correlation_id =}")

channel.basic_publish(
    exchange='', 
    routing_key='request_queue', 
    properties=pika.BasicProperties(
        reply_to=reply_queue.method.queue,
        correlation_id=correlation_id
    ), 
    body=message)


print("Starting client")

channel.start_consuming()

