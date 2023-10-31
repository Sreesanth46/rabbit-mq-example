import os
import ssl
import pika
from pika.exchange_type import ExchangeType
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
    print(f"consumer_one: Received new message: {body}")


connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel() # default

channel.exchange_declare(exchange='pubsub', exchange_type=ExchangeType.fanout)

# channel.queue_declare(queue='letterbox')

queue = channel.queue_declare(queue='', exclusive=True) # creates a dedicated queue for the consumer, once connection is closed the queue will be deleted

channel.queue_bind(exchange='pubsub', queue=queue.method.queue)

channel.basic_consume(queue=queue.method.queue, auto_ack=True, on_message_callback=on_message_received)

print("Starting consuming")

channel.start_consuming()

