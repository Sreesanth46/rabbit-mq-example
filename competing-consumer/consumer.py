import os
import ssl
import time
import random
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
    processing_time = random.randint(1,6)
    print(f"Received : {body}, will take {processing_time} to process")
    time.sleep(processing_time)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    print("Finished processing the message")

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel() # default

channel.queue_declare(queue='letterbox')

channel.basic_qos(prefetch_count=1) # only process 1 message at a time

channel.basic_consume('letterbox', on_message_callback=on_message_received)

print("Starting consuming")

channel.start_consuming()

