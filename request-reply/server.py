import os
import ssl
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

def on_request_message_received(ch, method, properties, body):
    print(f"Received request cr_id: {properties.correlation_id}")
    ch.basic_publish(
        '',
        routing_key=properties.reply_to, 
        body=f"reply to cr_id: {properties.correlation_id}"
    )


connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel() # default

channel.queue_declare(queue='request_queue')

channel.basic_consume('request_queue', auto_ack=True, on_message_callback=on_request_message_received)

print("Starting server")

channel.start_consuming()

