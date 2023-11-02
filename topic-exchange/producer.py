import os
import ssl
import pika
from dotenv import load_dotenv
from pika.exchange_type import ExchangeType

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

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel() # default

channel.exchange_declare(exchange='topicexchange', exchange_type=ExchangeType.topic)

# channel.queue_declare(queue='letterbox')

user_payments_message = "A european user paid for something"

channel.basic_publish(exchange='topicexchange', routing_key='user.europe.payments', body=user_payments_message)

print(f"sent message: {user_payments_message}")

business_order_message = "A business order had been placed in europe"

channel.basic_publish(exchange='topicexchange', routing_key='business.europe.order', body=business_order_message)

print(f"sent message: {business_order_message}")


connection.close()
