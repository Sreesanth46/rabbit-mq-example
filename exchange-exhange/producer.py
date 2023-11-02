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

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel() # default

channel.exchange_declare(exchange='firstexchange', exchange_type=ExchangeType.direct) # declare 2 exchanges

channel.exchange_declare(exchange='secondexchange', exchange_type=ExchangeType.fanout)

# where we want the message to go to first arg and from where its going from second arg
channel.exchange_bind("secondexchange", "firstexchange")

message = "This message has gone through multiple exchanges"

channel.basic_publish(exchange='firstexchange', routing_key='', body=message)

print(f"sent message: {message}")

connection.close()
