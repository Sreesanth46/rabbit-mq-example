import pika
from pika.exchange_type import ExchangeType
from basic_pica_client import BasicPikaClient


class TopicExchangeMessageProducer(BasicPikaClient):
    def declare_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name)

    def declare_exchange(self, exchange_name, exchange_type):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

    def send_message(self, exchange, routing_key, body):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json"
            )
        )
        print(f"Message sent : {body =}")
    
    def close(self):
        self.channel.close()
        self.connection.close()


def publish_message(
        message,
        routing_key = "client.research.create",
        exchange_name = "topic_exchange",
        exchange_type = ExchangeType.topic,
):

    message_sender = TopicExchangeMessageProducer()

    message_sender.declare_exchange(exchange_name, exchange_type)

    message_sender.send_message(exchange=exchange_name, routing_key=routing_key, body=message)

    message_sender.close()

publish_message("hello world")