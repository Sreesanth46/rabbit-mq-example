from pika.exchange_type import ExchangeType
from basic_pica_client import BasicPikaClient


class TopicExchangeMessageConsumer(BasicPikaClient):
    def declare_queue(self, queue_name, *args, **kwargs):
        self.queue = self.channel.queue_declare(queue=queue_name, *args, **kwargs)

    def declare_exchange(self, exchange_name, exchange_type):
        self.exchange_name = exchange_name
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

    def bind_queue(self, routing_key):
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.queue.method.queue,
            routing_key=routing_key
        )

    def consume_messages(self, *args, **kwargs):
        self.channel.basic_consume(
            queue=self.queue.method.queue,
            on_message_callback=message_callback,
            auto_ack=True,
            *args,
            **kwargs    
        )
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def close(self):
        self.channel.close()
        self.connection.close()

def message_callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

    print(f"\n {ch =} \n {method =} \n {properties =} \n {body =}")


def main():
    basic_message_receiver = TopicExchangeMessageConsumer()

    basic_message_receiver.declare_exchange("topic_exchange", ExchangeType.topic)

    basic_message_receiver.declare_queue("email_service_queue")

    basic_message_receiver.bind_queue("#.create")

    basic_message_receiver.consume_messages()

    basic_message_receiver.close()


if __name__ == "__main__":
    main()