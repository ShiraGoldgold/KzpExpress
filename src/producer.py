import pika
import json


class RabbitMQProducer:
    def __init__(self, host='localhost', queue_name='purchases_queue'):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.confirm_delivery()

    def publish(self, purchase_model):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(purchase_model.dict(), default=str),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                )
            )
            print(f"Successfully sent purchase {purchase_model.purchase_id}")
            return True
        except Exception as e:
            print(f"Failed to send message: {e}")
            return False

    def close(self):
        if self.connection:
            self.connection.close()