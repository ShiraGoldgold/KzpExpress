import pika
import json
import time


class RabbitMQProducer:
    def __init__(self, host='localhost', queue_name='purchases_queue', max_retries=3):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.max_retries = max_retries

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.confirm_delivery()

    def publish(self, purchase_model):
        retries = 0
        while retries < self.max_retries:
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
            except (pika.exceptions.AMQPError, pika.exceptions.ConnectionClosed) as e:
                retries += 1
                print(f"Connection error: {e}. Attempt {retries} retry")
                time.sleep(2)
                self.connect()
        print(f"Failed to send message {purchase_model.purchase_id} after {self.max_retries} attempts.")
        return False

    def close(self):
        if self.connection:
            self.connection.close()