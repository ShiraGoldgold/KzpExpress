import pika
import json
import time
from rabbitmq_base import RabbitMQBase


class RabbitMQProducer(RabbitMQBase):
    def _connect(self):
        success = super()._connect()
        if success:
            self.channel.confirm_delivery()
        return success

    def _send_to_queue(self, purchase_model):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=json.dumps(purchase_model.dict(), default=str),
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def publish(self, purchase_model):
        attempt = 0
        while True:
            try:
                if not self._ensure_connection():
                    raise Exception("Could not establish connection for producing")
                self._send_to_queue(purchase_model)
                print(f"Successfully sent purchase {purchase_model.purchase_id}")
            except Exception as e:
                attempt += 1
                print(f"Error producer: {e}")
                print(f"Attempt {attempt}. Retrying in {self.retry_delay} seconds")
                self.close()
                time.sleep(self.retry_delay)
