import pika
import json
from .rabbitmq_base import RabbitMQBase


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

    def _action_when_running(self, purchase_model):
        self._send_to_queue(purchase_model)
        print(f"Successfully sent purchase {purchase_model.purchase_id}")

    def publish(self, purchase_model):
        self._run_with_retry(purchase_model)
