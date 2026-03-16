import json
import time
from .rabbitmq_base import RabbitMQBase


class RabbitMQConsumer(RabbitMQBase):
    def _action_when_running(self, callback):
        method_frame, header_frame, body = (
            self.channel.basic_get(queue=self.queue_name, auto_ack=False))
        if method_frame:
            try:
                callback(json.loads(body))
                self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except Exception as e:
                self.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                print(f"Callback failed: {e}. Requeueing message")

    def consume_one_msg(self, callback):
        self._run_with_retry(callback)

