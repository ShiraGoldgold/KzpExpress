import json
import time
from .rabbitmq_base import RabbitMQBase


class RabbitMQConsumer(RabbitMQBase):
    def _set_consume_channel(self, callback):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=lambda ch, method, prop, body:
                self._callback_wrapper(callback, ch, method, prop, body),
            auto_ack=False
        )

    def _callback_wrapper(self, callback, ch, method, properties, body):
        try:
            data = json.loads(body)
            callback(data)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Callback failed: {e}. Requeueing message")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            time.sleep(self.retry_delay)

    def _action_when_running(self, callback):
        self._set_consume_channel(callback)
        print(f"Consumer for {self.queue_name} queue ready. Waiting for messages")
        self.channel.start_consuming()

    def consume(self, callback):
        self._run_with_retry(callback)

    def stop(self):
        if self.channel and self.channel.is_open:
            print("Stopping RabbitMQ consumption")
            self.channel.stop_consuming()
        super().stop()
