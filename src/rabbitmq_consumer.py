import time
from .rabbitmq_base import RabbitMQBase


class RabbitMQConsumer(RabbitMQBase):
    def _set_consume_channel(self, callback):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=False
        )

    def consume(self, callback):
        attempt = 0
        while True:
            try:
                if not self._ensure_connection():
                    raise Exception("Could not establish connection for consuming")
                self._set_consume_channel(callback)
                print(f"Consumer for {self.queue_name} queue ready. Waiting for messages")
                self.channel.start_consuming()
            except Exception as e:
                attempt += 1
                print(f"Error consumer: {e}")
                print(f"Connection Attempt {attempt}. Retrying in {self.retry_delay} seconds")
                self.close()
                time.sleep(self.retry_delay)