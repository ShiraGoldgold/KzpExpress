from .rabbitmq_base import RabbitMQBase


class RabbitMQConsumer(RabbitMQBase):
    def _set_consume_channel(self, callback):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=False
        )

    def _action_when_running(self, callback):
        self._set_consume_channel(callback)
        print(f"Consumer for {self.queue_name} queue ready. Waiting for messages")
        self.channel.start_consuming()

    def consume(self, callback):
        self._run_with_retry(callback)