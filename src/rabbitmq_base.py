from abc import ABC
import pika
from base_client import BaseClient


class RabbitMQBase(BaseClient, ABC):
    def __init__(self, host='localhost', queue_name='purchases_queue', retry_delay=5):
        super().__init__(host, retry_delay)
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def _connect(self):
        try:
            params = pika.ConnectionParameters(host=self.host, connection_attempts=1)
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            return True
        except Exception as e:
            print(f"Failed to connect to RabbitMQ at {self.host}: {e}")
            return False

    def _is_connected(self):
        return self.connection is not None and self.connection.is_open

    def _handle_error(self):
        self._close()

    def _close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except:
            pass
        finally:
            self.connection = None
            self.channel = None