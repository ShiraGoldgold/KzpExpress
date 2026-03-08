import pika


class RabbitMQBase:
    def __init__(self, host='localhost', queue_name='purchases_queue'):
        self.host = host
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
            print(f"Failed to connect to RabbitMQ: {e}")
            return False

    def _ensure_connection(self):
        if not self.connection or self.connection.is_closed:
            return self._connect()
        return True

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except:
            pass
        finally:
            self.connection = None
            self.channel = None