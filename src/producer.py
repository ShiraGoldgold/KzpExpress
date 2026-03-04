import pika
import json
import time

DEFAULT_RETRY_SECONDS_DELAY = 5


class RabbitMQProducer:
    def __init__(self, host='localhost', queue_name='purchases_queue'):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        try:
            params = pika.ConnectionParameters(host=self.host, connection_attempts=1)
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.confirm_delivery()
            return True
        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {e}")
            return False

    def _ensure_connection(self):
        if not self.connection or self.connection.is_closed:
            return self.connect()
        return True

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
                    raise Exception("Could not establish connection")
                self._send_to_queue(purchase_model)
                print(f"Successfully sent purchase {purchase_model.purchase_id}")
                return True
            except Exception as e:
                attempt += 1
                print(f"Error: {e}")
                print(f"Attempt {attempt}. Retrying in {DEFAULT_RETRY_SECONDS_DELAY} seconds")
                self.close()
                time.sleep(DEFAULT_RETRY_SECONDS_DELAY)

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except:
            pass
        finally:
            self.connection = None
            self.channel = None
