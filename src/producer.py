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
        self.SECONDS_WAIT_BEFORE_RETRY = 2

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

    def publish(self, purchase_model):
        retries = 0
        while retries < self.max_retries:
            if not self.connection or self.connection.is_closed:
                is_connected = self.connect()
                if not is_connected:
                    retries += 1
                    print(f"Connection failed. Attempt {retries} retry")
                    time.sleep(self.SECONDS_WAIT_BEFORE_RETRY)
                    continue
            try:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=json.dumps(purchase_model.dict(), default=str),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"Successfully sent purchase {purchase_model.purchase_id}")
                return True
            except Exception as e:
                retries += 1
                print(f"Publish error: {e}. Attempt {retries} retry")
                self.close()
                time.sleep(self.SECONDS_WAIT_BEFORE_RETRY)
        print(f"Failed to send message after {self.max_retries} attempts.")
        return False

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except:
            pass
        finally:
            self.connection = None
            self.channel = None
