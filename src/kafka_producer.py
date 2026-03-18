import json
from confluent_kafka import Producer
from .base_client import BaseClient


class KafkaProducer(BaseClient):
    def __init__(self, retry_delay, topic_name, host='localhost', port=9092):
        super().__init__(host, retry_delay)
        self.topic_name = topic_name
        self.producer = None
        self.port = port

    def _connect(self):
        try:
            config = {
                'bootstrap.servers': f"{self.host}:{self.port}",
                'acks': 'all',
                'enable.idempotence': True,
                'message.timeout.ms': 5000
            }
            self.producer = Producer(config)
            return True
        except Exception as e:
            print(f"Failed to connect to Kafka at {self.host}: {e}")
            return False

    def _is_connected(self):
        try:
            if self.producer:
                self.producer.list_topics(timeout=1)
                return True
            return False
        except:
            return False

    def _handle_error(self):
        self.close()

    def _delivery_report(self, error, msg):
        if error is not None:
            raise Exception(f"Kafka delivery failed: {error}")
        else:
            print(f"Successfully stored msg in Kafka topic {msg.topic()}")

    def _action_when_running(self, data):
        self.producer.produce(
            topic=self.topic_name,
            value=json.dumps(data, default=str),
            callback=self._delivery_report
        )
        self.producer.poll(0)
        self.producer.flush()
        return True

    def send_event(self, data):
        return self._run_with_retry(data)

    def close(self):
        if self.producer:
            self.producer.flush(timeout=5)
            self.producer = None
