import json
from confluent_kafka import Consumer, KafkaError
from .base_client import BaseClient


class KafkaConsumer(BaseClient):
    def __init__(self, retry_delay, topic_name, group_id, host='localhost', port=9092):
        super().__init__(host, retry_delay)
        self.topic_name = topic_name
        self.group_id = group_id
        self.port = port
        self.consumer = None

    def _connect(self):
        try:
            config = {
                'bootstrap.servers': f"{self.host}:{self.port}",
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
            self.consumer = Consumer(config)
            self.consumer.subscribe([self.topic_name])
            return True
        except Exception as e:
            print(f"Failed to connect Kafka Consumer: {e}")
            return False

    def _is_connected(self):
        return self.consumer is not None

    def _handle_error(self):
        self.close()

    def close(self):
        if self.consumer:
            self.consumer.close()
            self.consumer = None

    def _action_when_running(self, callback):
        msg = self.consumer.poll(1.0)
        if msg is None:
            return
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return
            else:
                raise Exception(msg.error())
        try:
            callback(json.loads(msg.value().decode('utf-8')))
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            print(f"Error processing Kafka message: {e}")
            raise e

    def consume_and_act_realtime_msgs(self, callback):
        while self.running:
            self._run_with_retry(callback)