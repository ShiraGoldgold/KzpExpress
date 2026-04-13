import json
from confluent_kafka import Consumer, KafkaError
from ..base_client import BaseClient


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
                'enable.auto.commit': False,
                'broker.address.family': 'v4',
                'session.timeout.ms': 6000,
                'heartbeat.interval.ms': 2000,
                'max.poll.interval.ms': 300000
            }
            self.consumer = Consumer(config)
            self.consumer.subscribe([self.topic_name])
            return True
        except Exception as e:
            print(f"Failed to connect Kafka Consumer: {e}")
            return False

    def _is_connected(self):
        try:
            if self.consumer is None:
                return False
            self.consumer.list_topics(topic=self.topic_name, timeout=0.5)
            return True
        except:
            return False

    def _handle_error(self):
        self.close()

    def _action_when_running(self, callback):
        while self.running:
            msg = self.consumer.poll(2.0)
            if msg is None:
                if not self._is_connected():
                    raise Exception("Kafka Broker is unreachable")
                return
            if msg.error():
                if msg.error().code() in [KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART]:
                    return
                else:
                    raise Exception(msg.error())
            callback(json.loads(msg.value().decode('utf-8')))
            self.consumer.commit(asynchronous=True)


    def consume_and_act_realtime_msg(self, callback):
        self._run_with_retry(callback)

    def close(self):
        if self.consumer:
            self.consumer.close()
            self.consumer = None