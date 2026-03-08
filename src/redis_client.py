import redis
import json
from .base_client import BaseClient


class RedisClient(BaseClient):
    def __init__(self, retry_delay, host='localhost', port=6379, db=0):
        super().__init__(host, retry_delay)
        self.port = port
        self.db = db
        self.client = None

    def _connect(self):
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=True
            )
            return self.client.ping()
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            return False

    def _is_connected(self):
        try:
            return self.client.ping()
        except:
            return False

    def _handle_error(self):
        self.close()

    def _action_when_running(self, data_id, data, ttl_seconds):
        json_data = json.dumps(data, default=str)
        self.client.set(name=data_id, value=json_data, ex=ttl_seconds)
        print(f"Successfully stored {data_id} in Redis with {ttl_seconds}s TTL")

    def store_data(self, data_id, data, ttl_seconds):
        self._run_with_retry(data_id, data, ttl_seconds)

    def close(self):
        if self.client:
            try:
                self.client.close()
            except:
                pass