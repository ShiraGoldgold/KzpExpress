import redis
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

    def _action_when_running(self, action, *args, **kwargs):
        action(*args, **kwargs)
        return True

    def add_to_hset(self, key, data_field, data_value, ttl_seconds):
        def logic():
            self.client.hset(key, data_field, data_value)
            self.client.expire(name=key, time=ttl_seconds)
        return self._run_with_retry(logic)

    def get_hset_values(self, key):
        def logic():
            return self.client.hvals(key)
        return self._run_with_retry(logic)

    def close(self):
        if self.client:
            try:
                self.client.close()
            except:
                pass