import redis
import json
import time


class RedisClient:
    def __init__(self, host='localhost', port=6379, db=0, retry_delay=5):
        self.host = host
        self.port = port
        self.db = db
        self.retry_delay = retry_delay
        self.client = None

    def connect(self):
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

    def _ensure_connection(self):
        if not self.client:
            return self.connect()
        try:
            return self.client.ping()
        except:
            return self.connect()

    def store_data(self, data_id, data, ttl_seconds=180):
        attempt = 0
        while True:
            try:
                if not self._ensure_connection():
                    raise Exception("Could not establish connection for redis")
                json_data = json.dumps(data, default=str)
                self.client.set(name=data_id, value=json_data, ex=ttl_seconds)
                print(f"Successfully stored {data_id} in Redis with {ttl_seconds}s TTL")
                return True
            except Exception as e:
                attempt += 1
                print(f"Redis Error: {e}. Attempt {attempt}. Retrying in {self.retry_delay}s...")
                time.sleep(self.retry_delay)