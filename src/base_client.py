from abc import ABC, abstractmethod
import time


class BaseClient(ABC):
    def __init__(self, host, retry_delay):
        self.host = host
        self.retry_delay = retry_delay
        self.running = True

    @abstractmethod
    def _connect(self):
        pass

    @abstractmethod
    def _is_connected(self):
        pass

    @abstractmethod
    def _handle_error(self):
        pass

    @abstractmethod
    def _action_when_running(self, *args, **kwargs):
        pass

    def _ensure_connection(self):
        if not self._is_connected():
            return self._connect()
        return True

    def _run_with_retry(self, *args, **kwargs):
        attempt = 0
        while self.running:
            try:
                if not self._ensure_connection():
                    raise Exception(f"Failed to connect to {self.host}")
                return self._action_when_running(*args, **kwargs)
            except Exception as e:
                if not self.running:
                     return self._handle_error()
                attempt += 1
                print(f"[{self.__class__.__name__}] Error: {e}. Attempt {attempt}. Retrying in {self.retry_delay}s")
                self._handle_error()
                time.sleep(self.retry_delay)

    @abstractmethod
    def close(self):
        pass

    def stop(self):
        print(f"[{self.__class__.__name__}] Stopping gracefully")
        self.running = False
