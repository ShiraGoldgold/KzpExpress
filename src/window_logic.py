from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from src import WINDOW_MINUTES


class WindowMLogic(ABC):
    @abstractmethod
    def get_window_key(self, time_for_window):
        pass

    def is_too_old(self, time_for_window):
        return datetime.now() - time_for_window > timedelta(minutes=WINDOW_MINUTES * 2)