from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from ..constants import WINDOW_MINUTES


class WindowMLogic(ABC):
    @abstractmethod
    def get_window_key(self, time_for_window):
        pass

    def get_previous_window_key(self, time_for_window):
        return self.get_window_key(time_for_window - timedelta(minutes=WINDOW_MINUTES))

    def is_too_old(self, time_for_window):
        return datetime.now() - time_for_window > timedelta(minutes=WINDOW_MINUTES * 2)