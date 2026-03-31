from datetime import timedelta
from constants import WINDOW_MINUTES
from .window_logic import WindowMLogic


class ThumblingWindowLogic(WindowMLogic):
    def get_window_key(self, time_for_window):
        total_minutes = time_for_window.hour * 60 + time_for_window.minute
        start_of_window_minutes = ((total_minutes // WINDOW_MINUTES)
                                   * WINDOW_MINUTES)
        window_start = time_for_window.replace(
            hour=start_of_window_minutes // 60, minute=start_of_window_minutes % 60,
            second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=WINDOW_MINUTES)
        return f"window:{window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')}"
