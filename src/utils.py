from datetime import timedelta, datetime
from constants import THUMBLING_WINDOW_MINUTES


def get_thumbling_window_key(purchase_time_str):
    purchase_time = datetime.strptime(purchase_time_str, "%Y-%m-%d %H:%M:%S.%f")
    total_minutes = purchase_time.hour * 60 + purchase_time.minute
    start_of_window_minutes = ((total_minutes // THUMBLING_WINDOW_MINUTES)
                               * THUMBLING_WINDOW_MINUTES)
    window_start = purchase_time.replace(
        hour=start_of_window_minutes // 60, minute=start_of_window_minutes % 60,
        second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=THUMBLING_WINDOW_MINUTES)
    return f"window:{window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')}"
