import pytest
from datetime import datetime, timedelta
from src.core.thumbling_window_logic import ThumblingWindowLogic


def test_get_window_key_logic():
    logic = ThumblingWindowLogic()
    test_time = datetime(2026, 4, 12, 10, 5, 30)
    key = logic.get_window_key(test_time)
    assert key == "window:10:05-10:06"


def test_is_too_old_logic():
    logic = ThumblingWindowLogic()
    now = datetime.now()
    old_time = now - timedelta(minutes=10)
    assert logic.is_too_old(old_time) is True
    assert logic.is_too_old(now) is False