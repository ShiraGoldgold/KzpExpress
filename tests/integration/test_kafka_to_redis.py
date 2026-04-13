import pytest
from unittest.mock import MagicMock
from apps.kafka_to_redis_main import processing_data_to_redis
from datetime import datetime


def test_processing_skips_old_message(mocker):
    mocker.patch("src.windowLogic.is_too_old", return_value=True)
    mock_print = mocker.patch("builtins.print")
    data = {
        "purchase_id": "123",
        "purchase_time": "2020-01-01 10:00:00.000000",
        "item_id": 1
    }
    processing_data_to_redis(data)
    mock_print.assert_any_call("Skipping old message from 2020-01-01 10:00:00")


def test_processing_stores_in_redis(mocker):
    mocker.patch("src.windowLogic.is_too_old", return_value=False)
    mocker.patch("src.windowLogic.get_window_key", return_value="window:10:00-10:01")
    mock_redis = mocker.patch("apps.kafka_to_redis_main.redis_client")
    mock_redis.add_to_hset.return_value = True
    data = {
        "purchase_id": "123",
        "purchase_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "item_id": 1
    }
    processing_data_to_redis(data)
    mock_redis.add_to_hset.assert_called_once()