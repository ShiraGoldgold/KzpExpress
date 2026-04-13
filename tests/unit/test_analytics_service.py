from unittest.mock import MagicMock
from src.services.analytics_service import AnalyticsService


def test_top_three_sorting():
    mock_redis = MagicMock()
    mock_redis.get_hset_values.return_value = ["1", "1", "1", "2", "2"]
    service = AnalyticsService(mock_redis)
    result = service.get_top_three("some_key")
    assert len(result) == 2
    assert result[0]["name"] == "Ball"
    assert result[0]["count"] == 3