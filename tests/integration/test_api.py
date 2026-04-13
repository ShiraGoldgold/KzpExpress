import pytest
from fastapi.testclient import TestClient
from apps.api_main import app

client = TestClient(app)


def test_api_returns_hot_products(mocker):
    mocker.patch("src.services.analytics_service.AnalyticsService.get_top_three",
                 return_value=[{"name": "Coffee", "count": 10}])
    response = client.get("/hot-products-last-1-minutes-window")
    assert response.status_code == 200
    assert response.json()["data"][0]["name"] == "Coffee"