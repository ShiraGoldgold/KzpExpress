import pytest
from apps.rabbitmq_to_kafka_main import processing_data_to_kafka


def test_processing_transfers_to_kafka(mocker):
    mock_kafka = mocker.patch("apps.rabbitmq_to_kafka_main.kafka_producer")
    sample_data = {"purchase_id": "test-123", "item_id": 1}
    processing_data_to_kafka(sample_data)
    mock_kafka.send_event.assert_called_once_with(sample_data)