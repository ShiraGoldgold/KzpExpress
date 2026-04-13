import pytest
from src.infra.base_client import BaseClient


class MockClient(BaseClient):
    def _connect(self):
        return True

    def _is_connected(self):
        return False

    def _handle_error(self):
        pass

    def _action_when_running(self, *args):
        return "success"

    def close(self):
        pass


def test_base_client_retry_logic(mocker):
    client = MockClient(host="localhost", retry_delay=0.1)
    mocker.patch.object(client, '_ensure_connection', side_effect=[False, False, True])
    result = client._run_with_retry()
    assert result == "success"