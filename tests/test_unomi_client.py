from unittest.mock import MagicMock, patch
import pytest
from urllib.error import URLError
from app.services.unomi_client import UnomiClient, UnomiClientError
from app.services.unomi_settings_service import UnomiConnectionConfig

def _cfg():
    return UnomiConnectionConfig(base_url="https://u", username="k", password="p", scope="b")

@patch("app.services.unomi_client.urlopen")
@patch("app.services.unomi_client.time.sleep")
def test_client_retries_transient_read_timeout(mock_sleep, mock_urlopen):
    ok_resp = MagicMock()
    ok_resp.read.return_value = b'{"itemId":"p1"}'
    ok_resp.__enter__.return_value = ok_resp
    ok_resp.__exit__.return_value = False
    mock_urlopen.side_effect = [URLError("The read operation timed out"), ok_resp]
    client = UnomiClient(_cfg(), timeout_sec=5.0, max_retries=2)
    assert client.get_profile("p1") == {"itemId": "p1"}
    assert mock_urlopen.call_count == 2
