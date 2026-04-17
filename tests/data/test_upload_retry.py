"""Tests for whole-operation retry in onelake.upload_file."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from pyfabric.data.onelake import upload_file


def _ok_response() -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    return resp


def _http_error(status: int) -> requests.HTTPError:
    resp = MagicMock()
    resp.status_code = status
    err = requests.HTTPError(f"HTTP {status}")
    err.response = resp
    return err


class TestUploadRetry:
    def test_success_first_attempt(self):
        session = MagicMock()
        session.put.return_value = _ok_response()
        session.patch.return_value = _ok_response()
        with patch("pyfabric.data.onelake._get_session", return_value=session):
            upload_file("tok", "ws", "lh", "Files/x", b"data", backoff_seconds=0)
        assert session.put.call_count == 1
        assert session.patch.call_count == 2

    def test_retries_on_5xx(self):
        """A 500 on flush PATCH re-runs the whole 3-step protocol."""
        session = MagicMock()
        session.put.return_value = _ok_response()
        # First flush fails with 500, second attempt succeeds on all three steps.
        ok = _ok_response()
        failing = MagicMock()
        failing.raise_for_status.side_effect = _http_error(500)
        session.patch.side_effect = [ok, failing, ok, ok]
        with patch("pyfabric.data.onelake._get_session", return_value=session):
            upload_file("tok", "ws", "lh", "Files/x", b"data", backoff_seconds=0)
        assert session.put.call_count == 2  # 1 initial + 1 retry
        assert session.patch.call_count == 4  # append+flush twice

    def test_retries_on_connection_error(self):
        session = MagicMock()
        session.put.side_effect = [
            requests.ConnectionError("boom"),
            _ok_response(),
        ]
        session.patch.return_value = _ok_response()
        with patch("pyfabric.data.onelake._get_session", return_value=session):
            upload_file("tok", "ws", "lh", "Files/x", b"data", backoff_seconds=0)
        assert session.put.call_count == 2

    def test_4xx_fails_fast(self):
        """403/404 etc must NOT trigger whole-operation retry."""
        session = MagicMock()
        failing = MagicMock()
        failing.raise_for_status.side_effect = _http_error(403)
        session.put.return_value = failing
        with (
            patch("pyfabric.data.onelake._get_session", return_value=session),
            pytest.raises(requests.HTTPError),
        ):
            upload_file("tok", "ws", "lh", "Files/x", b"data", backoff_seconds=0)
        assert session.put.call_count == 1

    def test_exhausts_attempts_then_raises(self):
        session = MagicMock()
        failing = MagicMock()
        failing.raise_for_status.side_effect = _http_error(503)
        session.put.return_value = failing
        with (
            patch("pyfabric.data.onelake._get_session", return_value=session),
            pytest.raises(requests.HTTPError),
        ):
            upload_file(
                "tok",
                "ws",
                "lh",
                "Files/x",
                b"data",
                max_attempts=3,
                backoff_seconds=0,
            )
        assert session.put.call_count == 3

    def test_max_attempts_one_disables_retry(self):
        session = MagicMock()
        failing = MagicMock()
        failing.raise_for_status.side_effect = _http_error(500)
        session.put.return_value = failing
        with (
            patch("pyfabric.data.onelake._get_session", return_value=session),
            pytest.raises(requests.HTTPError),
        ):
            upload_file(
                "tok",
                "ws",
                "lh",
                "Files/x",
                b"data",
                max_attempts=1,
                backoff_seconds=0,
            )
        assert session.put.call_count == 1
