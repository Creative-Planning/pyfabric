"""Error path tests for FabricClient — LRO failures, HTTP errors, retries.

Each test verifies that error messages contain enough context (URL, status,
body excerpt) for a human or AI to quickly determine root cause from logs.
"""

from unittest.mock import MagicMock

import pytest

from pyfabric.client.http import FabricClient, FabricError


def _make_client(mock_session: MagicMock) -> FabricClient:
    client = FabricClient("fake-token")
    client._session = mock_session
    return client


def _resp(status: int, body: str = "", headers: dict | None = None) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = body
    r.content = body.encode()
    r.headers = headers or {}
    r.json.return_value = {}
    if body:
        try:
            import json

            r.json.return_value = json.loads(body)
        except Exception:
            pass
    return r


class TestFabricErrorContext:
    """Verify FabricError carries URL, status, and body for diagnostics."""

    def test_includes_status_code(self):
        err = FabricError(
            403,
            '{"error":{"message":"Forbidden"}}',
            "https://api.fabric.microsoft.com/v1/workspaces/ws-1",
        )
        assert "403" in str(err)

    def test_includes_url(self):
        url = "https://api.fabric.microsoft.com/v1/workspaces/ws-1/items/item-1"
        err = FabricError(404, '{"error":{"message":"Not found"}}', url)
        assert "ws-1" in str(err)
        assert "item-1" in str(err)

    def test_includes_error_message_from_json(self):
        err = FabricError(
            400,
            '{"error":{"message":"Display name already exists"}}',
            "https://example.com",
        )
        assert "Display name already exists" in str(err)

    def test_handles_non_json_body(self):
        err = FabricError(502, "Bad Gateway", "https://example.com")
        assert "Bad Gateway" in str(err)

    def test_handles_empty_body(self):
        err = FabricError(500, "", "https://example.com")
        assert "500" in str(err)

    def test_attributes_accessible(self):
        err = FabricError(429, "Rate limited", "https://example.com/api")
        assert err.status == 429
        assert err.body == "Rate limited"
        assert err.url == "https://example.com/api"


class TestRequestErrors:
    """Test HTTP request error paths."""

    def test_4xx_raises_with_url_and_status(self):
        session = MagicMock()
        session.request.return_value = _resp(
            403, '{"error":{"message":"Access denied to workspace ws-123"}}'
        )
        client = _make_client(session)
        with pytest.raises(FabricError, match="403") as exc_info:
            client.get("workspaces/ws-123")
        assert "Access denied" in str(exc_info.value)

    def test_5xx_raises_with_body(self):
        session = MagicMock()
        session.request.return_value = _resp(500, "Internal Server Error")
        client = _make_client(session)
        with pytest.raises(FabricError, match="500"):
            client.post("workspaces", {"displayName": "test"})


class TestLroErrors:
    """Test Long-Running Operation error paths."""

    def test_lro_failed_status(self):
        """LRO poll returns status=Failed — error should include LRO URL."""
        session = MagicMock()
        # Initial 202 with Location
        session.request.side_effect = [
            _resp(
                202,
                "",
                headers={
                    "Location": "https://api.fabric.microsoft.com/v1/operations/op-1"
                },
            ),
            _resp(200, '{"status": "Failed"}'),
        ]
        client = _make_client(session)
        with pytest.raises(FabricError) as exc_info:
            client.post("workspaces/ws-1/items")
        assert "operations/op-1" in str(exc_info.value) or "Failed" in str(
            exc_info.value
        )

    def test_lro_cancelled_status(self):
        session = MagicMock()
        session.request.side_effect = [
            _resp(
                202,
                "",
                headers={
                    "Location": "https://api.fabric.microsoft.com/v1/operations/op-2"
                },
            ),
            _resp(200, '{"status": "Cancelled"}'),
        ]
        client = _make_client(session)
        with pytest.raises(FabricError):
            client.post("workspaces/ws-1/items")

    def test_202_missing_location_header(self):
        """202 without Location header — critical protocol error."""
        session = MagicMock()
        session.request.return_value = _resp(202, "", headers={})
        client = _make_client(session)
        with pytest.raises(RuntimeError, match="no Location header") as exc_info:
            client.post("workspaces/ws-1/items")
        assert "202" in str(exc_info.value)

    def test_lro_optional_result_fetch_failure_swallowed(self):
        """LRO /result fetch fails — should not raise, returns partial result."""
        session = MagicMock()
        # 202 -> succeeded with no id -> /result fetch fails
        session.request.side_effect = [
            _resp(
                202,
                "",
                headers={
                    "Location": "https://api.fabric.microsoft.com/v1/operations/op-3"
                },
            ),
            _resp(200, '{"status": "Succeeded"}'),  # no "id" key
            _resp(404, '{"error":{"message":"Not found"}}'),  # /result fails
        ]
        client = _make_client(session)
        # Should not raise — failure is swallowed
        result = client.post("workspaces/ws-1/items")
        assert result.get("status") == "Succeeded"


class TestLroPollingErrors:
    """Test LRO polling with unexpected HTTP statuses."""

    def test_poll_unexpected_status(self):
        session = MagicMock()
        session.request.side_effect = [
            _resp(
                202,
                "",
                headers={
                    "Location": "https://api.fabric.microsoft.com/v1/operations/op-4"
                },
            ),
            _resp(500, "Internal Server Error"),
        ]
        client = _make_client(session)
        with pytest.raises(FabricError, match="500"):
            client.post("workspaces/ws-1/items")
