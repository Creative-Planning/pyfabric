"""Tests for refresh_semantic_model (Power BI enhanced refresh)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyfabric.client.http import FabricError
from pyfabric.items.refresh import (
    refresh_lakehouse_sql_metadata,
    refresh_semantic_model,
    refresh_sql_endpoint_metadata,
)


def _session(post_resp, get_responses) -> MagicMock:
    s = MagicMock()
    s.post.return_value = post_resp
    s.get.side_effect = get_responses
    return s


def _patch_session(monkeypatch, session: MagicMock) -> None:
    monkeypatch.setattr("pyfabric.items.refresh.requests.Session", lambda: session)


class TestRefreshSemanticModel:
    def test_waits_for_completion_and_reports_progress(
        self, monkeypatch, mock_credential, mock_http_response
    ):
        mk = mock_http_response
        session = _session(
            mk(202, headers={"RequestId": "RID"}),
            [mk(200, {"status": "Unknown"}), mk(200, {"status": "Completed"})],
        )
        _patch_session(monkeypatch, session)
        seen: list[str] = []
        out = refresh_semantic_model(
            mock_credential, "WS", "DS", poll_interval=0, on_progress=seen.append
        )
        assert out["status"] == "Completed"
        assert seen == ["Unknown", "Completed"]
        # polled the specific request id
        assert "RID" in session.get.call_args[0][0]

    def test_failed_raises(self, monkeypatch, mock_credential, mock_http_response):
        mk = mock_http_response
        session = _session(
            mk(202, headers={"RequestId": "RID"}),
            [mk(200, {"status": "Failed", "serviceExceptionJson": "err"})],
        )
        _patch_session(monkeypatch, session)
        with pytest.raises(RuntimeError, match="Failed"):
            refresh_semantic_model(mock_credential, "WS", "DS", poll_interval=0)

    def test_no_wait_returns_request_id(
        self, monkeypatch, mock_credential, mock_http_response
    ):
        session = _session(mock_http_response(202, headers={"RequestId": "RID"}), [])
        _patch_session(monkeypatch, session)
        out = refresh_semantic_model(mock_credential, "WS", "DS", wait=False)
        assert out == {"status": "Accepted", "requestId": "RID"}

    def test_post_failure_raises(
        self, monkeypatch, mock_credential, mock_http_response
    ):
        session = _session(mock_http_response(400, text="bad request"), [])
        _patch_session(monkeypatch, session)
        with pytest.raises(RuntimeError, match="refresh POST failed"):
            refresh_semantic_model(mock_credential, "WS", "DS")

    def test_request_id_falls_back_to_location(
        self, monkeypatch, mock_credential, mock_http_response
    ):
        mk = mock_http_response
        session = _session(
            mk(202, headers={"Location": "https://api/.../refreshes/FROMLOC"}),
            [mk(200, {"status": "Completed"})],
        )
        _patch_session(monkeypatch, session)
        refresh_semantic_model(mock_credential, "WS", "DS", poll_interval=0)
        assert "FROMLOC" in session.get.call_args[0][0]

    def test_uses_powerbi_token(self, monkeypatch, mock_credential, mock_http_response):
        session = _session(
            mock_http_response(202, headers={"RequestId": "RID"}),
            [mock_http_response(200, {"status": "Completed"})],
        )
        _patch_session(monkeypatch, session)
        refresh_semantic_model(mock_credential, "WS", "DS", poll_interval=0)
        # Authorization header set from the credential's Power BI token
        auth = session.headers.update.call_args[0][0]["Authorization"]
        assert auth.startswith("Bearer ")


def _sql_client(raw_responses=None, get_return=None) -> MagicMock:
    """A FabricClient mock for the SQL-endpoint metadata helpers."""
    c = MagicMock()
    c._build_url = lambda path, params=None: (
        f"https://api.fabric.microsoft.com/v1/{path}"
    )
    if raw_responses is not None:
        c.raw_request.side_effect = raw_responses
    if get_return is not None:
        c.get.return_value = get_return
    return c


class TestRefreshSqlEndpointMetadata:
    def test_sync_200_returns_results(self, mock_http_response):
        mk = mock_http_response
        c = _sql_client(
            [mk(200, {"value": [{"tableName": "dbo.t", "status": "Success"}]})]
        )
        seen: list[str] = []
        out = refresh_sql_endpoint_metadata(c, "WS", "EP", on_progress=seen.append)
        assert out["value"][0]["status"] == "Success"
        assert seen == ["Completed"]
        method, url, _body = c.raw_request.call_args[0]
        assert method == "POST"
        assert "sqlEndpoints/EP/refreshMetadata" in url

    def test_202_polls_to_succeeded(self, mock_http_response):
        mk = mock_http_response
        c = _sql_client(
            [
                mk(202, headers={"Location": "https://poll/op", "Retry-After": "0"}),
                mk(200, {"status": "Running"}),
                mk(200, {"status": "Succeeded", "value": []}),
            ]
        )
        seen: list[str] = []
        out = refresh_sql_endpoint_metadata(
            c, "WS", "EP", poll_interval=0, on_progress=seen.append
        )
        assert out["status"] == "Succeeded"
        assert seen == ["Running", "Succeeded"]

    def test_no_wait_returns_location(self, mock_http_response):
        c = _sql_client(
            [mock_http_response(202, headers={"Location": "https://poll/op"})]
        )
        out = refresh_sql_endpoint_metadata(c, "WS", "EP", wait=False)
        assert out == {"status": "Accepted", "location": "https://poll/op"}

    def test_failed_status_raises(self, mock_http_response):
        mk = mock_http_response
        c = _sql_client(
            [
                mk(202, headers={"Location": "https://poll/op", "Retry-After": "0"}),
                mk(200, {"status": "Failed", "error": "boom"}),
            ]
        )
        with pytest.raises(RuntimeError, match="Failed"):
            refresh_sql_endpoint_metadata(c, "WS", "EP", poll_interval=0)

    def test_unexpected_status_raises_fabric_error(self, mock_http_response):
        c = _sql_client([mock_http_response(400, {"error": "bad"})])
        with pytest.raises(FabricError):
            refresh_sql_endpoint_metadata(c, "WS", "EP")


class TestRefreshLakehouseSqlMetadata:
    def test_resolves_endpoint_then_refreshes(self, mock_http_response):
        c = _sql_client(
            [mock_http_response(200, {"value": []})],
            get_return={"properties": {"sqlEndpointProperties": {"id": "EPID"}}},
        )
        refresh_lakehouse_sql_metadata(c, "WS", "LH")
        _method, url, _body = c.raw_request.call_args[0]
        assert "sqlEndpoints/EPID/refreshMetadata" in url

    def test_missing_endpoint_id_raises(self):
        c = MagicMock()
        c.get.return_value = {"properties": {}}
        with pytest.raises(RuntimeError, match="no SQL endpoint"):
            refresh_lakehouse_sql_metadata(c, "WS", "LH")
