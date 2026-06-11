"""Tests for refresh_semantic_model (Power BI enhanced refresh)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyfabric.items.refresh import refresh_semantic_model


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
