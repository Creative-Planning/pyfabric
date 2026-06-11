"""Tests for the Jobs API helpers (run_on_demand / run_notebook)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyfabric.items.jobs import _param_type, run_notebook, run_on_demand


def _client(responses: list) -> MagicMock:
    """A mock FabricClient whose ``raw_request`` yields ``responses`` in order."""
    c = MagicMock()
    c._build_url = lambda path, params=None: f"https://api/{path}?{params}"
    c.raw_request.side_effect = responses
    return c


class TestRunOnDemand:
    def test_waits_for_completion_and_reports_progress(self, mock_http_response):
        mk = mock_http_response
        client = _client(
            [
                mk(
                    202,
                    headers={"Location": "https://api/inst/JID", "Retry-After": "0"},
                ),
                mk(200, {"status": "InProgress"}),
                mk(200, {"status": "Completed", "id": "JID"}),
            ]
        )
        seen: list[str] = []
        result = run_on_demand(
            client,
            "WS",
            "ITEM",
            "RunNotebook",
            poll_interval=0,
            on_progress=seen.append,
        )
        assert result["status"] == "Completed"
        assert seen == ["InProgress", "Completed"]

    def test_sync_200_returns_immediately(self, mock_http_response):
        client = _client([mock_http_response(200, {"status": "Completed"})])
        assert (
            run_on_demand(client, "WS", "ITEM", "RunNotebook")["status"] == "Completed"
        )

    def test_failed_raises_with_failure_reason(self, mock_http_response):
        mk = mock_http_response
        client = _client(
            [
                mk(202, headers={"Location": "L", "Retry-After": "0"}),
                mk(200, {"status": "Failed", "failureReason": {"message": "boom"}}),
            ]
        )
        with pytest.raises(RuntimeError, match="boom"):
            run_on_demand(client, "WS", "ITEM", "RunNotebook", poll_interval=0)

    def test_no_wait_returns_location(self, mock_http_response):
        client = _client([mock_http_response(202, headers={"Location": "LOC"})])
        out = run_on_demand(client, "WS", "ITEM", "RunNotebook", wait=False)
        assert out == {"status": "Accepted", "location": "LOC"}

    def test_202_without_location_raises(self, mock_http_response):
        client = _client([mock_http_response(202, headers={})])
        with pytest.raises(RuntimeError, match="no Location"):
            run_on_demand(client, "WS", "ITEM", "RunNotebook")


class TestRunNotebook:
    def test_injects_typed_parameters(self, mock_http_response):
        client = _client([mock_http_response(200, {"status": "Completed"})])
        run_notebook(
            client, "WS", "NB", parameters={"folder": "x", "n": 3, "flag": True}
        )
        method, _url, body = client.raw_request.call_args[0]
        assert method == "POST"
        params = body["executionData"]["parameters"]
        assert params["folder"] == {"value": "x", "type": "string"}
        assert params["n"] == {"value": 3, "type": "int"}
        assert params["flag"] == {"value": True, "type": "bool"}

    def test_no_parameters_sends_no_body(self, mock_http_response):
        client = _client([mock_http_response(200, {"status": "Completed"})])
        run_notebook(client, "WS", "NB")
        assert client.raw_request.call_args[0][2] is None

    def test_uses_run_notebook_job_type(self, mock_http_response):
        client = _client([mock_http_response(200, {"status": "Completed"})])
        run_notebook(client, "WS", "NB")
        url = client.raw_request.call_args[0][1]
        assert "RunNotebook" in url and "items/NB/jobs/instances" in url


def test_param_type():
    assert _param_type(True) == "bool"
    assert _param_type(3) == "int"
    assert _param_type(3.5) == "float"
    assert _param_type("x") == "string"
