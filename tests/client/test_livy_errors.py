"""Error path tests for LivyClient — session failures, statement errors.

Each test verifies that error messages contain diagnostic context
(status codes, error details) for root cause analysis.
"""

import json
from unittest.mock import MagicMock

import pytest

from pyfabric.client.livy import LivyClient


def _make_livy(session_url: str | None = None) -> LivyClient:
    cred = MagicMock()
    cred.fabric_token = "fake-token"
    client = LivyClient(cred, "ws-test-id", "lh-test-id")
    client._session = MagicMock()
    if session_url:
        client.session_id = 42
        client.session_url = session_url
    return client


def _resp(status: int, body: dict | None = None) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = json.dumps(body) if body else ""
    r.json.return_value = body or {}
    return r


class TestSessionCreationErrors:
    def test_create_session_http_failure(self):
        client = _make_livy()
        client._session.post.return_value = _resp(
            500, {"error": "capacity unavailable"}
        )
        with pytest.raises(RuntimeError, match="Failed to create session") as exc_info:
            client.create_session()
        assert "500" in str(exc_info.value)

    def test_create_session_forbidden(self):
        client = _make_livy()
        client._session.post.return_value = _resp(403, {"error": "access denied"})
        with pytest.raises(RuntimeError, match="Failed to create session"):
            client.create_session()


class TestSessionStateErrors:
    def test_session_enters_dead_state(self):
        client = _make_livy("https://api.fabric.microsoft.com/sessions/42")
        client._session.get.return_value = _resp(200, {"state": "dead"})
        with pytest.raises(RuntimeError, match="bad state") as exc_info:
            client._wait_for_session_idle()
        assert "dead" in str(exc_info.value)

    def test_session_enters_killed_state(self):
        client = _make_livy("https://api.fabric.microsoft.com/sessions/42")
        client._session.get.return_value = _resp(200, {"state": "killed"})
        with pytest.raises(RuntimeError, match="bad state") as exc_info:
            client._wait_for_session_idle()
        assert "killed" in str(exc_info.value)

    def test_session_enters_error_state(self):
        client = _make_livy("https://api.fabric.microsoft.com/sessions/42")
        client._session.get.return_value = _resp(200, {"state": "error"})
        with pytest.raises(RuntimeError, match="bad state"):
            client._wait_for_session_idle()


class TestExecuteErrors:
    def test_execute_without_session_raises(self):
        client = _make_livy()  # no session_url
        with pytest.raises(RuntimeError, match="No active session"):
            client.execute("print(1)")

    def test_statement_submission_fails(self):
        client = _make_livy("https://api.fabric.microsoft.com/sessions/42")
        client._session.post.return_value = _resp(500, {"error": "session overloaded"})
        with pytest.raises(
            RuntimeError, match="Failed to submit statement"
        ) as exc_info:
            client.execute("print(1)")
        assert "500" in str(exc_info.value)

    def test_statement_enters_error_state(self):
        client = _make_livy("https://api.fabric.microsoft.com/sessions/42")
        # submit succeeds
        submit_resp = _resp(200, {"id": 1, "state": "available", "output": {}})
        # but output says error
        submit_resp.json.return_value = {
            "id": 1,
            "state": "error",
            "output": {
                "status": "error",
                "ename": "AnalysisException",
                "evalue": "Table not found: customers",
            },
        }
        client._session.post.return_value = submit_resp
        with pytest.raises(RuntimeError, match="Statement failed"):
            client.execute("SELECT * FROM customers")

    def test_spark_output_error(self):
        client = _make_livy("https://api.fabric.microsoft.com/sessions/42")
        submit_resp = _resp(
            200,
            {
                "id": 1,
                "state": "available",
                "output": {
                    "status": "error",
                    "ename": "SparkException",
                    "evalue": "Job aborted: Task 0 in stage 1.0 failed",
                },
            },
        )
        client._session.post.return_value = submit_resp
        with pytest.raises(RuntimeError, match="Spark error") as exc_info:
            client.execute("spark.sql('SELECT * FROM huge_table')")
        assert "SparkException" in str(exc_info.value)
        assert "Job aborted" in str(exc_info.value)
