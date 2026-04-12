"""Tests for LivyClient."""

from unittest.mock import MagicMock

from pyfabric.client.livy import LivyClient


class TestLivyEscape:
    def test_escapes_quotes(self):
        assert LivyClient._escape('SELECT "col"') == 'SELECT \\"col\\"'

    def test_escapes_backslash(self):
        assert LivyClient._escape("path\\to") == "path\\\\to"

    def test_no_escape_needed(self):
        assert LivyClient._escape("SELECT 1") == "SELECT 1"


class TestLivyClientInit:
    def test_builds_base_url(self):
        cred = MagicMock()
        client = LivyClient(cred, "ws-1", "lh-1")
        assert "ws-1" in client.base_url
        assert "lh-1" in client.base_url
        assert client.session_id is None

    def test_context_manager_calls_lifecycle(self):
        cred = MagicMock()
        client = LivyClient(cred, "ws-1", "lh-1")
        # Mock the session methods
        client.create_session = MagicMock(return_value=42)
        client.close_session = MagicMock()
        with client as c:
            assert c is client
            client.create_session.assert_called_once()
        client.close_session.assert_called_once()
