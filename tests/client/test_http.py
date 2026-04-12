"""Tests for FabricClient HTTP operations."""

from unittest.mock import MagicMock, patch

import pytest

from pyfabric.client.http import FabricClient, FabricError, _build_url


class TestBuildUrl:
    def test_relative_path(self):
        url = _build_url("workspaces/ws-1")
        assert url == "https://api.fabric.microsoft.com/v1/workspaces/ws-1"

    def test_absolute_url_passthrough(self):
        url = _build_url("https://example.com/api/resource")
        assert url == "https://example.com/api/resource"

    def test_with_params(self):
        url = _build_url("workspaces", {"type": "Lakehouse"})
        assert "type=Lakehouse" in url

    def test_strips_leading_slash(self):
        url = _build_url("/workspaces/ws-1")
        assert "//workspaces" not in url


class TestFabricError:
    def test_parses_json_error(self):
        body = '{"error": {"message": "Not found"}}'
        err = FabricError(404, body, "https://api.fabric.microsoft.com/v1/test")
        assert "Not found" in str(err)
        assert err.status == 404

    def test_handles_non_json_body(self):
        err = FabricError(500, "Internal Server Error", "https://example.com")
        assert "Internal Server Error" in str(err)


class TestFabricClientConstructor:
    def test_none_creates_default_credential(self):
        # When None is passed, FabricClient creates a FabricCredential internally.
        # We can't easily patch it due to isinstance checks, so just verify
        # the credential is set and token is not.
        with patch("pyfabric.client.auth._AzureIdentityProvider"):
            client = FabricClient(None)
            assert client._credential is not None
            assert client._static_token is None

    def test_string_token(self):
        client = FabricClient("my-static-token")
        assert client._static_token == "my-static-token"
        assert client._credential is None

    def test_credential_object(self):
        from pyfabric.client.auth import FabricCredential as RealCred

        mock_cred = MagicMock(spec=RealCred)
        client = FabricClient(mock_cred)
        assert client._credential is mock_cred
        assert client._static_token is None


class TestFabricClientRequests:
    def _make_client(self, mock_session):
        client = FabricClient("fake-token")
        client._session = mock_session
        return client

    def test_get(self, mock_requests_session):
        mock_requests_session.request.return_value.status_code = 200
        mock_requests_session.request.return_value.json.return_value = {"id": "item-1"}
        mock_requests_session.request.return_value.text = '{"id": "item-1"}'
        mock_requests_session.request.return_value.content = b'{"id": "item-1"}'
        client = self._make_client(mock_requests_session)
        result = client.get("workspaces/ws-1/items/item-1")
        assert result["id"] == "item-1"

    def test_get_paged_single_page(self, mock_requests_session):
        mock_requests_session.request.return_value.status_code = 200
        mock_requests_session.request.return_value.json.return_value = {
            "value": [{"id": "1"}, {"id": "2"}],
        }
        mock_requests_session.request.return_value.text = "data"
        mock_requests_session.request.return_value.content = b"data"
        client = self._make_client(mock_requests_session)
        result = client.get_paged("workspaces")
        assert len(result) == 2

    def test_get_paged_with_continuation(self, mock_requests_session):
        page1 = MagicMock()
        page1.status_code = 200
        page1.json.return_value = {"value": [{"id": "1"}], "continuationToken": "tok"}
        page1.text = "data"
        page1.content = b"data"

        page2 = MagicMock()
        page2.status_code = 200
        page2.json.return_value = {"value": [{"id": "2"}]}
        page2.text = "data"
        page2.content = b"data"

        mock_requests_session.request.side_effect = [page1, page2]
        client = self._make_client(mock_requests_session)
        result = client.get_paged("workspaces")
        assert len(result) == 2

    def test_request_raises_fabric_error_on_4xx(self, mock_requests_session):
        mock_requests_session.request.return_value.status_code = 404
        mock_requests_session.request.return_value.text = (
            '{"error": {"message": "Not found"}}'
        )
        mock_requests_session.request.return_value.content = b"err"
        client = self._make_client(mock_requests_session)
        with pytest.raises(FabricError):
            client.get("workspaces/bad-id")

    def test_delete(self, mock_requests_session):
        mock_requests_session.request.return_value.status_code = 200
        mock_requests_session.request.return_value.content = b""
        client = self._make_client(mock_requests_session)
        client.delete("workspaces/ws-1")  # should not raise

    def test_post_sync_200(self, mock_requests_session):
        mock_requests_session.request.return_value.status_code = 200
        mock_requests_session.request.return_value.json.return_value = {"id": "new"}
        mock_requests_session.request.return_value.text = '{"id": "new"}'
        mock_requests_session.request.return_value.content = b'{"id": "new"}'
        client = self._make_client(mock_requests_session)
        result = client.post("workspaces", {"displayName": "test"})
        assert result["id"] == "new"
