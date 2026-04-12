"""Tests for auth module."""

import time
from unittest.mock import MagicMock, patch

from pyfabric.client.auth import (
    FabricCredential,
    _AzCliProvider,
    _normalize_scope,
    _resolve_tenant,
    _TokenResult,
)


class TestScopeNormalization:
    def test_resource_url_gets_default(self):
        assert (
            _normalize_scope("https://storage.azure.com")
            == "https://storage.azure.com/.default"
        )

    def test_already_has_default_unchanged(self):
        assert (
            _normalize_scope("https://storage.azure.com/.default")
            == "https://storage.azure.com/.default"
        )

    def test_trailing_slash_stripped(self):
        assert (
            _normalize_scope("https://api.fabric.microsoft.com/")
            == "https://api.fabric.microsoft.com/.default"
        )

    def test_non_url_scope_unchanged(self):
        assert _normalize_scope("user_impersonation") == "user_impersonation"

    def test_fabric_resource(self):
        assert (
            _normalize_scope("https://api.fabric.microsoft.com")
            == "https://api.fabric.microsoft.com/.default"
        )

    def test_database_resource(self):
        assert (
            _normalize_scope("https://database.windows.net")
            == "https://database.windows.net/.default"
        )


class TestTenantResolution:
    def test_none_returns_none(self):
        assert _resolve_tenant(None) is None

    def test_bare_name_appends_dot_com(self):
        assert _resolve_tenant("contoso") == "contoso.com"
        assert _resolve_tenant("fabrikam") == "fabrikam.com"

    def test_email_extracts_domain(self):
        assert _resolve_tenant("user@contoso.com") == "contoso.com"
        assert _resolve_tenant("admin@fabrikam.com") == "fabrikam.com"

    def test_domain_passthrough(self):
        assert _resolve_tenant("contoso.com") == "contoso.com"
        assert _resolve_tenant("contoso.onmicrosoft.com") == "contoso.onmicrosoft.com"

    def test_guid_passthrough(self):
        guid = "12345678-1234-1234-1234-123456789012"
        assert _resolve_tenant(guid) == guid


class TestFabricCredential:
    def test_caches_tokens(self):
        mock_provider = MagicMock()
        mock_provider.get_token.return_value = _TokenResult("tok1", time.time() + 3600)

        cred = FabricCredential.__new__(FabricCredential)
        cred._provider = mock_provider
        cred._cache = {}
        cred._tenant_id = None

        t1 = cred.get_token("https://api.fabric.microsoft.com")
        assert t1 == "tok1"
        assert mock_provider.get_token.call_count == 1

        t2 = cred.get_token("https://api.fabric.microsoft.com")
        assert t2 == "tok1"
        assert mock_provider.get_token.call_count == 1

    def test_refreshes_expired_token(self):
        mock_provider = MagicMock()
        mock_provider.get_token.side_effect = [
            _TokenResult("tok1", time.time() - 1),
            _TokenResult("tok2", time.time() + 3600),
        ]

        cred = FabricCredential.__new__(FabricCredential)
        cred._provider = mock_provider
        cred._cache = {}
        cred._tenant_id = None

        cred.get_token("https://storage.azure.com")
        t2 = cred.get_token("https://storage.azure.com")
        assert t2 == "tok2"
        assert mock_provider.get_token.call_count == 2

    def test_separate_caches_per_scope(self):
        mock_provider = MagicMock()
        mock_provider.get_token.side_effect = [
            _TokenResult("fabric-tok", time.time() + 3600),
            _TokenResult("storage-tok", time.time() + 3600),
        ]

        cred = FabricCredential.__new__(FabricCredential)
        cred._provider = mock_provider
        cred._cache = {}
        cred._tenant_id = None

        assert cred.fabric_token == "fabric-tok"
        assert cred.storage_token == "storage-tok"
        assert mock_provider.get_token.call_count == 2


class TestAzCliProvider:
    def test_success(self):
        provider = _AzCliProvider(None)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "fake-token-value\n"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            result = provider.get_token("https://storage.azure.com/.default")

        assert result.token == "fake-token-value"
        assert result.expires_on > time.time()
        cmd = mock_run.call_args[0][0]
        assert "--scope" in cmd
        assert "storage.azure.com/.default" in cmd

    def test_failure_raises(self):
        provider = _AzCliProvider(None)
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "not logged in"

        with patch("subprocess.run", return_value=mock_result):
            import pytest

            with pytest.raises(Exception, match="authentication failed"):
                provider.get_token("https://api.fabric.microsoft.com/.default")
