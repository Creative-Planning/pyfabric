"""Error path tests for auth module — provider fallbacks, login failures.

Each test verifies error messages provide enough context for diagnostics.
"""

from unittest.mock import MagicMock, patch

import pytest

from pyfabric.client.auth import AuthError, FabricCredential


class TestProviderFallback:
    def test_azure_identity_import_error_falls_back_to_cli(self):
        """When azure.identity is not installed, should fall back to az CLI."""
        with patch(
            "pyfabric.client.auth._AzureIdentityProvider",
            side_effect=ImportError("No module named 'azure.identity'"),
        ):
            cred = FabricCredential.__new__(FabricCredential)
            cred._tenant_id = None
            cred._cache = {}
            provider = cred._create_provider()
            # Should be _AzCliProvider
            assert provider.__class__.__name__ == "_AzCliProvider"

    def test_azure_identity_generic_error_falls_back_to_cli(self):
        """When azure.identity fails with any error, should fall back to az CLI."""
        with patch(
            "pyfabric.client.auth._AzureIdentityProvider",
            side_effect=RuntimeError("credential chain exhausted"),
        ):
            cred = FabricCredential.__new__(FabricCredential)
            cred._tenant_id = None
            cred._cache = {}
            provider = cred._create_provider()
            assert provider.__class__.__name__ == "_AzCliProvider"


class TestAzLoginErrors:
    def test_az_login_subprocess_failure(self):
        mock_result = MagicMock()
        mock_result.returncode = 1

        with patch("subprocess.run", return_value=mock_result):
            from pyfabric.client.auth import az_login

            with pytest.raises(AuthError, match="az login failed"):
                az_login("contoso")

    def test_az_cli_token_empty_stdout(self):
        """az CLI returns empty token — should raise with scope context."""
        from pyfabric.client.auth import _AzCliProvider

        provider = _AzCliProvider(None)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""

        with (
            patch("subprocess.run", return_value=mock_result),
            pytest.raises(
                AuthError,
                match=r"authentication failed for scope https://api\.fabric\.microsoft\.com",
            ),
        ):
            provider.get_token("https://api.fabric.microsoft.com/.default")

    def test_az_cli_token_error_includes_scope(self):
        """Error message should include the scope that was requested."""
        from pyfabric.client.auth import _AzCliProvider

        provider = _AzCliProvider(None)
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "AADSTS700082: refresh token has expired"

        with (
            patch("subprocess.run", return_value=mock_result),
            pytest.raises(
                AuthError,
                match=r"authentication failed for scope https://storage\.azure\.com",
            ) as exc_info,
        ):
            provider.get_token("https://storage.azure.com/.default")
        assert "refresh token has expired" in str(exc_info.value)


class TestGetCurrentAccountErrors:
    def test_not_logged_in_returns_empty(self):
        from pyfabric.client.auth import get_current_account

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch("subprocess.run", return_value=mock_result):
            result = get_current_account()
            assert result == {}
