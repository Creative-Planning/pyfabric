"""
Credential management for Microsoft Fabric.

Resolves tokens through the platform credential chain:
  1. azure.identity.DefaultAzureCredential (managed identity, VS Code, az CLI, env vars)
  2. az CLI subprocess fallback (with --scope, not --resource)

Tokens are cached per-scope with TTL. Token values never appear in logs.

Usage:
    from pyfabric.client.auth import FabricCredential

    cred = FabricCredential()                  # uses DefaultAzureCredential chain
    cred = FabricCredential(tenant="contoso")  # target a specific tenant

    token = cred.get_token("https://api.fabric.microsoft.com")
    token = cred.fabric_token   # convenience
    token = cred.storage_token  # convenience
    token = cred.sql_token      # convenience

Backward-compatible free functions:
    from pyfabric.client.auth import get_token, get_current_account, FABRIC_RESOURCE
"""

import json
import logging
import subprocess
import time
from typing import Protocol

log = logging.getLogger(__name__)

# ── Scopes ───────────────────────────────────────────────────────────────────

FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
STORAGE_RESOURCE = "https://storage.azure.com"
SQL_RESOURCE = "https://database.windows.net"

# Token refresh before expiry (seconds)
_REFRESH_MARGIN = 300  # 5 minutes before expiry


# ── Scope normalization ──────────────────────────────────────────────────────


def _normalize_scope(resource_or_scope: str) -> str:
    """Normalize a resource URL to a scope string.

    'https://storage.azure.com'          -> 'https://storage.azure.com/.default'
    'https://storage.azure.com/.default' -> unchanged
    'user_impersonation'                 -> unchanged
    """
    if resource_or_scope.startswith("https://") and not resource_or_scope.endswith(
        "/.default"
    ):
        return resource_or_scope.rstrip("/") + "/.default"
    return resource_or_scope


def _resolve_tenant(tenant: str | None) -> str | None:
    """Resolve a tenant identifier to a value az CLI can use.

    Accepts any of:
      - Email address: "user@contoso.com" -> "contoso.com"
      - Domain: "contoso.com" -> passed through
      - Tenant GUID: "29bbcfd1-..." -> passed through
      - Bare name: "contoso" -> "contoso.com" (appends .com if no dots/dashes)

    Azure CLI's --tenant flag accepts domains, GUIDs, and *.onmicrosoft.com
    names natively, so no hardcoded lookup table is needed.
    """
    if tenant is None:
        return None
    # Extract domain from email
    if "@" in tenant:
        tenant = tenant.split("@")[1]
    # If it looks like a bare name (no dots, no dashes suggesting GUID),
    # assume it's a domain prefix and append .com
    if "." not in tenant and "-" not in tenant:
        tenant = f"{tenant}.com"
    return tenant


# ── Token provider protocol ──────────────────────────────────────────────────


class _TokenResult:
    __slots__ = ("expires_on", "token")

    def __init__(self, token: str, expires_on: float):
        self.token = token
        self.expires_on = expires_on


class _TokenProvider(Protocol):
    def get_token(self, scope: str) -> _TokenResult: ...


# ── azure.identity provider ──────────────────────────────────────────────────


class _AzureIdentityProvider:
    """Token provider backed by azure.identity.DefaultAzureCredential."""

    def __init__(self, tenant_id: str | None = None):
        from azure.identity import DefaultAzureCredential

        kwargs = {}
        if tenant_id:
            kwargs["additionally_allowed_tenants"] = [tenant_id]
            # Prefer the specified tenant
            kwargs["exclude_shared_token_cache_credential"] = True
        self._credential = DefaultAzureCredential(**kwargs)
        log.debug("Using azure.identity.DefaultAzureCredential")

    def get_token(self, scope: str) -> _TokenResult:
        result = self._credential.get_token(scope)
        return _TokenResult(result.token, result.expires_on)


# ── az CLI provider ──────────────────────────────────────────────────────────


class _AzCliProvider:
    """Token provider using az CLI subprocess with --scope."""

    def __init__(self, tenant_id: str | None = None):
        self._tenant_id = tenant_id
        log.debug("Using az CLI token provider (azure-identity not available)")

    def get_token(self, scope: str) -> _TokenResult:
        cmd = (
            f'az account get-access-token --scope "{scope}" --query accessToken -o tsv'
        )
        r = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        if r.returncode != 0 or not r.stdout.strip():
            msg = r.stderr.strip() if r.stderr else "No token returned"
            raise AuthError(
                f"az CLI authentication failed for scope {scope}: {msg}\n"
                "Run 'az login' to authenticate."
            )
        token = r.stdout.strip()
        # az CLI tokens typically expire in ~1 hour; use conservative TTL
        expires_on = time.time() + 3000  # ~50 minutes
        return _TokenResult(token, expires_on)


# ── Errors ───────────────────────────────────────────────────────────────────


class AuthError(Exception):
    """Raised when authentication fails."""


# ── FabricCredential ─────────────────────────────────────────────────────────


class FabricCredential:
    """
    Unified credential for all Fabric operations.

    Resolves tokens through the platform credential chain:
      1. azure.identity.DefaultAzureCredential (if installed)
      2. az CLI subprocess fallback

    Tokens are cached per-scope and refreshed before expiry.
    """

    def __init__(self, tenant: str | None = None):
        self._tenant_id = _resolve_tenant(tenant)
        self._provider = self._create_provider()
        self._cache: dict[str, _TokenResult] = {}

    def _create_provider(self) -> _TokenProvider:
        try:
            return _AzureIdentityProvider(self._tenant_id)
        except ImportError:
            return _AzCliProvider(self._tenant_id)
        except Exception as e:
            log.debug("azure.identity failed (%s), falling back to az CLI", e)
            return _AzCliProvider(self._tenant_id)

    def get_token(self, resource: str) -> str:
        """Get a bearer token for the given resource/scope.

        Resource URLs are automatically normalized to scope format:
            'https://storage.azure.com' -> 'https://storage.azure.com/.default'
        """
        scope = _normalize_scope(resource)
        cached = self._cache.get(scope)
        if cached and cached.expires_on > (time.time() + _REFRESH_MARGIN):
            return cached.token

        log.debug("Requesting token for scope: %s", scope)
        result = self._provider.get_token(scope)
        self._cache[scope] = result
        log.debug(
            "Token acquired (expires in %d seconds)",
            int(result.expires_on - time.time()),
        )
        return result.token

    @property
    def fabric_token(self) -> str:
        """Token for Fabric REST API."""
        return self.get_token(FABRIC_RESOURCE)

    @property
    def storage_token(self) -> str:
        """Token for OneLake DFS (storage.azure.com)."""
        return self.get_token(STORAGE_RESOURCE)

    @property
    def sql_token(self) -> str:
        """Token for SQL analytics endpoint (database.windows.net)."""
        return self.get_token(SQL_RESOURCE)

    def account_info(self) -> dict:
        """Return current az account info, or {} if unavailable."""
        return get_current_account()


# ── Free functions (backward compatibility) ──────────────────────────────────

_default_credential: FabricCredential | None = None


def _get_default() -> FabricCredential:
    global _default_credential
    if _default_credential is None:
        _default_credential = FabricCredential()
    return _default_credential


def get_token(resource: str = FABRIC_RESOURCE) -> str:
    """Get a token using the default credential chain.

    Backward-compatible with the original auth.py signature.
    """
    return _get_default().get_token(resource)


def get_current_account() -> dict:
    """Return the current az account show output as a dict, or {} if not logged in."""
    r = subprocess.run(
        "az account show",
        capture_output=True,
        text=True,
        shell=True,
    )
    if r.returncode == 0 and r.stdout.strip():
        return json.loads(r.stdout)
    return {}


def az_login(tenant: str | None = None) -> dict:
    """Launch an interactive browser login, optionally targeting a specific tenant.

    Accepts any tenant identifier: email, domain, bare name, or GUID.
    Returns the az account info dict after login.
    """
    resolved = _resolve_tenant(tenant)
    cmd = "az login --allow-no-subscriptions"
    if resolved:
        cmd += f" --tenant {resolved}"

    print("Opening browser for interactive login...")
    r = subprocess.run(cmd, shell=True)
    if r.returncode != 0:
        raise AuthError("az login failed. Check the browser window and try again.")

    acct = get_current_account()
    print(f"Logged in as: {acct.get('user', {}).get('name', '?')}")
    print(f"Tenant:       {acct.get('tenantId', '?')}")
    return acct


def ensure_logged_in(resource: str = FABRIC_RESOURCE, tenant: str | None = None) -> str:
    """Get a token, triggering interactive login if needed."""
    try:
        return get_token(resource)
    except AuthError:
        az_login(tenant)
        global _default_credential
        _default_credential = None  # reset after re-login
        return get_token(resource)
