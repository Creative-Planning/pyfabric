"""
HTTP client for the Microsoft Fabric REST API v1.

Features:
  - Token refresh via FabricCredential (azure.identity or az CLI chain)
  - requests.Session for connection reuse
  - Automatic pagination via continuationToken
  - Long-Running Operation (LRO) polling for 202 responses
  - Retry with backoff on 429 (rate limiting)
  - Structured logging (tokens masked)

Usage:
    from pyfabric.client.auth import FabricCredential
    from pyfabric.client.http import FabricClient

    client = FabricClient()                            # default credential
    client = FabricClient(FabricCredential("contoso")) # specific tenant
    client = FabricClient("eyJ...")                     # static token (backward compat)
"""

import json
import time
import urllib.parse
from typing import Any

import requests
import structlog
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .auth import FABRIC_RESOURCE, FabricCredential

log = structlog.get_logger()

BASE_URL = "https://api.fabric.microsoft.com/v1"
_DEFAULT_TIMEOUT = 30  # seconds


# ── Errors ───────────────────────────────────────────────────────────────────


class FabricError(Exception):
    """Raised when the Fabric API returns an error response."""

    def __init__(self, status: int, body: str, url: str):
        self.status = status
        self.body = body
        self.url = url
        try:
            detail = json.loads(body)
            msg = detail.get("error", {}).get("message", body)
        except Exception:
            msg = body[:500]
        super().__init__(f"HTTP {status} from {url}: {msg}")


# ── Client ───────────────────────────────────────────────────────────────────


class FabricClient:
    """
    Thin client for the Fabric REST API.

    Accepts a FabricCredential (preferred), a static token string (backward
    compat), or None (creates default FabricCredential).
    """

    def __init__(self, credential: FabricCredential | str | None = None):
        if isinstance(credential, str):
            self._credential = None
            self._static_token = credential
        elif isinstance(credential, FabricCredential):
            self._credential = credential
            self._static_token = None
        else:
            self._credential = FabricCredential()
            self._static_token = None

        self._session = self._create_session()

    @staticmethod
    def _create_session() -> requests.Session:
        s = requests.Session()
        # Retry on 429 (rate limited) and 503 (service unavailable)
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 503],
            allowed_methods=["GET", "POST", "PATCH", "DELETE"],
            respect_retry_after_header=True,
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        return s

    def _get_token(self) -> str:
        if self._static_token:
            return self._static_token
        return self._credential.get_token(FABRIC_RESOURCE)

    def _headers(self, extra: dict | None = None) -> dict:
        h = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }
        if extra:
            h.update(extra)
        return h

    # ------------------------------------------------------------------
    # Low-level request
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        body: Any = None,
        **kwargs,
    ) -> requests.Response:
        """Send a single HTTP request. Raises FabricError on 4xx/5xx."""
        data = json.dumps(body) if body is not None else None
        log.debug("%s %s", method, url)
        resp = self._session.request(
            method,
            url,
            headers=self._headers(),
            data=data,
            timeout=kwargs.get("timeout", _DEFAULT_TIMEOUT),
        )
        log.debug("  -> %d (%d bytes)", resp.status_code, len(resp.content))
        if resp.status_code >= 400:
            raise FabricError(resp.status_code, resp.text, url)
        return resp

    # ------------------------------------------------------------------
    # LRO polling
    # ------------------------------------------------------------------

    def _poll_lro(self, location: str, poll_interval: float = 2.0) -> dict:
        """Poll a Long-Running Operation until it completes."""
        while True:
            resp = self._request("GET", location)
            if resp.status_code == 200:
                body = resp.json() if resp.text else {}
                status = body.get("status", "")
                if status in ("Succeeded", ""):
                    return body
                if status in ("Failed", "Cancelled"):
                    raise FabricError(resp.status_code, resp.text, location)
                # Still running
                retry_after = float(resp.headers.get("Retry-After", poll_interval))
                log.debug("  LRO status=%s, retry in %.0fs", status, retry_after)
                time.sleep(retry_after)
                continue
            if resp.status_code == 202:
                retry_after = float(resp.headers.get("Retry-After", poll_interval))
                time.sleep(retry_after)
                continue
            raise FabricError(resp.status_code, resp.text, location)

    def _submit_and_poll(self, method: str, url: str, body: Any = None) -> dict:
        """Submit a request that may return 200 (sync) or 202 (async/LRO)."""
        resp = self._request(method, url, body)

        if resp.status_code in (200, 201):
            return resp.json() if resp.text else {}

        if resp.status_code == 202:
            location = resp.headers.get("Location")
            if not location:
                raise RuntimeError(f"202 from {url} has no Location header")
            log.debug("  LRO started, polling %s", location)
            result = self._poll_lro(location)
            # Some LROs need a /result sub-path
            if not result or "id" not in result:
                result_url = location.rstrip("/") + "/result"
                try:
                    r2 = self._request("GET", result_url)
                    if r2.text:
                        result = r2.json()
                except FabricError as e:
                    log.debug("LRO /result fetch failed (optional): %s", e)
            return result

        raise FabricError(resp.status_code, resp.text, url)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, path: str, params: dict | None = None) -> dict:
        """GET a single resource."""
        resp = self._request("GET", _build_url(path, params))
        return resp.json() if resp.text else {}

    def get_paged(self, path: str, params: dict | None = None) -> list:
        """GET all pages of a paginated collection."""
        results = []
        url = _build_url(path, params)
        while url:
            resp = self._request("GET", url)
            data = resp.json() if resp.text else {}
            results.extend(data.get("value", []))
            cont_uri = data.get("continuationUri")
            cont_token = data.get("continuationToken")
            if cont_uri:
                url = cont_uri
            elif cont_token:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}continuationToken={urllib.parse.quote(cont_token)}"
            else:
                url = None
        return results

    def post(self, path: str, body: Any = None) -> dict:
        """POST; handles sync (200/201) and async (202/LRO) responses."""
        return self._submit_and_poll("POST", _build_url(path), body)

    def patch(self, path: str, body: Any) -> dict:
        """PATCH; handles sync and async responses."""
        return self._submit_and_poll("PATCH", _build_url(path), body)

    def delete(self, path: str) -> None:
        """DELETE a resource."""
        self._request("DELETE", _build_url(path))


# ── Helpers ──────────────────────────────────────────────────────────────────


def _build_url(path: str, params: dict | None = None) -> str:
    url = path if path.startswith("http") else f"{BASE_URL}/{path.lstrip('/')}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    return url
