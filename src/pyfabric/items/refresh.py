"""Refresh a semantic model via the Power BI enhanced-refresh REST API.

Semantic-model refresh does not live on the Fabric API — it's the Power BI host
(``api.powerbi.com``) with a Power-BI-scoped token (:data:`PBI_RESOURCE`),
distinct from the Fabric token :class:`FabricClient` uses. So this takes a
:class:`FabricCredential` directly and issues its own requests, polling the
refresh status (``"Completed"`` / ``"Failed"`` / …) much like
:meth:`pyfabric.client.graph.GraphClient.refresh`.

In a Fabric workspace the workspace id doubles as the Power BI *group* id and the
semantic-model item id doubles as the *dataset* id.

Usage::

    from pyfabric.client.auth import FabricCredential
    from pyfabric.items.refresh import refresh_semantic_model

    refresh_semantic_model(
        FabricCredential(),
        ws_id,
        dataset_id,
        on_progress=lambda s: print("refresh:", s),
    )

The model's datasource credentials must be bound once in the portal before the
first refresh (the standard Lakehouse/SQL credential-binding step).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import requests
import structlog

from pyfabric.client.auth import PBI_RESOURCE, FabricCredential

log = structlog.get_logger()

OnProgress = Callable[[str], None] | None

_PBI_BASE = "https://api.powerbi.com/v1.0/myorg"
_IN_PROGRESS = ("Unknown", "NotStarted", "InProgress")
_FAILED = ("Failed", "Disabled", "Cancelled")


def refresh_semantic_model(
    credential: FabricCredential,
    workspace_id: str,
    dataset_id: str,
    *,
    refresh_type: str = "full",
    wait: bool = True,
    poll_interval: int = 15,
    timeout: int = 1800,
    on_progress: OnProgress = None,
) -> dict[str, Any]:
    """Trigger an enhanced refresh of a semantic model; wait unless ``wait=False``.

    Args:
        workspace_id:   Workspace id (= Power BI group id).
        dataset_id:     Semantic-model item id (= Power BI dataset id).
        refresh_type:   Power BI refresh type (``"full"``, ``"automatic"``, ...).
        wait:           Poll to completion (default). ``False`` returns
                        ``{"status": "Accepted", "requestId": ...}`` immediately.
        poll_interval:  Seconds between status polls.
        timeout:        Max seconds to wait before raising :class:`TimeoutError`.
        on_progress:    Optional ``callback(status)`` invoked on each poll.

    Returns the final refresh record. Raises :class:`RuntimeError` on a failed
    refresh and :class:`TimeoutError` if it doesn't finish within ``timeout``.
    """
    token = credential.get_token(PBI_RESOURCE)
    base = f"{_PBI_BASE}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    session = requests.Session()
    session.headers.update(
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )

    # A request body makes this an *enhanced* (async, trackable) refresh.
    resp = session.post(base, json={"type": refresh_type})
    if resp.status_code not in (200, 202):
        raise RuntimeError(
            f"semantic-model refresh POST failed: {resp.status_code} {resp.text}"
        )

    location = resp.headers.get("Location", "")
    request_id = resp.headers.get("RequestId") or (
        location.rstrip("/").rsplit("/", 1)[-1] if location else None
    )
    if not wait:
        return {"status": "Accepted", "requestId": request_id}

    deadline = time.monotonic() + timeout
    while True:
        time.sleep(poll_interval)
        if request_id:
            r = session.get(f"{base}/{request_id}")
            record = r.json() if r.text else {}
        else:
            r = session.get(base, params={"$top": 1})
            values = (r.json() if r.text else {}).get("value", [])
            record = values[0] if values else {}
        status = record.get("status", "Unknown")
        if on_progress:
            on_progress(status)
        log.debug("refresh_status", dataset_id=dataset_id, status=status)

        if status == "Completed":
            return record
        if status in _FAILED:
            detail = record.get("serviceExceptionJson") or record.get("error") or status
            raise RuntimeError(f"semantic-model refresh {status}: {detail}")
        if status not in _IN_PROGRESS:
            log.debug("refresh_status_unrecognized", status=status)
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"semantic-model refresh did not finish within {timeout}s "
                f"(last status {status!r})"
            )
