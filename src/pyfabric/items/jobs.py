"""Run and schedule an item's jobs via the Fabric Jobs API.

Triggers a job for a workspace item — a notebook today (``jobType=RunNotebook``),
any item via :func:`run_on_demand` — and, by default, polls to completion.
Schedule CRUD lives here too: :func:`list_schedules`, :func:`get_schedule`,
:func:`create_schedule`, :func:`update_schedule`, :func:`delete_schedule`,
plus :func:`list_job_instances` for run history.

The Jobs API LRO reports terminal status as ``"Completed"`` / ``"Failed"`` /
``"Cancelled"`` (with a ``failureReason`` payload), not the standard
``"Succeeded"``. So, like :meth:`pyfabric.client.graph.GraphClient.refresh`, we
submit with :meth:`FabricClient.raw_request` and poll the job instance manually
rather than relying on ``post()`` / ``_poll_lro``. Pass ``on_progress`` to surface
live status during long runs.

Usage::

    from pyfabric.client.http import FabricClient
    from pyfabric.items.jobs import run_notebook

    client = FabricClient()
    run_notebook(
        client,
        ws_id,
        notebook_id,
        parameters={"folder": "2026-06"},
        on_progress=lambda s: print("status:", s),
    )
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any

import structlog

from pyfabric.client.http import FabricClient, FabricError

log = structlog.get_logger()

OnProgress = Callable[[str], None] | None

_TERMINAL = ("Completed", "Failed", "Cancelled")


def run_on_demand(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    job_type: str,
    *,
    execution_data: dict[str, Any] | None = None,
    wait: bool = True,
    poll_interval: int = 15,
    on_progress: OnProgress = None,
) -> dict[str, Any]:
    """Trigger an on-demand job for an item; wait for it unless ``wait=False``.

    Args:
        job_type:        Fabric job type, e.g. ``"RunNotebook"``.
        execution_data:  Optional body for the job (``executionData``), e.g.
                         notebook parameters. Sent as ``{"executionData": ...}``.
        wait:            Poll to completion (default). When ``False``, returns
                         immediately with ``{"status": "Accepted", "location": ...}``.
        poll_interval:   Seconds between polls (clamped against the server's
                         ``Retry-After``).
        on_progress:     Optional ``callback(status)`` invoked on each poll.

    Returns the final job-instance body. Raises :class:`RuntimeError` on a
    ``Failed`` / ``Cancelled`` outcome (message taken from ``failureReason``).
    """
    url = client._build_url(
        f"workspaces/{workspace_id}/items/{item_id}/jobs/instances",
        params={"jobType": job_type},
    )
    body = {"executionData": execution_data} if execution_data else None
    resp = client.raw_request("POST", url, body)

    if resp.status_code in (200, 201):
        if on_progress:
            on_progress("Completed")
        return resp.json() if resp.text else {"status": "Completed"}

    if resp.status_code != 202:
        raise FabricError(resp.status_code, resp.text, url)

    location = resp.headers.get("Location")
    if not location:
        raise RuntimeError(f"202 from {url} has no Location header")
    retry_after = int(resp.headers.get("Retry-After", poll_interval))
    if not wait:
        return {"status": "Accepted", "location": location}

    while True:
        time.sleep(retry_after)
        poll = client.raw_request("GET", location)
        instance = poll.json() if poll.text else {}
        status = instance.get("status", "Unknown")
        if on_progress:
            on_progress(status)
        log.debug("job_status", item_id=item_id, job_type=job_type, status=status)

        if status in _TERMINAL:
            if status == "Completed" and not instance.get("failureReason"):
                return instance
            reason = instance.get("failureReason") or {}
            msg = reason.get("message") or str(reason) if reason else status
            raise RuntimeError(f"Job {status}: {msg}")
        retry_after = min(retry_after, poll_interval)


def _param_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def run_notebook(
    client: FabricClient,
    workspace_id: str,
    notebook_id: str,
    *,
    parameters: Mapping[str, Any] | None = None,
    wait: bool = True,
    poll_interval: int = 15,
    on_progress: OnProgress = None,
) -> dict[str, Any]:
    """Trigger an on-demand notebook run (``jobType=RunNotebook``).

    ``parameters`` (``{name: value}``) are injected into the notebook's
    parameters cell via the Jobs API ``executionData.parameters`` — the
    injection target ``NotebookBuilder.add_parameters_cell`` emits a cell for.
    Each value's Fabric param ``type`` is inferred (bool/int/float/string).
    """
    execution_data: dict[str, Any] | None = None
    if parameters:
        execution_data = {
            "parameters": {
                name: {"value": value, "type": _param_type(value)}
                for name, value in parameters.items()
            }
        }
    return run_on_demand(
        client,
        workspace_id,
        notebook_id,
        "RunNotebook",
        execution_data=execution_data,
        wait=wait,
        poll_interval=poll_interval,
        on_progress=on_progress,
    )


# ── Schedules ────────────────────────────────────────────────────────────────
#
# The schedules surface accepts DIFFERENT jobType strings than the
# job-instances surface for some item types. Verified against the live API:
#
#   item type              instances report          schedules endpoint accepts
#   ─────────────────      ───────────────────────   ──────────────────────────
#   Notebook               RunNotebook               RunNotebook  (verified 2026-07-03)
#   DataPipeline           Execute                   Execute
#   materialized lake      MaterializedLakeViews     RefreshMaterializedLakeViews
#   view refresh                                     (instances string → 400 InvalidJobType)
#
# Querying schedules with the jobType string copied from a job-instances
# response is therefore NOT always valid — the MLV asymmetry made a failing
# hourly schedule invisible for a day. All schedule helpers below normalize
# through this alias map so either spelling works.

_SCHEDULE_JOB_TYPE_ALIASES = {
    "MaterializedLakeViews": "RefreshMaterializedLakeViews",
}


def _schedule_job_type(job_type: str) -> str:
    """Normalize a jobType to the spelling the *schedules* endpoints accept."""
    return _SCHEDULE_JOB_TYPE_ALIASES.get(job_type, job_type)


def _schedules_path(workspace_id: str, item_id: str, job_type: str) -> str:
    jt = _schedule_job_type(job_type)
    return f"workspaces/{workspace_id}/items/{item_id}/jobs/{jt}/schedules"


def list_schedules(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    job_type: str,
) -> list[dict[str, Any]]:
    """List an item's schedules for one job type.

    ``job_type`` accepts either the schedules-surface spelling or the
    job-instances spelling (e.g. ``"MaterializedLakeViews"``) — instance
    spellings are normalized via the alias map before the call, so the
    ``InvalidJobType`` 400 trap can't bite.
    """
    return client.get_paged(_schedules_path(workspace_id, item_id, job_type))


def get_schedule(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    job_type: str,
    schedule_id: str,
) -> dict[str, Any]:
    """Fetch a single schedule by id (jobType normalized like ``list_schedules``)."""
    return client.get(
        f"{_schedules_path(workspace_id, item_id, job_type)}/{schedule_id}"
    )


def create_schedule(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    job_type: str,
    *,
    configuration: dict[str, Any],
    enabled: bool = True,
) -> dict[str, Any]:
    """Create a schedule for an item's job type.

    ``configuration`` is the REST API's ``ScheduleConfig`` shape, passed
    through as-is. A daily schedule, for example::

        create_schedule(
            client,
            ws_id,
            item_id,
            "RefreshMaterializedLakeViews",
            configuration={
                "type": "Daily",
                "times": ["07:00"],
                "localTimeZoneId": "Central Standard Time",
                "startDateTime": "2026-07-01T00:00:00",
                "endDateTime": "2036-07-01T00:00:00",
            },
        )

    and a fixed-interval one uses ``{"type": "Cron",
    "interval": <minutes>, ...}`` with the same timezone/start/end keys.

    ``job_type`` accepts either surface's spelling (see the alias map above).
    """
    body = {"enabled": enabled, "configuration": configuration}
    log.info(
        "schedule.create",
        item_id=item_id,
        job_type=_schedule_job_type(job_type),
        enabled=enabled,
    )
    return client.post(_schedules_path(workspace_id, item_id, job_type), body)


def update_schedule(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    job_type: str,
    schedule_id: str,
    *,
    configuration: dict[str, Any],
    enabled: bool = True,
) -> dict[str, Any]:
    """Update an existing schedule (full replacement, like the REST API).

    The Fabric API replaces the schedule definition rather than merging, so
    ``configuration`` must be the complete ``ScheduleConfig`` — fetch the
    current one with :func:`get_schedule` and modify it if you only want to
    flip ``enabled``.
    """
    body = {"enabled": enabled, "configuration": configuration}
    log.info(
        "schedule.update",
        item_id=item_id,
        job_type=_schedule_job_type(job_type),
        schedule_id=schedule_id,
        enabled=enabled,
    )
    return client.patch(
        f"{_schedules_path(workspace_id, item_id, job_type)}/{schedule_id}", body
    )


def delete_schedule(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    job_type: str,
    schedule_id: str,
) -> None:
    """Delete a schedule (jobType normalized like ``list_schedules``)."""
    log.info(
        "schedule.delete",
        item_id=item_id,
        job_type=_schedule_job_type(job_type),
        schedule_id=schedule_id,
    )
    client.delete(f"{_schedules_path(workspace_id, item_id, job_type)}/{schedule_id}")


def list_job_instances(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    *,
    job_type: str | None = None,
) -> list[dict[str, Any]]:
    """List an item's job instances (run history), newest first.

    ``job_type`` optionally filters client-side. The filter matches both
    jobType spellings — instances report ``"MaterializedLakeViews"`` while
    the schedules surface uses ``"RefreshMaterializedLakeViews"``, so
    filtering with either string finds the same runs.
    """
    instances = client.get_paged(
        f"workspaces/{workspace_id}/items/{item_id}/jobs/instances"
    )
    if job_type is None:
        return instances
    wanted = _schedule_job_type(job_type)
    return [i for i in instances if _schedule_job_type(i.get("jobType", "")) == wanted]
