"""Build and operate Microsoft Fabric Open Mirroring (Mirrored Database) items.

Open Mirroring is a Fabric pattern where a Mirrored Database item operates
in **GenericMirror** mode: producers push parquet files into a landing
zone (``Files/LandingZone/...``) and Fabric replicates each file as
rows in the matching Delta table under ``Tables/...``. The mirror item
itself owns the lifecycle (start / stop / get status); the data plane
is a separate concern handled by :mod:`pyfabric.data.open_mirror`
(landing-zone uploads).

This module covers the **item plane**:

- :class:`MirroredDatabaseBuilder` — emit a canonical git-sync
  ``MirroredDatabase`` artifact (``.platform`` + ``mirroring.json``)
  with a startable ``GenericMirror`` source + ``MountedRelationalDatabase``
  Delta target. Mirrors :class:`pyfabric.items.notebook.NotebookBuilder`
  shape (``to_bundle``, ``save_to_disk``).
- REST lifecycle helpers — :func:`create_mirrored_database`,
  :func:`start_mirroring`, :func:`stop_mirroring`,
  :func:`get_mirroring_status`, :func:`get_tables_mirroring_status`,
  :func:`wait_for_running`. Each takes a ``FabricClient``-like object
  (anything with ``post`` / ``get`` methods) so callers compose with
  the existing client surface and tests can stub it cheaply.

Usage::

    from pyfabric.client.http import FabricClient
    from pyfabric.items.mirrored_database import (
        MirroredDatabaseBuilder,
        create_mirrored_database,
        start_mirroring,
        wait_for_running,
    )

    # Build the artifact and ship via git-sync, OR push via REST:
    builder = MirroredDatabaseBuilder(default_schema="dbo")
    builder.save_to_disk("definitions/", display_name="open_bronze")

    # Or create directly via REST:
    client = FabricClient()
    item = create_mirrored_database(client, ws_id, display_name="open_bronze")
    start_mirroring(client, ws_id, item["id"])
    status = wait_for_running(client, ws_id, item["id"], timeout_s=120)

Credit
------

The Open Mirroring landing-zone protocol details — including the
"GenericMirror" + "MountedRelationalDatabase" definition shape required
for a startable open mirror — were first published as research notes by
the *Learn Microsoft Fabric* community at
https://github.com/UnifiedEducation/research/tree/main/open-mirroring.
That repo currently ships without a license, so this implementation is
**clean-room**: written from the publicly-documented Microsoft Fabric
protocol, not by copying any code. The research repo is the recommended
companion read for the *why* behind each shape.
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any, Protocol

import structlog

from pyfabric.items.bundle import ArtifactBundle
from pyfabric.items.normalize import write_artifact_file

log = structlog.get_logger()


# ── Item builder ─────────────────────────────────────────────────────────────


class MirroredDatabaseBuilder:
    """Build a startable Open Mirroring ``MirroredDatabase`` item.

    The output is a single ``mirroring.json`` paired with a ``.platform``
    file. Both are written in canonical Fabric bytes (LF, no trailing
    newline) so git-sync doesn't flap them on the next pull.

    Args:
        default_schema: Name of the destination schema in the mirrored
            database. Maps to ``properties.target.typeProperties.defaultSchema``.
            Defaults to ``"dbo"`` (the Fabric portal default).

    Raises:
        ValueError: If ``default_schema`` is empty / whitespace.
    """

    def __init__(self, default_schema: str = "dbo") -> None:
        if not default_schema or not default_schema.strip():
            raise ValueError(
                f"default_schema must be a non-empty string; got {default_schema!r}"
            )
        self.default_schema = default_schema

    def to_mirroring_json(self) -> str:
        """Render the ``mirroring.json`` content as a string.

        Output is ``json.dumps(..., indent=2)`` with no trailing newline —
        matches Fabric's canonical bytes for this file.
        """
        body = {
            "properties": {
                "source": {
                    "type": "GenericMirror",
                    "typeProperties": {},
                },
                "target": {
                    "type": "MountedRelationalDatabase",
                    "typeProperties": {
                        "defaultSchema": self.default_schema,
                        "format": "Delta",
                    },
                },
            }
        }
        return json.dumps(body, indent=2)

    def to_bundle(
        self,
        display_name: str,
        *,
        logical_id: str | None = None,
        description: str = "",
    ) -> ArtifactBundle:
        """Bundle the mirror artifact for disk save or REST upload.

        ``mirroring.json`` is stored as **canonical bytes** (UTF-8, LF,
        no trailing newline) so :func:`pyfabric.items.bundle.save_to_disk`
        takes the ``write_bytes`` branch — preserves bytes on Windows
        where ``write_text`` would inject CRLF.
        """
        canonical = self.to_mirroring_json().encode("utf-8")
        kwargs: dict[str, Any] = {
            "item_type": "MirroredDatabase",
            "display_name": display_name,
            "parts": {"mirroring.json": canonical},
            "description": description,
        }
        if logical_id is not None:
            kwargs["logical_id"] = logical_id
        return ArtifactBundle(**kwargs)

    def save_to_disk(
        self,
        output_dir: str | Path,
        *,
        display_name: str,
        logical_id: str | None = None,
        description: str = "",
    ) -> Path:
        """Write the artifact to disk in canonical Fabric bytes.

        Routes both ``.platform`` and ``mirroring.json`` through
        :func:`pyfabric.items.normalize.write_artifact_file`, so output
        is identical across Linux / macOS / Windows.

        Returns the artifact directory path.
        """
        bundle = self.to_bundle(
            display_name=display_name,
            logical_id=logical_id,
            description=description,
        )
        artifact_dir = Path(output_dir) / bundle.dir_name
        artifact_dir.mkdir(parents=True, exist_ok=True)

        write_artifact_file(artifact_dir / ".platform", bundle.platform_json())
        write_artifact_file(artifact_dir / "mirroring.json", self.to_mirroring_json())
        log.info(
            "mirrored_database_saved",
            display_name=display_name,
            default_schema=self.default_schema,
            path=str(artifact_dir),
        )
        return artifact_dir


# ── REST lifecycle ───────────────────────────────────────────────────────────


class _ClientLike(Protocol):
    """Subset of ``pyfabric.client.http.FabricClient`` the lifecycle helpers
    need. Declared as a Protocol so tests can pass a tiny stub without
    pulling in the whole client + credential machinery."""

    def post(self, path: str, body: Any = None) -> dict[str, Any]: ...

    def get(
        self, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]: ...


def _items_path(workspace_id: str) -> str:
    return f"workspaces/{workspace_id}/mirroredDatabases"


def _item_path(workspace_id: str, mirror_id: str) -> str:
    return f"{_items_path(workspace_id)}/{mirror_id}"


def open_mirror_definition(default_schema: str = "dbo") -> dict[str, Any]:
    """Return the minimal ``definition`` payload for a startable open mirror.

    Fabric's ``POST /mirroredDatabases`` requires a ``definition`` with
    a base64-encoded ``mirroring.json`` part. Without it, the item is
    created but ``startMirroring`` returns ``MirroringDefinitionMissing``.
    """
    body = MirroredDatabaseBuilder(default_schema=default_schema).to_mirroring_json()
    payload_b64 = base64.b64encode(body.encode("utf-8")).decode("ascii")
    return {
        "parts": [
            {
                "path": "mirroring.json",
                "payload": payload_b64,
                "payloadType": "InlineBase64",
            }
        ]
    }


def create_mirrored_database(
    client: _ClientLike,
    workspace_id: str,
    *,
    display_name: str,
    description: str = "",
    definition: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a Mirrored Database item.

    Args:
        client: A ``FabricClient`` (or anything with a compatible ``post``).
        workspace_id: Target workspace ID.
        display_name: Item display name.
        description: Optional description.
        definition: Inline-base64 definition. Defaults to
            :func:`open_mirror_definition` (a startable GenericMirror /
            MountedRelationalDatabase Delta target with ``defaultSchema=dbo``).
    """
    body = {
        "displayName": display_name,
        "description": description,
        "definition": definition
        if definition is not None
        else open_mirror_definition(),
    }
    return client.post(_items_path(workspace_id), body)


def get_mirrored_database(
    client: _ClientLike, workspace_id: str, mirror_id: str
) -> dict[str, Any]:
    """Fetch the item record for an existing Mirrored Database."""
    return client.get(_item_path(workspace_id, mirror_id))


def start_mirroring(
    client: _ClientLike, workspace_id: str, mirror_id: str
) -> dict[str, Any]:
    """Start replication on a Mirrored Database.

    Returns whatever the endpoint returns (typically empty body, 202 with
    LRO, or a small status object — the underlying client handles
    polling). Callers usually follow with :func:`wait_for_running`.
    """
    return client.post(f"{_item_path(workspace_id, mirror_id)}/startMirroring")


def stop_mirroring(
    client: _ClientLike, workspace_id: str, mirror_id: str
) -> dict[str, Any]:
    """Stop replication on a Mirrored Database."""
    return client.post(f"{_item_path(workspace_id, mirror_id)}/stopMirroring")


def get_mirroring_status(
    client: _ClientLike, workspace_id: str, mirror_id: str
) -> dict[str, Any]:
    """Return the mirror's overall status.

    Body shape varies by Fabric version — typically includes ``status``
    (or ``state``) with values like ``Initialized`` / ``Running`` /
    ``Stopped``. :func:`wait_for_running` accepts either field.
    """
    return client.post(f"{_item_path(workspace_id, mirror_id)}/getMirroringStatus")


def get_tables_mirroring_status(
    client: _ClientLike, workspace_id: str, mirror_id: str
) -> dict[str, Any]:
    """Return per-table replication status.

    Useful for diagnosing why a specific table isn't replicating after a
    parquet drop (often a SchemaMergeFailure surfaces here before the
    overall mirror status flips).
    """
    return client.post(
        f"{_item_path(workspace_id, mirror_id)}/getTablesMirroringStatus"
    )


def wait_for_running(
    client: _ClientLike,
    workspace_id: str,
    mirror_id: str,
    *,
    timeout_s: float = 300,
    poll_interval_s: float = 5,
) -> dict[str, Any]:
    """Poll :func:`get_mirroring_status` until the mirror is running.

    Accepts either ``status`` or ``state`` in the response (Fabric versions
    have used both). Comparison is case-insensitive — a value of
    ``"running"`` or ``"Running"`` both count.

    Args:
        timeout_s: Total wait budget. Set to ``0`` to make a single check
            and raise immediately if not running.
        poll_interval_s: Sleep between polls. Set to ``0`` in tests.

    Raises:
        TimeoutError: If the mirror does not reach ``Running`` within
            ``timeout_s`` seconds.
    """
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}
    while True:
        last = get_mirroring_status(client, workspace_id, mirror_id)
        state = last.get("status") or last.get("state") or ""
        if isinstance(state, str) and state.lower() == "running":
            return last
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Mirror did not reach Running within {timeout_s}s. Last status: {last}"
            )
        time.sleep(poll_interval_s)
