"""Open Mirroring landing-zone data plane.

Producers push parquet files into a Fabric Open Mirror's landing zone;
the mirror replicates each file's rows into the matching Delta table
under ``Tables/...``. This module owns the **data plane** — pushing
files. The **item plane** (creating / starting / stopping the mirror,
git-sync artifact) lives in :mod:`pyfabric.items.mirrored_database`.

Path shape (per the Microsoft Fabric Open Mirroring documentation)::

    Files/LandingZone/[<schema>.schema/]<table>/_metadata.json
    Files/LandingZone/[<schema>.schema/]<table>/<NNNNNNNNNNNNNNNNNNNN>.parquet

``<schema>.schema/`` is optional; use it for multi-source namespacing.
Sequential filenames are zero-padded to 20 digits — Fabric reads files
in numeric order unless ``_metadata.json`` opts in to
``LastUpdateTimeFileDetection``.

Usage::

    from pyfabric.client.auth import FabricCredential
    from pyfabric.data.open_mirror import OpenMirrorClient

    cred = FabricCredential()
    mirror = OpenMirrorClient(cred, ws_id, mirror_id)
    mirror.ensure_table("dim_customer", schema="bc2", key_columns=["id"])
    name = mirror.next_data_filename("dim_customer", schema="bc2")
    mirror.upload_data_file(
        "dim_customer",
        "tests/fixtures/dim_customer_001.parquet",
        schema="bc2",
        remote_filename=name,
    )

Credit
------

The Open Mirroring landing-zone protocol — path shape, ``_metadata.json``
keys (``keyColumns``, ``isUpsertDefaultRowMarker``,
``fileDetectionStrategy``), 20-digit sequential filename convention,
``_ProcessedFiles`` retention behaviour, and the ``__rowMarker__``
semantics covered in PR C — were first published as research notes by
the *Learn Microsoft Fabric* community at
https://github.com/UnifiedEducation/research/tree/main/open-mirroring.
That repo currently ships without a license, so this implementation is
**clean-room**: written from the publicly-documented Microsoft Fabric
protocol and the public README + ``docs/landing-zone-format.md`` only.
No code is copied. The research repo is the recommended companion read
for the *why* behind each shape.
"""

from __future__ import annotations

import io
import json
from enum import IntEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import structlog

from pyfabric.data import onelake

if TYPE_CHECKING:
    import pyarrow as pa

    from pyfabric.client.auth import FabricCredential

log = structlog.get_logger()


_LANDING_ZONE_ROOT = "Files/LandingZone"
_FILENAME_WIDTH = 20
_ROW_MARKER_COLUMN = "__rowMarker__"


# ── RowMarker ────────────────────────────────────────────────────────────────


class RowMarker(IntEnum):
    """Typed values for the mirror protocol's ``__rowMarker__`` column.

    Subclassing :class:`IntEnum` so values can be used directly in
    pyarrow array construction without a manual ``int()`` cast.
    """

    INSERT = 0
    UPDATE = 1
    DELETE = 2
    UPSERT = 4


WriteMode = Literal["insert", "update", "delete", "upsert"]
_MODE_TO_MARKER: dict[WriteMode, RowMarker] = {
    "insert": RowMarker.INSERT,
    "update": RowMarker.UPDATE,
    "delete": RowMarker.DELETE,
    "upsert": RowMarker.UPSERT,
}


# ── Schema-compat helper ─────────────────────────────────────────────────────


class OpenMirrorSchemaIncompatible(Exception):
    """Raised by :func:`assert_schema_compat` when a producer's new arrow
    schema would trigger Fabric's ``SchemaMergeFailure`` against the
    mirror's existing column shape.

    The ``violations`` attribute lists every offending column /
    rule pair so callers can present a single diagnostic for a multi-
    drift refactor instead of fixing one issue at a time.
    """

    def __init__(self, violations: list[str]) -> None:
        self.violations = violations
        super().__init__(
            "OpenMirror schema incompatibility:\n  - " + "\n  - ".join(violations)
        )


def assert_schema_compat(old: pa.Schema, new: pa.Schema) -> None:
    """Raise :class:`OpenMirrorSchemaIncompatible` if ``new`` would be
    rejected by Fabric mirroring relative to ``old``.

    Catches three drift patterns documented in the protocol:

    - **Type changed** on an existing column (e.g. ``int32 → int64`` or
      ``date32 → timestamp``). Fabric's mirror rewriter throws
      ``SchemaMergeFailure`` and stops replicating.
    - **New non-nullable column added.** Existing rows can't satisfy
      it; new rows would have to fabricate a default. Keep new
      columns nullable.
    - **``__rowMarker__`` not last** in ``new``. Fabric requires it as
      the trailing column.

    Removing a column is **not** an error: Fabric keeps unioned
    columns and NULLs out the absent value on new rows.

    Args:
        old: The schema currently understood by the mirror (the
            previous file's schema, or a producer's registered
            baseline).
        new: The schema of the about-to-be-uploaded file.

    Raises:
        OpenMirrorSchemaIncompatible: If any drift pattern fires.
    """
    violations: list[str] = []

    old_fields: dict[str, Any] = {f.name: f for f in old}
    new_fields: dict[str, Any] = {f.name: f for f in new}

    for name, new_field in new_fields.items():
        if name in old_fields:
            old_field = old_fields[name]
            if not new_field.type.equals(old_field.type):
                violations.append(
                    f"column {name!r}: type changed from {old_field.type} "
                    f"to {new_field.type}"
                )
        else:
            if not new_field.nullable:
                violations.append(
                    f"column {name!r}: new column added as NOT NULL (must be nullable)"
                )

    if _ROW_MARKER_COLUMN in new_fields:
        last = list(new_fields)[-1]
        if last != _ROW_MARKER_COLUMN:
            violations.append(
                f"column {_ROW_MARKER_COLUMN!r} must be the last column; "
                f"found {last!r} after it"
            )

    if violations:
        raise OpenMirrorSchemaIncompatible(violations)


class OpenMirrorClient:
    """Push parquet files into a Fabric Open Mirror's landing zone.

    Args:
        credential: Anything with a ``storage_token`` attribute that
            yields a fresh OneLake DFS bearer token. In production this
            is a :class:`pyfabric.client.auth.FabricCredential`; tests
            pass a tiny stub.
        workspace_id: Workspace GUID hosting the mirror.
        mirror_id: Mirrored Database item GUID.
    """

    def __init__(
        self,
        credential: FabricCredential | Any,
        workspace_id: str,
        mirror_id: str,
    ) -> None:
        self._credential = credential
        self.workspace_id = workspace_id
        self.mirror_id = mirror_id

    # ── Path helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def table_folder(table: str, *, schema: str | None = None) -> str:
        """Return the landing-zone folder path for a table.

        Forward-slash, item-relative — pass straight to
        :mod:`pyfabric.data.onelake` helpers as ``path``.
        """
        if schema:
            return f"{_LANDING_ZONE_ROOT}/{schema}.schema/{table}"
        return f"{_LANDING_ZONE_ROOT}/{table}"

    # ── Table lifecycle ──────────────────────────────────────────────────────

    def ensure_table(
        self,
        table: str,
        *,
        schema: str | None = None,
        key_columns: list[str],
        upsert_default: bool = False,
        detect_by_last_update: bool = False,
    ) -> None:
        """Create / overwrite the table's ``_metadata.json``.

        Fabric implicitly creates the table folder when the first file
        lands at ``<folder>/_metadata.json``; no explicit directory
        create is needed against the ADLS Gen2 hierarchical namespace.

        Args:
            table: Table name (no extension, no path separators).
            schema: Optional ``<schema>.schema/`` namespace prefix.
            key_columns: Required. Columns that uniquely identify a
                row — Fabric uses these to resolve update/delete/upsert
                row markers.
            upsert_default: If True, sets
                ``isUpsertDefaultRowMarker: true`` so rows without an
                explicit ``__rowMarker__`` are treated as upserts (4).
            detect_by_last_update: If True, sets
                ``fileDetectionStrategy: LastUpdateTimeFileDetection``
                so Fabric reads files in last-modified order instead of
                requiring sequential filenames.

        Raises:
            ValueError: If ``key_columns`` is empty.
        """
        if not key_columns:
            raise ValueError("key_columns must not be empty")

        meta: dict[str, Any] = {"keyColumns": list(key_columns)}
        if upsert_default:
            meta["isUpsertDefaultRowMarker"] = True
        if detect_by_last_update:
            meta["fileDetectionStrategy"] = "LastUpdateTimeFileDetection"

        folder = self.table_folder(table, schema=schema)
        path = f"{folder}/_metadata.json"
        payload = json.dumps(meta, indent=2).encode("utf-8")
        onelake.upload_file(
            self._credential.storage_token,
            self.workspace_id,
            self.mirror_id,
            path,
            payload,
        )
        log.info(
            "open_mirror_ensure_table",
            schema=schema,
            table=table,
            key_columns=list(key_columns),
            upsert_default=upsert_default,
            detect_by_last_update=detect_by_last_update,
        )

    # ── File management ─────────────────────────────────────────────────────

    def next_data_filename(
        self,
        table: str,
        *,
        schema: str | None = None,
        extension: str = "parquet",
    ) -> str:
        """Return the next zero-padded sequential filename for ``table``.

        Lists the table folder (one level, non-recursive), parses every
        file whose stem is exactly ``_FILENAME_WIDTH`` digits, returns
        ``f\"{max+1:020d}.{extension}\"``. Subdirectories
        (``_ProcessedFiles``, ``_FilesReadyToDelete``) and non-numeric
        names are ignored.

        Use only when the table is in **sequential-filename mode**; if
        ``ensure_table(..., detect_by_last_update=True)`` was called,
        any filename works and you don't need this.
        """
        folder = self.table_folder(table, schema=schema)
        entries = onelake.list_paths(
            self._credential.storage_token,
            self.workspace_id,
            self.mirror_id,
            folder,
            recursive=False,
        )
        max_num = 0
        for e in entries:
            if e.get("isDirectory", "false") == "true":
                continue
            base = Path(e.get("name", "")).name
            stem = base.split(".", 1)[0]
            if len(stem) == _FILENAME_WIDTH and stem.isdigit():
                max_num = max(max_num, int(stem))
        return f"{(max_num + 1):0{_FILENAME_WIDTH}d}.{extension}"

    def upload_data_file(
        self,
        table: str,
        local_path: str | Path,
        *,
        schema: str | None = None,
        remote_filename: str | None = None,
    ) -> str:
        """Upload a local file into the table's landing-zone folder.

        Args:
            table: Table name.
            local_path: Path to the file to upload — bytes are read
                whole into memory (fine for the parquet sizes Open
                Mirroring expects; if you need streaming, use
                :func:`pyfabric.data.onelake.upload_file` directly).
            schema: Optional ``<schema>.schema/`` namespace.
            remote_filename: If set, used verbatim as the filename and
                no folder listing is needed. If ``None``, falls back to
                :meth:`next_data_filename` with the local file's
                extension.

        Returns:
            The full DFS path that was written (``Files/LandingZone/...``).
        """
        local = Path(local_path)
        ext = local.suffix.lstrip(".") or "parquet"
        return self._upload_bytes(
            table,
            local.read_bytes(),
            schema=schema,
            remote_filename=remote_filename,
            extension=ext,
        )

    def _upload_bytes(
        self,
        table: str,
        data: bytes,
        *,
        schema: str | None,
        remote_filename: str | None,
        extension: str,
    ) -> str:
        """Internal: upload a bytes payload to the table folder.

        Shared by :meth:`upload_data_file` (file path → bytes) and
        :meth:`write_rows` (arrow table → parquet bytes via BytesIO).
        """
        if remote_filename is None:
            remote_filename = self.next_data_filename(
                table, schema=schema, extension=extension
            )
        folder = self.table_folder(table, schema=schema)
        remote_path = f"{folder}/{remote_filename}"
        onelake.upload_file(
            self._credential.storage_token,
            self.workspace_id,
            self.mirror_id,
            remote_path,
            data,
        )
        log.info(
            "open_mirror_upload",
            schema=schema,
            table=table,
            remote=remote_path,
            bytes=len(data),
        )
        return remote_path

    # ── High-level write_rows ───────────────────────────────────────────────

    def write_rows(
        self,
        table: str,
        arrow_table: pa.Table,
        *,
        schema: str | None = None,
        mode: WriteMode | None = None,
        expected_schema: pa.Schema | None = None,
        remote_filename: str | None = None,
    ) -> str:
        """Convert an arrow table to parquet and upload it as the next
        data file.

        Two stamping paths:

        - **Auto-stamped** (``mode != None``): the arrow table must
          NOT already contain a ``__rowMarker__`` column. The helper
          appends one with every row set to the corresponding
          :class:`RowMarker` value.
        - **Caller-stamped** (``mode is None``): the arrow table must
          contain ``__rowMarker__`` and it must be the last column.
          Useful when one file mixes inserts / updates / deletes.

        If ``expected_schema`` is provided, :func:`assert_schema_compat`
        runs **before** any upload — a mismatched schema raises
        :class:`OpenMirrorSchemaIncompatible` with no DFS side effect.
        Catches Fabric's silent ``SchemaMergeFailure`` gotcha at the
        producer's pre-commit boundary.

        Args:
            table: Table name.
            arrow_table: Rows to land. Column order is preserved.
            schema: Optional ``<schema>.schema/`` namespace.
            mode: Auto-stamp every row with the corresponding marker.
                One of ``"insert"`` (0), ``"update"`` (1),
                ``"delete"`` (2), ``"upsert"`` (4).
            expected_schema: Optional baseline to validate against.
            remote_filename: Override the auto-generated 20-digit
                sequential name.

        Returns:
            The DFS path that was written.

        Raises:
            ValueError: If ``mode`` and ``__rowMarker__`` are both
                present (ambiguous), or if ``mode`` is ``None`` and
                ``__rowMarker__`` is missing or not last.
            OpenMirrorSchemaIncompatible: If ``expected_schema`` is
                provided and the table's schema doesn't satisfy
                :func:`assert_schema_compat`.
        """
        import pyarrow as pa_mod
        import pyarrow.parquet as pq

        column_names = arrow_table.column_names

        if mode is not None:
            if _ROW_MARKER_COLUMN in column_names:
                raise ValueError(
                    f"mode={mode!r} stamps {_ROW_MARKER_COLUMN!r} but the arrow "
                    "table already contains that column — pass mode=None to use "
                    "the existing values, or drop the column to auto-stamp."
                )
            marker = _MODE_TO_MARKER[mode]
            row_count = arrow_table.num_rows
            arrow_table = arrow_table.append_column(
                _ROW_MARKER_COLUMN,
                pa_mod.array([int(marker)] * row_count, type=pa_mod.int32()),
            )
        else:
            if _ROW_MARKER_COLUMN not in column_names:
                raise ValueError(
                    f"mode is None so {_ROW_MARKER_COLUMN!r} must be present in "
                    "the arrow table; pass mode='insert'/'update'/'delete'/"
                    "'upsert' to auto-stamp instead."
                )
            if column_names[-1] != _ROW_MARKER_COLUMN:
                raise ValueError(
                    f"{_ROW_MARKER_COLUMN!r} must be the last column; found "
                    f"{column_names[-1]!r} after it."
                )

        if expected_schema is not None:
            assert_schema_compat(expected_schema, arrow_table.schema)

        buf = io.BytesIO()
        pq.write_table(arrow_table, buf)
        return self._upload_bytes(
            table,
            buf.getvalue(),
            schema=schema,
            remote_filename=remote_filename,
            extension="parquet",
        )

    # ── Cleanup observability ───────────────────────────────────────────────

    def list_processed(self, table: str, *, schema: str | None = None) -> list[str]:
        """List filenames currently in the table's ``_ProcessedFiles`` folder.

        Returns the basename of each file (no path). Returns ``[]`` when
        the folder doesn't exist (Fabric only creates it after the first
        file completes replication). Useful for cleanup helpers and
        debugging \"why hasn't this row replicated?\" — a file that's
        moved into ``_ProcessedFiles`` has been ingested.
        """
        folder = f"{self.table_folder(table, schema=schema)}/_ProcessedFiles"
        entries = onelake.list_paths(
            self._credential.storage_token,
            self.workspace_id,
            self.mirror_id,
            folder,
            recursive=False,
        )
        result: list[str] = []
        for e in entries:
            if e.get("isDirectory", "false") == "true":
                continue
            name = Path(e.get("name", "")).name
            if name:
                result.append(name)
        return result
