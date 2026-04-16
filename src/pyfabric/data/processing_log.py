"""
Processing log (watermark) table for incremental/idempotent extraction jobs.

A common pipeline pattern: walk a source, process each item once, and remember
what's already been done so a re-run skips it. This module provides a generic
watermark table with a small API around four operations:

  - :meth:`ProcessingLog.is_processed` — should we skip this source item?
  - :meth:`ProcessingLog.record_success` — mark an item as done.
  - :meth:`ProcessingLog.record_failure` — mark an item as failed (will retry).
  - :meth:`ProcessingLog.failures` — list rows that failed, for replay.

Backed by a :class:`~pyfabric.data.local_lakehouse.LocalLakehouse` (DuckDB).
The table is registered and created on instantiation using the shipped default
schema, or a caller-provided :class:`~pyfabric.data.schema.TableDef`.

Usage:
    from pyfabric.data.local_lakehouse import LocalLakehouse
    from pyfabric.data.processing_log import ProcessingLog

    lake = LocalLakehouse(db_path="extract.duckdb", ws_id="...", lh_id="...")
    plog = ProcessingLog(lake)

    for pdf_path, content_hash in iter_sources():
        if plog.is_processed(pdf_path, content_hash=content_hash):
            continue
        try:
            n = extract(pdf_path)
            plog.record_success(pdf_path, content_hash=content_hash, rows_written=n)
        except Exception as e:
            plog.record_failure(pdf_path, content_hash=content_hash, error=str(e))

Re-running the job is safe: already-successful rows are skipped, and
``plog.failures()`` surfaces what to retry.
"""

from __future__ import annotations

import datetime as _dt
from typing import TYPE_CHECKING

import structlog

from pyfabric.data.schema import Col, TableDef

if TYPE_CHECKING:
    from pyfabric.data.local_lakehouse import LocalLakehouse

log = structlog.get_logger()


STATUS_SUCCESS = "success"
STATUS_FAILURE = "failure"


DEFAULT_TABLE = TableDef(
    name="processing_log",
    description="Watermark/audit table for incremental extraction jobs.",
    columns=(
        Col("source_path", "string", nullable=False, pk=True),
        Col("content_hash", "string"),
        Col("status", "string", nullable=False),
        Col("rows_written", "bigint"),
        Col("error_summary", "string"),
        Col("processed_at", "timestamp", nullable=False),
    ),
)


class ProcessingLog:
    """Watermark table backed by a LocalLakehouse.

    Args:
        lakehouse:  A :class:`LocalLakehouse` to host the table.
        table_def:  Optional custom :class:`TableDef`. If given, it must
                    contain at least a ``source_path`` (string, pk) column
                    and a ``status`` (string) column. Defaults to
                    :data:`DEFAULT_TABLE`.
    """

    def __init__(
        self,
        lakehouse: LocalLakehouse,
        *,
        table_def: TableDef | None = None,
    ) -> None:
        self.lakehouse = lakehouse
        self.table_def = table_def or DEFAULT_TABLE
        self._validate_shape()
        lakehouse.register(self.table_def)

    def _validate_shape(self) -> None:
        names = set(self.table_def.column_names())
        missing = {"source_path", "status"} - names
        if missing:
            raise ValueError(
                f"ProcessingLog table_def is missing required columns: "
                f"{sorted(missing)}"
            )
        sp = self.table_def.column("source_path")
        if sp.type_key != "string" or not sp.pk:
            raise ValueError("'source_path' must be a string primary-key column")

    # ── Queries ──────────────────────────────────────────────────────────────

    def is_processed(
        self,
        source_path: str,
        *,
        content_hash: str | None = None,
    ) -> bool:
        """Return True iff source_path has a recorded success row.

        If ``content_hash`` is given, the stored row's ``content_hash`` must
        match; a hash mismatch means the source changed and should be re-processed.
        A row with status != success is treated as not-processed so it
        will be retried.
        """
        qualified = f"{self.lakehouse.schema}.{self.table_def.name}"
        row = self.lakehouse.conn.execute(
            f"SELECT status, content_hash FROM {qualified} WHERE source_path = ?",
            [source_path],
        ).fetchone()
        if not row:
            return False
        status, stored_hash = row[0], row[1]
        if status != STATUS_SUCCESS:
            return False
        return not (content_hash is not None and stored_hash != content_hash)

    def failures(self) -> list[dict]:
        """Return failed rows as list of dicts (for replay workflows)."""
        qualified = f"{self.lakehouse.schema}.{self.table_def.name}"
        cols = self.table_def.column_names()
        rows = self.lakehouse.conn.execute(
            f"SELECT {', '.join(cols)} FROM {qualified} WHERE status = ?",
            [STATUS_FAILURE],
        ).fetchall()
        return [dict(zip(cols, r, strict=True)) for r in rows]

    # ── Writes ───────────────────────────────────────────────────────────────

    def record_success(
        self,
        source_path: str,
        *,
        content_hash: str | None = None,
        rows_written: int | None = None,
        extra: dict | None = None,
    ) -> None:
        row = {
            "source_path": source_path,
            "content_hash": content_hash,
            "status": STATUS_SUCCESS,
            "rows_written": rows_written,
            "error_summary": None,
            "processed_at": _dt.datetime.now(tz=_dt.UTC),
        }
        if extra:
            row.update(extra)
        self._upsert(row)

    def record_failure(
        self,
        source_path: str,
        *,
        error: str,
        content_hash: str | None = None,
        extra: dict | None = None,
    ) -> None:
        row = {
            "source_path": source_path,
            "content_hash": content_hash,
            "status": STATUS_FAILURE,
            "rows_written": None,
            "error_summary": error[:4000],
            "processed_at": _dt.datetime.now(tz=_dt.UTC),
        }
        if extra:
            row.update(extra)
        self._upsert(row)

    def _upsert(self, row: dict) -> None:
        errors = self.table_def.validate_row(row)
        if errors:
            raise ValueError(
                f"ProcessingLog row invalid for {row.get('source_path')!r}: "
                + "; ".join(errors)
            )
        qualified = f"{self.lakehouse.schema}.{self.table_def.name}"
        cols = self.table_def.column_names()
        placeholders = ", ".join(["?"] * len(cols))
        values = tuple(row.get(c) for c in cols)
        # DuckDB DDL doesn't emit a PRIMARY KEY constraint (pk on Col is
        # informational), so emulate upsert with DELETE + INSERT in a txn.
        conn = self.lakehouse.conn
        conn.execute("BEGIN")
        try:
            conn.execute(
                f"DELETE FROM {qualified} WHERE source_path = ?", [row["source_path"]]
            )
            conn.execute(
                f"INSERT INTO {qualified} ({', '.join(cols)}) VALUES ({placeholders})",
                values,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
