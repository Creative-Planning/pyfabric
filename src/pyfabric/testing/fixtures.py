"""Pytest fixtures and test helpers for local Fabric notebook + pipeline testing.

The pytest fixtures are auto-registered via the plugin entry point so
users get them for free by installing ``pyfabric[testing]``. The
non-fixture helpers (``attach_duckdb_lakehouse``, ``snapshot_delta``)
are plain functions intended to be called from test code or test
fixtures the consumer defines.

Available fixtures:
- ``fabric_spark`` — DuckDBSparkSession with a temporary lakehouse root
- ``mock_notebookutils`` — MockNotebookUtils with a temporary filesystem root
- ``lakehouse_root`` — Path to the temporary lakehouse directory

Available helpers:
- ``attach_duckdb_lakehouse`` — wire parquet fixtures into a DuckDB
  connection as a three-part-named catalog (``<lh>.<schema>.<table>``)
- ``snapshot_delta`` — pull a slice of a OneLake delta table to a local
  parquet fixture for offline tests
"""

from __future__ import annotations

import re
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from .duckdb_spark import DuckDBSparkSession
from .mock_notebookutils import MockNotebookUtils

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa


@pytest.fixture
def lakehouse_root(tmp_path: Path) -> Path:
    """Temporary lakehouse root directory for testing."""
    return tmp_path / "lakehouses"


@pytest.fixture
def fabric_spark(lakehouse_root: Path) -> Generator[DuckDBSparkSession]:
    """DuckDB-backed Spark session for local notebook testing.

    Lakehouse data should be placed at:
        ``lakehouse_root/<lakehouse_name>/Tables/<table_name>/`` (Delta format)
    """
    lakehouse_root.mkdir(parents=True, exist_ok=True)
    session = DuckDBSparkSession(lakehouse_root=lakehouse_root)
    yield session
    session.stop()


@pytest.fixture
def mock_notebookutils(tmp_path: Path) -> MockNotebookUtils:
    """MockNotebookUtils with a temporary filesystem root."""
    root = tmp_path / "notebookutils"
    root.mkdir(parents=True, exist_ok=True)
    return MockNotebookUtils(root=root)


# ── Test helpers (plain functions, not pytest fixtures) ──────────────────────


# Identifier regex matches what DuckDB accepts as an unquoted identifier:
# letters, digits, underscores, starting with letter/underscore. Reject
# everything else so we can splice the names into a SQL statement
# without quoting (and without enabling SQL injection from caller-supplied
# identifiers).
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def attach_duckdb_lakehouse(
    con: duckdb.DuckDBPyConnection,
    lakehouse_name: str,
    schemas: dict[str, dict[str, str | Path]],
) -> str:
    """Attach a parquet-backed catalog to a DuckDB connection.

    After this call, three-part-name lookups resolve against the
    parquet files: ``con.sql("FROM <lakehouse_name>.<schema>.<table>")``
    reads the file you registered for ``(schema, table)``.

    Pairs with :func:`snapshot_delta` (one-time pull from OneLake) and
    the ``SqlConn`` Protocol so a wheel/SqlConn transform under test
    runs against fixture parquet exactly as it would against a real
    lakehouse.

    Args:
        con: An open ``duckdb.DuckDBPyConnection``.
        lakehouse_name: Catalog name (must be a valid SQL identifier:
            letters/digits/underscores, starting with a letter or
            underscore — no quoting required).
        schemas: ``{schema_name: {table_name: parquet_path}}``. Schema
            names and table names must also be valid identifiers.

    Returns:
        The catalog name (so callers can chain or reference the result).

    Raises:
        ValueError: If any name fails identifier validation.
    """
    _require_ident("lakehouse_name", lakehouse_name)
    for schema_name, tables in schemas.items():
        _require_ident("schema name", schema_name)
        for table_name in tables:
            _require_ident("table name", table_name)

    # ``ATTACH ':memory:' AS <name>`` opens a fresh in-memory catalog
    # that lives only for the connection's lifetime — perfect for tests.
    con.execute(f"ATTACH ':memory:' AS {lakehouse_name}")

    for schema_name, tables in schemas.items():
        con.execute(f"CREATE SCHEMA {lakehouse_name}.{schema_name}")
        for table_name, parquet_path in tables.items():
            path_str = str(parquet_path).replace("'", "''")
            # CREATE VIEW reads the parquet on demand — cheaper than
            # materializing the contents into the in-memory catalog,
            # and keeps the parquet file as the source of truth.
            con.execute(
                f"CREATE VIEW {lakehouse_name}.{schema_name}.{table_name} "
                f"AS SELECT * FROM read_parquet('{path_str}')"
            )

    return lakehouse_name


def _require_ident(label: str, value: str) -> None:
    if not _IDENT_RE.match(value):
        raise ValueError(
            f"{label} {value!r} is not a valid SQL identifier "
            "(letters/digits/underscores only, starting with letter or underscore)"
        )


# ── snapshot_delta — pull a OneLake delta slice to a parquet fixture ─────────

# ``deltalake`` reads plain delta tables but raises on Fabric-specific
# features (columnMapping, deletionVectors). The strings below are
# substring-matched against the exception message — when one of these
# is the cause, fall back to reading via the SQL analytics endpoint
# instead of propagating. Any unrelated error (auth, network, bad path)
# propagates normally.
_DELTA_SQL_FALLBACK_MARKERS: tuple[str, ...] = (
    "columnMapping",
    "deletionVectors",
)


def snapshot_delta(
    credential: object,
    *,
    source: str,
    dest: str | Path,
    max_rows: int | None = None,
    filter_expr: tuple[str, str, object] | None = None,
) -> Path:
    """Pull a slice of a OneLake delta table to a local parquet file.

    Use this once per fixture to produce a small offline parquet copy of
    a remote delta table; subsequent test runs read the parquet via
    :func:`attach_duckdb_lakehouse` (no network).

    Strategy: try ``deltalake`` first (works for plain delta — silver
    lakehouses, hand-built tables). Fall back to the SQL analytics
    endpoint when the delta library rejects the table because of
    Fabric-specific features (``columnMapping``, ``deletionVectors`` —
    typical for mirrored databases).

    Args:
        credential: A :class:`pyfabric.client.auth.FabricCredential`-like
            object with a ``storage_token`` attribute. Typed as
            ``object`` to keep this module import-safe without the
            azure extra.
        source: ``abfss://...`` URL pointing at the delta table folder.
        dest: Destination parquet path. Parent directories are created.
        max_rows: Optional row cap. ``None`` (the default) writes the
            full table.
        filter_expr: Optional ``(column, op, value)`` tuple passed to
            the underlying reader as a pushdown predicate.

    Returns:
        The destination ``Path`` (so callers can chain).
    """
    dest_path = Path(dest)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        table = _read_delta_table(credential, source, filter_expr=filter_expr)
    except Exception as e:
        msg = str(e)
        if not any(marker in msg for marker in _DELTA_SQL_FALLBACK_MARKERS):
            raise
        table = _read_via_sql_endpoint(
            credential, source=source, filter_expr=filter_expr
        )

    if max_rows is not None and table.num_rows > max_rows:
        table = table.slice(0, max_rows)

    import pyarrow.parquet as pq

    pq.write_table(table, str(dest_path))
    return dest_path


def _read_delta_table(
    credential: object,
    source: str,
    *,
    filter_expr: tuple[str, str, object] | None = None,
) -> pa.Table:
    """Read a delta table via the ``deltalake`` library.

    Isolated as its own function so :func:`snapshot_delta`'s tests can
    monkeypatch it without spinning up a real delta reader. The lazy
    import keeps ``pyfabric.testing.fixtures`` importable on hosts
    that don't have the ``[lakehouse-io]`` extra installed.
    """
    import deltalake

    storage_options = {"bearer_token": credential.storage_token}  # type: ignore[attr-defined]
    dt = deltalake.DeltaTable(source, storage_options=storage_options)
    if filter_expr is not None:
        return dt.to_pyarrow_table(partitions=[filter_expr])
    return dt.to_pyarrow_table()


def _read_via_sql_endpoint(
    credential: object,
    *,
    source: str,
    filter_expr: tuple[str, str, object] | None = None,
) -> pa.Table:
    """Read a delta table via the Fabric SQL analytics endpoint.

    The SQL endpoint handles ``columnMapping`` / ``deletionVectors``
    that the deltalake library can't, at the cost of a SQL connection
    setup. Real implementation is deferred to a follow-up; the current
    body raises :class:`NotImplementedError` so live callers get a
    clear pointer rather than a silent failure mode. Tests stub this
    function, so the not-implemented body never runs in the unit suite.
    """
    raise NotImplementedError(
        "SQL-endpoint fallback for snapshot_delta is not yet wired up. "
        "Live implementation tracked in a follow-up issue; for now this "
        "path is reachable only when the deltalake library raises one "
        f"of {_DELTA_SQL_FALLBACK_MARKERS} against {source}."
    )
