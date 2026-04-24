"""
Local DuckDB-backed lakehouse for development and batch processing.

Provides a local staging area that mirrors a Fabric lakehouse schema.
Process data locally (no Spark session), validate in DuckDB, then push
Delta tables to OneLake in bulk.

Usage:
    from pyfabric.client.auth import FabricCredential
    from pyfabric.data.local_lakehouse import LocalLakehouse

    cred = FabricCredential()
    local = LocalLakehouse(
        db_path="my_extract.duckdb",
        ws_id="9b0973...",
        lh_id="01a9eea7...",
        schema="ddb",
    )

    # Create tables from DDL strings
    local.execute_ddl([
        "CREATE TABLE IF NOT EXISTS ddb.products (id VARCHAR, name VARCHAR)",
    ])

    # Insert rows
    local.insert("products", [{"id": "1", "name": "Widget"}])

    # Query locally
    df = local.query_df("SELECT * FROM ddb.products")

    # Push all non-empty tables to OneLake as Delta
    local.push_all(cred)

    # Push a single table
    local.push_table(cred, "products")

    # Pull a table from OneLake into local DuckDB
    local.pull_table(cred, "products")
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

import structlog

from pyfabric.data.schema import TableDef

if TYPE_CHECKING:
    import duckdb as duckdb_mod
    import pandas as pd
    import pyarrow as pa

    from pyfabric.client.auth import FabricCredential

log = structlog.get_logger()


OnDrift = Literal["ignore", "raise", "evolve"]


class LocalLakehouseSchemaDrift(Exception):
    """Raised when ``register()`` detects that an existing DuckDB table
    has fewer columns than its current ``TableDef``.

    Callers can react by picking a different ``on_drift`` policy
    (``"evolve"`` to add the missing columns in place, or ``"ignore"``
    to preserve the pre-drift-detection behaviour) or by re-creating
    the database from scratch. The exception's ``drift`` attribute maps
    each affected table name to the list of missing column names.
    """

    def __init__(self, drift: dict[str, list[str]]):
        self.drift = drift
        parts = [f"{name}: missing {cols}" for name, cols in drift.items()]
        super().__init__(
            "LocalLakehouse schema drift detected — "
            + "; ".join(parts)
            + ". Pass on_drift='evolve' to add the missing columns, "
            "on_drift='ignore' to keep the existing schema, "
            "or re-create the database."
        )


class LocalLakehouse:
    """DuckDB-backed local mirror of a Fabric lakehouse.

    Args:
        db_path:    Path to DuckDB file (created if missing).
        ws_id:      Fabric workspace ID.
        lh_id:      Fabric lakehouse ID.
        schema:     DuckDB schema name — also used as the OneLake table path
                    prefix: ``Tables/{schema}/{table_name}/``.
        read_only:  Open DuckDB in read-only mode.
    """

    def __init__(
        self,
        db_path: str | Path,
        ws_id: str,
        lh_id: str,
        schema: str = "ddb",
        *,
        read_only: bool = False,
    ):
        import duckdb as duckdb_mod

        self.db_path = Path(db_path)
        self.ws_id = ws_id
        self.lh_id = lh_id
        self.schema = schema
        self._conn = duckdb_mod.connect(str(self.db_path), read_only=read_only)
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        self._tables: dict[str, TableDef] = {}
        log.info("LocalLakehouse opened", db=str(self.db_path), schema=schema)

    @property
    def conn(self) -> duckdb_mod.DuckDBPyConnection:
        """Raw DuckDB connection for advanced queries."""
        return self._conn

    # ── Schema management ────────────────────────────────────────────────────

    def execute_ddl(self, statements: list[str]) -> int:
        """Execute a list of DDL statements (CREATE TABLE, etc.).

        Returns the number of statements executed.
        """
        for stmt in statements:
            self._conn.execute(stmt)
        return len(statements)

    def register(
        self,
        tables: TableDef | tuple[TableDef, ...] | list[TableDef],
        *,
        on_drift: OnDrift = "raise",
    ) -> int:
        """Register one or more TableDefs and create the DuckDB tables.

        Registered tables are used by ``insert_typed`` to validate rows
        before insert. Tables that already exist with the same columns
        are left alone.

        When an existing table has **fewer** columns than its current
        ``TableDef`` (additive drift — e.g. new extractor fields added
        mid-sprint) the behaviour is controlled by ``on_drift``:

        - ``"raise"`` (default) — raise :class:`LocalLakehouseSchemaDrift`
          listing every affected table and missing column. Forces the
          caller to make an explicit choice.
        - ``"evolve"`` — issue ``ALTER TABLE ... ADD COLUMN`` for each
          missing column, preserving existing rows.
        - ``"ignore"`` — keep the existing table shape. Matches the
          pre-0.1.0b8 silent behaviour.

        Type changes and column removals are **not** detected here;
        only additive drift is. Use a fresh database if the column
        types change.

        Returns the number of tables registered.
        """
        if on_drift not in ("raise", "evolve", "ignore"):
            raise ValueError(
                f"on_drift must be 'raise', 'evolve', or 'ignore'; got {on_drift!r}"
            )

        if isinstance(tables, TableDef):
            tables = (tables,)

        drift = self._detect_drift(tables)
        if drift:
            if on_drift == "raise":
                raise LocalLakehouseSchemaDrift(drift)
            if on_drift == "evolve":
                self._apply_drift(drift, tables)
            # on_drift == "ignore" — intentionally fall through without ALTER.

        for table in tables:
            self._tables[table.name] = table
            self._conn.execute(table.to_duckdb_ddl(self.schema))
        return len(tables)

    def evolve_schema(
        self, tables: TableDef | tuple[TableDef, ...] | list[TableDef]
    ) -> int:
        """Evolve the DuckDB schema to match the given ``TableDef``(s).

        Creates any tables that don't exist yet and issues
        ``ALTER TABLE ... ADD COLUMN`` for every column present in the
        ``TableDef`` but missing from the existing table. Existing rows
        are preserved; new columns are added as nullable regardless of
        the ``Col.nullable`` flag, because DuckDB cannot add a NOT NULL
        column to a populated table without a default.

        No-ops when the live schema already matches the definitions.

        Returns the number of tables processed.
        """
        if isinstance(tables, TableDef):
            tables = (tables,)

        drift = self._detect_drift(tables)
        if drift:
            self._apply_drift(drift, tables)

        for table in tables:
            self._tables[table.name] = table
            # CREATE IF NOT EXISTS covers the "table is new" case; ALTERs
            # above have already handled additive drift on existing tables.
            self._conn.execute(table.to_duckdb_ddl(self.schema))
        return len(tables)

    def _existing_columns(self, table_name: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [self.schema, table_name],
        ).fetchall()
        return [r[0] for r in rows]

    def _detect_drift(
        self, tables: tuple[TableDef, ...] | list[TableDef]
    ) -> dict[str, list[str]]:
        """Return {table_name: [missing_column, ...]} for tables that
        already exist but are missing columns from their TableDef.

        Non-existent tables are skipped — they will be CREATE'd on the
        normal path. Only additive drift (TableDef columns missing from
        the live table) is reported.
        """
        drift: dict[str, list[str]] = {}
        for table in tables:
            existing = self._existing_columns(table.name)
            if not existing:
                continue
            existing_set = set(existing)
            missing = [c.name for c in table.columns if c.name not in existing_set]
            if missing:
                drift[table.name] = missing
        return drift

    def _apply_drift(
        self,
        drift: dict[str, list[str]],
        tables: tuple[TableDef, ...] | list[TableDef],
    ) -> None:
        from pyfabric.data.schema import DUCKDB_TYPES

        by_name = {t.name: t for t in tables}
        for table_name, missing in drift.items():
            table = by_name[table_name]
            for col_name in missing:
                col = table.column(col_name)
                duck_type = DUCKDB_TYPES[col.type_key]
                self._conn.execute(
                    f"ALTER TABLE {self.schema}.{table_name} "
                    f"ADD COLUMN {col_name} {duck_type}"
                )
                log.info(
                    "evolved_schema_added_column",
                    table=table_name,
                    column=col_name,
                    duckdb_type=duck_type,
                )

    def registered_tables(self) -> dict[str, TableDef]:
        """Return a copy of the registered TableDef map."""
        return dict(self._tables)

    def table_names(self) -> list[str]:
        """List table names in the local schema."""
        rows = self._conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [self.schema],
        ).fetchall()
        return [r[0] for r in rows]

    def row_count(self, table_name: str) -> int:
        """Return row count for a table."""
        result = self._conn.execute(
            f"SELECT COUNT(*) FROM {self.schema}.{table_name}"
        ).fetchone()
        return result[0] if result else 0

    def table_counts(self) -> dict[str, int]:
        """Return {table_name: row_count} for all non-empty tables."""
        counts = {}
        for name in self.table_names():
            c = self.row_count(name)
            if c > 0:
                counts[name] = c
        return counts

    # ── Local data operations ────────────────────────────────────────────────

    def insert(self, table_name: str, rows: list[dict]) -> int:
        """Insert rows into a local DuckDB table.

        Args:
            table_name: Table name (without schema prefix).
            rows:       List of row dicts. Keys must match column names.

        Returns:
            Number of rows inserted.
        """
        if not rows:
            return 0

        # Get column names from the table schema
        cols_info = self._conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [self.schema, table_name],
        ).fetchall()
        col_names = [c[0] for c in cols_info]

        if not col_names:
            raise ValueError(
                f"Table {self.schema}.{table_name} not found or has no columns"
            )

        placeholders = ", ".join(["?"] * len(col_names))
        cols_sql = ", ".join(col_names)
        sql = f"INSERT INTO {self.schema}.{table_name} ({cols_sql}) VALUES ({placeholders})"

        values = []
        for row in rows:
            vals = []
            for cn in col_names:
                val = row.get(cn)
                # Convert datetime/date objects to strings for DuckDB VARCHAR columns
                if val is not None and hasattr(val, "isoformat"):
                    val = val.isoformat() if hasattr(val, "hour") else str(val)
                vals.append(val)
            values.append(tuple(vals))

        self._conn.executemany(sql, values)
        return len(values)

    def insert_typed(self, table_name: str, rows: list[dict]) -> int:
        """Insert rows with strict type validation against a registered TableDef.

        Unlike :meth:`insert`, this method:
          - Requires the table to have been registered via :meth:`register`.
          - Validates every row against the TableDef, raising ``ValueError``
            if any row has a missing non-nullable column, a type mismatch,
            or an empty string on a non-string column.
          - Passes date/datetime values through as native objects rather
            than silently stringifying them.

        Args:
            table_name: Table name (without schema prefix). Must be registered.
            rows:       List of row dicts. Keys should match column names.

        Returns:
            Number of rows inserted.

        Raises:
            KeyError:   If the table has not been registered.
            ValueError: If any row fails validation. The message lists each
                        invalid row's index together with all of its errors.
        """
        if not rows:
            return 0

        if table_name not in self._tables:
            raise KeyError(
                f"Table {table_name!r} is not registered. "
                "Call register() with the TableDef first, "
                "or use insert() for untyped inserts."
            )

        table = self._tables[table_name]
        col_names = table.column_names()

        errors: list[str] = []
        for idx, row in enumerate(rows):
            unknown = set(row) - set(col_names)
            if unknown:
                errors.append(f"row[{idx}]: unknown columns {sorted(unknown)}")
            for msg in table.validate_row(row):
                errors.append(f"row[{idx}]: {msg}")

        if errors:
            joined = "\n  ".join(errors)
            raise ValueError(
                f"insert_typed rejected {len(rows)} row(s) into "
                f"{self.schema}.{table_name}:\n  {joined}"
            )

        placeholders = ", ".join(["?"] * len(col_names))
        cols_sql = ", ".join(col_names)
        sql = (
            f"INSERT INTO {self.schema}.{table_name} "
            f"({cols_sql}) VALUES ({placeholders})"
        )

        values = [tuple(row.get(cn) for cn in col_names) for row in rows]
        self._conn.executemany(sql, values)
        return len(values)

    def query_df(self, sql: str) -> pd.DataFrame:
        """Execute SQL and return a pandas DataFrame."""
        return self._conn.execute(sql).fetchdf()

    def query_arrow(self, sql: str) -> pa.Table:
        """Execute SQL and return a PyArrow Table."""
        return self._conn.execute(sql).arrow()

    def commit(self) -> None:
        """Commit the current transaction."""
        self._conn.commit()

    # ── OneLake push ─────────────────────────────────────────────────────────

    def push_table(
        self,
        credential: FabricCredential,
        table_name: str,
        *,
        mode: str = "overwrite",
        skip_empty: bool = False,
        target_schema: str | None = None,
    ) -> int:
        """Push a single DuckDB table to OneLake as a Delta table.

        Writes to ``Tables/{target_schema or self.schema}/{table_name}/``
        in the lakehouse. The source SELECT always reads from
        ``self.schema`` — ``target_schema`` only affects the destination
        OneLake path.

        Args:
            credential: FabricCredential for storage token.
            table_name: Table name (without schema prefix).
            mode:       "overwrite" or "append".
            skip_empty: If True, skip the write when the table has 0 rows
                and return 0. Defaults to False because DirectLake
                semantic models bind to ``abfss://.../Tables/<schema>/<table>``
                and fail refresh with "source tables either do not exist
                or access was denied" when the delta log is missing —
                zero-row deltas are valid and DirectLake-compatible, so
                writing them is the safer default.
            target_schema: Override the destination lakehouse schema.
                Defaults to ``self.schema``. Useful when one local
                DuckDB file holds multiple medallion layers
                (``dbo`` / ``silver`` / ``gold``) and each pushes to a
                separate Fabric lakehouse that uses ``dbo`` as its
                canonical target schema.

        Returns:
            Number of rows written.
        """
        from pyfabric.data.lakehouse import write_table

        if target_schema is not None and not target_schema.strip():
            raise ValueError(
                "target_schema must be a non-empty string or None; "
                f"got {target_schema!r}"
            )

        effective_target = target_schema or self.schema

        # .arrow() returns RecordBatchReader; .read_all() materializes to Table
        reader = self._conn.execute(f"SELECT * FROM {self.schema}.{table_name}").arrow()
        arrow_table = reader.read_all() if hasattr(reader, "read_all") else reader

        if arrow_table.num_rows == 0 and skip_empty:
            log.info("skipping_empty_table", table=table_name)
            return 0

        if effective_target != self.schema:
            log.info(
                "pushing_table_cross_schema",
                table=table_name,
                source_schema=self.schema,
                target_schema=effective_target,
                rows=arrow_table.num_rows,
            )

        result = write_table(
            credential,
            self.ws_id,
            self.lh_id,
            table_name,
            arrow_table,
            schema=effective_target,
            mode=mode,
            source="LocalLakehouse.push_table",
        )
        return result.row_count

    def push_all(
        self,
        credential: FabricCredential,
        *,
        mode: str = "overwrite",
        tables: list[str] | None = None,
        skip_empty: bool = False,
        target_schema: str | None = None,
    ) -> dict[str, int]:
        """Push tables to OneLake, including zero-row ones by default.

        Args:
            credential: FabricCredential for storage token.
            mode:       "overwrite" or "append".
            tables:     Optional list of table names to push. If None,
                        pushes all tables in the local schema.
            skip_empty: If True, skip tables with 0 rows. Defaults to
                        False because DirectLake semantic models bind to
                        ``abfss://.../Tables/<schema>/<table>`` and fail
                        refresh with "source tables either do not exist
                        or access was denied" when the delta log is
                        missing — writing zero-row deltas keeps the log
                        valid and DirectLake-compatible.
            target_schema: Override the destination lakehouse schema for
                        every pushed table. See :meth:`push_table`.

        Returns:
            Dict of {table_name: rows_written}. Skipped-empty tables
            are absent; failed pushes map to -1.
        """
        target_tables = tables or self.table_names()
        results = {}

        for name in target_tables:
            count = self.row_count(name)
            if count == 0 and skip_empty:
                continue
            log.info("pushing_table", table=name, rows=count)
            try:
                written = self.push_table(
                    credential,
                    name,
                    mode=mode,
                    skip_empty=skip_empty,
                    target_schema=target_schema,
                )
                results[name] = written
            except Exception as e:
                log.error("push_table_failed", table=name, error=str(e))
                results[name] = -1  # signal failure

        total = sum(v for v in results.values() if v >= 0)
        log.info(
            "push_complete",
            total_rows=total,
            table_count=sum(1 for v in results.values() if v >= 0),
        )
        return results

    # ── OneLake pull ─────────────────────────────────────────────────────────

    def pull_table(
        self,
        credential: FabricCredential,
        table_name: str,
        *,
        replace: bool = True,
    ) -> int:
        """Pull a Delta table from OneLake into local DuckDB.

        Args:
            credential: FabricCredential for storage token.
            table_name: Table name (without schema prefix).
            replace:    If True, DROP and recreate local table from remote data.
                        If False, append to existing local data.

        Returns:
            Number of rows pulled.
        """
        from pyfabric.data.lakehouse import read_table

        df = read_table(
            credential, self.ws_id, self.lh_id, table_name, schema=self.schema
        )

        if replace:
            self._conn.execute(f"DROP TABLE IF EXISTS {self.schema}.{table_name}")
            self._conn.execute(
                f"CREATE TABLE {self.schema}.{table_name} AS SELECT * FROM df"
            )
        else:
            self._conn.execute(
                f"INSERT INTO {self.schema}.{table_name} SELECT * FROM df"
            )

        return len(df)

    # ── Schema rename ────────────────────────────────────────────────────────

    def rename_schema(self, src_schema: str, dst_schema: str) -> list[str]:
        """Rename a DuckDB schema by moving every table into a new schema.

        DuckDB 1.5.x has no ``ALTER SCHEMA ... RENAME``, so this uses the
        portable ``CREATE TABLE dst.x AS SELECT * FROM src.x`` pattern
        followed by ``DROP TABLE src.x`` and ``DROP SCHEMA src``. Rows
        are preserved; column types follow whatever DuckDB infers from
        ``SELECT *`` (matches the source exactly).

        If ``src_schema`` is the current ``self.schema`` the attribute
        is updated to ``dst_schema`` so subsequent calls on this handle
        target the renamed location without a reopen.

        Returns the list of table names that moved.
        """
        if src_schema == dst_schema:
            raise ValueError(
                f"rename_schema src and dst are identical ({src_schema!r}); "
                "nothing to do"
            )

        schemas = {
            r[0]
            for r in self._conn.execute(
                "SELECT schema_name FROM information_schema.schemata"
            ).fetchall()
        }
        if src_schema not in schemas:
            raise ValueError(f"source schema {src_schema!r} does not exist")

        table_rows = self._conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? ORDER BY table_name",
            [src_schema],
        ).fetchall()
        tables = [r[0] for r in table_rows]

        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {dst_schema}")
        for name in tables:
            self._conn.execute(
                f"CREATE TABLE {dst_schema}.{name} AS SELECT * FROM {src_schema}.{name}"
            )
            self._conn.execute(f"DROP TABLE {src_schema}.{name}")
        self._conn.execute(f"DROP SCHEMA {src_schema}")

        if self.schema == src_schema:
            self.schema = dst_schema

        log.info(
            "local_rename_schema_complete",
            src_schema=src_schema,
            dst_schema=dst_schema,
            moved_count=len(tables),
        )
        return tables

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()

    def __enter__(self) -> LocalLakehouse:
        return self

    def __exit__(self, *args) -> None:
        self.close()
