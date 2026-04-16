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
from typing import TYPE_CHECKING

import structlog

from pyfabric.data.schema import TableDef

if TYPE_CHECKING:
    import duckdb as duckdb_mod
    import pandas as pd
    import pyarrow as pa

    from pyfabric.client.auth import FabricCredential

log = structlog.get_logger()


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

    def register(self, tables: TableDef | tuple[TableDef, ...] | list[TableDef]) -> int:
        """Register one or more TableDefs and create the DuckDB tables.

        Registered tables are used by ``insert_typed`` to validate rows
        before insert. Existing tables are not dropped — DDL uses
        ``CREATE TABLE IF NOT EXISTS``.

        Returns the number of tables registered.
        """
        if isinstance(tables, TableDef):
            tables = (tables,)

        for table in tables:
            self._tables[table.name] = table
            self._conn.execute(table.to_duckdb_ddl(self.schema))
        return len(tables)

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
    ) -> int:
        """Push a single DuckDB table to OneLake as a Delta table.

        Writes to ``Tables/{schema}/{table_name}/`` in the lakehouse.

        Args:
            credential: FabricCredential for storage token.
            table_name: Table name (without schema prefix).
            mode:       "overwrite" or "append".

        Returns:
            Number of rows written.
        """
        from pyfabric.data.lakehouse import write_table

        # .arrow() returns RecordBatchReader; .read_all() materializes to Table
        reader = self._conn.execute(f"SELECT * FROM {self.schema}.{table_name}").arrow()
        arrow_table = reader.read_all() if hasattr(reader, "read_all") else reader

        if arrow_table.num_rows == 0:
            log.info("Skipping empty table: %s", table_name)
            return 0

        result = write_table(
            credential,
            self.ws_id,
            self.lh_id,
            table_name,
            arrow_table,
            schema=self.schema,
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
    ) -> dict[str, int]:
        """Push all non-empty tables (or a subset) to OneLake.

        Args:
            credential: FabricCredential for storage token.
            mode:       "overwrite" or "append".
            tables:     Optional list of table names to push. If None, pushes all.

        Returns:
            Dict of {table_name: rows_written} for tables that were pushed.
        """
        target_tables = tables or self.table_names()
        results = {}

        for name in target_tables:
            count = self.row_count(name)
            if count == 0:
                continue
            log.info("Pushing %s (%d rows)", name, count)
            try:
                written = self.push_table(credential, name, mode=mode)
                results[name] = written
            except Exception as e:
                log.error("Failed to push %s: %s", name, e)
                results[name] = -1  # signal failure

        total = sum(v for v in results.values() if v > 0)
        log.info(
            "Push complete: %d rows across %d tables",
            total,
            sum(1 for v in results.values() if v > 0),
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

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()

    def __enter__(self) -> LocalLakehouse:
        return self

    def __exit__(self, *args) -> None:
        self.close()
