"""DuckDB-backed Spark session mock for local Fabric notebook testing.

Provides a drop-in replacement for PySpark's SparkSession that executes
SQL against DuckDB with automatic Delta table discovery. Supports:

- ``spark.sql("SELECT ...")`` with Delta table path rewriting
- ``spark.sql("SHOW TABLES IN <lakehouse>")``
- ``spark.catalog.listTables(dbName)``
- ``DataFrame.collect()``, ``.show()``, ``.count()``, iteration

Local lakehouse data is expected at::

    <lakehouse_root>/<lakehouse_name>/Tables/<table_name>/  (Delta format)

Usage::

    from pyfabric.testing.duckdb_spark import DuckDBSparkSession

    spark = DuckDBSparkSession(lakehouse_root=Path("./test_data"))
    df = spark.sql("SELECT * FROM my_lakehouse.my_table")
    print(df.count())
    spark.stop()
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import duckdb
import structlog

log = structlog.get_logger()


# ── Row and DataFrame ────────────────────────────────────────────────────────


class Row:
    """Minimal PySpark Row replacement."""

    __slots__ = ("_columns", "_values")

    def __init__(self, values: tuple[Any, ...], columns: list[str] | None = None):
        self._values = values
        self._columns = columns

    def __getitem__(self, index: int | str) -> Any:
        if isinstance(index, str):
            if self._columns is None:
                raise KeyError(f"Column access requires column names: {index}")
            return self._values[self._columns.index(index)]
        return self._values[index]

    def __repr__(self) -> str:
        if self._columns:
            pairs = ", ".join(
                f"{k}={v!r}" for k, v in zip(self._columns, self._values, strict=False)
            )
            return f"Row({pairs})"
        return f"Row{self._values}"

    def asDict(self) -> dict[str, Any]:
        """Convert to dict (PySpark Row compatibility)."""
        if self._columns is None:
            raise ValueError("Column names not available")
        return dict(zip(self._columns, self._values, strict=False))


class DataFrame:
    """Minimal PySpark DataFrame replacement wrapping a DuckDB result."""

    def __init__(self, result: duckdb.DuckDBPyConnection, columns: list[str]):
        self._result = result
        self._columns = columns

    def collect(self) -> list[Row]:
        """Return all rows as a list of Row objects."""
        return [Row(tuple(r), self._columns) for r in self._result.fetchall()]

    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Print rows in tabular format (PySpark-compatible output)."""
        rows = self._result.fetchmany(n)
        if not rows:
            print(f"Empty DataFrame with columns: {self._columns}")
            return
        widths = [
            max(len(c), max((len(str(r[i])) for r in rows), default=0))
            for i, c in enumerate(self._columns)
        ]
        header = " | ".join(
            c.ljust(w) for c, w in zip(self._columns, widths, strict=False)
        )
        print(header)
        print("-" * len(header))
        for row in rows:
            print(
                " | ".join(str(v).ljust(w) for v, w in zip(row, widths, strict=False))
            )

    def count(self) -> int:
        """Return the number of rows."""
        rows = self._result.fetchall()
        return len(rows)

    def toPandas(self) -> Any:
        """Convert to pandas DataFrame."""
        return self._result.df()

    def __iter__(self) -> Any:
        return iter(self.collect())


# ── Catalog ──────────────────────────────────────────────────────────────────


class TableInfo:
    """Minimal spark.catalog table info."""

    __slots__ = ("database", "isTemporary", "name")

    def __init__(self, database: str, name: str, *, isTemporary: bool = False):
        self.database = database
        self.name = name
        self.isTemporary = isTemporary

    def __repr__(self) -> str:
        return f"TableInfo(database={self.database!r}, name={self.name!r})"

    def __getitem__(self, index: int) -> Any:
        return (self.database, self.name, self.isTemporary)[index]


class Catalog:
    """Minimal spark.catalog replacement backed by local Delta directories."""

    def __init__(self, lakehouse_root: Path):
        self._root = lakehouse_root

    def listTables(self, dbName: str | None = None) -> list[TableInfo]:
        """List tables found as Delta directories under lakehouse_root."""
        results: list[TableInfo] = []
        search_roots: list[tuple[str, Path]] = []

        if dbName:
            for variant in [dbName, dbName.lower()]:
                tables_dir = self._root / variant / "Tables"
                if tables_dir.exists():
                    search_roots.append((variant, tables_dir))
        else:
            for child in sorted(self._root.iterdir()):
                if child.is_dir():
                    tables_dir = child / "Tables"
                    if tables_dir.exists():
                        search_roots.append((child.name, tables_dir))

        for db, tables_dir in search_roots:
            for entry in sorted(tables_dir.iterdir()):
                if entry.is_dir() and (entry / "_delta_log").exists():
                    results.append(TableInfo(database=db, name=entry.name))
                # Also check schema subdirectories (dbo/<table>)
                elif entry.is_dir():
                    for sub in sorted(entry.iterdir()):
                        if sub.is_dir() and (sub / "_delta_log").exists():
                            results.append(TableInfo(database=db, name=sub.name))

        return results

    def tableExists(self, tableName: str) -> bool:
        """Check if a table exists (searches all lakehouses)."""
        return any(info.name == tableName for info in self.listTables())


# ── Spark Session ────────────────────────────────────────────────────────────


class DuckDBSparkSession:
    """Drop-in replacement for PySpark SparkSession backed by DuckDB.

    Supports:
    - ``spark.sql("SELECT ...")`` with automatic Delta table rewriting
    - ``spark.sql("SHOW TABLES IN <lakehouse>")``
    - ``spark.catalog.listTables(dbName)``
    - ``spark.catalog.tableExists(tableName)``
    """

    def __init__(self, lakehouse_root: Path | None = None):
        self._root = lakehouse_root or Path.cwd() / "local_lakehouse"
        self._conn = duckdb.connect()
        self._conn.execute("INSTALL delta; LOAD delta;")
        self.catalog = Catalog(self._root)
        log.debug("DuckDBSparkSession initialized", lakehouse_root=str(self._root))

    def sql(self, query: str) -> DataFrame:
        """Execute a SQL query with automatic Fabric table reference rewriting."""
        translated = self._translate(query.strip())
        log.debug("SQL", original=query.strip()[:100], translated=translated[:100])
        result = self._conn.execute(translated)
        columns = [d[0] for d in result.description]
        return DataFrame(result, columns)

    def stop(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()
        log.debug("DuckDBSparkSession stopped")

    def _translate(self, query: str) -> str:
        """Rewrite Fabric/Spark SQL idioms to DuckDB-compatible SQL."""
        # SHOW TABLES IN <lakehouse>
        m = re.match(r"SHOW\s+TABLES\s+(?:IN\s+)?(\w+)", query, re.IGNORECASE)
        if m:
            return self._show_tables(m.group(1))

        # Rewrite <lakehouse>.<schema>.<table> or <lakehouse>.<table> references
        # to delta_scan() calls, but only when the path actually exists.
        def replace_ref(match: re.Match[str]) -> str:
            parts = match.group(0).split(".")
            if len(parts) == 3:
                lakehouse, schema, table = parts
                path = self._root / lakehouse / "Tables" / schema / table
            elif len(parts) == 2:
                lakehouse, table = parts
                path = self._root / lakehouse / "Tables" / table
            else:
                return match.group(0)
            if (path / "_delta_log").exists():
                return f"delta_scan('{path.as_posix()}')"
            return match.group(0)

        return re.sub(r"\b\w+\.\w+(?:\.\w+)?\b", replace_ref, query)

    def _show_tables(self, lakehouse: str) -> str:
        """Generate SQL that returns SHOW TABLES output for a lakehouse."""
        tables_dir = self._root / lakehouse / "Tables"
        if not tables_dir.exists():
            return "SELECT '' AS namespace, '' AS tableName WHERE 1=0"

        table_rows = []
        for entry in sorted(tables_dir.iterdir()):
            if entry.is_dir() and (entry / "_delta_log").exists():
                table_rows.append(
                    f"SELECT '{lakehouse}' AS namespace, '{entry.name}' AS tableName, false AS isTemporary"
                )
            # Schema subdirectories
            elif entry.is_dir():
                for sub in sorted(entry.iterdir()):
                    if sub.is_dir() and (sub / "_delta_log").exists():
                        table_rows.append(
                            f"SELECT '{lakehouse}' AS namespace, '{sub.name}' AS tableName, false AS isTemporary"
                        )

        if not table_rows:
            return "SELECT '' AS namespace, '' AS tableName WHERE 1=0"
        return " UNION ALL ".join(table_rows)
