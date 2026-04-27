"""Tests for ``pyfabric.testing.fixtures.attach_duckdb_lakehouse``.

The helper attaches a parquet-backed catalog to a DuckDB connection so
test code can write ``FROM <lh>.<schema>.<table>`` against fixture data
without re-implementing the ``ATTACH`` + ``CREATE SCHEMA`` + ``CREATE VIEW``
boilerplate in every wheel/SqlConn test suite.

Tests use real DuckDB and real (tiny) parquet files — no mocks needed.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyfabric.testing.fixtures import attach_duckdb_lakehouse


def _write_parquet(path: Path, **columns: list) -> Path:
    """Write a tiny parquet file from kwargs (column_name=values)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), str(path))
    return path


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


class TestSingleSchemaSingleTable:
    def test_resolves_three_part_qualified_name(self, tmp_path, con):
        parquet = _write_parquet(
            tmp_path / "dim_customer.parquet",
            id=[1, 2, 3],
            name=["a", "b", "c"],
        )
        attach_duckdb_lakehouse(
            con,
            "lh_silver",
            schemas={"bc2": {"dim_customer": parquet}},
        )
        rows = con.sql(
            "SELECT id, name FROM lh_silver.bc2.dim_customer ORDER BY id"
        ).fetchall()
        assert rows == [(1, "a"), (2, "b"), (3, "c")]

    def test_accepts_string_paths(self, tmp_path, con):
        parquet = _write_parquet(tmp_path / "t.parquet", x=[1])
        attach_duckdb_lakehouse(
            con,
            "lh",
            schemas={"s": {"t": str(parquet)}},
        )
        assert con.sql("FROM lh.s.t").fetchall() == [(1,)]


class TestMultipleSchemasAndTables:
    def test_multiple_schemas_each_with_multiple_tables(self, tmp_path, con):
        a = _write_parquet(tmp_path / "a.parquet", x=[1, 2])
        b = _write_parquet(tmp_path / "b.parquet", y=[10])
        c = _write_parquet(tmp_path / "c.parquet", z=[100, 200, 300])
        attach_duckdb_lakehouse(
            con,
            "lh_bc_silver",
            schemas={
                "bc2": {"dim_customer": a, "fact_sales": b},
                "shared": {"dim_date": c},
            },
        )
        assert con.sql("FROM lh_bc_silver.bc2.dim_customer").fetchall() == [(1,), (2,)]
        assert con.sql("FROM lh_bc_silver.bc2.fact_sales").fetchall() == [(10,)]
        assert con.sql("FROM lh_bc_silver.shared.dim_date").fetchall() == [
            (100,),
            (200,),
            (300,),
        ]

    def test_returns_attached_database_name(self, tmp_path, con):
        parquet = _write_parquet(tmp_path / "t.parquet", x=[1])
        result = attach_duckdb_lakehouse(
            con,
            "lh_returned",
            schemas={"s": {"t": parquet}},
        )
        # Returning the catalog name lets callers chain or compose.
        assert result == "lh_returned"


class TestEmptyAndIdempotency:
    def test_empty_schemas_dict_creates_empty_catalog(self, con):
        attach_duckdb_lakehouse(con, "lh_empty", schemas={})
        # Catalog should exist; querying any table fails clearly.
        catalogs = {
            r[0]
            for r in con.sql(
                "SELECT catalog_name FROM information_schema.schemata"
            ).fetchall()
        }
        assert "lh_empty" in catalogs

    def test_attaching_two_distinct_lakehouses_is_supported(self, tmp_path, con):
        a = _write_parquet(tmp_path / "a.parquet", x=[1])
        b = _write_parquet(tmp_path / "b.parquet", y=[2])
        attach_duckdb_lakehouse(con, "lh_a", schemas={"s": {"t": a}})
        attach_duckdb_lakehouse(con, "lh_b", schemas={"s": {"t": b}})
        assert con.sql("FROM lh_a.s.t").fetchall() == [(1,)]
        assert con.sql("FROM lh_b.s.t").fetchall() == [(2,)]


class TestErrorPaths:
    def test_missing_parquet_file_raises_clearly(self, tmp_path, con):
        bogus = tmp_path / "does_not_exist.parquet"
        with pytest.raises((FileNotFoundError, duckdb.IOException)):
            attach_duckdb_lakehouse(
                con,
                "lh",
                schemas={"s": {"t": bogus}},
            )

    def test_invalid_lakehouse_name_rejected(self, tmp_path, con):
        # SQL identifiers can't contain quotes/semicolons — reject before
        # we hand the name to the ATTACH statement.
        parquet = _write_parquet(tmp_path / "t.parquet", x=[1])
        with pytest.raises(ValueError, match="lakehouse_name"):
            attach_duckdb_lakehouse(
                con,
                "lh; DROP TABLE x",
                schemas={"s": {"t": parquet}},
            )
