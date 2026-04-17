"""Tests for validate_duckdb_schema and validate_arrow_schema."""

import duckdb
import pyarrow as pa
import pytest

from pyfabric.data.schema import (
    Col,
    TableDef,
    validate_arrow_schema,
    validate_duckdb_schema,
)


@pytest.fixture
def products() -> TableDef:
    return TableDef(
        name="products",
        columns=(
            Col("id", "int", nullable=False, pk=True),
            Col("name", "string", nullable=False),
            Col("price", "double"),
            Col("active", "boolean"),
        ),
    )


# ── DuckDB ──────────────────────────────────────────────────────────────────


class TestValidateDuckDB:
    def test_matching_table(self, products):
        conn = duckdb.connect(":memory:")
        conn.execute(products.to_duckdb_ddl())
        assert validate_duckdb_schema(products, conn) == []

    def test_table_in_schema(self, products):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA ddb")
        conn.execute(products.to_duckdb_ddl(schema="ddb"))
        assert validate_duckdb_schema(products, conn, schema="ddb") == []

    def test_missing_table(self, products):
        conn = duckdb.connect(":memory:")
        errors = validate_duckdb_schema(products, conn)
        assert any("not found" in e for e in errors)

    def test_missing_column(self, products):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE products (id INTEGER, name VARCHAR)")
        errors = validate_duckdb_schema(products, conn)
        assert any("missing column 'price'" in e for e in errors)
        assert any("missing column 'active'" in e for e in errors)

    def test_type_mismatch(self, products):
        conn = duckdb.connect(":memory:")
        conn.execute(
            "CREATE TABLE products "
            "(id VARCHAR, name VARCHAR, price DOUBLE, active BOOLEAN)"
        )
        errors = validate_duckdb_schema(products, conn)
        assert any("'id' type mismatch" in e for e in errors)

    def test_extra_column(self, products):
        conn = duckdb.connect(":memory:")
        conn.execute(
            "CREATE TABLE products "
            "(id INTEGER, name VARCHAR, price DOUBLE, active BOOLEAN, notes VARCHAR)"
        )
        errors = validate_duckdb_schema(products, conn)
        assert any("unexpected column 'notes'" in e for e in errors)


# ── PyArrow ─────────────────────────────────────────────────────────────────


class TestValidateArrow:
    def test_matching_schema(self, products):
        assert validate_arrow_schema(products, products.to_arrow_schema()) == []

    def test_missing_column(self, products):
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
            ]
        )
        errors = validate_arrow_schema(products, schema)
        assert any("missing column 'price'" in e for e in errors)

    def test_type_mismatch(self, products):
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),  # wrong: should be int32
                pa.field("name", pa.string()),
                pa.field("price", pa.float64()),
                pa.field("active", pa.bool_()),
            ]
        )
        errors = validate_arrow_schema(products, schema)
        assert any("'id' type mismatch" in e for e in errors)

    def test_timestamp_unit_does_not_matter(self):
        """timestamp[us] vs timestamp[ms] both map to 'timestamp'."""
        td = TableDef(
            name="events",
            columns=(Col("ts", "timestamp", nullable=False),),
        )
        schema = pa.schema([pa.field("ts", pa.timestamp("ms"))])
        assert validate_arrow_schema(td, schema) == []

    def test_extra_column(self, products):
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("price", pa.float64()),
                pa.field("active", pa.bool_()),
                pa.field("extra", pa.string()),
            ]
        )
        errors = validate_arrow_schema(products, schema)
        assert any("unexpected column 'extra'" in e for e in errors)
