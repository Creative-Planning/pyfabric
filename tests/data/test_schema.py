"""Unit tests for pyfabric.data.schema and LocalLakehouse.insert_typed.

The typed insert path exists to catch data-shape bugs at the DuckDB
boundary instead of at the Spark/Delta boundary in Fabric. The most
important regression guarded here is empty-string-for-int silently
coercing in DuckDB but failing later when the Delta writer sees a
mixed-type column.
"""

import datetime as dt

import pytest

from pyfabric.data import Col, LocalLakehouse, TableDef
from pyfabric.data.schema import (
    DUCKDB_TYPES,
    SPARK_TYPES,
    all_duckdb_ddl,
    all_spark_ddl,
)

# ── Col / TableDef basics ───────────────────────────────────────────────────


class TestColValidation:
    def test_rejects_unknown_type_key(self):
        with pytest.raises(ValueError, match="Unknown type_key 'decimal'"):
            Col("price", "decimal")

    def test_frozen_dataclass(self):
        c = Col("id", "int")
        with pytest.raises(Exception):  # noqa: B017 — dataclasses.FrozenInstanceError
            c.name = "other"

    def test_type_maps_are_aligned(self):
        assert set(SPARK_TYPES) == set(DUCKDB_TYPES)


class TestTableDefIntrospection:
    def test_column_names_and_pks(self):
        t = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False, pk=True),
                Col("name", "string", nullable=False),
                Col("price", "double"),
            ),
        )
        assert t.column_names() == ["id", "name", "price"]
        assert t.pk_columns() == ["id"]

    def test_column_lookup(self):
        t = TableDef(
            name="widgets",
            columns=(Col("id", "int"), Col("name", "string")),
        )
        assert t.column("name").type_key == "string"
        with pytest.raises(KeyError):
            t.column("missing")


class TestDDLGeneration:
    def test_spark_ddl_shape(self):
        t = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False),
                Col("name", "string"),
            ),
        )
        ddl = t.to_spark_ddl(schema="dbo")
        assert "CREATE TABLE IF NOT EXISTS dbo.widgets" in ddl
        assert "`id` INT NOT NULL" in ddl
        assert "`name` STRING" in ddl
        assert ddl.rstrip().endswith("USING DELTA")

    def test_duckdb_ddl_shape(self):
        t = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False),
                Col("name", "string"),
                Col("created_at", "timestamp"),
            ),
        )
        ddl = t.to_duckdb_ddl(schema="ddb")
        assert "CREATE TABLE IF NOT EXISTS ddb.widgets" in ddl
        assert "id INTEGER NOT NULL" in ddl
        assert "name VARCHAR" in ddl
        assert "created_at TIMESTAMP" in ddl

    def test_duckdb_ddl_without_schema(self):
        t = TableDef(name="widgets", columns=(Col("id", "int"),))
        ddl = t.to_duckdb_ddl()
        assert "CREATE TABLE IF NOT EXISTS widgets" in ddl

    def test_arrow_schema_matches_columns(self):
        pa = pytest.importorskip("pyarrow")
        t = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False),
                Col("amount", "bigint"),
                Col("price", "double"),
                Col("when", "timestamp"),
                Col("active", "boolean"),
            ),
        )
        schema = t.to_arrow_schema()
        assert schema.field("id").type == pa.int32()
        assert not schema.field("id").nullable
        assert schema.field("amount").type == pa.int64()
        assert schema.field("price").type == pa.float64()
        assert schema.field("when").type == pa.timestamp("us")
        assert schema.field("active").type == pa.bool_()

    def test_all_ddl_helpers(self):
        tables = (
            TableDef(name="a", columns=(Col("id", "int"),)),
            TableDef(name="b", columns=(Col("id", "int"),)),
        )
        assert len(all_spark_ddl(tables)) == 2
        assert len(all_duckdb_ddl(tables, schema="ddb")) == 2


# ── Row validation (the core regression guards) ────────────────────────────


class TestRowValidation:
    def test_empty_string_rejected_for_int_column(self):
        """The MGC regression — ``""`` for an int column must not pass."""
        t = TableDef(
            name="reports",
            columns=(
                Col("id", "int", nullable=False),
                Col("projection_number", "int"),
            ),
        )
        errs = t.validate_row({"id": 1, "projection_number": ""})
        assert any("empty string" in e for e in errs)
        assert any("projection_number" in e for e in errs)

    def test_empty_string_accepted_for_string_column(self):
        t = TableDef(name="x", columns=(Col("note", "string"),))
        assert t.validate_row({"note": ""}) == []

    def test_none_rejected_for_non_nullable(self):
        t = TableDef(name="x", columns=(Col("id", "int", nullable=False),))
        errs = t.validate_row({"id": None})
        assert errs == ["column 'id' is NOT NULL but got None"]

    def test_missing_column_rejected_for_non_nullable(self):
        t = TableDef(name="x", columns=(Col("id", "int", nullable=False),))
        errs = t.validate_row({})
        assert errs == ["missing required column 'id'"]

    def test_missing_nullable_column_is_ok(self):
        t = TableDef(
            name="x",
            columns=(Col("id", "int", nullable=False), Col("note", "string")),
        )
        assert t.validate_row({"id": 1}) == []

    def test_bool_rejected_for_int_column(self):
        t = TableDef(name="x", columns=(Col("count", "int"),))
        errs = t.validate_row({"count": True})
        assert errs and "bool" in errs[0]

    def test_wrong_type_rejected(self):
        t = TableDef(name="x", columns=(Col("price", "double"),))
        errs = t.validate_row({"price": "12.50"})
        assert errs and "expected" in errs[0]

    def test_date_and_timestamp_accept_native_objects(self):
        t = TableDef(
            name="x",
            columns=(
                Col("when_date", "date"),
                Col("when_ts", "timestamp"),
            ),
        )
        errs = t.validate_row(
            {
                "when_date": dt.date(2026, 4, 16),
                "when_ts": dt.datetime(2026, 4, 16, 12, 0, 0),
            }
        )
        assert errs == []

    def test_date_string_rejected(self):
        """Strings where date/datetime is expected must be caught."""
        t = TableDef(name="x", columns=(Col("when", "date"),))
        errs = t.validate_row({"when": "2026-04-16"})
        assert errs and "date" in errs[0]


# ── LocalLakehouse.register + insert_typed ──────────────────────────────────


@pytest.fixture
def local_lh(tmp_path):
    duckdb = pytest.importorskip("duckdb")  # noqa: F841 — trigger skip if missing
    lh = LocalLakehouse(
        db_path=tmp_path / "test.duckdb",
        ws_id="ws",
        lh_id="lh",
        schema="ddb",
    )
    yield lh
    lh.close()


@pytest.fixture
def widgets_table():
    return TableDef(
        name="widgets",
        columns=(
            Col("id", "int", nullable=False, pk=True),
            Col("name", "string", nullable=False),
            Col("price", "double"),
            Col("projection_number", "int"),
            Col("is_active", "boolean"),
            Col("when_ts", "timestamp"),
        ),
    )


class TestRegister:
    def test_register_creates_table(self, local_lh, widgets_table):
        count = local_lh.register(widgets_table)
        assert count == 1
        assert "widgets" in local_lh.table_names()
        assert "widgets" in local_lh.registered_tables()

    def test_register_tuple(self, local_lh):
        tables = (
            TableDef(name="a", columns=(Col("id", "int"),)),
            TableDef(name="b", columns=(Col("id", "int"),)),
        )
        assert local_lh.register(tables) == 2
        assert set(local_lh.registered_tables()) == {"a", "b"}

    def test_register_is_idempotent(self, local_lh, widgets_table):
        local_lh.register(widgets_table)
        local_lh.register(widgets_table)  # should not raise
        assert len(local_lh.registered_tables()) == 1


class TestInsertTyped:
    def test_happy_path(self, local_lh, widgets_table):
        local_lh.register(widgets_table)
        n = local_lh.insert_typed(
            "widgets",
            [
                {
                    "id": 1,
                    "name": "alpha",
                    "price": 9.99,
                    "projection_number": 3,
                    "is_active": True,
                    "when_ts": dt.datetime(2026, 4, 16, 12, 0, 0),
                },
            ],
        )
        assert n == 1
        assert local_lh.row_count("widgets") == 1

    def test_rejects_empty_string_for_int(self, local_lh, widgets_table):
        """MGC regression — empty string for int projection_number must raise."""
        local_lh.register(widgets_table)
        with pytest.raises(ValueError, match="empty string") as exc_info:
            local_lh.insert_typed(
                "widgets",
                [
                    {
                        "id": 1,
                        "name": "alpha",
                        "projection_number": "",
                    }
                ],
            )
        assert "projection_number" in str(exc_info.value)
        assert local_lh.row_count("widgets") == 0

    def test_rejects_none_for_non_nullable(self, local_lh, widgets_table):
        local_lh.register(widgets_table)
        with pytest.raises(ValueError, match="NOT NULL"):
            local_lh.insert_typed(
                "widgets",
                [{"id": 1, "name": None}],
            )

    def test_rejects_unknown_columns(self, local_lh, widgets_table):
        local_lh.register(widgets_table)
        with pytest.raises(ValueError, match="unknown columns"):
            local_lh.insert_typed(
                "widgets",
                [{"id": 1, "name": "alpha", "bogus": 42}],
            )

    def test_rejects_unregistered_table(self, local_lh):
        with pytest.raises(KeyError, match="not registered"):
            local_lh.insert_typed("widgets", [{"id": 1}])

    def test_empty_rows_is_noop(self, local_lh, widgets_table):
        local_lh.register(widgets_table)
        assert local_lh.insert_typed("widgets", []) == 0

    def test_error_message_lists_all_bad_rows(self, local_lh, widgets_table):
        local_lh.register(widgets_table)
        with pytest.raises(ValueError) as exc_info:
            local_lh.insert_typed(
                "widgets",
                [
                    {"id": 1, "name": "ok"},
                    {"id": "bad", "name": "also-bad", "projection_number": ""},
                ],
            )
        msg = str(exc_info.value)
        assert "row[1]" in msg
        assert "projection_number" in msg

    def test_datetime_not_stringified(self, local_lh, widgets_table):
        """insert_typed should preserve datetime semantics (no auto-isoformat)."""
        local_lh.register(widgets_table)
        ts = dt.datetime(2026, 4, 16, 12, 34, 56)
        local_lh.insert_typed(
            "widgets",
            [{"id": 1, "name": "alpha", "when_ts": ts}],
        )
        row = local_lh.conn.execute(
            "SELECT when_ts FROM ddb.widgets WHERE id = 1"
        ).fetchone()
        assert row is not None
        # DuckDB returns a datetime for TIMESTAMP columns — not a string.
        assert isinstance(row[0], dt.datetime)
        assert row[0] == ts
