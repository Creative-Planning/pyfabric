"""Tests for ``LocalLakehouse.register`` schema-drift detection and
``evolve_schema`` additive-migration support.

The underlying bug: ``register()`` uses ``CREATE TABLE IF NOT EXISTS``, so
when a caller reopens a pre-existing DuckDB file with a newer ``TableDef``
(extra columns added), the table silently keeps the old shape. Subsequent
inserts drop the new columns without warning. The fix surfaces the drift
explicitly (raise by default) and offers an opt-in evolve path.
"""

from __future__ import annotations

import pytest

from pyfabric.data import Col, LocalLakehouse, TableDef
from pyfabric.data.local_lakehouse import LocalLakehouseSchemaDrift


def _reopen(db_path, schema="dbo"):
    return LocalLakehouse(db_path=db_path, ws_id="w", lh_id="l", schema=schema)


class TestRegisterDriftRaisesByDefault:
    def test_register_raises_on_column_drift(self, tmp_path):
        db = tmp_path / "drift.duckdb"
        with _reopen(db) as lh:
            v1 = TableDef(
                name="widgets",
                columns=(
                    Col("id", "int", nullable=False, pk=True),
                    Col("name", "string"),
                ),
            )
            lh.register(v1)

        with _reopen(db) as lh:
            v2 = TableDef(
                name="widgets",
                columns=(
                    Col("id", "int", nullable=False, pk=True),
                    Col("name", "string"),
                    Col("color", "string"),
                ),
            )
            with pytest.raises(LocalLakehouseSchemaDrift) as exc:
                lh.register(v2)
        msg = str(exc.value)
        assert "widgets" in msg
        assert "color" in msg

    def test_drift_exception_lists_all_missing_columns(self, tmp_path):
        db = tmp_path / "drift_multi.duckdb"
        with _reopen(db) as lh:
            lh.register(
                TableDef(
                    name="widgets",
                    columns=(Col("id", "int", nullable=False, pk=True),),
                )
            )

        v2 = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False, pk=True),
                Col("name", "string"),
                Col("color", "string"),
                Col("qty", "int"),
            ),
        )
        with _reopen(db) as lh, pytest.raises(LocalLakehouseSchemaDrift) as exc:
            lh.register(v2)
        msg = str(exc.value)
        for added in ("name", "color", "qty"):
            assert added in msg

    def test_register_no_raise_when_schema_matches(self, tmp_path):
        db = tmp_path / "aligned.duckdb"
        t = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False, pk=True),
                Col("name", "string"),
            ),
        )
        with _reopen(db) as lh:
            lh.register(t)
        # Re-registering the same TableDef must be a no-op, not a raise.
        with _reopen(db) as lh:
            assert lh.register(t) == 1


class TestRegisterOnDriftIgnore:
    def test_ignore_preserves_old_silent_behaviour(self, tmp_path):
        db = tmp_path / "ignore.duckdb"
        with _reopen(db) as lh:
            lh.register(
                TableDef(
                    name="widgets",
                    columns=(
                        Col("id", "int", nullable=False, pk=True),
                        Col("name", "string"),
                    ),
                )
            )

        v2 = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False, pk=True),
                Col("name", "string"),
                Col("color", "string"),
            ),
        )
        with _reopen(db) as lh:
            # No raise — caller has explicitly opted out of drift detection.
            assert lh.register(v2, on_drift="ignore") == 1
            # Table still has only the original two columns.
            cols = [
                r[0]
                for r in lh.conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'dbo' AND table_name = 'widgets' "
                    "ORDER BY ordinal_position"
                ).fetchall()
            ]
            assert cols == ["id", "name"]


class TestRegisterOnDriftEvolve:
    def test_evolve_adds_missing_columns(self, tmp_path):
        db = tmp_path / "evolve_flag.duckdb"
        with _reopen(db) as lh:
            lh.register(
                TableDef(
                    name="widgets",
                    columns=(
                        Col("id", "int", nullable=False, pk=True),
                        Col("name", "string"),
                    ),
                )
            )
            lh.conn.execute("INSERT INTO dbo.widgets VALUES (1, 'alpha')")

        v2 = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False, pk=True),
                Col("name", "string"),
                Col("color", "string"),
            ),
        )
        with _reopen(db) as lh:
            assert lh.register(v2, on_drift="evolve") == 1
            cols = [
                r[0]
                for r in lh.conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'dbo' AND table_name = 'widgets' "
                    "ORDER BY ordinal_position"
                ).fetchall()
            ]
            assert cols == ["id", "name", "color"]
            row = lh.conn.execute("SELECT id, name, color FROM dbo.widgets").fetchone()
            assert row == (1, "alpha", None)


class TestEvolveSchemaMethod:
    def test_adds_missing_columns_preserving_existing_rows(self, tmp_path):
        db = tmp_path / "evolve.duckdb"
        with _reopen(db) as lh:
            lh.register(
                TableDef(
                    name="widgets",
                    columns=(
                        Col("id", "int", nullable=False, pk=True),
                        Col("name", "string"),
                    ),
                )
            )
            lh.conn.execute("INSERT INTO dbo.widgets VALUES (1, 'alpha')")

        v2 = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False, pk=True),
                Col("name", "string"),
                Col("color", "string"),
            ),
        )
        with _reopen(db) as lh:
            lh.evolve_schema(v2)
            cols = [
                r[0]
                for r in lh.conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'dbo' AND table_name = 'widgets' "
                    "ORDER BY ordinal_position"
                ).fetchall()
            ]
            assert cols == ["id", "name", "color"]
            row = lh.conn.execute("SELECT id, name, color FROM dbo.widgets").fetchone()
            assert row == (1, "alpha", None)

    def test_creates_missing_tables(self, tmp_path):
        db = tmp_path / "create_missing.duckdb"
        with _reopen(db) as lh:
            # No prior registration — evolve should create the table.
            lh.evolve_schema(
                TableDef(
                    name="gizmos",
                    columns=(
                        Col("id", "int", nullable=False, pk=True),
                        Col("label", "string"),
                    ),
                )
            )
            assert "gizmos" in lh.table_names()

    def test_no_op_when_aligned(self, tmp_path):
        db = tmp_path / "noop.duckdb"
        t = TableDef(
            name="widgets",
            columns=(
                Col("id", "int", nullable=False, pk=True),
                Col("name", "string"),
            ),
        )
        with _reopen(db) as lh:
            lh.register(t)
            lh.conn.execute("INSERT INTO dbo.widgets VALUES (1, 'alpha')")

        with _reopen(db) as lh:
            lh.evolve_schema(t)  # no drift — must not error
            cols = [
                r[0]
                for r in lh.conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'dbo' AND table_name = 'widgets' "
                    "ORDER BY ordinal_position"
                ).fetchall()
            ]
            assert cols == ["id", "name"]

    def test_accepts_sequence_of_tables(self, tmp_path):
        db = tmp_path / "evolve_many.duckdb"
        a = TableDef(
            name="a",
            columns=(Col("id", "int", nullable=False, pk=True),),
        )
        b = TableDef(
            name="b",
            columns=(Col("id", "int", nullable=False, pk=True),),
        )
        with _reopen(db) as lh:
            lh.evolve_schema([a, b])
            assert set(lh.table_names()) >= {"a", "b"}


class TestRegisterOnDriftInvalid:
    def test_rejects_unknown_on_drift_value(self, tmp_path):
        db = tmp_path / "bad_flag.duckdb"
        with _reopen(db) as lh, pytest.raises(ValueError, match="on_drift"):
            lh.register(
                TableDef(
                    name="widgets",
                    columns=(Col("id", "int", nullable=False, pk=True),),
                ),
                on_drift="wat",  # type: ignore[arg-type]
            )


class TestEvolveSchemaDuckTypedTableDef:
    """``evolve_schema`` should accept any object exposing ``.name``,
    ``.columns: Iterable[Col]``, and ``.to_duckdb_ddl(schema)`` — not
    require the in-pyfabric ``TableDef.column(name)`` helper. Downstream
    packages that built TableDef-like classes against the earlier API
    surface (columns + to_duckdb_ddl + column_names) hit AttributeError
    when ``evolve_schema`` calls ``table.column(col_name)``.
    """

    def test_evolve_schema_accepts_ducktyped_table_def(self, tmp_path):
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class _DuckTableDef:
            """Minimal TableDef-like class — no ``.column(name)`` method."""

            name: str
            columns: tuple[Col, ...]

            def to_duckdb_ddl(self, schema: str | None = None) -> str:
                from pyfabric.data.schema import DUCKDB_TYPES

                lines = []
                for c in self.columns:
                    duck_type = DUCKDB_TYPES[c.type_key]
                    null = "" if c.nullable else " NOT NULL"
                    lines.append(f"  {c.name} {duck_type}{null}")
                cols = ",\n".join(lines)
                qualified = f"{schema}.{self.name}" if schema else self.name
                return f"CREATE TABLE IF NOT EXISTS {qualified} (\n{cols}\n)"

        db = tmp_path / "duck.duckdb"
        with _reopen(db) as lh:
            v1 = _DuckTableDef(
                name="widgets",
                columns=(
                    Col("id", "int", nullable=False, pk=True),
                    Col("name", "string"),
                ),
            )
            # Bootstrap the table via execute_ddl since register() doesn't
            # accept duck-typed defs (it stores them in self._tables for
            # insert_typed validation, which is fine to skip).
            lh.execute_ddl([v1.to_duckdb_ddl(lh.schema)])

        with _reopen(db) as lh:
            v2 = _DuckTableDef(
                name="widgets",
                columns=(
                    Col("id", "int", nullable=False, pk=True),
                    Col("name", "string"),
                    Col("color", "string"),  # NEW
                ),
            )
            n_altered = lh.evolve_schema([v2])  # must not raise AttributeError
            assert n_altered == 1
            cols = [
                r[0]
                for r in lh.conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'dbo' AND table_name = 'widgets' "
                    "ORDER BY ordinal_position"
                ).fetchall()
            ]
            assert cols == ["id", "name", "color"]
