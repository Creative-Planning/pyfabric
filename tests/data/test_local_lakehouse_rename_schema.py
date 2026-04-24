"""Tests for ``LocalLakehouse.rename_schema``.

DuckDB 1.5.x doesn't support ``ALTER SCHEMA ... RENAME``. The helper
implements the standard workaround (CREATE TABLE dst.x AS SELECT * FROM
src.x; DROP TABLE src.x; DROP SCHEMA src) so caller code is portable
local↔remote with the matching OneLake helper.
"""

from __future__ import annotations

import pytest

from pyfabric.data import Col, LocalLakehouse, TableDef


def _lh(tmp_path, schema="old"):
    return LocalLakehouse(
        db_path=tmp_path / "lh.duckdb", ws_id="w", lh_id="l", schema=schema
    )


class TestLocalRenameSchema:
    def test_renames_schema_with_rows_preserved(self, tmp_path):
        lh = _lh(tmp_path)
        lh.register(
            TableDef(
                name="widgets",
                columns=(
                    Col("id", "int", nullable=False, pk=True),
                    Col("name", "string"),
                ),
            )
        )
        lh.conn.execute("INSERT INTO old.widgets VALUES (1, 'alpha'), (2, 'beta')")

        lh.rename_schema("old", "new")

        schemas = {
            r[0]
            for r in lh.conn.execute(
                "SELECT schema_name FROM information_schema.schemata"
            ).fetchall()
        }
        assert "new" in schemas
        assert "old" not in schemas

        row_count = lh.conn.execute("SELECT COUNT(*) FROM new.widgets").fetchone()
        assert row_count is not None and row_count[0] == 2

    def test_moves_self_schema_attribute(self, tmp_path):
        lh = _lh(tmp_path)
        lh.register(
            TableDef(
                name="widgets",
                columns=(Col("id", "int", nullable=False, pk=True),),
            )
        )
        lh.rename_schema("old", "new")
        # After renaming the current schema, subsequent operations should
        # target 'new' without the caller having to reopen the handle.
        assert lh.schema == "new"
        assert "widgets" in lh.table_names()

    def test_self_schema_unchanged_when_renaming_a_different_schema(self, tmp_path):
        lh = _lh(tmp_path, schema="dbo")
        lh.conn.execute("CREATE SCHEMA staging")
        lh.conn.execute("CREATE TABLE staging.t (id INT)")
        lh.rename_schema("staging", "stage2")
        assert lh.schema == "dbo"
        assert {
            r[0]
            for r in lh.conn.execute(
                "SELECT schema_name FROM information_schema.schemata"
            ).fetchall()
        } >= {"dbo", "stage2"}

    def test_src_equals_dst_rejected(self, tmp_path):
        lh = _lh(tmp_path)
        lh.register(
            TableDef(
                name="widgets",
                columns=(Col("id", "int", nullable=False, pk=True),),
            )
        )
        with pytest.raises(ValueError, match="identical"):
            lh.rename_schema("old", "old")

    def test_missing_source_schema_raises(self, tmp_path):
        lh = _lh(tmp_path, schema="dbo")
        with pytest.raises(ValueError, match="does not exist"):
            lh.rename_schema("never_created", "new")
