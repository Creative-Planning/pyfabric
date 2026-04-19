"""Tests for TMDL-specific validation (name collisions etc.)."""

from pathlib import Path

from pyfabric.items.validate_tmdl import (
    check_name_collisions,
    parse_table_identifiers,
)


def _write_table(item_dir: Path, name: str, body: str) -> Path:
    tables = item_dir / "definition" / "tables"
    tables.mkdir(parents=True, exist_ok=True)
    p = tables / f"{name}.tmdl"
    p.write_text(body, encoding="utf-8")
    return p


# ── parse_table_identifiers ─────────────────────────────────────────────────


class TestParseTableIdentifiers:
    def test_quoted_measure_and_bare_column(self):
        body = (
            "table fact_x\n"
            "\tmeasure 'Coverage Status' = SELECTEDVALUE(...)\n"
            "\tcolumn status\n"
        )
        measures, columns = parse_table_identifiers(body)
        assert measures == {"coverage status"}
        assert columns == {"status"}

    def test_special_chars_in_measure_names(self):
        body = (
            "table f\n"
            "\tmeasure '# PDFs OK' =\n"
            "\t\tCALCULATE(...)\n"
            "\tmeasure 'Detection %' = SELECTEDVALUE(...)\n"
        )
        measures, _ = parse_table_identifiers(body)
        assert measures == {"# pdfs ok", "detection %"}

    def test_bare_measure_identifier(self):
        body = "table x\n\tmeasure CountFoo = COUNTROWS(x)\n"
        measures, _ = parse_table_identifiers(body)
        assert measures == {"countfoo"}

    def test_quoted_column(self):
        body = "table x\n\tcolumn 'Display Name'\n"
        _, columns = parse_table_identifiers(body)
        assert columns == {"display name"}

    def test_case_insensitive(self):
        body = "table x\n\tmeasure 'Status' = ...\n\tcolumn STATUS\n"
        measures, columns = parse_table_identifiers(body)
        assert measures == {"status"}
        assert columns == {"status"}
        assert measures & columns == {"status"}

    def test_empty_table(self):
        measures, columns = parse_table_identifiers("table empty\n")
        assert measures == set()
        assert columns == set()


# ── check_name_collisions ──────────────────────────────────────────────────


class TestCheckNameCollisions:
    def test_no_collisions_returns_empty(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(
            item,
            "fact_x",
            "table fact_x\n\tmeasure 'Coverage Status' = ...\n\tcolumn status\n",
        )
        assert check_name_collisions(item) == []

    def test_detects_case_insensitive_clash(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        path = _write_table(
            item,
            "fact_x",
            "table fact_x\n\tmeasure 'Status' = SELECTEDVALUE(...)\n\tcolumn status\n",
        )
        issues = check_name_collisions(item)
        assert len(issues) == 1
        assert issues[0].path == path
        assert "'status'" in issues[0].message
        assert "case-insensitive" in issues[0].message

    def test_multiple_collisions_in_one_file(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(
            item,
            "fact_x",
            "table f\n"
            "\tmeasure 'Status' = ...\n"
            "\tmeasure 'Region' = ...\n"
            "\tcolumn status\n"
            "\tcolumn region\n",
        )
        issues = check_name_collisions(item)
        assert len(issues) == 1
        # Both names present in the message
        assert "'status'" in issues[0].message
        assert "'region'" in issues[0].message

    def test_separate_files_reported_separately(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(item, "f1", "table f1\n\tmeasure 'A' = 1\n\tcolumn a\n")
        _write_table(item, "f2", "table f2\n\tmeasure 'B' = 1\n\tcolumn b\n")
        issues = check_name_collisions(item)
        assert len(issues) == 2
        names = {i.path.name for i in issues}
        assert names == {"f1.tmdl", "f2.tmdl"}

    def test_no_tables_dir_returns_empty(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        item.mkdir()
        # No definition/tables/ subdir
        assert check_name_collisions(item) == []

    def test_collision_between_tables_does_not_fire(self, tmp_path: Path):
        # Same name on different tables is allowed; only same-table collisions reject.
        item = tmp_path / "sm.SemanticModel"
        _write_table(item, "f1", "table f1\n\tmeasure 'Status' = 1\n")
        _write_table(item, "f2", "table f2\n\tcolumn status\n")
        assert check_name_collisions(item) == []
