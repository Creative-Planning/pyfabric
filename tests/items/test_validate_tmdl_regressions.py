"""Regressions for the DAX paren check on measures with internal blank lines (issue #148)."""

from pathlib import Path

from pyfabric.items.validate_tmdl import check_dax_paren_balance


def _write_table(item_dir: Path, body: str) -> None:
    tables = item_dir / "definition" / "tables"
    tables.mkdir(parents=True, exist_ok=True)
    (tables / "T.tmdl").write_text(body, encoding="utf-8")


class TestMeasureWithInternalBlankLines:
    def test_balanced_multiline_measure_with_blank_line_is_clean(self, tmp_path: Path):
        # The shape from issue #148: a blank line inside the expression, then a
        # RETURN, then the closing paren, then the measure's own properties.
        body = (
            "table T\n"
            "\n"
            "\tmeasure 'M' =\n"
            "\t\t\tSUMX (\n"
            "\t\t\t\tVALUES ( T[Col] ),\n"
            "\t\t\t\n"
            "\t\t\t\tVAR X = 1\n"
            "\t\t\t\tRETURN X\n"
            "\t\t\t)\n"
            "\t\tformatString: 0\n"
        )
        _write_table(tmp_path, body)
        assert check_dax_paren_balance(tmp_path) == []

    def test_unbalanced_measure_across_blank_line_is_still_reported(
        self, tmp_path: Path
    ):
        body = (
            "table T\n"
            "\n"
            "\tmeasure 'M' =\n"
            "\t\t\tSUMX (\n"
            "\n"
            "\t\t\t\tVALUES ( T[Col] )\n"
            "\t\tformatString: 0\n"
        )
        _write_table(tmp_path, body)
        issues = check_dax_paren_balance(tmp_path)
        assert len(issues) == 1
        assert "unclosed" in issues[0].message

    def test_blank_line_before_next_measure_ends_the_body(self, tmp_path: Path):
        body = (
            "table T\n\n\tmeasure 'A' = SUM ( T[X] )\n\n\tmeasure 'B' = SUM ( T[Y] )\n"
        )
        _write_table(tmp_path, body)
        assert check_dax_paren_balance(tmp_path) == []
