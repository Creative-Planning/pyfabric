"""Tests for TMDL-specific validation (name collisions etc.)."""

from pathlib import Path

from pyfabric.items.validate_tmdl import (
    check_compatibility_level,
    check_dax_paren_balance,
    check_lineage_tag_uniqueness,
    check_name_collisions,
    check_orphan_columns,
    lint_semantic_model,
    parse_table_identifiers,
)


def _write_table(item_dir: Path, name: str, body: str) -> Path:
    tables = item_dir / "definition" / "tables"
    tables.mkdir(parents=True, exist_ok=True)
    p = tables / f"{name}.tmdl"
    p.write_text(body, encoding="utf-8")
    return p


def _write_definition_file(item_dir: Path, rel: str, body: str) -> Path:
    p = item_dir / "definition" / rel
    p.parent.mkdir(parents=True, exist_ok=True)
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


# ── check_compatibility_level ───────────────────────────────────────────────


class TestCheckCompatibilityLevel:
    def test_baseline_level_passes(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_definition_file(
            item, "database.tmdl", "database\n\tcompatibilityLevel: 1567"
        )
        assert check_compatibility_level(item) == []

    def test_missing_level_is_warning(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_definition_file(item, "database.tmdl", "database")
        issues = check_compatibility_level(item)
        assert len(issues) == 1
        assert issues[0].severity == "warning"
        assert "no compatibilityLevel" in issues[0].message

    def test_below_baseline_is_warning(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_definition_file(
            item, "database.tmdl", "database\n\tcompatibilityLevel: 1550"
        )
        issues = check_compatibility_level(item)
        assert len(issues) == 1
        assert issues[0].severity == "warning"
        assert "1550" in issues[0].message

    def test_directlake_below_1604_is_error(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_definition_file(
            item, "database.tmdl", "database\n\tcompatibilityLevel: 1567"
        )
        _write_table(
            item,
            "fact_x",
            "table fact_x\n\tpartition fact_x = entity\n\t\tmode: directLake\n",
        )
        issues = check_compatibility_level(item)
        assert len(issues) == 1
        assert issues[0].severity == "error"
        assert "1604" in issues[0].message

    def test_directlake_at_1604_passes(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_definition_file(
            item, "database.tmdl", "database\n\tcompatibilityLevel: 1604"
        )
        _write_table(
            item,
            "fact_x",
            "table fact_x\n\tpartition fact_x = entity\n\t\tmode: directLake\n",
        )
        assert check_compatibility_level(item) == []

    def test_missing_database_file_returns_empty(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        item.mkdir()
        assert check_compatibility_level(item) == []


# ── check_orphan_columns ────────────────────────────────────────────────────


class TestCheckOrphanColumns:
    def _model(self, tmp_path: Path) -> Path:
        item = tmp_path / "sm.SemanticModel"
        _write_table(
            item,
            "dim_section",
            "table dim_section\n\tcolumn section_key\n\tcolumn display_name\n",
        )
        return item

    def test_relationship_to_declared_column_passes(self, tmp_path: Path):
        item = self._model(tmp_path)
        _write_table(item, "fact_x", "table fact_x\n\tcolumn section_key\n")
        _write_definition_file(
            item,
            "relationships.tmdl",
            "relationship r1\n"
            "\tfromColumn: fact_x.section_key\n"
            "\ttoColumn: dim_section.section_key\n",
        )
        assert check_orphan_columns(item) == []

    def test_relationship_to_unknown_column_is_warning(self, tmp_path: Path):
        item = self._model(tmp_path)
        _write_definition_file(
            item,
            "relationships.tmdl",
            "relationship r1\n"
            "\tfromColumn: dim_section.nope\n"
            "\ttoColumn: dim_section.section_key\n",
        )
        issues = check_orphan_columns(item)
        assert len(issues) == 1
        assert issues[0].severity == "warning"
        assert "'dim_section'[nope]" in issues[0].message

    def test_quoted_relationship_endpoints(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(item, "dim", "table 'My Dim'\n\tcolumn 'The Key'\n")
        _write_definition_file(
            item,
            "relationships.tmdl",
            "relationship r1\n\tfromColumn: 'My Dim'.'Missing Col'\n",
        )
        issues = check_orphan_columns(item)
        assert len(issues) == 1
        assert "'My Dim'[Missing Col]" in issues[0].message

    def test_measure_dax_reference_to_unknown_column(self, tmp_path: Path):
        item = self._model(tmp_path)
        _write_table(
            item,
            "fact_x",
            "table fact_x\n"
            "\tmeasure 'Bad' = DISTINCTCOUNT('dim_section'[ghost])\n"
            "\tcolumn ok_col\n",
        )
        issues = check_orphan_columns(item)
        assert len(issues) == 1
        assert issues[0].severity == "warning"
        assert "measure 'Bad'" in issues[0].message
        assert "'dim_section'[ghost]" in issues[0].message

    def test_reference_to_undeclared_table_is_ignored(self, tmp_path: Path):
        # External / unknown tables can't be checked — don't false-positive.
        item = self._model(tmp_path)
        _write_table(
            item,
            "fact_x",
            "table fact_x\n\tmeasure 'M' = SUM(SomethingElse[amount])\n",
        )
        assert check_orphan_columns(item) == []

    def test_column_ref_inside_string_is_ignored(self, tmp_path: Path):
        item = self._model(tmp_path)
        _write_table(
            item,
            "fact_x",
            "table fact_x\n\tmeasure 'M' = \"text dim_section[ghost] text\"\n",
        )
        assert check_orphan_columns(item) == []


# ── check_dax_paren_balance ─────────────────────────────────────────────────


class TestCheckDaxParenBalance:
    def test_balanced_passes(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(
            item,
            "f",
            "table f\n\tmeasure 'M' = CALCULATE(SUM(f[x]), f[y] > 0)\n",
        )
        assert check_dax_paren_balance(item) == []

    def test_unclosed_paren_is_error(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(item, "f", "table f\n\tmeasure 'M' = CALCULATE(SUM(f[x])\n")
        issues = check_dax_paren_balance(item)
        assert len(issues) == 1
        assert issues[0].severity == "error"
        assert "unclosed '('" in issues[0].message
        assert "measure 'M'" in issues[0].message

    def test_extra_close_paren_is_error(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(item, "f", "table f\n\tmeasure 'M' = SUM(f[x]))\n")
        issues = check_dax_paren_balance(item)
        assert len(issues) == 1
        assert "extra ')'" in issues[0].message

    def test_parens_in_strings_and_comments_ignored(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(
            item,
            "f",
            "table f\n\tmeasure 'M' = SUM(f[x]) // comment with (((\n",
        )
        assert check_dax_paren_balance(item) == []
        _write_table(
            item,
            "g",
            "table g\n\tmeasure 'N' = \"literal with )))\" & SUM(g[x])\n",
        )
        assert check_dax_paren_balance(item) == []

    def test_multiline_measure_body(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(
            item,
            "f",
            "table f\n"
            "\tmeasure 'M' =\n"
            "\t\t\tCALCULATE(\n"
            "\t\t\t    SUM(f[x])\n"
            "\t\tformatString: #,0\n",
        )
        issues = check_dax_paren_balance(item)
        assert len(issues) == 1
        assert "unclosed '('" in issues[0].message


# ── check_lineage_tag_uniqueness ────────────────────────────────────────────


class TestCheckLineageTagUniqueness:
    def test_unique_tags_pass(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(item, "f1", "table f1\n\tlineageTag: aaa-111\n")
        _write_table(item, "f2", "table f2\n\tlineageTag: bbb-222\n")
        assert check_lineage_tag_uniqueness(item) == []

    def test_duplicate_across_files_is_error(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(item, "f1", "table f1\n\tlineageTag: dup-123\n")
        _write_table(item, "f2", "table f2\n\tlineageTag: dup-123\n")
        issues = check_lineage_tag_uniqueness(item)
        assert len(issues) == 1
        assert issues[0].severity == "error"
        assert "dup-123" in issues[0].message
        assert "f1.tmdl" in issues[0].message

    def test_duplicate_within_one_file_is_error(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_table(
            item,
            "f1",
            "table f1\n\tlineageTag: dup-9\n\tcolumn c\n\t\tlineageTag: dup-9\n",
        )
        issues = check_lineage_tag_uniqueness(item)
        assert len(issues) == 1


# ── lint_semantic_model (aggregate + builder output stays clean) ────────────


class TestLintSemanticModel:
    def test_builder_emitted_model_lints_clean(self, tmp_path: Path):
        from pyfabric.items.semantic_model import (
            Column,
            LakehouseSource,
            Measure,
            SemanticModel,
            Table,
        )

        source = LakehouseSource(name="Gold", workspace_id="w", lakehouse_id="l")
        item_dir = SemanticModel(
            name="sm_lint",
            description="Lint-clean fixture.",
            sources=[source],
            tables=[
                Table(
                    name="fact_x",
                    source=source,
                    description="Fact.",
                    columns=[Column("x_key", "string", description="PK.")],
                    measures=[
                        Measure(
                            name="# Rows",
                            expression="COUNTROWS('fact_x')",
                            description="Row count.",
                        )
                    ],
                )
            ],
        ).save_to_disk(tmp_path)
        assert lint_semantic_model(item_dir) == []

    def test_aggregate_combines_all_rules(self, tmp_path: Path):
        item = tmp_path / "sm.SemanticModel"
        _write_definition_file(
            item, "database.tmdl", "database\n\tcompatibilityLevel: 1550"
        )
        _write_table(
            item,
            "f",
            "table f\n"
            "\tmeasure 'Status' = SUM(f[amount]\n"  # collision + unbalanced + orphan
            "\tcolumn status\n"
            "\tlineageTag: t-1\n",
        )
        issues = lint_semantic_model(item)
        messages = "\n".join(i.message for i in issues)
        assert "collision" in messages
        assert "below the PBIP baseline" in messages
        assert "unbalanced" in messages
        assert "[amount]" in messages
