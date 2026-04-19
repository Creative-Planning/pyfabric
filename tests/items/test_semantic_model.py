"""Tests for the SemanticModel builder."""

import json
from pathlib import Path

import pytest

from pyfabric.items.semantic_model import (
    Column,
    LakehouseSource,
    Measure,
    Relationship,
    SemanticModel,
    SemanticModelError,
    Table,
    arrow_to_tmdl,
)

# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def gold() -> LakehouseSource:
    return LakehouseSource(
        name="Gold",
        workspace_id="ws-1234",
        lakehouse_id="lh-5678",
    )


@pytest.fixture
def minimal_model(gold: LakehouseSource) -> SemanticModel:
    """A small but complete model: 1 dim, 1 fact, 1 relationship, 1 measure.

    Every visible object has a non-empty description so the fixture
    passes the strict_descriptions gate; this also documents the
    expected user-facing pattern.
    """
    dim = Table(
        name="dim_section",
        source=gold,
        description="Test dim.",
        columns=[
            Column("section_key", "string", is_key=True, description="PK."),
            Column("section_display_name", "string", description="Display name."),
        ],
    )
    fact = Table(
        name="fact_status",
        source=gold,
        description="Test fact.",
        columns=[
            Column("section_key", "string", description="FK to dim_section."),
            Column("status", "string", description="Per-row status enum."),
            Column("projection_id", "string", description="Per-row PDF id."),
        ],
        measures=[
            Measure(
                name="# PDFs Total",
                expression="DISTINCTCOUNT('fact_status'[projection_id])",
                format_string="#,0",
                description="Distinct PDFs in current filter context.",
            ),
        ],
    )
    return SemanticModel(
        name="sm_test",
        description="Test model.",
        sources=[gold],
        tables=[dim, fact],
        relationships=[
            Relationship(
                from_table="fact_status",
                from_column="section_key",
                to_table="dim_section",
                to_column="section_key",
            ),
        ],
    )


# ── Type-mapping helpers ───────────────────────────────────────────────────


class TestArrowToTmdl:
    @pytest.mark.parametrize(
        "arrow, expected",
        [
            ("string", "string"),
            ("large_string", "string"),
            ("utf8", "string"),
            ("int8", "int64"),
            ("int32", "int64"),
            ("int64", "int64"),
            ("uint16", "int64"),
            ("float32", "double"),
            ("float64", "double"),
            ("double", "double"),
            ("decimal128(38, 10)", "double"),
            ("bool", "boolean"),
            ("date32[day]", "dateTime"),
            ("timestamp[us, tz=UTC]", "dateTime"),
            ("binary", "string"),  # fallback
            ("UNKNOWN", "string"),  # fallback
        ],
    )
    def test_mapping(self, arrow: str, expected: str) -> None:
        assert arrow_to_tmdl(arrow) == expected


# ── LakehouseSource ────────────────────────────────────────────────────────


class TestLakehouseSource:
    def test_invalid_identifier_rejected(self) -> None:
        with pytest.raises(ValueError, match="valid M identifier"):
            LakehouseSource(
                name="Not An Identifier", workspace_id="x", lakehouse_id="y"
            )


# ── Validation ─────────────────────────────────────────────────────────────


class TestValidate:
    def test_clean_model_passes(self, minimal_model: SemanticModel) -> None:
        assert minimal_model.validate() == []

    def test_duplicate_table_caught(self, gold: LakehouseSource) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(name="t", source=gold, columns=[Column("a", "string")]),
                Table(name="t", source=gold, columns=[Column("a", "string")]),
            ],
        )
        errs = sm.validate()
        assert any("duplicate table" in e for e in errs)

    def test_duplicate_column_caught(self, gold: LakehouseSource) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    columns=[Column("a", "string"), Column("a", "int64")],
                ),
            ],
        )
        errs = sm.validate()
        assert any("duplicate column" in e and "'a'" in e for e in errs)

    def test_measure_column_collision_caught_case_insensitive(
        self, gold: LakehouseSource
    ) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    columns=[Column("status", "string")],
                    measures=[Measure("Status", "1")],
                ),
            ],
        )
        errs = sm.validate()
        assert any("collides" in e and "'Status'" in e for e in errs), (
            f"unexpected errors: {errs}"
        )

    def test_relationship_unknown_table(self, gold: LakehouseSource) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[Table(name="t", source=gold, columns=[Column("a", "string")])],
            relationships=[
                Relationship(
                    from_table="t",
                    from_column="a",
                    to_table="missing",
                    to_column="a",
                ),
            ],
        )
        errs = sm.validate()
        assert any("unknown table" in e and "missing" in e for e in errs)

    def test_relationship_unknown_column(self, gold: LakehouseSource) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(name="t", source=gold, columns=[Column("a", "string")]),
                Table(name="d", source=gold, columns=[Column("a", "string")]),
            ],
            relationships=[
                Relationship(
                    from_table="t",
                    from_column="missing",
                    to_table="d",
                    to_column="a",
                ),
            ],
        )
        errs = sm.validate()
        assert any("unknown column" in e and "'missing'" in e for e in errs)

    def test_undeclared_source(self, gold: LakehouseSource) -> None:
        other = LakehouseSource(name="Silver", workspace_id="x", lakehouse_id="y")
        sm = SemanticModel(
            name="sm",
            sources=[gold],  # Silver intentionally not declared
            tables=[Table(name="t", source=other, columns=[Column("a", "string")])],
        )
        errs = sm.validate()
        assert any("source 'Silver'" in e for e in errs)


# ── Emission ───────────────────────────────────────────────────────────────


class TestSaveToDisk:
    def test_creates_full_folder_structure(
        self, minimal_model: SemanticModel, tmp_path: Path
    ) -> None:
        item_dir = minimal_model.save_to_disk(tmp_path)
        assert item_dir == tmp_path / "sm_test.SemanticModel"
        # Top level
        assert (item_dir / ".platform").exists()
        assert (item_dir / "definition.pbism").exists()
        # Definition
        d = item_dir / "definition"
        assert (d / "database.tmdl").exists()
        assert (d / "model.tmdl").exists()
        assert (d / "expressions.tmdl").exists()
        assert (d / "relationships.tmdl").exists()
        assert (d / "cultures" / "en-US.tmdl").exists()
        assert (d / "tables" / "dim_section.tmdl").exists()
        assert (d / "tables" / "fact_status.tmdl").exists()

    def test_validation_blocks_save(
        self, gold: LakehouseSource, tmp_path: Path
    ) -> None:
        bad = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    columns=[Column("status", "string")],
                    measures=[Measure("status", "1")],  # collision
                ),
            ],
            strict_descriptions=False,
        )
        with pytest.raises(SemanticModelError, match="collides"):
            bad.save_to_disk(tmp_path)
        assert not (tmp_path / "sm.SemanticModel").exists()

    def test_platform_has_required_fields(
        self, minimal_model: SemanticModel, tmp_path: Path
    ) -> None:
        item_dir = minimal_model.save_to_disk(tmp_path)
        platform = json.loads((item_dir / ".platform").read_text("utf-8"))
        assert platform["metadata"]["type"] == "SemanticModel"
        assert platform["metadata"]["displayName"] == "sm_test"
        assert platform["metadata"]["description"] == "Test model."
        assert platform["config"]["version"] == "2.0"
        assert platform["config"]["logicalId"]

    def test_table_tmdl_includes_partition_and_columns(
        self, minimal_model: SemanticModel, tmp_path: Path
    ) -> None:
        item_dir = minimal_model.save_to_disk(tmp_path)
        text = (item_dir / "definition" / "tables" / "dim_section.tmdl").read_text(
            "utf-8"
        )
        assert "table dim_section" in text
        assert "column section_key" in text
        assert "isKey" in text
        assert "partition dim_section = m" in text
        assert "Source = Gold" in text
        assert 'Source{[Id="dim_section", Schema="dbo"]}[Data]' in text

    def test_measure_tmdl_includes_description_and_format(
        self, minimal_model: SemanticModel, tmp_path: Path
    ) -> None:
        item_dir = minimal_model.save_to_disk(tmp_path)
        text = (item_dir / "definition" / "tables" / "fact_status.tmdl").read_text(
            "utf-8"
        )
        assert "/// Distinct PDFs in current filter context." in text
        assert "measure '# PDFs Total' = DISTINCTCOUNT" in text
        assert "formatString: #,0" in text

    def test_expressions_tmdl_includes_lakehouse_navigation(
        self, minimal_model: SemanticModel, tmp_path: Path
    ) -> None:
        item_dir = minimal_model.save_to_disk(tmp_path)
        text = (item_dir / "definition" / "expressions.tmdl").read_text("utf-8")
        assert 'expression GoldWorkspaceId = "ws-1234"' in text
        assert 'expression GoldLakehouseId = "lh-5678"' in text
        assert "expression Gold =" in text
        assert "Lakehouse.Contents(null)" in text
        assert "workspaceId = GoldWorkspaceId" in text
        assert "lakehouseId = GoldLakehouseId" in text

    def test_relationships_tmdl_emitted(
        self, minimal_model: SemanticModel, tmp_path: Path
    ) -> None:
        item_dir = minimal_model.save_to_disk(tmp_path)
        text = (item_dir / "definition" / "relationships.tmdl").read_text("utf-8")
        assert "relationship " in text
        assert "fromColumn: fact_status.section_key" in text
        assert "toColumn: dim_section.section_key" in text

    def test_files_use_lf_no_trailing_newline(
        self, minimal_model: SemanticModel, tmp_path: Path
    ) -> None:
        # All TMDL/JSON in a SemanticModel: LF, no trailing newline.
        item_dir = minimal_model.save_to_disk(tmp_path)
        for p in [
            item_dir / ".platform",
            item_dir / "definition.pbism",
            item_dir / "definition" / "model.tmdl",
            item_dir / "definition" / "tables" / "fact_status.tmdl",
        ]:
            raw = p.read_bytes()
            assert b"\r\n" not in raw, f"{p.name} contains CRLF"
            assert not raw.endswith(b"\n"), f"{p.name} has trailing newline"

    def test_lineage_tags_deterministic(
        self, minimal_model: SemanticModel, tmp_path: Path
    ) -> None:
        # Re-emitting the same model should produce identical TMDL files
        # (modulo logical_id which is per-instance random).
        a = minimal_model.save_to_disk(tmp_path / "a")
        b = minimal_model.save_to_disk(tmp_path / "b")
        for rel in (
            "definition/expressions.tmdl",
            "definition/relationships.tmdl",
            "definition/tables/dim_section.tmdl",
            "definition/tables/fact_status.tmdl",
        ):
            assert (a / rel).read_bytes() == (b / rel).read_bytes(), rel


# ── Table.from_parquet ─────────────────────────────────────────────────────


class TestTableFromParquet:
    def test_derives_columns_from_arrow_schema(
        self, gold: LakehouseSource, tmp_path: Path
    ) -> None:
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int32()),
                "name": pa.array(["a", "b"]),
                "amount": pa.array([1.5, 2.5], type=pa.float64()),
                "active": pa.array([True, False]),
                "ts": pa.array([0, 1], type=pa.timestamp("us")),
            }
        )
        path = tmp_path / "sample.parquet"
        pq.write_table(table, path)

        t = Table.from_parquet("dim_x", source=gold, parquet_path=path)
        assert t.name == "dim_x"
        assert t.source is gold
        col_types = {c.name: c.data_type for c in t.columns}
        assert col_types == {
            "id": "int64",
            "name": "string",
            "amount": "double",
            "active": "boolean",
            "ts": "dateTime",
        }


# ── User-supplied annotations ──────────────────────────────────────────────


class TestAnnotations:
    def test_table_annotation_emitted(
        self, gold: LakehouseSource, tmp_path: Path
    ) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    columns=[Column("a", "string")],
                    annotations={"PBI_NavigationStepName": "Navigation"},
                ),
            ],
            strict_descriptions=False,
        )
        item = sm.save_to_disk(tmp_path)
        text = (item / "definition" / "tables" / "t.tmdl").read_text("utf-8")
        assert "annotation PBI_ResultType = Table" in text
        assert "annotation PBI_NavigationStepName = Navigation" in text

    def test_table_annotation_overrides_auto(
        self, gold: LakehouseSource, tmp_path: Path
    ) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    columns=[Column("a", "string")],
                    annotations={"PBI_ResultType": "Calculated"},
                ),
            ],
            strict_descriptions=False,
        )
        item = sm.save_to_disk(tmp_path)
        text = (item / "definition" / "tables" / "t.tmdl").read_text("utf-8")
        # User override wins; the default Table value is replaced
        assert "annotation PBI_ResultType = Calculated" in text
        assert "annotation PBI_ResultType = Table" not in text

    def test_column_annotation_overrides_auto(
        self, gold: LakehouseSource, tmp_path: Path
    ) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    columns=[
                        Column(
                            "a",
                            "string",
                            annotations={"SummarizationSetBy": "User"},
                        ),
                    ],
                ),
            ],
            strict_descriptions=False,
        )
        item = sm.save_to_disk(tmp_path)
        text = (item / "definition" / "tables" / "t.tmdl").read_text("utf-8")
        assert "annotation SummarizationSetBy = User" in text
        assert "annotation SummarizationSetBy = Automatic" not in text

    def test_measure_annotation_emitted(
        self, gold: LakehouseSource, tmp_path: Path
    ) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    columns=[Column("a", "string")],
                    measures=[
                        Measure(
                            "M",
                            "1",
                            annotations={"PBI_FormatHint": '{"isGeneralNumber":true}'},
                        ),
                    ],
                ),
            ],
            strict_descriptions=False,
        )
        item = sm.save_to_disk(tmp_path)
        text = (item / "definition" / "tables" / "t.tmdl").read_text("utf-8")
        assert "annotation PBI_FormatHint" in text

    def test_model_annotation_appended(
        self, gold: LakehouseSource, tmp_path: Path
    ) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[Table(name="t", source=gold, columns=[Column("a", "string")])],
            annotations={"CustomAnnotationKey": "hello"},
            strict_descriptions=False,
        )
        item = sm.save_to_disk(tmp_path)
        text = (item / "definition" / "model.tmdl").read_text("utf-8")
        assert "annotation PBI_QueryOrder" in text
        assert "annotation CustomAnnotationKey = hello" in text


# ── Strict-descriptions enforcement ────────────────────────────────────────


class TestStrictDescriptions:
    def test_default_is_strict(self, gold: LakehouseSource, tmp_path: Path) -> None:
        # Visible columns/tables/measures missing descriptions → SemanticModelError.
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[Table(name="t", source=gold, columns=[Column("a", "string")])],
        )
        with pytest.raises(SemanticModelError, match="needs a description"):
            sm.save_to_disk(tmp_path)

    def test_error_message_names_each_missing_object(
        self, gold: LakehouseSource
    ) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    columns=[Column("a", "string"), Column("b", "string")],
                    measures=[Measure("M", "1")],
                ),
            ],
        )
        errs = sm.validate()
        # Each visible object reported individually
        assert any("table 't'" in e for e in errs)
        assert any("t.a" in e for e in errs)
        assert any("t.b" in e for e in errs)
        assert any("'M'" in e for e in errs)

    def test_hidden_objects_exempt(self, gold: LakehouseSource, tmp_path: Path) -> None:
        # Hidden columns / measures / tables don't need descriptions.
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    description="Visible table.",
                    columns=[
                        Column("visible", "string", description="Visible col."),
                        Column("hidden_audit_ts", "string", is_hidden=True),
                    ],
                    measures=[
                        Measure("M visible", "1", description="Visible measure."),
                        Measure("hidden_helper", "1", is_hidden=True),
                    ],
                ),
                Table(
                    name="hidden_lookup",
                    source=gold,
                    is_hidden=True,
                    columns=[Column("k", "string")],
                ),
            ],
        )
        # Should not raise — hidden objects exempt
        sm.save_to_disk(tmp_path)

    def test_opt_out_warns_but_succeeds(
        self, gold: LakehouseSource, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[Table(name="t", source=gold, columns=[Column("a", "string")])],
            strict_descriptions=False,
        )
        # Doesn't raise; produces a structlog warning naming the missing objects
        item = sm.save_to_disk(tmp_path)
        assert (item / "definition" / "model.tmdl").exists()

    def test_whitespace_only_description_treated_as_missing(
        self, gold: LakehouseSource
    ) -> None:
        sm = SemanticModel(
            name="sm",
            sources=[gold],
            tables=[
                Table(
                    name="t",
                    source=gold,
                    description="   ",  # whitespace only
                    columns=[Column("a", "string", description="X.")],
                ),
            ],
        )
        errs = sm.validate()
        assert any("table 't'" in e for e in errs)

    def test_minimal_model_with_full_descriptions_passes(
        self, minimal_model: SemanticModel
    ) -> None:
        # The fixture itself is the canonical "good" example — every
        # visible object has a description.
        assert minimal_model.validate() == []
