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
    """A small but complete model: 1 dim, 1 fact, 1 relationship, 1 measure."""
    dim = Table(
        name="dim_section",
        source=gold,
        columns=[
            Column("section_key", "string", is_key=True),
            Column("section_display_name", "string"),
        ],
    )
    fact = Table(
        name="fact_status",
        source=gold,
        columns=[
            Column("section_key", "string"),
            Column("status", "string"),
            Column("projection_id", "string"),
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
        )
        item = sm.save_to_disk(tmp_path)
        text = (item / "definition" / "model.tmdl").read_text("utf-8")
        assert "annotation PBI_QueryOrder" in text
        assert "annotation CustomAnnotationKey = hello" in text
