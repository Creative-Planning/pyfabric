"""Tests for the Report (PBIR-Legacy) builder."""

import json
from pathlib import Path

import pytest

from pyfabric.items.report import (
    Aggregate,
    Card,
    Column,
    Measure,
    MultiCard,
    Page,
    Position,
    Report,
    Slicer,
    Table,
    TableOrderBy,
)

# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def basic_page() -> Page:
    return Page(
        display_name="QA Summary",
        visuals=[
            Slicer(
                position=Position(x=10, y=10, width=200, height=80),
                field=Column("dim_projection", "region"),
                mode="Dropdown",
            ),
            Card(
                position=Position(x=10, y=100, width=200, height=120),
                measure=Measure("fact_x", "# PDFs Total", format_string="#,0"),
            ),
        ],
    )


@pytest.fixture
def report_minimal(basic_page: Page) -> Report:
    return Report(
        name="rpt_test",
        semantic_model_path="../sm_test.SemanticModel",
        pages=[basic_page],
    )


# ── Report.save_to_disk ─────────────────────────────────────────────────────


class TestSaveToDisk:
    def test_creates_full_folder_structure(
        self, report_minimal: Report, tmp_path: Path
    ) -> None:
        item_dir = report_minimal.save_to_disk(tmp_path)
        assert item_dir == tmp_path / "rpt_test.Report"
        assert (item_dir / ".platform").exists()
        assert (item_dir / "definition.pbir").exists()
        assert (item_dir / "report.json").exists()

    def test_platform_metadata(self, report_minimal: Report, tmp_path: Path) -> None:
        item_dir = report_minimal.save_to_disk(tmp_path)
        platform = json.loads((item_dir / ".platform").read_text("utf-8"))
        assert platform["metadata"]["type"] == "Report"
        assert platform["metadata"]["displayName"] == "rpt_test"
        assert platform["config"]["version"] == "2.0"
        assert platform["config"]["logicalId"]

    def test_pbir_byPath_reference(
        self, report_minimal: Report, tmp_path: Path
    ) -> None:
        item_dir = report_minimal.save_to_disk(tmp_path)
        pbir = json.loads((item_dir / "definition.pbir").read_text("utf-8"))
        assert pbir["version"] == "4.0"
        assert pbir["datasetReference"]["byPath"]["path"] == "../sm_test.SemanticModel"

    def test_files_use_lf_no_trailing_newline(
        self, report_minimal: Report, tmp_path: Path
    ) -> None:
        item_dir = report_minimal.save_to_disk(tmp_path)
        for p in (
            item_dir / ".platform",
            item_dir / "definition.pbir",
            item_dir / "report.json",
        ):
            raw = p.read_bytes()
            assert b"\r\n" not in raw, f"{p.name} has CRLF"
            assert not raw.endswith(b"\n"), f"{p.name} has trailing newline"

    def test_visual_names_auto_assigned(self, basic_page: Page, tmp_path: Path) -> None:
        # No names supplied; save_to_disk should fill them deterministically.
        for v in basic_page.visuals:
            assert v.name == ""
        Report(
            name="rpt", semantic_model_path="../x.SemanticModel", pages=[basic_page]
        ).save_to_disk(tmp_path)
        for v in basic_page.visuals:
            assert v.name != ""
            assert len(v.name) == 20

    def test_deterministic_ids(self, basic_page: Page, tmp_path: Path) -> None:
        # Two saves of the same model produce identical visual ids.
        a = Report(
            name="rpt",
            semantic_model_path="../x.SemanticModel",
            pages=[basic_page],
        ).save_to_disk(tmp_path / "a")
        # Page+visual names persist across saves; rebuild fresh page object
        page2 = Page(
            display_name=basic_page.display_name,
            visuals=[
                type(v)(
                    **{k: getattr(v, k) for k in v.__dataclass_fields__ if k != "name"}
                )
                for v in basic_page.visuals
            ],
        )
        b = Report(
            name="rpt",
            semantic_model_path="../x.SemanticModel",
            pages=[page2],
        ).save_to_disk(tmp_path / "b")
        rj_a = json.loads((a / "report.json").read_text("utf-8"))
        rj_b = json.loads((b / "report.json").read_text("utf-8"))
        a_names = [
            json.loads(v["config"])["name"]
            for v in rj_a["sections"][0]["visualContainers"]
        ]
        b_names = [
            json.loads(v["config"])["name"]
            for v in rj_b["sections"][0]["visualContainers"]
        ]
        assert a_names == b_names


# ── Report JSON structure ──────────────────────────────────────────────────


def _load_report_json(item_dir: Path) -> dict:
    return json.loads((item_dir / "report.json").read_text("utf-8"))


def _visual_configs(rj: dict, page_index: int = 0) -> list[dict]:
    return [
        json.loads(v["config"]) for v in rj["sections"][page_index]["visualContainers"]
    ]


class TestReportStructure:
    def test_pods_one_per_page(self, report_minimal: Report, tmp_path: Path) -> None:
        item_dir = report_minimal.save_to_disk(tmp_path)
        rj = _load_report_json(item_dir)
        assert len(rj["pods"]) == 1
        assert rj["pods"][0]["boundSection"] == report_minimal.pages[0].name

    def test_section_dimensions_and_name(
        self, report_minimal: Report, tmp_path: Path
    ) -> None:
        item_dir = report_minimal.save_to_disk(tmp_path)
        rj = _load_report_json(item_dir)
        section = rj["sections"][0]
        assert section["displayName"] == "QA Summary"
        assert section["width"] == 1280
        assert section["height"] == 720
        assert section["name"]


# ── Slicer ──────────────────────────────────────────────────────────────────


class TestSlicer:
    def test_dropdown_mode(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                Slicer(
                    position=Position(x=0, y=0, width=200, height=80),
                    field=Column("dim_projection", "region"),
                    mode="Dropdown",
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        rj = _load_report_json(tmp_path / "rpt.Report")
        cfg = _visual_configs(rj)[0]
        assert cfg["singleVisual"]["visualType"] == "slicer"
        assert (
            cfg["singleVisual"]["projections"]["Values"][0]["queryRef"]
            == "dim_projection.region"
        )
        mode = cfg["singleVisual"]["objects"]["data"][0]["properties"]["mode"]
        assert mode["expr"]["Literal"]["Value"] == "'Dropdown'"

    def test_allow_values_emits_filter(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                Slicer(
                    position=Position(x=0, y=0, width=200, height=80),
                    field=Column("fact_x", "status"),
                    mode="Basic",
                    allow_values=["INCOMPLETE", "NOT_DETECTED"],
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        rj = _load_report_json(tmp_path / "rpt.Report")
        cfg = _visual_configs(rj)[0]
        general = cfg["singleVisual"]["objects"]["general"][0]["properties"]
        values = general["filter"]["filter"]["Where"][0]["Condition"]["In"]["Values"]
        flat = [v[0]["Literal"]["Value"] for v in values]
        assert flat == ["'INCOMPLETE'", "'NOT_DETECTED'"]


# ── Card (single-metric) ───────────────────────────────────────────────────


class TestCard:
    def test_display_units_none_emits_1D(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                Card(
                    position=Position(x=0, y=0, width=200, height=120),
                    measure=Measure("fact_x", "# PDFs OK", format_string="#,0"),
                    display_units="None",
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        rj = _load_report_json(tmp_path / "rpt.Report")
        cfg = _visual_configs(rj)[0]
        assert cfg["singleVisual"]["visualType"] == "cardVisual"
        value = cfg["singleVisual"]["objects"]["value"][0]["properties"]
        assert value["displayUnits"]["expr"]["Literal"]["Value"] == "1D"
        # format_string flows into columnProperties
        cp = cfg["singleVisual"]["columnProperties"]
        assert cp["fact_x.# PDFs OK"]["formatString"] == "#,0"

    def test_display_units_auto_emits_0D(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                Card(
                    position=Position(x=0, y=0, width=200, height=120),
                    measure=Measure("fact_x", "Total"),
                    display_units="Auto",
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        cfg = _visual_configs(_load_report_json(tmp_path / "rpt.Report"))[0]
        value = cfg["singleVisual"]["objects"]["value"][0]["properties"]
        assert value["displayUnits"]["expr"]["Literal"]["Value"] == "0D"


# ── MultiCard ───────────────────────────────────────────────────────────────


class TestMultiCard:
    def test_emits_cards_layout_with_per_measure_referencing(
        self, tmp_path: Path
    ) -> None:
        page = Page(
            display_name="P",
            visuals=[
                MultiCard(
                    position=Position(x=0, y=0, width=600, height=120),
                    measures=[
                        Measure("fact_x", "# PDFs Total", format_string="#,0"),
                        Measure("fact_x", "# PDFs OK", format_string="#,0"),
                    ],
                    display_units="None",
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        cfg = _visual_configs(_load_report_json(tmp_path / "rpt.Report"))[0]
        objects = cfg["singleVisual"]["objects"]
        # Cards layout style
        assert (
            objects["layout"][0]["properties"]["style"]["expr"]["Literal"]["Value"]
            == "'Cards'"
        )
        # One referenceLabel entry per measure
        refs = objects["referenceLabel"]
        ref_names = sorted(r["selector"]["metadata"] for r in refs)
        assert ref_names == ["fact_x.# PDFs OK", "fact_x.# PDFs Total"]
        # 1D for each measure's valueDisplayUnits
        assert all(
            r["properties"]["valueDisplayUnits"]["expr"]["Literal"]["Value"] == "1D"
            for r in objects["referenceLabelValue"]
        )

    def test_polish_flags(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                MultiCard(
                    position=Position(x=0, y=0, width=600, height=120),
                    measures=[Measure("fact_x", "M1")],
                    show_outline=True,
                    show_accent_bar=True,
                    show_shadow=True,
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        objects = _visual_configs(_load_report_json(tmp_path / "rpt.Report"))[0][
            "singleVisual"
        ]["objects"]
        for key in ("outline", "accentBar", "shadowCustom"):
            assert key in objects, f"{key} missing"

    def test_polish_flags_default_off_for_shadow(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                MultiCard(
                    position=Position(x=0, y=0, width=600, height=120),
                    measures=[Measure("fact_x", "M1")],
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        objects = _visual_configs(_load_report_json(tmp_path / "rpt.Report"))[0][
            "singleVisual"
        ]["objects"]
        assert "shadowCustom" not in objects
        assert "outline" in objects
        assert "accentBar" in objects

    def test_rejects_empty_measures(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[MultiCard(position=Position(x=0, y=0, width=600, height=120))],
        )
        with pytest.raises(ValueError, match="at least one measure"):
            Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)

    def test_rejects_mixed_entities(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                MultiCard(
                    position=Position(x=0, y=0, width=600, height=120),
                    measures=[
                        Measure("fact_x", "M1"),
                        Measure("fact_y", "M2"),
                    ],
                ),
            ],
        )
        with pytest.raises(ValueError, match="same entity"):
            Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)


# ── Table ───────────────────────────────────────────────────────────────────


class TestTable:
    def test_columns_measures_and_aggregate(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                Table(
                    position=Position(x=0, y=0, width=1200, height=300),
                    fields=[
                        Column("dim_projection", "region"),
                        Column("dim_projection", "project_number"),
                        Measure("fact_x", "# PDFs OK"),
                        Aggregate("fact_x", "missing_field_count", function="sum"),
                    ],
                    order_by=TableOrderBy(
                        field=Aggregate(
                            "fact_x", "missing_field_count", function="sum"
                        ),
                        direction="desc",
                    ),
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        cfg = _visual_configs(_load_report_json(tmp_path / "rpt.Report"))[0]
        assert cfg["singleVisual"]["visualType"] == "tableEx"
        projections = cfg["singleVisual"]["projections"]["Values"]
        refs = [p["queryRef"] for p in projections]
        assert refs == [
            "dim_projection.region",
            "dim_projection.project_number",
            "fact_x.# PDFs OK",
            "Sum(fact_x.missing_field_count)",
        ]
        # From clause has one alias per distinct entity
        from_entities = sorted(
            f["Entity"] for f in cfg["singleVisual"]["prototypeQuery"]["From"]
        )
        assert from_entities == ["dim_projection", "fact_x"]
        # OrderBy uses the Aggregation wrapper, direction=2 (desc)
        ob = cfg["singleVisual"]["prototypeQuery"]["OrderBy"][0]
        assert ob["Direction"] == 2
        assert "Aggregation" in ob["Expression"]
        assert ob["Expression"]["Aggregation"]["Function"] == 0  # sum

    def test_orderby_by_column(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                Table(
                    position=Position(x=0, y=0, width=1200, height=300),
                    fields=[Column("dim_projection", "region")],
                    order_by=TableOrderBy(
                        field=Column("dim_projection", "region"), direction="asc"
                    ),
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        cfg = _visual_configs(_load_report_json(tmp_path / "rpt.Report"))[0]
        ob = cfg["singleVisual"]["prototypeQuery"]["OrderBy"][0]
        assert ob["Direction"] == 1
        assert "Column" in ob["Expression"]

    def test_no_orderby_when_unset(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                Table(
                    position=Position(x=0, y=0, width=1200, height=300),
                    fields=[Column("dim_projection", "region")],
                ),
            ],
        )
        Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
        cfg = _visual_configs(_load_report_json(tmp_path / "rpt.Report"))[0]
        assert "OrderBy" not in cfg["singleVisual"]["prototypeQuery"]

    def test_rejects_empty_fields(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[Table(position=Position(x=0, y=0, width=1200, height=300))],
        )
        with pytest.raises(ValueError, match="at least one field"):
            Report("rpt", "../x.SemanticModel", [page]).save_to_disk(tmp_path)
