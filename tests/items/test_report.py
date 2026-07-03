"""Tests for the Report (modern PBIR) builder.

The centerpiece is :class:`TestGoldenPbir`, a serializer proof: it
reconstructs a hand-cleaned, Fabric-derived PBIR report via the builder
(with every page/visual/filter id and position pinned to the fixture's
exact values) and asserts **byte-equality** against each fixture file.
The fixture under ``fixtures/pbir_report/`` was authored from the real
Fabric output of a slicer + two tables, scrubbed to generic entity names
and stripped of UI-default / drag / runtime-selection state — so the
test is grounded in Fabric's structure without being circular.

The remaining classes are focused unit tests per emitter (slicer, table,
cards, column charts, projections, sort, measure-bound title, automatic
page refresh) and coverage of the ``NotImplementedError`` boundaries for
features whose PBIR bytes are not yet attested (inline Aggregate,
sort-by-measure in tables).

The card and chart emitters were certified byte-for-byte against
Fabric-round-tripped ``visualContainer`` 2.10.0 reference bytes (a
minimal single-measure card, a 2-measure KPI card, a columnChart, and a
clusteredColumnChart) before the golden fixture page was generated.
"""

import json
from pathlib import Path

import pytest

from pyfabric.items.report import (
    Aggregate,
    Card,
    ClusteredColumnChart,
    Column,
    ColumnChart,
    Measure,
    MultiCard,
    Page,
    Position,
    Report,
    ReportError,
    Slicer,
    Table,
    TableOrderBy,
    Theme,
    Visual,
)

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "pbir_report" / "rpt_golden.Report"


# ── Golden byte-equality test ───────────────────────────────────────────────


def _golden_report() -> Report:
    """Reconstruct the golden fixture report via the builder.

    Every id and position is pinned to the fixture's exact values so the
    output is deterministic and byte-comparable.
    """
    page = Page(
        display_name="Page 1",
        name="page001slicertable",
        width=1280,
        height=720,
        visuals=[
            Slicer(
                name="vis0slicer0000000000",
                position=Position(x=10, y=0, z=0, width=430, height=280, tab_order=0),
                field=Column("dim_x", "name"),
            ),
            Table(
                name="vis1tablecol00000000",
                position=Position(x=460, y=0, z=1, width=780, height=270, tab_order=1),
                fields=[
                    Column("dim_x", "name"),
                    Column("dim_x", "city"),
                ],
                order_by=TableOrderBy(field=Column("dim_x", "name"), direction="asc"),
            ),
            Table(
                name="vis2tablemeas0000000",
                position=Position(
                    x=460, y=290, z=2, width=780, height=270, tab_order=2
                ),
                fields=[
                    Column("dim_x", "name"),
                    Measure("fact_x", "Total Rows"),
                ],
                order_by=TableOrderBy(field=Column("dim_x", "name"), direction="asc"),
            ),
        ],
    )
    cards_page = Page(
        display_name="Cards and Charts",
        name="page002cardscharts",
        width=1280,
        height=720,
        page_refresh="PT5M",
        visuals=[
            Card(
                name="vis3cardmulti0000000",
                position=Position(x=10, y=0, z=0, width=220, height=120, tab_order=0),
                measure=[
                    Measure("fact_x", "Total Rows", format_string="#,0"),
                    Measure("fact_x", "Total Amount", format_string="\\$#,0.00"),
                ],
                value_font_size=14,
                padding=8,
                show_border=True,
                show_visual_header=True,
            ),
            Card(
                name="vis4cardmini00000000",
                position=Position(x=10, y=680, z=1, width=300, height=36, tab_order=1),
                measure=Measure("fact_x", "Last Row Received"),
            ),
            ColumnChart(
                name="vis5colchart00000000",
                position=Position(
                    x=10, y=300, z=2, width=1240, height=370, tab_order=2
                ),
                category=Column("dim_x", "city", display_name="City"),
                values=[Measure("fact_x", "Total Rows", display_name="Rows")],
                value_axis_title="Rows",
                sort_by=Measure("fact_x", "Total Rows"),
                title="Rows by City",
            ),
            ClusteredColumnChart(
                name="vis6cluchart00000000",
                position=Position(x=560, y=50, z=3, width=700, height=240, tab_order=3),
                category=Column("dim_x", "name", display_name="Period"),
                values=[
                    Measure("fact_x", "Prior Period", display_name="Last (full)"),
                    Measure("fact_x", "Current Period", display_name="This (so far)"),
                ],
                legend=True,
                value_axis_title=True,
                category_axis_title=False,
                title="This period vs last",
                show_title=False,
            ),
        ],
    )
    return Report(
        name="rpt_golden",
        semantic_model_path="../sm_golden.SemanticModel",
        pages=[page, cards_page],
        description="Golden fixture report.",
        logical_id="f1d606f1-d262-b501-40f8-1f64df6fb535",
    )


def _all_fixture_files() -> list[Path]:
    return sorted(p for p in FIXTURE_ROOT.rglob("*") if p.is_file())


class TestGoldenPbir:
    def test_byte_equal_to_fixture(self, tmp_path: Path) -> None:
        item_dir = _golden_report().save_to_disk(tmp_path)
        assert item_dir == tmp_path / "rpt_golden.Report"

        fixture_files = _all_fixture_files()
        assert fixture_files, "fixture is empty — check FIXTURE_ROOT"

        for fixture_file in fixture_files:
            rel = fixture_file.relative_to(FIXTURE_ROOT)
            emitted = item_dir / rel
            assert emitted.exists(), f"builder did not emit {rel}"
            expected = fixture_file.read_bytes()
            actual = emitted.read_bytes()
            assert actual == expected, (
                f"byte mismatch in {rel}\n"
                f"--- expected (fixture) ---\n{expected.decode('utf-8')}\n"
                f"--- actual (emitted) ---\n{actual.decode('utf-8')}"
            )

    def test_emits_exactly_the_fixture_file_set(self, tmp_path: Path) -> None:
        item_dir = _golden_report().save_to_disk(tmp_path)
        emitted = {p.relative_to(item_dir) for p in item_dir.rglob("*") if p.is_file()}
        expected = {p.relative_to(FIXTURE_ROOT) for p in _all_fixture_files()}
        assert emitted == expected


# ── Helpers for structural unit tests ───────────────────────────────────────


def _visual_json(item_dir: Path, page_name: str, visual_name: str) -> dict:
    p = (
        item_dir
        / "definition"
        / "pages"
        / page_name
        / "visuals"
        / visual_name
        / "visual.json"
    )
    return json.loads(p.read_text("utf-8"))


def _one_visual_report(visual: Visual, *, page_name: str = "p") -> Report:
    page = Page(display_name="P", name=page_name, visuals=[visual])
    return Report("rpt", "../x.SemanticModel", [page], strict_descriptions=False)


# ── save_to_disk / folder structure ─────────────────────────────────────────


class TestSaveToDisk:
    def test_creates_pbir_folder_structure(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("dim_x", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        assert (item_dir / ".platform").exists()
        assert (item_dir / "definition.pbir").exists()
        assert (item_dir / "definition" / "version.json").exists()
        assert (item_dir / "definition" / "report.json").exists()
        assert (item_dir / "definition" / "pages" / "pages.json").exists()
        assert (item_dir / "definition" / "pages" / "p" / "page.json").exists()
        # No legacy single report.json at the item root.
        assert not (item_dir / "report.json").exists()

    def test_platform_omits_description(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("dim_x", "region"),
        )
        report = Report(
            "rpt",
            "../x.SemanticModel",
            [Page(display_name="P", name="p", visuals=[visual])],
            description="A real description that Fabric would strip.",
        )
        item_dir = report.save_to_disk(tmp_path)
        platform = json.loads((item_dir / ".platform").read_text("utf-8"))
        assert platform["metadata"]["type"] == "Report"
        assert platform["metadata"]["displayName"] == "rpt"
        # Fabric strips report descriptions from .platform → we never write one.
        assert "description" not in platform["metadata"]

    def test_pbir_byPath_reference(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("dim_x", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        pbir = json.loads((item_dir / "definition.pbir").read_text("utf-8"))
        assert pbir["version"] == "4.0"
        assert pbir["datasetReference"]["byPath"]["path"] == "../x.SemanticModel"

    def test_version_json(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("dim_x", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        version = json.loads(
            (item_dir / "definition" / "version.json").read_text("utf-8")
        )
        assert version["version"] == "2.0.0"

    def test_report_json_settings_are_enum_strings(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("dim_x", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        rj = json.loads((item_dir / "definition" / "report.json").read_text("utf-8"))
        # exportDataMode is an enum string in modern PBIR, not the legacy int.
        assert rj["settings"]["exportDataMode"] == "AllowSummarized"
        assert rj["objects"]["section"]

    def test_files_use_lf_no_trailing_newline(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("dim_x", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        for p in item_dir.rglob("*"):
            if not p.is_file():
                continue
            raw = p.read_bytes()
            assert b"\r\n" not in raw, f"{p.name} has CRLF"
            assert not raw.endswith(b"\n"), f"{p.name} has trailing newline"

    def test_visual_names_auto_assigned(self, tmp_path: Path) -> None:
        page = Page(
            display_name="P",
            visuals=[
                Slicer(
                    position=Position(x=0, y=0, width=200, height=80),
                    field=Column("dim_x", "region"),
                ),
            ],
        )
        for v in page.visuals:
            assert v.name == ""
        Report(
            "rpt", "../x.SemanticModel", [page], strict_descriptions=False
        ).save_to_disk(tmp_path)
        for v in page.visuals:
            assert v.name != ""
            assert len(v.name) == 20

    def test_deterministic_ids(self, tmp_path: Path) -> None:
        def fresh_page() -> Page:
            return Page(
                display_name="P",
                visuals=[
                    Slicer(
                        position=Position(x=0, y=0, width=200, height=80),
                        field=Column("dim_x", "region"),
                    ),
                ],
            )

        a = Report(
            "rpt", "../x.SemanticModel", [fresh_page()], strict_descriptions=False
        ).save_to_disk(tmp_path / "a")
        b = Report(
            "rpt", "../x.SemanticModel", [fresh_page()], strict_descriptions=False
        ).save_to_disk(tmp_path / "b")
        a_pages = json.loads(
            (a / "definition" / "pages" / "pages.json").read_text("utf-8")
        )["pageOrder"]
        b_pages = json.loads(
            (b / "definition" / "pages" / "pages.json").read_text("utf-8")
        )["pageOrder"]
        assert a_pages == b_pages


# ── Slicer ──────────────────────────────────────────────────────────────────


class TestSlicer:
    def test_list_slicer_projection(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("dim_projection", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        assert vj["visual"]["visualType"] == "listSlicer"
        proj = vj["visual"]["query"]["queryState"]["Values"]["projections"]
        assert proj[0]["queryRef"] == "dim_projection.region"
        assert proj[0]["nativeQueryRef"] == "region"
        assert proj[0]["active"] is True
        assert (
            proj[0]["field"]["Column"]["Expression"]["SourceRef"]["Entity"]
            == "dim_projection"
        )

    def test_multi_column_first_active_rest_inactive(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=[Column("dim_x", "company"), Column("dim_x", "vendor_id")],
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        proj = vj["visual"]["query"]["queryState"]["Values"]["projections"]
        assert [p["active"] for p in proj] == [True, False]

    def test_filter_config_per_column(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("dim_x", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        filters = vj["filterConfig"]["filters"]
        assert len(filters) == 1
        assert filters[0]["type"] == "Categorical"
        assert len(filters[0]["name"]) == 20

    def test_cross_entity_hierarchy_rejected(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=[Column("dim_x", "a"), Column("dim_y", "b")],
        )
        with pytest.raises(ValueError, match="must share an entity"):
            _one_visual_report(visual).save_to_disk(tmp_path)

    def test_allow_values_dropped_with_warning(self, tmp_path: Path) -> None:
        # No attested PBIR bytes for the allow-list filter; it is dropped,
        # not silently emitted wrong. Verify the slicer carries no scoping
        # filter (filterConfig keeps the per-column entry, but the visual
        # body has no objects/general filter).
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("fact_x", "status"),
            mode="Basic",
            allow_values=["INCOMPLETE", "NOT_DETECTED"],
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        assert "objects" not in vj["visual"]

    def test_non_default_mode_emits_plain_list_slicer(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, width=200, height=80),
            field=Column("dim_x", "region"),
            mode="Between",
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        assert vj["visual"]["visualType"] == "listSlicer"


# ── Table ───────────────────────────────────────────────────────────────────


class TestTable:
    def test_columns_and_measure_projections(self, tmp_path: Path) -> None:
        visual = Table(
            name="v",
            position=Position(x=0, y=0, width=1200, height=300),
            fields=[
                Column("dim_x", "region"),
                Measure("fact_x", "Total Rows"),
            ],
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        assert vj["visual"]["visualType"] == "tableEx"
        proj = vj["visual"]["query"]["queryState"]["Values"]["projections"]
        assert proj[0]["queryRef"] == "dim_x.region"
        assert "Column" in proj[0]["field"]
        assert proj[1]["queryRef"] == "fact_x.Total Rows"
        assert "Measure" in proj[1]["field"]

    def test_measure_filter_is_advanced(self, tmp_path: Path) -> None:
        visual = Table(
            name="v",
            position=Position(x=0, y=0, width=1200, height=300),
            fields=[Column("dim_x", "region"), Measure("fact_x", "Total Rows")],
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        filters = vj["filterConfig"]["filters"]
        types = {
            f["field"].get("Measure", f["field"].get("Column"))["Property"]: f["type"]
            for f in filters
        }
        assert types["region"] == "Categorical"
        assert types["Total Rows"] == "Advanced"

    def test_sort_by_column(self, tmp_path: Path) -> None:
        visual = Table(
            name="v",
            position=Position(x=0, y=0, width=1200, height=300),
            fields=[Column("dim_x", "region")],
            order_by=TableOrderBy(field=Column("dim_x", "region"), direction="desc"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        sort = vj["visual"]["query"]["sortDefinition"]["sort"][0]
        assert sort["direction"] == "Descending"
        assert sort["field"]["Column"]["Property"] == "region"

    def test_no_sort_when_unset(self, tmp_path: Path) -> None:
        visual = Table(
            name="v",
            position=Position(x=0, y=0, width=1200, height=300),
            fields=[Column("dim_x", "region")],
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        assert "sortDefinition" not in vj["visual"]["query"]

    def test_measure_bound_title(self, tmp_path: Path) -> None:
        # NOTE: only the fully-styled title is attested in the references;
        # this minimal form's exact bytes still need a UI capture. The hook
        # is tested structurally, not byte-equal.
        visual = Table(
            name="v",
            position=Position(x=0, y=0, width=1200, height=300),
            fields=[Column("dim_x", "region")],
            title=Measure("fact_x", "Vendor Title"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        title = vj["visual"]["visualContainerObjects"]["title"][0]["properties"]
        assert title["show"]["expr"]["Literal"]["Value"] == "true"
        measure = title["text"]["expr"]["Measure"]
        assert measure["Expression"]["SourceRef"]["Entity"] == "fact_x"
        assert measure["Property"] == "Vendor Title"

    def test_rejects_empty_fields(self, tmp_path: Path) -> None:
        visual = Table(name="v", position=Position(x=0, y=0, width=1200, height=300))
        with pytest.raises(ValueError, match="at least one field"):
            _one_visual_report(visual).save_to_disk(tmp_path)


# ── Position numeric rendering ──────────────────────────────────────────────


class TestPosition:
    def test_whole_numbers_emit_as_ints(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=10, y=0, z=1, width=430, height=280),
            field=Column("dim_x", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        raw = (
            item_dir / "definition" / "pages" / "p" / "visuals" / "v" / "visual.json"
        ).read_text("utf-8")
        # Whole numbers must be bare ints, not 0.0 / 10.0.
        assert '"y": 0,' in raw
        assert '"z": 1,' in raw
        assert '"x": 10,' in raw
        # No float-rendered coordinates (the schema URL "2.10.0" is fine).
        assert '"x": 10.0' not in raw
        assert '"y": 0.0' not in raw

    def test_fractional_values_stay_floats(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=9.5, y=0, width=430, height=280),
            field=Column("dim_x", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        assert vj["position"]["x"] == 9.5

    def test_tab_order_defaults_to_z(self, tmp_path: Path) -> None:
        visual = Slicer(
            name="v",
            position=Position(x=0, y=0, z=3, width=200, height=80),
            field=Column("dim_x", "region"),
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        vj = _visual_json(item_dir, "p", "v")
        assert vj["position"]["tabOrder"] == 3


# ── Theme ───────────────────────────────────────────────────────────────────


class TestTheme:
    def _slicer_page(self) -> Page:
        return Page(
            display_name="P",
            name="p",
            visuals=[
                Slicer(
                    name="v",
                    position=Position(x=0, y=0, width=200, height=80),
                    field=Column("dim_x", "region"),
                ),
            ],
        )

    def test_theme_file_emitted_and_registered(self, tmp_path: Path) -> None:
        theme = Theme(
            name="MyTheme",
            content={
                "name": "MyTheme",
                "dataColors": ["#118DFF", "#12239E", "#E66C37"],
                "background": "#FFFFFF",
                "foreground": "#252423",
            },
        )
        item_dir = Report(
            "rpt",
            "../x.SemanticModel",
            [self._slicer_page()],
            theme=theme,
            strict_descriptions=False,
        ).save_to_disk(tmp_path)
        theme_path = (
            item_dir
            / "StaticResources"
            / "SharedResources"
            / "BaseThemes"
            / "MyTheme.json"
        )
        assert theme_path.exists()
        assert json.loads(theme_path.read_text("utf-8"))["name"] == "MyTheme"
        rj = json.loads((item_dir / "definition" / "report.json").read_text("utf-8"))
        assert rj["themeCollection"]["baseTheme"]["name"] == "MyTheme"
        assert rj["resourcePackages"][0]["items"][0]["name"] == "MyTheme"
        assert rj["resourcePackages"][0]["items"][0]["type"] == "BaseTheme"

    def test_no_theme_omits_collection_and_resources(self, tmp_path: Path) -> None:
        item_dir = Report(
            "rpt",
            "../x.SemanticModel",
            [self._slicer_page()],
            strict_descriptions=False,
        ).save_to_disk(tmp_path)
        rj = json.loads((item_dir / "definition" / "report.json").read_text("utf-8"))
        assert "themeCollection" not in rj
        assert "resourcePackages" not in rj


# ── Card (modern cardVisual) ────────────────────────────────────────────────


def _lit(node: dict) -> str:
    """Unwrap a ``{"expr": {"Literal": {"Value": ...}}}`` property."""
    return node["expr"]["Literal"]["Value"]


class TestCard:
    def _emit(self, tmp_path: Path, card: Card) -> dict:
        item_dir = _one_visual_report(card).save_to_disk(tmp_path)
        return _visual_json(item_dir, "p", card.name)

    def test_single_measure_minimal_card(self, tmp_path: Path) -> None:
        # Shape certified against a Fabric-round-tripped minimal status
        # card (borderless, no visual header, 10/8pt fonts).
        card = Card(
            name="v",
            position=Position(x=0, y=0, width=300, height=36),
            measure=Measure("fact_x", "Last Row Received"),
        )
        vj = self._emit(tmp_path, card)
        body = vj["visual"]
        assert body["visualType"] == "cardVisual"
        projections = body["query"]["queryState"]["Data"]["projections"]
        assert len(projections) == 1
        assert projections[0]["field"]["Measure"]["Property"] == "Last Row Received"
        assert "sortDefinition" not in body["query"]
        assert _lit(body["objects"]["value"][0]["properties"]["fontSize"]) == "10D"
        # Single-measure: only the id:"default" selector entries.
        assert [e["selector"] for e in body["objects"]["outline"]] == [
            {"id": "default"}
        ]
        assert (
            _lit(body["visualContainerObjects"]["border"][0]["properties"]["show"])
            == "false"
        )
        assert (
            _lit(
                body["visualContainerObjects"]["visualHeader"][0]["properties"]["show"]
            )
            == "false"
        )

    def test_multi_measure_kpi_strip(self, tmp_path: Path) -> None:
        # Shape certified against a Fabric-round-tripped 2-measure KPI card.
        card = Card(
            name="v",
            position=Position(x=0, y=0, width=220, height=120),
            measure=[
                Measure("fact_x", "Total Rows", format_string="#,0"),
                Measure("fact_x", "Total Amount", format_string="\\$#,0.00"),
            ],
            value_font_size=14,
            padding=8,
            show_border=True,
            show_visual_header=True,
        )
        body = self._emit(tmp_path, card)["visual"]
        projections = body["query"]["queryState"]["Data"]["projections"]
        assert [p["format"] for p in projections] == ["#,0", "\\$#,0.00"]
        # Default sort on the first measure, flagged as the captured default.
        sort_def = body["query"]["sortDefinition"]
        assert sort_def["isDefaultSort"] is True
        assert sort_def["sort"][0]["field"]["Measure"]["Property"] == "Total Rows"
        assert sort_def["sort"][0]["direction"] == "Descending"
        # Per-tile metadata selectors + the default entry.
        assert [e["selector"] for e in body["objects"]["outline"]] == [
            {"metadata": "fact_x.Total Rows"},
            {"metadata": "fact_x.Total Amount"},
            {"id": "default"},
        ]
        # Padding: per-tile entries for all but the first, then default.
        assert [e["selector"] for e in body["objects"]["padding"]] == [
            {"metadata": "fact_x.Total Amount"},
            {"id": "default"},
        ]
        # Divider before each tile after the first.
        assert [e["selector"] for e in body["objects"]["divider"]] == [
            {"metadata": "fact_x.Total Amount"}
        ]
        border = body["visualContainerObjects"]["border"][0]["properties"]
        assert _lit(border["show"]) == "true"
        assert _lit(border["radius"]) == "5D"

    def test_dividers_off(self, tmp_path: Path) -> None:
        card = Card(
            name="v",
            position=Position(x=0, y=0, width=220, height=120),
            measure=[Measure("fact_x", "A"), Measure("fact_x", "B")],
            show_dividers=False,
        )
        body = self._emit(tmp_path, card)["visual"]
        assert "divider" not in body["objects"]

    def test_title_text_emitted(self, tmp_path: Path) -> None:
        card = Card(
            name="v",
            position=Position(x=0, y=0, width=220, height=120),
            measure=Measure("fact_x", "A"),
            title="Rows loaded",
        )
        body = self._emit(tmp_path, card)["visual"]
        title = body["visualContainerObjects"]["title"][0]["properties"]
        assert _lit(title["text"]) == "'Rows loaded'"

    def test_measure_filters_are_advanced(self, tmp_path: Path) -> None:
        card = Card(
            name="v",
            position=Position(x=0, y=0, width=220, height=120),
            measure=[Measure("fact_x", "A"), Measure("fact_x", "B")],
        )
        vj = self._emit(tmp_path, card)
        assert [f["type"] for f in vj["filterConfig"]["filters"]] == [
            "Advanced",
            "Advanced",
        ]

    def test_display_units_dropped_with_warning(self, tmp_path: Path) -> None:
        card = Card(
            name="v",
            position=Position(x=0, y=0, width=220, height=120),
            measure=Measure("fact_x", "A"),
            display_units="Millions",
        )
        body = self._emit(tmp_path, card)["visual"]
        assert "displayUnits" not in json.dumps(body)

    def test_empty_measures_rejected(self, tmp_path: Path) -> None:
        card = Card(
            name="v",
            position=Position(x=0, y=0, width=220, height=120),
            measure=[],
        )
        with pytest.raises(ValueError, match="at least one"):
            _one_visual_report(card).save_to_disk(tmp_path)


class TestMultiCard:
    def test_emits_kpi_strip_defaults(self, tmp_path: Path) -> None:
        visual = MultiCard(
            name="v",
            position=Position(x=0, y=0, width=600, height=120),
            measures=[Measure("fact_x", "A"), Measure("fact_x", "B")],
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        body = _visual_json(item_dir, "p", "v")["visual"]
        assert body["visualType"] == "cardVisual"
        assert len(body["query"]["queryState"]["Data"]["projections"]) == 2
        assert _lit(body["objects"]["value"][0]["properties"]["fontSize"]) == "14D"
        # show_outline defaults True; accent bar defaults True.
        assert _lit(body["objects"]["outline"][0]["properties"]["show"]) == "true"
        assert [e["selector"] for e in body["objects"]["accentBar"]] == [
            {"metadata": "fact_x.A"},
            {"metadata": "fact_x.B"},
        ]
        assert (
            _lit(body["visualContainerObjects"]["border"][0]["properties"]["show"])
            == "true"
        )

    def test_legacy_styling_knobs_dropped(self, tmp_path: Path) -> None:
        visual = MultiCard(
            name="v",
            position=Position(x=0, y=0, width=600, height=120),
            measures=[Measure("fact_x", "A")],
            arrangement="columns",
            label_heading="Heading2",
        )
        item_dir = _one_visual_report(visual).save_to_disk(tmp_path)
        body = _visual_json(item_dir, "p", "v")["visual"]
        assert "Heading2" not in json.dumps(body)


# ── Column charts ───────────────────────────────────────────────────────────


class TestColumnChart:
    def _emit(self, tmp_path: Path, chart: ColumnChart) -> dict:
        item_dir = _one_visual_report(chart).save_to_disk(tmp_path)
        return _visual_json(item_dir, "p", chart.name)

    def test_category_and_measure_projections(self, tmp_path: Path) -> None:
        # Shape certified against a Fabric-round-tripped columnChart.
        chart = ColumnChart(
            name="v",
            position=Position(x=0, y=0, width=1200, height=400),
            category=Column("dim_x", "city", display_name="City"),
            values=[Measure("fact_x", "Total Rows", display_name="Rows")],
            value_axis_title="Rows",
            sort_by=Measure("fact_x", "Total Rows"),
            title="Rows by City",
        )
        vj = self._emit(tmp_path, chart)
        body = vj["visual"]
        assert body["visualType"] == "columnChart"
        cat = body["query"]["queryState"]["Category"]["projections"][0]
        assert cat["active"] is True
        assert cat["displayName"] == "City"
        y = body["query"]["queryState"]["Y"]["projections"][0]
        assert y["displayName"] == "Rows"
        assert "active" not in y
        sort_def = body["query"]["sortDefinition"]
        assert sort_def["isDefaultSort"] is True
        assert sort_def["sort"][0]["direction"] == "Descending"
        assert (
            _lit(body["objects"]["valueAxis"][0]["properties"]["titleText"]) == "'Rows'"
        )
        title = body["visualContainerObjects"]["title"][0]["properties"]
        assert _lit(title["text"]) == "'Rows by City'"
        assert "show" not in title  # visible title emits text only
        # Chart references carry no filterConfig at all.
        assert "filterConfig" not in vj

    def test_clustered_with_legend_and_axis_toggles(self, tmp_path: Path) -> None:
        # Shape certified against a Fabric-round-tripped clusteredColumnChart.
        chart = ClusteredColumnChart(
            name="v",
            position=Position(x=0, y=0, width=700, height=240),
            category=Column("dim_x", "name", display_name="Period"),
            values=[
                Measure("fact_x", "Prior Period", display_name="Last (full)"),
                Measure("fact_x", "Current Period", display_name="This (so far)"),
            ],
            legend=True,
            value_axis_title=True,
            category_axis_title=False,
            title="This period vs last",
            show_title=False,
        )
        body = self._emit(tmp_path, chart)["visual"]
        assert body["visualType"] == "clusteredColumnChart"
        assert len(body["query"]["queryState"]["Y"]["projections"]) == 2
        legend = body["objects"]["legend"][0]["properties"]
        assert _lit(legend["show"]) == "true"
        assert _lit(legend["showGradientLegend"]) == "true"
        assert (
            _lit(body["objects"]["valueAxis"][0]["properties"]["showAxisTitle"])
            == "true"
        )
        assert (
            _lit(body["objects"]["categoryAxis"][0]["properties"]["showAxisTitle"])
            == "false"
        )
        title = body["visualContainerObjects"]["title"][0]["properties"]
        assert _lit(title["text"]) == "'This period vs last'"
        assert _lit(title["show"]) == "false"

    def test_axis_objects_omitted_when_unset(self, tmp_path: Path) -> None:
        chart = ColumnChart(
            name="v",
            position=Position(x=0, y=0, width=700, height=240),
            category=Column("dim_x", "city"),
            values=[Measure("fact_x", "Total Rows")],
        )
        body = self._emit(tmp_path, chart)["visual"]
        assert "valueAxis" not in body["objects"]
        assert "categoryAxis" not in body["objects"]
        assert "legend" not in body["objects"]
        assert "sortDefinition" not in body["query"]

    def test_missing_category_rejected(self, tmp_path: Path) -> None:
        chart = ColumnChart(
            name="v",
            position=Position(x=0, y=0, width=700, height=240),
            values=[Measure("fact_x", "Total Rows")],
        )
        with pytest.raises(ValueError, match="category"):
            _one_visual_report(chart).save_to_disk(tmp_path)

    def test_missing_values_rejected(self, tmp_path: Path) -> None:
        chart = ColumnChart(
            name="v",
            position=Position(x=0, y=0, width=700, height=240),
            category=Column("dim_x", "city"),
        )
        with pytest.raises(ValueError, match="at least one measure"):
            _one_visual_report(chart).save_to_disk(tmp_path)


# ── Automatic page refresh (APR) ────────────────────────────────────────────


class TestPageRefresh:
    def _page_json(self, tmp_path: Path, page: Page) -> dict:
        report = Report("rpt", "../x.SemanticModel", [page], strict_descriptions=False)
        item_dir = report.save_to_disk(tmp_path)
        p = item_dir / "definition" / "pages" / page.name / "page.json"
        return json.loads(p.read_text("utf-8"))

    def test_page_refresh_emits_captured_apr_shape(self, tmp_path: Path) -> None:
        pj = self._page_json(
            tmp_path, Page(display_name="P", name="p", page_refresh="PT5M")
        )
        apr = pj["objects"]["pageRefresh"][0]["properties"]
        assert _lit(apr["show"]) == "true"
        # Single-quoted ISO-8601 duration, captured from a Fabric
        # round-trip.
        assert _lit(apr["duration"]) == "'PT5M'"
        # Fixed-interval APR has NO refreshType property.
        assert "refreshType" not in apr

    def test_no_objects_key_without_page_refresh(self, tmp_path: Path) -> None:
        pj = self._page_json(tmp_path, Page(display_name="P", name="p"))
        assert "objects" not in pj


# ── NotImplemented boundaries (features without attested PBIR bytes) ─────────


class TestNotImplementedBoundaries:
    def test_inline_aggregate_projection_raises(self, tmp_path: Path) -> None:
        visual = Table(
            name="v",
            position=Position(x=0, y=0, width=1200, height=300),
            fields=[Aggregate("fact_x", "missing_field_count", function="sum")],
        )
        with pytest.raises(NotImplementedError, match="Aggregate"):
            _one_visual_report(visual).save_to_disk(tmp_path)

    def test_sort_by_measure_raises(self, tmp_path: Path) -> None:
        visual = Table(
            name="v",
            position=Position(x=0, y=0, width=1200, height=300),
            fields=[Column("dim_x", "region"), Measure("fact_x", "M")],
            order_by=TableOrderBy(field=Measure("fact_x", "M")),
        )
        with pytest.raises(NotImplementedError, match="sort-by-measure"):
            _one_visual_report(visual).save_to_disk(tmp_path)


# ── Strict descriptions ─────────────────────────────────────────────────────


class TestStrictDescriptions:
    def _page(self) -> Page:
        return Page(
            display_name="P",
            name="p",
            visuals=[
                Slicer(
                    name="v",
                    position=Position(x=0, y=0, width=200, height=80),
                    field=Column("dim_x", "region"),
                ),
            ],
        )

    def test_default_strict_blocks_save_when_description_missing(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(ReportError, match="needs a description"):
            Report("rpt", "../x.SemanticModel", [self._page()]).save_to_disk(tmp_path)

    def test_description_present_passes(self, tmp_path: Path) -> None:
        Report(
            "rpt",
            "../x.SemanticModel",
            [self._page()],
            description="A real report description.",
        ).save_to_disk(tmp_path)

    def test_opt_out_succeeds_when_description_missing(self, tmp_path: Path) -> None:
        Report(
            "rpt", "../x.SemanticModel", [self._page()], strict_descriptions=False
        ).save_to_disk(tmp_path)

    def test_whitespace_only_description_treated_as_missing(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(ReportError, match="needs a description"):
            Report(
                "rpt", "../x.SemanticModel", [self._page()], description="   "
            ).save_to_disk(tmp_path)
