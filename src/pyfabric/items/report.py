"""Build Fabric Report items (modern **PBIR** format) by hand.

A Page-and-Visual builder for slicers and detail tables linked to a
SemanticModel via a relative ``byPath`` reference. Emits the modern PBIR
``definition/`` folder format — a separate ``page.json`` per page and a
separate ``visual.json`` per visual — which is Microsoft's current
diff-able report format and the one current Fabric opens reliably.

Scope (what emits byte-confident PBIR today):

- **Slicer** → a ``listSlicer`` with one or more column projections
  (the first ``active``). The ``listSlicer`` has a built-in search box;
  there is no property to author for it.
- **Table** → a ``tableEx`` with mixed :class:`Column` / :class:`Measure`
  projections, an optional sort on a single column, and a per-projection
  ``filterConfig`` (columns ``Categorical``, measures ``Advanced``).
- **Card** / **MultiCard** → a modern ``cardVisual`` with 1..n measure
  projections (per-tile ``format`` / ``displayName``), value/label font
  sizes, border/outline/padding/divider objects.
- **ColumnChart** / **ClusteredColumnChart** → a ``columnChart`` /
  ``clusteredColumnChart`` with one category and 1..n measures, legend,
  data labels, and category/value axis-title toggles.
- **Automatic page refresh** → ``Page(page_refresh="PT5M")`` emits the
  captured fixed-interval APR shape (single-quoted ISO-8601 duration; no
  ``refreshType`` property).

PBIR shapes that still need a UI capture (no attested reference bytes —
would be guessed, so they are dropped-with-warning or raise rather than
emit invented JSON): tooltip pages, bookmarks, drillthrough,
hierarchy/expansion slicers, slicer ``mode`` variants (Dropdown/Between),
``allow_values`` slicer filters, inline column aggregations,
sort-by-measure in tables, card ``display_units``, and the dynamic
title (only the fully-styled form is attested; the minimal form emitted
here is provisional). Each is handled at the point it would be emitted.

Every write routes through
:func:`pyfabric.items.normalize.write_artifact_file` so emitted bytes
match Fabric's per-file-type byte convention (LF, no BOM, no trailing
newline for report JSON) and won't trigger sync flap.

Usage::

    from pathlib import Path
    from pyfabric.items.report import (
        Column,
        Measure,
        Page,
        Position,
        Report,
        Slicer,
        Table,
        TableOrderBy,
    )

    page = Page(
        display_name="Page 1",
        width=1280,
        height=720,
        visuals=[
            Slicer(
                position=Position(x=10, y=0, width=430, height=280),
                field=Column("dim_x", "name"),
            ),
            Table(
                position=Position(x=460, y=0, width=780, height=270),
                fields=[
                    Column("dim_x", "name"),
                    Column("dim_x", "city"),
                ],
                order_by=TableOrderBy(field=Column("dim_x", "name")),
            ),
        ],
    )

    Report(
        name="rpt_my_report",
        semantic_model_path="../sm_my_model.SemanticModel",
        pages=[page],
        description="An example report.",
    ).save_to_disk(Path("ws/"))
"""

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import structlog

from pyfabric.items.normalize import write_artifact_file

log = structlog.get_logger()


# ── Public types ────────────────────────────────────────────────────────────


SlicerMode = Literal["Dropdown", "Basic", "Between"]
DisplayUnits = Literal["Auto", "None", "Thousands", "Millions", "Billions", "Trillions"]
SortDirection = Literal["asc", "desc"]
AggregationFunction = Literal["sum", "avg", "min", "max", "count", "distinctCount"]
CardArrangement = Literal["rows", "columns"]
LabelPosition = Literal["belowValue", "aboveValue"]
# Power BI typography roles. Theme-driven font size + weight; preferable
# to setting fontSize directly because the role adapts on theme swap.
LabelHeading = Literal["Heading1", "Heading2", "Heading3", "Body"]

# PBIR sort directions (the modern format uses spelled-out enum strings,
# not the legacy integer codes).
_SORT_DIRECTION: dict[SortDirection, str] = {
    "asc": "Ascending",
    "desc": "Descending",
}

# Stable UUID namespace for deterministic visual / page / filter ids.
_REPORT_NS = uuid.UUID("c1d2e3f4-0001-4000-8000-000000000000")


# ── Theme types ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ThemeColor:
    """A reference to a color in the report's theme palette.

    Use in place of hex literals so visuals adapt when the theme is
    swapped. ``color_id`` indexes the theme's ``dataColors`` list
    (0-based; theme conventions vary). ``percent`` tints the color
    (0.0 = full saturation, 0.6 = soft tint suitable for backgrounds).
    """

    color_id: int
    percent: float = 0.0


@dataclass(frozen=True)
class Theme:
    """A Power BI base theme bundled with the report.

    The ``content`` dict is emitted verbatim as the theme JSON file at
    ``StaticResources/SharedResources/BaseThemes/<name>.json`` — pyfabric
    doesn't validate or rewrite it. At minimum, Power BI expects keys
    like ``name``, ``dataColors`` (a list of hex strings), and
    ``background`` / ``foreground``; for everything else, see Microsoft's
    Power BI report-theme JSON schema.
    """

    name: str
    content: dict[str, Any]


# ── Field references ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class Column:
    """A column reference in the SemanticModel, used inside a visual.

    ``entity`` is the SemanticModel table name; ``name`` is the column
    name on that table. ``format_string``, when set, is emitted as the
    projection-level ``format`` (attested in Fabric-round-tripped
    cardVisual bytes). ``display_name`` renames the field within the
    visual (the projection-level ``displayName``, attested in the chart
    references).
    """

    entity: str
    name: str
    format_string: str | None = None
    display_name: str | None = None


@dataclass(frozen=True)
class Measure:
    """A measure reference in the SemanticModel, used inside a visual.

    ``format_string``, when set, is emitted as the projection-level
    ``format``; ``display_name`` as the projection-level ``displayName``
    (both attested in Fabric-round-tripped reference bytes).
    """

    entity: str
    name: str
    format_string: str | None = None
    display_name: str | None = None


@dataclass(frozen=True)
class Aggregate:
    """An inline aggregation of a column inside a visual.

    Retained for source compatibility. The modern PBIR projection shape
    for an inline aggregation is **not attested** in the reference
    reports (they project only columns and model measures), so a
    :class:`Table` that references an ``Aggregate`` raises
    :class:`NotImplementedError` on save until UI-captured bytes exist.
    """

    entity: str
    column: str
    function: AggregationFunction
    format_string: str | None = None


# Anything that can be a field reference in a visual projection.
FieldRef = Column | Measure | Aggregate


# ── Position ────────────────────────────────────────────────────────────────


@dataclass
class Position:
    """Visual placement on a Page (canvas coordinates in pixels).

    Whole-number coordinates emit as JSON ints (``"y": 0``), matching
    Fabric's output for non-dragged visuals; fractional values emit as
    floats. ``tab_order`` defaults to ``z`` when left at 0.
    """

    x: float
    y: float
    width: float
    height: float
    z: float = 0.0
    tab_order: int | None = None


# ── Visuals ─────────────────────────────────────────────────────────────────


@dataclass
class Visual:
    """Base class for every visual on a page (do not instantiate directly)."""

    position: Position
    name: str = ""  # auto-filled from page+index if blank


@dataclass
class Slicer(Visual):
    """A slicer visual, emitted as a ``listSlicer``.

    ``field`` accepts a single :class:`Column` (the common case) or a
    list of columns. The first projection is marked ``active``; the rest
    inactive. The ``listSlicer`` has a built-in search box.

    ``mode`` and ``allow_values`` are accepted for source compatibility
    but are **not yet emitted** in PBIR — the modern bytes for non-list
    slicer modes and for a hardcoded allow-list filter need a UI capture
    (the only attested reference is a bare ``listSlicer``). A hierarchy
    (multi-column ``field``) emits as additional inactive projections,
    not as an ``expansionStates`` drill hierarchy (also un-attested).
    """

    field: Column | list[Column] = field(default_factory=lambda: Column("", ""))
    mode: SlicerMode = "Dropdown"
    allow_values: list[str] | None = None

    @property
    def field_levels(self) -> list[Column]:
        """Always-a-list view of ``field``; single column → one-element list."""
        return self.field if isinstance(self.field, list) else [self.field]

    @property
    def is_hierarchy(self) -> bool:
        return isinstance(self.field, list) and len(self.field) > 1

    @property
    def leaf_field(self) -> Column:
        """The deepest column in the hierarchy (or the only column)."""
        return self.field_levels[-1]


@dataclass
class Card(Visual):
    """A KPI card, emitted as a modern ``cardVisual`` with 1..n measures.

    ``measure`` accepts a single :class:`Measure` or a list (each measure
    renders as a tile; a divider separates tiles when ``show_dividers``).
    Set ``Measure.format_string`` for a per-tile display format (the
    projection-level ``format``) and ``Measure.display_name`` for a tile
    label override.

    Defaults match the attested minimal status card (borderless, no
    visual header, small value font). For the bordered multi-metric KPI
    strip look, set ``value_font_size=14``, ``padding=8``,
    ``show_border=True``, ``show_visual_header=True``.

    ``display_units`` is accepted for source compatibility but has no
    attested modern ``cardVisual`` bytes — a non-default value is dropped
    with a warning (bake scaling into the measure's ``format_string``
    instead).
    """

    measure: Measure | list[Measure] = field(default_factory=lambda: Measure("", ""))
    display_units: DisplayUnits = "None"
    title: str | None = None
    value_font_size: int = 10
    label_font_size: int = 8
    show_labels: bool = True
    padding: int = 2
    show_border: bool = False
    border_radius: int = 5
    show_visual_header: bool = False
    show_dividers: bool = True

    @property
    def measures(self) -> list[Measure]:
        """Always-a-list view of ``measure``."""
        return self.measure if isinstance(self.measure, list) else [self.measure]


@dataclass
class MultiCard(Visual):
    """A multi-metric KPI strip, emitted as a multi-measure ``cardVisual``.

    Equivalent to a :class:`Card` with the KPI-strip styling defaults
    (larger value font, uniform 8px tile padding, rounded border, visual
    header, dividers between tiles). ``show_outline`` / ``show_accent_bar``
    / ``show_shadow`` map to the per-tile ``outline`` / ``accentBar`` /
    ``shadowCustom`` objects.

    ``display_units``, ``arrangement``, ``label_heading``,
    ``label_position``, and ``label_font_color`` are accepted for source
    compatibility but have no attested modern ``cardVisual`` bytes —
    non-default values are dropped with a warning.
    """

    measures: list[Measure] = field(default_factory=list)
    display_units: DisplayUnits = "None"
    arrangement: CardArrangement = "rows"
    show_outline: bool = True
    show_accent_bar: bool = True
    show_shadow: bool = False
    label_heading: LabelHeading | None = None
    label_position: LabelPosition | None = None
    label_font_color: ThemeColor | None = None


@dataclass
class TableOrderBy:
    """Sort spec for a Table visual.

    Only sort-by-:class:`Column` is byte-confident from the references.
    Sorting by a :class:`Measure` or :class:`Aggregate` raises
    :class:`NotImplementedError` on save (un-attested PBIR shape).
    """

    field: FieldRef
    direction: SortDirection = "asc"


@dataclass
class Table(Visual):
    """A table visual, emitted as a ``tableEx``.

    ``fields`` may mix :class:`Column` and :class:`Measure` projections.
    ``order_by`` is optional and currently supports sort-by-column only.

    ``title`` optionally binds a measure-driven dynamic title (the
    ``visualContainerObjects.title`` shape). **Caveat:** only the
    *fully-styled* title form is attested in the reference reports; the
    minimal form emitted here (``show`` + ``text``) needs a UI capture to
    certify its exact bytes. The hook is provided so callers can author
    one, but treat its bytes as provisional.
    """

    fields: list[FieldRef] = field(default_factory=list)
    order_by: TableOrderBy | None = None
    title: Measure | None = None


@dataclass
class ColumnChart(Visual):
    """A column chart (``columnChart``): one category, 1..n measures.

    ``category`` projects into the ``Category`` role (marked ``active``);
    ``values`` into ``Y``. Use ``display_name`` on the refs to relabel
    axis/legend entries. ``sort_by`` (a projected measure) emits the
    chart's default sort, descending unless ``sort_direction="asc"``.

    Axis-title knobs: ``value_axis_title`` accepts a string (custom axis
    title text), ``True``/``False`` (show/hide the default title), or
    ``None`` (leave Fabric's default). ``category_axis_title`` accepts
    ``True``/``False``/``None`` the same way.

    ``title`` is the visual-container title text; ``show_title=False``
    keeps the text but hides it (the attested captured state for charts
    whose page already explains them).
    """

    category: Column = field(default_factory=lambda: Column("", ""))
    values: list[Measure] = field(default_factory=list)
    data_labels: bool = True
    legend: bool = False
    value_axis_title: str | bool | None = None
    category_axis_title: bool | None = None
    sort_by: Measure | None = None
    sort_direction: SortDirection = "desc"
    title: str | None = None
    show_title: bool = True
    show_border: bool = True
    border_radius: int = 5


@dataclass
class ClusteredColumnChart(ColumnChart):
    """A clustered column chart (``clusteredColumnChart``).

    Same authoring surface as :class:`ColumnChart`; multiple ``values``
    render side-by-side per category instead of as separate columns.
    Typically paired with ``legend=True`` so the series are labeled.
    """


# ── Page ────────────────────────────────────────────────────────────────────


PageDisplayOption = Literal["FitToPage", "ActualSize", "FitToWidth"]


@dataclass
class Page:
    """A single report page.

    ``name`` is the page's id (also its folder name under
    ``definition/pages/``). Leave blank to derive a deterministic id from
    the report + display name; pin it for byte-stable output.

    ``page_refresh`` enables automatic page refresh (APR) with a
    fixed interval, e.g. ``"PT5M"`` for every 5 minutes. The value is an
    ISO-8601 duration emitted as a single-quoted literal — the shape was
    captured from a Fabric round-trip (fixed-interval APR has **no**
    ``refreshType`` property; don't add one). APR only takes effect on
    DirectQuery-backed pages; Fabric ignores it elsewhere.
    """

    display_name: str
    visuals: list[Visual] = field(default_factory=list)
    width: float = 1280.0
    height: float = 720.0
    name: str = ""  # auto-filled from display_name if blank
    display_option: PageDisplayOption = "FitToPage"
    page_refresh: str | None = None


# ── Report ──────────────────────────────────────────────────────────────────


class ReportError(Exception):
    """Raised when the report fails pre-emit validation."""


@dataclass
class Report:
    """A full Fabric Report item, emitted in modern PBIR format.

    ``semantic_model_path`` is a relative path from the report folder to
    a sibling ``*.SemanticModel`` folder (e.g. ``"../sm_x.SemanticModel"``),
    emitted as a ``definition.pbir`` ``byPath`` reference.

    ``theme``, when set, bundles a base theme JSON file at
    ``StaticResources/SharedResources/BaseThemes/<theme.name>.json`` and
    registers it as the report's base theme. Without a theme, Power BI
    applies its workspace default. pyfabric never bundles a default
    theme — the caller supplies it.

    **A non-empty description is required by default.** It is validated
    and surfaced through the API, but — per Fabric's behavior — it is
    **not written into ``.platform``** (Fabric strips report descriptions
    there and would otherwise flap on sync). Set
    ``strict_descriptions=False`` to skip the validation (a warning is
    logged).
    """

    name: str
    semantic_model_path: str
    pages: list[Page]
    description: str = ""
    theme: Theme | None = None
    strict_descriptions: bool = True
    logical_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def validate(self) -> list[str]:
        """Return a list of human-readable error messages.

        Empty list means the report passes pre-emit validation. Called
        automatically from :meth:`save_to_disk`; expose it separately so
        callers can lint without writing.
        """
        errors: list[str] = []
        if not (self.description or "").strip():
            if self.strict_descriptions:
                errors.append(
                    f"report {self.name!r} needs a description "
                    f"(strict_descriptions=True; descriptions surface in "
                    f"the workspace listing and item info pane). Set "
                    f"Report(strict_descriptions=False) to opt out."
                )
            else:
                log.warning(
                    "report has no description (strict_descriptions=False — opt-out)",
                    report=self.name,
                )
        return errors

    def save_to_disk(self, output_dir: Path | str) -> Path:
        """Emit the full ``<name>.Report`` folder in modern PBIR format.

        Returns the path to the created folder. Raises :class:`ReportError`
        if pre-emit validation fails, and :class:`NotImplementedError`
        for visuals whose PBIR bytes are not yet implemented. All writes
        route through :func:`pyfabric.items.normalize.write_artifact_file`.
        """
        errors = self.validate()
        if errors:
            joined = "\n  - ".join(errors)
            raise ReportError(f"report {self.name!r} failed validation:\n  - {joined}")

        output_dir = Path(output_dir)
        item_dir = output_dir / f"{self.name}.Report"

        # Stamp deterministic ids on any unnamed pages/visuals before emit.
        for page_index, page in enumerate(self.pages):
            if not page.name:
                page.name = _id20(self.name, page.display_name, str(page_index))
            for visual_index, visual in enumerate(page.visuals):
                if not visual.name:
                    visual.name = _id20(
                        self.name, page.name, type(visual).__name__, str(visual_index)
                    )

        write_artifact_file(item_dir / ".platform", self._emit_platform())
        write_artifact_file(item_dir / "definition.pbir", self._emit_pbir())
        write_artifact_file(
            item_dir / "definition" / "version.json", self._emit_version()
        )
        write_artifact_file(
            item_dir / "definition" / "report.json", self._emit_report_json()
        )
        write_artifact_file(
            item_dir / "definition" / "pages" / "pages.json", self._emit_pages_json()
        )
        for page in self.pages:
            page_dir = item_dir / "definition" / "pages" / page.name
            write_artifact_file(page_dir / "page.json", _emit_page_json(page))
            for visual in page.visuals:
                visual_dir = page_dir / "visuals" / visual.name
                write_artifact_file(
                    visual_dir / "visual.json", _emit_visual_json(visual)
                )

        if self.theme is not None:
            theme_path = (
                item_dir
                / "StaticResources"
                / "SharedResources"
                / "BaseThemes"
                / f"{self.theme.name}.json"
            )
            write_artifact_file(theme_path, json.dumps(self.theme.content, indent=2))

        log.info(
            "report.save_to_disk complete",
            report=self.name,
            pages=len(self.pages),
            visuals=sum(len(p.visuals) for p in self.pages),
            path=str(item_dir),
        )
        return item_dir

    # ── File emitters ──────────────────────────────────────────────────────

    def _emit_platform(self) -> str:
        # NOTE: no ``description`` key — Fabric strips report descriptions
        # from .platform on sync, so emitting one causes a permanent flap.
        return json.dumps(
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                "metadata": {
                    "type": "Report",
                    "displayName": self.name,
                },
                "config": {"version": "2.0", "logicalId": self.logical_id},
            },
            indent=2,
        )

    def _emit_pbir(self) -> str:
        return json.dumps(
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
                "version": "4.0",
                "datasetReference": {"byPath": {"path": self.semantic_model_path}},
            },
            indent=2,
        )

    def _emit_version(self) -> str:
        return json.dumps(
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
                "version": "2.0.0",
            },
            indent=2,
        )

    def _emit_report_json(self) -> str:
        payload: dict[str, Any] = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.3.0/schema.json",
        }
        if self.theme is not None:
            payload["themeCollection"] = {
                "baseTheme": {
                    "name": self.theme.name,
                    "reportVersionAtImport": {
                        "visual": "2.9.0",
                        "report": "3.3.0",
                        "page": "2.3.1",
                    },
                    "type": "SharedResources",
                }
            }
        payload["objects"] = {
            "section": [
                {
                    "properties": {
                        "verticalAlignment": {"expr": {"Literal": {"Value": "'Top'"}}}
                    }
                }
            ]
        }
        if self.theme is not None:
            payload["resourcePackages"] = [
                {
                    "name": "SharedResources",
                    "type": "SharedResources",
                    "items": [
                        {
                            "name": self.theme.name,
                            "path": f"BaseThemes/{self.theme.name}.json",
                            "type": "BaseTheme",
                        }
                    ],
                }
            ]
        payload["settings"] = {
            "useStylableVisualContainerHeader": True,
            "exportDataMode": "AllowSummarized",
            "defaultDrillFilterOtherVisuals": True,
            "allowChangeFilterTypes": True,
            "useEnhancedTooltips": True,
            "useDefaultAggregateDisplayName": True,
        }
        return json.dumps(payload, indent=2)

    def _emit_pages_json(self) -> str:
        return json.dumps(
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json",
                "pageOrder": [p.name for p in self.pages],
                "activePageName": self.pages[0].name if self.pages else "",
            },
            indent=2,
        )


# ── Internal: id helpers ───────────────────────────────────────────────────


def _id20(*parts: str) -> str:
    """Deterministic 20-char hex id matching Fabric's visual/page id shape."""
    return uuid.uuid5(_REPORT_NS, ".".join(parts)).hex[:20]


def _num(value: float) -> float | int:
    """Whole numbers emit as ints (``0`` not ``0.0``); fractions stay floats.

    Matches Fabric's PBIR position output, where un-dragged coordinates
    are JSON integers and drag values are floats.
    """
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


# ── Internal: object-property literal expressions ───────────────────────────
#
# Visual object properties are tiny expression trees whose leaf is a typed
# literal token: booleans are bare ``true``/``false``, sizes carry a type
# suffix (``14D`` decimal, ``8L`` long — as captured from Fabric output),
# and strings are single-quoted.


def _literal(value: bool) -> dict[str, Any]:
    return {"expr": {"Literal": {"Value": "true" if value else "false"}}}


def _literal_num(value: int, suffix: str = "D") -> dict[str, Any]:
    return {"expr": {"Literal": {"Value": f"{value}{suffix}"}}}


def _literal_str(value: str) -> dict[str, Any]:
    return {"expr": {"Literal": {"Value": f"'{value}'"}}}


# ── Internal: page / visual emitters ───────────────────────────────────────


def _emit_page_json(page: Page) -> str:
    payload: dict[str, Any] = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json",
        "name": page.name,
        "displayName": page.display_name,
        "displayOption": page.display_option,
        "height": _num(page.height),
        "width": _num(page.width),
    }
    if page.page_refresh is not None:
        # Captured from a Fabric round-trip: ``duration`` is a
        # single-quoted ISO-8601 duration literal, and fixed-interval APR
        # has NO ``refreshType`` property (not derivable from the
        # published schema — do not add one).
        payload["objects"] = {
            "pageRefresh": [
                {
                    "properties": {
                        "show": _literal(True),
                        "duration": _literal_str(page.page_refresh),
                    }
                }
            ]
        }
    return json.dumps(payload, indent=2)


def _emit_visual_json(v: Visual) -> str:
    payload: dict[str, Any] = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.10.0/schema.json",
        "name": v.name,
        "position": _emit_position(v.position),
        "visual": _emit_visual_body(v),
    }
    filter_config = _emit_filter_config(v)
    if filter_config["filters"]:
        # Chart references carry no filterConfig at all — omit the key
        # rather than emit an empty list Fabric never writes.
        payload["filterConfig"] = filter_config
    return json.dumps(payload, indent=2)


def _emit_position(p: Position) -> dict[str, Any]:
    tab_order = p.tab_order if p.tab_order is not None else int(p.z)
    return {
        "x": _num(p.x),
        "y": _num(p.y),
        "z": _num(p.z),
        "height": _num(p.height),
        "width": _num(p.width),
        "tabOrder": tab_order,
    }


def _emit_visual_body(v: Visual) -> dict[str, Any]:
    """Dispatch to the per-visual ``visual`` body emitter."""
    if isinstance(v, Slicer):
        return _emit_slicer_body(v)
    if isinstance(v, Table):
        return _emit_table_body(v)
    if isinstance(v, Card):
        return _emit_card_body(v)
    if isinstance(v, MultiCard):
        return _emit_multicard_body(v)
    if isinstance(v, ColumnChart):  # covers ClusteredColumnChart
        return _emit_column_chart_body(v)
    raise TypeError(f"unsupported visual type: {type(v).__name__}")


# ── Field-reference shapes (modern PBIR) ────────────────────────────────────


def _field_ref(ref: Column | Measure) -> dict[str, Any]:
    """The ``field.{Column|Measure}.Expression.SourceRef.Entity`` shape.

    Modern PBIR references an entity directly — no From/Select alias
    machinery (that was the legacy prototype-query form).
    """
    kind = "Measure" if isinstance(ref, Measure) else "Column"
    return {
        "field": {
            kind: {
                "Expression": {"SourceRef": {"Entity": ref.entity}},
                "Property": ref.name,
            }
        }
    }


def _projection(ref: Column | Measure, *, active: bool | None = None) -> dict[str, Any]:
    """A ``query.queryState.<Role>.projections[]`` entry.

    Key order matches the Fabric-round-tripped references: ``field``,
    ``queryRef``, ``nativeQueryRef``, then ``active`` (when applicable),
    ``displayName``, and ``format``.
    """
    proj = _field_ref(ref)
    proj["queryRef"] = f"{ref.entity}.{ref.name}"
    proj["nativeQueryRef"] = ref.name
    if active is not None:
        proj["active"] = active
    if ref.display_name is not None:
        proj["displayName"] = ref.display_name
    if ref.format_string is not None:
        proj["format"] = ref.format_string
    return proj


def _projectable(ref: FieldRef) -> Column | Measure:
    """Narrow a field ref to a projectable one, or raise for un-attested kinds."""
    if isinstance(ref, Aggregate):
        raise NotImplementedError(
            "inline Aggregate projection has no attested PBIR shape — "
            "use a model measure or capture reference bytes"
        )
    return ref


# ── Slicer emitter ──────────────────────────────────────────────────────────


def _emit_slicer_body(s: Slicer) -> dict[str, Any]:
    levels = s.field_levels
    entities = {c.entity for c in levels}
    if len(entities) > 1:
        raise ValueError(
            f"Slicer hierarchy levels must share an entity; got {sorted(entities)}"
        )
    # These two knobs are accepted for source compatibility but have no
    # attested modern-PBIR bytes yet, so they are dropped rather than
    # guessed. Warn loudly — silently emitting a slicer that ignores a
    # caller's scoping intent is exactly the failure class this migration
    # set out to remove. See the module docstring's "PBIR shapes that
    # still need a UI capture" note.
    if s.mode != "Dropdown":
        log.warning(
            "Slicer mode ignored — listSlicer has no attested PBIR mode bytes; "
            "emitting a plain listSlicer (needs UI capture to support modes)",
            slicer=s.name,
            requested_mode=s.mode,
        )
    if s.allow_values:
        log.warning(
            "Slicer allow_values ignored — the hardcoded allow-list filter has "
            "no attested PBIR bytes yet; the slicer will NOT be scoped to those "
            "values (needs UI capture)",
            slicer=s.name,
            allow_values=s.allow_values,
        )
    projections = [_projection(col, active=i == 0) for i, col in enumerate(levels)]
    return {
        "visualType": "listSlicer",
        "query": {"queryState": {"Values": {"projections": projections}}},
        "drillFilterOtherVisuals": True,
    }


# ── Table emitter ──────────────────────────────────────────────────────────


def _emit_table_body(t: Table) -> dict[str, Any]:
    if not t.fields:
        raise ValueError("Table requires at least one field")
    projectables = [_projectable(f) for f in t.fields]

    query: dict[str, Any] = {
        "queryState": {
            "Values": {"projections": [_projection(f) for f in projectables]}
        }
    }
    if t.order_by is not None:
        query["sortDefinition"] = {"sort": [_sort_entry(t.order_by)]}

    body: dict[str, Any] = {
        "visualType": "tableEx",
        "query": query,
    }
    if t.title is not None:
        body["visualContainerObjects"] = {
            "title": [
                {
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {
                            "expr": {
                                "Measure": {
                                    "Expression": {
                                        "SourceRef": {"Entity": t.title.entity}
                                    },
                                    "Property": t.title.name,
                                }
                            }
                        },
                    }
                }
            ]
        }
    body["drillFilterOtherVisuals"] = True
    return body


def _sort_entry(ob: TableOrderBy) -> dict[str, Any]:
    if not isinstance(ob.field, Column):
        raise NotImplementedError(
            "sort-by-measure/aggregate has no attested PBIR shape — "
            "sort by a column or capture reference bytes"
        )
    entry = _field_ref(ob.field)
    entry["direction"] = _SORT_DIRECTION[ob.direction]
    return entry


# ── Card emitter (modern cardVisual) ────────────────────────────────────────


def _default_sort_definition(ref: Column | Measure, direction: str) -> dict[str, Any]:
    """A ``sortDefinition`` marking the visual's captured default sort."""
    entry = _field_ref(ref)
    entry["direction"] = direction
    return {"sort": [entry], "isDefaultSort": True}


def _metadata_selector(ref: Column | Measure) -> dict[str, str]:
    return {"metadata": f"{ref.entity}.{ref.name}"}


def _card_objects(
    measures: list[Measure],
    *,
    value_font_size: int,
    label_font_size: int,
    show_labels: bool,
    padding: int,
    show_dividers: bool,
    outline: bool,
    accent_bar: bool = False,
    shadow: bool = False,
) -> dict[str, Any]:
    """The ``objects`` block of a cardVisual, following the reference bytes.

    Multi-measure cards carry per-tile (``selector.metadata``) entries for
    outline/padding/divider plus the ``id: "default"`` entry; single-measure
    cards carry only the default entries — both as captured from Fabric.
    """
    multi = len(measures) > 1
    objects: dict[str, Any] = {
        "value": [
            {
                "properties": {
                    "horizontalAlignment": _literal_str("center"),
                    "fontSize": _literal_num(value_font_size),
                },
                "selector": {"id": "default"},
            }
        ]
    }

    outline_entries: list[dict[str, Any]] = []
    if multi:
        outline_entries.extend(
            {
                "properties": {"show": _literal(outline)},
                "selector": _metadata_selector(m),
            }
            for m in measures
        )
    outline_entries.append(
        {"properties": {"show": _literal(outline)}, "selector": {"id": "default"}}
    )
    objects["outline"] = outline_entries

    padding_entries: list[dict[str, Any]] = []
    if multi:
        padding_entries.extend(
            {
                "properties": {"paddingUniform": _literal_num(padding, "L")},
                "selector": _metadata_selector(m),
            }
            for m in measures[1:]
        )
    padding_entries.append(
        {
            "properties": {"paddingUniform": _literal_num(padding, "L")},
            "selector": {"id": "default"},
        }
    )
    objects["padding"] = padding_entries

    if multi and show_dividers:
        objects["divider"] = [
            {"properties": {"show": _literal(True)}, "selector": _metadata_selector(m)}
            for m in measures[1:]
        ]
    if accent_bar:
        objects["accentBar"] = [
            {"properties": {"show": _literal(True)}, "selector": _metadata_selector(m)}
            for m in measures
        ]
    if shadow:
        objects["shadowCustom"] = [
            {"properties": {"show": _literal(True)}, "selector": _metadata_selector(m)}
            for m in measures
        ]

    objects["fillCustom"] = [{"properties": {"show": _literal(False)}}]
    objects["label"] = [
        {
            "properties": {
                "show": _literal(show_labels),
                "fontSize": _literal_num(label_font_size),
            },
            "selector": {"id": "default"},
        }
    ]
    return objects


def _card_container_objects(
    *,
    show_border: bool,
    border_radius: int,
    title: str | None,
    show_visual_header: bool,
) -> dict[str, Any]:
    border_props: dict[str, Any] = {"show": _literal(show_border)}
    if show_border:
        border_props["radius"] = _literal_num(border_radius)
    title_props: dict[str, Any] = (
        {"text": _literal_str(title)}
        if title is not None
        else {"show": _literal(False)}
    )
    return {
        "border": [{"properties": border_props}],
        "title": [{"properties": title_props}],
        "visualHeader": [{"properties": {"show": _literal(show_visual_header)}}],
    }


def _card_query(measures: list[Measure]) -> dict[str, Any]:
    query: dict[str, Any] = {
        "queryState": {"Data": {"projections": [_projection(m) for m in measures]}}
    }
    if len(measures) > 1:
        # Captured multi-measure cards carry their tile order as the
        # default sort on the first measure.
        query["sortDefinition"] = _default_sort_definition(measures[0], "Descending")
    return query


def _emit_card_body(c: Card) -> dict[str, Any]:
    measures = c.measures
    if not measures or any(not m.name for m in measures):
        raise ValueError("Card requires at least one named measure")
    if c.display_units != "None":
        log.warning(
            "Card display_units ignored — no attested modern cardVisual bytes; "
            "bake the scaling into the measure's format_string instead",
            card=c.name,
            display_units=c.display_units,
        )
    return {
        "visualType": "cardVisual",
        "query": _card_query(measures),
        "objects": _card_objects(
            measures,
            value_font_size=c.value_font_size,
            label_font_size=c.label_font_size,
            show_labels=c.show_labels,
            padding=c.padding,
            show_dividers=c.show_dividers,
            outline=False,
        ),
        "visualContainerObjects": _card_container_objects(
            show_border=c.show_border,
            border_radius=c.border_radius,
            title=c.title,
            show_visual_header=c.show_visual_header,
        ),
        "drillFilterOtherVisuals": True,
    }


def _emit_multicard_body(m: MultiCard) -> dict[str, Any]:
    if not m.measures or any(not ms.name for ms in m.measures):
        raise ValueError("MultiCard requires at least one named measure")
    if m.display_units != "None":
        log.warning(
            "MultiCard display_units ignored — no attested modern cardVisual "
            "bytes; bake the scaling into the measure's format_string instead",
            multicard=m.name,
            display_units=m.display_units,
        )
    if m.arrangement != "rows":
        log.warning(
            "MultiCard arrangement ignored — no attested modern cardVisual "
            "bytes for a column arrangement",
            multicard=m.name,
            arrangement=m.arrangement,
        )
    for knob in ("label_heading", "label_position", "label_font_color"):
        if getattr(m, knob) is not None:
            log.warning(
                "MultiCard label styling knob ignored — no attested modern "
                "cardVisual bytes",
                multicard=m.name,
                knob=knob,
            )
    return {
        "visualType": "cardVisual",
        "query": _card_query(m.measures),
        "objects": _card_objects(
            m.measures,
            value_font_size=14,
            label_font_size=8,
            show_labels=True,
            padding=8,
            show_dividers=True,
            outline=m.show_outline,
            accent_bar=m.show_accent_bar,
            shadow=m.show_shadow,
        ),
        "visualContainerObjects": _card_container_objects(
            show_border=True,
            border_radius=5,
            title=None,
            show_visual_header=True,
        ),
        "drillFilterOtherVisuals": True,
    }


# ── Column chart emitter ────────────────────────────────────────────────────


def _emit_column_chart_body(ch: ColumnChart) -> dict[str, Any]:
    if not ch.category.name:
        raise ValueError(f"{type(ch).__name__} requires a category column")
    if not ch.values:
        raise ValueError(f"{type(ch).__name__} requires at least one measure")

    query: dict[str, Any] = {
        "queryState": {
            "Category": {"projections": [_projection(ch.category, active=True)]},
            "Y": {"projections": [_projection(m) for m in ch.values]},
        }
    }
    if ch.sort_by is not None:
        query["sortDefinition"] = _default_sort_definition(
            ch.sort_by, _SORT_DIRECTION[ch.sort_direction]
        )

    objects: dict[str, Any] = {
        "labels": [{"properties": {"show": _literal(ch.data_labels)}}]
    }
    if ch.legend:
        # Captured pair — Fabric emits showGradientLegend alongside show.
        objects["legend"] = [
            {
                "properties": {
                    "showGradientLegend": _literal(True),
                    "show": _literal(True),
                }
            }
        ]
    if ch.value_axis_title is not None:
        value_axis_props: dict[str, Any] = (
            {"titleText": _literal_str(ch.value_axis_title)}
            if isinstance(ch.value_axis_title, str)
            else {"showAxisTitle": _literal(ch.value_axis_title)}
        )
        objects["valueAxis"] = [{"properties": value_axis_props}]
    if ch.category_axis_title is not None:
        objects["categoryAxis"] = [
            {"properties": {"showAxisTitle": _literal(ch.category_axis_title)}}
        ]

    border_props: dict[str, Any] = {"show": _literal(ch.show_border)}
    if ch.show_border:
        border_props["radius"] = _literal_num(ch.border_radius)
    container: dict[str, Any] = {"border": [{"properties": border_props}]}
    if ch.title is not None:
        title_props: dict[str, Any] = {"text": _literal_str(ch.title)}
        if not ch.show_title:
            title_props["show"] = _literal(False)
        container["title"] = [{"properties": title_props}]

    return {
        "visualType": (
            "clusteredColumnChart"
            if isinstance(ch, ClusteredColumnChart)
            else "columnChart"
        ),
        "query": query,
        "objects": objects,
        "visualContainerObjects": container,
        "drillFilterOtherVisuals": True,
    }


# ── filterConfig (one filter per projected field) ──────────────────────────


def _emit_filter_config(v: Visual) -> dict[str, Any]:
    """One ``filterConfig.filters[]`` entry per projected field.

    Columns use ``type:"Categorical"``; measures use ``type:"Advanced"``
    (matches the reference reports). Filter ``name`` ids are derived
    deterministically from the visual name + property so output is
    byte-stable across saves.
    """
    fields = _filterable_fields(v)
    filters = []
    for ref in fields:
        entry = _field_ref(ref)
        entry_with_name: dict[str, Any] = {"name": _id20(v.name, ref.entity, ref.name)}
        entry_with_name.update(entry)
        entry_with_name["type"] = (
            "Advanced" if isinstance(ref, Measure) else "Categorical"
        )
        filters.append(entry_with_name)
    return {"filters": filters}


def _filterable_fields(v: Visual) -> list[Column | Measure]:
    if isinstance(v, Slicer):
        return list(v.field_levels)
    if isinstance(v, Table):
        return [_projectable(f) for f in v.fields]
    if isinstance(v, Card):
        return list(v.measures)
    if isinstance(v, MultiCard):
        return list(v.measures)
    # Chart references carry no filterConfig — return nothing so the
    # key is omitted.
    return []
