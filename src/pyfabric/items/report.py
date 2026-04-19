"""Build Fabric Report items (PBIR-Legacy format) by hand.

A Page-and-Visual builder for KPI strips, slicers, and detail tables
linked to a SemanticModel via a relative byPath reference. Supports
single-metric and multi-metric cards (with label typography + theme
color styling), single-column and hierarchy slicers (with optional
allow-list filter), tables with mixed Column/Measure/Aggregate fields
and OrderBy on any of them, and an optional report-level theme bundled
into ``StaticResources/SharedResources/BaseThemes/``.

Out of scope: tooltip pages (the wiring schema needs to be re-derived
from working examples), bookmarks, drillthrough, page-level filters
beyond a slicer's allow-list.

Output format is **PBIR-Legacy** (single ``report.json``) — the format
Fabric currently emits when a report is created via ``+ New item →
Report`` in the workspace UI. The newer folder-per-page PBIR format is
not emitted; if a tenant requires it, open the report in Desktop and
re-save to convert.

Every write routes through
:func:`pyfabric.items.normalize.write_artifact_file` so emitted bytes
match Fabric's per-file-type byte convention and won't trigger sync flap.

Usage::

    from pathlib import Path
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

    page = Page(
        display_name="QA Summary",
        width=1280,
        height=720,
        visuals=[
            Slicer(
                position=Position(x=10, y=12, width=212, height=80),
                field=Column("dim_projection", "region"),
                mode="Dropdown",
            ),
            MultiCard(
                position=Position(x=10, y=110, width=1260, height=120),
                measures=[
                    Measure("fact_x", "# PDFs Total", format_string="#,0"),
                    Measure("fact_x", "# PDFs OK", format_string="#,0"),
                ],
                display_units="None",
            ),
            Table(
                position=Position(x=10, y=240, width=1260, height=460),
                fields=[
                    Column("dim_projection", "project_number"),
                    Column("fact_x", "status"),
                    Aggregate("fact_x", "missing_field_count", function="sum"),
                ],
                order_by=TableOrderBy(
                    field=Aggregate("fact_x", "missing_field_count", function="sum"),
                    direction="desc",
                ),
            ),
        ],
    )

    Report(
        name="rpt_my_report",
        semantic_model_path="../sm_my_model.SemanticModel",
        pages=[page],
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


# ``valueDisplayUnits`` literal magnitudes used by Power BI cards.
# 1D = "show actual" (None); 0D = Auto (the abbreviation default that
# turns 1623 → "2K"); the rest are divisor magnitudes.
_DISPLAY_UNITS_LITERAL: dict[DisplayUnits, str] = {
    "Auto": "0D",
    "None": "1D",
    "Thousands": "1000D",
    "Millions": "1000000D",
    "Billions": "1000000000D",
    "Trillions": "1000000000000D",
}

# Aggregation function index in the PBIR ``Aggregation.Function`` enum.
_AGG_FN_INDEX: dict[AggregationFunction, int] = {
    "sum": 0,
    "avg": 1,
    "min": 2,
    "max": 3,
    "count": 4,
    "distinctCount": 5,
}

# Sort direction index in the PBIR ``OrderBy.Direction`` enum.
_SORT_DIR_INDEX: dict[SortDirection, int] = {
    "asc": 1,
    "desc": 2,
}

# Stable UUID namespace for deterministic visual / page / pod ids.
_REPORT_NS = uuid.UUID("c1d2e3f4-0001-4000-8000-000000000000")


# ── Theme types ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ThemeColor:
    """A reference to a color in the report's theme palette.

    Use in place of hex literals so cards and other visuals adapt when
    the theme is swapped. ``color_id`` indexes the theme's
    ``dataColors`` list (0-based; theme conventions vary). ``percent``
    tints the color (0.0 = full saturation, 0.6 = soft tint suitable
    for backgrounds).
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
    name on that table. ``format_string`` overrides the column-property
    formatter inside the visual (does not change the column's default).
    """

    entity: str
    name: str
    format_string: str | None = None


@dataclass(frozen=True)
class Measure:
    """A measure reference in the SemanticModel, used inside a visual."""

    entity: str
    name: str
    format_string: str | None = None


@dataclass(frozen=True)
class Aggregate:
    """An inline aggregation of a column inside a visual.

    Lets a Table or OrderBy reference ``Sum(table.col)`` without
    requiring a model-level measure. Power BI emits this with an
    ``Aggregation`` wrapper in the prototype query.
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
    """Visual placement on a Page (canvas coordinates in pixels)."""

    x: float
    y: float
    width: float
    height: float
    z: float = 0.0
    tab_order: int = 0


# ── Visuals ─────────────────────────────────────────────────────────────────


@dataclass
class Visual:
    """Base class for every visual on a page (do not instantiate directly)."""

    position: Position
    name: str = ""  # auto-filled from page+index if blank


@dataclass
class Slicer(Visual):
    """A slicer visual.

    ``field`` accepts either a single :class:`Column` (the common case)
    or a list of columns to render a **hierarchy slicer** with one
    drill level per entry. Hierarchy slicers must use ``mode="Basic"``;
    other modes are silently coerced. All hierarchy levels start
    collapsed (no preselected expansion); customize in Desktop if you
    need a specific drill path on first load.

    ``allow_values`` emits a hardcoded slicer-level filter that limits
    the dropdown to those values — useful for scoping the report
    (e.g. a status slicer showing only ``["INCOMPLETE", "NOT_DETECTED"]``).
    The filter targets the **leaf** column when ``field`` is a hierarchy.
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
    """A single-metric KPI card.

    ``display_units="None"`` (default) shows the actual integer; ``"Auto"``
    triggers Power BI's "2K" abbreviation. Set ``title`` to override the
    measure-name title; set to empty string to suppress.
    """

    measure: Measure = field(default_factory=lambda: Measure("", ""))
    display_units: DisplayUnits = "None"
    title: str | None = None


@dataclass
class MultiCard(Visual):
    """A multi-metric KPI strip (one cardVisual rendering several measures).

    All measures share ``display_units``. ``arrangement`` controls
    whether tiles flow in rows or columns. The polish flags
    (``show_outline``, ``show_accent_bar``, ``show_shadow``) wire the
    matching ``objects`` properties.

    The label-styling props target the small text under each value:

    - ``label_heading`` — Power BI typography role (``"Heading2"`` etc.).
      Theme-driven font size + weight; preferable to setting a literal
      font size because the role adapts on theme swap.
    - ``label_position`` — ``"belowValue"`` (default) or ``"aboveValue"``.
    - ``label_font_color`` — :class:`ThemeColor` reference for label text.
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
    """Sort spec for a Table visual."""

    field: FieldRef
    direction: SortDirection = "asc"


@dataclass
class Table(Visual):
    """A table visual with one or more field/measure/aggregate columns.

    ``order_by`` is optional; when omitted, Power BI uses its default
    sort. Provide an :class:`Aggregate` reference there to sort by
    something like ``Sum(missing_field_count)`` without defining a
    model measure.
    """

    fields: list[FieldRef] = field(default_factory=list)
    order_by: TableOrderBy | None = None


# ── Page ────────────────────────────────────────────────────────────────────


@dataclass
class Page:
    """A single report page (called a "section" in PBIR-Legacy JSON)."""

    display_name: str
    visuals: list[Visual] = field(default_factory=list)
    width: float = 1280.0
    height: float = 720.0
    name: str = ""  # auto-filled from display_name if blank


# ── Report ──────────────────────────────────────────────────────────────────


class ReportError(Exception):
    """Raised when the report fails pre-emit validation."""


@dataclass
class Report:
    """A full Fabric Report item.

    ``semantic_model_path`` is a relative path from the report folder
    to a sibling ``*.SemanticModel`` folder (e.g. ``"../sm_x.SemanticModel"``).
    Emitted as a PBIR-Legacy ``definition.pbir`` ``byPath`` reference.

    ``theme``, when set, bundles a base theme JSON file at
    ``StaticResources/SharedResources/BaseThemes/<theme.name>.json``
    and registers it as the report's base theme. Without a theme,
    Power BI applies its workspace default.

    **A non-empty description is required by default.** Reports show up
    in the workspace listing and item-info pane; an empty description
    is a poor consumer experience. Set ``strict_descriptions=False``
    to opt out (a warning is logged).
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
        automatically from :meth:`save_to_disk`; expose it separately
        so callers can lint without writing.
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
        """Emit the full ``<name>.Report`` folder.

        Returns the path to the created folder. Raises :class:`ReportError`
        if pre-emit validation fails. All writes route through
        :func:`pyfabric.items.normalize.write_artifact_file`.
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
        write_artifact_file(item_dir / "report.json", self._emit_report_json())
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
        return json.dumps(
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                "metadata": {
                    "type": "Report",
                    "displayName": self.name,
                    **({"description": self.description} if self.description else {}),
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

    def _emit_report_json(self) -> str:
        report_config: dict[str, Any] = {
            "version": "5.72",
            "activeSectionIndex": 0,
            "defaultDrillFilterOtherVisuals": True,
            "linguisticSchemaSyncVersion": 0,
            "settings": {
                "useNewFilterPaneExperience": True,
                "allowChangeFilterTypes": True,
                "useStylableVisualContainerHeader": True,
                "queryLimitOption": 6,
                "useEnhancedTooltips": True,
                "exportDataMode": 1,
                "useDefaultAggregateDisplayName": True,
            },
            "objects": {
                "section": [
                    {
                        "properties": {
                            "verticalAlignment": {
                                "expr": {"Literal": {"Value": "'Top'"}}
                            }
                        }
                    }
                ]
            },
        }
        if self.theme is not None:
            report_config["themeCollection"] = {
                "baseTheme": {
                    "name": self.theme.name,
                    "type": 2,
                    "version": {
                        "visual": "2.8.0",
                        "report": "3.2.0",
                        "page": "2.3.1",
                    },
                }
            }
        payload: dict[str, Any] = {
            "config": json.dumps(report_config),
            "layoutOptimization": 0,
            "pods": [_emit_pod(p, i) for i, p in enumerate(self.pages)],
            "sections": [_emit_section(p) for p in self.pages],
        }
        if self.theme is not None:
            payload["resourcePackages"] = [
                {
                    "resourcePackage": {
                        "disabled": False,
                        "items": [
                            {
                                "name": self.theme.name,
                                "path": f"BaseThemes/{self.theme.name}.json",
                                "type": 202,
                            }
                        ],
                        "name": "SharedResources",
                        "type": 2,
                    }
                }
            ]
        return json.dumps(payload, indent=2)


# ── Internal: id helpers ───────────────────────────────────────────────────


def _id20(*parts: str) -> str:
    """Deterministic 20-char hex id matching Fabric's visual/page id shape."""
    return uuid.uuid5(_REPORT_NS, ".".join(parts)).hex[:20]


def _theme_color_solid(c: ThemeColor) -> dict[str, Any]:
    """The ``{solid: {color: {expr: {ThemeDataColor: ...}}}}`` wrapper Power BI uses for theme color refs."""
    return {
        "solid": {
            "color": {
                "expr": {
                    "ThemeDataColor": {
                        "ColorId": c.color_id,
                        "Percent": c.percent,
                    }
                }
            }
        }
    }


# ── Internal: section / pod emitters ───────────────────────────────────────


def _emit_pod(page: Page, index: int) -> dict[str, Any]:
    """One ``pods[]`` entry binds a page to its rendering surface."""
    return {
        "boundSection": page.name,
        "config": "{}",
        "name": _id20("pod", page.name),
        "referenceScope": 1,
    }


def _emit_section(page: Page) -> dict[str, Any]:
    """A PBIR-Legacy section (the JSON for one page)."""
    return {
        "config": "{}",
        "displayName": page.display_name,
        "displayOption": 1,
        "filters": "[]",
        "height": page.height,
        "name": page.name,
        "visualContainers": [_emit_visual_container(v) for v in page.visuals],
        "width": page.width,
    }


def _emit_visual_container(v: Visual) -> dict[str, Any]:
    """The outer wrapper for a visual; the inner ``config`` is stringified JSON."""
    return {
        "config": json.dumps(_emit_visual_config(v)),
        "filters": "[]",
        "height": v.position.height,
        "width": v.position.width,
        "x": v.position.x,
        "y": v.position.y,
        "z": v.position.z,
    }


def _emit_visual_config(v: Visual) -> dict[str, Any]:
    """Dispatch to the per-visual emitter."""
    if isinstance(v, Slicer):
        return _emit_slicer_config(v)
    if isinstance(v, Card):
        return _emit_card_config(v)
    if isinstance(v, MultiCard):
        return _emit_multicard_config(v)
    if isinstance(v, Table):
        return _emit_table_config(v)
    raise TypeError(f"unsupported visual type: {type(v).__name__}")


def _layout_block(v: Visual) -> list[dict[str, Any]]:
    """Standard ``layouts`` block with the visual's position."""
    return [
        {
            "id": 0,
            "position": {
                "x": v.position.x,
                "y": v.position.y,
                "z": v.position.z,
                "width": v.position.width,
                "height": v.position.height,
                "tabOrder": v.position.tab_order or int(v.position.z),
            },
        }
    ]


# ── Slicer emitter ──────────────────────────────────────────────────────────


def _emit_slicer_config(s: Slicer) -> dict[str, Any]:
    levels = s.field_levels
    leaf = s.leaf_field
    # All slicer levels must come from the same entity (Power BI doesn't
    # render cross-entity hierarchies in a single slicer). Validate up
    # front rather than emit silently broken JSON.
    entities = {c.entity for c in levels}
    if len(entities) > 1:
        raise ValueError(
            f"Slicer hierarchy levels must share an entity; got {sorted(entities)}"
        )
    src = leaf.entity[0] or "d"
    # Hierarchy slicers only render correctly in Basic mode.
    mode: SlicerMode = "Basic" if s.is_hierarchy else s.mode

    objects: dict[str, Any] = {
        "data": [
            {"properties": {"mode": {"expr": {"Literal": {"Value": f"'{mode}'"}}}}}
        ]
    }
    if s.allow_values:
        objects["general"] = [
            {
                "properties": {
                    "filter": {
                        "filter": {
                            "Version": 2,
                            "From": [{"Name": src, "Entity": leaf.entity, "Type": 0}],
                            "Where": [
                                {
                                    "Condition": {
                                        "In": {
                                            "Expressions": [
                                                {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {"Source": src}
                                                        },
                                                        "Property": leaf.name,
                                                    }
                                                }
                                            ],
                                            "Values": [
                                                [{"Literal": {"Value": f"'{v}'"}}]
                                                for v in s.allow_values
                                            ],
                                        }
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        ]

    inner: dict[str, Any] = {
        "name": s.name,
        "layouts": _layout_block(s),
        "singleVisual": {
            "visualType": "slicer",
            "projections": {
                "Values": [
                    {"queryRef": f"{c.entity}.{c.name}", "active": True} for c in levels
                ]
            },
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": src, "Entity": leaf.entity, "Type": 0}],
                "Select": [_select_column(c, src) for c in levels],
            },
            "drillFilterOtherVisuals": True,
            "objects": objects,
        },
    }
    if s.is_hierarchy:
        # Default to all levels collapsed; users wanting a preselected
        # expansion path should customize in Desktop after first save.
        inner["singleVisual"]["expansionStates"] = [
            {
                "roles": ["Values"],
                "levels": [
                    {
                        "queryRefs": [f"{c.entity}.{c.name}"],
                        "isCollapsed": True,
                    }
                    for c in levels
                ],
                "root": {"identityValues": None, "children": []},
            }
        ]
    return inner


# ── Card (single-metric) emitter ───────────────────────────────────────────


def _emit_card_config(c: Card) -> dict[str, Any]:
    src = c.measure.entity[0] or "f"
    full_name = f"{c.measure.entity}.{c.measure.name}"
    objects: dict[str, Any] = {
        "value": [
            {
                "properties": {
                    "displayUnits": {
                        "expr": {
                            "Literal": {
                                "Value": _DISPLAY_UNITS_LITERAL[c.display_units]
                            }
                        }
                    }
                },
                "selector": {"id": "default"},
            }
        ]
    }
    column_properties: dict[str, Any] = {}
    if c.measure.format_string:
        column_properties[full_name] = {"formatString": c.measure.format_string}
    inner: dict[str, Any] = {
        "name": c.name,
        "layouts": _layout_block(c),
        "singleVisual": {
            "visualType": "cardVisual",
            "projections": {"Data": [{"queryRef": full_name}]},
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": src, "Entity": c.measure.entity, "Type": 0}],
                "Select": [_select_measure(c.measure, src)],
            },
            "columnProperties": column_properties,
            "drillFilterOtherVisuals": True,
            "objects": objects,
        },
    }
    if c.title is not None:
        inner["singleVisual"]["vcObjects"] = {
            "title": [
                {
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {"expr": {"Literal": {"Value": f"'{c.title}'"}}},
                    }
                }
            ]
            if c.title
            else [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}]
        }
    return inner


# ── MultiCard emitter ──────────────────────────────────────────────────────


def _emit_multicard_config(mc: MultiCard) -> dict[str, Any]:
    if not mc.measures:
        raise ValueError("MultiCard requires at least one measure")
    src = mc.measures[0].entity[0] or "f"
    entity = mc.measures[0].entity
    if any(m.entity != entity for m in mc.measures):
        raise ValueError("MultiCard measures must all live on the same entity (table)")

    projections = [{"queryRef": f"{m.entity}.{m.name}"} for m in mc.measures]
    select = [_select_measure(m, src) for m in mc.measures]
    column_properties = {
        f"{m.entity}.{m.name}": {"formatString": m.format_string}
        for m in mc.measures
        if m.format_string
    }

    # `referenceLabel` properties: one entry per measure with its
    # measure-expression value. This is the per-tile binding the
    # multi-card layout uses (in addition to `projections`).
    reference_label = [
        {
            "properties": {
                "value": {
                    "expr": {
                        "Measure": {
                            "Expression": {"SourceRef": {"Entity": m.entity}},
                            "Property": m.name,
                        }
                    }
                }
            },
            "selector": {
                "data": [{"dataViewWildcard": {"matchingOption": 0}}],
                "metadata": f"{m.entity}.{m.name}",
                "id": _id20("ref", mc.name, m.name),
                "order": 0,
            },
        }
        for m in mc.measures
    ]
    reference_label_value = [
        {
            "properties": {
                "valueDisplayUnits": {
                    "expr": {
                        "Literal": {"Value": _DISPLAY_UNITS_LITERAL[mc.display_units]}
                    }
                }
            },
            "selector": {
                "metadata": f"{m.entity}.{m.name}",
                "id": _id20("ref", mc.name, m.name),
            },
        }
        for m in mc.measures
    ]
    # Hide the auto-generated reference-label titles per measure so the
    # tile shows the value cleanly.
    reference_label_title = [
        {
            "properties": {"show": {"expr": {"Literal": {"Value": "false"}}}},
            "selector": {"metadata": f"{m.entity}.{m.name}"},
        }
        for m in mc.measures
    ]

    objects: dict[str, Any] = {
        "layout": [
            {
                "properties": {
                    "style": {"expr": {"Literal": {"Value": "'Cards'"}}},
                    "orientation": {"expr": {"Literal": {"Value": "2D"}}},
                }
            }
        ],
        "referenceLabel": reference_label,
        "referenceLabelTitle": reference_label_title,
        "referenceLabelValue": reference_label_value,
        "referenceLabelLayout": [
            {
                "properties": {
                    "horizontalAlignment": {"expr": {"Literal": {"Value": "'center'"}}},
                    "verticalAlignment": {"expr": {"Literal": {"Value": "'middle'"}}},
                    "arrangement": {
                        "expr": {"Literal": {"Value": f"'{mc.arrangement}'"}}
                    },
                },
                "selector": {"id": "default"},
            }
        ],
    }
    if mc.show_outline:
        objects["outline"] = [
            {
                "properties": {"show": {"expr": {"Literal": {"Value": "true"}}}},
                "selector": {"id": "default"},
            }
        ]
    if mc.show_accent_bar:
        objects["accentBar"] = [
            {
                "properties": {"show": {"expr": {"Literal": {"Value": "true"}}}},
                "selector": {"id": "default"},
            }
        ]
    if mc.show_shadow:
        objects["shadowCustom"] = [
            {
                "properties": {"show": {"expr": {"Literal": {"Value": "true"}}}},
                "selector": {"id": "default"},
            }
        ]

    # Label styling for the per-tile measure-name text. Only emit when
    # the user set at least one knob; otherwise leave Power BI defaults.
    if any(
        v is not None
        for v in (mc.label_heading, mc.label_position, mc.label_font_color)
    ):
        label_props: dict[str, Any] = {"show": {"expr": {"Literal": {"Value": "true"}}}}
        if mc.label_heading is not None:
            label_props["heading"] = {
                "expr": {"Literal": {"Value": f"'{mc.label_heading}'"}}
            }
        if mc.label_position is not None:
            label_props["position"] = {
                "expr": {"Literal": {"Value": f"'{mc.label_position}'"}}
            }
        if mc.label_font_color is not None:
            label_props["fontColor"] = _theme_color_solid(mc.label_font_color)
        objects["label"] = [{"properties": label_props, "selector": {"id": "default"}}]

    return {
        "name": mc.name,
        "layouts": _layout_block(mc),
        "singleVisual": {
            "visualType": "cardVisual",
            "projections": {"Data": projections},
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": src, "Entity": entity, "Type": 0}],
                "Select": select,
            },
            "columnProperties": column_properties,
            "drillFilterOtherVisuals": True,
            "objects": objects,
            "vcObjects": {
                "title": [
                    {"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}
                ]
            },
        },
    }


# ── Table emitter ──────────────────────────────────────────────────────────


def _emit_table_config(t: Table) -> dict[str, Any]:
    if not t.fields:
        raise ValueError("Table requires at least one field")

    # Build From clause: one alias per distinct entity referenced.
    entities: dict[str, str] = {}  # entity -> alias

    def _alias(entity: str) -> str:
        if entity not in entities:
            entities[entity] = f"t{len(entities)}"
        return entities[entity]

    select_clauses = []
    projections = []
    column_properties: dict[str, Any] = {}
    for f in t.fields:
        if isinstance(f, Column):
            alias = _alias(f.entity)
            select_clauses.append(_select_column(f, alias))
            projections.append({"queryRef": f"{f.entity}.{f.name}"})
            if f.format_string:
                column_properties[f"{f.entity}.{f.name}"] = {
                    "formatString": f.format_string
                }
        elif isinstance(f, Measure):
            alias = _alias(f.entity)
            select_clauses.append(_select_measure(f, alias))
            projections.append({"queryRef": f"{f.entity}.{f.name}"})
            if f.format_string:
                column_properties[f"{f.entity}.{f.name}"] = {
                    "formatString": f.format_string
                }
        else:  # Aggregate
            alias = _alias(f.entity)
            select_clauses.append(_select_aggregate(f, alias))
            ref_name = _aggregate_ref_name(f)
            projections.append({"queryRef": ref_name})
            if f.format_string:
                column_properties[ref_name] = {"formatString": f.format_string}

    from_clauses = [
        {"Name": alias, "Entity": entity, "Type": 0}
        for entity, alias in entities.items()
    ]

    prototype_query: dict[str, Any] = {
        "Version": 2,
        "From": from_clauses,
        "Select": select_clauses,
    }
    if t.order_by is not None:
        prototype_query["OrderBy"] = [_order_by_clause(t.order_by, _alias)]

    return {
        "name": t.name,
        "layouts": _layout_block(t),
        "singleVisual": {
            "visualType": "tableEx",
            "projections": {"Values": projections},
            "prototypeQuery": prototype_query,
            "columnProperties": column_properties,
            "drillFilterOtherVisuals": True,
        },
    }


# ── Internal: prototype-query Select / OrderBy clause builders ─────────────


def _select_column(c: Column, alias: str) -> dict[str, Any]:
    return {
        "Column": {
            "Expression": {"SourceRef": {"Source": alias}},
            "Property": c.name,
        },
        "Name": f"{c.entity}.{c.name}",
    }


def _select_measure(m: Measure, alias: str) -> dict[str, Any]:
    return {
        "Measure": {
            "Expression": {"SourceRef": {"Source": alias}},
            "Property": m.name,
        },
        "Name": f"{m.entity}.{m.name}",
        "NativeReferenceName": m.name,
    }


def _select_aggregate(a: Aggregate, alias: str) -> dict[str, Any]:
    return {
        "Aggregation": {
            "Expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Source": alias}},
                    "Property": a.column,
                }
            },
            "Function": _AGG_FN_INDEX[a.function],
        },
        "Name": _aggregate_ref_name(a),
        "NativeReferenceName": f"{_AGG_FN_LABEL[a.function]} of {a.column}",
    }


def _order_by_clause(ob: TableOrderBy, alias_lookup: Any) -> dict[str, Any]:
    direction = _SORT_DIR_INDEX[ob.direction]
    f = ob.field
    if isinstance(f, Column):
        alias = alias_lookup(f.entity)
        return {
            "Direction": direction,
            "Expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Source": alias}},
                    "Property": f.name,
                }
            },
        }
    if isinstance(f, Measure):
        alias = alias_lookup(f.entity)
        return {
            "Direction": direction,
            "Expression": {
                "Measure": {
                    "Expression": {"SourceRef": {"Source": alias}},
                    "Property": f.name,
                }
            },
        }
    # Aggregate
    alias = alias_lookup(f.entity)
    return {
        "Direction": direction,
        "Expression": {
            "Aggregation": {
                "Expression": {
                    "Column": {
                        "Expression": {"SourceRef": {"Source": alias}},
                        "Property": f.column,
                    }
                },
                "Function": _AGG_FN_INDEX[f.function],
            }
        },
    }


# Capitalized labels used in NativeReferenceName for aggregates
# (e.g. "Sum of missing_field_count") — matches what Power BI emits.
_AGG_FN_LABEL: dict[AggregationFunction, str] = {
    "sum": "Sum",
    "avg": "Average",
    "min": "Min",
    "max": "Max",
    "count": "Count",
    "distinctCount": "Count (Distinct)",
}


def _aggregate_ref_name(a: Aggregate) -> str:
    """The queryRef name for an aggregate column, e.g. ``Sum(table.col)``."""
    cap = _AGG_FN_LABEL[a.function].split(" ")[0]
    return f"{cap}({a.entity}.{a.column})"
