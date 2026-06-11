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

``Card`` and ``MultiCard`` remain in the public API for source
compatibility but their PBIR emit is **not yet implemented** — it needs
UI-captured reference bytes. Instantiating them on a saved page raises
``NotImplementedError``.

PBIR shapes that still need a UI capture (no attested reference bytes —
would be guessed, so they are dropped-with-warning or raise rather than
emit invented JSON): tooltip pages, bookmarks, drillthrough,
hierarchy/expansion slicers, slicer ``mode`` variants (Dropdown/Between),
``allow_values`` slicer filters, inline column aggregations,
``format_string`` column properties, sort-by-measure, and the dynamic
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
    name on that table. ``format_string`` is accepted for source
    compatibility but is **not yet emitted** in PBIR (column-property
    formatting needs UI-captured reference bytes).
    """

    entity: str
    name: str
    format_string: str | None = None


@dataclass(frozen=True)
class Measure:
    """A measure reference in the SemanticModel, used inside a visual.

    ``format_string`` is accepted for source compatibility but is **not
    yet emitted** in PBIR.
    """

    entity: str
    name: str
    format_string: str | None = None


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
    """A single-metric KPI card.

    Retained in the public API for source compatibility. **PBIR emit is
    not yet implemented** — needs UI-captured reference bytes. Saving a
    page that contains a ``Card`` raises :class:`NotImplementedError`.
    """

    measure: Measure = field(default_factory=lambda: Measure("", ""))
    display_units: DisplayUnits = "None"
    title: str | None = None


@dataclass
class MultiCard(Visual):
    """A multi-metric KPI strip.

    Retained in the public API for source compatibility. **PBIR emit is
    not yet implemented** — needs UI-captured reference bytes. Saving a
    page that contains a ``MultiCard`` raises :class:`NotImplementedError`.
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


# ── Page ────────────────────────────────────────────────────────────────────


PageDisplayOption = Literal["FitToPage", "ActualSize", "FitToWidth"]


@dataclass
class Page:
    """A single report page.

    ``name`` is the page's id (also its folder name under
    ``definition/pages/``). Leave blank to derive a deterministic id from
    the report + display name; pin it for byte-stable output.
    """

    display_name: str
    visuals: list[Visual] = field(default_factory=list)
    width: float = 1280.0
    height: float = 720.0
    name: str = ""  # auto-filled from display_name if blank
    display_option: PageDisplayOption = "FitToPage"


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


# ── Internal: page / visual emitters ───────────────────────────────────────


def _emit_page_json(page: Page) -> str:
    return json.dumps(
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json",
            "name": page.name,
            "displayName": page.display_name,
            "displayOption": page.display_option,
            "height": _num(page.height),
            "width": _num(page.width),
        },
        indent=2,
    )


def _emit_visual_json(v: Visual) -> str:
    return json.dumps(
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.10.0/schema.json",
            "name": v.name,
            "position": _emit_position(v.position),
            "visual": _emit_visual_body(v),
            "filterConfig": _emit_filter_config(v),
        },
        indent=2,
    )


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
    if isinstance(v, (Card, MultiCard)):
        raise NotImplementedError(
            f"{type(v).__name__} PBIR emit not yet implemented — needs reference bytes"
        )
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


def _projection(ref: Column | Measure) -> dict[str, Any]:
    """A ``query.queryState.Values.projections[]`` entry."""
    proj = _field_ref(ref)
    proj["queryRef"] = f"{ref.entity}.{ref.name}"
    proj["nativeQueryRef"] = ref.name
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
    projections = []
    for i, col in enumerate(levels):
        proj = _projection(col)
        proj["active"] = i == 0
        projections.append(proj)
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
    return []
