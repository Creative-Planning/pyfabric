"""Build Fabric SemanticModel items by hand without writing TMDL by hand.

MVP scope: import-mode tables sourced from a Fabric Lakehouse via a shared
``Lakehouse.Contents`` M expression. Out of scope (for now): DirectLake
partitions, calculated tables, perspectives, role-level security,
hierarchies, TMDL parsing.

Every write routes through :func:`pyfabric.items.normalize.write_artifact_file`,
so emitted bytes match Fabric's per-file-type byte convention and won't
trigger sync flap.

Validation runs at ``save_to_disk`` time and raises ``SemanticModelError``
on the failures most commonly observed when hand-authoring TMDL:

- Measure name collides (case-insensitive) with a column on the same table
- Relationship references a column or table that doesn't exist
- Duplicate table or column names
- Missing key column on a table referenced by a one-to-many relationship's
  ``to_*`` side

Usage::

    from pathlib import Path
    from pyfabric.items.semantic_model import (
        Column,
        LakehouseSource,
        Measure,
        Relationship,
        SemanticModel,
        Table,
    )

    gold = LakehouseSource(
        name="Gold",
        workspace_id="9b09...",
        lakehouse_id="fd2a...",
    )

    dim_section = Table(
        name="dim_section",
        source=gold,
        columns=[
            Column("section_key", "string", is_key=True),
            Column("section_display_name", "string"),
            Column("section_order", "int64", format_string="0"),
        ],
    )

    fact_status = Table(
        name="fact_qa_pdf_section_status",
        source=gold,
        columns=[
            Column("section_key", "string"),
            Column("status", "string"),
            Column("projection_report_id", "string"),
        ],
        measures=[
            Measure(
                name="# PDFs Total",
                expression="DISTINCTCOUNT('fact_qa_pdf_section_status'[projection_report_id])",
                format_string="#,0",
                description="Distinct PDFs in the current filter context.",
            ),
        ],
    )

    model = SemanticModel(
        name="sm_qa_coverage",
        description="QA coverage metrics.",
        sources=[gold],
        tables=[dim_section, fact_status],
        relationships=[
            Relationship(
                from_table="fact_qa_pdf_section_status",
                from_column="section_key",
                to_table="dim_section",
                to_column="section_key",
            ),
        ],
    )
    model.save_to_disk(Path("ws/"))
"""

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import structlog

from pyfabric.items.normalize import write_artifact_file
from pyfabric.items.validate_tmdl import check_name_collisions

log = structlog.get_logger()


# ── Public types ────────────────────────────────────────────────────────────


# TMDL data type names. Subset of what TMDL accepts; covers everything the
# MVP needs. Decimal/binary/variant are intentionally omitted — add when a
# real use case appears.
TmdlDataType = Literal["string", "int64", "double", "boolean", "dateTime"]

# How a column rolls up by default in Power BI visuals.
SummarizeBy = Literal["none", "sum", "average", "min", "max", "count"]

# Filter-direction for a relationship.
CrossFilter = Literal["single", "both"]

# Cardinality of a relationship.
Cardinality = Literal["manyToOne", "oneToMany", "oneToOne"]


# Stable UUID namespace for deterministic lineage tags. Hashing object names
# under this namespace yields the same UUIDs across regenerations, making
# diffs minimal when the model is re-emitted.
_LINEAGE_NS = uuid.UUID("b1c2d3e4-0001-4000-8000-000000000000")


class SemanticModelError(Exception):
    """Raised when the model fails pre-emit validation."""


# ── Sources ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LakehouseSource:
    """A Fabric Lakehouse referenced via a shared ``Lakehouse.Contents`` M expression.

    The emitted ``expressions.tmdl`` declares three parameter expressions
    (``WorkspaceId``, ``<name>LakehouseId``, plus the shared ``<name>``
    expression) and each table partition reads
    ``Source = <name>, tbl = Source{[Id="<table>", Schema=<schema>]}[Data]``.

    ``name`` is used as the M expression identifier — pick something
    short and identifier-safe (e.g. ``"Gold"``, ``"Silver"``).
    """

    name: str
    workspace_id: str
    lakehouse_id: str
    description: str | None = None

    def __post_init__(self) -> None:
        if not self.name.isidentifier():
            raise ValueError(
                f"LakehouseSource.name must be a valid M identifier, got {self.name!r}"
            )


# ── Columns ─────────────────────────────────────────────────────────────────


@dataclass
class Column:
    """A single column in a Table.

    ``data_type`` follows TMDL naming: ``string``, ``int64``, ``double``,
    ``boolean``, ``dateTime``. ``data_category`` (e.g. ``"WebUrl"``,
    ``"Time"``) flips Power BI's specialized rendering for the column.
    """

    name: str
    data_type: TmdlDataType
    summarize_by: SummarizeBy = "none"
    format_string: str | None = None
    description: str | None = None
    is_key: bool = False
    is_hidden: bool = False
    data_category: str | None = None
    source_column: str | None = None  # default = name
    annotations: dict[str, str] = field(default_factory=dict)


# ── Measures ───────────────────────────────────────────────────────────────


@dataclass
class Measure:
    """A DAX measure on a Table.

    ``expression`` is the raw DAX body (multi-line OK). The TMDL emitter
    indents the expression under ``measure '<name>' =`` correctly.

    ``name`` collisions with column names on the same table are checked at
    ``SemanticModel.save_to_disk`` time and raise ``SemanticModelError``
    pre-emit. Convention: Title Case with ``%`` / ``#`` markers
    (``Coverage Status``, ``Detection %``, ``# PDFs OK``) so they
    cannot collide with snake_case columns.
    """

    name: str
    expression: str
    format_string: str | None = None
    description: str | None = None
    is_hidden: bool = False
    annotations: dict[str, str] = field(default_factory=dict)


# ── Relationships ──────────────────────────────────────────────────────────


@dataclass
class Relationship:
    """A many-to-one relationship from a fact column to a dim column.

    Emits one ``relationship`` block in ``relationships.tmdl``. The
    fact-side (``from_*``) typically has many rows per dim row.
    Cross-filter direction defaults to ``"single"`` — use ``"both"``
    sparingly (bidirectional filtering can produce ambiguous joins).
    """

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cross_filter: CrossFilter = "single"
    cardinality: Cardinality = "manyToOne"
    is_active: bool = True


# ── Tables ──────────────────────────────────────────────────────────────────


@dataclass
class Table:
    """A table with columns, optional measures, and a Lakehouse partition source.

    ``schema`` is the lakehouse SQL schema (defaults to ``"dbo"`` for
    schema-enabled lakehouses). ``data_category`` on the table itself
    can be ``"Time"`` to flag a date dimension for Power BI's time
    intelligence.
    """

    name: str
    source: LakehouseSource
    columns: list[Column]
    measures: list[Measure] = field(default_factory=list)
    schema: str = "dbo"
    description: str | None = None
    is_hidden: bool = False
    data_category: str | None = None
    annotations: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_parquet(
        cls,
        name: str,
        source: LakehouseSource,
        parquet_path: Path | str,
        *,
        schema: str = "dbo",
        description: str | None = None,
    ) -> "Table":
        """Build a Table by reading column types from a Parquet file's schema.

        Requires ``pyarrow`` (the ``data`` extra). Maps Arrow types to
        TMDL data types via :func:`arrow_to_tmdl`. All columns default
        to ``summarize_by="none"`` and no format string — adjust the
        returned ``columns`` list to taste before saving.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError as e:  # pragma: no cover - exercised by users w/o extra
            raise ImportError(
                "Table.from_parquet requires pyarrow; install with `pip install pyfabric[data]`"
            ) from e

        schema_obj = pq.read_schema(str(parquet_path))
        columns = [
            Column(name=field.name, data_type=arrow_to_tmdl(str(field.type)))
            for field in schema_obj
        ]
        return cls(
            name=name,
            source=source,
            columns=columns,
            schema=schema,
            description=description,
        )


def arrow_to_tmdl(arrow_type: str) -> TmdlDataType:
    """Map a PyArrow type string to the TMDL data type pyfabric supports.

    Falls back to ``"string"`` for anything unrecognized — emit a TMDL
    string column rather than fail, so callers can edit the result.
    """
    s = arrow_type.lower()
    if s == "bool":
        return "boolean"
    if s.startswith(("int", "uint")):
        return "int64"
    if s.startswith("float") or s == "double" or s.startswith("decimal"):
        return "double"
    if s.startswith("date") or s.startswith("timestamp"):
        return "dateTime"
    if s in {"string", "large_string", "utf8"}:
        return "string"
    return "string"


# ── SemanticModel ──────────────────────────────────────────────────────────


@dataclass
class SemanticModel:
    """A complete tabular semantic model.

    Call :meth:`save_to_disk` to validate and emit the full
    ``*.SemanticModel`` folder structure (``.platform``,
    ``definition.pbism``, ``definition/database.tmdl``,
    ``definition/model.tmdl``, ``definition/expressions.tmdl``,
    ``definition/relationships.tmdl``, ``definition/cultures/<culture>.tmdl``,
    and one ``definition/tables/<name>.tmdl`` per table).
    """

    name: str
    sources: list[LakehouseSource]
    tables: list[Table]
    relationships: list[Relationship] = field(default_factory=list)
    description: str = ""
    compatibility_level: int = 1567
    culture: str = "en-US"
    annotations: dict[str, str] = field(default_factory=dict)
    logical_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # ── Validation ──────────────────────────────────────────────────────────

    def validate(self) -> list[str]:
        """Return a list of human-readable error messages.

        Empty list means the model passes pre-emit validation. Called
        automatically from :meth:`save_to_disk`; expose it separately so
        callers can lint without writing.
        """
        errors: list[str] = []
        table_index = {t.name: t for t in self.tables}

        # Duplicate table names
        seen_tables: set[str] = set()
        for t in self.tables:
            if t.name in seen_tables:
                errors.append(f"duplicate table: {t.name!r}")
            seen_tables.add(t.name)

        for t in self.tables:
            # Duplicate columns within a table
            seen_cols: set[str] = set()
            for c in t.columns:
                if c.name in seen_cols:
                    errors.append(f"{t.name}: duplicate column {c.name!r}")
                seen_cols.add(c.name)
            # Measure-vs-column collisions (case-insensitive)
            col_lower = {c.name.lower() for c in t.columns}
            for m in t.measures:
                if m.name.lower() in col_lower:
                    errors.append(
                        f"{t.name}: measure {m.name!r} collides "
                        f"(case-insensitive) with a column of the same name"
                    )

        # Relationships reference real columns
        for r in self.relationships:
            for tbl_name, col_name, side in (
                (r.from_table, r.from_column, "from"),
                (r.to_table, r.to_column, "to"),
            ):
                tbl = table_index.get(tbl_name)
                if tbl is None:
                    errors.append(
                        f"relationship references unknown table on {side} side: {tbl_name!r}"
                    )
                    continue
                if col_name not in {c.name for c in tbl.columns}:
                    errors.append(
                        f"relationship references unknown column "
                        f"{tbl_name!r}.{col_name!r} on {side} side"
                    )

        # Sources referenced by tables must be declared
        declared_sources = {s.name for s in self.sources}
        for t in self.tables:
            if t.source.name not in declared_sources:
                errors.append(
                    f"{t.name}: source {t.source.name!r} not in model.sources"
                )

        return errors

    # ── Emission ────────────────────────────────────────────────────────────

    def save_to_disk(self, output_dir: Path | str) -> Path:
        """Validate and emit the full SemanticModel folder.

        Raises :class:`SemanticModelError` if pre-emit validation fails.
        Returns the path to the created ``<name>.SemanticModel`` folder.
        """
        errors = self.validate()
        if errors:
            joined = "\n  - ".join(errors)
            raise SemanticModelError(
                f"semantic model {self.name!r} failed validation:\n  - {joined}"
            )

        output_dir = Path(output_dir)
        item_dir = output_dir / f"{self.name}.SemanticModel"
        definition = item_dir / "definition"

        write_artifact_file(item_dir / ".platform", self._emit_platform())
        write_artifact_file(item_dir / "definition.pbism", self._emit_pbism())
        write_artifact_file(definition / "database.tmdl", self._emit_database())
        write_artifact_file(definition / "model.tmdl", self._emit_model())
        write_artifact_file(definition / "expressions.tmdl", self._emit_expressions())
        write_artifact_file(
            definition / "relationships.tmdl", self._emit_relationships()
        )
        write_artifact_file(
            definition / "cultures" / f"{self.culture}.tmdl",
            self._emit_culture(),
        )
        for t in self.tables:
            write_artifact_file(
                definition / "tables" / f"{t.name}.tmdl", _emit_table(t)
            )

        # Run the lightweight TMDL collision check on what we just wrote.
        # This duplicates the in-memory check in `validate()` but guards
        # against any bug in our emission that produces a colliding pair.
        issues = check_name_collisions(item_dir)
        if issues:
            joined = "\n  - ".join(f"{i.path.name}: {i.message}" for i in issues)
            raise SemanticModelError(
                f"emitted TMDL still has collisions (this is a pyfabric bug):\n  - {joined}"
            )

        log.info(
            "semantic_model.save_to_disk complete",
            model=self.name,
            tables=len(self.tables),
            relationships=len(self.relationships),
            path=str(item_dir),
        )
        return item_dir

    # ── Per-file emitters ──────────────────────────────────────────────────

    def _emit_platform(self) -> str:
        import json

        return json.dumps(
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                "metadata": {
                    "type": "SemanticModel",
                    "displayName": self.name,
                    **({"description": self.description} if self.description else {}),
                },
                "config": {"version": "2.0", "logicalId": self.logical_id},
            },
            indent=2,
        )

    def _emit_pbism(self) -> str:
        import json

        return json.dumps({"version": "4.0", "settings": {}}, indent=2)

    def _emit_database(self) -> str:
        return f"database\n\tcompatibilityLevel: {self.compatibility_level}"

    def _emit_model(self) -> str:
        order = (
            [f"{s.name}WorkspaceId" for s in self.sources]
            + [f"{s.name}LakehouseId" for s in self.sources]
            + [s.name for s in self.sources]
            + [t.name for t in self.tables]
        )
        # PBI_QueryOrder is a JSON array embedded as a TMDL annotation value.
        import json

        order_json = json.dumps(order)
        # User annotations override the auto-generated ones if keys collide;
        # otherwise both sets render. PBI_QueryOrder is auto-emitted because
        # callers shouldn't have to re-derive the parameter+source+table
        # ordering by hand.
        auto_annotations = {
            "PBI_QueryOrder": order_json,
            "__PBI_TimeIntelligenceEnabled": "0",
        }
        merged_annotations = {**auto_annotations, **self.annotations}
        annotation_block = "\n\n".join(
            f"\tannotation {k} = {v}" for k, v in merged_annotations.items()
        )
        return (
            "model Model\n"
            f"\tculture: {self.culture}\n"
            "\tdefaultPowerBIDataSourceVersion: powerBI_V3\n"
            "\tsourceQueryCulture: en-US\n"
            "\tdataAccessOptions\n"
            "\t\tlegacyRedirects\n"
            "\t\treturnErrorValuesAsNull\n"
            "\n"
            f"{annotation_block}"
        )

    def _emit_expressions(self) -> str:
        chunks: list[str] = []
        for s in self.sources:
            ws_id_name = f"{s.name}WorkspaceId"
            lh_id_name = f"{s.name}LakehouseId"
            chunks.append(
                _emit_parameter_expression(
                    ws_id_name, s.workspace_id, _lineage(ws_id_name)
                )
            )
            chunks.append(
                _emit_parameter_expression(
                    lh_id_name, s.lakehouse_id, _lineage(lh_id_name)
                )
            )
            chunks.append(_emit_lakehouse_expression(s, _lineage(s.name)))
        return "\n\n".join(chunks)

    def _emit_relationships(self) -> str:
        if not self.relationships:
            return ""
        return "\n\n".join(_emit_relationship(r) for r in self.relationships)

    def _emit_culture(self) -> str:
        return f"cultureInfo {self.culture}"


# ── Internal emit helpers ──────────────────────────────────────────────────


def _lineage(*parts: str) -> str:
    """Deterministic UUID for an emitted-object lineageTag.

    Same parts → same UUID across regenerations, so re-emitting the
    model produces a minimal diff.
    """
    return str(uuid.uuid5(_LINEAGE_NS, ".".join(parts)))


def _emit_parameter_expression(name: str, value: str, lineage_tag: str) -> str:
    """A `expression Foo = "bar" meta [...]` parameter declaration."""
    escaped_value = value.replace('"', '""')
    return (
        f'expression {name} = "{escaped_value}" '
        'meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]\n'
        f"\tlineageTag: {lineage_tag}\n"
        "\n"
        "\tannotation PBI_NavigationStepName = Navigation\n"
        "\n"
        "\tannotation PBI_ResultType = Text"
    )


def _emit_lakehouse_expression(s: LakehouseSource, lineage_tag: str) -> str:
    """The shared ``Lakehouse.Contents`` navigation expression."""
    ws_id_name = f"{s.name}WorkspaceId"
    lh_id_name = f"{s.name}LakehouseId"
    return (
        f"expression {s.name} =\n"
        "\t\tlet\n"
        "\t\t    Source = Lakehouse.Contents(null),\n"
        f"\t\t    Workspace = Source{{[workspaceId = {ws_id_name}]}}[Data],\n"
        f"\t\t    Lakehouse = Workspace{{[lakehouseId = {lh_id_name}]}}[Data]\n"
        "\t\tin\n"
        "\t\t    Lakehouse\n"
        f"\tlineageTag: {lineage_tag}\n"
        "\n"
        "\tannotation PBI_NavigationStepName = Navigation\n"
        "\n"
        "\tannotation PBI_ResultType = Record"
    )


def _emit_table(t: Table) -> str:
    """One ``definition/tables/<name>.tmdl`` file."""
    parts: list[str] = []
    if t.description:
        parts.extend(_doc_comment(t.description))
    parts.append(f"table {t.name}")
    parts.append(f"\tlineageTag: {_lineage(t.name)}")
    if t.is_hidden:
        parts.append("\tisHidden")
    if t.data_category:
        parts.append(f"\tdataCategory: {t.data_category}")
    parts.append("")  # blank line before measures/columns

    for m in t.measures:
        parts.append(_emit_measure(t, m))
        parts.append("")

    for c in t.columns:
        parts.append(_emit_column(t, c))
        parts.append("")

    parts.append(_emit_partition(t))
    parts.append("")
    # Auto-emitted PBI_ResultType plus any user-supplied annotations.
    # User annotations override the auto one when keys collide.
    table_annotations = {"PBI_ResultType": "Table", **t.annotations}
    parts.extend(f"\tannotation {k} = {v}" for k, v in table_annotations.items())
    return "\n".join(parts)


def _emit_measure(t: Table, m: Measure) -> str:
    """Single-measure block under a table."""
    lines: list[str] = []
    if m.description:
        lines.extend(_doc_comment(m.description, indent="\t"))
    body_lines = m.expression.strip("\n").splitlines() or [""]
    if len(body_lines) == 1:
        lines.append(f"\tmeasure '{m.name}' = {body_lines[0]}")
    else:
        lines.append(f"\tmeasure '{m.name}' =")
        for body_line in body_lines:
            lines.append(f"\t\t\t{body_line}")
    if m.format_string is not None:
        lines.append(f"\t\tformatString: {m.format_string}")
    lines.append(f"\t\tlineageTag: {_lineage(t.name, 'measure', m.name)}")
    if m.is_hidden:
        lines.append("\t\tisHidden")
    for k, v in m.annotations.items():
        lines.append(f"\t\tannotation {k} = {v}")
    return "\n".join(lines)


def _emit_column(t: Table, c: Column) -> str:
    """Single-column block under a table."""
    lines: list[str] = []
    if c.description:
        lines.extend(_doc_comment(c.description, indent="\t"))
    lines.append(f"\tcolumn {c.name}")
    lines.append(f"\t\tdataType: {c.data_type}")
    if c.format_string is not None:
        lines.append(f"\t\tformatString: {c.format_string}")
    lines.append(f"\t\tlineageTag: {_lineage(t.name, 'column', c.name)}")
    if c.data_category:
        lines.append(f"\t\tdataCategory: {c.data_category}")
    lines.append(f"\t\tsummarizeBy: {c.summarize_by}")
    lines.append(f"\t\tsourceColumn: {c.source_column or c.name}")
    if c.is_key:
        lines.append("\t\tisKey")
    if c.is_hidden:
        lines.append("\t\tisHidden")
    # Auto-emit SummarizationSetBy = Automatic unless caller overrides it.
    column_annotations = {"SummarizationSetBy": "Automatic", **c.annotations}
    for k, v in column_annotations.items():
        lines.append(f"\t\tannotation {k} = {v}")
    return "\n".join(lines)


def _emit_partition(t: Table) -> str:
    """The ``partition`` block reading from the Lakehouse via the shared M expression."""
    return (
        f"\tpartition {t.name} = m\n"
        "\t\tmode: import\n"
        "\t\tsource =\n"
        "\t\t\t\tlet\n"
        f"\t\t\t\t    Source = {t.source.name},\n"
        f'\t\t\t\t    tbl = Source{{[Id="{t.name}", Schema="{t.schema}"]}}[Data]\n'
        "\t\t\t\tin\n"
        "\t\t\t\t    tbl"
    )


def _emit_relationship(r: Relationship) -> str:
    lineage_tag = _lineage(
        "relationship", r.from_table, r.from_column, r.to_table, r.to_column
    )
    parts = [
        f"relationship {lineage_tag}",
        f"\tfromColumn: {r.from_table}.{r.from_column}",
        f"\ttoColumn: {r.to_table}.{r.to_column}",
    ]
    if r.cross_filter != "single":
        parts.append(f"\tcrossFilteringBehavior: {r.cross_filter}DirectionFilter")
    if not r.is_active:
        parts.append("\tisActive: false")
    return "\n".join(parts)


def _doc_comment(text: str, *, indent: str = "") -> list[str]:
    """Render multi-line text as TMDL ``///`` description lines.

    TMDL concatenates consecutive ``///`` lines into a single description
    string, preserving line breaks between them.
    """
    return [f"{indent}/// {line}" for line in text.splitlines() or [""]]
