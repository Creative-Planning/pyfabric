"""Lightweight TMDL checks for SemanticModel items.

These catch the most common Fabric sync rejections that a folder-shape
validator (``pyfabric.items.validate``) misses, without depending on a
full TMDL parser: measure-vs-column name collisions, compatibility-level
problems, orphan column references, obviously-broken DAX (unbalanced
parentheses), and duplicate lineageTags. Run them all at once with
:func:`lint_semantic_model` (wired into
:func:`pyfabric.items.validate.validate_item` for SemanticModel items).

Background: DAX identifiers within a table live in one flat namespace
that is **case-insensitive** in the Analysis Services engine behind
Fabric. TMDL parsers don't enforce the rule at save time, so a model
with both ``measure 'Status'`` and ``column status`` on the same table
saves cleanly locally — and then Fabric rejects the import with::

    Dataset_Import_FailedToImportDataset: The 'Status' measure cannot
    be created because a column with the same name already exists.

The check here surfaces those collisions before push.

Usage::

    from pyfabric.items.validate_tmdl import check_name_collisions

    issues = check_name_collisions(Path("ws/sm_x.SemanticModel"))
    for issue in issues:
        print(f"{issue.path.name}: {issue.message}")
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

#: Minimum compatibilityLevel for a PBIP-format model (the baseline the
#: builders emit) and for DirectLake partitions respectively.
PBIP_BASELINE_COMPAT_LEVEL = 1567
DIRECTLAKE_MIN_COMPAT_LEVEL = 1604

# Match ``measure 'Foo Bar' = ...`` or ``measure FooBar = ...``.
# TMDL allows quoted (single quote) or bare identifiers; bare ones must
# match the standard identifier grammar (letter/underscore + word chars).
_MEASURE_RE = re.compile(
    r"""^\s*measure\s+
        (?: '([^']+)'                       # 'quoted name' (group 1)
          | ([A-Za-z_][A-Za-z0-9_]*)        # or bareIdentifier (group 2)
        )
        \s*=
    """,
    re.VERBOSE | re.MULTILINE,
)

# Match ``column 'Foo Bar'`` or ``column foo_bar``. Same identifier rules.
_COLUMN_RE = re.compile(
    r"""^\s*column\s+
        (?: '([^']+)'
          | ([A-Za-z_][A-Za-z0-9_]*)
        )
        \s*$
    """,
    re.VERBOSE | re.MULTILINE,
)


@dataclass(frozen=True)
class TmdlIssue:
    """A single check failure found in a TMDL file.

    ``severity`` routes the issue in ``validate_item``: ``"error"`` fails
    validation; ``"warning"`` is reported but non-fatal (used for rules
    whose regex parsing can produce false positives, e.g. orphan-column
    detection cannot see calculated columns).
    """

    path: Path
    message: str
    severity: Literal["error", "warning"] = "error"


def parse_table_identifiers(tmdl_text: str) -> tuple[set[str], set[str]]:
    """Extract measure names and column names from one table's TMDL.

    Returns ``(measure_names, column_names)``, both **case-insensitive**
    (lower-cased). Strips surrounding quotes if present. Tolerates
    indentation, leading whitespace, and quoted-or-bare identifiers as
    TMDL allows.
    """
    measures: set[str] = set()
    for m in _MEASURE_RE.finditer(tmdl_text):
        name = (m.group(1) or m.group(2) or "").strip()
        if name:
            measures.add(name.lower())

    columns: set[str] = set()
    for m in _COLUMN_RE.finditer(tmdl_text):
        name = (m.group(1) or m.group(2) or "").strip()
        if name:
            columns.add(name.lower())

    return measures, columns


def check_name_collisions(item_dir: Path) -> list[TmdlIssue]:
    """Find measure-vs-column name collisions in every table TMDL under ``item_dir``.

    ``item_dir`` should be the root of a ``*.SemanticModel`` folder; the
    function looks for table TMDLs under ``definition/tables/*.tmdl``.
    Returns one ``TmdlIssue`` per file that has at least one collision.
    """
    issues: list[TmdlIssue] = []
    tables_dir = item_dir / "definition" / "tables"
    if not tables_dir.is_dir():
        return issues
    for tmdl_path in sorted(tables_dir.glob("*.tmdl")):
        text = tmdl_path.read_text(encoding="utf-8")
        measures, columns = parse_table_identifiers(text)
        clash = sorted(measures & columns)
        if clash:
            quoted = ", ".join(f"'{c}'" for c in clash)
            issues.append(
                TmdlIssue(
                    path=tmdl_path,
                    message=(
                        f"measure/column name collision (case-insensitive): {quoted}. "
                        "Fabric AS engine will reject the import. "
                        "Rename the measure (e.g. add a '%' or '#' prefix/suffix) "
                        "so it cannot collide with a column name."
                    ),
                )
            )
    return issues


# ── Additional lint rules (issue #42) ────────────────────────────────────────

_COMPAT_RE = re.compile(r"^\s*compatibilityLevel:\s*(\d+)", re.MULTILINE)
_DIRECTLAKE_MODE_RE = re.compile(r"^\s*mode:\s*directLake\b", re.MULTILINE)

# Table declaration: ``table Foo`` or ``table 'Foo Bar'``.
_TABLE_RE = re.compile(
    r"""^table\s+
        (?: '([^']+)'
          | ([A-Za-z_][A-Za-z0-9_]*)
        )
    """,
    re.VERBOSE | re.MULTILINE,
)

# ``lineageTag: <value>`` anywhere in a definition file.
_LINEAGE_TAG_RE = re.compile(r"^\s*lineageTag:\s*(\S+)", re.MULTILINE)

# Relationship endpoints: ``fromColumn: Table.Column`` (either part may be
# single-quoted when it contains spaces).
_REL_ENDPOINT_RE = re.compile(r"^\s*(?:fromColumn|toColumn):\s*(.+?)\s*$", re.MULTILINE)
_ENDPOINT_PARTS_RE = re.compile(
    r"""^(?: '(?P<qt>[^']+)' | (?P<bt>[A-Za-z_][A-Za-z0-9_]*) )
        \.
        (?: '(?P<qc>[^']+)' | (?P<bc>.+) )$
    """,
    re.VERBOSE,
)

# DAX column references: ``'Table Name'[Column]`` or ``Table[Column]``.
_DAX_COLUMN_REF_RE = re.compile(
    r"(?:'(?P<qt>[^']+)'|\b(?P<bt>[A-Za-z_][A-Za-z0-9_]*))\[(?P<col>[^\[\]]+)\]"
)

# DAX noise to strip before structural checks: string literals ("" is an
# escaped quote, i.e. two adjacent literals — handled naturally), line
# comments (// and --), and block comments.
_DAX_NOISE_RE = re.compile(r'"[^"]*"|//[^\n]*|--[^\n]*|/\*.*?\*/', re.DOTALL)

# A measure header line: ``measure 'X' = <rest>`` (rest may be empty for
# multiline expressions).
_MEASURE_HEADER_RE = re.compile(
    r"""^\s*measure\s+
        (?: '(?P<q>[^']+)' | (?P<b>[A-Za-z_][A-Za-z0-9_]*) )
        \s*=(?P<rest>.*)$
    """,
    re.VERBOSE,
)
# Lines that terminate a measure's expression body.
_BLOCK_TERMINATOR_RE = re.compile(
    r"^\s*(formatString:|lineageTag:|displayFolder:|isHidden\b|dataCategory:"
    r"|annotation\b|changedProperty\b|extendedProperty\b|///"
    r"|measure\s|column\s|partition\s|table\s|hierarchy\s)"
)


def _table_name(tmdl_text: str) -> str | None:
    """The declared table name of one ``tables/*.tmdl`` file (or None)."""
    m = _TABLE_RE.search(tmdl_text)
    if not m:
        return None
    return (m.group(1) or m.group(2) or "").strip() or None


def _iter_measure_expressions(tmdl_text: str) -> list[tuple[str, str]]:
    """Return ``(measure_name, expression_text)`` pairs from a table TMDL.

    Handles both the single-line form (``measure 'X' = SUM(...)``) and
    the indented multiline form. The expression body ends at the first
    property line (``formatString:``, ``lineageTag:``, ...), declaration,
    or blank line — hand-authored DAX with internal blank lines is
    truncated there (acceptable for a lint).
    """
    results: list[tuple[str, str]] = []
    lines = tmdl_text.splitlines()
    i = 0
    while i < len(lines):
        m = _MEASURE_HEADER_RE.match(lines[i])
        i += 1
        if not m:
            continue
        name = (m.group("q") or m.group("b") or "").strip()
        body = [m.group("rest")]
        while i < len(lines):
            line = lines[i]
            if not line.strip() or _BLOCK_TERMINATOR_RE.match(line):
                break
            body.append(line)
            i += 1
        results.append((name, "\n".join(body)))
    return results


def check_compatibility_level(item_dir: Path) -> list[TmdlIssue]:
    """Lint ``definition/database.tmdl``'s ``compatibilityLevel``.

    - **error** when any table partition is ``mode: directLake`` and the
      level is below 1604 (Fabric's sync failure for this is opaque);
    - **warning** when the attribute is missing or below the 1567 PBIP
      baseline.
    """
    issues: list[TmdlIssue] = []
    database_path = item_dir / "definition" / "database.tmdl"
    if not database_path.is_file():
        # Folder-shape validation reports missing definition files.
        return issues
    text = database_path.read_text(encoding="utf-8")
    match = _COMPAT_RE.search(text)

    tables_dir = item_dir / "definition" / "tables"
    has_directlake = tables_dir.is_dir() and any(
        _DIRECTLAKE_MODE_RE.search(p.read_text(encoding="utf-8"))
        for p in sorted(tables_dir.glob("*.tmdl"))
    )

    if match is None:
        needed = (
            f" and DirectLake partitions need {DIRECTLAKE_MIN_COMPAT_LEVEL}+"
            if has_directlake
            else ""
        )
        issues.append(
            TmdlIssue(
                path=database_path,
                message=(
                    "database.tmdl has no compatibilityLevel; PBIP models "
                    f"need at least {PBIP_BASELINE_COMPAT_LEVEL}{needed}"
                ),
                severity="error" if has_directlake else "warning",
            )
        )
        return issues

    level = int(match.group(1))
    if has_directlake and level < DIRECTLAKE_MIN_COMPAT_LEVEL:
        issues.append(
            TmdlIssue(
                path=database_path,
                message=(
                    f"compatibilityLevel {level} is too low for DirectLake "
                    f"partitions (need {DIRECTLAKE_MIN_COMPAT_LEVEL}+); "
                    "Fabric's sync failure for this is opaque"
                ),
                severity="error",
            )
        )
    elif level < PBIP_BASELINE_COMPAT_LEVEL:
        issues.append(
            TmdlIssue(
                path=database_path,
                message=(
                    f"compatibilityLevel {level} is below the PBIP baseline "
                    f"({PBIP_BASELINE_COMPAT_LEVEL})"
                ),
                severity="warning",
            )
        )
    return issues


def check_orphan_columns(item_dir: Path) -> list[TmdlIssue]:
    """Flag references to columns that are not declared on their table.

    Covers relationship endpoints (``fromColumn:``/``toColumn:``) and DAX
    column references (``'Table'[Column]`` / ``Table[Column]``) in measure
    expressions. Only references naming a **declared table** are checked —
    and all findings are **warnings**, because regex parsing cannot see
    calculated columns or variations.
    """
    issues: list[TmdlIssue] = []
    tables_dir = item_dir / "definition" / "tables"
    if not tables_dir.is_dir():
        return issues

    declared: dict[str, set[str]] = {}
    table_texts: dict[Path, str] = {}
    for tmdl_path in sorted(tables_dir.glob("*.tmdl")):
        text = tmdl_path.read_text(encoding="utf-8")
        table_texts[tmdl_path] = text
        name = _table_name(text)
        if name:
            _measures, columns = parse_table_identifiers(text)
            declared[name.lower()] = columns

    def _check_ref(table: str, column: str, path: Path, context: str) -> None:
        cols = declared.get(table.lower())
        if cols is not None and column.lower() not in cols:
            issues.append(
                TmdlIssue(
                    path=path,
                    message=(
                        f"{context} references column "
                        f"'{table}'[{column}] but table '{table}' declares "
                        "no such column (calculated columns are invisible "
                        "to this lint — ignore if that's the case)"
                    ),
                    severity="warning",
                )
            )

    relationships_path = item_dir / "definition" / "relationships.tmdl"
    if relationships_path.is_file():
        rel_text = relationships_path.read_text(encoding="utf-8")
        for endpoint in _REL_ENDPOINT_RE.findall(rel_text):
            parts = _ENDPOINT_PARTS_RE.match(endpoint)
            if not parts:
                continue
            table = (parts.group("qt") or parts.group("bt") or "").strip()
            column = (parts.group("qc") or parts.group("bc") or "").strip()
            if table and column:
                _check_ref(table, column, relationships_path, "relationship")

    for tmdl_path, text in table_texts.items():
        for measure_name, expression in _iter_measure_expressions(text):
            clean = _DAX_NOISE_RE.sub(" ", expression)
            for ref in _DAX_COLUMN_REF_RE.finditer(clean):
                table = (ref.group("qt") or ref.group("bt") or "").strip()
                column = ref.group("col").strip()
                if table and column:
                    _check_ref(table, column, tmdl_path, f"measure '{measure_name}'")
    return issues


def check_dax_paren_balance(item_dir: Path) -> list[TmdlIssue]:
    """Flag measures whose DAX has unbalanced parentheses.

    String literals and comments are stripped first, so parens inside
    ``"text"`` / ``// comments`` / ``/* blocks */`` don't count.
    """
    issues: list[TmdlIssue] = []
    tables_dir = item_dir / "definition" / "tables"
    if not tables_dir.is_dir():
        return issues
    for tmdl_path in sorted(tables_dir.glob("*.tmdl")):
        text = tmdl_path.read_text(encoding="utf-8")
        for measure_name, expression in _iter_measure_expressions(text):
            clean = _DAX_NOISE_RE.sub(" ", expression)
            balance = clean.count("(") - clean.count(")")
            if balance != 0:
                direction = "unclosed '('" if balance > 0 else "extra ')'"
                issues.append(
                    TmdlIssue(
                        path=tmdl_path,
                        message=(
                            f"measure '{measure_name}' has unbalanced "
                            f"parentheses ({direction}, off by "
                            f"{abs(balance)}) — the model will fail to import"
                        ),
                        severity="error",
                    )
                )
    return issues


def check_lineage_tag_uniqueness(item_dir: Path) -> list[TmdlIssue]:
    """Flag duplicate ``lineageTag`` values across all definition TMDLs.

    Duplicate lineageTags break the Fabric import; the builders derive
    theirs deterministically per object, so duplicates indicate a
    hand-edit gone wrong (usually copy-paste).
    """
    issues: list[TmdlIssue] = []
    definition = item_dir / "definition"
    if not definition.is_dir():
        return issues
    seen: dict[str, Path] = {}
    for tmdl_path in sorted(definition.rglob("*.tmdl")):
        text = tmdl_path.read_text(encoding="utf-8")
        for tag in _LINEAGE_TAG_RE.findall(text):
            first = seen.get(tag)
            if first is None:
                seen[tag] = tmdl_path
            else:
                issues.append(
                    TmdlIssue(
                        path=tmdl_path,
                        message=(
                            f"duplicate lineageTag {tag} (first seen in "
                            f"{first.name}) — lineageTags must be unique "
                            "across the model"
                        ),
                        severity="error",
                    )
                )
    return issues


def lint_semantic_model(item_dir: Path) -> list[TmdlIssue]:
    """Run every TMDL lint rule over a ``*.SemanticModel`` folder.

    Aggregates :func:`check_name_collisions`,
    :func:`check_compatibility_level`, :func:`check_orphan_columns`,
    :func:`check_dax_paren_balance`, and
    :func:`check_lineage_tag_uniqueness`.
    """
    return (
        check_name_collisions(item_dir)
        + check_compatibility_level(item_dir)
        + check_orphan_columns(item_dir)
        + check_dax_paren_balance(item_dir)
        + check_lineage_tag_uniqueness(item_dir)
    )
