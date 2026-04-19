"""Lightweight TMDL checks for SemanticModel items.

These catch the most common Fabric sync rejections that a folder-shape
validator (``pyfabric.items.validate``) misses, without depending on a
full TMDL parser. Today: measure-vs-column name collisions on the same
table.

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
    """A single collision (or other check failure) found in a TMDL file."""

    path: Path
    message: str


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
