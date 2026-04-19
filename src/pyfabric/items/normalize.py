"""Normalize Fabric artifact files to the byte format Fabric writes itself.

Fabric's git-sync rewrites artifact files (`.platform`, `notebook-content.py`,
every `*.tmdl`, `*.json`, `*.yml`, etc.) in its own canonical form every
time a user opens or refreshes the item in the portal. The canonical
form is **file-type specific** — discovered by inspecting Fabric-authored
commits directly:

| Path pattern                                  | Line endings | Trailing NL |
|-----------------------------------------------|--------------|-------------|
| ``*.Lakehouse/alm.settings.json``             | CRLF         | No          |
| ``*.Environment/Setting/Sparkcompute.yml``    | CRLF         | Yes (CRLF)  |
| ``*.Notebook/notebook-content.py``            | LF           | Yes (LF)    |
| ``*.Notebook/notebook-content.sql``           | LF           | Yes (LF)    |
| everything else in Fabric artifact folders    | LF           | No          |

If committed bytes don't match this convention, every Fabric sync cycle
flags the file as "changed by Fabric" and pushes a no-op edit back into
git — a permanent flap. This module routes every artifact-folder write
through ``write_artifact_file`` so callers can't accidentally produce
the wrong bytes, and ``normalize_tree`` provides a one-shot fixer for
existing repos.

Usage::

    from pyfabric.items.normalize import (
        canonical_bytes,
        is_canonical,
        normalize_tree,
        write_artifact_file,
    )

    # Write a single file the right way for its path:
    write_artifact_file(
        Path("ws/sm_x.SemanticModel/definition/model.tmdl"),
        "model Model\\n\\tculture: en-US",
    )

    # Walk a tree and fix any drifted files:
    result = normalize_tree(Path("ws/"))
    print(f"Normalized {len(result.changed)} files")

    # Or check-only (returns False without writing):
    ok = is_canonical(Path("ws/sm_x.SemanticModel/definition/model.tmdl"))
"""

import fnmatch
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import structlog

log = structlog.get_logger()


# ── Public constants ────────────────────────────────────────────────────────

#: UTF-8 byte-order mark — stripped from artifact files because Fabric never emits one.
BOM = b"\xef\xbb\xbf"

#: Path globs covering every Fabric artifact file type git-sync handles.
#: Used by ``normalize_tree`` and ``lint_tree`` to enumerate files.
#: Patterns are relative to a workspace root (the directory containing
#: the ``*.{ItemType}`` folders).
ARTIFACT_GLOBS: tuple[str, ...] = (
    "*.Lakehouse/.platform",
    "*.Lakehouse/*.json",
    "*.Notebook/.platform",
    "*.Notebook/notebook-content.py",
    "*.Notebook/notebook-content.sql",
    "*.Dataflow/.platform",
    "*.Dataflow/*.json",
    "*.Dataflow/*.pq",
    "*.Environment/**/*",
    "*.VariableLibrary/**/*",
    "*.SemanticModel/.platform",
    "*.SemanticModel/definition.pbism",
    "*.SemanticModel/definition/**/*.tmdl",
    "*.SemanticModel/definition/**/*.json",
    "*.Report/.platform",
    "*.Report/definition.pbir",
    "*.Report/definition/**/*.json",
    "*.Report/definition/**/*.pbir",
)

# File-type-specific rules. First match wins. Order matters.
# Tuple shape: (path-glob, line-ending, trailing-newline-bool).
_RULES: tuple[tuple[str, str, bool], ...] = (
    # CRLF + no trailing newline
    ("*.Lakehouse/alm.settings.json", "\r\n", False),
    # CRLF + trailing CRLF
    ("*.Environment/Setting/Sparkcompute.yml", "\r\n", True),
    ("*.Environment/Setting/*.yml", "\r\n", True),  # future-proofing
    # LF + trailing LF
    ("*.Notebook/notebook-content.py", "\n", True),
    ("*.Notebook/notebook-content.sql", "\n", True),
    # LF + no trailing — default for everything else
    ("*", "\n", False),
)


# ── Public types ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FileRule:
    """The byte convention applied to a single artifact file."""

    line_ending: str  # "\n" or "\r\n"
    trailing_newline: bool


@dataclass
class NormalizeResult:
    """Summary of a ``normalize_tree`` (or ``lint_tree``) walk."""

    root: Path
    checked: list[Path] = field(default_factory=list)
    changed: list[Path] = field(default_factory=list)
    dry_run: bool = False

    @property
    def is_canonical(self) -> bool:
        """True if every checked file already matched the canonical bytes."""
        return len(self.changed) == 0


# ── Public functions ────────────────────────────────────────────────────────


def rule_for(rel_path: str) -> FileRule:
    """Return the byte-convention rule that applies to ``rel_path``.

    ``rel_path`` is the path relative to the workspace root, with forward
    slashes. The first matching glob in ``_RULES`` wins; the catch-all
    ``"*"`` ensures every artifact path resolves to a rule.
    """
    for pattern, eol, trailing in _RULES:
        if fnmatch.fnmatch(rel_path, pattern):
            return FileRule(line_ending=eol, trailing_newline=trailing)
    # Defensive fallback — the catch-all in _RULES means we never reach here.
    return FileRule(line_ending="\n", trailing_newline=False)


def canonical_bytes(rel_path: str, raw: bytes) -> bytes:
    """Return the canonical bytes Fabric would produce for this path.

    Strips a UTF-8 BOM if present, normalizes line endings to the rule
    for ``rel_path``, and adds or removes the trailing newline to match.
    Binary files (those that fail UTF-8 decode) pass through unchanged.
    """
    if raw.startswith(BOM):
        raw = raw[3:]
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw
    rule = rule_for(rel_path)
    joined = rule.line_ending.join(text.splitlines())
    if rule.trailing_newline:
        joined += rule.line_ending
    return joined.encode("utf-8")


def write_artifact_file(
    path: Path,
    content: str | bytes,
    *,
    workspace_root: Path | None = None,
) -> None:
    """Write content to a Fabric artifact file in the canonical byte format.

    Use this in place of ``Path.write_text`` / ``Path.write_bytes``
    anywhere inside a Fabric artifact folder. The rule applied is
    inferred from ``path``'s position relative to ``workspace_root``
    (or its filename pattern alone if ``workspace_root`` is ``None``).

    Args:
        path: Destination path. Parent directories are created if missing.
        content: File content. Strings are encoded as UTF-8 first; bytes
            are processed as-is.
        workspace_root: Root directory used to compute the relative path
            for rule matching. If ``None``, ``path.name`` is used, which
            is enough for most cases (the rules key off filename + nearest
            ``*.{ItemType}`` ancestor).
    """
    raw = content.encode("utf-8") if isinstance(content, str) else content
    rel = _relative_for_rule(path, workspace_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(rel, raw))


def is_canonical(path: Path, *, workspace_root: Path | None = None) -> bool:
    """Return True if ``path``'s bytes already match the canonical form."""
    raw = path.read_bytes()
    rel = _relative_for_rule(path, workspace_root)
    return canonical_bytes(rel, raw) == raw


def normalize_tree(
    root: Path,
    *,
    dry_run: bool = False,
    extra_globs: Iterable[str] | None = None,
) -> NormalizeResult:
    """Walk ``root`` and rewrite every artifact file to canonical bytes.

    Idempotent. ``dry_run=True`` performs the same comparison without
    writing — use it for a "would this commit be a no-op?" check (e.g. in
    a pre-push hook). ``extra_globs`` lets callers register additional
    artifact-path patterns beyond ``ARTIFACT_GLOBS``.

    Returns a ``NormalizeResult`` listing every file that was checked
    and every file that did (or would) change.
    """
    result = NormalizeResult(root=root, dry_run=dry_run)
    globs = tuple(ARTIFACT_GLOBS) + tuple(extra_globs or ())
    seen: set[Path] = set()
    for pattern in globs:
        for f in root.glob(pattern):
            if not f.is_file() or f in seen:
                continue
            seen.add(f)
            result.checked.append(f)
            raw = f.read_bytes()
            rel = _relative_with_fwdslash(f, root)
            new = canonical_bytes(rel, raw)
            if new != raw:
                result.changed.append(f)
                if not dry_run:
                    f.write_bytes(new)
    log.info(
        "normalize_tree complete",
        root=str(root),
        checked=len(result.checked),
        changed=len(result.changed),
        dry_run=dry_run,
    )
    return result


# ── Internals ───────────────────────────────────────────────────────────────


def _relative_with_fwdslash(p: Path, root: Path) -> str:
    """Return ``p`` relative to ``root`` with forward slashes (cross-platform)."""
    return str(p.relative_to(root)).replace("\\", "/")


def _relative_for_rule(path: Path, workspace_root: Path | None) -> str:
    """Compute the path string used to look up a rule.

    With a workspace root, returns the full relative path (forward slashes)
    so globs anchored at ``*.{ItemType}/`` match. Without it, walks up
    looking for the nearest ``*.{ItemType}`` ancestor and reconstructs a
    relative path from there. Falls back to the bare filename if no such
    ancestor exists — adequate for the catch-all ``"*"`` rule but means
    file-type-specific rules (e.g. CRLF for ``alm.settings.json``) only
    fire when an item folder is present in the path.
    """
    if workspace_root is not None:
        return _relative_with_fwdslash(path, workspace_root)
    for ancestor in path.parents:
        if "." in ancestor.name and ancestor.name.split(".")[-1] in _ITEM_TYPES:
            return _relative_with_fwdslash(path, ancestor.parent)
    return path.name


# Item-type suffixes recognized when walking up to find the workspace root.
# Keep in sync with ``pyfabric.items.types.ITEM_TYPES`` keys.
_ITEM_TYPES: frozenset[str] = frozenset(
    {
        "Lakehouse",
        "Notebook",
        "Dataflow",
        "Environment",
        "VariableLibrary",
        "SemanticModel",
        "Report",
    }
)
