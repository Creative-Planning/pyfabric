"""
Install pyfabric's Claude reference memories into the user's active Claude
profile (or the default profile if none is active).

Exposed via the `pyfabric` CLI as:

    pyfabric install-claude-memory [--target DIR] [--force] [--dry-run]

Memory source lives inside the installed package at
``pyfabric/claude_memory/*.md`` and ships with every PyPI release, so a
`pip install --pre pyfabric` followed by `pyfabric install-claude-memory`
is all a colleague needs to get Claude (or any Claude-compatible assistant
that honours file-based memory) primed for pyfabric work.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import shutil
import sys
from importlib import resources
from pathlib import Path
from typing import TextIO

_MEMORY_PKG = "pyfabric.claude_memory"
_INDEX_FILE = "MEMORY.md"
_LINK_RX = re.compile(r"\(([^)]+\.md)\)")
_FRONTMATTER_RX = re.compile(r"\A---\r?\n.*?\r?\n---\r?\n", re.DOTALL)


def _claude_config_root() -> Path:
    """Return the Claude config root — ``$CLAUDE_CONFIG_DIR`` if set, else ``~/.claude``."""
    claude_cfg = os.environ.get("CLAUDE_CONFIG_DIR")
    return Path(claude_cfg) if claude_cfg else Path.home() / ".claude"


def _slugify_path(p: Path) -> str:
    """Convert an absolute path to the project-slug format Claude uses.

    Claude Code stores per-project state under ``<config>/projects/<slug>/``
    where the slug is the absolute path with every non-alphanumeric
    character (except ``-``) replaced by ``-`` — so not just separators,
    but also ``.`` in user names and ``_`` in repo names. Examples:

    - ``C:\\Users\\dave.catlett\\repos\\my_repo`` -> ``C--Users-dave-catlett-repos-my-repo``
    - ``/home/dave/repo``                         -> ``-home-dave-repo``
    """
    return re.sub(r"[^A-Za-z0-9-]", "-", str(p.resolve()))


def _find_project_root(start: Path) -> Path:
    """Walk up from ``start`` looking for ``.git``; fall back to ``start``.

    Claude Code keys project memory off the git repo root, not whichever
    subdirectory you happened to launch from. Match that behavior so
    memories land where Claude actually reads them.
    """
    start = start.resolve()
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return start


def _default_memory_dir(
    *,
    project_path: Path | None = None,
) -> Path:
    """Resolve the Claude memory directory for this install.

    - ``project_path=None`` (default) — global install: the user home dir's
      project slug, so memories apply to every Claude session. Path:
      ``<config>/projects/<slug-of-home>/memory/``.
    - ``project_path=<some path>`` — scoped install: the git repo root that
      contains that path, matching Claude's per-project memory layout.
      Path: ``<config>/projects/<slug-of-repo-root>/memory/``.
    """
    root = _claude_config_root()
    anchor = Path.home() if project_path is None else _find_project_root(project_path)
    return root / "projects" / _slugify_path(anchor) / "memory"


def _iter_package_memory() -> list[tuple[str, str]]:
    """Return (filename, content) for every .md file shipped under claude_memory/."""
    out: list[tuple[str, str]] = []
    pkg = resources.files(_MEMORY_PKG)
    for entry in pkg.iterdir():
        if not entry.is_file():
            continue
        if not entry.name.lower().endswith(".md"):
            continue
        out.append((entry.name, entry.read_text(encoding="utf-8")))
    # Stable order: index first, then alphabetical
    out.sort(key=lambda nc: (nc[0] != _INDEX_FILE, nc[0].lower()))
    return out


def _merge_memory_index(existing: str | None, additions: str) -> tuple[str, int]:
    """Merge `additions` into an existing MEMORY.md without duplicating entries.

    Entries are matched by the .md file name(s) they link to (``[text](file.md)``).
    Returns (merged_content, newly_added_line_count).
    """
    existing = existing or ""
    existing_lines = existing.splitlines()
    seen: set[str] = set()
    for line in existing_lines:
        seen.update(_LINK_RX.findall(line))

    new_lines: list[str] = []
    for line in additions.splitlines():
        links = _LINK_RX.findall(line)
        if links and all(link in seen for link in links):
            continue
        if links:
            new_lines.append(line)
            seen.update(links)
        # Non-entry lines (blank, header, etc.) are dropped — MEMORY.md is an
        # index, and we don't want to duplicate headers or blank spacing.

    if not new_lines:
        return existing, 0

    body = existing
    if body and not body.endswith("\n"):
        body += "\n"
    body += "\n".join(new_lines) + "\n"
    return body, len(new_lines)


def install(
    target: Path | None = None,
    *,
    project_path: Path | None = None,
    force: bool = False,
    dry_run: bool = False,
    out: TextIO | None = None,
) -> int:
    """Install claude_memory/*.md into the target memory directory.

    - ``target`` — explicit memory dir to write into. If provided, wins.
    - ``project_path`` — when ``target`` is None, scope the install to the
      git repo containing this path (falls back to the path itself). If
      both are None, defaults to a global install under the user home's
      project slug so every Claude session picks up the memories.

    Returns a process exit code (0 success, non-zero on error).
    """
    out = out or sys.stdout
    if target is None:
        target = _default_memory_dir(project_path=project_path)
    try:
        memories = _iter_package_memory()
    except ModuleNotFoundError:
        print(
            f"ERROR: package '{_MEMORY_PKG}' not found. "
            f"Is pyfabric installed correctly?",
            file=sys.stderr,
        )
        return 2

    if not memories:
        print(
            "No claude_memory/*.md files bundled with this pyfabric install.",
            file=out,
        )
        return 0

    action = "[dry-run] would" if dry_run else ""
    if not dry_run:
        target.mkdir(parents=True, exist_ok=True)

    copied = 0
    merged_lines = 0
    skipped = 0

    for name, content in memories:
        dest = target / name
        if name == _INDEX_FILE:
            existing = dest.read_text(encoding="utf-8") if dest.exists() else None
            merged, added = _merge_memory_index(existing, content)
            if added == 0:
                print(f"  {name}: already up-to-date", file=out)
                continue
            print(f"  {name}: {action or 'merged'} {added} new entry line(s)", file=out)
            if not dry_run:
                dest.write_text(merged, encoding="utf-8")
            merged_lines += added
            continue

        if dest.exists() and not force:
            print(f"  {name}: already present (use --force to overwrite)", file=out)
            skipped += 1
            continue
        print(f"  {name}: {action or 'installed'}", file=out)
        if not dry_run:
            shutil.copyfile(
                str(resources.files(_MEMORY_PKG) / name),
                str(dest),
            )
        copied += 1

    print("", file=out)
    print(f"Target  : {target}", file=out)
    print(
        f"Summary : {copied} copied, {skipped} skipped, "
        f"{merged_lines} index line(s) merged" + (" (dry-run)" if dry_run else ""),
        file=out,
    )
    if not os.environ.get("CLAUDE_CONFIG_DIR"):
        print(
            "Note    : no CLAUDE_CONFIG_DIR set — memories went to the default "
            "~/.claude/. If you use Claude Sessions, activate a profile first "
            "(claude-use <name>) and re-run to install into that profile.",
            file=out,
        )
    return 0


def _strip_frontmatter(text: str) -> str:
    return _FRONTMATTER_RX.sub("", text, count=1).lstrip()


def emit_context(out: TextIO | None = None) -> int:
    """Write concatenated, frontmatter-stripped memory bodies to ``out``.

    Intended for piping into whatever file the user's AI assistant expects,
    e.g.::

        pyfabric emit-context > .github/copilot-instructions.md
        pyfabric emit-context > .cursorrules

    The MEMORY.md index is skipped — it's a pointer list that only makes
    sense inside Claude's memory directory. Each included file is preceded
    by an ``## <filename>`` heading so readers can see section boundaries.
    """
    if out is None:
        # Default stdout on Windows is often cp1252, which chokes on the em-dashes
        # and arrows in the memory files. Force UTF-8 so `> file.md` works everywhere.
        reconfigure = getattr(sys.stdout, "reconfigure", None)
        if reconfigure is not None:
            with contextlib.suppress(OSError):
                reconfigure(encoding="utf-8")
        out = sys.stdout
    try:
        memories = _iter_package_memory()
    except ModuleNotFoundError:
        print(
            f"ERROR: package '{_MEMORY_PKG}' not found. "
            f"Is pyfabric installed correctly?",
            file=sys.stderr,
        )
        return 2

    body_files = [(n, c) for n, c in memories if n != _INDEX_FILE]
    if not body_files:
        print("No memory files bundled with this pyfabric install.", file=sys.stderr)
        return 0

    print(
        "<!-- Generated by `pyfabric emit-context`. Safe to re-run; do not hand-edit — re-generate after upgrading pyfabric. -->",
        file=out,
    )
    print("", file=out)
    print("# pyfabric reference context", file=out)
    print("", file=out)
    for i, (name, content) in enumerate(body_files):
        if i > 0:
            print("", file=out)
            print("---", file=out)
            print("", file=out)
        print(f"## {name}", file=out)
        print("", file=out)
        print(_strip_frontmatter(content).rstrip(), file=out)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyfabric install-claude-memory",
        description=(
            "Install pyfabric's Claude reference memories into your active "
            "Claude profile. Default is a global install — memories apply "
            "to every Claude session. Use --project to scope to a single repo."
        ),
    )
    parser.add_argument(
        "--project",
        nargs="?",
        const=".",
        default=None,
        type=Path,
        metavar="PATH",
        help=(
            "Install per-project (Claude's git-repo-scoped memory). "
            "PATH defaults to the current directory; the installer walks up "
            "to the containing .git root. Omit for the default global install."
        ),
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=None,
        help=(
            "Explicit memory dir. Overrides both --project and the default "
            "global install. Useful for testing."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing individual memory files (MEMORY.md is always merged, never overwritten)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing anything",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return install(
        target=args.target,
        project_path=args.project,
        force=args.force,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
