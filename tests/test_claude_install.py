"""Tests for pyfabric.claude_install — memory installer."""

from __future__ import annotations

import io
from pathlib import Path

from pyfabric.claude_install import (
    _iter_package_memory,
    _merge_memory_index,
    _strip_frontmatter,
    emit_context,
    install,
)


class TestPackagedMemory:
    def test_ships_index_and_pyfabric_md(self):
        names = [n for n, _ in _iter_package_memory()]
        assert "MEMORY.md" in names
        assert "pyfabric.md" in names

    def test_index_sorted_first(self):
        names = [n for n, _ in _iter_package_memory()]
        assert names[0] == "MEMORY.md"


class TestMergeMemoryIndex:
    def test_merge_into_empty(self):
        merged, added = _merge_memory_index(None, "- [a](a.md) — desc\n")
        assert "a.md" in merged
        assert added == 1

    def test_skips_duplicate_by_filename(self):
        existing = "- [a](a.md) — desc\n"
        additions = "- [a](a.md) — different text but same link\n"
        merged, added = _merge_memory_index(existing, additions)
        assert added == 0
        assert merged == existing

    def test_appends_new_entry_alongside_existing(self):
        existing = "- [a](a.md) — first\n"
        additions = "- [b](b.md) — second\n"
        merged, added = _merge_memory_index(existing, additions)
        assert added == 1
        assert "a.md" in merged
        assert "b.md" in merged

    def test_drops_non_entry_noise(self):
        # Blank lines and headers in additions should not make it through;
        # MEMORY.md is a pure index.
        additions = "\n# Heading\n\n- [c](c.md) — ok\n"
        merged, added = _merge_memory_index("", additions)
        assert added == 1
        assert "Heading" not in merged


class TestInstall:
    def test_fresh_install(self, tmp_path: Path):
        buf = io.StringIO()
        rc = install(target=tmp_path, out=buf)
        assert rc == 0
        assert (tmp_path / "MEMORY.md").exists()
        assert (tmp_path / "pyfabric.md").exists()
        assert "pyfabric.md" in (tmp_path / "MEMORY.md").read_text()

    def test_rerun_is_idempotent(self, tmp_path: Path):
        install(target=tmp_path, out=io.StringIO())
        first = (tmp_path / "pyfabric.md").read_text()

        # Tamper with pyfabric.md — without --force it should NOT be overwritten
        (tmp_path / "pyfabric.md").write_text("LOCAL EDITS\n")
        buf = io.StringIO()
        install(target=tmp_path, out=buf)
        assert (tmp_path / "pyfabric.md").read_text() == "LOCAL EDITS\n"
        assert "already present" in buf.getvalue()

        # With --force, the package version wins
        install(target=tmp_path, force=True, out=io.StringIO())
        assert (tmp_path / "pyfabric.md").read_text() == first

    def test_dry_run_writes_nothing(self, tmp_path: Path):
        rc = install(target=tmp_path, dry_run=True, out=io.StringIO())
        assert rc == 0
        assert not (tmp_path / "MEMORY.md").exists()
        assert not (tmp_path / "pyfabric.md").exists()

    def test_strip_frontmatter(self):
        text = "---\nname: X\ntype: reference\n---\n\nBody here.\n"
        assert _strip_frontmatter(text) == "Body here.\n"

        # No frontmatter: leave intact
        plain = "Just body.\n"
        assert _strip_frontmatter(plain) == plain

    def test_emit_context_skips_index_and_strips_frontmatter(self):
        buf = io.StringIO()
        rc = emit_context(out=buf)
        assert rc == 0
        out = buf.getvalue()
        assert "pyfabric reference context" in out
        assert "## pyfabric.md" in out
        assert "## MEMORY.md" not in out  # index excluded
        assert "---\nname:" not in out  # frontmatter stripped

    def test_merge_preserves_foreign_entries(self, tmp_path: Path):
        (tmp_path).mkdir(parents=True, exist_ok=True)
        (tmp_path / "MEMORY.md").write_text(
            "- [other](other.md) -- installed by some other tool\n",
            encoding="utf-8",
        )
        (tmp_path / "other.md").write_text("placeholder\n", encoding="utf-8")

        install(target=tmp_path, out=io.StringIO())

        idx = (tmp_path / "MEMORY.md").read_text()
        assert "other.md" in idx  # foreign entry preserved
        assert "pyfabric.md" in idx  # our entry added
