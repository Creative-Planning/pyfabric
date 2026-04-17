"""Tests for pyfabric.claude_install — memory installer."""

from __future__ import annotations

import io
from pathlib import Path

from pyfabric.claude_install import (
    _claude_config_root,
    _default_memory_dir,
    _find_project_root,
    _iter_package_memory,
    _merge_memory_index,
    _slugify_path,
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


class TestPathSlug:
    def test_slug_replaces_separators_and_colon(self, tmp_path: Path):
        p = tmp_path / "repo"
        p.mkdir()
        slug = _slugify_path(p)
        # Slug is the fully-resolved absolute path with : \ / -> -
        assert ":" not in slug
        assert "\\" not in slug
        assert "/" not in slug
        # Trailing component survives
        assert slug.endswith("-repo")

    def test_find_project_root_walks_to_git(self, tmp_path: Path):
        root = tmp_path / "myrepo"
        sub = root / "pkg" / "deep"
        sub.mkdir(parents=True)
        (root / ".git").mkdir()  # dir marker is fine
        assert _find_project_root(sub) == root.resolve()

    def test_find_project_root_falls_back_when_no_git(self, tmp_path: Path):
        sub = tmp_path / "nogit" / "deep"
        sub.mkdir(parents=True)
        # No .git anywhere; falls back to start itself.
        assert _find_project_root(sub) == sub.resolve()


class TestDefaultMemoryDir:
    def test_global_uses_home_slug(self, monkeypatch):
        monkeypatch.delenv("CLAUDE_CONFIG_DIR", raising=False)
        result = _default_memory_dir(project_path=None)
        # Must sit under <claude_root>/projects/<home-slug>/memory
        parts = result.parts
        assert parts[-1] == "memory"
        assert parts[-3] == "projects"
        assert parts[-2] == _slugify_path(Path.home())

    def test_project_path_uses_git_root_slug(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("CLAUDE_CONFIG_DIR", raising=False)
        repo = tmp_path / "repo"
        nested = repo / "a" / "b"
        nested.mkdir(parents=True)
        (repo / ".git").mkdir()

        result = _default_memory_dir(project_path=nested)
        # Slug should match the repo root, not the nested subdir
        assert result.parts[-2] == _slugify_path(repo)

    def test_respects_claude_config_dir(self, tmp_path: Path, monkeypatch):
        fake_cfg = tmp_path / "profile"
        fake_cfg.mkdir()
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(fake_cfg))

        result = _default_memory_dir(project_path=None)
        assert _claude_config_root() == fake_cfg
        # Path starts at the fake config dir
        assert str(result).startswith(str(fake_cfg))


class TestInstallDefaultsToClaudesRealLayout:
    def test_global_install_lands_under_projects_slug_memory(
        self, tmp_path: Path, monkeypatch
    ):
        """Regression for prior bug where install wrote to <config>/memory
        (which Claude never reads) instead of <config>/projects/<slug>/memory."""
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path))

        rc = install(out=io.StringIO())  # no target, no project — global default
        assert rc == 0

        # Files should NOT be in the wrong place
        assert not (tmp_path / "memory" / "MEMORY.md").exists()
        # Files SHOULD be under projects/<home-slug>/memory/
        home_slug = _slugify_path(Path.home())
        expected = tmp_path / "projects" / home_slug / "memory"
        assert (expected / "MEMORY.md").exists()
        assert (expected / "pyfabric.md").exists()

    def test_project_install_lands_under_repo_slug(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "cfg"))
        repo = tmp_path / "fake-repo"
        (repo / ".git").mkdir(parents=True)

        rc = install(project_path=repo, out=io.StringIO())
        assert rc == 0

        expected = tmp_path / "cfg" / "projects" / _slugify_path(repo) / "memory"
        assert (expected / "pyfabric.md").exists()
