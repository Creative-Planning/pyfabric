"""Tests for byte normalization of Fabric artifact files."""

from pathlib import Path

import pytest

from pyfabric.items.normalize import (
    BOM,
    canonical_bytes,
    is_canonical,
    normalize_tree,
    rule_for,
    write_artifact_file,
)

# ── rule_for ────────────────────────────────────────────────────────────────


class TestRuleFor:
    @pytest.mark.parametrize(
        "rel_path, expected_eol, expected_trailing",
        [
            # CRLF + no trailing newline
            ("lh.Lakehouse/alm.settings.json", "\r\n", False),
            # CRLF + trailing CRLF
            ("env.Environment/Setting/Sparkcompute.yml", "\r\n", True),
            ("env.Environment/Setting/something.yml", "\r\n", True),
            # LF + trailing LF
            ("nb.Notebook/notebook-content.py", "\n", True),
            ("nb.Notebook/notebook-content.sql", "\n", True),
            # LF + no trailing — defaults
            ("sm.SemanticModel/.platform", "\n", False),
            ("sm.SemanticModel/definition.pbism", "\n", False),
            ("sm.SemanticModel/definition/model.tmdl", "\n", False),
            ("sm.SemanticModel/definition/tables/dim_x.tmdl", "\n", False),
            ("rpt.Report/report.json", "\n", False),
            ("any/random/file.txt", "\n", False),
        ],
    )
    def test_rule_match(self, rel_path, expected_eol, expected_trailing):
        rule = rule_for(rel_path)
        assert rule.line_ending == expected_eol
        assert rule.trailing_newline is expected_trailing


# ── canonical_bytes ─────────────────────────────────────────────────────────


class TestCanonicalBytes:
    def test_lf_no_trailing_default(self):
        # A SemanticModel TMDL with CRLF + trailing LF should drop both.
        raw = b"model Model\r\n\tculture: en-US\r\n"
        out = canonical_bytes("sm.SemanticModel/definition/model.tmdl", raw)
        assert out == b"model Model\n\tculture: en-US"

    def test_notebook_lf_with_trailing(self):
        raw = b"# cell 1\r\nprint('hi')"  # missing trailing newline + CRLF
        out = canonical_bytes("nb.Notebook/notebook-content.py", raw)
        assert out == b"# cell 1\nprint('hi')\n"

    def test_yml_crlf_with_trailing(self):
        raw = b"key: value\nother: thing"  # LF + no trailing
        out = canonical_bytes("env.Environment/Setting/Sparkcompute.yml", raw)
        assert out == b"key: value\r\nother: thing\r\n"

    def test_alm_settings_crlf_no_trailing(self):
        raw = b'{"k": "v"}\r\n'
        out = canonical_bytes("lh.Lakehouse/alm.settings.json", raw)
        # CRLF preserved, trailing CRLF stripped
        assert out == b'{"k": "v"}'

    def test_strips_bom(self):
        raw = BOM + b'{"a": 1}'
        out = canonical_bytes("sm.SemanticModel/.platform", raw)
        assert out == b'{"a": 1}'
        assert not out.startswith(BOM)

    def test_idempotent(self):
        raw = b"line one\nline two"
        once = canonical_bytes("sm.SemanticModel/definition/model.tmdl", raw)
        twice = canonical_bytes("sm.SemanticModel/definition/model.tmdl", once)
        assert once == twice

    def test_binary_passthrough(self):
        # PNG-ish bytes that aren't valid UTF-8
        raw = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + bytes(range(255))
        out = canonical_bytes("rpt.Report/StaticResources/img.png", raw)
        assert out == raw

    def test_handles_mixed_line_endings(self):
        # \r\n and \n in the same file
        raw = b"a\r\nb\nc\r\n"
        out = canonical_bytes("sm.SemanticModel/definition/model.tmdl", raw)
        assert out == b"a\nb\nc"


# ── write_artifact_file ────────────────────────────────────────────────────


class TestWriteArtifactFile:
    def test_writes_with_workspace_root(self, tmp_path: Path):
        ws = tmp_path / "ws"
        nb = ws / "x.Notebook" / "notebook-content.py"
        write_artifact_file(nb, "# header\nprint('x')", workspace_root=ws)
        # Expect LF line endings + trailing LF
        assert nb.read_bytes() == b"# header\nprint('x')\n"

    def test_writes_without_workspace_root_uses_ancestor(self, tmp_path: Path):
        sm = tmp_path / "sm.SemanticModel" / "definition" / "model.tmdl"
        write_artifact_file(sm, "model M\r\n\tculture: en-US\r\n")
        # LF, no trailing newline
        assert sm.read_bytes() == b"model M\n\tculture: en-US"

    def test_creates_parent_dirs(self, tmp_path: Path):
        deep = tmp_path / "sm.SemanticModel" / "definition" / "tables" / "x.tmdl"
        assert not deep.parent.exists()
        write_artifact_file(deep, "table x")
        assert deep.exists()

    def test_accepts_bytes(self, tmp_path: Path):
        f = tmp_path / "sm.SemanticModel" / "definition" / "model.tmdl"
        write_artifact_file(f, b"model M\r\n")
        assert f.read_bytes() == b"model M"


# ── is_canonical ────────────────────────────────────────────────────────────


class TestIsCanonical:
    def test_canonical_file_returns_true(self, tmp_path: Path):
        f = tmp_path / "sm.SemanticModel" / "definition" / "model.tmdl"
        write_artifact_file(f, "model M")
        assert is_canonical(f) is True

    def test_drifted_file_returns_false(self, tmp_path: Path):
        sm_dir = tmp_path / "sm.SemanticModel" / "definition"
        sm_dir.mkdir(parents=True)
        f = sm_dir / "model.tmdl"
        # CRLF + trailing newline — wrong for TMDL
        f.write_bytes(b"model M\r\nculture: en-US\r\n")
        assert is_canonical(f) is False


# ── normalize_tree ──────────────────────────────────────────────────────────


class TestNormalizeTree:
    def test_fixes_drifted_files(self, tmp_path: Path):
        sm = tmp_path / "sm.SemanticModel"
        (sm / "definition").mkdir(parents=True)
        (sm / ".platform").write_bytes(b'{"k": "v"}\r\n')
        (sm / "definition" / "model.tmdl").write_bytes(b"model M\r\n")
        result = normalize_tree(tmp_path)
        assert len(result.changed) == 2
        assert (sm / ".platform").read_bytes() == b'{"k": "v"}'
        assert (sm / "definition" / "model.tmdl").read_bytes() == b"model M"

    def test_dry_run_does_not_write(self, tmp_path: Path):
        sm = tmp_path / "sm.SemanticModel" / "definition"
        sm.mkdir(parents=True)
        (sm / "model.tmdl").write_bytes(b"model M\r\n")
        result = normalize_tree(tmp_path, dry_run=True)
        assert len(result.changed) == 1
        # File still drifted because dry_run skipped the write
        assert (sm / "model.tmdl").read_bytes() == b"model M\r\n"
        assert result.dry_run is True

    def test_idempotent(self, tmp_path: Path):
        sm = tmp_path / "sm.SemanticModel" / "definition"
        sm.mkdir(parents=True)
        (sm / "model.tmdl").write_bytes(b"model M\r\n")
        normalize_tree(tmp_path)  # first pass — fixes
        result = normalize_tree(tmp_path)  # second pass — no changes
        assert result.is_canonical
        assert len(result.changed) == 0

    def test_skips_non_artifact_files(self, tmp_path: Path):
        # A README in the workspace root shouldn't be touched
        (tmp_path / "README.md").write_bytes(b"# title\r\n")
        result = normalize_tree(tmp_path)
        assert (tmp_path / "README.md").read_bytes() == b"# title\r\n"
        assert len(result.changed) == 0

    def test_extra_globs(self, tmp_path: Path):
        # Caller can register extra path patterns
        custom = tmp_path / "custom.dir" / "file.txt"
        custom.parent.mkdir()
        custom.write_bytes(b"hello\r\n")
        result = normalize_tree(tmp_path, extra_globs=["custom.dir/*.txt"])
        assert custom in result.changed
        assert custom.read_bytes() == b"hello"
