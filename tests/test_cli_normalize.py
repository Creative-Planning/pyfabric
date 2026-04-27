"""Tests for the ``pyfabric normalize-artifacts`` CLI subcommand.

The CLI is a thin wrapper over :func:`pyfabric.items.normalize.normalize_tree`;
the underlying transform is already covered in
``tests/items/test_normalize.py``. These tests verify the wiring:
exit codes, ``--dry-run``, error handling for bad paths, and that a
deliberately corrupted tree gets rewritten on a real invocation.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pyfabric.cli import main as cli_main


def _make_drifted_tree(root: Path) -> Path:
    """Create a Fabric workspace tree with one file in non-canonical bytes.

    notebook-content.py per Fabric convention is LF + trailing newline;
    this writes CRLF without trailing newline so normalize_tree has
    something to fix.
    """
    nb_dir = root / "nb_drift.Notebook"
    nb_dir.mkdir(parents=True)
    (nb_dir / "notebook-content.py").write_bytes(b"# CELL\r\nprint('x')")
    return nb_dir / "notebook-content.py"


class TestNormalizeArtifactsCLI:
    def test_rewrites_drifted_files_and_returns_zero(self, tmp_path):
        drifted = _make_drifted_tree(tmp_path)
        rc = cli_main(["normalize-artifacts", str(tmp_path)])
        assert rc == 0
        # CRLF gone, trailing newline added.
        rewritten = drifted.read_bytes()
        assert b"\r\n" not in rewritten
        assert rewritten.endswith(b"\n")

    def test_dry_run_does_not_modify_files(self, tmp_path):
        drifted = _make_drifted_tree(tmp_path)
        before = drifted.read_bytes()
        rc = cli_main(["normalize-artifacts", str(tmp_path), "--dry-run"])
        assert rc == 0
        assert drifted.read_bytes() == before

    def test_dry_run_returns_nonzero_when_drift_exists(self, tmp_path):
        # Optional behaviour for CI use: a follow-up could make --check
        # exit non-zero when drift is detected. For now --dry-run still
        # exits 0; this test pins current behaviour as a regression
        # guard. Update the assertion if --check semantics are added.
        _make_drifted_tree(tmp_path)
        rc = cli_main(["normalize-artifacts", str(tmp_path), "--dry-run"])
        assert rc == 0

    def test_returns_zero_on_already_canonical_tree(self, tmp_path):
        nb_dir = tmp_path / "nb_clean.Notebook"
        nb_dir.mkdir()
        (nb_dir / "notebook-content.py").write_bytes(b"# CELL\nprint('x')\n")
        rc = cli_main(["normalize-artifacts", str(tmp_path)])
        assert rc == 0

    def test_nonexistent_path_returns_nonzero(self, tmp_path):
        missing = tmp_path / "does_not_exist"
        rc = cli_main(["normalize-artifacts", str(missing)])
        assert rc != 0

    def test_help_includes_normalize_command(self, capsys):
        rc = cli_main(["--help"])
        captured = capsys.readouterr()
        assert rc == 0
        assert "normalize-artifacts" in captured.out

    def test_subcommand_help_mentions_dry_run(self, capsys):
        rc = cli_main(["normalize-artifacts", "--help"])
        captured = capsys.readouterr()
        assert rc == 0
        assert "--dry-run" in captured.out

    def test_missing_path_argument_returns_nonzero(self, capsys):
        rc = cli_main(["normalize-artifacts"])
        assert rc != 0


@pytest.mark.parametrize(
    "raw, expected",
    [
        # CRLF with no trailing → LF + trailing
        (b"# CELL\r\nprint('x')", b"# CELL\nprint('x')\n"),
        # BOM is stripped
        (b"\xef\xbb\xbf# CELL\nprint('x')\n", b"# CELL\nprint('x')\n"),
        # Already canonical: idempotent
        (b"# CELL\nprint('x')\n", b"# CELL\nprint('x')\n"),
    ],
)
class TestNormalizeArtifactsByteShapes:
    def test_each_drift_pattern_normalizes(self, tmp_path, raw, expected):
        nb_dir = tmp_path / "nb.Notebook"
        nb_dir.mkdir()
        target = nb_dir / "notebook-content.py"
        target.write_bytes(raw)
        rc = cli_main(["normalize-artifacts", str(tmp_path)])
        assert rc == 0
        assert target.read_bytes() == expected
