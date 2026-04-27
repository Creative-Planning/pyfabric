"""Tests for :mod:`pyfabric.items.notebook` (NotebookBuilder).

The core correctness bar is **byte-equality with Fabric-emitted notebooks**.
Every Fabric git-sync cycle rewrites ``notebook-content.py`` to a strict
canonical form, so a builder that gets the cell-marker layout even
slightly wrong produces files that flap on every sync.

The tests below build two configurations that mirror real Fabric output
(``nb_minimal`` — header only, no cells; ``nb_full`` — lakehouse
dependency + markdown + ``%pip`` cell + python cell) and assert the
builder's output is byte-identical to a checked-in fixture.

Lakehouse/workspace IDs in the fixtures are synthetic
(``00000000-...-00000000000N``) — no client identifiers leak in.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pyfabric.items.notebook import NotebookBuilder

FIXTURES = Path(__file__).parent.parent / "fixtures" / "notebooks"


def _fixture_bytes(name: str) -> bytes:
    return (FIXTURES / f"{name}.Notebook" / "notebook-content.py").read_bytes()


# ── Empty / minimal builder ──────────────────────────────────────────────────


class TestMinimal:
    def test_empty_builder_produces_header_only_source(self):
        nb = NotebookBuilder()
        expected = _fixture_bytes("nb_minimal")
        assert nb.to_source_string().encode("utf-8") == expected

    def test_default_kernel_is_synapse_pyspark(self):
        nb = NotebookBuilder()
        src = nb.to_source_string()
        assert '"name": "synapse_pyspark"' in src

    def test_custom_kernel_propagates_into_header(self):
        nb = NotebookBuilder(kernel="jupyter")
        src = nb.to_source_string()
        assert '"name": "jupyter"' in src


# ── Full round-trip ──────────────────────────────────────────────────────────


class TestFullRoundTrip:
    def _build_full(self) -> NotebookBuilder:
        nb = NotebookBuilder()
        nb.attach_lakehouse(
            ws_id="00000000-0000-0000-0000-0000000000aa",
            lh_id="00000000-0000-0000-0000-000000000001",
            lh_name="lh_primary",
            default=True,
        )
        nb.attach_lakehouse(
            ws_id="00000000-0000-0000-0000-0000000000aa",
            lh_id="00000000-0000-0000-0000-000000000002",
        )
        nb.add_markdown("# Example notebook\nTwo markdown lines.")
        nb.pip_install_from_resources("example_pkg-0.1.0-py3-none-any.whl")
        nb.add_python('print("hello")\nx = 1 + 2')
        return nb

    def test_full_builder_produces_byte_identical_source(self):
        nb = self._build_full()
        assert nb.to_source_string().encode("utf-8") == _fixture_bytes("nb_full")

    def test_builder_methods_are_chainable(self):
        nb = (
            NotebookBuilder()
            .attach_lakehouse("w", "l", default=True)
            .add_markdown("# x")
            .add_python("pass")
        )
        assert isinstance(nb, NotebookBuilder)


# ── Per-cell shape ──────────────────────────────────────────────────────────


class TestMarkdownCell:
    def test_prefixes_every_line_with_hash_space(self):
        nb = NotebookBuilder().add_markdown("Line 1\nLine 2")
        src = nb.to_source_string()
        assert "# MARKDOWN ********************\n\n# Line 1\n# Line 2\n" in src

    def test_blank_line_is_rendered_as_hash_only(self):
        nb = NotebookBuilder().add_markdown("Para one\n\nPara two")
        src = nb.to_source_string()
        # Fabric emits "# " with trailing space stripped — rendered as "#".
        assert "# Para one\n#\n# Para two" in src


class TestPythonCell:
    def test_contains_cell_marker_code_and_trailing_metadata(self):
        nb = NotebookBuilder().add_python('print("hello")')
        src = nb.to_source_string()
        assert '# CELL ********************\n\nprint("hello")\n\n' in src
        assert '"language": "python"' in src

    def test_trailing_metadata_uses_language_group_from_kernel(self):
        nb = NotebookBuilder().add_python("pass")
        src = nb.to_source_string()
        assert '"language_group": "synapse_pyspark"' in src


class TestPipInstallFromResources:
    def test_emits_quoted_builtin_path_and_quiet_flag(self):
        nb = NotebookBuilder().pip_install_from_resources(
            "my_pkg-1.0.0-py3-none-any.whl"
        )
        src = nb.to_source_string()
        assert '%pip install "builtin/my_pkg-1.0.0-py3-none-any.whl" --quiet' in src


# ── attach_lakehouse ────────────────────────────────────────────────────────


class TestAttachLakehouse:
    def test_single_default_lakehouse_populates_default_fields(self):
        nb = NotebookBuilder().attach_lakehouse(
            ws_id="ws-1",
            lh_id="lh-1",
            lh_name="my_lh",
            default=True,
        )
        src = nb.to_source_string()
        assert '"default_lakehouse": "lh-1"' in src
        assert '"default_lakehouse_name": "my_lh"' in src
        assert '"default_lakehouse_workspace_id": "ws-1"' in src

    def test_multiple_lakehouses_listed_in_known_lakehouses(self):
        nb = (
            NotebookBuilder()
            .attach_lakehouse("ws-1", "lh-1", default=True)
            .attach_lakehouse("ws-1", "lh-2")
        )
        src = nb.to_source_string()
        assert '"id": "lh-1"' in src
        assert '"id": "lh-2"' in src

    def test_no_lakehouse_means_no_dependencies_block(self):
        src = NotebookBuilder().to_source_string()
        assert "dependencies" not in src

    def test_second_default_raises(self):
        nb = NotebookBuilder().attach_lakehouse("w", "a", default=True)
        with pytest.raises(ValueError, match="default"):
            nb.attach_lakehouse("w", "b", default=True)


# ── Bundle + disk integration ───────────────────────────────────────────────


class TestToBundle:
    def test_notebook_content_is_stored_as_canonical_bytes(self, tmp_path):
        """Stored as bytes so bundle.save_to_disk takes the write_bytes
        branch — which preserves LF on Windows. (String content goes
        through write_text which injects CRLF on Windows.)"""
        nb = NotebookBuilder().add_python("pass")
        bundle = nb.to_bundle(display_name="nb_test")
        content = bundle.parts["notebook-content.py"]
        assert isinstance(content, bytes)
        # Canonical: LF only, ends with trailing newline.
        assert b"\r\n" not in content
        assert content.endswith(b"\n")

    def test_bundle_has_notebook_item_type_and_display_name(self):
        bundle = NotebookBuilder().to_bundle(display_name="nb_foo")
        assert bundle.item_type == "Notebook"
        assert bundle.display_name == "nb_foo"
        assert bundle.dir_name == "nb_foo.Notebook"

    def test_logical_id_override(self):
        bundle = NotebookBuilder().to_bundle(
            display_name="nb_x",
            logical_id="11111111-1111-1111-1111-111111111111",
        )
        assert bundle.logical_id == "11111111-1111-1111-1111-111111111111"


class TestSaveToDisk:
    def test_writes_notebook_content_with_lf_and_trailing_newline(self, tmp_path):
        """Regression coverage for Windows: direct ``NotebookBuilder.save_to_disk``
        routes through :func:`write_artifact_file` which enforces LF +
        trailing newline regardless of OS default line endings."""
        nb = NotebookBuilder().add_python("pass")
        artifact_dir = nb.save_to_disk(tmp_path, display_name="nb_test")
        content_path = artifact_dir / "notebook-content.py"
        raw = content_path.read_bytes()
        assert b"\r\n" not in raw
        assert raw.endswith(b"\n")

    def test_writes_platform_json_with_lf_and_no_trailing_newline(self, tmp_path):
        """``.platform`` is LF + no trailing newline per Fabric convention."""
        nb = NotebookBuilder()
        artifact_dir = nb.save_to_disk(tmp_path, display_name="nb_test")
        raw = (artifact_dir / ".platform").read_bytes()
        assert b"\r\n" not in raw
        assert not raw.endswith(b"\n")

    def test_returns_artifact_directory_path(self, tmp_path):
        nb = NotebookBuilder()
        artifact_dir = nb.save_to_disk(tmp_path, display_name="nb_test")
        assert artifact_dir == tmp_path / "nb_test.Notebook"
        assert artifact_dir.is_dir()
