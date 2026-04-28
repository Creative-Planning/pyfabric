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


class TestNotebookSettingsJson:
    """Fabric requires ``notebook-settings.json`` with
    ``{"includeResourcesInGit": "on"}`` to include the
    ``Resources/`` subtree in git-sync. Without it, wheels under
    ``Resources/builtin/`` are silently excluded from the
    workspace item, so a `%pip install "builtin/..."` cell in
    ``notebook-content.py`` fails at runtime.

    ``save_to_disk`` and ``to_bundle`` must emit the file
    unconditionally — a no-op when ``Resources/`` is empty, but
    necessary the moment a project ships any wheel via
    ``pip_install_from_resources``.
    """

    def test_save_to_disk_emits_notebook_settings_json(self, tmp_path):
        nb = NotebookBuilder().pip_install_from_resources(
            "my_project-0.1.0-py3-none-any.whl"
        )
        artifact_dir = nb.save_to_disk(tmp_path, display_name="nb_x")
        settings_path = artifact_dir / "notebook-settings.json"
        assert settings_path.is_file()
        import json

        body = json.loads(settings_path.read_text(encoding="utf-8"))
        assert body == {"includeResourcesInGit": "on"}

    def test_save_to_disk_emits_settings_even_without_resources(self, tmp_path):
        """Always-on guarantees: a notebook that doesn't currently use
        Resources/ but later grows one stays correct without rewriting
        save_to_disk callers."""
        nb = NotebookBuilder()
        artifact_dir = nb.save_to_disk(tmp_path, display_name="nb_y")
        assert (artifact_dir / "notebook-settings.json").is_file()

    def test_notebook_settings_uses_lf_no_trailing_newline(self, tmp_path):
        """notebook-settings.json follows the standard "JSON config"
        convention: LF, no trailing newline (same as .platform)."""
        nb = NotebookBuilder()
        artifact_dir = nb.save_to_disk(tmp_path, display_name="nb_z")
        raw = (artifact_dir / "notebook-settings.json").read_bytes()
        assert b"\r\n" not in raw
        assert not raw.endswith(b"\n")

    def test_to_bundle_includes_notebook_settings_part(self):
        nb = NotebookBuilder()
        bundle = nb.to_bundle(display_name="nb_b")
        assert "notebook-settings.json" in bundle.parts


class TestAttachEnvironment:
    """``attach_environment`` adds a Fabric environment dependency
    to the notebook header METADATA block. The environment block
    accompanies any ``lakehouse`` block under ``dependencies``.

    Workspace ID convention: when the environment lives in the
    same workspace as the notebook (the common case), Fabric uses
    an all-zeros ``workspaceId`` instead of the actual workspace
    GUID. ``ws_id=None`` selects this default.
    """

    _ZERO_WS = "00000000-0000-0000-0000-000000000000"

    def test_default_ws_id_is_all_zeros(self):
        nb = NotebookBuilder().attach_environment("env-logical-1")
        src = nb.to_source_string()
        assert '"environment"' in src
        assert '"environmentId": "env-logical-1"' in src
        assert f'"workspaceId": "{self._ZERO_WS}"' in src

    def test_explicit_ws_id_is_emitted(self):
        nb = NotebookBuilder().attach_environment(
            "env-logical-2", ws_id="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
        )
        src = nb.to_source_string()
        assert '"environmentId": "env-logical-2"' in src
        assert '"workspaceId": "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"' in src

    def test_environment_block_lives_under_dependencies(self):
        nb = NotebookBuilder().attach_environment("env-1")
        src = nb.to_source_string()
        # The environment block must be inside the dependencies object,
        # not a sibling of kernel_info.
        deps_idx = src.index('"dependencies"')
        env_idx = src.index('"environment"')
        kernel_idx = src.index('"kernel_info"')
        assert kernel_idx < deps_idx < env_idx

    def test_environment_alongside_lakehouse(self):
        nb = (
            NotebookBuilder()
            .attach_lakehouse(
                ws_id="ws-1",
                lh_id="lh-1",
                lh_name="lh_primary",
                default=True,
            )
            .attach_environment("env-1")
        )
        src = nb.to_source_string()
        assert '"lakehouse"' in src
        assert '"environment"' in src
        assert '"environmentId": "env-1"' in src

    def test_chainable(self):
        nb = NotebookBuilder()
        result = nb.attach_environment("env-1")
        assert result is nb

    def test_second_call_replaces_first(self):
        """Only one environment can be attached at a time. A second
        call replaces the first rather than producing two
        ``environment`` keys (which Fabric would silently reduce to
        the last one anyway, so the explicit replace is more
        honest)."""
        nb = (
            NotebookBuilder()
            .attach_environment("env-old")
            .attach_environment("env-new")
        )
        src = nb.to_source_string()
        assert '"environmentId": "env-new"' in src
        assert "env-old" not in src
