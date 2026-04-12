"""Tests for item structure validation."""

import json
import uuid
from pathlib import Path

from pyfabric.items.validate import (
    validate_item,
    validate_workspace,
)


def _make_platform(item_type: str, display_name: str) -> str:
    return json.dumps(
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": item_type, "displayName": display_name},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
        }
    )


def _create_item(
    base: Path, display_name: str, item_type: str, files: dict[str, str]
) -> Path:
    """Helper to create a Fabric item directory with files."""
    item_dir = base / f"{display_name}.{item_type}"
    item_dir.mkdir(parents=True, exist_ok=True)
    (item_dir / ".platform").write_text(
        _make_platform(item_type, display_name), encoding="utf-8"
    )
    for rel_path, content in files.items():
        p = item_dir / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    return item_dir


class TestValidateItem:
    def test_valid_notebook(self, tmp_path: Path):
        item_dir = _create_item(
            tmp_path,
            "nb_test",
            "Notebook",
            {
                "notebook-content.py": "# Fabric notebook source\n",
            },
        )
        result = validate_item(item_dir)
        assert result.valid
        assert result.errors == []

    def test_notebook_missing_content_file(self, tmp_path: Path):
        item_dir = tmp_path / "nb_test.Notebook"
        item_dir.mkdir()
        (item_dir / ".platform").write_text(
            _make_platform("Notebook", "nb_test"), encoding="utf-8"
        )
        result = validate_item(item_dir)
        assert not result.valid
        assert any("notebook-content.py" in e.message for e in result.errors)

    def test_valid_lakehouse(self, tmp_path: Path):
        item_dir = _create_item(
            tmp_path,
            "lh_test",
            "Lakehouse",
            {
                "lakehouse.metadata.json": '{"defaultSchema":"dbo"}',
            },
        )
        result = validate_item(item_dir)
        assert result.valid

    def test_valid_environment(self, tmp_path: Path):
        item_dir = _create_item(
            tmp_path,
            "env_test",
            "Environment",
            {
                "Libraries/PublicLibraries/environment.yml": "dependencies:\n  - pip:\n      - requests\n",
                "Setting/Sparkcompute.yml": "runtime_version: 1.3\n",
            },
        )
        result = validate_item(item_dir)
        assert result.valid

    def test_valid_environment_custom_libs_only(self, tmp_path: Path):
        """Environment with custom libraries but no environment.yml is valid."""
        item_dir = _create_item(
            tmp_path,
            "env_custom",
            "Environment",
            {
                "Setting/Sparkcompute.yml": "runtime_version: 1.3\n",
            },
        )
        # Add a custom library file (not in required/optional, just present)
        whl_dir = item_dir / "Libraries" / "CustomLibraries"
        whl_dir.mkdir(parents=True)
        (whl_dir / "mylib-0.1.0-py3-none-any.whl").write_bytes(b"fake whl")
        result = validate_item(item_dir)
        assert result.valid

    def test_environment_missing_sparkcompute(self, tmp_path: Path):
        item_dir = _create_item(
            tmp_path,
            "env_test",
            "Environment",
            {
                "Libraries/PublicLibraries/environment.yml": "dependencies: []\n",
            },
        )
        result = validate_item(item_dir)
        assert not result.valid
        assert any("Sparkcompute.yml" in e.message for e in result.errors)

    def test_valid_variable_library(self, tmp_path: Path):
        item_dir = _create_item(
            tmp_path,
            "vl_test",
            "VariableLibrary",
            {
                "variables.json": '{"variables": []}',
                "settings.json": '{"valueSetsOrder": ["UAT"]}',
                "valueSets/UAT.json": '{"name": "UAT", "variableOverrides": []}',
            },
        )
        result = validate_item(item_dir)
        assert result.valid

    def test_variable_library_missing_settings(self, tmp_path: Path):
        item_dir = _create_item(
            tmp_path,
            "vl_test",
            "VariableLibrary",
            {
                "variables.json": '{"variables": []}',
            },
        )
        result = validate_item(item_dir)
        assert not result.valid
        assert any("settings.json" in e.message for e in result.errors)

    def test_valid_dataflow(self, tmp_path: Path):
        item_dir = _create_item(
            tmp_path,
            "df_test",
            "Dataflow",
            {
                "queryMetadata.json": '{"formatVersion": "202502"}',
                "mashup.pq": "binary content placeholder",
            },
        )
        result = validate_item(item_dir)
        assert result.valid

    def test_missing_platform_file(self, tmp_path: Path):
        item_dir = tmp_path / "nb_test.Notebook"
        item_dir.mkdir()
        (item_dir / "notebook-content.py").write_text("# code\n", encoding="utf-8")
        result = validate_item(item_dir)
        assert not result.valid
        assert any(".platform" in e.message for e in result.errors)

    def test_invalid_platform_json(self, tmp_path: Path):
        item_dir = tmp_path / "nb_test.Notebook"
        item_dir.mkdir()
        (item_dir / ".platform").write_text("not json", encoding="utf-8")
        (item_dir / "notebook-content.py").write_text("# code\n", encoding="utf-8")
        result = validate_item(item_dir)
        assert not result.valid
        assert any("Invalid" in e.message or "JSON" in e.message for e in result.errors)

    def test_dir_name_mismatch_warns(self, tmp_path: Path):
        """Dir name doesn't match .platform displayName — should produce a warning."""
        item_dir = tmp_path / "wrong_name.Notebook"
        item_dir.mkdir()
        (item_dir / ".platform").write_text(
            _make_platform("Notebook", "nb_correct_name"), encoding="utf-8"
        )
        (item_dir / "notebook-content.py").write_text("# code\n", encoding="utf-8")
        result = validate_item(item_dir)
        assert any("mismatch" in w.message.lower() for w in result.warnings)

    def test_unknown_item_type_produces_error(self, tmp_path: Path):
        item_dir = tmp_path / "test.FakeType"
        item_dir.mkdir()
        (item_dir / ".platform").write_text(
            _make_platform("FakeType", "test"), encoding="utf-8"
        )
        result = validate_item(item_dir)
        assert not result.valid
        assert any("Unknown item type" in e.message for e in result.errors)

    def test_result_item_path(self, tmp_path: Path):
        item_dir = _create_item(
            tmp_path,
            "nb_test",
            "Notebook",
            {
                "notebook-content.py": "# code\n",
            },
        )
        result = validate_item(item_dir)
        assert result.item_path == item_dir

    def test_result_item_type(self, tmp_path: Path):
        item_dir = _create_item(
            tmp_path,
            "nb_test",
            "Notebook",
            {
                "notebook-content.py": "# code\n",
            },
        )
        result = validate_item(item_dir)
        assert result.item_type == "Notebook"


class TestValidateWorkspace:
    def test_valid_workspace_with_multiple_items(self, tmp_path: Path):
        _create_item(
            tmp_path,
            "nb_one",
            "Notebook",
            {
                "notebook-content.py": "# code\n",
            },
        )
        _create_item(
            tmp_path,
            "lh_one",
            "Lakehouse",
            {
                "lakehouse.metadata.json": '{"defaultSchema":"dbo"}',
            },
        )
        results = validate_workspace(tmp_path)
        assert len(results) == 2
        assert all(r.valid for r in results)

    def test_workspace_with_invalid_item(self, tmp_path: Path):
        _create_item(
            tmp_path,
            "nb_good",
            "Notebook",
            {
                "notebook-content.py": "# code\n",
            },
        )
        # Bad notebook — missing content file
        bad_dir = tmp_path / "nb_bad.Notebook"
        bad_dir.mkdir()
        (bad_dir / ".platform").write_text(
            _make_platform("Notebook", "nb_bad"), encoding="utf-8"
        )
        results = validate_workspace(tmp_path)
        assert len(results) == 2
        valid_count = sum(1 for r in results if r.valid)
        assert valid_count == 1

    def test_empty_workspace(self, tmp_path: Path):
        results = validate_workspace(tmp_path)
        assert results == []

    def test_ignores_non_item_directories(self, tmp_path: Path):
        """Directories without .ItemType suffix should be ignored."""
        (tmp_path / "scripts").mkdir()
        (tmp_path / ".git").mkdir()
        (tmp_path / "README.md").write_text("# readme\n", encoding="utf-8")
        _create_item(
            tmp_path,
            "nb_test",
            "Notebook",
            {
                "notebook-content.py": "# code\n",
            },
        )
        results = validate_workspace(tmp_path)
        assert len(results) == 1
