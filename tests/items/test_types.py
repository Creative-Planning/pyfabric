"""Tests for item type definitions and .platform validation."""

import json
import uuid

import pytest

from pyfabric.items.types import (
    ITEM_TYPES,
    parse_platform,
    resolve_logical_id,
)


class TestItemType:
    def test_all_known_types_registered(self):
        expected = {
            "Notebook",
            "Lakehouse",
            "Dataflow",
            "Environment",
            "VariableLibrary",
            "SemanticModel",
            "Report",
            "DataPipeline",
            "Warehouse",
            "MirroredDatabase",
            "Ontology",
            "Map",
            "DataAgent",
        }
        registered = {it.type_name for it in ITEM_TYPES.values()}
        assert expected.issubset(registered)

    def test_item_type_has_required_files(self):
        for name, it in ITEM_TYPES.items():
            assert isinstance(it.required_files, list), f"{name} missing required_files"

    def test_notebook_requires_content_file(self):
        nb = ITEM_TYPES["Notebook"]
        assert ["notebook-content.py"] in nb.alt_required_files
        assert ["notebook-content.sql"] in nb.alt_required_files

    def test_lakehouse_requires_metadata(self):
        lh = ITEM_TYPES["Lakehouse"]
        assert "lakehouse.metadata.json" in lh.required_files

    def test_environment_requires_sparkcompute(self):
        env = ITEM_TYPES["Environment"]
        assert "Setting/Sparkcompute.yml" in env.required_files
        assert "Libraries/PublicLibraries/environment.yml" in env.optional_files

    def test_variable_library_requires_core_files(self):
        vl = ITEM_TYPES["VariableLibrary"]
        assert "variables.json" in vl.required_files
        assert "settings.json" in vl.required_files

    def test_dataflow_requires_query_metadata(self):
        df = ITEM_TYPES["Dataflow"]
        assert "queryMetadata.json" in df.required_files
        assert "mashup.pq" in df.required_files

    def test_data_agent_requires_config(self):
        da = ITEM_TYPES["DataAgent"]
        assert "Files/Config/data_agent.json" in da.required_files
        assert "Files/Config/publish_info.json" in da.optional_files
        assert "Files/Config/draft/stage_config.json" in da.optional_files
        assert "Files/Config/published/stage_config.json" in da.optional_files

    def test_lookup_by_type_name(self):
        assert ITEM_TYPES["Notebook"].type_name == "Notebook"
        assert ITEM_TYPES["Lakehouse"].type_name == "Lakehouse"

    def test_lookup_unknown_type_raises(self):
        with pytest.raises(KeyError):
            _ = ITEM_TYPES["NonExistentType"]

    def test_item_type_has_dir_suffix(self):
        for name, it in ITEM_TYPES.items():
            assert it.dir_suffix == f".{name}"


class TestPlatformFile:
    def test_parse_valid_platform(self):
        data = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "Notebook",
                "displayName": "nb_test",
            },
            "config": {
                "version": "2.0",
                "logicalId": "12345678-1234-1234-1234-123456789012",
            },
        }
        pf = parse_platform(json.dumps(data))
        assert pf.metadata.type == "Notebook"
        assert pf.metadata.display_name == "nb_test"
        assert pf.config.version == "2.0"
        assert pf.config.logical_id == "12345678-1234-1234-1234-123456789012"

    def test_parse_platform_with_description(self):
        data = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "Dataflow",
                "displayName": "df_test",
                "description": "A test dataflow",
            },
            "config": {
                "version": "2.0",
                "logicalId": str(uuid.uuid4()),
            },
        }
        pf = parse_platform(json.dumps(data))
        assert pf.metadata.description == "A test dataflow"

    def test_parse_platform_missing_metadata_raises(self):
        data = {
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
        }
        with pytest.raises(ValueError, match="metadata"):
            parse_platform(json.dumps(data))

    def test_parse_platform_missing_type_raises(self):
        data = {
            "metadata": {"displayName": "test"},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
        }
        with pytest.raises(ValueError, match="type"):
            parse_platform(json.dumps(data))

    def test_parse_platform_missing_display_name_raises(self):
        data = {
            "metadata": {"type": "Notebook"},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
        }
        with pytest.raises(ValueError, match="displayName"):
            parse_platform(json.dumps(data))

    def test_parse_platform_missing_config_raises(self):
        data = {
            "metadata": {"type": "Notebook", "displayName": "test"},
        }
        with pytest.raises(ValueError, match="config"):
            parse_platform(json.dumps(data))

    def test_parse_platform_missing_logical_id_raises(self):
        data = {
            "metadata": {"type": "Notebook", "displayName": "test"},
            "config": {"version": "2.0"},
        }
        with pytest.raises(ValueError, match="logicalId"):
            parse_platform(json.dumps(data))

    def test_parse_platform_invalid_json_raises(self):
        with pytest.raises(ValueError, match="Invalid JSON"):
            parse_platform("not json {{{")

    def test_platform_dir_name(self):
        data = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "Notebook", "displayName": "nb_test"},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
        }
        pf = parse_platform(json.dumps(data))
        assert pf.expected_dir_name == "nb_test.Notebook"


class TestResolveLogicalId:
    """resolve_logical_id: the identity-stability contract for rebuilds."""

    @staticmethod
    def _write_platform(item_dir, logical_id):
        item_dir.mkdir(parents=True, exist_ok=True)
        (item_dir / ".platform").write_text(
            json.dumps(
                {
                    "metadata": {"type": "Notebook", "displayName": "nb"},
                    "config": {"version": "2.0", "logicalId": logical_id},
                }
            ),
            encoding="utf-8",
        )

    def test_reuses_existing_platform_id_when_not_pinned(self, tmp_path):
        existing = str(uuid.uuid4())
        self._write_platform(tmp_path / "nb.Notebook", existing)
        assert resolve_logical_id(tmp_path / "nb.Notebook", None) == existing

    def test_mints_fresh_uuid_for_new_directory(self, tmp_path):
        resolved = resolve_logical_id(tmp_path / "nb.Notebook", None)
        uuid.UUID(resolved)  # raises if not a valid uuid

    def test_explicit_id_wins_over_existing(self, tmp_path):
        self._write_platform(tmp_path / "nb.Notebook", str(uuid.uuid4()))
        explicit = str(uuid.uuid4())
        assert resolve_logical_id(tmp_path / "nb.Notebook", explicit) == explicit

    def test_explicit_id_used_for_new_directory(self, tmp_path):
        explicit = str(uuid.uuid4())
        assert resolve_logical_id(tmp_path / "nb.Notebook", explicit) == explicit

    def test_corrupt_platform_falls_back_to_fresh_uuid(self, tmp_path):
        item_dir = tmp_path / "nb.Notebook"
        item_dir.mkdir(parents=True)
        (item_dir / ".platform").write_text("{not json", encoding="utf-8")
        resolved = resolve_logical_id(item_dir, None)
        uuid.UUID(resolved)

    def test_platform_missing_logical_id_falls_back(self, tmp_path):
        item_dir = tmp_path / "nb.Notebook"
        item_dir.mkdir(parents=True)
        (item_dir / ".platform").write_text(
            json.dumps({"metadata": {"type": "Notebook", "displayName": "nb"}}),
            encoding="utf-8",
        )
        resolved = resolve_logical_id(item_dir, None)
        uuid.UUID(resolved)
