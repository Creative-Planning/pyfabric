"""Pairwise validation tests using allpairspy for parameter space coverage."""

import json
import uuid
from pathlib import Path

from allpairspy import AllPairs

from pyfabric.items.types import ITEM_TYPES
from pyfabric.items.validate import validate_item


def _make_platform(item_type: str, display_name: str) -> str:
    return json.dumps(
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": item_type, "displayName": display_name},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
        }
    )


def _minimal_files(item_type: str) -> dict[str, str]:
    """Return minimal required files for an item type (excluding .platform)."""
    it = ITEM_TYPES.get(item_type)
    if not it:
        return {}
    return {f: f"placeholder content for {f}" for f in it.required_files}


class TestPairwiseValidation:
    """Generate pairwise test combinations across item types and error conditions."""

    def test_valid_items_for_all_types(self, tmp_path: Path):
        """Every known item type with all required files should validate."""
        for type_name, item_type in ITEM_TYPES.items():
            item_dir = tmp_path / f"test_{type_name.lower()}.{type_name}"
            item_dir.mkdir(parents=True, exist_ok=True)
            (item_dir / ".platform").write_text(
                _make_platform(type_name, f"test_{type_name.lower()}"), encoding="utf-8"
            )
            for rel_path in item_type.required_files:
                p = item_dir / rel_path
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(f"content for {rel_path}", encoding="utf-8")

            result = validate_item(item_dir)
            assert result.valid, f"{type_name}: {[e.message for e in result.errors]}"

    def test_pairwise_missing_files(self, tmp_path: Path):
        """For each item type, test that each missing required file is detected."""
        types_with_required = [
            (name, it) for name, it in ITEM_TYPES.items() if it.required_files
        ]

        for type_name, item_type in types_with_required:
            for missing_file in item_type.required_files:
                # Create item with all files except one
                item_dir = (
                    tmp_path
                    / f"miss_{type_name}_{missing_file.replace('/', '_')}.{type_name}"
                )
                item_dir.mkdir(parents=True, exist_ok=True)
                (item_dir / ".platform").write_text(
                    _make_platform(type_name, item_dir.name.rsplit(".", 1)[0]),
                    encoding="utf-8",
                )
                for rf in item_type.required_files:
                    if rf != missing_file:
                        p = item_dir / rf
                        p.parent.mkdir(parents=True, exist_ok=True)
                        p.write_text("content", encoding="utf-8")

                result = validate_item(item_dir)
                assert not result.valid, (
                    f"{type_name} should fail when {missing_file} is missing"
                )
                assert any(missing_file in e.message for e in result.errors)

    def test_pairwise_platform_field_errors(self, tmp_path: Path):
        """Use allpairspy to generate combinations of invalid .platform fields."""
        error_scenarios = [
            (
                "no_metadata",
                '{"config": {"version": "2.0", "logicalId": "'
                + str(uuid.uuid4())
                + '"}}',
            ),
            (
                "no_type",
                '{"metadata": {"displayName": "x"}, "config": {"version": "2.0", "logicalId": "'
                + str(uuid.uuid4())
                + '"}}',
            ),
            (
                "no_display_name",
                '{"metadata": {"type": "Notebook"}, "config": {"version": "2.0", "logicalId": "'
                + str(uuid.uuid4())
                + '"}}',
            ),
            ("no_config", '{"metadata": {"type": "Notebook", "displayName": "x"}}'),
            (
                "no_logical_id",
                '{"metadata": {"type": "Notebook", "displayName": "x"}, "config": {"version": "2.0"}}',
            ),
            ("bad_json", "{ not valid json !!!"),
        ]
        item_types_to_test = ["Notebook", "Lakehouse", "Environment"]

        for combo in AllPairs([error_scenarios, item_types_to_test]):
            (scenario_name, platform_content), item_type = combo
            item_dir = tmp_path / f"err_{scenario_name}_{item_type}.{item_type}"
            item_dir.mkdir(parents=True, exist_ok=True)
            (item_dir / ".platform").write_text(platform_content, encoding="utf-8")
            # Add required files so we isolate .platform errors
            for rf in ITEM_TYPES.get(item_type, ITEM_TYPES["Notebook"]).required_files:
                p = item_dir / rf
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text("content", encoding="utf-8")

            result = validate_item(item_dir)
            assert not result.valid, f"{scenario_name}/{item_type} should fail"
