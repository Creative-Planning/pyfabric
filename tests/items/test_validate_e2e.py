"""E2E validation tests against fixture and real workspaces."""

from pathlib import Path

import pytest

from pyfabric.items.validate import validate_item, validate_workspace


class TestFixtureWorkspace:
    """Validate the synthetic fixture workspace (always runs in CI)."""

    def test_all_valid_items_pass(self, fixture_workspace: Path):
        results = validate_workspace(fixture_workspace)
        assert len(results) >= 9  # one per item type
        for r in results:
            assert r.valid, f"{r.item_path.name}: {[e.message for e in r.errors]}"

    def test_each_item_type_represented(self, fixture_workspace: Path):
        results = validate_workspace(fixture_workspace)
        types = {r.item_type for r in results}
        expected = {
            "Notebook",
            "Lakehouse",
            "Dataflow",
            "Environment",
            "VariableLibrary",
            "SemanticModel",
            "Report",
            "Pipeline",
            "Warehouse",
        }
        assert expected == types


class TestInvalidFixtureWorkspace:
    """Validate the invalid fixture workspace catches errors."""

    def test_missing_content_detected(self, fixture_workspace_invalid: Path):
        item_dir = fixture_workspace_invalid / "nb_no_content.Notebook"
        result = validate_item(item_dir)
        assert not result.valid
        assert any("notebook-content.py" in e.message for e in result.errors)

    def test_bad_platform_detected(self, fixture_workspace_invalid: Path):
        item_dir = fixture_workspace_invalid / "bad_platform.Notebook"
        result = validate_item(item_dir)
        assert not result.valid
        assert any("JSON" in e.message or "Invalid" in e.message for e in result.errors)

    def test_wrong_name_warns(self, fixture_workspace_invalid: Path):
        item_dir = fixture_workspace_invalid / "wrong_name.Lakehouse"
        result = validate_item(item_dir)
        assert any("mismatch" in w.message.lower() for w in result.warnings)


@pytest.mark.e2e
class TestRealWorkspace:
    """Validate a real Fabric workspace (local only, skipped in CI)."""

    @pytest.fixture(autouse=True)
    def _require_workspace(self, real_workspace):
        if real_workspace is None:
            pytest.skip("PYFABRIC_TEST_WORKSPACE not set")
        self.ws_path = real_workspace

    def test_all_items_validate(self):
        results = validate_workspace(self.ws_path)
        assert len(results) > 0, "No items found in workspace"
        for r in results:
            if not r.valid:
                errors = [e.message for e in r.errors]
                pytest.fail(f"{r.item_path.name}: {errors}")
