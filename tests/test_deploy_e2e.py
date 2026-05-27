"""Live REST-based E2E tests for :mod:`pyfabric.deploy`.

Exercises the full create-update-orphan-delete lifecycle against a real
workspace. Skipped when ``PYFABRIC_TEST_WORKSPACE_ID`` is unset.

Each test creates uniquely-named items and cleans up in a ``finally``
block so failure paths don't leak artifacts in the validation workspace.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import uuid
from pathlib import Path

import pytest

from pyfabric.client.http import FabricClient
from pyfabric.deploy import publish_repo, unpublish_orphans
from pyfabric.items.crud import delete_item, list_items
from pyfabric.items.notebook import NotebookBuilder


@pytest.mark.e2e
class TestDeployLifecycleE2E:
    @pytest.fixture(autouse=True)
    def _require_workspace(self) -> None:
        ws_id = os.environ.get("PYFABRIC_TEST_WORKSPACE_ID")
        if not ws_id:
            pytest.skip("PYFABRIC_TEST_WORKSPACE_ID not set")
        self.ws_id = ws_id
        self.client = FabricClient()

    def _build_repo(self, tmp_path: Path, names: list[str]) -> Path:
        for name in names:
            nb = NotebookBuilder().add_python(f'print("{name}")')
            nb.save_to_disk(tmp_path, display_name=name)
        return tmp_path

    def _cleanup_by_names(self, names: set[str]) -> None:
        """Best-effort delete of any leftover items by display name."""
        for item in list_items(self.client, self.ws_id):
            if item["displayName"] in names and item["type"] == "Notebook":
                with contextlib.suppress(Exception):
                    delete_item(self.client, self.ws_id, item["id"])

    def test_full_create_update_orphan_delete_lifecycle(self, tmp_path: Path) -> None:
        run_id = uuid.uuid4().hex[:8]
        keep_name = f"pyfabric_e2e_deploy_{run_id}_keep"
        orphan_name = f"pyfabric_e2e_deploy_{run_id}_orphan"
        all_names = {keep_name, orphan_name}

        try:
            # 1. Build local repo with two notebooks, publish both → CREATE
            self._build_repo(tmp_path, [keep_name, orphan_name])
            created = publish_repo(
                self.client,
                self.ws_id,
                tmp_path,
                item_types_in_scope=["Notebook"],
            )
            created_names = {r.display_name for r in created}
            assert created_names == all_names
            assert all(r.action == "created" for r in created), (
                f"expected all created on first publish, got: "
                f"{[(r.display_name, r.action) for r in created]}"
            )

            # 2. Re-publish without changing anything → UPDATE
            republished = publish_repo(
                self.client,
                self.ws_id,
                tmp_path,
                item_types_in_scope=["Notebook"],
            )
            assert all(r.action == "updated" for r in republished), (
                f"expected all updated on second publish, got: "
                f"{[(r.display_name, r.action) for r in republished]}"
            )

            # 3. Verify no duplicates — create-vs-update logic worked
            ws_items = list_items(self.client, self.ws_id, item_type="Notebook")
            matching = [i for i in ws_items if i["displayName"] in all_names]
            assert len(matching) == len(all_names), (
                f"expected exactly {len(all_names)} items, found {len(matching)}: "
                f"{[(i['displayName'], i['id']) for i in matching]}"
            )

            # 4. Remove one local artifact, run unpublish_orphans dry-run first
            shutil.rmtree(tmp_path / f"{orphan_name}.Notebook")
            dry = unpublish_orphans(
                self.client,
                self.ws_id,
                tmp_path,
                item_types_in_scope=["Notebook"],
                dry_run=True,
            )
            dry_orphans = {r.display_name for r in dry}
            assert orphan_name in dry_orphans
            assert keep_name not in dry_orphans
            # Confirm dry-run didn't actually delete
            ws_items_after_dry = list_items(
                self.client, self.ws_id, item_type="Notebook"
            )
            still_there = {i["displayName"] for i in ws_items_after_dry}
            assert orphan_name in still_there

            # 5. Actually unpublish orphans
            deleted = unpublish_orphans(
                self.client,
                self.ws_id,
                tmp_path,
                item_types_in_scope=["Notebook"],
            )
            deleted_names = {r.display_name for r in deleted}
            assert orphan_name in deleted_names
            assert keep_name not in deleted_names

            # 6. Verify the orphan is actually gone and the keeper survives
            ws_items_final = list_items(self.client, self.ws_id, item_type="Notebook")
            final_names = {i["displayName"] for i in ws_items_final}
            assert orphan_name not in final_names
            assert keep_name in final_names

        finally:
            # Best-effort cleanup so a failed assertion doesn't leak items
            self._cleanup_by_names(all_names)
