"""Live REST-based E2E tests for :mod:`pyfabric.items.environment`.

Skipped when ``PYFABRIC_TEST_WORKSPACE_ID`` is unset, so CI and
contributors without a validation workspace still get a green run.

Each test creates a uniquely-named Environment item in the target
workspace, exercises the REST helpers, and cleans up in a ``finally``
block.

Heavy operations like a real ``publish_environment`` + full
``wait_for_published`` cycle (~5 min) are intentionally NOT exercised
here — the mocked tests in :mod:`test_environment` cover the polling
state machine. This file is for the shape-of-response and item
acceptance guarantees that only a real Fabric workspace can verify.
"""

from __future__ import annotations

import os
import uuid

import pytest

from pyfabric.client.http import FabricClient
from pyfabric.items.bundle import upload_to_workspace
from pyfabric.items.crud import delete_item
from pyfabric.items.environment import EnvironmentBuilder, get_environment_status


def _workspace_id() -> str | None:
    return os.environ.get("PYFABRIC_TEST_WORKSPACE_ID") or None


@pytest.mark.e2e
class TestEnvironmentLifecycleE2E:
    @pytest.fixture(autouse=True)
    def _require_workspace(self) -> None:
        ws_id = _workspace_id()
        if ws_id is None:
            pytest.skip("PYFABRIC_TEST_WORKSPACE_ID not set")
        self.ws_id = ws_id
        self.client = FabricClient()

    def test_environment_item_accepted_by_fabric(self) -> None:
        # No pip pins → no Spark package install → cleanup is fast.
        # Default compute settings match Fabric's portal-created shape.
        # We deliberately do NOT trigger publish_environment here — a
        # full publish + wait cycle is ~5min, and the mocked tests in
        # test_environment cover the state-machine of wait_for_published.
        display_name = f"pyfabric_e2e_env_{uuid.uuid4().hex[:8]}"
        bundle = EnvironmentBuilder().to_bundle(display_name=display_name)

        created = upload_to_workspace(bundle, self.client, self.ws_id)
        item_id = created.get("id")
        assert item_id, f"create_item returned no id: {created!r}"

        try:
            status = get_environment_status(self.client, self.ws_id, item_id)
            # The newly-created Environment record:
            #   {id, type, displayName, description, workspaceId,
            #    properties, attributes}
            # Note: publishDetails is NOT present until publish_environment
            # is called. wait_for_published correctly handles its absence
            # (treats as in-progress and keeps polling).
            assert status.get("id") == item_id
            assert status.get("displayName") == display_name
            assert status.get("type") == "Environment"
            assert status.get("workspaceId") == self.ws_id
        finally:
            delete_item(self.client, self.ws_id, item_id)
