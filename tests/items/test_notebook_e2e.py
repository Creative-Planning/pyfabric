"""Live REST-based E2E tests for :mod:`pyfabric.items.notebook`.

Skipped when ``PYFABRIC_TEST_WORKSPACE_ID`` is unset, so CI and
contributors without a validation workspace still get a green run.

Each test creates a uniquely-named notebook in the target workspace,
verifies the round-tripped definition, and cleans up in a ``finally``
block so failure paths don't leak items.
"""

from __future__ import annotations

import uuid

import pytest

from pyfabric.client.http import FabricClient
from pyfabric.items.bundle import upload_to_workspace
from pyfabric.items.crud import decode_part, delete_item, get_item_definition
from pyfabric.items.notebook import NotebookBuilder


@pytest.mark.e2e
class TestParametersCellRoundTrip:
    """Publish a notebook with a parameters cell to a real workspace,
    fetch it back via getDefinition, and assert the marker survives."""

    @pytest.fixture(autouse=True)
    def _require_workspace(self, real_workspace_id: str | None) -> None:
        if real_workspace_id is None:
            pytest.skip("PYFABRIC_TEST_WORKSPACE_ID not set")
        self.ws_id = real_workspace_id
        self.client = FabricClient()

    def test_parameters_cell_marker_survives_publish_and_fetch(self) -> None:
        # Unique name so a failed cleanup from a prior run doesn't collide.
        display_name = f"pyfabric_e2e_params_{uuid.uuid4().hex[:8]}"
        nb = (
            NotebookBuilder()
            .add_parameters_cell('BATCH = "default"')
            .add_python("print(BATCH)")
        )
        bundle = nb.to_bundle(display_name=display_name)

        created = upload_to_workspace(bundle, self.client, self.ws_id)
        item_id = created.get("id")
        assert item_id, f"create_item returned no id: {created!r}"

        try:
            definition = get_item_definition(self.client, self.ws_id, item_id)
            parts = definition.get("definition", {}).get("parts", [])
            content_part = next(
                (p for p in parts if p["path"] == "notebook-content.py"),
                None,
            )
            assert content_part is not None, (
                f"notebook-content.py missing from fetched parts: "
                f"{[p['path'] for p in parts]}"
            )
            content = decode_part(content_part).decode("utf-8")
            assert "# PARAMETERS CELL ********************" in content, (
                f"parameters cell marker absent from fetched notebook:\n{content}"
            )
            assert "# CELL ********************" in content, (
                "regular cell marker absent — round-trip dropped the python cell"
            )
        finally:
            delete_item(self.client, self.ws_id, item_id)
