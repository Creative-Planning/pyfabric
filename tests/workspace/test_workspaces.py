"""Tests for workspace CRUD operations."""

import pytest

from pyfabric.workspace.workspaces import (
    assign_to_capacity,
    create_workspace,
    delete_workspace,
    get_workspace,
    list_workspaces,
    update_workspace,
)


class TestWorkspaceCrud:
    def test_list_workspaces(self, mock_fabric_client):
        mock_fabric_client.get_paged.return_value = [{"id": "ws-1"}, {"id": "ws-2"}]
        result = list_workspaces(mock_fabric_client)
        assert len(result) == 2

    def test_get_workspace(self, mock_fabric_client):
        mock_fabric_client.get.return_value = {"id": "ws-1", "displayName": "My WS"}
        result = get_workspace(mock_fabric_client, "ws-1")
        assert result["displayName"] == "My WS"

    def test_create_workspace(self, mock_fabric_client):
        mock_fabric_client.post.return_value = {"id": "ws-new"}
        result = create_workspace(mock_fabric_client, "New Workspace")
        assert result["id"] == "ws-new"
        body = mock_fabric_client.post.call_args[0][1]
        assert body["displayName"] == "New Workspace"

    def test_create_workspace_with_capacity(self, mock_fabric_client):
        mock_fabric_client.post.return_value = {"id": "ws-new"}
        create_workspace(mock_fabric_client, "WS", capacity_id="cap-1")
        body = mock_fabric_client.post.call_args[0][1]
        assert body["capacityId"] == "cap-1"

    def test_update_workspace(self, mock_fabric_client):
        mock_fabric_client.patch.return_value = {}
        update_workspace(mock_fabric_client, "ws-1", display_name="Renamed")
        body = mock_fabric_client.patch.call_args[0][1]
        assert body["displayName"] == "Renamed"

    def test_update_workspace_no_fields_raises(self, mock_fabric_client):
        with pytest.raises(ValueError, match="at least one"):
            update_workspace(mock_fabric_client, "ws-1")

    def test_delete_workspace(self, mock_fabric_client):
        delete_workspace(mock_fabric_client, "ws-1")
        mock_fabric_client.delete.assert_called_once_with("workspaces/ws-1")

    def test_assign_to_capacity(self, mock_fabric_client):
        assign_to_capacity(mock_fabric_client, "ws-1", "cap-1")
        call_args = mock_fabric_client.post.call_args
        assert "assignToCapacity" in call_args[0][0]
