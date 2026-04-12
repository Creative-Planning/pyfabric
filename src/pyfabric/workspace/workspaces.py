"""
CRUD operations for Microsoft Fabric Workspaces.

API reference:
  https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces
"""

import logging

from pyfabric.client.http import FabricClient

log = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Read
# ------------------------------------------------------------------


def list_workspaces(client: FabricClient) -> list[dict]:
    """Return all workspaces the caller has access to."""
    return client.get_paged("workspaces")


def get_workspace(client: FabricClient, workspace_id: str) -> dict:
    """Return a single workspace by ID."""
    return client.get(f"workspaces/{workspace_id}")


# ------------------------------------------------------------------
# Create
# ------------------------------------------------------------------


def create_workspace(
    client: FabricClient,
    display_name: str,
    *,
    description: str = "",
    capacity_id: str | None = None,
) -> dict:
    """
    Create a new workspace.

    Args:
        display_name: Workspace display name (must be unique in tenant).
        description:  Optional description.
        capacity_id:  Optional Fabric capacity ID to assign on creation.

    Returns:
        The created workspace dict.
    """
    body: dict = {"displayName": display_name}
    if description:
        body["description"] = description
    if capacity_id:
        body["capacityId"] = capacity_id
    return client.post("workspaces", body)


# ------------------------------------------------------------------
# Update
# ------------------------------------------------------------------


def update_workspace(
    client: FabricClient,
    workspace_id: str,
    *,
    display_name: str | None = None,
    description: str | None = None,
) -> dict:
    """
    Update workspace display name and/or description.

    Only fields that are not None are sent in the PATCH body.
    """
    body: dict = {}
    if display_name is not None:
        body["displayName"] = display_name
    if description is not None:
        body["description"] = description
    if not body:
        raise ValueError(
            "Provide at least one of display_name or description to update."
        )
    return client.patch(f"workspaces/{workspace_id}", body)


# ------------------------------------------------------------------
# Delete
# ------------------------------------------------------------------


def delete_workspace(client: FabricClient, workspace_id: str) -> None:
    """Delete a workspace permanently."""
    client.delete(f"workspaces/{workspace_id}")


# ------------------------------------------------------------------
# Capacity assignment
# ------------------------------------------------------------------


def assign_to_capacity(
    client: FabricClient, workspace_id: str, capacity_id: str
) -> None:
    """Assign a workspace to a Fabric capacity."""
    client.post(
        f"workspaces/{workspace_id}/assignToCapacity", {"capacityId": capacity_id}
    )


def unassign_from_capacity(client: FabricClient, workspace_id: str) -> None:
    """Remove a workspace from its current capacity (returns it to shared/Pro)."""
    client.post(f"workspaces/{workspace_id}/unassignFromCapacity", {})


# ------------------------------------------------------------------
# Role assignments
# ------------------------------------------------------------------


def list_role_assignments(client: FabricClient, workspace_id: str) -> list[dict]:
    """Return all role assignments for a workspace."""
    return client.get_paged(f"workspaces/{workspace_id}/roleAssignments")


def add_role_assignment(
    client: FabricClient,
    workspace_id: str,
    principal_id: str,
    principal_type: str,
    role: str,
) -> dict:
    """
    Add a role assignment to a workspace.

    Args:
        principal_id:   Object ID of the user/group/service principal.
        principal_type: "User", "Group", or "ServicePrincipal".
        role:           "Admin", "Member", "Contributor", or "Viewer".
    """
    body = {
        "principal": {"id": principal_id, "type": principal_type},
        "role": role,
    }
    return client.post(f"workspaces/{workspace_id}/roleAssignments", body)


def delete_role_assignment(
    client: FabricClient, workspace_id: str, principal_id: str
) -> None:
    """Remove a role assignment from a workspace."""
    client.delete(f"workspaces/{workspace_id}/roleAssignments/{principal_id}")
