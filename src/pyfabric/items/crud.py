"""
CRUD operations for Microsoft Fabric Workspace Items.

Covers all generic item types (Lakehouse, Notebook, DataPipeline, SemanticModel,
Report, Dataflow, Warehouse, etc.).  Type-specific operations (e.g. running a
notebook, loading a table) belong in separate modules.

API reference:
  https://learn.microsoft.com/en-us/rest/api/fabric/core/items
"""

import base64

import structlog

from pyfabric.client.http import FabricClient

log = structlog.get_logger()


# ------------------------------------------------------------------
# Read
# ------------------------------------------------------------------


def list_items(
    client: FabricClient,
    workspace_id: str,
    *,
    item_type: str | None = None,
) -> list[dict]:
    """
    List all items in a workspace.

    Args:
        item_type: Optional filter, e.g. "Lakehouse", "Notebook", "DataPipeline",
                   "SemanticModel", "Report", "Dataflow", "Warehouse".
    """
    params = {"type": item_type} if item_type else None
    return client.get_paged(f"workspaces/{workspace_id}/items", params)


def get_item(client: FabricClient, workspace_id: str, item_id: str) -> dict:
    """Return a single item by workspace + item ID."""
    return client.get(f"workspaces/{workspace_id}/items/{item_id}")


def get_item_definition(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    *,
    format: str | None = None,
) -> dict:
    """
    Return the item definition (source code / payload).

    Args:
        format: Optional format string, e.g. "ipynb" for notebooks.

    Returns:
        Dict with a `definition.parts` list, each part having
        `path`, `payload` (base64), and `payloadType`.
    """
    params = {"format": format} if format else None
    return client.post(
        f"workspaces/{workspace_id}/items/{item_id}/getDefinition",
        params,
    )


# ------------------------------------------------------------------
# Create
# ------------------------------------------------------------------


def create_item(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    item_type: str,
    *,
    description: str = "",
    definition_parts: list[dict] | None = None,
) -> dict:
    """
    Create a workspace item.

    Args:
        display_name:      Item display name.
        item_type:         Fabric item type string, e.g. "Lakehouse", "Notebook".
        description:       Optional description.
        definition_parts:  Optional list of definition part dicts:
                           [{"path": "...", "payload": "<base64>", "payloadType": "InlineBase64"}]

    Returns:
        The created item dict.
    """
    log.info("Creating %s: %s", item_type, display_name)
    body: dict = {"displayName": display_name, "type": item_type}
    if description:
        body["description"] = description
    if definition_parts:
        body["definition"] = {"parts": definition_parts}
    return client.post(f"workspaces/{workspace_id}/items", body)


# ------------------------------------------------------------------
# Update
# ------------------------------------------------------------------


def update_item(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    *,
    display_name: str | None = None,
    description: str | None = None,
) -> dict:
    """Update an item's display name and/or description."""
    body: dict = {}
    if display_name is not None:
        body["displayName"] = display_name
    if description is not None:
        body["description"] = description
    if not body:
        raise ValueError(
            "Provide at least one of display_name or description to update."
        )
    return client.patch(f"workspaces/{workspace_id}/items/{item_id}", body)


def update_item_definition(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    definition_parts: list[dict],
    *,
    update_metadata: bool = False,
) -> dict:
    """
    Replace the item definition (source code / payload).

    Args:
        definition_parts:  List of part dicts with path, payload (base64), payloadType.
        update_metadata:   If True, pass updateMetadata=true query param.
    """
    path = f"workspaces/{workspace_id}/items/{item_id}/updateDefinition"
    if update_metadata:
        path = f"{path}?updateMetadata=true"
    return client.post(path, {"definition": {"parts": definition_parts}})


# ------------------------------------------------------------------
# Delete
# ------------------------------------------------------------------


def delete_item(client: FabricClient, workspace_id: str, item_id: str) -> None:
    """Delete a workspace item permanently."""
    client.delete(f"workspaces/{workspace_id}/items/{item_id}")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def encode_part(path: str, content: str | bytes) -> dict:
    """
    Build a definition part dict from a file path and its content.

    Args:
        path:    Part path as used by the Fabric API, e.g. "notebook-content.py".
        content: Raw text or bytes to base64-encode.

    Returns:
        {"path": path, "payload": "<base64>", "payloadType": "InlineBase64"}
    """
    if isinstance(content, str):
        content = content.encode()
    payload = base64.b64encode(content).decode()
    return {"path": path, "payload": payload, "payloadType": "InlineBase64"}


def decode_part(part: dict) -> bytes:
    """
    Decode the base64 payload from a definition part dict.

    Inverse of :func:`encode_part` — pass the whole part dict
    (e.g. an entry from ``defn["definition"]["parts"]``), not the
    bare ``payload`` string::

        part = encode_part("notebook-content.py", "print('hi')")
        assert decode_part(part) == b"print('hi')"

    Args:
        part: A part dict with at least a ``"payload"`` key holding a
            base64-encoded string (``payloadType: "InlineBase64"``).

    Returns:
        The decoded bytes of ``part["payload"]``.

    Raises:
        TypeError: If ``part`` is not a dict. The most common cause is
            passing ``part["payload"]`` directly; the message points
            back to the right call shape.
    """
    if not isinstance(part, dict):
        raise TypeError(
            f"decode_part expects a part dict (e.g. an entry from "
            f"definition['parts']), got {type(part).__name__}. "
            f"Pass the whole part dict, not part['payload']."
        )
    return base64.b64decode(part["payload"])
