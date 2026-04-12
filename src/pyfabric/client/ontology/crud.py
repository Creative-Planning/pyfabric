"""Ontology CRUD operations via FabricClient.

API reference:
    https://learn.microsoft.com/en-us/rest/api/fabric/ontology/items
"""

import structlog

from pyfabric.client.http import FabricClient

log = structlog.get_logger()


def list_ontologies(client: FabricClient, ws_id: str) -> list[dict]:
    """List all ontologies in a workspace."""
    return client.get_paged(f"workspaces/{ws_id}/ontologies")


def get_ontology(client: FabricClient, ws_id: str, ontology_id: str) -> dict:
    """Get a single ontology by ID."""
    return client.get(f"workspaces/{ws_id}/ontologies/{ontology_id}")


def create_ontology(
    client: FabricClient,
    ws_id: str,
    display_name: str,
    *,
    description: str = "",
    definition_parts: list[dict] | None = None,
) -> dict:
    """Create an ontology in a workspace."""
    log.info("Creating ontology: %s", display_name)
    body: dict = {"displayName": display_name}
    if description:
        body["description"] = description
    if definition_parts:
        body["definition"] = {"parts": definition_parts}
    return client.post(f"workspaces/{ws_id}/ontologies", body)


def get_ontology_definition(
    client: FabricClient,
    ws_id: str,
    ontology_id: str,
) -> dict:
    """Get the ontology definition (entity types, relationships, bindings)."""
    return client.post(
        f"workspaces/{ws_id}/ontologies/{ontology_id}/getDefinition",
    )


def update_ontology_definition(
    client: FabricClient,
    ws_id: str,
    ontology_id: str,
    definition_parts: list[dict],
) -> dict:
    """Replace the ontology definition."""
    log.info("Updating ontology definition: %s", ontology_id)
    return client.post(
        f"workspaces/{ws_id}/ontologies/{ontology_id}/updateDefinition",
        {"definition": {"parts": definition_parts}},
    )


def delete_ontology(client: FabricClient, ws_id: str, ontology_id: str) -> None:
    """Delete an ontology."""
    log.info("Deleting ontology: %s", ontology_id)
    client.delete(f"workspaces/{ws_id}/ontologies/{ontology_id}")
