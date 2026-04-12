"""
Client for the Fabric Graph Model REST API.

Wraps the Graph Model endpoints for listing, querying (GQL), refreshing,
and managing graph model definitions.

Usage:
    from pyfabric.client.http import FabricClient
    from pyfabric.client.graph import GraphClient

    client = FabricClient()
    graph = GraphClient(client, workspace_id="...")

    models = graph.list_graph_models()
    result = graph.execute_query(graph_id, "MATCH (n) RETURN n LIMIT 10")

API reference:
    https://learn.microsoft.com/en-us/rest/api/fabric/graphmodel/items
"""

import base64
import json
import logging
import time

from .http import FabricClient, FabricError

log = logging.getLogger(__name__)


class GraphClient:
    """Wrapper over the Fabric Graph Model REST API."""

    def __init__(self, client: FabricClient, workspace_id: str):
        self._client = client
        self.workspace_id = workspace_id

    def _path(self, graph_id: str | None = None, action: str | None = None) -> str:
        base = f"workspaces/{self.workspace_id}/graphModels"
        if graph_id:
            base = f"{base}/{graph_id}"
        if action:
            base = f"{base}/{action}"
        return base

    # -- List Graph Models -------------------------------------------------

    def list_graph_models(self) -> list[dict]:
        """List all graph models in the workspace."""
        return self._client.get_paged(self._path())

    # -- Get Graph Model ---------------------------------------------------

    def get_graph_model(self, graph_id: str) -> dict:
        """Get properties of a specific graph model."""
        return self._client.get(self._path(graph_id))

    # -- Get Graph Model Definition ----------------------------------------

    def get_definition(self, graph_id: str) -> dict:
        """Get the graph model definition. Handles LRO automatically."""
        return self._client.post(self._path(graph_id, "getDefinition"))

    def get_definition_decoded(self, graph_id: str) -> dict:
        """Get the graph model definition with base64 payloads decoded.

        Returns:
            Dict mapping {path: decoded_content} where content is parsed
            JSON (dict) or raw string if JSON parsing fails.
        """
        raw = self.get_definition(graph_id)
        parts = raw.get("definition", {}).get("parts", [])
        decoded = {}
        for part in parts:
            payload = part.get("payload", "")
            try:
                decoded[part["path"]] = json.loads(base64.b64decode(payload))
            except (json.JSONDecodeError, Exception):
                decoded[part["path"]] = base64.b64decode(payload).decode(
                    "utf-8", errors="replace"
                )
        return decoded

    # -- Execute Query (beta) ----------------------------------------------

    def execute_query(self, graph_id: str, query: str) -> dict:
        """Execute a GQL query against the graph model.

        Uses the beta API endpoint. Returns the full response dict.
        """
        return self._client.post(
            self._path(graph_id, "executeQuery") + "?beta=true",
            {"query": query},
        )

    # -- Get Queryable Graph Type (beta) -----------------------------------

    def get_queryable_graph_type(self, graph_id: str) -> dict:
        """Get the graph schema (node types and edge types)."""
        return self._client.get(
            self._path(graph_id, "getQueryableGraphType"),
            params={"beta": "true"},
        )

    # -- Refresh Graph (Background Job) ------------------------------------

    def refresh(
        self, graph_id: str, *, wait: bool = True, poll_interval: int = 15
    ) -> dict:
        """Trigger an on-demand graph refresh.

        If wait=True (default), polls until the refresh completes.
        If wait=False, returns immediately with status and location.

        The refresh job uses a different LRO pattern than standard Fabric
        operations (status "Completed" instead of "Succeeded", and a
        failureReason field), so we handle polling manually.
        """
        from .http import BASE_URL

        path = self._path(graph_id, "jobs/refreshGraph/instances")
        url = f"{BASE_URL}/{path}"
        resp = self._client._request("POST", url)

        if resp.status_code == 200:
            log.info("Refresh completed immediately")
            return {"status": "Completed"}

        if resp.status_code == 202:
            location = resp.headers.get("Location")
            retry_after = int(resp.headers.get("Retry-After", poll_interval))
            log.info("Refresh job accepted (polling every %ds)", retry_after)

            if not wait:
                return {"status": "Accepted", "location": location}

            while True:
                time.sleep(retry_after)
                poll_resp = self._client._request("GET", location)
                body = poll_resp.json() if poll_resp.text else {}
                status = body.get("status", "Unknown")
                log.debug("Refresh status: %s", status)

                if status in ("Completed", "Failed", "Cancelled"):
                    if body.get("failureReason"):
                        reason = body["failureReason"]
                        msg = reason.get("message", str(reason))
                        raise RuntimeError(f"Refresh {status}: {msg}")
                    return body
                retry_after = min(retry_after, 15)

        raise FabricError(resp.status_code, resp.text, url)

    # -- Delete Graph Model ------------------------------------------------

    def delete_graph_model(self, graph_id: str) -> None:
        """Delete a graph model."""
        self._client.delete(self._path(graph_id))
