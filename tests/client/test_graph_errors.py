"""Error path tests for GraphClient — refresh failures, decode errors."""

import base64
import json
from unittest.mock import MagicMock

import pytest

from pyfabric.client.graph import GraphClient
from pyfabric.client.http import FabricError


def _make_graph_client() -> tuple[GraphClient, MagicMock]:
    mock_fabric = MagicMock()
    graph = GraphClient(mock_fabric, workspace_id="ws-test")
    return graph, mock_fabric


class TestDefinitionDecodeErrors:
    def test_invalid_json_payload_falls_back_to_string(self):
        graph, mock = _make_graph_client()
        mock.post.return_value = {
            "definition": {
                "parts": [
                    {
                        "path": "model.json",
                        "payload": base64.b64encode(b"not valid json {{{").decode(),
                    }
                ]
            }
        }
        result = graph.get_definition_decoded("graph-1")
        # Should fall back to raw string, not raise
        assert isinstance(result["model.json"], str)
        assert "not valid json" in result["model.json"]


class TestRefreshErrors:
    def test_refresh_failure_with_reason(self):
        graph, mock = _make_graph_client()
        # Initial POST returns 202
        post_resp = MagicMock()
        post_resp.status_code = 202
        post_resp.headers = {
            "Location": "https://api.fabric.microsoft.com/v1/operations/op-1",
            "Retry-After": "0",
        }
        post_resp.content = b""

        # Poll returns Failed with reason
        poll_resp = MagicMock()
        poll_resp.status_code = 200
        poll_resp.text = json.dumps(
            {
                "status": "Failed",
                "failureReason": {
                    "message": "Data source connection failed: timeout after 30s"
                },
            }
        )
        poll_resp.json.return_value = {
            "status": "Failed",
            "failureReason": {
                "message": "Data source connection failed: timeout after 30s"
            },
        }

        mock.raw_request.side_effect = [post_resp, poll_resp]

        with pytest.raises(RuntimeError, match="Refresh Failed") as exc_info:
            graph.refresh("graph-1", poll_interval=0)
        assert "Data source connection failed" in str(exc_info.value)

    def test_refresh_unexpected_http_error(self):
        graph, mock = _make_graph_client()
        error_resp = MagicMock()
        error_resp.status_code = 500
        error_resp.text = "Internal Server Error"
        error_resp.content = b"Internal Server Error"

        mock.raw_request.return_value = error_resp

        with pytest.raises(FabricError, match="500"):
            graph.refresh("graph-1")
