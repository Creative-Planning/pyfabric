"""Query a published Fabric data agent over its MCP endpoint.

A published data agent exposes a Model Context Protocol (MCP) server with
a single tool: send it a natural-language question, get back an answer
grounded in the data the agent can reach. This module is a thin,
synchronous pass-through over the official ``mcp`` client — pyfabric adds
only the house rails: :class:`~pyfabric.client.auth.FabricCredential`
token acquisition, URL construction, and a pytest fixture.

pyfabric deliberately does NOT wrap agent authoring — create and
configure agents with Microsoft's ``fabric-data-agent-sdk``. See
``docs/data-agent.md`` for the adopt-first breakdown.

Install the transport with the ``dataagent`` extra::

    pip install pyfabric[dataagent]

Usage::

    from pyfabric.testing.data_agent import DataAgentClient

    client = DataAgentClient(workspace_id, agent_id)
    answer = client.ask("What was total revenue last month?")

The agent must be **published** — Fabric only serves the MCP endpoint
for the published stage; an unpublished agent returns errors even with
a correct URL.

The intended validation pattern (see ``docs/data-agent.md``): keep a
golden-question set of ``(question, ground_truth_sql, tolerance)``
entries, execute the SQL against the same lakehouse at test time, ask
the agent the question, and compare with type-aware tolerances. Gate the
live tests behind the ``data_agent_client`` fixture's environment
variables so suites skip cleanly when no published agent is reachable.
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog

log = structlog.get_logger()

#: MCP endpoint template for a published data agent.
MCP_URL_TEMPLATE = (
    "https://api.fabric.microsoft.com/v1/mcp/workspaces/{workspace_id}"
    "/dataagents/{agent_id}/agent"
)

#: Environment variables read by the ``data_agent_client`` pytest fixture.
ENV_WORKSPACE = "PYFABRIC_DATA_AGENT_WORKSPACE"
ENV_AGENT = "PYFABRIC_DATA_AGENT_ID"


class DataAgentError(Exception):
    """Raised when the data agent MCP exchange fails."""


class DataAgentClient:
    """Synchronous client for a published Fabric data agent's MCP endpoint.

    Args:
        workspace_id: Fabric workspace GUID containing the agent.
        agent_id: The data agent's item (artifact) GUID.
        credential: Anything with ``get_token(resource) -> str``.
            Defaults to :class:`~pyfabric.client.auth.FabricCredential`,
            which chains ``azure.identity`` and the az CLI.
    """

    def __init__(
        self,
        workspace_id: str,
        agent_id: str,
        *,
        credential: Any | None = None,
    ) -> None:
        self.workspace_id = workspace_id
        self.agent_id = agent_id
        self._credential = credential

    @property
    def mcp_url(self) -> str:
        """The agent's MCP endpoint URL."""
        return MCP_URL_TEMPLATE.format(
            workspace_id=self.workspace_id, agent_id=self.agent_id
        )

    def _headers(self) -> dict[str, str]:
        """Bearer-token auth header for the Fabric API scope."""
        if self._credential is None:
            from pyfabric.client.auth import FabricCredential

            self._credential = FabricCredential()
        token = self._credential.get_token("https://api.fabric.microsoft.com")
        return {"Authorization": f"Bearer {token}"}

    def ask(self, question: str) -> str:
        """Send ``question`` to the data agent and return its text answer.

        Opens a streamable-HTTP MCP connection, runs the ``initialize``
        handshake, discovers the agent's single tool (the question
        argument name is read from the tool's input schema, not
        hard-coded), calls it, and joins the text content blocks.

        Raises:
            DataAgentError: If the ``mcp`` package is missing, the agent
                advertises no tools (typically: not published), or the
                tool call returns an error result.
        """
        return asyncio.run(self._ask_async(question))

    async def _ask_async(self, question: str) -> str:
        try:
            from mcp import ClientSession
            from mcp.client.streamable_http import streamablehttp_client
        except ImportError as e:
            raise DataAgentError(
                "The 'mcp' package is required to query a data agent — "
                "install it with: pip install pyfabric[dataagent]"
            ) from e

        log.debug("Querying data agent", url=self.mcp_url)
        async with (
            streamablehttp_client(self.mcp_url, headers=self._headers()) as (
                read,
                write,
                _,
            ),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            return await self._ask_with_session(session, question)

    async def _ask_with_session(self, session: Any, question: str) -> str:
        """Run tool discovery + call on an initialized MCP session.

        Split out from the transport so tests can drive it with a fake
        session object — no ``mcp`` install or live endpoint needed.
        """
        tools = await session.list_tools()
        if not tools.tools:
            raise DataAgentError(
                f"Data agent at {self.mcp_url} advertises no MCP tools — "
                "is the agent published? Fabric serves the MCP endpoint "
                "only for the published stage."
            )
        tool = tools.tools[0]
        question_arg = next(iter(tool.inputSchema["properties"]))

        result = await session.call_tool(tool.name, {question_arg: question})
        texts = [
            block.text
            for block in result.content
            if getattr(block, "type", None) == "text"
        ]
        if getattr(result, "isError", False):
            raise DataAgentError(
                f"Data agent tool call failed: {' '.join(texts) or 'no detail'}"
            )
        return "\n".join(texts)
