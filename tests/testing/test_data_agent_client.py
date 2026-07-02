"""Tests for the data agent MCP client (mocked session — no live calls)."""

import asyncio
import sys

import pytest

from pyfabric.testing.data_agent import (
    MCP_URL_TEMPLATE,
    DataAgentClient,
    DataAgentError,
)

WS = "11111111-1111-1111-1111-111111111111"
AGENT = "22222222-2222-2222-2222-222222222222"


class _FakeCredential:
    def __init__(self):
        self.requested: list[str] = []

    def get_token(self, resource: str) -> str:
        self.requested.append(resource)
        return "fake-token"


class _FakeTool:
    name = "data_agent_tool"

    def __init__(self):
        self.inputSchema = {"properties": {"user_question": {"type": "string"}}}


class _FakeToolsResult:
    def __init__(self, tools):
        self.tools = tools


class _FakeTextBlock:
    type = "text"

    def __init__(self, text: str):
        self.text = text


class _FakeCallResult:
    def __init__(self, blocks, is_error=False):
        self.content = blocks
        self.isError = is_error


class _FakeSession:
    def __init__(self, tools, result):
        self._tools = tools
        self._result = result
        self.calls: list[tuple[str, dict]] = []

    async def list_tools(self):
        return _FakeToolsResult(self._tools)

    async def call_tool(self, name, args):
        self.calls.append((name, args))
        return self._result


class TestUrlAndHeaders:
    def test_mcp_url(self):
        client = DataAgentClient(WS, AGENT)
        assert client.mcp_url == MCP_URL_TEMPLATE.format(
            workspace_id=WS, agent_id=AGENT
        )
        assert WS in client.mcp_url and AGENT in client.mcp_url

    def test_headers_use_fabric_scope(self):
        cred = _FakeCredential()
        client = DataAgentClient(WS, AGENT, credential=cred)
        headers = client._headers()
        assert headers == {"Authorization": "Bearer fake-token"}
        assert cred.requested == ["https://api.fabric.microsoft.com"]


class TestAskWithSession:
    def _ask(self, session, question="How many widgets?"):
        client = DataAgentClient(WS, AGENT, credential=_FakeCredential())
        return asyncio.run(client._ask_with_session(session, question))

    def test_happy_path_joins_text_blocks(self):
        session = _FakeSession(
            tools=[_FakeTool()],
            result=_FakeCallResult([_FakeTextBlock("42"), _FakeTextBlock("units")]),
        )
        assert self._ask(session) == "42\nunits"
        # The question argument name comes from the tool's input schema,
        # not a hard-coded literal.
        assert session.calls == [
            ("data_agent_tool", {"user_question": "How many widgets?"})
        ]

    def test_non_text_blocks_ignored(self):
        class _ImageBlock:
            type = "image"
            text = "should-not-appear"

        session = _FakeSession(
            tools=[_FakeTool()],
            result=_FakeCallResult([_ImageBlock(), _FakeTextBlock("answer")]),
        )
        assert self._ask(session) == "answer"

    def test_no_tools_hints_at_unpublished_agent(self):
        session = _FakeSession(tools=[], result=None)
        with pytest.raises(DataAgentError, match="published"):
            self._ask(session)

    def test_error_result_raises_with_detail(self):
        session = _FakeSession(
            tools=[_FakeTool()],
            result=_FakeCallResult([_FakeTextBlock("boom")], is_error=True),
        )
        with pytest.raises(DataAgentError, match="boom"):
            self._ask(session)


class TestMissingMcpPackage:
    def test_ask_raises_install_hint(self, monkeypatch):
        # Setting a sys.modules entry to None makes `import mcp` raise
        # ImportError regardless of whether mcp is actually installed.
        monkeypatch.setitem(sys.modules, "mcp", None)
        monkeypatch.setitem(sys.modules, "mcp.client.streamable_http", None)
        client = DataAgentClient(WS, AGENT, credential=_FakeCredential())
        with pytest.raises(DataAgentError, match=r"pyfabric\[dataagent\]"):
            client.ask("anything")
