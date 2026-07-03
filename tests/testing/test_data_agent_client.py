"""Tests for the data agent MCP client (mocked session — no live calls)."""

import asyncio
import sys
import types
from contextlib import asynccontextmanager

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
        monkeypatch.setitem(sys.modules, "mcp.client", None)
        monkeypatch.setitem(sys.modules, "mcp.client.streamable_http", None)
        client = DataAgentClient(WS, AGENT, credential=_FakeCredential())
        with pytest.raises(DataAgentError, match=r"pyfabric\[dataagent\]"):
            client.ask("anything")


class _FakeInitializedSession(_FakeSession):
    """A _FakeSession that also supports the initialize handshake."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.initialized = False

    async def initialize(self):
        self.initialized = True


class _FakeClientSession:
    """Minimal stand-in for ``mcp.ClientSession``."""

    def __init__(self, read, write):
        self.read = read
        self.write = write
        self.session = _FakeInitializedSession(
            tools=[_FakeTool()],
            result=_FakeCallResult([_FakeTextBlock("answer")]),
        )

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *exc):
        return False


class _FakeHttpxClient:
    """Stands in for the httpx.AsyncClient the mcp factory returns."""

    def __init__(self, headers):
        self.headers = headers
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False


def _install_fake_mcp(monkeypatch, streamable_http_mod):
    """Register fake ``mcp`` / ``mcp.client`` / ``...streamable_http`` modules.

    Lets ``_ask_async`` run its lazy imports against controlled modules,
    so tests can pin exactly which transport names exist.
    """
    mcp_mod = types.ModuleType("mcp")
    mcp_mod.ClientSession = _FakeClientSession
    client_mod = types.ModuleType("mcp.client")
    client_mod.streamable_http = streamable_http_mod
    mcp_mod.client = client_mod
    monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
    monkeypatch.setitem(sys.modules, "mcp.client", client_mod)
    monkeypatch.setitem(sys.modules, "mcp.client.streamable_http", streamable_http_mod)


class TestTransportSelection:
    """mcp renamed streamablehttp_client -> streamable_http_client (1.24).

    The client must prefer the new name (which takes a caller-managed
    ``http_client=`` instead of ``headers=``) and fall back to the old
    one so the extra's ``mcp>=1.23.0`` floor keeps working.
    """

    def test_prefers_new_transport_name(self, monkeypatch):
        calls = {}
        sh = types.ModuleType("mcp.client.streamable_http")

        def create_mcp_http_client(headers=None):
            calls["factory_headers"] = headers
            return _FakeHttpxClient(headers)

        @asynccontextmanager
        async def streamable_http_client(url, *, http_client=None):
            calls["url"] = url
            calls["http_client"] = http_client
            yield ("read", "write", lambda: None)

        @asynccontextmanager
        async def streamablehttp_client(url, headers=None):
            raise AssertionError(
                "deprecated transport name must not be used when the new one exists"
            )
            yield  # pragma: no cover

        sh.create_mcp_http_client = create_mcp_http_client
        sh.streamable_http_client = streamable_http_client
        sh.streamablehttp_client = streamablehttp_client
        _install_fake_mcp(monkeypatch, sh)

        client = DataAgentClient(WS, AGENT, credential=_FakeCredential())
        assert client.ask("q") == "answer"
        assert calls["url"] == client.mcp_url
        assert calls["factory_headers"] == {"Authorization": "Bearer fake-token"}
        assert isinstance(calls["http_client"], _FakeHttpxClient)
        # We created the httpx client, so we must close it ourselves —
        # the new transport API only closes clients it creates itself.
        assert calls["http_client"].closed

    def test_falls_back_to_old_transport_name(self, monkeypatch):
        calls = {}
        sh = types.ModuleType("mcp.client.streamable_http")

        @asynccontextmanager
        async def streamablehttp_client(url, headers=None):
            calls["url"] = url
            calls["headers"] = headers
            yield ("read", "write", lambda: None)

        sh.streamablehttp_client = streamablehttp_client
        _install_fake_mcp(monkeypatch, sh)

        client = DataAgentClient(WS, AGENT, credential=_FakeCredential())
        assert client.ask("q") == "answer"
        assert calls["url"] == client.mcp_url
        assert calls["headers"] == {"Authorization": "Bearer fake-token"}
