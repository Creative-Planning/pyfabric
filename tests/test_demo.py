"""Tests for ``pyfabric.demo`` (the read-only workspace inspection demo)."""

import io
import sys
from typing import Any

import pytest

from pyfabric import demo

# ── Stub client ──────────────────────────────────────────────────────────────


class _StubClient:
    """Minimal stand-in for FabricClient used by ``run_demo``."""

    def __init__(
        self,
        *,
        workspaces: list[dict[str, Any]] | None = None,
        items_by_ws: dict[str, list[dict[str, Any]]] | None = None,
        definitions: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._workspaces = workspaces or []
        self._items_by_ws = items_by_ws or {}
        self._definitions = definitions or {}
        self.post_calls: list[tuple[str, Any]] = []

    def get_paged(
        self, path: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        if path == "workspaces":
            return list(self._workspaces)
        if path.startswith("workspaces/") and path.endswith("/items"):
            ws_id = path.split("/")[1]
            return list(self._items_by_ws.get(ws_id, []))
        raise AssertionError(f"unexpected get_paged path: {path}")

    def post(self, path: str, body: Any = None) -> dict[str, Any]:
        self.post_calls.append((path, body))
        # Path looks like workspaces/{ws}/items/{id}/getDefinition
        parts = path.split("/")
        item_id = parts[3] if len(parts) >= 4 else ""
        return self._definitions.get(item_id, {"definition": {"parts": []}})


def _make_client(**kwargs: Any) -> _StubClient:
    return _StubClient(**kwargs)


# ── Happy path ───────────────────────────────────────────────────────────────


class TestRunDemo:
    def test_resolves_exact_match_and_prints_sections(self) -> None:
        client = _make_client(
            workspaces=[
                {"id": "ws-1", "displayName": "Test WS"},
                {"id": "ws-2", "displayName": "Other WS"},
            ],
            items_by_ws={
                "ws-1": [
                    {"id": "lh-1", "type": "Lakehouse", "displayName": "Bronze"},
                    {"id": "lh-2", "type": "Lakehouse", "displayName": "Silver"},
                    {"id": "nb-1", "type": "Notebook", "displayName": "Ingest"},
                    {"id": "sm-1", "type": "SemanticModel", "displayName": "Sales"},
                ],
            },
        )
        out, err = io.StringIO(), io.StringIO()

        rc = demo.run_demo("Test WS", client=client, out=out, err=err)

        stdout = out.getvalue()
        assert rc == 0
        assert "Accessible workspaces (2)" in stdout
        assert "Resolved: Test WS" in stdout
        assert "ws-1" in stdout
        assert "Items (4)" in stdout
        assert "Lakehouse: 2" in stdout
        assert "Notebook: 1" in stdout
        assert "Lakehouse(s):" in stdout
        assert "Bronze" in stdout and "Silver" in stdout

    def test_case_insensitive_fallback(self) -> None:
        client = _make_client(
            workspaces=[{"id": "ws-1", "displayName": "Production"}],
            items_by_ws={"ws-1": []},
        )
        out, err = io.StringIO(), io.StringIO()

        rc = demo.run_demo("production", client=client, out=out, err=err)

        assert rc == 0
        assert "case-insensitive" in out.getvalue()
        assert "Resolved: Production" in out.getvalue()

    def test_show_definitions_fetches_one_item(self) -> None:
        client = _make_client(
            workspaces=[{"id": "ws-1", "displayName": "WS"}],
            items_by_ws={
                "ws-1": [
                    {"id": "nb-1", "type": "Notebook", "displayName": "Demo"},
                ],
            },
            definitions={
                "nb-1": {
                    "definition": {
                        "parts": [
                            {"path": "notebook-content.py", "payload": "..."},
                            {"path": ".platform", "payload": "..."},
                        ]
                    }
                }
            },
        )
        out, err = io.StringIO(), io.StringIO()

        rc = demo.run_demo("WS", client=client, show_definitions=True, out=out, err=err)

        assert rc == 0
        assert "Fetching definition for 'Demo'" in out.getvalue()
        assert "2 part(s)" in out.getvalue()
        assert "notebook-content.py" in out.getvalue()
        assert any(path.endswith("/getDefinition") for path, _ in client.post_calls)

    def test_lists_top_10_and_more_indicator(self) -> None:
        many = [{"id": f"ws-{i}", "displayName": f"WS{i:02d}"} for i in range(15)]
        client = _make_client(
            workspaces=many,
            items_by_ws={"ws-0": []},
        )
        out, err = io.StringIO(), io.StringIO()

        demo.run_demo("WS00", client=client, out=out, err=err)

        assert "and 5 more" in out.getvalue()


# ── Resolution failures ──────────────────────────────────────────────────────


class TestResolution:
    def test_ambiguous_exit_1_lists_ids(self) -> None:
        client = _make_client(
            workspaces=[
                {"id": "ws-A", "displayName": "Shared"},
                {"id": "ws-B", "displayName": "Shared"},
            ],
        )
        out, err = io.StringIO(), io.StringIO()

        rc = demo.run_demo("Shared", client=client, out=out, err=err)

        assert rc == 1
        stderr = err.getvalue()
        assert "Ambiguous" in stderr
        assert "ws-A" in stderr and "ws-B" in stderr

    def test_did_you_mean_suggestion_exit_1(self) -> None:
        client = _make_client(
            workspaces=[
                {"id": "ws-1", "displayName": "Production"},
                {"id": "ws-2", "displayName": "Staging"},
            ],
        )
        out, err = io.StringIO(), io.StringIO()

        rc = demo.run_demo("productn", client=client, out=out, err=err)

        assert rc == 1
        stderr = err.getvalue()
        assert "not found" in stderr
        assert "Did you mean" in stderr
        assert "Production" in stderr

    def test_not_found_no_close_match(self) -> None:
        client = _make_client(
            workspaces=[{"id": "ws-1", "displayName": "Production"}],
        )
        out, err = io.StringIO(), io.StringIO()

        rc = demo.run_demo("zzzzzzzz", client=client, out=out, err=err)

        assert rc == 1
        assert "not found" in err.getvalue()
        assert "Did you mean" not in err.getvalue()


# ── Install-hint path ───────────────────────────────────────────────────────


class TestMissingExtra:
    def test_missing_requests_prints_install_hint_exit_2(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Force ``import requests`` to fail inside run_demo.
        monkeypatch.setitem(sys.modules, "requests", None)
        # Also drop pyfabric.client.http so its module-level requests
        # import re-runs (and fails) instead of returning the cached
        # module that succeeded earlier in this test process.
        monkeypatch.delitem(sys.modules, "pyfabric.client.http", raising=False)

        out, err = io.StringIO(), io.StringIO()
        rc = demo.run_demo("anything", client=None, out=out, err=err)

        assert rc == 2
        stderr = err.getvalue()
        assert 'pip install --pre --upgrade "pyfabric[azure]"' in stderr


# ── CLI surface ─────────────────────────────────────────────────────────────


class TestCli:
    def test_demo_subcommand_dispatches(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        called: dict[str, Any] = {}

        def fake_main(argv: list[str]) -> int:
            called["argv"] = argv
            return 0

        monkeypatch.setattr("pyfabric.demo.main", fake_main)

        from pyfabric import cli

        rc = cli.main(["demo", "My WS", "--show-definitions"])

        assert rc == 0
        assert called["argv"] == ["My WS", "--show-definitions"]

    def test_demo_help_lists_subcommand(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from pyfabric import cli

        rc = cli.main(["--help"])
        assert rc == 0
        assert "pyfabric demo" in capsys.readouterr().out

    def test_demo_main_missing_arg_returns_nonzero(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = demo.main([])
        assert rc != 0
