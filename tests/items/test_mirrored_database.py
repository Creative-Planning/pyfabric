"""Tests for :mod:`pyfabric.items.mirrored_database`.

Covers:

- ``MirroredDatabaseBuilder`` round-trip: produces byte-identical
  ``mirroring.json`` to checked-in fixtures (default ``dbo`` and a
  custom default-schema variant). The exact byte shape matters because
  Fabric git-sync rewrites these files; a builder that doesn't match
  Fabric-canonical bytes flaps the file on every sync.
- ``MirroredDatabaseBuilder.save_to_disk`` enforces LF + no trailing
  newline on Windows (regression coverage for the same write_text
  CRLF gotcha NotebookBuilder addressed).
- REST lifecycle helpers (create, get, start, stop, status, table
  status, wait_for_running) issue the right verbs at the right URLs
  via a mocked ``requests.Session``.

The Open Mirroring landing-zone protocol details surfaced in
<https://github.com/UnifiedEducation/research/tree/main/open-mirroring>
were used as documentation; this test suite is independently authored.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pyfabric.items.mirrored_database import (
    MirroredDatabaseBuilder,
    create_mirrored_database,
    get_mirrored_database,
    get_mirroring_status,
    get_tables_mirroring_status,
    start_mirroring,
    stop_mirroring,
    wait_for_running,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "mirrors"
WS = "00000000-0000-0000-0000-0000000000aa"
MIRROR_ID = "11111111-1111-1111-1111-111111111111"


def _fixture_bytes(name: str) -> bytes:
    return (FIXTURES / f"{name}.MirroredDatabase" / "mirroring.json").read_bytes()


# ── Builder ──────────────────────────────────────────────────────────────────


class TestBuilderDefault:
    def test_default_builder_matches_fixture(self):
        b = MirroredDatabaseBuilder()
        assert b.to_mirroring_json().encode("utf-8") == _fixture_bytes("mirror_default")

    def test_default_schema_is_dbo(self):
        b = MirroredDatabaseBuilder()
        body = json.loads(b.to_mirroring_json())
        assert body["properties"]["target"]["typeProperties"]["defaultSchema"] == "dbo"

    def test_format_is_delta(self):
        b = MirroredDatabaseBuilder()
        body = json.loads(b.to_mirroring_json())
        assert body["properties"]["target"]["typeProperties"]["format"] == "Delta"

    def test_source_type_is_generic_mirror(self):
        """``GenericMirror`` is the source type required for Open Mirroring;
        a create body without it is rejected by ``startMirroring``."""
        b = MirroredDatabaseBuilder()
        body = json.loads(b.to_mirroring_json())
        assert body["properties"]["source"]["type"] == "GenericMirror"


class TestBuilderCustomSchema:
    def test_custom_default_schema_propagates(self):
        b = MirroredDatabaseBuilder(default_schema="bronze")
        assert b.to_mirroring_json().encode("utf-8") == _fixture_bytes("mirror_custom")

    def test_empty_default_schema_rejected(self):
        with pytest.raises(ValueError, match="default_schema"):
            MirroredDatabaseBuilder(default_schema="")


# ── Bundle + disk integration ───────────────────────────────────────────────


class TestToBundle:
    def test_mirroring_json_is_stored_as_canonical_bytes(self):
        b = MirroredDatabaseBuilder()
        bundle = b.to_bundle(display_name="open_bronze")
        content = bundle.parts["mirroring.json"]
        assert isinstance(content, bytes)
        assert b"\r\n" not in content
        assert not content.endswith(b"\n")

    def test_bundle_metadata(self):
        b = MirroredDatabaseBuilder()
        bundle = b.to_bundle(display_name="open_bronze")
        assert bundle.item_type == "MirroredDatabase"
        assert bundle.display_name == "open_bronze"
        assert bundle.dir_name == "open_bronze.MirroredDatabase"

    def test_logical_id_override(self):
        b = MirroredDatabaseBuilder()
        bundle = b.to_bundle(
            display_name="open_bronze",
            logical_id="22222222-2222-2222-2222-222222222222",
        )
        assert bundle.logical_id == "22222222-2222-2222-2222-222222222222"


class TestSaveToDisk:
    def test_writes_mirroring_json_with_lf_and_no_trailing_newline(self, tmp_path):
        """Per Fabric convention, ``mirroring.json`` is LF + no trailing
        newline. Routing through write_artifact_file keeps that invariant
        on Windows hosts where ``write_text`` would inject CRLF."""
        b = MirroredDatabaseBuilder()
        artifact_dir = b.save_to_disk(tmp_path, display_name="m_test")
        raw = (artifact_dir / "mirroring.json").read_bytes()
        assert b"\r\n" not in raw
        assert not raw.endswith(b"\n")

    def test_writes_platform_json_with_lf_and_no_trailing_newline(self, tmp_path):
        b = MirroredDatabaseBuilder()
        artifact_dir = b.save_to_disk(tmp_path, display_name="m_test")
        raw = (artifact_dir / ".platform").read_bytes()
        assert b"\r\n" not in raw
        assert not raw.endswith(b"\n")

    def test_returns_artifact_directory_path(self, tmp_path):
        b = MirroredDatabaseBuilder()
        artifact_dir = b.save_to_disk(tmp_path, display_name="m_test")
        assert artifact_dir == tmp_path / "m_test.MirroredDatabase"
        assert artifact_dir.is_dir()


# ── REST lifecycle helpers ──────────────────────────────────────────────────


def _make_response(status: int, body: dict | None = None) -> MagicMock:
    """Shape a MagicMock to look like a requests.Response for FabricClient."""
    resp = MagicMock()
    resp.status_code = status
    payload = json.dumps(body or {})
    resp.text = payload
    resp.content = payload.encode("utf-8")
    resp.json.return_value = body or {}
    resp.headers = {}
    return resp


class _StubClient:
    """A minimal stand-in for FabricClient. Records calls, returns canned
    responses. Avoids spinning up the real client (and its credential)."""

    def __init__(self, responses: dict[tuple[str, str], dict] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[tuple[str, str, object]] = []

    def post(self, path: str, body: object = None) -> dict:
        self.calls.append(("POST", path, body))
        return self.responses.get(("POST", path), {})

    def get(self, path: str, params: dict | None = None) -> dict:
        self.calls.append(("GET", path, params))
        return self.responses.get(("GET", path), {})

    def delete(self, path: str) -> None:
        self.calls.append(("DELETE", path, None))


class TestCreate:
    def test_create_posts_to_mirrored_databases_collection(self):
        client = _StubClient(
            {
                ("POST", f"workspaces/{WS}/mirroredDatabases"): {
                    "id": MIRROR_ID,
                    "displayName": "open_bronze",
                }
            }
        )
        result = create_mirrored_database(
            client, WS, display_name="open_bronze", description="bronze ingest"
        )
        assert result["id"] == MIRROR_ID
        assert client.calls[0][0] == "POST"
        assert client.calls[0][1] == f"workspaces/{WS}/mirroredDatabases"

    def test_create_attaches_default_definition_when_none_provided(self):
        client = _StubClient()
        create_mirrored_database(client, WS, display_name="open_bronze")
        body = client.calls[0][2]
        assert isinstance(body, dict)
        assert "definition" in body
        # The default definition must include a base64-encoded mirroring.json
        # part so startMirroring is allowed.
        parts = body["definition"]["parts"]
        assert any(p["path"] == "mirroring.json" for p in parts)

    def test_create_uses_caller_definition_when_provided(self):
        client = _StubClient()
        custom_definition = {"parts": [{"path": "mirroring.json", "payload": "Zm9v"}]}
        create_mirrored_database(
            client,
            WS,
            display_name="open_bronze",
            definition=custom_definition,
        )
        body = client.calls[0][2]
        assert body["definition"] is custom_definition


class TestGet:
    def test_get_calls_correct_path(self):
        client = _StubClient(
            {
                ("GET", f"workspaces/{WS}/mirroredDatabases/{MIRROR_ID}"): {
                    "id": MIRROR_ID
                }
            }
        )
        result = get_mirrored_database(client, WS, MIRROR_ID)
        assert result["id"] == MIRROR_ID


class TestStartStop:
    def test_start_posts_to_start_mirroring(self):
        client = _StubClient()
        start_mirroring(client, WS, MIRROR_ID)
        assert client.calls[0] == (
            "POST",
            f"workspaces/{WS}/mirroredDatabases/{MIRROR_ID}/startMirroring",
            None,
        )

    def test_stop_posts_to_stop_mirroring(self):
        client = _StubClient()
        stop_mirroring(client, WS, MIRROR_ID)
        assert client.calls[0] == (
            "POST",
            f"workspaces/{WS}/mirroredDatabases/{MIRROR_ID}/stopMirroring",
            None,
        )


class TestStatus:
    def test_get_mirroring_status_returns_status_dict(self):
        client = _StubClient(
            {
                (
                    "POST",
                    f"workspaces/{WS}/mirroredDatabases/{MIRROR_ID}/getMirroringStatus",
                ): {"status": "Running"}
            }
        )
        result = get_mirroring_status(client, WS, MIRROR_ID)
        assert result["status"] == "Running"

    def test_get_tables_status_returns_table_dict(self):
        client = _StubClient(
            {
                (
                    "POST",
                    f"workspaces/{WS}/mirroredDatabases/{MIRROR_ID}/getTablesMirroringStatus",
                ): {"data": [{"sourceTableName": "dim_x", "status": "Replicating"}]}
            }
        )
        result = get_tables_mirroring_status(client, WS, MIRROR_ID)
        assert result["data"][0]["status"] == "Replicating"


class TestWaitForRunning:
    def test_returns_when_status_is_running(self):
        client = _StubClient(
            {
                (
                    "POST",
                    f"workspaces/{WS}/mirroredDatabases/{MIRROR_ID}/getMirroringStatus",
                ): {"status": "Running"}
            }
        )
        result = wait_for_running(client, WS, MIRROR_ID, timeout_s=5, poll_interval_s=0)
        assert result["status"] == "Running"

    def test_polls_until_running(self, monkeypatch):
        """Initial polls return Initialized, eventual poll returns Running."""
        responses = iter([{"status": "Initialized"}, {"status": "Running"}])

        class _PollingClient(_StubClient):
            def post(self, path: str, body: object = None) -> dict:
                self.calls.append(("POST", path, body))
                return next(responses)

        client = _PollingClient()
        result = wait_for_running(client, WS, MIRROR_ID, timeout_s=5, poll_interval_s=0)
        assert result["status"] == "Running"
        assert len(client.calls) == 2

    def test_times_out_when_never_running(self, monkeypatch):
        client = _StubClient(
            {
                (
                    "POST",
                    f"workspaces/{WS}/mirroredDatabases/{MIRROR_ID}/getMirroringStatus",
                ): {"status": "Initialized"}
            }
        )
        # Force the timeout immediately by setting timeout_s=0.
        with pytest.raises(TimeoutError, match="Running"):
            wait_for_running(client, WS, MIRROR_ID, timeout_s=0, poll_interval_s=0)

    def test_status_field_case_insensitive(self):
        """Some endpoints return ``state`` instead of ``status``; helper must
        accept either, and the value comparison is case-insensitive."""
        client = _StubClient(
            {
                (
                    "POST",
                    f"workspaces/{WS}/mirroredDatabases/{MIRROR_ID}/getMirroringStatus",
                ): {"state": "running"}
            }
        )
        result = wait_for_running(client, WS, MIRROR_ID, timeout_s=5, poll_interval_s=0)
        assert (result.get("status") or result.get("state", "")).lower() == "running"


# ── Item-type registration ──────────────────────────────────────────────────


class TestItemTypeAlignment:
    def test_mirrored_database_is_in_normalize_glob_list(self):
        """ARTIFACT_GLOBS must cover MirroredDatabase so ``normalize-artifacts``
        walks into these folders."""
        from pyfabric.items.normalize import ARTIFACT_GLOBS

        assert any("MirroredDatabase" in g for g in ARTIFACT_GLOBS)
