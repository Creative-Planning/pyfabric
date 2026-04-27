"""Tests for :class:`pyfabric.data.open_mirror.OpenMirrorClient`.

Covers the landing-zone data-plane primitives — ``_metadata.json``
shape, sequential-filename math, parquet upload routing, and
``_ProcessedFiles`` listing — entirely against mocked DFS helpers
(``pyfabric.data.onelake.upload_file`` and ``list_paths``). No
network. Live Fabric coverage is tracked separately by #52.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pyfabric.data.open_mirror import OpenMirrorClient

WS = "00000000-0000-0000-0000-0000000000aa"
MIRROR = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def fake_credential():
    class _FakeCred:
        storage_token = "fake-token"

    return _FakeCred()


def _path_entry(name: str, *, is_dir: bool = False) -> dict:
    """Shape matches the DFS filesystem API response."""
    return {"name": name, "isDirectory": "true" if is_dir else "false"}


# ── ensure_table ────────────────────────────────────────────────────────────


class TestEnsureTable:
    def test_writes_metadata_with_key_columns_only(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with patch("pyfabric.data.open_mirror.onelake.upload_file") as up:
            client.ensure_table("dim_customer", schema="bc2", key_columns=["id"])
        up.assert_called_once()
        path_arg = up.call_args.args[3]
        assert path_arg == "Files/LandingZone/bc2.schema/dim_customer/_metadata.json"
        meta_bytes = up.call_args.args[4]
        meta = json.loads(meta_bytes.decode("utf-8"))
        assert meta == {"keyColumns": ["id"]}

    def test_flat_path_when_no_schema(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with patch("pyfabric.data.open_mirror.onelake.upload_file") as up:
            client.ensure_table("dim_customer", key_columns=["id"])
        path_arg = up.call_args.args[3]
        assert path_arg == "Files/LandingZone/dim_customer/_metadata.json"

    def test_includes_upsert_default_when_set(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with patch("pyfabric.data.open_mirror.onelake.upload_file") as up:
            client.ensure_table(
                "t", schema="s", key_columns=["id"], upsert_default=True
            )
        meta = json.loads(up.call_args.args[4].decode("utf-8"))
        assert meta["isUpsertDefaultRowMarker"] is True

    def test_includes_file_detection_strategy_when_set(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with patch("pyfabric.data.open_mirror.onelake.upload_file") as up:
            client.ensure_table(
                "t", schema="s", key_columns=["id"], detect_by_last_update=True
            )
        meta = json.loads(up.call_args.args[4].decode("utf-8"))
        assert meta["fileDetectionStrategy"] == "LastUpdateTimeFileDetection"

    def test_both_optional_keys_set_together(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with patch("pyfabric.data.open_mirror.onelake.upload_file") as up:
            client.ensure_table(
                "t",
                schema="s",
                key_columns=["a", "b"],
                upsert_default=True,
                detect_by_last_update=True,
            )
        meta = json.loads(up.call_args.args[4].decode("utf-8"))
        assert meta == {
            "keyColumns": ["a", "b"],
            "isUpsertDefaultRowMarker": True,
            "fileDetectionStrategy": "LastUpdateTimeFileDetection",
        }

    def test_empty_key_columns_rejected(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with (
            patch("pyfabric.data.open_mirror.onelake.upload_file") as up,
            pytest.raises(ValueError, match="key_columns"),
        ):
            client.ensure_table("t", schema="s", key_columns=[])
        up.assert_not_called()


# ── next_data_filename ──────────────────────────────────────────────────────


class TestNextDataFilename:
    def test_empty_folder_returns_one_padded_to_20(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with patch("pyfabric.data.open_mirror.onelake.list_paths", return_value=[]):
            name = client.next_data_filename("t", schema="s")
        assert name == "00000000000000000001.parquet"

    def test_picks_max_plus_one(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        entries = [
            _path_entry(
                f"{MIRROR}/Files/LandingZone/s.schema/t/00000000000000000001.parquet"
            ),
            _path_entry(
                f"{MIRROR}/Files/LandingZone/s.schema/t/00000000000000000002.parquet"
            ),
            _path_entry(
                f"{MIRROR}/Files/LandingZone/s.schema/t/00000000000000000005.parquet"
            ),
        ]
        with patch(
            "pyfabric.data.open_mirror.onelake.list_paths", return_value=entries
        ):
            name = client.next_data_filename("t", schema="s")
        assert name == "00000000000000000006.parquet"

    def test_ignores_non_sequential_filenames(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        entries = [
            _path_entry(f"{MIRROR}/Files/LandingZone/s.schema/t/_metadata.json"),
            _path_entry(f"{MIRROR}/Files/LandingZone/s.schema/t/abc.parquet"),
            _path_entry(
                f"{MIRROR}/Files/LandingZone/s.schema/t/00000000000000000007.parquet"
            ),
        ]
        with patch(
            "pyfabric.data.open_mirror.onelake.list_paths", return_value=entries
        ):
            name = client.next_data_filename("t", schema="s")
        assert name == "00000000000000000008.parquet"

    def test_ignores_subfolders_like_processed(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        entries = [
            _path_entry(
                f"{MIRROR}/Files/LandingZone/s.schema/t/_ProcessedFiles", is_dir=True
            ),
            _path_entry(
                f"{MIRROR}/Files/LandingZone/s.schema/t/00000000000000000003.parquet"
            ),
        ]
        with patch(
            "pyfabric.data.open_mirror.onelake.list_paths", return_value=entries
        ):
            name = client.next_data_filename("t", schema="s")
        assert name == "00000000000000000004.parquet"

    def test_custom_extension(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with patch("pyfabric.data.open_mirror.onelake.list_paths", return_value=[]):
            name = client.next_data_filename("t", schema="s", extension="parquet.gz")
        assert name == "00000000000000000001.parquet.gz"


# ── upload_data_file ────────────────────────────────────────────────────────


class TestUploadDataFile:
    def test_assigns_next_sequential_filename_by_default(
        self, tmp_path, fake_credential
    ):
        local = tmp_path / "data.parquet"
        local.write_bytes(b"PAR1FAKE")
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with (
            patch("pyfabric.data.open_mirror.onelake.list_paths", return_value=[]),
            patch("pyfabric.data.open_mirror.onelake.upload_file") as up,
        ):
            remote = client.upload_data_file("t", local, schema="s")
        assert remote == ("Files/LandingZone/s.schema/t/00000000000000000001.parquet")
        # Bytes from disk land at the auto-named path.
        up.assert_called_once()
        assert up.call_args.args[3] == remote
        assert up.call_args.args[4] == b"PAR1FAKE"

    def test_uses_explicit_remote_filename_when_given(self, tmp_path, fake_credential):
        local = tmp_path / "data.parquet"
        local.write_bytes(b"x")
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with (
            patch("pyfabric.data.open_mirror.onelake.list_paths") as ls,
            patch("pyfabric.data.open_mirror.onelake.upload_file") as up,
        ):
            remote = client.upload_data_file(
                "t", local, schema="s", remote_filename="00000000000000000099.parquet"
            )
        assert remote == "Files/LandingZone/s.schema/t/00000000000000000099.parquet"
        # No need to scan the folder when filename is explicit.
        ls.assert_not_called()
        assert up.call_args.args[3] == remote


# ── list_processed ──────────────────────────────────────────────────────────


class TestListProcessed:
    def test_returns_basenames_when_processed_dir_exists(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        entries = [
            _path_entry(
                f"{MIRROR}/Files/LandingZone/s.schema/t/_ProcessedFiles/00000000000000000001.parquet"
            ),
            _path_entry(
                f"{MIRROR}/Files/LandingZone/s.schema/t/_ProcessedFiles/00000000000000000002.parquet"
            ),
        ]
        with patch(
            "pyfabric.data.open_mirror.onelake.list_paths", return_value=entries
        ):
            names = client.list_processed("t", schema="s")
        assert sorted(names) == [
            "00000000000000000001.parquet",
            "00000000000000000002.parquet",
        ]

    def test_returns_empty_when_processed_dir_missing(self, fake_credential):
        # list_paths returns [] on 404 (existing onelake.list_paths contract).
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with patch("pyfabric.data.open_mirror.onelake.list_paths", return_value=[]):
            names = client.list_processed("t", schema="s")
        assert names == []


# ── path builders ───────────────────────────────────────────────────────────


class TestTableFolder:
    def test_table_folder_with_schema(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        assert (
            client.table_folder("dim_customer", schema="bc2")
            == "Files/LandingZone/bc2.schema/dim_customer"
        )

    def test_table_folder_without_schema(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        assert client.table_folder("dim_customer") == "Files/LandingZone/dim_customer"


# ── from research's contract ────────────────────────────────────────────────


class TestProtocolNotes:
    """Sanity checks on properties documented in the landing-zone format
    reference but easy to regress: filename padding width, extension
    handling, ``_metadata.json`` keys."""

    def test_filename_padding_is_exactly_20_digits(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with patch("pyfabric.data.open_mirror.onelake.list_paths", return_value=[]):
            name = client.next_data_filename("t", schema="s")
        stem = Path(name).stem
        assert len(stem) == 20
        assert stem.isdigit()
