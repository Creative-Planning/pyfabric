"""Tests for the high-level ``OpenMirrorClient.write_rows`` API,
``RowMarker`` enum, and offline ``assert_schema_compat`` check.

Together these make the data-plane safe to call from a producer:

- ``RowMarker`` provides typed constants for ``__rowMarker__`` values
  (the int-magic mirror protocol).
- ``write_rows`` builds a parquet, stamps ``__rowMarker__`` if a
  ``mode`` is given, validates ``__rowMarker__`` is the last column,
  and uploads.
- ``assert_schema_compat`` catches the ``SchemaMergeFailure`` gotcha
  (silent producer schema drift) **before** the parquet hits the
  mirror.

All tests use real pyarrow + a mocked DFS layer (``upload_file``,
``list_paths``). No network.
"""

from __future__ import annotations

import io
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyfabric.data.open_mirror import (
    OpenMirrorClient,
    OpenMirrorSchemaIncompatible,
    RowMarker,
    assert_schema_compat,
)

WS = "00000000-0000-0000-0000-0000000000aa"
MIRROR = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def fake_credential():
    class _FakeCred:
        storage_token = "fake-token"

    return _FakeCred()


# ── RowMarker enum ──────────────────────────────────────────────────────────


class TestRowMarker:
    def test_values_match_mirror_protocol(self):
        assert RowMarker.INSERT == 0
        assert RowMarker.UPDATE == 1
        assert RowMarker.DELETE == 2
        assert RowMarker.UPSERT == 4

    def test_is_int_enum(self):
        # Subclassing IntEnum lets RowMarker.X be used as an int directly
        # in pyarrow array construction without a manual int() call.
        assert int(RowMarker.UPSERT) == 4
        assert RowMarker.UPSERT + 1 == 5  # arithmetic works


# ── assert_schema_compat ────────────────────────────────────────────────────


def _schema_with(*fields: tuple[str, pa.DataType, bool]) -> pa.Schema:
    """``[(name, type, nullable), ...]`` → pa.Schema."""
    return pa.schema([pa.field(n, t, nullable=nul) for n, t, nul in fields])


class TestAssertSchemaCompat:
    def test_identical_schemas_no_raise(self):
        s = _schema_with(
            ("id", pa.string(), False),
            ("name", pa.string(), True),
            ("__rowMarker__", pa.int32(), True),
        )
        assert_schema_compat(s, s)  # no raise

    def test_additive_nullable_column_no_raise(self):
        old = _schema_with(
            ("id", pa.string(), False),
            ("name", pa.string(), True),
            ("__rowMarker__", pa.int32(), True),
        )
        new = _schema_with(
            ("id", pa.string(), False),
            ("name", pa.string(), True),
            ("color", pa.string(), True),  # added before __rowMarker__
            ("__rowMarker__", pa.int32(), True),
        )
        assert_schema_compat(old, new)  # no raise

    def test_type_change_raises(self):
        old = _schema_with(
            ("id", pa.string(), False),
            ("posts", pa.int32(), True),
            ("__rowMarker__", pa.int32(), True),
        )
        new = _schema_with(
            ("id", pa.string(), False),
            ("posts", pa.int64(), True),  # widened
            ("__rowMarker__", pa.int32(), True),
        )
        with pytest.raises(OpenMirrorSchemaIncompatible) as exc:
            assert_schema_compat(old, new)
        assert "posts" in str(exc.value)

    def test_added_non_nullable_raises(self):
        old = _schema_with(
            ("id", pa.string(), False),
            ("__rowMarker__", pa.int32(), True),
        )
        new = _schema_with(
            ("id", pa.string(), False),
            ("color", pa.string(), False),  # NEW, NOT NULL
            ("__rowMarker__", pa.int32(), True),
        )
        with pytest.raises(OpenMirrorSchemaIncompatible) as exc:
            assert_schema_compat(old, new)
        assert "color" in str(exc.value)

    def test_row_marker_not_last_raises(self):
        old = _schema_with(
            ("id", pa.string(), False),
            ("__rowMarker__", pa.int32(), True),
        )
        new = _schema_with(
            ("id", pa.string(), False),
            ("__rowMarker__", pa.int32(), True),
            ("color", pa.string(), True),  # AFTER row marker
        )
        with pytest.raises(OpenMirrorSchemaIncompatible) as exc:
            assert_schema_compat(old, new)
        assert "__rowMarker__" in str(exc.value)

    def test_multi_drift_reports_all(self):
        old = _schema_with(
            ("id", pa.string(), False),
            ("posts", pa.int32(), True),
            ("__rowMarker__", pa.int32(), True),
        )
        new = _schema_with(
            ("id", pa.string(), False),
            ("posts", pa.int64(), True),  # type changed
            ("forced", pa.string(), False),  # non-nullable add
            ("__rowMarker__", pa.int32(), True),
        )
        with pytest.raises(OpenMirrorSchemaIncompatible) as exc:
            assert_schema_compat(old, new)
        msg = str(exc.value)
        assert "posts" in msg
        assert "forced" in msg

    def test_dropped_column_no_raise(self):
        """Removing a column is non-destructive at the mirror level —
        Fabric keeps unioned columns and NULLs new rows. Still allowed."""
        old = _schema_with(
            ("id", pa.string(), False),
            ("posts", pa.int32(), True),
            ("__rowMarker__", pa.int32(), True),
        )
        new = _schema_with(
            ("id", pa.string(), False),
            ("__rowMarker__", pa.int32(), True),
        )
        assert_schema_compat(old, new)  # no raise


# ── write_rows ──────────────────────────────────────────────────────────────


def _table_inserts(n: int = 3) -> pa.Table:
    """A small arrow table with the protocol-mandatory shape (no marker yet)."""
    return pa.table(
        {
            "id": [f"r{i}" for i in range(n)],
            "name": [f"n{i}" for i in range(n)],
        }
    )


class TestWriteRowsModeStamping:
    @pytest.mark.parametrize(
        "mode, expected_marker",
        [
            ("insert", 0),
            ("update", 1),
            ("delete", 2),
            ("upsert", 4),
        ],
    )
    def test_mode_stamps_every_row_with_correct_marker(
        self, fake_credential, mode, expected_marker
    ):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        captured: dict[str, bytes] = {}

        def capture(token, ws_id, item_id, path, data, **kwargs):
            captured["data"] = data
            captured["path"] = path

        with (
            patch("pyfabric.data.open_mirror.onelake.list_paths", return_value=[]),
            patch("pyfabric.data.open_mirror.onelake.upload_file", side_effect=capture),
        ):
            client.write_rows("t", _table_inserts(3), schema="s", mode=mode)
        # Round-trip the captured bytes back through pyarrow.
        round_trip = pq.read_table(io.BytesIO(captured["data"]))
        assert round_trip.column_names[-1] == "__rowMarker__"
        markers = round_trip.column("__rowMarker__").to_pylist()
        assert markers == [expected_marker, expected_marker, expected_marker]

    def test_returns_full_remote_path(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with (
            patch("pyfabric.data.open_mirror.onelake.list_paths", return_value=[]),
            patch("pyfabric.data.open_mirror.onelake.upload_file"),
        ):
            remote = client.write_rows(
                "t", _table_inserts(1), schema="s", mode="insert"
            )
        assert remote == "Files/LandingZone/s.schema/t/00000000000000000001.parquet"


class TestWriteRowsCallerStamped:
    def test_no_mode_requires_row_marker_present_and_last(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        # Caller adds __rowMarker__ explicitly; some rows insert, some delete.
        tbl = pa.table(
            {
                "id": ["a", "b"],
                "name": ["alpha", None],
                "__rowMarker__": [0, 2],  # insert, delete
            }
        )
        with (
            patch("pyfabric.data.open_mirror.onelake.list_paths", return_value=[]),
            patch("pyfabric.data.open_mirror.onelake.upload_file"),
        ):
            client.write_rows("t", tbl, schema="s")  # no raise

    def test_no_mode_and_missing_row_marker_raises(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        with (
            patch("pyfabric.data.open_mirror.onelake.upload_file") as up,
            pytest.raises(ValueError, match="__rowMarker__"),
        ):
            client.write_rows("t", _table_inserts(1), schema="s")
        up.assert_not_called()

    def test_no_mode_and_row_marker_not_last_raises(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        tbl = pa.table(
            {
                "id": ["a"],
                "__rowMarker__": [0],
                "extra": ["x"],  # AFTER marker — bug
            }
        )
        with (
            patch("pyfabric.data.open_mirror.onelake.upload_file") as up,
            pytest.raises(ValueError, match="last column"),
        ):
            client.write_rows("t", tbl, schema="s")
        up.assert_not_called()


class TestWriteRowsAmbiguous:
    def test_mode_with_existing_row_marker_raises(self, fake_credential):
        """Caller passed both ``mode`` and a column called __rowMarker__ —
        ambiguous which to honor; raise rather than silently overwrite."""
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        tbl = pa.table({"id": ["a"], "__rowMarker__": [0]})
        with (
            patch("pyfabric.data.open_mirror.onelake.upload_file") as up,
            pytest.raises(ValueError, match="__rowMarker__"),
        ):
            client.write_rows("t", tbl, schema="s", mode="upsert")
        up.assert_not_called()


class TestWriteRowsSchemaCheck:
    def test_expected_schema_mismatch_raises_before_upload(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        expected = _schema_with(
            ("id", pa.string(), False),
            ("name", pa.string(), True),
            ("__rowMarker__", pa.int32(), True),
        )
        # New file widens posts: int32 → int64. Mismatch.
        new_table = pa.table(
            {
                "id": ["a"],
                "name": ["alpha"],
                "posts": pa.array([1], type=pa.int64()),
                "__rowMarker__": pa.array([0], type=pa.int32()),
            }
        )
        # The widening "posts" is actually an additive nullable column (it
        # didn't exist in old). It should pass. Now make it a real type
        # change instead.
        old_with_posts = _schema_with(
            ("id", pa.string(), False),
            ("name", pa.string(), True),
            ("posts", pa.int32(), True),  # was int32
            ("__rowMarker__", pa.int32(), True),
        )
        with (
            patch("pyfabric.data.open_mirror.onelake.upload_file") as up,
            pytest.raises(OpenMirrorSchemaIncompatible),
        ):
            client.write_rows(
                "t",
                new_table,
                schema="s",
                expected_schema=old_with_posts,
            )
        up.assert_not_called()
        del expected  # silence unused

    def test_expected_schema_match_uploads(self, fake_credential):
        client = OpenMirrorClient(fake_credential, WS, MIRROR)
        expected = _schema_with(
            ("id", pa.string(), False),
            ("__rowMarker__", pa.int32(), True),
        )
        tbl = pa.table({"id": ["a"], "__rowMarker__": pa.array([0], type=pa.int32())})
        with (
            patch("pyfabric.data.open_mirror.onelake.list_paths", return_value=[]),
            patch("pyfabric.data.open_mirror.onelake.upload_file") as up,
        ):
            client.write_rows("t", tbl, schema="s", expected_schema=expected)
        up.assert_called_once()
