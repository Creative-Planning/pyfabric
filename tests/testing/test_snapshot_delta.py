"""Tests for ``pyfabric.testing.fixtures.snapshot_delta``.

The helper pulls a slice of a OneLake delta table to a local parquet
fixture for offline tests. Live OneLake reads are out of scope here —
those would be exercised by the integration test harness gated on
``PYFABRIC_TEST_WS_ID`` (see issue #52). These tests verify the dispatch
logic, parameter passthrough, and the deltalake → SQL fallback for
``columnMapping`` / ``deletionVectors``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyfabric.testing.fixtures import snapshot_delta

ABFSS_URL = (
    "abfss://00000000-0000-0000-0000-0000000000aa@onelake.dfs.fabric.microsoft.com/"
    "00000000-0000-0000-0000-000000000001/Tables/bc2/dim_customer"
)


@pytest.fixture
def fake_credential():
    class _FakeCred:
        storage_token = "fake-token"

    return _FakeCred()


def _arrow_table(rows: int = 3) -> pa.Table:
    return pa.table({"id": list(range(rows)), "name": [f"r{i}" for i in range(rows)]})


# ── Happy path — deltalake succeeds ─────────────────────────────────────────


class TestDeltaLakePath:
    def test_writes_parquet_at_dest(self, tmp_path, fake_credential):
        dest = tmp_path / "fixture.parquet"
        with patch("pyfabric.testing.fixtures._read_delta_table") as mock_read:
            mock_read.return_value = _arrow_table(rows=5)
            written = snapshot_delta(fake_credential, source=ABFSS_URL, dest=dest)
        assert written == dest
        assert dest.exists()
        round_trip = pq.read_table(dest)
        assert round_trip.num_rows == 5
        assert round_trip.column_names == ["id", "name"]

    def test_max_rows_truncates_payload(self, tmp_path, fake_credential):
        dest = tmp_path / "fixture.parquet"
        with patch("pyfabric.testing.fixtures._read_delta_table") as mock_read:
            mock_read.return_value = _arrow_table(rows=100)
            snapshot_delta(fake_credential, source=ABFSS_URL, dest=dest, max_rows=10)
        assert pq.read_table(dest).num_rows == 10

    def test_filter_expr_passed_through_to_delta_reader(
        self, tmp_path, fake_credential
    ):
        dest = tmp_path / "fixture.parquet"
        with patch("pyfabric.testing.fixtures._read_delta_table") as mock_read:
            mock_read.return_value = _arrow_table(rows=2)
            snapshot_delta(
                fake_credential,
                source=ABFSS_URL,
                dest=dest,
                filter_expr=("Company", "=", "Example Mfg"),
            )
        # The reader saw the filter — we don't care here how the delta
        # library consumes it, only that we didn't drop the kwarg.
        assert mock_read.call_args.kwargs.get("filter_expr") == (
            "Company",
            "=",
            "Example Mfg",
        )

    def test_no_max_rows_writes_full_table(self, tmp_path, fake_credential):
        dest = tmp_path / "fixture.parquet"
        with patch("pyfabric.testing.fixtures._read_delta_table") as mock_read:
            mock_read.return_value = _arrow_table(rows=42)
            snapshot_delta(fake_credential, source=ABFSS_URL, dest=dest)
        assert pq.read_table(dest).num_rows == 42


# ── SQL endpoint fallback ───────────────────────────────────────────────────


class TestSqlFallback:
    def test_falls_back_when_delta_raises_columnmapping(
        self, tmp_path, fake_credential
    ):
        dest = tmp_path / "fixture.parquet"
        with (
            patch("pyfabric.testing.fixtures._read_delta_table") as mock_delta,
            patch("pyfabric.testing.fixtures._read_via_sql_endpoint") as mock_sql,
        ):
            mock_delta.side_effect = RuntimeError(
                "deltalake cannot read tables with columnMapping"
            )
            mock_sql.return_value = _arrow_table(rows=4)
            snapshot_delta(fake_credential, source=ABFSS_URL, dest=dest)
        assert pq.read_table(dest).num_rows == 4
        mock_sql.assert_called_once()

    def test_falls_back_when_delta_raises_deletion_vectors(
        self, tmp_path, fake_credential
    ):
        dest = tmp_path / "fixture.parquet"
        with (
            patch("pyfabric.testing.fixtures._read_delta_table") as mock_delta,
            patch("pyfabric.testing.fixtures._read_via_sql_endpoint") as mock_sql,
        ):
            mock_delta.side_effect = RuntimeError("deletionVectors not supported")
            mock_sql.return_value = _arrow_table(rows=1)
            snapshot_delta(fake_credential, source=ABFSS_URL, dest=dest)
        mock_sql.assert_called_once()

    def test_does_not_fall_back_on_unrelated_error(self, tmp_path, fake_credential):
        """Network / auth errors should propagate, not silently retry SQL."""
        dest = tmp_path / "fixture.parquet"
        with (
            patch("pyfabric.testing.fixtures._read_delta_table") as mock_delta,
            patch("pyfabric.testing.fixtures._read_via_sql_endpoint") as mock_sql,
        ):
            mock_delta.side_effect = RuntimeError("401 Unauthorized")
            with pytest.raises(RuntimeError, match="401"):
                snapshot_delta(fake_credential, source=ABFSS_URL, dest=dest)
        mock_sql.assert_not_called()

    def test_sql_fallback_receives_source_and_filter(self, tmp_path, fake_credential):
        dest = tmp_path / "fixture.parquet"
        with (
            patch("pyfabric.testing.fixtures._read_delta_table") as mock_delta,
            patch("pyfabric.testing.fixtures._read_via_sql_endpoint") as mock_sql,
        ):
            mock_delta.side_effect = RuntimeError("columnMapping")
            mock_sql.return_value = _arrow_table(rows=2)
            snapshot_delta(
                fake_credential,
                source=ABFSS_URL,
                dest=dest,
                filter_expr=("x", "=", "y"),
            )
        kwargs = mock_sql.call_args.kwargs
        assert kwargs["source"] == ABFSS_URL
        assert kwargs["filter_expr"] == ("x", "=", "y")


# ── Edge cases ──────────────────────────────────────────────────────────────


class TestEdges:
    def test_creates_parent_directory_if_missing(self, tmp_path, fake_credential):
        dest = tmp_path / "nested" / "deeper" / "fixture.parquet"
        with patch("pyfabric.testing.fixtures._read_delta_table") as mock_read:
            mock_read.return_value = _arrow_table(rows=1)
            snapshot_delta(fake_credential, source=ABFSS_URL, dest=dest)
        assert dest.exists()

    def test_zero_row_table_writes_empty_parquet(self, tmp_path, fake_credential):
        dest = tmp_path / "fixture.parquet"
        empty = pa.table({"id": pa.array([], type=pa.int64())})
        with patch("pyfabric.testing.fixtures._read_delta_table") as mock_read:
            mock_read.return_value = empty
            snapshot_delta(fake_credential, source=ABFSS_URL, dest=dest)
        assert pq.read_table(dest).num_rows == 0

    def test_dest_accepts_string_path(self, tmp_path, fake_credential):
        dest_str = str(tmp_path / "fixture.parquet")
        with patch("pyfabric.testing.fixtures._read_delta_table") as mock_read:
            mock_read.return_value = _arrow_table(rows=1)
            written = snapshot_delta(fake_credential, source=ABFSS_URL, dest=dest_str)
        assert isinstance(written, Path)
        assert written.exists()
