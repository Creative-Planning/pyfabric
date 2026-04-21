"""Tests for ``LocalLakehouse.push_table`` / ``push_all`` empty-delta behavior.

DirectLake semantic models bind to ``abfss://.../Tables/<schema>/<table>``
paths and fail refresh when the delta log is missing. The default must
therefore be "always write, even for zero-row tables" — callers can opt
into the old skip behavior with ``skip_empty=True``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pyfabric.client.auth import FabricCredential
from pyfabric.data.local_lakehouse import LocalLakehouse


@pytest.fixture
def lh(tmp_path):
    db_path = tmp_path / "lh.duckdb"
    lh = LocalLakehouse(
        db_path=db_path,
        ws_id="ws-uuid",
        lh_id="lh-uuid",
        schema="dbo",
    )
    # Two tables: one empty, one non-empty.
    lh.conn.execute("CREATE TABLE dbo.empty_tbl (id INTEGER, name VARCHAR)")
    lh.conn.execute(
        "CREATE TABLE dbo.full_tbl AS "
        "SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'"
    )
    yield lh
    lh.close()


@pytest.fixture
def fake_credential():
    return MagicMock(spec=FabricCredential)


class TestPushTableEmptyDefault:
    def test_push_table_writes_empty_delta_by_default(self, lh, fake_credential):
        """Default behavior: zero-row tables DO get written (delta log
        created). DirectLake refresh depends on this."""
        with patch("pyfabric.data.lakehouse.write_table") as mock_write:
            mock_write.return_value = MagicMock(row_count=0)
            result = lh.push_table(fake_credential, "empty_tbl")
        assert result == 0
        mock_write.assert_called_once()

    def test_push_table_skips_empty_when_skip_empty_true(self, lh, fake_credential):
        """Opt-in skip: when skip_empty=True, zero-row tables don't write."""
        with patch("pyfabric.data.lakehouse.write_table") as mock_write:
            result = lh.push_table(fake_credential, "empty_tbl", skip_empty=True)
        assert result == 0
        mock_write.assert_not_called()

    def test_push_table_writes_non_empty_table(self, lh, fake_credential):
        """Non-empty tables always write — both default + skip_empty paths."""
        with patch("pyfabric.data.lakehouse.write_table") as mock_write:
            mock_write.return_value = MagicMock(row_count=2)
            result = lh.push_table(fake_credential, "full_tbl")
        assert result == 2
        mock_write.assert_called_once()


class TestPushAllEmptyDefault:
    def test_push_all_writes_empty_tables_by_default(self, lh, fake_credential):
        """Default: push_all writes every table — empty ones get a zero-row
        delta so their path is DirectLake-compatible."""
        with patch("pyfabric.data.lakehouse.write_table") as mock_write:
            mock_write.return_value = MagicMock(row_count=0)
            results = lh.push_all(fake_credential)
        assert set(results.keys()) == {"empty_tbl", "full_tbl"}
        # Both tables triggered a write call
        assert mock_write.call_count == 2

    def test_push_all_skips_empty_when_opted_in(self, lh, fake_credential):
        """skip_empty=True restores the old behavior — empty tables are
        omitted from the result entirely."""
        with patch("pyfabric.data.lakehouse.write_table") as mock_write:
            mock_write.return_value = MagicMock(row_count=2)
            results = lh.push_all(fake_credential, skip_empty=True)
        assert set(results.keys()) == {"full_tbl"}
        assert mock_write.call_count == 1

    def test_push_all_respects_tables_filter(self, lh, fake_credential):
        """Explicit `tables` subset still honored, interacts cleanly with
        the empty-handling default."""
        with patch("pyfabric.data.lakehouse.write_table") as mock_write:
            mock_write.return_value = MagicMock(row_count=0)
            results = lh.push_all(fake_credential, tables=["empty_tbl"])
        assert set(results.keys()) == {"empty_tbl"}
        assert mock_write.call_count == 1
