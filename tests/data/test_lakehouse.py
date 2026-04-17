"""Tests for pyfabric.data.lakehouse.write_table."""

from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
import pytest
import structlog

from pyfabric.data.lakehouse import write_table


@pytest.fixture
def fake_credential(monkeypatch):
    """A FabricCredential whose storage_token property returns a fixed value."""

    class _FakeCred:
        storage_token = "fake-token"

    return _FakeCred()


class TestWriteTableTimestampNtzWarning:
    """Fabric's SQL analytics endpoint rejects Delta TIMESTAMP_NTZ columns.
    PyArrow timestamps without a tz map to TIMESTAMP_NTZ in Delta, so write_table
    should warn when it sees one — giving callers a chance to add tz='UTC' or
    cast to string before the write hits a downstream consumer that can't read it.
    """

    def _run_write(self, arrow_table: pa.Table, cred) -> list[dict]:
        """Call write_table in dry_run mode and return the structlog events."""
        with structlog.testing.capture_logs() as events:
            write_table(
                cred,
                ws_id="00000000-0000-0000-0000-000000000000",
                lh_id="00000000-0000-0000-0000-000000000000",
                table_name="t",
                data=arrow_table,
                schema="dbo",
                dry_run=True,
            )
        return events

    def _has_ntz_warning(self, events: list[dict]) -> bool:
        return any(
            e.get("log_level") == "warning" and "TIMESTAMP_NTZ" in e.get("event", "")
            for e in events
        )

    def test_naive_timestamp_emits_warning(self, fake_credential):
        naive = [datetime(2026, 4, 17, 10, 0, 0), datetime(2026, 4, 17, 11, 0, 0)]
        tbl = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "extracted_at": pa.array(naive, type=pa.timestamp("us")),
            }
        )
        events = self._run_write(tbl, fake_credential)
        assert self._has_ntz_warning(events)

    def test_tz_aware_timestamp_does_not_warn(self, fake_credential):
        utc = [
            datetime(2026, 4, 17, 10, 0, 0, tzinfo=UTC),
            datetime(2026, 4, 17, 11, 0, 0, tzinfo=UTC),
        ]
        tbl = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "extracted_at": pa.array(utc, type=pa.timestamp("us", tz="UTC")),
            }
        )
        events = self._run_write(tbl, fake_credential)
        assert not self._has_ntz_warning(events)

    def test_no_timestamp_columns_does_not_warn(self, fake_credential):
        tbl = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "name": pa.array(["a", "b"], type=pa.string()),
            }
        )
        events = self._run_write(tbl, fake_credential)
        assert not self._has_ntz_warning(events)

    def test_string_column_named_extracted_at_does_not_warn(self, fake_credential):
        # A column named 'extracted_at' but typed as string is fine — the warning
        # must be type-driven, not name-driven.
        tbl = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "extracted_at": pa.array(
                    ["2026-04-17T10:00:00+00:00"], type=pa.string()
                ),
            }
        )
        events = self._run_write(tbl, fake_credential)
        assert not self._has_ntz_warning(events)
