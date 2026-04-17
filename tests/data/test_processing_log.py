"""Tests for pyfabric.data.processing_log.ProcessingLog."""

import pytest

from pyfabric.data.local_lakehouse import LocalLakehouse
from pyfabric.data.processing_log import (
    DEFAULT_TABLE,
    STATUS_FAILURE,
    STATUS_SUCCESS,
    ProcessingLog,
)
from pyfabric.data.schema import Col, TableDef


@pytest.fixture
def lake(tmp_path):
    return LocalLakehouse(
        db_path=tmp_path / "plog.duckdb",
        ws_id="ws",
        lh_id="lh",
        schema="ddb",
    )


class TestSetup:
    def test_creates_table_on_init(self, lake):
        plog = ProcessingLog(lake)
        assert DEFAULT_TABLE.name in lake.table_names()
        assert plog.table_def is DEFAULT_TABLE

    def test_rejects_tabledef_without_source_path(self, lake):
        bad = TableDef(
            name="bad_log",
            columns=(
                Col("file", "string", nullable=False, pk=True),
                Col("status", "string", nullable=False),
            ),
        )
        with pytest.raises(ValueError, match="source_path"):
            ProcessingLog(lake, table_def=bad)

    def test_rejects_tabledef_where_source_path_not_pk(self, lake):
        bad = TableDef(
            name="bad_log",
            columns=(
                Col("source_path", "string", nullable=False),  # not pk
                Col("status", "string", nullable=False),
            ),
        )
        with pytest.raises(ValueError, match="primary-key"):
            ProcessingLog(lake, table_def=bad)


class TestSuccessFlow:
    def test_record_success_then_is_processed(self, lake):
        plog = ProcessingLog(lake)
        plog.record_success("a.pdf", content_hash="h1", rows_written=10)
        assert plog.is_processed("a.pdf") is True

    def test_is_processed_false_for_unseen(self, lake):
        plog = ProcessingLog(lake)
        assert plog.is_processed("never.pdf") is False

    def test_content_hash_mismatch_means_not_processed(self, lake):
        plog = ProcessingLog(lake)
        plog.record_success("a.pdf", content_hash="h1", rows_written=1)
        assert plog.is_processed("a.pdf", content_hash="h1") is True
        assert plog.is_processed("a.pdf", content_hash="h2") is False


class TestFailureFlow:
    def test_failure_is_not_processed(self, lake):
        plog = ProcessingLog(lake)
        plog.record_failure("a.pdf", content_hash="h1", error="parse error")
        assert plog.is_processed("a.pdf") is False

    def test_failures_listing(self, lake):
        plog = ProcessingLog(lake)
        plog.record_failure("a.pdf", error="bad")
        plog.record_success("b.pdf", rows_written=1)
        plog.record_failure("c.pdf", error="bad2")
        failures = plog.failures()
        assert {r["source_path"] for r in failures} == {"a.pdf", "c.pdf"}
        assert {r["status"] for r in failures} == {STATUS_FAILURE}

    def test_retry_after_failure_overwrites(self, lake):
        plog = ProcessingLog(lake)
        plog.record_failure("a.pdf", error="bad")
        plog.record_success("a.pdf", rows_written=5)
        assert plog.is_processed("a.pdf") is True
        assert plog.failures() == []

    def test_error_summary_truncated(self, lake):
        plog = ProcessingLog(lake)
        big = "X" * 10000
        plog.record_failure("a.pdf", error=big)
        row = plog.failures()[0]
        assert len(row["error_summary"]) == 4000


class TestUpsertSemantics:
    def test_second_success_updates_not_duplicates(self, lake):
        plog = ProcessingLog(lake)
        plog.record_success("a.pdf", content_hash="h1", rows_written=1)
        plog.record_success("a.pdf", content_hash="h2", rows_written=2)
        rows = lake.conn.execute(
            "SELECT content_hash, rows_written FROM ddb.processing_log "
            "WHERE source_path = 'a.pdf'"
        ).fetchall()
        assert rows == [("h2", 2)]

    def test_status_column_is_success_constant(self):
        assert STATUS_SUCCESS == "success"
        assert STATUS_FAILURE == "failure"
