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


class TestWriteTableWithoutPandas:
    """Regression coverage for CI divergence: dev venvs typically have pandas
    installed (transitively, via notebook tooling); the CI test env does not.
    A test that passes locally but fails on CI because pandas is missing is
    exactly the bug this locks in — so pre-checkin catches it before push.
    """

    def test_arrow_only_write_table_works_without_pandas(self, monkeypatch):
        import builtins
        import importlib
        import sys

        real_import = builtins.__import__

        def blocked_import(name, *args, **kwargs):
            if name == "pandas" or name.startswith("pandas."):
                raise ImportError("simulated: pandas not installed")
            return real_import(name, *args, **kwargs)

        # Uncache pandas so the next import goes through blocked_import.
        for mod in [
            k for k in list(sys.modules) if k == "pandas" or k.startswith("pandas.")
        ]:
            monkeypatch.delitem(sys.modules, mod, raising=False)
        monkeypatch.setattr(builtins, "__import__", blocked_import)

        # Reload the module so its try/except import block re-runs without pandas.
        from pyfabric.data import lakehouse as lh_mod

        importlib.reload(lh_mod)

        class _FakeCred:
            storage_token = "fake-token"

        tbl = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "ts": pa.array(
                    [datetime(2026, 4, 17, 10, 0, 0)], type=pa.timestamp("us")
                ),
            }
        )

        with structlog.testing.capture_logs() as events:
            result = lh_mod.write_table(
                _FakeCred(),
                ws_id="00000000-0000-0000-0000-000000000000",
                lh_id="00000000-0000-0000-0000-000000000000",
                table_name="t",
                data=tbl,
                schema="dbo",
                dry_run=True,
            )

        assert result.row_count == 1
        assert result.dry_run is True
        # Warning must still fire without pandas — it uses only pyarrow reflection.
        assert any(
            e.get("log_level") == "warning" and "TIMESTAMP_NTZ" in e.get("event", "")
            for e in events
        )


class TestWriteTableMergeMode:
    """Covers the four semantics of ``mode="merge"``:

      1. update — source rows whose merge_keys match a destination row
         replace that destination row.
      2. insert — source rows whose merge_keys don't match any destination
         row are added.
      3. preserve — destination rows whose merge_keys aren't in the source
         are left untouched (this is the value-add over overwrite).
      4. no-op — empty source is a no-op (no wasted Delta version commit).

    Plus the validation-error paths (missing merge_keys, unknown mode).

    Tests round-trip real Delta tables on the local filesystem via
    ``tmp_path``. We monkeypatch ``abfss_url`` to return a ``file://``
    path so write_table's existing call graph runs end-to-end without
    a Fabric round-trip. deltalake honors file:// URLs transparently.
    """

    @pytest.fixture
    def fake_credential(self):
        class _FakeCred:
            storage_token = "fake-token"

        return _FakeCred()

    @pytest.fixture
    def patched_abfss(self, monkeypatch, tmp_path):
        """Redirect abfss_url to a file:// path under tmp_path so writes
        land on the local filesystem instead of hitting OneLake.
        """
        from pyfabric.data import lakehouse as lh_mod

        def _fake_abfss(ws_id, lh_id, table_path):
            local = tmp_path / table_path.replace("/", "_")
            return local.as_uri()

        monkeypatch.setattr(lh_mod, "abfss_url", _fake_abfss)
        return tmp_path

    @staticmethod
    def _read_back(target_uri: str) -> list[dict]:
        """Read the Delta table at ``target_uri`` as a list of row dicts."""
        from deltalake import DeltaTable

        dt = DeltaTable(target_uri)
        return dt.to_pyarrow_table().to_pylist()

    def _target_uri(self, tmp_path, table_name: str, schema: str = "dbo") -> str:
        """Recompute the file:// URI the patched abfss_url produces."""
        return (tmp_path / f"Tables_{schema}_{table_name}").as_uri()

    def test_merge_without_keys_raises(self, fake_credential, patched_abfss):
        tbl = pa.table({"id": pa.array([1], type=pa.int64())})
        with pytest.raises(ValueError, match="merge_keys"):
            write_table(
                fake_credential,
                ws_id="00000000-0000-0000-0000-000000000000",
                lh_id="00000000-0000-0000-0000-000000000000",
                table_name="t",
                data=tbl,
                schema="dbo",
                mode="merge",
            )

    def test_unknown_mode_raises(self, fake_credential, patched_abfss):
        tbl = pa.table({"id": pa.array([1], type=pa.int64())})
        with pytest.raises(ValueError, match="Invalid mode"):
            write_table(
                fake_credential,
                ws_id="00000000-0000-0000-0000-000000000000",
                lh_id="00000000-0000-0000-0000-000000000000",
                table_name="t",
                data=tbl,
                schema="dbo",
                mode="upsert",  # not a valid mode
            )

    def test_empty_source_is_noop(self, fake_credential, patched_abfss):
        empty = pa.table({"id": pa.array([], type=pa.int64())})
        result = write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=empty,
            schema="dbo",
            mode="merge",
            merge_keys=["id"],
        )
        assert result.row_count == 0
        # No Delta table should have been created — empty merge is a no-op.
        target = self._target_uri(patched_abfss, "t")
        from deltalake import DeltaTable

        assert not DeltaTable.is_deltatable(target)

    def test_first_write_falls_through_to_overwrite(
        self, fake_credential, patched_abfss
    ):
        """When the destination doesn't exist yet, merge should create it."""
        src = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "val": pa.array(["a", "b", "c"], type=pa.string()),
            }
        )
        result = write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=src,
            schema="dbo",
            mode="merge",
            merge_keys=["id"],
        )
        assert result.row_count == 3
        rows = sorted(
            self._read_back(self._target_uri(patched_abfss, "t")), key=lambda r: r["id"]
        )
        assert rows == [
            {"id": 1, "val": "a"},
            {"id": 2, "val": "b"},
            {"id": 3, "val": "c"},
        ]

    def test_merge_updates_matching_rows(self, fake_credential, patched_abfss):
        # Seed destination with rows 1,2,3 then merge new values for 2,3.
        seed = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "val": pa.array(["a", "b", "c"], type=pa.string()),
            }
        )
        write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=seed,
            schema="dbo",
            mode="overwrite",
        )

        update = pa.table(
            {
                "id": pa.array([2, 3], type=pa.int64()),
                "val": pa.array(["B_NEW", "C_NEW"], type=pa.string()),
            }
        )
        write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=update,
            schema="dbo",
            mode="merge",
            merge_keys=["id"],
        )

        rows = sorted(
            self._read_back(self._target_uri(patched_abfss, "t")), key=lambda r: r["id"]
        )
        # id=1 preserved untouched; id=2 and 3 updated.
        assert rows == [
            {"id": 1, "val": "a"},
            {"id": 2, "val": "B_NEW"},
            {"id": 3, "val": "C_NEW"},
        ]

    def test_merge_inserts_new_rows(self, fake_credential, patched_abfss):
        seed = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "val": pa.array(["a", "b"], type=pa.string()),
            }
        )
        write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=seed,
            schema="dbo",
            mode="overwrite",
        )

        new = pa.table(
            {
                "id": pa.array([3, 4], type=pa.int64()),
                "val": pa.array(["c", "d"], type=pa.string()),
            }
        )
        write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=new,
            schema="dbo",
            mode="merge",
            merge_keys=["id"],
        )

        rows = sorted(
            self._read_back(self._target_uri(patched_abfss, "t")), key=lambda r: r["id"]
        )
        assert rows == [
            {"id": 1, "val": "a"},
            {"id": 2, "val": "b"},
            {"id": 3, "val": "c"},
            {"id": 4, "val": "d"},
        ]

    def test_merge_mixed_insert_update_preserve(self, fake_credential, patched_abfss):
        """The headline case: destination {A, B} + source {A', C} → {A', B, C}."""
        seed = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "val": pa.array(["A", "B"], type=pa.string()),
            }
        )
        write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=seed,
            schema="dbo",
            mode="overwrite",
        )

        mix = pa.table(
            {
                "id": pa.array([1, 3], type=pa.int64()),
                "val": pa.array(["A_PRIME", "C"], type=pa.string()),
            }
        )
        write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=mix,
            schema="dbo",
            mode="merge",
            merge_keys=["id"],
        )

        rows = sorted(
            self._read_back(self._target_uri(patched_abfss, "t")), key=lambda r: r["id"]
        )
        assert rows == [
            {"id": 1, "val": "A_PRIME"},  # updated
            {"id": 2, "val": "B"},  # preserved
            {"id": 3, "val": "C"},  # inserted
        ]

    def test_merge_with_composite_key(self, fake_credential, patched_abfss):
        """Composite merge keys: predicate joins multiple columns with AND."""
        seed = pa.table(
            {
                "year": pa.array([2025, 2025, 2026], type=pa.int32()),
                "pid": pa.array(["A", "B", "A"], type=pa.string()),
                "val": pa.array(["x", "y", "z"], type=pa.string()),
            }
        )
        write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=seed,
            schema="dbo",
            mode="overwrite",
        )

        # Update only (year=2025, pid=A). Leave 2025-B and 2026-A alone.
        update = pa.table(
            {
                "year": pa.array([2025], type=pa.int32()),
                "pid": pa.array(["A"], type=pa.string()),
                "val": pa.array(["X_NEW"], type=pa.string()),
            }
        )
        write_table(
            fake_credential,
            ws_id="00000000-0000-0000-0000-000000000000",
            lh_id="00000000-0000-0000-0000-000000000000",
            table_name="t",
            data=update,
            schema="dbo",
            mode="merge",
            merge_keys=["year", "pid"],
        )

        rows = sorted(
            self._read_back(self._target_uri(patched_abfss, "t")),
            key=lambda r: (r["year"], r["pid"]),
        )
        assert rows == [
            {"year": 2025, "pid": "A", "val": "X_NEW"},
            {"year": 2025, "pid": "B", "val": "y"},
            {"year": 2026, "pid": "A", "val": "z"},
        ]
