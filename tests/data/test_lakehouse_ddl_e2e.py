"""Live DDL e2e tests against a real (validation-workspace) lakehouse.

The mocked suite in ``test_lakehouse_ddl.py`` covers the DFS-seam
behavior; nothing there ever talks to real OneLake. These tests replay
the same operations live to catch DFS-protocol drift (rename semantics,
``recursive=true`` behavior) and auth-surface bugs that only appear
against a real workspace. See issue #52.

Gating (both required; tests skip cleanly otherwise):

- ``PYFABRIC_TEST_WORKSPACE_ID`` — validation workspace GUID
- ``PYFABRIC_TEST_LAKEHOUSE_ID`` — a schema-enabled lakehouse in it

Isolation: every run works inside a throwaway ``it_<hex8>`` schema and
drops it in teardown regardless of pass/fail, so a crashed run leaves at
most one orphaned ``it_*`` schema (safe to drop manually — see
docs/testing.md).
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterator

import pytest

pa = pytest.importorskip("pyarrow")

from pyfabric.data import lakehouse  # noqa: E402


@pytest.mark.e2e
class TestLakehouseDdlLive:
    @pytest.fixture(autouse=True)
    def _require_live_lakehouse(
        self, real_workspace_id: str | None, real_lakehouse_id: str | None
    ) -> Iterator[None]:
        if real_workspace_id is None:
            pytest.skip("PYFABRIC_TEST_WORKSPACE_ID not set")
        if real_lakehouse_id is None:
            pytest.skip("PYFABRIC_TEST_LAKEHOUSE_ID not set")
        from pyfabric.client.auth import AuthError, FabricCredential

        self.ws_id = real_workspace_id
        self.lh_id = real_lakehouse_id
        try:
            self.cred = FabricCredential()
            existing = lakehouse.list_schemas(self.cred, self.ws_id, self.lh_id)
        except AuthError as e:
            pytest.skip(f"credential unavailable for live DDL tests: {e}")
        if "dbo" not in existing:
            pytest.skip(
                "lakehouse does not look schema-enabled (no schemas listed); "
                "create a schema-enabled lakehouse in the validation "
                "workspace and point PYFABRIC_TEST_LAKEHOUSE_ID at it"
            )
        self.schema = f"it_{uuid.uuid4().hex[:8]}"
        try:
            yield
        finally:
            # Best-effort cleanup of both the primary and the renamed
            # schema so a crashed test never leaves litter behind.
            for schema in (self.schema, f"{self.schema}b"):
                with contextlib.suppress(Exception):
                    lakehouse.drop_schema(self.cred, self.ws_id, self.lh_id, schema)

    def _seed(self, table: str, schema: str | None = None) -> None:
        data = pa.table({"id": ["a", "b"], "n": [1, 2]})
        lakehouse.write_table(
            self.cred,
            self.ws_id,
            self.lh_id,
            table,
            data,
            schema=schema or self.schema,
        )

    def test_full_ddl_lifecycle(self) -> None:
        """One serial pass over the whole DDL surface (issue #52 matrix).

        Serial by design: each op builds on the last, so one seeded
        Delta table exercises create → list → rename_table →
        rename_schema → delete_table → drop_schema without re-seeding.
        """
        # write_table creates the schema implicitly.
        self._seed("t_alpha")
        assert self.schema in lakehouse.list_schemas(self.cred, self.ws_id, self.lh_id)
        assert lakehouse.list_tables(
            self.cred, self.ws_id, self.lh_id, schema=self.schema
        ) == ["t_alpha"]

        # rename_table: new name visible, old name gone.
        lakehouse.rename_table(
            self.cred,
            self.ws_id,
            self.lh_id,
            "t_alpha",
            "t_beta",
            schema=self.schema,
        )
        tables = lakehouse.list_tables(
            self.cred, self.ws_id, self.lh_id, schema=self.schema
        )
        assert tables == ["t_beta"]

        # rename_schema: both tables move.
        self._seed("t_gamma")
        renamed = f"{self.schema}b"
        lakehouse.rename_schema(self.cred, self.ws_id, self.lh_id, self.schema, renamed)
        schemas = lakehouse.list_schemas(self.cred, self.ws_id, self.lh_id)
        assert renamed in schemas
        assert self.schema not in schemas
        assert sorted(
            lakehouse.list_tables(self.cred, self.ws_id, self.lh_id, schema=renamed)
        ) == ["t_beta", "t_gamma"]

        # delete_table: gone from the listing.
        lakehouse.delete_table(
            self.cred, self.ws_id, self.lh_id, "t_beta", schema=renamed
        )
        assert lakehouse.list_tables(
            self.cred, self.ws_id, self.lh_id, schema=renamed
        ) == ["t_gamma"]

        # drop_schema: schema disappears entirely.
        lakehouse.drop_schema(self.cred, self.ws_id, self.lh_id, renamed)
        assert renamed not in lakehouse.list_schemas(self.cred, self.ws_id, self.lh_id)

    def test_rename_table_same_name_raises(self) -> None:
        self._seed("t_same")
        with pytest.raises(ValueError):
            lakehouse.rename_table(
                self.cred,
                self.ws_id,
                self.lh_id,
                "t_same",
                "t_same",
                schema=self.schema,
            )
