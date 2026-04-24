"""Tests for the lakehouse DDL-management helpers (issue #39).

Every schema migration today leaves orphaned tables behind because the
portal UI is the only way to drop/rename Delta tables in a Fabric
lakehouse. These helpers provide a programmatic path.

All tests mock the OneLake DFS layer at the ``pyfabric.data.onelake``
seam (``list_paths``, ``delete_path``, ``rename_path``). None of them
talk to the network.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# Attribute access on the module object rather than a ``from ... import``
# so that a sibling test's ``importlib.reload`` (see
# ``TestWriteTableWithoutPandas``) doesn't leave this module holding a
# stale class reference that no longer matches ``pytest.raises`` against
# what ``rename_schema`` actually raises at call time.
from pyfabric.data import lakehouse as _lh

delete_table = _lh.delete_table
drop_schema = _lh.drop_schema
list_schemas = _lh.list_schemas
list_tables = _lh.list_tables
rename_schema = _lh.rename_schema
rename_table = _lh.rename_table


WS = "00000000-0000-0000-0000-000000000000"
LH = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def fake_credential():
    class _FakeCred:
        storage_token = "fake-token"

    return _FakeCred()


def _path_entry(name: str, *, is_dir: bool) -> dict:
    """Shape matches the DFS filesystem API response."""
    return {"name": name, "isDirectory": "true" if is_dir else "false"}


# ── delete_table ────────────────────────────────────────────────────────────


class TestDeleteTable:
    def test_deletes_table_directory_recursively(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.delete_path") as mock_del:
            mock_del.return_value = True
            result = delete_table(fake_credential, WS, LH, "widgets", schema="dbo")
        assert result is True
        mock_del.assert_called_once()
        args, kwargs = mock_del.call_args
        assert args[0] == "fake-token"
        assert args[1] == WS
        assert args[2] == LH
        assert args[3] == "Tables/dbo/widgets"
        assert kwargs.get("recursive") is True

    def test_returns_false_when_table_missing(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.delete_path") as mock_del:
            mock_del.return_value = False
            result = delete_table(fake_credential, WS, LH, "missing")
        assert result is False


# ── rename_table ────────────────────────────────────────────────────────────


class TestRenameTable:
    def test_renames_within_same_schema(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.rename_path") as mock_ren:
            rename_table(fake_credential, WS, LH, "src", "dst", schema="dbo")
        mock_ren.assert_called_once_with(
            "fake-token",
            WS,
            LH,
            "Tables/dbo/src",
            "Tables/dbo/dst",
        )

    def test_refuses_when_src_equals_dst(self, fake_credential):
        with (
            patch("pyfabric.data.lakehouse.onelake.rename_path") as mock_ren,
            pytest.raises(ValueError, match="identical"),
        ):
            rename_table(fake_credential, WS, LH, "same", "same")
        mock_ren.assert_not_called()


# ── rename_schema ───────────────────────────────────────────────────────────


class TestRenameSchema:
    def test_moves_every_table_to_new_schema(self, fake_credential):
        with (
            patch("pyfabric.data.lakehouse.onelake.list_paths") as mock_ls,
            patch("pyfabric.data.lakehouse.onelake.rename_path") as mock_ren,
        ):
            mock_ls.return_value = [
                _path_entry(f"{LH}/Tables/old/a", is_dir=True),
                _path_entry(f"{LH}/Tables/old/b", is_dir=True),
                _path_entry(f"{LH}/Tables/old/c", is_dir=True),
            ]
            moved = rename_schema(fake_credential, WS, LH, "old", "new")
        assert sorted(moved) == ["a", "b", "c"]
        assert mock_ren.call_count == 3
        # Each rename sends src=Tables/old/<t>, dst=Tables/new/<t>
        src_dst_pairs = {
            (call.args[3], call.args[4]) for call in mock_ren.call_args_list
        }
        assert src_dst_pairs == {
            ("Tables/old/a", "Tables/new/a"),
            ("Tables/old/b", "Tables/new/b"),
            ("Tables/old/c", "Tables/new/c"),
        }

    def test_partial_failure_raises_structured_exception(self, fake_credential):
        with (
            patch("pyfabric.data.lakehouse.onelake.list_paths") as mock_ls,
            patch("pyfabric.data.lakehouse.onelake.rename_path") as mock_ren,
        ):
            mock_ls.return_value = [
                _path_entry(f"{LH}/Tables/old/a", is_dir=True),
                _path_entry(f"{LH}/Tables/old/b", is_dir=True),
                _path_entry(f"{LH}/Tables/old/c", is_dir=True),
            ]
            mock_ren.side_effect = [None, RuntimeError("409 conflict"), None]
            # Attribute lookup on the live module so the class identity
            # matches even if another test has reloaded the module since
            # import time.
            with pytest.raises(_lh.LakehouseRenameSchemaError) as exc:
                rename_schema(fake_credential, WS, LH, "old", "new")
        err = exc.value
        assert set(err.moved) == {"a", "c"}
        assert "b" in err.failed
        assert "409 conflict" in err.failed["b"]
        # Every table was attempted (no early abort).
        assert mock_ren.call_count == 3

    def test_no_tables_in_source_schema_returns_empty(self, fake_credential):
        with (
            patch("pyfabric.data.lakehouse.onelake.list_paths") as mock_ls,
            patch("pyfabric.data.lakehouse.onelake.rename_path") as mock_ren,
        ):
            mock_ls.return_value = []
            moved = rename_schema(fake_credential, WS, LH, "empty", "new")
        assert moved == []
        mock_ren.assert_not_called()

    def test_refuses_when_src_equals_dst(self, fake_credential):
        with (
            patch("pyfabric.data.lakehouse.onelake.rename_path") as mock_ren,
            pytest.raises(ValueError, match="identical"),
        ):
            rename_schema(fake_credential, WS, LH, "x", "x")
        mock_ren.assert_not_called()


# ── drop_schema ─────────────────────────────────────────────────────────────


class TestDropSchema:
    def test_deletes_schema_directory_recursively(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.delete_path") as mock_del:
            mock_del.return_value = True
            result = drop_schema(fake_credential, WS, LH, "old")
        assert result is True
        args, kwargs = mock_del.call_args
        assert args[3] == "Tables/old"
        assert kwargs.get("recursive") is True

    def test_returns_false_when_schema_missing(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.delete_path") as mock_del:
            mock_del.return_value = False
            result = drop_schema(fake_credential, WS, LH, "gone")
        assert result is False


# ── list_schemas ────────────────────────────────────────────────────────────


class TestListSchemas:
    def test_returns_schema_directory_basenames(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.list_paths") as mock_ls:
            mock_ls.return_value = [
                _path_entry(f"{LH}/Tables/dbo", is_dir=True),
                _path_entry(f"{LH}/Tables/silver", is_dir=True),
                _path_entry(f"{LH}/Tables/gold", is_dir=True),
            ]
            schemas = list_schemas(fake_credential, WS, LH)
        assert sorted(schemas) == ["dbo", "gold", "silver"]
        mock_ls.assert_called_once()
        assert mock_ls.call_args.args[3] == "Tables"

    def test_filters_out_non_directory_entries(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.list_paths") as mock_ls:
            mock_ls.return_value = [
                _path_entry(f"{LH}/Tables/dbo", is_dir=True),
                _path_entry(f"{LH}/Tables/readme.md", is_dir=False),
            ]
            schemas = list_schemas(fake_credential, WS, LH)
        assert schemas == ["dbo"]

    def test_empty_when_no_schemas(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.list_paths") as mock_ls:
            mock_ls.return_value = []
            schemas = list_schemas(fake_credential, WS, LH)
        assert schemas == []


# ── list_tables ─────────────────────────────────────────────────────────────


class TestListTables:
    def test_lists_tables_in_single_schema(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.list_paths") as mock_ls:
            mock_ls.return_value = [
                _path_entry(f"{LH}/Tables/dbo/widgets", is_dir=True),
                _path_entry(f"{LH}/Tables/dbo/gizmos", is_dir=True),
            ]
            tables = list_tables(fake_credential, WS, LH, schema="dbo")
        assert sorted(tables) == ["gizmos", "widgets"]
        assert mock_ls.call_args.args[3] == "Tables/dbo"

    def test_lists_all_tables_flattened_as_qualified_names(self, fake_credential):
        def side_effect(token, ws, lh, path, recursive=False):
            if path == "Tables":
                return [
                    _path_entry(f"{LH}/Tables/dbo", is_dir=True),
                    _path_entry(f"{LH}/Tables/silver", is_dir=True),
                ]
            if path == "Tables/dbo":
                return [_path_entry(f"{LH}/Tables/dbo/a", is_dir=True)]
            if path == "Tables/silver":
                return [_path_entry(f"{LH}/Tables/silver/b", is_dir=True)]
            return []

        with patch(
            "pyfabric.data.lakehouse.onelake.list_paths", side_effect=side_effect
        ):
            tables = list_tables(fake_credential, WS, LH)
        assert sorted(tables) == ["dbo.a", "silver.b"]

    def test_empty_schema_returns_empty(self, fake_credential):
        with patch("pyfabric.data.lakehouse.onelake.list_paths") as mock_ls:
            mock_ls.return_value = []
            assert list_tables(fake_credential, WS, LH, schema="dbo") == []


# ── onelake.delete_path / rename_path (unit-level) ──────────────────────────


class TestOnelakeDeletePath:
    def test_delete_path_issues_recursive_delete(self):
        from pyfabric.data import onelake

        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        session.delete.return_value = resp

        with patch("pyfabric.data.onelake._get_session", return_value=session):
            result = onelake.delete_path(
                "tok", WS, LH, "Tables/dbo/widgets", recursive=True
            )
        assert result is True
        # URL and ?recursive=true assembled correctly.
        url = session.delete.call_args.args[0]
        assert url.endswith(f"/{WS}/{LH}/Tables/dbo/widgets")
        assert session.delete.call_args.kwargs.get("params") == {"recursive": "true"}

    def test_delete_path_returns_false_on_404(self):
        from pyfabric.data import onelake

        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 404
        session.delete.return_value = resp
        with patch("pyfabric.data.onelake._get_session", return_value=session):
            result = onelake.delete_path("tok", WS, LH, "Tables/missing")
        assert result is False


class TestOnelakeRenamePath:
    def test_rename_path_issues_put_with_rename_source_header(self):
        from pyfabric.data import onelake

        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 201
        session.put.return_value = resp

        with patch("pyfabric.data.onelake._get_session", return_value=session):
            onelake.rename_path("tok", WS, LH, "Tables/old/a", "Tables/new/a")
        url = session.put.call_args.args[0]
        headers = session.put.call_args.kwargs["headers"]
        assert url.endswith(f"/{WS}/{LH}/Tables/new/a")
        # x-ms-rename-source points at /{filesystem}/{src_full_path}; the
        # filesystem is the workspace id and the path is item_id/src.
        assert headers["x-ms-rename-source"] == f"/{WS}/{LH}/Tables/old/a"
