"""Error path tests for SQL client — connection failures, query errors.

Each test verifies that SqlError messages include enough context
(server, database, query excerpt) for root cause analysis.
"""

from unittest.mock import MagicMock, patch

import pytest

from pyfabric.data.sql import FabricSql, SqlError, connect_lakehouse


class TestConnectionErrors:
    def test_pyodbc_not_installed(self):
        cred = MagicMock()
        cred.get_token.return_value = "fake-token"
        sql = FabricSql("server.database.windows.net", "my_db", cred)

        with (
            patch.dict("sys.modules", {"pyodbc": None}),
            patch(
                "builtins.__import__",
                side_effect=ImportError("No module named 'pyodbc'"),
            ),
            pytest.raises(SqlError, match="pyodbc is required"),
        ):
            sql._get_connection()

    def test_connection_failure_includes_inner_error(self):
        cred = MagicMock()
        cred.get_token.return_value = "fake-token"
        sql = FabricSql("bad-server.database.windows.net", "my_db", cred)

        mock_pyodbc = MagicMock()
        mock_pyodbc.connect.side_effect = Exception(
            "TCP Provider: No connection could be made"
        )
        with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
            with pytest.raises(SqlError, match="SQL connection failed") as exc_info:
                sql._get_connection()
            assert "TCP Provider" in str(exc_info.value)

    def test_stale_connection_reconnects(self):
        """If SELECT 1 fails, connection should be reset for retry."""
        cred = MagicMock()
        cred.get_token.return_value = "fake-token"
        sql = FabricSql("server", "db", cred)

        stale_conn = MagicMock()
        stale_conn.cursor.return_value.execute.side_effect = Exception(
            "connection reset"
        )
        sql._conn = stale_conn

        mock_pyodbc = MagicMock()
        new_conn = MagicMock()
        mock_pyodbc.connect.return_value = new_conn
        with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
            result = sql._get_connection()
            assert result is new_conn


class TestQueryErrors:
    def test_query_df_failure_resets_connection(self):
        cred = MagicMock()
        sql = FabricSql("server", "db", cred)
        mock_conn = MagicMock()
        # First execute is the SELECT 1 health check (succeeds); second is the query.
        mock_conn.cursor.return_value.execute.side_effect = [
            None,
            Exception("Invalid column name 'nonexistent'"),
        ]
        sql._conn = mock_conn

        mock_pd = MagicMock()
        with patch.dict("sys.modules", {"pandas": mock_pd}):
            with pytest.raises(SqlError, match="SQL query failed") as exc_info:
                sql.query_df("SELECT nonexistent FROM dbo.products")
            assert "Invalid column name" in str(exc_info.value)
        # Connection should be reset
        assert sql._conn is None

    def test_execute_failure_resets_connection(self):
        cred = MagicMock()
        sql = FabricSql("server", "db", cred)
        mock_conn = MagicMock()
        # SELECT 1 health check succeeds, then the real execute fails
        cursor = MagicMock()
        call_count = 0

        def execute_side_effect(stmt, *args):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return None  # SELECT 1 health check
            raise Exception("Syntax error near 'DROPTABLE'")

        cursor.execute.side_effect = execute_side_effect
        mock_conn.cursor.return_value = cursor
        sql._conn = mock_conn

        with pytest.raises(SqlError, match="SQL execute failed") as exc_info:
            sql.execute("DROPTABLE products")
        assert "Syntax error" in str(exc_info.value)
        assert sql._conn is None

    def test_table_exists_swallows_error(self):
        """table_exists() should return False on SqlError, not raise."""
        cred = MagicMock()
        sql = FabricSql("server", "db", cred)
        mock_conn = MagicMock()
        # First execute is the SELECT 1 health check; second is the INFORMATION_SCHEMA query.
        mock_conn.cursor.return_value.execute.side_effect = [
            None,
            Exception("connection lost"),
        ]
        sql._conn = mock_conn

        mock_pd = MagicMock()
        with patch.dict("sys.modules", {"pandas": mock_pd}):
            result = sql.table_exists("nonexistent_table")
            assert result is False


class TestQueryDfCursorPath:
    """query_df uses cursor.execute + DataFrame.from_records — no pd.read_sql."""

    def _make_sql(self) -> FabricSql:
        cred = MagicMock()
        sql = FabricSql("server", "db", cred)
        cursor = MagicMock()
        cursor.description = [("id",), ("name",)]
        cursor.fetchall.return_value = [(1, "alpha"), (2, "beta")]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor
        sql._conn = mock_conn
        return sql

    def _mock_pd(self) -> MagicMock:
        mock_pd = MagicMock()
        mock_pd.DataFrame.from_records.return_value = MagicMock()
        return mock_pd

    def test_from_records_called_with_correct_rows_and_columns(self):
        mock_pd = self._mock_pd()
        sql = self._make_sql()
        with patch.dict("sys.modules", {"pandas": mock_pd}):
            sql.query_df("SELECT id, name FROM dbo.t")
        mock_pd.DataFrame.from_records.assert_called_with(
            [(1, "alpha"), (2, "beta")], columns=["id", "name"]
        )

    def test_does_not_call_read_sql(self):
        mock_pd = self._mock_pd()
        sql = self._make_sql()
        with patch.dict("sys.modules", {"pandas": mock_pd}):
            sql.query_df("SELECT 1")
        mock_pd.read_sql.assert_not_called()

    def test_params_forwarded_to_cursor_execute(self):
        sql = self._make_sql()
        cursor = sql._conn.cursor.return_value

        with patch.dict("sys.modules", {"pandas": self._mock_pd()}):
            sql.query_df("SELECT * FROM t WHERE id = ?", params=[42])
        # _get_connection calls execute("SELECT 1") first; check the query call.
        cursor.execute.assert_called_with("SELECT * FROM t WHERE id = ?", [42])

    def test_empty_params_uses_empty_tuple(self):
        sql = self._make_sql()
        cursor = sql._conn.cursor.return_value

        with patch.dict("sys.modules", {"pandas": self._mock_pd()}):
            sql.query_df("SELECT 1")
        # _get_connection calls execute("SELECT 1") first; check the query call.
        cursor.execute.assert_called_with("SELECT 1", ())


class TestConnectLakehouseErrors:
    def test_lakehouse_not_found(self):
        client = MagicMock()
        cred = MagicMock()
        client.get_paged.return_value = [
            {"displayName": "lh_other", "id": "lh-other-id"},
        ]
        with pytest.raises(SqlError, match="not found") as exc_info:
            connect_lakehouse(client, cred, "ws-123", "lh_missing")
        error_msg = str(exc_info.value)
        assert "lh_missing" in error_msg
        assert "ws-123" in error_msg

    def test_no_sql_endpoint(self):
        client = MagicMock()
        cred = MagicMock()
        client.get_paged.return_value = [
            {"displayName": "lh_no_sql", "id": "lh-no-sql-id"},
        ]
        client.get.return_value = {
            "properties": {"sqlEndpointProperties": {}},
        }
        with pytest.raises(SqlError, match="No SQL endpoint") as exc_info:
            connect_lakehouse(client, cred, "ws-123", "lh_no_sql")
        assert "lh_no_sql" in str(exc_info.value)
