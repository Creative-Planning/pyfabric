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
        sql._conn = mock_conn

        mock_pd = MagicMock()
        mock_pd.read_sql.side_effect = Exception("Invalid column name 'nonexistent'")
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
        sql._conn = mock_conn

        mock_pd = MagicMock()
        mock_pd.read_sql.side_effect = Exception("connection lost")
        with patch.dict("sys.modules", {"pandas": mock_pd}):
            result = sql.table_exists("nonexistent_table")
            assert result is False


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
