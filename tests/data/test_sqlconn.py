"""Tests for the SqlConn Protocol + SparkSqlConn shim.

These exercise the public contract: DuckDB satisfies the Protocol
natively and SparkSqlConn delegates ``.execute()`` to ``spark.sql``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import duckdb

from pyfabric.data.sqlconn import SparkSqlConn, SqlConn


class TestSqlConnProtocol:
    def test_duckdb_connection_satisfies_protocol(self):
        con = duckdb.connect(":memory:")
        try:
            assert isinstance(con, SqlConn)
        finally:
            con.close()

    def test_spark_sql_conn_satisfies_protocol(self):
        spark = MagicMock()
        spark.sql = MagicMock(return_value="df")
        con = SparkSqlConn(spark)
        assert isinstance(con, SqlConn)

    def test_plain_object_without_execute_fails_protocol(self):
        assert not isinstance(object(), SqlConn)


class TestSparkSqlConn:
    def test_execute_delegates_to_spark_sql(self):
        spark = MagicMock()
        spark.sql = MagicMock(return_value="expected-df")
        con = SparkSqlConn(spark)

        result = con.execute("SELECT 1")

        assert result == "expected-df"
        spark.sql.assert_called_once_with("SELECT 1")

    def test_multiple_execute_calls_pass_through(self):
        spark = MagicMock()
        spark.sql = MagicMock(side_effect=["r1", "r2", "r3"])
        con = SparkSqlConn(spark)

        con.execute("stmt 1")
        con.execute("stmt 2")
        con.execute("stmt 3")

        assert spark.sql.call_count == 3
        assert [call.args[0] for call in spark.sql.call_args_list] == [
            "stmt 1",
            "stmt 2",
            "stmt 3",
        ]
