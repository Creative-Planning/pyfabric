"""SQL connection Protocol + Spark shim for portable transforms.

The "wheel + SqlConn pattern" lets the same transform code run locally
against DuckDB (fast inner loop, unit-testable) and in Fabric against
Spark (production schedule). Both targets satisfy a minimal
``.execute(sql)`` interface, so a transform module can type-hint
``SqlConn`` and callers pass whichever connection fits the environment.

Typical usage in a transform module::

    from pyfabric.data.sqlconn import SqlConn


    def build_silver(con: SqlConn, *, bronze_schema: str, silver_schema: str) -> None:
        con.execute(f'''
            CREATE OR REPLACE TABLE {silver_schema}.dim_customer AS
            SELECT ... FROM {bronze_schema}.customer
        ''')

Local test driver (DuckDB satisfies the Protocol natively)::

    import duckdb

    con = duckdb.connect()
    build_silver(con, bronze_schema="dbo", silver_schema="silver")

Fabric Spark notebook::

    from pyfabric.data.sqlconn import SparkSqlConn

    con = SparkSqlConn(spark)
    build_silver(con, bronze_schema="lh_bronze.dbo", silver_schema="lh_silver.dbo")
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SqlConn(Protocol):
    """Minimal SQL-connection interface for portable transforms.

    Satisfied natively by ``duckdb.DuckDBPyConnection``. Satisfied in
    Fabric Spark by :class:`SparkSqlConn`. Also trivially implementable
    by test doubles.
    """

    def execute(self, sql: str) -> Any:
        """Execute a SQL statement and return whatever the driver yields.

        Callers should not rely on the return type (DuckDB returns a
        result wrapper with ``.fetchone()``, Spark returns a DataFrame);
        transforms that only use DDL / INSERT / CTAS don't need the
        result at all.
        """
        ...


class SparkSqlConn:
    """Shim wrapping a ``pyspark.sql.SparkSession`` as a :class:`SqlConn`.

    The shim exists only to satisfy the Protocol — each call delegates
    to ``spark.sql(statement)``. Use directly in a Fabric notebook cell::

        from pyfabric.data.sqlconn import SparkSqlConn

        con = SparkSqlConn(spark)
        build_silver(con, ...)

    Why not just pass ``spark.sql`` itself? Two reasons:

    1. ``spark.sql`` is a bound method; it lacks the ``.execute()`` name
       the Protocol requires, so transforms would have to special-case
       Spark vs DuckDB.
    2. Having a named shim gives callers a single place to add Spark-
       specific overrides later (``config_set``, ``cache``, etc.) without
       changing transform code.
    """

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def execute(self, sql: str) -> Any:
        return self._spark.sql(sql)
