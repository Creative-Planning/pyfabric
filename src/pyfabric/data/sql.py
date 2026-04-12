"""
SQL analytics endpoint client for Microsoft Fabric lakehouses and warehouses.

Connects via pyodbc using AAD access tokens (database.windows.net scope).

Usage:
    from pyfabric.client.auth import FabricCredential
    from pyfabric.data.sql import FabricSql, connect_lakehouse

    cred = FabricCredential()

    # Direct connection (if you know the SQL endpoint server):
    sql = FabricSql(server="xxx.datawarehouse.pbidedicated.windows.net",
                    database="my_lakehouse", credential=cred)
    df = sql.query_df("SELECT id, name FROM dbo.products")

    # Auto-resolved from Fabric REST API:
    sql = connect_lakehouse(client, cred, ws_id, "my_lakehouse")
    df = sql.query_df("SELECT TOP 10 * FROM dbo.products")

Requirements:
    pip install pyodbc
    ODBC Driver 18 for SQL Server must be installed on the system.
"""

import logging
import struct
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from pyfabric.client.auth import SQL_RESOURCE, FabricCredential

log = logging.getLogger(__name__)


class SqlError(Exception):
    """Raised on SQL connection or query errors."""


class FabricSql:
    """SQL connection to a Fabric lakehouse or warehouse SQL endpoint."""

    def __init__(
        self,
        server: str,
        database: str,
        credential: FabricCredential,
    ):
        self.server = server
        self.database = database
        self._credential = credential
        self._conn = None

    def _get_connection(self):
        """Lazy-connect on first use. Reconnects if connection is dead."""
        if self._conn is not None:
            try:
                self._conn.cursor().execute("SELECT 1")
                return self._conn
            except Exception:
                log.debug("SQL connection stale, reconnecting")
                self._conn = None

        try:
            import pyodbc
        except ImportError:
            raise SqlError(
                "pyodbc is required for SQL endpoint access.\n"
                "Install: pip install pyodbc\n"
                "Also requires: ODBC Driver 18 for SQL Server"
            ) from None

        token = self._credential.get_token(SQL_RESOURCE)
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack(
            f"<I{len(token_bytes)}s",
            len(token_bytes),
            token_bytes,
        )

        conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={self.server};"
            f"Database={self.database};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
        )

        log.debug("Connecting to SQL: %s / %s", self.server, self.database)
        try:
            self._conn = pyodbc.connect(
                conn_str,
                attrs_before={1256: token_struct},  # SQL_COPT_SS_ACCESS_TOKEN
            )
        except Exception as e:
            raise SqlError(f"SQL connection failed: {e}") from e

        return self._conn

    def query_df(self, sql: str, params=None) -> "pd.DataFrame":
        """Execute a SELECT and return results as a DataFrame."""
        try:
            import pandas as pd_mod
        except ImportError:
            raise SqlError("pip install pandas") from None

        log.debug("SQL query: %s", sql[:200])
        conn = self._get_connection()
        try:
            df = pd_mod.read_sql(sql, conn, params=params)
            log.debug("  -> %d rows, %d columns", len(df), len(df.columns))
            return df
        except Exception as e:
            self._conn = None  # force reconnect on next call
            raise SqlError(f"SQL query failed: {e}") from e

    def execute(self, sql: str, params=None) -> int:
        """Execute a statement (DDL/DML). Returns affected row count."""
        log.debug("SQL execute: %s", sql[:200])
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params or ())
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            self._conn = None
            raise SqlError(f"SQL execute failed: {e}") from e

    def table_exists(self, table: str, schema: str = "dbo") -> bool:
        """Check if a table exists in the SQL endpoint."""
        sql = (
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        )
        try:
            df = self.query_df(sql, params=[schema, table])
            return len(df) > 0
        except SqlError:
            return False

    def list_tables(self, schema: str = "dbo") -> list[str]:
        """List table names in a schema."""
        sql = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME"
        )
        df = self.query_df(sql, params=[schema])
        return df["TABLE_NAME"].tolist()

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None


def connect_lakehouse(
    client,  # FabricClient
    credential: FabricCredential,
    ws_id: str,
    lakehouse_name: str,
) -> FabricSql:
    """Auto-resolve SQL endpoint from Fabric REST API and return a FabricSql.

    Looks up the lakehouse by name, extracts the SQL endpoint connection string
    from its properties.
    """
    items = client.get_paged(
        f"workspaces/{ws_id}/items",
        params={"type": "Lakehouse"},
    )
    lh = next(
        (i for i in items if i.get("displayName") == lakehouse_name),
        None,
    )
    if lh is None:
        raise SqlError(f"Lakehouse '{lakehouse_name}' not found in workspace {ws_id}")

    # Get lakehouse details (includes SQL endpoint properties)
    details = client.get(f"workspaces/{ws_id}/lakehouses/{lh['id']}")
    props = details.get("properties", {})
    sql_props = props.get("sqlEndpointProperties", {})
    server = sql_props.get("connectionString")

    if not server:
        raise SqlError(
            f"No SQL endpoint found for lakehouse '{lakehouse_name}'. "
            "Ensure the lakehouse has a SQL analytics endpoint enabled."
        )

    log.info("Resolved SQL endpoint: %s", server)
    return FabricSql(server=server, database=lakehouse_name, credential=credential)
