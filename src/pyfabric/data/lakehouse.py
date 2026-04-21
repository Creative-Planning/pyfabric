"""
High-level lakehouse table operations with SQL-first reads and DFS Delta writes.

Usage:
    from pyfabric.client.auth import FabricCredential
    from pyfabric.data.lakehouse import write_table, read_table

    cred = FabricCredential()

    # Write a DataFrame as a Delta table
    write_table(cred, ws_id, lh_id, "products", df, schema="dbo")

    # Read (tries SQL first, falls back to DFS)
    df = read_table(cred, ws_id, lh_id, "products",
                    columns=["product_id", "product_name"],
                    sql_filter="product_id = '42'")
"""

import datetime
import io
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

from pyfabric.client.auth import FabricCredential
from pyfabric.data.onelake import abfss_url

log = structlog.get_logger()


@dataclass
class WriteResult:
    """Result of a write_table operation."""

    table_path: str
    row_count: int
    column_count: int
    mode: str
    dry_run: bool


def write_table(
    credential: FabricCredential,
    ws_id: str,
    lh_id: str,
    table_name: str,
    data: "pd.DataFrame | pa.Table",
    *,
    schema: str = "dbo",
    mode: str = "overwrite",
    dry_run: bool = False,
    source: str | None = None,
) -> WriteResult:
    """
    Write a DataFrame/Arrow table as a Delta table in a Fabric lakehouse.

    Uses deltalake with abfss:// for direct OneLake DFS writes. Fabric
    auto-discovers Delta tables in Tables/{schema}/{table_name}/.

    Args:
        credential:  FabricCredential for storage token.
        ws_id:       Workspace ID.
        lh_id:       Lakehouse ID.
        table_name:  Table name (without schema prefix).
        data:        pandas DataFrame or PyArrow Table.
        schema:      Schema name (default "dbo" for schema-enabled lakehouses).
        mode:        "overwrite" or "append".
        dry_run:     If True, validate but do not write.
        source:      Optional source lineage string.

    Returns:
        WriteResult with metadata about the write.
    """
    try:
        import pyarrow as pa_mod
        from deltalake import CommitProperties, write_deltalake
    except ImportError:
        raise RuntimeError(
            "write_table needs deltalake + pyarrow. Install them with "
            "`pip install pyfabric[lakehouse-io]` (or the `all` extra)."
        ) from None

    # pandas is only required when the caller passes a DataFrame — keep the
    # import lazy so Arrow-only callers don't need pandas installed.
    try:
        import pandas as pd_mod
    except ImportError:
        pd_mod = None  # type: ignore[assignment]

    # Convert pandas to Arrow if needed
    if pd_mod is not None and isinstance(data, pd_mod.DataFrame):
        arrow_table = pa_mod.Table.from_pandas(data, preserve_index=False)
    elif isinstance(data, pa_mod.Table):
        arrow_table = data
    else:
        raise TypeError(
            f"data must be a pyarrow.Table or pandas.DataFrame, got {type(data).__name__}"
        )

    table_path = f"Tables/{schema}/{table_name}"
    target = abfss_url(ws_id, lh_id, table_path)
    row_count = arrow_table.num_rows
    col_count = arrow_table.num_columns

    log.info(
        "Target: %s/%s.%s (%d rows, %d cols)",
        lh_id[:8],
        schema,
        table_name,
        row_count,
        col_count,
    )
    log.debug("Arrow schema: %s", arrow_table.schema)
    log.debug("Target path: %s", target)

    if row_count == 0:
        log.warning("Data is empty (0 rows) - nothing to write")

    if mode not in ("overwrite", "append"):
        raise ValueError(f"Invalid mode '{mode}'. Use 'overwrite' or 'append'.")

    # Naive (tz-less) timestamp columns become Delta TIMESTAMP_NTZ, which the
    # Fabric SQL analytics endpoint rejects with "Columns of the specified
    # data types are not supported". Warn once per write so callers can
    # either add tz='UTC' to the Arrow schema or cast to string before
    # writing. The warning is informational — the write still proceeds.
    naive_ts_cols = [
        f.name
        for f in arrow_table.schema
        if pa_mod.types.is_timestamp(f.type) and f.type.tz is None
    ]
    if naive_ts_cols:
        log.warning(
            "Naive timestamp columns %s will be written as Delta TIMESTAMP_NTZ, "
            "which the Fabric SQL analytics endpoint does not support. Convert "
            "to tz-aware UTC (e.g. pa.timestamp('us', tz='UTC')) or cast to "
            "string (ISO-8601) before writing if downstream consumers use the "
            "SQL endpoint or Power BI DirectLake.",
            naive_ts_cols,
        )

    if dry_run:
        log.info(
            "[DRY RUN] Would write %d rows to %s.%s (mode=%s)",
            row_count,
            schema,
            table_name,
            mode,
        )
        preview_slice = arrow_table.slice(0, min(5, row_count))
        if pd_mod is not None:
            log.info(
                "[DRY RUN] Preview:\n%s",
                preview_slice.to_pandas().to_string(index=False),
            )
        else:
            # Fall back to Arrow's own repr when pandas isn't installed.
            log.info("[DRY RUN] Preview:\n%s", preview_slice)
        return WriteResult(
            table_path=table_path,
            row_count=row_count,
            column_count=col_count,
            mode=mode,
            dry_run=True,
        )

    storage_options = {"bearer_token": credential.storage_token}

    commit_properties = {
        "written_by": source or os.path.basename(os.sys.argv[0])
        if os.sys.argv
        else "unknown",
        "written_at": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H%M%SZ"),
        "row_count": str(row_count),
    }

    log.info("Writing %d rows to %s.%s (mode=%s)", row_count, schema, table_name, mode)
    write_deltalake(
        target,
        arrow_table,
        mode=mode,
        schema_mode="overwrite" if mode == "overwrite" else "merge",
        storage_options=storage_options,
        commit_properties=CommitProperties(custom_metadata=commit_properties),
    )
    log.info("Write complete: %s.%s", schema, table_name)

    return WriteResult(
        table_path=table_path,
        row_count=row_count,
        column_count=col_count,
        mode=mode,
        dry_run=False,
    )


def read_table(
    credential: FabricCredential,
    ws_id: str,
    lh_id: str,
    table_name: str,
    *,
    schema: str = "dbo",
    columns: list[str] | None = None,
    sql_filter: str | None = None,
    sql_connection=None,  # FabricSql | None
    dry_run: bool = False,
) -> "pd.DataFrame":
    """
    Read a table from a Fabric lakehouse.

    Strategy: SQL endpoint first (server-side filtering), DFS Delta fallback.

    Args:
        credential:      FabricCredential.
        ws_id:           Workspace ID.
        lh_id:           Lakehouse ID.
        table_name:      Table name (without schema prefix).
        schema:          Schema name (default "dbo").
        columns:         Column names to read (None = all). Applied as projection.
        sql_filter:      WHERE clause for SQL path (e.g. "status = 'active'").
        sql_connection:  Optional pre-built FabricSql connection. If provided,
                         SQL path is attempted first.
        dry_run:         If True, log what would be read but return empty DataFrame.

    Returns:
        pandas DataFrame with the requested data.
    """
    import pandas as pd_mod

    col_spec = ", ".join(columns) if columns else "*"
    qualified = f"{schema}.{table_name}"

    if dry_run:
        log.info(
            "[DRY RUN] Would read %s (columns: %s, filter: %s)",
            qualified,
            col_spec,
            sql_filter or "none",
        )
        return pd_mod.DataFrame(columns=columns or [])

    # ── Try SQL path first ────────────────────────────────────────────────
    if sql_connection is not None:
        try:
            sql = f"SELECT {col_spec} FROM {qualified}"
            if sql_filter:
                sql += f" WHERE {sql_filter}"
            log.info("Reading via SQL: %s", qualified)
            return sql_connection.query_df(sql)
        except Exception as e:
            log.warning("SQL read failed (%s), falling back to DFS", e)

    # ── DFS Delta fallback ────────────────────────────────────────────────
    log.info("Reading via DFS Delta: %s", qualified)
    return _read_delta(
        credential, ws_id, lh_id, f"Tables/{schema}/{table_name}", columns=columns
    )


def _read_delta(
    credential: FabricCredential,
    ws_id: str,
    item_id: str,
    table_path: str,
    *,
    columns: list[str] | None = None,
) -> "pd.DataFrame":
    """Read a Delta table via deltalake with column projection."""
    try:
        import deltalake
        import pandas as pd_mod
        import pyarrow.parquet as pq
    except ImportError:
        raise RuntimeError("pip install deltalake pyarrow pandas") from None

    target = abfss_url(ws_id, item_id, table_path)
    storage_options = {"bearer_token": credential.storage_token}

    dt = deltalake.DeltaTable(target, storage_options=storage_options)

    # Try native read first (works if no deletionVectors)
    try:
        tbl = dt.to_pyarrow_table(columns=columns)
        log.debug("Delta native read: %d rows", tbl.num_rows)
        return tbl.to_pandas()
    except Exception as e:
        if "deletionVectors" not in str(e):
            raise
        log.debug("deletionVectors not supported, using file-level fallback")

    # Fallback: read individual parquet files with column projection
    import pyarrow as pa
    import requests as req_mod

    file_uris = dt.file_uris()
    hdrs = {"Authorization": f"Bearer {credential.storage_token}"}
    arrow_tables = []

    for uri in file_uris:
        after_at = uri.split("@onelake.dfs.fabric.microsoft.com/")[1]
        ws_part = uri.split("abfss://")[1].split("@")[0]
        url = f"https://onelake.dfs.fabric.microsoft.com/{ws_part}/{after_at}"

        r = req_mod.get(url, headers=hdrs)
        if r.status_code != 200:
            log.warning("Skipping file (HTTP %d): %s", r.status_code, uri[-40:])
            continue

        arrow_tables.append(pq.read_table(io.BytesIO(r.content), columns=columns))

    if not arrow_tables:
        return pd_mod.DataFrame(columns=columns or [])

    # Concat at Arrow level (avoids N intermediate pandas DataFrames)
    result = pa.concat_tables(arrow_tables).to_pandas()
    log.debug(
        "Delta file-level read: %d rows from %d files", len(result), len(file_uris)
    )
    return result
