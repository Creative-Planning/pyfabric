"""
OneLake DFS (Data Lake Storage Gen2) helpers for Microsoft Fabric.

Uses the storage.azure.com token scope for all operations.

Path conventions
----------------
OneLake paths are structured as:
    https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{item_id}/{root}/{sub_path}

Schema-enabled lakehouses use Tables/dbo/{table}, others use Tables/{table}.
"""

import io
import logging
import urllib.parse
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger(__name__)

ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"
STORAGE_RESOURCE = "https://storage.azure.com"


# ── URL helpers ──────────────────────────────────────────────────────────────


def abfss_url(ws_id: str, item_id: str, path: str = "") -> str:
    """Build an abfss:// URL for deltalake access.

    Example:
        abfss_url(ws_id, lh_id, "Tables/dbo/products")
        -> "abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables/dbo/products"
    """
    base = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{item_id}"
    return f"{base}/{path}" if path else base


def _dfs_url(ws_id: str, item_id: str, path: str) -> str:
    """Build an HTTPS DFS endpoint URL."""
    return f"{ONELAKE_DFS}/{ws_id}/{item_id}/{path}"


def _hdrs(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ── Listing ──────────────────────────────────────────────────────────────────


def list_paths(
    token: str,
    ws_id: str,
    item_id: str,
    path: str,
    recursive: bool = False,
) -> list[dict]:
    """List paths under {item_id}/{path} using the DFS filesystem API.

    Returns a list of path objects with at least: name, isDirectory.
    Returns [] if the path does not exist (404).
    Handles continuation tokens for large directories.
    """
    url = _dfs_url(ws_id, item_id, path)
    params = {"resource": "filesystem", "recursive": str(recursive).lower()}
    results = []
    while True:
        r = requests.get(url, headers=_hdrs(token), params=params)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        results.extend(r.json().get("paths", []))
        cont = r.headers.get("x-ms-continuation")
        if not cont:
            break
        params["continuation"] = cont
    return results


def list_files(
    token: str,
    ws_id: str,
    item_id: str,
    path: str,
    recursive: bool = False,
    suffix: str | None = None,
) -> list[dict]:
    """list_paths filtered to non-directory entries, optionally by suffix."""
    entries = list_paths(token, ws_id, item_id, path, recursive=recursive)
    files = [p for p in entries if p.get("isDirectory", "false") != "true"]
    if suffix:
        files = [p for p in files if p["name"].endswith(suffix)]
    return files


# ── Reading ──────────────────────────────────────────────────────────────────


def read_file(token: str, ws_id: str, item_id: str, path: str) -> bytes:
    """Download a file at {item_id}/{path}. Raises on HTTP error."""
    url = _dfs_url(ws_id, item_id, urllib.parse.quote(path, safe="/"))
    r = requests.get(url, headers=_hdrs(token))
    r.raise_for_status()
    return r.content


def read_file_by_name(token: str, ws_id: str, ws_relative_name: str) -> bytes:
    """Download using the 'name' field from list_paths (workspace-relative path)."""
    url = f"{ONELAKE_DFS}/{ws_id}/{ws_relative_name}"
    r = requests.get(url, headers=_hdrs(token))
    r.raise_for_status()
    return r.content


def read_parquet_df(
    token: str,
    ws_id: str,
    item_id: str,
    path: str,
    *,
    columns: list[str] | None = None,
) -> "pd.DataFrame | None":
    """Download Parquet files under {item_id}/{path} and return a DataFrame.

    Args:
        columns: Optional list of column names to read (projection pushdown).
                 If None, reads all columns.

    Skips _delta_log and _symlink_format_manifest files.
    Returns None if no Parquet files are found.
    """
    try:
        import pandas as pd
        import pyarrow.parquet as pq
    except ImportError:
        raise RuntimeError("pip install pandas pyarrow") from None

    parquet_files = list_files(
        token,
        ws_id,
        item_id,
        path,
        recursive=True,
        suffix=".parquet",
    )
    parquet_files = [
        p
        for p in parquet_files
        if "_delta_log" not in p["name"] and "_symlink_format_manifest" not in p["name"]
    ]
    if not parquet_files:
        return None

    frames = []
    for p in parquet_files:
        raw = read_file_by_name(token, ws_id, p["name"])
        table = pq.read_table(io.BytesIO(raw), columns=columns)
        frames.append(table.to_pandas())

    return pd.concat(frames, ignore_index=True) if frames else None


# ── Writing ──────────────────────────────────────────────────────────────────


def upload_file(
    token: str,
    ws_id: str,
    item_id: str,
    path: str,
    data: bytes,
) -> None:
    """Upload bytes to {item_id}/{path} using the DFS 3-step protocol.

    Protocol: PUT ?resource=file -> PATCH ?action=append -> PATCH ?action=flush
    """
    url = _dfs_url(ws_id, item_id, path)
    hdrs = _hdrs(token)

    requests.put(url, headers=hdrs, params={"resource": "file"}).raise_for_status()
    requests.patch(
        url,
        headers={**hdrs, "Content-Type": "application/octet-stream"},
        params={"action": "append", "position": "0"},
        data=data,
    ).raise_for_status()
    requests.patch(
        url,
        headers=hdrs,
        params={"action": "flush", "position": str(len(data))},
    ).raise_for_status()


def delete_file(token: str, ws_id: str, item_id: str, path: str) -> bool:
    """Delete a file. Returns True if deleted, False if not found."""
    url = _dfs_url(ws_id, item_id, path)
    r = requests.delete(url, headers=_hdrs(token))
    return r.status_code in (200, 202)


def upload_parquet(
    token: str,
    ws_id: str,
    item_id: str,
    path: str,
    records: list[dict],
    columns: list[str],
) -> int:
    """Serialize records to a Parquet file (all-string schema) and upload.

    Returns the number of records written.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise RuntimeError("pip install pyarrow") from None

    pa_schema = pa.schema([pa.field(c, pa.string()) for c in columns])
    normalized = [{c: r.get(c, "") for c in columns} for r in records]
    table = pa.Table.from_pylist(normalized, schema=pa_schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    upload_file(token, ws_id, item_id, path, buf.getvalue())
    return len(records)
