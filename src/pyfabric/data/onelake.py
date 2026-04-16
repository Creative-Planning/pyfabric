"""
OneLake DFS (Data Lake Storage Gen2) helpers for Microsoft Fabric.

Uses the storage.azure.com token scope for all operations.

Path conventions
----------------
OneLake paths are structured as:
    https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{item_id}/{root}/{sub_path}

Schema-enabled lakehouses use Tables/dbo/{table}, others use Tables/{table}.
"""

import hashlib
import io
import os
import urllib.parse
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import requests
import structlog
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if TYPE_CHECKING:
    import pandas as pd

log = structlog.get_logger()

ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"
STORAGE_RESOURCE = "https://storage.azure.com"

# ── Shared HTTP session for connection reuse ─────────────────────────────────

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    """Get or create a shared requests.Session with retry and connection pooling."""
    global _session
    if _session is None:
        _session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 503],
            allowed_methods=["GET", "PUT", "PATCH", "DELETE"],
            respect_retry_after_header=True,
        )
        _session.mount("https://", HTTPAdapter(max_retries=retry))
    return _session


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
        r = _get_session().get(url, headers=_hdrs(token), params=params)
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


def walk(
    token: str,
    ws_id: str,
    item_id: str,
    path: str,
    *,
    suffix: str | None = None,
) -> Iterator[dict]:
    """Recursively yield file entries under {item_id}/{path}.

    Convenience wrapper around :func:`list_paths` for the common "find all
    PDFs under this folder" pattern. Handles two things callers otherwise
    reimplement:

      1. Filters out directory entries.
      2. Normalizes the ``name`` field — DFS returns paths relative to the
         workspace (``{item_id}/Files/...``), so each entry is augmented
         with a ``rel_path`` key giving the path relative to ``item_id``
         (for pass-through to :func:`read_file`) and a ``size`` int.

    Args:
        suffix: Optional filename suffix filter (case-sensitive).

    Yields:
        Dicts with at least ``name`` (DFS-native path), ``rel_path``
        (path relative to item_id), ``size`` (int bytes), plus any other
        fields returned by the DFS API.
    """
    prefix = f"{item_id}/"
    for entry in list_paths(token, ws_id, item_id, path, recursive=True):
        if entry.get("isDirectory", "false") == "true":
            continue
        name = entry.get("name", "")
        if suffix and not name.endswith(suffix):
            continue
        rel_path = name[len(prefix) :] if name.startswith(prefix) else name
        yield {
            **entry,
            "rel_path": rel_path,
            "size": int(entry.get("contentLength", 0)),
        }


# ── Hash utility ─────────────────────────────────────────────────────────────


def md5_file(path: str | Path, *, chunk_size: int = 8192) -> str:
    """Compute MD5 hex digest of a file, streaming in chunks."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Reading ──────────────────────────────────────────────────────────────────


def read_file(token: str, ws_id: str, item_id: str, path: str) -> bytes:
    """Download a file at {item_id}/{path}. Raises on HTTP error."""
    url = _dfs_url(ws_id, item_id, urllib.parse.quote(path, safe="/"))
    r = _get_session().get(url, headers=_hdrs(token))
    r.raise_for_status()
    return r.content


def read_file_by_name(token: str, ws_id: str, ws_relative_name: str) -> bytes:
    """Download using the 'name' field from list_paths (workspace-relative path)."""
    url = f"{ONELAKE_DFS}/{ws_id}/{ws_relative_name}"
    r = _get_session().get(url, headers=_hdrs(token))
    r.raise_for_status()
    return r.content


def download_with_cache(
    token: str,
    ws_id: str,
    item_id: str,
    rel_path: str,
    cache_dir: str | Path,
    *,
    read_only_caches: Sequence[str | Path] = (),
    expected_size: int | None = None,
    expected_md5: str | None = None,
) -> Path:
    """Resolve a OneLake file to a local path, downloading only if needed.

    Search order:
      1. Each directory in ``read_only_caches`` (shared fixture sets,
         previously downloaded bulk extracts, etc.) — checked in order.
      2. ``cache_dir`` — the writable cache for this run.
      3. OneLake — downloaded into ``cache_dir``.

    A cached copy is accepted only if it passes the validation the caller
    provided. If ``expected_md5`` is given, the file's MD5 must match; else
    if ``expected_size`` is given, the file's byte size must match; else
    any existing file at that path is accepted (size check is recommended
    for any caller that has the metadata).

    Args:
        rel_path:          File path relative to ``item_id`` (as produced by
                           :func:`walk`'s ``rel_path`` field).
        cache_dir:         Writable cache directory. Created if missing.
        read_only_caches:  Additional directories to check first. Never
                           written to by this function.
        expected_size:     Expected byte size for cache validation.
        expected_md5:      Expected MD5 hex digest for cache validation.

    Returns:
        Path to the local file.

    Raises:
        ValueError: If the downloaded file fails ``expected_md5`` check.
    """
    rel_os = rel_path.replace("/", os.sep)

    for ro_dir in read_only_caches:
        candidate = Path(ro_dir) / rel_os
        if _cache_hit(candidate, expected_size, expected_md5):
            return candidate

    cache_dir_path = Path(cache_dir)
    local_path = cache_dir_path / rel_os

    if _cache_hit(local_path, expected_size, expected_md5):
        return local_path

    local_path.parent.mkdir(parents=True, exist_ok=True)
    data = read_file(token, ws_id, item_id, rel_path)
    local_path.write_bytes(data)

    if expected_md5 is not None:
        actual = hashlib.md5(data).hexdigest()
        if actual != expected_md5:
            raise ValueError(
                f"MD5 mismatch for {rel_path}: expected {expected_md5}, got {actual}"
            )

    return local_path


def _cache_hit(path: Path, expected_size: int | None, expected_md5: str | None) -> bool:
    if not path.exists():
        return False
    if expected_md5 is not None:
        return md5_file(path) == expected_md5
    if expected_size is not None:
        return path.stat().st_size == expected_size
    return True


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

    _get_session().put(
        url, headers=hdrs, params={"resource": "file"}
    ).raise_for_status()
    _get_session().patch(
        url,
        headers={**hdrs, "Content-Type": "application/octet-stream"},
        params={"action": "append", "position": "0"},
        data=data,
    ).raise_for_status()
    _get_session().patch(
        url,
        headers=hdrs,
        params={"action": "flush", "position": str(len(data))},
    ).raise_for_status()


def delete_file(token: str, ws_id: str, item_id: str, path: str) -> bool:
    """Delete a file. Returns True if deleted, False if not found."""
    url = _dfs_url(ws_id, item_id, path)
    r = _get_session().delete(url, headers=_hdrs(token))
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
