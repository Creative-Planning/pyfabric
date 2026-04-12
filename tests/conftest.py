"""Shared pytest fixtures for pyfabric tests.

No live Azure or Fabric connections required. Tests use:
  - Mock credentials (deterministic fake tokens)
  - Mock HTTP responses for Fabric API tests
  - Local Delta tables in tmp directories
  - DuckDB for SQL contract testing (optional)
  - Synthetic Fabric workspace fixtures for E2E validation
"""

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ── Mock credential ──────────────────────────────────────────────────────────


class MockCredential:
    """FabricCredential substitute that returns deterministic fake tokens."""

    def get_token(self, resource: str) -> str:
        return f"mock-token-for-{resource.replace('https://', '').replace('/', '_')}"

    @property
    def fabric_token(self) -> str:
        return self.get_token("https://api.fabric.microsoft.com")

    @property
    def storage_token(self) -> str:
        return self.get_token("https://storage.azure.com")

    @property
    def sql_token(self) -> str:
        return self.get_token("https://database.windows.net")

    def account_info(self) -> dict:
        return {"user": {"name": "test@example.com"}, "tenantId": "test-tenant"}


@pytest.fixture
def mock_credential():
    """FabricCredential that returns deterministic fake tokens."""
    return MockCredential()


# ── Mock HTTP responses ──────────────────────────────────────────────────────


def make_response(
    status_code: int = 200,
    json_body: dict | list | None = None,
    headers: dict | None = None,
    text: str = "",
) -> MagicMock:
    """Factory for mock requests.Response objects."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    if json_body is not None:
        resp.json.return_value = json_body
        resp.text = str(json_body)
    else:
        resp.json.return_value = {}
        resp.text = text
    resp.content = (text or str(json_body or "")).encode()
    return resp


@pytest.fixture
def mock_http_response():
    """Factory fixture for creating mock HTTP responses."""
    return make_response


@pytest.fixture
def mock_requests_session():
    """Mock requests.Session for FabricClient tests."""
    session = MagicMock()
    session.request.return_value = make_response(200, {})
    session.get.return_value = make_response(200, {})
    session.post.return_value = make_response(200, {})
    session.delete.return_value = make_response(200, {})
    return session


# ── Mock Fabric client ───────────────────────────────────────────────────────


@pytest.fixture
def mock_fabric_client():
    """FabricClient mock for testing REST interactions."""
    client = MagicMock()
    client.get.return_value = {}
    client.get_paged.return_value = []
    client.post.return_value = {}
    client.patch.return_value = {}
    return client


# ── Mock subprocess ──────────────────────────────────────────────────────────


def make_subprocess_result(
    stdout: str = "", stderr: str = "", returncode: int = 0
) -> MagicMock:
    """Factory for mock subprocess.CompletedProcess objects."""
    result = MagicMock()
    result.stdout = stdout
    result.stderr = stderr
    result.returncode = returncode
    return result


@pytest.fixture
def mock_subprocess_result():
    """Factory fixture for creating mock subprocess results."""
    return make_subprocess_result


# ── Temp directories ─────────────────────────────────────────────────────────


@pytest.fixture
def tmp_dir(tmp_path):
    """A temporary directory that is cleaned up after the test."""
    return tmp_path


@pytest.fixture
def tmp_delta_dir(tmp_path):
    """Create a tmp directory with a sample Delta table for testing."""
    try:
        import pyarrow as pa
        from deltalake import write_deltalake
    except ImportError:
        pytest.skip("deltalake and pyarrow required")

    table_dir = tmp_path / "test_table"
    table = pa.table(
        {
            "id": pa.array([1, 2, 3]),
            "name": pa.array(["alpha", "beta", "gamma"]),
            "value": pa.array([10.0, 20.0, 30.0]),
        }
    )
    write_deltalake(str(table_dir), table)
    return tmp_path


# ── DuckDB ───────────────────────────────────────────────────────────────────


@pytest.fixture
def duckdb_conn(tmp_delta_dir):
    """DuckDB connection with a test Delta table registered."""
    try:
        import duckdb
    except ImportError:
        pytest.skip("duckdb required: pip install duckdb")

    conn = duckdb.connect()
    table_dir = tmp_delta_dir / "test_table"
    conn.execute(f"CREATE VIEW test_table AS SELECT * FROM delta_scan('{table_dir}')")
    yield conn
    conn.close()


# ── Fabric workspace fixtures ────────────────────────────────────────────────


@pytest.fixture
def fixture_workspace() -> Path:
    """Path to the synthetic valid Fabric workspace fixture directory."""
    return FIXTURES_DIR / "workspace"


@pytest.fixture
def fixture_workspace_invalid() -> Path:
    """Path to the synthetic invalid Fabric workspace fixture directory."""
    return FIXTURES_DIR / "workspace_invalid"


@pytest.fixture
def real_workspace() -> Path | None:
    """Path to a real Fabric workspace for E2E testing.

    Set the PYFABRIC_TEST_WORKSPACE environment variable to enable.
    Returns None if the env var is not set.
    """
    ws_path = os.environ.get("PYFABRIC_TEST_WORKSPACE")
    if ws_path:
        p = Path(ws_path)
        if p.is_dir():
            return p
    return None
