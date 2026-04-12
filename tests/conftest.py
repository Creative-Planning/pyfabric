"""Shared pytest fixtures for pyfabric tests.

No live Azure or Fabric connections required. Tests use:
  - Mock credentials (deterministic fake tokens)
  - Local Delta tables in tmp directories
  - DuckDB for SQL contract testing (optional)
"""

from unittest.mock import MagicMock

import pytest

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
