"""Pytest fixtures for local Fabric notebook and pipeline testing.

These fixtures are auto-registered via the pytest plugin entry point.
Users get them for free by installing ``pyfabric[testing]``.

Available fixtures:
- ``fabric_spark`` — DuckDBSparkSession with a temporary lakehouse root
- ``mock_notebookutils`` — MockNotebookUtils with a temporary filesystem root
- ``lakehouse_root`` — Path to the temporary lakehouse directory
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest

from .duckdb_spark import DuckDBSparkSession
from .mock_notebookutils import MockNotebookUtils


@pytest.fixture
def lakehouse_root(tmp_path: Path) -> Path:
    """Temporary lakehouse root directory for testing."""
    return tmp_path / "lakehouses"


@pytest.fixture
def fabric_spark(lakehouse_root: Path) -> Generator[DuckDBSparkSession]:
    """DuckDB-backed Spark session for local notebook testing.

    Lakehouse data should be placed at:
        ``lakehouse_root/<lakehouse_name>/Tables/<table_name>/`` (Delta format)
    """
    lakehouse_root.mkdir(parents=True, exist_ok=True)
    session = DuckDBSparkSession(lakehouse_root=lakehouse_root)
    yield session
    session.stop()


@pytest.fixture
def mock_notebookutils(tmp_path: Path) -> MockNotebookUtils:
    """MockNotebookUtils with a temporary filesystem root."""
    root = tmp_path / "notebookutils"
    root.mkdir(parents=True, exist_ok=True)
    return MockNotebookUtils(root=root)
