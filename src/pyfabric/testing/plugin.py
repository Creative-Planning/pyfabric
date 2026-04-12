"""Pytest plugin for pyfabric testing fixtures.

Auto-discovered by pytest via the ``pyfabric`` entry point in pyproject.toml.
Registers fixtures from ``pyfabric.testing.fixtures``.
"""

from pyfabric.testing.fixtures import fabric_spark, lakehouse_root, mock_notebookutils

__all__ = ["fabric_spark", "lakehouse_root", "mock_notebookutils"]
