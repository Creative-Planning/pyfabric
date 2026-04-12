"""pyfabric testing utilities for local Fabric development.

Provides DuckDB-backed Spark session mock, notebookutils mock, and pytest
fixtures so users can test notebook and pipeline logic locally without
deploying to Fabric.

Install: ``pip install pyfabric[testing]``

Usage in tests::

    def test_my_notebook(fabric_spark, mock_notebookutils):
        result = fabric_spark.sql("SELECT 1 AS value")
        assert result.collect()[0][0] == 1
"""
