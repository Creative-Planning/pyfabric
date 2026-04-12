# Testing Guide

This guide explains how to test Microsoft Fabric notebooks and pipelines
locally using pyfabric's testing framework.

## Installation

```bash
pip install pyfabric[testing]
```

This installs DuckDB, deltalake, and pytest along with the pyfabric testing
utilities. The pytest fixtures are auto-registered — no conftest imports needed.

## Available fixtures

After installing `pyfabric[testing]`, these fixtures are available in all
your test files automatically:

| Fixture | Type | Description |
|---------|------|-------------|
| `fabric_spark` | `DuckDBSparkSession` | Drop-in SparkSession backed by DuckDB |
| `mock_notebookutils` | `MockNotebookUtils` | Drop-in for Fabric notebookutils |
| `lakehouse_root` | `Path` | Temporary directory for lakehouse data |

## Testing notebook SQL logic

The `fabric_spark` fixture provides a DuckDB-backed SparkSession replacement.
It supports `spark.sql()`, `DataFrame.collect()`, `.show()`, `.count()`,
and automatic Delta table discovery.

### Basic SQL test

```python
def test_simple_query(fabric_spark):
    df = fabric_spark.sql("SELECT 1 AS id, 'hello' AS message")
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["message"] == "hello"
```

### Testing with Delta tables

Place Delta tables in the lakehouse directory structure that Fabric expects:

```python
import pyarrow as pa
from deltalake import write_deltalake

def test_with_delta_table(fabric_spark, lakehouse_root):
    # Create a lakehouse with a Delta table
    table_dir = lakehouse_root / "lh_test" / "Tables" / "products"
    table_dir.mkdir(parents=True)
    
    table = pa.table({
        "product_id": pa.array([1, 2, 3]),
        "name": pa.array(["Widget A", "Widget B", "Widget C"]),
        "price": pa.array([9.99, 19.99, 29.99]),
    })
    write_deltalake(str(table_dir), table)
    
    # Query using Fabric-style table references
    df = fabric_spark.sql("SELECT * FROM lh_test.products WHERE price > 15")
    assert df.count() == 2
```

### SHOW TABLES

```python
def test_show_tables(fabric_spark, lakehouse_root):
    # (create Delta tables first, as above)
    df = fabric_spark.sql("SHOW TABLES IN lh_test")
    tables = [row[1] for row in df.collect()]
    assert "products" in tables
```

### Catalog operations

```python
def test_catalog(fabric_spark, lakehouse_root):
    # (create Delta tables first)
    tables = fabric_spark.catalog.listTables("lh_test")
    assert any(t.name == "products" for t in tables)
    
    assert fabric_spark.catalog.tableExists("products")
    assert not fabric_spark.catalog.tableExists("nonexistent")
```

## Testing notebookutils operations

The `mock_notebookutils` fixture provides a local filesystem replacement for
Fabric's `notebookutils` / `mssparkutils`.

### File operations

```python
def test_file_operations(mock_notebookutils):
    # Create directories
    mock_notebookutils.fs.mkdirs("/output/tables")
    
    # Write a file
    mock_notebookutils.fs.put("/output/tables/result.csv", "id,name\n1,test")
    
    # Read it back
    content = mock_notebookutils.fs.head("/output/tables/result.csv")
    assert "id,name" in content
    
    # List files
    files = mock_notebookutils.fs.ls("/output/tables")
    assert len(files) == 1
    
    # Copy and remove
    mock_notebookutils.fs.cp("/output/tables/result.csv", "/output/backup.csv")
    mock_notebookutils.fs.rm("/output/tables/result.csv")
```

### Notebook execution (no-op in local mode)

```python
def test_notebook_run(mock_notebookutils):
    # notebook.run() is a no-op locally — it logs the call but does not execute
    result = mock_notebookutils.notebook.run(
        "nb_process_data",
        timeout_seconds=600,
        arguments={"env": "dev", "date": "2025-04-01"},
    )
    assert result == ""
```

### Credentials (raises in local mode)

```python
import pytest

def test_credentials_raise(mock_notebookutils):
    with pytest.raises(NotImplementedError, match="not available in local mode"):
        mock_notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
```

Use `pyfabric.client.auth.FabricCredential` instead for local authentication.

## Testing a real notebook function

A common pattern is to extract notebook logic into testable functions:

```python
# In your notebook or a shared module:
def process_data(spark, source_table, target_table):
    """Transform data from source to target table."""
    df = spark.sql(f"SELECT id, UPPER(name) AS name FROM {source_table}")
    return df

# In your test file:
import pyarrow as pa
from deltalake import write_deltalake

def test_process_data(fabric_spark, lakehouse_root):
    # Set up test data
    table_dir = lakehouse_root / "lh_bronze" / "Tables" / "raw_customers"
    table_dir.mkdir(parents=True)
    write_deltalake(str(table_dir), pa.table({
        "id": pa.array([1, 2]),
        "name": pa.array(["alice", "bob"]),
    }))
    
    # Run the function under test
    result = process_data(fabric_spark, "lh_bronze.raw_customers", "lh_silver.customers")
    
    # Verify
    rows = result.collect()
    assert rows[0]["name"] == "ALICE"
    assert rows[1]["name"] == "BOB"
```

## Running tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src/pyfabric --cov-branch --cov-report=term-missing

# Run only your project tests (not pyfabric internal tests)
pytest tests/

# Generate JSON report for AI analysis
pytest --json-report --json-report-file=test-report.json
```

## Validating Fabric workspace structure

Use `validate_workspace()` to check item structures before git-syncing:

```python
from pathlib import Path
from pyfabric.items.validate import validate_workspace

results = validate_workspace(Path("path/to/workspace"))
for r in results:
    if r.valid:
        print(f"OK: {r.item_path.name}")
    else:
        print(f"FAIL: {r.item_path.name}")
        for e in r.errors:
            print(f"  {e.message}")
```

You can also set the `PYFABRIC_TEST_WORKSPACE` environment variable to run
E2E validation as part of your test suite:

```bash
PYFABRIC_TEST_WORKSPACE=/path/to/workspace pytest -m e2e
```
