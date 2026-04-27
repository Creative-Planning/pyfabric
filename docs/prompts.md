# AI Prompts for Fabric Development

This document provides sample prompts for Claude and Copilot to create
Microsoft Fabric items, validate them, and run local tests using pyfabric.

## Creating Fabric items

### Notebook

```text
Create a Fabric notebook called nb_load_customers that:
1. Reads a CSV file from OneLake path Tables/dbo/raw_customers
2. Transforms the data: uppercase the name column, parse date strings to timestamps
3. Writes the result to lh_silver.dbo.customers as a Delta table
4. Uses the pyfabric Fabric notebook format with METADATA, CELL, and MARKDOWN sections
5. Save it in git-sync format at ws_dev/nb_load_customers.Notebook/
```

### Lakehouse

```text
Using pyfabric, create a Fabric lakehouse definition called lh_silver in git-sync format.
It should have:
- defaultSchema set to "dbo"
- No shortcuts
- Standard ALM settings

Save at ws_dev/lh_silver.Lakehouse
```

### Environment

```text
Using pyfabric, create a Fabric environment definition called env_data_processing.
It should:
- Include pip dependencies: pandas>=2.0, pyarrow>=14.0, deltalake>=0.17

Save in git-sync format at ws_dev/env_data_processing.Environment/
with the correct nested directory structure
```

### Variable Library

```text
Using pyfabric, create a Fabric variable library called vl_config with:
- Variables: workspace_id (String), lakehouse_id (String), env_name (String)
- Default values for a dev environment
- Two value sets: UAT and PROD with appropriate overrides
- Use the correct Fabric JSON schemas for variables.json, settings.json, and valueSets/*.json

Save in git-sync format at ws_dev/vl_config.VariableLibrary/

Remember: variable types use short names (String, Integer, Boolean, Guid)
not suffixed names (StringVariable, etc.).
```

### Dataflow

```text
Using pyfabric, create a Fabric dataflow definition called df_load_source_data that
[TODO]
Save in git-sync format at ws_dev/df_load_source_data.Dataflow/
```

### Semantic Model

```text
Create a Fabric semantic model definition called sm_sales_analytics.
[TODO]
Save in git-sync format at ws_dev/sm_sales_analytics.SemanticModel/
```

### Pipeline

```text
Using pyfabric, create a Fabric pipeline definition called pl_daily_refresh. It has two activities, first running nb_load_customers then when that completes successfully, runs nb_transform data.
```

## Validating items

### Validate a single item

```text
Use pyfabric to validate the nb_load_customers notebook
```

### Validate an entire workspace

```text
Use pyfabric to validate all Fabric items in the workspace at ws_dev
Print a summary showing which items pass and which fail.
```

## Writing and running local tests

### Test a notebook's SQL logic

```text
Write a pytest test for nb_load_customers that:
1. Uses the fabric_spark fixture (DuckDB-backed SparkSession)
2. Creates a test Delta table at lh_bronze/Tables/raw_customers with sample data
3. Calls the notebook's transform function
4. Asserts the output has the correct schema and transformed values
5. Run with: pytest tests/test_nb_load_customers.py -v
```

### Test with notebookutils

```text
Write a pytest test that verifies notebook file operations:
1. Uses the mock_notebookutils fixture
2. Creates directories with fs.mkdirs
3. Writes test data with fs.put
4. Reads it back with fs.head
5. Verifies the content matches
6. Cleans up with fs.rm

Run with: pytest tests/test_file_operations.py -v
```

### Test data quality

```text
Write pytest tests that validate data quality in a DuckDB table:
1. Use the sample_db fixture from pyfabric's test fixtures
2. Check for NULL values in required columns
3. Check for empty strings vs NULLs
4. Verify foreign key relationships (all order.customer_id values exist in customers)
5. Check for anomalous values (e.g., negative prices, future dates)
6. Print results in a format useful for debugging

Run with: pytest tests/test_data_quality.py -v --tb=long
```

### Run validation against a real workspace

```text
Run pyfabric's E2E validation tests against my local Fabric workspace:

PYFABRIC_TEST_WORKSPACE=C:/path/to/my/workspace/dev pytest tests/items/test_validate_e2e.py -v

If any items fail validation, explain what's wrong and how to fix it.
```

## Analyzing test failures

### Read a test report

```text
Read the pytest JSON report at .test-report.json and:
1. List all failed tests with their error messages
2. For each failure, identify the root cause
3. Suggest specific code fixes
4. Prioritize by severity (errors that block functionality vs warnings)
```

### Read log files

```text
Read the structlog JSON log file at .logs/my_script_20250401.jsonl and:
1. Find all ERROR and WARNING entries
2. Group related log entries by timestamp and context
3. Identify the root cause of any failures
4. Check for any masked [TOKEN] entries that might indicate credential issues
```

## Using pyfabric programmatically

### List workspace items

```text
Using pyfabric, connect to my Fabric tenant and:
1. List all workspaces I have access to
2. For a specific workspace, list all items grouped by type
3. Show the display name, type, and ID for each item

Use FabricCredential with my tenant, FabricClient, and list_items().
```

### Create and upload an item

```text
Using pyfabric, create a new Notebook called nb_hello_world with a simple
print statement, then:
1. Build an ArtifactBundle
2. Save it to disk in git-sync format
3. Validate it with validate_item()
4. If validation passes, print "Ready to commit"

Use pyfabric.items.bundle and pyfabric.items.validate.
```

### Read lakehouse data

```text
Using pyfabric, read data from my Fabric lakehouse:
1. Connect with FabricCredential
2. Use read_table() to read dbo.customers from lh_bronze
3. Print the first 10 rows
4. Show the column names and data types

Try the SQL endpoint first; if that fails, fall back to DFS Delta read.
```
