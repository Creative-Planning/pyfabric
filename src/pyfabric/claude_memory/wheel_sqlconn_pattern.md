---
name: Wheel + SqlConn transform pattern
description: Canonical pattern for Fabric Spark notebooks — transforms live in a pip wheel, tested against DuckDB locally, deployed to Fabric via SparkSqlConn. Use for any bronze→silver/gold materialization.
type: reference
---

When authoring a Fabric Spark notebook that materializes derived tables
(bronze→silver, silver→gold, etc.), use the **wheel + SqlConn pattern** so the
same transform code runs locally (DuckDB) and in Fabric (Spark) without
modification.

## Structure

1. **Transforms in a pip wheel** — pure Python functions typed against `SqlConn`:

   ```python
   # my_pkg/build.py
   from pyfabric.data.sqlconn import SqlConn

   def build_silver(con: SqlConn, *, bronze_schema: str, silver_schema: str) -> None:
       con.execute(f"""
           CREATE OR REPLACE TABLE {silver_schema}.dim_customer AS
           SELECT id, name FROM {bronze_schema}.customer
       """)
   ```

2. **Local tests against DuckDB** — `duckdb.connect()` satisfies `SqlConn` natively:

   ```python
   import duckdb
   from my_pkg.build import build_silver

   def test_build_silver():
       con = duckdb.connect(":memory:")
       con.execute("CREATE SCHEMA bronze; CREATE TABLE bronze.customer (id INT, name TEXT)")
       con.execute("INSERT INTO bronze.customer VALUES (1, 'Alice')")
       build_silver(con, bronze_schema="bronze", silver_schema="silver")
       result = con.execute("SELECT name FROM silver.dim_customer").fetchone()
       assert result[0] == "Alice"
   ```

3. **Fabric notebook cells**:

   ```python
   # Cell 1 — install wheel from Resources/builtin/
   # MAGIC %pip install "builtin/my_pkg-1.0.0-py3-none-any.whl" --quiet

   # Cell 2 — wrap Spark as SqlConn
   from pyfabric.data.sqlconn import SparkSqlConn
   con = SparkSqlConn(spark)

   # Cell 3 — run same build functions
   from my_pkg.build import build_silver
   build_silver(con, bronze_schema="lh_bronze.dbo", silver_schema="lh_silver.dbo")
   ```

## Deployment

- Wheel goes to `<Notebook>/Resources/builtin/` in the git repo.
- Guard with `notebook-settings.json: {"includeResourcesInGit": "on"}` and
  `fs-settings.json: {"gitExclusions": []}`.
- **Do NOT delete old wheel versions** — Fabric git-sync flaps on Resources
  deletes. Pin the version in the `%pip install` cell instead.

## Closing the local development loop

`LocalLakehouse.push_table()` lets you run the full pipeline locally and push
results directly to OneLake as Delta — no Fabric notebook needed during
development:

```python
from pyfabric.data import LocalLakehouse
import duckdb

con = duckdb.connect(":memory:")
# ... seed bronze data ...
build_silver(con, bronze_schema="bronze", silver_schema="silver")

lh = LocalLakehouse(cred, workspace_id=WS, lakehouse_id=LH)
lh.push_table("dim_customer", con.execute("SELECT * FROM silver.dim_customer").arrow())
```

## In the wild

- `mgc/ws_creative_planning` — projection PDF extraction pipeline
- `bc2fab_sales` — sales SemanticModel DirectLake migration
