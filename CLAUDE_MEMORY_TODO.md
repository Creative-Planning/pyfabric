# Claude Memory TODO

Tracked list of guidance that should ship with pyfabric via
`src/pyfabric/claude_memory/` (installed into consumer projects by
`pyfabric install-claude-memory`). Each item here is a candidate memory
file to add — pick it up when the shape is stable enough to commit.

Workflow:
1. When a pattern surfaces repeatedly in consumer projects (bc2fabric,
   mcg-projections, etc.) and belongs in every pyfabric session, add a
   TODO entry here.
2. When authoring the memory, create `src/pyfabric/claude_memory/<slug>.md`
   with the standard frontmatter (`name`, `description`, `type`) and add a
   one-line pointer to `src/pyfabric/claude_memory/MEMORY.md`.
3. Remove the TODO entry once shipped. Bump the pyfabric beta so consumers
   pick it up on their next `pyfabric install-claude-memory --project`.

---

## Pending

### Favor OneLake direct over SQL endpoint for inventory/schema tasks

**Proposed slug:** `onelake_vs_sql_endpoint.md`
**Type:** `reference` (cross-cutting guidance on which connector to reach for)

**Content sketch:**

Prefer OneLake direct (`pyfabric.data.onelake.walk()` +
`deltalake.DeltaTable(path).schema()` or `LocalLakehouse`) over `FabricSql`
(SQL analytics endpoint) for discovery-style work: listing tables,
inspecting columns, row counts, delta history, quick scans.

- SQL analytics endpoint has sync latency for `MirroredDatabase` — can lag
  seconds-to-minutes behind the delta files during mirror refresh. OneLake
  reads are up-to-the-second current.
- SQL endpoint requires ODBC handshake + AAD token on first query;
  OneLake needs a single ABFS `list` call for discovery.
- Columnar projection/pushdown on parquet is strictly faster than
  serverless SQL for one-table scans.

Reach for `FabricSql` only when you need cross-table joins, WHERE-clause
pushdown, or you're specifically validating the published SQL endpoint
(e.g., testing a downstream semantic model's query path).

**Origin:** Established 2026-04-19 while planning the bc2fab_mirror table
inventory for the bc2fabric Sales.SemanticModel migration. User explicitly
asked for this guidance to travel with pyfabric.

---

### Wheel + SqlConn pattern for transform notebooks (Fabric + local)

**Proposed slug:** `wheel_sqlconn_pattern.md`
**Type:** `reference`

**Content sketch:**

When authoring a Fabric Spark notebook that materializes derived tables
(bronze→silver, silver→gold, etc.), use the **wheel-plus-SqlConn** pattern
so the same transform code runs locally (DuckDB) and in Fabric (Spark):

1. Transforms live in a small pip wheel: pure Python functions taking a
   `SqlConn` Protocol (anything with `.execute(sql)`).
2. Tests stand up a `duckdb.connect(":memory:")` and ATTACH named DBs that
   match Fabric lakehouse names; pass `silver_schema="lh_bc_silver.bc2"`
   etc. through the build function's kwargs.
3. Notebook cell 1: `%pip install "builtin/<pkg>-<version>-py3-none-any.whl" --quiet`
4. Notebook cell 2: trivial `_SparkSqlConn` shim that wraps `spark.sql`.
5. Notebook cell 3: `from <pkg>.build import build_all; build_all(shim, silver_schema="lh_bc_silver.bc2", gold_schema="lh_bc_gold.<tier>")`.
6. Wheel deploys to `<Notebook>/Resources/builtin/`, guarded by
   `notebook-settings.json: {"includeResourcesInGit": "on"}` +
   `fs-settings.json: {"gitExclusions": []}`.
7. Do NOT delete old wheels — Fabric git-sync flaps on Resources deletes.
   Version-pin in the install cell instead.
8. **Close the inner loop with `LocalLakehouse.push_table()`**: run the
   build functions against a local DuckDB (same signature as Spark in
   production) and push results directly to OneLake as delta. Skips the
   Fabric notebook entirely during development. The notebook is only
   for scheduled pipeline runs. See `mgc_projections`'s
   `scripts/extract_local.py::push_all` for the canonical form.

Canonical references in the wild:
* `mgc_projections` at `C:/Users/dave.catlett/source/repos/mgc/ws_creative_planning/`
* `bc2fab_sales` at `C:/Users/dave.catlett/source/repos/lta/bc2fabric/`

**Origin:** Crystallized 2026-04-19 during the Sales.SemanticModel DirectLake
migration in bc2fabric. Alternative approaches considered and rejected:
raw `.sql` files (no Python composition, no types), `DuckDBSparkSession`
(lacks `.write`/DataFrame API parity), PySpark-DataFrame-API locally
(requires JVM, slow inner loop).

---

## Pyfabric library gaps (build these next)

Tracked concrete features pyfabric should add to better support the wheel
+ SqlConn pattern. Each is a small, independently shippable addition.

### `pyfabric.items.notebook.NotebookBuilder`

**Type:** new module, ~150 lines.

Every project that ships a Fabric notebook reimplements cell marker
assembly (`# METADATA *`, `# CELL *`, `# MARKDOWN *`, `# MAGIC `, per-cell
language META blocks). Propose a minimal builder:

```python
nb = NotebookBuilder(kernel="synapse_pyspark")
nb.attach_lakehouse(ws_id, lh_id, default=True)
nb.add_markdown("# My notebook\n\n...")
nb.add_python("import foo; foo.run()")
nb.add_sparksql("CREATE OR REPLACE TABLE ... AS SELECT ...")
nb.pip_install_from_resources("my_pkg-0.1.0-py3-none-any.whl")
source = nb.to_source_string()    # returns notebook-content.py content
bundle = nb.to_bundle(display_name="nb_foo")   # ArtifactBundle
```

Would pair with existing `ArtifactBundle` + `save_to_disk`.

### ~~`pyfabric.data.sqlconn.SparkSqlConn`~~ — DONE (PR in flight 2026-04-21)

Shipped as `pyfabric.data.sqlconn` with the `SqlConn` Protocol +
`SparkSqlConn` shim. Claude_memory doc with usage guidance is still
pending — ship with the next beta that introduces the wheel+SqlConn
pattern memory.

### `pyfabric.testing.fixtures.snapshot_delta`

**Type:** new helper, ~50 lines.

Testing these transforms requires snapshotting small slices of OneLake
delta tables to local parquet or delta files. Every project hand-rolls
this (see bc2fabric's `tests/gold/download_fixtures.py`). Propose:

```python
snapshot_delta(
    cred,
    source="abfss://<ws>@onelake.dfs.fabric.microsoft.com/<item>/Tables/bc2/dim_customer",
    dest=Path("tests/fixtures/silver_bc2_dim_customer.parquet"),
    max_rows=5000,
    filter_expr=("BCCompany", "=", "LTA Manufacturing"),
)
```

Must handle the two common failure modes:
* `deltalake` can read plain delta (silver lakehouses) but not
  columnMapping/deletionVectors (mirrored DBs).
* Fall back to SQL analytics endpoint (pyodbc via `FabricSql`) when
  `deltalake` fails — mirrors handle both features there.

### ~~`LocalLakehouse.push_table` empty-delta behavior~~ — DONE (PR in flight 2026-04-21)

Shipped: `push_table` and `push_all` now write zero-row deltas by
default (DirectLake-compatible); callers that want the old skip
behavior pass `skip_empty=True`.

### `pyfabric.testing.fixtures.attach_duckdb_lakehouse`

**Type:** test helper, ~30 lines.

The local-DuckDB-as-Fabric-catalog setup boilerplate (ATTACH + CREATE
SCHEMA + `CREATE TABLE FROM read_parquet()`) is the same every time:

```python
from pyfabric.testing.fixtures import attach_duckdb_lakehouse
con = duckdb.connect()
attach_duckdb_lakehouse(
    con,
    "lh_bc_silver",
    schemas={
        "bc2": {"dim_customer": "tests/fixtures/silver_bc2_dim_customer.parquet"},
    },
)
# now `FROM lh_bc_silver.bc2.dim_customer` resolves
```

