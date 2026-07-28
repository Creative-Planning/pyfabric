# API Reference

This document describes all public modules and functions in pyfabric.

## pyfabric.client — Fabric REST API

### pyfabric.client.auth

Authentication and credential management for Microsoft Fabric.

| Function / Class | Description |
| ----------------- | ------------- |
| `FabricCredential(tenant=None)` | Unified credential using Azure Identity or az CLI fallback. Caches tokens per scope. |
| `FabricCredential.get_token(resource)` | Get a bearer token for a resource URL. Normalizes to scope format automatically. |
| `FabricCredential.fabric_token` | Token for the Fabric REST API. |
| `FabricCredential.storage_token` | Token for OneLake DFS (storage.azure.com). |
| `FabricCredential.sql_token` | Token for SQL analytics endpoints. |
| `AuthError` | Raised when authentication fails. |
| `get_token(resource=FABRIC_RESOURCE)` | Get a token using the default credential chain. Convenience free function for scripts. |
| `get_current_account()` | Return the current `az account show` output as a dict. |
| `az_login(tenant=None)` | Launch interactive browser login. |
| `ensure_logged_in(resource, tenant)` | Get a token, triggering login if needed. Resets the default credential after re-login. |

**Example:**

```python
from pyfabric.client.auth import FabricCredential

cred = FabricCredential(tenant="contoso")
token = cred.fabric_token
```

### pyfabric.client.http

HTTP client for the Fabric REST API v1 with retry, pagination, and LRO polling.

| Function / Class | Description |
| ----------------- | ------------- |
| `FabricClient(credential=None, *, base_url=None, timeout=None)` | HTTP client. Accepts FabricCredential, token string, or None (creates default). Optional `base_url` and `timeout` for testing. |
| `FabricClient.raw_request(method, url, body=None, params=None)` | Low-level HTTP request for custom polling patterns. Accepts absolute URLs or API-relative paths. Returns raw `requests.Response`. |
| `FabricClient.get(path, params)` | GET a single resource. |
| `FabricClient.get_paged(path, params)` | GET all pages of a paginated collection. |
| `FabricClient.post(path, body)` | POST with sync (200) and async (202/LRO) support. |
| `FabricClient.patch(path, body)` | PATCH with sync and async support. |
| `FabricClient.delete(path)` | DELETE a resource. |
| `FabricError` | Raised on HTTP 4xx/5xx. Contains status, body, and URL for diagnostics. |

**Example:**

```python
from pyfabric.client.http import FabricClient

client = FabricClient(cred)
items = client.get_paged("workspaces/ws-id/items")

# For testing against a mock server:
test_client = FabricClient(cred, base_url="http://localhost:8000/v1", timeout=5)
```

### pyfabric.client.graph

Client for the Fabric Graph Model REST API.

| Function / Class | Description |
| ----------------- | ------------- |
| `GraphClient(client, workspace_id)` | Wrapper for graph model operations. |
| `GraphClient.list_graph_models()` | List all graph models in the workspace. |
| `GraphClient.get_definition_decoded(graph_id)` | Get definition with base64 payloads decoded. |
| `GraphClient.execute_query(graph_id, query)` | Execute a GQL query. |
| `GraphClient.refresh(graph_id, wait=True)` | Trigger an on-demand graph refresh. |

### pyfabric.client.livy

Client for the Fabric Livy API (Spark SQL execution).

| Function / Class | Description |
| ----------------- | ------------- |
| `LivyClient(credential, workspace_id, lakehouse_id)` | Spark session client. Supports context manager protocol. |
| `LivyClient.create_session()` | Create a new Spark session and wait for idle state. |
| `LivyClient.sql(statement)` | Execute a Spark SQL statement. |
| `LivyClient.execute(code, kind)` | Execute arbitrary Spark/PySpark code. |
| `LivyClient.close_session()` | Delete the Spark session. |

**Example:**

```python
from pyfabric.client.livy import LivyClient

with LivyClient(cred, ws_id, lh_id) as livy:
    livy.sql("CREATE TABLE t (id STRING) USING DELTA")
    result = livy.sql("SELECT * FROM t")
```

### pyfabric.client.ontology

Ontology CRUD, builder, and definition helpers for Fabric IQ. This is a
sub-package split into focused modules for maintainability. All public
symbols are importable from `pyfabric.client.ontology`:

```python
from pyfabric.client.ontology import OntologyBuilder, create_ontology
```

#### pyfabric.client.ontology.crud

| Function | Description |
| ---------- | ------------- |
| `list_ontologies(client, ws_id)` | List all ontologies in a workspace. |
| `get_ontology(client, ws_id, ontology_id)` | Get a single ontology. |
| `create_ontology(client, ws_id, display_name)` | Create an ontology via REST API. |
| `get_ontology_definition(client, ws_id, ontology_id)` | Get the ontology definition. |
| `update_ontology_definition(client, ws_id, ontology_id, parts)` | Replace the ontology definition. |
| `delete_ontology(client, ws_id, ontology_id)` | Delete an ontology. |

#### pyfabric.client.ontology.builder

| Class | Description |
| ------- | ------------- |
| `OntologyBuilder()` | High-level builder for ontology definitions. |
| `OntologyBuilder.add_entity_type(name, properties)` | Add an entity type. Returns entity type ID. |
| `OntologyBuilder.add_data_binding(entity_type_id, ...)` | Bind entity properties to a lakehouse table. |
| `OntologyBuilder.add_relationship(name, source_id, target_id)` | Add a relationship between entity types. |
| `OntologyBuilder.validate()` | Validate the ontology. Returns list of error messages. |
| `OntologyBuilder.to_bundle(display_name)` | Build an ArtifactBundle for git-sync format. |
| `Property` | Dataclass for entity type properties. |
| `EntityType` | Dataclass for ontology entity types. |
| `DataBinding` | Dataclass for entity-to-table bindings. |
| `RelationshipType` | Dataclass for relationships between entity types. |
| `Contextualization` | Dataclass for relationship data bindings. |

#### pyfabric.client.ontology.parts

Low-level definition parts manipulation. Operates on lists of `{path, content}`
dicts decoded from the API format.

| Function | Description |
| ---------- | ------------- |
| `decode_definition(raw)` | Decode an API definition response. |
| `encode_definition(parts)` | Encode parts back to API format. |
| `make_property(name, value_type)` | Build a property dict. |
| `make_entity_type_def(name, properties)` | Build an entity type definition. |
| `make_relationship_type_def(name, source_id, target_id)` | Build a relationship type definition. |
| `make_lakehouse_binding(...)` | Build a Lakehouse data binding. |
| `make_warehouse_binding(...)` | Build a Warehouse data binding. |
| `make_kql_binding(...)` | Build a KQL (Eventhouse) data binding. |
| `add_entity_type_to_parts(parts, et_id, definition)` | Add entity type to parts list. |
| `get_entity_type_from_parts(parts, et_id)` | Get entity type from parts list. |
| `list_entity_types_from_parts(parts)` | List all entity types. |
| `build_from_config(config)` | Build ontology from a JSON config dict. |

### pyfabric.client.ontology_sync

Synchronize ontology entity types to Lakehouse tables and data bindings.

| Function / Class | Description |
| ----------------- | ------------- |
| `sync_all_entities(client, ws_id, ontology_id, livy, lh_id, *, entity_ids=None, table_map=None)` | Sync all (or specified) entities to tables and bindings in one round trip. |
| `sync_entity_to_lakehouse(client, ws_id, ontology_id, entity_type_id, livy, lh_id, table_name)` | Sync a single entity type. |

### pyfabric.client.git

Workspace ↔ git synchronization, both directions.

| Function / Class | Description |
| ----------------- | ------------- |
| `GitClient(client)` | Wrapper for the workspace git-integration API. |
| `GitClient.get_status(workspace_id)` | Return a `GitStatus` (remote/workspace heads, per-item changes). |
| `GitClient.update_from_git(workspace_id, ...)` | Pull: apply remote git commits into the workspace ("Update from Git"). |
| `GitClient.commit_to_git(workspace_id, ...)` | Push: commit workspace changes to the connected git branch. |
| `GitClient.sync_workspace(workspace_id, *, direction)` | One-call sync — `"pull"`, `"push"`, or `"both"`. |
| `GitStatus` / `ItemChange` | Status dataclasses returned by `get_status`. |

Caveat: `update_from_git` never applies `.platform` description changes to
an existing item, so a later `commit_to_git` can silently revert a
git-side description edit — PATCH the item first, then commit (see the
`git_commit_description_revert` claude-memory entry).

---

## pyfabric.items — Fabric Item Definitions

### pyfabric.items.types

Item type definitions and `.platform` file parsing.

| Function / Class | Description |
| ----------------- | ------------- |
| `ITEM_TYPES` | Dict mapping type names to `ItemType` definitions. Registered types: Notebook, Lakehouse, Dataflow, Environment, VariableLibrary, SemanticModel, Report, DataPipeline, Warehouse, MirroredDatabase, Ontology, Map. |
| `ItemType` | Dataclass with `type_name`, `required_files`, `optional_files`, `alt_required_files`. `alt_required_files` lists alternative file sets (OR-of-ANDs) for types with multiple valid formats. |
| `parse_platform(content)` | Parse a `.platform` JSON file. Returns `PlatformFile` with metadata and config. |
| `PlatformFile` | Parsed `.platform` with `metadata.type`, `metadata.display_name`, `config.logical_id`. |

### pyfabric.items.validate

Validate Fabric item directory structures before git-syncing.

| Function / Class | Description |
| ----------------- | ------------- |
| `validate_item(item_dir)` | Validate a single item directory. Returns `ValidationResult`. |
| `validate_workspace(workspace_dir)` | Validate all items in a workspace directory. Returns list of results. |
| `ValidationResult` | Contains `valid` (bool), `errors`, `warnings`, `item_type`, `item_path`. |
| `ValidationError` | A single error or warning with `message` and optional `path`. |

Beyond folder shape, `validate_item` runs type-specific checks:
**SemanticModel** items get the full TMDL lint (`lint_semantic_model`,
below — error-severity issues fail validation, warnings don't);
**Report** items get base-theme wiring validation (a missing
`themeCollection.baseTheme` or unregistered theme fails Fabric import
silently); **DataAgent** items get the instruction-guardrail lint
(warnings only).

**Example:**

```python
from pyfabric.items.validate import validate_item
from pathlib import Path

result = validate_item(Path("ws/nb_test.Notebook"))
if not result.valid:
    for e in result.errors:
        print(f"ERROR: {e.message}")
```

### pyfabric.items.bundle

Build and manage Fabric item definitions in git-sync format.

| Function / Class | Description |
| ----------------- | ------------- |
| `ArtifactBundle(item_type, display_name, parts)` | A complete Fabric item definition. |
| `save_to_disk(bundle, output_dir)` | Write artifact in git-sync directory format. |
| `load_from_disk(artifact_dir)` | Read a git-sync artifact directory into a bundle. |
| `upload_to_workspace(bundle, client, ws_id)` | Push artifact to workspace via REST API. |
| `diff_bundles(local, remote)` | Compare two bundles. Returns added, removed, modified paths. |

### pyfabric.items.crud

CRUD operations for Fabric workspace items via REST API.

| Function / Class | Description |
| ----------------- | ------------- |
| `list_items(client, workspace_id, item_type=None)` | List all items in a workspace. |
| `get_item(client, workspace_id, item_id)` | Get a single item. |
| `create_item(client, workspace_id, display_name, item_type)` | Create a workspace item. |
| `update_item(client, workspace_id, item_id, display_name=None)` | Update item metadata. |
| `delete_item(client, workspace_id, item_id)` | Delete a workspace item. |
| `encode_part(path, content)` | Build a definition part dict for the API. |
| `decode_part(part)` | Decode base64 payload from a part dict. |

### pyfabric.items.datapipeline

Build Fabric Data Pipeline (`pipeline-content.json`) artifacts that round-trip
cleanly through git-sync. Emits Fabric's canonical form: notebook activities
reference the target notebook's git `logicalId` (not its workspace object id),
`workspaceId` is zeroed, keys follow Fabric's order, and bytes are LF with no
trailing newline.

| Function / Class | Description |
| ----------------- | ------------- |
| `DataPipelineBuilder(description="")` | Builder for a pipeline definition. |
| `.add_pipeline_parameter(name, *, type="string", default_value=None)` | Declare a pipeline-level parameter (`properties.parameters`). `type` must be lowercase (`string`, `int`, `float`, `bool`, `array`, `object`, `secureString`) — the capitalized ADF form makes Fabric silently drop the whole block on git-sync, so it's rejected here. Returns a `PipelineParameter` reference. |
| `PipelineParameter(name)` | Reference to a declared pipeline parameter. Pass as an activity-parameter value to emit the Expression binding `@pipeline().parameters.<name>` instead of a literal. |
| `.add_notebook_activity(name, notebook, *, parameters=None, depends_on=None, workspace_id=None)` | Add a `TridentNotebook` activity. `notebook` is a `.Notebook` dir, its `.platform`, or a logicalId GUID. `parameters` values may be literals or `PipelineParameter` references. Returns the activity name. |
| `.add_semantic_model_refresh(name, *, dataset_id, connection, depends_on=None)` | Add a `PBISemanticModelRefresh` activity. `connection` must be a real provisioned Power BI connection id. |
| `.add_activity(name, activity_type, type_properties, *, depends_on=None, external_references=None)` | Generic activity escape hatch. |
| `.to_pipeline_content()` | Render `pipeline-content.json` as a string. |
| `.to_bundle(display_name, *, logical_id=None)` / `.save_to_disk(output_dir, *, display_name, ...)` | Materialize an `ArtifactBundle` / write the `.DataPipeline` folder with canonical bytes. |
| `notebook_logical_id(notebook)` | Resolve a notebook's git logicalId from its `.platform` (or pass a GUID through). |

Activity names are validated to Fabric's allowed set (letters, numbers, `-`,
`_`, spaces) — anything else raises before the pipeline reaches the portal.

**Example:**

```python
from pyfabric.items.datapipeline import DataPipelineBuilder

pl = DataPipelineBuilder(description="Daily refresh")
pl.add_notebook_activity("Extract", "ws/nb_extract.Notebook", parameters={"path": ""})
pl.add_notebook_activity("Transform", "ws/nb_transform.Notebook", depends_on=["Extract"])
pl.save_to_disk("ws/", display_name="pl_daily")
```

**Parameterized pipeline** (trigger-time parameters bound to an activity):

```python
pl = DataPipelineBuilder(description="Parameterized refresh")
pdf_path = pl.add_pipeline_parameter("pdf_path", default_value="")
pl.add_notebook_activity(
    "Extract", "ws/nb_extract.Notebook", parameters={"pdf_path": pdf_path}
)
# emits: properties.parameters.pdf_path = {"type": "string", "defaultValue": ""}
# and the activity param as
# {"value": {"value": "@pipeline().parameters.pdf_path", "type": "Expression"}, "type": "string"}
```

### pyfabric.items.notebook

Build Fabric `notebook-content.py` sources that round-trip byte-exactly
through git-sync (cell markers, META blocks, LF, trailing newline).
`save_to_disk` also emits `notebook-settings.json` so `Resources/` files
survive the first sync pull.

| Function / Class | Description |
| ----------------- | ------------- |
| `NotebookBuilder(kernel="synapse_pyspark")` | Fluent builder for a notebook definition. |
| `.attach_lakehouse(ws_id, lh_id, *, lh_name=None, default=False)` | Register a lakehouse dependency (at most one `default=True`). |
| `.attach_environment(env_id, *, ws_id=None)` | Attach a Fabric Environment (`None` ws = same workspace). |
| `.add_markdown(content)` | Markdown cell — every line emitted with the hash-space prefix; blank lines as hash + trailing space (bare `#` lines get stripped by Fabric and flap the file). |
| `.add_python(code)` | Python code cell. |
| `.add_sparksql(sql)` | Spark SQL cell — raw SQL body, META block `{"language": "sparksql"}` (no `language_group`; byte-verified against a Fabric-emitted fixture). |
| `.add_parameters_cell(code)` | Python cell with the `# PARAMETERS CELL` marker (Jobs API injection target). |
| `.pip_install_from_resources(wheel_name)` | Convenience `%pip install "builtin/<wheel>" --quiet` cell. |
| `.to_source_string()` / `.to_bundle(display_name, *, logical_id=None)` / `.save_to_disk(output_dir, *, display_name, logical_id=None)` | Materialize the source / an `ArtifactBundle` / the `.Notebook` folder. |

### pyfabric.items.semantic_model

Build TMDL semantic models (`*.SemanticModel` folders). Descriptions are
**required by default** on visible tables/columns/measures
(`strict_descriptions=False` to opt out with a logged warning); measures
must not collide (case-insensitively) with column names on the same table.

| Function / Class | Description |
| ----------------- | ------------- |
| `SemanticModel(name, sources, tables, relationships=[], ...)` | The model. `validate()` returns error strings; `save_to_disk(output_dir)` validates and emits the folder. |
| `LakehouseSource(name, workspace_id, lakehouse_id)` | Import via the shared `Lakehouse.Contents` navigation. **dbo-schema tables only** — a non-dbo schema is a validation error (the connector returns an empty navigation table for schema-enabled lakehouses; use `SqlEndpointSource`). |
| `SqlEndpointSource(name, server, database)` | Import via the SQL analytics endpoint (`Sql.Database` navigation with `Schema=`/`Item=` keys) — works for all schemas incl. schema-enabled lakehouses. `database` is the lakehouse display name. |
| `SqlDatabaseSource(server, endpoint_id, name="DatabaseQuery")` | DirectLake `entity` partitions over the SQL endpoint. Requires `compatibility_level=1604+`. |
| `SqlQuerySource(name, server, database)` | DirectQuery — each table sets `Table.query` (native T-SQL, inlined per partition). |
| `StaticSource(name)` | Rows defined **in the model** — each table sets `Table.m_expression` (inline M, typically `#table(...)`). For disconnected scaffolds: a label/ordinal table driving a "label \| value" detail panel, a banding or parameter table. Contributes no expression to `expressions.tmdl`; changing the rows needs only a model refresh, no lakehouse write. |
| `Table(name, source, columns, measures=[], schema="dbo", query=None, m_expression=None, ...)` | One table; partition shape follows the source kind. |
| `Column` / `Measure` / `Relationship` | Model objects. Measure names: Title Case with `%`/`#`; column names: snake_case (collision rule above). |
| `Column(..., sort_by_column="Other")` | Emits `sortByColumn`, so a label column orders by a hidden ordinal instead of alphabetically. Validated to name a different, existing column on the same table. |

`save_to_disk` reuses an existing `.platform` logicalId in the target
directory when `logical_id` isn't pinned, so rebuild scripts are
identity-stable (same for `Report` and every bundle-based builder).

### pyfabric.items.report

Build PBIR-format reports (`*.Report` folders). A non-empty report
description is required by default (surfaced via the API; deliberately
not written into `.platform`, which Fabric would strip).

| Function / Class | Description |
| ----------------- | ------------- |
| `Report(name, semantic_model_path, pages, description, theme=None)` | The report; `semantic_model_path` is the relative `../sm_x.SemanticModel` byPath reference. |
| `Page(name, display_name, visuals=[], page_refresh=None)` | One page; `page_refresh="PT5M"` enables automatic page refresh. |
| `Card` / `MultiCard` | Modern `cardVisual` (single metric / multi-metric strip). |
| `ColumnChart` / `ClusteredColumnChart` | Column chart visuals. |
| `Table` / `Slicer` | Tabular visual (with `TableOrderBy`) / slicer (fields may be a drill hierarchy). |
| `Column` / `Measure` / `Aggregate` | Field references for visuals (support `display_name` and `format_string`). |
| `Theme` / `ThemeColor` | Base-theme definition; the emitted theme wiring passes `validate_item`'s theme check. |

### pyfabric.items.environment

| Function / Class | Description |
| ----------------- | ------------- |
| `EnvironmentBuilder()` | Fluent builder — `.runtime("1.3")`, `.compute(...)`, `.pip(*pins)` — emitting `Setting/Sparkcompute.yml` (+ `environment.yml` when pins exist). Don't list `pyfabric`/`structlog` in `.pip()`; ship project wheels, not dev tooling. |
| `publish_environment(client, ws_id, env_id)` / `get_environment_status(...)` / `wait_for_published(...)` | REST publish lifecycle (publishing takes minutes). |

### pyfabric.items.mirrored_database

| Function / Class | Description |
| ----------------- | ------------- |
| `MirroredDatabaseBuilder(default_schema="dbo")` | Emit a startable Open Mirroring artifact (`mirroring.json` + `.platform`). |
| `create_mirrored_database(...)` / `start_mirroring(...)` / `stop_mirroring(...)` / `get_mirroring_status(...)` / `get_tables_mirroring_status(...)` / `wait_for_running(...)` | REST lifecycle helpers. |

The landing-zone data plane lives in `pyfabric.data.open_mirror`
(`OpenMirrorClient` — see the `open_mirror_landing_zone` claude-memory
entry for the protocol).

### pyfabric.items.jobs

Run and schedule item jobs.

| Function / Class | Description |
| ----------------- | ------------- |
| `run_on_demand(client, ws_id, item_id, job_type, ...)` / `run_notebook(client, ws_id, notebook_id, parameters=None)` | Trigger a job (notebook parameters are injected into the parameters cell). |
| `list_schedules` / `get_schedule` / `create_schedule` / `update_schedule` / `delete_schedule` | Schedule CRUD per item + job type (jobType aliases normalized). |
| `list_job_instances(client, ws_id, item_id)` | Recent job runs with status. |

### pyfabric.items.validate_tmdl

TMDL lint for SemanticModel folders — regex-based, no full TMDL parser.
Run automatically by `validate_item` for SemanticModel items.

| Function / Class | Description |
| ----------------- | ------------- |
| `lint_semantic_model(item_dir)` | Run every rule; returns `list[TmdlIssue]`. |
| `check_name_collisions(item_dir)` | Measure vs column name collision (case-insensitive, same table) — error. |
| `check_compatibility_level(item_dir)` | Error when a `directLake` partition exists below level 1604; warning when missing or below the 1567 PBIP baseline. |
| `check_orphan_columns(item_dir)` | Relationship endpoints / measure-DAX refs naming a declared table but an undeclared column — warning (calculated columns are invisible to the lint). |
| `check_dax_paren_balance(item_dir)` | Unbalanced parens per measure (DAX strings/comments stripped first) — error. |
| `check_lineage_tag_uniqueness(item_dir)` | Duplicate `lineageTag` values across the definition — error. |
| `TmdlIssue` | `path`, `message`, `severity` (`"error"` / `"warning"`). |

### pyfabric.items.data_agent

| Function / Class | Description |
| ----------------- | ------------- |
| `lint_data_agent(item_dir)` | Instruction-guardrail lint for a `*.DataAgent` folder (warnings via `validate_item`). |
| `lint_instruction_text(text)` / `validate_instructions(text, *, strict=True)` | Lint raw aiInstructions text. |

---

## pyfabric.deploy — Repo-to-Workspace Deployment

Full guide: [docs/deploy.md](deploy.md).

| Function / Class | Description |
| ----------------- | ------------- |
| `publish_repo(client, workspace_id, repo_dir, *, item_types_in_scope=None)` | Create/update every artifact folder in **dependency order** (Report → SemanticModel via `definition.pbir`, DataPipeline → Notebook via logicalId, type tiers otherwise). Cycles raise `PublishOrderError`. |
| `unpublish_orphans(client, workspace_id, repo_dir, *, item_types_in_scope=None, dry_run=False)` | Delete workspace items with no matching folder. Always pass `item_types_in_scope`. |
| `substitute_parameters(repo_dir, parameter_yml, *, environment, output_dir=None)` | Apply a fabric-cicd-style `find_replace` file for one environment; returns a byte-faithful staged copy for `publish_repo`. Requires the `deploy` extra (PyYAML). |
| `PublishResult` / `UnpublishResult` / `PublishOrderError` | Result / error types. |

---

## pyfabric.data — Data Access

### pyfabric.data.onelake

OneLake DFS (Data Lake Storage Gen2) helpers.

| Function / Class | Description |
| ----------------- | ------------- |
| `abfss_url(ws_id, item_id, path)` | Build an `abfss://` URL for Delta lake access. |
| `list_paths(token, ws_id, item_id, path)` | List paths using the DFS filesystem API. |
| `list_files(token, ws_id, item_id, path)` | List non-directory entries, optionally filtered by suffix. |
| `walk(token, ws_id, item_id, path, *, suffix=None)` | Recursively yield file entries (manual descent — DFS's recursive flag goes shallow on deep subdirectories). |
| `read_file(token, ws_id, item_id, path)` | Download a file as bytes. |
| `download_with_cache(token, ws_id, item_id, rel_path, cache_dir, ...)` | Download with local cache (size/md5 validation). |
| `upload_file(token, ws_id, item_id, path, data)` | Upload bytes using the 3-step DFS protocol. |
| `create_directory(token, ws_id, item_id, path)` | Create a directory (idempotent). Required before `rename_path` into a new directory — the DFS protocol 404s a rename whose destination parent doesn't exist. |
| `rename_path(token, ws_id, item_id, src_path, dst_path)` | Metadata-only move via `x-ms-rename-source`. |
| `delete_path(token, ws_id, item_id, path, *, recursive=False)` | Delete a file or directory (`recursive=True` for non-empty dirs). |
| `read_parquet_df(token, ws_id, item_id, path)` | Download Parquet files and return a DataFrame. |

### pyfabric.data.sql

SQL analytics endpoint client for Fabric lakehouses and warehouses.

| Function / Class | Description |
| ----------------- | ------------- |
| `FabricSql(server, database, credential)` | SQL connection using pyodbc with AAD tokens. |
| `FabricSql.query_df(sql, params)` | Execute SELECT and return a pandas DataFrame. |
| `FabricSql.execute(sql, params)` | Execute DDL/DML. Returns affected row count. |
| `FabricSql.table_exists(table, schema)` | Check if a table exists. |
| `FabricSql.list_tables(schema)` | List table names in a schema. |
| `connect_lakehouse(client, credential, ws_id, lakehouse_name)` | Auto-resolve SQL endpoint from REST API. |
| `SqlError` | Raised on SQL connection or query errors. |

### pyfabric.data.lakehouse

High-level lakehouse table operations: SQL-first reads, DFS Delta writes,
and programmatic DDL (schema/table management as OneLake directory ops).

| Function / Class | Description |
| ----------------- | ------------- |
| `delete_table(credential, ws_id, lh_id, table, schema="dbo")` | Drop a Delta table (recursive directory delete). |
| `rename_table(credential, ws_id, lh_id, src, dst, schema="dbo")` | Metadata-only table rename within a schema. |
| `rename_schema(credential, ws_id, lh_id, src_schema, dst_schema)` | Move every table to the destination schema (created first), then remove the now-empty source directory (left in place with a warning if anything unexpected remains). Partial failure raises `LakehouseRenameSchemaError` with `moved`/`failed` so callers can retry only the failures. |
| `drop_schema(credential, ws_id, lh_id, schema)` | Remove a schema directory and everything under it. |
| `list_schemas(credential, ws_id, lh_id)` / `list_tables(credential, ws_id, lh_id, schema=None)` | Enumerate schemas / tables. |
| `write_table(credential, ws_id, lh_id, table_name, data)` | Write a DataFrame as a Delta table. |
| `read_table(credential, ws_id, lh_id, table_name)` | Read a table (SQL first, DFS fallback). |
| `WriteResult` | Result dataclass with table_path, row_count, mode, dry_run. |

---

## pyfabric.workspace — Workspace Management

### pyfabric.workspace.workspaces

CRUD operations for Fabric workspaces.

| Function / Class | Description |
| ----------------- | ------------- |
| `list_workspaces(client)` | List all accessible workspaces. |
| `get_workspace(client, workspace_id)` | Get a single workspace. |
| `create_workspace(client, display_name)` | Create a new workspace. |
| `update_workspace(client, workspace_id)` | Update workspace name or description. |
| `delete_workspace(client, workspace_id)` | Delete a workspace. |
| `assign_to_capacity(client, workspace_id, capacity_id)` | Assign workspace to a Fabric capacity. |
| `add_role_assignment(client, workspace_id, principal_id, principal_type, role)` | Add a role assignment. |

---

## pyfabric.testing — Local Testing Utilities

### pyfabric.testing.duckdb_spark

DuckDB-backed Spark session mock for local notebook testing.

| Function / Class | Description |
| ----------------- | ------------- |
| `DuckDBSparkSession(lakehouse_root=None)` | Drop-in SparkSession replacement. |
| `DuckDBSparkSession.sql(query)` | Execute SQL with automatic Delta table rewriting. |
| `DuckDBSparkSession.catalog.listTables(dbName)` | List Delta tables in a lakehouse directory. |
| `DuckDBSparkSession.catalog.tableExists(tableName)` | Check if a table exists. |
| `DataFrame` | PySpark DataFrame replacement with `collect()`, `show()`, `count()`, `toPandas()`. |
| `Row` | PySpark Row replacement with index and column-name access. |

### pyfabric.testing.mock_notebookutils

Mock for Fabric notebookutils / mssparkutils.

| Function / Class | Description |
| ----------------- | ------------- |
| `MockNotebookUtils(root=None)` | Drop-in notebookutils replacement. |
| `.fs.ls(path)` | List files at path. |
| `.fs.mkdirs(path)` | Create directories. |
| `.fs.cp(src, dst, recurse)` | Copy file or directory. |
| `.fs.rm(path, recurse)` | Remove file or directory. |
| `.fs.put(path, content)` | Write content to a file. |
| `.fs.head(path)` | Read the first bytes of a file. |
| `.notebook.run(name)` | No-op (logs the call). |
| `.notebook.exit(value)` | No-op (logs the value). |
| `.credentials.getToken(audience)` | Raises `NotImplementedError` with guidance. |

### pyfabric.testing.fixtures

Pytest fixtures auto-registered via plugin entry point.

| Fixture | Description |
| --------- | ------------- |
| `fabric_spark` | DuckDBSparkSession with a temporary lakehouse root. |
| `mock_notebookutils` | MockNotebookUtils with a temporary filesystem root. |
| `lakehouse_root` | Path to the temporary lakehouse directory. |

### pyfabric.testing.analyze

AI-powered test and log analysis (placeholder for future Ollama integration).

| Function | Description |
| ---------- | ------------- |
| `analyze_test_report(report_path, model)` | Analyze pytest JSON report with local LLM. (Not yet implemented.) |
| `analyze_log_file(log_path, model)` | Analyze structlog JSON log file with local LLM. (Not yet implemented.) |

---

## pyfabric.logging — Structured Logging

Dual-output logging using structlog: console (terse) and JSON Lines file (verbose).

| Function / Class | Description |
| ----------------- | ------------- |
| `setup_logging(script_name, verbose=False)` | Configure structured logging. Returns the log file path. |
| `get_log_path(script_name)` | Return the log file path for a script. |
| `mask_tokens_processor(logger, method_name, event_dict)` | Structlog processor that redacts JWT tokens. |
| `TokenMaskingFilter` | Stdlib logging filter for token redaction (backward compatibility). |

## pyfabric.cli — Command-Line Interface

Standard CLI argument parsing and script execution wrapper.

| Function / Class | Description |
| ----------------- | ------------- |
| `add_standard_args(parser, project)` | Add `--env`, `--dry-run`, `--tenant`, `--verbose` to argparse. |
| `run_main(fn, parser)` | Parse args, set up logging, run function, handle errors. |
| `register_env(project, env_name, config)` | Register an environment config. |
| `resolve_env(project, env_name)` | Look up an environment config. |
| `get_credential(args)` | Build a FabricCredential from CLI args. |
