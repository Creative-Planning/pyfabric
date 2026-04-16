# API Reference

This document describes all public modules and functions in pyfabric.

## pyfabric.client — Fabric REST API

### pyfabric.client.auth

Authentication and credential management for Microsoft Fabric.

| Function / Class | Description |
|-----------------|-------------|
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
|-----------------|-------------|
| `FabricClient(credential=None, *, base_url=None, timeout=None)` | HTTP client. Accepts FabricCredential, token string, or None (creates default). Optional `base_url` and `timeout` for testing. |
| `FabricClient.raw_request(method, url, body)` | Low-level HTTP request for custom polling patterns. Returns raw `requests.Response`. |
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
|-----------------|-------------|
| `GraphClient(client, workspace_id)` | Wrapper for graph model operations. |
| `GraphClient.list_graph_models()` | List all graph models in the workspace. |
| `GraphClient.get_definition_decoded(graph_id)` | Get definition with base64 payloads decoded. |
| `GraphClient.execute_query(graph_id, query)` | Execute a GQL query. |
| `GraphClient.refresh(graph_id, wait=True)` | Trigger an on-demand graph refresh. |

### pyfabric.client.livy

Client for the Fabric Livy API (Spark SQL execution).

| Function / Class | Description |
|-----------------|-------------|
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
|----------|-------------|
| `list_ontologies(client, ws_id)` | List all ontologies in a workspace. |
| `get_ontology(client, ws_id, ontology_id)` | Get a single ontology. |
| `create_ontology(client, ws_id, display_name)` | Create an ontology via REST API. |
| `get_ontology_definition(client, ws_id, ontology_id)` | Get the ontology definition. |
| `update_ontology_definition(client, ws_id, ontology_id, parts)` | Replace the ontology definition. |
| `delete_ontology(client, ws_id, ontology_id)` | Delete an ontology. |

#### pyfabric.client.ontology.builder

| Class | Description |
|-------|-------------|
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
|----------|-------------|
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
|-----------------|-------------|
| `sync_all_entities(client, ws_id, ontology_id, livy, lh_id, *, entity_ids=None, table_map=None)` | Sync all (or specified) entities to tables and bindings in one round trip. |
| `sync_entity_to_lakehouse(client, ws_id, ontology_id, entity_type_id, livy, lh_id, table_name)` | Sync a single entity type. |

---

## pyfabric.items — Fabric Item Definitions

### pyfabric.items.types

Item type definitions and `.platform` file parsing.

| Function / Class | Description |
|-----------------|-------------|
| `ITEM_TYPES` | Dict mapping type names to `ItemType` definitions. Registered types: Notebook, Lakehouse, Dataflow, Environment, VariableLibrary, SemanticModel, Report, DataPipeline, Warehouse, MirroredDatabase, Ontology, Map. |
| `ItemType` | Dataclass with `type_name`, `required_files`, `optional_files`, `alt_required_files`. `alt_required_files` lists alternative file sets (OR-of-ANDs) for types with multiple valid formats. |
| `parse_platform(content)` | Parse a `.platform` JSON file. Returns `PlatformFile` with metadata and config. |
| `PlatformFile` | Parsed `.platform` with `metadata.type`, `metadata.display_name`, `config.logical_id`. |

### pyfabric.items.validate

Validate Fabric item directory structures before git-syncing.

| Function / Class | Description |
|-----------------|-------------|
| `validate_item(item_dir)` | Validate a single item directory. Returns `ValidationResult`. |
| `validate_workspace(workspace_dir)` | Validate all items in a workspace directory. Returns list of results. |
| `ValidationResult` | Contains `valid` (bool), `errors`, `warnings`, `item_type`, `item_path`. |
| `ValidationError` | A single error or warning with `message` and optional `path`. |

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
|-----------------|-------------|
| `ArtifactBundle(item_type, display_name, parts)` | A complete Fabric item definition. |
| `save_to_disk(bundle, output_dir)` | Write artifact in git-sync directory format. |
| `load_from_disk(artifact_dir)` | Read a git-sync artifact directory into a bundle. |
| `upload_to_workspace(bundle, client, ws_id)` | Push artifact to workspace via REST API. |
| `diff_bundles(local, remote)` | Compare two bundles. Returns added, removed, modified paths. |

### pyfabric.items.crud

CRUD operations for Fabric workspace items via REST API.

| Function / Class | Description |
|-----------------|-------------|
| `list_items(client, workspace_id, item_type=None)` | List all items in a workspace. |
| `get_item(client, workspace_id, item_id)` | Get a single item. |
| `create_item(client, workspace_id, display_name, item_type)` | Create a workspace item. |
| `update_item(client, workspace_id, item_id, display_name=None)` | Update item metadata. |
| `delete_item(client, workspace_id, item_id)` | Delete a workspace item. |
| `encode_part(path, content)` | Build a definition part dict for the API. |
| `decode_part(part)` | Decode base64 payload from a part dict. |

---

## pyfabric.data — Data Access

### pyfabric.data.onelake

OneLake DFS (Data Lake Storage Gen2) helpers.

| Function / Class | Description |
|-----------------|-------------|
| `abfss_url(ws_id, item_id, path)` | Build an `abfss://` URL for Delta lake access. |
| `list_paths(token, ws_id, item_id, path)` | List paths using the DFS filesystem API. |
| `list_files(token, ws_id, item_id, path)` | List non-directory entries, optionally filtered by suffix. |
| `read_file(token, ws_id, item_id, path)` | Download a file as bytes. |
| `upload_file(token, ws_id, item_id, path, data)` | Upload bytes using the 3-step DFS protocol. |
| `read_parquet_df(token, ws_id, item_id, path)` | Download Parquet files and return a DataFrame. |

### pyfabric.data.sql

SQL analytics endpoint client for Fabric lakehouses and warehouses.

| Function / Class | Description |
|-----------------|-------------|
| `FabricSql(server, database, credential)` | SQL connection using pyodbc with AAD tokens. |
| `FabricSql.query_df(sql, params)` | Execute SELECT and return a pandas DataFrame. |
| `FabricSql.execute(sql, params)` | Execute DDL/DML. Returns affected row count. |
| `FabricSql.table_exists(table, schema)` | Check if a table exists. |
| `FabricSql.list_tables(schema)` | List table names in a schema. |
| `connect_lakehouse(client, credential, ws_id, lakehouse_name)` | Auto-resolve SQL endpoint from REST API. |
| `SqlError` | Raised on SQL connection or query errors. |

### pyfabric.data.lakehouse

High-level lakehouse table operations with SQL-first reads and DFS Delta writes.

| Function / Class | Description |
|-----------------|-------------|
| `write_table(credential, ws_id, lh_id, table_name, data)` | Write a DataFrame as a Delta table. |
| `read_table(credential, ws_id, lh_id, table_name)` | Read a table (SQL first, DFS fallback). |
| `WriteResult` | Result dataclass with table_path, row_count, mode, dry_run. |

---

## pyfabric.workspace — Workspace Management

### pyfabric.workspace.workspaces

CRUD operations for Fabric workspaces.

| Function / Class | Description |
|-----------------|-------------|
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
|-----------------|-------------|
| `DuckDBSparkSession(lakehouse_root=None)` | Drop-in SparkSession replacement. |
| `DuckDBSparkSession.sql(query)` | Execute SQL with automatic Delta table rewriting. |
| `DuckDBSparkSession.catalog.listTables(dbName)` | List Delta tables in a lakehouse directory. |
| `DuckDBSparkSession.catalog.tableExists(tableName)` | Check if a table exists. |
| `DataFrame` | PySpark DataFrame replacement with `collect()`, `show()`, `count()`, `toPandas()`. |
| `Row` | PySpark Row replacement with index and column-name access. |

### pyfabric.testing.mock_notebookutils

Mock for Fabric notebookutils / mssparkutils.

| Function / Class | Description |
|-----------------|-------------|
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
|---------|-------------|
| `fabric_spark` | DuckDBSparkSession with a temporary lakehouse root. |
| `mock_notebookutils` | MockNotebookUtils with a temporary filesystem root. |
| `lakehouse_root` | Path to the temporary lakehouse directory. |

### pyfabric.testing.analyze

AI-powered test and log analysis (placeholder for future Ollama integration).

| Function | Description |
|----------|-------------|
| `analyze_test_report(report_path, model)` | Analyze pytest JSON report with local LLM. (Not yet implemented.) |
| `analyze_log_file(log_path, model)` | Analyze structlog JSON log file with local LLM. (Not yet implemented.) |

---

## pyfabric.logging — Structured Logging

Dual-output logging using structlog: console (terse) and JSON Lines file (verbose).

| Function / Class | Description |
|-----------------|-------------|
| `setup_logging(script_name, verbose=False)` | Configure structured logging. Returns the log file path. |
| `get_log_path(script_name)` | Return the log file path for a script. |
| `mask_tokens_processor(logger, method_name, event_dict)` | Structlog processor that redacts JWT tokens. |
| `TokenMaskingFilter` | Stdlib logging filter for token redaction (backward compatibility). |

## pyfabric.cli — Command-Line Interface

Standard CLI argument parsing and script execution wrapper.

| Function / Class | Description |
|-----------------|-------------|
| `add_standard_args(parser, project)` | Add `--env`, `--dry-run`, `--tenant`, `--verbose` to argparse. |
| `run_main(fn, parser)` | Parse args, set up logging, run function, handle errors. |
| `register_env(project, env_name, config)` | Register an environment config. |
| `resolve_env(project, env_name)` | Look up an environment config. |
| `get_credential(args)` | Build a FabricCredential from CLI args. |
