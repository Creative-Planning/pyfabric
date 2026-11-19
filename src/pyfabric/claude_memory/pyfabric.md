---
name: pyfabric reference
description: How to use the pyfabric Python library for Microsoft Fabric workspace CRUD, OneLake/DuckDB data access, and auth. Use when the user imports pyfabric or asks about Fabric workspaces, lakehouses, OneLake, or git-sync items.
type: reference
---

**Package:** [pyfabric](https://pypi.org/project/pyfabric/) — Python library
helping AI coding assistants create and locally validate Microsoft Fabric
items compatible with Fabric git sync. Also provides workspace CRUD, OneLake
access, and a pytest plugin.

Install pyfabric and the extras your workflow uses.

```bash
pip install 'pyfabric[all]'
```

**Extras** (composable):

- `azure` for Azure authentication and REST access.
- `data` for OneLake and SQL data access.
- `lakehouse-io` for Delta table writes.
- `testing` for DuckDB and pytest fixtures, including `lakehouse-io`.
- `dataagent` for the published Data Agent MCP query client.
- `deploy` for YAML-driven parameter substitution.

**Imports worth knowing:**

```python
from pyfabric.client.auth import FabricCredential
from pyfabric.client.http import FabricClient
from pyfabric.cli import add_standard_args, run_main
from pyfabric.data import LocalLakehouse
from pyfabric.workspace.workspaces import list_workspaces
```

`pyfabric.cli.run_main(fn, parser)` is a standard wrapper: parses args, sets
up structured logging, captures exceptions to a log file, prints one-line
summary on failure. Use it for any new script — don't roll your own.

## Auth model

`FabricCredential` uses `azure.identity.DefaultAzureCredential` when it
can initialize it. The Azure CLI subprocess provider is the fallback when
initialization fails or Azure Identity is absent. `tenant=` informs the
Azure Identity provider's allowed tenants. The subprocess provider uses the
current Azure CLI account. Verify the identity and workspace permissions
for the provider actually in use.

**Pitfalls:**

- When using Azure CLI authentication in an isolated shell, preserve its
  session-specific `AZURE_CONFIG_DIR`.
- The package registers `pyfabric.testing.plugin` as a pytest entry point.
  Install the `testing` extra to use the local test fixtures.
- Lakehouse GUIDs are easiest to grab from the Fabric UI (Settings →
  Identifier). Workspace GUIDs come from listing workspaces via pyfabric.
- **logicalId identity on rebuild**: Fabric git-sync keys deployed items
  by `.platform/logicalId`; a rebuild that changes it lands as a **new**
  artifact colliding with the deployed one (same displayName, different
  logicalId) and the sync fails. Since pyfabric 0.1.11, `save_to_disk`
  on every builder **reuses the logicalId of an existing `.platform`**
  in the target directory when you don't pass `logical_id=`, so
  re-running a build script over a committed artifact is identity-stable
  by default. Passing an explicit `logical_id=` that differs from the
  on-disk one still wins but logs a `logical_id_mismatch` warning —
  that's almost always a mistake. Pinning module-level UUID constants
  via `logical_id=` remains good practice for builds into **fresh**
  directories (CI temp dirs, first-time builds). If a wrong id ever
  reaches main, restore from the first successful deploy with
  `git show <sha>:path/to/.platform`.

**Common operations:**

```python
# List workspaces
# docs-snippet: network
from pyfabric.client.http import FabricClient
from pyfabric.workspace.workspaces import list_workspaces

client = FabricClient(cred)
for ws in list_workspaces(client):
    print(ws["id"], ws["displayName"])
```

```python
# Query a synthetic lakehouse table via OneLake and DuckDB
# docs-snippet: network
from pyfabric.data import LocalLakehouse

with LocalLakehouse(":memory:", WS, LH, schema="dbo") as lh:
    lh.pull_table(cred, "customers")
    df = lh.query_df("SELECT * FROM dbo.customers LIMIT 10")
```

## Further reading

- [When to use MCP, fab, or pyfabric](mcp_vs_pyfabric.md)

- PyPI: <https://pypi.org/project/pyfabric/>
- Source: <https://github.com/Creative-Planning/pyfabric>
- `pyfabric install-claude-memory --help` — refresh these memories after upgrading pyfabric
