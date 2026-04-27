---
name: pyfabric reference
description: How to use the pyfabric Python library for Microsoft Fabric workspace CRUD, OneLake/DuckDB data access, and auth. Use when the user imports pyfabric or asks about Fabric workspaces, lakehouses, OneLake, or git-sync items.
type: reference
---

**Package:** [pyfabric](https://pypi.org/project/pyfabric/) — Python library
helping AI coding assistants create and locally validate Microsoft Fabric
items compatible with Fabric git sync. Also provides workspace CRUD, OneLake
access, and a pytest plugin.

**Status:** beta (`0.1.0b*`). Always install with `--pre`:

```bash
pip install --pre pyfabric[all]
```

**Extras** (composable):

- `azure` — `azure-identity`, `requests` (auth + HTTP)
- `data` — `pyodbc`, `azure-storage-file-datalake` (OneLake DFS client)
- `testing` — `duckdb`, `deltalake`, `pytest` (local lakehouse mirror +
  pytest plugin exposed under `pyfabric.testing`)

**Imports worth knowing:**

```python
from pyfabric.client.auth import FabricCredential   # az-identity wrapper
from pyfabric.workspace import workspaces           # workspace CRUD
from pyfabric.data import ...                       # OneLake / DuckDB helpers
from pyfabric.cli import add_standard_args, run_main  # CLI script scaffolding
```

`pyfabric.cli.run_main(fn, parser)` is a standard wrapper: parses args, sets
up structured logging, captures exceptions to a log file, prints one-line
summary on failure. Use it for any new script — don't roll your own.

**Auth model:** `FabricCredential` wraps `azure.identity.DefaultAzureCredential`
(with `AzureCliCredential` first). Whichever tenant `az login` is signed into
for the current shell is the tenant pyfabric talks to. Pass `tenant=` to
`FabricCredential` to override.

**Pitfalls:**

- `pip install pyfabric` (without `--pre`) fails or resolves a stub while the
  package is in beta.
- If `az` isn't signed into the target tenant, pyfabric calls return 401/403.
  Fix by signing into the right tenant in **this shell only** — don't
  polluate other windows. If the environment provides per-window tenant
  isolation (e.g. `AZURE_CONFIG_DIR` set per session), use that.
- `pyfabric.testing.plugin` registers as a pytest11 entry point — if pytest
  discovers it unexpectedly, verify you actually meant to install the
  `testing` extra.
- Lakehouse GUIDs are easiest to grab from the Fabric UI (Settings →
  Identifier). Workspace GUIDs come from listing workspaces via pyfabric.
- **Pin the `logical_id=` on every `SemanticModel(...)` / `Report(...)` you
  regenerate from a build script.** The builders mint a fresh logicalId
  via `<factory>` on each run; Fabric git-sync keys deployed items by
  `.platform/logicalId`, so a rebuild lands as a **new** artifact that
  collides with the already-deployed one (same displayName, different
  logicalId). The sync fails, listing both the deployed item (real
  ObjectId) and the new one (ObjectId `00000000-…`). Fix: declare
  module-level UUID constants once, pass them via `logical_id=` on every
  rebuild, and never let a `<factory>`-generated logicalId reach main.
  If one slips through, restore from the first successful deploy with
  `git show <sha>:path/to/.platform`.

**Common operations:**

```python
# List workspaces
from pyfabric.workspace import workspaces
for ws in workspaces.list(cred):
    print(ws.id, ws.display_name)

# Query a lakehouse table via OneLake + DuckDB
from pyfabric.data import LocalLakehouse
lh = LocalLakehouse(cred, workspace_id=WS, lakehouse_id=LH)
df = lh.query("SELECT * FROM {table} LIMIT 10", table="customers")
```

**Further reading:**

- PyPI: <https://pypi.org/project/pyfabric/>
- Source: <https://github.com/Creative-Planning/pyfabric>
- `pyfabric install-claude-memory --help` — refresh these memories after upgrading pyfabric
