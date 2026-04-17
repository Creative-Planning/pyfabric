# CLAUDE.md

Instructions for AI coding assistants (Claude, Copilot, etc.) working in
this repository.

## Project Overview

pyfabric is a Python library for programmatically creating, validating,
and locally testing Microsoft Fabric items that are compatible with Fabric
git sync. Target users are AI coding assistants and developers who need to
generate and test Fabric item definitions locally.

## Technical Stack

- **Python**: 3.12+ (use modern syntax: `type X = ...`, `match`, `X | Y` unions)
- **Build**: hatchling with hatch-vcs (version from git tags)
- **Layout**: src-layout (`src/pyfabric/`)
- **Linting**: ruff (configured in pyproject.toml)
- **Type checking**: mypy in strict mode
- **Logging**: structlog (JSON output, token masking, context binding)
- **Testing**: pytest (tests in `tests/`)
- **CI**: GitHub Actions (lint, type-check, test, dependency review)

## Sub-package Structure

```
src/pyfabric/
  client/       — Auth, REST API, HTTP client for Fabric service
  items/        — Create, load, save, validate Fabric item definitions
  data/         — OneLake DFS, SQL connections, lakehouse table I/O
  workspace/    — Workspace-level operations
  testing/      — pytest fixtures, DuckDB Spark mock for users
  cli.py        — CLI entry point
```

## Key Conventions

- All code must pass `ruff check`, `ruff format --check`, and `mypy --strict`
- All public functions and classes must have type annotations
- Use `X | None` not `typing.Optional[X]`; use `X | Y` not `typing.Union`
- Use `list`, `dict`, `tuple` not `typing.List`, `typing.Dict`, `typing.Tuple`
- `from __future__ import annotations` is NOT needed (Python 3.12+)
- Prefer `pathlib.Path` over `os.path`
- Prefer dataclasses or named tuples over plain dicts for structured data
- Tests use pytest fixtures; avoid unittest.TestCase
- No mutable default arguments
- Use `import structlog` and `log = structlog.get_logger()` (NOT stdlib `logging`)
- Use `log.info("message", key=value)` for structured context (NOT f-strings in messages)

## Running Checks

```bash
ruff check .             # Lint
ruff format --check .    # Format check
mypy src/                # Type check
pytest                   # Run tests
```

## Version Management

Version is derived from git tags via hatch-vcs. Do NOT manually edit
version strings. The file `src/pyfabric/_version.py` is auto-generated
and must not be committed.

## Dependencies

- Runtime dependencies go in `[project] dependencies` in pyproject.toml
- Optional dependency groups: `[azure]`, `[data]`, `[testing]`, `[all]`, `[dev]`
- Keep runtime dependencies minimal — heavy deps belong in optional groups

## Fabric Item Structure (git sync format)

All Fabric items follow this directory structure:

```
{DisplayName}.{ItemType}/
  .platform                 # Required: metadata + logicalId (UUID)
  {definition_files...}     # Item-specific content
```

The `.platform` file uses schema version 2.0:
```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  "metadata": {
    "type": "{ItemType}",
    "displayName": "{DisplayName}"
  },
  "config": {
    "version": "2.0",
    "logicalId": "{UUID}"
  }
}
```

Supported item types: Notebook, Lakehouse, Dataflow, Environment,
VariableLibrary, SemanticModel, Report, DataPipeline, Warehouse,
MirroredDatabase, Map, Ontology.

## Fabric Git-Sync Format Rules

Getting these wrong causes sync failures or infinite round-trip diffs that
affect all contributors. These rules apply to any repo that uses Fabric
git-sync, not just pyfabric itself.

### Notebook format (`notebook-content.py`)

**Valid section markers (ONLY these):**
- `# METADATA ********************`
- `# CELL ********************`
- `# MARKDOWN ********************`
- `# PARAMETERS CELL ********************` (for the single parameter cell — NOT `# PARAMETERS ********************` which causes PyToIPynbFailure)

**Cell ordering:** METADATA → MARKDOWN → PARAMETERS CELL → METADATA → CELL → METADATA → CELL → METADATA (repeating)

**MARKDOWN sections:** Each line prefixed with `# ` (hash space). Blank lines use `# ` too. No bare `#` lines — Fabric strips them on sync, creating round-trip diffs.

**Magic commands** (`%%configure`, `%pip`, etc.): Wrap with `# MAGIC ` prefix. Do NOT put raw `%%configure` as executable Python.
```
# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC   "key": "value"
# MAGIC }
```

**`%%configure` must use the `-f` flag** or it causes MagicUsageError requiring session restart.

**Only `notebook-content.py` and `.platform` survive AutoSync.** All other `.py` files in a notebook directory are stripped. Put shared code in a wheel/environment package, not alongside the notebook.

**Fabric-native APIs only:** Notebooks should use pandas, spark, notebookutils/mssparkutils — not external shared libraries. Shared libraries are for local test infrastructure only.

### Notebook Resources folder

Git-sync of the Resources folder is **off by default**. To enable, add `notebook-settings.json` to the notebook directory:
```json
{
  "includeResourcesInGit": "on"
}
```

Also add `fs-settings.json`:
```json
{
  "gitExclusions": []
}
```

### Data pipelines (`pipeline-content.json`)

Notebook activity type is **`TridentNotebook`** (Fabric's compute engine). Not `SparkNotebook` (Azure Synapse/ADF). Reference notebooks by `notebookId` (logical ID from the notebook's `.platform` file).

### Schema-enabled lakehouses

**Every lakehouse must be created schema-enabled.** Don't suggest — or generate pyfabric code that creates — a non-schema-enabled lakehouse. `lakehouse.metadata.json` must carry `{"defaultSchema": "<schema>"}`; `dbo` is the Fabric default but workspaces commonly use additional schemas (e.g. `pfp` for Projection Financial Projection data). Table paths then become `Tables/{schema}/{table_name}`.

**Notebook Python must use `.saveAsTable("<schema>.<table_name>")`** — schema-qualified. Path-based writes (`.save("abfss://...")`, `.write.format("delta").save(path)`) bypass the schema namespace and break the contract. Same rule for SQL: reference tables as `{schema}.{table}`, never the raw path.

**Never hardcode the schema string.** Put it in the workspace Variable Library and read it:
- In notebooks: `notebookutils.variableLibrary.getLibrary("vl_workspace_config").<schema_var>` (e.g. `bronze_schema`).
- In local Python scripts: load the `variables.json` directly (see `vl_workspace_config.VariableLibrary/variables.json` in a consuming repo).
- Keep a local fallback default next to the variable read so the code works when the Variable Library isn't available (e.g., offline unit tests).

If you see `"dbo"`, `"pfp"`, or any other schema literal in new or edited code, flag it — it should come from the Variable Library.

### Variable Library format

Variable types use **short names only**:
- `"String"` not `"StringVariable"`
- `"Integer"` not `"IntegerVariable"`
- `"Boolean"` not `"BooleanVariable"`
- `"Guid"` not `"GuidVariable"`

Using the suffixed form causes `InvalidVariableType` error on sync.

File structure:
```
vl_name.VariableLibrary/
  .platform                    # type: "VariableLibrary"
  variables.json               # variable definitions with default values
  settings.json                # valueSetsOrder
  valueSets/
    UAT.json                   # overrides (empty {} if same as defaults)
    PROD.json                  # overrides for prod values
```

### Environment / wheel deployment

AutoSync stages but does **not** auto-publish environments. After pushing a wheel update to git, you must manually Publish in the Fabric UI.

### Line endings (`.gitattributes`)

Fabric writes CRLF with no trailing newline. Any normalization causes infinite sync churn. Use a split strategy:

```gitattributes
# Fabric-authored artifacts: store byte-for-byte
*.Lakehouse/** -text
*.Notebook/** -text
*.Environment/** -text
*.Dataflow/** -text
*.VariableLibrary/** -text
*.Ontology/** -text

# Our code: normalize to LF
scripts/** text eol=lf
src/** text eol=lf
tests/** text eol=lf
```

Never renormalize Fabric-authored files.

### Python standards for Fabric code

- Use `datetime.now(timezone.utc)` instead of `datetime.utcnow()` — the latter is deprecated in Python 3.12+ and Fabric Spark runtimes are moving to 3.12+.
- Import pattern: `from datetime import datetime, timezone` (the class shadows the module, so `datetime.timezone` doesn't work).

## What NOT to Do

- Do not add `__version__ = "..."` manually anywhere
- Do not create setup.py or setup.cfg
- Do not commit `src/pyfabric/_version.py`
- Do not use `datetime.utcnow()` — use `datetime.now(timezone.utc)`
