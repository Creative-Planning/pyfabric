# CLAUDE.md

Instructions for AI coding assistants (Claude, Copilot, etc.) working in
this repository.

## Project Overview

pyfabric is a Python library for programmatically creating, validating,
and locally testing Microsoft Fabric items that are compatible with Fabric
git sync. Target users are AI coding assistants and developers who need to
generate and test Fabric item definitions locally.

## What pyfabric is — and is not

pyfabric is a **dev-time / build-time** tool. It runs on the developer's
machine (or in CI), not inside Fabric. Its three jobs are:

1. **Create** Fabric artifacts (notebooks, semantic models, reports,
   variable libraries, lakehouse DDL) as files committed to a
   git-synced workspace repo.
2. **Test locally** — DuckDB lakehouse mock, OneLake helpers, schema
   validation, transform logic — without standing up Fabric.
3. **Manage Fabric operationally** from outside the platform — trigger
   notebook runs via the Jobs API, refresh semantic models, audit
   workspaces.

**pyfabric is NOT a notebook runtime dependency.** A Fabric Spark
notebook should not `import pyfabric`. Several past sessions have
wasted time trying to install pyfabric inside Fabric (in
`Resources/builtin/`, in custom Environments, via `%pip install
pyfabric` cells). The right pattern is:

- Notebooks stay thin — orchestration only.
- The project's transform / DDL / SQL helpers live in a project
  wheel (e.g. `fabric_utils-*.whl`) shipped via
  `Resources/builtin/` (see `claude_memory/notebook_wheel_resources_pattern.md`).
- That project wheel depends only on Fabric Spark runtime
  pre-installed packages (`azure-identity`,
  `azure-storage-file-datalake`, `pyarrow`, `requests`), not on
  pyfabric.
- pyfabric stays on the dev machine, where it builds and validates
  the notebook + the project wheel.

When you find yourself reaching for pyfabric inside a notebook,
that's a sign the logic should move into the project wheel where
it's locally testable; the notebook then calls the wheel.

## Technical Stack

- **Python**: 3.11+ (use modern syntax: `match`, `X | Y` unions, `Self` from `typing`). Avoid 3.12-only syntax — Fabric Spark runtime 1.3 is 3.11. Specifically: do NOT use `type X = Y` (PEP 695, 3.12+) or `class Foo[T]` (PEP 695, 3.12+).
- **Build**: hatchling with hatch-vcs (version from git tags)
- **Layout**: src-layout (`src/pyfabric/`)
- **Linting**: ruff (configured in pyproject.toml)
- **Type checking**: mypy in strict mode
- **Logging**: structlog (JSON output, token masking, context binding)
- **Testing**: pytest (tests in `tests/`)
- **CI**: GitHub Actions (lint, type-check, test, dependency review)

## Sub-package Structure

```text
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
- `from __future__ import annotations` is NOT needed (Python 3.11+ supports `X | Y` unions natively via PEP 604)
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

```text
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
MirroredDatabase, Map.

## Provisioning Workflow (git-first)

pyfabric ships two complementary ways to create Fabric items:

1. **Builder → git artifact** (`NotebookBuilder.save_to_disk()`,
   `MirroredDatabaseBuilder.save_to_disk()`, etc.) writes a
   `.platform` + definition files to a folder under your
   git-synced workspace repo. **This is the primary path.**
2. **REST helpers** (`create_item`, `create_mirrored_database`, …)
   call the Fabric REST API directly. **Use only for scripted
   automation in workspaces that are not git-synced.**

Mixing the two on the same workspace causes a duplicate-item
conflict: the REST-created item and the later git-synced item live
under different IDs, and Fabric will not merge them. The
git-synced item also receives a fresh ID, breaking any committed
references (e.g. notebook parameters that hard-code the mirror's
GUID).

### Recommended order for a git-synced workspace

1. Generate the artifact locally with the builder.
2. Run local tests (producer logic, schema-compat, artifact shape,
   `validate_workspace`).
3. Commit + push to the workspace's git-synced branch.
4. Have a human trigger the **manual git-sync** in the Fabric portal
   (and bind any required OAuth credentials on first refresh — see
   `claude_memory/first_refresh_cred_binding.md`).
5. Only then perform data-plane mutations (upload landing-zone
   files, `start_mirroring`, `update_definition`, …) — these
   target the just-synced item by its real GUID.

AI assistants using pyfabric in a git-synced workspace should
**pause after step 3** and wait for the user to confirm the sync
before issuing any REST mutation that would create or modify an
item.

## What NOT to Do

- Do not add `__version__ = "..."` manually anywhere
- Do not create setup.py or setup.cfg
- Do not commit `src/pyfabric/_version.py`
- Do not use `datetime.utcnow()` — use `datetime.now(timezone.utc)`
