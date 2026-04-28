---
name: pyfabric — notebook Resources/builtin wheel pattern
description: How to deploy a project's own helper wheel (e.g. `fabric_utils`, transforms) to a git-synced Fabric Spark notebook so `%pip install "builtin/..."` resolves. Covers Fabric Spark runtime 1.3 pre-installed packages, the Resources/builtin layout, the case-sensitivity gotcha, and the LivyClient limitation. **Do not** use this pattern to ship pyfabric or structlog into a notebook — pyfabric is a dev-time tool, not a notebook runtime dependency.
type: reference
---

## When to use this pattern

Use `Resources/builtin/` to ship **your project's own helper
wheel** into a git-synced Fabric Spark notebook — typically a
wheel that holds the transforms / DDL / SQL helpers that the
notebook calls. Examples: `fabric_utils-*.whl`,
`mgc_projections-*.whl`, `<project>-*.whl`. This is the canonical
way to keep the notebook itself thin (orchestration only) while
the testable logic lives in a wheel.

## What this pattern is **not** for

**Don't ship `pyfabric` or `structlog` into the notebook.** Several
sessions have wasted time trying to install pyfabric inside Fabric
(via `Resources/builtin/`, custom Environments, `%pip install
pyfabric` cells, etc.). pyfabric is a **dev-time / build-time
tool** that runs on the developer's machine to:

- create Fabric artifacts (notebooks, semantic models, reports,
  variable libraries) as files committed to a git-synced workspace
  repo,
- test those artifacts and the notebook's transform logic locally
  (DuckDB lakehouse mock, OneLake helpers, schema validation),
- manage Fabric operationally from outside (trigger notebook runs
  via the Jobs API, refresh semantic models, audit workspaces).

Notebooks running inside Fabric Spark should depend on **Fabric's
pre-installed packages** (see below) plus the project's own helper
wheel — that's it. If a notebook seems to need pyfabric, the
transform logic almost certainly belongs in the project wheel
instead, where it's locally testable; the notebook then calls the
project wheel.

## Fabric Spark runtime 1.3 — pre-installed packages

Available without any install step:

- `azure-identity 1.15.0`
- `azure-storage-file-datalake 12.16.0`
- `pyarrow 14.0.2`
- `requests 2.31.0`

Most data-plane needs (auth, OneLake DFS, parquet, REST) are
already covered. Project wheels that depend only on these install
cleanly via the pattern below.

## The Resources/builtin layout

Drop your wheel into `Resources/builtin/` inside the notebook
artifact:

```text
fabric/ws_x/
  nb_my_notebook.Notebook/
    .platform
    notebook-settings.json    # {"includeResourcesInGit": "on"}
    notebook-content.py
    Resources/
      builtin/
        my_project-0.1.7-py3-none-any.whl
```

Both `Resources/` and `builtin/` are **case-sensitive** in Fabric
git-sync. `Builtin/`, `BUILTIN/`, `resources/` will not be found
at runtime. `notebook-settings.json` is required — without
`{"includeResourcesInGit": "on"}` Fabric does **not** include the
`Resources/` subtree in git-sync, and the wheels silently
disappear on the workspace side. `NotebookBuilder.save_to_disk()`
(≥ v0.1.0rc1) emits this file unconditionally.

## First-cell install

```python
%pip install "builtin/my_project-0.1.7-py3-none-any.whl" \
             --no-deps --quiet
```

- `builtin/...` is a Fabric notebook magic prefix that resolves to
  the notebook's `Resources/builtin/` folder. Other prefixes
  (`./builtin/...`, absolute paths) will not work.
- `--no-deps` is essential. Fabric Spark restricts outbound
  network egress, so transitive resolution against PyPI may fail.
  Either pin the wheel to use only pre-installed packages, or ship
  every required wheel under `Resources/builtin/` and list them
  all in the install command.
- Run this cell **before** any code that imports the project
  wheel.

## Don't delete old wheels — sort and pick the newest

Fabric re-pushes locally-cached wheels back into git after a
delete; trying to remove an old version causes flap. Leave the
older wheel in place and let the install cell pick by version:

```python
import pathlib, re

def _newest(prefix: str) -> str:
    wheels = sorted(
        pathlib.Path("builtin").glob(f"{prefix}-*.whl"),
        key=lambda p: tuple(
            int(x) if x.isdigit() else x
            for x in re.split(r"[-.]", p.stem)
            if x
        ),
    )
    return f"builtin/{wheels[-1].name}"

%pip install "$(_newest('my_project'))" --no-deps --quiet
```

## Building / shipping the wheel

Build the wheel from the project's own `pyproject.toml` (with
`hatchling` / `setuptools` / etc.) and copy it into the notebook's
`Resources/builtin/` folder:

```bash
python -m build
cp dist/my_project-*.whl \
   fabric/ws_x/nb_my_notebook.Notebook/Resources/builtin/
```

If you already have a project script that bumps the version and
drops the wheel in place (`scripts/build_wheel.py --bump` is a
common pattern), use that — it keeps the version pin in the
notebook and the wheel filename in lockstep.

### Windows case-sensitivity workaround

If a tool creates a lowercase `resources/` folder on Windows
(case-insensitive filesystem), Fabric's git-sync — which is
case-sensitive — won't find it. Force the rename with a two-step
`git mv` (a single case-only rename is a no-op on the local
filesystem):

```bash
git mv resources Resources_tmp
git mv Resources_tmp Resources
```

Commit and push.

## Why LivyClient cannot use this pattern

`pyfabric.client.livy.LivyClient` runs Python code in a Fabric
Spark session via the Livy REST API, **outside** the notebook
runtime. Two consequences:

- `%pip install "builtin/..."` is a notebook-only magic. Livy
  sessions don't have a notebook context, so the `builtin/`
  resolver never fires.
- Driver-side `subprocess.run([sys.executable, "-m", "pip",
  "install", ...])` installs only on the driver; UDFs and
  executor-side code still hit `ModuleNotFoundError`.

**Practical guidance:** use `LivyClient` only for SQL / DDL / data
queries against pre-installed packages. For notebooks that import
a project wheel, run them through the Fabric portal notebook UI
or the Jobs API (`run_notebook`) — not via LivyClient.

## Recap

- Ship **your project's wheel** in `Resources/builtin/` (capital
  R, lowercase b). **Not pyfabric.** **Not structlog.**
- Make sure `notebook-settings.json` is `{"includeResourcesInGit":
  "on"}` — `NotebookBuilder.save_to_disk()` emits it for you.
- Install in the first cell with `%pip install "builtin/..." --no-deps --quiet`.
- Don't delete old wheels — pick by version.
- Don't try this from `LivyClient`; it can't see `builtin/`.
- If you find yourself wanting pyfabric inside the notebook, move
  the transform logic into your project wheel and have the
  notebook call the wheel. pyfabric stays on the dev machine.
