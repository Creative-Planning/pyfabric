---
name: pyfabric — notebook Resources/builtin wheel pattern
description: How to deploy custom Python packages (including pyfabric itself) to a git-synced Fabric Spark notebook so `%pip install "builtin/..."` resolves. Covers pre-installed Spark runtime packages, the Resources/builtin layout, the Windows case-sensitivity gotcha, and the LivyClient limitation.
type: reference
---

A git-synced Fabric notebook that does `import pyfabric` fails at
runtime with `ModuleNotFoundError` unless the wheel is shipped with
the notebook artifact and installed in the first cell. This page is
the canonical pattern.

## Fabric Spark runtime 1.3 — pre-installed packages

These are available without any install step:

- `azure-identity 1.15.0`
- `azure-storage-file-datalake 12.16.0`
- `pyarrow 14.0.2`
- `requests 2.31.0`

`pyfabric`, `structlog`, `deltalake`, and most third-party packages
are **not** pre-installed.

## The Resources/builtin layout

Drop wheels into `Resources/builtin/` inside the notebook artifact:

```text
fabric/ws_x/
  nb_my_notebook.Notebook/
    .platform
    notebook-content.py
    Resources/
      builtin/
        pyfabric-0.1.0rc1-py3-none-any.whl
        structlog-25.5.0-py3-none-any.whl
```

Both `Resources/` and `builtin/` are **case-sensitive** in Fabric
git-sync. `Builtin/`, `BUILTIN/`, `resources/` will not be found at
runtime.

## First-cell install

```python
%pip install "builtin/pyfabric-0.1.0rc1-py3-none-any.whl" \
             "builtin/structlog-25.5.0-py3-none-any.whl" \
             --no-deps --quiet
```

- `builtin/...` is a Fabric notebook magic prefix that resolves to
  the notebook's `Resources/builtin/` folder. Other prefixes
  (`./builtin/...`, absolute paths) will not work.
- `--no-deps` is essential. Fabric Spark restricts outbound network
  egress, so transitive resolution against PyPI may fail. List every
  required wheel explicitly and ship them all under
  `Resources/builtin/`.
- Run this cell **before** any code that imports the package — it
  must execute before the first import even if a later cell does
  the actual work.

If you have multiple versions of the same wheel under
`Resources/builtin/` (e.g. left an older one in place because Fabric
will re-push deletes — see
`feedback_fabric_resources_no_delete.md`), let the install cell
sort by version and pick the newest:

```python
import pathlib, re

def _newest_wheel(prefix: str) -> str:
    wheels = sorted(
        pathlib.Path("builtin").glob(f"{prefix}-*.whl"),
        key=lambda p: tuple(
            int(x) if x.isdigit() else x
            for x in re.split(r"[-.]", p.stem)
            if x
        ),
    )
    return f"builtin/{wheels[-1].name}"

%pip install "$(_newest_wheel('pyfabric'))" --no-deps --quiet
```

## Downloading the wheels

```bash
pip download pyfabric==0.1.0rc1 structlog==25.5.0 \
    --no-deps \
    --dest fabric/ws_x/nb_my_notebook.Notebook/Resources/builtin/
```

### Windows case-sensitivity workaround

`pip download` on Windows creates a lowercase `resources/` folder.
Fabric git-sync requires `Resources/` (capital R). Fix with a
two-step `git mv` (case-only renames don't take effect on
case-insensitive filesystems with a single `git mv`):

```bash
git mv resources Resources_tmp
git mv Resources_tmp Resources
```

Commit and push. After the portal triggers git-sync, the wheels
are available in the notebook.

## Why LivyClient cannot use this pattern

`pyfabric.client.livy.LivyClient` creates an interactive Spark
session via the Livy REST API and runs Python code outside the
notebook runtime. Two consequences:

- `%pip install "builtin/..."` is a **notebook-only magic**. Livy
  sessions don't have a notebook context, so the `builtin/`
  resolver never fires.
- `subprocess.run([sys.executable, "-m", "pip", "install", ...])`
  inside a Livy session installs on the driver only. `import
  pyfabric` then succeeds on the driver but fails inside any UDF
  or executor-side code with `ModuleNotFoundError`.

`spark.addFile(...)` + manual `sys.path` manipulation may work for
narrow cases but is fragile and not tested in pyfabric.

**Practical guidance:** Use `LivyClient` only for SQL / DDL / data
queries against already-installed packages (the pre-installed list
above covers most data-plane needs). For notebooks that import
pyfabric or another custom package, run them through the Fabric
portal notebook UI or the Jobs API (`run_notebook` — see
`pyfabric.client.http._poll_lro`'s `Completed` handling).

## Recap

- Ship wheels in `Resources/builtin/` (capital R, lowercase b).
- Install in the first cell with `%pip install "builtin/..." --no-deps --quiet`.
- Don't delete old wheels — let the install cell pick by version.
- Don't try this from `LivyClient`; it can't see `builtin/`.
