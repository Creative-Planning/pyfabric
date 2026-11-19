# Deploy

`pyfabric.deploy` is the deployment layer that complements the authoring
layer (builders in `pyfabric.items`). It walks a repository directory of
git-sync-format artifact folders and publishes each via REST to a workspace
without Git integration. You can also use it to preview and remove items that
have no corresponding folder in a fully owned scope.

It uses pyfabric's existing bundle loader, definition uploader, item operations,
and authentication.

## When to use this

For workspaces without Git integration, use `pyfabric.deploy` when a Python
script already uses pyfabric to build or validate artifacts and needs to
publish them in dependency order. For standard direct repository-to-workspace
promotion, start with `fabric-cicd`. Use `fab` for interactive terminal work
such as inspecting items and jobs. See [the tooling matrix](ecosystem.md).

For Git-connected targets, build and validate the artifacts locally, commit and
push them to the connected branch, and have the workspace owner manually sync
from the Fabric portal. Follow that workflow for provisioning, publishing and
artifact removal. [Git-first provisioning workflow](../CLAUDE.md#provisioning-workflow-git-first)

## API

This example assumes the target workspace has no Git integration and the
repository owns all its Notebook and Environment items. The cleanup call
previews the items whose types are in scope and whose names are absent from the
local definitions for those types.

```python
# docs-snippet: network
from pyfabric.client.http import FabricClient
from pyfabric.deploy import publish_repo, unpublish_orphans

client = FabricClient()  # uses FabricCredential

# Create or update every Notebook + Environment under definitions/
results = publish_repo(
    client,
    workspace_id=workspace_id,
    repo_dir="definitions/",
    item_types_in_scope=["Notebook", "Environment"],
)
for r in results:
    print(f"{r.action}: {r.item_type} {r.display_name} (id={r.item_id})")

# Preview orphaned Notebook and Environment items.
orphans = unpublish_orphans(
    client,
    workspace_id=workspace_id,
    repo_dir="definitions/",
    item_types_in_scope=["Notebook", "Environment"],
    dry_run=True,
)
for orphan in orphans:
    print(
        f"Would delete {orphan.item_type} {orphan.display_name} (id={orphan.item_id})"
    )
```

Both functions accept `repo_dir` as a `str` or `Path`.

### `publish_repo`

- Walks `repo_dir` for `{DisplayName}.{ItemType}/` directories that
  contain a `.platform` file.
- For each, loads the bundle via `load_from_disk` and either creates a
  new workspace item or updates an existing one with the same display
  name + type.
- Fail-fast: a REST error on any item halts the run.
- Returns `list[PublishResult]` with the item ID, display name, type,
  and `"created"` vs `"updated"` action.

### `unpublish_orphans`

Use orphan cleanup only when the repository owns every workspace item of the
selected types, for example in a workspace dedicated to the repository.

- `item_types_in_scope=[...]` limits local discovery and workspace deletion by
  type. Every workspace item of those types whose `(displayName, type)` pair is
  absent from `repo_dir` is a deletion candidate, including items created
  manually or by other workflows. Omitting the scope considers every item type.
- `dry_run=True` returns the candidate list without deleting items. Review
  the candidates and verify they are intended deletions before running the
  cleanup with `dry_run=False`.

## Recommended flow

Run the byte preflight against the files matched by
`pyfabric.items.normalize.ARTIFACT_GLOBS`. The publisher can include files
outside those defaults, such as `notebook-settings.json` and `fs-settings.json`.
Inspect `ARTIFACT_GLOBS` for coverage and pass project-specific `extra_globs`
for additional files whose expected bytes you have verified against the
normalizer's rules. Extra patterns expand file discovery and use the same byte
rules.

```python
# docs-snippet: network
from pathlib import Path

from pyfabric.client.http import FabricClient
from pyfabric.deploy import publish_repo, unpublish_orphans
from pyfabric.items.normalize import normalize_tree

REPO = "definitions/"
WS_ID = workspace_id
SCOPE = ["Notebook", "Environment"]

# 1. Check files matched by the default artifact patterns.
lint = normalize_tree(Path(REPO), dry_run=True)
assert lint.is_canonical, f"non-canonical files: {lint.changed}"

# 2. Publish (create + update)
client = FabricClient()
publish_repo(client, WS_ID, REPO, item_types_in_scope=SCOPE)

# 3. Preview cleanup once this repo owns every item of the selected types.
orphans = unpublish_orphans(
    client, WS_ID, REPO, item_types_in_scope=SCOPE, dry_run=True
)
for orphan in orphans:
    print(
        f"Would delete {orphan.item_type} {orphan.display_name} (id={orphan.item_id})"
    )
```

## How REST publish handles `.platform`

The disposable-workspace Notebook probe returned a zeroed `logicalId` in
`.platform` after REST publish and `getDefinition`, even though the input
contained an explicit nonzero ID. The returned metadata omitted the optional
`description`. See [the live probe](live-validation.md#rest-metadata-findings).

Use the pinned `.platform` ID for git-sync identity tracking. Use the
workspace item ID returned by REST when operating on the published item.

## Publish ordering

`publish_repo` publishes in **dependency order**, not directory order:

- **Report → SemanticModel**: a Report publishes after the model its
  `definition.pbir` references via `datasetReference.byPath.path`.
- **DataPipeline → Notebook**: a pipeline publishes after every
  Notebook whose git logicalId appears as an activity `notebookId`
  (including activities nested in ForEach / IfCondition / Until
  containers).
- Everything else follows a coarse **type tier**: Lakehouse /
  Warehouse / Environment / MirroredDatabase first, then Notebook,
  then SemanticModel, then DataPipeline / Report / DataAgent. Ties
  break on directory name, so the order is fully deterministic.

Limitations: a Notebook's attached Environment / Lakehouse reference
is a **workspace object id**, not a local logicalId, so those pairs
are ordered only by the type tier (which puts the Environment /
Lakehouse first anyway). References that point outside the repo — or
at artifacts excluded by `item_types_in_scope` — are ignored with a
debug log. A dependency **cycle** raises `PublishOrderError` naming
the participating artifacts.

## Parameter substitution (multi-environment promotion)

`substitute_parameters` applies the supported `find_replace` subset of
fabric-cicd-style `parameter.yml` to a copy of the repo, so one artifact bundle promotes
across DEV / PPE / PROD workspaces with different lakehouse ids,
connection strings, etc. Use the direct publish below for targets without Git
integration. Requires the `deploy` extra
(`pip install 'pyfabric[deploy]'` — pulls in PyYAML).

```yaml
# parameter.yml
find_replace:
  - find_value: "11111111-1111-1111-1111-111111111111"   # DEV lakehouse id
    replace_value:
      DEV: "11111111-1111-1111-1111-111111111111"
      PROD: "22222222-2222-2222-2222-222222222222"
```

```python
# docs-snippet: network
from pyfabric.deploy import publish_repo, substitute_parameters

staged = substitute_parameters(
    "definitions/", "parameter.yml", environment="PROD"
)  # returns a temp copy; pass output_dir= to control the location
publish_repo(client, prod_ws_id, staged, item_types_in_scope=["Notebook"])
```

Guarantees: the source repo is never modified; the copy is
byte-faithful except the replaced spans; binary files (e.g.
`Resources/builtin/*.whl`) copy untouched; a missing `parameter.yml`
raises instead of silently no-oping; an entry with no value for the
requested environment raises naming the variable and the environments
it does define.

## Not currently in scope

These would be follow-ups, not v1:

- **Concurrent publish.** Items publish serially. Fast enough for
  typical workspaces; parallelize externally if needed.

## Testing your own deploy flow

`tests/test_deploy_e2e.py` is the reference: it builds two notebooks
with `NotebookBuilder`, publishes them, re-publishes (asserts update),
removes one local artifact, runs `unpublish_orphans` dry-run then live,
and cleans up. Gate any similar live tests on the `PYFABRIC_TEST_WORKSPACE_ID`
env var so they skip cleanly in environments without a workspace.
