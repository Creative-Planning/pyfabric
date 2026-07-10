# Deploy

`pyfabric.deploy` is the deployment layer that complements the authoring
layer (builders in `pyfabric.items`). It walks a repository directory of
git-sync-format artifact folders, publishes each to a target workspace
via REST, and removes items that no longer have a corresponding folder.

It composes existing pyfabric primitives — `load_from_disk`,
`upload_to_workspace`, `list_items`, `delete_item` — so the byte
canonicalization guarantees and auth chain stay consistent end-to-end.

## When to use this

Use `pyfabric.deploy` when you want a Python-native, repo-driven
deployment story with no extra dependencies. Specifically:

- You already use pyfabric for authoring and want one tool for the
  full lifecycle.
- You need to run on Python 3.14 (or any Python version not yet covered
  by other Fabric deployment tools).
- You want the canonical-byte guarantee from `normalize_tree` enforced
  before every publish.

For workspace pulls (git remote → Fabric), use `pyfabric.client.git`
(`GitClient.sync_workspace`) — that's the complementary direction this
module doesn't cover.

## API

Two functions and two result types:

```python
from pyfabric.client.http import FabricClient
from pyfabric.deploy import publish_repo, unpublish_orphans

client = FabricClient()  # uses DefaultAzureCredential chain

# Create or update every Notebook + Environment under definitions/
results = publish_repo(
    client,
    workspace_id="...",
    repo_dir="definitions/",
    item_types_in_scope=["Notebook", "Environment"],
)
for r in results:
    print(f"{r.action}: {r.item_type} {r.display_name} (id={r.item_id})")

# Delete any Notebook + Environment in the workspace that doesn't
# have a corresponding folder in definitions/
orphans = unpublish_orphans(
    client,
    workspace_id="...",
    repo_dir="definitions/",
    item_types_in_scope=["Notebook", "Environment"],
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

- Lists workspace items via `list_items`, builds the set of
  `(displayName, type)` pairs present in `repo_dir`, and deletes any
  workspace item NOT in that set.
- `item_types_in_scope=[...]` constrains both the local-discovery side
  and the workspace-side. **Critical safety rail** — without it, an
  empty `repo_dir` deletes everything in the workspace.
- `dry_run=True` returns the would-delete list without calling
  `delete_item`.

## Recommended flow

```python
from pyfabric.client.http import FabricClient
from pyfabric.deploy import publish_repo, unpublish_orphans
from pyfabric.items.normalize import normalize_tree

REPO = "definitions/"
WS_ID = "..."
SCOPE = ["Notebook", "Environment"]

# 1. Pre-publish lint — every artifact file must already match
#    Fabric's canonical bytes. Drift here means a downstream git-sync
#    cycle would mark the file as changed by Fabric.
lint = normalize_tree(REPO, dry_run=True)
assert lint.is_canonical, f"non-canonical files: {lint.changed}"

# 2. Publish (create + update)
client = FabricClient()
publish_repo(client, WS_ID, REPO, item_types_in_scope=SCOPE)

# 3. Remove items that no longer have a corresponding folder
unpublish_orphans(client, WS_ID, REPO, item_types_in_scope=SCOPE)
```

## How REST publish handles `.platform`

Verified live against a validation workspace 2026-05-27:

- `notebook-content.py` and other content files round-trip byte-for-byte
  through `publish_repo` + `get_item_definition`.
- `.platform` does **not** round-trip. Fabric:
  - **adds** `"description": ""` to `metadata` when the bundle has no
    description, and
  - **zeros** the `logicalId` field in `getDefinition` responses,
    regardless of what was pinned locally.

The pinned `logicalId` is preserved for **git-sync** identity tracking
(see the `pyfabric-logicalid-pinning` claude-memory entry) but is NOT
preserved through REST publish + `getDefinition`. If you mix the two
paths, be aware: the same artifact authored once may appear under
different identity in each path.

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

`substitute_parameters` applies a fabric-cicd-style `parameter.yml`
find/replace to a copy of the repo, so one artifact bundle promotes
across DEV / PPE / PROD workspaces with different lakehouse ids,
connection strings, etc. Requires the `deploy` extra
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
