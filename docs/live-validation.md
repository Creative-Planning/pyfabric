# Live validation

Use disposable, synthetic resources for REST validation. Never run the deploy
lifecycle suite in a shared or git-connected workspace: its
`unpublish_orphans(..., item_types_in_scope=["Notebook"])` deletes every
Notebook absent from the test's local repo, including unrelated Notebooks.

## Preflight and cleanup

1. Record the capacity's original state and the intended tenant. Resume a paused
   capacity before Fabric calls. Use `az fabric capacity resume` with an isolated
   `microsoft-fabric` extension, or the ARM `resume` action with API
   `2023-11-01`; poll until the state is `Active`.
2. Read the candidate workspace's git connection/status and item metadata.
   A git connection or unrelated Notebooks disqualifies it for these REST suites.
   Create an empty, disconnected throwaway workspace on the same capacity instead.
3. Create a schema-enabled lakehouse in that workspace using the Lakehouse API
   with `creationPayload: {"enableSchemas": true}`. Verify the workspace is empty
   before seeding it; do not reuse a workspace merely because its name looks temporary.
4. Run the offline baseline first, with live gates unset. Set only the disposable
   workspace/lakehouse IDs for the live run. Review every additional suite's scope
   before enabling it. The local-export check uses `PYFABRIC_TEST_WORKSPACE`, a
   filesystem path, rather than the workspace GUID.
5. Let running operations finish. Clean up only resources created by this run.
   Verify the throwaway workspace's ID, name and disconnected git state before
   deleting it. Test teardown alone is insufficient: a failed create can leave
   an item before the test enters its `finally` block.
6. Before restoring a capacity that this run resumed, inspect its activity history
   for another operator's resume and check for concurrent use. If another operator
   needs it, coordinate rather than pausing their work. Otherwise use
   `az fabric capacity suspend` (or the ARM `suspend` action), then poll for
   `Paused`. Record the observed final state, not just a successful HTTP response.

Keep tokens out of command output. Pin the tenant when acquiring tokens. With
`fab auth login --azure-cli`, first check `az account show`: fab 1.9.0 rejects
combining `--azure-cli` and `--tenant`.

```powershell
# In an environment with pyfabric's dev dependencies installed:
python -m pytest -m "not e2e" -q -o addopts=''
$env:PYFABRIC_TEST_WORKSPACE_ID = '<disposable-workspace-guid>'
$env:PYFABRIC_TEST_LAKEHOUSE_ID = '<disposable-lakehouse-guid>'
python -m pytest -m e2e -v -o addopts='' --tb=short
```

## Baseline: 2026-11-19 UTC

Checkout: `1130c6b`, Python 3.14.8, pytest 9.1.1. Source was loaded from this
checkout, not the installed pyfabric wheel. The first offline collection failed
because `allpairspy` was absent; installing 2.5.1 into ignored scratch storage
resolved collection without changing project dependencies.

| Check | Result |
| --- | --- |
| Offline suite (`-m "not e2e"`) | 1,089 passed; 6 deselected, 21.55 seconds |
| Live DDL lifecycle | Passed |
| Live same-name table rename rejection | Passed |
| Live Notebook parameters-cell round-trip | Passed |
| Live deploy create/update/orphan-delete lifecycle | Passed |
| Live Environment create/status | Failed: `SparkSettingsComputeExceedsPoolLimit` |
| Local exported workspace validation | Skipped: `PYFABRIC_TEST_WORKSPACE` unset |
| Live total | 4 passed, 1 failed, 1 skipped; 1,089 deselected, 173.04 seconds |

The Environment builder requested 20 cores / 140 memory units while the selected
F2 pool allowed 16 / 112. This is an observed baseline limitation, not a passing
Environment round-trip. No capacity upgrade or builder change was made as part
of the baseline. A separate confirmed change needs its own failing test.

The existing `ws_da_sandbox` was git-connected, not synchronized, had workspace
changes and contained Notebooks. No REST test mutations targeted it. The run
created `pyfabric_refresh_20261119` (ID
`51c9b16b-b73c-435b-ad12-c4ba5573a58c`), verified it empty and `NotConnected`,
and used its schema-enabled `refresh_validation_lh` for synthetic DDL data.

Cleanup was observed: deleting the verified disposable workspace returned HTTP
200 at 07:36:49 UTC. The capacity activity log contained only this run's
successful resume (07:26:19 UTC), with no other resume during the validation
window. After suspend initially reported `Pausing`, a subsequent ARM read
confirmed `Paused` with provisioning state `Succeeded`. No live work remains
running from this validation.

## REST metadata findings

A synthetic Notebook created with `upload_to_workspace` returned a `.platform`
whose `logicalId` was `00000000-0000-0000-0000-000000000000` through
`get_item_definition`. REST publishing therefore still does not preserve the
artifact's logical ID or its exact `.platform` bytes.
The repeat probe explicitly pinned a nonzero synthetic UUID before upload;
the response still contained the zero UUID.

The earlier claim that Fabric always adds `"description": ""` was **not**
reproduced: the returned metadata omitted that property. The owner explicitly
approved continuing PR1 with this corrected finding. Do not treat empty-description
insertion as a current guarantee. The git-sync pinning guidance remains the
existing workflow; this run did not commit or mutate a git-connected workspace
to retest git integration.

## Primary references

- [Lakehouse schema creation](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas)
- [Create Lakehouse REST API](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/items/create-lakehouse)
- [Fabric capacity CLI](https://learn.microsoft.com/en-us/cli/azure/fabric/capacity)
- [Git connection](https://learn.microsoft.com/en-us/rest/api/fabric/core/git/get-connection)
- [Ecosystem versions and tool choices](ecosystem.md)
