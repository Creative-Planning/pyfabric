---
name: pyfabric — first refresh after git-sync needs portal credential binding
description: Lakehouse.Contents-based semantic models always fail their first refresh in a new workspace until OAuth credentials are bound in the portal. Recognize the error and tell the user; this is not an agent-fixable failure.
type: feedback
---

When a `SemanticModel` whose partitions reference `Lakehouse.Contents`
git-syncs into a workspace for the first time, the very first refresh
**always fails** until somebody clicks through the portal's "bind
credentials" prompt. The artifact is fine; the workspace is missing
a one-time per-workspace credential binding for the OAuth data source.

## Error signature

In the Fabric portal, dataset refresh fails with a message similar to:

> Failed to refresh the dataset because the data source credentials
> are not configured. Please update them in the data source settings
> and refresh the dataset.

The Power BI service log / TMSL error returns one of:

- `DM_GWPipeline_UnknownError` with `"data source credentials are not configured"`
- `Microsoft.PowerBI.RefreshError`: `Credentials are required to connect to the OAuth2 source`
- `DMTS_DatasourceHasNoCredentialError` (older message form)

## Resolution (one-time per workspace)

1. Open the workspace in the Fabric portal.
2. Locate the semantic model in the items list, click the kebab menu →
   **Settings**.
3. Expand **Data source credentials**.
4. For each Lakehouse.Contents source listed, click **Edit credentials**.
5. Authentication method: **OAuth2**. Privacy level: **Organizational**
   (or whatever your tenant requires). Sign in with the account that
   owns or has refresh permission on the lakehouse.
6. Click **Sign in and continue**, then **Save**.
7. Trigger a refresh — it should now succeed.

Subsequent refreshes work without re-prompting until the credential
expires or is invalidated by an admin.

## What this means for an AI agent

This is **not** an agent-fixable failure. If you've just created or
git-synced a SemanticModel and the first refresh fails with a
credentials error, do not edit the artifact, do not bump versions,
do not retry the refresh — surface the error to the user with a
short note that they need to bind credentials in the portal once,
and link to this doc. After they've done it, refreshes work
automatically.

## When this does NOT apply

- DirectLake-only models (no Lakehouse.Contents M expression) don't
  hit this — DirectLake binds at the workspace level via the
  default lakehouse.
- Subsequent refreshes after the one-time binding work without any
  portal interaction. If a *later* refresh fails with a credentials
  error, the binding has been invalidated (admin revoke, password
  change, tenant migration); same fix path, just less common.
