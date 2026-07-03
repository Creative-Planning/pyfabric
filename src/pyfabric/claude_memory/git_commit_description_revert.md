---
name: pyfabric — commitToGit can revert .platform description edits
description: updateFromGit never applies .platform metadata.description changes to an existing item, so a later commitToGit mode=All silently reverts the git-side description. Durable pattern = PATCH the item, then commit. Push direction lives on GitClient.commit_to_git / sync_workspace(direction=...).
type: feedback
---

`GitClient` covers both sync directions:

- **Pull**: `update_from_git` / `sync_workspace(direction="pull")` — apply
  remote commits to the workspace.
- **Push**: `commit_to_git` / `sync_workspace(direction="push")` — commit
  workspace-side changes back to the connected branch
  (`POST workspaces/{ws}/git/commitToGit`, `mode="All"` or `"Selective"`
  with an `items` list). It's an LRO; the `/result` fetch may 400 with
  `OperationHasNoResult`, which is benign and already tolerated.

## The description-revert hazard

`updateFromGit` does **not** apply `.platform` `metadata.description`
changes to an *existing* workspace item. The sequence that loses data:

1. You edit `description` in an item's `.platform` and commit — git has it.
2. Any later `updateFromGit` runs — the item still has no description,
   but the API reports the workspace as in sync.
3. Any later `commitToGit mode=All` writes the item's workspace state
   back — the **git-side description is silently reverted**.

### Recognizing it

`get_status` shows a workspace-side `Modified` for an item whose only
git-side edit was `.platform` metadata. `GitStatus.has_workspace_changes`
is the programmatic check — inspect *why* an item is Modified before
blanket-committing over it.

### Durable pattern

Set the description through the Items API first, then commit:

```python
client.patch(f"workspaces/{ws}/items/{item_id}", {"description": "..."})
git.commit_to_git(ws, comment="sync item description")
```

That updates the workspace item itself, so the subsequent commit writes
the same description back to git instead of erasing it.
