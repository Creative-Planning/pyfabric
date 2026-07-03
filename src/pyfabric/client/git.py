"""
Client for the Fabric Git Integration REST API.

Wraps the workspace ↔ git repository sync endpoints with typed helpers:

- :class:`GitClient.get_status` — what's different between workspace and remote
- :class:`GitClient.update_from_git` — apply remote commits to the workspace (LRO)
- :class:`GitClient.commit_to_git` — commit workspace changes back to git (LRO)
- :class:`GitClient.sync_workspace` — convenience: status + pull and/or push

The Long-Running Operation (LRO) polling for 202 responses is handled by
the underlying :class:`pyfabric.client.http.FabricClient`. Callers don't
need to poll explicitly.

Usage::

    from pyfabric.client.auth import FabricCredential
    from pyfabric.client.git import GitClient

    git = GitClient(FabricCredential("contoso"))

    # One-shot sync: pull remote into workspace if behind.
    status = git.sync_workspace("00000000-0000-0000-0000-000000000000")
    if status.is_synced:
        print(f"In sync at {status.workspace_head[:8]}")
    else:
        print(f"Still {len(status.changes)} change(s) pending")

API docs:
    https://learn.microsoft.com/en-us/rest/api/fabric/core/git
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import structlog

from .auth import FabricCredential
from .http import FabricClient

log = structlog.get_logger()


# ── Types ───────────────────────────────────────────────────────────────────

ChangeType = Literal["Added", "Modified", "Deleted"]
ConflictType = Literal["None", "Conflict", "SameChanges"]
ConflictPolicy = Literal["PreferRemote", "PreferWorkspace"]


@dataclass(frozen=True)
class ItemChange:
    """A single item that differs between workspace and remote git."""

    item_type: str
    """Fabric item type, e.g. ``"Notebook"``, ``"Lakehouse"``, ``"Report"``."""

    display_name: str
    """Human-readable item name. Falls back to the remote name when the
    workspace doesn't have the item yet."""

    workspace_change: ChangeType | None
    """How the item changed on the workspace side, or ``None`` if it didn't."""

    remote_change: ChangeType | None
    """How the item changed on the remote side, or ``None`` if it didn't."""

    conflict_type: ConflictType
    """``"None"`` for one-sided changes, ``"Conflict"`` when both sides
    changed differently, ``"SameChanges"`` when both sides made the
    identical change."""

    @classmethod
    def _from_api(cls, payload: dict[str, Any]) -> ItemChange:
        meta = payload.get("itemMetadata") or {}
        return cls(
            item_type=meta.get("itemType", ""),
            display_name=meta.get("displayName", ""),
            workspace_change=payload.get("workspaceChange"),
            remote_change=payload.get("remoteChange"),
            conflict_type=payload.get("conflictType", "None"),
        )


@dataclass(frozen=True)
class GitStatus:
    """Snapshot of the workspace ↔ remote git relationship."""

    workspace_head: str
    """The commit SHA the workspace is currently synced to."""

    remote_commit_hash: str
    """The latest commit SHA on the connected remote branch."""

    changes: tuple[ItemChange, ...]
    """Items that differ between workspace and remote since
    ``workspace_head``."""

    @property
    def is_synced(self) -> bool:
        """True when the workspace matches remote and no items differ."""
        return self.workspace_head == self.remote_commit_hash and not self.changes

    @property
    def is_behind(self) -> bool:
        """True when the remote has commits the workspace hasn't pulled.

        This is the condition :meth:`GitClient.update_from_git` resolves."""
        return self.workspace_head != self.remote_commit_hash

    @property
    def has_workspace_changes(self) -> bool:
        """True when any item changed on the workspace side.

        This is the condition :meth:`GitClient.commit_to_git` resolves.

        Caution: a workspace-side ``Modified`` on an item whose only edit
        you made was ``.platform`` ``metadata.description`` in git is the
        *description-revert signature* — see :meth:`GitClient.commit_to_git`
        for why committing it would overwrite the git-side description."""
        return any(c.workspace_change for c in self.changes)

    @classmethod
    def _from_api(cls, payload: dict[str, Any]) -> GitStatus:
        return cls(
            workspace_head=payload.get("workspaceHead", ""),
            remote_commit_hash=payload.get("remoteCommitHash", ""),
            changes=tuple(ItemChange._from_api(c) for c in payload.get("changes", [])),
        )


# ── Client ──────────────────────────────────────────────────────────────────


class GitClient:
    """Typed wrapper for the Fabric workspace ↔ git endpoints.

    Each method maps to one REST endpoint plus its LRO polling (handled
    by the underlying :class:`FabricClient`).
    """

    def __init__(
        self,
        credential: FabricCredential | str | None = None,
        *,
        client: FabricClient | None = None,
    ):
        """Build a GitClient.

        Args:
            credential: Authentication. Either a :class:`FabricCredential`,
                a raw bearer token string, or ``None`` to use the default
                credential chain. Ignored when ``client`` is provided.
            client: Pre-built :class:`FabricClient` to reuse. Useful in
                tests and when sharing a session across multiple clients.
        """
        self._client = client if client is not None else FabricClient(credential)

    def get_status(self, workspace_id: str) -> GitStatus:
        """Return the current sync state of a workspace.

        Args:
            workspace_id: The workspace's GUID.

        Returns:
            A :class:`GitStatus` describing pending changes.

        Raises:
            FabricError: When the API returns 4xx/5xx. Common error codes:
                ``WorkspaceNotConnectedToGit``, ``WorkspaceHasNoCapacityAssigned``,
                ``InsufficientPrivileges``.
        """
        payload = self._client.get(f"workspaces/{workspace_id}/git/status")
        return GitStatus._from_api(payload)

    def update_from_git(
        self,
        workspace_id: str,
        *,
        remote_commit_hash: str,
        workspace_head: str,
        conflict_policy: ConflictPolicy = "PreferRemote",
        allow_override_items: bool = True,
    ) -> dict[str, Any]:
        """Apply remote commits to the workspace.

        This is a Long-Running Operation. The underlying client polls
        until completion before returning.

        Args:
            workspace_id: The workspace's GUID.
            remote_commit_hash: The remote SHA to advance the workspace
                to. Get this from a fresh :meth:`get_status` call.
            workspace_head: The SHA the workspace is currently at. The
                server validates this matches its view to detect races.
            conflict_policy: How to resolve items modified on both sides.
                ``"PreferRemote"`` is the typical CI choice; pull always
                wins. ``"PreferWorkspace"`` keeps workspace edits.
            allow_override_items: When ``True`` (default), the update
                proceeds even if remote items would replace workspace
                items. When ``False`` and incoming items are present,
                the operation refuses to start.

        Returns:
            The terminal operation result body, or ``{}`` for sync 200.

        Raises:
            FabricError: On API errors. Common codes:
                ``WorkspaceHeadMismatch`` (someone else synced first;
                re-read status and retry), ``WorkspacePreviousOperationInProgress``
                (an earlier sync is still running), ``MissingDependency``,
                ``InsufficientPrivileges``.
        """
        body: dict[str, Any] = {
            "remoteCommitHash": remote_commit_hash,
            "workspaceHead": workspace_head,
            "conflictResolution": {
                "conflictResolutionType": "Workspace",
                "conflictResolutionPolicy": conflict_policy,
            },
            "options": {"allowOverrideItems": allow_override_items},
        }
        log.info(
            "git.update_from_git",
            workspace_id=workspace_id,
            target=remote_commit_hash[:8],
            from_=workspace_head[:8],
            policy=conflict_policy,
        )
        return self._client.post(
            f"workspaces/{workspace_id}/git/updateFromGit", body=body
        )

    def commit_to_git(
        self,
        workspace_id: str,
        *,
        comment: str | None = None,
        items: list[dict[str, str]] | None = None,
        workspace_head: str | None = None,
    ) -> dict[str, Any]:
        """Commit workspace-side changes back to the connected git branch.

        Maps to ``POST workspaces/{id}/git/commitToGit``. Without ``items``
        this is a ``mode="All"`` commit (every workspace-side change);
        passing ``items`` switches to ``mode="Selective"`` and commits only
        those items.

        This is a Long-Running Operation; the underlying client polls to
        completion. The operation's ``/result`` fetch can return 400
        ``OperationHasNoResult`` — that is benign (commitToGit has no result
        payload) and is already tolerated by the LRO helper.

        Warning — the description-revert hazard:
            ``updateFromGit`` does NOT apply ``.platform``
            ``metadata.description`` changes to an *existing* workspace item
            (the API reports in-sync anyway). A later ``commit_to_git`` with
            ``mode="All"`` then writes the item's workspace state back,
            silently **reverting the description in git**. The durable way to
            change an existing item's description is
            ``PATCH workspaces/{ws}/items/{id}`` with the new description,
            then commit. If :meth:`get_status` shows a workspace-side
            ``Modified`` for an item whose only git-side edit was
            ``.platform`` metadata, that's this failure mode — don't blanket
            commit over it.

        Args:
            workspace_id: The workspace's GUID.
            comment: Commit message recorded on the git commit. Optional per
                the API, but strongly recommended.
            items: For a selective commit, the item identifiers to include —
                dicts with ``objectId`` and/or ``logicalId`` keys, matching
                the REST API's ``ItemIdentifier`` shape.
            workspace_head: The commit SHA the workspace is believed to be
                at; the server rejects the commit if it doesn't match
                (race detection). Omit to let the service use its current
                head.

        Returns:
            The terminal operation body, or ``{}`` when the operation has
            no result payload.

        Raises:
            FabricError: On API errors. Common codes:
                ``WorkspaceNotConnectedToGit``, ``NothingToCommit``,
                ``WorkspaceHeadMismatch``, ``InsufficientPrivileges``.
        """
        body: dict[str, Any] = {"mode": "Selective" if items else "All"}
        if comment is not None:
            body["comment"] = comment
        if items:
            body["items"] = items
        if workspace_head is not None:
            body["workspaceHead"] = workspace_head
        log.info(
            "git.commit_to_git",
            workspace_id=workspace_id,
            mode=body["mode"],
            items=len(items) if items else None,
        )
        return self._client.post(
            f"workspaces/{workspace_id}/git/commitToGit", body=body
        )

    def sync_workspace(
        self,
        workspace_id: str,
        *,
        direction: Literal["pull", "push", "both"] = "pull",
        conflict_policy: ConflictPolicy = "PreferRemote",
        allow_override_items: bool = True,
        comment: str | None = None,
    ) -> GitStatus:
        """Sync workspace and git. No-op if already in sync.

        Wraps :meth:`get_status`, :meth:`update_from_git`, and
        :meth:`commit_to_git` for the common one-shot flows:

        - ``direction="pull"`` (default): make Fabric match the latest git
          commit — pull remote into the workspace if behind.
        - ``direction="push"``: commit workspace-side changes back to git
          (e.g. Fabric normalized an item after a pull, leaving a
          workspace-side ``Modified`` that git doesn't have).
        - ``direction="both"``: pull first, then push whatever
          workspace-side changes remain.

        Before pushing, read the description-revert warning on
        :meth:`commit_to_git` — a blanket push can revert git-side
        ``.platform`` description edits that ``updateFromGit`` never applied.

        Args:
            workspace_id: The workspace's GUID.
            direction: Which way to move changes (see above).
            conflict_policy: Forwarded to :meth:`update_from_git`.
            allow_override_items: Forwarded to :meth:`update_from_git`.
            comment: Forwarded to :meth:`commit_to_git` when pushing.

        Returns:
            The :class:`GitStatus` observed after the sync (or the original
            status when no sync was needed). For a successful pull, the
            returned ``workspace_head`` equals the ``remote_commit_hash``
            that was current when this call started.
        """
        status = self.get_status(workspace_id)

        if direction in ("pull", "both") and status.is_behind:
            self.update_from_git(
                workspace_id,
                remote_commit_hash=status.remote_commit_hash,
                workspace_head=status.workspace_head,
                conflict_policy=conflict_policy,
                allow_override_items=allow_override_items,
            )
            # Re-read status so the push decision (and the caller) see the
            # post-update snapshot, incl. items the API flagged after merge.
            status = self.get_status(workspace_id)

        if direction in ("push", "both") and status.has_workspace_changes:
            self.commit_to_git(workspace_id, comment=comment)
            status = self.get_status(workspace_id)

        if status.is_synced:
            log.info(
                "git.sync_workspace.in_sync",
                workspace_id=workspace_id,
                head=status.workspace_head[:8],
            )
        return status
