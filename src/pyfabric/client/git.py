"""
Client for the Fabric Git Integration REST API.

Wraps the workspace ↔ git repository sync endpoints with typed helpers.
This module starts with the read + pull operations most useful for CI/CD
automation:

- :class:`GitClient.get_status` — what's different between workspace and remote
- :class:`GitClient.update_from_git` — apply remote commits to the workspace (LRO)
- :class:`GitClient.sync_workspace` — convenience: get_status then update if behind

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

    def sync_workspace(
        self,
        workspace_id: str,
        *,
        conflict_policy: ConflictPolicy = "PreferRemote",
        allow_override_items: bool = True,
    ) -> GitStatus:
        """Pull remote into workspace if behind. No-op if already in sync.

        Wraps :meth:`get_status` and :meth:`update_from_git` for the common
        "make Fabric match the latest git commit" flow.

        Args:
            workspace_id: The workspace's GUID.
            conflict_policy: Forwarded to :meth:`update_from_git`.
            allow_override_items: Forwarded to :meth:`update_from_git`.

        Returns:
            The :class:`GitStatus` observed after the sync (or the original
            status when no sync was needed). When successful, the returned
            ``workspace_head`` equals the ``remote_commit_hash`` that was
            current when this call started.
        """
        status = self.get_status(workspace_id)
        if not status.is_behind:
            log.info(
                "git.sync_workspace.in_sync",
                workspace_id=workspace_id,
                head=status.workspace_head[:8],
            )
            return status

        self.update_from_git(
            workspace_id,
            remote_commit_hash=status.remote_commit_hash,
            workspace_head=status.workspace_head,
            conflict_policy=conflict_policy,
            allow_override_items=allow_override_items,
        )
        # Re-read status so the caller gets the post-update snapshot
        # (incl. any items the API flagged after the merge).
        return self.get_status(workspace_id)
