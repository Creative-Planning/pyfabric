"""Tests for GitClient (workspace ↔ git sync)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyfabric.client.git import (
    GitClient,
    GitStatus,
    ItemChange,
)

# ── ItemChange._from_api / GitStatus._from_api ─────────────────────────────


class TestItemChangeFromApi:
    def test_remote_only_change(self):
        payload = {
            "itemMetadata": {"itemType": "Notebook", "displayName": "nb_extract"},
            "remoteChange": "Modified",
            "conflictType": "None",
        }
        ch = ItemChange._from_api(payload)
        assert ch.item_type == "Notebook"
        assert ch.display_name == "nb_extract"
        assert ch.remote_change == "Modified"
        assert ch.workspace_change is None
        assert ch.conflict_type == "None"

    def test_workspace_only_change(self):
        payload = {
            "itemMetadata": {"itemType": "Report", "displayName": "rpt_status"},
            "workspaceChange": "Added",
            "conflictType": "None",
        }
        ch = ItemChange._from_api(payload)
        assert ch.workspace_change == "Added"
        assert ch.remote_change is None

    def test_two_sided_conflict(self):
        payload = {
            "itemMetadata": {"itemType": "SemanticModel", "displayName": "sm_kpi"},
            "workspaceChange": "Modified",
            "remoteChange": "Modified",
            "conflictType": "Conflict",
        }
        ch = ItemChange._from_api(payload)
        assert ch.conflict_type == "Conflict"
        assert ch.workspace_change == "Modified"
        assert ch.remote_change == "Modified"

    def test_missing_metadata_defaults_empty(self):
        ch = ItemChange._from_api({})
        assert ch.item_type == ""
        assert ch.display_name == ""
        assert ch.conflict_type == "None"


class TestGitStatusFromApi:
    def test_in_sync_payload(self):
        payload = {
            "workspaceHead": "abc123",
            "remoteCommitHash": "abc123",
            "changes": [],
        }
        status = GitStatus._from_api(payload)
        assert status.is_synced
        assert not status.is_behind
        assert status.changes == ()

    def test_behind_payload(self):
        payload = {
            "workspaceHead": "abc123",
            "remoteCommitHash": "def456",
            "changes": [
                {
                    "itemMetadata": {"itemType": "Notebook", "displayName": "n"},
                    "remoteChange": "Modified",
                    "conflictType": "None",
                }
            ],
        }
        status = GitStatus._from_api(payload)
        assert status.is_behind
        assert not status.is_synced
        assert len(status.changes) == 1
        assert status.changes[0].item_type == "Notebook"

    def test_same_hash_but_workspace_dirty_not_synced(self):
        # Edge case: hashes match but workspace has unpushed local edits.
        # The Fabric API surfaces this as workspaceChange entries with
        # matching hashes. is_synced should be False so callers don't
        # assume "nothing to do."
        payload = {
            "workspaceHead": "abc123",
            "remoteCommitHash": "abc123",
            "changes": [
                {
                    "itemMetadata": {"itemType": "Report", "displayName": "r"},
                    "workspaceChange": "Modified",
                    "conflictType": "None",
                }
            ],
        }
        status = GitStatus._from_api(payload)
        assert not status.is_synced
        assert not status.is_behind  # head matches; workspace just dirty


# ── GitClient ──────────────────────────────────────────────────────────────


def _make_client(get_return=None, post_return=None):
    """Build a GitClient wrapping a MagicMock FabricClient."""
    fabric = MagicMock()
    if get_return is not None:
        fabric.get.return_value = get_return
    if post_return is not None:
        fabric.post.return_value = post_return
    return GitClient(client=fabric), fabric


class TestGitClientGetStatus:
    def test_calls_correct_path(self):
        client, fabric = _make_client(
            get_return={
                "workspaceHead": "a" * 40,
                "remoteCommitHash": "a" * 40,
                "changes": [],
            }
        )
        client.get_status("ws-1")
        fabric.get.assert_called_once_with("workspaces/ws-1/git/status")

    def test_returns_typed_status(self):
        client, _ = _make_client(
            get_return={
                "workspaceHead": "old",
                "remoteCommitHash": "new",
                "changes": [
                    {
                        "itemMetadata": {
                            "itemType": "Notebook",
                            "displayName": "nb_x",
                        },
                        "remoteChange": "Added",
                        "conflictType": "None",
                    }
                ],
            }
        )
        status = client.get_status("ws-1")
        assert isinstance(status, GitStatus)
        assert status.is_behind
        assert status.changes[0].display_name == "nb_x"


class TestGitClientUpdateFromGit:
    def test_request_body_shape(self):
        client, fabric = _make_client(post_return={})
        client.update_from_git(
            "ws-1",
            remote_commit_hash="new",
            workspace_head="old",
        )
        path, kwargs = fabric.post.call_args
        # FabricClient.post(path, body=...)
        assert path[0] == "workspaces/ws-1/git/updateFromGit"
        body = kwargs["body"]
        assert body["remoteCommitHash"] == "new"
        assert body["workspaceHead"] == "old"
        assert body["conflictResolution"]["conflictResolutionPolicy"] == "PreferRemote"
        assert body["conflictResolution"]["conflictResolutionType"] == "Workspace"
        assert body["options"]["allowOverrideItems"] is True

    def test_overrides_propagate(self):
        client, fabric = _make_client(post_return={})
        client.update_from_git(
            "ws-1",
            remote_commit_hash="new",
            workspace_head="old",
            conflict_policy="PreferWorkspace",
            allow_override_items=False,
        )
        body = fabric.post.call_args.kwargs["body"]
        assert (
            body["conflictResolution"]["conflictResolutionPolicy"] == "PreferWorkspace"
        )
        assert body["options"]["allowOverrideItems"] is False


class TestGitClientSyncWorkspace:
    def test_noop_when_in_sync(self):
        client, fabric = _make_client(
            get_return={
                "workspaceHead": "abc",
                "remoteCommitHash": "abc",
                "changes": [],
            }
        )
        status = client.sync_workspace("ws-1")
        # Only one get_status call; no update_from_git invocation.
        assert fabric.get.call_count == 1
        fabric.post.assert_not_called()
        assert status.is_synced

    def test_pulls_when_behind(self):
        # First get_status returns 'behind', then update_from_git, then
        # second get_status returns 'in sync'.
        behind = {
            "workspaceHead": "old",
            "remoteCommitHash": "new",
            "changes": [
                {
                    "itemMetadata": {"itemType": "Notebook", "displayName": "n"},
                    "remoteChange": "Modified",
                    "conflictType": "None",
                }
            ],
        }
        in_sync = {"workspaceHead": "new", "remoteCommitHash": "new", "changes": []}
        fabric = MagicMock()
        fabric.get.side_effect = [behind, in_sync]
        fabric.post.return_value = {}
        client = GitClient(client=fabric)

        status = client.sync_workspace("ws-1")
        assert fabric.get.call_count == 2
        # update_from_git was called with the hashes from the first status read
        body = fabric.post.call_args.kwargs["body"]
        assert body["remoteCommitHash"] == "new"
        assert body["workspaceHead"] == "old"
        assert status.is_synced

    def test_sync_propagates_conflict_policy(self):
        behind = {
            "workspaceHead": "old",
            "remoteCommitHash": "new",
            "changes": [],
        }
        in_sync = {"workspaceHead": "new", "remoteCommitHash": "new", "changes": []}
        fabric = MagicMock()
        fabric.get.side_effect = [behind, in_sync]
        fabric.post.return_value = {}
        client = GitClient(client=fabric)

        client.sync_workspace("ws-1", conflict_policy="PreferWorkspace")
        body = fabric.post.call_args.kwargs["body"]
        assert (
            body["conflictResolution"]["conflictResolutionPolicy"] == "PreferWorkspace"
        )


# ── Constructor ────────────────────────────────────────────────────────────


class TestGitClientConstructor:
    def test_reuses_supplied_client(self):
        fabric = MagicMock()
        client = GitClient(client=fabric)
        assert client._client is fabric

    def test_builds_fabric_client_when_none(self, monkeypatch):
        sentinel = MagicMock()
        called: dict[str, object] = {}

        def fake_ctor(credential):
            called["credential"] = credential
            return sentinel

        monkeypatch.setattr("pyfabric.client.git.FabricClient", fake_ctor)
        client = GitClient("static-token")
        assert client._client is sentinel
        assert called["credential"] == "static-token"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
