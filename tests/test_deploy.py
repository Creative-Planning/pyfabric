"""Tests for :mod:`pyfabric.deploy` — repo-to-workspace publish + orphan delete."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from pyfabric.deploy import (
    PublishResult,
    UnpublishResult,
    publish_repo,
    unpublish_orphans,
)
from pyfabric.items.bundle import ArtifactBundle, save_to_disk


def _make_bundle(display_name: str, item_type: str = "Notebook") -> ArtifactBundle:
    """Minimal bundle with a single text part — enough for round-trip tests."""
    return ArtifactBundle(
        item_type=item_type,
        display_name=display_name,
        parts={"notebook-content.py": "# stub\n"},
    )


# ── _discover_artifacts (via publish_repo with mocked client) ──────────────


class TestDiscovery:
    def test_empty_repo_dir_returns_empty(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_paged.return_value = []
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == []

    def test_skips_directories_without_dot_itemtype_suffix(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "no_suffix").mkdir()
        (tmp_path / "no_suffix" / ".platform").write_text("{}")
        client = MagicMock()
        client.get_paged.return_value = []
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == []

    def test_skips_artifact_dir_without_platform_file(self, tmp_path: Path) -> None:
        (tmp_path / "nb_x.Notebook").mkdir()
        client = MagicMock()
        client.get_paged.return_value = []
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == []

    def test_item_types_in_scope_filters(self, tmp_path: Path) -> None:
        save_to_disk(_make_bundle("nb_x", "Notebook"), tmp_path)
        save_to_disk(_make_bundle("env_x", "Environment"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = []
        client.post.return_value = {"id": "new-id"}
        result = publish_repo(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"]
        )
        assert len(result) == 1
        assert result[0].display_name == "nb_x"
        assert result[0].item_type == "Notebook"


# ── publish_repo ──────────────────────────────────────────────────────────


class TestPublishRepo:
    def test_creates_when_no_existing_item(self, tmp_path: Path) -> None:
        save_to_disk(_make_bundle("nb_new"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = []
        client.post.return_value = {"id": "abc-123"}
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == [
            PublishResult(
                item_id="abc-123",
                display_name="nb_new",
                item_type="Notebook",
                action="created",
            )
        ]

    def test_updates_when_existing_item_with_same_name_and_type(
        self, tmp_path: Path
    ) -> None:
        save_to_disk(_make_bundle("nb_existing"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "existing-456", "displayName": "nb_existing", "type": "Notebook"}
        ]
        client.post.return_value = {}
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == [
            PublishResult(
                item_id="existing-456",
                display_name="nb_existing",
                item_type="Notebook",
                action="updated",
            )
        ]

    def test_creates_when_existing_item_has_different_type(
        self, tmp_path: Path
    ) -> None:
        # Same displayName but different type — must create, not update.
        save_to_disk(_make_bundle("shared_name", "Notebook"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "env-id", "displayName": "shared_name", "type": "Environment"}
        ]
        client.post.return_value = {"id": "nb-id"}
        result = publish_repo(client, "ws-x", tmp_path)
        assert result[0].action == "created"
        assert result[0].item_id == "nb-id"


# ── unpublish_orphans ─────────────────────────────────────────────────────


class TestUnpublishOrphans:
    def test_deletes_workspace_items_not_in_repo(self, tmp_path: Path) -> None:
        save_to_disk(_make_bundle("nb_keep"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "keep-id", "displayName": "nb_keep", "type": "Notebook"},
            {"id": "orphan-id", "displayName": "nb_orphan", "type": "Notebook"},
        ]
        result = unpublish_orphans(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"]
        )
        assert result == [
            UnpublishResult(
                item_id="orphan-id",
                display_name="nb_orphan",
                item_type="Notebook",
            )
        ]
        client.delete.assert_called_once_with("workspaces/ws-x/items/orphan-id")

    def test_dry_run_does_not_call_delete(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "orphan-id", "displayName": "nb_orphan", "type": "Notebook"},
        ]
        result = unpublish_orphans(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"], dry_run=True
        )
        assert len(result) == 1
        client.delete.assert_not_called()

    def test_scope_excludes_other_types_from_deletion(self, tmp_path: Path) -> None:
        # Empty repo for Notebook scope — an Environment in workspace must
        # NOT be deleted because Environment is out of scope.
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "env-id", "displayName": "env_x", "type": "Environment"},
        ]
        result = unpublish_orphans(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"]
        )
        assert result == []
        client.delete.assert_not_called()

    def test_no_scope_considers_all_types(self, tmp_path: Path) -> None:
        # Empty repo, no scope → every workspace item is an orphan.
        # This is the documented footgun; the test guards the behavior.
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "nb-id", "displayName": "nb_x", "type": "Notebook"},
            {"id": "env-id", "displayName": "env_x", "type": "Environment"},
        ]
        result = unpublish_orphans(client, "ws-x", tmp_path)
        assert len(result) == 2
        assert {r.item_type for r in result} == {"Notebook", "Environment"}


# ── Integration via existing pyfabric primitives ──────────────────────────


class TestPublishAfterBuilderSaveToDisk:
    """publish_repo composes correctly with the builder-emitted layout."""

    def test_picks_up_notebook_saved_via_save_to_disk(self, tmp_path: Path) -> None:
        from pyfabric.items.notebook import NotebookBuilder

        nb = NotebookBuilder().add_python('print("hi")')
        nb.save_to_disk(tmp_path, display_name="nb_builder")

        client = MagicMock()
        client.get_paged.return_value = []
        client.post.return_value = {"id": "builder-id"}
        result = publish_repo(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"]
        )
        assert len(result) == 1
        assert result[0].display_name == "nb_builder"
        assert result[0].action == "created"


# ── Edge cases ────────────────────────────────────────────────────────────


class TestEdgeCases:
    def test_passing_str_path_for_repo_dir_works(self, tmp_path: Path) -> None:
        save_to_disk(_make_bundle("nb_str"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = []
        client.post.return_value = {"id": "x"}
        result = publish_repo(client, "ws-x", str(tmp_path))
        assert len(result) == 1

    def test_unpublish_orphans_with_str_path(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_paged.return_value = []
        result = unpublish_orphans(client, "ws-x", str(tmp_path))
        assert result == []
