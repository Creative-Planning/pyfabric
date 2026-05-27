"""Deploy pyfabric-emitted artifact bundles to Fabric workspaces.

The companion to the authoring layer (builders in :mod:`pyfabric.items`):
walks a repository directory of git-sync-format artifact folders,
publishes each to a target workspace via REST, and removes items that
no longer have a corresponding folder.

Composes existing pyfabric primitives:

- :func:`pyfabric.items.bundle.load_from_disk` reads each artifact
  directory into an :class:`ArtifactBundle`.
- :func:`pyfabric.items.bundle.upload_to_workspace` creates or updates
  the item.
- :func:`pyfabric.items.crud.list_items` enumerates the workspace.
- :func:`pyfabric.items.crud.delete_item` removes orphans.

Usage::

    from pyfabric.client.http import FabricClient
    from pyfabric.deploy import publish_repo, unpublish_orphans

    client = FabricClient()
    publish_repo(
        client, ws_id, "definitions/", item_types_in_scope=["Notebook", "Environment"]
    )
    unpublish_orphans(
        client, ws_id, "definitions/", item_types_in_scope=["Notebook", "Environment"]
    )

**Safety:** ``unpublish_orphans`` deletes every workspace item not
present in ``repo_dir``. Always pass ``item_types_in_scope`` unless
you are intentionally synchronizing the entire workspace. Without it,
an empty or partial ``repo_dir`` will wipe the workspace.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import structlog

from pyfabric.client.http import FabricClient
from pyfabric.items.bundle import load_from_disk, upload_to_workspace
from pyfabric.items.crud import delete_item, list_items

log = structlog.get_logger()


@dataclass(frozen=True)
class PublishResult:
    """Outcome of a single artifact publish."""

    item_id: str
    display_name: str
    item_type: str
    action: str  # "created" or "updated"


@dataclass(frozen=True)
class UnpublishResult:
    """Outcome of a single orphan deletion (or would-have-deleted in dry-run)."""

    item_id: str
    display_name: str
    item_type: str


def _discover_artifacts(
    repo_dir: Path,
    item_types_in_scope: list[str] | None,
) -> list[Path]:
    """Find every ``{DisplayName}.{ItemType}/`` directory with a ``.platform``."""
    artifacts: list[Path] = []
    for child in sorted(repo_dir.iterdir()):
        if not child.is_dir() or "." not in child.name:
            continue
        item_type = child.name.rsplit(".", 1)[1]
        if item_types_in_scope is not None and item_type not in item_types_in_scope:
            continue
        if not (child / ".platform").exists():
            continue
        artifacts.append(child)
    return artifacts


def publish_repo(
    client: FabricClient,
    workspace_id: str,
    repo_dir: Path | str,
    *,
    item_types_in_scope: list[str] | None = None,
) -> list[PublishResult]:
    """Publish every artifact bundle under ``repo_dir`` to ``workspace_id``.

    Walks ``repo_dir`` for ``{DisplayName}.{ItemType}/`` directories
    that contain a ``.platform`` file. For each, loads the bundle via
    :func:`load_from_disk` and either creates a new workspace item (if
    no item with the same display name + type exists) or updates the
    existing item's definition.

    Args:
        item_types_in_scope: If provided, only artifacts whose type is
            in the list are published. ``None`` publishes everything.

    Returns:
        One :class:`PublishResult` per published artifact.

    Raises:
        Whatever the underlying REST calls raise. Fail-fast — does not
        catch and continue.
    """
    repo_dir = Path(repo_dir)
    artifacts = _discover_artifacts(repo_dir, item_types_in_scope)
    if not artifacts:
        log.info("publish_repo_empty", repo_dir=str(repo_dir))
        return []

    # Build a lookup of existing items by (displayName, type) so we know
    # whether each artifact is a create or an update.
    existing = list_items(client, workspace_id)
    existing_by_key: dict[tuple[str, str], str] = {
        (i["displayName"], i["type"]): i["id"] for i in existing
    }

    results: list[PublishResult] = []
    for artifact_dir in artifacts:
        bundle = load_from_disk(artifact_dir)
        key = (bundle.display_name, bundle.item_type)
        existing_id = existing_by_key.get(key)
        response = upload_to_workspace(
            bundle, client, workspace_id, item_id=existing_id
        )
        # update_item_definition returns the response without an "id";
        # in that case we already know the id from the lookup.
        item_id = existing_id or str(response.get("id", ""))
        action = "updated" if existing_id else "created"
        results.append(
            PublishResult(
                item_id=item_id,
                display_name=bundle.display_name,
                item_type=bundle.item_type,
                action=action,
            )
        )
        log.info(
            "published",
            display_name=bundle.display_name,
            item_type=bundle.item_type,
            action=action,
        )
    return results


def unpublish_orphans(
    client: FabricClient,
    workspace_id: str,
    repo_dir: Path | str,
    *,
    item_types_in_scope: list[str] | None = None,
    dry_run: bool = False,
) -> list[UnpublishResult]:
    """Delete workspace items that have no matching artifact in ``repo_dir``.

    Lists the workspace, builds the set of ``(displayName, type)``
    pairs present locally in ``repo_dir``, and deletes any workspace
    item NOT in that set.

    Args:
        item_types_in_scope: If provided, only items whose type is in
            the list are considered for deletion. **Critical safety
            rail** — without this, an empty ``repo_dir`` would delete
            every item in the workspace.
        dry_run: If ``True``, return the would-delete list without
            calling :func:`delete_item`.

    Returns:
        One :class:`UnpublishResult` per orphan deleted (or per orphan
        identified in dry-run mode).
    """
    repo_dir = Path(repo_dir)
    local_artifacts = _discover_artifacts(repo_dir, item_types_in_scope)
    local_keys: set[tuple[str, str]] = set()
    for artifact_dir in local_artifacts:
        bundle = load_from_disk(artifact_dir)
        local_keys.add((bundle.display_name, bundle.item_type))

    workspace_items = list_items(client, workspace_id)
    results: list[UnpublishResult] = []
    for item in workspace_items:
        if item_types_in_scope is not None and item["type"] not in item_types_in_scope:
            continue
        if (item["displayName"], item["type"]) in local_keys:
            continue
        if not dry_run:
            delete_item(client, workspace_id, item["id"])
        results.append(
            UnpublishResult(
                item_id=item["id"],
                display_name=item["displayName"],
                item_type=item["type"],
            )
        )
        log.info(
            "orphan_identified" if dry_run else "orphan_deleted",
            display_name=item["displayName"],
            item_type=item["type"],
            dry_run=dry_run,
        )
    return results
