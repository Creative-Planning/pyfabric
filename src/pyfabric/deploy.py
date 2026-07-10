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

import heapq
import json
from dataclasses import dataclass
from pathlib import Path

import structlog

from pyfabric.client.http import FabricClient
from pyfabric.items.bundle import load_from_disk, upload_to_workspace
from pyfabric.items.crud import delete_item, list_items
from pyfabric.items.types import parse_platform

log = structlog.get_logger()


class PublishOrderError(RuntimeError):
    """Raised when repo artifacts have a dependency cycle."""


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


# Coarse publish tiers by item type: data stores and environments first,
# then notebooks, then models, then the item types that reference others.
# Used as a fallback ordering for references the repo can't resolve
# precisely (e.g. a Notebook's attached Environment/Lakehouse is a
# workspace object id, not a local logicalId).
_TYPE_TIERS: dict[str, int] = {
    "Lakehouse": 0,
    "Warehouse": 0,
    "Environment": 0,
    "MirroredDatabase": 0,
    "Notebook": 1,
    "SemanticModel": 2,
    "DataPipeline": 3,
    "Report": 3,
    "DataAgent": 3,
}
_DEFAULT_TIER = 2


def _item_type_of(artifact_dir: Path) -> str:
    return artifact_dir.name.rsplit(".", 1)[1]


def _collect_notebook_ids(node: object) -> set[str]:
    """Recursively collect every ``notebookId`` value in a pipeline JSON.

    Walks the whole document so notebook activities nested inside
    container activities (ForEach / IfCondition / Until / Switch) are
    found too.
    """
    ids: set[str] = set()
    if isinstance(node, dict):
        notebook_id = node.get("notebookId")
        if isinstance(notebook_id, str) and notebook_id:
            ids.add(notebook_id)
        for value in node.values():
            ids |= _collect_notebook_ids(value)
    elif isinstance(node, list):
        for value in node:
            ids |= _collect_notebook_ids(value)
    return ids


def _read_json(path: Path) -> object | None:
    try:
        data: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.debug("deploy.dependency_scan_unreadable", path=str(path), error=str(e))
        return None
    return data


def _dependency_order(artifacts: list[Path]) -> list[Path]:
    """Order artifacts so local dependencies publish before their dependents.

    Two reference kinds resolve to precise edges:

    - **Report → SemanticModel** via ``definition.pbir``'s
      ``datasetReference.byPath.path`` (a relative directory reference);
    - **DataPipeline → Notebook** via activity ``notebookId`` values,
      which are git logicalIds matched against each Notebook artifact's
      ``.platform``.

    References that point outside the repo (or at artifacts filtered out
    of scope) are dropped with a debug log — ``publish_repo`` only
    sequences what it publishes. Everything else is ordered by a coarse
    type tier (stores/environments → notebooks → models → reports/
    pipelines), and ties break on directory name, so the order is fully
    deterministic. A dependency cycle raises :class:`PublishOrderError`.
    """
    logical_ids: dict[str, Path] = {}
    for artifact_dir in artifacts:
        try:
            platform = parse_platform(
                (artifact_dir / ".platform").read_text(encoding="utf-8")
            )
        except (OSError, ValueError):
            continue  # discovery guarantees the file exists; tolerate bad JSON
        logical_ids[platform.config.logical_id] = artifact_dir

    by_resolved_path = {a.resolve(): a for a in artifacts}
    depends_on: dict[Path, set[Path]] = {a: set() for a in artifacts}

    for artifact_dir in artifacts:
        item_type = _item_type_of(artifact_dir)
        if item_type == "Report":
            pbir = _read_json(artifact_dir / "definition.pbir")
            if not isinstance(pbir, dict):
                continue
            by_path = (pbir.get("datasetReference") or {}).get("byPath") or {}
            rel = by_path.get("path")
            if not isinstance(rel, str) or not rel:
                continue
            target = by_resolved_path.get((artifact_dir / rel).resolve())
            if target is not None and target != artifact_dir:
                depends_on[artifact_dir].add(target)
            elif target is None:
                log.debug(
                    "deploy.dependency_external",
                    artifact=artifact_dir.name,
                    reference=rel,
                )
        elif item_type == "DataPipeline":
            content = _read_json(artifact_dir / "pipeline-content.json")
            if content is None:
                continue
            for notebook_id in sorted(_collect_notebook_ids(content)):
                target = logical_ids.get(notebook_id)
                if target is not None and target != artifact_dir:
                    depends_on[artifact_dir].add(target)
                else:
                    log.debug(
                        "deploy.dependency_external",
                        artifact=artifact_dir.name,
                        reference=notebook_id,
                    )

    def sort_key(artifact_dir: Path) -> tuple[int, str]:
        return (
            _TYPE_TIERS.get(_item_type_of(artifact_dir), _DEFAULT_TIER),
            artifact_dir.name,
        )

    dependents: dict[Path, list[Path]] = {a: [] for a in artifacts}
    remaining: dict[Path, int] = {}
    for artifact_dir, deps in depends_on.items():
        remaining[artifact_dir] = len(deps)
        for dep in deps:
            dependents[dep].append(artifact_dir)

    ready = [(sort_key(a), str(a), a) for a in artifacts if remaining[a] == 0]
    heapq.heapify(ready)
    order: list[Path] = []
    while ready:
        _, _, artifact_dir = heapq.heappop(ready)
        order.append(artifact_dir)
        for dependent in dependents[artifact_dir]:
            remaining[dependent] -= 1
            if remaining[dependent] == 0:
                heapq.heappush(ready, (sort_key(dependent), str(dependent), dependent))

    if len(order) != len(artifacts):
        cycle_members = sorted(a.name for a in artifacts if remaining[a] > 0)
        raise PublishOrderError(
            "dependency cycle among artifacts: " + ", ".join(cycle_members)
        )
    return order


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

    Artifacts publish in **dependency order** (see
    :func:`_dependency_order`): a Report publishes after the
    SemanticModel its ``definition.pbir`` references, and a DataPipeline
    after the Notebooks its activities reference; everything else is
    ordered by a coarse type tier. A cycle raises
    :class:`PublishOrderError`.

    Args:
        item_types_in_scope: If provided, only artifacts whose type is
            in the list are published. ``None`` publishes everything.
            Filtered-out artifacts also drop out of the dependency
            graph (their edges are ignored).

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
    artifacts = _dependency_order(artifacts)

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
