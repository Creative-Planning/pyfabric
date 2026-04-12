"""
Build Fabric item definitions in git-sync format and optionally upload via REST.

Primary path: write to disk -> commit to git -> Fabric auto-syncs.
Fallback path: upload_to_workspace() for workspaces without git integration.

Git-sync directory format:
    {output_dir}/{DisplayName}.{ItemType}/
        .platform                   # item metadata + logical ID
        {definition files...}       # e.g. notebook-content.py, model.bim, etc.

Usage:
    from pyfabric.items.bundle import ArtifactBundle, save_to_disk, upload_to_workspace

    bundle = ArtifactBundle(
        item_type="Notebook",
        display_name="NB_My_Notebook",
        parts={"notebook-content.py": notebook_source},
    )

    # Primary: write to git-synced directory
    save_to_disk(bundle, "definitions/")

    # Fallback: push to workspace directly
    upload_to_workspace(bundle, client, ws_id)
"""

import base64
import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger()

# ── Platform schema ──────────────────────────────────────────────────────────

_PLATFORM_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"


# ── Bundle ───────────────────────────────────────────────────────────────────


@dataclass
class ArtifactBundle:
    """A complete Fabric item definition ready for disk or API."""

    item_type: str  # "Notebook", "SemanticModel", "Lakehouse", etc.
    display_name: str  # e.g. "NB_My_Notebook"
    parts: dict[str, str | bytes]  # {relative_path: content}
    description: str = ""
    logical_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def dir_name(self) -> str:
        """Directory name in git-sync format: {DisplayName}.{ItemType}"""
        return f"{self.display_name}.{self.item_type}"

    def platform_json(self) -> str:
        """Generate the .platform file content."""
        return json.dumps(
            {
                "$schema": _PLATFORM_SCHEMA,
                "metadata": {
                    "type": self.item_type,
                    "displayName": self.display_name,
                },
                "config": {
                    "version": "2.0",
                    "logicalId": self.logical_id,
                },
            },
            indent=2,
        )


# ── Disk operations (primary path) ──────────────────────────────────────────


def save_to_disk(bundle: ArtifactBundle, output_dir: str | Path) -> Path:
    """
    Write artifact in Fabric git-sync directory format.

    Creates:
        output_dir/{display_name}.{item_type}/
            .platform
            {part files...}

    Returns the artifact directory path.
    """
    output_dir = Path(output_dir)
    artifact_dir = output_dir / bundle.dir_name
    artifact_dir.mkdir(parents=True, exist_ok=True)

    # Write .platform
    platform_path = artifact_dir / ".platform"
    platform_path.write_text(bundle.platform_json(), encoding="utf-8")
    log.debug("Wrote %s", platform_path)

    # Write definition parts
    for rel_path, content in bundle.parts.items():
        part_path = artifact_dir / rel_path
        part_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(content, bytes):
            part_path.write_bytes(content)
        else:
            part_path.write_text(content, encoding="utf-8")
        log.debug("Wrote %s", part_path)

    log.info("Saved artifact: %s", artifact_dir)
    return artifact_dir


def load_from_disk(artifact_dir: str | Path) -> ArtifactBundle:
    """Read a git-sync artifact directory back into a bundle."""
    artifact_dir = Path(artifact_dir)

    if not (artifact_dir / ".platform").exists():
        raise FileNotFoundError(f"No .platform file in {artifact_dir}")

    platform = json.loads((artifact_dir / ".platform").read_text(encoding="utf-8"))
    metadata = platform.get("metadata", {})
    config = platform.get("config", {})

    parts = {}
    for p in artifact_dir.rglob("*"):
        if p.is_file() and p.name != ".platform":
            rel = str(p.relative_to(artifact_dir)).replace("\\", "/")
            try:
                parts[rel] = p.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                parts[rel] = p.read_bytes()

    return ArtifactBundle(
        item_type=metadata.get("type", ""),
        display_name=metadata.get("displayName", ""),
        parts=parts,
        logical_id=config.get("logicalId", str(uuid.uuid4())),
    )


# ── REST upload (fallback for non-git-connected workspaces) ──────────────────


def _encode_part(path: str, content: str | bytes) -> dict:
    """Encode a definition part for the Fabric REST API."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return {
        "path": path,
        "payload": base64.b64encode(content).decode("ascii"),
        "payloadType": "InlineBase64",
    }


def upload_to_workspace(
    bundle: ArtifactBundle,
    client: Any,  # FabricClient
    ws_id: str,
    *,
    item_id: str | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Push an artifact to a Fabric workspace via REST API.

    Use only for workspaces without git integration.

    Args:
        bundle:   The artifact to upload.
        client:   FabricClient instance.
        ws_id:    Target workspace ID.
        item_id:  If provided, updates an existing item. Otherwise creates new.
        dry_run:  If True, validate but don't upload.

    Returns:
        The created/updated item dict from the API.
    """
    api_parts = [_encode_part(path, content) for path, content in bundle.parts.items()]
    # Add .platform as a part
    api_parts.append(_encode_part(".platform", bundle.platform_json()))

    log.info(
        "%s artifact: %s (%s, %d parts)",
        "Updating" if item_id else "Creating",
        bundle.display_name,
        bundle.item_type,
        len(api_parts),
    )

    if dry_run:
        log.info(
            "[DRY RUN] Would %s %s in workspace %s",
            "update" if item_id else "create",
            bundle.display_name,
            ws_id[:8],
        )
        for p in api_parts:
            log.info(
                "[DRY RUN]   Part: %s (%d bytes)",
                p["path"],
                len(base64.b64decode(p["payload"])),
            )
        return {}

    if item_id:
        from .crud import update_item_definition

        return update_item_definition(client, ws_id, item_id, api_parts)
    else:
        from .crud import create_item

        return create_item(
            client,
            ws_id,
            display_name=bundle.display_name,
            item_type=bundle.item_type,
            description=bundle.description,
            definition_parts=api_parts,
        )


# ── Diff utility ─────────────────────────────────────────────────────────────


def diff_bundles(local: ArtifactBundle, remote: ArtifactBundle) -> dict:
    """Compare two bundles and return a summary of differences.

    Returns dict with keys: added, removed, modified (lists of part paths).
    """
    local_keys = set(local.parts.keys())
    remote_keys = set(remote.parts.keys())

    added = sorted(local_keys - remote_keys)
    removed = sorted(remote_keys - local_keys)
    modified = []

    for key in sorted(local_keys & remote_keys):
        local_content = local.parts[key]
        remote_content = remote.parts[key]
        if isinstance(local_content, str):
            local_content = local_content.encode("utf-8")
        if isinstance(remote_content, str):
            remote_content = remote_content.encode("utf-8")
        if local_content != remote_content:
            modified.append(key)

    return {"added": added, "removed": removed, "modified": modified}
