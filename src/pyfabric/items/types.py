"""Fabric item type definitions and .platform file parsing.

Defines the known Fabric git-sync item types, their required and optional
files, and provides parsing/validation for .platform metadata files.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

# ── .platform file model ─────────────────────────────────────────────────────

PLATFORM_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"


@dataclass(frozen=True)
class PlatformMetadata:
    """The ``metadata`` section of a .platform file."""

    type: str
    display_name: str
    description: str = ""


@dataclass(frozen=True)
class PlatformConfig:
    """The ``config`` section of a .platform file."""

    version: str
    logical_id: str


@dataclass(frozen=True)
class PlatformFile:
    """Parsed representation of a Fabric .platform file."""

    metadata: PlatformMetadata
    config: PlatformConfig
    schema: str = PLATFORM_SCHEMA

    @property
    def expected_dir_name(self) -> str:
        """Expected directory name: ``{displayName}.{type}``."""
        return f"{self.metadata.display_name}.{self.metadata.type}"


def parse_platform(content: str) -> PlatformFile:
    """Parse a .platform file from its JSON string content.

    Raises ``ValueError`` with a descriptive message if required fields
    are missing or the JSON is invalid.
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in .platform file: {e}") from None

    metadata_raw = data.get("metadata")
    if metadata_raw is None:
        raise ValueError(".platform file missing required 'metadata' section")

    item_type = metadata_raw.get("type")
    if not item_type:
        raise ValueError(".platform metadata missing required 'type' field")

    display_name = metadata_raw.get("displayName")
    if not display_name:
        raise ValueError(".platform metadata missing required 'displayName' field")

    config_raw = data.get("config")
    if config_raw is None:
        raise ValueError(".platform file missing required 'config' section")

    logical_id = config_raw.get("logicalId")
    if not logical_id:
        raise ValueError(".platform config missing required 'logicalId' field")

    return PlatformFile(
        metadata=PlatformMetadata(
            type=item_type,
            display_name=display_name,
            description=metadata_raw.get("description", ""),
        ),
        config=PlatformConfig(
            version=config_raw.get("version", "2.0"),
            logical_id=logical_id,
        ),
        schema=data.get("$schema", PLATFORM_SCHEMA),
    )


# ── Item type registry ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class ItemType:
    """Definition of a Fabric item type and its expected file structure.

    ``required_files`` lists files that must all be present (AND logic).
    ``alt_required_files`` lists alternative file sets — the item is valid
    if *any one* set is fully present (OR-of-ANDs).  When both fields are
    populated, the item must satisfy ``required_files`` AND at least one
    ``alt_required_files`` set.  When only ``alt_required_files`` is given,
    the item must satisfy at least one set.

    Example — SemanticModel accepts either legacy (``model.bim``) or
    TMDL (``definition.pbism``) format::

        ItemType(
            type_name="SemanticModel",
            alt_required_files=[["model.bim"], ["definition.pbism"]],
        )
    """

    type_name: str
    required_files: list[str] = field(default_factory=list)
    optional_files: list[str] = field(default_factory=list)
    alt_required_files: list[list[str]] = field(default_factory=list)

    @property
    def dir_suffix(self) -> str:
        """Directory suffix in git-sync format: ``.{type_name}``."""
        return f".{self.type_name}"


ITEM_TYPES: dict[str, ItemType] = {
    # ── Notebooks ───────────────────────────────────────────────────────
    # Content file is either .py (Python/PySpark) or .sql (Spark SQL).
    "Notebook": ItemType(
        type_name="Notebook",
        alt_required_files=[["notebook-content.py"], ["notebook-content.sql"]],
        optional_files=["notebook-settings.json", "fs-settings.json"],
    ),
    # ── Lakehouses ──────────────────────────────────────────────────────
    "Lakehouse": ItemType(
        type_name="Lakehouse",
        required_files=["lakehouse.metadata.json"],
        optional_files=["alm.settings.json", "shortcuts.metadata.json"],
    ),
    # ── Dataflows ───────────────────────────────────────────────────────
    "Dataflow": ItemType(
        type_name="Dataflow",
        required_files=["queryMetadata.json", "mashup.pq"],
    ),
    # ── Environments ────────────────────────────────────────────────────
    "Environment": ItemType(
        type_name="Environment",
        required_files=[
            "Setting/Sparkcompute.yml",
        ],
        optional_files=["Libraries/PublicLibraries/environment.yml"],
    ),
    # ── Variable Libraries ──────────────────────────────────────────────
    "VariableLibrary": ItemType(
        type_name="VariableLibrary",
        required_files=["variables.json", "settings.json"],
    ),
    # ── Semantic Models ─────────────────────────────────────────────────
    # Legacy format uses model.bim; newer TMDL format uses definition.pbism.
    "SemanticModel": ItemType(
        type_name="SemanticModel",
        alt_required_files=[["model.bim"], ["definition.pbism"]],
        optional_files=["definition.pbixproj"],
    ),
    # ── Reports ─────────────────────────────────────────────────────────
    # Legacy format uses report.json; PBIR format uses definition.pbir.
    "Report": ItemType(
        type_name="Report",
        alt_required_files=[["report.json"], ["definition.pbir"]],
    ),
    # ── Data Pipelines ──────────────────────────────────────────────────
    # Fabric git-sync uses "DataPipeline" as the type string (not "Pipeline").
    "DataPipeline": ItemType(
        type_name="DataPipeline",
        required_files=["pipeline-content.json"],
    ),
    # ── Warehouses ──────────────────────────────────────────────────────
    "Warehouse": ItemType(
        type_name="Warehouse",
        required_files=[],
    ),
    # ── Mirrored Databases ──────────────────────────────────────────────
    "MirroredDatabase": ItemType(
        type_name="MirroredDatabase",
        required_files=["mirroring.json"],
    ),
    # ── Ontologies ──────────────────────────────────────────────────────
    "Ontology": ItemType(
        type_name="Ontology",
        required_files=["definition.json"],
    ),
    # ── Maps ────────────────────────────────────────────────────────────
    "Map": ItemType(
        type_name="Map",
        required_files=["map.json"],
    ),
}
