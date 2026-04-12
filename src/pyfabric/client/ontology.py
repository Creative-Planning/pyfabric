"""
Ontology CRUD, builder, and definition parts helpers for Microsoft Fabric IQ.

This module combines:
  - OntologyBuilder (high-level stateful builder for new ontologies)
  - CRUD operations (list, get, create, update, delete via FabricClient)
  - Definition parts helpers (low-level functional CRUD on decoded parts lists)
  - Config-driven builder (build ontology from JSON config)

Usage:
    from pyfabric.client.ontology import OntologyBuilder, create_ontology, list_ontologies
    from pyfabric.items.bundle import ArtifactBundle, save_to_disk

    # Build an ontology definition
    builder = OntologyBuilder()
    customer_id = builder.add_entity_type("Customer", properties=[
        ("CustomerId", "String"),
        ("FullName", "String"),
    ])
    builder.add_data_binding(customer_id, ws_id=WS, item_id=LH,
                             table="customers", schema="dbo",
                             column_map={"CustomerId": "CustomerId", ...})

    # Save as git-sync artifact
    bundle = builder.to_bundle("My_Ontology")
    save_to_disk(bundle, "definitions/")

    # Or create via REST API
    create_ontology(client, ws_id, "My_Ontology", definition_parts=builder.to_api_parts())

    # Low-level parts manipulation
    from pyfabric.client.ontology import decode_definition, encode_definition
    parts = decode_definition(raw_api_response)
    parts = add_entity_type_to_parts(parts, et_id, definition)
    encoded = encode_definition(parts)

API reference:
    https://learn.microsoft.com/en-us/rest/api/fabric/ontology/items
"""

import base64
import json
import random
import re
import uuid
from dataclasses import dataclass, field
from typing import Any

import structlog

from .http import FabricClient

log = structlog.get_logger()


# ══════════════════════════════════════════════════════════════════════════════
# ID generation
# ══════════════════════════════════════════════════════════════════════════════


def _generate_bigint_id() -> str:
    """Generate a positive 64-bit integer ID as a string."""
    return str(random.randint(10**12, 10**16 - 1))


def generate_id() -> str:
    """Generate a random positive 64-bit integer ID as a string."""
    return str(random.randint(10**15, 10**18))


def generate_guid() -> str:
    """Generate a UUID for data binding IDs."""
    return str(uuid.uuid4())


# ══════════════════════════════════════════════════════════════════════════════
# Value types
# ══════════════════════════════════════════════════════════════════════════════

VALID_VALUE_TYPES = {"String", "Boolean", "DateTime", "Object", "BigInt", "Double"}
VALID_WIDGET_TYPES = {"lineChart", "barChart", "file", "graph", "liveMap"}
VALID_SOURCE_TYPES = {"LakehouseTable", "KustoTable"}


# ══════════════════════════════════════════════════════════════════════════════
# CRUD operations (via FabricClient)
# ══════════════════════════════════════════════════════════════════════════════


def list_ontologies(client: FabricClient, ws_id: str) -> list[dict]:
    """List all ontologies in a workspace."""
    return client.get_paged(f"workspaces/{ws_id}/ontologies")


def get_ontology(client: FabricClient, ws_id: str, ontology_id: str) -> dict:
    """Get a single ontology by ID."""
    return client.get(f"workspaces/{ws_id}/ontologies/{ontology_id}")


def create_ontology(
    client: FabricClient,
    ws_id: str,
    display_name: str,
    *,
    description: str = "",
    definition_parts: list[dict] | None = None,
) -> dict:
    """Create an ontology in a workspace."""
    log.info("Creating ontology: %s", display_name)
    body: dict = {"displayName": display_name}
    if description:
        body["description"] = description
    if definition_parts:
        body["definition"] = {"parts": definition_parts}
    return client.post(f"workspaces/{ws_id}/ontologies", body)


def get_ontology_definition(
    client: FabricClient,
    ws_id: str,
    ontology_id: str,
) -> dict:
    """Get the ontology definition (entity types, relationships, bindings)."""
    return client.post(
        f"workspaces/{ws_id}/ontologies/{ontology_id}/getDefinition",
    )


def update_ontology_definition(
    client: FabricClient,
    ws_id: str,
    ontology_id: str,
    definition_parts: list[dict],
) -> dict:
    """Replace the ontology definition."""
    log.info("Updating ontology definition: %s", ontology_id)
    return client.post(
        f"workspaces/{ws_id}/ontologies/{ontology_id}/updateDefinition",
        {"definition": {"parts": definition_parts}},
    )


def delete_ontology(client: FabricClient, ws_id: str, ontology_id: str) -> None:
    """Delete an ontology."""
    log.info("Deleting ontology: %s", ontology_id)
    client.delete(f"workspaces/{ws_id}/ontologies/{ontology_id}")


# ══════════════════════════════════════════════════════════════════════════════
# Data structures (for OntologyBuilder)
# ══════════════════════════════════════════════════════════════════════════════


@dataclass
class Property:
    """An entity type property."""

    name: str
    value_type: str = "String"
    id: str = field(default_factory=_generate_bigint_id)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "redefines": None,
            "baseTypeNamespaceType": None,
            "valueType": self.value_type,
        }


@dataclass
class DataBinding:
    """Binds entity properties to columns in a lakehouse table."""

    source_type: str
    workspace_id: str
    item_id: str
    table_name: str
    source_schema: str
    property_bindings: list[dict]
    binding_type: str = "NonTimeSeries"
    timestamp_column: str | None = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict:
        config: dict[str, Any] = {
            "dataBindingType": self.binding_type,
            "propertyBindings": self.property_bindings,
            "sourceTableProperties": {
                "sourceType": self.source_type,
                "workspaceId": self.workspace_id,
                "itemId": self.item_id,
                "sourceTableName": self.table_name,
                "sourceSchema": self.source_schema,
            },
        }
        if self.binding_type == "TimeSeries" and self.timestamp_column:
            config["timestampColumnName"] = self.timestamp_column
        return {"id": self.id, "dataBindingConfiguration": config}


@dataclass
class EntityType:
    """An ontology entity type with properties and optional data bindings."""

    name: str
    properties: list[Property] = field(default_factory=list)
    timeseries_properties: list[Property] = field(default_factory=list)
    data_bindings: list[DataBinding] = field(default_factory=list)
    documents: list[dict] = field(default_factory=list)
    id: str = field(default_factory=_generate_bigint_id)
    display_name_property_id: str | None = None
    entity_id_parts: list[str] | None = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "namespace": "usertypes",
            "baseEntityTypeId": None,
            "name": self.name,
            "entityIdParts": self.entity_id_parts
            or ([self.properties[0].id] if self.properties else []),
            "displayNamePropertyId": self.display_name_property_id
            or (self.properties[0].id if self.properties else None),
            "namespaceType": "Custom",
            "visibility": "Visible",
            "properties": [p.to_dict() for p in self.properties],
            "timeseriesProperties": [p.to_dict() for p in self.timeseries_properties],
        }


@dataclass
class RelationshipType:
    """A relationship between two entity types."""

    name: str
    source_entity_type_id: str
    target_entity_type_id: str
    contextualizations: list[dict] = field(default_factory=list)
    id: str = field(default_factory=_generate_bigint_id)

    def to_dict(self) -> dict:
        return {
            "namespace": "usertypes",
            "id": self.id,
            "name": self.name,
            "namespaceType": "Custom",
            "source": {"entityTypeId": self.source_entity_type_id},
            "target": {"entityTypeId": self.target_entity_type_id},
        }


@dataclass
class Contextualization:
    """Data binding for a relationship — maps join columns to entity keys."""

    workspace_id: str
    item_id: str
    table_name: str
    source_schema: str
    source_key_bindings: list[dict]
    target_key_bindings: list[dict]
    source_type: str = "LakehouseTable"
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "dataBindingTable": {
                "workspaceId": self.workspace_id,
                "itemId": self.item_id,
                "sourceTableName": self.table_name,
                "sourceSchema": self.source_schema,
                "sourceType": self.source_type,
            },
            "sourceKeyRefBindings": self.source_key_bindings,
            "targetKeyRefBindings": self.target_key_bindings,
        }


# ══════════════════════════════════════════════════════════════════════════════
# OntologyBuilder (high-level stateful builder)
# ══════════════════════════════════════════════════════════════════════════════


class OntologyBuilder:
    """Programmatic builder for ontology definitions.

    Produces definition parts compatible with ArtifactBundle (git-sync)
    and the Fabric REST API.
    """

    def __init__(self):
        self._entity_types: dict[str, EntityType] = {}
        self._relationship_types: dict[str, RelationshipType] = {}

    def add_entity_type(
        self,
        name: str,
        *,
        properties: list[tuple[str, str]] | None = None,
        timeseries_properties: list[tuple[str, str]] | None = None,
        entity_id_property: str | None = None,
        display_name_property: str | None = None,
    ) -> str:
        """Add an entity type. Returns the entity type ID."""
        props = [Property(n, vt) for n, vt in (properties or [])]
        ts_props = [Property(n, vt) for n, vt in (timeseries_properties or [])]

        for p in props + ts_props:
            if p.value_type not in VALID_VALUE_TYPES:
                raise ValueError(
                    f"Invalid value type '{p.value_type}' for property '{p.name}'. "
                    f"Valid: {VALID_VALUE_TYPES}"
                )

        entity = EntityType(name=name, properties=props, timeseries_properties=ts_props)

        if entity_id_property:
            match = next((p for p in props if p.name == entity_id_property), None)
            if not match:
                raise ValueError(
                    f"entity_id_property '{entity_id_property}' not in properties"
                )
            entity.entity_id_parts = [match.id]

        if display_name_property:
            match = next((p for p in props if p.name == display_name_property), None)
            if not match:
                raise ValueError(
                    f"display_name_property '{display_name_property}' not in properties"
                )
            entity.display_name_property_id = match.id

        self._entity_types[entity.id] = entity
        log.debug(
            "Added entity type: %s (id=%s, %d props)", name, entity.id, len(props)
        )
        return entity.id

    def add_data_binding(
        self,
        entity_type_id: str,
        *,
        workspace_id: str,
        item_id: str,
        table_name: str,
        source_schema: str = "dbo",
        column_map: dict[str, str] | None = None,
        binding_type: str = "NonTimeSeries",
        timestamp_column: str | None = None,
    ) -> str:
        """Bind entity properties to a lakehouse table. Returns binding ID."""
        entity = self._entity_types.get(entity_type_id)
        if not entity:
            raise ValueError(f"Entity type '{entity_type_id}' not found")

        if binding_type == "TimeSeries" and not timestamp_column:
            raise ValueError("timestamp_column required for TimeSeries bindings")

        all_props = entity.properties + entity.timeseries_properties
        prop_by_name = {p.name: p for p in all_props}

        if column_map is None:
            column_map = {p.name: p.name for p in all_props}

        property_bindings = []
        for src_col, prop_name in column_map.items():
            prop = prop_by_name.get(prop_name)
            if not prop:
                raise ValueError(
                    f"Property '{prop_name}' not found on entity type '{entity.name}'. "
                    f"Available: {list(prop_by_name.keys())}"
                )
            property_bindings.append(
                {
                    "sourceColumnName": src_col,
                    "targetPropertyId": prop.id,
                }
            )

        binding = DataBinding(
            source_type="LakehouseTable",
            workspace_id=workspace_id,
            item_id=item_id,
            table_name=table_name,
            source_schema=source_schema,
            property_bindings=property_bindings,
            binding_type=binding_type,
            timestamp_column=timestamp_column,
        )
        entity.data_bindings.append(binding)
        log.debug(
            "Added data binding: %s -> %s.%s (%d cols)",
            entity.name,
            source_schema,
            table_name,
            len(property_bindings),
        )
        return binding.id

    def add_relationship(
        self,
        name: str,
        source_entity_type_id: str,
        target_entity_type_id: str,
    ) -> str:
        """Add a relationship between two entity types. Returns relationship ID."""
        if source_entity_type_id not in self._entity_types:
            raise ValueError(f"Source entity type '{source_entity_type_id}' not found")
        if target_entity_type_id not in self._entity_types:
            raise ValueError(f"Target entity type '{target_entity_type_id}' not found")

        rel = RelationshipType(
            name=name,
            source_entity_type_id=source_entity_type_id,
            target_entity_type_id=target_entity_type_id,
        )
        self._relationship_types[rel.id] = rel
        src_name = self._entity_types[source_entity_type_id].name
        tgt_name = self._entity_types[target_entity_type_id].name
        log.debug("Added relationship: %s -> %s (%s)", src_name, tgt_name, name)
        return rel.id

    def add_contextualization(
        self,
        relationship_type_id: str,
        *,
        workspace_id: str,
        item_id: str,
        table_name: str,
        source_schema: str = "dbo",
        source_key_map: dict[str, str],
        target_key_map: dict[str, str],
    ) -> str:
        """Add a data binding for a relationship (join table). Returns contextualization ID."""
        rel = self._relationship_types.get(relationship_type_id)
        if not rel:
            raise ValueError(f"Relationship type '{relationship_type_id}' not found")

        src_entity = self._entity_types[rel.source_entity_type_id]
        tgt_entity = self._entity_types[rel.target_entity_type_id]

        def _resolve_bindings(col_map, entity):
            prop_by_name = {p.name: p for p in entity.properties}
            bindings = []
            for col, prop_name in col_map.items():
                prop = prop_by_name.get(prop_name)
                if not prop:
                    raise ValueError(
                        f"Property '{prop_name}' not found on '{entity.name}'"
                    )
                bindings.append({"sourceColumnName": col, "targetPropertyId": prop.id})
            return bindings

        ctx = Contextualization(
            workspace_id=workspace_id,
            item_id=item_id,
            table_name=table_name,
            source_schema=source_schema,
            source_key_bindings=_resolve_bindings(source_key_map, src_entity),
            target_key_bindings=_resolve_bindings(target_key_map, tgt_entity),
        )
        rel.contextualizations.append(ctx)
        return ctx.id

    # ── Validation ───────────────────────────────────────────────────────

    def validate(self) -> list[str]:
        """Validate the ontology definition. Returns a list of error messages."""
        errors = []

        if not self._entity_types:
            errors.append("Ontology has no entity types")

        for _eid, entity in self._entity_types.items():
            if not entity.properties and not entity.timeseries_properties:
                errors.append(f"Entity type '{entity.name}' has no properties")

            for prop in entity.properties + entity.timeseries_properties:
                if prop.value_type not in VALID_VALUE_TYPES:
                    errors.append(
                        f"Entity '{entity.name}' property '{prop.name}' "
                        f"has invalid type '{prop.value_type}'"
                    )

            for binding in entity.data_bindings:
                if not binding.property_bindings:
                    errors.append(
                        f"Data binding on '{entity.name}' has no property bindings"
                    )
                if (
                    binding.binding_type == "TimeSeries"
                    and not binding.timestamp_column
                ):
                    errors.append(
                        f"TimeSeries binding on '{entity.name}' missing timestamp_column"
                    )

        for _rid, rel in self._relationship_types.items():
            if rel.source_entity_type_id not in self._entity_types:
                errors.append(f"Relationship '{rel.name}' source entity not found")
            if rel.target_entity_type_id not in self._entity_types:
                errors.append(f"Relationship '{rel.name}' target entity not found")

        return errors

    # ── Output ───────────────────────────────────────────────────────────

    def to_parts(self) -> dict[str, str]:
        """Build definition parts as {path: json_content} dict."""
        parts: dict[str, str] = {}
        parts["definition.json"] = json.dumps({})

        for entity in self._entity_types.values():
            b = f"EntityTypes/{entity.id}"
            parts[f"{b}/definition.json"] = json.dumps(entity.to_dict(), indent=2)
            for binding in entity.data_bindings:
                parts[f"{b}/DataBindings/{binding.id}.json"] = json.dumps(
                    binding.to_dict(), indent=2
                )
            for i, doc in enumerate(entity.documents):
                parts[f"{b}/Documents/document{i + 1}.json"] = json.dumps(doc, indent=2)

        for rel in self._relationship_types.values():
            b = f"RelationshipTypes/{rel.id}"
            parts[f"{b}/definition.json"] = json.dumps(rel.to_dict(), indent=2)
            for ctx in rel.contextualizations:
                parts[f"{b}/Contextualizations/{ctx.id}.json"] = json.dumps(
                    ctx.to_dict(), indent=2
                )

        return parts

    def to_api_parts(self) -> list[dict]:
        """Build definition parts as a list of API part dicts (base64-encoded)."""
        parts_dict = self.to_parts()
        api_parts = []
        for path, content in parts_dict.items():
            if isinstance(content, str):
                content = content.encode("utf-8")
            api_parts.append(
                {
                    "path": path,
                    "payload": base64.b64encode(content).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
            )
        return api_parts

    def to_bundle(self, display_name: str, *, description: str = ""):
        """Build an ArtifactBundle for git-sync format."""
        from pyfabric.items.bundle import ArtifactBundle

        return ArtifactBundle(
            item_type="Ontology",
            display_name=display_name,
            description=description,
            parts=self.to_parts(),
        )

    def summary(self) -> str:
        """Human-readable summary of the ontology definition."""
        lines = []
        lines.append(f"Entity types: {len(self._entity_types)}")
        for entity in self._entity_types.values():
            props = len(entity.properties) + len(entity.timeseries_properties)
            bindings = len(entity.data_bindings)
            lines.append(
                f"  {entity.name}: {props} properties, {bindings} data bindings"
            )
        lines.append(f"Relationships: {len(self._relationship_types)}")
        for rel in self._relationship_types.values():
            src = self._entity_types.get(rel.source_entity_type_id, None)
            tgt = self._entity_types.get(rel.target_entity_type_id, None)
            src_name = src.name if src else "?"
            tgt_name = tgt.name if tgt else "?"
            lines.append(f"  {rel.name}: {src_name} -> {tgt_name}")
        return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# Definition parts helpers (low-level functional operations on decoded parts)
# ══════════════════════════════════════════════════════════════════════════════


def decode_definition(raw: dict) -> list[dict]:
    """Decode an API definition response into a list of {path, content} dicts."""
    parts = raw.get("definition", {}).get("parts", [])
    decoded = []
    for part in parts:
        payload_b64 = part.get("payload", "")
        try:
            payload_str = base64.b64decode(payload_b64).decode("utf-8")
            content = json.loads(payload_str) if payload_str.strip() else {}
        except (json.JSONDecodeError, Exception):
            content = base64.b64decode(payload_b64).decode("utf-8", errors="replace")
        decoded.append({"path": part["path"], "content": content})
    return decoded


def encode_definition(parts: list[dict]) -> dict:
    """Encode a list of {path, content} dicts back into the API format."""
    encoded_parts = []
    for part in parts:
        content = part["content"]
        payload_str = json.dumps(content) if isinstance(content, dict) else str(content)
        payload_b64 = base64.b64encode(payload_str.encode("utf-8")).decode("ascii")
        encoded_parts.append(
            {
                "path": part["path"],
                "payload": payload_b64,
                "payloadType": "InlineBase64",
            }
        )
    return {"parts": encoded_parts}


def make_property(name: str, value_type: str, prop_id: str | None = None) -> dict:
    """Build a property dict for use in make_entity_type_def."""
    return {
        "id": prop_id or generate_id(),
        "name": name,
        "valueType": value_type,
    }


def make_property_binding(source_column: str, target_property_id: str) -> dict:
    """Build a property binding mapping a source column to an entity property."""
    return {
        "sourceColumnName": source_column,
        "targetPropertyId": target_property_id,
    }


def make_key_ref_binding(source_column: str, target_property_id: str) -> dict:
    """Build a key reference binding for a contextualization."""
    return {
        "sourceColumnName": source_column,
        "targetPropertyId": target_property_id,
    }


def make_entity_type_def(
    name: str,
    properties: list[dict] | None = None,
    entity_id: str | None = None,
    entity_id_parts: list[str] | None = None,
    display_name_property_id: str | None = None,
    timeseries_properties: list[dict] | None = None,
) -> tuple[str, dict]:
    """Build an entity type definition dict.

    Properties should be dicts with keys: name, valueType (and optionally id).
    Returns (entity_type_id, definition_dict).
    """
    et_id = entity_id or generate_id()
    props = []
    for p in properties or []:
        props.append(
            {
                "id": p.get("id", generate_id()),
                "name": p["name"],
                "redefines": None,
                "baseTypeNamespaceType": None,
                "valueType": p["valueType"],
            }
        )

    definition = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/ontology/entityType/1.0.0/schema.json",
        "id": et_id,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": name,
        "entityIdParts": entity_id_parts or [],
        "displayNamePropertyId": display_name_property_id,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": props,
        "timeseriesProperties": timeseries_properties or [],
    }
    return et_id, definition


def add_entity_type_to_parts(
    parts: list[dict], et_id: str, definition: dict
) -> list[dict]:
    """Add an entity type to the parts list."""
    path = f"EntityTypes/{et_id}/definition.json"
    return [*parts, {"path": path, "content": definition}]


def get_entity_type_from_parts(parts: list[dict], et_id: str) -> dict | None:
    """Get an entity type definition from the parts list."""
    path = f"EntityTypes/{et_id}/definition.json"
    for part in parts:
        if part["path"] == path:
            return part["content"]
    return None


def update_entity_type_in_parts(
    parts: list[dict], et_id: str, definition: dict
) -> list[dict]:
    """Replace an entity type definition in the parts list."""
    path = f"EntityTypes/{et_id}/definition.json"
    return [
        {"path": p["path"], "content": definition} if p["path"] == path else p
        for p in parts
    ]


def remove_entity_type_from_parts(parts: list[dict], et_id: str) -> list[dict]:
    """Remove an entity type and all its sub-parts."""
    prefix = f"EntityTypes/{et_id}/"
    return [p for p in parts if not p["path"].startswith(prefix)]


def list_entity_types_from_parts(parts: list[dict]) -> list[dict]:
    """List all entity type definitions from the parts."""
    return [
        p["content"]
        for p in parts
        if "EntityTypes/" in p["path"]
        and p["path"].endswith("/definition.json")
        and "Overviews" not in p["path"]
    ]


def make_relationship_type_def(
    name: str,
    source_entity_type_id: str,
    target_entity_type_id: str,
    relationship_id: str | None = None,
) -> tuple[str, dict]:
    """Build a relationship type definition. Returns (id, definition_dict)."""
    rt_id = relationship_id or generate_id()
    definition = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/ontology/relationshipType/1.0.0/schema.json",
        "namespace": "usertypes",
        "id": rt_id,
        "name": name,
        "namespaceType": "Custom",
        "source": {"entityTypeId": source_entity_type_id},
        "target": {"entityTypeId": target_entity_type_id},
    }
    return rt_id, definition


def add_relationship_type_to_parts(
    parts: list[dict], rt_id: str, definition: dict
) -> list[dict]:
    """Add a relationship type to the parts list."""
    path = f"RelationshipTypes/{rt_id}/definition.json"
    return [*parts, {"path": path, "content": definition}]


def get_relationship_type_from_parts(parts: list[dict], rt_id: str) -> dict | None:
    """Get a relationship type definition from the parts list."""
    path = f"RelationshipTypes/{rt_id}/definition.json"
    for part in parts:
        if part["path"] == path:
            return part["content"]
    return None


def update_relationship_type_in_parts(
    parts: list[dict], rt_id: str, definition: dict
) -> list[dict]:
    """Replace a relationship type definition in the parts list."""
    path = f"RelationshipTypes/{rt_id}/definition.json"
    return [
        {"path": p["path"], "content": definition} if p["path"] == path else p
        for p in parts
    ]


def remove_relationship_type_from_parts(parts: list[dict], rt_id: str) -> list[dict]:
    """Remove a relationship type and its contextualizations."""
    prefix = f"RelationshipTypes/{rt_id}/"
    return [p for p in parts if not p["path"].startswith(prefix)]


def list_relationship_types_from_parts(parts: list[dict]) -> list[dict]:
    """List all relationship type definitions from the parts."""
    return [
        p["content"]
        for p in parts
        if "RelationshipTypes/" in p["path"] and p["path"].endswith("/definition.json")
    ]


def make_lakehouse_binding(
    entity_type_id: str,
    property_bindings: list[dict],
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    *,
    binding_type: str = "NonTimeSeries",
    timestamp_column: str | None = None,
    source_schema: str = "dbo",
    binding_id: str | None = None,
) -> tuple[str, dict]:
    """Build a Lakehouse data binding definition. Returns (binding_id, definition_dict)."""
    bid = binding_id or generate_guid()
    config = {
        "dataBindingType": binding_type,
        "propertyBindings": property_bindings,
        "sourceTableProperties": {
            "sourceType": "LakehouseTable",
            "workspaceId": workspace_id,
            "itemId": lakehouse_id,
            "sourceTableName": table_name,
            "sourceSchema": source_schema,
        },
    }
    if binding_type == "TimeSeries":
        config["timestampColumnName"] = timestamp_column
    return bid, {"id": bid, "dataBindingConfiguration": config}


def make_warehouse_binding(
    entity_type_id: str,
    property_bindings: list[dict],
    workspace_id: str,
    warehouse_id: str,
    table_name: str,
    *,
    source_schema: str = "dbo",
    binding_id: str | None = None,
) -> tuple[str, dict]:
    """Build a Warehouse data binding definition. Returns (binding_id, definition_dict)."""
    bid = binding_id or generate_guid()
    config = {
        "dataBindingType": "NonTimeSeries",
        "propertyBindings": property_bindings,
        "sourceTableProperties": {
            "sourceType": "LakehouseTable",
            "workspaceId": workspace_id,
            "itemId": warehouse_id,
            "sourceTableName": table_name,
            "sourceSchema": source_schema,
        },
    }
    return bid, {"id": bid, "dataBindingConfiguration": config}


def make_kql_binding(
    entity_type_id: str,
    property_bindings: list[dict],
    workspace_id: str,
    eventhouse_id: str,
    cluster_uri: str,
    database_name: str,
    table_name: str,
    *,
    timestamp_column: str | None = None,
    binding_id: str | None = None,
) -> tuple[str, dict]:
    """Build a KQL (Eventhouse) data binding definition. Returns (binding_id, definition_dict)."""
    bid = binding_id or generate_guid()
    config = {
        "dataBindingType": "TimeSeries",
        "timestampColumnName": timestamp_column,
        "propertyBindings": property_bindings,
        "sourceTableProperties": {
            "sourceType": "KustoTable",
            "workspaceId": workspace_id,
            "itemId": eventhouse_id,
            "clusterUri": cluster_uri,
            "databaseName": database_name,
            "sourceTableName": table_name,
        },
    }
    return bid, {"id": bid, "dataBindingConfiguration": config}


def add_data_binding_to_parts(
    parts: list[dict],
    entity_type_id: str,
    binding_id: str,
    definition: dict,
) -> list[dict]:
    """Add a data binding to an entity type in the parts list."""
    path = f"EntityTypes/{entity_type_id}/DataBindings/{binding_id}.json"
    return [*parts, {"path": path, "content": definition}]


def get_data_binding_from_parts(
    parts: list[dict],
    entity_type_id: str,
    binding_id: str,
) -> dict | None:
    """Get a specific data binding from the parts."""
    path = f"EntityTypes/{entity_type_id}/DataBindings/{binding_id}.json"
    for part in parts:
        if part["path"] == path:
            return part["content"]
    return None


def update_data_binding_in_parts(
    parts: list[dict],
    entity_type_id: str,
    binding_id: str,
    definition: dict,
) -> list[dict]:
    """Replace a data binding definition in the parts list."""
    path = f"EntityTypes/{entity_type_id}/DataBindings/{binding_id}.json"
    return [
        {"path": p["path"], "content": definition} if p["path"] == path else p
        for p in parts
    ]


def remove_data_binding_from_parts(
    parts: list[dict],
    entity_type_id: str,
    binding_id: str,
) -> list[dict]:
    """Remove a specific data binding from the parts list."""
    path = f"EntityTypes/{entity_type_id}/DataBindings/{binding_id}.json"
    return [p for p in parts if p["path"] != path]


def list_data_bindings_from_parts(
    parts: list[dict],
    entity_type_id: str | None = None,
) -> list[dict]:
    """List data bindings. If entity_type_id given, only for that entity."""
    results = []
    for p in parts:
        if "/DataBindings/" not in p["path"]:
            continue
        if entity_type_id and f"EntityTypes/{entity_type_id}/" not in p["path"]:
            continue
        results.append({"path": p["path"], "content": p["content"]})
    return results


def make_contextualization_def(
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    source_key_bindings: list[dict],
    target_key_bindings: list[dict],
    source_schema: str = "dbo",
    ctx_id: str | None = None,
) -> tuple[str, dict]:
    """Build a contextualization (relationship data binding). Returns (id, definition_dict)."""
    cid = ctx_id or generate_guid()
    definition = {
        "id": cid,
        "dataBindingTable": {
            "workspaceId": workspace_id,
            "itemId": lakehouse_id,
            "sourceTableName": table_name,
            "sourceSchema": source_schema,
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": source_key_bindings,
        "targetKeyRefBindings": target_key_bindings,
    }
    return cid, definition


def add_contextualization_to_parts(
    parts: list[dict],
    rt_id: str,
    ctx_id: str,
    definition: dict,
) -> list[dict]:
    """Add a contextualization to a relationship type in the parts list."""
    path = f"RelationshipTypes/{rt_id}/Contextualizations/{ctx_id}.json"
    return [*parts, {"path": path, "content": definition}]


def list_contextualizations_from_parts(
    parts: list[dict],
    rt_id: str | None = None,
) -> list[dict]:
    """List contextualizations. If rt_id given, only for that relationship."""
    results = []
    for p in parts:
        if "/Contextualizations/" not in p["path"]:
            continue
        if rt_id and f"RelationshipTypes/{rt_id}/" not in p["path"]:
            continue
        results.append({"path": p["path"], "content": p["content"]})
    return results


def remove_contextualization_from_parts(
    parts: list[dict],
    rt_id: str,
    ctx_id: str,
) -> list[dict]:
    """Remove a specific contextualization from the parts list."""
    path = f"RelationshipTypes/{rt_id}/Contextualizations/{ctx_id}.json"
    return [p for p in parts if p["path"] != path]


# ══════════════════════════════════════════════════════════════════════════════
# Config-driven builders
# ══════════════════════════════════════════════════════════════════════════════


def entity_name_to_table(name: str) -> str:
    """Convert an entity type name to a Lakehouse table name (snake_case)."""
    s = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)
    s = re.sub(r"(?<=[A-Z])([A-Z][a-z])", r"_\1", s)
    return s.lower().replace(" ", "_")


def build_from_config(config: dict, table_namer=None):
    """
    Build an ontology definition from a config dict.

    Config format:
        entities:      [{name, keyProperty, displayProperty?, properties: [{name, valueType}]}]
        relationships: [{name, source, target, contextEntity?}]
        tablePrefix:   string prefix for Lakehouse table names

    Returns (parts, entity_map, relationship_map).
    """
    prefix = config.get("tablePrefix", "ont")
    if table_namer is None:

        def table_namer(name):
            return f"{prefix}_{entity_name_to_table(name)}"

    parts = [{"path": "definition.json", "content": {}}]
    entity_map = {}

    for entity_cfg in config["entities"]:
        name = entity_cfg["name"]
        key_prop_name = entity_cfg["keyProperty"]
        display_prop_name = entity_cfg.get("displayProperty", key_prop_name)

        properties = [
            make_property(p["name"], p["valueType"]) for p in entity_cfg["properties"]
        ]

        key_prop_id = None
        display_prop_id = None
        for p in properties:
            if p["name"] == key_prop_name:
                key_prop_id = p["id"]
            if p["name"] == display_prop_name:
                display_prop_id = p["id"]

        if not key_prop_id:
            raise ValueError(
                f"Entity '{name}': keyProperty '{key_prop_name}' not in properties"
            )

        et_id, et_def = make_entity_type_def(
            name,
            properties=properties,
            entity_id_parts=[key_prop_id],
            display_name_property_id=display_prop_id,
        )
        parts = add_entity_type_to_parts(parts, et_id, et_def)

        entity_map[name] = {
            "id": et_id,
            "key_prop_id": key_prop_id,
            "key_prop_name": key_prop_name,
            "prop_ids": {p["name"]: p["id"] for p in et_def["properties"]},
            "table": table_namer(name),
        }

    relationship_map = {}
    for rel_cfg in config["relationships"]:
        rel_name = rel_cfg["name"]
        source_name = rel_cfg["source"]
        target_name = rel_cfg["target"]
        context_entity = rel_cfg.get("contextEntity", target_name)

        rt_id, rt_def = make_relationship_type_def(
            rel_name,
            entity_map[source_name]["id"],
            entity_map[target_name]["id"],
        )
        parts = add_relationship_type_to_parts(parts, rt_id, rt_def)

        relationship_map[rel_name] = {
            "id": rt_id,
            "source": source_name,
            "target": target_name,
            "contextEntity": context_entity,
        }

    return parts, entity_map, relationship_map


def add_all_bindings(
    parts: list,
    entity_map: dict,
    entities_config: list,
    workspace_id: str,
    lakehouse_id: str,
) -> list:
    """Add data bindings for all entities in one pass."""
    for entity_cfg in entities_config:
        name = entity_cfg["name"]
        info = entity_map[name]

        prop_bindings = [
            make_property_binding(p["name"], info["prop_ids"][p["name"]])
            for p in entity_cfg["properties"]
        ]

        bid, binding_def = make_lakehouse_binding(
            info["id"],
            prop_bindings,
            workspace_id,
            lakehouse_id,
            info["table"],
        )
        parts = add_data_binding_to_parts(parts, info["id"], bid, binding_def)

    return parts


def add_all_contextualizations(
    parts: list,
    relationship_map: dict,
    entity_map: dict,
    workspace_id: str,
    lakehouse_id: str,
) -> list:
    """Add contextualizations for all relationships in one pass."""
    for _rel_name, rel_info in relationship_map.items():
        source_info = entity_map[rel_info["source"]]
        target_info = entity_map[rel_info["target"]]
        context_info = entity_map[rel_info["contextEntity"]]

        source_bindings = [
            make_key_ref_binding(
                source_info["key_prop_name"], source_info["key_prop_id"]
            )
        ]
        target_bindings = [
            make_key_ref_binding(
                target_info["key_prop_name"], target_info["key_prop_id"]
            )
        ]

        ctx_id, ctx_def = make_contextualization_def(
            workspace_id,
            lakehouse_id,
            context_info["table"],
            source_bindings,
            target_bindings,
        )
        parts = add_contextualization_to_parts(parts, rel_info["id"], ctx_id, ctx_def)

    return parts
