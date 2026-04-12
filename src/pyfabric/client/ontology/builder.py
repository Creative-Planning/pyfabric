"""OntologyBuilder — high-level stateful builder for ontology definitions."""

import base64
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

import structlog

from pyfabric.client.ontology._id_gen import _generate_bigint_id

log = structlog.get_logger()

# Value types
VALID_VALUE_TYPES = {"String", "Boolean", "DateTime", "Object", "BigInt", "Double"}
VALID_WIDGET_TYPES = {"lineChart", "barChart", "file", "graph", "liveMap"}
VALID_SOURCE_TYPES = {"LakehouseTable", "KustoTable"}


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
