"""Ontology CRUD, builder, and definition parts helpers for Microsoft Fabric IQ.

This package is split into focused modules for maintainability:

- ``crud``: CRUD operations (list, get, create, update, delete)
- ``builder``: OntologyBuilder and data structure classes
- ``parts``: Low-level definition parts manipulation
- ``_id_gen``: ID generation utilities

All public symbols are re-exported here for backward compatibility::

    from pyfabric.client.ontology import OntologyBuilder, create_ontology
"""

# CRUD operations
# ID generators
from pyfabric.client.ontology._id_gen import (
    generate_guid,
    generate_id,
)

# Builder and data structures
from pyfabric.client.ontology.builder import (
    VALID_SOURCE_TYPES,
    VALID_VALUE_TYPES,
    VALID_WIDGET_TYPES,
    Contextualization,
    DataBinding,
    EntityType,
    OntologyBuilder,
    Property,
    RelationshipType,
)
from pyfabric.client.ontology.crud import (
    create_ontology,
    delete_ontology,
    get_ontology,
    get_ontology_definition,
    list_ontologies,
    update_ontology_definition,
)

# Parts helpers
from pyfabric.client.ontology.parts import (
    add_all_bindings,
    add_all_contextualizations,
    add_contextualization_to_parts,
    add_data_binding_to_parts,
    add_entity_type_to_parts,
    add_relationship_type_to_parts,
    build_from_config,
    decode_definition,
    encode_definition,
    entity_name_to_table,
    get_data_binding_from_parts,
    get_entity_type_from_parts,
    get_relationship_type_from_parts,
    list_contextualizations_from_parts,
    list_data_bindings_from_parts,
    list_entity_types_from_parts,
    list_relationship_types_from_parts,
    make_contextualization_def,
    make_entity_type_def,
    make_key_ref_binding,
    make_kql_binding,
    make_lakehouse_binding,
    make_property,
    make_property_binding,
    make_relationship_type_def,
    make_warehouse_binding,
    remove_contextualization_from_parts,
    remove_data_binding_from_parts,
    remove_entity_type_from_parts,
    remove_relationship_type_from_parts,
    update_data_binding_in_parts,
    update_entity_type_in_parts,
    update_relationship_type_in_parts,
)

__all__ = [
    "VALID_SOURCE_TYPES",
    "VALID_VALUE_TYPES",
    "VALID_WIDGET_TYPES",
    # Builder
    "Contextualization",
    "DataBinding",
    "EntityType",
    "OntologyBuilder",
    "Property",
    "RelationshipType",
    # Parts
    "add_all_bindings",
    "add_all_contextualizations",
    "add_contextualization_to_parts",
    "add_data_binding_to_parts",
    "add_entity_type_to_parts",
    "add_relationship_type_to_parts",
    "build_from_config",
    # CRUD
    "create_ontology",
    "decode_definition",
    "delete_ontology",
    "encode_definition",
    "entity_name_to_table",
    # ID generators
    "generate_guid",
    "generate_id",
    "get_data_binding_from_parts",
    "get_entity_type_from_parts",
    "get_ontology",
    "get_ontology_definition",
    "get_relationship_type_from_parts",
    "list_contextualizations_from_parts",
    "list_data_bindings_from_parts",
    "list_entity_types_from_parts",
    "list_ontologies",
    "list_relationship_types_from_parts",
    "make_contextualization_def",
    "make_entity_type_def",
    "make_key_ref_binding",
    "make_kql_binding",
    "make_lakehouse_binding",
    "make_property",
    "make_property_binding",
    "make_relationship_type_def",
    "make_warehouse_binding",
    "remove_contextualization_from_parts",
    "remove_data_binding_from_parts",
    "remove_entity_type_from_parts",
    "remove_relationship_type_from_parts",
    "update_data_binding_in_parts",
    "update_entity_type_in_parts",
    "update_ontology_definition",
    "update_relationship_type_in_parts",
]
