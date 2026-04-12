"""Tests for ontology module."""

import base64
import json

import pytest

from pyfabric.client.ontology import (
    EntityType,
    OntologyBuilder,
    Property,
    _generate_bigint_id,
    add_entity_type_to_parts,
    build_from_config,
    decode_definition,
    encode_definition,
    entity_name_to_table,
    get_entity_type_from_parts,
    list_entity_types_from_parts,
    make_entity_type_def,
    make_property,
    remove_entity_type_from_parts,
)


class TestIdGeneration:
    def test_generates_string(self):
        id_ = _generate_bigint_id()
        assert isinstance(id_, str)

    def test_is_positive_integer(self):
        id_ = _generate_bigint_id()
        assert int(id_) > 0

    def test_unique(self):
        ids = {_generate_bigint_id() for _ in range(100)}
        assert len(ids) == 100


class TestProperty:
    def test_to_dict(self):
        p = Property(name="CustomerId", value_type="String", id="12345")
        d = p.to_dict()
        assert d["name"] == "CustomerId"
        assert d["valueType"] == "String"
        assert d["id"] == "12345"
        assert d["redefines"] is None


class TestEntityType:
    def test_to_dict_basic(self):
        props = [Property("Name", "String", id="100")]
        entity = EntityType(name="Customer", properties=props, id="999")
        d = entity.to_dict()
        assert d["id"] == "999"
        assert d["name"] == "Customer"
        assert d["namespace"] == "usertypes"
        assert d["namespaceType"] == "Custom"
        assert len(d["properties"]) == 1
        assert d["entityIdParts"] == ["100"]
        assert d["displayNamePropertyId"] == "100"

    def test_to_dict_with_timeseries(self):
        props = [Property("Id", "String", id="1")]
        ts = [Property("Temperature", "Double", id="2")]
        entity = EntityType(
            name="Sensor", properties=props, timeseries_properties=ts, id="888"
        )
        d = entity.to_dict()
        assert len(d["timeseriesProperties"]) == 1
        assert d["timeseriesProperties"][0]["valueType"] == "Double"


class TestOntologyBuilder:
    def _simple_builder(self):
        b = OntologyBuilder()
        customer_id = b.add_entity_type(
            "Customer",
            properties=[
                ("CustomerId", "String"),
                ("FirstName", "String"),
                ("LastName", "String"),
            ],
        )
        order_id = b.add_entity_type(
            "Order",
            properties=[
                ("OrderId", "BigInt"),
                ("CustomerId", "String"),
                ("Amount", "Double"),
            ],
        )
        b.add_relationship("customer_orders", customer_id, order_id)
        return b, customer_id, order_id

    def test_add_entity_type(self):
        b = OntologyBuilder()
        eid = b.add_entity_type("Customer", properties=[("CustomerId", "String")])
        assert eid
        assert int(eid) > 0

    def test_add_entity_type_invalid_value_type(self):
        b = OntologyBuilder()
        with pytest.raises(ValueError, match="Invalid value type"):
            b.add_entity_type("Bad", properties=[("x", "InvalidType")])

    def test_add_data_binding(self):
        b = OntologyBuilder()
        eid = b.add_entity_type(
            "Customer",
            properties=[
                ("CustomerId", "String"),
                ("Name", "String"),
            ],
        )
        bid = b.add_data_binding(
            eid,
            workspace_id="ws-1",
            item_id="lh-1",
            table_name="customers",
            source_schema="dbo",
            column_map={"CustomerId": "CustomerId", "FullName": "Name"},
        )
        assert bid

    def test_add_data_binding_auto_map(self):
        b = OntologyBuilder()
        eid = b.add_entity_type("Simple", properties=[("Id", "String")])
        b.add_data_binding(
            eid, workspace_id="ws", item_id="lh", table_name="t", source_schema="dbo"
        )
        entity = b._entity_types[eid]
        assert len(entity.data_bindings) == 1
        assert entity.data_bindings[0].property_bindings[0]["sourceColumnName"] == "Id"

    def test_add_data_binding_invalid_property(self):
        b = OntologyBuilder()
        eid = b.add_entity_type("X", properties=[("Id", "String")])
        with pytest.raises(ValueError, match="not found on entity"):
            b.add_data_binding(
                eid,
                workspace_id="ws",
                item_id="lh",
                table_name="t",
                source_schema="dbo",
                column_map={"col": "NonExistent"},
            )

    def test_add_data_binding_timeseries_requires_timestamp(self):
        b = OntologyBuilder()
        eid = b.add_entity_type("X", properties=[("Id", "String")])
        with pytest.raises(ValueError, match="timestamp_column required"):
            b.add_data_binding(
                eid,
                workspace_id="ws",
                item_id="lh",
                table_name="t",
                source_schema="dbo",
                binding_type="TimeSeries",
            )

    def test_add_relationship(self):
        b, _customer_id, _order_id = self._simple_builder()
        assert len(b._relationship_types) == 1

    def test_add_relationship_invalid_entity(self):
        b = OntologyBuilder()
        eid = b.add_entity_type("A", properties=[("Id", "String")])
        with pytest.raises(ValueError, match="not found"):
            b.add_relationship("bad_rel", eid, "nonexistent-id")

    def test_add_contextualization(self):
        b, _customer_id, _order_id = self._simple_builder()
        rel_id = next(iter(b._relationship_types.keys()))
        ctx_id = b.add_contextualization(
            rel_id,
            workspace_id="ws",
            item_id="lh",
            table_name="customer_orders",
            source_schema="dbo",
            source_key_map={"customer_id_col": "CustomerId"},
            target_key_map={"order_id_col": "OrderId"},
        )
        assert ctx_id

    def test_validate_empty(self):
        b = OntologyBuilder()
        errors = b.validate()
        assert any("no entity types" in e for e in errors)

    def test_validate_no_properties(self):
        b = OntologyBuilder()
        b.add_entity_type("Empty", properties=[])
        errors = b.validate()
        assert any("no properties" in e for e in errors)

    def test_validate_valid(self):
        b, _, _ = self._simple_builder()
        errors = b.validate()
        assert errors == []


class TestBuilderOutput:
    def _full_builder(self):
        b = OntologyBuilder()
        customer_id = b.add_entity_type(
            "Customer",
            properties=[
                ("CustomerId", "String"),
                ("Name", "String"),
            ],
        )
        b.add_data_binding(
            customer_id,
            workspace_id="ws-1",
            item_id="lh-1",
            table_name="customers",
            source_schema="dbo",
        )
        order_id = b.add_entity_type(
            "Order",
            properties=[
                ("OrderId", "BigInt"),
                ("Amount", "Double"),
            ],
        )
        b.add_relationship("customer_orders", customer_id, order_id)
        return b

    def test_to_parts_has_definition_json(self):
        b = self._full_builder()
        parts = b.to_parts()
        assert "definition.json" in parts
        assert json.loads(parts["definition.json"]) == {}

    def test_to_parts_has_entity_types(self):
        b = self._full_builder()
        parts = b.to_parts()
        entity_defs = [
            k
            for k in parts
            if k.startswith("EntityTypes/") and k.endswith("/definition.json")
        ]
        assert len(entity_defs) == 2

    def test_to_parts_has_data_bindings(self):
        b = self._full_builder()
        parts = b.to_parts()
        bindings = [k for k in parts if "/DataBindings/" in k]
        assert len(bindings) == 1

    def test_to_parts_has_relationships(self):
        b = self._full_builder()
        parts = b.to_parts()
        rels = [k for k in parts if k.startswith("RelationshipTypes/")]
        assert len(rels) == 1

    def test_to_api_parts_base64_encoded(self):
        b = self._full_builder()
        api_parts = b.to_api_parts()
        assert all(p["payloadType"] == "InlineBase64" for p in api_parts)
        for p in api_parts:
            decoded = base64.b64decode(p["payload"]).decode("utf-8")
            json.loads(decoded)

    def test_to_bundle(self):
        b = self._full_builder()
        bundle = b.to_bundle("Test_Ontology", description="Test desc")
        assert bundle.item_type == "Ontology"
        assert bundle.display_name == "Test_Ontology"
        assert bundle.description == "Test desc"
        assert "definition.json" in bundle.parts

    def test_to_bundle_round_trip_disk(self, tmp_dir):
        from pyfabric.items.bundle import load_from_disk, save_to_disk

        b = self._full_builder()
        bundle = b.to_bundle("Ontology_RT")
        artifact_dir = save_to_disk(bundle, tmp_dir)

        assert (artifact_dir / ".platform").exists()
        assert (artifact_dir / "definition.json").exists()

        loaded = load_from_disk(artifact_dir)
        assert loaded.item_type == "Ontology"
        assert loaded.display_name == "Ontology_RT"
        assert "definition.json" in loaded.parts

        entity_defs = [
            k
            for k in loaded.parts
            if "EntityTypes" in k and k.endswith("definition.json")
        ]
        assert len(entity_defs) == 2

    def test_summary(self):
        b = self._full_builder()
        s = b.summary()
        assert "Entity types: 2" in s
        assert "Customer" in s
        assert "Order" in s
        assert "Relationships: 1" in s
        assert "customer_orders" in s


class TestCrud:
    def test_list_ontologies(self, mock_fabric_client):
        from pyfabric.client.ontology import list_ontologies

        mock_fabric_client.get_paged.return_value = [{"id": "1", "displayName": "O1"}]
        result = list_ontologies(mock_fabric_client, "ws-id")
        assert len(result) == 1
        mock_fabric_client.get_paged.assert_called_once_with(
            "workspaces/ws-id/ontologies"
        )

    def test_get_ontology(self, mock_fabric_client):
        from pyfabric.client.ontology import get_ontology

        mock_fabric_client.get.return_value = {"id": "ont-1"}
        result = get_ontology(mock_fabric_client, "ws-id", "ont-1")
        assert result["id"] == "ont-1"

    def test_create_ontology_empty(self, mock_fabric_client):
        from pyfabric.client.ontology import create_ontology

        mock_fabric_client.post.return_value = {"id": "new-ont"}
        result = create_ontology(mock_fabric_client, "ws-id", "My_Ontology")
        assert result["id"] == "new-ont"
        call_args = mock_fabric_client.post.call_args
        assert call_args[0][0] == "workspaces/ws-id/ontologies"
        body = call_args[0][1]
        assert body["displayName"] == "My_Ontology"
        assert "definition" not in body

    def test_create_ontology_with_definition(self, mock_fabric_client):
        from pyfabric.client.ontology import create_ontology

        mock_fabric_client.post.return_value = {"id": "new-ont"}
        parts = [
            {
                "path": "definition.json",
                "payload": "e30=",
                "payloadType": "InlineBase64",
            }
        ]
        create_ontology(mock_fabric_client, "ws-id", "O1", definition_parts=parts)
        body = mock_fabric_client.post.call_args[0][1]
        assert "definition" in body
        assert body["definition"]["parts"] == parts

    def test_delete_ontology(self, mock_fabric_client):
        from pyfabric.client.ontology import delete_ontology

        delete_ontology(mock_fabric_client, "ws-id", "ont-1")
        mock_fabric_client.delete.assert_called_once_with(
            "workspaces/ws-id/ontologies/ont-1"
        )

    def test_update_ontology_definition(self, mock_fabric_client):
        from pyfabric.client.ontology import update_ontology_definition

        mock_fabric_client.post.return_value = {}
        parts = [
            {
                "path": "definition.json",
                "payload": "e30=",
                "payloadType": "InlineBase64",
            }
        ]
        update_ontology_definition(mock_fabric_client, "ws-id", "ont-1", parts)
        call_args = mock_fabric_client.post.call_args
        assert "updateDefinition" in call_args[0][0]


class TestPartsHelpers:
    """Tests for the low-level definition parts helpers."""

    def test_decode_encode_round_trip(self):
        original = {
            "definition": {
                "parts": [
                    {
                        "path": "definition.json",
                        "payload": base64.b64encode(b'{"key": "value"}').decode(),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }
        decoded = decode_definition(original)
        assert len(decoded) == 1
        assert decoded[0]["path"] == "definition.json"
        assert decoded[0]["content"] == {"key": "value"}

        encoded = encode_definition(decoded)
        assert len(encoded["parts"]) == 1
        payload = base64.b64decode(encoded["parts"][0]["payload"]).decode()
        assert json.loads(payload) == {"key": "value"}

    def test_entity_type_parts_crud(self):
        parts = [{"path": "definition.json", "content": {}}]

        et_id, et_def = make_entity_type_def(
            "TestEntity",
            properties=[
                make_property("Name", "String"),
            ],
        )
        parts = add_entity_type_to_parts(parts, et_id, et_def)
        assert len(parts) == 2

        retrieved = get_entity_type_from_parts(parts, et_id)
        assert retrieved["name"] == "TestEntity"

        entities = list_entity_types_from_parts(parts)
        assert len(entities) == 1

        parts = remove_entity_type_from_parts(parts, et_id)
        assert len(parts) == 1

    def test_entity_name_to_table(self):
        assert entity_name_to_table("CourseModule") == "course_module"
        assert entity_name_to_table("HTMLParser") == "html_parser"
        assert entity_name_to_table("Simple") == "simple"

    def test_build_from_config(self):
        config = {
            "tablePrefix": "test",
            "entities": [
                {
                    "name": "Student",
                    "keyProperty": "StudentId",
                    "properties": [
                        {"name": "StudentId", "valueType": "String"},
                        {"name": "Name", "valueType": "String"},
                    ],
                },
            ],
            "relationships": [],
        }
        parts, entity_map, _rel_map = build_from_config(config)
        assert "Student" in entity_map
        assert entity_map["Student"]["table"] == "test_student"
        assert len(parts) == 2  # definition.json + entity
