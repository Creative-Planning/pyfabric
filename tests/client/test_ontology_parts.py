"""Tests for ontology low-level parts helpers using pairwise combinations."""

import json

from allpairspy import AllPairs

from pyfabric.client.ontology import (
    add_contextualization_to_parts,
    add_data_binding_to_parts,
    add_entity_type_to_parts,
    add_relationship_type_to_parts,
    decode_definition,
    encode_definition,
    entity_name_to_table,
    generate_guid,
    generate_id,
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
    remove_data_binding_from_parts,
    remove_entity_type_from_parts,
    remove_relationship_type_from_parts,
    update_data_binding_in_parts,
    update_entity_type_in_parts,
    update_relationship_type_in_parts,
)


class TestIdGenerators:
    def test_generate_id_is_large_positive(self):
        for _ in range(10):
            val = int(generate_id())
            assert val > 10**14

    def test_generate_guid_is_uuid_format(self):
        guid = generate_guid()
        assert len(guid) == 36
        assert guid.count("-") == 4


class TestMakeProperty:
    def test_basic(self):
        p = make_property("Age", "BigInt")
        assert p["name"] == "Age"
        assert p["valueType"] == "BigInt"
        assert "id" in p

    def test_with_explicit_id(self):
        p = make_property("Name", "String", prop_id="custom-id")
        assert p["id"] == "custom-id"


class TestMakeBindings:
    def test_property_binding(self):
        b = make_property_binding("col_name", "prop-123")
        assert b["sourceColumnName"] == "col_name"
        assert b["targetPropertyId"] == "prop-123"

    def test_key_ref_binding(self):
        b = make_key_ref_binding("fk_col", "prop-456")
        assert b["sourceColumnName"] == "fk_col"
        assert b["targetPropertyId"] == "prop-456"


class TestMakeEntityTypeDef:
    def test_basic(self):
        et_id, et_def = make_entity_type_def("Customer")
        assert et_def["name"] == "Customer"
        assert et_def["namespace"] == "usertypes"
        assert et_id == et_def["id"]

    def test_with_properties(self):
        props = [make_property("Id", "String"), make_property("Name", "String")]
        _et_id, et_def = make_entity_type_def("Customer", properties=props)
        assert len(et_def["properties"]) == 2

    def test_with_entity_id_parts(self):
        _, et_def = make_entity_type_def("X", entity_id_parts=["p1", "p2"])
        assert et_def["entityIdParts"] == ["p1", "p2"]


class TestMakeRelationshipTypeDef:
    def test_basic(self):
        _rt_id, rt_def = make_relationship_type_def("has_orders", "et-1", "et-2")
        assert rt_def["name"] == "has_orders"
        assert rt_def["source"]["entityTypeId"] == "et-1"
        assert rt_def["target"]["entityTypeId"] == "et-2"


class TestMakeLakehouseBinding:
    def test_basic(self):
        bindings = [make_property_binding("col", "prop-1")]
        bid, bdef = make_lakehouse_binding(
            "et-1", bindings, "ws-1", "lh-1", "customers"
        )
        assert bdef["id"] == bid
        src = bdef["dataBindingConfiguration"]["sourceTableProperties"]
        assert src["sourceType"] == "LakehouseTable"
        assert src["sourceTableName"] == "customers"

    def test_timeseries(self):
        bindings = [make_property_binding("ts", "prop-ts")]
        _, bdef = make_lakehouse_binding(
            "et-1",
            bindings,
            "ws-1",
            "lh-1",
            "events",
            binding_type="TimeSeries",
            timestamp_column="ts",
        )
        assert bdef["dataBindingConfiguration"]["dataBindingType"] == "TimeSeries"
        assert bdef["dataBindingConfiguration"]["timestampColumnName"] == "ts"


class TestMakeWarehouseBinding:
    def test_basic(self):
        bindings = [make_property_binding("col", "prop-1")]
        _bid, bdef = make_warehouse_binding(
            "et-1", bindings, "ws-1", "wh-1", "dim_customer"
        )
        assert (
            bdef["dataBindingConfiguration"]["sourceTableProperties"]["sourceTableName"]
            == "dim_customer"
        )


class TestMakeKqlBinding:
    def test_basic(self):
        bindings = [make_property_binding("col", "prop-1")]
        _bid, bdef = make_kql_binding(
            "et-1",
            bindings,
            "ws-1",
            "eh-1",
            "https://cluster.kusto.windows.net",
            "mydb",
            "events",
            timestamp_column="Timestamp",
        )
        src = bdef["dataBindingConfiguration"]["sourceTableProperties"]
        assert src["sourceType"] == "KustoTable"
        assert src["clusterUri"] == "https://cluster.kusto.windows.net"


class TestMakeContextualizationDef:
    def test_basic(self):
        src_bindings = [make_key_ref_binding("fk_customer", "prop-c")]
        tgt_bindings = [make_key_ref_binding("fk_order", "prop-o")]
        cid, cdef = make_contextualization_def(
            "ws-1", "lh-1", "join_table", src_bindings, tgt_bindings
        )
        assert cdef["id"] == cid
        assert cdef["dataBindingTable"]["sourceTableName"] == "join_table"


class TestEntityTypePartsCrud:
    def _base_parts(self):
        return [{"path": "definition.json", "content": {}}]

    def test_add_and_get(self):
        parts = self._base_parts()
        et_id, et_def = make_entity_type_def(
            "Customer", properties=[make_property("Id", "String")]
        )
        parts = add_entity_type_to_parts(parts, et_id, et_def)
        assert len(parts) == 2
        retrieved = get_entity_type_from_parts(parts, et_id)
        assert retrieved["name"] == "Customer"

    def test_list(self):
        parts = self._base_parts()
        for name in ["A", "B", "C"]:
            et_id, et_def = make_entity_type_def(name)
            parts = add_entity_type_to_parts(parts, et_id, et_def)
        assert len(list_entity_types_from_parts(parts)) == 3

    def test_update(self):
        parts = self._base_parts()
        et_id, et_def = make_entity_type_def("Old")
        parts = add_entity_type_to_parts(parts, et_id, et_def)
        _, new_def = make_entity_type_def("New", entity_id=et_id)
        parts = update_entity_type_in_parts(parts, et_id, new_def)
        assert get_entity_type_from_parts(parts, et_id)["name"] == "New"

    def test_remove(self):
        parts = self._base_parts()
        et_id, et_def = make_entity_type_def("ToRemove")
        parts = add_entity_type_to_parts(parts, et_id, et_def)
        parts = remove_entity_type_from_parts(parts, et_id)
        assert get_entity_type_from_parts(parts, et_id) is None

    def test_get_nonexistent_returns_none(self):
        assert get_entity_type_from_parts(self._base_parts(), "fake-id") is None


class TestRelationshipTypePartsCrud:
    def _base_parts(self):
        return [{"path": "definition.json", "content": {}}]

    def test_add_and_get(self):
        parts = self._base_parts()
        rt_id, rt_def = make_relationship_type_def("rel", "et-1", "et-2")
        parts = add_relationship_type_to_parts(parts, rt_id, rt_def)
        assert get_relationship_type_from_parts(parts, rt_id)["name"] == "rel"

    def test_list(self):
        parts = self._base_parts()
        for name in ["r1", "r2"]:
            rt_id, rt_def = make_relationship_type_def(name, "et-1", "et-2")
            parts = add_relationship_type_to_parts(parts, rt_id, rt_def)
        assert len(list_relationship_types_from_parts(parts)) == 2

    def test_update(self):
        parts = self._base_parts()
        rt_id, rt_def = make_relationship_type_def("old_rel", "et-1", "et-2")
        parts = add_relationship_type_to_parts(parts, rt_id, rt_def)
        _, new_def = make_relationship_type_def(
            "new_rel", "et-1", "et-2", relationship_id=rt_id
        )
        parts = update_relationship_type_in_parts(parts, rt_id, new_def)
        assert get_relationship_type_from_parts(parts, rt_id)["name"] == "new_rel"

    def test_remove(self):
        parts = self._base_parts()
        rt_id, rt_def = make_relationship_type_def("to_remove", "et-1", "et-2")
        parts = add_relationship_type_to_parts(parts, rt_id, rt_def)
        parts = remove_relationship_type_from_parts(parts, rt_id)
        assert get_relationship_type_from_parts(parts, rt_id) is None


class TestDataBindingPartsCrud:
    def _setup(self):
        parts = [{"path": "definition.json", "content": {}}]
        et_id, et_def = make_entity_type_def(
            "E", properties=[make_property("Id", "String")]
        )
        parts = add_entity_type_to_parts(parts, et_id, et_def)
        return parts, et_id

    def test_add_and_get(self):
        parts, et_id = self._setup()
        bindings = [make_property_binding("col", "prop-1")]
        bid, bdef = make_lakehouse_binding(et_id, bindings, "ws", "lh", "tbl")
        parts = add_data_binding_to_parts(parts, et_id, bid, bdef)
        assert get_data_binding_from_parts(parts, et_id, bid)["id"] == bid

    def test_list(self):
        parts, et_id = self._setup()
        for i in range(3):
            bid, bdef = make_lakehouse_binding(et_id, [], "ws", "lh", f"tbl_{i}")
            parts = add_data_binding_to_parts(parts, et_id, bid, bdef)
        assert len(list_data_bindings_from_parts(parts, et_id)) == 3

    def test_list_all(self):
        parts, et_id = self._setup()
        bid, bdef = make_lakehouse_binding(et_id, [], "ws", "lh", "tbl")
        parts = add_data_binding_to_parts(parts, et_id, bid, bdef)
        assert len(list_data_bindings_from_parts(parts)) == 1

    def test_update(self):
        parts, et_id = self._setup()
        bid, bdef = make_lakehouse_binding(et_id, [], "ws", "lh", "old_tbl")
        parts = add_data_binding_to_parts(parts, et_id, bid, bdef)
        _, new_bdef = make_lakehouse_binding(
            et_id, [], "ws", "lh", "new_tbl", binding_id=bid
        )
        parts = update_data_binding_in_parts(parts, et_id, bid, new_bdef)
        updated = get_data_binding_from_parts(parts, et_id, bid)
        assert (
            updated["dataBindingConfiguration"]["sourceTableProperties"][
                "sourceTableName"
            ]
            == "new_tbl"
        )

    def test_remove(self):
        parts, et_id = self._setup()
        bid, bdef = make_lakehouse_binding(et_id, [], "ws", "lh", "tbl")
        parts = add_data_binding_to_parts(parts, et_id, bid, bdef)
        parts = remove_data_binding_from_parts(parts, et_id, bid)
        assert get_data_binding_from_parts(parts, et_id, bid) is None


class TestContextualizationPartsCrud:
    def _setup(self):
        parts = [{"path": "definition.json", "content": {}}]
        rt_id, rt_def = make_relationship_type_def("rel", "et-1", "et-2")
        parts = add_relationship_type_to_parts(parts, rt_id, rt_def)
        return parts, rt_id

    def test_add_and_list(self):
        parts, rt_id = self._setup()
        src = [make_key_ref_binding("fk", "p1")]
        tgt = [make_key_ref_binding("pk", "p2")]
        cid, cdef = make_contextualization_def("ws", "lh", "join", src, tgt)
        parts = add_contextualization_to_parts(parts, rt_id, cid, cdef)
        ctxs = list_contextualizations_from_parts(parts, rt_id)
        assert len(ctxs) == 1

    def test_list_all(self):
        parts, rt_id = self._setup()
        cid, cdef = make_contextualization_def("ws", "lh", "j", [], [])
        parts = add_contextualization_to_parts(parts, rt_id, cid, cdef)
        assert len(list_contextualizations_from_parts(parts)) == 1


class TestDecodeEncode:
    def test_round_trip(self):
        import base64

        original = {
            "definition": {
                "parts": [
                    {
                        "path": "test.json",
                        "payload": base64.b64encode(
                            json.dumps({"key": "value"}).encode()
                        ).decode(),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }
        decoded = decode_definition(original)
        assert decoded[0]["content"] == {"key": "value"}
        encoded = encode_definition(decoded)
        re_decoded = json.loads(base64.b64decode(encoded["parts"][0]["payload"]))
        assert re_decoded == {"key": "value"}


class TestEntityNameToTable:
    def test_camel_case(self):
        assert entity_name_to_table("CourseModule") == "course_module"

    def test_acronym(self):
        assert entity_name_to_table("HTMLParser") == "html_parser"

    def test_simple(self):
        assert entity_name_to_table("Simple") == "simple"

    def test_spaces(self):
        assert entity_name_to_table("My Entity") == "my_entity"


class TestPairwiseMakeEntityTypeDef:
    """Use allpairspy to cover combinations of entity type parameters."""

    def test_pairwise_entity_type_creation(self):
        names = ["Customer", "Order", "Product"]
        value_types = ["String", "BigInt", "Double", "Boolean", "DateTime"]
        prop_counts = [0, 1, 3]

        for combo in AllPairs([names, value_types, prop_counts]):
            name, vtype, count = combo
            props = [make_property(f"prop_{i}", vtype) for i in range(count)]
            _et_id, et_def = make_entity_type_def(
                name, properties=props if props else None
            )
            assert et_def["name"] == name
            assert len(et_def["properties"]) == count
