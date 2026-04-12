"""Tests for item CRUD operations."""

import base64

from pyfabric.items.crud import (
    create_item,
    decode_part,
    delete_item,
    encode_part,
    get_item,
    list_items,
    update_item,
)


class TestEncodePart:
    def test_encodes_string(self):
        part = encode_part("notebook-content.py", "print('hello')")
        assert part["path"] == "notebook-content.py"
        assert part["payloadType"] == "InlineBase64"
        decoded = base64.b64decode(part["payload"]).decode()
        assert decoded == "print('hello')"

    def test_encodes_bytes(self):
        part = encode_part("data.bin", b"\x00\x01\x02")
        decoded = base64.b64decode(part["payload"])
        assert decoded == b"\x00\x01\x02"


class TestDecodePart:
    def test_decodes_base64(self):
        payload = base64.b64encode(b"hello world").decode()
        result = decode_part({"payload": payload})
        assert result == b"hello world"


class TestItemCrud:
    def test_list_items(self, mock_fabric_client):
        mock_fabric_client.get_paged.return_value = [{"id": "1"}, {"id": "2"}]
        result = list_items(mock_fabric_client, "ws-1")
        assert len(result) == 2
        mock_fabric_client.get_paged.assert_called_once()

    def test_list_items_filtered(self, mock_fabric_client):
        mock_fabric_client.get_paged.return_value = [{"id": "1", "type": "Notebook"}]
        list_items(mock_fabric_client, "ws-1", item_type="Notebook")
        call_args = mock_fabric_client.get_paged.call_args
        assert call_args[0][1] == {"type": "Notebook"}

    def test_get_item(self, mock_fabric_client):
        mock_fabric_client.get.return_value = {"id": "item-1", "displayName": "nb_test"}
        result = get_item(mock_fabric_client, "ws-1", "item-1")
        assert result["displayName"] == "nb_test"

    def test_create_item(self, mock_fabric_client):
        mock_fabric_client.post.return_value = {"id": "new-item"}
        result = create_item(mock_fabric_client, "ws-1", "nb_new", "Notebook")
        assert result["id"] == "new-item"
        body = mock_fabric_client.post.call_args[0][1]
        assert body["displayName"] == "nb_new"
        assert body["type"] == "Notebook"

    def test_create_item_with_definition(self, mock_fabric_client):
        mock_fabric_client.post.return_value = {"id": "new"}
        parts = [encode_part("notebook-content.py", "# code")]
        create_item(
            mock_fabric_client, "ws-1", "nb", "Notebook", definition_parts=parts
        )
        body = mock_fabric_client.post.call_args[0][1]
        assert "definition" in body
        assert body["definition"]["parts"] == parts

    def test_delete_item(self, mock_fabric_client):
        delete_item(mock_fabric_client, "ws-1", "item-1")
        mock_fabric_client.delete.assert_called_once_with(
            "workspaces/ws-1/items/item-1"
        )

    def test_update_item(self, mock_fabric_client):
        mock_fabric_client.patch.return_value = {"id": "item-1"}
        update_item(mock_fabric_client, "ws-1", "item-1", display_name="new_name")
        body = mock_fabric_client.patch.call_args[0][1]
        assert body["displayName"] == "new_name"
