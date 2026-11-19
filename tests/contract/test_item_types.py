"""Check item types against Microsoft's public Fabric REST API specification."""

import os
import warnings
from unittest.mock import Mock

import pytest
import requests

from pyfabric.items.normalize import _ITEM_TYPES
from pyfabric.items.types import ITEM_TYPES

_SPEC_URL = (
    "https://raw.githubusercontent.com/microsoft/fabric-rest-api-specs/"
    "main/common/definitions.json"
)
_NETWORK_OPT_IN = "PYFABRIC_TEST_FABRIC_SPECS"


def _load_upstream_item_types() -> set[str]:
    try:
        response = requests.get(_SPEC_URL, timeout=15)
        response.raise_for_status()
    except requests.RequestException as exc:
        pytest.skip(f"Could not fetch Microsoft's Fabric REST API spec: {exc}")

    # Both the workspace Item and CreateItemRequest schemas reference this enum.
    # It gives the wire names without counting service areas such as admin or spark.
    item_types = response.json()["definitions"]["ItemType"]["enum"]
    assert (
        isinstance(item_types, list)
        and item_types
        and all(isinstance(name, str) and name for name in item_types)
    ), "Microsoft's Fabric REST API spec has no valid ItemType enum"
    return set(item_types)


def _assert_registry_matches(registered: set[str], upstream: set[str]) -> None:
    extra = sorted(registered - upstream)
    if extra:
        warnings.warn(
            f"Extra pyfabric item types absent from Microsoft's spec: {', '.join(extra)}",
            stacklevel=2,
        )
    missing = sorted(upstream - registered)
    if missing:
        pytest.fail(f"Item types missing from pyfabric: {', '.join(missing)}")


def test_normalizer_recognizes_all_registered_item_types() -> None:
    assert set(ITEM_TYPES) == _ITEM_TYPES


@pytest.mark.skipif(
    os.environ.get(_NETWORK_OPT_IN) != "1",
    reason=f"Set {_NETWORK_OPT_IN}=1 to fetch Microsoft's public Fabric REST API spec",
)
def test_registry_matches_microsoft_spec() -> None:
    _assert_registry_matches(set(ITEM_TYPES), _load_upstream_item_types())


def test_spec_reader_uses_shared_item_type_enum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get = Mock()
    get.return_value.json.return_value = {
        "definitions": {
            "ItemType": {"enum": ["Notebook", "CosmosDBDatabase"]},
            "ItemPermissions": {"enum": ["Read", "Write"]},
        }
    }
    monkeypatch.setattr(requests, "get", get)

    assert _load_upstream_item_types() == {"Notebook", "CosmosDBDatabase"}
    get.assert_called_once_with(_SPEC_URL, timeout=15)
    get.return_value.raise_for_status.assert_called_once_with()


def test_spec_fetch_failure_skips_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        requests, "get", Mock(side_effect=requests.Timeout("timed out"))
    )

    with pytest.raises(pytest.skip.Exception, match=r"Could not fetch.*timed out"):
        _load_upstream_item_types()


def test_comparison_reports_missing_type_by_name() -> None:
    with pytest.raises(
        pytest.fail.Exception, match=r"Item types missing from pyfabric.*Map"
    ):
        _assert_registry_matches({"Notebook"}, {"Map", "Notebook"})


def test_comparison_reports_extra_type_without_failing() -> None:
    with pytest.warns(UserWarning, match=r"Extra pyfabric item types.*FutureItem"):
        _assert_registry_matches({"FutureItem", "Notebook"}, {"Notebook"})
