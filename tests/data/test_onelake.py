"""Tests for OneLake URL builders and helper functions."""

from pyfabric.data.onelake import ONELAKE_DFS, _dfs_url, _hdrs, abfss_url


class TestAbfssUrl:
    def test_with_path(self):
        url = abfss_url("ws-1", "lh-1", "Tables/dbo/products")
        assert (
            url
            == "abfss://ws-1@onelake.dfs.fabric.microsoft.com/lh-1/Tables/dbo/products"
        )

    def test_without_path(self):
        url = abfss_url("ws-1", "lh-1")
        assert url == "abfss://ws-1@onelake.dfs.fabric.microsoft.com/lh-1"

    def test_empty_path(self):
        url = abfss_url("ws-1", "lh-1", "")
        assert url == "abfss://ws-1@onelake.dfs.fabric.microsoft.com/lh-1"


class TestDfsUrl:
    def test_builds_https_url(self):
        url = _dfs_url("ws-1", "lh-1", "Tables/dbo/products")
        assert url == f"{ONELAKE_DFS}/ws-1/lh-1/Tables/dbo/products"


class TestHdrs:
    def test_builds_bearer_header(self):
        h = _hdrs("my-token")
        assert h == {"Authorization": "Bearer my-token"}
