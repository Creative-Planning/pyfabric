"""Tests for CLI environment registry and argument helpers."""

import pytest

from pyfabric.cli import _ENV_REGISTRY, list_envs, register_env, resolve_env


@pytest.fixture(autouse=True)
def _clear_registry():
    """Clear the env registry before each test."""
    _ENV_REGISTRY.clear()
    yield
    _ENV_REGISTRY.clear()


class TestEnvironmentRegistry:
    def test_register_and_list(self):
        register_env("proj", "dev", {"ws_id": "ws-dev"})
        register_env("proj", "prod", {"ws_id": "ws-prod"})
        assert set(list_envs("proj")) == {"dev", "prod"}

    def test_list_empty_project(self):
        assert list_envs("unknown") == []

    def test_resolve_env_found(self):
        register_env("proj", "uat", {"ws_id": "ws-uat", "lh_id": "lh-1"})
        config = resolve_env("proj", "uat")
        assert config["ws_id"] == "ws-uat"
        assert config["lh_id"] == "lh-1"

    def test_resolve_env_missing_raises(self):
        register_env("proj", "dev", {})
        with pytest.raises(KeyError, match="Unknown environment"):
            resolve_env("proj", "staging")

    def test_resolve_env_no_project_raises(self):
        with pytest.raises(KeyError, match="Unknown environment"):
            resolve_env("missing_proj", "dev")

    def test_register_overwrites(self):
        register_env("proj", "dev", {"v": 1})
        register_env("proj", "dev", {"v": 2})
        assert resolve_env("proj", "dev")["v"] == 2
