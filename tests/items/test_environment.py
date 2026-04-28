"""Tests for :mod:`pyfabric.items.environment`.

Two surfaces:

- :class:`EnvironmentBuilder` (artifact plane) — emits a Fabric
  Environment item with ``Setting/Sparkcompute.yml`` (CRLF + trailing
  CRLF) and optional ``Libraries/PublicLibraries/environment.yml``
  (LF, no trailing newline). The byte-equality bar matches the rest
  of pyfabric's builders: hand-rolling these files is the trap
  every Fabric round-trip rewrites.

- REST lifecycle: ``publish_environment``, ``get_environment_status``,
  ``wait_for_published``. Stubbed via a tiny client-like object so
  the tests don't need a credential or network.

Examples in this module use generic package names (``my_project``,
``requests``) rather than pyfabric or structlog. Per
``CLAUDE.md`` and ``claude_memory/notebook_wheel_resources_pattern.md``,
**pyfabric is dev-time only** and is not a notebook runtime
dependency.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from pyfabric.items.environment import (
    EnvironmentBuilder,
    get_environment_status,
    publish_environment,
    wait_for_published,
)

# ── Builder: Sparkcompute.yml ────────────────────────────────────────────────


class TestSparkcomputeYml:
    def test_default_runtime_is_one_three(self):
        env = EnvironmentBuilder()
        yml = env.to_sparkcompute_yml()
        # Unquoted number per Fabric's normalised form.
        assert "runtime_version: 1.3" in yml
        assert 'runtime_version: "1.3"' not in yml

    def test_runtime_override_is_emitted_unquoted(self):
        env = EnvironmentBuilder().runtime("1.4")
        yml = env.to_sparkcompute_yml()
        assert "runtime_version: 1.4" in yml

    def test_default_compute_block_matches_fabric_defaults(self):
        env = EnvironmentBuilder()
        yml = env.to_sparkcompute_yml()
        for line in (
            "driver_cores: 4",
            "driver_memory: 28g",
            "executor_cores: 4",
            "executor_memory: 28g",
            "enable_native_execution_engine: false",
        ):
            assert line in yml

    def test_dynamic_executor_allocation_block_is_nested(self):
        env = EnvironmentBuilder().compute(min_executors=2, max_executors=8)
        yml = env.to_sparkcompute_yml()
        assert "dynamic_executor_allocation:" in yml
        assert "  enabled: true" in yml
        assert "  min_executors: 2" in yml
        assert "  max_executors: 8" in yml

    def test_compute_overrides_propagate(self):
        env = EnvironmentBuilder().compute(
            driver_cores=8, executor_memory="56g", native_execution_engine=True
        )
        yml = env.to_sparkcompute_yml()
        assert "driver_cores: 8" in yml
        assert "executor_memory: 56g" in yml
        assert "enable_native_execution_engine: true" in yml


# ── Builder: environment.yml ─────────────────────────────────────────────────


class TestEnvironmentYml:
    def test_no_pip_packages_means_no_environment_yml(self):
        env = EnvironmentBuilder()
        assert env.to_environment_yml() is None

    def test_pip_packages_render_under_dependencies_pip(self):
        env = EnvironmentBuilder().pip("requests==2.31.0", "my_project==0.1.7")
        yml = env.to_environment_yml()
        assert yml is not None
        assert "dependencies:" in yml
        assert "  - pip:" in yml
        assert "      - requests==2.31.0" in yml
        assert "      - my_project==0.1.7" in yml

    def test_pip_chain_appends(self):
        env = EnvironmentBuilder().pip("a==1").pip("b==2")
        yml = env.to_environment_yml()
        assert yml is not None
        assert "      - a==1" in yml
        assert "      - b==2" in yml


# ── Builder: bundle + disk ───────────────────────────────────────────────────


class TestToBundle:
    def test_parts_use_canonical_subpaths(self):
        env = EnvironmentBuilder()
        bundle = env.to_bundle(display_name="env_test")
        assert bundle.item_type == "Environment"
        assert "Setting/Sparkcompute.yml" in bundle.parts

    def test_environment_yml_only_when_pip_present(self):
        env = EnvironmentBuilder()
        bundle = env.to_bundle(display_name="env_test")
        assert "Libraries/PublicLibraries/environment.yml" not in bundle.parts

        env_with_pip = EnvironmentBuilder().pip("requests==2.31.0")
        bundle2 = env_with_pip.to_bundle(display_name="env_test")
        assert "Libraries/PublicLibraries/environment.yml" in bundle2.parts

    def test_logical_id_override(self):
        bundle = EnvironmentBuilder().to_bundle(
            display_name="env_test",
            logical_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        )
        assert bundle.logical_id == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


class TestSaveToDisk:
    def test_writes_platform_lf_no_trailing_newline(self, tmp_path):
        env = EnvironmentBuilder()
        artifact_dir = env.save_to_disk(tmp_path, display_name="env_test")
        raw = (artifact_dir / ".platform").read_bytes()
        assert b"\r\n" not in raw
        assert not raw.endswith(b"\n")

    def test_writes_sparkcompute_yml_crlf_with_trailing_crlf(self, tmp_path):
        """Sparkcompute.yml is the one Fabric file with CRLF + trailing
        CRLF. The normalize rule already covers this; the test pins the
        contract."""
        env = EnvironmentBuilder()
        artifact_dir = env.save_to_disk(tmp_path, display_name="env_test")
        raw = (artifact_dir / "Setting" / "Sparkcompute.yml").read_bytes()
        assert b"\r\n" in raw
        assert raw.endswith(b"\r\n")
        # No bare LF that wasn't part of a CRLF pair.
        assert raw.count(b"\n") == raw.count(b"\r\n")

    def test_environment_yml_lf_no_trailing_newline(self, tmp_path):
        env = EnvironmentBuilder().pip("requests==2.31.0")
        artifact_dir = env.save_to_disk(tmp_path, display_name="env_test")
        raw = (
            artifact_dir / "Libraries" / "PublicLibraries" / "environment.yml"
        ).read_bytes()
        assert b"\r\n" not in raw
        assert not raw.endswith(b"\n")

    def test_environment_yml_skipped_when_no_pip(self, tmp_path):
        env = EnvironmentBuilder()
        artifact_dir = env.save_to_disk(tmp_path, display_name="env_test")
        env_yml = artifact_dir / "Libraries" / "PublicLibraries" / "environment.yml"
        assert not env_yml.exists()

    def test_returns_artifact_dir(self, tmp_path):
        env = EnvironmentBuilder()
        artifact_dir = env.save_to_disk(tmp_path, display_name="env_test")
        assert artifact_dir == tmp_path / "env_test.Environment"
        assert artifact_dir.is_dir()


# ── REST lifecycle ───────────────────────────────────────────────────────────


class _FakeClient:
    """Minimal _ClientLike stub. Captures last call for assertions."""

    def __init__(self) -> None:
        self.post_calls: list[tuple[str, Any]] = []
        self.get_calls: list[tuple[str, Any]] = []
        self.post_returns: dict[str, Any] = {}
        self.get_returns: dict[str, Any] = {}

    def post(self, path: str, body: Any = None) -> dict[str, Any]:
        self.post_calls.append((path, body))
        return self.post_returns.copy() if self.post_returns else {}

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.get_calls.append((path, params))
        return self.get_returns.copy() if self.get_returns else {}


WS = "11111111-1111-1111-1111-111111111111"
ENV = "22222222-2222-2222-2222-222222222222"


class TestPublishEnvironment:
    def test_posts_to_publish_endpoint(self):
        client = _FakeClient()
        publish_environment(client, WS, ENV)
        assert len(client.post_calls) == 1
        path, body = client.post_calls[0]
        assert path.endswith(f"/environments/{ENV}/staging/publish")
        assert body is None


class TestGetEnvironmentStatus:
    def test_gets_publish_details(self):
        client = _FakeClient()
        client.get_returns = {"publishDetails": {"state": "Running"}}
        body = get_environment_status(client, WS, ENV)
        assert body["publishDetails"]["state"] == "Running"
        path, _ = client.get_calls[0]
        assert path.endswith(f"/environments/{ENV}")


class TestWaitForPublished:
    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("pyfabric.items.environment.time.sleep"):
            yield

    def test_returns_when_state_is_success(self):
        client = _FakeClient()
        client.get_returns = {"publishDetails": {"state": "Success"}}
        body = wait_for_published(client, WS, ENV, poll_interval_s=0)
        assert body["publishDetails"]["state"] == "Success"

    def test_accepts_published_alias(self):
        """Some Fabric tenants emit ``Published`` instead of ``Success``;
        the poller treats both as terminal-OK."""
        client = _FakeClient()
        client.get_returns = {"publishDetails": {"state": "Published"}}
        body = wait_for_published(client, WS, ENV, poll_interval_s=0)
        assert body["publishDetails"]["state"] == "Published"

    def test_raises_runtime_error_on_failed(self):
        client = _FakeClient()
        client.get_returns = {"publishDetails": {"state": "Failed"}}
        with pytest.raises(RuntimeError, match="Failed"):
            wait_for_published(client, WS, ENV, poll_interval_s=0)

    def test_raises_timeout_when_running_past_deadline(self):
        client = _FakeClient()
        client.get_returns = {"publishDetails": {"state": "Running"}}
        with pytest.raises(TimeoutError):
            wait_for_published(client, WS, ENV, timeout_s=0, poll_interval_s=0)

    def test_state_lookup_is_case_insensitive(self):
        client = _FakeClient()
        client.get_returns = {"publishDetails": {"state": "success"}}
        body = wait_for_published(client, WS, ENV, poll_interval_s=0)
        assert body["publishDetails"]["state"] == "success"
