"""Build and operate Microsoft Fabric Environment items.

A Fabric Environment item bundles a Spark compute profile and an
optional set of pip dependencies that augment the runtime's
pre-installed packages. Notebooks attach to an Environment via
``NotebookBuilder.attach_environment(env_logical_id)`` so the
runtime's package set is consistent across runs and discoverable
from the notebook source.

This module covers the **item plane** for Environments:

- :class:`EnvironmentBuilder` — emit a canonical git-sync
  ``Environment`` artifact (``.platform`` + ``Setting/Sparkcompute.yml``
  + optional ``Libraries/PublicLibraries/environment.yml``). Mirrors
  :class:`pyfabric.items.notebook.NotebookBuilder` and
  :class:`pyfabric.items.mirrored_database.MirroredDatabaseBuilder`
  in shape (``to_bundle`` / ``save_to_disk``).
- REST lifecycle helpers — :func:`publish_environment`,
  :func:`get_environment_status`, :func:`wait_for_published`. Each
  takes a ``FabricClient``-like object so tests can stub it cheaply.

Usage::

    from pyfabric.items.environment import (
        EnvironmentBuilder,
        publish_environment,
        wait_for_published,
    )

    env = (
        EnvironmentBuilder()
        .runtime("1.3")
        .compute(
            driver_cores=4,
            driver_memory="28g",
            executor_cores=4,
            executor_memory="28g",
            min_executors=1,
            max_executors=4,
        )
        .pip("requests==2.31.0", "my_project==0.1.7")
    )
    env.save_to_disk("definitions/", display_name="env_my_project")

    # After git-sync creates the Environment item:
    publish_environment(client, ws_id, env_item_id)
    wait_for_published(client, ws_id, env_item_id)

Anti-pattern note
-----------------

Don't list ``pyfabric`` (or ``structlog``) in ``.pip(...)``. pyfabric
is a dev-time / build-time tool, not a notebook runtime dependency.
A notebook should depend on Fabric's pre-installed packages plus the
project's own helper wheel — see
``CLAUDE.md`` and ``claude_memory/notebook_wheel_resources_pattern.md``.
Sessions that try to install pyfabric inside Fabric routinely waste
hours; the right pattern is to move the transform logic into the
project wheel and have the notebook call the wheel.
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import structlog

from pyfabric.items.bundle import ArtifactBundle
from pyfabric.items.normalize import write_artifact_file

log = structlog.get_logger()


_SPARKCOMPUTE_PATH = "Setting/Sparkcompute.yml"
_ENVIRONMENT_YML_PATH = "Libraries/PublicLibraries/environment.yml"

_TERMINAL_OK_STATES = {"success", "published"}
_TERMINAL_FAIL_STATES = {"failed", "cancelled"}


@dataclass
class _Compute:
    driver_cores: int = 4
    driver_memory: str = "28g"
    executor_cores: int = 4
    executor_memory: str = "28g"
    min_executors: int = 1
    max_executors: int = 4
    native_execution_engine: bool = False


# ── Builder ──────────────────────────────────────────────────────────────────


class EnvironmentBuilder:
    """Build a Fabric Environment artifact programmatically.

    Construct, chain ``runtime`` / ``compute`` / ``pip`` calls to
    describe the environment, then call :meth:`to_bundle` or
    :meth:`save_to_disk` to materialize bytes.

    Defaults match Fabric's portal-created Environment shape: runtime
    1.3, 4-core/28g driver and executor, dynamic allocation 1-4
    executors, native execution engine off.
    """

    def __init__(self) -> None:
        self._runtime: str = "1.3"
        self._compute = _Compute()
        self._pip: list[str] = []

    # ── Configuration ────────────────────────────────────────────────────────

    def runtime(self, version: str) -> "EnvironmentBuilder":
        """Set the Fabric Spark runtime version (e.g. ``"1.3"``).

        Emitted unquoted in ``Sparkcompute.yml`` per Fabric's
        normalised form — the round-trip rewrites ``"1.3"`` to
        ``1.3``, so the builder pins the unquoted shape directly.
        """
        self._runtime = version
        return self

    def compute(
        self,
        *,
        driver_cores: int | None = None,
        driver_memory: str | None = None,
        executor_cores: int | None = None,
        executor_memory: str | None = None,
        min_executors: int | None = None,
        max_executors: int | None = None,
        native_execution_engine: bool | None = None,
    ) -> "EnvironmentBuilder":
        """Override one or more compute settings.

        Any argument left as ``None`` keeps the prior value. Defaults
        match Fabric's portal-created Environment.
        """
        c = self._compute
        if driver_cores is not None:
            c.driver_cores = driver_cores
        if driver_memory is not None:
            c.driver_memory = driver_memory
        if executor_cores is not None:
            c.executor_cores = executor_cores
        if executor_memory is not None:
            c.executor_memory = executor_memory
        if min_executors is not None:
            c.min_executors = min_executors
        if max_executors is not None:
            c.max_executors = max_executors
        if native_execution_engine is not None:
            c.native_execution_engine = native_execution_engine
        return self

    def pip(self, *packages: str) -> "EnvironmentBuilder":
        """Append pip pins to the environment.yml.

        Each argument is a single PEP 440 requirement string
        (``"requests==2.31.0"``, ``"my_project==0.1.7"``). Successive
        calls append; the order is preserved.

        Don't include ``pyfabric`` or ``structlog`` here — see the
        module docstring's anti-pattern note.
        """
        self._pip.extend(packages)
        return self

    # ── Rendering ────────────────────────────────────────────────────────────

    def to_sparkcompute_yml(self) -> str:
        """Render ``Sparkcompute.yml`` content as a string.

        Output uses LF internally; :func:`write_artifact_file`
        applies the CRLF + trailing-CRLF normalize rule when writing
        to disk.
        """
        c = self._compute
        return (
            f"enable_native_execution_engine: "
            f"{'true' if c.native_execution_engine else 'false'}\n"
            f"driver_cores: {c.driver_cores}\n"
            f"driver_memory: {c.driver_memory}\n"
            f"executor_cores: {c.executor_cores}\n"
            f"executor_memory: {c.executor_memory}\n"
            f"dynamic_executor_allocation:\n"
            f"  enabled: true\n"
            f"  min_executors: {c.min_executors}\n"
            f"  max_executors: {c.max_executors}\n"
            f"runtime_version: {self._runtime}"
        )

    def to_environment_yml(self) -> str | None:
        """Render ``environment.yml`` content or ``None`` if no pip
        pins are set (in which case the file is omitted entirely)."""
        if not self._pip:
            return None
        lines = ["dependencies:", "  - pip:"]
        lines.extend(f"      - {pkg}" for pkg in self._pip)
        return "\n".join(lines)

    # ── Bundle / disk integration ────────────────────────────────────────────

    def to_bundle(
        self,
        display_name: str,
        *,
        logical_id: str | None = None,
        description: str = "",
    ) -> ArtifactBundle:
        """Bundle the environment artifact for disk save or REST
        upload. ``Sparkcompute.yml`` is stored as a string; line-ending
        normalisation happens at the disk-write boundary."""
        parts: dict[str, str | bytes] = {
            _SPARKCOMPUTE_PATH: self.to_sparkcompute_yml(),
        }
        env_yml = self.to_environment_yml()
        if env_yml is not None:
            parts[_ENVIRONMENT_YML_PATH] = env_yml
        kwargs: dict[str, Any] = {
            "item_type": "Environment",
            "display_name": display_name,
            "parts": parts,
            "description": description,
        }
        if logical_id is not None:
            kwargs["logical_id"] = logical_id
        return ArtifactBundle(**kwargs)

    def save_to_disk(
        self,
        output_dir: str | Path,
        *,
        display_name: str,
        logical_id: str | None = None,
        description: str = "",
    ) -> Path:
        """Write the artifact to disk in canonical Fabric bytes.

        Routes every file through :func:`write_artifact_file`, so
        line-ending and trailing-newline rules are applied
        regardless of OS default. Returns the artifact directory
        path.
        """
        bundle = self.to_bundle(
            display_name=display_name,
            logical_id=logical_id,
            description=description,
        )
        artifact_dir = Path(output_dir) / bundle.dir_name
        artifact_dir.mkdir(parents=True, exist_ok=True)

        write_artifact_file(artifact_dir / ".platform", bundle.platform_json())

        sparkcompute_path = artifact_dir / _SPARKCOMPUTE_PATH
        sparkcompute_path.parent.mkdir(parents=True, exist_ok=True)
        write_artifact_file(sparkcompute_path, self.to_sparkcompute_yml())

        env_yml = self.to_environment_yml()
        if env_yml is not None:
            env_yml_path = artifact_dir / _ENVIRONMENT_YML_PATH
            env_yml_path.parent.mkdir(parents=True, exist_ok=True)
            write_artifact_file(env_yml_path, env_yml)

        log.info(
            "environment_saved",
            display_name=display_name,
            runtime=self._runtime,
            pip_pkgs=len(self._pip),
            path=str(artifact_dir),
        )
        return artifact_dir


# ── REST lifecycle ───────────────────────────────────────────────────────────


class _ClientLike(Protocol):
    """Subset of ``pyfabric.client.http.FabricClient`` the lifecycle
    helpers need. Declared as a Protocol so tests can pass a tiny
    stub without pulling in the whole client + credential machinery."""

    def post(self, path: str, body: Any = None) -> dict[str, Any]: ...

    def get(
        self, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]: ...


def _env_path(workspace_id: str, environment_id: str) -> str:
    return f"workspaces/{workspace_id}/environments/{environment_id}"


def publish_environment(
    client: _ClientLike, workspace_id: str, environment_id: str
) -> dict[str, Any]:
    """Trigger a publish on the staging copy of an Environment.

    Fabric Environments use a staging-then-publish model: the
    artifact files (Sparkcompute.yml, environment.yml) land in the
    staging slot on git-sync; calling
    ``/environments/{id}/staging/publish`` promotes staging to
    published and starts the Spark package install (~5 minutes for
    a non-trivial pip set).

    Returns the API response — typically empty for a 202 LRO. Use
    :func:`wait_for_published` to block until the build finishes.
    """
    return client.post(f"{_env_path(workspace_id, environment_id)}/staging/publish")


def get_environment_status(
    client: _ClientLike, workspace_id: str, environment_id: str
) -> dict[str, Any]:
    """Return the Environment's publish status.

    Body shape (Fabric API v1)::

        {
          "id": "...",
          "displayName": "...",
          ...,
          "publishDetails": {
            "state": "Running" | "Success" | "Published" | "Failed" | "Cancelled",
            "targetVersion": "...",
            ...
          }
        }
    """
    return client.get(_env_path(workspace_id, environment_id))


def wait_for_published(
    client: _ClientLike,
    workspace_id: str,
    environment_id: str,
    *,
    timeout_s: float = 600,
    poll_interval_s: float = 30,
) -> dict[str, Any]:
    """Poll :func:`get_environment_status` until publishing completes.

    Treats ``Success`` and ``Published`` as terminal-OK (Fabric uses
    both depending on tenant). ``Failed`` and ``Cancelled`` raise
    :class:`RuntimeError`. Anything else (typically ``Running``) is
    treated as in-progress.

    Args:
        timeout_s: Total wait budget in seconds. Set to ``0`` to make
            a single check and raise immediately if not terminal.
        poll_interval_s: Sleep between polls. Set to ``0`` in tests.

    Raises:
        RuntimeError: If the environment publish failed or was cancelled.
        TimeoutError: If the publish does not complete within
            ``timeout_s`` seconds.
    """
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}
    while True:
        last = get_environment_status(client, workspace_id, environment_id)
        details = last.get("publishDetails") or {}
        state_raw = details.get("state", "")
        state = state_raw.lower() if isinstance(state_raw, str) else ""

        if state in _TERMINAL_OK_STATES:
            return last
        if state in _TERMINAL_FAIL_STATES:
            raise RuntimeError(f"Environment publish {state_raw}: {last}")
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Environment did not publish within {timeout_s}s. Last status: {last}"
            )
        time.sleep(poll_interval_s)
