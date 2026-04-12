"""Mock notebookutils / mssparkutils for local Fabric notebook testing.

Provides a drop-in replacement for Fabric's ``notebookutils`` that works
locally without a Fabric runtime. Filesystem operations use the local
filesystem; notebook.run() is a no-op; credentials.getToken() raises.

Usage::

    from pyfabric.testing.mock_notebookutils import MockNotebookUtils

    notebookutils = MockNotebookUtils(root=Path("./test_data"))
    notebookutils.fs.mkdirs("/my/path")
    files = notebookutils.fs.ls("/my/path")
"""

from __future__ import annotations

import shutil
from pathlib import Path

import structlog

log = structlog.get_logger()


class _MockFs:
    """Mock for notebookutils.fs — local filesystem operations."""

    def __init__(self, root: Path):
        self._root = root

    def _resolve(self, path: str) -> Path:
        """Resolve a path relative to the mock root."""
        return self._root / path.lstrip("/")

    def ls(self, path: str) -> list[str]:
        """List files/directories at path."""
        p = self._resolve(path)
        if not p.exists():
            return []
        return [str(child) for child in sorted(p.iterdir())]

    def mkdirs(self, path: str) -> None:
        """Create directories recursively."""
        p = self._resolve(path)
        p.mkdir(parents=True, exist_ok=True)
        log.debug("notebookutils.fs.mkdirs", path=str(p))

    def cp(self, src: str, dst: str, recurse: bool = False) -> None:
        """Copy file or directory."""
        src_p, dst_p = self._resolve(src), self._resolve(dst)
        if recurse:
            shutil.copytree(src_p, dst_p, dirs_exist_ok=True)
        else:
            shutil.copy2(src_p, dst_p)

    def rm(self, path: str, recurse: bool = False) -> None:
        """Remove file or directory."""
        p = self._resolve(path)
        if recurse and p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()

    def put(self, path: str, content: str | bytes) -> None:
        """Write content to a file."""
        p = self._resolve(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            p.write_bytes(content)
        else:
            p.write_text(content, encoding="utf-8")

    def head(self, path: str, max_bytes: int = 65536) -> str:
        """Read the first N bytes of a file as text."""
        p = self._resolve(path)
        return p.read_text(encoding="utf-8")[:max_bytes]


class _MockNotebook:
    """Mock for notebookutils.notebook — run/exit are no-ops."""

    def run(
        self,
        name: str,
        timeout_seconds: int = 300,
        arguments: dict[str, str] | None = None,
    ) -> str:
        """No-op: logs the call but doesn't execute anything."""
        log.info("notebookutils.notebook.run", name=name, arguments=arguments)
        return ""

    def exit(self, value: str) -> None:
        """No-op: logs the exit value."""
        log.debug("notebookutils.notebook.exit", value=value)


class _MockCredentials:
    """Mock for notebookutils.credentials — always raises."""

    def getToken(self, audience: str, client_id: str | None = None) -> str:
        """Raises NotImplementedError — use pyfabric.client.auth instead."""
        raise NotImplementedError(
            f"getToken('{audience}') is not available in local mode. "
            "Use pyfabric.client.auth.FabricCredential instead."
        )


class MockNotebookUtils:
    """Drop-in replacement for Fabric notebookutils / mssparkutils.

    Args:
        root: Base directory for filesystem operations. Defaults to cwd.
    """

    def __init__(self, root: Path | None = None):
        self._root = root or Path.cwd()
        self.fs = _MockFs(self._root)
        self.notebook = _MockNotebook()
        self.credentials = _MockCredentials()
        # Fabric compatibility alias
        self.mssparkutils = self
