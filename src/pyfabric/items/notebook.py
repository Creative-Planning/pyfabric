"""Programmatic emission of Fabric ``notebook-content.py`` files.

Every Fabric git-sync cycle rewrites ``notebook-content.py`` to a strict
canonical byte layout — header METADATA block, per-cell markers, per-cell
trailing METADATA block, LF line endings, trailing newline. Hand-rolling
those bytes is error-prone; :class:`NotebookBuilder` centralizes the
cell-marker assembly so callers can describe a notebook at a higher level
and trust the output round-trips cleanly through git-sync.

Usage::

    from pyfabric.items.notebook import NotebookBuilder
    from pyfabric.items.bundle import save_to_disk

    nb = (
        NotebookBuilder()
        .attach_lakehouse(ws_id, lh_id, lh_name="lh_bronze", default=True)
        .add_markdown("# Bronze ingest")
        .pip_install_from_resources("mgc_projections-0.1.23-py3-none-any.whl")
        .add_python("from mgc_projections.bronze import run; run()")
    )

    # Route A: builder owns the write, every file goes through
    # :func:`pyfabric.items.normalize.write_artifact_file`.
    nb.save_to_disk("definitions/", display_name="nb_bronze_ingest")

    # Route B: build an ArtifactBundle and hand it to the existing bundle
    # machinery. The notebook-content.py content is stored as canonical
    # bytes so ``bundle.save_to_disk`` preserves LF on Windows.
    bundle = nb.to_bundle(display_name="nb_bronze_ingest")
    save_to_disk(bundle, "definitions/")
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import structlog

from pyfabric.items.bundle import ArtifactBundle
from pyfabric.items.normalize import canonical_bytes, write_artifact_file

log = structlog.get_logger()


CellLanguage = Literal["python", "sparksql"]


# Cell language → language_group mapping for the per-cell trailing META
# block. Keyed on the concrete cell language; the language_group is the
# kernel family Fabric associates with each language.
_LANGUAGE_GROUP: dict[CellLanguage, str] = {
    "python": "synapse_pyspark",
    "sparksql": "synapse_pyspark",
}


@dataclass
class _Lakehouse:
    ws_id: str
    lh_id: str
    lh_name: str | None = None


@dataclass
class _Environment:
    env_id: str
    ws_id: str  # all-zeros means "same workspace as the notebook"


@dataclass
class _Cell:
    kind: Literal["markdown", "code"]
    content: str
    language: CellLanguage | None = None  # code-only


_SAME_WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"
_NOTEBOOK_SETTINGS_JSON = '{"includeResourcesInGit": "on"}'


class NotebookBuilder:
    """Build a Fabric ``notebook-content.py`` source programmatically.

    Construct, chain ``attach_lakehouse`` / ``add_*`` calls to describe
    the notebook, then call :meth:`to_source_string`, :meth:`to_bundle`,
    or :meth:`save_to_disk` to materialize bytes.

    Args:
        kernel: Fabric kernel name used in the header ``kernel_info``
            block. Default ``"synapse_pyspark"`` matches the Fabric
            default for Lakehouse-attached notebooks.
    """

    def __init__(self, kernel: str = "synapse_pyspark") -> None:
        self.kernel = kernel
        self._lakehouses: list[_Lakehouse] = []
        self._default_lakehouse_idx: int | None = None
        self._environment: _Environment | None = None
        self._cells: list[_Cell] = []

    # ── Lakehouse attach ─────────────────────────────────────────────────────

    def attach_lakehouse(
        self,
        ws_id: str,
        lh_id: str,
        *,
        lh_name: str | None = None,
        default: bool = False,
    ) -> "NotebookBuilder":
        """Register a lakehouse dependency on the notebook.

        Args:
            ws_id: Workspace ID that owns the lakehouse.
            lh_id: Lakehouse item ID.
            lh_name: Display name — only meaningful when ``default=True``
                (populates the ``default_lakehouse_name`` field).
            default: Whether this lakehouse is the notebook's default.
                Only one lakehouse can be flagged default.
        """
        if default and self._default_lakehouse_idx is not None:
            raise ValueError("a default lakehouse is already set; only one is allowed")
        self._lakehouses.append(_Lakehouse(ws_id=ws_id, lh_id=lh_id, lh_name=lh_name))
        if default:
            self._default_lakehouse_idx = len(self._lakehouses) - 1
        return self

    # ── Environment attach ───────────────────────────────────────────────────

    def attach_environment(
        self,
        env_id: str,
        *,
        ws_id: str | None = None,
    ) -> "NotebookBuilder":
        """Attach a Fabric Environment dependency to the notebook.

        Adds an ``environment`` block under ``dependencies`` in the
        notebook header METADATA. The environment supplies any
        non-runtime Python packages the notebook needs (e.g. a
        project's own helper wheel pinned via the Environment's
        custom libraries).

        Args:
            env_id: Environment logicalId (the GUID Fabric uses to
                identify the Environment item).
            ws_id: Workspace GUID hosting the Environment. Pass
                ``None`` (default) when the Environment lives in the
                same workspace as the notebook — Fabric's convention
                is to render that as an all-zeros GUID.

        A second call replaces the first attachment; only one
        environment per notebook is supported.
        """
        self._environment = _Environment(
            env_id=env_id,
            ws_id=ws_id if ws_id is not None else _SAME_WORKSPACE_ID,
        )
        return self

    # ── Cells ────────────────────────────────────────────────────────────────

    def add_markdown(self, content: str) -> "NotebookBuilder":
        """Append a markdown cell. Every line is prefixed with ``# ``
        (hash-space) when emitted."""
        self._cells.append(_Cell(kind="markdown", content=content))
        return self

    def add_python(self, code: str) -> "NotebookBuilder":
        """Append a Python code cell."""
        self._cells.append(_Cell(kind="code", content=code, language="python"))
        return self

    def pip_install_from_resources(self, wheel_name: str) -> "NotebookBuilder":
        """Convenience for ``%pip install "builtin/<wheel_name>" --quiet``.

        Fabric resolves ``builtin/<name>`` against the notebook's
        attached resources folder, so this is the canonical install
        pattern for wheels shipped alongside the notebook.
        """
        line = f'%pip install "builtin/{wheel_name}" --quiet'
        return self.add_python(line)

    # ── Emission ─────────────────────────────────────────────────────────────

    def to_source_string(self) -> str:
        """Render the full ``notebook-content.py`` source as a string.

        The returned string ends with a trailing ``\\n`` — Fabric's
        canonical form for ``notebook-content.py``.
        """
        parts: list[str] = [
            "# Fabric notebook source\n",
            "\n",
            self._header_block(),
        ]
        for cell in self._cells:
            parts.append("\n")
            parts.append(self._render_cell(cell))
        return "".join(parts)

    def to_bundle(
        self,
        display_name: str,
        *,
        logical_id: str | None = None,
        description: str = "",
    ) -> ArtifactBundle:
        """Bundle the notebook for disk save or REST upload.

        The ``notebook-content.py`` content is stored as **canonical
        bytes** (LF + trailing newline, no BOM) rather than a string —
        this makes :func:`pyfabric.items.bundle.save_to_disk` take the
        ``write_bytes`` branch, which preserves LF on Windows. Using a
        plain string would route through ``write_text`` and corrupt the
        line endings on non-Unix hosts.
        """
        source = self.to_source_string()
        canonical_content = canonical_bytes(
            "nb.Notebook/notebook-content.py", source.encode("utf-8")
        )
        # notebook-settings.json: required for Fabric to include
        # Resources/ in git-sync. Emit unconditionally — a no-op when
        # Resources/ is empty, but the moment a project wheel ships
        # via pip_install_from_resources the file is mandatory.
        canonical_settings = canonical_bytes(
            "nb.Notebook/notebook-settings.json",
            _NOTEBOOK_SETTINGS_JSON.encode("utf-8"),
        )
        parts: dict[str, bytes | str] = {
            "notebook-content.py": canonical_content,
            "notebook-settings.json": canonical_settings,
        }
        kwargs: dict[str, object] = {
            "item_type": "Notebook",
            "display_name": display_name,
            "parts": parts,
            "description": description,
        }
        if logical_id is not None:
            kwargs["logical_id"] = logical_id
        return ArtifactBundle(**kwargs)  # type: ignore[arg-type]

    def save_to_disk(
        self,
        output_dir: str | Path,
        *,
        display_name: str,
        logical_id: str | None = None,
        description: str = "",
    ) -> Path:
        """Write the notebook bundle to ``output_dir`` with canonical bytes.

        Routes every file (``.platform`` and ``notebook-content.py``)
        through :func:`pyfabric.items.normalize.write_artifact_file`,
        so output is identical across Linux / macOS / Windows.

        Returns the artifact directory path
        (``output_dir / "{display_name}.Notebook"``).
        """
        bundle = self.to_bundle(
            display_name=display_name,
            logical_id=logical_id,
            description=description,
        )
        artifact_dir = Path(output_dir) / bundle.dir_name
        artifact_dir.mkdir(parents=True, exist_ok=True)

        # Route every file through write_artifact_file so line-ending
        # rules are applied regardless of OS default.
        write_artifact_file(artifact_dir / ".platform", bundle.platform_json())
        write_artifact_file(
            artifact_dir / "notebook-content.py", self.to_source_string()
        )
        write_artifact_file(
            artifact_dir / "notebook-settings.json", _NOTEBOOK_SETTINGS_JSON
        )
        log.info(
            "notebook_saved",
            display_name=display_name,
            path=str(artifact_dir),
            cells=len(self._cells),
        )
        return artifact_dir

    # ── Internals ────────────────────────────────────────────────────────────

    def _header_block(self) -> str:
        """Render the top-of-file METADATA block (kernel + dependencies)."""
        header: dict[str, object] = {
            "kernel_info": {"name": self.kernel},
        }
        deps = self._dependencies_block()
        if deps is not None:
            header["dependencies"] = deps

        body = json.dumps(header, indent=2)
        return self._meta_block(body)

    def _dependencies_block(self) -> dict[str, object] | None:
        if not self._lakehouses and self._environment is None:
            return None
        dep: dict[str, object] = {}

        if self._lakehouses:
            lakehouse: dict[str, object] = {}
            if self._default_lakehouse_idx is not None:
                default = self._lakehouses[self._default_lakehouse_idx]
                lakehouse["default_lakehouse"] = default.lh_id
                if default.lh_name is not None:
                    lakehouse["default_lakehouse_name"] = default.lh_name
                lakehouse["default_lakehouse_workspace_id"] = default.ws_id
            lakehouse["known_lakehouses"] = [
                {"id": lh.lh_id} for lh in self._lakehouses
            ]
            dep["lakehouse"] = lakehouse

        if self._environment is not None:
            dep["environment"] = {
                "environmentId": self._environment.env_id,
                "workspaceId": self._environment.ws_id,
            }

        return dep

    def _render_cell(self, cell: _Cell) -> str:
        if cell.kind == "markdown":
            return self._render_markdown_cell(cell.content)
        return self._render_code_cell(cell.content, cell.language or "python")

    @staticmethod
    def _render_markdown_cell(content: str) -> str:
        lines = content.split("\n")
        # Blank lines render as bare ``#`` (no trailing space) — matches
        # Fabric's round-tripped output.
        prefixed = ["#" if line == "" else f"# {line}" for line in lines]
        return "# MARKDOWN ********************\n\n" + "\n".join(prefixed) + "\n"

    def _render_code_cell(self, code: str, language: CellLanguage) -> str:
        meta_body = json.dumps(
            {
                "language": language,
                "language_group": _LANGUAGE_GROUP[language],
            },
            indent=2,
        )
        return (
            "# CELL ********************\n\n"
            + code.rstrip("\n")
            + "\n\n"
            + self._meta_block(meta_body)
        )

    @staticmethod
    def _meta_block(body: str) -> str:
        """Wrap a JSON body as a Fabric ``# METADATA`` block.

        Prefixes every line of the JSON with ``# META `` (or ``# META``
        for blank-ish lines — Fabric never emits those inside the
        indented JSON, so in practice every line gets the space).
        """
        lines = body.split("\n")
        prefixed = [f"# META {line}" for line in lines]
        return "# METADATA ********************\n\n" + "\n".join(prefixed) + "\n"
