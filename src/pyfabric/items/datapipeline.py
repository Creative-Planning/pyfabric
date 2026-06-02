"""Programmatic emission of Fabric ``pipeline-content.json`` files.

Hand-authoring a Data Pipeline definition flaps on the first git-sync because
Fabric re-serializes it to a canonical form that differs from the obvious one:

* an activity's ``typeProperties.notebookId`` is the referenced notebook's git
  **logicalId** (from that ``*.Notebook/.platform``), NOT its workspace object id;
* ``typeProperties.workspaceId`` is **zeroed** (``00000000-...``), resolved at
  runtime within the workspace;
* keys are emitted in a fixed order — activity ``type, typeProperties, policy,
  name, dependsOn`` and policy ``timeout, retry, retryIntervalInSeconds,
  secureInput, secureOutput``.

:class:`DataPipelineBuilder` centralizes those conventions so callers describe a
pipeline at a higher level (notebook activities + success dependencies) and trust
the output round-trips cleanly through git-sync. Bytes are written via
:func:`pyfabric.items.normalize.write_artifact_file` (LF, no trailing newline),
which requires the ``*.DataPipeline`` globs registered in
:data:`pyfabric.items.normalize.ARTIFACT_GLOBS`.

Usage::

    from pyfabric.items.datapipeline import DataPipelineBuilder

    pl = (
        DataPipelineBuilder(description="Daily refresh")
        # reference notebooks by their .Notebook dir — the logicalId is read
        # from each notebook's .platform automatically:
        .add_notebook_activity(
            "Extract", "definitions/nb_extract.Notebook", parameters={"path": ""}
        )
    )
    pl.add_notebook_activity(
        "Transform", "definitions/nb_transform.Notebook", depends_on=["Extract"]
    )
    pl.save_to_disk("definitions/", display_name="pl_daily")
"""

import json
import re
from pathlib import Path

import structlog

from pyfabric.items.bundle import ArtifactBundle
from pyfabric.items.normalize import canonical_bytes, write_artifact_file
from pyfabric.items.types import parse_platform

log = structlog.get_logger()

#: Activity type for a Fabric notebook (Trident) run.
NOTEBOOK_ACTIVITY_TYPE = "TridentNotebook"
#: Activity type for a Power BI / Fabric semantic model refresh.
SEMANTIC_MODEL_REFRESH_TYPE = "PBISemanticModelRefresh"

#: Same-workspace sentinel — Fabric zeroes ``workspaceId`` in git and resolves it
#: at runtime within the pipeline's own workspace.
_SAME_WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"

_DEFAULT_TIMEOUT = "0.12:00:00"
_GUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
# Fabric rejects activity names with any character outside this set — letters,
# numbers, dashes, underscores, spaces. (Parentheses, etc. fail with
# "Activity name should only contain letters, numbers, dashes (-), underscores
# (_), or spaces.") Validate at build time so the pipeline isn't rejected only
# after it reaches the portal.
_VALID_ACTIVITY_NAME_RE = re.compile(r"^[A-Za-z0-9_\- ]+$")

# Pipeline-relative path for canonical_bytes rule matching (LF, no trailing).
_PIPELINE_REL = "pl.DataPipeline/pipeline-content.json"


def notebook_logical_id(notebook: str | Path) -> str:
    """Resolve a notebook's git ``logicalId`` from its ``.platform``.

    ``notebook`` may be the ``*.Notebook`` directory, the ``.platform`` file
    itself, or a bare logicalId GUID (returned as-is). Raising means the
    ``.platform`` is missing or malformed — fail loud rather than emit a
    pipeline that references a non-existent notebook.
    """
    s = str(notebook)
    if _GUID_RE.fullmatch(s.strip()):
        return s.strip()
    path = Path(notebook)
    platform = path if path.name == ".platform" else path / ".platform"
    return parse_platform(platform.read_text(encoding="utf-8")).config.logical_id


def _param_type(value: object) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


class DataPipelineBuilder:
    """Build a Fabric ``pipeline-content.json`` programmatically.

    Construct, chain ``add_*`` calls to describe the activities, then call
    :meth:`to_pipeline_content`, :meth:`to_bundle`, or :meth:`save_to_disk`.

    Args:
        description: Optional pipeline description, emitted as
            ``properties.description`` in ``pipeline-content.json``.
    """

    def __init__(self, *, description: str = "") -> None:
        self.description = description
        self._activities: list[dict[str, object]] = []
        self._names: set[str] = set()

    # ── Activities ───────────────────────────────────────────────────────────

    def add_notebook_activity(
        self,
        name: str,
        notebook: str | Path,
        *,
        parameters: dict[str, object] | None = None,
        depends_on: list[str] | None = None,
        workspace_id: str | None = None,
        timeout: str = _DEFAULT_TIMEOUT,
        retry: int = 0,
        retry_interval_seconds: int = 30,
    ) -> str:
        """Append a notebook (``TridentNotebook``) activity.

        Args:
            name: Activity display name (unique within the pipeline).
            notebook: The referenced notebook — a ``*.Notebook`` directory, its
                ``.platform`` file, or a bare logicalId GUID. The activity's
                ``notebookId`` is resolved to the notebook's git **logicalId**.
            parameters: Notebook parameters; each value is wrapped as
                ``{"value": v, "type": <inferred>}`` (str/int/float/bool).
            depends_on: Names of activities that must succeed first.
            workspace_id: Override the (zeroed) same-workspace default.

        Returns the activity ``name`` (handy for chaining ``depends_on``).
        """
        type_props: dict[str, object] = {
            "notebookId": notebook_logical_id(notebook),
            "workspaceId": workspace_id or _SAME_WORKSPACE_ID,
        }
        if parameters:
            type_props["parameters"] = {
                k: {"value": v, "type": _param_type(v)} for k, v in parameters.items()
            }
        return self.add_activity(
            name,
            NOTEBOOK_ACTIVITY_TYPE,
            type_props,
            depends_on=depends_on,
            timeout=timeout,
            retry=retry,
            retry_interval_seconds=retry_interval_seconds,
        )

    def add_semantic_model_refresh(
        self,
        name: str,
        *,
        dataset_id: str,
        connection: str,
        workspace_id: str | None = None,
        depends_on: list[str] | None = None,
        commit_mode: str = "transactional",
        refresh_type: str = "Full",
        timeout: str = _DEFAULT_TIMEOUT,
        retry: int = 0,
        retry_interval_seconds: int = 30,
    ) -> str:
        """Append a ``PBISemanticModelRefresh`` activity.

        NOTE: ``connection`` MUST be a real provisioned Power BI connection id —
        a placeholder/zero GUID makes Fabric reject the whole pipeline on
        git-sync with ``RequestValidationFailed: User does not have access to the
        connection``. Provision the connection in the portal first (it is usually
        configured there), then pass its id here. ``dataset_id`` is the semantic
        model's id; ``workspace_id`` defaults to the (zeroed) same-workspace.
        """
        type_props: dict[str, object] = {
            "method": "POST",
            "groupId": workspace_id or _SAME_WORKSPACE_ID,
            "datasetId": dataset_id,
            "type": refresh_type,
            "commitMode": commit_mode,
            "waitOnCompletion": True,
            "operationType": "RefreshDataset",
        }
        return self.add_activity(
            name,
            SEMANTIC_MODEL_REFRESH_TYPE,
            type_props,
            depends_on=depends_on,
            timeout=timeout,
            retry=retry,
            retry_interval_seconds=retry_interval_seconds,
            external_references={"connection": connection},
        )

    def add_activity(
        self,
        name: str,
        activity_type: str,
        type_properties: dict[str, object],
        *,
        depends_on: list[str] | None = None,
        timeout: str = _DEFAULT_TIMEOUT,
        retry: int = 0,
        retry_interval_seconds: int = 30,
        external_references: dict[str, object] | None = None,
    ) -> str:
        """Append an activity of an arbitrary type (escape hatch).

        Emits keys in Fabric's canonical order: ``type, typeProperties, policy,
        [externalReferences,] name, dependsOn``. ``depends_on`` names must refer
        to already-added activities.
        """
        if not _VALID_ACTIVITY_NAME_RE.fullmatch(name):
            raise ValueError(
                f"invalid activity name {name!r}: Fabric allows only letters, "
                f"numbers, dashes (-), underscores (_), and spaces"
            )
        if name in self._names:
            raise ValueError(f"duplicate activity name: {name!r}")
        for dep in depends_on or []:
            if dep not in self._names:
                raise ValueError(
                    f"activity {name!r} depends on unknown activity {dep!r}"
                )
        activity: dict[str, object] = {
            "type": activity_type,
            "typeProperties": type_properties,
            "policy": {
                "timeout": timeout,
                "retry": retry,
                "retryIntervalInSeconds": retry_interval_seconds,
                "secureInput": False,
                "secureOutput": False,
            },
        }
        if external_references is not None:
            activity["externalReferences"] = external_references
        activity["name"] = name
        activity["dependsOn"] = [
            {"activity": dep, "dependencyConditions": ["Succeeded"]}
            for dep in (depends_on or [])
        ]
        self._activities.append(activity)
        self._names.add(name)
        return name

    # ── Emission ─────────────────────────────────────────────────────────────

    def to_pipeline_content(self) -> str:
        """Render ``pipeline-content.json`` as a string (2-space indent)."""
        properties: dict[str, object] = {}
        if self.description:
            properties["description"] = self.description
        properties["activities"] = self._activities
        return json.dumps({"properties": properties}, indent=2)

    def to_bundle(
        self,
        display_name: str,
        *,
        logical_id: str | None = None,
        description: str = "",
    ) -> ArtifactBundle:
        """Bundle the pipeline for disk save or REST upload.

        ``pipeline-content.json`` is stored as canonical bytes (LF, no trailing
        newline, no BOM) so :func:`pyfabric.items.bundle.save_to_disk` preserves
        LF on Windows. ``description`` is the item (.platform) description; the
        pipeline-content description is set on the builder.
        """
        content = canonical_bytes(
            _PIPELINE_REL, self.to_pipeline_content().encode("utf-8")
        )
        kwargs: dict[str, object] = {
            "item_type": "DataPipeline",
            "display_name": display_name,
            "parts": {"pipeline-content.json": content},
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
        """Write ``{display_name}.DataPipeline/`` with canonical bytes.

        Routes ``.platform`` and ``pipeline-content.json`` through
        :func:`pyfabric.items.normalize.write_artifact_file`. Returns the
        artifact directory path.
        """
        bundle = self.to_bundle(
            display_name, logical_id=logical_id, description=description
        )
        artifact_dir = Path(output_dir) / bundle.dir_name
        artifact_dir.mkdir(parents=True, exist_ok=True)
        write_artifact_file(artifact_dir / ".platform", bundle.platform_json())
        write_artifact_file(
            artifact_dir / "pipeline-content.json", self.to_pipeline_content()
        )
        log.info(
            "datapipeline_saved",
            display_name=display_name,
            path=str(artifact_dir),
            activities=len(self._activities),
        )
        return artifact_dir
