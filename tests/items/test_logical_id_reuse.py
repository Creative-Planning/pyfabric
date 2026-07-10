"""Cross-builder tests for logicalId identity stability on rebuild.

Fabric git-sync keys deployed items by ``.platform/logicalId``. Re-running
a build script over an existing artifact directory must not re-identify
the item — ``save_to_disk`` reuses the on-disk logicalId when the caller
didn't pin one. See issue #130.
"""

import json
import uuid
from pathlib import Path

from pyfabric.items.bundle import ArtifactBundle
from pyfabric.items.bundle import save_to_disk as bundle_save_to_disk
from pyfabric.items.datapipeline import DataPipelineBuilder
from pyfabric.items.environment import EnvironmentBuilder
from pyfabric.items.mirrored_database import MirroredDatabaseBuilder
from pyfabric.items.notebook import NotebookBuilder
from pyfabric.items.report import Page, Report
from pyfabric.items.semantic_model import (
    Column,
    LakehouseSource,
    SemanticModel,
    Table,
)


def _platform_logical_id(item_dir: Path) -> str:
    data = json.loads((item_dir / ".platform").read_text(encoding="utf-8"))
    return data["config"]["logicalId"]


def _minimal_model() -> SemanticModel:
    source = LakehouseSource(name="Gold", workspace_id="ws-1", lakehouse_id="lh-1")
    return SemanticModel(
        name="sm_reuse",
        description="Identity-reuse test model.",
        sources=[source],
        tables=[
            Table(
                name="dim_x",
                source=source,
                description="Test dim.",
                columns=[Column("x_key", "string", description="PK.")],
            )
        ],
    )


def _minimal_report() -> Report:
    return Report(
        name="rpt_reuse",
        semantic_model_path="../sm_reuse.SemanticModel",
        pages=[Page(name="p1", display_name="Page 1")],
        description="Identity-reuse test report.",
    )


class TestDoubleSaveKeepsIdentity:
    """Second save into the same directory must not change .platform."""

    def test_notebook_builder(self, tmp_path: Path) -> None:
        first = NotebookBuilder().add_python("pass")
        d1 = first.save_to_disk(tmp_path, display_name="nb_reuse")
        original = (d1 / ".platform").read_bytes()
        # A NEW builder instance (fresh script run) must adopt the id.
        second = NotebookBuilder().add_python("pass")
        d2 = second.save_to_disk(tmp_path, display_name="nb_reuse")
        assert (d2 / ".platform").read_bytes() == original

    def test_datapipeline_builder(self, tmp_path: Path) -> None:
        d1 = DataPipelineBuilder().save_to_disk(tmp_path, display_name="pl_reuse")
        original = (d1 / ".platform").read_bytes()
        d2 = DataPipelineBuilder().save_to_disk(tmp_path, display_name="pl_reuse")
        assert (d2 / ".platform").read_bytes() == original

    def test_environment_builder(self, tmp_path: Path) -> None:
        d1 = EnvironmentBuilder().save_to_disk(tmp_path, display_name="env_reuse")
        original = (d1 / ".platform").read_bytes()
        d2 = EnvironmentBuilder().save_to_disk(tmp_path, display_name="env_reuse")
        assert (d2 / ".platform").read_bytes() == original

    def test_mirrored_database_builder(self, tmp_path: Path) -> None:
        d1 = MirroredDatabaseBuilder().save_to_disk(tmp_path, display_name="mdb_reuse")
        original = (d1 / ".platform").read_bytes()
        d2 = MirroredDatabaseBuilder().save_to_disk(tmp_path, display_name="mdb_reuse")
        assert (d2 / ".platform").read_bytes() == original

    def test_semantic_model(self, tmp_path: Path) -> None:
        d1 = _minimal_model().save_to_disk(tmp_path)
        original = (d1 / ".platform").read_bytes()
        d2 = _minimal_model().save_to_disk(tmp_path)
        assert (d2 / ".platform").read_bytes() == original

    def test_report(self, tmp_path: Path) -> None:
        d1 = _minimal_report().save_to_disk(tmp_path)
        original = (d1 / ".platform").read_bytes()
        d2 = _minimal_report().save_to_disk(tmp_path)
        assert (d2 / ".platform").read_bytes() == original

    def test_module_level_bundle_save(self, tmp_path: Path) -> None:
        d1 = bundle_save_to_disk(
            ArtifactBundle(
                item_type="Notebook",
                display_name="nb_bundle_reuse",
                parts={"notebook-content.py": "# stub\n"},
            ),
            tmp_path,
        )
        original = (d1 / ".platform").read_bytes()
        d2 = bundle_save_to_disk(
            ArtifactBundle(
                item_type="Notebook",
                display_name="nb_bundle_reuse",
                parts={"notebook-content.py": "# stub\n"},
            ),
            tmp_path,
        )
        assert (d2 / ".platform").read_bytes() == original


class TestExplicitIdPrecedence:
    def test_explicit_id_overrides_existing_platform(self, tmp_path: Path) -> None:
        NotebookBuilder().add_python("pass").save_to_disk(
            tmp_path, display_name="nb_pin"
        )
        pinned = str(uuid.uuid4())
        d = (
            NotebookBuilder()
            .add_python("pass")
            .save_to_disk(tmp_path, display_name="nb_pin", logical_id=pinned)
        )
        assert _platform_logical_id(d) == pinned

    def test_semantic_model_explicit_id_survives(self, tmp_path: Path) -> None:
        pinned = str(uuid.uuid4())
        model = _minimal_model()
        model.logical_id = pinned
        d = model.save_to_disk(tmp_path)
        assert _platform_logical_id(d) == pinned

    def test_instance_id_stable_across_two_dirs(self, tmp_path: Path) -> None:
        # One instance saved to two fresh dirs keeps the id it minted first
        # (the resolver treats the now-set instance id as explicit).
        model = _minimal_model()
        d1 = model.save_to_disk(tmp_path / "a")
        d2 = model.save_to_disk(tmp_path / "b")
        assert _platform_logical_id(d1) == _platform_logical_id(d2)


class TestRestUploadPathStillMintsIds:
    def test_platform_json_memoizes_fresh_uuid(self) -> None:
        bundle = ArtifactBundle(item_type="Notebook", display_name="nb_rest", parts={})
        first = bundle.platform_json()
        assert bundle.logical_id is not None
        uuid.UUID(bundle.logical_id)
        assert bundle.platform_json() == first
