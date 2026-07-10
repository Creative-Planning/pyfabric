"""Tests for :mod:`pyfabric.deploy` — repo-to-workspace publish + orphan delete."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pyfabric.deploy import (
    PublishOrderError,
    PublishResult,
    UnpublishResult,
    publish_repo,
    substitute_parameters,
    unpublish_orphans,
)
from pyfabric.items.bundle import ArtifactBundle, save_to_disk


def _make_bundle(display_name: str, item_type: str = "Notebook") -> ArtifactBundle:
    """Minimal bundle with a single text part — enough for round-trip tests."""
    return ArtifactBundle(
        item_type=item_type,
        display_name=display_name,
        parts={"notebook-content.py": "# stub\n"},
    )


# ── _discover_artifacts (via publish_repo with mocked client) ──────────────


class TestDiscovery:
    def test_empty_repo_dir_returns_empty(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_paged.return_value = []
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == []

    def test_skips_directories_without_dot_itemtype_suffix(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "no_suffix").mkdir()
        (tmp_path / "no_suffix" / ".platform").write_text("{}")
        client = MagicMock()
        client.get_paged.return_value = []
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == []

    def test_skips_artifact_dir_without_platform_file(self, tmp_path: Path) -> None:
        (tmp_path / "nb_x.Notebook").mkdir()
        client = MagicMock()
        client.get_paged.return_value = []
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == []

    def test_item_types_in_scope_filters(self, tmp_path: Path) -> None:
        save_to_disk(_make_bundle("nb_x", "Notebook"), tmp_path)
        save_to_disk(_make_bundle("env_x", "Environment"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = []
        client.post.return_value = {"id": "new-id"}
        result = publish_repo(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"]
        )
        assert len(result) == 1
        assert result[0].display_name == "nb_x"
        assert result[0].item_type == "Notebook"


# ── publish_repo ──────────────────────────────────────────────────────────


class TestPublishRepo:
    def test_creates_when_no_existing_item(self, tmp_path: Path) -> None:
        save_to_disk(_make_bundle("nb_new"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = []
        client.post.return_value = {"id": "abc-123"}
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == [
            PublishResult(
                item_id="abc-123",
                display_name="nb_new",
                item_type="Notebook",
                action="created",
            )
        ]

    def test_updates_when_existing_item_with_same_name_and_type(
        self, tmp_path: Path
    ) -> None:
        save_to_disk(_make_bundle("nb_existing"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "existing-456", "displayName": "nb_existing", "type": "Notebook"}
        ]
        client.post.return_value = {}
        result = publish_repo(client, "ws-x", tmp_path)
        assert result == [
            PublishResult(
                item_id="existing-456",
                display_name="nb_existing",
                item_type="Notebook",
                action="updated",
            )
        ]

    def test_creates_when_existing_item_has_different_type(
        self, tmp_path: Path
    ) -> None:
        # Same displayName but different type — must create, not update.
        save_to_disk(_make_bundle("shared_name", "Notebook"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "env-id", "displayName": "shared_name", "type": "Environment"}
        ]
        client.post.return_value = {"id": "nb-id"}
        result = publish_repo(client, "ws-x", tmp_path)
        assert result[0].action == "created"
        assert result[0].item_id == "nb-id"


# ── unpublish_orphans ─────────────────────────────────────────────────────


class TestUnpublishOrphans:
    def test_deletes_workspace_items_not_in_repo(self, tmp_path: Path) -> None:
        save_to_disk(_make_bundle("nb_keep"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "keep-id", "displayName": "nb_keep", "type": "Notebook"},
            {"id": "orphan-id", "displayName": "nb_orphan", "type": "Notebook"},
        ]
        result = unpublish_orphans(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"]
        )
        assert result == [
            UnpublishResult(
                item_id="orphan-id",
                display_name="nb_orphan",
                item_type="Notebook",
            )
        ]
        client.delete.assert_called_once_with("workspaces/ws-x/items/orphan-id")

    def test_dry_run_does_not_call_delete(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "orphan-id", "displayName": "nb_orphan", "type": "Notebook"},
        ]
        result = unpublish_orphans(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"], dry_run=True
        )
        assert len(result) == 1
        client.delete.assert_not_called()

    def test_scope_excludes_other_types_from_deletion(self, tmp_path: Path) -> None:
        # Empty repo for Notebook scope — an Environment in workspace must
        # NOT be deleted because Environment is out of scope.
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "env-id", "displayName": "env_x", "type": "Environment"},
        ]
        result = unpublish_orphans(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"]
        )
        assert result == []
        client.delete.assert_not_called()

    def test_no_scope_considers_all_types(self, tmp_path: Path) -> None:
        # Empty repo, no scope → every workspace item is an orphan.
        # This is the documented footgun; the test guards the behavior.
        client = MagicMock()
        client.get_paged.return_value = [
            {"id": "nb-id", "displayName": "nb_x", "type": "Notebook"},
            {"id": "env-id", "displayName": "env_x", "type": "Environment"},
        ]
        result = unpublish_orphans(client, "ws-x", tmp_path)
        assert len(result) == 2
        assert {r.item_type for r in result} == {"Notebook", "Environment"}


# ── Integration via existing pyfabric primitives ──────────────────────────


class TestPublishAfterBuilderSaveToDisk:
    """publish_repo composes correctly with the builder-emitted layout."""

    def test_picks_up_notebook_saved_via_save_to_disk(self, tmp_path: Path) -> None:
        from pyfabric.items.notebook import NotebookBuilder

        nb = NotebookBuilder().add_python('print("hi")')
        nb.save_to_disk(tmp_path, display_name="nb_builder")

        client = MagicMock()
        client.get_paged.return_value = []
        client.post.return_value = {"id": "builder-id"}
        result = publish_repo(
            client, "ws-x", tmp_path, item_types_in_scope=["Notebook"]
        )
        assert len(result) == 1
        assert result[0].display_name == "nb_builder"
        assert result[0].action == "created"


# ── Edge cases ────────────────────────────────────────────────────────────


class TestEdgeCases:
    def test_passing_str_path_for_repo_dir_works(self, tmp_path: Path) -> None:
        save_to_disk(_make_bundle("nb_str"), tmp_path)
        client = MagicMock()
        client.get_paged.return_value = []
        client.post.return_value = {"id": "x"}
        result = publish_repo(client, "ws-x", str(tmp_path))
        assert len(result) == 1

    def test_unpublish_orphans_with_str_path(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_paged.return_value = []
        result = unpublish_orphans(client, "ws-x", str(tmp_path))
        assert result == []


# ── Dependency ordering (issue #98) ─────────────────────────────────────────


def _make_item(
    base: Path,
    display_name: str,
    item_type: str,
    *,
    logical_id: str | None = None,
    files: dict[str, str] | None = None,
) -> Path:
    """Write a minimal artifact dir: .platform + optional definition files."""
    item_dir = base / f"{display_name}.{item_type}"
    item_dir.mkdir(parents=True, exist_ok=True)
    platform = {
        "metadata": {"type": item_type, "displayName": display_name},
        "config": {"version": "2.0", "logicalId": logical_id or str(uuid.uuid4())},
    }
    (item_dir / ".platform").write_text(json.dumps(platform), encoding="utf-8")
    for rel, content in (files or {}).items():
        p = item_dir / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    return item_dir


def _pbir(model_dir_name: str) -> str:
    return json.dumps(
        {"datasetReference": {"byPath": {"path": f"../{model_dir_name}"}}}
    )


def _pipeline_content(*notebook_ids: str, nested: bool = False) -> str:
    activities: list[dict[str, object]] = [
        {
            "type": "TridentNotebook",
            "typeProperties": {"notebookId": nb_id},
            "name": f"a{i}",
        }
        for i, nb_id in enumerate(notebook_ids)
    ]
    if nested:
        activities = [
            {
                "type": "ForEach",
                "typeProperties": {"activities": activities},
                "name": "loop",
            }
        ]
    return json.dumps({"properties": {"activities": activities}})


def _publish_order(client: MagicMock, tmp_path: Path, **kwargs: object) -> list[str]:
    client.get_paged.return_value = []
    client.post.return_value = {"id": "x"}
    results = publish_repo(client, "ws-x", tmp_path, **kwargs)  # type: ignore[arg-type]
    return [r.display_name for r in results]


class TestDependencyOrder:
    def test_report_publishes_after_its_semantic_model(self, tmp_path: Path) -> None:
        # 'a_report' sorts before 'z_model' in directory order — the edge
        # must override that.
        _make_item(
            tmp_path, "z_model", "SemanticModel", files={"definition.pbism": "{}"}
        )
        _make_item(
            tmp_path,
            "a_report",
            "Report",
            files={"definition.pbir": _pbir("z_model.SemanticModel")},
        )
        order = _publish_order(MagicMock(), tmp_path)
        assert order.index("z_model") < order.index("a_report")

    def test_pipeline_publishes_after_referenced_notebook(self, tmp_path: Path) -> None:
        nb_id = str(uuid.uuid4())
        _make_item(
            tmp_path,
            "z_notebook",
            "Notebook",
            logical_id=nb_id,
            files={"notebook-content.py": "# stub\n"},
        )
        _make_item(
            tmp_path,
            "a_pipeline",
            "DataPipeline",
            files={"pipeline-content.json": _pipeline_content(nb_id)},
        )
        order = _publish_order(MagicMock(), tmp_path)
        assert order.index("z_notebook") < order.index("a_pipeline")

    def test_nested_container_notebook_ids_found(self, tmp_path: Path) -> None:
        nb_id = str(uuid.uuid4())
        _make_item(
            tmp_path,
            "z_notebook",
            "Notebook",
            logical_id=nb_id,
            files={"notebook-content.py": "# stub\n"},
        )
        _make_item(
            tmp_path,
            "a_pipeline",
            "DataPipeline",
            files={"pipeline-content.json": _pipeline_content(nb_id, nested=True)},
        )
        order = _publish_order(MagicMock(), tmp_path)
        assert order.index("z_notebook") < order.index("a_pipeline")

    def test_external_notebook_id_is_ignored(self, tmp_path: Path) -> None:
        _make_item(
            tmp_path,
            "pl_solo",
            "DataPipeline",
            files={"pipeline-content.json": _pipeline_content(str(uuid.uuid4()))},
        )
        assert _publish_order(MagicMock(), tmp_path) == ["pl_solo"]

    def test_cycle_raises_naming_members(self, tmp_path: Path) -> None:
        _make_item(
            tmp_path,
            "rpt_a",
            "Report",
            files={"definition.pbir": _pbir("rpt_b.Report")},
        )
        _make_item(
            tmp_path,
            "rpt_b",
            "Report",
            files={"definition.pbir": _pbir("rpt_a.Report")},
        )
        client = MagicMock()
        client.get_paged.return_value = []
        with pytest.raises(PublishOrderError, match=r"rpt_a\.Report.*rpt_b\.Report"):
            publish_repo(client, "ws-x", tmp_path)

    def test_scope_filter_drops_edges_to_filtered_artifacts(
        self, tmp_path: Path
    ) -> None:
        _make_item(tmp_path, "sm_x", "SemanticModel", files={"definition.pbism": "{}"})
        _make_item(
            tmp_path,
            "rpt_x",
            "Report",
            files={"definition.pbir": _pbir("sm_x.SemanticModel")},
        )
        order = _publish_order(MagicMock(), tmp_path, item_types_in_scope=["Report"])
        assert order == ["rpt_x"]

    def test_tier_ordering_without_edges(self, tmp_path: Path) -> None:
        _make_item(
            tmp_path,
            "z_env",
            "Environment",
            files={"Setting/Sparkcompute.yml": "x: 1\n"},
        )
        _make_item(
            tmp_path, "m_notebook", "Notebook", files={"notebook-content.py": "#\n"}
        )
        _make_item(tmp_path, "a_report", "Report", files={"definition.pbir": "{}"})
        order = _publish_order(MagicMock(), tmp_path)
        assert order == ["z_env", "m_notebook", "a_report"]

    def test_order_is_deterministic(self, tmp_path: Path) -> None:
        nb_id = str(uuid.uuid4())
        _make_item(
            tmp_path,
            "nb_1",
            "Notebook",
            logical_id=nb_id,
            files={"notebook-content.py": "#\n"},
        )
        _make_item(tmp_path, "nb_2", "Notebook", files={"notebook-content.py": "#\n"})
        _make_item(tmp_path, "sm_1", "SemanticModel", files={"definition.pbism": "{}"})
        _make_item(
            tmp_path,
            "pl_1",
            "DataPipeline",
            files={"pipeline-content.json": _pipeline_content(nb_id)},
        )
        first = _publish_order(MagicMock(), tmp_path)
        second = _publish_order(MagicMock(), tmp_path)
        assert first == second
        assert first.index("nb_1") < first.index("pl_1")


# ── substitute_parameters (issue #97) ────────────────────────────────────────


_PARAM_YML = """\
find_replace:
  - find_value: "11111111-1111-1111-1111-111111111111"
    replace_value:
      DEV: "22222222-2222-2222-2222-222222222222"
      PROD: "33333333-3333-3333-3333-333333333333"
  - find_value: "<CONN_STRING>"
    replace_value:
      DEV: "dev-endpoint.example"
      PROD: "prod-endpoint.example"
"""


def _staged_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    _make_item(
        repo,
        "nb_x",
        "Notebook",
        files={
            "notebook-content.py": (
                "# lakehouse: 11111111-1111-1111-1111-111111111111\n"
                "# server: <CONN_STRING>\n"
            )
        },
    )
    yml = tmp_path / "parameter.yml"
    yml.write_text(_PARAM_YML, encoding="utf-8")
    return repo, yml


class TestSubstituteParameters:
    def test_substitutes_for_selected_environment(self, tmp_path: Path) -> None:
        repo, yml = _staged_repo(tmp_path)
        out = substitute_parameters(repo, yml, environment="PROD")
        content = (out / "nb_x.Notebook" / "notebook-content.py").read_text("utf-8")
        assert "33333333-3333-3333-3333-333333333333" in content
        assert "prod-endpoint.example" in content
        assert "11111111" not in content

    def test_environment_selection_differs(self, tmp_path: Path) -> None:
        repo, yml = _staged_repo(tmp_path)
        dev = substitute_parameters(repo, yml, environment="DEV")
        content = (dev / "nb_x.Notebook" / "notebook-content.py").read_text("utf-8")
        assert "22222222-2222-2222-2222-222222222222" in content

    def test_source_repo_untouched(self, tmp_path: Path) -> None:
        repo, yml = _staged_repo(tmp_path)
        before = (repo / "nb_x.Notebook" / "notebook-content.py").read_bytes()
        substitute_parameters(repo, yml, environment="PROD")
        assert (repo / "nb_x.Notebook" / "notebook-content.py").read_bytes() == before

    def test_output_dir_honored(self, tmp_path: Path) -> None:
        repo, yml = _staged_repo(tmp_path)
        dest = tmp_path / "staged"
        out = substitute_parameters(repo, yml, environment="DEV", output_dir=dest)
        assert out == dest
        assert (dest / "nb_x.Notebook" / ".platform").exists()

    def test_binary_files_copied_untouched(self, tmp_path: Path) -> None:
        repo, yml = _staged_repo(tmp_path)
        wheel = repo / "nb_x.Notebook" / "Resources" / "builtin" / "pkg.whl"
        wheel.parent.mkdir(parents=True)
        payload = bytes([0, 255, 254, 1]) + b"11111111-1111-1111-1111-111111111111"
        wheel.write_bytes(payload)
        out = substitute_parameters(repo, yml, environment="PROD")
        copied = out / "nb_x.Notebook" / "Resources" / "builtin" / "pkg.whl"
        assert copied.read_bytes() == payload

    def test_missing_environment_names_variable_and_options(
        self, tmp_path: Path
    ) -> None:
        repo, yml = _staged_repo(tmp_path)
        with pytest.raises(ValueError, match=r"'PPE'.*DEV, PROD"):
            substitute_parameters(repo, yml, environment="PPE")

    def test_missing_yml_raises(self, tmp_path: Path) -> None:
        repo, _ = _staged_repo(tmp_path)
        with pytest.raises(FileNotFoundError, match="parameter file not found"):
            substitute_parameters(repo, tmp_path / "nope.yml", environment="DEV")

    def test_malformed_yml_raises(self, tmp_path: Path) -> None:
        repo, yml = _staged_repo(tmp_path)
        yml.write_text("something_else: []\n", encoding="utf-8")
        with pytest.raises(ValueError, match="find_replace"):
            substitute_parameters(repo, yml, environment="DEV")

    def test_entry_without_replace_value_raises(self, tmp_path: Path) -> None:
        repo, yml = _staged_repo(tmp_path)
        yml.write_text('find_replace:\n  - find_value: "x"\n', encoding="utf-8")
        with pytest.raises(ValueError, match="replace_value"):
            substitute_parameters(repo, yml, environment="DEV")

    def test_composes_with_publish_repo(self, tmp_path: Path) -> None:
        repo, yml = _staged_repo(tmp_path)
        out = substitute_parameters(repo, yml, environment="PROD")
        client = MagicMock()
        client.get_paged.return_value = []
        client.post.return_value = {"id": "x"}
        results = publish_repo(client, "ws-x", out)
        assert [r.display_name for r in results] == ["nb_x"]

    def test_import_error_names_the_extra(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sys

        repo, yml = _staged_repo(tmp_path)
        monkeypatch.setitem(sys.modules, "yaml", None)
        with pytest.raises(ImportError, match=r"pyfabric\[deploy\]"):
            substitute_parameters(repo, yml, environment="DEV")
