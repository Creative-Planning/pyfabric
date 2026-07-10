"""Tests for :mod:`pyfabric.items.datapipeline` (DataPipelineBuilder).

The correctness bar is **byte-equality with Fabric's canonical
``pipeline-content.json``**. Fabric re-serializes the pipeline on first
git-sync to a fixed shape — git logicalId for ``notebookId``, zeroed
``workspaceId``, and a specific activity/policy key order — so a builder that
gets any of that wrong produces a file that flaps on every sync.

All ids in the fixtures are synthetic (``00000000-...``) — no client
identifiers leak in.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pyfabric.items.datapipeline import (
    DataPipelineBuilder,
    PipelineParameter,
    notebook_logical_id,
)
from pyfabric.items.normalize import canonical_bytes
from pyfabric.items.types import parse_platform

FIXTURES = Path(__file__).parent.parent / "fixtures" / "datapipelines"
_PIPELINE_REL = "pl.DataPipeline/pipeline-content.json"

_NB_A = "00000000-0000-0000-0000-00000000000a"
_NB_B = "00000000-0000-0000-0000-00000000000b"
_ZERO_WS = "00000000-0000-0000-0000-000000000000"


def _build_sample() -> DataPipelineBuilder:
    b = DataPipelineBuilder(description="Example two-step pipeline.")
    b.add_notebook_activity("Extract", _NB_A, parameters={"path": ""})
    b.add_notebook_activity("Transform", _NB_B, depends_on=["Extract"])
    return b


def _make_notebook_dir(tmp_path: Path, name: str, logical_id: str) -> Path:
    nb = tmp_path / f"{name}.Notebook"
    nb.mkdir()
    (nb / ".platform").write_text(
        json.dumps(
            {
                "$schema": "https://example/schema.json",
                "metadata": {"type": "Notebook", "displayName": name},
                "config": {"version": "2.0", "logicalId": logical_id},
            }
        ),
        encoding="utf-8",
    )
    return nb


# ── Byte-equality with Fabric's canonical form ───────────────────────────────


class TestByteEquality:
    def test_matches_fabric_canonical_fixture(self, tmp_path: Path):
        out = _build_sample().save_to_disk(tmp_path, display_name="pl_sample")
        got = (out / "pipeline-content.json").read_bytes()
        # Normalize the fixture the same way (strips any trailing newline an
        # editor added) so the comparison is about content, not the fixture's
        # on-disk trailing byte.
        expected = canonical_bytes(
            _PIPELINE_REL,
            (
                FIXTURES / "pl_sample.DataPipeline" / "pipeline-content.json"
            ).read_bytes(),
        )
        assert got == expected

    def test_bytes_are_lf_no_trailing_no_bom(self, tmp_path: Path):
        out = _build_sample().save_to_disk(tmp_path, display_name="pl_sample")
        raw = (out / "pipeline-content.json").read_bytes()
        assert b"\r\n" not in raw
        assert not raw.endswith(b"\n")
        assert not raw.startswith(b"\xef\xbb\xbf")


# ── Canonical conventions ────────────────────────────────────────────────────


class TestConventions:
    def test_notebookid_resolves_to_logicalid_from_platform(self, tmp_path: Path):
        nb = _make_notebook_dir(
            tmp_path, "nb_x", "11111111-1111-1111-1111-111111111111"
        )
        b = DataPipelineBuilder()
        b.add_notebook_activity("A", nb)
        act = json.loads(b.to_pipeline_content())["properties"]["activities"][0]
        assert (
            act["typeProperties"]["notebookId"]
            == "11111111-1111-1111-1111-111111111111"
        )

    def test_notebook_logical_id_helper_accepts_dir_file_or_guid(self, tmp_path: Path):
        nb = _make_notebook_dir(
            tmp_path, "nb_y", "22222222-2222-2222-2222-222222222222"
        )
        assert notebook_logical_id(nb) == "22222222-2222-2222-2222-222222222222"
        assert (
            notebook_logical_id(nb / ".platform")
            == "22222222-2222-2222-2222-222222222222"
        )
        assert notebook_logical_id(_NB_A) == _NB_A  # bare GUID passes through

    def test_workspaceid_zeroed_by_default(self):
        b = DataPipelineBuilder()
        b.add_notebook_activity("A", _NB_A)
        act = json.loads(b.to_pipeline_content())["properties"]["activities"][0]
        assert act["typeProperties"]["workspaceId"] == _ZERO_WS

    def test_activity_key_order(self):
        b = DataPipelineBuilder()
        b.add_notebook_activity("A", _NB_A)
        act = json.loads(b.to_pipeline_content())["properties"]["activities"][0]
        assert list(act.keys()) == [
            "type",
            "typeProperties",
            "policy",
            "name",
            "dependsOn",
        ]
        assert list(act["policy"].keys()) == [
            "timeout",
            "retry",
            "retryIntervalInSeconds",
            "secureInput",
            "secureOutput",
        ]

    def test_depends_on_emits_succeeded_chain(self):
        b = _build_sample()
        acts = json.loads(b.to_pipeline_content())["properties"]["activities"]
        assert acts[0]["dependsOn"] == []
        assert acts[1]["dependsOn"] == [
            {"activity": "Extract", "dependencyConditions": ["Succeeded"]}
        ]

    def test_unknown_dependency_raises(self):
        b = DataPipelineBuilder()
        with pytest.raises(ValueError, match="unknown activity"):
            b.add_notebook_activity("A", _NB_A, depends_on=["Nope"])

    def test_duplicate_name_raises(self):
        b = DataPipelineBuilder()
        b.add_notebook_activity("A", _NB_A)
        with pytest.raises(ValueError, match="duplicate activity name"):
            b.add_notebook_activity("A", _NB_B)

    @pytest.mark.parametrize(
        "bad_name", ["Extract (Bronze)", "a/b", "a.b", "x@y", "a,b"]
    )
    def test_invalid_activity_name_raises(self, bad_name):
        # Fabric rejects names with anything outside [A-Za-z0-9_- ].
        b = DataPipelineBuilder()
        with pytest.raises(ValueError, match="invalid activity name"):
            b.add_notebook_activity(bad_name, _NB_A)

    @pytest.mark.parametrize(
        "ok_name",
        ["Extract Projections Bronze", "bronze_to_silver", "Silver-to-Gold", "Step 1"],
    )
    def test_valid_activity_names_accepted(self, ok_name):
        b = DataPipelineBuilder()
        b.add_notebook_activity(ok_name, _NB_A)  # no raise

    def test_parameter_type_inference(self):
        b = DataPipelineBuilder()
        b.add_notebook_activity(
            "A", _NB_A, parameters={"s": "x", "n": 3, "f": 1.5, "flag": True}
        )
        params = json.loads(b.to_pipeline_content())["properties"]["activities"][0][
            "typeProperties"
        ]["parameters"]
        assert params["s"] == {"value": "x", "type": "string"}
        assert params["n"] == {"value": 3, "type": "int"}
        assert params["f"] == {"value": 1.5, "type": "float"}
        # bool before int — order matters in _param_type
        assert params["flag"] == {"value": True, "type": "bool"}


# ── Semantic model refresh ───────────────────────────────────────────────────


class TestSemanticModelRefresh:
    def test_includes_connection_and_dataset(self):
        b = DataPipelineBuilder()
        b.add_notebook_activity("Gold", _NB_A)
        b.add_semantic_model_refresh(
            "Refresh",
            dataset_id="33333333-3333-3333-3333-333333333333",
            connection="44444444-4444-4444-4444-444444444444",
            depends_on=["Gold"],
        )
        act = json.loads(b.to_pipeline_content())["properties"]["activities"][1]
        assert act["type"] == "PBISemanticModelRefresh"
        assert (
            act["typeProperties"]["datasetId"] == "33333333-3333-3333-3333-333333333333"
        )
        assert act["externalReferences"] == {
            "connection": "44444444-4444-4444-4444-444444444444"
        }
        assert act["dependsOn"][0]["activity"] == "Gold"


# ── save_to_disk / .platform ─────────────────────────────────────────────────


class TestSaveToDisk:
    def test_platform_declares_datapipeline(self, tmp_path: Path):
        out = _build_sample().save_to_disk(
            tmp_path,
            display_name="pl_sample",
            logical_id="55555555-5555-5555-5555-555555555555",
        )
        assert out.name == "pl_sample.DataPipeline"
        pf = parse_platform((out / ".platform").read_text(encoding="utf-8"))
        assert pf.metadata.type == "DataPipeline"
        assert pf.metadata.display_name == "pl_sample"
        assert pf.config.logical_id == "55555555-5555-5555-5555-555555555555"


class TestPipelineParameters:
    """Issue #102: pipeline-level parameters + Expression-bound activity params."""

    def test_declares_parameters_block_after_activities(self) -> None:
        b = DataPipelineBuilder()
        b.add_pipeline_parameter("pdf_path", default_value="")
        b.add_notebook_activity("Extract", _NB_A)
        props = json.loads(b.to_pipeline_content())["properties"]
        assert list(props.keys()) == ["activities", "parameters"]
        assert props["parameters"] == {
            "pdf_path": {"type": "string", "defaultValue": ""}
        }

    def test_description_stays_first_in_key_order(self) -> None:
        b = DataPipelineBuilder(description="Parameterized.")
        b.add_pipeline_parameter("n", type="int", default_value=1)
        props = json.loads(b.to_pipeline_content())["properties"]
        assert list(props.keys()) == ["description", "activities", "parameters"]

    def test_no_parameters_block_when_none_declared(self) -> None:
        b = _build_sample()
        assert "parameters" not in json.loads(b.to_pipeline_content())["properties"]

    def test_expression_bound_activity_parameter(self) -> None:
        b = DataPipelineBuilder()
        ref = b.add_pipeline_parameter("pdf_path", default_value="")
        b.add_notebook_activity("Extract", _NB_A, parameters={"pdf_path": ref, "n": 5})
        params = json.loads(b.to_pipeline_content())["properties"]["activities"][0][
            "typeProperties"
        ]["parameters"]
        assert params["pdf_path"] == {
            "value": {
                "value": "@pipeline().parameters.pdf_path",
                "type": "Expression",
            },
            "type": "string",
        }
        # Plain values keep the literal shape (back-compat).
        assert params["n"] == {"value": 5, "type": "int"}

    def test_expression_type_comes_from_declaration(self) -> None:
        b = DataPipelineBuilder()
        ref = b.add_pipeline_parameter("retries", type="int", default_value=3)
        b.add_notebook_activity("Extract", _NB_A, parameters={"r": ref})
        params = json.loads(b.to_pipeline_content())["properties"]["activities"][0][
            "typeProperties"
        ]["parameters"]
        assert params["r"]["type"] == "int"

    def test_direct_reference_construction_works(self) -> None:
        b = DataPipelineBuilder()
        b.add_pipeline_parameter("pdf_path")
        b.add_notebook_activity(
            "Extract", _NB_A, parameters={"p": PipelineParameter("pdf_path")}
        )  # no raise

    def test_capitalized_type_rejected_with_silent_drop_explanation(self) -> None:
        b = DataPipelineBuilder()
        with pytest.raises(ValueError, match="silently drops"):
            b.add_pipeline_parameter("pdf_path", type="String")

    def test_securestring_case_enforced(self) -> None:
        b = DataPipelineBuilder()
        with pytest.raises(ValueError, match="secureString"):
            b.add_pipeline_parameter("secret", type="securestring")

    def test_unknown_type_rejected_listing_valid_set(self) -> None:
        b = DataPipelineBuilder()
        with pytest.raises(ValueError, match="valid types"):
            b.add_pipeline_parameter("x", type="guid")

    def test_duplicate_parameter_name_rejected(self) -> None:
        b = DataPipelineBuilder()
        b.add_pipeline_parameter("x")
        with pytest.raises(ValueError, match="duplicate pipeline parameter"):
            b.add_pipeline_parameter("x")

    def test_undeclared_reference_rejected(self) -> None:
        b = DataPipelineBuilder()
        with pytest.raises(ValueError, match="undeclared pipeline parameter"):
            b.add_notebook_activity(
                "Extract", _NB_A, parameters={"p": PipelineParameter("nope")}
            )

    def test_default_value_omitted_when_none(self) -> None:
        b = DataPipelineBuilder()
        b.add_pipeline_parameter("flag", type="bool")
        props = json.loads(b.to_pipeline_content())["properties"]
        assert props["parameters"] == {"flag": {"type": "bool"}}
