"""Byte-fidelity to the form Fabric itself writes back.

Emitting *semantically* correct TMDL is not enough. When Fabric round-trips a
model on a portal edit it rewrites the files into its own canonical form, so any
place the builder disagrees becomes a diff on every sync — the generator writes
one shape, Fabric writes the other, forever. That flapping is what these tests
pin down.

Each assertion below corresponds to something Fabric was observed changing on a
real model that had been emitted by this builder.
"""

import json
from pathlib import Path

import pytest

from pyfabric.items.semantic_model import (
    Column,
    LakehouseSource,
    SemanticModel,
    StaticSource,
    Table,
    _ordered_table_names,
)


@pytest.fixture
def gold() -> LakehouseSource:
    return LakehouseSource(name="Gold", workspace_id="ws-1", lakehouse_id="lh-1")


def _model(gold: LakehouseSource, **kw) -> SemanticModel:
    return SemanticModel(
        name="sm_canon",
        sources=[gold],
        tables=[
            Table(
                name="dim_thing",
                source=gold,
                description="Dim.",
                columns=[
                    Column(
                        name="thing_id",
                        data_type="string",
                        is_key=True,
                        description="Key.",
                    ),
                    Column(
                        name="amount",
                        data_type="int64",
                        is_hidden=True,
                        format_string="#,0",
                    ),
                    Column(
                        name="link",
                        data_type="string",
                        data_category="WebUrl",
                        description="Link.",
                    ),
                ],
            )
        ],
        description="Canonical form.",
        **kw,
    )


def _lines(sm: SemanticModel, tmp_path: Path, rel: str) -> list[str]:
    item = sm.save_to_disk(tmp_path)
    return (item / rel).read_text("utf-8").splitlines()


# ── Column property order ───────────────────────────────────────────────────


def test_is_key_and_is_hidden_follow_data_type(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    """Fabric puts isKey/isHidden immediately after dataType, not last."""
    lines = _lines(_model(gold), tmp_path, "definition/tables/dim_thing.tmdl")

    i = lines.index("\tcolumn thing_id")
    assert lines[i + 1].strip() == "dataType: string"
    assert lines[i + 2].strip() == "isKey"

    j = lines.index("\tcolumn amount")
    assert lines[j + 1].strip() == "dataType: int64"
    assert lines[j + 2].strip() == "isHidden"
    # formatString comes after the flags, before lineageTag
    assert lines[j + 3].strip() == "formatString: #,0"
    assert lines[j + 4].strip().startswith("lineageTag:")


def test_blank_line_precedes_column_annotations(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    lines = _lines(_model(gold), tmp_path, "definition/tables/dim_thing.tmdl")
    ann = lines.index("\t\tannotation SummarizationSetBy = Automatic")
    assert lines[ann - 1] == "", "Fabric separates the annotation with a blank line"


def test_data_category_still_follows_lineage_tag(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    lines = _lines(_model(gold), tmp_path, "definition/tables/dim_thing.tmdl")
    k = lines.index("\tcolumn link")
    order = [ln.strip().split(":")[0] for ln in lines[k + 1 : k + 6]]
    assert order[:4] == ["dataType", "lineageTag", "dataCategory", "summarizeBy"]


# ── model.tmdl ──────────────────────────────────────────────────────────────


def test_model_annotations_sit_at_column_zero(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    lines = _lines(_model(gold), tmp_path, "definition/model.tmdl")
    ann = [ln for ln in lines if "PBI_QueryOrder" in ln]
    assert ann and not ann[0].startswith("\t"), "model annotations are not indented"


def test_model_emits_table_and_culture_refs(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    lines = _lines(_model(gold), tmp_path, "definition/model.tmdl")
    assert "ref table dim_thing" in lines
    assert "ref cultureInfo en-US" in lines
    # the culture ref comes last, after the table refs
    assert lines.index("ref cultureInfo en-US") > lines.index("ref table dim_thing")


def test_ref_order_is_preserved_from_an_existing_model_file(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    """Fabric's ref order reflects its internal state and cannot be recomputed.

    Whatever the prior file said wins, so a regenerate does not reorder what
    Fabric last wrote — the same principle as reusing an on-disk logicalId.
    """
    static = StaticSource(name="Inline")
    sm = SemanticModel(
        name="sm_refs",
        sources=[gold, static],
        tables=[
            Table(
                name="dim_thing",
                source=gold,
                description="Dim.",
                columns=[Column(name="k", data_type="string", description="Key.")],
            ),
            Table(
                name="Label Scaffold",
                source=static,
                description="Scaffold.",
                m_expression='let Source = #table(type table [L = text], {{"a"}}) in Source',
                columns=[Column(name="L", data_type="string", description="L.")],
            ),
        ],
        description="Refs.",
    )
    item = sm.save_to_disk(tmp_path)
    model_path = item / "definition" / "model.tmdl"

    # declared order on a first write
    first = model_path.read_text("utf-8").splitlines()
    assert first.index("ref table dim_thing") < first.index(
        "ref table 'Label Scaffold'"
    )

    # simulate Fabric reordering, then regenerate
    swapped = model_path.read_text("utf-8").replace(
        "ref table dim_thing\nref table 'Label Scaffold'",
        "ref table 'Label Scaffold'\nref table dim_thing",
    )
    model_path.write_text(swapped, encoding="utf-8", newline="")
    sm.save_to_disk(tmp_path)

    after = model_path.read_text("utf-8").splitlines()
    assert after.index("ref table 'Label Scaffold'") < after.index(
        "ref table dim_thing"
    ), "regeneration must not reorder what Fabric wrote"


@pytest.mark.parametrize(
    ("declared", "prior", "expected"),
    [
        # prior order wins
        (["a", "b", "c"], ["c", "a", "b"], ["c", "a", "b"]),
        # a table the prior file never saw is appended in declared order
        (["a", "b", "new"], ["b", "a"], ["b", "a", "new"]),
        # a name the prior file lists but the model dropped is ignored
        (["a", "b"], ["gone", "b", "a"], ["b", "a"]),
        # no prior file -> declared order
        (["a", "b"], None, ["a", "b"]),
        (["a", "b"], [], ["a", "b"]),
    ],
)
def test_ordered_table_names(declared, prior, expected) -> None:
    assert _ordered_table_names(declared, prior) == expected


# ── Item-level files ────────────────────────────────────────────────────────


def test_pbism_carries_schema_and_current_version(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    item = _model(gold).save_to_disk(tmp_path)
    pbism = json.loads((item / "definition.pbism").read_text("utf-8"))
    assert pbism["version"] == "4.2"
    assert "semanticModel/definitionProperties" in pbism["$schema"]


def test_compatibility_level_defaults_to_the_level_fabric_upgrades_to(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    """Fabric rewrites an older level to 1606, so emitting 1567 guaranteed a diff."""
    item = _model(gold).save_to_disk(tmp_path)
    assert "compatibilityLevel: 1606" in (
        item / "definition" / "database.tmdl"
    ).read_text("utf-8")


def test_explicit_compatibility_level_still_wins(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    item = _model(gold, compatibility_level=1604).save_to_disk(tmp_path)
    assert "compatibilityLevel: 1604" in (
        item / "definition" / "database.tmdl"
    ).read_text("utf-8")


def test_platform_keeps_literal_non_ascii(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    """Fabric writes literal UTF-8; ASCII-escaping it diffs on every sync."""
    sm = SemanticModel(
        name="sm_dash",
        sources=[gold],
        tables=[
            Table(
                name="dim_thing",
                source=gold,
                description="Dim.",
                columns=[Column(name="k", data_type="string", description="Key.")],
            )
        ],
        description="Vendor data — profiles and files.",
    )
    raw = (sm.save_to_disk(tmp_path) / ".platform").read_text("utf-8")
    assert "—" in raw
    assert "\\u2014" not in raw
