"""Tests for disconnected scaffold tables: StaticSource + Column.sort_by_column.

Together these let a model carry a small table whose rows live in the model
itself (a label/ordinal pair driving a "label | value" detail panel, a banding
or parameter table) and order it by a hidden ordinal rather than alphabetically.
"""

import json
from pathlib import Path

import pytest

from pyfabric.items.semantic_model import (
    Column,
    LakehouseSource,
    SemanticModel,
    SemanticModelError,
    StaticSource,
    Table,
)


@pytest.fixture
def gold() -> LakehouseSource:
    return LakehouseSource(name="Gold", workspace_id="ws-1234", lakehouse_id="lh-5678")


def _tmdl(sm: SemanticModel, tmp_path: Path, name: str) -> str:
    item = sm.save_to_disk(tmp_path)
    return (item / "definition" / "tables" / f"{name}.tmdl").read_text("utf-8")


def _depth(tmdl: str, content: str) -> int:
    """Tab depth of the one line whose stripped content is ``content``."""
    hits = [ln for ln in tmdl.splitlines() if ln.strip() == content]
    assert len(hits) == 1, f"expected exactly one {content!r} line, got {len(hits)}"
    return len(hits[0]) - len(hits[0].lstrip("\t"))


# ── sortByColumn ────────────────────────────────────────────────────────────


def _labels_table(src, **col_kw) -> Table:
    """A scaffold table: a visible label ordered by a hidden ordinal."""
    return Table(
        name="Labels",
        source=src,
        description="Label scaffold.",
        columns=[
            Column(
                name="Label",
                data_type="string",
                description="Displayed label.",
                **col_kw,
            ),
            Column(name="Sort", data_type="int64", is_hidden=True),
        ],
    )


def _sort_model(gold: LakehouseSource, **col_kw) -> SemanticModel:
    return SemanticModel(
        name="sm_sort",
        sources=[gold],
        tables=[_labels_table(gold, **col_kw)],
        description="Sort test.",
    )


def test_sort_by_column_emits_under_the_column(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    sm = _sort_model(gold, sort_by_column="Sort")
    sm.validate()
    tmdl = _tmdl(sm, tmp_path, "Labels")

    assert _depth(tmdl, "sortByColumn: Sort") == 2
    # Fabric reads sourceColumn then sortByColumn; keep that order
    assert tmdl.index("sourceColumn: Label") < tmdl.index("sortByColumn: Sort")
    # only the column that asked for it gets one
    assert tmdl.count("sortByColumn:") == 1


def test_sort_by_column_omitted_when_unset(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    sm = _sort_model(gold)
    sm.validate()
    assert "sortByColumn" not in _tmdl(sm, tmp_path, "Labels")


def test_sort_by_column_must_reference_a_column_on_the_same_table(
    gold: LakehouseSource,
) -> None:
    sm = _sort_model(gold, sort_by_column="NotAColumn")
    assert any("not a column on this table" in e for e in sm.validate())


def test_sort_by_column_cannot_be_self_referential(gold: LakehouseSource) -> None:
    sm = _sort_model(gold, sort_by_column="Label")
    assert any("cannot sort by itself" in e for e in sm.validate())


# ── StaticSource (inline-M tables) ──────────────────────────────────────────

_LABELS_M = """
let
    Source = #table(
        type table [Label = text, Sort = Int64.Type],
        {
        {"City", 1},
        {"State", 2}
        }
    )
in
    Source
"""


def _static_model(gold: LakehouseSource, **table_kw) -> SemanticModel:
    static = StaticSource(name="inline")
    fact = Table(
        name="fact_x",
        source=gold,
        description="Fact.",
        columns=[Column(name="k", data_type="string", description="Key.")],
    )
    labels = Table(
        name="Labels",
        source=static,
        description="Label scaffold.",
        columns=[
            Column(name="Label", data_type="string", description="Displayed label."),
            Column(name="Sort", data_type="int64", is_hidden=True),
        ],
        **table_kw,
    )
    return SemanticModel(
        name="sm_static",
        sources=[gold, static],
        tables=[fact, labels],
        description="Static table test.",
    )


def test_static_source_emits_an_inline_import_partition(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    sm = _static_model(gold, m_expression=_LABELS_M)
    sm.validate()
    tmdl = _tmdl(sm, tmp_path, "Labels")

    assert _depth(tmdl, "partition Labels = m") == 1
    assert _depth(tmdl, "mode: import") == 2
    assert _depth(tmdl, "source =") == 2
    # the caller writes plain M; the emitter owns TMDL indentation
    assert _depth(tmdl, "let") == 4
    assert _depth(tmdl, "in") == 4
    assert "\t\t\t\t    Source = #table(" in tmdl
    # a static table borrows no shared expression
    assert "Source = inline" not in tmdl


def test_static_source_contributes_no_expression(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    sm = _static_model(gold, m_expression=_LABELS_M)
    sm.validate()
    item = sm.save_to_disk(tmp_path)
    expressions = (item / "definition" / "expressions.tmdl").read_text("utf-8")

    assert "inline" not in expressions
    # the lakehouse source still contributes its parameters + navigation
    assert "GoldWorkspaceId" in expressions


def test_static_table_appears_in_query_order(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    sm = _static_model(gold, m_expression=_LABELS_M)
    sm.validate()
    item = sm.save_to_disk(tmp_path)
    model_tmdl = (item / "definition" / "model.tmdl").read_text("utf-8")
    order = json.loads(model_tmdl.split("PBI_QueryOrder = ", 1)[1].splitlines()[0])

    assert "Labels" in order
    assert "inline" not in order  # the source itself is not an M query


def test_static_source_requires_an_m_expression(gold: LakehouseSource) -> None:
    sm = _static_model(gold)
    assert any("need an inline M expression" in e for e in sm.validate())


def test_m_expression_rejected_on_non_static_sources(gold: LakehouseSource) -> None:
    sm = SemanticModel(
        name="sm_badm",
        sources=[gold],
        tables=[
            Table(
                name="fact_x",
                source=gold,
                description="Fact.",
                columns=[Column(name="k", data_type="string", description="Key.")],
                m_expression=_LABELS_M,
            )
        ],
        description="Bad m_expression.",
    )
    assert any("only valid with a StaticSource" in e for e in sm.validate())


def test_static_source_name_must_be_an_m_identifier() -> None:
    with pytest.raises(ValueError, match="valid M identifier"):
        StaticSource(name="not an identifier")


def test_static_table_lineage_is_stable_across_runs(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    first = _static_model(gold, m_expression=_LABELS_M)
    first.validate()
    second = _static_model(gold, m_expression=_LABELS_M)
    second.validate()

    assert _tmdl(first, tmp_path / "a", "Labels") == _tmdl(
        second, tmp_path / "b", "Labels"
    ), "lineage tags must be deterministic across runs"


def test_scaffold_table_combines_static_rows_with_an_ordinal_sort(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    """The two features together — the shape a detail-panel scaffold needs."""
    static = StaticSource(name="inline")
    labels = Table(
        name="Labels",
        source=static,
        description="Label scaffold.",
        m_expression=_LABELS_M,
        columns=[
            Column(
                name="Label",
                data_type="string",
                description="Displayed label.",
                sort_by_column="Sort",
            ),
            Column(name="Sort", data_type="int64", is_hidden=True),
        ],
    )
    sm = SemanticModel(
        name="sm_panel",
        sources=[gold, static],
        tables=[
            Table(
                name="fact_x",
                source=gold,
                description="Fact.",
                columns=[Column(name="k", data_type="string", description="Key.")],
            ),
            labels,
        ],
        description="Panel scaffold.",
    )
    sm.validate()
    tmdl = _tmdl(sm, tmp_path, "Labels")

    assert _depth(tmdl, "sortByColumn: Sort") == 2
    assert _depth(tmdl, "partition Labels = m") == 1
    assert "#table(" in tmdl


def test_validation_errors_block_save(gold: LakehouseSource, tmp_path: Path) -> None:
    """validate() reports; save_to_disk() is what refuses to emit."""
    sm = _static_model(gold)  # StaticSource table with no m_expression
    with pytest.raises(SemanticModelError, match="need an inline M expression"):
        sm.save_to_disk(tmp_path)
