"""TMDL identifier quoting.

A name that is not a plain identifier — a space, a hyphen, a dot, a leading
digit — must be single-quoted in TMDL or the declaration mis-parses:
``table Vendor Info Labels`` reads as the name ``Vendor`` followed by junk.
Measures were already quoted at their emit site; table, column, partition and
sortByColumn declarations were emitted bare, so any model with a spaced table
name produced TMDL Fabric cannot load.
"""

from pathlib import Path

import pytest

from pyfabric.items.semantic_model import (
    Column,
    LakehouseSource,
    Measure,
    SemanticModel,
    StaticSource,
    Table,
    _ident,
)


@pytest.fixture
def gold() -> LakehouseSource:
    return LakehouseSource(name="Gold", workspace_id="ws-1", lakehouse_id="lh-1")


def _tmdl(sm: SemanticModel, tmp_path: Path, name: str) -> str:
    item = sm.save_to_disk(tmp_path)
    return (item / "definition" / "tables" / f"{name}.tmdl").read_text("utf-8")


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("dim_vendor", "dim_vendor"),
        ("_private", "_private"),
        ("Table1", "Table1"),
        ("Vendor Info Labels", "'Vendor Info Labels'"),
        ("has-hyphen", "'has-hyphen'"),
        ("has.dot", "'has.dot'"),
        ("1leading", "'1leading'"),
        ("", "''"),
        ("it's", "'it''s'"),
    ],
)
def test_ident_quotes_only_when_needed(name: str, expected: str) -> None:
    assert _ident(name) == expected


def test_spaced_table_name_is_quoted_in_declaration_and_partition(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    sm = SemanticModel(
        name="sm_spaced",
        sources=[gold],
        tables=[
            Table(
                name="Vendor Info Labels",
                source=gold,
                description="Spaced name.",
                columns=[Column(name="k", data_type="string", description="Key.")],
            )
        ],
        description="Spaced table.",
    )
    sm.validate()
    tmdl = _tmdl(sm, tmp_path, "Vendor Info Labels")

    assert "table 'Vendor Info Labels'" in tmdl
    assert "partition 'Vendor Info Labels' = m" in tmdl
    # the bare forms would mis-parse
    assert "table Vendor Info Labels" not in tmdl
    assert "partition Vendor Info Labels" not in tmdl


def test_spaced_column_and_sort_by_column_are_quoted(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    sm = SemanticModel(
        name="sm_spacedcol",
        sources=[gold],
        tables=[
            Table(
                name="t",
                source=gold,
                description="T.",
                columns=[
                    Column(
                        name="Display Label",
                        data_type="string",
                        description="Label.",
                        sort_by_column="Sort Order",
                    ),
                    Column(name="Sort Order", data_type="int64", is_hidden=True),
                ],
            )
        ],
        description="Spaced columns.",
    )
    sm.validate()
    tmdl = _tmdl(sm, tmp_path, "t")

    assert "column 'Display Label'" in tmdl
    assert "column 'Sort Order'" in tmdl
    assert "sortByColumn: 'Sort Order'" in tmdl


def test_unspaced_names_stay_bare(gold: LakehouseSource, tmp_path: Path) -> None:
    """Guard against over-quoting — the existing corpus must not churn."""
    sm = SemanticModel(
        name="sm_plain",
        sources=[gold],
        tables=[
            Table(
                name="dim_vendor",
                source=gold,
                description="Dim.",
                columns=[
                    Column(name="vendor_id", data_type="string", description="Key."),
                    Column(name="sort_key", data_type="int64", is_hidden=True),
                ],
            )
        ],
        description="Plain names.",
    )
    sm.validate()
    tmdl = _tmdl(sm, tmp_path, "dim_vendor")

    assert "table dim_vendor" in tmdl
    assert "column vendor_id" in tmdl
    assert "column sort_key" in tmdl
    assert "partition dim_vendor = m" in tmdl
    # no stray quoting of the plain names
    assert "table 'dim_vendor'" not in tmdl
    assert "column 'vendor_id'" not in tmdl
    assert "partition 'dim_vendor'" not in tmdl


def test_spaced_static_scaffold_round_trips(
    gold: LakehouseSource, tmp_path: Path
) -> None:
    """The real shape that exposed this: a spaced, statically-sourced scaffold
    with an ordinal sort and measures referencing it."""
    static = StaticSource(name="VendorInfoLabels")
    sm = SemanticModel(
        name="sm_panel",
        sources=[gold, static],
        tables=[
            Table(
                name="dim_vendor",
                source=gold,
                description="Dim.",
                columns=[Column(name="city", data_type="string", description="City.")],
            ),
            Table(
                name="Vendor Info Labels",
                source=static,
                description="Label scaffold.",
                m_expression=(
                    "let\n"
                    "    Source = #table(type table [Label = text, Sort = Int64.Type], "
                    '{{"City", 1}})\n'
                    "in\n"
                    "    Source"
                ),
                columns=[
                    Column(
                        name="Label",
                        data_type="string",
                        description="Label.",
                        sort_by_column="Sort",
                    ),
                    Column(name="Sort", data_type="int64", is_hidden=True),
                ],
                measures=[
                    Measure(
                        name="Vendor Info Value",
                        expression="SELECTEDVALUE(dim_vendor[city])",
                        description="Value for the label.",
                    )
                ],
            ),
        ],
        description="Panel.",
    )
    sm.validate()
    tmdl = _tmdl(sm, tmp_path, "Vendor Info Labels")

    assert "table 'Vendor Info Labels'" in tmdl
    assert "partition 'Vendor Info Labels' = m" in tmdl
    assert "measure 'Vendor Info Value'" in tmdl
    assert "sortByColumn: Sort" in tmdl  # bare is correct here
    assert "#table(" in tmdl
