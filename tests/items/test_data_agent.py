"""Tests for the data agent instruction-guardrail lint."""

import json
from pathlib import Path

import pytest

from pyfabric.items.data_agent import (
    GUARDRAIL_CATEGORIES,
    InstructionLintError,
    lint_data_agent,
    lint_instruction_text,
    validate_instructions,
)
from pyfabric.items.validate import validate_item

GOOD_INSTRUCTIONS = """\
### Objective
This agent answers questions about monthly widget projections.

### Grounding
- Answer only from executed query results. If a query returns no rows,
  say that no data was found — never estimate a value.
- Never invent identifiers, dates, or amounts.

### Scope
Politely decline questions that are out of scope for projection data.

### Terminology
- "TCV" refers to total contract value. "margin" refers to gross margin.
"""


class TestLintInstructionText:
    def test_good_instructions_pass(self):
        assert lint_instruction_text(GOOD_INSTRUCTIONS) == []

    @pytest.mark.parametrize("text", [None, "", "   \r\n  "])
    def test_empty_is_single_finding(self, text):
        issues = lint_instruction_text(text)
        assert len(issues) == 1
        assert "empty" in issues[0]

    def test_formatting_only_instructions_flag_every_category(self):
        # The classic failure mode: instructions that only cover output
        # formatting, with no grounding/scope/terminology at all.
        issues = lint_instruction_text(
            "Always show results in table format and use USD currency."
        )
        assert len(issues) == len(GUARDRAIL_CATEGORIES)

    def test_missing_single_category_reported_by_name(self):
        text = GOOD_INSTRUCTIONS.replace("### Terminology", "").replace(
            '"TCV" refers to total contract value. "margin" refers to gross margin.', ""
        )
        issues = lint_instruction_text(text)
        assert len(issues) == 1
        assert "terminology" in issues[0]

    def test_case_insensitive(self):
        assert lint_instruction_text(GOOD_INSTRUCTIONS.upper()) == []


class TestValidateInstructions:
    def test_strict_raises_with_all_findings(self):
        with pytest.raises(InstructionLintError) as exc:
            validate_instructions("be helpful")
        for category in GUARDRAIL_CATEGORIES:
            assert category in str(exc.value)

    def test_non_strict_returns_findings(self):
        issues = validate_instructions("be helpful", strict=False)
        assert len(issues) == len(GUARDRAIL_CATEGORIES)

    def test_strict_passes_good_text(self):
        assert validate_instructions(GOOD_INSTRUCTIONS) == []


def _write_agent(
    base: Path,
    *,
    instructions: str | None = GOOD_INSTRUCTIONS,
    publish_description: str | None = "Answers projection questions.",
    stage_config: bool = True,
) -> Path:
    item_dir = base / "da_test.DataAgent"
    config = item_dir / "Files" / "Config"
    config.mkdir(parents=True)
    (config / "data_agent.json").write_text('{"$schema": "..."}', encoding="utf-8")
    if stage_config:
        (config / "draft").mkdir()
        (config / "draft" / "stage_config.json").write_text(
            json.dumps({"aiInstructions": instructions}), encoding="utf-8"
        )
    if publish_description is not None:
        (config / "publish_info.json").write_text(
            json.dumps({"description": publish_description}), encoding="utf-8"
        )
    return item_dir


class TestLintDataAgent:
    def test_well_formed_agent_is_clean(self, tmp_path: Path):
        assert lint_data_agent(_write_agent(tmp_path)) == []

    def test_missing_stage_config_reported(self, tmp_path: Path):
        item_dir = _write_agent(tmp_path, stage_config=False)
        issues = lint_data_agent(item_dir)
        assert any("stage_config.json missing" in i for i in issues)

    def test_empty_instructions_reported(self, tmp_path: Path):
        item_dir = _write_agent(tmp_path, instructions="")
        issues = lint_data_agent(item_dir)
        assert any("empty" in i for i in issues)

    def test_blank_publish_description_reported(self, tmp_path: Path):
        item_dir = _write_agent(tmp_path, publish_description=" ")
        issues = lint_data_agent(item_dir)
        assert any("MCP tool description" in i for i in issues)

    def test_unpublished_agent_not_penalized(self, tmp_path: Path):
        # No publish_info.json just means "not yet published" — not a finding.
        item_dir = _write_agent(tmp_path, publish_description=None)
        assert lint_data_agent(item_dir) == []

    def test_unparsable_stage_config_reported_once(self, tmp_path: Path):
        item_dir = _write_agent(tmp_path)
        stage = item_dir / "Files" / "Config" / "draft" / "stage_config.json"
        stage.write_text("{not json", encoding="utf-8")
        issues = lint_data_agent(item_dir)
        assert len(issues) == 1
        assert "could not be parsed" in issues[0]


class TestValidateItemIntegration:
    def test_findings_surface_as_warnings_not_errors(self, tmp_path: Path):
        item_dir = _write_agent(tmp_path, instructions="table format please")
        (item_dir / ".platform").write_text(
            json.dumps(
                {
                    "metadata": {"type": "DataAgent", "displayName": "da_test"},
                    "config": {"version": "2.0", "logicalId": "0" * 36},
                }
            ),
            encoding="utf-8",
        )
        result = validate_item(item_dir)
        assert result.valid  # lint findings are warnings, never errors
        assert len(result.warnings) == len(GUARDRAIL_CATEGORIES)
