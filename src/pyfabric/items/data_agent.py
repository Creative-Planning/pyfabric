"""Instruction lint for Fabric data agent artifacts.

Fabric's own configuration guidance (objective, data-source priority,
key terminology) is necessary but not sufficient for answer accuracy —
agents whose ``aiInstructions`` carry no explicit grounding rules will
happily estimate, extrapolate, or invent identifiers when a query comes
back empty. This module applies the same posture as the SemanticModel /
Report builders' ``strict_descriptions``: the guardrails are linted, and
missing ones are surfaced before the artifact ships.

This is a lint, **not** a builder — pyfabric deliberately does not
author data agents (use Microsoft's ``fabric-data-agent-sdk``; see
``docs/data-agent.md``). The lint reads the git-synced artifact text:

- ``Files/Config/draft/stage_config.json`` → ``aiInstructions``
- ``Files/Config/publish_info.json`` → ``description`` (this doubles as
  the MCP tool description that client orchestrators use for routing)

Usage::

    from pyfabric.items.data_agent import lint_data_agent, validate_instructions

    issues = lint_data_agent(Path("ws/da_example.DataAgent"))  # messages
    validate_instructions(ai_instructions_text)  # raises when sections missing

``validate_item`` runs :func:`lint_data_agent` automatically for
DataAgent folders and reports the findings as warnings.
"""

from __future__ import annotations

import json
from pathlib import Path

# ── Guardrail categories ─────────────────────────────────────────────────────

#: Each category passes when ANY of its (lowercase) markers appears in the
#: instruction text. Keyword heuristics are deliberately loose — the lint
#: flags *absent* guardrail intent, it doesn't grade prose quality.
GUARDRAIL_CATEGORIES: dict[str, tuple[str, ...]] = {
    # What the agent is for / what it covers.
    "objective": (
        "objective",
        "purpose",
        "goal",
        "this agent",
        "the agent answers",
        "you answer questions",
    ),
    # Answers must come from executed queries, never invented.
    "grounding": (
        "query result",
        "executed quer",
        "only from the data",
        "answer only from",
        "no data found",
        "never estimate",
        "do not estimate",
        "never fabricate",
        "do not fabricate",
        "never invent",
        "do not invent",
        "never guess",
        "do not guess",
    ),
    # Behavior for questions outside the agent's scope.
    "refusal": (
        "out of scope",
        "out-of-scope",
        "outside the scope",
        "refuse",
        "decline",
        "cannot answer",
        "can't answer",
        "do not answer questions about",
    ),
    # Domain terms, synonyms, abbreviations the NL layer must map.
    "terminology": (
        "terminology",
        "synonym",
        "glossary",
        "abbreviation",
        "acronym",
        "refers to",
        "stands for",
    ),
}


class InstructionLintError(Exception):
    """Raised by :func:`validate_instructions` in strict mode."""


# ── Lint functions ───────────────────────────────────────────────────────────


def lint_instruction_text(text: str | None) -> list[str]:
    """Lint agent-level instruction text for missing guardrail categories.

    Returns one message per problem; an empty list means all categories
    are covered. Empty/blank instructions are a single (fatal-worded)
    finding — there's no point listing every category against nothing.
    """
    if text is None or not text.strip():
        return [
            "aiInstructions are empty — the agent runs with no grounding, "
            "scope, or terminology guidance at all"
        ]
    lowered = text.lower()
    issues: list[str] = []
    for category, markers in GUARDRAIL_CATEGORIES.items():
        if not any(marker in lowered for marker in markers):
            issues.append(
                f"aiInstructions lack a '{category}' section — see "
                "docs/data-agent.md for the guardrail checklist"
            )
    return issues


def validate_instructions(text: str | None, *, strict: bool = True) -> list[str]:
    """Lint instruction text; in strict mode raise when anything is missing.

    Mirrors the builders' ``strict_descriptions`` posture: reach for
    ``strict=False`` to inspect findings, not to ship without guardrails.
    """
    issues = lint_instruction_text(text)
    if strict and issues:
        raise InstructionLintError(
            "Data agent instructions failed guardrail lint:\n  - "
            + "\n  - ".join(issues)
        )
    return issues


def lint_data_agent(item_dir: Path) -> list[str]:
    """Lint a git-synced ``*.DataAgent`` folder. Returns finding messages.

    Reads the draft-stage ``aiInstructions`` (the stage humans edit) and,
    when the agent has been published, the ``publish_info.json``
    description. Files that are absent or unparsable are reported rather
    than raised — this feeds ``validate_item`` warnings.
    """
    issues: list[str] = []
    config_dir = item_dir / "Files" / "Config"

    stage_path = config_dir / "draft" / "stage_config.json"
    if stage_path.exists():
        parse_issues: list[str] = []
        instructions = _read_json_field(stage_path, "aiInstructions", parse_issues)
        issues.extend(parse_issues)
        if not parse_issues:
            issues.extend(lint_instruction_text(instructions))
    else:
        issues.append("draft/stage_config.json missing — no aiInstructions to lint")

    publish_path = config_dir / "publish_info.json"
    if publish_path.exists():
        description = _read_json_field(publish_path, "description", issues)
        if description is not None and not description.strip():
            issues.append(
                "publish_info.json description is empty — it becomes the MCP "
                "tool description orchestrators use to route questions"
            )
    # An absent publish_info.json just means the agent hasn't been
    # published yet — normal for a new artifact, not a finding.

    return issues


def _read_json_field(path: Path, field: str, issues: list[str]) -> str | None:
    """Read one string field from a JSON file, recording parse problems."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        issues.append(f"{path.name} could not be parsed: {e}")
        return None
    value = data.get(field)
    if value is not None and not isinstance(value, str):
        issues.append(f"{path.name} field '{field}' is not a string")
        return None
    return value
