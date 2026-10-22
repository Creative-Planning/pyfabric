"""Regressions for normalize rules found in issues #149 and #150."""

import pytest

from pyfabric.items.normalize import canonical_bytes, rule_for


class TestDataAgentPlatformIsLF:
    # Issue #149: Fabric writes DataAgent's .platform as LF, while the rest of the
    # DataAgent folder is CRLF.
    def test_platform_uses_lf(self):
        rule = rule_for("Agent.DataAgent/.platform")
        assert rule.line_ending == "\n"
        assert rule.trailing_newline is False

    def test_platform_bytes_keep_lf(self):
        raw = b'{\n  "metadata": {}\n}'
        assert canonical_bytes("Agent.DataAgent/.platform", raw) == raw

    def test_nested_config_json_stays_crlf(self):
        rule = rule_for("Agent.DataAgent/Files/Config/data_agent.json")
        assert rule.line_ending == "\r\n"

    def test_other_items_platform_stays_lf(self):
        assert rule_for("Model.SemanticModel/.platform").line_ending == "\n"


class TestTrailingNewlineIsAFixedPoint:
    # Issue #150: extra trailing blank lines were kept for notebooks, so the file
    # flip-flopped between states instead of settling.
    @pytest.mark.parametrize(
        "raw,expected",
        [
            (b"a\n", b"a\n"),
            (b"a\n\n", b"a\n"),
            (b"a\n\n\n", b"a\n"),
            (b"a", b"a\n"),
        ],
    )
    def test_notebook_trailing_lines_collapse_to_one_newline(self, raw, expected):
        assert canonical_bytes("W.Notebook/notebook-content.py", raw) == expected

    @pytest.mark.parametrize("raw", [b"a\n", b"a\n\n", b"a"])
    def test_result_is_a_fixed_point(self, raw):
        once = canonical_bytes("W.Notebook/notebook-content.py", raw)
        assert canonical_bytes("W.Notebook/notebook-content.py", once) == once
