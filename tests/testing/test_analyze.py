"""Tests for Ollama analysis placeholder."""

import pytest

from pyfabric.testing.analyze import analyze_log_file, analyze_test_report


class TestAnalyzePlaceholder:
    def test_analyze_test_report_raises(self):
        with pytest.raises(RuntimeError, match="not yet implemented"):
            analyze_test_report("test-report.json")

    def test_analyze_log_file_raises(self):
        with pytest.raises(RuntimeError, match="not yet implemented"):
            analyze_log_file("some.jsonl")
