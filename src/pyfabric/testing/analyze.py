"""AI-powered test and log analysis (Ollama integration placeholder).

This module provides hooks for local LLM-powered analysis of test reports
and log files. Currently raises RuntimeError — the implementation will be
added when the ``pyfabric[llm]`` optional dependency is available.

Future usage::

    from pyfabric.testing.analyze import analyze_test_report, analyze_log_file

    # Analyze a pytest JSON report for failure patterns
    summary = analyze_test_report("test-report.json", model="gemma3")

    # Analyze a structlog JSON log file for anomalies
    findings = analyze_log_file(".logs/my_script_20250401.jsonl", model="gemma3")
"""

from __future__ import annotations

from pathlib import Path


def analyze_test_report(
    report_path: str | Path,
    *,
    model: str = "gemma3",
    host: str = "http://localhost:11434",
) -> str:
    """Analyze a pytest JSON report using a local Ollama model.

    Reads the test report, extracts failures and errors, and asks
    the local LLM to summarize root causes and suggest fixes.

    Args:
        report_path: Path to pytest-json-report output file.
        model: Ollama model name (default: gemma3).
        host: Ollama API endpoint.

    Returns:
        AI-generated analysis summary.

    Raises:
        RuntimeError: Always — Ollama integration not yet implemented.
    """
    raise RuntimeError(
        "Ollama integration not yet implemented. "
        "Install: pip install ollama, then run: ollama pull gemma3"
    )


def analyze_log_file(
    log_path: str | Path,
    *,
    model: str = "gemma3",
    host: str = "http://localhost:11434",
) -> str:
    """Analyze a structlog JSON Lines log file using a local Ollama model.

    Reads the log file, identifies error patterns and anomalies, and asks
    the local LLM to summarize findings and recommend actions.

    Args:
        log_path: Path to a .jsonl log file from pyfabric._logging.
        model: Ollama model name (default: gemma3).
        host: Ollama API endpoint.

    Returns:
        AI-generated analysis summary.

    Raises:
        RuntimeError: Always — Ollama integration not yet implemented.
    """
    raise RuntimeError(
        "Ollama integration not yet implemented. "
        "Install: pip install ollama, then run: ollama pull gemma3"
    )
