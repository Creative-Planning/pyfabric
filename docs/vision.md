# Vision

pyfabric empowers data engineers to leverage Microsoft Fabric's git sync
capability to author Fabric artifacts locally using tools like Claude Code
and GitHub Copilot with VS Code (or standalone CLIs) and have them sync
into Fabric with no changes needed on import.

## Design Principles

### Git sync clean

Every artifact pyfabric produces must be valid for Fabric git sync import
with zero modifications. If Fabric can't import it cleanly, it's a bug.

### Human-first, AI-augmented

pyfabric provides command-line capabilities — Python scripts a human can
run within PowerShell — to interact with Fabric: query data via SQL
endpoints or directly, manage Fabric items with a light wrapper around
Fabric REST APIs. A human can run these tools and see results without
burning AI tokens. AI can run them equally well when the human desires.

Humans and AI working hand-in-hand, as efficiently and cost-effectively
as possible.

### Structured logging for dual audiences

A human can find the root cause of a failure quickly from the structured
logs. A human can also hand those same logs to an AI to evaluate and
suggest fixes. The logging system (structlog with JSON output and console
rendering) serves both audiences by design.

### LLM-flexible data analysis

Deep data analysis — for example, comparing two tables and figuring out
what's the same and different about the data states — powered by the
user's choice of LLM: online (Claude, Copilot) or local-only (Ollama)
depending on data sensitivity and cost preference. Not locked to any
single AI provider.

## Evaluating features

Every feature should be evaluated against these principles:

- If it requires the Fabric UI to fix something on import, it's not done.
- If it can only be used by AI and not a human at a terminal, it's not done.
- If it forces a specific LLM provider, it's not done.
