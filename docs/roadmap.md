# Roadmap

This document tracks the implementation plan for pyfabric across four
phases, from library foundation through LLM-powered data analysis.

## Phase 1: Library Foundation (Complete)

Built the core library scaffold, ported fabric-libs, added validation,
testing, documentation, and refactored for maintainability.

| Step | Description | PR |
|------|------------|----|
| 1 | Library scaffold — project structure, CI/CD, supply chain security | #1 |
| 2 | Port fabric-libs into pyfabric sub-package structure | #4 |
| 3 | Fabric item type definitions + structure validation (TDD) | #5 |
| 4 | Comprehensive test coverage (104 tests + error paths + E2E fixes) | #6, #7 |
| 5 | Migrate stdlib logging to structlog | #8 |
| 6 | Testing sub-package — DuckDB Spark mock, notebookutils, pytest fixtures | #9 |
| 7 | Documentation — API reference, testing guide, AI prompt samples | #10 |
| 8 | OOP refactoring — cohesion, coupling, testability, performance | #11 |
| 9 | PyPI publication (tag + trusted publisher OIDC) | — |

## Phase 2: Fabric Item Authoring Depth

Expand supported Fabric item types:

- **Semantic Models** — full tabular model authoring (model.bim with
  tables, columns, measures, relationships)
- **Reports** (Power BI) — report definition authoring tied to semantic
  models
- **Dataflow Gen2** — mashup/Power Query authoring support
- **Pipelines** — activity graph authoring (notebook activities, copy
  activities, orchestration)
- **Data Agents** — programmatic data agent definitions
- **Lakehouses** — add new tables with schemas and data (not just the lakehouse
  shell)
- Integration with Workspace Identity and Azure Key Vault for secure ETL/ELT

## Phase 3: Event-Driven Automation

- **Activators with EventStreams** — listen to a lakehouse files folder
  for activity (file uploaded, file changed) and automatically trigger a
  pipeline or notebook
- Wire up event-driven patterns for CI/CD and data pipeline automation
- Integration with Power Automate flows

## Phase 4: LLM-Powered Data Analysis

Local or online data quality analysis:

- Pull production data from Fabric locally
- Mask/scramble production data for safe local testing with near-real data
- Run notebook/pipeline transformations locally against DuckDB
- Integrate Ollama + user's choice of local LLM model for deep data QA on sensitive data
- Compare tables and analyze data state differences using LLM of choice
  (online or local, depending on sensitivity and cost)
- Anomaly detection, data quality analysis, all on-box when needed so sensitive data doesn't go off-box
