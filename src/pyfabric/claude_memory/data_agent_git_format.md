---
name: Data agent git format + adopt-first tooling
description: DataAgent artifact anatomy, CRLF config JSON and LF .platform, publish semantics, and which Microsoft tools to use instead of building — fabric-data-agent-sdk for authoring, MCP endpoint for querying/validation.
type: reference
---

**Artifact anatomy** (`<name>.DataAgent/`): `.platform`;
`Files/Config/data_agent.json` (schema pointer only);
`Files/Config/publish_info.json` (**its `description` becomes the MCP tool
description** orchestrators use for routing — write it like a tool
description); `Files/Config/{draft,published}/stage_config.json`
(`aiInstructions`); `Files/Config/{draft,published}/<type>-<name>/datasource.json`
(large Fabric-minted GUID element tree with `is_selected` flags,
`userDescription`, `dataSourceInstructions`). `publish_info.json` and
`published/` exist only after first publish.

DataAgent config JSON uses CRLF and `.platform` uses LF. Both omit a trailing
newline. Use `normalize_tree` or `write_artifact_file` for these conventions.

## Tool choice

- Authoring (create, datasources, table selection, instructions, example
  queries, publish) uses **`fabric-data-agent-sdk`** (preview). Check the SDK's
  supported Python range and dependency constraints before installing.
- Querying a **published** agent: MCP endpoint
  `https://api.fabric.microsoft.com/v1/mcp/workspaces/{ws}/dataagents/{id}/agent`,
  bearer token scope `https://api.fabric.microsoft.com/.default`
  (`mcp` + `azure-identity` packages). Unpublished agents return errors.
  Use the published agent endpoint for Q&A and the Fabric Core MCP
  server for ad-hoc Fabric operations.
- LLM-critic eval: `evaluate_data_agent` in the same SDK.

**Practical authoring flow**: configure via SDK/portal → publish →
commit-from-Fabric so git adopts canonical bytes. Hand-edit only the small
text files (`stage_config.json`, `publish_info.json`) in git; treat
datasource GUIDs as Fabric-owned.

**Accuracy validation pattern**: golden-question set (question +
`ground_truth_sql` + data-type/tolerance); execute the SQL against the
same lakehouse at test time; ask the published agent via
`pyfabric.testing.data_agent.DataAgentClient(ws, agent).ask(q)`
(`pip install pyfabric[dataagent]`; auto-registered `data_agent_client`
fixture env-gates on `PYFABRIC_DATA_AGENT_WORKSPACE`/`..._ID`); compare
with type-aware tolerances. `aiInstructions` must carry grounding
guardrails (answer only from executed query results; "no data found"
over estimation; show the query; never invent identifiers/amounts;
canonical filters; scoped refusal) — `pyfabric.items.data_agent`'s
`validate_instructions` / `lint_data_agent` lint for exactly these, and
`validate_item` warns on DataAgent folders missing them. See
`docs/data-agent.md`.
