---
name: Data agent git format + adopt-first tooling
description: DataAgent artifact anatomy, CRLF byte convention (incl. .platform), publish semantics, and which Microsoft tools to use instead of building — fabric-data-agent-sdk for authoring, MCP endpoint for querying/validation.
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

**Byte convention**: every DataAgent file is **CRLF + no trailing newline,
including `.platform`** — the one item type whose `.platform` is not LF.
`normalize_tree` / `write_artifact_file` handle this; never write these
files with plain LF text writers.

**Adopt-first — do not build**:

- Authoring (create, datasources, table selection, instructions, example
  queries, publish): **`fabric-data-agent-sdk`** (PyPI, preview) — works
  inside and outside Fabric notebooks (AzureCliCredential / SPN).
- Querying a **published** agent: MCP endpoint
  `https://api.fabric.microsoft.com/v1/mcp/workspaces/{ws}/dataagents/{id}/agent`,
  bearer token scope `https://api.fabric.microsoft.com/.default`
  (`mcp` + `azure-identity` packages). Unpublished agents return errors.
  The OpenAI-Assistants external client is deprecated (shutdown
  2026-08-26) — don't use it.
- LLM-critic eval: `evaluate_data_agent` in the same SDK.

**Practical authoring flow**: configure via SDK/portal → publish →
commit-from-Fabric so git adopts canonical bytes. Hand-edit only the small
text files (`stage_config.json`, `publish_info.json`) in git; treat
datasource GUIDs as Fabric-owned.

**Accuracy validation pattern**: golden-question set (question +
`ground_truth_sql` + data-type/tolerance); execute the SQL against the
same lakehouse at test time; ask the published agent via MCP; compare
with type-aware tolerances; env-gate the live tests. `aiInstructions`
must carry grounding guardrails (answer only from executed query results;
"no data found" over estimation; show the query; never invent
identifiers/amounts; canonical filters; scoped refusal). See
`docs/data-agent.md`.
