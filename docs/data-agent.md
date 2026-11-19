# Data Agents

Microsoft provides tools for authoring, publishing, and querying Fabric Data
Agents. pyfabric validates their git-sync artifacts and provides a pytest
fixture for testing published answers.

## Choose a tool for each operation

| Concern | Use | Notes |
| --- | --- | --- |
| Create/configure/publish agents | [`fabric-data-agent-sdk`](https://learn.microsoft.com/en-us/fabric/data-science/fabric-data-agent-sdk) | Preview SDK for data sources, table selection, instructions, example queries and publishing. Follow the SDK's supported Python range and authentication setup. See [the verified versions and dependency constraints](ecosystem.md#verified-baseline). |
| Query a published agent | [MCP endpoint](https://learn.microsoft.com/en-us/fabric/data-science/data-agent-mcp-server) | `https://api.fabric.microsoft.com/v1/mcp/workspaces/{ws}/dataagents/{id}/agent`, bearer token for `https://api.fabric.microsoft.com/.default`. Works only after publish. Use this endpoint for data-agent questions. |
| LLM-critic evaluation | `evaluate_data_agent` in the same SDK | Ground-truth Q&A pairs + critic prompt, results to a lakehouse table. |
| Instruction-writing guidance | [Configuration best practices](https://learn.microsoft.com/en-us/fabric/data-science/data-agent-configuration-best-practices) | Objective / data-source priority / key-terminology format, SQL fragments, example values. |
| Deploy from git | [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/) | Officially supported; `DataAgent` is among its supported item types. |

Use pyfabric for git-sync structure validation, byte normalization, and
instruction lint. The published data-agent endpoint and the [Fabric Core MCP
server](ecosystem.md) serve different workflows.

## Git-sync format

```text
da_example.DataAgent/
├── .platform
└── Files/Config/
    ├── data_agent.json                   # schema pointer only (dataAgent/2.1.0)
    ├── publish_info.json                 # description → becomes the MCP tool description
    ├── draft/
    │   ├── stage_config.json             # aiInstructions (agent-level)
    │   └── <type>-<name>/datasource.json # per-datasource element tree
    └── published/                        # snapshot of draft at last publish
        ├── stage_config.json
        └── <type>-<name>/datasource.json
```

- `publish_info.json` and the `published/` stage appear only after the
  agent's first publish. Its `description` doubles as the MCP tool
  description that client orchestrators read when deciding whether to
  route a question to this agent — write it like a tool description, not
  a display blurb.
- `datasource.json` is a large Fabric-minted element tree: a GUID per
  schema/table/column with `is_selected` flags, plus `userDescription`
  and `dataSourceInstructions` text fields. Treat the GUIDs as
  Fabric-owned — don't mint or rewrite them by hand. The practical
  authoring flow is: configure via SDK/portal → publish → commit-from-
  Fabric so git adopts canonical bytes; hand-edit only the small text
  files (`stage_config.json`, `publish_info.json`) in git.

## Byte conventions

DataAgent config JSON uses CRLF and its `.platform` uses LF. Both omit a
trailing newline. The current convention was verified from three Fabric-authored commits for
one published agent in one workspace, as recorded in [issue #149](https://github.com/Creative-Planning/pyfabric/issues/149).

`normalize_tree` / `write_artifact_file` / `is_canonical` handle this
automatically; `validate_item` checks the folder structure:

```python
from pathlib import Path
from pyfabric.items.normalize import normalize_tree
from pyfabric.items.validate import validate_item

validate_item(Path("ws/da_example.DataAgent"))
normalize_tree(Path("ws/"), dry_run=True)  # pre-push flap check
```

## Querying a published agent: `DataAgentClient`

`pyfabric.testing.data_agent.DataAgentClient` is a thin synchronous
pass-through over the official `mcp` client — pyfabric adds only the
house rails (`FabricCredential` token acquisition, URL construction,
a pytest fixture):

```python
# docs-snippet: network
from pyfabric.testing.data_agent import DataAgentClient

client = DataAgentClient(workspace_id, agent_id, credential=cred)
answer = client.ask("How many products are in the synthetic catalog?")
```

Install the transport with `pip install pyfabric[dataagent]`. When `credential`
is omitted, the client uses `FabricCredential`. The agent
must be **published** — Fabric serves the MCP endpoint only for the
published stage.

The auto-registered `data_agent_client` pytest fixture builds the client
from `PYFABRIC_DATA_AGENT_WORKSPACE` / `PYFABRIC_DATA_AGENT_ID` and
skips the test when either is unset, so live suites stay green on
machines with no published agent reachable.

## Instruction lint

`pyfabric.items.data_agent` applies the `strict_descriptions` posture to
`aiInstructions`: guardrail categories (objective, grounding, refusal,
terminology) are linted, and missing ones are surfaced before the
artifact ships.

```python
from pathlib import Path

from pyfabric.items.data_agent import lint_data_agent, validate_instructions

lint_data_agent(Path("ws/da_example.DataAgent"))  # finding messages
validate_instructions(instructions_text)  # raises when missing
```

`validate_item` runs the lint automatically for DataAgent folders and
reports findings as warnings (the artifact syncs fine without
guardrails — the answers just aren't guarded).

## Validating answer accuracy with pytest

The recommended pattern for guarding against hallucinated answers:

1. Keep a golden-question set alongside your tests: each entry holds a
   natural-language `question`, a `ground_truth_sql` statement, and a
   data-type + tolerance for comparison.
2. At test time, execute `ground_truth_sql` against the same lakehouse
   the agent reads (SQL endpoint, or a local DuckDB mirror for offline
   dry runs) — ground truth comes from the *data*, not from hand-typed
   constants that go stale.
3. Ask the published agent the question over its MCP endpoint and
   compare the extracted value(s) with type-aware tolerances
   (currency ±0.01, percent ±0.0005, exact dates).
4. Gate the live tests behind an environment variable / pytest marker so
   the suite skips cleanly when no published agent is reachable.

Pair deterministic comparison with the SDK's `evaluate_data_agent`
critic when you also want judgment on phrasing and query choice.

## Instruction guardrails

Fabric's best-practices format (Objective, data-source priority, key
terminology) is necessary but not sufficient for accuracy. Add explicit
grounding rules to `aiInstructions`:

- Answer **only** from executed query results — never from general
  knowledge or extrapolation.
- If a query returns no rows, say that no data was found; never
  estimate or fabricate a value.
- Show the query used to produce the answer.
- Don't perform arithmetic beyond the values the query returned.
- Never invent identifiers, dates, or amounts.
- State canonical filters (e.g. latest-snapshot predicates) so repeated
  questions deterministically hit the same rows.
- Define scope and a standard refusal for out-of-scope questions.
