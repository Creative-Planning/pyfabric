---
name: When to use MCP, fab, or pyfabric
description: Choose tools for Fabric operations, Git-first provisioning, direct Python publishing to workspaces without Git integration, and local tests.
type: reference
---

- Start with Fabric Core MCP for ad-hoc operations from an agent, such as
  inspecting a workspace or calling an exposed Fabric tool.
- Start with `fab` for interactive terminal work. Start with `fabric-cicd` for
  standard direct promotion to a workspace without Git integration.
- Use pyfabric to build, validate and normalize git-sync artifacts and run
  local DuckDB and pytest checks. Use dependency-ordered REST publishing and
  orphan cleanup only in workspaces without Git integration. Before cleanup,
  ensure the repo owns every item of the selected types and review the
  dry-run list. Every item of those types missing from the repo is a deletion
  candidate.
- For a Git-connected workspace, build and validate artifacts locally, commit
  and push to the connected branch, and have the workspace owner manually sync
  from the Fabric portal.
- Use `fabric-data-agent-sdk` for authoring and publishing a Data Agent. Its
  published MCP endpoint answers questions. Use pyfabric's instruction lint
  and pytest fixture when checking the artifact or its published answers.
- Use the Azure CLI `microsoft-fabric` extension for Azure-side capacity
  operations and `semantic-link-labs` for interactive model exploration
  where its runtime is supported.

See the [tooling matrix](https://github.com/Creative-Planning/pyfabric/blob/main/docs/ecosystem.md)
for verified versions, overlap and dependency constraints.
