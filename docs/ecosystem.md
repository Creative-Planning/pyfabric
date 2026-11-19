# Microsoft Fabric tooling and pyfabric

## Choose by workflow

Use Fabric MCP for ad-hoc agent operations and `fab` for interactive terminal
work. Use pyfabric for validated git-sync changes, offline tests that can run
in CI, and dependency-ordered Python publishing to workspaces without Git
integration. For Git-connected workspaces, build and validate the artifacts
locally, commit and push them to the connected branch, and have the workspace
owner manually sync from the Fabric portal.
[Git-first provisioning workflow](../CLAUDE.md#provisioning-workflow-git-first)

| Workflow | Start with | Where pyfabric fits |
| --- | --- | --- |
| An agent needs to inspect a workspace, look up an item, or invoke an available Fabric operation | Fabric Core MCP (remote), or the local Fabric MCP server where the host uses it | Python automation that needs explicit client control and repeatable tests |
| A developer needs to browse items, inspect a job, or issue a one-off API request | `fab` | Reusable Python scripts that compose workspace, item, job, and definition helpers |
| Promote a supported artifact repository directly to a workspace without Git integration | `fabric-cicd` | `publish_repo` when an existing pyfabric workflow needs explicit Report/Model or Pipeline/Notebook ordering and local preflight |
| Author an item or diagnose a git-sync diff | pyfabric builders, `validate_workspace`, and `normalize_tree` | Build and check the local artifact before the platform ingests it |
| Unit-test notebook transforms or artifact contracts without capacity | pyfabric's DuckDB Spark mock, notebookutils mock, and pytest fixtures | Run against synthetic data in local development and CI |
| Author, publish, or evaluate a Data Agent | `fabric-data-agent-sdk` | Validate git-sync files, lint instructions, and normalize artifact bytes |
| Ask a published Data Agent a question | The agent's own MCP endpoint | Use `DataAgentClient` and the pytest fixture for repeatable published-answer checks |
| Inspect or change Azure-side Fabric capacity resources | Azure CLI's `microsoft-fabric` extension | Keep capacity management with Azure tooling |
| Explore semantic models from a Fabric notebook | `semantic-link-labs` where its runtime is supported | Build or lint local semantic model definitions before sync |

## What pyfabric adds

- Builders write versionable definitions and reuse the existing `.platform`
  logical ID when a build targets an existing artifact.
- Validation and normalization catch required-file, schema, and byte-level
  problems before git sync, including item-specific line endings.
- Dependency-ordered REST publishing supports Python artifact workflows for
  workspaces without Git integration. For orphan cleanup, the repository must
  own every workspace item of the selected types. Preview and review every
  candidate before deleting. [Deployment guide](deploy.md#unpublish_orphans)
- DuckDB, local filesystem mocks, and pytest fixtures allow repeatable
  transform and artifact checks without a workspace or a Fabric token.

Evaluate overlap on the workflow's actual contract before retiring a wrapper.
Compare caller usage, supported item types, authentication, dependency ordering,
artifact bytes, recovery behavior, and whether the replacement can run in the
same CI environment. An interactive command or a live API is sufficient for
some operations. Offline validation and stable Python call sites need their own
replacement evidence.

## Verified baseline

Verified on 2026-11-19 UTC. Versions describe the evidence gathered for this
refresh, not a promise that these are always the newest releases.

| Tool | Verified version/status | Python requirement | Evidence |
| --- | --- | --- | --- |
| Fabric MCP Server (local) | 1.8.0, released 2026-11-11 | Not a Python requirement | [GitHub release](https://github.com/microsoft/mcp/releases/tag/Fabric.Mcp.Server-1.8.0) and live GitHub changelog |
| Fabric Core MCP Server (remote) | Generally available, September 2026 | MCP host with Streamable HTTP and Entra authentication | [Microsoft GA listing](https://learn.microsoft.com/en-us/fabric/fundamentals/whats-new), [connection guide](https://learn.microsoft.com/en-us/rest/api/fabric/articles/mcp-servers/core-remote/get-started-core) |
| `fab` (`ms-fabric-cli`) | 1.9.0 | `>=3.10,<3.15` | [PyPI](https://pypi.org/project/ms-fabric-cli/1.9.0/), installed CLI version/help and a read-only `fab api` call |
| `fabric-cicd` | 1.4.0 | `>=3.9,<3.15` | [PyPI](https://pypi.org/project/fabric-cicd/1.4.0/), successfully installed on Python 3.14.8 |
| `fabric-data-agent-sdk` | 0.1.36a0 (prerelease) | `>=3.10,<3.14` | [PyPI](https://pypi.org/project/fabric-data-agent-sdk/0.1.36a0/) metadata; not installed or run in this refresh |
| Azure CLI `microsoft-fabric` extension | 1.0.0b1 (preview) | Azure CLI extension | Installed in a temporary extension directory; `az fabric capacity --help` |
| `semantic-link-labs` | 0.18.1 | `>=3.10,<3.14` | [PyPI](https://pypi.org/project/semantic-link-labs/0.18.1/) metadata; not installed or run |

The local MCP changelog returned stale content through the web cache (1.8.0
unreleased). A direct GitHub request confirmed 1.8.0's release and the live
changelog's next unreleased version, 1.9.0. Use the dated release as evidence.
The remote server is a Microsoft-hosted endpoint, not a locally versioned pip
package; no MCP server was installed or probed.
The existing development environment has Python package `mcp` 2.3.0; client
transport and published-agent task checks remain U8 work.

`fab` 1.9.0 exposes `api`, `ls`, create/update/delete commands, `export`,
`bulk-export`, `import`, `deploy`, `job`, `table`, `acl`, `assign` and `unassign`.
Its `api` command successfully read disposable-workspace metadata using the
existing Azure CLI identity. These capabilities overlap interactive operations;
their presence does not prove byte-for-byte artifact compatibility.

The tools have material dependencies. `ms-fabric-cli` pulls in fabric-cicd,
MSAL, Azure Identity, cryptography and interactive-shell packages. fabric-cicd
uses Azure Identity, PyYAML, requests, dpath and jsonpath-ng. The data-agent SDK
pulls in sempy, semantic-link-labs, Fabric analytics packages, OpenAI, Delta and
OneLake packages. Its `azure-identity==1.17.1` pin conflicts with the CLI's
`azure-identity>=1.25.0` requirement. Use separate environments rather than
promising that every complementary extra resolves together; keep heavy tools
out of pyfabric's core dependency set.

See [live validation](live-validation.md) for safety gates, baseline counts,
the F2 Environment limitation and the corrected REST metadata findings.

## Evidence boundaries

Microsoft's [current GA listing](https://learn.microsoft.com/en-us/fabric/fundamentals/whats-new)
also lists bulk item-definition APIs as generally available in September 2026.
The plan's preview label is superseded by this primary source. No bulk request
was made: bulk implementation remains gated on the owner's U7 scope decision,
followed by its own specification reading and live verification. API availability
in this tenant is unverified; a documentation status is not a live result.

The deployment-plan request shape, parameter schema, new item fixtures, and
MCP task support are not established by this baseline. Their respective units
must perform their own checks. The data-agent SDK was inspected through package
metadata only; it was not installed into Python 3.14 or used against data.

## Item-type contract check

The offline check keeps the normalizer's recognized suffixes equal to the
registry. Its initial failure identified `Warehouse`, `Ontology` and `Map`.
Three public-writer regressions also failed: an outer `.DataAgent` directory
could be mistaken for the item's ancestor and select CRLF rules. Adding those
three existing registry names fixed those initial failures. The initial
focused registry/normalizer suite passed 83 tests with one visible network
opt-in skip.

When no workspace root is supplied, path-based normalization now prefers the
nearest recognized ancestor with a `.platform` file over a directory that
only looks like an item by suffix. It uses the nearest recognized suffix as
a fallback for unmarked artifacts. An item's own `.platform` uses the
containing item folder even on the first write. Six public-path regression
cases cover these behaviors.

Run the upstream comparison explicitly:

```powershell
$env:PYFABRIC_TEST_FABRIC_SPECS = '1'
python -m pytest -o addopts='' -ra tests/contract/test_item_types.py
```

At 2026-11-19 07:42:10 UTC it fetched Microsoft's
[`common/definitions.json` ItemType enum](https://github.com/microsoft/fabric-rest-api-specs/blob/main/common/definitions.json)
and reported **47 missing types, zero extra types**. This shared enum is used
by workspace Item and CreateItemRequest schemas. It avoids treating every
service directory as an item type. The opt-in comparison currently fails by
design on the documented unsupported set; do not enable it as a required
offline CI gate. A fetch failure skips visibly rather than reporting parity.

Types absent from pyfabric's registry:

- AnomalyDetector, ApacheAirflowJob, AppBackend, AzureDatabricksStorage, CopyJob,
  CosmosDBDatabase, CustomStreamConnector, Dashboard, DataBuildToolJob, Datamart
- DeploymentPlan, DigitalTwinBuilder, DigitalTwinBuilderFlow, EventSchemaSet,
  Eventhouse, Eventstream, GraphModel, GraphQLApi, GraphQuerySet
- KQLDashboard, KQLDatabase, KQLQueryset, MLExperiment, MLModel,
  MirroredAzureDatabricksCatalog, MirroredCatalog, MirroredOneLake,
  MirroredWarehouse, Monitoring, MountedDataFactory
- OperationsAgent, OrgApp, OrgAppAudience, PaginatedReport, Plan, Playbook,
  PolicySet, PostgreSQLDatabase, Reflex, SQLDatabase, SQLEndpoint, Share,
  ShareSubscription, SnowflakeDatabase, SparkJobDefinition, UserDataFunction,
  WarehouseSnapshot

These are spec inventory gaps, not proof that every type supports git sync or
that a valid artifact can be inferred from its name. New registrations remain
U4 work and require canonical live fixtures and known required files. In
particular, the plan's Cosmos DB candidate has the wire spelling
`CosmosDBDatabase`.

## Retirement decisions

The refresh follows R14: first deprecate with a warning and replacement pointer,
then consider removal in a later release. No candidate below has been shown to
meet all three retirement criteria, so this change adds no deprecation warnings
and removes no public API.

All three conditions are required to retire a surface:

1. A first-party tool covers the same behavior, verified by running it.
2. pyfabric adds no distinguishing guarantee needed by a human-runnable Python
   workflow, such as local testability, artifact normalization or composition
   through an existing client.
3. No documented consumer workflow breaks. Search consumers before deprecating;
   absence of a search match alone is not proof of non-use.

| Surface | Decision | Evidence and reason |
| --- | --- | --- |
| `src/pyfabric/workspace/workspaces.py` | Keep | Provides the callable Python workspace API used by the shipped listing example and tests, with an injectable `FabricClient`. `fab api` demonstrated read-only REST access, but no complete workspace mutation/role equivalence or consumer migration was verified. A shell/MCP replacement alone does not preserve that Python workflow. |
| `src/pyfabric/items/crud.py` | Keep | Consumer build, cleanup and definition-inspection scripts import these functions. Removing them breaks those workflows. Generic first-party item operations overlap, but this run did not establish a compatible Python replacement. |
| `src/pyfabric/items/jobs.py` | Keep | Consumer notebook-deployment/evaluation scripts call `run_notebook`. CLI job commands do not replace that imported Python contract. No first-party job execution was performed for equivalence in this baseline. |
| `src/pyfabric/deploy.py` | Keep | Retains the documented/tested Python `publish_repo`, dependency checks, staged parameter substitution and orphan handling. The create/update/delete lifecycle passed live. `fab deploy` and fabric-cicd overlap deployment; this baseline did not prove an equivalent migration of these contracts. Python 3.14 support is no longer a reason to prefer pyfabric. |

The local source-import search covered Creative Planning checkouts while
excluding virtual environments, vendored code and pyfabric's own checkouts/tests.
Concrete consumers include da-fabric's `demo/scripts/build_agents.py`,
`cleanup.py`, `show_columns.py` and `deploy_eval_notebook.py`; another local
consumer has the same CRUD/jobs imports. This is evidence of use, not an
exhaustive census of remote, dynamic or notebook callers.

Revisit a keep decision when a named consumer can migrate and the replacement
preserves its required Python behavior. Run targeted equivalence checks before
deprecating. A capability list, or an API being newer, is insufficient evidence
for removal.
