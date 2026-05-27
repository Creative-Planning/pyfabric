# Execution plan

Phased plan for working off the currently-open issue list, sequenced by
reliability impact and by what each phase unlocks for downstream
consumers running pyfabric at production scale.

The four-phase roadmap in [`roadmap.md`](roadmap.md) describes thematic
direction. This document is concrete: open issues, ordered, with an
explicit dependency chain. Update as issues land or new ones are filed.

## Sequencing rationale

- **Reliability before everything else.** Multi-environment promotion,
  new builders, and downstream automation all amplify the blast radius
  of any latent correctness bug. Bugs that are merely annoying in
  single-workspace use become outages when fanned out.
- **Authoring depth before deployment layer.** Cleaner builder outputs
  make the fabric-cicd interop story cheaper — the publish layer reads
  what we emit, so emitting the right thing closes interop gaps
  before they're filed as bugs against the consumer.
- **Builders before pipelines that orchestrate them.** DataPipeline and
  DataAgent builders are most valuable once promotion across
  environments is solved, otherwise they get refactored for the
  parameterization model right after they ship.

## Phase A — Reliability bugs

Land first. Each is small surface, high return.

**Status:** four of the five Phase A items (#64, #66, #67, #69) were
already resolved by PR #76 ("small fixes", merged 2026-04-28) and just
hadn't been auto-closed. Verified on `main` at PR-92 merge: targeted
test subset is 66 passed in 3.42s; the four issues are closing with
references back to PR #76. Only #91 remains.

| Issue | Status | Symptom | Fix shape |
| ----- | ------ | ------- | --------- |
| [#91](https://github.com/Creative-Planning/pyfabric/issues/91) | **Open** | `ArtifactBundle.save_to_disk()` and the writers in `src/pyfabric/items/bundle.py` still call `Path.write_text` / `Path.write_bytes` directly instead of `write_artifact_file`. Notebook, SemanticModel, Report, and Environment builders all migrated; bundle.py is the last bypass and represents a re-introduction risk for the byte-flap class of bug normalization was built to prevent. | Migrate every direct write in `bundle.py` (lines 102, 111, 113) to `pyfabric.items.normalize.write_artifact_file`. Add a unit test that loads a bundle, saves it, and asserts every emitted file passes `is_canonical`. |
| [#66](https://github.com/Creative-Planning/pyfabric/issues/66) | **Done** (PR #76) | `run_notebook._poll_lro` looped forever because the Jobs API returns `Completed` not `Succeeded` | `src/pyfabric/client/http.py:160` accepts both terminal states. |
| [#67](https://github.com/Creative-Planning/pyfabric/issues/67) | **Done** (PR #76) | `write_rows(expected_schema=...)` failed on auto-stamped tables — schema check ran after `__rowMarker__` was appended | `src/pyfabric/data/open_mirror.py:479` runs `assert_schema_compat` pre-stamp. |
| [#69](https://github.com/Creative-Planning/pyfabric/issues/69) | **Done** (PR #76) | `LocalLakehouse.evolve_schema` required `TableDef.column(name)` method access | Implementation iterates `table.columns` and uses `.name` attribute access. |
| [#64](https://github.com/Creative-Planning/pyfabric/issues/64) | **Done** (PR #76) | `decode_part` API pairing with `encode_part` was confusing; wrong input raised unhelpful `TypeError` | `src/pyfabric/items/crud.py:210` guards with a clear error message naming the right call shape. |

Done-when: #91 closes, CI green, no new bypasses of `write_artifact_file` in `src/`.

## Phase B — Notebook + Environment authoring

Parallel with A. Different module, different reviewers, independent
test surface.

**Status:** like Phase A, the bulk of Phase B was already in `main`
before this plan was written and the issues just weren't auto-closed.
PR #79 shipped `attach_environment` + `notebook-settings.json` (#73).
PR #80 shipped the `EnvironmentBuilder` + REST lifecycle (#74). PR #94
adds `add_parameters_cell` (#78) plus the first live REST-based E2E
test pattern (`PYFABRIC_TEST_WORKSPACE_ID`-gated round-trip against a
validation workspace) that the rest of the codebase can build on.

| Issue | Status | Outcome |
| ----- | ------ | ------- |
| [#78](https://github.com/Creative-Planning/pyfabric/issues/78) | **Done** (PR #94) | `NotebookBuilder.add_parameters_cell(code)` emits `# PARAMETERS CELL ********************`. Live round-trip test against a validation workspace confirms the marker survives Fabric's canonicalization. |
| [#73](https://github.com/Creative-Planning/pyfabric/issues/73) | **Done** (PR #79) | `NotebookBuilder.attach_environment(env_id, ws_id=None)` emits the dependencies.environment block; `save_to_disk` / `to_bundle` emit `notebook-settings.json` unconditionally. |
| [#74](https://github.com/Creative-Planning/pyfabric/issues/74) | **Done** (PR #80) | `EnvironmentBuilder` (runtime/compute/pip) + `publish_environment` / `get_environment_status` / `wait_for_published`. Sparkcompute.yml CRLF + trailing-CRLF and environment.yml LF + no-trailing-newline rules live in `normalize._RULES`. |

Done-when: a downstream consumer can describe a Notebook + Environment
pair entirely through builders, with no hand-written
`notebook-settings.json` / `fs-settings.json` / `Environment.platform`. ✓

## Phase C — `pyfabric.deploy` (repo-to-workspace deployment)

Gated on Phase A. Originally scoped as "fabric-cicd interoperability"
but pivoted after the live spike against a validation workspace
surfaced three findings that made the interop story weak:

1. **fabric-cicd does not run on Python 3.14** (the latest 1.1.0
   requires `<3.14`). pyfabric does. Until upstream catches up,
   fabric-cicd users on Python 3.14 have no path.
2. **`.platform` does not byte-roundtrip through any REST publish**,
   whether via fabric-cicd or pyfabric's own `upload_to_workspace`.
   Fabric adds `"description": ""` to metadata and zeros the pinned
   `logicalId` in `getDefinition` responses. The "byte-roundtrip"
   claim in the original Phase C plan was empirically false for
   metadata files.
3. **logical_id pinning is git-sync-only**, not REST-publish. This
   wasn't fabric-cicd's fault — it's a property of Fabric's REST
   surface — but it invalidated the "pinned IDs survive deployment"
   selling point.

Net: fabric-cicd's unique value reduced to its repo-walking publish,
multi-env parameter substitution, and orphan-delete helpers. Each is
small (~50–100 LOC) on top of primitives pyfabric already exposes
(`load_from_disk`, `upload_to_workspace`, `list_items`, `delete_item`).
Building this in pyfabric removes the Python version gap, gives one
auth surface, and keeps the canonicalization guarantees consistent
end-to-end.

| Deliverable | Status | Notes |
| ----------- | ------ | ----- |
| `src/pyfabric/deploy.py` — `publish_repo` + `unpublish_orphans` | **Done** (this PR) | Composes existing primitives; fail-fast on errors; `item_types_in_scope` safety rail on unpublish. |
| `tests/test_deploy.py` — 14 unit tests with mocked client | **Done** (this PR) | Covers discovery, create-vs-update logic, scope filtering, dry-run. |
| `tests/test_deploy_e2e.py` — live full-lifecycle test | **Done** (this PR) | Gated on `PYFABRIC_TEST_WORKSPACE_ID`. Exercises create → update → dry-run-orphan → unpublish → verify-survivor + cleanup. ~88s end-to-end. |
| `docs/deploy.md` | **Done** (this PR) | API reference, recommended flow, REST-publish gotchas (the `.platform` mutation), out-of-scope notes. |
| Parameter substitution (`pyfabric.deploy.substitute_parameters`) | **Open** — follow-up | Match the `parameter.yml` `find_replace` schema from fabric-cicd so consumers can migrate. Not blocking the deployment story. |
| Dependency ordering on publish | **Open** — follow-up | v1 publishes in directory order; works for typical layouts. Add topological sort if/when a real consumer hits a "B references A" failure. |

Done-when: `pyfabric.deploy` is the documented and tested path for
repo-to-workspace deployment, and the `feedback_pyfabric_logical_id_pinning`
memory carries the REST-publish caveat from finding #3. ✓

## Phase D — SemanticModel + governance

Parallel with C. Pure additive.

| Issue | Outcome |
| ----- | ------- |
| [#89](https://github.com/Creative-Planning/pyfabric/issues/89) | `SemanticModel` builder gains `SqlDatabaseSource` for DirectLake over the `Sql.Database` endpoint. Enables consumers to move from import semantic models to DirectLake without leaving the builder API. |
| [#90](https://github.com/Creative-Planning/pyfabric/issues/90) | Ship a default claude-memory entry establishing no-production-data-to-cloud-AI governance so consumers wiring up data-agent / LLM patterns inherit the safe default. |

Done-when: a DirectLake semantic model can be authored entirely
through the builder, and `pyfabric install-claude-memory` installs the
governance entry by default.

## Phase E — Phase 2 builder expansion

After C. These are large net-new builders, not currently filed as
issues; file them first so the work is visible.

- **DataPipeline builder.** Activity graph authoring: notebook
  activity, copy activity, dataflow activity, plus dependency edges,
  parameters, and the v2 expression language for parameterized values.
  Take cues from the JSON shape that Fabric's pipeline designer
  produces; cover the activity types that show up most often in
  production deployments first.
- **DataAgent builder.** Programmatic data agent definitions paired
  with the governance memory from Phase D so the default posture is
  safe.

These are roadmap.md Phase 2 items; promoting them to concrete
deliverables here once issues are filed.

Done-when: both builders land, both have docs in `docs/api.md`, both
have at least one example in `examples/`, both are emitted through
`write_artifact_file`.

## Phase F — Docs + tests cleanup

Continuous, not a separate budget. Land alongside the code that
motivates each entry.

| Issue | Notes |
| ----- | ----- |
| [#68](https://github.com/Creative-Planning/pyfabric/issues/68) | claude-memory: notebook Resources/builtin wheel pattern. Pair with Phase B `#73`. |
| [#71](https://github.com/Creative-Planning/pyfabric/issues/71) | `open_mirror_landing_zone.md` anti-patterns + caller-stamped CDC. Standalone doc work. |
| [#52](https://github.com/Creative-Planning/pyfabric/issues/52) | Live DDL integration tests against a dedicated validation workspace. Same workspace can host the Phase C roundtrip test. |
| [#55](https://github.com/Creative-Planning/pyfabric/issues/55) | `NotebookBuilder.add_sparksql` — explicitly blocked on a real-world fixture. Defer until one lands. |
| [#42](https://github.com/Creative-Planning/pyfabric/issues/42) | Tier 3: DirectLake partition swapper, TMDL linter, sempy bridge. Tackle after C; the partition swapper composes well with the `SqlDatabaseSource` work from D. |

## Dependency graph

```text
A (reliability)  ─────────┐
                          │
B (notebook + env)  ──────┤
                          │
                          ├──► C (fabric-cicd interop)
                          │           │
                          │           ├──► E (DataPipeline + DataAgent)
                          │
D (semantic model + gov)  ─┘
                                      │
F (docs + tests)  ── continuous ──────┘
```

## Rough effort estimate

| Phase | Effort |
| ----- | ------ |
| A | 5 PRs, mostly small. 1–2 weeks. |
| B | 3 PRs, medium. 1–2 weeks. |
| C | Integration test + docs, plus any upstream fabric-cicd issues that surface. 2 weeks. |
| D | 2 PRs (one code, one memory). 1 week. |
| E | 2 builders, large. 3–4 weeks. |
| F | Continuous, no separate budget. |

Time-to-"production-promotion-ready": A + B + C + D ≈ 5–7 weeks of
pyfabric throughput. E is the next stage after that.
