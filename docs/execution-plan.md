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

| Issue | Outcome |
| ----- | ------- |
| [#73](https://github.com/Creative-Planning/pyfabric/issues/73) | `NotebookBuilder.attach_environment()` plus emission of `notebook-settings.json`. Removes the need for consumers to hand-author the settings file that gates `Resources/` git-sync inclusion. |
| [#74](https://github.com/Creative-Planning/pyfabric/issues/74) | `EnvironmentBuilder` + REST lifecycle (`publish`, `get_status`, `wait_for_published`). Replaces the hand-maintained Environment artifact pattern with a builder + driver. |
| [#78](https://github.com/Creative-Planning/pyfabric/issues/78) | `NotebookBuilder.add_parameters_cell()` (or `parameters=True` kwarg). Required input for any deployment story that parameterizes notebook execution across environments. |

Done-when: a downstream consumer can describe a Notebook + Environment
pair entirely through builders, with no hand-written
`notebook-settings.json` / `fs-settings.json` / `Environment.platform`.

## Phase C — fabric-cicd interoperability

Gated on Phase A. Don't promote buggy code across environments.

fabric-cicd v1.0.0 is now Microsoft's officially supported deployment
tool. The pyfabric position: stay the authoring + local-test layer;
make sure what we emit round-trips cleanly through a fabric-cicd
publish. Concretely:

1. **Pre-publish lint integration.** Document the pattern for calling
   `pyfabric.items.normalize.normalize_tree(repo_root, dry_run=True)`
   and `is_canonical(path)` from a pre-fabric-cicd-publish CI step.
   Goal: catch byte-canonicalization drift before fabric-cicd ships
   non-canonical bytes that come back as drift on the next round-trip.
2. **Byte-roundtrip test.** Add an integration test
   (`tests/integration/test_fabric_cicd_roundtrip.py`, gated on an
   env var) that publishes a builder-generated artifact via fabric-cicd
   into a validation workspace, downloads it back via the item
   definition REST API, and asserts byte equality with what
   `write_artifact_file` produced locally. Any divergence is either a
   pyfabric normalization gap or a fabric-cicd issue to file upstream.
3. **`logical_id` preservation evidence.** Existing builders already
   pin `logical_id` (see the [logical-id pinning
   memory](claude-memory.d/logicalid-pinning.md) if installed). The
   roundtrip test above doubles as evidence that fabric-cicd preserves
   pinned IDs through publish/download.
4. **Docs update.** New section in `docs/api.md` (or a sibling
   `docs/fabric-cicd-interop.md`) describing the integration shape so
   downstream teams have one place to look.

Done-when: integration test green against a validation workspace, docs
landed, and any divergence between pyfabric canonical bytes and
fabric-cicd output is either fixed in pyfabric or filed upstream.

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
