---
name: pyfabric — Open Mirroring landing-zone protocol
description: Path shape, _metadata.json keys, __rowMarker__ semantics, and schema-evolution rules for pushing parquet into a Fabric Open Mirror. Read this before writing a producer or hand-rolling landing-zone bytes.
type: reference
---

Protocol summary derived from the public Microsoft Fabric documentation
and from the research notes published at
<https://github.com/UnifiedEducation/research/tree/main/open-mirroring>
by the *Learn Microsoft Fabric* community. This file is independently
authored; the research repo is the recommended companion read for the
*why* behind each rule.

pyfabric's API for these operations lives in:

- `pyfabric.items.mirrored_database` — item plane (build the
  MirroredDatabase artifact, REST lifecycle: create / start / stop /
  status / wait_for_running).
- `pyfabric.data.open_mirror` — data plane (`OpenMirrorClient` with
  `ensure_table`, `next_data_filename`, `upload_data_file`,
  `write_rows`, `list_processed`; plus `RowMarker` enum and
  `assert_schema_compat`).

Use those rather than re-deriving the protocol from this doc.

## Path shape

```text
Files/LandingZone/[<schema>.schema/]<table>/_metadata.json
Files/LandingZone/[<schema>.schema/]<table>/<NNNNNNNNNNNNNNNNNNNN>.parquet
```

- `<schema>.schema/` is **optional**. Use it to namespace multiple
  sources inside one mirror (e.g. `youtube.schema/` and `skool.schema/`
  side by side).
- `<table>/` — one folder per logical table. Creating the folder (or
  uploading the first file under it) creates the table in the mirror's
  Delta layer.
- Sequential filenames are zero-padded to **exactly 20 digits**. The
  mirror reads files in numeric order unless `_metadata.json` opts in
  to last-update-time detection.

## `_metadata.json`

```json
{
  "keyColumns": ["id"],
  "isUpsertDefaultRowMarker": true,
  "fileDetectionStrategy": "LastUpdateTimeFileDetection"
}
```

- `keyColumns` — **required.** Columns that uniquely identify a row.
  Fabric uses these to resolve update / delete / upsert markers.
- `isUpsertDefaultRowMarker` — when true, rows that omit
  `__rowMarker__` are treated as upserts (4) by default.
- `fileDetectionStrategy: "LastUpdateTimeFileDetection"` — read files
  in last-modified order instead of requiring sequential 20-digit
  names. Drop the producer's sequence-number bookkeeping.

Without `_metadata.json`, updates and deletes silently don't work.

## `__rowMarker__` semantics

`__rowMarker__` must be the **last column** of every parquet file.
Values:

| Value | Meaning | Required columns |
|---|---|---|
| 0 | Insert | full row |
| 1 | Update | full row |
| 2 | Delete | key columns only |
| 4 | Upsert | full row |

For a key change, write a delete row and an insert row in the same
file (delete-of-old then insert-of-new).

Updates need the **full row**, not just changed columns.

## Schema evolution rules

**Adding a nullable column is zero-downtime.** Update the producer's
`pa.schema([...])`, deploy, write the next file. Mirror unions the
Delta schema within ~30s; old rows show NULL for the new column.

**Hard rules (each one fires `SchemaMergeFailure` and stops
replication on that table):**

| Don't | Why |
|---|---|
| Change a column's type (`int32 → int64`, `date32 → timestamp`) | Mirror rewriter rejects merges. |
| Move a column past `__rowMarker__` | Marker must remain last. |
| Add a column declared NOT NULL | Existing rows can't satisfy it. |
| Rename a column in place | Treated as drop + add — old rows lose the value. |

**Soft rules:**

- Removing a column is non-destructive: Fabric keeps the unioned
  column and NULLs new rows. To truly drop, you have to drop the
  whole folder (loses history).
- Backfill new column values on historical rows by writing a
  follow-up file with `__rowMarker__=1` for each row, full row
  payload included.

`pyfabric.data.open_mirror.assert_schema_compat(old, new)` runs these
checks offline so a producer's pre-commit catches drift before the
file lands in Fabric.

## Landing-zone retention

The landing zone is **transient**, not a durable store:

- Just-uploaded files: under the table folder until processed
  (seconds–minutes).
- Processed files: moved to `<table>/_ProcessedFiles/`, then
  `<table>/_FilesReadyToDelete/`, then auto-purged after **7 days**.
- Latest file stays in place so producers can find the next sequence
  number.

If you need raw payloads later (audit, replay), copy them into your
own OneLake folder on upload — the mirror's copy is gone within 7
days.

For recovery, use Delta time-travel on the mirrored table
(`VERSION AS OF ...`), not landing-zone files.

## Producer checklist

1. Acquire a token via `pyfabric.client.auth.FabricCredential`.
2. Construct `OpenMirrorClient(cred, ws_id, mirror_id)`.
3. `ensure_table(table, schema=…, key_columns=[…])` (once per
   producer run is fine — it's an overwrite).
4. Build a parquet with the producer's pinned `pa.schema([…])` —
   never let pandas type-inference pick column types; that's how
   `int32 → int64` widening sneaks in. Cast each column explicitly
   before constructing the arrow table:

   ```python
   PINNED = pa.schema([
       pa.field("employee_id", pa.string(),                 nullable=False),
       pa.field("date",        pa.date32(),                 nullable=False),
       pa.field("hours",       pa.float64(),                nullable=True),
       pa.field("fetched_at",  pa.timestamp("us", tz="UTC"), nullable=False),
   ])

   pdf = df.toPandas()
   pdf["date"]       = pd.to_datetime(pdf["date"]).dt.date
   pdf["hours"]      = pd.to_numeric(pdf["hours"], errors="coerce")
   pdf["fetched_at"] = pd.Timestamp.now(tz="UTC")
   arrow = pa.Table.from_pandas(pdf, schema=PINNED, safe=True)
   ```

5. `client.write_rows(table, arrow, schema=…, mode="upsert")` (or
   another mode), or use `upload_data_file` with a pre-built parquet.

   - `expected_schema` is optional. If supplied under
     `mode != None`, pass the **producer's natural schema** (no
     `__rowMarker__`); pyfabric ≥ v0.1.0rc1 compares against the
     pre-stamp shape. On older releases the check ran post-stamp
     and either rejected legitimate writes or hid drift; validate
     types in the producer's own `build_X_arrow_table` step in
     that case.

6. Visibility — observed end-to-end latency from a healthy mirror:

   | Stage | Typical latency |
   |---|---|
   | Upload file to landing zone | < 5 seconds |
   | Landing zone → mirror sync cycle | 2 – 10 min (periodic; not manually triggerable) |
   | Sync → SQL endpoint visible | 1 – 3 min |
   | **Total: upload → SQL** | **3 – 15 min** |

   If a row hasn't appeared in 20 min, check the mirror's status
   in the portal before assuming the producer is at fault.

## Caller-stamped CDC (mixed INSERT / UPDATE / DELETE in one file)

When the source already provides a change-type column, stamp
`__rowMarker__` yourself and pass `mode=None` to `write_rows`. The
auto-stamp path can only emit a single marker per file.

```python
_CHANGE_TYPE_MAP = {"INSERT": 0, "UPDATE": 1, "DELETE": 2, "UPSERT": 4}
markers = [_CHANGE_TYPE_MAP[v] for v in pdf["change_type"].str.upper()]

arrow_table = arrow_table.append_column(
    "__rowMarker__",
    pa.array(markers, type=pa.int32()),
)

mirror.write_rows(table, arrow_table, schema=..., mode=None)
```

Requirements:

- `__rowMarker__` must be the **last column**. `write_rows` raises
  `ValueError` otherwise.
- Delete rows need only the key columns populated; the rest may be
  null. Insert / update / upsert rows need the full row.
- Don't combine `mode=...` with a pre-existing `__rowMarker__`
  column — `write_rows` raises `ValueError` rather than silently
  overwriting.

## What this does NOT cover

- Authoring the `MirroredDatabase` git-sync item — see
  `pyfabric.items.mirrored_database.MirroredDatabaseBuilder`.
- Live integration tests for the lifecycle / data plane — tracked by
  the integration-test issue alongside the rest of pyfabric's
  workspace-bound tests.
