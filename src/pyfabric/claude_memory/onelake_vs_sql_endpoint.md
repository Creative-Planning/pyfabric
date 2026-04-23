---
name: OneLake direct vs SQL analytics endpoint
description: When to use pyfabric.data.onelake / LocalLakehouse vs FabricSql. Critical for MirroredDatabase — SQL endpoint lags behind delta files during mirror refresh.
type: reference
---

Prefer **OneLake direct** (`pyfabric.data.onelake.walk()` + `deltalake.DeltaTable`
or `LocalLakehouse`) over **`FabricSql`** (SQL analytics endpoint) for
discovery-style work: listing tables, inspecting columns, row counts, delta
history, quick scans.

**Why OneLake direct is better for discovery:**

- SQL analytics endpoint has sync latency for `MirroredDatabase` — can lag
  seconds-to-minutes behind the delta files during mirror refresh. OneLake
  reads are up-to-the-second current.
- SQL endpoint requires ODBC handshake + AAD token on first query; OneLake
  needs a single ABFS `list` call for discovery.
- Columnar projection/pushdown on parquet is strictly faster than serverless
  SQL for single-table scans.

**When to reach for `FabricSql` instead:**

- Cross-table joins that would be awkward with DuckDB ATTACH.
- WHERE-clause pushdown where server-side filtering matters for performance.
- Testing the published SQL endpoint path itself (e.g., validating a
  downstream semantic model's query behavior).

**Quick reference:**

```python
# Discovery — prefer this
from pyfabric.data import LocalLakehouse
lh = LocalLakehouse(cred, workspace_id=WS, lakehouse_id=LH)
df = lh.query("SELECT COUNT(*) FROM {table}", table="customers")

# Schema inspection without downloading data
from deltalake import DeltaTable
from pyfabric.data.onelake import abfs_path
schema = DeltaTable(abfs_path(WS, LH, "Tables/customers"), storage_options=...).schema()

# SQL endpoint — for joins or endpoint validation
from pyfabric.data.sql import FabricSql
sql = FabricSql(server=..., database=..., credential=cred)
df = sql.query_df("SELECT a.id, b.name FROM a JOIN b ON a.id = b.id")
```
