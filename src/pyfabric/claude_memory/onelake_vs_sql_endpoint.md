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
  seconds to minutes behind the Delta files during mirror refresh.
- SQL endpoint requires ODBC handshake + AAD token on first query; OneLake
  needs a single ABFS `list` call for discovery.
- Columnar projection can reduce reads for exploratory single-table scans.

**When to reach for `FabricSql` instead:**

- Cross-table joins that would be awkward with DuckDB ATTACH.
- WHERE-clause pushdown where server-side filtering matters for performance.
- Testing the published SQL endpoint path itself (e.g., validating a
  downstream semantic model's query behavior).

**Quick reference:**

```python
# docs-snippet: network
from deltalake import DeltaTable

from pyfabric.data import LocalLakehouse
from pyfabric.data.onelake import abfss_url
from pyfabric.data.sql import FabricSql

# Inspect the Delta schema without scanning table rows.
url = abfss_url(WS, LH, "Tables/dbo/customers")
schema = DeltaTable(url, storage_options={"bearer_token": cred.storage_token}).schema()

# Pull a synthetic table into local DuckDB and query it.
with LocalLakehouse(":memory:", WS, LH, schema="dbo") as lh:
    lh.pull_table(cred, "customers")
    counts = lh.query_df("SELECT COUNT(*) FROM dbo.customers")

# Exercise the SQL endpoint's own query path.
sql = FabricSql(server=sql_endpoint, database=database_name, credential=cred)
df = sql.query_df("SELECT a.id, b.name FROM a JOIN b ON a.id = b.id")
```
