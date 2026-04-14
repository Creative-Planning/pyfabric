# pyfabric

[![CI](https://github.com/Creative-Planning/pyfabric/actions/workflows/ci.yml/badge.svg)](https://github.com/Creative-Planning/pyfabric/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/pyfabric)](https://pypi.org/project/pyfabric/)
[![Python](https://img.shields.io/pypi/pyversions/pyfabric)](https://pypi.org/project/pyfabric/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Python library for creating, validating, and locally testing Microsoft Fabric
items that are compatible with Fabric git sync.

## Installation

```bash
pip install pyfabric
```

### Optional dependencies

```bash
pip install pyfabric[azure]    # Azure authentication and REST client
pip install pyfabric[data]     # OneLake and SQL data access
pip install pyfabric[testing]  # DuckDB Spark mock and pytest fixtures
pip install pyfabric[all]      # All optional dependencies
```

## Quick start

### Validate a Fabric workspace

```python
from pathlib import Path
from pyfabric.items.validate import validate_workspace

results = validate_workspace(Path("my_workspace/"))
for r in results:
    status = "OK" if r.valid else "FAIL"
    print(f"{status}: {r.item_path.name}")
    for e in r.errors:
        print(f"  ERROR: {e.message}")
```

### List workspaces with the REST client

```python
from pyfabric.client.auth import FabricCredential
from pyfabric.client.http import FabricClient
from pyfabric.workspace.workspaces import list_workspaces

cred = FabricCredential(tenant="contoso")
client = FabricClient(cred)

for ws in list_workspaces(client):
    print(f"{ws['displayName']}  {ws['id']}")
```

### Test notebook logic locally

```python
# In your test file (pytest)
def test_my_notebook(fabric_spark, mock_notebookutils):
    # fabric_spark is a DuckDB-backed SparkSession replacement
    df = fabric_spark.sql("SELECT 1 AS value, 'hello' AS msg")
    rows = df.collect()
    assert rows[0]["value"] == 1

    # mock_notebookutils replaces Fabric notebookutils
    mock_notebookutils.fs.mkdirs("/output")
    mock_notebookutils.fs.put("/output/result.txt", "done")
```

## Documentation

| Document | Description |
|----------|-------------|
| [Vision](docs/vision.md) | Project mission and design principles |
| [Roadmap](docs/roadmap.md) | Implementation phases and current status |
| [API Reference](docs/api.md) | All sub-packages and their public functions |
| [Testing Guide](docs/testing.md) | Local testing with DuckDB Spark mock and pytest fixtures |
| [AI Prompts](docs/prompts.md) | Sample prompts for Claude and Copilot to create Fabric items |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development setup and release process |
| [CLAUDE.md](CLAUDE.md) | Instructions for AI coding assistants |

## Sub-packages

| Package | Purpose | Install |
|---------|---------|---------|
| `pyfabric.client` | Fabric REST API authentication and HTTP client | `pyfabric[azure]` |
| `pyfabric.items` | Create, validate, and manage Fabric item definitions | (included) |
| `pyfabric.data` | OneLake, SQL endpoint, and lakehouse table operations | `pyfabric[data]` |
| `pyfabric.workspace` | Workspace management (list, create, roles) | `pyfabric[azure]` |
| `pyfabric.testing` | DuckDB Spark mock, notebookutils mock, pytest fixtures | `pyfabric[testing]` |

## Supply Chain Security

Every release of pyfabric includes supply chain security attestations:

- **SLSA Build Provenance** — each release is built in GitHub Actions and
  attested with [SLSA provenance](https://slsa.dev/), verifiable with
  `gh attestation verify`
- **SBOM (SPDX)** — a Software Bill of Materials in SPDX JSON format is
  generated for every release and attached as a release asset
- **PyPI Trusted Publisher** — packages are published to PyPI via
  [OpenID Connect](https://docs.pypi.org/trusted-publishers/) with no
  long-lived credentials
- **Dependency Review** — every PR is scanned for known vulnerabilities
  via `pip-audit` and GitHub's dependency review action

### Verifying a release

```bash
# Verify SLSA provenance of a downloaded package
gh attestation verify pyfabric-*.whl --repo Creative-Planning/pyfabric

# Download the SBOM for a specific release
gh release download v0.1.0a2 --repo Creative-Planning/pyfabric --pattern "*.spdx.json"
```

The SBOM for each release is available at:
`https://github.com/Creative-Planning/pyfabric/releases/download/<tag>/pyfabric-build.spdx.json`

## Requirements

- Python 3.12 or later

## Contributing

This project is maintained by [Creative Planning](https://www.creativeplanning.com).
We do not accept external contributions at this time. You are welcome to fork and
modify under the MIT license. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
