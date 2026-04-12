# pyfabric

[![CI](https://github.com/Creative-Planning/pyfabric/actions/workflows/ci.yml/badge.svg)](https://github.com/Creative-Planning/pyfabric/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/pyfabric)](https://pypi.org/project/pyfabric/)
[![Python](https://img.shields.io/pypi/pyversions/pyfabric)](https://pypi.org/project/pyfabric/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Python libraries helping AI coding assistants create and locally validate
Microsoft Fabric items compatible with Fabric git sync.

## Installation

```bash
pip install pyfabric
```

### Optional dependencies

```bash
pip install pyfabric[azure]    # Azure auth + REST client
pip install pyfabric[data]     # OneLake + SQL data access
pip install pyfabric[testing]  # DuckDB-based local testing fixtures
pip install pyfabric[all]      # Everything
```

## Overview

pyfabric provides utilities for:

- Creating Microsoft Fabric item definitions (notebooks, lakehouses,
  semantic models, environments, variable libraries, pipelines, etc.)
  programmatically
- Validating item structures locally before committing to a Fabric
  git-synced repository
- Running notebook and pipeline transformations locally against DuckDB
  to verify data logic without deploying to Fabric
- Generating correct `.platform` files, directory layouts, and metadata
  that Fabric git sync expects

## Requirements

- Python 3.12 or later

## Contributing

This project is maintained by [Creative Planning](https://www.creativeplanning.com).
We do not accept external contributions at this time. You are welcome to fork and
modify under the MIT license. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
