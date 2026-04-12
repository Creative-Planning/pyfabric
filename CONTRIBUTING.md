# Contributing

This project is maintained exclusively by the Creative Planning engineering
team. We do not accept external pull requests, issues, or feature requests.

You are welcome to fork this repository and modify it under the terms of
the [MIT License](LICENSE).

---

## For Creative Planning Team Members

### Development Setup

```bash
git clone https://github.com/Creative-Planning/pyfabric.git
cd pyfabric
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"
```

### Running Checks Locally

```bash
ruff check .               # Lint
ruff format --check .      # Format check
ruff format .              # Auto-format
mypy src/                  # Type check
pytest                     # Tests
pytest --cov=pyfabric      # Tests with coverage
pip-audit                  # Vulnerability scan
```

### Pull Request Requirements

All PRs require:

- Passing CI (lint, type check, tests, dependency review)
- One approving review from a code owner

### Releasing

1. Merge all changes to `main`.
2. Create a git tag: `git tag v0.1.0`
3. Push the tag: `git push origin v0.1.0`
4. Create a GitHub Release from the tag.
5. The publish workflow handles PyPI upload, SBOM generation, and
   SLSA attestation automatically.
