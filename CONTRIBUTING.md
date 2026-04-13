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

### Branch Workflow

1. Create a feature branch from `main`
2. Make your changes and commit locally
3. Push your branch and open a pull request against `main`
4. CI checks must pass (lint, format, type check, tests, dependency review)
5. A CODEOWNERS review is required
6. PRs are **squash merged** — your commits are combined into one clean
   commit on `main` with a linear history

Squash merge is the only merge strategy enabled on this repo.

### Pull Request Requirements

All PRs require:

- Passing CI (lint, type check, tests, dependency review)
- One approving review from a code owner

All PRs are squash merged, so GitHub signs the resulting commit on `main`
automatically. Local commit signing is not required.

### Releasing

1. Merge all changes to `main`.
2. Create a GitHub Release via CLI:
   `gh release create v0.1.0 --target main --title "v0.1.0" --prerelease`
3. The publish workflow handles PyPI upload, SBOM generation, and
   SLSA attestation automatically.
