# Contributing

This project is maintained exclusively by the Creative Planning engineering
team. We do not accept external pull requests, issues, or feature requests.

You are welcome to fork this repository and modify it under the terms of
the [MIT License](LICENSE).

---

## For Creative Planning Team Members

### Prerequisites

- **Python 3.12+** — required by the project and CI
- **Git** — for cloning and the pre-commit hook system
- **pre-commit** — installed automatically with `pip install -e ".[dev]"`, or standalone via `pip install pre-commit`

### Development Setup

```bash
git clone https://github.com/Creative-Planning/pyfabric.git
cd pyfabric
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"

# Install git hooks (required — CI will reject what these hooks catch)
pre-commit install
pre-commit install --hook-type pre-push
```

The pre-commit hooks run automatically:

| Hook | Runs on | What it does |
|------|---------|--------------|
| `ruff check` | `git commit` | Lint with auto-fix |
| `ruff format` | `git commit` | Format check |
| `mypy` | `git commit` | Type checking |
| `pytest` | `git push` | Full test suite |

To run all hooks manually against the full repo:

```bash
pre-commit run --all-files       # commit-stage hooks
pre-commit run --all-files --hook-stage pre-push  # push-stage hooks
```

### Running Checks Individually

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

### Tracking work (Claude sessions + human)

Cross-session tracking lives on the [Issues tab](https://github.com/Creative-Planning/pyfabric/issues)
— a single source of truth visible to every Claude session and human
contributor. Before proposing new work:

1. Run `gh issue list` (or browse the Issues tab) and check if it's
   already tracked. If yes, comment on that issue with new context or
   open a PR that references it (`Fixes #N` / `Refs #N`).
2. For **new library features or bug fixes**, file an
   [Issue](https://github.com/Creative-Planning/pyfabric/issues/new)
   labeled `enhancement` (feature) or `bug` (defect). Describe the
   problem, proposed API, scope estimate, and any real-world incident
   that motivates the work.
3. For **new `src/pyfabric/claude_memory/*.md` docs** (guidance that
   ships with the wheel and lands in consumer projects via
   `pyfabric install-claude-memory`), file an Issue labeled
   `documentation`. The issue describes the guidance; the PR adds
   the file plus the one-line pointer in
   `src/pyfabric/claude_memory/MEMORY.md`. Bump the beta version
   so consumers pick it up on their next install.
4. For **AI-detected cross-session patterns** (something you notice in
   one consumer project that other consumer projects probably hit too),
   file an Issue — do not stash it in a local TODO file or client-
   specific memory. Future sessions need to be able to find it via
   `gh issue list`.

Do not create parallel tracking files in the repo (`TODO.md`,
`ROADMAP.md`, etc.) — issues are authoritative. If you find an old
one, migrate its contents into issues and delete the file.

### Releasing

1. Merge all changes to `main`.
2. Create a GitHub Release via CLI:
   `gh release create v0.1.0 --target main --title "v0.1.0" --prerelease`
3. The publish workflow handles PyPI upload, SBOM generation, and
   SLSA attestation automatically.
