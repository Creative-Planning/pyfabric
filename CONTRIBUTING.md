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

### Commit Signing

The `main` branch requires signed commits. Because all PRs are squash
merged through the GitHub UI, **GitHub signs the squash commit
automatically** — you do not need to configure commit signing on your
machine to contribute via pull requests.

If you want verified signatures on your own branch commits (optional),
configure SSH signing:

1. Generate or import an ed25519 SSH key
2. Add the public key to your GitHub account as a **signing key**
   (Settings > SSH and GPG keys > New SSH key > Key type: Signing key)
3. Configure git:
   ```bash
   git config --global gpg.format ssh
   git config --global user.signingkey "key::ssh-ed25519 AAAA...your-public-key..."
   git config --global commit.gpgsign true
   ```
4. On Windows, point git at the Windows OpenSSH binaries so it can reach
   the system SSH agent:
   ```bash
   git config --global core.sshCommand "C:/Windows/System32/OpenSSH/ssh.exe"
   git config --global gpg.ssh.program "C:/Windows/System32/OpenSSH/ssh-keygen.exe"
   ```
5. Ensure the Windows OpenSSH agent service is running (requires
   administrator PowerShell):
   ```powershell
   Set-Service ssh-agent -StartupType Automatic
   Start-Service ssh-agent
   ```

### Pull Request Requirements

All PRs require:

- Passing CI (lint, type check, tests, dependency review)
- One approving review from a code owner
- Signed commits on `main` (handled automatically by squash merge)

### Releasing

1. Merge all changes to `main`.
2. Create a git tag: `git tag v0.1.0`
3. Push the tag: `git push origin v0.1.0`
4. Create a GitHub Release from the tag.
5. The publish workflow handles PyPI upload, SBOM generation, and
   SLSA attestation automatically.
