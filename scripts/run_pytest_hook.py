"""Run pytest for the pre-push hook, or say plainly why this Python cannot.

The hook runs whichever `python` is first on PATH. pytest's configured options
(`--json-report`) come from the dev extras, so a Python outside the project's
virtual environment fails with "unrecognized arguments", which hides the cause.
This checks the two things the test run needs and names the fix instead.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys

REQUIRED = {
    "pytest_jsonreport": "pytest-json-report (the pytest options in pyproject.toml use it)",
    "pyfabric": "pyfabric itself (an editable install)",
}


def main() -> int:
    missing = [
        label
        for module, label in REQUIRED.items()
        if importlib.util.find_spec(module) is None
    ]
    if missing:
        print(
            f"pre-push: {sys.executable} cannot run the tests. Missing:",
            file=sys.stderr,
        )
        for label in missing:
            print(f"  - {label}", file=sys.stderr)
        print(
            "Activate the project's virtual environment and push again, or create it with\n"
            '  python -m venv .venv && .venv/bin/pip install -e ".[dev]"\n'
            "(on Windows: .venv\\Scripts\\pip). See CONTRIBUTING.md, Development Setup.",
            file=sys.stderr,
        )
        return 1
    return subprocess.call([sys.executable, "-m", "pytest", *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
