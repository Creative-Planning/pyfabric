"""Tests against pyproject.toml metadata.

Catches regressions where the published wheel's requires-python is bumped
above the Python version Fabric Spark runs at runtime — pip enforces
Requires-Python regardless of --no-deps, so a too-strict pin makes
pyfabric uninstallable inside Fabric Environment artifacts and notebook
%pip cells.

Also guards the ruff version against drifting between pyproject and
pre-commit — see :func:`test_ruff_pin_matches_pre_commit_rev`.
"""

import re
import tomllib
from pathlib import Path

_PYPROJECT = Path(__file__).parent.parent / "pyproject.toml"
_PRE_COMMIT = Path(__file__).parent.parent / ".pre-commit-config.yaml"


def _requires_python_min() -> tuple[int, int]:
    data = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    requires = data["project"]["requires-python"]
    m = re.match(r">=\s*(\d+)\.(\d+)", requires)
    assert m, f"unexpected requires-python format: {requires!r}"
    return int(m.group(1)), int(m.group(2))


def test_requires_python_allows_fabric_spark_runtime() -> None:
    """Fabric Spark runtime 1.3 runs Python 3.11 (cluster path
    ~/cluster-env/trident_env/lib/python3.11/). requires-python must be
    <= 3.11 so the wheel installs there.
    """
    major, minor = _requires_python_min()
    assert (major, minor) <= (3, 11), (
        f"requires-python = {major}.{minor} blocks installation in "
        f"Fabric Spark runtime 1.3 (Python 3.11). Lower to >=3.11."
    )


def test_requires_python_does_not_regress_below_310() -> None:
    """Lower bound: don't accept ancient Pythons that we definitely
    don't test on. 3.10 is the floor for `match`/PEP 604."""
    major, minor = _requires_python_min()
    assert (major, minor) >= (3, 10), (
        f"requires-python = {major}.{minor} is too low; the codebase "
        f"uses syntax that needs 3.10+ at minimum."
    )


def _dev_ruff_spec() -> str:
    data = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    dev = data["project"]["optional-dependencies"]["dev"]
    specs = [d for d in dev if re.match(r"^ruff\b", d)]
    assert len(specs) == 1, f"expected exactly one ruff spec in [dev], got {specs}"
    return specs[0]


def _pre_commit_ruff_rev() -> str:
    text = _PRE_COMMIT.read_text(encoding="utf-8")
    m = re.search(
        r"repo:\s*https://github\.com/astral-sh/ruff-pre-commit\s*\n\s*rev:\s*v?([\d.]+)",
        text,
    )
    assert m, "could not find the ruff-pre-commit rev in .pre-commit-config.yaml"
    return m.group(1)


def test_ruff_pin_is_exact() -> None:
    """ruff must be pinned, not floored.

    A floor like ``ruff>=0.8`` gives Dependabot nothing to bump, so a new
    ruff release enters CI silently on the next fresh ``pip install -e
    .[dev]`` — while every local check keeps running whatever version the
    developer resolved months ago. That is what broke Lint & Format on
    every open PR when 0.16 started formatting code blocks in markdown.
    An exact pin turns the upgrade into a reviewable Dependabot PR that
    carries any reformat with it.
    """
    spec = _dev_ruff_spec()
    assert "==" in spec, (
        f"ruff must be pinned exactly in [dev], got {spec!r}. An unpinned "
        f"floor drifts silently between local and CI."
    )


def test_ruff_pin_matches_pre_commit_rev() -> None:
    """The two declared ruff versions must agree.

    pre-commit and CI are separate sources of truth for the same tool. When
    they disagree, hooks pass locally and Lint & Format fails in CI on a diff
    the developer cannot reproduce. Bump both together.
    """
    spec = _dev_ruff_spec()
    assert "==" in spec, f"ruff is not pinned in [dev] ({spec!r}); nothing to compare"
    pinned = spec.split("==", 1)[1].strip()
    rev = _pre_commit_ruff_rev()
    assert pinned == rev, (
        f"ruff pinned to {pinned!r} in pyproject [dev] but "
        f".pre-commit-config.yaml uses rev v{rev}. Local hooks and CI would "
        f"run different formatters — bump both together."
    )
