"""Tests against pyproject.toml metadata.

Catches regressions where the published wheel's requires-python is bumped
above the Python version Fabric Spark runs at runtime — pip enforces
Requires-Python regardless of --no-deps, so a too-strict pin makes
pyfabric uninstallable inside Fabric Environment artifacts and notebook
%pip cells.
"""

import re
import tomllib
from pathlib import Path

_PYPROJECT = Path(__file__).parent.parent / "pyproject.toml"


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
