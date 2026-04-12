"""Smoke test to verify pyfabric is importable and has a version."""

import pyfabric


def test_version_exists() -> None:
    assert pyfabric.__version__ is not None
    assert isinstance(pyfabric.__version__, str)
    assert len(pyfabric.__version__) > 0
