"""
Read-only workspace inspection demo.

A self-contained smoke test for new pyfabric users:

    python examples/workspace_demo.py "<workspace_name>"
    python examples/workspace_demo.py "<workspace_name>" --show-definitions

Equivalent to (and a thin wrapper around) the ``pyfabric demo``
CLI subcommand. Both routes call ``pyfabric.demo.run_demo`` so they
stay in lockstep.

Requires the ``[azure]`` extra:

    pip install --pre --upgrade "pyfabric[azure]"
"""

import sys

from pyfabric.demo import main

if __name__ == "__main__":
    sys.exit(main())
