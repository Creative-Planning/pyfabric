"""
Standard CLI argument parsing and script execution wrapper.

Every script gets --env, --dry-run, --tenant, --verbose for free.
The run_main() wrapper handles logging setup, error reporting, and
structured log file output.

Usage:
    from pyfabric.cli import add_standard_args, run_main

    def main(args):
        ...  # script logic

    if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        add_standard_args(parser, project="my_project")
        parser.add_argument("--months", help="...")
        run_main(main, parser)
"""

import argparse
import sys
from pathlib import Path

import structlog

from pyfabric.client.auth import FabricCredential
from pyfabric.logging import setup_logging

log = structlog.get_logger()

# ── Environment registry ─────────────────────────────────────────────────────

# {project: {env_name: config_dict}}
_ENV_REGISTRY: dict[str, dict[str, dict]] = {}


def register_env(project: str, env_name: str, config: dict) -> None:
    """Register an environment config for a project.

    Config is an arbitrary dict — typically contains workspace_id,
    lakehouse_id, capacity_id, etc.
    """
    _ENV_REGISTRY.setdefault(project, {})[env_name] = config


def list_envs(project: str) -> list[str]:
    """Return registered environment names for a project."""
    return list(_ENV_REGISTRY.get(project, {}).keys())


def resolve_env(project: str, env_name: str) -> dict:
    """Look up an environment config. Raises KeyError if not found."""
    envs = _ENV_REGISTRY.get(project, {})
    if env_name not in envs:
        available = ", ".join(envs.keys()) or "(none registered)"
        raise KeyError(
            f"Unknown environment '{env_name}' for project '{project}'. "
            f"Available: {available}"
        )
    return envs[env_name]


# ── Argument helpers ─────────────────────────────────────────────────────────


def add_standard_args(
    parser: argparse.ArgumentParser,
    *,
    project: str | None = None,
) -> None:
    """Add --env, --dry-run, --tenant, --verbose to an argument parser.

    If project is provided and has registered environments, --env choices
    are constrained to those names.
    """
    envs = list_envs(project) if project else []

    if envs:
        parser.add_argument(
            "--env",
            required=True,
            choices=envs,
            help="Target environment",
        )
    else:
        parser.add_argument(
            "--env",
            required=True,
            help="Target environment (e.g. prod, uat, dev)",
        )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate everything but do not mutate remote state",
    )
    parser.add_argument(
        "--tenant",
        help="Azure tenant (alias, domain, GUID, or email)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show DEBUG output on console",
    )


def get_credential(args: argparse.Namespace) -> FabricCredential:
    """Build a FabricCredential from parsed CLI args."""
    return FabricCredential(tenant=getattr(args, "tenant", None))


# ── Script runner ────────────────────────────────────────────────────────────


def run_main(
    fn,
    parser: argparse.ArgumentParser,
    *,
    script_name: str | None = None,
) -> None:
    """
    Parse args, set up logging, run fn(args), handle errors.

    On success: exits 0.
    On failure: prints one-line summary + log file path, exits 1.
    The full traceback goes to the log file, not the console.
    """
    args = parser.parse_args()
    name = script_name or Path(sys.argv[0]).stem
    log_path = setup_logging(name, verbose=getattr(args, "verbose", False))

    log.debug("Script: %s", name)
    log.debug("Args: %s", vars(args))
    log.debug("Log file: %s", log_path)

    try:
        fn(args)
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as e:
        log.error("FAILED: %s", e, exc_info=True)
        print(f"\nFAILED: {e}", file=sys.stderr)
        print(f"Log: {log_path}", file=sys.stderr)
        sys.exit(1)


def main() -> int:
    """Run the pyfabric command-line interface."""
    print("pyfabric: no commands implemented yet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
