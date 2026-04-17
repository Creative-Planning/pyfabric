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


def main(argv: list[str] | None = None) -> int:
    """Run the pyfabric command-line interface."""
    argv = list(sys.argv[1:] if argv is None else argv)

    # First positional arg is the subcommand. Keep dispatch dead-simple so
    # each subcommand can own its own argparse instance (and --help text).
    if not argv or argv[0] in ("-h", "--help"):
        print(
            "pyfabric — Microsoft Fabric helpers for Python\n"
            "\n"
            "Usage:\n"
            "  pyfabric install-claude-memory [--target DIR] [--force] [--dry-run]\n"
            "      Install pyfabric's reference memories into your active\n"
            "      Claude profile (or ~/.claude if none is active).\n"
            "\n"
            "  pyfabric emit-context\n"
            "      Print the memories as portable markdown (no Claude-specific\n"
            "      frontmatter). Pipe into whatever file your AI assistant\n"
            "      expects, e.g.:\n"
            "        pyfabric emit-context > .github/copilot-instructions.md\n"
            "        pyfabric emit-context > .cursorrules\n"
            "\n"
            "  pyfabric --help\n"
            "      Show this help.\n"
        )
        return 0 if argv else 1

    subcommand = argv[0]
    rest = argv[1:]

    if subcommand == "install-claude-memory":
        from pyfabric.claude_install import main as install_main

        return install_main(rest)

    if subcommand == "emit-context":
        from pyfabric.claude_install import emit_context

        if rest and rest[0] in ("-h", "--help"):
            print(
                "pyfabric emit-context — print reference memories as portable "
                "markdown.\n"
                "\n"
                "Redirect to whatever file your assistant reads:\n"
                "  .github/copilot-instructions.md  (GitHub Copilot)\n"
                "  .cursorrules                     (Cursor)\n"
                "  .continuerules                   (Continue)\n"
                "  CONVENTIONS.md                   (Aider)\n"
            )
            return 0
        return emit_context()

    print(f"pyfabric: unknown command '{subcommand}'", file=sys.stderr)
    print("Run 'pyfabric --help' for available commands.", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
