"""
Read-only workspace inspection demo.

A self-contained smoke test that verifies the chain a new user has to
get right before pyfabric becomes useful: install (with the right
extra), auth, list workspaces, resolve a workspace by display name,
and read items.

Wired as ``pyfabric demo <workspace_name>`` and as
``examples/workspace_demo.py``.

This module never writes, never deletes, and never pushes. The most
mutating thing it does is POST ``getDefinition`` for a single item
(only when ``--show-definitions`` is passed); that endpoint is read-
only despite the verb.
"""

import argparse
import difflib
import sys
from collections import Counter
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol

import structlog

if TYPE_CHECKING:
    from pyfabric.client.http import FabricClient

log = structlog.get_logger()


_INSTALL_HINT = (
    "pyfabric demo: the [azure] extra is required.\n"
    '  pip install --pre --upgrade "pyfabric[azure]"\n'
)


class _PagedClient(Protocol):
    """Subset of ``FabricClient`` used by the demo (eases stubbing in tests)."""

    def get_paged(
        self, path: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]: ...

    def post(self, path: str, body: Any = None) -> dict[str, Any]: ...


# ── Public entrypoint ────────────────────────────────────────────────────────


def run_demo(
    workspace_name: str,
    *,
    client: "FabricClient | _PagedClient | None" = None,
    show_definitions: bool = False,
    out: Any = None,
    err: Any = None,
) -> int:
    """Run the read-only workspace inspection demo.

    Returns a process exit code: 0 success, 1 user-input error
    (workspace not found / ambiguous), 2 environment error (missing
    install extra).

    Args:
        workspace_name:    Workspace display name to inspect.
        client:            Pre-built client (tests inject a stub).
                           If ``None``, a real ``FabricClient`` is
                           constructed — which requires the ``[azure]``
                           extra.
        show_definitions:  If True, fetch one item definition to
                           demonstrate ``get_item_definition``.
        out:               File for stdout (defaults to ``sys.stdout``).
        err:               File for stderr (defaults to ``sys.stderr``).
    """
    out = out or sys.stdout
    err = err or sys.stderr

    if client is None:
        try:
            import requests  # noqa: F401  (probe for the [azure] extra)

            from pyfabric.client.http import FabricClient as _FabricClient
        except ImportError as e:
            print(_INSTALL_HINT, file=err, end="")
            print(f"  (underlying error: {e})", file=err)
            return 2
        client = _FabricClient()

    from pyfabric.items.crud import list_items
    from pyfabric.workspace.workspaces import list_workspaces

    workspaces = list_workspaces(client)  # type: ignore[arg-type]
    _print_workspace_list(workspaces, out=out)

    resolved = _resolve_workspace(workspace_name, workspaces, out=out, err=err)
    if resolved is None:
        return 1

    print(
        f"\nResolved: {resolved['displayName']}  ({resolved['id']})",
        file=out,
    )

    items = list_items(client, resolved["id"])  # type: ignore[arg-type]
    _print_item_summary(items, out=out)

    if show_definitions and items:
        _print_one_definition(client, resolved["id"], items, out=out, err=err)

    return 0


# ── Workspace listing ────────────────────────────────────────────────────────


def _print_workspace_list(workspaces: list[dict[str, Any]], *, out: Any) -> None:
    print(f"Accessible workspaces ({len(workspaces)}):", file=out)
    top = workspaces[:10]
    for ws in top:
        print(
            f"  - {ws.get('displayName', '<no name>')}  ({ws.get('id', '?')})", file=out
        )
    remaining = len(workspaces) - len(top)
    if remaining > 0:
        print(f"  ... and {remaining} more", file=out)


# ── Resolution ───────────────────────────────────────────────────────────────


def _resolve_workspace(
    name: str,
    workspaces: list[dict[str, Any]],
    *,
    out: Any,
    err: Any,
) -> dict[str, Any] | None:
    """Resolve a workspace by display name.

    Resolution order: exact match, case-insensitive match, ambiguous
    (>1 hit by either rule), did-you-mean suggestion, not-found.
    """
    exact = [w for w in workspaces if w.get("displayName") == name]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        _print_ambiguous(name, exact, err=err)
        return None

    lower = name.lower()
    ci = [w for w in workspaces if str(w.get("displayName", "")).lower() == lower]
    if len(ci) == 1:
        match = ci[0]
        print(
            f"Note: case-insensitive match for {name!r} -> {match['displayName']!r}",
            file=out,
        )
        return match
    if len(ci) > 1:
        _print_ambiguous(name, ci, err=err)
        return None

    candidates = [str(w.get("displayName", "")) for w in workspaces]
    suggestions = difflib.get_close_matches(name, candidates, n=5, cutoff=0.5)
    print(f"\nWorkspace not found: {name!r}", file=err)
    if suggestions:
        print("Did you mean:", file=err)
        for s in suggestions:
            print(f"  - {s}", file=err)
    return None


def _print_ambiguous(
    name: str,
    matches: list[dict[str, Any]],
    *,
    err: Any,
) -> None:
    print(
        f"\nAmbiguous workspace name {name!r} — {len(matches)} matches:",
        file=err,
    )
    for w in matches:
        print(
            f"  - {w.get('displayName', '<no name>')}  ({w.get('id', '?')})",
            file=err,
        )
    print("Resolve by ID instead.", file=err)


# ── Item summary ─────────────────────────────────────────────────────────────


_ENUMERATED_TYPES = ("Lakehouse", "Notebook", "SemanticModel", "Report")


def _print_item_summary(items: list[dict[str, Any]], *, out: Any) -> None:
    print(f"\nItems ({len(items)}):", file=out)
    counts = Counter(str(it.get("type", "Unknown")) for it in items)
    for item_type, n in sorted(counts.items()):
        print(f"  {item_type}: {n}", file=out)

    for item_type in _ENUMERATED_TYPES:
        matches = [it for it in items if it.get("type") == item_type]
        if not matches:
            continue
        print(f"\n{item_type}(s):", file=out)
        for it in matches[:10]:
            print(
                f"  - {it.get('displayName', '<no name>')}  ({it.get('id', '?')})",
                file=out,
            )
        if len(matches) > 10:
            print(f"  ... and {len(matches) - 10} more", file=out)


# ── Optional definition fetch ────────────────────────────────────────────────


def _print_one_definition(
    client: "FabricClient | _PagedClient",
    workspace_id: str,
    items: list[dict[str, Any]],
    *,
    out: Any,
    err: Any,
) -> None:
    from pyfabric.items.crud import get_item_definition

    target = next(
        (it for it in items if it.get("type") in _ENUMERATED_TYPES),
        items[0],
    )
    name = target.get("displayName", "<no name>")
    print(
        f"\nFetching definition for {name!r} ({target.get('type')})...",
        file=out,
    )
    try:
        definition = get_item_definition(client, workspace_id, target["id"])  # type: ignore[arg-type]
    except Exception as e:
        print(f"  (definition fetch failed: {e})", file=err)
        return
    parts = definition.get("definition", {}).get("parts", [])
    print(f"  {len(parts)} part(s):", file=out)
    for p in parts[:5]:
        print(f"    - {p.get('path', '?')}", file=out)
    if len(parts) > 5:
        print(f"    ... and {len(parts) - 5} more", file=out)


# ── CLI entrypoint ───────────────────────────────────────────────────────────


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for ``pyfabric demo``."""
    parser = argparse.ArgumentParser(
        prog="pyfabric demo",
        description=(
            "Read-only workspace inspection: list workspaces, resolve "
            "one by display name, and summarise its items. Useful as a "
            "first-run smoke test after install."
        ),
    )
    parser.add_argument(
        "workspace_name",
        help="Workspace display name to inspect.",
    )
    parser.add_argument(
        "--show-definitions",
        action="store_true",
        help="Fetch one item definition to demonstrate get_item_definition.",
    )
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return int(e.code) if isinstance(e.code, int) else 2

    return run_demo(
        args.workspace_name,
        show_definitions=args.show_definitions,
    )


if __name__ == "__main__":
    sys.exit(main())
