"""Validate Fabric item structures before git-syncing.

Checks that item directories have the correct structure, required files,
and valid .platform metadata for their item type.

Usage:
    from pyfabric.items.validate import validate_item, validate_workspace

    result = validate_item(Path("ws/nb_test.Notebook"))
    if not result.valid:
        for error in result.errors:
            print(f"ERROR: {error.message}")

    results = validate_workspace(Path("ws/"))
    for r in results:
        status = "OK" if r.valid else "FAIL"
        print(f"{status}: {r.item_path.name}")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .types import ITEM_TYPES, parse_platform


@dataclass(frozen=True)
class ValidationError:
    """A single validation error or warning."""

    message: str
    path: Path | None = None


@dataclass
class ValidationResult:
    """Result of validating a single Fabric item."""

    item_path: Path
    item_type: str | None = None
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        """True if there are no errors (warnings are acceptable)."""
        return len(self.errors) == 0


def validate_item(item_dir: Path) -> ValidationResult:
    """Validate a single Fabric item directory.

    Checks:
    - .platform file exists and is valid JSON with required fields
    - Item type is known
    - All required files for the item type are present
    - Directory name matches the expected ``{displayName}.{type}`` pattern

    Returns a ``ValidationResult`` with errors and warnings.
    """
    result = ValidationResult(item_path=item_dir)
    platform_path = item_dir / ".platform"

    # Check .platform exists
    if not platform_path.exists():
        result.errors.append(
            ValidationError(".platform file is missing", platform_path)
        )
        return result

    # Parse .platform
    try:
        content = platform_path.read_text(encoding="utf-8")
        platform = parse_platform(content)
    except ValueError as e:
        result.errors.append(ValidationError(str(e), platform_path))
        return result

    result.item_type = platform.metadata.type

    # Check item type is known
    item_type_def = ITEM_TYPES.get(platform.metadata.type)
    if item_type_def is None:
        result.errors.append(
            ValidationError(
                f"Unknown item type '{platform.metadata.type}'",
                platform_path,
            )
        )
        return result

    # Check directory name matches
    expected_dir = platform.expected_dir_name
    if item_dir.name != expected_dir:
        result.warnings.append(
            ValidationError(
                f"Directory name mismatch: '{item_dir.name}' "
                f"(expected '{expected_dir}')",
                item_dir,
            )
        )

    # Check required files
    for required_file in item_type_def.required_files:
        if not (item_dir / required_file).exists():
            result.errors.append(
                ValidationError(
                    f"Required file missing: {required_file}",
                    item_dir / required_file,
                )
            )

    return result


def validate_workspace(workspace_dir: Path) -> list[ValidationResult]:
    """Validate all Fabric items in a workspace directory.

    Scans for directories matching the ``{name}.{ItemType}`` pattern
    and validates each one. Non-item directories are ignored.

    Returns a list of ``ValidationResult``, one per item found.
    """
    results: list[ValidationResult] = []
    if not workspace_dir.is_dir():
        return results

    for entry in sorted(workspace_dir.iterdir()):
        if not entry.is_dir():
            continue
        # Item directories have a dot-separated suffix matching a known type
        # or at least contain a .platform file
        parts = entry.name.rsplit(".", 1)
        if len(parts) != 2:
            continue
        _display_name, type_suffix = parts
        # Accept any directory with Type suffix pattern (known or unknown)
        if not type_suffix[0].isupper():
            continue
        results.append(validate_item(entry))

    return results
