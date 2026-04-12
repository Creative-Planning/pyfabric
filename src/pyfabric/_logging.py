"""
Dual-output logging: terse console + verbose JSON Lines file.

Console output is ASCII-safe (no unicode) at INFO level.
File output is JSON Lines at DEBUG level, written to .logs/.

Usage in scripts:
    from pyfabric._logging import setup_logging, get_log_path
    log_path = setup_logging("my_script")

Usage in library modules:
    import logging
    log = logging.getLogger(__name__)
    log.debug("detail for log file")
    log.info("visible on console")
"""

import datetime
import json
import logging
import re
import sys
from pathlib import Path

# ── Constants ────────────────────────────────────────────────────────────────

LOGS_DIR = Path.cwd() / ".logs"

# JWT pattern: eyJ followed by base64url chars, at least 20 chars total
_TOKEN_RE = re.compile(r"eyJ[A-Za-z0-9_-]{20,}")


# ── Token masking ────────────────────────────────────────────────────────────


def _mask_tokens(text: str) -> str:
    """Replace JWT-like strings with [TOKEN]."""
    return _TOKEN_RE.sub("[TOKEN]", text)


class TokenMaskingFilter(logging.Filter):
    """Logging filter that redacts JWT tokens from all log output."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _mask_tokens(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    k: _mask_tokens(str(v)) if isinstance(v, str) else v
                    for k, v in record.args.items()
                }
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    _mask_tokens(str(a)) if isinstance(a, str) else a
                    for a in record.args
                )
        return True


# ── JSON Lines formatter ─────────────────────────────────────────────────────


class JsonLinesFormatter(logging.Formatter):
    """Emit one JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.datetime.fromtimestamp(
                record.created, tz=datetime.UTC
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


# ── Console formatter ────────────────────────────────────────────────────────


class AsciiFormatter(logging.Formatter):
    """Terse console formatter that only emits ASCII-safe characters."""

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        # Encode to ascii, replacing anything that would crash cp1252/ascii
        msg = msg.encode("ascii", errors="replace").decode("ascii")
        if record.exc_info and record.exc_info[0] is not None:
            msg += "\n" + self.formatException(record.exc_info)
        return msg


# ── Setup ────────────────────────────────────────────────────────────────────


def get_log_path(script_name: str) -> Path:
    """Return the log file path for a script invocation."""
    ts = datetime.datetime.now(tz=datetime.UTC).strftime("%Y%m%d_%H%M%S")
    return LOGS_DIR / f"{script_name}_{ts}.jsonl"


def setup_logging(
    script_name: str,
    *,
    verbose: bool = False,
) -> Path:
    """
    Configure dual logging: console (INFO/DEBUG) + JSON Lines file (DEBUG).

    Returns the log file path (for printing on failure).
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = get_log_path(script_name)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Remove any existing handlers (in case of re-init)
    root.handlers.clear()

    # Token masking on all output
    mask_filter = TokenMaskingFilter()

    # Console: terse, ASCII-safe
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(AsciiFormatter())
    console.addFilter(mask_filter)
    root.addHandler(console)

    # File: verbose JSON Lines
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(JsonLinesFormatter())
    file_handler.addFilter(mask_filter)
    root.addHandler(file_handler)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)

    return log_path
