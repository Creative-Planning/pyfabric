"""
Structured logging for pyfabric using structlog.

Dual output: terse console + verbose JSON Lines file.
All log output is machine-parseable (JSON) for AI-assisted analysis.

Usage in scripts:
    from pyfabric._logging import setup_logging, get_log_path
    log_path = setup_logging("my_script")

Usage in library modules:
    import structlog
    log = structlog.get_logger()
    log.debug("detail for log file", workspace_id="ws-1")
    log.info("visible on console")
"""

import datetime
import logging
import re
import sys
from pathlib import Path

import structlog

# ── Constants ────────────────────────────────────────────────────────────────

LOGS_DIR = Path.cwd() / ".logs"

# JWT pattern: eyJ followed by base64url chars, at least 20 chars total
_TOKEN_RE = re.compile(r"eyJ[A-Za-z0-9_-]{20,}")


# ── Token masking ────────────────────────────────────────────────────────────


def _mask_tokens(text: str) -> str:
    """Replace JWT-like strings with [TOKEN]."""
    return _TOKEN_RE.sub("[TOKEN]", text)


def mask_tokens_processor(logger: object, method_name: str, event_dict: dict) -> dict:
    """Structlog processor that redacts JWT tokens from all event values."""
    for key, value in event_dict.items():
        if isinstance(value, str):
            event_dict[key] = _mask_tokens(value)
    return event_dict


# ── Stdlib compatibility ─────────────────────────────────────────────────────
# Keep these for backward compatibility with tests and any code that
# still references them directly.


class TokenMaskingFilter(logging.Filter):
    """Logging filter that redacts JWT tokens from stdlib log output."""

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


class JsonLinesFormatter(logging.Formatter):
    """Emit one JSON object per stdlib log record."""

    def format(self, record: logging.LogRecord) -> str:
        import json

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


class AsciiFormatter(logging.Formatter):
    """Terse console formatter that only emits ASCII-safe characters."""

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
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
    Configure structured logging: console (INFO/DEBUG) + JSON Lines file (DEBUG).

    Sets up structlog with processors for token masking and JSON rendering.
    Also configures stdlib logging for third-party libraries (requests, azure).

    Returns the log file path.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = get_log_path(script_name)

    # ── Stdlib logging (for third-party libs: requests, azure, urllib3) ───
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    mask_filter = TokenMaskingFilter()

    # Console: terse
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

    # ── Structlog configuration ──────────────────────────────────────────
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            mask_tokens_processor,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return log_path
