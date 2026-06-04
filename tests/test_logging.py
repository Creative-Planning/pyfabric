"""Tests for logging module — token masking, formatters, path generation."""

import logging

from pyfabric.logging import (
    AsciiFormatter,
    JsonLinesFormatter,
    TokenMaskingFilter,
    _mask_tokens,
    get_log_path,
)


class TestTokenMasking:
    def test_masks_jwt_like_string(self):
        text = "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature"
        result = _mask_tokens(text)
        assert "eyJ" not in result
        assert "[TOKEN]" in result

    def test_preserves_non_jwt_text(self):
        text = "no tokens here, just normal text"
        assert _mask_tokens(text) == text

    def test_masks_multiple_tokens(self):
        text = "token1=eyJabcdefghijklmnopqrst token2=eyJzyxwvutsrqponmlkjihg"
        result = _mask_tokens(text)
        assert result.count("[TOKEN]") == 2

    def test_short_eyj_not_masked(self):
        text = "eyJshort"
        assert _mask_tokens(text) == text


class TestTokenMaskingFilter:
    def test_filter_masks_message(self):
        f = TokenMaskingFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="token: eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9abcdef",
            args=None,
            exc_info=None,
        )
        f.filter(record)
        assert "[TOKEN]" in record.msg

    def test_filter_masks_string_args(self):
        f = TokenMaskingFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="scope: %s",
            args=("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9abcdef",),
            exc_info=None,
        )
        f.filter(record)
        assert "[TOKEN]" in record.args[0]

    def test_filter_returns_true(self):
        f = TokenMaskingFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="clean message",
            args=None,
            exc_info=None,
        )
        assert f.filter(record) is True


class TestJsonLinesFormatter:
    def test_formats_json(self):
        fmt = JsonLinesFormatter()
        record = logging.LogRecord(
            name="pyfabric.client",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="request sent",
            args=None,
            exc_info=None,
        )
        import json

        output = fmt.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "pyfabric.client"
        assert parsed["msg"] == "request sent"
        assert "ts" in parsed


class TestAsciiFormatter:
    def test_formats_ascii_safe(self):
        fmt = AsciiFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="caf\u00e9 data",
            args=None,
            exc_info=None,
        )
        output = fmt.format(record)
        assert output.isascii()


class TestGetLogPath:
    def test_returns_path_with_script_name(self):
        path = get_log_path("my_script")
        assert "my_script" in path.name
        assert path.suffix == ".jsonl"


class TestPositionalArgsInterpolation:
    """#103: %-style positional args must be interpolated into the event message,
    not left verbatim with the values stranded in a `positional_args` field."""

    def test_percent_args_interpolated_not_stranded(self, tmp_path, monkeypatch):
        import structlog

        import pyfabric.logging as plog

        monkeypatch.setattr(plog, "LOGS_DIR", tmp_path)
        log_path = plog.setup_logging("posargs_test")
        try:
            # A library-style %-format call, exactly like data/lakehouse.py uses.
            structlog.get_logger("test.posargs").info(
                "Target: %s/%s.%s (%d rows, %d cols)", "lh", "dbo", "tbl", 5, 3
            )
            for h in logging.getLogger().handlers:
                h.flush()
            content = log_path.read_text(encoding="utf-8")
        finally:
            # Release the file handle so tmp_path can be cleaned on Windows.
            root = logging.getLogger()
            for h in list(root.handlers):
                h.close()
                root.removeHandler(h)

        assert "Target: lh/dbo.tbl (5 rows, 3 cols)" in content
        assert "positional_args" not in content
        assert "%s" not in content

    def test_formatter_is_in_configured_chain(self, tmp_path, monkeypatch):
        import structlog

        import pyfabric.logging as plog

        monkeypatch.setattr(plog, "LOGS_DIR", tmp_path)
        plog.setup_logging("posargs_chain")
        try:
            processors = structlog.get_config()["processors"]
            assert any(
                isinstance(p, structlog.stdlib.PositionalArgumentsFormatter)
                for p in processors
            ), "PositionalArgumentsFormatter missing from the processor chain (#103)"
        finally:
            root = logging.getLogger()
            for h in list(root.handlers):
                h.close()
                root.removeHandler(h)
