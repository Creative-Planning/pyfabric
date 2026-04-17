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
