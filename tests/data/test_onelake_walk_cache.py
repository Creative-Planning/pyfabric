"""Tests for onelake.walk() and onelake.download_with_cache().

Mocks the shared HTTP session so no network calls are made. Validates
the patterns that extraction pipelines reimplement today:
  - walking a directory tree recursively to find files by suffix
  - resolving a OneLake file through read-only fixture caches and a
    writable run cache, falling back to download only when needed
"""

import hashlib
from unittest.mock import MagicMock, patch

import pytest

from pyfabric.data.onelake import (
    download_with_cache,
    list_paths,
    md5_file,
    walk,
)


def _mock_session_with_pages(*pages: list[dict]) -> MagicMock:
    """Build a mock session whose GET returns the given pages in order.

    Each page is a list of path entries returned by the DFS filesystem API.
    The last page has no continuation header; earlier pages do.
    """
    responses = []
    for i, page in enumerate(pages):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"paths": page}
        is_last = i == len(pages) - 1
        resp.headers = {} if is_last else {"x-ms-continuation": f"tok-{i}"}
        responses.append(resp)

    session = MagicMock()
    session.get.side_effect = responses
    return session


# ── walk() ──────────────────────────────────────────────────────────────────


def _mock_session_per_directory(tree: dict[str, list[dict]]) -> MagicMock:
    """Build a mock session that returns different entries per directory.

    `tree` maps a request URL suffix (the directory path) to the list of
    entries that should be returned when that directory is listed.
    Missing entries (directories not in `tree`) yield an empty list.

    Mirrors real DFS semantics: calling with a directory path returns
    only its direct children. Manual-recursion in :func:`walk` drives
    a sequence of calls, one per subdirectory.
    """

    def handle_get(url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        # URL shape: {base}/{ws}/{item}/{path}. Extract the path tail.
        suffix = url.rstrip("/").rsplit("/lh/", 1)[-1] if "/lh/" in url else url
        # Normalize: suffix == "lh" when path == "" (root)
        page = tree.get(suffix, [])
        resp.json.return_value = {"paths": page}
        resp.headers = {}
        return resp

    session = MagicMock()
    session.get.side_effect = handle_get
    return session


class TestWalk:
    def test_yields_files_not_directories(self):
        # Real DFS semantics: list_paths("Files") returns direct children
        # only. walk() descends manually into "Files/sub".
        tree = {
            "Files": [
                {"name": "lh/Files/a.pdf", "contentLength": "100"},
                {"name": "lh/Files/sub", "isDirectory": "true"},
            ],
            "Files/sub": [
                {"name": "lh/Files/sub/b.pdf", "contentLength": "200"},
            ],
        }
        with patch(
            "pyfabric.data.onelake._get_session",
            return_value=_mock_session_per_directory(tree),
        ):
            results = list(walk("tok", "ws", "lh", "Files"))
        assert sorted(r["rel_path"] for r in results) == [
            "Files/a.pdf",
            "Files/sub/b.pdf",
        ]
        assert sorted(r["size"] for r in results) == [100, 200]

    def test_suffix_filter(self):
        tree = {
            "Files": [
                {"name": "lh/Files/a.pdf", "contentLength": "1"},
                {"name": "lh/Files/b.txt", "contentLength": "2"},
                {"name": "lh/Files/c.PDF", "contentLength": "3"},  # case-sensitive
            ],
        }
        with patch(
            "pyfabric.data.onelake._get_session",
            return_value=_mock_session_per_directory(tree),
        ):
            results = list(walk("tok", "ws", "lh", "Files", suffix=".pdf"))
        assert [r["rel_path"] for r in results] == ["Files/a.pdf"]

    def test_empty_dir_yields_nothing(self):
        with patch(
            "pyfabric.data.onelake._get_session",
            return_value=_mock_session_per_directory({"Files": []}),
        ):
            results = list(walk("tok", "ws", "lh", "Files"))
        assert results == []

    def test_404_yields_nothing(self):
        resp = MagicMock(status_code=404)
        session = MagicMock()
        session.get.return_value = resp
        with patch(
            "pyfabric.data.onelake._get_session",
            return_value=session,
        ):
            results = list(walk("tok", "ws", "lh", "Files"))
        assert results == []

    def test_names_without_item_prefix_pass_through(self):
        tree = {"Files": [{"name": "Files/a.pdf", "contentLength": "1"}]}
        with patch(
            "pyfabric.data.onelake._get_session",
            return_value=_mock_session_per_directory(tree),
        ):
            results = list(walk("tok", "ws", "lh", "Files"))
        assert results[0]["rel_path"] == "Files/a.pdf"

    def test_deep_tree_recurses_all_the_way_down(self):
        """Regression test for OneLake DFS quirk (2026-04-21): calling
        list_paths with recursive=true on a deep subdirectory returns
        only its direct children — not the full subtree. walk() must
        descend manually so any depth traversal returns all files.
        """
        tree = {
            "root": [
                {"name": "lh/root/year", "isDirectory": "true"},
            ],
            "root/year": [
                {"name": "lh/root/year/month", "isDirectory": "true"},
            ],
            "root/year/month": [
                {"name": "lh/root/year/month/region", "isDirectory": "true"},
            ],
            "root/year/month/region": [
                {"name": "lh/root/year/month/region/a.pdf", "contentLength": "10"},
            ],
        }
        with patch(
            "pyfabric.data.onelake._get_session",
            return_value=_mock_session_per_directory(tree),
        ):
            results = list(walk("tok", "ws", "lh", "root"))
        assert [r["rel_path"] for r in results] == ["root/year/month/region/a.pdf"]


# ── list_paths passes continuation tokens through ───────────────────────────


class TestListPathsContinuation:
    def test_paginates(self):
        page1 = [{"name": "lh/Files/a.pdf"}]
        page2 = [{"name": "lh/Files/b.pdf"}]
        session = _mock_session_with_pages(page1, page2)
        with patch("pyfabric.data.onelake._get_session", return_value=session):
            results = list_paths("tok", "ws", "lh", "Files")
        assert len(results) == 2
        assert session.get.call_count == 2


# ── md5_file ────────────────────────────────────────────────────────────────


class TestMd5File:
    def test_known_value(self, tmp_path):
        p = tmp_path / "data.bin"
        p.write_bytes(b"hello world")
        assert md5_file(p) == hashlib.md5(b"hello world").hexdigest()

    def test_streaming_matches_oneshot(self, tmp_path):
        payload = b"x" * 100_000
        p = tmp_path / "big.bin"
        p.write_bytes(payload)
        assert md5_file(p, chunk_size=4096) == hashlib.md5(payload).hexdigest()


# ── download_with_cache ─────────────────────────────────────────────────────


class TestDownloadWithCache:
    def test_hits_readonly_cache_first(self, tmp_path):
        ro = tmp_path / "readonly"
        writable = tmp_path / "cache"
        (ro / "Files").mkdir(parents=True)
        (ro / "Files" / "a.pdf").write_bytes(b"from-readonly")

        with patch("pyfabric.data.onelake.read_file") as mock_read:
            result = download_with_cache(
                "tok",
                "ws",
                "lh",
                "Files/a.pdf",
                cache_dir=writable,
                read_only_caches=[ro],
                expected_size=len(b"from-readonly"),
            )
        assert result == ro / "Files" / "a.pdf"
        mock_read.assert_not_called()

    def test_hits_writable_cache(self, tmp_path):
        cache = tmp_path / "cache"
        (cache / "Files").mkdir(parents=True)
        (cache / "Files" / "a.pdf").write_bytes(b"cached")

        with patch("pyfabric.data.onelake.read_file") as mock_read:
            result = download_with_cache(
                "tok",
                "ws",
                "lh",
                "Files/a.pdf",
                cache_dir=cache,
                expected_size=len(b"cached"),
            )
        assert result == cache / "Files" / "a.pdf"
        mock_read.assert_not_called()

    def test_size_mismatch_re_downloads(self, tmp_path):
        cache = tmp_path / "cache"
        (cache / "Files").mkdir(parents=True)
        stale = cache / "Files" / "a.pdf"
        stale.write_bytes(b"stale-and-wrong-size")

        with patch(
            "pyfabric.data.onelake.read_file", return_value=b"fresh"
        ) as mock_read:
            result = download_with_cache(
                "tok",
                "ws",
                "lh",
                "Files/a.pdf",
                cache_dir=cache,
                expected_size=len(b"fresh"),
            )
        assert result.read_bytes() == b"fresh"
        mock_read.assert_called_once_with("tok", "ws", "lh", "Files/a.pdf")

    def test_downloads_when_absent(self, tmp_path):
        cache = tmp_path / "cache"
        with patch(
            "pyfabric.data.onelake.read_file", return_value=b"new-bytes"
        ) as mock_read:
            result = download_with_cache(
                "tok",
                "ws",
                "lh",
                "Files/sub/a.pdf",
                cache_dir=cache,
            )
        assert result == cache / "Files" / "sub" / "a.pdf"
        assert result.read_bytes() == b"new-bytes"
        mock_read.assert_called_once()

    def test_md5_validation_on_download(self, tmp_path):
        data = b"payload"
        expected = hashlib.md5(data).hexdigest()
        with patch("pyfabric.data.onelake.read_file", return_value=data):
            result = download_with_cache(
                "tok",
                "ws",
                "lh",
                "Files/a.pdf",
                cache_dir=tmp_path / "cache",
                expected_md5=expected,
            )
        assert result.read_bytes() == data

    def test_md5_mismatch_raises(self, tmp_path):
        with (
            patch("pyfabric.data.onelake.read_file", return_value=b"tampered"),
            pytest.raises(ValueError, match="MD5 mismatch"),
        ):
            download_with_cache(
                "tok",
                "ws",
                "lh",
                "Files/a.pdf",
                cache_dir=tmp_path / "cache",
                expected_md5="deadbeef" * 4,
            )

    def test_md5_takes_precedence_over_size(self, tmp_path):
        """When md5 is given, size check alone must not be enough."""
        cache = tmp_path / "cache"
        (cache / "Files").mkdir(parents=True)
        wrong_but_right_size = cache / "Files" / "a.pdf"
        wrong_but_right_size.write_bytes(b"XXXXXXX")  # 7 bytes
        real_data = b"YYYYYYY"  # also 7 bytes, different hash
        real_md5 = hashlib.md5(real_data).hexdigest()

        with patch(
            "pyfabric.data.onelake.read_file", return_value=real_data
        ) as mock_read:
            result = download_with_cache(
                "tok",
                "ws",
                "lh",
                "Files/a.pdf",
                cache_dir=cache,
                expected_size=7,
                expected_md5=real_md5,
            )
        # Should have re-downloaded because md5 didn't match, despite size match.
        mock_read.assert_called_once()
        assert result.read_bytes() == real_data
