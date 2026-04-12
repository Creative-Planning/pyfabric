"""Tests for MockNotebookUtils — filesystem, notebook, credentials mocks."""

from pathlib import Path

import pytest

from pyfabric.testing.mock_notebookutils import MockNotebookUtils


class TestMockFs:
    def test_mkdirs_creates_directory(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        nu.fs.mkdirs("/data/tables")
        assert (tmp_path / "data" / "tables").is_dir()

    def test_ls_lists_files(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        (tmp_path / "a.txt").write_text("hello")
        (tmp_path / "b.txt").write_text("world")
        result = nu.fs.ls("/")
        assert len(result) == 2

    def test_ls_nonexistent_returns_empty(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        assert nu.fs.ls("/nonexistent") == []

    def test_put_and_head(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        nu.fs.put("/data/test.txt", "hello world")
        content = nu.fs.head("/data/test.txt")
        assert content == "hello world"

    def test_put_bytes(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        nu.fs.put("/data/binary.bin", b"\x00\x01\x02")
        assert (tmp_path / "data" / "binary.bin").read_bytes() == b"\x00\x01\x02"

    def test_cp_file(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        (tmp_path / "src.txt").write_text("data")
        nu.fs.cp("/src.txt", "/dst.txt")
        assert (tmp_path / "dst.txt").read_text() == "data"

    def test_cp_recurse(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        src = tmp_path / "src_dir"
        src.mkdir()
        (src / "file.txt").write_text("data")
        nu.fs.cp("/src_dir", "/dst_dir", recurse=True)
        assert (tmp_path / "dst_dir" / "file.txt").read_text() == "data"

    def test_rm_file(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        f = tmp_path / "to_delete.txt"
        f.write_text("delete me")
        nu.fs.rm("/to_delete.txt")
        assert not f.exists()

    def test_rm_directory_recurse(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        d = tmp_path / "dir_to_delete"
        d.mkdir()
        (d / "file.txt").write_text("data")
        nu.fs.rm("/dir_to_delete", recurse=True)
        assert not d.exists()


class TestMockNotebook:
    def test_run_returns_empty_string(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        result = nu.notebook.run("nb_test", arguments={"key": "val"})
        assert result == ""

    def test_exit_does_not_raise(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        nu.notebook.exit("success")  # should not raise


class TestMockCredentials:
    def test_get_token_raises(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        with pytest.raises(NotImplementedError, match="not available in local mode"):
            nu.credentials.getToken("https://api.fabric.microsoft.com")


class TestMssparkutilsAlias:
    def test_alias_points_to_self(self, tmp_path: Path):
        nu = MockNotebookUtils(root=tmp_path)
        assert nu.mssparkutils is nu
