"""Error path tests for ArtifactBundle — file I/O errors, load failures."""

from pathlib import Path

import pytest

from pyfabric.items.bundle import ArtifactBundle, load_from_disk, save_to_disk


class TestLoadFromDiskErrors:
    def test_missing_platform_file(self, tmp_path: Path):
        """Loading a directory without .platform should raise with path context."""
        item_dir = tmp_path / "nb_test.Notebook"
        item_dir.mkdir()
        (item_dir / "notebook-content.py").write_text("# code\n", encoding="utf-8")

        with pytest.raises(FileNotFoundError, match=r"\.platform") as exc_info:
            load_from_disk(item_dir)
        assert str(item_dir) in str(exc_info.value)

    def test_binary_file_handled_gracefully(self, tmp_path: Path):
        """Binary files in artifact should be loaded as bytes, not crash."""
        import json

        item_dir = tmp_path / "df_test.Dataflow"
        item_dir.mkdir()
        (item_dir / ".platform").write_text(
            json.dumps(
                {
                    "metadata": {"type": "Dataflow", "displayName": "df_test"},
                    "config": {
                        "version": "2.0",
                        "logicalId": "00000000-0000-0000-0000-000000000000",
                    },
                }
            ),
            encoding="utf-8",
        )
        (item_dir / "mashup.pq").write_bytes(b"\x00\x01\xff\xfe binary data")

        bundle = load_from_disk(item_dir)
        assert isinstance(bundle.parts["mashup.pq"], bytes)


class TestSaveToDisk:
    def test_creates_nested_directories(self, tmp_path: Path):
        bundle = ArtifactBundle(
            item_type="Environment",
            display_name="env_test",
            parts={
                "Libraries/PublicLibraries/environment.yml": "dependencies: []\n",
                "Setting/Sparkcompute.yml": "runtime_version: 1.3\n",
            },
        )
        artifact_dir = save_to_disk(bundle, tmp_path)
        assert (
            artifact_dir / "Libraries" / "PublicLibraries" / "environment.yml"
        ).exists()
        assert (artifact_dir / "Setting" / "Sparkcompute.yml").exists()

    def test_writes_platform_file(self, tmp_path: Path):
        import json

        bundle = ArtifactBundle(
            item_type="Notebook",
            display_name="nb_test",
            parts={"notebook-content.py": "# code\n"},
        )
        artifact_dir = save_to_disk(bundle, tmp_path)
        platform = json.loads((artifact_dir / ".platform").read_text(encoding="utf-8"))
        assert platform["metadata"]["type"] == "Notebook"
        assert platform["metadata"]["displayName"] == "nb_test"

    def test_saves_bytes_content(self, tmp_path: Path):
        bundle = ArtifactBundle(
            item_type="Dataflow",
            display_name="df_test",
            parts={"mashup.pq": b"\x00\x01\x02 binary"},
        )
        artifact_dir = save_to_disk(bundle, tmp_path)
        assert (artifact_dir / "mashup.pq").read_bytes() == b"\x00\x01\x02 binary"
