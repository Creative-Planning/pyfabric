"""Tests for :func:`pyfabric.items.bundle.save_to_disk` byte canonicalization."""

from __future__ import annotations

from pyfabric.items.bundle import ArtifactBundle, save_to_disk
from pyfabric.items.normalize import is_canonical


class TestSaveToDiskCanonicalBytes:
    def test_every_emitted_file_passes_is_canonical(self, tmp_path):
        bundle = ArtifactBundle(
            item_type="Notebook",
            display_name="NB_Test",
            parts={
                "notebook-content.py": "# cell 1\nprint('hi')\n",
                "definition/tables/dim_test.tmdl": (
                    "table dim_test\n\tcolumn id\n\t\tdataType: int64"
                ),
                "Resources/builtin/sample.whl": b"\xff\xfe\x00\x01binary\x80\x81",
            },
        )
        artifact_dir = save_to_disk(bundle, tmp_path)

        for emitted in artifact_dir.rglob("*"):
            if emitted.is_file():
                assert is_canonical(emitted), (
                    f"non-canonical: {emitted.relative_to(tmp_path)}"
                )

    def test_crlf_input_becomes_lf_on_disk(self, tmp_path):
        bundle = ArtifactBundle(
            item_type="Notebook",
            display_name="NB_Crlf",
            parts={
                "notebook-content.py": "# line one\r\n# line two\r\nprint('hi')\r\n",
            },
        )
        artifact_dir = save_to_disk(bundle, tmp_path)
        raw = (artifact_dir / "notebook-content.py").read_bytes()
        assert b"\r\n" not in raw
        # notebook-content.py's rule is LF + trailing newline
        assert raw.endswith(b"\n")

    def test_binary_part_preserves_bytes(self, tmp_path):
        # bytes(range(256)) contains 0xC0/0xC1 and 0xF5-0xFF — invalid UTF-8
        # lead bytes — so canonical_bytes triggers its passthrough branch.
        payload = b"\x89PNG\r\n\x1a\n" + bytes(range(256))
        bundle = ArtifactBundle(
            item_type="Notebook",
            display_name="NB_Binary",
            parts={"Resources/builtin/sample.png": payload},
        )
        artifact_dir = save_to_disk(bundle, tmp_path)
        on_disk = (artifact_dir / "Resources" / "builtin" / "sample.png").read_bytes()
        assert on_disk == payload

    def test_platform_file_is_canonical(self, tmp_path):
        bundle = ArtifactBundle(
            item_type="Notebook",
            display_name="NB_Platform",
            parts={"notebook-content.py": "print('hi')\n"},
        )
        artifact_dir = save_to_disk(bundle, tmp_path)
        assert is_canonical(artifact_dir / ".platform")
