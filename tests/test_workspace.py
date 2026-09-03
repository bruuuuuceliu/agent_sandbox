from __future__ import annotations

import os
import tempfile
import unittest

from sandbox_manager.workspace import (
    ChecksumMismatchError,
    SystemPathInspector,
    WorkspaceError,
    WorkspaceEscapeError,
    WorkspaceLayout,
    checksum_bytes,
    require_checksum,
    verify_checksum,
)


class FakeInspector:
    def __init__(
        self,
        symlinks: frozenset[str] = frozenset(),
        special: frozenset[str] = frozenset(),
    ) -> None:
        self._symlinks = symlinks
        self._special = special

    def is_symlink(self, path: str) -> bool:
        return path in self._symlinks

    def is_special_file(self, path: str) -> bool:
        return path in self._special


def layout(
    readable_roots: tuple[str, ...] = ("/results",),
) -> WorkspaceLayout:
    return WorkspaceLayout(
        workspace_root="/workspace", readable_roots=readable_roots
    )


class WorkspaceLayoutValidationTests(unittest.TestCase):
    def test_accepts_normalized_absolute_roots(self) -> None:
        parsed = WorkspaceLayout(
            workspace_root="/workspace", readable_roots=("/results", "/workspace")
        )

        self.assertEqual(parsed.workspace_root, "/workspace")
        self.assertEqual(parsed.readable_roots, ("/results", "/workspace"))

    def test_rejects_relative_workspace_root(self) -> None:
        with self.assertRaises(WorkspaceError):
            WorkspaceLayout(workspace_root="workspace")

    def test_rejects_filesystem_root(self) -> None:
        with self.assertRaises(WorkspaceError):
            WorkspaceLayout(workspace_root="/")

    def test_rejects_unnormalized_root(self) -> None:
        with self.assertRaises(WorkspaceError):
            WorkspaceLayout(workspace_root="/workspace/../etc")

    def test_rejects_duplicate_readable_roots(self) -> None:
        with self.assertRaises(WorkspaceError):
            WorkspaceLayout(
                workspace_root="/workspace", readable_roots=("/results", "/results")
            )


class UploadResolutionTests(unittest.TestCase):
    def test_resolves_nested_relative_destination(self) -> None:
        self.assertEqual(
            layout().resolve_upload("project/src/main.py"),
            "/workspace/project/src/main.py",
        )

    def test_rejects_absolute_destination(self) -> None:
        with self.assertRaises(WorkspaceError):
            layout().resolve_upload("/etc/passwd")

    def test_rejects_traversal_destination(self) -> None:
        with self.assertRaises(WorkspaceError):
            layout().resolve_upload("project/../../etc/passwd")

    def test_rejects_root_dot_destination(self) -> None:
        with self.assertRaises(WorkspaceError):
            layout().resolve_upload(".")

    def test_rejects_blank_destination(self) -> None:
        with self.assertRaises(WorkspaceError):
            layout().resolve_upload(" ")

    def test_rejects_backslash_destination(self) -> None:
        with self.assertRaises(WorkspaceError):
            layout().resolve_upload("project\\escape")

    def test_rejects_the_workspace_root_itself(self) -> None:
        with self.assertRaises(WorkspaceError):
            layout().resolve_upload("sub/..")


class DownloadResolutionTests(unittest.TestCase):
    def test_resolves_path_beneath_readable_root(self) -> None:
        self.assertEqual(
            layout().resolve_download("/results/report.tar.gz"),
            "/results/report.tar.gz",
        )

    def test_collapses_traversal_before_containment(self) -> None:
        with self.assertRaises(WorkspaceEscapeError):
            layout().resolve_download("/results/../../etc/passwd")

    def test_rejects_sibling_prefix_of_readable_root(self) -> None:
        with self.assertRaises(WorkspaceEscapeError):
            layout().resolve_download("/results-secrets/keys")

    def test_rejects_path_outside_readable_roots(self) -> None:
        with self.assertRaises(WorkspaceEscapeError):
            layout().resolve_download("/var/log/syslog")

    def test_rejects_relative_source(self) -> None:
        with self.assertRaises(WorkspaceError):
            layout().resolve_download("results/report.tar.gz")


class TransferSafetyTests(unittest.TestCase):
    def test_accepts_regular_component_paths(self) -> None:
        layout().ensure_transfer_safe("/workspace/project/main.py", FakeInspector())

    def test_rejects_symlinked_component(self) -> None:
        inspector = FakeInspector(symlinks=frozenset({"/workspace/project"}))

        with self.assertRaises(WorkspaceEscapeError):
            layout().ensure_transfer_safe("/workspace/project/main.py", inspector)

    def test_rejects_special_file_destination(self) -> None:
        inspector = FakeInspector(special=frozenset({"/workspace/dev"}))

        with self.assertRaises(WorkspaceError):
            layout().ensure_transfer_safe("/workspace/dev", inspector)

    def test_rejects_relative_path(self) -> None:
        with self.assertRaises(WorkspaceError):
            layout().ensure_transfer_safe("project/main.py", FakeInspector())


class SystemPathInspectorTests(unittest.TestCase):
    def test_classifies_real_files_and_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target.txt")
            link = os.path.join(directory, "link.txt")
            with open(target, "wb") as handle:
                handle.write(b"data")
            os.symlink(target, link)

            inspector = SystemPathInspector()

            self.assertFalse(inspector.is_symlink(target))
            self.assertTrue(inspector.is_symlink(link))
            self.assertFalse(inspector.is_special_file(target))

    def test_reports_fifo_as_special(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fifo = os.path.join(directory, "pipe")
            os.mkfifo(fifo)

            self.assertTrue(SystemPathInspector().is_special_file(fifo))


class ChecksumTests(unittest.TestCase):
    def test_require_checksum_accepts_lowercase_hex(self) -> None:
        self.assertEqual(
            require_checksum(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            ),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )

    def test_require_checksum_rejects_uppercase_hex(self) -> None:
        with self.assertRaises(WorkspaceError):
            require_checksum(
                "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"
            )

    def test_require_checksum_rejects_short_digest(self) -> None:
        with self.assertRaises(WorkspaceError):
            require_checksum("e3b0c44")

    def test_checksum_bytes_matches_verify(self) -> None:
        payload = b"agent sandbox workspace payload"
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "payload.bin")
            with open(path, "wb") as handle:
                handle.write(payload)

            verify_checksum(path, checksum_bytes(payload))

    def test_verify_checksum_rejects_mismatch(self) -> None:
        payload = b"agent sandbox workspace payload"
        declared = checksum_bytes(b"different payload")
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "payload.bin")
            with open(path, "wb") as handle:
                handle.write(payload)

            with self.assertRaises(ChecksumMismatchError) as raised:
                verify_checksum(path, declared)

        self.assertEqual(raised.exception.actual, checksum_bytes(payload))

    def test_checksum_bytes_rejects_text(self) -> None:
        with self.assertRaises(WorkspaceError):
            checksum_bytes("not bytes")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
