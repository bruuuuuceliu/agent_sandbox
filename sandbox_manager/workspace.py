"""Path-safe workspace resolution for relay file transfers.

``/workspace`` is ephemeral and dies with the Pod; ``/results`` is the
retained PVC and is never used as a transfer staging area.
Uploads are confined beneath the configured workspace root, downloads beneath
explicitly readable roots. Absolute paths, ``..`` traversal, symlink escapes,
and special files are rejected.
"""

from __future__ import annotations

import hashlib
import os
import posixpath
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol

from .commands import CommandResult

_CHECKSUM_PATTERN = re.compile(r"^[0-9a-f]{64}$")

_CHUNK_BYTES = 64 * 1024


class WorkspaceError(ValueError):
    """Raised when a transfer path or checksum is invalid."""


class WorkspaceEscapeError(WorkspaceError):
    """Raised when a path escapes its approved root."""

    def __init__(self, path: str, root: str) -> None:
        self.path = path
        self.root = root
        super().__init__(f"path escapes its approved root: {path}")


class ChecksumMismatchError(WorkspaceError):
    """Raised when transferred bytes do not match the declared checksum."""

    def __init__(self, declared: str, actual: str) -> None:
        self.declared = declared
        self.actual = actual
        super().__init__("transferred content does not match the declared checksum")


class PathInspector(Protocol):
    def is_symlink(self, path: str) -> bool: ...

    def is_special_file(self, path: str) -> bool: ...


class SystemPathInspector:
    """Filesystem-backed symlink and special-file inspection."""

    def is_symlink(self, path: str) -> bool:
        return os.path.islink(path)

    def is_special_file(self, path: str) -> bool:
        try:
            mode = os.stat(path).st_mode
        except OSError:
            return False
        return mode & 0o170000 != 0o100000


def require_checksum(value: str) -> str:
    """Validate one lowercase hexadecimal SHA-256 checksum."""

    if not isinstance(value, str) or not _CHECKSUM_PATTERN.fullmatch(value):
        raise WorkspaceError("checksum must be a lowercase SHA-256 hex digest")
    return value


@dataclass(frozen=True, slots=True)
class WorkspaceLayout:
    """Approved transfer roots for one sandbox profile."""

    workspace_root: str
    readable_roots: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "workspace_root",
            self._require_root(self.workspace_root, "workspace root"),
        )
        roots = tuple(self.readable_roots)
        for root in roots:
            self._require_root(root, "readable root")
        if len(set(roots)) != len(roots):
            raise WorkspaceError("readable roots must be unique")
        object.__setattr__(self, "readable_roots", roots)

    def resolve_upload(self, relative: str) -> str:
        """Resolve one upload destination beneath the workspace root."""

        _require_relative(relative, "upload destination")
        resolved = posixpath.normpath(posixpath.join(self.workspace_root, relative))
        if resolved == self.workspace_root or not _is_within(
            resolved, self.workspace_root
        ):
            raise WorkspaceEscapeError(relative, self.workspace_root)
        return resolved

    def resolve_download(self, path: str) -> str:
        """Resolve one download source beneath an approved readable root.

        ``normpath`` collapses traversal components, so containment against
        the approved roots is the decisive check.
        """

        if not isinstance(path, str) or not path.startswith("/"):
            raise WorkspaceError("download source must be an absolute path")
        normalized = posixpath.normpath(path)
        for root in self.readable_roots:
            if _is_within(normalized, root):
                return normalized
        raise WorkspaceEscapeError(path, "readable roots")

    def ensure_transfer_safe(self, path: str, inspector: PathInspector) -> None:
        """Reject paths that traverse a symlink or end at a special file."""

        if not isinstance(path, str) or not path.startswith("/"):
            raise WorkspaceError("transfer path must be absolute")
        normalized = posixpath.normpath(path)
        components = normalized.split(posixpath.sep)
        walked = ""
        for component in components:
            if not component:
                continue
            walked = f"{walked}/{component}"
            if inspector.is_symlink(walked):
                raise WorkspaceEscapeError(path, walked)
        if inspector.is_special_file(normalized):
            raise WorkspaceError("transfer path refers to a special file")

    def _require_root(self, value: str, field: str) -> str:
        if not isinstance(value, str) or not value.startswith("/"):
            raise WorkspaceError(f"{field} must be an absolute path")
        normalized = posixpath.normpath(value)
        if normalized != value or normalized == "/" or ".." in normalized.split(
            posixpath.sep
        ):
            raise WorkspaceError(f"{field} must be a normalized non-root path")
        if any(ord(character) < 32 for character in normalized):
            raise WorkspaceError(f"{field} contains invalid text")
        return normalized


def verify_checksum(path: str, declared: str) -> None:
    """Stream one local file and confirm its SHA-256 checksum."""

    require_checksum(declared)
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != declared:
        raise ChecksumMismatchError(declared, actual)


def checksum_bytes(data: bytes) -> str:
    """Return the SHA-256 checksum of an in-memory payload."""

    if not isinstance(data, (bytes, bytearray)):
        raise WorkspaceError("payload must be bytes")
    return hashlib.sha256(bytes(data)).hexdigest()


class CommandWorkspace(Protocol):
    """Upload and download operations built on the approved workspace layout."""

    async def upload(
        self,
        task_id: str,
        command_id: str,
        destination: str,
        declared_size: int,
        declared_checksum: str,
        frames: AsyncIterator[bytes],
    ) -> CommandResult:
        """Store one uploaded file atomically beneath the workspace root."""

        ...

    def download(
        self,
        task_id: str,
        command_id: str,
        source: str,
        max_bytes: int,
    ) -> AsyncIterator[bytes]:
        """Stream one approved download source in bounded frames."""

        ...


def _is_within(path: str, root: str) -> bool:
    if path == root:
        return True
    return path.startswith(root + posixpath.sep)


def _require_relative(value: str, field: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise WorkspaceError(f"{field} must be a non-empty relative path")
    if value.startswith("/") or "\\" in value or "\0" in value:
        raise WorkspaceError(f"{field} must be a relative POSIX path")
    if any(ord(character) < 32 for character in value):
        raise WorkspaceError(f"{field} contains invalid text")
    for component in value.split("/"):
        if component in {"", ".", ".."}:
            raise WorkspaceError(f"{field} must not contain traversal components")
