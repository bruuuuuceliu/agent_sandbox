"""Immutable sandbox profile definitions and lookup."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

PROFILE_SCHEMA_VERSION: Literal[1] = 1

_DIGEST_PINNED_IMAGE = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")


class ProfileError(ValueError):
    """Raised when a sandbox profile is invalid."""


class UnknownSandboxTypeError(LookupError):
    """Raised when no approved profile exists for a sandbox type."""

    def __init__(self, sandbox_type: str) -> None:
        self.sandbox_type = sandbox_type
        super().__init__(f"unsupported sandbox type: {sandbox_type}")


@dataclass(frozen=True, slots=True)
class ProfileMount:
    name: str
    mount_path: str
    read_only: bool = False

    def __post_init__(self) -> None:
        _validate_text(self.name, "mount name")
        _validate_text(self.mount_path, "mount path")
        if not self.mount_path.startswith("/"):
            raise ProfileError("mount path must be absolute")


@dataclass(frozen=True, slots=True)
class SandboxProfile:
    schema_version: Literal[1]
    profile_version: int
    sandbox_type: str
    image: str
    command: tuple[str, ...]
    volume_mounts: tuple[ProfileMount, ...]

    def __post_init__(self) -> None:
        if self.schema_version != PROFILE_SCHEMA_VERSION:
            raise ProfileError(
                f"unsupported profile schema version: {self.schema_version}"
            )
        if type(self.profile_version) is not int or self.profile_version < 1:
            raise ProfileError("profile version must be a positive integer")
        _validate_text(self.sandbox_type, "sandbox type")
        if not _DIGEST_PINNED_IMAGE.fullmatch(self.image):
            raise ProfileError("profile image must use a sha256 digest reference")

        if not isinstance(self.command, tuple):
            raise ProfileError("profile command must be a tuple")
        command = tuple(self.command)
        if not command:
            raise ProfileError("profile command must not be empty")
        for argument in command:
            _validate_text(argument, "command argument")
        object.__setattr__(self, "command", command)

        if not isinstance(self.volume_mounts, tuple) or not all(
            isinstance(mount, ProfileMount) for mount in self.volume_mounts
        ):
            raise ProfileError("profile mounts must be a tuple of mounts")
        mounts = tuple(self.volume_mounts)
        if not mounts:
            raise ProfileError("profile must define at least one volume mount")
        if len({mount.name for mount in mounts}) != len(mounts):
            raise ProfileError("profile mount names must be unique")
        if len({mount.mount_path for mount in mounts}) != len(mounts):
            raise ProfileError("profile mount paths must be unique")
        if len(mounts) != 1 or mounts[0].name != "results":
            raise ProfileError("profile must define exactly one results mount")
        if mounts[0].read_only:
            raise ProfileError("results mount must be writable")
        object.__setattr__(self, "volume_mounts", mounts)

    @property
    def results_mount_path(self) -> str:
        return self.volume_mounts[0].mount_path


class SandboxProfileRegistry:
    def __init__(self, profiles: Iterable[SandboxProfile]) -> None:
        by_type: dict[str, SandboxProfile] = {}
        for profile in profiles:
            if profile.sandbox_type in by_type:
                raise ProfileError(f"duplicate sandbox profile: {profile.sandbox_type}")
            by_type[profile.sandbox_type] = profile
        self._profiles: Mapping[str, SandboxProfile] = MappingProxyType(by_type)

    def resolve(self, sandbox_type: str) -> SandboxProfile:
        try:
            return self._profiles[sandbox_type]
        except KeyError as error:
            raise UnknownSandboxTypeError(sandbox_type) from error


def _validate_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ProfileError(f"{field} must be a non-empty string")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ProfileError(f"{field} contains invalid text")
