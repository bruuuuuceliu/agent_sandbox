from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError

from sandbox_manager.profiles import (
    PROFILE_SCHEMA_VERSION,
    ProfileError,
    ProfileMount,
    SandboxProfile,
    SandboxProfileRegistry,
    UnknownSandboxTypeError,
)


def profile(**overrides: object) -> SandboxProfile:
    values: dict[str, object] = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "profile_version": 1,
        "sandbox_type": "python",
        "image": "registry.example/sandbox/python@sha256:" + "a" * 64,
        "command": ("python", "-m", "sandbox_agent"),
        "volume_mounts": (ProfileMount("results", "/results"),),
    }
    values.update(overrides)
    return SandboxProfile(**values)  # type: ignore[arg-type]


class SandboxProfileTests(unittest.TestCase):
    def test_resolves_an_immutable_approved_profile(self) -> None:
        approved = profile()
        registry = SandboxProfileRegistry([approved])

        resolved = registry.resolve("python")

        self.assertIs(resolved, approved)
        self.assertEqual(resolved.profile_version, 1)
        with self.assertRaises(FrozenInstanceError):
            resolved.image = "registry.example/other@sha256:" + "b" * 64  # type: ignore[misc]

    def test_requires_a_digest_pinned_image(self) -> None:
        invalid_images = (
            "registry.example/sandbox/python:latest",
            "registry.example/sandbox/python@sha256:abcd",
            "registry.example/sandbox/python@sha512:" + "a" * 64,
            "registry.example/sandbox/python@sha256:" + "A" * 64,
        )

        for image in invalid_images:
            with (
                self.subTest(image=image),
                self.assertRaisesRegex(ProfileError, "sha256 digest"),
            ):
                profile(image=image)

    def test_rejects_unknown_and_duplicate_sandbox_types(self) -> None:
        registry = SandboxProfileRegistry([profile()])

        with self.assertRaises(UnknownSandboxTypeError):
            registry.resolve("node")
        with self.assertRaisesRegex(ProfileError, "duplicate"):
            SandboxProfileRegistry([profile(), profile(profile_version=2)])

    def test_validates_profile_contract(self) -> None:
        cases: dict[str, dict[str, object]] = {
            "schema version": {"schema_version": 2},
            "boolean schema version": {"schema_version": True},
            "profile version": {"profile_version": 0},
            "empty command": {"command": ()},
            "string command": {"command": "python"},
            "no mounts": {"volume_mounts": ()},
            "unknown mount": {"volume_mounts": (ProfileMount("cache", "/cache"),)},
            "read-only results": {
                "volume_mounts": (ProfileMount("results", "/results", True),)
            },
            "duplicate mounts": {
                "volume_mounts": (
                    ProfileMount("results", "/results"),
                    ProfileMount("results", "/cache"),
                )
            },
        }

        for name, overrides in cases.items():
            with self.subTest(name=name), self.assertRaises(ProfileError):
                profile(**overrides)


if __name__ == "__main__":
    unittest.main()
