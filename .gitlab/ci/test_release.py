"""Offline checks for retention and promotion failures with deployment impact."""
import contextlib
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("dev_release", Path(__file__).with_name("release.py"))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


@contextlib.contextmanager
def workspace():
    original = Path.cwd()
    with tempfile.TemporaryDirectory() as directory:
        os.chdir(directory)
        try:
            yield
        finally:
            os.chdir(original)


class ReleaseChecks(unittest.TestCase):
    def test_eight_newest_builds_and_no_other_tags_are_deleted(self):
        tags = [{"name": f"dev-{n}-aabbccdd"} for n in range(1, 13)]
        tags += [{"name": name} for name in ["dev", "latest", "v1.0", "dev-other"]]
        self.assertEqual(release.obsolete(tags), [f"dev-{n}-aabbccdd" for n in range(4, 0, -1)])

    def test_current_alias_is_preserved_if_newer_builds_failed(self):
        tags = [{"name": f"dev-{n}-aabbccdd"} for n in range(1, 13)]
        deleted = release.obsolete(tags, preserve=["dev-1-aabbccdd"])
        self.assertNotIn("dev-1-aabbccdd", deleted)
        self.assertEqual(len(deleted), 4)
        self.assertEqual(len(tags) - len(deleted), 8)

    def test_superseded_pipeline_does_not_publish(self):
        with workspace(), patch.object(release, "current", return_value=False):
            with patch.object(release, "registry_auth") as auth:
                release.publish()
                auth.assert_not_called()
                self.assertEqual(Path("dev-release.env").read_text(), "DEV_PUBLISHED=false\n")

    def test_running_old_version_and_alias_are_retained_within_eight(self):
        tags = [{"name": f"dev-{n}-aabbccdd"} for n in range(1, 13)]
        deleted = release.obsolete(tags, preserve=["dev-1-aabbccdd", "dev-2-aabbccdd"])
        self.assertNotIn("dev-1-aabbccdd", deleted)
        self.assertNotIn("dev-2-aabbccdd", deleted)
        self.assertEqual(len(tags) - len(deleted), 8)

    def test_too_many_running_versions_refuses_destructive_cleanup(self):
        tags = [{"name": f"dev-{n}-aabbccdd"} for n in range(1, 10)]
        with self.assertRaises(RuntimeError):
            release.obsolete(tags, preserve=[tag["name"] for tag in tags])

    def test_all_components_are_checked_before_first_alias_is_moved(self):
        with workspace(), patch.object(release, "current", return_value=True):
            with patch.object(release, "image_names", return_value=["registry/one", "registry/two"]):
                with patch.object(release, "registry_auth", side_effect=[({}, b"{}", "application/json"), RuntimeError("missing")]):
                    with patch.object(release, "request") as request:
                        with self.assertRaises(RuntimeError):
                            release.publish()
                        request.assert_not_called()
                        self.assertEqual(Path("dev-release.env").read_text(), "DEV_PUBLISHED=false\n")

    def test_branch_advance_during_preflight_does_not_publish(self):
        with workspace(), patch.object(release, "current", side_effect=[True, False]):
            with patch.object(release, "image_names", return_value=["registry/one"]):
                with patch.object(release, "registry_auth", return_value=({}, b"{}", "application/json")):
                    with patch.object(release, "request") as request:
                        release.publish()
                        request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
