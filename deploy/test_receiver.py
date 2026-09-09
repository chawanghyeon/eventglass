"""Exercise deployment state transitions in isolated directories, without systemd."""

import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import receiver


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.root = root / "releases"
        self.root.mkdir()
        self.bin = root / "bin" / "eventglass"
        self.bin.parent.mkdir()
        for name, value in (("ROOT", self.root), ("BIN", self.bin)):
            mocked = patch.object(receiver, name, value)
            mocked.start()
            self.addCleanup(mocked.stop)
        mocked = patch.object(receiver, "validate")
        mocked.start()
        self.addCleanup(mocked.stop)
        self.revision = "a" * 40

    def stage(self, data=b"candidate", revision=None):
        receiver.stage(
            io.BytesIO(data), revision or self.revision, hashlib.sha256(data).hexdigest()
        )

    def test_staged_commit_is_immutable(self):
        self.stage()
        self.stage()
        with self.assertRaisesRegex(ValueError, "different bytes"):
            self.stage(b"different")
        self.assertEqual((self.root / self.revision / "eventglass").read_bytes(), b"candidate")

    @patch.object(receiver, "healthy", return_value=True)
    @patch.object(receiver.subprocess, "run")
    def test_repeated_activation_does_not_restart_healthy_service(self, run, _healthy):
        self.stage()
        receiver.activate(self.revision)
        receiver.activate(self.revision)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(self.bin.read_bytes(), b"candidate")
        self.assertEqual((self.root / "active").read_text().strip(), self.revision)

    @patch.object(receiver, "healthy", return_value=True)
    @patch.object(receiver.subprocess, "run")
    @patch.object(receiver, "prune", side_effect=OSError("disk error"))
    def test_cleanup_failure_does_not_roll_back_healthy_release(self, _prune, run, _healthy):
        self.stage()
        receiver.activate(self.revision)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(self.bin.read_bytes(), b"candidate")
        self.assertEqual((self.root / "active").read_text().strip(), self.revision)

    @patch.object(receiver.time, "sleep")
    @patch.object(receiver, "healthy", return_value=False)
    @patch.object(receiver.subprocess, "run")
    def test_failed_readiness_restores_previous_binary_and_receipt(self, run, _healthy, _sleep):
        previous = Path(f"{self.bin}.{'b' * 40}")
        previous.write_bytes(b"previous")
        self.bin.symlink_to(previous)
        receiver.write_active("b" * 40)
        self.stage()
        with self.assertRaisesRegex(ValueError, "readiness"):
            receiver.activate(self.revision)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(self.bin.read_bytes(), b"previous")
        self.assertEqual((self.root / "active").read_text().strip(), "b" * 40)


if __name__ == "__main__":
    unittest.main()
