"""Repository-layout and immutable release routing regressions (no Docker/SSH)."""

import json
import os
from pathlib import Path
import shlex
import shutil
import runpy
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class LayoutTests(unittest.TestCase):
    def test_rust_product_is_self_contained(self):
        for name in (
            "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "build.rs",
            "deny.toml", "Dockerfile", "src", "tests", "migrations", "schemas", "web",
        ):
            self.assertTrue((ROOT / "rust" / name).exists(), name)
            self.assertFalse((ROOT / name).exists(), name)

    def test_docker_copy_sources_resolve_from_repository_context(self):
        for line in (ROOT / "rust/Dockerfile").read_text().splitlines():
            if not line.startswith("COPY ") or "--from=" in line:
                continue
            for source in shlex.split(line)[1:-1]:
                self.assertTrue(source.startswith("rust/"), source)
                self.assertTrue((ROOT / source).exists(), source)

    def test_resource_and_benchmark_inputs_resolve_to_rust(self):
        for name in ("check-resource", "check-benchmark"):
            module = runpy.run_path(str(ROOT / "scripts" / name))
            self.assertEqual(module["ROOT"], ROOT)
            self.assertEqual(module["RUST_ROOT"], ROOT / "rust")
            for source in ("Cargo.toml", "Cargo.lock", "build.rs", "src", "tests", "migrations"):
                self.assertTrue((module["RUST_ROOT"] / source).exists(), source)
        self.assertEqual(len(module["source_hash"]()), 64)

    def test_go_handoff_is_english_and_complete(self):
        for document in (ROOT / "go/eventglass").glob("*.md"):
            text = document.read_text()
            self.assertFalse(any("\uac00" <= char <= "\ud7a3" for char in text), document)
        design = (ROOT / "go/eventglass/DESIGN.md").read_text()
        for gate in range(9):
            self.assertIn(f"### G0{gate} ", design)

    def test_release_uses_archived_layout_and_not_dirty_worktree(self):
        for nested in (False, True):
            with self.subTest(nested=nested):
                self.exercise_release(nested)

    def exercise_release(self, nested):
        with tempfile.TemporaryDirectory(prefix="eventglass-layout-") as directory:
            root = Path(directory) / "repository with spaces"
            root.mkdir()
            (root / "scripts").mkdir()
            shutil.copy2(ROOT / "scripts/build-release", root / "scripts/build-release")
            product = root / "rust" if nested else root
            product.mkdir(exist_ok=True)
            (product / "Dockerfile").write_text("FROM scratch\n")
            (product / "source.txt").write_text("committed-source")

            def git(*args):
                return subprocess.check_output(
                    ["git", *args], cwd=root, text=True, stderr=subprocess.DEVNULL
                ).strip()

            git("init", "--quiet")
            git("add", ".")
            git("-c", "user.name=Layout Test", "-c", "user.email=layout@example.invalid",
                "-c", "core.hooksPath=/dev/null", "commit", "--quiet", "-m", "fixture")
            revision = git("rev-parse", "HEAD")
            (product / "source.txt").write_text("dirty-must-not-build")
            # The opposite layout in the working tree must not influence selection.
            opposite = root if nested else root / "rust"
            opposite.mkdir(exist_ok=True)
            (opposite / "Dockerfile").write_text("dirty-layout-must-not-build\n")
            binaries = root / "fake-bin"
            binaries.mkdir()
            capture = root / "capture.json"
            docker = binaries / "docker"
            docker.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, pathlib, sys\n"
                "args = sys.argv[1:]\n"
                "context = pathlib.Path(args[-1])\n"
                "dockerfile = pathlib.Path(args[args.index('--file') + 1])\n"
                "assert dockerfile.read_text() == 'FROM scratch\\n'\n"
                "assert (dockerfile.parent / 'source.txt').read_text() == 'committed-source'\n"
                "assert args[:2] == ['buildx', 'build']\n"
                "assert args[args.index('--target') + 1] == 'artifact'\n"
                "output = args[args.index('--output') + 1].split('dest=', 1)[1]\n"
                "pathlib.Path(output).mkdir(parents=True)\n"
                "(pathlib.Path(output) / 'eventglass').write_bytes(b'fixture artifact')\n"
                "pathlib.Path(os.environ['LAYOUT_CAPTURE']).write_text(json.dumps({"
                "'dockerfile': str(dockerfile.relative_to(context)),"
                "'revision': args[args.index('--build-arg') + 1],"
                "'context': str(context)}))\n"
            )
            docker.chmod(0o755)
            file_command = binaries / "file"
            file_command.write_text("#!/bin/sh\nprintf '%s: ELF ARM aarch64\\n' \"$1\"\n")
            file_command.chmod(0o755)
            output = root / "output" / "eventglass"
            environment = dict(os.environ, PATH=f"{binaries}:{os.environ['PATH']}",
                               LAYOUT_CAPTURE=str(capture),
                               EVENTGLASS_RELEASE_REVISION=revision,
                               EVENTGLASS_RELEASE_PLATFORM="linux/arm64")
            subprocess.run([str(root / "scripts/build-release"), str(output)],
                           cwd=directory, env=environment, check=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            recorded = json.loads(capture.read_text())
            self.assertEqual(recorded["dockerfile"], "rust/Dockerfile" if nested else "Dockerfile")
            self.assertEqual(recorded["revision"], f"EVENTGLASS_REVISION={revision}")
            self.assertFalse(Path(recorded["context"]).exists())
            self.assertEqual(output.read_bytes(), b"fixture artifact")
            self.assertEqual((product / "source.txt").read_text(), "dirty-must-not-build")


if __name__ == "__main__":
    unittest.main()
