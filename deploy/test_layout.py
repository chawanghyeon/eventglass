import hashlib
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class GoLayoutTests(unittest.TestCase):
    def test_product_is_flattened_at_repository_root(self):
        for relative in (
            "go.mod",
            "go.sum",
            "Dockerfile",
            "DESIGN.md",
            "SDK-SOURCES.md",
            "SDK-SUPPORT.md",
            "cmd/eventglass-go/main.go",
            "internal/engine/child.go",
            "tests/integration/g00_test.go",
            "scripts/check",
            "deploy/compose.yaml",
            "deploy/versions.lock",
        ):
            self.assertTrue((ROOT / relative).is_file(), relative)
        self.assertFalse((ROOT / "go").exists())
        self.assertFalse((ROOT / "rust").exists())

    def test_module_and_docker_context_are_rooted(self):
        module = (ROOT / "go.mod").read_text().splitlines()[0]
        self.assertEqual(module, "module github.com/chawanghyeon/eventglass")
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("COPY go.mod go.sum ./", dockerfile)
        self.assertIn("COPY . .", dockerfile)
        self.assertNotIn("go/eventglass", dockerfile)

    def test_go_version_pin_is_consistent(self):
        module_lines = (ROOT / "go.mod").read_text().splitlines()
        module_version = next(line.split()[1] for line in module_lines if line.startswith("go "))
        version_lock = dict(
            line.split("=", 1)
            for line in (ROOT / "deploy/versions.lock").read_text().splitlines()
            if line and not line.startswith("#")
        )
        tool_versions = dict(
            line.split("=", 1)
            for line in (ROOT / "tools/versions.env").read_text().splitlines()
            if line and not line.startswith("#")
        )
        self.assertEqual(module_version, "1.26.5")
        self.assertEqual(version_lock["go"], module_version)
        self.assertEqual(tool_versions["GO_VERSION"], module_version)
        self.assertIn(f"VERSION={module_version}", (ROOT / "scripts/bootstrap").read_text())

    def test_source_design_is_unchanged(self):
        digest = hashlib.sha256(
            (ROOT / "docs/observe/source-design.md").read_bytes()
        ).hexdigest()
        self.assertEqual(
            digest,
            "4cccddc98f76d5c38099958587386c55756a08cf366c26ed9e65eeedec0526e5",
        )

    def test_fixture_baseline_hashes_match(self):
        manifest = json.loads((ROOT / "tests/fixtures/manifest.json").read_text())
        for key, relative in (
            ("source_design_sha256", "docs/observe/source-design.md"),
            ("go_design_sha256", "DESIGN.md"),
            ("sdk_sources_sha256", "SDK-SOURCES.md"),
            ("sdk_support_sha256", "SDK-SUPPORT.md"),
        ):
            digest = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
            self.assertEqual(manifest[key], digest, relative)


if __name__ == "__main__":
    unittest.main()
