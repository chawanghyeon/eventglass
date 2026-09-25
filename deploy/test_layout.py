import hashlib
import json
import subprocess
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
            "deploy/garage/compose.yaml",
            "deploy/garage/Caddyfile",
            "deploy/garage/garage.toml",
            "scripts/check-garage",
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
        self.assertIn("duckdb-go-bindings\\/lib\\//d", dockerfile)
        self.assertIn("-tags=duckdb_use_static_lib", dockerfile)

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
        self.assertEqual(module_version, "1.27.1")
        self.assertEqual(version_lock["go"], module_version)
        self.assertEqual(tool_versions["GO_VERSION"], module_version)
        self.assertIn(f"VERSION={module_version}", (ROOT / "scripts/bootstrap").read_text())

    def test_garage_contract_fixture_is_pinned_and_isolated(self):
        lock = dict(
            line.split("=", 1)
            for line in (ROOT / "deploy/versions.lock").read_text().splitlines()
            if line and not line.startswith("#")
        )
        compose = (ROOT / "deploy/garage/compose.yaml").read_text()
        runner = (ROOT / "scripts/check-garage").read_text()
        check = (ROOT / "scripts/check").read_text()
        self.assertIn(f"image: {lock['garage_image']}", compose)
        self.assertIn(f"image: {lock['caddy_image']}", compose)
        self.assertIn("platform: linux/arm64", compose)
        self.assertIn('"127.0.0.1::3900"', compose)
        self.assertIn("/var/lib/garage:rw,nosuid,nodev,size=256m,mode=1777", compose)
        self.assertIn("TestS3PutRangeListMultipartAbortAndPermissions", runner)
        self.assertIn("TestDurableWorkflowVerifiedUploadDuplicateAndUnsupportedOnly", runner)
        self.assertIn("down --volumes --remove-orphans", runner)
        self.assertIn("garage-backup-proxy", compose)
        self.assertIn("tls internal", (ROOT / "deploy/garage/Caddyfile").read_text())
        self.assertIn("garage) check_go_version", check)

    def test_native_contract_linking_is_serial_to_bound_scratch(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("RUN go test -p=1 -tags=duckdb_use_static_lib -count=1 ./...", dockerfile)
        self.assertIn("RUN go vet -p=1 -tags=duckdb_use_static_lib ./...", dockerfile)

    def test_comparison_runner_outlives_the_official_profile(self):
        runner = (ROOT / "scripts/check-comparison").read_text()
        self.assertIn("warmup=5m", runner)
        self.assertIn("load=30m", runner)
        self.assertIn("drain=10m", runner)
        self.assertIn("test_timeout=1h", runner)
        self.assertIn('-test.timeout "$test_timeout"', runner)
        self.assertLess(runner.index("-o /comparison/comparison.test"), runner.index("start_runtime()"))
        self.assertEqual(runner.count('--entrypoint /comparison.test "$build_image"'), 3)
        self.assertNotIn("test -tags comparison -count=1 -run '^TestPrepareComparisonInstallation$'", runner)

    def test_native_cache_recipe_excludes_product_sources(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        extract = '/^FROM / { stages++; if (stages == 2) exit } { print }'

        def recipe(text):
            return subprocess.run(
                ["awk", extract], input=text, text=True, check=True,
                capture_output=True,
            ).stdout

        native = recipe(dockerfile)
        self.assertIn("make bundle-library", native)
        self.assertNotIn("COPY . .", native)
        self.assertEqual(native, recipe(dockerfile.replace("COPY . .", "COPY ./ ./")))
        self.assertNotEqual(native, recipe(dockerfile.replace("CORE_EXTENSIONS=", "CHANGED_EXTENSIONS=")))
        for script in ("cache-native", "build-image"):
            self.assertIn(extract, (ROOT / "scripts" / script).read_text())

    def test_maintenance_resource_runner_is_bounded_and_real(self):
        source = (ROOT / "scripts/check-maintenance-resource").read_text()
        for required in (
            "./scripts/build-image", "--platform linux/arm64", "--network none",
            "--cpus 1 --memory 512m --memory-swap 512m", "--user 65532:65532",
            "--read-only", "GOMEMLIMIT=96MiB", "GOMAXPROCS=1",
            "--wait --wait-timeout 60 postgres minio", "EVENTGLASS_INTEGRATION_REQUIRED=1",
            "^TestMaintenanceMaximumBundle(Resource|Dispatcher)$", "memory.peak", "memory.events",
            "docker volume rm", "tr '[:upper:]' '[:lower:]'",
        ):
            self.assertIn(required, source)
        self.assertLess(source.index("go test -tags="), source.index("--cpus 1"))
        self.assertNotIn("--tmpfs /scratch", source)
        self.assertNotIn("REUSE_IMAGES", source)

    def test_image_helper_rejects_unverified_overrides_before_docker(self):
        for args in (
            ["--platform", "linux/amd64"], ["--file=elsewhere"], ["-felsewhere"],
            ["--build-context", "duckdb-build=elsewhere"], ["--build-arg", "DUCKDB_VERSION=other"],
            ["--target"], ["--tag", ""], ["--tag", "--file"], ["https://example.invalid/context"],
        ):
            with self.subTest(args=args):
                result = subprocess.run(
                    [str(ROOT / "scripts/build-image"), *args],
                    text=True, capture_output=True, check=False,
                )
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertNotIn("reusing pinned local native dependency", result.stderr)

    def test_image_gates_build_current_sources_with_dependency_cache(self):
        for script in (
            "check", "check-browser", "check-comparison", "check-recovery",
            "check-resource", "check-scale",
        ):
            source = (ROOT / "scripts" / script).read_text()
            self.assertIn("./scripts/build-image", source)
            self.assertNotIn("docker buildx build", source)

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
