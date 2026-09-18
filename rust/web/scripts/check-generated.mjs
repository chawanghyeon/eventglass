import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const outputDirectory = mkdtempSync(join(tmpdir(), "eventglass-openapi-"));
const generated = join(outputDirectory, "generated.ts");

try {
  execFileSync(
    process.platform === "win32"
      ? "node_modules/.bin/openapi-typescript.cmd"
      : "node_modules/.bin/openapi-typescript",
    ["../schemas/openapi.yaml", "-o", generated],
    { cwd: new URL("..", import.meta.url), stdio: "inherit" },
  );
  const expected = readFileSync(
    new URL("../src/api/generated.ts", import.meta.url),
    "utf8",
  );
  const actual = readFileSync(generated, "utf8");
  if (actual !== expected) {
    console.error("Generated API types are stale; run npm run generate:api.");
    process.exitCode = 1;
  }
} finally {
  rmSync(outputDirectory, { recursive: true, force: true });
}
