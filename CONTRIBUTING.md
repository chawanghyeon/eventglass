# Contributing to Eventglass

The active product is the root Go module. Use Go 1.26.5 exactly and run commands from the repository root unless noted otherwise.

```sh
./scripts/bootstrap
./scripts/check unit
./scripts/check codegen
./scripts/check architecture
./scripts/check focused ingest
./scripts/check journal-bench
./scripts/check contracts
./scripts/check integration
./scripts/check sdk
./scripts/check crash
./scripts/check web
```

Commands for later implementation gates intentionally fail until their gate is implemented. `./scripts/check codegen` regenerates the Go and TypeScript wire contracts in a temporary tree and requires byte-identical output; install its exact tool lock with `npm ci --prefix tools/codegen --ignore-scripts` when changing `api/openapi.yaml`, then run `./scripts/generate-api`. `./scripts/check sdk` replays committed captures and runs the pinned SDK applications against a localhost Go handler; run `tools/sdk-fixtures/bootstrap.sh` once to install its locked tools. Docker is required for the PostgreSQL/S3 integration environment and Linux ARM64 image checks. Tests must use temporary databases, buckets, prefixes, directories, and localhost receivers. Linux AMD64 is not a currently verified or supported release target.

The operator UI uses the exact Node/npm versions in `web/package.json`. Install
its locked dependencies with `npm ci --prefix web --ignore-scripts`; then
`./scripts/check web` runs Vitest, strict TypeScript, and the Vite production
build. Generated API types remain owned by `./scripts/generate-api`.

Complete and verify one gate at a time, then commit directly to `main` and run `git push origin main`. Use commit subjects such as `feat: 한국어 변경 요약`, selecting `fix`, `perf`, `test`, `docs`, or `chore` as appropriate. Do not bypass hooks or rewrite published history merely to normalize messages.

The former Rust implementation is recoverable from the `rust-version` tag and is not an active build, test, or deployment target.

Read DESIGN.md for behavior and ARCHITECTURE.md for package/state ownership.
Before continuing implementation, read docs/implementation/README.md and the
selected packet in docs/implementation/work-plan.md. Its linked contracts fix
schema/API/transaction decisions; do not infer missing behavior from old Rust docs.
Use focused pure-Go checks during development; run unit and relevant SDK/PG/S3
checks before shipping. Native changes additionally require contracts. The native
Docker stage is cached separately from Go sources; use ARM64 only. The journal
benchmark reports host allocations/time, not Linux RSS or production throughput.

Current pre-push runs unit/layout/architecture checks. Production deployment and
historical Rust artifact staging are not implemented by this hook. Gate completion
requires its executable evidence, not just the hook passing.

The `run` command starts the API and/or publication worker roles against the
exactly migrated PostgreSQL schema and matching S3 identity. Configure
`EVENTGLASS_DATABASE_URL`, `EVENTGLASS_PUBLIC_URL`, `EVENTGLASS_ROLES`,
`EVENTGLASS_SCRATCH_DIR`, `EVENTGLASS_S3_REGION`, `EVENTGLASS_S3_BUCKET`, and
optional endpoint/prefix/path-style settings. The API role also requires
`EVENTGLASS_AUTH_HASH_KEY_FILE`, whose file contains one random 32-byte value as
64 lowercase hexadecimal characters, and `EVENTGLASS_TOKEN_KEY_FILE` with an
independent value in the same format for signed read/cursor tokens. Alert API
management additionally requires `EVENTGLASS_ALERT_ENCRYPTION_KEY_FILE` with a
third independent value in the same format; PostgreSQL stores only its key ID
and AES-256-GCM ciphertext for destination secrets. A fresh installation additionally uses
`EVENTGLASS_BOOTSTRAP_TOKEN_FILE` in the same format; startup stores only its
hash, exposes the recoverable setup surface, and keeps readiness and ingestion
closed until setup commits. Production management cookies require HTTPS.
`EVENTGLASS_INSECURE_COOKIE=true` is accepted only with an explicit loopback HTTP
public URL for local development. AWS credentials use the default SDK chain.
The scheduler role runs retention and A1 alert evaluation; webhook delivery and
production deployment remain later gates.

`./scripts/check crash` cross-compiles the crash and ingress-resource tests for
Linux ARM64, then runs them with CPU1/512MiB/no-swap limits against disposable
PostgreSQL and MinIO. It exercises test-only inherited IPC barriers and SIGKILL;
no failpoint is exposed by the product HTTP server or runtime environment.
