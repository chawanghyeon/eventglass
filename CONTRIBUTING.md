# Contributing to Eventglass

The active product is the root Go module. Use Go 1.26.5 exactly and run commands from the repository root unless noted otherwise.

```sh
./scripts/bootstrap
./scripts/check unit
./scripts/check architecture
./scripts/check focused ingest
./scripts/check journal-bench
./scripts/check contracts
./scripts/check integration
./scripts/check sdk
./scripts/check crash
```

Commands for later implementation gates intentionally fail until their gate is implemented. `./scripts/check sdk` replays committed captures and runs the pinned SDK applications against a localhost Go handler; run `tools/sdk-fixtures/bootstrap.sh` once to install its locked tools. Docker is required for the PostgreSQL/S3 integration environment and Linux ARM64 image checks. Tests must use temporary databases, buckets, prefixes, directories, and localhost receivers. Linux AMD64 is not a currently verified or supported release target.

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

The `run` command currently starts the G02 API role only. It requires an exactly
migrated and explicitly initialized PostgreSQL installation plus its verified S3
installation marker; it never creates either authority at startup. Configure
`EVENTGLASS_DATABASE_URL`, `EVENTGLASS_PUBLIC_URL`, `EVENTGLASS_ROLES=api`,
`EVENTGLASS_SCRATCH_DIR`, `EVENTGLASS_S3_REGION`, `EVENTGLASS_S3_BUCKET`, and
optional endpoint/prefix/path-style settings. AWS credentials use the default
SDK chain. Worker/scheduler roles, setup, and deployment remain later gates.

`./scripts/check crash` cross-compiles the crash and ingress-resource tests for
Linux ARM64, then runs them with CPU1/512MiB/no-swap limits against disposable
PostgreSQL and MinIO. It exercises test-only inherited IPC barriers and SIGKILL;
no failpoint is exposed by the product HTTP server or runtime environment.
