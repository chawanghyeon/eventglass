# Eventglass working instructions

Complete the requested implementation, relevant verification, and necessary documentation. Resolve routine local work without approval loops. The primary agent works directly; do not use subagents.

## Product constraints

- Keep `docs/observe/source-design.md` byte-identical to the user attachment; document corrections separately.
- Backend dependencies flow from HTTP/app to operations/domain/storage. Domain modules do not depend on HTTP; transaction policy belongs outside handlers.
- Frontend separates generated API contracts, server query state, URL search state, and local form/view state.
- PostgreSQL owns receipts, authorization, jobs, and catalog state; S3 owns sanitized journal and bundle bytes. Preserve durable ACK, fenced Accept/Publish, shared search authorization, and coordinated PG/S3 recovery.
- Use the root Go module and ARM64 verification. DuckDB execution must use the pinned 2.0 build. Read `DESIGN.md` for behavior and `ARCHITECTURE.md` for ownership before changing a subsystem.
- Use existing query/storage engines. Avoid speculative service/repository layers.
- Test data belongs in isolated temporary directories. Do not send external alerts.

## Task references

- Setup and check commands: `CONTRIBUTING.md`.
- Behavior or subsystem contracts: relevant sections of `DESIGN.md`.
- Architecture or state ownership: `ARCHITECTURE.md`.
- Test infrastructure, resource limits, or release validation: `docs/observe/quality.md`.
- Continuing planned implementation: `DESIGN.md` section 22 and the gate status in `README.md`. Historical source material is not an active implementation instruction.

Record changes and executed verification in commits, without per-stage Markdown diaries. Follow the commit-then-push workflow on `main` in `CONTRIBUTING.md`. The current pre-push hook runs Go checks; production deployment is not implemented or authorized by that hook. Do not claim historical Rust deployment machinery is available.
