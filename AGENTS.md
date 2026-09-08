# Eventglass implementation rules

Read docs/observe/implementation.md, architecture.md, quality.md and the relevant work package before changing a subsystem. Latest user instruction (2026-09-09): do not create or use subagents; the primary agent performs the remaining work directly. This supersedes earlier delegation authorization.

- Keep source-design.md byte-identical to the user attachment. Document corrections separately.
- Backend dependencies flow from HTTP/app to concrete operations/domain/storage; domain modules must not depend on HTTP. Move transaction policy out of handlers.
- Frontend separates generated API contracts, server query state, URL search state, and local form/view state. Follow docs/observe/architecture.md.
- One operational SQLite DB and one sequential Indexer. Preserve durable ACK, commit/finalize/publish boundaries, shared search authorization, and complete checkpoint recovery.
- Do not invent a query parser, WAL, general aggregation engine, or speculative service/repository abstraction.
- Do not use subagents. Complete and verify each coherent stage directly, then commit and immediately push it. Keep changes and executed test results in commit messages; do not create per-stage Markdown diaries.
- Use real tests and report executed commands. Missing tools/tests, 0 selected tests, skipped required checks, and unexecuted benchmarks are not passing evidence.
- The user authorizes commit-then-push for every stage. After the full implementation and verification finish, deploy through `ssh oracle`, following game-uridogu-com deployment conventions and selecting an appropriate domain. Do not deploy unfinished work or send external alerts. Test data belongs in isolated temporary directories.
