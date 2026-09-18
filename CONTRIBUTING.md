# Contributing to Eventglass

The active product is the Go implementation in `go/eventglass`. Use Go 1.26.5 exactly and run commands from that directory unless noted otherwise.

```sh
cd go/eventglass
./scripts/check unit
./scripts/check contracts
./scripts/check integration
```

Commands for later implementation gates intentionally fail until their gate is implemented. Docker is required for the PostgreSQL/S3 integration environment and Linux multi-architecture image checks. Tests must use temporary databases, buckets, prefixes, directories, and localhost receivers.

Complete and verify one gate at a time, then commit directly to `main` and run `git push origin main`. Use commit subjects such as `feat: 한국어 변경 요약`, selecting `fix`, `perf`, `test`, `docs`, or `chore` as appropriate. Do not bypass hooks or rewrite published history merely to normalize messages.

The former Rust implementation is recoverable from the `rust-version` tag and is not an active build, test, or deployment target.
