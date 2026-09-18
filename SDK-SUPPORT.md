# SDK compatibility evidence

This report describes the compatibility actually exercised by the G01 gate. It is not a claim of compatibility with every Sentry SDK, SDK version, transport, or Sentry product feature.

## Verified runtimes

| Runtime | SDK | Executed scenarios | Captured HTTP |
|---|---|---|---|
| Python 3.14.7 | `sentry-sdk==2.69.0` | exception, message, default and opt-in DEBUG logging, ERROR Event plus Log, FastAPI shutdown, Celery fork/worker shutdown | gzip envelopes |
| Node.js 24.21.0 | `@sentry/node@10.73.0` | exception, message, all log levels, batch flush, client report, all-category rate-limit backoff | identity envelopes |
| Chromium via Playwright | `@sentry/browser@10.73.0` | exception, message, console integration, CORS preflight, Unicode, flush | identity envelopes |
| Go 1.26.5 darwin/arm64 | `github.com/getsentry/sentry-go@v0.49.0` | exception, message, real structured logger, exact int64 and array attributes, versionless container, flush | identity envelopes |

Every row was sent both to a localhost recorder for committed byte fixtures and to the Go ingestion handler. The offline replay checks byte length and SHA-256 before normalization. The live gate starts the Go API and executes the pinned SDK applications again. The Node rate-limit test verifies that the SDK suppresses later sends after the first all-category `429`.

## Protocol coverage

- Envelope byte lengths, optional final LF, unknown binary items, duplicate keys, invalid UTF-8, one Event/Transaction limit, and mixed Event plus Logs.
- Envelope, query, and `X-Sentry-Auth` identity agreement; project matching; browser origin allowlist and preflight.
- identity, gzip, zlib-deflate, Brotli, and Zstandard HTTP decoding with separate wire and expansion limits. The committed SDK captures exercise identity and gzip; deterministic Go transport vectors cover the other three encodings.
- Direct JSON store requests and the legacy form `sentry_data=base64(zlib(JSON))` adapter. Compression is selected by the endpoint content type and is never sniffed.
- Historical versionless logs, explicit version 1, and version 2 adapters; unknown versions fail the whole request. Official sentry-python commit `17cc8c7b2c31c2df130418bb49137814d4d35f7b` changed the historical versionless container directly to version 2, so the historical version-1 wire evidence is versionless. The explicit `version: 1` path is retained as a fixed protocol vector using the same legacy schema.
- Exact integer attributes through 38 digits, explicit larger-integer classification, declared doubles, heterogeneous arrays, units, RFC3339 and decimal/exponent timestamps, Unicode, and trace/span validation.
- Client-report outcomes, per-request UUIDv4 acceptance identity, domain-separated record IDs, scrubbing before canonical raw storage, 1 MiB record/20 MiB request limits, 1,000 attributes, two decoders, and 64 MiB byte admission.

## Limits of the claim

Transactions are preserved as transaction records but full Sentry performance/APM behavior is not claimed. Attachments and other unsupported item types are discarded after framing validation and diagnosed; they are not forwarded. Replay, profiles, metrics, sessions, check-ins, feedback, minidumps, source maps, and symbolication are not supported.

Java, .NET, PHP, Ruby, Rust, mobile, and native SDKs are unverified. Linux AMD64 is also unverified. Future SDK releases require pinned source inspection, a localhost wire capture, offline replay, and a live Go API run before they can be added to this table.

Run the gate with:

```sh
./scripts/check sdk
```
