# SDK source inspection and evidence

Inspected on2026-09-18. This is an evidence appendix to [DESIGN.md](DESIGN.md), not a replacement for its normative contracts. Source inspection does not count as an executed compatibility test.

## 1. Pinned inspection targets

| Repository | Tag | Verified commit |
|---|---|---|
| sentry-python | 2.69.0 | f186a62b34ea44eb6d0db1d20f535817991c32fd |
| sentry-javascript | 10.73.0 | f109d922f5971e2ade549b6c755524168b101818 |
| sentry-go | v0.49.0 | 78b09d19307aafb162cd57838bd5c72055b14c8c |
| getsentry/develop | Master snapshot at inspection | 26cabd61bbd94ac8cffd05f1cacd03950a5576c7 |

Compared SDK versions to shared tools/sdk-fixtures Python requirements.lock, Node/Browser package locks, and Go go.mod. Create independent Go fixtures/locks without changing these source inputs. Historical server success is not evidence of the new Go server; every declared runtime requires a fresh localhost wire fixture.

## 2. Python

Inspected sources:

- [_log_batcher.py](https://github.com/getsentry/sentry-python/blob/f186a62b34ea44eb6d0db1d20f535817991c32fd/sentry_sdk/_log_batcher.py): transport formatting, typed attributes, severity/trace/span, lost-event categories.
- [_batcher.py](https://github.com/getsentry/sentry-python/blob/f186a62b34ea44eb6d0db1d20f535817991c32fd/sentry_sdk/_batcher.py): version2 container, batching/wait/overflow/fork buffer behavior.
- [integrations/logging.py](https://github.com/getsentry/sentry-python/blob/f186a62b34ea44eb6d0db1d20f535817991c32fd/sentry_sdk/integrations/logging.py): separate breadcrumb/event/log paths, template parameters and code/logger metadata.
- [utils.py](https://github.com/getsentry/sentry-python/blob/f186a62b34ea44eb6d0db1d20f535817991c32fd/sentry_sdk/utils.py): serialize_attribute sections, bool/int distinctions, arrays and fallback stringification.

Consequences: SDK batching uses100 items, buffer cap1,000, and roughly5s intervals; server visibility does not remove SDK delay, so fixtures must flush/wait. Typed severity attributes cannot be ignored. One logging call's three paths must not be message-hash deduplicated. Do not reverse SDK stringification into inferred objects.

## 3. JavaScript: Node and Browser

- [logs/envelope.ts](https://github.com/getsentry/sentry-javascript/blob/f109d922f5971e2ade549b6c755524168b101818/packages/core/src/logs/envelope.ts): type/content_type/item_count/version2, browser ingest_settings, tunnel DSN.
- [logs/internal.ts](https://github.com/getsentry/sentry-javascript/blob/f109d922f5971e2ade549b6c755524168b101818/packages/core/src/logs/internal.ts): scope/severity/trace/parent span/template/sequence/beforeSendLog/lone-surrogate handling.
- [attributes.ts](https://github.com/getsentry/sentry-javascript/blob/f109d922f5971e2ade549b6c755524168b101818/packages/core/src/attributes.ts): typed values, arrays, unit, fallback.
- [transports/base.ts](https://github.com/getsentry/sentry-javascript/blob/f109d922f5971e2ade549b6c755524168b101818/packages/core/src/transports/base.ts): buffering, rate-limit filtering, queue/network loss outcomes, responses.

Consequences: a batch can contain multiple traces. Parent span may be sentry.trace.parent_span_id. Preserve sentry.message.parameter.N and sequence, but sequence is not a global dedupe ID. Browser inference requests do not override server privacy policy. The inspected base transport is not a guaranteed lossless retry queue; do not extend this conclusion into claims about separately configurable offline transports that were not verified.

## 4. Go

Inspected tagged sources and matching local module cache:

- [internal/protocol/item_container.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/internal/protocol/item_container.go): versionless items container.
- [interfaces.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/interfaces.go): Log time.Time/trace/span/severity/body/attribute fields.
- [log.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/log.go): release/environment/server/SDK attributes and plural sentry.message.parameters.N.
- [log_batch_processor.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/log_batch_processor.go): legacy event-wrapper flush path.
- [transport.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/transport.go) and [transport_test.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/transport_test.go): envelope log items and versionless examples.

Consequences: requiring a version would reject valid Go logs. Accept observed extra wrapper metadata. Support RFC3339, not just numeric epochs. Existing Go fixtures cover CaptureException/CaptureMessage, not structured logs; add actual logger fixtures for the new server.

## 5. Official protocol

- [Envelopes](https://github.com/getsentry/develop/blob/26cabd61bbd94ac8cffd05f1cacd03950a5576c7/src/docs/sdk/envelopes.mdx): byte lengths, optional final LF, envelope event_id precedence, at most one event/transaction, unknown binary boundaries.
- [Rate limiting](https://github.com/getsentry/develop/blob/26cabd61bbd94ac8cffd05f1cacd03950a5576c7/src/docs/sdk/rate-limiting.mdx): category syntax, empty=all, headers on200,429/Retry-After.
- [Event payload](https://github.com/getsentry/develop/blob/26cabd61bbd94ac8cffd05f1cacd03950a5576c7/src/docs/sdk/event-payloads/index.mdx): common fields, time representations, fingerprints.
- [Client reports](https://github.com/getsentry/develop/blob/26cabd61bbd94ac8cffd05f1cacd03950a5576c7/src/docs/sdk/client-reports.mdx): discarded category/reason/quantity and best-effort loss reporting.

This is not Relay forwarding/storage. Supported-item ingestion plus unsupported diagnostics is a deliberate product restriction. Reading protocols does not establish Sentry grouping equivalence, symbolication, APM, or Replay support.

## 6. Required execution evidence

| Category | Minimum fixtures/oracles |
|---|---|
| Python | Exceptions/messages, three logging paths, enable_logs, templates/arrays/large ints, trace, flush/fork, hook drops |
| Node | Exceptions/levels/templates, multiple traces/batch, scoped attrs, flush, rate-limit/network outcomes |
| Browser | Real Playwright page, CORS/preflight, errors/logs/typed attrs, DSN/tunnel shape, disabled inference |
| Go | Real logger beyond exceptions/messages, versionless container, RFC3339/nanoseconds, plural parameters, flush |
| Protocol | Historical version1 evidence, missing/unknown versions, lengths/EOF/binary, duplicate keys, mixed support, event multiplicity |
| Values | i64/38-digit/bigger integers, typed double1.0, null/missing/empty, arrays/unit, Unicode/surrogates, dotted versus nested |
| Transport | Each enabled compression/limit,400/413/415/429/503, categories, reply loss and actual retry/drop |
| Analytics | Independent expected rows/count/avg/groups/raw correspondence, breadcrumb/log/error distinctions, repeated IDs |

Record SDK lock/commit/runtime/config including PII/sampling/log enable/flush, wire hash, scrubbed golden, expected records/outcomes, and server revision. Only synthetic secrets in fixture wire; never use production payloads. Do not derive expected results automatically from server output to hide failures.

## 7. Verification boundary

Completed: inspection of the listed ingestion/log sources/protocols, baseline lock comparison, and design contracts. Not completed: new Go SDK execution, DuckDB/native/Parquet experiments, AWS,512MiB/scaling/cost validation; these belong to G00–G08.

Not comprehensively inspected or guaranteed: Java/.NET/PHP/Ruby/Rust/mobile/native SDKs, all historical/future versions, or excluded Sentry features. New support requires pinned version, relevant wire source inspection, localhost capture, and normalization/search/failure oracles. HTTP200 alone is not compatibility.
