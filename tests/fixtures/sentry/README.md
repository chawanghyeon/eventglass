# Sentry SDK capture fixtures

These fixtures are sanitized captures from real pinned SDKs sending to the localhost HTTP collector in `tools/sdk-fixtures/generate.py`. They contain no Sentry credentials and make no external ingest request.

Each case contains:

- `metadata.json`: SDK/runtime/options, response contract, encoding, byte counts, hashes, and item types.
- `headers.json`: sanitized replay headers for every HTTP request. Node originally used HTTP chunked transfer; replay headers use a recomputed `Content-Length` for the stored body and metadata preserves the original transfer encoding.
- `envelopes/*.envelope`: the exact stored HTTP request body, including the SDK's observed content encoding. Inner envelope item lengths are recomputed after sanitization.
- `expected.normalized.json`: unordered partial `Record` mappings checked by the offline normalizer suite and the explicit live Eventglass gate.
- `generate.sh`: regeneration entry point for that case after `tools/sdk-fixtures/bootstrap.sh`.

Run `tools/sdk-fixtures/verify.py` for dependency-free offline byte/protocol checks. Run `tools/sdk-fixtures/generate-all.sh` only when intentionally refreshing captures from the pinned SDK apps.

Run `tools/sdk-fixtures/bootstrap-live.sh` to install the exact Python lock and Node package-lock into ignored task-local directories and verify their versions. After building `target/debug/eventglass`, run `tools/sdk-fixtures/run-live.sh` for the explicit localhost gate. It creates marked temporary data directories, configures projects through the real admin API, checks observer-captured 202 receipts, restarts the first process, inspects published native documents, and repeats the Node case directly against a second Eventglass process without the observer. Missing or mismatched SDK environments fail this gate; they are never silently skipped.
