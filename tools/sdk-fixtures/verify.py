#!/usr/bin/env python3
"""Verify committed fixtures without importing or installing either Sentry SDK."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

from generate import EXPECTED, FIXTURE_ROOT, ROOT, decode_wire, split_envelope


def verify_case(case: str) -> None:
    target = FIXTURE_ROOT / case
    metadata = json.loads((target / "metadata.json").read_text())
    headers = json.loads((target / "headers.json").read_text())
    expected = json.loads((target / "expected.normalized.json").read_text())
    if expected != EXPECTED[case]:
        raise AssertionError(f"{case}: expected.normalized.json differs from the generator contract")
    if metadata["capture"] != "real_sdk_to_localhost_http":
        raise AssertionError(f"{case}: capture provenance is missing")
    if len(headers) != len(metadata["requests"]):
        raise AssertionError(f"{case}: header/request count mismatch")

    decoded_bodies: list[bytes] = []
    actual_records: list[dict[str, object]] = []
    for request, header_entry in zip(metadata["requests"], headers, strict=True):
        wire = (target / request["file"]).read_bytes()
        if len(wire) != request["wire_bytes"]:
            raise AssertionError(f"{case}/{request['file']}: wire byte count mismatch")
        if hashlib.sha256(wire).hexdigest() != request["sha256"]:
            raise AssertionError(f"{case}/{request['file']}: sha256 mismatch")
        if int(header_entry["headers"]["content-length"]) != len(wire):
            raise AssertionError(f"{case}/{request['file']}: HTTP content-length mismatch")
        decoded = decode_wire(wire, None if request["content_encoding"] == "identity" else request["content_encoding"])
        if len(decoded) != request["decompressed_bytes"]:
            raise AssertionError(f"{case}/{request['file']}: decompressed byte count mismatch")
        _header, items = split_envelope(decoded)
        if [str(item_header.get("type", "unknown")) for item_header, _payload in items] != request["item_types"]:
            raise AssertionError(f"{case}/{request['file']}: item type mismatch")
        for item_header, payload_bytes in items:
            payload = json.loads(payload_bytes)
            if item_header["type"] == "event":
                message = payload.get("message")
                if not isinstance(message, str):
                    message = payload.get("logentry", {}).get("formatted")
                if not isinstance(message, str):
                    message = payload["exception"]["values"][-1]["value"]
                record: dict[str, object] = {
                    "kind": "error",
                    "project_id": 1,
                    "environment": payload.get("environment"),
                    "release": payload.get("release"),
                    "level": payload.get("level", "error"),
                    "message": message,
                }
                if payload.get("logger") is not None:
                    record["logger"] = payload["logger"]
                actual_records.append(record)
            elif item_header["type"] == "log":
                for log in payload["items"]:
                    attributes = log.get("attributes", {})
                    level = "warning" if log["level"] == "warn" else log["level"]
                    record = {
                        "kind": "log",
                        "project_id": 1,
                        "environment": attributes["sentry.environment"]["value"],
                        "release": attributes["sentry.release"]["value"],
                        "level": level,
                        "message": log["body"],
                    }
                    if "logger.name" in attributes:
                        record["logger"] = attributes["logger.name"]["value"]
                    actual_records.append(record)
        decoded_bodies.append(decoded)

    joined = b"".join(decoded_bodies)
    forbidden_bytes = [str(ROOT).encode()]
    for forbidden in forbidden_bytes:
        if forbidden in joined:
            raise AssertionError(f"{case}: unsanitized dynamic value remains: {forbidden!r}")
    for message in expected.get("must_not_contain", []):
        if message.encode() in joined:
            raise AssertionError(f"{case}: forbidden message is present: {message}")
    for record in expected["records"]:
        if record["message"].encode() not in joined:
            raise AssertionError(f"{case}: expected message is absent: {record['message']}")
    if re.search(rb"127\.0\.0\.1:\d+", joined):
        raise AssertionError(f"{case}: unsanitized localhost port remains")

    sort_key = lambda record: json.dumps(record, sort_keys=True, ensure_ascii=False)
    if sorted(actual_records, key=sort_key) != sorted(expected["records"], key=sort_key):
        raise AssertionError(f"{case}: raw event/log records do not match expected mapping subsets")


def main() -> int:
    for case in sorted(EXPECTED):
        verify_case(case)
        print(f"verified {case}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, KeyError, OSError, ValueError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
