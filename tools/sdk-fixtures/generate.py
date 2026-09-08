#!/usr/bin/env python3
"""Capture real Sentry SDK HTTP sends into deterministic offline fixtures."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import zlib
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
TOOL_DIR = ROOT / "tools" / "sdk-fixtures"
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "sentry"
PUBLIC_KEY = "fixturePublicKey"
FIXED_DSN = f"http://{PUBLIC_KEY}@127.0.0.1:PORT/1"
FIXED_TIMESTAMP = "2026-01-02T03:04:05.000000Z"


EXPECTED: dict[str, dict[str, Any]] = {
    "python-events": {
        "schema_version": 1,
        "fixture_role": "normalization_contract_mapping",
        "assertion": "unordered_record_subsets",
        "server_normalization_executed": False,
        "mapping_only": True,
        "records": [
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "error", "message": "python fixture exception"},
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "warning", "message": "python fixture message"},
        ],
    },
    "python-logging-default": {
        "schema_version": 1,
        "fixture_role": "normalization_contract_mapping",
        "assertion": "unordered_record_subsets",
        "server_normalization_executed": False,
        "mapping_only": True,
        "records": [
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "info", "logger": "fixture.python", "message": "python default info"},
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "warning", "logger": "fixture.python", "message": "python default warning"},
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "error", "logger": "fixture.python", "message": "python default error"},
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "error", "logger": "fixture.python", "message": "python default error"},
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "fatal", "logger": "fixture.python", "message": "python default critical"},
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "fatal", "logger": "fixture.python", "message": "python default critical"},
        ],
        "must_not_contain": ["python default debug omitted"],
    },
    "python-logging-debug": {
        "schema_version": 1,
        "fixture_role": "normalization_contract_mapping",
        "assertion": "unordered_record_subsets",
        "server_normalization_executed": False,
        "mapping_only": True,
        "records": [
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "debug", "logger": "fixture.python", "message": "python opt-in debug"},
        ],
    },
    "python-fastapi": {
        "schema_version": 1,
        "fixture_role": "normalization_contract_mapping",
        "assertion": "unordered_record_subsets",
        "server_normalization_executed": False,
        "mapping_only": True,
        "records": [
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "error", "message": "fastapi fixture exception"},
        ],
    },
    "python-celery-fork": {
        "schema_version": 1,
        "fixture_role": "normalization_contract_mapping",
        "assertion": "unordered_record_subsets",
        "server_normalization_executed": False,
        "mapping_only": True,
        "records": [
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "error", "message": "celery fixture exception"},
        ],
    },
    "node-events-and-logs": {
        "schema_version": 1,
        "fixture_role": "normalization_contract_mapping",
        "assertion": "unordered_record_subsets",
        "server_normalization_executed": False,
        "mapping_only": True,
        "records": [
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "error", "message": "node fixture exception"},
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "warning", "message": "node fixture message"},
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "trace", "message": "node fixture trace"},
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "debug", "message": "node fixture debug"},
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "info", "message": "node fixture info"},
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "warning", "message": "node fixture warning"},
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "error", "message": "node fixture error"},
            {"kind": "log", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "fatal", "message": "node fixture fatal"},
        ],
    },
    "go-events": {
        "schema_version": 1,
        "fixture_role": "normalization_contract_mapping",
        "assertion": "unordered_record_subsets",
        "server_normalization_executed": False,
        "mapping_only": True,
        "records": [
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "error", "message": "go fixture exception"},
            {"kind": "error", "project_id": 1, "environment": "fixture", "release": "eventglass-sdk-fixture@1", "level": "info", "message": "go fixture message"},
        ],
    },
}


@dataclass
class Capture:
    path: str
    headers: dict[str, str]
    body: bytes
    response_status: int


class CaptureHandler(BaseHTTPRequestHandler):
    server_version = "EventglassFixtureCapture/1"

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        transfer_encoding = self.headers.get("Transfer-Encoding", "").lower()
        if transfer_encoding == "chunked":
            chunks = bytearray()
            while True:
                size_line = self.rfile.readline().strip()
                size = int(size_line.split(b";", 1)[0], 16)
                if size == 0:
                    while self.rfile.readline() not in {b"\r\n", b"\n", b""}:
                        pass
                    break
                chunks.extend(self.rfile.read(size))
                if self.rfile.read(2) != b"\r\n":
                    raise ValueError("malformed HTTP chunk terminator")
            body = bytes(chunks)
        else:
            length = int(self.headers["Content-Length"])
            body = self.rfile.read(length)
        capture = Capture(
            path=self.path,
            headers={key.lower(): value for key, value in self.headers.items()},
            body=body,
            response_status=202,
        )
        self.server.captures.append(capture)  # type: ignore[attr-defined]
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, _format: str, *_args: object) -> None:
        return


def decode_wire(body: bytes, encoding: str | None) -> bytes:
    if encoding is None or encoding == "identity":
        return body
    if encoding == "gzip":
        return gzip.decompress(body)
    if encoding == "deflate":
        return zlib.decompress(body)
    raise ValueError(f"unsupported content-encoding from pinned SDK: {encoding}")


def encode_wire(body: bytes, encoding: str | None) -> bytes:
    if encoding is None or encoding == "identity":
        return body
    if encoding == "gzip":
        return gzip.compress(body, mtime=0)
    if encoding == "deflate":
        return zlib.compress(body)
    raise ValueError(f"unsupported content-encoding from pinned SDK: {encoding}")


def split_envelope(data: bytes) -> tuple[dict[str, Any], list[tuple[dict[str, Any], bytes]]]:
    header_end = data.find(b"\n")
    if header_end < 0:
        raise ValueError("envelope header has no newline")
    envelope_header = json.loads(data[:header_end])
    items: list[tuple[dict[str, Any], bytes]] = []
    cursor = header_end + 1
    while cursor < len(data):
        item_header_end = data.find(b"\n", cursor)
        if item_header_end < 0:
            raise ValueError("item header has no newline")
        item_header = json.loads(data[cursor:item_header_end])
        cursor = item_header_end + 1
        if "length" in item_header:
            length = int(item_header["length"])
            payload = data[cursor : cursor + length]
            if len(payload) != length:
                raise ValueError("item payload is shorter than declared byte length")
            cursor += length
            if cursor < len(data) and data[cursor : cursor + 1] == b"\n":
                cursor += 1
        else:
            payload_end = data.find(b"\n", cursor)
            if payload_end < 0:
                payload_end = len(data)
            payload = data[cursor:payload_end]
            cursor = min(payload_end + 1, len(data))
        items.append((item_header, payload))
    return envelope_header, items


class Sanitizer:
    def __init__(self) -> None:
        self.ids: dict[tuple[str, str], str] = {}
        self.counters: dict[str, int] = {}

    def id_for(self, key: str, value: str) -> str:
        lookup = (key, value)
        if lookup not in self.ids:
            ordinal = self.counters.get(key, 0) + 1
            self.counters[key] = ordinal
            width = 16 if key == "span_id" else 32
            self.ids[lookup] = f"{ordinal:0{width}x}"
        return self.ids[lookup]

    def value(self, value: Any, key: str = "") -> Any:
        structured_dynamic = {"process.pid", "thread.id", "sentry.timestamp.sequence"}
        if key in structured_dynamic and isinstance(value, dict) and "value" in value:
            value = dict(value)
            value["value"] = 0 if key == "sentry.timestamp.sequence" else 4242
        if isinstance(value, dict):
            return {child_key: self.value(child, child_key) for child_key, child in value.items()}
        if isinstance(value, list):
            return [self.value(child, key) for child in value]
        if key in {"event_id", "trace_id", "span_id", "parent_span_id"} and isinstance(value, str):
            return self.id_for(key, value)
        if key in {"timestamp", "start_timestamp", "sent_at"}:
            return 1767323045.0 if isinstance(value, (int, float)) else FIXED_TIMESTAMP
        if key in {"pid", "process_id", "thread_id", "process.pid", "thread.id"} and isinstance(value, (int, str)):
            return 4242
        if key == "sentry.timestamp.sequence":
            return 0
        if key == "server_name":
            return "fixture-host"
        if isinstance(value, str):
            value = value.replace(str(ROOT), "<REPO>")
            value = re.sub(r"127\.0\.0\.1:\d+", "127.0.0.1:PORT", value)
            value = re.sub(r"localhost:\d+", "localhost:PORT", value)
        return value


def sanitize_envelope(raw: bytes, sanitizer: Sanitizer) -> tuple[bytes, list[str]]:
    envelope_header, items = split_envelope(raw)
    envelope_header = sanitizer.value(envelope_header)
    if "dsn" in envelope_header:
        envelope_header["dsn"] = FIXED_DSN
    item_types: list[str] = []
    output = bytearray(json.dumps(envelope_header, ensure_ascii=False, separators=(",", ":")).encode())
    output.extend(b"\n")
    for item_header, payload in items:
        item_types.append(str(item_header.get("type", "unknown")))
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            sanitized_payload = payload.replace(str(ROOT).encode(), b"<REPO>")
        else:
            sanitized_payload = json.dumps(
                sanitizer.value(decoded), ensure_ascii=False, separators=(",", ":")
            ).encode()
        item_header = sanitizer.value(item_header)
        item_header["length"] = len(sanitized_payload)
        output.extend(json.dumps(item_header, ensure_ascii=False, separators=(",", ":")).encode())
        output.extend(b"\n")
        output.extend(sanitized_payload)
        output.extend(b"\n")
    return bytes(output), item_types


def sanitize_headers(headers: dict[str, str], wire_length: int) -> dict[str, str]:
    sanitized: dict[str, str] = {}
    for key, value in sorted(headers.items()):
        value = re.sub(r"127\.0\.0\.1:\d+", "127.0.0.1:PORT", value)
        value = re.sub(r"sentry_key=[^, ]+", f"sentry_key={PUBLIC_KEY}", value)
        sanitized[key] = value
    sanitized.pop("transfer-encoding", None)
    sanitized["content-length"] = str(wire_length)
    if "host" in sanitized:
        sanitized["host"] = "127.0.0.1:PORT"
    return sanitized


def command_for(case: str) -> list[str]:
    if case.startswith("python-"):
        python = TOOL_DIR / ".venv" / "bin" / "python"
        mode = case.removeprefix("python-")
        return [str(python), str(TOOL_DIR / "python-app" / "app.py"), mode]
    if case == "node-events-and-logs":
        return ["node", str(TOOL_DIR / "node-app" / "app.mjs")]
    if case == "go-events":
        return [str(TOOL_DIR / "go-app" / "eventglass-go-fixture")]
    raise ValueError(case)


def sdk_version(case: str) -> tuple[str, str]:
    if case.startswith("python-"):
        result = subprocess.run(
            [str(TOOL_DIR / ".venv" / "bin" / "python"), "-c", "import importlib.metadata; print(importlib.metadata.version('sentry-sdk'))"],
            check=True,
            capture_output=True,
            text=True,
        )
        return "sentry-sdk", result.stdout.strip()
    package = json.loads((TOOL_DIR / "node-app" / "node_modules" / "@sentry" / "node" / "package.json").read_text())
    if case == "node-events-and-logs":
        return "@sentry/node", package["version"]
    if case == "go-events":
        return "github.com/getsentry/sentry-go", "0.49.0"
    raise ValueError(case)


def run_case(case: str) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), CaptureHandler)
    server.captures = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    env = os.environ.copy()
    env["SENTRY_FIXTURE_DSN"] = f"http://{PUBLIC_KEY}@127.0.0.1:{port}/1"
    env.pop("SENTRY_FIXTURE_SECRET", None)
    env.pop("SENTRY_FIXTURE_MODE", None)
    try:
        subprocess.run(command_for(case), cwd=ROOT, env=env, check=True, timeout=30)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    captures: list[Capture] = server.captures  # type: ignore[attr-defined]
    if not captures:
        raise RuntimeError(f"{case}: SDK sent no HTTP requests")

    expected_messages = [record["message"].encode() for record in EXPECTED[case]["records"]]

    def capture_sort_key(capture: Capture) -> tuple[int, str]:
        decoded = decode_wire(capture.body, capture.headers.get("content-encoding"))
        positions = [index for index, message in enumerate(expected_messages) if message in decoded]
        return (min(positions, default=len(expected_messages)), capture.path)

    captures.sort(key=capture_sort_key)

    target = FIXTURE_ROOT / case
    envelope_dir = target / "envelopes"
    envelope_dir.mkdir(parents=True, exist_ok=True)
    for stale in envelope_dir.glob("*.envelope"):
        stale.unlink()

    sanitizer = Sanitizer()
    header_manifest: list[dict[str, Any]] = []
    request_manifest: list[dict[str, Any]] = []
    all_sanitized = bytearray()
    for index, capture in enumerate(captures, start=1):
        encoding = capture.headers.get("content-encoding")
        raw_envelope = decode_wire(capture.body, encoding)
        sanitized_envelope, item_types = sanitize_envelope(raw_envelope, sanitizer)
        wire = encode_wire(sanitized_envelope, encoding)
        filename = f"{index:03d}.envelope"
        (envelope_dir / filename).write_bytes(wire)
        headers = sanitize_headers(capture.headers, len(wire))
        header_manifest.append(
            {
                "method": "POST",
                "path": re.sub(r"127\.0\.0\.1:\d+", "127.0.0.1:PORT", capture.path),
                "response_status": capture.response_status,
                "headers": headers,
            }
        )
        request_manifest.append(
            {
                "file": f"envelopes/{filename}",
                "content_encoding": encoding or "identity",
                "original_transfer_encoding": capture.headers.get("transfer-encoding", "content-length"),
                "wire_bytes": len(wire),
                "decompressed_bytes": len(sanitized_envelope),
                "sha256": hashlib.sha256(wire).hexdigest(),
                "item_types": item_types,
            }
        )
        all_sanitized.extend(sanitized_envelope)

    expected = EXPECTED[case]
    for forbidden in expected.get("must_not_contain", []):
        if forbidden.encode() in all_sanitized:
            raise RuntimeError(f"{case}: default-threshold DEBUG log was captured: {forbidden}")
    for record in expected["records"]:
        if record["message"].encode() not in all_sanitized:
            raise RuntimeError(f"{case}: expected message absent from SDK captures: {record['message']}")

    package_name, package_version = sdk_version(case)
    if case.startswith("python-"):
        runtime = subprocess.run(
            [sys.executable, "--version"], capture_output=True, text=True, check=True
        ).stdout.strip()
    elif case.startswith("node-"):
        runtime = subprocess.run(
            ["node", "--version"], capture_output=True, text=True, check=True
        ).stdout.strip()
    else:
        runtime = subprocess.run(
            ["go", "version"], capture_output=True, text=True, check=True
        ).stdout.strip()
    metadata = {
        "schema_version": 1,
        "case": case,
        "capture": "real_sdk_to_localhost_http",
        "sdk": {"package": package_name, "version": package_version},
        "runtime": runtime,
        "dsn": FIXED_DSN,
        "response": {"status": 202, "content_type": "application/json", "body": "{}"},
        "integration_options": (
            {"enable_logs": True, "logging_level": "INFO", "event_level": "ERROR", "sentry_logs_level": "DEBUG" if case == "python-logging-debug" else "default(INFO)"}
            if case.startswith("python-")
            else {"enableLogs": True, "defaultIntegrations": False}
            if case.startswith("node-")
            else {"attach_stacktrace": True, "flush_after_each_event": True}
        ),
        "expected_kinds": sorted({record["kind"] for record in expected["records"]}),
        "requests": request_manifest,
        "sanitization": [
            "dynamic event/trace/span IDs replaced by stable ordinal IDs",
            "timestamps fixed to 2026-01-02T03:04:05Z",
            "repository path replaced by <REPO>",
            "localhost ephemeral port replaced by PORT",
            "inner item byte lengths and outer HTTP content-length recomputed",
        ],
    }
    (target / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    (target / "headers.json").write_text(json.dumps(header_manifest, ensure_ascii=False, indent=2) + "\n")
    (target / "expected.normalized.json").write_text(json.dumps(expected, ensure_ascii=False, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=sorted(EXPECTED))
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if args.all == bool(args.case):
        parser.error("choose exactly one of --all or --case")
    cases = sorted(EXPECTED) if args.all else [args.case]
    for case in cases:
        run_case(case)
        print(f"captured {case}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
