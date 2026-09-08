#!/usr/bin/env python3
"""Run pinned Sentry SDKs through localhost HTTP into a real Eventglass process."""

from __future__ import annotations

import argparse
import contextlib
import http.client
import ipaddress
import json
import os
import secrets
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from http.cookiejar import CookieJar
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from generate import EXPECTED, ROOT, TOOL_DIR, command_for, decode_wire, split_envelope


SENTINEL = "eventglass-live-scrub-sentinel"
CASES = (
    "python-events",
    "python-logging-default",
    "python-logging-debug",
    "node-events-and-logs",
)
START_TIMEOUT_SECONDS = 15.0
SDK_TIMEOUT_SECONDS = 30.0


@dataclass
class Ack:
    case: str
    status: int
    body: dict[str, Any]
    content_encoding: str
    transfer_encoding: str
    wire_bytes: int
    decoded_bytes: int
    item_types: list[str]
    sentinel_sent: bool

    def report(self) -> dict[str, Any]:
        return {
            "case": self.case,
            "status": self.status,
            "accepted": self.body.get("accepted"),
            "first_ingest_seq": self.body.get("first_ingest_seq"),
            "last_ingest_seq": self.body.get("last_ingest_seq"),
            "content_encoding": self.content_encoding,
            "transfer_encoding": self.transfer_encoding,
            "wire_bytes": self.wire_bytes,
            "decoded_bytes": self.decoded_bytes,
            "item_types": self.item_types,
            "sentinel_sent": self.sentinel_sent,
        }


class ObserverServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], target_port: int) -> None:
        super().__init__(address, ObserverHandler)
        self.target_port = target_port
        self.current_case = "unassigned"
        self.acks: list[Ack] = []
        self.lock = threading.Lock()


class ObserverHandler(BaseHTTPRequestHandler):
    server_version = "EventglassSdkLiveObserver/1"

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        server: ObserverServer = self.server  # type: ignore[assignment]
        try:
            transfer = self.headers.get("Transfer-Encoding", "content-length").lower()
            body = self._read_body(transfer)
            forwarded_headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower() not in {"host", "connection", "transfer-encoding", "content-length"}
            }
            forwarded_headers["Content-Length"] = str(len(body))
            connection = http.client.HTTPConnection("127.0.0.1", server.target_port, timeout=10)
            try:
                connection.request("POST", self.path, body=body, headers=forwarded_headers)
                response = connection.getresponse()
                response_body = response.read()
                status = response.status
                response_type = response.getheader("Content-Type", "application/json")
            finally:
                connection.close()

            decoded = decode_wire(body, self.headers.get("Content-Encoding"))
            _header, items = split_envelope(decoded)
            parsed = json.loads(response_body or b"{}")
            with server.lock:
                server.acks.append(
                    Ack(
                        case=server.current_case,
                        status=status,
                        body=parsed,
                        content_encoding=self.headers.get("Content-Encoding", "identity").lower(),
                        transfer_encoding=transfer,
                        wire_bytes=len(body),
                        decoded_bytes=len(decoded),
                        item_types=[str(header.get("type", "unknown")) for header, _ in items],
                        sentinel_sent=SENTINEL.encode() in decoded,
                    )
                )
            self.send_response(status)
            self.send_header("Content-Type", response_type)
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)
        except Exception as error:  # noqa: BLE001 - preserve a useful SDK-side failure
            message = json.dumps({"error": str(error)}).encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(message)))
            self.end_headers()
            self.wfile.write(message)

    def _read_body(self, transfer: str) -> bytes:
        if transfer == "chunked":
            body = bytearray()
            while True:
                size_line = self.rfile.readline().strip()
                size = int(size_line.split(b";", 1)[0], 16)
                if size == 0:
                    while self.rfile.readline() not in {b"\r\n", b"\n", b""}:
                        pass
                    return bytes(body)
                body.extend(self.rfile.read(size))
                if self.rfile.read(2) != b"\r\n":
                    raise ValueError("malformed HTTP chunk terminator")
        length = int(self.headers["Content-Length"])
        return self.rfile.read(length)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def assert_loopback(url: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname is None:
        raise RuntimeError(f"live runner requires a localhost HTTP URL: {url}")
    try:
        loopback = ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        loopback = parsed.hostname == "localhost"
    if not loopback:
        raise RuntimeError(f"live runner refuses a non-loopback URL: {url}")


def verify_dependencies() -> dict[str, str]:
    python = TOOL_DIR / ".venv" / "bin" / "python"
    if not python.is_file():
        raise RuntimeError("missing pinned Python environment; run tools/sdk-fixtures/bootstrap.sh")
    required: dict[str, str] = {}
    for line in (TOOL_DIR / "python-app" / "requirements.lock").read_text().splitlines():
        if line and not line.startswith("#"):
            package, version = line.split("==", 1)
            required[package] = version
    probe = (
        "import importlib.metadata,json; "
        f"print(json.dumps({{p:importlib.metadata.version(p) for p in {list(required)!r}}}))"
    )
    installed = json.loads(subprocess.run(
        [str(python), "-c", probe], check=True, capture_output=True, text=True
    ).stdout)
    if installed != required:
        raise RuntimeError(f"pinned Python environment mismatch: {installed!r} != {required!r}")

    node = shutil.which("node")
    node_package = TOOL_DIR / "node-app" / "node_modules" / "@sentry" / "node" / "package.json"
    if node is None or not node_package.is_file():
        raise RuntimeError("missing pinned Node environment; run tools/sdk-fixtures/bootstrap.sh")
    installed_node_sdk = json.loads(node_package.read_text())["version"]
    lock = json.loads((TOOL_DIR / "node-app" / "package-lock.json").read_text())
    locked_node_sdk = lock["packages"]["node_modules/@sentry/node"]["version"]
    if installed_node_sdk != locked_node_sdk or installed_node_sdk != "10.73.0":
        raise RuntimeError("pinned @sentry/node environment does not match package-lock.json")
    node_version = subprocess.run(
        [node, "--version"], check=True, capture_output=True, text=True
    ).stdout.strip()
    python_version = subprocess.run(
        [str(python), "--version"], check=True, capture_output=True, text=True
    ).stdout.strip()
    return {
        "python": python_version,
        "sentry_sdk_python": installed["sentry-sdk"],
        "node": node_version,
        "sentry_sdk_node": installed_node_sdk,
    }


def request_json(
    opener: urllib.request.OpenerDirector,
    method: str,
    url: str,
    body: dict[str, Any],
    origin: str,
    csrf: str | None = None,
) -> tuple[int, dict[str, Any]]:
    assert_loopback(url)
    headers = {"Content-Type": "application/json", "Origin": origin}
    if csrf is not None:
        headers["X-CSRF-Token"] = csrf
    request = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers=headers, method=method
    )
    try:
        with opener.open(request, timeout=10) as response:
            raw = response.read()
            return response.status, json.loads(raw or b"{}")
    except urllib.error.HTTPError as error:
        raw = error.read()
        raise RuntimeError(f"{method} {url} returned {error.code}: {raw.decode(errors='replace')}") from error


def wait_ready(base_url: str) -> None:
    deadline = time.monotonic() + START_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/readyz", timeout=1) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(0.05)
    raise RuntimeError(f"Eventglass did not become ready: {last_error}")


def start_server(binary: Path, environment: dict[str, str], log_path: Path) -> subprocess.Popen[bytes]:
    log = log_path.open("ab")
    process = subprocess.Popen(
        [str(binary), "serve"], cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT
    )
    log.close()
    return process


def stop_server(process: subprocess.Popen[bytes], log_path: Path) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise RuntimeError("Eventglass did not stop after SIGINT")
    if process.returncode != 0:
        log = log_path.read_text(errors="replace")[-4000:]
        raise RuntimeError(f"Eventglass exited {process.returncode}:\n{log}")


def read_ledger(database: Path) -> dict[str, int]:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=5)
    try:
        row = connection.execute(
            "SELECT next_ingest_seq,last_applied_inbox_id,last_applied_ingest_seq,"
            "inbox_records,inbox_bytes FROM runtime_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise RuntimeError("runtime_state is absent")
        return dict(zip(
            ("next_ingest_seq", "last_applied_inbox_id", "last_applied_ingest_seq", "inbox_records", "inbox_bytes"),
            map(int, row),
            strict=True,
        ))
    finally:
        connection.close()


def scrub_environment(environment: dict[str, str]) -> dict[str, str]:
    clean = dict(environment)
    for key in list(clean):
        if key.upper() in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"}:
            clean.pop(key)
    clean["NO_PROXY"] = "127.0.0.1,localhost"
    return clean


def durable_record_count(ledger: dict[str, int]) -> int:
    return ledger["last_applied_ingest_seq"] + ledger["inbox_records"]


def wait_fully_indexed(database: Path, expected: int) -> dict[str, int]:
    deadline = time.monotonic() + START_TIMEOUT_SECONDS
    last: dict[str, int] | None = None
    while time.monotonic() < deadline:
        last = read_ledger(database)
        if last["last_applied_ingest_seq"] == expected and last["inbox_records"] == 0:
            return last
        time.sleep(0.05)
    raise RuntimeError(f"Indexer did not publish the acknowledged cut: {last!r}")


def validate_data_dir(data_dir: Path) -> Path:
    data_dir = data_dir.resolve()
    marker = data_dir / ".sdk-live-owned"
    if not data_dir.is_dir() or marker.read_text() != "Eventglass SDK live fixture\n":
        raise RuntimeError("supplied data directory lacks the SDK live ownership marker")
    if set(data_dir.iterdir()) != {marker}:
        raise RuntimeError("supplied SDK live data directory must otherwise be empty")
    return data_dir


def run(binary: Path, report_path: Path | None, supplied_data_dir: Path) -> dict[str, Any]:
    versions = verify_dependencies()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"built Eventglass binary is missing or not executable: {binary}")
    binary_version = subprocess.run(
        [str(binary), "--version"], check=True, capture_output=True, text=True
    ).stdout.strip()

    started = time.monotonic()
    supplied_data_dir = validate_data_dir(supplied_data_dir)
    with contextlib.nullcontext(str(supplied_data_dir)) as temporary:
        data_dir = Path(temporary)
        marker = data_dir / ".sdk-live-owned"
        if not marker.exists():
            marker.write_text("Eventglass SDK live fixture\n")
        server_port = free_port()
        observer_port = free_port()
        base_url = f"http://127.0.0.1:{server_port}"
        observer_url = f"http://127.0.0.1:{observer_port}"
        assert_loopback(base_url)
        assert_loopback(observer_url)
        environment = scrub_environment(os.environ.copy())
        environment.update({
            "EVENTGLASS_ADDR": f"127.0.0.1:{server_port}",
            "EVENTGLASS_DATA_DIR": str(data_dir),
            "EVENTGLASS_BASE_URL": base_url,
        })
        setup_token = subprocess.run(
            [str(binary), "admin", "setup-token"],
            cwd=ROOT,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout.strip()
        if not setup_token:
            raise RuntimeError("setup-token command returned no token")

        log_path = data_dir / "server.log"
        server = start_server(binary, environment, log_path)
        observer = ObserverServer(("127.0.0.1", observer_port), server_port)
        observer_thread = threading.Thread(target=observer.serve_forever, daemon=True)
        observer_thread.start()
        try:
            wait_ready(base_url)
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
            password = secrets.token_urlsafe(24)
            status, _ = request_json(
                opener,
                "POST",
                f"{base_url}/api/setup",
                {"token": setup_token, "email": "sdk-live@example.invalid", "password": password},
                base_url,
            )
            if status != 201:
                raise RuntimeError(f"setup returned {status}")
            status, login = request_json(
                opener,
                "POST",
                f"{base_url}/api/auth/login",
                {"email": "sdk-live@example.invalid", "password": password},
                base_url,
            )
            if status != 200 or not isinstance(login.get("csrf_token"), str):
                raise RuntimeError("login did not return a CSRF token")
            csrf = login["csrf_token"]
            status, project = request_json(
                opener,
                "POST",
                f"{base_url}/api/projects",
                {"slug": "sdk-live", "name": "SDK Live"},
                base_url,
                csrf,
            )
            if status != 201:
                raise RuntimeError(f"project creation returned {status}")
            project_id = int(project["id"])
            status, key = request_json(
                opener,
                "POST",
                f"{base_url}/api/projects/{project_id}/keys",
                {},
                base_url,
                csrf,
            )
            if status != 201:
                raise RuntimeError(f"key creation returned {status}")
            sdk_dsn = f"http://{key['public_key']}@127.0.0.1:{observer_port}/{project_id}"

            expected_total = 0
            case_ledgers: list[dict[str, Any]] = []
            for case in CASES:
                before_count = len(observer.acks)
                observer.current_case = case
                sdk_environment = scrub_environment(environment)
                sdk_environment["SENTRY_FIXTURE_DSN"] = sdk_dsn
                sdk_environment["SENTRY_FIXTURE_SECRET"] = SENTINEL
                sdk_environment["SENTRY_FIXTURE_MODE"] = "live-sequential"
                subprocess.run(
                    command_for(case),
                    cwd=ROOT,
                    env=sdk_environment,
                    check=True,
                    timeout=SDK_TIMEOUT_SECONDS,
                )
                with observer.lock:
                    case_acks = list(observer.acks[before_count:])
                accepted = sum(int(ack.body.get("accepted", -1)) for ack in case_acks)
                expected_case = len(EXPECTED[case]["records"])
                if not case_acks or any(ack.status != 202 for ack in case_acks):
                    responses = [
                        {"status": ack.status, "body": ack.body, "item_types": ack.item_types}
                        for ack in case_acks
                    ]
                    raise RuntimeError(
                        f"{case}: SDK did not observe only Eventglass 202 responses: {responses!r}"
                    )
                if accepted != expected_case:
                    raise RuntimeError(f"{case}: accepted {accepted}, expected {expected_case}")
                if not any(ack.sentinel_sent for ack in case_acks):
                    raise RuntimeError(f"{case}: scrub sentinel was absent from SDK requests")
                expected_total += expected_case
                ledger = read_ledger(data_dir / "meta.db")
                if (
                    ledger["next_ingest_seq"] != expected_total + 1
                    or durable_record_count(ledger) != expected_total
                ):
                    raise RuntimeError(f"{case}: durable ACK ledger did not advance")
                case_ledgers.append({
                    "case": case,
                    "accepted": accepted,
                    "next_ingest_seq": ledger["next_ingest_seq"],
                    "requests": len(case_acks),
                })

            with observer.lock:
                acks = list(observer.acks)
            if not any(ack.content_encoding == "gzip" and ack.case.startswith("python-") for ack in acks):
                raise RuntimeError("pinned Python SDK did not exercise gzip transport")
            node_acks = [ack for ack in acks if ack.case == "node-events-and-logs"]
            if len(node_acks) != 3 or any(
                ack.transfer_encoding != "chunked" for ack in node_acks
            ):
                raise RuntimeError("all three pinned Node SDK requests must use chunked transfer")
            item_types = {item_type for ack in acks for item_type in ack.item_types}
            if not {"event", "log"}.issubset(item_types):
                raise RuntimeError(f"actual SDK envelope framing lacked event/log items: {item_types}")

            ledger_before = wait_fully_indexed(data_dir / "meta.db", expected_total)
        finally:
            observer.shutdown()
            observer.server_close()
            observer_thread.join(timeout=5)
            stop_server(server, log_path)

        server = start_server(binary, environment, log_path)
        try:
            wait_ready(base_url)
        finally:
            stop_server(server, log_path)
        ledger_after = read_ledger(data_dir / "meta.db")
        if ledger_after != ledger_before or durable_record_count(ledger_after) != expected_total:
            raise RuntimeError("durable ACK ledger changed across graceful restart")

        database_bytes = sum(
            path.stat().st_size
            for path in data_dir.glob("meta.db*")
            if path.is_file()
        )
        report = {
            "schema_version": 1,
            "result": "pass",
            "localhost_only": True,
            "binary": binary_version,
            "versions": versions,
            "cases": case_ledgers,
            "requests": [ack.report() for ack in acks],
            "acknowledged_records": expected_total,
            "durable_cut_before_restart": durable_record_count(ledger_before),
            "durable_cut_after_restart": durable_record_count(ledger_after),
            "ledger_before_restart": ledger_before,
            "ledger_after_restart": ledger_after,
            "gzip_exercised": True,
            "chunked_transfer_exercised": True,
            "event_and_log_envelope_items_exercised": True,
            "sdk_mode": "live-sequential",
            "native_content_inspection_required": True,
            "database_bytes_at_end": database_bytes,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
        if report_path is not None:
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        return report


def run_direct_node(
    binary: Path, report_path: Path | None, supplied_data_dir: Path
) -> dict[str, Any]:
    versions = verify_dependencies()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"built Eventglass binary is missing or not executable: {binary}")
    binary_version = subprocess.run(
        [str(binary), "--version"], check=True, capture_output=True, text=True
    ).stdout.strip()
    data_dir = validate_data_dir(supplied_data_dir)
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    assert_loopback(base_url)
    environment = scrub_environment(os.environ.copy())
    environment.update({
        "EVENTGLASS_ADDR": f"127.0.0.1:{port}",
        "EVENTGLASS_DATA_DIR": str(data_dir),
        "EVENTGLASS_BASE_URL": base_url,
    })
    setup_token = subprocess.run(
        [str(binary), "admin", "setup-token"],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    ).stdout.strip()
    if not setup_token:
        raise RuntimeError("setup-token command returned no token")

    started = time.monotonic()
    log_path = data_dir / "server.log"
    server = start_server(binary, environment, log_path)
    try:
        wait_ready(base_url)
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
        password = secrets.token_urlsafe(24)
        status, _ = request_json(
            opener,
            "POST",
            f"{base_url}/api/setup",
            {"token": setup_token, "email": "sdk-direct@example.invalid", "password": password},
            base_url,
        )
        if status != 201:
            raise RuntimeError(f"direct Node setup returned {status}")
        status, login = request_json(
            opener,
            "POST",
            f"{base_url}/api/auth/login",
            {"email": "sdk-direct@example.invalid", "password": password},
            base_url,
        )
        if status != 200 or not isinstance(login.get("csrf_token"), str):
            raise RuntimeError("direct Node login did not return a CSRF token")
        csrf = login["csrf_token"]
        status, project = request_json(
            opener,
            "POST",
            f"{base_url}/api/projects",
            {"slug": "sdk-direct", "name": "SDK Direct"},
            base_url,
            csrf,
        )
        if status != 201:
            raise RuntimeError(f"direct Node project creation returned {status}")
        project_id = int(project["id"])
        status, key = request_json(
            opener,
            "POST",
            f"{base_url}/api/projects/{project_id}/keys",
            {},
            base_url,
            csrf,
        )
        if status != 201:
            raise RuntimeError(f"direct Node key creation returned {status}")
        sdk_dsn = str(key["dsn"])
        assert_loopback(sdk_dsn)
        sdk_environment = scrub_environment(environment)
        sdk_environment["SENTRY_FIXTURE_DSN"] = sdk_dsn
        sdk_environment["SENTRY_FIXTURE_SECRET"] = SENTINEL
        sdk_environment["SENTRY_FIXTURE_MODE"] = "live-sequential"
        subprocess.run(
            command_for("node-events-and-logs"),
            cwd=ROOT,
            env=sdk_environment,
            check=True,
            timeout=SDK_TIMEOUT_SECONDS,
        )
        ledger = wait_fully_indexed(data_dir / "meta.db", 8)
        if ledger["next_ingest_seq"] != 9 or durable_record_count(ledger) != 8:
            raise RuntimeError("direct Node durable cut differs from eight expected records")
    finally:
        stop_server(server, log_path)

    report = {
        "schema_version": 1,
        "result": "pass",
        "mode": "direct_node",
        "localhost_only": True,
        "transport_observer": False,
        "binary": binary_version,
        "versions": versions,
        "expected_records": 8,
        "durable_cut": durable_record_count(ledger),
        "ledger": ledger,
        "sdk_flush_completed": True,
        "sdk_mode": "live-sequential",
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    if report_path is not None:
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--binary",
        type=Path,
        default=Path(os.environ.get("EVENTGLASS_BIN", ROOT / "target" / "debug" / "eventglass")),
        help="already-built Eventglass binary (default: EVENTGLASS_BIN or target/debug/eventglass)",
    )
    parser.add_argument("--report", type=Path, help="optional sanitized JSON result path")
    parser.add_argument(
        "--mode", choices=("observer", "direct-node"), default="observer"
    )
    parser.add_argument("--check-dependencies", action="store_true")
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="test-owned empty directory containing .sdk-live-owned; retained for native inspection",
    )
    arguments = parser.parse_args()
    if arguments.check_dependencies:
        print(json.dumps(verify_dependencies(), ensure_ascii=False, indent=2))
        return 0
    if arguments.data_dir is None:
        parser.error("--data-dir is required unless --check-dependencies is used")
    if arguments.mode == "direct-node":
        report = run_direct_node(
            arguments.binary.resolve(), arguments.report, arguments.data_dir
        )
    else:
        report = run(arguments.binary.resolve(), arguments.report, arguments.data_dir)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, OSError, RuntimeError, subprocess.SubprocessError, ValueError) as error:
        print(f"sdk live fixture failed: {error}", file=sys.stderr)
        raise SystemExit(1)
