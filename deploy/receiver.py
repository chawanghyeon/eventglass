"""Receive immutable Eventglass ARM64 binaries and activate one exact commit."""

import hashlib
import os
from pathlib import Path
import pwd
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request


ROOT = Path("/data/eventglass-deploy")
BIN = Path("/usr/local/bin/eventglass")
LIMIT = 128 * 1024 * 1024


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate(path, revision):
    with path.open("rb") as stream:
        head = stream.read(20)
    if head[:6] != b"\x7fELF\x02\x01" or head[18:20] != b"\xb7\x00":
        raise ValueError("candidate is not a Linux AArch64 ELF binary")
    account = pwd.getpwnam("eventglass")
    result = subprocess.run(
        [str(path), "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
        user=account.pw_uid,
        group=account.pw_gid,
        extra_groups=[],
    )
    if result.stdout.strip() != f"eventglass 0.1.0 ({revision})":
        raise ValueError("candidate revision does not match staged commit")


def sync(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_active(revision):
    temporary = ROOT / "active.new"
    temporary.write_text(revision + "\n")
    sync(temporary)
    os.replace(temporary, ROOT / "active")
    sync(ROOT)


def stage(stream, revision, digest):
    ROOT.mkdir(mode=0o711, parents=True, exist_ok=True)
    ROOT.chmod(0o711)
    with tempfile.TemporaryDirectory(prefix="incoming-", dir=ROOT) as temporary:
        temporary = Path(temporary)
        temporary.chmod(0o755)
        candidate = temporary / "eventglass"
        with candidate.open("wb") as output:
            count = 0
            while block := stream.read(1024 * 1024):
                count += len(block)
                if count > LIMIT:
                    raise ValueError("candidate exceeds staging budget")
                output.write(block)
        if count == 0 or sha256(candidate) != digest:
            raise ValueError("candidate checksum mismatch")
        candidate.chmod(0o755)
        validate(candidate, revision)
        destination = ROOT / revision
        if destination.exists():
            if (destination / "sha256").read_text().strip() != digest:
                raise ValueError("commit is already staged with different bytes")
        else:
            (temporary / "sha256").write_text(digest + "\n")
            sync(candidate)
            sync(temporary / "sha256")
            sync(temporary)
            os.rename(temporary, destination)
            sync(ROOT)
    print(f"[recv] staged {revision}", flush=True)


def healthy():
    try:
        with urllib.request.urlopen("http://127.0.0.1:8088/readyz", timeout=1) as response:
            return response.status == 200
    except (OSError, TimeoutError):
        return False


def activate(revision):
    directory = ROOT / revision
    candidate = directory / "eventglass"
    digest = directory / "sha256"
    if not candidate.is_file() or not digest.is_file():
        raise ValueError("commit was not staged locally before push")
    if sha256(candidate) != digest.read_text().strip():
        raise ValueError("staged candidate checksum mismatch")
    validate(candidate, revision)
    installed = Path(f"{BIN}.{revision}")
    if BIN.is_symlink() and BIN.resolve() == installed.resolve() and installed.is_file():
        if sha256(installed) == digest.read_text().strip() and healthy():
            write_active(revision)
            print(f"[recv] already healthy {revision}", flush=True)
            return
    shutil.copy2(candidate, f"{installed}.new")
    os.chmod(f"{installed}.new", 0o755)
    sync(f"{installed}.new")
    os.replace(f"{installed}.new", installed)
    sync(installed.parent)
    previous = BIN.resolve() if BIN.is_symlink() else None
    replacement = Path(f"{BIN}.next")
    replacement.unlink(missing_ok=True)
    replacement.symlink_to(installed)
    os.replace(replacement, BIN)
    sync(BIN.parent)
    try:
        subprocess.run(["systemctl", "restart", "eventglass"], check=True, timeout=40)
        for _ in range(30):
            if healthy():
                write_active(revision)
                break
            time.sleep(1)
        else:
            raise ValueError("production readiness check failed")
    except Exception:
        if previous is not None and previous.is_file():
            replacement.unlink(missing_ok=True)
            replacement.symlink_to(previous)
            os.replace(replacement, BIN)
            subprocess.run(["systemctl", "restart", "eventglass"], check=True, timeout=40)
        else:
            BIN.unlink(missing_ok=True)
            subprocess.run(["systemctl", "stop", "eventglass"], check=False, timeout=40)
        raise
    # Housekeeping failure must not roll back a healthy, recorded deployment.
    try:
        prune(revision, previous)
    except OSError as error:
        print(f"[recv] cleanup deferred: {error}", file=sys.stderr)
    print(f"[recv] healthy {revision}", flush=True)


def prune(active, previous=None):
    releases = sorted(
        (
            path
            for path in ROOT.iterdir()
            if path.is_dir() and re.fullmatch(r"[0-9a-f]{40}", path.name)
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in releases[5:]:
        installed = Path(f"{BIN}.{path.name}")
        if (
            path.name != active
            and installed != previous
            and time.time() - path.stat().st_mtime > 86400
        ):
            shutil.rmtree(path)
            installed.unlink(missing_ok=True)


def main():
    stream = sys.stdin.buffer
    request = stream.readline(256).decode("ascii").strip().split()
    if len(request) not in (2, 3) or not re.fullmatch(r"[0-9a-f]{40}", request[1]):
        raise ValueError("invalid deployment request")
    allowed = sys.argv[1] if len(sys.argv) == 2 else "all"
    if allowed not in ("all", "stage", "activate") or (
        allowed != "all" and request[0] != allowed
    ):
        raise ValueError("deployment operation is not allowed for this key")
    if (
        request[0] == "stage"
        and len(request) == 3
        and re.fullmatch(r"[0-9a-f]{64}", request[2])
    ):
        stage(stream, request[1], request[2])
    elif request[0] == "activate" and len(request) == 2:
        activate(request[1])
    else:
        raise ValueError("expected stage SHA SHA256 or activate SHA")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"[recv] FAILED: {error}", file=sys.stderr)
        sys.exit(1)
