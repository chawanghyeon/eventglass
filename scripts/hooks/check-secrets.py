#!/usr/bin/env python3
"""Scan against audited fingerprints without mutating the tracked baseline."""

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    scanner = Path(sys.executable).parent / "detect-secrets-hook"
    files = [name for name in sys.argv[1:] if Path(name).resolve() != root / ".secrets.baseline"]
    if not files:
        return 0
    with tempfile.TemporaryDirectory(prefix="eventglass-secret-check-") as directory:
        baseline = Path(directory) / "baseline.json"
        shutil.copyfile(root / ".secrets.baseline", baseline)
        return subprocess.call([str(scanner), "--baseline", str(baseline), *files])


if __name__ == "__main__":
    raise SystemExit(main())
