#!/usr/bin/env python3
"""Fail when the repository-local quality environment drifts from its lock."""

from __future__ import annotations

import importlib.metadata
import pathlib
import sys


def read_versions() -> dict[str, str]:
    root = pathlib.Path(__file__).resolve().parents[2]
    values: dict[str, str] = {}
    for raw_line in (root / "tools" / "versions.env").read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def main() -> int:
    expected = read_versions()
    packages = {
        "pre-commit": expected["PRE_COMMIT_VERSION"],
        "detect-secrets": expected["DETECT_SECRETS_VERSION"],
        "PyYAML": expected["PYYAML_VERSION"],
    }
    mismatches = []
    for package, wanted in packages.items():
        try:
            actual = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            actual = "missing"
        if actual != wanted:
            mismatches.append(f"{package}: {actual} (expected {wanted})")
    if mismatches:
        print("quality tool mismatch; run ./scripts/bootstrap", file=sys.stderr)
        for mismatch in mismatches:
            print(f"  {mismatch}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
