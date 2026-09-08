#!/usr/bin/env python3
"""Check text hygiene without modifying staged or working-tree files."""

from __future__ import annotations

import pathlib
import sys


CONFLICT_MARKERS = (b"<<<<<<< ", b"=======", b">>>>>>> ")


def check(path: pathlib.Path) -> list[str]:
    data = path.read_bytes()
    if b"\0" in data:
        return []
    problems: list[str] = []
    if data and not data.endswith(b"\n"):
        problems.append("missing final newline")
    for line_number, line in enumerate(data.splitlines(), start=1):
        if any(line.startswith(marker) for marker in CONFLICT_MARKERS):
            problems.append(f"line {line_number}: merge conflict marker")
        stripped = line.rstrip(b" \t")
        whitespace = line[len(stripped) :]
        markdown_break = path.suffix.lower() in {".md", ".markdown"} and whitespace == b"  "
        if whitespace and not markdown_break:
            problems.append(f"line {line_number}: trailing whitespace")
    return problems


def main(paths: list[str]) -> int:
    failed = False
    for name in paths:
        path = pathlib.Path(name)
        try:
            problems = check(path)
        except UnicodeError as error:
            print(f"{path}: invalid text encoding: {error}", file=sys.stderr)
            failed = True
            continue
        for problem in problems:
            print(f"{path}: {problem}", file=sys.stderr)
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
