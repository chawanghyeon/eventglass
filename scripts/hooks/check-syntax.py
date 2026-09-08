#!/usr/bin/env python3
"""Parse JSON, TOML, and all YAML documents without rewriting them."""

from __future__ import annotations

import json
import pathlib
import sys
import tomllib

import yaml


def parse(path: pathlib.Path) -> None:
    suffix = path.suffix.lower()
    with path.open("rb") as stream:
        if suffix == ".json":
            json.load(stream)
        elif suffix == ".toml" or path.name == "Cargo.lock":
            tomllib.load(stream)
        elif suffix in {".yaml", ".yml"}:
            list(yaml.safe_load_all(stream))
        else:
            raise ValueError(f"unsupported syntax file: {path}")


def main(paths: list[str]) -> int:
    failed = False
    for name in paths:
        path = pathlib.Path(name)
        try:
            parse(path)
        except Exception as error:
            print(f"{path}: {error}", file=sys.stderr)
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
