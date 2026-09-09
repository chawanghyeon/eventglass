#!/usr/bin/env python3
"""Generate deterministic SPDX, third-party notices, and checksums."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import re
import subprocess
import sys
import uuid


def run(*args: str, cwd: Path) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def spdx_id(ecosystem: str, name: str, version: str) -> str:
    key = f"{ecosystem}:{name}:{version}".encode()
    safe = re.sub(r"[^A-Za-z0-9.-]", "-", name).strip("-") or "package"
    return f"SPDXRef-{ecosystem}-{safe}-{hashlib.sha256(key).hexdigest()[:12]}"


def rust_packages(root: Path) -> list[dict[str, str]]:
    metadata = json.loads(
        run(
            "cargo",
            "metadata",
            "--locked",
            "--features",
            "embed-ui,s3",
            "--format-version",
            "1",
            cwd=root,
        )
    )
    packages = {item["id"]: item for item in metadata["packages"]}
    root_id = next(
        item
        for item in metadata["workspace_members"]
        if packages[item]["name"] == "eventglass"
    )
    nodes = {item["id"]: item for item in metadata["resolve"]["nodes"]}
    pending = [root_id]
    seen = {root_id}
    while pending:
        node = nodes[pending.pop()]
        for dependency in node["deps"]:
            kinds = dependency.get("dep_kinds") or [{"kind": None}]
            if not any(kind.get("kind") in (None, "build") for kind in kinds):
                continue
            package_id = dependency["pkg"]
            if package_id not in seen:
                seen.add(package_id)
                pending.append(package_id)
    return [
        {
            "ecosystem": "cargo",
            "name": packages[item]["name"],
            "version": packages[item]["version"],
            "license": packages[item]["license"],
            "source": packages[item].get("repository")
            or packages[item].get("homepage")
            or packages[item].get("source")
            or "NOASSERTION",
        }
        for item in sorted(seen - {root_id})
    ]


def npm_packages(root: Path) -> list[dict[str, str]]:
    lock = json.loads((root / "web/package-lock.json").read_text())
    packages = []
    for location, item in lock["packages"].items():
        if not location or item.get("dev") is True or "name" not in item:
            continue
        packages.append(
            {
                "ecosystem": "npm",
                "name": item["name"],
                "version": item["version"],
                "license": item.get("license") or "NOASSERTION",
                "source": item.get("resolved") or "NOASSERTION",
            }
        )
    return packages


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: release-metadata.py OUTPUT_DIRECTORY")
    root = Path(__file__).resolve().parent.parent
    output = Path(sys.argv[1]).resolve()
    revision = run("git", "rev-parse", "HEAD", cwd=root)
    committed = run("git", "show", "-s", "--format=%cI", revision, cwd=root)
    created = (
        datetime.fromisoformat(committed)
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    binaries = [output / "eventglass-linux-amd64", output / "eventglass-linux-arm64"]
    missing = [str(path) for path in binaries if not path.is_file()]
    if missing:
        raise SystemExit(f"missing release binaries: {', '.join(missing)}")

    dependencies = sorted(
        rust_packages(root) + npm_packages(root),
        key=lambda item: (item["ecosystem"], item["name"], item["version"]),
    )
    root_package = {
        "SPDXID": "SPDXRef-Package-eventglass",
        "name": "eventglass",
        "versionInfo": revision,
        "downloadLocation": "NOASSERTION",
        "filesAnalyzed": False,
        "licenseConcluded": "NOASSERTION",
        "licenseDeclared": "NOASSERTION",
        "copyrightText": "NOASSERTION",
    }
    packages = [root_package]
    relationships = [
        {
            "spdxElementId": "SPDXRef-DOCUMENT",
            "relationshipType": "DESCRIBES",
            "relatedSpdxElement": root_package["SPDXID"],
        }
    ]
    for item in dependencies:
        identifier = spdx_id(item["ecosystem"], item["name"], item["version"])
        packages.append(
            {
                "SPDXID": identifier,
                "name": item["name"],
                "versionInfo": item["version"],
                "downloadLocation": item["source"],
                "filesAnalyzed": False,
                "licenseConcluded": item["license"],
                "licenseDeclared": item["license"],
                "copyrightText": "NOASSERTION",
                "externalRefs": [
                    {
                        "referenceCategory": "PACKAGE-MANAGER",
                        "referenceType": "purl",
                        "referenceLocator": (
                            f"pkg:{item['ecosystem']}/{item['name']}@{item['version']}"
                        ),
                    }
                ],
            }
        )
        relationships.append(
            {
                "spdxElementId": root_package["SPDXID"],
                "relationshipType": "DEPENDS_ON",
                "relatedSpdxElement": identifier,
            }
        )
    files = []
    for path in binaries:
        identifier = f"SPDXRef-File-{path.name}"
        files.append(
            {
                "SPDXID": identifier,
                "fileName": f"./{path.name}",
                "checksums": [{"algorithm": "SHA256", "checksumValue": digest(path)}],
                "licenseConcluded": "NOASSERTION",
                "copyrightText": "NOASSERTION",
            }
        )
        relationships.append(
            {
                "spdxElementId": root_package["SPDXID"],
                "relationshipType": "CONTAINS",
                "relatedSpdxElement": identifier,
            }
        )
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, f"eventglass:{revision}")
    document = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": f"eventglass-{revision}",
        "documentNamespace": f"urn:uuid:{namespace}",
        "creationInfo": {
            "created": created,
            "creators": ["Tool: eventglass-release-metadata-1"],
        },
        "packages": packages,
        "files": files,
        "relationships": relationships,
    }
    (output / "eventglass.spdx.json").write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n"
    )
    notices = [
        "Eventglass third-party dependency notices",
        f"Revision: {revision}",
        "",
        "This inventory records declared dependency licenses and source locations.",
        "The Eventglass project license is intentionally not declared by this file.",
        "",
    ]
    notices.extend(
        f"{item['ecosystem']} {item['name']} {item['version']} | "
        f"{item['license']} | {item['source']}"
        for item in dependencies
    )
    (output / "THIRD_PARTY_NOTICES.txt").write_text("\n".join(notices) + "\n")
    checksum_paths = binaries + [
        output / "eventglass.spdx.json",
        output / "THIRD_PARTY_NOTICES.txt",
    ]
    (output / "SHA256SUMS").write_text(
        "".join(f"{digest(path)}  {path.name}\n" for path in checksum_paths)
    )
    print(
        f"release metadata: {len(dependencies)} dependencies, "
        f"{len(checksum_paths)} checksums, revision {revision}"
    )


if __name__ == "__main__":
    main()
