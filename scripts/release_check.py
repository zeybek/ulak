#!/usr/bin/env python3

import json
import sys
from pathlib import Path


def extract_control_value(text: str, key: str) -> str | None:
    prefix = f"{key} = '"
    for line in text.splitlines():
        stripped = line.split("#")[0].strip()
        if stripped.startswith(prefix) and stripped.endswith("'"):
            return stripped[len(prefix) : -1]
    return None


def main() -> int:
    version = Path("version.txt").read_text().strip()
    control = Path("ulak.control").read_text()
    meta = json.loads(Path("META.json").read_text())
    expected_sql = Path(f"sql/ulak--{version}.sql")

    errors: list[str] = []

    control_version = extract_control_value(control, "default_version")
    if control_version != version:
        errors.append(
            f"ulak.control default_version={control_version!r} does not match version.txt={version!r}"
        )

    if meta.get("version") != version:
        errors.append(f"META.json version={meta.get('version')!r} does not match version.txt={version!r}")

    provides = meta.get("provides", {}).get("ulak", {})
    if provides.get("version") != version:
        errors.append(
            "META.json provides.ulak.version="
            f"{provides.get('version')!r} does not match version.txt={version!r}"
        )

    expected_sql_file = f"sql/ulak--{version}.sql"
    if provides.get("file") != expected_sql_file:
        provides["file"] = expected_sql_file
        Path("META.json").write_text(json.dumps(meta, indent=2) + "\n")
        print(f"Auto-fixed META.json provides.ulak.file to {expected_sql_file}")

    if not expected_sql.exists():
        errors.append(f"Expected release SQL file missing: {expected_sql}")

    if errors:
        print("Release metadata validation failed:", file=sys.stderr)
        for err in errors:
            print(f" - {err}", file=sys.stderr)
        return 1

    print(f"Release metadata OK for version {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
