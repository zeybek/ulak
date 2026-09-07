#!/usr/bin/env python3

import json
import re
import sys
from pathlib import Path

UPGRADE_SCRIPT_RE = re.compile(r"^ulak--(\d+\.\d+\.\d+)--(\d+\.\d+\.\d+)\.sql$")


def extract_control_value(text: str, key: str) -> str | None:
    prefix = f"{key} = '"
    for line in text.splitlines():
        stripped = line.split("#")[0].strip()
        if stripped.startswith(prefix) and stripped.endswith("'"):
            return stripped[len(prefix) : -1]
    return None


def parse_version(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def check_upgrade_chain(version: str, errors: list[str]) -> None:
    """Require that ALTER EXTENSION ulak UPDATE can reach `version`.

    Upgrade scripts (sql/ulak--A--B.sql) are hand-written and name the next
    version before release-please decides it. If the guessed target and the
    released version disagree (e.g. a script to 0.0.4 but a breaking change
    bumps to 0.1.0), nothing else notices and users of the previous release
    are left without an upgrade path.
    """
    steps: dict[str, list[str]] = {}
    for path in sorted(Path("sql").glob("ulak--*--*.sql")):
        match = UPGRADE_SCRIPT_RE.match(path.name)
        if not match:
            errors.append(f"{path}: upgrade script name is not ulak--<from>--<to>.sql")
            continue
        src, dst = match.groups()
        if parse_version(src) >= parse_version(dst):
            errors.append(f"{path}: source version must be lower than target version")
            continue
        steps.setdefault(src, []).append(dst)

    if not steps:
        return  # first release: nothing to upgrade from

    # Every released version must be able to reach the current one, so walk
    # from the first release (release-please's initial-version), not merely
    # from the lowest script we happen to have.
    start = min(steps, key=parse_version)
    config_path = Path("release-please-config.json")
    if config_path.exists():
        packages = json.loads(config_path.read_text()).get("packages", {})
        initial = packages.get(".", {}).get("initial-version")
        if initial and parse_version(initial) < parse_version(version):
            start = initial
    current = start
    seen = {current}
    while current != version:
        targets = steps.get(current)
        if not targets:
            break
        current = max(targets, key=parse_version)
        if current in seen:
            errors.append(f"upgrade scripts form a cycle at {current}")
            return
        seen.add(current)

    if current != version:
        errors.append(
            f"no upgrade path from {start} to version.txt={version}: "
            f"the chain stops at {current} (expected a sql/ulak--{current}--<next>.sql "
            f"step that eventually reaches {version})"
        )

    for src, targets in steps.items():
        for dst in targets:
            if parse_version(dst) > parse_version(version) and src != version:
                errors.append(
                    f"sql/ulak--{src}--{dst}.sql targets a version newer than "
                    f"version.txt={version} but does not start from {version}"
                )


def main() -> int:
    version = Path("version.txt").read_text().strip()
    control = Path("ulak.control").read_text()
    meta = json.loads(Path("META.json").read_text())

    errors: list[str] = []
    fixed: list[str] = []

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

    check_upgrade_chain(version, errors)

    expected_sql_file = f"sql/ulak--{version}.sql"
    if provides.get("file") != expected_sql_file:
        provides["file"] = expected_sql_file
        Path("META.json").write_text(json.dumps(meta, indent=2) + "\n")
        fixed.append(f"Auto-fixed META.json provides.ulak.file to {expected_sql_file}")

    for msg in fixed:
        print(msg)

    if errors:
        print("Release metadata validation failed:", file=sys.stderr)
        for err in errors:
            print(f" - {err}", file=sys.stderr)
        return 1

    print(f"Release metadata OK for version {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
