#!/usr/bin/env python3

import sys
import zipfile
from pathlib import Path


def main() -> int:
    version = Path("version.txt").read_text().strip()
    archive = Path(f"dist/ulak-{version}.zip")
    prefix = f"ulak-{version}/"
    required = [
        "META.json",
        "Makefile",
        "README.md",
        "CHANGELOG.md",
        "LICENSE.md",
        "ulak.control",
        f"sql/ulak--{version}.sql",
    ]
    forbidden = [
        ".git/",
        ".github/",
        ".claude/",
        ".DS_Store",
        "BLOG_TR.md",
        "Dockerfile",
        "Dockerfile.publish",
        "Doxyfile",
        "compile_commands.json",
        "lefthook.yml",
        "ulak.so",
        "regression.out",
        "regression.diffs",
        "results/",
        "tmp_check/",
        "tmp_check_iso/",
        "docs/doxygen/",
        "roadmap/",
    ]

    if not archive.exists():
        print(f"Archive not found: {archive}", file=sys.stderr)
        return 1

    with zipfile.ZipFile(archive) as zf:
        names = zf.namelist()

    if not names:
        print("Archive is empty", file=sys.stderr)
        return 1

    bad_prefix = [name for name in names if not name.startswith(prefix)]
    if bad_prefix:
        print("Archive contains entries outside the expected top-level directory:", file=sys.stderr)
        for name in bad_prefix[:20]:
            print(f" - {name}", file=sys.stderr)
        return 1

    missing = [item for item in required if prefix + item not in names]
    if missing:
        print("Archive is missing required files:", file=sys.stderr)
        for item in missing:
            print(f" - {item}", file=sys.stderr)
        return 1

    present_forbidden: list[str] = []
    for item in forbidden:
        needle = prefix + item
        if item.endswith("/"):
            if any(name.startswith(needle) for name in names):
                present_forbidden.append(item)
        elif needle in names:
            present_forbidden.append(item)

    if present_forbidden:
        print("Archive contains excluded files or directories:", file=sys.stderr)
        for item in present_forbidden:
            print(f" - {item}", file=sys.stderr)
        return 1

    print(f"Archive layout OK: {archive}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
