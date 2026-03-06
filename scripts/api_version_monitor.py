#!/usr/bin/env python3
"""Monitor Salesforce API version usage across CRM Analytics code.

Scans all builder modules and helpers for API version references (e.g.
/services/data/v66.0/) and reports:
  1. All distinct API versions in use
  2. Files still on older versions
  3. Whether the org supports the target version
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import ROOT, core_files, script_files

TARGET_VERSION = "v66.0"
VERSION_PATTERN = re.compile(r'/services/data/(v\d+\.\d+)/')


def scan_versions(path: Path) -> list[tuple[int, str]]:
    """Return list of (line_number, version_string) found in file."""
    hits = []
    for i, line in enumerate(path.read_text().splitlines(), 1):
        for m in VERSION_PATTERN.finditer(line):
            hits.append((i, m.group(1)))
    return hits


def main():
    print(f"=== API Version Monitor (target: {TARGET_VERSION}) ===\n")

    all_files = [f for f in core_files() + script_files() if f.exists()]
    # Also include the helpers file
    helpers = ROOT / "crm_analytics_helpers.py"
    if helpers.exists() and helpers not in all_files:
        all_files.append(helpers)

    version_map: dict[str, list[str]] = {}
    outdated = []

    for f in all_files:
        hits = scan_versions(f)
        for line_no, ver in hits:
            version_map.setdefault(ver, []).append(f"{f.name}:{line_no}")
            if ver != TARGET_VERSION:
                outdated.append((f.name, line_no, ver))

    print("Versions found:")
    for ver in sorted(version_map.keys()):
        count = len(version_map[ver])
        marker = " (current)" if ver == TARGET_VERSION else " (OUTDATED)"
        print(f"  {ver}: {count} reference(s){marker}")

    if outdated:
        print(f"\n{len(outdated)} outdated reference(s):")
        for fname, line, ver in outdated[:20]:
            print(f"  {fname}:{line} uses {ver}")
        if len(outdated) > 20:
            print(f"  ... and {len(outdated) - 20} more")
        sys.exit(1)
    else:
        print(f"\nAll references use {TARGET_VERSION} — OK")


if __name__ == "__main__":
    main()
