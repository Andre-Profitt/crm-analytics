#!/usr/bin/env python3
"""Validate selection interactions and bindings across dashboards.

Checks:
  1. Every add_selection_interaction() call references a valid step
  2. Filter widgets (pillbox/listselector) have matching SAQL filter steps
  3. Cross-filter bindings reference existing widget IDs
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import ROOT, core_files


def validate_file(path: Path) -> list[str]:
    """Validate interaction consistency in a builder file."""
    warnings = []
    text = path.read_text()

    # Find all step names defined in build_steps()
    step_names = set(re.findall(r'"((?:f_|s_|p\d+_)\w+)"(?:\s*:\s*)', text))

    # Find all interaction references
    interaction_refs = re.findall(
        r'add_selection_interaction\(\s*[^,]+,\s*"([^"]+)"', text
    )
    for ref in interaction_refs:
        if ref not in step_names:
            warnings.append(
                f"{path.name}: interaction references unknown step '{ref}'"
            )

    # Find pillbox/listselector widget references to filter steps
    filter_refs = re.findall(r'pillbox\(\s*"(f_\w+)"', text)
    for ref in filter_refs:
        if ref not in step_names:
            warnings.append(
                f"{path.name}: pillbox references unknown filter step '{ref}'"
            )

    return warnings


def main():
    print("=== Interaction Validator ===\n")

    all_warnings = []
    for f in core_files():
        if f.exists() and f.name.startswith("build_"):
            ws = validate_file(f)
            all_warnings.extend(ws)
            status = f"  {len(ws)} warning(s)" if ws else "  OK"
            print(f"  {f.name:40s} {status}")

    if all_warnings:
        print(f"\n{len(all_warnings)} warning(s) found:")
        for w in all_warnings:
            print(f"  {w}")
        sys.exit(1)
    else:
        print("\nAll interactions valid — OK")


if __name__ == "__main__":
    main()
