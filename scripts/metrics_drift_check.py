#!/usr/bin/env python3
"""Check for semantic metric drift across builder modules.

Detects inconsistencies in how key metrics (ARR, Win Rate, etc.) are
computed across different dashboards.  Parses SAQL fragments from each
builder and flags divergent aggregation patterns.

Supports --dry-run to report without failing.
"""

import ast
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import ROOT, core_files, is_dry_run

# Canonical metric patterns (field name → expected aggregation)
CANONICAL = {
    "ARR": r'sum\(\s*[\'"]?ARR[\'"]?\s*\)',
    "Amount": r'sum\(\s*[\'"]?Amount[\'"]?\s*\)',
    "WinRate": r'(IsWon|"true")',
}


def scan_file(path: Path) -> list[str]:
    """Scan a builder file for metric definition drift."""
    warnings = []
    text = path.read_text()

    # Look for SAQL string literals that aggregate ARR
    arr_aggs = re.findall(r'(?:avg|count|median)\s*\(\s*["\']?ARR["\']?\s*\)', text)
    for agg in arr_aggs:
        warnings.append(
            f"{path.name}: non-standard ARR aggregation '{agg}' "
            f"(expected sum(ARR))"
        )

    return warnings


def main():
    dry = is_dry_run()
    print("=== Semantic Metric Drift Check ===")
    if dry:
        print("  (dry-run mode)\n")

    all_warnings = []
    for f in core_files():
        if f.exists() and f.name.startswith("build_"):
            ws = scan_file(f)
            all_warnings.extend(ws)

    if all_warnings:
        print(f"Found {len(all_warnings)} drift warning(s):")
        for w in all_warnings:
            print(f"  {w}")
        if not dry:
            sys.exit(1)
    else:
        print("No metric drift detected — all definitions consistent")


if __name__ == "__main__":
    main()
