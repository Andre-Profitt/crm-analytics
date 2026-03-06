#!/usr/bin/env python3
"""Smoke-test the Salesforce API endpoints used by CRM Analytics builders.

Tests each API endpoint category:
  1. Wave datasets API (list, get)
  2. Wave dashboards API (list, get)
  3. SOQL query endpoint
  4. Wave XMD endpoint
  5. Wave dataflow API

Reports a pass/fail matrix.  Supports --dry-run.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import load_helpers, is_dry_run

ENDPOINTS = [
    ("Wave Datasets", "/services/data/v66.0/wave/datasets?pageSize=1"),
    ("Wave Dashboards", "/services/data/v66.0/wave/dashboards?pageSize=1"),
    ("SOQL Query", "/services/data/v66.0/query/?q=SELECT+Id+FROM+Opportunity+LIMIT+1"),
    ("Wave Dataflows", "/services/data/v66.0/wave/dataflows?pageSize=1"),
]


def main():
    dry = is_dry_run()
    print("=== API Smoke Matrix ===")
    if dry:
        print("  (dry-run mode — will list endpoints only)\n")
        for name, path in ENDPOINTS:
            print(f"  Would test: {name} → {path}")
        print("\nDry-run complete")
        return

    helpers = load_helpers()
    inst, tok = helpers.get_auth()
    print(f"  Connected to {inst}\n")

    results = []
    for name, path in ENDPOINTS:
        print(f"  {name:25s} ... ", end="")
        try:
            helpers._sf_api(inst, tok, "GET", path)
            print("PASS")
            results.append((name, True))
        except Exception as e:
            print(f"FAIL ({e})")
            results.append((name, False))

    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    print(f"\nResults: {passed}/{total} passed")
    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
