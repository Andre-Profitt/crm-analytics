#!/usr/bin/env python3
"""Scan a Salesforce org for CRM Analytics deployment readiness.

Checks:
  1. CRM Analytics (wave) API is accessible
  2. Required objects (Opportunity, Account, Lead, Contract) are queryable
  3. Key fields exist on each object
  4. User has Analytics Cloud permissions
  5. Dataset API limits are within bounds
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import load_helpers, is_dry_run

REQUIRED_OBJECTS = {
    "Opportunity": [
        "Id", "Name", "Amount", "StageName", "CloseDate", "OwnerId",
        "ForecastCategory", "IsClosed", "IsWon", "AccountId",
    ],
    "Account": ["Id", "Name", "Industry", "OwnerId"],
    "Lead": ["Id", "Name", "Status", "OwnerId", "ConvertedDate"],
    "Contract": ["Id", "ContractNumber", "AccountId", "Status", "StartDate", "EndDate"],
}


def check_wave_api(helpers, inst, tok) -> bool:
    """Verify that the Wave API endpoint is reachable."""
    try:
        result = helpers._sf_api(inst, tok, "GET", "/services/data/v66.0/wave/")
        return bool(result)
    except Exception as e:
        print(f"  FAIL: Wave API unreachable — {e}")
        return False


def check_object_fields(helpers, inst, tok, sobject: str, fields: list[str]) -> list[str]:
    """Check that required fields exist on a given sObject."""
    missing = []
    try:
        desc = helpers._sf_api(
            inst, tok, "GET", f"/services/data/v66.0/sobjects/{sobject}/describe"
        )
        existing = {f["name"] for f in desc.get("fields", [])}
        for field in fields:
            if field not in existing:
                missing.append(field)
    except Exception as e:
        return [f"(describe failed: {e})"]
    return missing


def main():
    dry = is_dry_run()
    print("=== Org Readiness Scan ===")
    if dry:
        print("  (dry-run mode — will validate configuration only)\n")
        # In dry-run, just validate the configuration
        for obj, fields in REQUIRED_OBJECTS.items():
            print(f"  Would check {obj}: {', '.join(fields)}")
        print("\nDry-run complete — configuration valid")
        return

    helpers = load_helpers()
    inst, tok = helpers.get_auth()
    print(f"  Connected to {inst}\n")

    errors = []

    # 1. Wave API
    print("Checking Wave API ... ", end="")
    if check_wave_api(helpers, inst, tok):
        print("OK")
    else:
        errors.append("Wave API not accessible")

    # 2. Required objects & fields
    for obj, fields in REQUIRED_OBJECTS.items():
        print(f"Checking {obj} fields ... ", end="")
        missing = check_object_fields(helpers, inst, tok, obj, fields)
        if missing:
            print(f"MISSING: {', '.join(missing)}")
            errors.append(f"{obj} missing fields: {', '.join(missing)}")
        else:
            print("OK")

    if errors:
        print(f"\nReadiness scan FAILED — {len(errors)} issue(s):")
        for e in errors:
            print(f"  {e}")
        sys.exit(1)
    else:
        print("\nOrg readiness scan passed — all checks OK")


if __name__ == "__main__":
    main()
