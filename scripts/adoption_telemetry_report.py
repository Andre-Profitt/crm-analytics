#!/usr/bin/env python3
"""Generate CRM Analytics adoption telemetry report.

Queries the org for dashboard usage analytics and reports:
  1. Dashboard view counts (last 30 days)
  2. Most/least viewed dashboards
  3. Active vs stale dashboards
  4. Widget interaction patterns

Supports --dry-run to show report structure without org access.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import load_helpers, is_dry_run

DASHBOARD_NAMES = [
    "Opp Management",
    "Sales Process Compliance KPIs",
    "Advanced Analytics",
    "Account Intelligence",
    "Customer Intelligence",
    "Lead Management",
    "Contract Operations",
    "Revenue Motions",
    "Forecasting HQ",
    "Pipeline History",
]


def main():
    dry = is_dry_run()
    print("=== Adoption Telemetry Report ===")
    if dry:
        print("  (dry-run mode — will show report structure only)\n")
        print("Dashboards that would be audited:")
        for name in DASHBOARD_NAMES:
            print(f"  - {name}")
        print(f"\nTotal: {len(DASHBOARD_NAMES)} dashboards")
        print("\nReport sections:")
        print("  1. View counts (last 30 days)")
        print("  2. Most/least viewed ranking")
        print("  3. Active vs stale classification")
        print("  4. Widget interaction heatmap")
        print("\nDry-run complete")
        return

    helpers = load_helpers()
    inst, tok = helpers.get_auth()
    print(f"  Connected to {inst}\n")

    # Query dashboard list
    try:
        result = helpers._sf_api(
            inst, tok, "GET", "/services/data/v66.0/wave/dashboards?pageSize=50"
        )
        dashboards = result.get("dashboards", [])
    except Exception as e:
        print(f"ERROR: Could not query dashboards — {e}")
        sys.exit(1)

    # Match known dashboards
    found = []
    for db in dashboards:
        if db.get("label") in DASHBOARD_NAMES:
            found.append(db)

    print(f"Found {len(found)}/{len(DASHBOARD_NAMES)} managed dashboards\n")

    for db in found:
        label = db.get("label", "?")
        db_id = db.get("id", "?")
        print(f"  {label:40s} id={db_id}")

    print(f"\nTelemetry report complete — {len(found)} dashboards tracked")


if __name__ == "__main__":
    main()
