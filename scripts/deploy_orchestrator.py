#!/usr/bin/env python3
"""Orchestrate full CRM Analytics deployment in dependency order.

Execution order:
  1. build_dashboard.py        (creates Opp_Mgmt_KPIs dataset)
  2. build_sales_compliance.py (reuses Opp_Mgmt_KPIs)
  3. build_advanced_analytics.py
  4. build_account_intelligence.py
  5. build_customer_intelligence.py
  6. build_lead_management.py
  7. build_contract_operations.py
  8. build_forecasting.py
  9. build_revenue_motions.py
  10. build_pipeline_history.py

Supports --dry-run to print the plan without executing.
"""

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import ROOT, is_dry_run

DEPLOY_ORDER = [
    "build_dashboard.py",
    "build_sales_compliance.py",
    "build_advanced_analytics.py",
    "build_account_intelligence.py",
    "build_customer_intelligence.py",
    "build_lead_management.py",
    "build_contract_operations.py",
    "build_forecasting.py",
    "build_revenue_motions.py",
    "build_pipeline_history.py",
]


def main():
    dry = is_dry_run()
    print("=== CRM Analytics Deploy Orchestrator ===")
    if dry:
        print("  (dry-run mode — no builders will be executed)\n")

    failed = []
    for i, name in enumerate(DEPLOY_ORDER, 1):
        script = ROOT / name
        if not script.exists():
            print(f"  [{i}/{len(DEPLOY_ORDER)}] SKIP {name} (not found)")
            continue

        print(f"  [{i}/{len(DEPLOY_ORDER)}] {'Would run' if dry else 'Running'} {name}")
        if dry:
            continue

        result = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(ROOT),
        )
        if result.returncode != 0:
            failed.append(name)
            print(f"    FAILED (exit {result.returncode})")
        else:
            print(f"    OK")

    if failed:
        print(f"\nDeployment completed with {len(failed)} failure(s): {', '.join(failed)}")
        sys.exit(1)
    else:
        print("\nDeployment completed successfully" + (" (dry-run)" if dry else ""))


if __name__ == "__main__":
    main()
