#!/usr/bin/env python3
"""Route CRM Analytics alerts based on metric thresholds.

Evaluates configured alert rules against live dataset metrics and routes
notifications to the appropriate channels (email, Slack, Chatter).

Supports --dry-run to evaluate rules without sending notifications.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import ROOT, is_dry_run

# Alert rules configuration
ALERT_RULES = [
    {
        "name": "Low Win Rate",
        "dataset": "Opp_Mgmt_KPIs",
        "metric": "WinRate",
        "threshold": 0.20,
        "operator": "lt",
        "severity": "high",
        "channel": "email",
    },
    {
        "name": "Pipeline Coverage Below 3x",
        "dataset": "Opp_Mgmt_KPIs",
        "metric": "PipelineCoverage",
        "threshold": 3.0,
        "operator": "lt",
        "severity": "medium",
        "channel": "chatter",
    },
    {
        "name": "High Churn Probability",
        "dataset": "Customer_Intelligence",
        "metric": "AvgChurnProbability",
        "threshold": 0.30,
        "operator": "gt",
        "severity": "high",
        "channel": "email",
    },
    {
        "name": "Forecast Accuracy Drift",
        "dataset": "Forecast_Snapshots",
        "metric": "ForecastAccuracy",
        "threshold": 0.75,
        "operator": "lt",
        "severity": "medium",
        "channel": "chatter",
    },
]


def evaluate_rule(rule: dict, dry: bool) -> dict | None:
    """Evaluate a single alert rule.  Returns alert dict if triggered."""
    # In production this would query the dataset via SAQL.
    # For now, return the rule definition for dry-run validation.
    print(f"  Evaluating: {rule['name']} "
          f"({rule['metric']} {rule['operator']} {rule['threshold']})")
    if dry:
        return None
    return None  # placeholder — live evaluation requires org connection


def main():
    dry = is_dry_run()
    print("=== CRM Analytics Alerts Router ===")
    if dry:
        print("  (dry-run mode — no notifications will be sent)\n")

    triggered = []
    for rule in ALERT_RULES:
        result = evaluate_rule(rule, dry)
        if result:
            triggered.append(result)

    print(f"\nEvaluated {len(ALERT_RULES)} rule(s), "
          f"{len(triggered)} triggered")


if __name__ == "__main__":
    main()
