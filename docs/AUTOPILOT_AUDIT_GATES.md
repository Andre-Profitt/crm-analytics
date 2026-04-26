# Autopilot Audit Gates

These gates exist so the overnight build loop does not blindly redeploy dashboards and call them good.

## Core Gates

- `Metric spine first`
  - No dashboard should be treated as valid until the source-of-truth totals reconcile against the intended KPI logic.

- `Timeframe correctness`
  - Prior-year values may be used for targets or baselines, but they must not leak into visible current-year series unless that is explicitly the point of the widget.

- `Forecast-of-record consistency`
  - A dashboard cannot mix `Best Case` and `Pipeline` narratives without stating the difference.
  - KPI tiles, bridges, and region diagnostics should use the same forecast-of-record unless the visual is explicitly a scenario comparison.

- `Story completeness`
  - Executive pages should answer:
    - where are we now
    - what is the target
    - what confidence ladder exists
    - what must change to hit plan
    - where intervention is needed

- `Action layer presence`
  - Manager and queue surfaces must expose `RecommendedAction`, `NextStep`, and record links or drill paths.

- `Live render verification`
  - A deploy is not accepted just because the API returned `OK`.
  - The loop must export live JSON, capture browser artifacts, and keep a readable run log.

## Executive Revenue Profile

The first fully codified audit profile is:

- [audit_source_truth_executive_revenue.py](/Users/test/crm-analytics/scripts/audit_source_truth_executive_revenue.py)

It checks:

- line chart uses `Actual`, `Commit`, `BestCase`, `Pipeline`, `Target`
- no visible `FY2025` leakage in the current-year ladder
- forecast bridge closes against `Best Case`, not `Pipeline`
- regional confidence table includes `CoverageStatus`, `NeededFromPipelineARR`, and promotable low-confidence pipeline
- queue columns include `RecommendedAction`, `NextStep`, and `PriorityScore`
- KPI strip tells the full confidence story

## Queue Integration

Autopilot queue items can specify an `audit_script`.

Current queue:

- [dashboard_autopilot_queue.json](/Users/test/crm-analytics/config/dashboard_autopilot_queue.json)
- [widget_decision_profiles.json](/Users/test/crm-analytics/config/widget_decision_profiles.json)

Runner:

- [run_dashboard_autopilot.py](/Users/test/crm-analytics/scripts/run_dashboard_autopilot.py)
- [WIDGET_DECISION_LIBRARY.md](/Users/test/crm-analytics/docs/WIDGET_DECISION_LIBRARY.md)

The goal is to keep expanding audit profiles by domain so the overnight loop gets stricter over time instead of just faster.
