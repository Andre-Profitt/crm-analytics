#!/usr/bin/env python3
"""Add grouped bullet charts to KPI summary pages across Gen 2 dashboards.

For each target dashboard, inserts a bullet chart showing key KPIs as
% of target, positioned between the KPI number strip and the main
visualizations. Shifts existing content down to make room.

Data source: KPI_Scorecard dataset (PctOfTarget as actual, 100 as target)
Custom SAQL for dashboards where metrics aren't in KPI_Scorecard.
"""

import json
from pathlib import Path
import subprocess
import sys
import urllib.request


# ── Shared utilities (same pattern as other Tier 0 scripts) ──────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def get_token():
    r = subprocess.run(
        ["sf", "org", "display", "--json"], capture_output=True, text=True
    )
    d = json.loads(r.stdout)
    return d["result"]["accessToken"], d["result"]["instanceUrl"]


def _normalize_state(state):
    from crm_analytics_helpers import normalize_dashboard_state_for_patch

    return normalize_dashboard_state_for_patch(state)


def get_dashboard(instance, token, dashboard_id):
    url = f"{instance}/services/data/v66.0/wave/dashboards/{dashboard_id}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())


def patch_dashboard(instance, token, dashboard_id, state):
    url = f"{instance}/services/data/v66.0/wave/dashboards/{dashboard_id}"
    payload = json.dumps({"state": state}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    resp = urllib.request.urlopen(req)
    return resp.status


# ── Bullet chart SAQL builders ──────────────────────────────────────


def kpi_scorecard_saql(kpi_ids):
    """Build SAQL that loads KPIs from KPI_Scorecard as actual/target pairs."""
    id_filter = ", ".join(f'"{kid}"' for kid in kpi_ids)
    return (
        f'q = load "KPI_Scorecard";\n'
        f"q = filter q by KPI_ID in [{id_filter}];\n"
        f"q = filter q by PctOfTarget > 0 && PctOfTarget <= 250;\n"
        f"q = group q by (KPI_Name, SortOrder);\n"
        f"q = foreach q generate KPI_Name, SortOrder,\n"
        f"    avg(PctOfTarget) as actual, 100 as target;\n"
        f"q = order q by SortOrder asc;"
    )


def retention_bullet_saql():
    """Custom SAQL for Revenue Retention metrics (not in KPI_Scorecard).

    Normalizes to PctOfTarget format (actual vs 100 target):
      NRR: actual = NRR (already %, target 100%)
      GRR: actual = (GRR / 90) * 100 (target 90% → normalized to 100)
      Non-Churn: actual = ((100 - ChurnRate) / 95) * 100 (target 95% → normalized to 100)
    """
    return (
        'q_nrr = load "Revenue_Retention_Health";\n'
        'q_nrr = filter q_nrr by RecordType == "yearly_metric";\n'
        'q_nrr = foreach q_nrr generate "Net Revenue Retention" as KPI,\n'
        "    NRR as actual, 100 as target, 1 as SortOrder, Year;\n"
        "q_nrr = order q_nrr by Year desc;\n"
        "q_nrr = limit q_nrr 1;\n"
        "\n"
        'q_grr = load "Revenue_Retention_Health";\n'
        'q_grr = filter q_grr by RecordType == "yearly_metric";\n'
        'q_grr = foreach q_grr generate "Gross Revenue Retention" as KPI,\n'
        "    (GRR / 90) * 100 as actual, 100 as target, 2 as SortOrder, Year;\n"
        "q_grr = order q_grr by Year desc;\n"
        "q_grr = limit q_grr 1;\n"
        "\n"
        'q_churn = load "Revenue_Retention_Health";\n'
        'q_churn = filter q_churn by RecordType == "yearly_metric";\n'
        'q_churn = foreach q_churn generate "Non-Churn Rate" as KPI,\n'
        "    ((100 - ChurnRate) / 95) * 100 as actual, 100 as target, 3 as SortOrder, Year;\n"
        "q_churn = order q_churn by Year desc;\n"
        "q_churn = limit q_churn 1;\n"
        "\n"
        "q = union q_nrr, q_grr, q_churn;\n"
        "q = order q by SortOrder asc;"
    )


# ── Bullet chart widget builder ─────────────────────────────────────


def make_bullet_widget(
    step_name, title, subtitle, axis_title="% of Target", number_format="0.0%"
):
    """Build a bullet chart widget params dict."""
    return {
        "type": "chart",
        "parameters": {
            "visualizationType": "bullet",
            "step": step_name,
            "theme": "wave",
            "axisMode": "sync",
            "measureAxis1": {
                "numberFormat": number_format,
                "showTitle": True,
                "showAxis": True,
                "title": axis_title,
                "customDomain": {
                    "showMin": False,
                    "showMax": False,
                },
            },
            "legend": {
                "showHeader": True,
                "show": True,
                "customSize": "auto",
                "position": "right-top",
                "inside": False,
            },
            "title": {
                "fontSize": 14,
                "subtitleFontSize": 11,
                "label": title,
                "align": "center",
                "subtitleLabel": subtitle,
            },
        },
    }


def make_saql_step(saql_query):
    """Build a SAQL step dict.

    Note: SAQL steps must NOT include 'datasets' — that field is only
    valid for aggregateflex steps. Including it causes HTTP 400.
    """
    return {
        "type": "saql",
        "query": saql_query,
        "selectMode": "single",
    }


# ── Dashboard bullet chart configurations ────────────────────────────

# Each entry: (dashboard_label, dashboard_id, page_index, page_name,
#              insert_row, bullet_rowspan, step_name, widget_name,
#              title, subtitle, saql_builder_args)

BULLET_CONFIGS = [
    {
        "label": "Executive Summary",
        "did": "0FKTb0000000Io9OAE",
        "page_index": 0,
        "page_name": "summary",
        "insert_row": 7,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_kpi_summary",
        "widget_name": "w_bullet_kpis",
        "title": "Key Performance Indicators \u2014 % of Target",
        "subtitle": "Actual vs target \u2014 green = on track, red = needs attention",
        "kpi_ids": ["1.4.1", "1.4.2", "1.5.1", "1.4.3", "1.7.1", "2.5.3"],
        "custom_saql": None,
    },
    {
        "label": "Analytics Command Center",
        "did": "0FKTb0000000IEfOAM",
        "page_index": 0,
        "page_name": "pulse",
        "insert_row": 6,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_kpi_pulse",
        "widget_name": "w_bullet_kpis",
        "title": "KPI Attainment \u2014 % of Target",
        "subtitle": "Revenue, pipeline, win rate, and forecast confidence vs targets",
        "kpi_ids": ["1.4.1", "1.4.2", "1.5.1", "1.4.3"],
        "custom_saql": None,
    },
    {
        "label": "Executive Revenue & Forecast",
        "did": "0FKTb0000000HqTOAU",
        "page_index": 0,
        "page_name": "revenue",
        "insert_row": 13,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_rev_kpis",
        "widget_name": "w_bullet_rev_kpis",
        "title": "Revenue KPI Attainment \u2014 % of Target",
        "subtitle": "Key revenue metrics vs plan targets",
        "kpi_ids": ["1.4.1", "1.4.2", "1.4.3", "2.5.1"],
        "custom_saql": None,
    },
    {
        "label": "Customer & Account Health",
        "did": "0FKTb0000000HvJOAU",
        "page_index": 0,
        "page_name": "summary",
        "insert_row": 13,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_cust_kpis",
        "widget_name": "w_bullet_cust_kpis",
        "title": "Customer KPI Attainment \u2014 % of Target",
        "subtitle": "Health, data quality, and coverage vs targets",
        "kpi_ids": ["1.7.1", "2.3.4", "2.3.3", "2.5.3"],
        "custom_saql": None,
    },
    {
        "label": "Pipeline & Opportunity Operations",
        "did": "0FKTb0000000Hs5OAE",
        "page_index": 0,
        "page_name": "summary",
        "insert_row": 13,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_pipe_kpis",
        "widget_name": "w_bullet_pipe_kpis",
        "title": "Pipeline KPI Attainment \u2014 % of Target",
        "subtitle": "Win rate, coverage, and cycle time vs targets",
        "kpi_ids": ["1.5.1", "1.4.1", "1.4.2", "1.5.3"],
        "custom_saql": None,
    },
    {
        "label": "Revenue Retention & Health",
        "did": "0FKTb0000000ITBOA2",
        "page_index": 0,
        "page_name": "summary",
        "insert_row": 13,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_retention",
        "widget_name": "w_bullet_retention",
        "title": "Retention KPI Attainment \u2014 % of Target",
        "subtitle": "NRR, GRR, and churn rate vs benchmarks",
        "kpi_ids": None,
        "custom_saql": retention_bullet_saql(),
    },
    {
        "label": "Sales Operations Command Center",
        "did": "0FKTb0000000IHtOAM",
        "page_index": 0,
        "page_name": "Forecast & Pipeline",
        "insert_row": 8,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_ops_kpis",
        "widget_name": "w_bullet_ops_kpis",
        "title": "Operational KPI Attainment \u2014 % of Target",
        "subtitle": "Revenue, pipeline, win rate, and forecast vs targets",
        "kpi_ids": ["1.4.1", "1.4.2", "1.5.1", "1.4.3"],
        "custom_saql": None,
    },
    {
        "label": "Finance Revenue Operations",
        "did": "0FKTb0000000IOLOA2",
        "page_index": 0,
        "page_name": "revenue",
        "insert_row": 10,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_fin_kpis",
        "widget_name": "w_bullet_fin_kpis",
        "title": "Finance KPI Attainment \u2014 % of Target",
        "subtitle": "Revenue attainment, coverage, and business mix vs targets",
        "kpi_ids": ["1.4.1", "1.4.2", "2.5.1", "2.5.2"],
        "custom_saql": None,
    },
    # Tier 2: Manager/Operational dashboards
    {
        "label": "Forecast & Revenue Motions",
        "did": "0FKTb0000000HthOAE",
        "page_index": 0,
        "page_name": "summary",
        "insert_row": 13,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_forecast_kpis",
        "widget_name": "w_bullet_forecast_kpis",
        "title": "Forecast KPI Attainment \u2014 % of Target",
        "subtitle": "Revenue, coverage, and confidence vs targets",
        "kpi_ids": ["1.4.1", "1.4.2", "1.4.3", "1.5.1"],
        "custom_saql": None,
    },
    {
        "label": "Lead Funnel",
        "did": "0FKTb0000000HwvOAE",
        "page_index": 0,
        "page_name": "summary",
        "insert_row": 13,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_lead_kpis",
        "widget_name": "w_bullet_lead_kpis",
        "title": "Lead KPI Attainment \u2014 % of Target",
        "subtitle": "Conversion, response time, and follow-up vs targets",
        "kpi_ids": ["1.1.4", "2.8.1", "2.8.2", "2.8.4"],
        "custom_saql": None,
    },
    {
        "label": "Contract Operations & Renewals",
        "did": "0FKTb0000000HyXOAU",
        "page_index": 0,
        "page_name": "summary",
        "insert_row": 13,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_contract_kpis",
        "widget_name": "w_bullet_contract_kpis",
        "title": "Contract KPI Attainment \u2014 % of Target",
        "subtitle": "Renewal rate, coverage, and data quality vs targets",
        "kpi_ids": ["2.5.3", "1.8.4", "2.3.4", "1.7.1"],
        "custom_saql": None,
    },
    {
        "label": "BDR Manager",
        "did": "0FKTb0000000I8DOAU",
        "page_index": 0,
        "page_name": "pg_pipeline",
        "insert_row": 13,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_bdr_kpis",
        "widget_name": "w_bullet_bdr_kpis",
        "title": "BDR KPI Attainment \u2014 % of Target",
        "subtitle": "Conversion, response time, and meeting scheduling vs targets",
        "kpi_ids": ["1.1.4", "2.8.1", "2.8.2", "2.8.3"],
        "custom_saql": None,
    },
    {
        "label": "AE Performance Dashboard",
        "did": "0FKTb0000000IGHOA2",
        "page_index": 0,
        "page_name": "Leaderboard",
        "insert_row": 13,
        "bullet_rowspan": 5,
        "step_name": "s_bullet_ae_kpis",
        "widget_name": "w_bullet_ae_kpis",
        "title": "AE KPI Attainment \u2014 % of Target",
        "subtitle": "Win rate, pipeline coverage, and cycle time vs targets",
        "kpi_ids": ["1.5.1", "1.4.2", "1.5.3", "1.4.1"],
        "custom_saql": None,
    },
]


# ── Main processing ─────────────────────────────────────────────────


def insert_bullet_at_row(page_widgets, insert_row, bullet_rowspan):
    """Shift all widgets at insert_row or below down by bullet_rowspan,
    then return the insert_row for the new bullet chart.

    This makes room for the bullet by pushing existing content down."""
    shifted = 0
    for w in page_widgets:
        r = w.get("row", 0)
        if r >= insert_row:
            w["row"] = r + bullet_rowspan
            shifted += 1
    return shifted


def process_dashboard(instance, token, config, dry_run=False):
    """Add a bullet chart to one dashboard."""
    label = config["label"]
    did = config["did"]
    page_idx = config["page_index"]

    data = get_dashboard(instance, token, did)
    state = _normalize_state(data.get("state", {}))

    widgets = state.get("widgets", {})
    steps = state.get("steps", {})
    grid = state.get("gridLayouts", [{}])[0] if state.get("gridLayouts") else {}
    pages = grid.get("pages", [])

    if page_idx >= len(pages):
        print(f"  {label}: page index {page_idx} out of range ({len(pages)} pages)")
        return 0

    page = pages[page_idx]
    page_widgets = page.get("widgets", [])
    page_name = page.get("name", f"page_{page_idx}")

    # Check if bullet already exists
    step_name = config["step_name"]
    widget_name = config["widget_name"]

    if step_name in steps:
        print(f"  {label}: step '{step_name}' already exists — skipping")
        return 0
    if widget_name in widgets:
        print(f"  {label}: widget '{widget_name}' already exists — skipping")
        return 0

    # Insert at configured row — shift everything below down
    insert_row = config["insert_row"]
    bullet_rowspan = config["bullet_rowspan"]
    shifted = insert_bullet_at_row(page_widgets, insert_row, bullet_rowspan)

    # Build SAQL
    if config["custom_saql"]:
        saql = config["custom_saql"]
    else:
        saql = kpi_scorecard_saql(config["kpi_ids"])

    # Create step
    steps[step_name] = make_saql_step(saql)

    # Create widget
    bullet_widget = make_bullet_widget(
        step_name=step_name,
        title=config["title"],
        subtitle=config["subtitle"],
    )
    widgets[widget_name] = bullet_widget

    # Add widget to page layout
    page_widgets.append(
        {
            "name": widget_name,
            "row": insert_row,
            "column": 0,
            "rowspan": bullet_rowspan,
            "colspan": 12,
        }
    )

    if dry_run:
        print(
            f"  {label} [{page_name}]: would insert bullet at row {insert_row} "
            f"(shifted {shifted} widgets down by {bullet_rowspan})"
        )
        print(f"    Step: {step_name}")
        print(f"    KPIs: {config.get('kpi_ids', 'custom')}")
        print(f"    SAQL preview: {saql[:120]}...")
        return 1

    # Deploy
    try:
        status = patch_dashboard(instance, token, did, state)
        print(
            f"\u2713 {label} [{page_name}]: {status} \u2014 bullet at row {insert_row}, "
            f"{shifted} widgets shifted"
        )
        return 1
    except Exception as e:
        print(f"\u2717 {label}: FAILED \u2014 {e}")
        if hasattr(e, "read"):
            err = e.read().decode("utf-8", errors="replace")[:500]
            print(f"  {err}")
        return 0


def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN MODE ===\n")

    token, instance = get_token()
    print(f"Auth OK: {instance}")
    print(f"Processing {len(BULLET_CONFIGS)} dashboards...\n")

    total = 0
    for config in BULLET_CONFIGS:
        try:
            count = process_dashboard(instance, token, config, dry_run=dry_run)
            total += count
        except Exception as e:
            print(f"\u2717 {config['label']}: ERROR \u2014 {e}")

    print(f"\n{'=' * 60}")
    mode = "Would add" if dry_run else "Added"
    print(f"{mode} {total} bullet charts across {len(BULLET_CONFIGS)} dashboards")


if __name__ == "__main__":
    main()
