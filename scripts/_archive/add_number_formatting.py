#!/usr/bin/env python3
"""Add numberFormat to all number widgets and chart measure axes across all Gen 2 dashboards.

Heuristics:
  - Title/field contains %/Rate/Coverage/Confidence/Pacing/YoY/pct → "0.0%"
  - Title/field contains ARR/Revenue/Amount/Gap/Plan/Quota/$/Value/ACV/MRR → "$#,##0"
  - Title/field contains Days/Hours/Duration/Avg Time → "#,##0.0"
  - Title/field contains Count/Total/# or is a count metric → "#,##0"
  - Default: "#,##0"

Also adds compactForm where compact=True and currency numbers.
"""

import json
from pathlib import Path
import re
import subprocess
import sys
import urllib.request

# ── Active Gen 2 dashboard IDs ──────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DASHBOARDS = {
    "Executive Revenue & Forecast": "0FKTb0000000HqTOAU",
    "Executive Pipeline Risk & Process": "0FKTb0000000I09OAE",
    "Executive Customer Risk & Growth": "0FKTb0000000I1lOAE",
    "Executive Product Mix & Industry": "0FKTb0000000IBROA2",
    "Executive Business Health": "0FKTb0000000IbFOAU",
    "Executive Summary": "0FKTb0000000Io9OAE",
    "Pipeline & Opportunity Operations": "0FKTb0000000Hs5OAE",
    "Forecast & Revenue Motions": "0FKTb0000000HthOAE",
    "Customer & Account Health": "0FKTb0000000HvJOAU",
    "Lead Funnel": "0FKTb0000000HwvOAE",
    "Contract Operations & Renewals": "0FKTb0000000HyXOAU",
    "BDR Manager": "0FKTb0000000I8DOAU",
    "BDR Rep Queue": "0FKTb0000000I9pOAE",
    "AE Performance Dashboard": "0FKTb0000000IGHOA2",
    "Sales Operations Command Center": "0FKTb0000000IHtOAM",
    "Manager Coaching Dashboard": "0FKTb0000000IJVOA2",
    "Finance Revenue Operations": "0FKTb0000000IOLOA2",
    "Revenue Retention & Health": "0FKTb0000000ITBOA2",
    "SaaS Transition & Delivery Model": "0FKTb0000000IUnOAM",
    "Sales Activity & Productivity": "0FKTb0000000IRZOA2",
    "KPI Scorecard": "0FKTb0000000IZdOAM",
    "Analytics Command Center": "0FKTb0000000IEfOAM",
    "Product Portfolio & Whitespace Analysis": "0FKTb0000000IWPOA2",
    "Product ML & Recommendations": "0FKTb0000000ID3OAM",
    "Anomaly Detection & Forecasting Lab": "0FKTb0000000IPxOAM",
    "Forecast Intelligence": "0FKTb0000000IcrOAE",
    "Revenue/Pipeline Analyst Lab": "0FKTb0000000I3NOAU",
    "Customer/Revenue Analyst Lab": "0FKTb0000000I4zOAE",
    "Advanced Pipeline Analytics": "0FKTb0000000HnFOAU",
    "Pipeline History": "0FKTb0000000ImXOAU",
    "Opp Management": "0FKTb0000000IjJOAU",
    "Lead Management KPIs": "0FKTb0000000Ig5OAE",
    "Sales Process Compliance KPIs": "0FKTb0000000IeTOAU",
    "Contract Operations KPIs": "0FKTb0000000IhhOAE",
    "Account Intelligence KPIs": "0FKTb0000000IkvOAE",
    "Marketing Pipeline Attribution": "0FKTb0000000IMjOAM",
}


# ── Shared utilities ─────────────────────────────────────────────────────


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


# ── Format inference ─────────────────────────────────────────────────────

# Patterns for format detection (applied to lowercased title + field name)
PCT_PATTERNS = re.compile(
    r"(%|_pct|pct_|percent|rate|coverage|confidence|pacing|"
    r"yoy|qoq|mom|change|delta_p|conversion|win.?rate|"
    r"close.?rate|fill.?rate|attainment|hit.?rate|"
    r"growth.?%|margin|utilization|adoption|completion|"
    r"accuracy|variance_p|ratio|share|penetration|mix)",
    re.IGNORECASE,
)

CURRENCY_PATTERNS = re.compile(
    r"(\$|arr|arpa|revenue|amount|quota|plan|gap|acv|mrr|"
    r"value|closed.?won|pipeline|forecast|target|"
    r"expansion|contract.?value|deal|weighted|bookings|"
    r"billing|spend|budget|cost|price|ltv|cac|"
    r"renewal.?value|open.?weighted|projected|"
    r"new.?business|churn.?arr|net.?retention.?val|"
    r"arpu|arpac|aov|asp|avg.?deal)",
    re.IGNORECASE,
)

DAYS_PATTERNS = re.compile(
    r"(days|hours|hrs|duration|cycle.?time|"
    r"time.?to|dwell|velocity|lead.?time|"
    r"response.?time|first.?touch|cadence|"
    r"avg.?age|median.?age|aging)",
    re.IGNORECASE,
)

COUNT_PATTERNS = re.compile(
    r"(count|total|opps|leads|accounts|contacts|"
    r"activities|tasks|meetings|calls|emails|"
    r"#|num_|number.?of|qty|quantity)",
    re.IGNORECASE,
)

# Viz types where we should NOT set numberFormat on measureAxis1
# (these don't have traditional axes)
NO_AXIS_FORMAT_TYPES = {
    "donut",
    "pie",
    "funnel",
    "waterfall",
    "treemap",
    "gauge",
    "scatter",
    "matrix",
    "heatmap",
}


def infer_number_format(title, field_name=""):
    """Infer the appropriate numberFormat from widget title and field name.

    Returns (format_string, is_currency) tuple.
    """
    combined = f"{title} {field_name}".lower()

    # Percentages first (most specific)
    if PCT_PATTERNS.search(combined):
        return "0.0%", False

    # Currency
    if CURRENCY_PATTERNS.search(combined):
        return "$#,##0", True

    # Days/time
    if DAYS_PATTERNS.search(combined):
        return "#,##0.0", False

    # Counts
    if COUNT_PATTERNS.search(combined):
        return "#,##0", False

    # Scores (0-100 range typically)
    if re.search(r"score", combined, re.IGNORECASE):
        return "#,##0.0", False

    # Default: integer with commas
    return "#,##0", False


def infer_axis_format(axis_title, chart_title="", plot_fields=None):
    """Infer numberFormat for a chart's measureAxis1."""
    combined = f"{axis_title} {chart_title}".lower()
    if plot_fields:
        combined += " " + " ".join(plot_fields).lower()

    if PCT_PATTERNS.search(combined):
        return "0.0%"
    if CURRENCY_PATTERNS.search(combined):
        return "$#,##0"
    if DAYS_PATTERNS.search(combined):
        return "#,##0.0"
    return "#,##0"


# ── Main processing ──────────────────────────────────────────────────────


def process_dashboard(instance, token, label, dashboard_id):
    """Add numberFormat to all number widgets and chart axes in a dashboard."""
    data = get_dashboard(instance, token, dashboard_id)
    state = _normalize_state(data.get("state", {}))

    widgets = state.get("widgets", {})
    num_modified = 0
    chart_modified = 0
    changes = []

    for wname, widget in widgets.items():
        wtype = widget.get("type", "")
        params = widget.get("parameters", {})

        if wtype == "number":
            # NOTE: Wave API PATCH rejects BOTH "numberFormat" AND "compactForm"
            # on number widget parameters (Unrecognized field error).
            # Number display formatting must come from:
            #   1. compact=True (already set on most currency widgets)
            #   2. Dataset XMD numberFormat on the measure field
            #   3. SAQL number_to_string() in the step query
            # We skip number widgets entirely here.
            pass

        elif wtype == "chart":
            viz = params.get("visualizationType", "")

            # Skip viz types that don't have measure axes
            if viz in NO_AXIS_FORMAT_TYPES:
                continue

            axis1 = params.get("measureAxis1", {})
            if not isinstance(axis1, dict):
                continue

            # Skip if already formatted
            if axis1.get("numberFormat"):
                continue

            # Get title and plot fields for inference
            title_obj = params.get("title", {})
            chart_title = (
                title_obj.get("label", "") if isinstance(title_obj, dict) else ""
            )
            axis_title = axis1.get("title", "")

            # Get plot fields from columnMap
            plot_fields = []
            cm = params.get("columnMap", {})
            if isinstance(cm, dict):
                plot_fields = cm.get("plots", [])

            fmt = infer_axis_format(axis_title, chart_title, plot_fields)
            axis1["numberFormat"] = fmt

            chart_modified += 1
            changes.append(f'  CHART {wname}: viz={viz} axis="{axis_title}" → {fmt}')

    if num_modified == 0 and chart_modified == 0:
        print(f"✓ {label}: no changes needed")
        return 0

    # Deploy
    try:
        status = patch_dashboard(instance, token, dashboard_id, state)
        print(
            f"✓ {label}: {status} — {num_modified} numbers, {chart_modified} charts formatted"
        )
        for c in changes:
            print(c)
        return num_modified + chart_modified
    except Exception as e:
        print(f"✗ {label}: FAILED — {e}")
        # Print first 500 chars of error body if available
        if hasattr(e, "read"):
            err_body = e.read().decode("utf-8", errors="replace")[:500]
            print(f"  Error body: {err_body}")
        return 0


def main():
    token, instance = get_token()
    print(f"Auth OK: {instance}")
    print(f"Processing {len(DASHBOARDS)} dashboards...\n")

    total = 0
    success = 0
    errors = 0

    for label, did in sorted(DASHBOARDS.items()):
        try:
            count = process_dashboard(instance, token, label, did)
            total += count
            success += 1
        except Exception as e:
            print(f"✗ {label}: ERROR — {e}")
            errors += 1

    print(f"\n{'=' * 60}")
    print(f"Done: {success} dashboards processed, {errors} errors")
    print(f"Total widgets formatted: {total}")


if __name__ == "__main__":
    main()
