#!/usr/bin/env python3
"""Fix unnamed pages and add filter controls to 6 dashboards missing them.

Part 1: Rename unnamed pages on Executive Revenue & Forecast + Executive Pipeline Risk
Part 2: Add filter controls to 6 dashboards:
  - Executive Summary
  - Finance Revenue Operations
  - Marketing Pipeline Attribution
  - Sales Operations Command Center
  - Manager Coaching Dashboard
  - AE Performance Dashboard
"""

import json
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _normalize_state(state, *, strip_page_labels=False):
    from crm_analytics_helpers import normalize_dashboard_state_for_patch

    return normalize_dashboard_state_for_patch(
        state,
        strip_page_labels=strip_page_labels,
    )


def sf_auth():
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", "apro@simcorp.com", "--json"],
        capture_output=True,
        text=True,
    )
    d = json.loads(r.stdout)["result"]
    return d["accessToken"], d["instanceUrl"]

def get_dash(token, url, did):
    req = urllib.request.Request(
        f"{url}/services/data/v66.0/wave/dashboards/{did}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def patch_dash(token, url, did, state):
    data = json.dumps({"state": state}).encode()
    req = urllib.request.Request(
        f"{url}/services/data/v66.0/wave/dashboards/{did}",
        data=data,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        print(f"  ERROR {e.code}: {e.read().decode()[:500]}")
        raise

def get_pages(state):
    gl = state.get("gridLayout")
    if gl and isinstance(gl, dict):
        return gl.get("pages", []), "rowSpan", "colSpan"
    gls = state.get("gridLayouts")
    if gls and isinstance(gls, list) and len(gls) > 0:
        pp = gls[0].get("pages", [])
        for p in pp:
            for w in p.get("widgets", []):
                if "rowspan" in w:
                    return pp, "rowspan", "colspan"
        return pp, "rowSpan", "colSpan"
    return [], "rowSpan", "colSpan"


def make_filter_step(ds_name, field_name, step_name):
    """Create an aggregateflex step for a filter selector.
    Uses the correct nested-JSON query format for aggregateflex steps."""
    inner_query = json.dumps({"measures": [["count", "*"]], "groups": [field_name]})
    return {
        "type": "aggregateflex",
        "query": {"query": inner_query, "version": -1.0},
        "datasets": [{"name": ds_name}],
        "broadcastFacet": True,
        "isGlobal": False,
        "selectMode": "multi",
        "receiveFacetSource": {"mode": "all", "steps": []},
    }


def make_filter_widget(step_name, title):
    """Create a listselector widget."""
    return {
        "type": "listselector",
        "parameters": {
            "step": step_name,
            "title": title,
            "instant": True,
            "compact": True,
            "expanded": False,
        },
    }


def shift_rows(page, rs_key, amount):
    """Shift all widget rows down by amount to make room for control bar."""
    for wref in page.get("widgets", []):
        wref["row"] = wref["row"] + amount


def add_filters_to_dashboard(token, url, did, label, filter_defs, page_labels=None):
    """
    Add filter controls to a dashboard.
    filter_defs: list of (dataset_name, field_name, step_name, widget_name, title)
    page_labels: optional dict of {page_index: {name: ..., label: ...}} to rename pages
    """
    print(f"\n  Processing: {label} ({did})")
    dash = get_dash(token, url, did)
    state = _normalize_state(dash["state"], strip_page_labels=False)
    pages, rs_key, cs_key = get_pages(state)

    # Rename pages if needed
    if page_labels:
        for idx, props in page_labels.items():
            if idx < len(pages):
                for k, v in props.items():
                    pages[idx][k] = v
                    print(f"    Page {idx}: set {k}={v}")

    # Add filter steps
    for ds_name, field_name, step_name, _, _ in filter_defs:
        state["steps"][step_name] = make_filter_step(ds_name, field_name, step_name)

    # Add filter widgets
    for _, _, step_name, widget_name, title in filter_defs:
        state["widgets"][widget_name] = make_filter_widget(step_name, title)

    # Place filter widgets on first page (row 0, shift existing content down)
    first_page = pages[0]
    n_filters = len(filter_defs)

    if n_filters > 0:
        # Shift existing widgets down by 2 rows to make room
        shift_rows(first_page, rs_key, 2)

        # Place filters in a row at top
        cols_per_filter = 12 // min(n_filters, 4)
        for i, (_, _, _, widget_name, _) in enumerate(filter_defs):
            col = (i % 4) * cols_per_filter
            row = 0 if i < 4 else 1  # second row if > 4 filters
            first_page["widgets"].append(
                {
                    "name": widget_name,
                    "row": row,
                    "column": col,
                    cs_key: cols_per_filter,
                    rs_key: 1,
                }
            )

    status = patch_dash(token, url, did, state)
    print(f"    PATCH: {status} — Added {n_filters} filter controls")
    return status


def main():
    token, url = sf_auth()

    # ═══ PART 1: Fix unnamed pages ═══
    print("=" * 60)
    print("PART 1: Fix unnamed pages")
    print("=" * 60)

    # Executive Revenue & Forecast — "Page 0" → "Revenue & Pacing", "Page 1" → "Pipeline Coverage"
    add_filters_to_dashboard(
        token,
        url,
        "0FKTb0000000HqTOAU",
        "Executive Revenue & Forecast",
        [],  # no new filters (already has 6 listselectors)
        page_labels={
            0: {"label": "Revenue & Pacing", "name": "revenue"},
            1: {"label": "Pipeline Coverage", "name": "pipeline"},
        },
    )

    # Executive Pipeline Risk & Process — "Page 0" → "Risk Overview", "Page 1" → "Process Compliance"
    add_filters_to_dashboard(
        token,
        url,
        "0FKTb0000000I09OAE",
        "Executive Pipeline Risk & Process",
        [],  # no new filters (already has 6 listselectors)
        page_labels={
            0: {"label": "Risk Overview", "name": "risk"},
            1: {"label": "Process Compliance", "name": "process"},
        },
    )

    # ═══ PART 2: Add filter controls to 6 dashboards ═══
    print("\n" + "=" * 60)
    print("PART 2: Add filter controls to 6 dashboards")
    print("=" * 60)

    # 1. Executive Summary — light touch: FiscalQuarter only
    add_filters_to_dashboard(
        token,
        url,
        "0FKTb0000000Io9OAE",
        "Executive Summary",
        [
            (
                "KPI_Scorecard",
                "Category",
                "sf_kpi_category",
                "w_f_category",
                "KPI Category",
            ),
        ],
    )

    # 2. Finance Revenue Operations — FiscalQuarter + Region
    add_filters_to_dashboard(
        token,
        url,
        "0FKTb0000000IOLOA2",
        "Finance Revenue Operations",
        [
            ("Forecast_Revenue_Motions", "FYLabel", "sf_fy", "w_f_fy", "Fiscal Year"),
            (
                "Pipeline_Opportunity_Operations",
                "Region",
                "sf_region",
                "w_f_region",
                "Region",
            ),
            (
                "Pipeline_Opportunity_Operations",
                "ProductFamily",
                "sf_product",
                "w_f_product",
                "Product Family",
            ),
        ],
    )

    # 3. Marketing Pipeline Attribution — Channel + Quarter
    add_filters_to_dashboard(
        token,
        url,
        "0FKTb0000000IMjOAM",
        "Marketing Pipeline Attribution",
        [
            (
                "Pipeline_Opportunity_Operations",
                "ForecastCategory",
                "sf_fcat",
                "w_f_fcat",
                "Forecast Category",
            ),
            ("Lead_Funnel", "LeadSource", "sf_source", "w_f_source", "Lead Source"),
            (
                "Pipeline_Opportunity_Operations",
                "Region",
                "sf_region2",
                "w_f_region2",
                "Region",
            ),
        ],
    )

    # 4. Sales Operations Command Center — Owner + Quarter + Stage
    add_filters_to_dashboard(
        token,
        url,
        "0FKTb0000000IHtOAM",
        "Sales Operations Command Center",
        [
            (
                "Pipeline_Opportunity_Operations",
                "OwnerName",
                "sf_owner",
                "w_f_owner",
                "Owner",
            ),
            (
                "Executive_Revenue_Forecast",
                "FiscalMonth",
                "sf_month",
                "w_f_month",
                "Fiscal Month",
            ),
            (
                "Pipeline_Opportunity_Operations",
                "StageName",
                "sf_stage",
                "w_f_stage",
                "Stage",
            ),
        ],
    )

    # 5. Manager Coaching Dashboard — Owner + Quarter
    add_filters_to_dashboard(
        token,
        url,
        "0FKTb0000000IJVOA2",
        "Manager Coaching Dashboard",
        [
            (
                "Pipeline_Opportunity_Operations",
                "OwnerName",
                "sf_owner3",
                "w_f_owner3",
                "Rep / Owner",
            ),
            ("Forecast_Revenue_Motions", "FYLabel", "sf_fy3", "w_f_fy3", "Fiscal Year"),
        ],
    )

    # 6. AE Performance Dashboard — Owner + Quarter + Stage
    add_filters_to_dashboard(
        token,
        url,
        "0FKTb0000000IGHOA2",
        "AE Performance Dashboard",
        [
            (
                "Pipeline_Opportunity_Operations",
                "OwnerName",
                "sf_owner4",
                "w_f_owner4",
                "Rep / Owner",
            ),
            (
                "Pipeline_Opportunity_Operations",
                "StageName",
                "sf_stage4",
                "w_f_stage4",
                "Stage",
            ),
            ("Forecast_Revenue_Motions", "FYLabel", "sf_fy4", "w_f_fy4", "Fiscal Year"),
        ],
    )

    print("\n" + "=" * 60)
    print("DONE — Pages renamed + 6 dashboards now have filter controls!")
    print("=" * 60)


if __name__ == "__main__":
    main()
