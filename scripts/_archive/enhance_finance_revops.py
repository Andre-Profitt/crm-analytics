#!/usr/bin/env python3
"""Enhance Finance Revenue Operations dashboard with NRR/GRR/Churn metrics.

Adds a 4th page ("Retention") with:
- NRR, GRR, Churn Rate, Ending ARR KPI numbers
- ARR Bridge waterfall chart
- NRR & GRR Trend combo chart
- Quarterly Churn column chart
- Navigation links across all 4 pages

Uses Revenue_Retention_Health dataset.
"""

import json
import sys
import urllib.error
import urllib.request

sys.path.insert(0, "/Users/test/crm-analytics")
from crm_analytics_helpers import get_auth, normalize_dashboard_state_for_patch

DASHBOARD_ID = "0FKTb0000000IOLOA2"
API_PATH = f"/services/data/v66.0/wave/dashboards/{DASHBOARD_ID}"


# ═══════════════════════════════════════════════════════════════════════════
#  New SAQL Steps
# ═══════════════════════════════════════════════════════════════════════════

NEW_STEPS = {
    "s_nrr_grr": {
        "query": (
            'q = load "Revenue_Retention_Health";\n'
            'q = filter q by RecordType == "yearly_metric";\n'
            "q = order q by Year desc;\n"
            "q = limit q 1;"
        ),
        "type": "saql",
    },
    "s_retention_trend": {
        "query": (
            'q = load "Revenue_Retention_Health";\n'
            'q = filter q by RecordType == "yearly_metric";\n'
            "q = foreach q generate QuarterLabel as Year, NRR, GRR, ChurnRate, "
            "StartingARR, ExpansionARR, ChurnARR, EndingARR, NewLogoARR;\n"
            "q = order q by Year asc;"
        ),
        "type": "saql",
    },
    "s_arr_bridge": {
        "query": (
            'q = load "Revenue_Retention_Health";\n'
            'q = filter q by RecordType == "waterfall";\n'
            "q = foreach q generate Motion as Category, Amount, "
            'case when Motion == "Starting ARR" then 1 '
            'when Motion == "Renewal Won" then 2 '
            'when Motion == "Expansion" then 3 '
            'when Motion == "New Logos" then 4 '
            'when Motion == "Churn" then 5 '
            'when Motion == "Ending ARR" then 6 '
            "else 7 end as SortOrder;\n"
            "q = order q by SortOrder asc;"
        ),
        "type": "saql",
    },
    "s_churn_trend": {
        "query": (
            'q = load "Revenue_Retention_Health";\n'
            'q = filter q by RecordType == "opp_detail" '
            'and OppType == "Renewal" and IsClosed == 1 and IsWon == 0;\n'
            "q = group q by QuarterLabel;\n"
            "q = foreach q generate QuarterLabel, sum(ARR) as ChurnedARR, "
            "count() as ChurnedDeals;\n"
            "q = order q by QuarterLabel asc;"
        ),
        "type": "saql",
    },
}


# ═══════════════════════════════════════════════════════════════════════════
#  Widget Builders
# ═══════════════════════════════════════════════════════════════════════════


def make_number_widget(step, field, title, color, compact=False):
    return {
        "type": "number",
        "parameters": {
            "step": step,
            "measureField": field,
            "title": title,
            "titleColor": "#546E7A",
            "titleSize": 12,
            "numberColor": color,
            "numberSize": 24,
            "compact": compact,
            "textAlignment": "center",
            "exploreLink": False,
            "interactions": [],
        },
    }


def make_chart_widget(viz_type, step, title, measure_title="ARR"):
    w = {
        "type": "chart",
        "parameters": {
            "visualizationType": viz_type,
            "step": step,
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "autoFitMode": "fit",
            "showValues": True,
            "showActionMenu": True,
            "exploreLink": True,
            "theme": "wave",
            "applyConditionalFormatting": True,
            "interactions": [],
            "legend": {
                "show": True,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "measureAxis1": {
                "showAxis": True,
                "showTitle": True,
                "title": measure_title,
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
            },
            "dimensionAxis": {
                "showAxis": True,
                "showTitle": False,
                "title": "",
                "customSize": "auto",
                "icons": {
                    "useIcons": False,
                    "iconProps": {"fit": "cover", "column": "", "type": "round"},
                },
            },
        },
    }
    return w


def make_text_widget(title, subtitle):
    return {
        "type": "text",
        "parameters": {
            "content": {
                "richTextContent": [
                    {
                        "attributes": {
                            "size": "24px",
                            "color": "#091A3E",
                            "bold": True,
                        },
                        "insert": title,
                    },
                    {"attributes": {}, "insert": "\n"},
                    {
                        "attributes": {"size": "14px", "color": "#54698D"},
                        "insert": subtitle,
                    },
                    {"attributes": {}, "insert": "\n"},
                ]
            },
            "interactions": [],
        },
    }


def make_link_widget(page_name, label, active=False):
    return {
        "type": "link",
        "parameters": {
            "destinationType": "page",
            "destinationLink": {"name": page_name},
            "text": label,
            "fontSize": 14,
            "textAlignment": "center",
            "textColor": "#091A3E" if active else "#0070D2",
            "includeState": False,
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Page 4 Widgets
# ═══════════════════════════════════════════════════════════════════════════

NEW_WIDGETS = {
    # Header
    "p4_hdr": make_text_widget(
        "Revenue Retention & Unit Economics",
        "NRR, GRR, churn rate, and ARR bridge \u2014 the financial health of the installed base",
    ),
    # KPI Numbers
    "p4_n_nrr": make_number_widget(
        "s_nrr_grr", "NRR", "Net Revenue Retention %", "#0070D2"
    ),
    "p4_n_grr": make_number_widget(
        "s_nrr_grr", "GRR", "Gross Revenue Retention %", "#04844B"
    ),
    "p4_n_churn": make_number_widget(
        "s_nrr_grr", "ChurnRate", "Churn Rate %", "#D4504C"
    ),
    "p4_n_ending": make_number_widget(
        "s_nrr_grr", "EndingARR", "Ending ARR", "#9050E9", compact=True
    ),
    # Charts — waterfall and combo get NO columnMap (crashes them)
    "p4_ch_bridge": make_chart_widget(
        "waterfall", "s_arr_bridge", "ARR Bridge \u2014 Starting \u2192 Ending"
    ),
    "p4_ch_trend": make_chart_widget(
        "combo", "s_retention_trend", "NRR & GRR Trend by Year", "Rate %"
    ),
    "p4_ch_churn": make_chart_widget(
        "column", "s_churn_trend", "Quarterly Churn", "Churned ARR"
    ),
    # Page 4 navigation links
    "p4_nav1": make_link_widget("revenue", "Revenue"),
    "p4_nav2": make_link_widget("pipeline", "Pipeline"),
    "p4_nav3": make_link_widget("forecast", "Forecast"),
    "p4_nav4": make_link_widget("retention", "Retention", active=True),
}


# ═══════════════════════════════════════════════════════════════════════════
#  Page 4 Layout
# ═══════════════════════════════════════════════════════════════════════════

PAGE_4_LAYOUT = {
    "name": "retention",
    "widgets": [
        {"name": "p4_nav1", "row": 0, "column": 0, "colspan": 3, "rowspan": 1},
        {"name": "p4_nav2", "row": 0, "column": 3, "colspan": 3, "rowspan": 1},
        {"name": "p4_nav3", "row": 0, "column": 6, "colspan": 3, "rowspan": 1},
        {"name": "p4_nav4", "row": 0, "column": 9, "colspan": 3, "rowspan": 1},
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_n_nrr", "row": 3, "column": 0, "colspan": 3, "rowspan": 3},
        {"name": "p4_n_grr", "row": 3, "column": 3, "colspan": 3, "rowspan": 3},
        {"name": "p4_n_churn", "row": 3, "column": 6, "colspan": 3, "rowspan": 3},
        {"name": "p4_n_ending", "row": 3, "column": 9, "colspan": 3, "rowspan": 3},
        {"name": "p4_ch_bridge", "row": 6, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p4_ch_trend", "row": 14, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p4_ch_churn", "row": 14, "column": 6, "colspan": 6, "rowspan": 7},
    ],
}


# ═══════════════════════════════════════════════════════════════════════════
#  Nav Link Updater — adds 4th nav to existing pages
# ═══════════════════════════════════════════════════════════════════════════

# Map page prefixes to their page names
PAGE_NAV_CONFIG = {
    "p1": {"page_name": "revenue", "label": "Revenue"},
    "p2": {"page_name": "pipeline", "label": "Pipeline"},
    "p3": {"page_name": "forecast", "label": "Forecast"},
}


def add_nav4_to_existing_pages(state):
    """Add a 4th navigation link (Retention) to pages 1-3.

    For each page:
    - Resize existing 3 nav links from colspan=4 to colspan=3
    - Adjust column positions to 0, 3, 6
    - Add new nav4 widget at column=9, colspan=3
    """
    widgets = state["widgets"]
    grid = state["gridLayouts"][0]

    for prefix, cfg in PAGE_NAV_CONFIG.items():
        page_name = cfg["page_name"]

        # Add the new nav4 widget definition
        nav4_name = f"{prefix}_nav4"
        widgets[nav4_name] = make_link_widget("retention", "Retention")

        # Find the page in gridLayout and update nav widgets
        for page in grid["pages"]:
            if page.get("name") == page_name:
                # Resize existing nav widgets (nav1, nav2, nav3) from colspan=4 to colspan=3
                nav_positions = [0, 3, 6]  # New column positions
                nav_idx = 0
                for w in page["widgets"]:
                    w_name = w.get("name", "")
                    if w_name.startswith(prefix + "_nav") and w_name != nav4_name:
                        if nav_idx < len(nav_positions):
                            w["colspan"] = 3
                            w["column"] = nav_positions[nav_idx]
                            nav_idx += 1

                # Add the 4th nav widget to the page layout
                page["widgets"].append(
                    {
                        "name": nav4_name,
                        "row": 0,
                        "column": 9,
                        "colspan": 3,
                        "rowspan": 1,
                    }
                )
                break


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main():
    print("=" * 60)
    print("Enhance Finance Revenue Operations — NRR/GRR/Churn")
    print("=" * 60)

    # 1. Auth
    print("\n1. Authenticating...")
    inst, tok = get_auth()
    headers = {
        "Authorization": f"Bearer {tok}",
        "Content-Type": "application/json",
    }
    print(f"   Instance: {inst}")

    # 2. GET current dashboard state
    print("\n2. Fetching dashboard state...")
    url = f"{inst}{API_PATH}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as resp:
        dash = json.loads(resp.read().decode())
    state = dash["state"]
    n_steps = len(state.get("steps", {}))
    n_widgets = len(state.get("widgets", {}))
    n_pages = len(state.get("gridLayouts", [{}])[0].get("pages", []))
    print(f"   Current: {n_steps} steps, {n_widgets} widgets, {n_pages} pages")

    # 3. Clean state (MANDATORY for GET→PATCH round-trip)
    print("\n3. Cleaning state (unescape SAQL, strip layouts/metadata)...")
    state = normalize_dashboard_state_for_patch(state)
    print("   Done.")

    # 4. Add new SAQL steps
    print("\n4. Adding 4 new SAQL steps...")
    for step_name, step_def in NEW_STEPS.items():
        state["steps"][step_name] = step_def
        print(f"   + {step_name}")

    # 5. Add new widgets
    print("\n5. Adding new widgets...")
    for widget_name, widget_def in NEW_WIDGETS.items():
        state["widgets"][widget_name] = widget_def
        print(f"   + {widget_name}")

    # 6. Add 4th nav link to existing pages (p1, p2, p3)
    print("\n6. Updating existing pages with 4th nav link...")
    add_nav4_to_existing_pages(state)
    print("   + p1_nav4 (Revenue page)")
    print("   + p2_nav4 (Pipeline page)")
    print("   + p3_nav4 (Forecast page)")

    # 7. Add page 4 to grid layout
    print("\n7. Adding Retention page to grid layout...")
    state["gridLayouts"][0]["pages"].append(PAGE_4_LAYOUT)
    print("   + retention page (12 widgets)")

    # 8. Summary before deploy
    n_steps_new = len(state.get("steps", {}))
    n_widgets_new = len(state.get("widgets", {}))
    n_pages_new = len(state.get("gridLayouts", [{}])[0].get("pages", []))
    print(
        f"\n   Result: {n_steps_new} steps (+{n_steps_new - n_steps}), "
        f"{n_widgets_new} widgets (+{n_widgets_new - n_widgets}), "
        f"{n_pages_new} pages (+{n_pages_new - n_pages})"
    )

    # 9. PATCH
    print("\n8. Deploying via PATCH...")
    body = json.dumps({"state": state})
    print(f"   Payload: {len(body):,} bytes")
    req = urllib.request.Request(
        url, data=body.encode(), method="PATCH", headers=headers
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode())
            print(f"\n   OK — Dashboard '{result.get('name')}' updated successfully")
            st = result.get("state", {})
            print(f"   Steps:   {len(st.get('steps', {}))}")
            print(f"   Widgets: {len(st.get('widgets', {}))}")
            gl = st.get("gridLayouts", [{}])[0]
            for p in gl.get("pages", []):
                print(
                    f"   Page '{p.get('label', p.get('name'))}': "
                    f"{len(p.get('widgets', []))} widgets"
                )
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"\n   FAIL (HTTP {e.code}): {err[:2000]}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Done.")
    print("=" * 60)


if __name__ == "__main__":
    main()
