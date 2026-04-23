#!/usr/bin/env python3
"""Fix ACC Navigate page - wire all 15 link widgets with actual dashboard IDs.
Also expand the Navigate page to include ALL Gen 2 dashboards organized by tier."""

import json
from pathlib import Path
import subprocess
import sys

DASHBOARD_ID = "0FKTb0000000IEfOAM"

# ── Link → Dashboard ID mapping ──
LINK_MAP = {
    # Executive Suite
    "w_nav_exec_0": "0FKTb0000000IbFOAU",   # Executive Business Health
    "w_nav_exec_1": "0FKTb0000000HqTOAU",   # Executive Revenue & Forecast
    "w_nav_exec_2": "0FKTb0000000HnFOAU",   # Advanced Pipeline Analytics
    "w_nav_exec_3": "0FKTb0000000I1lOAE",   # Executive Customer Risk & Growth
    "w_nav_exec_4": "0FKTb0000000IWPOA2",   # Product Portfolio & Whitespace Analysis
    # Manager Hub
    "w_nav_mgr_0":  "0FKTb0000000Hs5OAE",   # Pipeline & Opportunity Operations
    "w_nav_mgr_1":  "0FKTb0000000HthOAE",   # Forecast & Revenue Motions
    "w_nav_mgr_2":  "0FKTb0000000HwvOAE",   # Lead Funnel
    "w_nav_mgr_3":  "0FKTb0000000HyXOAU",   # Contract Operations & Renewals
    "w_nav_mgr_4":  "0FKTb0000000I8DOAU",   # BDR Manager
    "w_nav_mgr_5":  "0FKTb0000000IHtOAM",   # Sales Operations Command Center
    "w_nav_mgr_6":  "0FKTb0000000IMjOAM",   # Marketing Pipeline Attribution
    "w_nav_mgr_7":  "0FKTb0000000IZdOAM",   # KPI Scorecard
    # Analyst Labs
    "w_nav_analyst_0": "0FKTb0000000I3NOAU", # Revenue/Pipeline Analyst Lab
    "w_nav_analyst_1": "0FKTb0000000I4zOAE", # Customer/Revenue Analyst Lab
}

# ── Additional dashboards to add as new rows ──
ADDITIONAL_EXEC = [
    ("Executive Pipeline Risk & Process",   "0FKTb0000000I09OAE"),
    ("Executive Product Mix & Industry",    "0FKTb0000000IBROA2"),
    ("Executive Summary",                   "0FKTb0000000Io9OAE"),
]

ADDITIONAL_MGR = [
    ("Customer & Account Health",           "0FKTb0000000HvJOAU"),
    ("Revenue Retention & Health",          "0FKTb0000000ITBOA2"),
    ("Product ML & Recommendations",        "0FKTb0000000ID3OAM"),
    ("SaaS Transition & Delivery Model",    "0FKTb0000000IUnOAM"),
]

ADDITIONAL_ANALYST = [
    ("Anomaly Detection & Forecasting Lab", "0FKTb0000000IPxOAM"),
    ("Forecast Intelligence",               "0FKTb0000000IcrOAE"),
]

# ── Additional "Specialty" tier ──
SPECIALTY = [
    ("Sales Activity & Productivity",       "0FKTb0000000IRZOA2"),
    ("AE Performance Dashboard",            "0FKTb0000000IGHOA2"),
    ("Manager Coaching Dashboard",          "0FKTb0000000IJVOA2"),
    ("Finance Revenue Operations",          "0FKTb0000000IOLOA2"),
    ("BDR Rep Queue",                       "0FKTb0000000I9pOAE"),
    ("Pipeline History",                    "0FKTb0000000ImXOAU"),
]

# ── Additional KPI dashboards ──
KPI_DASHBOARDS = [
    ("Lead Management KPIs",               "0FKTb0000000Ig5OAE"),
    ("Sales Process Compliance KPIs",      "0FKTb0000000IeTOAU"),
    ("Contract Operations KPIs",           "0FKTb0000000IhhOAE"),
    ("Account Intelligence KPIs",          "0FKTb0000000IkvOAE"),
    ("Opp Management",                     "0FKTb0000000IjJOAU"),
]

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
        capture_output=True, text=True
    )
    d = json.loads(r.stdout)["result"]
    return d["accessToken"], d["instanceUrl"]


def get_dashboard(token, url):
    import urllib.request
    req = urllib.request.Request(
        f"{url}/services/data/v66.0/wave/dashboards/{DASHBOARD_ID}",
        headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def patch_dashboard(token, url, state):
    import urllib.request
    data = json.dumps({"state": state}).encode()
    req = urllib.request.Request(
        f"{url}/services/data/v66.0/wave/dashboards/{DASHBOARD_ID}",
        data=data, method="PATCH",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    )
    with urllib.request.urlopen(req) as resp:
        return resp.status


def make_link_widget(text, dashboard_id):
    return {
        "type": "link",
        "parameters": {
            "destinationLink": {"id": dashboard_id},
            "destinationType": "dashboard",
            "fontSize": 14,
            "includeState": True,
            "text": text,
            "textAlignment": "center",
            "textColor": "#0070D2"
        }
    }


def make_section_label(text):
    return {
        "type": "text",
        "parameters": {
            "content": {
                "richTextContent": [
                    {"attributes": {"size": "16px", "color": "#54698D", "bold": True}, "insert": text},
                    {"attributes": {"align": "left"}, "insert": "\n"}
                ]
            },
            "interactions": []
        }
    }


def main():
    print("1. Authenticating...")
    token, url = sf_auth()

    print("2. Fetching dashboard state...")
    dash = get_dashboard(token, url)
    state = _normalize_state(dash["state"], strip_page_labels=False)

    # 3. Wire existing 15 link widgets
    print("3. Wiring 15 existing link widgets...")
    wired = 0
    for wname, did in LINK_MAP.items():
        w = state["widgets"].get(wname)
        if w and w["type"] == "link":
            w["parameters"]["destinationLink"] = {"id": did}
            wired += 1
            print(f"   ✓ {wname} → {did}")
    print(f"   Wired {wired}/15 links")

    # 4. Add new link widgets for dashboards not yet on the Navigate page
    print("4. Adding additional dashboard links...")
    nav_page = None
    for page in state["gridLayouts"][0]["pages"]:
        if page["name"] == "navigate":
            nav_page = page
            break

    if not nav_page:
        print("ERROR: Navigate page not found!")
        sys.exit(1)

    # Find current max row on navigate page
    max_row = 0
    for wref in nav_page["widgets"]:
        end_row = wref["row"] + wref.get("rowspan", wref.get("rowSpan", 1))
        if end_row > max_row:
            max_row = end_row

    current_row = max_row

    # Add Executive additions (row of 3, then 1 alone)
    if ADDITIONAL_EXEC:
        # Add section label
        label_name = "w_nav_exec_extra_label"
        state["widgets"][label_name] = make_section_label("Executive Suite (continued)")
        nav_page["widgets"].append({"name": label_name, "row": current_row, "column": 0, "colspan": 12, "rowspan": 1})
        current_row += 1

        for i, (text, did) in enumerate(ADDITIONAL_EXEC):
            wname = f"w_nav_exec_x{i}"
            state["widgets"][wname] = make_link_widget(text, did)
            col = (i % 3) * 4
            nav_page["widgets"].append({"name": wname, "row": current_row, "column": col, "colspan": 4, "rowspan": 2})
            if (i + 1) % 3 == 0:
                current_row += 2
        if len(ADDITIONAL_EXEC) % 3 != 0:
            current_row += 2
        print(f"   Added {len(ADDITIONAL_EXEC)} executive links")

    # Add Manager additions (row of 4)
    if ADDITIONAL_MGR:
        label_name = "w_nav_mgr_extra_label"
        state["widgets"][label_name] = make_section_label("Manager Hub (continued)")
        nav_page["widgets"].append({"name": label_name, "row": current_row, "column": 0, "colspan": 12, "rowspan": 1})
        current_row += 1

        for i, (text, did) in enumerate(ADDITIONAL_MGR):
            wname = f"w_nav_mgr_x{i}"
            state["widgets"][wname] = make_link_widget(text, did)
            col = (i % 4) * 3
            nav_page["widgets"].append({"name": wname, "row": current_row, "column": col, "colspan": 3, "rowspan": 2})
            if (i + 1) % 4 == 0:
                current_row += 2
        if len(ADDITIONAL_MGR) % 4 != 0:
            current_row += 2
        print(f"   Added {len(ADDITIONAL_MGR)} manager links")

    # Add Analyst additions
    if ADDITIONAL_ANALYST:
        label_name = "w_nav_analyst_extra_label"
        state["widgets"][label_name] = make_section_label("Analyst Labs (continued)")
        nav_page["widgets"].append({"name": label_name, "row": current_row, "column": 0, "colspan": 12, "rowspan": 1})
        current_row += 1

        for i, (text, did) in enumerate(ADDITIONAL_ANALYST):
            wname = f"w_nav_analyst_x{i}"
            state["widgets"][wname] = make_link_widget(text, did)
            col = (i % 2) * 6
            nav_page["widgets"].append({"name": wname, "row": current_row, "column": col, "colspan": 6, "rowspan": 2})
            if (i + 1) % 2 == 0:
                current_row += 2
        if len(ADDITIONAL_ANALYST) % 2 != 0:
            current_row += 2
        print(f"   Added {len(ADDITIONAL_ANALYST)} analyst links")

    # Add Specialty section
    if SPECIALTY:
        label_name = "w_nav_specialty_label"
        state["widgets"][label_name] = make_section_label("Specialty & Operations")
        nav_page["widgets"].append({"name": label_name, "row": current_row, "column": 0, "colspan": 12, "rowspan": 1})
        current_row += 1

        for i, (text, did) in enumerate(SPECIALTY):
            wname = f"w_nav_spec_{i}"
            state["widgets"][wname] = make_link_widget(text, did)
            col = (i % 3) * 4
            nav_page["widgets"].append({"name": wname, "row": current_row, "column": col, "colspan": 4, "rowspan": 2})
            if (i + 1) % 3 == 0:
                current_row += 2
        if len(SPECIALTY) % 3 != 0:
            current_row += 2
        print(f"   Added {len(SPECIALTY)} specialty links")

    # Add KPI dashboard section
    if KPI_DASHBOARDS:
        label_name = "w_nav_kpi_label"
        state["widgets"][label_name] = make_section_label("KPI Dashboards")
        nav_page["widgets"].append({"name": label_name, "row": current_row, "column": 0, "colspan": 12, "rowspan": 1})
        current_row += 1

        for i, (text, did) in enumerate(KPI_DASHBOARDS):
            wname = f"w_nav_kpi_{i}"
            state["widgets"][wname] = make_link_widget(text, did)
            col = (i % 3) * 4
            nav_page["widgets"].append({"name": wname, "row": current_row, "column": col, "colspan": 4, "rowspan": 2})
            if (i + 1) % 3 == 0:
                current_row += 2
        if len(KPI_DASHBOARDS) % 3 != 0:
            current_row += 2
        print(f"   Added {len(KPI_DASHBOARDS)} KPI links")

    # 5. Count total widgets
    total_widgets = len(state["widgets"])
    nav_widgets = len(nav_page["widgets"])
    print(f"\n   Total widgets: {total_widgets}")
    print(f"   Navigate page widgets: {nav_widgets}")

    # 6. Deploy
    print("5. Deploying...")
    status = patch_dashboard(token, url, state)
    print(f"   PATCH status: {status}")
    print(f"\n✅ ACC Navigate page wired with {wired} existing + {total_widgets - 31} new links")


if __name__ == "__main__":
    main()
