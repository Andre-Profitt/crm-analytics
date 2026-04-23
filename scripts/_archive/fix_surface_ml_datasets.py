#!/usr/bin/env python3
"""Surface orphaned ML datasets in existing dashboards:
1. Add Whitespace Propensity + Next-Family tables to Product Portfolio & Whitespace Analysis
2. Add ML Model Health + Data Freshness alerts to Analytics Command Center
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

def get_dashboard(token, url, did):
    req = urllib.request.Request(
        f"{url}/services/data/v66.0/wave/dashboards/{did}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def patch_dashboard(token, url, did, state):
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
        body = e.read().decode()
        print(f"  ERROR {e.code}: {body[:500]}")
        raise

def get_pages(state):
    gl = state.get("gridLayout")
    if gl and isinstance(gl, dict):
        return gl.get("pages", []), "rowSpan", "colSpan"
    gls = state.get("gridLayouts")
    if gls and isinstance(gls, list) and len(gls) > 0:
        pages = gls[0].get("pages", [])
        # Detect key format
        for p in pages:
            for w in p.get("widgets", []):
                if "rowspan" in w:
                    return pages, "rowspan", "colspan"
                if "rowSpan" in w:
                    return pages, "rowSpan", "colSpan"
        return pages, "rowspan", "colspan"  # default to lowercase
    return [], "rowSpan", "colSpan"


def max_row(page, rs_key):
    mx = 0
    for w in page.get("widgets", []):
        end = w["row"] + w.get(rs_key, 1)
        if end > mx:
            mx = end
    return mx


def main():
    token, url = sf_auth()

    # ═══════════════════════════════════════════════════════════
    # PART 1: Product Portfolio & Whitespace Analysis
    # ═══════════════════════════════════════════════════════════
    PPW_ID = "0FKTb0000000IWPOA2"
    print("=" * 60)
    print("PART 1: Product Portfolio & Whitespace Analysis")
    print("=" * 60)

    dash = get_dashboard(token, url, PPW_ID)
    state = _normalize_state(dash["state"], strip_page_labels=False)
    pages, rs_key, cs_key = get_pages(state)

    print(f"  Format: {rs_key}/{cs_key}")
    print(f"  Pages: {len(pages)}")
    for i, p in enumerate(pages):
        print(
            f"    Page {i}: {p.get('label', p.get('name', f'P{i}'))} — {len(p.get('widgets', []))} widgets"
        )

    # Add to last page (Page 3)
    target_page = pages[-1]
    row = max_row(target_page, rs_key)
    print(f"  Adding to page {len(pages) - 1}, starting at row {row}")

    # Add section header
    state["widgets"]["w_ml_header"] = {
        "type": "text",
        "parameters": {
            "content": {
                "richTextContent": [
                    {
                        "attributes": {
                            "size": "16px",
                            "color": "#16325C",
                            "bold": True,
                        },
                        "insert": "ML-Powered Insights",
                    },
                    {"attributes": {"align": "left"}, "insert": "\n"},
                ]
            },
            "interactions": [],
        },
    }
    target_page["widgets"].append(
        {"name": "w_ml_header", "row": row, "column": 0, cs_key: 12, rs_key: 1}
    )
    row += 1

    # Whitespace propensity top-20 table
    state["steps"]["s_ml_whitespace_top20"] = {
        "type": "saql",
        "query": "q = load \"Whitespace_Propensity_Scores\";\nq = filter q by CurrentlyOwned == \"No\";\nq = filter q by PropensityScore >= 20;\nq = foreach q generate 'AccountName' as 'Account', 'ProductFamily' as 'Product Family', PropensityScore as 'Propensity Score', PeerAdoptionRate as 'Peer Adoption %';\nq = order q by 'Propensity Score' desc;\nq = limit q 20;\n",
        "broadcastFacet": True,
    }
    state["widgets"]["w_ml_whitespace_table"] = {
        "type": "chart",
        "parameters": {
            "visualizationType": "valuesTable",
            "step": "s_ml_whitespace_top20",
            "theme": "wave",
            "title": {
                "fontSize": 14,
                "subtitleFontSize": 11,
                "label": "Top 20 Whitespace Opportunities (ML Propensity)",
                "align": "center",
                "subtitleLabel": "Accounts not owning product, ranked by ML propensity score",
            },
        },
    }
    target_page["widgets"].append(
        {
            "name": "w_ml_whitespace_table",
            "row": row,
            "column": 0,
            cs_key: 12,
            rs_key: 8,
        }
    )
    row += 8

    # Whitespace propensity by product family hbar
    state["steps"]["s_ml_whitespace_by_family"] = {
        "type": "saql",
        "query": "q = load \"Whitespace_Propensity_Scores\";\nq = filter q by CurrentlyOwned == \"No\";\nq = filter q by PropensityScore >= 20;\nq = group q by 'ProductFamily';\nq = foreach q generate 'ProductFamily' as 'Product Family', count() as 'High-Propensity Accounts', avg(PropensityScore) as 'Avg Score';\nq = order q by 'High-Propensity Accounts' desc;\n",
        "broadcastFacet": True,
    }
    state["widgets"]["w_ml_whitespace_hbar"] = {
        "type": "chart",
        "parameters": {
            "visualizationType": "hbar",
            "step": "s_ml_whitespace_by_family",
            "theme": "wave",
            "title": {
                "fontSize": 14,
                "subtitleFontSize": 11,
                "label": "Whitespace Propensity by Product Family",
                "align": "center",
                "subtitleLabel": "Count of high-propensity accounts (score ≥ 20)",
            },
        },
    }
    target_page["widgets"].append(
        {"name": "w_ml_whitespace_hbar", "row": row, "column": 0, cs_key: 6, rs_key: 6}
    )

    # Next-family recommendations table
    state["steps"]["s_ml_next_family"] = {
        "type": "saql",
        "query": "q = load \"Next_Family_Recommendations\";\nq = foreach q generate 'AccountName' as 'Account', 'RecommendedFamily' as 'Recommended Product', SimilarityScore as 'Similarity Score', SimilarAccountCount as 'Similar Accounts', 'ReasonText' as 'Reason';\nq = order q by 'Similarity Score' desc;\nq = limit q 20;\n",
        "broadcastFacet": True,
    }
    state["widgets"]["w_ml_next_family_table"] = {
        "type": "chart",
        "parameters": {
            "visualizationType": "valuesTable",
            "step": "s_ml_next_family",
            "theme": "wave",
            "title": {
                "fontSize": 14,
                "subtitleFontSize": 11,
                "label": "Next-Best Product Recommendations (ML)",
                "align": "center",
                "subtitleLabel": "Based on collaborative filtering with similar accounts",
            },
        },
    }
    target_page["widgets"].append(
        {
            "name": "w_ml_next_family_table",
            "row": row,
            "column": 6,
            cs_key: 6,
            rs_key: 6,
        }
    )
    row += 6

    status = patch_dashboard(token, url, PPW_ID, state)
    print(f"  PATCH status: {status}")
    print(
        "  ✅ Added 4 ML widgets (header + whitespace table + hbar + next-family table)"
    )

    # ═══════════════════════════════════════════════════════════
    # PART 2: Analytics Command Center — ML Health & Freshness
    # ═══════════════════════════════════════════════════════════
    ACC_ID = "0FKTb0000000IEfOAM"
    print("\n" + "=" * 60)
    print("PART 2: Analytics Command Center — ML Health & Freshness")
    print("=" * 60)

    dash2 = get_dashboard(token, url, ACC_ID)
    state2 = _normalize_state(dash2["state"], strip_page_labels=False)
    pages2, rs_key2, cs_key2 = get_pages(state2)

    # Add to Executive Pulse page (page 0) — after existing alerts row
    pulse_page = pages2[0]
    row2 = max_row(pulse_page, rs_key2)
    print(f"  Pulse page max row: {row2}")

    # ML Model Health KPI
    state2["steps"]["s_ml_health_summary"] = {
        "type": "saql",
        "query": 'q = load "ML_Model_Monitor";\nq = filter q by HealthStatus == "Healthy";\nq = group q by all;\nq = foreach q generate count() as \'HealthyModels\';\n',
        "broadcastFacet": True,
    }
    state2["widgets"]["w_ml_health_count"] = {
        "type": "number",
        "parameters": {
            "step": "s_ml_health_summary",
            "measureField": "HealthyModels",
            "title": "Healthy ML Models",
            "titleColor": "#54698D",
            "titleSize": 10,
            "numberColor": "#04844B",
            "numberSize": 24,
            "textAlignment": "center",
            "compact": True,
            "interactions": [],
        },
    }
    pulse_page["widgets"].append(
        {"name": "w_ml_health_count", "row": row2, "column": 0, cs_key2: 3, rs_key2: 3}
    )

    # Stale Datasets KPI
    state2["steps"]["s_stale_datasets"] = {
        "type": "saql",
        "query": 'q = load "Data_Freshness_Monitor";\nq = filter q by FreshnessStatus in ["Stale", "Critical"];\nq = group q by all;\nq = foreach q generate count() as \'StaleDatasets\';\n',
        "broadcastFacet": True,
    }
    state2["widgets"]["w_stale_datasets"] = {
        "type": "number",
        "parameters": {
            "step": "s_stale_datasets",
            "measureField": "StaleDatasets",
            "title": "Stale Datasets",
            "titleColor": "#8C4B02",
            "titleSize": 10,
            "numberColor": "#D4380D",
            "numberSize": 24,
            "textAlignment": "center",
            "compact": True,
            "interactions": [],
        },
    }
    pulse_page["widgets"].append(
        {"name": "w_stale_datasets", "row": row2, "column": 3, cs_key2: 3, rs_key2: 3}
    )

    # ML Model Monitor table
    state2["steps"]["s_ml_monitor_table"] = {
        "type": "saql",
        "query": "q = load \"ML_Model_Monitor\";\nq = foreach q generate 'ModelName' as 'Model', 'ModelType' as 'Type', 'HealthStatus' as 'Status', RowCount as 'Rows', AvgScore as 'Avg Score', HoursSinceUpdate as 'Hours Since Update';\nq = order q by 'Model' asc;\n",
        "broadcastFacet": True,
    }
    state2["widgets"]["w_ml_monitor_table"] = {
        "type": "chart",
        "parameters": {
            "visualizationType": "valuesTable",
            "step": "s_ml_monitor_table",
            "theme": "wave",
            "title": {
                "fontSize": 14,
                "subtitleFontSize": 11,
                "label": "ML Model Health Monitor",
                "align": "center",
                "subtitleLabel": "Real-time health status of all ML scoring models",
            },
        },
    }
    pulse_page["widgets"].append(
        {"name": "w_ml_monitor_table", "row": row2, "column": 6, cs_key2: 6, rs_key2: 6}
    )

    # Data Freshness stale datasets table
    state2["steps"]["s_freshness_stale_table"] = {
        "type": "saql",
        "query": "q = load \"Data_Freshness_Monitor\";\nq = filter q by FreshnessStatus in [\"Stale\", \"Critical\"];\nq = foreach q generate 'DatasetName' as 'Dataset', 'Category' as 'Category', DaysSinceUpdate as 'Days Stale', 'FreshnessStatus' as 'Status', 'IsWithinSLA' as 'SLA Met';\nq = order q by 'Days Stale' desc;\n",
        "broadcastFacet": True,
    }
    state2["widgets"]["w_freshness_stale_table"] = {
        "type": "chart",
        "parameters": {
            "visualizationType": "valuesTable",
            "step": "s_freshness_stale_table",
            "theme": "wave",
            "title": {
                "fontSize": 14,
                "subtitleFontSize": 11,
                "label": "Stale Datasets Alert",
                "align": "center",
                "subtitleLabel": "Datasets overdue for refresh",
            },
        },
    }
    pulse_page["widgets"].append(
        {
            "name": "w_freshness_stale_table",
            "row": row2 + 3,
            "column": 0,
            cs_key2: 6,
            rs_key2: 3,
        }
    )

    status2 = patch_dashboard(token, url, ACC_ID, state2)
    print(f"  PATCH status: {status2}")
    print(
        "  ✅ Added 4 monitoring widgets to Executive Pulse (ML Health KPI + Stale Datasets KPI + ML table + Freshness table)"
    )

    print("\n" + "=" * 60)
    print("DONE — All 4 orphaned ML datasets now surfaced!")
    print("=" * 60)


if __name__ == "__main__":
    main()
