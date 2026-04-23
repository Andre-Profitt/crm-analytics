#!/usr/bin/env python3
"""Add conditional formatting rules (formatRules) to compare table widgets.

Applies color-coded thresholds based on field name patterns:
  - Risk scores (0-100): Red ≥70, Orange ≥40, Green <40
  - Win/Health/Confidence scores: Green ≥70, Orange ≥40, Red <40
  - Days (aging, dwell): Red ≥90, Orange ≥30
  - Push/backward counts: Red ≥5, Orange ≥2
  - Propensity/conversion scores: Green ≥70, Orange ≥40

Also cleans the Exec Revenue p2_tbl_risk test rule from earlier.
"""

import json
from pathlib import Path
import re
import subprocess
import sys
import urllib.request

# ── Shared utilities (same as add_number_formatting.py) ───────────────

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


# ── Format rule builders ─────────────────────────────────────────────

# Colors
RED = "#D4504C"
ORANGE = "#FFB75D"
GREEN = "#04844B"
BLUE = "#0176D3"


def risk_rule(field):
    """Higher is worse: Red ≥70, Orange ≥40."""
    return {
        "type": "threshold",
        "field": field,
        "rules": [
            {"value": 70, "color": RED, "operator": "gte"},
            {"value": 40, "color": ORANGE, "operator": "gte"},
        ],
    }


def positive_rule(field):
    """Higher is better: Green ≥70, Orange ≥40, Red <40."""
    return {
        "type": "threshold",
        "field": field,
        "rules": [
            {"value": 70, "color": GREEN, "operator": "gte"},
            {"value": 40, "color": ORANGE, "operator": "gte"},
            {"value": 0, "color": RED, "operator": "gte"},
        ],
    }


def days_rule(field):
    """Aging: Red ≥90d, Orange ≥30d."""
    return {
        "type": "threshold",
        "field": field,
        "rules": [
            {"value": 90, "color": RED, "operator": "gte"},
            {"value": 30, "color": ORANGE, "operator": "gte"},
        ],
    }


def count_rule(field):
    """Push/backward counts: Red ≥5, Orange ≥2."""
    return {
        "type": "threshold",
        "field": field,
        "rules": [
            {"value": 5, "color": RED, "operator": "gte"},
            {"value": 2, "color": ORANGE, "operator": "gte"},
        ],
    }


def rank_rule(field):
    """Rank (1 = worst): Red 1-3, Orange 4-7."""
    return {
        "type": "threshold",
        "field": field,
        "rules": [
            {"value": 1, "color": RED, "operator": "gte"},
        ],
    }


# ── Field → rule mapping ────────────────────────────────────────────

# Risk scores (higher = worse)
RISK_FIELDS = re.compile(
    r"(risk.?score|slip.?risk|total.?risk|renewal.?risk.?score|"
    r"response.?risk|cycle.?risk|operating.?gap|churn.?risk)",
    re.IGNORECASE,
)

# Positive scores (higher = better)
POSITIVE_FIELDS = re.compile(
    r"(win.?score|health.?score|confidence|propensity|"
    r"conversion.?propensity|expected.?conversion|"
    r"lead.?score|nrr|expansion.?score|quality.?score)",
    re.IGNORECASE,
)

# Day/time fields
DAY_FIELDS = re.compile(
    r"(days.?in.?stage|days.?since|days.?to|age.?days|"
    r"lead.?age|contract.?age|dwell|cycle.?days)",
    re.IGNORECASE,
)

# Count fields (push/backward)
COUNT_FIELDS = re.compile(
    r"(push.?count|backward.?move|missing.?approval)",
    re.IGNORECASE,
)


def build_rules_for_fields(field_aliases):
    """Given a list of SAQL field aliases, return appropriate formatRules."""
    rules = []
    for field in field_aliases:
        if RISK_FIELDS.search(field):
            rules.append(risk_rule(field))
        elif POSITIVE_FIELDS.search(field):
            rules.append(positive_rule(field))
        elif DAY_FIELDS.search(field):
            rules.append(days_rule(field))
        elif COUNT_FIELDS.search(field):
            rules.append(count_rule(field))
    return rules


# ── All active Gen 2 dashboards ──────────────────────────────────────

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

COMPARE_TABLE_TYPES = {
    "comparisontable",
    "comparetable",
    "comparisonTable",
    "compareTable",
    "valuesTable",
    "valuestable",
}


def process_dashboard(instance, token, label, dashboard_id):
    """Add formatRules to compare table widgets based on field name patterns."""
    data = get_dashboard(instance, token, dashboard_id)
    state = _normalize_state(data.get("state", {}))

    widgets = state.get("widgets", {})
    steps = state.get("steps", {})
    tables_modified = 0
    changes = []

    for wname, widget in sorted(widgets.items()):
        params = widget.get("parameters", {})
        viz = params.get("visualizationType", "")

        if viz not in COMPARE_TABLE_TYPES:
            continue

        # Get step's SAQL to find field aliases
        step_name = params.get("step", "")
        step = steps.get(step_name, {})
        saql = step.get("query", "")
        if isinstance(saql, dict):
            saql = saql.get("query", "")

        # Extract field aliases from SAQL 'as FIELD' patterns
        aliases = re.findall(r"\bas\s+(\w+)", saql)

        if not aliases:
            continue

        # Build rules for these fields
        rules = build_rules_for_fields(aliases)

        if not rules:
            continue

        # Set formatRules (overwrite any existing)
        params["formatRules"] = rules
        params["applyConditionalFormatting"] = True
        tables_modified += 1

        changes.append(
            f"  TBL {wname}: {len(rules)} rules on {[r['field'] for r in rules]}"
        )

    if tables_modified == 0:
        print(f"  {label}: no tables with formattable fields")
        return 0

    try:
        status = patch_dashboard(instance, token, dashboard_id, state)
        print(f"✓ {label}: {status} — {tables_modified} tables formatted")
        for c in changes:
            print(c)
        return tables_modified
    except Exception as e:
        print(f"✗ {label}: FAILED — {e}")
        if hasattr(e, "read"):
            err = e.read().decode("utf-8", errors="replace")[:500]
            print(f"  {err}")
        return 0


def main():
    token, instance = get_token()
    print(f"Auth OK: {instance}")
    print(f"Processing {len(DASHBOARDS)} dashboards...\n")

    total = 0
    for label, did in sorted(DASHBOARDS.items()):
        try:
            count = process_dashboard(instance, token, label, did)
            total += count
        except Exception as e:
            print(f"✗ {label}: ERROR — {e}")

    print(f"\n{'=' * 60}")
    print(f"Total tables with conditional formatting: {total}")


if __name__ == "__main__":
    main()
