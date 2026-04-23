#!/usr/bin/env python3
"""Scan all dashboards for comparison table widgets and check ID field availability.

Outputs a comprehensive report of:
1. All comparison table widgets found
2. Which steps they're bound to
3. Whether ID fields (OppId, AccountId, LeadId, etc.) are projected in the SAQL
4. Whether salesforceActions interactions already exist
"""

import html
import json
import subprocess
import urllib.request


DASHBOARD_IDS = [
    ("0FKTb0000000HqTOAU", "Executive Revenue & Forecast"),
    ("0FKTb0000000I09OAE", "Executive Pipeline Risk"),
    ("0FKTb0000000I1lOAE", "Executive Customer Risk"),
    ("0FKTb0000000IBROA2", "Executive Product Mix"),
    ("0FKTb0000000HthOAE", "Forecast & Revenue Motions"),
    ("0FKTb0000000Hs5OAE", "Pipeline & Opportunity Operations"),
    ("0FKTb0000000HvJOAU", "Customer & Account Health"),
    ("0FKTb0000000HwvOAE", "Lead Funnel"),
    ("0FKTb0000000HyXOAU", "Contract Operations & Renewals"),
    ("0FKTb0000000I8DOAU", "BDR Manager"),
    ("0FKTb0000000IGHOA2", "AE Performance"),
    ("0FKTb0000000IJVOA2", "Manager Coaching"),
    ("0FKTb0000000ITBOA2", "Revenue Retention Health"),
    ("0FKTb0000000IRZOA2", "Sales Activity & Productivity"),
    ("0FKTb0000000IUnOAM", "SaaS Transition & Delivery Model"),
]

ID_FIELDS = [
    "OppId",
    "OpportunityId",
    "AccountId",
    "LeadId",
    "ContractId",
    "CampaignId",
    "OwnerId",
    "UserId",
    "ContactId",
    "CaseId",
]


def get_token():
    result = subprocess.run(
        ["sf", "org", "display", "--json", "-o", "apro@simcorp.com"],
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)["result"]
    return data["instanceUrl"], data["accessToken"]


def fully_unescape(text):
    prev = None
    while text != prev:
        prev = text
        text = html.unescape(text)
    return text


def get_dashboard(inst, tok, dashboard_id):
    url = f"{inst}/services/data/v66.0/wave/dashboards/{dashboard_id}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def scan_dashboard(inst, tok, dashboard_id, name):
    """Scan a dashboard for comparison table widgets."""
    dash = get_dashboard(inst, tok, dashboard_id)
    state = dash["state"]
    widgets = state.get("widgets", {})
    steps = state.get("steps", {})

    tables = []

    for widget_name, widget in widgets.items():
        params = widget.get("parameters", widget)
        viz_type = params.get("visualizationType", "")

        if viz_type != "comparisontable":
            continue

        step_name = params.get("step", "")
        has_actions = any(
            i.get("type") == "salesforceActions" for i in params.get("interactions", [])
        )

        # Get step SAQL
        step = steps.get(step_name, {})
        saql = ""
        if "query" in step:
            saql = fully_unescape(step["query"])
        elif "queries" in step:
            saql = "\n".join(fully_unescape(q) for q in step["queries"])

        # Check which ID fields are in the SAQL
        found_ids = [f for f in ID_FIELDS if f in saql]

        # Extract the generate/foreach clause to see projected fields
        generate_match = ""
        for line in saql.split(";"):
            line = line.strip()
            if "generate" in line.lower() or "foreach" in line.lower():
                generate_match = line

        tables.append(
            {
                "widget_name": widget_name,
                "step_name": step_name,
                "has_actions": has_actions,
                "id_fields_found": found_ids,
                "saql_preview": saql[:200] if saql else "(no SAQL)",
                "generate_clause": generate_match[:200] if generate_match else "(none)",
            }
        )

    return tables


def main():
    inst, tok = get_token()
    print(f"Authenticated: {inst}\n")

    all_tables = []
    no_actions_count = 0
    has_actions_count = 0
    no_id_count = 0

    for dashboard_id, name in DASHBOARD_IDS:
        print(f"{'=' * 70}")
        print(f"  {name} ({dashboard_id})")
        print(f"{'=' * 70}")

        tables = scan_dashboard(inst, tok, dashboard_id, name)

        if not tables:
            print("  (no comparison tables found)")
            continue

        for t in tables:
            status = "HAS_ACTIONS" if t["has_actions"] else "NEEDS_ACTIONS"
            ids = (
                ", ".join(t["id_fields_found"])
                if t["id_fields_found"]
                else "NO_ID_FIELDS"
            )

            if t["has_actions"]:
                has_actions_count += 1
            else:
                no_actions_count += 1
                if not t["id_fields_found"]:
                    no_id_count += 1

            print(f"  [{status}] {t['widget_name']}")
            print(f"    Step: {t['step_name']}")
            print(f"    IDs:  {ids}")
            if not t["has_actions"] and not t["id_fields_found"]:
                print(f"    SAQL: {t['saql_preview']}")
            print()

        all_tables.extend([(name, t) for t in tables])

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total comparison tables: {len(all_tables)}")
    print(f"  Already have actions:  {has_actions_count}")
    print(f"  Need actions (has ID): {no_actions_count - no_id_count}")
    print(f"  Need actions (NO ID):  {no_id_count}")

    # Tables that need actions and have ID fields
    print("\n--- READY FOR RECORD ACTIONS ---")
    for name, t in all_tables:
        if not t["has_actions"] and t["id_fields_found"]:
            print(
                f"  {name} → {t['widget_name']} → IDs: {', '.join(t['id_fields_found'])}"
            )

    # Tables that need actions but have NO ID fields
    print("\n--- NEED SAQL UPDATE (no ID field) ---")
    for name, t in all_tables:
        if not t["has_actions"] and not t["id_fields_found"]:
            print(f"  {name} → {t['widget_name']} (step: {t['step_name']})")
            print(f"    SAQL: {t['saql_preview']}")


if __name__ == "__main__":
    main()
