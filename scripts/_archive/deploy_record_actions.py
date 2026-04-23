#!/usr/bin/env python3
"""Deploy Salesforce record actions to ALL comparison table widgets.

Three-phase approach per table:
1. If ID field already in SAQL (detail or aggregate) → add interaction only
2. If detail table (no group by) without ID → add ID to generate + add interaction
3. If aggregate table without ID field → skip (no individual record IDs)

Many exception tables use group by (Name, ..., Id) — they ARE aggregate but DO
project the record ID, so they qualify for record actions (phase 1).

Uses GET → unescape → modify → clean → PATCH pattern.
"""

import argparse
import json
from pathlib import Path
import re
import subprocess
import time
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm_analytics_helpers import normalize_dashboard_state_for_patch  # noqa: E402


def _make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def _make_result(
    *,
    status: str,
    messages: list[dict[str, str]],
    **extra,
):
    payload = {
        "status": status,
        "tool": "deploy_record_actions",
        "lane": "dashboard_mutations",
        "command_class": "mutating",
        "messages": messages,
        "artifacts": [],
    }
    payload.update(extra)
    return payload


def get_token():
    """Get Salesforce access token and instance URL."""
    result = subprocess.run(
        ["sf", "org", "display", "--json", "-o", "apro@simcorp.com"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "sf org display failed")
    data = json.loads(result.stdout)["result"]
    return data["instanceUrl"], data["accessToken"]

def get_dashboard(inst, tok, dashboard_id):
    """GET a dashboard and return parsed JSON."""
    url = f"{inst}/services/data/v66.0/wave/dashboards/{dashboard_id}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def patch_dashboard(inst, tok, dashboard_id, state):
    """PATCH a dashboard with the given state."""
    url = f"{inst}/services/data/v66.0/wave/dashboards/{dashboard_id}"
    body = json.dumps({"state": state}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return resp.status


def _log(text: str, *, emit_text: bool) -> None:
    if emit_text:
        print(text)


def build_record_action(object_name, id_field):
    """Build salesforceActions interaction for View Record + Create Task."""
    return {
        "type": "salesforceActions",
        "group": "main",
        "columns": [],
        "enabled": True,
        "options": {
            "cell": {
                "enabled": True,
                "actions": [
                    {
                        "enabled": True,
                        "type": "recordAction",
                        "label": "View Record",
                        "actionName": "record",
                        "objectApiName": object_name,
                        "recordIdColumn": id_field,
                    },
                    {
                        "enabled": True,
                        "type": "salesforceAction",
                        "label": "Create Task",
                        "actionName": "NewTask",
                        "objectApiName": "Task",
                        "recordIdColumn": id_field,
                    },
                ],
            },
        },
    }


def has_salesforce_actions(widget):
    """Check if a widget already has salesforceActions interaction."""
    params = widget.get("parameters", widget)
    interactions = params.get("interactions", [])
    return any(i.get("type") == "salesforceActions" for i in interactions)


def get_step_saql(state, step_name):
    """Get the SAQL query text for a step."""
    step = state.get("steps", {}).get(step_name, {})
    if "query" in step:
        return step["query"]
    if "queries" in step:
        return "\n".join(step["queries"])
    return ""


def set_step_saql(state, step_name, new_saql):
    """Set the SAQL query text for a step."""
    step = state.get("steps", {}).get(step_name, {})
    if "query" in step:
        step["query"] = new_saql
    elif "queries" in step:
        # For multi-query steps, set the first query
        step["queries"][0] = new_saql


def is_aggregate_saql(saql):
    """Check if SAQL uses group by (aggregate query)."""
    return bool(re.search(r"\bgroup\s+\w+\s+by\b", saql, re.IGNORECASE))


def add_id_to_generate(saql, id_field):
    """Add ID field to the generate clause of a detail SAQL query.

    Handles both single-line and multi-line generate clauses:
      foreach q generate OppName, ...
      foreach q generate
          OppName, ...

    Returns (modified_saql, success).
    """
    # Pattern: "foreach <var> generate" followed by whitespace then fields
    # We insert the ID field right after "generate" + whitespace
    pattern = r"(foreach\s+\w+\s+generate)([\s]+)"
    match = re.search(pattern, saql, re.IGNORECASE)
    if match:
        insert_pos = match.end()
        # Preserve the whitespace pattern (newline + indent vs space)
        ws = match.group(2)
        if "\n" in ws:
            # Multi-line: "generate\n    OppName" → "generate\n    Id, OppName"
            modified = (
                saql[: match.end(1)] + ws + id_field + "," + ws + saql[insert_pos:]
            )
        else:
            # Single-line: "generate OppName" → "generate Id, OppName"
            modified = saql[: match.end(1)] + ws + id_field + ", " + saql[insert_pos:]
        return modified, True

    return saql, False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dashboard",
        action="append",
        default=[],
        help="Optional exact dashboard name or dashboard id filter. Can be passed multiple times.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and compute the planned changes without PATCHing live dashboards.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.5,
        help="Pause between dashboards to avoid hammering the API.",
    )
    return parser


def select_dashboard_configs(filters: list[str]) -> list[dict[str, object]]:
    if not filters:
        return DASHBOARD_CONFIGS
    wanted = {item.lower() for item in filters}
    selected = [
        config
        for config in DASHBOARD_CONFIGS
        if config["id"].lower() in wanted or config["name"].lower() in wanted
    ]
    missing = sorted(
        raw for raw in filters if raw.lower() not in {c["id"].lower() for c in selected} | {c["name"].lower() for c in selected}
    )
    if missing:
        raise ValueError(f"Unknown dashboard filter(s): {', '.join(missing)}")
    return selected


# ─── Dashboard Configuration ────────────────────────────────────────────────
# Each table entry specifies:
#   widget: widget name in dashboard state
#   object: Salesforce object API name for View Record
#   id_field: column name in SAQL result set containing the record ID
#
# Field name mapping (confirmed via SAQL queries):
#   Pipeline_Opportunity_Operations  → Id (Opportunity), AccountId (Account)
#   Forecast_Revenue_Motions         → Id (Opportunity), AccountId (Account)
#   Customer_Account_Health          → AccountId (Account)
#   Lead_Funnel                      → LeadId (Lead)
#   Contract_Operations_Renewals     → AccountId (Account) [no ContractId]
#   BDR_Operating_Rhythm             → LeadId (Lead) [no CampaignId]
#   Revenue_Retention_Health         → OppId (Opportunity), AccountId (Account)
#   Sales_Activity_Productivity      → OpportunityId (Opportunity), AccountId (Account)
#   SaaS_Transition_Delivery         → AccountId (Account, line_item only)
#   Product_Portfolio_Whitespace     → AccountId (Account)

DASHBOARD_CONFIGS = [
    {
        "id": "0FKTb0000000HqTOAU",
        "name": "Executive Revenue & Forecast",
        "tables": [
            {"widget": "p2_tbl_process", "object": "Opportunity", "id_field": "Id"},
            {"widget": "p2_tbl_risk", "object": "Opportunity", "id_field": "Id"},
        ],
    },
    {
        "id": "0FKTb0000000I09OAE",
        "name": "Executive Pipeline Risk",
        "tables": [
            {"widget": "p2_tbl_process", "object": "Opportunity", "id_field": "Id"},
            {"widget": "p2_tbl_risk", "object": "Opportunity", "id_field": "Id"},
        ],
    },
    {
        "id": "0FKTb0000000I1lOAE",
        "name": "Executive Customer Risk",
        "tables": [
            {"widget": "p2_tbl_growth", "object": "Account", "id_field": "AccountId"},
            {"widget": "p2_tbl_risk", "object": "Account", "id_field": "AccountId"},
        ],
    },
    {
        "id": "0FKTb0000000IBROA2",
        "name": "Executive Product Mix",
        "tables": [
            {"widget": "p2_tbl_accounts", "object": "Account", "id_field": "AccountId"},
            # p1_tbl_industry: group by IndustryVertical — NO Id, skip
        ],
    },
    {
        "id": "0FKTb0000000HthOAE",
        "name": "Forecast & Revenue Motions",
        "tables": [
            # p4_tbl_owner: group by OwnerName — NO Id, skip
            {"widget": "p4_tbl_forecast", "object": "Opportunity", "id_field": "Id"},
            # w_rep_ranking: group by OwnerName — NO Id, skip
            {"widget": "p4_tbl_renewals", "object": "Opportunity", "id_field": "Id"},
        ],
    },
    {
        "id": "0FKTb0000000Hs5OAE",
        "name": "Pipeline & Opportunity Operations",
        "tables": [
            {"widget": "p4_tbl_risk", "object": "Opportunity", "id_field": "Id"},
            {"widget": "p4_tbl_process", "object": "Opportunity", "id_field": "Id"},
        ],
    },
    {
        "id": "0FKTb0000000HvJOAU",
        "name": "Customer & Account Health",
        "tables": [
            {"widget": "p4_tbl_risk", "object": "Account", "id_field": "AccountId"},
            {"widget": "w_ml_table", "object": "Account", "id_field": "AccountId"},
            {"widget": "p4_tbl_gaps", "object": "Account", "id_field": "AccountId"},
        ],
    },
    {
        "id": "0FKTb0000000HwvOAE",
        "name": "Lead Funnel",
        "tables": [
            # p4_tbl_risk: group by (..., Id) — Id = Lead Id
            {"widget": "p4_tbl_risk", "object": "Lead", "id_field": "Id"},
            # p4_tbl_stalled: group by (..., Id) — Id = Lead Id
            {"widget": "p4_tbl_stalled", "object": "Lead", "id_field": "Id"},
            # w_ls_table: detail, has LeadId
            {"widget": "w_ls_table", "object": "Lead", "id_field": "LeadId"},
        ],
    },
    {
        "id": "0FKTb0000000HyXOAU",
        "name": "Contract Operations & Renewals",
        "tables": [
            # group by (ContractNumber, ..., Id) — Id is ContractId equivalent
            {"widget": "p4_tbl_backlog", "object": "Contract", "id_field": "Id"},
            {"widget": "p4_tbl_expiring", "object": "Contract", "id_field": "Id"},
        ],
    },
    {
        "id": "0FKTb0000000I8DOAU",
        "name": "BDR Manager",
        "tables": [
            # p3_tbl_campaign: group by (Campaign, SourceGroup) — NO LeadId, skip
            # p2_tbl_rep: group by (OwnerName, BDRTeam, BDRRole) — NO LeadId, skip
            {"widget": "p4_tbl_priority", "object": "Lead", "id_field": "LeadId"},
            {"widget": "p4_tbl_upcoming", "object": "Lead", "id_field": "LeadId"},
            # p4_tbl_stale: group by Company — NO LeadId, skip
        ],
    },
    {
        "id": "0FKTb0000000IGHOA2",
        "name": "AE Performance",
        "tables": [
            # All 3 tables are aggregates (group by OwnerName) — skip
            # w_pipe_table, w_leaderboard, w_win_table
        ],
    },
    {
        "id": "0FKTb0000000IJVOA2",
        "name": "Manager Coaching",
        "tables": [
            # Both tables are aggregates (group by OwnerName) — skip
            # w_scorecard, w_coaching
        ],
    },
    {
        "id": "0FKTb0000000ITBOA2",
        "name": "Revenue Retention Health",
        "tables": [
            {"widget": "p4_ch_detail", "object": "Opportunity", "id_field": "OppId"},
            {"widget": "p3_ch_table", "object": "Opportunity", "id_field": "OppId"},
        ],
    },
    {
        "id": "0FKTb0000000IRZOA2",
        "name": "Sales Activity & Productivity",
        "tables": [
            # p3_ch_opp: group by (Subject, OppStage, OwnerName, OppType) — NO Id, skip
            # p3_ch_acct: group by (AccountName, DaysAgo, OppType) — NO Id, skip
            {
                "widget": "p4_ch_no_step",
                "object": "Opportunity",
                "id_field": "OpportunityId",
            },
            # p2_ch_compare: group by OwnerName — NO Id, skip
            # p1_ch_reps: group by OwnerName — NO Id, skip
            # p4_ch_low_reps: group by OwnerName — NO Id, skip
            {"widget": "p4_ch_stale", "object": "Account", "id_field": "AccountId"},
        ],
    },
    {
        "id": "0FKTb0000000IUnOAM",
        "name": "SaaS Transition & Delivery Model",
        "tables": [
            # p2_ch_saas_pct: aggregate (group by Industry) → skip
            # p3_ch_onprem: account_year has no AccountId → skip
            # p3_ch_leaders: account_year has no AccountId → skip
        ],
    },
]


def process_dashboard(inst, tok, config, *, dry_run: bool = False, emit_text: bool = True):
    """Process a single dashboard: add record actions to comparison tables."""
    dashboard_id = config["id"]
    name = config["name"]
    tables = config["tables"]

    if not tables:
        _log(f"\n{'=' * 65}", emit_text=emit_text)
        _log(f"SKIP  {name}", emit_text=emit_text)
        _log("  (no actionable tables — all aggregate or missing IDs)", emit_text=emit_text)
        return {
            "dashboard_id": dashboard_id,
            "name": name,
            "status": "skipped",
            "actions_added": 0,
            "saql_modified": 0,
            "skipped": [],
            "patch_status": None,
            "dry_run": dry_run,
        }

    _log(f"\n{'=' * 65}", emit_text=emit_text)
    _log(f"Processing: {name} ({dashboard_id})", emit_text=emit_text)
    _log(f"  Tables: {len(tables)}", emit_text=emit_text)

    # GET dashboard
    try:
        dash = get_dashboard(inst, tok, dashboard_id)
    except Exception as e:
        _log(f"  ERROR: GET failed: {e}", emit_text=emit_text)
        return {
            "dashboard_id": dashboard_id,
            "name": name,
            "status": "error",
            "actions_added": 0,
            "saql_modified": 0,
            "skipped": [{"target": "GET", "reason": str(e)}],
            "patch_status": None,
            "dry_run": dry_run,
        }

    state = normalize_dashboard_state_for_patch(dash["state"])

    widgets = state.get("widgets", {})
    actions_added = 0
    saql_modified = 0
    skipped = []
    modified = False

    for tc in tables:
        wname = tc["widget"]
        sf_obj = tc["object"]
        id_field = tc["id_field"]

        # 1. Find the widget
        widget = widgets.get(wname)
        if not widget:
            _log(f"  WARN  {wname}: widget not found", emit_text=emit_text)
            skipped.append({"target": wname, "reason": "not found"})
            continue

        params = widget.get("parameters", widget)

        # 2. Already has actions?
        if has_salesforce_actions(widget):
            _log(f"  EXIST {wname}: already has salesforceActions", emit_text=emit_text)
            skipped.append({"target": wname, "reason": "already exists"})
            continue

        # 3. Get the bound step SAQL
        step_name = params.get("step", "")
        saql = get_step_saql(state, step_name)

        if not saql:
            _log(f"  WARN  {wname}: no SAQL in step '{step_name}'", emit_text=emit_text)
            skipped.append({"target": wname, "reason": "no SAQL"})
            continue

        is_agg = is_aggregate_saql(saql)

        # 4. Check if ID field is in the SAQL (works for both agg and detail)
        #    For bare "Id", use word-boundary check to avoid false positives
        if id_field == "Id":
            id_in_saql = bool(re.search(r"(?<![A-Za-z])Id(?![A-Za-z])", saql))
        else:
            id_in_saql = id_field in saql

        if id_in_saql:
            # ID field already projected — just add interaction
            tag = "AGG+" if is_agg else "OK  "
            _log(
                f"  {tag} {wname}: '{id_field}' found in step '{step_name}'",
                emit_text=emit_text,
            )
        elif is_agg:
            # Aggregate without the ID field — cannot add record actions
            _log(
                f"  AGG-  {wname}: aggregate, no '{id_field}' — skipping",
                emit_text=emit_text,
            )
            skipped.append({"target": wname, "reason": f"aggregate, no {id_field}"})
            continue
        else:
            # Detail table without ID field — add to generate clause
            new_saql, ok = add_id_to_generate(saql, id_field)
            if ok:
                set_step_saql(state, step_name, new_saql)
                saql_modified += 1
                _log(
                    f"  SAQL+ {wname}: added '{id_field}' to step '{step_name}'",
                    emit_text=emit_text,
                )
            else:
                _log(
                    f"  WARN  {wname}: couldn't find generate clause in step '{step_name}'",
                    emit_text=emit_text,
                )
                skipped.append({"target": wname, "reason": "no generate clause"})
                continue

        # 6. Add the salesforceActions interaction
        if "interactions" not in params:
            params["interactions"] = []

        interaction = build_record_action(sf_obj, id_field)
        params["interactions"].append(interaction)
        actions_added += 1
        modified = True
        _log(f"  ADD   {wname}: {sf_obj} via '{id_field}'", emit_text=emit_text)

    if not modified:
        _log(f"  No changes for {name}", emit_text=emit_text)
        return {
            "dashboard_id": dashboard_id,
            "name": name,
            "status": "no_changes",
            "actions_added": actions_added,
            "saql_modified": saql_modified,
            "skipped": skipped,
            "patch_status": None,
            "dry_run": dry_run,
        }

    if dry_run:
        _log(
            f"  DRYRUN planned {actions_added} actions, {saql_modified} SAQL mods",
            emit_text=emit_text,
        )
        return {
            "dashboard_id": dashboard_id,
            "name": name,
            "status": "dry_run",
            "actions_added": actions_added,
            "saql_modified": saql_modified,
            "skipped": skipped,
            "patch_status": None,
            "dry_run": True,
        }

    try:
        status = patch_dashboard(inst, tok, dashboard_id, state)
        _log(
            f"  DEPLOY HTTP {status} — {actions_added} actions, {saql_modified} SAQL mods",
            emit_text=emit_text,
        )
        return {
            "dashboard_id": dashboard_id,
            "name": name,
            "status": f"deployed ({status})",
            "actions_added": actions_added,
            "saql_modified": saql_modified,
            "skipped": skipped,
            "patch_status": status,
            "dry_run": False,
        }
    except Exception as e:
        err_detail = str(e)
        if hasattr(e, "read"):
            err_body = e.read().decode("utf-8", errors="replace")
            err_detail += f"\n    Body: {err_body[:500]}"
        _log(f"  ERROR: PATCH failed: {err_detail}", emit_text=emit_text)
        return {
            "dashboard_id": dashboard_id,
            "name": name,
            "status": "error",
            "actions_added": actions_added,
            "saql_modified": saql_modified,
            "skipped": [{"target": "PATCH", "reason": err_detail[:200]}],
            "patch_status": None,
            "dry_run": dry_run,
        }


def run_deployment(
    configs: list[dict[str, object]],
    *,
    dry_run: bool = False,
    sleep_seconds: float = 0.5,
    emit_text: bool = True,
) -> tuple[dict[str, object], int]:
    _log("=" * 65, emit_text=emit_text)
    _log("Deploy Salesforce Record Actions to Comparison Tables", emit_text=emit_text)
    _log("=" * 65, emit_text=emit_text)

    inst, tok = get_token()
    _log(f"Authenticated: {inst}", emit_text=emit_text)

    results = []
    total_actions = 0
    total_saql = 0
    total_skipped = 0
    errors = 0

    for config in configs:
        result = process_dashboard(
            inst,
            tok,
            config,
            dry_run=dry_run,
            emit_text=emit_text,
        )
        results.append(result)
        total_actions += result["actions_added"]
        total_saql += result["saql_modified"]
        total_skipped += len(result["skipped"])
        if "error" in result["status"]:
            errors += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    # ── Summary ──────────────────────────────────────────────────────────
    _log("\n" + "=" * 65, emit_text=emit_text)
    _log("DEPLOYMENT SUMMARY", emit_text=emit_text)
    _log("=" * 65, emit_text=emit_text)
    _log(f"Dashboards processed: {len(configs)}", emit_text=emit_text)
    _log(f"Record actions added: {total_actions}", emit_text=emit_text)
    _log(f"SAQL steps modified:  {total_saql}", emit_text=emit_text)
    _log(f"Tables skipped:       {total_skipped}", emit_text=emit_text)
    _log(f"Errors:               {errors}", emit_text=emit_text)

    _log("\nPer-dashboard:", emit_text=emit_text)
    for r in results:
        if r["actions_added"] > 0:
            icon = "✅"
        elif r["status"] == "skipped":
            icon = "⏭️ "
        elif "error" in r["status"]:
            icon = "❌"
        else:
            icon = "➖"

        saql_note = f", {r['saql_modified']} SAQL mods" if r["saql_modified"] else ""
        _log(
            f"  {icon} {r['name']}: {r['actions_added']} actions{saql_note} [{r['status']}]",
            emit_text=emit_text,
        )

        for skipped_entry in r["skipped"]:
            _log(
                f"      ↳ {skipped_entry['target']}: {skipped_entry['reason']}",
                emit_text=emit_text,
            )

    # Aggregate tables intentionally skipped (no record IDs in their query)
    _log("\n--- Tables skipped (aggregate with no record ID) ---", emit_text=emit_text)
    aggregate_skips = [
        "AE Performance: w_pipe_table, w_leaderboard, w_win_table (group by OwnerName)",
        "Manager Coaching: w_scorecard, w_coaching (group by OwnerName)",
        "Sales Activity: p3_ch_opp, p3_ch_acct, p2_ch_compare, p1_ch_reps, p4_ch_low_reps",
        "SaaS Transition: p2_ch_saas_pct (Industry), p3_ch_onprem/leaders (no AccountId)",
        "BDR Manager: p3_tbl_campaign (no CampaignId), p2_tbl_rep (OwnerName), p4_tbl_stale (Company)",
        "Forecast: p4_tbl_owner (OwnerName), w_rep_ranking (OwnerName)",
        "Exec Product Mix: p1_tbl_industry (IndustryVertical)",
    ]
    for skip in aggregate_skips:
        _log(f"  • {skip}", emit_text=emit_text)

    status = "error" if errors else "ok"
    messages = []
    if dry_run:
        messages.append(
            _make_message(
                "info",
                "dry_run",
                "Computed planned record-action changes without PATCHing live dashboards.",
            )
        )
    if errors:
        messages.append(
            _make_message(
                "error",
                "deployment_errors",
                f"Encountered {errors} dashboard error(s) during record-action deployment.",
            )
        )
    else:
        messages.append(
            _make_message(
                "info",
                "deployment_complete",
                f"Processed {len(configs)} dashboard(s) with {total_actions} action changes.",
            )
        )
    payload = _make_result(
        status=status,
        messages=messages,
        summary={
            "dashboards_processed": len(configs),
            "record_actions_added": total_actions,
            "saql_steps_modified": total_saql,
            "tables_skipped": total_skipped,
            "errors": errors,
            "dry_run": dry_run,
        },
        dashboards=results,
    )
    return payload, (1 if errors else 0)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        configs = select_dashboard_configs(args.dashboard)
    except ValueError as exc:
        payload = _make_result(
            status="error",
            messages=[_make_message("error", "invalid_dashboard_filter", str(exc))],
            summary={
                "dashboards_processed": 0,
                "record_actions_added": 0,
                "saql_steps_modified": 0,
                "tables_skipped": 0,
                "errors": 1,
                "dry_run": args.dry_run,
            },
            dashboards=[],
        )
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print(str(exc), file=sys.stderr)
        return 1

    payload, exit_code = run_deployment(
        configs,
        dry_run=args.dry_run,
        sleep_seconds=args.sleep_seconds,
        emit_text=not args.json,
    )
    if args.json:
        print(json.dumps(payload, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
