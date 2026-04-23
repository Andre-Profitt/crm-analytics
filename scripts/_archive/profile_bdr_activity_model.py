#!/usr/bin/env python3
"""Profile how SimCorp BDR work is captured across Leads, Contacts, Accounts, and Opportunities."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm_analytics_helpers import _soql, get_auth  # noqa: E402

START_DATE = "2025-01-01T00:00:00Z"
DEFAULT_OUT_DIR = Path("/Users/test/crm-analytics/docs/generated/bdr_activity_model_2026-03-12")


def _make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def _make_artifact(kind: str, path: Path) -> dict[str, str]:
    return {"kind": kind, "path": str(path)}


def _make_result(
    *,
    status: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "profile_bdr_activity_model",
        "lane": "salesforce_data_profiles",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def _dt(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _iso_day(value: object) -> str:
    dt = _dt(value)
    return dt.date().isoformat() if dt else ""


def _hours_between(start: object, end: object) -> float | None:
    start_dt = _dt(start)
    end_dt = _dt(end)
    if not start_dt or not end_dt:
        return None
    return (end_dt - start_dt).total_seconds() / 3600.0


def _quoted(values: list[str]) -> str:
    return ",".join(f"'{value}'" for value in values)


def fetch_live_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    inst, tok = get_auth()

    users = _soql(
        inst,
        tok,
        "SELECT Id, Name, Title, Department, UserRole.Name, Manager.Name "
        "FROM User "
        "WHERE IsActive = true "
        "AND Title LIKE '%Business Development%' "
        "AND (Department LIKE '%NA Sales%' OR Department LIKE '%North America%') "
        "ORDER BY Name",
    )
    user_ids = [row["Id"] for row in users if row.get("Id")]
    if not user_ids:
        return users, [], [], []

    leads = _soql(
        inst,
        tok,
        "SELECT Id, Name, Company, Status, CreatedDate, OwnerId, Owner.Name, CreatedById, CreatedBy.Name, "
        "IsConverted, ConvertedDate, ConvertedContactId, ConvertedAccountId, ConvertedOpportunityId, "
        "LeadSource, Dimension_Persona__c, Industry "
        "FROM Lead "
        f"WHERE CreatedDate >= {START_DATE} "
        f"AND (OwnerId IN ({_quoted(user_ids)}) OR CreatedById IN ({_quoted(user_ids)}))",
    )
    tasks = _soql(
        inst,
        tok,
        "SELECT Id, Subject, Type, Status, CreatedDate, ActivityDate, OwnerId, Owner.Name, WhoId, WhatId "
        "FROM Task "
        f"WHERE CreatedDate >= {START_DATE} AND OwnerId IN ({_quoted(user_ids)})",
    )
    events = _soql(
        inst,
        tok,
        "SELECT Id, Subject, CreatedDate, ActivityDate, OwnerId, Owner.Name, WhoId, WhatId "
        "FROM Event "
        f"WHERE CreatedDate >= {START_DATE} AND OwnerId IN ({_quoted(user_ids)})",
    )
    return users, leads, tasks, events


def build_profile(
    users: list[dict[str, Any]],
    leads: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    lead_to_contact: dict[str, str] = {
        row["Id"]: row["ConvertedContactId"] for row in leads if row.get("ConvertedContactId")
    }
    lead_to_account: dict[str, str] = {
        row["Id"]: row["ConvertedAccountId"] for row in leads if row.get("ConvertedAccountId")
    }
    lead_to_opp: dict[str, str] = {
        row["Id"]: row["ConvertedOpportunityId"] for row in leads if row.get("ConvertedOpportunityId")
    }

    direct_activities_by_lead: dict[str, list[dict[str, Any]]] = defaultdict(list)
    via_contact_by_lead: dict[str, list[dict[str, Any]]] = defaultdict(list)
    via_account_by_lead: dict[str, list[dict[str, Any]]] = defaultdict(list)
    via_opp_by_lead: dict[str, list[dict[str, Any]]] = defaultdict(list)
    owner_link_mix = Counter()

    contact_to_leads: dict[str, list[str]] = defaultdict(list)
    account_to_leads: dict[str, list[str]] = defaultdict(list)
    opp_to_leads: dict[str, list[str]] = defaultdict(list)
    for lead_id, contact_id in lead_to_contact.items():
        contact_to_leads[contact_id].append(lead_id)
    for lead_id, account_id in lead_to_account.items():
        account_to_leads[account_id].append(lead_id)
    for lead_id, opp_id in lead_to_opp.items():
        opp_to_leads[opp_id].append(lead_id)

    def classify_activity(row: dict[str, Any], activity_kind: str) -> None:
        who_id = str(row.get("WhoId") or "")
        what_id = str(row.get("WhatId") or "")
        if who_id.startswith("00Q"):
            owner_link_mix[f"{activity_kind}:Lead"] += 1
            direct_activities_by_lead[who_id].append(row)
            return
        if who_id.startswith("003"):
            owner_link_mix[f"{activity_kind}:Contact"] += 1
            for lead_id in contact_to_leads.get(who_id, []):
                via_contact_by_lead[lead_id].append(row)
        elif what_id.startswith("001"):
            owner_link_mix[f"{activity_kind}:Account"] += 1
            for lead_id in account_to_leads.get(what_id, []):
                via_account_by_lead[lead_id].append(row)
        elif what_id.startswith("006"):
            owner_link_mix[f"{activity_kind}:Opportunity"] += 1
            for lead_id in opp_to_leads.get(what_id, []):
                via_opp_by_lead[lead_id].append(row)
        else:
            owner_link_mix[f"{activity_kind}:Other"] += 1

    for row in tasks:
        classify_activity(row, "Task")
    for row in events:
        classify_activity(row, "Event")

    lead_rows: list[dict[str, Any]] = []
    summary_counts = Counter()
    first_touch_path_counts = Counter()
    for lead in leads:
        lead_id = lead["Id"]
        created_date = lead.get("CreatedDate")
        created_dt = _dt(created_date)

        def _post_lead(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            if not created_dt:
                return rows
            filtered: list[dict[str, Any]] = []
            for row in rows:
                row_dt = _dt(row.get("CreatedDate"))
                if row_dt and row_dt >= created_dt:
                    filtered.append(row)
            return filtered

        direct_rows = sorted(
            _post_lead(direct_activities_by_lead.get(lead_id, [])),
            key=lambda r: str(r.get("CreatedDate") or ""),
        )
        contact_rows = sorted(
            _post_lead(via_contact_by_lead.get(lead_id, [])),
            key=lambda r: str(r.get("CreatedDate") or ""),
        )
        account_rows = sorted(
            _post_lead(via_account_by_lead.get(lead_id, [])),
            key=lambda r: str(r.get("CreatedDate") or ""),
        )
        opp_rows = sorted(
            _post_lead(via_opp_by_lead.get(lead_id, [])),
            key=lambda r: str(r.get("CreatedDate") or ""),
        )

        earliest_candidates: list[tuple[str, dict[str, Any]]] = []
        if direct_rows:
            earliest_candidates.append(("Lead", direct_rows[0]))
        if contact_rows:
            earliest_candidates.append(("Converted Contact", contact_rows[0]))
        if account_rows:
            earliest_candidates.append(("Converted Account", account_rows[0]))
        if opp_rows:
            earliest_candidates.append(("Converted Opportunity", opp_rows[0]))
        earliest_candidates.sort(key=lambda item: str(item[1].get("CreatedDate") or ""))

        first_path = earliest_candidates[0][0] if earliest_candidates else "No Activity"
        first_activity = earliest_candidates[0][1] if earliest_candidates else {}
        first_hours = _hours_between(created_date, first_activity.get("CreatedDate")) if first_activity else None
        first_touch_24h = first_hours is not None and first_hours <= 24
        direct_hours = _hours_between(created_date, direct_rows[0].get("CreatedDate")) if direct_rows else None
        direct_touch_24h = direct_hours is not None and direct_hours <= 24

        summary_counts["lead_count"] += 1
        summary_counts["converted_count"] += 1 if lead.get("IsConverted") else 0
        summary_counts["direct_touch_24h_count"] += 1 if direct_touch_24h else 0
        summary_counts["associated_touch_24h_count"] += 1 if first_touch_24h else 0
        summary_counts["direct_activity_leads"] += 1 if direct_rows else 0
        summary_counts["contact_activity_leads"] += 1 if contact_rows else 0
        summary_counts["account_activity_leads"] += 1 if account_rows else 0
        summary_counts["opp_activity_leads"] += 1 if opp_rows else 0
        first_touch_path_counts[first_path] += 1

        lead_rows.append(
            {
                "LeadId": lead_id,
                "LeadName": lead.get("Name") or "",
                "Company": lead.get("Company") or "",
                "Status": lead.get("Status") or "",
                "CreatedDate": _iso_day(created_date),
                "OwnerName": ((lead.get("Owner") or {}).get("Name")) or "",
                "CreatedByName": ((lead.get("CreatedBy") or {}).get("Name")) or "",
                "IsConverted": bool(lead.get("IsConverted")),
                "ConvertedDate": _iso_day(lead.get("ConvertedDate")),
                "LeadSource": lead.get("LeadSource") or "",
                "Persona": lead.get("Dimension_Persona__c") or "",
                "Industry": lead.get("Industry") or "",
                "FirstTouchPath": first_path,
                "FirstTouchCreatedDate": _iso_day(first_activity.get("CreatedDate")),
                "FirstTouchHours": round(first_hours, 1) if first_hours is not None else None,
                "DirectLeadTouchHours": round(direct_hours, 1) if direct_hours is not None else None,
                "DirectLeadTouch24h": direct_touch_24h,
                "AssociatedTouch24h": first_touch_24h,
                "DirectActivityCount": len(direct_rows),
                "ConvertedContactActivityCount": len(contact_rows),
                "ConvertedAccountActivityCount": len(account_rows),
                "ConvertedOppActivityCount": len(opp_rows),
            }
        )

    lead_rows.sort(key=lambda row: (row["FirstTouchPath"] != "No Activity", row["FirstTouchHours"] or 999999))
    status_path = Counter((row["Status"], row["FirstTouchPath"]) for row in lead_rows)

    return {
        "team": [
            {
                "Id": row.get("Id"),
                "Name": row.get("Name"),
                "Title": row.get("Title"),
                "Department": row.get("Department"),
                "Role": ((row.get("UserRole") or {}).get("Name")) or "",
                "Manager": ((row.get("Manager") or {}).get("Name")) or "",
            }
            for row in users
        ],
        "summary": dict(summary_counts),
        "first_touch_path": dict(first_touch_path_counts),
        "owner_link_mix": dict(owner_link_mix),
        "status_path": {f"{status} | {path}": count for (status, path), count in status_path.items()},
        "lead_rows": lead_rows,
    }


def write_markdown(payload: dict[str, Any], output_path: Path) -> None:
    team = payload["team"]
    summary_counts = payload["summary"]
    owner_link_mix = Counter(payload["owner_link_mix"])
    first_touch_path_counts = Counter(payload["first_touch_path"])
    status_path_items = [
        tuple(key.split(" | ", 1)) + (count,)
        for key, count in payload["status_path"].items()
    ]
    lead_rows = payload["lead_rows"]

    md: list[str] = []
    md.append("# BDR Activity Model Audit")
    md.append("")
    md.append("This profile checks how North America BDR work is actually captured across Leads, Contacts, Accounts, and Opportunities.")
    md.append("")
    md.append("## Team In Scope")
    md.append("")
    md.append("| BDR | Title | Department | Role | Manager |")
    md.append("| --- | --- | --- | --- | --- |")
    for row in team:
        md.append(
            f"| {row.get('Name','')} | {row.get('Title','')} | {row.get('Department','')} | {row.get('Role','')} | {row.get('Manager','')} |"
        )
    md.append("")
    md.append("## Summary")
    md.append("")
    md.append(f"- Leads in scope: `{summary_counts['lead_count']}`")
    md.append(f"- Converted leads: `{summary_counts['converted_count']}`")
    md.append(f"- Leads with direct lead-linked touch inside 24h: `{summary_counts['direct_touch_24h_count']}`")
    md.append(f"- Leads with any associated-object touch inside 24h: `{summary_counts['associated_touch_24h_count']}`")
    md.append(f"- Leads with any direct lead-linked activity: `{summary_counts['direct_activity_leads']}`")
    md.append(f"- Leads with converted-contact activity: `{summary_counts['contact_activity_leads']}`")
    md.append(f"- Leads with converted-account activity: `{summary_counts['account_activity_leads']}`")
    md.append(f"- Leads with converted-opportunity activity: `{summary_counts['opp_activity_leads']}`")
    md.append("")
    md.append("## Activity Link Mix")
    md.append("")
    md.append("| Link Type | Count |")
    md.append("| --- | --- |")
    for key, count in owner_link_mix.most_common():
        md.append(f"| {key} | {count} |")
    md.append("")
    md.append("## First Touch Path By Lead")
    md.append("")
    md.append("| First Touch Path | Leads |")
    md.append("| --- | --- |")
    for key, count in first_touch_path_counts.most_common():
        md.append(f"| {key} | {count} |")
    md.append("")
    md.append("## Status x First Touch Path")
    md.append("")
    md.append("| Status | First Touch Path | Leads |")
    md.append("| --- | --- | --- |")
    for status, path, count in sorted(status_path_items, key=lambda item: (-item[2], item[0], item[1])):
        md.append(f"| {status} | {path} | {count} |")
    md.append("")
    md.append("## Sample Leads Where First Response Was Not On The Lead")
    md.append("")
    md.append("| Owner | Company | Status | Created | First Touch Path | First Touch Hours | Direct Lead Touch Hours |")
    md.append("| --- | --- | --- | --- | --- | --- | --- |")
    shown = 0
    for row in lead_rows:
        if row["FirstTouchPath"] in {"No Activity", "Lead"}:
            continue
        md.append(
            f"| {row['OwnerName']} | {row['Company']} | {row['Status']} | {row['CreatedDate']} | {row['FirstTouchPath']} | "
            f"{'-' if row['FirstTouchHours'] is None else row['FirstTouchHours']} | "
            f"{'-' if row['DirectLeadTouchHours'] is None else row['DirectLeadTouchHours']} |"
        )
        shown += 1
        if shown >= 12:
            break
    md.append("")
    md.append("## Recommended Model")
    md.append("")
    md.append("1. Keep `Lead 24h response SLA` strict. It should only count direct Lead-linked first touch before conversion.")
    md.append("2. Add a separate `Associated Prospect Response` metric that accepts Contact/Account/Opportunity activity on the converted record chain.")
    md.append("3. Split BDR operating metrics into two families:")
    md.append("   - lead handling and inbound response")
    md.append("   - contact/account nurture and meeting creation after conversion")
    md.append("4. Do not punish BDRs for marketing-disqualified leads in workload queues, but keep those leads visible in source and campaign quality.")
    md.append("5. For launch, show both `direct lead-linked` and `associated-object` response so managers can see whether the process or the rep behavior is the real problem.")
    md.append("")
    output_path.write_text("\n".join(md), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile how BDR work is captured across core CRM objects.")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="Directory for profile.md and profile.json",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    return parser


def run_profile_command(out_dir: Path, *, emit_text: bool = True) -> tuple[dict[str, Any], int]:
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    users, leads, tasks, events = fetch_live_rows()
    profile = build_profile(users, leads, tasks, events)

    json_path = out_dir / "profile.json"
    md_path = out_dir / "profile.md"
    json_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    write_markdown(profile, md_path)

    if emit_text:
        print(f"Wrote BDR activity model profile to {out_dir}")

    summary = profile["summary"]
    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote BDR activity model profile to {out_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "bdr_count": len(profile["team"]),
            "lead_count": summary["lead_count"],
            "converted_count": summary["converted_count"],
            "direct_touch_24h_count": summary["direct_touch_24h_count"],
            "associated_touch_24h_count": summary["associated_touch_24h_count"],
            "lead_row_count": len(profile["lead_rows"]),
            "output_dir": str(out_dir),
        },
        profile=profile,
    )
    return result, 0


def main() -> int:
    args = build_parser().parse_args()
    result, exit_code = run_profile_command(args.out_dir, emit_text=not args.json)
    if args.json:
        print(json.dumps(result, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
