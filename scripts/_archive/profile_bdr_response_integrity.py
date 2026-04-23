#!/usr/bin/env python3
"""Profile BDR response-time integrity for the North America team.

The key question is whether a true 24-hour response SLA can be measured from
current Salesforce behavior, or whether activity logging is split across Lead,
Contact, Account, and other objects in a way that hides real work.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


ROOT = Path("/Users/test/crm-analytics")
DEFAULT_OUT_DIR = ROOT / "docs" / "generated" / f"bdr_response_integrity_{date.today().isoformat()}"
START = "2025-01-01T00:00:00Z"


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
        "tool": "profile_bdr_response_integrity",
        "lane": "salesforce_data_profiles",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def run_sf(query: str) -> list[dict]:
    cmd = ["sf", "data", "query", "--query", query, "--json"]
    raw = subprocess.check_output(cmd, text=True)
    return json.loads(raw)["result"]["records"]


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def md_table(headers: list[str], rows: list[list[object]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(lines)


def prefix(value: object) -> str:
    text = str(value or "")
    return text[:3] if len(text) >= 3 else ""


def obj_label_from_prefix(pref: str) -> str:
    return {
        "00Q": "Lead",
        "003": "Contact",
        "001": "Account",
        "006": "Opportunity",
        "00U": "Event",
        "00T": "Task",
    }.get(pref, pref or "None")


def parse_dt(value: object) -> datetime | None:
    text = str(value or "")
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


@dataclass(frozen=True)
class BdrUser:
    id: str
    name: str
    manager: str


def get_na_bdr_users() -> list[BdrUser]:
    rows = run_sf(
        "SELECT Id, Name, Manager.Name "
        "FROM User "
        "WHERE IsActive = true "
        "AND ("
        "Title LIKE '%Business Development Representative%' OR "
        "Title LIKE '%Lead Business Development Representative%' OR "
        "Title LIKE '%Senior Business Development Representative%') "
        "AND (Department LIKE '%NA%' OR Department LIKE '%North America%' OR Department LIKE '%US (%') "
        "ORDER BY Name"
    )
    return [BdrUser(id=row["Id"], name=row["Name"], manager=((row.get("Manager") or {}).get("Name") or "")) for row in rows]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile BDR response-time integrity for the NA team.")
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
    users = get_na_bdr_users()
    owner_ids = ",".join(f"'{user.id}'" for user in users)
    owner_map = {user.id: user.name for user in users}

    leads = run_sf(
        "SELECT Id, OwnerId, Owner.Name, CreatedDate, ConvertedDate, ConvertedContactId, ConvertedAccountId, "
        "Status, Company, LeadSource, Dimension_Persona__c "
        f"FROM Lead WHERE OwnerId IN ({owner_ids}) AND CreatedDate >= {START}"
    )
    tasks = run_sf(
        "SELECT Id, OwnerId, Owner.Name, CreatedDate, ActivityDate, Status, Type, Subject, WhoId, WhatId "
        f"FROM Task WHERE OwnerId IN ({owner_ids}) AND CreatedDate >= {START}"
    )
    events = run_sf(
        "SELECT Id, OwnerId, Owner.Name, CreatedDate, ActivityDate, Subject, WhoId, WhatId "
        f"FROM Event WHERE OwnerId IN ({owner_ids}) AND CreatedDate >= {START}"
    )

    by_owner = {
        user.name: {
            "lead_count": 0,
            "lead_touch_24h": 0,
            "lead_touch_any": 0,
            "task_total": 0,
            "event_total": 0,
            "who_prefix": Counter(),
            "what_prefix": Counter(),
        }
        for user in users
    }

    activity_by_owner: dict[str, list[dict]] = defaultdict(list)
    for row in tasks:
        owner = owner_map.get(row.get("OwnerId") or "", "")
        if not owner:
            continue
        by_owner[owner]["task_total"] += 1
        by_owner[owner]["who_prefix"][obj_label_from_prefix(prefix(row.get("WhoId")))] += 1
        by_owner[owner]["what_prefix"][obj_label_from_prefix(prefix(row.get("WhatId")))] += 1
        activity_by_owner[owner].append(
            {
                "kind": "Task",
                "created": parse_dt(row.get("CreatedDate")),
                "who": str(row.get("WhoId") or ""),
                "what": str(row.get("WhatId") or ""),
                "subject": row.get("Subject") or "",
            }
        )

    for row in events:
        owner = owner_map.get(row.get("OwnerId") or "", "")
        if not owner:
            continue
        by_owner[owner]["event_total"] += 1
        by_owner[owner]["who_prefix"][obj_label_from_prefix(prefix(row.get("WhoId")))] += 1
        by_owner[owner]["what_prefix"][obj_label_from_prefix(prefix(row.get("WhatId")))] += 1
        activity_by_owner[owner].append(
            {
                "kind": "Event",
                "created": parse_dt(row.get("CreatedDate")),
                "who": str(row.get("WhoId") or ""),
                "what": str(row.get("WhatId") or ""),
                "subject": row.get("Subject") or "",
            }
        )

    lead_examples_missing = []
    for lead in leads:
        owner = (lead.get("Owner") or {}).get("Name") or ""
        if not owner:
            continue
        by_owner[owner]["lead_count"] += 1
        lead_id = lead["Id"]
        created = parse_dt(lead.get("CreatedDate"))
        if not created:
            continue
        lead_linked = [
            act for act in activity_by_owner[owner]
            if act["who"] == lead_id and act["created"] and act["created"] >= created
        ]
        if lead_linked:
            by_owner[owner]["lead_touch_any"] += 1
        within_24h = [
            act for act in lead_linked
            if act["created"] and (act["created"] - created).total_seconds() <= 24 * 3600
        ]
        if within_24h:
            by_owner[owner]["lead_touch_24h"] += 1
        else:
            if len(lead_examples_missing) < 15:
                lead_examples_missing.append(
                    {
                        "owner": owner,
                        "lead_id": lead_id,
                        "company": lead.get("Company") or "",
                        "status": lead.get("Status") or "",
                        "created_date": str(lead.get("CreatedDate") or ""),
                        "converted_contact_id": lead.get("ConvertedContactId") or "",
                        "converted_account_id": lead.get("ConvertedAccountId") or "",
                    }
                )

    owner_rows = []
    for owner, metrics in by_owner.items():
        total_acts = metrics["task_total"] + metrics["event_total"]
        lead_linked_acts = metrics["who_prefix"].get("Lead", 0)
        contact_linked_acts = metrics["who_prefix"].get("Contact", 0)
        account_linked_acts = metrics["what_prefix"].get("Account", 0)
        opp_linked_acts = metrics["what_prefix"].get("Opportunity", 0)
        owner_rows.append(
            {
                "owner": owner,
                "lead_count": metrics["lead_count"],
                "lead_touch_24h": metrics["lead_touch_24h"],
                "lead_touch_any": metrics["lead_touch_any"],
                "task_total": metrics["task_total"],
                "event_total": metrics["event_total"],
                "total_activity": total_acts,
                "lead_linked_activity": lead_linked_acts,
                "contact_linked_activity": contact_linked_acts,
                "account_linked_activity": account_linked_acts,
                "opportunity_linked_activity": opp_linked_acts,
            }
        )

    overall = {
        "lead_count": len(leads),
        "task_count": len(tasks),
        "event_count": len(events),
        "lead_touch_24h": sum(row["lead_touch_24h"] for row in owner_rows),
        "lead_touch_any": sum(row["lead_touch_any"] for row in owner_rows),
        "lead_linked_activity": sum(row["lead_linked_activity"] for row in owner_rows),
        "contact_linked_activity": sum(row["contact_linked_activity"] for row in owner_rows),
        "account_linked_activity": sum(row["account_linked_activity"] for row in owner_rows),
        "opportunity_linked_activity": sum(row["opportunity_linked_activity"] for row in owner_rows),
    }

    payload = {
        "owners": owner_rows,
        "overall": overall,
        "lead_examples_missing_direct_touch": lead_examples_missing,
    }
    json_path = out_dir / "profile.json"
    md_path = out_dir / "profile.md"
    write_json(json_path, payload)

    owner_table = [
        [
            row["owner"],
            row["lead_count"],
            row["lead_touch_24h"],
            row["lead_touch_any"],
            row["total_activity"],
            row["lead_linked_activity"],
            row["contact_linked_activity"],
            row["account_linked_activity"],
            row["opportunity_linked_activity"],
        ]
        for row in owner_rows
    ]
    example_table = [
        [
            row["owner"],
            row["company"] or "-",
            row["status"] or "-",
            (row["created_date"] or "")[:10],
            row["converted_contact_id"] or "-",
            row["converted_account_id"] or "-",
        ]
        for row in lead_examples_missing[:12]
    ]

    lines = [
        "# BDR Response Integrity",
        "",
        "This profile checks whether SimCorp can measure a true 24-hour BDR response SLA from current Salesforce logging behavior.",
        "",
        "## What This Means",
        "",
        "- `lead touch within 24h` only counts activity directly linked to the Lead record (`WhoId = Lead`).",
        "- if reps complete work on Contacts, Accounts, or Opportunities instead, the current BDR SLA logic will undercount true response behavior.",
        "- this is a process-integrity issue first, not just a dashboard issue.",
        "",
        "## Owner-Level Integrity",
        "",
        md_table(
            [
                "Owner",
                "Leads",
                "Lead Touch <24h",
                "Lead Touch Any Time",
                "Total Activity",
                "Lead-Linked Activity",
                "Contact-Linked Activity",
                "Account-Linked Activity",
                "Opp-Linked Activity",
            ],
            owner_table,
        ),
        "",
        "## Overall Read",
        "",
        f"- Leads in scope: `{overall['lead_count']}`",
        f"- Tasks in scope: `{overall['task_count']}`",
        f"- Events in scope: `{overall['event_count']}`",
        f"- Leads with direct lead-linked touch inside 24h: `{overall['lead_touch_24h']}`",
        f"- Leads with any direct lead-linked touch: `{overall['lead_touch_any']}`",
        f"- Lead-linked activity count: `{overall['lead_linked_activity']}`",
        f"- Contact-linked activity count: `{overall['contact_linked_activity']}`",
        f"- Account-linked activity count: `{overall['account_linked_activity']}`",
        f"- Opportunity-linked activity count: `{overall['opportunity_linked_activity']}`",
        "",
        "## Sample Leads Missing Direct Lead-Linked Touch",
        "",
        md_table(
            ["Owner", "Company", "Status", "Created", "Converted Contact", "Converted Account"],
            example_table,
        ),
        "",
        "## Recommended Process Fixes",
        "",
        "1. Define the SLA on the object that must be touched first. If the expectation is first-touch on new leads, activity must be logged to the Lead before or at first outreach.",
        "2. Standardize BDR execution so calls/emails completed before conversion are lead-linked, not only account/contact-linked.",
        "3. Once a lead converts, move it out of the BDR SLA denominator and into meeting / handoff / follow-up metrics.",
        "4. Add a launch gate: dashboards must distinguish `direct lead-linked response` from `all owner activity` so managers can see whether the process itself is broken.",
        "",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if emit_text:
        print(str(out_dir))

    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote BDR response integrity profile to {out_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "bdr_count": len(users),
            "lead_count": overall["lead_count"],
            "lead_touch_24h": overall["lead_touch_24h"],
            "lead_touch_any": overall["lead_touch_any"],
            "missing_direct_touch_examples": len(lead_examples_missing),
            "output_dir": str(out_dir),
        },
        profile=payload,
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
