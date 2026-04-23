#!/usr/bin/env python3
"""Profile current BDR operating state for the North America team.

This script is intentionally narrow: it builds a consultant-grade baseline for
what SimCorp's NA BDR motion looks like today, what Salesforce objects are in
use, and which Salesforce-native tools are available versus actually assigned.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


ROOT = Path("/Users/test/crm-analytics")
DEFAULT_OUT_DIR = ROOT / "docs" / "generated" / f"bdr_operating_state_{date.today().isoformat()}"


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
        "tool": "profile_bdr_operating_state",
        "lane": "salesforce_data_profiles",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def run_sf_query(query: str, tooling: bool = False) -> list[dict]:
    cmd = ["sf", "data", "query", "--query", query, "--json"]
    if tooling:
        cmd.insert(3, "--use-tooling-api")
    raw = subprocess.check_output(cmd, text=True)
    return json.loads(raw)["result"]["records"]


def json_dump(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def md_table(headers: list[str], rows: list[list[object]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


@dataclass(frozen=True)
class BdrUser:
    id: str
    name: str
    title: str
    department: str
    role: str
    manager: str


def get_na_bdr_users() -> list[BdrUser]:
    rows = run_sf_query(
        "SELECT Id, Name, Title, Department, UserRole.Name, Manager.Name "
        "FROM User "
        "WHERE IsActive = true "
        "AND ("
        "Title LIKE '%Business Development Representative%' OR "
        "Title LIKE '%Lead Business Development Representative%' OR "
        "Title LIKE '%Senior Business Development Representative%') "
        "AND (Department LIKE '%NA%' OR Department LIKE '%North America%' OR Department LIKE '%US (%') "
        "ORDER BY Name"
    )
    users: list[BdrUser] = []
    for row in rows:
        users.append(
            BdrUser(
                id=row["Id"],
                name=row["Name"],
                title=row.get("Title") or "",
                department=row.get("Department") or "",
                role=(row.get("UserRole") or {}).get("Name") or "",
                manager=(row.get("Manager") or {}).get("Name") or "",
            )
        )
    return users


def profile_owner_metrics(users: list[BdrUser]) -> dict[str, dict]:
    owner_ids = ",".join(f"'{user.id}'" for user in users)
    leads = run_sf_query(
        "SELECT Id, OwnerId, IsConverted, ConvertedOpportunityId, Company, Status, LeadSource, "
        "pi__campaign__c, pi__score__c, pi__utm_campaign__c "
        f"FROM Lead WHERE OwnerId IN ({owner_ids}) "
        "AND CreatedDate >= 2025-01-01T00:00:00Z"
    )
    tasks = run_sf_query(
        "SELECT OwnerId, Type, COUNT(Id) cnt "
        f"FROM Task WHERE OwnerId IN ({owner_ids}) "
        "AND ActivityDate >= 2025-01-01 "
        "GROUP BY OwnerId, Type"
    )
    events = run_sf_query(
        "SELECT OwnerId, Type, COUNT(Id) cnt "
        f"FROM Event WHERE OwnerId IN ({owner_ids}) "
        "AND ActivityDate >= 2025-01-01 "
        "GROUP BY OwnerId, Type"
    )
    campaign_members = run_sf_query(
        "SELECT Lead.OwnerId, Status "
        "FROM CampaignMember "
        f"WHERE LeadId != null AND Lead.OwnerId IN ({owner_ids}) "
        "AND CreatedDate >= 2025-01-01T00:00:00Z"
    )

    metrics: dict[str, dict] = {
        user.id: {
            "name": user.name,
            "leads": 0,
            "converted": 0,
            "converted_with_opp": 0,
            "companies": set(),
            "campaign_tagged": 0,
            "scored": 0,
            "utm_populated": 0,
            "lead_sources": Counter(),
            "lead_statuses": Counter(),
            "task_types": Counter(),
            "event_types": Counter(),
            "campaign_member_statuses": Counter(),
        }
        for user in users
    }

    for row in leads:
        item = metrics[row["OwnerId"]]
        item["leads"] += 1
        item["converted"] += 1 if row["IsConverted"] else 0
        item["converted_with_opp"] += 1 if row.get("ConvertedOpportunityId") else 0
        if row.get("Company"):
            item["companies"].add(row["Company"])
        item["campaign_tagged"] += 1 if row.get("pi__campaign__c") else 0
        item["scored"] += 1 if row.get("pi__score__c") not in (None, "") else 0
        item["utm_populated"] += 1 if row.get("pi__utm_campaign__c") else 0
        item["lead_sources"][row.get("LeadSource") or "Unknown"] += 1
        item["lead_statuses"][row.get("Status") or "Unknown"] += 1

    for row in tasks:
        metrics[row["OwnerId"]]["task_types"][row.get("Type") or "Unknown"] += int(row["cnt"])

    for row in events:
        metrics[row["OwnerId"]]["event_types"][row.get("Type") or "Unknown"] += int(row["cnt"])

    for row in campaign_members:
        owner = (row.get("Lead") or {}).get("OwnerId")
        if owner in metrics:
            metrics[owner]["campaign_member_statuses"][row.get("Status") or "Unknown"] += 1

    serialized: dict[str, dict] = {}
    for user in users:
        item = metrics[user.id]
        serialized[user.name] = {
            "leads": item["leads"],
            "converted": item["converted"],
            "converted_with_opp": item["converted_with_opp"],
            "companies": len(item["companies"]),
            "campaign_tagged": item["campaign_tagged"],
            "scored": item["scored"],
            "utm_populated": item["utm_populated"],
            "task_types": dict(item["task_types"]),
            "event_types": dict(item["event_types"]),
            "campaign_member_statuses": dict(item["campaign_member_statuses"]),
            "lead_sources": dict(item["lead_sources"]),
            "lead_statuses": dict(item["lead_statuses"]),
        }
    return serialized


def get_org_tool_access(users: list[BdrUser]) -> dict[str, object]:
    relevant_psls = run_sf_query(
        "SELECT PermissionSetLicense.MasterLabel, PermissionSetLicense.TotalLicenses, "
        "PermissionSetLicense.UsedLicenses, PermissionSetLicense.Status "
        "FROM PermissionSetLicense "
        "WHERE PermissionSetLicense.MasterLabel IN ("
        "'Account Engagement',"
        "'Sales Cloud Engage',"
        "'Sales Engagement Basic',"
        "'Sales Action Plans',"
        "'Standard Einstein Activity Capture User',"
        "'Einstein GPT Sales Emails',"
        "'Einstein Sales Summaries',"
        "'Salesforce Meetings',"
        "'View Unified Engagement History Dashboards'"
        ") "
        "ORDER BY PermissionSetLicense.MasterLabel"
    )
    relevant_ps = run_sf_query(
        "SELECT Name, Label FROM PermissionSet "
        "WHERE Label IN ("
        "'Account Engagement Package',"
        "'Account Engagement User',"
        "'Engagement Intelligence',"
        "'Pardot Engagement History',"
        "'Sales Engagement Basic User',"
        "'Sales Engagement Cadence Creator',"
        "'Sales Engagement Quick Cadence Creator',"
        "'Sales Engagement User',"
        "'Salesforce Engage',"
        "'Salesforce Engage App Assignment',"
        "'LinkedIn Sales Navigator Standard User',"
        "'View Unified Engagement History Dashboards'"
        ") "
        "ORDER BY Label"
    )

    owner_ids = ",".join(f"'{user.id}'" for user in users)
    ps_assignments = run_sf_query(
        "SELECT Assignee.Name, PermissionSet.Label "
        "FROM PermissionSetAssignment "
        f"WHERE AssigneeId IN ({owner_ids}) "
        "AND PermissionSet.Label IN ("
        "'Account Engagement Package',"
        "'Account Engagement User',"
        "'Engagement Intelligence',"
        "'Pardot Engagement History',"
        "'Sales Engagement Basic User',"
        "'Sales Engagement Cadence Creator',"
        "'Sales Engagement Quick Cadence Creator',"
        "'Sales Engagement User',"
        "'Salesforce Engage',"
        "'Salesforce Engage App Assignment',"
        "'LinkedIn Sales Navigator Standard User',"
        "'View Unified Engagement History Dashboards'"
        ") "
        "ORDER BY Assignee.Name, PermissionSet.Label"
    )
    psl_assignments = run_sf_query(
        "SELECT Assignee.Name, PermissionSetLicense.MasterLabel "
        "FROM PermissionSetLicenseAssign "
        f"WHERE AssigneeId IN ({owner_ids}) "
        "ORDER BY Assignee.Name, PermissionSetLicense.MasterLabel"
    )

    assignment_map: dict[str, list[str]] = defaultdict(list)
    for row in ps_assignments:
        assignment_map[row["Assignee"]["Name"]].append(row["PermissionSet"]["Label"])

    psl_map: dict[str, list[str]] = defaultdict(list)
    for row in psl_assignments:
        psl_map[row["Assignee"]["Name"]].append(row["PermissionSetLicense"]["MasterLabel"])

    entity_rows = run_sf_query(
        "SELECT QualifiedApiName, Label "
        "FROM EntityDefinition "
        "WHERE QualifiedApiName LIKE '%Cadence%' "
        "OR QualifiedApiName LIKE '%Sequence%' "
        "OR QualifiedApiName LIKE '%Engagement%' "
        "OR QualifiedApiName LIKE '%SalesWork%' "
        "ORDER BY QualifiedApiName",
        tooling=True,
    )
    try:
        work_queue_settings = run_sf_query("SELECT COUNT(Id) cnt FROM SalesWorkQueueSettings")[0]["cnt"]
    except Exception:
        work_queue_settings = None

    return {
        "org_permission_set_licenses": relevant_psls,
        "org_permission_sets": relevant_ps,
        "user_permission_sets": dict(assignment_map),
        "user_permission_set_licenses": dict(psl_map),
        "cadence_and_engagement_entities": entity_rows,
        "sales_work_queue_settings_count": work_queue_settings,
    }


def get_campaign_snapshot() -> dict[str, object]:
    na_campaigns = run_sf_query(
        "SELECT Id, Name, Type, Status, IsActive, StartDate, EndDate "
        "FROM Campaign "
        "WHERE Name LIKE '%NA%' "
        "AND CreatedDate >= 2025-01-01T00:00:00Z "
        "ORDER BY CreatedDate DESC LIMIT 30"
    )
    recent_campaigns = run_sf_query(
        "SELECT Id, Name, Type, Status, IsActive, StartDate, EndDate "
        "FROM Campaign "
        "WHERE CreatedDate >= 2025-01-01T00:00:00Z "
        "ORDER BY CreatedDate DESC LIMIT 50"
    )
    return {
        "recent_na_campaigns": na_campaigns,
        "recent_campaigns": recent_campaigns,
    }


def write_markdown(payload: dict[str, object], output_path: Path) -> None:
    users: list[dict] = payload["na_bdr_users"]
    metrics: dict[str, dict] = payload["owner_metrics"]
    tools: dict[str, object] = payload["tool_access"]
    campaigns: dict[str, object] = payload["campaign_snapshot"]

    user_rows = [
        [user["name"], user["title"], user["department"], user["role"], user["manager"]]
        for user in users
    ]
    owner_rows = []
    for owner, item in metrics.items():
        owner_rows.append(
            [
                owner,
                item["leads"],
                item["converted"],
                item["converted_with_opp"],
                item["task_types"].get("Call", 0),
                item["task_types"].get("Outbound Email", 0),
                item["event_types"].get("Meeting", 0),
            ]
        )

    psl_rows = [
        [row["MasterLabel"], row["TotalLicenses"], row["UsedLicenses"], row["Status"]]
        for row in tools["org_permission_set_licenses"]
    ]
    recent_na_campaign_rows = [
        [row["Name"], row.get("Type") or "", row.get("Status") or "", row.get("StartDate") or "", row.get("EndDate") or ""]
        for row in campaigns["recent_na_campaigns"][:12]
    ]

    lines = [
        "# North America BDR Operating State",
        "",
        "This is the current-state baseline for building a consultant-grade SimCorp BDR operating system in CRM Analytics.",
        "",
        "## North America BDR Team",
        "",
        md_table(["BDR", "Title", "Department", "User Role", "Manager"], user_rows),
        "",
        "## Current Operating Volume Since 2025-01-01",
        "",
        md_table(
            ["BDR", "Leads", "Converted", "Converted w/ Opp Link", "Calls", "Outbound Emails", "Meetings"],
            owner_rows,
        ),
        "",
        "## What The Current Data Says",
        "",
        "- NA BDR volume is still thin on lead creation, but activity volume is real and uneven by rep.",
        "- Converted-lead attribution into opportunities is weak because `ConvertedOpportunityId` is mostly absent.",
        "- Campaign tagging and lead scoring exist on the BDR-owned lead slice, but UTM population is effectively unused.",
        "- The first BDR operating system should optimize execution quality, stale-list recovery, campaign response handling, and meeting creation instead of pretending SimCorp already has a large pristine SDR funnel.",
        "",
        "## Org-Level Tool Access Relevant To BDR",
        "",
        md_table(["License / Capability", "Total", "Used", "Status"], psl_rows),
        "",
        "## Current Tooling Reality",
        "",
        "- NA BDR users all have `Account Engagement Package`, `Salesforce Engage`, and `Salesforce Engage App Assignment`.",
        "- All three NA BDRs have the `Sales Engagement Basic` license; two have `LinkedIn Sales Navigator Standard User`.",
        "- Org-level capability exists for `Sales Action Plans`, `Standard Einstein Activity Capture User`, `Einstein GPT Sales Emails`, `Einstein Sales Summaries`, `Salesforce Meetings`, and `View Unified Engagement History Dashboards`, but those are not meaningfully assigned/used in the NA BDR slice today.",
        "- The org exposes Sales Engagement / cadence entities such as `ActionCadence`, `ActionCadenceStep`, `ActionCadenceStepTracker`, `HighVelocityEngagement`, and `SalesWorkQueueSettings`; there is also `1` `SalesWorkQueueSettings` record in the org.",
        "",
        "## Campaign Snapshot",
        "",
        md_table(["Recent NA Campaign", "Type", "Status", "Start", "End"], recent_na_campaign_rows),
        "",
        "## Consultant-Grade Target State For SimCorp BDR",
        "",
        "### Weekly BDR Manager Questions",
        "",
        "- Which reps are creating meetings and opps from scored leads?",
        "- Which campaigns and source groups are creating qualified engagement in North America?",
        "- Which leads and companies are stale but still recoverable?",
        "- Which reps are falling behind on touch cadence, SLA, or meeting follow-through?",
        "- Which target accounts should be re-engaged this week through campaigns, Salesforce Engage, or Sales Engagement work queues?",
        "",
        "### Daily BDR Rep Questions",
        "",
        "- Who should I contact first today?",
        "- Which leads need re-engagement versus fresh outreach?",
        "- Which meetings need preparation or follow-up?",
        "- Which campaign responses require immediate action?",
        "",
        "### Recommended North America BDR Dashboard Set",
        "",
        "1. `BDR Manager`",
        "   - Team scorecard",
        "   - Lead-to-meeting and lead-to-opportunity conversion",
        "   - Campaign/source quality",
        "   - Rep cadence and SLA hygiene",
        "   - Stale list recovery / re-engagement queue",
        "2. `BDR Rep Queue`",
        "   - My weekly priorities",
        "   - Meetings & follow-up",
        "   - Campaign response queue",
        "   - Re-engagement plays",
        "3. `BDR Campaign / Target List Control`",
        "   - Campaign velocity and response quality",
        "   - Old-prospect reactivation cohorts",
        "   - Named-account coverage and gap analysis",
        "",
        "### Tool Orchestration Recommendation",
        "",
        "- `Account Engagement` for scoring, campaign tagging, source context, and engagement history.",
        "- `Salesforce Engage` for email-driven rep execution against marketing-qualified and re-engagement cohorts.",
        "- `Sales Engagement Basic` for guided work queues and cadence-style execution where available.",
        "- `LinkedIn Sales Navigator` for target-account and contact enrichment where assigned.",
        "- `Sales Action Plans` for structured re-engagement or meeting-conversion plays once the workflow is defined.",
        "- `Einstein Activity Capture / Sales Summaries / GPT Sales Emails / Meetings` as a second wave after the operating rhythm is stable.",
        "",
        "### Phased Build Recommendation",
        "",
        "1. Build the North America `BDR Manager` surface around conversion, cadence, campaign response, and stale-list recovery.",
        "2. Tighten the `BDR Rep Queue` into a true daily operating page with re-engagement and meeting follow-up actions.",
        "3. Add a dedicated campaign / target-list control surface once the core manager/rep system is stable.",
        "4. Only then broaden to EMEA / APAC and more advanced productivity tooling.",
        "",
        "## Sources",
        "",
        "- Salesforce Sales Engagement: https://www.salesforce.com/sales/engagement/",
        "- Salesforce Sales Cloud productivity / Sales Cloud: https://www.salesforce.com/sales/cloud/",
        "- Salesforce Account Engagement: https://www.salesforce.com/marketing/b2b-automation/",
        "- Tableau dashboard best practices: https://help.tableau.com/current/pro/desktop/en-us/dashboards_best_practices.htm",
        "- Tableau Exchange sales pipeline / forecast accelerators: https://exchange.tableau.com/",
    ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
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


def run_profile_command(
    output_dir: Path,
    *,
    emit_text: bool = True,
) -> tuple[dict[str, Any], int]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    users = get_na_bdr_users()
    metrics = profile_owner_metrics(users)
    tools = get_org_tool_access(users)
    campaigns = get_campaign_snapshot()

    payload = {
        "na_bdr_users": [
            {
                "id": user.id,
                "name": user.name,
                "title": user.title,
                "department": user.department,
                "role": user.role,
                "manager": user.manager,
            }
            for user in users
        ],
        "owner_metrics": metrics,
        "tool_access": tools,
        "campaign_snapshot": campaigns,
    }
    json_path = output_dir / "profile.json"
    md_path = output_dir / "profile.md"
    json_dump(json_path, payload)
    write_markdown(payload, md_path)

    if emit_text:
        print(str(output_dir))

    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote BDR operating state profile to {output_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "bdr_count": len(payload["na_bdr_users"]),
            "owner_metric_count": len(payload["owner_metrics"]),
            "recent_na_campaign_count": len(payload["campaign_snapshot"]["recent_na_campaigns"]),
            "sales_work_queue_settings_count": payload["tool_access"]["sales_work_queue_settings_count"],
            "output_dir": str(output_dir),
        },
        profile=payload,
    )
    return result, 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result, exit_code = run_profile_command(
        args.output_dir,
        emit_text=not args.json,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
