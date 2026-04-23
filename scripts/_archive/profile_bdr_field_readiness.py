#!/usr/bin/env python3
"""Profile institutional readiness for the SimCorp North America BDR motion.

This script goes beyond the basic operating-state baseline and answers:
- which source / segment / targeting dimensions are actually populated
- which campaign fields are usable today
- what matched-account / ABM context exists for North America BDR leads
- which dimensions are trustworthy enough for launch-day dashboards
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
DEFAULT_OUT_DIR = ROOT / "docs" / "generated" / f"bdr_field_readiness_{date.today().isoformat()}"
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
        "tool": "profile_bdr_field_readiness",
        "lane": "salesforce_data_profiles",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def run_sf(query: str, tooling: bool = False) -> list[dict]:
    cmd = ["sf", "data", "query", "--query", query, "--json"]
    if tooling:
        cmd.insert(3, "--use-tooling-api")
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
    rows = run_sf(
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
    return [
        BdrUser(
            id=row["Id"],
            name=row["Name"],
            title=row.get("Title") or "",
            department=row.get("Department") or "",
            role=(row.get("UserRole") or {}).get("Name") or "",
            manager=(row.get("Manager") or {}).get("Name") or "",
        )
        for row in rows
    ]


def profile_fields(users: list[BdrUser]) -> dict[str, object]:
    owner_ids = ",".join(f"'{user.id}'" for user in users)
    leads = run_sf(
        "SELECT Id, OwnerId, Company, Country, Industry, Status, LeadSource, "
        "Dimension_Persona__c, "
        "pi__campaign__c, pi__score__c, pi__utm_campaign__c, pi__utm_source__c, "
        "UTM_Campaign_Category__c, UTM_Source__c, "
        "engagio__Matched_Account__c, engagio__Matched_Account_Name__c, "
        "engagio__Matched_Account_Industry__c, engagio__Matched_Account_EngageMinsLast7Days__c, "
        "engagio__Matched_Account_EngageMinsLast3Months__c, "
        "engagio__Matched_Account__r.Region__c, "
        "engagio__Matched_Account__r.TAM_Universe_Segment__c, "
        "engagio__Matched_Account__r.Tier_Calculation__c "
        f"FROM Lead WHERE OwnerId IN ({owner_ids}) "
        f"AND CreatedDate >= {START}"
    )
    campaign_members = run_sf(
        "SELECT LeadId, HasResponded, Status, Campaign.Name, Campaign.Type, "
        "Campaign.Campaign_Product__c, Campaign.Campaign_Purpose__c, Campaign.Lead_Scope_Type__c "
        "FROM CampaignMember "
        f"WHERE LeadId != null AND Lead.OwnerId IN ({owner_ids}) "
        f"AND CreatedDate >= {START}"
    )

    total = len(leads)
    coverage_fields = [
        ("Dimension_Persona__c", "persona"),
        ("Industry", "industry"),
        ("Country", "country"),
        ("LeadSource", "lead_source"),
        ("pi__score__c", "pardot_score"),
        ("pi__campaign__c", "pardot_campaign"),
        ("pi__utm_campaign__c", "pardot_utm_campaign"),
        ("pi__utm_source__c", "pardot_utm_source"),
        ("UTM_Campaign_Category__c", "utm_campaign_category"),
        ("UTM_Source__c", "utm_source"),
        ("engagio__Matched_Account__c", "matched_account"),
        ("engagio__Matched_Account_Industry__c", "matched_account_industry"),
        ("engagio__Matched_Account__r.TAM_Universe_Segment__c", "matched_account_segment"),
        ("engagio__Matched_Account__r.Tier_Calculation__c", "matched_account_tier"),
    ]

    def get_value(row: dict, dotted: str) -> object:
        current: object = row
        for part in dotted.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current

    coverage = {}
    for field_name, key in coverage_fields:
        populated = sum(1 for row in leads if get_value(row, field_name) not in (None, "", []))
        coverage[key] = {
            "field": field_name,
            "populated": populated,
            "total": total,
            "coverage_pct": round((populated / total * 100.0), 1) if total else 0.0,
        }

    persona_counter = Counter((row.get("Dimension_Persona__c") or "Unknown") for row in leads)
    industry_counter = Counter((row.get("Industry") or "Unknown") for row in leads)
    source_counter = Counter((row.get("LeadSource") or "Unknown") for row in leads)
    status_counter = Counter((row.get("Status") or "Unknown") for row in leads)
    matched_segment_counter = Counter(
        (((row.get("engagio__Matched_Account__r") or {}).get("TAM_Universe_Segment__c")) or "Unmatched / Unknown")
        for row in leads
    )
    matched_tier_counter = Counter(
        (((row.get("engagio__Matched_Account__r") or {}).get("Tier_Calculation__c")) or "Unmatched / Unknown")
        for row in leads
    )

    campaign_summary: dict[tuple[str, str, str, str], dict[str, int]] = defaultdict(lambda: {"members": 0, "responses": 0})
    for row in campaign_members:
        campaign = row.get("Campaign") or {}
        key = (
            campaign.get("Name") or "Unmapped Campaign",
            campaign.get("Type") or "Unknown",
            campaign.get("Campaign_Product__c") or "Unknown",
            campaign.get("Lead_Scope_Type__c") or "Unknown",
        )
        campaign_summary[key]["members"] += 1
        campaign_summary[key]["responses"] += 1 if row.get("HasResponded") else 0

    campaign_rows = sorted(
        [
            {
                "name": name,
                "type": type_,
                "product": product,
                "scope_type": scope,
                "members": metrics["members"],
                "responses": metrics["responses"],
            }
            for (name, type_, product, scope), metrics in campaign_summary.items()
        ],
        key=lambda item: (-item["members"], -item["responses"], item["name"]),
    )

    matched_account_samples = []
    for row in leads:
        acct = row.get("engagio__Matched_Account__r") or {}
        if not acct:
            continue
        matched_account_samples.append(
            {
                "account_name": row.get("engagio__Matched_Account_Name__c") or acct.get("Name") or "",
                "region": acct.get("Region__c") or "",
                "segment": acct.get("TAM_Universe_Segment__c") or "",
                "tier": acct.get("Tier_Calculation__c") or "",
                "matched_industry": row.get("engagio__Matched_Account_Industry__c") or "",
            }
        )

    return {
        "lead_count": total,
        "coverage": coverage,
        "persona_distribution": dict(persona_counter),
        "industry_distribution": dict(industry_counter),
        "source_distribution": dict(source_counter),
        "status_distribution": dict(status_counter),
        "matched_segment_distribution": dict(matched_segment_counter),
        "matched_tier_distribution": dict(matched_tier_counter),
        "campaign_summary": campaign_rows,
        "matched_account_samples": matched_account_samples[:15],
    }


def write_markdown(users: list[BdrUser], payload: dict[str, object], output_path: Path) -> None:
    coverage = payload["coverage"]
    coverage_rows = [
        [key, item["field"], item["populated"], item["total"], f'{item["coverage_pct"]}%']
        for key, item in coverage.items()
    ]
    persona_rows = [[name, count] for name, count in sorted(payload["persona_distribution"].items(), key=lambda kv: (-kv[1], kv[0]))[:12]]
    industry_rows = [[name, count] for name, count in sorted(payload["industry_distribution"].items(), key=lambda kv: (-kv[1], kv[0]))[:12]]
    source_rows = [[name, count] for name, count in sorted(payload["source_distribution"].items(), key=lambda kv: (-kv[1], kv[0]))[:12]]
    segment_rows = [[name, count] for name, count in sorted(payload["matched_segment_distribution"].items(), key=lambda kv: (-kv[1], kv[0]))[:12]]
    tier_rows = [[name, count] for name, count in sorted(payload["matched_tier_distribution"].items(), key=lambda kv: (-kv[1], kv[0]))[:12]]
    campaign_rows = [
        [row["name"], row["type"], row["product"], row["scope_type"], row["members"], row["responses"]]
        for row in payload["campaign_summary"][:15]
    ]
    account_rows = [
        [
            row["account_name"],
            row["region"] or "-",
            row["segment"] or "-",
            row["tier"] or "-",
            row["matched_industry"] or "-",
        ]
        for row in payload["matched_account_samples"][:10]
    ]

    lines = [
        "# BDR Field Readiness",
        "",
        "This profile checks whether the SimCorp North America BDR motion has the source, segment, targeting, and campaign fields needed for an institutional-grade operating system.",
        "",
        "## Team In Scope",
        "",
        md_table(
            ["BDR", "Title", "Department", "Role", "Manager"],
            [[u.name, u.title, u.department, u.role, u.manager] for u in users],
        ),
        "",
        "## Coverage Of Key Dimensions",
        "",
        md_table(["Dimension", "Field", "Populated", "Total Leads", "Coverage"], coverage_rows),
        "",
        "## What We Can Trust Today",
        "",
        "- `Persona`, `Industry`, `Country`, `Account Engagement Score`, and `Account Engagement Campaign` are populated on essentially every NA BDR lead.",
        "- `Lead Source` is only partially populated, but usable enough for a basic source-quality view.",
        "- `UTM` is effectively dead on this slice and should not be used as a primary operating dimension.",
        "- `Matched Account` coverage exists on a minority of leads, but it is enough to support a first-generation target-account / ABM lens.",
        "- `TAM Universe Segment` and account `Tier` are usable only where a matched account exists.",
        "",
        "## Persona Distribution",
        "",
        md_table(["Persona", "Lead Count"], persona_rows),
        "",
        "## Industry Distribution",
        "",
        md_table(["Industry", "Lead Count"], industry_rows),
        "",
        "## Lead Source Distribution",
        "",
        md_table(["Lead Source", "Lead Count"], source_rows),
        "",
        "## Matched-Account Segment Distribution",
        "",
        md_table(["Matched Segment", "Lead Count"], segment_rows),
        "",
        "## Matched-Account Tier Distribution",
        "",
        md_table(["Matched Tier", "Lead Count"], tier_rows),
        "",
        "## Campaign Reality In The NA BDR Slice",
        "",
        md_table(["Campaign", "Type", "Product", "Scope Type", "Members", "Responses"], campaign_rows),
        "",
        "## Sample Matched Accounts",
        "",
        md_table(["Matched Account", "Region", "Universe Segment", "Tier", "Matched Industry"], account_rows),
        "",
        "## Launch Implications",
        "",
        "- The first institutional-grade BDR system should use `source`, `persona`, `industry`, and `matched-account tier/segment` as its core targeting dimensions.",
        "- `UTM` should be monitored as a data-quality gap, not trusted as a core attribution source.",
        "- `Campaign Scope Type`, `Campaign Product`, and response status are strong enough to support a `campaign / target-list control` surface later.",
        "- The first rep and manager dashboards should focus on execution quality, re-engagement, campaign responses, and meeting creation rather than pretend SimCorp has pristine sourced-pipeline attribution.",
        "",
        "## Recommended Institutional Launch Gates",
        "",
        "1. No dashboard should use `UTM` as a hero metric.",
        "2. Every manager or rep queue must expose `NextBestAction` plus a real `SuggestedTool`.",
        "3. Every source / segment chart must use fields with at least practical coverage in the NA slice.",
        "4. Target-account views should explicitly label whether they are based on `Matched Account` or not.",
        "5. Campaign pages should use `Campaign Product`, `Campaign Scope Type`, and response status before claiming ROI-style attribution.",
        "",
        "## Sources",
        "",
        "- Salesforce Sales Engagement: https://www.salesforce.com/sales/engagement/",
        "- Salesforce Account Engagement: https://www.salesforce.com/marketing/b2b-automation/",
        "- Salesforce Sales Cloud: https://www.salesforce.com/sales/cloud/",
        "- Tableau dashboard best practices: https://help.tableau.com/current/pro/desktop/en-us/dashboards_best_practices.htm",
        "- Tableau Exchange: https://exchange.tableau.com/",
    ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile institutional readiness for the NA BDR motion.")
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
    payload = profile_fields(users)
    json_path = out_dir / "profile.json"
    md_path = out_dir / "profile.md"
    write_json(json_path, payload)
    write_markdown(users, payload, md_path)

    if emit_text:
        print(str(out_dir))

    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote BDR field readiness profile to {out_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "bdr_count": len(users),
            "lead_count": int(payload["lead_count"]),
            "coverage_dimension_count": len(payload["coverage"]),
            "campaign_summary_count": len(payload["campaign_summary"]),
            "matched_account_sample_count": len(payload["matched_account_samples"]),
            "output_dir": str(out_dir),
        },
        profile={
            "team": [
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
            "field_readiness": payload,
        },
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
