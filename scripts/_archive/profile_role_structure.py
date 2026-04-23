#!/usr/bin/env python3
"""Profile commercial role structures from live Salesforce user and ownership data.

This is analysis-only. It maps the internal commercial slice, highlights the
signals available for persona classification, and shows how those roles appear
in account and opportunity ownership.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crm_analytics_helpers import get_auth
from build_revenue_retention_health import run_soql


USER_FILTER = (
    "IsActive = true AND UserType = 'Standard' AND ("
    "UserRole.Name LIKE 'SC %' "
    "OR Title LIKE '%Sales%' "
    "OR Title LIKE '%Customer Success%' "
    "OR Title LIKE '%Consult%' "
    "OR Title LIKE '%Service Delivery%' "
    "OR Title LIKE '%Support%' "
    "OR Title LIKE '%Marketing%' "
    "OR Department LIKE '%Customer Experience%' "
    "OR Division LIKE 'Market Unit%' "
    "OR Division = 'Commercial Management' "
    "OR Division = 'Services Division' "
    "OR Division = 'XaaS Delivery Center'"
    ")"
)

USER_QUERY = (
    "SELECT Id, Name, Title, Department, Division, UserType, UserRole.Name, "
    "ManagerId, Manager.Name "
    "FROM User "
    f"WHERE {USER_FILTER} "
    "ORDER BY UserRole.Name, Division, Department, Title, Name"
)

OPP_OWNER_QUERY = (
    "SELECT OwnerId, COUNT(Id) cnt "
    "FROM Opportunity "
    "WHERE CloseDate >= 2023-01-01 "
    "GROUP BY OwnerId "
    "ORDER BY COUNT(Id) DESC "
    "LIMIT 2000"
)

ACCOUNT_OWNER_QUERY = (
    "SELECT OwnerId, COUNT(Id) cnt "
    "FROM Account "
    "WHERE IsDeleted = false "
    "GROUP BY OwnerId "
    "ORDER BY COUNT(Id) DESC "
    "LIMIT 2000"
)

ACTIVE_USER_TYPE_QUERY = (
    "SELECT UserType, COUNT(Id) cnt "
    "FROM User "
    "WHERE IsActive = true "
    "GROUP BY UserType "
    "ORDER BY COUNT(Id) DESC"
)

OPP_OWNER_TYPE_QUERY = (
    "SELECT OwnerId, Type, COUNT(Id) cnt "
    "FROM Opportunity "
    "WHERE CloseDate >= 2023-01-01 "
    "AND Type IN ('Land','Expand','Renewal') "
    "GROUP BY OwnerId, Type "
    "LIMIT 5000"
)


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
        "tool": "profile_role_structure",
        "lane": "salesforce_data_profiles",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def norm(value: str | None) -> str:
    return (value or "").strip()


def lower_join(*parts: str | None) -> str:
    return " | ".join(norm(part) for part in parts if norm(part)).lower()


def classify_role_signal(title: str, role: str, department: str, division: str) -> str:
    text = lower_join(title, role, department, division)
    if "marketing" in text:
        return "Marketing"
    if "sales" in text and "customer success" not in text and "cx" not in text:
        return "Sales"
    if (
        "customer success" in text
        or re.search(r"\bcx\b", text)
        or "customer experience" in text
    ):
        return "CX"
    if (
        "consult" in text
        or "professional services" in text
        or "service delivery" in text
        or "delivery center" in text
        or "support" in text
        or "testing services" in text
        or "opportunity owners" in text
    ):
        return "Services"
    if "managing director" in text or "vice president" in text:
        return "Leadership"
    return "Other"


def classify_role_only(role: str) -> str:
    return classify_role_signal("", role, "", "")


def classify_title_only(title: str, department: str, division: str) -> str:
    return classify_role_signal(title, "", department, division)


def region_signal(role: str, division: str, department: str) -> str:
    text = lower_join(role, division, department)
    if "north america" in text or " na " in f" {text} " or "sc na" in text:
        return "North America"
    if "emea" in text or "sc ne" in text or "sc se" in text:
        return "EMEA"
    if "asia" in text or "apac" in text or "sc asia" in text:
        return "APAC"
    return "Unknown"


def serialize_user(row: dict[str, Any]) -> dict[str, Any]:
    role = (row.get("UserRole") or {}).get("Name") or ""
    title = row.get("Title") or ""
    department = row.get("Department") or ""
    division = row.get("Division") or ""
    manager_name = (row.get("Manager") or {}).get("Name") or ""
    persona = classify_role_signal(title, role, department, division)
    role_persona = classify_role_only(role)
    title_persona = classify_title_only(title, department, division)
    return {
        "Id": row.get("Id"),
        "Name": row.get("Name"),
        "Title": title,
        "Department": department,
        "Division": division,
        "UserType": row.get("UserType"),
        "UserRole": role,
        "ManagerId": row.get("ManagerId") or "",
        "ManagerName": manager_name,
        "Persona": persona,
        "RolePersona": role_persona,
        "TitlePersona": title_persona,
        "Region": region_signal(role, division, department),
    }


def build_profile(
    users: list[dict[str, Any]],
    opp_owner_rows: list[dict[str, Any]],
    account_owner_rows: list[dict[str, Any]],
    user_type_rows: list[dict[str, Any]],
    opp_owner_type_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    user_records = [serialize_user(row) for row in users]
    user_by_id = {row["Id"]: row for row in user_records}

    opp_counts = {row.get("OwnerId"): int(row.get("cnt") or 0) for row in opp_owner_rows}
    account_counts = {row.get("OwnerId"): int(row.get("cnt") or 0) for row in account_owner_rows}
    opp_persona_by_type: dict[str, Counter[str]] = defaultdict(Counter)

    persona_counts = Counter(row["Persona"] for row in user_records)
    region_counts = Counter(row["Region"] for row in user_records)
    role_counts = Counter(row["UserRole"] for row in user_records)
    division_counts = Counter(row["Division"] for row in user_records)

    opp_owner_enriched = []
    for owner_id, count in sorted(opp_counts.items(), key=lambda item: item[1], reverse=True):
        user = user_by_id.get(owner_id)
        opp_owner_enriched.append(
            {
                "OwnerId": owner_id,
                "OpportunityCount": count,
                "Name": (user or {}).get("Name", "<not in slice>"),
                "Persona": (user or {}).get("Persona", "Unknown"),
                "Title": (user or {}).get("Title", ""),
                "UserRole": (user or {}).get("UserRole", ""),
                "Division": (user or {}).get("Division", ""),
                "ManagerName": (user or {}).get("ManagerName", ""),
            }
        )

    for row in opp_owner_type_rows:
        owner_id = row.get("OwnerId")
        opp_type = row.get("Type") or "Unknown"
        persona = (user_by_id.get(owner_id) or {}).get("Persona", "Unknown")
        opp_persona_by_type[opp_type][persona] += int(row.get("cnt") or 0)

    account_owner_enriched = []
    for owner_id, count in sorted(account_counts.items(), key=lambda item: item[1], reverse=True):
        user = user_by_id.get(owner_id)
        account_owner_enriched.append(
            {
                "OwnerId": owner_id,
                "AccountCount": count,
                "Name": (user or {}).get("Name", "<not in slice>"),
                "Persona": (user or {}).get("Persona", "Unknown"),
                "Title": (user or {}).get("Title", ""),
                "UserRole": (user or {}).get("UserRole", ""),
                "Division": (user or {}).get("Division", ""),
                "ManagerName": (user or {}).get("ManagerName", ""),
            }
        )

    duplicate_names = []
    grouped_by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for user in user_records:
        grouped_by_name[user["Name"]].append(user)
    for name, rows in grouped_by_name.items():
        if len(rows) > 1:
            duplicate_names.append(
                {
                    "Name": name,
                    "Count": len(rows),
                    "Variants": [
                        {
                            "Id": row["Id"],
                            "Title": row["Title"],
                            "UserRole": row["UserRole"],
                            "Division": row["Division"],
                            "ManagerName": row["ManagerName"],
                        }
                        for row in rows
                    ],
                }
            )

    mismatch_rows = []
    for user in user_records:
        if user["RolePersona"] != "Other" and user["TitlePersona"] != "Other" and user["RolePersona"] != user["TitlePersona"]:
            mismatch_rows.append(user)
        elif user["RolePersona"] == "CX" and user["TitlePersona"] == "Sales":
            mismatch_rows.append(user)
        elif user["RolePersona"] == "Sales" and user["TitlePersona"] == "CX":
            mismatch_rows.append(user)

    manager_rollup: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"direct_reports": 0, "persona_counts": Counter(), "opp_count": 0, "account_count": 0}
    )
    for user in user_records:
        manager = user["ManagerName"] or "<no manager>"
        manager_rollup[manager]["direct_reports"] += 1
        manager_rollup[manager]["persona_counts"][user["Persona"]] += 1
        manager_rollup[manager]["opp_count"] += opp_counts.get(user["Id"], 0)
        manager_rollup[manager]["account_count"] += account_counts.get(user["Id"], 0)

    manager_rows = [
        {
            "ManagerName": name,
            "DirectReports": data["direct_reports"],
            "PersonaCounts": dict(data["persona_counts"]),
            "OwnedOpportunities": data["opp_count"],
            "OwnedAccounts": data["account_count"],
        }
        for name, data in sorted(
            manager_rollup.items(),
            key=lambda item: (item[1]["opp_count"] + item[1]["account_count"], item[1]["direct_reports"]),
            reverse=True,
        )
    ]

    user_type_counts = {row.get("UserType"): int(row.get("cnt") or 0) for row in user_type_rows}

    return {
        "active_user_type_counts": user_type_counts,
        "internal_slice_count": len(user_records),
        "persona_counts": dict(persona_counts),
        "region_counts": dict(region_counts),
        "top_user_roles": role_counts.most_common(20),
        "top_divisions": division_counts.most_common(20),
        "top_opportunity_owners": opp_owner_enriched[:40],
        "top_account_owners": account_owner_enriched[:40],
        "opportunity_persona_by_type": {
            opp_type: dict(counter.most_common())
            for opp_type, counter in sorted(opp_persona_by_type.items())
        },
        "duplicate_names": duplicate_names,
        "mismatch_examples": mismatch_rows[:40],
        "manager_rollup": manager_rows[:40],
    }


def format_table_moneyless(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    header = "| " + " | ".join(columns) + " |"
    sep = "|---" * len(columns) + "|"
    lines = [header, sep]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(col, "")) for col in columns) + " |")
    return lines


def render_markdown(profile: dict[str, Any]) -> str:
    lines = [
        "# Commercial Role Structure Profile",
        "",
        "This report maps the internal commercial role structure using live Salesforce `User`, `Opportunity`, and `Account` ownership data.",
        "",
        "## Key Findings",
        "",
        f"- Active users in org: `{sum(profile['active_user_type_counts'].values()):,}`; most are `PowerCustomerSuccess`, not internal operators.",
        f"- Internal commercial slice identified: `{profile['internal_slice_count']:,}` active `Standard` users.",
        f"- Personas inferred from live labels: `{profile['persona_counts']}`.",
        "- `OwnerName` alone is not enough; `OwnerId`, `UserRole`, `Title`, `Division`, and `Manager` all matter.",
        "- Duplicate names exist, so owner mapping should be `Id`-based, not name-based.",
        "",
        "## Active User Types",
        "",
    ]

    lines.extend(
        format_table_moneyless(
            [{"UserType": k, "Count": v} for k, v in profile["active_user_type_counts"].items()],
            ["UserType", "Count"],
        )
    )

    lines.extend(
        [
            "",
            "## Internal Persona Counts",
            "",
        ]
    )
    lines.extend(
        format_table_moneyless(
            [{"Persona": k, "Count": v} for k, v in sorted(profile["persona_counts"].items(), key=lambda item: item[1], reverse=True)],
            ["Persona", "Count"],
        )
    )

    lines.extend(
        [
            "",
            "## Opportunity Ownership by Motion",
            "",
        ]
    )
    lines.extend(
        format_table_moneyless(
            [
                {"Type": opp_type, **counts}
                for opp_type, counts in profile["opportunity_persona_by_type"].items()
            ],
            ["Type", "Sales", "CX", "Services", "Marketing", "Leadership", "Other", "Unknown"],
        )
    )

    lines.extend(
        [
            "",
            "## Top Opportunity Owners",
            "",
        ]
    )
    lines.extend(
        format_table_moneyless(
            profile["top_opportunity_owners"][:15],
            ["Name", "Persona", "Title", "UserRole", "Division", "ManagerName", "OpportunityCount"],
        )
    )

    lines.extend(
        [
            "",
            "## Top Account Owners",
            "",
        ]
    )
    lines.extend(
        format_table_moneyless(
            profile["top_account_owners"][:15],
            ["Name", "Persona", "Title", "UserRole", "Division", "ManagerName", "AccountCount"],
        )
    )

    lines.extend(
        [
            "",
            "## Manager Rollup",
            "",
        ]
    )
    lines.extend(
        format_table_moneyless(
            [
                {
                    "ManagerName": row["ManagerName"],
                    "DirectReports": row["DirectReports"],
                    "OwnedOpportunities": row["OwnedOpportunities"],
                    "OwnedAccounts": row["OwnedAccounts"],
                    "PersonaCounts": row["PersonaCounts"],
                }
                for row in profile["manager_rollup"][:15]
            ],
            ["ManagerName", "DirectReports", "OwnedOpportunities", "OwnedAccounts", "PersonaCounts"],
        )
    )

    if profile["mismatch_examples"]:
        lines.extend(
            [
                "",
                "## Mismatch Examples",
                "",
                "These are users where title-driven persona and role-driven persona disagree.",
                "",
            ]
        )
        lines.extend(
            format_table_moneyless(
                [
                    {
                        "Name": row["Name"],
                        "Title": row["Title"],
                        "UserRole": row["UserRole"],
                        "Division": row["Division"],
                        "RolePersona": row["RolePersona"],
                        "TitlePersona": row["TitlePersona"],
                    }
                    for row in profile["mismatch_examples"][:15]
                ],
                ["Name", "Title", "UserRole", "Division", "RolePersona", "TitlePersona"],
            )
        )

    if profile["duplicate_names"]:
        lines.extend(
            [
                "",
                "## Duplicate Active Names",
                "",
                "These names appear on multiple active standard users and require `Id`-based handling.",
                "",
            ]
        )
        lines.extend(
            format_table_moneyless(
                [{"Name": row["Name"], "Count": row["Count"]} for row in profile["duplicate_names"][:20]],
                ["Name", "Count"],
            )
        )

    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "- Use `OwnerId` / `Account.OwnerId` as the primary join key everywhere.",
            "- Derive persona from a combination of `UserRole.Name`, `Title`, `Division`, `Department`, and `ManagerId`.",
            "- Do not rely on `ManagerName` alone to split Sales vs CSM.",
            "- Add a durable user-role dimension for dashboard builders so `Sales Manager`, `CSM Manager`, and `Individual` surfaces can filter correctly.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile commercial role structures.")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("/Users/test/crm-analytics/docs/generated/role_structure_profile_2026-03-11"),
        help="Directory for profile.md and profile.json",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    return parser


def run_profile_command(
    out_dir: Path,
    *,
    emit_text: bool = True,
) -> tuple[dict[str, Any], int]:
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    inst, tok = get_auth()
    users = run_soql(inst, tok, USER_QUERY)
    opp_owner_rows = run_soql(inst, tok, OPP_OWNER_QUERY)
    account_owner_rows = run_soql(inst, tok, ACCOUNT_OWNER_QUERY)
    user_type_rows = run_soql(inst, tok, ACTIVE_USER_TYPE_QUERY)
    opp_owner_type_rows = run_soql(inst, tok, OPP_OWNER_TYPE_QUERY)

    profile = build_profile(
        users,
        opp_owner_rows,
        account_owner_rows,
        user_type_rows,
        opp_owner_type_rows,
    )

    json_path = out_dir / "profile.json"
    md_path = out_dir / "profile.md"
    json_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(profile), encoding="utf-8")

    if emit_text:
        print(f"Wrote role structure profile to {out_dir}")

    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote role structure profile to {out_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "internal_slice_count": profile["internal_slice_count"],
            "persona_counts": profile["persona_counts"],
            "region_counts": profile["region_counts"],
            "duplicate_name_count": len(profile["duplicate_names"]),
            "mismatch_example_count": len(profile["mismatch_examples"]),
            "output_dir": str(out_dir),
        },
        profile=profile,
    )
    return result, 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    result, exit_code = run_profile_command(
        args.out_dir,
        emit_text=not args.json,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
