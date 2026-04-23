#!/usr/bin/env python3
"""Profile quote-line product evidence for the NA/AMERS BDR universe.

Goal:
- validate whether Apttus quote line items can materially improve product
  hypothesis coverage on BDR-owned prospect/current/former-client accounts
- compare quote-line coverage to the current account/opportunity product signals
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm_analytics_helpers import _soql, get_auth  # noqa: E402

BDR_USER_SOQL = (
    "SELECT Id, Name, Title, Department, Manager.Name "
    "FROM User "
    "WHERE IsActive = true AND ("
    "Title LIKE '%Business Development Representative%' OR "
    "Title LIKE '%Senior Business Development Representative%' OR "
    "Title LIKE '%Lead Business Development Representative%' OR "
    "Title LIKE '%Senior Manager Business Development%' OR "
    "Title LIKE '%Sr.Manager Business Development%') "
    "ORDER BY Department, Title, Name"
)

ACCOUNT_SOQL_TEMPLATE = (
    "SELECT Id, Name, OwnerId, Owner.Name, Type, Industry, Region__c, TAM_Universe_Segment__c, Tier_Calculation__c, "
    "Finance_Client__c, Ex_Customer__c, SaaS_Client__c, Axioma_Client__c, Customer_Segment__c, "
    "Product_Opportunity__c, Product_Mainline__c, Heat_Map_Red_Lostdate__c, TM_Account_Status__c, "
    "Ex_Customer_Prospecting_Date__c, C_Level_Personas__c, H_Level_Personas__c, Persona_Contacts__c, Unique_Personas__c, Non_Persona_Contacts__c "
    "FROM Account "
    "WHERE OwnerId IN ({owner_ids})"
)

OPP_SOQL_TEMPLATE = (
    "SELECT Id, Name, AccountId, LeadSource, StageName, ForecastCategoryName, Type, IsClosed, IsWon, "
    "CreatedDate, CreatedById, CreatedBy.Name, CreatedBy.Title, "
    "OwnerId, Owner.Name, Owner.Title, "
    "CloseDate, convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
    "New_Stage_15_Date__c, New_Stage_20_Date__c, "
    "New_Stage_15_Score__c, New_Stage_20_Score__c, "
    "Submit_for_Stage_20_Review__c, Submit_for_Stage_20_Review_Date__c, "
    "Stage_20_Approval__c, Stage_20_Approval_Date__c, "
    "HasOverdueTask, LastStageChangeInDays, Stage_with_Product_Scope__c, APTS_RH_Product_Family__c "
    "FROM Opportunity "
    "WHERE AccountId IN ({account_ids})"
)

LINE_QUERY_TEMPLATE = """
SELECT Id,
       Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__c,
       Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.AccountId,
       Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.Account.Name,
       Apttus_Proposal__Product__r.Name,
       APTS_Product_Area__c,
       APTS_Strategic_Product__c,
       APTS_Net_Product_Price__c,
       Apttus_QPConfig__NetPrice__c,
       Apttus_QPConfig__StartDate__c,
       Apttus_QPConfig__EndDate__c
FROM Apttus_Proposal__Proposal_Line_Item__c
WHERE Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__c IN ({opp_ids})
"""

DEFAULT_OUT_DIR = Path("/Users/test/crm-analytics/docs/generated/bdr_quote_product_signal_profile_2026-03-13")


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
        "tool": "profile_bdr_quote_product_signals",
        "lane": "salesforce_data_profiles",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clean_multivalue_text(value: object) -> list[str]:
    raw = _clean_text(value)
    if not raw:
        return []
    parts: list[str] = []
    for chunk in raw.replace("|", ";").split(";"):
        text = chunk.strip()
        if text:
            parts.append(text)
    return parts


def _normalize_product_token(value: object) -> str:
    token = _clean_text(value)
    if not token:
        return ""
    lowered = token.lower()
    if lowered in {"unknown", "all", "null", "-"}:
        return ""
    if "simcorp saas" in lowered or lowered == "xaas" or " xaas" in lowered:
        return "SimCorp SaaS / XaaS"
    if "scd software" in lowered:
        return "SCD Software"
    if "analytics services" in lowered:
        return "Analytics Services"
    if "data management services" in lowered:
        return "Data Management Services"
    if "white label" in lowered:
        return "White Label"
    if "3rd party" in lowered or "third party" in lowered:
        return "3rd Party"
    if lowered == "mbo":
        return "MBO"
    if lowered == "ims":
        return "IMS Mainline"
    return token[:255]


def _product_priority(token: str) -> int:
    lowered = token.lower()
    if "simcorp saas / xaas" in lowered:
        return 100
    if "scd software" in lowered:
        return 95
    if "data management services" in lowered:
        return 90
    if "analytics services" in lowered:
        return 85
    if lowered == "mbo":
        return 80
    if "ims mainline" in lowered:
        return 20
    if "white label" in lowered:
        return 10
    if "3rd party" in lowered:
        return 5
    return 60


def _all_product_signals(value: object) -> list[str]:
    normalized: list[str] = []
    for token in _clean_multivalue_text(value):
        clean = _normalize_product_token(token)
        if clean and clean not in normalized:
            normalized.append(clean)
    return sorted(normalized, key=lambda item: (-_product_priority(item), item))


def _team_from_department(value: object) -> str:
    department = str(value or "").lower()
    if "apac" in department:
        return "APAC"
    if "emea" in department or "value advisory" in department:
        return "EMEA"
    if "na sales" in department or "north america" in department or "us (" in department:
        return "AMERS"
    if "axioma" in department:
        return "Axioma"
    return "Shared BDR"


def _queue_team(queue_name: str) -> str:
    upper = queue_name.upper()
    if "APAC" in upper:
        return "APAC"
    if "EMEA" in upper:
        return "EMEA"
    if "AMERS" in upper or "NA" in upper:
        return "AMERS"
    return "Shared BDR"


def _chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def _quoted(values: list[str]) -> str:
    return ",".join(f"'{value}'" for value in values)


def build_account_query(owner_ids: list[str]) -> str:
    return ACCOUNT_SOQL_TEMPLATE.format(owner_ids=_quoted(owner_ids))


def build_opportunity_query(account_ids: list[str]) -> str:
    return OPP_SOQL_TEMPLATE.format(account_ids=_quoted(account_ids))


def build_profile(
    users: list[dict[str, Any]],
    accounts: list[dict[str, Any]],
    opps: list[dict[str, Any]],
    line_items: list[dict[str, Any]],
) -> dict[str, Any]:
    amers_user_ids = {
        u["Id"]
        for u in users
        if _queue_team(_team_from_department((u.get("Department") or ""))) == "AMERS"
        or _team_from_department((u.get("Department") or "")) == "AMERS"
    }

    amers_accounts = [a for a in accounts if a.get("OwnerId") in amers_user_ids]
    amers_account_ids = {a["Id"] for a in amers_accounts}
    account_by_id = {a["Id"]: a for a in amers_accounts}
    amers_opps = [o for o in opps if o.get("AccountId") in amers_account_ids]

    opp_raw_nonunknown = {
        o["Id"]
        for o in amers_opps
        if _all_product_signals(o.get("APTS_RH_Product_Family__c"))
    }
    opp_scope_nonempty = {
        o["Id"]
        for o in amers_opps
        if _clean_text(o.get("Stage_with_Product_Scope__c"))
    }
    account_product_opportunity_nonunknown = {
        a["Id"]
        for a in amers_accounts
        if _all_product_signals(a.get("Product_Opportunity__c"))
    }

    line_area_counter: Counter[str] = Counter()
    strategic_counter: Counter[str] = Counter()
    quote_product_name_counter: Counter[str] = Counter()
    opp_quote_area_presence: set[str] = set()
    account_quote_area_presence: set[str] = set()
    account_quote_area_only: set[str] = set()
    account_to_quote_areas: dict[str, set[str]] = defaultdict(set)

    for line in line_items:
        opp_id = ((line.get("Apttus_Proposal__Proposal__r") or {}).get("Apttus_Proposal__Opportunity__c")) or ""
        acct_id = ((line.get("Apttus_Proposal__Proposal__r") or {}).get("Apttus_Proposal__Opportunity__r") or {}).get("AccountId") or ""
        for area in _all_product_signals(line.get("APTS_Product_Area__c")):
            line_area_counter[area] += 1
            if opp_id:
                opp_quote_area_presence.add(opp_id)
            if acct_id:
                account_quote_area_presence.add(acct_id)
                account_to_quote_areas[acct_id].add(area)
        for strategic in _all_product_signals(line.get("APTS_Strategic_Product__c")):
            strategic_counter[strategic] += 1
        product_name = _clean_text(((line.get("Apttus_Proposal__Product__r") or {}).get("Name")) or "")
        if product_name:
            quote_product_name_counter[product_name] += 1

    for acct_id in account_quote_area_presence:
        if acct_id not in account_product_opportunity_nonunknown:
            related_opp_ids = [o["Id"] for o in amers_opps if o.get("AccountId") == acct_id]
            has_raw = any(oid in opp_raw_nonunknown for oid in related_opp_ids)
            has_scope = any(oid in opp_scope_nonempty for oid in related_opp_ids)
            if not has_raw and not has_scope:
                account_quote_area_only.add(acct_id)

    top_accounts_quote_only = []
    for acct_id in sorted(account_quote_area_only)[:25]:
        acct = account_by_id.get(acct_id, {})
        top_accounts_quote_only.append(
            {
                "AccountId": acct_id,
                "AccountName": acct.get("Name") or "",
                "Industry": acct.get("Industry") or "",
                "ProductOpportunity": acct.get("Product_Opportunity__c") or "",
                "QuoteAreas": sorted(account_to_quote_areas.get(acct_id, set())),
            }
        )

    return {
        "scope": "AMERS BDR-owned accounts",
        "counts": {
            "amers_bdr_users": len(amers_user_ids),
            "amers_accounts": len(amers_accounts),
            "amers_opportunities": len(amers_opps),
            "amers_quote_line_items": len(line_items),
        },
        "coverage": {
            "accounts_with_account_product_opportunity": len(account_product_opportunity_nonunknown),
            "opps_with_raw_opportunity_product": len(opp_raw_nonunknown),
            "opps_with_stage_scope": len(opp_scope_nonempty),
            "opps_with_quote_product_area": len(opp_quote_area_presence),
            "accounts_with_quote_product_area": len(account_quote_area_presence),
            "accounts_quote_area_only": len(account_quote_area_only),
        },
        "top_quote_product_areas": line_area_counter.most_common(20),
        "top_strategic_products": strategic_counter.most_common(20),
        "top_quote_product_names": quote_product_name_counter.most_common(20),
        "quote_area_only_accounts": top_accounts_quote_only,
    }


def write_markdown(payload: dict[str, Any], output_path: Path) -> None:
    counts = payload["counts"]
    coverage = payload["coverage"]
    lines = [
        "# BDR Quote Product Signal Profile",
        "",
        "Scope: AMERS BDR-owned account universe",
        "",
        "## Counts",
        f"- AMERS BDR users: `{counts['amers_bdr_users']}`",
        f"- AMERS accounts: `{counts['amers_accounts']}`",
        f"- AMERS opportunities: `{counts['amers_opportunities']}`",
        f"- Quote line items on those opportunities: `{counts['amers_quote_line_items']}`",
        "",
        "## Coverage",
        f"- Accounts with `Product_Opportunity__c`: `{coverage['accounts_with_account_product_opportunity']}`",
        f"- Opportunities with raw `APTS_RH_Product_Family__c`: `{coverage['opps_with_raw_opportunity_product']}`",
        f"- Opportunities with `Stage_with_Product_Scope__c`: `{coverage['opps_with_stage_scope']}`",
        f"- Opportunities with quote product area: `{coverage['opps_with_quote_product_area']}`",
        f"- Accounts with quote product area: `{coverage['accounts_with_quote_product_area']}`",
        f"- Accounts where quote product area is the only meaningful product signal: `{coverage['accounts_quote_area_only']}`",
        "",
        "## Top Quote Product Areas",
    ]
    lines.extend([f"- `{name}`: `{count}`" for name, count in payload["top_quote_product_areas"]])
    lines.extend(["", "## Top Strategic Products"])
    lines.extend([f"- `{name}`: `{count}`" for name, count in payload["top_strategic_products"]])
    lines.extend(["", "## Top Accounts With Quote-Area-Only Signal"])
    for row in payload["quote_area_only_accounts"]:
        lines.append(
            f"- `{row['AccountName']}` | `{row['Industry']}` | quote areas = `{', '.join(row['QuoteAreas'])}` | "
            f"account product = `{row['ProductOpportunity'] or 'Unknown'}`"
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile quote-line product evidence for the AMERS BDR universe.")
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

    inst, tok = get_auth()
    users = _soql(inst, tok, BDR_USER_SOQL)

    amers_user_ids = sorted(
        {
        u["Id"]
        for u in users
        if _queue_team(_team_from_department((u.get("Department") or ""))) == "AMERS"
        or _team_from_department((u.get("Department") or "")) == "AMERS"
        }
    )

    accounts: list[dict[str, Any]] = []
    for chunk in _chunked(amers_user_ids, 100):
        accounts.extend(_soql(inst, tok, build_account_query(chunk)))

    amers_account_ids = sorted({a["Id"] for a in accounts if a.get("Id")})

    opps: list[dict[str, Any]] = []
    for chunk in _chunked(amers_account_ids, 150):
        opps.extend(_soql(inst, tok, build_opportunity_query(chunk)))

    amers_opp_ids = [o["Id"] for o in opps if o.get("AccountId") in amers_account_ids]

    line_items: list[dict[str, Any]] = []
    for chunk in _chunked(amers_opp_ids, 150):
        ids = ",".join(f"'{oid}'" for oid in chunk)
        line_items.extend(_soql(inst, tok, LINE_QUERY_TEMPLATE.format(opp_ids=ids)))

    profile = build_profile(users, accounts, opps, line_items)

    json_path = out_dir / "profile.json"
    md_path = out_dir / "profile.md"
    json_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    write_markdown(profile, md_path)

    if emit_text:
        print(f"Wrote BDR quote product signal profile to {out_dir}")

    counts = profile["counts"]
    coverage = profile["coverage"]
    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote BDR quote product signal profile to {out_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "amers_bdr_user_count": counts["amers_bdr_users"],
            "amers_account_count": counts["amers_accounts"],
            "amers_opportunity_count": counts["amers_opportunities"],
            "quote_line_item_count": counts["amers_quote_line_items"],
            "quote_area_only_account_count": coverage["accounts_quote_area_only"],
            "top_quote_product_area_count": len(profile["top_quote_product_areas"]),
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
