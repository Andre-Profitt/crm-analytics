#!/usr/bin/env python3
"""Determine the right grain for product-informed retention classification.

This is intentionally analysis-only. It profiles three candidate grains:

1. Account level: business story / base-protection truth
2. Opportunity level: event classification using existing opp fields
3. Quote level: supporting product evidence from Apttus proposals and line items

It does not change datasets or dashboards.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crm_analytics_helpers import get_auth
from build_revenue_retention_health import run_soql


ANALYSIS_START = "2023-01-01"
REPRESENTATIVE_ACCOUNTS = (
    "AllianceBernstein L.P.",
    "FIIG Securities",
    "Nykredit Realkredit A/S",
    "PFA Asset Management",
    "Finanz Informatik GmbH & Co. KG",
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
        "tool": "profile_retention_product_grain",
        "lane": "salesforce_data_profiles",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload

OPP_QUERY = (
    "SELECT Id, Name, Account.Name, Type, IsClosed, IsWon, StageName, CloseDate, "
    "APTS_RH_Product_Family__c, APTS_Primary_Quote__c "
    "FROM Opportunity "
    "WHERE Type IN ('Land','Expand','Renewal') "
    f"AND CloseDate >= {ANALYSIS_START}"
)

LINE_ITEM_AGG_QUERY = (
    "SELECT Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.Type oppType, "
    "COUNT(Id) lineCount, "
    "COUNT(Apttus_QPConfig__StartDate__c) withStart, "
    "COUNT(Apttus_QPConfig__EndDate__c) withEnd, "
    "COUNT(APTS_Product_Area__c) withProductArea "
    "FROM Apttus_Proposal__Proposal_Line_Item__c "
    "WHERE Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.Type IN ('Land','Expand','Renewal') "
    f"AND Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.CloseDate >= {ANALYSIS_START} "
    "GROUP BY Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.Type"
)

ACCOUNT_OPP_QUERY = (
    "SELECT Id, Name, Account.Name, Type, IsWon, IsClosed, StageName, CloseDate, "
    "APTS_RH_Product_Family__c, APTS_Opportunity_Sub_Type__c, "
    "APTS_Primary_Quote__c, APTS_Primary_Quote__r.Name "
    "FROM Opportunity "
    f"WHERE Account.Name IN {tuple(REPRESENTATIVE_ACCOUNTS)} "
    "AND Type IN ('Land','Expand','Renewal') "
    f"AND CloseDate >= {ANALYSIS_START} "
    "ORDER BY Account.Name, CloseDate"
)

ACCOUNT_LINE_QUERY = (
    "SELECT Id, "
    "Apttus_Proposal__Proposal__r.Name, "
    "Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.Name, "
    "Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.Account.Name, "
    "Apttus_Proposal__Product__r.Name, "
    "APTS_Product_Area__c, "
    "APTS_Strategic_Product__c, "
    "Apttus_QPConfig__StartDate__c, "
    "Apttus_QPConfig__EndDate__c "
    "FROM Apttus_Proposal__Proposal_Line_Item__c "
    f"WHERE Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.Account.Name IN {tuple(REPRESENTATIVE_ACCOUNTS)} "
    "ORDER BY Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.Account.Name, "
    "Apttus_Proposal__Proposal__r.Name"
)


@dataclass
class GrainRecommendation:
    account_level_role: str
    opportunity_level_role: str
    quote_level_role: str
    primary_grain: str
    model_shape: str


def pct(numerator: int, denominator: int) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator


def summarize_opp_coverage(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    by_state: dict[str, dict[str, Any]] = {}
    for row in rows:
        opp_type = row.get("Type") or "Unknown"
        opp = bool(row.get("APTS_RH_Product_Family__c"))
        quote = bool(row.get("APTS_Primary_Quote__c"))

        bucket = summary.setdefault(
            opp_type,
            {"total": 0, "both": 0, "quote_only": 0, "opp_only": 0, "neither": 0},
        )
        bucket["total"] += 1
        if opp and quote:
            bucket["both"] += 1
        elif quote:
            bucket["quote_only"] += 1
        elif opp:
            bucket["opp_only"] += 1
        else:
            bucket["neither"] += 1

        state_key = f"{opp_type}|closed={row.get('IsClosed')}|won={row.get('IsWon')}"
        state_bucket = by_state.setdefault(
            state_key,
            {
                "total": 0,
                "with_opp_product": 0,
                "with_primary_quote": 0,
                "both": 0,
                "quote_only": 0,
                "opp_only": 0,
                "neither": 0,
            },
        )
        state_bucket["total"] += 1
        state_bucket["with_opp_product"] += int(opp)
        state_bucket["with_primary_quote"] += int(quote)
        if opp and quote:
            state_bucket["both"] += 1
        elif quote:
            state_bucket["quote_only"] += 1
        elif opp:
            state_bucket["opp_only"] += 1
        else:
            state_bucket["neither"] += 1

    for bucket in summary.values():
        total = bucket["total"]
        bucket["both_pct"] = pct(bucket["both"], total)
        bucket["quote_only_pct"] = pct(bucket["quote_only"], total)
        bucket["opp_only_pct"] = pct(bucket["opp_only"], total)
        bucket["neither_pct"] = pct(bucket["neither"], total)

    for bucket in by_state.values():
        total = bucket["total"]
        bucket["with_opp_product_pct"] = pct(bucket["with_opp_product"], total)
        bucket["with_primary_quote_pct"] = pct(bucket["with_primary_quote"], total)

    return {"by_type": summary, "by_state": by_state}


def summarize_line_coverage(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for row in rows:
        opp_type = row.get("oppType") or "Unknown"
        line_count = int(row.get("lineCount") or 0)
        with_start = int(row.get("withStart") or 0)
        with_end = int(row.get("withEnd") or 0)
        with_area = int(row.get("withProductArea") or 0)
        summary[opp_type] = {
            "line_count": line_count,
            "with_start": with_start,
            "with_end": with_end,
            "with_product_area": with_area,
            "with_start_pct": pct(with_start, line_count),
            "with_end_pct": pct(with_end, line_count),
            "with_product_area_pct": pct(with_area, line_count),
        }
    return summary


def summarize_representative_accounts(
    opp_rows: list[dict[str, Any]], line_rows: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    account_summary: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "opp_count": 0,
            "opp_with_primary_quote": 0,
            "opp_with_product_family": 0,
            "won_renewal_products": set(),
            "won_expand_products": set(),
            "quote_product_areas": set(),
            "quote_products": set(),
        }
    )
    for row in opp_rows:
        account = (row.get("Account") or {}).get("Name") or "Unknown"
        bucket = account_summary[account]
        bucket["opp_count"] += 1
        bucket["opp_with_primary_quote"] += int(bool(row.get("APTS_Primary_Quote__c")))
        bucket["opp_with_product_family"] += int(bool(row.get("APTS_RH_Product_Family__c")))
        if row.get("IsWon") and row.get("Type") == "Renewal" and row.get("APTS_RH_Product_Family__c"):
            bucket["won_renewal_products"].update(
                p.strip() for p in (row.get("APTS_RH_Product_Family__c") or "").split(";") if p.strip()
            )
        if row.get("IsWon") and row.get("Type") == "Expand" and row.get("APTS_RH_Product_Family__c"):
            bucket["won_expand_products"].update(
                p.strip() for p in (row.get("APTS_RH_Product_Family__c") or "").split(";") if p.strip()
            )

    for row in line_rows:
        account = (
            ((row.get("Apttus_Proposal__Proposal__r") or {})
             .get("Apttus_Proposal__Opportunity__r") or {})
            .get("Account", {})
            .get("Name")
            or "Unknown"
        )
        bucket = account_summary[account]
        area = row.get("APTS_Product_Area__c")
        product = (row.get("Apttus_Proposal__Product__r") or {}).get("Name")
        if area:
            bucket["quote_product_areas"].add(area)
        if product:
            bucket["quote_products"].add(product)

    final: dict[str, dict[str, Any]] = {}
    for account, bucket in account_summary.items():
        final[account] = {
            "opp_count": bucket["opp_count"],
            "opp_with_primary_quote": bucket["opp_with_primary_quote"],
            "opp_with_product_family": bucket["opp_with_product_family"],
            "won_renewal_products": sorted(bucket["won_renewal_products"]),
            "won_expand_products": sorted(bucket["won_expand_products"]),
            "quote_product_areas_sample": sorted(bucket["quote_product_areas"])[:12],
            "quote_product_sample": sorted(bucket["quote_products"])[:12],
        }
    return final


def build_recommendation(opp_summary: dict[str, Any], line_summary: dict[str, Any]) -> GrainRecommendation:
    renewal_by_type = opp_summary["by_type"]["Renewal"]
    expand_by_type = opp_summary["by_type"]["Expand"]
    quote_level_reliable_for_renewal = renewal_by_type["both_pct"] + renewal_by_type["quote_only_pct"] > 0.9
    quote_level_partial_for_expand = expand_by_type["both_pct"] + expand_by_type["quote_only_pct"] < 0.7
    if quote_level_reliable_for_renewal and quote_level_partial_for_expand:
        model_shape = "account narrative + opportunity event classification + quote-assisted product evidence"
    else:
        model_shape = "account narrative + opportunity event classification"
    return GrainRecommendation(
        account_level_role="Primary business-truth grain for retained base, protected base, churn, and account story.",
        opportunity_level_role="Primary event-classification grain for renewal vs expand vs land, close timing, stage, and forecast motion.",
        quote_level_role="Supporting product-evidence grain when a primary quote exists; strongest for renewal windows, partial for expand.",
        primary_grain="Account-year / account-timeline",
        model_shape=model_shape,
    )


def render_markdown(
    opp_summary: dict[str, Any],
    line_summary: dict[str, Any],
    account_summary: dict[str, Any],
    recommendation: GrainRecommendation,
) -> str:
    lines = [
        "# Retention Product Grain Recommendation",
        "",
        "## Verdict",
        "",
        f"- Primary grain: `{recommendation.primary_grain}`",
        f"- Model shape: `{recommendation.model_shape}`",
        f"- Account level: {recommendation.account_level_role}",
        f"- Opportunity level: {recommendation.opportunity_level_role}",
        f"- Quote level: {recommendation.quote_level_role}",
        "",
        "## Opportunity Coverage By Type",
        "",
        "| Type | Total | Both opp+quote | Quote only | Opp only | Neither |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for opp_type, stats in opp_summary["by_type"].items():
        lines.append(
            f"| {opp_type} | {stats['total']:,} | {stats['both']} ({stats['both_pct']:.1%}) | "
            f"{stats['quote_only']} ({stats['quote_only_pct']:.1%}) | "
            f"{stats['opp_only']} ({stats['opp_only_pct']:.1%}) | "
            f"{stats['neither']} ({stats['neither_pct']:.1%}) |"
        )

    lines.extend(
        [
            "",
            "## Opportunity Coverage By Type / Outcome",
            "",
            "| Bucket | Total | Opp product | Primary quote |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for bucket, stats in sorted(opp_summary["by_state"].items()):
        lines.append(
            f"| {bucket} | {stats['total']:,} | "
            f"{stats['with_opp_product']} ({stats['with_opp_product_pct']:.1%}) | "
            f"{stats['with_primary_quote']} ({stats['with_primary_quote_pct']:.1%}) |"
        )

    lines.extend(
        [
            "",
            "## Quote Line Coverage",
            "",
            "| Opportunity Type | Line count | Product area populated | Start date populated | End date populated |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for opp_type, stats in sorted(line_summary.items()):
        lines.append(
            f"| {opp_type} | {stats['line_count']:,} | "
            f"{stats['with_product_area']} ({stats['with_product_area_pct']:.1%}) | "
            f"{stats['with_start']} ({stats['with_start_pct']:.1%}) | "
            f"{stats['with_end']} ({stats['with_end_pct']:.1%}) |"
        )

    lines.extend(
        [
            "",
            "## Representative Account Readout",
            "",
        ]
    )
    for account, stats in account_summary.items():
        lines.extend(
            [
                f"### {account}",
                f"- Opportunities in sample: `{stats['opp_count']}`",
                f"- Opps with primary quote: `{stats['opp_with_primary_quote']}`",
                f"- Opps with opportunity product family: `{stats['opp_with_product_family']}`",
                f"- Won renewal opportunity products: `{', '.join(stats['won_renewal_products']) or 'none'}`",
                f"- Won expand opportunity products: `{', '.join(stats['won_expand_products']) or 'none'}`",
                f"- Quote product areas sample: `{', '.join(stats['quote_product_areas_sample']) or 'none'}`",
                f"- Quote product sample: `{', '.join(stats['quote_product_sample']) or 'none'}`",
                "",
            ]
        )

    lines.extend(
        [
            "## Interpretation",
            "",
            "- Account level is the only grain that can express the business truth: did this customer preserve base, churn, expand, or migrate.",
            "- Opportunity level is still required for classifying events because CRM process lives there: `Renewal`, `Expand`, `Land`, stage, forecast category, and close timing.",
            "- Quote level is too incomplete to be the main grain, especially for `Land` and `Expand`, but it is the best product-evidence layer when a primary quote exists.",
            "- Renewal opportunities are the cleanest place to use quote-level product evidence because primary-quote coverage is high and line items are well populated.",
            "- Expand opportunities are too mixed to make quote the primary grain. Many have no quote or no opp product family, so the model must tolerate missing product evidence.",
            "",
            "## Recommended Modeling Rule",
            "",
            "1. Build the retention classifier at `account x year` (and eventually account timeline) level.",
            "2. Score each opportunity as an event using opportunity fields first.",
            "3. When a primary quote exists, use proposal line items to strengthen product-overlap / migration evidence.",
            "4. If quote evidence is absent, fall back to opportunity `APTS_RH_Product_Family__c`.",
            "5. If neither exists, keep the revenue in `unclassified` rather than faking precision.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/Users/test/crm-analytics/docs/generated/retention_product_grain_2026-03-11"),
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

    inst, tok = get_auth()
    opp_rows = run_soql(inst, tok, OPP_QUERY)
    line_aggs = run_soql(inst, tok, LINE_ITEM_AGG_QUERY)
    account_opp_rows = run_soql(inst, tok, ACCOUNT_OPP_QUERY)
    account_line_rows = run_soql(inst, tok, ACCOUNT_LINE_QUERY)

    opp_summary = summarize_opp_coverage(opp_rows)
    line_summary = summarize_line_coverage(line_aggs)
    account_summary = summarize_representative_accounts(account_opp_rows, account_line_rows)
    recommendation = build_recommendation(opp_summary, line_summary)

    payload = {
        "opportunity_coverage": opp_summary,
        "quote_line_coverage": line_summary,
        "representative_accounts": account_summary,
        "recommendation": asdict(recommendation),
    }
    json_path = output_dir / "profile.json"
    md_path = output_dir / "profile.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(
        render_markdown(opp_summary, line_summary, account_summary, recommendation),
        encoding="utf-8",
    )

    if emit_text:
        print(output_dir)

    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote retention product grain profile to {output_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "primary_grain": payload["recommendation"]["primary_grain"],
            "model_shape": payload["recommendation"]["model_shape"],
            "representative_account_count": len(payload["representative_accounts"]),
            "opportunity_types_profiled": sorted(payload["opportunity_coverage"]["by_type"]),
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
