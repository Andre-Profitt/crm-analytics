#!/usr/bin/env python3
"""Profile retention semantics for the SimCorp CRM process.

This script does not change dashboards. It profiles how renewal-like revenue is
actually recorded in Opportunity data so we can define robust retention formulas
before more dashboard work.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crm_analytics_helpers import get_auth
from build_revenue_retention_health import run_soql, safe_float


PROFILE_QUERY = (
    "SELECT Id, Name, AccountId, Account.Name, Type, StageName, IsWon, IsClosed, "
    "CloseDate, CreatedDate, ForecastCategoryName, Account_Type__c, "
    "APTS_Opportunity_Sub_Type__c, APTS_Primary_Quote_Type__c, "
    "APTS_Opportunity_ARR__c, APTS_Renewal_ACV__c, Amount, "
    "APTS_RH_Product_Family__c, APTS_Contract_Start_Date__c, APTS_Contract_End_Date__c "
    "FROM Opportunity "
    "WHERE Type IN ('Land','Expand','Renewal') "
    "AND CloseDate >= 2023-01-01 "
    "ORDER BY AccountId, CloseDate"
)

ANALYSIS_START_YEAR = 2024
RENEWALISH_TOKENS = (
    "renewal",
    "extension",
    "opt out",
    "continuous testing",
    "arr increase",
    "contract",
    "prolong",
    "swap",
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
        "tool": "profile_retention_semantics",
        "lane": "salesforce_data_profiles",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


@dataclass
class CandidateMatch:
    renewal_id: str
    renewal_name: str
    renewal_close_date: str
    account_name: str
    renewal_value: float
    renewal_stage: str
    candidate_id: str
    candidate_name: str
    candidate_type: str
    candidate_close_date: str
    candidate_arr: float
    days_delta: int
    product_overlap: bool
    renewalish_name: bool
    confidence: str


def parse_date(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def outcome(row: dict[str, Any]) -> str:
    if row.get("IsWon"):
        return "won"
    if not row.get("IsClosed"):
        return "open"
    stage = row.get("StageName") or ""
    if "No Opportunity" in stage:
        return "no_opportunity"
    if "Lost" in stage:
        return "lost"
    return "closed_not_won"


def product_set(row: dict[str, Any]) -> set[str]:
    raw = row.get("APTS_RH_Product_Family__c") or ""
    return {part.strip() for part in raw.split(";") if part.strip()}


def renewal_value(row: dict[str, Any]) -> float:
    """Best available value for retention semantics, not dashboard math."""
    if row.get("Type") == "Renewal":
        return (
            safe_float(row.get("APTS_Renewal_ACV__c"))
            or safe_float(row.get("APTS_Opportunity_ARR__c"))
            or safe_float(row.get("Amount"))
        )
    return safe_float(row.get("APTS_Opportunity_ARR__c")) or safe_float(row.get("Amount"))


def is_customer(row: dict[str, Any]) -> bool:
    return (row.get("Account_Type__c") or "") == "Customer"


def is_analysis_period(row: dict[str, Any]) -> bool:
    close_date = parse_date(row.get("CloseDate") or "")
    return bool(close_date and close_date.year >= ANALYSIS_START_YEAR)


def score_candidate(renewal_row: dict[str, Any], candidate_row: dict[str, Any]) -> CandidateMatch | None:
    renewal_date = parse_date(renewal_row.get("CloseDate") or "")
    candidate_date = parse_date(candidate_row.get("CloseDate") or "")
    if not renewal_date or not candidate_date:
        return None

    days_delta = (candidate_date - renewal_date).days
    if abs(days_delta) > 365:
        return None

    arr = safe_float(candidate_row.get("APTS_Opportunity_ARR__c"))
    if arr <= 0:
        return None

    overlap = bool(product_set(renewal_row) & product_set(candidate_row))
    candidate_name = (candidate_row.get("Name") or "").lower()
    renewalish_name = any(token in candidate_name for token in RENEWALISH_TOKENS)

    if abs(days_delta) <= 90 and overlap:
        confidence = "high"
    elif abs(days_delta) <= 180 and (overlap or renewalish_name):
        confidence = "medium"
    elif abs(days_delta) <= 180:
        confidence = "low"
    else:
        return None

    return CandidateMatch(
        renewal_id=renewal_row.get("Id") or "",
        renewal_name=renewal_row.get("Name") or "",
        renewal_close_date=renewal_row.get("CloseDate") or "",
        account_name=(renewal_row.get("Account") or {}).get("Name", ""),
        renewal_value=renewal_value(renewal_row),
        renewal_stage=renewal_row.get("StageName") or "",
        candidate_id=candidate_row.get("Id") or "",
        candidate_name=candidate_row.get("Name") or "",
        candidate_type=candidate_row.get("Type") or "",
        candidate_close_date=candidate_row.get("CloseDate") or "",
        candidate_arr=arr,
        days_delta=days_delta,
        product_overlap=overlap,
        renewalish_name=renewalish_name,
        confidence=confidence,
    )


def summarize_field_quality(rows: list[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not is_analysis_period(row):
            continue
        key = f"{row.get('Type')}|{outcome(row)}"
        bucket = stats.setdefault(
            key,
            {
                "count": 0,
                "arr_nonzero": 0,
                "renewal_acv_nonzero": 0,
                "amount_nonzero": 0,
                "arr_sum": 0.0,
                "renewal_acv_sum": 0.0,
                "amount_sum": 0.0,
            },
        )
        arr = safe_float(row.get("APTS_Opportunity_ARR__c"))
        racv = safe_float(row.get("APTS_Renewal_ACV__c"))
        amt = safe_float(row.get("Amount"))
        bucket["count"] += 1
        bucket["arr_nonzero"] += int(arr > 0)
        bucket["renewal_acv_nonzero"] += int(racv > 0)
        bucket["amount_nonzero"] += int(amt > 0)
        bucket["arr_sum"] += arr
        bucket["renewal_acv_sum"] += racv
        bucket["amount_sum"] += amt
    return stats


def summarize_customer_closed_won(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0, "arr": 0.0, "renewal_acv": 0.0, "amount": 0.0})
    for row in rows:
        if not (is_analysis_period(row) and is_customer(row) and row.get("IsClosed") and row.get("IsWon")):
            continue
        bucket = summary[row.get("Type") or "Unknown"]
        bucket["count"] += 1
        bucket["arr"] += safe_float(row.get("APTS_Opportunity_ARR__c"))
        bucket["renewal_acv"] += safe_float(row.get("APTS_Renewal_ACV__c"))
        bucket["amount"] += safe_float(row.get("Amount"))
    return summary


def summarize_expand_customer(rows: list[dict[str, Any]]) -> dict[str, Any]:
    subtype: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0, "arr": 0.0})
    quote_type: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0, "arr": 0.0})
    renewalish = {"count": 0, "arr": 0.0}
    for row in rows:
        if not (
            is_analysis_period(row)
            and is_customer(row)
            and row.get("Type") == "Expand"
            and row.get("IsClosed")
            and row.get("IsWon")
        ):
            continue
        arr = safe_float(row.get("APTS_Opportunity_ARR__c"))
        subtype[row.get("APTS_Opportunity_Sub_Type__c") or "<null>"]["count"] += 1
        subtype[row.get("APTS_Opportunity_Sub_Type__c") or "<null>"]["arr"] += arr
        quote_type[row.get("APTS_Primary_Quote_Type__c") or "<null>"]["count"] += 1
        quote_type[row.get("APTS_Primary_Quote_Type__c") or "<null>"]["arr"] += arr
        name = (row.get("Name") or "").lower()
        if any(token in name for token in RENEWALISH_TOKENS):
            renewalish["count"] += 1
            renewalish["arr"] += arr
    return {
        "subtype": dict(sorted(subtype.items(), key=lambda item: item[1]["arr"], reverse=True)),
        "quote_type": dict(sorted(quote_type.items(), key=lambda item: item[1]["arr"], reverse=True)),
        "renewalish_name_slice": renewalish,
    }


def renewal_amount_ratios(rows: list[dict[str, Any]]) -> dict[str, float]:
    ratios: list[float] = []
    for row in rows:
        if not (
            is_analysis_period(row)
            and row.get("Type") == "Renewal"
            and row.get("IsClosed")
            and safe_float(row.get("APTS_Renewal_ACV__c")) > 0
            and safe_float(row.get("Amount")) > 0
        ):
            continue
        ratios.append(safe_float(row.get("Amount")) / safe_float(row.get("APTS_Renewal_ACV__c")))
    ratios.sort()
    if not ratios:
        return {}
    def pct(p: float) -> float:
        idx = min(len(ratios) - 1, max(0, math.floor((len(ratios) - 1) * p)))
        return ratios[idx]
    return {
        "count": len(ratios),
        "median_amount_to_renewal_acv": median(ratios),
        "p25_amount_to_renewal_acv": pct(0.25),
        "p75_amount_to_renewal_acv": pct(0.75),
        "p90_amount_to_renewal_acv": pct(0.90),
    }


def build_candidate_matches(rows: list[dict[str, Any]]) -> list[CandidateMatch]:
    by_account: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if is_analysis_period(row) and row.get("AccountId"):
            by_account[row.get("AccountId")].append(row)

    matches: list[CandidateMatch] = []
    for account_rows in by_account.values():
        renewal_losses = [
            row for row in account_rows
            if is_customer(row)
            and row.get("Type") == "Renewal"
            and row.get("IsClosed")
            and not row.get("IsWon")
            and renewal_value(row) > 0
        ]
        candidate_wins = [
            row for row in account_rows
            if is_customer(row)
            and row.get("Type") in {"Expand", "Land"}
            and row.get("IsClosed")
            and row.get("IsWon")
        ]
        for renewal_row in renewal_losses:
            for candidate_row in candidate_wins:
                match = score_candidate(renewal_row, candidate_row)
                if match:
                    matches.append(match)
    return matches


def summarize_matches(matches: list[CandidateMatch]) -> dict[str, Any]:
    renewal_best: dict[str, CandidateMatch] = {}
    confidence_rank = {"high": 3, "medium": 2, "low": 1}
    for match in matches:
        current = renewal_best.get(match.renewal_id)
        if current is None:
            renewal_best[match.renewal_id] = match
            continue
        current_rank = (confidence_rank[current.confidence], current.candidate_arr)
        new_rank = (confidence_rank[match.confidence], match.candidate_arr)
        if new_rank > current_rank:
            renewal_best[match.renewal_id] = match

    summary = {
        "renewal_losses_with_match_count": len(renewal_best),
        "renewal_value_with_match": 0.0,
        "candidate_arr_on_best_matches": 0.0,
        "confidence_counts": Counter(),
        "sample_matches": [],
    }
    for match in renewal_best.values():
        summary["renewal_value_with_match"] += match.renewal_value
        summary["candidate_arr_on_best_matches"] += match.candidate_arr
        summary["confidence_counts"][match.confidence] += 1
    sample = sorted(
        renewal_best.values(),
        key=lambda item: ({"high": 0, "medium": 1, "low": 2}[item.confidence], -item.renewal_value),
    )[:15]
    summary["sample_matches"] = [asdict(item) for item in sample]
    summary["confidence_counts"] = dict(summary["confidence_counts"])
    return summary


def write_markdown(report: dict[str, Any], output_path: Path) -> None:
    field_quality = report["field_quality"]
    customer_won = report["customer_closed_won"]
    expand_customer = report["expand_customer"]
    match_summary = report["potential_protection_matches"]
    amount_ratio = report["renewal_amount_ratios"]

    lines = [
        "# Retention Semantics Profile",
        "",
        "This report profiles how renewal-like revenue is actually recorded in SimCorp Opportunity data.",
        "",
        "## Key Findings",
        "",
        f"- Closed-won customer `Expand` since {ANALYSIS_START_YEAR}: `{customer_won.get('Expand', {}).get('count', 0):,}` deals / `{customer_won.get('Expand', {}).get('arr', 0.0):,.2f} ARR`.",
        f"- Closed-won customer `Renewal` since {ANALYSIS_START_YEAR}: `{customer_won.get('Renewal', {}).get('count', 0):,}` deals / `{customer_won.get('Renewal', {}).get('renewal_acv', 0.0):,.2f} Renewal ACV`.",
        f"- Renewal `Amount / Renewal ACV` median ratio: `{amount_ratio.get('median_amount_to_renewal_acv', 0.0):.2f}x`.",
        f"- Closed-won customer `Expand` subtype `New Revenue`: `{expand_customer['subtype'].get('New Revenue', {}).get('count', 0):,}` deals / `{expand_customer['subtype'].get('New Revenue', {}).get('arr', 0.0):,.2f} ARR`.",
        f"- Renewal-ish closed-won customer `Expand` by name heuristic: `{expand_customer['renewalish_name_slice']['count']:,}` deals / `{expand_customer['renewalish_name_slice']['arr']:,.2f} ARR`.",
        f"- Lost / no-opportunity renewals with a same-account won `Expand`/`Land` match: `{match_summary['renewal_losses_with_match_count']:,}` losses covering `{match_summary['renewal_value_with_match']:,.2f}` renewal value.",
        "",
        "## Field Quality by Type and Outcome",
        "",
        "| Bucket | Count | ARR Nonzero | Renewal ACV Nonzero | Amount Nonzero | ARR Sum | Renewal ACV Sum | Amount Sum |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key, value in sorted(field_quality.items()):
        lines.append(
            f"| `{key}` | {value['count']:,} | {value['arr_nonzero']:,} | {value['renewal_acv_nonzero']:,} | {value['amount_nonzero']:,} | {value['arr_sum']:,.2f} | {value['renewal_acv_sum']:,.2f} | {value['amount_sum']:,.2f} |"
        )

    lines.extend(
        [
            "",
            "## Closed-Won Customer Revenue by Type",
            "",
            "| Type | Count | ARR | Renewal ACV | Amount |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for key, value in customer_won.items():
        lines.append(
            f"| `{key}` | {value['count']:,} | {value['arr']:,.2f} | {value['renewal_acv']:,.2f} | {value['amount']:,.2f} |"
        )

    lines.extend(
        [
            "",
            "## Closed-Won Customer Expand by Subtype",
            "",
            "| Subtype | Count | ARR |",
            "|---|---:|---:|",
        ]
    )
    for key, value in expand_customer["subtype"].items():
        lines.append(f"| `{key}` | {value['count']:,} | {value['arr']:,.2f} |")

    lines.extend(
        [
            "",
            "## Closed-Won Customer Expand by Quote Type",
            "",
            "| Quote Type | Count | ARR |",
            "|---|---:|---:|",
        ]
    )
    for key, value in expand_customer["quote_type"].items():
        lines.append(f"| `{key}` | {value['count']:,} | {value['arr']:,.2f} |")

    lines.extend(
        [
            "",
            "## Potential Protection Matches",
            "",
            f"- Best-match confidence counts: `{match_summary['confidence_counts']}`",
            f"- Renewal value on matched losses: `{match_summary['renewal_value_with_match']:,.2f}`",
            f"- Candidate ARR on matched wins: `{match_summary['candidate_arr_on_best_matches']:,.2f}`",
            "",
            "| Confidence | Account | Renewal | Renewal Value | Candidate | Candidate Type | Candidate ARR | Δ Days | Product Overlap |",
            "|---|---|---|---:|---|---|---:|---:|---|",
        ]
    )
    for item in match_summary["sample_matches"]:
        lines.append(
            f"| `{item['confidence']}` | {item['account_name']} | {item['renewal_name']} | {item['renewal_value']:,.2f} | {item['candidate_name']} | {item['candidate_type']} | {item['candidate_arr']:,.2f} | {item['days_delta']} | {item['product_overlap']} |"
        )

    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "- Use `APTS_Renewal_ACV__c` for strict renewal value.",
            "- Use `APTS_Opportunity_ARR__c` for Land / Expand value.",
            "- Do not use `Amount` as the primary retention metric; it behaves like a multi-year or commercial-value measure, not annual retention value.",
            "- Expose both `Strict Renewal` and `Effective Retention` in the semantic model.",
            "- Treat quote type as a segmentation / explanation field first, not a hard classification rule.",
        ]
    )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/Users/test/crm-analytics/docs/generated/retention_semantics_profile_2026-03-11"),
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
    rows = run_soql(inst, tok, PROFILE_QUERY)

    report = {
        "row_count": len(rows),
        "analysis_start_year": ANALYSIS_START_YEAR,
        "field_quality": summarize_field_quality(rows),
        "customer_closed_won": summarize_customer_closed_won(rows),
        "expand_customer": summarize_expand_customer(rows),
        "renewal_amount_ratios": renewal_amount_ratios(rows),
    }
    matches = build_candidate_matches(rows)
    report["potential_protection_matches"] = summarize_matches(matches)

    json_path = output_dir / "profile.json"
    md_path = output_dir / "profile.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_markdown(report, md_path)

    if emit_text:
        print(json_path)
        print(md_path)

    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote retention semantics profile to {output_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "row_count": report["row_count"],
            "analysis_start_year": report["analysis_start_year"],
            "renewal_loss_match_count": report["potential_protection_matches"]["renewal_losses_with_match_count"],
            "expand_customer_count": report["customer_closed_won"].get("Expand", {}).get("count", 0),
            "renewal_customer_count": report["customer_closed_won"].get("Renewal", {}).get("count", 0),
            "output_dir": str(output_dir),
        },
        profile=report,
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
