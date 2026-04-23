#!/usr/bin/env python3
"""Audit the BDR truth layer for account/product scope and product-signal integrity."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean
from typing import Any

ROOT = Path("/Users/test/crm-analytics")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm_analytics_helpers import execute_query, get_auth, get_dataset_id  # noqa: E402
DS = "BDR_Operating_Rhythm"


@dataclass
class AuditCheck:
    category: str
    name: str
    passed: bool
    detail: str


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
        "tool": "audit_bdr_truth_layer",
        "lane": "wave_data_validations",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def run_wave_query(inst: str, tok: str, query: str) -> list[dict[str, Any]]:
    payload = execute_query(inst, tok, query)
    return payload.get("results", {}).get("records", [])


def run_audit() -> dict[str, Any]:
    inst, tok = get_auth()
    dataset_id = get_dataset_id(inst, tok, DS)
    if not dataset_id:
        raise RuntimeError(f"Could not resolve dataset id for {DS}")

    record_type_rows = run_wave_query(
        inst,
        tok,
        f"""
q = load "{DS}";
q = group q by 'RecordType';
q = foreach q generate 'RecordType' as RecordType, count() as RowCount;
q = order q by RowCount desc;
""".strip(),
    )
    record_type_counts = {
        row.get("RecordType") or "Unknown": int(float(row.get("RowCount") or 0))
        for row in record_type_rows
    }

    account_product_rows = run_wave_query(
        inst,
        tok,
        f"""
q = load "{DS}";
q = filter q by RecordType == "account_product_target";
q = group q by 'ContextAccountId';
q = foreach q generate 'ContextAccountId' as ContextAccountId, count() as ProductCount;
q = order q by ProductCount desc;
""".strip(),
    )
    product_counts = [int(float(row.get("ProductCount") or 0)) for row in account_product_rows]
    total_accounts = len(product_counts)
    multi_product_accounts = sum(1 for count in product_counts if count > 1)
    max_products_per_account = max(product_counts) if product_counts else 0
    avg_products_per_account = round(mean(product_counts), 2) if product_counts else 0.0

    product_source_rows = run_wave_query(
        inst,
        tok,
        f"""
q = load "{DS}";
q = filter q by RecordType == "account_product_target";
q = group q by 'TargetedProductSource';
q = foreach q generate 'TargetedProductSource' as TargetedProductSource, count() as RowCount;
q = order q by RowCount desc;
""".strip(),
    )
    product_source_counts = {
        row.get("TargetedProductSource") or "Unknown": int(float(row.get("RowCount") or 0))
        for row in product_source_rows
    }

    product_confidence_rows = run_wave_query(
        inst,
        tok,
        f"""
q = load "{DS}";
q = filter q by RecordType == "account_product_target";
q = group q by 'ProductSignalConfidence';
q = foreach q generate 'ProductSignalConfidence' as ProductSignalConfidence, count() as RowCount;
q = order q by RowCount desc;
""".strip(),
    )
    product_confidence_counts = {
        row.get("ProductSignalConfidence") or "Unknown": int(float(row.get("RowCount") or 0))
        for row in product_confidence_rows
    }

    product_focus_rows = run_wave_query(
        inst,
        tok,
        f"""
q = load "{DS}";
q = filter q by RecordType == "account_product_target";
q = group q by 'TargetedProduct';
q = foreach q generate 'TargetedProduct' as TargetedProduct, count() as RowCount;
q = order q by RowCount desc;
q = limit q 20;
""".strip(),
    )
    top_targeted_products = [
        {
            "product": row.get("TargetedProduct") or "Unknown",
            "row_count": int(float(row.get("RowCount") or 0)),
        }
        for row in product_focus_rows
    ]
    dominant_product_share = 0.0
    if top_targeted_products and record_type_counts.get("account_product_target"):
        dominant_product_share = round(
            top_targeted_products[0]["row_count"] / record_type_counts["account_product_target"],
            4,
        )

    opp_raw_rows = run_wave_query(
        inst,
        tok,
        f"""
q = load "{DS}";
q = filter q by RecordType == "opportunity_detail";
q = group q by all;
q = foreach q generate count() as TotalRows;
""".strip(),
    )
    opp_total = int(float((opp_raw_rows[0] if opp_raw_rows else {}).get("TotalRows") or 0))

    opp_raw_nonempty_rows = run_wave_query(
        inst,
        tok,
        f"""
q = load "{DS}";
q = filter q by RecordType == "opportunity_detail";
q = filter q by OpportunityProductRaw != "";
q = group q by all;
q = foreach q generate count() as TotalRows;
""".strip(),
    )
    opp_raw_nonempty = int(float((opp_raw_nonempty_rows[0] if opp_raw_nonempty_rows else {}).get("TotalRows") or 0))

    opp_fallback_rows = run_wave_query(
        inst,
        tok,
        f"""
q = load "{DS}";
q = filter q by RecordType == "opportunity_detail";
q = filter q by OpportunityProductRaw == "" && OpportunityProduct != "";
q = group q by all;
q = foreach q generate count() as TotalRows;
""".strip(),
    )
    opp_fallback = int(float((opp_fallback_rows[0] if opp_fallback_rows else {}).get("TotalRows") or 0))

    checks = [
        AuditCheck(
            category="truth_layer",
            name="has_account_product_target_rows",
            passed=record_type_counts.get("account_product_target", 0) > 0,
            detail=f"account_product_target rows = {record_type_counts.get('account_product_target', 0)}",
        ),
        AuditCheck(
            category="truth_layer",
            name="multi_product_accounts_exist",
            passed=multi_product_accounts > 0,
            detail=f"multi-product accounts = {multi_product_accounts} / {total_accounts}; avg products/account = {avg_products_per_account}",
        ),
        AuditCheck(
            category="truth_layer",
            name="product_scope_not_overcollapsed",
            passed=dominant_product_share <= 0.8,
            detail=f"dominant targeted-product share = {dominant_product_share:.1%}; top products = {top_targeted_products[:5]}",
        ),
        AuditCheck(
            category="truth_layer",
            name="known_product_signal_has_high_or_medium_support",
            passed=(product_confidence_counts.get("High", 0) + product_confidence_counts.get("Medium", 0)) > 0,
            detail=f"product signal confidence counts = {product_confidence_counts}",
        ),
        AuditCheck(
            category="truth_layer",
            name="account_mainline_not_used_as_target_source",
            passed=product_source_counts.get("Account Mainline", 0) == 0,
            detail=f"Account Mainline target-source rows = {product_source_counts.get('Account Mainline', 0)}",
        ),
        AuditCheck(
            category="truth_layer",
            name="raw_opportunity_product_preserved",
            passed=opp_total == 0 or opp_raw_nonempty > 0,
            detail=f"opportunity_detail rows = {opp_total}; nonempty OpportunityProductRaw rows = {opp_raw_nonempty}",
        ),
        AuditCheck(
            category="truth_layer",
            name="opportunity_product_not_mostly_fallback",
            passed=opp_total == 0 or opp_fallback == 0,
            detail=f"fallback-heavy opportunity rows = {opp_fallback} / {opp_total}",
        ),
    ]

    return {
        "dataset": DS,
        "dataset_id": dataset_id,
        "record_type_counts": record_type_counts,
        "account_product_stats": {
            "total_accounts": total_accounts,
            "multi_product_accounts": multi_product_accounts,
            "avg_products_per_account": avg_products_per_account,
            "max_products_per_account": max_products_per_account,
            "product_source_counts": product_source_counts,
            "product_confidence_counts": product_confidence_counts,
            "top_targeted_products": top_targeted_products,
            "dominant_product_share": dominant_product_share,
        },
        "opportunity_product_stats": {
            "opportunity_rows": opp_total,
            "nonempty_raw_rows": opp_raw_nonempty,
            "fallback_rows": opp_fallback,
        },
        "checks": [asdict(check) for check in checks],
        "pass_count": sum(1 for check in checks if check.passed),
        "fail_count": sum(1 for check in checks if not check.passed),
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    account_stats = payload["account_product_stats"]
    opp_stats = payload["opportunity_product_stats"]
    lines = [
        "# BDR Truth Layer Audit",
        "",
        f"- Dataset: `{payload['dataset']}`",
        f"- Dataset ID: `{payload['dataset_id']}`",
        f"- Record types: `{payload['record_type_counts']}`",
        f"- Total account-product accounts: `{account_stats['total_accounts']}`",
        f"- Multi-product accounts: `{account_stats['multi_product_accounts']}`",
        f"- Avg products/account: `{account_stats['avg_products_per_account']}`",
        f"- Dominant product share: `{account_stats['dominant_product_share']:.1%}`",
        f"- Opportunity rows: `{opp_stats['opportunity_rows']}`",
        f"- Opportunity fallback rows: `{opp_stats['fallback_rows']}`",
        "",
        "## Checks",
        "",
    ]
    current_category = None
    for check in payload["checks"]:
        if check["category"] != current_category:
            current_category = check["category"]
            lines.extend(["", f"### {current_category.title()}"])
        status = "PASS" if check["passed"] else "FAIL"
        lines.append(f"- `{status}` `{check['name']}`: {check['detail']}")
    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    return parser


def run_audit_command(
    output_dir: Path,
    *,
    emit_text: bool = True,
) -> tuple[dict[str, Any], int]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = run_audit()
    audit_json_path = output_dir / "audit.json"
    audit_md_path = output_dir / "audit.md"
    audit_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_markdown(audit_md_path, payload)

    if emit_text:
        print(audit_json_path)
        print(audit_md_path)

    status = "warn" if payload["fail_count"] else "ok"
    messages = [
        _make_message(
            "warn" if payload["fail_count"] else "info",
            "audit_findings" if payload["fail_count"] else "audit_clean",
            (
                f"Audit found {payload['fail_count']} failing check(s) out of "
                f"{len(payload['checks'])}."
            )
            if payload["fail_count"]
            else f"Audit passed {payload['pass_count']} of {len(payload['checks'])} checks.",
        )
    ]
    result = _make_result(
        status=status,
        messages=messages,
        artifacts=[
            _make_artifact("json", audit_json_path),
            _make_artifact("markdown", audit_md_path),
        ],
        summary={
            "dataset": payload["dataset"],
            "dataset_id": payload["dataset_id"],
            "pass_count": payload["pass_count"],
            "fail_count": payload["fail_count"],
            "total_accounts": payload["account_product_stats"]["total_accounts"],
            "multi_product_accounts": payload["account_product_stats"]["multi_product_accounts"],
            "dominant_product_share": payload["account_product_stats"]["dominant_product_share"],
            "output_dir": str(output_dir),
        },
        audit=payload,
    )
    return result, (1 if payload["fail_count"] else 0)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result, exit_code = run_audit_command(
        args.output_dir.resolve(),
        emit_text=not args.json,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
