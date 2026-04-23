#!/usr/bin/env python3
"""Audit Commercial Rhythm Control Tower against operating-rhythm rules."""

from __future__ import annotations

import argparse
import html
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


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
        "tool": "audit_commercial_rhythm_control_tower",
        "lane": "export_audits",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _page_labels(dashboard: dict[str, Any]) -> list[str]:
    return [
        html.unescape(page.get("label", ""))
        for page in dashboard["state"]["gridLayouts"][0].get("pages", [])
    ]


def _viz_counts(dashboard: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for widget in dashboard["state"]["widgets"].values():
        viz = widget.get("parameters", {}).get("visualizationType", widget.get("type"))
        counts[viz] = counts.get(viz, 0) + 1
    return counts


def _chrome_ratio(dashboard: dict[str, Any]) -> float:
    counts = _viz_counts(dashboard)
    chrome = sum(counts.get(viz, 0) for viz in ("link", "text"))
    total = sum(counts.values()) or 1
    return chrome / total


def _widget(dashboard: dict[str, Any], name: str) -> dict[str, Any]:
    return dashboard["state"]["widgets"][name]


def _step_query(dashboard: dict[str, Any], name: str) -> str:
    return html.unescape(dashboard["state"]["steps"][name]["query"])


def run_audit(live_export_dir: Path) -> dict[str, Any]:
    dashboard = _load(live_export_dir / "commercial_rhythm_control_tower" / "dashboard.json")
    page_labels = _page_labels(dashboard)
    chrome_ratio = _chrome_ratio(dashboard)
    viz_counts = _viz_counts(dashboard)

    checks: list[AuditCheck] = []
    checks.append(
        AuditCheck(
            category="persona",
            name="page_set_matches_control_tower",
            passed=page_labels == ["Summary", "Ownership & Handoffs", "Process Quality"],
            detail=f"Pages = {page_labels}",
        )
    )
    checks.append(
        AuditCheck(
            category="persona",
            name="cross_dashboard_links_present_on_all_pages",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in (
                    "p1_link_sales",
                    "p1_link_csm",
                    "p1_link_account",
                    "p2_link_sales",
                    "p2_link_csm",
                    "p2_link_account",
                    "p3_link_sales",
                    "p3_link_csm",
                    "p3_link_account",
                )
            ),
            detail="Control tower should explicitly route to Sales Manager, CSM Manager, and Account 360 from every page.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="shared_filters_present_on_all_pages",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in (
                    "p1_f_fy",
                    "p1_f_region",
                    "p1_f_motion",
                    "p1_f_persona",
                    "p1_f_manager",
                    "p2_f_fy",
                    "p2_f_region",
                    "p2_f_motion",
                    "p2_f_persona",
                    "p2_f_manager",
                    "p3_f_fy",
                    "p3_f_region",
                    "p3_f_motion",
                    "p3_f_persona",
                    "p3_f_manager",
                )
            ),
            detail="Fiscal year, region, motion, persona, and manager filters should exist across the entire control tower.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="summary_page_has_kpi_spine_and_breach_queue",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in (
                    "p1_n_open",
                    "p1_n_review",
                    "p1_n_next",
                    "p1_n_coverage",
                    "p1_ch_value",
                    "p1_ch_count",
                    "p1_tbl_queue",
                )
            ),
            detail="Summary should show open value, ownership review, next-step pressure, renewal coverage, trends, and the top breach queue.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="ownership_page_has_matrix_pressure_and_queue",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in ("p2_ch_matrix", "p2_tbl_pressure", "p2_tbl_review")
            ),
            detail="Ownership page should include motion/persona matrix, manager handoff pressure, and a review queue.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="quality_page_has_semantic_and_hygiene_tables",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in ("p3_tbl_conf", "p3_tbl_hygiene", "p3_tbl_zero")
            ),
            detail="Process Quality page should show renewal semantic confidence, owner hygiene, and zero-value renewal anomalies.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="breach_queue_has_record_context_and_clear_ask",
            passed=all(
                token in _step_query(dashboard, "s_breach_queue")
                for token in (
                    "OpportunityName",
                    "AccountName",
                    "MotionType",
                    "OppOwnerName",
                    "OppOwnerPersona",
                    "HandoffState",
                    "ReviewPulse",
                    "LeadershipAsk",
                    "Id",
                    "AccountId",
                )
            ),
            detail="Top rhythm breach queue should include record context plus pulse and ask fields.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="ownership_review_queue_is_open_opportunity_only",
            passed='q = filter q by IsClosed == "false";' in _step_query(dashboard, "s_ownership_review_queue")
            and "OwnershipReviewCount > 0" in _step_query(dashboard, "s_ownership_review_queue"),
            detail="Ownership review queue should only surface open deals that actually need ownership review.",
        )
    )
    checks.append(
        AuditCheck(
            category="semantic",
            name="renewal_coverage_is_explicitly_calculated",
            passed=all(
                token in _step_query(dashboard, "s_summary")
                for token in ("CoveredRenewalOppCount", "RenewalOppCount", "RenewalCoveragePct")
            ),
            detail="Summary should explicitly compute renewal coverage instead of implying it.",
        )
    )
    checks.append(
        AuditCheck(
            category="semantic",
            name="renewal_confidence_table_carries_alignment_and_coverage",
            passed=all(
                token in _step_query(dashboard, "s_renewal_confidence")
                for token in (
                    "OppManagerName",
                    "OppOwnerName",
                    "OppOwnerPersona",
                    "OppOwnershipAlignment",
                    "RenewalCoveragePct",
                    "ZeroValueRenewalCount",
                    "AtRiskRenewalValue",
                )
            ),
            detail="Renewal confidence table should combine alignment, coverage, anomaly count, and at-risk value.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="chrome_ratio_reasonable",
            passed=chrome_ratio <= 0.60,
            detail=f"Chrome ratio = {chrome_ratio:.1%}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="visual_mix_matches_control_tower_job",
            passed=viz_counts.get("line", 0) >= 2
            and viz_counts.get("comparisontable", 0) >= 5
            and viz_counts.get("heatmap", 0) >= 1,
            detail=f"Viz counts = {viz_counts}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="nav_shell_not_overweighted",
            passed=viz_counts.get("link", 0) <= 12 and viz_counts.get("text", 0) <= 12,
            detail=f"Viz counts = {viz_counts}",
        )
    )

    return {
        "live_export_dir": str(live_export_dir),
        "dashboard": dashboard["label"],
        "page_labels": page_labels,
        "widget_count": len(dashboard["state"].get("widgets", {})),
        "step_count": len(dashboard["state"].get("steps", {})),
        "chrome_ratio": chrome_ratio,
        "checks": [asdict(check) for check in checks],
        "pass_count": sum(1 for check in checks if check.passed),
        "fail_count": sum(1 for check in checks if not check.passed),
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Commercial Rhythm Control Tower Audit",
        "",
        f"- Live export: `{payload['live_export_dir']}`",
        f"- Dashboard: `{payload['dashboard']}`",
        f"- Pages: `{payload['page_labels']}`",
        f"- Widget count: `{payload['widget_count']}`",
        f"- Step count: `{payload['step_count']}`",
        f"- Chrome ratio: `{payload['chrome_ratio']:.1%}`",
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
    parser.add_argument("--live-export-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    return parser


def run_audit_command(
    live_export_dir: Path,
    output_dir: Path,
    *,
    emit_text: bool = True,
) -> tuple[dict[str, Any], int]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = run_audit(live_export_dir.resolve())
    audit_json_path = output_dir / "audit.json"
    audit_md_path = output_dir / "audit.md"
    audit_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_markdown(audit_md_path, payload)

    if emit_text:
        print(audit_json_path)
        print(audit_md_path)

    exit_code = 0 if payload["fail_count"] == 0 else 1
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
            "dashboard": payload["dashboard"],
            "page_labels": payload["page_labels"],
            "pass_count": payload["pass_count"],
            "fail_count": payload["fail_count"],
            "widget_count": payload["widget_count"],
            "step_count": payload["step_count"],
            "chrome_ratio": payload["chrome_ratio"],
            "output_dir": str(output_dir),
        },
        audit=payload,
    )
    return result, exit_code


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result, exit_code = run_audit_command(
        args.live_export_dir.resolve(),
        args.output_dir.resolve(),
        emit_text=not args.json,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
