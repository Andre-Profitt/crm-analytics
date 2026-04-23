#!/usr/bin/env python3
"""Audit the BDR campaign / target control dashboard."""

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
        "tool": "audit_bdr_campaign_control",
        "lane": "export_audits",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _viz_counts(dashboard: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for widget in dashboard["state"]["widgets"].values():
        viz = widget.get("parameters", {}).get("visualizationType", widget.get("type"))
        counts[viz] = counts.get(viz, 0) + 1
    return counts


def _chrome_ratio(dashboard: dict[str, Any]) -> float:
    counts = _viz_counts(dashboard)
    chrome = sum(counts.get(viz, 0) for viz in ("link", "listselector", "text"))
    total = sum(counts.values()) or 1
    return chrome / total


def _page_labels(dashboard: dict[str, Any]) -> list[str]:
    return [html.unescape(page.get("label", "")) for page in dashboard["state"]["gridLayouts"][0].get("pages", [])]


def _step_query(dashboard: dict[str, Any], step_name: str) -> str:
    return dashboard["state"]["steps"][step_name]["query"]


def run_audit(live_export_dir: Path) -> dict[str, Any]:
    dash = _load(live_export_dir / "bdr_campaign_target_control" / "dashboard.json")
    ratio = _chrome_ratio(dash)
    page_labels = _page_labels(dash)
    viz_counts = _viz_counts(dash)
    checks: list[AuditCheck] = []

    checks.append(
        AuditCheck(
            category="persona",
            name="page_set",
            passed=page_labels == [
                "Campaign Performance",
                "Persona & Product",
                "Cohort Plays",
                "Activation Queues",
                "Strategic Target Lists",
            ],
            detail=f"Pages = {page_labels}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="chrome_ratio_reasonable",
            passed=ratio <= 0.60,
            detail=f"Chrome ratio = {ratio:.1%}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="campaign_quality_present",
            passed=all(
                token in _step_query(dash, "s_campaign_quality")
                for token in [
                    "Campaign",
                    "CampaignProduct",
                    "CampaignScopeType",
                    "SourceGroup",
                    "LeadCount",
                    "MQLLeadCount",
                    "SQLLeadCount",
                    "ResponseRatePct",
                    "LeadToMeetingPct",
                    "LeadToOppPct",
                ]
            ),
            detail="Campaign performance should expose campaign, product, scope, lifecycle mix, and conversion.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="weekly_rhythm_present",
            passed=all(
                token in _step_query(dash, "s_weekly_rhythm")
                for token in ["WeekStartDate", "LeadCreatedCount", "ResponseCount", "MeetingHeldCount", "QualifiedCount"]
            ),
            detail="Campaign control should include a weekly rhythm view for lead creation, responses, meetings, and handoffs.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="monthly_rhythm_present",
            passed=all(
                token in _step_query(dash, "s_monthly_rhythm")
                for token in ["MonthStartDate", "LeadCount", "ResponseCount", "MeetingHeldCount", "OpportunityHandoffCount"]
            ),
            detail="Campaign control should include a monthly seasonality view for lead creation, responses, meetings, and handoffs.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="persona_product_targeting_present",
            passed=all(
                token in _step_query(dash, "s_persona_product")
                for token in [
                    "Persona",
                    "ProductFocus",
                    "LeadCount",
                    "ResponseRatePct",
                    "LeadToOppPct",
                    "KnownAttributedARR",
                ]
            )
            and "RoleFocus" in _step_query(dash, "s_role_industry"),
            detail="Dashboard should expose persona/product and role/title x industry targeting views.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="monthly_product_view_present",
            passed=all(
                token in _step_query(dash, "s_monthly_product_engagement")
                for token in ["ProductFocus", "MonthStartDate", "EngagementScore"]
            ),
            detail="Dashboard should expose monthly product engagement seasonality, not just aggregate product totals.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="cohort_play_tables_present",
            passed=all(
                name in dash["state"]["widgets"]
                for name in ["p3_tbl_former", "p3_tbl_current", "p3_tbl_untouched", "p3_tbl_cold"]
            )
            and all(
                token in _step_query(dash, step_name)
                for step_name in ["s_former_client", "s_tm_handback", "s_untouched", "s_cold_reengage"]
                for token in ["NextBestAction", "SuggestedTool", "LeadId"]
            ),
            detail="Cohort pages should provide actionable former-client, telemarketing hand-back, untouched, and cold-lead queues.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="activation_queues_present",
            passed=all(
                name in dash["state"]["widgets"]
                for name in ["p4_tbl_response", "p4_tbl_stage3", "p4_tbl_target", "p4_tbl_cold"]
            )
            and all(token in _step_query(dash, "s_responder_queue") for token in ["NextBestAction", "SuggestedTool", "LeadId"])
            and all(
                token in _step_query(dash, "s_stage3_queue")
                for token in ["NextBestAction", "SuggestedTool", "SourcedOpportunityId"]
            )
            and all(token in _step_query(dash, "s_target_queue") for token in ["NextBestAction", "SuggestedTool", "LeadId"])
            and all(token in _step_query(dash, "s_reentry_handback") for token in ["NextBestAction", "SuggestedTool", "LeadId"]),
            detail="Activation queues should cover responders, stage 2 -> 3 handoff, target-account activation, and re-entry / hand-back motions.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="strategic_target_lists_present",
            passed=all(
                name in dash["state"]["widgets"]
                for name in ["p5_tbl_pockets", "p5_tbl_role_product", "p5_tbl_named", "p5_tbl_former", "p5_tbl_cold"]
            )
            and all(
                token in _step_query(dash, "s_industry_product_targets")
                for token in ["Industry", "ProductFocus", "ClientBaseClass", "TargetAccountLeadCount", "LeadToOppPct", "KnownAttributedARR"]
            )
            and all(
                token in _step_query(dash, "s_role_industry_product")
                for token in ["RoleFocus", "Industry", "ProductFocus", "LeadToOppPct", "KnownAttributedARR"]
            )
            and all(token in _step_query(dash, "s_named_account_targets_long") for token in ["MatchedAccountName", "MatchedAccountTier", "MatchedAccountSegment", "LeadId"])
            and all(token in _step_query(dash, "s_former_client_long") for token in ["FormerClientAgeBand", "FormerClientLostDate", "LeadId"])
            and all(token in _step_query(dash, "s_cold_prospect_long") for token in ["ProductFocus", "SourceGroup", "Campaign", "LeadId"]),
            detail="Strategic targeting page should include industry/product pockets plus larger named-account, former-client, and cold-prospect target lists.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="has_diagnostic_mix",
            passed=all(viz in viz_counts for viz in ["comparisontable", "hbar", "heatmap", "number"]),
            detail=f"Viz mix = {sorted(viz_counts)}",
        )
    )

    return {
        "live_export_dir": str(live_export_dir),
        "dashboard": dash["label"],
        "page_labels": page_labels,
        "widget_count": len(dash["state"].get("widgets", {})),
        "step_count": len(dash["state"].get("steps", {})),
        "chrome_ratio": ratio,
        "checks": [asdict(check) for check in checks],
        "pass_count": sum(1 for check in checks if check.passed),
        "fail_count": sum(1 for check in checks if not check.passed),
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# BDR Campaign Control Audit",
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
    return result, (1 if payload["fail_count"] else 0)


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
