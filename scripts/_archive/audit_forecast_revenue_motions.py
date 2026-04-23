#!/usr/bin/env python3
"""Audit Forecast & Revenue Motions against manager revenue-surface rules."""

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
        "tool": "audit_forecast_revenue_motions",
        "lane": "export_audits",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_dashboard_path(live_export_dir: Path) -> Path:
    direct = live_export_dir / "dashboard.json"
    nested = live_export_dir / "forecast_revenue_motions" / "dashboard.json"
    if direct.exists():
        return direct
    if nested.exists():
        return nested
    raise FileNotFoundError(
        f"Could not find dashboard.json under {live_export_dir} or {nested.parent}"
    )


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
    chrome = sum(counts.get(viz, 0) for viz in ("link", "listselector", "text"))
    total = sum(counts.values()) or 1
    return chrome / total


def _widget(dashboard: dict[str, Any], name: str) -> dict[str, Any]:
    return dashboard["state"]["widgets"][name]


def _step_query(dashboard: dict[str, Any], name: str) -> str:
    return dashboard["state"]["steps"][name]["query"]


def _unused_widgets(dashboard: dict[str, Any]) -> list[str]:
    used: set[str] = set()
    for page in dashboard["state"]["gridLayouts"][0].get("pages", []):
        for widget in page.get("widgets", []):
            name = widget.get("name")
            if name:
                used.add(name)
    return sorted(set(dashboard["state"]["widgets"]) - used)


def run_audit(live_export_dir: Path) -> dict[str, Any]:
    dashboard = _load(_resolve_dashboard_path(live_export_dir))
    page_labels = _page_labels(dashboard)
    chrome_ratio = _chrome_ratio(dashboard)
    viz_counts = _viz_counts(dashboard)

    checks: list[AuditCheck] = []
    checks.append(
        AuditCheck(
            category="persona",
            name="manager_page_set_reasonable",
            passed=page_labels
            == [
                "Summary",
                "Trend & Forecast",
                "Drivers & Segments",
                "Exceptions & Actions",
                "Week over Week",
            ],
            detail=f"Pages = {page_labels}",
        )
    )
    checks.append(
        AuditCheck(
            category="persona",
            name="cross_dashboard_manager_tabs_present",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in (
                    "p1_tab_sales",
                    "p1_tab_csm",
                    "p2_tab_sales",
                    "p2_tab_csm",
                    "p3_tab_sales",
                    "p3_tab_csm",
                    "p4_tab_sales",
                    "p4_tab_csm",
                    "p5_tab_sales",
                    "p5_tab_csm",
                )
            ),
            detail="Manager surface should expose explicit Sales Manager / CSM Manager tabs on every page.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="core_forecast_story_widgets_present",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in (
                    "p1_ch_timeline",
                    "p1_ch_bridge",
                    "p1_tbl_actions",
                    "p1_tbl_big_bets",
                    "p2_ch_forecast_quality",
                    "p4_tbl_review",
                    "p4_tbl_commit",
                    "p4_tbl_forecast",
                    "p4_tbl_omitted",
                    "p5_ch_timeline",
                    "p5_tbl_big_bets",
                    "p5_tbl_pushes",
                )
            ),
            detail="Dashboard should include trajectory, bridge, quality, exception tables, and a weekly-movement page.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="summary_page_has_compact_weekly_signal",
            passed="WeekChangeStory" in _step_query(dashboard, "s_wow_big_bet_table")
            and "OpportunityId" in _step_query(dashboard, "s_wow_big_bet_table")
            and "p1_tbl_big_bets" in dashboard["state"]["widgets"],
            detail="Summary page should keep one compact big-bets/weekly-change signal visible without forcing a page switch.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="quarter_filter_present_on_all_pages",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in (
                    "p1_f_quarter",
                    "p2_f_quarter",
                    "p3_f_quarter",
                    "p4_f_quarter",
                    "p5_f_quarter",
                )
            ),
            detail="Manager surface should support a Close Quarter cut on every page.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_filter_present_on_all_pages",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in (
                    "p1_f_manager",
                    "p2_f_manager",
                    "p3_f_manager",
                    "p4_f_manager",
                    "p5_f_manager",
                )
            ),
            detail="Manager surface should support a Manager cut on every page.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="wow_page_uses_weekly_datasets",
            passed=all(
                'Weekly_Forecast_' in _step_query(dashboard, step)
                for step in ("s_wow_commit", "s_wow_best_case", "s_wow_timeline")
            )
            and all(
                "CurrentWeekFlag" in _step_query(dashboard, step)
                for step in ("s_wow_commit", "s_wow_best_case")
            ),
            detail="WoW page should be driven by weekly snapshot datasets, not the static forecast dataset.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="manager_action_queue_present",
            passed=all(
                token in _step_query(dashboard, "s_manager_action_queue")
                for token in ("OpportunityName", "AccountName", "Escalation", "DealPulse", "CloseQuarter")
            ),
            detail="Summary page should surface a visible manager action queue.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="action_queues_use_real_process_blockers",
            passed=all(
                "OverdueTaskFlag" in _step_query(dashboard, step)
                for step in (
                    "s_manager_action_queue",
                    "s_deal_review_candidates",
                    "s_top_commit_protection",
                    "s_top_forecast_risk",
                    "s_top_omitted",
                )
            )
            and all(
                "CommercialApprovalFlag" in _step_query(dashboard, step)
                for step in (
                    "s_manager_action_queue",
                    "s_deal_review_candidates",
                    "s_top_commit_protection",
                    "s_top_forecast_risk",
                )
            ),
            detail="Manager action queues should surface overdue-task blockers everywhere, and commercial-approval blockers on the review/protection/promotion queues where they are relevant.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="exception_tables_have_record_context",
            passed=all(
                token in _step_query(dashboard, step)
                for step in (
                    "s_deal_review_candidates",
                    "s_top_commit_protection",
                    "s_top_forecast_risk",
                    "s_top_omitted",
                )
                for token in ("Id", "OpportunityName", "AccountName", "CloseQuarter")
            ),
            detail="Deal-review, commit-protection, promotion, and omitted queues should include record ids plus opportunity/account context.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="owner_gap_queue_has_gap_context",
            passed=all(
                token in _step_query(dashboard, "s_owner_gap")
                for token in (
                    "ManagerName",
                    "OwnerName",
                    "QuotaAmount",
                    "WeightedModelARR",
                    "CommitForecastARR",
                    "NeededPromotionARR",
                    "BestCaseForecastARR",
                )
            ),
            detail="Owner queue should include rep, quota, weighted model, commit, best case, and promotion pressure.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="chrome_ratio_reasonable",
            passed=chrome_ratio <= 0.55,
            detail=f"Chrome ratio = {chrome_ratio:.1%}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="manager_uses_forecast_specific_visuals",
            passed=viz_counts.get("waterfall", 0) >= 1
            and viz_counts.get("comparisontable", 0) >= 2
            and viz_counts.get("heatmap", 0) >= 1
            and viz_counts.get("line", 0) >= 5,
            detail=f"Viz counts = {viz_counts}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="owner_tables_include_rep_coaching_fields",
            passed=all(
                token in _step_query(dashboard, step)
                for step in ("s_stage_aging_pressure", "s_owner_confidence")
                for token in ("OpenOppCount", "NoNextStepCount", "OwnerName", "ManagerName")
            ),
            detail="Trend page owner tables should show rep coaching fields like open opp count and missing next-step count.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="process_table_uses_real_sales_process_fields",
            passed=all(
                token in _step_query(dashboard, "s_process_compliance")
                for token in (
                    "DealReviewCount",
                    "PendingApprovalCount",
                    "StaleApprovalCount",
                    "PendingApprovalARR",
                    "OverdueTaskCount",
                )
            ),
            detail="Trend page process table should use live sales-process signals like deal review, pending approval aging, and overdue task counts.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="approval_queues_show_pending_approval_aging",
            passed=all(
                "CommercialApprovalAgeDays" in _step_query(dashboard, step)
                for step in (
                    "s_deal_review_candidates",
                    "s_top_commit_protection",
                    "s_top_forecast_risk",
                )
            ),
            detail="Review, commit-protection, and promotion queues should expose pending commercial-approval aging.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="account_360_handoff_present",
            passed="p4_link_account360" in dashboard["state"]["widgets"],
            detail="Exceptions page should expose an explicit Account 360 handoff.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="monthly_trajectory_uses_robust_line_viz",
            passed=_widget(dashboard, "p1_ch_timeline")
            .get("parameters", {})
            .get("visualizationType")
            == "line",
            detail="Monthly trajectory should use the durable line-chart path rather than the brittle timeline widget.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="forecast_views_are_not_bar_first",
            passed=all(
                _widget(dashboard, name).get("parameters", {}).get("visualizationType")
                == "line"
                for name in ("p1_ch_timeline", "p1_ch_call", "p2_ch_forecast_quality")
            ),
            detail="Manager forecast scenario views should be line-based, not generic bars.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="summary_trajectory_uses_monthly_grain",
            passed="group q by MonthDate" in _step_query(dashboard, "s_monthly_trajectory")
            and "generate MonthDate" in _step_query(dashboard, "s_monthly_trajectory")
            and "order q by MonthDate asc" in _step_query(dashboard, "s_monthly_trajectory"),
            detail="Summary trajectory should be a monthly trend grouped and ordered by MonthDate.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="scenario_ladder_is_monthly_not_quarterly",
            passed="group q1 by MonthDate" in _step_query(dashboard, "s_sales_call_ladder")
            and "generate MonthDate" in _step_query(dashboard, "s_sales_call_ladder")
            and "order q by MonthDate asc" in _step_query(dashboard, "s_sales_call_ladder")
            and "group q1 by CloseQuarter" not in _step_query(dashboard, "s_sales_call_ladder"),
            detail="Scenario forecast view should use MonthDate rather than quarterly grouping.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="plan_bridge_step_is_waterfall_compatible",
            passed="BridgeOrder" not in _step_query(dashboard, "s_plan_bridge")
            and "BridgeARR" in _step_query(dashboard, "s_plan_bridge")
            and "BridgeStep" in _step_query(dashboard, "s_plan_bridge"),
            detail="Waterfall bridge must emit exactly one grouping field plus one measure; the step should not project BridgeOrder.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="nav_shell_not_overweighted",
            passed=viz_counts.get("link", 0) <= 12
            and viz_counts.get("text", 0) <= 10
            and chrome_ratio <= 0.55,
            detail=f"Viz counts = {viz_counts}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="no_orphaned_widgets",
            passed=not _unused_widgets(dashboard),
            detail=f"Unused widgets = {_unused_widgets(dashboard)}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="process_issue_trend_groups_by_close_quarter",
            passed="group q by CloseQuarter" in _step_query(dashboard, "s_process_issue_trend")
            and "group q by (FiscalQuarter, CloseQuarter)" not in _step_query(dashboard, "s_process_issue_trend"),
            detail="Quarterly process-pressure trend should group by CloseQuarter, not the numeric FiscalQuarter field.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="process_table_uses_single_sort_score",
            passed="ProcessPressureScore" in _step_query(dashboard, "s_process_compliance")
            and "order q by ProcessPressureScore desc" in _step_query(dashboard, "s_process_compliance"),
            detail="Process table should use a single deterministic sort score rather than a brittle multi-column order clause.",
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
        "# Forecast & Revenue Motions Audit",
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
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = run_audit(live_export_dir)
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
