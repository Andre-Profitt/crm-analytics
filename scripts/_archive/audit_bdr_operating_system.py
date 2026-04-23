#!/usr/bin/env python3
"""Audit the BDR manager/rep dashboards against action-first operating rules."""

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
        "tool": "audit_bdr_operating_system",
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


def _widget(dashboard: dict[str, Any], name: str) -> dict[str, Any]:
    return dashboard["state"]["widgets"][name]


def _step_query(dashboard: dict[str, Any], step_name: str) -> str:
    return dashboard["state"]["steps"][step_name]["query"]


def run_audit(live_export_dir: Path) -> dict[str, Any]:
    rep = _load(live_export_dir / "bdr_rep_queue" / "dashboard.json")
    mgr = _load(live_export_dir / "bdr_manager" / "dashboard.json")

    checks: list[AuditCheck] = []

    rep_ratio = _chrome_ratio(rep)
    mgr_ratio = _chrome_ratio(mgr)

    checks.append(
        AuditCheck(
            category="persona",
            name="rep_queue_page_set",
            passed=_page_labels(rep) == ["My Day", "Meetings & Follow-up", "Campaign Responders", "Target Accounts & Handoffs"],
            detail=f"Rep pages = {_page_labels(rep)}",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="rep_priority_query_has_action_fields",
            passed=all(
                token in _step_query(rep, "s_priority")
                for token in ["NextBestAction", "SuggestedTool", "LeadId", "ClientBaseClass", "ProductFocus", "Campaign"]
            ),
            detail="Rep priority query includes action, tool, client-base class, product, and campaign context.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="rep_summary_query_tracks_daily_operating_counts",
            passed=all(
                token in _step_query(rep, "s_summary")
                for token in ["open_leads", "open_mql_leads", "open_sql_leads", "responder_queue_count", "upcoming_meetings", "reengage_count"]
            ),
            detail="Rep summary should track open leads, open MQL/SQL mix, responders, meetings, and re-engagement counts.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="rep_followup_queries_have_action_fields",
            passed=all(
                token in _step_query(rep, step_name)
                for step_name in ["s_upcoming", "s_response", "s_sla", "s_target_reengage"]
                for token in ["NextBestAction", "SuggestedTool", "LeadId"]
            ),
            detail="Rep follow-up and re-engagement queues include next action, tool, and record id.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="rep_chrome_ratio_reasonable",
            passed=rep_ratio <= 0.60,
            detail=f"Rep chrome ratio = {rep_ratio:.1%}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="rep_has_queue_tables",
            passed=_viz_counts(rep).get("comparisontable", 0) >= 6,
            detail=f"Rep compare tables = {_viz_counts(rep).get('comparisontable', 0)}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="rep_has_work_mix_view",
            passed="p1_ch_mix" in rep["state"]["widgets"]
            and all(token in _step_query(rep, "s_priority_mix") for token in ["WorkBucket", "LeadCount"]),
            detail="Rep dashboard should include a daily work-mix view for responders, meetings, SQL/hot leads, re-engagement, and target-account work.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="rep_has_segment_and_target_views",
            passed=all(
                name in rep["state"]["widgets"]
                for name in ["p3_tbl_response", "p3_ch_product", "p3_ch_heatmap", "p3_tbl_campaign", "p3_tbl_client_product", "p3_tbl_source_product", "p4_tbl_target", "p4_tbl_segment", "p4_tbl_handoff", "p4_tbl_reengage"]
            ),
            detail="Rep dashboard includes campaign responders, campaign/product, role-industry, client/prospect mix, target-account, and handoff views.",
        )
    )
    checks.append(
        AuditCheck(
            category="process",
            name="rep_has_activity_logging_integrity_view",
            passed="p2_tbl_integrity" in rep["state"]["widgets"]
            and all(
                token in _step_query(rep, "s_integrity")
                for token in [
                    "DirectLeadTouch24hPct",
                    "AssociatedTouch24hPct",
                    "LeadLinkedActivityPct",
                    "ContactLinkedActivityPct",
                    "AccountLinkedActivityPct",
                    "TotalActivityCount",
                ]
            ),
            detail="Rep dashboard should expose direct lead-touch SLA plus associated prospect response and contact/account-heavy logging.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="rep_has_execution_hbars",
            passed=all(name in rep["state"]["widgets"] for name in ["p2_b_sla", "p2_b_assoc", "p2_b_leadlink"])
            and all(
                _widget(rep, name).get("parameters", {}).get("visualizationType") == "hbar"
                for name in ["p2_b_sla", "p2_b_assoc", "p2_b_leadlink"]
            )
            and all(
                token in _step_query(rep, step_name)
                for step_name in ["s_sla_bar", "s_assoc_bar", "s_lead_link_bar"]
                for token in ["MetricLabel", "Actual", "Target"]
            ),
            detail="Rep dashboard should include stable target-vs-actual hbars for direct lead touch, associated response, and lead-linked activity.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="rep_has_yoy_rhythm_view",
            passed="p1_tbl_yoy" in rep["state"]["widgets"]
            and all(
                token in _step_query(rep, "s_yoy_rhythm")
                for token in ["FY2026", "FY2025", "LeadCreatedCount", "MeetingHeldCount", "QualifiedCount", "KnownAttributedARR"]
            ),
            detail="Rep dashboard should compare FY2025 vs FY2026 lead, meeting, handoff, and known ARR rhythm.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="rep_has_campaign_persona_product_diagnostics",
            passed=all(
                token in _step_query(rep, "s_campaign_quality")
                for token in ["CampaignProduct", "LeadCount", "MQLLeadCount", "SQLLeadCount", "MeetingHeldCount", "QualifiedCount"]
            )
            and all(
                token in _step_query(rep, "s_persona_product")
                for token in ["ProductFocus", "ResponseRatePct", "LeadToOppPct", "KnownAttributedARR"]
            )
            and all(
                token in _step_query(rep, "s_client_product_mix")
                for token in ["ClientBaseClass", "ProductFocus", "LeadToOppPct"]
            )
            and all(
                token in _step_query(rep, "s_role_industry")
                for token in ["RoleFocus", "Industry", "LeadToOppPct"]
            ),
            detail="Rep dashboard should diagnose campaign/product, persona/product, client/prospect product mix, and role-industry opportunity patterns.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="rep_has_stage23_handoff_queue",
            passed="p4_tbl_handoff" in rep["state"]["widgets"]
            and all(
                token in _step_query(rep, "s_stage3_queue")
                for token in ["SourcedOpportunityName", "SourcedOpportunityStage", "HandoffQualityBand", "Stage2To3Days", "ProductFocus", "KnownAttributedARR", "SourcedOpportunityId"]
            ),
            detail="Rep dashboard should include an opportunity-native Stage 2 -> 3 handoff queue with product and attributed-value context.",
        )
    )

    checks.append(
        AuditCheck(
            category="persona",
            name="manager_page_set",
            passed=_page_labels(mgr) == ["NA Rhythm", "Rep Cadence", "Campaign & Product", "Persona, Industry & Product", "Handoff & Action Center"],
            detail=f"Manager pages = {_page_labels(mgr)}",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="manager_priority_query_has_action_fields",
            passed=all(
                token in _step_query(mgr, "s_prioritized")
                for token in ["NextBestAction", "SuggestedTool", "LeadId", "PriorityScore"]
            ),
            detail="Manager prioritized query includes NextBestAction, SuggestedTool, LeadId, and PriorityScore.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="manager_chrome_ratio_reasonable",
            passed=mgr_ratio <= 0.60,
            detail=f"Manager chrome ratio = {mgr_ratio:.1%}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_action_page_has_operating_queues",
            passed=all(name in mgr["state"]["widgets"] for name in ["p4_tbl_priority", "p4_tbl_response", "p4_tbl_upcoming", "p4_tbl_target", "p4_tbl_reengage"]),
            detail="Manager action page includes priority, responder, Stage 2 -> 3, named-account, and re-engagement queues.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_has_rep_load_mix_view",
            passed="p1_tbl_rep" in mgr["state"]["widgets"]
            and all(
                token in _step_query(mgr, "s_rep_table")
                for token in [
                    "OpenMQLLeadCount",
                    "OpenSQLLeadCount",
                    "MarketingDisqualifiedLeadCount",
                    "SalesDisqualifiedLeadCount",
                    "OpportunityHandoffCount",
                    "PendingStage3ReviewCount",
                    "Stage3ApprovedCount",
                    "OpenLeadCount",
                ]
            ),
            detail="Manager dashboard should expose rep lead load plus MQL/SQL/handoff mix.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_has_product_segment_and_handoff_diagnostics",
            passed=all(
                name in mgr["state"]["widgets"]
                for name in ["p3_ch_product", "p3_tbl_source_product", "p4s_tbl_persona", "p4s_tbl_industry", "p4s_tbl_segment", "p4s_tbl_stage", "p4_tbl_upcoming"]
            )
            and all(
                token in _step_query(mgr, "s_persona_product")
                for token in ["ProductFocus", "ResponseRatePct", "LeadToOppPct", "KnownAttributedARR"]
            )
            and all(
                token in _step_query(mgr, "s_source_product")
                for token in ["SourceGroup", "ProductFocus", "QualifiedCount", "LeadToOppPct"]
            )
            and all(
                token in _step_query(mgr, "s_stage3_queue")
                for token in [
                    "SourcedOpportunityName",
                    "SourcedOpportunityStage",
                    "HandoffQualityBand",
                    "Stage2To3Days",
                    "ProductFocus",
                    "KnownAttributedARR",
                ]
            ),
            detail="Manager dashboard should diagnose campaign/product, persona/product, segment performance, and expose an opportunity-native Stage 2 -> 3 handoff queue.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_has_meeting_speed_metric",
            passed="p2_tbl_exec" in mgr["state"]["widgets"]
            and "AvgDaysToFirstMeeting" in _step_query(mgr, "s_rep_execution")
            and _widget(mgr, "p2_tbl_exec").get("parameters", {}).get("step") == "s_rep_execution",
            detail="Manager rep scorecard should include average days to first meeting.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_has_pipeline_monthly_view",
            passed="p3_ch_week" in mgr["state"]["widgets"]
            and all(
                token in _step_query(mgr, "s_pipeline_monthly")
                for token in ["MonthStartDate", "OpenOpportunityCount", "DiscoveryHandoffCount", "OpportunityHandoffCount"]
            )
            and _widget(mgr, "p3_ch_week").get("parameters", {}).get("step") == "s_pipeline_monthly",
            detail="Manager dashboard should show monthly BDR pipeline and handoffs.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_has_industry_outreach_view",
            passed="p4s_tbl_industry" in mgr["state"]["widgets"]
            and all(
                token in _step_query(mgr, "s_industry_outreach")
                for token in ["Industry", "ContactTouchCount", "ActiveCoveragePct", "MeetingHeldCount", "OpenOpportunityCount", "OpportunityHandoffCount"]
            )
            and _widget(mgr, "p4s_tbl_industry").get("parameters", {}).get("step") == "s_industry_outreach",
            detail="Manager dashboard should show industry outreach, meetings, and opportunity handoffs.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_has_open_opp_product_arr_view",
            passed="p4s_tbl_stage" in mgr["state"]["widgets"]
            and all(
                token in _step_query(mgr, "s_open_opp_product")
                for token in ["ProductFocus", "OpenOpportunityCount", "DiscoveryHandoffCount", "OpportunityHandoffCount", "KnownAttributedARR"]
            )
            and _widget(mgr, "p4s_tbl_stage").get("parameters", {}).get("step") == "s_open_opp_product",
            detail="Manager dashboard should show open BDR opportunity mix by product and ARR.",
        )
    )
    checks.append(
        AuditCheck(
            category="process",
            name="manager_has_logging_integrity_view",
            passed="p2_tbl_integrity" in mgr["state"]["widgets"]
            and all(
                token in _step_query(mgr, "s_rep_integrity")
                for token in [
                    "DirectLeadTouch24hPct",
                    "AssociatedTouch24hPct",
                    "LeadLinkedActivityPct",
                    "ContactLinkedActivityPct",
                    "AccountLinkedActivityPct",
                    "TotalActivityCount",
                ]
            ),
            detail="Manager dashboard should show strict lead SLA, associated prospect response, and whether BDR activity is logged on leads versus contacts/accounts.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_has_rep_handoff_scorecard",
            passed="p2_tbl_coach" in mgr["state"]["widgets"]
            and all(
                token in _step_query(mgr, "s_rep_handoff")
                for token in [
                    "Stage2DiscoveryCount",
                    "Stage3EngagementCount",
                    "PendingStage3ReviewCount",
                    "Stage3ApprovedCount",
                    "AvgStage2To3Days",
                ]
            ),
            detail="Manager dashboard should show a rep Stage 2 -> 3 handoff scorecard by owner.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_has_client_vs_prospect_outreach_mix",
            passed="p2_tbl_mix" in mgr["state"]["widgets"]
            and all(
                token in _step_query(mgr, "s_outreach_mix")
                for token in [
                    "ProspectActivityCount",
                    "CurrentClientActivityCount",
                    "FormerClientActivityCount",
                    "PartnerActivityCount",
                    "UnclassifiedActivityCount",
                ]
            ),
            detail="Manager dashboard should show client vs prospect outreach mix by BDR.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_has_yoy_rhythm_view",
            passed="p1_tbl_yoy" in mgr["state"]["widgets"]
            and all(
                token in _step_query(mgr, "s_yoy_rhythm")
                for token in ["FY2026", "FY2025", "LeadCreatedCount", "MeetingHeldCount", "QualifiedCount", "KnownAttributedARR"]
            ),
            detail="Manager overview should compare FY2025 vs FY2026 lead, meeting, handoff, and known ARR rhythm.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="manager_has_rep_activity_mix_chart",
            passed="p2_ch_activity_mix" in mgr["state"]["widgets"]
            and all(
                token in _step_query(mgr, "s_rep_activity_mix")
                for token in ["OwnerName", "CallCount", "EmailCount", "MeetingHeldCount"]
            ),
            detail="Manager dashboard should make rep activity mix visible without forcing managers to parse only tables.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="manager_has_diagnostic_mix",
            passed=all(viz in _viz_counts(mgr) for viz in ["line", "comparisontable", "hbar", "heatmap", "flatgauge"]),
            detail=f"Manager viz mix = {sorted(_viz_counts(mgr))}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="manager_summary_uses_supported_target_vs_actual_widgets",
            passed=all(name in mgr["state"]["widgets"] for name in ["p1_b_sla", "p1_b_integrity", "p1_b_source"])
            and all(
                _widget(mgr, name).get("parameters", {}).get("visualizationType") in {"hbar", "flatgauge"}
                for name in ["p1_b_sla", "p1_b_integrity", "p1_b_source"]
            )
            and all(
                token in _step_query(mgr, step_name)
                for step_name in ["s_sla_bullet", "s_integrity_bullet", "s_source_bullet"]
                for token in ["MetricLabel", "Actual", "Target"]
            ),
            detail="Manager overview should include stable supported target-vs-actual widgets for 24h SLA, associated prospect response, and source completeness.",
        )
    )
    checks.append(
        AuditCheck(
            category="runtime",
            name="manager_story_step_uses_real_weekly_lead_field",
            passed="sum(LeadCreatedCount)" in _step_query(mgr, "s_story"),
            detail="Weekly summary story must be backed by LeadCreatedCount in rep_week rows, not a missing field.",
        )
    )

    return {
        "live_export_dir": str(live_export_dir),
        "rep_dashboard": rep["label"],
        "manager_dashboard": mgr["label"],
        "rep_widget_count": len(rep["state"].get("widgets", {})),
        "manager_widget_count": len(mgr["state"].get("widgets", {})),
        "rep_chrome_ratio": rep_ratio,
        "manager_chrome_ratio": mgr_ratio,
        "checks": [asdict(check) for check in checks],
        "pass_count": sum(1 for check in checks if check.passed),
        "fail_count": sum(1 for check in checks if not check.passed),
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# BDR Operating System Audit",
        "",
        f"- Live export: `{payload['live_export_dir']}`",
        f"- Rep dashboard: `{payload['rep_dashboard']}`",
        f"- Manager dashboard: `{payload['manager_dashboard']}`",
        f"- Rep chrome ratio: `{payload['rep_chrome_ratio']:.1%}`",
        f"- Manager chrome ratio: `{payload['manager_chrome_ratio']:.1%}`",
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
            "rep_dashboard": payload["rep_dashboard"],
            "manager_dashboard": payload["manager_dashboard"],
            "pass_count": payload["pass_count"],
            "fail_count": payload["fail_count"],
            "rep_widget_count": payload["rep_widget_count"],
            "manager_widget_count": payload["manager_widget_count"],
            "rep_chrome_ratio": payload["rep_chrome_ratio"],
            "manager_chrome_ratio": payload["manager_chrome_ratio"],
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
