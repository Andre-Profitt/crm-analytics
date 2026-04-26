#!/usr/bin/env python3
"""Build validated regional fact packs and PowerPoint prompts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from monthly_platform.quarterly_pipeline import (
        quarterly_pipeline_display_from_snapshot,
    )
except ModuleNotFoundError:  # pragma: no cover
    from scripts.monthly_platform.quarterly_pipeline import (
        quarterly_pipeline_display_from_snapshot,
    )


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SHELL_CONTRACT_PATH = REPO_ROOT / "config" / "sales_region_monthly_shell.json"
DEFAULT_GOLD_REFERENCE_PATH = (
    REPO_ROOT
    / "output"
    / "sales_region_gold_decks"
    / "2026-04-10"
    / "Sales Region Monthly - EMEA Gold Example.pptx"
)


def load_snapshot(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_shell_contract(path: Path = DEFAULT_SHELL_CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def as_number(value: Any) -> float:
    if value in (None, "", "—", "-"):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value).replace(",", "").replace("€", "").replace("EUR", "").replace("%", "").replace("M", ""))


def fmt_eur(amount: float) -> str:
    value = float(amount or 0)
    if abs(value) >= 1_000_000:
        return f"€{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"€{value / 1_000:.0f}K"
    return f"€{value:,.0f}"


def fmt_count(value: Any) -> str:
    return f"{int(round(float(value or 0))):,}"


def pct(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator * 100.0


def markdown_section(lines: list[str], heading: str, bullets: list[str]) -> None:
    lines.append(f"## {heading}")
    lines.extend(f"- {bullet}" for bullet in bullets)
    lines.append("")


def top_n(rows: list[dict[str, Any]], key: str, n: int = 3) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: float(row.get(key) or 0), reverse=True)[:n]


def safe_list(rows: Any) -> list[dict[str, Any]]:
    return list(rows or [])


def format_watchlist_rows(
    rows: list[dict[str, Any]],
    *,
    amount_key: str,
    limit: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in top_n(rows, amount_key, n=limit):
        out.append(
            {
                "opportunity": as_text(row.get("Opportunity")),
                "owner": as_text(row.get("Owner")),
                "territory": as_text(row.get("Territory") or row.get("Region") or row.get("Book")),
                "forecast_category": as_text(row.get("Forecast Category")),
                "stage": as_text(row.get("Stage") or row.get("Stage Name") or row.get("StageName")),
                "close_date": as_text(row.get("Close Date")),
                "arr_eur": fmt_eur(as_number(row.get(amount_key))),
                "renewal_acv_eur": fmt_eur(as_number(row.get("Renewal ACV (€ converted)"))),
                "reason": as_text(row.get("Reason Won/Lost") or row.get("Reason")),
                "next_action": as_text(row.get("Next Action") or row.get("Action")),
            }
        )
    return out


def compact_watchlist_text(rows: list[dict[str, Any]], *, amount_key: str, limit: int = 3) -> str:
    items = [
        f"{as_text(row.get('Opportunity'))} ({fmt_eur(as_number(row.get(amount_key)))}"
        + (f", {as_text(row.get('Forecast Category'))}" if as_text(row.get("Forecast Category")) else "")
        + ")"
        for row in top_n(rows, amount_key, n=limit)
        if as_text(row.get("Opportunity"))
    ]
    return ", ".join(items) or "n/a"


def derive_top_risk(snapshot: dict[str, Any]) -> str:
    scorecard = (snapshot.get("scorecard") or {}).get("sections") or {}
    risk = (scorecard.get("risk") or {}).get("metrics") or {}
    process = (scorecard.get("process-compliance") or {}).get("metrics") or {}
    stale_arr = as_text(risk.get("Stale 30d+ (ARR)")) or "€0"
    aging_arr = as_text(risk.get("Aging 365+ (ARR)")) or "€0"
    missing = fmt_count(process.get("Missing Approval (Land, stage 3+)"))
    return (
        f"Execution pressure remains elevated with {stale_arr} stale ARR, {aging_arr} aged 365+ ARR, "
        f"and {missing} stage 3+ deals still missing approval."
    )


def derive_top_action(snapshot: dict[str, Any]) -> str:
    commercial = snapshot.get("commercial_approval") or {}
    missing_candidates = safe_list(commercial.get("missing_candidates"))
    if missing_candidates:
        top = top_n(missing_candidates, "ARR (€ converted)", n=1)[0]
        return (
            f"Force commercial approval decisions on the largest missing candidate, "
            f"{as_text(top.get('Opportunity'))} at {fmt_eur(as_number(top.get('ARR (€ converted)')))} ARR."
        )
    renewals = snapshot.get("renewals") or {}
    q2_renewals = safe_list(renewals.get("q2_open_renewals"))
    if q2_renewals:
        top = top_n(q2_renewals, "Renewal ACV (€ converted)", n=1)[0]
        return (
            f"Prioritize executive follow-up on the largest Q2 renewal, "
            f"{as_text(top.get('Opportunity'))} at {fmt_eur(as_number(top.get('Renewal ACV (€ converted)')))} ACV."
        )
    return "No single escalation dominates; use the deck watchlists to prioritize book-owner follow-up."


def coverage_statement(snapshot: dict[str, Any]) -> str:
    pipeline = (((snapshot.get("scorecard") or {}).get("sections") or {}).get("pipeline-health") or {}).get("metrics") or {}
    display = resolve_quarterly_pipeline_display(snapshot)
    display_quarter = (display.get("display_quarter") or {})
    q_rows = safe_list(display_quarter.get("top_active_opportunities")) or safe_list(
        ((snapshot.get("pipeline_detail") or {}).get("q2_active_opportunities"))
    )
    quarter_title = as_text(display_quarter.get("title")) or "Current quarter"
    active_arr = fmt_eur(as_number(display_quarter.get("active_arr")))
    return (
        "Quota and targets are not available in the current workbook contract, so coverage is qualified. "
        f"Use {quarter_title} active ARR of {active_arr} "
        f"and weighted ARR of {as_text(pipeline.get('Weighted Pipeline (probability-adj)')) or '—'} as the current proxy, "
        f"with concentration visible in the top {len(q_rows[:3])} {as_text(display_quarter.get('label')) or 'quarter'} opportunities."
    )


def renewal_risk_summary(snapshot: dict[str, Any]) -> str:
    risk_rows = safe_list((snapshot.get("renewals") or {}).get("risk_levels"))
    if not risk_rows:
        return "Renewal risk tagging is unavailable in the current snapshot."
    top = risk_rows[0]
    if as_text(top.get("Risk Level")).lower() in {"unspecified", "unknown", ""}:
        return (
            f"Renewal risk tagging is sparse: top bucket is {as_text(top.get('Risk Level')) or 'Unspecified'} "
            f"with {fmt_count(top.get('Deal Count'))} deals and {fmt_eur(as_number(top.get('ACV (€ converted)')))} ACV."
        )
    return (
        f"Top tagged renewal bucket is {as_text(top.get('Risk Level'))} with "
        f"{fmt_count(top.get('Deal Count'))} deals and {fmt_eur(as_number(top.get('ACV (€ converted)')))} ACV."
    )


def slip_root_cause_summary(snapshot: dict[str, Any]) -> str:
    rows = safe_list(((snapshot.get("q1_review") or {}).get("forecast_movement_summary")))
    if not rows:
        return "Root-cause summary is incomplete and requires book-owner follow-up."
    top = rows[0]
    return (
        f"The largest validated movement is {as_text(top.get('from')) or 'Unknown'} -> {as_text(top.get('to')) or 'Unknown'} "
        f"across {fmt_count(top.get('count'))} deals carrying {fmt_eur(as_number(top.get('arr')))} ARR. "
        "Owner commentary is still required for full root-cause depth."
    )


def sources_lineage_summary(snapshot: dict[str, Any]) -> str:
    paths = safe_list(snapshot.get("source_snapshot_paths"))
    return (
        f"Regional rollup built from {len(paths)} validated director snapshots using the model "
        f"'{as_text(snapshot.get('rollup_model'))}'."
    )


def metric_definition_notes(snapshot: dict[str, Any]) -> list[str]:
    notes = [
        "Pipeline metrics are ARR in EUR converted unless a slide explicitly states otherwise.",
        "Renewal metrics are ACV in EUR converted.",
        "Omitted is kept visible but excluded from active headline pipeline.",
        "Q1 promise baselines are qualified and not treated as clean commitment numbers.",
    ]
    if snapshot.get("forecast_hierarchy_note"):
        notes.append(as_text(snapshot["forecast_hierarchy_note"]))
    return notes


def resolve_quarterly_pipeline_display(snapshot: dict[str, Any]) -> dict[str, Any]:
    display = snapshot.get("quarterly_pipeline_display")
    if isinstance(display, dict) and display.get("display_quarter"):
        return display
    return quarterly_pipeline_display_from_snapshot(snapshot)


def build_structured_fill_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    shell = load_shell_contract()
    scorecard = (snapshot.get("scorecard") or {}).get("sections") or {}
    pipeline = (scorecard.get("pipeline-health") or {}).get("metrics") or {}
    process = (scorecard.get("process-compliance") or {}).get("metrics") or {}
    risk = (scorecard.get("risk") or {}).get("metrics") or {}
    quarterly_pipeline = resolve_quarterly_pipeline_display(snapshot)
    display_quarter = (quarterly_pipeline.get("display_quarter") or {})
    display_q = display_quarter.get("by_category") or {}
    display_label = as_text(display_quarter.get("label")) or "Q2"
    display_title = as_text(display_quarter.get("title")) or display_label
    display_footnote = as_text(display_quarter.get("footnote"))
    commercial = snapshot.get("commercial_approval") or {}
    renewals = snapshot.get("renewals") or {}
    q1 = snapshot.get("q1_review") or {}
    data_quality = snapshot.get("data_quality") or {}
    q1_actuals = q1.get("actuals") or {}
    component_books = safe_list(snapshot.get("component_books"))
    ranked_books = sorted(component_books, key=lambda row: as_number(row.get("q2_arr")), reverse=True)
    largest_book = ranked_books[0] if ranked_books else {}
    weakest_book = ranked_books[-1] if ranked_books else {}
    q2_active_rows = safe_list(display_quarter.get("top_active_opportunities")) or safe_list(
        (snapshot.get("pipeline_detail") or {}).get("q2_active_opportunities")
    )
    lost_rows = [
        row for row in safe_list((snapshot.get("won_lost") or {}).get("lost"))
        if "external competitor" in as_text(row.get("Reason Won/Lost")).lower()
    ]
    missing_candidates = safe_list(commercial.get("missing_candidates"))
    renewal_summary = renewals.get("summary_metrics") or {}

    slots_by_slide: dict[str, dict[str, Any]] = {
        "executive-summary": {
            "headline_pipeline_arr_all_open": as_text(pipeline.get("Pipeline ARR — All Open (any close date)")) or "—",
            "headline_pipeline_arr_fy26": as_text(pipeline.get("Pipeline ARR — FY26 Close Dates Only (excl. Omitted)")) or "—",
            "headline_pipeline_arr_q2": as_text(pipeline.get("Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)")) or "—",
            "headline_renewal_acv": fmt_eur(as_number(renewal_summary.get("open_acv"))),
            "top_risk": derive_top_risk(snapshot),
            "top_action": derive_top_action(snapshot),
        },
        "q1-review": {
            "q1_won_count": fmt_count(q1_actuals.get("won_count")),
            "q1_won_arr": fmt_eur(as_number(q1_actuals.get("won_arr"))),
            "q1_lost_count": fmt_count(q1_actuals.get("lost_count")),
            "q1_lost_arr": fmt_eur(as_number(q1_actuals.get("lost_arr"))),
            "q1_slipped_count": fmt_count(q1_actuals.get("slipped_count")),
            "q1_slipped_arr": fmt_eur(as_number(q1_actuals.get("slipped_arr"))),
            "q1_promise_baseline_qualification": as_text(q1.get("scope_warning"))
            or "Promise baseline must be qualified.",
        },
        "quarterly-pipeline": {
            "headline_pipeline_arr_q2": fmt_eur(as_number(display_quarter.get("active_arr"))),
            "q2_commit_arr": fmt_eur(as_number((display_q.get("Commit") or {}).get("ARR (€ converted)"))),
            "q2_best_case_arr": fmt_eur(as_number((display_q.get("Best Case") or {}).get("ARR (€ converted)"))),
            "q2_omitted_arr": fmt_eur(as_number((display_q.get("Omitted") or {}).get("ARR (€ converted)"))),
            "quarterly_pipeline_label": display_label,
            "quarterly_pipeline_title": display_title,
            "quarterly_pipeline_display_reason": as_text(display_quarter.get("reason")) or "current_quarter",
            "quarterly_pipeline_footnote": display_footnote,
            "quarterly_pipeline_active_deal_count": fmt_count(display_quarter.get("active_deal_count")),
        },
        "regional-book-breakdown": {
            "regional_component_books": [
                {
                    "territory": as_text(row.get("territory")),
                    "director_name": as_text(row.get("director_name")),
                    "all_open_arr": fmt_eur(as_number(row.get("all_open_arr"))),
                    "fy26_arr": fmt_eur(as_number(row.get("fy26_arr"))),
                    "q2_arr": fmt_eur(as_number(row.get("q2_arr"))),
                    "approval_rate": as_text(row.get("approval_rate")) or "—",
                    "renewal_open_acv": fmt_eur(as_number(row.get("renewal_open_acv"))),
                }
                for row in ranked_books
            ],
            "largest_book_summary": (
                f"Largest Q2 book is {as_text(largest_book.get('territory'))} "
                f"({as_text(largest_book.get('director_name'))}) at {fmt_eur(as_number(largest_book.get('q2_arr')))} ARR."
                if largest_book
                else "No component-book summary available."
            ),
            "weakest_book_summary": (
                f"Smallest Q2 book is {as_text(weakest_book.get('territory'))} "
                f"({as_text(weakest_book.get('director_name'))}) at {fmt_eur(as_number(weakest_book.get('q2_arr')))} ARR."
                if weakest_book
                else "No component-book summary available."
            ),
        },
        "pipeline-coverage-intel": {
            "pipeline_coverage_statement": coverage_statement(snapshot),
            "weighted_pipeline_arr": as_text(pipeline.get("Weighted Pipeline (probability-adj)")) or "—",
            "top_opportunities": format_watchlist_rows(q2_active_rows, amount_key="ARR (€ converted)", limit=8),
            "stale_arr": as_text(risk.get("Stale 30d+ (ARR)")) or "—",
            "aging_arr": as_text(risk.get("Aging 365+ (ARR)")) or "—",
            "data_quality_backlog": fmt_count((data_quality.get("total") or {}).get("Total Issues")),
            "quarterly_pipeline_label": display_label,
            "quarterly_pipeline_title": display_title,
            "quarterly_pipeline_footnote": display_footnote,
            "competitive_loss_watchlist": format_watchlist_rows(lost_rows, amount_key="ARR (€ converted)", limit=5),
        },
        "commercial-approval-overview": {
            "approval_rate_stage3_plus": as_text(process.get("Approval Rate (stage 3+)")) or "—",
            "approved_deal_count": fmt_count(next((row.get("Deal Count") for row in safe_list(commercial.get("summary")) if as_text(row.get("Category")) == "Approved"), 0)),
            "approved_arr": fmt_eur(as_number(next((row.get("ARR (€ converted)") for row in safe_list(commercial.get("summary")) if as_text(row.get("Category")) == "Approved"), 0))),
            "pending_missing_approval_count": fmt_count(next((row.get("Deal Count") for row in safe_list(commercial.get("summary")) if as_text(row.get("Category")) == "Pending / Missing Approval"), 0)),
            "pending_missing_approval_arr": fmt_eur(as_number(next((row.get("ARR (€ converted)") for row in safe_list(commercial.get("summary")) if as_text(row.get("Category")) == "Pending / Missing Approval"), 0))),
            "missing_approval_candidate_count": fmt_count(len(missing_candidates)),
        },
        "missing-commercial-approvals": {
            "missing_approval_candidates": format_watchlist_rows(missing_candidates, amount_key="ARR (€ converted)", limit=12),
        },
        "renewals-retention": {
            "renewal_open_deal_count": fmt_count(renewal_summary.get("open_deal_count")),
            "renewal_open_acv": fmt_eur(as_number(renewal_summary.get("open_acv"))),
            "renewal_risk_bucket_summary": renewal_risk_summary(snapshot),
            "renewal_watchlist": format_watchlist_rows(safe_list(renewals.get("q2_open_renewals")), amount_key="Renewal ACV (€ converted)", limit=10),
        },
        "slipped-deals": {
            "q1_slipped_count": fmt_count(q1_actuals.get("slipped_count")),
            "q1_slipped_arr": fmt_eur(as_number(q1_actuals.get("slipped_arr"))),
            "slipped_deal_watchlist": format_watchlist_rows(safe_list(q1.get("pushed_deals")), amount_key="ARR (€ converted)", limit=10),
            "slip_root_cause_summary": slip_root_cause_summary(snapshot),
        },
        "churn-finance": {
            "finance_churn_inputs_status": "Finance churn input is not integrated into the workbook contract.",
            "finance_churn_owner": "Owner not yet wired into the snapshot contract.",
            "churn_placeholder_notes": "Keep this slide as an explicit placeholder until Finance-owned churn reporting is operationalized.",
        },
        "appendix-notes": {
            "sources_lineage_summary": sources_lineage_summary(snapshot),
            "metric_definition_notes": metric_definition_notes(snapshot),
            "data_quality_backlog": fmt_count((data_quality.get("total") or {}).get("Total Issues")),
            "q1_promise_baseline_qualification": as_text(q1.get("scope_warning"))
            or "Promise baseline must be qualified.",
        },
    }

    slides: list[dict[str, Any]] = []
    for slide in shell.get("slides", []):
        slide_id = slide["id"]
        slides.append(
            {
                "id": slide_id,
                "title": slide["title"],
                "support_level": (slide.get("data_contract") or {}).get("support_level"),
                "required_slots": slide.get("required_slots", []),
                "known_gaps": (slide.get("data_contract") or {}).get("known_gaps", []),
                "slots": slots_by_slide.get(slide_id, {}),
            }
        )
    return {
        "template_name": shell.get("template_name"),
        "region_name": snapshot.get("region_name"),
        "snapshot_date": snapshot.get("snapshot_date"),
        "slides": slides,
    }


def build_authoritative_brief(snapshot: dict[str, Any]) -> str:
    region_name = snapshot.get("region_name")
    scorecard = (snapshot.get("scorecard") or {}).get("sections") or {}
    pipeline = (scorecard.get("pipeline-health") or {}).get("metrics") or {}
    process = (scorecard.get("process-compliance") or {}).get("metrics") or {}
    risk = (scorecard.get("risk") or {}).get("metrics") or {}
    quarterly_pipeline = resolve_quarterly_pipeline_display(snapshot)
    display_quarter = (quarterly_pipeline.get("display_quarter") or {})
    display_q = display_quarter.get("by_category") or {}
    display_title = as_text(display_quarter.get("title")) or "Current quarter"
    display_footnote = as_text(display_quarter.get("footnote"))
    commercial = snapshot.get("commercial_approval") or {}
    renewals = snapshot.get("renewals") or {}
    q1 = snapshot.get("q1_review") or {}
    data_quality = snapshot.get("data_quality") or {}
    component_books = snapshot.get("component_books") or []

    top_books = ", ".join(
        f"{row.get('territory')} ({row.get('director_name')})" for row in component_books
    )
    q2_top_rows = safe_list(display_quarter.get("top_active_opportunities")) or (
        (snapshot.get("pipeline_detail") or {}).get("q2_active_opportunities") or []
    )
    top_opps = ", ".join(
        f"{as_text(row.get('Opportunity'))} ({fmt_eur(float(row.get('ARR (€ converted)') or 0))} ARR)"
        for row in top_n(q2_top_rows or (snapshot.get("pipeline_detail") or {}).get("top_opportunities") or [], "ARR (€ converted)")
    ) or "n/a"
    missing_candidates = commercial.get("missing_candidates") or []
    biggest_missing = missing_candidates[0] if missing_candidates else {}
    renewal_rows = renewals.get("open_renewals") or []
    renewal_risk_rows = renewals.get("risk_levels") or []
    top_renewal_risk = renewal_risk_rows[0] if renewal_risk_rows else {}
    renewal_summary = renewals.get("summary_metrics") or {}
    q1_actuals = q1.get("actuals") or {}
    won_count = float(q1_actuals.get("won_count") or 0)
    lost_count = float(q1_actuals.get("lost_count") or 0)
    won_arr = float(q1_actuals.get("won_arr") or 0)
    lost_arr = float(q1_actuals.get("lost_arr") or 0)
    slipped_count = float(q1_actuals.get("slipped_count") or 0)
    slipped_arr = float(q1_actuals.get("slipped_arr") or 0)
    count_win_rate = pct(won_count, won_count + lost_count)
    arr_win_rate = pct(won_arr, won_arr + lost_arr)
    comp_losses = [
        row for row in (snapshot.get("won_lost") or {}).get("lost") or []
        if "external competitor" in as_text(row.get("Reason Won/Lost")).lower()
    ]
    comp_loss_text = ", ".join(
        f"{as_text(row.get('Opportunity'))} ({fmt_eur(float(row.get('ARR (€ converted)') or 0))} ARR)"
        for row in top_n(comp_losses, "ARR (€ converted)")
    ) or "No explicit external-competitor losses in the top regional loss rows"
    dq_total = data_quality.get("total") or {}

    lines: list[str] = [
        f"# Validated Regional Fact Pack: {region_name}",
        "",
        f"Snapshot date: {snapshot.get('snapshot_date')}",
        "",
        "Use this as the authoritative bridge into PowerPoint Claude.",
        "This snapshot is a deterministic rollup of validated director snapshots, not a freehand regional narrative.",
        "",
    ]
    markdown_section(
        lines,
        "Regional Rollup",
        [
            f"Component books in this region: {top_books}.",
            snapshot.get("forecast_hierarchy_note") or "This regional deck uses the forecast-region rollup model defined in territory mappings.",
        ],
    )
    markdown_section(
        lines,
        "Pipeline Overview",
        [
            f"All-open pipeline is {pipeline.get('Pipeline ARR — All Open (any close date)', '—')} ARR across {fmt_count(pipeline.get('Deal Count'))} deals, with FY26 ARR at {pipeline.get('Pipeline ARR — FY26 Close Dates Only (excl. Omitted)', '—')}.",
            f"{display_title} active pipeline is {fmt_eur(as_number(display_quarter.get('active_arr')))} ARR. Weighted pipeline is {pipeline.get('Weighted Pipeline (probability-adj)', '—')} and new pipeline this quarter is {pipeline.get('New Pipeline This Quarter (excl. Omitted)', '—')}.",
            f"Top {as_text(display_quarter.get('label')) or 'quarter'}-active opportunities: {top_opps}.",
            *([display_footnote] if display_footnote else []),
        ],
    )
    markdown_section(
        lines,
        "Commercial Approval",
        [
            f"Approval rate for stage 3+ deals is {process.get('Approval Rate (stage 3+)', '—')}, with {fmt_count(process.get('Missing Approval (Land, stage 3+)'))} missing-approval candidates.",
            f"Regional approval-rate method: {as_text(commercial.get('approval_rate_method')) or 'See component-book scorecards.'}",
            f"Largest missing-approval candidate is {as_text(biggest_missing.get('Opportunity')) or 'n/a'} at {fmt_eur(float(biggest_missing.get('ARR (€ converted)') or 0))} ARR.",
        ],
    )
    markdown_section(
        lines,
        "Renewals",
        [
            f"Open renewals in this region: {fmt_count(renewal_summary.get('open_deal_count'))} deals totaling {fmt_eur(float(renewal_summary.get('open_acv') or 0))} ACV. Q2 subset is {fmt_count(renewal_summary.get('q2_open_deal_count'))} deals totaling {fmt_eur(float(renewal_summary.get('q2_open_acv') or 0))} ACV.",
            f"Top renewal risk bucket is {as_text(top_renewal_risk.get('Risk Level')) or 'n/a'} with {fmt_count(top_renewal_risk.get('Deal Count'))} deals and {fmt_eur(float(top_renewal_risk.get('ACV (€ converted)') or 0))} ACV.",
        ],
    )
    markdown_section(
        lines,
        "Q1 Promised vs Delivered",
        [
            f"Delivered Q1 actuals: {fmt_count(won_count)} won for {fmt_eur(won_arr)} ARR versus {fmt_count(lost_count)} lost for {fmt_eur(lost_arr)} ARR. Count win rate is {count_win_rate:.1f}% and ARR win rate is {arr_win_rate:.1f}%.",
            f"Slipped out of Q1: {fmt_count(slipped_count)} deals carrying {fmt_eur(slipped_arr)} ARR.",
            "Promise baseline is an aggregated pipeline-inspection population and should be qualified if used as a commitment statement.",
        ],
    )
    markdown_section(
        lines,
        "Coverage and Intel",
        [
            f"{display_title} mix is Commit {fmt_eur(float((display_q.get('Commit') or {}).get('ARR (€ converted)') or 0))} ARR, Best Case {fmt_eur(float((display_q.get('Best Case') or {}).get('ARR (€ converted)') or 0))} ARR, Pipeline {fmt_eur(float((display_q.get('Pipeline') or {}).get('ARR (€ converted)') or 0))} ARR, and Omitted {fmt_eur(float((display_q.get('Omitted') or {}).get('ARR (€ converted)') or 0))} ARR.",
            f"Stale 30d+ is {risk.get('Stale 30d+ (ARR)', '—')} ARR and aging 365+ is {risk.get('Aging 365+ (ARR)', '—')}.",
            f"Data quality backlog totals {fmt_count(dq_total.get('Total Issues'))} issue-points. Competitive-loss watchlist: {comp_loss_text}.",
        ],
    )
    return "\n".join(lines).strip() + "\n"


def build_powerpoint_build_prompt(snapshot: dict[str, Any], validated_brief: str) -> str:
    shell = load_shell_contract()
    fill_payload = build_structured_fill_payload(snapshot)
    slide_lines: list[str] = []
    for index, slide in enumerate(shell.get("slides", []), start=1):
        contract = slide.get("data_contract") or {}
        slide_lines.append(f"{index}. {slide['title']} (`{slide['id']}`)")
        if slide.get("subtitle"):
            slide_lines.append(f"   Intent: {slide['subtitle']}")
        if contract.get("support_level"):
            slide_lines.append(f"   Support level: {contract['support_level']}")
        slide_lines.append("   Required slots: " + ", ".join(f"`{slot}`" for slot in slide.get("required_slots", [])))
        if contract.get("source_tabs"):
            slide_lines.append("   Source tabs: " + ", ".join(f"`{tab}`" for tab in contract["source_tabs"]))
        if contract.get("known_gaps"):
            slide_lines.append("   Known gaps: " + "; ".join(contract["known_gaps"]))
    gold_reference_note = (
        f"Gold regional benchmark deck is available at: {DEFAULT_GOLD_REFERENCE_PATH}\n"
        if DEFAULT_GOLD_REFERENCE_PATH.exists()
        else ""
    )
    shell_block = "\n".join(slide_lines)
    payload_block = json.dumps(fill_payload, indent=2, ensure_ascii=True)
    return (
        f"Update the current regional PowerPoint deck for {snapshot.get('region_name')}.\n\n"
        "Use the SD PowerPoint Builder skill if it is available.\n\n"
        "Treat the validated regional fact pack below as the authoritative source of truth. "
        "The current deck is already the fixed SimCorp regional shell. Replace shell guidance with executive-ready content, "
        "but preserve the template, slide master, layouts, and branding.\n\n"
        f"{gold_reference_note}"
        "Fill this fixed regional shell:\n"
        f"{shell_block}\n\n"
        "Rules:\n"
        "- keep pipeline metrics labeled with ARR and explicit horizon\n"
        "- keep renewals labeled with ACV\n"
        "- keep Omitted separate from active headline pipeline\n"
        "- keep the region-book mapping explicit where needed; do not move ME&A out of EMEA\n"
        "- if churn inputs are missing, leave the churn slide as an explicit placeholder\n"
        "- if the Q1 promise baseline is ambiguous, qualify it rather than overstating certainty\n"
        "- where a slide is marked qualified, preserve the qualification in the wording and speaker text\n"
        "- where a slide is marked placeholder, keep it as a controlled placeholder and do not backfill invented numbers\n"
        "- do not leave shell guidance text visible in the finished deck\n"
        "- rewrite visible slide titles into message titles that state the conclusion, not just the topic\n"
        "- match the gold benchmark deck's density: short titles, 2-3 key takeaways, and no wall-of-text panels\n"
        "- keep watchlists and candidate lists structured and scannable; do not turn them into paragraphs\n"
        "- preserve the shell's visual families; do not redesign the deck\n"
        "- use the structured fill payload to populate named slots before doing any editorial rewrite\n"
        "- respond with exactly these headings:\n"
        "## Deck Changes\n"
        "## Remaining Gaps\n\n"
        "Validated regional fact pack:\n\n"
        f"{validated_brief}\n\n"
        "Structured fill payload (JSON):\n\n"
        f"```json\n{payload_block}\n```"
    )


def build_validation_artifacts(snapshot: dict[str, Any]) -> dict[str, Any]:
    validated_brief = build_authoritative_brief(snapshot)
    structured_fill_payload = build_structured_fill_payload(snapshot)
    powerpoint_build_prompt = build_powerpoint_build_prompt(snapshot, validated_brief)
    return {
        "validated_brief": validated_brief,
        "structured_fill_payload": structured_fill_payload,
        "powerpoint_build_prompt": powerpoint_build_prompt,
    }


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    snapshot = load_snapshot(args.snapshot)
    artifacts = build_validation_artifacts(snapshot)
    write_text(args.output_dir / "validated-fact-pack.md", artifacts["validated_brief"])
    write_text(
        args.output_dir / "powerpoint-fill-payload.json",
        json.dumps(artifacts["structured_fill_payload"], indent=2, ensure_ascii=True),
    )
    write_text(args.output_dir / "powerpoint-build-prompt.txt", artifacts["powerpoint_build_prompt"])
    print(json.dumps(
        {
            "validated_fact_pack": str(args.output_dir / "validated-fact-pack.md"),
            "powerpoint_fill_payload": str(args.output_dir / "powerpoint-fill-payload.json"),
            "powerpoint_build_prompt": str(args.output_dir / "powerpoint-build-prompt.txt"),
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
