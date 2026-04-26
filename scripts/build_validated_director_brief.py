#!/usr/bin/env python3
"""Build a validated fact pack between Excel Claude and PowerPoint Claude."""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter
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
DEFAULT_SHELL_CONTRACT_PATH = REPO_ROOT / "config" / "sales_director_monthly_shell.json"
DEFAULT_EXTERNAL_INPUTS_ROOT = REPO_ROOT / "output" / "sales_deck_external_inputs"


def load_external_inputs(
    snapshot_date: str, *, root: Path = DEFAULT_EXTERNAL_INPUTS_ROOT
) -> dict[str, Any]:
    """Load all provided external source files for a snapshot date."""
    inputs: dict[str, Any] = {}
    base = root / snapshot_date
    if not base.is_dir():
        return inputs
    for source_dir in sorted(base.iterdir()):
        if not source_dir.is_dir() or source_dir.name.startswith("."):
            continue
        export_json = source_dir / "export.json"
        overlay_json = source_dir / "overlay.json"
        if export_json.exists():
            inputs[source_dir.name] = json.loads(
                export_json.read_text(encoding="utf-8")
            )
        elif overlay_json.exists():
            inputs[source_dir.name] = json.loads(
                overlay_json.read_text(encoding="utf-8")
            )
    return inputs


def as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def as_number(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    token = str(value).replace(",", "").replace("€", "").replace("EUR", "").strip()
    try:
        return float(token)
    except ValueError:
        return 0.0


def fmt_eur(amount: float) -> str:
    value = float(amount or 0)
    if abs(value) >= 1_000_000:
        return f"€{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"€{value / 1_000:.0f}K"
    return f"€{value:,.0f}"


def fmt_count(value: Any) -> str:
    if value in (None, ""):
        return "0"
    return f"{int(round(as_number(value))):,}"


def pct(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator * 100.0


def slugify(name: str) -> str:
    token = re.sub(r"[^0-9A-Za-z]+", "-", (name or "").strip().lower()).strip("-")
    return token or "director"


def load_snapshot(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_shell_contract(path: Path = DEFAULT_SHELL_CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def markdown_section(lines: list[str], heading: str, bullets: list[str]) -> None:
    lines.append(f"## {heading}")
    lines.extend(f"- {bullet}" for bullet in bullets)
    lines.append("")


def top_n_opportunities(snapshot: dict[str, Any], n: int = 3) -> list[dict[str, Any]]:
    display = quarterly_pipeline_display_from_snapshot(snapshot)
    display_rows = (display.get("display_quarter") or {}).get(
        "top_active_opportunities"
    ) or []
    if display_rows:
        return display_rows[:n]
    q2_rows = (snapshot.get("q2_outlook") or {}).get("top_q2_active_opportunities") or []
    if q2_rows:
        return q2_rows[:n]
    return (snapshot.get("pipeline_detail") or {}).get("top_opportunities")[:n]


def renewal_summary(snapshot: dict[str, Any]) -> tuple[int, float, int, float]:
    renewals = snapshot.get("renewals") or {}
    summary = renewals.get("summary_metrics") or {}
    if summary:
        return (
            int(as_number(summary.get("open_deal_count"))),
            round(as_number(summary.get("open_acv")), 2),
            int(as_number(summary.get("q2_open_deal_count"))),
            round(as_number(summary.get("q2_open_acv")), 2),
        )
    rows = renewals.get("open_renewals") or []
    total_count = len(rows)
    total_acv = round(
        sum(as_number(row.get("Renewal ACV (€ converted)")) for row in rows), 2
    )
    return total_count, total_acv, 0, 0.0


def top_lost_competitive(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    lost_rows = (snapshot.get("won_lost") or {}).get("lost") or []
    filtered = [
        row
        for row in lost_rows
        if "external competitor" in as_text(row.get("Reason Won/Lost")).lower()
    ]
    filtered.sort(key=lambda row: as_number(row.get("ARR (€ converted)")), reverse=True)
    return filtered[:3]


def safe_list(rows: Any) -> list[dict[str, Any]]:
    return list(rows or [])


def format_watchlist_rows(
    rows: list[dict[str, Any]],
    *,
    amount_key: str,
    limit: int,
) -> list[dict[str, Any]]:
    ranked = sorted(rows, key=lambda row: as_number(row.get(amount_key)), reverse=True)[
        :limit
    ]
    out: list[dict[str, Any]] = []
    for row in ranked:
        out.append(
            {
                "opportunity": as_text(row.get("Opportunity")),
                "owner": as_text(row.get("Owner")),
                "stage": as_text(
                    row.get("Stage") or row.get("Stage Name") or row.get("StageName")
                ),
                "forecast_category": as_text(row.get("Forecast Category")),
                "close_date": as_text(row.get("Close Date")),
                "arr_eur": fmt_eur(as_number(row.get(amount_key))),
                "renewal_acv_eur": fmt_eur(
                    as_number(row.get("Renewal ACV (€ converted)"))
                ),
                "reason": as_text(row.get("Reason Won/Lost") or row.get("Reason")),
                "next_action": as_text(row.get("Next Action") or row.get("Action")),
                "activity_days_ago": fmt_count(row.get("Activity Days Ago")),
                "push_count": fmt_count(row.get("Push Count")),
            }
        )
    return out


def total_data_quality_issues(snapshot: dict[str, Any]) -> int:
    total = (snapshot.get("data_quality") or {}).get("total") or {}
    if "Total Issues" in total:
        return int(as_number(total.get("Total Issues")))
    skip = {"Rep", "Owner", "Total", "Notes"}
    return int(sum(as_number(value) for key, value in total.items() if key not in skip))


def missing_win_loss_reason_rows(
    snapshot: dict[str, Any], *, limit: int | None = 10
) -> list[dict[str, Any]]:
    rows = (snapshot.get("won_lost") or {}).get("records") or []
    missing: list[dict[str, Any]] = []
    for row in rows:
        reason = as_text(row.get("Reason Won/Lost"))
        stage = as_text(row.get("Stage")).lower()
        if reason:
            continue
        if stage == "0 - no opportunity":
            continue
        missing.append(row)
    missing.sort(key=lambda row: as_number(row.get("ARR (€ converted)")), reverse=True)
    if limit is None:
        return missing
    return missing[:limit]


def overdue_close_open_rows(
    snapshot: dict[str, Any], *, limit: int | None = 10
) -> list[dict[str, Any]]:
    snapshot_date = as_text(snapshot.get("snapshot_date"))[:10]
    rows = (snapshot.get("pipeline_detail") or {}).get("records") or []
    overdue = [
        row
        for row in rows
        if as_text(row.get("Close Date"))[:10]
        and as_text(row.get("Close Date"))[:10] < snapshot_date
    ]
    overdue.sort(
        key=lambda row: (
            -as_number(row.get("ARR (€ converted)")),
            as_text(row.get("Close Date")),
            as_text(row.get("Opportunity")),
        )
    )
    if limit is None:
        return overdue
    return overdue[:limit]


def overdue_close_owner_summary(snapshot: dict[str, Any]) -> list[tuple[str, int]]:
    rows = overdue_close_open_rows(snapshot, limit=None)
    counts = Counter(as_text(row.get("Owner")) or "Unknown" for row in rows)
    return counts.most_common(5)


def top_issue_rep_text(snapshot: dict[str, Any]) -> str:
    rows = (snapshot.get("data_quality") or {}).get("top_issues") or []
    parts = []
    for row in rows[:3]:
        parts.append(
            f"{as_text(row.get('Rep'))} ({fmt_count(row.get('Total Issues'))} issue-points)"
        )
    return ", ".join(parts) or "n/a"


def rep_concentration_summary(snapshot: dict[str, Any]) -> str:
    rows = (snapshot.get("rep_performance") or {}).get("top_reps") or []
    if not rows:
        return "Rep concentration summary unavailable."
    top = rows[0]
    return (
        f"Top pipeline carrier is {as_text(top.get('Rep'))} with "
        f"{fmt_eur(as_number(top.get('Open Pipeline ARR (€ converted)')))} open ARR across "
        f"{fmt_count(top.get('Deal Count'))} deals, plus {fmt_count(top.get('Stale Deals'))} stale deals "
        f"and {fmt_count(top.get('Missing Approvals'))} missing approvals."
    )


def top_risk_register_text(snapshot: dict[str, Any]) -> str:
    rows = (snapshot.get("risk_register") or {}).get("top_arr") or []
    parts = []
    for row in rows[:3]:
        parts.append(
            f"{as_text(row.get('Opportunity'))} ({fmt_eur(as_number(row.get('ARR (€ converted)')))} ARR, "
            f"{fmt_count(row.get('Activity Days Ago'))} activity days, {fmt_count(row.get('Push Count'))} pushes)"
        )
    return ", ".join(parts) or "n/a"


def coverage_statement(snapshot: dict[str, Any]) -> str:
    pipeline = (
        (
            ((snapshot.get("scorecard") or {}).get("sections") or {}).get(
                "pipeline-health"
            )
        )
        or {}
    ).get("metrics") or {}
    display = quarterly_pipeline_display_from_snapshot(snapshot)
    display_quarter = (display.get("display_quarter") or {})
    q_rows = top_n_opportunities(snapshot, 3)
    quarter_title = as_text(display_quarter.get("title")) or "Current quarter"
    active_arr = fmt_eur(as_number(display_quarter.get("active_arr")))
    return (
        "Quota and targets are not available in the current workbook contract, so coverage remains qualified. "
        f"Use {quarter_title} active ARR of {active_arr} "
        f"and weighted ARR of {as_text(pipeline.get('Weighted Pipeline (probability-adj)')) or '—'} as the current proxy, "
        f"with concentration visible in the top {len(q_rows)} {as_text(display_quarter.get('label')) or 'quarter'} opportunities."
    )


def renewal_risk_summary(snapshot: dict[str, Any]) -> str:
    risk_rows = safe_list((snapshot.get("renewals") or {}).get("risk_levels"))
    if not risk_rows:
        return "Renewal risk tagging is unavailable in the current snapshot."
    top = risk_rows[0]
    risk_level = as_text(top.get("Risk Level")) or "Unspecified"
    if risk_level.lower() in {"unspecified", "unknown"}:
        return (
            f"Renewal risk tagging is sparse: top bucket is {risk_level} with "
            f"{fmt_count(top.get('Deal Count'))} deals and {fmt_eur(as_number(top.get('ACV (€ converted)')))} ACV."
        )
    return (
        f"Top renewal risk bucket is {risk_level} with "
        f"{fmt_count(top.get('Deal Count'))} deals and {fmt_eur(as_number(top.get('ACV (€ converted)')))} ACV."
    )


def slip_root_cause_summary(snapshot: dict[str, Any]) -> str:
    rows = safe_list((snapshot.get("q1_review") or {}).get("forecast_movement_summary"))
    if not rows:
        return "Validated slip drivers require owner follow-up for root-cause depth."
    top = rows[0]
    return (
        f"Largest validated movement is {as_text(top.get('from')) or 'Unknown'} -> {as_text(top.get('to')) or 'Unknown'} "
        f"across {fmt_count(top.get('count'))} deals carrying {fmt_eur(as_number(top.get('arr')))} ARR. "
        "Owner commentary is still required for full root-cause explanation."
    )


def sources_lineage_summary(snapshot: dict[str, Any]) -> str:
    return (
        "Validated fact pack built from workbook tabs: Scorecard, Pipeline Detail, Q1 Review, Won-Lost, "
        "Q2 Outlook, Commercial Approval, Renewals & Retention, Risk Register, and Data Quality."
    )


def metric_definition_notes(snapshot: dict[str, Any]) -> list[str]:
    return [
        "Pipeline metrics are ARR in EUR converted unless a slide explicitly states otherwise.",
        "Renewal metrics are ACV in EUR converted.",
        "Omitted remains visible but excluded from active headline pipeline.",
        as_text((snapshot.get("q1_review") or {}).get("scope_warning"))
        or "Q1 promise baselines must be qualified before they are used as commitment language.",
    ]


def derive_top_risk(snapshot: dict[str, Any]) -> str:
    scorecard = (snapshot.get("scorecard") or {}).get("sections") or {}
    risk = (scorecard.get("risk") or {}).get("metrics") or {}
    process = (scorecard.get("process-compliance") or {}).get("metrics") or {}
    return (
        f"Execution pressure remains elevated with {as_text(risk.get('Stale 30d+ (ARR)')) or '€0'} stale ARR, "
        f"{as_text(risk.get('Aging 365+ (ARR)')) or '€0'} aging 365+ ARR, and "
        f"{fmt_count(process.get('Missing Approval (Land, stage 3+)'))} stage 3+ land deals missing approval."
    )


def derive_top_action(snapshot: dict[str, Any]) -> str:
    missing_candidates = safe_list(
        (snapshot.get("commercial_approval") or {}).get("missing_candidates")
    )
    if missing_candidates:
        top = missing_candidates[0]
        return (
            f"Force approval decisions on {as_text(top.get('Opportunity'))} at "
            f"{fmt_eur(as_number(top.get('ARR (€ converted)')))} ARR and clean the remaining stage 3+ approval backlog."
        )
    q2_renewals = safe_list((snapshot.get("renewals") or {}).get("q2_open_renewals"))
    if q2_renewals:
        top = sorted(
            q2_renewals,
            key=lambda row: as_number(row.get("Renewal ACV (€ converted)")),
            reverse=True,
        )[0]
        return (
            f"Prioritize executive follow-up on {as_text(top.get('Opportunity'))} at "
            f"{fmt_eur(as_number(top.get('Renewal ACV (€ converted)')))} ACV in the Q2 renewal watchlist."
        )
    return "Use the approval, slip, and hygiene watchlists to set the next leadership actions."


def _build_churn_slots(ext: dict[str, Any]) -> dict[str, Any]:
    churn = ext.get("finance_churn_overlay") or {}
    if churn.get("status") == "placeholder" or not churn:
        return {
            "finance_churn_inputs_status": "Placeholder — Finance churn overlay not yet provided.",
            "finance_churn_owner": as_text(churn.get("owner"))
            or "Pending Finance handoff",
            "churn_placeholder_notes": as_text(churn.get("summary_note"))
            or "Keep this slide as an explicit placeholder until Finance-owned churn reporting is operationalized.",
        }
    return {
        "finance_churn_inputs_status": as_text(churn.get("headline"))
        or "Finance churn overlay provided.",
        "finance_churn_owner": as_text(churn.get("owner")),
        "churn_placeholder_notes": as_text(churn.get("summary_note")),
        "churn_top_accounts": churn.get("top_accounts") or [],
    }


_INTERNAL_ACCOUNT_PATTERNS = re.compile(
    r"(?i)\bsc\s+test\b|^sc$|^sc\s|simcorp\s*q[tu]c|^simcorp\b|^clm.simcorp\b"
)


def _is_internal_account(account_name: str) -> bool:
    """Return True if the account name looks like an internal/test SimCorp account."""
    return bool(_INTERNAL_ACCOUNT_PATTERNS.search(account_name.strip()))


def _filter_records(
    records: list[dict[str, Any]], account_key: str = "Account"
) -> list[dict[str, Any]]:
    """Remove records belonging to internal/test accounts."""
    return [r for r in records if not _is_internal_account(as_text(r.get(account_key)))]


def _resolve_director_region(snapshot: dict[str, Any]) -> str:
    """Get the Sales Region value for this director from their pipeline records."""
    records = (snapshot.get("pipeline_detail") or {}).get("records") or []
    for r in records:
        region = r.get("Region", "")
        if region:
            return region
    return ""


def _build_approved_deals_2026(
    ext: dict[str, Any], director_region: str
) -> list[dict[str, Any]]:
    approved = ext.get("commercial_approval_approved_2026_salesforce") or {}
    records = approved.get("records") or []
    filtered = (
        [r for r in records if r.get("sales_region") == director_region]
        if director_region
        else records
    )
    return [
        {
            "opportunity": as_text(r.get("opportunity_name")),
            "owner": as_text(r.get("opportunity_owner")),
            "stage": as_text(r.get("stage")),
            "close_date": as_text(r.get("close_date")),
            "arr": fmt_eur(as_number(r.get("forecast_arr_eur"))),
            "approval_date": as_text(r.get("commercial_approval_date")),
        }
        for r in filtered
    ]


def _build_candidates_for_director(
    ext: dict[str, Any], director_region: str
) -> list[dict[str, Any]]:
    candidates = ext.get("commercial_approval_candidates_salesforce") or {}
    records = candidates.get("records") or []
    filtered = (
        [r for r in records if r.get("sales_region") == director_region]
        if director_region
        else records
    )
    return [
        {
            "opportunity": as_text(r.get("opportunity_name")),
            "owner": as_text(r.get("opportunity_owner")),
            "stage": as_text(r.get("stage")),
            "close_date": as_text(r.get("close_date")),
            "arr": fmt_eur(as_number(r.get("opportunity_arr_eur"))),
            "age": str(int(as_number(r.get("age")))),
        }
        for r in filtered
    ]


def _build_commercial_approval_slots(
    process: dict[str, Any],
    approved_row: dict[str, Any],
    pending_row: dict[str, Any],
    commercial: dict[str, Any],
    snapshot: dict[str, Any],
    ext: dict[str, Any],
) -> dict[str, Any]:
    """Build commercial approval slots using workbook detail lists as primary source.

    The workbook approved_ytd and missing_candidates lists are already territory-scoped.
    The SF export provides the deal detail tables but may have coarser region granularity.
    The workbook summary row ('Approved' count) is broader than 2026-only — do not use it.
    """
    director_region = _resolve_director_region(snapshot)

    # Approved deals: use workbook approved_ytd (territory-scoped) for count/ARR,
    # SF export for the detail table
    wb_approved_ytd = safe_list(commercial.get("approved_ytd"))
    sf_approved_detail = _build_approved_deals_2026(ext, director_region)

    if wb_approved_ytd:
        approved_count = len(wb_approved_ytd)
        approved_arr = sum(
            as_number(r.get("ARR (€ converted)")) for r in wb_approved_ytd
        )
    elif sf_approved_detail:
        approved_count = len(sf_approved_detail)
        raw = (ext.get("commercial_approval_approved_2026_salesforce") or {}).get(
            "records", []
        )
        region = (
            [r for r in raw if r.get("sales_region") == director_region]
            if director_region
            else raw
        )
        approved_arr = sum(as_number(r.get("forecast_arr_eur")) for r in region)
    else:
        approved_count = 0
        approved_arr = 0.0

    # Missing candidates: use workbook list (territory-scoped), SF export for detail table
    wb_missing = safe_list(commercial.get("missing_candidates"))
    candidates_sf = _build_candidates_for_director(ext, director_region)
    candidate_count = len(wb_missing) if wb_missing else len(candidates_sf)

    return {
        "approval_rate_stage3_plus": as_text(process.get("Approval Rate (stage 3+)"))
        or "—",
        "approved_deal_count": fmt_count(approved_count),
        "approved_arr": fmt_eur(approved_arr),
        "pending_missing_approval_count": fmt_count(pending_row.get("Deal Count")),
        "pending_missing_approval_arr": fmt_eur(
            as_number(pending_row.get("ARR (€ converted)"))
        ),
        "missing_approval_candidate_count": fmt_count(candidate_count),
        "approved_deals_2026": sf_approved_detail,
        "missing_approval_candidates_sf": candidates_sf,
    }


def _build_kyc_status(ext: dict[str, Any]) -> str:
    kyc = ext.get("kyc_not_completed_salesforce") or {}
    records = kyc.get("records") or []
    if not records:
        return "KYC-missing Salesforce source is not yet integrated into the validated fact pack."
    count = len(records)
    names = ", ".join(r.get("account_name", "?") for r in records[:5])
    total_opps = sum(int(r.get("heat_map_open_opportunities") or 0) for r in records)
    return (
        f"{count} prospect account(s) with open opportunities have KYC Not Started: {names}. "
        f"{total_opps} open opportunity(ies) affected."
    )


def _is_fy26(record: dict[str, Any]) -> bool:
    """Return True if the record's close date is within FY26 (≤ 2026-12-31)."""
    close = as_text(record.get("Close Date") or record.get("close_date"))
    if not close:
        return True  # keep records without a close date
    # Handle both YYYY-MM-DD and DD.MM.YYYY formats
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            from datetime import datetime

            dt = datetime.strptime(close, fmt)
            return dt.year <= 2026
        except ValueError:
            continue
    return True  # keep if unparseable


def _clean_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of the snapshot with internal/test accounts and FY27+ data removed."""
    import copy

    snap = copy.deepcopy(snapshot)

    # Filter all record lists: remove internal accounts and FY27+ close dates
    pd = snap.get("pipeline_detail") or {}
    if pd.get("records"):
        pd["records"] = [r for r in _filter_records(pd["records"]) if _is_fy26(r)]
    if pd.get("top_opportunities"):
        pd["top_opportunities"] = [
            r for r in _filter_records(pd["top_opportunities"]) if _is_fy26(r)
        ]

    renewals = snap.get("renewals") or {}
    for key in ("open_renewals", "q2_open_renewals"):
        if renewals.get(key):
            renewals[key] = [r for r in _filter_records(renewals[key]) if _is_fy26(r)]

    ca = snap.get("commercial_approval") or {}
    for key in ("missing_candidates", "approved_ytd"):
        if ca.get(key):
            ca[key] = _filter_records(ca[key])

    wl = snap.get("won_lost") or {}
    for key in ("won", "lost", "records"):
        if wl.get(key):
            wl[key] = _filter_records(wl[key])

    rr = snap.get("risk_register") or {}
    if rr.get("top_arr"):
        rr["top_arr"] = _filter_records(rr["top_arr"])

    q1 = snap.get("q1_review") or {}
    if q1.get("pushed_deals"):
        q1["pushed_deals"] = _filter_records(q1["pushed_deals"])

    q2 = snap.get("q2_outlook") or {}
    for key in ("commit_deals", "best_case_deals", "top_q2_active_opportunities"):
        if q2.get(key):
            q2[key] = _filter_records(q2[key])

    return snap


def build_structured_fill_payload(
    snapshot: dict[str, Any],
    external_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Clean internal/test accounts before building payload
    snapshot = _clean_snapshot(snapshot)

    shell = load_shell_contract()
    scorecard = (snapshot.get("scorecard") or {}).get("sections") or {}
    pipeline = (scorecard.get("pipeline-health") or {}).get("metrics") or {}
    process = (scorecard.get("process-compliance") or {}).get("metrics") or {}
    risk = (scorecard.get("risk") or {}).get("metrics") or {}
    quarterly_pipeline = quarterly_pipeline_display_from_snapshot(snapshot)
    display_quarter = (quarterly_pipeline.get("display_quarter") or {})
    display_q = display_quarter.get("by_category") or {}
    display_label = as_text(display_quarter.get("label")) or "Q2"
    display_title = as_text(display_quarter.get("title")) or display_label
    display_footnote = as_text(display_quarter.get("footnote"))
    renewals = snapshot.get("renewals") or {}
    renewal_risk_rows = safe_list(renewals.get("risk_levels"))
    commercial = snapshot.get("commercial_approval") or {}
    commercial_summary = safe_list(commercial.get("summary"))
    q1 = snapshot.get("q1_review") or {}
    q1_actuals = q1.get("actuals") or {}
    data_quality = (snapshot.get("data_quality") or {}).get("total") or {}
    missing_reason_rows = missing_win_loss_reason_rows(snapshot, limit=None)
    overdue_rows = overdue_close_open_rows(snapshot, limit=None)
    overdue_owner_summary = overdue_close_owner_summary(snapshot)
    renewal_count, renewal_acv, _, _ = renewal_summary(snapshot)
    q2_renewals = safe_list(renewals.get("q2_open_renewals")) or safe_list(
        renewals.get("open_renewals")
    )

    def summary_row(category: str) -> dict[str, Any]:
        return next(
            (
                row
                for row in commercial_summary
                if as_text(row.get("Category")) == category
            ),
            {},
        )

    approved_row = summary_row("Approved")
    pending_row = summary_row("Pending / Missing Approval")

    slots_by_slide: dict[str, dict[str, Any]] = {
        "executive-summary": {
            "headline_pipeline_arr_all_open": as_text(
                pipeline.get("Pipeline ARR — All Open (any close date)")
            )
            or "—",
            "headline_pipeline_arr_fy26": as_text(
                pipeline.get("Pipeline ARR — FY26 Close Dates Only (excl. Omitted)")
            )
            or "—",
            "headline_pipeline_arr_q2": as_text(
                pipeline.get("Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)")
            )
            or "—",
            "headline_renewal_acv": fmt_eur(renewal_acv),
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
            or "Promise baseline must be qualified before it is used as commitment language.",
        },
        "quarterly-pipeline": {
            "headline_pipeline_arr_q2": fmt_eur(
                as_number(display_quarter.get("active_arr"))
            ),
            "q2_commit_arr": fmt_eur(
                as_number((display_q.get("Commit") or {}).get("ARR (€ converted)"))
            ),
            "q2_best_case_arr": fmt_eur(
                as_number((display_q.get("Best Case") or {}).get("ARR (€ converted)"))
            ),
            "q2_omitted_arr": fmt_eur(
                as_number((display_q.get("Omitted") or {}).get("ARR (€ converted)"))
            ),
            "quarterly_pipeline_label": display_label,
            "quarterly_pipeline_title": display_title,
            "quarterly_pipeline_display_reason": as_text(
                display_quarter.get("reason")
            )
            or "current_quarter",
            "quarterly_pipeline_footnote": display_footnote,
            "quarterly_pipeline_active_deal_count": fmt_count(
                display_quarter.get("active_deal_count")
            ),
        },
        "pipeline-coverage-intel": {
            "pipeline_coverage_statement": coverage_statement(snapshot),
            "weighted_pipeline_arr": as_text(
                pipeline.get("Weighted Pipeline (probability-adj)")
            )
            or "—",
            "top_opportunities": format_watchlist_rows(
                top_n_opportunities(snapshot, 8),
                amount_key="ARR (€ converted)",
                limit=8,
            ),
            "stale_arr": as_text(risk.get("Stale 30d+ (ARR)")) or "—",
            "aging_arr": as_text(risk.get("Aging 365+ (ARR)")) or "—",
            "data_quality_backlog": fmt_count(total_data_quality_issues(snapshot)),
            "quarterly_pipeline_label": display_label,
            "quarterly_pipeline_title": display_title,
            "quarterly_pipeline_footnote": display_footnote,
            "competitive_loss_watchlist": format_watchlist_rows(
                top_lost_competitive(snapshot),
                amount_key="ARR (€ converted)",
                limit=5,
            ),
        },
        "commercial-approval-overview": _build_commercial_approval_slots(
            process,
            approved_row,
            pending_row,
            commercial,
            snapshot,
            external_inputs or {},
        ),
        "missing-commercial-approvals": {
            "missing_approval_candidates": format_watchlist_rows(
                safe_list(commercial.get("missing_candidates")),
                amount_key="ARR (€ converted)",
                limit=12,
            ),
        },
        "renewals-retention": {
            "renewal_open_deal_count": fmt_count(renewal_count),
            "renewal_open_acv": fmt_eur(renewal_acv),
            "renewal_risk_bucket_summary": renewal_risk_summary(snapshot),
            "renewal_watchlist": format_watchlist_rows(
                q2_renewals,
                amount_key="Renewal ACV (€ converted)",
                limit=10,
            ),
        },
        "slipped-deals": {
            "q1_slipped_count": fmt_count(q1_actuals.get("slipped_count")),
            "q1_slipped_arr": fmt_eur(as_number(q1_actuals.get("slipped_arr"))),
            "slipped_deal_watchlist": format_watchlist_rows(
                safe_list(q1.get("pushed_deals")),
                amount_key="ARR (€ converted)",
                limit=10,
            ),
            "slip_root_cause_summary": slip_root_cause_summary(snapshot),
        },
        "salesforce-hygiene-activity": {
            "no_activity_count": fmt_count(data_quality.get("No Activity")),
            "overdue_close_count": fmt_count(len(overdue_rows)),
            "missing_next_step_count": fmt_count(data_quality.get("Missing Next Step")),
            "total_data_quality_issues": fmt_count(total_data_quality_issues(snapshot)),
            "top_issue_reps": [
                {
                    "rep": as_text(row.get("Rep")),
                    "total_issues": fmt_count(row.get("Total Issues")),
                    "no_activity": fmt_count(row.get("No Activity")),
                    "overdue_close": fmt_count(row.get("Overdue Close")),
                    "missing_next_step": fmt_count(row.get("Missing Next Step")),
                }
                for row in safe_list(
                    (snapshot.get("data_quality") or {}).get("top_issues")
                )[:5]
            ],
            "rep_concentration_summary": rep_concentration_summary(snapshot),
            "top_risk_register_items": format_watchlist_rows(
                safe_list((snapshot.get("risk_register") or {}).get("top_arr")),
                amount_key="ARR (€ converted)",
                limit=5,
            ),
        },
        "missing-win-loss-reason": {
            "missing_win_loss_reason_count": fmt_count(len(missing_reason_rows)),
            "missing_win_loss_reason_rows": format_watchlist_rows(
                missing_reason_rows,
                amount_key="ARR (€ converted)",
                limit=10,
            ),
            "missing_win_loss_reason_rule_note": "Treat `0 - No Opportunity` with no reason as acceptable; all other blank-reason outcomes require hygiene follow-up.",
        },
        "overdue-close-open-opps": {
            "overdue_close_count": fmt_count(len(overdue_rows)),
            "overdue_close_watchlist": format_watchlist_rows(
                overdue_rows,
                amount_key="ARR (€ converted)",
                limit=10,
            ),
            "overdue_close_owner_summary": [
                {"owner": owner, "record_count": count}
                for owner, count in overdue_owner_summary
            ],
        },
        "churn-finance": _build_churn_slots(external_inputs or {}),
        "appendix-notes": {
            "sources_lineage_summary": sources_lineage_summary(snapshot),
            "metric_definition_notes": metric_definition_notes(snapshot),
            "data_quality_backlog": fmt_count(total_data_quality_issues(snapshot)),
            "q1_promise_baseline_qualification": as_text(q1.get("scope_warning"))
            or "Promise baseline must be qualified before it is used as commitment language.",
            "kyc_missing_status": _build_kyc_status(external_inputs or {}),
        },
    }

    slides: list[dict[str, Any]] = []
    for slide in shell.get("slides", []):
        slide_id = slide["id"]
        slides.append(
            {
                "id": slide_id,
                "title": slide["title"],
                "management_question": slide.get("management_question"),
                "visual_family": slide.get("visual_family"),
                "action_seam": slide.get("action_seam"),
                "title_rewrite_rule": slide.get("title_rewrite_rule"),
                "density_limit": slide.get("density_limit") or {},
                "anti_patterns": slide.get("anti_patterns") or [],
                "support_level": (slide.get("data_contract") or {}).get(
                    "support_level"
                ),
                "required_slots": slide.get("required_slots", []),
                "known_gaps": (slide.get("data_contract") or {}).get("known_gaps", []),
                "slots": slots_by_slide.get(slide_id, {}),
            }
        )
    return {
        "template_name": shell.get("template_name"),
        "director_name": snapshot.get("director_name"),
        "territory": snapshot.get("territory"),
        "snapshot_date": snapshot.get("snapshot_date"),
        "slides": slides,
    }


def build_authoritative_brief(snapshot: dict[str, Any]) -> str:
    snapshot = _clean_snapshot(snapshot)
    scorecard = (snapshot.get("scorecard") or {}).get("sections") or {}
    pipeline = (scorecard.get("pipeline-health") or {}).get("metrics") or {}
    process = (scorecard.get("process-compliance") or {}).get("metrics") or {}
    risk = (scorecard.get("risk") or {}).get("metrics") or {}
    quarterly_pipeline = quarterly_pipeline_display_from_snapshot(snapshot)
    display_quarter = (quarterly_pipeline.get("display_quarter") or {})
    display_q = display_quarter.get("by_category") or {}
    display_title = as_text(display_quarter.get("title")) or "Current quarter"
    display_footnote = as_text(display_quarter.get("footnote"))
    renewals = snapshot.get("renewals") or {}
    q1 = snapshot.get("q1_review") or {}
    data_quality = (snapshot.get("data_quality") or {}).get("total") or {}
    commercial = snapshot.get("commercial_approval") or {}
    commercial_summary = commercial.get("summary") or []
    missing_candidates = commercial.get("missing_candidates") or []

    top_opps = top_n_opportunities(snapshot, 3)
    top_opp_text = (
        ", ".join(
            f"{as_text(row.get('Opportunity'))} ({fmt_eur(as_number(row.get('ARR (€ converted)')))} ARR)"
            for row in top_opps
        )
        or "n/a"
    )

    renewal_count, renewal_acv, q2_renewal_count, q2_renewal_acv = renewal_summary(
        snapshot
    )
    renewal_risk = (renewals.get("risk_levels") or [{}])[0]
    best_case_arr = as_number(
        (display_q.get("Best Case") or {}).get("ARR (€ converted)")
    )
    commit_arr = as_number((display_q.get("Commit") or {}).get("ARR (€ converted)"))
    pipeline_arr = as_number(
        (display_q.get("Pipeline") or {}).get("ARR (€ converted)")
    )
    omitted_arr = as_number((display_q.get("Omitted") or {}).get("ARR (€ converted)"))
    won_count = int(as_number((q1.get("actuals") or {}).get("won_count")))
    lost_count = int(as_number((q1.get("actuals") or {}).get("lost_count")))
    won_arr = as_number((q1.get("actuals") or {}).get("won_arr"))
    lost_arr = as_number((q1.get("actuals") or {}).get("lost_arr"))
    slipped_count = int(as_number((q1.get("actuals") or {}).get("slipped_count")))
    slipped_arr = as_number((q1.get("actuals") or {}).get("slipped_arr"))
    count_win_rate = pct(won_count, won_count + lost_count)
    arr_win_rate = pct(won_arr, won_arr + lost_arr)
    biggest_missing = missing_candidates[0] if missing_candidates else {}
    summary_lookup = {
        as_text(row.get("Category")): row
        for row in commercial_summary
        if isinstance(row, dict)
    }
    approved_row = summary_lookup.get("Approved", {})
    pending_row = summary_lookup.get("Pending / Missing Approval", {})
    no_approval_row = summary_lookup.get("No Approval Needed", {})
    comp_losses = top_lost_competitive(snapshot)
    missing_reason_rows = missing_win_loss_reason_rows(snapshot, limit=None)
    overdue_rows = overdue_close_open_rows(snapshot, limit=None)
    owner_summary = overdue_close_owner_summary(snapshot)

    lines: list[str] = [
        f"# Validated Fact Pack: {snapshot.get('director_name')} ({snapshot.get('territory')})",
        "",
        f"Snapshot date: {snapshot.get('snapshot_date')}",
        "",
        "Use this as the authoritative bridge from Excel Claude into PowerPoint Claude.",
        "Treat the workbook-native Q1 ForecastingItem block as global reference only.",
        "",
    ]

    markdown_section(
        lines,
        "Pipeline Overview",
        [
            (
                f"All-open pipeline is {pipeline.get('Pipeline ARR — All Open (any close date)', '—')} ARR across "
                f"{fmt_count(pipeline.get('Deal Count'))} deals, with FY26 close-date ARR at "
                f"{pipeline.get('Pipeline ARR — FY26 Close Dates Only (excl. Omitted)', '—')}."
            ),
            (
                f"{display_title} active pipeline is {fmt_eur(as_number(display_quarter.get('active_arr')))} ARR. "
                f"Weighted pipeline is {pipeline.get('Weighted Pipeline (probability-adj)', '—')} and new pipeline this quarter is "
                f"{pipeline.get('New Pipeline This Quarter (excl. Omitted)', '—')}."
            ),
            (
                f"Top ARR opportunities: {top_opp_text}. Stale 30d+ is {risk.get('Stale 30d+ (ARR)', '—')} ARR and "
                f"aging 365+ is {risk.get('Aging 365+ (ARR)', '—')}."
            ),
            *([display_footnote] if display_footnote else []),
        ],
    )

    markdown_section(
        lines,
        "Commercial Approval",
        [
            (
                f"Approval rate for stage 3+ deals is {process.get('Approval Rate (stage 3+)', '—')}, with "
                f"{fmt_count(process.get('Missing Approval (Land, stage 3+)'))} missing-approval candidates."
            ),
            (
                f"Commercial summary shows {fmt_count(approved_row.get('Deal Count'))} approved deals at "
                f"{fmt_eur(as_number(approved_row.get('ARR (€ converted)')))} ARR, "
                f"{fmt_count(pending_row.get('Deal Count'))} pending/missing at "
                f"{fmt_eur(as_number(pending_row.get('ARR (€ converted)')))} ARR, and "
                f"{fmt_count(no_approval_row.get('Deal Count'))} marked no approval needed."
            ),
            (
                f"Largest missing-approval candidate is {as_text(biggest_missing.get('Opportunity')) or 'n/a'} at "
                f"{fmt_eur(as_number(biggest_missing.get('ARR (€ converted)')))} ARR."
            ),
        ],
    )

    markdown_section(
        lines,
        "Renewals",
        [
            (
                f"Open renewals in this director book: {renewal_count} deals totaling {fmt_eur(renewal_acv)} ACV. Q2 subset is {q2_renewal_count} deals totaling {fmt_eur(q2_renewal_acv)} ACV."
            ),
            (
                f"Renewal risk tagging is currently sparse: top risk bucket is "
                f"{as_text(renewal_risk.get('Risk Level')) or 'n/a'} with "
                f"{fmt_count(renewal_risk.get('Deal Count'))} deals and "
                f"{fmt_eur(as_number(renewal_risk.get('ACV (€ converted)')))} ACV."
            ),
            (
                "FY2025 retention KPIs in the workbook are org-wide reference metrics, not director-specific pipeline commitments."
            ),
        ],
    )

    markdown_section(
        lines,
        "Q1 Promised vs Delivered",
        [
            (
                f"Delivered Q1 actuals: {won_count} won for {fmt_eur(won_arr)} ARR versus {lost_count} lost for {fmt_eur(lost_arr)} ARR. "
                f"Count win rate is {count_win_rate:.1f}% and ARR win rate is {arr_win_rate:.1f}%."
            ),
            (
                f"Slipped out of Q1: {slipped_count} deals carrying {fmt_eur(slipped_arr)} ARR, based on director-scoped CloseDate history from cache."
            ),
            (
                "Promise baseline is ambiguous. Use workbook `Q1 Review` OwnerOnly ForecastingItem values only as global CRO reference; do not label them as the director commitment without qualification."
            ),
        ],
    )

    comp_loss_text = (
        ", ".join(
            f"{as_text(row.get('Opportunity'))} ({fmt_eur(as_number(row.get('ARR (€ converted)')))} ARR)"
            for row in comp_losses
        )
        or "No explicit external-competitor losses in top loss rows"
    )
    markdown_section(
        lines,
        "Coverage and Intel",
        [
            (
                f"{display_title} mix is Commit {fmt_eur(commit_arr)} ARR, Best Case {fmt_eur(best_case_arr)} ARR, Pipeline {fmt_eur(pipeline_arr)} ARR, and Omitted {fmt_eur(omitted_arr)} ARR."
            ),
            (
                f"Data quality backlog is {fmt_count(total_data_quality_issues(snapshot))} issue-points across the total row; "
                f"missing amount is {fmt_count(data_quality.get('Missing Amount'))} and missing next step is {fmt_count(data_quality.get('Missing Next Step'))}."
            ),
            (f"Competitive-loss watchlist from Won-Lost: {comp_loss_text}."),
        ],
    )
    markdown_section(
        lines,
        "Salesforce Hygiene and Activity Controls",
        [
            (
                f"Data quality total row shows {fmt_count(total_data_quality_issues(snapshot))} issue-points, "
                f"with {fmt_count(data_quality.get('No Activity'))} no-activity, "
                f"{fmt_count(data_quality.get('Overdue Close'))} overdue-close, "
                f"{fmt_count(data_quality.get('Missing Next Step'))} missing-next-step, and "
                f"{fmt_count(data_quality.get('Missing Amount'))} missing-amount flags."
            ),
            (
                f"Top issue reps: {top_issue_rep_text(snapshot)}. Rep concentration summary: {rep_concentration_summary(snapshot)}"
            ),
            (f"Top risk-register exposures: {top_risk_register_text(snapshot)}."),
        ],
    )
    markdown_section(
        lines,
        "Outcome and Overdue Controls",
        [
            (
                f"Missing win/loss reason control shows {len(missing_reason_rows)} materially ranked rows missing reason hygiene. "
                "Treat `0 - No Opportunity` with no reason as acceptable."
            ),
            (
                f"Overdue close-date open opportunities: {len(overdue_rows)} ranked rows in the watchlist. "
                f"Top owners by overdue-close count: {', '.join(f'{owner} ({count})' for owner, count in owner_summary) or 'n/a'}."
            ),
        ],
    )
    return "\n".join(lines).strip() + "\n"


def validate_excel_brief(snapshot: dict[str, Any], excel_brief: str) -> dict[str, Any]:
    q1 = snapshot.get("q1_review") or {}
    actuals = q1.get("actuals") or {}
    issues: list[dict[str, Any]] = []

    def add_issue(message: str, severity: str = "warn") -> None:
        issues.append({"severity": severity, "message": message})

    if not excel_brief.strip():
        add_issue(
            "No Excel Claude brief supplied; generated the fact pack directly from the validated snapshot.",
            severity="info",
        )
        return {"issues": issues}

    won = int(as_number(actuals.get("won_count")))
    lost = int(as_number(actuals.get("lost_count")))
    expected_count_win_rate = pct(won, won + lost)
    match = re.search(r"Win rate by count was ([0-9]+(?:\.[0-9]+)?)%", excel_brief)
    if match:
        reported = float(match.group(1))
        if not math.isclose(
            reported, expected_count_win_rate, rel_tol=0.0, abs_tol=0.2
        ):
            add_issue(
                f"Excel Claude count win rate {reported:.1f}% does not match validated {expected_count_win_rate:.1f}%.",
                severity="error",
            )

    expected_slipped_count = int(as_number(actuals.get("slipped_count")))
    slipped_match = re.search(r"([0-9]+) deals were pushed out of Q1", excel_brief)
    if slipped_match:
        reported = int(slipped_match.group(1))
        if reported != expected_slipped_count:
            add_issue(
                f"Excel Claude slipped-deal count {reported} does not match validated {expected_slipped_count}.",
                severity="error",
            )

    if (
        "org-wide CRO ForecastingItem" in excel_brief
        or "org-wide" in excel_brief.lower()
    ):
        add_issue(
            "Excel Claude correctly flagged the workbook Q1 promise baseline as global/ambiguous.",
            severity="info",
        )

    if not issues:
        add_issue(
            "No material validation discrepancies detected in the Excel Claude brief.",
            severity="info",
        )

    return {"issues": issues}


def build_powerpoint_prompt(
    snapshot: dict[str, Any], validated_brief: str, validation_report: dict[str, Any]
) -> str:
    issue_lines = [
        f"- {item['severity'].upper()}: {item['message']}"
        for item in validation_report.get("issues", [])
    ]
    issues_block = (
        "\n".join(issue_lines)
        if issue_lines
        else "- INFO: No validation issues recorded."
    )
    return (
        f"Review this PowerPoint deck for {snapshot.get('director_name')} ({snapshot.get('territory')}).\n\n"
        "Use the SD Deck Audit skill if it is available.\n\n"
        "Use the validated fact pack below as the authoritative source of truth. "
        "Do not trust ambiguous or unlabeled numbers already in the deck over this fact pack.\n\n"
        "Respond with exactly these headings:\n"
        "## Missing Slides\n"
        "## Number Framing\n"
        "## Narrative Gaps\n"
        "## Immediate Fixes\n\n"
        "Requirements:\n"
        "- stay factual to the current deck contents plus the validated fact pack\n"
        "- identify weak or missing coverage for quarterly pipeline overview, commercial approval gaps, renewals, Q1 promised vs delivered, slipped deals/root causes, churn placeholder, pipeline coverage, and opportunity-detail support\n"
        "- identify weak or missing coverage for Salesforce hygiene/activity, missing win/loss reason control, and overdue close open opportunities\n"
        "- call out ambiguous metric labels like pipeline without ARR/ACV/horizon\n"
        "- where the deck is wrong or incomplete, reference the validated fact pack rather than inventing numbers\n"
        "- do not ask follow-up questions\n\n"
        "Validation notes from Codex:\n"
        f"{issues_block}\n\n"
        "Validated fact pack:\n\n"
        f"{validated_brief}"
    )


def build_powerpoint_build_prompt(
    snapshot: dict[str, Any],
    validated_brief: str,
    validation_report: dict[str, Any],
    external_inputs: dict[str, Any] | None = None,
) -> str:
    shell = load_shell_contract()
    structured_fill_payload = build_structured_fill_payload(snapshot, external_inputs)
    issue_lines = [
        f"- {item['severity'].upper()}: {item['message']}"
        for item in validation_report.get("issues", [])
        if item["severity"] in {"error", "warn"}
    ]
    issues_block = (
        "\n".join(issue_lines)
        if issue_lines
        else "- No validation discrepancies currently open."
    )
    slide_lines: list[str] = []
    for index, slide in enumerate(shell.get("slides", []), start=1):
        slide_lines.append(f"{index}. {slide['title']} (`{slide['id']}`)")
        if slide.get("subtitle"):
            slide_lines.append(f"   Intent: {slide['subtitle']}")
        if slide.get("management_question"):
            slide_lines.append(
                f"   Management question: {slide['management_question']}"
            )
        if slide.get("visual_family"):
            slide_lines.append(f"   Visual family: {slide['visual_family']}")
        if slide.get("action_seam"):
            slide_lines.append(f"   Action seam: {slide['action_seam']}")
        if slide.get("density_limit"):
            slide_lines.append(
                "   Density limit: "
                + json.dumps(
                    slide.get("density_limit"), ensure_ascii=True, sort_keys=True
                )
            )
        slide_lines.append(
            "   Required slots: "
            + ", ".join(f"`{slot}`" for slot in slide.get("required_slots", []))
        )
        anti_patterns = slide.get("anti_patterns") or []
        if anti_patterns:
            slide_lines.append("   Avoid: " + "; ".join(anti_patterns))
    shell_block = "\n".join(slide_lines)
    return (
        f"Update the current PowerPoint deck for {snapshot.get('director_name')} ({snapshot.get('territory')}).\n\n"
        "Use the SD PowerPoint Builder skill if it is available.\n\n"
        "Treat the validated fact pack below as the authoritative source of truth. "
        "Rewrite or update the deck in place so it becomes the canonical executive SimCorp monthly presentation, "
        "while preserving the current template, slide master, layouts, fonts, and branding.\n\n"
        "The current deck is already the fixed monthly shell built from the SimCorp template. "
        "Do not invent a new structure or leave internal placeholder guidance visible. "
        "Replace the shell guidance with executive-ready content for these slides:\n"
        f"{shell_block}\n\n"
        "Rules:\n"
        "- keep all pipeline metrics labeled with ARR and time horizon\n"
        "- keep renewal metrics labeled with ACV\n"
        "- do not hide Omitted inside active headline pipeline\n"
        "- every slide must answer its management question; do not merge multiple decisions into one slide\n"
        "- use message titles in the populated deck, not topic titles\n"
        "- honor the slide-specific visual family and density limit from the shell contract\n"
        "- keep Salesforce controls factual: no activity, overdue close, missing next step, missing win/loss reason, and approval gaps should remain operational control views rather than narrative filler\n"
        "- if Finance churn inputs are missing, keep churn as an explicit placeholder rather than inventing content\n"
        "- KYC missing is not yet in the validated fact pack; if referenced, keep it as a source-gap note rather than fabricated content\n"
        "- if the Q1 promise baseline is ambiguous, state that clearly rather than overstating certainty\n"
        "- use the structured fill payload JSON below as the primary slot map for the shell; use the fact pack for nuance and message-title wording\n"
        "- prefer concise executive slides over dense table dumps\n"
        "- when the shell already has placeholder guidance, replace it with polished executive text rather than freewriting new sections\n"
        "- if a shell slot cannot be supported from the validated fact pack, leave a crisp placeholder note instead of fabricating content\n"
        "- after updating the deck, respond with exactly these headings:\n"
        "## Deck Changes\n"
        "## Remaining Gaps\n\n"
        "Validation notes from Codex:\n"
        f"{issues_block}\n\n"
        "Validated fact pack:\n\n"
        f"{validated_brief}\n\n"
        "Structured fill payload (JSON):\n\n"
        f"{json.dumps(structured_fill_payload, indent=2, ensure_ascii=True)}"
    )


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def build_validation_artifacts(
    snapshot: dict[str, Any],
    excel_brief: str,
    external_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validated_brief = build_authoritative_brief(snapshot)
    structured_fill_payload = build_structured_fill_payload(snapshot, external_inputs)
    validation_report = validate_excel_brief(snapshot, excel_brief)
    powerpoint_prompt = build_powerpoint_prompt(
        snapshot, validated_brief, validation_report
    )
    powerpoint_build_prompt = build_powerpoint_build_prompt(
        snapshot, validated_brief, validation_report, external_inputs
    )
    return {
        "validated_brief": validated_brief,
        "structured_fill_payload": structured_fill_payload,
        "validation_report": validation_report,
        "powerpoint_prompt": powerpoint_prompt,
        "powerpoint_build_prompt": powerpoint_build_prompt,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--excel-brief", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--snapshot-date", default="2026-04-10")
    parser.add_argument(
        "--external-inputs-root", type=Path, default=DEFAULT_EXTERNAL_INPUTS_ROOT
    )
    args = parser.parse_args()

    snapshot = load_snapshot(args.snapshot)
    excel_brief = args.excel_brief.read_text(encoding="utf-8")
    external_inputs = load_external_inputs(
        args.snapshot_date, root=args.external_inputs_root
    )
    artifacts = build_validation_artifacts(snapshot, excel_brief, external_inputs)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_text(args.output_dir / "validated-fact-pack.md", artifacts["validated_brief"])
    write_text(
        args.output_dir / "powerpoint-fill-payload.json",
        json.dumps(artifacts["structured_fill_payload"], indent=2, ensure_ascii=True),
    )
    write_text(
        args.output_dir / "powerpoint-validated-prompt.txt",
        artifacts["powerpoint_prompt"],
    )
    write_text(
        args.output_dir / "powerpoint-build-prompt.txt",
        artifacts["powerpoint_build_prompt"],
    )
    write_text(args.output_dir / "excel-raw-brief.txt", excel_brief)
    write_text(
        args.output_dir / "validation-report.json",
        json.dumps(artifacts["validation_report"], indent=2),
    )
    print(
        json.dumps(
            {
                "validated_fact_pack": str(args.output_dir / "validated-fact-pack.md"),
                "powerpoint_fill_payload": str(
                    args.output_dir / "powerpoint-fill-payload.json"
                ),
                "powerpoint_prompt": str(
                    args.output_dir / "powerpoint-validated-prompt.txt"
                ),
                "powerpoint_build_prompt": str(
                    args.output_dir / "powerpoint-build-prompt.txt"
                ),
                "validation_report": str(args.output_dir / "validation-report.json"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
