#!/usr/bin/env python3
"""Build deterministic regional snapshots from validated director snapshots."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
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

try:
    from territory_mapping import get_director_book, get_forecast_rollup_for_region
except ModuleNotFoundError:  # pragma: no cover
    from scripts.territory_mapping import get_director_book, get_forecast_rollup_for_region


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIRECTOR_SNAPSHOT_ROOT = REPO_ROOT / "output" / "director_workbook_snapshots"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "sales_region_snapshots"
DIRECTOR_ORDER = [
    "Jesper Tyrer",
    "Sarah Pittroff",
    "Francois Thaury",
    "Dan Peppett",
    "Christian Ebbesen",
    "Mourad Essofi",
    "Megan Miceli",
    "Patrick Gaughan",
    "Adam Steinhaus",
]


def slugify(value: str) -> str:
    token = re.sub(r"[^0-9A-Za-z]+", "-", (value or "").strip().lower()).strip("-")
    return token or "region"


def as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def as_number(value: Any) -> float:
    if value in (None, "", "—", "-"):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    token = str(value).strip().replace("€", "").replace("EUR", "").replace(",", "")
    multiplier = 1.0
    if token.endswith("%"):
        token = token[:-1]
    if token.endswith("B"):
        multiplier = 1_000_000_000
        token = token[:-1]
    elif token.endswith("M"):
        multiplier = 1_000_000
        token = token[:-1]
    elif token.endswith("K"):
        multiplier = 1_000
        token = token[:-1]
    try:
        return float(token) * multiplier
    except ValueError:
        return 0.0


def compact_eur(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"€{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"€{value / 1_000:.0f}K"
    return f"€{value:,.0f}"


def compact_pct(value: float) -> str:
    return f"{value:.1f}%"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def quarter_window(snapshot_date: str) -> tuple[str, str]:
    dt = datetime.fromisoformat(as_text(snapshot_date)[:10]).date()
    quarter_start_month = ((dt.month - 1) // 3) * 3 + 1
    quarter_start = date(dt.year, quarter_start_month, 1)
    if quarter_start_month == 10:
        next_quarter = date(dt.year + 1, 1, 1)
    else:
        next_quarter = date(dt.year, quarter_start_month + 3, 1)
    quarter_end = next_quarter - timedelta(days=1)
    return quarter_start.isoformat(), quarter_end.isoformat()


def is_within_window(date_text: Any, start: str, end: str) -> bool:
    token = as_text(date_text)[:10]
    return bool(token) and start <= token <= end


def director_names_for_region(region_name: str) -> list[str]:
    matched: list[str] = []
    for director_name in DIRECTOR_ORDER:
        book = get_director_book(director_name)
        if get_forecast_rollup_for_region(book["sales_region"]) == region_name:
            matched.append(director_name)
    return matched


def director_snapshot_path(snapshot_root: Path, snapshot_date: str, director_name: str) -> Path:
    return snapshot_root / snapshot_date / f"{slugify(director_name)}.json"


def sum_category_rows(rows: list[dict[str, Any]], *, count_key: str, amount_key: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        category = as_text(row.get("Category") or row.get("Forecast Category") or row.get("Risk Level")) or "Unknown"
        if category not in grouped:
            grouped[category] = {count_key: 0, amount_key: 0.0}
            if "Category" in row:
                grouped[category]["Category"] = category
            elif "Forecast Category" in row:
                grouped[category]["Forecast Category"] = category
            else:
                grouped[category]["Risk Level"] = category
        grouped[category][count_key] += int(round(as_number(row.get(count_key))))
        grouped[category][amount_key] += as_number(row.get(amount_key))
    out = list(grouped.values())
    out.sort(key=lambda item: -as_number(item.get(amount_key)))
    for item in out:
        item[amount_key] = round(as_number(item.get(amount_key)), 2)
    return out


def top_rows(rows: list[dict[str, Any]], *, key: str, limit: int) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: as_number(row.get(key)), reverse=True)
    return ordered[:limit]


def director_q2_active_opportunities(snapshot: dict[str, Any], snapshot_date: str) -> list[dict[str, Any]]:
    q2 = snapshot.get("q2_outlook") or {}
    explicit = q2.get("top_q2_active_opportunities")
    if explicit:
        return list(explicit)
    start, end = quarter_window(snapshot_date)
    records = ((snapshot.get("pipeline_detail") or {}).get("records") or [])
    filtered = [
        row
        for row in records
        if is_within_window(row.get("Close Date"), start, end)
        and as_text(row.get("Forecast Category")).lower() != "omitted"
    ]
    filtered.sort(key=lambda row: -as_number(row.get("ARR (€ converted)")))
    return filtered[:10]


def director_q2_renewals(snapshot: dict[str, Any], snapshot_date: str) -> list[dict[str, Any]]:
    renewals = snapshot.get("renewals") or {}
    explicit = renewals.get("q2_open_renewals")
    if explicit:
        return list(explicit)
    start, end = quarter_window(snapshot_date)
    rows = renewals.get("open_renewals") or []
    filtered = [
        row
        for row in rows
        if is_within_window(row.get("Close Date"), start, end)
    ]
    filtered.sort(key=lambda row: -as_number(row.get("Renewal ACV (€ converted)")))
    return filtered[:10]


def build_region_snapshot(
    *,
    region_name: str,
    snapshot_date: str,
    director_snapshot_root: Path = DEFAULT_DIRECTOR_SNAPSHOT_ROOT,
) -> dict[str, Any]:
    director_names = director_names_for_region(region_name)
    snapshots: list[dict[str, Any]] = []
    source_paths: list[str] = []
    pipeline_records: list[dict[str, Any]] = []
    for director_name in director_names:
        path = director_snapshot_path(director_snapshot_root, snapshot_date, director_name)
        if not path.exists():
            raise FileNotFoundError(f"Missing director snapshot for {director_name}: {path}")
        snapshots.append(load_json(path))
        source_paths.append(str(path))

    pipeline_detail_rows: list[dict[str, Any]] = []
    q2_active_opportunity_rows: list[dict[str, Any]] = []
    commit_rows: list[dict[str, Any]] = []
    best_case_rows: list[dict[str, Any]] = []
    missing_candidates: list[dict[str, Any]] = []
    approved_ytd: list[dict[str, Any]] = []
    open_renewals: list[dict[str, Any]] = []
    q2_open_renewals: list[dict[str, Any]] = []
    q1_pushed_deals: list[dict[str, Any]] = []
    forecast_movements: list[dict[str, Any]] = []
    won_rows: list[dict[str, Any]] = []
    lost_rows: list[dict[str, Any]] = []
    data_quality_records: list[dict[str, Any]] = []

    all_open_arr = fy26_arr = q2_arr = deal_count = weighted_arr = new_pipeline_arr = 0.0
    stale_count = stale_arr = pushed5_count = pushed5_arr = aging365_count = aging365_arr = 0.0
    approved_count = approved_arr = pending_count = pending_arr = no_approval_count = 0.0
    approval_rate_values: list[float] = []
    missing_approval_metric_count = 0.0
    renewal_deal_count = renewal_acv = q2_renewal_deal_count = q2_renewal_acv = 0.0
    q1_won_count = q1_won_arr = q1_lost_count = q1_lost_arr = q1_slipped_count = q1_slipped_arr = 0.0

    q2_by_category: defaultdict[str, dict[str, float]] = defaultdict(lambda: {"Deal Count": 0.0, "ARR (€ converted)": 0.0, "ACV (€ converted)": 0.0})
    promise_baseline: defaultdict[str, dict[str, float]] = defaultdict(lambda: {"Count": 0.0, "ARR (€ converted)": 0.0})
    renewal_risk: defaultdict[str, dict[str, float]] = defaultdict(lambda: {"Deal Count": 0.0, "ACV (€ converted)": 0.0})
    movement_summary: defaultdict[tuple[str, str], dict[str, float]] = defaultdict(lambda: {"count": 0.0, "arr": 0.0})
    dq_totals: defaultdict[str, float] = defaultdict(float)
    dq_by_rep: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))

    for snapshot in snapshots:
        scorecard = ((snapshot.get("scorecard") or {}).get("sections") or {})
        pipeline = ((scorecard.get("pipeline-health") or {}).get("metrics") or {})
        process = ((scorecard.get("process-compliance") or {}).get("metrics") or {})
        risk = ((scorecard.get("risk") or {}).get("metrics") or {})

        all_open_arr += as_number(pipeline.get("Pipeline ARR — All Open (any close date)"))
        fy26_arr += as_number(pipeline.get("Pipeline ARR — FY26 Close Dates Only (excl. Omitted)"))
        q2_arr += as_number(pipeline.get("Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)"))
        deal_count += as_number(pipeline.get("Deal Count"))
        weighted_arr += as_number(pipeline.get("Weighted Pipeline (probability-adj)"))
        new_pipeline_arr += as_number(pipeline.get("New Pipeline This Quarter (excl. Omitted)"))
        if process.get("Approval Rate (stage 3+)") not in (None, "", "—", "-"):
            approval_rate_values.append(as_number(process.get("Approval Rate (stage 3+)")))
        missing_approval_metric_count += as_number(process.get("Missing Approval (Land, stage 3+)"))

        stale_count += as_number(risk.get("Stale 30d+ (count)"))
        stale_arr += as_number(risk.get("Stale 30d+ (ARR)"))
        pushed5_count += as_number(risk.get("Pushed 5+ (count)"))
        pushed5_arr += as_number(risk.get("Pushed 5+ (ARR)"))
        aging365_count += as_number(risk.get("Aging 365+ (count)"))
        aging365_arr += as_number(risk.get("Aging 365+ (ARR)"))

        pipeline_records.extend(((snapshot.get("pipeline_detail") or {}).get("records") or []))
        pipeline_detail_rows.extend(((snapshot.get("pipeline_detail") or {}).get("top_opportunities") or []))
        q2_active_opportunity_rows.extend(director_q2_active_opportunities(snapshot, snapshot_date))

        q2_outlook = snapshot.get("q2_outlook") or {}
        for row in q2_outlook.get("breakdown") or []:
            category = as_text(row.get("Forecast Category")) or "Unknown"
            q2_by_category[category]["Deal Count"] += as_number(row.get("Deal Count"))
            q2_by_category[category]["ARR (€ converted)"] += as_number(row.get("ARR (€ converted)"))
            q2_by_category[category]["ACV (€ converted)"] += as_number(row.get("ACV (€ converted)"))
        commit_rows.extend(q2_outlook.get("commit_deals") or [])
        best_case_rows.extend(q2_outlook.get("best_case_deals") or [])

        commercial = snapshot.get("commercial_approval") or {}
        for row in commercial.get("summary") or []:
            category = as_text(row.get("Category"))
            if category == "Approved":
                approved_count += as_number(row.get("Deal Count"))
                approved_arr += as_number(row.get("ARR (€ converted)"))
            elif category == "Pending / Missing Approval":
                pending_count += as_number(row.get("Deal Count"))
                pending_arr += as_number(row.get("ARR (€ converted)"))
            elif category == "No Approval Needed":
                no_approval_count += as_number(row.get("Deal Count"))
        missing_candidates.extend(commercial.get("missing_candidates") or [])
        approved_ytd.extend(commercial.get("approved_ytd") or [])

        renewals = snapshot.get("renewals") or {}
        rows = renewals.get("open_renewals") or []
        open_renewals.extend(rows)
        summary_metrics = renewals.get("summary_metrics") or {}
        if summary_metrics:
            renewal_deal_count += as_number(summary_metrics.get("open_deal_count"))
            renewal_acv += as_number(summary_metrics.get("open_acv"))
            q2_renewal_deal_count += as_number(summary_metrics.get("q2_open_deal_count"))
            q2_renewal_acv += as_number(summary_metrics.get("q2_open_acv"))
        else:
            renewal_deal_count += len(rows)
            renewal_acv += sum(as_number(row.get("Renewal ACV (€ converted)")) for row in rows)
            q2_rows = director_q2_renewals(snapshot, snapshot_date)
            q2_renewal_deal_count += len(q2_rows)
            q2_renewal_acv += sum(as_number(row.get("Renewal ACV (€ converted)")) for row in q2_rows)
        q2_open_renewals.extend(director_q2_renewals(snapshot, snapshot_date))
        for row in renewals.get("risk_levels") or []:
            level = as_text(row.get("Risk Level")) or "Unknown"
            renewal_risk[level]["Deal Count"] += as_number(row.get("Deal Count"))
            renewal_risk[level]["ACV (€ converted)"] += as_number(row.get("ACV (€ converted)"))

        q1 = snapshot.get("q1_review") or {}
        actuals = q1.get("actuals") or {}
        q1_won_count += as_number(actuals.get("won_count"))
        q1_won_arr += as_number(actuals.get("won_arr"))
        q1_lost_count += as_number(actuals.get("lost_count"))
        q1_lost_arr += as_number(actuals.get("lost_arr"))
        q1_slipped_count += as_number(actuals.get("slipped_count"))
        q1_slipped_arr += as_number(actuals.get("slipped_arr"))
        q1_pushed_deals.extend(q1.get("pushed_deals") or [])
        forecast_movements.extend(q1.get("forecast_movements") or [])
        for row in q1.get("promise_baseline") or []:
            category = as_text(row.get("Category")) or "Unknown"
            promise_baseline[category]["Count"] += as_number(row.get("Count"))
            promise_baseline[category]["ARR (€ converted)"] += as_number(row.get("ARR (€ converted)"))
        for row in q1.get("forecast_movement_summary") or []:
            key = (as_text(row.get("from")), as_text(row.get("to")))
            movement_summary[key]["count"] += as_number(row.get("count"))
            movement_summary[key]["arr"] += as_number(row.get("arr"))

        won_lost = snapshot.get("won_lost") or {}
        won_rows.extend(won_lost.get("won") or [])
        lost_rows.extend(won_lost.get("lost") or [])

        data_quality = snapshot.get("data_quality") or {}
        total = data_quality.get("total") or {}
        for key, value in total.items():
            if key != "Rep":
                dq_totals[key] += as_number(value)
        for row in data_quality.get("records") or []:
            rep = as_text(row.get("Rep")) or "Unknown"
            for key, value in row.items():
                if key != "Rep":
                    dq_by_rep[rep][key] += as_number(value)
            data_quality_records.append(row)

    component_books = []
    for component_snapshot, source_path in zip(snapshots, source_paths, strict=False):
        component_scorecard = ((component_snapshot.get("scorecard") or {}).get("sections") or {})
        component_pipeline = ((component_scorecard.get("pipeline-health") or {}).get("metrics") or {})
        component_process = ((component_scorecard.get("process-compliance") or {}).get("metrics") or {})
        component_renewals_root = component_snapshot.get("renewals") or {}
        component_renewals = component_renewals_root.get("summary_metrics") or {}
        component_renewal_open_acv = as_number(component_renewals.get("open_acv"))
        if not component_renewal_open_acv:
            component_renewal_open_acv = sum(
                as_number(row.get("Renewal ACV (€ converted)"))
                for row in (component_renewals_root.get("open_renewals") or [])
            )
        component_books.append(
            {
                "director_name": component_snapshot["director_name"],
                "territory": component_snapshot["territory"],
                "snapshot_path": source_path,
                "all_open_arr": as_number(component_pipeline.get("Pipeline ARR — All Open (any close date)")),
                "fy26_arr": as_number(component_pipeline.get("Pipeline ARR — FY26 Close Dates Only (excl. Omitted)")),
                "q2_arr": as_number(component_pipeline.get("Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)")),
                "deal_count": int(round(as_number(component_pipeline.get("Deal Count")))),
                "approval_rate": as_text(component_process.get("Approval Rate (stage 3+)")),
                "renewal_open_acv": component_renewal_open_acv,
            }
        )
    q2_breakdown = []
    for category, values in q2_by_category.items():
        q2_breakdown.append(
            {
                "Forecast Category": category,
                "Deal Count": int(round(values["Deal Count"])),
                "ARR (€ converted)": round(values["ARR (€ converted)"], 2),
                "ACV (€ converted)": round(values["ACV (€ converted)"], 2),
            }
        )
    q2_breakdown.sort(key=lambda row: row["ARR (€ converted)"], reverse=True)

    renewal_risk_levels = []
    for level, values in renewal_risk.items():
        renewal_risk_levels.append(
            {
                "Risk Level": level,
                "Deal Count": int(round(values["Deal Count"])),
                "ACV (€ converted)": round(values["ACV (€ converted)"], 2),
            }
        )
    renewal_risk_levels.sort(key=lambda row: row["ACV (€ converted)"], reverse=True)

    promise_rows = []
    for category, values in promise_baseline.items():
        promise_rows.append(
            {
                "Category": category,
                "Count": int(round(values["Count"])),
                "ARR (€ converted)": round(values["ARR (€ converted)"], 2),
            }
        )
    promise_rows.sort(key=lambda row: row["ARR (€ converted)"], reverse=True)

    movement_rows = []
    for (old, new), values in movement_summary.items():
        movement_rows.append(
            {
                "from": old,
                "to": new,
                "count": int(round(values["count"])),
                "arr": round(values["arr"], 2),
            }
        )
    movement_rows.sort(key=lambda row: (-row["count"], -row["arr"]))

    dq_total_row = {"Rep": "TOTAL"}
    for key, value in dq_totals.items():
        dq_total_row[key] = int(round(value))
    dq_top_issues = []
    for rep, values in dq_by_rep.items():
        row = {"Rep": rep}
        for key, value in values.items():
            row[key] = int(round(value))
        dq_top_issues.append(row)
    dq_top_issues.sort(key=lambda row: -as_number(row.get("Total Issues")))

    approval_rate = sum(approval_rate_values) / len(approval_rate_values) if approval_rate_values else 0.0

    snapshot = {
        "region_name": region_name,
        "snapshot_date": snapshot_date,
        "rollup_model": "director-book rollup aligned to forecast region hierarchy",
        "component_books": component_books,
        "source_snapshot_paths": source_paths,
        "scorecard": {
            "sections": {
                "pipeline-health": {
                    "title": "Pipeline Health",
                    "metrics": {
                        "Pipeline ARR — All Open (any close date)": compact_eur(all_open_arr),
                        "Pipeline ARR — FY26 Close Dates Only (excl. Omitted)": compact_eur(fy26_arr),
                        "Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)": compact_eur(q2_arr),
                        "Deal Count": int(round(deal_count)),
                        "Avg Deal Size": compact_eur(all_open_arr / deal_count if deal_count else 0.0),
                        "Weighted Pipeline (probability-adj)": compact_eur(weighted_arr),
                        "New Pipeline This Quarter (excl. Omitted)": compact_eur(new_pipeline_arr),
                    },
                },
                "process-compliance": {
                    "title": "Process Compliance",
                    "metrics": {
                        "Approval Rate (stage 3+)": compact_pct(approval_rate),
                        "Missing Approval (Land, stage 3+)": int(round(missing_approval_metric_count)),
                    },
                },
                "risk": {
                    "title": "Risk",
                    "metrics": {
                        "Stale 30d+ (count)": int(round(stale_count)),
                        "Stale 30d+ (ARR)": compact_eur(stale_arr),
                        "Pushed 5+ (count)": int(round(pushed5_count)),
                        "Pushed 5+ (ARR)": compact_eur(pushed5_arr),
                        "Aging 365+ (count)": int(round(aging365_count)),
                        "Aging 365+ (ARR)": compact_eur(aging365_arr),
                    },
                },
            }
        },
        "pipeline_detail": {
            "records": pipeline_records,
            "top_opportunities": top_rows(pipeline_detail_rows, key="ARR (€ converted)", limit=15),
            "q2_active_opportunities": top_rows(q2_active_opportunity_rows, key="ARR (€ converted)", limit=15),
        },
        "q2_outlook": {
            "by_category": {
                row["Forecast Category"]: row for row in q2_breakdown
            },
            "breakdown": q2_breakdown,
            "commit_deals": top_rows(commit_rows, key="ARR (€ converted)", limit=15),
            "best_case_deals": top_rows(best_case_rows, key="ARR (€ converted)", limit=15),
            "coverage": {
                "Active Pipeline ARR (Q2 close, excl. Omitted)": round(
                    sum(
                        row["ARR (€ converted)"]
                        for row in q2_breakdown
                        if row["Forecast Category"] != "Omitted"
                    ),
                    2,
                ),
                "Commit ARR (Q2 close)": round(q2_by_category["Commit"]["ARR (€ converted)"], 2),
                "Best Case ARR (Q2 close)": round(q2_by_category["Best Case"]["ARR (€ converted)"], 2),
                "Pipeline ARR (Q2 close)": round(q2_by_category["Pipeline"]["ARR (€ converted)"], 2),
                "Omitted ARR (Q2 close)": round(q2_by_category["Omitted"]["ARR (€ converted)"], 2),
            },
        },
        "commercial_approval": {
            "approval_rate_method": "Simple average of component-book approval-rate metrics from validated director scorecards.",
            "summary": [
                {"Category": "Approved", "Deal Count": int(round(approved_count)), "ARR (€ converted)": round(approved_arr, 2)},
                {"Category": "Pending / Missing Approval", "Deal Count": int(round(pending_count)), "ARR (€ converted)": round(pending_arr, 2)},
                {"Category": "No Approval Needed", "Deal Count": int(round(no_approval_count)), "ARR (€ converted)": 0.0},
            ],
            "missing_candidates": top_rows(missing_candidates, key="ARR (€ converted)", limit=20),
            "approved_ytd": top_rows(approved_ytd, key="ARR (€ converted)", limit=20),
        },
        "renewals": {
            "open_renewals": top_rows(open_renewals, key="Renewal ACV (€ converted)", limit=20),
            "q2_open_renewals": top_rows(q2_open_renewals, key="Renewal ACV (€ converted)", limit=15),
            "risk_levels": renewal_risk_levels,
            "summary_metrics": {
                "open_deal_count": int(round(renewal_deal_count)),
                "open_acv": round(renewal_acv, 2),
                "q2_open_deal_count": int(round(q2_renewal_deal_count)),
                "q2_open_acv": round(q2_renewal_acv, 2),
            },
        },
        "q1_review": {
            "actuals": {
                "won_count": int(round(q1_won_count)),
                "won_arr": round(q1_won_arr, 2),
                "lost_count": int(round(q1_lost_count)),
                "lost_arr": round(q1_lost_arr, 2),
                "slipped_count": int(round(q1_slipped_count)),
                "slipped_arr": round(q1_slipped_arr, 2),
            },
            "promise_baseline": promise_rows,
            "pushed_deals": top_rows(q1_pushed_deals, key="ARR (€ converted)", limit=20),
            "forecast_movements": top_rows(forecast_movements, key="ARR (€ converted)", limit=25),
            "forecast_movement_summary": movement_rows,
            "scope_warning": "Regional Q1 promise baselines are aggregated from director-scoped pipeline-inspection populations and must be qualified if used as commitments.",
        },
        "data_quality": {
            "total": dq_total_row,
            "top_issues": dq_top_issues[:15],
        },
        "won_lost": {
            "won": top_rows(won_rows, key="ARR (€ converted)", limit=20),
            "lost": top_rows(lost_rows, key="ARR (€ converted)", limit=20),
        },
    }
    snapshot["quarterly_pipeline_display"] = quarterly_pipeline_display_from_snapshot(
        snapshot
    )
    if region_name == "EMEA":
        snapshot["forecast_hierarchy_note"] = "Middle East & Africa is included under EMEA in the forecast hierarchy."
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region-name", choices=("APAC", "EMEA", "North America"), required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--director-snapshot-root", type=Path, default=DEFAULT_DIRECTOR_SNAPSHOT_ROOT)
    parser.add_argument("--output-path", type=Path, required=True)
    args = parser.parse_args()

    snapshot = build_region_snapshot(
        region_name=args.region_name,
        snapshot_date=args.snapshot_date,
        director_snapshot_root=args.director_snapshot_root,
    )
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps({"snapshot_path": str(args.output_path), "component_books": snapshot["component_books"]}, indent=2))


if __name__ == "__main__":
    main()
