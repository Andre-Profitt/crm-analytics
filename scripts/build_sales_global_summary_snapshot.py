#!/usr/bin/env python3
"""Build a deterministic global summary snapshot from validated regional snapshots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from build_sales_region_snapshot import build_region_snapshot
except ModuleNotFoundError:  # pragma: no cover
    from scripts.build_sales_region_snapshot import build_region_snapshot


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGION_SNAPSHOT_ROOT = REPO_ROOT / "output" / "sales_region_snapshots"
DEFAULT_DIRECTOR_SNAPSHOT_ROOT = REPO_ROOT / "output" / "director_workbook_snapshots"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "sales_global_summary_snapshots"
GLOBAL_REGIONS = ["APAC", "EMEA", "North America"]


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


def region_slug(region_name: str) -> str:
    return region_name.lower().replace(" ", "-")


def top_rows(rows: list[dict[str, Any]], *, key: str, limit: int) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: as_number(row.get(key)), reverse=True)[:limit]


def region_display_quarter(snapshot: dict[str, Any]) -> dict[str, Any]:
    display = ((snapshot.get("quarterly_pipeline_display") or {}).get("display_quarter") or {})
    if display:
        return display
    pipeline = (((snapshot.get("scorecard") or {}).get("sections") or {}).get("pipeline-health") or {}).get("metrics") or {}
    q2 = (snapshot.get("q2_outlook") or {}).get("by_category") or {}
    snapshot_date = as_text(snapshot.get("snapshot_date"))[:4]
    title = f"Q2 {snapshot_date}" if snapshot_date else "Q2"
    active_arr = as_number(
        pipeline.get("Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)")
    ) or sum(
        as_number((row or {}).get("ARR (€ converted)"))
        for category, row in q2.items()
        if as_text(category).lower() != "omitted"
    )
    return {
        "label": "Q2",
        "title": title,
        "by_category": q2,
        "active_arr": active_arr,
        "reason": "current_quarter",
        "footnote": "",
    }


def load_or_build_region_snapshot(
    *,
    region_name: str,
    snapshot_date: str,
    region_snapshot_root: Path,
    director_snapshot_root: Path,
) -> tuple[dict[str, Any], Path]:
    path = region_snapshot_root / snapshot_date / f"{region_slug(region_name)}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8")), path
    snapshot = build_region_snapshot(
        region_name=region_name,
        snapshot_date=snapshot_date,
        director_snapshot_root=director_snapshot_root,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=True), encoding="utf-8")
    return snapshot, path


def derive_region_top_risk(snapshot: dict[str, Any]) -> str:
    scorecard = (snapshot.get("scorecard") or {}).get("sections") or {}
    risk = (scorecard.get("risk") or {}).get("metrics") or {}
    process = (scorecard.get("process-compliance") or {}).get("metrics") or {}
    return (
        f"{as_text(snapshot.get('region_name'))}: {as_text(risk.get('Stale 30d+ (ARR)')) or '€0'} stale ARR, "
        f"{as_text(risk.get('Aging 365+ (ARR)')) or '€0'} aging ARR, and "
        f"{int(round(as_number(process.get('Missing Approval (Land, stage 3+)'))))} missing-approval candidates."
    )


def derive_region_top_action(snapshot: dict[str, Any]) -> str:
    missing_candidates = (snapshot.get("commercial_approval") or {}).get("missing_candidates") or []
    if missing_candidates:
        top = missing_candidates[0]
        return (
            f"Escalate {as_text(top.get('Opportunity'))} in {as_text(snapshot.get('region_name'))} at "
            f"{compact_eur(as_number(top.get('ARR (€ converted)')))} ARR."
        )
    renewals = (snapshot.get("renewals") or {}).get("q2_open_renewals") or []
    if renewals:
        top = top_rows(renewals, key="Renewal ACV (€ converted)", limit=1)[0]
        return (
            f"Prioritize {as_text(top.get('Opportunity'))} in {as_text(snapshot.get('region_name'))} at "
            f"{compact_eur(as_number(top.get('Renewal ACV (€ converted)')))} ACV."
        )
    return f"No single escalation dominates in {as_text(snapshot.get('region_name'))}."


def build_global_summary_snapshot(
    *,
    snapshot_date: str,
    region_snapshot_root: Path = DEFAULT_REGION_SNAPSHOT_ROOT,
    director_snapshot_root: Path = DEFAULT_DIRECTOR_SNAPSHOT_ROOT,
) -> dict[str, Any]:
    regions: list[dict[str, Any]] = []
    source_paths: list[str] = []
    approved_2026_by_region: list[dict[str, Any]] = []
    missing_approval_by_region: list[dict[str, Any]] = []
    largest_global_missing_candidates: list[dict[str, Any]] = []
    global_q2_pipeline = 0.0
    global_q2_renewal_acv = 0.0
    global_missing_approval_count = 0
    region_rollup_notes: list[str] = []

    for region_name in GLOBAL_REGIONS:
        snapshot, path = load_or_build_region_snapshot(
            region_name=region_name,
            snapshot_date=snapshot_date,
            region_snapshot_root=region_snapshot_root,
            director_snapshot_root=director_snapshot_root,
        )
        source_paths.append(str(path))
        pipeline = (((snapshot.get("scorecard") or {}).get("sections") or {}).get("pipeline-health") or {}).get("metrics") or {}
        process = (((snapshot.get("scorecard") or {}).get("sections") or {}).get("process-compliance") or {}).get("metrics") or {}
        q2 = (snapshot.get("q2_outlook") or {}).get("by_category") or {}
        renewals = snapshot.get("renewals") or {}
        summary_metrics = renewals.get("summary_metrics") or {}
        commercial = snapshot.get("commercial_approval") or {}
        display_quarter = region_display_quarter(snapshot)
        display_q = display_quarter.get("by_category") or {}
        approved_rows = commercial.get("approved_ytd") or []
        missing_rows = commercial.get("missing_candidates") or []
        approved_arr = sum(as_number(row.get("ARR (€ converted)")) for row in approved_rows)
        missing_arr = sum(as_number(row.get("ARR (€ converted)")) for row in missing_rows)
        global_q2_pipeline += as_number(pipeline.get("Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)"))
        global_q2_renewal_acv += as_number(summary_metrics.get("q2_open_acv"))
        global_missing_approval_count += len(missing_rows)
        if snapshot.get("forecast_hierarchy_note"):
            region_rollup_notes.append(as_text(snapshot.get("forecast_hierarchy_note")))

        regions.append(
            {
                "region_name": region_name,
                "snapshot_path": str(path),
                "headline_pipeline_arr_q2": compact_eur(as_number(display_quarter.get("active_arr"))),
                "q2_commit_arr": compact_eur(as_number((display_q.get("Commit") or {}).get("ARR (€ converted)"))),
                "q2_best_case_arr": compact_eur(as_number((display_q.get("Best Case") or {}).get("ARR (€ converted)"))),
                "q2_omitted_arr": compact_eur(as_number((display_q.get("Omitted") or {}).get("ARR (€ converted)"))),
                "quarterly_pipeline_label": as_text(display_quarter.get("label")) or "Q2",
                "quarterly_pipeline_title": as_text(display_quarter.get("title")) or "Q2",
                "quarterly_pipeline_display_reason": as_text(display_quarter.get("reason")) or "current_quarter",
                "quarterly_pipeline_footnote": as_text(display_quarter.get("footnote")),
                "approval_rate_stage3_plus": as_text(process.get("Approval Rate (stage 3+)")) or "—",
                "renewal_open_acv": compact_eur(as_number(summary_metrics.get("open_acv"))),
                "top_risk": derive_region_top_risk(snapshot),
                "top_action": derive_region_top_action(snapshot),
            }
        )
        approved_2026_by_region.append(
            {
                "region_name": region_name,
                "deal_count": len(approved_rows),
                "arr_eur": compact_eur(approved_arr),
            }
        )
        missing_approval_by_region.append(
            {
                "region_name": region_name,
                "candidate_count": len(missing_rows),
                "arr_eur": compact_eur(missing_arr),
            }
        )
        for row in missing_rows:
            enriched = dict(row)
            enriched["Region"] = region_name
            largest_global_missing_candidates.append(enriched)

    largest_global_missing_candidates = top_rows(
        largest_global_missing_candidates, key="ARR (€ converted)", limit=12
    )
    biggest_missing = largest_global_missing_candidates[0] if largest_global_missing_candidates else {}
    global_top_risk = (
        max(regions, key=lambda row: as_number(row.get("headline_pipeline_arr_q2")))["top_risk"]
        if regions
        else "No regional risk summary available."
    )
    global_top_action = (
        f"Global priority is {as_text(biggest_missing.get('Opportunity'))} in {as_text(biggest_missing.get('Region'))} "
        f"at {compact_eur(as_number(biggest_missing.get('ARR (€ converted)')))} ARR."
        if biggest_missing
        else "No global missing-approval escalation currently dominates."
    )

    return {
        "snapshot_date": snapshot_date,
        "source_region_snapshot_paths": source_paths,
        "regions": regions,
        "global_summary": {
            "global_pipeline_arr_q2": compact_eur(global_q2_pipeline),
            "global_renewal_acv_q2": compact_eur(global_q2_renewal_acv),
            "global_missing_approval_count": global_missing_approval_count,
            "global_top_risk": global_top_risk,
            "global_top_action": global_top_action,
        },
        "commercial_approval": {
            "approved_2026_by_region": approved_2026_by_region,
            "missing_approval_by_region": missing_approval_by_region,
            "largest_global_missing_candidates": [
                {
                    "region_name": as_text(row.get("Region")),
                    "opportunity": as_text(row.get("Opportunity")),
                    "owner": as_text(row.get("Owner")),
                    "stage": as_text(row.get("Stage") or row.get("Stage Name") or row.get("StageName")),
                    "arr_eur": compact_eur(as_number(row.get("ARR (€ converted)"))),
                }
                for row in largest_global_missing_candidates
            ],
        },
        "metric_definition_notes": [
            "Pipeline metrics are ARR in EUR converted.",
            "Renewal metrics are ACV in EUR converted.",
            "Regional slides are deterministic rollups from validated regional snapshots.",
        ],
        "region_rollup_notes": region_rollup_notes,
        "known_gaps": [
            "Global summary uses regional control metrics and does not replace the director operating deck.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--region-snapshot-root", type=Path, default=DEFAULT_REGION_SNAPSHOT_ROOT)
    parser.add_argument("--director-snapshot-root", type=Path, default=DEFAULT_DIRECTOR_SNAPSHOT_ROOT)
    parser.add_argument("--output-path", type=Path, required=True)
    args = parser.parse_args()

    snapshot = build_global_summary_snapshot(
        snapshot_date=args.snapshot_date,
        region_snapshot_root=args.region_snapshot_root,
        director_snapshot_root=args.director_snapshot_root,
    )
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps({"snapshot_path": str(args.output_path), "regions": [r["region_name"] for r in snapshot["regions"]]}, indent=2))


if __name__ == "__main__":
    main()
