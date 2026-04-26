#!/usr/bin/env python3
"""Build a Gold analytics pack from a DirectorBundle JSON artifact."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.monthly_platform.intelligence import build_gold_analytics_pack  # noqa: E402
from scripts.monthly_platform.models import DirectorBundle  # noqa: E402

REGION_BY_TERRITORY = {
    "APAC": "APAC",
    "Central Europe": "EMEA",
    "Southern Europe": "EMEA",
    "UK & Ireland": "EMEA",
    "NL & Nordics": "EMEA",
    "Northern Europe": "EMEA",
    "Middle East & Africa": "EMEA",
    "Canada": "North America",
    "NA Asset Management": "North America",
    "Pension & Insurance": "North America",
}


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "unknown"


def default_output_dir(pack: dict[str, Any]) -> Path:
    return (
        ROOT
        / "output"
        / "director_gold_analytics"
        / pack["snapshot_date"]
        / slugify(pack["director"])
    )


def default_output_root() -> Path:
    return ROOT / "output" / "director_gold_analytics"


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def region_for_territory(territory: str) -> str:
    return REGION_BY_TERRITORY.get(territory, "Unmapped")


def entry_totals(entries: list[dict[str, Any]]) -> dict[str, int | float]:
    return {
        "open_deals": sum(entry["open_deals"] for entry in entries),
        "open_arr": round(sum(entry["open_arr"] for entry in entries), 2),
        "deal_risk_rows": sum(entry["deal_risk_rows"] for entry in entries),
        "close_date_event_count": sum(
            entry["close_date_event_count"] for entry in entries
        ),
    }


def regional_rollups(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_region: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        by_region.setdefault(region_for_territory(entry["territory"]), []).append(entry)

    rollups = []
    for region, region_entries in sorted(by_region.items()):
        rollups.append(
            {
                "region": region,
                "rollup_basis": "director_book_opportunity_rollup",
                "director_count": len(region_entries),
                "territories": sorted({entry["territory"] for entry in region_entries}),
                "totals": entry_totals(region_entries),
            }
        )
    return rollups


def markdown_summary(pack: dict[str, Any]) -> str:
    summary = pack["summary"]
    analytics = pack["analytics"]
    lines = [
        f"# Director Gold Analytics - {pack['director']}",
        "",
        f"- Snapshot date: `{pack['snapshot_date']}`",
        f"- Territory: `{pack['territory']}`",
        f"- Open deals: `{summary['open_deals']}`",
        f"- Open ARR: `{summary['open_arr']:,.0f}`",
        f"- Close-date events: `{summary['close_date_event_count']}`",
        f"- Deal risk rows: `{summary['deal_risk_rows']}`",
        f"- Stage 3+ zero-ARR count: `{summary['high_stage_zero_arr_count']}`",
        "",
        "## Deck-Ready Insights",
        "",
    ]
    for insight in pack["deck_ready_insights"]:
        lines.append(f"- **{insight['theme']}**: {insight['insight']}")

    lines.extend(["", "## Pipeline Quality By Quarter", ""])
    lines.append(
        "| Quarter | Deals | ARR | Weighted ARR | Active ex-Omitted | Omitted % | No-Touch Deals |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for row in analytics["pipeline_quality_by_quarter"]:
        lines.append(
            f"| {row['quarter']} | {row['deal_count']} | {row['arr_unweighted']:,.0f} | "
            f"{row['weighted_arr']:,.0f} | {row['active_arr_ex_omitted']:,.0f} | "
            f"{row['omitted_pct']}% | {row['no_touch_deal_count']} |"
        )

    lines.extend(["", "## Top Owner Portfolio Health", ""])
    lines.append(
        "| Owner | Deals | ARR | Weighted Coverage % | No-Touch ARR % | Risk Rows | Avg Risk |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for row in analytics["owner_portfolio_health"][:12]:
        lines.append(
            f"| {row['owner']} | {row['open_deals']} | {row['arr_unweighted']:,.0f} | "
            f"{row['weighted_coverage_pct']}% | {row['no_touch_arr_pct']}% | "
            f"{row['risk_rows']} | {row['avg_risk_score']} |"
        )

    lines.extend(["", "## Close Date Volatility", ""])
    vol = analytics["close_date_volatility"]
    lines.append(f"- Pushed out: `{vol['direction_counts'].get('pushed_out', 0)}`")
    lines.append(f"- Pulled in: `{vol['direction_counts'].get('pulled_in', 0)}`")
    lines.append(f"- Average net days: `{vol['avg_net_days']}`")
    lines.append(f"- Average gross days: `{vol['avg_gross_days']}`")
    lines.append("")
    lines.append("| Opportunity | Owner | Events | Net Days | Gross Days | Max ARR |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for row in vol["by_opportunity"][:12]:
        lines.append(
            f"| {row['opportunity']} | {row['owner']} | {row['event_count']} | "
            f"{row['net_days']} | {row['gross_days']} | {row['max_arr_unweighted']:,.0f} |"
        )

    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--bundle", type=Path)
    source.add_argument("--bundle-dir", type=Path)
    parser.add_argument("--output-dir", type=Path, help="Exact output dir for --bundle.")
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Root output dir for --bundle-dir. Defaults to output/director_gold_analytics.",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def discover_bundle_paths(bundle_dir: Path) -> list[Path]:
    paths = [
        path
        for path in sorted(bundle_dir.glob("*.json"))
        if path.name not in {"manifest.json", "director_bundle_manifest.json"}
        and not path.name.endswith("_manifest.json")
    ]
    if not paths:
        raise FileNotFoundError(f"No director bundle JSON files found in {bundle_dir}")
    return paths


def load_pack(bundle_path: Path) -> dict[str, Any]:
    bundle = DirectorBundle.from_json(bundle_path.read_text(encoding="utf-8"))
    return build_gold_analytics_pack(bundle)


def save_pack(
    pack: dict[str, Any],
    bundle_path: Path,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    target_dir = output_dir or default_output_dir(pack)
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / "gold_analytics.json"
    summary_path = target_dir / "summary.md"
    save_json(json_path, pack)
    summary_path.write_text(markdown_summary(pack), encoding="utf-8")
    return {
        "status": "ok",
        "director": pack["director"],
        "territory": pack["territory"],
        "snapshot_date": pack["snapshot_date"],
        "bundle_path": str(bundle_path),
        "json_path": str(json_path),
        "summary_path": str(summary_path),
        "open_deals": pack["summary"]["open_deals"],
        "open_arr": pack["summary"]["open_arr"],
        "deal_risk_rows": pack["summary"]["deal_risk_rows"],
        "close_date_event_count": pack["summary"]["close_date_event_count"],
    }


def build_one(bundle_path: Path, output_dir: Path | None = None) -> dict[str, Any]:
    return save_pack(load_pack(bundle_path), bundle_path, output_dir)


def write_batch_manifests(
    results: list[dict[str, Any]],
    bundle_dir: Path,
    output_root: Path,
) -> list[str]:
    manifest_paths: list[str] = []
    by_snapshot: dict[str, list[dict[str, Any]]] = {}
    for result in results:
        by_snapshot.setdefault(result["snapshot_date"], []).append(result)

    for snapshot_date, entries in sorted(by_snapshot.items()):
        manifest_path = output_root / snapshot_date / "manifest.json"
        manifest = {
            "artifact_type": "director_gold_analytics_manifest",
            "schema_version": "1",
            "snapshot_date": snapshot_date,
            "bundle_dir": str(bundle_dir),
            "generated_at": datetime.now(UTC).isoformat(),
            "director_count": len(entries),
            "territories": sorted({entry["territory"] for entry in entries}),
            "rollup_basis": "director_book_opportunity_rollup",
            "totals": entry_totals(entries),
            "regional_rollups": regional_rollups(entries),
            "directors": entries,
        }
        save_json(manifest_path, manifest)
        manifest_paths.append(str(manifest_path))
    return manifest_paths


def build_batch(bundle_dir: Path, output_root: Path) -> dict[str, Any]:
    results = []
    for bundle_path in discover_bundle_paths(bundle_dir):
        pack = load_pack(bundle_path)
        output_dir = output_root / pack["snapshot_date"] / slugify(pack["director"])
        results.append(save_pack(pack, bundle_path, output_dir))

    manifest_paths = write_batch_manifests(results, bundle_dir, output_root)
    return {
        "status": "ok",
        "bundle_dir": str(bundle_dir),
        "director_count": len(results),
        "manifest_paths": manifest_paths,
        "results": results,
    }


def main() -> int:
    args = parse_args()
    if args.bundle and args.output_root:
        raise SystemExit("--output-root is only valid with --bundle-dir")
    if args.bundle_dir and args.output_dir:
        raise SystemExit("--output-dir is only valid with --bundle")

    if args.bundle_dir:
        result = build_batch(args.bundle_dir, args.output_root or default_output_root())
        print(json.dumps(result, indent=2) if args.json else result)
        return 0

    result = build_one(args.bundle, args.output_dir)
    print(json.dumps(result, indent=2) if args.json else result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
