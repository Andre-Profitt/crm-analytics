#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TERRITORY_CONFIG_PATH = ROOT / "config" / "sd_monthly_territories.json"
SOURCE_CONTRACT_AUDIT_ROOT = ROOT / "output" / "source_contract_audit"
OUTPUT_ROOT = ROOT / "output" / "source_contract_registry_refresh"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _candidate_lane(payload: dict[str, Any]) -> dict[str, Any]:
    lane = payload.get("candidate_forward_quarter") or payload.get("candidate_q3") or {}
    return lane if isinstance(lane, dict) else {}


def _extract_discovered_sources(audit_payload: dict[str, Any]) -> dict[str, Any]:
    candidate = _candidate_lane(audit_payload)
    quarter_label = str(candidate.get("quarter_label") or "").strip()
    quarter_title = str(candidate.get("quarter_title") or "").strip()
    pi_sources: dict[str, dict[str, str]] = {}
    historical_sources: dict[str, str] = {}

    for item in candidate.get("pi_list_views") or []:
        territory = str(item.get("territory") or "").strip()
        list_view_id = str(item.get("list_view_id") or "").strip()
        if (
            territory
            and list_view_id
            and str(item.get("source_origin") or "").strip() == "discovered"
            and str(item.get("status") or "").strip() == "ok"
        ):
            pi_sources[territory] = {
                "list_view_id": list_view_id,
                "list_view_label": str(item.get("list_view_label") or "").strip(),
            }

    for item in candidate.get("historical_reports") or []:
        territory = str(item.get("director_slug") or "").strip()
        report_id = str(item.get("report_id") or "").strip()
        if (
            territory
            and report_id
            and str(item.get("source_origin") or "").strip() == "discovered"
            and str(item.get("status") or "").strip() == "ok"
        ):
            historical_sources[territory] = report_id

    return {
        "quarter_label": quarter_label,
        "quarter_title": quarter_title,
        "pi_list_views": pi_sources,
        "historical_reports": historical_sources,
    }


def build_refresh_plan(
    territory_config: dict[str, Any],
    audit_payload: dict[str, Any],
) -> dict[str, Any]:
    discovered = _extract_discovered_sources(audit_payload)
    quarter_label = str(discovered["quarter_label"])
    quarter_title = str(discovered["quarter_title"])
    territories = territory_config.get("territories") or {}
    updated_config = json.loads(json.dumps(territory_config))
    updated_territories = updated_config.get("territories") or {}
    changes: list[dict[str, Any]] = []

    for territory, source in discovered["pi_list_views"].items():
        territory_entry = updated_territories.get(territory) or {}
        registry = territory_entry.setdefault("forward_quarter_pi_list_views", {})
        existing = registry.get(quarter_label)
        proposed = {
            "list_view_id": str(source["list_view_id"]),
            "list_view_label": str(source.get("list_view_label") or "").strip(),
        }
        if not existing:
            registry[quarter_label] = proposed
            changes.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_pi_list_views",
                    "quarter_label": quarter_label,
                    "status": "promoted",
                    "proposed": proposed,
                }
            )
        elif existing == proposed:
            changes.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_pi_list_views",
                    "quarter_label": quarter_label,
                    "status": "already_current",
                    "existing": existing,
                }
            )
        else:
            changes.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_pi_list_views",
                    "quarter_label": quarter_label,
                    "status": "conflict_existing",
                    "existing": existing,
                    "proposed": proposed,
                }
            )

    for territory, report_id in discovered["historical_reports"].items():
        territory_entry = updated_territories.get(territory) or {}
        registry = territory_entry.setdefault(
            "forward_quarter_historical_trending_report_ids", {}
        )
        existing = str(registry.get(quarter_label) or "").strip()
        proposed = str(report_id).strip()
        if not existing:
            registry[quarter_label] = proposed
            changes.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_historical_trending_report_ids",
                    "quarter_label": quarter_label,
                    "status": "promoted",
                    "proposed": proposed,
                }
            )
        elif existing == proposed:
            changes.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_historical_trending_report_ids",
                    "quarter_label": quarter_label,
                    "status": "already_current",
                    "existing": existing,
                }
            )
        else:
            changes.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_historical_trending_report_ids",
                    "quarter_label": quarter_label,
                    "status": "conflict_existing",
                    "existing": existing,
                    "proposed": proposed,
                }
            )

    promoted = [item for item in changes if item["status"] == "promoted"]
    conflicts = [item for item in changes if item["status"] == "conflict_existing"]
    already_current = [item for item in changes if item["status"] == "already_current"]
    return {
        "run_date": str(audit_payload.get("run_date") or ""),
        "quarter_label": quarter_label,
        "quarter_title": quarter_title,
        "changes": changes,
        "promoted_count": len(promoted),
        "conflict_count": len(conflicts),
        "already_current_count": len(already_current),
        "updated_config": updated_config,
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        f"# Forward Quarter Registry Refresh — {payload['run_date']}",
        "",
        f"- Quarter: `{payload.get('quarter_title') or payload.get('quarter_label') or 'unknown'}`",
        f"- Promoted entries: `{payload['promoted_count']}`",
        f"- Already current: `{payload['already_current_count']}`",
        f"- Conflicts: `{payload['conflict_count']}`",
        "",
        "## Changes",
        "",
    ]
    changes = payload.get("changes") or []
    if not changes:
        lines.append("- none")
    else:
        for item in changes:
            lines.append(
                f"- `{item['territory']}` / `{item['source']}`: `{item['status']}`"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Source-contract audit date, YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply promoted entries into config/sd_monthly_territories.json.",
    )
    args = parser.parse_args()

    run_date = str(args.date)[:10]
    audit_path = SOURCE_CONTRACT_AUDIT_ROOT / run_date / "source_contract_audit.json"
    if not audit_path.exists():
        print(f"Source audit missing: {audit_path}", file=sys.stderr)
        return 1

    territory_config = _load_json(TERRITORY_CONFIG_PATH)
    audit_payload = _load_json(audit_path)
    payload = build_refresh_plan(territory_config, audit_payload)

    output_dir = OUTPUT_ROOT / run_date
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "registry_refresh.json").write_text(
        json.dumps({k: v for k, v in payload.items() if k != "updated_config"}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    (output_dir / "proposed_sd_monthly_territories.json").write_text(
        json.dumps(payload["updated_config"], indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)

    if args.apply and payload["promoted_count"] > 0:
        TERRITORY_CONFIG_PATH.write_text(
            json.dumps(payload["updated_config"], indent=2) + "\n",
            encoding="utf-8",
        )

    print("Forward quarter registry refresh: ok")
    print(f"Output: {_display_path(output_dir)}")
    if args.apply:
        print(f"Applied promotions: {payload['promoted_count']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
