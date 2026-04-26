#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = ROOT / "output" / "director_live_extract"
OUTPUT_ROOT = ROOT / "output" / "director_live_extract_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _audit_path(run_date: str) -> Path:
    return AUDIT_ROOT / str(run_date)[:10] / "director_live_extract_audit.json"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _available_audit_dates() -> list[str]:
    dates = []
    for path in sorted(AUDIT_ROOT.glob("*/director_live_extract_audit.json")):
        dates.append(path.parent.name)
    return dates


def _resolve_baseline_date(current_date: str, baseline_date: str | None = None) -> str | None:
    if baseline_date:
        return str(baseline_date)[:10]
    dates = [date for date in _available_audit_dates() if date != current_date]
    earlier = [date for date in dates if date < current_date]
    if earlier:
        return earlier[-1]
    return None


def _processed_index(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in items:
        territory = str(item.get("territory") or "").strip()
        if territory:
            index[territory] = item
    return index


def _failure_index(items: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    index: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in items:
        key = (
            str(item.get("territory") or "").strip(),
            str(item.get("error_type") or "").strip(),
            str(item.get("message") or "").strip(),
        )
        index[key] = item
    return index


def _diff_scalar_map(
    before_map: dict[str, Any],
    after_map: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    changes: dict[str, dict[str, Any]] = {}
    for key in sorted(set(before_map) | set(after_map)):
        before_value = before_map.get(key)
        after_value = after_map.get(key)
        if before_value == after_value:
            continue
        entry: dict[str, Any] = {
            "before": before_value,
            "after": after_value,
        }
        if isinstance(before_value, (int, float)) and isinstance(after_value, (int, float)):
            entry["delta"] = round(float(after_value) - float(before_value), 2)
        changes[key] = entry
    return changes


def _diff_processed_items(
    baseline_items: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_index = _processed_index(baseline_items)
    current_index = _processed_index(current_items)
    changes: list[dict[str, Any]] = []
    for territory in sorted(set(baseline_index) | set(current_index)):
        before_item = baseline_index.get(territory)
        after_item = current_index.get(territory)
        if before_item is None:
            changes.append(
                {
                    "change": "added",
                    "territory": territory,
                    "after": after_item,
                }
            )
            continue
        if after_item is None:
            changes.append(
                {
                    "change": "removed",
                    "territory": territory,
                    "before": before_item,
                }
            )
            continue

        field_changes: dict[str, Any] = {}
        scalar_changes = _diff_scalar_map(
            {
                "director": before_item.get("director"),
                "analysis_year": before_item.get("analysis_year"),
                "fy_label": before_item.get("fy_label"),
            },
            {
                "director": after_item.get("director"),
                "analysis_year": after_item.get("analysis_year"),
                "fy_label": after_item.get("fy_label"),
            },
        )
        if scalar_changes:
            field_changes["metadata"] = scalar_changes
        counts_changes = _diff_scalar_map(
            dict(before_item.get("counts") or {}),
            dict(after_item.get("counts") or {}),
        )
        if counts_changes:
            field_changes["counts"] = counts_changes
        arr_changes = _diff_scalar_map(
            dict(before_item.get("arr") or {}),
            dict(after_item.get("arr") or {}),
        )
        if arr_changes:
            field_changes["arr"] = arr_changes
        pi_source_changes = _diff_scalar_map(
            dict(before_item.get("pi_source") or {}),
            dict(after_item.get("pi_source") or {}),
        )
        if pi_source_changes:
            field_changes["pi_source"] = pi_source_changes
        forward_pi_changes = _diff_scalar_map(
            dict(before_item.get("forward_quarter_pi") or {}),
            dict(after_item.get("forward_quarter_pi") or {}),
        )
        if forward_pi_changes:
            field_changes["forward_quarter_pi"] = forward_pi_changes
        if field_changes:
            changes.append(
                {
                    "change": "modified",
                    "territory": territory,
                    "changes": field_changes,
                }
            )
    return changes


def _diff_failures(
    baseline_items: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    baseline_index = _failure_index(baseline_items)
    current_index = _failure_index(current_items)
    return {
        "added": [
            current_index[key]
            for key in sorted(set(current_index) - set(baseline_index))
        ],
        "resolved": [
            baseline_index[key]
            for key in sorted(set(baseline_index) - set(current_index))
        ],
    }


def build_snapshot_diff(
    baseline_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> dict[str, Any]:
    baseline_processed = list(baseline_payload.get("processed") or [])
    current_processed = list(current_payload.get("processed") or [])
    territory_changes = _diff_processed_items(baseline_processed, current_processed)
    failure_changes = _diff_failures(
        list(baseline_payload.get("failures") or []),
        list(current_payload.get("failures") or []),
    )
    return {
        "status": "ok",
        "baseline_run_date": str(baseline_payload.get("run_date") or ""),
        "current_run_date": str(current_payload.get("run_date") or ""),
        "extract": {
            "status_before": baseline_payload.get("status"),
            "status_after": current_payload.get("status"),
            "scope_before": baseline_payload.get("scope"),
            "scope_after": current_payload.get("scope"),
            "territories_requested_before": list(
                baseline_payload.get("territories_requested") or []
            ),
            "territories_requested_after": list(
                current_payload.get("territories_requested") or []
            ),
            "processed_count_before": len(baseline_processed),
            "processed_count_after": len(current_processed),
            "failure_count_before": len(list(baseline_payload.get("failures") or [])),
            "failure_count_after": len(list(current_payload.get("failures") or [])),
            "query_telemetry_totals": _diff_scalar_map(
                dict(baseline_payload.get("query_telemetry_totals") or {}),
                dict(current_payload.get("query_telemetry_totals") or {}),
            ),
            "territory_changes": territory_changes,
            "failure_changes": failure_changes,
        },
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Director Live Extract Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
            "",
            "- No earlier director-live extract audit was available to diff against.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    extract = payload["extract"]
    territory_changes = list(extract.get("territory_changes") or [])
    added = sum(1 for item in territory_changes if item.get("change") == "added")
    removed = sum(1 for item in territory_changes if item.get("change") == "removed")
    modified = sum(1 for item in territory_changes if item.get("change") == "modified")
    lines = [
        f"# Director Live Extract Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Extract status: `{extract['status_before']}` -> `{extract['status_after']}`",
        f"- Scope: `{extract['scope_before']}` -> `{extract['scope_after']}`",
        f"- Processed territories: `{extract['processed_count_before']}` -> `{extract['processed_count_after']}`",
        f"- Failures: `{extract['failure_count_before']}` -> `{extract['failure_count_after']}`",
        f"- Query telemetry delta: `{extract['query_telemetry_totals'] or 'none'}`",
        "",
        "## Territory Changes",
        "",
        f"- Added territories: `{added}`",
        f"- Removed territories: `{removed}`",
        f"- Modified territories: `{modified}`",
        "",
        "## Failure Changes",
        "",
        f"- New failures: `{len(extract['failure_changes']['added'])}`",
        f"- Resolved failures: `{len(extract['failure_changes']['resolved'])}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current director-live extract audit date, YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--baseline-date",
        default=None,
        help="Optional baseline audit date. Defaults to the latest prior audit on disk.",
    )
    args = parser.parse_args()

    current_date = str(args.current_date)[:10]
    current_path = _audit_path(current_date)
    if not current_path.exists():
        print(f"Current audit missing: {current_path}", file=sys.stderr)
        return 1

    output_dir = OUTPUT_ROOT / current_date
    output_dir.mkdir(parents=True, exist_ok=True)

    baseline_date = _resolve_baseline_date(current_date, args.baseline_date)
    if not baseline_date:
        payload = {
            "status": "skipped",
            "reason": "baseline_not_found",
            "current_run_date": current_date,
        }
        (output_dir / "director_live_extract_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print("Director live extract snapshot diff: skipped (no baseline found)")
        print(f"Output: {_display_path(output_dir)}")
        return 0

    baseline_path = _audit_path(baseline_date)
    if not baseline_path.exists():
        print(f"Baseline audit missing: {baseline_path}", file=sys.stderr)
        return 1

    payload = build_snapshot_diff(
        _load_json(baseline_path),
        _load_json(current_path),
    )
    (output_dir / "director_live_extract_snapshot_diff.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)

    print("Director live extract snapshot diff: ok")
    print(f"Baseline: {baseline_date}")
    print(f"Output: {_display_path(output_dir)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
