#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = ROOT / "output" / "historical_trending_extract"
OUTPUT_ROOT = ROOT / "output" / "historical_trending_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _audit_path(run_date: str) -> Path:
    return AUDIT_ROOT / str(run_date)[:10] / "historical_trending_extract_audit.json"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _available_audit_dates() -> list[str]:
    dates = []
    for path in sorted(AUDIT_ROOT.glob("*/historical_trending_extract_audit.json")):
        dates.append(path.parent.name)
    return dates


def _resolve_baseline_date(
    current_date: str, baseline_date: str | None = None
) -> str | None:
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
        slug = str(item.get("slug") or "").strip()
        if slug:
            index[slug] = item
    return index


def _sheet_index(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in items:
        name = str(item.get("sheet_name") or "").strip()
        if name:
            index[name] = item
    return index


def _failure_index(
    items: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    index: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in items:
        key = (
            str(item.get("slug") or "").strip(),
            str(item.get("sheet_name") or "").strip(),
            str(item.get("report_id") or "").strip(),
            ",".join(sorted(str(issue) for issue in (item.get("issues") or []))),
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


def _diff_sheet_items(
    baseline_items: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_index = _sheet_index(baseline_items)
    current_index = _sheet_index(current_items)
    changes: list[dict[str, Any]] = []
    for sheet_name in sorted(set(baseline_index) | set(current_index)):
        before_item = baseline_index.get(sheet_name)
        after_item = current_index.get(sheet_name)
        if before_item is None:
            changes.append({"change": "added", "sheet_name": sheet_name, "after": after_item})
            continue
        if after_item is None:
            changes.append({"change": "removed", "sheet_name": sheet_name, "before": before_item})
            continue
        field_changes = _diff_scalar_map(
            {
                "report_id": before_item.get("report_id"),
                "row_count": before_item.get("row_count"),
                "latest_snapshot_date": before_item.get("latest_snapshot_date"),
                "snapshot_dates": list(before_item.get("snapshot_dates") or []),
            },
            {
                "report_id": after_item.get("report_id"),
                "row_count": after_item.get("row_count"),
                "latest_snapshot_date": after_item.get("latest_snapshot_date"),
                "snapshot_dates": list(after_item.get("snapshot_dates") or []),
            },
        )
        if field_changes:
            changes.append(
                {
                    "change": "modified",
                    "sheet_name": sheet_name,
                    "changes": field_changes,
                }
            )
    return changes


def _diff_processed_items(
    baseline_items: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_index = _processed_index(baseline_items)
    current_index = _processed_index(current_items)
    changes: list[dict[str, Any]] = []
    for slug in sorted(set(baseline_index) | set(current_index)):
        before_item = baseline_index.get(slug)
        after_item = current_index.get(slug)
        if before_item is None:
            changes.append({"change": "added", "slug": slug, "after": after_item})
            continue
        if after_item is None:
            changes.append({"change": "removed", "slug": slug, "before": before_item})
            continue
        field_changes: dict[str, Any] = {}
        metadata_changes = _diff_scalar_map(
            {"workbook_path": before_item.get("workbook_path")},
            {"workbook_path": after_item.get("workbook_path")},
        )
        if metadata_changes:
            field_changes["metadata"] = metadata_changes
        sheet_changes = _diff_sheet_items(
            list(before_item.get("sheets") or []),
            list(after_item.get("sheets") or []),
        )
        if sheet_changes:
            field_changes["sheet_changes"] = sheet_changes
        if field_changes:
            changes.append(
                {
                    "change": "modified",
                    "slug": slug,
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
    return {
        "status": "ok",
        "baseline_run_date": str(baseline_payload.get("run_date") or ""),
        "current_run_date": str(current_payload.get("run_date") or ""),
        "historical_trending": {
            "status_before": baseline_payload.get("status"),
            "status_after": current_payload.get("status"),
            "scope_before": baseline_payload.get("scope"),
            "scope_after": current_payload.get("scope"),
            "retrospective_quarter_before": baseline_payload.get(
                "retrospective_quarter_title"
            ),
            "retrospective_quarter_after": current_payload.get(
                "retrospective_quarter_title"
            ),
            "current_quarter_before": baseline_payload.get("current_quarter_title"),
            "current_quarter_after": current_payload.get("current_quarter_title"),
            "processed_count_before": len(baseline_processed),
            "processed_count_after": len(current_processed),
            "failure_count_before": len(list(baseline_payload.get("failures") or [])),
            "failure_count_after": len(list(current_payload.get("failures") or [])),
            "processed_changes": _diff_processed_items(
                baseline_processed,
                current_processed,
            ),
            "failure_changes": _diff_failures(
                list(baseline_payload.get("failures") or []),
                list(current_payload.get("failures") or []),
            ),
        },
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Historical Trending Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
            "",
            "- No earlier historical-trending audit was available to diff against.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    history = payload["historical_trending"]
    processed_changes = list(history.get("processed_changes") or [])
    added = sum(1 for item in processed_changes if item.get("change") == "added")
    removed = sum(1 for item in processed_changes if item.get("change") == "removed")
    modified = sum(1 for item in processed_changes if item.get("change") == "modified")
    lines = [
        f"# Historical Trending Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Status: `{history['status_before']}` -> `{history['status_after']}`",
        f"- Scope: `{history['scope_before']}` -> `{history['scope_after']}`",
        f"- Quarter family: `{history['retrospective_quarter_before']} / {history['current_quarter_before']}` -> `{history['retrospective_quarter_after']} / {history['current_quarter_after']}`",
        f"- Processed workbooks: `{history['processed_count_before']}` -> `{history['processed_count_after']}`",
        f"- Failures: `{history['failure_count_before']}` -> `{history['failure_count_after']}`",
        "",
        "## Processed Changes",
        "",
        f"- Added workbooks: `{added}`",
        f"- Removed workbooks: `{removed}`",
        f"- Modified workbooks: `{modified}`",
        "",
        "## Failure Changes",
        "",
        f"- New failures: `{len(history['failure_changes']['added'])}`",
        f"- Resolved failures: `{len(history['failure_changes']['resolved'])}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current historical-trending audit date, YYYY-MM-DD. Defaults to today.",
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
        (output_dir / "historical_trending_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print("Historical trending snapshot diff: skipped (no baseline found)")
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
    (output_dir / "historical_trending_snapshot_diff.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)

    print("Historical trending snapshot diff: ok")
    print(f"Baseline: {baseline_date}")
    print(f"Output: {_display_path(output_dir)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
