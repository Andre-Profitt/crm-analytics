#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = ROOT / "output" / "data_quality"
OUTPUT_ROOT = ROOT / "output" / "data_quality_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _audit_path(run_date: str) -> Path:
    return AUDIT_ROOT / str(run_date)[:10] / "flags.json"


def _available_audit_dates() -> list[str]:
    dates = []
    for path in sorted(AUDIT_ROOT.glob("*/flags.json")):
        dates.append(path.parent.name)
    return dates


def _resolve_baseline_date(
    current_date: str,
    baseline_date: str | None = None,
) -> str | None:
    if baseline_date:
        return str(baseline_date)[:10]
    dates = [date for date in _available_audit_dates() if date != current_date]
    earlier = [date for date in dates if date < current_date]
    if earlier:
        return earlier[-1]
    return None


def _result_index(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in items:
        key = str(item.get("key") or "").strip()
        if key:
            index[key] = item
    return index


def _severity_totals(items: list[dict[str, Any]]) -> dict[str, int]:
    totals = {"Critical": 0, "Important": 0, "Domain": 0}
    for item in items:
        severity = str(item.get("severity") or "")
        count = item.get("count")
        if severity in totals and isinstance(count, int):
            totals[severity] += count
    return totals


def _error_items(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    errors: dict[str, dict[str, Any]] = {}
    for item in items:
        key = str(item.get("key") or "").strip()
        error = item.get("error")
        if key and error:
            errors[key] = {
                "key": key,
                "label": item.get("label"),
                "severity": item.get("severity"),
                "error": error,
            }
    return errors


def _count_changes(items_before: list[dict[str, Any]], items_after: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    before_index = _result_index(items_before)
    after_index = _result_index(items_after)
    gap_changes: list[dict[str, Any]] = []
    baseline_changes: list[dict[str, Any]] = []
    for key in sorted(set(before_index) | set(after_index)):
        before_item = before_index.get(key, {})
        after_item = after_index.get(key, {})
        before_count = before_item.get("count")
        after_count = after_item.get("count")
        before_error = before_item.get("error")
        after_error = after_item.get("error")
        if (
            before_count == after_count
            and before_error == after_error
            and before_item.get("severity") == after_item.get("severity")
        ):
            continue
        entry: dict[str, Any] = {
            "key": key,
            "label": after_item.get("label") or before_item.get("label") or key,
            "severity": after_item.get("severity") or before_item.get("severity"),
            "before": before_count,
            "after": after_count,
            "before_error": before_error,
            "after_error": after_error,
        }
        if isinstance(before_count, int) and isinstance(after_count, int):
            delta = after_count - before_count
            entry["delta"] = delta
            if str(entry["severity"]) != "baseline":
                if delta > 0:
                    entry["direction"] = "worse"
                elif delta < 0:
                    entry["direction"] = "better"
                else:
                    entry["direction"] = "unchanged"
        target = (
            baseline_changes
            if str(entry["severity"]) == "baseline"
            else gap_changes
        )
        target.append(entry)
    gap_changes.sort(key=lambda item: abs(int(item.get("delta") or 0)), reverse=True)
    baseline_changes.sort(
        key=lambda item: abs(int(item.get("delta") or 0)),
        reverse=True,
    )
    return {
        "gap_changes": gap_changes,
        "baseline_changes": baseline_changes,
    }


def build_snapshot_diff(
    baseline_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> dict[str, Any]:
    baseline_items = list(baseline_payload.get("results") or [])
    current_items = list(current_payload.get("results") or [])
    count_changes = _count_changes(baseline_items, current_items)
    baseline_errors = _error_items(baseline_items)
    current_errors = _error_items(current_items)
    return {
        "status": "ok",
        "baseline_run_date": str(baseline_payload.get("run_date") or ""),
        "current_run_date": str(current_payload.get("run_date") or ""),
        "data_quality": {
            "check_count_before": len(baseline_items),
            "check_count_after": len(current_items),
            "error_count_before": len(baseline_errors),
            "error_count_after": len(current_errors),
            "severity_totals_before": _severity_totals(baseline_items),
            "severity_totals_after": _severity_totals(current_items),
            "gap_changes": count_changes["gap_changes"],
            "baseline_changes": count_changes["baseline_changes"],
            "new_errors": [
                current_errors[key]
                for key in sorted(set(current_errors) - set(baseline_errors))
            ],
            "resolved_errors": [
                baseline_errors[key]
                for key in sorted(set(baseline_errors) - set(current_errors))
            ],
        },
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Data Quality Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    data_quality = payload["data_quality"]
    severity_before = data_quality["severity_totals_before"]
    severity_after = data_quality["severity_totals_after"]
    worsened = sum(
        1
        for item in data_quality["gap_changes"]
        if item.get("direction") == "worse"
    )
    improved = sum(
        1
        for item in data_quality["gap_changes"]
        if item.get("direction") == "better"
    )
    lines = [
        f"# Data Quality Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        "- Status: `ok`",
        f"- Checks: `{data_quality['check_count_before']}` -> `{data_quality['check_count_after']}`",
        f"- Query errors: `{data_quality['error_count_before']}` -> `{data_quality['error_count_after']}`",
        f"- Critical backlog: `{severity_before['Critical']}` -> `{severity_after['Critical']}`",
        f"- Important backlog: `{severity_before['Important']}` -> `{severity_after['Important']}`",
        f"- Domain backlog: `{severity_before['Domain']}` -> `{severity_after['Domain']}`",
        "",
        "## Drift",
        "",
        f"- Gap checks changed: `{len(data_quality['gap_changes'])}`",
        f"- Baseline metrics changed: `{len(data_quality['baseline_changes'])}`",
        f"- Worsened checks: `{worsened}`",
        f"- Improved checks: `{improved}`",
        f"- New query errors: `{len(data_quality['new_errors'])}`",
        f"- Resolved query errors: `{len(data_quality['resolved_errors'])}`",
    ]
    top_changes = list(data_quality["gap_changes"][:5])
    if top_changes:
        lines.extend(["", "## Largest Gap Moves", ""])
        for item in top_changes:
            before = item.get("before")
            after = item.get("after")
            delta = item.get("delta")
            delta_label = f"{delta:+d}" if isinstance(delta, int) else "n/a"
            lines.append(
                f"- `{item['label']}`: `{before}` -> `{after}` ({delta_label})"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current data-quality audit date, YYYY-MM-DD. Defaults to today.",
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
    else:
        payload = build_snapshot_diff(
            _load_json(_audit_path(baseline_date)),
            _load_json(current_path),
        )
        payload["baseline_run_date"] = baseline_date
        payload["current_run_date"] = current_date

    payload_path = output_dir / "data_quality_snapshot_diff.json"
    summary_path = output_dir / "summary.md"
    payload_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _write_summary(summary_path, payload)
    if payload["status"] == "skipped":
        print("Data quality snapshot diff: skipped (no baseline found)")
    else:
        print("Data quality snapshot diff: ok")
    print(f"Output: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
