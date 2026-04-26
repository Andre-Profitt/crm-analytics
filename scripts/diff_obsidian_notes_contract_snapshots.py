#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = ROOT / "output" / "obsidian_notes_contract"
OUTPUT_ROOT = ROOT / "output" / "obsidian_notes_contract_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _audit_path(run_date: str) -> Path:
    return AUDIT_ROOT / str(run_date)[:10] / "obsidian_notes_contract_audit.json"


def _available_audit_dates() -> list[str]:
    dates = []
    for path in sorted(AUDIT_ROOT.glob("*/obsidian_notes_contract_audit.json")):
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


def _validated_index(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in items:
        director = str(item.get("director") or "").strip()
        if director:
            index[director] = item
    return index


def _issue_index(items: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    index: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in items:
        key = (
            str(item.get("director") or "").strip(),
            str(item.get("issue") or "").strip(),
            str(item.get("message") or "").strip(),
        )
        index[key] = item
    return index


def _diff_validated_items(
    baseline_items: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_index = _validated_index(baseline_items)
    current_index = _validated_index(current_items)
    changes: list[dict[str, Any]] = []
    for director in sorted(set(baseline_index) | set(current_index)):
        before_item = baseline_index.get(director)
        after_item = current_index.get(director)
        if before_item is None:
            changes.append({"change": "added", "director": director, "after": after_item})
            continue
        if after_item is None:
            changes.append({"change": "removed", "director": director, "before": before_item})
            continue
        field_changes = {}
        for key in ["territory", "snapshot_history_present"]:
            before_value = before_item.get(key)
            after_value = after_item.get(key)
            if before_value != after_value:
                field_changes[key] = {"before": before_value, "after": after_value}
        if field_changes:
            changes.append(
                {
                    "change": "modified",
                    "director": director,
                    "changes": field_changes,
                }
            )
    return changes


def _diff_issues(
    baseline_items: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    baseline_index = _issue_index(baseline_items)
    current_index = _issue_index(current_items)
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
    baseline_validated = list(baseline_payload.get("validated") or [])
    current_validated = list(current_payload.get("validated") or [])
    baseline_failures = list(baseline_payload.get("failures") or [])
    current_failures = list(current_payload.get("failures") or [])
    baseline_warnings = list(baseline_payload.get("warnings") or [])
    current_warnings = list(current_payload.get("warnings") or [])
    return {
        "status": "ok",
        "baseline_run_date": str(baseline_payload.get("run_date") or ""),
        "current_run_date": str(current_payload.get("run_date") or ""),
        "obsidian_notes": {
            "status_before": baseline_payload.get("status"),
            "status_after": current_payload.get("status"),
            "validated_count_before": len(baseline_validated),
            "validated_count_after": len(current_validated),
            "failure_count_before": len(baseline_failures),
            "failure_count_after": len(current_failures),
            "warning_count_before": len(baseline_warnings),
            "warning_count_after": len(current_warnings),
            "validated_changes": _diff_validated_items(
                baseline_validated,
                current_validated,
            ),
            "failure_changes": _diff_issues(
                baseline_failures,
                current_failures,
            ),
            "warning_changes": _diff_issues(
                baseline_warnings,
                current_warnings,
            ),
        },
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Obsidian Notes Contract Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    notes = payload["obsidian_notes"]
    validated_changes = list(notes.get("validated_changes") or [])
    lines = [
        f"# Obsidian Notes Contract Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Status: `{notes['status_before']}` -> `{notes['status_after']}`",
        f"- Validated directors: `{notes['validated_count_before']}` -> `{notes['validated_count_after']}`",
        f"- Failures: `{notes['failure_count_before']}` -> `{notes['failure_count_after']}`",
        f"- Warnings: `{notes['warning_count_before']}` -> `{notes['warning_count_after']}`",
        "",
        "## Drift",
        "",
        f"- Validated set changes: `{len(validated_changes)}`",
        f"- New failures: `{len(notes['failure_changes']['added'])}`",
        f"- Resolved failures: `{len(notes['failure_changes']['resolved'])}`",
        f"- New warnings: `{len(notes['warning_changes']['added'])}`",
        f"- Resolved warnings: `{len(notes['warning_changes']['resolved'])}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current obsidian-notes audit date, YYYY-MM-DD. Defaults to today.",
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

    payload_path = output_dir / "obsidian_notes_contract_snapshot_diff.json"
    summary_path = output_dir / "summary.md"
    payload_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _write_summary(summary_path, payload)
    if payload["status"] == "skipped":
        print("Obsidian notes contract snapshot diff: skipped (no baseline found)")
    else:
        print("Obsidian notes contract snapshot diff: ok")
    print(f"Output: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
