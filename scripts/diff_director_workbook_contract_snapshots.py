#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = ROOT / "output" / "director_workbook_contract"
OUTPUT_ROOT = ROOT / "output" / "director_workbook_contract_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _audit_path(run_date: str) -> Path:
    return AUDIT_ROOT / str(run_date)[:10] / "director_workbook_contract_audit.json"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _available_audit_dates() -> list[str]:
    dates = []
    for path in sorted(AUDIT_ROOT.glob("*/director_workbook_contract_audit.json")):
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


def _validated_index(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in items:
        slug = str(item.get("slug") or "").strip()
        if slug:
            index[slug] = item
    return index


def _issue_index(
    items: list[dict[str, Any]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    index: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in items:
        key = (
            str(item.get("slug") or "").strip(),
            str(item.get("issue") or "").strip(),
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


def _diff_validated_items(
    baseline_items: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_index = _validated_index(baseline_items)
    current_index = _validated_index(current_items)
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
        field_changes = _diff_scalar_map(
            {
                "workbook_path": before_item.get("workbook_path"),
                "sheet_count": before_item.get("sheet_count"),
                "historical_sheets": list(before_item.get("historical_sheets") or []),
            },
            {
                "workbook_path": after_item.get("workbook_path"),
                "sheet_count": after_item.get("sheet_count"),
                "historical_sheets": list(after_item.get("historical_sheets") or []),
            },
        )
        if field_changes:
            changes.append(
                {
                    "change": "modified",
                    "slug": slug,
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
        "workbook_contract": {
            "status_before": baseline_payload.get("status"),
            "status_after": current_payload.get("status"),
            "scope_before": baseline_payload.get("scope"),
            "scope_after": current_payload.get("scope"),
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
            f"# Director Workbook Contract Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
            "",
            "- No earlier workbook-contract audit was available to diff against.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    contract = payload["workbook_contract"]
    validated_changes = list(contract.get("validated_changes") or [])
    added = sum(1 for item in validated_changes if item.get("change") == "added")
    removed = sum(1 for item in validated_changes if item.get("change") == "removed")
    modified = sum(1 for item in validated_changes if item.get("change") == "modified")
    lines = [
        f"# Director Workbook Contract Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Status: `{contract['status_before']}` -> `{contract['status_after']}`",
        f"- Scope: `{contract['scope_before']}` -> `{contract['scope_after']}`",
        f"- Validated workbooks: `{contract['validated_count_before']}` -> `{contract['validated_count_after']}`",
        f"- Failures: `{contract['failure_count_before']}` -> `{contract['failure_count_after']}`",
        f"- Warnings: `{contract['warning_count_before']}` -> `{contract['warning_count_after']}`",
        "",
        "## Workbook Changes",
        "",
        f"- Added workbooks: `{added}`",
        f"- Removed workbooks: `{removed}`",
        f"- Modified workbooks: `{modified}`",
        "",
        "## Issue Changes",
        "",
        f"- New failures: `{len(contract['failure_changes']['added'])}`",
        f"- Resolved failures: `{len(contract['failure_changes']['resolved'])}`",
        f"- New warnings: `{len(contract['warning_changes']['added'])}`",
        f"- Resolved warnings: `{len(contract['warning_changes']['resolved'])}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current workbook-contract audit date, YYYY-MM-DD. Defaults to today.",
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
        (output_dir / "director_workbook_contract_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print("Director workbook contract snapshot diff: skipped (no baseline found)")
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
    (output_dir / "director_workbook_contract_snapshot_diff.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)

    print("Director workbook contract snapshot diff: ok")
    print(f"Baseline: {baseline_date}")
    print(f"Output: {_display_path(output_dir)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
