#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = ROOT / "output" / "tie_out"
OUTPUT_ROOT = ROOT / "output" / "tie_out_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _audit_path(run_date: str) -> Path:
    return AUDIT_ROOT / str(run_date)[:10] / "tie_out_audit.json"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _available_audit_dates() -> list[str]:
    dates = []
    for path in sorted(AUDIT_ROOT.glob("*/tie_out_audit.json")):
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


def _director_index(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in items:
        slug = str(item.get("slug") or "").strip()
        if slug:
            index[slug] = item
    return index


def _metric_index(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in items:
        metric = str(item.get("metric") or "").strip()
        if metric:
            index[metric] = item
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
        entry: dict[str, Any] = {"before": before_value, "after": after_value}
        if isinstance(before_value, (int, float)) and isinstance(after_value, (int, float)):
            entry["delta"] = round(float(after_value) - float(before_value), 2)
        changes[key] = entry
    return changes


def _diff_metric_items(
    baseline_items: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_index = _metric_index(baseline_items)
    current_index = _metric_index(current_items)
    changes: list[dict[str, Any]] = []
    for metric in sorted(set(baseline_index) | set(current_index)):
        before_item = baseline_index.get(metric)
        after_item = current_index.get(metric)
        if before_item is None:
            changes.append({"change": "added", "metric": metric, "after": after_item})
            continue
        if after_item is None:
            changes.append({"change": "removed", "metric": metric, "before": before_item})
            continue
        field_changes = _diff_scalar_map(
            {
                "salesforce": before_item.get("salesforce"),
                "extract": before_item.get("extract"),
                "regional": before_item.get("regional"),
                "deck": before_item.get("deck"),
                "status": before_item.get("status"),
            },
            {
                "salesforce": after_item.get("salesforce"),
                "extract": after_item.get("extract"),
                "regional": after_item.get("regional"),
                "deck": after_item.get("deck"),
                "status": after_item.get("status"),
            },
        )
        if field_changes:
            changes.append(
                {
                    "change": "modified",
                    "metric": metric,
                    "changes": field_changes,
                }
            )
    return changes


def _diff_director_items(
    baseline_items: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_index = _director_index(baseline_items)
    current_index = _director_index(current_items)
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
            {
                "director": before_item.get("director"),
                "territory": before_item.get("territory"),
                "mismatch_count": before_item.get("mismatch_count"),
            },
            {
                "director": after_item.get("director"),
                "territory": after_item.get("territory"),
                "mismatch_count": after_item.get("mismatch_count"),
            },
        )
        if metadata_changes:
            field_changes["metadata"] = metadata_changes
        metric_changes = _diff_metric_items(
            list(before_item.get("metrics") or []),
            list(after_item.get("metrics") or []),
        )
        if metric_changes:
            field_changes["metric_changes"] = metric_changes
        if field_changes:
            changes.append(
                {
                    "change": "modified",
                    "slug": slug,
                    "changes": field_changes,
                }
            )
    return changes


def build_snapshot_diff(
    baseline_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": "ok",
        "baseline_run_date": str(baseline_payload.get("run_date") or ""),
        "current_run_date": str(current_payload.get("run_date") or ""),
        "tie_out": {
            "status_before": baseline_payload.get("status"),
            "status_after": current_payload.get("status"),
            "checks_before": baseline_payload.get("checks"),
            "checks_after": current_payload.get("checks"),
            "mismatches_before": baseline_payload.get("mismatches"),
            "mismatches_after": current_payload.get("mismatches"),
            "directors_audited_before": baseline_payload.get("directors_audited"),
            "directors_audited_after": current_payload.get("directors_audited"),
            "directors_with_mismatches_before": baseline_payload.get("directors_with_mismatches"),
            "directors_with_mismatches_after": current_payload.get("directors_with_mismatches"),
            "director_changes": _diff_director_items(
                list(baseline_payload.get("directors") or []),
                list(current_payload.get("directors") or []),
            ),
            "failure_changes": _diff_scalar_map(
                {"failure_count": len(list(baseline_payload.get("failures") or []))},
                {"failure_count": len(list(current_payload.get("failures") or []))},
            ),
        },
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Tie-Out Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
            "",
            "- No earlier tie-out audit was available to diff against.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    tie_out = payload["tie_out"]
    director_changes = list(tie_out.get("director_changes") or [])
    added = sum(1 for item in director_changes if item.get("change") == "added")
    removed = sum(1 for item in director_changes if item.get("change") == "removed")
    modified = sum(1 for item in director_changes if item.get("change") == "modified")
    lines = [
        f"# Tie-Out Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Status: `{tie_out['status_before']}` -> `{tie_out['status_after']}`",
        f"- Checks: `{tie_out['checks_before']}` -> `{tie_out['checks_after']}`",
        f"- Mismatches: `{tie_out['mismatches_before']}` -> `{tie_out['mismatches_after']}`",
        f"- Directors with mismatches: `{tie_out['directors_with_mismatches_before']}` -> `{tie_out['directors_with_mismatches_after']}`",
        "",
        "## Director Changes",
        "",
        f"- Added directors: `{added}`",
        f"- Removed directors: `{removed}`",
        f"- Modified directors: `{modified}`",
        "",
        "## Failure Changes",
        "",
        f"- Failure count delta: `{tie_out['failure_changes'] or 'none'}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current tie-out audit date, YYYY-MM-DD. Defaults to today.",
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
        (output_dir / "tie_out_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print("Tie-out snapshot diff: skipped (no baseline found)")
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
    (output_dir / "tie_out_snapshot_diff.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)

    print("Tie-out snapshot diff: ok")
    print(f"Baseline: {baseline_date}")
    print(f"Output: {_display_path(output_dir)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
