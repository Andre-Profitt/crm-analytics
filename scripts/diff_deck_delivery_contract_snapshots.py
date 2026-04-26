#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = ROOT / "output" / "deck_delivery_contract"
OUTPUT_ROOT = ROOT / "output" / "deck_delivery_contract_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _audit_path(run_date: str) -> Path:
    return AUDIT_ROOT / str(run_date)[:10] / "deck_delivery_contract_audit.json"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _available_audit_dates() -> list[str]:
    dates = []
    for path in sorted(AUDIT_ROOT.glob("*/deck_delivery_contract_audit.json")):
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
                "deck_path": before_item.get("deck_path"),
                "sidecar_path": before_item.get("sidecar_path"),
                "slide_count": before_item.get("slide_count"),
                "file_size_bytes": before_item.get("file_size_bytes"),
            },
            {
                "deck_path": after_item.get("deck_path"),
                "sidecar_path": after_item.get("sidecar_path"),
                "slide_count": after_item.get("slide_count"),
                "file_size_bytes": after_item.get("file_size_bytes"),
            },
        )
        if metadata_changes:
            field_changes["metadata"] = metadata_changes
        sidecar_metric_changes = _diff_scalar_map(
            dict(before_item.get("sidecar_metrics") or {}),
            dict(after_item.get("sidecar_metrics") or {}),
        )
        if sidecar_metric_changes:
            field_changes["sidecar_metrics"] = sidecar_metric_changes
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
        "deck_delivery": {
            "status_before": baseline_payload.get("status"),
            "status_after": current_payload.get("status"),
            "expected_director_count_before": baseline_payload.get("expected_director_count"),
            "expected_director_count_after": current_payload.get("expected_director_count"),
            "validated_director_count_before": baseline_payload.get("validated_director_count"),
            "validated_director_count_after": current_payload.get("validated_director_count"),
            "failure_count_before": len(list(baseline_payload.get("failures") or [])),
            "failure_count_after": len(list(current_payload.get("failures") or [])),
            "warning_count_before": len(list(baseline_payload.get("warnings") or [])),
            "warning_count_after": len(list(current_payload.get("warnings") or [])),
            "director_changes": _diff_director_items(
                list(baseline_payload.get("directors") or []),
                list(current_payload.get("directors") or []),
            ),
            "exec_rollup_changes": _diff_scalar_map(
                dict(baseline_payload.get("exec_rollup") or {}),
                dict(current_payload.get("exec_rollup") or {}),
            ),
        },
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Deck Delivery Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
            "",
            "- No earlier deck-delivery audit was available to diff against.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    deck_delivery = payload["deck_delivery"]
    director_changes = list(deck_delivery.get("director_changes") or [])
    added = sum(1 for item in director_changes if item.get("change") == "added")
    removed = sum(1 for item in director_changes if item.get("change") == "removed")
    modified = sum(1 for item in director_changes if item.get("change") == "modified")
    lines = [
        f"# Deck Delivery Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Status: `{deck_delivery['status_before']}` -> `{deck_delivery['status_after']}`",
        f"- Expected director decks: `{deck_delivery['expected_director_count_before']}` -> `{deck_delivery['expected_director_count_after']}`",
        f"- Validated director decks: `{deck_delivery['validated_director_count_before']}` -> `{deck_delivery['validated_director_count_after']}`",
        f"- Failures: `{deck_delivery['failure_count_before']}` -> `{deck_delivery['failure_count_after']}`",
        f"- Warnings: `{deck_delivery['warning_count_before']}` -> `{deck_delivery['warning_count_after']}`",
        "",
        "## Director Changes",
        "",
        f"- Added directors: `{added}`",
        f"- Removed directors: `{removed}`",
        f"- Modified directors: `{modified}`",
        "",
        "## Exec Rollup Changes",
        "",
        f"- Changes: `{deck_delivery['exec_rollup_changes'] or 'none'}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current deck-delivery audit date, YYYY-MM-DD. Defaults to today.",
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
        (output_dir / "deck_delivery_contract_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print("Deck delivery snapshot diff: skipped (no baseline found)")
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
    (output_dir / "deck_delivery_contract_snapshot_diff.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)
    print("Deck delivery snapshot diff: ok")
    print(f"Baseline: {baseline_date}")
    print(f"Output: {_display_path(output_dir)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
