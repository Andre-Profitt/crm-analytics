#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = ROOT / "output" / "deck_font_audit"
OUTPUT_ROOT = ROOT / "output" / "deck_font_audit_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _audit_path(run_date: str) -> Path:
    return AUDIT_ROOT / str(run_date)[:10] / "deck_font_audit.json"


def _available_audit_dates() -> list[str]:
    return sorted(path.parent.name for path in AUDIT_ROOT.glob("*/deck_font_audit.json"))


def _resolve_baseline_date(current_date: str, baseline_date: str | None = None) -> str | None:
    if baseline_date:
        return str(baseline_date)[:10]
    earlier = [run_date for run_date in _available_audit_dates() if run_date < current_date]
    if earlier:
        return earlier[-1]
    return None


def _deck_index(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in payload.get("decks") or []:
        name = str(item.get("deck") or "").strip()
        if name:
            index[name] = item
    return index


def _diff_list(before: list[Any], after: list[Any]) -> dict[str, list[Any]]:
    before_set = set(before)
    after_set = set(after)
    return {
        "added": sorted(after_set - before_set),
        "removed": sorted(before_set - after_set),
    }


def build_snapshot_diff(
    baseline_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> dict[str, Any]:
    baseline_index = _deck_index(baseline_payload)
    current_index = _deck_index(current_payload)
    deck_changes: list[dict[str, Any]] = []
    for deck_name in sorted(set(baseline_index) | set(current_index)):
        before_item = baseline_index.get(deck_name)
        after_item = current_index.get(deck_name)
        if before_item is None:
            deck_changes.append({"change": "added", "deck": deck_name, "after": after_item})
            continue
        if after_item is None:
            deck_changes.append({"change": "removed", "deck": deck_name, "before": before_item})
            continue
        field_changes: dict[str, Any] = {}
        missing_delta = _diff_list(
            list(before_item.get("font_missing_overall") or []),
            list(after_item.get("font_missing_overall") or []),
        )
        substituted_delta = _diff_list(
            list(before_item.get("font_substituted_overall") or []),
            list(after_item.get("font_substituted_overall") or []),
        )
        if missing_delta["added"] or missing_delta["removed"]:
            field_changes["font_missing_overall"] = missing_delta
        if substituted_delta["added"] or substituted_delta["removed"]:
            field_changes["font_substituted_overall"] = substituted_delta
        if before_item.get("font_missing_count") != after_item.get("font_missing_count"):
            field_changes["font_missing_count"] = {
                "before": before_item.get("font_missing_count"),
                "after": after_item.get("font_missing_count"),
                "delta": int(after_item.get("font_missing_count") or 0)
                - int(before_item.get("font_missing_count") or 0),
            }
        if before_item.get("font_substituted_count") != after_item.get("font_substituted_count"):
            field_changes["font_substituted_count"] = {
                "before": before_item.get("font_substituted_count"),
                "after": after_item.get("font_substituted_count"),
                "delta": int(after_item.get("font_substituted_count") or 0)
                - int(before_item.get("font_substituted_count") or 0),
            }
        if field_changes:
            deck_changes.append(
                {
                    "change": "modified",
                    "deck": deck_name,
                    "changes": field_changes,
                }
            )

    return {
        "status": "ok",
        "baseline_run_date": str(baseline_payload.get("run_date") or ""),
        "current_run_date": str(current_payload.get("run_date") or ""),
        "font_audit": {
            "status_before": baseline_payload.get("status"),
            "status_after": current_payload.get("status"),
            "deck_count_before": baseline_payload.get("deck_count"),
            "deck_count_after": current_payload.get("deck_count"),
            "decks_with_issues_before": baseline_payload.get("decks_with_issues"),
            "decks_with_issues_after": current_payload.get("decks_with_issues"),
            "failure_count_before": len(list(baseline_payload.get("failures") or [])),
            "failure_count_after": len(list(current_payload.get("failures") or [])),
            "deck_changes": deck_changes,
        },
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Deck Font Audit Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return
    font_audit = payload["font_audit"]
    deck_changes = list(font_audit.get("deck_changes") or [])
    lines = [
        f"# Deck Font Audit Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Status: `{font_audit['status_before']}` -> `{font_audit['status_after']}`",
        f"- Decks with issues: `{font_audit['decks_with_issues_before']}` -> `{font_audit['decks_with_issues_after']}`",
        f"- Failures: `{font_audit['failure_count_before']}` -> `{font_audit['failure_count_after']}`",
        f"- Modified deck audits: `{sum(1 for item in deck_changes if item.get('change') == 'modified')}`",
        "",
        "## Deck Changes",
        "",
    ]
    if not deck_changes:
        lines.append("- none")
    else:
        for item in deck_changes:
            if item["change"] == "modified":
                lines.append(f"- `{item['deck']}`: `{', '.join(sorted(item['changes'].keys()))}`")
            else:
                lines.append(f"- `{item['deck']}`: `{item['change']}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current font-audit date, YYYY-MM-DD. Defaults to today.",
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
        (output_dir / "deck_font_audit_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print("Deck font audit snapshot diff: skipped (no baseline found)")
        print(f"Output: {output_dir}")
        return 0

    baseline_path = _audit_path(baseline_date)
    if not baseline_path.exists():
        print(f"Baseline audit missing: {baseline_path}", file=sys.stderr)
        return 1

    payload = build_snapshot_diff(_load_json(baseline_path), _load_json(current_path))
    (output_dir / "deck_font_audit_snapshot_diff.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)
    print("Deck font audit snapshot diff: ok")
    print(f"Baseline: {baseline_date}")
    print(f"Output: {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
