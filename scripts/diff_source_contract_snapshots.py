#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = ROOT / "output" / "source_contract_audit"
OUTPUT_ROOT = ROOT / "output" / "source_contract_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _audit_path(run_date: str) -> Path:
    return AUDIT_ROOT / str(run_date)[:10] / "source_contract_audit.json"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _available_audit_dates() -> list[str]:
    dates = []
    for path in sorted(AUDIT_ROOT.glob("*/source_contract_audit.json")):
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


def _candidate_lane(payload: dict[str, Any]) -> dict[str, Any]:
    lane = payload.get("candidate_forward_quarter") or payload.get("candidate_q3") or {}
    return lane if isinstance(lane, dict) else {}


def _index_items(
    items: list[dict[str, Any]],
    *,
    key_fields: tuple[str, ...],
) -> dict[tuple[str, ...], dict[str, Any]]:
    index: dict[tuple[str, ...], dict[str, Any]] = {}
    for item in items:
        key = tuple(str(item.get(field) or "") for field in key_fields)
        index[key] = item
    return index


def _diff_items(
    before_items: list[dict[str, Any]],
    after_items: list[dict[str, Any]],
    *,
    key_fields: tuple[str, ...],
    scalar_fields: tuple[str, ...] = (),
    set_fields: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    before_index = _index_items(before_items, key_fields=key_fields)
    after_index = _index_items(after_items, key_fields=key_fields)
    changes: list[dict[str, Any]] = []
    for key in sorted(set(before_index) | set(after_index)):
        before_item = before_index.get(key)
        after_item = after_index.get(key)
        identity = {
            field: key[idx]
            for idx, field in enumerate(key_fields)
        }
        if before_item is None:
            changes.append(
                {
                    "change": "added",
                    "identity": identity,
                    "after": after_item,
                }
            )
            continue
        if after_item is None:
            changes.append(
                {
                    "change": "removed",
                    "identity": identity,
                    "before": before_item,
                }
            )
            continue

        field_changes: dict[str, Any] = {}
        for field in scalar_fields:
            before_value = before_item.get(field)
            after_value = after_item.get(field)
            if before_value != after_value:
                field_changes[field] = {
                    "before": before_value,
                    "after": after_value,
                }
        for field in set_fields:
            before_values = sorted({str(value) for value in (before_item.get(field) or [])})
            after_values = sorted({str(value) for value in (after_item.get(field) or [])})
            if before_values != after_values:
                field_changes[field] = {
                    "added": sorted(set(after_values) - set(before_values)),
                    "removed": sorted(set(before_values) - set(after_values)),
                }
        if field_changes:
            changes.append(
                {
                    "change": "modified",
                    "identity": identity,
                    "changes": field_changes,
                }
            )
    return changes


def _diff_missing_config(
    before_items: list[dict[str, Any]],
    after_items: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    before_index = _index_items(
        before_items,
        key_fields=("territory", "source", "quarter_label"),
    )
    after_index = _index_items(
        after_items,
        key_fields=("territory", "source", "quarter_label"),
    )
    added = [
        after_index[key]
        for key in sorted(set(after_index) - set(before_index))
    ]
    resolved = [
        before_index[key]
        for key in sorted(set(before_index) - set(after_index))
    ]
    return {"added": added, "resolved": resolved}


def _issue_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for item in items:
        for issue in item.get("issues") or []:
            counts[str(issue)] += 1
    return dict(sorted(counts.items()))


def _issue_delta(
    before_items: list[dict[str, Any]],
    after_items: list[dict[str, Any]],
) -> dict[str, dict[str, int]]:
    before_counts = Counter(_issue_counts(before_items))
    after_counts = Counter(_issue_counts(after_items))
    new_counts: dict[str, int] = {}
    resolved_counts: dict[str, int] = {}
    for issue in sorted(set(before_counts) | set(after_counts)):
        delta = after_counts[issue] - before_counts[issue]
        if delta > 0:
            new_counts[issue] = delta
        elif delta < 0:
            resolved_counts[issue] = -delta
    return {
        "before": dict(sorted(before_counts.items())),
        "after": dict(sorted(after_counts.items())),
        "new": new_counts,
        "resolved": resolved_counts,
    }


def _quarter_labels(items: list[dict[str, Any]]) -> list[str]:
    return sorted({str(item.get("quarter_label") or "").strip() for item in items if item.get("quarter_label")})


def build_snapshot_diff(
    baseline_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> dict[str, Any]:
    baseline_active = baseline_payload.get("active_lane") or {}
    current_active = current_payload.get("active_lane") or {}
    baseline_candidate = _candidate_lane(baseline_payload)
    current_candidate = _candidate_lane(current_payload)

    active_dashboard_changes = _diff_items(
        list(baseline_active.get("dashboards") or []),
        list(current_active.get("dashboards") or []),
        key_fields=("dashboard_id",),
        scalar_fields=("status", "component_count"),
        set_fields=("component_report_ids", "missing_required_report_ids"),
    )
    active_pi_changes = _diff_items(
        list(baseline_active.get("pi_list_views") or []),
        list(current_active.get("pi_list_views") or []),
        key_fields=("territory",),
        scalar_fields=("status", "status_code", "list_view_id", "row_probe_count"),
        set_fields=("sample_fields",),
    )
    active_historical_changes = _diff_items(
        list(baseline_active.get("historical_reports") or []),
        list(current_active.get("historical_reports") or []),
        key_fields=("director_slug", "quarter_label"),
        scalar_fields=(
            "status",
            "report_id",
            "expected_start",
            "expected_end",
            "actual_start",
            "actual_end",
            "latest_snapshot_date",
        ),
        set_fields=("issues", "snapshot_dates"),
    )

    candidate_pi_changes = _diff_items(
        list(baseline_candidate.get("pi_list_views") or []),
        list(current_candidate.get("pi_list_views") or []),
        key_fields=("territory",),
        scalar_fields=("status", "status_code", "list_view_id", "row_probe_count"),
        set_fields=("sample_fields",),
    )
    candidate_historical_changes = _diff_items(
        list(baseline_candidate.get("historical_reports") or []),
        list(current_candidate.get("historical_reports") or []),
        key_fields=("director_slug", "quarter_label"),
        scalar_fields=(
            "status",
            "report_id",
            "expected_start",
            "expected_end",
            "actual_start",
            "actual_end",
            "latest_snapshot_date",
        ),
        set_fields=("issues", "snapshot_dates"),
    )
    candidate_missing_config = _diff_missing_config(
        list(baseline_candidate.get("missing_config") or []),
        list(current_candidate.get("missing_config") or []),
    )

    return {
        "status": "ok",
        "baseline_run_date": str(baseline_payload.get("run_date") or ""),
        "current_run_date": str(current_payload.get("run_date") or ""),
        "active_lane": {
            "status_before": baseline_active.get("status"),
            "status_after": current_active.get("status"),
            "quarter_labels_before": _quarter_labels(
                list(baseline_active.get("historical_reports") or [])
            ),
            "quarter_labels_after": _quarter_labels(
                list(current_active.get("historical_reports") or [])
            ),
            "issue_delta": _issue_delta(
                list(baseline_active.get("historical_reports") or []),
                list(current_active.get("historical_reports") or []),
            ),
            "dashboard_changes": active_dashboard_changes,
            "pi_list_view_changes": active_pi_changes,
            "historical_report_changes": active_historical_changes,
        },
        "candidate_lane": {
            "status_before": baseline_candidate.get("status"),
            "status_after": current_candidate.get("status"),
            "quarter_title_before": baseline_candidate.get("quarter_title"),
            "quarter_title_after": current_candidate.get("quarter_title"),
            "issue_delta": _issue_delta(
                list(baseline_candidate.get("historical_reports") or []),
                list(current_candidate.get("historical_reports") or []),
            ),
            "missing_config_count_before": len(
                list(baseline_candidate.get("missing_config") or [])
            ),
            "missing_config_count_after": len(
                list(current_candidate.get("missing_config") or [])
            ),
            "missing_config_changes": candidate_missing_config,
            "pi_list_view_changes": candidate_pi_changes,
            "historical_report_changes": candidate_historical_changes,
        },
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Source Contract Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
            "",
            "- No earlier source-contract audit was available to diff against.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    active = payload["active_lane"]
    candidate = payload["candidate_lane"]
    lines = [
        f"# Source Contract Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Active lane: `{active['status_before']}` -> `{active['status_after']}`",
        f"- Candidate lane: `{candidate['status_before']}` ({candidate['quarter_title_before']}) -> `{candidate['status_after']}` ({candidate['quarter_title_after']})",
        "",
        "## Active Lane",
        "",
        f"- Historical quarter family: `{', '.join(active['quarter_labels_before']) or 'none'}` -> `{', '.join(active['quarter_labels_after']) or 'none'}`",
        f"- New historical issues: `{active['issue_delta']['new'] or 'none'}`",
        f"- Resolved historical issues: `{active['issue_delta']['resolved'] or 'none'}`",
        f"- Dashboard changes: `{len(active['dashboard_changes'])}`",
        f"- PI list-view changes: `{len(active['pi_list_view_changes'])}`",
        f"- Historical report changes: `{len(active['historical_report_changes'])}`",
        "",
        "## Candidate Lane",
        "",
        f"- Candidate quarter: `{candidate['quarter_title_before']}` -> `{candidate['quarter_title_after']}`",
        f"- Missing config count: `{candidate['missing_config_count_before']}` -> `{candidate['missing_config_count_after']}`",
        f"- New historical issues: `{candidate['issue_delta']['new'] or 'none'}`",
        f"- Resolved historical issues: `{candidate['issue_delta']['resolved'] or 'none'}`",
        f"- PI list-view changes: `{len(candidate['pi_list_view_changes'])}`",
        f"- Historical report changes: `{len(candidate['historical_report_changes'])}`",
        f"- Missing config additions: `{len(candidate['missing_config_changes']['added'])}`",
        f"- Missing config resolutions: `{len(candidate['missing_config_changes']['resolved'])}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current source-contract audit date, YYYY-MM-DD. Defaults to today.",
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
        (output_dir / "source_contract_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print("Source contract snapshot diff: skipped (no baseline found)")
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
    (output_dir / "source_contract_snapshot_diff.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)

    print("Source contract snapshot diff: ok")
    print(f"Baseline: {baseline_date}")
    print(f"Output: {_display_path(output_dir)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
