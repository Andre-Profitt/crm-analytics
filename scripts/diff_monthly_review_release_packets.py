#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PACKET_ROOT = ROOT / "output" / "monthly_review_release_packets"
OUTPUT_ROOT = ROOT / "output" / "monthly_review_release_packet_snapshot_diff"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _packet_path(run_date: str, *, packet_root: Path | None = None) -> Path:
    root = packet_root or PACKET_ROOT
    return root / str(run_date)[:10] / "legacy_monthly_review_release_packet.json"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _available_packet_dates(*, packet_root: Path | None = None) -> list[str]:
    root = packet_root or PACKET_ROOT
    dates = []
    for path in sorted(root.glob("*/legacy_monthly_review_release_packet.json")):
        dates.append(path.parent.name)
    return dates


def _resolve_baseline_date(
    current_date: str,
    baseline_date: str | None = None,
    *,
    packet_root: Path | None = None,
) -> str | None:
    if baseline_date:
        return str(baseline_date)[:10]
    dates = [
        date
        for date in _available_packet_dates(packet_root=packet_root)
        if date != current_date
    ]
    earlier = [date for date in dates if date < current_date]
    if earlier:
        return earlier[-1]
    return None


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


def _diff_string_list(
    before_items: list[Any],
    after_items: list[Any],
) -> dict[str, list[str]]:
    before = {str(item).strip() for item in before_items if str(item).strip()}
    after = {str(item).strip() for item in after_items if str(item).strip()}
    return {
        "added": sorted(after - before),
        "resolved": sorted(before - after),
    }


def build_snapshot_diff(
    baseline_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> dict[str, Any]:
    gate_changes: dict[str, dict[str, dict[str, Any]]] = {}
    for key in (
        "source_contract",
        "data_quality",
        "workbook_contract",
        "sharepoint_analysis_contract",
        "deck_delivery_contract",
        "deck_font_audit",
        "tie_out",
        "obsidian_notes_contract",
    ):
        before = baseline_payload.get(key) or {}
        after = current_payload.get(key) or {}
        if not isinstance(before, dict) or not isinstance(after, dict):
            continue
        changes = _diff_scalar_map(before, after)
        if changes:
            gate_changes[key] = changes

    return {
        "status": "ok",
        "baseline_run_date": str(baseline_payload.get("run_date") or ""),
        "current_run_date": str(current_payload.get("run_date") or ""),
        "release_packet": {
            "status_before": baseline_payload.get("status"),
            "status_after": current_payload.get("status"),
            "publish_ready_before": baseline_payload.get("publish_ready"),
            "publish_ready_after": current_payload.get("publish_ready"),
            "pipeline_ok_before": baseline_payload.get("pipeline_ok"),
            "pipeline_ok_after": current_payload.get("pipeline_ok"),
            "step_count_changes": _diff_scalar_map(
                dict(baseline_payload.get("step_counts") or {}),
                dict(current_payload.get("step_counts") or {}),
            ),
            "output_count_changes": _diff_scalar_map(
                dict(baseline_payload.get("output_counts") or {}),
                dict(current_payload.get("output_counts") or {}),
            ),
            "publish_blocker_changes": _diff_string_list(
                list(baseline_payload.get("publish_blockers") or []),
                list(current_payload.get("publish_blockers") or []),
            ),
            "pipeline_blocker_changes": _diff_string_list(
                list(baseline_payload.get("pipeline_blockers") or []),
                list(current_payload.get("pipeline_blockers") or []),
            ),
            "gate_changes": gate_changes,
            "changed_gates": sorted(gate_changes),
        },
    }


def _render_change_map(changes: dict[str, dict[str, Any]]) -> str:
    parts: list[str] = []
    for index, (field, entry) in enumerate(changes.items()):
        if index >= 4:
            parts.append(f"+{len(changes) - 4} more")
            break
        parts.append(f"{field} `{entry.get('before')}` -> `{entry.get('after')}`")
    return "; ".join(parts)


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Monthly Review Release Packet Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
            "",
            "- No earlier release packet was available to diff against.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    release_packet = payload["release_packet"]
    publish_blockers = release_packet["publish_blocker_changes"]
    pipeline_blockers = release_packet["pipeline_blocker_changes"]
    lines = [
        (
            "# Monthly Review Release Packet Snapshot Diff — "
            f"{payload['baseline_run_date']} -> {payload['current_run_date']}"
        ),
        "",
        f"- Status: `{release_packet['status_before']}` -> `{release_packet['status_after']}`",
        (
            f"- Publish ready: `{release_packet['publish_ready_before']}` -> "
            f"`{release_packet['publish_ready_after']}`"
        ),
        (
            f"- Pipeline ok: `{release_packet['pipeline_ok_before']}` -> "
            f"`{release_packet['pipeline_ok_after']}`"
        ),
        f"- Changed gates: `{len(release_packet['changed_gates'])}`",
        f"- Publish blockers added: `{len(publish_blockers['added'])}`",
        f"- Publish blockers resolved: `{len(publish_blockers['resolved'])}`",
        f"- Pipeline blockers added: `{len(pipeline_blockers['added'])}`",
        f"- Pipeline blockers resolved: `{len(pipeline_blockers['resolved'])}`",
        "",
        "## Changed Gates",
        "",
    ]
    if release_packet["changed_gates"]:
        for gate_name in release_packet["changed_gates"]:
            lines.append(
                f"- `{gate_name}`: {_render_change_map(release_packet['gate_changes'][gate_name])}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Blocker Changes", ""])
    if publish_blockers["added"]:
        lines.append(f"- Publish blockers added: `{publish_blockers['added']}`")
    if publish_blockers["resolved"]:
        lines.append(f"- Publish blockers resolved: `{publish_blockers['resolved']}`")
    if pipeline_blockers["added"]:
        lines.append(f"- Pipeline blockers added: `{pipeline_blockers['added']}`")
    if pipeline_blockers["resolved"]:
        lines.append(f"- Pipeline blockers resolved: `{pipeline_blockers['resolved']}`")
    if (
        not publish_blockers["added"]
        and not publish_blockers["resolved"]
        and not pipeline_blockers["added"]
        and not pipeline_blockers["resolved"]
    ):
        lines.append("- none")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_snapshot_diff_bundle(*, output_root: Path, payload: dict[str, Any]) -> Path:
    run_date = str(payload["current_run_date"])
    run_dir = output_root / run_date
    run_dir.mkdir(parents=True, exist_ok=True)
    json_path = run_dir / "monthly_review_release_packet_snapshot_diff.json"
    summary_path = run_dir / "summary.md"
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _write_summary(summary_path, payload)
    return run_dir


def build_snapshot_diff_bundle(
    *,
    current_payload: dict[str, Any],
    packet_root: Path | None = None,
    output_root: Path | None = None,
    baseline_date: str | None = None,
) -> tuple[dict[str, Any], Path]:
    current_date = str(current_payload.get("run_date") or "")[:10]
    if not current_date:
        raise ValueError("current payload missing run_date")
    resolved_packet_root = packet_root or PACKET_ROOT
    resolved_output_root = output_root or OUTPUT_ROOT

    resolved_baseline = _resolve_baseline_date(
        current_date,
        baseline_date,
        packet_root=resolved_packet_root,
    )
    if resolved_baseline is None:
        payload = {
            "status": "skipped",
            "reason": "baseline_not_found",
            "baseline_run_date": None,
            "current_run_date": current_date,
        }
    else:
        baseline_payload = _load_json(
            _packet_path(resolved_baseline, packet_root=resolved_packet_root)
        )
        payload = build_snapshot_diff(baseline_payload, current_payload)
    run_dir = write_snapshot_diff_bundle(
        output_root=resolved_output_root,
        payload=payload,
    )
    return payload, run_dir


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current release packet date, YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--baseline-date",
        default=None,
        help="Optional baseline packet date. Defaults to the latest prior packet on disk.",
    )
    args = parser.parse_args()

    current_date = str(args.current_date)[:10]
    current_path = _packet_path(current_date)
    if not current_path.exists():
        raise SystemExit(f"Release packet missing: {current_path}")

    current_payload = _load_json(current_path)
    payload, run_dir = build_snapshot_diff_bundle(
        current_payload=current_payload,
        baseline_date=args.baseline_date,
    )
    print(f"Monthly review release packet snapshot diff: {payload['status']}")
    print(f"Output: {_display_path(run_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
