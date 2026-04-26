#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PACKET_ROOT = ROOT / "output" / "monthly_review_release_packets"
PACKET_DIFF_ROOT = ROOT / "output" / "monthly_review_release_packet_snapshot_diff"
OUTPUT_ROOT = ROOT / "output" / "monthly_review_release_packet_history"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _packet_paths(packet_root: Path) -> list[Path]:
    return sorted(packet_root.glob("*/legacy_monthly_review_release_packet.json"))


def _packet_diff_paths(packet_diff_root: Path) -> list[Path]:
    return sorted(packet_diff_root.glob("*/monthly_review_release_packet_snapshot_diff.json"))


def _is_green(packet: dict[str, Any]) -> bool:
    return (
        str(packet.get("status") or "") == "ok"
        and bool(packet.get("publish_ready"))
        and bool(packet.get("pipeline_ok"))
    )


def _entry_from_packet(path: Path) -> dict[str, Any]:
    packet = _load_json(path)
    data_quality = packet.get("data_quality") or {}
    workbook = packet.get("workbook_contract") or {}
    deck_font = packet.get("deck_font_audit") or {}
    tie_out = packet.get("tie_out") or {}
    return {
        "run_date": str(packet.get("run_date") or ""),
        "status": packet.get("status"),
        "publish_ready": bool(packet.get("publish_ready")),
        "pipeline_ok": bool(packet.get("pipeline_ok")),
        "step_total": int((packet.get("step_counts") or {}).get("total") or 0),
        "step_failed": int((packet.get("step_counts") or {}).get("failed") or 0),
        "extract_count": int((packet.get("output_counts") or {}).get("extracts") or 0),
        "deck_count": int((packet.get("output_counts") or {}).get("decks") or 0),
        "report_count": int((packet.get("output_counts") or {}).get("reports") or 0),
        "publish_blocker_count": len(packet.get("publish_blockers") or []),
        "pipeline_blocker_count": len(packet.get("pipeline_blockers") or []),
        "source_contract_status": (packet.get("source_contract") or {}).get(
            "active_lane_status"
        ),
        "data_quality_gap_changes": data_quality.get("gap_changes"),
        "data_quality_critical_backlog_after": data_quality.get(
            "critical_backlog_after"
        ),
        "data_quality_important_backlog_after": data_quality.get(
            "important_backlog_after"
        ),
        "workbook_contract_status": workbook.get("status"),
        "workbook_validated_count": workbook.get("validated_count"),
        "deck_font_status": deck_font.get("status"),
        "deck_font_issues": deck_font.get("decks_with_issues"),
        "tie_out_status": tie_out.get("status"),
        "tie_out_mismatches": tie_out.get("mismatches"),
        "publish_blockers": list(packet.get("publish_blockers") or []),
        "pipeline_blockers": list(packet.get("pipeline_blockers") or []),
        "packet_path": _display_path(path),
        "summary_path": _display_path(path.parent / "summary.md"),
    }


def _blocker_rollup(
    entries: list[dict[str, Any]],
    blocker_key: str,
) -> list[dict[str, Any]]:
    blocker_counts: dict[str, int] = {}
    blocker_latest_dates: dict[str, str] = {}
    for entry in entries:
        run_date = str(entry.get("run_date") or "")
        for blocker in entry.get(blocker_key) or []:
            blocker_text = str(blocker).strip()
            if not blocker_text:
                continue
            blocker_counts[blocker_text] = blocker_counts.get(blocker_text, 0) + 1
            latest_run_date = blocker_latest_dates.get(blocker_text)
            if latest_run_date is None or run_date > latest_run_date:
                blocker_latest_dates[blocker_text] = run_date
    return [
        {
            "blocker": blocker,
            "count": blocker_counts[blocker],
            "latest_run_date": blocker_latest_dates.get(blocker),
        }
        for blocker in sorted(
            blocker_counts,
            key=lambda blocker: (
                -blocker_counts[blocker],
                blocker_latest_dates.get(blocker) or "",
                blocker,
            ),
        )
    ]


def _render_change_map(changes: dict[str, dict[str, Any]]) -> str:
    parts: list[str] = []
    for index, (field, entry) in enumerate(changes.items()):
        if index >= 4:
            parts.append(f"+{len(changes) - 4} more")
            break
        parts.append(f"{field} `{entry.get('before')}` -> `{entry.get('after')}`")
    return "; ".join(parts)


def _gate_change_summaries(release_packet: dict[str, Any]) -> list[str]:
    gate_changes = release_packet.get("gate_changes") or {}
    summaries: list[str] = []
    for gate_name in list(release_packet.get("changed_gates") or []):
        changes = gate_changes.get(gate_name) or {}
        if not isinstance(changes, dict) or not changes:
            summaries.append(str(gate_name))
            continue
        summaries.append(f"{gate_name}: {_render_change_map(changes)}")
    return summaries


def _diff_entry_from_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    if str(payload.get("status") or "") != "ok":
        return None
    release_packet = payload.get("release_packet") or {}
    changed_gates = list(release_packet.get("changed_gates") or [])
    publish_blocker_changes = release_packet.get("publish_blocker_changes") or {}
    pipeline_blocker_changes = release_packet.get("pipeline_blocker_changes") or {}
    return {
        "baseline_run_date": str(payload.get("baseline_run_date") or ""),
        "current_run_date": str(payload.get("current_run_date") or ""),
        "status_before": release_packet.get("status_before"),
        "status_after": release_packet.get("status_after"),
        "publish_ready_before": release_packet.get("publish_ready_before"),
        "publish_ready_after": release_packet.get("publish_ready_after"),
        "pipeline_ok_before": release_packet.get("pipeline_ok_before"),
        "pipeline_ok_after": release_packet.get("pipeline_ok_after"),
        "changed_gates": changed_gates,
        "changed_gate_count": len(changed_gates),
        "publish_blockers_added": list(publish_blocker_changes.get("added") or []),
        "publish_blockers_resolved": list(publish_blocker_changes.get("resolved") or []),
        "pipeline_blockers_added": list(pipeline_blocker_changes.get("added") or []),
        "pipeline_blockers_resolved": list(
            pipeline_blocker_changes.get("resolved") or []
        ),
        "gate_change_summaries": _gate_change_summaries(release_packet),
    }


def _load_recent_packet_diffs(packet_diff_root: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in _packet_diff_paths(packet_diff_root):
        entry = _diff_entry_from_payload(_load_json(path))
        if entry is not None:
            entries.append(entry)
    entries.sort(key=lambda item: str(item.get("current_run_date") or ""), reverse=True)
    return entries


def _core_state_transition_entries(
    recent_packet_diffs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for item in recent_packet_diffs:
        changes: list[str] = []
        if item.get("status_before") != item.get("status_after"):
            changes.append(
                f"status `{item.get('status_before')}` -> `{item.get('status_after')}`"
            )
        if item.get("publish_ready_before") != item.get("publish_ready_after"):
            changes.append(
                "publish_ready "
                f"`{item.get('publish_ready_before')}` -> `{item.get('publish_ready_after')}`"
            )
        if not changes:
            continue
        entries.append({**item, "core_state_changes": changes})
    return entries


def build_release_packet_history_payload(
    *,
    packet_root: Path = PACKET_ROOT,
    packet_diff_root: Path = PACKET_DIFF_ROOT,
) -> dict[str, Any]:
    entries = [_entry_from_packet(path) for path in _packet_paths(packet_root)]
    entries.sort(key=lambda item: str(item.get("run_date") or ""), reverse=True)
    blocked_runs = [entry for entry in entries if not _is_green(entry)]
    latest_blocked_run = blocked_runs[0] if blocked_runs else None
    recent_packet_diffs = _load_recent_packet_diffs(packet_diff_root)
    core_state_transitions = _core_state_transition_entries(recent_packet_diffs)
    green_gate_drift = [
        entry
        for entry in recent_packet_diffs
        if entry.get("status_before") == "ok"
        and entry.get("status_after") == "ok"
        and entry.get("changed_gate_count")
    ]

    green_run_count = sum(1 for entry in entries if _is_green(entry))
    blocked_run_count = sum(1 for entry in entries if not _is_green(entry))
    current_green_streak = 0
    for entry in entries:
        if _is_green(entry):
            current_green_streak += 1
            continue
        break

    latest = entries[0] if entries else {}
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_count": len(entries),
        "green_run_count": green_run_count,
        "blocked_run_count": blocked_run_count,
        "current_green_streak": current_green_streak,
        "latest_run_date": latest.get("run_date"),
        "latest_status": latest.get("status"),
        "latest_publish_ready": latest.get("publish_ready"),
        "latest_pipeline_ok": latest.get("pipeline_ok"),
        "latest_blocked_run_date": None
        if latest_blocked_run is None
        else latest_blocked_run.get("run_date"),
        "latest_blocked_status": None
        if latest_blocked_run is None
        else latest_blocked_run.get("status"),
        "latest_blocked_publish_blockers": []
        if latest_blocked_run is None
        else list(latest_blocked_run.get("publish_blockers") or []),
        "latest_blocked_pipeline_blockers": []
        if latest_blocked_run is None
        else list(latest_blocked_run.get("pipeline_blockers") or []),
        "latest_core_state_transition": core_state_transitions[0]
        if core_state_transitions
        else None,
        "recurring_publish_blockers": _blocker_rollup(entries, "publish_blockers"),
        "recurring_pipeline_blockers": _blocker_rollup(entries, "pipeline_blockers"),
        "latest_packet_diff": recent_packet_diffs[0] if recent_packet_diffs else None,
        "recent_packet_diffs": recent_packet_diffs,
        "core_state_transitions": core_state_transitions,
        "green_gate_drift": green_gate_drift,
        "blocked_runs": blocked_runs,
        "entries": entries,
    }


def build_release_packet_history_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Monthly Review Release Packet History",
        "",
        f"- Runs tracked: `{payload.get('run_count', 0)}`",
        f"- Green runs: `{payload.get('green_run_count', 0)}`",
        f"- Blocked runs: `{payload.get('blocked_run_count', 0)}`",
        f"- Current green streak: `{payload.get('current_green_streak', 0)}`",
        f"- Latest run: `{payload.get('latest_run_date') or 'none'}`",
        f"- Latest status: `{payload.get('latest_status') or 'none'}`",
        f"- Latest publish ready: `{payload.get('latest_publish_ready')}`",
        f"- Latest pipeline ok: `{payload.get('latest_pipeline_ok')}`",
        f"- Latest blocked run: `{payload.get('latest_blocked_run_date') or 'none'}`",
        "",
        "## Active Exceptions",
        "",
    ]
    latest_blocked_run_date = payload.get("latest_blocked_run_date")
    if not latest_blocked_run_date:
        lines.append("- None.")
    else:
        lines.append(
            f"- Latest blocked run `{latest_blocked_run_date}` with status `{payload.get('latest_blocked_status')}`."
        )
        latest_publish_blockers = list(payload.get("latest_blocked_publish_blockers") or [])
        latest_pipeline_blockers = list(payload.get("latest_blocked_pipeline_blockers") or [])
        if latest_publish_blockers:
            for blocker in latest_publish_blockers:
                lines.append(f"- Publish blocker: `{blocker}`")
        if latest_pipeline_blockers:
            for blocker in latest_pipeline_blockers:
                lines.append(f"- Pipeline blocker: `{blocker}`")

    lines.extend(["", "## Core State Transitions", ""])
    latest_core_state_transition = payload.get("latest_core_state_transition") or {}
    if not latest_core_state_transition:
        lines.append("- None.")
    else:
        lines.append(
            f"- Latest core state transition `{latest_core_state_transition.get('baseline_run_date')}` -> "
            f"`{latest_core_state_transition.get('current_run_date')}`: "
            f"`{latest_core_state_transition.get('core_state_changes')}`"
        )
        latest_publish_blockers_added = list(
            latest_core_state_transition.get("publish_blockers_added") or []
        )
        latest_publish_blockers_resolved = list(
            latest_core_state_transition.get("publish_blockers_resolved") or []
        )
        latest_pipeline_blockers_added = list(
            latest_core_state_transition.get("pipeline_blockers_added") or []
        )
        latest_pipeline_blockers_resolved = list(
            latest_core_state_transition.get("pipeline_blockers_resolved") or []
        )
        if latest_publish_blockers_added:
            lines.append(
                "- Latest core state transition publish blockers added: "
                f"`{latest_publish_blockers_added}`"
            )
        if latest_publish_blockers_resolved:
            lines.append(
                "- Latest core state transition publish blockers resolved: "
                f"`{latest_publish_blockers_resolved}`"
            )
        if latest_pipeline_blockers_added:
            lines.append(
                "- Latest core state transition pipeline blockers added: "
                f"`{latest_pipeline_blockers_added}`"
            )
        if latest_pipeline_blockers_resolved:
            lines.append(
                "- Latest core state transition pipeline blockers resolved: "
                f"`{latest_pipeline_blockers_resolved}`"
            )
        for entry in list(payload.get("core_state_transitions") or [])[1:5]:
            lines.append(
                f"- Earlier transition `{entry.get('baseline_run_date')}` -> "
                f"`{entry.get('current_run_date')}`: `{entry.get('core_state_changes')}`"
            )

    lines.extend(["", "## Recurring Blockers", ""])
    recurring_publish = list(payload.get("recurring_publish_blockers") or [])
    recurring_pipeline = list(payload.get("recurring_pipeline_blockers") or [])
    if not recurring_publish and not recurring_pipeline:
        lines.append("- None.")
    else:
        for item in recurring_publish[:5]:
            lines.append(
                f"- Publish: `{item['blocker']}` (`{item['count']}` runs, latest `{item['latest_run_date']}`)"
            )
        for item in recurring_pipeline[:5]:
            lines.append(
                f"- Pipeline: `{item['blocker']}` (`{item['count']}` runs, latest `{item['latest_run_date']}`)"
            )

    lines.extend(["", "## Blocked Runs", ""])
    blocked_runs = list(payload.get("blocked_runs") or [])
    if not blocked_runs:
        lines.append("- None.")
    else:
        for entry in blocked_runs[:10]:
            blocker_parts: list[str] = []
            publish_blockers = list(entry.get("publish_blockers") or [])
            pipeline_blockers = list(entry.get("pipeline_blockers") or [])
            if publish_blockers:
                blocker_parts.append(f"publish `{publish_blockers[0]}`")
            if pipeline_blockers:
                blocker_parts.append(f"pipeline `{pipeline_blockers[0]}`")
            blocker_summary = "; ".join(blocker_parts) if blocker_parts else "no blockers recorded"
            lines.append(
                f"- `{entry['run_date']}`: status `{entry['status']}`, publish `{entry['publish_ready']}`, pipeline `{entry['pipeline_ok']}`, {blocker_summary}"
            )

    lines.extend(["", "## Recent Gate Drift", ""])
    green_gate_drift = list(payload.get("green_gate_drift") or [])
    if not green_gate_drift:
        lines.append("- None.")
    else:
        for entry in green_gate_drift[:5]:
            lines.append(
                f"- `{entry['baseline_run_date']} -> {entry['current_run_date']}`: "
                f"status `{entry['status_before']}` -> `{entry['status_after']}`, "
                f"publish `{entry['publish_ready_before']}` -> `{entry['publish_ready_after']}`, "
                f"pipeline `{entry['pipeline_ok_before']}` -> `{entry['pipeline_ok_after']}`, "
                f"changed gates `{entry['changed_gates']}`"
            )
            gate_change_summaries = list(entry.get("gate_change_summaries") or [])
            if gate_change_summaries:
                lines.append(f"- Drift details: `{gate_change_summaries}`")

    lines.extend(["", "## Recent Runs", ""])
    entries = list(payload.get("entries") or [])
    if not entries:
        lines.append("- none")
    else:
        for entry in entries[:10]:
            lines.append(
                f"- `{entry['run_date']}`: status `{entry['status']}`, "
                f"publish `{entry['publish_ready']}`, pipeline `{entry['pipeline_ok']}`, "
                f"critical `{entry['data_quality_critical_backlog_after']}`, "
                f"important `{entry['data_quality_important_backlog_after']}`, "
                f"font issues `{entry['deck_font_issues']}`, "
                f"tie-out mismatches `{entry['tie_out_mismatches']}`"
            )
    return "\n".join(lines) + "\n"


def write_release_packet_history_bundle(
    *,
    output_root: Path,
    payload: dict[str, Any],
) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    markdown = build_release_packet_history_markdown(payload)
    _save_json(output_root / "history.json", payload)
    _save_text(output_root / "summary.md", markdown)
    _save_json(output_root / "latest.json", payload)
    _save_text(output_root / "latest.md", markdown)
    return output_root


def refresh_release_packet_history(
    *,
    packet_root: Path = PACKET_ROOT,
    packet_diff_root: Path = PACKET_DIFF_ROOT,
    output_root: Path = OUTPUT_ROOT,
) -> tuple[dict[str, Any], Path]:
    payload = build_release_packet_history_payload(
        packet_root=packet_root,
        packet_diff_root=packet_diff_root,
    )
    run_dir = write_release_packet_history_bundle(
        output_root=output_root,
        payload=payload,
    )
    return payload, run_dir


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--packet-root",
        type=Path,
        default=PACKET_ROOT,
        help="Root directory containing dated legacy monthly review release packets.",
    )
    parser.add_argument(
        "--packet-diff-root",
        type=Path,
        default=PACKET_DIFF_ROOT,
        help="Root directory containing dated monthly review release packet snapshot diffs.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=OUTPUT_ROOT,
        help="Output root for rolling release packet history artifacts.",
    )
    args = parser.parse_args()

    payload, run_dir = refresh_release_packet_history(
        packet_root=Path(args.packet_root),
        packet_diff_root=Path(args.packet_diff_root),
        output_root=Path(args.output_root),
    )
    print(f"Monthly review release packet history: {payload['run_count']} run(s)")
    print(f"Output: {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
