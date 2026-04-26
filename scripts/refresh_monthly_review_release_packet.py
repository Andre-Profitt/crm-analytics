#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOGS_ROOT = ROOT / "output" / "pipeline_logs"
DEFAULT_PACKET_ROOT = ROOT / "output" / "monthly_review_release_packets"
DEFAULT_PACKET_DIFF_ROOT = ROOT / "output" / "monthly_review_release_packet_snapshot_diff"
DEFAULT_REFRESH_AUDIT_ROOT = ROOT / "output" / "monthly_review_release_packet_refresh"
CORE_SEMANTIC_RELEASE_PACKET_FIELDS = {
    "status",
    "publish_ready",
}
VOLATILE_RELEASE_PACKET_FIELDS = {
    "history_generated_at",
    "refresh_audit_generated_at",
    "refresh_audit_scope",
    "refresh_audit_status",
    "refresh_semantic_change",
    "refresh_semantic_changed_fields",
    "refresh_audit_dir",
    "refresh_audit_json_path",
    "refresh_audit_summary_path",
}


def _load_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _manifest_paths(logs_root: Path) -> list[Path]:
    return sorted(logs_root.glob("*/manifest.json"))


def _display_path(path: Path, repo_root: Path = ROOT) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def _count_result_values(results: list[dict], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in results:
        value = str(item.get(key) or "missing")
        counts[value] = counts.get(value, 0) + 1
    return {
        status: counts[status]
        for status in sorted(counts, key=lambda status: (-counts[status], status))
    }


def _semantic_release_packet(payload: dict) -> dict:
    return {
        key: value
        for key, value in dict(payload or {}).items()
        if key in CORE_SEMANTIC_RELEASE_PACKET_FIELDS
    }


def _semantic_changed_fields(before_payload: dict, after_payload: dict) -> list[str]:
    before = _semantic_release_packet(before_payload)
    after = _semantic_release_packet(after_payload)
    changed = [
        key
        for key in sorted(set(before) | set(after))
        if before.get(key) != after.get(key)
    ]
    return changed


def build_refresh_audit_payload(
    *,
    scope: str,
    refresh_payload: dict,
    requested_run_date: str | None = None,
    requested_manifest_path: str | None = None,
) -> dict:
    results = list(refresh_payload.get("results") or [])
    failures = list(refresh_payload.get("failures") or [])
    history_counts = [
        int(item.get("history_run_count") or 0)
        for item in results
        if item.get("history_run_count") is not None
    ]
    semantic_change_counts = {"changed": 0, "unchanged": 0}
    semantic_change_field_counts: dict[str, int] = {}
    semantic_changed_run_dates: list[str] = []
    for item in results:
        changed_fields = list(item.get("semantic_changed_fields") or [])
        if changed_fields:
            semantic_change_counts["changed"] += 1
            run_date = str(item.get("run_date") or "")
            if run_date:
                semantic_changed_run_dates.append(run_date)
            for field in changed_fields:
                semantic_change_field_counts[field] = (
                    semantic_change_field_counts.get(field, 0) + 1
                )
        else:
            semantic_change_counts["unchanged"] += 1
    latest_history_result = None
    latest_history_count = -1
    for item in results:
        history_run_count = int(item.get("history_run_count") or 0)
        if history_run_count >= latest_history_count:
            latest_history_count = history_run_count
            latest_history_result = item
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": scope,
        "status": refresh_payload.get("status"),
        "manifest_count": int(refresh_payload.get("manifest_count") or 0),
        "refreshed_count": int(refresh_payload.get("refreshed_count") or 0),
        "failure_count": int(refresh_payload.get("failure_count") or 0),
        "requested_run_date": requested_run_date,
        "requested_manifest_path": requested_manifest_path,
        "packet_status_counts": _count_result_values(results, "packet_status"),
        "packet_diff_status_counts": _count_result_values(results, "packet_diff_status"),
        "history_run_count_after": max(history_counts) if history_counts else None,
        "history_current_green_streak_after": None
        if latest_history_result is None
        else latest_history_result.get("history_current_green_streak"),
        "history_latest_blocked_run_date_after": None
        if latest_history_result is None
        else latest_history_result.get("history_latest_blocked_run_date"),
        "history_latest_blocked_publish_blockers_after": []
        if latest_history_result is None
        else list(
            latest_history_result.get("history_latest_blocked_publish_blockers") or []
        ),
        "history_latest_blocked_pipeline_blockers_after": []
        if latest_history_result is None
        else list(
            latest_history_result.get("history_latest_blocked_pipeline_blockers") or []
        ),
        "history_latest_core_state_transition_baseline_run_date_after": None
        if latest_history_result is None
        else latest_history_result.get(
            "history_latest_core_state_transition_baseline_run_date"
        ),
        "history_latest_core_state_transition_run_date_after": None
        if latest_history_result is None
        else latest_history_result.get("history_latest_core_state_transition_run_date"),
        "history_latest_core_state_transition_changes_after": []
        if latest_history_result is None
        else list(
            latest_history_result.get("history_latest_core_state_transition_changes")
            or []
        ),
        "history_latest_core_state_transition_publish_blockers_added_after": []
        if latest_history_result is None
        else list(
            latest_history_result.get(
                "history_latest_core_state_transition_publish_blockers_added"
            )
            or []
        ),
        "history_latest_core_state_transition_publish_blockers_resolved_after": []
        if latest_history_result is None
        else list(
            latest_history_result.get(
                "history_latest_core_state_transition_publish_blockers_resolved"
            )
            or []
        ),
        "history_latest_core_state_transition_pipeline_blockers_added_after": []
        if latest_history_result is None
        else list(
            latest_history_result.get(
                "history_latest_core_state_transition_pipeline_blockers_added"
            )
            or []
        ),
        "history_latest_core_state_transition_pipeline_blockers_resolved_after": []
        if latest_history_result is None
        else list(
            latest_history_result.get(
                "history_latest_core_state_transition_pipeline_blockers_resolved"
            )
            or []
        ),
        "history_latest_drift_baseline_run_date_after": None
        if latest_history_result is None
        else latest_history_result.get("history_latest_drift_baseline_run_date"),
        "history_latest_drift_run_date_after": None
        if latest_history_result is None
        else latest_history_result.get("history_latest_drift_run_date"),
        "history_latest_drift_changed_gates_after": []
        if latest_history_result is None
        else list(
            latest_history_result.get("history_latest_drift_changed_gates") or []
        ),
        "history_latest_drift_change_summaries_after": []
        if latest_history_result is None
        else list(
            latest_history_result.get("history_latest_drift_change_summaries") or []
        ),
        "semantic_change_counts": semantic_change_counts,
        "semantic_change_field_counts": {
            field: semantic_change_field_counts[field]
            for field in sorted(
                semantic_change_field_counts,
                key=lambda field: (-semantic_change_field_counts[field], field),
            )
        },
        "semantic_changed_run_dates": semantic_changed_run_dates,
        "refreshed_run_dates": [str(item.get("run_date") or "") for item in results],
        "results": results,
        "failures": failures,
    }


def build_refresh_audit_markdown(payload: dict) -> str:
    lines = [
        "# Monthly Review Release Packet Refresh Audit",
        "",
        f"- Status: `{payload.get('status')}`",
        f"- Scope: `{payload.get('scope')}`",
        f"- Manifests seen: `{payload.get('manifest_count', 0)}`",
        f"- Refreshed: `{payload.get('refreshed_count', 0)}`",
        f"- Failed: `{payload.get('failure_count', 0)}`",
        f"- History run count after refresh: `{payload.get('history_run_count_after')}`",
        f"- Current green streak after refresh: `{payload.get('history_current_green_streak_after')}`",
        f"- Latest blocked run after refresh: `{payload.get('history_latest_blocked_run_date_after') or 'none'}`",
        f"- Semantic changes: `{(payload.get('semantic_change_counts') or {}).get('changed', 0)}`",
        f"- Semantic no-op refreshes: `{(payload.get('semantic_change_counts') or {}).get('unchanged', 0)}`",
    ]
    if payload.get("requested_run_date"):
        lines.append(f"- Requested run date: `{payload.get('requested_run_date')}`")
    if payload.get("requested_manifest_path"):
        lines.append(
            f"- Requested manifest: `{payload.get('requested_manifest_path')}`"
        )
    latest_blocked_publish_blockers_after = list(
        payload.get("history_latest_blocked_publish_blockers_after") or []
    )
    latest_blocked_pipeline_blockers_after = list(
        payload.get("history_latest_blocked_pipeline_blockers_after") or []
    )
    if latest_blocked_publish_blockers_after:
        lines.append(
            "- Latest blocked publish blockers after refresh: "
            f"`{latest_blocked_publish_blockers_after}`"
        )
    if latest_blocked_pipeline_blockers_after:
        lines.append(
            "- Latest blocked pipeline blockers after refresh: "
            f"`{latest_blocked_pipeline_blockers_after}`"
        )

    history_latest_core_state_transition_run_date_after = payload.get(
        "history_latest_core_state_transition_run_date_after"
    )
    if history_latest_core_state_transition_run_date_after:
        lines.append(
            "- Latest core state transition after refresh: "
            f"`{payload.get('history_latest_core_state_transition_baseline_run_date_after')}` -> "
            f"`{history_latest_core_state_transition_run_date_after}` "
            f"`{payload.get('history_latest_core_state_transition_changes_after')}`"
        )
    latest_core_publish_blockers_added_after = list(
        payload.get("history_latest_core_state_transition_publish_blockers_added_after")
        or []
    )
    latest_core_publish_blockers_resolved_after = list(
        payload.get(
            "history_latest_core_state_transition_publish_blockers_resolved_after"
        )
        or []
    )
    latest_core_pipeline_blockers_added_after = list(
        payload.get(
            "history_latest_core_state_transition_pipeline_blockers_added_after"
        )
        or []
    )
    latest_core_pipeline_blockers_resolved_after = list(
        payload.get(
            "history_latest_core_state_transition_pipeline_blockers_resolved_after"
        )
        or []
    )
    if latest_core_publish_blockers_added_after:
        lines.append(
            "- Latest core state transition publish blockers added after refresh: "
            f"`{latest_core_publish_blockers_added_after}`"
        )
    if latest_core_publish_blockers_resolved_after:
        lines.append(
            "- Latest core state transition publish blockers resolved after refresh: "
            f"`{latest_core_publish_blockers_resolved_after}`"
        )
    if latest_core_pipeline_blockers_added_after:
        lines.append(
            "- Latest core state transition pipeline blockers added after refresh: "
            f"`{latest_core_pipeline_blockers_added_after}`"
        )
    if latest_core_pipeline_blockers_resolved_after:
        lines.append(
            "- Latest core state transition pipeline blockers resolved after refresh: "
            f"`{latest_core_pipeline_blockers_resolved_after}`"
        )
    history_latest_drift_run_date_after = payload.get(
        "history_latest_drift_run_date_after"
    )
    if history_latest_drift_run_date_after:
        lines.append(
            "- Latest green drift after refresh: "
            f"`{payload.get('history_latest_drift_baseline_run_date_after')}` -> "
            f"`{history_latest_drift_run_date_after}` "
            f"`{payload.get('history_latest_drift_changed_gates_after')}`"
        )
    latest_drift_change_summaries_after = list(
        payload.get("history_latest_drift_change_summaries_after") or []
    )
    if latest_drift_change_summaries_after:
        lines.append(
            "- Latest green drift details after refresh: "
            f"`{latest_drift_change_summaries_after}`"
        )

    lines.extend(["", "## Packet Status Counts", ""])
    packet_status_counts = dict(payload.get("packet_status_counts") or {})
    if not packet_status_counts:
        lines.append("- None.")
    else:
        for status, count in packet_status_counts.items():
            lines.append(f"- `{status}`: `{count}`")

    lines.extend(["", "## Snapshot Diff Status Counts", ""])
    packet_diff_status_counts = dict(payload.get("packet_diff_status_counts") or {})
    if not packet_diff_status_counts:
        lines.append("- None.")
    else:
        for status, count in packet_diff_status_counts.items():
            lines.append(f"- `{status}`: `{count}`")

    lines.extend(["", "## Refreshed Runs", ""])
    results = list(payload.get("results") or [])
    if not results:
        lines.append("- None.")
    else:
        for item in results[:20]:
            changed_fields = list(item.get("semantic_changed_fields") or [])
            semantic_label = "changed" if changed_fields else "no-op"
            changed_fields_suffix = (
                f", fields `{changed_fields}`" if changed_fields else ""
            )
            lines.append(
                f"- `{item.get('run_date')}`: packet `{item.get('packet_status')}`, "
                f"diff `{item.get('packet_diff_status')}`, publish `{item.get('publish_ready')}`, "
                f"semantic `{semantic_label}`{changed_fields_suffix}"
            )

    lines.extend(["", "## Semantic Change Fields", ""])
    semantic_change_field_counts = dict(payload.get("semantic_change_field_counts") or {})
    if not semantic_change_field_counts:
        lines.append("- None.")
    else:
        for field, count in semantic_change_field_counts.items():
            lines.append(f"- `{field}`: `{count}`")

    lines.extend(["", "## Failures", ""])
    failures = list(payload.get("failures") or [])
    if not failures:
        lines.append("- None.")
    else:
        for item in failures[:20]:
            lines.append(
                f"- `{item.get('manifest_path')}`: `{item.get('error')}`"
            )

    return "\n".join(lines) + "\n"


def write_refresh_audit_bundle(
    *,
    output_root: Path,
    payload: dict,
    run_token: str,
) -> Path:
    run_dir = output_root / run_token
    run_dir.mkdir(parents=True, exist_ok=True)
    markdown = build_refresh_audit_markdown(payload)
    _save_json(run_dir / "refresh_audit.json", payload)
    _save_text(run_dir / "summary.md", markdown)
    _save_json(output_root / "latest.json", payload)
    _save_text(output_root / "latest.md", markdown)
    return run_dir


def attach_refresh_audit_to_manifest(
    *,
    manifest_path: Path,
    refresh_audit_dir: Path,
    repo_root: Path = ROOT,
) -> None:
    manifest = _load_json(manifest_path)
    refresh_audit_payload = _load_json(refresh_audit_dir / "refresh_audit.json")
    manifest_path_key = str(manifest_path.resolve())
    matched_result = None
    for item in refresh_audit_payload.get("results") or []:
        item_manifest_path = str(item.get("manifest_path") or "").strip()
        if not item_manifest_path:
            continue
        try:
            item_manifest_path = str(Path(item_manifest_path).resolve())
        except OSError:
            pass
        if item_manifest_path == manifest_path_key:
            matched_result = item
            break
    release_packet = dict(manifest.get("release_packet") or {})
    release_packet.update(
        {
            "refresh_audit_generated_at": refresh_audit_payload.get("generated_at"),
            "refresh_audit_scope": refresh_audit_payload.get("scope"),
            "refresh_audit_status": refresh_audit_payload.get("status"),
            "refresh_semantic_change": None
            if matched_result is None
            else bool(matched_result.get("semantic_change")),
            "refresh_semantic_changed_fields": []
            if matched_result is None
            else list(matched_result.get("semantic_changed_fields") or []),
            "refresh_audit_dir": _display_path(refresh_audit_dir, repo_root),
            "refresh_audit_json_path": _display_path(
                refresh_audit_dir / "refresh_audit.json",
                repo_root,
            ),
            "refresh_audit_summary_path": _display_path(
                refresh_audit_dir / "summary.md",
                repo_root,
            ),
        }
    )
    manifest["release_packet"] = release_packet
    _save_json(manifest_path, manifest)


def _history_manifest_fields(history_payload: dict, history_dir: Path, repo_root: Path) -> dict:
    latest_packet_diff = history_payload.get("latest_packet_diff") or {}
    latest_core_state_transition = history_payload.get("latest_core_state_transition") or {}
    return {
        "history_generated_at": history_payload.get("generated_at"),
        "history_run_count": history_payload.get("run_count"),
        "history_green_run_count": history_payload.get("green_run_count"),
        "history_blocked_run_count": history_payload.get("blocked_run_count"),
        "history_current_green_streak": history_payload.get("current_green_streak"),
        "history_latest_core_state_transition_baseline_run_date": latest_core_state_transition.get(
            "baseline_run_date"
        ),
        "history_latest_core_state_transition_run_date": latest_core_state_transition.get(
            "current_run_date"
        ),
        "history_latest_core_state_transition_changes": list(
            latest_core_state_transition.get("core_state_changes") or []
        ),
        "history_latest_core_state_transition_publish_blockers_added": list(
            latest_core_state_transition.get("publish_blockers_added") or []
        ),
        "history_latest_core_state_transition_publish_blockers_resolved": list(
            latest_core_state_transition.get("publish_blockers_resolved") or []
        ),
        "history_latest_core_state_transition_pipeline_blockers_added": list(
            latest_core_state_transition.get("pipeline_blockers_added") or []
        ),
        "history_latest_core_state_transition_pipeline_blockers_resolved": list(
            latest_core_state_transition.get("pipeline_blockers_resolved") or []
        ),
        "history_latest_blocked_run_date": history_payload.get("latest_blocked_run_date"),
        "history_latest_blocked_publish_blockers": list(
            history_payload.get("latest_blocked_publish_blockers") or []
        ),
        "history_latest_blocked_pipeline_blockers": list(
            history_payload.get("latest_blocked_pipeline_blockers") or []
        ),
        "history_latest_drift_baseline_run_date": latest_packet_diff.get(
            "baseline_run_date"
        ),
        "history_latest_drift_run_date": latest_packet_diff.get("current_run_date"),
        "history_latest_drift_changed_gates": list(
            latest_packet_diff.get("changed_gates") or []
        ),
        "history_latest_drift_change_summaries": list(
            latest_packet_diff.get("gate_change_summaries") or []
        ),
        "history_dir": str(history_dir.relative_to(repo_root)),
        "history_json_path": str((history_dir / "history.json").relative_to(repo_root)),
        "history_summary_path": str((history_dir / "summary.md").relative_to(repo_root)),
    }


def refresh_release_packet_for_manifest(
    *,
    manifest_path: Path,
    repo_root: Path = ROOT,
    packet_root: Path = DEFAULT_PACKET_ROOT,
    packet_diff_root: Path = DEFAULT_PACKET_DIFF_ROOT,
) -> dict:
    try:
        from build_monthly_review_release_packet import (
            build_monthly_review_release_packet,
            build_release_packet_manifest_payload,
            write_monthly_review_release_packet_bundle,
        )
        from diff_monthly_review_release_packets import build_snapshot_diff_bundle
    except ModuleNotFoundError:  # pragma: no cover
        from scripts.build_monthly_review_release_packet import (
            build_monthly_review_release_packet,
            build_release_packet_manifest_payload,
            write_monthly_review_release_packet_bundle,
        )
        from scripts.diff_monthly_review_release_packets import (
            build_snapshot_diff_bundle,
        )
    try:
        from build_monthly_review_release_packet_history import (
            refresh_release_packet_history,
        )
    except ModuleNotFoundError:  # pragma: no cover
        from scripts.build_monthly_review_release_packet_history import (
            refresh_release_packet_history,
        )

    manifest = _load_json(manifest_path)
    prior_release_packet = dict(manifest.get("release_packet") or {})
    packet = build_monthly_review_release_packet(
        manifest=manifest,
        manifest_path=manifest_path,
        repo_root=repo_root,
    )
    packet_dir = write_monthly_review_release_packet_bundle(
        output_root=packet_root,
        packet=packet,
    )
    packet_diff, packet_diff_dir = build_snapshot_diff_bundle(
        current_payload=packet,
        packet_root=packet_root,
        output_root=packet_diff_root,
    )
    manifest["release_packet"] = build_release_packet_manifest_payload(
        repo_root=repo_root,
        packet=packet,
        packet_dir=packet_dir,
        packet_diff=packet_diff,
        packet_diff_dir=packet_diff_dir,
    )
    history_payload, history_dir = refresh_release_packet_history(
        packet_root=packet_root,
        packet_diff_root=packet_diff_root,
        output_root=repo_root / "output" / "monthly_review_release_packet_history",
    )
    latest_core_state_transition = history_payload.get("latest_core_state_transition") or {}
    manifest["release_packet"].update(
        _history_manifest_fields(history_payload, history_dir, repo_root)
    )
    semantic_changed_fields = _semantic_changed_fields(
        prior_release_packet,
        manifest.get("release_packet") or {},
    )
    _save_json(manifest_path, manifest)
    return {
        "run_date": packet.get("run_date"),
        "packet_status": packet.get("status"),
        "publish_ready": packet.get("publish_ready"),
        "packet_dir": str(packet_dir),
        "packet_diff_status": packet_diff.get("status"),
        "packet_diff_dir": str(packet_diff_dir),
        "history_run_count": history_payload.get("run_count"),
        "history_current_green_streak": history_payload.get("current_green_streak"),
        "history_latest_blocked_run_date": history_payload.get(
            "latest_blocked_run_date"
        ),
        "history_latest_blocked_publish_blockers": list(
            history_payload.get("latest_blocked_publish_blockers") or []
        ),
        "history_latest_blocked_pipeline_blockers": list(
            history_payload.get("latest_blocked_pipeline_blockers") or []
        ),
        "history_latest_core_state_transition_baseline_run_date": (
            history_payload.get("latest_core_state_transition") or {}
        ).get("baseline_run_date"),
        "history_latest_core_state_transition_run_date": (
            history_payload.get("latest_core_state_transition") or {}
        ).get("current_run_date"),
        "history_latest_core_state_transition_changes": list(
            latest_core_state_transition.get("core_state_changes") or []
        ),
        "history_latest_core_state_transition_publish_blockers_added": list(
            latest_core_state_transition.get("publish_blockers_added") or []
        ),
        "history_latest_core_state_transition_publish_blockers_resolved": list(
            latest_core_state_transition.get("publish_blockers_resolved") or []
        ),
        "history_latest_core_state_transition_pipeline_blockers_added": list(
            latest_core_state_transition.get("pipeline_blockers_added") or []
        ),
        "history_latest_core_state_transition_pipeline_blockers_resolved": list(
            latest_core_state_transition.get("pipeline_blockers_resolved") or []
        ),
        "history_latest_drift_baseline_run_date": (
            (history_payload.get("latest_packet_diff") or {}).get("baseline_run_date")
        ),
        "history_latest_drift_run_date": (
            (history_payload.get("latest_packet_diff") or {}).get("current_run_date")
        ),
        "history_latest_drift_changed_gates": list(
            ((history_payload.get("latest_packet_diff") or {}).get("changed_gates"))
            or []
        ),
        "history_latest_drift_change_summaries": list(
            ((history_payload.get("latest_packet_diff") or {}).get(
                "gate_change_summaries"
            ))
            or []
        ),
        "history_dir": str(history_dir),
        "semantic_change": bool(semantic_changed_fields),
        "semantic_changed_fields": semantic_changed_fields,
        "manifest_path": str(manifest_path),
    }


def refresh_release_packets_for_all_manifests(
    *,
    logs_root: Path = DEFAULT_LOGS_ROOT,
    repo_root: Path = ROOT,
    packet_root: Path = DEFAULT_PACKET_ROOT,
    packet_diff_root: Path = DEFAULT_PACKET_DIFF_ROOT,
) -> dict:
    results: list[dict] = []
    failures: list[dict] = []
    for manifest_path in _manifest_paths(logs_root):
        try:
            results.append(
                refresh_release_packet_for_manifest(
                    manifest_path=manifest_path,
                    repo_root=repo_root,
                    packet_root=packet_root,
                    packet_diff_root=packet_diff_root,
                )
            )
        except Exception as exc:
            failures.append(
                {
                    "manifest_path": str(manifest_path),
                    "error": str(exc),
                }
            )
    return {
        "status": "ok" if not failures else "partial",
        "manifest_count": len(results) + len(failures),
        "refreshed_count": len(results),
        "failure_count": len(failures),
        "results": results,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--all",
        action="store_true",
        help="Refresh every dated manifest under output/pipeline_logs.",
    )
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Run date, YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Optional explicit manifest path. Defaults to output/pipeline_logs/<date>/manifest.json.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_REFRESH_AUDIT_ROOT,
        help="Output root for refresh audit artifacts.",
    )
    args = parser.parse_args()
    run_token = datetime.now().strftime("%Y%m%d-%H%M%S")

    if args.all:
        payload = refresh_release_packets_for_all_manifests()
        audit_payload = build_refresh_audit_payload(
            scope="all",
            refresh_payload=payload,
        )
        run_dir = write_refresh_audit_bundle(
            output_root=Path(args.output_root),
            payload=audit_payload,
            run_token=run_token,
        )
        for result in payload.get("results") or []:
            manifest_value = str(result.get("manifest_path") or "").strip()
            if not manifest_value:
                continue
            attach_refresh_audit_to_manifest(
                manifest_path=Path(manifest_value),
                refresh_audit_dir=run_dir,
            )
        print(
            "Monthly review release packet refresh all: "
            f"{payload['refreshed_count']} refreshed, {payload['failure_count']} failed"
        )
        print(f"Output: {_display_path(run_dir)}")
        return 0 if payload["failure_count"] == 0 else 1

    run_date = str(args.date)[:10]
    manifest_path = args.manifest or (DEFAULT_LOGS_ROOT / run_date / "manifest.json")
    if not manifest_path.exists():
        raise SystemExit(f"Manifest missing: {manifest_path}")

    payload = refresh_release_packet_for_manifest(manifest_path=manifest_path)
    audit_payload = build_refresh_audit_payload(
        scope="single",
        refresh_payload={
            "status": "ok",
            "manifest_count": 1,
            "refreshed_count": 1,
            "failure_count": 0,
            "results": [payload],
            "failures": [],
        },
        requested_run_date=run_date,
        requested_manifest_path=str(manifest_path),
    )
    run_dir = write_refresh_audit_bundle(
        output_root=Path(args.output_root),
        payload=audit_payload,
        run_token=run_token,
    )
    attach_refresh_audit_to_manifest(
        manifest_path=manifest_path,
        refresh_audit_dir=run_dir,
    )
    print(f"Monthly review release packet refresh: {payload['packet_status']}")
    print(f"Snapshot diff: {payload['packet_diff_status']}")
    print(f"Manifest: {payload['manifest_path']}")
    print(f"Output: {_display_path(run_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
