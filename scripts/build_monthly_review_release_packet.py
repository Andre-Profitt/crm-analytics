#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOGS_ROOT = ROOT / "output" / "pipeline_logs"
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "monthly_review_release_packets"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _display_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def _step_index(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for step in manifest.get("steps") or []:
        name = str(step.get("name") or "").strip()
        if name:
            index[name] = step
    return index


def _find_artifact_path(
    manifest: dict[str, Any],
    artifact_type: str,
    repo_root: Path,
) -> Path | None:
    for step in manifest.get("steps") or []:
        for artifact in step.get("artifacts") or []:
            if str(artifact.get("type") or "") != artifact_type:
                continue
            value = str(artifact.get("path") or "").strip()
            if not value:
                continue
            path = Path(value)
            return path if path.is_absolute() else repo_root / path
    return None


def _fallback_artifact_path(run_date: str, artifact_type: str, repo_root: Path) -> Path | None:
    date_token = str(run_date)[:10]
    mapping = {
        "source_contract_audit": repo_root / "output" / "source_contract_audit" / date_token / "source_contract_audit.json",
        "source_contract_summary": repo_root / "output" / "source_contract_audit" / date_token / "summary.md",
        "data_quality_flags": repo_root / "output" / "data_quality" / date_token / "flags.json",
        "data_quality_summary": repo_root / "output" / "data_quality" / date_token / "summary.md",
        "data_quality_snapshot_diff": repo_root / "output" / "data_quality_snapshot_diff" / date_token / "data_quality_snapshot_diff.json",
        "data_quality_snapshot_diff_summary": repo_root / "output" / "data_quality_snapshot_diff" / date_token / "summary.md",
        "director_workbook_contract_audit": repo_root / "output" / "director_workbook_contract" / date_token / "director_workbook_contract_audit.json",
        "director_workbook_contract_summary": repo_root / "output" / "director_workbook_contract" / date_token / "summary.md",
        "sharepoint_analysis_contract_audit": repo_root / "output" / "sharepoint_analysis_contract" / date_token / "sharepoint_analysis_contract_audit.json",
        "sharepoint_analysis_contract_summary": repo_root / "output" / "sharepoint_analysis_contract" / date_token / "summary.md",
        "deck_delivery_contract_audit": repo_root / "output" / "deck_delivery_contract" / date_token / "deck_delivery_contract_audit.json",
        "deck_delivery_contract_summary": repo_root / "output" / "deck_delivery_contract" / date_token / "summary.md",
        "deck_font_audit": repo_root / "output" / "deck_font_audit" / date_token / "deck_font_audit.json",
        "deck_font_audit_summary": repo_root / "output" / "deck_font_audit" / date_token / "summary.md",
        "tie_out_audit": repo_root / "output" / "tie_out" / date_token / "tie_out_audit.json",
        "tie_out_summary": repo_root / "output" / "tie_out" / date_token / "summary.md",
        "deck_scope_audit": repo_root / "output" / "deck_scope_audit" / date_token / "deck_scope_audit.json",
        "deck_scope_summary": repo_root / "output" / "deck_scope_audit" / date_token / "summary.md",
        "obsidian_notes_contract_audit": repo_root / "output" / "obsidian_notes_contract" / date_token / "obsidian_notes_contract_audit.json",
        "obsidian_notes_contract_summary": repo_root / "output" / "obsidian_notes_contract" / date_token / "summary.md",
    }
    return mapping.get(artifact_type)


def _resolve_artifact_path(
    manifest: dict[str, Any],
    artifact_type: str,
    *,
    repo_root: Path,
    run_date: str,
) -> Path | None:
    path = _find_artifact_path(manifest, artifact_type, repo_root)
    if path is not None and path.exists():
        return path
    fallback = _fallback_artifact_path(run_date, artifact_type, repo_root)
    if fallback is not None and fallback.exists():
        return fallback
    return path


def _load_optional_artifact_json(
    manifest: dict[str, Any],
    artifact_type: str,
    *,
    repo_root: Path,
    run_date: str,
) -> dict[str, Any] | None:
    path = _resolve_artifact_path(
        manifest,
        artifact_type,
        repo_root=repo_root,
        run_date=run_date,
    )
    if path is None or not path.exists():
        return None
    return _load_json(path)


def _artifact_reference(
    manifest: dict[str, Any],
    artifact_type: str,
    *,
    repo_root: Path,
    run_date: str,
) -> str | None:
    path = _resolve_artifact_path(
        manifest,
        artifact_type,
        repo_root=repo_root,
        run_date=run_date,
    )
    if path is None or not path.exists():
        return None
    return _display_path(path, repo_root)


def _step_summary(manifest: dict[str, Any]) -> dict[str, int]:
    counts = {"ok": 0, "failed": 0, "blocked": 0, "other": 0, "total": 0}
    for step in manifest.get("steps") or []:
        counts["total"] += 1
        status = str(step.get("status") or "")
        if status in counts:
            counts[status] += 1
        else:
            counts["other"] += 1
    return counts


def _first_step_failure(manifest: dict[str, Any]) -> str | None:
    for step in manifest.get("steps") or []:
        if str(step.get("status") or "") in {"failed", "blocked"}:
            return f"{step.get('name')}: {step.get('status')}"
    return None


def _source_candidate_lane(source_audit: dict[str, Any] | None) -> dict[str, Any] | None:
    if not source_audit:
        return None
    candidate = source_audit.get("candidate_forward_quarter")
    if isinstance(candidate, dict):
        return candidate
    candidate = source_audit.get("candidate_q3")
    return candidate if isinstance(candidate, dict) else None


def build_monthly_review_release_packet(
    *,
    manifest: dict[str, Any],
    manifest_path: Path,
    repo_root: Path,
) -> dict[str, Any]:
    run_date = str(manifest.get("run_date") or "")[:10]
    if not run_date:
        raise ValueError("manifest missing run_date")

    step_counts = _step_summary(manifest)
    source_audit = _load_optional_artifact_json(
        manifest,
        "source_contract_audit",
        repo_root=repo_root,
        run_date=run_date,
    )
    data_quality_diff = _load_optional_artifact_json(
        manifest,
        "data_quality_snapshot_diff",
        repo_root=repo_root,
        run_date=run_date,
    )
    workbook_contract = _load_optional_artifact_json(
        manifest,
        "director_workbook_contract_audit",
        repo_root=repo_root,
        run_date=run_date,
    )
    sharepoint_contract = _load_optional_artifact_json(
        manifest,
        "sharepoint_analysis_contract_audit",
        repo_root=repo_root,
        run_date=run_date,
    )
    deck_delivery = _load_optional_artifact_json(
        manifest,
        "deck_delivery_contract_audit",
        repo_root=repo_root,
        run_date=run_date,
    )
    deck_font = _load_optional_artifact_json(
        manifest,
        "deck_font_audit",
        repo_root=repo_root,
        run_date=run_date,
    )
    tie_out = _load_optional_artifact_json(
        manifest,
        "tie_out_audit",
        repo_root=repo_root,
        run_date=run_date,
    )
    deck_scope = _load_optional_artifact_json(
        manifest,
        "deck_scope_audit",
        repo_root=repo_root,
        run_date=run_date,
    )
    obsidian_notes = _load_optional_artifact_json(
        manifest,
        "obsidian_notes_contract_audit",
        repo_root=repo_root,
        run_date=run_date,
    )

    source_active_status = None
    source_candidate_status = None
    if source_audit:
        active_lane = source_audit.get("active_lane") or {}
        source_active_status = active_lane.get("status")
        candidate_lane = _source_candidate_lane(source_audit) or {}
        source_candidate_status = candidate_lane.get("status")

    publish_blockers: list[str] = []
    pipeline_blockers: list[str] = []

    if source_active_status not in {None, "ok"}:
        publish_blockers.append(f"Active source contract status is `{source_active_status}`.")
    if workbook_contract and str(workbook_contract.get("status") or "") != "ok":
        publish_blockers.append(
            f"Director workbook contract is `{workbook_contract.get('status')}`."
        )
    if sharepoint_contract and str(sharepoint_contract.get("status") or "") != "ok":
        publish_blockers.append(
            f"SharePoint analysis contract is `{sharepoint_contract.get('status')}`."
        )
    if deck_delivery and str(deck_delivery.get("status") or "") != "ok":
        publish_blockers.append(
            f"Deck delivery contract is `{deck_delivery.get('status')}`."
        )
    if tie_out:
        tie_out_status = str(tie_out.get("status") or "")
        mismatches = int(tie_out.get("mismatches") or 0)
        if tie_out_status != "ok" or mismatches > 0:
            publish_blockers.append(
                f"Tie-out is `{tie_out_status}` with `{mismatches}` mismatches."
            )
    if deck_font:
        font_status = str(deck_font.get("status") or "")
        decks_with_issues = int(deck_font.get("decks_with_issues") or 0)
        if font_status != "ok" or decks_with_issues > 0:
            publish_blockers.append(
                f"Deck font audit is `{font_status}` with `{decks_with_issues}` deck(s) showing issues."
            )

    step_failure = _first_step_failure(manifest)
    if step_failure:
        pipeline_blockers.append(f"Pipeline step failure: {step_failure}.")
    if deck_scope and str(deck_scope.get("status") or "") != "ok":
        pipeline_blockers.append(
            f"Deck scope audit is `{deck_scope.get('status')}`."
        )
    if obsidian_notes and str(obsidian_notes.get("status") or "") != "ok":
        pipeline_blockers.append(
            f"Obsidian notes contract is `{obsidian_notes.get('status')}`."
        )

    publish_ready = not publish_blockers
    pipeline_ok = step_counts["failed"] == 0 and step_counts["blocked"] == 0 and not pipeline_blockers
    status = "ok" if publish_ready and pipeline_ok else "blocked"

    outputs = manifest.get("outputs") or {}
    data_quality_summary = None
    if data_quality_diff:
        dq = data_quality_diff.get("data_quality") or {}
        data_quality_summary = {
            "gap_changes": len(dq.get("gap_changes") or []),
            "baseline_changes": len(dq.get("baseline_changes") or []),
            "critical_backlog_after": (dq.get("severity_totals_after") or {}).get("Critical"),
            "important_backlog_after": (dq.get("severity_totals_after") or {}).get("Important"),
        }

    packet = {
        "status": status,
        "run_date": run_date,
        "started_at": manifest.get("started_at"),
        "finished_at": manifest.get("finished_at"),
        "manifest_path": _display_path(manifest_path, repo_root),
        "step_counts": step_counts,
        "output_counts": {
            "extracts": len(outputs.get("extracts") or []),
            "decks": len(outputs.get("decks") or []),
            "reports": len(outputs.get("reports") or []),
        },
        "publish_ready": publish_ready,
        "pipeline_ok": pipeline_ok,
        "publish_blockers": publish_blockers,
        "pipeline_blockers": pipeline_blockers,
        "source_contract": {
            "active_lane_status": source_active_status,
            "candidate_forward_status": source_candidate_status,
        },
        "data_quality": data_quality_summary,
        "workbook_contract": (
            None
            if workbook_contract is None
            else {
                "status": workbook_contract.get("status"),
                "scope": workbook_contract.get("scope"),
                "validated_count": len(workbook_contract.get("validated") or []),
                "failure_count": len(workbook_contract.get("failures") or []),
                "warning_count": len(workbook_contract.get("warnings") or []),
            }
        ),
        "sharepoint_analysis_contract": (
            None
            if sharepoint_contract is None
            else {
                "status": sharepoint_contract.get("status"),
                "validated_count": len(sharepoint_contract.get("validated") or []),
                "failure_count": len(sharepoint_contract.get("failures") or []),
                "warning_count": len(sharepoint_contract.get("warnings") or []),
            }
        ),
        "deck_delivery_contract": (
            None
            if deck_delivery is None
            else {
                "status": deck_delivery.get("status"),
                "validated_director_count": deck_delivery.get("validated_director_count"),
                "expected_director_count": deck_delivery.get("expected_director_count"),
                "failure_count": len(deck_delivery.get("failures") or []),
                "warning_count": len(deck_delivery.get("warnings") or []),
            }
        ),
        "deck_font_audit": (
            None
            if deck_font is None
            else {
                "status": deck_font.get("status"),
                "deck_count": deck_font.get("deck_count"),
                "decks_with_issues": deck_font.get("decks_with_issues"),
                "failure_count": len(deck_font.get("failures") or []),
            }
        ),
        "tie_out": (
            None
            if tie_out is None
            else {
                "status": tie_out.get("status"),
                "checks": tie_out.get("checks"),
                "mismatches": tie_out.get("mismatches"),
                "directors_audited": tie_out.get("directors_audited"),
            }
        ),
        "obsidian_notes_contract": (
            None
            if obsidian_notes is None
            else {
                "status": obsidian_notes.get("status"),
                "validated_count": len(obsidian_notes.get("validated") or []),
                "failure_count": len(obsidian_notes.get("failures") or []),
            }
        ),
        "key_artifacts": {
            "source_contract_summary": _artifact_reference(
                manifest,
                "source_contract_summary",
                repo_root=repo_root,
                run_date=run_date,
            ),
            "data_quality_snapshot_diff_summary": _artifact_reference(
                manifest,
                "data_quality_snapshot_diff_summary",
                repo_root=repo_root,
                run_date=run_date,
            ),
            "sharepoint_analysis_contract_summary": _artifact_reference(
                manifest,
                "sharepoint_analysis_contract_summary",
                repo_root=repo_root,
                run_date=run_date,
            ),
            "deck_delivery_contract_summary": _artifact_reference(
                manifest,
                "deck_delivery_contract_summary",
                repo_root=repo_root,
                run_date=run_date,
            ),
            "deck_font_audit_summary": _artifact_reference(
                manifest,
                "deck_font_audit_summary",
                repo_root=repo_root,
                run_date=run_date,
            ),
            "tie_out_summary": _artifact_reference(
                manifest,
                "tie_out_summary",
                repo_root=repo_root,
                run_date=run_date,
            ),
            "obsidian_notes_contract_summary": _artifact_reference(
                manifest,
                "obsidian_notes_contract_summary",
                repo_root=repo_root,
                run_date=run_date,
            ),
        },
    }
    return packet


def build_monthly_review_release_packet_markdown(packet: dict[str, Any]) -> str:
    lines = [
        f"# Monthly Review Release Packet — {packet['run_date']}",
        "",
        f"- Status: `{packet['status']}`",
        f"- Publish ready: `{packet['publish_ready']}`",
        f"- Pipeline ok: `{packet['pipeline_ok']}`",
        f"- Manifest: `{packet['manifest_path']}`",
        "",
        "## Step Summary",
        "",
        f"- Steps total: `{packet['step_counts']['total']}`",
        f"- Steps ok: `{packet['step_counts']['ok']}`",
        f"- Steps failed: `{packet['step_counts']['failed']}`",
        f"- Steps blocked: `{packet['step_counts']['blocked']}`",
        "",
        "## Outputs",
        "",
        f"- Extract workbooks: `{packet['output_counts']['extracts']}`",
        f"- Decks: `{packet['output_counts']['decks']}`",
        f"- Analysis reports: `{packet['output_counts']['reports']}`",
        "",
        "## Publish Blockers",
        "",
    ]
    if packet["publish_blockers"]:
        for blocker in packet["publish_blockers"]:
            lines.append(f"- {blocker}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Pipeline Blockers", ""])
    if packet["pipeline_blockers"]:
        for blocker in packet["pipeline_blockers"]:
            lines.append(f"- {blocker}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Gate Summary", ""])
    source_contract = packet.get("source_contract") or {}
    lines.append(
        f"- Source contract: active `{source_contract.get('active_lane_status')}`, "
        f"forward candidate `{source_contract.get('candidate_forward_status')}`"
    )
    if packet.get("data_quality"):
        dq = packet["data_quality"]
        lines.append(
            f"- Data quality drift: `{dq.get('gap_changes')}` gap changes, "
            f"critical backlog `{dq.get('critical_backlog_after')}`, "
            f"important backlog `{dq.get('important_backlog_after')}`"
        )
    workbook = packet.get("workbook_contract") or {}
    if workbook:
        lines.append(
            f"- Workbook contract: `{workbook.get('status')}`, "
            f"scope `{workbook.get('scope')}`, "
            f"validated `{workbook.get('validated_count')}`, failures `{workbook.get('failure_count')}`"
        )
    sharepoint = packet.get("sharepoint_analysis_contract") or {}
    if sharepoint:
        lines.append(
            f"- SharePoint analysis contract: `{sharepoint.get('status')}`, "
            f"validated `{sharepoint.get('validated_count')}`, failures `{sharepoint.get('failure_count')}`"
        )
    deck_delivery = packet.get("deck_delivery_contract") or {}
    if deck_delivery:
        lines.append(
            f"- Deck delivery contract: `{deck_delivery.get('status')}`, "
            f"validated `{deck_delivery.get('validated_director_count')}` / `{deck_delivery.get('expected_director_count')}`"
        )
    deck_font = packet.get("deck_font_audit") or {}
    if deck_font:
        lines.append(
            f"- Deck font audit: `{deck_font.get('status')}`, "
            f"decks with issues `{deck_font.get('decks_with_issues')}` / `{deck_font.get('deck_count')}`"
        )
    tie_out = packet.get("tie_out") or {}
    if tie_out:
        lines.append(
            f"- Tie-out: `{tie_out.get('status')}`, "
            f"mismatches `{tie_out.get('mismatches')}`, checks `{tie_out.get('checks')}`"
        )
    obsidian = packet.get("obsidian_notes_contract") or {}
    if obsidian:
        lines.append(
            f"- Obsidian notes contract: `{obsidian.get('status')}`, "
            f"validated `{obsidian.get('validated_count')}`, failures `{obsidian.get('failure_count')}`"
        )

    lines.extend(["", "## Key Artifacts", ""])
    for key, value in (packet.get("key_artifacts") or {}).items():
        lines.append(f"- {key}: `{value or 'missing'}`")

    return "\n".join(lines) + "\n"


def write_monthly_review_release_packet_bundle(
    *,
    output_root: Path,
    packet: dict[str, Any],
) -> Path:
    run_date = str(packet["run_date"])
    run_dir = output_root / run_date
    run_dir.mkdir(parents=True, exist_ok=True)
    markdown = build_monthly_review_release_packet_markdown(packet)
    json_path = run_dir / "legacy_monthly_review_release_packet.json"
    md_path = run_dir / "summary.md"
    _save_json(json_path, packet)
    _save_text(md_path, markdown)

    latest_payload = {**packet, "packet_dir": str(run_dir)}
    _save_json(output_root / run_date / "latest.json", latest_payload)
    _save_text(output_root / run_date / "latest.md", markdown)
    _save_json(output_root / "latest.json", latest_payload)
    _save_text(output_root / "latest.md", markdown)
    return run_dir


def build_release_packet_manifest_payload(
    *,
    repo_root: Path,
    packet: dict[str, Any],
    packet_dir: Path,
    packet_diff: dict[str, Any] | None = None,
    packet_diff_dir: Path | None = None,
) -> dict[str, Any]:
    payload = {
        "status": packet.get("status"),
        "publish_ready": packet.get("publish_ready"),
        "packet_dir": _display_path(packet_dir, repo_root),
        "json_path": _display_path(
            packet_dir / "legacy_monthly_review_release_packet.json",
            repo_root,
        ),
        "summary_path": _display_path(packet_dir / "summary.md", repo_root),
    }
    if packet_diff is not None and packet_diff_dir is not None:
        payload.update(
            {
                "snapshot_diff_status": packet_diff.get("status"),
                "snapshot_diff_dir": _display_path(packet_diff_dir, repo_root),
                "snapshot_diff_json_path": _display_path(
                    packet_diff_dir / "monthly_review_release_packet_snapshot_diff.json",
                    repo_root,
                ),
                "snapshot_diff_summary_path": _display_path(
                    packet_diff_dir / "summary.md",
                    repo_root,
                ),
            }
        )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
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
        default=DEFAULT_OUTPUT_ROOT,
        help="Output root for release packet artifacts.",
    )
    args = parser.parse_args()

    run_date = str(args.date)[:10]
    manifest_path = args.manifest or (DEFAULT_LOGS_ROOT / run_date / "manifest.json")
    if not manifest_path.exists():
        raise SystemExit(f"Manifest missing: {manifest_path}")

    manifest = _load_json(manifest_path)
    packet = build_monthly_review_release_packet(
        manifest=manifest,
        manifest_path=manifest_path,
        repo_root=ROOT,
    )
    run_dir = write_monthly_review_release_packet_bundle(
        output_root=Path(args.output_root),
        packet=packet,
    )
    try:
        try:
            from build_monthly_review_release_packet_history import (
                refresh_release_packet_history,
            )
        except ModuleNotFoundError:  # pragma: no cover
            from scripts.build_monthly_review_release_packet_history import (
                refresh_release_packet_history,
            )

        refresh_release_packet_history(
            packet_root=Path(args.output_root),
            output_root=ROOT / "output" / "monthly_review_release_packet_history",
        )
    except Exception:  # pragma: no cover
        pass
    print(f"Monthly review release packet: {packet['status']}")
    print(f"Output: {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
