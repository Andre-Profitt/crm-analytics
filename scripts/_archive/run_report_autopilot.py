#!/usr/bin/env python3
"""Run a durable safe-workflow loop for queued CRM Analytics report queries."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from builder_brain_handoff_targets import (
    DEFAULT_TARGET_ORG as DEFAULT_REPORT_TARGET_ORG,
    REGISTRY_PATH as HANDOFF_TARGET_REGISTRY_PATH,
    find_registry_target as find_handoff_registry_target,
    load_registry as load_handoff_target_registry,
)

ANALYTICS_INTELLIGENCE = REPO_ROOT / "scripts" / "analytics_intelligence.py"
SALESFORCE_REPORT_EXECUTOR = REPO_ROOT / "scripts" / "salesforce_report_executor.py"


@dataclass
class WorkflowCommandResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    payload: dict[str, Any] | None

    @property
    def parsed(self) -> bool:
        return isinstance(self.payload, dict)


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def write_json(path: Path, value: Any) -> None:
    write_text(path, json.dumps(value, indent=2) + "\n")


def persist_manifest(run_dir: Path, manifest: dict[str, Any]) -> None:
    write_text(run_dir / "manifest.json", json.dumps(manifest, indent=2))


def run_report_workflow(*, query: str, output_dir: Path) -> WorkflowCommandResult:
    command = [
        sys.executable,
        str(ANALYTICS_INTELLIGENCE),
        "workflow",
        "--query",
        query,
        "--execute-safe",
        "--output-dir",
        str(output_dir),
        "--json",
    ]
    proc = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    payload: dict[str, Any] | None = None
    if proc.stdout.strip():
        try:
            raw_payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload = None
        else:
            payload = raw_payload if isinstance(raw_payload, dict) else None
    return WorkflowCommandResult(
        command=command,
        returncode=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
        payload=payload,
    )


def _as_int(value: Any) -> int | None:
    return value if isinstance(value, int) else None


def _manual_followup_reasons(
    *,
    manual_filter_intent_count: int | None,
    manual_detail_intent_count: int | None,
    omitted_sort_intent_count: int | None,
) -> list[str]:
    reasons: list[str] = []
    if isinstance(manual_filter_intent_count, int) and manual_filter_intent_count > 0:
        reasons.append(f"manual_filter_intents:{manual_filter_intent_count}")
    if isinstance(manual_detail_intent_count, int) and manual_detail_intent_count > 0:
        reasons.append(f"manual_detail_intents:{manual_detail_intent_count}")
    if isinstance(omitted_sort_intent_count, int) and omitted_sort_intent_count > 0:
        reasons.append(f"omitted_sort_intents:{omitted_sort_intent_count}")
    return reasons


def _read_json_file(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _resolve_clone_source_report_id(candidate_surface_id: str | None) -> str | None:
    if not isinstance(candidate_surface_id, str) or not candidate_surface_id:
        return None
    try:
        registry = load_handoff_target_registry(HANDOFF_TARGET_REGISTRY_PATH)
    except Exception:
        return None
    target = find_handoff_registry_target(
        registry,
        source_surface_id=candidate_surface_id,
        target_surface_type="salesforce_report",
    )
    if not isinstance(target, dict):
        return None
    target_surface_id = target.get("target_surface_id")
    return target_surface_id if isinstance(target_surface_id, str) and target_surface_id else None


def _build_filter_overrides_payload(applied_filter_overrides: Any) -> dict[str, Any]:
    if not isinstance(applied_filter_overrides, list):
        return {}
    payload: dict[str, Any] = {}
    for entry in applied_filter_overrides:
        if not isinstance(entry, dict):
            continue
        source_label = entry.get("source_label")
        value = entry.get("value")
        if not isinstance(source_label, str) or not source_label or value is None:
            continue
        operator = entry.get("operator")
        if isinstance(operator, str) and operator:
            payload[source_label] = {"value": value, "operator": operator}
        else:
            payload[source_label] = value
    return payload


def _write_manual_followup_artifact(
    *,
    path: Path,
    item: dict[str, Any],
    classification: dict[str, Any],
    apply_preview: dict[str, Any],
) -> None:
    lines = [
        f"# Manual Follow-Up: {item['key']}",
        "",
        f"- Query: {item['query']}",
        f"- Manual filter intents: `{classification.get('manual_report_filter_intent_count')}`",
        f"- Manual detail intents: `{classification.get('report_manual_detail_intent_count')}`",
        f"- Omitted sort intents: `{classification.get('report_omitted_sort_intent_count')}`",
        "",
    ]

    manual_filter_intents = apply_preview.get("manual_filter_intents") or []
    if manual_filter_intents:
        lines.extend(["## Manual Filters", ""])
        for entry in manual_filter_intents:
            if not isinstance(entry, dict):
                continue
            lines.append(
                f"- `{entry.get('source_label') or 'unknown'}`: {entry.get('guidance') or entry.get('reason') or 'Manual filter value still required.'}"
            )
        lines.append("")

    manual_detail_intents = apply_preview.get("manual_detail_intents") or []
    if manual_detail_intents:
        lines.extend(["## Manual Detail Columns", ""])
        for entry in manual_detail_intents:
            if not isinstance(entry, dict):
                continue
            lines.append(
                f"- `{entry.get('source_label') or 'unknown'}`: {entry.get('guidance') or entry.get('reason') or 'Native report field mapping still required.'}"
            )
        lines.append("")

    omitted_sort_intents = apply_preview.get("omitted_sort_intents") or []
    if omitted_sort_intents:
        lines.extend(["## Omitted Sorts", ""])
        for entry in omitted_sort_intents:
            if not isinstance(entry, dict):
                continue
            lines.append(
                f"- `{entry.get('source_label') or 'unknown'}`: {entry.get('guidance') or entry.get('reason') or 'Native sort still requires manual authoring.'}"
            )
        lines.append("")

    lines.extend(
        [
            "## Next Step",
            "",
            "- Resolve the native follow-up items above or trim them out of the packaged contract.",
            "- Re-run the report autopilot or replay the workflow after the package is clean.",
        ]
    )
    write_text(path, "\n".join(lines) + "\n")


def build_follow_on_artifacts(
    *,
    item_dir: Path,
    item: dict[str, Any],
    classification: dict[str, Any],
) -> dict[str, Any]:
    workflow_output_dir_value = classification.get("workflow_output_dir")
    if not isinstance(workflow_output_dir_value, str) or not workflow_output_dir_value:
        return {
            "promotion_runnable": False,
            "promotion_plan_artifact": None,
            "complete_command_artifact": None,
            "manual_followup_artifact": None,
            "filter_overrides_artifact": None,
        }

    workflow_output_dir = Path(workflow_output_dir_value)
    package_path = workflow_output_dir / "report_handoff" / "build_package.json"
    evaluation_path = workflow_output_dir / "evaluation" / "evaluation.json"
    apply_preview_path = workflow_output_dir / "report_apply_preview" / "salesforce_report_apply_preview.json"
    apply_preview = _read_json_file(apply_preview_path)
    if not isinstance(apply_preview, dict):
        apply_preview = {}

    target_org = (
        apply_preview.get("target_org")
        if isinstance(apply_preview.get("target_org"), str) and apply_preview.get("target_org")
        else DEFAULT_REPORT_TARGET_ORG
    )
    clone_from_report_id = _resolve_clone_source_report_id(classification.get("candidate_surface_id"))
    filter_overrides_payload = _build_filter_overrides_payload(apply_preview.get("applied_filter_overrides"))
    filter_overrides_artifact: str | None = None
    if filter_overrides_payload:
        filter_overrides_path = item_dir / "filter_overrides.json"
        write_json(filter_overrides_path, filter_overrides_payload)
        filter_overrides_artifact = str(filter_overrides_path)

    manual_followup_artifact: str | None = None
    manual_followup_required = bool(classification.get("manual_followup_required"))
    if manual_followup_required:
        manual_followup_path = item_dir / "MANUAL_FOLLOWUP.md"
        _write_manual_followup_artifact(
            path=manual_followup_path,
            item=item,
            classification=classification,
            apply_preview=apply_preview,
        )
        manual_followup_artifact = str(manual_followup_path)

    complete_command: list[str] | None = None
    complete_command_artifact: str | None = None
    promotion_runnable = False
    runnable_reasons: list[str] = []
    complete_output_dir = item_dir / "live_complete"

    if classification.get("status") == "ready":
        if not package_path.exists():
            runnable_reasons.append("missing_package_artifact")
        if not evaluation_path.exists():
            runnable_reasons.append("missing_evaluation_artifact")
        if not clone_from_report_id:
            runnable_reasons.append("missing_clone_source_report_id")
        if not target_org:
            runnable_reasons.append("missing_target_org")
        if not runnable_reasons:
            complete_command = [
                sys.executable,
                str(SALESFORCE_REPORT_EXECUTOR),
                "complete",
                "--package",
                str(package_path),
                "--clone-from-report-id",
                clone_from_report_id,
                "--autofill-live",
                "--target-org",
                target_org,
                "--evaluation",
                str(evaluation_path),
                "--output-dir",
                str(complete_output_dir),
                "--json",
            ]
            if filter_overrides_artifact:
                complete_command.extend(["--filter-overrides-json", filter_overrides_artifact])
            complete_command_path = item_dir / "complete_command.sh"
            write_text(
                complete_command_path,
                "\n".join(
                    [
                        "#!/usr/bin/env bash",
                        "set -euo pipefail",
                        f"cd {shlex.quote(str(REPO_ROOT))}",
                        shlex.join(complete_command),
                        "",
                    ]
                ),
            )
            complete_command_path.chmod(0o755)
            complete_command_artifact = str(complete_command_path)
            promotion_runnable = True
            runnable_reasons.append("all_gates_clear")
    elif manual_followup_required:
        runnable_reasons.append("manual_native_followup_required")
    else:
        runnable_reasons.append("workflow_not_ready")

    promotion_plan_path = item_dir / "promotion_plan.json"
    write_json(
        promotion_plan_path,
        {
            "artifact_type": "salesforce_report_promotion_plan",
            "item_key": item["key"],
            "query": item["query"],
            "status": classification.get("status"),
            "promotion_runnable": promotion_runnable,
            "runnable_reasons": runnable_reasons,
            "workflow_output_dir": str(workflow_output_dir),
            "package_path": str(package_path) if package_path.exists() else None,
            "evaluation_path": str(evaluation_path) if evaluation_path.exists() else None,
            "apply_preview_path": str(apply_preview_path) if apply_preview_path.exists() else None,
            "target_org": target_org,
            "clone_from_report_id": clone_from_report_id,
            "complete_output_dir": str(complete_output_dir),
            "filter_overrides_artifact": filter_overrides_artifact,
            "applied_filter_overrides": apply_preview.get("applied_filter_overrides") or [],
            "manual_followup_required": manual_followup_required,
            "manual_followup_reasons": classification.get("manual_followup_reasons") or [],
            "manual_filter_intents": apply_preview.get("manual_filter_intents") or [],
            "manual_detail_intents": apply_preview.get("manual_detail_intents") or [],
            "omitted_sort_intents": apply_preview.get("omitted_sort_intents") or [],
            "complete_command": complete_command,
            "complete_command_shell": shlex.join(complete_command) if complete_command else None,
            "review_artifact": classification.get("review_artifact"),
        },
    )

    return {
        "promotion_runnable": promotion_runnable,
        "promotion_plan_artifact": str(promotion_plan_path),
        "complete_command_artifact": complete_command_artifact,
        "manual_followup_artifact": manual_followup_artifact,
        "filter_overrides_artifact": filter_overrides_artifact,
    }


def classify_workflow_result(result: WorkflowCommandResult) -> dict[str, Any]:
    if not result.parsed:
        return {
            "status": "error",
            "payload_status": None,
            "workflow_status": None,
            "effective_surface_type": None,
            "candidate_surface_id": None,
            "evaluation_verdict": None,
            "report_apply_ready": None,
            "report_apply_strategy": None,
            "report_native_authoring_ready": None,
            "report_external_fill_requirement_count": None,
            "resolved_report_filter_override_count": None,
            "manual_report_filter_intent_count": None,
            "report_manual_detail_intent_count": None,
            "report_omitted_sort_intent_count": None,
            "manual_followup_required": False,
            "manual_followup_reasons": [],
            "planned_report_filter_overrides": [],
            "review_artifact": None,
            "workflow_output_dir": None,
            "blocked_reasons": ["invalid_json_output"],
            "step_messages": [],
        }

    payload = result.payload or {}
    workflow = payload.get("workflow") if isinstance(payload.get("workflow"), dict) else {}
    summary = workflow.get("summary") if isinstance(workflow.get("summary"), dict) else {}
    executed_steps = summary.get("executed_steps") if isinstance(summary.get("executed_steps"), list) else []
    step_messages: list[str] = []
    for step in executed_steps:
        if not isinstance(step, dict):
            continue
        for code in step.get("messages") or []:
            if isinstance(code, str) and code not in step_messages:
                step_messages.append(code)

    blocked_reasons: list[str] = []
    evaluation_verdict = summary.get("evaluation_verdict")
    if evaluation_verdict != "pass":
        blocked_reasons.append(f"evaluation_verdict:{evaluation_verdict or 'missing'}")
    report_apply_ready = summary.get("report_apply_ready")
    if report_apply_ready is not True:
        blocked_reasons.append("report_apply_not_ready")
    external_fill_requirement_count = summary.get("report_external_fill_requirement_count")
    if isinstance(external_fill_requirement_count, int) and external_fill_requirement_count > 0:
        blocked_reasons.append(f"external_fill_requirements:{external_fill_requirement_count}")

    manual_filter_intent_count = _as_int(summary.get("manual_report_filter_intent_count"))
    manual_detail_intent_count = _as_int(summary.get("report_manual_detail_intent_count"))
    omitted_sort_intent_count = _as_int(summary.get("report_omitted_sort_intent_count"))
    manual_followup_reasons = _manual_followup_reasons(
        manual_filter_intent_count=manual_filter_intent_count,
        manual_detail_intent_count=manual_detail_intent_count,
        omitted_sort_intent_count=omitted_sort_intent_count,
    )

    payload_status = payload.get("status")
    if payload_status == "error":
        status = "error"
    elif report_apply_ready is True and evaluation_verdict == "pass":
        status = "ready_manual_followup" if manual_followup_reasons else "ready"
    else:
        status = "blocked"

    return {
        "status": status,
        "payload_status": payload_status,
        "workflow_status": summary.get("workflow_status"),
        "effective_surface_type": summary.get("effective_surface_type"),
        "candidate_surface_id": summary.get("candidate_surface_id"),
        "evaluation_verdict": evaluation_verdict,
        "report_apply_ready": report_apply_ready,
        "report_apply_strategy": summary.get("report_apply_strategy"),
        "report_native_authoring_ready": summary.get("report_native_authoring_ready"),
        "report_external_fill_requirement_count": external_fill_requirement_count,
        "resolved_report_filter_override_count": summary.get("resolved_report_filter_override_count"),
        "manual_report_filter_intent_count": manual_filter_intent_count,
        "report_manual_detail_intent_count": manual_detail_intent_count,
        "report_omitted_sort_intent_count": omitted_sort_intent_count,
        "manual_followup_required": bool(manual_followup_reasons),
        "manual_followup_reasons": manual_followup_reasons,
        "planned_report_filter_overrides": summary.get("planned_report_filter_overrides") or [],
        "review_artifact": summary.get("review_artifact"),
        "workflow_output_dir": workflow.get("output_dir"),
        "blocked_reasons": blocked_reasons,
        "step_messages": step_messages,
    }


def write_item_summary(path: Path, *, item: dict[str, Any], classification: dict[str, Any]) -> None:
    lines = [
        f"# {item['key']}",
        "",
        f"- Query: {item['query']}",
        f"- Status: `{classification.get('status') or 'unknown'}`",
        f"- Workflow status: `{classification.get('workflow_status') or 'unknown'}`",
        f"- Effective surface: `{classification.get('effective_surface_type') or 'unknown'}`",
        f"- Evaluation verdict: `{classification.get('evaluation_verdict') or 'unknown'}`",
        f"- Report apply ready: `{classification.get('report_apply_ready')}`",
        f"- Report apply strategy: `{classification.get('report_apply_strategy') or 'unknown'}`",
        f"- Native authoring ready: `{classification.get('report_native_authoring_ready')}`",
        f"- External fill requirements: `{classification.get('report_external_fill_requirement_count')}`",
        f"- Resolved filter overrides: `{classification.get('resolved_report_filter_override_count')}`",
        f"- Remaining manual filter intents: `{classification.get('manual_report_filter_intent_count')}`",
        f"- Remaining manual detail intents: `{classification.get('report_manual_detail_intent_count')}`",
        f"- Remaining omitted sort intents: `{classification.get('report_omitted_sort_intent_count')}`",
    ]
    if item.get("notes"):
        lines.append(f"- Notes: {item['notes']}")
    if classification.get("review_artifact"):
        lines.append(f"- Workflow review: `{classification['review_artifact']}`")
    if classification.get("workflow_output_dir"):
        lines.append(f"- Workflow output dir: `{classification['workflow_output_dir']}`")
    if classification.get("promotion_plan_artifact"):
        lines.append(f"- Promotion plan: `{classification['promotion_plan_artifact']}`")
    if classification.get("complete_command_artifact"):
        lines.append(f"- Live complete command: `{classification['complete_command_artifact']}`")
    if classification.get("manual_followup_artifact"):
        lines.append(f"- Manual follow-up guide: `{classification['manual_followup_artifact']}`")
    if classification.get("filter_overrides_artifact"):
        lines.append(f"- Filter overrides artifact: `{classification['filter_overrides_artifact']}`")
    planned_overrides = classification.get("planned_report_filter_overrides") or []
    if planned_overrides:
        rendered = ", ".join(
            f"{entry['source_label']}={entry['value']}"
            for entry in planned_overrides
            if isinstance(entry, dict)
            and isinstance(entry.get("source_label"), str)
            and isinstance(entry.get("value"), str)
        )
        if rendered:
            lines.append(f"- Planned filter overrides: `{rendered}`")
    manual_followup_reasons = classification.get("manual_followup_reasons") or []
    if manual_followup_reasons:
        lines.append(f"- Manual follow-up required: `{', '.join(manual_followup_reasons)}`")
    blocked_reasons = classification.get("blocked_reasons") or []
    if blocked_reasons:
        lines.extend(["", "## Blockers"])
        lines.extend(f"- `{reason}`" for reason in blocked_reasons)
    if manual_followup_reasons:
        lines.extend(["", "## Manual Follow-Up"])
        lines.extend(f"- `{reason}`" for reason in manual_followup_reasons)
    step_messages = classification.get("step_messages") or []
    if step_messages:
        lines.extend(["", "## Step Messages"])
        lines.extend(f"- `{message}`" for message in step_messages)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--queue",
        default="config/report_autopilot_queue.json",
        help="Queue config JSON path.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Override run output directory.",
    )
    parser.add_argument(
        "--resume-run",
        default=None,
        help="Resume an existing run directory and skip items with recorded results.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Optional maximum number of queue items to process.",
    )
    args = parser.parse_args()

    queue_path = (REPO_ROOT / args.queue).resolve() if not Path(args.queue).is_absolute() else Path(args.queue)
    queue_config = json.loads(queue_path.read_text(encoding="utf-8"))
    enabled_items = [item for item in queue_config.get("items", []) if item.get("enabled", True)]
    enabled_items.sort(key=lambda item: item.get("priority", 999))
    if args.max_items is not None:
        enabled_items = enabled_items[: args.max_items]

    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    if args.resume_run:
        run_dir = Path(args.resume_run).resolve()
    else:
        run_dir = (
            Path(args.output_dir).resolve()
            if args.output_dir
            else (REPO_ROOT / "output" / "report_autopilot" / "runs" / timestamp).resolve()
        )
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = run_dir / "manifest.json"
    if args.resume_run and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = {
            "queue": str(queue_path),
            "run_dir": str(run_dir),
            "started_at": timestamp,
            "items": [],
        }
    persist_manifest(run_dir, manifest)
    print(f"[report-autopilot] run_dir={run_dir}", flush=True)

    completed_keys = {
        item["key"]
        for item in manifest.get("items", [])
        if isinstance(item, dict) and isinstance(item.get("status"), str)
    }

    for item in enabled_items:
        key = item["key"]
        if key in completed_keys:
            print(f"[report-autopilot] skip_completed key={key}", flush=True)
            continue

        item_dir = run_dir / key
        item_dir.mkdir(parents=True, exist_ok=True)
        workflow_output_dir = item_dir / "workflow"
        print(f"[report-autopilot] start key={key}", flush=True)

        result = run_report_workflow(query=item["query"], output_dir=workflow_output_dir)
        classification = classify_workflow_result(result)
        write_text(item_dir / "workflow.stdout.log", result.stdout)
        write_text(item_dir / "workflow.stderr.log", result.stderr)
        classification.update(build_follow_on_artifacts(item_dir=item_dir, item=item, classification=classification))
        write_item_summary(item_dir / "README.md", item=item, classification=classification)

        item_manifest = {
            "key": key,
            "query": item["query"],
            "status": classification["status"],
            "payload_status": classification["payload_status"],
            "workflow_status": classification["workflow_status"],
            "returncode": result.returncode,
            "effective_surface_type": classification["effective_surface_type"],
            "candidate_surface_id": classification["candidate_surface_id"],
            "evaluation_verdict": classification["evaluation_verdict"],
            "report_apply_ready": classification["report_apply_ready"],
            "report_apply_strategy": classification["report_apply_strategy"],
            "report_native_authoring_ready": classification["report_native_authoring_ready"],
            "report_external_fill_requirement_count": classification["report_external_fill_requirement_count"],
            "resolved_report_filter_override_count": classification["resolved_report_filter_override_count"],
            "manual_report_filter_intent_count": classification["manual_report_filter_intent_count"],
            "report_manual_detail_intent_count": classification["report_manual_detail_intent_count"],
            "report_omitted_sort_intent_count": classification["report_omitted_sort_intent_count"],
            "manual_followup_required": classification["manual_followup_required"],
            "manual_followup_reasons": classification["manual_followup_reasons"],
            "planned_report_filter_overrides": classification["planned_report_filter_overrides"],
            "review_artifact": classification["review_artifact"],
            "workflow_output_dir": classification["workflow_output_dir"],
            "promotion_runnable": classification["promotion_runnable"],
            "promotion_plan_artifact": classification["promotion_plan_artifact"],
            "complete_command_artifact": classification["complete_command_artifact"],
            "manual_followup_artifact": classification["manual_followup_artifact"],
            "filter_overrides_artifact": classification["filter_overrides_artifact"],
            "blocked_reasons": classification["blocked_reasons"],
        }
        manifest["items"] = [entry for entry in manifest["items"] if entry.get("key") != key]
        manifest["items"].append(item_manifest)
        persist_manifest(run_dir, manifest)
        print(
            f"[report-autopilot] done key={key} status={classification['status']} apply_ready={classification['report_apply_ready']}",
            flush=True,
        )

    ready_count = sum(1 for item in manifest["items"] if item.get("status") == "ready")
    ready_manual_followup_count = sum(
        1 for item in manifest["items"] if item.get("status") == "ready_manual_followup"
    )
    blocked_count = sum(1 for item in manifest["items"] if item.get("status") == "blocked")
    error_count = sum(1 for item in manifest["items"] if item.get("status") == "error")

    lines = [
        f"# Report Autopilot Run ({timestamp})",
        "",
        f"- Queue: `{queue_path}`",
        f"- Run dir: `{run_dir}`",
        f"- Ready items: `{ready_count}`",
        f"- Ready with manual follow-up items: `{ready_manual_followup_count}`",
        f"- Blocked items: `{blocked_count}`",
        f"- Error items: `{error_count}`",
        "",
        "## Items",
        "",
    ]
    for item in manifest["items"]:
        lines.append(
            textwrap.dedent(
                f"""\
                ### {item['key']}
                - Status: `{item.get('status') or 'unknown'}`
                - Evaluation verdict: `{item.get('evaluation_verdict') or 'unknown'}`
                - Report apply ready: `{item.get('report_apply_ready')}`
                - External fill requirements: `{item.get('report_external_fill_requirement_count')}`
                - Manual detail intents: `{item.get('report_manual_detail_intent_count')}`
                - Omitted sort intents: `{item.get('report_omitted_sort_intent_count')}`
                - Workflow review: `{item.get('review_artifact') or '-'}`
                """
            ).strip()
        )
        blocked_reasons = item.get("blocked_reasons") or []
        if blocked_reasons:
            lines.append(f"- Blockers: `{', '.join(blocked_reasons)}`")
        manual_followup_reasons = item.get("manual_followup_reasons") or []
        if manual_followup_reasons:
            lines.append(f"- Manual follow-up: `{', '.join(manual_followup_reasons)}`")
        if item.get("complete_command_artifact"):
            lines.append(f"- Live complete command: `{item.get('complete_command_artifact')}`")
        elif item.get("promotion_plan_artifact"):
            lines.append(f"- Promotion plan: `{item.get('promotion_plan_artifact')}`")
        lines.append("")
    write_text(run_dir / "RUN_SUMMARY.md", "\n".join(lines))

    print("[report-autopilot] complete", flush=True)
    print(run_dir)
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
