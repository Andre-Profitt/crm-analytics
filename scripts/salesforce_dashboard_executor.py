#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


API_VERSION = "66.0"
ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
AUTOFILL_VOCAB_PATH = ROOT / "config" / "native_surface_autofill_vocab.json"
DEFAULT_DASHBOARD_FILTER_AUTOMATION_SCRIPT = ROOT / "scripts" / "salesforce_dashboard_filter_automation.py"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import executor_policy
import native_surface_browser
import native_surface_io
import native_surface_policy


_ORG_SESSION_CACHE = native_surface_io._ORG_SESSION_CACHE


def make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def make_result(
    *,
    status: str,
    command: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    command_class: str = "read_only",
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "salesforce_dashboard_executor",
        "lane": "native_surface_authoring",
        "command_class": command_class,
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return executor_policy.apply_policy_exceptions(payload)


def load_build_package(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _load_json_file(path: Path) -> dict[str, Any]:
    return native_surface_io.load_json_file(path)


def load_autofill_vocab() -> dict[str, Any]:
    if not AUTOFILL_VOCAB_PATH.exists():
        return {
            "version": 1,
            "dashboard_component_aliases": [],
            "dashboard_component_property_aliases": [],
            "dashboard_filter_aliases": [],
            "dashboard_filter_templates": [],
            "dashboard_baseline_aliases": [],
        }
    return _load_json_file(AUTOFILL_VOCAB_PATH)


def _format_sf_error(stdout: str, stderr: str, *, path: str) -> str:
    return native_surface_io.format_sf_error(stdout, stderr, path=path)


def _normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _label_tokens(value: str) -> set[str]:
    return {token for token in re.split(r"[^a-z0-9]+", value.lower()) if token}


def _resolve_component_report_id(source_label: str, candidates: dict[str, str]) -> str | None:
    normalized = _normalize_label(source_label)
    exact_matches = [report_id for label, report_id in candidates.items() if _normalize_label(label) == normalized]
    if len(exact_matches) == 1:
        return exact_matches[0]

    fallback_matches: list[str] = []
    for label, report_id in candidates.items():
        label_key = _normalize_label(label)
        if label_key == normalized:
            continue
        if (
            label_key.endswith(normalized)
            or label_key.startswith(normalized)
            or normalized.endswith(label_key)
            or normalized.startswith(label_key)
            or normalized in label_key
            or label_key in normalized
        ) and report_id not in fallback_matches:
            fallback_matches.append(report_id)
    if len(fallback_matches) == 1:
        return fallback_matches[0]

    source_tokens = _label_tokens(source_label)
    if source_tokens:
        scored_matches: list[tuple[int, str]] = []
        for label, report_id in candidates.items():
            candidate_tokens = _label_tokens(label)
            overlap = len(source_tokens & candidate_tokens)
            if overlap >= 2:
                scored_matches.append((overlap, report_id))
        if scored_matches:
            scored_matches.sort(key=lambda item: (-item[0], item[1]))
            if len(scored_matches) == 1 or scored_matches[0][0] > scored_matches[1][0]:
                return scored_matches[0][1]
    return None


def _resolve_component_alias(
    source_label: str,
    *,
    package_developer_name: str | None,
    autofill_vocab: dict[str, Any] | None = None,
) -> tuple[str | None, str | None]:
    if not autofill_vocab:
        return None, None
    normalized = _normalize_label(source_label)
    for alias in autofill_vocab.get("dashboard_component_aliases") or []:
        if _normalize_label(str(alias.get("source_label", ""))) != normalized:
            continue
        alias_package = alias.get("package_developer_name")
        if alias_package and package_developer_name and alias_package != package_developer_name:
            continue
        report_id = alias.get("report_id")
        if isinstance(report_id, str) and report_id:
            report_label = alias.get("report_label") or report_id
            return report_id, f"repo_vocab.component_alias:{source_label}->{report_label}"
    return None, None


def _resolve_component_property_alias(
    source_label: str,
    *,
    report_id: str | None,
    package_developer_name: str | None,
    autofill_vocab: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    if not autofill_vocab:
        return None, None
    normalized = _normalize_label(source_label)
    for alias in autofill_vocab.get("dashboard_component_property_aliases") or []:
        if _normalize_label(str(alias.get("source_label", ""))) != normalized:
            continue
        alias_package = alias.get("package_developer_name")
        if alias_package and package_developer_name and alias_package != package_developer_name:
            continue
        alias_report_id = alias.get("report_id")
        if alias_report_id and report_id and alias_report_id != report_id:
            continue
        properties = alias.get("properties")
        if isinstance(properties, dict) and properties:
            component_label = alias.get("component_label") or source_label
            return copy.deepcopy(properties), f"repo_vocab.component_properties:{component_label}"
    return None, None


def _resolve_dashboard_baseline_alias(
    *,
    package_developer_name: str | None,
    autofill_vocab: dict[str, Any] | None = None,
) -> tuple[str | None, dict[str, str] | None]:
    if not autofill_vocab or not package_developer_name:
        return None, None
    for alias in autofill_vocab.get("dashboard_baseline_aliases") or []:
        alias_package = alias.get("package_developer_name")
        if alias_package != package_developer_name:
            continue
        dashboard_id = alias.get("dashboard_id")
        if not isinstance(dashboard_id, str) or not dashboard_id:
            continue
        metadata = {
            "dashboard_id": dashboard_id,
            "dashboard_label": str(alias.get("dashboard_label") or dashboard_id),
            "source": str(alias.get("source") or "repo_vocab.dashboard_baseline"),
            "confidence": str(alias.get("confidence") or "medium"),
        }
        return dashboard_id, metadata
    return None, None


def _resolve_dashboard_filter_alias(
    source_label: str,
    *,
    package_developer_name: str | None,
    autofill_vocab: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    if not autofill_vocab:
        return None, None
    normalized = _normalize_label(source_label)
    for alias in autofill_vocab.get("dashboard_filter_aliases") or []:
        if _normalize_label(str(alias.get("source_label", ""))) != normalized:
            continue
        alias_package = alias.get("package_developer_name")
        if alias_package and package_developer_name and alias_package != package_developer_name:
            continue
        filter_payload = alias.get("filter")
        if isinstance(filter_payload, dict) and filter_payload:
            filter_name = filter_payload.get("name") or source_label
            return copy.deepcopy(filter_payload), f"repo_vocab.dashboard_filter:{source_label}->{filter_name}"
    return None, None


def _resolve_dashboard_filter_template(
    source_label: str,
    *,
    package_developer_name: str | None,
    autofill_vocab: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    if not autofill_vocab:
        return None, None
    normalized = _normalize_label(source_label)
    for template in autofill_vocab.get("dashboard_filter_templates") or []:
        if _normalize_label(str(template.get("source_label", ""))) != normalized:
            continue
        alias_package = template.get("package_developer_name")
        if alias_package and package_developer_name and alias_package != package_developer_name:
            continue
        filter_payload = template.get("filter")
        if not isinstance(filter_payload, dict) or not filter_payload:
            continue
        filter_name = filter_payload.get("name") or source_label
        return copy.deepcopy(template), f"repo_vocab.dashboard_filter_template:{source_label}->{filter_name}"
    return None, None


def _run_rest_request(
    path: str,
    *,
    target_org: str | None,
    method: str = "GET",
    body: Any | None = None,
) -> dict[str, Any]:
    if target_org:
        org_session = _get_org_session(target_org)
        if org_session:
            payload = _run_direct_rest_request(
                path,
                org_session=org_session,
                method=method,
                body=body,
            )
            if not isinstance(payload, dict):
                raise RuntimeError(f"unexpected non-object payload returned for {path}")
            return payload
    payload = native_surface_io.run_rest_request(
        path,
        root=ROOT,
        target_org=target_org,
        method=method,
        body=body,
        expect_dict=True,
    )
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected non-object payload returned for {path}")
    return payload


def _fetch_rest_json(path: str, *, target_org: str | None) -> dict[str, Any]:
    return native_surface_io.fetch_rest_json(path, root=ROOT, target_org=target_org)


def _is_missing_dashboard_error(exc: Exception) -> bool:
    message = str(exc).upper()
    return "NOT_FOUND" in message or "ENTITY_IS_DELETED" in message


def _wait_for_dashboard_deletion(
    *,
    dashboard_id: str,
    target_org: str,
    verify_attempts: int,
    verify_delay_seconds: float,
) -> dict[str, Any]:
    dashboard_path = f"/services/data/v{API_VERSION}/analytics/dashboards/{dashboard_id}"
    attempt_results: list[dict[str, Any]] = []

    for attempt in range(1, max(1, verify_attempts) + 1):
        try:
            dashboard_payload = _fetch_rest_json(dashboard_path, target_org=target_org)
            attempt_results.append(
                {
                    "attempt": attempt,
                    "status": "still_exists",
                    "name": dashboard_payload.get("name"),
                }
            )
        except Exception as exc:
            if _is_missing_dashboard_error(exc):
                attempt_results.append(
                    {
                        "attempt": attempt,
                        "status": "deleted",
                        "detail": str(exc),
                    }
                )
                return {
                    "deleted": True,
                    "attempt_count": attempt,
                    "attempt_results": attempt_results,
                }
            attempt_results.append(
                {
                    "attempt": attempt,
                    "status": "error",
                    "detail": str(exc),
                }
            )
            return {
                "deleted": False,
                "attempt_count": attempt,
                "attempt_results": attempt_results,
                "error": str(exc),
            }
        if attempt < max(1, verify_attempts) and verify_delay_seconds > 0:
            time.sleep(verify_delay_seconds)

    return {
        "deleted": False,
        "attempt_count": max(1, verify_attempts),
        "attempt_results": attempt_results,
        "error": "Dashboard still returned a payload after the delete request.",
    }


def _run_rest_request_any(
    path: str,
    *,
    target_org: str | None,
    method: str = "GET",
    body: Any | None = None,
) -> Any:
    if target_org:
        org_session = _get_org_session(target_org)
        if org_session:
            return _run_direct_rest_request(
                path,
                org_session=org_session,
                method=method,
                body=body,
            )
    return native_surface_io.run_rest_request(
        path,
        root=ROOT,
        target_org=target_org,
        method=method,
        body=body,
        expect_dict=False,
    )


def _get_org_session(target_org: str) -> dict[str, str] | None:
    return native_surface_io.get_org_session(target_org, root=ROOT)


def _run_direct_rest_request(
    path: str,
    *,
    org_session: dict[str, str],
    method: str = "GET",
    body: Any | None = None,
) -> Any:
    return native_surface_io.run_direct_rest_request(
        path,
        org_session=org_session,
        method=method,
        body=body,
    )


def _run_dashboard_filter_options_analysis(
    *,
    target_org: str | None,
    filter_columns: list[dict[str, str]],
    options: list[dict[str, Any]],
) -> dict[str, Any]:
    if not target_org:
        raise ValueError("target_org is required to run dashboard filter options analysis.")
    return _run_rest_request(
        f"/services/data/v{API_VERSION}/analytics/dashboards/filteroptionsanalysis",
        target_org=target_org,
        method="POST",
        body={
            "filterColumns": filter_columns,
            "options": options,
        },
    )


def load_dashboard_metadata(
    *,
    dashboard_id: str | None,
    target_org: str | None,
    baseline_dashboard_json: Path | None,
) -> dict[str, Any] | None:
    if baseline_dashboard_json is not None:
        return _load_json_file(baseline_dashboard_json)
    if dashboard_id and target_org:
        return _fetch_rest_json(
            f"/services/data/v{API_VERSION}/analytics/dashboards/{dashboard_id}",
            target_org=target_org,
        )
    return None


def _dashboard_metadata_object(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("dashboardMetadata")
    if isinstance(metadata, dict):
        return metadata
    return payload


def _normalize_dashboard_filter(filter_payload: dict[str, Any]) -> dict[str, Any]:
    options = filter_payload.get("options") or []
    normalized_options = []
    for option in options:
        if not isinstance(option, dict):
            continue
        normalized_option = {}
        for key in ("alias", "operation", "startValue", "endValue", "value"):
            if key in option:
                normalized_option[key] = option.get(key)
        normalized_options.append(normalized_option)
    return {
        "name": filter_payload.get("name"),
        "options": normalized_options,
        "selectedOption": filter_payload.get("selectedOption"),
    }


def _normalized_filter_options(filter_payload: dict[str, Any]) -> list[tuple[str, str, str, str, str]]:
    normalized = _normalize_dashboard_filter(filter_payload)
    options = []
    for option in normalized.get("options") or []:
        if not isinstance(option, dict):
            continue
        options.append(
            (
                str(option.get("alias") or ""),
                str(option.get("operation") or ""),
                str(option.get("startValue") or ""),
                str(option.get("endValue") or ""),
                str(option.get("value") or ""),
            )
        )
    return sorted(options)


def _sanitize_dashboard_filter_payload(filter_payload: dict[str, Any]) -> dict[str, Any]:
    options = filter_payload.get("options") or []
    sanitized_options = []
    for option in options:
        if not isinstance(option, dict):
            continue
        sanitized_option = {}
        for key in ("alias", "operation", "startValue", "endValue", "value"):
            if key in option:
                sanitized_option[key] = option.get(key)
        sanitized_options.append(sanitized_option)
    return {
        "name": filter_payload.get("name"),
        "options": sanitized_options,
        "selectedOption": filter_payload.get("selectedOption"),
    }


def _load_optional_json_file(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return _load_json_file(path)


def _load_evaluation_gate(path: Path) -> dict[str, Any]:
    return executor_policy.load_evaluation_gate(path)


def _append_evaluation_bypass_artifact(
    *,
    output_dir: Path | None,
    artifacts: list[dict[str, str]],
    command: str,
    target_org: str | None,
    evaluation_gate: dict[str, Any] | None,
    summary: dict[str, Any],
) -> None:
    executor_policy.append_evaluation_bypass_artifact(
        output_dir=output_dir,
        artifacts=artifacts,
        command=command,
        target_org=target_org,
        evaluation_gate=evaluation_gate,
        summary=summary,
    )


def _attach_memory_record(
    *,
    result: dict[str, Any],
    planning_context: dict[str, Any] | None,
    command: str,
    evaluation_gate: dict[str, Any] | None,
    extra_tags: list[str] | None = None,
) -> dict[str, Any]:
    return executor_policy.attach_memory_record(
        result=result,
        planning_context=planning_context,
        command=command,
        evaluation_gate=evaluation_gate,
        script_path="scripts/salesforce_dashboard_executor.py",
        make_message=make_message,
        extra_tags=extra_tags,
    )


def _derive_dashboard_memory_context(
    *,
    build_package: dict[str, Any],
    planning_context: dict[str, Any] | None,
    evaluation_gate: dict[str, Any] | None,
    output_dir: Path | None,
    package_path: Path,
    command: str,
) -> dict[str, Any] | None:
    return native_surface_policy.derive_native_surface_memory_context(
        build_package=build_package,
        planning_context=planning_context,
        evaluation_gate=evaluation_gate,
        output_dir=output_dir,
        package_path=package_path,
        command=command,
        default_goal_prefix="execute salesforce dashboard",
        operation="mutate_dashboard",
    )


def _resolve_dashboard_evaluation_gate(
    *,
    build_package: dict[str, Any],
    evaluation_path: Path | None,
    require_pass: bool,
    allow_missing: bool,
) -> tuple[dict[str, Any] | None, list[dict[str, str]], dict[str, str] | None]:
    return native_surface_policy.resolve_native_surface_evaluation_gate(
        build_package=build_package,
        evaluation_path=evaluation_path,
        require_pass=require_pass,
        allow_missing=allow_missing,
        make_message=make_message,
        surface_name="dashboard",
    )


def _artifact_path_from_result(result: dict[str, Any], artifact_type: str) -> str | None:
    for artifact in result.get("artifacts") or []:
        if not isinstance(artifact, dict):
            continue
        if artifact.get("type") != artifact_type:
            continue
        path = artifact.get("path")
        if isinstance(path, str) and path:
            return path
    return None


def _run_json_command(command: list[str]) -> tuple[int, dict[str, Any]]:
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    stdout = result.stdout.strip()
    if not stdout:
        raise RuntimeError(result.stderr.strip() or f"Command produced no JSON output: {' '.join(command)}")
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Unable to parse JSON output from command {' '.join(command)}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object from command {' '.join(command)}.")
    return result.returncode, payload


def _build_dashboard_manual_filter_authoring_artifact(
    *,
    build_package: dict[str, Any],
    preview: dict[str, Any],
    autofill_summary: dict[str, Any] | None,
    target_org: str | None,
    applied_dashboard_id: str | None = None,
    candidate_clone_baselines: list[dict[str, Any]] | None = None,
    baseline_strategy: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    manual_filter_intents = preview.get("manual_filter_intents") or []
    if not manual_filter_intents:
        return None

    surface_contract = build_package.get("surface_contract") or {}
    baseline = preview.get("resolved_clone_baseline")
    baseline_filter_count = None
    if isinstance(autofill_summary, dict):
        baseline_filter_count = ((autofill_summary.get("summary") or {}).get("baseline_filter_count"))

    return {
        "artifact_type": "salesforce_dashboard_manual_filter_authoring",
        "package_developer_name": surface_contract.get("surface_type") == "salesforce_dashboard"
        and _suggested_dashboard_developer_name(build_package)
        or None,
        "suggested_dashboard_label": _suggested_dashboard_label(build_package),
        "target_org": target_org,
        "target_dashboard_id": applied_dashboard_id,
        "resolved_clone_baseline": baseline,
        "baseline_filter_count": baseline_filter_count,
        "candidate_clone_baselines": candidate_clone_baselines or [],
        "baseline_strategy": baseline_strategy,
        "recommended_steps": [
            "Open the cloned/authored dashboard in Salesforce dashboard edit mode.",
            "Create each proposed native dashboard filter manually using the validated proposal contract below.",
            "If reusable native filters matter, evaluate the suggested alternative clone baselines before rebuilding.",
            "Re-run salesforce_dashboard_executor.py verify after authoring the filters to confirm component/report integrity still matches the packaged contract.",
        ],
        "filter_intents": [
            {
                "source_label": item.get("source_label"),
                "proposal_source": item.get("proposal_source"),
                "compatibility_status": item.get("compatibility_status"),
                "compatibility_message": item.get("compatibility_message"),
                "reason": item.get("reason"),
                "guidance": item.get("guidance"),
                "proposed_filter": item.get("proposed_filter"),
            }
            for item in manual_filter_intents
        ],
    }


def _build_dashboard_manual_filter_playbook(
    *,
    build_package: dict[str, Any],
    manual_filter_authoring_artifact: dict[str, Any],
) -> dict[str, Any]:
    filter_intents = manual_filter_authoring_artifact.get("filter_intents") or []
    playbook_filters = []
    for index, intent in enumerate(filter_intents, start=1):
        proposed_filter = intent.get("proposed_filter") or {}
        filter_name = proposed_filter.get("name") or intent.get("source_label")
        options = proposed_filter.get("options") or []
        playbook_filters.append(
            {
                "order": index,
                "source_label": intent.get("source_label"),
                "display_name": filter_name,
                "field_picker_terms": [item for item in [filter_name, intent.get("source_label")] if item],
                "option_count": len(options),
                "steps": [
                    {
                        "action": "open_add_filter",
                        "field": filter_name,
                        "display_name": filter_name,
                    },
                    *[
                        {
                            "action": "add_filter_value",
                            "alias": option.get("alias"),
                            "operation": option.get("operation"),
                            "start_value": option.get("startValue"),
                            "end_value": option.get("endValue"),
                            "value": option.get("value"),
                        }
                        for option in options
                        if isinstance(option, dict)
                    ],
                    {
                        "action": "save_filter",
                        "filter_name": filter_name,
                    },
                ],
                "verification_expectation": {
                    "filter_name": filter_name,
                    "option_aliases": [
                        option.get("alias")
                        for option in options
                        if isinstance(option, dict) and option.get("alias")
                    ],
                },
            }
        )

    return {
        "artifact_type": "salesforce_dashboard_filter_playbook",
        "package_developer_name": manual_filter_authoring_artifact.get("package_developer_name"),
        "suggested_dashboard_label": manual_filter_authoring_artifact.get("suggested_dashboard_label")
        or _suggested_dashboard_label(build_package),
        "target_org": manual_filter_authoring_artifact.get("target_org"),
        "target_dashboard_id": manual_filter_authoring_artifact.get("target_dashboard_id"),
        "resolved_clone_baseline": manual_filter_authoring_artifact.get("resolved_clone_baseline"),
        "baseline_strategy": manual_filter_authoring_artifact.get("baseline_strategy"),
        "recommended_steps": [
            "Open the dashboard in edit mode.",
            "Author the filters below in the listed order.",
            "Save the dashboard, then exit edit mode.",
            "Run salesforce_dashboard_executor.py verify with the same manual filter artifact.",
        ],
        "filters": playbook_filters,
    }


def _build_dashboard_manual_filter_automation_plan(
    *,
    manual_filter_authoring_artifact: dict[str, Any],
    manual_filter_playbook: dict[str, Any],
) -> dict[str, Any]:
    target_dashboard_id = manual_filter_authoring_artifact.get("target_dashboard_id")
    dashboard_id_token = target_dashboard_id or "__FILL_TARGET_DASHBOARD_ID__"
    target_org = manual_filter_authoring_artifact.get("target_org") or "__FILL_TARGET_ORG__"
    edit_route = f"/lightning/r/Dashboard/{dashboard_id_token}/edit"
    manual_filter_artifact_path = "salesforce_dashboard_manual_filter_authoring.json"

    return {
        "artifact_type": "salesforce_dashboard_filter_automation_plan",
        "executor": "playwright_cli",
        "target_org": target_org,
        "target_dashboard_id": target_dashboard_id,
        "relative_edit_route": edit_route,
        "relative_edit_route_template": "/lightning/r/Dashboard/{dashboard_id}/edit",
        "session_requirements": {
            "headed": True,
            "snapshot_before_refs": True,
            "resnapshot_after_dom_change": True,
        },
        "preflight_actions": [
            {
                "order": 1,
                "action": "goto_edit_route",
                "relative_url": edit_route,
                "dashboard_id": dashboard_id_token,
            },
            {
                "order": 2,
                "action": "snapshot",
                "reason": "Capture dashboard editor refs before filter authoring.",
            },
            {
                "order": 3,
                "action": "assert_dashboard_editor",
                "success_signals": ["Add filter", "Save", "Done"],
            },
        ],
        "filter_actions": [
            {
                "order": filter_item.get("order"),
                "action": "author_dashboard_filter",
                "source_label": filter_item.get("source_label"),
                "filter_name": filter_item.get("display_name"),
                "field_picker_terms": filter_item.get("field_picker_terms", []),
                "options": [
                    {
                        "alias": step.get("alias"),
                        "operation": step.get("operation"),
                        "start_value": step.get("start_value"),
                        "end_value": step.get("end_value"),
                        "value": step.get("value"),
                    }
                    for step in filter_item.get("steps", [])
                    if step.get("action") == "add_filter_value"
                ],
                "save_after": True,
                "verification_expectation": filter_item.get("verification_expectation", {}),
            }
            for filter_item in manual_filter_playbook.get("filters", [])
        ],
        "post_actions": [
            {
                "order": 1,
                "action": "save_dashboard",
                "button_label": "Save",
            },
            {
                "order": 2,
                "action": "exit_dashboard_editor",
                "button_label": "Done",
            },
            {
                "order": 3,
                "action": "run_verify_cli",
                "command_template": [
                    "python3",
                    "scripts/salesforce_dashboard_executor.py",
                    "verify",
                    "--package",
                    "__BUILD_PACKAGE_JSON__",
                    "--dashboard-id",
                    dashboard_id_token,
                    "--autofill-live",
                    "--target-org",
                    target_org,
                    "--manual-filter-authoring-json",
                    manual_filter_artifact_path,
                    "--output-dir",
                    "__VERIFY_OUTPUT_DIR__",
                    "--json",
                ],
            },
        ],
    }


def _render_dashboard_manual_filter_playbook_markdown(playbook: dict[str, Any]) -> str:
    lines = [
        "# Salesforce Dashboard Filter Playbook",
        "",
        f"- Dashboard: {playbook.get('suggested_dashboard_label') or '-'}",
        f"- Dashboard Id: {playbook.get('target_dashboard_id') or '-'}",
        f"- Target Org: {playbook.get('target_org') or '-'}",
        "",
    ]
    baseline_strategy = playbook.get("baseline_strategy") or {}
    if baseline_strategy.get("summary"):
        lines.extend(
            [
                "## Baseline Strategy",
                "",
                f"{baseline_strategy['summary']}",
                "",
            ]
        )

    lines.extend(
        [
            "## Authoring Steps",
            "",
        ]
    )
    for step in playbook.get("recommended_steps") or []:
        lines.append(f"- {step}")
    lines.append("")

    for filter_item in playbook.get("filters") or []:
        lines.extend(
            [
                f"## Filter {filter_item.get('order')}: {filter_item.get('display_name')}",
                "",
                f"- Source Label: `{filter_item.get('source_label')}`",
                f"- Field Picker Terms: {', '.join(f'`{item}`' for item in (filter_item.get('field_picker_terms') or []))}",
                f"- Option Count: {filter_item.get('option_count')}",
                "",
                "### Values",
                "",
            ]
        )
        for option_step in filter_item.get("steps") or []:
            if option_step.get("action") != "add_filter_value":
                continue
            lines.append(
                "- "
                + ", ".join(
                    [
                        f"alias `{option_step.get('alias')}`" if option_step.get("alias") else "alias `-`",
                        f"operator `{option_step.get('operation')}`" if option_step.get("operation") else "operator `-`",
                        f"start `{option_step.get('start_value')}`" if option_step.get("start_value") else "start `-`",
                        f"end `{option_step.get('end_value')}`" if option_step.get("end_value") else "end `-`",
                        f"value `{option_step.get('value')}`" if option_step.get("value") else "value `-`",
                    ]
                )
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _write_manual_filter_artifacts(
    *,
    output_dir: Path,
    artifacts: list[dict[str, str]],
    manual_filter_authoring_artifact: dict[str, Any] | None,
    manual_filter_playbook: dict[str, Any] | None,
    manual_filter_automation_plan: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if manual_filter_authoring_artifact is None:
        return None

    manual_filter_path = output_dir / "salesforce_dashboard_manual_filter_authoring.json"
    manual_filter_path.write_text(json.dumps(manual_filter_authoring_artifact, indent=2), encoding="utf-8")
    if not any(item.get("type") == "salesforce_dashboard_manual_filter_authoring" for item in artifacts):
        artifacts.append({"type": "salesforce_dashboard_manual_filter_authoring", "path": str(manual_filter_path)})

    if manual_filter_playbook is not None:
        playbook_json_path = output_dir / "salesforce_dashboard_filter_playbook.json"
        playbook_json_path.write_text(json.dumps(manual_filter_playbook, indent=2), encoding="utf-8")
        if not any(item.get("type") == "salesforce_dashboard_filter_playbook" for item in artifacts):
            artifacts.append({"type": "salesforce_dashboard_filter_playbook", "path": str(playbook_json_path)})

        playbook_markdown_path = output_dir / "salesforce_dashboard_filter_playbook.md"
        playbook_markdown_path.write_text(
            _render_dashboard_manual_filter_playbook_markdown(manual_filter_playbook),
            encoding="utf-8",
        )
        if not any(item.get("type") == "salesforce_dashboard_filter_playbook_markdown" for item in artifacts):
            artifacts.append({"type": "salesforce_dashboard_filter_playbook_markdown", "path": str(playbook_markdown_path)})

    if manual_filter_automation_plan is not None:
        automation_plan_path = output_dir / "salesforce_dashboard_filter_automation_plan.json"
        automation_plan_path.write_text(json.dumps(manual_filter_automation_plan, indent=2), encoding="utf-8")
        if not any(item.get("type") == "salesforce_dashboard_filter_automation_plan" for item in artifacts):
            artifacts.append({"type": "salesforce_dashboard_filter_automation_plan", "path": str(automation_plan_path)})

    return manual_filter_playbook


def _score_baseline_filter_match(candidate_name: str, manual_filter_intent: dict[str, Any]) -> int:
    normalized_candidate = _normalize_label(candidate_name)
    candidate_tokens = _label_tokens(candidate_name)
    comparison_values = [
        str(manual_filter_intent.get("source_label") or ""),
        str(((manual_filter_intent.get("proposed_filter") or {}).get("name")) or ""),
    ]
    best_score = 0
    for value in comparison_values:
        if not value:
            continue
        normalized_value = _normalize_label(value)
        value_tokens = _label_tokens(value)
        if normalized_candidate == normalized_value:
            best_score = max(best_score, 4)
            continue
        if normalized_candidate and normalized_value and (
            normalized_candidate in normalized_value or normalized_value in normalized_candidate
        ):
            best_score = max(best_score, 3)
        overlap = len(candidate_tokens & value_tokens)
        if overlap >= 2:
            best_score = max(best_score, 2)
    return best_score


def _extract_preview_patch_body(preview: dict[str, Any]) -> dict[str, Any]:
    requests = preview.get("requests") or []
    if not requests:
        return {}
    patch_request = requests[-1]
    if not isinstance(patch_request, dict):
        return {}
    body = patch_request.get("body")
    if isinstance(body, dict):
        return body
    return {}


def _discover_dashboard_baseline_candidates(
    *,
    preview: dict[str, Any],
    target_org: str | None,
    current_baseline_id: str | None = None,
    limit: int = 3,
) -> list[dict[str, Any]]:
    if not target_org:
        return []
    manual_filter_intents = preview.get("manual_filter_intents") or []
    if not manual_filter_intents:
        return []

    patch_body = _extract_preview_patch_body(preview)
    target_components = [item for item in patch_body.get("components") or [] if isinstance(item, dict)]
    target_report_ids = {str(item.get("reportId")) for item in target_components if item.get("reportId")}
    target_component_count = len(target_components)

    dashboard_index = _run_rest_request_any(
        f"/services/data/v{API_VERSION}/analytics/dashboards",
        target_org=target_org,
        method="GET",
    )
    if not isinstance(dashboard_index, list):
        return []

    suggestions: list[dict[str, Any]] = []
    for item in dashboard_index:
        if not isinstance(item, dict):
            continue
        dashboard_id = item.get("id")
        if not isinstance(dashboard_id, str) or not dashboard_id or dashboard_id == current_baseline_id:
            continue
        describe = _fetch_rest_json(
            f"/services/data/v{API_VERSION}/analytics/dashboards/{dashboard_id}/describe",
            target_org=target_org,
        )
        metadata = _dashboard_metadata_object(describe)
        components = [component for component in metadata.get("components") or [] if isinstance(component, dict)]
        filters = [filter_item for filter_item in metadata.get("filters") or [] if isinstance(filter_item, dict)]
        if not filters:
            continue

        candidate_report_ids = {str(component.get("reportId")) for component in components if component.get("reportId")}
        report_overlap = sorted(target_report_ids & candidate_report_ids)
        filter_names = [str(filter_item.get("name") or "") for filter_item in filters if filter_item.get("name")]
        matched_filter_names: list[str] = []
        filter_match_score = 0
        for filter_name in filter_names:
            score = max(_score_baseline_filter_match(filter_name, intent) for intent in manual_filter_intents)
            if score > 0:
                matched_filter_names.append(filter_name)
                filter_match_score += score
        if not matched_filter_names and not report_overlap:
            continue

        component_count = len(components)
        component_distance = abs(component_count - target_component_count)
        overall_score = (len(report_overlap) * 5) + filter_match_score - component_distance
        tradeoffs: list[str] = []
        if component_count > max(target_component_count * 2, target_component_count + 4):
            tradeoffs.append(
                f"Much heavier than the target package ({component_count} components vs {target_component_count})."
            )
        if not report_overlap:
            tradeoffs.append("No backing-report overlap with the packaged dashboard components.")

        suggestions.append(
            {
                "dashboard_id": dashboard_id,
                "dashboard_label": describe.get("name") or item.get("name") or dashboard_id,
                "component_count": component_count,
                "filter_count": len(filters),
                "filter_names": filter_names,
                "matched_filter_names": matched_filter_names,
                "report_overlap_count": len(report_overlap),
                "overlapping_report_ids": report_overlap,
                "score": overall_score,
                "tradeoffs": tradeoffs,
            }
        )

    suggestions.sort(
        key=lambda item: (
            -int(item.get("score") or 0),
            -int(item.get("report_overlap_count") or 0),
            -len(item.get("matched_filter_names") or []),
            int(item.get("component_count") or 0),
            str(item.get("dashboard_label") or ""),
        )
    )
    return suggestions[:limit]


def _recommend_clone_baseline_strategy(
    *,
    resolved_clone_baseline: dict[str, Any] | None,
    candidate_clone_baselines: list[dict[str, Any]],
    target_component_count: int,
) -> dict[str, Any]:
    heavy_threshold = max(target_component_count * 2, target_component_count + 4)
    baseline_label = (
        str((resolved_clone_baseline or {}).get("dashboard_label") or "the current lightweight baseline")
    )
    if not candidate_clone_baselines:
        return {
            "code": "keep_current_baseline_manual_filters",
            "summary": (
                f"No filter-bearing baseline matches the packaged dashboard well enough. "
                f"Keep {baseline_label} and author the validated native filters manually after cloning."
            ),
            "recommended_baseline": resolved_clone_baseline,
            "recommended_candidate": None,
        }

    best_candidate = candidate_clone_baselines[0]
    best_report_overlap = int(best_candidate.get("report_overlap_count") or 0)
    best_component_count = int(best_candidate.get("component_count") or 0)
    best_filter_matches = len(best_candidate.get("matched_filter_names") or [])

    if best_report_overlap == 0 and best_component_count > heavy_threshold:
        return {
            "code": "keep_current_baseline_manual_filters",
            "summary": (
                f"{baseline_label} remains the best structural fit. The best filter-bearing alternative "
                f"({best_candidate.get('dashboard_label')}) is materially heavier and does not reuse the packaged reports."
            ),
            "recommended_baseline": resolved_clone_baseline,
            "recommended_candidate": best_candidate,
        }

    if best_report_overlap > 0 or best_filter_matches >= 2:
        return {
            "code": "evaluate_alternative_filter_baseline",
            "summary": (
                f"{best_candidate.get('dashboard_label')} may reduce manual filter work. Compare it against "
                f"{baseline_label} before rebuilding."
            ),
            "recommended_baseline": resolved_clone_baseline,
            "recommended_candidate": best_candidate,
        }

    return {
        "code": "keep_current_baseline_manual_filters",
        "summary": (
            f"{baseline_label} remains the safer baseline. Alternative dashboards do not improve the build enough "
            "to justify switching away from the current lightweight clone path."
        ),
        "recommended_baseline": resolved_clone_baseline,
        "recommended_candidate": best_candidate,
    }


def _extract_dashboard_contract(dashboard_payload: dict[str, Any]) -> dict[str, Any]:
    metadata = _dashboard_metadata_object(dashboard_payload)
    components = metadata.get("components") or []
    filters = metadata.get("filters") or []
    return {
        "folderId": metadata.get("folderId"),
        "components": [
            {
                "title": component.get("title") or component.get("header"),
                "reportId": component.get("reportId"),
                "visualizationType": ((component.get("properties") or {}).get("visualizationType")),
            }
            for component in components
            if isinstance(component, dict)
        ],
        "filters": [
            _normalize_dashboard_filter(item)
            for item in filters
            if isinstance(item, dict)
        ],
    }


def _make_finding(
    *,
    level: str,
    code: str,
    text: str,
    path: str,
    expected: Any | None = None,
    actual: Any | None = None,
) -> dict[str, Any]:
    finding: dict[str, Any] = {
        "level": level,
        "code": code,
        "text": text,
        "path": path,
    }
    if expected is not None:
        finding["expected"] = expected
    if actual is not None:
        finding["actual"] = actual
    return finding


def verify_dashboard_contract(
    *,
    preview: dict[str, Any],
    dashboard_payload: dict[str, Any],
    manual_filter_artifact: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]:
    requests = preview.get("requests") or []
    patch_request = requests[-1] if requests else {}
    expected_contract = _extract_dashboard_contract((patch_request.get("body") or {}) if isinstance(patch_request, dict) else {})
    actual_contract = _extract_dashboard_contract(dashboard_payload)
    manual_filter_intents = preview.get("manual_filter_intents") or []
    if isinstance(manual_filter_artifact, dict):
        artifact_filter_intents = manual_filter_artifact.get("filter_intents")
        if isinstance(artifact_filter_intents, list) and artifact_filter_intents:
            manual_filter_intents = artifact_filter_intents

    findings: list[dict[str, Any]] = []

    if expected_contract["folderId"] != actual_contract["folderId"]:
        findings.append(
            _make_finding(
                level="warn",
                code="folder_id_mismatch",
                text=f"Expected folderId {expected_contract['folderId']!r}, found {actual_contract['folderId']!r}.",
                path="dashboardMetadata.folderId",
                expected=expected_contract["folderId"],
                actual=actual_contract["folderId"],
            )
        )

    expected_components = expected_contract["components"]
    actual_components = actual_contract["components"]
    if len(expected_components) != len(actual_components):
        findings.append(
            _make_finding(
                level="warn",
                code="component_count_mismatch",
                text="The live dashboard component count does not match the packaged contract.",
                path="dashboardMetadata.components",
                expected=len(expected_components),
                actual=len(actual_components),
            )
        )

    unmatched_actual_components = list(actual_components)
    for index, expected_component in enumerate(expected_components):
        actual_component: dict[str, Any] | None = None
        report_id = expected_component.get("reportId")
        title = expected_component.get("title")
        for candidate_index, candidate in enumerate(unmatched_actual_components):
            if report_id and candidate.get("reportId") == report_id:
                actual_component = unmatched_actual_components.pop(candidate_index)
                break
        if actual_component is None and title:
            for candidate_index, candidate in enumerate(unmatched_actual_components):
                if candidate.get("title") == title:
                    actual_component = unmatched_actual_components.pop(candidate_index)
                    break
        if actual_component is None:
            findings.append(
                _make_finding(
                    level="warn",
                    code="missing_expected_component",
                    text=f"Dashboard component {index + 1} from the packaged contract was not found in the live dashboard.",
                    path="dashboardMetadata.components",
                    expected=expected_component,
                    actual=actual_components,
                )
            )
            continue
        if expected_component.get("title") != actual_component.get("title"):
            findings.append(
                _make_finding(
                    level="warn",
                    code="component_title_mismatch",
                    text=f"Dashboard component {index + 1} title does not match the packaged contract.",
                    path=f"dashboardMetadata.components[{index}].title",
                    expected=expected_component.get("title"),
                    actual=actual_component.get("title"),
                )
            )
        if expected_component.get("reportId") != actual_component.get("reportId"):
            findings.append(
                _make_finding(
                    level="warn",
                    code="component_report_id_mismatch",
                    text=f"Dashboard component {index + 1} report binding does not match the packaged contract.",
                    path=f"dashboardMetadata.components[{index}].reportId",
                    expected=expected_component.get("reportId"),
                    actual=actual_component.get("reportId"),
                )
            )
        if expected_component.get("visualizationType") != actual_component.get("visualizationType"):
            findings.append(
                _make_finding(
                    level="warn",
                    code="component_visualization_mismatch",
                    text=f"Dashboard component {index + 1} visualization type does not match the packaged contract.",
                    path=f"dashboardMetadata.components[{index}].properties.visualizationType",
                    expected=expected_component.get("visualizationType"),
                    actual=actual_component.get("visualizationType"),
                )
            )

    expected_filters = expected_contract["filters"]
    actual_filters = actual_contract["filters"]
    actual_filters_by_name = {
        _normalize_label(str(item.get("name") or "")): item
        for item in actual_filters
        if item.get("name")
    }
    for expected_filter in expected_filters:
        filter_name = _normalize_label(str(expected_filter.get("name") or ""))
        actual_filter = actual_filters_by_name.get(filter_name)
        if actual_filter is None:
            findings.append(
                _make_finding(
                    level="warn",
                    code="missing_dashboard_filter",
                    text=f"Dashboard filter {expected_filter.get('name')!r} is missing from the live dashboard.",
                    path="dashboardMetadata.filters",
                    expected=expected_filter,
                    actual=actual_filters,
                )
            )
            continue
        if expected_filter != actual_filter:
            findings.append(
                _make_finding(
                    level="warn",
                    code="dashboard_filter_mismatch",
                    text=f"Dashboard filter {expected_filter.get('name')!r} does not match the packaged contract.",
                    path="dashboardMetadata.filters",
                    expected=expected_filter,
                    actual=actual_filter,
                )
            )

    expected_filter_names = {
        _normalize_label(str(item.get("name") or ""))
        for item in expected_filters
    }
    extra_filters = [
        item
        for item in actual_filters
        if _normalize_label(str(item.get("name") or ""))
        not in expected_filter_names
    ]
    unmatched_manual_candidate_filters = list(actual_filters)
    manual_filter_verification = {
        "source": "manual_filter_authoring_artifact" if isinstance(manual_filter_artifact, dict) else "preview_manual_filter_intents",
        "verified_filters": [],
        "missing_filters": [],
        "mismatched_filters": [],
        "unexpected_filters": [],
    }
    if manual_filter_intents:
        for intent in manual_filter_intents:
            proposed_filter = intent.get("proposed_filter")
            if not isinstance(proposed_filter, dict) or not proposed_filter:
                continue
            matched_index = None
            matched_filter = None
            best_score = 0
            for index, candidate in enumerate(unmatched_manual_candidate_filters):
                candidate_name = str(candidate.get("name") or "")
                score = _score_baseline_filter_match(candidate_name, intent)
                if score > best_score:
                    best_score = score
                    matched_index = index
                    matched_filter = candidate
            proposed_name = proposed_filter.get("name") or intent.get("source_label")
            if matched_filter is None or best_score <= 0:
                missing_filter = {
                    "source_label": intent.get("source_label"),
                    "expected_filter_name": proposed_name,
                    "proposal_source": intent.get("proposal_source"),
                }
                manual_filter_verification["missing_filters"].append(missing_filter)
                findings.append(
                    _make_finding(
                        level="warn",
                        code="missing_manual_dashboard_filter",
                        text=f"Authored dashboard filter {proposed_name!r} from the manual filter contract was not found.",
                        path="dashboardMetadata.filters",
                        expected=proposed_filter,
                        actual=actual_filters,
                    )
                )
                continue
            unmatched_manual_candidate_filters.pop(matched_index)
            proposed_options = _normalized_filter_options(proposed_filter)
            actual_options = _normalized_filter_options(matched_filter)
            if proposed_options != actual_options:
                mismatch = {
                    "source_label": intent.get("source_label"),
                    "expected_filter_name": proposed_name,
                    "expected_filter": _normalize_dashboard_filter(proposed_filter),
                    "actual_filter": _normalize_dashboard_filter(matched_filter),
                }
                manual_filter_verification["mismatched_filters"].append(mismatch)
                findings.append(
                    _make_finding(
                        level="warn",
                        code="manual_dashboard_filter_mismatch",
                        text=f"Authored dashboard filter {proposed_name!r} does not match the manual filter contract.",
                        path="dashboardMetadata.filters",
                        expected=_normalize_dashboard_filter(proposed_filter),
                        actual=_normalize_dashboard_filter(matched_filter),
                    )
                )
                continue
            verified_filter = {
                "source_label": intent.get("source_label"),
                "expected_filter_name": proposed_name,
                "actual_filter_name": matched_filter.get("name"),
            }
            manual_filter_verification["verified_filters"].append(verified_filter)

        if manual_filter_verification["verified_filters"] and not manual_filter_verification["missing_filters"] and not manual_filter_verification["mismatched_filters"]:
            findings.append(
                _make_finding(
                    level="info",
                    code="manual_dashboard_filters_verified",
                    text=f"Verified {len(manual_filter_verification['verified_filters'])} authored dashboard filter(s) against the manual filter contract.",
                    path="dashboardMetadata.filters",
                    expected=[item.get("expected_filter_name") for item in manual_filter_verification["verified_filters"]],
                    actual=[item.get("actual_filter_name") for item in manual_filter_verification["verified_filters"]],
                )
            )

    unmatched_extra_filters = []
    for item in extra_filters:
        normalized_name = _normalize_label(str(item.get("name") or ""))
        if any(
            _normalize_label(str(filter_item.get("name") or "")) == normalized_name
            for filter_item in manual_filter_verification["unexpected_filters"]
        ):
            continue
        if any(
            _normalize_label(str(filter_item.get("actual_filter_name") or "")) == normalized_name
            for filter_item in manual_filter_verification["verified_filters"]
        ):
            continue
        if any(
            _normalize_label(str((filter_item.get("actual_filter") or {}).get("name") or "")) == normalized_name
            for filter_item in manual_filter_verification["mismatched_filters"]
        ):
            continue
        unmatched_extra_filters.append(item)

    if unmatched_extra_filters:
        manual_filter_verification["unexpected_filters"] = [_normalize_dashboard_filter(item) for item in unmatched_extra_filters]
        findings.append(
            _make_finding(
                level="warn",
                code="unexpected_live_dashboard_filters" if manual_filter_intents else "extra_live_dashboard_filters",
                text="The live dashboard contains extra filters beyond the explicit packaged payload and manual filter contract."
                if manual_filter_intents
                else "The live dashboard contains extra filters beyond the explicit packaged payload.",
                path="dashboardMetadata.filters",
                expected=expected_filters,
                actual=unmatched_extra_filters,
            )
        )

    summary = {
        "surface_type": "salesforce_dashboard",
        "component_count": len(expected_components),
        "explicit_filter_count": len(expected_filters),
        "manual_filter_intent_count": len(manual_filter_intents),
        "manual_filter_verified_count": len(manual_filter_verification["verified_filters"]),
        "manual_filter_missing_count": len(manual_filter_verification["missing_filters"]),
        "manual_filter_mismatch_count": len(manual_filter_verification["mismatched_filters"]),
        "unexpected_extra_filter_count": len(manual_filter_verification["unexpected_filters"]),
        "finding_count": len(findings),
        "warn_count": sum(1 for item in findings if item["level"] == "warn"),
        "info_count": sum(1 for item in findings if item["level"] == "info"),
    }
    return findings, expected_contract, {**actual_contract, "summary": summary}, manual_filter_verification


def autofill_dashboard_preview(
    preview: dict[str, Any],
    fill_requirements: list[dict[str, str]],
    *,
    dashboard_payload: dict[str, Any],
    target_org: str | None = None,
    autofill_vocab: dict[str, Any] | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    metadata = _dashboard_metadata_object(dashboard_payload)
    requests = preview.get("requests") or []
    patch_request = requests[-1] if requests else {}
    dashboard_metadata = _dashboard_metadata_object((patch_request.get("body") or {}) if isinstance(patch_request, dict) else {})
    components = dashboard_metadata.get("components", [])
    filters = dashboard_metadata.get("filters", [])
    package_developer_name = dashboard_metadata.get("developerName") if isinstance(dashboard_metadata, dict) else None

    component_candidates: dict[str, str] = {}
    component_property_candidates: dict[str, dict[str, Any]] = {}
    for component in metadata.get("components") or []:
        if not isinstance(component, dict):
            continue
        label = component.get("title") or component.get("header")
        report_id = component.get("reportId")
        if isinstance(label, str) and isinstance(report_id, str) and label and report_id:
            component_candidates[label] = report_id
            properties = component.get("properties")
            if isinstance(properties, dict):
                component_property_candidates[report_id] = copy.deepcopy(properties)

    filter_candidates: dict[str, dict[str, Any]] = {}
    for item in metadata.get("filters") or []:
        if isinstance(item, dict) and isinstance(item.get("name"), str):
            filter_candidates[_normalize_label(item["name"])] = item

    autofills: list[dict[str, str]] = []
    remaining: list[dict[str, str]] = []
    manual_filter_intents: list[dict[str, str]] = []
    manual_filter_indexes: set[int] = set()

    folder_id = metadata.get("folderId")
    if folder_id:
        for request in requests:
            body = request.get("body") or {}
            if "folderId" in body and body.get("folderId") == "__FILL_FOLDER_ID__":
                body["folderId"] = folder_id
            request_dashboard_metadata = body.get("dashboardMetadata")
            if isinstance(request_dashboard_metadata, dict) and request_dashboard_metadata.get("folderId") == "__FILL_FOLDER_ID__":
                request_dashboard_metadata["folderId"] = folder_id
        autofills.append({"category": "folder_id", "value": str(folder_id), "source": "baseline_dashboard.folderId"})

    for requirement in fill_requirements:
        category = requirement.get("category")
        target_path = requirement.get("target_path", "")
        source_label = requirement.get("source_label")
        resolved = False

        if category == "component_report_id" and source_label:
            report_id = _resolve_component_report_id(source_label, component_candidates)
            resolved_source = f"baseline_dashboard.component:{source_label}"
            if report_id is None:
                report_id, resolved_source = _resolve_component_alias(
                    source_label,
                    package_developer_name=package_developer_name if isinstance(package_developer_name, str) else None,
                    autofill_vocab=autofill_vocab,
                )
            match = re.search(r"components\[(\d+)\]", target_path)
            if report_id and match:
                index = int(match.group(1))
                if index < len(components):
                    components[index]["reportId"] = report_id
                    resolved = True
                    autofills.append(
                        {
                            "category": "component_report_id",
                            "value": report_id,
                            "source": resolved_source,
                        }
                    )
        elif category == "component_properties" and source_label:
            match = re.search(r"components\[(\d+)\]", target_path)
            if match:
                index = int(match.group(1))
                if index < len(components):
                    component = components[index]
                    candidate_properties: dict[str, Any] | None = None
                    candidate_source: str | None = None
                    component_report_id = component.get("reportId") if isinstance(component, dict) else None
                    if isinstance(component_report_id, str) and component_report_id:
                        candidate_properties = component_property_candidates.get(component_report_id)
                        if candidate_properties is not None:
                            candidate_source = f"baseline_dashboard.component_properties:{source_label}"
                    if candidate_properties is None:
                        candidate_properties, candidate_source = _resolve_component_property_alias(
                            source_label,
                            report_id=component_report_id if isinstance(component_report_id, str) else None,
                            package_developer_name=package_developer_name if isinstance(package_developer_name, str) else None,
                            autofill_vocab=autofill_vocab,
                        )
                    if candidate_properties is not None:
                        component["properties"] = copy.deepcopy(candidate_properties)
                        resolved = True
                        autofills.append(
                            {
                                "category": "component_properties",
                                "value": source_label,
                                "source": candidate_source or f"baseline_dashboard.component_properties:{source_label}",
                            }
                        )
        elif category == "dashboard_filter_options" and source_label:
            match = re.search(r"filters\[(\d+)\]", target_path)
            candidate = filter_candidates.get(_normalize_label(source_label))
            candidate_source = f"baseline_dashboard.filter:{source_label}"
            if isinstance(candidate, dict) and match:
                index = int(match.group(1))
                if index < len(filters):
                    filters[index] = _sanitize_dashboard_filter_payload(candidate)
                    resolved = True
                    autofills.append(
                        {
                            "category": "dashboard_filter_options",
                            "value": source_label,
                            "source": candidate_source,
                        }
                    )
            elif match:
                index = int(match.group(1))
                if index < len(filters):
                    template, template_source = _resolve_dashboard_filter_template(
                        source_label,
                        package_developer_name=package_developer_name if isinstance(package_developer_name, str) else None,
                        autofill_vocab=autofill_vocab,
                    )
                    alias_candidate, alias_source = _resolve_dashboard_filter_alias(
                        source_label,
                        package_developer_name=package_developer_name if isinstance(package_developer_name, str) else None,
                        autofill_vocab=autofill_vocab,
                    )
                    proposed_filter = None
                    proposal_source = None
                    compatibility_status = "unverified"
                    compatibility_message = None
                    if isinstance(template, dict):
                        proposed_filter = _sanitize_dashboard_filter_payload(template["filter"])
                        proposal_source = template_source
                        analysis = template.get("analysis")
                        if isinstance(analysis, dict):
                            filter_columns = analysis.get("filter_columns") or []
                            options = proposed_filter.get("options") or []
                            if target_org and isinstance(filter_columns, list) and filter_columns and isinstance(options, list):
                                try:
                                    _run_dashboard_filter_options_analysis(
                                        target_org=target_org,
                                        filter_columns=[
                                            {
                                                "reportId": str(item["reportId"]),
                                                "name": str(item["name"]),
                                            }
                                            for item in filter_columns
                                            if isinstance(item, dict) and item.get("reportId") and item.get("name")
                                        ],
                                        options=options,
                                    )
                                    compatibility_status = "validated"
                                except Exception as exc:
                                    compatibility_status = "invalid"
                                    compatibility_message = str(exc)
                            elif filter_columns:
                                compatibility_status = "analysis_skipped"
                                compatibility_message = "Live filter compatibility analysis requires --autofill-live and --target-org."
                    elif isinstance(alias_candidate, dict):
                        proposed_filter = _sanitize_dashboard_filter_payload(alias_candidate)
                        proposal_source = alias_source
                        compatibility_status = "unverified"
                        compatibility_message = "Imported filter contracts from other dashboards are not treated as PATCH-ready unless the cloned baseline already contains the filter."

                    manual_filter_indexes.add(index)
                    reason = "No concrete native dashboard filter options were available from baseline or live metadata."
                    guidance = "Author this dashboard filter manually with real options and selectedOption before live use."
                    if proposed_filter is not None:
                        reason = (
                            "A concrete native dashboard filter proposal was identified, but it is not PATCH-ready from the current clone baseline."
                        )
                        guidance = (
                            "Author this dashboard filter manually after cloning, or start from a baseline dashboard that already contains a matching native filter contract."
                        )
                    if compatibility_status == "validated":
                        guidance = (
                            "Salesforce filter-options analysis validated the proposed filter values against the referenced report fields. "
                            "Author this filter manually after cloning, because creating new native dashboard filters from this baseline is not yet proven."
                        )
                    elif compatibility_status == "invalid" and compatibility_message:
                        reason = "The proposed native dashboard filter failed Salesforce filter compatibility analysis."
                        guidance = compatibility_message
                    manual_filter_intents.append(
                        {
                            "source_label": source_label,
                            "reason": reason,
                            "guidance": guidance,
                            "proposal_source": proposal_source,
                            "compatibility_status": compatibility_status,
                            "compatibility_message": compatibility_message,
                            "proposed_filter": proposed_filter,
                        }
                    )
                    resolved = True

        elif category == "folder_id" and folder_id:
            continue

        if not resolved and not (category == "folder_id" and folder_id):
            remaining.append(requirement)

    if manual_filter_indexes:
        dashboard_metadata["filters"] = [
            value for index, value in enumerate(filters) if index not in manual_filter_indexes
        ]
        preview["manual_filter_intents"] = manual_filter_intents
        notes = preview.setdefault("notes", [])
        manual_filter_note = (
            "Dashboard filter intents are preserved separately and omitted from the REST payload when no concrete filter option contract is available."
        )
        if manual_filter_note not in notes:
            notes.append(manual_filter_note)

    baseline_component_count = sum(1 for item in metadata.get("components") or [] if isinstance(item, dict))
    baseline_filter_count = sum(1 for item in metadata.get("filters") or [] if isinstance(item, dict))
    target_component_count = sum(1 for item in components if isinstance(item, dict))
    if (
        any(isinstance(request, dict) and request.get("id") == "clone_dashboard" for request in requests)
        and target_component_count
        and baseline_component_count > max(target_component_count * 2, target_component_count + 5)
    ):
        remaining.append(
            _fill_requirement(
                category="baseline_component_shape",
                target_path="clone_dashboard.path",
                current_value=f"{baseline_component_count}_to_{target_component_count}",
                guidance=(
                    "Use a baseline dashboard with a closer component count and topology before live apply. "
                    "Large component-set rewrites from the current clone baseline are not yet proven by the Dashboards REST save flow."
                ),
                request_id="clone_dashboard",
            )
        )

    summary = {
        "applied_count": len(autofills),
        "remaining_count": len(remaining),
        "applied_categories": sorted({item["category"] for item in autofills}),
        "manual_filter_intent_count": len(manual_filter_intents),
        "baseline_component_count": baseline_component_count,
        "baseline_filter_count": baseline_filter_count,
        "target_component_count": target_component_count,
    }
    return remaining, {
        "artifact_type": "salesforce_dashboard_autofill_summary",
        "autofills": autofills,
        "manual_filter_intents": manual_filter_intents,
        "summary": summary,
    }


def validate_build_package(build_package: dict[str, Any]) -> tuple[list[str], list[str], dict[str, Any]]:
    errors: list[str] = []
    warnings: list[str] = []

    surface_contract = build_package.get("surface_contract")
    if not isinstance(surface_contract, dict):
        errors.append("surface_contract must be an object")
        return errors, warnings, {}

    if surface_contract.get("surface_type") != "salesforce_dashboard":
        errors.append("surface_contract.surface_type must be salesforce_dashboard")

    page_model = surface_contract.get("page_model")
    if not isinstance(page_model, list) or not page_model:
        errors.append("surface_contract.page_model must be a non-empty list")

    page_storyboard = surface_contract.get("page_storyboard")
    if not isinstance(page_storyboard, list) or not page_storyboard:
        errors.append("surface_contract.page_storyboard must be a non-empty list")
        page_storyboard = []

    filters = surface_contract.get("filters")
    if not isinstance(filters, list):
        errors.append("surface_contract.filters must be a list")
        filters = []

    handoff_target = surface_contract.get("handoff_target")
    if handoff_target is not None and not isinstance(handoff_target, dict):
        errors.append("surface_contract.handoff_target must be an object when present")

    section_count = 0
    widget_count = 0
    for page in page_storyboard:
        if not isinstance(page, dict):
            errors.append("surface_contract.page_storyboard items must be objects")
            continue
        sections = page.get("sections")
        if not isinstance(sections, list):
            errors.append("Each page_storyboard page must include a sections list")
            continue
        section_count += len(sections)
        for section in sections:
            widgets = section.get("widgets") if isinstance(section, dict) else None
            if not isinstance(widgets, list):
                errors.append("Each page_storyboard section must include a widgets list")
                continue
            widget_count += len(widgets)

    if isinstance(page_model, list) and len(page_model) > 2:
        warnings.append("Native Salesforce dashboards should stay lightweight; more than 2 views may be too heavy")

    summary = {
        "surface_type": surface_contract.get("surface_type"),
        "page_count": len(page_model) if isinstance(page_model, list) else 0,
        "storyboard_page_count": len(page_storyboard),
        "section_count": section_count,
        "widget_count": widget_count,
        "filter_count": len(filters),
        "has_handoff_target": isinstance(handoff_target, dict),
    }
    return errors, warnings, summary


def _suggested_dashboard_label(build_package: dict[str, Any]) -> str:
    build_brief = build_package.get("build_brief") or {}
    excellence_target = build_brief.get("excellence_target")
    reference_exemplar = build_brief.get("reference_exemplar")
    persona = build_brief.get("persona")
    domain = build_brief.get("domain")
    parts = [part for part in [persona, domain, excellence_target, reference_exemplar] if isinstance(part, str) and part]
    if parts:
        return (" ".join(parts[:3]))[:80]
    return "Builder Brain Dashboard"


def _suggested_dashboard_developer_name(build_package: dict[str, Any]) -> str:
    label = _suggested_dashboard_label(build_package)
    slug = re.sub(r"[^A-Za-z0-9]+", "_", label).strip("_")
    slug = slug[:40] or "Builder_Brain_Dashboard"
    if slug[0].isdigit():
        slug = f"D_{slug}"
    return slug


def build_dashboard_bundle(build_package: dict[str, Any]) -> dict[str, Any]:
    surface_contract = build_package["surface_contract"]
    page_storyboard = surface_contract.get("page_storyboard", [])

    dashboard_definition = {
        "artifact_type": "salesforce_dashboard_definition",
        "suggested_label": _suggested_dashboard_label(build_package),
        "suggested_developer_name": _suggested_dashboard_developer_name(build_package),
        "page_model": surface_contract.get("page_model", []),
        "filters": surface_contract.get("filters", []),
        "page_storyboard": page_storyboard,
        "handoff_surface": surface_contract.get("handoff_surface"),
    }
    component_plan = {
        "artifact_type": "salesforce_dashboard_component_plan",
        "components": [
            {
                "page": page.get("page"),
                "section": section.get("section"),
                "intent": section.get("intent"),
                "widgets": [
                    {
                        "role": widget.get("role"),
                        "visualization_type": widget.get("widget"),
                        "metric": widget.get("metric"),
                    }
                    for widget in section.get("widgets", [])
                ],
            }
            for page in page_storyboard
            for section in page.get("sections", [])
        ],
    }
    report_dependencies = {
        "artifact_type": "salesforce_dashboard_report_dependencies",
        "dependencies": [
            {
                "page": page.get("page"),
                "section": section.get("section"),
                "widget_role": widget.get("role"),
                "metric": widget.get("metric"),
                "expected_report_shape": (
                    "tabular_queue" if widget.get("widget") == "comparisontable" else "summary_visual_source"
                ),
            }
            for page in page_storyboard
            for section in page.get("sections", [])
            for widget in section.get("widgets", [])
        ],
    }
    validation_checklist = {
        "artifact_type": "salesforce_dashboard_validation_checklist",
        "review_gates": build_package.get("review_gates", []),
        "acceptance_criteria": build_package.get("acceptance_criteria", []),
        "design_constraints": build_package.get("design_constraints", []),
        "required_checks": [
            "dashboard stays lightweight and avoids CRMA-style page sprawl",
            "component layout preserves the packaged story order",
            "backing report dependencies are explicit before authoring",
            "screenshot review confirms executive readability",
        ],
    }
    authoring_steps = {
        "artifact_type": "salesforce_dashboard_authoring_steps",
        "delivery_mode": build_package.get("delivery_mode"),
        "steps": [
            {
                "sequence": 1,
                "phase": "dashboard_core",
                "objective": "Author the native dashboard using the packaged page/view model.",
                "actions": [
                    f"Keep the dashboard within {len(surface_contract.get('page_model', []))} view(s).",
                    "Lay out components in the packaged story order.",
                    "Preserve the packaged handoff surface instead of duplicating row-level detail.",
                ],
            },
            {
                "sequence": 2,
                "phase": "validation",
                "objective": "Validate the dashboard against the packaged review gates.",
                "actions": [
                    "Confirm the dashboard remains lightweight and scan-fast.",
                    "Validate all backing report dependencies before rollout.",
                    "Run screenshot review on the finished dashboard.",
                ],
            },
        ],
    }

    return {
        "artifact_type": "salesforce_dashboard_authoring_bundle",
        "build_brief": build_package.get("build_brief", {}),
        "dashboard_definition": dashboard_definition,
        "component_plan": component_plan,
        "report_dependencies": report_dependencies,
        "validation_checklist": validation_checklist,
        "authoring_steps": authoring_steps,
        "handoff_target": surface_contract.get("handoff_target"),
        "acceptance_criteria": build_package.get("acceptance_criteria", []),
        "revision_summary": build_package.get("revision_summary", []),
        "repo_execution_fit": build_package.get("repo_execution_fit"),
    }


def _placeholder(prefix: str, label: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    return f"__FILL_{prefix}_{slug or 'value'}__"


def _fill_requirement(
    *,
    category: str,
    target_path: str,
    current_value: str,
    guidance: str,
    source_label: str | None = None,
    request_id: str,
) -> dict[str, str]:
    payload = {
        "category": category,
        "target_path": target_path,
        "current_value": current_value,
        "guidance": guidance,
        "request_id": request_id,
    }
    if source_label:
        payload["source_label"] = source_label
    return payload


def _visualization_type(widget_type: str) -> str:
    mapping = {
        "number": "Metric",
        "line": "Line",
        "bar": "Column",
        "comparisontable": "Table",
        "table": "Table",
    }
    return mapping.get(widget_type, "Table")


def _build_native_layout(layout_columns: list[dict[str, list[int]]], component_count: int) -> dict[str, Any]:
    if not layout_columns:
        return {"gridLayout": True, "numColumns": 12, "rowHeight": 36, "components": []}

    column_count = len(layout_columns)
    if column_count == 1:
        widths = [12]
    elif column_count == 2:
        widths = [8, 4]
    elif column_count == 3:
        widths = [4, 4, 4]
    else:
        base = 12 // column_count
        remainder = 12 % column_count
        widths = [base + (1 if index < remainder else 0) for index in range(column_count)]

    start_columns: list[int] = []
    cursor = 0
    for width in widths:
        start_columns.append(cursor)
        cursor += width

    max_column_depth = max(len(column.get("components") or []) for column in layout_columns) or 1
    total_rows = max_column_depth * 7
    layout_components: list[dict[str, int] | None] = [None] * component_count

    for column_index, column in enumerate(layout_columns):
        component_indexes = column.get("components") or []
        current_row = 0
        for position, component_index in enumerate(component_indexes):
            remaining_components = len(component_indexes) - position
            remaining_rows = total_rows - current_row
            rowspan = remaining_rows if remaining_components == 1 else max(7, remaining_rows // remaining_components)
            layout_components[component_index] = {
                "column": start_columns[column_index],
                "colspan": widths[column_index],
                "row": current_row,
                "rowspan": rowspan,
            }
            current_row += rowspan

    return {
        "gridLayout": True,
        "numColumns": 12,
        "rowHeight": 36,
        "components": [item for item in layout_components if isinstance(item, dict)],
    }


def build_dashboard_rest_preview(
    build_package: dict[str, Any],
    *,
    dashboard_id: str | None,
    clone_from_dashboard_id: str | None,
    folder_id: str | None,
) -> tuple[dict[str, Any], list[dict[str, str]], dict[str, Any]]:
    authoring_bundle = build_dashboard_bundle(build_package)
    dashboard_definition = authoring_bundle["dashboard_definition"]

    component_records: list[dict[str, Any]] = []
    layout_columns: list[dict[str, list[int]]] = []
    fill_requirements: list[dict[str, str]] = []
    component_index = 0
    request_id_for_patch = "patch_dashboard"

    for page in dashboard_definition["page_storyboard"]:
        for section in page.get("sections", []):
            section_components: list[int] = []
            for widget in section.get("widgets", []):
                report_placeholder = _placeholder("REPORT_ID", str(widget.get("metric", "metric")))
                component_records.append(
                    {
                        "componentData": component_index,
                        "header": widget.get("metric"),
                        "footer": None,
                        "properties": {
                            "visualizationType": _visualization_type(str(widget.get("widget", "table"))),
                            "useReportChart": False,
                        },
                        "reportId": report_placeholder,
                        "title": widget.get("metric"),
                        "type": "Report",
                    }
                )
                fill_requirements.append(
                    _fill_requirement(
                        category="component_report_id",
                        target_path=f"{request_id_for_patch}.body.components[{component_index}].reportId",
                        current_value=report_placeholder,
                        guidance="Fill with the backing report id for this dashboard component.",
                        source_label=str(widget.get("metric")),
                        request_id=request_id_for_patch,
                    )
                )
                fill_requirements.append(
                    _fill_requirement(
                        category="component_properties",
                        target_path=f"{request_id_for_patch}.body.components[{component_index}].properties",
                        current_value=str(widget.get("widget", "table")),
                        guidance="Fill with a compatible native dashboard component properties contract, usually copied from a working baseline component.",
                        source_label=str(widget.get("metric")),
                        request_id=request_id_for_patch,
                    )
                )
                section_components.append(component_index)
                component_index += 1
            if section_components:
                layout_columns.append({"components": section_components})

    dashboard_metadata = {
        "name": dashboard_definition["suggested_label"],
        "developerName": dashboard_definition["suggested_developer_name"],
        "folderId": folder_id or "__FILL_FOLDER_ID__",
        "layout": _build_native_layout(layout_columns, len(component_records)),
        "components": component_records,
        "filters": [
            {
                "name": filter_name,
                "options": [],
                "selectedOption": None,
            }
            for filter_name in dashboard_definition["filters"]
        ],
    }

    requests: list[dict[str, Any]] = []
    if dashboard_id:
        strategy = "patch_existing"
        requests.append(
            {
                "id": request_id_for_patch,
                "method": "PATCH",
                "path": f"/services/data/v{API_VERSION}/analytics/dashboards/{dashboard_id}",
                "body": dashboard_metadata,
                "purpose": "Save the packaged native dashboard using the Dashboard Results resource.",
            }
        )
    else:
        strategy = "clone_then_patch"
        clone_request_id = "clone_dashboard"
        clone_source = clone_from_dashboard_id or "__FILL_BASELINE_DASHBOARD_ID__"
        requests.extend(
            [
                {
                    "id": clone_request_id,
                    "method": "POST",
                    "path": f"/services/data/v{API_VERSION}/analytics/dashboards?cloneId={clone_source}",
                    "body": {"folderId": folder_id or "__FILL_FOLDER_ID__"},
                    "purpose": "Clone a lightweight baseline dashboard before applying the packaged metadata.",
                },
                {
                    "id": request_id_for_patch,
                    "method": "PATCH",
                    "path": f"/services/data/v{API_VERSION}/analytics/dashboards/__FILL_CLONED_DASHBOARD_ID__",
                    "body": dashboard_metadata,
                    "purpose": "Save the packaged dashboard contract onto the cloned dashboard.",
                },
            ]
        )
        fill_requirements.append(
            _fill_requirement(
                category="cloned_dashboard_id",
                target_path=f"{request_id_for_patch}.path",
                current_value="__FILL_CLONED_DASHBOARD_ID__",
                guidance="Fill with the dashboard id returned by the clone response before sending the PATCH request.",
                request_id=request_id_for_patch,
            )
        )
        if clone_from_dashboard_id is None:
            fill_requirements.append(
                _fill_requirement(
                    category="baseline_dashboard_id",
                    target_path=f"{clone_request_id}.path",
                    current_value="__FILL_BASELINE_DASHBOARD_ID__",
                    guidance="Fill with the id of the dashboard you want to clone as the baseline.",
                    request_id=clone_request_id,
                )
            )

    if folder_id is None:
        fill_requirements.append(
            _fill_requirement(
                category="folder_id",
                target_path=(
                    "clone_dashboard.body.folderId, "
                    f"{request_id_for_patch}.body.folderId"
                    if strategy == "clone_then_patch"
                    else f"{request_id_for_patch}.body.folderId"
                ),
                current_value="__FILL_FOLDER_ID__",
                guidance="Fill with the target dashboard folder id.",
                request_id=request_id_for_patch,
            )
        )

    for index, filter_name in enumerate(dashboard_definition["filters"]):
        fill_requirements.append(
            _fill_requirement(
                category="dashboard_filter_options",
                target_path=f"{request_id_for_patch}.body.filters[{index}]",
                current_value=filter_name,
                guidance="Populate the dashboard filter options and selectedOption using the concrete dashboard filter contract.",
                source_label=str(filter_name),
                request_id=request_id_for_patch,
            )
        )

    preview = {
        "artifact_type": "salesforce_dashboard_rest_preview",
        "strategy": strategy,
        "api_version": API_VERSION,
        "requests": requests,
        "notes": [
            "The Dashboards REST API supports PATCH save, PUT refresh, POST clone, and DELETE on dashboard resources.",
            "This preview stays fill-first because the builder package does not yet resolve backing report ids or dashboard filter option ids.",
        ],
    }
    summary = {
        "surface_type": "salesforce_dashboard",
        "strategy": strategy,
        "request_count": len(requests),
        "fill_requirement_count": len(fill_requirements),
        "manual_filter_intent_count": 0,
    }
    return preview, fill_requirements, summary


def prepare_dashboard_preview(
    *,
    build_package: dict[str, Any],
    dashboard_id: str | None,
    clone_from_dashboard_id: str | None,
    folder_id: str | None,
    baseline_dashboard_json: Path | None,
    autofill_live: bool,
    target_org: str | None,
    autofill_vocab: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[dict[str, str]], dict[str, Any], str, dict[str, Any] | None]:
    package_developer_name = _suggested_dashboard_developer_name(build_package)
    effective_clone_from_dashboard_id = clone_from_dashboard_id
    baseline_resolution: dict[str, str] | None = None
    if dashboard_id is None and effective_clone_from_dashboard_id is None:
        effective_clone_from_dashboard_id, baseline_resolution = _resolve_dashboard_baseline_alias(
            package_developer_name=package_developer_name,
            autofill_vocab=autofill_vocab,
        )

    preview, fill_requirements, preview_summary = build_dashboard_rest_preview(
        build_package,
        dashboard_id=dashboard_id,
        clone_from_dashboard_id=effective_clone_from_dashboard_id,
        folder_id=folder_id,
    )
    command_class = "read_only"
    autofill_summary: dict[str, Any] | None = None
    source_dashboard_id = effective_clone_from_dashboard_id or dashboard_id

    if baseline_resolution is not None:
        preview["resolved_clone_baseline"] = baseline_resolution
        preview.setdefault("notes", []).append(
            f"Resolved clone baseline {baseline_resolution['dashboard_label']} ({baseline_resolution['dashboard_id']}) "
            f"from {baseline_resolution['source']}."
        )
        preview_summary["resolved_clone_baseline"] = baseline_resolution

    if autofill_live and not target_org:
        raise ValueError("--target-org is required with --autofill-live.")

    if baseline_dashboard_json or autofill_live:
        dashboard_payload = load_dashboard_metadata(
            dashboard_id=source_dashboard_id,
            target_org=target_org if autofill_live else None,
            baseline_dashboard_json=baseline_dashboard_json,
        )
        if dashboard_payload:
            fill_requirements, autofill_summary = autofill_dashboard_preview(
                preview,
                fill_requirements,
                dashboard_payload=dashboard_payload,
                target_org=target_org if autofill_live else None,
                autofill_vocab=autofill_vocab,
            )
        if autofill_live:
            command_class = "live_read"

    candidate_clone_baselines: list[dict[str, Any]] = []
    baseline_strategy: dict[str, Any] | None = None
    if target_org and preview.get("manual_filter_intents"):
        candidate_clone_baselines = _discover_dashboard_baseline_candidates(
            preview=preview,
            target_org=target_org,
            current_baseline_id=source_dashboard_id,
        )
        preview["candidate_clone_baselines"] = candidate_clone_baselines
        baseline_strategy = _recommend_clone_baseline_strategy(
            resolved_clone_baseline=preview.get("resolved_clone_baseline"),
            candidate_clone_baselines=candidate_clone_baselines,
            target_component_count=((autofill_summary or {}).get("summary") or {}).get(
                "target_component_count",
                len([item for item in (_extract_preview_patch_body(preview).get("components") or []) if isinstance(item, dict)]),
            ),
        )
        preview["baseline_strategy"] = baseline_strategy
        preview_summary["baseline_strategy"] = baseline_strategy["code"]
        if candidate_clone_baselines:
            preview.setdefault("notes", []).append(
                "Identified filter-bearing alternative clone baselines from live dashboard metadata."
            )
            preview_summary["candidate_clone_baseline_count"] = len(candidate_clone_baselines)
        if baseline_strategy:
            preview.setdefault("notes", []).append(baseline_strategy["summary"])
        if command_class == "read_only":
            command_class = "live_read"

    preview_summary["fill_requirement_count"] = len(fill_requirements)
    if autofill_summary is not None:
        preview_summary["autofill_count"] = autofill_summary["summary"]["applied_count"]
        preview_summary["manual_filter_intent_count"] = autofill_summary["summary"]["manual_filter_intent_count"]

    return preview, fill_requirements, preview_summary, command_class, autofill_summary


def _external_fill_requirements(fill_requirements: list[dict[str, str]]) -> list[dict[str, str]]:
    return [item for item in fill_requirements if item.get("category") != "cloned_dashboard_id"]


def _extract_dashboard_id(response_payload: dict[str, Any]) -> str | None:
    for key in ("id", "dashboardId"):
        value = response_payload.get(key)
        if isinstance(value, str) and value:
            return value
    attributes = response_payload.get("attributes")
    if isinstance(attributes, dict):
        for key in ("id", "dashboardId"):
            value = attributes.get(key)
            if isinstance(value, str) and value:
                return value
    dashboard_payload = response_payload.get("dashboard")
    if isinstance(dashboard_payload, dict):
        return _extract_dashboard_id(dashboard_payload)
    return None


def execute_dashboard_requests(
    preview: dict[str, Any],
    *,
    target_org: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
    execution_requests: list[dict[str, Any]] = []
    responses: list[dict[str, Any]] = []
    cloned_dashboard_id: str | None = None

    for request in preview.get("requests") or []:
        if not isinstance(request, dict):
            continue
        request_path = str(request.get("path") or "")
        if "__FILL_CLONED_DASHBOARD_ID__" in request_path:
            if not cloned_dashboard_id:
                raise RuntimeError("clone response did not return a dashboard id for the follow-on PATCH request.")
            request_path = request_path.replace("__FILL_CLONED_DASHBOARD_ID__", cloned_dashboard_id)

        response_payload = _run_rest_request(
            request_path,
            target_org=target_org,
            method=str(request.get("method") or "GET"),
            body=request.get("body"),
        )
        response_dashboard_id = _extract_dashboard_id(response_payload)
        if request.get("id") == "clone_dashboard":
            cloned_dashboard_id = response_dashboard_id
            if not cloned_dashboard_id:
                raise RuntimeError("clone response did not return a usable dashboard id.")

        execution_requests.append(
            {
                "id": request.get("id"),
                "method": request.get("method"),
                "path": request_path,
                "purpose": request.get("purpose"),
            }
        )
        responses.append(
            {
                "request_id": request.get("id"),
                "method": request.get("method"),
                "path": request_path,
                "dashboard_id": response_dashboard_id,
                "name": response_payload.get("name"),
                "payload": response_payload,
            }
        )

    return execution_requests, responses, cloned_dashboard_id


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Salesforce dashboard authoring executor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate a builder_brain salesforce_dashboard package.")
    validate.add_argument("--package", required=True, help="Path to build_package.json")
    validate.add_argument("--json", action="store_true", help="Print JSON output.")

    bundle = subparsers.add_parser("bundle", help="Compile a salesforce_dashboard authoring bundle.")
    bundle.add_argument("--package", required=True, help="Path to build_package.json")
    bundle.add_argument("--output-dir", default=None, help="Optional directory for emitted authoring artifacts.")
    bundle.add_argument("--json", action="store_true", help="Print JSON output.")

    preview = subparsers.add_parser("preview", help="Compile a Dashboards REST preview contract from a builder package.")
    preview.add_argument("--package", required=True, help="Path to build_package.json")
    preview.add_argument("--dashboard-id", default=None, help="Patch an existing dashboard id.")
    preview.add_argument("--clone-from-dashboard-id", default=None, help="Clone a baseline dashboard before patching.")
    preview.add_argument("--folder-id", default=None, help="Optional target folder id.")
    preview.add_argument(
        "--baseline-dashboard-json",
        default=None,
        help="Optional local dashboard JSON used to autofill preview mappings.",
    )
    preview.add_argument(
        "--autofill-live",
        action="store_true",
        help="Load dashboard metadata from the live org to autofill preview mappings.",
    )
    preview.add_argument(
        "--target-org",
        default=None,
        help="Target org alias/username for live preview autofill reads.",
    )
    preview.add_argument("--output-dir", default=None, help="Optional directory for emitted REST preview artifacts.")
    preview.add_argument("--json", action="store_true", help="Print JSON output.")

    verify = subparsers.add_parser(
        "verify",
        help="Verify a live or local dashboard payload against the packaged dashboard contract.",
    )
    verify.add_argument("--package", required=True, help="Path to build_package.json")
    verify.add_argument("--dashboard-id", default=None, help="Live dashboard id to verify.")
    verify.add_argument("--clone-from-dashboard-id", default=None, help="Optional baseline dashboard id used for autofill source context.")
    verify.add_argument("--folder-id", default=None, help="Optional target folder id override for expected contract compilation.")
    verify.add_argument(
        "--baseline-dashboard-json",
        default=None,
        help="Optional local dashboard JSON used to autofill expected contract mappings.",
    )
    verify.add_argument(
        "--actual-dashboard-json",
        default=None,
        help="Optional local dashboard JSON used as the actual verification target.",
    )
    verify.add_argument(
        "--manual-filter-authoring-json",
        default=None,
        help="Optional manual filter authoring artifact used to verify authored native dashboard filters.",
    )
    verify.add_argument(
        "--autofill-live",
        action="store_true",
        help="Load dashboard metadata from the live org to autofill expected contract mappings.",
    )
    verify.add_argument(
        "--target-org",
        default=None,
        help="Target org alias/username for live verification and/or autofill reads.",
    )
    verify.add_argument("--output-dir", default=None, help="Optional directory for emitted verify artifacts.")
    verify.add_argument("--json", action="store_true", help="Print JSON output.")

    apply = subparsers.add_parser(
        "apply",
        help="Preview or execute the packaged Dashboards REST request sequence.",
    )
    apply.add_argument("--package", required=True, help="Path to build_package.json")
    apply.add_argument("--dashboard-id", default=None, help="Patch an existing dashboard id.")
    apply.add_argument("--clone-from-dashboard-id", default=None, help="Clone a baseline dashboard before patching.")
    apply.add_argument("--folder-id", default=None, help="Optional target folder id.")
    apply.add_argument(
        "--baseline-dashboard-json",
        default=None,
        help="Optional local dashboard JSON used to autofill preview mappings.",
    )
    apply.add_argument(
        "--autofill-live",
        action="store_true",
        help="Load dashboard metadata from the live org to autofill preview mappings.",
    )
    apply.add_argument(
        "--target-org",
        default=None,
        help="Target org alias/username for live preview autofill reads and REST execution.",
    )
    apply.add_argument("--evaluation", default=None, help="Optional path to evaluation.json from the plan evaluator.")
    apply.add_argument(
        "--allow-missing-evaluation",
        action="store_true",
        help="Allow live dashboard mutation to continue without a pass evaluator verdict.",
    )
    apply.add_argument("--apply", action="store_true", help="Execute the REST request sequence instead of previewing it.")
    apply.add_argument("--output-dir", default=None, help="Optional directory for emitted apply preview/apply artifacts.")
    apply.add_argument("--json", action="store_true", help="Print JSON output.")

    complete = subparsers.add_parser(
        "complete",
        help="Apply the packaged dashboard, author all planned manual filters, and verify the live result.",
    )
    complete.add_argument("--package", required=True, help="Path to build_package.json")
    complete.add_argument("--dashboard-id", default=None, help="Patch an existing dashboard id.")
    complete.add_argument("--clone-from-dashboard-id", default=None, help="Clone a baseline dashboard before patching.")
    complete.add_argument("--folder-id", default=None, help="Optional target folder id.")
    complete.add_argument(
        "--baseline-dashboard-json",
        default=None,
        help="Optional local dashboard JSON used to autofill preview mappings.",
    )
    complete.add_argument(
        "--autofill-live",
        action="store_true",
        help="Load dashboard metadata from the live org to autofill preview mappings.",
    )
    complete.add_argument(
        "--target-org",
        default=None,
        help="Target org alias/username for live preview autofill reads, REST execution, and browser authoring.",
    )
    complete.add_argument("--evaluation", default=None, help="Optional path to evaluation.json from the plan evaluator.")
    complete.add_argument(
        "--allow-missing-evaluation",
        action="store_true",
        help="Allow live dashboard mutation to continue without a pass evaluator verdict.",
    )
    complete.add_argument("--session", required=True, help="Playwright session name for the manual filter authoring flow.")
    complete.add_argument(
        "--dashboard-filter-automation-script",
        default=str(DEFAULT_DASHBOARD_FILTER_AUTOMATION_SCRIPT),
        help="Path to salesforce_dashboard_filter_automation.py or a compatible browser helper.",
    )
    complete.add_argument("--output-dir", default=None, help="Optional directory for emitted apply/filter/verify artifacts.")
    complete.add_argument("--json", action="store_true", help="Print JSON output.")

    delete = subparsers.add_parser(
        "delete",
        help="Delete a live dashboard and confirm the dashboard no longer resolves.",
    )
    delete.add_argument("--dashboard-id", required=True, help="Live dashboard id to delete.")
    delete.add_argument("--target-org", required=True, help="Target org alias/username for live dashboard deletion.")
    delete.add_argument("--verify-attempts", type=int, default=5, help="Number of dashboard fetches to confirm deletion.")
    delete.add_argument(
        "--verify-delay-seconds",
        type=float,
        default=1.0,
        help="Delay between dashboard deletion verification attempts.",
    )
    delete.add_argument("--output-dir", default=None, help="Optional directory for emitted delete artifacts.")
    delete.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser


def _print_text(result: dict[str, Any]) -> None:
    print(f"status: {result['status']}")
    for message in result.get("messages", []):
        print(f"{message['level']}: {message['code']}: {message['text']}")
    if isinstance(result.get("review_artifact"), str) and result["review_artifact"]:
        print(f"review_artifact: {result['review_artifact']}")
    if isinstance(result.get("collection_landing_artifact"), str) and result["collection_landing_artifact"]:
        print(f"salesforce_dashboard_collection_landing_artifact: {result['collection_landing_artifact']}")
    if isinstance(result.get("browser_landing_artifact"), str) and result["browser_landing_artifact"]:
        print(f"ai_os_browser_landing_artifact: {result['browser_landing_artifact']}")
    if isinstance(result.get("browser_health_landing_artifact"), str) and result["browser_health_landing_artifact"]:
        print(f"ai_os_health_landing_artifact: {result['browser_health_landing_artifact']}")


def _emit_result(*, result: dict[str, Any], output_dir: Path | None, json_mode: bool) -> None:
    result = native_surface_browser.attach_native_surface_browser_artifacts(
        result=result,
        output_dir=output_dir,
        surface="dashboard",
        make_message=make_message,
    )
    if json_mode:
        print(json.dumps(result, indent=2))
    else:
        _print_text(result)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    runtime_output_dir = Path(getattr(args, "output_dir", "")) if getattr(args, "output_dir", None) else None

    def emit_result(result: dict[str, Any]) -> None:
        _emit_result(result=result, output_dir=runtime_output_dir, json_mode=args.json)

    if args.command == "delete":
        delete_path = f"/services/data/v{API_VERSION}/analytics/dashboards/{args.dashboard_id}"
        artifacts: list[dict[str, str]] = []
        try:
            delete_response = _run_rest_request(
                delete_path,
                target_org=args.target_org,
                method="DELETE",
            )
            delete_verification = _wait_for_dashboard_deletion(
                dashboard_id=args.dashboard_id,
                target_org=args.target_org,
                verify_attempts=args.verify_attempts,
                verify_delay_seconds=args.verify_delay_seconds,
            )
        except Exception as exc:
            result = make_result(
                status="error",
                command="delete",
                messages=[make_message("error", "delete_failed", str(exc)[:2000])],
                summary={"deleted_dashboard_id": args.dashboard_id},
                command_class="mutating",
            )
            emit_result(result)
            return 1

        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            delete_response_path = output_dir / "salesforce_dashboard_delete_response.json"
            delete_verify_path = output_dir / "salesforce_dashboard_delete_verify.json"
            delete_response_path.write_text(
                json.dumps(
                    {
                        "artifact_type": "salesforce_dashboard_delete_response",
                        "dashboard_id": args.dashboard_id,
                        "path": delete_path,
                        "response": delete_response,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            delete_verify_path.write_text(
                json.dumps(
                    {
                        "artifact_type": "salesforce_dashboard_delete_verify",
                        "dashboard_id": args.dashboard_id,
                        **delete_verification,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            artifacts.extend(
                [
                    {"type": "salesforce_dashboard_delete_response", "path": str(delete_response_path)},
                    {"type": "salesforce_dashboard_delete_verify", "path": str(delete_verify_path)},
                ]
            )

        deletion_verified = bool(delete_verification.get("deleted"))
        result = make_result(
            status="ok" if deletion_verified else "warn",
            command="delete",
            messages=[
                make_message("info", "delete_request_complete", f"Sent DELETE for dashboard {args.dashboard_id}."),
                make_message(
                    "info" if deletion_verified else "warn",
                    "delete_verified" if deletion_verified else "delete_verification_inconclusive",
                    "Confirmed the dashboard no longer resolves from the Dashboards REST endpoint."
                    if deletion_verified
                    else "Delete request completed but follow-on dashboard reads still resolved the dashboard.",
                ),
            ],
            artifacts=artifacts,
            summary={
                "deleted_dashboard_id": args.dashboard_id,
                "delete_verified": deletion_verified,
                "delete_verify_attempt_count": delete_verification.get("attempt_count"),
            },
            delete_response=delete_response,
            delete_verification=delete_verification,
            command_class="mutating",
        )
        emit_result(result)
        return 0 if deletion_verified else 1

    autofill_vocab = load_autofill_vocab()

    build_package = load_build_package(Path(args.package))
    planning_context = build_package.get("planning_context")
    if not isinstance(planning_context, dict):
        planning_context = None
    errors, warnings, summary = validate_build_package(build_package)

    if args.command == "validate":
        status = "error" if errors else ("warn" if warnings else "ok")
        result = make_result(
            status=status,
            command="validate",
            messages=[
                *[make_message("error", "invalid_build_package", item) for item in errors],
                *[make_message("warn", "build_package_warning", item) for item in warnings],
                make_message(
                    "info" if not errors else "error",
                    "validation_complete",
                    "Validated Salesforce dashboard package."
                    if not errors
                    else "Salesforce dashboard package validation failed.",
                ),
            ],
            summary=summary,
        )
        emit_result(result)
        return 1 if errors else 0

    if errors:
        result = make_result(
            status="error",
            command=args.command,
            messages=[make_message("error", "invalid_build_package", item) for item in errors],
            summary=summary,
        )
        emit_result(result)
        return 1

    if args.command == "bundle":
        authoring_bundle = build_dashboard_bundle(build_package)
        artifacts: list[dict[str, str]] = []
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            bundle_path = output_dir / "salesforce_dashboard_bundle.json"
            definition_path = output_dir / "salesforce_dashboard_definition.json"
            component_path = output_dir / "salesforce_dashboard_component_plan.json"
            bundle_path.write_text(json.dumps(authoring_bundle, indent=2), encoding="utf-8")
            definition_path.write_text(json.dumps(authoring_bundle["dashboard_definition"], indent=2), encoding="utf-8")
            component_path.write_text(json.dumps(authoring_bundle["component_plan"], indent=2), encoding="utf-8")
            artifacts.extend(
                [
                    {"type": "salesforce_dashboard_bundle", "path": str(bundle_path)},
                    {"type": "salesforce_dashboard_definition", "path": str(definition_path)},
                    {"type": "salesforce_dashboard_component_plan", "path": str(component_path)},
                ]
            )

        result = make_result(
            status="warn" if warnings else "ok",
            command="bundle",
            messages=[
                *[make_message("warn", "build_package_warning", item) for item in warnings],
                make_message("info", "bundle_ready", "Compiled Salesforce dashboard authoring bundle."),
            ],
            artifacts=artifacts,
            summary=summary,
            authoring_bundle=authoring_bundle,
        )
        emit_result(result)
        return 0

    if args.command == "verify":
        baseline_dashboard_json = Path(args.baseline_dashboard_json) if args.baseline_dashboard_json else None
        actual_dashboard_json = Path(args.actual_dashboard_json) if args.actual_dashboard_json else None
        manual_filter_authoring_json = (
            Path(args.manual_filter_authoring_json) if args.manual_filter_authoring_json else None
        )
        if not args.dashboard_id and actual_dashboard_json is None:
            result = make_result(
                status="error",
                command="verify",
                messages=[
                    make_message(
                        "error",
                        "verification_target_required",
                        "Provide --dashboard-id or --actual-dashboard-json for verify.",
                    )
                ],
                summary=summary,
            )
            emit_result(result)
            return 1

        try:
            preview, fill_requirements, preview_summary, command_class, autofill_summary = prepare_dashboard_preview(
                build_package=build_package,
                dashboard_id=args.dashboard_id,
                clone_from_dashboard_id=args.clone_from_dashboard_id,
                folder_id=args.folder_id,
                baseline_dashboard_json=baseline_dashboard_json,
                autofill_live=args.autofill_live,
                target_org=args.target_org,
                autofill_vocab=autofill_vocab,
            )
            actual_dashboard_payload = load_dashboard_metadata(
                dashboard_id=args.dashboard_id,
                target_org=args.target_org,
                baseline_dashboard_json=actual_dashboard_json,
            )
            manual_filter_artifact = _load_optional_json_file(manual_filter_authoring_json)
            if manual_filter_artifact is None and preview.get("manual_filter_intents"):
                manual_filter_artifact = _build_dashboard_manual_filter_authoring_artifact(
                    build_package=build_package,
                    preview=preview,
                    autofill_summary=autofill_summary,
                    target_org=args.target_org,
                    candidate_clone_baselines=preview.get("candidate_clone_baselines"),
                    baseline_strategy=preview.get("baseline_strategy"),
                )
            if actual_dashboard_payload is None:
                raise ValueError("Unable to load the actual dashboard payload for verify.")
            if actual_dashboard_json is None and args.dashboard_id and args.target_org:
                command_class = "live_read"
        except ValueError as exc:
            result = make_result(
                status="error",
                command="verify",
                messages=[make_message("error", "verify_setup_failed", str(exc))],
                summary=summary,
                command_class="live_read" if args.autofill_live or (args.dashboard_id and args.target_org) else "read_only",
            )
            emit_result(result)
            return 1
        except Exception as exc:
            result = make_result(
                status="error",
                command="verify",
                messages=[make_message("error", "verify_load_failed", str(exc))],
                summary=summary,
                command_class="live_read" if args.autofill_live or (args.dashboard_id and args.target_org) else "read_only",
            )
            emit_result(result)
            return 1

        findings, expected_contract, actual_contract, manual_filter_verification = verify_dashboard_contract(
            preview=preview,
            dashboard_payload=actual_dashboard_payload,
            manual_filter_artifact=manual_filter_artifact,
        )
        verify_summary = actual_contract.pop("summary")
        verify_artifact = {
            "artifact_type": "salesforce_dashboard_verify",
            "target_dashboard_id": args.dashboard_id,
            "expected_contract": expected_contract,
            "actual_contract": actual_contract,
            "fill_requirements": fill_requirements,
            "findings": findings,
            "manual_filter_verification": manual_filter_verification,
            "summary": verify_summary,
        }
        artifacts: list[dict[str, str]] = []
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            verify_path = output_dir / "salesforce_dashboard_verify.json"
            verify_path.write_text(json.dumps(verify_artifact, indent=2), encoding="utf-8")
            artifacts.append({"type": "salesforce_dashboard_verify", "path": str(verify_path)})

        status = "warn" if any(item["level"] == "warn" for item in findings) else "ok"
        result = make_result(
            status=status,
            command="verify",
            messages=[
                *[
                    make_message(item["level"], item["code"], item["text"])
                    for item in findings
                ],
                make_message(
                    "warn" if status == "warn" else "info",
                    "verify_complete",
                    "Verified the dashboard against the packaged contract with findings."
                    if status == "warn"
                    else "Verified the dashboard against the packaged contract with no blocking mismatches.",
                ),
            ],
            artifacts=artifacts,
            summary={**summary, **preview_summary, **verify_summary},
            command_class=command_class,
            expected_contract=expected_contract,
            actual_contract=actual_contract,
            fill_requirements=fill_requirements,
            findings=findings,
            autofill_summary=autofill_summary,
            manual_filter_verification=manual_filter_verification,
        )
        emit_result(result)
        return 0

    if args.command == "complete":
        if not args.target_org:
            result = make_result(
                status="error",
                command="complete",
                messages=[make_message("error", "target_org_required", "--target-org is required for complete.")],
                summary=summary,
                command_class="mutating",
            )
            emit_result(result)
            return 1

        automation_script = Path(args.dashboard_filter_automation_script)
        if not automation_script.exists():
            result = make_result(
                status="error",
                command="complete",
                messages=[
                    make_message(
                        "error",
                        "dashboard_filter_automation_script_missing",
                        f"Dashboard filter automation script not found: {automation_script}",
                    )
                ],
                summary=summary,
                command_class="mutating",
            )
            emit_result(result)
            return 1

        output_root = Path(args.output_dir) if args.output_dir else None
        apply_output_dir = output_root / "01_apply" if output_root else None
        flow_output_dir = output_root / "02_filter_flow" if output_root else None
        direct_verify_output_dir = output_root / "02_verify" if output_root else None
        complete_memory_context = _derive_dashboard_memory_context(
            build_package=build_package,
            planning_context=planning_context,
            evaluation_gate=None,
            output_dir=output_root,
            package_path=Path(args.package),
            command="complete",
        )

        apply_command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "apply",
            "--package",
            str(Path(args.package)),
            "--target-org",
            args.target_org,
            "--apply",
            "--json",
        ]
        if args.dashboard_id:
            apply_command.extend(["--dashboard-id", args.dashboard_id])
        if args.clone_from_dashboard_id:
            apply_command.extend(["--clone-from-dashboard-id", args.clone_from_dashboard_id])
        if args.folder_id:
            apply_command.extend(["--folder-id", args.folder_id])
        if args.baseline_dashboard_json:
            apply_command.extend(["--baseline-dashboard-json", args.baseline_dashboard_json])
        if args.autofill_live:
            apply_command.append("--autofill-live")
        if args.evaluation:
            apply_command.extend(["--evaluation", args.evaluation])
        if args.allow_missing_evaluation:
            apply_command.append("--allow-missing-evaluation")
        if apply_output_dir is not None:
            apply_output_dir.mkdir(parents=True, exist_ok=True)
            apply_command.extend(["--output-dir", str(apply_output_dir)])

        try:
            apply_exit_code, apply_result = _run_json_command(apply_command)
        except Exception as exc:
            result = make_result(
                status="error",
                command="complete",
                messages=[make_message("error", "complete_apply_failed", str(exc))],
                summary=summary,
                command_class="mutating",
            )
            emit_result(result)
            return 1

        if apply_exit_code != 0 or apply_result.get("status") == "error":
            result = make_result(
                status="error",
                command="complete",
                messages=[
                    *apply_result.get("messages", []),
                    make_message("error", "complete_apply_failed", "Native dashboard apply failed before browser authoring."),
                ],
                artifacts=apply_result.get("artifacts", []),
                summary={
                    **summary,
                    "apply_status": apply_result.get("status"),
                },
                apply_result=apply_result,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=complete_memory_context,
                command="complete",
                evaluation_gate=apply_result.get("evaluation_gate"),
            )
            emit_result(result)
            return 1

        applied_dashboard_id = (apply_result.get("applied_dashboard") or {}).get("id")
        if not isinstance(applied_dashboard_id, str) or not applied_dashboard_id:
            result = make_result(
                status="error",
                command="complete",
                messages=[
                    *apply_result.get("messages", []),
                    make_message("error", "missing_applied_dashboard_id", "Apply completed without an applied dashboard id."),
                ],
                artifacts=apply_result.get("artifacts", []),
                summary={**summary, "apply_status": apply_result.get("status")},
                apply_result=apply_result,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=complete_memory_context,
                command="complete",
                evaluation_gate=apply_result.get("evaluation_gate"),
            )
            emit_result(result)
            return 1

        plan_path = _artifact_path_from_result(apply_result, "salesforce_dashboard_filter_automation_plan")
        manual_filter_authoring_path = _artifact_path_from_result(apply_result, "salesforce_dashboard_manual_filter_authoring")

        if plan_path is not None:
            if flow_output_dir is not None:
                flow_output_dir.mkdir(parents=True, exist_ok=True)
            verify_output_dir = (flow_output_dir / "09_verify_dashboard") if flow_output_dir is not None else None
            flow_command = [
                sys.executable,
                str(automation_script),
                "run-filter-flow",
                "--plan",
                plan_path,
                "--target-org",
                args.target_org,
                "--dashboard-id",
                applied_dashboard_id,
                "--all-filters",
                "--through",
                "verify-dashboard",
                "--verify-package",
                str(Path(args.package)),
                "--session",
                args.session,
                "--json",
            ]
            if manual_filter_authoring_path is not None:
                flow_command.extend(["--manual-filter-authoring-json", manual_filter_authoring_path])
            if flow_output_dir is not None:
                flow_command.extend(["--output-dir", str(flow_output_dir)])
            if verify_output_dir is not None:
                flow_command.extend(["--verify-output-dir", str(verify_output_dir)])

            try:
                flow_exit_code, filter_flow_result = _run_json_command(flow_command)
            except Exception as exc:
                result = make_result(
                    status="error",
                    command="complete",
                    messages=[
                        *apply_result.get("messages", []),
                        make_message("error", "complete_filter_flow_failed", str(exc)),
                    ],
                    artifacts=apply_result.get("artifacts", []),
                    summary={
                        **summary,
                        "apply_status": apply_result.get("status"),
                        "applied_dashboard_id": applied_dashboard_id,
                    },
                    apply_result=apply_result,
                    command_class="mutating",
                )
                result = _attach_memory_record(
                    result=result,
                    planning_context=complete_memory_context,
                    command="complete",
                    evaluation_gate=apply_result.get("evaluation_gate"),
                )
                emit_result(result)
                return 1

            result = make_result(
                status=filter_flow_result.get("status", "error"),
                command="complete",
                messages=[
                    *apply_result.get("messages", []),
                    *filter_flow_result.get("messages", []),
                ],
                artifacts=[
                    *(apply_result.get("artifacts") or []),
                    *(filter_flow_result.get("artifacts") or []),
                ],
                summary={
                    **summary,
                    "apply_status": apply_result.get("status"),
                    "filter_flow_status": filter_flow_result.get("status"),
                    "applied_dashboard_id": applied_dashboard_id,
                    "authored_filter_count": (filter_flow_result.get("summary") or {}).get("authored_filter_count"),
                    "manual_filter_verified_count": (filter_flow_result.get("summary") or {}).get("manual_filter_verified_count"),
                },
                apply_result=apply_result,
                filter_flow_result=filter_flow_result,
                applied_dashboard={
                    "id": applied_dashboard_id,
                    "name": (apply_result.get("applied_dashboard") or {}).get("name"),
                },
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=complete_memory_context,
                command="complete",
                evaluation_gate=apply_result.get("evaluation_gate"),
            )
            emit_result(result)
            return 0 if flow_exit_code == 0 and filter_flow_result.get("status") != "error" else 1

        verify_command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "verify",
            "--package",
            str(Path(args.package)),
            "--dashboard-id",
            applied_dashboard_id,
            "--target-org",
            args.target_org,
            "--json",
        ]
        if args.autofill_live:
            verify_command.append("--autofill-live")
        if direct_verify_output_dir is not None:
            direct_verify_output_dir.mkdir(parents=True, exist_ok=True)
            verify_command.extend(["--output-dir", str(direct_verify_output_dir)])

        try:
            verify_exit_code, verify_result = _run_json_command(verify_command)
        except Exception as exc:
            result = make_result(
                status="error",
                command="complete",
                messages=[
                    *apply_result.get("messages", []),
                    make_message("error", "complete_verify_failed", str(exc)),
                ],
                artifacts=apply_result.get("artifacts", []),
                summary={
                    **summary,
                    "apply_status": apply_result.get("status"),
                    "applied_dashboard_id": applied_dashboard_id,
                },
                apply_result=apply_result,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=complete_memory_context,
                command="complete",
                evaluation_gate=apply_result.get("evaluation_gate"),
            )
            emit_result(result)
            return 1

        result = make_result(
            status=verify_result.get("status", "error"),
            command="complete",
            messages=[
                *apply_result.get("messages", []),
                *verify_result.get("messages", []),
            ],
            artifacts=[
                *(apply_result.get("artifacts") or []),
                *(verify_result.get("artifacts") or []),
            ],
            summary={
                **summary,
                "apply_status": apply_result.get("status"),
                "verify_status": verify_result.get("status"),
                "applied_dashboard_id": applied_dashboard_id,
            },
            apply_result=apply_result,
            verify_result=verify_result,
            applied_dashboard={
                "id": applied_dashboard_id,
                "name": (apply_result.get("applied_dashboard") or {}).get("name"),
            },
            command_class="mutating",
        )
        result = _attach_memory_record(
            result=result,
            planning_context=complete_memory_context,
            command="complete",
            evaluation_gate=apply_result.get("evaluation_gate"),
        )
        emit_result(result)
        return 0 if verify_exit_code == 0 and verify_result.get("status") != "error" else 1

    baseline_dashboard_json = Path(args.baseline_dashboard_json) if args.baseline_dashboard_json else None
    try:
        preview, fill_requirements, preview_summary, command_class, autofill_summary = prepare_dashboard_preview(
            build_package=build_package,
            dashboard_id=args.dashboard_id,
            clone_from_dashboard_id=args.clone_from_dashboard_id,
            folder_id=args.folder_id,
            baseline_dashboard_json=baseline_dashboard_json,
            autofill_live=args.autofill_live,
            target_org=args.target_org,
            autofill_vocab=autofill_vocab,
        )
    except ValueError as exc:
        result = make_result(
            status="error",
            command=args.command,
            messages=[make_message("error", "target_org_required", str(exc))],
            summary=summary,
            command_class="live_read" if args.autofill_live else "read_only",
        )
        emit_result(result)
        return 1
    except Exception as exc:
        result = make_result(
            status="error",
            command=args.command,
            messages=[make_message("error", "autofill_load_failed", str(exc))],
            summary=summary,
            command_class="live_read" if args.autofill_live else "read_only",
        )
        emit_result(result)
        return 1

    if args.command == "apply":
        output_dir = Path(args.output_dir) if args.output_dir else None
        evaluation_path = Path(args.evaluation) if args.evaluation else None
        evaluation_gate, gate_messages, gate_error = _resolve_dashboard_evaluation_gate(
            build_package=build_package,
            evaluation_path=evaluation_path,
            require_pass=args.apply,
            allow_missing=args.allow_missing_evaluation,
        )
        apply_memory_context = _derive_dashboard_memory_context(
            build_package=build_package,
            planning_context=planning_context,
            evaluation_gate=evaluation_gate,
            output_dir=output_dir,
            package_path=Path(args.package),
            command="apply",
        )
        if gate_error is not None:
            result = make_result(
                status="error",
                command="apply",
                messages=[make_message("error", gate_error["code"], gate_error["text"])],
                summary=summary,
                command_class="mutating" if args.apply else "read_only",
            )
            if args.apply:
                result = _attach_memory_record(
                    result=result,
                    planning_context=apply_memory_context,
                    command="apply",
                    evaluation_gate=evaluation_gate,
                )
            emit_result(result)
            return 1

        external_fill_requirements = _external_fill_requirements(fill_requirements)
        apply_ready = not external_fill_requirements
        apply_summary = {
            "mode": "apply" if args.apply else "dry_run",
            "strategy": preview_summary.get("strategy"),
            "request_count": len(preview.get("requests") or []),
            "fill_requirement_count": len(fill_requirements),
            "external_fill_requirement_count": len(external_fill_requirements),
            "internal_fill_requirement_count": len(fill_requirements) - len(external_fill_requirements),
            "manual_filter_intent_count": preview_summary.get("manual_filter_intent_count", 0),
            "apply_ready": apply_ready,
            "evaluation_verdict": (evaluation_gate or {}).get("verdict"),
            "evaluation_bypassed": bool((evaluation_gate or {}).get("bypassed")),
        }
        request_preview = {
            "artifact_type": "salesforce_dashboard_apply_preview",
            "mode": apply_summary["mode"],
            "target_org": args.target_org,
            "strategy": apply_summary["strategy"],
            "requests": preview.get("requests") or [],
            "fill_requirements": fill_requirements,
            "external_fill_requirements": external_fill_requirements,
            "manual_filter_intents": preview.get("manual_filter_intents", []),
        }
        artifacts: list[dict[str, str]] = []
        manual_filter_authoring_artifact = _build_dashboard_manual_filter_authoring_artifact(
            build_package=build_package,
            preview=preview,
            autofill_summary=autofill_summary,
            target_org=args.target_org,
            candidate_clone_baselines=preview.get("candidate_clone_baselines"),
            baseline_strategy=preview.get("baseline_strategy"),
        )
        manual_filter_playbook = (
            _build_dashboard_manual_filter_playbook(
                build_package=build_package,
                manual_filter_authoring_artifact=manual_filter_authoring_artifact,
            )
            if manual_filter_authoring_artifact is not None
            else None
        )
        manual_filter_automation_plan = (
            _build_dashboard_manual_filter_automation_plan(
                manual_filter_authoring_artifact=manual_filter_authoring_artifact,
                manual_filter_playbook=manual_filter_playbook,
            )
            if manual_filter_playbook is not None
            else None
        )
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            preview_path = output_dir / "salesforce_dashboard_apply_preview.json"
            preview_path.write_text(json.dumps(request_preview, indent=2), encoding="utf-8")
            artifacts.append({"type": "salesforce_dashboard_apply_preview", "path": str(preview_path)})
            _append_evaluation_bypass_artifact(
                output_dir=output_dir,
                artifacts=artifacts,
                command="apply",
                target_org=args.target_org,
                evaluation_gate=evaluation_gate,
                summary=apply_summary,
            )
            manual_filter_playbook = _write_manual_filter_artifacts(
                output_dir=output_dir,
                artifacts=artifacts,
                manual_filter_authoring_artifact=manual_filter_authoring_artifact,
                manual_filter_playbook=manual_filter_playbook,
                manual_filter_automation_plan=manual_filter_automation_plan,
            )
            candidate_baselines = preview.get("candidate_clone_baselines")
            if isinstance(candidate_baselines, list) and candidate_baselines:
                baseline_candidates_path = output_dir / "salesforce_dashboard_baseline_candidates.json"
                baseline_candidates_path.write_text(
                    json.dumps(
                        {
                            "artifact_type": "salesforce_dashboard_baseline_candidates",
                            "resolved_clone_baseline": preview.get("resolved_clone_baseline"),
                            "baseline_strategy": preview.get("baseline_strategy"),
                            "candidates": candidate_baselines,
                        },
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                artifacts.append({"type": "salesforce_dashboard_baseline_candidates", "path": str(baseline_candidates_path)})

        if args.apply and not args.target_org:
            result = make_result(
                status="error",
                command="apply",
                messages=[
                    *gate_messages,
                    make_message("error", "target_org_required", "--target-org is required with --apply."),
                ],
                artifacts=artifacts,
                summary={**summary, **preview_summary},
                apply_summary=apply_summary,
                request_preview=request_preview,
                fill_requirements=fill_requirements,
                manual_filter_authoring=manual_filter_authoring_artifact,
                manual_filter_playbook=manual_filter_playbook,
                manual_filter_automation_plan=manual_filter_automation_plan,
                candidate_clone_baselines=preview.get("candidate_clone_baselines", []),
                evaluation_gate=evaluation_gate,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=apply_memory_context,
                command="apply",
                evaluation_gate=evaluation_gate,
            )
            emit_result(result)
            return 1

        if not apply_ready:
            result = make_result(
                status="error" if args.apply else "warn",
                command="apply",
                messages=[
                    *gate_messages,
                    make_message(
                        "error" if args.apply else "warn",
                        "apply_blocked",
                        "The dashboard request sequence still has unresolved external fill requirements.",
                    ),
                    *[
                        make_message(
                            "warn",
                            "external_fill_requirement",
                            f"{item['category']}: {item.get('source_label') or item['target_path']}",
                        )
                        for item in external_fill_requirements
                    ],
                ],
                artifacts=artifacts,
                summary={**summary, **preview_summary},
                apply_summary=apply_summary,
                request_preview=request_preview,
                fill_requirements=fill_requirements,
                autofill_summary=autofill_summary,
                manual_filter_authoring=manual_filter_authoring_artifact,
                manual_filter_playbook=manual_filter_playbook,
                manual_filter_automation_plan=manual_filter_automation_plan,
                baseline_strategy=preview.get("baseline_strategy"),
                candidate_clone_baselines=preview.get("candidate_clone_baselines", []),
                evaluation_gate=evaluation_gate,
                command_class="mutating" if args.apply else command_class,
            )
            if args.apply:
                result = _attach_memory_record(
                    result=result,
                    planning_context=apply_memory_context,
                    command="apply",
                    evaluation_gate=evaluation_gate,
                )
            emit_result(result)
            return 1 if args.apply else 0

        if not args.apply:
            result = make_result(
                status="ok",
                command="apply",
                messages=[
                    *gate_messages,
                    make_message(
                        "info",
                        "apply_preview_ready",
                        "Salesforce dashboard REST apply preview is ready; only clone-response substitution remains internal.",
                    )
                ],
                artifacts=artifacts,
                summary={**summary, **preview_summary},
                apply_summary=apply_summary,
                request_preview=request_preview,
                fill_requirements=fill_requirements,
                autofill_summary=autofill_summary,
                manual_filter_authoring=manual_filter_authoring_artifact,
                manual_filter_playbook=manual_filter_playbook,
                manual_filter_automation_plan=manual_filter_automation_plan,
                baseline_strategy=preview.get("baseline_strategy"),
                candidate_clone_baselines=preview.get("candidate_clone_baselines", []),
                evaluation_gate=evaluation_gate,
                command_class=command_class,
            )
            emit_result(result)
            return 0

        try:
            execution_requests, responses, cloned_dashboard_id = execute_dashboard_requests(
                preview,
                target_org=args.target_org,
            )
        except Exception as exc:
            result = make_result(
                status="error",
                command="apply",
                messages=[*gate_messages, make_message("error", "apply_failed", str(exc)[:2000])],
                artifacts=artifacts,
                summary={**summary, **preview_summary},
                apply_summary=apply_summary,
                request_preview=request_preview,
                fill_requirements=fill_requirements,
                autofill_summary=autofill_summary,
                evaluation_gate=evaluation_gate,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=apply_memory_context,
                command="apply",
                evaluation_gate=evaluation_gate,
            )
            emit_result(result)
            return 1

        applied_dashboard = responses[-1] if responses else {}
        manual_filter_authoring_artifact = _build_dashboard_manual_filter_authoring_artifact(
            build_package=build_package,
            preview=preview,
            autofill_summary=autofill_summary,
            target_org=args.target_org,
            applied_dashboard_id=applied_dashboard.get("dashboard_id") or cloned_dashboard_id,
            candidate_clone_baselines=preview.get("candidate_clone_baselines"),
            baseline_strategy=preview.get("baseline_strategy"),
        )
        manual_filter_playbook = (
            _build_dashboard_manual_filter_playbook(
                build_package=build_package,
                manual_filter_authoring_artifact=manual_filter_authoring_artifact,
            )
            if manual_filter_authoring_artifact is not None
            else None
        )
        manual_filter_automation_plan = (
            _build_dashboard_manual_filter_automation_plan(
                manual_filter_authoring_artifact=manual_filter_authoring_artifact,
                manual_filter_playbook=manual_filter_playbook,
            )
            if manual_filter_playbook is not None
            else None
        )
        if args.output_dir:
            output_dir = Path(args.output_dir)
            manual_filter_playbook = _write_manual_filter_artifacts(
                output_dir=output_dir,
                artifacts=artifacts,
                manual_filter_authoring_artifact=manual_filter_authoring_artifact,
                manual_filter_playbook=manual_filter_playbook,
                manual_filter_automation_plan=manual_filter_automation_plan,
            )
        if args.output_dir:
            output_dir = Path(args.output_dir)
            apply_path = output_dir / "salesforce_dashboard_apply_response.json"
            apply_payload = {
                "artifact_type": "salesforce_dashboard_apply_response",
                "execution_requests": execution_requests,
                "responses": responses,
                "cloned_dashboard_id": cloned_dashboard_id,
            }
            apply_path.write_text(json.dumps(apply_payload, indent=2), encoding="utf-8")
            artifacts.append({"type": "salesforce_dashboard_apply_response", "path": str(apply_path)})

        result = make_result(
            status="ok",
            command="apply",
            messages=[
                *gate_messages,
                make_message(
                    "info",
                    "apply_complete",
                    f"Applied the packaged dashboard REST sequence to dashboard {applied_dashboard.get('dashboard_id') or cloned_dashboard_id}.",
                )
            ],
            artifacts=artifacts,
            summary={**summary, **preview_summary},
            apply_summary=apply_summary,
            request_preview=request_preview,
            fill_requirements=fill_requirements,
            autofill_summary=autofill_summary,
            manual_filter_authoring=manual_filter_authoring_artifact,
            manual_filter_playbook=manual_filter_playbook,
            manual_filter_automation_plan=manual_filter_automation_plan,
            baseline_strategy=preview.get("baseline_strategy"),
            candidate_clone_baselines=preview.get("candidate_clone_baselines", []),
            evaluation_gate=evaluation_gate,
            execution_requests=execution_requests,
            responses=responses,
            applied_dashboard={
                "id": applied_dashboard.get("dashboard_id") or cloned_dashboard_id,
                "name": applied_dashboard.get("name"),
            },
            command_class="mutating",
        )
        result = _attach_memory_record(
            result=result,
            planning_context=apply_memory_context,
            command="apply",
            evaluation_gate=evaluation_gate,
        )
        emit_result(result)
        return 0

    artifacts: list[dict[str, str]] = []
    manual_filter_authoring_artifact = _build_dashboard_manual_filter_authoring_artifact(
        build_package=build_package,
        preview=preview,
        autofill_summary=autofill_summary,
        target_org=args.target_org,
        candidate_clone_baselines=preview.get("candidate_clone_baselines"),
        baseline_strategy=preview.get("baseline_strategy"),
    )
    manual_filter_playbook = (
        _build_dashboard_manual_filter_playbook(
            build_package=build_package,
            manual_filter_authoring_artifact=manual_filter_authoring_artifact,
        )
        if manual_filter_authoring_artifact is not None
        else None
    )
    manual_filter_automation_plan = (
        _build_dashboard_manual_filter_automation_plan(
            manual_filter_authoring_artifact=manual_filter_authoring_artifact,
            manual_filter_playbook=manual_filter_playbook,
        )
        if manual_filter_playbook is not None
        else None
    )
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        preview_path = output_dir / "salesforce_dashboard_rest_preview.json"
        fill_path = output_dir / "salesforce_dashboard_fill_requirements.json"
        autofill_path = output_dir / "salesforce_dashboard_autofill_summary.json"
        preview_path.write_text(json.dumps(preview, indent=2), encoding="utf-8")
        fill_path.write_text(json.dumps({"fill_requirements": fill_requirements}, indent=2), encoding="utf-8")
        artifacts.extend(
            [
                {"type": "salesforce_dashboard_rest_preview", "path": str(preview_path)},
                {"type": "salesforce_dashboard_fill_requirements", "path": str(fill_path)},
            ]
        )
        if autofill_summary is not None:
            autofill_path.write_text(json.dumps(autofill_summary, indent=2), encoding="utf-8")
            artifacts.append({"type": "salesforce_dashboard_autofill_summary", "path": str(autofill_path)})
        manual_filter_playbook = _write_manual_filter_artifacts(
            output_dir=output_dir,
            artifacts=artifacts,
            manual_filter_authoring_artifact=manual_filter_authoring_artifact,
            manual_filter_playbook=manual_filter_playbook,
            manual_filter_automation_plan=manual_filter_automation_plan,
        )
        candidate_baselines = preview.get("candidate_clone_baselines")
        if isinstance(candidate_baselines, list) and candidate_baselines:
            baseline_candidates_path = output_dir / "salesforce_dashboard_baseline_candidates.json"
            baseline_candidates_path.write_text(
                json.dumps(
                    {
                        "artifact_type": "salesforce_dashboard_baseline_candidates",
                        "resolved_clone_baseline": preview.get("resolved_clone_baseline"),
                        "baseline_strategy": preview.get("baseline_strategy"),
                        "candidates": candidate_baselines,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            artifacts.append({"type": "salesforce_dashboard_baseline_candidates", "path": str(baseline_candidates_path)})

    result = make_result(
        status="warn" if fill_requirements or warnings or preview_summary.get("manual_filter_intent_count") else "ok",
        command="preview",
        messages=[
            *[make_message("warn", "build_package_warning", item) for item in warnings],
            *(
                [
                    make_message(
                        "warn",
                        "manual_filter_intent",
                        f"Preserved {preview_summary['manual_filter_intent_count']} manual native-dashboard filter intent(s) outside the REST payload.",
                    ),
                    make_message(
                        "info",
                        "manual_filter_authoring_ready",
                        "Wrote a native dashboard manual filter authoring artifact with validated proposals and baseline guidance.",
                    ),
                    make_message(
                        "info",
                        "manual_filter_playbook_ready",
                        "Wrote a native dashboard filter playbook artifact with explicit UI authoring steps.",
                    ),
                    make_message(
                        "info",
                        "manual_filter_automation_plan_ready",
                        "Wrote a browser-automation plan artifact for the post-clone native filter flow.",
                    ),
                    *(
                        [
                            make_message(
                                "info",
                                "candidate_clone_baselines_ready",
                                f"Found {preview_summary['candidate_clone_baseline_count']} alternative filter-bearing clone baseline candidate(s).",
                            ),
                            *(
                                [
                                    make_message(
                                        "info",
                                        "baseline_strategy",
                                        str((preview.get("baseline_strategy") or {}).get("summary")),
                                    )
                                ]
                                if (preview.get("baseline_strategy") or {}).get("summary")
                                else []
                            ),
                        ]
                        if preview_summary.get("candidate_clone_baseline_count")
                        else []
                    ),
                    *(
                        [
                            make_message(
                                "info",
                                "baseline_strategy",
                                str((preview.get("baseline_strategy") or {}).get("summary")),
                            )
                        ]
                        if not preview_summary.get("candidate_clone_baseline_count")
                        and (preview.get("baseline_strategy") or {}).get("summary")
                        else []
                    ),
                ]
                if preview_summary.get("manual_filter_intent_count")
                else []
            ),
            make_message(
                "warn" if fill_requirements or preview_summary.get("manual_filter_intent_count") else "info",
                "rest_preview_ready",
                "Compiled a Salesforce dashboard REST preview; fill the unresolved mappings and review the preserved manual filter intent before live use."
                if fill_requirements or preview_summary.get("manual_filter_intent_count")
                else "Compiled a Salesforce dashboard REST preview with no unresolved mappings.",
            ),
        ],
        artifacts=artifacts,
        command_class=command_class,
        summary={**summary, **preview_summary},
        rest_preview=preview,
        fill_requirements=fill_requirements,
        autofill_summary=autofill_summary,
        manual_filter_authoring=manual_filter_authoring_artifact,
        manual_filter_playbook=manual_filter_playbook,
        manual_filter_automation_plan=manual_filter_automation_plan,
        baseline_strategy=preview.get("baseline_strategy"),
        candidate_clone_baselines=preview.get("candidate_clone_baselines", []),
    )
    emit_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
