#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PLAYWRIGHT_WRAPPER = Path.home() / ".codex" / "skills" / "playwright" / "scripts" / "playwright_cli.sh"
DASHBOARD_EXECUTOR_SCRIPT = ROOT / "scripts" / "salesforce_dashboard_executor.py"
SNAPSHOT_REF_RE = re.compile(r"\[ref=([^\]]+)\]")
FRONTDOOR_SPLIT = "/secur/frontdoor.jsp"
URL_RE = re.compile(r"https://\S+")
SNAPSHOT_LINK_RE = re.compile(r"\[Snapshot\]\(([^)]+)\)")
BUTTON_TEXT_RE = re.compile(r'button "([^"]+)"', re.IGNORECASE)
EDITOR_BLOCKING_PATTERNS = [
    ("entity_deleted", "entity is deleted", "The dashboard editor reports that the target dashboard entity is deleted."),
    ("insufficient_access", "insufficient privileges", "The dashboard editor reports insufficient access for this dashboard."),
    ("page_error", "looks like there's a problem", "The dashboard editor surfaced a generic page problem message."),
]


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
        "tool": "salesforce_dashboard_filter_automation",
        "lane": "native_surface_authoring",
        "command_class": command_class,
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _validate_plan_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    if payload.get("artifact_type") != "salesforce_dashboard_filter_automation_plan":
        messages.append(
            make_message(
                "error",
                "invalid_artifact_type",
                "Expected artifact_type salesforce_dashboard_filter_automation_plan.",
            )
        )
    if not isinstance(payload.get("filter_actions"), list) or not payload.get("filter_actions"):
        messages.append(
            make_message(
                "error",
                "missing_filter_actions",
                "Automation plan must include at least one filter action.",
            )
        )
    if not isinstance(payload.get("preflight_actions"), list) or not payload.get("preflight_actions"):
        messages.append(
            make_message(
                "error",
                "missing_preflight_actions",
                "Automation plan must include preflight actions.",
            )
        )
    if not isinstance(payload.get("post_actions"), list) or not payload.get("post_actions"):
        messages.append(
            make_message(
                "error",
                "missing_post_actions",
                "Automation plan must include post actions.",
            )
        )
    if not payload.get("relative_edit_route_template"):
        messages.append(
            make_message(
                "error",
                "missing_edit_route_template",
                "Automation plan must include relative_edit_route_template.",
            )
        )
    return messages


def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def _require_npx() -> None:
    result = subprocess.run(
        ["bash", "-lc", "command -v npx >/dev/null 2>&1"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError("npx is required for the Playwright wrapper but is not available in PATH.")


def _resolve_frontdoor_url(target_org: str) -> str:
    result = _run_command(["sf", "org", "open", "--url-only", "--target-org", target_org])
    if result.returncode != 0 or not result.stdout.strip():
        message = (result.stderr or result.stdout or "Unable to resolve org frontdoor URL.").strip()
        raise RuntimeError(message)
    matches = URL_RE.findall(result.stdout)
    if not matches:
        raise RuntimeError("Unable to parse frontdoor URL from sf org open output.")
    return matches[-1].rstrip()


def _base_url_from_frontdoor(frontdoor_url: str) -> str:
    if FRONTDOOR_SPLIT not in frontdoor_url:
        raise RuntimeError("Unexpected frontdoor URL format; could not derive instance base URL.")
    return frontdoor_url.split(FRONTDOOR_SPLIT, 1)[0]


def _run_playwright(
    *,
    playwright_wrapper: Path,
    session: str,
    command: list[str],
) -> subprocess.CompletedProcess[str]:
    return _run_command(["bash", str(playwright_wrapper), f"-s={session}", *command])


def _extract_candidate_refs(snapshot_text: str, labels: list[str]) -> dict[str, list[dict[str, str]]]:
    candidates: dict[str, list[dict[str, str]]] = {}
    lines = snapshot_text.splitlines()
    for label in labels:
        matches: list[dict[str, str]] = []
        for line in lines:
            if label.lower() not in line.lower():
                continue
            ref_match = SNAPSHOT_REF_RE.search(line)
            if not ref_match:
                continue
            matches.append(
                {
                    "ref": ref_match.group(1),
                    "line": line.strip(),
                    "disabled": "[disabled]" in line.lower(),
                }
            )
        if matches:
            candidates[label] = matches
    return candidates


def _resolve_snapshot_text(snapshot_stdout: str) -> str:
    link_match = SNAPSHOT_LINK_RE.search(snapshot_stdout)
    if not link_match:
        return snapshot_stdout
    linked_path = (ROOT / link_match.group(1)).resolve()
    if linked_path.exists():
        return linked_path.read_text(encoding="utf-8")
    return snapshot_stdout


def _snapshot_artifact_path(artifact_path: Path, artifact_type: str) -> Path:
    sibling_name = {
        "salesforce_dashboard_filter_value": "salesforce_dashboard_filter_value_snapshot.yml",
    }.get(artifact_type)
    if sibling_name is None:
        raise RuntimeError(f"No snapshot sibling mapping registered for artifact type {artifact_type}.")
    return artifact_path.with_name(sibling_name)


def _find_named_ref(snapshot_text: str, *, role: str, name: str) -> str:
    pattern = re.compile(rf'- {re.escape(role)} "{re.escape(name)}".*?\[ref=([^\]]+)\]')
    match = pattern.search(snapshot_text)
    if not match:
        raise RuntimeError(f"Unable to find {role} {name!r} in snapshot.")
    return match.group(1)


def _find_named_refs(snapshot_text: str, *, role: str, name: str) -> list[str]:
    pattern = re.compile(rf'- {re.escape(role)} "{re.escape(name)}".*?\[ref=([^\]]+)\]')
    return pattern.findall(snapshot_text)


def _detect_editor_blocking_signals(snapshot_text: str) -> list[dict[str, str]]:
    lowered = snapshot_text.lower()
    matches: list[dict[str, str]] = []
    for code, phrase, text in EDITOR_BLOCKING_PATTERNS:
        if phrase in lowered:
            matches.append({"code": code, "phrase": phrase, "text": text})
    return matches


def _add_filter_is_disabled(candidate_refs: dict[str, list[dict[str, str]]]) -> bool:
    add_filter_candidates = candidate_refs.get("Add filter") or []
    return bool(add_filter_candidates and _candidate_ref_disabled(add_filter_candidates[0]))


def _classify_editor_state(
    *,
    candidate_refs: dict[str, list[dict[str, str]]],
    blocking_signals: list[dict[str, str]],
) -> str:
    if blocking_signals:
        return "blocked"
    if _add_filter_is_disabled(candidate_refs):
        return "constrained"
    return "ready"


def _build_prepare_summary(
    *,
    target_org: str,
    dashboard_id: str,
    session: str,
    candidate_refs: dict[str, list[dict[str, str]]],
    blocking_signals: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "target_org": target_org,
        "target_dashboard_id": dashboard_id,
        "session": session,
        "candidate_ref_count": sum(len(items) for items in candidate_refs.values()),
        "preflight_ref_labels": sorted(candidate_refs),
        "editor_state": _classify_editor_state(candidate_refs=candidate_refs, blocking_signals=blocking_signals),
        "blocking_signal_count": len(blocking_signals),
        "add_filter_disabled": _add_filter_is_disabled(candidate_refs),
    }


def _write_prepare_artifacts(
    *,
    output_dir: Path,
    snapshot_text: str,
    prepare_artifact: dict[str, Any],
) -> list[dict[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "salesforce_dashboard_filter_prepare_snapshot.yml"
    snapshot_path.write_text(snapshot_text, encoding="utf-8")
    artifact_path = output_dir / "salesforce_dashboard_filter_prepare.json"
    artifact_path.write_text(json.dumps(prepare_artifact, indent=2), encoding="utf-8")
    return [
        {"type": "salesforce_dashboard_filter_prepare_snapshot", "path": str(snapshot_path)},
        {"type": "salesforce_dashboard_filter_prepare", "path": str(artifact_path)},
    ]


def _write_open_filter_artifacts(
    *,
    output_dir: Path,
    snapshot_text: str,
    open_artifact: dict[str, Any],
) -> list[dict[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "salesforce_dashboard_filter_open_snapshot.yml"
    snapshot_path.write_text(snapshot_text, encoding="utf-8")
    artifact_path = output_dir / "salesforce_dashboard_filter_open.json"
    artifact_path.write_text(json.dumps(open_artifact, indent=2), encoding="utf-8")
    return [
        {"type": "salesforce_dashboard_filter_open_snapshot", "path": str(snapshot_path)},
        {"type": "salesforce_dashboard_filter_open", "path": str(artifact_path)},
    ]


def _write_open_filter_field_artifacts(
    *,
    output_dir: Path,
    snapshot_text: str,
    field_artifact: dict[str, Any],
) -> list[dict[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "salesforce_dashboard_filter_field_snapshot.yml"
    snapshot_path.write_text(snapshot_text, encoding="utf-8")
    artifact_path = output_dir / "salesforce_dashboard_filter_field.json"
    artifact_path.write_text(json.dumps(field_artifact, indent=2), encoding="utf-8")
    return [
        {"type": "salesforce_dashboard_filter_field_snapshot", "path": str(snapshot_path)},
        {"type": "salesforce_dashboard_filter_field", "path": str(artifact_path)},
    ]


def _write_open_filter_value_artifacts(
    *,
    output_dir: Path,
    snapshot_text: str,
    value_artifact: dict[str, Any],
) -> list[dict[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "salesforce_dashboard_filter_value_snapshot.yml"
    snapshot_path.write_text(snapshot_text, encoding="utf-8")
    artifact_path = output_dir / "salesforce_dashboard_filter_value.json"
    artifact_path.write_text(json.dumps(value_artifact, indent=2), encoding="utf-8")
    return [
        {"type": "salesforce_dashboard_filter_value_snapshot", "path": str(snapshot_path)},
        {"type": "salesforce_dashboard_filter_value", "path": str(artifact_path)},
    ]


def _write_select_filter_option_artifacts(
    *,
    output_dir: Path,
    snapshot_text: str,
    option_artifact: dict[str, Any],
) -> list[dict[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "salesforce_dashboard_filter_option_snapshot.yml"
    snapshot_path.write_text(snapshot_text, encoding="utf-8")
    artifact_path = output_dir / "salesforce_dashboard_filter_option.json"
    artifact_path.write_text(json.dumps(option_artifact, indent=2), encoding="utf-8")
    return [
        {"type": "salesforce_dashboard_filter_option_snapshot", "path": str(snapshot_path)},
        {"type": "salesforce_dashboard_filter_option", "path": str(artifact_path)},
    ]


def _write_apply_filter_value_artifacts(
    *,
    output_dir: Path,
    snapshot_text: str,
    apply_artifact: dict[str, Any],
) -> list[dict[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "salesforce_dashboard_filter_apply_snapshot.yml"
    snapshot_path.write_text(snapshot_text, encoding="utf-8")
    artifact_path = output_dir / "salesforce_dashboard_filter_apply.json"
    artifact_path.write_text(json.dumps(apply_artifact, indent=2), encoding="utf-8")
    return [
        {"type": "salesforce_dashboard_filter_apply_snapshot", "path": str(snapshot_path)},
        {"type": "salesforce_dashboard_filter_apply", "path": str(artifact_path)},
    ]


def _write_commit_dashboard_filter_artifacts(
    *,
    output_dir: Path,
    snapshot_text: str,
    commit_artifact: dict[str, Any],
) -> list[dict[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "salesforce_dashboard_filter_commit_snapshot.yml"
    snapshot_path.write_text(snapshot_text, encoding="utf-8")
    artifact_path = output_dir / "salesforce_dashboard_filter_commit.json"
    artifact_path.write_text(json.dumps(commit_artifact, indent=2), encoding="utf-8")
    return [
        {"type": "salesforce_dashboard_filter_commit_snapshot", "path": str(snapshot_path)},
        {"type": "salesforce_dashboard_filter_commit", "path": str(artifact_path)},
    ]


def _write_save_dashboard_artifacts(
    *,
    output_dir: Path,
    snapshot_text: str,
    save_artifact: dict[str, Any],
) -> list[dict[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "salesforce_dashboard_filter_save_snapshot.yml"
    snapshot_path.write_text(snapshot_text, encoding="utf-8")
    artifact_path = output_dir / "salesforce_dashboard_filter_save.json"
    artifact_path.write_text(json.dumps(save_artifact, indent=2), encoding="utf-8")
    return [
        {"type": "salesforce_dashboard_filter_save_snapshot", "path": str(snapshot_path)},
        {"type": "salesforce_dashboard_filter_save", "path": str(artifact_path)},
    ]


def _prepare_snapshot_labels(plan: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    for action in plan.get("preflight_actions") or []:
        if not isinstance(action, dict):
            continue
        for signal in action.get("success_signals") or []:
            if isinstance(signal, str) and signal not in labels:
                labels.append(signal)
    for fallback in ("Add filter", "Save", "Done"):
        if fallback not in labels:
            labels.append(fallback)
    return labels


def _capture_editor_snapshot(
    *,
    playwright_wrapper: Path,
    session: str,
    labels: list[str],
    attempts: int = 4,
    pause_seconds: float = 2.0,
) -> tuple[str, dict[str, list[dict[str, str]]]]:
    last_snapshot_text = ""
    last_refs: dict[str, list[dict[str, str]]] = {}
    for attempt in range(1, attempts + 1):
        if attempt > 1:
            time.sleep(pause_seconds)
        snapshot = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["snapshot"],
        )
        if snapshot.returncode != 0 or not snapshot.stdout.strip():
            continue
        snapshot_text = _resolve_snapshot_text(snapshot.stdout)
        candidate_refs = _extract_candidate_refs(snapshot_text, labels)
        last_snapshot_text = snapshot_text
        last_refs = candidate_refs
        if candidate_refs:
            return snapshot_text, candidate_refs
    if last_snapshot_text:
        return last_snapshot_text, last_refs
    raise RuntimeError("Failed to capture a usable dashboard editor snapshot.")


def _resolve_target_dashboard_id(plan: dict[str, Any], override: str | None) -> str:
    dashboard_id = override or plan.get("target_dashboard_id")
    if not dashboard_id or not isinstance(dashboard_id, str):
        raise RuntimeError("Automation plan does not define target_dashboard_id; pass --dashboard-id.")
    return dashboard_id


def _resolve_target_org(plan: dict[str, Any], override: str | None) -> str:
    target_org = override or plan.get("target_org")
    if not target_org or not isinstance(target_org, str):
        raise RuntimeError("Automation plan does not define target_org; pass --target-org.")
    return target_org


def _resolve_session(payload: dict[str, Any], override: str | None) -> str:
    session = override or payload.get("session")
    if not session or not isinstance(session, str):
        raise RuntimeError("Prepare artifact does not define session; pass --session.")
    return session


def _artifact_path_by_type(result: dict[str, Any], artifact_type: str) -> Path:
    for artifact in result.get("artifacts") or []:
        if artifact.get("type") == artifact_type and artifact.get("path"):
            return (ROOT / str(artifact["path"])).resolve()
    raise RuntimeError(f"Expected artifact type {artifact_type} in command result.")


def _flow_command_class(through_stage: str) -> str:
    return "mutating" if through_stage in {"save-dashboard", "verify-dashboard"} else "live_read"


def _normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _path_slug(value: str | None, *, fallback: str) -> str:
    if isinstance(value, str) and value:
        slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
        if slug:
            return slug
    return fallback


def _load_manual_filter_authoring_artifact(
    *,
    plan_path: Path,
    manual_filter_authoring_json: Path | None,
) -> tuple[Path, dict[str, Any]]:
    candidate_path = manual_filter_authoring_json or plan_path.parent / "salesforce_dashboard_manual_filter_authoring.json"
    if not candidate_path.exists():
        raise RuntimeError(
            "Unable to locate salesforce_dashboard_manual_filter_authoring.json; pass --manual-filter-authoring-json."
        )
    payload = _load_json(candidate_path)
    filter_intents = payload.get("filter_intents")
    if not isinstance(filter_intents, list) or not filter_intents:
        raise RuntimeError("Manual filter authoring artifact does not contain any filter_intents to verify.")
    return candidate_path, payload


def _resolve_manual_filter_intent(
    *,
    manual_filter_authoring_artifact: dict[str, Any],
    filter_name: str | None,
) -> dict[str, Any]:
    filter_intents = manual_filter_authoring_artifact.get("filter_intents") or []
    if filter_name:
        normalized_filter_name = _normalize_label(filter_name)
        for intent in filter_intents:
            if not isinstance(intent, dict):
                continue
            proposed_filter = intent.get("proposed_filter") or {}
            candidate_labels = [
                str(intent.get("source_label") or ""),
                str(proposed_filter.get("name") or ""),
            ]
            if any(_normalize_label(label) == normalized_filter_name for label in candidate_labels if label):
                return copy.deepcopy(intent)
    for intent in filter_intents:
        if isinstance(intent, dict):
            return copy.deepcopy(intent)
    raise RuntimeError("Unable to resolve a filter intent from the manual filter authoring artifact.")


def _resolve_manual_filter_option(
    *,
    filter_intent: dict[str, Any],
    option_alias: str | None,
) -> dict[str, Any] | None:
    proposed_filter = filter_intent.get("proposed_filter") or {}
    options = proposed_filter.get("options") or []
    if not isinstance(options, list) or not options:
        return None
    if option_alias:
        normalized_option_alias = _normalize_label(option_alias)
        for option in options:
            if not isinstance(option, dict):
                continue
            candidate_labels = [
                str(option.get("alias") or ""),
                str(option.get("value") or ""),
            ]
            if any(_normalize_label(label) == normalized_option_alias for label in candidate_labels if label):
                return copy.deepcopy(option)
    for option in options:
        if isinstance(option, dict):
            return copy.deepcopy(option)
    return None


def _build_verification_filter_contract(
    *,
    manual_filter_authoring_artifact: dict[str, Any],
    target_dashboard_id: str,
    target_org: str,
    filter_selections: list[dict[str, str | None]],
) -> dict[str, Any]:
    selected_intents: list[dict[str, Any]] = []
    for selection in filter_selections:
        selected_intent = _resolve_manual_filter_intent(
            manual_filter_authoring_artifact=manual_filter_authoring_artifact,
            filter_name=selection.get("filter_name"),
        )
        proposed_filter = copy.deepcopy(selected_intent.get("proposed_filter") or {})
        selected_option = _resolve_manual_filter_option(
            filter_intent=selected_intent,
            option_alias=selection.get("option_alias"),
        )
        if selected_option is not None:
            proposed_filter["options"] = [selected_option]
            proposed_filter["selectedOption"] = selected_option.get("alias") or selected_option.get("value")
        selected_intent["proposed_filter"] = proposed_filter
        selected_intents.append(selected_intent)
    return {
        "artifact_type": "salesforce_dashboard_manual_filter_authoring",
        "package_developer_name": manual_filter_authoring_artifact.get("package_developer_name"),
        "suggested_dashboard_label": manual_filter_authoring_artifact.get("suggested_dashboard_label"),
        "target_org": target_org,
        "target_dashboard_id": target_dashboard_id,
        "resolved_clone_baseline": manual_filter_authoring_artifact.get("resolved_clone_baseline"),
        "baseline_filter_count": manual_filter_authoring_artifact.get("baseline_filter_count"),
        "candidate_clone_baselines": manual_filter_authoring_artifact.get("candidate_clone_baselines") or [],
        "baseline_strategy": manual_filter_authoring_artifact.get("baseline_strategy"),
        "recommended_steps": [
            "Run salesforce_dashboard_executor.py verify against the authored dashboard.",
            "Confirm the selected manual dashboard filter still matches the proposed contract.",
        ],
        "filter_intents": selected_intents,
    }


def _write_json_artifact(*, output_path: Path, payload: dict[str, Any]) -> dict[str, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {"type": output_path.stem, "path": str(output_path)}


def _run_dashboard_verify(
    *,
    dashboard_executor_script: Path,
    verify_package: Path,
    target_dashboard_id: str,
    target_org: str,
    manual_filter_authoring_json: Path,
    output_dir: Path,
) -> tuple[int, dict[str, Any]]:
    if not dashboard_executor_script.exists():
        raise RuntimeError(f"Dashboard executor script not found: {dashboard_executor_script}")
    command = [
        sys.executable,
        str(dashboard_executor_script),
        "verify",
        "--package",
        str(verify_package),
        "--dashboard-id",
        target_dashboard_id,
        "--manual-filter-authoring-json",
        str(manual_filter_authoring_json),
        "--autofill-live",
        "--target-org",
        target_org,
        "--output-dir",
        str(output_dir),
        "--json",
    ]
    result = _run_command(command)
    stdout = result.stdout.strip()
    if not stdout:
        raise RuntimeError(result.stderr.strip() or "Dashboard verify produced no JSON output.")
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Unable to parse dashboard verify JSON output: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Dashboard verify did not return a JSON object.")
    return result.returncode, payload


def _candidate_ref_disabled(candidate: dict[str, Any]) -> bool:
    if isinstance(candidate.get("disabled"), bool):
        return bool(candidate["disabled"])
    line = candidate.get("line")
    return isinstance(line, str) and "[disabled]" in line.lower()


def _build_edit_route(plan: dict[str, Any], dashboard_id: str) -> str:
    template = plan.get("relative_edit_route_template")
    if isinstance(template, str) and "{dashboard_id}" in template:
        return template.format(dashboard_id=dashboard_id)
    relative_route = plan.get("relative_edit_route")
    if isinstance(relative_route, str) and "__FILL_TARGET_DASHBOARD_ID__" in relative_route:
        return relative_route.replace("__FILL_TARGET_DASHBOARD_ID__", dashboard_id)
    if isinstance(relative_route, str) and dashboard_id in relative_route:
        return relative_route
    return f"/lightning/r/Dashboard/{dashboard_id}/edit"


def _resolve_filter_action(
    *,
    plan: dict[str, Any] | None,
    filter_name_override: str | None,
) -> dict[str, Any]:
    actions = plan.get("filter_actions") if isinstance(plan, dict) else None
    if isinstance(actions, list):
        if filter_name_override:
            for action in actions:
                if not isinstance(action, dict):
                    continue
                if action.get("filter_name") == filter_name_override or action.get("source_label") == filter_name_override:
                    return action
        for action in actions:
            if isinstance(action, dict):
                return action
    if filter_name_override:
        return {"filter_name": filter_name_override, "field_picker_terms": [filter_name_override], "options": []}
    raise RuntimeError("Unable to resolve a filter action from the automation plan; pass --filter-name.")


def _resolve_filter_actions(
    *,
    plan: dict[str, Any] | None,
    filter_name_override: str | None,
    all_filters: bool,
) -> list[dict[str, Any]]:
    actions = plan.get("filter_actions") if isinstance(plan, dict) else None
    resolved_actions = [action for action in actions or [] if isinstance(action, dict)]
    if all_filters:
        if not resolved_actions:
            raise RuntimeError("Automation plan does not define any filter actions.")
        if filter_name_override:
            normalized_filter_name = _normalize_label(filter_name_override)
            resolved_actions = [
                action
                for action in resolved_actions
                if _normalize_label(str(action.get("filter_name") or action.get("source_label") or "")) == normalized_filter_name
            ]
            if not resolved_actions:
                raise RuntimeError(f"Unable to find filter action {filter_name_override!r} in the automation plan.")
        return [copy.deepcopy(action) for action in resolved_actions]
    return [copy.deepcopy(_resolve_filter_action(plan=plan, filter_name_override=filter_name_override))]


def _field_picker_search_labels(action: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    filter_name = action.get("filter_name")
    if isinstance(filter_name, str) and filter_name not in labels:
        labels.append(filter_name)
    for term in action.get("field_picker_terms") or []:
        if isinstance(term, str) and term not in labels:
            labels.append(term)
    if not labels:
        raise RuntimeError("Resolved filter action does not define any field picker terms.")
    return labels


def _choose_field_picker_candidate(
    *,
    candidate_refs: dict[str, list[dict[str, Any]]],
    labels: list[str],
) -> tuple[str, dict[str, Any]]:
    scored_candidates: list[tuple[int, str, dict[str, Any]]] = []
    for label in labels:
        for key, candidates in candidate_refs.items():
            if key.lower() != label.lower():
                continue
            for candidate in candidates:
                if _candidate_ref_disabled(candidate):
                    continue
                line = str(candidate.get("line") or "")
                lowered = line.lower()
                label_lower = label.lower()
                score = 0
                if f'option "{label_lower}"' in lowered:
                    score += 100
                elif "option" in lowered and label_lower in lowered:
                    score += 80
                elif f'button "{label_lower}"' in lowered:
                    score += 40
                elif f'generic "{label_lower}"' in lowered:
                    score += 20
                if "[cursor=pointer]" in lowered:
                    score += 10
                scored_candidates.append((score, label, candidate))
    if not scored_candidates:
        raise RuntimeError(f"Open-filter artifact does not include a selectable candidate for: {', '.join(labels)}")
    scored_candidates.sort(key=lambda item: item[0], reverse=True)
    _, selected_label, selected_candidate = scored_candidates[0]
    return selected_label, selected_candidate


def _choose_exact_button_candidate(
    *,
    candidate_refs: dict[str, list[dict[str, Any]]],
    button_text: str,
) -> dict[str, Any]:
    scored_candidates: list[tuple[int, dict[str, Any]]] = []
    for candidates in candidate_refs.values():
        for candidate in candidates:
            if _candidate_ref_disabled(candidate):
                continue
            line = str(candidate.get("line") or "")
            lowered = line.lower()
            match = BUTTON_TEXT_RE.search(line)
            if not match:
                continue
            actual_button_text = match.group(1).strip()
            if actual_button_text != button_text:
                continue
            score = 100
            if "[cursor=pointer]" in lowered:
                score += 10
            scored_candidates.append((score, candidate))
    if not scored_candidates:
        raise RuntimeError(f"Artifact does not include a selectable candidate for button: {button_text}")
    scored_candidates.sort(key=lambda item: item[0], reverse=True)
    return scored_candidates[0][1]


def _resolve_filter_option(
    *,
    action: dict[str, Any],
    option_alias_override: str | None,
) -> dict[str, Any]:
    options = action.get("options")
    if isinstance(options, list):
        if option_alias_override:
            for option in options:
                if not isinstance(option, dict):
                    continue
                if option.get("alias") == option_alias_override or option.get("value") == option_alias_override:
                    return option
        for option in options:
            if isinstance(option, dict):
                return option
    if option_alias_override:
        return {"alias": option_alias_override, "value": option_alias_override}
    raise RuntimeError("Unable to resolve a filter option from the automation plan; pass --option-alias.")


def _resolved_option_alias_for_action(
    *,
    action: dict[str, Any],
    option_alias_override: str | None,
) -> str | None:
    option = _resolve_filter_option(action=action, option_alias_override=option_alias_override)
    alias = option.get("alias")
    value = option.get("value")
    if isinstance(alias, str) and alias:
        return alias
    if isinstance(value, str) and value:
        return value
    return None


def _is_date_range_option(option: dict[str, Any]) -> bool:
    operation = str(option.get("operation") or "").lower()
    start_value = option.get("start_value", option.get("startValue"))
    end_value = option.get("end_value", option.get("endValue"))
    return operation == "between" and bool(start_value) and bool(end_value)


def _can_populate_manual_filter_option(option: dict[str, Any]) -> bool:
    if _is_date_range_option(option):
        return True
    operation = str(option.get("operation") or "").lower()
    return bool(operation and operation != "equals" and option.get("value") is not None)


def _populate_manual_filter_option(
    *,
    value_artifact_path: Path,
    value_artifact: dict[str, Any],
    selected_filter_name: str | None,
    selected_option_alias: str | None,
    option: dict[str, Any],
    session: str,
    playwright_wrapper: Path,
) -> tuple[str, dict[str, Any], str]:
    snapshot_path = _snapshot_artifact_path(value_artifact_path, "salesforce_dashboard_filter_value")
    snapshot_text = snapshot_path.read_text(encoding="utf-8")
    target_operation = str(option.get("operation") or "").lower()
    if target_operation and target_operation != "equals":
        operator_ref = _find_named_ref(snapshot_text, role="button", name="Operator")
        click_result = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["click", operator_ref],
        )
        if click_result.returncode != 0:
            raise RuntimeError((click_result.stderr or click_result.stdout or "Failed to open the filter operator menu.").strip())
        _, operator_candidate_refs = _capture_editor_snapshot(
            playwright_wrapper=playwright_wrapper,
            session=session,
            labels=[target_operation, "Apply", "Display Text", "Value"],
        )
        operator_candidates = operator_candidate_refs.get(target_operation) or []
        if not operator_candidates:
            raise RuntimeError(f"Filter operator menu did not expose a selectable {target_operation!r} option.")
        operator_result = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["click", str(operator_candidates[0]["ref"])],
        )
        if operator_result.returncode != 0:
            raise RuntimeError((operator_result.stderr or operator_result.stdout or f"Failed to select the {target_operation!r} filter operator.").strip())
        snapshot_text, _ = _capture_editor_snapshot(
            playwright_wrapper=playwright_wrapper,
            session=session,
            labels=["Apply", "Display Text", "Value", target_operation],
        )

    value_refs = _find_named_refs(snapshot_text, role="textbox", name="Value")
    if _is_date_range_option(option):
        if len(value_refs) < 2:
            raise RuntimeError("Date range filter editor did not expose both start and end Value textboxes.")
        value_pairs = [
            (value_refs[0], str(option.get("start_value", option.get("startValue")) or "")),
            (value_refs[1], str(option.get("end_value", option.get("endValue")) or "")),
        ]
    else:
        if not value_refs:
            raise RuntimeError("Filter editor did not expose a Value textbox for manual option entry.")
        value_pairs = [(value_refs[0], str(option.get("value") or ""))]
    display_text_ref = _find_named_ref(snapshot_text, role="textbox", name="Display Text")
    display_text = str(selected_option_alias or option.get("alias") or option.get("value") or "New Filter Value")
    for ref, field_value in [*value_pairs, (display_text_ref, display_text)]:
        fill_result = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["fill", str(ref), field_value],
        )
        if fill_result.returncode != 0:
            raise RuntimeError((fill_result.stderr or fill_result.stdout or f"Failed to fill date filter field {ref}.").strip())

    labels = ["Add", "Cancel", "Filter", "Apply"]
    if isinstance(selected_filter_name, str) and selected_filter_name not in labels:
        labels.append(selected_filter_name)
    if display_text not in labels:
        labels.append(display_text)
    snapshot_text, candidate_refs = _capture_editor_snapshot(
        playwright_wrapper=playwright_wrapper,
        session=session,
        labels=labels,
    )
    return snapshot_text, candidate_refs, display_text


def validate_plan(plan_path: Path) -> dict[str, Any]:
    try:
        plan = _load_json(plan_path)
    except Exception as exc:
        return make_result(
            status="error",
            command="validate",
            command_class="read_only",
            messages=[make_message("error", "load_failed", str(exc))],
            plan_path=str(plan_path),
        )

    findings = _validate_plan_payload(plan)
    status = "error" if any(item["level"] == "error" for item in findings) else "ok"
    messages = findings or [make_message("info", "plan_valid", "Automation plan passed validation.")]
    return make_result(
        status=status,
        command="validate",
        command_class="read_only",
        messages=messages,
        plan_path=str(plan_path),
        summary={
            "filter_action_count": len(plan.get("filter_actions") or []),
            "preflight_action_count": len(plan.get("preflight_actions") or []),
            "post_action_count": len(plan.get("post_actions") or []),
        },
    )


def prepare_plan(
    *,
    plan_path: Path,
    target_org_override: str | None,
    dashboard_id_override: str | None,
    session: str,
    output_dir: Path | None,
    playwright_wrapper: Path,
) -> tuple[int, dict[str, Any]]:
    try:
        plan = _load_json(plan_path)
        findings = _validate_plan_payload(plan)
        if findings:
            return 1, make_result(
                status="error",
                command="prepare",
                command_class="live_read",
                messages=findings,
                plan_path=str(plan_path),
            )
        _require_npx()
        if not playwright_wrapper.exists():
            raise RuntimeError(f"Playwright wrapper not found: {playwright_wrapper}")
        target_org = _resolve_target_org(plan, target_org_override)
        dashboard_id = _resolve_target_dashboard_id(plan, dashboard_id_override)
        relative_edit_route = _build_edit_route(plan, dashboard_id)
        frontdoor_url = _resolve_frontdoor_url(target_org)
        base_url = _base_url_from_frontdoor(frontdoor_url)
        edit_url = f"{base_url}{relative_edit_route}"

        open_browser = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["open", frontdoor_url, "--headed"],
        )
        if open_browser.returncode != 0:
            raise RuntimeError((open_browser.stderr or open_browser.stdout or "Failed to open Playwright browser session.").strip())

        goto_edit = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["goto", edit_url],
        )
        if goto_edit.returncode != 0:
            raise RuntimeError((goto_edit.stderr or goto_edit.stdout or "Failed to open dashboard edit route.").strip())

        _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["resize", "1600", "2200"],
        )
        labels = _prepare_snapshot_labels(plan)
        snapshot_text = ""
        candidate_refs: dict[str, list[dict[str, str]]] = {}
        blocking_signals: list[dict[str, str]] = []
        for attempt in range(1, 5):
            snapshot_text, candidate_refs = _capture_editor_snapshot(
                playwright_wrapper=playwright_wrapper,
                session=session,
                labels=labels,
            )
            blocking_signals = _detect_editor_blocking_signals(snapshot_text)
            if blocking_signals or not _add_filter_is_disabled(candidate_refs):
                break
            time.sleep(2.0)
        prepare_artifact = {
            "artifact_type": "salesforce_dashboard_filter_prepare",
            "target_org": target_org,
            "target_dashboard_id": dashboard_id,
            "session": session,
            "relative_edit_route": relative_edit_route,
            "edit_url": edit_url,
            "candidate_refs": candidate_refs,
            "blocking_signals": blocking_signals,
            "editor_state": _classify_editor_state(candidate_refs=candidate_refs, blocking_signals=blocking_signals),
            "next_actions": [
                {
                    "action": "click_add_filter",
                    "candidate_refs": [item["ref"] for item in candidate_refs.get("Add filter", [])],
                },
                {
                    "action": "save_dashboard",
                    "candidate_refs": [item["ref"] for item in candidate_refs.get("Save", [])],
                },
                {
                    "action": "exit_dashboard_editor",
                    "candidate_refs": [item["ref"] for item in candidate_refs.get("Done", [])],
                },
            ],
        }
        artifacts: list[dict[str, str]] = []
        if output_dir is not None:
            artifacts = _write_prepare_artifacts(
                output_dir=output_dir,
                snapshot_text=snapshot_text,
                prepare_artifact=prepare_artifact,
            )

        return 0, make_result(
            status="warn" if blocking_signals or _add_filter_is_disabled(candidate_refs) else "ok",
            command="prepare",
            command_class="live_read",
            messages=[
                *[
                    make_message("warn", "editor_blocking_signal", signal["text"])
                    for signal in blocking_signals
                ],
                *(
                    [
                        make_message(
                            "warn",
                            "add_filter_disabled",
                            "Add filter is disabled in the prepared dashboard editor state.",
                        )
                    ]
                    if _add_filter_is_disabled(candidate_refs)
                    else []
                ),
                make_message(
                    "info",
                    "prepare_ready",
                    "Opened the dashboard edit route, captured a snapshot, and extracted candidate editor refs.",
                )
            ],
            artifacts=artifacts,
            plan_path=str(plan_path),
            summary=_build_prepare_summary(
                target_org=target_org,
                dashboard_id=dashboard_id,
                session=session,
                candidate_refs=candidate_refs,
                blocking_signals=blocking_signals,
            ),
            prepare_artifact=prepare_artifact,
        )
    except Exception as exc:
        return 1, make_result(
            status="error",
            command="prepare",
            command_class="live_read",
            messages=[make_message("error", "prepare_failed", str(exc))],
            plan_path=str(plan_path),
        )


def _open_filter_from_editor_artifact(
    *,
    editor_artifact: dict[str, Any],
    editor_artifact_path: Path,
    plan: dict[str, Any] | None,
    session_override: str | None,
    output_dir: Path | None,
    playwright_wrapper: Path,
    command_name: str,
    artifact_path_field: str,
) -> tuple[int, dict[str, Any]]:
    _require_npx()
    if not playwright_wrapper.exists():
        raise RuntimeError(f"Playwright wrapper not found: {playwright_wrapper}")

    session = _resolve_session(editor_artifact, session_override)
    blocking_signals = editor_artifact.get("blocking_signals") or []
    add_filter_candidates = (editor_artifact.get("candidate_refs") or {}).get("Add filter") or []
    if not add_filter_candidates:
        raise RuntimeError("Editor artifact does not include an Add filter candidate ref.")
    add_filter_ref = add_filter_candidates[0]
    if _candidate_ref_disabled(add_filter_ref):
        return 0, make_result(
            status="warn",
            command=command_name,
            command_class="live_read",
            messages=[
                *[
                    make_message("warn", "editor_blocking_signal", signal.get("text", str(signal)))
                    for signal in blocking_signals
                    if isinstance(signal, dict)
                ],
                make_message(
                    "warn",
                    "add_filter_disabled",
                    "Add filter is currently disabled in the prepared dashboard editor state.",
                ),
            ],
            **{artifact_path_field: str(editor_artifact_path)},
            summary={
                "session": session,
                "add_filter_ref": add_filter_ref.get("ref"),
                "disabled": True,
                "blocking_signal_count": len(blocking_signals),
            },
            prepare_artifact=editor_artifact if command_name == "open-filter" else None,
        )

    click_result = _run_playwright(
        playwright_wrapper=playwright_wrapper,
        session=session,
        command=["click", str(add_filter_ref["ref"])],
    )
    if click_result.returncode != 0:
        raise RuntimeError((click_result.stderr or click_result.stdout or "Failed to click Add filter.").strip())

    labels = ["Add", "Cancel", "Filter", "Field"]
    if isinstance(plan, dict):
        for action in plan.get("filter_actions") or []:
            if not isinstance(action, dict):
                continue
            filter_name = action.get("filter_name")
            if isinstance(filter_name, str) and filter_name not in labels:
                labels.append(filter_name)
            for term in action.get("field_picker_terms") or []:
                if isinstance(term, str) and term not in labels:
                    labels.append(term)

    snapshot_text, candidate_refs = _capture_editor_snapshot(
        playwright_wrapper=playwright_wrapper,
        session=session,
        labels=labels,
    )
    open_artifact = {
        "artifact_type": "salesforce_dashboard_filter_open",
        "session": session,
        artifact_path_field: str(editor_artifact_path),
        "source_add_filter_ref": add_filter_ref,
        "candidate_refs": candidate_refs,
        "next_actions": [
            {
                "action": "select_filter_field",
                "candidate_ref_labels": sorted(candidate_refs),
            }
        ],
    }
    artifacts: list[dict[str, str]] = []
    if output_dir is not None:
        artifacts = _write_open_filter_artifacts(
            output_dir=output_dir,
            snapshot_text=snapshot_text,
            open_artifact=open_artifact,
        )

    return 0, make_result(
        status="ok",
        command=command_name,
        command_class="live_read",
        messages=[
            make_message(
                "info",
                "filter_picker_ready",
                "Clicked Add filter and captured the follow-on filter authoring snapshot.",
            )
        ],
        artifacts=artifacts,
        **{artifact_path_field: str(editor_artifact_path)},
        summary={
            "session": session,
            "candidate_ref_count": sum(len(items) for items in candidate_refs.values()),
            "candidate_ref_labels": sorted(candidate_refs),
        },
        open_filter_artifact=open_artifact,
    )


def open_filter_from_prepare(
    *,
    prepare_path: Path,
    plan_path: Path | None,
    session_override: str | None,
    output_dir: Path | None,
    playwright_wrapper: Path,
) -> tuple[int, dict[str, Any]]:
    try:
        prepare_artifact = _load_json(prepare_path)
        plan = _load_json(plan_path) if plan_path is not None else None
        return _open_filter_from_editor_artifact(
            editor_artifact=prepare_artifact,
            editor_artifact_path=prepare_path,
            plan=plan,
            session_override=session_override,
            output_dir=output_dir,
            playwright_wrapper=playwright_wrapper,
            command_name="open-filter",
            artifact_path_field="prepare_artifact_path",
        )
    except Exception as exc:
        return 1, make_result(
            status="error",
            command="open-filter",
            command_class="live_read",
            messages=[make_message("error", "open_filter_failed", str(exc))],
            prepare_path=str(prepare_path),
        )


def open_filter_field_from_artifact(
    *,
    open_filter_path: Path,
    plan_path: Path | None,
    filter_name_override: str | None,
    session_override: str | None,
    output_dir: Path | None,
    playwright_wrapper: Path,
) -> tuple[int, dict[str, Any]]:
    try:
        open_artifact = _load_json(open_filter_path)
        plan = _load_json(plan_path) if plan_path is not None else None
        _require_npx()
        if not playwright_wrapper.exists():
            raise RuntimeError(f"Playwright wrapper not found: {playwright_wrapper}")

        session = _resolve_session(open_artifact, session_override)
        filter_action = _resolve_filter_action(plan=plan, filter_name_override=filter_name_override)
        field_labels = _field_picker_search_labels(filter_action)
        selected_label, selected_candidate = _choose_field_picker_candidate(
            candidate_refs=open_artifact.get("candidate_refs") or {},
            labels=field_labels,
        )

        click_result = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["click", str(selected_candidate["ref"])],
        )
        if click_result.returncode != 0:
            raise RuntimeError((click_result.stderr or click_result.stdout or "Failed to click filter field.").strip())

        labels = ["Add", "Cancel", "Field", "Filter", selected_label]
        for option in filter_action.get("options") or []:
            if not isinstance(option, dict):
                continue
            value = option.get("value")
            if isinstance(value, str) and value not in labels:
                labels.append(value)

        snapshot_text, candidate_refs = _capture_editor_snapshot(
            playwright_wrapper=playwright_wrapper,
            session=session,
            labels=labels,
        )
        field_artifact = {
            "artifact_type": "salesforce_dashboard_filter_field",
            "session": session,
            "open_filter_artifact_path": str(open_filter_path),
            "selected_filter_name": filter_action.get("filter_name"),
            "selected_field_term": selected_label,
            "selected_field_ref": selected_candidate,
            "candidate_refs": candidate_refs,
            "next_actions": [
                {
                    "action": "configure_filter_values",
                    "candidate_ref_labels": sorted(candidate_refs),
                }
            ],
        }
        artifacts: list[dict[str, str]] = []
        if output_dir is not None:
            artifacts = _write_open_filter_field_artifacts(
                output_dir=output_dir,
                snapshot_text=snapshot_text,
                field_artifact=field_artifact,
            )

        return 0, make_result(
            status="ok",
            command="open-filter-field",
            command_class="live_read",
            messages=[
                make_message(
                    "info",
                    "filter_field_ready",
                    "Selected the target filter field and captured the follow-on authoring snapshot.",
                )
            ],
            artifacts=artifacts,
            open_filter_path=str(open_filter_path),
            summary={
                "session": session,
                "selected_field_term": selected_label,
                "selected_field_ref": selected_candidate.get("ref"),
                "candidate_ref_count": sum(len(items) for items in candidate_refs.values()),
                "candidate_ref_labels": sorted(candidate_refs),
            },
            open_filter_field_artifact=field_artifact,
        )
    except Exception as exc:
        return 1, make_result(
            status="error",
            command="open-filter-field",
            command_class="live_read",
            messages=[make_message("error", "open_filter_field_failed", str(exc))],
            open_filter_path=str(open_filter_path),
        )


def open_filter_value_from_field_artifact(
    *,
    field_artifact_path: Path,
    plan_path: Path | None,
    filter_name_override: str | None,
    session_override: str | None,
    output_dir: Path | None,
    playwright_wrapper: Path,
) -> tuple[int, dict[str, Any]]:
    try:
        field_artifact = _load_json(field_artifact_path)
        plan = _load_json(plan_path) if plan_path is not None else None
        _require_npx()
        if not playwright_wrapper.exists():
            raise RuntimeError(f"Playwright wrapper not found: {playwright_wrapper}")

        session = _resolve_session(field_artifact, session_override)
        filter_name = filter_name_override or field_artifact.get("selected_filter_name")
        filter_action = _resolve_filter_action(plan=plan, filter_name_override=filter_name if isinstance(filter_name, str) else None)
        add_value_candidate = _choose_exact_button_candidate(
            candidate_refs=field_artifact.get("candidate_refs") or {},
            button_text="Add Filter Value",
        )

        click_result = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["click", str(add_value_candidate["ref"])],
        )
        if click_result.returncode != 0:
            raise RuntimeError((click_result.stderr or click_result.stdout or "Failed to click Add Filter Value.").strip())

        labels = ["Add", "Cancel", "Filter"]
        filter_name = filter_action.get("filter_name")
        if isinstance(filter_name, str) and filter_name not in labels:
            labels.append(filter_name)
        for option in filter_action.get("options") or []:
            if not isinstance(option, dict):
                continue
            alias = option.get("alias")
            value = option.get("value")
            if isinstance(alias, str) and alias not in labels:
                labels.append(alias)
            if isinstance(value, str) and value not in labels:
                labels.append(value)

        snapshot_text, candidate_refs = _capture_editor_snapshot(
            playwright_wrapper=playwright_wrapper,
            session=session,
            labels=labels,
        )
        value_artifact = {
            "artifact_type": "salesforce_dashboard_filter_value",
            "session": session,
            "field_artifact_path": str(field_artifact_path),
            "selected_filter_name": filter_action.get("filter_name"),
            "source_add_filter_value_ref": add_value_candidate,
            "candidate_refs": candidate_refs,
            "next_actions": [
                {
                    "action": "select_filter_option",
                    "candidate_ref_labels": sorted(candidate_refs),
                }
            ],
        }
        artifacts: list[dict[str, str]] = []
        if output_dir is not None:
            artifacts = _write_open_filter_value_artifacts(
                output_dir=output_dir,
                snapshot_text=snapshot_text,
                value_artifact=value_artifact,
            )

        return 0, make_result(
            status="ok",
            command="open-filter-value",
            command_class="live_read",
            messages=[
                make_message(
                    "info",
                    "filter_value_ready",
                    "Opened the filter value picker and captured the follow-on authoring snapshot.",
                )
            ],
            artifacts=artifacts,
            field_artifact_path=str(field_artifact_path),
            summary={
                "session": session,
                "selected_filter_name": filter_action.get("filter_name"),
                "source_add_filter_value_ref": add_value_candidate.get("ref"),
                "candidate_ref_count": sum(len(items) for items in candidate_refs.values()),
                "candidate_ref_labels": sorted(candidate_refs),
            },
            open_filter_value_artifact=value_artifact,
        )
    except Exception as exc:
        return 1, make_result(
            status="error",
            command="open-filter-value",
            command_class="live_read",
            messages=[make_message("error", "open_filter_value_failed", str(exc))],
            field_artifact_path=str(field_artifact_path),
        )


def select_filter_option_from_value_artifact(
    *,
    value_artifact_path: Path,
    plan_path: Path | None,
    filter_name_override: str | None,
    option_alias_override: str | None,
    session_override: str | None,
    output_dir: Path | None,
    playwright_wrapper: Path,
) -> tuple[int, dict[str, Any]]:
    try:
        value_artifact = _load_json(value_artifact_path)
        plan = _load_json(plan_path) if plan_path is not None else None
        _require_npx()
        if not playwright_wrapper.exists():
            raise RuntimeError(f"Playwright wrapper not found: {playwright_wrapper}")

        session = _resolve_session(value_artifact, session_override)
        filter_name = filter_name_override or value_artifact.get("selected_filter_name")
        filter_action = _resolve_filter_action(plan=plan, filter_name_override=filter_name if isinstance(filter_name, str) else None)
        option = _resolve_filter_option(action=filter_action, option_alias_override=option_alias_override)
        option_labels: list[str] = []
        alias = option.get("alias")
        value = option.get("value")
        if isinstance(alias, str):
            option_labels.append(alias)
        if isinstance(value, str) and value not in option_labels:
            option_labels.append(value)
        try:
            selected_label, selected_candidate = _choose_field_picker_candidate(
                candidate_refs=value_artifact.get("candidate_refs") or {},
                labels=option_labels,
            )

            click_result = _run_playwright(
                playwright_wrapper=playwright_wrapper,
                session=session,
                command=["click", str(selected_candidate["ref"])],
            )
            if click_result.returncode != 0:
                raise RuntimeError((click_result.stderr or click_result.stdout or "Failed to click filter option.").strip())

            labels = ["Add", "Cancel", "Filter", selected_label]
            labels.append("Apply")
            filter_name = filter_action.get("filter_name")
            if isinstance(filter_name, str) and filter_name not in labels:
                labels.append(filter_name)

            snapshot_text, candidate_refs = _capture_editor_snapshot(
                playwright_wrapper=playwright_wrapper,
                session=session,
                labels=labels,
            )
        except RuntimeError:
            if not _can_populate_manual_filter_option(option):
                raise
            snapshot_text, candidate_refs, selected_label = _populate_manual_filter_option(
                value_artifact_path=value_artifact_path,
                value_artifact=value_artifact,
                selected_filter_name=filter_action.get("filter_name"),
                selected_option_alias=alias if isinstance(alias, str) else (value if isinstance(value, str) else None),
                option=option,
                session=session,
                playwright_wrapper=playwright_wrapper,
            )
            selected_candidate = {
                "ref": "date_range_manual",
                "line": f"Authored date range option {selected_label}",
                "disabled": False,
            }
        option_artifact = {
            "artifact_type": "salesforce_dashboard_filter_option",
            "session": session,
            "value_artifact_path": str(value_artifact_path),
            "selected_filter_name": filter_action.get("filter_name"),
            "selected_option_alias": alias,
            "selected_option_ref": selected_candidate,
            "candidate_refs": candidate_refs,
            "next_actions": [
                {
                    "action": "commit_filter_value",
                    "candidate_ref_labels": sorted(candidate_refs),
                }
            ],
        }
        artifacts: list[dict[str, str]] = []
        if output_dir is not None:
            artifacts = _write_select_filter_option_artifacts(
                output_dir=output_dir,
                snapshot_text=snapshot_text,
                option_artifact=option_artifact,
            )

        return 0, make_result(
            status="ok",
            command="select-filter-option",
            command_class="live_read",
            messages=[
                make_message(
                    "info",
                    "filter_option_ready",
                    "Selected the target filter option and captured the follow-on authoring snapshot.",
                )
            ],
            artifacts=artifacts,
            value_artifact_path=str(value_artifact_path),
            summary={
                "session": session,
                "selected_filter_name": filter_action.get("filter_name"),
                "selected_option_alias": alias,
                "selected_option_ref": selected_candidate.get("ref"),
                "candidate_ref_count": sum(len(items) for items in candidate_refs.values()),
                "candidate_ref_labels": sorted(candidate_refs),
            },
            select_filter_option_artifact=option_artifact,
        )
    except Exception as exc:
        return 1, make_result(
            status="error",
            command="select-filter-option",
            command_class="live_read",
            messages=[make_message("error", "select_filter_option_failed", str(exc))],
            value_artifact_path=str(value_artifact_path),
        )


def apply_filter_value_from_option_artifact(
    *,
    option_artifact_path: Path,
    session_override: str | None,
    output_dir: Path | None,
    playwright_wrapper: Path,
) -> tuple[int, dict[str, Any]]:
    try:
        option_artifact = _load_json(option_artifact_path)
        _require_npx()
        if not playwright_wrapper.exists():
            raise RuntimeError(f"Playwright wrapper not found: {playwright_wrapper}")

        session = _resolve_session(option_artifact, session_override)
        apply_candidate = _choose_exact_button_candidate(
            candidate_refs=option_artifact.get("candidate_refs") or {},
            button_text="Apply",
        )

        click_result = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["click", str(apply_candidate["ref"])],
        )
        if click_result.returncode != 0:
            raise RuntimeError((click_result.stderr or click_result.stdout or "Failed to click Apply.").strip())

        labels = ["Add", "Cancel", "Filter"]
        selected_filter_name = option_artifact.get("selected_filter_name")
        selected_option_alias = option_artifact.get("selected_option_alias")
        if isinstance(selected_filter_name, str) and selected_filter_name not in labels:
            labels.append(selected_filter_name)
        if isinstance(selected_option_alias, str) and selected_option_alias not in labels:
            labels.append(selected_option_alias)

        snapshot_text, candidate_refs = _capture_editor_snapshot(
            playwright_wrapper=playwright_wrapper,
            session=session,
            labels=labels,
        )
        apply_artifact = {
            "artifact_type": "salesforce_dashboard_filter_apply",
            "session": session,
            "option_artifact_path": str(option_artifact_path),
            "selected_filter_name": selected_filter_name,
            "selected_option_alias": selected_option_alias,
            "source_apply_ref": apply_candidate,
            "candidate_refs": candidate_refs,
            "add_ready": False,
            "next_actions": [
                {
                    "action": "commit_dashboard_filter",
                    "candidate_ref_labels": sorted(candidate_refs),
                }
            ],
        }
        try:
            add_candidate = _choose_exact_button_candidate(candidate_refs=candidate_refs, button_text="Add")
            apply_artifact["add_ready"] = not _candidate_ref_disabled(add_candidate)
            apply_artifact["commit_add_ref"] = add_candidate
        except RuntimeError:
            apply_artifact["add_ready"] = False
        artifacts: list[dict[str, str]] = []
        if output_dir is not None:
            artifacts = _write_apply_filter_value_artifacts(
                output_dir=output_dir,
                snapshot_text=snapshot_text,
                apply_artifact=apply_artifact,
            )

        return 0, make_result(
            status="ok",
            command="apply-filter-value",
            command_class="live_read",
            messages=[
                make_message(
                    "info",
                    "filter_value_applied",
                    "Applied the selected filter value and captured the follow-on dashboard filter state.",
                )
            ],
            artifacts=artifacts,
            option_artifact_path=str(option_artifact_path),
            summary={
                "session": session,
                "selected_filter_name": selected_filter_name,
                "selected_option_alias": selected_option_alias,
                "source_apply_ref": apply_candidate.get("ref"),
                "candidate_ref_count": sum(len(items) for items in candidate_refs.values()),
                "candidate_ref_labels": sorted(candidate_refs),
                "add_ready": apply_artifact["add_ready"],
            },
            apply_filter_value_artifact=apply_artifact,
        )
    except Exception as exc:
        return 1, make_result(
            status="error",
            command="apply-filter-value",
            command_class="live_read",
            messages=[make_message("error", "apply_filter_value_failed", str(exc))],
            option_artifact_path=str(option_artifact_path),
        )


def commit_dashboard_filter_from_apply_artifact(
    *,
    apply_artifact_path: Path,
    session_override: str | None,
    output_dir: Path | None,
    playwright_wrapper: Path,
) -> tuple[int, dict[str, Any]]:
    try:
        apply_artifact = _load_json(apply_artifact_path)
        _require_npx()
        if not playwright_wrapper.exists():
            raise RuntimeError(f"Playwright wrapper not found: {playwright_wrapper}")

        session = _resolve_session(apply_artifact, session_override)
        add_candidate = _choose_exact_button_candidate(
            candidate_refs=apply_artifact.get("candidate_refs") or {},
            button_text="Add",
        )
        if _candidate_ref_disabled(add_candidate):
            raise RuntimeError("Dashboard filter Add button is still disabled; the filter value is not ready to commit.")

        click_result = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["click", str(add_candidate["ref"])],
        )
        if click_result.returncode != 0:
            raise RuntimeError((click_result.stderr or click_result.stdout or "Failed to click Add.").strip())

        labels = ["Add filter", "Save", "Done", "Forecast Category", "Pipeline"]
        snapshot_text, candidate_refs = _capture_editor_snapshot(
            playwright_wrapper=playwright_wrapper,
            session=session,
            labels=labels,
        )
        commit_artifact = {
            "artifact_type": "salesforce_dashboard_filter_commit",
            "session": session,
            "apply_artifact_path": str(apply_artifact_path),
            "selected_filter_name": apply_artifact.get("selected_filter_name"),
            "selected_option_alias": apply_artifact.get("selected_option_alias"),
            "source_add_ref": add_candidate,
            "candidate_refs": candidate_refs,
            "save_ready": False,
            "next_actions": [
                {
                    "action": "save_dashboard",
                    "candidate_ref_labels": sorted(candidate_refs),
                }
            ],
        }
        try:
            save_candidate = _choose_exact_button_candidate(candidate_refs=candidate_refs, button_text="Save")
            commit_artifact["save_ready"] = not _candidate_ref_disabled(save_candidate)
            commit_artifact["save_ref"] = save_candidate
        except RuntimeError:
            commit_artifact["save_ready"] = False

        artifacts: list[dict[str, str]] = []
        if output_dir is not None:
            artifacts = _write_commit_dashboard_filter_artifacts(
                output_dir=output_dir,
                snapshot_text=snapshot_text,
                commit_artifact=commit_artifact,
            )

        return 0, make_result(
            status="ok",
            command="commit-dashboard-filter",
            command_class="live_read",
            messages=[
                make_message(
                    "info",
                    "dashboard_filter_committed",
                    "Committed the dashboard filter draft and captured the post-add editor state.",
                )
            ],
            artifacts=artifacts,
            apply_artifact_path=str(apply_artifact_path),
            summary={
                "session": session,
                "selected_filter_name": apply_artifact.get("selected_filter_name"),
                "selected_option_alias": apply_artifact.get("selected_option_alias"),
                "source_add_ref": add_candidate.get("ref"),
                "candidate_ref_count": sum(len(items) for items in candidate_refs.values()),
                "candidate_ref_labels": sorted(candidate_refs),
                "save_ready": commit_artifact["save_ready"],
            },
            commit_dashboard_filter_artifact=commit_artifact,
        )
    except Exception as exc:
        return 1, make_result(
            status="error",
            command="commit-dashboard-filter",
            command_class="live_read",
            messages=[make_message("error", "commit_dashboard_filter_failed", str(exc))],
            apply_artifact_path=str(apply_artifact_path),
        )


def save_dashboard_from_commit_artifact(
    *,
    commit_artifact_path: Path,
    session_override: str | None,
    output_dir: Path | None,
    playwright_wrapper: Path,
) -> tuple[int, dict[str, Any]]:
    try:
        commit_artifact = _load_json(commit_artifact_path)
        _require_npx()
        if not playwright_wrapper.exists():
            raise RuntimeError(f"Playwright wrapper not found: {playwright_wrapper}")

        session = _resolve_session(commit_artifact, session_override)
        save_candidate = _choose_exact_button_candidate(
            candidate_refs=commit_artifact.get("candidate_refs") or {},
            button_text="Save",
        )
        if _candidate_ref_disabled(save_candidate):
            raise RuntimeError("Dashboard Save button is still disabled; the dashboard is not ready to persist the filter.")

        click_result = _run_playwright(
            playwright_wrapper=playwright_wrapper,
            session=session,
            command=["click", str(save_candidate["ref"])],
        )
        if click_result.returncode != 0:
            raise RuntimeError((click_result.stderr or click_result.stdout or "Failed to click Save.").strip())

        labels = ["Add filter", "Save", "Done", "Forecast Category", "Pipeline", "saved", "error"]
        snapshot_text, candidate_refs = _capture_editor_snapshot(
            playwright_wrapper=playwright_wrapper,
            session=session,
            labels=labels,
            attempts=6,
            pause_seconds=2.0,
        )
        save_artifact = {
            "artifact_type": "salesforce_dashboard_filter_save",
            "session": session,
            "commit_artifact_path": str(commit_artifact_path),
            "selected_filter_name": commit_artifact.get("selected_filter_name"),
            "selected_option_alias": commit_artifact.get("selected_option_alias"),
            "source_save_ref": save_candidate,
            "candidate_refs": candidate_refs,
            "done_ready": False,
            "next_actions": [
                {
                    "action": "verify_live_dashboard_filters",
                    "candidate_ref_labels": sorted(candidate_refs),
                }
            ],
        }
        try:
            done_candidate = _choose_exact_button_candidate(candidate_refs=candidate_refs, button_text="Done")
            save_artifact["done_ready"] = not _candidate_ref_disabled(done_candidate)
            save_artifact["done_ref"] = done_candidate
        except RuntimeError:
            save_artifact["done_ready"] = False

        artifacts: list[dict[str, str]] = []
        if output_dir is not None:
            artifacts = _write_save_dashboard_artifacts(
                output_dir=output_dir,
                snapshot_text=snapshot_text,
                save_artifact=save_artifact,
            )

        return 0, make_result(
            status="ok",
            command="save-dashboard",
            command_class="live_read",
            messages=[
                make_message(
                    "info",
                    "dashboard_saved",
                    "Clicked Save and captured the post-save dashboard editor state.",
                )
            ],
            artifacts=artifacts,
            commit_artifact_path=str(commit_artifact_path),
            summary={
                "session": session,
                "selected_filter_name": commit_artifact.get("selected_filter_name"),
                "selected_option_alias": commit_artifact.get("selected_option_alias"),
                "source_save_ref": save_candidate.get("ref"),
                "candidate_ref_count": sum(len(items) for items in candidate_refs.values()),
                "candidate_ref_labels": sorted(candidate_refs),
                "done_ready": save_artifact["done_ready"],
            },
            save_dashboard_artifact=save_artifact,
        )
    except Exception as exc:
        return 1, make_result(
            status="error",
            command="save-dashboard",
            command_class="live_read",
            messages=[make_message("error", "save_dashboard_failed", str(exc))],
            commit_artifact_path=str(commit_artifact_path),
        )


def run_filter_flow(
    *,
    plan_path: Path,
    target_org_override: str | None,
    dashboard_id_override: str | None,
    filter_name_override: str | None,
    option_alias_override: str | None,
    through_stage: str,
    session: str,
    output_dir: Path | None,
    playwright_wrapper: Path,
    all_filters: bool,
    verify_package: Path | None,
    manual_filter_authoring_json: Path | None,
    verify_output_dir: Path | None,
    dashboard_executor_script: Path,
) -> tuple[int, dict[str, Any]]:
    stage_order = [
        "prepare",
        "open-filter",
        "open-filter-field",
        "open-filter-value",
        "select-filter-option",
        "apply-filter-value",
        "commit-dashboard-filter",
        "save-dashboard",
        "verify-dashboard",
    ]
    if through_stage not in stage_order:
        return 1, make_result(
            status="error",
            command="run-filter-flow",
            command_class=_flow_command_class(through_stage),
            messages=[make_message("error", "invalid_through_stage", f"Unsupported flow stage: {through_stage}")],
            plan_path=str(plan_path),
        )
    if through_stage == "verify-dashboard" and verify_package is None:
        return 1, make_result(
            status="error",
            command="run-filter-flow",
            command_class=_flow_command_class(through_stage),
            messages=[
                make_message(
                    "error",
                    "verify_package_required",
                    "Provide --verify-package when --through verify-dashboard is requested.",
                )
            ],
            plan_path=str(plan_path),
        )
    if all_filters and option_alias_override:
        return 1, make_result(
            status="error",
            command="run-filter-flow",
            command_class=_flow_command_class(through_stage),
            messages=[
                make_message(
                    "error",
                    "option_alias_not_supported_with_all_filters",
                    "Do not pass --option-alias with --all-filters; the flow will use each filter action's default option.",
                )
            ],
            plan_path=str(plan_path),
        )

    stage_results: list[dict[str, Any]] = []
    stage_artifacts: list[dict[str, str]] = []
    authored_filters: list[dict[str, str | None]] = []
    root_output_dir: Path
    if output_dir is None:
        root_output_dir = Path(tempfile.mkdtemp(prefix="salesforce_dashboard_filter_flow_"))
    else:
        root_output_dir = output_dir
        root_output_dir.mkdir(parents=True, exist_ok=True)
    plan = _load_json(plan_path)
    resolved_filter_actions = _resolve_filter_actions(
        plan=plan,
        filter_name_override=filter_name_override,
        all_filters=all_filters,
    )

    def _finish(
        *,
        status: str,
        exit_code: int,
        messages: list[dict[str, str]],
        summary_extra: dict[str, Any] | None = None,
        verification_contract: dict[str, Any] | None = None,
        verify_result: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        summary = {
            "through_stage": through_stage,
            "executed_steps": [item["command"] for item in stage_results],
            "session": session,
            "authored_filter_count": len(authored_filters),
            "authored_filters": authored_filters,
            "output_dir": str(root_output_dir),
        }
        if authored_filters:
            summary["filter_name"] = authored_filters[-1].get("filter_name")
            summary["option_alias"] = authored_filters[-1].get("option_alias")
        if summary_extra:
            summary.update(summary_extra)
        payload = make_result(
            status=status,
            command="run-filter-flow",
            command_class=_flow_command_class(through_stage),
            messages=messages,
            artifacts=stage_artifacts,
            plan_path=str(plan_path),
            summary=summary,
            step_results=stage_results,
        )
        if verification_contract is not None:
            payload["verification_contract"] = verification_contract
        if verify_result is not None:
            payload["verify_result"] = verify_result
        return exit_code, payload

    prepare_dir = root_output_dir / "01_prepare"
    _, prepare_result = prepare_plan(
        plan_path=plan_path,
        target_org_override=target_org_override,
        dashboard_id_override=dashboard_id_override,
        session=session,
        output_dir=prepare_dir,
        playwright_wrapper=playwright_wrapper,
    )
    stage_results.append(prepare_result)
    stage_artifacts.extend(prepare_result.get("artifacts") or [])
    if prepare_result["status"] != "ok" or through_stage == "prepare":
        return _finish(
            status=prepare_result["status"],
            exit_code=0 if prepare_result["status"] != "error" else 1,
            messages=[
                *prepare_result.get("messages", []),
                make_message("info", "flow_complete", f"Stopped after stage {prepare_result['command']}."),
            ],
            summary_extra={
                "target_dashboard_id": (prepare_result.get("prepare_artifact") or {}).get("target_dashboard_id"),
                "filter_name": filter_name_override,
                "option_alias": option_alias_override,
            },
        )
    current_editor_artifact = prepare_result.get("prepare_artifact") or {}
    current_editor_artifact_path = _artifact_path_by_type(prepare_result, "salesforce_dashboard_filter_prepare")
    current_editor_artifact_field = "prepare_artifact_path"
    target_dashboard_id = (prepare_result.get("prepare_artifact") or {}).get("target_dashboard_id")
    target_org = (prepare_result.get("prepare_artifact") or {}).get("target_org")
    save_result: dict[str, Any] | None = None
    for filter_index, filter_action in enumerate(resolved_filter_actions, start=1):
        filter_name = str(filter_action.get("filter_name") or filter_action.get("source_label") or f"filter_{filter_index}")
        option_alias = _resolved_option_alias_for_action(
            action=filter_action,
            option_alias_override=option_alias_override if not all_filters else None,
        )
        if all_filters:
            filter_slug = _path_slug(filter_action.get("source_label") or filter_name, fallback=f"filter_{filter_index:02d}")
            filter_root = root_output_dir / f"{filter_index + 1:02d}_filter_{filter_index:02d}_{filter_slug}"
            open_dir = filter_root / "01_open_filter"
            field_dir = filter_root / "02_open_filter_field"
            value_dir = filter_root / "03_open_filter_value"
            option_dir = filter_root / "04_select_filter_option"
            apply_dir = filter_root / "05_apply_filter_value"
            commit_dir = filter_root / "06_commit_dashboard_filter"
            save_dir = filter_root / "07_save_dashboard"
        else:
            open_dir = root_output_dir / "02_open_filter"
            field_dir = root_output_dir / "03_open_filter_field"
            value_dir = root_output_dir / "04_open_filter_value"
            option_dir = root_output_dir / "05_select_filter_option"
            apply_dir = root_output_dir / "06_apply_filter_value"
            commit_dir = root_output_dir / "07_commit_dashboard_filter"
            save_dir = root_output_dir / "08_save_dashboard"

        _, open_filter_result = _open_filter_from_editor_artifact(
            editor_artifact=current_editor_artifact,
            editor_artifact_path=current_editor_artifact_path,
            plan=plan,
            session_override=session,
            output_dir=open_dir,
            playwright_wrapper=playwright_wrapper,
            command_name="open-filter",
            artifact_path_field=current_editor_artifact_field,
        )
        stage_results.append(open_filter_result)
        stage_artifacts.extend(open_filter_result.get("artifacts") or [])
        if open_filter_result["status"] != "ok" or through_stage == "open-filter":
            return _finish(
                status=open_filter_result["status"],
                exit_code=0 if open_filter_result["status"] != "error" else 1,
                messages=[
                    *open_filter_result.get("messages", []),
                    make_message("info", "flow_complete", f"Stopped after stage {open_filter_result['command']}."),
                ],
                summary_extra={
                    "target_dashboard_id": target_dashboard_id,
                    "target_org": target_org,
                    "filter_name": filter_name,
                    "option_alias": option_alias,
                },
            )
        open_filter_artifact_path = _artifact_path_by_type(open_filter_result, "salesforce_dashboard_filter_open")

        _, field_result = open_filter_field_from_artifact(
            open_filter_path=open_filter_artifact_path,
            plan_path=plan_path,
            filter_name_override=filter_name,
            session_override=session,
            output_dir=field_dir,
            playwright_wrapper=playwright_wrapper,
        )
        stage_results.append(field_result)
        stage_artifacts.extend(field_result.get("artifacts") or [])
        if field_result["status"] != "ok" or through_stage == "open-filter-field":
            return _finish(
                status=field_result["status"],
                exit_code=0 if field_result["status"] != "error" else 1,
                messages=[
                    *field_result.get("messages", []),
                    make_message("info", "flow_complete", f"Stopped after stage {field_result['command']}."),
                ],
                summary_extra={
                    "target_dashboard_id": target_dashboard_id,
                    "target_org": target_org,
                    "filter_name": filter_name,
                    "option_alias": option_alias,
                },
            )
        field_artifact_path = _artifact_path_by_type(field_result, "salesforce_dashboard_filter_field")

        _, value_result = open_filter_value_from_field_artifact(
            field_artifact_path=field_artifact_path,
            plan_path=plan_path,
            filter_name_override=filter_name,
            session_override=session,
            output_dir=value_dir,
            playwright_wrapper=playwright_wrapper,
        )
        stage_results.append(value_result)
        stage_artifacts.extend(value_result.get("artifacts") or [])
        if value_result["status"] != "ok" or through_stage == "open-filter-value":
            return _finish(
                status=value_result["status"],
                exit_code=0 if value_result["status"] != "error" else 1,
                messages=[
                    *value_result.get("messages", []),
                    make_message("info", "flow_complete", f"Stopped after stage {value_result['command']}."),
                ],
                summary_extra={
                    "target_dashboard_id": target_dashboard_id,
                    "target_org": target_org,
                    "filter_name": filter_name,
                    "option_alias": option_alias,
                },
            )
        value_artifact_path = _artifact_path_by_type(value_result, "salesforce_dashboard_filter_value")

        _, option_result = select_filter_option_from_value_artifact(
            value_artifact_path=value_artifact_path,
            plan_path=plan_path,
            filter_name_override=filter_name,
            option_alias_override=option_alias,
            session_override=session,
            output_dir=option_dir,
            playwright_wrapper=playwright_wrapper,
        )
        stage_results.append(option_result)
        stage_artifacts.extend(option_result.get("artifacts") or [])
        if option_result["status"] != "ok" or through_stage == "select-filter-option":
            return _finish(
                status=option_result["status"],
                exit_code=0 if option_result["status"] != "error" else 1,
                messages=[
                    *option_result.get("messages", []),
                    make_message("info", "flow_complete", f"Stopped after stage {option_result['command']}."),
                ],
                summary_extra={
                    "target_dashboard_id": target_dashboard_id,
                    "target_org": target_org,
                    "filter_name": filter_name,
                    "option_alias": option_alias,
                },
            )
        option_artifact_path = _artifact_path_by_type(option_result, "salesforce_dashboard_filter_option")

        _, apply_result = apply_filter_value_from_option_artifact(
            option_artifact_path=option_artifact_path,
            session_override=session,
            output_dir=apply_dir,
            playwright_wrapper=playwright_wrapper,
        )
        stage_results.append(apply_result)
        stage_artifacts.extend(apply_result.get("artifacts") or [])
        if apply_result["status"] != "ok" or through_stage == "apply-filter-value":
            return _finish(
                status=apply_result["status"],
                exit_code=0 if apply_result["status"] != "error" else 1,
                messages=[
                    *apply_result.get("messages", []),
                    make_message("info", "flow_complete", f"Stopped after stage {apply_result['command']}."),
                ],
                summary_extra={
                    "target_dashboard_id": target_dashboard_id,
                    "target_org": target_org,
                    "filter_name": filter_name,
                    "option_alias": option_alias,
                    "add_ready": (apply_result.get("apply_filter_value_artifact") or {}).get("add_ready"),
                },
            )
        apply_artifact_path = _artifact_path_by_type(apply_result, "salesforce_dashboard_filter_apply")

        _, commit_result = commit_dashboard_filter_from_apply_artifact(
            apply_artifact_path=apply_artifact_path,
            session_override=session,
            output_dir=commit_dir,
            playwright_wrapper=playwright_wrapper,
        )
        stage_results.append(commit_result)
        stage_artifacts.extend(commit_result.get("artifacts") or [])
        if commit_result["status"] != "ok" or through_stage == "commit-dashboard-filter":
            return _finish(
                status=commit_result["status"],
                exit_code=0 if commit_result["status"] != "error" else 1,
                messages=[
                    *commit_result.get("messages", []),
                    make_message("info", "flow_complete", f"Stopped after stage {commit_result['command']}."),
                ],
                summary_extra={
                    "target_dashboard_id": target_dashboard_id,
                    "target_org": target_org,
                    "filter_name": filter_name,
                    "option_alias": option_alias,
                    "save_ready": (commit_result.get("commit_dashboard_filter_artifact") or {}).get("save_ready"),
                },
            )
        commit_artifact_path = _artifact_path_by_type(commit_result, "salesforce_dashboard_filter_commit")

        _, save_result = save_dashboard_from_commit_artifact(
            commit_artifact_path=commit_artifact_path,
            session_override=session,
            output_dir=save_dir,
            playwright_wrapper=playwright_wrapper,
        )
        stage_results.append(save_result)
        stage_artifacts.extend(save_result.get("artifacts") or [])
        authored_filters.append({"filter_name": filter_name, "option_alias": option_alias})
        if save_result["status"] != "ok":
            return _finish(
                status=save_result["status"],
                exit_code=0 if save_result["status"] != "error" else 1,
                messages=[
                    *save_result.get("messages", []),
                    make_message("info", "flow_complete", f"Stopped after stage {save_result['command']}."),
                ],
                summary_extra={
                    "target_dashboard_id": target_dashboard_id,
                    "target_org": target_org,
                    "filter_name": filter_name,
                    "option_alias": option_alias,
                    "done_ready": (save_result.get("save_dashboard_artifact") or {}).get("done_ready"),
                },
            )
        current_editor_artifact = save_result.get("save_dashboard_artifact") or {}
        current_editor_artifact_path = _artifact_path_by_type(save_result, "salesforce_dashboard_filter_save")
        current_editor_artifact_field = "save_artifact_path"
        if through_stage == "save-dashboard" and filter_index == len(resolved_filter_actions):
            return _finish(
                status=save_result["status"],
                exit_code=0 if save_result["status"] != "error" else 1,
                messages=[
                    *save_result.get("messages", []),
                    make_message("info", "flow_complete", f"Completed flow through stage {save_result['command']}."),
                ],
                summary_extra={
                    "target_dashboard_id": target_dashboard_id,
                    "target_org": target_org,
                    "filter_name": filter_name,
                    "option_alias": option_alias,
                    "done_ready": (save_result.get("save_dashboard_artifact") or {}).get("done_ready"),
                },
            )
    if save_result["status"] != "ok" or through_stage == "save-dashboard":
        return _finish(
            status=save_result["status"],
            exit_code=0 if save_result["status"] != "error" else 1,
            messages=[
                *save_result.get("messages", []),
                make_message("info", "flow_complete", f"Completed flow through stage {save_result['command']}."),
            ],
            summary_extra={
                "target_dashboard_id": target_dashboard_id,
                "target_org": target_org,
                "filter_name": filter_name_override or (save_result.get("save_dashboard_artifact") or {}).get("selected_filter_name"),
                "option_alias": option_alias_override or (save_result.get("save_dashboard_artifact") or {}).get("selected_option_alias"),
                "done_ready": (save_result.get("save_dashboard_artifact") or {}).get("done_ready"),
            },
        )

    try:
        if not isinstance(target_org, str) or not target_org:
            raise RuntimeError("Unable to resolve target org for dashboard verify.")
        if not isinstance(target_dashboard_id, str) or not target_dashboard_id:
            raise RuntimeError("Unable to resolve target dashboard id for dashboard verify.")
        _, manual_filter_authoring_artifact = _load_manual_filter_authoring_artifact(
            plan_path=plan_path,
            manual_filter_authoring_json=manual_filter_authoring_json,
        )
        verification_contract = _build_verification_filter_contract(
            manual_filter_authoring_artifact=manual_filter_authoring_artifact,
            target_dashboard_id=target_dashboard_id,
            target_org=target_org,
            filter_selections=authored_filters,
        )
        verify_dir = verify_output_dir or (root_output_dir / "09_verify_dashboard")
        verify_dir.mkdir(parents=True, exist_ok=True)
        verification_contract_path = verify_dir / "salesforce_dashboard_manual_filter_verification.json"
        stage_artifacts.append(_write_json_artifact(output_path=verification_contract_path, payload=verification_contract))
        verify_exit_code, verify_result = _run_dashboard_verify(
            dashboard_executor_script=dashboard_executor_script,
            verify_package=verify_package,
            target_dashboard_id=target_dashboard_id,
            target_org=target_org,
            manual_filter_authoring_json=verification_contract_path,
            output_dir=verify_dir,
        )
        stage_results.append(verify_result)
        stage_artifacts.extend(verify_result.get("artifacts") or [])
    except Exception as exc:
        return (
            1,
            _finish(
                status="error",
                exit_code=1,
                messages=[make_message("error", "verify_dashboard_failed", str(exc))],
            )[1],
        )

    return _finish(
        status=verify_result["status"],
        exit_code=0 if verify_result["status"] != "error" and verify_exit_code == 0 else 1,
        messages=[
            *verify_result.get("messages", []),
            make_message("info", "flow_complete", f"Completed flow through stage {verify_result['command']}."),
        ],
        summary_extra={
            "target_dashboard_id": target_dashboard_id,
            "target_org": target_org,
            "done_ready": (save_result.get("save_dashboard_artifact") or {}).get("done_ready"),
            "verify_status": verify_result.get("status"),
            "manual_filter_verified_count": ((verify_result.get("summary") or {}).get("manual_filter_verified_count")),
            "verification_contract_path": str(verification_contract_path),
        },
        verification_contract=verification_contract,
        verify_result=verify_result,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Browser helper for native Salesforce dashboard filter authoring plans.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate a dashboard filter automation plan.")
    validate.add_argument("--plan", required=True, help="Path to salesforce_dashboard_filter_automation_plan.json")
    validate.add_argument("--json", action="store_true", help="Print JSON output.")

    prepare = subparsers.add_parser("prepare", help="Open the dashboard editor and snapshot candidate refs.")
    prepare.add_argument("--plan", required=True, help="Path to salesforce_dashboard_filter_automation_plan.json")
    prepare.add_argument("--target-org", default=None, help="Override target org alias/username.")
    prepare.add_argument("--dashboard-id", default=None, help="Override dashboard id.")
    prepare.add_argument("--session", required=True, help="Playwright session name to use.")
    prepare.add_argument("--output-dir", default=None, help="Optional output directory for emitted artifacts.")
    prepare.add_argument(
        "--playwright-wrapper",
        default=str(DEFAULT_PLAYWRIGHT_WRAPPER),
        help="Path to the Playwright CLI wrapper script.",
    )
    prepare.add_argument("--json", action="store_true", help="Print JSON output.")

    open_filter = subparsers.add_parser("open-filter", help="Click Add filter from a prepared editor state and snapshot the filter picker.")
    open_filter.add_argument("--prepare-artifact", required=True, help="Path to salesforce_dashboard_filter_prepare.json")
    open_filter.add_argument("--plan", default=None, help="Optional automation plan path for field-term hints.")
    open_filter.add_argument("--session", default=None, help="Override Playwright session name.")
    open_filter.add_argument("--output-dir", default=None, help="Optional output directory for emitted artifacts.")
    open_filter.add_argument(
        "--playwright-wrapper",
        default=str(DEFAULT_PLAYWRIGHT_WRAPPER),
        help="Path to the Playwright CLI wrapper script.",
    )
    open_filter.add_argument("--json", action="store_true", help="Print JSON output.")

    open_filter_field = subparsers.add_parser(
        "open-filter-field",
        help="Select a filter field from the open filter picker and snapshot the configuration state.",
    )
    open_filter_field.add_argument("--open-filter-artifact", required=True, help="Path to salesforce_dashboard_filter_open.json")
    open_filter_field.add_argument("--plan", default=None, help="Optional automation plan path for filter field/value hints.")
    open_filter_field.add_argument("--filter-name", default=None, help="Optional filter name override when the plan has multiple filter actions.")
    open_filter_field.add_argument("--session", default=None, help="Override Playwright session name.")
    open_filter_field.add_argument("--output-dir", default=None, help="Optional output directory for emitted artifacts.")
    open_filter_field.add_argument(
        "--playwright-wrapper",
        default=str(DEFAULT_PLAYWRIGHT_WRAPPER),
        help="Path to the Playwright CLI wrapper script.",
    )
    open_filter_field.add_argument("--json", action="store_true", help="Print JSON output.")

    open_filter_value = subparsers.add_parser(
        "open-filter-value",
        help="Click Add Filter Value from a selected field state and snapshot the value picker.",
    )
    open_filter_value.add_argument("--field-artifact", required=True, help="Path to salesforce_dashboard_filter_field.json")
    open_filter_value.add_argument("--plan", default=None, help="Optional automation plan path for filter value hints.")
    open_filter_value.add_argument("--filter-name", default=None, help="Optional filter name override when the plan has multiple filter actions.")
    open_filter_value.add_argument("--session", default=None, help="Override Playwright session name.")
    open_filter_value.add_argument("--output-dir", default=None, help="Optional output directory for emitted artifacts.")
    open_filter_value.add_argument(
        "--playwright-wrapper",
        default=str(DEFAULT_PLAYWRIGHT_WRAPPER),
        help="Path to the Playwright CLI wrapper script.",
    )
    open_filter_value.add_argument("--json", action="store_true", help="Print JSON output.")

    select_filter_option = subparsers.add_parser(
        "select-filter-option",
        help="Select a concrete filter option from the open value picker and snapshot the commit state.",
    )
    select_filter_option.add_argument("--value-artifact", required=True, help="Path to salesforce_dashboard_filter_value.json")
    select_filter_option.add_argument("--plan", default=None, help="Optional automation plan path for option hints.")
    select_filter_option.add_argument("--filter-name", default=None, help="Optional filter name override when the plan has multiple filter actions.")
    select_filter_option.add_argument("--option-alias", default=None, help="Optional option alias/value override.")
    select_filter_option.add_argument("--session", default=None, help="Override Playwright session name.")
    select_filter_option.add_argument("--output-dir", default=None, help="Optional output directory for emitted artifacts.")
    select_filter_option.add_argument(
        "--playwright-wrapper",
        default=str(DEFAULT_PLAYWRIGHT_WRAPPER),
        help="Path to the Playwright CLI wrapper script.",
    )
    select_filter_option.add_argument("--json", action="store_true", help="Print JSON output.")

    apply_filter_value = subparsers.add_parser(
        "apply-filter-value",
        help="Click Apply in the filter value dialog and snapshot the dashboard filter commit state.",
    )
    apply_filter_value.add_argument("--option-artifact", required=True, help="Path to salesforce_dashboard_filter_option.json")
    apply_filter_value.add_argument("--session", default=None, help="Override Playwright session name.")
    apply_filter_value.add_argument("--output-dir", default=None, help="Optional output directory for emitted artifacts.")
    apply_filter_value.add_argument(
        "--playwright-wrapper",
        default=str(DEFAULT_PLAYWRIGHT_WRAPPER),
        help="Path to the Playwright CLI wrapper script.",
    )
    apply_filter_value.add_argument("--json", action="store_true", help="Print JSON output.")

    commit_dashboard_filter = subparsers.add_parser(
        "commit-dashboard-filter",
        help="Click the dashboard-level Add button after a filter value is applied and snapshot the post-add editor state.",
    )
    commit_dashboard_filter.add_argument("--apply-artifact", required=True, help="Path to salesforce_dashboard_filter_apply.json")
    commit_dashboard_filter.add_argument("--session", default=None, help="Override Playwright session name.")
    commit_dashboard_filter.add_argument("--output-dir", default=None, help="Optional output directory for emitted artifacts.")
    commit_dashboard_filter.add_argument(
        "--playwright-wrapper",
        default=str(DEFAULT_PLAYWRIGHT_WRAPPER),
        help="Path to the Playwright CLI wrapper script.",
    )
    commit_dashboard_filter.add_argument("--json", action="store_true", help="Print JSON output.")

    save_dashboard = subparsers.add_parser(
        "save-dashboard",
        help="Click Save from a committed dashboard filter state and snapshot the post-save editor state.",
    )
    save_dashboard.add_argument("--commit-artifact", required=True, help="Path to salesforce_dashboard_filter_commit.json")
    save_dashboard.add_argument("--session", default=None, help="Override Playwright session name.")
    save_dashboard.add_argument("--output-dir", default=None, help="Optional output directory for emitted artifacts.")
    save_dashboard.add_argument(
        "--playwright-wrapper",
        default=str(DEFAULT_PLAYWRIGHT_WRAPPER),
        help="Path to the Playwright CLI wrapper script.",
    )
    save_dashboard.add_argument("--json", action="store_true", help="Print JSON output.")

    run_filter_flow_cmd = subparsers.add_parser(
        "run-filter-flow",
        help="Run a full native dashboard filter authoring flow through a requested stage.",
    )
    run_filter_flow_cmd.add_argument("--plan", required=True, help="Path to salesforce_dashboard_filter_automation_plan.json")
    run_filter_flow_cmd.add_argument("--target-org", default=None, help="Override target org alias/username.")
    run_filter_flow_cmd.add_argument("--dashboard-id", default=None, help="Override dashboard id.")
    run_filter_flow_cmd.add_argument("--filter-name", default=None, help="Optional filter name override when the plan has multiple filter actions.")
    run_filter_flow_cmd.add_argument("--option-alias", default=None, help="Optional option alias/value override.")
    run_filter_flow_cmd.add_argument(
        "--all-filters",
        action="store_true",
        help="Author all filter actions from the automation plan in sequence instead of only one filter.",
    )
    run_filter_flow_cmd.add_argument(
        "--through",
        default="save-dashboard",
        choices=[
            "prepare",
            "open-filter",
            "open-filter-field",
            "open-filter-value",
            "select-filter-option",
            "apply-filter-value",
            "commit-dashboard-filter",
            "save-dashboard",
            "verify-dashboard",
        ],
        help="Final flow stage to execute before stopping.",
    )
    run_filter_flow_cmd.add_argument(
        "--verify-package",
        default=None,
        help="Optional build_package.json path for a follow-on salesforce_dashboard_executor.py verify run.",
    )
    run_filter_flow_cmd.add_argument(
        "--manual-filter-authoring-json",
        default=None,
        help="Optional manual filter artifact override; defaults to the sibling salesforce_dashboard_manual_filter_authoring.json beside the plan.",
    )
    run_filter_flow_cmd.add_argument(
        "--verify-output-dir",
        default=None,
        help="Optional output directory for generated verification artifacts and dashboard verify output.",
    )
    run_filter_flow_cmd.add_argument(
        "--dashboard-executor-script",
        default=str(DASHBOARD_EXECUTOR_SCRIPT),
        help="Path to salesforce_dashboard_executor.py or a compatible verify CLI.",
    )
    run_filter_flow_cmd.add_argument("--session", required=True, help="Playwright session name to use.")
    run_filter_flow_cmd.add_argument("--output-dir", default=None, help="Optional root output directory for emitted stage artifacts.")
    run_filter_flow_cmd.add_argument(
        "--playwright-wrapper",
        default=str(DEFAULT_PLAYWRIGHT_WRAPPER),
        help="Path to the Playwright CLI wrapper script.",
    )
    run_filter_flow_cmd.add_argument("--json", action="store_true", help="Print JSON output.")

    return parser


def _print_text(result: dict[str, Any]) -> None:
    print(f"[{result['status']}] {result['tool']} {result['command']}")
    for message in result.get("messages", []):
        print(f"- {message['level']}: {message['code']}: {message['text']}")
    summary = result.get("summary") or {}
    if summary:
        print(json.dumps(summary, indent=2))


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "validate":
        result = validate_plan(Path(args.plan))
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_text(result)
        return 0 if result["status"] == "ok" else 1

    if args.command == "open-filter":
        exit_code, result = open_filter_from_prepare(
            prepare_path=Path(args.prepare_artifact),
            plan_path=Path(args.plan) if args.plan else None,
            session_override=args.session,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            playwright_wrapper=Path(args.playwright_wrapper),
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_text(result)
        return exit_code

    if args.command == "open-filter-field":
        exit_code, result = open_filter_field_from_artifact(
            open_filter_path=Path(args.open_filter_artifact),
            plan_path=Path(args.plan) if args.plan else None,
            filter_name_override=args.filter_name,
            session_override=args.session,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            playwright_wrapper=Path(args.playwright_wrapper),
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_text(result)
        return exit_code

    if args.command == "open-filter-value":
        exit_code, result = open_filter_value_from_field_artifact(
            field_artifact_path=Path(args.field_artifact),
            plan_path=Path(args.plan) if args.plan else None,
            filter_name_override=args.filter_name,
            session_override=args.session,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            playwright_wrapper=Path(args.playwright_wrapper),
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_text(result)
        return exit_code

    if args.command == "select-filter-option":
        exit_code, result = select_filter_option_from_value_artifact(
            value_artifact_path=Path(args.value_artifact),
            plan_path=Path(args.plan) if args.plan else None,
            filter_name_override=args.filter_name,
            option_alias_override=args.option_alias,
            session_override=args.session,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            playwright_wrapper=Path(args.playwright_wrapper),
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_text(result)
        return exit_code

    if args.command == "apply-filter-value":
        exit_code, result = apply_filter_value_from_option_artifact(
            option_artifact_path=Path(args.option_artifact),
            session_override=args.session,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            playwright_wrapper=Path(args.playwright_wrapper),
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_text(result)
        return exit_code

    if args.command == "commit-dashboard-filter":
        exit_code, result = commit_dashboard_filter_from_apply_artifact(
            apply_artifact_path=Path(args.apply_artifact),
            session_override=args.session,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            playwright_wrapper=Path(args.playwright_wrapper),
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_text(result)
        return exit_code

    if args.command == "save-dashboard":
        exit_code, result = save_dashboard_from_commit_artifact(
            commit_artifact_path=Path(args.commit_artifact),
            session_override=args.session,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            playwright_wrapper=Path(args.playwright_wrapper),
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_text(result)
        return exit_code

    if args.command == "run-filter-flow":
        exit_code, result = run_filter_flow(
            plan_path=Path(args.plan),
            target_org_override=args.target_org,
            dashboard_id_override=args.dashboard_id,
            filter_name_override=args.filter_name,
            option_alias_override=args.option_alias,
            through_stage=args.through,
            session=args.session,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            playwright_wrapper=Path(args.playwright_wrapper),
            all_filters=args.all_filters,
            verify_package=Path(args.verify_package) if args.verify_package else None,
            manual_filter_authoring_json=Path(args.manual_filter_authoring_json) if args.manual_filter_authoring_json else None,
            verify_output_dir=Path(args.verify_output_dir) if args.verify_output_dir else None,
            dashboard_executor_script=Path(args.dashboard_executor_script),
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_text(result)
        return exit_code

    exit_code, result = prepare_plan(
        plan_path=Path(args.plan),
        target_org_override=args.target_org,
        dashboard_id_override=args.dashboard_id,
        session=args.session,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        playwright_wrapper=Path(args.playwright_wrapper),
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        _print_text(result)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
