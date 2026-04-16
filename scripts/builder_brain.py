#!/usr/bin/env python3
"""CLI-first builder brain for reports, native dashboards, and CRMA."""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import ai_os_browser
import analytics_intelligence as ai  # noqa: E402
import report_surface_intelligence


PROFILES_PATH = ROOT / "config" / "builder_brain_profiles.json"
EXCELLENCE_PATH = ROOT / "config" / "builder_brain_excellence_library.json"
HANDOFF_TARGETS_PATH = ROOT / "config" / "builder_brain_handoff_targets.json"
REPORT_EXECUTOR_SCRIPT = ROOT / "scripts" / "salesforce_report_executor.py"
DASHBOARD_EXECUTOR_SCRIPT = ROOT / "scripts" / "salesforce_dashboard_executor.py"
DEFAULT_DASHBOARD_FILTER_AUTOMATION_SCRIPT = ROOT / "scripts" / "salesforce_dashboard_filter_automation.py"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_planning_context(
    *,
    plan_path: Path | None,
    evaluation_path: Path | None,
    query: str | None = None,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
) -> dict[str, Any] | None:
    if plan_path is None and evaluation_path is None:
        return None

    planning_context: dict[str, Any] = {}
    if plan_path is not None:
        plan_payload = load_json(plan_path)
        planning_context["plan_path"] = str(plan_path)
        if isinstance(plan_payload.get("run_id"), str) and plan_payload["run_id"]:
            planning_context["run_id"] = plan_payload["run_id"]
        if isinstance(plan_payload.get("goal"), str) and plan_payload["goal"]:
            planning_context["goal"] = plan_payload["goal"]
        resolved = plan_payload.get("resolved")
        if isinstance(resolved, dict):
            for source_key, target_key in (
                ("persona", "persona"),
                ("domain", "domain"),
                ("operation", "operation"),
            ):
                value = resolved.get(source_key)
                if isinstance(value, str) and value:
                    planning_context[target_key] = value
        route = plan_payload.get("route")
        if isinstance(route, dict):
            surface_type = route.get("recommended_surface_type")
            if isinstance(surface_type, str) and surface_type:
                planning_context["surface_type"] = surface_type
        candidate_surface = plan_payload.get("candidate_surface")
        if isinstance(candidate_surface, dict):
            candidate_surface_id = candidate_surface.get("id")
            if isinstance(candidate_surface_id, str) and candidate_surface_id:
                planning_context["candidate_surface_id"] = candidate_surface_id
        required_evidence = plan_payload.get("required_evidence")
        if isinstance(required_evidence, list) and required_evidence:
            planning_context["required_evidence"] = [
                item for item in required_evidence if isinstance(item, str) and item
            ]
        memory_summary = plan_payload.get("memory_summary")
        if isinstance(memory_summary, dict):
            memory_health = memory_summary.get("memory_health")
            if isinstance(memory_health, dict) and memory_health:
                planning_context["memory_health"] = memory_health

    if evaluation_path is not None:
        evaluation_payload = load_json(evaluation_path)
        evaluation = evaluation_payload.get("evaluation")
        if not isinstance(evaluation, dict):
            evaluation = evaluation_payload
        planning_context["evaluation_path"] = str(evaluation_path)
        verdict = evaluation.get("verdict")
        if isinstance(verdict, str) and verdict:
            planning_context["evaluation_verdict"] = verdict
        if "run_id" not in planning_context:
            run_id = evaluation.get("run_id")
            if isinstance(run_id, str) and run_id:
                planning_context["run_id"] = run_id

    if "goal" not in planning_context and isinstance(query, str) and query:
        planning_context["goal"] = query
    if "persona" not in planning_context and isinstance(persona, str) and persona:
        planning_context["persona"] = persona
    if "domain" not in planning_context and isinstance(domain, str) and domain:
        planning_context["domain"] = domain
    if "operation" not in planning_context and isinstance(operation, str) and operation:
        planning_context["operation"] = operation

    return planning_context


def _resolve_manifest_relative_path(manifest_path: Path, raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate
    return (manifest_path.expanduser().resolve().parent / candidate).resolve()


def load_inputs() -> dict[str, Any]:
    inputs = ai.load_inputs()
    inputs["builder_profiles"] = load_json(PROFILES_PATH)
    inputs["builder_excellence"] = load_json(EXCELLENCE_PATH)
    if HANDOFF_TARGETS_PATH.exists():
        inputs["builder_brain_handoff_targets"] = load_json(HANDOFF_TARGETS_PATH)
    else:
        inputs["builder_brain_handoff_targets"] = {"version": 1, "targets": []}
    return inputs


def make_result(
    *,
    status: str,
    command: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "builder_brain",
        "lane": "intelligence_control",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return payload


def _run_json_command(command: list[str], *, timeout_seconds: int | None = None) -> tuple[int, dict[str, Any]]:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Command timed out after {timeout_seconds} second(s): {' '.join(command)}"
        ) from exc
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


def _package_probe_slug(package_path: Path) -> str:
    stem = package_path.stem
    if stem == "build_package":
        stem = package_path.parent.name or stem
    return re.sub(r"[^a-z0-9]+", "_", stem.lower()).strip("_") or "builder_probe"


def _primary_lane_from_package(build_package_payload: dict[str, Any]) -> str | None:
    execution_lane = build_package_payload.get("execution_lane")
    if isinstance(execution_lane, str) and execution_lane:
        return execution_lane
    surface_contract = build_package_payload.get("surface_contract")
    if not isinstance(surface_contract, dict):
        return None
    surface_type = surface_contract.get("surface_type")
    lane_by_surface = {
        "salesforce_report": "salesforce_report_handoff",
        "salesforce_dashboard": "salesforce_dashboard_handoff",
        "crma_dashboard": "crma_dashboard_handoff",
    }
    if isinstance(surface_type, str):
        return lane_by_surface.get(surface_type)
    return None


def _build_handoff_from_package(
    inputs: dict[str, Any],
    *,
    package_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    build_package_payload = load_json(package_path)
    primary_lane = _primary_lane_from_package(build_package_payload)
    if primary_lane:
        build_package_payload = deepcopy(build_package_payload)
        build_package_payload["execution_lane"] = primary_lane

    surface_contract = build_package_payload.get("surface_contract")
    if not isinstance(surface_contract, dict):
        return make_result(
            status="error",
            command="handoff",
            messages=[
                ai.make_message(
                    "error",
                    "invalid_build_package",
                    "Provided build package is missing a surface_contract block.",
                )
            ],
        )

    surface_type = surface_contract.get("surface_type")
    if not isinstance(surface_type, str) or not surface_type:
        return make_result(
            status="error",
            command="handoff",
            messages=[
                ai.make_message(
                    "error",
                    "invalid_build_package",
                    "Provided build package is missing surface_contract.surface_type.",
                )
            ],
        )

    spec = {
        "primary_surface": surface_type,
        "candidate_surface_labels": [],
        "reference_exemplar": None,
    }
    draft = {
        "handoff_surface": surface_contract.get("handoff_surface"),
    }
    executor_handoff, artifacts = build_executor_handoff(
        inputs=inputs,
        spec=spec,
        draft=draft,
        build_package_payload=build_package_payload,
        output_dir=output_dir,
    )
    source_reference_path = output_dir / "provided_package_reference.json"
    source_reference_path.write_text(
        json.dumps(
            {
                "source_package_path": str(package_path),
                "resolved_primary_lane": executor_handoff.get("primary_lane"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    artifacts.append({"type": "provided_build_package_source", "path": str(source_reference_path)})
    return make_result(
        status="ok",
        command="handoff",
        messages=[
            ai.make_message(
                "warn",
                "handoff_package_reused",
                "Reused a provided build package instead of rebuilding a fresh handoff from query routing.",
            )
        ],
        artifacts=artifacts,
        executor_handoff=executor_handoff,
    )


def _matrix_entry_slug(index: int, entry: dict[str, Any]) -> str:
    seed = entry.get("name") or entry.get("package") or entry.get("query") or f"probe_{index + 1}"
    if not isinstance(seed, str):
        seed = f"probe_{index + 1}"
    normalized = re.sub(r"[^a-z0-9]+", "_", seed.lower()).strip("_")
    return f"{index + 1:02d}_{normalized or f'probe_{index + 1}'}"


def _recover_probe_asset_from_apply_artifact(
    *,
    primary_lane: str,
    execute_output_dir: Path,
) -> tuple[str | None, list[dict[str, str]], list[dict[str, str]]]:
    artifact_name_by_lane = {
        "salesforce_report_handoff": "salesforce_report_apply_response.json",
        "salesforce_dashboard_handoff": "salesforce_dashboard_apply_response.json",
    }
    artifact_type_by_lane = {
        "salesforce_report_handoff": "salesforce_report_apply_response",
        "salesforce_dashboard_handoff": "salesforce_dashboard_apply_response",
    }
    response_id_key_by_lane = {
        "salesforce_report_handoff": "report_id",
        "salesforce_dashboard_handoff": "dashboard_id",
    }
    cloned_id_key_by_lane = {
        "salesforce_report_handoff": "cloned_report_id",
        "salesforce_dashboard_handoff": "cloned_dashboard_id",
    }

    artifact_name = artifact_name_by_lane.get(primary_lane)
    if not artifact_name:
        return None, [], []

    artifact_path = execute_output_dir / "01_apply" / artifact_name
    if not artifact_path.exists():
        return None, [], []

    artifacts = [{"type": artifact_type_by_lane[primary_lane], "path": str(artifact_path)}]
    try:
        payload = load_json(artifact_path)
    except Exception as exc:
        return None, artifacts, [ai.make_message("warn", "probe_apply_artifact_unreadable", str(exc))]

    recovered_asset_id = payload.get(cloned_id_key_by_lane[primary_lane])
    if not isinstance(recovered_asset_id, str) or not recovered_asset_id:
        responses = payload.get("responses")
        if isinstance(responses, list):
            response_id_key = response_id_key_by_lane[primary_lane]
            for item in reversed(responses):
                if not isinstance(item, dict):
                    continue
                recovered_asset_id = item.get(response_id_key)
                if isinstance(recovered_asset_id, str) and recovered_asset_id:
                    break

    if isinstance(recovered_asset_id, str) and recovered_asset_id:
        return recovered_asset_id, artifacts, [
            ai.make_message(
                "warn",
                "probe_asset_recovered",
                f"Recovered live asset id {recovered_asset_id} from a partial apply artifact after probe failure.",
            )
        ]
    return None, artifacts, [
        ai.make_message(
            "warn",
            "probe_asset_recovery_failed",
            "Found a partial apply artifact but could not recover a live asset id from it.",
        )
    ]


def _attempt_failed_probe_cleanup(
    *,
    primary_lane: str,
    execute_output_dir: Path,
    delete_command_template: list[str] | None,
    applied_asset_id: str | None,
    cleanup_requested: bool,
    executor_timeout_seconds: int | None,
) -> tuple[str | None, dict[str, Any] | None, list[dict[str, str]], list[dict[str, str]]]:
    if not cleanup_requested or delete_command_template is None:
        return applied_asset_id, None, [], []

    recovered_artifacts: list[dict[str, str]] = []
    recovery_messages: list[dict[str, str]] = []
    effective_asset_id = applied_asset_id

    if not isinstance(effective_asset_id, str) or not effective_asset_id:
        effective_asset_id, recovered_artifacts, recovery_messages = _recover_probe_asset_from_apply_artifact(
            primary_lane=primary_lane,
            execute_output_dir=execute_output_dir,
        )

    if not isinstance(effective_asset_id, str) or not effective_asset_id:
        recovery_messages.append(
            ai.make_message(
                "warn",
                "probe_cleanup_unavailable",
                "Probe cleanup was requested, but no live asset id could be recovered after the failed execution.",
            )
        )
        return None, None, recovered_artifacts, recovery_messages

    delete_command = [effective_asset_id if item == "__FILL_ASSET_ID__" else item for item in delete_command_template]
    try:
        cleanup_exit_code, cleanup_result = _run_json_command(
            delete_command,
            timeout_seconds=executor_timeout_seconds,
        )
    except Exception as exc:
        recovery_messages.append(ai.make_message("error", "probe_cleanup_failed", str(exc)))
        return effective_asset_id, None, recovered_artifacts, recovery_messages

    if cleanup_exit_code != 0 and cleanup_result.get("status") == "ok":
        recovery_messages.append(
            ai.make_message(
                "warn",
                "probe_cleanup_exit_nonzero",
                "Probe cleanup returned a non-zero exit code even though the delete payload reported ok.",
            )
        )
    return effective_asset_id, cleanup_result, recovered_artifacts, recovery_messages


def _question_id(resolution: dict[str, Any]) -> str | None:
    question = resolution.get("question")
    if isinstance(question, dict):
        question_id = question.get("id")
        return question_id if isinstance(question_id, str) else None
    return None


def _candidate_surface(resolution: dict[str, Any]) -> dict[str, Any] | None:
    candidates = resolution.get("candidate_surfaces", [])
    if candidates:
        return candidates[0]
    return None


def _pattern_catalog(inputs: dict[str, Any]) -> dict[str, Any]:
    return inputs["builder_excellence"]["patterns"]


def _exemplar_catalog(inputs: dict[str, Any]) -> dict[str, Any]:
    return {item["id"]: item for item in inputs["builder_excellence"]["surface_exemplars"]}


def _reference_exemplar(inputs: dict[str, Any], candidate: dict[str, Any] | None) -> dict[str, Any] | None:
    if not candidate:
        return None
    exemplars = _exemplar_catalog(inputs)
    exemplar = exemplars.get(candidate.get("id"))
    if exemplar:
        return exemplar
    return None


def _surface_override(value: str | None) -> str | None:
    if not value:
        return None
    normalized = ai.normalize_text(value).replace(" ", "_")
    return normalized or None


def _select_primary_surface(
    *,
    resolution: dict[str, Any],
    route: dict[str, Any],
    profiles: dict[str, Any],
    surface_override: str | None,
    metric_roles: list[dict[str, str]],
    recommended_filters: list[str],
) -> tuple[str, str | None, str]:
    adapters = profiles["surface_adapters"]
    if surface_override:
        secondary = None
        if route["recommended_surface_type"] == "hybrid":
            secondary = adapters[surface_override].get("default_handoff_surface")
            if secondary == surface_override:
                secondary = None
        return surface_override, secondary, "explicit_override"

    recommended = route["recommended_surface_type"]
    if recommended in adapters and recommended != "hybrid":
        return recommended, None, "route_recommendation"

    if recommended == "hybrid":
        query_text = ai.normalize_text(resolution["query"])
        persona = resolution.get("resolved_persona")
        question_id = _question_id(resolution)
        if (
            any(token in query_text for token in ("report", "owner list", "follow up", "follow-up", "tabular"))
            or question_id in {"which_deals_need_intervention", "who_needs_work_now", "renewal_risk_queue"}
        ) and persona in {"manager", "individual"}:
            report_probe = _report_action_surface_assessment(
                {
                    "query": resolution["query"],
                    "persona": persona,
                    "domain": resolution.get("resolved_domain"),
                    "question_id": question_id,
                    "metric_roles": metric_roles,
                    "recommended_filters": recommended_filters,
                    "primary_surface": "salesforce_report",
                    "secondary_surface": "crma_dashboard",
                    "excellence_target": {},
                }
            )
            if report_probe.get("verdict") == "weak_follow_up_fit" or (
                not report_probe.get("queue_ready_format") and not _queue_like(question_id)
            ):
                return "crma_dashboard", "salesforce_report", "hybrid_story_plus_action_report_demoted"
            return "salesforce_report", "crma_dashboard", "hybrid_queue_handoff"
        if any(token in query_text for token in ("native dashboard", "simple summary", "headline", "headline rollup")):
            return "salesforce_dashboard", "salesforce_report", "hybrid_native_summary"
        return "crma_dashboard", "salesforce_report", "hybrid_story_plus_action"

    return "crma_dashboard", None, "repo_default"


def _build_mode(
    *,
    candidate: dict[str, Any] | None,
    route: dict[str, Any],
    primary_surface: str,
    secondary_surface: str | None,
    surface_override: str | None,
) -> str:
    if secondary_surface:
        return "paired_handoff"
    if candidate and candidate.get("live_urls"):
        if route["operation_mode"] in {"review_dashboard", "mutate_dashboard", "export_assets"}:
            return "refine_existing"
        if surface_override and surface_override != route["recommended_surface_type"]:
            return "new_surface"
        return "reuse_existing"
    return "new_surface"


def _choose_excellence_target(
    *,
    inputs: dict[str, Any],
    resolution: dict[str, Any],
    candidate: dict[str, Any] | None,
    primary_surface: str,
    secondary_surface: str | None,
    build_mode: str,
) -> dict[str, Any]:
    patterns = _pattern_catalog(inputs)
    exemplars = _exemplar_catalog(inputs)
    persona = resolution.get("resolved_persona")
    domain = resolution.get("resolved_domain")
    question_id = _question_id(resolution)

    if candidate and candidate.get("id") in exemplars:
        exemplar = exemplars[candidate["id"]]
        pattern = patterns[exemplar["pattern_id"]]
        if exemplar["primary_surface"] == primary_surface:
            return {
                "kind": "exemplar",
                "target_id": exemplar["id"],
                "label": exemplar["label"],
                "pattern_id": exemplar["pattern_id"],
                "primary_surface": exemplar["primary_surface"],
                "preferred_secondary_surface": pattern.get("preferred_secondary_surface"),
                "preferred_page_model": exemplar.get("page_model", pattern.get("preferred_page_model", [])),
                "preferred_report_format": pattern.get("preferred_report_format"),
                "required_sections": pattern.get("required_sections", []),
                "max_pages": pattern.get("max_pages"),
                "requires_action_layer": pattern.get("requires_action_layer", False),
                "requires_handoff": pattern.get("requires_handoff", False),
                "why": exemplar.get("why_excellent", []),
                "source": "candidate_surface",
            }

    ranked_patterns: list[tuple[int, str, dict[str, Any]]] = []
    for pattern_id, pattern in patterns.items():
        score = 0
        if pattern["primary_surface"] == primary_surface:
            score += 5
        if persona in pattern.get("personas", []):
            score += 3
        if secondary_surface and pattern.get("preferred_secondary_surface") == secondary_surface:
            score += 2
        if build_mode == "paired_handoff" and pattern.get("requires_handoff"):
            score += 2
        if question_id in {"which_deals_need_intervention", "who_needs_work_now", "renewal_risk_queue"}:
            if pattern_id in {"manager_operating_system", "owner_accountability_report"}:
                score += 1
        if domain == "retention" and pattern_id in {"single_page_executive_truth", "manager_operating_system", "cross_suite_control_tower", "owner_accountability_report"}:
            score += 1
        if domain == "product_gtm" and pattern_id == "executive_mix_snapshot":
            score += 2
        if persona == "individual" and pattern_id == "queue_first_rep_surface":
            score += 2
        ranked_patterns.append((score, pattern_id, pattern))

    ranked_patterns.sort(key=lambda item: (-item[0], item[1]))
    _, pattern_id, pattern = ranked_patterns[0]
    return {
        "kind": "pattern",
        "target_id": pattern_id,
        "label": pattern_id.replace("_", " "),
        "pattern_id": pattern_id,
        "primary_surface": pattern["primary_surface"],
        "preferred_secondary_surface": pattern.get("preferred_secondary_surface"),
        "preferred_page_model": pattern.get("preferred_page_model", []),
        "preferred_report_format": pattern.get("preferred_report_format"),
        "required_sections": pattern.get("required_sections", []),
        "max_pages": pattern.get("max_pages"),
        "requires_action_layer": pattern.get("requires_action_layer", False),
        "requires_handoff": pattern.get("requires_handoff", False),
        "why": pattern.get("why", []),
        "source": "pattern_library",
    }


def _score_pattern_match(
    *,
    pattern_id: str,
    pattern: dict[str, Any],
    resolution: dict[str, Any],
    primary_surface: str,
    secondary_surface: str | None,
) -> int:
    persona = resolution.get("resolved_persona")
    domain = resolution.get("resolved_domain")
    question_id = _question_id(resolution)
    score = 0
    if pattern["primary_surface"] == primary_surface:
        score += 5
    if persona in pattern.get("personas", []):
        score += 3
    if secondary_surface and pattern.get("preferred_secondary_surface") == secondary_surface:
        score += 2
    if question_id in {"which_deals_need_intervention", "who_needs_work_now", "renewal_risk_queue"}:
        if pattern_id in {"manager_operating_system", "owner_accountability_report", "cross_suite_control_tower"}:
            score += 2
    if domain == "retention" and pattern_id in {
        "single_page_executive_truth",
        "manager_operating_system",
        "cross_suite_control_tower",
        "owner_accountability_report",
    }:
        score += 1
    if domain == "product_gtm" and pattern_id == "executive_mix_snapshot":
        score += 2
    if persona == "individual" and pattern_id == "queue_first_rep_surface":
        score += 2
    return score


def _retrieve_design_context(
    *,
    inputs: dict[str, Any],
    resolution: dict[str, Any],
    candidate: dict[str, Any] | None,
    primary_surface: str,
    secondary_surface: str | None,
    excellence_target: dict[str, Any],
    reference_exemplar: dict[str, Any] | None,
) -> dict[str, Any]:
    patterns = _pattern_catalog(inputs)
    exemplars = _exemplar_catalog(inputs)
    query_text = ai.normalize_text(resolution["query"])
    query_tokens = set(ai.tokenize(resolution["query"]))

    ranked_patterns: list[dict[str, Any]] = []
    for pattern_id, pattern in patterns.items():
        score = _score_pattern_match(
            pattern_id=pattern_id,
            pattern=pattern,
            resolution=resolution,
            primary_surface=primary_surface,
            secondary_surface=secondary_surface,
        )
        cues = [
            *pattern.get("why", []),
            f"Keep the page budget at {pattern.get('max_pages')} or less." if pattern.get("max_pages") else "",
            (
                f"Preserve the {pattern.get('preferred_secondary_surface')} handoff."
                if pattern.get("preferred_secondary_surface")
                else ""
            ),
        ]
        ranked_patterns.append(
            {
                "id": pattern_id,
                "label": pattern_id.replace("_", " "),
                "score": score,
                "primary_surface": pattern["primary_surface"],
                "preferred_page_model": pattern.get("preferred_page_model", []),
                "required_sections": pattern.get("required_sections", []),
                "preferred_secondary_surface": pattern.get("preferred_secondary_surface"),
                "cues": [cue for cue in cues if cue],
            }
        )
    ranked_patterns.sort(key=lambda item: (-item["score"], item["id"]))

    ranked_exemplars: list[dict[str, Any]] = []
    for exemplar in exemplars.values():
        text_chunks = [
            exemplar["id"].replace("_", " "),
            exemplar["label"],
            *exemplar.get("page_model", []),
            *exemplar.get("why_excellent", []),
        ]
        score = ai.alias_score(query_text, query_tokens, text_chunks)
        if exemplar["primary_surface"] == primary_surface:
            score += 5
        if candidate and exemplar["id"] == candidate.get("id"):
            score += 6
        if exemplar["pattern_id"] == excellence_target.get("pattern_id"):
            score += 3
        if reference_exemplar and exemplar["id"] == reference_exemplar.get("id"):
            score += 2
        ranked_exemplars.append(
            {
                "id": exemplar["id"],
                "label": exemplar["label"],
                "score": score,
                "pattern_id": exemplar["pattern_id"],
                "primary_surface": exemplar["primary_surface"],
                "page_model": exemplar.get("page_model", []),
                "cues": exemplar.get("why_excellent", []),
            }
        )
    ranked_exemplars.sort(key=lambda item: (-item["score"], item["id"]))

    aggregated_cues: list[str] = []
    for item in ranked_patterns[:2]:
        aggregated_cues.extend(item.get("cues", []))
    for item in ranked_exemplars[:2]:
        aggregated_cues.extend(item.get("cues", []))
    deduped_cues = list(dict.fromkeys(cue for cue in aggregated_cues if cue))

    return {
        "patterns": ranked_patterns[:3],
        "exemplars": ranked_exemplars[:3],
        "design_cues": deduped_cues[:6],
    }


def _default_filters(profiles: dict[str, Any], domain: str | None) -> list[str]:
    if domain and domain in profiles["domain_filters"]:
        return profiles["domain_filters"][domain]
    return ["fiscal_period", "region", "owner"]


def _metric_roles(candidate: dict[str, Any] | None, domain: str | None) -> list[dict[str, str]]:
    focus = candidate.get("kpi_focus", []) if candidate else []
    roles = ["headline_metric", "driver_metric", "risk_metric", "action_metric"]
    if not focus:
        fallback_by_domain = {
            "revenue": ["Forecast", "Pipeline Coverage", "Win Rate", "Deals Requiring Action"],
            "demand": ["Response SLA", "Qualification Volume", "Meeting Conversion", "Leads Requiring Action"],
            "customer": ["Health", "Coverage", "Risk", "Accounts Requiring Action"],
            "retention": ["Renewal Amount", "Renewal Risk", "Churn Risk", "Renewals Requiring Action"],
            "product_gtm": ["Product Mix", "Segment Share", "Whitespace", "Accounts Requiring Action"],
            "productivity": ["Activity Rate", "Cycle Time", "Bottleneck Risk", "Items Requiring Action"],
        }
        focus = fallback_by_domain.get(domain or "", ["Primary KPI", "Driver KPI", "Risk KPI", "Action KPI"])
    payload: list[dict[str, str]] = []
    for index, role in enumerate(roles):
        metric = focus[index] if index < len(focus) else focus[-1]
        payload.append({"role": role, "metric": metric})
    return payload


def _decision_statement(
    resolution: dict[str, Any],
    primary_surface: str,
    secondary_surface: str | None,
) -> str:
    persona = resolution.get("resolved_persona") or "operator"
    domain = resolution.get("resolved_domain") or "business"
    question = _question_id(resolution) or "core decision"
    if question == "core decision":
        base = (
            f"Build a {primary_surface.replace('_', ' ')} for the {persona} persona "
            f"to answer: {resolution['query']}."
        )
    else:
        base = (
            f"Build a {primary_surface.replace('_', ' ')} for the {persona} persona "
            f"to answer the {question.replace('_', ' ')} question in the {domain} domain."
        )
    if secondary_surface:
        return f"{base} Add a {secondary_surface.replace('_', ' ')} handoff for follow-up and action."
    return base


def build_spec(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    surface_override: str | None,
) -> dict[str, Any]:
    resolution = ai.resolve_request(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
    )
    route = ai.route_surface(inputs, resolution)
    review = ai.build_review_plan(inputs, resolved=resolution, route=route)
    builder_profiles = inputs["builder_profiles"]
    candidate = _candidate_surface(resolution)
    recommended_filters = _default_filters(builder_profiles, resolution.get("resolved_domain"))
    metric_roles = _metric_roles(candidate, resolution.get("resolved_domain"))
    default_primary_surface, default_secondary_surface, _ = _select_primary_surface(
        resolution=resolution,
        route=route,
        profiles=builder_profiles,
        surface_override=None,
        metric_roles=metric_roles,
        recommended_filters=recommended_filters,
    )

    primary_surface, secondary_surface, selection_reason = _select_primary_surface(
        resolution=resolution,
        route=route,
        profiles=builder_profiles,
        surface_override=surface_override,
        metric_roles=metric_roles,
        recommended_filters=recommended_filters,
    )
    reference_exemplar = _reference_exemplar(inputs, candidate)
    build_mode = _build_mode(
        candidate=candidate,
        route=route,
        primary_surface=primary_surface,
        secondary_surface=secondary_surface,
        surface_override=surface_override,
    )
    excellence_target = _choose_excellence_target(
        inputs=inputs,
        resolution=resolution,
        candidate=candidate,
        primary_surface=primary_surface,
        secondary_surface=secondary_surface,
        build_mode=build_mode,
    )
    retrieval_context = _retrieve_design_context(
        inputs=inputs,
        resolution=resolution,
        candidate=candidate,
        primary_surface=primary_surface,
        secondary_surface=secondary_surface,
        excellence_target=excellence_target,
        reference_exemplar=reference_exemplar,
    )
    primary_adapter = builder_profiles["surface_adapters"][primary_surface]
    persona_key = resolution.get("resolved_persona") or "manager"
    persona_layout = builder_profiles["persona_layouts"].get(
        persona_key,
        builder_profiles["persona_layouts"]["manager"],
    )

    spec = {
        "query": query,
        "persona": resolution.get("resolved_persona"),
        "domain": resolution.get("resolved_domain"),
        "operation": route["operation_mode"],
        "question_id": _question_id(resolution),
        "candidate_surface_id": candidate.get("id") if candidate else None,
        "candidate_surface_labels": candidate.get("labels", []) if candidate else [],
        "reference_exemplar": reference_exemplar,
        "selection_reason": selection_reason,
        "build_mode": build_mode,
        "primary_surface": primary_surface,
        "secondary_surface": secondary_surface,
        "builder_default_primary_surface": default_primary_surface,
        "builder_default_secondary_surface": default_secondary_surface,
        "excellence_target": excellence_target,
        "retrieval_context": retrieval_context,
        "decision_statement": _decision_statement(resolution, primary_surface, secondary_surface),
        "primary_adapter": primary_adapter["draft_shape"],
        "persona_layout": persona_layout["section_order"],
        "recommended_filters": recommended_filters,
        "metric_roles": metric_roles,
        "review_gate_ids": [gate["id"] for gate in review["gates"]],
        "routing": {
            "recommended_surface_type": route["recommended_surface_type"],
            "surface_scores": route["surface_scores"],
        },
        "drill_path": {
            "primary": "dashboard detail page" if primary_surface != "salesforce_report" else "row-level owner list",
            "secondary": secondary_surface,
        },
        "action_path": (
            "manager queue or follow-up report"
            if resolution.get("resolved_persona") in {"manager", "individual"}
            else "executive summary to manager handoff"
        ),
    }
    if primary_surface == "salesforce_report" or secondary_surface == "salesforce_report":
        spec["report_action_surface_assessment"] = _report_action_surface_assessment(spec)
    return spec


def _queue_like(question_id: str | None) -> bool:
    return question_id in {"which_deals_need_intervention", "who_needs_work_now", "renewal_risk_queue"}


def _report_format(spec: dict[str, Any]) -> str:
    target = spec.get("excellence_target", {})
    preferred_report_format = target.get("preferred_report_format")
    if isinstance(preferred_report_format, str):
        return preferred_report_format
    query_text = ai.normalize_text(spec["query"])
    if _queue_like(spec.get("question_id")) or any(
        token in query_text for token in ("owner list", "follow up", "follow-up", "queue", "tabular")
    ):
        return "tabular"
    if spec.get("persona") == "executive":
        return "summary"
    return "matrix"


def _report_columns(spec: dict[str, Any]) -> list[str]:
    domain = spec.get("domain")
    metric_names = [item["metric"] for item in spec["metric_roles"][:2]]
    common = ["Owner", "Account"]
    if domain == "revenue":
        common.extend(["Opportunity", "Forecast Category", "Close Date"])
    elif domain == "retention":
        common.extend(["Renewal Date", "Risk Band", "Product Family"])
    elif domain == "demand":
        common.extend(["Lead Status", "Lead Source", "Next Step"])
    elif domain == "customer":
        common.extend(["Health Band", "Coverage Gap", "Next Action"])
    else:
        common.extend(["Segment", "Status", "Next Action"])
    return common + metric_names


def _group_by(spec: dict[str, Any]) -> list[str]:
    persona = spec.get("persona")
    domain = spec.get("domain")
    if persona == "executive":
        return ["Region", "Manager"]
    if domain in {"revenue", "retention"}:
        return ["Manager", "Owner"]
    return ["Team", "Owner"]


def _report_surface_contract_for_assessment(
    spec: dict[str, Any],
    draft: dict[str, Any] | None = None,
) -> dict[str, Any]:
    handoff_surface = None
    if draft is not None:
        handoff_surface = draft.get("handoff_surface")
    if not isinstance(handoff_surface, str) or not handoff_surface:
        if spec.get("primary_surface") == "salesforce_report":
            handoff_surface = spec.get("secondary_surface")
        else:
            handoff_surface = spec.get("primary_surface")
    if not isinstance(handoff_surface, str) or not handoff_surface or handoff_surface == "salesforce_report":
        handoff_surface = "crma_dashboard"

    metric_roles = spec.get("metric_roles", [])
    action_metric = None
    if isinstance(metric_roles, list) and len(metric_roles) >= 4 and isinstance(metric_roles[3], dict):
        action_metric = metric_roles[3].get("metric")

    return {
        "surface_type": "salesforce_report",
        "report_format": draft.get("report_format") if draft is not None else _report_format(spec),
        "group_by": draft.get("group_by", []) if draft is not None else _group_by(spec),
        "columns": draft.get("columns", []) if draft is not None else _report_columns(spec),
        "filters": draft.get("filters", []) if draft is not None else spec.get("recommended_filters", []),
        "sort_by": (
            draft.get("sort_by", [])
            if draft is not None
            else [item for item in (action_metric, "Owner") if isinstance(item, str) and item]
        ),
        "handoff_surface": handoff_surface,
        "page_blueprint": draft.get("page_blueprint", []) if draft is not None else [],
        "handoff_target": {
            "surface_type": handoff_surface,
            "destination_type": "report" if handoff_surface == "salesforce_report" else "dashboard",
        },
    }


def _report_action_surface_assessment(
    spec: dict[str, Any],
    draft: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return report_surface_intelligence.assess_report_action_surface_contract(
        _report_surface_contract_for_assessment(spec, draft)
    )


def _dashboard_blocks(spec: dict[str, Any], inputs: dict[str, Any]) -> list[dict[str, Any]]:
    widget_profiles = inputs["widget_profiles"]
    domain = spec.get("domain")
    question_id = spec.get("question_id")
    question_cfg = None
    if domain and question_id:
        question_cfg = widget_profiles.get("domains", {}).get(domain, {}).get("common_questions", {}).get(question_id)
    primary_widget = "comparisontable"
    if question_cfg:
        primary_widget = question_cfg.get("recommended", primary_widget)
    metric_names = [item["metric"] for item in spec["metric_roles"]]

    blocks: list[dict[str, Any]] = [
        {
            "section": "headline_story",
            "intent": "Name the decision, current state, and why it matters.",
            "widgets": [
                {"role": "headline_metric", "widget": "number", "metric": metric_names[0]},
                {"role": "primary_question", "widget": primary_widget, "metric": metric_names[1]},
            ],
        },
        {
            "section": "diagnostic_breakdown",
            "intent": "Show the main drivers, slices, or variance drivers.",
            "widgets": [
                {"role": "driver_metric", "widget": "hbar", "metric": metric_names[1]},
                {"role": "risk_metric", "widget": "comparisontable", "metric": metric_names[2]},
            ],
        },
    ]
    if spec.get("persona") in {"manager", "individual"} or spec.get("secondary_surface"):
        blocks.append(
            {
                "section": "action_layer",
                "intent": "End in a queue, linked report, or explicit follow-up path.",
                "widgets": [
                    {"role": "action_metric", "widget": "comparisontable", "metric": metric_names[3]}
                ],
            }
        )
    return blocks


def _draft_design_cues(spec: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
    retrieval = spec.get("retrieval_context", {})
    widget_profiles = inputs["widget_profiles"]
    persona = spec.get("persona")
    domain = spec.get("domain")
    question_id = spec.get("question_id")
    avoid: list[str] = []
    if persona:
        avoid.extend(widget_profiles.get("personas", {}).get(persona, {}).get("avoid_default", []))
    if domain and question_id:
        avoid.extend(
            widget_profiles.get("domains", {})
            .get(domain, {})
            .get("common_questions", {})
            .get(question_id, {})
            .get("avoid", [])
        )

    return {
        "reference_patterns": [item["id"] for item in retrieval.get("patterns", [])[:2]],
        "reference_exemplars": [item["id"] for item in retrieval.get("exemplars", [])[:2]],
        "carry_forward": retrieval.get("design_cues", [])[:4],
        "avoid": list(dict.fromkeys(item for item in avoid if item)),
        "page_budget": spec.get("excellence_target", {}).get("max_pages"),
    }


def build_draft(spec: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
    builder_profiles = inputs["builder_profiles"]
    primary_surface = spec["primary_surface"]
    primary_adapter = builder_profiles["surface_adapters"][primary_surface]
    excellence_target = spec.get("excellence_target", {})
    persona_layout = builder_profiles["persona_layouts"].get(
        spec.get("persona") or "manager",
        builder_profiles["persona_layouts"]["manager"],
    )
    page_model = excellence_target.get("preferred_page_model") or persona_layout["section_order"]
    required_sections = excellence_target.get("required_sections") or persona_layout["section_order"]

    if primary_surface == "salesforce_report":
        draft = {
            "shape": primary_adapter["draft_shape"],
            "report_format": _report_format(spec),
            "group_by": _group_by(spec),
            "columns": _report_columns(spec),
            "filters": spec["recommended_filters"],
            "sort_by": [spec["metric_roles"][3]["metric"], "Owner"],
            "handoff_surface": spec.get("secondary_surface")
            or excellence_target.get("preferred_secondary_surface")
            or primary_adapter.get("default_handoff_surface"),
            "page_model": page_model,
        }
        draft["action_surface_assessment"] = _report_action_surface_assessment(spec, draft)
    else:
        draft = {
            "shape": primary_adapter["draft_shape"],
            "section_order": required_sections,
            "filters": spec["recommended_filters"],
            "blocks": _dashboard_blocks(spec, inputs),
            "handoff_surface": spec.get("secondary_surface")
            or excellence_target.get("preferred_secondary_surface")
            or primary_adapter.get("default_handoff_surface"),
            "page_model": page_model,
        }

    draft["baseline_status"] = "baseline_only"
    draft["revision_targets"] = [
        "tighten the primary decision story",
        "remove generic or duplicated summary blocks",
        "confirm the action path is real before deployment",
    ]
    draft["design_cues"] = _draft_design_cues(spec, inputs)
    return draft


def _role_metric_label(role: str, metric: str) -> str:
    normalized = ai.normalize_text(metric)
    if role == "headline_metric":
        if any(token in normalized for token in ("actual", "target", "headline")):
            return metric
        return f"Actual {metric}"
    if role == "driver_metric":
        if any(token in normalized for token in ("variance", "driver")):
            return metric
        return f"Variance Driver: {metric}"
    if role == "risk_metric":
        if "risk" in normalized:
            return metric
        return f"Risk: {metric}"
    if role == "action_metric":
        if any(token in normalized for token in ("action", "queue", "follow up", "follow-up")):
            return metric
        return f"Action Queue: {metric}"
    return metric


def _page_blueprint(spec: dict[str, Any], draft: dict[str, Any]) -> list[dict[str, str]]:
    blueprint: list[dict[str, str]] = []
    metric_map = {item["role"]: item["metric"] for item in spec["metric_roles"]}
    for page in draft.get("page_model", []):
        normalized = ai.normalize_text(page)
        purpose = "Support the primary decision."
        emphasis = metric_map.get("headline_metric", "")
        if "summary" in normalized or "overview" in normalized:
            purpose = "State the current position and the top business implication."
            emphasis = metric_map.get("headline_metric", "")
        elif "trend" in normalized or "forecast" in normalized:
            purpose = "Show movement over time and where risk or forecast deviation is emerging."
            emphasis = metric_map.get("driver_metric", "")
        elif "driver" in normalized or "segment" in normalized or "industry" in normalized:
            purpose = "Break the result into the main slices or drivers."
            emphasis = metric_map.get("driver_metric", "")
        elif "exception" in normalized or "action" in normalized or "queue" in normalized:
            purpose = "End in an actionable queue or follow-up path."
            emphasis = metric_map.get("action_metric", "")
        elif "ownership" in normalized or "handoff" in normalized:
            purpose = "Clarify ownership and where handoffs are breaking."
            emphasis = metric_map.get("action_metric", "")
        elif "process" in normalized or "quality" in normalized:
            purpose = "Diagnose process health and semantic quality."
            emphasis = metric_map.get("risk_metric", "")
        elif "my day" in normalized:
            purpose = "Start with immediate work and priorities for the day."
            emphasis = metric_map.get("action_metric", "")
        blueprint.append({"page": page, "purpose": purpose, "emphasis_metric": emphasis})
    return blueprint


def revise_draft(
    spec: dict[str, Any],
    draft: dict[str, Any],
    critique: dict[str, Any],
    inputs: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, str]]]:
    revision_log: list[dict[str, str]] = []
    finding_codes = {item["code"] for item in critique.get("findings", [])}
    critic_reviews = {item["critic"]: item for item in critique.get("critic_reviews", [])}
    revised_spec = deepcopy(spec)

    should_correct_surface = bool(
        revised_spec.get("builder_default_primary_surface")
        and revised_spec["builder_default_primary_surface"] != revised_spec["primary_surface"]
        and finding_codes.intersection({"surface_fit", "executive_story", "excellence_pattern"})
    )
    if should_correct_surface:
        revised_spec = build_spec(
            inputs,
            query=spec["query"],
            persona=spec.get("persona"),
            domain=spec.get("domain"),
            operation=spec.get("operation"),
            surface_override=spec.get("builder_default_primary_surface"),
        )
        revision_log.append(
            {
                "change": "surface_correction",
                "reason": "Aligned the revised draft to the builder default and closest local exemplar.",
            }
        )

    revised_roles: list[dict[str, str]] = []
    metric_changed = False
    for item in revised_spec["metric_roles"]:
        labeled_metric = _role_metric_label(item["role"], item["metric"])
        revised_roles.append({"role": item["role"], "metric": labeled_metric})
        if labeled_metric != item["metric"]:
            metric_changed = True
    revised_spec["metric_roles"] = revised_roles
    if metric_changed:
        revision_log.append(
            {
                "change": "metric_role_labels",
                "reason": "Added actual/variance/risk/action labeling to metric roles.",
            }
        )

    excellence_target = revised_spec.get("excellence_target", {})
    if excellence_target.get("requires_handoff") and not revised_spec.get("secondary_surface"):
        revised_spec["secondary_surface"] = excellence_target.get("preferred_secondary_surface")
        revision_log.append(
            {
                "change": "handoff_added",
                "reason": "Added the preferred secondary handoff surface from the excellence target.",
            }
        )

    story_review = critic_reviews.get("story_critic", {})
    if story_review.get("status") == "warn":
        preferred_page_model = revised_spec.get("excellence_target", {}).get("preferred_page_model") or []
        if preferred_page_model and revised_spec.get("primary_surface") != "salesforce_report":
            current_page_model = revised_spec.get("excellence_target", {}).get("preferred_page_model", [])
            if current_page_model != preferred_page_model:
                revised_spec["excellence_target"]["preferred_page_model"] = preferred_page_model
        if preferred_page_model:
            revision_log.append(
                {
                    "change": "story_page_model_alignment",
                    "reason": "Aligned the revised draft to the compact page model expected by the story critic.",
                }
            )

    action_review = critic_reviews.get("action_critic", {})
    if action_review.get("status") == "warn":
        if revised_spec["primary_surface"] == "salesforce_report":
            revised_spec["secondary_surface"] = (
                revised_spec.get("secondary_surface")
                or revised_spec.get("excellence_target", {}).get("preferred_secondary_surface")
                or "crma_dashboard"
            )
            revision_log.append(
                {
                    "change": "action_handoff_enforced",
                    "reason": "Kept the report tied to a richer follow-up surface so the action path stays real.",
                }
            )
        elif revised_spec["primary_surface"] != "salesforce_report":
            if "action_layer" not in revised_spec["excellence_target"].get("required_sections", []):
                revised_spec["excellence_target"]["required_sections"] = [
                    *revised_spec["excellence_target"].get("required_sections", []),
                    "action_layer",
                ]
            revision_log.append(
                {
                    "change": "action_layer_enforced",
                    "reason": "Forced an explicit action layer because the action critic found the follow-up path weak.",
                }
            )

    visual_review = critic_reviews.get("visual_critic", {})
    if visual_review.get("status") == "warn":
        if revised_spec["primary_surface"] == "salesforce_dashboard":
            revised_spec["excellence_target"]["preferred_page_model"] = (
                revised_spec.get("excellence_target", {}).get("preferred_page_model", [])[:2]
            )
            revision_log.append(
                {
                    "change": "native_dashboard_simplified",
                    "reason": "Reduced the native dashboard to a lighter headline-oriented page budget.",
                }
            )

    revised_draft = build_draft(revised_spec, inputs)
    revised_draft["baseline_status"] = "critic_revised"
    revised_draft["page_blueprint"] = _page_blueprint(revised_spec, revised_draft)
    revised_draft["revision_targets"] = [
        "validate the final surface against live data and screenshot review",
        "confirm the action path resolves to a real queue or handoff",
    ]

    if revised_spec["primary_surface"] == "crma_dashboard":
        for block in revised_draft.get("blocks", []):
            if block.get("section") == "headline_story" and block.get("widgets"):
                primary_question = block["widgets"][-1]
                if (
                    revised_spec.get("persona") == "executive"
                    and primary_question.get("widget") == "comparisontable"
                ):
                    primary_question["widget"] = "hbar"
                    revision_log.append(
                        {
                            "change": "executive_story_widget",
                            "reason": "Replaced a queue-like headline widget with a more executive-readable comparison view.",
                        }
                    )
                break

    if revised_spec["primary_surface"] == "salesforce_report":
        revised_draft["report_format"] = _report_format(revised_spec)
        revised_draft["handoff_surface"] = (
            revised_spec.get("secondary_surface")
            or revised_spec.get("excellence_target", {}).get("preferred_secondary_surface")
            or revised_draft.get("handoff_surface")
        )
        if revised_draft["report_format"] == "tabular":
            revised_draft["sort_by"] = [revised_spec["metric_roles"][3]["metric"], "Owner"]
        if revised_spec.get("persona") == "executive":
            revised_draft["page_model"] = ["Queue / Follow-up"]
            revised_draft["page_blueprint"] = _page_blueprint(revised_spec, revised_draft)
            revision_log.append(
                {
                    "change": "executive_report_constrained",
                    "reason": "Kept the report narrowly queue-first while relying on the handoff for executive narrative depth.",
                }
            )

    if not revision_log:
        revision_log.append(
            {
                "change": "baseline_promoted",
                "reason": "Promoted the baseline draft into a reviewed second pass without structural changes.",
            }
        )

    return revised_spec, revised_draft, revision_log


def _review_status(findings: list[dict[str, Any]]) -> str:
    if any(item["severity"] == "error" for item in findings):
        return "error"
    if findings:
        return "warn"
    return "ok"


def _matching_strengths(strengths: list[str], keywords: tuple[str, ...]) -> list[str]:
    return [item for item in strengths if any(keyword in item.lower() for keyword in keywords)]


def _specialist_reviews(
    spec: dict[str, Any],
    draft: dict[str, Any],
    findings: list[dict[str, Any]],
    strengths: list[str],
) -> list[dict[str, Any]]:
    reviews: list[dict[str, Any]] = []
    review_specs = [
        {
            "critic": "surface_planner",
            "focus": "Surface choice and handoff design",
            "expectation": "Choose the right surface first, then use handoffs only where they sharpen the action path.",
            "codes": {"surface_fit", "excellence_pattern", "paired_handoff"},
            "strength_keywords": ("surface", "handoff"),
        },
        {
            "critic": "story_critic",
            "focus": "Story arc and page discipline",
            "expectation": "Keep the page model compact and make the narrative order obvious to the audience.",
            "codes": {"executive_story", "page_sprawl"},
            "strength_keywords": ("page model", "sections expected"),
        },
        {
            "critic": "action_critic",
            "focus": "Action path and queue quality",
            "expectation": "Every operating surface should end in a real queue, handoff, or follow-up path.",
            "codes": {"manager_action_path", "paired_handoff", "excellence_pattern"},
            "strength_keywords": ("queue", "action path", "handoff"),
        },
        {
            "critic": "visual_critic",
            "focus": "Visual grammar and widget fit",
            "expectation": (
                "Use visual forms that match the surface: scan-fast story for CRMA executive views, "
                "queue-first tables for reports, lightweight rollups for native dashboards."
            ),
            "codes": {"surface_visual_fit", "metric_contract"},
            "strength_keywords": ("critic-driven revision", "metric"),
        },
    ]

    for review in review_specs:
        relevant_findings = [item for item in findings if item["code"] in review["codes"]]
        review_strengths = _matching_strengths(strengths, review["strength_keywords"])
        if not review_strengths and review["critic"] == "visual_critic" and draft.get("design_cues", {}).get("carry_forward"):
            review_strengths = [
                f"Draft is carrying forward local design cues: {', '.join(draft['design_cues']['carry_forward'][:2])}."
            ]
        reviews.append(
            {
                "critic": review["critic"],
                "focus": review["focus"],
                "expectation": review["expectation"],
                "status": _review_status(relevant_findings),
                "findings": relevant_findings,
                "strengths": review_strengths,
            }
        )
    return reviews


def critique_draft(spec: dict[str, Any], draft: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
    profiles = inputs["builder_profiles"]
    findings: list[dict[str, Any]] = []
    strengths: list[str] = []

    def add_finding(code: str, text: str) -> None:
        config = profiles["critique_checks"][code]
        findings.append(
            {
                "code": code,
                "severity": config["severity"],
                "text": text,
            }
        )

    persona = spec.get("persona")
    primary_surface = spec["primary_surface"]
    recommended_surface = spec["routing"]["recommended_surface_type"]
    default_primary_surface = spec.get("builder_default_primary_surface")
    excellence_target = spec.get("excellence_target", {})
    reference_exemplar = spec.get("reference_exemplar")

    if (
        primary_surface != default_primary_surface
        or recommended_surface not in {"hybrid", primary_surface}
    ):
        add_finding(
            "surface_fit",
            f"The builder default was {default_primary_surface or recommended_surface}, but the draft is centered on {primary_surface}.",
        )
    else:
        strengths.append("Primary surface matches the builder default for this ask.")

    if excellence_target:
        if excellence_target.get("primary_surface") != primary_surface:
            add_finding(
                "excellence_pattern",
                f"The selected excellence target expects {excellence_target.get('primary_surface')}, not {primary_surface}.",
            )
        else:
            strengths.append(
                f"Draft aligns to the {excellence_target.get('label')} excellence target."
            )

        preferred_secondary = excellence_target.get("preferred_secondary_surface")
        if excellence_target.get("requires_handoff"):
            if spec.get("secondary_surface") != preferred_secondary:
                add_finding(
                    "excellence_pattern",
                    f"The excellence target expects a {preferred_secondary} handoff.",
                )
            else:
                strengths.append("Secondary handoff surface matches the local excellence pattern.")

        preferred_page_model = excellence_target.get("preferred_page_model", [])
        draft_page_model = draft.get("page_model", [])
        if preferred_page_model and draft_page_model:
            overlap = [page for page in preferred_page_model if page in draft_page_model]
            if len(overlap) < max(1, min(len(preferred_page_model), len(draft_page_model)) // 2):
                add_finding(
                    "excellence_pattern",
                    "The draft page model drifts too far from the local excellence pattern.",
                )
            else:
                strengths.append("Draft page model is aligned with a proven local surface pattern.")

        max_pages = excellence_target.get("max_pages")
        if isinstance(max_pages, int) and draft_page_model and len(draft_page_model) > max_pages:
            add_finding(
                "page_sprawl",
                f"The draft page model has {len(draft_page_model)} pages/views against a budget of {max_pages}.",
            )

        required_sections = set(excellence_target.get("required_sections", []))
        if required_sections and "blocks" in draft:
            actual_sections = {block.get("section") for block in draft["blocks"]}
            missing_sections = [section for section in required_sections if section not in actual_sections]
            if missing_sections:
                add_finding(
                    "excellence_pattern",
                    f"The draft is missing required sections from the excellence pattern: {', '.join(missing_sections)}.",
                )
            else:
                strengths.append("Draft includes the sections expected by the excellence pattern.")

    if (
        reference_exemplar
        and reference_exemplar.get("primary_surface") != primary_surface
        and primary_surface != default_primary_surface
    ):
        add_finding(
            "excellence_pattern",
            f"The closest local exemplar is {reference_exemplar.get('label')} on {reference_exemplar.get('primary_surface')}, not {primary_surface}.",
        )

    if persona == "executive" and primary_surface == "salesforce_report":
        add_finding(
            "executive_story",
            "Executive asks should not stop at a raw report unless there is an explicit summary handoff.",
        )

    if primary_surface == "crma_dashboard":
        headline_widgets = []
        for block in draft.get("blocks", []):
            if block.get("section") == "headline_story":
                headline_widgets = [item.get("widget") for item in block.get("widgets", [])]
                break
        if persona == "executive" and "comparisontable" in headline_widgets:
            add_finding(
                "surface_visual_fit",
                "Executive CRMA headline views should use a scan-fast comparison widget, not a queue-like table.",
            )
        elif persona == "executive" and headline_widgets:
            strengths.append("Executive headline visual is using a scan-fast story shape.")

    if primary_surface == "salesforce_report":
        action_surface_assessment = draft.get("action_surface_assessment")
        if not isinstance(action_surface_assessment, dict):
            action_surface_assessment = spec.get("report_action_surface_assessment")
        if persona in {"manager", "individual"} and draft.get("report_format") != "tabular":
            add_finding(
                "surface_visual_fit",
                "Operating reports should stay tabular so the action queue remains explicit.",
            )
        elif draft.get("report_format") == "tabular":
            strengths.append("Report format is queue-first and aligned to operating use.")
        if isinstance(action_surface_assessment, dict):
            verdict = action_surface_assessment.get("verdict")
            if verdict == "weak_follow_up_fit":
                add_finding(
                    "surface_fit",
                    "The report package is too weak to carry the primary operating surface and should stay a follow-up handoff instead.",
                )
            elif (
                not action_surface_assessment.get("queue_ready_format")
                and not _queue_like(spec.get("question_id"))
            ):
                add_finding(
                    "surface_fit",
                    "The report resolves as a diagnostic shape instead of a queue-first operating surface for this ask.",
                )
            elif verdict in {"moderate_follow_up_fit", "strong_follow_up_fit"}:
                strengths.append(
                    f"Report action-surface assessment is {str(verdict).replace('_', ' ')} with explicit ownership/time/value cues."
                )

    if primary_surface == "salesforce_dashboard":
        if len(draft.get("page_model", [])) > 2:
            add_finding(
                "surface_visual_fit",
                "Native Salesforce dashboards should stay lightweight and not sprawl into multi-page systems.",
            )
        else:
            strengths.append("Native dashboard page budget is staying lightweight.")

    requires_queue = profiles["persona_layouts"].get(persona or "", {}).get("requires_queue", False)
    has_queue = False
    if "blocks" in draft:
        has_queue = any(
            widget.get("widget") == "comparisontable"
            and widget.get("role") == "action_metric"
            for block in draft["blocks"]
            for widget in block.get("widgets", [])
        )
    elif primary_surface == "salesforce_report":
        has_queue = draft.get("report_format") == "tabular"
    if requires_queue and not has_queue:
        add_finding(
            "manager_action_path",
            "The draft does not end in a queue or explicit follow-up path for the operating user.",
        )
    elif requires_queue:
        strengths.append("Draft includes an explicit queue or action path for the operating persona.")

    metric_names = [item["metric"] for item in spec["metric_roles"]]
    if not any(
        any(token in ai.normalize_text(metric) for token in ("actual", "target", "forecast", "variance", "risk"))
        for metric in metric_names
    ):
        add_finding(
            "metric_contract",
            "The draft metric roles are still generic and need explicit actual/target/forecast/risk labeling.",
        )

    if spec["build_mode"] == "paired_handoff" and not spec.get("secondary_surface"):
        add_finding(
            "paired_handoff",
            "The build mode expects a secondary handoff surface, but none is named.",
        )

    if draft.get("baseline_status") != "critic_revised":
        add_finding(
            "baseline_only",
            "This draft is only a baseline and still needs a human or critic pass for final design quality.",
        )
    else:
        strengths.append("Draft has already been promoted through a critic-driven revision pass.")

    status = "ok"
    if findings:
        status = "error" if any(item["severity"] == "error" for item in findings) else "warn"
    score = 100
    for item in findings:
        score -= 25 if item["severity"] == "error" else 10
    score = max(score, 0)
    specialist_reviews = _specialist_reviews(spec, draft, findings, strengths)
    if status == "error":
        verdict = "surface_rethink_required"
    elif status == "warn":
        verdict = "ready_for_revision"
    else:
        verdict = "ready_for_build"

    return {
        "status": status,
        "findings": findings,
        "strengths": strengths,
        "excellence_target": {
            "target_id": excellence_target.get("target_id"),
            "pattern_id": excellence_target.get("pattern_id"),
            "label": excellence_target.get("label"),
            "source": excellence_target.get("source"),
            "reference_exemplar_id": reference_exemplar.get("id") if reference_exemplar else None,
        },
        "score": score,
        "verdict": verdict,
        "critic_reviews": specialist_reviews,
    }


def _execution_profile(primary_surface: str) -> dict[str, str]:
    profiles = {
        "crma_dashboard": {
            "execution_lane": "crma_api_direct",
            "repo_execution_fit": "strong",
            "delivery_mode": "wave_api_patch",
        },
        "salesforce_report": {
            "execution_lane": "salesforce_report_handoff",
            "repo_execution_fit": "partial",
            "delivery_mode": "native_report_authoring",
        },
        "salesforce_dashboard": {
            "execution_lane": "salesforce_dashboard_handoff",
            "repo_execution_fit": "partial",
            "delivery_mode": "native_dashboard_authoring",
        },
    }
    return profiles[primary_surface]


def _page_storyboard(spec: dict[str, Any], draft: dict[str, Any]) -> list[dict[str, Any]]:
    page_blueprint = draft.get("page_blueprint") or _page_blueprint(spec, draft)
    blocks = draft.get("blocks", [])
    if not page_blueprint:
        return []

    storyboard: list[dict[str, Any]] = []
    if not blocks:
        for page in page_blueprint:
            storyboard.append({**page, "sections": []})
        return storyboard

    block_count = len(blocks)
    page_count = len(page_blueprint)
    for index, page in enumerate(page_blueprint):
        assigned_blocks: list[dict[str, Any]]
        if page_count == 1:
            assigned_blocks = blocks
        elif index < block_count:
            assigned_blocks = [blocks[index]]
        elif index == page_count - 1 and block_count > page_count:
            assigned_blocks = blocks[index:]
        else:
            assigned_blocks = []
        storyboard.append(
            {
                **page,
                "sections": [
                    {
                        "section": block.get("section"),
                        "intent": block.get("intent"),
                        "widgets": block.get("widgets", []),
                    }
                    for block in assigned_blocks
                ],
            }
        )
    return storyboard


def _surface_contract(spec: dict[str, Any], draft: dict[str, Any]) -> dict[str, Any]:
    primary_surface = spec["primary_surface"]
    if primary_surface == "salesforce_report":
        return {
            "surface_type": primary_surface,
            "report_format": draft.get("report_format"),
            "group_by": draft.get("group_by", []),
            "columns": draft.get("columns", []),
            "filters": draft.get("filters", []),
            "sort_by": draft.get("sort_by", []),
            "handoff_surface": draft.get("handoff_surface"),
            "page_blueprint": draft.get("page_blueprint", []),
        }

    return {
        "surface_type": primary_surface,
        "filters": draft.get("filters", []),
        "handoff_surface": draft.get("handoff_surface"),
        "page_model": draft.get("page_model", []),
        "page_storyboard": _page_storyboard(spec, draft),
    }


def _acceptance_criteria(
    spec: dict[str, Any],
    draft: dict[str, Any],
    critique_after: dict[str, Any],
) -> list[str]:
    criteria = [
        f"Primary surface remains {spec['primary_surface']} and stays aligned to the builder decision.",
        "Metric roles remain explicitly labeled as actual, variance/driver, risk, and action.",
    ]
    if spec.get("secondary_surface") or draft.get("handoff_surface"):
        criteria.append(
            f"Handoff path is real and points to {draft.get('handoff_surface') or spec.get('secondary_surface')}."
        )
    if draft.get("page_model"):
        criteria.append(
            f"Final surface stays within the planned page/view budget of {len(draft['page_model'])}."
        )
    if spec["primary_surface"] == "salesforce_report":
        criteria.append("Report remains queue-first and keeps the specified grouping, columns, and sort order.")
    else:
        criteria.append("Dashboard preserves the planned story order and does not collapse the action layer.")
    if critique_after.get("verdict") == "ready_for_build":
        criteria.append("Final asset passes live data validation and screenshot review before rollout.")
    return criteria


def _revision_critic(change: str) -> str:
    mapping = {
        "surface_correction": "surface_planner",
        "handoff_added": "action_critic",
        "action_handoff_enforced": "action_critic",
        "action_layer_enforced": "action_critic",
        "story_page_model_alignment": "story_critic",
        "executive_story_widget": "visual_critic",
        "native_dashboard_simplified": "visual_critic",
        "metric_role_labels": "visual_critic",
        "executive_report_constrained": "story_critic",
        "baseline_promoted": "surface_planner",
    }
    return mapping.get(change, "surface_planner")


def _critic_rationale(
    critique_before: dict[str, Any],
    critique_after: dict[str, Any],
    revisions: list[dict[str, str]],
) -> list[dict[str, Any]]:
    before_reviews = {
        item["critic"]: item
        for item in critique_before.get("critic_reviews", [])
        if isinstance(item, dict) and item.get("critic")
    }
    after_reviews = {
        item["critic"]: item
        for item in critique_after.get("critic_reviews", [])
        if isinstance(item, dict) and item.get("critic")
    }
    revisions_by_critic: dict[str, list[dict[str, str]]] = {}
    for item in revisions:
        critic = _revision_critic(item["change"])
        revisions_by_critic.setdefault(critic, []).append(item)

    ordered_critics = [
        "surface_planner",
        "story_critic",
        "action_critic",
        "visual_critic",
    ]
    rationale: list[dict[str, Any]] = []
    for critic in ordered_critics:
        before = before_reviews.get(critic)
        after = after_reviews.get(critic)
        related_revisions = revisions_by_critic.get(critic, [])
        if not before and not after and not related_revisions:
            continue
        focus = (before or after or {}).get("focus")
        expectation = (before or after or {}).get("expectation")
        findings_before = [item["text"] for item in (before or {}).get("findings", [])]
        strengths_after = (after or {}).get("strengths", [])
        constraints: list[str] = []
        if critic == "surface_planner":
            constraints = [
                "Keep the build on the chosen primary surface unless the planner is explicitly overridden again.",
                "Preserve the named handoff only when it sharpens the action path.",
            ]
        elif critic == "story_critic":
            constraints = [
                "Keep the page model within the planned budget and preserve the narrative order.",
                "Do not expand the asset into extra pages or summary clutter without a new story review.",
            ]
        elif critic == "action_critic":
            constraints = [
                "Preserve the real queue, action layer, or follow-up handoff path.",
                "Do not remove the explicit operating path from the final surface.",
            ]
        elif critic == "visual_critic":
            constraints = [
                "Keep the visual grammar aligned to the audience and surface type.",
                "Do not reintroduce generic queue-like headline widgets or unlabeled metrics.",
            ]
        rationale.append(
            {
                "critic": critic,
                "focus": focus,
                "status_before": (before or {}).get("status"),
                "status_after": (after or {}).get("status"),
                "expectation": expectation,
                "findings_before": findings_before,
                "revisions_applied": related_revisions,
                "constraints": constraints,
                "strengths_after": strengths_after,
            }
        )
    return rationale


def _design_constraints(critic_rationale: list[dict[str, Any]]) -> list[str]:
    constraints: list[str] = []
    for item in critic_rationale:
        constraints.extend(item.get("constraints", []))
    return list(dict.fromkeys(constraints))


def _crma_patch_guardrails() -> list[str]:
    return [
        "Run normalized contract lint before any PATCH attempt.",
        "Keep comparison tables on a complete four-key columnMap and use null for unsupported chart families.",
        "Do not introduce PATCH-banned step dataset fields such as label/url or aggregateflex.isFacet.",
        "Keep page names aligned to navigation link destinations.",
    ]


def _page_name(value: str) -> str:
    return ai.normalize_text(value).replace(" ", "_")


def _asset_export_slug(value: str) -> str:
    return ai.normalize_text(value).replace(" ", "_")


def _extract_dashboard_id_from_url(live_url: str | None) -> str | None:
    if not live_url:
        return None
    match = re.search(r"/analytics/dashboard/([^/?#]+)", live_url)
    if match:
        return match.group(1)
    return None


def _preferred_candidate_label(spec: dict[str, Any]) -> str | None:
    labels = spec.get("candidate_surface_labels") or []
    if labels:
        return labels[0]
    reference_exemplar = spec.get("reference_exemplar") or {}
    label = reference_exemplar.get("label")
    return label if isinstance(label, str) and label else None


def _context_dashboard_target(inputs: dict[str, Any], label: str | None) -> dict[str, Any] | None:
    if not label:
        return None
    normalized = ai.normalize_text(label)
    for item in inputs["context_registry"].get("dashboards", []):
        if ai.normalize_text(item.get("name", "")) == normalized or ai.normalize_text(item.get("id", "")) == normalized:
            return {
                "surface_type": "crma_dashboard",
                "destination_type": "dashboard",
                "target_surface_id": _extract_dashboard_id_from_url(item.get("live_url")),
                "target_surface_label": item.get("name"),
                "target_destination_name": _extract_dashboard_id_from_url(item.get("live_url")) or item.get("name"),
                "resolution_source": "context_registry",
            }
    return None


def _registry_handoff_target(inputs: dict[str, Any], spec: dict[str, Any], handoff_surface: str) -> dict[str, Any] | None:
    registry = inputs.get("builder_brain_handoff_targets", {})
    source_surface_ids = {
        spec.get("candidate_surface_id"),
        (spec.get("reference_exemplar") or {}).get("id"),
    }
    source_surface_ids = {item for item in source_surface_ids if item}
    for item in registry.get("targets", []):
        if item.get("target_surface_type") != handoff_surface:
            continue
        if item.get("source_surface_id") not in source_surface_ids:
            continue
        destination_type = item.get("destination_type")
        if not destination_type:
            destination_type = "report" if handoff_surface == "salesforce_report" else "dashboard"
        return {
            "surface_type": handoff_surface,
            "destination_type": destination_type,
            "target_surface_id": item.get("target_surface_id"),
            "target_surface_label": item.get("target_surface_label"),
            "target_destination_name": item.get("target_destination_name")
            or item.get("target_surface_id")
            or item.get("target_surface_label"),
            "resolution_source": "builder_brain_handoff_targets",
        }
    return None


def _handoff_target(inputs: dict[str, Any], spec: dict[str, Any], handoff_surface: str | None) -> dict[str, Any] | None:
    if not handoff_surface:
        return None
    if handoff_surface == "crma_dashboard":
        candidate_target = _context_dashboard_target(inputs, _preferred_candidate_label(spec))
        if candidate_target:
            return candidate_target
    registry_target = _registry_handoff_target(inputs, spec, handoff_surface)
    if registry_target:
        return registry_target
    destination_type = "report" if handoff_surface == "salesforce_report" else "dashboard"
    return {
        "surface_type": handoff_surface,
        "destination_type": destination_type,
        "target_surface_id": None,
        "target_surface_label": None,
        "target_destination_name": handoff_surface,
        "resolution_source": None,
    }


def _widget_contract(widget_type: str) -> dict[str, Any]:
    explicit_full = {"hbar", "column", "donut", "stackhbar", "stackcolumn", "pie", "vbar", "stackvbar"}
    auto_null = {"funnel", "waterfall", "treemap"}
    auto_detect = {"comparisontable", "heatmap", "combo", "area", "stackarea", "line", "scatter", "bubble", "timeline", "bullet", "number"}
    if widget_type in explicit_full:
        return {
            "column_map_strategy": "explicit_full",
            "contract_checks": [
                "Provide all four columnMap keys: dimensionAxis, plots, trellis, split.",
                "Preserve a single ranked comparison story; do not turn this into a timeline or queue.",
            ],
        }
    if widget_type in auto_null:
        return {
            "column_map_strategy": "null",
            "contract_checks": [
                "Force columnMap: null for this visualization family.",
            ],
        }
    if widget_type == "gauge":
        return {
            "column_map_strategy": "special_gauge",
            "contract_checks": [
                "Provide gauge-specific plots/trellis mapping only.",
            ],
        }
    if widget_type == "choropleth":
        return {
            "column_map_strategy": "special_choropleth",
            "contract_checks": [
                "Provide locations/color plus standard dimensionAxis/plots/trellis keys.",
            ],
        }
    if widget_type in auto_detect:
        return {
            "column_map_strategy": "auto_detect",
            "contract_checks": [
                "Do not force a columnMap unless the live asset proves it is necessary.",
            ],
        }
    return {
        "column_map_strategy": "review_required",
        "contract_checks": [
            "Validate the widget contract manually before PATCH because this type is not yet codified.",
        ],
    }


def _section_layout_band(section_name: str | None) -> str:
    normalized = ai.normalize_text(section_name or "")
    if normalized == "headline story":
        return "hero_row"
    if normalized == "diagnostic breakdown":
        return "analysis_row"
    if normalized == "action layer":
        return "queue_row"
    return "supporting_row"


def _widget_component_key(page_name: str, section_name: str | None, widget_order: int) -> str:
    base = f"{page_name} {section_name or 'section'} {widget_order}"
    return ai.normalize_text(base).replace(" ", "_")


def _wave_patch_payload(
    spec: dict[str, Any],
    surface_contract: dict[str, Any],
    review_gates: list[str],
    design_constraints: list[str],
) -> dict[str, Any]:
    page_storyboard = surface_contract.get("page_storyboard", [])
    multi_page = len(page_storyboard) > 1
    page_mutations: list[dict[str, Any]] = []
    for page in page_storyboard:
        page_label = page.get("page")
        page_name = _page_name(page_label)
        section_mutations: list[dict[str, Any]] = []
        for section_order, section in enumerate(page.get("sections", []), start=1):
            section_name = section.get("section")
            widget_mutations: list[dict[str, Any]] = []
            for widget_order, widget in enumerate(section.get("widgets", []), start=1):
                widget_contract = _widget_contract(widget.get("widget", ""))
                widget_mutations.append(
                    {
                        "component_key": _widget_component_key(page_name, section_name, widget_order),
                        "order": widget_order,
                        "role": widget.get("role"),
                        "metric": widget.get("metric"),
                        "visualization_type": widget.get("widget"),
                        "layout_band": _section_layout_band(section_name),
                        "recommended_step_alias": _widget_component_key(
                            page_name,
                            widget.get("metric") or section_name,
                            widget_order,
                        ),
                        **widget_contract,
                    }
                )
            section_mutations.append(
                {
                    "section": section_name,
                    "section_order": section_order,
                    "layout_band": _section_layout_band(section_name),
                    "intent": section.get("intent"),
                    "widget_mutations": widget_mutations,
                }
            )
        page_mutations.append(
            {
                "page": page_label,
                "page_name": page_name,
                "purpose": page.get("purpose"),
                "emphasis_metric": page.get("emphasis_metric"),
                "nav_destination_name": page_name if multi_page else None,
                "section_mutations": section_mutations,
            }
        )

    return {
        "payload_type": "wave_patch_payload",
        "target_surface": {
            "surface_type": "crma_dashboard",
            "candidate_surface_id": spec.get("candidate_surface_id"),
            "candidate_surface_labels": spec.get("candidate_surface_labels", []),
            "reference_exemplar": (
                spec.get("reference_exemplar", {}).get("id") if spec.get("reference_exemplar") else None
            ),
        },
        "baseline_requirements": {
            "requires_live_export": True,
            "normalization_required": True,
            "guardrails": _crma_patch_guardrails(),
        },
        "navigation_contract": {
            "mode": "multi_page" if multi_page else "single_page",
            "pages": [
                {
                    "page": item.get("page"),
                    "page_name": item.get("page_name"),
                    "destination_name": item.get("page_name"),
                }
                for item in page_mutations
            ],
        },
        "page_mutations": page_mutations,
        "handoff_link": (
            {
                "target_surface": surface_contract.get("handoff_surface"),
                "target_surface_id": (surface_contract.get("handoff_target") or {}).get("target_surface_id"),
                "target_surface_label": (surface_contract.get("handoff_target") or {}).get("target_surface_label"),
                "target_destination_name": (
                    (surface_contract.get("handoff_target") or {}).get("target_destination_name")
                ),
                "destination_type": (surface_contract.get("handoff_target") or {}).get("destination_type"),
                "mode": "named_surface_link",
            }
            if surface_contract.get("handoff_surface")
            else None
        ),
        "validation_contract": {
            "review_gates": review_gates,
            "design_constraints": design_constraints,
            "required_checks": [
                "normalized_contract_lint",
                "nav_name_alignment",
                "widget_contract_review",
                "live_data_validation",
                "screenshot_review",
            ],
        },
    }


def _wave_patch_operations(surface_contract: dict[str, Any]) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = [
        {
            "sequence": 1,
            "action": "normalize_patch_state",
            "purpose": "Strip GET-only fields and normalize dashboard state before editing.",
            "checks": _crma_patch_guardrails(),
        }
    ]

    page_storyboard = surface_contract.get("page_storyboard", [])
    multi_page = len(page_storyboard) > 1
    for page_index, page in enumerate(page_storyboard, start=1):
        page_label = page.get("page")
        page_name = _page_name(page_label)
        operations.append(
            {
                "sequence": len(operations) + 1,
                "action": "ensure_page_scaffold",
                "page": page_label,
                "page_name": page_name,
                "purpose": "Create or align the page scaffold before widgets are patched in.",
                "checks": [
                    "Keep the page label/page name pair stable for navigation.",
                    "If nav links exist, destinationLink.name must equal the page_name.",
                ],
            }
        )
        if multi_page:
            operations.append(
                {
                    "sequence": len(operations) + 1,
                    "action": "wire_page_navigation",
                    "page": page_label,
                    "page_name": page_name,
                    "purpose": "Keep navigation aligned across all pages.",
                    "checks": [
                        "gridLayouts[].pages[].name must match nav destinationLink.name.",
                    ],
                }
            )
        for section_index, section in enumerate(page.get("sections", []), start=1):
            operations.append(
                {
                    "sequence": len(operations) + 1,
                    "action": "upsert_section_widgets",
                    "page": page_label,
                    "page_name": page_name,
                    "section": section.get("section"),
                    "section_order": section_index,
                    "purpose": section.get("intent"),
                    "widgets": [
                        {
                            "order": widget_index,
                            "role": widget.get("role"),
                            "visualization_type": widget.get("widget"),
                            "metric": widget.get("metric"),
                            **_widget_contract(widget.get("widget", "")),
                        }
                        for widget_index, widget in enumerate(section.get("widgets", []), start=1)
                    ],
                }
            )

    handoff_surface = surface_contract.get("handoff_surface")
    if handoff_surface:
        operations.append(
            {
                "sequence": len(operations) + 1,
                "action": "wire_handoff_link",
                "target_surface": handoff_surface,
                "purpose": "Keep the action path connected to the named follow-up surface.",
                "checks": [
                    "Preserve the handoff target exactly as packaged.",
                ],
            }
        )

    operations.append(
        {
            "sequence": len(operations) + 1,
            "action": "run_patch_validation",
            "purpose": "Run final PATCH-safety and screenshot validation before promotion.",
            "checks": [
                "Run normalized contract lint on the mutated state.",
                "Confirm page names, nav links, and widget contracts still align.",
                "Run screenshot review on the patched dashboard.",
            ],
        }
    )
    return operations


def _execution_plan(
    spec: dict[str, Any],
    draft: dict[str, Any],
    surface_contract: dict[str, Any],
    review_gates: list[str],
    design_constraints: list[str],
) -> dict[str, Any]:
    primary_surface = spec["primary_surface"]
    if primary_surface == "crma_dashboard":
        storyboard = surface_contract.get("page_storyboard", [])
        patch_operations = _wave_patch_operations(surface_contract)
        patch_payload = _wave_patch_payload(spec, surface_contract, review_gates, design_constraints)
        page_steps: list[dict[str, Any]] = []
        for page in storyboard:
            page_steps.append(
                {
                    "page": page.get("page"),
                    "purpose": page.get("purpose"),
                    "emphasis_metric": page.get("emphasis_metric"),
                    "sections": [
                        {
                            "section": section.get("section"),
                            "intent": section.get("intent"),
                            "widgets": [
                                f"{widget.get('widget')}: {widget.get('metric')}"
                                for widget in section.get("widgets", [])
                            ],
                        }
                        for section in page.get("sections", [])
                    ],
                }
            )
        return {
            "plan_type": "wave_patch_plan",
            "delivery_mode": "wave_api_patch",
            "phases": [
                {
                    "phase": "baseline_export",
                    "objective": "Capture and lint the live CRMA baseline before mutation.",
                    "actions": [
                        "Export the closest live CRMA surface as the patch baseline.",
                        "Run normalized contract lint on the exported state.",
                        "Confirm the packaged handoff target still matches the intended follow-up surface.",
                    ],
                    "guardrails": _crma_patch_guardrails(),
                },
                {
                    "phase": "storyboard_patch",
                    "objective": "Apply the packaged page and section story in order.",
                    "pages": page_steps,
                    "patch_operations": patch_operations,
                    "wave_patch_payload": patch_payload,
                },
                {
                    "phase": "validation",
                    "objective": "Validate the patched dashboard before rollout.",
                    "review_gates": review_gates,
                    "actions": [
                        "Confirm the story order and action layer remain intact after patching.",
                        "Run live data validation and screenshot review against the patched asset.",
                    ],
                    "constraints": design_constraints,
                },
            ],
        }

    if primary_surface == "salesforce_report":
        return {
            "plan_type": "salesforce_report_authoring_skeleton",
            "delivery_mode": "native_report_authoring",
            "phases": [
                {
                    "phase": "report_core",
                    "objective": "Author the report exactly as packaged.",
                    "actions": [
                        f"Use a {surface_contract.get('report_format')} report.",
                        f"Group by: {', '.join(surface_contract.get('group_by', []))}.",
                        f"Expose columns: {', '.join(surface_contract.get('columns', []))}.",
                        f"Sort by: {', '.join(surface_contract.get('sort_by', []))}.",
                    ],
                },
                {
                    "phase": "handoff_and_validation",
                    "objective": "Preserve the follow-up path and validate the queue.",
                    "review_gates": review_gates,
                    "actions": [
                        f"Preserve the {surface_contract.get('handoff_surface')} handoff.",
                        "Validate filter vocabulary, row-level accountability, and screenshot fit before rollout.",
                    ],
                    "constraints": design_constraints,
                },
            ],
        }

    return {
        "plan_type": "salesforce_dashboard_authoring_skeleton",
        "delivery_mode": "native_dashboard_authoring",
        "phases": [
            {
                "phase": "dashboard_core",
                "objective": "Author the native dashboard with a lightweight headline-first structure.",
                "actions": [
                    f"Keep the dashboard within {len(surface_contract.get('page_model', []))} page(s)/view(s).",
                    f"Preserve the named handoff surface: {surface_contract.get('handoff_surface')}.",
                ],
            },
            {
                "phase": "validation",
                "objective": "Validate the native dashboard before rollout.",
                "review_gates": review_gates,
                "actions": [
                    "Confirm the dashboard stays lightweight and does not absorb row-level queue detail.",
                    "Validate screenshot fit and handoff behavior in the live org.",
                ],
                "constraints": design_constraints,
            },
        ],
    }


def _next_steps(spec: dict[str, Any], draft: dict[str, Any]) -> list[str]:
    primary_surface = spec["primary_surface"]
    handoff_surface = draft.get("handoff_surface")
    if primary_surface == "crma_dashboard":
        steps = [
            "Execute through direct Wave API/export workflows, not legacy build_*.py files.",
            "Implement the pages and sections in the storyboard order shown by the package.",
        ]
        if handoff_surface:
            steps.append(f"Wire the {handoff_surface} handoff so the action path is real.")
        steps.append("Run live data validation plus screenshot review before promoting the surface.")
        return steps
    if primary_surface == "salesforce_report":
        steps = [
            "Author the report with the specified format, grouping, filters, columns, and sort order.",
            "Keep the report queue-first; do not turn it into an executive story surface.",
        ]
        if handoff_surface:
            steps.append(f"Preserve the {handoff_surface} handoff for richer follow-up diagnostics.")
        steps.append("Validate the filter vocabulary and row-level action path before rollout.")
        return steps
    steps = [
        "Keep the native dashboard lightweight and headline-oriented.",
        "Back each headline block with a real report-level drill or handoff path.",
    ]
    if handoff_surface:
        steps.append(f"Preserve the {handoff_surface} follow-up path.")
    steps.append("Validate the dashboard in the live org with screenshot review before rollout.")
    return steps


def build_package(
    inputs: dict[str, Any],
    spec: dict[str, Any],
    draft: dict[str, Any],
    critique_before: dict[str, Any],
    critique_after: dict[str, Any],
    revisions: list[dict[str, str]],
    planning_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    execution = _execution_profile(spec["primary_surface"])
    surface_contract = _surface_contract(spec, draft)
    surface_contract["handoff_target"] = _handoff_target(inputs, spec, surface_contract.get("handoff_surface"))
    report_action_surface_assessment = None
    if spec["primary_surface"] == "salesforce_report":
        report_action_surface_assessment = report_surface_intelligence.assess_report_action_surface_contract(
            surface_contract
        )
        surface_contract["action_surface_assessment"] = report_action_surface_assessment
    critic_rationale = _critic_rationale(critique_before, critique_after, revisions)
    design_constraints = _design_constraints(critic_rationale)
    execution_plan = _execution_plan(
        spec,
        draft,
        surface_contract,
        spec.get("review_gate_ids", []),
        design_constraints,
    )
    package = {
        "package_version": 1,
        "package_status": (
            "ready_for_execution"
            if critique_after.get("verdict") == "ready_for_build"
            else "needs_more_revision"
        ),
        "execution_lane": execution["execution_lane"],
        "repo_execution_fit": execution["repo_execution_fit"],
        "delivery_mode": execution["delivery_mode"],
        "build_brief": {
            "persona": spec.get("persona"),
            "domain": spec.get("domain"),
            "decision_statement": spec.get("decision_statement"),
            "build_mode": spec.get("build_mode"),
            "excellence_target": spec.get("excellence_target", {}).get("label"),
            "reference_exemplar": spec.get("reference_exemplar", {}).get("label")
            if spec.get("reference_exemplar")
            else None,
        },
        "surface_contract": surface_contract,
        "review_gates": spec.get("review_gate_ids", []),
        "critic_rationale": critic_rationale,
        "design_constraints": design_constraints,
        "execution_plan": execution_plan,
        "acceptance_criteria": _acceptance_criteria(spec, draft, critique_after),
        "revision_summary": [item["change"] for item in revisions],
        "next_steps": _next_steps(spec, draft),
    }
    if planning_context:
        package["planning_context"] = planning_context
    if report_action_surface_assessment:
        package["report_action_surface_assessment"] = report_action_surface_assessment
    return package


def _package_slug(spec: dict[str, Any]) -> str:
    base = spec.get("candidate_surface_id") or spec.get("question_id") or spec.get("query") or "builder_brain"
    return ai.normalize_text(base).replace(" ", "_")


def _registry_script(inputs: dict[str, Any], script_path: str) -> dict[str, Any] | None:
    for item in inputs["registry"].get("scripts", []):
        if item.get("path") == script_path:
            return item
    return None


def _command_entry(
    *,
    inputs: dict[str, Any],
    script_path: str,
    command: str,
    purpose: str,
) -> dict[str, Any]:
    registry_item = _registry_script(inputs, script_path) or {}
    return {
        "name": Path(script_path).stem,
        "script": script_path,
        "lane": registry_item.get("lane"),
        "command_class": registry_item.get("command_class"),
        "command": command,
        "purpose": purpose,
    }


def _append_evaluation_arg(command: str, planning_context: dict[str, Any] | None) -> str:
    if not planning_context:
        return command
    evaluation_path = planning_context.get("evaluation_path")
    if not isinstance(evaluation_path, str) or not evaluation_path:
        return command
    return f"{command} --evaluation {json.dumps(evaluation_path)}"


def _write_memory_health_artifacts(
    *,
    output_dir: Path,
    planning_context: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, Path | None, Path | None]:
    if not isinstance(planning_context, dict):
        return None, None, None
    memory_health = planning_context.get("memory_health")
    if not isinstance(memory_health, dict) or not memory_health:
        return None, None, None

    summary_payload = {
        "artifact_type": "memory_health",
        "goal": planning_context.get("goal"),
        "run_id": planning_context.get("run_id"),
        "domain": planning_context.get("domain"),
        "operation": planning_context.get("operation"),
        "memory_health": memory_health,
    }
    summary_path = output_dir / "memory_health.json"
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    report_lines = [
        "# Memory Health",
        "",
        f"- Goal: {planning_context.get('goal') or 'unknown'}",
        f"- Run ID: {planning_context.get('run_id') or 'unknown'}",
        f"- Considered hits: {memory_health.get('considered_hits', 0)}",
        f"- Excluded policy-exception hits: {memory_health.get('policy_exception_hits_excluded', 0)}",
        f"- Included failing hits: {memory_health.get('included_fail_count', 0)}",
        f"- Included needs-more-evidence hits: {memory_health.get('included_needs_more_evidence_count', 0)}",
        f"- Included generic-goal hits: {memory_health.get('included_generic_goal_count', 0)}",
        f"- Included missing-context hits: {memory_health.get('included_missing_context_count', 0)}",
    ]
    excluded_runs = memory_health.get("excluded_policy_exception_runs") or []
    if excluded_runs:
        report_lines.extend(["", "## Excluded Policy-Exception Runs"])
        for item in excluded_runs:
            if not isinstance(item, dict):
                continue
            report_lines.append(
                f"- {item.get('run_id')}: {item.get('goal')} [{', '.join(item.get('policy_exceptions') or [])}]"
            )
    report_path = output_dir / "memory_health.md"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return memory_health, summary_path, report_path


def _write_handoff_review_artifact(
    *,
    output_dir: Path,
    handoff: dict[str, Any],
) -> Path:
    lines = [
        "# Executor Handoff Review",
        "",
        f"- Primary lane: {handoff.get('primary_lane') or 'unknown'}",
        f"- Repo execution fit: {handoff.get('repo_execution_fit') or 'unknown'}",
    ]
    memory_health = handoff.get("memory_health")
    if isinstance(memory_health, dict):
        lines.extend(
            [
                f"- Excluded policy-exception hits: {memory_health.get('policy_exception_hits_excluded', 0)}",
                f"- Included failing hits: {memory_health.get('included_fail_count', 0)}",
                f"- Included generic-goal hits: {memory_health.get('included_generic_goal_count', 0)}",
            ]
        )

    lines.extend(["", "## Artifacts"])
    artifact_fields = (
        ("package_artifact", "Build package"),
        ("execution_plan_artifact", "Execution plan"),
        ("wave_patch_payload_artifact", "Wave patch payload"),
        ("memory_health_artifact", "Memory health JSON"),
        ("memory_health_report_artifact", "Memory health review"),
    )
    for field, label in artifact_fields:
        value = handoff.get(field)
        if isinstance(value, str) and value:
            lines.append(f"- {label}: `{value}`")

    commands = handoff.get("available_commands") or []
    if commands:
        lines.extend(["", "## Available Commands"])
        for item in commands:
            if not isinstance(item, dict):
                continue
            name = item.get("name") or "command"
            purpose = item.get("purpose") or ""
            command = item.get("command") or ""
            lines.append(f"- `{name}`: {purpose}")
            if command:
                lines.append(f"  Command: `{command}`")

    external_steps = handoff.get("external_steps") or []
    if external_steps:
        lines.extend(["", "## External Steps"])
        for step in external_steps:
            lines.append(f"- {step}")

    report_path = output_dir / "handoff.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def _write_probe_matrix_review_artifact(
    *,
    matrix_root: Path,
    summary: dict[str, Any],
    probe_runs: list[dict[str, Any]],
) -> tuple[Path, Path]:
    lines = [
        "# Probe Matrix Review",
        "",
        f"- Completed: {summary.get('completed', 0)} / {summary.get('total_requested', 0)}",
        f"- OK: {summary.get('ok_count', 0)}",
        f"- Warn: {summary.get('warn_count', 0)}",
        f"- Error: {summary.get('error_count', 0)}",
        f"- Cleanup requested: {summary.get('cleanup_requested_count', 0)}",
        f"- Stopped early: {summary.get('stopped_early', False)}",
    ]
    lines.extend(["", "## Probe Runs"])
    for run in probe_runs:
        if not isinstance(run, dict):
            continue
        run_summary = run.get("summary") if isinstance(run.get("summary"), dict) else {}
        lines.append(
            f"- Probe {run.get('index')}: {run.get('status')} :: {run.get('name') or Path(str(run.get('result_path') or '')).stem}"
        )
        result_path = run.get("result_path")
        if isinstance(result_path, str) and result_path:
            lines.append(f"  Result: `{result_path}`")
        review_path = run_summary.get("handoff_review_artifact")
        if isinstance(review_path, str) and review_path:
            lines.append(f"  Handoff review: `{review_path}`")
        memory_review_path = run_summary.get("memory_health_report_artifact")
        if isinstance(memory_review_path, str) and memory_review_path:
            lines.append(f"  Memory health review: `{memory_review_path}`")
        created_asset_id = run_summary.get("created_asset_id")
        if isinstance(created_asset_id, str) and created_asset_id:
            lines.append(f"  Created asset: `{created_asset_id}`")

    review_path = matrix_root / "probe_matrix_review.md"
    review_text = "\n".join(lines) + "\n"
    review_path.write_text(review_text, encoding="utf-8")
    readme_path = matrix_root / "README.md"
    readme_path.write_text(review_text, encoding="utf-8")
    return review_path, readme_path


def _render_builder_brain_collection_entry(item: dict[str, Any]) -> list[str]:
    run_label = item.get("label") or Path(str(item.get("run_dir") or "")).name or "run"
    lines = [
        f"### {run_label}",
        f"- Command: `{item.get('command') or 'unknown'}`",
        f"- Status: `{item.get('status') or 'unknown'}`",
        f"- Updated: `{item.get('updated_at') or 'unknown'}`",
    ]
    if isinstance(item.get("run_dir"), str) and item["run_dir"]:
        lines.append(f"- Run dir: `{item['run_dir']}`")
    if isinstance(item.get("landing_artifact"), str) and item["landing_artifact"]:
        lines.append(f"- Landing page: `{item['landing_artifact']}`")
    if (
        isinstance(item.get("review_artifact"), str)
        and item["review_artifact"]
        and item["review_artifact"] != item.get("landing_artifact")
    ):
        lines.append(f"- Review artifact: `{item['review_artifact']}`")
    if isinstance(item.get("manifest_path"), str) and item["manifest_path"]:
        lines.append(f"- Manifest: `{item['manifest_path']}`")
    if isinstance(item.get("completed"), int) and isinstance(item.get("total_requested"), int):
        lines.append(f"- Progress: `{item['completed']}/{item['total_requested']}`")
    if all(isinstance(item.get(key), int) for key in ("ok_count", "warn_count", "error_count")):
        lines.append(
            f"- Status counts: `ok={item['ok_count']}` `warn={item['warn_count']}` `error={item['error_count']}`"
        )
    return lines


def _write_builder_brain_collection_index(
    *,
    collection_root: Path,
    entry: dict[str, Any],
) -> tuple[Path, Path]:
    return ai_os_browser.write_run_collection_index(
        collection_root=collection_root,
        index_filename="builder_brain_run_index.json",
        overview_filename="README.md",
        title="# Builder Brain Runs",
        entry={
            "command": entry.get("command"),
            "status": entry.get("status"),
            "label": entry.get("label"),
            "run_dir": entry.get("run_dir"),
            "landing_artifact": entry.get("landing_artifact"),
            "review_artifact": entry.get("review_artifact"),
            "manifest_path": entry.get("manifest_path"),
            "completed": entry.get("completed"),
            "total_requested": entry.get("total_requested"),
            "ok_count": entry.get("ok_count"),
            "warn_count": entry.get("warn_count"),
            "error_count": entry.get("error_count"),
        },
        render_entry_lines=_render_builder_brain_collection_entry,
    )


def _resolve_builder_brain_browser_root(*, collection_root: Path) -> Path:
    resolved_root = collection_root.resolve()
    for candidate in (resolved_root, *resolved_root.parents):
        if candidate.name == "builder_brain":
            return candidate
    return collection_root


def _render_builder_brain_browser_collection(item: dict[str, Any]) -> list[str]:
    collection_label = Path(str(item.get("collection_dir") or "")).name or "collection"
    lines = [f"### {collection_label}"]
    if isinstance(item.get("updated_at"), str) and item["updated_at"]:
        lines.append(f"- Updated: `{item['updated_at']}`")
    if isinstance(item.get("collection_dir"), str) and item["collection_dir"]:
        lines.append(f"- Collection dir: `{item['collection_dir']}`")
    if isinstance(item.get("collection_landing_artifact"), str) and item["collection_landing_artifact"]:
        lines.append(f"- Collection landing page: `{item['collection_landing_artifact']}`")
    if isinstance(item.get("collection_index_artifact"), str) and item["collection_index_artifact"]:
        lines.append(f"- Collection index: `{item['collection_index_artifact']}`")
    if isinstance(item.get("latest_label"), str) and item["latest_label"]:
        lines.append(f"- Latest run: `{item['latest_label']}`")
    if isinstance(item.get("latest_status"), str) and item["latest_status"]:
        lines.append(f"- Latest status: `{item['latest_status']}`")
    if isinstance(item.get("latest_run_dir"), str) and item["latest_run_dir"]:
        lines.append(f"- Latest run dir: `{item['latest_run_dir']}`")
    if isinstance(item.get("latest_landing_artifact"), str) and item["latest_landing_artifact"]:
        lines.append(f"- Latest landing page: `{item['latest_landing_artifact']}`")
    if isinstance(item.get("run_count"), int):
        lines.append(f"- Indexed runs: `{item['run_count']}`")
    return lines


def _write_builder_brain_browser_index(*, browser_root: Path) -> tuple[Path, Path]:
    return ai_os_browser.write_collection_browser_index(
        browser_root=browser_root,
        source_index_filename="builder_brain_run_index.json",
        collection_landing_filename="README.md",
        output_index_filename="builder_brain_collections_index.json",
        output_overview_filename="builder_brain_overview.md",
        title="# Builder Brain Collections",
        render_collection_lines=_render_builder_brain_browser_collection,
    )


def build_executor_handoff(
    *,
    inputs: dict[str, Any],
    spec: dict[str, Any],
    draft: dict[str, Any],
    build_package_payload: dict[str, Any],
    output_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    package_path = output_dir / "build_package.json"
    package_path.write_text(json.dumps(build_package_payload, indent=2), encoding="utf-8")
    execution_plan = build_package_payload.get("execution_plan")
    execution_plan_path = None
    if execution_plan:
        execution_plan_path = output_dir / "execution_plan.json"
        execution_plan_path.write_text(json.dumps(execution_plan, indent=2), encoding="utf-8")
    planning_context = build_package_payload.get("planning_context")
    if not isinstance(planning_context, dict):
        planning_context = None
    memory_health, memory_health_path, memory_health_report_path = _write_memory_health_artifacts(
        output_dir=output_dir,
        planning_context=planning_context,
    )
    wave_patch_payload = None
    wave_patch_payload_path = None
    if spec["primary_surface"] == "crma_dashboard" and execution_plan:
        for phase in execution_plan.get("phases", []):
            if phase.get("phase") == "storyboard_patch" and phase.get("wave_patch_payload"):
                wave_patch_payload = phase["wave_patch_payload"]
                break
    if wave_patch_payload:
        wave_patch_payload = deepcopy(wave_patch_payload)
        if planning_context:
            wave_patch_payload["planning_context"] = planning_context
        wave_patch_payload_path = output_dir / "wave_patch_payload.json"
        wave_patch_payload_path.write_text(json.dumps(wave_patch_payload, indent=2), encoding="utf-8")

    primary_surface = spec["primary_surface"]
    candidate_label = None
    labels = spec.get("candidate_surface_labels", [])
    if labels:
        candidate_label = labels[0]
    elif spec.get("reference_exemplar"):
        candidate_label = spec["reference_exemplar"].get("label")

    available_commands: list[dict[str, Any]] = []
    external_steps: list[str] = []
    memory_health = memory_health if isinstance(memory_health, dict) else None
    if isinstance(memory_health, dict):
        excluded_hits = memory_health.get("policy_exception_hits_excluded")
        fail_hits = memory_health.get("included_fail_count")
        generic_hits = memory_health.get("included_generic_goal_count")
        if any(isinstance(value, int) and value > 0 for value in (excluded_hits, fail_hits, generic_hits)):
            goal = planning_context.get("goal")
            if isinstance(goal, str) and goal:
                command_parts = [
                    "python3 scripts/run_memory.py search",
                    f"--goal {json.dumps(goal)}",
                ]
                domain = planning_context.get("domain")
                if isinstance(domain, str) and domain:
                    command_parts.append(f"--domain {json.dumps(domain)}")
                operation = planning_context.get("operation")
                if isinstance(operation, str) and operation:
                    command_parts.append(f"--operation {json.dumps(operation)}")
                command_parts.append("--include-policy-exceptions --json")
                available_commands.append(
                    _command_entry(
                        inputs=inputs,
                        script_path="scripts/run_memory.py",
                        command=" ".join(command_parts),
                        purpose="Inspect similar run memory, including excluded policy-exception hits, before trusting reuse.",
                    )
                )
            if isinstance(excluded_hits, int) and excluded_hits > 0:
                external_steps.append(
                    f"Review {excluded_hits} excluded policy-exception memory hit(s) before relying on prior run patterns."
                )
            if isinstance(fail_hits, int) and fail_hits > 0:
                external_steps.append(
                    f"Review {fail_hits} prior failing memory hit(s) before live execution."
                )
            if isinstance(generic_hits, int) and generic_hits > 0:
                external_steps.append(
                    f"Audit {generic_hits} generic-goal memory hit(s); quarantine them if they are polluting reuse quality."
                )

    if primary_surface == "crma_dashboard":
        if candidate_label:
            export_dir = output_dir / "live_export"
            baseline_dashboard_path = export_dir / _asset_export_slug(candidate_label) / "dashboard.json"
            available_commands.append(
                _command_entry(
                    inputs=inputs,
                    script_path="scripts/export_live_crma_assets.py",
                    command=(
                        f"python3 scripts/export_live_crma_assets.py "
                        f"{json.dumps(candidate_label)} --output-dir {json.dumps(str(export_dir))} --json"
                    ),
                    purpose="Export the closest live CRMA surface as the execution baseline.",
                )
            )
            available_commands.append(
                _command_entry(
                    inputs=inputs,
                    script_path="scripts/contract_lint.py",
                    command=(
                        f"python3 scripts/contract_lint.py --normalized --summary --json "
                        f"{json.dumps(str(export_dir))}"
                    ),
                    purpose="Run PATCH guardrails against the exported CRMA baseline before any mutation work.",
                )
            )
        if wave_patch_payload_path and candidate_label:
            bundle_output_dir = output_dir / "wave_patch_bundle"
            available_commands.append(
                _command_entry(
                    inputs=inputs,
                    script_path="scripts/wave_patch_executor.py",
                    command=_append_evaluation_arg(
                        (
                        f"python3 scripts/wave_patch_executor.py bundle "
                        f"--payload {json.dumps(str(wave_patch_payload_path))} "
                        f"--baseline {json.dumps(str(baseline_dashboard_path))} "
                        f"--output-dir {json.dumps(str(bundle_output_dir))} --json"
                        ),
                        planning_context,
                    ),
                    purpose="Compile the packaged CRMA payload and exported baseline into a normalized Wave patch bundle.",
                )
            )
            deploy_output_dir = output_dir / "wave_patch_deploy_preview"
            available_commands.append(
                _command_entry(
                    inputs=inputs,
                    script_path="scripts/wave_patch_executor.py",
                    command=_append_evaluation_arg(
                        (
                        f"python3 scripts/wave_patch_executor.py deploy "
                        f"--state {json.dumps(str(bundle_output_dir / 'dashboard_state.patch.json'))} "
                        f"--baseline {json.dumps(str(baseline_dashboard_path))} "
                        f"--output-dir {json.dumps(str(deploy_output_dir))} --json"
                        ),
                        planning_context,
                    ),
                    purpose="Preview the final Wave PATCH request against the inferred live dashboard target before any live mutation.",
                )
            )
        external_steps.append(
            "After the deploy preview is clean, rerun wave_patch_executor.py deploy with --apply to PATCH the live dashboard through the direct Wave workflow."
        )
    elif primary_surface == "salesforce_report":
        if draft.get("handoff_surface") == "crma_dashboard" and candidate_label:
            export_dir = output_dir / "handoff_export"
            available_commands.append(
                _command_entry(
                    inputs=inputs,
                    script_path="scripts/export_live_crma_assets.py",
                    command=(
                        f"python3 scripts/export_live_crma_assets.py "
                        f"{json.dumps(candidate_label)} --output-dir {json.dumps(str(export_dir))} --json"
                    ),
                    purpose="Validate the CRMA handoff surface that the report should link into.",
                )
            )
        report_bundle_dir = output_dir / "salesforce_report_bundle"
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_report_executor.py",
                command=(
                    f"python3 scripts/salesforce_report_executor.py validate "
                    f"--package {json.dumps(str(package_path))} --json"
                ),
                purpose="Validate the packaged Salesforce report authoring contract before native authoring.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_report_executor.py",
                command=(
                    f"python3 scripts/salesforce_report_executor.py bundle "
                    f"--package {json.dumps(str(package_path))} "
                    f"--output-dir {json.dumps(str(report_bundle_dir))} --json"
                ),
                purpose="Compile the packaged Salesforce report into a concrete authoring bundle and validation checklist.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_report_executor.py",
                command=(
                    f"python3 scripts/salesforce_report_executor.py preview "
                    f"--package {json.dumps(str(package_path))} "
                    f"--output-dir {json.dumps(str(report_bundle_dir))} --json"
                ),
                purpose="Compile a Reports REST preview contract and fill requirements before any native report mutation.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_report_executor.py",
                command=(
                    f"python3 scripts/salesforce_report_executor.py verify "
                    f"--package {json.dumps(str(package_path))} "
                    f"--output-dir {json.dumps(str(report_bundle_dir))} --json"
                ),
                purpose="Verify a native Salesforce report back against the packaged contract after authoring or apply.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_report_executor.py",
                command=_append_evaluation_arg(
                    (
                    f"python3 scripts/salesforce_report_executor.py apply "
                    f"--package {json.dumps(str(package_path))} "
                    f"--output-dir {json.dumps(str(report_bundle_dir))} --json"
                    ),
                    planning_context,
                ),
                purpose="Preview the executable Reports REST request sequence and confirm whether the package is mutation-ready.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_report_executor.py",
                command=_append_evaluation_arg(
                    (
                    f"python3 scripts/salesforce_report_executor.py complete "
                    f"--package {json.dumps(str(package_path))} "
                    f"--target-org __FILL_TARGET_ORG__ "
                    f"--output-dir {json.dumps(str(report_bundle_dir))} --json"
                    ),
                    planning_context,
                ),
                purpose="Apply the native report and immediately verify the authored live result in one CLI flow.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_report_executor.py",
                command=(
                    f"python3 scripts/salesforce_report_executor.py delete "
                    f"--report-id __FILL_REPORT_ID__ "
                    f"--target-org __FILL_TARGET_ORG__ "
                    f"--output-dir {json.dumps(str(report_bundle_dir / 'delete_cleanup'))} --json"
                ),
                purpose="Delete a throwaway or probe report and confirm it no longer resolves from the Reports REST endpoint.",
            )
        )
        external_steps.append(
            "Implement the emitted Salesforce report bundle natively in Salesforce using the packaged grouping, columns, filters, and sort order."
        )
        external_steps.append(
            "Preserve the CRMA handoff exactly as named in the package so the queue resolves into richer diagnostics."
        )
    else:
        dashboard_bundle_dir = output_dir / "salesforce_dashboard_bundle"
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_dashboard_executor.py",
                command=(
                    f"python3 scripts/salesforce_dashboard_executor.py validate "
                    f"--package {json.dumps(str(package_path))} --json"
                ),
                purpose="Validate the packaged native Salesforce dashboard contract before authoring.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_dashboard_executor.py",
                command=(
                    f"python3 scripts/salesforce_dashboard_executor.py bundle "
                    f"--package {json.dumps(str(package_path))} "
                    f"--output-dir {json.dumps(str(dashboard_bundle_dir))} --json"
                ),
                purpose="Compile the packaged native Salesforce dashboard into a concrete component plan and validation checklist.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_dashboard_executor.py",
                command=(
                    f"python3 scripts/salesforce_dashboard_executor.py preview "
                    f"--package {json.dumps(str(package_path))} "
                    f"--output-dir {json.dumps(str(dashboard_bundle_dir))} --json"
                ),
                purpose="Compile a Dashboards REST preview contract and fill requirements before any native dashboard mutation.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_dashboard_executor.py",
                command=(
                    f"python3 scripts/salesforce_dashboard_executor.py verify "
                    f"--package {json.dumps(str(package_path))} "
                    f"--output-dir {json.dumps(str(dashboard_bundle_dir))} --json"
                ),
                purpose="Verify a native Salesforce dashboard back against the packaged contract after authoring or apply.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_dashboard_executor.py",
                command=_append_evaluation_arg(
                    (
                    f"python3 scripts/salesforce_dashboard_executor.py apply "
                    f"--package {json.dumps(str(package_path))} "
                    f"--output-dir {json.dumps(str(dashboard_bundle_dir))} --json"
                    ),
                    planning_context,
                ),
                purpose="Preview the executable Dashboards REST request sequence and confirm whether the package is mutation-ready.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_dashboard_executor.py",
                command=_append_evaluation_arg(
                    (
                    f"python3 scripts/salesforce_dashboard_executor.py complete "
                    f"--package {json.dumps(str(package_path))} "
                    f"--session __FILL_PLAYWRIGHT_SESSION__ "
                    f"--target-org __FILL_TARGET_ORG__ "
                    f"--output-dir {json.dumps(str(dashboard_bundle_dir))} --json"
                    ),
                    planning_context,
                ),
                purpose="Apply the native dashboard, author all planned manual filters in one browser flow, and verify the finished live asset.",
            )
        )
        available_commands.append(
            _command_entry(
                inputs=inputs,
                script_path="scripts/salesforce_dashboard_executor.py",
                command=(
                    f"python3 scripts/salesforce_dashboard_executor.py delete "
                    f"--dashboard-id __FILL_DASHBOARD_ID__ "
                    f"--target-org __FILL_TARGET_ORG__ "
                    f"--output-dir {json.dumps(str(dashboard_bundle_dir / 'delete_cleanup'))} --json"
                ),
                purpose="Delete a throwaway or probe dashboard and confirm it no longer resolves from the Dashboards REST endpoint.",
            )
        )
        external_steps.append(
            "Implement the emitted native Salesforce dashboard bundle in Salesforce and keep it lightweight."
        )
        if draft.get("handoff_surface") == "salesforce_report":
            external_steps.append(
                "Back the native dashboard with a real Salesforce report handoff rather than duplicating row-level detail in tiles."
            )

    handoff = {
        "handoff_version": 1,
        "primary_lane": build_package_payload["execution_lane"],
        "repo_execution_fit": build_package_payload["repo_execution_fit"],
        "package_artifact": str(package_path),
        "execution_plan_artifact": str(execution_plan_path) if execution_plan_path else None,
        "wave_patch_payload_artifact": str(wave_patch_payload_path) if wave_patch_payload_path else None,
        "design_constraints": build_package_payload.get("design_constraints", []),
        "critic_rationale": build_package_payload.get("critic_rationale", []),
        "execution_plan": execution_plan,
        "wave_patch_payload": wave_patch_payload,
        "planning_context": planning_context,
        "memory_health": memory_health if isinstance(memory_health, dict) else None,
        "memory_health_artifact": str(memory_health_path) if memory_health_path else None,
        "memory_health_report_artifact": str(memory_health_report_path) if memory_health_report_path else None,
        "available_commands": available_commands,
        "external_steps": external_steps,
    }
    handoff_review_path = _write_handoff_review_artifact(output_dir=output_dir, handoff=handoff)
    handoff["handoff_review_artifact"] = str(handoff_review_path)
    artifacts = [{"type": "build_package", "path": str(package_path)}]
    if execution_plan_path:
        artifacts.append({"type": "execution_plan", "path": str(execution_plan_path)})
    if wave_patch_payload_path:
        artifacts.append({"type": "wave_patch_payload", "path": str(wave_patch_payload_path)})
    if memory_health_path:
        artifacts.append({"type": "memory_health", "path": str(memory_health_path)})
    if memory_health_report_path:
        artifacts.append({"type": "memory_health_report", "path": str(memory_health_report_path)})
    artifacts.append({"type": "handoff_review", "path": str(handoff_review_path)})
    return handoff, artifacts


def build_inventory(inputs: dict[str, Any]) -> dict[str, Any]:
    profiles = inputs["builder_profiles"]
    excellence = inputs["builder_excellence"]
    return make_result(
        status="ok",
        command="inventory",
        messages=[ai.make_message("info", "inventory_ready", "Loaded builder-brain profiles.")],
        inventory={
            "surface_adapters": profiles["surface_adapters"],
            "build_modes": profiles["build_modes"],
            "critique_checks": profiles["critique_checks"],
            "patterns": excellence["patterns"],
            "surface_exemplars": excellence["surface_exemplars"],
        },
    )


def validate_profiles(inputs: dict[str, Any]) -> dict[str, Any]:
    profiles = inputs["builder_profiles"]
    intelligence_profiles = inputs["profiles"]
    excellence = inputs["builder_excellence"]
    errors: list[str] = []

    for surface in profiles["surface_adapters"]:
        if surface not in intelligence_profiles["surface_types"]:
            errors.append(f"unknown surface adapter: {surface}")
    for persona in profiles["persona_layouts"]:
        if persona not in intelligence_profiles["persona_aliases"]:
            errors.append(f"unknown persona layout: {persona}")
    for domain in profiles["domain_filters"]:
        if domain not in intelligence_profiles["domain_aliases"]:
            errors.append(f"unknown domain filter profile: {domain}")
    for pattern in excellence["patterns"].values():
        if pattern["primary_surface"] not in profiles["surface_adapters"]:
            errors.append(f"unknown excellence primary surface: {pattern['primary_surface']}")
        preferred_secondary = pattern.get("preferred_secondary_surface")
        if preferred_secondary and preferred_secondary not in profiles["surface_adapters"]:
            errors.append(f"unknown excellence secondary surface: {preferred_secondary}")
    for exemplar in excellence["surface_exemplars"]:
        if exemplar["pattern_id"] not in excellence["patterns"]:
            errors.append(f"unknown exemplar pattern: {exemplar['pattern_id']}")

    status = "error" if errors else "ok"
    return make_result(
        status=status,
        command="validate",
        messages=[
            ai.make_message(
                "error" if errors else "info",
                "profiles_invalid" if errors else "profiles_valid",
                "Builder-brain profiles are invalid." if errors else "Builder-brain profiles are valid.",
            )
        ],
        coverage={
            "surface_adapters": len(profiles["surface_adapters"]),
            "build_modes": len(profiles["build_modes"]),
            "persona_layouts": len(profiles["persona_layouts"]),
            "critique_checks": len(profiles["critique_checks"]),
            "patterns": len(excellence["patterns"]),
            "surface_exemplars": len(excellence["surface_exemplars"]),
        },
        errors=errors,
    )


def build_retrieve_result(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    surface_override: str | None,
) -> dict[str, Any]:
    spec = build_spec(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        surface_override=surface_override,
    )
    retrieval_context = spec["retrieval_context"]
    return make_result(
        status="ok",
        command="retrieve",
        messages=[
            ai.make_message(
                "info",
                "retrieval_ready",
                "Retrieved local patterns and exemplar surfaces for the builder context.",
            )
        ],
        spec=spec,
        retrieval_context=retrieval_context,
    )


def build_spec_result(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    surface_override: str | None,
) -> dict[str, Any]:
    spec = build_spec(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        surface_override=surface_override,
    )
    return make_result(
        status="ok",
        command="spec",
        messages=[ai.make_message("info", "spec_ready", "Built a neutral analytics build spec.")],
        spec=spec,
    )


def build_draft_result(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    surface_override: str | None,
) -> dict[str, Any]:
    spec = build_spec(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        surface_override=surface_override,
    )
    draft = build_draft(spec, inputs)
    return make_result(
        status="ok",
        command="draft",
        messages=[ai.make_message("info", "draft_ready", "Built a first-pass surface draft.")],
        spec=spec,
        draft=draft,
    )


def build_critique_result(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    surface_override: str | None,
) -> dict[str, Any]:
    spec = build_spec(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        surface_override=surface_override,
    )
    draft = build_draft(spec, inputs)
    critique = critique_draft(spec, draft, inputs)
    return make_result(
        status=critique["status"],
        command="critique",
        messages=[
            ai.make_message(
                "warn" if critique["status"] != "ok" else "info",
                "critique_complete",
                "Completed builder-brain critique.",
            )
        ],
        spec=spec,
        draft=draft,
        critique=critique,
    )


def build_revise_result(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    surface_override: str | None,
) -> dict[str, Any]:
    spec = build_spec(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        surface_override=surface_override,
    )
    draft = build_draft(spec, inputs)
    critique_before = critique_draft(spec, draft, inputs)
    revised_spec, revised_draft, revision_log = revise_draft(spec, draft, critique_before, inputs)
    critique_after = critique_draft(revised_spec, revised_draft, inputs)
    return make_result(
        status=critique_after["status"],
        command="revise",
        messages=[
            ai.make_message(
                "warn" if critique_after["status"] != "ok" else "info",
                "revision_complete",
                "Built a second-pass revised draft from critique findings.",
            )
        ],
        spec=spec,
        draft=draft,
        critique_before=critique_before,
        revised_spec=revised_spec,
        revised_draft=revised_draft,
        critique_after=critique_after,
        revisions=revision_log,
    )


def build_package_result(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    surface_override: str | None,
    plan_path: Path | None,
    evaluation_path: Path | None,
) -> dict[str, Any]:
    try:
        planning_context = _build_planning_context(
            plan_path=plan_path,
            evaluation_path=evaluation_path,
            query=query,
            persona=persona,
            domain=domain,
            operation=operation,
        )
    except Exception as exc:
        return make_result(
            status="error",
            command="package",
            messages=[ai.make_message("error", "planning_context_invalid", str(exc))],
        )

    spec = build_spec(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        surface_override=surface_override,
    )
    draft = build_draft(spec, inputs)
    critique_before = critique_draft(spec, draft, inputs)
    revised_spec, revised_draft, revision_log = revise_draft(spec, draft, critique_before, inputs)
    critique_after = critique_draft(revised_spec, revised_draft, inputs)
    build_package_payload = build_package(
        inputs,
        revised_spec,
        revised_draft,
        critique_before,
        critique_after,
        revision_log,
        planning_context,
    )
    return make_result(
        status=critique_after["status"],
        command="package",
        messages=[
            ai.make_message(
                "warn" if critique_after["status"] != "ok" else "info",
                "package_complete",
                "Built an execution handoff package from the revised draft.",
            )
        ],
        spec=spec,
        draft=draft,
        critique_before=critique_before,
        revised_spec=revised_spec,
        revised_draft=revised_draft,
        critique_after=critique_after,
        revisions=revision_log,
        build_package=build_package_payload,
    )


def build_handoff_result(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    surface_override: str | None,
    output_dir: Path | None,
    plan_path: Path | None,
    evaluation_path: Path | None,
) -> dict[str, Any]:
    try:
        planning_context = _build_planning_context(
            plan_path=plan_path,
            evaluation_path=evaluation_path,
            query=query,
            persona=persona,
            domain=domain,
            operation=operation,
        )
    except Exception as exc:
        return make_result(
            status="error",
            command="handoff",
            messages=[ai.make_message("error", "planning_context_invalid", str(exc))],
        )

    spec = build_spec(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        surface_override=surface_override,
    )
    draft = build_draft(spec, inputs)
    critique_before = critique_draft(spec, draft, inputs)
    revised_spec, revised_draft, revision_log = revise_draft(spec, draft, critique_before, inputs)
    critique_after = critique_draft(revised_spec, revised_draft, inputs)
    build_package_payload = build_package(
        inputs,
        revised_spec,
        revised_draft,
        critique_before,
        critique_after,
        revision_log,
        planning_context,
    )
    effective_output_dir = output_dir or (ROOT / "output" / "builder_brain" / _package_slug(revised_spec))
    executor_handoff, artifacts = build_executor_handoff(
        inputs=inputs,
        spec=revised_spec,
        draft=revised_draft,
        build_package_payload=build_package_payload,
        output_dir=effective_output_dir,
    )
    return make_result(
        status=critique_after["status"],
        command="handoff",
        messages=[
            ai.make_message(
                "warn" if critique_after["status"] != "ok" else "info",
                "handoff_complete",
                "Built an executor-facing handoff from the revised package.",
            )
        ],
        artifacts=artifacts,
        spec=spec,
        draft=draft,
        critique_before=critique_before,
        revised_spec=revised_spec,
        revised_draft=revised_draft,
        critique_after=critique_after,
        revisions=revision_log,
        build_package=build_package_payload,
        executor_handoff=executor_handoff,
    )


def build_probe_result(
    inputs: dict[str, Any],
    *,
    query: str | None,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    surface_override: str | None,
    output_dir: Path | None,
    target_org: str,
    session: str | None,
    cleanup: bool,
    clone_from_report_id: str | None,
    clone_from_dashboard_id: str | None,
    dashboard_filter_automation_script: Path,
    package_path: Path | None,
    executor_timeout_seconds: int | None,
) -> dict[str, Any]:
    if package_path is None and not query:
        return make_result(
            status="error",
            command="probe",
            messages=[
                ai.make_message(
                    "error",
                    "probe_requires_query_or_package",
                    "Provide either --query or --package when running a builder probe.",
                )
            ],
            command_class="mutating",
        )

    probe_slug = (
        _package_probe_slug(package_path.expanduser())
        if package_path is not None
        else re.sub(r"[^a-z0-9]+", "_", query.lower()).strip("_") or "builder_probe"
    )
    handoff_root = output_dir or (ROOT / "output" / "builder_brain" / f"{probe_slug}_probe")
    try:
        if package_path is not None:
            handoff_result = _build_handoff_from_package(
                inputs,
                package_path=package_path.expanduser(),
                output_dir=handoff_root / "00_handoff",
            )
        else:
            handoff_result = build_handoff_result(
                inputs,
                query=query,
                persona=persona,
                domain=domain,
                operation=operation,
                surface_override=surface_override,
                output_dir=handoff_root / "00_handoff",
                plan_path=None,
                evaluation_path=None,
            )
    except Exception as exc:
        return make_result(
            status="error",
            command="probe",
            messages=[ai.make_message("error", "probe_handoff_failed", str(exc))],
            command_class="mutating",
        )
    if handoff_result.get("status") == "error":
        handoff_result["command_class"] = "mutating"
        return handoff_result
    executor_handoff = handoff_result.get("executor_handoff") or {}
    primary_lane = executor_handoff.get("primary_lane")
    package_artifact = executor_handoff.get("package_artifact")

    if primary_lane not in {"salesforce_report_handoff", "salesforce_dashboard_handoff"}:
        return make_result(
            status="error",
            command="probe",
            messages=[
                ai.make_message(
                    "error",
                    "probe_surface_unsupported",
                    "Probe currently supports only native Salesforce reports and native Salesforce dashboards.",
                )
            ],
            artifacts=handoff_result.get("artifacts", []),
            command_class="mutating",
            handoff_result=handoff_result,
        )

    if not isinstance(package_artifact, str) or not package_artifact:
        return make_result(
            status="error",
            command="probe",
            messages=[ai.make_message("error", "probe_package_missing", "Builder handoff did not emit a package artifact.")],
            artifacts=handoff_result.get("artifacts", []),
            command_class="mutating",
            handoff_result=handoff_result,
        )

    effective_output_dir = handoff_root
    execute_output_dir = effective_output_dir / "01_complete"
    cleanup_output_dir = effective_output_dir / "02_delete"

    if primary_lane == "salesforce_report_handoff":
        if not clone_from_report_id:
            return make_result(
                status="error",
                command="probe",
                messages=[
                    ai.make_message(
                        "error",
                        "clone_from_report_required",
                        "Provide --clone-from-report-id when probing a native Salesforce report build.",
                    )
                ],
                artifacts=handoff_result.get("artifacts", []),
                command_class="mutating",
                handoff_result=handoff_result,
            )
        execute_command = [
            sys.executable,
            str(REPORT_EXECUTOR_SCRIPT),
            "complete",
            "--package",
            package_artifact,
            "--clone-from-report-id",
            clone_from_report_id,
            "--autofill-live",
            "--target-org",
            target_org,
            "--allow-missing-evaluation",
            "--output-dir",
            str(execute_output_dir),
            "--json",
        ]
        delete_command_template = [
            sys.executable,
            str(REPORT_EXECUTOR_SCRIPT),
            "delete",
            "--report-id",
            "__FILL_ASSET_ID__",
            "--target-org",
            target_org,
            "--output-dir",
            str(cleanup_output_dir),
            "--json",
        ]
        asset_key = "applied_report"
        asset_id_key = "id"
    else:
        if not session:
            return make_result(
                status="error",
                command="probe",
                messages=[
                    ai.make_message(
                        "error",
                        "session_required",
                        "Provide --session when probing a native Salesforce dashboard build.",
                    )
                ],
                artifacts=handoff_result.get("artifacts", []),
                command_class="mutating",
                handoff_result=handoff_result,
            )
        execute_command = [
            sys.executable,
            str(DASHBOARD_EXECUTOR_SCRIPT),
            "complete",
            "--package",
            package_artifact,
            "--autofill-live",
            "--target-org",
            target_org,
            "--allow-missing-evaluation",
            "--session",
            session,
            "--dashboard-filter-automation-script",
            str(dashboard_filter_automation_script),
            "--output-dir",
            str(execute_output_dir),
            "--json",
        ]
        if clone_from_dashboard_id:
            execute_command.extend(["--clone-from-dashboard-id", clone_from_dashboard_id])
        delete_command_template = [
            sys.executable,
            str(DASHBOARD_EXECUTOR_SCRIPT),
            "delete",
            "--dashboard-id",
            "__FILL_ASSET_ID__",
            "--target-org",
            target_org,
            "--output-dir",
            str(cleanup_output_dir),
            "--json",
        ]
        asset_key = "applied_dashboard"
        asset_id_key = "id"

    try:
        execute_exit_code, execution_result = _run_json_command(
            execute_command,
            timeout_seconds=executor_timeout_seconds,
        )
    except Exception as exc:
        recovered_asset_id, recovered_cleanup_result, recovered_artifacts, recovered_messages = _attempt_failed_probe_cleanup(
            primary_lane=primary_lane,
            execute_output_dir=execute_output_dir,
            delete_command_template=delete_command_template,
            applied_asset_id=None,
            cleanup_requested=cleanup,
            executor_timeout_seconds=executor_timeout_seconds,
        )
        combined_artifacts = [
            *(handoff_result.get("artifacts") or []),
            *recovered_artifacts,
            *((recovered_cleanup_result or {}).get("artifacts") or []),
        ]
        return make_result(
            status="error",
            command="probe",
            messages=[
                *handoff_result.get("messages", []),
                *recovered_messages,
                ai.make_message("error", "probe_execution_failed", str(exc)),
            ],
            artifacts=combined_artifacts,
            command_class="mutating",
            handoff_result=handoff_result,
            cleanup_result=recovered_cleanup_result,
            summary={
                "primary_lane": primary_lane,
                "target_org": target_org,
                "execution_status": "error",
                "cleanup_requested": cleanup,
                "created_asset_id": recovered_asset_id,
                "package_source": "provided_package" if package_path is not None else "query_routing",
                **({"cleanup_status": recovered_cleanup_result.get("status")} if recovered_cleanup_result is not None else {}),
            },
        )

    combined_artifacts = [
        *(handoff_result.get("artifacts") or []),
        *(execution_result.get("artifacts") or []),
    ]
    applied_asset = execution_result.get(asset_key) or {}
    applied_asset_id = applied_asset.get(asset_id_key) if isinstance(applied_asset, dict) else None

    if execute_exit_code != 0 or execution_result.get("status") == "error":
        recovered_asset_id, recovered_cleanup_result, recovered_artifacts, recovered_messages = _attempt_failed_probe_cleanup(
            primary_lane=primary_lane,
            execute_output_dir=execute_output_dir,
            delete_command_template=delete_command_template,
            applied_asset_id=applied_asset_id if isinstance(applied_asset_id, str) else None,
            cleanup_requested=cleanup,
            executor_timeout_seconds=executor_timeout_seconds,
        )
        combined_artifacts.extend(recovered_artifacts)
        if recovered_cleanup_result is not None:
            combined_artifacts.extend(recovered_cleanup_result.get("artifacts") or [])
        return make_result(
            status="error",
            command="probe",
            messages=[
                *handoff_result.get("messages", []),
                *execution_result.get("messages", []),
                *recovered_messages,
                ai.make_message("error", "probe_execution_failed", "Probe execution failed before cleanup."),
            ],
            artifacts=combined_artifacts,
            command_class="mutating",
            handoff_result=handoff_result,
            execution_result=execution_result,
            cleanup_result=recovered_cleanup_result,
            summary={
                "primary_lane": primary_lane,
                "target_org": target_org,
                "execution_status": execution_result.get("status"),
                "cleanup_requested": cleanup,
                "package_source": "provided_package" if package_path is not None else "query_routing",
                "created_asset_id": recovered_asset_id if isinstance(recovered_asset_id, str) else applied_asset_id,
                **({"cleanup_status": recovered_cleanup_result.get("status")} if recovered_cleanup_result is not None else {}),
            },
        )

    cleanup_result: dict[str, Any] | None = None
    if cleanup:
        if not isinstance(applied_asset_id, str) or not applied_asset_id:
            return make_result(
                status="error",
                command="probe",
                messages=[
                    *handoff_result.get("messages", []),
                    *execution_result.get("messages", []),
                    ai.make_message("error", "probe_cleanup_missing_asset_id", "Execution completed without a live asset id to clean up."),
                ],
                artifacts=combined_artifacts,
                command_class="mutating",
                handoff_result=handoff_result,
                execution_result=execution_result,
                summary={
                    "primary_lane": primary_lane,
                    "target_org": target_org,
                    "execution_status": execution_result.get("status"),
                    "cleanup_requested": cleanup,
                    "package_source": "provided_package" if package_path is not None else "query_routing",
                },
            )
        delete_command = [applied_asset_id if item == "__FILL_ASSET_ID__" else item for item in delete_command_template]
        try:
            cleanup_exit_code, cleanup_result = _run_json_command(
                delete_command,
                timeout_seconds=executor_timeout_seconds,
            )
        except Exception as exc:
            return make_result(
                status="error",
                command="probe",
                messages=[
                    *handoff_result.get("messages", []),
                    *execution_result.get("messages", []),
                    ai.make_message("error", "probe_cleanup_failed", str(exc)),
                ],
                artifacts=combined_artifacts,
                command_class="mutating",
                handoff_result=handoff_result,
                execution_result=execution_result,
                summary={
                    "primary_lane": primary_lane,
                    "target_org": target_org,
                    "execution_status": execution_result.get("status"),
                    "cleanup_requested": cleanup,
                    "created_asset_id": applied_asset_id,
                    "package_source": "provided_package" if package_path is not None else "query_routing",
                },
            )
        combined_artifacts.extend(cleanup_result.get("artifacts") or [])
        final_status = cleanup_result.get("status", "error")
        if cleanup_exit_code != 0 and final_status == "ok":
            final_status = "warn"
    else:
        final_status = execution_result.get("status", "error")

    probe_summary: dict[str, Any] = {
        "primary_lane": primary_lane,
        "target_org": target_org,
        "execution_status": execution_result.get("status"),
        "cleanup_requested": cleanup,
        "created_asset_id": applied_asset_id,
        "package_source": "provided_package" if package_path is not None else "query_routing",
        "package_artifact": executor_handoff.get("package_artifact"),
        "execution_plan_artifact": executor_handoff.get("execution_plan_artifact"),
        "memory_health_artifact": executor_handoff.get("memory_health_artifact"),
        "memory_health_report_artifact": executor_handoff.get("memory_health_report_artifact"),
        "handoff_review_artifact": executor_handoff.get("handoff_review_artifact"),
    }
    if cleanup_result is not None:
        probe_summary["cleanup_status"] = cleanup_result.get("status")

    result_messages = [
        *handoff_result.get("messages", []),
        *execution_result.get("messages", []),
    ]
    if cleanup_result is not None:
        result_messages.extend(cleanup_result.get("messages", []))
    else:
        result_messages.append(
            ai.make_message(
                "info",
                "probe_cleanup_skipped",
                "Probe asset was left in place because --cleanup was not requested.",
            )
        )

    return make_result(
        status=final_status,
        command="probe",
        messages=result_messages,
        artifacts=combined_artifacts,
        command_class="mutating",
        handoff_result=handoff_result,
        execution_result=execution_result,
        cleanup_result=cleanup_result,
        summary=probe_summary,
    )


def build_probe_matrix_result(
    inputs: dict[str, Any],
    *,
    manifest_path: Path,
    output_dir: Path | None,
    target_org: str | None,
    cleanup: bool,
    dashboard_filter_automation_script: Path,
    stop_on_error: bool,
    executor_timeout_seconds: int | None,
) -> dict[str, Any]:
    try:
        manifest_file = manifest_path.expanduser()
        manifest = load_json(manifest_file)
    except Exception as exc:
        return make_result(
            status="error",
            command="probe-matrix",
            messages=[ai.make_message("error", "probe_matrix_manifest_invalid", str(exc))],
            command_class="mutating",
        )

    defaults = manifest.get("defaults", {})
    if defaults and not isinstance(defaults, dict):
        return make_result(
            status="error",
            command="probe-matrix",
            messages=[
                ai.make_message(
                    "error",
                    "probe_matrix_defaults_invalid",
                    "Manifest defaults must be a JSON object when provided.",
                )
            ],
            command_class="mutating",
        )
    if not isinstance(defaults, dict):
        defaults = {}

    probes = manifest.get("probes")
    if not isinstance(probes, list) or not probes:
        return make_result(
            status="error",
            command="probe-matrix",
            messages=[
                ai.make_message(
                    "error",
                    "probe_matrix_probes_missing",
                    "Manifest must define a non-empty probes array.",
                )
            ],
            command_class="mutating",
        )

    matrix_root = output_dir or (ROOT / "output" / "builder_brain" / f"{manifest_file.stem}_probe_matrix")
    matrix_root.mkdir(parents=True, exist_ok=True)
    manifest_copy_path = matrix_root / "probe_matrix_manifest.json"
    manifest_copy_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    probe_runs: list[dict[str, Any]] = []
    ok_count = 0
    warn_count = 0
    error_count = 0
    stopped_early = False
    cleanup_requested_count = 0

    for index, probe_entry in enumerate(probes):
        if not isinstance(probe_entry, dict):
            result_payload = make_result(
                status="error",
                command="probe",
                messages=[
                    ai.make_message(
                        "error",
                        "probe_matrix_entry_invalid",
                        f"Probe entry {index + 1} must be a JSON object.",
                    )
                ],
                command_class="mutating",
            )
            entry_slug = _matrix_entry_slug(index, {})
        else:
            entry_slug = _matrix_entry_slug(index, probe_entry)
            entry_target_org = probe_entry.get("target_org") or target_org or defaults.get("target_org")
            entry_cleanup = probe_entry.get("cleanup", defaults.get("cleanup", cleanup))
            entry_timeout_seconds = probe_entry.get(
                "executor_timeout_seconds",
                defaults.get("executor_timeout_seconds", executor_timeout_seconds),
            )
            entry_dashboard_filter_script_value = probe_entry.get("dashboard_filter_automation_script")
            if not isinstance(entry_dashboard_filter_script_value, str) or not entry_dashboard_filter_script_value:
                entry_dashboard_filter_script_value = defaults.get("dashboard_filter_automation_script")
            if isinstance(entry_dashboard_filter_script_value, str) and entry_dashboard_filter_script_value:
                entry_dashboard_filter_script = _resolve_manifest_relative_path(
                    manifest_file,
                    entry_dashboard_filter_script_value,
                )
            else:
                entry_dashboard_filter_script = dashboard_filter_automation_script.expanduser()
            entry_surface = probe_entry.get("surface")
            surface_override = _surface_override(entry_surface) if isinstance(entry_surface, str) else None
            entry_output_dir = matrix_root / entry_slug
            entry_package_path = None
            if isinstance(probe_entry.get("package"), str):
                entry_package_path = _resolve_manifest_relative_path(manifest_file, probe_entry["package"])

            if not isinstance(entry_target_org, str) or not entry_target_org:
                result_payload = make_result(
                    status="error",
                    command="probe",
                    messages=[
                        ai.make_message(
                            "error",
                            "probe_matrix_target_org_missing",
                            f"Probe entry {index + 1} is missing target_org and no matrix-level default was provided.",
                        )
                    ],
                    command_class="mutating",
                )
            else:
                result_payload = build_probe_result(
                    inputs,
                    query=probe_entry.get("query") if isinstance(probe_entry.get("query"), str) else None,
                    persona=probe_entry.get("persona") if isinstance(probe_entry.get("persona"), str) else None,
                    domain=probe_entry.get("domain") if isinstance(probe_entry.get("domain"), str) else None,
                    operation=probe_entry.get("operation") if isinstance(probe_entry.get("operation"), str) else None,
                    surface_override=surface_override,
                    output_dir=entry_output_dir,
                    target_org=entry_target_org,
                    session=probe_entry.get("session") if isinstance(probe_entry.get("session"), str) else None,
                    cleanup=bool(entry_cleanup),
                    clone_from_report_id=probe_entry.get("clone_from_report_id")
                    if isinstance(probe_entry.get("clone_from_report_id"), str)
                    else None,
                    clone_from_dashboard_id=probe_entry.get("clone_from_dashboard_id")
                    if isinstance(probe_entry.get("clone_from_dashboard_id"), str)
                    else None,
                    dashboard_filter_automation_script=entry_dashboard_filter_script,
                    package_path=entry_package_path,
                    executor_timeout_seconds=entry_timeout_seconds
                    if isinstance(entry_timeout_seconds, int) and entry_timeout_seconds > 0
                    else None,
                )

        entry_output_dir = matrix_root / entry_slug
        entry_output_dir.mkdir(parents=True, exist_ok=True)
        result_path = entry_output_dir / "probe_result.json"
        result_path.write_text(json.dumps(result_payload, indent=2), encoding="utf-8")

        status = result_payload.get("status", "error")
        if status == "ok":
            ok_count += 1
        elif status == "warn":
            warn_count += 1
        else:
            error_count += 1

        entry_summary = result_payload.get("summary", {})
        if isinstance(entry_summary, dict) and entry_summary.get("cleanup_requested"):
            cleanup_requested_count += 1

        probe_runs.append(
            {
                "index": index + 1,
                "name": probe_entry.get("name") if isinstance(probe_entry, dict) else None,
                "status": status,
                "summary": entry_summary if isinstance(entry_summary, dict) else {},
                "result_path": str(result_path),
            }
        )

        if status == "error" and stop_on_error:
            stopped_early = True
            break

    overall_status = "ok"
    if error_count:
        overall_status = "error"
    elif warn_count:
        overall_status = "warn"

    summary = {
        "manifest_path": str(manifest_file),
        "total_requested": len(probes),
        "completed": len(probe_runs),
        "ok_count": ok_count,
        "warn_count": warn_count,
        "error_count": error_count,
        "cleanup_requested_count": cleanup_requested_count,
        "stopped_early": stopped_early,
    }
    matrix_review_path, matrix_readme_path = _write_probe_matrix_review_artifact(
        matrix_root=matrix_root,
        summary=summary,
        probe_runs=probe_runs,
    )
    collection_index_path, collection_readme_path = _write_builder_brain_collection_index(
        collection_root=matrix_root.parent,
        entry={
            "command": "probe-matrix",
            "status": overall_status,
            "label": matrix_root.name,
            "run_dir": str(matrix_root),
            "landing_artifact": str(matrix_readme_path),
            "review_artifact": str(matrix_review_path),
            "manifest_path": str(manifest_file),
            "completed": len(probe_runs),
            "total_requested": len(probes),
            "ok_count": ok_count,
            "warn_count": warn_count,
            "error_count": error_count,
        },
    )
    browser_index_path, browser_overview_path = _write_builder_brain_browser_index(
        browser_root=_resolve_builder_brain_browser_root(collection_root=matrix_root.parent),
    )
    ai_os_browser_index_path, ai_os_browser_overview_path = ai_os_browser.write_ai_os_browser_index(
        browser_root=ai_os_browser.resolve_ai_os_browser_root(collection_root=matrix_root.parent),
    )
    ai_os_health_summary = ai_os_browser.load_ai_os_browser_health_summary(index_path=ai_os_browser_index_path)
    ai_os_health_index_path, ai_os_health_overview_path = ai_os_browser.resolve_ai_os_health_paths(
        browser_root=ai_os_browser_index_path.parent,
    )
    summary["review_artifact"] = str(matrix_review_path)
    summary["landing_artifact"] = str(matrix_readme_path)
    summary["collection_index_artifact"] = str(collection_index_path)
    summary["collection_landing_artifact"] = str(collection_readme_path)
    summary["browser_index_artifact"] = str(browser_index_path)
    summary["browser_landing_artifact"] = str(browser_overview_path)
    summary["ai_os_browser_index_artifact"] = str(ai_os_browser_index_path)
    summary["ai_os_browser_landing_artifact"] = str(ai_os_browser_overview_path)
    summary["ai_os_health_index_artifact"] = str(ai_os_health_index_path)
    summary["ai_os_health_landing_artifact"] = str(ai_os_health_overview_path)
    summary["ai_os_health_summary"] = ai_os_health_summary
    summary_path = matrix_root / "probe_matrix_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "summary": summary,
                "probe_runs": probe_runs,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result_messages = [
        ai.make_message(
            "error" if overall_status == "error" else "warn" if overall_status == "warn" else "info",
            "probe_matrix_complete",
            f"Completed {len(probe_runs)} of {len(probes)} probe run(s).",
        ),
        ai.make_message(
            "info",
            "probe_matrix_review_ready",
            f"Operator landing page: {matrix_readme_path}",
        ),
        ai.make_message(
            "info",
            "builder_brain_collection_index_ready",
            f"Collection landing page: {collection_readme_path}",
        ),
        ai.make_message(
            "info",
            "builder_brain_browser_ready",
            f"Builder-brain browser: {browser_overview_path}",
        ),
        ai.make_message(
            "info",
            "ai_os_browser_ready",
            f"AI OS browser: {ai_os_browser_overview_path}",
        ),
        ai.make_message(
            "info",
            "ai_os_health_ready",
            f"AI OS health: {ai_os_health_overview_path}",
        ),
    ]
    return make_result(
        status=overall_status,
        command="probe-matrix",
        messages=result_messages,
        artifacts=[
            {"type": "probe_matrix_manifest", "path": str(manifest_copy_path)},
            {"type": "probe_matrix_summary", "path": str(summary_path)},
            {"type": "probe_matrix_review", "path": str(matrix_review_path)},
            {"type": "probe_matrix_readme", "path": str(matrix_readme_path)},
            {"type": "builder_brain_run_index", "path": str(collection_index_path)},
            {"type": "builder_brain_collection_readme", "path": str(collection_readme_path)},
            {"type": "builder_brain_collections_index", "path": str(browser_index_path)},
            {"type": "builder_brain_overview", "path": str(browser_overview_path)},
            {"type": "ai_os_collections_index", "path": str(ai_os_browser_index_path)},
            {"type": "ai_os_overview", "path": str(ai_os_browser_overview_path)},
            {"type": "ai_os_health", "path": str(ai_os_health_index_path)},
            {"type": "ai_os_health_overview", "path": str(ai_os_health_overview_path)},
            *({"type": "probe_result", "path": run["result_path"]} for run in probe_runs),
        ],
        command_class="mutating",
        summary=summary,
        probe_runs=probe_runs,
    )


def print_text(payload: dict[str, Any]) -> None:
    if payload["command"] == "validate":
        coverage = payload["coverage"]
        print(f"surface_adapters: {coverage['surface_adapters']}")
        print(f"build_modes: {coverage['build_modes']}")
        print(f"persona_layouts: {coverage['persona_layouts']}")
        print(f"critique_checks: {coverage['critique_checks']}")
        print(f"patterns: {coverage['patterns']}")
        print(f"surface_exemplars: {coverage['surface_exemplars']}")
        return
    if payload["command"] == "inventory":
        print("surface adapters:")
        for surface in payload["inventory"]["surface_adapters"]:
            print(f"- {surface}")
        print("excellence patterns:")
        for pattern in payload["inventory"]["patterns"]:
            print(f"- {pattern}")
        return
    if payload["command"] == "retrieve":
        spec = payload["spec"]
        retrieval = payload["retrieval_context"]
        print(f"primary_surface: {spec['primary_surface']}")
        print("patterns:")
        for item in retrieval["patterns"]:
            print(f"- {item['id']} ({item['score']})")
        print("exemplars:")
        for item in retrieval["exemplars"]:
            print(f"- {item['id']} ({item['score']})")
        return
    spec = payload.get("spec", {})
    if spec:
        print(f"primary_surface: {spec['primary_surface']}")
        if spec.get("secondary_surface"):
            print(f"secondary_surface: {spec['secondary_surface']}")
        print(f"build_mode: {spec['build_mode']}")
        print(f"decision: {spec['decision_statement']}")
    if payload["command"] == "draft":
        draft = payload["draft"]
        print(f"shape: {draft['shape']}")
        print(f"baseline_status: {draft['baseline_status']}")
    if payload["command"] == "critique":
        print(f"score: {payload['critique']['score']}")
        print(f"verdict: {payload['critique']['verdict']}")
        for finding in payload["critique"]["findings"]:
            print(f"- {finding['severity']}: {finding['code']}: {finding['text']}")
    if payload["command"] == "revise":
        print("revisions:")
        for item in payload["revisions"]:
            print(f"- {item['change']}: {item['reason']}")
        print(f"revised_primary_surface: {payload['revised_spec']['primary_surface']}")
        print(f"revised_score: {payload['critique_after']['score']}")
        print(f"revised_verdict: {payload['critique_after']['verdict']}")
    if payload["command"] == "package":
        package = payload["build_package"]
        print(f"package_status: {package['package_status']}")
        print(f"execution_lane: {package['execution_lane']}")
        print(f"repo_execution_fit: {package['repo_execution_fit']}")
        print(f"revised_primary_surface: {payload['revised_spec']['primary_surface']}")
        print(f"execution_plan: {package['execution_plan']['plan_type']}")
        memory_health = (package.get("planning_context") or {}).get("memory_health")
        if isinstance(memory_health, dict):
            excluded_hits = memory_health.get("policy_exception_hits_excluded")
            if isinstance(excluded_hits, int) and excluded_hits > 0:
                print(f"excluded_policy_exception_hits: {excluded_hits}")
        for item in package.get("critic_rationale", []):
            print(f"- critic: {item['critic']} ({item['status_before']} -> {item['status_after']})")
        for step in package["next_steps"]:
            print(f"- next: {step}")
    if payload["command"] == "handoff":
        handoff = payload["executor_handoff"]
        print(f"primary_lane: {handoff['primary_lane']}")
        print(f"repo_execution_fit: {handoff['repo_execution_fit']}")
        print(f"package_artifact: {handoff['package_artifact']}")
        memory_health = handoff.get("memory_health")
        if isinstance(memory_health, dict):
            excluded_hits = memory_health.get("policy_exception_hits_excluded")
            if isinstance(excluded_hits, int) and excluded_hits > 0:
                print(f"excluded_policy_exception_hits: {excluded_hits}")
        if handoff.get("execution_plan_artifact"):
            print(f"execution_plan_artifact: {handoff['execution_plan_artifact']}")
        if handoff.get("wave_patch_payload_artifact"):
            print(f"wave_patch_payload_artifact: {handoff['wave_patch_payload_artifact']}")
        if handoff.get("memory_health_artifact"):
            print(f"memory_health_artifact: {handoff['memory_health_artifact']}")
        if handoff.get("memory_health_report_artifact"):
            print(f"memory_health_report_artifact: {handoff['memory_health_report_artifact']}")
        if handoff.get("handoff_review_artifact"):
            print(f"handoff_review_artifact: {handoff['handoff_review_artifact']}")
        for item in handoff.get("design_constraints", []):
            print(f"- constraint: {item}")
        for item in handoff["available_commands"]:
            print(f"- command: {item['command']}")
        for step in handoff["external_steps"]:
            print(f"- external: {step}")
    if payload["command"] == "probe":
        summary = payload["summary"]
        print(f"primary_lane: {summary['primary_lane']}")
        print(f"execution_status: {summary['execution_status']}")
        print(f"created_asset_id: {summary.get('created_asset_id')}")
        print(f"cleanup_requested: {summary['cleanup_requested']}")
        print(f"package_source: {summary.get('package_source')}")
        if summary.get("handoff_review_artifact"):
            print(f"handoff_review_artifact: {summary['handoff_review_artifact']}")
        if summary.get("memory_health_report_artifact"):
            print(f"memory_health_report_artifact: {summary['memory_health_report_artifact']}")
        if "cleanup_status" in summary:
            print(f"cleanup_status: {summary['cleanup_status']}")
    if payload["command"] == "probe-matrix":
        summary = payload["summary"]
        print(f"completed: {summary['completed']}/{summary['total_requested']}")
        print(f"ok_count: {summary['ok_count']}")
        print(f"warn_count: {summary['warn_count']}")
        print(f"error_count: {summary['error_count']}")
        print(f"cleanup_requested_count: {summary['cleanup_requested_count']}")
        print(f"stopped_early: {summary['stopped_early']}")
        if summary.get("ai_os_browser_landing_artifact"):
            print(f"ai_os_browser_landing_artifact: {summary['ai_os_browser_landing_artifact']}")
        if summary.get("ai_os_browser_index_artifact"):
            print(f"ai_os_browser_index_artifact: {summary['ai_os_browser_index_artifact']}")
        if summary.get("ai_os_health_landing_artifact"):
            print(f"ai_os_health_landing_artifact: {summary['ai_os_health_landing_artifact']}")
        if summary.get("ai_os_health_index_artifact"):
            print(f"ai_os_health_index_artifact: {summary['ai_os_health_index_artifact']}")
        ai_os_health_summary = summary.get("ai_os_health_summary") or {}
        if isinstance(ai_os_health_summary, dict) and ai_os_health_summary:
            print(f"ai_os_risk_run_count: {ai_os_health_summary.get('risk_run_count', 0)}")
            print(f"ai_os_attention_run_count: {ai_os_health_summary.get('attention_run_count', 0)}")
            print(f"ai_os_evaluation_bypass_count: {ai_os_health_summary.get('evaluation_bypass_count', 0)}")
            print(f"ai_os_stale_collection_count: {ai_os_health_summary.get('stale_collection_count', 0)}")
        if summary.get("browser_landing_artifact"):
            print(f"builder_brain_browser_landing_artifact: {summary['browser_landing_artifact']}")
        if summary.get("browser_index_artifact"):
            print(f"builder_brain_browser_index_artifact: {summary['browser_index_artifact']}")
        if summary.get("collection_landing_artifact"):
            print(f"builder_brain_collection_landing_artifact: {summary['collection_landing_artifact']}")
        if summary.get("collection_index_artifact"):
            print(f"builder_brain_collection_index_artifact: {summary['collection_index_artifact']}")
        if summary.get("landing_artifact"):
            print(f"probe_matrix_landing_artifact: {summary['landing_artifact']}")
        if summary.get("review_artifact"):
            print(f"probe_matrix_review_artifact: {summary['review_artifact']}")
        for run in payload.get("probe_runs", []):
            review_path = (run.get("summary") or {}).get("handoff_review_artifact")
            if isinstance(review_path, str) and review_path:
                print(f"- probe[{run['index']}]: {run['status']} -> {run['result_path']} :: review={review_path}")
            else:
                print(f"- probe[{run['index']}]: {run['status']} -> {run['result_path']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Builder-brain CLI for cross-surface analytics design.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory = subparsers.add_parser("inventory", help="Show supported builder-brain surfaces and modes.")
    inventory.add_argument("--json", action="store_true", help="Print JSON output.")

    validate = subparsers.add_parser("validate", help="Validate builder-brain profiles.")
    validate.add_argument("--json", action="store_true", help="Print JSON output.")

    for command in ("retrieve", "spec", "draft", "critique", "revise", "package"):
        subparser = subparsers.add_parser(command, help=f"Build a {command} result for a business ask.")
        subparser.add_argument("--query", required=True, help="Free-form business request.")
        subparser.add_argument("--persona", default=None, help="Optional persona override.")
        subparser.add_argument("--domain", default=None, help="Optional domain override.")
        subparser.add_argument("--operation", default=None, help="Optional operation override.")
        subparser.add_argument(
            "--surface",
            default=None,
            help="Optional forced primary surface (salesforce_report, salesforce_dashboard, crma_dashboard).",
        )
        if command == "package":
            subparser.add_argument("--plan", default=None, help="Optional path to plan.json from the planner.")
            subparser.add_argument(
                "--evaluation",
                default=None,
                help="Optional path to evaluation.json from the plan evaluator.",
            )
        subparser.add_argument("--json", action="store_true", help="Print JSON output.")

    handoff = subparsers.add_parser("handoff", help="Build an executor-facing handoff from the revised package.")
    handoff.add_argument("--query", required=True, help="Free-form business request.")
    handoff.add_argument("--persona", default=None, help="Optional persona override.")
    handoff.add_argument("--domain", default=None, help="Optional domain override.")
    handoff.add_argument("--operation", default=None, help="Optional operation override.")
    handoff.add_argument(
        "--surface",
        default=None,
        help="Optional forced primary surface (salesforce_report, salesforce_dashboard, crma_dashboard).",
    )
    handoff.add_argument(
        "--output-dir",
        default=None,
        help="Optional directory for the emitted build package artifact.",
    )
    handoff.add_argument("--plan", default=None, help="Optional path to plan.json from the planner.")
    handoff.add_argument(
        "--evaluation",
        default=None,
        help="Optional path to evaluation.json from the plan evaluator.",
    )
    handoff.add_argument("--json", action="store_true", help="Print JSON output.")

    probe = subparsers.add_parser(
        "probe",
        help="Create, verify, and optionally clean up a throwaway native report or dashboard from one top-level builder command.",
    )
    probe.add_argument("--query", default=None, help="Free-form business request.")
    probe.add_argument(
        "--package",
        default=None,
        help="Optional path to an existing build_package.json to probe directly instead of rebuilding from query routing.",
    )
    probe.add_argument("--persona", default=None, help="Optional persona override.")
    probe.add_argument("--domain", default=None, help="Optional domain override.")
    probe.add_argument("--operation", default=None, help="Optional operation override.")
    probe.add_argument(
        "--surface",
        default=None,
        help="Optional forced primary surface (salesforce_report or salesforce_dashboard).",
    )
    probe.add_argument("--target-org", required=True, help="Target org alias/username for live probe execution.")
    probe.add_argument("--session", default=None, help="Required for native dashboard probes that author manual filters.")
    probe.add_argument(
        "--clone-from-report-id",
        default=None,
        help="Baseline report id used to autofill and seed native report probes.",
    )
    probe.add_argument(
        "--clone-from-dashboard-id",
        default=None,
        help="Optional baseline dashboard id used to seed native dashboard probes.",
    )
    probe.add_argument(
        "--dashboard-filter-automation-script",
        default=str(DEFAULT_DASHBOARD_FILTER_AUTOMATION_SCRIPT),
        help="Path to salesforce_dashboard_filter_automation.py or a compatible browser helper.",
    )
    probe.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete the authored probe asset after verify.",
    )
    probe.add_argument(
        "--output-dir",
        default=None,
        help="Optional directory for emitted handoff, execution, and cleanup artifacts.",
    )
    probe.add_argument(
        "--executor-timeout-seconds",
        type=int,
        default=None,
        help="Optional timeout applied to each nested executor command.",
    )
    probe.add_argument("--json", action="store_true", help="Print JSON output.")

    probe_matrix = subparsers.add_parser(
        "probe-matrix",
        help="Run a manifest of native report/dashboard probes and collect one summary plus one result artifact per entry.",
    )
    probe_matrix.add_argument("--manifest", required=True, help="Path to a JSON manifest describing the probe runs.")
    probe_matrix.add_argument("--target-org", default=None, help="Optional default target org alias/username.")
    probe_matrix.add_argument(
        "--cleanup",
        action="store_true",
        help="Default cleanup behavior for manifest entries that do not declare cleanup explicitly.",
    )
    probe_matrix.add_argument(
        "--dashboard-filter-automation-script",
        default=str(DEFAULT_DASHBOARD_FILTER_AUTOMATION_SCRIPT),
        help="Default path to salesforce_dashboard_filter_automation.py or a compatible browser helper.",
    )
    probe_matrix.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop the matrix after the first error instead of continuing through later entries.",
    )
    probe_matrix.add_argument(
        "--executor-timeout-seconds",
        type=int,
        default=None,
        help="Optional default timeout applied to each nested probe executor command.",
    )
    probe_matrix.add_argument(
        "--output-dir",
        default=None,
        help="Optional directory for emitted manifest, summary, and per-entry probe result artifacts.",
    )
    probe_matrix.add_argument("--json", action="store_true", help="Print JSON output.")

    return parser


def main() -> int:
    args = build_parser().parse_args()
    inputs = load_inputs()

    if args.command == "inventory":
        payload = build_inventory(inputs)
    elif args.command == "validate":
        payload = validate_profiles(inputs)
    else:
        surface_override = _surface_override(getattr(args, "surface", None))
        if surface_override and surface_override not in inputs["builder_profiles"]["surface_adapters"]:
            payload = make_result(
                status="error",
                command=args.command,
                messages=[
                    ai.make_message(
                        "error",
                        "unknown_surface",
                        f"Unknown builder surface override: {args.surface}.",
                    )
                ],
            )
        elif args.command == "spec":
            payload = build_spec_result(
                inputs,
                query=args.query,
                persona=args.persona,
                domain=args.domain,
                operation=args.operation,
                surface_override=surface_override,
            )
        elif args.command == "retrieve":
            payload = build_retrieve_result(
                inputs,
                query=args.query,
                persona=args.persona,
                domain=args.domain,
                operation=args.operation,
                surface_override=surface_override,
            )
        elif args.command == "draft":
            payload = build_draft_result(
                inputs,
                query=args.query,
                persona=args.persona,
                domain=args.domain,
                operation=args.operation,
                surface_override=surface_override,
            )
        elif args.command == "critique":
            payload = build_critique_result(
                inputs,
                query=args.query,
                persona=args.persona,
                domain=args.domain,
                operation=args.operation,
                surface_override=surface_override,
            )
        elif args.command == "revise":
            payload = build_revise_result(
                inputs,
                query=args.query,
                persona=args.persona,
                domain=args.domain,
                operation=args.operation,
                surface_override=surface_override,
            )
        elif args.command == "package":
            payload = build_package_result(
                inputs,
                query=args.query,
                persona=args.persona,
                domain=args.domain,
                operation=args.operation,
                surface_override=surface_override,
                plan_path=Path(args.plan).expanduser() if args.plan else None,
                evaluation_path=Path(args.evaluation).expanduser() if args.evaluation else None,
            )
        elif args.command == "probe":
            payload = build_probe_result(
                inputs,
                query=args.query,
                persona=args.persona,
                domain=args.domain,
                operation=args.operation,
                surface_override=surface_override,
                output_dir=Path(args.output_dir).expanduser() if args.output_dir else None,
                target_org=args.target_org,
                session=args.session,
                cleanup=args.cleanup,
                clone_from_report_id=args.clone_from_report_id,
                clone_from_dashboard_id=args.clone_from_dashboard_id,
                dashboard_filter_automation_script=Path(args.dashboard_filter_automation_script).expanduser(),
                package_path=Path(args.package).expanduser() if args.package else None,
                executor_timeout_seconds=args.executor_timeout_seconds,
            )
        elif args.command == "probe-matrix":
            payload = build_probe_matrix_result(
                inputs,
                manifest_path=Path(args.manifest).expanduser(),
                output_dir=Path(args.output_dir).expanduser() if args.output_dir else None,
                target_org=args.target_org,
                cleanup=args.cleanup,
                dashboard_filter_automation_script=Path(args.dashboard_filter_automation_script).expanduser(),
                stop_on_error=args.stop_on_error,
                executor_timeout_seconds=args.executor_timeout_seconds,
            )
        else:
            payload = build_handoff_result(
                inputs,
                query=args.query,
                persona=args.persona,
                domain=args.domain,
                operation=args.operation,
                surface_override=surface_override,
                output_dir=Path(args.output_dir).expanduser() if args.output_dir else None,
                plan_path=Path(args.plan).expanduser() if args.plan else None,
                evaluation_path=Path(args.evaluation).expanduser() if args.evaluation else None,
            )

    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2))
    else:
        print_text(payload)
    return 0 if payload["status"] in {"ok", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
