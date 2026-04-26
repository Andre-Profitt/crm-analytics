#!/usr/bin/env python3
"""Evidence-first harness planner over the CRM Analytics registry surface."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import analytics_intelligence as ai  # noqa: E402
import harness_registry as registry_tools  # noqa: E402
import run_memory  # noqa: E402


PROFILES_PATH = ROOT / "config" / "harness_planner_profiles.json"
CORE_MEMORY_PATH = ROOT / "config" / "agent_os_core_memory.json"
_BUILDER_INPUTS_CACHE: dict[str, Any] | None = None


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_inputs() -> dict[str, Any]:
    inputs = ai.load_inputs()
    inputs["registry"] = registry_tools.load_registry()
    inputs["planner_profiles"] = load_json(PROFILES_PATH)
    inputs["core_memory"] = load_json(CORE_MEMORY_PATH) if CORE_MEMORY_PATH.exists() else {}
    return inputs


def _load_builder_inputs() -> dict[str, Any]:
    global _BUILDER_INPUTS_CACHE
    if _BUILDER_INPUTS_CACHE is None:
        import builder_brain

        _BUILDER_INPUTS_CACHE = builder_brain.load_inputs()
    return _BUILDER_INPUTS_CACHE


def build_surface_advisory(
    *,
    inputs: dict[str, Any],
    query: str,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
    resolution: dict[str, Any] | None = None,
    route: dict[str, Any] | None = None,
) -> dict[str, Any]:
    import builder_brain

    resolution = resolution or ai.resolve_request(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
    )
    route = route or ai.route_surface(inputs, resolution)
    spec = builder_brain.build_spec(
        _load_builder_inputs(),
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        surface_override=None,
    )
    primary_surface = spec.get("primary_surface")
    effective_surface = (
        primary_surface
        if isinstance(primary_surface, str) and primary_surface
        else route.get("recommended_surface_type")
    )
    return {
        "route_surface_type": route.get("recommended_surface_type"),
        "effective_surface_type": effective_surface,
        "primary_surface": primary_surface,
        "secondary_surface": spec.get("secondary_surface"),
        "selection_reason": spec.get("selection_reason"),
        "builder_default_primary_surface": spec.get("builder_default_primary_surface"),
        "builder_default_secondary_surface": spec.get("builder_default_secondary_surface"),
        "report_action_surface_assessment": spec.get("report_action_surface_assessment"),
    }


def effective_surface_type(
    *,
    route: dict[str, Any],
    surface_advisory: dict[str, Any] | None = None,
) -> str:
    if isinstance(surface_advisory, dict):
        for key in ("effective_surface_type", "primary_surface"):
            value = surface_advisory.get(key)
            if isinstance(value, str) and value:
                return value
    return str(route.get("recommended_surface_type") or "")


def build_surface_memory_tags(
    *,
    route: dict[str, Any],
    candidate: dict[str, Any] | None,
    surface_advisory: dict[str, Any] | None = None,
) -> list[str]:
    tags: list[str] = []
    surface_type = effective_surface_type(route=route, surface_advisory=surface_advisory)
    if surface_type:
        tags.append(surface_type)
    candidate_id = candidate.get("id") if isinstance(candidate, dict) else None
    if isinstance(candidate_id, str) and candidate_id and candidate_id not in tags:
        tags.append(candidate_id)
    return tags


def required_evidence_for_surface(
    *,
    planner_profile: dict[str, Any],
    surface_advisory: dict[str, Any] | None = None,
) -> list[str]:
    effective_surface = (
        surface_advisory.get("effective_surface_type")
        if isinstance(surface_advisory, dict)
        else None
    )
    if effective_surface == "salesforce_report":
        return ["build_package", "report_rest_preview"]
    return list(planner_profile.get("required_evidence", []))


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
        "tool": "harness_planner",
        "lane": "intelligence_control",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return payload


def _load_memory_hits(
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    tags: list[str] | None = None,
    evidence_types: list[str] | None = None,
    memory_file: Path | None = None,
) -> list[dict[str, Any]]:
    if memory_file:
        payload = load_json(memory_file)
        hits = payload.get("similar_runs", [])
        return [item for item in hits if isinstance(item, dict)]
    return run_memory.search_runs(
        goal=query,
        persona=persona,
        domain=domain,
        operation=operation,
        tags=tags,
        evidence_types=evidence_types,
    )


def _step_name(script_path: str) -> str:
    names = {
        "scripts/export_live_crma_assets.py": "export_live_assets",
        "scripts/contract_lint.py": "contract_lint",
        "scripts/builder_brain.py": "build_report_handoff",
        "scripts/salesforce_report_executor.py": "preview_report_rest",
    }
    if script_path in names:
        return names[script_path]
    if script_path.startswith("scripts/audit_"):
        return "audit_surface"
    if script_path.startswith("scripts/profile_"):
        return "profile_surface"
    return Path(script_path).stem


def _normalized_values(values: list[str] | None) -> set[str]:
    normalized: set[str] = set()
    for value in values or []:
        if not isinstance(value, str):
            continue
        text = ai.normalize_text(value)
        if text:
            normalized.add(text)
    return normalized


def _build_memory_context(
    *,
    memory_hits: list[dict[str, Any]],
    required_evidence: list[str],
    resolved_persona: str | None,
    resolved_domain: str | None,
    operation_mode: str,
    surface_tags: list[str],
    candidate_audit_script: str | None,
    scoring: dict[str, Any],
) -> dict[str, Any]:
    success_boosts: dict[str, int] = {}
    failure_counts: dict[str, dict[str, int]] = {}
    incomplete_counts: dict[str, int] = {}
    preferred_success_hit: dict[str, Any] | None = None
    preferred_success_score = -1
    required_evidence_set = _normalized_values(required_evidence)
    surface_tag_set = _normalized_values(surface_tags)
    surface_type_tag = ai.normalize_text(surface_tags[0]) if surface_tags else ""
    candidate_tag = ai.normalize_text(surface_tags[1]) if len(surface_tags) > 1 and surface_tags[1] else ""

    for hit in memory_hits:
        sequence = [item for item in hit.get("sequence", []) if isinstance(item, str)]
        if not sequence:
            continue

        hit_tags = _normalized_values(hit.get("tags", []))
        hit_evidence = _normalized_values(hit.get("evidence_types", []))
        evidence_overlap = len(required_evidence_set & hit_evidence)
        route_bonus = int(hit.get("operation") == operation_mode) * int(scoring.get("memory_success_route_boost", 3))
        domain_bonus = int(bool(resolved_domain) and hit.get("domain") == resolved_domain) * 2
        persona_bonus = int(bool(resolved_persona) and hit.get("persona") == resolved_persona) * 2
        tag_bonus = len(surface_tag_set & hit_tags) * int(scoring.get("memory_success_tag_boost", 2))
        evidence_bonus = evidence_overlap * int(scoring.get("memory_success_evidence_boost", 2))
        full_match_bonus = 0
        if required_evidence_set and hit_evidence and required_evidence_set.issubset(hit_evidence):
            full_match_bonus = int(scoring.get("memory_success_full_match_boost", 6))
        candidate_match = bool(candidate_tag and candidate_tag in hit_tags)
        surface_type_match = bool(surface_type_tag and surface_type_tag in hit_tags)
        hit_audit_steps = [path for path in sequence if path.startswith("scripts/audit_")]
        if candidate_match and candidate_audit_script and hit_audit_steps and candidate_audit_script not in hit_audit_steps:
            continue
        success_reuse_allowed = False
        if candidate_tag:
            success_reuse_allowed = candidate_match
        else:
            success_reuse_allowed = surface_type_match and hit.get("operation") == operation_mode and (
                not required_evidence_set
                or bool(hit_evidence and required_evidence_set.issubset(hit_evidence))
            )

        verdict = hit.get("verdict")
        if verdict == "pass":
            if hit_tags and not success_reuse_allowed:
                continue

            per_script_bonus = int(scoring.get("memory_sequence_boost", 4))
            if success_reuse_allowed:
                per_script_bonus += (
                    route_bonus
                    + domain_bonus
                    + persona_bonus
                    + tag_bonus
                    + evidence_bonus
                    + full_match_bonus
                )
            for index, path in enumerate(sequence):
                success_boosts[path] = success_boosts.get(path, 0) + max(per_script_bonus - index, 1)

            hit_score = per_script_bonus + int(hit.get("score", 0))
            if success_reuse_allowed and hit_score > preferred_success_score:
                preferred_success_score = hit_score
                preferred_success_hit = hit
            continue

        if verdict == "fail":
            reason = str(hit.get("failure_reason") or "unknown_failure")
            for path in sequence:
                reasons = failure_counts.setdefault(path, {})
                reasons[reason] = reasons.get(reason, 0) + 1
            continue

        if verdict == "needs_more_evidence":
            for path in sequence:
                incomplete_counts[path] = incomplete_counts.get(path, 0) + 1
            continue

        legacy_boost = int(scoring.get("memory_sequence_boost", 4))
        for index, path in enumerate(sequence):
            success_boosts[path] = success_boosts.get(path, 0) + max(legacy_boost - index, 1)

    failure_penalties: dict[str, int] = {}
    repeated_failure_codes: dict[str, list[str]] = {}
    for path, reasons in failure_counts.items():
        penalty = 0
        repeated_codes: list[str] = []
        for reason, count in reasons.items():
            penalty += count * int(scoring.get("memory_failure_penalty", 5))
            if count > 1:
                penalty += (count - 1) * int(scoring.get("repeated_failure_penalty", 2))
                repeated_codes.append(reason)
        if incomplete_counts.get(path):
            penalty += incomplete_counts[path] * int(scoring.get("memory_incomplete_penalty", 2))
        failure_penalties[path] = penalty
        if repeated_codes:
            repeated_failure_codes[path] = sorted(repeated_codes)

    for path, count in incomplete_counts.items():
        if path not in failure_penalties and count:
            failure_penalties[path] = count * int(scoring.get("memory_incomplete_penalty", 2))

    return {
        "success_boosts": success_boosts,
        "failure_penalties": failure_penalties,
        "preferred_success_hit": preferred_success_hit,
        "repeated_failure_codes": repeated_failure_codes,
    }


def _compatible(prev_script: dict[str, Any] | None, next_script: dict[str, Any]) -> bool:
    if next_script["command_class"] == "mutating":
        return False
    if prev_script is None:
        return next_script["command_class"] in {"read_only", "live_read"} and next_script["lane"] != "native_surface_authoring"
    if next_script["lane"] not in prev_script["allowed_successor_lanes"]:
        return False
    predecessors = next_script["allowed_predecessor_lanes"]
    return not predecessors or prev_script["lane"] in predecessors


def _candidate_scripts_for_lane(
    route: dict[str, Any],
    *,
    lane: str,
    registry_by_path: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    for suggestion in route["script_suggestions"]:
        if suggestion["lane"] == lane:
            candidates: list[dict[str, Any]] = []
            for item in suggestion["scripts"]:
                path = item["path"]
                script = registry_by_path.get(path)
                if not script:
                    continue
                candidates.append({**script, "route_score": int(item.get("route_score", 0))})
            return candidates
    return []


def _score_script(
    script: dict[str, Any],
    *,
    missing_evidence: set[str],
    candidate_audit_script: str | None,
    memory_context: dict[str, Any],
    selected_paths: set[str],
    current_evidence: set[str],
    prev_script: dict[str, Any] | None,
    scoring: dict[str, Any],
) -> int:
    if script["path"] in selected_paths or not _compatible(prev_script, script):
        return -1_000
    if script["requires_local_export"] and "dashboard_json" not in current_evidence:
        return -500

    produced = set(script["evidence_types_produced"])
    missing = produced & missing_evidence
    score = len(missing) * int(scoring.get("missing_evidence_weight", 10))
    score += int(script.get("route_score", 0))

    if script["path"] == candidate_audit_script:
        score += int(scoring.get("candidate_script_boost", 6))
    if script["live_system"] != "none":
        score -= int(scoring.get("unnecessary_live_penalty", 1))
    if not missing and script["path"] != candidate_audit_script:
        score -= int(scoring.get("duplicate_evidence_penalty", 5))
    score += int(memory_context.get("success_boosts", {}).get(script["path"], 0))
    score -= int(memory_context.get("failure_penalties", {}).get(script["path"], 0))

    return score


def build_plan(
    *,
    inputs: dict[str, Any],
    query: str,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
    memory_hits: list[dict[str, Any]] | None = None,
    memory_health: dict[str, Any] | None = None,
    run_id: str | None = None,
    resolution: dict[str, Any] | None = None,
    route: dict[str, Any] | None = None,
    surface_advisory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    planner_profiles = inputs["planner_profiles"]
    registry_by_path = {item["path"]: item for item in inputs["registry"]["scripts"]}
    resolution = resolution or ai.resolve_request(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
    )
    route = route or ai.route_surface(inputs, resolution)
    surface_advisory = surface_advisory or build_surface_advisory(
        inputs=inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        resolution=resolution,
        route=route,
    )
    effective_surface = effective_surface_type(route=route, surface_advisory=surface_advisory)
    secondary_surface = (
        surface_advisory.get("secondary_surface")
        if isinstance(surface_advisory, dict)
        else None
    )
    candidate = resolution["candidate_surfaces"][0] if resolution["candidate_surfaces"] else None
    candidate_audit_script = candidate.get("audit_script") if candidate else None
    if effective_surface == "salesforce_report" and not isinstance(secondary_surface, str):
        candidate_audit_script = None
    candidate_audit_entry = registry_by_path.get(candidate_audit_script) if candidate_audit_script else None

    operation_key = route["operation_mode"]
    planner_profile = planner_profiles["operation_defaults"].get(
        operation_key,
        planner_profiles["operation_defaults"]["review_dashboard"],
    )
    lane_order = list(planner_profile["lane_order"])
    report_primary_safe_probe = effective_surface == "salesforce_report"
    if report_primary_safe_probe:
        lane_order = ["intelligence_control", "native_surface_authoring"]
    if (
        candidate_audit_entry
        and candidate_audit_entry.get("requires_local_export")
        and candidate_audit_entry["lane"] in lane_order
        and "live_inventory" in lane_order
    ):
        live_inventory_index = lane_order.index("live_inventory")
        candidate_lane_index = lane_order.index(candidate_audit_entry["lane"])
        if live_inventory_index > candidate_lane_index:
            lane_order.pop(live_inventory_index)
            candidate_lane_index = lane_order.index(candidate_audit_entry["lane"])
            lane_order.insert(candidate_lane_index, "live_inventory")

    required_evidence = required_evidence_for_surface(
        planner_profile=planner_profile,
        surface_advisory=surface_advisory,
    )
    if (
        candidate_audit_entry
        and candidate_audit_entry.get("requires_local_export")
        and "dashboard_json" not in required_evidence
    ):
        required_evidence.append("dashboard_json")
    missing_evidence = set(required_evidence)
    scoring = planner_profiles["scoring"]
    max_steps = int(planner_profiles.get("max_steps_default", 5))
    memory_context = _build_memory_context(
        memory_hits=memory_hits or [],
        required_evidence=required_evidence,
        resolved_persona=resolution.get("resolved_persona"),
        resolved_domain=resolution.get("resolved_domain"),
        operation_mode=route["operation_mode"],
        surface_tags=build_surface_memory_tags(
            route=route,
            candidate=candidate,
            surface_advisory=surface_advisory,
        ),
        candidate_audit_script=candidate_audit_script,
        scoring=scoring,
    )

    sequence: list[dict[str, Any]] = []
    selected_paths: set[str] = set()
    current_evidence: set[str] = set()
    planner_notes: list[str] = []
    prev_script: dict[str, Any] | None = None
    if report_primary_safe_probe:
        planner_notes.append(
            "Primary salesforce_report surface will use a read-only builder handoff plus REST preview as the safe probe sequence."
        )
    if effective_surface != route["recommended_surface_type"]:
        planner_notes.append(
            f"Surface advisory shifted the effective surface from {route['recommended_surface_type']} to {effective_surface}."
        )

    for lane in lane_order:
        lane_candidates = _candidate_scripts_for_lane(
            route,
            lane=lane,
            registry_by_path=registry_by_path,
        )
        if not lane_candidates:
            lane_candidates = [
                item
                for item in inputs["registry"]["scripts"]
                if item["lane"] == lane and item["command_class"] != "mutating"
            ]

        scored: list[tuple[int, dict[str, Any]]] = []
        for script in lane_candidates:
            score = _score_script(
                script,
                missing_evidence=missing_evidence,
                candidate_audit_script=candidate_audit_script,
                memory_context=memory_context,
                selected_paths=selected_paths,
                current_evidence=current_evidence,
                prev_script=prev_script,
                scoring=scoring,
            )
            scored.append((score, script))

        if not scored:
            continue
        scored.sort(key=lambda item: (-item[0], item[1]["path"]))
        best_score, best_script = scored[0]
        if best_score <= 0:
            continue

        produced = [item for item in best_script["evidence_types_produced"] if item in missing_evidence or best_script["path"] == candidate_audit_script]
        if not produced and best_script["path"] != candidate_audit_script:
            continue

        sequence.append(
            {
                "step_id": f"s{len(sequence) + 1}",
                "name": _step_name(best_script["path"]),
                "script": best_script["path"],
                "lane": best_script["lane"],
                "why": (
                    f"Need {', '.join(produced)} before moving forward."
                    if produced
                    else f"Candidate-linked script {best_script['path']} provides high-signal validation."
                ),
                "expected_evidence": best_script["evidence_types_produced"],
            }
        )
        selected_paths.add(best_script["path"])
        current_evidence.update(best_script["evidence_types_produced"])
        missing_evidence.difference_update(best_script["evidence_types_produced"])
        if best_script["path"].startswith("scripts/audit_"):
            current_evidence.add("audit_report")
            missing_evidence.discard("audit_report")
        prev_script = best_script

        if len(sequence) >= max_steps:
            break

    mutation_candidates: list[str] = []
    if route["operation_mode"] == "mutate_dashboard":
        if effective_surface in {"crma_dashboard", "hybrid"}:
            mutation_candidates.append("scripts/wave_patch_executor.py")
        elif effective_surface == "salesforce_dashboard":
            mutation_candidates.append("scripts/salesforce_dashboard_executor.py")
        elif effective_surface == "salesforce_report":
            mutation_candidates.append("scripts/salesforce_report_executor.py")

    if candidate_audit_script and candidate_audit_script in selected_paths:
        planner_notes.append("Candidate-linked audit was included in the evidence plan.")
    if memory_hits:
        planner_notes.append(f"Planner considered {len(memory_hits)} prior memory hit(s).")
    if isinstance(memory_health, dict):
        excluded_hits = memory_health.get("policy_exception_hits_excluded")
        if isinstance(excluded_hits, int) and excluded_hits > 0:
            planner_notes.append(
                f"Planner ignored {excluded_hits} similar run(s) carrying policy exceptions."
            )
        fail_count = memory_health.get("included_fail_count")
        if isinstance(fail_count, int) and fail_count > 0:
            planner_notes.append(f"Planner saw {fail_count} prior failing hit(s) in active memory.")
        generic_count = memory_health.get("included_generic_goal_count")
        if isinstance(generic_count, int) and generic_count > 0:
            planner_notes.append(
                f"Active memory still contains {generic_count} generic-goal run(s); audit/quarantine them if reuse quality drops."
            )
    preferred_success_hit = memory_context.get("preferred_success_hit")
    if isinstance(preferred_success_hit, dict) and preferred_success_hit.get("run_id"):
        planner_notes.append(
            f"Planner reused success signals from prior run {preferred_success_hit['run_id']}."
        )
    repeated_failure_codes = memory_context.get("repeated_failure_codes", {})
    if repeated_failure_codes:
        repeated_codes: set[str] = set()
        for codes in repeated_failure_codes.values():
            for code in codes:
                repeated_codes.add(code)
        planner_notes.append(
            "Planner penalized repeated failure patterns: " + ", ".join(sorted(repeated_codes)) + "."
        )
    if not any(registry_by_path[item["script"]]["command_class"] == "mutating" for item in sequence):
        planner_notes.append("No mutating harness was placed before evidence collection.")

    safe_execution_supported = bool(sequence) and not missing_evidence and all(
        registry_by_path[item["script"]]["command_class"] != "mutating" for item in sequence
    )

    return {
        "run_id": run_id,
        "goal": query,
        "resolved": {
            "persona": resolution.get("resolved_persona"),
            "domain": resolution.get("resolved_domain"),
            "operation": route["operation_mode"],
        },
        "candidate_surface": candidate,
        "surface_advisory": surface_advisory,
        "recommended_sequence": sequence,
        "required_evidence": required_evidence,
        "missing_evidence": sorted(missing_evidence),
        "mutation_candidates": mutation_candidates,
        "stop_conditions": [
            "Missing required evidence after planned steps complete",
            "Unsupported lane transition",
            "Mutating harness proposed before evidence collection",
        ],
        "safe_execution_supported": safe_execution_supported,
        "planner_notes": planner_notes,
        "memory_summary": {
            "considered_hits": len(memory_hits or []),
            "memory_health": memory_health or {},
            "preferred_success_run_id": (
                preferred_success_hit.get("run_id")
                if isinstance(preferred_success_hit, dict)
                else None
            ),
            "repeated_failure_codes": repeated_failure_codes,
        },
        "route": {
            "recommended_surface_type": route["recommended_surface_type"],
            "effective_surface_type": effective_surface,
            "operation_mode": route["operation_mode"],
        },
    }


def run_plan(
    *,
    query: str,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
    memory_file: str | None = None,
    output_dir: str | None = None,
    run_id: str | None = None,
) -> tuple[dict[str, Any], int]:
    inputs = load_inputs()
    resolution = ai.resolve_request(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
    )
    route = ai.route_surface(inputs, resolution)
    surface_advisory = build_surface_advisory(
        inputs=inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        resolution=resolution,
        route=route,
    )
    planner_profile = inputs["planner_profiles"]["operation_defaults"].get(
        route["operation_mode"],
        inputs["planner_profiles"]["operation_defaults"]["review_dashboard"],
    )
    candidate = resolution["candidate_surfaces"][0] if resolution["candidate_surfaces"] else None
    planner_required_evidence = required_evidence_for_surface(
        planner_profile=planner_profile,
        surface_advisory=surface_advisory,
    )
    memory_hits = _load_memory_hits(
        query=query,
        persona=resolution.get("resolved_persona"),
        domain=resolution.get("resolved_domain"),
        operation=route["operation_mode"],
        tags=build_surface_memory_tags(
            route=route,
            candidate=candidate,
            surface_advisory=surface_advisory,
        ),
        evidence_types=planner_required_evidence,
        memory_file=Path(memory_file).resolve() if memory_file else None,
    )
    memory_health = run_memory.summarize_search_health(
        goal=query,
        persona=resolution.get("resolved_persona"),
        domain=resolution.get("resolved_domain"),
        operation=route["operation_mode"],
        tags=build_surface_memory_tags(
            route=route,
            candidate=candidate,
            surface_advisory=surface_advisory,
        ),
        evidence_types=planner_required_evidence,
    )
    plan = build_plan(
        inputs=inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        memory_hits=memory_hits,
        memory_health=memory_health,
        run_id=run_id,
        resolution=resolution,
        route=route,
        surface_advisory=surface_advisory,
    )

    artifacts: list[dict[str, str]] = []
    if output_dir:
        destination = Path(output_dir).resolve()
        destination.mkdir(parents=True, exist_ok=True)
        plan_path = destination / "plan.json"
        plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")
        artifacts.append({"type": "plan", "path": str(plan_path)})

    payload = make_result(
        status="ok" if plan["recommended_sequence"] else "warn",
        command="plan",
        messages=[
            ai.make_message(
                "info" if plan["recommended_sequence"] else "warn",
                "plan_ready" if plan["recommended_sequence"] else "plan_incomplete",
                (
                    f"Built a {len(plan['recommended_sequence'])}-step evidence-first harness plan."
                    if plan["recommended_sequence"]
                    else "No evidence-first harness plan could be built from the current route."
                ),
            )
        ],
        artifacts=artifacts,
        memory_hits=memory_hits,
        memory_health=memory_health,
        plan=plan,
    )
    return payload, 0 if payload["status"] != "error" else 1


def print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2))


def print_text(payload: dict[str, Any]) -> None:
    print(payload["messages"][0]["text"])
    for step in payload["plan"]["recommended_sequence"]:
        print(f"- {step['step_id']} {step['name']} :: {step['script']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="Build an evidence-first harness plan.")
    plan.add_argument("--query", required=True, help="Free-form business request.")
    plan.add_argument("--persona", default=None, help="Optional persona override.")
    plan.add_argument("--domain", default=None, help="Optional domain override.")
    plan.add_argument("--operation", default=None, help="Optional operation override.")
    plan.add_argument("--memory-file", default=None, help="Optional memory search artifact.")
    plan.add_argument("--output-dir", default=None, help="Optional output directory for plan.json.")
    plan.add_argument("--run-id", default=None, help="Optional stable run identifier.")
    plan.add_argument("--json", action="store_true", help="Print JSON output.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    payload, exit_code = run_plan(
        query=args.query,
        persona=args.persona,
        domain=args.domain,
        operation=args.operation,
        memory_file=args.memory_file,
        output_dir=args.output_dir,
        run_id=args.run_id,
    )
    if args.json:
        print_json(payload)
    else:
        print_text(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
