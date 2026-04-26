#!/usr/bin/env python3
"""CLI-first intelligence layer over the CRM Analytics harness surface."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
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
from builder_brain_handoff_targets import (
    DEFAULT_TARGET_ORG as DEFAULT_REPORT_TARGET_ORG,
    REGISTRY_PATH as HANDOFF_TARGET_REGISTRY_PATH,
    find_registry_target as find_handoff_registry_target,
    load_registry as load_handoff_target_registry,
)

PROFILES_PATH = ROOT / "config" / "analytics_intelligence_profiles.json"
REGISTRY_PATH = ROOT / "config" / "harness_registry.json"
WIDGET_PROFILES_PATH = ROOT / "config" / "widget_decision_profiles.json"
CONTEXT_REGISTRY_PATH = ROOT / "config" / "context_registry.json"
COMMERCIAL_MODEL_PATH = ROOT / "config" / "commercial_operating_model.json"
SALES_PROCESS_PATH = ROOT / "config" / "sales_process_codification.json"
SURFACE_QUEUE_PATH = ROOT / "config" / "dashboard_autopilot_queue.json"
HARNESS_PLANNER_PROFILES_PATH = ROOT / "config" / "harness_planner_profiles.json"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_inputs() -> dict[str, Any]:
    import harness_registry as registry_tools

    return {
        "profiles": load_json(PROFILES_PATH),
        "registry": registry_tools.load_registry(),
        "widget_profiles": load_json(WIDGET_PROFILES_PATH),
        "context_registry": load_json(CONTEXT_REGISTRY_PATH),
        "commercial_model": load_json(COMMERCIAL_MODEL_PATH),
        "sales_process": load_json(SALES_PROCESS_PATH),
        "surface_queue": load_json(SURFACE_QUEUE_PATH),
        "planner_profiles": load_json(HARNESS_PLANNER_PROFILES_PATH),
    }


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def tokenize(value: str) -> list[str]:
    return [token for token in normalize_text(value).split(" ") if token]


def make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


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
        "tool": "analytics_intelligence",
        "lane": "intelligence_control",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return payload


def alias_score(query_text: str, query_tokens: set[str], aliases: list[str]) -> int:
    score = 0
    for alias in aliases:
        alias_text = normalize_text(alias)
        if not alias_text:
            continue
        alias_tokens = set(alias_text.split())
        if alias_text in query_text:
            score += len(alias_tokens) * 3
        else:
            score += len(query_tokens & alias_tokens)
    return score


def choose_alias(
    query_text: str,
    query_tokens: set[str],
    candidates: dict[str, list[str]],
    explicit_value: str | None = None,
) -> tuple[str | None, dict[str, int]]:
    scores: dict[str, int] = {}
    if explicit_value:
        normalized = normalize_text(explicit_value).replace(" ", "_")
        for key in candidates:
            if normalized == key or explicit_value.lower() == key:
                scores[key] = 999
                return key, scores
    for key, aliases in candidates.items():
        scores[key] = alias_score(query_text, query_tokens, aliases + [key.replace("_", " ")])
    best_key = None
    best_score = 0
    for key, score in scores.items():
        if score > best_score:
            best_key = key
            best_score = score
    return (best_key if best_score > 0 else None), scores


def build_surface_catalog(inputs: dict[str, Any]) -> list[dict[str, Any]]:
    context_dashboards = inputs["context_registry"].get("dashboards", [])
    live_url_by_name = {
        normalize_text(item["name"]): item.get("live_url")
        for item in context_dashboards
    }
    status_by_name = {
        normalize_text(item["name"]): item.get("status")
        for item in context_dashboards
    }

    catalog: list[dict[str, Any]] = []
    for item in inputs["surface_queue"].get("items", []):
        labels = item.get("dashboard_labels", [])
        live_urls = []
        statuses = []
        for label in labels:
            key = normalize_text(label)
            if live_url_by_name.get(key):
                live_urls.append(live_url_by_name[key])
            if status_by_name.get(key):
                statuses.append(status_by_name[key])
        catalog.append(
            {
                "id": item["key"],
                "labels": labels,
                "personas": [normalize_text(value) for value in item.get("personas", [])],
                "domains": item.get("domains", []),
                "kpi_focus": item.get("kpi_focus", []),
                "notes": item.get("notes", ""),
                "audit_script": item.get("audit_script"),
                "live_urls": live_urls,
                "statuses": statuses,
            }
        )
    return catalog


def resolve_question(
    domain: str | None,
    query_text: str,
    query_tokens: set[str],
    widget_profiles: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, int]]:
    if not domain or domain not in widget_profiles.get("domains", {}):
        return None, {}
    questions = widget_profiles["domains"][domain].get("common_questions", {})
    scores: dict[str, int] = {}
    best_id = None
    best_score = 0
    for question_id, config in questions.items():
        aliases = [
            question_id.replace("_", " "),
            config.get("recommended", ""),
            *config.get("alternatives", []),
        ]
        score = alias_score(query_text, query_tokens, aliases)
        scores[question_id] = score
        if score > best_score:
            best_id = question_id
            best_score = score
    if not best_id or best_score == 0:
        return None, scores
    question = questions[best_id]
    return (
        {
            "id": best_id,
            "recommended_widget": question.get("recommended"),
            "alternatives": question.get("alternatives", []),
            "avoid": question.get("avoid", []),
        },
        scores,
    )


def infer_cadence(persona: str | None, inputs: dict[str, Any]) -> str | None:
    if not persona:
        return None
    cadence_map = {
        "executive": "weekly_to_monthly",
        "manager": "weekly",
        "individual": "daily",
        "analyst": "ad_hoc",
    }
    if persona in cadence_map:
        return cadence_map[persona]
    return None


def resolve_motion(
    query_text: str,
    query_tokens: set[str],
    profiles: dict[str, Any],
) -> tuple[str | None, dict[str, int]]:
    return choose_alias(query_text, query_tokens, profiles["motion_aliases"])


def score_surface_candidate(
    candidate: dict[str, Any],
    query_text: str,
    query_tokens: set[str],
    resolved_persona: str | None,
    resolved_domain: str | None,
) -> int:
    chunks = [
        candidate["id"].replace("_", " "),
        *candidate.get("labels", []),
        *candidate.get("domains", []),
        *candidate.get("kpi_focus", []),
        candidate.get("notes", ""),
    ]
    score = alias_score(query_text, query_tokens, chunks)
    if resolved_persona and resolved_persona in candidate.get("personas", []):
        score += 4
    if resolved_domain:
        lowered_domains = normalize_text(" ".join(candidate.get("domains", [])))
        if resolved_domain == "revenue" and any(
            token in lowered_domains for token in ("forecast", "revenue", "opportunity")
        ):
            score += 3
        elif resolved_domain == "demand" and any(
            token in lowered_domains for token in ("lead", "bdr", "demand")
        ):
            score += 3
        elif resolved_domain == "customer" and any(
            token in lowered_domains for token in ("account", "customer", "contact")
        ):
            score += 3
        elif resolved_domain == "retention" and any(
            token in lowered_domains for token in ("renewal", "retention", "churn")
        ):
            score += 3
        elif resolved_domain == "product_gtm" and any(
            token in lowered_domains for token in ("product", "pricing")
        ):
            score += 3
    return score


def resolve_request(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
) -> dict[str, Any]:
    profiles = inputs["profiles"]
    query_text = normalize_text(query)
    query_tokens = set(tokenize(query))

    resolved_persona, persona_scores = choose_alias(
        query_text,
        query_tokens,
        profiles["persona_aliases"],
        explicit_value=persona,
    )
    resolved_domain, domain_scores = choose_alias(
        query_text,
        query_tokens,
        profiles["domain_aliases"],
        explicit_value=domain,
    )
    resolved_operation, operation_scores = choose_alias(
        query_text,
        query_tokens,
        {key: value["aliases"] for key, value in profiles["operation_modes"].items()},
        explicit_value=operation,
    )
    resolved_motion, motion_scores = resolve_motion(query_text, query_tokens, profiles)
    question, question_scores = resolve_question(
        resolved_domain,
        query_text,
        query_tokens,
        inputs["widget_profiles"],
    )

    candidates = []
    for candidate in build_surface_catalog(inputs):
        score = score_surface_candidate(
            candidate,
            query_text,
            query_tokens,
            resolved_persona,
            resolved_domain,
        )
        if score > 0:
            enriched = dict(candidate)
            enriched["score"] = score
            candidates.append(enriched)
    candidates.sort(key=lambda item: (-item["score"], item["id"]))

    return {
        "query": query,
        "resolved_persona": resolved_persona,
        "resolved_domain": resolved_domain,
        "resolved_operation": resolved_operation,
        "resolved_motion": resolved_motion,
        "resolved_cadence": infer_cadence(resolved_persona, inputs),
        "question": question,
        "candidate_surfaces": candidates[:5],
        "scores": {
            "persona": persona_scores,
            "domain": domain_scores,
            "operation": operation_scores,
            "motion": motion_scores,
            "question": question_scores,
        },
    }


def _rank_script_for_request(
    script: dict[str, Any],
    *,
    resolved: dict[str, Any],
    candidate: dict[str, Any] | None,
) -> int:
    query_text = normalize_text(resolved["query"])
    query_tokens = set(tokenize(resolved["query"]))
    script_text = normalize_text(
        " ".join(
            [
                script["path"].replace("/", " ").replace("_", " "),
                script.get("summary", ""),
                candidate.get("id", "") if candidate else "",
                " ".join(candidate.get("labels", [])) if candidate else "",
            ]
        )
    )
    score = alias_score(
        query_text,
        query_tokens,
        [
            script["path"].replace("/", " ").replace("_", " "),
            script.get("summary", ""),
        ],
    )

    domain_hints = {
        "revenue": {"forecast", "revenue", "renewal", "retention", "role", "ownership", "closed"},
        "demand": {"bdr", "lead", "campaign", "quote", "response"},
        "customer": {"account", "customer", "contact", "coverage"},
        "retention": {"renewal", "retention", "owner", "churn"},
        "product_gtm": {"product", "quote", "industry", "segment"},
    }
    for token in domain_hints.get(resolved.get("resolved_domain"), set()):
        if token in script_text:
            score += 4

    if resolved.get("resolved_operation") == "validate_truth":
        for token in ("source", "integrity", "semantic", "ownership", "coverage", "reconcile"):
            if token in query_text and token in script_text:
                score += 1
    if resolved.get("resolved_operation") == "understand_metric" and "profile" in script_text:
        score += 1
    return score


def _preferred_lane_scripts(
    *,
    lane: str,
    profiles: dict[str, Any],
    registry_scripts: dict[str, dict[str, Any]],
    resolved: dict[str, Any],
    candidate: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    preferred_scripts: list[dict[str, Any]] = []
    seen_paths: set[str] = set()

    candidate_path = None
    if candidate:
        raw_candidate_path = candidate.get("audit_script")
        if (
            isinstance(raw_candidate_path, str)
            and raw_candidate_path in registry_scripts
            and registry_scripts[raw_candidate_path]["lane"] == lane
        ):
            candidate_path = raw_candidate_path

    for path in [candidate_path, *profiles["lane_entrypoints"].get(lane, [])]:
        if not path or path in seen_paths:
            continue
        script = registry_scripts.get(path)
        if not script:
            continue
        preferred_scripts.append(
            {
                **script,
                "route_score": _rank_script_for_request(script, resolved=resolved, candidate=candidate),
            }
        )
        seen_paths.add(path)

    if not preferred_scripts:
        return preferred_scripts

    anchored: list[dict[str, Any]] = []
    tail = preferred_scripts
    if candidate_path and preferred_scripts[0]["path"] == candidate_path:
        anchored = [preferred_scripts[0]]
        tail = preferred_scripts[1:]

    tail.sort(
        key=lambda item: (
            -int(item.get("route_score", 0)),
            item["path"],
        )
    )
    return anchored + tail


def route_surface(inputs: dict[str, Any], resolved: dict[str, Any]) -> dict[str, Any]:
    profiles = inputs["profiles"]
    query_text = normalize_text(resolved["query"])
    query_tokens = set(tokenize(resolved["query"]))
    scores: dict[str, int] = {}
    resolved_question = resolved.get("question")
    question_id = resolved_question.get("id") if isinstance(resolved_question, dict) else None

    for surface_type, config in profiles["surface_types"].items():
        score = 0
        for rule in profiles["routing_rules"]:
            if rule["surface_type"] != surface_type:
                continue
            score += alias_score(query_text, query_tokens, rule.get("query_tokens", []))
            if resolved.get("resolved_persona") in rule.get("persona_bias", []):
                score += rule["score"]
            if question_id in rule.get("question_bias", []):
                score += rule["score"]
        if surface_type == "crma_dashboard" and resolved.get("resolved_domain") in {
            "revenue",
            "demand",
            "customer",
            "retention",
            "product_gtm",
        }:
            score += 1
        scores[surface_type] = score

    if resolved.get("resolved_operation") == "mutate_dashboard":
        scores["crma_dashboard"] = scores.get("crma_dashboard", 0) + 5
    if resolved.get("resolved_operation") == "export_assets":
        scores["crma_dashboard"] = scores.get("crma_dashboard", 0) + 2

    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    selected_surface = ranked[0][0] if ranked and ranked[0][1] > 0 else "crma_dashboard"
    selected_config = profiles["surface_types"][selected_surface]

    operation_key = resolved.get("resolved_operation") or "review_dashboard"
    if operation_key not in profiles["operation_modes"]:
        operation_key = "review_dashboard"
    operation_config = profiles["operation_modes"][operation_key]

    registry_scripts = {item["path"]: item for item in inputs["registry"]["scripts"]}
    candidate = resolved["candidate_surfaces"][0] if resolved.get("candidate_surfaces") else None
    script_suggestions: list[dict[str, Any]] = []
    for lane in operation_config["lane_sequence"]:
        preferred_scripts = _preferred_lane_scripts(
            lane=lane,
            profiles=profiles,
            registry_scripts=registry_scripts,
            resolved=resolved,
            candidate=candidate,
        )
        script_suggestions.append({"lane": lane, "scripts": preferred_scripts})

    review_gate_ids = []
    for gate_id in selected_config.get("review_gates", []):
        if gate_id not in review_gate_ids:
            review_gate_ids.append(gate_id)
    for gate_id in operation_config.get("review_gates", []):
        if gate_id not in review_gate_ids:
            review_gate_ids.append(gate_id)

    return {
        "recommended_surface_type": selected_surface,
        "surface_profile": selected_config,
        "surface_scores": scores,
        "operation_mode": operation_key,
        "operation_profile": operation_config,
        "lane_sequence": operation_config["lane_sequence"],
        "script_suggestions": script_suggestions,
        "review_gate_ids": review_gate_ids,
    }


def build_review_plan(
    inputs: dict[str, Any],
    *,
    script_path: str | None = None,
    resolved: dict[str, Any] | None = None,
    route: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profiles = inputs["profiles"]
    registry_scripts = {item["path"]: item for item in inputs["registry"]["scripts"]}

    gates: list[str] = []
    context_payload: dict[str, Any] = {}

    if script_path:
        script = registry_scripts.get(script_path)
        if not script:
            return {
                "status": "error",
                "messages": [
                    make_message("error", "unknown_script", f"Unknown registered script: {script_path}.")
                ],
            }
        context_payload["script"] = script
        for gate_id in profiles["lane_review_profiles"].get(script["lane"], []):
            if gate_id not in gates:
                gates.append(gate_id)
        if script["command_class"] == "mutating":
            for gate_id in ("execution_boundary", "pre_export_snapshot", "post_mutation_reexport"):
                if gate_id not in gates:
                    gates.append(gate_id)
    else:
        if route:
            for gate_id in route["review_gate_ids"]:
                if gate_id not in gates:
                    gates.append(gate_id)
        if resolved and resolved.get("question"):
            recommended_widget = resolved["question"].get("recommended_widget")
            if recommended_widget and "visual_fit" not in gates:
                gates.append("visual_fit")

    gate_payload = []
    for gate_id in gates:
        config = profiles["review_gates"][gate_id]
        gate_payload.append(
            {
                "id": gate_id,
                "label": config["label"],
                "description": config["description"],
                "refs": config["refs"],
            }
        )
    return {"status": "ok", "gates": gate_payload, **context_payload}


def print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2))


def print_inventory_text(payload: dict[str, Any]) -> None:
    print("Analytics Intelligence")
    print("Surface Types")
    for key, value in payload["surface_types"].items():
        print(f"- {key}: support={value['support_level']} best_for={', '.join(value['best_for'][:2])}")
    print("")
    print("Operation Modes")
    for key, value in payload["operation_modes"].items():
        print(f"- {key}: lanes={', '.join(value['lane_sequence'])}")


def print_resolve_text(payload: dict[str, Any]) -> None:
    resolution = payload["resolution"]
    print(f"persona: {resolution.get('resolved_persona') or 'unresolved'}")
    print(f"domain: {resolution.get('resolved_domain') or 'unresolved'}")
    print(f"operation: {resolution.get('resolved_operation') or 'unresolved'}")
    print(f"motion: {resolution.get('resolved_motion') or 'unresolved'}")
    print(f"cadence: {resolution.get('resolved_cadence') or 'unresolved'}")
    if resolution.get("question"):
        question = resolution["question"]
        print(
            f"question: {question['id']} -> recommended_widget={question['recommended_widget']}"
        )
    if resolution["candidate_surfaces"]:
        print("")
        print("candidate surfaces")
        for candidate in resolution["candidate_surfaces"]:
            labels = ", ".join(candidate["labels"])
            print(f"- {candidate['id']} ({labels}) score={candidate['score']}")


def print_route_text(payload: dict[str, Any]) -> None:
    route = payload["route"]
    print(f"surface: {route['recommended_surface_type']}")
    print(f"support: {route['surface_profile']['support_level']}")
    print(f"operation: {route['operation_mode']}")
    print("")
    print("lane sequence")
    for lane in route["lane_sequence"]:
        print(f"- {lane}")
    print("")
    print("script suggestions")
    for item in route["script_suggestions"]:
        scripts = ", ".join(script["path"] for script in item["scripts"]) or "(none)"
        print(f"- {item['lane']}: {scripts}")


def print_review_text(payload: dict[str, Any]) -> None:
    if payload["status"] != "ok":
        print(payload["messages"][0]["text"], file=sys.stderr)
        return
    if payload.get("script"):
        print(f"script: {payload['script']['path']}")
        print(f"lane: {payload['script']['lane']}")
        print(f"class: {payload['script']['command_class']}")
        print("")
    print("review gates")
    for gate in payload["gates"]:
        print(f"- {gate['id']}: {gate['description']}")


def print_execute_text(payload: dict[str, Any]) -> None:
    execution = payload["execution"]
    print(f"script: {execution['script']}")
    print(f"lane: {execution['lane']}")
    print(f"class: {execution['command_class']}")
    print(f"risk: {execution['risk']}")
    print(f"returncode: {execution['returncode']}")
    if execution.get("stdout_excerpt"):
        print("")
        print("stdout")
        print(execution["stdout_excerpt"])
    if execution.get("stderr_excerpt"):
        print("")
        print("stderr")
        print(execution["stderr_excerpt"])


def print_workflow_text(payload: dict[str, Any]) -> None:
    workflow = payload["workflow"]
    print(f"mode: {workflow['mode']}")
    print(f"surface: {workflow['route']['recommended_surface_type']}")
    summary = workflow.get("summary", {})
    if summary.get("effective_surface_type") and summary["effective_surface_type"] != workflow["route"]["recommended_surface_type"]:
        print(f"effective_surface: {summary['effective_surface_type']}")
    candidate = workflow.get("candidate_surface")
    if candidate:
        print(f"candidate: {candidate['id']}")
    if summary.get("surface_selection_reason"):
        print(f"surface_selection_reason: {summary['surface_selection_reason']}")
    if summary.get("secondary_surface_type"):
        print(f"secondary_surface: {summary['secondary_surface_type']}")
    if summary.get("report_surface_verdict"):
        print(f"report_surface_verdict: {summary['report_surface_verdict']}")
    planned_report_filter_overrides = summary.get("planned_report_filter_overrides") or []
    if planned_report_filter_overrides:
        rendered_overrides = ", ".join(
            f"{item['source_label']}={item['value']}"
            for item in planned_report_filter_overrides
            if isinstance(item, dict)
            and isinstance(item.get("source_label"), str)
            and isinstance(item.get("value"), str)
        )
        if rendered_overrides:
            print(f"planned_report_filter_overrides: {rendered_overrides}")
    if summary.get("resolved_report_filter_override_count") is not None:
        print(
            "resolved_report_filter_override_count: "
            f"{summary['resolved_report_filter_override_count']}"
        )
    if summary.get("manual_report_filter_intent_count") is not None:
        print(
            "manual_report_filter_intent_count: "
            f"{summary['manual_report_filter_intent_count']}"
        )
    if summary.get("report_apply_strategy") is not None:
        print(f"report_apply_strategy: {summary['report_apply_strategy']}")
    if summary.get("report_apply_ready") is not None:
        print(f"report_apply_ready: {summary['report_apply_ready']}")
    if summary.get("report_native_authoring_ready") is not None:
        print(f"report_native_authoring_ready: {summary['report_native_authoring_ready']}")
    if summary.get("report_external_fill_requirement_count") is not None:
        print(
            "report_external_fill_requirement_count: "
            f"{summary['report_external_fill_requirement_count']}"
        )
    if summary.get("report_manual_detail_intent_count") is not None:
        print(
            "report_manual_detail_intent_count: "
            f"{summary['report_manual_detail_intent_count']}"
        )
    if summary.get("report_omitted_sort_intent_count") is not None:
        print(
            "report_omitted_sort_intent_count: "
            f"{summary['report_omitted_sort_intent_count']}"
        )
    if summary.get("workflow_status"):
        print(f"workflow_status: {summary['workflow_status']}")
    memory_health = workflow.get("memory_health") or {}
    excluded_hits = memory_health.get("policy_exception_hits_excluded")
    if isinstance(excluded_hits, int) and excluded_hits > 0:
        print(f"excluded_policy_exception_hits: {excluded_hits}")
    if summary.get("browser_landing_artifact"):
        print(f"ai_os_browser_landing_artifact: {summary['browser_landing_artifact']}")
    if summary.get("browser_index_artifact"):
        print(f"ai_os_browser_index_artifact: {summary['browser_index_artifact']}")
    if summary.get("browser_health_landing_artifact"):
        print(f"ai_os_health_landing_artifact: {summary['browser_health_landing_artifact']}")
    if summary.get("browser_health_index_artifact"):
        print(f"ai_os_health_index_artifact: {summary['browser_health_index_artifact']}")
    browser_health_summary = summary.get("browser_health_summary") or {}
    if isinstance(browser_health_summary, dict) and browser_health_summary:
        print(f"ai_os_risk_run_count: {browser_health_summary.get('risk_run_count', 0)}")
        print(f"ai_os_attention_run_count: {browser_health_summary.get('attention_run_count', 0)}")
        print(f"ai_os_evaluation_bypass_count: {browser_health_summary.get('evaluation_bypass_count', 0)}")
        print(f"ai_os_stale_collection_count: {browser_health_summary.get('stale_collection_count', 0)}")
    if summary.get("collection_landing_artifact"):
        print(f"workflow_collection_landing_artifact: {summary['collection_landing_artifact']}")
    if summary.get("collection_index_artifact"):
        print(f"workflow_collection_index_artifact: {summary['collection_index_artifact']}")
    if summary.get("review_artifact"):
        print(f"workflow_review_artifact: {summary['review_artifact']}")
    print("")
    print("steps")
    for step in workflow["steps"]:
        status = step.get("status", "planned")
        print(f"- {step['name']}: {status}")
        if step.get("script"):
            print(f"  script: {step['script']}")


def print_validate_text(payload: dict[str, Any]) -> None:
    print(
        "analytics_intelligence:"
        f" status={payload['status']}"
        f" files_checked={payload['coverage']['files_checked']}"
        f" gates={payload['coverage']['review_gates']}"
    )
    for message in payload["messages"]:
        print(f"{message['level'].upper()} {message['code']}: {message['text']}")


def build_inventory(inputs: dict[str, Any]) -> dict[str, Any]:
    return make_result(
        status="ok",
        command="inventory",
        messages=[
            make_message("info", "inventory_ready", "Loaded intelligence surface types and operation modes.")
        ],
        surface_types=inputs["profiles"]["surface_types"],
        operation_modes=inputs["profiles"]["operation_modes"],
        agent_graph=inputs["profiles"]["agent_graph"],
    )


def _supports_json_output(script_entry: dict[str, Any]) -> bool:
    return bool(script_entry.get("supports_json_output", False))


def _has_json_flag(script_args: list[str]) -> bool:
    return "--json" in script_args


def _validate_child_result_envelope(
    registry: dict[str, Any],
    payload: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    required_keys = registry["result_envelope"]["required_keys"]
    for key in required_keys:
        if key not in payload:
            errors.append(f"missing required result key: {key}")
    if payload.get("status") not in registry["result_envelope"]["status_values"]:
        errors.append(f"invalid child status: {payload.get('status')}")
    for message in payload.get("messages", []):
        if message.get("level") not in registry["result_envelope"]["message_levels"]:
            errors.append(f"invalid child message level: {message.get('level')}")
    return errors


def _combine_status(*statuses: str) -> str:
    if any(status == "error" for status in statuses):
        return "error"
    if any(status == "warn" for status in statuses):
        return "warn"
    return "ok"


def _extract_child_output(step: dict[str, Any]) -> dict[str, Any] | None:
    result = step.get("result")
    if not isinstance(result, dict):
        return None
    execution = result.get("execution")
    if not isinstance(execution, dict):
        return None
    child = execution.get("structured_output")
    return child if isinstance(child, dict) else None


def _child_lane(step: dict[str, Any]) -> str | None:
    child = _extract_child_output(step)
    if child and isinstance(child.get("lane"), str):
        return child["lane"]
    result = step.get("result")
    if isinstance(result, dict):
        execution = result.get("execution")
        if isinstance(execution, dict) and isinstance(execution.get("lane"), str):
            return execution["lane"]
    return step.get("lane") if isinstance(step.get("lane"), str) else None


def _collect_child_artifacts(steps: list[dict[str, Any]]) -> list[dict[str, str]]:
    artifacts: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for step in steps:
        child = _extract_child_output(step)
        if not child:
            continue
        for artifact in child.get("artifacts", []):
            kind = artifact.get("kind")
            path = artifact.get("path")
            if not isinstance(kind, str) or not isinstance(path, str):
                continue
            key = (kind, path)
            if key in seen:
                continue
            seen.add(key)
            artifacts.append({"kind": kind, "path": path})
    return artifacts


def _parse_filter_override_arg(raw_arg: Any) -> dict[str, str] | None:
    if not isinstance(raw_arg, str) or "=" not in raw_arg:
        return None
    source_label, value = raw_arg.split("=", 1)
    source_label = source_label.strip()
    value = value.strip()
    if not source_label or not value:
        return None
    return {
        "source_label": source_label,
        "value": value,
    }


def _planned_report_filter_overrides(steps: list[dict[str, Any]]) -> list[dict[str, str]]:
    overrides: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for step in steps:
        if step.get("script") != "scripts/salesforce_report_executor.py":
            continue
        args = step.get("args")
        if not isinstance(args, list):
            continue
        for index, arg in enumerate(args):
            if arg != "--filter-override" or index + 1 >= len(args):
                continue
            parsed = _parse_filter_override_arg(args[index + 1])
            if parsed and (parsed["source_label"], parsed["value"]) not in seen:
                seen.add((parsed["source_label"], parsed["value"]))
                overrides.append(parsed)
    return overrides


def _default_report_target_org() -> str | None:
    override = os.environ.get("CRM_AI_DEFAULT_TARGET_ORG")
    if isinstance(override, str) and override.strip():
        return override.strip()
    return DEFAULT_REPORT_TARGET_ORG


def _default_report_clone_from_id(candidate: dict[str, Any] | None) -> str | None:
    if not isinstance(candidate, dict):
        return None
    source_surface_id = candidate.get("id")
    if not isinstance(source_surface_id, str) or not source_surface_id:
        return None
    try:
        registry = load_handoff_target_registry(HANDOFF_TARGET_REGISTRY_PATH)
    except Exception:
        return None
    target = find_handoff_registry_target(
        registry,
        source_surface_id=source_surface_id,
        target_surface_type="salesforce_report",
    )
    if not isinstance(target, dict):
        return None
    clone_from_report_id = target.get("target_surface_id")
    if isinstance(clone_from_report_id, str) and clone_from_report_id:
        return clone_from_report_id
    return None


def _workflow_step_summary(step: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "name": step["name"],
        "status": step["status"],
    }
    if step.get("script"):
        summary["script"] = step["script"]
    lane = _child_lane(step)
    if lane:
        summary["lane"] = lane
    child = _extract_child_output(step)
    if not child:
        if step.get("args"):
            summary["args"] = step["args"]
        return summary

    child_summary = child.get("summary", {})
    summary["tool"] = child.get("tool")
    summary["messages"] = [message.get("code") for message in child.get("messages", [])]
    if lane == "salesforce_data_profiles":
        summary["profile"] = child_summary
    elif lane == "wave_data_validations":
        summary["validation"] = child_summary
    elif step["name"] == "export_live_assets":
        summary["export"] = {
            "dashboards_requested": child_summary.get("dashboards_requested"),
            "dashboards_exported": child_summary.get("dashboards_exported"),
            "dashboard_errors": child_summary.get("dashboard_errors"),
            "dataset_warning_count": child_summary.get("dataset_warning_count"),
            "output_dir": child_summary.get("output_dir"),
        }
    elif step["name"] == "contract_lint":
        summary["lint"] = {
            "files_checked": child_summary.get("files_checked"),
            "files_with_violations": child_summary.get("files_with_violations"),
            "total_violations": child_summary.get("total_violations"),
            "file_errors": child_summary.get("file_errors"),
            "normalized": child_summary.get("normalized"),
        }
    elif step["name"] == "audit_surface":
        summary["audit"] = {
            "dashboard": child_summary.get("dashboard"),
            "pass_count": child_summary.get("pass_count"),
            "fail_count": child_summary.get("fail_count"),
            "widget_count": child_summary.get("widget_count"),
            "step_count": child_summary.get("step_count"),
            "chrome_ratio": child_summary.get("chrome_ratio"),
            "output_dir": child_summary.get("output_dir"),
        }
    elif step["name"] == "build_report_handoff":
        executor_handoff = child.get("executor_handoff", {})
        revised_spec = child.get("revised_spec", {})
        summary["report_handoff"] = {
            "primary_lane": executor_handoff.get("primary_lane"),
            "package_artifact": executor_handoff.get("package_artifact"),
            "primary_surface": revised_spec.get("primary_surface"),
            "secondary_surface": revised_spec.get("secondary_surface"),
        }
    elif step["name"] == "preview_report_rest":
        summary["report_preview"] = {
            "request_count": child_summary.get("request_count"),
            "fill_requirement_count": child_summary.get("fill_requirement_count"),
            "action_surface_verdict": child_summary.get("action_surface_verdict"),
            "manual_authoring_pressure_score": child_summary.get("manual_authoring_pressure_score"),
            "resolved_filter_override_count": child_summary.get("resolved_filter_override_count"),
            "manual_filter_intent_count": child_summary.get("manual_filter_intent_count"),
        }
    elif step["name"] == "apply_report_rest_dry_run":
        apply_summary = child.get("apply_summary", {})
        summary["report_apply"] = {
            "strategy": apply_summary.get("strategy"),
            "request_count": apply_summary.get("request_count"),
            "fill_requirement_count": apply_summary.get("fill_requirement_count"),
            "external_fill_requirement_count": apply_summary.get("external_fill_requirement_count"),
            "internal_fill_requirement_count": apply_summary.get("internal_fill_requirement_count"),
            "resolved_filter_override_count": apply_summary.get("resolved_filter_override_count"),
            "manual_filter_intent_count": apply_summary.get("manual_filter_intent_count"),
            "manual_detail_intent_count": apply_summary.get("manual_detail_intent_count"),
            "omitted_sort_intent_count": apply_summary.get("omitted_sort_intent_count"),
            "native_authoring_support": apply_summary.get("native_authoring_support"),
            "native_authoring_ready": apply_summary.get("native_authoring_ready"),
            "apply_ready": apply_summary.get("apply_ready"),
        }
    else:
        summary["child_summary"] = child_summary
    return summary


def _build_workflow_summary(
    *,
    route: dict[str, Any],
    review: dict[str, Any],
    candidate: dict[str, Any] | None,
    steps: list[dict[str, Any]],
    supported_execution: bool,
    effective_surface_type: str | None = None,
    surface_advisory: dict[str, Any] | None = None,
    workflow_status: str | None = None,
    planner_notes: list[str] | None = None,
    evaluation_verdict: str | None = None,
    memory_health: dict[str, Any] | None = None,
) -> dict[str, Any]:
    executed_steps = [
        _workflow_step_summary(step)
        for step in steps
        if "result" in step
    ]
    report_preview_summary = next(
        (
            step.get("report_preview")
            for step in executed_steps
            if step.get("name") == "preview_report_rest" and isinstance(step.get("report_preview"), dict)
        ),
        None,
    )
    report_apply_summary = next(
        (
            step.get("report_apply")
            for step in executed_steps
            if step.get("name") == "apply_report_rest_dry_run" and isinstance(step.get("report_apply"), dict)
        ),
        None,
    )
    return {
        "recommended_surface_type": route["recommended_surface_type"],
        "effective_surface_type": effective_surface_type or route["recommended_surface_type"],
        "surface_selection_reason": (
            surface_advisory.get("selection_reason")
            if isinstance(surface_advisory, dict)
            else None
        ),
        "secondary_surface_type": (
            surface_advisory.get("secondary_surface")
            if isinstance(surface_advisory, dict)
            else None
        ),
        "report_surface_verdict": (
            (surface_advisory.get("report_action_surface_assessment") or {}).get("verdict")
            if isinstance(surface_advisory, dict)
            else None
        ),
        "candidate_surface_id": candidate["id"] if candidate else None,
        "safe_execution_supported": supported_execution,
        "review_gate_ids": [gate["id"] for gate in review["gates"]],
        "planned_steps": [step["name"] for step in steps],
        "planned_scripts": [step["script"] for step in steps if step.get("script")],
        "planned_report_filter_overrides": _planned_report_filter_overrides(steps),
        "executed_steps": executed_steps,
        "executed_profile_steps": [
            step for step in executed_steps if step.get("lane") == "salesforce_data_profiles"
        ],
        "executed_validation_steps": [
            step for step in executed_steps if step.get("lane") == "wave_data_validations"
        ],
        "resolved_report_filter_override_count": (
            report_preview_summary.get("resolved_filter_override_count")
            if isinstance(report_preview_summary, dict)
            else None
        ),
        "manual_report_filter_intent_count": (
            report_preview_summary.get("manual_filter_intent_count")
            if isinstance(report_preview_summary, dict)
            else None
        ),
        "report_apply_strategy": (
            report_apply_summary.get("strategy")
            if isinstance(report_apply_summary, dict)
            else None
        ),
        "report_apply_ready": (
            report_apply_summary.get("apply_ready")
            if isinstance(report_apply_summary, dict)
            else None
        ),
        "report_native_authoring_ready": (
            report_apply_summary.get("native_authoring_ready")
            if isinstance(report_apply_summary, dict)
            else None
        ),
        "report_external_fill_requirement_count": (
            report_apply_summary.get("external_fill_requirement_count")
            if isinstance(report_apply_summary, dict)
            else None
        ),
        "report_manual_detail_intent_count": (
            report_apply_summary.get("manual_detail_intent_count")
            if isinstance(report_apply_summary, dict)
            else None
        ),
        "report_omitted_sort_intent_count": (
            report_apply_summary.get("omitted_sort_intent_count")
            if isinstance(report_apply_summary, dict)
            else None
        ),
        "workflow_status": workflow_status,
        "planner_notes": planner_notes or [],
        "evaluation_verdict": evaluation_verdict,
        "memory_health": memory_health or {},
    }


def _supports_safe_script(script_entry: dict[str, Any]) -> bool:
    return (
        script_entry.get("supports_json_output") is True
        and script_entry.get("command_class") in {"read_only", "live_read"}
        and script_entry.get("risk") != "destructive"
    )


def _output_dir_flag(script_path: str) -> str | None:
    script_text = (ROOT / script_path).read_text(encoding="utf-8")
    if "--output-dir" in script_text:
        return "--output-dir"
    if "--out-dir" in script_text:
        return "--out-dir"
    return None


def _requires_live_export_dir(script_path: str) -> bool:
    return "--live-export-dir" in (ROOT / script_path).read_text(encoding="utf-8")


def _select_lane_scripts(
    route: dict[str, Any],
    *,
    lane: str,
    limit: int,
    exclude_paths: set[str] | None = None,
) -> list[dict[str, Any]]:
    exclude_paths = exclude_paths or set()
    minimum_score_by_lane = {
        "salesforce_data_profiles": 1,
        "wave_data_validations": 2,
    }
    minimum_score = minimum_score_by_lane.get(lane, 0)
    lane_scripts = next(
        (item["scripts"] for item in route["script_suggestions"] if item["lane"] == lane),
        [],
    )
    selected: list[dict[str, Any]] = []
    for script in lane_scripts:
        if script["path"] in exclude_paths:
            continue
        if not _supports_safe_script(script):
            continue
        if int(script.get("route_score", 0)) < minimum_score:
            continue
        selected.append(script)
        if len(selected) >= limit:
            break
    return selected


def _build_planned_lane_step(
    *,
    script_entry: dict[str, Any],
    output_dir: Path,
    live_export_dir: Path | None = None,
) -> dict[str, Any] | None:
    args: list[str] = []
    if _requires_live_export_dir(script_entry["path"]):
        if live_export_dir is None:
            return None
        args.extend(["--live-export-dir", str(live_export_dir)])
    output_flag = _output_dir_flag(script_entry["path"])
    if output_flag:
        args.extend([output_flag, str(output_dir)])
    return {
        "name": Path(script_entry["path"]).stem,
        "status": "planned",
        "script": script_entry["path"],
        "lane": script_entry["lane"],
        "args": args,
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_workflow_review_artifact(
    *,
    workflow_output_dir: Path,
    query: str,
    mode: str,
    resolution: dict[str, Any],
    route: dict[str, Any],
    candidate: dict[str, Any] | None,
    summary: dict[str, Any],
    artifact_paths: dict[str, str | None],
) -> Path:
    lines = [
        "# Intelligence Workflow Review",
        "",
        f"- Goal: {query}",
        f"- Mode: {mode}",
        f"- Route surface: {route.get('recommended_surface_type') or 'unknown'}",
        f"- Effective surface: {summary.get('effective_surface_type') or route.get('recommended_surface_type') or 'unknown'}",
        f"- Operation: {route.get('operation_mode') or 'unknown'}",
        f"- Workflow status: {summary.get('workflow_status') or 'unknown'}",
    ]
    if isinstance(summary.get("surface_selection_reason"), str) and summary["surface_selection_reason"]:
        lines.append(f"- Surface selection reason: {summary['surface_selection_reason']}")
    if isinstance(summary.get("secondary_surface_type"), str) and summary["secondary_surface_type"]:
        lines.append(f"- Secondary surface: {summary['secondary_surface_type']}")
    if candidate and isinstance(candidate.get("id"), str):
        lines.append(f"- Candidate surface: {candidate['id']}")
    if isinstance(summary.get("evaluation_verdict"), str) and summary["evaluation_verdict"]:
        lines.append(f"- Evaluation verdict: {summary['evaluation_verdict']}")
    if isinstance(summary.get("report_surface_verdict"), str) and summary["report_surface_verdict"]:
        lines.append(f"- Report surface verdict: {summary['report_surface_verdict']}")
    planned_report_filter_overrides = summary.get("planned_report_filter_overrides") or []
    if planned_report_filter_overrides:
        rendered_overrides = ", ".join(
            f"{item['source_label']}={item['value']}"
            for item in planned_report_filter_overrides
            if isinstance(item, dict)
            and isinstance(item.get("source_label"), str)
            and isinstance(item.get("value"), str)
        )
        if rendered_overrides:
            lines.append(f"- Planned report filter overrides: `{rendered_overrides}`")
    if summary.get("resolved_report_filter_override_count") is not None:
        lines.append(
            f"- Resolved report filter overrides: `{summary['resolved_report_filter_override_count']}`"
        )
    if summary.get("manual_report_filter_intent_count") is not None:
        lines.append(
            f"- Remaining manual report filter intents: `{summary['manual_report_filter_intent_count']}`"
        )
    if summary.get("report_apply_strategy") is not None:
        lines.append(f"- Report apply strategy: `{summary['report_apply_strategy']}`")
    if summary.get("report_apply_ready") is not None:
        lines.append(f"- Report apply ready: `{summary['report_apply_ready']}`")
    if summary.get("report_native_authoring_ready") is not None:
        lines.append(f"- Native report authoring ready: `{summary['report_native_authoring_ready']}`")
    if summary.get("report_external_fill_requirement_count") is not None:
        lines.append(
            f"- Remaining external report fill requirements: `{summary['report_external_fill_requirement_count']}`"
        )

    lines.extend(
        [
            "",
            "## Resolution",
            f"- Persona: `{resolution.get('resolved_persona') or 'unknown'}`",
            f"- Domain: `{resolution.get('resolved_domain') or 'unknown'}`",
        ]
    )

    memory_health = summary.get("memory_health") or {}
    if isinstance(memory_health, dict):
        lines.extend(
            [
                "",
                "## Memory Health",
                f"- Excluded policy-exception hits: `{memory_health.get('policy_exception_hits_excluded', 0)}`",
                f"- Included failing hits: `{memory_health.get('included_fail_count', 0)}`",
                f"- Included generic-goal hits: `{memory_health.get('included_generic_goal_count', 0)}`",
            ]
        )

    lines.extend(["", "## Artifacts"])
    artifact_labels = (
        ("goal_artifact", "Goal"),
        ("memory_artifact", "Memory hits"),
        ("plan_artifact", "Plan"),
        ("evaluation_artifact", "Evaluation"),
        ("memory_record_artifact", "Memory record"),
    )
    for key, label in artifact_labels:
        value = artifact_paths.get(key)
        if isinstance(value, str) and value:
            lines.append(f"- {label}: `{value}`")

    planner_notes = summary.get("planner_notes") or []
    if planner_notes:
        lines.extend(["", "## Planner Notes"])
        for note in planner_notes:
            lines.append(f"- {note}")

    lines.extend(["", "## Steps"])
    for step in summary.get("executed_steps", []):
        if not isinstance(step, dict):
            continue
        lines.append(f"- {step.get('name')}: `{step.get('status', 'unknown')}`")
        if isinstance(step.get("script"), str) and step["script"]:
            lines.append(f"  Script: `{step['script']}`")

    if not summary.get("executed_steps"):
        for step_name in summary.get("planned_steps", []):
            lines.append(f"- {step_name}: `planned`")

    review_path = workflow_output_dir / "README.md"
    review_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return review_path


def _render_intelligence_workflow_collection_entry(item: dict[str, Any]) -> list[str]:
    run_label = item.get("label") or Path(str(item.get("run_dir") or "")).name or "run"
    lines = [
        f"### {run_label}",
        f"- Status: `{item.get('status') or 'unknown'}`",
        f"- Updated: `{item.get('updated_at') or 'unknown'}`",
    ]
    if isinstance(item.get("mode"), str) and item["mode"]:
        lines.append(f"- Mode: `{item['mode']}`")
    if isinstance(item.get("surface"), str) and item["surface"]:
        lines.append(f"- Surface: `{item['surface']}`")
    if isinstance(item.get("candidate_surface_id"), str) and item["candidate_surface_id"]:
        lines.append(f"- Candidate surface: `{item['candidate_surface_id']}`")
    if isinstance(item.get("evaluation_verdict"), str) and item["evaluation_verdict"]:
        lines.append(f"- Evaluation verdict: `{item['evaluation_verdict']}`")
    if isinstance(item.get("run_dir"), str) and item["run_dir"]:
        lines.append(f"- Run dir: `{item['run_dir']}`")
    if isinstance(item.get("landing_artifact"), str) and item["landing_artifact"]:
        lines.append(f"- Landing page: `{item['landing_artifact']}`")
    return lines


def _write_intelligence_workflow_collection_index(
    *,
    collection_root: Path,
    entry: dict[str, Any],
) -> tuple[Path, Path]:
    return ai_os_browser.write_run_collection_index(
        collection_root=collection_root,
        index_filename="intelligence_workflow_run_index.json",
        overview_filename="intelligence_workflow_overview.md",
        title="# Intelligence Workflow Runs",
        entry={
            "command": entry.get("command"),
            "status": entry.get("status"),
            "label": entry.get("label"),
            "run_dir": entry.get("run_dir"),
            "landing_artifact": entry.get("landing_artifact"),
            "review_artifact": entry.get("review_artifact"),
            "surface": entry.get("surface"),
            "candidate_surface_id": entry.get("candidate_surface_id"),
            "mode": entry.get("mode"),
            "evaluation_verdict": entry.get("evaluation_verdict"),
        },
        render_entry_lines=_render_intelligence_workflow_collection_entry,
    )


def _serialize_child_output(
    *,
    step_result: dict[str, Any],
    output_path: Path,
) -> dict[str, str] | None:
    execution = step_result.get("execution")
    if not isinstance(execution, dict):
        return None
    child = execution.get("structured_output")
    if not isinstance(child, dict):
        return None

    _write_json(output_path, child)
    artifacts = child.setdefault("artifacts", [])
    if not isinstance(artifacts, list):
        artifacts = []
        child["artifacts"] = artifacts
    artifact = {"kind": "json", "path": str(output_path)}
    if artifact not in artifacts:
        artifacts.append(artifact)
    return artifact


def _planned_output_root(
    *,
    script_entry: dict[str, Any],
    workflow_base_dir: Path,
    audit_dir: Path,
    profile_dir: Path,
    validation_dir: Path,
) -> Path:
    if script_entry["lane"] == "salesforce_data_profiles":
        return profile_dir / Path(script_entry["path"]).stem
    if script_entry["lane"] == "wave_data_validations":
        return validation_dir / Path(script_entry["path"]).stem
    if script_entry["lane"] == "export_audits":
        return audit_dir
    if script_entry["lane"] == "patch_guardrails":
        return workflow_base_dir / "guardrails" / Path(script_entry["path"]).stem
    return workflow_base_dir / Path(script_entry["path"]).stem


def _build_planned_report_executor_step(
    *,
    name: str,
    command: str,
    workflow_base_dir: Path,
    query: str,
    lane: str,
    candidate: dict[str, Any] | None,
) -> dict[str, Any]:
    package_path = workflow_base_dir / "report_handoff" / "build_package.json"
    output_dir = workflow_base_dir / ("report_preview" if command == "preview" else "report_apply_preview")
    args = [
        command,
        "--package",
        str(package_path),
        "--output-dir",
        str(output_dir),
    ]
    clone_from_report_id = _default_report_clone_from_id(candidate)
    if clone_from_report_id:
        args.extend(["--clone-from-report-id", clone_from_report_id])
    target_org = _default_report_target_org()
    if target_org:
        args.extend(["--autofill-live", "--target-org", target_org])
    args.extend(_infer_report_filter_override_args(query=query))
    return {
        "name": name,
        "status": "planned",
        "script": "scripts/salesforce_report_executor.py",
        "lane": lane,
        "args": args,
    }


def _build_planned_workflow_step(
    *,
    planner_step: dict[str, Any],
    script_entry: dict[str, Any],
    workflow_base_dir: Path,
    live_export_dir: Path,
    audit_dir: Path,
    profile_dir: Path,
    validation_dir: Path,
    candidate: dict[str, Any] | None,
    query: str,
    resolution: dict[str, Any],
) -> dict[str, Any] | None:
    script_path = planner_step["script"]
    if script_path == "scripts/builder_brain.py":
        handoff_dir = workflow_base_dir / "report_handoff"
        args = [
            "handoff",
            "--query",
            query,
            "--output-dir",
            str(handoff_dir),
        ]
        if isinstance(resolution.get("resolved_persona"), str) and resolution["resolved_persona"]:
            args.extend(["--persona", resolution["resolved_persona"]])
        if isinstance(resolution.get("resolved_domain"), str) and resolution["resolved_domain"]:
            args.extend(["--domain", resolution["resolved_domain"]])
        return {
            "name": planner_step["name"],
            "status": "planned",
            "script": script_path,
            "lane": script_entry["lane"],
            "args": args,
        }
    if script_path == "scripts/salesforce_report_executor.py":
        return _build_planned_report_executor_step(
            name=planner_step["name"],
            command="preview",
            workflow_base_dir=workflow_base_dir,
            query=query,
            lane=script_entry["lane"],
            candidate=candidate,
        )
    if script_path == "scripts/export_live_crma_assets.py":
        labels = candidate.get("labels", []) if candidate else []
        if not labels:
            return None
        return {
            "name": planner_step["name"],
            "status": "planned",
            "script": script_path,
            "lane": script_entry["lane"],
            "args": ["--output-dir", str(live_export_dir), *labels],
        }
    if script_path == "scripts/contract_lint.py":
        return {
            "name": planner_step["name"],
            "status": "planned",
            "script": script_path,
            "lane": script_entry["lane"],
            "args": [str(live_export_dir), "--summary"],
        }

    planned = _build_planned_lane_step(
        script_entry=script_entry,
        output_dir=_planned_output_root(
            script_entry=script_entry,
            workflow_base_dir=workflow_base_dir,
            audit_dir=audit_dir,
            profile_dir=profile_dir,
            validation_dir=validation_dir,
        ),
        live_export_dir=live_export_dir,
    )
    if not planned:
        return None
    planned["name"] = planner_step["name"]
    return planned


def _infer_report_filter_override_args(*, query: str) -> list[str]:
    def add_override(overrides: dict[str, str], label: str, value: str | None) -> None:
        if isinstance(value, str) and value.strip() and label not in overrides:
            overrides[label] = value.strip()

    def extract_owner_override(raw_query: str) -> str | None:
        for pattern in (
            r'(?i)\bowner\s+["\']([^"\']+)["\']',
            r'(?i)\bowned by\s+["\']([^"\']+)["\']',
            r'(?i)\bfor owner\s+["\']([^"\']+)["\']',
        ):
            match = re.search(pattern, raw_query)
            if match:
                return match.group(1).strip()
        for pattern in (
            r"\bowner\s+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+){1,2})\b",
            r"\bowned by\s+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+){1,2})\b",
            r"\bfor owner\s+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+){1,2})\b",
        ):
            match = re.search(pattern, raw_query)
            if match:
                return match.group(1).strip()
        return None

    def extract_product_family_override(raw_query: str, normalized_query: str) -> str | None:
        for pattern in (
            r'(?i)\bproduct family\s+["\']([^"\']+)["\']',
            r'(?i)\bproduct\s+["\']([^"\']+)["\']',
        ):
            match = re.search(pattern, raw_query)
            if match:
                return match.group(1).strip()
        for token, value in (
            ("axioma", "Axioma"),
            ("analytics services", "Analytics Services"),
            ("data management services", "Data Management Services"),
        ):
            if token in normalized_query:
                return value
        return None

    normalized_query = normalize_text(query)
    overrides: dict[str, str] = {}

    renewal_period_overrides = (
        ("this week", "This Week"),
        ("current week", "This Week"),
        ("next week", "Next Week"),
        ("this month", "This Month"),
        ("current month", "This Month"),
        ("next month", "Next Month"),
        ("this quarter", "This Quarter"),
        ("current quarter", "This Quarter"),
        ("next quarter", "Next Quarter"),
        ("this year", "This Year"),
        ("current year", "This Year"),
    )
    for token, value in renewal_period_overrides:
        if token in normalized_query:
            add_override(overrides, "renewal_period", value)
            break

    add_override(overrides, "owner", extract_owner_override(query))
    add_override(overrides, "product_family", extract_product_family_override(query, normalized_query))

    risk_band_overrides = (
        ("high risk", "High"),
        ("medium risk", "Medium"),
        ("moderate risk", "Medium"),
        ("low risk", "Low"),
    )
    for token, value in risk_band_overrides:
        if token in normalized_query:
            add_override(overrides, "risk_band", value)
            break

    args: list[str] = []
    for label in ("renewal_period", "owner", "product_family", "risk_band"):
        if label in overrides:
            args.extend(["--filter-override", f"{label}={overrides[label]}"])
    return args


def build_resolve_result(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
) -> dict[str, Any]:
    resolution = resolve_request(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
    )
    return make_result(
        status="ok",
        command="resolve",
        messages=[make_message("info", "resolution_ready", "Resolved query against local semantic context.")],
        resolution=resolution,
    )


def build_route_result(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
) -> dict[str, Any]:
    resolution = resolve_request(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
    )
    route = route_surface(inputs, resolution)
    return make_result(
        status="ok",
        command="route",
        messages=[make_message("info", "route_ready", "Built surface and lane routing guidance.")],
        resolution=resolution,
        route=route,
    )


def build_review_result(
    inputs: dict[str, Any],
    *,
    script_path: str | None,
    query: str | None,
    persona: str | None,
    domain: str | None,
    operation: str | None,
) -> dict[str, Any]:
    if script_path:
        review = build_review_plan(inputs, script_path=script_path)
        status = review.pop("status")
        messages = review.pop("messages", [make_message("info", "review_ready", "Built review gates for script.")])
        return make_result(
            status=status,
            command="review",
            messages=messages,
            **review,
        )

    if not query:
        return make_result(
            status="error",
            command="review",
            messages=[
                make_message("error", "missing_review_target", "Provide either --script or --query.")
            ],
        )
    resolution = resolve_request(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
    )
    route = route_surface(inputs, resolution)
    review = build_review_plan(inputs, resolved=resolution, route=route)
    return make_result(
        status="ok",
        command="review",
        messages=[make_message("info", "review_ready", "Built review gates for routed request.")],
        resolution=resolution,
        route=route,
        gates=review["gates"],
    )


def execute_registered_script(
    inputs: dict[str, Any],
    *,
    script_path: str,
    script_args: list[str],
    allow_mutating: bool,
    allow_destructive: bool,
) -> dict[str, Any]:
    script_entry = next(
        (item for item in inputs["registry"]["scripts"] if item["path"] == script_path),
        None,
    )
    if not script_entry:
        return make_result(
            status="error",
            command="execute",
            messages=[make_message("error", "unknown_script", f"Script is not registered: {script_path}.")],
        )

    if script_entry["command_class"] == "mutating" and not allow_mutating:
        return make_result(
            status="error",
            command="execute",
            messages=[
                make_message(
                    "error",
                    "mutating_not_allowed",
                    f"{script_path} is mutating; rerun with --allow-mutating to execute it.",
                )
            ],
            execution={
                "script": script_path,
                "lane": script_entry["lane"],
                "command_class": script_entry["command_class"],
                "risk": script_entry["risk"],
            },
        )
    if script_entry["risk"] == "destructive" and not allow_destructive:
        return make_result(
            status="error",
            command="execute",
            messages=[
                make_message(
                    "error",
                    "destructive_not_allowed",
                    f"{script_path} is destructive; rerun with --allow-destructive to execute it.",
                )
            ],
            execution={
                "script": script_path,
                "lane": script_entry["lane"],
                "command_class": script_entry["command_class"],
                "risk": script_entry["risk"],
            },
        )

    full_script = ROOT / script_path
    command_args = list(script_args)
    json_requested = False
    if _supports_json_output(script_entry) and not _has_json_flag(command_args):
        command_args.append("--json")
        json_requested = True
    command = [sys.executable, str(full_script), *command_args]
    proc = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    parsed_output: dict[str, Any] | None = None
    if _supports_json_output(script_entry):
        try:
            parsed_output = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            return make_result(
                status="error",
                command="execute",
                messages=[
                    make_message(
                        "error",
                        "structured_output_parse_failed",
                        f"{script_path} advertised structured output but returned invalid JSON: {exc}.",
                    )
                ],
                execution={
                    "script": script_path,
                    "lane": script_entry["lane"],
                    "command_class": script_entry["command_class"],
                    "risk": script_entry["risk"],
                    "invoked_command": command,
                    "returncode": proc.returncode,
                    "json_requested": json_requested,
                    "structured_output_supported": True,
                    "stdout_excerpt": proc.stdout[:2000],
                    "stderr_excerpt": proc.stderr[:2000],
                },
            )
        envelope_errors = _validate_child_result_envelope(inputs["registry"], parsed_output)
        if envelope_errors:
            return make_result(
                status="error",
                command="execute",
                messages=[
                    make_message(
                        "error",
                        "structured_output_invalid",
                        f"{script_path} returned an invalid result envelope: {'; '.join(envelope_errors)}",
                    )
                ],
                execution={
                    "script": script_path,
                    "lane": script_entry["lane"],
                    "command_class": script_entry["command_class"],
                    "risk": script_entry["risk"],
                    "invoked_command": command,
                    "returncode": proc.returncode,
                    "json_requested": json_requested,
                    "structured_output_supported": True,
                    "structured_output": parsed_output,
                    "stdout_excerpt": proc.stdout[:2000],
                    "stderr_excerpt": proc.stderr[:2000],
                },
            )

    status = parsed_output["status"] if parsed_output else ("ok" if proc.returncode == 0 else "error")
    child_messages = parsed_output.get("messages", []) if parsed_output else []
    return make_result(
        status=status,
        command="execute",
        messages=[
            make_message(
                "info" if status == "ok" else ("warn" if status == "warn" else "error"),
                "execution_finished" if status in {"ok", "warn"} else "execution_failed",
                f"Executed {script_path} with return code {proc.returncode}.",
            )
        ]
        + child_messages,
        execution={
            "script": script_path,
            "lane": script_entry["lane"],
            "command_class": script_entry["command_class"],
            "risk": script_entry["risk"],
            "invoked_command": command,
            "returncode": proc.returncode,
            "json_requested": json_requested,
            "structured_output_supported": _supports_json_output(script_entry),
            "structured_output": parsed_output,
            "stdout_excerpt": proc.stdout[:2000],
            "stderr_excerpt": proc.stderr[:2000],
        },
    )


def build_workflow(
    inputs: dict[str, Any],
    *,
    query: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    execute_safe: bool,
    output_dir: str | None,
) -> dict[str, Any]:
    import harness_planner
    import plan_evaluator
    import run_memory

    resolution = resolve_request(
        inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
    )
    route = route_surface(inputs, resolution)
    surface_advisory = harness_planner.build_surface_advisory(
        inputs=inputs,
        query=query,
        persona=persona,
        domain=domain,
        operation=operation,
        resolution=resolution,
        route=route,
    )
    effective_surface = harness_planner.effective_surface_type(
        route=route,
        surface_advisory=surface_advisory,
    )
    review = build_review_plan(inputs, resolved=resolution, route=route)
    candidate = resolution["candidate_surfaces"][0] if resolution["candidate_surfaces"] else None

    workflow_output_dir = None
    if output_dir:
        workflow_output_dir = Path(output_dir).resolve()
    elif execute_safe:
        slug = re.sub(r"[^a-z0-9]+", "_", normalize_text(query)).strip("_")[:60] or "workflow"
        workflow_output_dir = (
            ROOT
            / "output"
            / "intelligence_workflows"
            / f"{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}_{slug}"
        )
    workflow_base_dir = workflow_output_dir or (ROOT / "output" / "intelligence_workflows" / "preview")
    run_id = workflow_output_dir.name if workflow_output_dir else None
    goal_dir = workflow_base_dir / "goal"
    memory_dir = workflow_base_dir / "memory"
    plan_dir = workflow_base_dir / "plan"
    steps_dir = workflow_base_dir / "steps"
    evaluation_dir = workflow_base_dir / "evaluation"
    live_export_dir = workflow_base_dir / "live_export"
    audit_dir = workflow_base_dir / "audit"
    profile_dir = workflow_base_dir / "profiles"
    validation_dir = workflow_base_dir / "validations"

    steps: list[dict[str, Any]] = [
        {"name": "resolve", "status": "ok", "result": resolution},
        {"name": "route", "status": "ok", "result": route},
        {"name": "review", "status": "ok", "result": {"gates": review["gates"]}},
    ]
    messages = [make_message("info", "workflow_built", "Built intelligence workflow.")]
    memory_record_payload: dict[str, Any] | None = None
    planner_profile = inputs["planner_profiles"]["operation_defaults"].get(
        route["operation_mode"],
        inputs["planner_profiles"]["operation_defaults"]["review_dashboard"],
    )
    planner_required_evidence = harness_planner.required_evidence_for_surface(
        planner_profile=planner_profile,
        surface_advisory=surface_advisory,
    )
    memory_search_tags = harness_planner.build_surface_memory_tags(
        route=route,
        candidate=candidate,
        surface_advisory=surface_advisory,
    )

    memory_hits = run_memory.search_runs(
        goal=query,
        persona=resolution.get("resolved_persona"),
        domain=resolution.get("resolved_domain"),
        operation=route["operation_mode"],
        tags=memory_search_tags,
        evidence_types=planner_required_evidence,
    )
    memory_health = run_memory.summarize_search_health(
        goal=query,
        persona=resolution.get("resolved_persona"),
        domain=resolution.get("resolved_domain"),
        operation=route["operation_mode"],
        tags=memory_search_tags,
        evidence_types=planner_required_evidence,
    )
    plan = harness_planner.build_plan(
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

    registry_by_path = {item["path"]: item for item in inputs["registry"]["scripts"]}
    planned_commands: list[dict[str, Any]] = []
    for planner_step in plan["recommended_sequence"]:
        script_entry = registry_by_path.get(planner_step["script"])
        if not script_entry:
            continue
        planned = _build_planned_workflow_step(
            planner_step=planner_step,
            script_entry=script_entry,
            workflow_base_dir=workflow_base_dir,
            live_export_dir=live_export_dir,
            audit_dir=audit_dir,
            profile_dir=profile_dir,
            validation_dir=validation_dir,
            candidate=candidate,
            query=query,
            resolution=resolution,
        )
        if planned:
            planned_commands.append(planned)
            if (
                planned["script"] == "scripts/salesforce_report_executor.py"
                and planned["name"] == "preview_report_rest"
            ):
                planned_commands.append(
                    _build_planned_report_executor_step(
                        name="apply_report_rest_dry_run",
                        command="apply",
                        workflow_base_dir=workflow_base_dir,
                        query=query,
                        lane=script_entry["lane"],
                        candidate=candidate,
                    )
                )

    supported_execution = bool(planned_commands)
    persisted_artifacts: list[dict[str, str]] = []
    plan_path: Path | None = None
    memory_hits_path: Path | None = None

    if workflow_output_dir is not None:
        goal_dir.mkdir(parents=True, exist_ok=True)
        goal_text = "\n".join(
            [
                f"# Goal\n{query}",
                "",
                "## Resolution",
                f"- Persona: {resolution.get('resolved_persona')}",
                f"- Domain: {resolution.get('resolved_domain')}",
                f"- Operation: {route['operation_mode']}",
            ]
        )
        (goal_dir / "goal.md").write_text(goal_text, encoding="utf-8")
        memory_hits_path = memory_dir / "memory_hits.json"
        _write_json(
            memory_hits_path,
            {
                "query": query,
                "persona": resolution.get("resolved_persona"),
                "domain": resolution.get("resolved_domain"),
                "operation": route["operation_mode"],
                "similar_runs": memory_hits,
                "memory_health": memory_health,
            },
        )
        plan_path = plan_dir / "plan.json"
        _write_json(plan_path, plan)
        persisted_artifacts.extend(
            [
                {"kind": "markdown", "path": str(goal_dir / "goal.md")},
                {"kind": "json", "path": str(memory_hits_path)},
                {"kind": "json", "path": str(plan_path)},
            ]
        )

    if not execute_safe:
        for planned in planned_commands:
            steps.append(planned)
        summary = _build_workflow_summary(
            route=route,
            review=review,
            candidate=candidate,
            steps=steps,
            supported_execution=bool(supported_execution),
            effective_surface_type=effective_surface,
            surface_advisory=plan.get("surface_advisory"),
            workflow_status="ok",
            planner_notes=plan.get("planner_notes", []),
            memory_health=memory_health,
        )
        if workflow_output_dir is not None:
            review_path = _write_workflow_review_artifact(
                workflow_output_dir=workflow_output_dir,
                query=query,
                mode="plan",
                resolution=resolution,
                route=route,
                candidate=candidate,
                summary=summary,
                artifact_paths={
                    "goal_artifact": str(goal_dir / "goal.md"),
                    "memory_artifact": str(memory_hits_path) if memory_hits_path else None,
                    "plan_artifact": str(plan_path) if plan_path else None,
                    "evaluation_artifact": None,
                    "memory_record_artifact": None,
                },
            )
            collection_index_path, collection_overview_path = _write_intelligence_workflow_collection_index(
                collection_root=workflow_output_dir.parent,
                entry={
                    "command": "workflow",
                    "status": "ok",
                    "label": workflow_output_dir.name,
                    "run_dir": str(workflow_output_dir),
                    "landing_artifact": str(review_path),
                    "review_artifact": str(review_path),
                    "surface": effective_surface,
                    "candidate_surface_id": candidate["id"] if candidate else None,
                    "mode": "plan",
                    "evaluation_verdict": None,
                },
            )
            browser_index_path, browser_overview_path = ai_os_browser.write_ai_os_browser_index(
                browser_root=ai_os_browser.resolve_ai_os_browser_root(collection_root=workflow_output_dir.parent),
            )
            health_summary = ai_os_browser.load_ai_os_browser_health_summary(index_path=browser_index_path)
            health_index_path, health_overview_path = ai_os_browser.resolve_ai_os_health_paths(
                browser_root=browser_index_path.parent,
            )
            summary.update(
                {
                    "review_artifact": str(review_path),
                    "collection_index_artifact": str(collection_index_path),
                    "collection_landing_artifact": str(collection_overview_path),
                    "browser_index_artifact": str(browser_index_path),
                    "browser_landing_artifact": str(browser_overview_path),
                    "browser_health_index_artifact": str(health_index_path),
                    "browser_health_landing_artifact": str(health_overview_path),
                    "browser_health_summary": health_summary,
                }
            )
            for artifact in (
                {"kind": "markdown", "path": str(review_path)},
                {"kind": "json", "path": str(collection_index_path)},
                {"kind": "markdown", "path": str(collection_overview_path)},
                {"kind": "json", "path": str(browser_index_path)},
                {"kind": "markdown", "path": str(browser_overview_path)},
                {"kind": "json", "path": str(health_index_path)},
                {"kind": "markdown", "path": str(health_overview_path)},
            ):
                if artifact not in persisted_artifacts:
                    persisted_artifacts.append(artifact)
            messages.extend(
                [
                    make_message("info", "workflow_review_ready", f"Workflow review: {review_path}"),
                    make_message(
                        "info",
                        "intelligence_workflow_collection_index_ready",
                        f"Workflow collection overview: {collection_overview_path}",
                    ),
                    make_message("info", "ai_os_browser_ready", f"AI OS browser: {browser_overview_path}"),
                    make_message("info", "ai_os_health_ready", f"AI OS health: {health_overview_path}"),
                ]
            )
        return make_result(
            status="ok",
            command="workflow",
            messages=messages,
            artifacts=persisted_artifacts,
            workflow={
                "mode": "plan",
                "resolution": resolution,
                "route": route,
                "review": {"gates": review["gates"]},
                "candidate_surface": candidate,
                "memory_hits": memory_hits,
                "memory_health": memory_health,
                "memory_record": None,
                "plan": plan,
                "surface_advisory": plan.get("surface_advisory"),
                "evaluation": None,
                "output_dir": str(workflow_output_dir) if workflow_output_dir else None,
                "summary": summary,
                "steps": steps,
            },
        )

    if not supported_execution:
        messages.append(
            make_message(
                "warn",
                "workflow_execution_not_supported",
                "Safe execution currently supports workflows with typed profile, validation, or candidate-surface steps.",
            )
        )
        summary = _build_workflow_summary(
            route=route,
            review=review,
            candidate=candidate,
            steps=steps,
            supported_execution=bool(supported_execution),
            effective_surface_type=effective_surface,
            surface_advisory=plan.get("surface_advisory"),
            workflow_status="warn",
            planner_notes=plan.get("planner_notes", []),
            memory_health=memory_health,
        )
        if workflow_output_dir is not None:
            review_path = _write_workflow_review_artifact(
                workflow_output_dir=workflow_output_dir,
                query=query,
                mode="execute_safe",
                resolution=resolution,
                route=route,
                candidate=candidate,
                summary=summary,
                artifact_paths={
                    "goal_artifact": str(goal_dir / "goal.md"),
                    "memory_artifact": str(memory_hits_path) if memory_hits_path else None,
                    "plan_artifact": str(plan_path) if plan_path else None,
                    "evaluation_artifact": None,
                    "memory_record_artifact": None,
                },
            )
            collection_index_path, collection_overview_path = _write_intelligence_workflow_collection_index(
                collection_root=workflow_output_dir.parent,
                entry={
                    "command": "workflow",
                    "status": "warn",
                    "label": workflow_output_dir.name,
                    "run_dir": str(workflow_output_dir),
                    "landing_artifact": str(review_path),
                    "review_artifact": str(review_path),
                    "surface": effective_surface,
                    "candidate_surface_id": candidate["id"] if candidate else None,
                    "mode": "execute_safe",
                    "evaluation_verdict": None,
                },
            )
            browser_index_path, browser_overview_path = ai_os_browser.write_ai_os_browser_index(
                browser_root=ai_os_browser.resolve_ai_os_browser_root(collection_root=workflow_output_dir.parent),
            )
            health_summary = ai_os_browser.load_ai_os_browser_health_summary(index_path=browser_index_path)
            health_index_path, health_overview_path = ai_os_browser.resolve_ai_os_health_paths(
                browser_root=browser_index_path.parent,
            )
            summary.update(
                {
                    "review_artifact": str(review_path),
                    "collection_index_artifact": str(collection_index_path),
                    "collection_landing_artifact": str(collection_overview_path),
                    "browser_index_artifact": str(browser_index_path),
                    "browser_landing_artifact": str(browser_overview_path),
                    "browser_health_index_artifact": str(health_index_path),
                    "browser_health_landing_artifact": str(health_overview_path),
                    "browser_health_summary": health_summary,
                }
            )
            for artifact in (
                {"kind": "markdown", "path": str(review_path)},
                {"kind": "json", "path": str(collection_index_path)},
                {"kind": "markdown", "path": str(collection_overview_path)},
                {"kind": "json", "path": str(browser_index_path)},
                {"kind": "markdown", "path": str(browser_overview_path)},
                {"kind": "json", "path": str(health_index_path)},
                {"kind": "markdown", "path": str(health_overview_path)},
            ):
                if artifact not in persisted_artifacts:
                    persisted_artifacts.append(artifact)
            messages.extend(
                [
                    make_message("info", "workflow_review_ready", f"Workflow review: {review_path}"),
                    make_message(
                        "info",
                        "intelligence_workflow_collection_index_ready",
                        f"Workflow collection overview: {collection_overview_path}",
                    ),
                    make_message("info", "ai_os_browser_ready", f"AI OS browser: {browser_overview_path}"),
                    make_message("info", "ai_os_health_ready", f"AI OS health: {health_overview_path}"),
                ]
            )
        return make_result(
            status="warn",
            command="workflow",
            messages=messages,
            artifacts=persisted_artifacts,
            workflow={
                "mode": "execute_safe",
                "resolution": resolution,
                "route": route,
                "review": {"gates": review["gates"]},
                "candidate_surface": candidate,
                "memory_hits": memory_hits,
                "memory_health": memory_health,
                "memory_record": None,
                "plan": plan,
                "surface_advisory": plan.get("surface_advisory"),
                "evaluation": None,
                "output_dir": str(workflow_output_dir) if workflow_output_dir else None,
                "summary": summary,
                "steps": steps,
            },
        )

    assert workflow_output_dir is not None
    live_export_dir.mkdir(parents=True, exist_ok=True)
    steps_dir.mkdir(parents=True, exist_ok=True)

    workflow_status = "ok"
    export_status = None
    for index, planned in enumerate(planned_commands, start=1):
        requires_export = planned["script"] == "scripts/contract_lint.py" or _requires_live_export_dir(planned["script"])
        if requires_export and export_status not in {"ok", "warn"} and planned["script"] != "scripts/export_live_crma_assets.py":
            continue

        step_result = execute_registered_script(
            inputs,
            script_path=planned["script"],
            script_args=planned["args"],
            allow_mutating=False,
            allow_destructive=False,
        )
        child_artifact = _serialize_child_output(
            step_result=step_result,
            output_path=steps_dir / f"{index:02d}_{planned['name']}_{Path(planned['script']).stem}" / "child_output.json",
        )
        if child_artifact and child_artifact not in persisted_artifacts:
            persisted_artifacts.append(child_artifact)
        steps.append(
            {
                "name": planned["name"],
                "status": step_result["status"],
                "script": planned["script"],
                "lane": planned["lane"],
                "args": planned["args"],
                "result": step_result,
            }
        )
        workflow_status = _combine_status(workflow_status, step_result["status"])
        if planned["script"] == "scripts/export_live_crma_assets.py":
            export_status = step_result["status"]

    evaluation_payload: dict[str, Any] | None = None
    if plan_path is not None:
        evaluation_result, _ = plan_evaluator.evaluate_plan(
            inputs=plan_evaluator.load_inputs(),
            plan_path=plan_path,
            artifacts_dir=workflow_base_dir,
            output_dir=evaluation_dir,
        )
        evaluation_payload = evaluation_result.get("evaluation")
        persisted_artifacts.extend(
            [
                artifact
                for artifact in evaluation_result.get("artifacts", [])
                if artifact not in persisted_artifacts
            ]
        )
        verdict = evaluation_payload.get("verdict") if isinstance(evaluation_payload, dict) else None
        if verdict == "fail":
            workflow_status = "error"
        elif verdict == "needs_more_evidence" and workflow_status == "ok":
            workflow_status = "warn"

    messages.append(
        make_message(
            "info" if workflow_status == "ok" else ("warn" if workflow_status == "warn" else "error"),
            "workflow_finished" if workflow_status in {"ok", "warn"} else "workflow_failed",
            f"Workflow completed with status {workflow_status}.",
        )
    )
    child_artifacts = _collect_child_artifacts(steps)
    if run_id and isinstance(evaluation_payload, dict):
        artifact_paths: list[str] = []
        for artifact in [*persisted_artifacts, *child_artifacts]:
            if not isinstance(artifact, dict):
                continue
            path = artifact.get("path")
            if isinstance(path, str) and path and path not in artifact_paths:
                artifact_paths.append(path)

        executed_sequence = [
            step["script"]
            for step in steps
            if isinstance(step, dict) and isinstance(step.get("script"), str) and step.get("script")
        ]

        failure_reason = None
        if evaluation_payload.get("verdict") == "fail":
            blocking_findings = evaluation_payload.get("blocking_findings") or []
            if blocking_findings and isinstance(blocking_findings[0], dict):
                failure_reason = blocking_findings[0].get("code")
            if not failure_reason:
                failure_reason = "evaluation_failed"
        elif evaluation_payload.get("verdict") == "needs_more_evidence":
            failure_reason = "needs_more_evidence"
        elif workflow_status != "ok":
            for step in steps:
                if not isinstance(step, dict) or step.get("status") not in {"warn", "error"}:
                    continue
                step_messages = (step.get("result") or {}).get("messages") or []
                if step_messages and isinstance(step_messages[0], dict):
                    failure_reason = step_messages[0].get("code")
                    break

        tags: list[str] = []
        for value in (
            resolution.get("resolved_persona"),
            resolution.get("resolved_domain"),
            effective_surface,
            route.get("operation_mode"),
            (candidate or {}).get("id") if isinstance(candidate, dict) else None,
        ):
            if isinstance(value, str) and value and value not in tags:
                tags.append(value)

        memory_result, memory_exit_code = run_memory.record_run(
            run_id=run_id,
            goal=query,
            persona=resolution.get("resolved_persona"),
            domain=resolution.get("resolved_domain"),
            operation=route["operation_mode"],
            sequence=executed_sequence,
            verdict=evaluation_payload["verdict"],
            outcome=f"workflow_{workflow_status}",
            failure_reason=failure_reason,
            artifacts=artifact_paths,
            tags=tags,
            evidence_types=plan.get("required_evidence", []),
        )
        if memory_exit_code == 0:
            memory_record_payload = memory_result.get("record")
            for artifact in memory_result.get("artifacts", []):
                if artifact not in persisted_artifacts:
                    persisted_artifacts.append(artifact)
        else:
            memory_messages = memory_result.get("messages") or []
            message_text = memory_messages[0].get("text") if memory_messages and isinstance(memory_messages[0], dict) else "Unable to record workflow memory."
            messages.append(make_message("warn", "workflow_memory_record_failed", str(message_text)))

    artifacts = [{"kind": "directory", "path": str(workflow_output_dir)}]
    artifacts.extend(persisted_artifacts)
    artifacts.extend(child_artifacts)
    summary = _build_workflow_summary(
        route=route,
        review=review,
        candidate=candidate,
        steps=steps,
        supported_execution=bool(supported_execution),
        effective_surface_type=effective_surface,
        surface_advisory=plan.get("surface_advisory"),
        workflow_status=workflow_status,
        planner_notes=plan.get("planner_notes", []),
        evaluation_verdict=evaluation_payload.get("verdict") if isinstance(evaluation_payload, dict) else None,
        memory_health=memory_health,
    )
    memory_record_artifact = None
    if memory_result_artifact := next(
        (
            artifact.get("path")
            for artifact in persisted_artifacts
            if isinstance(artifact, dict)
            and artifact.get("kind") == "json"
            and isinstance(artifact.get("path"), str)
            and artifact["path"].endswith(f"{run_id}.json")
        ),
        None,
    ):
        memory_record_artifact = memory_result_artifact
    review_path = _write_workflow_review_artifact(
        workflow_output_dir=workflow_output_dir,
        query=query,
        mode="execute_safe",
        resolution=resolution,
        route=route,
        candidate=candidate,
        summary=summary,
        artifact_paths={
            "goal_artifact": str(goal_dir / "goal.md"),
            "memory_artifact": str(memory_hits_path) if memory_hits_path else None,
            "plan_artifact": str(plan_path) if plan_path else None,
            "evaluation_artifact": str(evaluation_dir / "evaluation.json") if (evaluation_dir / "evaluation.json").exists() else None,
            "memory_record_artifact": memory_record_artifact,
        },
    )
    collection_index_path, collection_overview_path = _write_intelligence_workflow_collection_index(
        collection_root=workflow_output_dir.parent,
        entry={
            "command": "workflow",
            "status": workflow_status,
            "label": workflow_output_dir.name,
            "run_dir": str(workflow_output_dir),
            "landing_artifact": str(review_path),
            "review_artifact": str(review_path),
            "surface": effective_surface,
            "candidate_surface_id": candidate["id"] if candidate else None,
            "mode": "execute_safe",
            "evaluation_verdict": evaluation_payload.get("verdict") if isinstance(evaluation_payload, dict) else None,
        },
    )
    browser_index_path, browser_overview_path = ai_os_browser.write_ai_os_browser_index(
        browser_root=ai_os_browser.resolve_ai_os_browser_root(collection_root=workflow_output_dir.parent),
    )
    health_summary = ai_os_browser.load_ai_os_browser_health_summary(index_path=browser_index_path)
    health_index_path, health_overview_path = ai_os_browser.resolve_ai_os_health_paths(
        browser_root=browser_index_path.parent,
    )
    summary.update(
        {
            "review_artifact": str(review_path),
            "collection_index_artifact": str(collection_index_path),
            "collection_landing_artifact": str(collection_overview_path),
            "browser_index_artifact": str(browser_index_path),
            "browser_landing_artifact": str(browser_overview_path),
            "browser_health_index_artifact": str(health_index_path),
            "browser_health_landing_artifact": str(health_overview_path),
            "browser_health_summary": health_summary,
        }
    )
    for artifact in (
        {"kind": "markdown", "path": str(review_path)},
        {"kind": "json", "path": str(collection_index_path)},
        {"kind": "markdown", "path": str(collection_overview_path)},
        {"kind": "json", "path": str(browser_index_path)},
        {"kind": "markdown", "path": str(browser_overview_path)},
        {"kind": "json", "path": str(health_index_path)},
        {"kind": "markdown", "path": str(health_overview_path)},
    ):
        if artifact not in artifacts:
            artifacts.append(artifact)
    messages.extend(
        [
            make_message("info", "workflow_review_ready", f"Workflow review: {review_path}"),
            make_message(
                "info",
            "intelligence_workflow_collection_index_ready",
            f"Workflow collection overview: {collection_overview_path}",
        ),
        make_message("info", "ai_os_browser_ready", f"AI OS browser: {browser_overview_path}"),
        make_message("info", "ai_os_health_ready", f"AI OS health: {health_overview_path}"),
    ]
    )
    return make_result(
        status=workflow_status,
        command="workflow",
        messages=messages,
        artifacts=artifacts,
        workflow={
            "mode": "execute_safe",
            "resolution": resolution,
            "route": route,
            "review": {"gates": review["gates"]},
            "candidate_surface": candidate,
            "memory_hits": memory_hits,
            "memory_health": memory_health,
            "memory_record": memory_record_payload,
            "plan": plan,
            "surface_advisory": plan.get("surface_advisory"),
            "evaluation": evaluation_payload,
            "output_dir": str(workflow_output_dir),
            "summary": summary,
            "steps": steps,
        },
    )


def validate_profiles(inputs: dict[str, Any]) -> dict[str, Any]:
    messages: list[dict[str, str]] = []
    registry_lanes = {item["id"] for item in inputs["registry"]["lanes"]}
    registry_scripts = {item["path"] for item in inputs["registry"]["scripts"]}

    files_to_check = [
        PROFILES_PATH,
        REGISTRY_PATH,
        WIDGET_PROFILES_PATH,
        CONTEXT_REGISTRY_PATH,
        COMMERCIAL_MODEL_PATH,
        SALES_PROCESS_PATH,
        SURFACE_QUEUE_PATH,
    ]
    for path in files_to_check:
        if not path.exists():
            messages.append(
                make_message("error", "missing_file", f"Required file missing: {path.relative_to(ROOT)}")
            )

    for operation, config in inputs["profiles"]["operation_modes"].items():
        for lane in config["lane_sequence"]:
            if lane not in registry_lanes:
                messages.append(
                    make_message(
                        "error",
                        "unknown_lane_sequence",
                        f"Operation {operation} references unknown lane {lane}.",
                    )
                )
        for gate_id in config["review_gates"]:
            if gate_id not in inputs["profiles"]["review_gates"]:
                messages.append(
                    make_message(
                        "error",
                        "unknown_review_gate",
                        f"Operation {operation} references unknown review gate {gate_id}.",
                    )
                )

    for lane, scripts in inputs["profiles"]["lane_entrypoints"].items():
        if lane not in registry_lanes:
            messages.append(
                make_message(
                    "error",
                    "unknown_lane_entrypoint",
                    f"Lane entrypoint group references unknown lane {lane}.",
                )
            )
        for script in scripts:
            if script not in registry_scripts and script != "scripts/analytics_intelligence.py":
                messages.append(
                    make_message(
                        "error",
                        "unknown_entrypoint_script",
                        f"Lane {lane} references unregistered script {script}.",
                    )
                )

    for gate_id, config in inputs["profiles"]["review_gates"].items():
        for ref in config["refs"]:
            if not (ROOT / ref).exists():
                messages.append(
                    make_message(
                        "error",
                        "missing_review_ref",
                        f"Review gate {gate_id} references missing file {ref}.",
                    )
                )

    for script in inputs["registry"]["scripts"]:
        if "supports_json_output" in script and not isinstance(
            script["supports_json_output"], bool
        ):
            messages.append(
                make_message(
                    "error",
                    "invalid_supports_json_output",
                    f"{script['path']} must use a boolean supports_json_output value.",
                )
            )

    status = "error" if any(message["level"] == "error" for message in messages) else "ok"
    if status == "ok":
        messages.append(
            make_message("info", "profiles_valid", "Analytics intelligence profiles validated successfully.")
        )
    return make_result(
        status=status,
        command="validate",
        messages=messages,
        coverage={
            "files_checked": len(files_to_check),
            "review_gates": len(inputs["profiles"]["review_gates"]),
            "surface_types": len(inputs["profiles"]["surface_types"]),
        },
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory = subparsers.add_parser("inventory", help="List intelligence surface types and operation modes.")
    inventory.add_argument("--json", action="store_true", help="Print JSON output.")

    resolve = subparsers.add_parser("resolve", help="Resolve a business request into semantic context.")
    resolve.add_argument("--query", required=True, help="Free-form business request.")
    resolve.add_argument("--persona", default=None, help="Optional explicit persona override.")
    resolve.add_argument("--domain", default=None, help="Optional explicit domain override.")
    resolve.add_argument("--operation", default=None, help="Optional explicit operation override.")
    resolve.add_argument("--json", action="store_true", help="Print JSON output.")

    route = subparsers.add_parser("route", help="Route a request to a surface type and harness lanes.")
    route.add_argument("--query", required=True, help="Free-form business request.")
    route.add_argument("--persona", default=None, help="Optional explicit persona override.")
    route.add_argument("--domain", default=None, help="Optional explicit domain override.")
    route.add_argument("--operation", default=None, help="Optional explicit operation override.")
    route.add_argument("--json", action="store_true", help="Print JSON output.")

    review = subparsers.add_parser("review", help="Build a review checklist for a script or routed request.")
    review.add_argument("--script", default=None, help="Registered script path.")
    review.add_argument("--query", default=None, help="Free-form business request.")
    review.add_argument("--persona", default=None, help="Optional explicit persona override.")
    review.add_argument("--domain", default=None, help="Optional explicit domain override.")
    review.add_argument("--operation", default=None, help="Optional explicit operation override.")
    review.add_argument("--json", action="store_true", help="Print JSON output.")

    execute = subparsers.add_parser("execute", help="Execute a registered script behind typed safety checks.")
    execute.add_argument("--script", required=True, help="Registered script path.")
    execute.add_argument("--allow-mutating", action="store_true", help="Allow mutating scripts to run.")
    execute.add_argument("--allow-destructive", action="store_true", help="Allow destructive scripts to run.")
    execute.add_argument("--json", action="store_true", help="Print JSON output.")
    execute.add_argument("script_args", nargs=argparse.REMAINDER, help="Arguments passed to the script after '--'.")

    workflow = subparsers.add_parser("workflow", help="Plan or safely run a composite intelligence workflow.")
    workflow.add_argument("--query", required=True, help="Free-form business request.")
    workflow.add_argument("--persona", default=None, help="Optional explicit persona override.")
    workflow.add_argument("--domain", default=None, help="Optional explicit domain override.")
    workflow.add_argument("--operation", default=None, help="Optional explicit operation override.")
    workflow.add_argument(
        "--execute-safe",
        action="store_true",
        help="Run the safe CRMA workflow steps: export, contract lint, and audit when supported.",
    )
    workflow.add_argument(
        "--output-dir",
        default=None,
        help="Optional explicit workflow output directory.",
    )
    workflow.add_argument("--json", action="store_true", help="Print JSON output.")

    validate = subparsers.add_parser("validate", help="Validate intelligence profiles and references.")
    validate.add_argument("--json", action="store_true", help="Print JSON output.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    inputs = load_inputs()

    if args.command == "inventory":
        payload = build_inventory(inputs)
        if args.json:
            print_json(payload)
        else:
            print_inventory_text(payload)
        return 0

    if args.command == "resolve":
        payload = build_resolve_result(
            inputs,
            query=args.query,
            persona=args.persona,
            domain=args.domain,
            operation=args.operation,
        )
        if args.json:
            print_json(payload)
        else:
            print_resolve_text(payload)
        return 0

    if args.command == "route":
        payload = build_route_result(
            inputs,
            query=args.query,
            persona=args.persona,
            domain=args.domain,
            operation=args.operation,
        )
        if args.json:
            print_json(payload)
        else:
            print_route_text(payload)
        return 0

    if args.command == "review":
        payload = build_review_result(
            inputs,
            script_path=args.script,
            query=args.query,
            persona=args.persona,
            domain=args.domain,
            operation=args.operation,
        )
        if args.json:
            print_json(payload)
        else:
            print_review_text(payload)
        return 0 if payload["status"] == "ok" else 1

    if args.command == "execute":
        script_args = list(args.script_args)
        if script_args and script_args[0] == "--":
            script_args = script_args[1:]
        payload = execute_registered_script(
            inputs,
            script_path=args.script,
            script_args=script_args,
            allow_mutating=args.allow_mutating,
            allow_destructive=args.allow_destructive,
        )
        if args.json:
            print_json(payload)
        else:
            print_execute_text(payload)
        return 0 if payload["status"] == "ok" else 1

    if args.command == "workflow":
        payload = build_workflow(
            inputs,
            query=args.query,
            persona=args.persona,
            domain=args.domain,
            operation=args.operation,
            execute_safe=args.execute_safe,
            output_dir=args.output_dir,
        )
        if args.json:
            print_json(payload)
        else:
            print_workflow_text(payload)
        return 0 if payload["status"] == "ok" else 1

    payload = validate_profiles(inputs)
    if args.json:
        print_json(payload)
    else:
        print_validate_text(payload)
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
