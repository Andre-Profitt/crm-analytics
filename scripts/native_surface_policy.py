#!/usr/bin/env python3
"""Shared helpers for native dashboard/report mutation policy."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import executor_policy


def derive_native_surface_memory_context(
    *,
    build_package: dict[str, Any],
    planning_context: dict[str, Any] | None,
    evaluation_gate: dict[str, Any] | None,
    output_dir: Path | None,
    package_path: Path,
    command: str,
    default_goal_prefix: str,
    operation: str,
) -> dict[str, Any] | None:
    context = dict(planning_context or {})
    build_brief = build_package.get("build_brief")
    if not isinstance(build_brief, dict):
        build_brief = {}
    surface_contract = build_package.get("surface_contract")
    if not isinstance(surface_contract, dict):
        surface_contract = {}

    if "goal" not in context:
        decision_statement = build_brief.get("decision_statement")
        if isinstance(decision_statement, str) and decision_statement:
            context["goal"] = decision_statement
        else:
            context["goal"] = f"{default_goal_prefix} {command}"
    if "persona" not in context:
        persona = build_brief.get("persona")
        if isinstance(persona, str) and persona:
            context["persona"] = persona
    if "domain" not in context:
        domain = build_brief.get("domain")
        if isinstance(domain, str) and domain:
            context["domain"] = domain
    context.setdefault("operation", operation)
    surface_type = surface_contract.get("surface_type")
    if isinstance(surface_type, str) and surface_type:
        context.setdefault("surface_type", surface_type)
    handoff_target = surface_contract.get("handoff_target")
    if isinstance(handoff_target, dict):
        target_surface_id = handoff_target.get("target_surface_id")
        if isinstance(target_surface_id, str) and target_surface_id:
            context.setdefault("candidate_surface_id", target_surface_id)
    if "run_id" not in context:
        candidate_run_id = None
        if isinstance(evaluation_gate, dict):
            candidate_run_id = evaluation_gate.get("run_id")
        if not isinstance(candidate_run_id, str) or not candidate_run_id:
            candidate_run_id = output_dir.name if output_dir is not None else None
        if not isinstance(candidate_run_id, str) or not candidate_run_id:
            candidate_run_id = f"{package_path.stem}_{command}"
        context["run_id"] = candidate_run_id

    if not isinstance(context.get("goal"), str) or not context.get("goal"):
        return None
    if not isinstance(context.get("run_id"), str) or not context.get("run_id"):
        return None
    return context


def resolve_native_surface_evaluation_gate(
    *,
    build_package: dict[str, Any],
    evaluation_path: Path | None,
    require_pass: bool,
    allow_missing: bool,
    make_message: Callable[[str, str, str], dict[str, str]],
    surface_name: str,
) -> tuple[dict[str, Any] | None, list[dict[str, str]], dict[str, str] | None]:
    planning_context = build_package.get("planning_context")
    if not isinstance(planning_context, dict):
        planning_context = {}

    gate_messages: list[dict[str, str]] = []
    resolved_path = evaluation_path
    source = "cli"
    if resolved_path is None and require_pass:
        planning_path = planning_context.get("evaluation_path")
        if isinstance(planning_path, str) and planning_path:
            resolved_path = Path(planning_path)
            source = "package_planning_context"

    evaluation_gate = None
    if resolved_path is not None:
        try:
            evaluation_gate = executor_policy.load_evaluation_gate(resolved_path)
        except Exception as exc:
            return None, [], {"code": "evaluation_invalid", "text": str(exc)}
        evaluation_gate["source"] = source

    if not require_pass:
        return evaluation_gate, gate_messages, None

    if evaluation_gate is None:
        if allow_missing:
            evaluation_gate = {
                "path": None,
                "verdict": planning_context.get("evaluation_verdict"),
                "run_id": planning_context.get("run_id"),
                "bypassed": True,
                "source": "bypass",
            }
            gate_messages.append(
                make_message(
                    "warn",
                    "evaluation_bypass_used",
                    (
                        f"Continuing native {surface_name} mutation without a pass evaluator verdict because "
                        "--allow-missing-evaluation was provided."
                    ),
                )
            )
            return evaluation_gate, gate_messages, None
        return None, [], {
            "code": "evaluation_required",
            "text": (
                f"Live {surface_name} mutation requires --evaluation with verdict pass, "
                "a package planning_context.evaluation_path, or --allow-missing-evaluation."
            ),
        }

    if evaluation_gate.get("verdict") == "pass":
        return evaluation_gate, gate_messages, None

    if allow_missing:
        evaluation_gate["bypassed"] = True
        gate_messages.append(
            make_message(
                "warn",
                "evaluation_bypass_used",
                (
                    f"Continuing native {surface_name} mutation despite evaluator verdict "
                    f"{evaluation_gate.get('verdict')} because --allow-missing-evaluation was provided."
                ),
            )
        )
        return evaluation_gate, gate_messages, None

    return None, [], {
        "code": "evaluation_not_pass",
        "text": f"Live {surface_name} mutation requires evaluator verdict pass; received {evaluation_gate.get('verdict')}.",
    }
