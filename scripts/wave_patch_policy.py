#!/usr/bin/env python3
"""Shared Wave PATCH policy helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import executor_policy


def derive_wave_memory_context(
    *,
    planning_context: dict[str, Any] | None,
    payload: dict[str, Any] | None = None,
    evaluation_gate: dict[str, Any] | None = None,
    deploy_target: dict[str, Any] | None = None,
    output_dir: Path | None = None,
    state_path: Path | None = None,
    command: str,
) -> dict[str, Any] | None:
    context = dict(planning_context or {})
    target_surface = payload.get("target_surface") if isinstance(payload, dict) else None
    if not isinstance(target_surface, dict):
        target_surface = {}
    if not isinstance(deploy_target, dict):
        deploy_target = {}

    candidate_labels = target_surface.get("candidate_surface_labels")
    if isinstance(candidate_labels, list):
        first_label = next((item for item in candidate_labels if isinstance(item, str) and item), None)
    else:
        first_label = None
    candidate_surface_id = target_surface.get("candidate_surface_id")
    deploy_label = deploy_target.get("dashboard_label")
    deploy_id = deploy_target.get("dashboard_id")

    preferred_goal: str
    preferred_goal_rank: int
    if isinstance(first_label, str) and first_label:
        preferred_goal = f"Wave PATCH for {first_label}"
        preferred_goal_rank = 4
    elif isinstance(deploy_label, str) and deploy_label:
        preferred_goal = f"Wave PATCH for {deploy_label}"
        preferred_goal_rank = 4
    elif isinstance(candidate_surface_id, str) and candidate_surface_id:
        preferred_goal = f"Wave PATCH for {candidate_surface_id}"
        preferred_goal_rank = 3
    elif isinstance(deploy_id, str) and deploy_id:
        preferred_goal = f"Wave PATCH for dashboard {deploy_id}"
        preferred_goal_rank = 2
    else:
        preferred_goal = f"Wave PATCH {command}"
        preferred_goal_rank = 1

    existing_goal = context.get("goal")
    if isinstance(existing_goal, str) and existing_goal:
        if isinstance(first_label, str) and existing_goal == f"Wave PATCH for {first_label}":
            existing_goal_rank = 4
        elif isinstance(deploy_label, str) and existing_goal == f"Wave PATCH for {deploy_label}":
            existing_goal_rank = 4
        elif isinstance(candidate_surface_id, str) and existing_goal == f"Wave PATCH for {candidate_surface_id}":
            existing_goal_rank = 3
        elif isinstance(deploy_id, str) and existing_goal == f"Wave PATCH for dashboard {deploy_id}":
            existing_goal_rank = 2
        elif existing_goal == f"Wave PATCH {command}":
            existing_goal_rank = 1
        else:
            existing_goal_rank = 99
    else:
        existing_goal_rank = 0

    if existing_goal_rank < preferred_goal_rank:
        context["goal"] = preferred_goal
    context.setdefault("operation", "mutate_dashboard")
    surface_type = target_surface.get("surface_type")
    if isinstance(surface_type, str) and surface_type:
        context.setdefault("surface_type", surface_type)
    else:
        context.setdefault("surface_type", "crma_dashboard")
    if isinstance(candidate_surface_id, str) and candidate_surface_id:
        context.setdefault("candidate_surface_id", candidate_surface_id)
    elif isinstance(deploy_target.get("dashboard_id"), str) and deploy_target.get("dashboard_id"):
        context.setdefault("candidate_surface_id", deploy_target["dashboard_id"])
    if "run_id" not in context:
        candidate_run_id = None
        if isinstance(evaluation_gate, dict):
            candidate_run_id = evaluation_gate.get("run_id")
        if not isinstance(candidate_run_id, str) or not candidate_run_id:
            candidate_run_id = output_dir.name if output_dir is not None else None
        if not isinstance(candidate_run_id, str) or not candidate_run_id:
            candidate_run_id = state_path.parent.name if state_path is not None and state_path.parent.name else None
        if not isinstance(candidate_run_id, str) or not candidate_run_id:
            candidate_run_id = f"wave_patch_{command}"
        context["run_id"] = candidate_run_id

    if not isinstance(context.get("goal"), str) or not context.get("goal"):
        return None
    if not isinstance(context.get("run_id"), str) or not context.get("run_id"):
        return None
    return context


def resolve_wave_deploy_evaluation_gate(
    *,
    evaluation_path: Path | None,
    require_pass: bool,
    allow_missing: bool,
    make_message: Callable[[str, str, str], dict[str, str]],
) -> tuple[dict[str, Any] | None, list[dict[str, str]], dict[str, str] | None]:
    gate_messages: list[dict[str, str]] = []
    evaluation_gate = None
    if evaluation_path is not None:
        try:
            evaluation_gate = executor_policy.load_evaluation_gate(evaluation_path)
        except Exception as exc:
            return None, [], {"code": "evaluation_invalid", "text": str(exc)}

    if not require_pass:
        return evaluation_gate, gate_messages, None

    if evaluation_gate is None:
        if allow_missing:
            evaluation_gate = {"path": None, "verdict": None, "run_id": None, "bypassed": True}
            gate_messages.append(
                make_message(
                    "warn",
                    "evaluation_bypass_used",
                    "Continuing live Wave PATCH without a pass evaluator verdict because --allow-missing-evaluation was provided.",
                )
            )
            return evaluation_gate, gate_messages, None
        return None, [], {
            "code": "evaluation_required",
            "text": "Live Wave PATCH apply requires --evaluation with verdict pass, or --allow-missing-evaluation.",
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
                    "Continuing live Wave PATCH despite evaluator verdict "
                    f"{evaluation_gate.get('verdict')} because --allow-missing-evaluation was provided."
                ),
            )
        )
        return evaluation_gate, gate_messages, None

    return evaluation_gate, [], {
        "code": "evaluation_not_pass",
        "text": f"Live Wave PATCH apply requires evaluator verdict pass; received {evaluation_gate.get('verdict')}.",
    }
