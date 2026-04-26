#!/usr/bin/env python3
"""Shared evaluation-policy helpers for live mutation executors."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import run_memory


def apply_policy_exceptions(payload: dict[str, Any]) -> dict[str, Any]:
    if "policy_exceptions" not in payload:
        evaluation_gate = payload.get("evaluation_gate")
        if isinstance(evaluation_gate, dict) and evaluation_gate.get("bypassed"):
            payload["policy_exceptions"] = ["evaluation_bypass"]
    return payload


def load_evaluation_gate(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    evaluation = payload.get("evaluation")
    if isinstance(evaluation, dict):
        payload = evaluation
    verdict = payload.get("verdict")
    if not isinstance(verdict, str) or not verdict:
        raise ValueError(f"{path}: expected evaluation verdict")
    return {
        "path": str(path),
        "verdict": verdict,
        "run_id": payload.get("run_id"),
        "bypassed": False,
    }


def append_evaluation_bypass_artifact(
    *,
    output_dir: Path | None,
    artifacts: list[dict[str, str]],
    command: str,
    target_org: str | None,
    evaluation_gate: dict[str, Any] | None,
    summary: dict[str, Any],
) -> None:
    if output_dir is None or not evaluation_gate or not evaluation_gate.get("bypassed"):
        return
    bypass_path = output_dir / "evaluation_bypass_audit.json"
    bypass_payload = {
        "artifact_type": "evaluation_bypass_audit",
        "command": command,
        "target_org": target_org,
        "evaluation_gate": evaluation_gate,
        "summary": summary,
        "policy_exceptions": ["evaluation_bypass"],
    }
    bypass_path.write_text(json.dumps(bypass_payload, indent=2), encoding="utf-8")
    artifacts.append({"type": "evaluation_bypass_audit", "path": str(bypass_path)})


def attach_memory_record(
    *,
    result: dict[str, Any],
    planning_context: dict[str, Any] | None,
    command: str,
    evaluation_gate: dict[str, Any] | None,
    script_path: str,
    make_message,
    extra_tags: list[str] | None = None,
) -> dict[str, Any]:
    memory_result, memory_exit_code = run_memory.record_executor_outcome(
        planning_context=planning_context,
        script_path=script_path,
        command=command,
        status=result.get("status", "error"),
        messages=result.get("messages"),
        artifacts=result.get("artifacts"),
        evaluation_gate=evaluation_gate,
        extra_tags=extra_tags,
    )
    if memory_result is None:
        return result

    artifacts = result.setdefault("artifacts", [])
    messages = result.setdefault("messages", [])
    if memory_exit_code == 0:
        result["memory_record"] = memory_result.get("record")
        for artifact in memory_result.get("artifacts", []):
            if artifact not in artifacts:
                artifacts.append(artifact)
        return result

    memory_messages = memory_result.get("messages") or []
    if memory_messages and isinstance(memory_messages[0], dict):
        messages.append(
            make_message(
                "warn",
                "workflow_memory_record_failed",
                str(memory_messages[0].get("text") or "Unable to record executor memory."),
            )
        )
    return result
