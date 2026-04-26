#!/usr/bin/env python3
"""File-backed run memory for the harness-first AI OS."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MEMORY_ROOT = Path(os.environ.get("CRM_AI_MEMORY_ROOT", ROOT / "output" / "agent_memory")).expanduser()
INDEX_PATH = MEMORY_ROOT / "run_index.jsonl"
RUNS_DIR = MEMORY_ROOT / "runs"
VALID_VERDICTS = {"pass", "fail", "needs_more_evidence"}


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


GENERIC_GOAL_TEXTS = {
    normalize_text("Wave PATCH bundle"),
    normalize_text("Wave PATCH deploy"),
    normalize_text("Wave PATCH validate"),
    normalize_text("Wave PATCH worklist"),
    normalize_text("execute salesforce dashboard apply"),
    normalize_text("execute salesforce dashboard complete"),
    normalize_text("execute salesforce report apply"),
    normalize_text("execute salesforce report complete"),
}


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
        "tool": "run_memory",
        "lane": "intelligence_control",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return payload


def ensure_memory_layout() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not INDEX_PATH.exists():
        INDEX_PATH.write_text("", encoding="utf-8")


def _load_index_records() -> list[dict[str, Any]]:
    ensure_memory_layout()
    records: list[dict[str, Any]] = []
    for line in INDEX_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _write_index_records(records: list[dict[str, Any]]) -> None:
    ensure_memory_layout()
    INDEX_PATH.write_text(
        "".join(f"{json.dumps(record, sort_keys=True)}\n" for record in records),
        encoding="utf-8",
    )


def _record_path(run_id: str) -> Path:
    return RUNS_DIR / f"{run_id}.json"


def _has_duplicates(values: list[str]) -> bool:
    return len(values) != len(set(values))


def _normalized_values(values: list[str] | None) -> set[str]:
    normalized: set[str] = set()
    for value in values or []:
        if not isinstance(value, str):
            continue
        text = normalize_text(value)
        if text:
            normalized.add(text)
    return normalized


def _merge_unique_values(existing: list[str] | None, incoming: list[str] | None) -> list[str]:
    merged: list[str] = []
    for value in [*(existing or []), *(incoming or [])]:
        if not isinstance(value, str) or not value or value in merged:
            continue
        merged.append(value)
    return merged


def _persist_record(record: dict[str, Any]) -> Path:
    ensure_memory_layout()
    record_path = _record_path(str(record["run_id"]))
    record_path.write_text(json.dumps(record, indent=2), encoding="utf-8")

    records = [item for item in _load_index_records() if item.get("run_id") != record.get("run_id")]
    records.append(record)
    records.sort(key=lambda item: item["run_id"])
    _write_index_records(records)
    return record_path


def _is_generic_goal(goal: str | None) -> bool:
    if not isinstance(goal, str) or not goal:
        return False
    return normalize_text(goal) in GENERIC_GOAL_TEXTS


def _record_summary(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": record.get("run_id"),
        "goal": record.get("goal"),
        "verdict": record.get("verdict"),
        "outcome": record.get("outcome"),
        "policy_exceptions": record.get("policy_exceptions") or [],
    }


def _score_record(
    record: dict[str, Any],
    *,
    goal: str,
    persona: str | None,
    domain: str | None,
    operation: str | None,
    tags: list[str] | None,
    evidence_types: list[str] | None,
    failure_reason: str | None,
) -> int:
    query_tokens = set(tokenize(goal))
    record_tokens = set(tokenize(record.get("goal", "")))
    score = len(query_tokens & record_tokens) * 2

    normalized_goal = normalize_text(goal)
    record_goal = normalize_text(record.get("goal", ""))
    if normalized_goal and normalized_goal in record_goal:
        score += 3

    if domain and record.get("domain") == domain:
        score += 4
    if persona and record.get("persona") == persona:
        score += 3
    if operation and record.get("operation") == operation:
        score += 4

    query_tags = _normalized_values(tags) or query_tokens
    record_tags = _normalized_values(record.get("tags", []))
    tag_overlap = len(query_tags & record_tags)
    score += tag_overlap * 2
    if tags and record_tags and tag_overlap == 0:
        score -= max(2, len(query_tags))

    requested_evidence = _normalized_values(evidence_types)
    record_evidence = _normalized_values(record.get("evidence_types", []))
    evidence_overlap = len(requested_evidence & record_evidence)
    score += evidence_overlap * 3
    if evidence_types and record_evidence and evidence_overlap == 0:
        score -= 2

    verdict = record.get("verdict")
    if verdict == "pass":
        score += 4
    elif verdict == "fail":
        score -= 3
    elif verdict == "needs_more_evidence":
        score -= 1

    outcome = normalize_text(record.get("outcome") or "")
    if outcome in {"patch applied", "workflow ok"}:
        score += 1

    if failure_reason and normalize_text(record.get("failure_reason") or "") == normalize_text(failure_reason):
        score += 3

    return score


def _missing_context_fields(record: dict[str, Any]) -> list[str]:
    return [
        field
        for field in ("domain", "operation")
        if not isinstance(record.get(field), str) or not str(record.get(field)).strip()
    ]


def _rank_runs(
    *,
    goal: str,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
    tags: list[str] | None = None,
    evidence_types: list[str] | None = None,
    failure_reason: str | None = None,
    include_policy_exceptions: bool = False,
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for record in _load_index_records():
        if not include_policy_exceptions and record.get("policy_exceptions"):
            continue
        score = _score_record(
            record,
            goal=goal,
            persona=persona,
            domain=domain,
            operation=operation,
            tags=tags,
            evidence_types=evidence_types,
            failure_reason=failure_reason,
        )
        if score <= 0:
            continue
        ranked.append({**record, "score": score})
    ranked.sort(key=lambda item: (-item["score"], item["run_id"]))
    return ranked


def search_runs(
    *,
    goal: str,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
    top_k: int = 5,
    tags: list[str] | None = None,
    evidence_types: list[str] | None = None,
    failure_reason: str | None = None,
    include_policy_exceptions: bool = False,
) -> list[dict[str, Any]]:
    ranked = _rank_runs(
        goal=goal,
        persona=persona,
        domain=domain,
        operation=operation,
        tags=tags,
        evidence_types=evidence_types,
        failure_reason=failure_reason,
        include_policy_exceptions=include_policy_exceptions,
    )
    return ranked[:top_k]


def summarize_search_health(
    *,
    goal: str,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
    tags: list[str] | None = None,
    evidence_types: list[str] | None = None,
    failure_reason: str | None = None,
    sample_limit: int = 5,
) -> dict[str, Any]:
    included = _rank_runs(
        goal=goal,
        persona=persona,
        domain=domain,
        operation=operation,
        tags=tags,
        evidence_types=evidence_types,
        failure_reason=failure_reason,
        include_policy_exceptions=False,
    )
    all_ranked = _rank_runs(
        goal=goal,
        persona=persona,
        domain=domain,
        operation=operation,
        tags=tags,
        evidence_types=evidence_types,
        failure_reason=failure_reason,
        include_policy_exceptions=True,
    )
    included_ids = {item["run_id"] for item in included if isinstance(item.get("run_id"), str)}
    excluded_policy_exception_runs = [
        _record_summary(item)
        for item in all_ranked
        if item.get("run_id") not in included_ids and item.get("policy_exceptions")
    ]
    included_fail_count = sum(1 for item in included if item.get("verdict") == "fail")
    included_needs_more_evidence_count = sum(
        1 for item in included if item.get("verdict") == "needs_more_evidence"
    )
    included_generic_goal_count = sum(1 for item in included if _is_generic_goal(item.get("goal")))
    included_missing_context_count = sum(1 for item in included if _missing_context_fields(item))
    return {
        "considered_hits": len(included),
        "policy_exception_hits_excluded": len(excluded_policy_exception_runs),
        "excluded_policy_exception_runs": excluded_policy_exception_runs[:sample_limit],
        "included_fail_count": included_fail_count,
        "included_needs_more_evidence_count": included_needs_more_evidence_count,
        "included_generic_goal_count": included_generic_goal_count,
        "included_missing_context_count": included_missing_context_count,
    }


def record_run(
    *,
    run_id: str,
    goal: str,
    verdict: str,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
    sequence: list[str] | None = None,
    outcome: str | None = None,
    failure_reason: str | None = None,
    artifacts: list[str] | None = None,
    tags: list[str] | None = None,
    evidence_types: list[str] | None = None,
    policy_exceptions: list[str] | None = None,
    operator_notes: list[str] | None = None,
) -> tuple[dict[str, Any], int]:
    sequence = sequence or []
    artifacts = artifacts or []
    tags = tags or []
    evidence_types = evidence_types or []
    policy_exceptions = policy_exceptions or []
    operator_notes = operator_notes or []

    if not run_id:
        return (
            make_result(
                status="error",
                command="record",
                messages=[make_message("error", "missing_run_id", "run_id is required.")],
            ),
            1,
        )
    if not goal:
        return (
            make_result(
                status="error",
                command="record",
                messages=[make_message("error", "missing_goal", "goal is required.")],
            ),
            1,
        )
    if verdict not in VALID_VERDICTS:
        return (
            make_result(
                status="error",
                command="record",
                messages=[
                    make_message(
                        "error",
                        "invalid_verdict",
                        f"verdict must be one of {sorted(VALID_VERDICTS)}.",
                    )
                ],
            ),
            1,
        )
    if _has_duplicates(sequence):
        return (
            make_result(
                status="error",
                command="record",
                messages=[
                    make_message(
                        "error",
                        "duplicate_sequence_entry",
                        "sequence contains duplicate script paths.",
                    )
                ],
            ),
            1,
        )
    if _has_duplicates(artifacts):
        return (
            make_result(
                status="error",
                command="record",
                messages=[
                    make_message(
                        "error",
                        "duplicate_artifact",
                        "artifacts contains duplicate paths.",
                    )
                ],
            ),
            1,
        )
    if _has_duplicates(tags):
        return (
            make_result(
                status="error",
                command="record",
                messages=[
                    make_message(
                        "error",
                        "duplicate_tag",
                        "tags contains duplicate values.",
                    )
                ],
            ),
            1,
        )
    if _has_duplicates(evidence_types):
        return (
            make_result(
                status="error",
                command="record",
                messages=[
                    make_message(
                        "error",
                        "duplicate_evidence_type",
                        "evidence_types contains duplicate values.",
                    )
                ],
            ),
            1,
        )
    if _has_duplicates(policy_exceptions):
        return (
            make_result(
                status="error",
                command="record",
                messages=[
                    make_message(
                        "error",
                        "duplicate_policy_exception",
                        "policy_exceptions contains duplicate values.",
                    )
                ],
            ),
            1,
        )
    if _has_duplicates(operator_notes):
        return (
            make_result(
                status="error",
                command="record",
                messages=[
                    make_message(
                        "error",
                        "duplicate_operator_note",
                        "operator_notes contains duplicate values.",
                    )
                ],
            ),
            1,
        )

    ensure_memory_layout()
    existing_record: dict[str, Any] = {}
    record_path = _record_path(run_id)
    if record_path.exists():
        payload = json.loads(record_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            existing_record = payload

    effective_failure_reason = failure_reason
    if effective_failure_reason is None and verdict == "pass":
        effective_failure_reason = None
    elif effective_failure_reason is None:
        effective_failure_reason = existing_record.get("failure_reason")

    record = {
        "run_id": run_id,
        "goal": goal,
        "persona": persona if persona is not None else existing_record.get("persona"),
        "domain": domain if domain is not None else existing_record.get("domain"),
        "operation": operation if operation is not None else existing_record.get("operation"),
        "sequence": _merge_unique_values(existing_record.get("sequence"), sequence),
        "verdict": verdict,
        "outcome": outcome if outcome is not None else existing_record.get("outcome"),
        "failure_reason": effective_failure_reason,
        "artifacts": _merge_unique_values(existing_record.get("artifacts"), artifacts),
        "tags": _merge_unique_values(existing_record.get("tags"), tags),
        "evidence_types": _merge_unique_values(existing_record.get("evidence_types"), evidence_types),
        "policy_exceptions": _merge_unique_values(existing_record.get("policy_exceptions"), policy_exceptions),
        "operator_notes": _merge_unique_values(existing_record.get("operator_notes"), operator_notes),
    }
    record_path = _persist_record(record)

    payload = make_result(
        status="ok",
        command="record",
        messages=[make_message("info", "memory_recorded", f"Recorded run {run_id}.")],
        artifacts=[{"type": "memory_record", "path": str(record_path)}],
        record={"run_id": run_id, "goal": goal, "verdict": verdict},
    )
    return payload, 0


def record_executor_outcome(
    *,
    planning_context: dict[str, Any] | None,
    script_path: str,
    command: str,
    status: str,
    messages: list[dict[str, Any]] | None,
    artifacts: list[dict[str, Any]] | None,
    evaluation_gate: dict[str, Any] | None = None,
    extra_tags: list[str] | None = None,
) -> tuple[dict[str, Any] | None, int]:
    if not isinstance(planning_context, dict):
        return None, 0

    run_id = planning_context.get("run_id")
    goal = planning_context.get("goal")
    if not isinstance(run_id, str) or not run_id or not isinstance(goal, str) or not goal:
        return None, 0

    verdict = "pass"
    if status == "warn":
        verdict = "needs_more_evidence"
    elif status == "error":
        verdict = "fail"

    failure_reason = None
    if verdict != "pass":
        for message in messages or []:
            if not isinstance(message, dict):
                continue
            level = message.get("level")
            code = message.get("code")
            if level in {"warn", "error"} and isinstance(code, str) and code:
                failure_reason = code
                break

    artifact_paths: list[str] = []
    for artifact in artifacts or []:
        if not isinstance(artifact, dict):
            continue
        path = artifact.get("path")
        if isinstance(path, str) and path and path not in artifact_paths:
            artifact_paths.append(path)

    tags = _merge_unique_values(
        [
            planning_context.get("surface_type"),
            planning_context.get("candidate_surface_id"),
            Path(script_path).stem,
            command,
        ],
        extra_tags,
    )
    policy_exceptions = ["evaluation_bypass"] if isinstance(evaluation_gate, dict) and evaluation_gate.get("bypassed") else []
    outcome_suffix = "ok" if status == "ok" else ("warn" if status == "warn" else "error")

    payload, exit_code = record_run(
        run_id=run_id,
        goal=goal,
        persona=planning_context.get("persona") if isinstance(planning_context.get("persona"), str) else None,
        domain=planning_context.get("domain") if isinstance(planning_context.get("domain"), str) else None,
        operation=planning_context.get("operation") if isinstance(planning_context.get("operation"), str) else None,
        sequence=[script_path],
        verdict=verdict,
        outcome=f"{Path(script_path).stem}_{command}_{outcome_suffix}",
        failure_reason=failure_reason,
        artifacts=artifact_paths,
        tags=tags,
        evidence_types=[
            item
            for item in (planning_context.get("required_evidence") or [])
            if isinstance(item, str)
        ],
        policy_exceptions=policy_exceptions,
    )
    return payload, exit_code


def audit_runs(*, limit: int = 10) -> dict[str, Any]:
    records = _load_index_records()
    verdict_counts = {verdict: 0 for verdict in sorted(VALID_VERDICTS)}
    policy_exception_counts: dict[str, int] = {}
    failure_reason_counts: dict[str, int] = {}
    runs_with_policy_exceptions: list[dict[str, Any]] = []
    runs_missing_context: list[dict[str, Any]] = []
    generic_goal_runs: list[dict[str, Any]] = []

    for record in sorted(records, key=lambda item: str(item.get("run_id", ""))):
        verdict = record.get("verdict")
        if verdict in verdict_counts:
            verdict_counts[str(verdict)] += 1

        for exception in record.get("policy_exceptions") or []:
            if isinstance(exception, str) and exception:
                policy_exception_counts[exception] = policy_exception_counts.get(exception, 0) + 1

        failure_reason = record.get("failure_reason")
        if isinstance(failure_reason, str) and failure_reason:
            failure_reason_counts[failure_reason] = failure_reason_counts.get(failure_reason, 0) + 1

        if record.get("policy_exceptions"):
            runs_with_policy_exceptions.append(_record_summary(record))

        missing_fields = _missing_context_fields(record)
        if missing_fields:
            runs_missing_context.append({**_record_summary(record), "missing_fields": missing_fields})

        if _is_generic_goal(record.get("goal")):
            generic_goal_runs.append(_record_summary(record))

    top_failure_reasons = [
        {"failure_reason": reason, "count": count}
        for reason, count in sorted(failure_reason_counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]
    return make_result(
        status="ok",
        command="audit",
        messages=[
            make_message(
                "info",
                "memory_audit_ready",
                f"Audited {len(records)} stored run(s).",
            )
        ],
        audit={
            "total_runs": len(records),
            "verdict_counts": verdict_counts,
            "policy_exception_counts": dict(
                sorted(policy_exception_counts.items(), key=lambda item: (-item[1], item[0]))
            ),
            "top_failure_reasons": top_failure_reasons,
            "runs_with_policy_exceptions": runs_with_policy_exceptions[:limit],
            "runs_missing_context": runs_missing_context[:limit],
            "generic_goal_runs": generic_goal_runs[:limit],
        },
    )


def quarantine_run(
    *,
    run_id: str,
    policy_exception: str = "memory_quarantine",
    notes: list[str] | None = None,
) -> tuple[dict[str, Any], int]:
    record_path = _record_path(run_id)
    if not record_path.exists():
        return (
            make_result(
                status="error",
                command="quarantine",
                messages=[
                    make_message(
                        "error",
                        "unknown_run_id",
                        f"No stored run was found for {run_id}.",
                    )
                ],
            ),
            1,
        )
    if not policy_exception:
        return (
            make_result(
                status="error",
                command="quarantine",
                messages=[
                    make_message(
                        "error",
                        "missing_policy_exception",
                        "policy_exception is required.",
                    )
                ],
            ),
            1,
        )

    notes = notes or []
    if _has_duplicates(notes):
        return (
            make_result(
                status="error",
                command="quarantine",
                messages=[
                    make_message(
                        "error",
                        "duplicate_operator_note",
                        "notes contains duplicate values.",
                    )
                ],
            ),
            1,
        )

    record = json.loads(record_path.read_text(encoding="utf-8"))
    if not isinstance(record, dict):
        return (
            make_result(
                status="error",
                command="quarantine",
                messages=[make_message("error", "invalid_record", f"Stored run {run_id} is not a valid object.")],
            ),
            1,
        )

    record["policy_exceptions"] = _merge_unique_values(record.get("policy_exceptions"), [policy_exception])
    record["operator_notes"] = _merge_unique_values(record.get("operator_notes"), notes)
    persisted_path = _persist_record(record)
    payload = make_result(
        status="ok",
        command="quarantine",
        messages=[
            make_message(
                "info",
                "run_quarantined",
                f"Quarantined run {run_id} with policy exception {policy_exception}.",
            )
        ],
        artifacts=[{"type": "memory_record", "path": str(persisted_path)}],
        record=_record_summary(record),
    )
    return payload, 0


def show_run(run_id: str) -> tuple[dict[str, Any], int]:
    record_path = _record_path(run_id)
    if not record_path.exists():
        return (
            make_result(
                status="error",
                command="show",
                messages=[
                    make_message(
                        "error",
                        "unknown_run_id",
                        f"No stored run was found for {run_id}.",
                    )
                ],
            ),
            1,
        )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    payload = make_result(
        status="ok",
        command="show",
        messages=[],
        artifacts=[{"type": "memory_record", "path": str(record_path)}],
        record=record,
    )
    return payload, 0


def build_search_result(
    *,
    goal: str,
    persona: str | None = None,
    domain: str | None = None,
    operation: str | None = None,
    top_k: int = 5,
    tags: list[str] | None = None,
    evidence_types: list[str] | None = None,
    failure_reason: str | None = None,
    include_policy_exceptions: bool = False,
) -> dict[str, Any]:
    similar_runs = search_runs(
        goal=goal,
        persona=persona,
        domain=domain,
        operation=operation,
        top_k=top_k,
        tags=tags,
        evidence_types=evidence_types,
        failure_reason=failure_reason,
        include_policy_exceptions=include_policy_exceptions,
    )
    memory_health = summarize_search_health(
        goal=goal,
        persona=persona,
        domain=domain,
        operation=operation,
        tags=tags,
        evidence_types=evidence_types,
        failure_reason=failure_reason,
    )
    return make_result(
        status="ok",
        command="search",
        messages=[
            make_message(
                "info",
                "memory_hits_ready",
                f"Found {len(similar_runs)} similar prior run(s).",
            )
        ],
        query={
            "goal": goal,
            "persona": persona,
            "domain": domain,
            "operation": operation,
            "tags": tags or [],
            "evidence_types": evidence_types or [],
            "failure_reason": failure_reason,
            "include_policy_exceptions": include_policy_exceptions,
        },
        similar_runs=similar_runs,
        memory_health=memory_health,
    )


def print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2))


def print_text(payload: dict[str, Any]) -> None:
    if payload["command"] == "search":
        print(payload["messages"][0]["text"])
        memory_health = payload.get("memory_health") or {}
        excluded_hits = memory_health.get("policy_exception_hits_excluded")
        if isinstance(excluded_hits, int) and excluded_hits > 0:
            print(f"excluded_policy_exception_hits: {excluded_hits}")
        for item in payload["similar_runs"]:
            print(
                f"- {item['run_id']} score={item['score']} verdict={item['verdict']} :: {item['goal']}"
            )
        return
    if payload["command"] == "audit":
        audit = payload["audit"]
        print(payload["messages"][0]["text"])
        print(f"total_runs: {audit['total_runs']}")
        print(f"verdict_counts: {json.dumps(audit['verdict_counts'], sort_keys=True)}")
        print(f"policy_exception_counts: {json.dumps(audit['policy_exception_counts'], sort_keys=True)}")
        print(f"generic_goal_runs: {len(audit['generic_goal_runs'])}")
        print(f"runs_missing_context: {len(audit['runs_missing_context'])}")
        return
    if payload["command"] == "record":
        print(payload["messages"][0]["text"])
        return
    if payload["command"] == "quarantine":
        print(payload["messages"][0]["text"])
        return
    if payload["status"] == "error":
        print(payload["messages"][0]["text"], file=sys.stderr)
        return
    print(payload["record"]["run_id"])
    print(f"verdict: {payload['record']['verdict']}")
    print(payload["record"]["goal"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    search = subparsers.add_parser("search", help="Find similar prior runs.")
    search.add_argument("--goal", required=True, help="Free-form goal text.")
    search.add_argument("--persona", default=None, help="Optional persona filter.")
    search.add_argument("--domain", default=None, help="Optional domain filter.")
    search.add_argument("--operation", default=None, help="Optional operation filter.")
    search.add_argument("--tag", action="append", default=[], help="Optional retrieval tag.")
    search.add_argument("--evidence-type", action="append", default=[], help="Optional evidence type.")
    search.add_argument("--failure-reason", default=None, help="Optional failure code to match.")
    search.add_argument(
        "--include-policy-exceptions",
        action="store_true",
        help="Include runs that carried policy exceptions such as evaluator bypasses.",
    )
    search.add_argument("--top-k", type=int, default=5, help="Maximum number of hits to return.")
    search.add_argument("--json", action="store_true", help="Print JSON output.")

    audit = subparsers.add_parser("audit", help="Summarize memory health, bypasses, and weak records.")
    audit.add_argument("--limit", type=int, default=10, help="Maximum number of flagged runs to return per category.")
    audit.add_argument("--json", action="store_true", help="Print JSON output.")

    record = subparsers.add_parser("record", help="Record one completed or partial run.")
    record.add_argument("--run-id", required=True, help="Stable run identifier.")
    record.add_argument("--goal", required=True, help="Original or normalized goal text.")
    record.add_argument("--persona", default=None, help="Optional persona value.")
    record.add_argument("--domain", default=None, help="Optional domain value.")
    record.add_argument("--operation", default=None, help="Optional operation value.")
    record.add_argument("--sequence", action="append", default=[], help="Ordered script path.")
    record.add_argument(
        "--verdict",
        required=True,
        choices=sorted(VALID_VERDICTS),
        help="Final evaluator verdict.",
    )
    record.add_argument("--outcome", default=None, help="Optional run outcome summary.")
    record.add_argument("--failure-reason", default=None, help="Optional failure code or summary.")
    record.add_argument("--artifact", action="append", default=[], help="Artifact path to persist.")
    record.add_argument("--tag", action="append", default=[], help="Optional retrieval tag.")
    record.add_argument("--evidence-type", action="append", default=[], help="Evidence type produced by the run.")
    record.add_argument(
        "--policy-exception",
        action="append",
        default=[],
        help="Policy exception carried by the run, such as evaluation_bypass.",
    )
    record.add_argument(
        "--operator-note",
        action="append",
        default=[],
        help="Operator note to persist alongside the run record.",
    )
    record.add_argument("--json", action="store_true", help="Print JSON output.")

    quarantine = subparsers.add_parser("quarantine", help="Exclude a stored run from default planner reuse.")
    quarantine.add_argument("--run-id", required=True, help="Stable run identifier.")
    quarantine.add_argument(
        "--policy-exception",
        default="memory_quarantine",
        help="Policy exception to append to the stored run.",
    )
    quarantine.add_argument("--note", action="append", default=[], help="Operator note explaining the quarantine.")
    quarantine.add_argument("--json", action="store_true", help="Print JSON output.")

    show = subparsers.add_parser("show", help="Show one stored run.")
    show.add_argument("--run-id", required=True, help="Stable run identifier.")
    show.add_argument("--json", action="store_true", help="Print JSON output.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "search":
        payload = build_search_result(
            goal=args.goal,
            persona=args.persona,
            domain=args.domain,
            operation=args.operation,
            top_k=args.top_k,
            tags=args.tag,
            evidence_types=args.evidence_type,
            failure_reason=args.failure_reason,
            include_policy_exceptions=args.include_policy_exceptions,
        )
        if args.json:
            print_json(payload)
        else:
            print_text(payload)
        return 0

    if args.command == "audit":
        payload = audit_runs(limit=args.limit)
        if args.json:
            print_json(payload)
        else:
            print_text(payload)
        return 0

    if args.command == "record":
        payload, exit_code = record_run(
            run_id=args.run_id,
            goal=args.goal,
            persona=args.persona,
            domain=args.domain,
            operation=args.operation,
            sequence=args.sequence,
            verdict=args.verdict,
            outcome=args.outcome,
            failure_reason=args.failure_reason,
            artifacts=args.artifact,
            tags=args.tag,
            evidence_types=args.evidence_type,
            policy_exceptions=args.policy_exception,
            operator_notes=args.operator_note,
        )
        if args.json:
            print_json(payload)
        else:
            print_text(payload)
        return exit_code

    if args.command == "quarantine":
        payload, exit_code = quarantine_run(
            run_id=args.run_id,
            policy_exception=args.policy_exception,
            notes=args.note,
        )
        if args.json:
            print_json(payload)
        else:
            print_text(payload)
        return exit_code

    payload, exit_code = show_run(args.run_id)
    if args.json:
        print_json(payload)
    else:
        print_text(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
