#!/usr/bin/env python3
"""Deterministic evaluator for planner output and gathered evidence."""

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


PROFILES_PATH = ROOT / "config" / "plan_evaluator_profiles.json"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_inputs() -> dict[str, Any]:
    return {
        "registry": registry_tools.load_registry(),
        "profiles": load_json(PROFILES_PATH),
    }


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
        "tool": "plan_evaluator",
        "lane": "patch_guardrails",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return payload


def _artifact_type_lookup(profiles: dict[str, Any]) -> dict[str, list[str]]:
    lookup: dict[str, list[str]] = {}
    for evidence_type, artifact_types in profiles["artifact_type_map"].items():
        for artifact_type in artifact_types:
            lookup.setdefault(artifact_type, []).append(evidence_type)
    return lookup


def _dashboard_contract_findings(payload: dict[str, Any]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    state = payload.get("state", payload)
    if not isinstance(state, dict):
        return findings

    for step_name, step in state.get("steps", {}).items():
        if not isinstance(step, dict):
            continue
        for dataset in step.get("datasets", []):
            if isinstance(dataset, dict) and any(key in dataset for key in ("label", "url")):
                findings.append(
                    {
                        "code": "dashboard_json_contract_violation",
                        "severity": "blocker",
                        "message": f"Step {step_name} includes forbidden dataset keys.",
                    }
                )
        if step.get("type") == "aggregateflex" and "isFacet" in step:
            findings.append(
                {
                    "code": "dashboard_json_contract_violation",
                    "severity": "blocker",
                    "message": f"Step {step_name} includes forbidden aggregateflex isFacet.",
                }
            )

    required_column_map_keys = {"dimensionAxis", "plots", "trellis", "split"}
    for widget_name, widget in state.get("widgets", {}).items():
        if not isinstance(widget, dict):
            continue
        parameters = widget.get("parameters", {})
        if not isinstance(parameters, dict):
            continue
        column_map = parameters.get("columnMap")
        if isinstance(column_map, dict) and any(key not in column_map for key in required_column_map_keys):
            findings.append(
                {
                    "code": "column_map_contract_violation",
                    "severity": "blocker",
                    "message": f"Widget {widget_name} has an incomplete columnMap.",
                }
            )
    return findings


def collect_artifact_facts(
    artifacts_dir: Path,
    *,
    profiles: dict[str, Any],
) -> tuple[set[str], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    evidence: set[str] = set()
    lint_payloads: list[dict[str, Any]] = []
    dashboard_payloads: list[dict[str, Any]] = []
    findings: list[dict[str, str]] = []
    artifact_lookup = _artifact_type_lookup(profiles)

    for path in artifacts_dir.rglob("*.json"):
        if path.name == "plan.json":
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        if path.name == "dashboard.json":
            evidence.add("dashboard_json")
            if isinstance(payload, dict):
                dashboard_payloads.append(payload)
        if path.name == "dataset.json":
            evidence.add("dataset_metadata")
        if "xmd" in path.name:
            evidence.add("xmd")

        if not isinstance(payload, dict):
            continue

        tool = payload.get("tool")
        lane = payload.get("lane")
        summary = payload.get("summary", {})
        if tool == "export_live_crma_assets" and isinstance(summary, dict):
            if int(summary.get("dashboards_exported", 0) or 0) > 0:
                evidence.update({"dashboard_json", "dataset_metadata", "xmd"})
        if tool == "contract_lint":
            evidence.add("contract_lint_report")
            lint_payloads.append(payload)
        if isinstance(tool, str) and tool.startswith("audit_"):
            evidence.add("audit_report")
        if lane == "wave_data_validations":
            evidence.add("dataset_validation_report")
        if lane == "salesforce_data_profiles":
            evidence.add("salesforce_profile_report")

        for artifact in payload.get("artifacts", []):
            if not isinstance(artifact, dict):
                continue
            artifact_type = artifact.get("type") or artifact.get("kind")
            if not isinstance(artifact_type, str):
                continue
            for evidence_type in artifact_lookup.get(artifact_type, []):
                evidence.add(evidence_type)

    for payload in dashboard_payloads:
        findings.extend(_dashboard_contract_findings(payload))

    return evidence, lint_payloads, dashboard_payloads, findings


def evaluate_plan(
    *,
    inputs: dict[str, Any],
    plan_path: Path,
    artifacts_dir: Path,
    output_dir: Path | None = None,
) -> tuple[dict[str, Any], int]:
    plan = load_json(plan_path)
    registry_by_path = {item["path"]: item for item in inputs["registry"]["scripts"]}
    profiles = inputs["profiles"]
    severity_by_code = profiles["severity_by_code"]

    blocking_findings: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    rule_checks = {
        "required_evidence_present": "pass",
        "lane_transitions_valid": "pass",
        "plan_integrity": "pass",
        "contract_lint_report": "pass",
        "dashboard_json_contract": "pass",
        "report_surface_fit": "pass",
    }

    sequence = plan.get("recommended_sequence", [])
    if not isinstance(sequence, list):
        sequence = []
        blocking_findings.append(
            {
                "code": "invalid_plan_sequence",
                "severity": "blocker",
                "message": "Plan recommended_sequence is missing or invalid.",
            }
        )
        rule_checks["plan_integrity"] = "fail"

    prev_script: dict[str, Any] | None = None
    for step in sequence:
        script_path = step.get("script")
        script = registry_by_path.get(script_path)
        if not script:
            blocking_findings.append(
                {
                    "code": "unknown_script",
                    "severity": "blocker",
                    "message": f"Plan references unknown script {script_path}.",
                }
            )
            rule_checks["plan_integrity"] = "fail"
            continue
        if script["command_class"] == "mutating":
            blocking_findings.append(
                {
                    "code": "mutating_script_in_plan",
                    "severity": "blocker",
                    "message": f"Plan includes mutating script {script_path}.",
                }
            )
            rule_checks["plan_integrity"] = "fail"
        if prev_script:
            valid_successor = script["lane"] in prev_script["allowed_successor_lanes"]
            valid_predecessor = (
                not script["allowed_predecessor_lanes"]
                or prev_script["lane"] in script["allowed_predecessor_lanes"]
            )
            if not (valid_successor and valid_predecessor):
                blocking_findings.append(
                    {
                        "code": "invalid_lane_transition",
                        "severity": "blocker",
                        "message": f"Invalid lane transition from {prev_script['path']} to {script_path}.",
                    }
                )
                rule_checks["lane_transitions_valid"] = "fail"
        prev_script = script

    evidence_found, lint_payloads, _dashboard_payloads, dashboard_findings = collect_artifact_facts(
        artifacts_dir,
        profiles=profiles,
    )
    for finding in dashboard_findings:
        if severity_by_code.get(finding["code"]) == "blocker":
            blocking_findings.append(finding)
            rule_checks["dashboard_json_contract"] = "fail"

    for lint_payload in lint_payloads:
        summary = lint_payload.get("summary", {})
        total_violations = summary.get("total_violations", 0) if isinstance(summary, dict) else 0
        if lint_payload.get("status") in {"warn", "error"} or total_violations:
            blocking_findings.append(
                {
                    "code": "contract_lint_blocker",
                    "severity": "blocker",
                    "message": "Contract lint returned warnings or blocking violations.",
                }
            )
            rule_checks["contract_lint_report"] = "fail"

    required_evidence = plan.get("required_evidence")
    if not required_evidence:
        operation = plan.get("resolved", {}).get("operation")
        required_evidence = profiles["required_evidence_by_operation"].get(operation, [])
    missing_evidence = sorted(set(required_evidence) - evidence_found)
    if missing_evidence:
        rule_checks["required_evidence_present"] = "fail"

    if plan.get("resolved", {}).get("operation") == "validate_truth" and "dataset_validation_report" not in evidence_found:
        warnings.append(
            {
                "code": "dataset_integrity_not_validated",
                "severity": "warning",
                "message": "Truth-validation flow is missing dataset validation evidence.",
            }
        )

    surface_advisory = plan.get("surface_advisory")
    if isinstance(surface_advisory, dict):
        effective_surface = surface_advisory.get("effective_surface_type") or surface_advisory.get("primary_surface")
        report_assessment = surface_advisory.get("report_action_surface_assessment")
        if effective_surface == "salesforce_report":
            if isinstance(report_assessment, dict):
                verdict = report_assessment.get("verdict")
                primary_fit = report_assessment.get("primary_surface_fit")
                verdict_cap = report_assessment.get("verdict_cap")
                if verdict == "weak_follow_up_fit" or primary_fit == "weak_primary_fit":
                    blocking_findings.append(
                        {
                            "code": "report_surface_primary_fit",
                            "severity": "blocker",
                            "message": "Primary salesforce_report surface is too weak to serve as the main follow-up surface.",
                        }
                    )
                    rule_checks["report_surface_fit"] = "fail"
                elif isinstance(verdict_cap, str) or primary_fit == "limited_primary_fit":
                    warnings.append(
                        {
                            "code": "report_surface_primary_limited",
                            "severity": "warning",
                            "message": "Primary salesforce_report surface is capped and should be treated as limited rather than queue-first.",
                        }
                    )
            else:
                warnings.append(
                    {
                        "code": "report_surface_assessment_missing",
                        "severity": "warning",
                        "message": "Primary salesforce_report surface is missing an action-surface assessment.",
                    }
                )
        if (
            surface_advisory.get("selection_reason") == "hybrid_story_plus_action_report_demoted"
            or (
                plan.get("route", {}).get("recommended_surface_type") == "salesforce_report"
                and effective_surface
                and effective_surface != plan.get("route", {}).get("recommended_surface_type")
            )
        ):
            warnings.append(
                {
                    "code": "report_surface_demoted",
                    "severity": "warning",
                    "message": "Surface advisory demoted salesforce_report behind a stronger primary surface.",
                }
            )

    verdict = "pass"
    if blocking_findings:
        verdict = "fail"
    elif missing_evidence:
        verdict = "needs_more_evidence"

    evaluation = {
        "run_id": plan.get("run_id"),
        "verdict": verdict,
        "rule_checks": rule_checks,
        "blocking_findings": blocking_findings,
        "required_next_steps": (
            [f"Collect evidence for: {', '.join(missing_evidence)}."] if missing_evidence and verdict == "needs_more_evidence" else []
        ),
        "evidence_gaps": missing_evidence,
        "warnings": warnings,
        "surface_advisory": surface_advisory if isinstance(surface_advisory, dict) else None,
        "mutation_ready": verdict == "pass",
    }

    artifacts: list[dict[str, str]] = []
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        evaluation_path = output_dir / "evaluation.json"
        evaluation_path.write_text(json.dumps(evaluation, indent=2), encoding="utf-8")
        artifacts.append({"type": "evaluation", "path": str(evaluation_path)})

    status = "error" if verdict == "fail" else ("warn" if verdict == "needs_more_evidence" else "ok")
    payload = make_result(
        status=status,
        command="evaluate",
        messages=[
            ai.make_message(
                "error" if verdict == "fail" else ("warn" if verdict == "needs_more_evidence" else "info"),
                "evaluation_ready",
                f"Evaluation completed with verdict {verdict}.",
            )
        ],
        artifacts=artifacts,
        evaluation=evaluation,
    )
    return payload, 1 if verdict == "fail" else 0


def print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2))


def print_text(payload: dict[str, Any]) -> None:
    print(payload["messages"][0]["text"])
    for finding in payload["evaluation"]["blocking_findings"]:
        print(f"- {finding['code']}: {finding['message']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    evaluate = subparsers.add_parser("evaluate", help="Evaluate one plan against gathered artifacts.")
    evaluate.add_argument("--plan", required=True, help="Path to plan.json.")
    evaluate.add_argument("--artifacts-dir", required=True, help="Artifact root for the run.")
    evaluate.add_argument("--output-dir", default=None, help="Optional directory for evaluation.json.")
    evaluate.add_argument("--json", action="store_true", help="Print JSON output.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    inputs = load_inputs()

    payload, exit_code = evaluate_plan(
        inputs=inputs,
        plan_path=Path(args.plan).resolve(),
        artifacts_dir=Path(args.artifacts_dir).resolve(),
        output_dir=Path(args.output_dir).resolve() if args.output_dir else None,
    )
    if args.json:
        print_json(payload)
    else:
        print_text(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
