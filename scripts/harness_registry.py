#!/usr/bin/env python3
"""Machine-readable registry for the supported CRM Analytics CLI harness surface."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "config" / "harness_registry.json"

ALLOWED_CLASSES = {"read_only", "live_read", "mutating"}
ALLOWED_LIVE_SYSTEMS = {"none", "wave_api", "salesforce_api"}
ALLOWED_ARTIFACT_MODES = {"stdout_only", "artifacts_only", "stdout_and_artifacts"}
ALLOWED_RISKS = {"none", "low", "medium", "high", "destructive"}
ALLOWED_POLICY_PROFILES = {"research", "audit", "authoring", "mutation_review"}

LANE_DEFAULT_POLICY_PROFILES = {
    "intelligence_control": "research",
    "patch_guardrails": "audit",
    "live_inventory": "audit",
    "dashboard_mutations": "mutation_review",
    "export_audits": "audit",
    "wave_data_validations": "audit",
    "salesforce_data_profiles": "audit",
    "native_surface_authoring": "authoring",
    "local_intake": "research",
}

SELF_LOOP_ALLOWED_PATHS = {
    "scripts/run_memory.py",
    "scripts/salesforce_dashboard_filter_automation.py",
}

MUTATION_REVIEW_ALLOWED_PATHS = {
    "scripts/wave_patch_executor.py",
}


def _script_policy_defaults(script: dict[str, Any]) -> dict[str, Any]:
    path = script["path"]
    lane = script["lane"]
    defaults: dict[str, Any] = {
        "policy_profile": LANE_DEFAULT_POLICY_PROFILES.get(lane, "research"),
        "approval_required": script["command_class"] == "mutating",
        "allowed_predecessor_lanes": [],
        "allowed_successor_lanes": [],
        "evidence_types_produced": [],
        "memory_tags": [],
    }

    if lane == "dashboard_mutations":
        defaults.update(
            {
                "approval_required": True,
                "allowed_predecessor_lanes": ["patch_guardrails", "intelligence_control"],
                "allowed_successor_lanes": [],
                "evidence_types_produced": ["mutation_response"],
                "memory_tags": ["dashboard_mutation", "wave_live_write"],
            }
        )
    elif lane == "export_audits":
        defaults.update(
            {
                "allowed_predecessor_lanes": [
                    "live_inventory",
                    "intelligence_control",
                    "patch_guardrails",
                    "wave_data_validations",
                    "salesforce_data_profiles",
                ],
                "allowed_successor_lanes": ["patch_guardrails", "intelligence_control"],
                "evidence_types_produced": ["audit_report"],
                "memory_tags": ["dashboard_audit"],
            }
        )
    elif lane == "wave_data_validations":
        defaults.update(
            {
                "allowed_predecessor_lanes": [
                    "intelligence_control",
                    "live_inventory",
                    "salesforce_data_profiles",
                ],
                "allowed_successor_lanes": [
                    "patch_guardrails",
                    "intelligence_control",
                    "live_inventory",
                    "export_audits",
                ],
                "evidence_types_produced": ["dataset_validation_report"],
                "memory_tags": ["truth_validation"],
            }
        )
    elif lane == "salesforce_data_profiles":
        defaults.update(
            {
                "allowed_predecessor_lanes": ["intelligence_control"],
                "allowed_successor_lanes": [
                    "patch_guardrails",
                    "intelligence_control",
                    "wave_data_validations",
                    "live_inventory",
                    "export_audits",
                ],
                "evidence_types_produced": ["salesforce_profile_report"],
                "memory_tags": ["salesforce_profile"],
            }
        )

    overrides: dict[str, dict[str, Any]] = {
        "scripts/analytics_intelligence.py": {
            "allowed_successor_lanes": [
                "intelligence_control",
                "patch_guardrails",
                "live_inventory",
                "export_audits",
                "wave_data_validations",
                "salesforce_data_profiles",
            ],
            "evidence_types_produced": ["resolution", "route", "workflow_summary"],
            "memory_tags": ["planner_entrypoint", "workflow_router"],
        },
        "scripts/builder_brain.py": {
            "policy_profile": "authoring",
            "allowed_predecessor_lanes": ["intelligence_control", "patch_guardrails"],
            "allowed_successor_lanes": ["patch_guardrails", "native_surface_authoring"],
            "evidence_types_produced": ["build_package", "executor_handoff", "execution_plan"],
            "memory_tags": ["authoring", "handoff_builder"],
        },
        "scripts/wave_patch_executor.py": {
            "policy_profile": "mutation_review",
            "allowed_predecessor_lanes": [
                "intelligence_control",
                "patch_guardrails",
                "export_audits",
                "wave_data_validations",
            ],
            "allowed_successor_lanes": ["dashboard_mutations"],
            "evidence_types_produced": ["wave_patch_worklist", "wave_patch_bundle"],
            "memory_tags": ["wave_patch", "mutation_prep"],
        },
        "scripts/builder_brain_handoff_targets.py": {
            "policy_profile": "authoring",
            "allowed_predecessor_lanes": ["intelligence_control"],
            "allowed_successor_lanes": ["native_surface_authoring"],
            "evidence_types_produced": ["target_resolution"],
            "memory_tags": ["handoff_targeting"],
        },
        "scripts/run_memory.py": {
            "allowed_predecessor_lanes": ["intelligence_control"],
            "allowed_successor_lanes": ["intelligence_control"],
            "evidence_types_produced": ["memory_hits", "memory_record"],
            "memory_tags": ["memory", "run_history"],
        },
        "scripts/ai_os_browser_cli.py": {
            "allowed_predecessor_lanes": ["intelligence_control"],
            "allowed_successor_lanes": [],
            "evidence_types_produced": ["browser_index", "collection_listing"],
            "memory_tags": ["browser", "operator_review"],
        },
        "scripts/harness_planner.py": {
            "allowed_predecessor_lanes": [],
            "allowed_successor_lanes": [
                "intelligence_control",
                "patch_guardrails",
                "live_inventory",
                "export_audits",
                "wave_data_validations",
                "salesforce_data_profiles",
            ],
            "evidence_types_produced": ["plan"],
            "memory_tags": ["planner", "evidence_planning"],
        },
        "scripts/plan_evaluator.py": {
            "allowed_predecessor_lanes": [
                "intelligence_control",
                "live_inventory",
                "export_audits",
                "wave_data_validations",
                "salesforce_data_profiles",
            ],
            "allowed_successor_lanes": ["intelligence_control", "dashboard_mutations", "native_surface_authoring"],
            "evidence_types_produced": ["evaluation"],
            "memory_tags": ["evaluation", "mutation_gate"],
        },
        "scripts/contract_lint.py": {
            "allowed_predecessor_lanes": [
                "intelligence_control",
                "live_inventory",
                "native_surface_authoring",
            ],
            "allowed_successor_lanes": [
                "intelligence_control",
                "export_audits",
                "dashboard_mutations",
                "native_surface_authoring",
            ],
            "evidence_types_produced": ["contract_lint_report"],
            "memory_tags": ["patch_guardrail", "contract_lint"],
        },
        "scripts/dashboard_portfolio_review.py": {
            "allowed_predecessor_lanes": ["intelligence_control"],
            "allowed_successor_lanes": [
                "patch_guardrails",
                "export_audits",
                "wave_data_validations",
                "intelligence_control",
            ],
            "evidence_types_produced": ["portfolio_review"],
            "memory_tags": ["live_inventory"],
        },
        "scripts/export_live_crma_assets.py": {
            "allowed_predecessor_lanes": [
                "intelligence_control",
                "salesforce_data_profiles",
                "wave_data_validations",
            ],
            "allowed_successor_lanes": [
                "patch_guardrails",
                "export_audits",
                "wave_data_validations",
                "intelligence_control",
            ],
            "evidence_types_produced": ["dashboard_json", "dataset_metadata", "xmd"],
            "memory_tags": ["live_inventory", "dashboard_export"],
        },
        "scripts/scan_record_actions.py": {
            "allowed_predecessor_lanes": [
                "intelligence_control",
                "salesforce_data_profiles",
                "wave_data_validations",
            ],
            "allowed_successor_lanes": [
                "patch_guardrails",
                "export_audits",
                "wave_data_validations",
                "intelligence_control",
            ],
            "evidence_types_produced": ["record_action_scan"],
            "memory_tags": ["live_inventory"],
        },
        "scripts/reset_andre_dashboards.py": {
            "approval_required": True,
            "evidence_types_produced": ["backup_reference", "delete_response"],
        },
        "scripts/salesforce_report_executor.py": {
            "approval_required": True,
            "allowed_predecessor_lanes": ["intelligence_control", "patch_guardrails"],
            "allowed_successor_lanes": [],
            "evidence_types_produced": [
                "report_rest_preview",
                "report_bundle",
                "report_verify_result",
                "report_apply_result",
            ],
            "memory_tags": ["native_authoring", "report_executor"],
        },
        "scripts/salesforce_dashboard_executor.py": {
            "approval_required": True,
            "allowed_predecessor_lanes": ["intelligence_control", "patch_guardrails"],
            "allowed_successor_lanes": [],
            "evidence_types_produced": [
                "dashboard_bundle",
                "dashboard_verify_result",
                "dashboard_apply_result",
            ],
            "memory_tags": ["native_authoring", "dashboard_executor"],
        },
        "scripts/salesforce_dashboard_filter_automation.py": {
            "allowed_predecessor_lanes": ["native_surface_authoring"],
            "allowed_successor_lanes": ["native_surface_authoring"],
            "evidence_types_produced": ["filter_automation_result"],
            "memory_tags": ["native_authoring", "filter_automation"],
        },
        "scripts/extract_kpi_workbook.py": {
            "allowed_successor_lanes": ["intelligence_control"],
            "evidence_types_produced": ["workbook_extract"],
            "memory_tags": ["local_intake"],
        },
    }
    defaults.update(overrides.get(path, {}))
    return defaults


def normalize_registry(registry: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(registry)
    normalized["scripts"] = []
    for script in registry["scripts"]:
        entry = dict(script)
        for key, value in _script_policy_defaults(entry).items():
            entry.setdefault(key, value)
        normalized["scripts"].append(entry)
    return normalized


def load_registry() -> dict[str, Any]:
    return normalize_registry(json.loads(REGISTRY_PATH.read_text(encoding="utf-8")))


def discover_non_builder_scripts() -> list[str]:
    script_paths: list[str] = []
    for path in sorted((ROOT / "scripts").glob("*.py")):
        if path.name.startswith("build_"):
            continue
        if path.name == "harness_registry.py":
            continue
        script_paths.append(path.relative_to(ROOT).as_posix())
    return script_paths


def make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def make_result(
    *,
    status: str,
    lane: str,
    command_class: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "harness_registry",
        "lane": lane,
        "command_class": command_class,
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def build_inventory(
    registry: dict[str, Any],
    *,
    lane: str | None = None,
    command_class: str | None = None,
) -> dict[str, Any]:
    scripts = list(registry["scripts"])

    if lane:
        scripts = [item for item in scripts if item["lane"] == lane]
    if command_class:
        scripts = [item for item in scripts if item["command_class"] == command_class]

    lane_counts: dict[str, int] = {}
    for item in scripts:
        lane_counts[item["lane"]] = lane_counts.get(item["lane"], 0) + 1

    selected_lanes = [
        dict(item, script_count=lane_counts.get(item["id"], 0))
        for item in registry["lanes"]
        if (not lane or item["id"] == lane) and lane_counts.get(item["id"], 0) > 0
    ]

    return make_result(
        status="ok",
        lane="registry",
        command_class="read_only",
        messages=[
            make_message(
                "info",
                "inventory_ready",
                f"{len(scripts)} script(s) across {len(selected_lanes)} lane(s).",
            )
        ],
        filters={"lane": lane, "command_class": command_class},
        lanes=selected_lanes,
        scripts=scripts,
        excluded_scripts=registry["excluded_scripts"],
        command_classes=registry["command_classes"],
    )


def describe_script(registry: dict[str, Any], script_path: str) -> dict[str, Any]:
    matches = [item for item in registry["scripts"] if item["path"] == script_path]
    if not matches:
        return make_result(
            status="error",
            lane="registry",
            command_class="read_only",
            messages=[
                make_message(
                    "error",
                    "unknown_script",
                    f"Registry does not define {script_path}.",
                )
            ],
            script=None,
        )

    script = matches[0]
    lane = next(
        item for item in registry["lanes"] if item["id"] == script["lane"]
    )
    return make_result(
        status="ok",
        lane=script["lane"],
        command_class=script["command_class"],
        messages=[
            make_message(
                "info",
                "script_found",
                f"{script_path} is mapped to lane {script['lane']}.",
            )
        ],
        script=script,
        lane_definition=lane,
    )


def validate_registry(registry: dict[str, Any]) -> dict[str, Any]:
    registry = normalize_registry(registry)
    messages: list[dict[str, str]] = []
    lane_ids = [item["id"] for item in registry["lanes"]]
    lane_set = set(lane_ids)
    script_paths = [item["path"] for item in registry["scripts"]]
    excluded_paths = [item["path"] for item in registry["excluded_scripts"]]
    covered_paths = set(script_paths) | set(excluded_paths)

    if len(lane_ids) != len(lane_set):
        messages.append(
            make_message("error", "duplicate_lane_id", "Lane IDs must be unique.")
        )
    if len(script_paths) != len(set(script_paths)):
        messages.append(
            make_message("error", "duplicate_script_path", "Script paths must be unique.")
        )
    if len(excluded_paths) != len(set(excluded_paths)):
        messages.append(
            make_message(
                "error",
                "duplicate_excluded_path",
                "Excluded script paths must be unique.",
            )
        )

    for lane in registry["lanes"]:
        if not lane.get("summary"):
            messages.append(
                make_message(
                    "error",
                    "lane_missing_summary",
                    f"Lane {lane['id']} is missing a summary.",
                )
            )

    for script in registry["scripts"]:
        path = ROOT / script["path"]
        if not path.exists():
            messages.append(
                make_message(
                    "error",
                    "missing_script",
                    f"Registry path does not exist: {script['path']}.",
                )
            )
        if script["lane"] not in lane_set:
            messages.append(
                make_message(
                    "error",
                    "unknown_lane",
                    f"{script['path']} references unknown lane {script['lane']}.",
                )
            )
        if script["command_class"] not in ALLOWED_CLASSES:
            messages.append(
                make_message(
                    "error",
                    "invalid_command_class",
                    f"{script['path']} uses invalid command_class {script['command_class']}.",
                )
            )
        if script["live_system"] not in ALLOWED_LIVE_SYSTEMS:
            messages.append(
                make_message(
                    "error",
                    "invalid_live_system",
                    f"{script['path']} uses invalid live_system {script['live_system']}.",
                )
            )
        if script["artifact_mode"] not in ALLOWED_ARTIFACT_MODES:
            messages.append(
                make_message(
                    "error",
                    "invalid_artifact_mode",
                    f"{script['path']} uses invalid artifact_mode {script['artifact_mode']}.",
                )
            )
        if script["risk"] not in ALLOWED_RISKS:
            messages.append(
                make_message(
                    "error",
                    "invalid_risk",
                    f"{script['path']} uses invalid risk {script['risk']}.",
                )
            )
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
        if script["command_class"] == "read_only" and script["live_system"] != "none":
            messages.append(
                make_message(
                    "error",
                    "readonly_live_system_mismatch",
                    f"{script['path']} is read_only but declares live_system {script['live_system']}.",
                )
            )
        if script["command_class"] == "mutating" and script["live_system"] == "none":
            messages.append(
                make_message(
                    "error",
                    "mutating_live_system_missing",
                    f"{script['path']} is mutating but live_system is none.",
                )
            )
        if script["risk"] == "destructive" and script["command_class"] != "mutating":
            messages.append(
                make_message(
                    "error",
                    "destructive_not_mutating",
                    f"{script['path']} is destructive but not marked mutating.",
                )
            )
        if script["policy_profile"] not in ALLOWED_POLICY_PROFILES:
            messages.append(
                make_message(
                    "error",
                    "invalid_policy_profile",
                    f"{script['path']} uses invalid policy_profile {script['policy_profile']}.",
                )
            )
        if not isinstance(script["approval_required"], bool):
            messages.append(
                make_message(
                    "error",
                    "invalid_approval_required",
                    f"{script['path']} must use a boolean approval_required value.",
                )
            )
        for key, code, require_non_empty in (
            ("allowed_predecessor_lanes", "invalid_predecessor_lanes", False),
            ("allowed_successor_lanes", "invalid_successor_lanes", False),
            ("evidence_types_produced", "invalid_evidence_types_produced", True),
            ("memory_tags", "invalid_memory_tags", True),
        ):
            value = script.get(key)
            if not isinstance(value, list) or (require_non_empty and not value) or not all(
                isinstance(item, str) and item for item in value
            ):
                messages.append(
                    make_message(
                        "error",
                        code,
                        f"{script['path']} must use a non-empty string list for {key}.",
                    )
                )
        for lane_id in script["allowed_predecessor_lanes"]:
            if lane_id not in lane_set:
                messages.append(
                    make_message(
                        "error",
                        "unknown_predecessor_lane",
                        f"{script['path']} references unknown predecessor lane {lane_id}.",
                    )
                )
        for lane_id in script["allowed_successor_lanes"]:
            if lane_id not in lane_set:
                messages.append(
                    make_message(
                        "error",
                        "unknown_successor_lane",
                        f"{script['path']} references unknown successor lane {lane_id}.",
                    )
                )
        if len(script["allowed_predecessor_lanes"]) != len(set(script["allowed_predecessor_lanes"])):
            messages.append(
                make_message(
                    "error",
                    "duplicate_predecessor_lane",
                    f"{script['path']} contains duplicate allowed_predecessor_lanes values.",
                )
            )
        if len(script["allowed_successor_lanes"]) != len(set(script["allowed_successor_lanes"])):
            messages.append(
                make_message(
                    "error",
                    "duplicate_successor_lane",
                    f"{script['path']} contains duplicate allowed_successor_lanes values.",
                )
            )
        if len(script["evidence_types_produced"]) != len(set(script["evidence_types_produced"])):
            messages.append(
                make_message(
                    "error",
                    "duplicate_evidence_type",
                    f"{script['path']} contains duplicate evidence_types_produced values.",
                )
            )
        if len(script["memory_tags"]) != len(set(script["memory_tags"])):
            messages.append(
                make_message(
                    "error",
                    "duplicate_memory_tag",
                    f"{script['path']} contains duplicate memory_tags values.",
                )
            )
        if script["command_class"] == "mutating" and not script["approval_required"]:
            messages.append(
                make_message(
                    "error",
                    "mutating_requires_approval",
                    f"{script['path']} is mutating and must set approval_required to true.",
                )
            )
        if script["risk"] == "destructive" and not script["approval_required"]:
            messages.append(
                make_message(
                    "error",
                    "destructive_requires_approval",
                    f"{script['path']} is destructive and must set approval_required to true.",
                )
            )
        if script["command_class"] == "mutating" and script["allowed_successor_lanes"]:
            messages.append(
                make_message(
                    "error",
                    "mutating_lane_must_be_terminal",
                    f"{script['path']} is mutating and must not declare allowed_successor_lanes.",
                )
            )
        if (
            script["policy_profile"] == "mutation_review"
            and script["lane"] != "dashboard_mutations"
            and script["path"] not in MUTATION_REVIEW_ALLOWED_PATHS
        ):
            messages.append(
                make_message(
                    "error",
                    "invalid_mutation_review_policy",
                    f"{script['path']} uses mutation_review outside the allowed mutation-prep surface.",
                )
            )
        if (
            script["lane"] in script["allowed_predecessor_lanes"]
            and script["lane"] in script["allowed_successor_lanes"]
            and script["path"] not in SELF_LOOP_ALLOWED_PATHS
        ):
            messages.append(
                make_message(
                    "error",
                    "pointless_self_loop",
                    f"{script['path']} declares the current lane as both predecessor and successor.",
                )
            )

    for excluded in registry["excluded_scripts"]:
        path = ROOT / excluded["path"]
        if not path.exists():
            messages.append(
                make_message(
                    "error",
                    "missing_excluded_script",
                    f"Excluded script path does not exist: {excluded['path']}.",
                )
            )
        if excluded["path"] in set(script_paths):
            messages.append(
                make_message(
                    "error",
                    "excluded_and_registered",
                    f"{excluded['path']} appears in both scripts and excluded_scripts.",
                )
            )
        if not excluded.get("reason"):
            messages.append(
                make_message(
                    "error",
                    "excluded_missing_reason",
                    f"{excluded['path']} is missing an exclusion reason.",
                )
            )

    discovered = discover_non_builder_scripts()
    missing = sorted(set(discovered) - covered_paths)
    extra = sorted(covered_paths - set(discovered))

    for path in missing:
        messages.append(
            make_message(
                "error",
                "unclassified_script",
                f"{path} is not classified in harness_registry.json.",
            )
        )
    for path in extra:
        messages.append(
            make_message(
                "error",
                "unknown_registry_path",
                f"{path} is listed in harness_registry.json but not present under scripts/.",
            )
        )

    status = "error" if any(item["level"] == "error" for item in messages) else "ok"
    if status == "ok":
        messages.append(
            make_message(
                "info",
                "registry_valid",
                f"Covered {len(discovered)} non-builder script(s): {len(script_paths)} registered, {len(excluded_paths)} excluded.",
            )
        )

    lane_counts: dict[str, int] = {}
    class_counts: dict[str, int] = {}
    for script in registry["scripts"]:
        lane_counts[script["lane"]] = lane_counts.get(script["lane"], 0) + 1
        class_counts[script["command_class"]] = class_counts.get(script["command_class"], 0) + 1

    return make_result(
        status=status,
        lane="registry",
        command_class="read_only",
        messages=messages,
        coverage={
            "discovered_non_builder_scripts": len(discovered),
            "registered_scripts": len(script_paths),
            "excluded_scripts": len(excluded_paths),
        },
        lane_counts=lane_counts,
        command_class_counts=class_counts,
    )


def print_inventory_text(payload: dict[str, Any]) -> None:
    print("Harness Registry")
    for lane in payload["lanes"]:
        print(
            f"- {lane['id']}: {lane['script_count']} script(s) :: {lane['summary']}"
        )
    print("")
    for script in payload["scripts"]:
        print(
            f"{script['path']} :: lane={script['lane']} class={script['command_class']} "
            f"live={script['live_system']} risk={script['risk']}"
        )
    if payload["excluded_scripts"]:
        print("")
        print("Excluded")
        for item in payload["excluded_scripts"]:
            print(f"- {item['path']} :: {item['reason']}")


def print_describe_text(payload: dict[str, Any]) -> None:
    if payload["status"] != "ok":
        print(payload["messages"][0]["text"], file=sys.stderr)
        return
    script = payload["script"]
    lane = payload["lane_definition"]
    print(script["path"])
    print(f"lane: {script['lane']} :: {lane['summary']}")
    print(f"class: {script['command_class']}")
    print(f"live_system: {script['live_system']}")
    print(f"artifact_mode: {script['artifact_mode']}")
    print(f"supports_json_output: {script.get('supports_json_output', False)}")
    print(f"risk: {script['risk']}")
    print(f"requires_local_export: {script['requires_local_export']}")
    print(f"policy_profile: {script['policy_profile']}")
    print(f"approval_required: {script['approval_required']}")
    print(f"allowed_predecessor_lanes: {', '.join(script['allowed_predecessor_lanes']) or '-'}")
    print(f"allowed_successor_lanes: {', '.join(script['allowed_successor_lanes']) or '-'}")
    print(f"evidence_types_produced: {', '.join(script['evidence_types_produced'])}")
    print(f"memory_tags: {', '.join(script['memory_tags'])}")
    print(f"summary: {script['summary']}")


def print_validate_text(payload: dict[str, Any]) -> None:
    summary = payload["coverage"]
    print(
        "harness_registry:"
        f" status={payload['status']}"
        f" discovered={summary['discovered_non_builder_scripts']}"
        f" registered={summary['registered_scripts']}"
        f" excluded={summary['excluded_scripts']}"
    )
    for message in payload["messages"]:
        prefix = message["level"].upper()
        print(f"{prefix} {message['code']}: {message['text']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory = subparsers.add_parser("inventory", help="List registered harness scripts.")
    inventory.add_argument("--lane", default=None, help="Filter by lane id.")
    inventory.add_argument(
        "--command-class",
        default=None,
        choices=sorted(ALLOWED_CLASSES),
        help="Filter by command class.",
    )
    inventory.add_argument("--json", action="store_true", help="Print JSON output.")

    describe = subparsers.add_parser("describe", help="Describe one registered script.")
    describe.add_argument("--script", required=True, help="Script path, e.g. scripts/contract_lint.py")
    describe.add_argument("--json", action="store_true", help="Print JSON output.")

    validate = subparsers.add_parser("validate", help="Validate registry coverage and schema.")
    validate.add_argument("--json", action="store_true", help="Print JSON output.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    registry = load_registry()

    if args.command == "inventory":
        payload = build_inventory(
            registry,
            lane=args.lane,
            command_class=args.command_class,
        )
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print_inventory_text(payload)
        return 0

    if args.command == "describe":
        payload = describe_script(registry, args.script)
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print_describe_text(payload)
        return 0 if payload["status"] == "ok" else 1

    payload = validate_registry(registry)
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print_validate_text(payload)
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
