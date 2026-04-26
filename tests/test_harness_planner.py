from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "harness_planner.py"


def load_module():
    spec = importlib.util.spec_from_file_location("harness_planner_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_forecast_mutation_plan_prioritizes_export_lint_and_audit() -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = module.build_plan(
        inputs=inputs,
        query="Manager action queue for deals needing intervention this week",
        operation="mutate_dashboard",
    )

    paths = [step["script"] for step in plan["recommended_sequence"]]
    assert paths == [
        "scripts/export_live_crma_assets.py",
        "scripts/contract_lint.py",
        "scripts/audit_forecast_revenue_motions.py",
    ]
    assert plan["mutation_candidates"] == ["scripts/wave_patch_executor.py"]
    assert plan["missing_evidence"] == []
    assert plan["safe_execution_supported"] is True


def test_plan_never_includes_mutating_script() -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = module.build_plan(
        inputs=inputs,
        query="Manager action queue for deals needing intervention this week",
        operation="mutate_dashboard",
    )

    registry_by_path = {item["path"]: item for item in inputs["registry"]["scripts"]}
    assert all(
        registry_by_path[step["script"]]["command_class"] != "mutating"
        for step in plan["recommended_sequence"]
    )


def test_local_export_dependent_audit_runs_after_export() -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = module.build_plan(
        inputs=inputs,
        query="Manager action queue for deals needing intervention this week",
        operation="mutate_dashboard",
    )

    paths = [step["script"] for step in plan["recommended_sequence"]]
    assert paths.index("scripts/export_live_crma_assets.py") < paths.index(
        "scripts/audit_forecast_revenue_motions.py"
    )


def test_memory_hits_are_reflected_in_planner_notes() -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = module.build_plan(
        inputs=inputs,
        query="Manager action queue for deals needing intervention this week",
        operation="mutate_dashboard",
        memory_hits=[
            {
                "run_id": "run_001",
                "sequence": [
                    "scripts/export_live_crma_assets.py",
                    "scripts/contract_lint.py",
                ],
            }
        ],
    )

    assert any("memory hit" in note for note in plan["planner_notes"])


def test_memory_health_is_reflected_in_planner_notes() -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = module.build_plan(
        inputs=inputs,
        query="Manager action queue for deals needing intervention this week",
        operation="mutate_dashboard",
        memory_hits=[],
        memory_health={
            "policy_exception_hits_excluded": 2,
            "included_fail_count": 1,
            "included_generic_goal_count": 1,
        },
    )

    assert plan["memory_summary"]["memory_health"]["policy_exception_hits_excluded"] == 2
    assert any("policy exceptions" in note for note in plan["planner_notes"])
    assert any("prior failing hit" in note for note in plan["planner_notes"])
    assert any("generic-goal" in note for note in plan["planner_notes"])


def test_successful_memory_hits_surface_reuse_metadata() -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = module.build_plan(
        inputs=inputs,
        query="Manager action queue for deals needing intervention this week",
        operation="mutate_dashboard",
        memory_hits=[
            {
                "run_id": "run_success",
                "verdict": "pass",
                "operation": "mutate_dashboard",
                "domain": "revenue",
                "tags": ["crma_dashboard", "forecast_revenue_motions"],
                "evidence_types": [
                    "dashboard_json",
                    "dataset_metadata",
                    "xmd",
                    "contract_lint_report",
                    "audit_report",
                ],
                "sequence": [
                    "scripts/export_live_crma_assets.py",
                    "scripts/contract_lint.py",
                    "scripts/audit_forecast_revenue_motions.py",
                ],
            }
        ],
    )

    assert plan["memory_summary"]["preferred_success_run_id"] == "run_success"
    assert any("run_success" in note for note in plan["planner_notes"])


def test_repeated_failed_paths_are_reported_in_memory_summary() -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = module.build_plan(
        inputs=inputs,
        query="Manager action queue for deals needing intervention this week",
        operation="mutate_dashboard",
        memory_hits=[
            {
                "run_id": "run_fail_1",
                "verdict": "fail",
                "failure_reason": "contract_lint_blocker",
                "sequence": ["scripts/audit_forecast_revenue_motions.py"],
            },
            {
                "run_id": "run_fail_2",
                "verdict": "fail",
                "failure_reason": "contract_lint_blocker",
                "sequence": ["scripts/audit_forecast_revenue_motions.py"],
            },
        ],
    )

    repeated = plan["memory_summary"]["repeated_failure_codes"]
    assert repeated["scripts/audit_forecast_revenue_motions.py"] == ["contract_lint_blocker"]
    assert any("contract_lint_blocker" in note for note in plan["planner_notes"])


def test_run_plan_writes_plan_artifact(tmp_path: Path) -> None:
    module = load_module()

    payload, exit_code = module.run_plan(
        query="Manager action queue for deals needing intervention this week",
        operation="mutate_dashboard",
        output_dir=str(tmp_path),
        run_id="run_test",
    )

    assert exit_code == 0
    assert payload["status"] == "ok"
    plan_path = tmp_path / "plan.json"
    assert plan_path.exists()
    saved = json.loads(plan_path.read_text(encoding="utf-8"))
    assert saved["run_id"] == "run_test"


def test_report_primary_plan_uses_surface_advisory_and_disables_safe_probe() -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = module.build_plan(
        inputs=inputs,
        query="Manager owner list report for renewals needing follow-up this week",
    )

    assert plan["route"]["recommended_surface_type"] == "salesforce_report"
    assert plan["route"]["effective_surface_type"] == "salesforce_report"
    assert plan["surface_advisory"]["selection_reason"] == "route_recommendation"
    assert plan["surface_advisory"]["report_action_surface_assessment"]["primary_surface_fit"] == "strong_primary_fit"
    assert [step["script"] for step in plan["recommended_sequence"]] == [
        "scripts/builder_brain.py",
        "scripts/salesforce_report_executor.py",
    ]
    assert plan["required_evidence"] == ["build_package", "report_rest_preview"]
    assert plan["safe_execution_supported"] is True
    assert any("builder handoff plus REST preview" in note for note in plan["planner_notes"])


def test_story_heavy_hybrid_query_demotes_report_to_dashboard_in_planner() -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = module.build_plan(
        inputs=inputs,
        query="Manager dashboard report for ownership alignment and handoff quality",
    )

    assert plan["route"]["recommended_surface_type"] == "hybrid"
    assert plan["route"]["effective_surface_type"] == "crma_dashboard"
    assert plan["surface_advisory"]["selection_reason"] == "hybrid_story_plus_action_report_demoted"
    assert plan["surface_advisory"]["secondary_surface"] == "salesforce_report"
    assert plan["surface_advisory"]["report_action_surface_assessment"]["verdict"] == "weak_follow_up_fit"
    assert plan["recommended_sequence"][0]["script"] == "scripts/export_live_crma_assets.py"
