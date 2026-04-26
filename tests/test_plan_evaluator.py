from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "plan_evaluator.py"


def load_module():
    spec = importlib.util.spec_from_file_location("plan_evaluator_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def valid_plan_payload() -> dict[str, object]:
    return {
        "run_id": "run_test",
        "resolved": {"operation": "mutate_dashboard"},
        "required_evidence": [
            "dashboard_json",
            "dataset_metadata",
            "xmd",
            "contract_lint_report",
            "audit_report",
        ],
        "recommended_sequence": [
            {"script": "scripts/export_live_crma_assets.py"},
            {"script": "scripts/contract_lint.py"},
            {"script": "scripts/audit_forecast_revenue_motions.py"},
        ],
    }


def valid_report_plan_payload() -> dict[str, object]:
    return {
        "run_id": "run_report",
        "resolved": {"operation": "review_dashboard"},
        "route": {
            "recommended_surface_type": "salesforce_report",
            "effective_surface_type": "salesforce_report",
        },
        "required_evidence": [
            "build_package",
            "report_rest_preview",
        ],
        "surface_advisory": {
            "effective_surface_type": "salesforce_report",
            "selection_reason": "route_recommendation",
            "report_action_surface_assessment": {
                "verdict": "strong_follow_up_fit",
                "primary_surface_fit": "strong_primary_fit",
                "queue_ready_format": True,
            },
        },
        "recommended_sequence": [
            {"script": "scripts/builder_brain.py"},
            {"script": "scripts/salesforce_report_executor.py"},
        ],
    }


def test_evaluate_plan_passes_with_complete_evidence(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan_path = tmp_path / "plan.json"
    write_json(plan_path, valid_plan_payload())

    write_json(tmp_path / "live_export" / "dashboard.json", {"state": {"steps": {}, "widgets": {}}})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "dataset.json", {"name": "Dataset"})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "xmd_main.json", {"foo": "bar"})
    write_json(
        tmp_path / "lint" / "contract_lint.json",
        {"tool": "contract_lint", "status": "ok", "summary": {"total_violations": 0}},
    )
    write_json(
        tmp_path / "audit" / "audit.json",
        {"tool": "audit_forecast_revenue_motions", "status": "ok"},
    )

    payload, exit_code = module.evaluate_plan(
        inputs=inputs,
        plan_path=plan_path,
        artifacts_dir=tmp_path,
        output_dir=tmp_path / "evaluation",
    )

    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["evaluation"]["verdict"] == "pass"
    assert payload["evaluation"]["mutation_ready"] is True
    assert (tmp_path / "evaluation" / "evaluation.json").exists()


def test_evaluate_plan_requests_more_evidence_when_artifacts_are_missing(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan_path = tmp_path / "plan.json"
    write_json(plan_path, valid_plan_payload())

    write_json(tmp_path / "live_export" / "dashboard.json", {"state": {"steps": {}, "widgets": {}}})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "dataset.json", {"name": "Dataset"})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "xmd_main.json", {"foo": "bar"})

    payload, exit_code = module.evaluate_plan(
        inputs=inputs,
        plan_path=plan_path,
        artifacts_dir=tmp_path,
    )

    assert exit_code == 0
    assert payload["status"] == "warn"
    assert payload["evaluation"]["verdict"] == "needs_more_evidence"
    assert "contract_lint_report" in payload["evaluation"]["evidence_gaps"]
    assert "audit_report" in payload["evaluation"]["evidence_gaps"]


def test_evaluate_plan_fails_when_mutating_script_is_in_plan(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = valid_plan_payload()
    plan["recommended_sequence"] = [{"script": "scripts/deploy_record_actions.py"}]
    plan_path = tmp_path / "plan.json"
    write_json(plan_path, plan)

    payload, exit_code = module.evaluate_plan(
        inputs=inputs,
        plan_path=plan_path,
        artifacts_dir=tmp_path,
    )

    assert exit_code == 1
    assert payload["status"] == "error"
    codes = {item["code"] for item in payload["evaluation"]["blocking_findings"]}
    assert "mutating_script_in_plan" in codes


def test_evaluate_plan_fails_on_invalid_lane_transition(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = valid_plan_payload()
    plan["recommended_sequence"] = [
        {"script": "scripts/export_live_crma_assets.py"},
        {"script": "scripts/profile_bdr_response_integrity.py"},
    ]
    plan_path = tmp_path / "plan.json"
    write_json(plan_path, plan)

    payload, exit_code = module.evaluate_plan(
        inputs=inputs,
        plan_path=plan_path,
        artifacts_dir=tmp_path,
    )

    assert exit_code == 1
    assert payload["status"] == "error"
    codes = {item["code"] for item in payload["evaluation"]["blocking_findings"]}
    assert "invalid_lane_transition" in codes


def test_evaluate_plan_fails_on_contract_lint_blocker(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan_path = tmp_path / "plan.json"
    write_json(plan_path, valid_plan_payload())

    write_json(tmp_path / "live_export" / "dashboard.json", {"state": {"steps": {}, "widgets": {}}})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "dataset.json", {"name": "Dataset"})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "xmd_main.json", {"foo": "bar"})
    write_json(
        tmp_path / "lint" / "contract_lint.json",
        {"tool": "contract_lint", "status": "warn", "summary": {"total_violations": 1}},
    )
    write_json(
        tmp_path / "audit" / "audit.json",
        {"tool": "audit_forecast_revenue_motions", "status": "ok"},
    )

    payload, exit_code = module.evaluate_plan(
        inputs=inputs,
        plan_path=plan_path,
        artifacts_dir=tmp_path,
    )

    assert exit_code == 1
    assert payload["status"] == "error"
    codes = {item["code"] for item in payload["evaluation"]["blocking_findings"]}
    assert "contract_lint_blocker" in codes


def test_evaluate_plan_fails_when_primary_report_surface_is_weak(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = valid_plan_payload()
    plan["route"] = {"recommended_surface_type": "salesforce_report"}
    plan["surface_advisory"] = {
        "effective_surface_type": "salesforce_report",
        "selection_reason": "route_recommendation",
        "report_action_surface_assessment": {
            "verdict": "weak_follow_up_fit",
            "primary_surface_fit": "weak_primary_fit",
            "queue_ready_format": False,
        },
    }
    plan_path = tmp_path / "plan.json"
    write_json(plan_path, plan)

    write_json(tmp_path / "live_export" / "dashboard.json", {"state": {"steps": {}, "widgets": {}}})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "dataset.json", {"name": "Dataset"})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "xmd_main.json", {"foo": "bar"})
    write_json(
        tmp_path / "lint" / "contract_lint.json",
        {"tool": "contract_lint", "status": "ok", "summary": {"total_violations": 0}},
    )
    write_json(
        tmp_path / "audit" / "audit.json",
        {"tool": "audit_forecast_revenue_motions", "status": "ok"},
    )

    payload, exit_code = module.evaluate_plan(
        inputs=inputs,
        plan_path=plan_path,
        artifacts_dir=tmp_path,
    )

    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["evaluation"]["rule_checks"]["report_surface_fit"] == "fail"
    codes = {item["code"] for item in payload["evaluation"]["blocking_findings"]}
    assert "report_surface_primary_fit" in codes


def test_evaluate_plan_warns_when_report_is_demoted_behind_dashboard(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan = valid_plan_payload()
    plan["route"] = {"recommended_surface_type": "hybrid"}
    plan["surface_advisory"] = {
        "effective_surface_type": "crma_dashboard",
        "primary_surface": "crma_dashboard",
        "secondary_surface": "salesforce_report",
        "selection_reason": "hybrid_story_plus_action_report_demoted",
        "report_action_surface_assessment": {
            "verdict": "weak_follow_up_fit",
            "primary_surface_fit": "weak_primary_fit",
            "verdict_cap": "matrix_caps_follow_up_fit",
        },
    }
    plan_path = tmp_path / "plan.json"
    write_json(plan_path, plan)

    write_json(tmp_path / "live_export" / "dashboard.json", {"state": {"steps": {}, "widgets": {}}})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "dataset.json", {"name": "Dataset"})
    write_json(tmp_path / "live_export" / "datasets" / "forecast" / "xmd_main.json", {"foo": "bar"})
    write_json(
        tmp_path / "lint" / "contract_lint.json",
        {"tool": "contract_lint", "status": "ok", "summary": {"total_violations": 0}},
    )
    write_json(
        tmp_path / "audit" / "audit.json",
        {"tool": "audit_forecast_revenue_motions", "status": "ok"},
    )

    payload, exit_code = module.evaluate_plan(
        inputs=inputs,
        plan_path=plan_path,
        artifacts_dir=tmp_path,
    )

    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["evaluation"]["verdict"] == "pass"
    warning_codes = {item["code"] for item in payload["evaluation"]["warnings"]}
    assert "report_surface_demoted" in warning_codes


def test_evaluate_plan_passes_for_report_native_safe_probe_artifacts(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    plan_path = tmp_path / "plan.json"
    write_json(plan_path, valid_report_plan_payload())
    write_json(
        tmp_path / "steps" / "01_handoff" / "child_output.json",
        {
            "tool": "builder_brain",
            "status": "ok",
            "artifacts": [{"type": "build_package", "path": str(tmp_path / "report_handoff" / "build_package.json")}],
        },
    )
    write_json(
        tmp_path / "steps" / "02_preview" / "child_output.json",
        {
            "tool": "salesforce_report_executor",
            "status": "warn",
            "artifacts": [
                {
                    "type": "salesforce_report_rest_preview",
                    "path": str(tmp_path / "report_preview" / "salesforce_report_rest_preview.json"),
                }
            ],
        },
    )

    payload, exit_code = module.evaluate_plan(
        inputs=inputs,
        plan_path=plan_path,
        artifacts_dir=tmp_path,
    )

    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["evaluation"]["verdict"] == "pass"
    assert payload["evaluation"]["rule_checks"]["required_evidence_present"] == "pass"
