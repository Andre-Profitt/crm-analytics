from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "run_memory.py"


def load_module():
    spec = importlib.util.spec_from_file_location("run_memory_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def configure_memory_paths(module, tmp_path: Path) -> None:
    memory_root = tmp_path / "agent_memory"
    module.MEMORY_ROOT = memory_root
    module.INDEX_PATH = memory_root / "run_index.jsonl"
    module.RUNS_DIR = memory_root / "runs"


def test_record_and_show_run(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    payload, exit_code = module.record_run(
        run_id="run_001",
        goal="fix broken forecast dashboard filters",
        domain="revenue",
        operation="mutate_dashboard",
        sequence=["scripts/export_live_crma_assets.py", "scripts/contract_lint.py"],
        verdict="pass",
        artifacts=["output/agent_runs/run_001/03_plan/plan.json"],
        tags=["filters", "crma_dashboard"],
        evidence_types=["dashboard_json", "contract_lint_report"],
    )
    assert exit_code == 0
    assert payload["status"] == "ok"

    show_payload, show_exit_code = module.show_run("run_001")
    assert show_exit_code == 0
    assert show_payload["record"]["goal"] == "fix broken forecast dashboard filters"
    assert show_payload["record"]["sequence"] == [
        "scripts/export_live_crma_assets.py",
        "scripts/contract_lint.py",
    ]
    assert show_payload["record"]["evidence_types"] == ["dashboard_json", "contract_lint_report"]


def test_search_prefers_exact_domain_and_operation_matches(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    module.record_run(
        run_id="run_001",
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        verdict="pass",
        tags=["filters"],
    )
    module.record_run(
        run_id="run_002",
        goal="repair forecast dashboard filter wiring",
        domain="customer",
        operation="review_dashboard",
        verdict="pass",
        tags=["filters"],
    )

    results = module.search_runs(
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
    )
    assert results
    assert results[0]["run_id"] == "run_001"
    assert results[0]["score"] > results[1]["score"]


def test_search_prefers_successful_runs_with_matching_tags_and_evidence(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    module.record_run(
        run_id="run_success",
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        verdict="pass",
        tags=["crma_dashboard", "forecast_revenue_motions"],
        evidence_types=["dashboard_json", "dataset_metadata", "contract_lint_report"],
        outcome="workflow_ok",
    )
    module.record_run(
        run_id="run_fail",
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        verdict="fail",
        tags=["salesforce_dashboard"],
        evidence_types=["dashboard_json"],
        failure_reason="contract_lint_blocker",
    )

    results = module.search_runs(
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        tags=["crma_dashboard", "forecast_revenue_motions"],
        evidence_types=["dashboard_json", "dataset_metadata", "contract_lint_report"],
    )
    assert results
    assert results[0]["run_id"] == "run_success"
    assert results[0]["score"] > results[1]["score"]


def test_record_rejects_invalid_verdict(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    payload, exit_code = module.record_run(
        run_id="run_001",
        goal="fix broken forecast dashboard filters",
        verdict="maybe",
    )
    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "invalid_verdict"


def test_record_rejects_duplicate_evidence_type(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    payload, exit_code = module.record_run(
        run_id="run_001",
        goal="fix broken forecast dashboard filters",
        verdict="pass",
        evidence_types=["dashboard_json", "dashboard_json"],
    )
    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "duplicate_evidence_type"


def test_search_excludes_policy_exception_runs_by_default(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    module.record_run(
        run_id="run_normal",
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        verdict="pass",
        tags=["crma_dashboard"],
    )
    module.record_run(
        run_id="run_bypass",
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        verdict="pass",
        tags=["crma_dashboard"],
        policy_exceptions=["evaluation_bypass"],
    )

    default_results = module.search_runs(
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
    )
    assert [item["run_id"] for item in default_results] == ["run_normal"]

    included_results = module.search_runs(
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        include_policy_exceptions=True,
    )
    assert {item["run_id"] for item in included_results} == {"run_normal", "run_bypass"}


def test_record_run_merges_existing_memory_record(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    module.record_run(
        run_id="run_001",
        goal="fix broken forecast dashboard filters",
        domain="revenue",
        operation="mutate_dashboard",
        sequence=["scripts/export_live_crma_assets.py"],
        verdict="needs_more_evidence",
        artifacts=["output/agent_runs/run_001/03_plan/plan.json"],
        tags=["crma_dashboard"],
        evidence_types=["dashboard_json"],
    )
    payload, exit_code = module.record_run(
        run_id="run_001",
        goal="fix broken forecast dashboard filters",
        sequence=["scripts/contract_lint.py"],
        verdict="pass",
        outcome="salesforce_dashboard_executor_apply_ok",
        artifacts=["output/agent_runs/run_001/06_execution/result.json"],
        tags=["salesforce_dashboard_executor"],
        evidence_types=["contract_lint_report"],
        policy_exceptions=["evaluation_bypass"],
    )
    assert exit_code == 0
    assert payload["status"] == "ok"

    show_payload, show_exit_code = module.show_run("run_001")
    assert show_exit_code == 0
    record = show_payload["record"]
    assert record["verdict"] == "pass"
    assert record["sequence"] == [
        "scripts/export_live_crma_assets.py",
        "scripts/contract_lint.py",
    ]
    assert record["artifacts"] == [
        "output/agent_runs/run_001/03_plan/plan.json",
        "output/agent_runs/run_001/06_execution/result.json",
    ]
    assert record["evidence_types"] == ["dashboard_json", "contract_lint_report"]
    assert record["policy_exceptions"] == ["evaluation_bypass"]


def test_record_executor_outcome_uses_planning_context(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    payload, exit_code = module.record_executor_outcome(
        planning_context={
            "run_id": "run_002",
            "goal": "fix broken forecast dashboard filters",
            "persona": "manager",
            "domain": "revenue",
            "operation": "mutate_dashboard",
            "surface_type": "salesforce_dashboard",
            "candidate_surface_id": "forecast_revenue_motions",
            "required_evidence": ["dashboard_json", "contract_lint_report"],
        },
        script_path="scripts/salesforce_dashboard_executor.py",
        command="apply",
        status="error",
        messages=[
            {"level": "warn", "code": "evaluation_bypass_used", "text": "Bypassed evaluation."},
            {"level": "error", "code": "target_org_required", "text": "--target-org is required with --apply."},
        ],
        artifacts=[{"type": "preview", "path": "output/agent_runs/run_002/06_execution/preview.json"}],
        evaluation_gate={"verdict": None, "run_id": "run_002", "bypassed": True},
    )
    assert exit_code == 0
    assert payload["record"]["run_id"] == "run_002"

    show_payload, show_exit_code = module.show_run("run_002")
    assert show_exit_code == 0
    record = show_payload["record"]
    assert record["goal"] == "fix broken forecast dashboard filters"
    assert record["persona"] == "manager"
    assert record["domain"] == "revenue"
    assert record["operation"] == "mutate_dashboard"
    assert record["sequence"] == ["scripts/salesforce_dashboard_executor.py"]
    assert record["failure_reason"] == "evaluation_bypass_used"
    assert record["policy_exceptions"] == ["evaluation_bypass"]
    assert set(record["tags"]) >= {"salesforce_dashboard", "forecast_revenue_motions", "salesforce_dashboard_executor", "apply"}


def test_audit_summarizes_policy_exceptions_and_generic_goals(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    module.record_run(
        run_id="run_ok",
        goal="fix broken forecast dashboard filters",
        domain="revenue",
        operation="mutate_dashboard",
        verdict="pass",
        tags=["crma_dashboard"],
    )
    module.record_run(
        run_id="run_bypass",
        goal="Wave PATCH deploy",
        verdict="fail",
        failure_reason="evaluation_bypass_used",
        policy_exceptions=["evaluation_bypass"],
    )

    payload = module.audit_runs(limit=10)
    assert payload["status"] == "ok"
    audit = payload["audit"]
    assert audit["total_runs"] == 2
    assert audit["verdict_counts"]["pass"] == 1
    assert audit["verdict_counts"]["fail"] == 1
    assert audit["policy_exception_counts"] == {"evaluation_bypass": 1}
    assert audit["top_failure_reasons"] == [{"failure_reason": "evaluation_bypass_used", "count": 1}]
    assert audit["runs_with_policy_exceptions"][0]["run_id"] == "run_bypass"
    assert audit["generic_goal_runs"][0]["run_id"] == "run_bypass"
    assert audit["runs_missing_context"][0]["run_id"] == "run_bypass"
    assert audit["runs_missing_context"][0]["missing_fields"] == ["domain", "operation"]


def test_quarantine_run_excludes_record_from_default_search(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    module.record_run(
        run_id="run_003",
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        verdict="pass",
        tags=["crma_dashboard"],
    )

    payload, exit_code = module.quarantine_run(
        run_id="run_003",
        notes=["operator flagged this record after manual validation"],
    )
    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["record"]["policy_exceptions"] == ["memory_quarantine"]

    show_payload, show_exit_code = module.show_run("run_003")
    assert show_exit_code == 0
    assert show_payload["record"]["policy_exceptions"] == ["memory_quarantine"]
    assert show_payload["record"]["operator_notes"] == ["operator flagged this record after manual validation"]

    default_results = module.search_runs(
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
    )
    assert default_results == []

    included_results = module.search_runs(
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        include_policy_exceptions=True,
    )
    assert [item["run_id"] for item in included_results] == ["run_003"]


def test_search_health_reports_excluded_policy_exception_hits(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    module.record_run(
        run_id="run_visible",
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        verdict="pass",
        tags=["crma_dashboard"],
    )
    module.record_run(
        run_id="run_hidden",
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        verdict="fail",
        tags=["crma_dashboard"],
        policy_exceptions=["evaluation_bypass"],
    )

    health = module.summarize_search_health(
        goal="repair forecast dashboard filter wiring",
        domain="revenue",
        operation="mutate_dashboard",
        tags=["crma_dashboard"],
    )

    assert health["considered_hits"] == 1
    assert health["policy_exception_hits_excluded"] == 1
    assert health["excluded_policy_exception_runs"][0]["run_id"] == "run_hidden"


def test_show_errors_for_unknown_run_id(tmp_path: Path) -> None:
    module = load_module()
    configure_memory_paths(module, tmp_path)

    payload, exit_code = module.show_run("missing_run")
    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "unknown_run_id"
