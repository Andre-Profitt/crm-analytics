from __future__ import annotations

import json
import importlib.util
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "analytics_intelligence.py"


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def load_module():
    spec = importlib.util.spec_from_file_location("analytics_intelligence_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_validate_profiles_json() -> None:
    result = run_cli("validate", "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["coverage"]["surface_types"] == 5


def test_resolve_manager_revenue_queue() -> None:
    result = run_cli(
        "resolve",
        "--query",
        "Manager action queue for deals needing intervention this week",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    resolution = payload["resolution"]
    assert resolution["resolved_persona"] == "manager"
    assert resolution["resolved_domain"] == "revenue"
    assert resolution["question"]["id"] == "which_deals_need_intervention"
    assert resolution["candidate_surfaces"]
    assert resolution["candidate_surfaces"][0]["id"] == "forecast_revenue_motions"


def test_route_prefers_crma_for_manager_queue() -> None:
    result = run_cli(
        "route",
        "--query",
        "Manager action queue for deals needing intervention this week",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    assert route["recommended_surface_type"] == "crma_dashboard"
    assert route["operation_mode"] == "review_dashboard"
    assert "live_inventory" in route["lane_sequence"]
    assert route["script_suggestions"][0]["scripts"]


def test_route_prioritizes_candidate_bdr_audit_script() -> None:
    result = run_cli(
        "route",
        "--query",
        "BDR manager queue for responders, SLA misses, and target-account handoffs",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    export_audit_scripts = next(
        item["scripts"] for item in route["script_suggestions"] if item["lane"] == "export_audits"
    )
    assert export_audit_scripts[0]["path"] == "scripts/audit_bdr_operating_system.py"


def test_route_handles_query_without_question_match() -> None:
    result = run_cli(
        "route",
        "--query",
        "Manager account hygiene queue for KYC, contact coverage, and whitespace opportunities",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    export_audit_scripts = next(
        item["scripts"] for item in route["script_suggestions"] if item["lane"] == "export_audits"
    )
    assert payload["resolution"]["question"] is None
    assert export_audit_scripts[0]["path"] == "scripts/audit_account_intelligence.py"


def test_review_mutating_script_has_patch_gates() -> None:
    result = run_cli(
        "review",
        "--script",
        "scripts/upgrade_executive_revenue_live.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    gate_ids = [gate["id"] for gate in payload["gates"]]
    assert "patch_contract" in gate_ids
    assert "execution_boundary" in gate_ids


def test_execute_blocks_mutating_without_flag() -> None:
    result = run_cli(
        "execute",
        "--script",
        "scripts/deploy_record_actions.py",
        "--json",
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "mutating_not_allowed"


def test_execute_read_only_contract_lint() -> None:
    result = run_cli(
        "execute",
        "--script",
        "scripts/contract_lint.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["execution"]["returncode"] == 0
    assert payload["execution"]["structured_output_supported"] is True
    assert payload["execution"]["structured_output"]["tool"] == "contract_lint"


def test_workflow_plan_for_manager_queue() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "Manager action queue for deals needing intervention this week",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    workflow = payload["workflow"]
    assert workflow["mode"] == "plan"
    assert workflow["route"]["recommended_surface_type"] == "crma_dashboard"
    assert isinstance(workflow["memory_hits"], list)
    assert isinstance(workflow["memory_health"], dict)
    assert "policy_exception_hits_excluded" in workflow["memory_health"]
    assert workflow["plan"]["recommended_sequence"]
    assert any(step["name"] == "export_live_assets" for step in workflow["steps"])
    assert workflow["summary"]["candidate_surface_id"] == "forecast_revenue_motions"
    assert workflow["summary"]["safe_execution_supported"] is True
    assert "contract_lint" in workflow["summary"]["planned_steps"]
    assert workflow["summary"]["planner_notes"]
    assert workflow["summary"]["memory_health"] == workflow["memory_health"]


def test_workflow_plan_for_bdr_queue() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "BDR manager queue for responders, SLA misses, and target-account handoffs",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    workflow = payload["workflow"]
    assert workflow["summary"]["candidate_surface_id"] == "bdr_operating_system"
    assert workflow["route"]["recommended_surface_type"] == "hybrid"
    assert "scripts/audit_bdr_operating_system.py" in workflow["summary"]["planned_scripts"]


def test_workflow_plan_for_account_hygiene_queue() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "Manager account hygiene queue for KYC, contact coverage, and whitespace opportunities",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    workflow = payload["workflow"]
    assert workflow["summary"]["candidate_surface_id"] == "account_intelligence"
    assert "scripts/audit_account_intelligence.py" in workflow["summary"]["planned_scripts"]


def test_route_prioritizes_candidate_lead_management_audit_script() -> None:
    result = run_cli(
        "route",
        "--query",
        "Manager lead operations queue for contact-me response, qualification speed, and source attribution gaps",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    export_audit_scripts = next(
        item["scripts"] for item in route["script_suggestions"] if item["lane"] == "export_audits"
    )
    assert payload["resolution"]["question"] is None
    assert export_audit_scripts[0]["path"] == "scripts/audit_lead_management.py"


def test_workflow_plan_for_lead_operations_queue() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "Manager lead operations queue for contact-me response, qualification speed, and source attribution gaps",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    workflow = payload["workflow"]
    assert workflow["summary"]["candidate_surface_id"] == "lead_management"
    assert "scripts/audit_lead_management.py" in workflow["summary"]["planned_scripts"]


def test_route_prioritizes_candidate_customer_intelligence_audit_script() -> None:
    result = run_cli(
        "route",
        "--query",
        "Manager customer health queue for adoption risk, relationship coverage, and termination warning signs",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    export_audit_scripts = next(
        item["scripts"] for item in route["script_suggestions"] if item["lane"] == "export_audits"
    )
    assert payload["resolution"]["question"]["id"] == "health_vs_target"
    assert export_audit_scripts[0]["path"] == "scripts/audit_customer_intelligence.py"


def test_workflow_plan_for_customer_health_queue() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "Manager customer health queue for adoption risk, relationship coverage, and termination warning signs",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    workflow = payload["workflow"]
    assert workflow["summary"]["candidate_surface_id"] == "customer_intelligence"
    assert "scripts/audit_customer_intelligence.py" in workflow["summary"]["planned_scripts"]


def test_route_prioritizes_candidate_lead_funnel_audit_script() -> None:
    result = run_cli(
        "route",
        "--query",
        "Executive funnel view for lead volume, qualification conversion, and disqualification leakage",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    export_audit_scripts = next(
        item["scripts"] for item in route["script_suggestions"] if item["lane"] == "export_audits"
    )
    assert payload["resolution"]["question"]["id"] == "funnel_conversion"
    assert export_audit_scripts[0]["path"] == "scripts/audit_lead_funnel.py"


def test_workflow_plan_for_executive_funnel_query() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "Executive funnel view for lead volume, qualification conversion, and disqualification leakage",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    workflow = payload["workflow"]
    assert workflow["summary"]["candidate_surface_id"] == "lead_funnel"
    assert workflow["route"]["recommended_surface_type"] == "hybrid"
    assert "scripts/audit_lead_funnel.py" in workflow["summary"]["planned_scripts"]


def test_route_prioritizes_candidate_retention_audit_script() -> None:
    result = run_cli(
        "route",
        "--query",
        "Executive retention view for churn risk, renewal coverage, and save actions",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    export_audit_scripts = next(
        item["scripts"] for item in route["script_suggestions"] if item["lane"] == "export_audits"
    )
    assert payload["resolution"]["question"]["id"] == "renewal_risk_queue"
    assert export_audit_scripts[0]["path"] == "scripts/audit_revenue_retention_health.py"


def test_workflow_plan_for_retention_query() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "Executive retention view for churn risk, renewal coverage, and save actions",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    workflow = payload["workflow"]
    assert workflow["summary"]["candidate_surface_id"] == "revenue_retention_health"
    assert "scripts/audit_revenue_retention_health.py" in workflow["summary"]["planned_scripts"]


def test_route_prioritizes_candidate_exec_product_mix_audit_script() -> None:
    result = run_cli(
        "route",
        "--query",
        "Executive product mix and industry view for SaaS ARR, pipeline mix, and segment shifts",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    export_audit_scripts = next(
        item["scripts"] for item in route["script_suggestions"] if item["lane"] == "export_audits"
    )
    assert payload["resolution"]["question"]["id"] == "product_mix_by_segment"
    assert export_audit_scripts[0]["path"] == "scripts/audit_executive_product_mix_industry.py"


def test_workflow_plan_for_exec_product_mix_query() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "Executive product mix and industry view for SaaS ARR, pipeline mix, and segment shifts",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    workflow = payload["workflow"]
    assert workflow["summary"]["candidate_surface_id"] == "executive_product_mix_industry"
    assert "scripts/audit_executive_product_mix_industry.py" in workflow["summary"]["planned_scripts"]


def test_route_selects_commercial_rhythm_candidate() -> None:
    result = run_cli(
        "route",
        "--query",
        "Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    assert payload["resolution"]["candidate_surfaces"][0]["id"] == "commercial_rhythm_control_tower"
    assert route["recommended_surface_type"] == "hybrid"
    assert route["operation_mode"] == "understand_metric"


def test_workflow_plan_for_commercial_rhythm_query() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    workflow = payload["workflow"]
    assert workflow["summary"]["candidate_surface_id"] == "commercial_rhythm_control_tower"
    assert "scripts/audit_commercial_rhythm_control_tower.py" in workflow["summary"]["planned_scripts"]


def test_route_prioritizes_candidate_source_truth_validation_script() -> None:
    result = run_cli(
        "route",
        "--query",
        "Validate executive revenue source truth for commit, best case, pipeline, and closed won reconciliation",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    validation_scripts = next(
        item["scripts"] for item in route["script_suggestions"] if item["lane"] == "wave_data_validations"
    )
    assert payload["resolution"]["resolved_operation"] == "validate_truth"
    assert validation_scripts[0]["path"] == "scripts/audit_source_truth_executive_revenue.py"


def test_workflow_plan_for_source_truth_validation_query() -> None:
    result = run_cli(
        "workflow",
        "--query",
        "Validate executive revenue source truth for commit, best case, pipeline, and closed won reconciliation",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    workflow = payload["workflow"]
    assert workflow["summary"]["candidate_surface_id"] == "executive_revenue_source_truth"
    assert "scripts/audit_source_truth_executive_revenue.py" in workflow["summary"]["planned_scripts"]
    assert any(
        script.startswith("scripts/profile_") for script in workflow["summary"]["planned_scripts"]
    )


def test_route_includes_bdr_truth_layer_for_bdr_truth_query() -> None:
    result = run_cli(
        "route",
        "--query",
        "Executive BDR truth-layer validation for account-product scope and product-signal integrity",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    route = payload["route"]
    validation_scripts = next(
        item["scripts"] for item in route["script_suggestions"] if item["lane"] == "wave_data_validations"
    )
    assert payload["resolution"]["resolved_operation"] == "understand_metric"
    assert any(script["path"] == "scripts/audit_bdr_truth_layer.py" for script in validation_scripts)


def test_route_includes_structured_profile_scripts_for_truth_query() -> None:
    result = run_cli(
        "route",
        "--query",
        "Validate executive revenue source truth for commit, best case, pipeline, and closed won reconciliation",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    profile_scripts = next(
        item["scripts"] for item in payload["route"]["script_suggestions"] if item["lane"] == "salesforce_data_profiles"
    )
    assert any(
        script["path"] == "scripts/profile_role_structure.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_retention_semantics.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_bdr_operating_state.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_bdr_field_readiness.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_bdr_response_integrity.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_bdr_activity_model.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_bdr_quote_product_signals.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_retention_product_grain.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_renewal_outcomes.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_renewal_ownership.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )
    assert any(
        script["path"] == "scripts/profile_retention_owner_validation.py" and script["supports_json_output"] is True
        for script in profile_scripts
    )


def test_workflow_execute_safe_with_mocked_children(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()
    calls: list[tuple[str, list[str]]] = []

    def fake_execute_registered_script(
        inputs,
        *,
        script_path,
        script_args,
        allow_mutating,
        allow_destructive,
    ):
        calls.append((script_path, list(script_args)))
        if script_path == "scripts/export_live_crma_assets.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "export_complete", "export ok")],
                execution={
                    "script": script_path,
                    "lane": "live_inventory",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "export_live_crma_assets",
                        "lane": "live_inventory",
                        "command_class": "live_read",
                        "messages": [],
                        "artifacts": [],
                        "summary": {"dashboards_exported": 1},
                    },
                },
            )
        if script_path == "scripts/contract_lint.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "lint_clean", "lint ok")],
                execution={
                    "script": script_path,
                    "lane": "patch_guardrails",
                    "command_class": "read_only",
                    "risk": "none",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "contract_lint",
                        "lane": "patch_guardrails",
                        "command_class": "read_only",
                        "messages": [],
                        "artifacts": [],
                        "summary": {"total_violations": 0},
                    },
                },
            )
        return module.make_result(
            status="ok",
            command="execute",
            messages=[module.make_message("info", "audit_complete", "audit ok")],
            execution={
                "script": script_path,
                "lane": "export_audits",
                "command_class": "read_only",
                "risk": "none",
                "returncode": 0,
                "structured_output_supported": True,
                "structured_output": {
                    "status": "warn",
                    "tool": "audit_forecast_revenue_motions",
                    "lane": "export_audits",
                    "command_class": "read_only",
                    "messages": [
                        module.make_message("warn", "audit_findings", "audit findings present")
                    ],
                    "artifacts": [
                        {"kind": "json", "path": str(tmp_path / "audit" / "audit.json")}
                    ],
                    "summary": {
                        "dashboard": "Forecast & Revenue Motions",
                        "pass_count": 20,
                        "fail_count": 2,
                        "widget_count": 40,
                        "step_count": 18,
                        "chrome_ratio": 0.3,
                        "output_dir": str(tmp_path / "audit"),
                    },
                },
            },
        )

    module.execute_registered_script = fake_execute_registered_script
    payload = module.build_workflow(
        inputs,
        query="Manager action queue for deals needing intervention this week",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=True,
        output_dir=str(tmp_path),
    )
    assert payload["status"] == "ok"
    workflow = payload["workflow"]
    assert workflow["mode"] == "execute_safe"
    assert workflow["candidate_surface"]["id"] == "forecast_revenue_motions"
    assert workflow["evaluation"]["verdict"] == "pass"
    assert any(step["name"] == "export_live_assets" for step in workflow["steps"])
    assert any(step["name"] == "contract_lint" for step in workflow["steps"])
    assert workflow["summary"]["workflow_status"] == "ok"
    assert workflow["summary"]["evaluation_verdict"] == "pass"
    executed = {step["name"]: step for step in workflow["summary"]["executed_steps"]}
    assert executed["export_live_assets"]["export"]["dashboards_exported"] == 1
    assert executed["contract_lint"]["lint"]["total_violations"] == 0
    assert executed["audit_surface"]["audit"]["fail_count"] == 2
    assert calls[0][0] == "scripts/export_live_crma_assets.py"
    assert calls[1][0] == "scripts/contract_lint.py"


def test_workflow_execute_safe_records_memory(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    inputs = module.load_inputs()
    scripts_dir = ROOT / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    import run_memory as run_memory_module

    memory_root = tmp_path / "agent_memory"
    monkeypatch.setattr(run_memory_module, "MEMORY_ROOT", memory_root)
    monkeypatch.setattr(run_memory_module, "INDEX_PATH", memory_root / "run_index.jsonl")
    monkeypatch.setattr(run_memory_module, "RUNS_DIR", memory_root / "runs")

    def fake_execute_registered_script(
        inputs,
        *,
        script_path,
        script_args,
        allow_mutating,
        allow_destructive,
    ):
        if script_path == "scripts/export_live_crma_assets.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "export_complete", "export ok")],
                execution={
                    "script": script_path,
                    "lane": "live_inventory",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "export_live_crma_assets",
                        "lane": "live_inventory",
                        "command_class": "live_read",
                        "messages": [],
                        "artifacts": [],
                        "summary": {"dashboards_exported": 1},
                    },
                },
            )
        if script_path == "scripts/contract_lint.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "lint_clean", "lint ok")],
                execution={
                    "script": script_path,
                    "lane": "patch_guardrails",
                    "command_class": "read_only",
                    "risk": "none",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "contract_lint",
                        "lane": "patch_guardrails",
                        "command_class": "read_only",
                        "messages": [],
                        "artifacts": [],
                        "summary": {"total_violations": 0},
                    },
                },
            )
        return module.make_result(
            status="ok",
            command="execute",
            messages=[module.make_message("info", "audit_complete", "audit ok")],
            execution={
                "script": script_path,
                "lane": "export_audits",
                "command_class": "read_only",
                "risk": "none",
                "returncode": 0,
                "structured_output_supported": True,
                "structured_output": {
                    "status": "ok",
                    "tool": "audit_forecast_revenue_motions",
                    "lane": "export_audits",
                    "command_class": "read_only",
                    "messages": [],
                    "artifacts": [],
                    "summary": {"pass_count": 20, "fail_count": 0},
                },
            },
        )

    module.execute_registered_script = fake_execute_registered_script
    workflow_dir = tmp_path / "workflow_run"
    payload = module.build_workflow(
        inputs,
        query="Manager action queue for deals needing intervention this week",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=True,
        output_dir=str(workflow_dir),
    )
    assert payload["status"] == "ok"
    workflow = payload["workflow"]
    assert workflow["memory_record"]["run_id"] == "workflow_run"
    assert Path(workflow["summary"]["review_artifact"]).exists()
    assert Path(workflow["summary"]["collection_index_artifact"]).exists()
    assert Path(workflow["summary"]["collection_landing_artifact"]).exists()
    assert Path(workflow["summary"]["browser_index_artifact"]).exists()
    assert Path(workflow["summary"]["browser_landing_artifact"]).exists()
    assert Path(workflow["summary"]["browser_health_index_artifact"]).exists()
    assert Path(workflow["summary"]["browser_health_landing_artifact"]).exists()
    assert isinstance(workflow["summary"]["browser_health_summary"], dict)
    assert isinstance(workflow["summary"]["browser_health_summary"].get("run_recency_counts"), dict)
    health_index_path = Path(workflow["summary"]["browser_health_index_artifact"])
    health_landing_path = Path(workflow["summary"]["browser_health_landing_artifact"])
    assert health_index_path.exists()
    assert health_landing_path.exists()
    workflow_review = Path(workflow["summary"]["review_artifact"]).read_text(encoding="utf-8")
    collection_overview = Path(workflow["summary"]["collection_landing_artifact"]).read_text(encoding="utf-8")
    browser_overview = Path(workflow["summary"]["browser_landing_artifact"]).read_text(encoding="utf-8")
    collection_index = json.loads(Path(workflow["summary"]["collection_index_artifact"]).read_text(encoding="utf-8"))
    browser_index = json.loads(Path(workflow["summary"]["browser_index_artifact"]).read_text(encoding="utf-8"))
    browser_health = json.loads(health_index_path.read_text(encoding="utf-8"))
    assert "# Intelligence Workflow Review" in workflow_review
    assert "Evaluation verdict: pass" in workflow_review
    assert "# Intelligence Workflow Runs" in collection_overview
    assert workflow["summary"]["review_artifact"] in collection_overview
    assert collection_index["entries"][0]["run_dir"] == str(workflow_dir)
    assert "# AI OS Collections" in browser_overview
    assert "## Health Snapshot" in browser_overview
    assert workflow["summary"]["collection_landing_artifact"] in browser_overview
    assert str(health_landing_path) in browser_overview
    assert browser_index["collections"][0]["collection_dir"] == str(workflow_dir.parent)
    assert browser_index["health_summary"]["collection_count"] >= 1
    assert browser_health["collection_count"] >= 1
    assert workflow["summary"]["browser_health_summary"]["collection_count"] == browser_health["collection_count"]
    record_path = memory_root / "runs" / "workflow_run.json"
    assert record_path.exists()
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["verdict"] == "pass"
    assert record["outcome"] == "workflow_ok"
    assert record["tags"] == [
        "manager",
        "revenue",
        "crma_dashboard",
        "review_dashboard",
        "forecast_revenue_motions",
    ]
    assert record["evidence_types"] == workflow["plan"]["required_evidence"]
    assert record["sequence"] == [
        "scripts/export_live_crma_assets.py",
        "scripts/contract_lint.py",
        "scripts/audit_forecast_revenue_motions.py",
    ]
    assert any(message["code"] == "workflow_review_ready" for message in payload["messages"])
    assert any(message["code"] == "intelligence_workflow_collection_index_ready" for message in payload["messages"])
    assert any(message["code"] == "ai_os_browser_ready" for message in payload["messages"])
    assert any(message["code"] == "ai_os_health_ready" for message in payload["messages"])


def test_workflow_plan_uses_effective_report_surface_for_report_primary_query(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()
    workflow_root = (tmp_path / "report_plan").resolve()

    payload = module.build_workflow(
        inputs,
        query="Manager owner list report for renewals needing follow-up this week",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=False,
        output_dir=str(workflow_root),
    )

    assert payload["status"] == "ok"
    workflow = payload["workflow"]
    assert workflow["route"]["recommended_surface_type"] == "salesforce_report"
    assert workflow["summary"]["effective_surface_type"] == "salesforce_report"
    assert workflow["summary"]["safe_execution_supported"] is True
    assert workflow["summary"]["planned_report_filter_overrides"] == [
        {"source_label": "renewal_period", "value": "This Week"}
    ]
    assert "apply_report_rest_dry_run" in workflow["summary"]["planned_steps"]
    assert workflow["plan"]["surface_advisory"]["selection_reason"] == "route_recommendation"
    assert [step["script"] for step in workflow["plan"]["recommended_sequence"]] == [
        "scripts/builder_brain.py",
        "scripts/salesforce_report_executor.py",
    ]
    registry_by_path = {item["path"]: item for item in inputs["registry"]["scripts"]}
    report_planner_step = next(
        step
        for step in workflow["plan"]["recommended_sequence"]
        if step["script"] == "scripts/salesforce_report_executor.py"
    )
    report_step = module._build_planned_workflow_step(
        planner_step=report_planner_step,
        script_entry=registry_by_path["scripts/salesforce_report_executor.py"],
        workflow_base_dir=workflow_root,
        live_export_dir=workflow_root / "live_export",
        audit_dir=workflow_root / "audit",
        profile_dir=workflow_root / "profiles",
        validation_dir=workflow_root / "validations",
        candidate=workflow["candidate_surface"],
        query=workflow["resolution"]["query"],
        resolution=workflow["resolution"],
    )
    assert report_step is not None
    assert report_step["args"][0] == "preview"
    assert "--clone-from-report-id" in report_step["args"]
    assert "00OTb000008TZaTMAW" in report_step["args"]
    assert "--autofill-live" in report_step["args"]
    assert "--target-org" in report_step["args"]
    assert "apro@simcorp.com" in report_step["args"]
    assert "--filter-override" in report_step["args"]
    assert "renewal_period=This Week" in report_step["args"]
    apply_step = next(step for step in workflow["steps"] if step.get("name") == "apply_report_rest_dry_run")
    assert apply_step["args"][0] == "apply"
    assert "--clone-from-report-id" in apply_step["args"]
    assert "00OTb000008TZaTMAW" in apply_step["args"]
    assert "--autofill-live" in apply_step["args"]
    assert "--target-org" in apply_step["args"]
    assert "apro@simcorp.com" in apply_step["args"]
    assert "renewal_period=This Week" in apply_step["args"]


def test_workflow_execute_safe_for_report_primary_with_mocked_children(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()
    calls: list[tuple[str, list[str]]] = []

    def fake_execute_registered_script(
        inputs,
        *,
        script_path,
        script_args,
        allow_mutating,
        allow_destructive,
    ):
        calls.append((script_path, list(script_args)))
        if script_path == "scripts/builder_brain.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "handoff_complete", "handoff ok")],
                execution={
                    "script": script_path,
                    "lane": "intelligence_control",
                    "command_class": "read_only",
                    "risk": "none",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "builder_brain",
                        "lane": "intelligence_control",
                        "command_class": "read_only",
                        "messages": [],
                        "artifacts": [
                            {"type": "build_package", "path": str(tmp_path / "report_handoff" / "build_package.json")}
                        ],
                        "revised_spec": {
                            "primary_surface": "salesforce_report",
                            "secondary_surface": "crma_dashboard",
                        },
                        "executor_handoff": {
                            "primary_lane": "salesforce_report_handoff",
                            "package_artifact": str(tmp_path / "report_handoff" / "build_package.json"),
                        },
                    },
                },
            )
        if script_args and script_args[0] == "apply":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "apply_preview_ready", "apply dry run ready")],
                execution={
                    "script": script_path,
                    "lane": "native_surface_authoring",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "salesforce_report_executor",
                        "lane": "native_surface_authoring",
                        "command_class": "live_read",
                        "messages": [module.make_message("info", "apply_preview_ready", "apply dry run ready")],
                        "summary": {
                            "request_count": 1,
                            "fill_requirement_count": 0,
                        },
                        "apply_summary": {
                            "strategy": "create_new",
                            "request_count": 1,
                            "fill_requirement_count": 0,
                            "external_fill_requirement_count": 0,
                            "internal_fill_requirement_count": 0,
                            "resolved_filter_override_count": 1,
                            "manual_filter_intent_count": 0,
                            "manual_detail_intent_count": 0,
                            "omitted_sort_intent_count": 0,
                            "native_authoring_support": "fully_supported",
                            "native_authoring_ready": True,
                            "apply_ready": True,
                        },
                    },
                },
            )
        return module.make_result(
            status="warn",
            command="execute",
            messages=[module.make_message("warn", "rest_preview_ready", "preview ready")],
            execution={
                "script": script_path,
                "lane": "native_surface_authoring",
                "command_class": "live_read",
                "risk": "low",
                "returncode": 0,
                "structured_output_supported": True,
                "structured_output": {
                    "status": "warn",
                    "tool": "salesforce_report_executor",
                    "lane": "native_surface_authoring",
                    "command_class": "live_read",
                    "messages": [module.make_message("warn", "rest_preview_ready", "preview ready")],
                    "artifacts": [
                        {
                            "type": "salesforce_report_rest_preview",
                            "path": str(tmp_path / "report_preview" / "salesforce_report_rest_preview.json"),
                        }
                    ],
                    "summary": {
                        "request_count": 1,
                        "fill_requirement_count": 10,
                        "action_surface_verdict": "strong_follow_up_fit",
                        "manual_authoring_pressure_score": 7,
                        "resolved_filter_override_count": 1,
                        "manual_filter_intent_count": 0,
                    },
                },
            },
        )

    module.execute_registered_script = fake_execute_registered_script
    payload = module.build_workflow(
        inputs,
        query="Manager owner list report for renewals needing follow-up this week",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=True,
        output_dir=str(tmp_path),
    )

    assert payload["status"] == "warn"
    workflow = payload["workflow"]
    assert workflow["summary"]["effective_surface_type"] == "salesforce_report"
    assert workflow["evaluation"]["verdict"] == "pass"
    assert workflow["summary"]["evaluation_verdict"] == "pass"
    assert workflow["summary"]["workflow_status"] == "warn"
    assert workflow["summary"]["planned_report_filter_overrides"] == [
        {"source_label": "renewal_period", "value": "This Week"}
    ]
    assert workflow["summary"]["resolved_report_filter_override_count"] == 1
    assert workflow["summary"]["manual_report_filter_intent_count"] == 0
    assert workflow["summary"]["report_apply_strategy"] == "create_new"
    assert workflow["summary"]["report_apply_ready"] is True
    assert workflow["summary"]["report_native_authoring_ready"] is True
    assert workflow["summary"]["report_external_fill_requirement_count"] == 0
    executed = {step["name"]: step for step in workflow["summary"]["executed_steps"]}
    assert executed["build_report_handoff"]["report_handoff"]["primary_lane"] == "salesforce_report_handoff"
    assert executed["preview_report_rest"]["report_preview"]["request_count"] == 1
    assert executed["preview_report_rest"]["report_preview"]["resolved_filter_override_count"] == 1
    assert executed["preview_report_rest"]["report_preview"]["manual_filter_intent_count"] == 0
    assert executed["apply_report_rest_dry_run"]["report_apply"]["strategy"] == "create_new"
    assert executed["apply_report_rest_dry_run"]["report_apply"]["apply_ready"] is True
    assert calls[0][0] == "scripts/builder_brain.py"
    assert calls[0][1][0] == "handoff"
    assert calls[1][0] == "scripts/salesforce_report_executor.py"
    assert calls[1][1][0] == "preview"
    assert "--clone-from-report-id" in calls[1][1]
    assert "00OTb000008TZaTMAW" in calls[1][1]
    assert "--autofill-live" in calls[1][1]
    assert "--target-org" in calls[1][1]
    assert "apro@simcorp.com" in calls[1][1]
    assert "renewal_period=This Week" in calls[1][1]
    assert calls[2][0] == "scripts/salesforce_report_executor.py"
    assert calls[2][1][0] == "apply"
    assert "--clone-from-report-id" in calls[2][1]
    assert "00OTb000008TZaTMAW" in calls[2][1]
    assert "--autofill-live" in calls[2][1]
    assert "--target-org" in calls[2][1]
    assert "apro@simcorp.com" in calls[2][1]
    assert "renewal_period=This Week" in calls[2][1]


def test_workflow_plan_surfaces_multiple_inferred_report_filter_overrides(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    payload = module.build_workflow(
        inputs,
        query="Manager owner list report for high risk renewals needing follow-up this week",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=False,
        output_dir=str((tmp_path / "report_plan_multi_override").resolve()),
    )

    assert payload["status"] == "ok"
    workflow = payload["workflow"]
    assert workflow["route"]["recommended_surface_type"] == "salesforce_report"
    assert workflow["summary"]["planned_report_filter_overrides"] == [
        {"source_label": "renewal_period", "value": "This Week"},
        {"source_label": "risk_band", "value": "High"},
    ]


def test_workflow_plan_surfaces_owner_and_product_family_report_filter_overrides(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    payload = module.build_workflow(
        inputs,
        query='Manager owner list report for owner "Taylor Smith" in product family "Axioma" high risk renewals needing follow-up this week',
        persona=None,
        domain=None,
        operation=None,
        execute_safe=False,
        output_dir=str((tmp_path / "report_plan_explicit_override").resolve()),
    )

    assert payload["status"] == "ok"
    workflow = payload["workflow"]
    assert workflow["route"]["recommended_surface_type"] == "salesforce_report"
    assert workflow["summary"]["planned_report_filter_overrides"] == [
        {"source_label": "renewal_period", "value": "This Week"},
        {"source_label": "owner", "value": "Taylor Smith"},
        {"source_label": "product_family", "value": "Axioma"},
        {"source_label": "risk_band", "value": "High"},
    ]


def test_workflow_plan_demotes_story_heavy_report_to_dashboard_surface(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()

    payload = module.build_workflow(
        inputs,
        query="Manager dashboard report for ownership alignment and handoff quality",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=False,
        output_dir=str(tmp_path / "story_hybrid"),
    )

    assert payload["status"] == "ok"
    workflow = payload["workflow"]
    assert workflow["route"]["recommended_surface_type"] == "hybrid"
    assert workflow["summary"]["effective_surface_type"] == "crma_dashboard"
    assert workflow["summary"]["surface_selection_reason"] == "hybrid_story_plus_action_report_demoted"
    assert workflow["summary"]["secondary_surface_type"] == "salesforce_report"
    assert workflow["plan"]["surface_advisory"]["report_action_surface_assessment"]["verdict"] == "weak_follow_up_fit"
    assert workflow["plan"]["recommended_sequence"][0]["script"] == "scripts/export_live_crma_assets.py"


def test_workflow_execute_safe_for_account_surface_with_mocked_children(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()
    calls: list[tuple[str, list[str]]] = []

    def fake_execute_registered_script(
        inputs,
        *,
        script_path,
        script_args,
        allow_mutating,
        allow_destructive,
    ):
        calls.append((script_path, list(script_args)))
        if script_path == "scripts/export_live_crma_assets.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "export_complete", "export ok")],
                execution={
                    "script": script_path,
                    "lane": "live_inventory",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "export_live_crma_assets",
                        "lane": "live_inventory",
                        "command_class": "live_read",
                        "messages": [],
                        "artifacts": [],
                        "summary": {
                            "dashboards_requested": 1,
                            "dashboards_exported": 1,
                            "dashboard_errors": 0,
                            "dataset_warning_count": 0,
                            "output_dir": str(tmp_path / "live_export"),
                        },
                    },
                },
            )
        if script_path == "scripts/contract_lint.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "lint_clean", "lint ok")],
                execution={
                    "script": script_path,
                    "lane": "patch_guardrails",
                    "command_class": "read_only",
                    "risk": "none",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "contract_lint",
                        "lane": "patch_guardrails",
                        "command_class": "read_only",
                        "messages": [],
                        "artifacts": [],
                        "summary": {
                            "files_checked": 1,
                            "files_with_violations": 0,
                            "total_violations": 0,
                            "file_errors": 0,
                            "normalized": False,
                        },
                    },
                },
            )
        return module.make_result(
            status="warn",
            command="execute",
            messages=[module.make_message("warn", "audit_findings", "audit findings present")],
            execution={
                "script": script_path,
                "lane": "export_audits",
                "command_class": "read_only",
                "risk": "none",
                "returncode": 1,
                "structured_output_supported": True,
                "structured_output": {
                    "status": "warn",
                    "tool": "audit_account_intelligence",
                    "lane": "export_audits",
                    "command_class": "read_only",
                    "messages": [
                        module.make_message("warn", "audit_findings", "account findings present")
                    ],
                    "artifacts": [
                        {"kind": "json", "path": str(tmp_path / "audit" / "audit.json")}
                    ],
                    "summary": {
                        "dashboard": "Account Intelligence KPIs",
                        "pass_count": 7,
                        "fail_count": 1,
                        "widget_count": 24,
                        "step_count": 18,
                        "chrome_ratio": 0.25,
                        "output_dir": str(tmp_path / "audit"),
                    },
                },
            },
        )

    module.execute_registered_script = fake_execute_registered_script
    payload = module.build_workflow(
        inputs,
        query="Manager account hygiene queue for KYC, contact coverage, and whitespace opportunities",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=True,
        output_dir=str(tmp_path),
    )
    assert payload["status"] == "warn"
    workflow = payload["workflow"]
    assert workflow["mode"] == "execute_safe"
    assert workflow["candidate_surface"]["id"] == "account_intelligence"
    assert workflow["summary"]["workflow_status"] == "warn"
    executed = {step["name"]: step for step in workflow["summary"]["executed_steps"]}
    assert executed["audit_surface"]["audit"]["dashboard"] == "Account Intelligence KPIs"
    assert executed["audit_surface"]["audit"]["fail_count"] == 1
    assert calls[2][0] == "scripts/audit_account_intelligence.py"


def test_workflow_execute_safe_for_executive_funnel_with_mocked_children(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()
    calls: list[tuple[str, list[str]]] = []

    def fake_execute_registered_script(
        inputs,
        *,
        script_path,
        script_args,
        allow_mutating,
        allow_destructive,
    ):
        calls.append((script_path, list(script_args)))
        if script_path == "scripts/export_live_crma_assets.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "export_complete", "export ok")],
                execution={
                    "script": script_path,
                    "lane": "live_inventory",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "export_live_crma_assets",
                        "lane": "live_inventory",
                        "command_class": "live_read",
                        "messages": [],
                        "artifacts": [],
                        "summary": {
                            "dashboards_requested": 1,
                            "dashboards_exported": 1,
                            "dashboard_errors": 0,
                            "dataset_warning_count": 0,
                            "output_dir": str(tmp_path / "live_export"),
                        },
                    },
                },
            )
        if script_path == "scripts/contract_lint.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "lint_clean", "lint ok")],
                execution={
                    "script": script_path,
                    "lane": "patch_guardrails",
                    "command_class": "read_only",
                    "risk": "none",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "contract_lint",
                        "lane": "patch_guardrails",
                        "command_class": "read_only",
                        "messages": [],
                        "artifacts": [],
                        "summary": {
                            "files_checked": 1,
                            "files_with_violations": 0,
                            "total_violations": 0,
                            "file_errors": 0,
                            "normalized": False,
                        },
                    },
                },
            )
        if script_path == "scripts/audit_executive_product_mix_industry.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "audit_clean", "audit ok")],
                execution={
                    "script": script_path,
                    "lane": "export_audits",
                    "command_class": "read_only",
                    "risk": "none",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "audit_executive_product_mix_industry",
                        "lane": "export_audits",
                        "command_class": "read_only",
                        "messages": [module.make_message("info", "audit_clean", "audit ok")],
                        "artifacts": [
                            {"kind": "json", "path": str(tmp_path / "audit" / "audit.json")}
                        ],
                        "summary": {
                            "dashboard": "Executive Product Mix & Industry",
                            "pass_count": 10,
                            "fail_count": 0,
                            "output_dir": str(tmp_path / "audit"),
                        },
                    },
                },
            )
        return module.make_result(
            status="ok",
            command="execute",
            messages=[module.make_message("info", "audit_clean", "funnel audit clean")],
            execution={
                "script": script_path,
                "lane": "export_audits",
                "command_class": "read_only",
                "risk": "none",
                "returncode": 0,
                "structured_output_supported": True,
                "structured_output": {
                    "status": "ok",
                    "tool": "audit_lead_funnel",
                    "lane": "export_audits",
                    "command_class": "read_only",
                    "messages": [
                        module.make_message("info", "audit_clean", "lead funnel clean")
                    ],
                    "artifacts": [
                        {"kind": "json", "path": str(tmp_path / "audit" / "audit.json")}
                    ],
                    "summary": {
                        "dashboard": "Lead Funnel",
                        "pass_count": 7,
                        "fail_count": 0,
                        "widget_count": 16,
                        "step_count": 11,
                        "output_dir": str(tmp_path / "audit"),
                    },
                },
            },
        )

    module.execute_registered_script = fake_execute_registered_script
    payload = module.build_workflow(
        inputs,
        query="Executive funnel view for lead volume, qualification conversion, and disqualification leakage",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=True,
        output_dir=str(tmp_path),
    )
    assert payload["status"] == "ok"
    workflow = payload["workflow"]
    assert workflow["mode"] == "execute_safe"
    assert workflow["candidate_surface"]["id"] == "lead_funnel"
    assert workflow["route"]["recommended_surface_type"] == "hybrid"
    assert workflow["summary"]["workflow_status"] == "ok"
    executed = {step["name"]: step for step in workflow["summary"]["executed_steps"]}
    assert executed["audit_surface"]["audit"]["dashboard"] == "Lead Funnel"
    assert executed["audit_surface"]["audit"]["fail_count"] == 0
    assert calls[2][0] == "scripts/audit_lead_funnel.py"


def test_workflow_execute_safe_runs_profile_and_validation_steps_for_bdr_truth_query(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()
    calls: list[tuple[str, list[str]]] = []

    def fake_execute_registered_script(
        inputs,
        *,
        script_path,
        script_args,
        allow_mutating,
        allow_destructive,
    ):
        calls.append((script_path, list(script_args)))
        if script_path.startswith("scripts/profile_"):
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "profile_ready", "profile ok")],
                execution={
                    "script": script_path,
                    "lane": "salesforce_data_profiles",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": Path(script_path).stem,
                        "lane": "salesforce_data_profiles",
                        "command_class": "live_read",
                        "messages": [module.make_message("info", "profile_ready", "profile ok")],
                        "artifacts": [
                            {"kind": "json", "path": str(tmp_path / "profiles" / Path(script_path).stem / "profile.json")}
                        ],
                        "summary": {
                            "lead_count": 12,
                            "output_dir": str(tmp_path / "profiles" / Path(script_path).stem),
                        },
                    },
                },
            )
        if script_path == "scripts/audit_bdr_truth_layer.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "validation_ready", "validation ok")],
                execution={
                    "script": script_path,
                    "lane": "wave_data_validations",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "audit_bdr_truth_layer",
                        "lane": "wave_data_validations",
                        "command_class": "live_read",
                        "messages": [module.make_message("info", "validation_ready", "validation ok")],
                        "artifacts": [
                            {"kind": "json", "path": str(tmp_path / "validations" / "audit_bdr_truth_layer" / "audit.json")}
                        ],
                        "summary": {
                            "record_type_count": 4,
                            "output_dir": str(tmp_path / "validations" / "audit_bdr_truth_layer"),
                        },
                    },
                },
            )
        if script_path == "scripts/export_live_crma_assets.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "export_complete", "export ok")],
                execution={
                    "script": script_path,
                    "lane": "live_inventory",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "export_live_crma_assets",
                        "lane": "live_inventory",
                        "command_class": "live_read",
                        "messages": [],
                        "artifacts": [],
                        "summary": {
                            "dashboards_requested": 1,
                            "dashboards_exported": 1,
                            "dashboard_errors": 0,
                            "dataset_warning_count": 0,
                            "output_dir": str(tmp_path / "live_export"),
                        },
                    },
                },
            )
        if script_path == "scripts/contract_lint.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "lint_clean", "lint ok")],
                execution={
                    "script": script_path,
                    "lane": "patch_guardrails",
                    "command_class": "read_only",
                    "risk": "none",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "contract_lint",
                        "lane": "patch_guardrails",
                        "command_class": "read_only",
                        "messages": [],
                        "artifacts": [],
                        "summary": {"total_violations": 0},
                    },
                },
            )
        return module.make_result(
            status="ok",
            command="execute",
            messages=[module.make_message("info", "audit_clean", "audit ok")],
            execution={
                "script": script_path,
                "lane": "export_audits",
                "command_class": "read_only",
                "risk": "none",
                "returncode": 0,
                "structured_output_supported": True,
                "structured_output": {
                    "status": "ok",
                    "tool": "audit_commercial_rhythm_control_tower",
                    "lane": "export_audits",
                    "command_class": "read_only",
                    "messages": [module.make_message("info", "audit_clean", "audit ok")],
                    "artifacts": [
                        {"kind": "json", "path": str(tmp_path / "audit" / "audit.json")}
                    ],
                    "summary": {
                        "dashboard": "Commercial Rhythm Control Tower",
                        "pass_count": 10,
                        "fail_count": 0,
                        "output_dir": str(tmp_path / "audit"),
                    },
                },
            },
        )

    module.execute_registered_script = fake_execute_registered_script
    payload = module.build_workflow(
        inputs,
        query="Executive BDR truth-layer validation for account-product scope and product-signal integrity",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=True,
        output_dir=str(tmp_path),
    )

    assert payload["status"] == "ok"
    workflow = payload["workflow"]
    assert workflow["summary"]["workflow_status"] == "ok"
    assert workflow["summary"]["executed_profile_steps"]
    assert workflow["summary"]["executed_validation_steps"]
    assert any(step.get("profile") for step in workflow["summary"]["executed_profile_steps"])
    assert any(step.get("validation") for step in workflow["summary"]["executed_validation_steps"])
    assert any(script.startswith("scripts/profile_") for script, _ in calls)
    assert any(script == "scripts/audit_bdr_truth_layer.py" for script, _ in calls)
    assert any(script == "scripts/export_live_crma_assets.py" for script, _ in calls)
    assert all(script != "scripts/contract_lint.py" for script, _ in calls)
    assert any(script == "scripts/audit_executive_product_mix_industry.py" for script, _ in calls)


def test_workflow_execute_safe_runs_export_backed_validation_for_source_truth_query(tmp_path: Path) -> None:
    module = load_module()
    inputs = module.load_inputs()
    calls: list[tuple[str, list[str]]] = []

    def fake_execute_registered_script(
        inputs,
        *,
        script_path,
        script_args,
        allow_mutating,
        allow_destructive,
    ):
        calls.append((script_path, list(script_args)))
        if script_path.startswith("scripts/profile_"):
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "profile_ready", "profile ok")],
                execution={
                    "script": script_path,
                    "lane": "salesforce_data_profiles",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": Path(script_path).stem,
                        "lane": "salesforce_data_profiles",
                        "command_class": "live_read",
                        "messages": [module.make_message("info", "profile_ready", "profile ok")],
                        "artifacts": [],
                        "summary": {
                            "owner_count": 4,
                            "output_dir": str(tmp_path / "profiles" / Path(script_path).stem),
                        },
                    },
                },
            )
        if script_path == "scripts/export_live_crma_assets.py":
            return module.make_result(
                status="ok",
                command="execute",
                messages=[module.make_message("info", "export_complete", "export ok")],
                execution={
                    "script": script_path,
                    "lane": "live_inventory",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 0,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "ok",
                        "tool": "export_live_crma_assets",
                        "lane": "live_inventory",
                        "command_class": "live_read",
                        "messages": [],
                        "artifacts": [],
                        "summary": {
                            "dashboards_requested": 1,
                            "dashboards_exported": 1,
                            "dashboard_errors": 0,
                            "dataset_warning_count": 0,
                            "output_dir": str(tmp_path / "live_export"),
                        },
                    },
                },
            )
        if script_path == "scripts/audit_source_truth_executive_revenue.py":
            return module.make_result(
                status="warn",
                command="execute",
                messages=[module.make_message("warn", "audit_findings", "validation findings present")],
                execution={
                    "script": script_path,
                    "lane": "wave_data_validations",
                    "command_class": "live_read",
                    "risk": "low",
                    "returncode": 1,
                    "structured_output_supported": True,
                    "structured_output": {
                        "status": "warn",
                        "tool": "audit_source_truth_executive_revenue",
                        "lane": "wave_data_validations",
                        "command_class": "live_read",
                        "messages": [module.make_message("warn", "audit_findings", "validation findings present")],
                        "artifacts": [],
                        "summary": {
                            "pass_count": 5,
                            "fail_count": 1,
                            "output_dir": str(tmp_path / "audit"),
                        },
                    },
                },
            )
        raise AssertionError(script_path)

    module.execute_registered_script = fake_execute_registered_script
    payload = module.build_workflow(
        inputs,
        query="Validate executive revenue source truth for commit, best case, pipeline, and closed won reconciliation",
        persona=None,
        domain=None,
        operation=None,
        execute_safe=True,
        output_dir=str(tmp_path),
    )

    assert payload["status"] == "warn"
    workflow = payload["workflow"]
    assert workflow["summary"]["workflow_status"] == "warn"
    executed = {step["name"]: step for step in workflow["summary"]["executed_steps"]}
    assert "export_live_assets" in executed
    assert "audit_surface" in executed
    assert "contract_lint" not in executed
    assert executed["audit_surface"]["validation"]["fail_count"] == 1
    assert any(script == "scripts/export_live_crma_assets.py" for script, _ in calls)
    assert any(script == "scripts/audit_source_truth_executive_revenue.py" for script, _ in calls)
    assert all(script != "scripts/contract_lint.py" for script, _ in calls)
