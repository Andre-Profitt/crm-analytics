from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "harness_registry.py"


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def load_module():
    spec = importlib.util.spec_from_file_location("harness_registry_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_validate_registry_json() -> None:
    result = run_cli("validate", "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    # Coverage counts drift as new scripts land. Assert structural invariants
    # rather than exact counts: registry + excluded should never exceed the
    # discovered set, and both the registered and excluded sets should have
    # grown past their initial baseline.
    coverage = payload["coverage"]
    assert coverage["registered_scripts"] >= 53
    assert coverage["excluded_scripts"] >= 9
    assert (
        coverage["registered_scripts"] + coverage["excluded_scripts"]
        <= coverage["discovered_non_builder_scripts"]
    )


def test_inventory_filters_dashboard_mutations() -> None:
    result = run_cli(
        "inventory",
        "--lane",
        "dashboard_mutations",
        "--command-class",
        "mutating",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    scripts = payload["scripts"]
    assert scripts
    assert all(item["lane"] == "dashboard_mutations" for item in scripts)
    assert all(item["command_class"] == "mutating" for item in scripts)
    assert any(item["path"] == "scripts/deploy_record_actions.py" for item in scripts)


def test_describe_contract_lint() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/contract_lint.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "patch_guardrails"
    assert payload["script"]["command_class"] == "read_only"
    assert payload["script"]["supports_json_output"] is True
    assert payload["script"]["policy_profile"] == "audit"
    assert payload["script"]["approval_required"] is False
    assert payload["script"]["evidence_types_produced"] == ["contract_lint_report"]
    assert payload["script"]["memory_tags"] == ["patch_guardrail", "contract_lint"]


def test_describe_run_memory() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/run_memory.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "intelligence_control"
    assert payload["script"]["command_class"] == "read_only"
    assert payload["script"]["policy_profile"] == "research"
    assert payload["script"]["supports_json_output"] is True


def test_describe_ai_os_browser_cli() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/ai_os_browser_cli.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "intelligence_control"
    assert payload["script"]["command_class"] == "read_only"
    assert payload["script"]["policy_profile"] == "research"
    assert payload["script"]["evidence_types_produced"] == [
        "browser_index",
        "collection_listing",
    ]
    assert payload["script"]["memory_tags"] == ["browser", "operator_review"]


def test_describe_harness_planner() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/harness_planner.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "intelligence_control"
    assert payload["script"]["command_class"] == "read_only"
    assert payload["script"]["policy_profile"] == "research"
    assert payload["script"]["evidence_types_produced"] == ["plan"]


def test_describe_plan_evaluator() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/plan_evaluator.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "patch_guardrails"
    assert payload["script"]["command_class"] == "read_only"
    assert payload["script"]["policy_profile"] == "audit"
    assert payload["script"]["evidence_types_produced"] == ["evaluation"]


def test_describe_wave_patch_executor() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/wave_patch_executor.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "intelligence_control"
    assert payload["script"]["command_class"] == "read_only"
    assert payload["script"]["supports_json_output"] is True


def test_describe_builder_brain_handoff_targets() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/builder_brain_handoff_targets.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "intelligence_control"
    assert payload["script"]["command_class"] == "live_read"
    assert payload["script"]["supports_json_output"] is True


def test_describe_builder_brain() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/builder_brain.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "intelligence_control"
    assert payload["script"]["command_class"] == "read_only"
    assert payload["script"]["supports_json_output"] is True


def test_describe_salesforce_report_executor() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/salesforce_report_executor.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "native_surface_authoring"
    assert payload["script"]["command_class"] == "read_only"
    assert payload["script"]["supports_json_output"] is True


def test_describe_salesforce_dashboard_executor() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/salesforce_dashboard_executor.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "native_surface_authoring"
    assert payload["script"]["command_class"] == "read_only"
    assert payload["script"]["supports_json_output"] is True


def test_describe_salesforce_dashboard_filter_automation() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/salesforce_dashboard_filter_automation.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "native_surface_authoring"
    assert payload["script"]["command_class"] == "live_read"
    assert payload["script"]["supports_json_output"] is True


def test_describe_bdr_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_bdr_operating_system.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "export_audits"
    assert payload["script"]["supports_json_output"] is True


def test_describe_account_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_account_intelligence.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "export_audits"
    assert payload["script"]["supports_json_output"] is True


def test_describe_lead_management_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_lead_management.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "export_audits"
    assert payload["script"]["supports_json_output"] is True


def test_describe_customer_intelligence_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_customer_intelligence.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "export_audits"
    assert payload["script"]["supports_json_output"] is True


def test_describe_lead_funnel_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_lead_funnel.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "export_audits"
    assert payload["script"]["supports_json_output"] is True


def test_describe_retention_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_revenue_retention_health.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "export_audits"
    assert payload["script"]["supports_json_output"] is True


def test_describe_exec_product_mix_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_executive_product_mix_industry.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "export_audits"
    assert payload["script"]["supports_json_output"] is True


def test_describe_bdr_campaign_control_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_bdr_campaign_control.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "export_audits"
    assert payload["script"]["supports_json_output"] is True


def test_describe_bdr_truth_layer_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_bdr_truth_layer.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "wave_data_validations"
    assert payload["script"]["supports_json_output"] is True


def test_describe_role_structure_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_role_structure.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_retention_semantics_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_retention_semantics.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_bdr_operating_state_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_bdr_operating_state.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_bdr_field_readiness_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_bdr_field_readiness.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_bdr_response_integrity_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_bdr_response_integrity.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_bdr_activity_model_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_bdr_activity_model.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_bdr_quote_product_signals_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_bdr_quote_product_signals.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_retention_product_grain_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_retention_product_grain.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_renewal_outcomes_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_renewal_outcomes.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_renewal_ownership_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_renewal_ownership.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_retention_owner_validation_profile_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/profile_retention_owner_validation.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "salesforce_data_profiles"
    assert payload["script"]["supports_json_output"] is True


def test_describe_commercial_rhythm_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_commercial_rhythm_control_tower.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "export_audits"
    assert payload["script"]["supports_json_output"] is True


def test_describe_source_truth_audit_has_structured_output() -> None:
    result = run_cli(
        "describe",
        "--script",
        "scripts/audit_source_truth_executive_revenue.py",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["script"]["lane"] == "wave_data_validations"
    assert payload["script"]["supports_json_output"] is True


def test_validate_registry_rejects_invalid_policy_profile() -> None:
    module = load_module()
    registry = module.load_registry()
    registry["scripts"][0]["policy_profile"] = "bad_policy"

    payload = module.validate_registry(registry)
    codes = {message["code"] for message in payload["messages"]}
    assert payload["status"] == "error"
    assert "invalid_policy_profile" in codes


def test_validate_registry_rejects_mutating_script_without_approval() -> None:
    module = load_module()
    registry = module.load_registry()
    target = next(
        item for item in registry["scripts"] if item["command_class"] == "mutating"
    )
    target["approval_required"] = False

    payload = module.validate_registry(registry)
    codes = {message["code"] for message in payload["messages"]}
    assert payload["status"] == "error"
    assert "mutating_requires_approval" in codes
