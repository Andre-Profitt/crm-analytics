from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "builder_brain.py"


def run_cli(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
    )


def build_fake_dashboard_filter_automation_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "fake_dashboard_filter_automation.py"
    script_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import argparse, json",
                "from pathlib import Path",
                "parser = argparse.ArgumentParser()",
                "subparsers = parser.add_subparsers(dest='command', required=True)",
                "run = subparsers.add_parser('run-filter-flow')",
                "run.add_argument('--plan', required=True)",
                "run.add_argument('--target-org', required=True)",
                "run.add_argument('--dashboard-id', required=True)",
                "run.add_argument('--all-filters', action='store_true')",
                "run.add_argument('--through', required=True)",
                "run.add_argument('--verify-package', required=True)",
                "run.add_argument('--session', required=True)",
                "run.add_argument('--manual-filter-authoring-json', default=None)",
                "run.add_argument('--output-dir', default=None)",
                "run.add_argument('--verify-output-dir', default=None)",
                "run.add_argument('--json', action='store_true')",
                "args = parser.parse_args()",
                "artifacts = []",
                "if args.output_dir:",
                "    output_dir = Path(args.output_dir)",
                "    output_dir.mkdir(parents=True, exist_ok=True)",
                "    invocation_path = output_dir / 'run_filter_flow_invocation.json'",
                "    invocation_path.write_text(json.dumps({",
                "        'plan': args.plan,",
                "        'target_org': args.target_org,",
                "        'dashboard_id': args.dashboard_id,",
                "        'all_filters': args.all_filters,",
                "        'through': args.through,",
                "        'verify_package': args.verify_package,",
                "        'session': args.session,",
                "        'manual_filter_authoring_json': args.manual_filter_authoring_json,",
                "    }, indent=2), encoding='utf-8')",
                "    artifacts.append({'type': 'run_filter_flow_invocation', 'path': str(invocation_path)})",
                "if args.verify_output_dir:",
                "    verify_dir = Path(args.verify_output_dir)",
                "    verify_dir.mkdir(parents=True, exist_ok=True)",
                "    verify_path = verify_dir / 'salesforce_dashboard_verify.json'",
                "    verify_path.write_text(json.dumps({'status': 'ok'}, indent=2), encoding='utf-8')",
                "    artifacts.append({'type': 'salesforce_dashboard_verify', 'path': str(verify_path)})",
                "payload = {",
                "    'status': 'ok',",
                "    'tool': 'salesforce_dashboard_filter_automation',",
                "    'lane': 'native_surface_authoring',",
                "    'command_class': 'mutating',",
                "    'messages': [{'level': 'info', 'code': 'flow_complete', 'text': 'Authored and verified all planned dashboard filters.'}],",
                "    'artifacts': artifacts,",
                "    'command': 'run-filter-flow',",
                "    'summary': {",
                "        'through_stage': args.through,",
                "        'authored_filter_count': 3,",
                "        'manual_filter_verified_count': 3,",
                "        'target_dashboard_id': args.dashboard_id,",
                "        'target_org': args.target_org,",
                "    },",
                "}",
                "print(json.dumps(payload, indent=2))",
            ]
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)
    return script_path


def build_slow_dashboard_filter_automation_script(tmp_path: Path, *, sleep_seconds: int = 2) -> Path:
    script_path = tmp_path / "slow_dashboard_filter_automation.py"
    script_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import argparse, json, time",
                "from pathlib import Path",
                "parser = argparse.ArgumentParser()",
                "subparsers = parser.add_subparsers(dest='command', required=True)",
                "run = subparsers.add_parser('run-filter-flow')",
                "run.add_argument('--plan', required=True)",
                "run.add_argument('--target-org', required=True)",
                "run.add_argument('--dashboard-id', required=True)",
                "run.add_argument('--all-filters', action='store_true')",
                "run.add_argument('--through', required=True)",
                "run.add_argument('--verify-package', required=True)",
                "run.add_argument('--session', required=True)",
                "run.add_argument('--manual-filter-authoring-json', default=None)",
                "run.add_argument('--output-dir', default=None)",
                "run.add_argument('--verify-output-dir', default=None)",
                "run.add_argument('--json', action='store_true')",
                "args = parser.parse_args()",
                f"time.sleep({sleep_seconds})",
                "if args.output_dir:",
                "    output_dir = Path(args.output_dir)",
                "    output_dir.mkdir(parents=True, exist_ok=True)",
                "    (output_dir / 'run_filter_flow_invocation.json').write_text(json.dumps({'dashboard_id': args.dashboard_id}, indent=2), encoding='utf-8')",
                "print(json.dumps({'status': 'ok', 'tool': 'salesforce_dashboard_filter_automation', 'lane': 'native_surface_authoring', 'command_class': 'mutating', 'messages': [{'level': 'info', 'code': 'flow_complete', 'text': 'slow flow complete'}], 'artifacts': [], 'command': 'run-filter-flow'}))",
            ]
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)
    return script_path


def build_fake_probe_matrix_sf(fake_bin: Path, report_state: Path, dashboard_state: Path) -> Path:
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, os, sys",
                "from pathlib import Path",
                "args = sys.argv[1:]",
                "if args[:2] == ['org', 'display']:",
                "    raise SystemExit(1)",
                "path = next((item for item in args if item.startswith('/services/data/')), None)",
                "method = 'GET'",
                "if '--method' in args:",
                "    method = args[args.index('--method') + 1]",
                "report_state = json.loads(Path(os.environ['FAKE_BUILDER_REPORT_STATE']).read_text())",
                "dashboard_state_path = Path(os.environ['FAKE_BUILDER_DASHBOARD_STATE'])",
                "dashboard_state = json.loads(dashboard_state_path.read_text())",
                "if path and path.endswith('/analytics/reports/00OTBASELINEAAA/describe') and method == 'GET':",
                "    print(json.dumps({",
                "        'reportMetadata': {",
                "            'folderId': '00lTEST0000001AAA',",
                "            'reportType': {'type': 'Opportunity'},",
                "            'reportFormat': 'SUMMARY',",
                "            'groupingsDown': [{'name': 'OWNER_MANAGER'}, {'name': 'FULL_NAME'}],",
                "            'detailColumns': ['ACCOUNT_NAME', 'Account.Gain_Annual_Renewal_Date__c', 'Opportunity.Risk_Assessment_Level__c', 'Opportunity.APTS_RH_Product_Family__c'],",
                "            'reportFilters': [],",
                "            'sortBy': [],",
                "        },",
                "        'reportExtendedMetadata': {",
                "            'detailColumnInfo': {",
                "                'FULL_NAME': {'label': 'Opportunity Owner'},",
                "                'ACCOUNT_NAME': {'label': 'Account Name'},",
                "                'Account.Gain_Annual_Renewal_Date__c': {'label': 'Renewal Date'},",
                "                'Opportunity.Risk_Assessment_Level__c': {'label': 'Risk Assessment Level'},",
                "                'Opportunity.APTS_RH_Product_Family__c': {'label': 'Product Family'},",
                "            },",
                "            'groupingColumnInfo': {",
                "                'OWNER_MANAGER': {'label': 'Opportunity Owner: Manager'}",
                "            }",
                "        }",
                "    }))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/reports') and method == 'POST':",
                "    print(json.dumps({'id': '00OTPROBEAAA', 'name': 'Probe Report'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/reports/00OTPROBEAAA/describe') and method == 'GET':",
                "    if report_state['deleted']:",
                "        print(json.dumps([{'errorCode': 'NOT_FOUND', 'message': 'The data you’re trying to access is unavailable.'}]))",
                "        raise SystemExit(1)",
                "    print(json.dumps({",
                "        'reportMetadata': {",
                "            'folderId': '00lTEST0000001AAA',",
                "            'reportType': {'type': 'Opportunity'},",
                "            'reportFormat': 'SUMMARY',",
                "            'groupingsDown': [{'name': 'OWNER_MANAGER'}, {'name': 'FULL_NAME'}],",
                "            'detailColumns': ['ACCOUNT_NAME', 'Account.Gain_Annual_Renewal_Date__c', 'Opportunity.Risk_Assessment_Level__c', 'Opportunity.APTS_RH_Product_Family__c'],",
                "            'reportFilters': [],",
                "            'sortBy': [],",
                "        }",
                "    }))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/reports/00OTPROBEAAA') and method == 'DELETE':",
                "    Path(os.environ['FAKE_BUILDER_REPORT_STATE']).write_text(json.dumps({'deleted': True}))",
                "    print('{}')",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards') and method == 'GET':",
                "    print(json.dumps([]))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZBASELINEAAA') and method == 'GET':",
                "    print(json.dumps({",
                "        'dashboardMetadata': {",
                "            'folderId': '005QA000003DUwWYAW',",
                "            'components': [",
                "                {'header': 'Forecast & Closed Won', 'reportId': '00OTb000008TZaTMAW', 'properties': {'visualizationType': 'Metric', 'aggregates': ['RowCount']}},",
                "                {'header': 'Pipeline Coverage by Stage', 'reportId': '00OTb000008TZc5MAG', 'properties': {'visualizationType': 'Line', 'groupings': ['CLOSE_DATE']}},",
                "                {'header': 'Overdue Close Date — Open Opps', 'reportId': '00OTb000008TaBZMA0', 'properties': {'visualizationType': 'Table', 'columns': ['FULL_NAME']}},",
                "                {'header': 'Opportunity Win Rate (Close Rate)', 'reportId': '00OTESTRISKAAA', 'properties': {'visualizationType': 'Table', 'columns': ['FULL_NAME']}}",
                "            ],",
                "            'filters': []",
                "        }",
                "    }))",
                "    raise SystemExit(0)",
                "if path and 'analytics/dashboards?cloneId=01ZBASELINEAAA' in path and method == 'POST':",
                "    print(json.dumps({'id': '01ZDASHPROBEAAA', 'name': 'Probe Dashboard'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZDASHPROBEAAA') and method == 'PATCH':",
                "    print(json.dumps({'id': '01ZDASHPROBEAAA', 'name': 'Probe Dashboard'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZDASHPROBEAAA') and method == 'GET':",
                "    if dashboard_state['deleted']:",
                "        print(json.dumps([{'errorCode': 'ENTITY_IS_DELETED', 'message': 'entity is deleted'}]))",
                "        raise SystemExit(1)",
                "    print(json.dumps({'id': '01ZDASHPROBEAAA', 'name': 'Probe Dashboard', 'components': [], 'filters': []}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZDASHPROBEAAA') and method == 'DELETE':",
                "    dashboard_state['deleted'] = True",
                "    dashboard_state_path.write_text(json.dumps(dashboard_state))",
                "    print('{}')",
                "    raise SystemExit(0)",
                "print(json.dumps({'args': args}))",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)
    return fake_sf


def test_validate_builder_profiles() -> None:
    result = run_cli("validate", "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["coverage"]["surface_adapters"] == 3
    assert payload["coverage"]["build_modes"] == 4
    assert payload["coverage"]["patterns"] == 6
    assert payload["coverage"]["surface_exemplars"] == 8


def test_spec_prefers_salesforce_report_for_owner_list_query() -> None:
    result = run_cli(
        "spec",
        "--query",
        "Manager owner list report for renewals needing follow-up this week",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    spec = payload["spec"]
    assert spec["primary_surface"] == "salesforce_report"
    assert spec["build_mode"] == "new_surface"
    assert "owner list" in spec["decision_statement"].lower()
    assert spec["excellence_target"]["pattern_id"] == "owner_accountability_report"
    assert spec["retrieval_context"]["patterns"][0]["id"] == "owner_accountability_report"


def test_draft_builds_paired_crma_handoff_for_executive_view() -> None:
    result = run_cli(
        "draft",
        "--query",
        "Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    spec = payload["spec"]
    draft = payload["draft"]
    assert spec["primary_surface"] == "crma_dashboard"
    assert spec["secondary_surface"] == "salesforce_report"
    assert spec["build_mode"] == "paired_handoff"
    assert spec["excellence_target"]["target_id"] == "commercial_rhythm_control_tower"
    assert draft["shape"] == "crma_dashboard_spec"
    assert draft["page_model"] == ["Summary", "Ownership & Handoffs", "Process Quality"]
    assert "commercial_rhythm_control_tower" in draft["design_cues"]["reference_exemplars"]
    assert any(block["section"] == "action_layer" for block in draft["blocks"])


def test_draft_report_includes_owner_columns_and_handoff() -> None:
    result = run_cli(
        "draft",
        "--query",
        "Manager owner list report for renewals needing follow-up this week",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    draft = payload["draft"]
    assert draft["shape"] == "report_spec"
    assert draft["report_format"] == "tabular"
    assert "Owner" in draft["columns"]
    assert draft["handoff_surface"] == "crma_dashboard"
    assert draft["page_model"] == ["Queue / Follow-up"]


def test_critique_flags_bad_executive_surface_override() -> None:
    result = run_cli(
        "critique",
        "--query",
        "Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence",
        "--surface",
        "salesforce_report",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    finding_codes = {item["code"] for item in payload["critique"]["findings"]}
    assert "surface_fit" in finding_codes
    assert "executive_story" in finding_codes
    assert "excellence_pattern" in finding_codes
    assert "baseline_only" in finding_codes
    assert payload["critique"]["excellence_target"]["target_id"] == "owner_accountability_report"
    assert payload["critique"]["excellence_target"]["reference_exemplar_id"] == "commercial_rhythm_control_tower"
    assert payload["critique"]["score"] < 100
    assert payload["critique"]["verdict"] == "ready_for_revision"
    critic_ids = {item["critic"] for item in payload["critique"]["critic_reviews"]}
    assert critic_ids == {"surface_planner", "story_critic", "action_critic", "visual_critic"}


def test_retrieve_returns_ranked_local_context() -> None:
    result = run_cli(
        "retrieve",
        "--query",
        "Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    retrieval = payload["retrieval_context"]
    assert retrieval["patterns"][0]["id"] == "cross_suite_control_tower"
    assert retrieval["exemplars"][0]["id"] == "commercial_rhythm_control_tower"
    assert retrieval["design_cues"]


def test_revise_corrects_bad_executive_surface_override() -> None:
    result = run_cli(
        "revise",
        "--query",
        "Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence",
        "--surface",
        "salesforce_report",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["critique_before"]["status"] == "warn"
    assert payload["revised_spec"]["primary_surface"] == "crma_dashboard"
    assert payload["revised_spec"]["secondary_surface"] == "salesforce_report"
    assert payload["revised_spec"]["excellence_target"]["target_id"] == "commercial_rhythm_control_tower"
    assert payload["revised_draft"]["baseline_status"] == "critic_revised"
    assert payload["critique_after"]["status"] == "ok"
    assert payload["critique_after"]["score"] == 100
    assert payload["critique_after"]["verdict"] == "ready_for_build"
    assert any(item["critic"] == "visual_critic" for item in payload["critique_after"]["critic_reviews"])
    assert any(item["change"] == "surface_correction" for item in payload["revisions"])
    assert any(item["change"] == "story_page_model_alignment" for item in payload["revisions"])
    assert any(item["page"] == "Ownership & Handoffs" for item in payload["revised_draft"]["page_blueprint"])


def test_revise_promotes_manager_report_baseline() -> None:
    result = run_cli(
        "revise",
        "--query",
        "Manager owner list report for renewals needing follow-up this week",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["revised_spec"]["primary_surface"] == "salesforce_report"
    assert payload["revised_draft"]["report_format"] == "tabular"
    metrics = [item["metric"] for item in payload["revised_spec"]["metric_roles"]]
    assert any("Actual" in metric for metric in metrics)
    assert any("Variance" in metric for metric in metrics)
    assert any("Risk" in metric for metric in metrics)
    assert payload["critique_after"]["status"] == "ok"
    assert payload["critique_after"]["score"] == 100
    assert payload["critique_after"]["verdict"] == "ready_for_build"
    assert any(item["change"] == "metric_role_labels" for item in payload["revisions"])
    assert any(item["change"] == "action_handoff_enforced" for item in payload["revisions"])


def test_package_builds_crma_execution_handoff_for_executive_view() -> None:
    result = run_cli(
        "package",
        "--query",
        "Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence",
        "--surface",
        "salesforce_report",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    package = payload["build_package"]
    assert payload["status"] == "ok"
    assert package["package_status"] == "ready_for_execution"
    assert package["execution_lane"] == "crma_api_direct"
    assert package["repo_execution_fit"] == "strong"
    assert package["surface_contract"]["surface_type"] == "crma_dashboard"
    assert len(package["surface_contract"]["page_storyboard"]) == 3
    assert package["execution_plan"]["plan_type"] == "wave_patch_plan"
    assert package["execution_plan"]["phases"][1]["phase"] == "storyboard_patch"
    patch_ops = package["execution_plan"]["phases"][1]["patch_operations"]
    patch_payload = package["execution_plan"]["phases"][1]["wave_patch_payload"]
    assert any(item["action"] == "ensure_page_scaffold" for item in patch_ops)
    assert any(item["action"] == "wire_handoff_link" for item in patch_ops)
    assert any(
        item["action"] == "upsert_section_widgets"
        and any(widget["column_map_strategy"] == "explicit_full" for widget in item["widgets"])
        for item in patch_ops
    )
    assert patch_payload["payload_type"] == "wave_patch_payload"
    assert len(patch_payload["page_mutations"]) == 3
    assert patch_payload["navigation_contract"]["mode"] == "multi_page"
    assert package["surface_contract"]["handoff_target"]["surface_type"] == "salesforce_report"
    assert package["surface_contract"]["handoff_target"]["destination_type"] == "report"
    assert patch_payload["handoff_link"]["destination_type"] == "report"
    assert any(
        widget["column_map_strategy"] == "explicit_full"
        for page in patch_payload["page_mutations"]
        for section in page["section_mutations"]
        for widget in section["widget_mutations"]
    )
    assert any(item["critic"] == "story_critic" for item in package["critic_rationale"])
    assert any("page model within the planned budget" in item for item in package["design_constraints"])
    assert any("Wave API" in step for step in package["next_steps"])


def test_package_builds_report_execution_handoff_for_manager_queue() -> None:
    result = run_cli(
        "package",
        "--query",
        "Manager owner list report for renewals needing follow-up this week",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    package = payload["build_package"]
    assert payload["status"] == "ok"
    assert package["package_status"] == "ready_for_execution"
    assert package["execution_lane"] == "salesforce_report_handoff"
    assert package["repo_execution_fit"] == "partial"
    assert package["surface_contract"]["surface_type"] == "salesforce_report"
    assert package["surface_contract"]["report_format"] == "tabular"
    assert package["surface_contract"]["handoff_surface"] == "crma_dashboard"
    assert package["surface_contract"]["handoff_target"]["surface_type"] == "crma_dashboard"
    assert package["surface_contract"]["handoff_target"]["destination_type"] == "dashboard"
    assert package["execution_plan"]["plan_type"] == "salesforce_report_authoring_skeleton"
    assert package["execution_plan"]["phases"][0]["phase"] == "report_core"
    assert any(item["critic"] == "action_critic" for item in package["critic_rationale"])
    assert any("real queue" in item for item in package["design_constraints"])
    assert any("queue-first" in step for step in package["next_steps"])


def test_handoff_builds_crma_executor_plan_and_artifact(tmp_path: Path) -> None:
    result = run_cli(
        "handoff",
        "--query",
        "Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence",
        "--surface",
        "salesforce_report",
        "--output-dir",
        str(tmp_path),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    handoff = payload["executor_handoff"]
    assert payload["status"] == "ok"
    assert handoff["primary_lane"] == "crma_api_direct"
    assert handoff["repo_execution_fit"] == "strong"
    assert Path(handoff["package_artifact"]).exists()
    assert Path(handoff["execution_plan_artifact"]).exists()
    assert Path(handoff["wave_patch_payload_artifact"]).exists()
    assert handoff["execution_plan"]["plan_type"] == "wave_patch_plan"
    assert any(
        item["action"] == "run_patch_validation"
        for item in handoff["execution_plan"]["phases"][1]["patch_operations"]
    )
    assert handoff["wave_patch_payload"]["payload_type"] == "wave_patch_payload"
    assert handoff["wave_patch_payload"]["handoff_link"]["target_surface"] == "salesforce_report"
    assert handoff["wave_patch_payload"]["handoff_link"]["destination_type"] == "report"
    assert any(item["critic"] == "surface_planner" for item in handoff["critic_rationale"])
    assert any("chosen primary surface" in item for item in handoff["design_constraints"])
    assert any(item["name"] == "export_live_crma_assets" for item in handoff["available_commands"])
    assert any(item["name"] == "contract_lint" for item in handoff["available_commands"])
    assert any(item["name"] == "wave_patch_executor" for item in handoff["available_commands"])
    assert any("bundle" in item["command"] for item in handoff["available_commands"] if item["name"] == "wave_patch_executor")
    assert any("deploy --state" in item["command"] for item in handoff["available_commands"] if item["name"] == "wave_patch_executor")
    assert any("--apply" in step for step in handoff["external_steps"])


def test_handoff_builds_report_executor_plan_and_artifact(tmp_path: Path) -> None:
    result = run_cli(
        "handoff",
        "--query",
        "Manager owner list report for renewals needing follow-up this week",
        "--output-dir",
        str(tmp_path),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    handoff = payload["executor_handoff"]
    assert payload["status"] == "ok"
    assert handoff["primary_lane"] == "salesforce_report_handoff"
    assert handoff["repo_execution_fit"] == "partial"
    assert Path(handoff["package_artifact"]).exists()
    assert Path(handoff["execution_plan_artifact"]).exists()
    assert handoff["execution_plan"]["plan_type"] == "salesforce_report_authoring_skeleton"
    assert any(item["critic"] == "action_critic" for item in handoff["critic_rationale"])
    assert any("operating path" in item for item in handoff["design_constraints"])
    assert any(item["name"] == "export_live_crma_assets" for item in handoff["available_commands"])
    assert any(item["name"] == "salesforce_report_executor" for item in handoff["available_commands"])
    assert any("validate --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_report_executor")
    assert any("bundle --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_report_executor")
    assert any("preview --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_report_executor")
    assert any("verify --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_report_executor")
    assert any("apply --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_report_executor")
    assert any("complete --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_report_executor")
    assert any("delete --report-id" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_report_executor")
    assert any("Implement the emitted Salesforce report bundle" in step for step in handoff["external_steps"])


def test_handoff_builds_dashboard_executor_plan_and_artifact(tmp_path: Path) -> None:
    result = run_cli(
        "handoff",
        "--query",
        "Native dashboard headline rollup for manager forecast inspection",
        "--output-dir",
        str(tmp_path),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    handoff = payload["executor_handoff"]
    assert payload["status"] == "warn"
    assert handoff["primary_lane"] == "salesforce_dashboard_handoff"
    assert handoff["repo_execution_fit"] == "partial"
    assert Path(handoff["package_artifact"]).exists()
    assert Path(handoff["execution_plan_artifact"]).exists()
    assert handoff["execution_plan"]["plan_type"] == "salesforce_dashboard_authoring_skeleton"
    assert any(item["name"] == "salesforce_dashboard_executor" for item in handoff["available_commands"])
    assert any("validate --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_dashboard_executor")
    assert any("bundle --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_dashboard_executor")
    assert any("preview --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_dashboard_executor")
    assert any("verify --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_dashboard_executor")
    assert any("apply --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_dashboard_executor")
    assert any("complete --package" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_dashboard_executor")
    assert any("delete --dashboard-id" in item["command"] for item in handoff["available_commands"] if item["name"] == "salesforce_dashboard_executor")
    assert any("native Salesforce dashboard bundle" in step for step in handoff["external_steps"])


def test_probe_runs_report_complete_and_cleanup(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_state = tmp_path / "report_probe_state.json"
    fake_state.write_text(json.dumps({"deleted": False}), encoding="utf-8")
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, os, sys",
                "from pathlib import Path",
                "args = sys.argv[1:]",
                "if args[:2] == ['org', 'display']:",
                "    raise SystemExit(1)",
                "path = next((item for item in args if item.startswith('/services/data/')), None)",
                "method = 'GET'",
                "if '--method' in args:",
                "    method = args[args.index('--method') + 1]",
                "state_path = Path(os.environ['FAKE_BUILDER_REPORT_STATE'])",
                "state = json.loads(state_path.read_text())",
                "if path and path.endswith('/analytics/reports/00OTBASELINEAAA/describe') and method == 'GET':",
                "    print(json.dumps({",
                "        'reportMetadata': {",
                "            'folderId': '00lTEST0000001AAA',",
                "            'reportType': {'type': 'Opportunity'},",
                "            'reportFormat': 'SUMMARY',",
                "            'groupingsDown': [{'name': 'OWNER_MANAGER'}, {'name': 'FULL_NAME'}],",
                "            'detailColumns': ['ACCOUNT_NAME', 'Account.Gain_Annual_Renewal_Date__c', 'Opportunity.Risk_Assessment_Level__c', 'Opportunity.APTS_RH_Product_Family__c'],",
                "            'reportFilters': [],",
                "            'sortBy': [],",
                "        },",
                "        'reportExtendedMetadata': {",
                "            'detailColumnInfo': {",
                "                'FULL_NAME': {'label': 'Opportunity Owner'},",
                "                'ACCOUNT_NAME': {'label': 'Account Name'},",
                "                'Account.Gain_Annual_Renewal_Date__c': {'label': 'Renewal Date'},",
                "                'Opportunity.Risk_Assessment_Level__c': {'label': 'Risk Assessment Level'},",
                "                'Opportunity.APTS_RH_Product_Family__c': {'label': 'Product Family'},",
                "            },",
                "            'groupingColumnInfo': {",
                "                'OWNER_MANAGER': {'label': 'Opportunity Owner: Manager'}",
                "            }",
                "        }",
                "    }))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/reports') and method == 'POST':",
                "    print(json.dumps({'id': '00OTPROBEAAA', 'name': 'Probe Report'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/reports/00OTPROBEAAA/describe') and method == 'GET':",
                "    if state['deleted']:",
                "        print(json.dumps([{'errorCode': 'NOT_FOUND', 'message': 'The data you’re trying to access is unavailable.'}]))",
                "        raise SystemExit(1)",
                "    print(json.dumps({",
                "        'reportMetadata': {",
                "            'folderId': '00lTEST0000001AAA',",
                "            'reportType': {'type': 'Opportunity'},",
                "            'reportFormat': 'SUMMARY',",
                "            'groupingsDown': [{'name': 'OWNER_MANAGER'}, {'name': 'FULL_NAME'}],",
                "            'detailColumns': ['ACCOUNT_NAME', 'Account.Gain_Annual_Renewal_Date__c', 'Opportunity.Risk_Assessment_Level__c', 'Opportunity.APTS_RH_Product_Family__c'],",
                "            'reportFilters': [],",
                "            'sortBy': [],",
                "        }",
                "    }))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/reports/00OTPROBEAAA') and method == 'DELETE':",
                "    state['deleted'] = True",
                "    state_path.write_text(json.dumps(state))",
                "    print('{}')",
                "    raise SystemExit(0)",
                "print(json.dumps({'args': args}))",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)

    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_BUILDER_REPORT_STATE"] = str(fake_state)

    output_dir = tmp_path / "report_probe"
    result = run_cli(
        "probe",
        "--query",
        "Manager owner list report for renewals needing follow-up this week",
        "--target-org",
        "apro@simcorp.com",
        "--clone-from-report-id",
        "00OTBASELINEAAA",
        "--cleanup",
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "mutating"
    assert payload["summary"]["primary_lane"] == "salesforce_report_handoff"
    assert payload["summary"]["execution_status"] == "ok"
    assert payload["summary"]["cleanup_requested"] is True
    assert payload["summary"]["cleanup_status"] == "ok"
    assert payload["summary"]["created_asset_id"] == "00OTPROBEAAA"
    assert payload["execution_result"]["applied_report"]["id"] == "00OTPROBEAAA"
    assert payload["cleanup_result"]["summary"]["deleted_report_id"] == "00OTPROBEAAA"
    assert (output_dir / "00_handoff" / "build_package.json").exists()
    assert (output_dir / "01_complete" / "01_apply" / "salesforce_report_apply_response.json").exists()
    assert (output_dir / "02_delete" / "salesforce_report_delete_verify.json").exists()


def test_probe_runs_dashboard_complete_and_cleanup(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_state = tmp_path / "dashboard_probe_state.json"
    fake_state.write_text(json.dumps({"deleted": False}), encoding="utf-8")
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, os, sys",
                "from pathlib import Path",
                "args = sys.argv[1:]",
                "if args[:2] == ['org', 'display']:",
                "    raise SystemExit(1)",
                "path = next((item for item in args if item.startswith('/services/data/')), None)",
                "method = 'GET'",
                "if '--method' in args:",
                "    method = args[args.index('--method') + 1]",
                "state_path = Path(os.environ['FAKE_BUILDER_DASHBOARD_STATE'])",
                "state = json.loads(state_path.read_text())",
                "if path and path.endswith('/analytics/dashboards') and method == 'GET':",
                "    print(json.dumps([]))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZBASELINEAAA') and method == 'GET':",
                "    print(json.dumps({",
                "        'dashboardMetadata': {",
                "            'folderId': '005QA000003DUwWYAW',",
                "            'components': [",
                "                {'header': 'Forecast & Closed Won', 'reportId': '00OTb000008TZaTMAW', 'properties': {'visualizationType': 'Metric', 'aggregates': ['RowCount']}},",
                "                {'header': 'Pipeline Coverage by Stage', 'reportId': '00OTb000008TZc5MAG', 'properties': {'visualizationType': 'Line', 'groupings': ['CLOSE_DATE']}},",
                "                {'header': 'Overdue Close Date — Open Opps', 'reportId': '00OTb000008TaBZMA0', 'properties': {'visualizationType': 'Table', 'columns': ['FULL_NAME']}},",
                "                {'header': 'Opportunity Win Rate (Close Rate)', 'reportId': '00OTESTRISKAAA', 'properties': {'visualizationType': 'Table', 'columns': ['FULL_NAME']}}",
                "            ],",
                "            'filters': []",
                "        }",
                "    }))",
                "    raise SystemExit(0)",
                "if path and 'analytics/dashboards?cloneId=01ZBASELINEAAA' in path and method == 'POST':",
                "    print(json.dumps({'id': '01ZDASHPROBEAAA', 'name': 'Probe Dashboard'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZDASHPROBEAAA') and method == 'PATCH':",
                "    print(json.dumps({'id': '01ZDASHPROBEAAA', 'name': 'Probe Dashboard'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZDASHPROBEAAA') and method == 'GET':",
                "    if state['deleted']:",
                "        print(json.dumps([{'errorCode': 'ENTITY_IS_DELETED', 'message': 'entity is deleted'}]))",
                "        raise SystemExit(1)",
                "    print(json.dumps({'id': '01ZDASHPROBEAAA', 'name': 'Probe Dashboard', 'components': [], 'filters': []}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZDASHPROBEAAA') and method == 'DELETE':",
                "    state['deleted'] = True",
                "    state_path.write_text(json.dumps(state))",
                "    print('{}')",
                "    raise SystemExit(0)",
                "print(json.dumps({'args': args}))",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)
    fake_filter_automation = build_fake_dashboard_filter_automation_script(tmp_path)

    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_BUILDER_DASHBOARD_STATE"] = str(fake_state)

    output_dir = tmp_path / "dashboard_probe"
    result = run_cli(
        "probe",
        "--query",
        "Native dashboard headline rollup for manager forecast inspection",
        "--target-org",
        "apro@simcorp.com",
        "--clone-from-dashboard-id",
        "01ZBASELINEAAA",
        "--session",
        "dashboard_probe_session",
        "--dashboard-filter-automation-script",
        str(fake_filter_automation),
        "--cleanup",
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "mutating"
    assert payload["summary"]["primary_lane"] == "salesforce_dashboard_handoff"
    assert payload["summary"]["execution_status"] == "ok"
    assert payload["summary"]["cleanup_requested"] is True
    assert payload["summary"]["cleanup_status"] == "ok"
    assert payload["summary"]["created_asset_id"] == "01ZDASHPROBEAAA"
    assert payload["execution_result"]["applied_dashboard"]["id"] == "01ZDASHPROBEAAA"
    assert payload["cleanup_result"]["summary"]["deleted_dashboard_id"] == "01ZDASHPROBEAAA"
    assert (output_dir / "00_handoff" / "build_package.json").exists()
    assert (output_dir / "01_complete" / "01_apply" / "salesforce_dashboard_apply_response.json").exists()
    assert (output_dir / "01_complete" / "02_filter_flow" / "run_filter_flow_invocation.json").exists()
    assert (output_dir / "02_delete" / "salesforce_dashboard_delete_verify.json").exists()


def test_probe_accepts_explicit_dashboard_package(tmp_path: Path) -> None:
    seed_dir = tmp_path / "seed_handoff"
    seed = run_cli(
        "handoff",
        "--query",
        "Native dashboard headline rollup for manager forecast inspection",
        "--output-dir",
        str(seed_dir),
        "--json",
    )
    assert seed.returncode == 0, seed.stderr or seed.stdout
    package_path = seed_dir / "build_package.json"
    assert package_path.exists()

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_state = tmp_path / "dashboard_probe_state.json"
    fake_state.write_text(json.dumps({"deleted": False}), encoding="utf-8")
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, os, sys",
                "from pathlib import Path",
                "args = sys.argv[1:]",
                "if args[:2] == ['org', 'display']:",
                "    raise SystemExit(1)",
                "path = next((item for item in args if item.startswith('/services/data/')), None)",
                "method = 'GET'",
                "if '--method' in args:",
                "    method = args[args.index('--method') + 1]",
                "state_path = Path(os.environ['FAKE_BUILDER_DASHBOARD_STATE'])",
                "state = json.loads(state_path.read_text())",
                "if path and path.endswith('/analytics/dashboards') and method == 'GET':",
                "    print(json.dumps([]))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZBASELINEAAA') and method == 'GET':",
                "    print(json.dumps({",
                "        'dashboardMetadata': {",
                "            'folderId': '005QA000003DUwWYAW',",
                "            'components': [",
                "                {'header': 'Forecast & Closed Won', 'reportId': '00OTb000008TZaTMAW', 'properties': {'visualizationType': 'Metric', 'aggregates': ['RowCount']}},",
                "                {'header': 'Pipeline Coverage by Stage', 'reportId': '00OTb000008TZc5MAG', 'properties': {'visualizationType': 'Line', 'groupings': ['CLOSE_DATE']}},",
                "                {'header': 'Overdue Close Date — Open Opps', 'reportId': '00OTb000008TaBZMA0', 'properties': {'visualizationType': 'Table', 'columns': ['FULL_NAME']}},",
                "                {'header': 'Opportunity Win Rate (Close Rate)', 'reportId': '00OTESTRISKAAA', 'properties': {'visualizationType': 'Table', 'columns': ['FULL_NAME']}}",
                "            ],",
                "            'filters': []",
                "        }",
                "    }))",
                "    raise SystemExit(0)",
                "if path and 'analytics/dashboards?cloneId=01ZBASELINEAAA' in path and method == 'POST':",
                "    print(json.dumps({'id': '01ZDASHPROBEAAA', 'name': 'Probe Dashboard'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZDASHPROBEAAA') and method == 'PATCH':",
                "    print(json.dumps({'id': '01ZDASHPROBEAAA', 'name': 'Probe Dashboard'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZDASHPROBEAAA') and method == 'GET':",
                "    if state['deleted']:",
                "        print(json.dumps([{'errorCode': 'ENTITY_IS_DELETED', 'message': 'entity is deleted'}]))",
                "        raise SystemExit(1)",
                "    print(json.dumps({'id': '01ZDASHPROBEAAA', 'name': 'Probe Dashboard', 'components': [], 'filters': []}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZDASHPROBEAAA') and method == 'DELETE':",
                "    state['deleted'] = True",
                "    state_path.write_text(json.dumps(state))",
                "    print('{}')",
                "    raise SystemExit(0)",
                "print(json.dumps({'args': args}))",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)
    fake_filter_automation = build_fake_dashboard_filter_automation_script(tmp_path)

    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_BUILDER_DASHBOARD_STATE"] = str(fake_state)

    output_dir = tmp_path / "dashboard_probe_from_package"
    result = run_cli(
        "probe",
        "--package",
        str(package_path),
        "--target-org",
        "apro@simcorp.com",
        "--clone-from-dashboard-id",
        "01ZBASELINEAAA",
        "--session",
        "dashboard_probe_session",
        "--dashboard-filter-automation-script",
        str(fake_filter_automation),
        "--cleanup",
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["package_source"] == "provided_package"
    assert payload["summary"]["created_asset_id"] == "01ZDASHPROBEAAA"
    assert (output_dir / "00_handoff" / "build_package.json").exists()
    assert (output_dir / "00_handoff" / "provided_package_reference.json").exists()


def test_probe_timeout_recovers_dashboard_cleanup(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_state = tmp_path / "dashboard_probe_state.json"
    fake_state.write_text(json.dumps({"deleted": False}), encoding="utf-8")
    build_fake_probe_matrix_sf(fake_bin, tmp_path / "unused_report_state.json", fake_state)
    slow_filter_automation = build_slow_dashboard_filter_automation_script(tmp_path, sleep_seconds=2)

    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_BUILDER_REPORT_STATE"] = str(tmp_path / "unused_report_state.json")
    env["FAKE_BUILDER_DASHBOARD_STATE"] = str(fake_state)
    (tmp_path / "unused_report_state.json").write_text(json.dumps({"deleted": False}), encoding="utf-8")

    output_dir = tmp_path / "dashboard_probe_timeout"
    result = run_cli(
        "probe",
        "--query",
        "Native dashboard headline rollup for manager forecast inspection",
        "--target-org",
        "apro@simcorp.com",
        "--clone-from-dashboard-id",
        "01ZBASELINEAAA",
        "--session",
        "dashboard_probe_timeout_session",
        "--dashboard-filter-automation-script",
        str(slow_filter_automation),
        "--executor-timeout-seconds",
        "1",
        "--cleanup",
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )

    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["summary"]["created_asset_id"] == "01ZDASHPROBEAAA"
    assert payload["summary"]["cleanup_status"] == "ok"
    assert any(item["code"] == "probe_asset_recovered" for item in payload["messages"])
    assert payload["cleanup_result"]["summary"]["deleted_dashboard_id"] == "01ZDASHPROBEAAA"
    assert (output_dir / "01_complete" / "01_apply" / "salesforce_dashboard_apply_response.json").exists()
    assert (output_dir / "02_delete" / "salesforce_dashboard_delete_verify.json").exists()


def test_probe_matrix_runs_report_and_dashboard_entries(tmp_path: Path) -> None:
    report_seed_dir = tmp_path / "report_seed"
    report_seed = run_cli(
        "handoff",
        "--query",
        "Manager owner list report for renewals needing follow-up this week",
        "--output-dir",
        str(report_seed_dir),
        "--json",
    )
    assert report_seed.returncode == 0, report_seed.stderr or report_seed.stdout

    dashboard_seed_dir = tmp_path / "dashboard_seed"
    dashboard_seed = run_cli(
        "handoff",
        "--query",
        "Native dashboard headline rollup for manager forecast inspection",
        "--output-dir",
        str(dashboard_seed_dir),
        "--json",
    )
    assert dashboard_seed.returncode == 0, dashboard_seed.stderr or dashboard_seed.stdout

    manifest_path = tmp_path / "probe_matrix_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "defaults": {
                    "target_org": "apro@simcorp.com",
                    "cleanup": True,
                },
                "probes": [
                    {
                        "name": "manager_report_probe",
                        "package": str(report_seed_dir / "build_package.json"),
                        "clone_from_report_id": "00OTBASELINEAAA",
                    },
                    {
                        "name": "manager_dashboard_probe",
                        "package": str(dashboard_seed_dir / "build_package.json"),
                        "clone_from_dashboard_id": "01ZBASELINEAAA",
                        "session": "dashboard_probe_matrix_session",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    report_state = tmp_path / "report_probe_state.json"
    report_state.write_text(json.dumps({"deleted": False}), encoding="utf-8")
    dashboard_state = tmp_path / "dashboard_probe_state.json"
    dashboard_state.write_text(json.dumps({"deleted": False}), encoding="utf-8")
    build_fake_probe_matrix_sf(fake_bin, report_state, dashboard_state)
    fake_filter_automation = build_fake_dashboard_filter_automation_script(tmp_path)

    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_BUILDER_REPORT_STATE"] = str(report_state)
    env["FAKE_BUILDER_DASHBOARD_STATE"] = str(dashboard_state)

    output_dir = tmp_path / "probe_matrix"
    result = run_cli(
        "probe-matrix",
        "--manifest",
        str(manifest_path),
        "--dashboard-filter-automation-script",
        str(fake_filter_automation),
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "mutating"
    assert payload["summary"]["completed"] == 2
    assert payload["summary"]["ok_count"] == 2
    assert payload["summary"]["cleanup_requested_count"] == 2
    assert payload["summary"]["stopped_early"] is False
    assert len(payload["probe_runs"]) == 2
    assert payload["probe_runs"][0]["summary"]["created_asset_id"] == "00OTPROBEAAA"
    assert payload["probe_runs"][1]["summary"]["created_asset_id"] == "01ZDASHPROBEAAA"
    assert (output_dir / "probe_matrix_manifest.json").exists()
    assert (output_dir / "probe_matrix_summary.json").exists()
    assert (output_dir / "01_manager_report_probe" / "probe_result.json").exists()
    assert (output_dir / "02_manager_dashboard_probe" / "probe_result.json").exists()


def test_probe_matrix_resolves_manifest_relative_paths(tmp_path: Path) -> None:
    report_seed_dir = tmp_path / "report_seed"
    report_seed = run_cli(
        "handoff",
        "--query",
        "Manager owner list report for renewals needing follow-up this week",
        "--output-dir",
        str(report_seed_dir),
        "--json",
    )
    assert report_seed.returncode == 0, report_seed.stderr or report_seed.stdout

    dashboard_seed_dir = tmp_path / "dashboard_seed"
    dashboard_seed = run_cli(
        "handoff",
        "--query",
        "Native dashboard headline rollup for manager forecast inspection",
        "--output-dir",
        str(dashboard_seed_dir),
        "--json",
    )
    assert dashboard_seed.returncode == 0, dashboard_seed.stderr or dashboard_seed.stdout

    manifest_dir = tmp_path / "manifest_suite"
    fixtures_dir = manifest_dir / "fixtures"
    fixtures_dir.mkdir(parents=True)
    (fixtures_dir / "manager_report_probe.build_package.json").write_text(
        (report_seed_dir / "build_package.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (fixtures_dir / "manager_dashboard_probe.build_package.json").write_text(
        (dashboard_seed_dir / "build_package.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    build_fake_dashboard_filter_automation_script(manifest_dir)

    manifest_path = manifest_dir / "probe_matrix_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "defaults": {
                    "target_org": "apro@simcorp.com",
                    "cleanup": True,
                    "dashboard_filter_automation_script": "./fake_dashboard_filter_automation.py",
                },
                "probes": [
                    {
                        "name": "manager_report_probe",
                        "package": "./fixtures/manager_report_probe.build_package.json",
                        "clone_from_report_id": "00OTBASELINEAAA",
                    },
                    {
                        "name": "manager_dashboard_probe",
                        "package": "./fixtures/manager_dashboard_probe.build_package.json",
                        "clone_from_dashboard_id": "01ZBASELINEAAA",
                        "session": "dashboard_probe_matrix_relative_session",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    report_state = tmp_path / "report_probe_state.json"
    report_state.write_text(json.dumps({"deleted": False}), encoding="utf-8")
    dashboard_state = tmp_path / "dashboard_probe_state.json"
    dashboard_state.write_text(json.dumps({"deleted": False}), encoding="utf-8")
    build_fake_probe_matrix_sf(fake_bin, report_state, dashboard_state)

    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_BUILDER_REPORT_STATE"] = str(report_state)
    env["FAKE_BUILDER_DASHBOARD_STATE"] = str(dashboard_state)

    output_dir = tmp_path / "probe_matrix_relative"
    result = run_cli(
        "probe-matrix",
        "--manifest",
        str(manifest_path),
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["completed"] == 2
    assert payload["summary"]["ok_count"] == 2
    assert payload["probe_runs"][0]["summary"]["created_asset_id"] == "00OTPROBEAAA"
    assert payload["probe_runs"][1]["summary"]["created_asset_id"] == "01ZDASHPROBEAAA"
    invocation_path = output_dir / "02_manager_dashboard_probe" / "01_complete" / "02_filter_flow" / "run_filter_flow_invocation.json"
    assert invocation_path.exists()
    invocation = json.loads(invocation_path.read_text(encoding="utf-8"))
    assert invocation["session"] == "dashboard_probe_matrix_relative_session"
    assert invocation["through"] == "verify-dashboard"
    assert Path(invocation["plan"]).exists()
