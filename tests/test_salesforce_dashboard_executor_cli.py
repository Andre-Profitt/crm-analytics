from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import pytest

from scripts import salesforce_dashboard_executor as dashboard_executor

ROOT = Path(__file__).resolve().parents[1]
EXECUTOR = ROOT / "scripts" / "salesforce_dashboard_executor.py"


def run_executor(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    effective_env = os.environ.copy()
    if env:
        effective_env.update(env)
    effective_env.setdefault("CRM_AI_MEMORY_ROOT", tempfile.mkdtemp(prefix="crm_ai_dashboard_memory_"))
    return subprocess.run(
        [sys.executable, str(EXECUTOR), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=effective_env,
    )


def write_evaluation_artifact(tmp_path: Path, *, verdict: str = "pass") -> Path:
    evaluation_path = tmp_path / f"evaluation_{verdict}.json"
    evaluation_path.write_text(
        json.dumps(
            {
                "run_id": "run_20260329_001",
                "verdict": verdict,
                "mutation_ready": verdict == "pass",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return evaluation_path


def build_dashboard_package(tmp_path: Path, *, evaluation_path: Path | None = None) -> Path:
    package_path = tmp_path / "build_package.json"
    planning_context = None
    if evaluation_path is not None:
        evaluation_payload = json.loads(evaluation_path.read_text(encoding="utf-8"))
        planning_context = {
            "run_id": "run_20260329_001",
            "goal": "fix broken forecast dashboard filters",
            "persona": "manager",
            "domain": "revenue",
            "operation": "mutate_dashboard",
            "surface_type": "salesforce_dashboard",
            "candidate_surface_id": "forecast_revenue_motions",
            "evaluation_path": str(evaluation_path),
            "evaluation_verdict": evaluation_payload["verdict"],
        }
    package_path.write_text(
        json.dumps(
            {
                "package_version": 1,
                "package_status": "ready_for_execution",
                "execution_lane": "salesforce_dashboard_handoff",
                "repo_execution_fit": "partial",
                "delivery_mode": "native_dashboard_authoring",
                "build_brief": {
                    "persona": "manager",
                    "domain": "revenue",
                    "decision_statement": "Build a native Salesforce dashboard for manager forecast inspection.",
                    "build_mode": "new_surface",
                    "excellence_target": "lightweight native dashboard",
                    "reference_exemplar": "Manager Forecast Snapshot",
                },
                "surface_contract": {
                    "surface_type": "salesforce_dashboard",
                    "filters": ["fiscal_period", "manager", "forecast_category"],
                    "handoff_surface": "salesforce_report",
                    "page_model": ["Summary"],
                    "page_storyboard": [
                        {
                            "page": "Summary",
                            "purpose": "State the current forecast posture and the top exceptions.",
                            "emphasis_metric": "Actual Forecast & Closed Won",
                            "sections": [
                                {
                                    "section": "headline_story",
                                    "intent": "Show the current position.",
                                    "widgets": [
                                        {
                                            "role": "headline_metric",
                                            "widget": "number",
                                            "metric": "Actual Forecast & Closed Won",
                                        },
                                        {
                                            "role": "driver_metric",
                                            "widget": "line",
                                            "metric": "Variance Driver: Pipeline Coverage",
                                        },
                                    ],
                                },
                                {
                                    "section": "action_layer",
                                    "intent": "Make the follow-up path explicit.",
                                    "widgets": [
                                        {
                                            "role": "action_metric",
                                            "widget": "comparisontable",
                                            "metric": "Action Queue: Close-Date Risk",
                                        }
                                    ],
                                },
                            ],
                        }
                    ],
                    "handoff_target": {
                        "surface_type": "salesforce_report",
                        "destination_type": "report",
                        "target_surface_id": "00OTb000008TZaTMAW",
                        "target_surface_label": "Forecast & Closed Won",
                        "target_destination_name": "00OTb000008TZaTMAW",
                        "resolution_source": "builder_brain_handoff_targets",
                    },
                },
                "review_gates": ["decision_fit", "visual_fit", "action_layer"],
                "design_constraints": [
                    "Keep the dashboard lightweight.",
                    "Preserve the report handoff.",
                ],
                "acceptance_criteria": [
                    "Dashboard stays single-view and scan-fast.",
                    "Follow-up report handoff is real.",
                ],
                "revision_summary": ["metric_role_labels"],
                **({"planning_context": planning_context} if planning_context else {}),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return package_path


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
                "    invocation_payload = {",
                "        'plan': args.plan,",
                "        'target_org': args.target_org,",
                "        'dashboard_id': args.dashboard_id,",
                "        'all_filters': args.all_filters,",
                "        'through': args.through,",
                "        'verify_package': args.verify_package,",
                "        'session': args.session,",
                "        'manual_filter_authoring_json': args.manual_filter_authoring_json,",
                "    }",
                "    invocation_path.write_text(json.dumps(invocation_payload, indent=2), encoding='utf-8')",
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


def test_discover_dashboard_baseline_candidates_prefers_filter_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    preview = {
        "requests": [
            {
                "id": "patch_dashboard",
                "body": {
                    "components": [
                        {"reportId": "00OTb000008TZaTMAW"},
                        {"reportId": "00OTb000008TZc5MAG"},
                        {"reportId": "00OTb000008TaBZMA0"},
                    ]
                },
            }
        ],
        "manual_filter_intents": [
            {"source_label": "fiscal_period", "proposed_filter": {"name": "Close Date"}},
            {"source_label": "manager", "proposed_filter": {"name": "Opportunity Owner"}},
            {"source_label": "forecast_category", "proposed_filter": {"name": "Forecast Category"}},
        ],
    }

    dashboard_index = [
        {"id": "current"},
        {"id": "match-heavy"},
        {"id": "match-light"},
        {"id": "filter-irrelevant"},
        {"id": "no-filters"},
    ]
    describe_payloads = {
        "match-heavy": {
            "name": "NA Sales Mgmt - Pipeline review 2026",
            "components": [{"reportId": "x1"}] * 16,
            "filters": [
                {"name": "Team / Opportunity Owner"},
                {"name": "Fiscal Period"},
                {"name": "Current Stage"},
            ],
        },
        "match-light": {
            "name": "Engagement",
            "components": [
                {"reportId": "00OTb000008TaBZMA0"},
                {"reportId": "y1"},
                {"reportId": "y2"},
                {"reportId": "y3"},
            ],
            "filters": [
                {"name": "Opportunity Owner"},
            ],
        },
        "filter-irrelevant": {
            "name": "Engagement",
            "components": [
                {"reportId": "y1"},
                {"reportId": "y2"},
                {"reportId": "y3"},
                {"reportId": "y4"},
            ],
            "filters": [
                {"name": "Account Name: Sales Region"},
            ],
        },
        "no-filters": {
            "name": "Operational Work Done",
            "components": [{"reportId": "00OTb000008TaBZMA0"}] * 6,
            "filters": [],
        },
    }

    def fake_run_rest_request_any(path: str, *, target_org: str | None, method: str = "GET", body=None):
        assert target_org == "apro@simcorp.com"
        assert method == "GET"
        assert path.endswith("/analytics/dashboards")
        return dashboard_index

    def fake_fetch_rest_json(path: str, *, target_org: str | None):
        dashboard_id = path.rsplit("/", 2)[-2] if path.endswith("/describe") else path.rsplit("/", 1)[-1]
        return describe_payloads[dashboard_id]

    monkeypatch.setattr(dashboard_executor, "_run_rest_request_any", fake_run_rest_request_any)
    monkeypatch.setattr(dashboard_executor, "_fetch_rest_json", fake_fetch_rest_json)

    candidates = dashboard_executor._discover_dashboard_baseline_candidates(
        preview=preview,
        target_org="apro@simcorp.com",
        current_baseline_id="current",
    )

    assert [item["dashboard_id"] for item in candidates] == ["match-light", "match-heavy"]
    assert candidates[0]["matched_filter_names"] == ["Opportunity Owner"]
    assert candidates[0]["report_overlap_count"] == 1
    assert "Much heavier" not in " ".join(candidates[0]["tradeoffs"])
    assert candidates[1]["matched_filter_names"] == ["Team / Opportunity Owner", "Fiscal Period"]
    assert candidates[1]["report_overlap_count"] == 0
    assert any("Much heavier" in item for item in candidates[1]["tradeoffs"])


def test_recommend_clone_baseline_strategy_keeps_current_lightweight_baseline() -> None:
    strategy = dashboard_executor._recommend_clone_baseline_strategy(
        resolved_clone_baseline={
            "dashboard_id": "01ZTb00000CyYpVMAV",
            "dashboard_label": "Operational Work Done",
        },
        candidate_clone_baselines=[
            {
                "dashboard_id": "01ZTb00000DNFBlMAP",
                "dashboard_label": "NA Sales Mgmt - Pipeline review 2026",
                "component_count": 16,
                "matched_filter_names": ["Team / Opportunity Owner", "Fiscal Period"],
                "report_overlap_count": 0,
            }
        ],
        target_component_count=3,
    )

    assert strategy["code"] == "keep_current_baseline_manual_filters"
    assert strategy["recommended_baseline"]["dashboard_label"] == "Operational Work Done"
    assert strategy["recommended_candidate"]["dashboard_id"] == "01ZTb00000DNFBlMAP"
    assert "best structural fit" in strategy["summary"]


def test_validate_salesforce_dashboard_package(tmp_path: Path) -> None:
    package_path = build_dashboard_package(tmp_path)
    result = run_executor("validate", "--package", str(package_path), "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["surface_type"] == "salesforce_dashboard"
    assert payload["summary"]["page_count"] == 1
    assert payload["summary"]["section_count"] == 2
    assert payload["summary"]["widget_count"] == 3


def test_bundle_salesforce_dashboard_package(tmp_path: Path) -> None:
    package_path = build_dashboard_package(tmp_path)
    output_dir = tmp_path / "dashboard_bundle"
    result = run_executor(
        "bundle",
        "--package",
        str(package_path),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert (output_dir / "salesforce_dashboard_bundle.json").exists()
    assert (output_dir / "salesforce_dashboard_definition.json").exists()
    assert (output_dir / "salesforce_dashboard_component_plan.json").exists()
    assert Path(payload["review_artifact"]).exists()
    assert Path(payload["collection_index_artifact"]).exists()
    assert Path(payload["collection_landing_artifact"]).exists()
    assert Path(payload["browser_index_artifact"]).exists()
    assert Path(payload["browser_landing_artifact"]).exists()
    assert Path(payload["browser_health_index_artifact"]).exists()
    assert Path(payload["browser_health_landing_artifact"]).exists()
    assert isinstance(payload["browser_health_summary"], dict)
    assert isinstance(payload["browser_health_summary"].get("run_recency_counts"), dict)
    health_index_path = Path(payload["browser_health_index_artifact"])
    health_landing_path = Path(payload["browser_health_landing_artifact"])
    assert health_index_path.exists()
    assert health_landing_path.exists()
    review_text = Path(payload["review_artifact"]).read_text(encoding="utf-8")
    collection_overview = Path(payload["collection_landing_artifact"]).read_text(encoding="utf-8")
    browser_overview = Path(payload["browser_landing_artifact"]).read_text(encoding="utf-8")
    browser_health = json.loads(health_index_path.read_text(encoding="utf-8"))
    assert "# Salesforce Dashboard Run" in review_text
    assert "# Salesforce Dashboard Runs" in collection_overview
    assert "# AI OS Collections" in browser_overview
    assert "## Health Snapshot" in browser_overview
    assert str(health_landing_path) in browser_overview
    assert json.loads(Path(payload["browser_index_artifact"]).read_text(encoding="utf-8"))["health_summary"]["collection_count"] >= 1
    assert browser_health["collection_count"] >= 1
    assert payload["browser_health_summary"]["collection_count"] == browser_health["collection_count"]
    assert any(item["code"] == "salesforce_dashboard_review_ready" for item in payload["messages"])
    assert any(item["code"] == "salesforce_dashboard_collection_index_ready" for item in payload["messages"])
    assert any(item["code"] == "ai_os_browser_ready" for item in payload["messages"])
    assert any(item["code"] == "ai_os_health_ready" for item in payload["messages"])
    bundle = payload["authoring_bundle"]
    assert bundle["artifact_type"] == "salesforce_dashboard_authoring_bundle"
    assert bundle["dashboard_definition"]["page_model"] == ["Summary"]
    assert bundle["handoff_target"]["surface_type"] == "salesforce_report"
    assert bundle["handoff_target"]["destination_type"] == "report"
    assert len(bundle["component_plan"]["components"]) == 2
    assert len(bundle["report_dependencies"]["dependencies"]) == 3


def test_preview_salesforce_dashboard_package(tmp_path: Path) -> None:
    package_path = build_dashboard_package(tmp_path)
    baseline_dashboard = tmp_path / "baseline_dashboard.json"
    baseline_dashboard.write_text(
        json.dumps(
            {
                "dashboardMetadata": {
                    "folderId": "00lTEST0000002AAA",
                    "components": [
                        {
                            "header": "Forecast & Closed Won",
                            "reportId": "00OTEST0000001AAA",
                            "properties": {
                                "visualizationType": "Metric",
                                "aggregates": ["RowCount"],
                            },
                        },
                        {
                            "header": "Pipeline Coverage by Stage",
                            "reportId": "00OTEST0000002AAA",
                            "properties": {
                                "visualizationType": "Line",
                                "groupings": ["CLOSE_DATE"],
                            },
                        },
                    ],
                    "filters": [
                        {
                            "name": "manager",
                            "options": [{"id": "f1", "value": "Andre"}],
                            "selectedOption": "f1",
                        }
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "dashboard_preview"
    result = run_executor(
        "preview",
        "--package",
        str(package_path),
        "--clone-from-dashboard-id",
        "01ZTb00000DoGYLMA3",
        "--baseline-dashboard-json",
        str(baseline_dashboard),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["summary"]["strategy"] == "clone_then_patch"
    assert payload["summary"]["request_count"] == 2
    assert payload["summary"]["fill_requirement_count"] > 0
    assert payload["summary"]["manual_filter_intent_count"] == 2
    assert payload["summary"]["autofill_count"] >= 8
    assert (output_dir / "salesforce_dashboard_rest_preview.json").exists()
    assert (output_dir / "salesforce_dashboard_fill_requirements.json").exists()
    assert (output_dir / "salesforce_dashboard_autofill_summary.json").exists()
    assert (output_dir / "salesforce_dashboard_manual_filter_authoring.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_playbook.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_playbook.md").exists()
    assert (output_dir / "salesforce_dashboard_filter_automation_plan.json").exists()
    preview = payload["rest_preview"]
    manual_filter_authoring = payload["manual_filter_authoring"]
    manual_filter_playbook = payload["manual_filter_playbook"]
    manual_filter_automation_plan = payload["manual_filter_automation_plan"]
    assert preview["artifact_type"] == "salesforce_dashboard_rest_preview"
    assert preview["requests"][0]["method"] == "POST"
    assert "cloneId=01ZTb00000DoGYLMA3" in preview["requests"][0]["path"]
    assert preview["requests"][1]["method"] == "PATCH"
    assert preview["requests"][1]["body"]["folderId"] == "00lTEST0000002AAA"
    assert preview["requests"][1]["body"]["layout"]["gridLayout"] is True
    assert preview["requests"][1]["body"]["layout"]["numColumns"] == 12
    assert len(preview["requests"][1]["body"]["layout"]["components"]) == 3
    assert preview["requests"][1]["body"]["components"][0]["reportId"] == "00OTEST0000001AAA"
    assert preview["requests"][1]["body"]["components"][0]["properties"] == {
        "visualizationType": "Metric",
        "aggregates": ["RowCount"],
    }
    assert preview["requests"][1]["body"]["components"][1]["reportId"] == "00OTEST0000002AAA"
    assert preview["requests"][1]["body"]["components"][1]["properties"] == {
        "visualizationType": "Line",
        "groupings": ["CLOSE_DATE"],
    }
    assert preview["requests"][1]["body"]["components"][2]["reportId"] == "00OTb000008TaBZMA0"
    assert preview["requests"][1]["body"]["components"][2]["properties"]["visualizationType"] == "FlexTable"
    assert preview["requests"][1]["body"]["components"][2]["properties"]["reportFormat"] == "TABULAR"
    assert preview["requests"][1]["body"]["components"][2]["properties"]["visualizationProperties"]["flexTableType"] == "summary"
    assert preview["requests"][1]["body"]["filters"] == [
        {
            "name": "manager",
            "options": [{"value": "Andre"}],
            "selectedOption": "f1",
        }
    ]
    assert not any(item["category"] == "component_properties" for item in payload["fill_requirements"])
    assert [item["source_label"] for item in preview["manual_filter_intents"]] == ["fiscal_period", "forecast_category"]
    assert preview["manual_filter_intents"][0]["compatibility_status"] == "analysis_skipped"
    assert preview["manual_filter_intents"][0]["proposed_filter"]["name"] == "Close Date"
    assert preview["manual_filter_intents"][1]["proposed_filter"]["name"] == "Forecast Category"
    assert manual_filter_authoring["artifact_type"] == "salesforce_dashboard_manual_filter_authoring"
    assert manual_filter_authoring["baseline_filter_count"] == 1
    assert [item["source_label"] for item in manual_filter_authoring["filter_intents"]] == ["fiscal_period", "forecast_category"]
    assert manual_filter_playbook["artifact_type"] == "salesforce_dashboard_filter_playbook"
    assert manual_filter_playbook["target_dashboard_id"] is None
    assert [item["display_name"] for item in manual_filter_playbook["filters"]] == ["Close Date", "Forecast Category"]
    assert manual_filter_playbook["filters"][0]["steps"][1]["action"] == "add_filter_value"
    assert manual_filter_automation_plan["artifact_type"] == "salesforce_dashboard_filter_automation_plan"
    assert manual_filter_automation_plan["relative_edit_route"] == "/lightning/r/Dashboard/__FILL_TARGET_DASHBOARD_ID__/edit"
    assert manual_filter_automation_plan["preflight_actions"][0]["action"] == "goto_edit_route"
    assert manual_filter_automation_plan["filter_actions"][0]["action"] == "author_dashboard_filter"
    assert manual_filter_automation_plan["post_actions"][-1]["action"] == "run_verify_cli"
    assert not any(item["category"] == "dashboard_filter_options" for item in payload["fill_requirements"])


def test_preview_salesforce_dashboard_package_uses_repo_baseline_and_property_vocab(tmp_path: Path) -> None:
    package_path = build_dashboard_package(tmp_path)
    baseline_dashboard = tmp_path / "baseline_dashboard.json"
    baseline_dashboard.write_text(
        json.dumps(
            {
                "dashboardMetadata": {
                    "folderId": "00lTEST0000002AAA",
                    "components": [],
                    "filters": [],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "dashboard_preview_repo_vocab"
    result = run_executor(
        "preview",
        "--package",
        str(package_path),
        "--baseline-dashboard-json",
        str(baseline_dashboard),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    preview = payload["rest_preview"]
    assert preview["requests"][0]["path"] == "/services/data/v66.0/analytics/dashboards?cloneId=01ZTb00000CyYpVMAV"
    assert preview["resolved_clone_baseline"] == {
        "dashboard_id": "01ZTb00000CyYpVMAV",
        "dashboard_label": "Operational Work Done",
        "source": "live_clone_probe",
        "confidence": "medium",
    }
    assert preview["requests"][1]["body"]["layout"]["gridLayout"] is True
    assert not any(item["category"] == "baseline_dashboard_id" for item in payload["fill_requirements"])
    components = preview["requests"][1]["body"]["components"]
    assert components[0]["reportId"] == "00OTb000008TZaTMAW"
    assert components[0]["properties"]["visualizationType"] == "Funnel"
    assert components[1]["reportId"] == "00OTb000008TZc5MAG"
    assert components[1]["properties"]["visualizationType"] == "Bar"
    assert components[2]["reportId"] == "00OTb000008TaBZMA0"
    assert components[2]["properties"]["visualizationType"] == "FlexTable"
    filters = preview["requests"][1]["body"]["filters"]
    assert filters == []
    assert payload["summary"]["autofill_count"] == 7
    assert payload["summary"]["manual_filter_intent_count"] == 3
    assert [item["source_label"] for item in preview["manual_filter_intents"]] == [
        "fiscal_period",
        "manager",
        "forecast_category",
    ]
    assert preview["manual_filter_intents"][0]["proposed_filter"]["name"] == "Close Date"
    assert preview["manual_filter_intents"][1]["proposed_filter"]["name"] == "Opportunity Owner"
    assert preview["manual_filter_intents"][2]["proposed_filter"]["name"] == "Forecast Category"
    assert payload["manual_filter_authoring"]["baseline_filter_count"] == 0


def test_verify_salesforce_dashboard_package(tmp_path: Path) -> None:
    package_path = build_dashboard_package(tmp_path)
    baseline_dashboard = tmp_path / "baseline_dashboard.json"
    baseline_dashboard.write_text(
        json.dumps(
            {
                "dashboardMetadata": {
                    "folderId": "00lTEST0000002AAA",
                    "components": [
                        {
                            "header": "Forecast & Closed Won",
                            "reportId": "00OTEST0000001AAA",
                            "properties": {"visualizationType": "Metric"},
                        },
                        {
                            "header": "Pipeline Coverage by Stage",
                            "reportId": "00OTEST0000002AAA",
                            "properties": {"visualizationType": "Line"},
                        }
                    ],
                    "filters": [
                        {
                            "name": "manager",
                            "options": [{"id": "f1", "value": "Andre"}],
                            "selectedOption": "f1",
                        }
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    actual_dashboard = tmp_path / "actual_dashboard.json"
    actual_dashboard.write_text(
        json.dumps(
            {
                "dashboardMetadata": {
                    "folderId": "00lTEST0000002AAA",
                    "components": [
                        {
                            "title": "Actual Forecast & Closed Won",
                            "reportId": "00OTEST0000001AAA",
                            "properties": {"visualizationType": "Metric"},
                        },
                        {
                            "title": "Variance Driver: Pipeline Coverage",
                            "reportId": "00OTEST0000002AAA",
                            "properties": {"visualizationType": "Line"},
                        },
                        {
                            "title": "Action Queue: Close-Date Risk",
                            "reportId": "00OTb000008TaBZMA0",
                            "properties": {"visualizationType": "FlexTable"},
                        },
                    ],
                    "filters": [
                        {
                            "name": "manager",
                            "options": [{"value": "Andre"}],
                            "selectedOption": "f1",
                        },
                        {
                            "name": "Close Date",
                            "options": [
                                {"alias": "Q1-2026", "operation": "between", "startValue": "01.02.2026", "endValue": "30.04.2026", "value": None},
                                {"alias": "Q2-2026", "operation": "between", "startValue": "01.05.2026", "endValue": "31.07.2026", "value": None},
                                {"alias": "Q3-2026", "operation": "between", "startValue": "01.08.2026", "endValue": "31.10.2026", "value": None},
                                {"alias": "Q4-2026", "operation": "between", "startValue": "01.11.2026", "endValue": "31.01.2027", "value": None},
                            ],
                            "selectedOption": None,
                        },
                        {
                            "name": "Forecast Category",
                            "options": [
                                {"alias": "Pipeline", "operation": "equals", "startValue": None, "endValue": None, "value": "Pipeline"},
                                {"alias": "Best Case", "operation": "equals", "startValue": None, "endValue": None, "value": "Best Case"},
                                {"alias": "Commit", "operation": "equals", "startValue": None, "endValue": None, "value": "Commit"},
                                {"alias": "Won", "operation": "equals", "startValue": None, "endValue": None, "value": "Won"},
                            ],
                            "selectedOption": None,
                        },
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "dashboard_verify"
    result = run_executor(
        "verify",
        "--package",
        str(package_path),
        "--clone-from-dashboard-id",
        "01ZTb00000DoGYLMA3",
        "--baseline-dashboard-json",
        str(baseline_dashboard),
        "--actual-dashboard-json",
        str(actual_dashboard),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "read_only"
    assert payload["summary"]["finding_count"] == 1
    assert payload["summary"]["warn_count"] == 0
    assert payload["summary"]["info_count"] == 1
    assert payload["summary"]["manual_filter_intent_count"] == 2
    assert payload["summary"]["manual_filter_verified_count"] == 2
    assert payload["summary"]["manual_filter_missing_count"] == 0
    assert payload["summary"]["manual_filter_mismatch_count"] == 0
    assert payload["findings"][0]["code"] == "manual_dashboard_filters_verified"
    assert payload["manual_filter_verification"]["source"] == "manual_filter_authoring_artifact"
    assert len(payload["manual_filter_verification"]["verified_filters"]) == 2
    assert (output_dir / "salesforce_dashboard_verify.json").exists()
    assert payload["expected_contract"]["components"][2]["reportId"] == "00OTb000008TaBZMA0"
    assert len(payload["expected_contract"]["filters"]) == 1
    assert len(payload["actual_contract"]["filters"]) == 3


def test_verify_salesforce_dashboard_package_with_manual_filter_artifact(tmp_path: Path) -> None:
    package_path = build_dashboard_package(tmp_path)
    baseline_dashboard = tmp_path / "baseline_dashboard.json"
    baseline_dashboard.write_text(
        json.dumps(
            {
                "dashboardMetadata": {
                    "folderId": "00lTEST0000002AAA",
                    "components": [
                        {
                            "header": "Forecast & Closed Won",
                            "reportId": "00OTEST0000001AAA",
                            "properties": {"visualizationType": "Metric"},
                        },
                        {
                            "header": "Pipeline Coverage by Stage",
                            "reportId": "00OTEST0000002AAA",
                            "properties": {"visualizationType": "Line"},
                        }
                    ],
                    "filters": [
                        {
                            "name": "manager",
                            "options": [{"id": "f1", "value": "Andre"}],
                            "selectedOption": "f1",
                        }
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    actual_dashboard = tmp_path / "actual_dashboard.json"
    actual_dashboard.write_text(
        json.dumps(
            {
                "dashboardMetadata": {
                    "folderId": "00lTEST0000002AAA",
                    "components": [
                        {
                            "title": "Actual Forecast & Closed Won",
                            "reportId": "00OTEST0000001AAA",
                            "properties": {"visualizationType": "Metric"},
                        },
                        {
                            "title": "Variance Driver: Pipeline Coverage",
                            "reportId": "00OTEST0000002AAA",
                            "properties": {"visualizationType": "Line"},
                        },
                        {
                            "title": "Action Queue: Close-Date Risk",
                            "reportId": "00OTb000008TaBZMA0",
                            "properties": {"visualizationType": "FlexTable"},
                        },
                    ],
                    "filters": [
                        {
                            "name": "manager",
                            "options": [{"value": "Andre"}],
                            "selectedOption": "f1",
                        },
                        {
                            "name": "Forecast Category",
                            "options": [
                                {"alias": "Pipeline", "operation": "equals", "startValue": None, "endValue": None, "value": "Pipeline"},
                                {"alias": "Best Case", "operation": "equals", "startValue": None, "endValue": None, "value": "Best Case"},
                                {"alias": "Commit", "operation": "equals", "startValue": None, "endValue": None, "value": "Commit"},
                                {"alias": "Won", "operation": "equals", "startValue": None, "endValue": None, "value": "Won"},
                            ],
                            "selectedOption": None,
                        },
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    manual_filter_authoring = tmp_path / "salesforce_dashboard_manual_filter_authoring.json"
    manual_filter_authoring.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_manual_filter_authoring",
                "filter_intents": [
                    {
                        "source_label": "fiscal_period",
                        "proposal_source": "repo_vocab.dashboard_filter_template:fiscal_period->Close Date",
                        "proposed_filter": {
                            "name": "Close Date",
                            "options": [
                                {"alias": "Q1-2026", "operation": "between", "startValue": "01.02.2026", "endValue": "30.04.2026", "value": None}
                            ],
                            "selectedOption": None,
                        },
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    result = run_executor(
        "verify",
        "--package",
        str(package_path),
        "--clone-from-dashboard-id",
        "01ZTb00000DoGYLMA3",
        "--baseline-dashboard-json",
        str(baseline_dashboard),
        "--actual-dashboard-json",
        str(actual_dashboard),
        "--manual-filter-authoring-json",
        str(manual_filter_authoring),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["summary"]["manual_filter_missing_count"] == 1
    assert payload["summary"]["manual_filter_verified_count"] == 0
    assert payload["manual_filter_verification"]["source"] == "manual_filter_authoring_artifact"
    assert "missing_manual_dashboard_filter" in [item["code"] for item in payload["findings"]]


def test_apply_salesforce_dashboard_package(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path)
    package_path = build_dashboard_package(tmp_path, evaluation_path=evaluation_path)
    baseline_dashboard = tmp_path / "baseline_dashboard.json"
    baseline_dashboard.write_text(
        json.dumps(
            {
                "dashboardMetadata": {
                    "folderId": "00lTEST0000002AAA",
                    "components": [
                        {
                            "header": "Forecast & Closed Won",
                            "reportId": "00OTEST0000001AAA",
                            "properties": {
                                "visualizationType": "Metric",
                                "aggregates": ["RowCount"],
                            },
                        },
                        {
                            "header": "Pipeline Coverage by Stage",
                            "reportId": "00OTEST0000002AAA",
                            "properties": {
                                "visualizationType": "Line",
                                "groupings": ["CLOSE_DATE"],
                            },
                        },
                    ],
                    "filters": [
                        {
                            "name": "manager",
                            "options": [{"id": "f1", "value": "Andre"}],
                            "selectedOption": "f1",
                        }
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, sys",
                "args = sys.argv[1:]",
                "path = next((item for item in args if item.startswith('/services/data/')), None)",
                "method = 'GET'",
                "if '--method' in args:",
                "    method = args[args.index('--method') + 1]",
                "if path and path.endswith('/analytics/dashboards') and method == 'GET':",
                "    print(json.dumps([]))",
                "    raise SystemExit(0)",
                "if path and 'analytics/dashboards?cloneId=' in path and method == 'POST':",
                "    print(json.dumps({'id': '01ZTESTCLONEDAAA', 'name': 'Cloned Dashboard'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZTESTCLONEDAAA') and method == 'PATCH':",
                "    print(json.dumps({'id': '01ZTESTCLONEDAAA', 'name': 'Applied Dashboard'}))",
                "    raise SystemExit(0)",
                "print(json.dumps({'args': args}))",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["CRM_AI_MEMORY_ROOT"] = str(tmp_path / "agent_memory")
    output_dir = tmp_path / "dashboard_apply"
    result = run_executor(
        "apply",
        "--package",
        str(package_path),
        "--clone-from-dashboard-id",
        "01ZTb00000DoGYLMA3",
        "--baseline-dashboard-json",
        str(baseline_dashboard),
        "--target-org",
        "apro@simcorp.com",
        "--output-dir",
        str(output_dir),
        "--apply",
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "mutating"
    assert payload["evaluation_gate"]["verdict"] == "pass"
    assert payload["apply_summary"]["strategy"] == "clone_then_patch"
    assert payload["apply_summary"]["external_fill_requirement_count"] == 0
    assert payload["apply_summary"]["internal_fill_requirement_count"] == 1
    assert payload["request_preview"]["fill_requirements"][0]["category"] == "cloned_dashboard_id"
    assert payload["execution_requests"][0]["method"] == "POST"
    assert payload["execution_requests"][1]["path"].endswith("/analytics/dashboards/01ZTESTCLONEDAAA")
    assert payload["manual_filter_authoring"]["target_dashboard_id"] == "01ZTESTCLONEDAAA"
    assert len(payload["manual_filter_authoring"]["filter_intents"]) == 2
    assert payload["manual_filter_playbook"]["artifact_type"] == "salesforce_dashboard_filter_playbook"
    assert payload["manual_filter_playbook"]["target_dashboard_id"] == "01ZTESTCLONEDAAA"
    assert payload["manual_filter_playbook"]["filters"][0]["steps"][0]["action"] == "open_add_filter"
    assert payload["manual_filter_automation_plan"]["artifact_type"] == "salesforce_dashboard_filter_automation_plan"
    assert payload["manual_filter_automation_plan"]["target_dashboard_id"] == "01ZTESTCLONEDAAA"
    assert payload["manual_filter_automation_plan"]["relative_edit_route"] == "/lightning/r/Dashboard/01ZTESTCLONEDAAA/edit"
    assert payload["manual_filter_automation_plan"]["post_actions"][-1]["command_template"][6] == "01ZTESTCLONEDAAA"
    assert payload["applied_dashboard"] == {
        "id": "01ZTESTCLONEDAAA",
        "name": "Applied Dashboard",
    }
    assert payload["memory_record"]["run_id"] == "run_20260329_001"
    assert (output_dir / "salesforce_dashboard_apply_preview.json").exists()
    assert (output_dir / "salesforce_dashboard_apply_response.json").exists()
    assert (output_dir / "salesforce_dashboard_manual_filter_authoring.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_playbook.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_playbook.md").exists()
    assert (output_dir / "salesforce_dashboard_filter_automation_plan.json").exists()
    assert Path(payload["review_artifact"]).exists()
    assert Path(payload["collection_index_artifact"]).exists()
    assert Path(payload["collection_landing_artifact"]).exists()
    assert Path(payload["browser_index_artifact"]).exists()
    assert Path(payload["browser_landing_artifact"]).exists()
    review_text = Path(payload["review_artifact"]).read_text(encoding="utf-8")
    collection_overview = Path(payload["collection_landing_artifact"]).read_text(encoding="utf-8")
    browser_overview = Path(payload["browser_landing_artifact"]).read_text(encoding="utf-8")
    assert "# Salesforce Dashboard Run" in review_text
    assert "# Salesforce Dashboard Runs" in collection_overview
    assert "# AI OS Collections" in browser_overview
    assert any(item["code"] == "salesforce_dashboard_review_ready" for item in payload["messages"])
    assert any(item["code"] == "salesforce_dashboard_collection_index_ready" for item in payload["messages"])
    assert any(item["code"] == "ai_os_browser_ready" for item in payload["messages"])
    memory_record = json.loads((tmp_path / "agent_memory" / "runs" / "run_20260329_001.json").read_text(encoding="utf-8"))
    assert memory_record["outcome"] == "salesforce_dashboard_executor_apply_ok"
    assert "scripts/salesforce_dashboard_executor.py" in memory_record["sequence"]


def test_complete_salesforce_dashboard_package_runs_filter_flow(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path)
    package_path = build_dashboard_package(tmp_path, evaluation_path=evaluation_path)
    baseline_dashboard = tmp_path / "baseline_dashboard.json"
    baseline_dashboard.write_text(
        json.dumps(
            {
                "dashboardMetadata": {
                    "folderId": "00lTEST0000002AAA",
                    "components": [
                        {
                            "header": "Forecast & Closed Won",
                            "reportId": "00OTEST0000001AAA",
                            "properties": {
                                "visualizationType": "Metric",
                                "aggregates": ["RowCount"],
                            },
                        },
                        {
                            "header": "Pipeline Coverage by Stage",
                            "reportId": "00OTEST0000002AAA",
                            "properties": {
                                "visualizationType": "Line",
                                "groupings": ["CLOSE_DATE"],
                            },
                        },
                    ],
                    "filters": [
                        {
                            "name": "manager",
                            "options": [{"id": "f1", "value": "Andre"}],
                            "selectedOption": "f1",
                        }
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, sys",
                "args = sys.argv[1:]",
                "path = next((item for item in args if item.startswith('/services/data/')), None)",
                "method = 'GET'",
                "if '--method' in args:",
                "    method = args[args.index('--method') + 1]",
                "if path and path.endswith('/analytics/dashboards') and method == 'GET':",
                "    print(json.dumps([]))",
                "    raise SystemExit(0)",
                "if path and 'analytics/dashboards?cloneId=' in path and method == 'POST':",
                "    print(json.dumps({'id': '01ZTESTCLONEDAAA', 'name': 'Cloned Dashboard'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZTESTCLONEDAAA') and method == 'PATCH':",
                "    print(json.dumps({'id': '01ZTESTCLONEDAAA', 'name': 'Applied Dashboard'}))",
                "    raise SystemExit(0)",
                "print(json.dumps({'args': args}))",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)
    fake_filter_automation = build_fake_dashboard_filter_automation_script(tmp_path)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["CRM_AI_MEMORY_ROOT"] = str(tmp_path / "agent_memory")
    output_dir = tmp_path / "dashboard_complete"
    result = run_executor(
        "complete",
        "--package",
        str(package_path),
        "--baseline-dashboard-json",
        str(baseline_dashboard),
        "--target-org",
        "apro@simcorp.com",
        "--autofill-live",
        "--session",
        "dashboard_complete_test_session",
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
    assert payload["summary"]["apply_status"] == "ok"
    assert payload["summary"]["filter_flow_status"] == "ok"
    assert payload["summary"]["applied_dashboard_id"] == "01ZTESTCLONEDAAA"
    assert payload["summary"]["authored_filter_count"] == 3
    assert payload["summary"]["manual_filter_verified_count"] == 3
    assert payload["applied_dashboard"] == {
        "id": "01ZTESTCLONEDAAA",
        "name": "Applied Dashboard",
    }
    assert payload["apply_result"]["evaluation_gate"]["verdict"] == "pass"
    assert payload["apply_result"]["applied_dashboard"]["id"] == "01ZTESTCLONEDAAA"
    assert payload["filter_flow_result"]["summary"]["through_stage"] == "verify-dashboard"
    assert payload["filter_flow_result"]["summary"]["target_dashboard_id"] == "01ZTESTCLONEDAAA"
    assert payload["memory_record"]["run_id"] == "run_20260329_001"
    assert (output_dir / "01_apply" / "salesforce_dashboard_filter_automation_plan.json").exists()
    assert (output_dir / "02_filter_flow" / "run_filter_flow_invocation.json").exists()
    assert (output_dir / "02_filter_flow" / "09_verify_dashboard" / "salesforce_dashboard_verify.json").exists()
    assert Path(payload["review_artifact"]).exists()
    assert Path(payload["collection_index_artifact"]).exists()
    assert Path(payload["collection_landing_artifact"]).exists()
    assert Path(payload["browser_index_artifact"]).exists()
    assert Path(payload["browser_landing_artifact"]).exists()

    invocation = json.loads((output_dir / "02_filter_flow" / "run_filter_flow_invocation.json").read_text(encoding="utf-8"))
    assert invocation["dashboard_id"] == "01ZTESTCLONEDAAA"
    assert invocation["all_filters"] is True
    assert invocation["through"] == "verify-dashboard"
    assert invocation["verify_package"] == str(package_path)
    assert invocation["session"] == "dashboard_complete_test_session"
    memory_record = json.loads((tmp_path / "agent_memory" / "runs" / "run_20260329_001.json").read_text(encoding="utf-8"))
    assert memory_record["outcome"] == "salesforce_dashboard_executor_complete_ok"


def test_apply_blocks_without_pass_evaluation(tmp_path: Path) -> None:
    package_path = build_dashboard_package(tmp_path)
    baseline_dashboard = tmp_path / "baseline_dashboard.json"
    baseline_dashboard.write_text(
        json.dumps({"dashboardMetadata": {"folderId": "00lTEST0000002AAA", "components": [], "filters": []}}, indent=2),
        encoding="utf-8",
    )
    result = run_executor(
        "apply",
        "--package",
        str(package_path),
        "--clone-from-dashboard-id",
        "01ZTb00000DoGYLMA3",
        "--baseline-dashboard-json",
        str(baseline_dashboard),
        "--target-org",
        "apro@simcorp.com",
        "--apply",
        "--json",
    )
    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert any(item["code"] == "evaluation_required" for item in payload["messages"])


def test_apply_writes_bypass_audit_artifact(tmp_path: Path) -> None:
    package_path = build_dashboard_package(tmp_path)
    baseline_dashboard = tmp_path / "baseline_dashboard.json"
    baseline_dashboard.write_text(
        json.dumps({"dashboardMetadata": {"folderId": "00lTEST0000002AAA", "components": [], "filters": []}}, indent=2),
        encoding="utf-8",
    )
    output_dir = tmp_path / "dashboard_apply"
    env = os.environ.copy()
    env["CRM_AI_MEMORY_ROOT"] = str(tmp_path / "agent_memory")
    result = run_executor(
        "apply",
        "--package",
        str(package_path),
        "--clone-from-dashboard-id",
        "01ZTb00000DoGYLMA3",
        "--baseline-dashboard-json",
        str(baseline_dashboard),
        "--apply",
        "--allow-missing-evaluation",
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )
    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert any(item["code"] == "evaluation_bypass_used" for item in payload["messages"])
    assert payload["policy_exceptions"] == ["evaluation_bypass"]
    assert payload["memory_record"]["run_id"] == "dashboard_apply"
    bypass_path = output_dir / "evaluation_bypass_audit.json"
    assert bypass_path.exists()
    bypass_payload = json.loads(bypass_path.read_text(encoding="utf-8"))
    assert bypass_payload["policy_exceptions"] == ["evaluation_bypass"]
    assert bypass_payload["evaluation_gate"]["bypassed"] is True
    memory_record = json.loads((tmp_path / "agent_memory" / "runs" / "dashboard_apply.json").read_text(encoding="utf-8"))
    assert memory_record["goal"] == "Build a native Salesforce dashboard for manager forecast inspection."
    assert memory_record["policy_exceptions"] == ["evaluation_bypass"]
    assert memory_record["outcome"] == "salesforce_dashboard_executor_apply_error"


def test_complete_blocks_non_pass_evaluation(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path, verdict="needs_more_evidence")
    package_path = build_dashboard_package(tmp_path, evaluation_path=evaluation_path)
    baseline_dashboard = tmp_path / "baseline_dashboard.json"
    baseline_dashboard.write_text(
        json.dumps({"dashboardMetadata": {"folderId": "00lTEST0000002AAA", "components": [], "filters": []}}, indent=2),
        encoding="utf-8",
    )
    result = run_executor(
        "complete",
        "--package",
        str(package_path),
        "--baseline-dashboard-json",
        str(baseline_dashboard),
        "--target-org",
        "apro@simcorp.com",
        "--session",
        "dashboard_complete_test_session",
        "--json",
    )
    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert any(item["code"] == "evaluation_not_pass" for item in payload["messages"])


def test_delete_salesforce_dashboard(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_state = tmp_path / "delete_state.json"
    fake_state.write_text(json.dumps({"delete_calls": 0, "dashboard_calls": 0}), encoding="utf-8")
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
                "state_path = Path(os.environ['FAKE_DASHBOARD_DELETE_STATE'])",
                "state = json.loads(state_path.read_text())",
                "if path and path.endswith('/analytics/dashboards/01ZTESTDELETEAAA') and method == 'DELETE':",
                "    state['delete_calls'] += 1",
                "    state_path.write_text(json.dumps(state))",
                "    print('{}')",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/dashboards/01ZTESTDELETEAAA') and method == 'GET':",
                "    state['dashboard_calls'] += 1",
                "    state_path.write_text(json.dumps(state))",
                "    if state['dashboard_calls'] == 1:",
                "        print(json.dumps({'id': '01ZTESTDELETEAAA', 'name': 'Temporary Dashboard'}))",
                "        raise SystemExit(0)",
                "    print(json.dumps([{'errorCode': 'ENTITY_IS_DELETED', 'message': 'entity is deleted'}]))",
                "    raise SystemExit(1)",
                "print(json.dumps({'args': args}))",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_DASHBOARD_DELETE_STATE"] = str(fake_state)

    output_dir = tmp_path / "dashboard_delete"
    result = run_executor(
        "delete",
        "--dashboard-id",
        "01ZTESTDELETEAAA",
        "--target-org",
        "apro@simcorp.com",
        "--verify-attempts",
        "3",
        "--verify-delay-seconds",
        "0",
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "mutating"
    assert payload["summary"]["deleted_dashboard_id"] == "01ZTESTDELETEAAA"
    assert payload["summary"]["delete_verified"] is True
    assert payload["summary"]["delete_verify_attempt_count"] == 2
    assert payload["delete_verification"]["deleted"] is True
    assert payload["delete_verification"]["attempt_results"][0]["status"] == "still_exists"
    assert payload["delete_verification"]["attempt_results"][1]["status"] == "deleted"
    assert (output_dir / "salesforce_dashboard_delete_response.json").exists()
    assert (output_dir / "salesforce_dashboard_delete_verify.json").exists()
