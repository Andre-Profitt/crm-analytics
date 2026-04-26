from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from scripts import salesforce_report_executor as report_executor


ROOT = Path(__file__).resolve().parents[1]
BUILDER_BRAIN = ROOT / "scripts" / "builder_brain.py"
EXECUTOR = ROOT / "scripts" / "salesforce_report_executor.py"


def run_builder_brain(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(BUILDER_BRAIN), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def run_executor(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    effective_env = os.environ.copy()
    if env:
        effective_env.update(env)
    effective_env.setdefault("CRM_AI_MEMORY_ROOT", tempfile.mkdtemp(prefix="crm_ai_report_memory_"))
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


def build_report_package(tmp_path: Path, *, evaluation_path: Path | None = None) -> Path:
    command = [
        "handoff",
        "--query",
        "Manager owner list report for renewals needing follow-up this week",
        "--output-dir",
        str(tmp_path),
    ]
    if evaluation_path is not None:
        command.extend(["--evaluation", str(evaluation_path)])
    command.append("--json")
    result = run_builder_brain(*command)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    return Path(payload["executor_handoff"]["package_artifact"])


def write_matrix_report_package(tmp_path: Path) -> Path:
    package_path = tmp_path / "matrix_report_package.json"
    package_path.write_text(
        json.dumps(
            {
                "build_brief": {
                    "persona": "manager",
                    "domain": "renewals",
                    "decision_statement": "Create a diagnostic matrix report.",
                },
                "surface_contract": {
                    "surface_type": "salesforce_report",
                    "report_format": "matrix",
                    "columns": ["Owner", "Account", "Opportunity", "Close Date", "Amount", "Forecast Category"],
                    "filters": ["renewal_period", "owner", "risk_band"],
                    "group_by": ["Manager", "Owner"],
                    "sort_by": ["Amount", "Owner"],
                    "page_blueprint": [],
                },
                "review_gates": ["Validate the package."],
                "acceptance_criteria": ["The report saves."],
                "execution_plan": {"phases": [{"actions": ["Author the report."]}]},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return package_path


def write_report_baseline_describe(tmp_path: Path) -> Path:
    baseline_describe = tmp_path / "baseline_report_describe.json"
    baseline_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {
                    "folderId": "00lTEST0000001AAA",
                    "reportType": {"type": "Opportunity"},
                },
                "reportExtendedMetadata": {
                    "detailColumnInfo": {
                        "FULL_NAME": {"label": "Owner"},
                        "ACCOUNT_NAME": {"label": "Account"},
                        "OPPORTUNITY_NAME": {"label": "Opportunity"},
                        "CLOSE_DATE": {"label": "Close Date"},
                        "AMOUNT": {"label": "Amount"},
                        "FORECAST_CATEGORY": {"label": "Forecast Category"},
                    },
                    "groupingColumnInfo": {
                        "OWNER_MANAGER": {"label": "Manager"},
                        "FULL_NAME": {"label": "Owner"},
                    },
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return baseline_describe


def write_manager_report_baseline_describe(tmp_path: Path) -> Path:
    baseline_describe = tmp_path / "manager_report_baseline_describe.json"
    baseline_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {
                    "folderId": "00lTEST0000001AAA",
                    "reportType": {"type": "Opportunity"},
                },
                "reportExtendedMetadata": {
                    "detailColumnInfo": {
                        "FULL_NAME": {"label": "Opportunity Owner"},
                        "ACCOUNT_NAME": {"label": "Account Name"},
                        "Account.Gain_Annual_Renewal_Date__c": {"label": "DM Software Annual Renewal Date"},
                        "Opportunity.Risk_Assessment_Level__c": {"label": "Risk Assessment Level"},
                        "Opportunity.APTS_RH_Product_Family__c": {"label": "Product Family"},
                    },
                    "groupingColumnInfo": {
                        "OWNER_MANAGER": {"label": "Opportunity Owner: Manager"},
                    },
                },
                "reportTypeMetadata": {
                    "categories": [
                        {
                            "label": "Opportunity Owner Information",
                            "columns": {
                                "FULL_NAME": {"label": "Opportunity Owner"},
                                "OWNER_MANAGER": {"label": "Opportunity Owner: Manager"},
                            },
                        },
                        {
                            "label": "Opportunity: Custom Info",
                            "columns": {
                                "Opportunity.APTS_RH_Product_Family__c": {"label": "Product Family"},
                                "Opportunity.Risk_Assessment_Level__c": {"label": "Risk Assessment Level"},
                            },
                        },
                        {
                            "label": "Account: Custom Info",
                            "columns": {
                                "Account.Gain_Renewal_Period__c": {"label": "DM Software Renewal Period"},
                            },
                        },
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return baseline_describe


def write_filter_overrides_json(tmp_path: Path) -> Path:
    override_path = tmp_path / "report_filter_overrides.json"
    override_path.write_text(
        json.dumps(
            {
                "renewal_period": {"value": "This Week"},
                "owner": "Taylor Smith",
                "product_family": "Axioma",
                "risk_band": {"operator": "equals", "value": "High"},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return override_path


def test_run_rest_request_uses_direct_org_session(monkeypatch) -> None:
    monkeypatch.setattr(
        report_executor,
        "_get_org_session",
        lambda target_org: {"access_token": "token", "instance_url": "https://example.my.salesforce.com"},
    )

    captured: dict[str, object] = {}

    def fake_direct(path: str, *, org_session: dict[str, str], method: str = "GET", body: object | None = None) -> object:
        captured["path"] = path
        captured["org_session"] = org_session
        captured["method"] = method
        captured["body"] = body
        return {"id": "00OEXAMPLE", "name": "Example Report"}

    monkeypatch.setattr(report_executor, "_run_direct_rest_request", fake_direct)

    payload = report_executor._run_rest_request(
        "/services/data/v66.0/analytics/reports/00OEXAMPLE",
        target_org="apro@simcorp.com",
        method="PATCH",
        body={"name": "Example Report"},
    )

    assert payload == {"id": "00OEXAMPLE", "name": "Example Report"}
    assert captured == {
        "path": "/services/data/v66.0/analytics/reports/00OEXAMPLE",
        "org_session": {"access_token": "token", "instance_url": "https://example.my.salesforce.com"},
        "method": "PATCH",
        "body": {"name": "Example Report"},
    }


def test_run_rest_request_falls_back_to_cli(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(report_executor, "_get_org_session", lambda target_org: None)
    report_executor._ORG_SESSION_CACHE.clear()

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json",
                "print(json.dumps({'id': '00OCLI', 'name': 'CLI Report'}))",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)

    original_path = os.environ.get("PATH", "")
    monkeypatch.setenv("PATH", f"{fake_bin}:{original_path}")

    payload = report_executor._run_rest_request(
        "/services/data/v66.0/analytics/reports/00OCLI",
        target_org="apro@simcorp.com",
        method="GET",
    )

    assert payload == {"id": "00OCLI", "name": "CLI Report"}


def test_validate_salesforce_report_package(tmp_path: Path) -> None:
    package_path = build_report_package(tmp_path)
    result = run_executor("validate", "--package", str(package_path), "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["surface_type"] == "salesforce_report"
    assert payload["summary"]["column_count"] >= 6
    assert payload["summary"]["has_handoff_target"] is True
    assessment = payload["summary"]["action_surface_assessment"]
    assert assessment["verdict"] in {"moderate_follow_up_fit", "strong_follow_up_fit"}
    assert assessment["owner_visibility"] is True
    assert assessment["date_coverage"] is True
    assert assessment["explicit_sort"] is True


def test_validate_salesforce_report_package_warns_on_weak_action_surface(tmp_path: Path) -> None:
    package_path = tmp_path / "weak_report_package.json"
    package_path.write_text(
        json.dumps(
            {
                "build_brief": {
                    "persona": "manager",
                    "domain": "renewals",
                    "decision_statement": "Create a diagnostic matrix report.",
                },
                "surface_contract": {
                    "surface_type": "salesforce_report",
                    "report_format": "matrix",
                    "columns": ["Region", "Segment"],
                    "filters": ["segment"],
                    "group_by": [],
                    "sort_by": [],
                    "page_blueprint": [],
                },
                "review_gates": ["Validate the package."],
                "acceptance_criteria": ["The report saves."],
                "execution_plan": {"phases": [{"actions": ["Author the report."]}]},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    result = run_executor("validate", "--package", str(package_path), "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assessment = payload["summary"]["action_surface_assessment"]
    assert payload["summary"]["normalized_report_format"] == "MATRIX"
    assert payload["summary"]["native_authoring_support"] == "manual_required"
    assert assessment["verdict"] == "weak_follow_up_fit"
    assert assessment["owner_visibility"] is False
    assert assessment["date_coverage"] is False
    assert assessment["explicit_sort"] is False
    warning_codes = {item["code"] for item in payload["messages"]}
    assert "build_package_warning" in warning_codes
    warning_texts = [item["text"] for item in payload["messages"] if item["code"] == "build_package_warning"]
    assert any("groupingsAcross" in item for item in warning_texts)


def test_validate_salesforce_report_package_rejects_unknown_report_format(tmp_path: Path) -> None:
    package_path = tmp_path / "invalid_report_format_package.json"
    package_path.write_text(
        json.dumps(
            {
                "build_brief": {
                    "persona": "manager",
                    "domain": "renewals",
                    "decision_statement": "Create a report in an unsupported format.",
                },
                "surface_contract": {
                    "surface_type": "salesforce_report",
                    "report_format": "joined",
                    "columns": ["Owner", "Account", "Close Date", "Amount"],
                    "filters": ["renewal_period"],
                    "group_by": [],
                    "sort_by": ["Amount"],
                    "page_blueprint": [],
                },
                "review_gates": ["Validate the package."],
                "acceptance_criteria": ["The report saves."],
                "execution_plan": {"phases": [{"actions": ["Author the report."]}]},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    result = run_executor("validate", "--package", str(package_path), "--json")
    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["summary"]["native_authoring_support"] == "unsupported"
    assert any(
        item["code"] == "invalid_build_package"
        and "surface_contract.report_format must be one of" in item["text"]
        for item in payload["messages"]
    )


def test_validate_salesforce_report_package_caps_summary_follow_up_fit(tmp_path: Path) -> None:
    package_path = tmp_path / "summary_report_package.json"
    package_path.write_text(
        json.dumps(
            {
                "build_brief": {
                    "persona": "manager",
                    "domain": "renewals",
                    "decision_statement": "Create a compact summary follow-up report.",
                },
                "surface_contract": {
                    "surface_type": "salesforce_report",
                    "report_format": "summary",
                    "columns": [
                        "Owner",
                        "Account",
                        "Opportunity",
                        "Close Date",
                        "Amount",
                        "Forecast Category",
                    ],
                    "filters": ["renewal_period", "owner", "risk_band"],
                    "group_by": ["Manager", "Owner"],
                    "sort_by": ["Amount", "Owner"],
                    "handoff_surface": "crma_dashboard",
                    "handoff_target": {"destination_type": "dashboard"},
                    "page_blueprint": [],
                },
                "review_gates": ["Validate the package."],
                "acceptance_criteria": ["The report saves."],
                "execution_plan": {"phases": [{"actions": ["Author the report."]}]},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    result = run_executor("validate", "--package", str(package_path), "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assessment = payload["summary"]["action_surface_assessment"]
    assert assessment["raw_verdict"] == "strong_follow_up_fit"
    assert assessment["verdict"] == "moderate_follow_up_fit"
    assert assessment["verdict_cap"] == "summary_caps_follow_up_fit"
    assert assessment["primary_surface_fit"] == "limited_primary_fit"


def test_bundle_salesforce_report_package(tmp_path: Path) -> None:
    package_path = build_report_package(tmp_path)
    output_dir = tmp_path / "report_bundle"
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
    assert (output_dir / "salesforce_report_bundle.json").exists()
    assert (output_dir / "salesforce_report_definition.json").exists()
    assert (output_dir / "salesforce_report_validation_checklist.json").exists()
    assert Path(payload["review_artifact"]).exists()
    assert Path(payload["collection_index_artifact"]).exists()
    assert Path(payload["collection_landing_artifact"]).exists()
    assert Path(payload["browser_index_artifact"]).exists()
    assert Path(payload["browser_landing_artifact"]).exists()
    assert Path(payload["browser_health_index_artifact"]).exists()
    assert Path(payload["browser_health_landing_artifact"]).exists()
    assert isinstance(payload["browser_health_summary"], dict)
    assert isinstance(payload["browser_health_summary"].get("run_recency_counts"), dict)
    review_text = Path(payload["review_artifact"]).read_text(encoding="utf-8")
    collection_overview = Path(payload["collection_landing_artifact"]).read_text(encoding="utf-8")
    browser_overview = Path(payload["browser_landing_artifact"]).read_text(encoding="utf-8")
    browser_health = json.loads(Path(payload["browser_health_index_artifact"]).read_text(encoding="utf-8"))
    assert "# Salesforce Report Run" in review_text
    assert "# Salesforce Report Runs" in collection_overview
    assert "# AI OS Collections" in browser_overview
    assert "## Health Snapshot" in browser_overview
    assert payload["browser_health_landing_artifact"] in browser_overview
    assert json.loads(Path(payload["browser_index_artifact"]).read_text(encoding="utf-8"))["health_summary"]["collection_count"] >= 1
    assert browser_health["collection_count"] >= 1
    assert payload["browser_health_summary"]["collection_count"] == browser_health["collection_count"]
    assert any(item["code"] == "salesforce_report_review_ready" for item in payload["messages"])
    assert any(item["code"] == "salesforce_report_collection_index_ready" for item in payload["messages"])
    assert any(item["code"] == "ai_os_browser_ready" for item in payload["messages"])
    assert any(item["code"] == "ai_os_health_ready" for item in payload["messages"])
    bundle = payload["authoring_bundle"]
    assert bundle["artifact_type"] == "salesforce_report_authoring_bundle"
    assert bundle["report_definition"]["report_format"] == "tabular"
    assert "Owner" in bundle["report_definition"]["columns"]
    assert bundle["handoff_target"]["surface_type"] == "crma_dashboard"
    assert bundle["handoff_target"]["destination_type"] == "dashboard"
    assert bundle["action_surface_assessment"]["verdict"] in {"moderate_follow_up_fit", "strong_follow_up_fit"}
    assert bundle["validation_checklist"]["action_surface_assessment"] == bundle["action_surface_assessment"]
    assert any("grouping keeps row-level owner accountability visible" in item for item in bundle["validation_checklist"]["required_checks"])
    assert bundle["authoring_steps"]["steps"][0]["phase"] == "report_core"


def test_preview_salesforce_report_package(tmp_path: Path) -> None:
    package_path = build_report_package(tmp_path)
    baseline_describe = tmp_path / "baseline_report_describe.json"
    baseline_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {
                    "folderId": "00lTEST0000001AAA",
                    "reportType": {"type": "Opportunity"},
                },
                "reportExtendedMetadata": {
                    "detailColumnInfo": {
                        "FULL_NAME": {"label": "Opportunity Owner"},
                        "ACCOUNT_NAME": {"label": "Account Name"},
                        "Account.Gain_Annual_Renewal_Date__c": {"label": "DM Software Annual Renewal Date"},
                        "Opportunity.Risk_Assessment_Level__c": {"label": "Risk Assessment Level"},
                        "Opportunity.APTS_RH_Product_Family__c": {"label": "Product Family"},
                    },
                    "groupingColumnInfo": {
                        "OWNER_MANAGER": {"label": "Opportunity Owner: Manager"},
                    },
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "report_preview"
    result = run_executor(
        "preview",
        "--package",
        str(package_path),
        "--clone-from-report-id",
        "00OTb000008TZaTMAW",
        "--baseline-report-describe-json",
        str(baseline_describe),
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
    assert payload["summary"]["native_report_format"] == "SUMMARY"
    assert payload["summary"]["manual_filter_intent_count"] == 4
    assert payload["summary"]["manual_detail_intent_count"] == 2
    assert payload["summary"]["omitted_sort_intent_count"] == 2
    assert payload["summary"]["manual_authoring_pressure_score"] > 0
    assert payload["summary"]["action_surface_verdict"] in {"moderate_follow_up_fit", "strong_follow_up_fit"}
    assert payload["summary"]["autofill_count"] >= 8
    assert (output_dir / "salesforce_report_rest_preview.json").exists()
    assert (output_dir / "salesforce_report_fill_requirements.json").exists()
    assert (output_dir / "salesforce_report_autofill_summary.json").exists()
    preview = payload["rest_preview"]
    assert preview["artifact_type"] == "salesforce_report_rest_preview"
    assert preview["requests"][0]["method"] == "POST"
    assert "cloneId=00OTb000008TZaTMAW" in preview["requests"][0]["path"]
    assert preview["requests"][1]["method"] == "PATCH"
    assert preview["requests"][1]["body"]["reportMetadata"]["folderId"] == "00lTEST0000001AAA"
    assert preview["requests"][1]["body"]["reportMetadata"]["reportFormat"] == "SUMMARY"
    assert preview["requests"][1]["body"]["reportMetadata"]["reportType"]["type"] == "Opportunity"
    assert preview["requests"][1]["body"]["reportMetadata"]["groupingsDown"][0]["name"] == "OWNER_MANAGER"
    assert preview["requests"][1]["body"]["reportMetadata"]["groupingsDown"][1]["name"] == "FULL_NAME"
    assert preview["requests"][1]["body"]["reportMetadata"]["detailColumns"] == [
        "ACCOUNT_NAME",
        "Account.Gain_Annual_Renewal_Date__c",
        "Opportunity.Risk_Assessment_Level__c",
        "Opportunity.APTS_RH_Product_Family__c",
    ]
    assert preview["requests"][1]["body"]["reportMetadata"]["reportFilters"] == []
    assert preview["manual_detail_intents"] == [
        {
            "source_label": "Actual Ownership Alignment",
            "current_value": "__FILL_COLUMN_actual_ownership_alignment__",
            "reason": "No native Salesforce report field mapping was found for this packaged semantic column.",
            "guidance": "Carry this column as manual authoring intent or replace it with a real native report field/formula before live use.",
        },
        {
            "source_label": "Variance Driver: Forecast Hygiene",
            "current_value": "__FILL_COLUMN_variance_driver_forecast_hygiene__",
            "reason": "No native Salesforce report field mapping was found for this packaged semantic column.",
            "guidance": "Carry this column as manual authoring intent or replace it with a real native report field/formula before live use.",
        },
    ]
    assert preview["requests"][1]["body"]["reportMetadata"]["sortBy"] == []
    assert preview["manual_filter_intents"] == [
        {
            "source_label": "renewal_period",
            "operator": "equals",
            "value_mode": "manual",
            "reason": "No fixed filter value is packaged for this native report filter intent.",
            "guidance": "Apply this filter manually during native report authoring when the concrete operator value is known.",
        },
        {
            "source_label": "owner",
            "operator": "equals",
            "value_mode": "manual",
            "reason": "No fixed filter value is packaged for this native report filter intent.",
            "guidance": "Apply this filter manually during native report authoring when the concrete operator value is known.",
        },
        {
            "source_label": "product_family",
            "operator": "equals",
            "value_mode": "manual",
            "reason": "No fixed filter value is packaged for this native report filter intent.",
            "guidance": "Apply this filter manually during native report authoring when the concrete operator value is known.",
        },
        {
            "source_label": "risk_band",
            "operator": "equals",
            "value_mode": "manual",
            "reason": "No fixed filter value is packaged for this native report filter intent.",
            "guidance": "Apply this filter manually during native report authoring when the concrete operator value is known.",
        },
    ]
    assert preview["omitted_sort_intents"] == [
        {
            "source_label": "Action Queue: Handoff Quality",
            "sortOrder": "Desc",
            "reason": "No native Salesforce report field mapping is defined for this builder sort intent.",
            "source": "semantic_intent",
            "guidance": "Carry this sort intent into manual report authoring or omit it from the REST patch body.",
        },
        {
            "source_label": "Owner",
            "sortOrder": "Asc",
            "reason": "Grouped native report fields are omitted from automated sortBy payloads to keep the REST save contract stable.",
            "source": "native_summary_safety",
            "guidance": "Apply this grouped sort manually during native report authoring if the saved report still needs explicit grouping sort order.",
        }
    ]
    assert not any(item["category"] == "filter_operator" for item in payload["fill_requirements"])
    assert not any(item["category"] == "detail_column_mapping" for item in payload["fill_requirements"])
    assert not any(item["category"] == "filter_column_mapping" for item in payload["fill_requirements"])
    assert not any(item["category"] == "filter_value" for item in payload["fill_requirements"])
    assert not any(item["category"] == "sort_column_mapping" for item in payload["fill_requirements"])


def test_preview_salesforce_report_package_marks_matrix_as_manual_native_authoring(tmp_path: Path) -> None:
    package_path = write_matrix_report_package(tmp_path)
    output_dir = tmp_path / "matrix_report_preview"
    result = run_executor(
        "preview",
        "--package",
        str(package_path),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["summary"]["native_report_format"] == "MATRIX"
    assert payload["summary"]["native_authoring_support"] == "manual_required"
    assert any("groupingsAcross contract" in item for item in payload["rest_preview"]["notes"])


def test_preview_salesforce_report_package_applies_filter_overrides(tmp_path: Path) -> None:
    package_path = build_report_package(tmp_path)
    baseline_describe = write_manager_report_baseline_describe(tmp_path)
    override_path = write_filter_overrides_json(tmp_path)
    result = run_executor(
        "preview",
        "--package",
        str(package_path),
        "--clone-from-report-id",
        "00OTb000008TZaTMAW",
        "--baseline-report-describe-json",
        str(baseline_describe),
        "--filter-overrides-json",
        str(override_path),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["summary"]["resolved_filter_override_count"] == 4
    assert payload["summary"]["manual_filter_intent_count"] == 0
    assert any(item["code"] == "filter_override_applied" for item in payload["messages"])
    preview = payload["rest_preview"]
    assert preview["applied_filter_overrides"] == [
        {
            "source_label": "renewal_period",
            "operator": "equals",
            "value": "This Week",
            "source": f"json:{override_path}",
        },
        {
            "source_label": "owner",
            "operator": "equals",
            "value": "Taylor Smith",
            "source": f"json:{override_path}",
        },
        {
            "source_label": "product_family",
            "operator": "equals",
            "value": "Axioma",
            "source": f"json:{override_path}",
        },
        {
            "source_label": "risk_band",
            "operator": "equals",
            "value": "High",
            "source": f"json:{override_path}",
        },
    ]
    assert preview["manual_filter_intents"] == []
    assert preview["requests"][1]["body"]["reportMetadata"]["reportFilters"] == [
        {
            "column": "Account.Gain_Renewal_Period__c",
            "filterType": "fieldValue",
            "operator": "equals",
            "value": "This Week",
        },
        {
            "column": "FULL_NAME",
            "filterType": "fieldValue",
            "operator": "equals",
            "value": "Taylor Smith",
        },
        {
            "column": "Opportunity.APTS_RH_Product_Family__c",
            "filterType": "fieldValue",
            "operator": "equals",
            "value": "Axioma",
        },
        {
            "column": "Opportunity.Risk_Assessment_Level__c",
            "filterType": "fieldValue",
            "operator": "equals",
            "value": "High",
        },
    ]
    assert not any(item["category"] == "filter_value" for item in payload["fill_requirements"])


def test_apply_salesforce_report_package_warns_when_matrix_requires_manual_authoring(tmp_path: Path) -> None:
    package_path = write_matrix_report_package(tmp_path)
    baseline_describe = write_report_baseline_describe(tmp_path)
    output_dir = tmp_path / "matrix_report_apply_preview"
    result = run_executor(
        "apply",
        "--package",
        str(package_path),
        "--baseline-report-describe-json",
        str(baseline_describe),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["apply_summary"]["native_authoring_support"] == "manual_required"
    assert payload["apply_summary"]["native_authoring_ready"] is False
    assert payload["apply_summary"]["external_fill_requirement_count"] == 0
    assert payload["apply_summary"]["apply_ready"] is False
    assert any(item["code"] == "native_authoring_manual_required" for item in payload["messages"])


def test_apply_salesforce_report_package_blocks_live_matrix_native_authoring(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path)
    package_path = write_matrix_report_package(tmp_path)
    baseline_describe = write_report_baseline_describe(tmp_path)
    output_dir = tmp_path / "matrix_report_apply"
    result = run_executor(
        "apply",
        "--package",
        str(package_path),
        "--baseline-report-describe-json",
        str(baseline_describe),
        "--target-org",
        "apro@simcorp.com",
        "--evaluation",
        str(evaluation_path),
        "--output-dir",
        str(output_dir),
        "--apply",
        "--json",
    )
    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["apply_summary"]["native_authoring_support"] == "manual_required"
    assert payload["apply_summary"]["apply_ready"] is False
    assert any(item["code"] == "native_authoring_manual_required" for item in payload["messages"])
    assert all(item["code"] != "apply_complete" for item in payload["messages"])


def test_complete_salesforce_report_package_blocks_live_matrix_native_authoring(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path)
    package_path = write_matrix_report_package(tmp_path)
    baseline_describe = write_report_baseline_describe(tmp_path)
    result = run_executor(
        "complete",
        "--package",
        str(package_path),
        "--baseline-report-describe-json",
        str(baseline_describe),
        "--target-org",
        "apro@simcorp.com",
        "--evaluation",
        str(evaluation_path),
        "--json",
    )
    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert any(item["code"] == "native_authoring_manual_required" for item in payload["messages"])
    assert any(item["code"] == "complete_apply_failed" for item in payload["messages"])


def test_verify_salesforce_report_package(tmp_path: Path) -> None:
    package_path = build_report_package(tmp_path)
    baseline_describe = tmp_path / "baseline_report_describe.json"
    baseline_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {
                    "folderId": "00lTEST0000001AAA",
                    "reportType": {"type": "Opportunity"},
                },
                "reportExtendedMetadata": {
                    "detailColumnInfo": {
                        "FULL_NAME": {"label": "Opportunity Owner"},
                        "ACCOUNT_NAME": {"label": "Account Name"},
                        "Account.Gain_Annual_Renewal_Date__c": {"label": "DM Software Annual Renewal Date"},
                        "Opportunity.Risk_Assessment_Level__c": {"label": "Risk Assessment Level"},
                        "Opportunity.APTS_RH_Product_Family__c": {"label": "Product Family"},
                    },
                    "groupingColumnInfo": {
                        "OWNER_MANAGER": {"label": "Opportunity Owner: Manager"},
                    },
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    actual_describe = tmp_path / "actual_report_describe.json"
    actual_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {
                    "folderId": "00lTEST0000001AAA",
                    "reportType": {"type": "Opportunity"},
                    "reportFormat": "SUMMARY",
                    "groupingsDown": [
                        {"name": "OWNER_MANAGER"},
                        {"name": "FULL_NAME"},
                    ],
                    "detailColumns": [
                        "FULL_NAME",
                        "ACCOUNT_NAME",
                        "Account.Gain_Annual_Renewal_Date__c",
                        "Opportunity.Risk_Assessment_Level__c",
                        "Opportunity.APTS_RH_Product_Family__c",
                        "Opportunity.Manual_Semantic_Column__c",
                    ],
                    "reportFilters": [
                        {
                            "column": "Opportunity.StageName",
                            "filterType": "fieldValue",
                            "operator": "equals",
                            "value": "Proposal",
                        }
                    ],
                    "sortBy": [
                        {
                            "sortColumn": "FULL_NAME",
                            "sortOrder": "Asc",
                        },
                        {
                            "sortColumn": "Opportunity.StageName",
                            "sortOrder": "Desc",
                        },
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "report_verify"
    result = run_executor(
        "verify",
        "--package",
        str(package_path),
        "--clone-from-report-id",
        "00OTb000008TZaTMAW",
        "--baseline-report-describe-json",
        str(baseline_describe),
        "--actual-report-describe-json",
        str(actual_describe),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "read_only"
    assert payload["summary"]["finding_count"] == 3
    assert payload["summary"]["warn_count"] == 0
    assert payload["summary"]["info_count"] == 3
    assert payload["summary"]["manual_filter_intent_count"] == 4
    assert payload["summary"]["manual_detail_intent_count"] == 2
    assert payload["summary"]["omitted_sort_intent_count"] == 2
    finding_codes = {item["code"] for item in payload["findings"]}
    assert finding_codes == {
        "extra_live_detail_columns",
        "extra_live_report_filters",
        "extra_live_sorts",
    }
    assert (output_dir / "salesforce_report_verify.json").exists()
    assert payload["expected_contract"]["groupingsDown"] == ["OWNER_MANAGER", "FULL_NAME"]
    assert payload["actual_contract"]["detailColumns"][-1] == "Opportunity.Manual_Semantic_Column__c"


def test_apply_salesforce_report_package(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path)
    package_path = build_report_package(tmp_path, evaluation_path=evaluation_path)
    baseline_describe = tmp_path / "baseline_report_describe.json"
    baseline_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {
                    "folderId": "00lTEST0000001AAA",
                    "reportType": {"type": "Opportunity"},
                },
                "reportExtendedMetadata": {
                    "detailColumnInfo": {
                        "FULL_NAME": {"label": "Opportunity Owner"},
                        "ACCOUNT_NAME": {"label": "Account Name"},
                        "Account.Gain_Annual_Renewal_Date__c": {"label": "DM Software Annual Renewal Date"},
                        "Opportunity.Risk_Assessment_Level__c": {"label": "Risk Assessment Level"},
                        "Opportunity.APTS_RH_Product_Family__c": {"label": "Product Family"},
                    },
                    "groupingColumnInfo": {
                        "OWNER_MANAGER": {"label": "Opportunity Owner: Manager"},
                    },
                },
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
                "if path and path.endswith('/analytics/reports') and method == 'POST':",
                "    print(json.dumps({'id': '00OTESTCREATEDAAA', 'name': 'Applied Report'}))",
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
    output_dir = tmp_path / "report_apply"
    result = run_executor(
        "apply",
        "--package",
        str(package_path),
        "--clone-from-report-id",
        "00OTb000008TZaTMAW",
        "--baseline-report-describe-json",
        str(baseline_describe),
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
    assert payload["apply_summary"]["strategy"] == "create_new"
    assert payload["apply_summary"]["source_strategy"] == "clone_then_patch"
    assert payload["apply_summary"]["external_fill_requirement_count"] == 0
    assert payload["apply_summary"]["internal_fill_requirement_count"] == 0
    assert payload["request_preview"]["fill_requirements"] == []
    assert payload["execution_requests"][0]["method"] == "POST"
    assert payload["execution_requests"][0]["path"].endswith("/analytics/reports")
    assert payload["applied_report"] == {
        "id": "00OTESTCREATEDAAA",
        "name": "Applied Report",
    }
    assert payload["memory_record"]["run_id"] == "run_20260329_001"
    assert (output_dir / "salesforce_report_apply_preview.json").exists()
    assert (output_dir / "salesforce_report_apply_response.json").exists()
    assert Path(payload["review_artifact"]).exists()
    assert Path(payload["collection_index_artifact"]).exists()
    assert Path(payload["collection_landing_artifact"]).exists()
    assert Path(payload["browser_index_artifact"]).exists()
    assert Path(payload["browser_landing_artifact"]).exists()
    review_text = Path(payload["review_artifact"]).read_text(encoding="utf-8")
    collection_overview = Path(payload["collection_landing_artifact"]).read_text(encoding="utf-8")
    browser_overview = Path(payload["browser_landing_artifact"]).read_text(encoding="utf-8")
    assert "# Salesforce Report Run" in review_text
    assert "# Salesforce Report Runs" in collection_overview
    assert "# AI OS Collections" in browser_overview
    assert any(item["code"] == "salesforce_report_review_ready" for item in payload["messages"])
    assert any(item["code"] == "salesforce_report_collection_index_ready" for item in payload["messages"])
    assert any(item["code"] == "ai_os_browser_ready" for item in payload["messages"])
    memory_record = json.loads((tmp_path / "agent_memory" / "runs" / "run_20260329_001.json").read_text(encoding="utf-8"))
    assert memory_record["outcome"] == "salesforce_report_executor_apply_ok"
    assert "scripts/salesforce_report_executor.py" in memory_record["sequence"]


def test_complete_salesforce_report_package(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path)
    package_path = build_report_package(tmp_path, evaluation_path=evaluation_path)
    baseline_describe = tmp_path / "baseline_report_describe.json"
    baseline_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {
                    "folderId": "00lTEST0000001AAA",
                    "reportType": {"type": "Opportunity"},
                },
                "reportExtendedMetadata": {
                    "detailColumnInfo": {
                        "FULL_NAME": {"label": "Opportunity Owner"},
                        "ACCOUNT_NAME": {"label": "Account Name"},
                        "Account.Gain_Annual_Renewal_Date__c": {"label": "DM Software Annual Renewal Date"},
                        "Opportunity.Risk_Assessment_Level__c": {"label": "Risk Assessment Level"},
                        "Opportunity.APTS_RH_Product_Family__c": {"label": "Product Family"},
                    },
                    "groupingColumnInfo": {
                        "OWNER_MANAGER": {"label": "Opportunity Owner: Manager"},
                    },
                },
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
                "if path and path.endswith('/analytics/reports') and method == 'POST':",
                "    print(json.dumps({'id': '00OTESTCREATEDAAA', 'name': 'Applied Report'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/reports/00OTESTCREATEDAAA/describe') and method == 'GET':",
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
                "print(json.dumps({'args': args}))",
                "raise SystemExit(1)",
            ]
        ),
        encoding='utf-8',
    )
    fake_sf.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["CRM_AI_MEMORY_ROOT"] = str(tmp_path / "agent_memory")
    output_dir = tmp_path / "report_complete"
    result = run_executor(
        "complete",
        "--package",
        str(package_path),
        "--clone-from-report-id",
        "00OTb000008TZaTMAW",
        "--baseline-report-describe-json",
        str(baseline_describe),
        "--target-org",
        "apro@simcorp.com",
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
    assert payload["summary"]["verify_status"] == "ok"
    assert payload["summary"]["applied_report_id"] == "00OTESTCREATEDAAA"
    assert payload["applied_report"] == {
        "id": "00OTESTCREATEDAAA",
        "name": "Applied Report",
    }
    assert payload["apply_result"]["evaluation_gate"]["verdict"] == "pass"
    assert payload["apply_result"]["applied_report"]["id"] == "00OTESTCREATEDAAA"
    assert payload["verify_result"]["status"] == "ok"
    assert payload["memory_record"]["run_id"] == "run_20260329_001"
    assert (output_dir / "01_apply" / "salesforce_report_apply_response.json").exists()
    assert (output_dir / "02_verify" / "salesforce_report_verify.json").exists()
    assert Path(payload["review_artifact"]).exists()
    assert Path(payload["collection_index_artifact"]).exists()
    assert Path(payload["collection_landing_artifact"]).exists()
    assert Path(payload["browser_index_artifact"]).exists()
    assert Path(payload["browser_landing_artifact"]).exists()
    memory_record = json.loads((tmp_path / "agent_memory" / "runs" / "run_20260329_001.json").read_text(encoding="utf-8"))
    assert memory_record["outcome"] == "salesforce_report_executor_complete_ok"


def test_complete_salesforce_report_package_applies_filter_overrides(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path)
    package_path = build_report_package(tmp_path, evaluation_path=evaluation_path)
    baseline_describe = write_manager_report_baseline_describe(tmp_path)
    override_path = write_filter_overrides_json(tmp_path)
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
                "if path and path.endswith('/analytics/reports') and method == 'POST':",
                "    print(json.dumps({'id': '00OTESTCREATEDAAA', 'name': 'Applied Report'}))",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/reports/00OTESTCREATEDAAA/describe') and method == 'GET':",
                "    print(json.dumps({",
                "        'reportMetadata': {",
                "            'folderId': '00lTEST0000001AAA',",
                "            'reportType': {'type': 'Opportunity'},",
                "            'reportFormat': 'SUMMARY',",
                "            'groupingsDown': [{'name': 'OWNER_MANAGER'}, {'name': 'FULL_NAME'}],",
                "            'detailColumns': ['ACCOUNT_NAME', 'Account.Gain_Annual_Renewal_Date__c', 'Opportunity.Risk_Assessment_Level__c', 'Opportunity.APTS_RH_Product_Family__c'],",
                "            'reportFilters': [",
                "                {'column': 'Account.Gain_Renewal_Period__c', 'filterType': 'fieldValue', 'operator': 'equals', 'value': 'This Week'},",
                "                {'column': 'FULL_NAME', 'filterType': 'fieldValue', 'operator': 'equals', 'value': 'Taylor Smith'},",
                "                {'column': 'Opportunity.APTS_RH_Product_Family__c', 'filterType': 'fieldValue', 'operator': 'equals', 'value': 'Axioma'},",
                "                {'column': 'Opportunity.Risk_Assessment_Level__c', 'filterType': 'fieldValue', 'operator': 'equals', 'value': 'High'}",
                "            ],",
                "            'sortBy': []",
                "        }",
                "    }))",
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
    result = run_executor(
        "complete",
        "--package",
        str(package_path),
        "--clone-from-report-id",
        "00OTb000008TZaTMAW",
        "--baseline-report-describe-json",
        str(baseline_describe),
        "--filter-overrides-json",
        str(override_path),
        "--target-org",
        "apro@simcorp.com",
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["apply_result"]["apply_summary"]["resolved_filter_override_count"] == 4
    assert payload["apply_result"]["apply_summary"]["manual_filter_intent_count"] == 0
    assert payload["apply_result"]["request_preview"]["applied_filter_overrides"] == [
        {
            "source_label": "renewal_period",
            "operator": "equals",
            "value": "This Week",
            "source": f"json:{override_path}",
        },
        {
            "source_label": "owner",
            "operator": "equals",
            "value": "Taylor Smith",
            "source": f"json:{override_path}",
        },
        {
            "source_label": "product_family",
            "operator": "equals",
            "value": "Axioma",
            "source": f"json:{override_path}",
        },
        {
            "source_label": "risk_band",
            "operator": "equals",
            "value": "High",
            "source": f"json:{override_path}",
        },
    ]
    assert payload["verify_result"]["summary"]["manual_filter_intent_count"] == 0
    assert payload["verify_result"]["summary"]["warn_count"] == 0


def test_apply_report_blocks_without_pass_evaluation(tmp_path: Path) -> None:
    package_path = build_report_package(tmp_path)
    baseline_describe = tmp_path / "baseline_report_describe.json"
    baseline_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {"folderId": "00lTEST0000001AAA", "reportType": {"type": "Opportunity"}},
                "reportExtendedMetadata": {},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    result = run_executor(
        "apply",
        "--package",
        str(package_path),
        "--clone-from-report-id",
        "00OTb000008TZaTMAW",
        "--baseline-report-describe-json",
        str(baseline_describe),
        "--target-org",
        "apro@simcorp.com",
        "--apply",
        "--json",
    )
    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert any(item["code"] == "evaluation_required" for item in payload["messages"])


def test_apply_report_writes_bypass_audit_artifact(tmp_path: Path) -> None:
    package_path = build_report_package(tmp_path)
    baseline_describe = tmp_path / "baseline_report_describe.json"
    baseline_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {"folderId": "00lTEST0000001AAA", "reportType": {"type": "Opportunity"}},
                "reportExtendedMetadata": {},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "report_apply"
    env = os.environ.copy()
    env["CRM_AI_MEMORY_ROOT"] = str(tmp_path / "agent_memory")
    result = run_executor(
        "apply",
        "--package",
        str(package_path),
        "--clone-from-report-id",
        "00OTb000008TZaTMAW",
        "--baseline-report-describe-json",
        str(baseline_describe),
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
    assert payload["memory_record"]["run_id"] == "report_apply"
    bypass_path = output_dir / "evaluation_bypass_audit.json"
    assert bypass_path.exists()
    bypass_payload = json.loads(bypass_path.read_text(encoding="utf-8"))
    assert bypass_payload["policy_exceptions"] == ["evaluation_bypass"]
    assert bypass_payload["evaluation_gate"]["bypassed"] is True
    memory_record = json.loads((tmp_path / "agent_memory" / "runs" / "report_apply.json").read_text(encoding="utf-8"))
    assert "Build a salesforce report" in memory_record["goal"]
    assert "Manager owner list report for renewals needing follow-up this week" in memory_record["goal"]
    assert memory_record["policy_exceptions"] == ["evaluation_bypass"]
    assert memory_record["outcome"] == "salesforce_report_executor_apply_error"


def test_complete_report_blocks_non_pass_evaluation(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path, verdict="needs_more_evidence")
    package_path = build_report_package(tmp_path, evaluation_path=evaluation_path)
    baseline_describe = tmp_path / "baseline_report_describe.json"
    baseline_describe.write_text(
        json.dumps(
            {
                "reportMetadata": {"folderId": "00lTEST0000001AAA", "reportType": {"type": "Opportunity"}},
                "reportExtendedMetadata": {},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    result = run_executor(
        "complete",
        "--package",
        str(package_path),
        "--clone-from-report-id",
        "00OTb000008TZaTMAW",
        "--baseline-report-describe-json",
        str(baseline_describe),
        "--target-org",
        "apro@simcorp.com",
        "--json",
    )
    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert any(item["code"] == "evaluation_not_pass" for item in payload["messages"])


def test_delete_salesforce_report(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_state = tmp_path / "delete_state.json"
    fake_state.write_text(json.dumps({"delete_calls": 0, "describe_calls": 0}), encoding="utf-8")
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
                "state_path = Path(os.environ['FAKE_REPORT_DELETE_STATE'])",
                "state = json.loads(state_path.read_text())",
                "if path and path.endswith('/analytics/reports/00OTESTDELETEAAA') and method == 'DELETE':",
                "    state['delete_calls'] += 1",
                "    state_path.write_text(json.dumps(state))",
                "    print('{}')",
                "    raise SystemExit(0)",
                "if path and path.endswith('/analytics/reports/00OTESTDELETEAAA/describe') and method == 'GET':",
                "    state['describe_calls'] += 1",
                "    state_path.write_text(json.dumps(state))",
                "    if state['describe_calls'] == 1:",
                "        print(json.dumps({'reportMetadata': {'name': 'Temporary Report'}}))",
                "        raise SystemExit(0)",
                "    print(json.dumps([{'errorCode': 'NOT_FOUND', 'message': 'The data you’re trying to access is unavailable.'}]))",
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
    env["FAKE_REPORT_DELETE_STATE"] = str(fake_state)

    output_dir = tmp_path / "report_delete"
    result = run_executor(
        "delete",
        "--report-id",
        "00OTESTDELETEAAA",
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
    assert payload["summary"]["deleted_report_id"] == "00OTESTDELETEAAA"
    assert payload["summary"]["delete_verified"] is True
    assert payload["summary"]["delete_verify_attempt_count"] == 2
    assert payload["delete_verification"]["deleted"] is True
    assert payload["delete_verification"]["attempt_count"] == 2
    assert payload["delete_verification"]["attempt_results"][0]["status"] == "still_exists"
    assert payload["delete_verification"]["attempt_results"][1]["status"] == "deleted"
    assert (output_dir / "salesforce_report_delete_response.json").exists()
    assert (output_dir / "salesforce_report_delete_verify.json").exists()
