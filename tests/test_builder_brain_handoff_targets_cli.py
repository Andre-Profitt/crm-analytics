from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "builder_brain_handoff_targets.py"


def load_module():
    spec = importlib.util.spec_from_file_location("builder_brain_handoff_targets", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def write_registry(tmp_path: Path, payload: dict[str, object]) -> Path:
    path = tmp_path / "builder_brain_handoff_targets.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def valid_registry_payload() -> dict[str, object]:
    return {
        "version": 1,
        "updated_on": "2026-03-26",
        "targets": [
            {
                "source_surface_id": "commercial_rhythm_control_tower",
                "target_surface_type": "salesforce_report",
                "destination_type": "report",
                "target_surface_id": "00OTb000008TZsDMAW",
                "target_surface_label": "Forecast Accuracy",
                "target_destination_name": "00OTb000008TZsDMAW",
            },
            {
                "source_surface_id": "bdr_manager",
                "target_surface_type": "crma_dashboard",
                "destination_type": "dashboard",
                "target_surface_id": "0FKTb0000000IzROAU",
                "target_surface_label": "BDR Manager",
                "target_destination_name": "0FKTb0000000IzROAU",
            },
        ],
    }


def test_validate_registry_json(tmp_path: Path) -> None:
    registry_path = write_registry(tmp_path, valid_registry_payload())
    result = run_cli("validate", "--registry-path", str(registry_path), "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["total_targets"] == 2
    assert payload["summary"]["resolved_targets"] == 2
    assert payload["summary"]["discovery_hint_targets"] == 0
    assert payload["summary"]["by_target_surface_type"]["salesforce_report"] == 1


def test_validate_rejects_duplicate_source_target_pair(tmp_path: Path) -> None:
    registry = valid_registry_payload()
    duplicate = dict(registry["targets"][0])
    registry["targets"].append(duplicate)
    registry_path = write_registry(tmp_path, registry)

    result = run_cli("validate", "--registry-path", str(registry_path), "--json")
    assert result.returncode == 1, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert any(item["code"] == "duplicate_source_target_pair" for item in payload["messages"])


def test_inventory_filters_unresolved_targets(tmp_path: Path) -> None:
    registry = valid_registry_payload()
    registry["targets"].append(
        {
            "source_surface_id": "forecast_revenue_motions",
            "target_surface_type": "salesforce_report",
            "destination_type": "report",
            "target_surface_id": None,
            "target_surface_label": "Forecast & Closed Won",
            "target_destination_name": None,
        }
    )
    registry_path = write_registry(tmp_path, registry)

    result = run_cli(
        "inventory",
        "--registry-path",
        str(registry_path),
        "--target-surface-type",
        "salesforce_report",
        "--unresolved-only",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert len(payload["targets"]) == 1
    assert payload["targets"][0]["source_surface_id"] == "forecast_revenue_motions"
    assert payload["targets"][0]["resolved"] is False
    assert payload["targets"][0]["has_discovery_hints"] is False
    assert payload["targets"][0]["source_surface_label"] == "Forecast & Revenue Motions"


def test_resolve_returns_registry_target(tmp_path: Path) -> None:
    registry_path = write_registry(tmp_path, valid_registry_payload())
    result = run_cli(
        "resolve",
        "--registry-path",
        str(registry_path),
        "--source-surface-id",
        "commercial_rhythm_control_tower",
        "--target-surface-type",
        "salesforce_report",
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["source_surface"]["source_surface_label"] == "Commercial Rhythm Control Tower"
    assert payload["resolved_target"]["target_surface_id"] == "00OTb000008TZsDMAW"
    assert payload["resolved_target"]["target_surface_type"] == "salesforce_report"


def test_validate_allows_discovery_hints_without_target_id(tmp_path: Path) -> None:
    registry = {
        "version": 1,
        "updated_on": "2026-03-26",
        "targets": [
            {
                "source_surface_id": "commercial_rhythm_control_tower",
                "target_surface_type": "salesforce_report",
                "destination_type": "report",
                "target_surface_id": None,
                "target_surface_label": None,
                "target_destination_name": None,
                "preferred_search_terms": ["Forecast Accuracy", "Forecast & Closed Won"],
            }
        ],
    }
    registry_path = write_registry(tmp_path, registry)
    result = run_cli("validate", "--registry-path", str(registry_path), "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["discovery_hint_targets"] == 1
    assert any(item["code"] == "target_discovery_hints_present" for item in payload["messages"])


def test_resolve_can_query_live_candidates_without_registry_match(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    registry = {"version": 1, "updated_on": "2026-03-26", "targets": []}
    source_lookup = module.build_source_lookup()

    monkeypatch.setattr(
        module,
        "query_salesforce_reports",
        lambda **_: [
            {
                "Id": "00OTb000008TXATMA4",
                "DeveloperName": "Forecast_ARR_vs_Quota_qeW",
                "Name": "Forecast ARR vs Quota",
                "FolderName": "COO Run The Business",
                "LastModifiedDate": "2026-02-09T14:14:00.000+0000",
                "LastViewedDate": "2026-02-23T14:01:47.000+0000",
            }
        ],
    )

    payload = module.resolve_target(
        registry,
        source_lookup=source_lookup,
        source_surface_id="commercial_rhythm_control_tower",
        target_surface_type="salesforce_report",
        check_live=True,
        target_org="apro@simcorp.com",
        search_terms=["Forecast", "Quota"],
        limit=10,
        describe_top=0,
    )
    assert payload["status"] == "warn"
    assert payload["search_terms"] == ["Forecast", "Quota"]
    assert payload["live_candidates"][0]["Id"] == "00OTb000008TXATMA4"
    assert payload["live_candidates"][0]["score"] > 0
    assert payload["suggested_target"]["Id"] == "00OTb000008TXATMA4"
    assert payload["suggested_target"]["confidence"] in {"medium", "high"}
    assert any(item["code"] == "registry_target_missing" for item in payload["messages"])


def test_rank_live_report_candidates_prefers_recent_exact_match() -> None:
    module = load_module()
    source_lookup = module.build_source_lookup()
    source_surface = source_lookup["commercial_rhythm_control_tower"]

    ranked, suggested = module.rank_live_report_candidates(
        [
            {
                "Id": "00OTb000008TZ4DMAW",
                "DeveloperName": "Forecast_Accuracy_6zx",
                "Name": "Forecast Accuracy",
                "FolderName": "COO Run The Business",
                "LastModifiedDate": "2026-02-09T02:41:52.000+0000",
                "LastViewedDate": "2026-02-09T02:41:52.000+0000",
            },
            {
                "Id": "00OTb000008TZsDMAW",
                "DeveloperName": "Forecast_Accuracy_XPQ",
                "Name": "Forecast Accuracy",
                "FolderName": "COO Run The Business",
                "LastModifiedDate": "2026-02-09T02:47:24.000+0000",
                "LastViewedDate": "2026-02-09T02:47:24.000+0000",
            },
            {
                "Id": "00OTb000008Rh4HMAS",
                "DeveloperName": "Won_Deals_Forecast_Accuracy_RSM",
                "Name": "Won Deals Forecast Accuracy",
                "FolderName": "Private Reports",
                "LastModifiedDate": "2026-01-31T12:00:00.000+0000",
                "LastViewedDate": "2026-01-31T12:00:00.000+0000",
            },
        ],
        source_surface=source_surface,
        search_terms=["Forecast Accuracy"],
    )

    assert ranked[0]["Id"] == "00OTb000008TZsDMAW"
    assert ranked[0]["score"] >= ranked[1]["score"]
    assert suggested["Id"] == "00OTb000008TZsDMAW"
    assert suggested["confidence"] == "medium"
    assert suggested["duplicate_name_count"] == 2


def test_resolve_uses_registry_discovery_hints(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    registry = {
        "version": 1,
        "updated_on": "2026-03-26",
        "targets": [
            {
                "source_surface_id": "commercial_rhythm_control_tower",
                "target_surface_type": "salesforce_report",
                "destination_type": "report",
                "target_surface_id": None,
                "target_surface_label": None,
                "target_destination_name": None,
                "preferred_search_terms": ["Forecast Accuracy"],
            }
        ],
    }
    source_lookup = module.build_source_lookup()

    monkeypatch.setattr(
        module,
        "query_salesforce_reports",
        lambda **_: [
            {
                "Id": "00OTb000008TZsDMAW",
                "DeveloperName": "Forecast_Accuracy_XPQ",
                "Name": "Forecast Accuracy",
                "FolderName": "COO Run The Business",
                "LastModifiedDate": "2026-02-09T02:47:24.000+0000",
                "LastViewedDate": "2026-02-09T02:47:24.000+0000",
            }
        ],
    )

    payload = module.resolve_target(
        registry,
        source_lookup=source_lookup,
        source_surface_id="commercial_rhythm_control_tower",
        target_surface_type="salesforce_report",
        check_live=True,
        target_org="apro@simcorp.com",
        search_terms=[],
        limit=10,
        describe_top=0,
    )
    assert payload["status"] == "warn"
    assert payload["registry_discovery_hints"] == ["Forecast Accuracy"]
    assert payload["search_terms"] == ["Forecast Accuracy"]
    assert payload["suggested_target"]["Id"] == "00OTb000008TZsDMAW"


def test_resolve_can_attach_report_fingerprints(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    registry = {
        "version": 1,
        "updated_on": "2026-03-26",
        "targets": [
            {
                "source_surface_id": "commercial_rhythm_control_tower",
                "target_surface_type": "salesforce_report",
                "destination_type": "report",
                "target_surface_id": None,
                "target_surface_label": None,
                "target_destination_name": None,
                "preferred_search_terms": ["Forecast & Closed Won"],
            }
        ],
    }
    source_lookup = module.build_source_lookup()

    monkeypatch.setattr(
        module,
        "query_salesforce_reports",
        lambda **_: [
            {
                "Id": "00OTb000008TZaTMAW",
                "DeveloperName": "Forecast_Closed_Won_tKq",
                "Name": "Forecast & Closed Won",
                "FolderName": "COO Run The Business",
                "LastModifiedDate": "2026-02-09T14:14:00.000+0000",
                "LastViewedDate": "2026-02-23T14:01:47.000+0000",
                "LastRunDate": "2026-03-03T17:22:15.000+0000",
            }
        ],
    )
    monkeypatch.setattr(
        module,
        "describe_salesforce_report",
        lambda **_: {
            "report_name": "Forecast & Closed Won",
            "report_format": "SUMMARY",
            "detail_column_count": 5,
            "detail_columns_preview": ["ACCOUNT_NAME", "OPPORTUNITY_NAME"],
            "report_filter_count": 1,
            "groupings_down_count": 1,
            "groupings_across_count": 0,
            "has_standard_date_filter": True,
        },
    )

    payload = module.resolve_target(
        registry,
        source_lookup=source_lookup,
        source_surface_id="commercial_rhythm_control_tower",
        target_surface_type="salesforce_report",
        check_live=True,
        target_org="apro@simcorp.com",
        search_terms=[],
        limit=10,
        describe_top=1,
    )
    assert payload["status"] == "ok"
    assert payload["candidate_fingerprints"]["00OTb000008TZaTMAW"]["report_format"] == "SUMMARY"
    assert payload["live_candidates"][0]["report_fingerprint"]["report_filter_count"] == 1
    assert payload["suggested_target"]["report_fingerprint"]["detail_column_count"] == 5
    assert any(item["code"] == "report_describe_complete" for item in payload["messages"])


def test_compare_targets_ranks_and_summarizes_differences(monkeypatch) -> None:
    module = load_module()
    source_lookup = module.build_source_lookup()
    registry = {
        "version": 1,
        "updated_on": "2026-03-26",
        "targets": [
            {
                "source_surface_id": "commercial_rhythm_control_tower",
                "target_surface_type": "salesforce_report",
                "destination_type": "report",
                "target_surface_id": None,
                "target_surface_label": None,
                "target_destination_name": None,
                "preferred_search_terms": ["Forecast Accuracy", "Forecast & Closed Won"],
            }
        ],
    }
    record_map = {
        "00OTb000008TZaTMAW": {
            "Id": "00OTb000008TZaTMAW",
            "DeveloperName": "Forecast_Closed_Won_tKq",
            "Name": "Forecast & Closed Won",
            "FolderName": "COO Run The Business",
            "LastModifiedDate": "2026-02-09T14:14:00.000+0000",
            "LastViewedDate": "2026-02-23T14:01:47.000+0000",
            "LastRunDate": "2026-03-03T17:22:15.000+0000",
        },
        "00OTb000008TZsDMAW": {
            "Id": "00OTb000008TZsDMAW",
            "DeveloperName": "Forecast_Accuracy_XPQ",
            "Name": "Forecast Accuracy",
            "FolderName": "COO Run The Business",
            "LastModifiedDate": "2026-02-09T02:47:24.000+0000",
            "LastViewedDate": "2026-02-09T02:47:24.000+0000",
            "LastRunDate": "2026-03-03T17:22:15.000+0000",
        },
    }
    fingerprint_map = {
        "00OTb000008TZaTMAW": {
            "report_name": "Forecast & Closed Won",
            "report_format": "SUMMARY",
            "detail_column_count": 5,
            "detail_columns_preview": ["ACCOUNT_NAME", "OPPORTUNITY_NAME"],
            "report_filter_count": 1,
            "groupings_down_count": 0,
            "groupings_across_count": 0,
            "has_standard_date_filter": True,
        },
        "00OTb000008TZsDMAW": {
            "report_name": "Forecast Accuracy",
            "report_format": "MATRIX",
            "detail_column_count": 4,
            "detail_columns_preview": ["ACCOUNT_NAME", "OPPORTUNITY_NAME"],
            "report_filter_count": 1,
            "groupings_down_count": 0,
            "groupings_across_count": 0,
            "has_standard_date_filter": True,
        },
    }

    monkeypatch.setattr(
        module,
        "fetch_salesforce_report_record",
        lambda *, target_org, report_id: record_map[report_id],
    )
    monkeypatch.setattr(
        module,
        "describe_salesforce_report",
        lambda *, target_org, report_id: fingerprint_map[report_id],
    )

    payload = module.compare_targets(
        source_lookup=source_lookup,
        registry=registry,
        source_surface_id="commercial_rhythm_control_tower",
        report_ids=["00OTb000008TZaTMAW", "00OTb000008TZsDMAW"],
        target_org="apro@simcorp.com",
        search_terms=[],
    )
    assert payload["status"] == "warn"
    assert payload["suggested_target"]["Id"] == "00OTb000008TZaTMAW"
    assert payload["fit_recommendation"]["Id"] == "00OTb000008TZaTMAW"
    assert payload["fit_recommendation"]["confidence"] == "high"
    assert payload["comparison"]["compared_count"] == 2
    assert payload["comparison"]["identical_fields"]["FolderName"] == "COO Run The Business"
    assert payload["comparison"]["differing_fields"]["report_format"]["00OTb000008TZsDMAW"] == "MATRIX"
    assert payload["reports"][0]["report_fingerprint"]["report_format"] == "SUMMARY"
    assert payload["reports"][0]["fit_assessment"]["source_pattern_id"] == "cross_suite_control_tower"
    assert payload["reports"][0]["fit_assessment"]["field_filter_alignment"] >= 3
    assert payload["reports"][0]["fit_assessment"]["executive_story_substitution_risk"] == 2
    assert payload["reports"][0]["fit_assessment"]["raw_verdict"] == "strong_follow_up_fit"
    assert payload["reports"][0]["fit_assessment"]["verdict"] == "moderate_follow_up_fit"
    assert payload["reports"][0]["fit_assessment"]["verdict_cap"] == "summary_caps_follow_up_fit"
    assert payload["reports"][1]["fit_assessment"]["executive_story_substitution_risk"] == 3
    assert payload["reports"][1]["fit_assessment"]["verdict"] == "weak_follow_up_fit"
    assert any(item["code"] == "compare_complete" for item in payload["messages"])
    assert any(item["code"] == "compare_fit_recommendation" for item in payload["messages"])


def test_assess_report_handoff_fit_prefers_tabular_follow_up_for_control_tower() -> None:
    module = load_module()
    source_surface = module.build_source_lookup()["commercial_rhythm_control_tower"]

    tabular_fit = module.assess_report_handoff_fit(
        {
            "report_format": "TABULAR",
            "report_filter_count": 2,
            "detail_column_count": 6,
            "detail_columns_preview": [
                "OWNER_NAME",
                "ACCOUNT_NAME",
                "OPPORTUNITY_NAME",
                "CLOSE_DATE",
                "AMOUNT",
                "FORECAST_CATEGORY",
            ],
        },
        source_surface=source_surface,
    )
    summary_fit = module.assess_report_handoff_fit(
        {
            "report_format": "SUMMARY",
            "report_filter_count": 2,
            "detail_column_count": 6,
            "detail_columns_preview": [
                "OWNER_NAME",
                "ACCOUNT_NAME",
                "OPPORTUNITY_NAME",
                "CLOSE_DATE",
                "AMOUNT",
                "FORECAST_CATEGORY",
            ],
        },
        source_surface=source_surface,
    )

    assert tabular_fit["owner_accountability_fit"] > summary_fit["owner_accountability_fit"]
    assert tabular_fit["handoff_complementarity"] > summary_fit["handoff_complementarity"]
    assert tabular_fit["executive_story_substitution_risk"] == 0
    assert summary_fit["executive_story_substitution_risk"] == 2
    assert tabular_fit["overall_score"] > summary_fit["overall_score"]
    assert tabular_fit["verdict"] == "strong_follow_up_fit"
    assert summary_fit["raw_verdict"] == "strong_follow_up_fit"
    assert summary_fit["verdict"] == "moderate_follow_up_fit"
    assert summary_fit["verdict_cap"] == "summary_caps_follow_up_fit"
