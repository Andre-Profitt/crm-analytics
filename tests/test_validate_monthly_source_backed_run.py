import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts import validate_monthly_source_backed_run as gate


NOW = "2026-04-30T09:30:00Z"
SNAPSHOT_DATE = "2026-04-30"
SOURCE_RUN_ID = "source-run"


def _artifact(path: Path, artifact_type: str) -> dict[str, Any]:
    return {
        "artifact_id": path.stem,
        "artifact_type": artifact_type,
        "path": str(path),
        "format": "json",
        "byte_count": path.stat().st_size if path.exists() else 0,
        "row_count": None,
        "sha256": "sha",
        "schema_sha256": None,
        "metadata": {},
    }


def _plan_item(
    requirement_id: str,
    dataset: str,
    source_id: str,
    period_role: str,
) -> dict[str, Any]:
    return {
        "requirement_id": requirement_id,
        "source_system": "salesforce",
        "source_type": "salesforce_list_view",
        "dataset": dataset,
        "output_grain": "opportunity",
        "scope": "territory",
        "territory": "APAC",
        "director": "Jesper Tyrer",
        "region": None,
        "period_role": period_role,
        "quarter_label": "Q3" if period_role == "forward_quarter" else "Q2",
        "quarter_title": "Q3 2026" if period_role == "forward_quarter" else "Q2 2026",
        "source_id": source_id,
        "source_label": source_id,
        "status": "configured",
        "required_fields": [],
        "row_count_policy": {
            "allow_zero": True,
            "min_rows": 0,
            "zero_row_action": "ok",
        },
        "fallback_policy": None,
        "consumers": [],
        "tags": [],
    }


def _source_extract(
    requirement_id: str,
    dataset: str,
    source_id: str,
    period_role: str,
) -> dict[str, Any]:
    raw_path = Path(f"/tmp/{requirement_id}-{period_role}.json")
    return {
        "source_extract_id": f"src-{requirement_id}-{period_role}",
        "snapshot_date": SNAPSHOT_DATE,
        "source_system": "salesforce",
        "source_type": "salesforce_list_view",
        "source_id": source_id,
        "source_label": source_id,
        "territory": "APAC",
        "director": "Jesper Tyrer",
        "region": None,
        "period_role": period_role,
        "quarter_label": "Q3" if period_role == "forward_quarter" else "Q2",
        "status": "ok",
        "row_count": 1,
        "raw_artifact": _artifact(raw_path, "raw_source_extract"),
        "normalized_artifact": None,
        "schema_sha256": "schema",
        "rowset_sha256": "rowset",
        "metadata": {
            "requirement_id": requirement_id,
            "dataset": dataset,
        },
    }


def _source_plan_items() -> list[dict[str, Any]]:
    return [
        _plan_item(
            "sd_pipeline_open",
            "pipeline_open",
            "00B-pipeline-open",
            "current_quarter",
        ),
        _plan_item(
            "sd_pipeline_inspection",
            "pipeline_inspection",
            "00B-pi-current",
            "current_quarter",
        ),
        _plan_item(
            "sd_pipeline_inspection",
            "pipeline_inspection",
            "00B-pi-forward",
            "forward_quarter",
        ),
    ]


def _source_extracts() -> list[dict[str, Any]]:
    return [
        _source_extract(
            "sd_pipeline_open",
            "pipeline_open",
            "00B-pipeline-open",
            "current_quarter",
        ),
        _source_extract(
            "sd_pipeline_inspection",
            "pipeline_inspection",
            "00B-pi-current",
            "current_quarter",
        ),
        _source_extract(
            "sd_pipeline_inspection",
            "pipeline_inspection",
            "00B-pi-forward",
            "forward_quarter",
        ),
    ]


def _pipeline_deal() -> dict[str, Any]:
    return {
        "account": "Acme",
        "opportunity": "Big Deal",
        "owner": "Owner",
        "stage": "3 - Engagement",
        "forecast_category": "Commit",
        "close_date": "2026-06-30",
        "arr_unweighted": 500.0,
        "arr_weighted": 250.0,
        "probability": 50.0,
        "push_count": 1,
        "deal_type": "Land",
        "lead_scope": "",
        "industry": "",
        "tier": "",
        "sales_region": "APAC",
        "created_date": "2026-01-01",
        "last_activity_date": None,
        "next_step": "",
        "last_modified_date": "2026-04-01",
        "approved": False,
        "approval_date": None,
        "competitor": "",
        "currency": "EUR",
        "age_days": 10,
        "quarter": "Q2 2026",
    }


def _pi_deal(name: str) -> dict[str, Any]:
    return {
        "opportunity": name,
        "owner": "Owner",
        "stage": "3 - Engagement",
        "forecast_category": "Commit",
        "arr_weighted": 250.0,
        "currency": "EUR",
        "close_date": "2026-06-30",
        "push_count": 1,
        "score": 75,
        "priority": True,
    }


def _director_bundle_payload() -> dict[str, Any]:
    dataset_counts = {
        "pipeline_open": 1,
        "won_lost": 0,
        "renewals": 0,
        "approvals": 0,
        "pi_current": 1,
        "pi_forward": 1,
        "activity": 0,
        "commit_items": 0,
        "stage_events": 0,
        "forecast_category_events": 0,
        "close_date_events": 0,
        "movement_prior": 0,
        "movement_current": 0,
        "snapshot_trend": 1,
    }
    return {
        "schema_version": "1",
        "snapshot_date": SNAPSHOT_DATE,
        "director": "Jesper Tyrer",
        "territory": "APAC",
        "corp_ccy": "EUR",
        "extract_timestamp": NOW,
        "source_contract": {
            "sf_org": "simcorp.my.salesforce.com",
            "api_version": "v66.0",
            "territory_soql_where": "source_bundle:APAC",
            "extract_timestamp": NOW,
            "sources": {
                "source_bundle": {
                    "source_type": "source_bundle",
                    "source_id": SOURCE_RUN_ID,
                    "query_label": "APAC:source_bundle",
                    "row_count": 4,
                    "duration_ms": 0,
                },
                "pipeline_open": {
                    "source_type": "salesforce_list_view",
                    "source_id": "src-pipeline-open",
                    "query_label": "APAC:pipeline_open",
                    "row_count": 1,
                    "duration_ms": 0,
                },
                "pi_current": {
                    "source_type": "salesforce_list_view",
                    "source_id": "src-pi-current",
                    "query_label": "APAC:pi_current",
                    "row_count": 1,
                    "duration_ms": 0,
                },
                "pi_forward": {
                    "source_type": "salesforce_list_view",
                    "source_id": "src-pi-forward",
                    "query_label": "APAC:pi_forward",
                    "row_count": 1,
                    "duration_ms": 0,
                },
                "snapshot_trend": {
                    "source_type": "salesforce_report",
                    "source_id": "src-history",
                    "query_label": "APAC:snapshot_trend",
                    "row_count": 1,
                    "duration_ms": 0,
                },
            },
        },
        "dataset_counts": dataset_counts,
        "datasets": {
            "pipeline_open": [_pipeline_deal()],
            "won_lost": [],
            "renewals": [],
            "approvals": [],
            "pi_current": [_pi_deal("Big Deal")],
            "pi_forward": [_pi_deal("Forward Deal")],
            "activity": [],
            "commit_items": [],
            "stage_events": [],
            "forecast_category_events": [],
            "close_date_events": [],
            "movement_prior": [],
            "movement_current": [],
            "snapshot_trend": [
                {
                    "opportunity": "Big Deal",
                    "account": "Acme",
                    "close_date": "2026-06-30",
                    "snapshot_date": "2026-04-01",
                    "arr_at_snapshot": 250.0,
                    "stage_at_snapshot": "3 - Engagement",
                }
            ],
        },
    }


def _readiness_report(status: str = "ready") -> dict[str, Any]:
    missing = [] if status == "ready" else ["probability"]
    return {
        "schema_version": "monthly_platform.dataset_readiness.v1",
        "snapshot_date": SNAPSHOT_DATE,
        "source_run_id": SOURCE_RUN_ID,
        "dataset": "pipeline_open",
        "status": status,
        "candidate_source_datasets": ["pipeline_open"],
        "required_fields": ["opportunity", "probability"],
        "optional_fields": [],
        "matched_fields": {
            "opportunity": ["Name"],
            "probability": [] if missing else ["Probability"],
        },
        "missing_required_fields": missing,
        "available_columns_by_source_dataset": {"pipeline_open": ["Name"]},
        "row_counts_by_source_dataset": {"pipeline_open": 1},
        "findings": [],
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_complete_fixture(
    tmp_path: Path,
    *,
    extracts: list[dict[str, Any]] | None = None,
    source_bundle_status: str = "ok",
    director_status: str = "ok",
    director_transform: Callable[[dict[str, Any]], None] | None = None,
    readiness_status: str | None = "ready",
) -> dict[str, Path]:
    source_run_dir = tmp_path / "monthly_salesforce_sources" / SNAPSHOT_DATE / SOURCE_RUN_ID
    source_bundle_dir = tmp_path / "monthly_source_bundles" / SNAPSHOT_DATE / SOURCE_RUN_ID
    director_bundle_dir = (
        tmp_path / "monthly_director_bundles_from_sources" / SNAPSHOT_DATE / SOURCE_RUN_ID
    )
    readiness_dir = tmp_path / "monthly_dataset_readiness" / SNAPSHOT_DATE / SOURCE_RUN_ID

    plan_path = source_run_dir / "plans" / "source_requirement_plan.json"
    _write_json(
        plan_path,
        {
            "snapshot_date": SNAPSHOT_DATE,
            "status": "ok",
            "items": _source_plan_items(),
            "findings": [],
        },
    )
    plan_artifact = _artifact(plan_path, "source_requirement_plan")
    _write_json(
        source_run_dir / "run_manifest.json",
        {
            "schema_version": "monthly_platform.contracts.v1",
            "run_id": SOURCE_RUN_ID,
            "snapshot_date": SNAPSHOT_DATE,
            "status": "ok",
            "created_at": NOW,
            "updated_at": NOW,
            "period_context": {},
            "stages": [
                {
                    "stage_name": "extract_salesforce_sources",
                    "status": "ok",
                    "started_at": NOW,
                    "finished_at": NOW,
                    "duration_seconds": 0.1,
                    "inputs": [],
                    "outputs": [plan_artifact],
                    "source_extracts": [],
                    "findings": [],
                    "metadata": {"filters": {}},
                }
            ],
            "artifacts": [plan_artifact],
            "source_extracts": extracts if extracts is not None else _source_extracts(),
            "metadata": {},
        },
    )

    source_bundle_path = source_bundle_dir / "apac-source-bundle.json"
    _write_json(source_bundle_path, {"territory": "APAC"})
    _write_json(
        source_bundle_dir / "source_bundle_manifest.json",
        {
            "schema_version": "monthly_platform.source_bundle_manifest.v1",
            "snapshot_date": SNAPSHOT_DATE,
            "source_run_id": SOURCE_RUN_ID,
            "status": source_bundle_status,
            "generated_at": NOW,
            "source_manifest_path": str(source_run_dir / "run_manifest.json"),
            "output_dir": str(source_bundle_dir),
            "territory_count": 1,
            "bundle_paths": [str(source_bundle_path)],
            "summary": {},
            "findings": [],
        },
    )

    director_payload = _director_bundle_payload()
    if director_transform:
        director_transform(director_payload)
    director_bundle_path = director_bundle_dir / "apac.json"
    _write_json(director_bundle_path, director_payload)
    _write_json(
        director_bundle_dir / "director_bundle_manifest.json",
        {
            "schema_version": "monthly_platform.director_bundle_manifest.v1",
            "snapshot_date": SNAPSHOT_DATE,
            "source_run_id": SOURCE_RUN_ID,
            "status": director_status,
            "generated_at": NOW,
            "source_bundle_manifest_path": str(
                source_bundle_dir / "source_bundle_manifest.json"
            ),
            "output_dir": str(director_bundle_dir),
            "bundle_paths": [str(director_bundle_path)],
            "summary": {},
            "findings": [],
        },
    )

    if readiness_status:
        _write_json(readiness_dir / "pipeline_open.json", _readiness_report(readiness_status))

    return {
        "source_run_dir": source_run_dir,
        "source_bundle_dir": source_bundle_dir,
        "director_bundle_dir": director_bundle_dir,
        "readiness_dir": readiness_dir,
    }


def _run_gate(
    paths: dict[str, Path],
    capsys,
    *,
    output_path: Path | None = None,
    list_view_audit_path: Path | None = None,
) -> tuple[int, dict[str, Any]]:
    args = [
        "--source-run-dir",
        str(paths["source_run_dir"]),
        "--source-bundle-dir",
        str(paths["source_bundle_dir"]),
        "--director-bundle-dir",
        str(paths["director_bundle_dir"]),
        "--readiness-dir",
        str(paths["readiness_dir"]),
    ]
    if output_path:
        args.extend(["--output-path", str(output_path)])
    if list_view_audit_path:
        args.extend(["--list-view-audit", str(list_view_audit_path)])
    exit_code = gate.main(
        args
    )
    payload = json.loads(capsys.readouterr().out)
    return exit_code, payload


def test_main_passes_complete_source_backed_run(tmp_path: Path, capsys) -> None:
    paths = _write_complete_fixture(tmp_path)
    output_path = tmp_path / "gate" / "source_backed_publish_gate.json"

    exit_code, payload = _run_gate(paths, capsys, output_path=output_path)

    assert exit_code == 0
    assert payload["status"] == "ok"
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "ok"
    assert payload["counts"]["selected_source_count"] == 3
    assert payload["counts"]["missing_selected_source_count"] == 0
    assert payload["counts"]["director_bundle_coverage_count"] == 1
    assert payload["counts"]["readiness_not_ready_count"] == 0
    assert payload["findings"] == []


def test_main_accepts_ok_list_view_filter_audit(tmp_path: Path, capsys) -> None:
    paths = _write_complete_fixture(tmp_path)
    audit_path = tmp_path / "list-view-audit.json"
    _write_json(
        audit_path,
        {
            "schema_version": "monthly_platform.pi_list_view_filter_audit.v1",
            "status": "ok",
            "view_count": 27,
            "finding_count": 0,
            "findings": [],
            "views": [],
        },
    )

    exit_code, payload = _run_gate(
        paths,
        capsys,
        list_view_audit_path=audit_path,
    )

    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["counts"]["list_view_audit_view_count"] == 27
    assert payload["counts"]["list_view_audit_finding_count"] == 0
    assert payload["list_view_audit_path"] == str(audit_path)


def test_main_blocks_non_ok_list_view_filter_audit(tmp_path: Path, capsys) -> None:
    paths = _write_complete_fixture(tmp_path)
    audit_path = tmp_path / "list-view-audit.json"
    _write_json(
        audit_path,
        {
            "schema_version": "monthly_platform.pi_list_view_filter_audit.v1",
            "status": "blocked",
            "view_count": 27,
            "finding_count": 1,
            "findings": [
                {
                    "severity": "high",
                    "issue": "expected_filter_missing",
                    "evidence": "Sales_Region__c",
                    "territory": "UK & Ireland",
                    "source_kind": "current_pi",
                    "list_view_id": "00B-current",
                }
            ],
            "views": [],
        },
    )

    exit_code, payload = _run_gate(
        paths,
        capsys,
        list_view_audit_path=audit_path,
    )

    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["counts"]["list_view_audit_finding_count"] == 1
    issues = {finding["issue"] for finding in payload["findings"]}
    assert "list_view_filter_audit_not_ok" in issues
    assert "expected_filter_missing" in issues


def test_main_blocks_missing_selected_extract(tmp_path: Path, capsys) -> None:
    paths = _write_complete_fixture(tmp_path, extracts=_source_extracts()[:-1])

    exit_code, payload = _run_gate(paths, capsys)

    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["counts"]["missing_selected_source_count"] == 1
    assert any(
        finding["issue"] == "selected_source_extract_missing"
        and "00B-pi-forward" in finding["evidence"]
        for finding in payload["findings"]
    )


def test_main_blocks_non_ok_stage_manifests(tmp_path: Path, capsys) -> None:
    paths = _write_complete_fixture(
        tmp_path,
        source_bundle_status="warning",
        director_status="warning",
    )

    exit_code, payload = _run_gate(paths, capsys)

    assert exit_code == 2
    assert payload["status"] == "blocked"
    issues = {finding["issue"] for finding in payload["findings"]}
    assert "source_bundle_manifest_not_ok" in issues
    assert "director_bundle_manifest_not_ok" in issues


def test_main_blocks_missing_required_director_coverage(
    tmp_path: Path,
    capsys,
) -> None:
    def remove_snapshot_trend(payload: dict[str, Any]) -> None:
        payload["dataset_counts"].pop("snapshot_trend")

    paths = _write_complete_fixture(tmp_path, director_transform=remove_snapshot_trend)

    exit_code, payload = _run_gate(paths, capsys)

    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert any(
        finding["issue"] == "publish_required_dataset_missing_from_director_coverage"
        and "snapshot_trend" in finding["evidence"]
        for finding in payload["findings"]
    )


def test_main_blocks_not_ready_publish_required_readiness_report(
    tmp_path: Path,
    capsys,
) -> None:
    paths = _write_complete_fixture(tmp_path, readiness_status="blocked")

    exit_code, payload = _run_gate(paths, capsys)

    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["counts"]["readiness_not_ready_count"] == 1
    assert any(
        finding["issue"] == "readiness_report_not_ready"
        and "pipeline_open" in finding["evidence"]
        for finding in payload["findings"]
    )
