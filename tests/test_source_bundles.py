from pathlib import Path

import json

from scripts.monthly_platform.contracts import StageResult, utc_now_iso
from scripts.monthly_platform.source_bundles import (
    SourceBundleManifest,
    build_source_bundles,
)
from scripts.monthly_platform.source_requirements import SourceRequirementPlan
from scripts.monthly_platform.storage import MonthlyStorage


def _source_item(
    *,
    period_role: str,
    source_id: str,
    requirement_id: str = "sd_pipeline_inspection",
    source_type: str = "salesforce_list_view",
    dataset: str = "pipeline_inspection",
    territory: str | None = "APAC",
    director: str | None = "Jesper Tyrer",
    scope: str = "territory",
) -> dict:
    return {
        "requirement_id": requirement_id,
        "source_system": "salesforce",
        "source_type": source_type,
        "dataset": dataset,
        "output_grain": "opportunity",
        "scope": scope,
        "territory": territory,
        "director": director,
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
            "zero_row_action": "fallback",
        },
        "fallback_policy": {
            "trigger": "zero_current_quarter_active_land_pipeline",
            "from_period_role": "current_quarter",
            "to_period_role": "forward_quarter",
            "description": "test",
        },
        "consumers": [],
        "tags": [],
    }


def _make_source_run(
    tmp_path: Path,
    *,
    current_rows: list[dict],
    forward_rows: list[dict] | None = None,
    include_forward_extract: bool = True,
) -> Path:
    storage = MonthlyStorage(
        root=tmp_path,
        snapshot_date="2026-04-30",
        run_id="source-run",
    )
    storage.create_run()
    plan = SourceRequirementPlan.model_validate(
        {
            "snapshot_date": "2026-04-30",
            "status": "ok",
            "items": [
                _source_item(period_role="current_quarter", source_id="00B-current"),
                _source_item(period_role="forward_quarter", source_id="00B-forward"),
            ],
            "findings": [],
        }
    )
    plan_artifact = storage.register_json_artifact(
        artifact_id="source_requirement_plan",
        artifact_type="source_requirement_plan",
        payload=plan.model_dump(mode="json"),
        relative_path="plans/source_requirement_plan.json",
        stage_name="extract_salesforce_sources",
    )
    extracts = [
        storage.register_source_extract(
            source_type="salesforce_list_view",
            source_id="00B-current",
            source_label="PI APAC Q2",
            rows=current_rows,
            stage_name="extract_salesforce_sources",
            territory="APAC",
            director="Jesper Tyrer",
            period_role="current_quarter",
            quarter_label="Q2",
            metadata={
                "requirement_id": "sd_pipeline_inspection",
                "dataset": "pipeline_inspection",
            },
        )
    ]
    if include_forward_extract:
        extracts.append(
            storage.register_source_extract(
                source_type="salesforce_list_view",
                source_id="00B-forward",
                source_label="PI APAC Q3",
                rows=forward_rows or [],
                stage_name="extract_salesforce_sources",
                territory="APAC",
                director="Jesper Tyrer",
                period_role="forward_quarter",
                quarter_label="Q3",
                metadata={
                    "requirement_id": "sd_pipeline_inspection",
                    "dataset": "pipeline_inspection",
                },
            )
        )
    now = utc_now_iso()
    outputs = [plan_artifact]
    for extract in extracts:
        outputs.append(extract.raw_artifact)
        if extract.normalized_artifact:
            outputs.append(extract.normalized_artifact)
    storage.record_stage_result(
        StageResult(
            stage_name="extract_salesforce_sources",
            status="ok",
            started_at=now,
            finished_at=now,
            duration_seconds=0.1,
            outputs=outputs,
            source_extracts=extracts,
            metadata={
                "filters": {
                    "only_requirement": "sd_pipeline_inspection",
                    "only_territory": "APAC",
                    "max_sources": None,
                }
            },
        )
    )
    return storage.run_dir


def _pi_row(
    *,
    name: str = "Alpha",
    close_date: str = "2026-05-31",
    forecast_category: str = "Commit",
    deal_type: str = "Land",
    is_closed: bool = False,
    arr: float = 100.0,
) -> dict:
    return {
        "Name": name,
        "Account__display": "Acme",
        "Owner__display": "Owner",
        "StageName": "3 - Engagement",
        "ForecastCategoryName": forecast_category,
        "APTS_Forecast_ARR__c": arr,
        "CurrencyIsoCode": "EUR",
        "CloseDate": close_date,
        "Type": deal_type,
        "IsClosed": is_closed,
        "PushCount": 1,
        "IsPriorityRecord": False,
    }


def _pipeline_open_row() -> dict:
    return {
        "Name": "Open Deal",
        "Account__display": "Acme",
        "Owner__display": "Owner",
        "StageName": "3 - Engagement",
        "ForecastCategoryName": "Commit",
        "CloseDate": "2026-06-30",
        "APTS_Opportunity_ARR__c": 500.0,
        "APTS_Forecast_ARR__c": 250.0,
        "Probability": 50.0,
        "Type": "Land",
        "CreatedDate": "2026-01-01T00:00:00.000Z",
        "LastModifiedDate": "2026-04-01T00:00:00.000Z",
        "CurrencyIsoCode": "EUR",
    }


def test_source_bundle_maps_pipeline_open_rows(tmp_path: Path) -> None:
    storage = MonthlyStorage(
        root=tmp_path,
        snapshot_date="2026-04-30",
        run_id="source-run",
    )
    storage.create_run()
    plan = SourceRequirementPlan.model_validate(
        {
            "snapshot_date": "2026-04-30",
            "status": "ok",
            "items": [
                _source_item(
                    period_role="current_quarter",
                    source_id="00B-pipeline-open",
                    requirement_id="sd_pipeline_open",
                    dataset="pipeline_open",
                ),
            ],
            "findings": [],
        }
    )
    plan_artifact = storage.register_json_artifact(
        artifact_id="source_requirement_plan",
        artifact_type="source_requirement_plan",
        payload=plan.model_dump(mode="json"),
        relative_path="plans/source_requirement_plan.json",
        stage_name="extract_salesforce_sources",
    )
    extract = storage.register_source_extract(
        source_type="salesforce_list_view",
        source_id="00B-pipeline-open",
        source_label="Pipeline Open APAC",
        rows=[_pipeline_open_row()],
        stage_name="extract_salesforce_sources",
        territory="APAC",
        director="Jesper Tyrer",
        period_role="current_quarter",
        quarter_label="Q2",
        metadata={
            "requirement_id": "sd_pipeline_open",
            "dataset": "pipeline_open",
        },
    )
    now = utc_now_iso()
    outputs = [plan_artifact, extract.raw_artifact]
    if extract.normalized_artifact:
        outputs.append(extract.normalized_artifact)
    storage.record_stage_result(
        StageResult(
            stage_name="extract_salesforce_sources",
            status="ok",
            started_at=now,
            finished_at=now,
            duration_seconds=0.1,
            outputs=outputs,
            source_extracts=[extract],
            metadata={
                "filters": {
                    "only_requirement": "sd_pipeline_open",
                    "only_territory": "APAC",
                    "max_sources": None,
                }
            },
        )
    )

    manifest = build_source_bundles(
        source_run_dir=storage.run_dir,
        output_dir=tmp_path / "bundles",
        require_complete=True,
    )

    assert manifest.status == "ok"
    apac = json.loads((tmp_path / "bundles" / "apac.json").read_text())
    assert len(apac["pipeline_open"]) == 1
    assert apac["pipeline_open"][0]["arr_unweighted"] == 500.0
    assert apac["pipeline_open"][0]["probability"] == 50.0
    assert apac["pipeline_open"][0]["quarter"] == "Q2 2026"


def test_source_bundle_uses_reference_report_to_split_na_verticals(tmp_path: Path) -> None:
    storage = MonthlyStorage(
        root=tmp_path,
        snapshot_date="2026-04-30",
        run_id="source-run",
    )
    storage.create_run()
    plan = SourceRequirementPlan.model_validate(
        {
            "snapshot_date": "2026-04-30",
            "status": "ok",
            "items": [
                _source_item(
                    period_role="current_quarter",
                    source_id="00O-pipeline-open-reference",
                    requirement_id="sd_pipeline_open_reference",
                    source_type="salesforce_report",
                    dataset="pipeline_open_reference",
                    territory=None,
                    director=None,
                    scope="global",
                ),
                _source_item(
                    period_role="current_quarter",
                    source_id="00B-na-am-pipeline-open",
                    requirement_id="sd_pipeline_open",
                    dataset="pipeline_open",
                    territory="NA Asset Management",
                    director="Patrick Gaughan",
                ),
            ],
            "findings": [],
        }
    )
    plan_artifact = storage.register_json_artifact(
        artifact_id="source_requirement_plan",
        artifact_type="source_requirement_plan",
        payload=plan.model_dump(mode="json"),
        relative_path="plans/source_requirement_plan.json",
        stage_name="extract_salesforce_sources",
    )
    reference_extract = storage.register_source_extract(
        source_type="salesforce_report",
        source_id="00O-pipeline-open-reference",
        source_label="SD Pipeline Open FY26",
        rows=[
            {"Opportunity Name": "006-am", "Industry": "Asset Management"},
            {"Opportunity Name": "006-ins", "Industry": "Insurance"},
        ],
        stage_name="extract_salesforce_sources",
        territory=None,
        director=None,
        period_role="current_quarter",
        quarter_label="Q2",
        metadata={
            "requirement_id": "sd_pipeline_open_reference",
            "dataset": "pipeline_open_reference",
        },
    )
    am_row = {**_pipeline_open_row(), "id": "006-am", "Name": "AM Deal"}
    insurance_row = {**_pipeline_open_row(), "id": "006-ins", "Name": "Insurance Deal"}
    pipeline_extract = storage.register_source_extract(
        source_type="salesforce_list_view",
        source_id="00B-na-am-pipeline-open",
        source_label="Pipeline Open NA AM",
        rows=[am_row, insurance_row],
        stage_name="extract_salesforce_sources",
        territory="NA Asset Management",
        director="Patrick Gaughan",
        period_role="current_quarter",
        quarter_label="Q2",
        metadata={
            "requirement_id": "sd_pipeline_open",
            "dataset": "pipeline_open",
        },
    )
    now = utc_now_iso()
    outputs = [plan_artifact]
    for extract in [reference_extract, pipeline_extract]:
        outputs.append(extract.raw_artifact)
        if extract.normalized_artifact:
            outputs.append(extract.normalized_artifact)
    storage.record_stage_result(
        StageResult(
            stage_name="extract_salesforce_sources",
            status="ok",
            started_at=now,
            finished_at=now,
            duration_seconds=0.1,
            outputs=outputs,
            source_extracts=[reference_extract, pipeline_extract],
        )
    )

    manifest = build_source_bundles(
        source_run_dir=storage.run_dir,
        output_dir=tmp_path / "bundles",
        require_complete=True,
    )

    assert manifest.status == "ok"
    bundle = json.loads((tmp_path / "bundles" / "na-asset-management.json").read_text())
    assert [row["opportunity"] for row in bundle["pipeline_open"]] == ["AM Deal"]
    assert bundle["pipeline_open"][0]["industry"] == "Asset Management"


def test_source_bundle_keeps_current_quarter_when_active(tmp_path: Path) -> None:
    source_run = _make_source_run(
        tmp_path,
        current_rows=[_pi_row()],
        forward_rows=[_pi_row(name="Forward", close_date="2026-08-31")],
    )

    manifest = build_source_bundles(
        source_run_dir=source_run,
        output_dir=tmp_path / "bundles",
    )

    assert manifest.status == "ok"
    bundle = SourceBundleManifest.model_validate_json(
        (tmp_path / "bundles" / "source_bundle_manifest.json").read_text()
    )
    assert bundle.summary["forward_fallback_count"] == 0
    apac = json.loads((tmp_path / "bundles" / "apac.json").read_text())
    assert apac["pipeline_display_decision"]["display_reason"] == "current_quarter"


def test_source_bundle_falls_forward_when_current_quarter_empty(
    tmp_path: Path,
) -> None:
    source_run = _make_source_run(
        tmp_path,
        current_rows=[
            _pi_row(name="Omitted", forecast_category="Omitted"),
            _pi_row(name="Future", close_date="2027-01-31"),
        ],
        forward_rows=[_pi_row(name="Forward", close_date="2026-08-31", arr=250.0)],
    )

    manifest = build_source_bundles(
        source_run_dir=source_run,
        output_dir=tmp_path / "bundles",
    )

    assert manifest.status == "ok"
    assert manifest.summary["forward_fallback_count"] == 1
    apac = json.loads((tmp_path / "bundles" / "apac.json").read_text())
    assert (
        apac["pipeline_display_decision"]["display_reason"]
        == "forward_quarter_fallback"
    )
    assert apac["pipeline_display_decision"]["forward_quarter_active_deals"] == 1


def test_source_bundle_blocks_when_required_extract_missing(tmp_path: Path) -> None:
    source_run = _make_source_run(
        tmp_path,
        current_rows=[_pi_row()],
        include_forward_extract=False,
    )

    manifest = build_source_bundles(
        source_run_dir=source_run,
        output_dir=tmp_path / "bundles",
        require_complete=True,
    )

    assert manifest.status == "blocked"
    assert any(
        finding.issue == "selected_source_extract_missing"
        for finding in manifest.findings
    )
