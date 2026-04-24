from pathlib import Path

from scripts.monthly_platform.contracts import StageResult, utc_now_iso
from scripts.monthly_platform.dataset_readiness import audit_dataset_readiness
from scripts.monthly_platform.storage import MonthlyStorage


def _make_source_run(tmp_path: Path, rows: list[dict]) -> Path:
    storage = MonthlyStorage(
        root=tmp_path,
        snapshot_date="2026-04-30",
        run_id="source-run",
    )
    storage.create_run()
    extract = storage.register_source_extract(
        source_type="salesforce_list_view",
        source_id="00B-current",
        source_label="PI APAC Q2",
        rows=rows,
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
    now = utc_now_iso()
    outputs = [extract.raw_artifact]
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
        )
    )
    return storage.run_dir


def test_pipeline_open_readiness_blocks_missing_core_fields(tmp_path: Path) -> None:
    source_run = _make_source_run(
        tmp_path,
        rows=[
            {
                "Name": "Big Deal",
                "Account__display": "Acme",
                "StageName": "3 - Engagement",
                "ForecastCategoryName": "Commit",
                "CloseDate": "2026-06-30",
                "APTS_Forecast_ARR__c": 250.0,
                "Type": "Land",
                "CreatedDate": "2026-01-01",
            }
        ],
    )

    report = audit_dataset_readiness(
        source_run_dir=source_run,
        dataset="pipeline_open",
    )

    assert report.status == "blocked"
    assert "arr_unweighted" in report.missing_required_fields
    assert "probability" in report.missing_required_fields


def test_pipeline_open_readiness_passes_when_required_fields_exist(
    tmp_path: Path,
) -> None:
    source_run = _make_source_run(
        tmp_path,
        rows=[
            {
                "Name": "Big Deal",
                "Account__display": "Acme",
                "Owner__display": "Owner",
                "StageName": "3 - Engagement",
                "ForecastCategoryName": "Commit",
                "CloseDate": "2026-06-30",
                "APTS_Opportunity_ARR__c": 500.0,
                "APTS_Forecast_ARR__c": 250.0,
                "Probability": 50,
                "Type": "Land",
                "CreatedDate": "2026-01-01",
            }
        ],
    )

    report = audit_dataset_readiness(
        source_run_dir=source_run,
        dataset="pipeline_open",
    )

    assert report.status == "ready"
    assert report.missing_required_fields == []
