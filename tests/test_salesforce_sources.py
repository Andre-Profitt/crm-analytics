import json
from pathlib import Path

from scripts import extract_salesforce_sources as extractor
from scripts.monthly_platform.salesforce_auth import SalesforceAuth
from scripts.monthly_platform.salesforce_reports import (
    SalesforceSourceResult,
    normalize_list_view_record,
    normalize_report_rows,
)
from scripts.monthly_platform.source_requirements import (
    FieldContract,
    RowCountPolicy,
    SourcePlanItem,
)


def _write_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "territories": {
                    "APAC": {
                        "director": "Jesper Tyrer",
                        "pipeline_open_list_view_id": "00B-apac-pipeline-open",
                        "pipeline_open_list_view_label": "Pipeline Open APAC",
                        "pi_list_view_id": "00B-apac-q2",
                        "pi_list_view_label": "PI APAC Q2",
                        "forward_quarter_pi_list_views": {
                            "Q3": {
                                "list_view_id": "00B-apac-q3",
                                "list_view_label": "PI APAC Q3",
                            }
                        },
                        "historical_trending_report_ids": {
                            "Q1": "00O-apac-q1",
                            "Q2": "00O-apac-q2",
                        },
                        "forward_quarter_historical_trending_report_ids": {
                            "Q3": "00O-apac-q3"
                        },
                    }
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )


def test_normalize_report_rows_uses_column_labels() -> None:
    payload = {
        "reportMetadata": {
            "detailColumns": ["Opportunity.Name", "Opportunity.Amount"],
        },
        "reportExtendedMetadata": {
            "detailColumnInfo": {
                "Opportunity.Name": {"label": "Opportunity"},
                "Opportunity.Amount": {"label": "ARR"},
            }
        },
        "factMap": {
            "T!T": {
                "rows": [
                    {
                        "dataCells": [
                            {"label": "Alpha", "value": "Alpha"},
                            {"label": "EUR 100", "value": 100},
                        ]
                    }
                ]
            }
        },
    }

    assert normalize_report_rows(payload) == [{"Opportunity": "Alpha", "ARR": 100}]


def test_normalize_report_rows_prefers_label_for_struct_values() -> None:
    payload = {
        "reportMetadata": {"detailColumns": ["Opportunity.Forecast_ARR"]},
        "reportExtendedMetadata": {
            "detailColumnInfo": {"Opportunity.Forecast_ARR": {"label": "Forecast ARR"}}
        },
        "factMap": {
            "T!T": {
                "rows": [
                    {
                        "dataCells": [
                            {
                                "label": "EUR 100",
                                "value": {"amount": 100, "currency": "EUR"},
                            }
                        ]
                    }
                ]
            }
        },
    }

    assert normalize_report_rows(payload) == [{"Forecast ARR": "EUR 100"}]


def test_normalize_list_view_record_flattens_fields() -> None:
    record = {
        "id": "006-test",
        "apiName": "Opportunity",
        "fields": {
            "Name": {"value": "Alpha", "displayValue": None},
            "Amount": {"value": 100, "displayValue": "EUR 100"},
        },
    }

    assert normalize_list_view_record(record) == {
        "id": "006-test",
        "apiName": "Opportunity",
        "Name": "Alpha",
        "Amount": 100,
        "Amount__display": "EUR 100",
    }


def test_extract_sources_dry_run_writes_plan_artifact(tmp_path: Path) -> None:
    territory_config = tmp_path / "territories.json"
    output_root = tmp_path / "storage"
    _write_config(territory_config)

    result = extractor.extract_sources(
        snapshot_date="2026-04-30",
        territory_config_path=territory_config,
        output_root=output_root,
        dry_run=True,
    )

    assert result["status"] == "ok"
    assert result["dry_run"] is True
    assert result["selected_source_count"] == 7
    assert result["executed_source_count"] == 0
    assert result["skipped_source_count"] == 7
    assert Path(result["source_plan_path"]).exists()
    assert Path(result["manifest_path"]).exists()


def test_extract_sources_live_path_uses_storage_with_fake_client(
    tmp_path: Path,
    monkeypatch,
) -> None:
    territory_config = tmp_path / "territories.json"
    output_root = tmp_path / "storage"
    _write_config(territory_config)

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def run_report(self, *, report_id: str, source_label: str):
            return SalesforceSourceResult(
                source_type="salesforce_report",
                source_id=report_id,
                source_label=source_label,
                rows=[
                    {
                        "Opportunity Name": "Alpha",
                        "Industry": "Financial Services",
                        "Account Unit Group": "SC Asia",
                        "Sales Region": "APAC",
                        "ARR": 100,
                    }
                ],
                raw_payload={"reportMetadata": {"name": source_label}},
                duration_ms=12,
                status_code=200,
                metadata={"name": source_label},
            )

        def run_list_view(self, *, list_view_id: str, source_label: str):
            return SalesforceSourceResult(
                source_type="salesforce_list_view",
                source_id=list_view_id,
                source_label=source_label,
                rows=[
                    {
                        "Name": "Alpha",
                        "Account.Name": "Acme",
                        "Owner.Name": "Owner",
                        "StageName": "3 - Engagement",
                        "ForecastCategoryName": "Commit",
                        "CloseDate": "2026-06-30",
                        "APTS_Opportunity_ARR__c": 100,
                        "APTS_Forecast_ARR__c": 50,
                        "Probability": 50,
                        "Type": "Land",
                        "CreatedDate": "2026-01-01",
                    }
                ],
                raw_payload={"pages": []},
                duration_ms=7,
                status_code=200,
                metadata={"record_count": 1},
            )

    monkeypatch.setattr(
        extractor,
        "get_salesforce_auth",
        lambda target_org: SalesforceAuth(
            access_token="token",
            instance_url="https://example.my.salesforce.com",
            target_org=target_org,
        ),
    )
    monkeypatch.setattr(extractor, "build_salesforce_session", lambda auth: object())
    monkeypatch.setattr(extractor, "SalesforceSourceClient", FakeClient)

    result = extractor.extract_sources(
        snapshot_date="2026-04-30",
        territory_config_path=territory_config,
        output_root=output_root,
        dry_run=False,
        max_sources=2,
    )

    assert result["status"] == "ok"
    assert result["selected_source_count"] == 2
    assert result["executed_source_count"] == 2
    assert result["source_extract_count"] == 2
    assert result["quality_source_count"] == 2
    assert result["quality_high_finding_count"] == 0
    assert result["artifact_count"] == 6
    assert Path(result["quality_audit_path"]).exists()


def test_source_extract_quality_blocks_missing_required_field() -> None:
    item = SourcePlanItem(
        requirement_id="pipeline_open",
        source_system="salesforce",
        source_type="salesforce_list_view",
        dataset="pipeline_open",
        output_grain="opportunity",
        scope="territory",
        territory="APAC",
        period_role="current_quarter",
        quarter_label="Q2",
        quarter_title="Q2 2026",
        source_id="00BFAKE",
        source_label="APAC Pipeline Open",
        status="configured",
        required_fields=[
            FieldContract(name="Name"),
            FieldContract(name="ForecastCategoryName"),
        ],
        row_count_policy=RowCountPolicy(allow_zero=False, min_rows=1),
    )

    quality, findings = extractor.audit_source_extract_quality(
        item=item,
        rows=[{"Name": "Alpha"}],
        source_metadata={},
    )

    assert quality["status"] == "blocked"
    assert quality["missing_required_fields"] == ["ForecastCategoryName"]
    assert [finding.issue for finding in findings] == ["source_required_field_missing"]


def test_source_extract_quality_accepts_salesforce_report_display_labels() -> None:
    item = SourcePlanItem(
        requirement_id="sd_historical_trending",
        source_system="salesforce",
        source_type="salesforce_report",
        dataset="historical_trending",
        output_grain="opportunity_snapshot",
        scope="territory",
        territory="APAC",
        period_role="prior_quarter",
        quarter_label="Q1",
        quarter_title="Q1 2026",
        source_id="00OFAKE",
        source_label="APAC Historical Trending",
        status="configured",
        required_fields=[
            FieldContract(name="Opportunity Name"),
            FieldContract(name="Account Name"),
            FieldContract(name="Close Date"),
            FieldContract(name="ARR"),
            FieldContract(name="Stage"),
        ],
        row_count_policy=RowCountPolicy(allow_zero=True, min_rows=0),
    )

    quality, findings = extractor.audit_source_extract_quality(
        item=item,
        rows=[
            {
                "Opportunity Name": "006",
                "Account Name: Account Name": "001",
                "Close Date": "2026-01-14",
                "Forecast ARR (converted) (2026-01-01)": "EUR 100,00",
                "Stage (2026-01-01)": "3 - Engagement",
            }
        ],
        source_metadata={},
    )

    assert quality["status"] == "ok"
    assert quality["missing_required_fields"] == []
    assert findings == []
