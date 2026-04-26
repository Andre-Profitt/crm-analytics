import scripts.audit_sales_director_source_contract as audit_script
from scripts.audit_sales_director_source_contract import (
    _candidate_lane,
    _discover_forward_quarter_sources,
    _evaluate_historical_alignment,
    _extract_dashboard_component_report_ids,
    _extract_historical_snapshot_dates,
    _load_forward_quarter_candidate_registry,
    _replace_quarter_tokens,
)


def test_extract_dashboard_component_report_ids_ignores_nested_dashboards() -> None:
    payload = {
        "components": [
            {"type": "Report", "reportId": "00OTb0000000001AAA"},
            {"type": "Dashboard", "id": "01aTb0000000001AAA"},
            {"type": "Report", "reportMetadata": {"id": "00OTb0000000002AAA"}},
            {"type": "Report", "sourceReportId": "00OTb0000000003AAA"},
        ]
    }

    assert _extract_dashboard_component_report_ids(payload) == [
        "00OTb0000000001AAA",
        "00OTb0000000002AAA",
        "00OTb0000000003AAA",
    ]


def test_extract_historical_snapshot_dates_dedupes_and_sorts() -> None:
    detail_columns = [
        "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-12",
        "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-07.Change",
        "Opportunity__hd.StageName__hst.CONVERT.2026-04-12",
        "Opportunity.Name",
    ]

    assert _extract_historical_snapshot_dates(detail_columns) == [
        "2026-04-07",
        "2026-04-12",
    ]


def test_evaluate_historical_alignment_flags_q3_snapshot_anchor_mismatch() -> None:
    result = _evaluate_historical_alignment(
        standard_date_filter={
            "startDate": "2026-07-01",
            "endDate": "2026-09-30",
        },
        detail_columns=[
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-01",
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-07",
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-12.Change",
        ],
        expected_start="2026-07-01",
        expected_end="2026-09-30",
        run_date="2026-08-22",
    )

    assert result["aligned"] is False
    assert result["issues"] == ["snapshot_review_month_mismatch"]
    assert result["snapshot_dates_before_window"] == [
        "2026-04-01",
        "2026-04-07",
        "2026-04-12",
    ]


def test_evaluate_historical_alignment_flags_filter_mismatch() -> None:
    result = _evaluate_historical_alignment(
        standard_date_filter={
            "startDate": "2026-04-01",
            "endDate": "2026-06-30",
        },
        detail_columns=[
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-01",
        ],
        expected_start="2026-01-01",
        expected_end="2026-03-31",
        run_date="2026-04-22",
    )

    assert result["aligned"] is False
    assert result["issues"] == ["standard_date_filter_mismatch"]


def test_evaluate_historical_alignment_allows_q1_review_snapshots_after_quarter_end() -> None:
    result = _evaluate_historical_alignment(
        standard_date_filter={
            "startDate": "2026-01-01",
            "endDate": "2026-03-31",
        },
        detail_columns=[
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-01-01",
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-03-31",
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-07",
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-12.Change",
        ],
        expected_start="2026-01-01",
        expected_end="2026-03-31",
        run_date="2026-04-22",
    )

    assert result["aligned"] is True
    assert result["issues"] == []


def test_evaluate_historical_alignment_allows_april_q3_snapshots_during_april_run() -> None:
    result = _evaluate_historical_alignment(
        standard_date_filter={
            "startDate": "2026-07-01",
            "endDate": "2026-09-30",
        },
        detail_columns=[
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-01",
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-07",
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-12",
            "Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-15",
        ],
        expected_start="2026-07-01",
        expected_end="2026-09-30",
        run_date="2026-04-22",
    )

    assert result["aligned"] is True
    assert result["issues"] == []


def test_load_forward_quarter_candidate_registry_reads_shared_config_shape() -> None:
    registry = _load_forward_quarter_candidate_registry(
        {
            "APAC": {
                "forward_quarter_pi_list_views": {
                    "Q3": {
                        "list_view_id": "00BTb00000LEdNBMA1",
                        "list_view_label": "PI ARR Forecast APAC Q3 2026 Land",
                    }
                },
                "forward_quarter_historical_trending_report_ids": {
                    "Q3": "00OTb000008jXo1MAE"
                },
            },
            "Canada": {},
        },
        "Q3",
    )

    assert registry["pi_list_views"] == {
        "APAC": {
            "list_view_id": "00BTb00000LEdNBMA1",
            "list_view_label": "PI ARR Forecast APAC Q3 2026 Land",
        }
    }
    assert registry["historical_reports"] == {
        "APAC": "00OTb000008jXo1MAE"
    }
    assert registry["missing_config"] == [
        {
            "territory": "Canada",
            "source": "forward_quarter_pi_list_views",
            "quarter_label": "Q3",
        },
        {
            "territory": "Canada",
            "source": "forward_quarter_historical_trending_report_ids",
            "quarter_label": "Q3",
        },
    ]


def test_candidate_lane_prefers_forward_quarter_alias() -> None:
    lane = _candidate_lane(
        {
            "candidate_forward_quarter": {
                "status": "warning",
                "quarter_title": "Q4 2026",
            },
            "candidate_q3": {
                "status": "ok",
                "quarter_title": "Q3 2026",
            },
        }
    )

    assert lane == {
        "status": "warning",
        "quarter_title": "Q4 2026",
    }


def test_replace_quarter_tokens_updates_title_and_identifier_forms() -> None:
    assert _replace_quarter_tokens(
        "Pipeline Forecast Review APAC Q3 2026",
        source_quarter_label="Q3",
        source_year=2026,
        target_quarter_label="Q4",
        target_year=2026,
    ) == "Pipeline Forecast Review APAC Q4 2026"
    assert _replace_quarter_tokens(
        "Pipeline_Forecast_Review_APAC_Q3_2026",
        source_quarter_label="Q3",
        source_year=2026,
        target_quarter_label="Q4",
        target_year=2026,
    ) == "Pipeline_Forecast_Review_APAC_Q4_2026"


def test_discover_forward_quarter_sources_derives_expected_names_and_matches_ids(
    monkeypatch,
) -> None:
    def fake_soql_query(_session, _instance, query: str) -> list[dict]:
        if "WHERE Id IN" in query:
            return [
                {
                    "Id": "00OT-q3-apac",
                    "Name": "Pipeline Forecast Review APAC Q3 2026",
                    "FolderName": "Revenue Operations",
                }
            ]
        if "FROM ListView" in query:
            return [
                {
                    "Id": "00B-q4-apac",
                    "Name": "PI ARR Forecast APAC Q4 2026 Land",
                    "DeveloperName": "PI_ARR_Forecast_APAC_Q4_2026_Land",
                    "SobjectType": "Opportunity",
                }
            ]
        if "FROM Report" in query and "FolderName = 'Revenue Operations'" in query:
            return [
                {
                    "Id": "00OT-q4-apac",
                    "Name": "Pipeline Forecast Review APAC Q4 2026",
                    "FolderName": "Revenue Operations",
                }
            ]
        raise AssertionError(query)

    monkeypatch.setattr(audit_script, "_soql_query", fake_soql_query)

    discovery = _discover_forward_quarter_sources(
        session=None,
        instance="https://example.my.salesforce.com",
        territory_config={
            "APAC": {
                "forward_quarter_pi_list_views": {
                    "Q3": {
                        "list_view_id": "00B-q3-apac",
                        "list_view_label": "PI ARR Forecast APAC Q3 2026 Land",
                    }
                },
                "forward_quarter_historical_trending_report_ids": {
                    "Q3": "00OT-q3-apac"
                },
            },
            "Canada": {},
        },
        quarter_label="Q4",
        quarter_year=2026,
        missing_config=[
            {
                "territory": "APAC",
                "source": "forward_quarter_pi_list_views",
                "quarter_label": "Q4",
            },
            {
                "territory": "APAC",
                "source": "forward_quarter_historical_trending_report_ids",
                "quarter_label": "Q4",
            },
            {
                "territory": "Canada",
                "source": "forward_quarter_pi_list_views",
                "quarter_label": "Q4",
            },
        ],
    )

    assert discovery["pi_list_views"] == {
        "APAC": {
            "list_view_id": "00B-q4-apac",
            "list_view_label": "PI ARR Forecast APAC Q4 2026 Land",
        }
    }
    assert discovery["historical_reports"] == {"APAC": "00OT-q4-apac"}
    assert {
        "territory": "APAC",
        "source": "forward_quarter_pi_list_views",
        "quarter_label": "Q4",
        "status": "discovered",
        "expected_name": "PI ARR Forecast APAC Q4 2026 Land",
        "discovered_id": "00B-q4-apac",
    } in discovery["discovery"]
    assert {
        "territory": "Canada",
        "source": "forward_quarter_pi_list_views",
        "quarter_label": "Q4",
        "status": "reference_unavailable",
    } in discovery["discovery"]
