from scripts.manage_pipeline_open_list_views import (
    DISPLAY_COLUMNS,
    pipeline_open_filters,
    pipeline_open_label,
)
from scripts.create_pi_land_forecast_views import build_list_payload


def test_pipeline_open_labels_fit_salesforce_limit() -> None:
    territories = [
        "APAC",
        "Central Europe",
        "UK & Ireland",
        "Southern Europe",
        "NL & Nordics",
        "Middle East & Africa",
        "Canada",
        "NA Asset Management",
        "Pension & Insurance",
    ]

    labels = [pipeline_open_label(territory, "FY26") for territory in territories]

    assert all(len(label) <= 40 for label in labels)
    assert pipeline_open_label("Pension & Insurance", "FY26").endswith("P&I FY26")


def test_pipeline_open_filters_use_sales_region_and_book_only_for_na_splits() -> None:
    apac_filters = pipeline_open_filters("APAC")
    central_filters = pipeline_open_filters("Central Europe")
    na_am_filters = pipeline_open_filters("NA Asset Management")
    pi_filters = pipeline_open_filters("Pension & Insurance")

    assert any(
        item["fieldApiName"] == "Sales_Region__c"
        and item["operator"] == "Contains"
        and item["operandLabels"] == ["APAC"]
        for item in apac_filters
    )
    assert not any(item["fieldApiName"] == "Sales_Director_Book__c" for item in central_filters)
    assert any(
        item["fieldApiName"] == "Sales_Region__c"
        and item["operator"] == "Contains"
        and item["operandLabels"] == ["Central Europe"]
        for item in central_filters
    )
    assert not any(item["fieldApiName"] == "Sales_Director_Book__c" for item in na_am_filters)
    assert not any(item["fieldApiName"] == "Sales_Director_Book__c" for item in pi_filters)
    assert not any(item["fieldApiName"] == "Account.Industry" for item in na_am_filters)
    assert any(
        item["fieldApiName"] == "Account_Unit__c"
        and item["operandLabels"] == ["SC USA"]
        for item in na_am_filters
    )


def test_pipeline_open_payload_respects_list_view_column_cap() -> None:
    assert len(DISPLAY_COLUMNS) == 15


def test_forward_quarter_pi_payload_uses_sales_region_policy() -> None:
    payload = build_list_payload(
        territory_label="Southern Europe",
        quarter_code="Q3 2026",
        close_date_literal="NEXT FISCAL QUARTER",
    )

    assert not any(
        item["fieldApiName"] == "Sales_Director_Book__c"
        for item in payload["filteredByInfo"]
    )
    assert any(
        item["fieldApiName"] == "Sales_Region__c"
        and item["operator"] == "Contains"
        and item["operandLabels"] == ["Southwestern Europe"]
        for item in payload["filteredByInfo"]
    )
    assert any(
        item["fieldApiName"] == "CloseDate"
        and item["operandLabels"] == ["NEXT FISCAL QUARTER"]
        for item in payload["filteredByInfo"]
    )
