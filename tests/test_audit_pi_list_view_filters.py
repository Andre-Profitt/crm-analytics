from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_pi_list_view_filters import (  # noqa: E402
    ListViewEntry,
    audit_list_view_infos,
    configured_list_view_entries,
    expected_filters_for_entry,
    load_dead_or_invalid_filter_fields,
)
from scripts.manage_pipeline_open_list_views import list_view_filter  # noqa: E402


def test_configured_list_view_entries_collects_current_open_and_forward_views() -> None:
    entries = configured_list_view_entries(
        {
            "territories": {
                "APAC": {
                    "pi_list_view_id": "00B-current",
                    "pi_list_view_label": "PI APAC SR Scope",
                    "pipeline_open_list_view_id": "00B-open",
                    "pipeline_open_list_view_label": "SD Monthly Pipeline Open APAC FY26",
                    "forward_quarter_pi_list_views": {
                        "Q3": {
                            "list_view_id": "00B-forward",
                            "list_view_label": "PI APAC Q3 SR Land",
                        }
                    },
                }
            }
        }
    )

    assert [entry.source_kind for entry in entries] == [
        "current_pi",
        "pipeline_open",
        "forward_pi",
    ]
    assert [entry.list_view_id for entry in entries] == [
        "00B-current",
        "00B-open",
        "00B-forward",
    ]


def test_expected_filters_use_sales_region_contains_and_no_dead_book_field() -> None:
    entry = ListViewEntry(
        territory="Southern Europe",
        source_kind="forward_pi",
        list_view_id="00B-forward",
        configured_label="PI SWE Q3 SR Land",
        quarter_label="Q3",
    )

    filters = expected_filters_for_entry(entry)

    assert not any(
        item["fieldApiName"] == "Sales_Director_Book__c" for item in filters
    )
    assert any(
        item["fieldApiName"] == "Sales_Region__c"
        and item["operator"] == "Contains"
        and item["operandLabels"] == ["Southwestern Europe"]
        for item in filters
    )
    assert any(
        item["fieldApiName"] == "CloseDate"
        and item["operandLabels"] == ["NEXT FISCAL QUARTER"]
        for item in filters
    )


def test_audit_list_view_infos_passes_safe_filters() -> None:
    entry = ListViewEntry(
        territory="Canada",
        source_kind="pipeline_open",
        list_view_id="00B-open",
        configured_label="SD Monthly Pipeline Open Canada FY26",
    )
    filters = expected_filters_for_entry(entry)

    audit = audit_list_view_infos(
        entries=[entry],
        records_by_id={
            "00B-open": {
                "Id": "00B-open",
                "DeveloperName": "SD_Monthly_Pipeline_Open_Canada_FY26",
                "Name": "SD Monthly Pipeline Open Canada FY26",
            }
        },
        infos_by_api_name={
            "SD_Monthly_Pipeline_Open_Canada_FY26": {
                "label": "SD Monthly Pipeline Open Canada FY26",
                "filteredByInfo": filters,
            }
        },
    )

    assert audit["status"] == "ok"
    assert audit["finding_count"] == 0
    assert audit["view_count"] == 1


def test_audit_list_view_infos_blocks_missing_region_and_dead_book_filter() -> None:
    entry = ListViewEntry(
        territory="UK & Ireland",
        source_kind="current_pi",
        list_view_id="00B-current",
        configured_label="PI UKI SR Scope",
    )
    filters = [
        list_view_filter("Sales_Director_Book__c", ["UK & Ireland"]),
        list_view_filter("StageName", ["1 - Prospecting"]),
        list_view_filter("Type", ["Land"]),
        list_view_filter("ForecastCategoryName", ["Pipeline", "Best Case", "Commit"]),
    ]

    audit = audit_list_view_infos(
        entries=[entry],
        records_by_id={
            "00B-current": {
                "Id": "00B-current",
                "DeveloperName": "PI_UKI_SR_Scope",
                "Name": "PI UKI SR Scope",
            }
        },
        infos_by_api_name={
            "PI_UKI_SR_Scope": {
                "label": "PI UKI SR Scope",
                "filteredByInfo": filters,
            }
        },
    )

    issues = {finding["issue"] for finding in audit["findings"]}
    assert audit["status"] == "blocked"
    assert "dead_or_invalid_filter_present" in issues
    assert "expected_filter_missing" in issues


def test_dead_filter_fields_are_loaded_from_guardrail_config(tmp_path: Path) -> None:
    guardrails_path = tmp_path / "salesforce_field_guardrails.json"
    guardrails_path.write_text(
        """
        {
          "schema_version": "test",
          "dead_or_invalid_filter_fields_by_object": {
            "*": ["Global_Dead__c"],
            "Opportunity": ["Territory_Book__c"]
          }
        }
        """,
        encoding="utf-8",
    )

    fields = load_dead_or_invalid_filter_fields(
        guardrails_path,
        object_api_name="Opportunity",
    )

    assert fields == {"Global_Dead__c", "Territory_Book__c"}
