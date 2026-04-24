from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts import build_source_fingerprint_preflight as preflight
from scripts.monthly_platform.salesforce_reports import SalesforceSourceResult
from scripts.monthly_platform.source_requirements import (
    FieldContract,
    RowCountPolicy,
    SourcePlanItem,
)


def test_source_fingerprint_preflight_dry_run_writes_planned_manifest(
    tmp_path: Path,
) -> None:
    requirements_path = tmp_path / "requirements.json"
    territory_path = tmp_path / "territories.json"
    requirements_path.write_text(
        json.dumps(
            {
                "schema_version": "monthly_source_requirements.v1",
                "requirements": [
                    {
                        "requirement_id": "pipeline_open",
                        "source_type": "salesforce_list_view",
                        "dataset": "pipeline_open",
                        "output_grain": "opportunity",
                        "scope": "territory",
                        "period_roles": ["current_quarter"],
                        "source_path_rules": [
                            {
                                "period_role": "current_quarter",
                                "source_id_path": "list_view_id",
                                "source_label_template": "{territory} Pipeline Open",
                            }
                        ],
                        "required_fields": [{"name": "Name"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    territory_path.write_text(
        json.dumps(
            {
                "territories": {
                    "APAC": {
                        "director": "Jesper Tyrer",
                        "region": "APAC",
                        "list_view_id": "00BFAKE",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    manifest = preflight.build_source_fingerprint_preflight(
        snapshot_date="2026-04-30",
        requirements_path=requirements_path,
        territory_config_path=territory_path,
        output_root=tmp_path / "out",
        run_id="dry-run",
        dry_run=True,
    )

    assert manifest["status"] == "planned"
    assert manifest["summary"]["selected_source_count"] == 1
    assert manifest["summary"]["fingerprinted_source_count"] == 1
    assert manifest["fingerprints"][0]["salesforce_id"] == "00BFAKE"
    assert Path(manifest["output_path"]).exists()


def test_fingerprint_item_hashes_columns_and_surfaces_missing_fields() -> None:
    item = SourcePlanItem(
        requirement_id="pipeline_open_reference",
        source_system="salesforce",
        source_type="salesforce_report",
        dataset="pipeline_open_reference",
        output_grain="opportunity_reference",
        scope="global",
        period_role="current_quarter",
        quarter_label="Q2",
        quarter_title="Q2 2026",
        source_id="00OFAKE",
        source_label="Reference Report",
        status="configured",
        required_fields=[
            FieldContract(name="Opportunity Name"),
            FieldContract(name="Sales Region"),
        ],
        row_count_policy=RowCountPolicy(allow_zero=False, min_rows=1),
    )

    class FakeClient:
        def describe_report(self, **kwargs: Any) -> SalesforceSourceResult:
            return SalesforceSourceResult(
                source_type="salesforce_report",
                source_id=kwargs["report_id"],
                source_label=kwargs["source_label"],
                rows=[],
                raw_payload={},
                duration_ms=10,
                status_code=200,
                metadata={
                    "name": "Reference Report",
                    "detail_columns": ["Opportunity Name", "toLabel(StageName)"],
                    "detail_column_info_keys": ["Opportunity Name", "toLabel(StageName)"],
                    "report_filters": [{"column": "CloseDate", "operator": "equals"}],
                },
            )

    fingerprint, findings = preflight._fingerprint_item(FakeClient(), item)

    assert fingerprint["status"] == "ok"
    assert fingerprint["columns_hash"]
    assert fingerprint["filter_hash"]
    assert fingerprint["fingerprint_hash"]
    assert fingerprint["missing_required_fields"] == ["Sales Region"]
    assert preflight._field_present("StageName", preflight._column_tokens(fingerprint["metadata"]))
    assert [finding.issue for finding in findings] == [
        "source_fingerprint_required_fields_not_observed"
    ]
