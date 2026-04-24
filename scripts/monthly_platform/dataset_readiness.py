"""Audit whether extracted monthly sources can promote a DirectorBundle dataset."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import Field

from scripts.monthly_platform.contracts import (
    ContractModel,
    Finding,
    MonthlyRunManifest,
    SourceExtract,
)

ReadinessStatus = Literal["ready", "blocked"]


class DatasetFieldRequirement(ContractModel):
    field_name: str
    required: bool = True
    source_columns: list[str]
    rationale: str = ""


class DatasetReadinessReport(ContractModel):
    schema_version: str = "monthly_platform.dataset_readiness.v1"
    snapshot_date: str
    source_run_id: str
    dataset: str
    status: ReadinessStatus
    candidate_source_datasets: list[str]
    required_fields: list[str]
    optional_fields: list[str]
    matched_fields: dict[str, list[str]]
    missing_required_fields: list[str]
    available_columns_by_source_dataset: dict[str, list[str]]
    row_counts_by_source_dataset: dict[str, int]
    findings: list[Finding] = Field(default_factory=list)


PIPELINE_OPEN_REQUIREMENTS = [
    DatasetFieldRequirement(
        field_name="account",
        source_columns=["Account__display", "Account Name", "Account"],
    ),
    DatasetFieldRequirement(
        field_name="opportunity",
        source_columns=["Name", "Opportunity Name"],
    ),
    DatasetFieldRequirement(
        field_name="owner",
        source_columns=["Owner__display", "Opportunity Owner: Full Name", "Owner"],
    ),
    DatasetFieldRequirement(
        field_name="stage",
        source_columns=["StageName", "Stage"],
    ),
    DatasetFieldRequirement(
        field_name="forecast_category",
        source_columns=["ForecastCategoryName", "Forecast Category"],
    ),
    DatasetFieldRequirement(
        field_name="close_date",
        source_columns=["CloseDate", "Close Date"],
    ),
    DatasetFieldRequirement(
        field_name="arr_unweighted",
        source_columns=["APTS_Opportunity_ARR__c", "Opportunity ARR", "ARR"],
        rationale="Do not reuse weighted forecast ARR as unweighted ARR.",
    ),
    DatasetFieldRequirement(
        field_name="arr_weighted",
        source_columns=["APTS_Forecast_ARR__c", "Forecast ARR"],
    ),
    DatasetFieldRequirement(
        field_name="probability",
        source_columns=["Probability"],
        rationale="Needed to validate weighted ARR instead of trusting display values.",
    ),
    DatasetFieldRequirement(
        field_name="deal_type",
        source_columns=["Type", "Deal Type"],
    ),
    DatasetFieldRequirement(
        field_name="created_date",
        source_columns=["CreatedDate", "Created Date"],
    ),
    DatasetFieldRequirement(
        field_name="sales_region",
        required=False,
        source_columns=["Sales_Region__c", "Sales Region", "Territory2__display"],
    ),
    DatasetFieldRequirement(
        field_name="lead_scope",
        required=False,
        source_columns=["Lead_Scope__c", "Lead Scope"],
    ),
    DatasetFieldRequirement(
        field_name="industry",
        required=False,
        source_columns=["Account.Industry", "Industry"],
    ),
    DatasetFieldRequirement(
        field_name="tier",
        required=False,
        source_columns=["Account.Tier_Calculation__c", "Tier"],
    ),
]

DATASET_REQUIREMENTS: dict[str, list[DatasetFieldRequirement]] = {
    "pipeline_open": PIPELINE_OPEN_REQUIREMENTS,
}

DATASET_CANDIDATE_SOURCE_DATASETS: dict[str, list[str]] = {
    "pipeline_open": ["pipeline_open", "pipeline_inspection"],
}


def audit_dataset_readiness(
    *,
    source_run_dir: Path,
    dataset: str,
) -> DatasetReadinessReport:
    if dataset not in DATASET_REQUIREMENTS:
        raise ValueError(f"Unsupported dataset readiness audit: {dataset}")
    run_manifest = MonthlyRunManifest.model_validate_json(
        (source_run_dir / "run_manifest.json").read_text(encoding="utf-8")
    )
    candidate_source_datasets = DATASET_CANDIDATE_SOURCE_DATASETS[dataset]
    extracts = [
        extract
        for extract in run_manifest.source_extracts
        if str(extract.metadata.get("dataset") or "") in candidate_source_datasets
    ]
    columns_by_source_dataset = _columns_by_source_dataset(extracts)
    row_counts_by_source_dataset = _row_counts_by_source_dataset(extracts)
    available_columns = {
        column
        for columns in columns_by_source_dataset.values()
        for column in columns
    }
    requirements = DATASET_REQUIREMENTS[dataset]
    matched_fields: dict[str, list[str]] = {}
    missing_required_fields: list[str] = []
    for requirement in requirements:
        matched = [
            column
            for column in requirement.source_columns
            if column in available_columns
        ]
        matched_fields[requirement.field_name] = matched
        if requirement.required and not matched:
            missing_required_fields.append(requirement.field_name)
    findings = [
        Finding(
            severity="high",
            issue="dataset_not_ready_for_source_backed_promotion",
            evidence=(
                f"{dataset} missing required fields: "
                f"{', '.join(missing_required_fields)}"
            ),
        )
    ] if missing_required_fields else []
    return DatasetReadinessReport(
        snapshot_date=run_manifest.snapshot_date,
        source_run_id=run_manifest.run_id,
        dataset=dataset,
        status="blocked" if missing_required_fields else "ready",
        candidate_source_datasets=candidate_source_datasets,
        required_fields=[
            requirement.field_name for requirement in requirements if requirement.required
        ],
        optional_fields=[
            requirement.field_name
            for requirement in requirements
            if not requirement.required
        ],
        matched_fields=matched_fields,
        missing_required_fields=missing_required_fields,
        available_columns_by_source_dataset=columns_by_source_dataset,
        row_counts_by_source_dataset=row_counts_by_source_dataset,
        findings=findings,
    )


def _columns_by_source_dataset(
    extracts: list[SourceExtract],
) -> dict[str, list[str]]:
    columns_by_dataset: dict[str, set[str]] = {}
    for extract in extracts:
        dataset = str(extract.metadata.get("dataset") or "unknown")
        columns_by_dataset.setdefault(dataset, set()).update(_extract_columns(extract))
    return {
        dataset: sorted(columns)
        for dataset, columns in sorted(columns_by_dataset.items())
    }


def _row_counts_by_source_dataset(
    extracts: list[SourceExtract],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for extract in extracts:
        dataset = str(extract.metadata.get("dataset") or "unknown")
        counts[dataset] = counts.get(dataset, 0) + extract.row_count
    return counts


def _extract_columns(extract: SourceExtract) -> list[str]:
    if not extract.normalized_artifact:
        return []
    table = pd.read_parquet(extract.normalized_artifact.path)
    return [str(column) for column in table.columns]


def report_to_json(report: DatasetReadinessReport) -> str:
    return json.dumps(report.model_dump(mode="json"), indent=2)
