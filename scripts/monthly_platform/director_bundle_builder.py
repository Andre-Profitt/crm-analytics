"""Build legacy DirectorBundle artifacts from source-bundle contracts.

This adapter is intentionally conservative: it maps only datasets backed by
the new report/list-view source bundles and leaves unsupported legacy datasets
empty until their Salesforce requirements are added to the registry.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any

from pydantic import Field

from scripts.monthly_platform.bundle_validation import validate_bundle
from scripts.monthly_platform.contracts import ContractModel, Finding, utc_now_iso
from scripts.monthly_platform.director_bundle_contract import (
    DirectorBundleContract,
    coverage_summary,
    validate_director_bundle_coverage,
)
from scripts.monthly_platform.models import (
    DatasetSource,
    Datasets,
    DirectorBundle,
    PIDeal,
    PipelineDeal,
    SourceContract,
    TrendSnapshot,
)
from scripts.monthly_platform.source_bundles import (
    PipelineInspectionRow,
    PipelineOpenRow,
    SourceBundleManifest,
    TerritorySourceBundle,
)

SALESFORCE_ORG = "simcorp.my.salesforce.com"
SALESFORCE_API_VERSION = "v66.0"
DIRECTOR_BUNDLE_SCHEMA_VERSION = "1"
DIRECTOR_BUNDLE_ADAPTER_SCHEMA_VERSION = "monthly_platform.director_bundle_manifest.v1"


class DirectorBundleBuildManifest(ContractModel):
    schema_version: str = DIRECTOR_BUNDLE_ADAPTER_SCHEMA_VERSION
    snapshot_date: str
    source_run_id: str
    status: str
    generated_at: str = Field(default_factory=utc_now_iso)
    source_bundle_manifest_path: str
    output_dir: str
    bundle_paths: list[str]
    summary: dict[str, Any]
    findings: list[Finding] = Field(default_factory=list)


def build_director_bundle_from_source_bundle(
    source_bundle: TerritorySourceBundle,
    *,
    corp_ccy: str = "EUR",
    sf_org: str = SALESFORCE_ORG,
    api_version: str = SALESFORCE_API_VERSION,
) -> DirectorBundle:
    pipeline_open = [_to_pipeline_deal(row) for row in source_bundle.pipeline_open]
    pi_current = [_to_pi_deal(row) for row in source_bundle.pi_current]
    pi_forward = [_to_pi_deal(row) for row in source_bundle.pi_forward]
    snapshot_trend = _historical_rows_to_snapshot_trend(source_bundle)
    datasets = Datasets(
        pipeline_open=pipeline_open,
        won_lost=[],
        renewals=[],
        approvals=[],
        pi_current=_sort_pi_deals(pi_current),
        pi_forward=_sort_pi_deals(pi_forward),
        activity=[],
        commit_items=[],
        stage_events=[],
        forecast_category_events=[],
        close_date_events=[],
        movement_prior=[],
        movement_current=[],
        snapshot_trend=snapshot_trend,
    )
    dataset_counts = _dataset_counts(datasets)
    source_contract = SourceContract(
        sf_org=sf_org,
        api_version=api_version,
        territory_soql_where=f"source_bundle:{source_bundle.territory}",
        extract_timestamp=source_bundle.generated_at,
        sources={
            "source_bundle": DatasetSource(
                source_type="source_bundle",
                source_id=source_bundle.source_run_id,
                query_label=f"{source_bundle.territory}:source_bundle",
                row_count=sum(dataset_counts.values()),
                duration_ms=0,
            ),
            "pipeline_open": DatasetSource(
                source_type="salesforce_list_view",
                source_id=_source_extract_id_for_pipeline(source_bundle.pipeline_open),
                query_label=f"{source_bundle.territory}:pipeline_open",
                row_count=len(pipeline_open),
                duration_ms=0,
            ),
            "pi_current": DatasetSource(
                source_type="salesforce_list_view",
                source_id=_source_extract_id_for_pi(source_bundle.pi_current),
                query_label=f"{source_bundle.territory}:pi_current",
                row_count=len(pi_current),
                duration_ms=0,
            ),
            "pi_forward": DatasetSource(
                source_type="salesforce_list_view",
                source_id=_source_extract_id_for_pi(source_bundle.pi_forward),
                query_label=f"{source_bundle.territory}:pi_forward",
                row_count=len(pi_forward),
                duration_ms=0,
            ),
            "snapshot_trend": DatasetSource(
                source_type="salesforce_report",
                source_id=None,
                query_label=f"{source_bundle.territory}:historical_trending",
                row_count=len(snapshot_trend),
                duration_ms=0,
            ),
        },
    )
    return DirectorBundle(
        schema_version=DIRECTOR_BUNDLE_SCHEMA_VERSION,
        snapshot_date=source_bundle.snapshot_date,
        director=source_bundle.director or source_bundle.territory,
        territory=source_bundle.territory,
        corp_ccy=corp_ccy,
        extract_timestamp=source_bundle.generated_at,
        source_contract=source_contract,
        dataset_counts=dataset_counts,
        datasets=datasets,
    )


def build_director_bundles_from_source_bundles(
    *,
    source_bundle_dir: Path,
    output_dir: Path,
    require_valid: bool = False,
    bundle_contract: DirectorBundleContract | None = None,
) -> DirectorBundleBuildManifest:
    source_manifest_path = source_bundle_dir / "source_bundle_manifest.json"
    source_manifest = SourceBundleManifest.model_validate_json(
        source_manifest_path.read_text(encoding="utf-8")
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    findings: list[Finding] = list(source_manifest.findings)
    bundle_paths: list[str] = []
    summaries: list[dict[str, Any]] = []
    for bundle_path_text in source_manifest.bundle_paths:
        source_bundle_path = Path(bundle_path_text)
        source_bundle = TerritorySourceBundle.model_validate_json(
            source_bundle_path.read_text(encoding="utf-8")
        )
        director_bundle = build_director_bundle_from_source_bundle(source_bundle)
        validation_errors = validate_bundle(director_bundle)
        for error in validation_errors:
            findings.append(
                Finding(
                    severity="high",
                    issue="director_bundle_validation_failed",
                    evidence=f"{source_bundle.territory}: {error}",
                )
            )
        dataset_coverage: dict[str, Any] | None = None
        if bundle_contract:
            coverage_findings = validate_director_bundle_coverage(
                bundle=director_bundle,
                contract=bundle_contract,
            )
            findings.extend(coverage_findings)
            dataset_coverage = coverage_summary(
                bundle=director_bundle,
                contract=bundle_contract,
            )
        bundle_path = output_dir / f"{_slugify(source_bundle.territory)}.json"
        bundle_path.write_text(director_bundle.to_json() + "\n", encoding="utf-8")
        bundle_paths.append(str(bundle_path))
        summary = {
            "director": director_bundle.director,
            "territory": director_bundle.territory,
            "dataset_counts": director_bundle.dataset_counts,
            "display_reason": source_bundle.pipeline_display_decision.display_reason,
            "display_quarter_title": (
                source_bundle.pipeline_display_decision.display_quarter_title
            ),
        }
        if dataset_coverage:
            summary["dataset_coverage"] = dataset_coverage
        summaries.append(summary)
    status = _status_from_findings(findings, require_valid=require_valid)
    manifest = DirectorBundleBuildManifest(
        snapshot_date=source_manifest.snapshot_date,
        source_run_id=source_manifest.source_run_id,
        status=status,
        source_bundle_manifest_path=str(source_manifest_path),
        output_dir=str(output_dir),
        bundle_paths=bundle_paths,
        summary={
            "bundle_count": len(bundle_paths),
            "source_bundle_count": len(source_manifest.bundle_paths),
            "directors": summaries,
            "unsupported_datasets_empty": _unsupported_datasets(bundle_contract),
        },
        findings=findings,
    )
    (output_dir / "director_bundle_manifest.json").write_text(
        manifest.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def _to_pipeline_deal(row: PipelineOpenRow) -> PipelineDeal:
    return PipelineDeal(
        account=row.account,
        opportunity=row.opportunity,
        owner=row.owner,
        stage=row.stage,
        forecast_category=row.forecast_category,
        close_date=row.close_date,
        arr_unweighted=row.arr_unweighted,
        arr_weighted=row.arr_weighted,
        probability=row.probability,
        push_count=row.push_count,
        deal_type=row.deal_type,
        lead_scope=row.lead_scope,
        industry=row.industry,
        tier=row.tier,
        sales_region=row.sales_region,
        created_date=row.created_date,
        last_activity_date=row.last_activity_date,
        next_step=row.next_step,
        last_modified_date=row.last_modified_date,
        approved=False,
        approval_date=None,
        competitor="",
        currency=row.currency,
        age_days=row.age_days,
        quarter=row.quarter,
    )


def _to_pi_deal(row: PipelineInspectionRow) -> PIDeal:
    return PIDeal(
        opportunity=row.opportunity,
        owner=row.owner,
        stage=row.stage,
        forecast_category=row.forecast_category,
        arr_weighted=row.arr_weighted,
        currency=row.currency,
        close_date=row.close_date,
        push_count=row.push_count,
        score=row.score,
        priority=row.priority,
    )


def _historical_rows_to_snapshot_trend(
    source_bundle: TerritorySourceBundle,
) -> list[TrendSnapshot]:
    rows_by_role = {
        "prior_quarter": source_bundle.historical_trending.prior_quarter,
        "current_quarter": source_bundle.historical_trending.current_quarter,
        "forward_quarter": source_bundle.historical_trending.forward_quarter,
    }
    snapshots: list[TrendSnapshot] = []
    seen: set[tuple[str, str, str]] = set()
    for rows in rows_by_role.values():
        for row in rows:
            opportunity = str(
                _first(row, "Opportunity Name", "Opportunity", "Name") or ""
            )
            account = str(
                _first(row, "Account Name", "Account Name: Account Name", "Account")
                or ""
            )
            close_date = str(_first(row, "Close Date", "CloseDate") or "")[:10]
            snapshot_columns = _snapshot_columns(row)
            for snapshot_date in sorted(snapshot_columns):
                stage_key = snapshot_columns[snapshot_date].get("stage")
                arr_key = snapshot_columns[snapshot_date].get("arr")
                stage = str(row.get(stage_key) or "") if stage_key else ""
                arr = _to_float(row.get(arr_key)) if arr_key else 0.0
                key = (opportunity, snapshot_date, close_date)
                if key in seen or not (opportunity or account):
                    continue
                seen.add(key)
                snapshots.append(
                    TrendSnapshot(
                        opportunity=opportunity,
                        account=account,
                        close_date=close_date,
                        snapshot_date=snapshot_date,
                        arr_at_snapshot=arr,
                        stage_at_snapshot=stage,
                    )
                )
    return sorted(
        snapshots,
        key=lambda row: (row.snapshot_date, row.close_date, row.opportunity),
    )


def _snapshot_columns(row: dict[str, Any]) -> dict[str, dict[str, str]]:
    columns: dict[str, dict[str, str]] = {}
    for key in row:
        key_text = str(key)
        if "Change" in key_text:
            continue
        date_token = _last_iso_date(key_text)
        if not date_token:
            continue
        bucket = columns.setdefault(date_token, {})
        if key_text.startswith("Stage"):
            bucket["stage"] = key_text
        elif "ARR" in key_text:
            bucket["arr"] = key_text
    return columns


def _dataset_counts(datasets: Datasets) -> dict[str, int]:
    payload = asdict(datasets)
    return {key: len(value) for key, value in payload.items()}


def _sort_pi_deals(rows: list[PIDeal]) -> list[PIDeal]:
    return sorted(rows, key=lambda row: (-(row.arr_weighted or 0), row.close_date))


def _source_extract_id_for_pi(rows: list[PipelineInspectionRow]) -> str | None:
    ids = sorted({row.source_extract_id for row in rows if row.source_extract_id})
    return ",".join(ids) if ids else None


def _source_extract_id_for_pipeline(rows: list[PipelineOpenRow]) -> str | None:
    ids = sorted({row.source_extract_id for row in rows if row.source_extract_id})
    return ",".join(ids) if ids else None


def _status_from_findings(
    findings: list[Finding],
    *,
    require_valid: bool,
) -> str:
    if any(finding.severity == "high" for finding in findings):
        return "blocked" if require_valid else "warning"
    if any(finding.severity == "medium" for finding in findings):
        return "warning"
    return "ok"


def _unsupported_datasets(
    bundle_contract: DirectorBundleContract | None,
) -> list[str]:
    if not bundle_contract:
        return []
    return sorted(
        dataset.dataset
        for dataset in bundle_contract.datasets
        if dataset.policy == "optional_empty"
    )


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _last_iso_date(value: str) -> str | None:
    matches = re.findall(r"\d{4}-\d{2}-\d{2}", value)
    return matches[-1] if matches else None


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, int | float):
        return float(value)
    tokens = re.findall(r"[-+]?\d[\d.,]*", str(value))
    if not tokens:
        return 0.0
    token = tokens[-1]
    if "," in token and "." in token:
        token = token.replace(".", "").replace(",", ".")
    elif "," in token:
        token = token.replace(",", ".")
    try:
        return float(token)
    except ValueError:
        return 0.0


def _slugify(value: str) -> str:
    return "".join(ch if ch.isalnum() else "-" for ch in value.lower()).strip("-")


def manifest_to_json(manifest: DirectorBundleBuildManifest) -> str:
    return json.dumps(manifest.model_dump(mode="json"), indent=2)
