"""Transform stored Salesforce source extracts into territory source bundles.

This is the seam between raw report/list-view extraction and the future
DirectorBundle builder. It does not pretend every DirectorBundle field exists
yet; it creates a replayable, validated source bundle with fallback decisions.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import Field

from scripts.monthly_platform.contracts import (
    ContractModel,
    Finding,
    MonthlyRunManifest,
    SourceExtract,
    utc_now_iso,
)
from scripts.monthly_platform.period import PeriodContext, resolve_period_context
from scripts.monthly_platform.policy import make_reporting_scope
from scripts.monthly_platform.source_requirements import (
    SourcePlanItem,
    SourceRequirementPlan,
    filter_plan_items,
)


class PipelineInspectionRow(ContractModel):
    opportunity: str
    account: str = ""
    owner: str = ""
    stage: str = ""
    forecast_category: str = ""
    arr_weighted: float = 0.0
    arr_display: str = ""
    currency: str = ""
    close_date: str = ""
    deal_type: str = ""
    is_closed: bool = False
    push_count: int = 0
    score: int | None = None
    priority: bool = False
    source_extract_id: str
    active_in_period: bool = False


class PipelineOpenRow(ContractModel):
    opportunity: str
    account: str = ""
    owner: str = ""
    stage: str = ""
    forecast_category: str = ""
    close_date: str = ""
    arr_unweighted: float = 0.0
    arr_weighted: float = 0.0
    probability: float = 0.0
    push_count: int = 0
    deal_type: str = ""
    lead_scope: str = ""
    industry: str = ""
    tier: str = ""
    sales_region: str = ""
    created_date: str = ""
    last_activity_date: str | None = None
    next_step: str = ""
    last_modified_date: str = ""
    currency: str = ""
    age_days: int = 0
    quarter: str = ""
    source_extract_id: str


class HistoricalTrendRows(ContractModel):
    prior_quarter: list[dict[str, Any]] = Field(default_factory=list)
    current_quarter: list[dict[str, Any]] = Field(default_factory=list)
    forward_quarter: list[dict[str, Any]] = Field(default_factory=list)


class PipelineDisplayDecision(ContractModel):
    display_period_role: str
    display_quarter_label: str
    display_quarter_title: str
    display_reason: str
    requires_forward_quarter_fallback: bool
    current_quarter_active_deals: int
    current_quarter_active_arr: float
    forward_quarter_active_deals: int
    forward_quarter_active_arr: float


class TerritorySourceBundle(ContractModel):
    schema_version: str = "monthly_platform.source_bundle.v1"
    snapshot_date: str
    source_run_id: str
    generated_at: str = Field(default_factory=utc_now_iso)
    territory: str
    director: str | None = None
    period_context: dict[str, Any]
    source_extract_ids: list[str]
    pipeline_open: list[PipelineOpenRow] = Field(default_factory=list)
    historical_trending: HistoricalTrendRows = Field(default_factory=HistoricalTrendRows)
    pi_current: list[PipelineInspectionRow] = Field(default_factory=list)
    pi_forward: list[PipelineInspectionRow] = Field(default_factory=list)
    pipeline_display_decision: PipelineDisplayDecision
    findings: list[Finding] = Field(default_factory=list)


class SourceBundleManifest(ContractModel):
    schema_version: str = "monthly_platform.source_bundle_manifest.v1"
    snapshot_date: str
    source_run_id: str
    status: str
    generated_at: str = Field(default_factory=utc_now_iso)
    source_manifest_path: str
    output_dir: str
    territory_count: int
    bundle_paths: list[str]
    summary: dict[str, Any]
    findings: list[Finding] = Field(default_factory=list)


NA_PIPELINE_OPEN_INDUSTRIES = {
    "NA Asset Management": {"Asset Management", "Wealth Management"},
    "Pension & Insurance": {"Pension", "Insurance"},
}


def build_source_bundles(
    *,
    source_run_dir: Path,
    output_dir: Path,
    require_complete: bool = False,
) -> SourceBundleManifest:
    run_manifest_path = source_run_dir / "run_manifest.json"
    run_manifest = MonthlyRunManifest.model_validate_json(
        run_manifest_path.read_text(encoding="utf-8")
    )
    period = _period_from_manifest(run_manifest)
    source_plan = _load_source_plan(run_manifest)
    selected_plan_items = _selected_plan_items(run_manifest, source_plan)
    extracts = run_manifest.source_extracts
    findings: list[Finding] = []
    findings.extend(
        _missing_selected_extract_findings(
            selected_plan_items,
            extracts,
            require_complete=require_complete,
        )
    )

    grouped: dict[str, list[SourceExtract]] = defaultdict(list)
    for extract in extracts:
        grouped[extract.territory or "Global"].append(extract)
    global_extracts = grouped.pop("Global", [])

    output_dir.mkdir(parents=True, exist_ok=True)
    bundle_paths: list[str] = []
    bundle_summaries: list[dict[str, Any]] = []
    for territory, territory_extracts in sorted(grouped.items()):
        bundle = build_territory_source_bundle(
            source_run_id=run_manifest.run_id,
            snapshot_date=run_manifest.snapshot_date,
            territory=territory,
            extracts=[*global_extracts, *territory_extracts],
            period=period,
        )
        bundle_path = output_dir / f"{_slugify(territory)}.json"
        bundle_path.write_text(bundle.model_dump_json(indent=2) + "\n", encoding="utf-8")
        bundle_paths.append(str(bundle_path))
        bundle_summaries.append(
            {
                "territory": bundle.territory,
                "director": bundle.director,
                "source_extract_count": len(bundle.source_extract_ids),
                "pipeline_open_rows": len(bundle.pipeline_open),
                "pi_current_rows": len(bundle.pi_current),
                "pi_forward_rows": len(bundle.pi_forward),
                "display_reason": bundle.pipeline_display_decision.display_reason,
                "display_quarter_title": (
                    bundle.pipeline_display_decision.display_quarter_title
                ),
                "current_quarter_active_deals": (
                    bundle.pipeline_display_decision.current_quarter_active_deals
                ),
                "forward_quarter_active_deals": (
                    bundle.pipeline_display_decision.forward_quarter_active_deals
                ),
            }
        )
        findings.extend(bundle.findings)

    if require_complete and any(f.severity == "high" for f in findings):
        status = "blocked"
    elif any(f.severity in {"high", "medium"} for f in findings):
        status = "warning"
    else:
        status = "ok"
    manifest = SourceBundleManifest(
        snapshot_date=run_manifest.snapshot_date,
        source_run_id=run_manifest.run_id,
        status=status,
        source_manifest_path=str(run_manifest_path),
        output_dir=str(output_dir),
        territory_count=len(grouped),
        bundle_paths=bundle_paths,
        summary={
            "territories": bundle_summaries,
            "source_extract_count": len(extracts),
            "selected_source_count": len(selected_plan_items),
            "missing_selected_source_count": sum(
                1
                for finding in findings
                if finding.issue == "selected_source_extract_missing"
            ),
            "forward_fallback_count": sum(
                1
                for row in bundle_summaries
                if row["display_reason"] == "forward_quarter_fallback"
            ),
        },
        findings=findings,
    )
    (output_dir / "source_bundle_manifest.json").write_text(
        manifest.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def build_territory_source_bundle(
    *,
    source_run_id: str,
    snapshot_date: str,
    territory: str,
    extracts: list[SourceExtract],
    period: PeriodContext,
) -> TerritorySourceBundle:
    director = next((extract.director for extract in extracts if extract.director), None)
    historical_rows: dict[str, list[dict[str, Any]]] = {
        "prior_quarter": [],
        "current_quarter": [],
        "forward_quarter": [],
    }
    pipeline_open: list[PipelineOpenRow] = []
    pi_current: list[PipelineInspectionRow] = []
    pi_forward: list[PipelineInspectionRow] = []
    findings: list[Finding] = []
    pipeline_reference_by_id = pipeline_open_reference_by_id(extracts)
    for extract in extracts:
        dataset = str(extract.metadata.get("dataset") or "")
        rows = load_extract_rows(extract)
        if dataset == "historical_trending":
            historical_rows[str(extract.period_role)] = rows
        elif dataset == "pipeline_open_reference":
            continue
        elif dataset == "pipeline_open":
            pipeline_open.extend(
                normalize_pipeline_open_rows(
                    rows,
                    extract=extract,
                    period=period,
                    snapshot_date=snapshot_date,
                    reference_by_id=pipeline_reference_by_id,
                )
            )
        elif dataset == "pipeline_inspection":
            normalized = normalize_pipeline_inspection_rows(
                rows,
                extract=extract,
                period=period,
            )
            if extract.period_role == "forward_quarter":
                pi_forward.extend(normalized)
            else:
                pi_current.extend(normalized)
        else:
            findings.append(
                Finding(
                    severity="info",
                    issue="source_dataset_not_transformed",
                    evidence=f"{extract.source_extract_id}: {dataset or 'unknown'}",
                )
            )

    decision = pipeline_display_decision(
        period=period,
        pi_current=pi_current,
        pi_forward=pi_forward,
    )
    return TerritorySourceBundle(
        snapshot_date=snapshot_date,
        source_run_id=source_run_id,
        territory=territory,
        director=director,
        period_context=period.as_dict(),
        source_extract_ids=[extract.source_extract_id for extract in extracts],
        pipeline_open=pipeline_open,
        historical_trending=HistoricalTrendRows(**historical_rows),
        pi_current=pi_current,
        pi_forward=pi_forward,
        pipeline_display_decision=decision,
        findings=findings,
    )


def load_extract_rows(extract: SourceExtract) -> list[dict[str, Any]]:
    if not extract.normalized_artifact:
        return []
    table = pd.read_parquet(extract.normalized_artifact.path)
    table = table.where(pd.notna(table), None)
    return [
        {str(key): _json_clean(value) for key, value in row.items()}
        for row in table.to_dict(orient="records")
    ]


def pipeline_open_reference_by_id(
    extracts: list[SourceExtract],
) -> dict[str, dict[str, str]]:
    reference: dict[str, dict[str, str]] = {}
    for extract in extracts:
        if str(extract.metadata.get("dataset") or "") != "pipeline_open_reference":
            continue
        for row in load_extract_rows(extract):
            opportunity_id = str(_first(row, "Opportunity Name", "Opportunity ID", "id") or "")
            if not opportunity_id:
                continue
            reference[opportunity_id] = {
                "industry": str(_first(row, "Industry") or ""),
                "tier": str(_first(row, "Tier") or ""),
                "sales_region": str(_first(row, "Sales Region") or ""),
                "account_unit_group": str(_first(row, "Account Unit Group") or ""),
            }
    return reference


def normalize_pipeline_open_rows(
    rows: list[dict[str, Any]],
    *,
    extract: SourceExtract,
    period: PeriodContext,
    snapshot_date: str,
    reference_by_id: dict[str, dict[str, str]] | None = None,
) -> list[PipelineOpenRow]:
    normalized: list[PipelineOpenRow] = []
    reference_by_id = reference_by_id or {}
    for row in rows:
        opportunity_id = str(_first(row, "id", "Opportunity ID") or "")
        reference = reference_by_id.get(opportunity_id, {})
        industry = str(_first(row, "Account.Industry", "Industry") or reference.get("industry") or "")
        if not _include_pipeline_open_row(extract.territory, industry):
            continue
        close_date = str(_first(row, "CloseDate", "Close Date") or "")[:10]
        created_date = str(_first(row, "CreatedDate", "Created Date") or "")[:10]
        last_activity_date = str(
            _first(row, "LastActivityDate", "Last Activity Date") or ""
        )[:10]
        last_modified_date = str(
            _first(row, "LastModifiedDate", "Last Modified Date") or ""
        )[:10]
        normalized.append(
            PipelineOpenRow(
                opportunity=str(_first(row, "Name", "Opportunity Name") or ""),
                account=str(
                    _first(row, "Account__display", "Account Name", "Account") or ""
                ),
                owner=str(
                    _first(row, "Owner__display", "Opportunity Owner", "Owner") or ""
                ),
                stage=str(_first(row, "StageName", "Stage") or ""),
                forecast_category=str(
                    _first(row, "ForecastCategoryName", "Forecast Category") or ""
                ),
                close_date=close_date,
                arr_unweighted=_to_float(
                    _first(row, "APTS_Opportunity_ARR__c", "Opportunity ARR", "ARR")
                ),
                arr_weighted=_to_float(
                    _first(row, "APTS_Forecast_ARR__c", "Forecast ARR")
                ),
                probability=_to_float(_first(row, "Probability")),
                push_count=int(_to_float(_first(row, "PushCount", "Push Count"))),
                deal_type=str(_first(row, "Type", "Deal Type") or ""),
                lead_scope=str(_first(row, "Lead_Scope__c", "Opportunity Scope") or ""),
                industry=industry,
                tier=str(
                    _first(row, "Account.Tier_Calculation__c", "Tier")
                    or reference.get("tier")
                    or ""
                ),
                sales_region=str(
                    _first(
                        row,
                        "Sales_Region__c",
                        "Sales Region",
                        "Territory2__display",
                    )
                    or reference.get("sales_region")
                    or extract.territory
                    or ""
                ),
                created_date=created_date,
                last_activity_date=last_activity_date or None,
                next_step=str(_first(row, "NextStep", "Next Step") or ""),
                last_modified_date=last_modified_date,
                currency=str(_first(row, "CurrencyIsoCode", "Currency") or ""),
                age_days=_age_days(snapshot_date, created_date),
                quarter=_quarter_label(close_date, period.current_quarter.year),
                source_extract_id=extract.source_extract_id,
            )
        )
    return sorted(
        normalized,
        key=lambda row: (-(row.arr_unweighted or 0), row.close_date),
    )


def _include_pipeline_open_row(territory: str | None, industry: str) -> bool:
    allowed = NA_PIPELINE_OPEN_INDUSTRIES.get(str(territory or ""))
    if not allowed:
        return True
    return industry.strip() in allowed


def normalize_pipeline_inspection_rows(
    rows: list[dict[str, Any]],
    *,
    extract: SourceExtract,
    period: PeriodContext,
) -> list[PipelineInspectionRow]:
    scope = (
        make_reporting_scope(
            period.forward_quarter.start_date,
            period.forward_quarter.end_date,
        )
        if extract.period_role == "forward_quarter"
        else make_reporting_scope(
            period.current_quarter.start_date,
            period.current_quarter.end_date,
        )
    )
    normalized: list[PipelineInspectionRow] = []
    for row in rows:
        close_date = str(_first(row, "CloseDate", "Close Date") or "")[:10]
        forecast_category = str(
            _first(row, "ForecastCategoryName", "Forecast Category") or ""
        ).strip()
        deal_type = str(_first(row, "Type", "Deal Type") or "").strip()
        is_closed = _to_bool(_first(row, "IsClosed", "Closed"))
        active = (
            scope.contains(close_date)
            and deal_type.lower() == "land"
            and not is_closed
            and forecast_category not in ("", "Omitted", "Closed")
        )
        normalized.append(
            PipelineInspectionRow(
                opportunity=str(_first(row, "Name", "Opportunity", "Opportunity Name") or ""),
                account=str(
                    _first(row, "Account__display", "Account Name", "Account") or ""
                ),
                owner=str(_first(row, "Owner__display", "Owner") or ""),
                stage=str(_first(row, "StageName", "Stage") or ""),
                forecast_category=forecast_category,
                arr_weighted=_to_float(
                    _first(row, "APTS_Forecast_ARR__c", "ARR", "ARR Weighted")
                ),
                arr_display=str(
                    _first(row, "APTS_Forecast_ARR__c__display", "ARR Display") or ""
                ),
                currency=str(_first(row, "CurrencyIsoCode", "Currency") or ""),
                close_date=close_date,
                deal_type=deal_type,
                is_closed=is_closed,
                push_count=int(_to_float(_first(row, "PushCount", "Push Count"))),
                score=_score_from_row(row),
                priority=_to_bool(_first(row, "IsPriorityRecord", "Priority")),
                source_extract_id=extract.source_extract_id,
                active_in_period=active,
            )
        )
    return normalized


def pipeline_display_decision(
    *,
    period: PeriodContext,
    pi_current: list[PipelineInspectionRow],
    pi_forward: list[PipelineInspectionRow],
) -> PipelineDisplayDecision:
    current_active = [row for row in pi_current if row.active_in_period]
    forward_active = [row for row in pi_forward if row.active_in_period]
    current_arr = round(sum(row.arr_weighted for row in current_active), 2)
    forward_arr = round(sum(row.arr_weighted for row in forward_active), 2)
    if current_active:
        return PipelineDisplayDecision(
            display_period_role="current_quarter",
            display_quarter_label=period.current_quarter.label,
            display_quarter_title=period.current_quarter.title,
            display_reason="current_quarter",
            requires_forward_quarter_fallback=False,
            current_quarter_active_deals=len(current_active),
            current_quarter_active_arr=current_arr,
            forward_quarter_active_deals=len(forward_active),
            forward_quarter_active_arr=forward_arr,
        )
    if forward_active:
        return PipelineDisplayDecision(
            display_period_role="forward_quarter",
            display_quarter_label=period.forward_quarter.label,
            display_quarter_title=period.forward_quarter.title,
            display_reason="forward_quarter_fallback",
            requires_forward_quarter_fallback=True,
            current_quarter_active_deals=0,
            current_quarter_active_arr=0.0,
            forward_quarter_active_deals=len(forward_active),
            forward_quarter_active_arr=forward_arr,
        )
    return PipelineDisplayDecision(
        display_period_role="current_quarter",
        display_quarter_label=period.current_quarter.label,
        display_quarter_title=period.current_quarter.title,
        display_reason="empty_current_and_forward_quarter",
        requires_forward_quarter_fallback=False,
        current_quarter_active_deals=0,
        current_quarter_active_arr=0.0,
        forward_quarter_active_deals=0,
        forward_quarter_active_arr=0.0,
    )


def _period_from_manifest(manifest: MonthlyRunManifest) -> PeriodContext:
    return resolve_period_context(snapshot_date=manifest.snapshot_date)


def _load_source_plan(manifest: MonthlyRunManifest) -> SourceRequirementPlan | None:
    plan_artifact = next(
        (
            artifact
            for artifact in manifest.artifacts
            if artifact.artifact_type == "source_requirement_plan"
        ),
        None,
    )
    if not plan_artifact:
        return None
    return SourceRequirementPlan.model_validate_json(
        Path(plan_artifact.path).read_text(encoding="utf-8")
    )


def _selected_plan_items(
    manifest: MonthlyRunManifest,
    source_plan: SourceRequirementPlan | None,
) -> list[SourcePlanItem]:
    if not source_plan or not manifest.stages:
        return []
    filters = manifest.stages[-1].metadata.get("filters") or {}
    return filter_plan_items(
        source_plan,
        only_requirement=filters.get("only_requirement"),
        only_territory=filters.get("only_territory"),
        max_sources=filters.get("max_sources"),
    )


def _missing_selected_extract_findings(
    selected_items: list[SourcePlanItem],
    extracts: list[SourceExtract],
    *,
    require_complete: bool,
) -> list[Finding]:
    extracted_keys = {
        (
            str(extract.metadata.get("requirement_id") or ""),
            extract.territory,
            extract.period_role,
            extract.source_type,
            extract.source_id,
        )
        for extract in extracts
    }
    findings: list[Finding] = []
    for item in selected_items:
        key = (
            item.requirement_id,
            item.territory,
            item.period_role,
            item.source_type,
            item.source_id,
        )
        if key in extracted_keys:
            continue
        findings.append(
            Finding(
                severity="high" if require_complete else "medium",
                issue="selected_source_extract_missing",
                evidence=(
                    f"{item.requirement_id} {item.territory or 'global'} "
                    f"{item.period_role} {item.source_type} {item.source_id}"
                ),
            )
        )
    return findings


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, int | float):
        if isinstance(value, float) and math.isnan(value):
            return 0.0
        return float(value)
    token = str(value).replace("EUR", "").replace("USD", "").replace("AUD", "").strip()
    token = token.replace(".", "").replace(",", ".") if "," in token else token
    try:
        return float(token)
    except ValueError:
        return 0.0


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def _score_from_row(row: dict[str, Any]) -> int | None:
    value = _first(row, "OpportunityScore.Score", "Score")
    if value not in (None, ""):
        return int(_to_float(value))
    raw = row.get("OpportunityScore")
    if not isinstance(raw, str) or not raw.strip().startswith("{"):
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    score = (
        payload.get("fields", {})
        .get("Score", {})
        .get("value")
    )
    return int(_to_float(score)) if score not in (None, "") else None


def _age_days(snapshot_date: str, created_date: str) -> int:
    if not snapshot_date or not created_date:
        return 0
    try:
        from datetime import date

        return (
            date.fromisoformat(snapshot_date[:10]) - date.fromisoformat(created_date[:10])
        ).days
    except ValueError:
        return 0


def _quarter_label(close_date: str, analysis_year: int) -> str:
    token = str(close_date or "")[:10]
    if len(token) < 7 or not token.startswith(str(analysis_year)):
        return ""
    try:
        month = int(token[5:7])
    except ValueError:
        return ""
    return f"Q{(month - 1) // 3 + 1} {analysis_year}"


def _json_clean(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _slugify(value: str) -> str:
    return "".join(ch if ch.isalnum() else "-" for ch in value.lower()).strip("-")
