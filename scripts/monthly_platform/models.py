"""Typed data models for the SD Monthly pipeline.

All models are frozen dataclasses. Dates are ISO strings (validated elsewhere).
Amounts are floats. This module has zero I/O — it is pure data + serialization.
"""

from __future__ import annotations

import dataclasses  # noqa: F811
import json
from dataclasses import dataclass


# ── Row models ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PipelineDeal:
    account: str
    opportunity: str
    owner: str
    stage: str
    forecast_category: str
    close_date: str
    arr_unweighted: float
    arr_weighted: float
    probability: float
    push_count: int
    deal_type: str
    lead_scope: str
    industry: str
    tier: str
    sales_region: str
    created_date: str
    last_activity_date: str | None
    next_step: str
    last_modified_date: str
    approved: bool
    approval_date: str | None
    competitor: str
    currency: str
    age_days: int
    quarter: str


@dataclass(frozen=True)
class WonLostDeal:
    account: str
    opportunity: str
    owner: str
    stage: str
    close_date: str
    arr_unweighted: float
    deal_type: str
    industry: str
    sales_region: str
    reason_won_lost: str
    competitor: str
    created_date: str
    currency: str
    age_days: int
    quarter: str


@dataclass(frozen=True)
class RenewalDeal:
    account: str
    opportunity: str
    owner: str
    stage: str
    close_date: str
    acv_unweighted: float
    deal_type: str
    quarter: str
    probability: float
    comments: str


@dataclass(frozen=True)
class ApprovalDeal:
    account: str
    opportunity: str
    owner: str
    stage: str
    close_date: str
    arr_unweighted: float
    status: str
    approval_date: str | None
    next_step: str
    quarter: str
    lead_scope: str


@dataclass(frozen=True)
class PIDeal:
    opportunity: str
    owner: str
    stage: str
    forecast_category: str
    arr_weighted: float
    currency: str
    close_date: str
    push_count: int
    score: int | None
    priority: bool


@dataclass(frozen=True)
class ActivitySignal:
    account: str
    opportunity: str
    owner: str
    tasks_90d: int
    events_90d: int
    total_touches_90d: int
    last_activity_date: str | None
    flag: str


@dataclass(frozen=True)
class CommitItem:
    account: str
    opportunity: str
    owner: str
    forecast_category: str
    arr_weighted: float
    arr_unweighted: float
    close_date: str
    period: str
    stage: str


@dataclass(frozen=True)
class StageEvent:
    opportunity_id: str
    opportunity: str
    account: str
    owner: str
    current_stage: str
    old_value: str
    new_value: str
    created_date: str
    arr_unweighted: float
    is_closed: bool
    is_won: bool


@dataclass(frozen=True)
class ForecastEvent:
    opportunity_id: str
    opportunity: str
    account: str
    owner: str
    current_stage: str
    old_value: str
    new_value: str
    created_date: str
    arr_unweighted: float


@dataclass(frozen=True)
class CloseDateEvent:
    opportunity_id: str
    opportunity: str
    account: str
    owner: str
    current_stage: str
    old_value: str
    new_value: str
    created_date: str
    arr_unweighted: float
    is_closed: bool


@dataclass(frozen=True)
class MovementEvent:
    account: str
    opportunity: str
    owner: str
    stage: str
    movement_type: str
    old_close: str
    new_close: str
    changed_on: str
    arr_unweighted: float


@dataclass(frozen=True)
class TrendSnapshot:
    opportunity: str
    account: str
    close_date: str
    snapshot_date: str
    arr_at_snapshot: float
    stage_at_snapshot: str


# ── Container models ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DatasetSource:
    source_type: str
    source_id: str | None
    query_label: str
    row_count: int
    duration_ms: int


@dataclass(frozen=True)
class SourceContract:
    sf_org: str
    api_version: str
    territory_soql_where: str
    extract_timestamp: str
    sources: dict[str, DatasetSource]


@dataclass(frozen=True)
class Datasets:
    pipeline_open: list[PipelineDeal]
    won_lost: list[WonLostDeal]
    renewals: list[RenewalDeal]
    approvals: list[ApprovalDeal]
    pi_current: list[PIDeal]
    pi_forward: list[PIDeal]
    activity: list[ActivitySignal]
    commit_items: list[CommitItem]
    stage_events: list[StageEvent]
    forecast_category_events: list[ForecastEvent]
    close_date_events: list[CloseDateEvent]
    movement_prior: list[MovementEvent]
    movement_current: list[MovementEvent]
    snapshot_trend: list[TrendSnapshot]


@dataclass(frozen=True)
class DirectorBundle:
    schema_version: str
    snapshot_date: str
    director: str
    territory: str
    corp_ccy: str
    extract_timestamp: str
    source_contract: SourceContract
    dataset_counts: dict[str, int]
    datasets: Datasets

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self), indent=2)

    @classmethod
    def from_json(cls, text: str) -> DirectorBundle:
        return cls.from_dict(json.loads(text))

    @classmethod
    def from_dict(cls, d: dict) -> DirectorBundle:
        sc_raw = d["source_contract"]
        sources = {k: DatasetSource(**v) for k, v in sc_raw["sources"].items()}
        source_contract = SourceContract(
            sf_org=sc_raw["sf_org"],
            api_version=sc_raw["api_version"],
            territory_soql_where=sc_raw["territory_soql_where"],
            extract_timestamp=sc_raw["extract_timestamp"],
            sources=sources,
        )
        ds = d["datasets"]
        datasets = Datasets(
            pipeline_open=[PipelineDeal(**r) for r in ds["pipeline_open"]],
            won_lost=[WonLostDeal(**r) for r in ds["won_lost"]],
            renewals=[RenewalDeal(**r) for r in ds["renewals"]],
            approvals=[ApprovalDeal(**r) for r in ds["approvals"]],
            pi_current=[PIDeal(**r) for r in ds["pi_current"]],
            pi_forward=[PIDeal(**r) for r in ds["pi_forward"]],
            activity=[ActivitySignal(**r) for r in ds["activity"]],
            commit_items=[CommitItem(**r) for r in ds["commit_items"]],
            stage_events=[StageEvent(**r) for r in ds["stage_events"]],
            forecast_category_events=[
                ForecastEvent(**r) for r in ds["forecast_category_events"]
            ],
            close_date_events=[CloseDateEvent(**r) for r in ds["close_date_events"]],
            movement_prior=[MovementEvent(**r) for r in ds["movement_prior"]],
            movement_current=[MovementEvent(**r) for r in ds["movement_current"]],
            snapshot_trend=[TrendSnapshot(**r) for r in ds["snapshot_trend"]],
        )
        return cls(
            schema_version=d["schema_version"],
            snapshot_date=d["snapshot_date"],
            director=d["director"],
            territory=d["territory"],
            corp_ccy=d["corp_ccy"],
            extract_timestamp=d["extract_timestamp"],
            source_contract=source_contract,
            dataset_counts=d["dataset_counts"],
            datasets=datasets,
        )


@dataclass(frozen=True)
class BundleManifestEntry:
    name: str
    territory: str
    status: str
    bundle_path: str
    workbook_path: str
    row_counts: dict[str, int]
    duration_seconds: float
    failure_reason: str | None = None


@dataclass(frozen=True)
class RunManifest:
    schema_version: str
    run_date: str
    started_at: str
    finished_at: str
    directors: list[BundleManifestEntry]
    failures: list[BundleManifestEntry]
    telemetry: dict[str, int | float]
