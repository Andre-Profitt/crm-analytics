"""Typed data models for the SD Monthly pipeline.

All models are frozen dataclasses. Dates are ISO strings (validated elsewhere).
Amounts are floats. This module has zero I/O — it is pure data + serialization.
"""

from __future__ import annotations

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
