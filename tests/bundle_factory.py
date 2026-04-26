"""Shared test fixture factory for DirectorBundle."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.monthly_platform.models import (
    ActivitySignal,
    DatasetSource,
    Datasets,
    DirectorBundle,
    PIDeal,
    PipelineDeal,
    SourceContract,
    WonLostDeal,
)


def make_test_bundle(
    director: str = "Jesper Tyrer",
    territory: str = "APAC",
    snapshot_date: str = "2026-04-22",
    pipeline_arr: float = 500000.0,
) -> DirectorBundle:
    """Build a minimal but complete DirectorBundle for testing."""
    return DirectorBundle(
        schema_version="1",
        snapshot_date=snapshot_date,
        director=director,
        territory=territory,
        corp_ccy="EUR",
        extract_timestamp=f"{snapshot_date}T09:30:00Z",
        source_contract=SourceContract(
            sf_org="simcorp.my.salesforce.com",
            api_version="v66.0",
            territory_soql_where="Account_Unit_Group__c = 'SC Asia'",
            extract_timestamp=f"{snapshot_date}T09:30:00Z",
            sources={
                "pipeline_open": DatasetSource(
                    source_type="soql",
                    source_id=None,
                    query_label="APAC:pipeline_open",
                    row_count=1,
                    duration_ms=500,
                ),
                "won_lost": DatasetSource(
                    source_type="soql",
                    source_id=None,
                    query_label="APAC:won_lost",
                    row_count=1,
                    duration_ms=300,
                ),
                "pi_current": DatasetSource(
                    source_type="list_view",
                    source_id="00BTb00000Ksa4bMAB",
                    query_label="APAC:pi",
                    row_count=1,
                    duration_ms=200,
                ),
            },
        ),
        dataset_counts={
            "pipeline_open": 1,
            "won_lost": 1,
            "renewals": 0,
            "approvals": 0,
            "pi_current": 1,
            "pi_forward": 0,
            "activity": 1,
            "commit_items": 0,
            "stage_events": 0,
            "forecast_category_events": 0,
            "close_date_events": 0,
            "movement_prior": 0,
            "movement_current": 0,
            "snapshot_trend": 0,
        },
        datasets=Datasets(
            pipeline_open=[
                PipelineDeal(
                    account="Acme Corp",
                    opportunity="Big Deal",
                    owner="Jane Smith",
                    stage="3 - Engagement",
                    forecast_category="Pipeline",
                    close_date="2026-06-30",
                    arr_unweighted=pipeline_arr,
                    arr_weighted=pipeline_arr * 0.5,
                    probability=50.0,
                    push_count=2,
                    deal_type="Land",
                    lead_scope="Core",
                    industry="Insurance",
                    tier="Tier 1",
                    sales_region="APAC",
                    created_date="2026-01-15",
                    last_activity_date="2026-04-01",
                    next_step="Demo scheduled",
                    last_modified_date="2026-04-10",
                    approved=True,
                    approval_date="2026-03-01",
                    competitor="Competitor X",
                    currency="EUR",
                    age_days=97,
                    quarter="Q2 2026",
                ),
            ],
            won_lost=[
                WonLostDeal(
                    account="Beta Inc",
                    opportunity="Lost Opp",
                    owner="John Doe",
                    stage="8 - Closed Lost",
                    close_date="2026-03-15",
                    arr_unweighted=200000.0,
                    deal_type="Land",
                    industry="Asset Management",
                    sales_region="APAC",
                    reason_won_lost="Price",
                    competitor="Rival Co",
                    created_date="2025-11-01",
                    currency="EUR",
                    age_days=135,
                    quarter="Q1 2026",
                ),
            ],
            renewals=[],
            approvals=[],
            pi_current=[
                PIDeal(
                    opportunity="PI Deal",
                    owner="Jane Smith",
                    stage="4 - Shortlisted",
                    forecast_category="Pipeline",
                    arr_weighted=300000.0,
                    currency="USD",
                    close_date="2026-06-30",
                    push_count=1,
                    score=75,
                    priority=True,
                ),
            ],
            pi_forward=[],
            activity=[
                ActivitySignal(
                    account="Acme Corp",
                    opportunity="Big Deal",
                    owner="Jane Smith",
                    tasks_90d=5,
                    events_90d=3,
                    total_touches_90d=8,
                    last_activity_date="2026-04-01",
                    flag="",
                ),
            ],
            commit_items=[],
            stage_events=[],
            forecast_category_events=[],
            close_date_events=[],
            movement_prior=[],
            movement_current=[],
            snapshot_trend=[],
        ),
    )
