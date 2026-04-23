# tests/test_models.py
import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.monthly_platform.models import (
    PipelineDeal,
    WonLostDeal,
    StageEvent,
    MovementEvent,
)


def test_pipeline_deal_frozen_round_trip():
    deal = PipelineDeal(
        account="Acme Corp",
        opportunity="Big Deal",
        owner="Jane Smith",
        stage="3 - Engagement",
        forecast_category="Pipeline",
        close_date="2026-06-30",
        arr_unweighted=500000.0,
        arr_weighted=250000.0,
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
        age_days=98,
        quarter="Q2 2026",
    )
    d = dataclasses.asdict(deal)
    restored = PipelineDeal(**d)
    assert restored == deal
    assert restored.arr_unweighted == 500000.0
    assert restored.approved is True


def test_won_lost_deal_round_trip():
    deal = WonLostDeal(
        account="Beta Inc",
        opportunity="Lost Opp",
        owner="John Doe",
        stage="8 - Closed Lost",
        close_date="2026-03-15",
        arr_unweighted=200000.0,
        deal_type="Land",
        industry="Asset Management",
        sales_region="Central Europe",
        reason_won_lost="Price",
        competitor="Rival Co",
        created_date="2025-11-01",
        currency="EUR",
        age_days=135,
        quarter="Q1 2026",
    )
    assert WonLostDeal(**dataclasses.asdict(deal)) == deal


def test_movement_event_round_trip():
    event = MovementEvent(
        account="Acme Corp",
        opportunity="Big Deal",
        owner="Jane Smith",
        stage="4 - Shortlisted",
        movement_type="Q1 Slipped",
        old_close="2026-03-15",
        new_close="2026-06-30",
        changed_on="2026-03-20",
        arr_unweighted=500000.0,
    )
    assert MovementEvent(**dataclasses.asdict(event)) == event


def test_stage_event_round_trip():
    event = StageEvent(
        opportunity_id="006xxx",
        opportunity="Big Deal",
        account="Acme Corp",
        owner="Jane Smith",
        current_stage="4 - Shortlisted",
        old_value="3 - Engagement",
        new_value="4 - Shortlisted",
        created_date="2026-03-10",
        arr_unweighted=500000.0,
        is_closed=False,
        is_won=False,
    )
    assert StageEvent(**dataclasses.asdict(event)) == event


def test_all_models_are_frozen():
    import pytest

    deal = PipelineDeal(
        account="A",
        opportunity="B",
        owner="C",
        stage="1 - Prospecting",
        forecast_category="Pipeline",
        close_date="2026-01-01",
        arr_unweighted=0,
        arr_weighted=0,
        probability=0,
        push_count=0,
        deal_type="Land",
        lead_scope="",
        industry="",
        tier="",
        sales_region="",
        created_date="2026-01-01",
        last_activity_date=None,
        next_step="",
        last_modified_date="2026-01-01",
        approved=False,
        approval_date=None,
        competitor="",
        currency="EUR",
        age_days=0,
        quarter="Q1 2026",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        deal.account = "Changed"
