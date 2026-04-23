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


# ── Container model serialization tests ─────────────────────────────────────

import json

from bundle_factory import make_test_bundle
from scripts.monthly_platform.models import (
    DirectorBundle,
    BundleManifestEntry,
    RunManifest,
)


def test_director_bundle_json_round_trip():
    bundle = make_test_bundle()
    json_str = bundle.to_json()
    restored = DirectorBundle.from_json(json_str)
    assert restored == bundle


def test_director_bundle_from_dict():
    bundle = make_test_bundle()
    d = json.loads(bundle.to_json())
    restored = DirectorBundle.from_dict(d)
    assert restored.director == "Jesper Tyrer"
    assert restored.territory == "APAC"
    assert len(restored.datasets.pipeline_open) == 1
    assert restored.datasets.pipeline_open[0].arr_unweighted == 500000.0
    assert (
        restored.source_contract.sources["pi_current"].source_id == "00BTb00000Ksa4bMAB"
    )


def test_dataset_counts_match_actual_lengths():
    bundle = make_test_bundle()
    d = json.loads(bundle.to_json())
    restored = DirectorBundle.from_dict(d)
    assert restored.dataset_counts["pipeline_open"] == len(
        restored.datasets.pipeline_open
    )
    assert restored.dataset_counts["won_lost"] == len(restored.datasets.won_lost)
    assert restored.dataset_counts["pi_current"] == len(restored.datasets.pi_current)


def test_bundle_manifest_entry_round_trip():
    entry = BundleManifestEntry(
        name="Jesper Tyrer",
        territory="APAC",
        status="ok",
        bundle_path="output/director_bundles/2026-04-22/jesper-tyrer.json",
        workbook_path="output/director_live_workbooks/2026-04-22/jesper-tyrer.xlsx",
        row_counts={"pipeline_open": 12, "won_lost": 5},
        duration_seconds=3.2,
        failure_reason=None,
    )
    d = dataclasses.asdict(entry)
    restored = BundleManifestEntry(**d)
    assert restored == entry


def test_run_manifest_round_trip():
    entry = BundleManifestEntry(
        name="Jesper Tyrer",
        territory="APAC",
        status="ok",
        bundle_path="output/director_bundles/2026-04-22/jesper-tyrer.json",
        workbook_path="output/director_live_workbooks/2026-04-22/jesper-tyrer.xlsx",
        row_counts={"pipeline_open": 12},
        duration_seconds=3.2,
        failure_reason=None,
    )
    manifest = RunManifest(
        schema_version="1",
        run_date="2026-04-22",
        started_at="2026-04-22T09:30:00Z",
        finished_at="2026-04-22T09:32:45Z",
        directors=[entry],
        failures=[],
        telemetry={
            "total_queries": 76,
            "total_rows": 24539,
            "total_duration_seconds": 21.9,
        },
    )
    json_str = json.dumps(dataclasses.asdict(manifest), indent=2)
    d = json.loads(json_str)
    restored = RunManifest(
        **{
            **d,
            "directors": [BundleManifestEntry(**e) for e in d["directors"]],
            "failures": [BundleManifestEntry(**e) for e in d["failures"]],
        }
    )
    assert restored == manifest
