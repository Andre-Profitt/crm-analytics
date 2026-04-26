# Semantic Data Layer Phase 1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add typed Python models, JSON bundle serialization, and an Excel renderer so the extract produces both machine-readable JSON bundles and human-readable Excel workbooks from a shared data layer.

**Architecture:** `extract_territory()` builds frozen dataclasses from SOQL responses, assembles a `DirectorBundle`, writes it as JSON, and calls `render_bundle_to_excel()` for the Excel artifact. The old audit artifact continues to be written for backward compat. No consumers are changed.

**Tech Stack:** Python 3.12+ dataclasses (frozen), `json` stdlib, `openpyxl` (existing dep), `pytest`

**Spec:** `docs/superpowers/specs/2026-04-22-semantic-data-layer-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `scripts/monthly_platform/models.py` | Create | All 18 frozen dataclasses + `DirectorBundle.to_json/from_json/from_dict` |
| `scripts/monthly_platform/excel_renderer.py` | Create | `render_bundle_to_excel()` — takes DirectorBundle, produces Excel workbook |
| `scripts/monthly_platform/bundle_validation.py` | Create | `validate_bundle()` — returns list of validation errors |
| `scripts/extract_director_live.py` | Modify | Build typed models, assemble bundle, write JSON, call renderer, add timing |
| `tests/bundle_factory.py` | Create | `make_test_bundle()` shared fixture factory |
| `tests/test_models.py` | Create | Round-trip serialization tests |
| `tests/test_bundle_validation.py` | Create | Validation rule tests |
| `tests/test_excel_renderer.py` | Create | Renderer output tests |
| `tests/test_extract_director_live_period.py` | Modify | Add JSON bundle integration test |

### Notes on spec extensions

The spec defines minimal models. Several need extra fields to produce identical Excel output in Phase 1. Extensions marked with `# BWC:` (backward-compat) comments:

- `RenewalDeal`: +`probability`, +`comments` (current sheet has both)
- `ApprovalDeal`: +`lead_scope` (current 10th column; spec has `quarter` instead)
- `CommitItem`: +`account`, +`arr_unweighted`, +`stage` (current sheet has 9 cols, spec has 6)
- `StageEvent`, `ForecastEvent`, `CloseDateEvent`: +`current_stage` (current "Stage (live)" column)

---

## Task 1: Row Model Dataclasses

**Files:**
- Create: `scripts/monthly_platform/models.py`
- Create: `tests/test_models.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_models.py
import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.monthly_platform.models import (
    PipelineDeal,
    WonLostDeal,
    RenewalDeal,
    ApprovalDeal,
    PIDeal,
    ActivitySignal,
    CommitItem,
    StageEvent,
    ForecastEvent,
    CloseDateEvent,
    MovementEvent,
    TrendSnapshot,
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
    """Every row model must be frozen (immutable)."""
    import pytest

    deal = PipelineDeal(
        account="A", opportunity="B", owner="C", stage="1 - Prospecting",
        forecast_category="Pipeline", close_date="2026-01-01",
        arr_unweighted=0, arr_weighted=0, probability=0, push_count=0,
        deal_type="Land", lead_scope="", industry="", tier="", sales_region="",
        created_date="2026-01-01", last_activity_date=None, next_step="",
        last_modified_date="2026-01-01", approved=False, approval_date=None,
        competitor="", currency="EUR", age_days=0, quarter="Q1 2026",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        deal.account = "Changed"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.monthly_platform.models'`

- [ ] **Step 3: Create models.py with all row dataclasses**

```python
# scripts/monthly_platform/models.py
"""Typed data models for the SD Monthly pipeline.

All models are frozen dataclasses. Dates are ISO strings (validated elsewhere).
Amounts are floats. This module has zero I/O — it is pure data + serialization.
"""
from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field


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
    probability: float  # BWC: current sheet has Probability %
    comments: str  # BWC: current sheet has Comments (always empty, for director use)


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
    lead_scope: str  # BWC: current 10th column


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
    account: str  # BWC: current sheet col 1
    opportunity: str
    owner: str
    forecast_category: str
    arr_weighted: float
    arr_unweighted: float  # BWC: current sheet col 6
    close_date: str
    period: str
    stage: str  # BWC: current sheet col 9


@dataclass(frozen=True)
class StageEvent:
    opportunity_id: str
    opportunity: str
    account: str
    owner: str
    current_stage: str  # BWC: "Stage (live)" column
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
    current_stage: str  # BWC: "Stage (live)" column
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
    current_stage: str  # BWC: for future sheet use
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_models.py -v`
Expected: 5 PASSED

- [ ] **Step 5: Commit**

```bash
cd /Users/test/crm-analytics
git add scripts/monthly_platform/models.py tests/test_models.py
git commit -m "feat: add row model dataclasses for semantic data layer"
```

---

## Task 2: Container Models + Serialization

**Files:**
- Modify: `scripts/monthly_platform/models.py` (append container models)
- Create: `tests/bundle_factory.py`
- Modify: `tests/test_models.py` (add serialization tests)

- [ ] **Step 1: Write the failing tests**

First, create the shared fixture factory:

```python
# tests/bundle_factory.py
"""Shared test fixture factory for DirectorBundle."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.monthly_platform.models import (
    ActivitySignal,
    ApprovalDeal,
    BundleManifestEntry,
    CloseDateEvent,
    CommitItem,
    DatasetSource,
    Datasets,
    DirectorBundle,
    ForecastEvent,
    MovementEvent,
    PIDeal,
    PipelineDeal,
    RenewalDeal,
    RunManifest,
    SourceContract,
    StageEvent,
    TrendSnapshot,
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
```

Then add the serialization tests:

```python
# Append to tests/test_models.py

from tests.bundle_factory import make_test_bundle
from scripts.monthly_platform.models import (
    DatasetSource,
    SourceContract,
    Datasets,
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
    assert restored.source_contract.sources["pi_current"].source_id == "00BTb00000Ksa4bMAB"


def test_dataset_counts_match_actual_lengths():
    bundle = make_test_bundle()
    d = json.loads(bundle.to_json())
    restored = DirectorBundle.from_dict(d)
    assert restored.dataset_counts["pipeline_open"] == len(restored.datasets.pipeline_open)
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
        telemetry={"total_queries": 76, "total_rows": 24539, "total_duration_seconds": 21.9},
    )
    json_str = json.dumps(dataclasses.asdict(manifest), indent=2)
    d = json.loads(json_str)
    restored = RunManifest(**{
        **d,
        "directors": [BundleManifestEntry(**e) for e in d["directors"]],
        "failures": [BundleManifestEntry(**e) for e in d["failures"]],
    })
    assert restored == manifest
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_models.py -v -k "bundle or manifest or dataset_counts"`
Expected: FAIL — `ImportError: cannot import name 'DatasetSource'`

- [ ] **Step 3: Add container models + serialization to models.py**

Append to `scripts/monthly_platform/models.py`:

```python
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
            forecast_category_events=[ForecastEvent(**r) for r in ds["forecast_category_events"]],
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_models.py -v`
Expected: ALL PASSED (10 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/test/crm-analytics
git add scripts/monthly_platform/models.py tests/test_models.py tests/bundle_factory.py
git commit -m "feat: add container models and DirectorBundle serialization"
```

---

## Task 3: Bundle Validation

**Files:**
- Create: `scripts/monthly_platform/bundle_validation.py`
- Create: `tests/test_bundle_validation.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_bundle_validation.py
import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.monthly_platform.bundle_validation import validate_bundle
from scripts.monthly_platform.models import PipelineDeal
from tests.bundle_factory import make_test_bundle


def test_valid_bundle_has_no_errors():
    bundle = make_test_bundle()
    errors = validate_bundle(bundle)
    assert errors == []


def test_catches_negative_pipeline_arr():
    bundle = make_test_bundle(pipeline_arr=-100.0)
    errors = validate_bundle(bundle)
    assert any("negative" in e.lower() and "arr" in e.lower() for e in errors)


def test_catches_mismatched_dataset_counts():
    bundle = make_test_bundle()
    bad_counts = {**bundle.dataset_counts, "pipeline_open": 999}
    bundle = dataclasses.replace(bundle, dataset_counts=bad_counts)
    errors = validate_bundle(bundle)
    assert any("pipeline_open" in e and "count" in e.lower() for e in errors)


def test_catches_invalid_close_date_format():
    bundle = make_test_bundle()
    bad_deal = dataclasses.replace(
        bundle.datasets.pipeline_open[0], close_date="June 30 2026"
    )
    bad_datasets = dataclasses.replace(bundle.datasets, pipeline_open=[bad_deal])
    bundle = dataclasses.replace(bundle, datasets=bad_datasets)
    errors = validate_bundle(bundle)
    assert any("date" in e.lower() for e in errors)


def test_catches_invalid_stage_name():
    bundle = make_test_bundle()
    bad_deal = dataclasses.replace(
        bundle.datasets.pipeline_open[0], stage="Invalid Stage"
    )
    bad_datasets = dataclasses.replace(bundle.datasets, pipeline_open=[bad_deal])
    bundle = dataclasses.replace(bundle, datasets=bad_datasets)
    errors = validate_bundle(bundle)
    assert any("stage" in e.lower() for e in errors)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_bundle_validation.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create bundle_validation.py**

```python
# scripts/monthly_platform/bundle_validation.py
"""Validation rules for DirectorBundle. Run after extract, before consumers."""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import DirectorBundle

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_VALID_STAGES = {
    "1 - Prospecting",
    "2 - Discovery",
    "3 - Engagement",
    "4 - Shortlisted",
    "5 - Preferred",
    "6 - Contracting",
    "7 - Closed Won",
    "8 - Closed Lost",
    "9 - Closed Opt Out",
}

_VALID_FORECAST_CATEGORIES = {
    "Omitted",
    "Pipeline",
    "Best Case",
    "Commit",
    "Closed",
}


def validate_bundle(bundle: DirectorBundle) -> list[str]:
    errors: list[str] = []

    # Dataset count consistency
    ds = bundle.datasets
    actual = {
        "pipeline_open": len(ds.pipeline_open),
        "won_lost": len(ds.won_lost),
        "renewals": len(ds.renewals),
        "approvals": len(ds.approvals),
        "pi_current": len(ds.pi_current),
        "pi_forward": len(ds.pi_forward),
        "activity": len(ds.activity),
        "commit_items": len(ds.commit_items),
        "stage_events": len(ds.stage_events),
        "forecast_category_events": len(ds.forecast_category_events),
        "close_date_events": len(ds.close_date_events),
        "movement_prior": len(ds.movement_prior),
        "movement_current": len(ds.movement_current),
        "snapshot_trend": len(ds.snapshot_trend),
    }
    for key, count in actual.items():
        declared = bundle.dataset_counts.get(key, -1)
        if declared != count:
            errors.append(
                f"dataset_counts['{key}'] = {declared} but actual count = {count}"
            )

    # Pipeline deals
    for i, d in enumerate(ds.pipeline_open):
        if d.arr_unweighted < 0:
            errors.append(f"pipeline_open[{i}]: negative arr_unweighted ({d.arr_unweighted})")
        if d.close_date and not _ISO_DATE_RE.match(d.close_date):
            errors.append(f"pipeline_open[{i}]: invalid date format '{d.close_date}'")
        if d.stage and d.stage not in _VALID_STAGES:
            errors.append(f"pipeline_open[{i}]: invalid stage '{d.stage}'")
        if d.forecast_category and d.forecast_category not in _VALID_FORECAST_CATEGORIES:
            errors.append(f"pipeline_open[{i}]: invalid forecast_category '{d.forecast_category}'")

    # Won/Lost deals
    for i, d in enumerate(ds.won_lost):
        if d.arr_unweighted < 0:
            errors.append(f"won_lost[{i}]: negative arr_unweighted ({d.arr_unweighted})")
        if d.close_date and not _ISO_DATE_RE.match(d.close_date):
            errors.append(f"won_lost[{i}]: invalid date format '{d.close_date}'")

    # Renewals
    for i, d in enumerate(ds.renewals):
        if d.acv_unweighted < 0:
            errors.append(f"renewals[{i}]: negative acv_unweighted ({d.acv_unweighted})")

    return errors
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_bundle_validation.py -v`
Expected: ALL PASSED (5 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/test/crm-analytics
git add scripts/monthly_platform/bundle_validation.py tests/test_bundle_validation.py
git commit -m "feat: add bundle validation rules"
```

---

## Task 4: Excel Renderer

**Files:**
- Create: `scripts/monthly_platform/excel_renderer.py`
- Create: `tests/test_excel_renderer.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_excel_renderer.py
import sys
from pathlib import Path

from openpyxl import load_workbook

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.monthly_platform.excel_renderer import render_bundle_to_excel
from tests.bundle_factory import make_test_bundle


def test_render_produces_all_expected_sheets(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    assert "Summary" in wb.sheetnames
    assert "Pipeline Open FY26" in wb.sheetnames
    assert "Won Lost FY26" in wb.sheetnames
    assert "Pipeline Inspection" in wb.sheetnames
    assert "Activity Volume" in wb.sheetnames


def test_summary_is_first_sheet(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    assert wb.sheetnames[0] == "Summary"


def test_pipeline_headers_match(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Pipeline Open FY26"]
    headers = [ws.cell(1, c).value for c in range(1, 23)]
    assert headers[0] == "Account"
    assert headers[1] == "Opportunity"
    assert headers[6] == "ARR Unweighted (EUR)"
    assert headers[7] == "ARR Weighted (EUR)"
    assert headers[21] == "Competitor"


def test_pipeline_data_row(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Pipeline Open FY26"]
    assert ws.cell(2, 1).value == "Acme Corp"
    assert ws.cell(2, 7).value == 500000.0
    assert ws.cell(2, 20).value == "Yes"  # approved bool -> "Yes"


def test_freeze_panes_set(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Pipeline Open FY26"]
    assert ws.freeze_panes == "A2"


def test_eur_formatting_applied(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Pipeline Open FY26"]
    assert ws.cell(2, 7).number_format == "#,##0"


def test_empty_datasets_produce_no_rows(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Renewals FY26"]
    assert ws.cell(1, 1).value is not None  # headers exist
    assert ws.cell(2, 1).value is None  # no data rows
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_excel_renderer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create excel_renderer.py**

```python
# scripts/monthly_platform/excel_renderer.py
"""Render a DirectorBundle to an Excel workbook.

Pure function: DirectorBundle in, Excel workbook out.
No Salesforce calls. No territory resolution. No sidecar logic.
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

if TYPE_CHECKING:
    from .models import DirectorBundle

HEADER_FILL = PatternFill(start_color="083EA7", end_color="083EA7", fill_type="solid")
HEADER_FONT = Font(bold=True, color="FFFFFF", size=10)
DATA_FONT = Font(size=9)
EUR_FMT = "#,##0"


def _add_sheet(wb, name, headers, rows, eur_cols=None):
    ws = wb.create_sheet(title=name[:31])
    for ci, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=ci, value=h)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center")
    for ri, row in enumerate(rows, 2):
        for ci, val in enumerate(row, 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.font = DATA_FONT
            if eur_cols and ci in eur_cols and isinstance(val, (int, float)):
                cell.number_format = EUR_FMT
    for ci in range(1, len(headers) + 1):
        col_letter = get_column_letter(ci)
        max_len = max(
            len(str(headers[ci - 1])),
            *(len(str(r[ci - 1])) for r in rows[:50]) if rows else [0],
        )
        ws.column_dimensions[col_letter].width = min(max_len + 3, 40)
    if rows:
        end_col = get_column_letter(len(headers))
        table_name = (
            name.replace(" ", "_")
            .replace("-", "_")
            .replace("&", "And")
            .replace("/", "")[:30]
        )
        try:
            table = Table(displayName=table_name, ref=f"A1:{end_col}{len(rows) + 1}")
            table.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium2",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=True,
            )
            ws.add_table(table)
        except Exception:
            pass
    ws.freeze_panes = "A2"


def render_bundle_to_excel(bundle: DirectorBundle, output_path: Path) -> None:
    from .models import DirectorBundle as _DB  # resolve TYPE_CHECKING

    analysis_year = int(bundle.snapshot_date[:4])
    fy = f"FY{analysis_year % 100:02d}"
    ccy = bundle.corp_ccy
    ds = bundle.datasets

    wb = Workbook()
    wb.remove(wb.active)

    # ── Pipeline Open ──
    _add_sheet(
        wb,
        f"Pipeline Open {fy}",
        [
            "Account", "Opportunity", "Owner", "Stage", "Forecast Category",
            "Close Date", f"ARR Unweighted ({ccy})", f"ARR Weighted ({ccy})",
            "Probability %", "Push Count", "Type", "Lead Scope", "Industry",
            "Tier", "Sales Region", "Created", "Last Activity", "Next Step",
            "Last Modified", "Approved", "Approval Date", "Competitor",
        ],
        [
            [
                d.account, d.opportunity, d.owner, d.stage, d.forecast_category,
                d.close_date, d.arr_unweighted, d.arr_weighted,
                d.probability, d.push_count, d.deal_type, d.lead_scope,
                d.industry, d.tier, d.sales_region, d.created_date,
                d.last_activity_date or "", d.next_step, d.last_modified_date,
                "Yes" if d.approved else "No", d.approval_date or "", d.competitor,
            ]
            for d in ds.pipeline_open
        ],
        eur_cols=[7, 8],
    )

    # ── Won Lost ──
    _add_sheet(
        wb,
        f"Won Lost {fy}",
        [
            "Account", "Opportunity", "Owner", "Stage", "Close Date",
            f"ARR Unweighted ({ccy})", "Type", "Reason", "Lost To Competitor",
            "Industry", "Sales Region", "Created",
        ],
        [
            [
                d.account, d.opportunity, d.owner, d.stage, d.close_date,
                d.arr_unweighted, d.deal_type, d.reason_won_lost, d.competitor,
                d.industry, d.sales_region, d.created_date,
            ]
            for d in ds.won_lost
        ],
        eur_cols=[6],
    )

    # ── Commercial Approval ──
    _add_sheet(
        wb,
        "Commercial Approval",
        [
            "Account", "Opportunity", "Owner", "Stage", "Close Date",
            f"ARR Unweighted ({ccy})", "Status", "Approval Date",
            "Next Step", "Lead Scope",
        ],
        [
            [
                d.account, d.opportunity, d.owner, d.stage, d.close_date,
                d.arr_unweighted, d.status, d.approval_date or "",
                d.next_step, d.lead_scope,
            ]
            for d in ds.approvals
        ],
        eur_cols=[6],
    )

    # ── Renewals ──
    _add_sheet(
        wb,
        f"Renewals {fy}",
        [
            "Close Date", "Account", "Opportunity", "Owner", "Stage",
            f"ACV Unweighted ({ccy})", "Probability %", "Comments",
        ],
        [
            [
                d.close_date, d.account, d.opportunity, d.owner, d.stage,
                d.acv_unweighted, d.probability, d.comments,
            ]
            for d in ds.renewals
        ],
        eur_cols=[6],
    )

    # ── Pipeline Inspection ──
    pi_headers = [
        "Opportunity", "Owner", "Stage", "Forecast Category",
        "ARR Weighted (native ccy)", "Currency", "Close Date",
        "Push Count", "Score", "Priority",
    ]
    _add_sheet(
        wb,
        "Pipeline Inspection",
        pi_headers,
        [
            [
                d.opportunity, d.owner, d.stage, d.forecast_category,
                d.arr_weighted, d.currency, d.close_date, d.push_count,
                d.score, "Yes" if d.priority else "",
            ]
            for d in ds.pi_current
        ],
        eur_cols=[5],
    )

    # ── Pipeline Inspection Forward ──
    if ds.pi_forward:
        _add_sheet(
            wb,
            "Pipeline Inspection Forward",
            pi_headers,
            [
                [
                    d.opportunity, d.owner, d.stage, d.forecast_category,
                    d.arr_weighted, d.currency, d.close_date, d.push_count,
                    d.score, "Yes" if d.priority else "",
                ]
                for d in ds.pi_forward
            ],
            eur_cols=[5],
        )

    # ── Activity Volume ──
    activity_sorted = sorted(
        ds.activity, key=lambda a: (a.total_touches_90d, a.last_activity_date or "")
    )
    _add_sheet(
        wb,
        "Activity Volume",
        [
            "Account", "Opportunity", "Owner", "Tasks 90d", "Events 90d",
            "Total Touches 90d", "Last Activity", "Flag",
        ],
        [
            [
                a.account, a.opportunity, a.owner, a.tasks_90d, a.events_90d,
                a.total_touches_90d, a.last_activity_date or "", a.flag,
            ]
            for a in activity_sorted
        ],
    )

    # ── Commit Items ──
    commit_sorted = sorted(ds.commit_items, key=lambda c: -(c.arr_weighted or 0))
    _add_sheet(
        wb,
        "Commit Items",
        [
            "Account", "Opportunity", "Owner", "Forecast Category",
            f"Forecast ARR Wtd ({ccy})", f"ARR Unwtd ({ccy})",
            "Close Date", "Period", "Stage",
        ],
        [
            [
                c.account, c.opportunity, c.owner, c.forecast_category,
                c.arr_weighted, c.arr_unweighted, c.close_date, c.period, c.stage,
            ]
            for c in commit_sorted
        ],
        eur_cols=[5, 6],
    )

    # ── Q1 Movement ──
    movement_headers = [
        "Account", "Opportunity", "Owner", "Stage", "Movement",
        "Old Close", "New Close", "Changed On", f"ARR Unweighted ({ccy})",
    ]
    q1_sorted = sorted(ds.movement_prior, key=lambda m: -(m.arr_unweighted or 0))
    _add_sheet(
        wb,
        "Q1 Movement",
        movement_headers,
        [
            [
                m.account, m.opportunity, m.owner, m.stage, m.movement_type,
                m.old_close, m.new_close, m.changed_on, m.arr_unweighted,
            ]
            for m in q1_sorted
        ],
        eur_cols=[9],
    )

    # ── Q2 Movement ──
    q2_sorted = sorted(ds.movement_current, key=lambda m: -(m.arr_unweighted or 0))
    _add_sheet(
        wb,
        "Q2 Movement",
        movement_headers,
        [
            [
                m.account, m.opportunity, m.owner, m.stage, m.movement_type,
                m.old_close, m.new_close, m.changed_on, m.arr_unweighted,
            ]
            for m in q2_sorted
        ],
        eur_cols=[9],
    )

    # ── Stage History ──
    stage_sorted = sorted(ds.stage_events, key=lambda s: s.created_date, reverse=True)
    _add_sheet(
        wb,
        "Stage History",
        [
            "Account", "Opportunity", "Owner", "Stage (live)",
            "From Stage", "To Stage", "Changed On", f"ARR Unweighted ({ccy})",
        ],
        [
            [
                s.account, s.opportunity, s.owner, s.current_stage,
                s.old_value, s.new_value, s.created_date, s.arr_unweighted,
            ]
            for s in stage_sorted
        ],
        eur_cols=[8],
    )

    # ── Forecast Category History ──
    fcat_sorted = sorted(
        ds.forecast_category_events, key=lambda f: f.created_date, reverse=True
    )
    _add_sheet(
        wb,
        "Forecast Category History",
        [
            "Account", "Opportunity", "Owner", "Stage (live)",
            "From Category", "To Category", "Changed On",
            f"ARR Unweighted ({ccy})",
        ],
        [
            [
                f.account, f.opportunity, f.owner, f.current_stage,
                f.old_value, f.new_value, f.created_date, f.arr_unweighted,
            ]
            for f in fcat_sorted
        ],
        eur_cols=[8],
    )

    # ── Summary (first tab, built last) ──
    _build_summary(wb, bundle, fy, ccy, analysis_year)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(output_path))


def _build_summary(wb, bundle, fy, ccy, analysis_year):
    from .models import DirectorBundle as _DB

    ds = bundle.datasets
    ws = wb.create_sheet(title="Summary", index=0)

    ws["A1"] = f"{bundle.director} ({bundle.territory})"
    ws["A1"].font = Font(bold=True, size=14, color="083EA7")
    ws["A2"] = f"Reporting period: {fy} (Q1-Q4)"
    ws["A2"].font = Font(size=10, color="666666")
    ws["A3"] = f"Snapshot date: {bundle.snapshot_date} — live pull from Salesforce"
    ws["A3"].font = Font(size=10, color="666666")
    ws["A4"] = (
        "Methodology: Alex P — ARR Unweighted = APTS_Opportunity_ARR__c (full deal value); "
        "ARR Weighted = APTS_Forecast_ARR__c (probability-weighted). "
        "Excl simcorp/test/delete accounts, excl Sabiniewicz/Profit owners."
    )
    ws["A4"].font = Font(size=8, italic=True, color="999999")

    total_pipeline_arr = sum(d.arr_unweighted for d in ds.pipeline_open)
    won = [d for d in ds.won_lost if "Won" in d.stage]
    lost = [d for d in ds.won_lost if "Lost" in d.stage or "Opt Out" in d.stage]
    won_arr = sum(d.arr_unweighted for d in won)
    lost_arr = sum(d.arr_unweighted for d in lost)
    renewal_acv = sum(d.acv_unweighted for d in ds.renewals)

    approved_current = [d for d in ds.approvals if d.status.startswith(f"Approved {analysis_year}")]
    approved_prior = [d for d in ds.approvals if d.status == "Approved (prior year)"]
    pending = [d for d in ds.approvals if d.status == "Pending Approval"]
    missing = [d for d in ds.approvals if d.status == "Missing (Stage 3+)"]

    ws["A6"] = "KPI"
    ws["B6"] = "Value"
    ws["A6"].font = HEADER_FONT
    ws["A6"].fill = HEADER_FILL
    ws["B6"].font = HEADER_FONT
    ws["B6"].fill = HEADER_FILL

    kpis = [
        ("Open Pipeline Unweighted (stages 1-6)", f"{ccy} {total_pipeline_arr:,.0f}"),
        ("Open Deal Count", str(len(ds.pipeline_open))),
        (f"Won ARR Unweighted {fy}", f"{ccy} {won_arr:,.0f}"),
        ("Won Deal Count", str(len(won))),
        (f"Lost ARR Unweighted {fy}", f"{ccy} {lost_arr:,.0f}"),
        ("Lost Deal Count", str(len(lost))),
        (f"Approved {analysis_year} (Land)", str(len(approved_current))),
        ("Approved Prior Year", str(len(approved_prior))),
        ("Pending Approval", str(len(pending))),
        ("Missing Approval (Stage 3+)", str(len(missing))),
        ("Open Renewal ACV Unweighted", f"{ccy} {renewal_acv:,.0f}"),
        ("Open Renewals", str(len(ds.renewals))),
        (f"PI Open Deals ({fy})", str(len(ds.pi_current))),
    ]
    if ds.pi_forward:
        kpis.append((f"PI Forward Deals", str(len(ds.pi_forward))))

    for i, (label, val) in enumerate(kpis, 7):
        ws[f"A{i}"] = label
        ws[f"B{i}"] = val

    sheet_row = len(kpis) + 8
    ws[f"A{sheet_row}"] = "Sheet"
    ws[f"B{sheet_row}"] = "Records"
    ws[f"C{sheet_row}"] = "Source"
    for col in ("A", "B", "C"):
        ws[f"{col}{sheet_row}"].font = HEADER_FONT
        ws[f"{col}{sheet_row}"].fill = HEADER_FILL

    sheets_info = [
        (f"Pipeline Open {fy}", len(ds.pipeline_open), f"SOQL — open, stages 1-6, {fy}"),
        (f"Won Lost {fy}", len(ds.won_lost), f"SOQL — closed, stages 0/7/8, {fy}"),
        ("Commercial Approval", len(ds.approvals), f"SOQL — open Land, {fy}"),
        (f"Renewals {fy}", len(ds.renewals), f"SOQL — open Renewal, {fy}"),
        ("Pipeline Inspection", len(ds.pi_current), f"PI list view — broad coaching population, {fy}"),
    ]
    if ds.pi_forward:
        sheets_info.append(("Pipeline Inspection Forward", len(ds.pi_forward), "PI list view — forward quarter"))

    for i, (sname, count, source) in enumerate(sheets_info, sheet_row + 1):
        ws[f"A{i}"] = sname
        ws[f"B{i}"] = count
        ws[f"C{i}"] = source

    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 20
    ws.column_dimensions["C"].width = 40
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_excel_renderer.py -v`
Expected: ALL PASSED (7 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/test/crm-analytics
git add scripts/monthly_platform/excel_renderer.py tests/test_excel_renderer.py
git commit -m "feat: add Excel renderer for DirectorBundle"
```

---

## Task 5: Extract Refactor

This is the core integration task. Modify `extract_territory()` to build typed models, assemble a `DirectorBundle`, write the JSON bundle, and call the renderer for Excel output. The SOQL queries and field mapping are unchanged — only the output pipeline changes.

**Files:**
- Modify: `scripts/extract_director_live.py:1-100` (imports), `:491-530` (remove `_add_sheet`), `:87-91` (remove styling constants), `:533-1496` (refactor `extract_territory`)
- Modify: `tests/test_extract_director_live_period.py` (add integration test)

- [ ] **Step 1: Write the failing integration test**

Add to `tests/test_extract_director_live_period.py`:

```python
from scripts.monthly_platform.models import DirectorBundle


def test_extract_territory_writes_json_bundle(tmp_path, monkeypatch):
    """Phase 1 integration: extract produces both JSON bundle and Excel workbook."""
    monkeypatch.setattr(extract_live, "TERRITORIES", {
        "APAC": {
            "director": "Jesper Tyrer",
            "soql_where": "Account_Unit_Group__c = 'SC Asia'",
            "pi_list_view_id": "00BTb00000Ksa4bMAB",
            "forward_quarter_pi_list_views": {},
        },
    })
    monkeypatch.setattr(
        extract_live, "BUNDLE_OUTPUT_ROOT", tmp_path / "director_bundles"
    )

    calls = []

    def fake_run_soql(session, instance_url, query, label=""):
        calls.append(label)
        if "all_fy_deals" in label:
            return [_make_opportunity(
                opp_id="006TEST",
                name="Test Opp",
                stage="3 - Engagement",
                close_date="2026-06-15",
                approval_status="",
                approved=False,
            )]
        if "renewals" in label:
            return []
        if "field_history" in label:
            return []
        if "tasks_90d" in label or "events_90d" in label:
            return []
        return []

    def fake_fetch_pi(session, instance_url, lv_id, label=""):
        return []

    monkeypatch.setattr(extract_live, "run_soql", fake_run_soql)
    monkeypatch.setattr(extract_live, "fetch_pi", fake_fetch_pi)
    monkeypatch.setattr(extract_live, "get_auth", lambda: ("token", "https://test.my.salesforce.com"))
    monkeypatch.setattr(extract_live, "build_session", lambda t: None)

    workbook_path = tmp_path / "2026-04-22" / "jesper-tyrer.xlsx"
    result = extract_live.extract_territory(
        "APAC", "2026-04-22", workbook_path,
        session=None, instance_url="https://test.my.salesforce.com",
    )

    # JSON bundle written
    bundle_path = tmp_path / "director_bundles" / "2026-04-22" / "jesper-tyrer.json"
    assert bundle_path.exists(), f"Expected bundle at {bundle_path}"

    # Round-trip the bundle
    bundle = DirectorBundle.from_json(bundle_path.read_text())
    assert bundle.director == "Jesper Tyrer"
    assert bundle.territory == "APAC"
    assert bundle.schema_version == "1"
    assert len(bundle.datasets.pipeline_open) == 1
    assert bundle.datasets.pipeline_open[0].opportunity == "Test Opp"
    assert bundle.dataset_counts["pipeline_open"] == 1

    # Workbook also written
    assert workbook_path.exists()

    # Return dict still has expected keys (backward compat)
    assert result["territory"] == "APAC"
    assert result["director"] == "Jesper Tyrer"
    assert "bundle_path" in result
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_extract_director_live_period.py::test_extract_territory_writes_json_bundle -v`
Expected: FAIL — `AttributeError: module has no attribute 'BUNDLE_OUTPUT_ROOT'`

- [ ] **Step 3: Modify extract_director_live.py imports and constants**

At the top of `scripts/extract_director_live.py`, make these changes:

1. **Add BUNDLE_OUTPUT_ROOT constant** (after line 41):
```python
BUNDLE_OUTPUT_ROOT = REPO_ROOT / "output" / "director_bundles"
```

2. **Add model imports** (after the `monthly_platform` import block, ~line 36):
```python
try:
    from monthly_platform.models import (
        ActivitySignal, ApprovalDeal, CloseDateEvent, CommitItem,
        DatasetSource, Datasets, DirectorBundle, ForecastEvent,
        MovementEvent, PIDeal, PipelineDeal, RenewalDeal,
        SourceContract, StageEvent, WonLostDeal,
    )
    from monthly_platform.excel_renderer import render_bundle_to_excel
except ModuleNotFoundError:  # pragma: no cover
    from scripts.monthly_platform.models import (
        ActivitySignal, ApprovalDeal, CloseDateEvent, CommitItem,
        DatasetSource, Datasets, DirectorBundle, ForecastEvent,
        MovementEvent, PIDeal, PipelineDeal, RenewalDeal,
        SourceContract, StageEvent, WonLostDeal,
    )
    from scripts.monthly_platform.excel_renderer import render_bundle_to_excel
```

3. **Remove styling constants** (lines 87-91) — they now live in `excel_renderer.py`.

4. **Remove `_add_sheet` function** (lines 491-530) — it now lives in `excel_renderer.py`.

5. **Remove openpyxl imports** (lines 27-30) — no longer needed in the extract. Keep the `Font` import only if the Summary sheet building remains here (it doesn't — it moves to the renderer). Remove all openpyxl imports:
```python
# DELETE these lines:
# from openpyxl import Workbook
# from openpyxl.styles import Alignment, Font, PatternFill
# from openpyxl.utils import get_column_letter
# from openpyxl.worksheet.table import Table, TableStyleInfo
```

- [ ] **Step 4: Refactor extract_territory() to build models and bundles**

Replace the body of `extract_territory()` (lines 540-1496) with the refactored version. The SOQL queries and field mapping stay identical — the changes are in what we do with the data after querying.

**Key structural changes:**

After each SOQL section, build typed model instances instead of tuple rows. The pattern for pipeline deals (replacing lines 640-669):

```python
    # Build typed pipeline models (replaces tuple rows)
    pipeline_models = []
    for r in pipeline:
        created = (r.get("CreatedDate") or "")[:10]
        close = r.get("CloseDate", "")
        age = 0
        if created and close:
            try:
                age = (date.fromisoformat(snapshot_date) - date.fromisoformat(created)).days
            except ValueError:
                pass
        pipeline_models.append(PipelineDeal(
            account=_val(r, "Account.Name"),
            opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"),
            stage=r.get("StageName", ""),
            forecast_category=r.get("ForecastCategoryName", ""),
            close_date=close,
            arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
            arr_weighted=r.get("APTS_Forecast_ARR__c") or 0,
            probability=r.get("Probability") or 0,
            push_count=r.get("PushCount") or 0,
            deal_type=r.get("Type", ""),
            lead_scope=r.get("Lead_Scope__c", ""),
            industry=_val(r, "Account.Industry"),
            tier=_val(r, "Account.Tier_Calculation__c"),
            sales_region=r.get("Sales_Region__c", ""),
            created_date=created,
            last_activity_date=r.get("LastActivityDate") or None,
            next_step=r.get("NextStep", ""),
            last_modified_date=(r.get("LastModifiedDate") or "")[:10],
            approved=bool(r.get("Stage_20_Approval__c")),
            approval_date=r.get("Stage_20_Approval_Date__c") or None,
            competitor=r.get("Lost_to_Competitor__c", "") or "",
            currency=corp_ccy,
            age_days=age,
            quarter=_quarter_label(close, int(period["analysis_year"])),
        ))
```

Won/Lost deals (replacing lines 689-705):

```python
    won_lost_models = []
    for r in won_lost:
        created = (r.get("CreatedDate") or "")[:10]
        close = r.get("CloseDate", "")
        age = 0
        if created and close:
            try:
                age = (date.fromisoformat(snapshot_date) - date.fromisoformat(created)).days
            except ValueError:
                pass
        won_lost_models.append(WonLostDeal(
            account=_val(r, "Account.Name"),
            opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"),
            stage=r.get("StageName", ""),
            close_date=close,
            arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
            deal_type=r.get("Type", ""),
            industry=_val(r, "Account.Industry"),
            sales_region=r.get("Sales_Region__c", ""),
            reason_won_lost=r.get("Reason_Won_Lost__c", ""),
            competitor=r.get("Lost_to_Competitor__c", ""),
            created_date=created,
            currency=corp_ccy,
            age_days=age,
            quarter=_quarter_label(close, int(period["analysis_year"])),
        ))
```

Approval deals (replacing lines 754-815). Classification logic stays; output changes to ApprovalDeal instances:

```python
    approval_models = []
    for r in approved_2026:
        approval_models.append(ApprovalDeal(
            account=_val(r, "Account.Name"), opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"), stage=r.get("StageName", ""),
            close_date=r.get("CloseDate", ""),
            arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
            status=f"Approved {period['analysis_year']}",
            approval_date=r.get("Stage_20_Approval_Date__c") or None,
            next_step=r.get("NextStep", ""),
            quarter=_quarter_label(r.get("CloseDate", ""), int(period["analysis_year"])),
            lead_scope=r.get("Lead_Scope__c", ""),
        ))
    for r in approved_prior:
        approval_models.append(ApprovalDeal(
            account=_val(r, "Account.Name"), opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"), stage=r.get("StageName", ""),
            close_date=r.get("CloseDate", ""),
            arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
            status="Approved (prior year)",
            approval_date=r.get("Stage_20_Approval_Date__c") or None,
            next_step=r.get("NextStep", ""),
            quarter=_quarter_label(r.get("CloseDate", ""), int(period["analysis_year"])),
            lead_scope=r.get("Lead_Scope__c", ""),
        ))
    for r in pending:
        approval_models.append(ApprovalDeal(
            account=_val(r, "Account.Name"), opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"), stage=r.get("StageName", ""),
            close_date=r.get("CloseDate", ""),
            arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
            status="Pending Approval",
            approval_date=r.get("Submit_for_Stage_20_Review_Date__c") or None,
            next_step=r.get("NextStep", ""),
            quarter=_quarter_label(r.get("CloseDate", ""), int(period["analysis_year"])),
            lead_scope=r.get("Lead_Scope__c", ""),
        ))
    for r in missing:
        approval_models.append(ApprovalDeal(
            account=_val(r, "Account.Name"), opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"), stage=r.get("StageName", ""),
            close_date=r.get("CloseDate", ""),
            arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
            status="Missing (Stage 3+)",
            approval_date=None,
            next_step=r.get("NextStep", ""),
            quarter=_quarter_label(r.get("CloseDate", ""), int(period["analysis_year"])),
            lead_scope=r.get("Lead_Scope__c", ""),
        ))
```

Renewals (replacing lines 839-852):

```python
    renewal_models = []
    for r in renewals:
        renewal_models.append(RenewalDeal(
            account=_val(r, "Account.Name"),
            opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"),
            stage=r.get("StageName", ""),
            close_date=r.get("CloseDate", ""),
            acv_unweighted=r.get("Amount") or 0,
            deal_type="Renewal",
            quarter=_quarter_label(r.get("CloseDate", ""), int(period["analysis_year"])),
            probability=r.get("Probability") or 0,
            comments="",
        ))
```

PI deals (replacing lines 870-874). Keep `_build_pipeline_inspection_rows` as-is, add a conversion wrapper:

```python
    pi_rows = _build_pipeline_inspection_rows(
        pi_raw, analysis_year=int(period["analysis_year"]), corp_ccy=corp_ccy
    )
    pi_models = [
        PIDeal(
            opportunity=row[0], owner=row[1], stage=row[2],
            forecast_category=row[3], arr_weighted=row[4] or 0,
            currency=row[5], close_date=row[6], push_count=row[7] or 0,
            score=row[8], priority=row[9] == "Yes",
        )
        for row in pi_rows
    ]
    # Same for forward PI
    forward_pi_models = []
    if forward_pi_source:
        forward_pi_rows = _build_pipeline_inspection_rows(...)  # existing code
        forward_pi_models = [
            PIDeal(
                opportunity=row[0], owner=row[1], stage=row[2],
                forecast_category=row[3], arr_weighted=row[4] or 0,
                currency=row[5], close_date=row[6], push_count=row[7] or 0,
                score=row[8], priority=row[9] == "Yes",
            )
            for row in forward_pi_rows
        ]
```

Activity signals (replacing lines 992-1003):

```python
    activity_models = []
    for rec in pipeline:
        oid = rec.get("Id")
        agg = act_by_opp.get(str(oid or ""), {"tasks": 0, "events": 0})
        total_90 = int(agg.get("tasks", 0)) + int(agg.get("events", 0))
        activity_models.append(ActivitySignal(
            account=_val(rec, "Account.Name"),
            opportunity=rec.get("Name", ""),
            owner=_val(rec, "Owner.Name"),
            tasks_90d=int(agg.get("tasks", 0)),
            events_90d=int(agg.get("events", 0)),
            total_touches_90d=total_90,
            last_activity_date=rec.get("LastActivityDate") or None,
            flag="No touch 90d" if total_90 == 0 else "",
        ))
```

Commit items (replacing lines 1040-1051):

```python
    commit_models = []
    for r in pipeline:
        cat = str(r.get("ForecastCategoryName") or "").strip()
        if not cat or cat == "Omitted":
            continue
        close = str(r.get("CloseDate") or "")[:10]
        commit_models.append(CommitItem(
            account=_val(r, "Account.Name"),
            opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"),
            forecast_category=cat,
            arr_weighted=r.get("APTS_Forecast_ARR__c") or 0,
            arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
            close_date=close,
            period=_quarter_label(close, int(period["analysis_year"])),
            stage=r.get("StageName", ""),
        ))
```

Movement events — Q1 (replacing lines 1150-1161 and 1167-1178):

```python
    q1_movement_models = []
    # ... existing classification loop, but append MovementEvent instead of list:
    if oid not in seen_slip:
        seen_slip.add(oid)
        q1_movement_models.append(MovementEvent(
            account=account, opportunity=name, owner=owner,
            stage=stage, movement_type="Q1 Slipped",
            old_close=old_val, new_close=new_val,
            changed_on=change_date, arr_unweighted=arr,
        ))
    # Similar for post-Q1 push
```

Q2 movement — same pattern, produces `q2_movement_models`.

Stage/Forecast/CloseDate history events. Build from the existing `stage_history_events`, `fcat_history_events`, and `close_history` lists:

```python
    def _event_fields(r):
        opp = r.get("Opportunity") or {}
        return {
            "opportunity_id": r.get("OpportunityId", ""),
            "opportunity": opp.get("Name", ""),
            "account": (opp.get("Account") or {}).get("Name", ""),
            "owner": (opp.get("Owner") or {}).get("Name", ""),
            "current_stage": opp.get("StageName", ""),
            "old_value": str(r.get("OldValue") or ""),
            "new_value": str(r.get("NewValue") or ""),
            "created_date": str(r.get("CreatedDate", ""))[:10],
            "arr_unweighted": opp.get("APTS_Opportunity_ARR__c") or 0,
        }

    stage_models = [
        StageEvent(
            **_event_fields(r),
            is_closed=((r.get("Opportunity") or {}).get("IsClosed", False)),
            is_won=((r.get("Opportunity") or {}).get("IsWon", False)),
        )
        for r in stage_history_events
    ]

    fcat_models = [ForecastEvent(**_event_fields(r)) for r in fcat_history_events]

    close_date_models = [
        CloseDateEvent(
            **_event_fields(r),
            is_closed=((r.get("Opportunity") or {}).get("IsClosed", False)),
        )
        for r in close_history
    ]
```

**Assemble the DirectorBundle** (replace the Summary sheet building + save + return at lines 1309-1496):

```python
    import re as _re
    import time

    # Build SourceContract from query telemetry
    source_contract = SourceContract(
        sf_org="simcorp.my.salesforce.com",
        api_version=SF_API_VERSION,
        territory_soql_where=where,
        extract_timestamp=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        sources={
            "pipeline_open": DatasetSource("soql", None, f"{territory}:all_fy_deals", len(pipeline), 0),
            "won_lost": DatasetSource("soql", None, f"{territory}:all_fy_deals", len(won_lost), 0),
            "renewals": DatasetSource("soql", None, f"{territory}:renewals", len(renewals), 0),
            "pi_current": DatasetSource("list_view", str(pi_lv), f"{territory}:pi", len(pi_models), 0),
            "pi_forward": DatasetSource("list_view",
                str((forward_pi_source or {}).get("list_view_id", "")) or None,
                f"{territory}:pi_forward",
                len(forward_pi_models), 0,
            ) if forward_pi_source else DatasetSource("list_view", None, f"{territory}:pi_forward", 0, 0),
            "activity": DatasetSource("soql", None, f"{territory}:activity", len(activity_models), 0),
            "stage_events": DatasetSource("field_history", None, f"{territory}:field_history", len(stage_models), 0),
        },
    )

    datasets = Datasets(
        pipeline_open=pipeline_models,
        won_lost=won_lost_models,
        renewals=renewal_models,
        approvals=approval_models,
        pi_current=pi_models,
        pi_forward=forward_pi_models,
        activity=activity_models,
        commit_items=commit_models,
        stage_events=stage_models,
        forecast_category_events=fcat_models,
        close_date_events=close_date_models,
        movement_prior=q1_movement_models,
        movement_current=q2_movement_models,
        snapshot_trend=[],
    )

    dataset_counts = {
        "pipeline_open": len(pipeline_models),
        "won_lost": len(won_lost_models),
        "renewals": len(renewal_models),
        "approvals": len(approval_models),
        "pi_current": len(pi_models),
        "pi_forward": len(forward_pi_models),
        "activity": len(activity_models),
        "commit_items": len(commit_models),
        "stage_events": len(stage_models),
        "forecast_category_events": len(fcat_models),
        "close_date_events": len(close_date_models),
        "movement_prior": len(q1_movement_models),
        "movement_current": len(q2_movement_models),
        "snapshot_trend": 0,
    }

    bundle = DirectorBundle(
        schema_version="1",
        snapshot_date=snapshot_date,
        director=director,
        territory=territory,
        corp_ccy=corp_ccy,
        extract_timestamp=source_contract.extract_timestamp,
        source_contract=source_contract,
        dataset_counts=dataset_counts,
        datasets=datasets,
    )

    # Write JSON bundle
    slug = _re.sub(r"[^a-z0-9]+", "-", director.lower()).strip("-")
    bundle_dir = BUNDLE_OUTPUT_ROOT / snapshot_date
    bundle_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = bundle_dir / f"{slug}.json"
    bundle_path.write_text(bundle.to_json() + "\n", encoding="utf-8")

    # Render Excel workbook from the bundle
    render_bundle_to_excel(bundle, output_path)

    print(f"\n  Saved: {output_path}")
    print(f"  Bundle: {bundle_path}")
    print(f"  Pipeline: {len(pipeline_models)} deals, {corp_ccy} {sum(d.arr_unweighted for d in pipeline_models):,.0f}")

    # Return dict (backward compat for audit — Phase 1)
    won = [d for d in won_lost_models if "Won" in d.stage]
    lost = [d for d in won_lost_models if "Lost" in d.stage or "Opt Out" in d.stage]
    return {
        "territory": territory,
        "director": director,
        "snapshot_date": snapshot_date,
        "workbook_path": _display_path(output_path),
        "bundle_path": _display_path(bundle_path),
        "analysis_year": int(period["analysis_year"]),
        "fy_label": str(period["fy_label"]),
        "counts": dataset_counts,
        "arr": {
            "pipeline_open_eur": sum(d.arr_unweighted for d in pipeline_models),
            "won_eur": sum(d.arr_unweighted for d in won),
            "lost_eur": sum(d.arr_unweighted for d in lost),
            "renewal_acv_eur": sum(d.acv_unweighted for d in renewal_models),
        },
        "pi_source": {
            "list_view_id": str(pi_lv),
            "scope": str(period["fy_label"]),
            "deal_count": len(pi_models),
        },
        "forward_quarter_pi": {
            "status": forward_pi_source_origin,
            "quarter_label": str(period["forward_quarter_label"]),
            "quarter_title": str(period["forward_quarter_title"]),
            "list_view_id": str((forward_pi_source or {}).get("list_view_id", "")) if isinstance(forward_pi_source, dict) else "",
            "list_view_label": str((forward_pi_source or {}).get("list_view_label", "")) if isinstance(forward_pi_source, dict) else "",
            "deal_count": len(forward_pi_models),
        },
    }
```

- [ ] **Step 5: Run the new test**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_extract_director_live_period.py::test_extract_territory_writes_json_bundle -v`
Expected: PASS

- [ ] **Step 6: Run all existing extract tests**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_extract_director_live_period.py -v`
Expected: ALL PASS — existing tests should still work since the return dict format is backward-compatible and the workbook output is identical.

- [ ] **Step 7: Commit**

```bash
cd /Users/test/crm-analytics
git add scripts/extract_director_live.py tests/test_extract_director_live_period.py
git commit -m "feat: refactor extract_territory to build typed models and write JSON bundles"
```

---

## Task 6: Manifest Writer + Integration

Modify `main()` to write a `RunManifest` alongside the existing `director_live_extract_audit.json`.

**Files:**
- Modify: `scripts/extract_director_live.py:1499-1651` (main function)
- Modify: `tests/test_extract_director_live_period.py` (add manifest test)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_extract_director_live_period.py`:

```python
from scripts.monthly_platform.models import BundleManifestEntry, RunManifest


def test_write_run_manifest(tmp_path):
    """Phase 1: manifest.json written alongside old audit artifact."""
    processed = [
        {
            "territory": "APAC",
            "director": "Jesper Tyrer",
            "snapshot_date": "2026-04-22",
            "workbook_path": "output/director_live_workbooks/2026-04-22/jesper-tyrer.xlsx",
            "bundle_path": "output/director_bundles/2026-04-22/jesper-tyrer.json",
            "counts": {"pipeline_open": 12, "won_lost": 5},
            "arr": {"pipeline_open_eur": 1000000},
            "pi_source": {"list_view_id": "00BTb", "scope": "FY26", "deal_count": 12},
            "forward_quarter_pi": {"status": "configured", "deal_count": 6},
        },
    ]
    durations = {"APAC": 3.2}

    from scripts.extract_director_live import _write_run_manifest

    manifest_path = tmp_path / "manifest.json"
    _write_run_manifest(
        manifest_path,
        processed=processed,
        failures=[],
        durations=durations,
        snapshot_date="2026-04-22",
        started_at="2026-04-22T09:30:00Z",
        finished_at="2026-04-22T09:32:45Z",
        query_telemetry_totals={"queries": 76, "rows": 24539, "duration_ms": 21900},
    )

    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text())
    assert data["schema_version"] == "1"
    assert data["run_date"] == "2026-04-22"
    assert len(data["directors"]) == 1
    assert data["directors"][0]["name"] == "Jesper Tyrer"
    assert data["directors"][0]["status"] == "ok"
    assert data["directors"][0]["duration_seconds"] == 3.2
    assert data["failures"] == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_extract_director_live_period.py::test_write_run_manifest -v`
Expected: FAIL — `ImportError: cannot import name '_write_run_manifest'`

- [ ] **Step 3: Add `_write_run_manifest` function and call it from `main()`**

Add this function to `scripts/extract_director_live.py` (near `_write_run_audit`):

```python
def _write_run_manifest(
    manifest_path: Path,
    *,
    processed: list[dict],
    failures: list[dict],
    durations: dict[str, float],
    snapshot_date: str,
    started_at: str,
    finished_at: str,
    query_telemetry_totals: dict,
) -> None:
    from monthly_platform.models import BundleManifestEntry, RunManifest

    directors = [
        BundleManifestEntry(
            name=item["director"],
            territory=item["territory"],
            status="ok",
            bundle_path=item.get("bundle_path", ""),
            workbook_path=item.get("workbook_path", ""),
            row_counts=item.get("counts", {}),
            duration_seconds=durations.get(item["territory"], 0.0),
        )
        for item in processed
    ]
    failed = [
        BundleManifestEntry(
            name=item.get("territory", ""),
            territory=item.get("territory", ""),
            status="failed",
            bundle_path="",
            workbook_path="",
            row_counts={},
            duration_seconds=0.0,
            failure_reason=item.get("message", ""),
        )
        for item in failures
    ]
    manifest = RunManifest(
        schema_version="1",
        run_date=snapshot_date,
        started_at=started_at,
        finished_at=finished_at,
        directors=directors,
        failures=failed,
        telemetry={
            "total_queries": query_telemetry_totals.get("queries", 0),
            "total_rows": query_telemetry_totals.get("rows", 0),
            "total_duration_seconds": query_telemetry_totals.get("duration_ms", 0) / 1000.0,
        },
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(dataclasses.asdict(manifest), indent=2) + "\n",
        encoding="utf-8",
    )
```

Add `import dataclasses` to the top of the file if not already present.

Then, in `main()` after `_write_run_audit(audit_dir, audit_payload)` (around line 1637), add:

```python
    # Phase 1 additive: write manifest.json alongside old audit
    import time as _time

    _write_run_manifest(
        BUNDLE_OUTPUT_ROOT / args.snapshot_date / "manifest.json",
        processed=processed,
        failures=failures,
        durations=_territory_durations,  # see step 4
        snapshot_date=args.snapshot_date,
        started_at=_run_started_at,
        finished_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        query_telemetry_totals=query_totals,
    )
```

To capture timing, add at the start of `main()`:

```python
    _run_started_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
```

And wrap `_run_one` to capture per-territory durations:

```python
    _territory_durations: dict[str, float] = {}

    def _run_one_timed(territory):
        import time
        t0 = time.monotonic()
        result = _run_one(territory)
        _territory_durations[territory] = round(time.monotonic() - t0, 1)
        return result
```

Replace calls from `_run_one(t)` to `_run_one_timed(t)` in the parallel and sequential execution blocks.

- [ ] **Step 4: Run tests**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/test_extract_director_live_period.py -v`
Expected: ALL PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/test/crm-analytics && python -m pytest tests/ -v --tb=short`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/test/crm-analytics
git add scripts/extract_director_live.py tests/test_extract_director_live_period.py
git commit -m "feat: add RunManifest writer alongside existing extract audit"
```

---

## Verification Checklist

After all tasks are complete, verify:

- [ ] `python -m pytest tests/test_models.py tests/test_bundle_validation.py tests/test_excel_renderer.py tests/test_extract_director_live_period.py -v` — all green
- [ ] `python -m pytest tests/ -v --tb=short` — no regressions
- [ ] JSON bundle at `output/director_bundles/{date}/{slug}.json` round-trips through `DirectorBundle.from_json()`
- [ ] Excel workbook at `output/director_live_workbooks/{date}/{slug}.xlsx` is identical in sheet structure and data to pre-refactor output
- [ ] `output/director_live_extract/{date}/director_live_extract_audit.json` still written (backward compat)
- [ ] `output/director_bundles/{date}/manifest.json` written with correct entry counts
- [ ] No openpyxl imports remain in `extract_director_live.py`
- [ ] No `_add_sheet` function remains in `extract_director_live.py`
- [ ] Styling constants (`HEADER_FILL`, etc.) only in `excel_renderer.py`
