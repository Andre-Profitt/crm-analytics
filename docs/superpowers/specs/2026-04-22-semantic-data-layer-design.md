# Semantic Data Layer for SD Monthly Pipeline

## Problem

The extract writes directly to Excel sheets. Every downstream consumer (deck builder, SharePoint analysis, tie-out validator, Obsidian notes, exec rollup) reads those sheets back into untyped `list[dict]` via `read_sheet()`. Excel is both the contract and the transport. This creates three problems:

1. No type safety. Every consumer does `r.get("ARR Unweighted (EUR)") or 0` with no guarantee the key exists or the value is numeric.
2. No schema evolution. Renaming a column breaks every consumer silently.
3. Excel is a rendering concern, not a data concern. The pipeline cannot produce JSON, Parquet, or BI tool output without re-extracting from Salesforce.

## Solution

A typed intermediate data layer using frozen Python dataclasses. The extract builds typed models, serializes to JSON, and optionally renders Excel from the models. The JSON bundle is the contract. Excel is a downstream render artifact.

## Architecture

```
Salesforce (SOQL + PI API + Forecast API)
    |
    v
extract_territory()
    |
    v
DirectorBundle (typed dataclasses)
    |
    +---> bundle JSON (contract)
    +---> Excel workbook (render artifact, via render_bundle_to_excel)
    |
    v
Consumers read DirectorBundle.from_json(), never Excel sheets
```

## Bundle format

One JSON file per director. Row-oriented, flat, boring.

```json
{
  "schema_version": "1",
  "snapshot_date": "2026-04-22",
  "director": "Jesper Tyrer",
  "territory": "APAC",
  "corp_ccy": "EUR",
  "extract_timestamp": "2026-04-22T09:32:24",
  "source_contract": {
    "sf_org": "simcorp.my.salesforce.com",
    "api_version": "v66.0",
    "soql_where": "Account_Unit_Group__c = 'SC Asia'",
    "query_count": 8,
    "total_rows": 2714,
    "query_duration_ms": 4200
  },
  "dataset_counts": {
    "pipeline_open": 12,
    "won_lost": 45,
    "renewals": 3,
    "approvals": 8,
    "pi_current": 18,
    "pi_forward": 6,
    "activity": 42,
    "commit_items": 5,
    "stage_events": 180,
    "forecast_category_events": 90,
    "close_date_events": 120,
    "movement_prior": 15,
    "movement_current": 8,
    "snapshot_trend": 24
  },
  "datasets": {
    "pipeline_open": [...],
    "won_lost": [...],
    "renewals": [...],
    "approvals": [...],
    "pi_current": [...],
    "pi_forward": [...],
    "activity": [...],
    "commit_items": [...],
    "stage_events": [...],
    "forecast_category_events": [...],
    "close_date_events": [...],
    "movement_prior": [...],
    "movement_current": [...],
    "snapshot_trend": [...]
  }
}
```

## Output structure

```
output/director_bundles/{date}/
  jesper-tyrer.json
  sarah-pittroff.json
  ...                        (9 bundles)
  manifest.json              (run manifest, replaces extract audit)

output/director_live_workbooks/{date}/
  jesper-tyrer.xlsx           (render artifact from bundle)
  ...
```

`manifest.json` is the primary machine-readable run output:

```json
{
  "schema_version": "1",
  "run_date": "2026-04-22",
  "started_at": "2026-04-22T09:30:00",
  "finished_at": "2026-04-22T09:32:45",
  "directors": [
    {
      "name": "Jesper Tyrer",
      "territory": "APAC",
      "status": "ok",
      "bundle_path": "output/director_bundles/2026-04-22/jesper-tyrer.json",
      "workbook_path": "output/director_live_workbooks/2026-04-22/jesper-tyrer.xlsx",
      "row_counts": {"pipeline_open": 12, "won_lost": 45, ...},
      "duration_seconds": 3.2
    },
    ...
  ],
  "failures": [],
  "telemetry": {
    "total_queries": 76,
    "total_rows": 24539,
    "total_duration_seconds": 21.9
  }
}
```

## Models

All models live in `scripts/monthly_platform/models.py`. One file. Frozen dataclasses. Dates as `str | None` (ISO format, validated in one place). Amounts as `float`.

### Row models

```python
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
    age_days: int               # derived: snapshot_date - created_date
    quarter: str                # derived: "Q1", "Q2", "Q3", "Q4"

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

@dataclass(frozen=True)
class ApprovalDeal:
    account: str
    opportunity: str
    owner: str
    stage: str
    close_date: str
    arr_unweighted: float
    status: str                  # "Approved 2026", "Pending Approval", "Missing (Stage 3+)"
    approval_date: str | None
    next_step: str
    quarter: str

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
    flag: str                    # "Active", "Quiet 30d", "Silent 60d+", "No touch 90d"

@dataclass(frozen=True)
class CommitItem:
    opportunity: str
    owner: str
    forecast_category: str
    arr_weighted: float
    close_date: str
    period: str                  # quarter label

@dataclass(frozen=True)
class StageEvent:
    opportunity_id: str
    opportunity: str
    account: str
    owner: str
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
    movement_type: str           # "Q1 Slipped", "Post-Q1 Push", etc.
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

### Container models

```python
@dataclass(frozen=True)
class SourceContract:
    sf_org: str
    api_version: str
    soql_where: str
    query_count: int
    total_rows: int
    query_duration_ms: int

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

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self), indent=2)

    @classmethod
    def from_json(cls, text: str) -> "DirectorBundle":
        return cls.from_dict(json.loads(text))

    @classmethod
    def from_dict(cls, d: dict) -> "DirectorBundle":
        # reconstruct nested dataclasses from dicts
        ...

@dataclass(frozen=True)
class BundleManifestEntry:
    name: str
    territory: str
    status: str                  # "ok", "partial", "failed"
    bundle_path: str
    workbook_path: str
    row_counts: dict[str, int]
    duration_seconds: float
    failure_reason: str | None

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

## Renderer

`scripts/monthly_platform/excel_renderer.py` (~100-150 lines):

```python
def render_bundle_to_excel(bundle: DirectorBundle, output_path: Path) -> None:
    """Pure function: DirectorBundle in, Excel workbook out.

    No Salesforce calls. No territory resolution. No sidecar logic.
    """
```

This replaces the inline `_add_sheet` calls in `extract_territory()`. It uses the same formatting (Excel Tables, freeze panes, column widths, currency formatting) but reads from typed fields instead of raw lists.

## Extract refactor

`extract_territory()` becomes:

```python
def extract_territory(territory, config, session, instance_url, period, corp_ccy):
    # 1. Query SF (same SOQL as today)
    # 2. Build typed rows (PipelineDeal, WonLostDeal, etc.)
    # 3. Assemble DirectorBundle
    # 4. Write JSON bundle
    # 5. Call render_bundle_to_excel() for the Excel artifact
    # 6. Return BundleManifestEntry
```

The function's return type changes from `dict` (audit payload) to `BundleManifestEntry`. The SOQL queries and field mapping are unchanged — only the output format changes.

## Consumer migration order

1. **Deck builder** — highest value. Reads `DirectorBundle.from_json()` instead of `read_sheet(wb, ...)`. All `r.get("ARR Unweighted (EUR)") or 0` become `deal.arr_unweighted`.
2. **Tie-out validator** — validates bundle fields against live SF, not Excel cells.
3. **Obsidian notes** — reads bundle instead of iterating workbook sheets.
4. **SharePoint analysis** — loads 9 bundles for aggregation instead of 9 workbooks.
5. **Exec rollup** — same as SharePoint analysis.

Each migration is independent. Consumers can be cut over one at a time while the extract writes both JSON and Excel.

## Validation

`scripts/monthly_platform/bundle_validation.py` (~50 lines):

```python
def validate_bundle(bundle: DirectorBundle) -> list[str]:
    """Returns list of validation errors. Empty = valid."""
```

Checks: ISO date formats, non-negative ARR, valid stage names, valid forecast categories, dataset_counts match actual list lengths, quarter labels match close dates. Run after extract, before any consumer.

## What this does NOT do

- Does not change any SOQL queries or SF source logic.
- Does not change any deck slide content or formatting.
- Does not add new analytics or metrics.
- Does not change the cadence orchestrator's control flow.
- Does not remove Excel — it becomes a render artifact produced from the bundle.

## Migration strategy

Phase 1: Add models, bundle serialization, and Excel renderer. Extract writes both JSON and Excel. No consumer changes. Existing pipeline continues to work from Excel.

Phase 2: Cut consumers over one at a time (deck builder first). Each consumer reads from the bundle instead of the workbook. The workbook is still generated for directors who open it directly.

Phase 3: Remove `read_sheet()` calls from consumers. The Excel renderer is the only code that writes workbook sheets. Extract → Bundle → (JSON + Excel + Deck + Analytics) is the final architecture.
