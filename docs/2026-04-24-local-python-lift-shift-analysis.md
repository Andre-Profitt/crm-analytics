# Local Python Lift-Shift Analysis - 2026-04-24

## Executive Read

The current system is not fundamentally wrong. It has proven business logic, working Salesforce access, typed bundles, Excel/PPT outputs, and serious truth gates. The local architecture is the weak point: too much source extraction, transformation, workbook rendering, deck rendering, Salesforce tie-out, and orchestration live inside large scripts.

The right upgrade is a staged local lift-shift, not a rewrite.

Target direction:

```text
Salesforce reports/list views
  -> immutable raw extracts
  -> local DuckDB/Parquet warehouse
  -> typed DirectorBundle / RegionBundle
  -> deterministic Gold metrics + fact packet
  -> analyst workbook
  -> think-cell source workbook
  -> PPT + visual/truth gates
```

AI should stay outside the numeric truth path. Claude/GPT can draft analyst notes, critique decks, and compare narrative style against prior runs, but every number should come from the local fact packet and claim registry.

## Inventory Snapshot

Measured from the repo excluding `scripts/_archive`, `.worktrees`, generated output, and virtualenv caches:

| Area | Count |
| --- | ---: |
| Python files in `scripts/` + `tests/` | 252 |
| Approx source/test lines | 122,440 |
| CLI-like entrypoints | 119 |
| Files touching Salesforce concepts/API | 92 |
| Files touching Excel/workbooks | 70 |
| Files touching PowerPoint/decks | 63 |
| Files touching Claude/AI lanes | 16 |

The active monthly-deck path is much smaller:

| Active monthly path | Count |
| --- | ---: |
| Core files | 25 |
| Approx lines | 23,735 |

Largest active files:

| File | Lines | Problem |
| --- | ---: | --- |
| `scripts/build_sharepoint_analysis.py` | 5,705 | Analytics, live Salesforce pulls, workbook rendering, charts, notes, and regional branching in one file. |
| `scripts/build_deck_from_excel.py` | 4,130 | Reads Excel, computes insights, calls live SOQL enrichment, renders PPT, writes sidecar. |
| `scripts/run_sales_director_monthly_master_builder.py` | 1,700 | Orchestration plus Claude lanes plus deck truth gate. |
| `scripts/run_sales_director_monthly_cadence.py` | 1,675 | Canonical operator runner but still subprocess/string-command oriented. |
| `scripts/extract_director_live.py` | 1,599 | Salesforce queries, transformation, bundle creation, Excel render, telemetry, manifest all together. |
| `scripts/validate_tie_out.py` | 845 | Validator still runs live SOQL as a peer source instead of comparing warehouse facts to reports. |

## What Is Already Good

- `scripts/monthly_platform/period.py` gives a reusable period resolver.
- `scripts/monthly_platform/models.py` gives the typed `DirectorBundle` contract.
- `scripts/monthly_platform/excel_renderer.py` already separates workbook rendering from Salesforce access.
- `scripts/monthly_platform/intelligence.py` is a good start for deterministic Gold analytics.
- `scripts/monthly_platform/bundle_validation.py` creates bundle-level quality checks.
- `scripts/build_deck_truth_packet.py` is the right fact/claim/think-cell bridge.
- `scripts/build_monthly_source_contract.py` is the right source-plan guardrail.
- Current truth gates are valuable and should be kept while internals move underneath them.

## Core Diagnosis

### 1. Excel is still too central

The codebase now has typed bundles and Gold analytics, but several important paths still behave as if Excel is the operating source:

- `build_sharepoint_analysis.py` reads director workbooks as primary inputs.
- `build_deck_from_excel.py` reads workbook tabs and then enriches with live SOQL.
- `validate_tie_out.py` reconciles live Salesforce, workbook, regional workbook, and deck sidecar.

The target should be: local warehouse and typed bundles are truth; Excel is a rendered analyst surface.

### 2. Salesforce access is duplicated

Live Salesforce calls appear in extraction, analysis, deck rendering, and tie-out. That makes runs hard to replay and makes monthly results vulnerable to source-system changes after extraction.

Desired rule:

- Extraction stage may call Salesforce.
- Validation may call Salesforce only for controlled tie-out probes.
- Rendering stages must not call Salesforce.

### 3. Period policy is improved but not fully locked

There is now a period resolver and explicit monthly source contract, but risk remains because older code still has local quarter/date assumptions, filename assumptions, and FY labels.

Desired rule:

- One `PeriodContext` is created at run start.
- Every stage consumes the serialized period context from the run manifest.
- No renderer computes its own quarter from `datetime.now()`.

### 4. Orchestration is command-string heavy

`run_sales_director_monthly_cadence.py` is the canonical operator path, but it runs many stages through subprocess calls and then re-parses JSON/stdout.

That is workable but fragile. The next local evolution should be a typed stage runner with explicit inputs/outputs and resumable state.

### 5. The big scripts contain the seams we need

The lift-shift can be surgical because the large files already have natural internal seams:

- `extract_director_live.py`
  - `_build_pipeline_inspection_rows`
  - `_write_run_audit`
  - `_write_run_manifest`
  - `extract_territory` is the 830-line split target.
- `build_sharepoint_analysis.py`
  - Many sheet-builder functions already exist; move analytics prep away from workbook rendering.
- `build_deck_from_excel.py`
  - Slide functions are already separated; the 529-line `build_deck` function should become an adapter over a deck data contract.
- `build_deck_truth_packet.py`
  - `build_packet`, `write_workbook`, and `write_ppttc` are clean enough to keep and harden.

## Recommended Local Tooling

### Adopt now

| Tool | Role | Why |
| --- | --- | --- |
| DuckDB | Local analytical warehouse | Fast local SQL over CSV/JSON/Parquet; ideal for monthly snapshots and reproducible analysis. |
| Parquet | Immutable extract format | Keeps raw and normalized tables compact and replayable. |
| Pydantic | Manifest/source contract models | Strong validation for run manifests, source contracts, period contexts, and stage outputs. |
| Pandera | Dataframe/table validation | Great for report schemas, required columns, date bounds, forecast categories, ARR fields. |
| SQLite | Run ledger | Already familiar locally; perfect for stage state, artifact hashes, and resumability. |

### Add after the core is stable

| Tool | Role | When |
| --- | --- | --- |
| marimo | Local analyst cockpit | After warehouse tables exist; replaces ad hoc notebook/spreadsheet inspection. |
| Prefect | Local orchestration UI/retries | Only after typed stage functions exist; do not wrap the current subprocess maze first. |
| LanceDB | Local style/RAG memory | After fact packets stabilize; use for prior notes/decks/style retrieval, not numeric truth. |
| Playwright/LibreOffice image gates | Deck visual regression | After PPT output is stable enough to baseline. |

### Avoid for now

- Full dbt/Dagster/Great Expectations migration. Too much platform ceremony for this phase.
- More Excel-first automation.
- AI-generated metrics/charts.
- Power BI as a core workflow surface.

## Target Package Shape

Keep the current scripts as thin CLI adapters. Move reusable logic into `scripts/monthly_platform/` first, then later split into a real package if needed.

Proposed local modules:

```text
scripts/monthly_platform/
  contracts.py          # Pydantic contracts for source, runs, stages, metrics
  storage.py            # DuckDB/Parquet/SQLite artifact ledger
  salesforce_auth.py    # sf CLI auth, token, instance URL
  salesforce_reports.py # report/list-view execution, pagination, schema capture
  source_contract.py    # source contract resolver + validator
  transforms.py         # raw report rows -> normalized tables
  metrics.py            # Gold metric calculations
  analyst_notes.py      # deterministic inputs + AI note hooks
  excel_renderer.py     # analyst workbook only
  thinkcell.py          # clean think-cell source/ppttc writer
  deck_contract.py      # slide/element/claim mapping
  visual_gate.py        # render/export/screenshot checks
  runner.py             # typed local stage runner
```

Thin scripts then become:

```text
scripts/monthly_run.py
scripts/extract_salesforce_reports.py
scripts/build_analyst_workbook.py
scripts/build_deck_truth_packet.py
scripts/build_thinkcell_source.py
scripts/render_director_decks.py
scripts/validate_monthly_publish.py
```

## Lift-Shift Sequence

### Phase 0 - Freeze the current truth chain

Goal: preserve the working system before moving internals.

Actions:

1. Fix stale monthly-period test drift around `deck_source`.
2. Add a focused monthly verification command, separate from legacy CRM dashboard builders.
3. Update `Makefile` so `make monthly-verify` does not call root `build_*.py` legacy files.
4. Record the current artifact contract in docs:
   - required inputs
   - required outputs
   - row counts
   - truth gates
   - known non-blocking warnings

Success gate:

```bash
python3 -m pytest \
  tests/test_build_monthly_source_contract.py \
  tests/test_audit_sales_director_source_contract.py \
  tests/test_extract_director_live_period.py \
  tests/test_extract_historical_trending_period.py \
  tests/test_director_gold_analytics.py \
  tests/test_audit_director_etl_intelligence.py \
  tests/test_excel_renderer.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  tests/test_validate_tie_out_artifacts.py \
  -q
```

### Phase 1 - Build local storage underneath existing outputs

Goal: add the engine room without changing decks/workbooks yet.

Actions:

1. Add `monthly_platform/storage.py`.
2. Write every extracted source to:
   - raw JSON
   - normalized CSV/Parquet
   - DuckDB table
   - artifact hash
   - schema hash
   - row count
3. Add `monthly_platform/contracts.py` for:
   - `RunManifest`
   - `StageResult`
   - `SourceContract`
   - `SourceExtract`
   - `TableContract`
4. Add `monthly_platform/salesforce_reports.py` for Salesforce Reports API and list-view extraction.

Success gate:

```bash
python3 scripts/build_monthly_source_contract.py --snapshot-date 2026-04-30 --json
python3 scripts/extract_salesforce_reports.py --snapshot-date 2026-04-30 --dry-run --json
python3 -m pytest tests/test_monthly_storage.py tests/test_salesforce_reports.py -q
```

### Phase 2 - Make reports the canonical source path

Goal: stop treating SOQL as the primary source for monthly deck data.

Actions:

1. Implement report-first extraction from the contract manifest.
2. Execute current-quarter reports first.
3. Count active, non-omitted Land pipeline by region/director.
4. If current-quarter pipeline is empty, execute forward-quarter report/list view and mark fallback explicitly.
5. Preserve raw Salesforce report metadata and schema.
6. Use SOQL only for validation probes.

Success gate:

```bash
python3 scripts/extract_salesforce_reports.py --snapshot-date 2026-04-30 --json
python3 scripts/validate_source_extracts.py --snapshot-date 2026-04-30 --json
```

### Phase 3 - Convert report extracts to typed bundles

Goal: `DirectorBundle` becomes the only contract consumed by Excel, analytics, fact packets, and decks.

Actions:

1. Add transforms from raw report rows to canonical datasets.
2. Keep current `extract_director_live.py` as a wrapper while moving logic under `monthly_platform/transforms.py`.
3. Render existing workbooks from bundles using `excel_renderer.py`.
4. Compare old workbook row counts to new bundle-derived workbook row counts.

Success gate:

```bash
python3 scripts/build_director_bundles_from_reports.py --snapshot-date 2026-04-30 --json
python3 scripts/validate_director_workbook_contract.py --snapshot-date 2026-04-30
```

### Phase 4 - Split analytics from workbook rendering

Goal: retire the `build_sharepoint_analysis.py` monolith gradually.

Actions:

1. Move pure calculations into `monthly_platform/metrics.py`.
2. Move workbook-only formatting into a renderer module.
3. Replace workbook reads with DuckDB/Gold table reads.
4. Keep the old workbook output names until gates are green.

Success gate:

```bash
python3 scripts/build_analyst_workbook.py --snapshot-date 2026-04-30 --json
python3 scripts/validate_sharepoint_analysis_contract.py --date 2026-04-30 --sharepoint-root output/sharepoint
```

### Phase 5 - Make deck rendering fact-packet only

Goal: deck renderers never call Salesforce and never recalculate strategic metrics from workbook tabs.

Actions:

1. Add a `DeckDataContract` from fact packet + claim registry.
2. Make `build_deck_from_excel.py` a compatibility adapter.
3. Move live enrichment out of `build_deck_from_excel.py`.
4. Generate `thinkcell_source.xlsx` from `deck_truth_packet.json`, not from exploratory workbook tabs.
5. Require sidecar claim IDs for every numeric deck field.

Success gate:

```bash
python3 scripts/build_deck_truth_packet.py --snapshot-date 2026-04-30 --json
python3 scripts/build_thinkcell_source.py --snapshot-date 2026-04-30 --json
python3 scripts/validate_deck_delivery_contract.py --date 2026-04-30
```

### Phase 6 - Replace subprocess orchestration with typed stages

Goal: one local state machine can run, resume, fail, and report cleanly.

Actions:

1. Add `monthly_platform/runner.py`.
2. Each stage returns a typed `StageResult`.
3. Stage outputs are written to the SQLite run ledger and JSON manifest.
4. Keep `run_sales_director_monthly_cadence.py` as the operator CLI.
5. Move from subprocess calls to Python calls only after each stage has tests.

Success gate:

```bash
python3 scripts/run_sales_director_monthly_cadence.py monthly-run \
  --snapshot-date 2026-04-30 \
  --unattended \
  --plan-only
```

### Phase 7 - Add local analyst cockpit and visual QA

Goal: make the output feel consultant-grade without risking truth drift.

Actions:

1. Add a marimo app over DuckDB Gold tables and fact packets.
2. Add AI analyst-note generation constrained to claim IDs and deal exceptions.
3. Export PPT to PDF/images.
4. Add visual regression checks for:
   - blank slides
   - placeholder text
   - missing titles
   - impossible table/chart dimensions
   - required claim values appearing in the rendered deck

Success gate:

```bash
python3 scripts/run_analyst_cockpit_export.py --snapshot-date 2026-04-30 --json
python3 scripts/validate_deck_visuals.py --snapshot-date 2026-04-30 --json
```

## Work Packet Plan For Multi-Agent Orchestration

These are designed as non-overlapping work packets.

### Worker A - Contracts and Storage

Owns:

- `scripts/monthly_platform/contracts.py`
- `scripts/monthly_platform/storage.py`
- `tests/test_monthly_storage.py`
- dependency additions for DuckDB/Pydantic/Pandera if needed

Success:

- Can create a monthly run ledger.
- Can write/read raw extract metadata.
- Can calculate row/schema hashes.
- Tests pass without Salesforce access.

### Worker B - Salesforce Report Extractor

Owns:

- `scripts/monthly_platform/salesforce_auth.py`
- `scripts/monthly_platform/salesforce_reports.py`
- `scripts/extract_salesforce_reports.py`
- `tests/test_salesforce_reports.py`

Success:

- Dry-run resolves all report/list-view sources from `monthly_source_contract`.
- Live run writes raw report outputs and metadata.
- No workbook/deck code touched.

### Worker C - Bundle Transform

Owns:

- `scripts/monthly_platform/transforms.py`
- `scripts/build_director_bundles_from_reports.py`
- focused transform tests/fixtures

Success:

- Report extracts become `DirectorBundle` JSON.
- Forward-quarter fallback is encoded in the bundle/manifest.
- Existing bundle validation passes.

### Worker D - Analytics Split

Owns:

- `scripts/monthly_platform/metrics.py`
- refactor seams from `scripts/build_sharepoint_analysis.py`
- analyst workbook renderer tests

Success:

- Gold metrics are computed without reading workbook cells.
- Existing SharePoint/analysis workbook output still validates.

### Worker E - Deck Contract / Visual Gate

Owns:

- `scripts/monthly_platform/deck_contract.py`
- `scripts/monthly_platform/thinkcell.py`
- `scripts/monthly_platform/visual_gate.py`
- deck truth/visual tests

Success:

- Deck-sidecar numbers map to claim IDs.
- think-cell source is generated from claims.
- Visual gate catches blank/placeholder deck failures.

## Immediate Next Implementation

Start with Phase 1, not the whole migration.

Smallest useful patch:

1. Add `scripts/monthly_platform/contracts.py`.
2. Add `scripts/monthly_platform/storage.py`.
3. Add tests proving:
   - a run manifest can be created for `2026-04-30`
   - a source extract can be registered with row/schema hashes
   - an artifact can be retrieved by stage/source id
4. Do not change extraction/rendering behavior yet.

This gives the current scripts a proper local substrate and lets the report-first extractor land cleanly next.

## Implementation Update - Phase 1 Substrate

The first local substrate patch is now landed without changing extraction, workbook rendering, deck rendering, or publish gates.

Added:

- `scripts/monthly_platform/contracts.py`
  - Pydantic control-plane contracts for run manifests, stage results, source extracts, artifact refs, and findings.
  - Separate from `monthly_platform/models.py`, which remains the business-data `DirectorBundle` contract.
- `scripts/monthly_platform/storage.py`
  - Local monthly storage manager.
  - Writes raw source extract JSON.
  - Writes normalized Parquet tables.
  - Writes/queries a SQLite artifact ledger.
  - Registers DuckDB views when `duckdb` is installed.
  - Keeps Salesforce, Excel, PowerPoint, and AI concerns out of the storage layer.
- `tests/test_monthly_storage.py`
  - Verifies run manifest creation.
  - Verifies source extract registration.
  - Verifies row/schema hashes.
  - Verifies artifact lookup by stage/source id.

Requirements updated:

- `pyarrow>=15.0.0`
- `duckdb>=1.1.0`
- `pydantic>=2.7.0`

Verification:

```bash
python3 -m ruff check \
  scripts/monthly_platform/contracts.py \
  scripts/monthly_platform/storage.py \
  tests/test_monthly_storage.py

python3 -m py_compile \
  scripts/monthly_platform/contracts.py \
  scripts/monthly_platform/storage.py

python3 -m pytest tests/test_monthly_storage.py -q
```

Latest result: `4 passed`.

Adjacent monthly regression:

```bash
python3 -m pytest \
  tests/test_monthly_storage.py \
  tests/test_models.py \
  tests/test_bundle_validation.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  -q
```

Latest result: `27 passed`.

Next surgical move:

1. Add `scripts/monthly_platform/salesforce_auth.py`.
2. Add `scripts/monthly_platform/salesforce_reports.py`.
3. Add `scripts/extract_salesforce_reports.py`.
4. Feed `build_monthly_source_contract.py` output into the new extractor.
5. Write every report/list-view result through `MonthlyStorage.register_source_extract`.

## Implementation Update - Modular Source Requirements

The platform now has a declarative source-requirements registry above the territory-specific Salesforce IDs.

Added:

- `config/monthly_source_requirements.json`
  - Describes what the monthly deck platform needs, not just where each territory's source IDs happen to live.
  - Current requirements:
    - `sd_historical_trending`: prior/current/forward Salesforce reports.
    - `sd_pipeline_inspection`: current/forward Pipeline Inspection list views with forward-quarter fallback policy.
- `scripts/monthly_platform/source_requirements.py`
  - Pydantic models for source requirements, field contracts, row-count policy, fallback policy, and resolved source plans.
  - Resolves requirement config against `sd_monthly_territories.json` and the canonical `PeriodContext`.
  - Produces explicit findings when a required territory/period source ID is missing.
- `tests/test_source_requirements.py`
  - Verifies enabled requirements resolve into concrete report/list-view source plan items.
  - Verifies missing IDs block the plan.
  - Verifies disabled requirements drop out without code changes.

Live registry check for `2026-04-30` against current territory config:

- Status: `ok`
- Configured source needs: `45`
- Salesforce reports: `27`
- Salesforce list views: `18`
- Missing source IDs: `0`

Verification:

```bash
python3 -m ruff check \
  scripts/monthly_platform/source_requirements.py \
  tests/test_source_requirements.py

python3 -m py_compile scripts/monthly_platform/source_requirements.py

python3 -m pytest tests/test_source_requirements.py -q
```

Latest result: `3 passed`.

Combined substrate/source-contract gate:

```bash
python3 -m pytest \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  -q
```

Latest result: `15 passed`.

## Implementation Update - Report-First Extractor

The first report-first extraction adapter is now implemented.

Added:

- `scripts/monthly_platform/salesforce_auth.py`
  - Shared `sf org display` auth helper.
  - Builds a `requests.Session` with bearer auth.
- `scripts/monthly_platform/salesforce_reports.py`
  - Runs Salesforce Reports API sources.
  - Runs Salesforce UI API list-view sources.
  - Normalizes report `factMap` detail rows and list-view field payloads into row dictionaries.
  - Preserves raw Salesforce JSON while writing scalar-safe Parquet rows.
- `scripts/extract_salesforce_sources.py`
  - CLI adapter over `monthly_source_requirements.json`.
  - Supports `--dry-run`, `--require-live`, `--only-requirement`, `--only-territory`, `--max-sources`, and `--json`.
  - Writes source plan, run manifest, SQLite ledger, raw JSON, and Parquet through `MonthlyStorage`.
- `tests/test_salesforce_sources.py`
  - Covers report row normalization.
  - Covers list-view row normalization.
  - Covers dry-run source planning.
  - Covers live extraction path with a fake Salesforce client.

Important implementation detail:

- Salesforce report cells can contain nested structs. The storage layer now preserves raw JSON exactly but converts nested row values to stable JSON strings for Parquet, preventing Arrow type-mix failures.

Dry-run gate:

```bash
python3 scripts/extract_salesforce_sources.py \
  --snapshot-date 2026-04-30 \
  --dry-run \
  --run-id dry-run-final \
  --json
```

Latest result:

- Status: `ok`
- Source plan items: `45`
- Salesforce reports: `27`
- Salesforce list views: `18`
- Missing source IDs: `0`
- Live calls: `0`

Live smoke gates:

```bash
python3 scripts/extract_salesforce_sources.py \
  --snapshot-date 2026-04-30 \
  --only-territory APAC \
  --max-sources 1 \
  --run-id live-smoke-apac-1b \
  --json
```

Latest report smoke:

- Status: `ok`
- Source: `00OTb000008g11VMAQ`
- Rows: `15`
- Artifacts: source plan + raw JSON + Parquet

```bash
python3 scripts/extract_salesforce_sources.py \
  --snapshot-date 2026-04-30 \
  --only-territory APAC \
  --only-requirement sd_pipeline_inspection \
  --max-sources 1 \
  --run-id live-smoke-apac-pi-1 \
  --json
```

Latest list-view smoke:

- Status: `ok`
- Source: `00BTb00000Ksa4bMAB`
- Rows: `21`
- Artifacts: source plan + raw JSON + Parquet

Verification:

```bash
python3 -m ruff check \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/monthly_platform/source_requirements.py \
  scripts/monthly_platform/storage.py \
  scripts/extract_salesforce_sources.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py

python3 -m py_compile \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/extract_salesforce_sources.py \
  scripts/monthly_platform/storage.py

python3 -m pytest \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py \
  -q
```

Latest result: `13 passed`.

Broader focused regression:

```bash
python3 -m pytest \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  -q
```

Latest result: `21 passed`.

## Implementation Update - Source Bundle Transform

The extract-to-bundle seam is now in place. This is not yet the final `DirectorBundle`; it is the typed intermediate that lets the platform prove report/list-view extraction, month/quarter policy, and fallback behavior before old workbook assumptions are replaced.

Added:

- `scripts/monthly_platform/source_bundles.py`
  - Defines `TerritorySourceBundle`, `SourceBundleManifest`, normalized Pipeline Inspection rows, historical-trending row groups, and a `PipelineDisplayDecision`.
  - Reads `MonthlyRunManifest`, source plan artifacts, source extracts, and Parquet rows from `MonthlyStorage`.
  - Groups selected source extracts by territory and period role.
  - Chooses current quarter unless current active pipeline is empty and forward active pipeline exists.
- `scripts/build_source_bundles_from_extracts.py`
  - CLI over stored source extracts.
  - Supports `--source-run-dir`, `--source-root`, `--output-root`, `--run-id`, `--require-complete`, and `--json`.
  - Writes territory bundle JSON files and a manifest under `output/monthly_source_bundles`.
- `tests/test_source_bundles.py`
  - Verifies current quarter stays current when active.
  - Verifies forward-quarter fallback when current is empty and forward is active.
  - Verifies `--require-complete` blocks missing selected extracts.

APAC live source extraction:

```bash
python3 scripts/extract_salesforce_sources.py \
  --snapshot-date 2026-04-30 \
  --only-territory APAC \
  --run-id live-apac-sources \
  --json
```

Latest result:

- Status: `ok`
- Selected sources: `5`
- Executed sources: `5`
- Source extracts: `5`
- Findings: `0`

APAC source-bundle build:

```bash
python3 scripts/build_source_bundles_from_extracts.py \
  --snapshot-date 2026-04-30 \
  --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-apac-sources \
  --run-id live-apac-sources \
  --require-complete \
  --json
```

Latest result:

- Status: `ok`
- Territory bundles: `1`
- Source extracts: `5`
- Missing selected sources: `0`
- Forward fallback count: `0`
- APAC display quarter: `Q2 2026`
- APAC current active pipeline: `4` deals / `$3.26M` ARR
- APAC forward active pipeline: `4` deals / `$1.14M` ARR

Verification:

```bash
python3 -m ruff check \
  scripts/monthly_platform/source_bundles.py \
  scripts/build_source_bundles_from_extracts.py \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/monthly_platform/source_requirements.py \
  scripts/monthly_platform/storage.py \
  scripts/extract_salesforce_sources.py \
  tests/test_source_bundles.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py

python3 -m py_compile \
  scripts/monthly_platform/source_bundles.py \
  scripts/build_source_bundles_from_extracts.py \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/extract_salesforce_sources.py \
  scripts/monthly_platform/storage.py

python3 -m pytest \
  tests/test_source_bundles.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  -q
```

Latest result: `24 passed`.

## Implementation Update - DirectorBundle Adapter

The platform now has a first adapter from source bundles into the existing legacy `DirectorBundle` model. The key design choice is conservative: use the source-backed data we have, and make unsupported datasets visibly empty rather than inventing SOQL-equivalent fields from partial list-view data.

Added:

- `scripts/monthly_platform/director_bundle_builder.py`
  - Builds `DirectorBundle` from `TerritorySourceBundle`.
  - Maps Pipeline Inspection current/forward rows into `PIDeal`.
  - Maps historical-trending snapshot columns into `TrendSnapshot`.
  - Preserves source provenance through `SourceContract`.
  - Validates generated bundles with `validate_bundle`.
- `scripts/build_director_bundles_from_sources.py`
  - CLI over `output/monthly_source_bundles`.
  - Supports `--source-bundle-dir`, `--source-root`, `--output-root`, `--run-id`, `--require-valid`, and `--json`.
  - Writes bundle JSON files and a `director_bundle_manifest.json`.
- `tests/test_director_bundle_builder.py`
  - Verifies PI mapping.
  - Verifies historical snapshot conversion.
  - Verifies legacy `DirectorBundle.from_json` round-trip.
  - Verifies manifest and artifact output.

APAC DirectorBundle build from the live APAC source bundle:

```bash
python3 scripts/build_director_bundles_from_sources.py \
  --snapshot-date 2026-04-30 \
  --source-bundle-dir output/monthly_source_bundles/2026-04-30/live-apac-sources \
  --run-id live-apac-sources \
  --require-valid \
  --json
```

Latest result:

- Status: `ok`
- Director bundles: `1`
- Director: `Jesper Tyrer`
- Territory: `APAC`
- `pi_current`: `21`
- `pi_forward`: `4`
- `snapshot_trend`: `60`
- Unsupported legacy datasets remain explicitly empty until their Salesforce source requirements are added.

Verification:

```bash
python3 -m ruff check \
  scripts/monthly_platform/director_bundle_builder.py \
  scripts/build_director_bundles_from_sources.py \
  scripts/monthly_platform/source_bundles.py \
  scripts/build_source_bundles_from_extracts.py \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/monthly_platform/source_requirements.py \
  scripts/monthly_platform/storage.py \
  scripts/extract_salesforce_sources.py \
  tests/test_director_bundle_builder.py \
  tests/test_source_bundles.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py

python3 -m py_compile \
  scripts/monthly_platform/director_bundle_builder.py \
  scripts/build_director_bundles_from_sources.py \
  scripts/monthly_platform/source_bundles.py \
  scripts/build_source_bundles_from_extracts.py \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/extract_salesforce_sources.py \
  scripts/monthly_platform/storage.py

python3 -m pytest \
  tests/test_director_bundle_builder.py \
  tests/test_source_bundles.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  -q
```

Latest result: `27 passed`.

## Implementation Update - DirectorBundle Coverage Contract

The `DirectorBundle` adapter now has a config-governed dataset coverage contract. This turns the uncomfortable truth into machinery: if a dataset is empty, it is either explicitly optional-empty in config or the build emits a coverage finding.

Added:

- `config/monthly_director_bundle_contract.json`
  - Defines every `DirectorBundle` dataset as `source_backed` or `optional_empty`.
  - Marks publish-required source-backed datasets.
  - Records the source requirement IDs that support each source-backed dataset.
- `scripts/monthly_platform/director_bundle_contract.py`
  - Loads and validates the dataset coverage contract.
  - Validates generated bundles against `SourceContract` coverage.
  - Produces manifest-ready coverage summaries.
- `tests/test_director_bundle_contract.py`
  - Verifies the default contract covers every legacy `DirectorBundle` dataset.
  - Verifies source-backed/optional-empty datasets are explicit.
  - Verifies a missing source-backed contract key creates a high-severity finding.

Updated:

- `scripts/monthly_platform/director_bundle_builder.py`
  - Accepts a `DirectorBundleContract`.
  - Adds dataset coverage into each director summary.
  - Derives unsupported datasets from config instead of a hardcoded list.
- `scripts/build_director_bundles_from_sources.py`
  - Adds `--contract`.
  - Defaults to `config/monthly_director_bundle_contract.json`.

Current coverage state:

- Source-backed: `pi_current`, `pi_forward`, `snapshot_trend`.
- Optional-empty: `pipeline_open`, `won_lost`, `renewals`, `approvals`, `activity`, `commit_items`, `stage_events`, `forecast_category_events`, `close_date_events`, `movement_prior`, `movement_current`.

APAC live coverage gate:

```bash
python3 scripts/build_director_bundles_from_sources.py \
  --snapshot-date 2026-04-30 \
  --source-bundle-dir output/monthly_source_bundles/2026-04-30/live-apac-sources \
  --run-id live-apac-sources-contract \
  --require-valid \
  --json
```

Latest result:

- Status: `ok`
- Findings: `0`
- Source-backed: `pi_current`, `pi_forward`, `snapshot_trend`
- Publish-required: `pi_current`, `pi_forward`, `snapshot_trend`
- APAC counts: `pi_current=21`, `pi_forward=4`, `snapshot_trend=60`

Verification:

```bash
python3 -m ruff check \
  scripts/monthly_platform/director_bundle_contract.py \
  scripts/monthly_platform/director_bundle_builder.py \
  scripts/build_director_bundles_from_sources.py \
  scripts/monthly_platform/source_bundles.py \
  scripts/build_source_bundles_from_extracts.py \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/monthly_platform/source_requirements.py \
  scripts/monthly_platform/storage.py \
  scripts/extract_salesforce_sources.py \
  tests/test_director_bundle_contract.py \
  tests/test_director_bundle_builder.py \
  tests/test_source_bundles.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py

python3 -m py_compile \
  scripts/monthly_platform/director_bundle_contract.py \
  scripts/monthly_platform/director_bundle_builder.py \
  scripts/build_director_bundles_from_sources.py \
  scripts/monthly_platform/source_bundles.py \
  scripts/build_source_bundles_from_extracts.py \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/extract_salesforce_sources.py \
  scripts/monthly_platform/storage.py

python3 -m pytest \
  tests/test_director_bundle_contract.py \
  tests/test_director_bundle_builder.py \
  tests/test_source_bundles.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  -q
```

Latest result: `30 passed`.

## Implementation Update - Dataset Promotion Readiness Audit

The platform now has a dataset promotion audit. This prevents the dangerous move where Pipeline Inspection gets treated as open pipeline just because it has enough rows. It checks actual extracted columns before a dataset can be promoted from `optional_empty` to `source_backed`.

Added:

- `scripts/monthly_platform/dataset_readiness.py`
  - Defines field requirements for promotion candidates.
  - Audits actual source-extract Parquet columns.
  - Emits a typed readiness report with matched fields, missing required fields, row counts, and findings.
- `scripts/audit_director_dataset_readiness.py`
  - CLI for readiness audits.
  - Supports `--dataset pipeline_open`, `--source-run-dir`, `--source-root`, `--output-path`, and `--json`.
- `tests/test_dataset_readiness.py`
  - Verifies `pipeline_open` blocks when unweighted ARR and Probability are missing.
  - Verifies `pipeline_open` passes when required fields exist.

Live APAC readiness audit:

```bash
python3 scripts/audit_director_dataset_readiness.py \
  --snapshot-date 2026-04-30 \
  --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-apac-sources \
  --dataset pipeline_open \
  --output-path output/monthly_dataset_readiness/2026-04-30/live-apac-sources/pipeline_open_readiness.json \
  --json
```

Latest result:

- Status: `blocked`
- Candidate source dataset: `pipeline_inspection`
- Pipeline Inspection rows audited: `25`
- Missing required fields: `arr_unweighted`, `probability`
- Matched fields include account, opportunity, owner, stage, forecast category, close date, weighted ARR, type, created date, and territory display.

Decision:

- Do not promote `pipeline_open` from current Pipeline Inspection extracts.
- Add or identify an explicit Salesforce report/list view with unweighted ARR and Probability before flipping `pipeline_open` to `source_backed`.

Canonical artifact:

- `output/monthly_dataset_readiness/2026-04-30/live-apac-sources/pipeline_open_readiness.json`

Verification:

```bash
python3 -m ruff check \
  scripts/monthly_platform/dataset_readiness.py \
  scripts/audit_director_dataset_readiness.py \
  scripts/monthly_platform/director_bundle_contract.py \
  scripts/monthly_platform/director_bundle_builder.py \
  scripts/build_director_bundles_from_sources.py \
  scripts/monthly_platform/source_bundles.py \
  scripts/build_source_bundles_from_extracts.py \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/monthly_platform/source_requirements.py \
  scripts/monthly_platform/storage.py \
  scripts/extract_salesforce_sources.py \
  tests/test_dataset_readiness.py \
  tests/test_director_bundle_contract.py \
  tests/test_director_bundle_builder.py \
  tests/test_source_bundles.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py

python3 -m py_compile \
  scripts/monthly_platform/dataset_readiness.py \
  scripts/audit_director_dataset_readiness.py \
  scripts/monthly_platform/director_bundle_contract.py \
  scripts/monthly_platform/director_bundle_builder.py \
  scripts/build_director_bundles_from_sources.py \
  scripts/monthly_platform/source_bundles.py \
  scripts/build_source_bundles_from_extracts.py \
  scripts/monthly_platform/salesforce_auth.py \
  scripts/monthly_platform/salesforce_reports.py \
  scripts/extract_salesforce_sources.py \
  scripts/monthly_platform/storage.py

python3 -m pytest \
  tests/test_dataset_readiness.py \
  tests/test_director_bundle_contract.py \
  tests/test_director_bundle_builder.py \
  tests/test_source_bundles.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  -q
```

Latest result: `32 passed`.

## Implementation Update - APAC Pipeline Open Promotion

`pipeline_open` is now source-backed for the APAC pilot. The readiness audit did its job: Pipeline Inspection remained insufficient, so a dedicated private Opportunity list view was created and registered as the source.

Salesforce source created:

- Label: `SD Monthly Pipeline Open APAC FY26`
- API name: `SD_Monthly_Pipeline_Open_APAC_FY26`
- List view ID: `00BTb00000LJNODMA5`
- Visibility: private
- Filters: APAC account unit group, open stages `1-6`, Land, `Pipeline/Best Case/Commit`, `THIS FISCAL YEAR`
- Columns: capped at Salesforce's 15-list-view-column limit; includes unweighted ARR, forecast ARR, Probability, stage, forecast category, owner, account, close date, type, created date, activity/next-step/modified/push fields

Updated:

- `config/monthly_source_requirements.json`
  - Adds `sd_pipeline_open`.
  - Uses `territories: ["APAC"]` while this remains a pilot source-backed dataset.
- `config/sd_monthly_territories.json`
  - Adds APAC `pipeline_open_list_view_id` and label.
- `config/monthly_director_bundle_contract.json`
  - Flips `pipeline_open` to `source_backed` and publish-required.
- `scripts/monthly_platform/source_requirements.py`
  - Adds optional requirement-level territory allow-listing.
- `scripts/monthly_platform/source_bundles.py`
  - Adds `PipelineOpenRow` and normalization from the new list-view extract.
- `scripts/monthly_platform/director_bundle_builder.py`
  - Maps `PipelineOpenRow` into legacy `PipelineDeal`.
- `scripts/monthly_platform/dataset_readiness.py`
  - Treats `pipeline_open` extracts as the primary readiness source.

APAC live run:

```bash
python3 scripts/extract_salesforce_sources.py \
  --snapshot-date 2026-04-30 \
  --only-territory APAC \
  --run-id live-apac-sources-pipeline-open \
  --json

python3 scripts/build_source_bundles_from_extracts.py \
  --snapshot-date 2026-04-30 \
  --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-apac-sources-pipeline-open \
  --run-id live-apac-sources-pipeline-open \
  --require-complete \
  --json

python3 scripts/build_director_bundles_from_sources.py \
  --snapshot-date 2026-04-30 \
  --source-bundle-dir output/monthly_source_bundles/2026-04-30/live-apac-sources-pipeline-open \
  --run-id live-apac-sources-pipeline-open \
  --require-valid \
  --json

python3 scripts/audit_director_dataset_readiness.py \
  --snapshot-date 2026-04-30 \
  --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-apac-sources-pipeline-open \
  --dataset pipeline_open \
  --output-path output/monthly_dataset_readiness/2026-04-30/live-apac-sources-pipeline-open/pipeline_open_readiness.json \
  --json
```

Latest result:

- Source registry: `46` configured sources, `0` missing IDs.
- APAC extraction: `6` selected / `6` executed / `0` findings.
- Source bundle: `pipeline_open_rows=13`, `pi_current_rows=21`, `pi_forward_rows=4`, `snapshot_trend=60`.
- DirectorBundle: `pipeline_open=13`, `pi_current=21`, `pi_forward=4`, `snapshot_trend=60`.
- Readiness: `pipeline_open` is `ready`; missing required fields `[]`.
- Focused regression: `33 passed`.

Canonical artifacts:

- `output/monthly_salesforce_sources/2026-04-30/live-apac-sources-pipeline-open/run_manifest.json`
- `output/monthly_source_bundles/2026-04-30/live-apac-sources-pipeline-open/source_bundle_manifest.json`
- `output/monthly_director_bundles_from_sources/2026-04-30/live-apac-sources-pipeline-open/director_bundle_manifest.json`
- `output/monthly_dataset_readiness/2026-04-30/live-apac-sources-pipeline-open/pipeline_open_readiness.json`

Next surgical move:

1. Create equivalent `pipeline_open` list views for the other 8 territories.
2. Use Sales Region filters where the reporting rule requires Sales Region instead of Sales Director Book.
3. Add source IDs to `config/sd_monthly_territories.json`.
4. Expand/remove the `sd_pipeline_open.territories` allow-list.
5. Run full extraction and readiness gates across all territories.

## Strategic Decision

Do not rewrite the pipeline from scratch.

Do:

- Keep the verified 2026-04-23 truth chain as the regression baseline.
- Build the local warehouse under the current outputs.
- Move one seam at a time out of monolith scripts.
- Keep Excel/PPT output names stable while internals improve.
- Use AI only after facts are locked.

Do not:

- Let AI choose source reports or metrics.
- Keep adding logic to `build_sharepoint_analysis.py` or `build_deck_from_excel.py`.
- Add Prefect/Dagster before typed stage functions exist.
- Turn Excel into the canonical database.

## Implementation Update - Full Source-Backed Monthly Lane

The monthly lane now has a full source-backed spine for 2026-04-30:

- `55` Salesforce sources resolved and extracted: `27` list views, `28` reports.
- `pipeline_open` is source-backed for all `9` territories.
- NA Asset Management and Pension & Insurance are split by joining list-view detail rows to the global `SD Pipeline Open FY26` report Industry reference; this avoids impossible `Account.Industry` list-view filters.
- Source bundles and DirectorBundles build cleanly for all territories.
- Analyst workbook and think-cell source artifacts are deterministic and read only DirectorBundle manifests.
- Full source-backed publish gate is implemented in `scripts/validate_monthly_source_backed_run.py` and passes with `0` findings.

Canonical artifact root: `live-all-sources-pipeline-open-v3`.

Primary output artifacts:

- `output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v3/run_manifest.json`
- `output/monthly_source_bundles/2026-04-30/live-all-sources-pipeline-open-v3/source_bundle_manifest.json`
- `output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3/director_bundle_manifest.json`
- `output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_analyst_workbook.xlsx`
- `output/thinkcell_source_from_bundles/2026-04-30/live-all-sources-pipeline-open-v3/thinkcell_source.xlsx`
- `output/thinkcell_source_from_bundles/2026-04-30/live-all-sources-pipeline-open-v3/thinkcell_data.ppttc`
- `output/monthly_dataset_readiness/2026-04-30/live-all-sources-pipeline-open-v3/pipeline_open_readiness.json`

Verification status:

- Dry-run source plan: `55` configured, `0` missing, `0` findings.
- Live extraction: `55/55` executed, `0` failed.
- Publish validation: `ok`, `0` findings.
- Ruff: passed.
- Focused pytest: passed (`46` tests).

## Implementation Update - Source-Backed Deck Truth Bridge

The source-backed monthly lane now reaches the deck truth packet layer:

- Gold analytics can be batch-built directly from `output/monthly_director_bundles_from_sources/...` without choking on `director_bundle_manifest.json`.
- `build_deck_truth_packet.py` accepts the single `source_backed_analyst_workbook.xlsx` as workbook evidence for all directors.
- The master builder deck-truth gate has a source-backed mode so deck sidecars/tie-out can be optional while upstream source/fact artifacts are being assembled.
- The resulting source-backed deck truth packet is green: `0` high blockers, `0` tie-out mismatches, `60` metrics, `60` claims.

Canonical deck-truth artifact root: `output/deck_truth_packets_from_sources/2026-04-30/`.

## Implementation Update - Zero-Blocker Source-Backed Truth Packet

The source-backed deck truth packet is now blocker-clean:

- `validate_monthly_source_backed_run.py` can persist the publish gate JSON with `--output-path`.
- `build_deck_truth_packet.py` accepts `--source-backed-publish-gate` and treats that green gate as the ETL trust evidence for this source-backed lane.
- The master deck-truth gate accepts `--deck-truth-source-backed-publish-gate`.
- `output/deck_truth_packets_from_sources/2026-04-30/deck_truth_packet.json` is `ok` with `0` blockers, `60` metrics, and `60` claims.

This means the local source-backed spine now reaches: Salesforce extraction -> SourceBundle -> DirectorBundle -> analyst workbook -> think-cell source -> gold analytics -> deck truth packet.

## Implementation Update - Source-Backed PPTX Deck Lane

The source-backed spine now produces a standard native PowerPoint control deck and a publish-blocking visual/package audit.

Added:

- `scripts/build_source_backed_deck.py` builds a six-slide PPTX from `deck_truth_packet.json` plus the source-backed publish gate and source bundle manifest.
- `scripts/validate_source_backed_deck_visuals.py` validates slide count, required text, missing placeholders, blank slides, native tables/charts, and visible truth-summary values.
- Focused tests cover deck generation, manifest writing, audit pass, and placeholder blocking.

Canonical artifacts:

- `output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_monthly_review.pptx`
- `output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_deck_manifest.json`
- `output/source_backed_deck_visuals/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_deck_visual_audit.json`

Validation status:

- Deck status: `ok`
- Visual/package audit: `ok`
- Findings: `0`
- Slides: `6`
- Tables: `5`
- Native chart objects: `1`
- Focused pytest after this lane: `55` tests passed.

This extends the working chain to: Salesforce extraction -> SourceBundle -> DirectorBundle -> analyst workbook -> think-cell source -> gold analytics -> deck truth packet -> native source-backed PPTX -> visual/package audit.

## Implementation Update - Master Gate Deck Integration

The source-backed PPTX and visual/package audit are now callable from the monthly master builder after deck-truth compilation.

Master builder additions:

- `run_source_backed_deck_gate(...)`
- `--skip-source-backed-deck`
- `--require-source-backed-deck`
- `--source-backed-deck-output-root`
- `--source-backed-deck-visual-output-root`
- `--source-backed-deck-source-bundle-manifest`

Live master seam:

- Deck truth packet gate: `ok`
- Source-backed deck gate: `ok`
- Visual audit: `ok`
- Output deck: `output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_monthly_review.pptx`
- Output audit: `output/source_backed_deck_visuals/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_deck_visual_audit.json`

Final focused verification:

- Ruff: passed.
- Pytest: `57` focused tests passed.

The modular monthly chain now reaches: Salesforce extraction -> SourceBundle -> DirectorBundle -> analyst workbook -> think-cell source -> gold analytics -> deck truth packet -> native source-backed PPTX -> visual/package audit -> master builder stage.

## Implementation Update - PI View Repair / Deleted Field Cleanup

The live Salesforce PI/list-view layer and local builders have been cleaned up after the `Sales_Director_Book__c` deletion.

Live result:

- Patched `27` config-pinned Opportunity list views through the Salesforce UI API.
- Scope covered: current PI list views, Pipeline Open FY26 list views, and Q3 forward-quarter Land list views.
- Post-patch audit: `ok`, `27` views, `0` findings.
- Repair artifact: `output/pi_list_view_filter_repair_2026-04-24.json`
- Audit artifact: `output/pi_list_view_filter_audit_2026-04-24.json`

Code result:

- Removed active `Sales_Director_Book__c` usage from the SD Monthly extraction/build path.
- Updated PI list-view builders to use Sales Region / Account Unit filters only.
- Removed obsolete uptick custom-field references from `build_dashboard.py`.
- Deleted the obsolete `deploy_uptick_fields.py` deployment helper.

Caveat:

- Salesforce UI API rejects Opportunity list-view filters on `Account.Industry`, `INDUSTRY`, and `Industry`; NA AM / Pension & Insurance exact vertical splitting remains in the source-backed ETL join to the FY26 Pipeline Open report Industry reference.

Focused verification:

- Ruff: passed.
- Pytest: `26` focused tests passed.

## Implementation Update - Reusable PI List View Audit Gate

The live PI/list-view repair is now codified as a first-class monthly gate.

Added:

- `scripts/audit_pi_list_view_filters.py`
- `tests/test_audit_pi_list_view_filters.py`
- `scripts/validate_monthly_source_backed_run.py --list-view-audit`

Live gate result:

- Audit artifact: `output/pi_list_view_filter_audit/2026-04-30/live-all-sources-pipeline-open-v3/pi_list_view_filter_audit.json`
- Source-backed publish gate: `output/monthly_source_backed_publish_gate/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_publish_gate.json`
- Audited views: `27`
- Audit findings: `0`
- Publish gate findings: `0`

This means list-view scope drift is no longer an invisible Salesforce/UI issue; it is now part of the JSON publish gate.
