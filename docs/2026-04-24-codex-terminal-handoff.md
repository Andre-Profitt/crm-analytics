# Codex Terminal Handoff - Monthly Deck Truth Chain

## Context

Repo: `/Users/test/crm-analytics`

Andre asked to make the Sales Director monthly deck platform real: live ETL, Gold analytics, Excel/PPT truth gates, RAG-ready claim grounding, and think-cell handoff artifacts.

Do not restart discovery from scratch. The live `2026-04-23` chain has been rebuilt and verified end to end.

## Current Status

The deck truth chain is publish-clean on hard gates:

- Deck truth packet: `ok`
- Four-source tie-out: `ok`
- Tie-out checks: `90`
- Tie-out mismatches: `0`
- Directors audited: `9`
- Deck delivery contract: `ok`, `9 / 9` director decks validated
- SharePoint/regional contract: `ok`
- Gold analytics manifest: `9` directors
- Gold totals: `1,205` open deals, `EUR 304,977,026.82` open ARR, `1,188` deal-risk rows, `8,698` close-date events

One remaining source-data warning is intentionally non-blocking:

- `dan-peppett`: `pipeline_open[106]: negative arr_unweighted (-3624.36)`
- Opportunity: `PCAP - MBL-to-SBL - new money`
- It is a tiny Q4 source-system negative ARR adjustment, now surfaced as a medium warning rather than a publish blocker.

## Root Cause Fixed

The main tie-out failure was not PowerPoint. It was extractor truth.

`scripts/extract_director_live.py` split open versus won/lost using `IsClosed`, but the all-FY Opportunity SOQL did not select `IsClosed`. Closed Q1 won/lost rows were silently dropped from the typed bundles and workbooks.

Fix:

- Added `IsClosed, IsWon` to `PIPELINE_FIELDS`.
- Regenerated all 9 live DirectorBundle JSON files and workbooks for `2026-04-23`.
- Rebuilt Gold analytics.
- Rebuilt SharePoint/regional workbooks.
- Rebuilt all 9 director decks plus exec rollup.
- Reran tie-out and truth packet.

Second bug fixed:

- `scripts/build_deck_from_excel.py` had `_fy_label` defined only inside the empty-renewals branch.
- Decks with renewal rows crashed before fresh sidecars were written.
- `_fy_label` is now defined before the branch.

## Key Code Changes

- `scripts/extract_director_live.py`
  - Selects `IsClosed, IsWon` in the all-FY deal query.

- `scripts/build_deck_from_excel.py`
  - Fixes renewal slide `_fy_label` crash.
  - Removes two stale unused locals so ruff passes.

- `scripts/monthly_platform/intelligence.py`
  - Shared Gold analytics layer: risk table, owner health, movement/churn, transition matrices, concentration, deck-ready insights.

- `scripts/build_director_gold_analytics.py`
  - Supports `--bundle-dir`.
  - Writes one Gold pack per director.
  - Writes per-snapshot manifest with APAC / EMEA / North America regional rollups.

- `scripts/audit_director_etl_intelligence.py`
  - Audits DirectorBundle JSON versus workbook render coverage.

- `scripts/monthly_platform/excel_renderer.py`
  - Adds `Close Date History`.
  - Adds `Deal Risk Index`.
  - Summary sheet includes the expanded source counts.

- `scripts/build_deck_truth_packet.py`
  - Builds grounded deck truth packet.
  - Outputs claim registry, RAG JSONL corpus, think-cell source workbook, and `.ppttc` data payload.
  - Treats immaterial negative ARR validation findings as medium warnings.

## Canonical Artifacts

Truth packet:

- `output/deck_truth_packets/2026-04-23/deck_truth_packet.json`
- `output/deck_truth_packets/2026-04-23/summary.md`
- `output/deck_truth_packets/2026-04-23/rag_corpus.jsonl`
- `output/deck_truth_packets/2026-04-23/thinkcell_source.xlsx`
- `output/deck_truth_packets/2026-04-23/thinkcell_data.ppttc`

Gold analytics:

- `output/director_gold_analytics/2026-04-23/manifest.json`
- `output/director_gold_analytics/2026-04-23/<director>/gold_analytics.json`
- `output/director_gold_analytics/2026-04-23/<director>/summary.md`

Typed bundles and workbooks:

- `output/director_bundles/2026-04-23/*.json`
- `output/director_live_workbooks/2026-04-23/*.xlsx`

Decks:

- `output/simcorp_director_decks/2026-04-23/land-only/*-LAND.pptx`
- `output/simcorp_director_decks/2026-04-23/land-only/*-LAND.json`
- `output/simcorp_director_decks/2026-04-23/land-only/Exec Rollup.pptx`
- `output/simcorp_director_decks/2026-04-23/land-only/Exec Rollup.json`

Gates:

- `output/tie_out/2026-04-23/tie_out_audit.json`
- `output/deck_delivery_contract/2026-04-23/deck_delivery_contract_audit.json`
- `output/sharepoint_analysis_contract/2026-04-23/sharepoint_analysis_contract_audit.json`
- `output/etl_intelligence_audit/2026-04-23/<director>/etl_intelligence_audit.json`

## Rebuild Commands

Full live extraction for all director books:

```bash
python3 scripts/extract_director_live.py --all --snapshot-date 2026-04-23
```

Gold analytics:

```bash
python3 scripts/build_director_gold_analytics.py \
  --bundle-dir output/director_bundles/2026-04-23 \
  --json
```

Rebuild regional SharePoint workbooks:

```bash
python3 scripts/build_sharepoint_analysis.py \
  --workbooks-dir output/director_live_workbooks/2026-04-23 \
  --date 2026-04-23

for territory in \
  "APAC" "EMEA Central" "EMEA MEA" "EMEA NE" "EMEA South West" \
  "EMEA UK & Ireland" "NA Asset Mgmt" "NA Canada" "NA Insurance"; do
  python3 scripts/build_sharepoint_analysis.py \
    --workbooks-dir output/director_live_workbooks/2026-04-23 \
    --date 2026-04-23 \
    --territory "$territory"
done
```

Rebuild director decks and exec rollup:

```bash
for wb in output/director_live_workbooks/2026-04-23/*.xlsx; do
  slug=$(basename "$wb" .xlsx)
  python3 scripts/build_deck_from_excel.py \
    --workbook "$wb" \
    --template assets/SimCorp_PPT_Template.pptx \
    --output "output/simcorp_director_decks/2026-04-23/land-only/${slug}-LAND.pptx" \
    --land-only \
    --date 2026-04-23
done

python3 scripts/build_exec_rollup_deck.py \
  --workbooks-dir output/director_live_workbooks/2026-04-23 \
  --template assets/SimCorp_PPT_Template.pptx \
  --output "output/simcorp_director_decks/2026-04-23/land-only/Exec Rollup.pptx"
```

Truth gates:

```bash
python3 scripts/validate_deck_delivery_contract.py --date 2026-04-23
python3 scripts/validate_sharepoint_analysis_contract.py --date 2026-04-23 --sharepoint-root output/sharepoint
python3 scripts/validate_tie_out.py --date 2026-04-23
python3 scripts/build_deck_truth_packet.py --snapshot-date 2026-04-23 --json
```

Focused verification:

```bash
python3 -m ruff check \
  scripts/extract_director_live.py \
  scripts/build_deck_from_excel.py \
  scripts/build_deck_truth_packet.py \
  tests/test_build_deck_truth_packet.py

python3 -m py_compile \
  scripts/extract_director_live.py \
  scripts/build_deck_from_excel.py \
  scripts/build_deck_truth_packet.py

python3 -m pytest \
  tests/test_build_deck_truth_packet.py \
  tests/test_director_gold_analytics.py \
  tests/test_audit_director_etl_intelligence.py \
  tests/test_excel_renderer.py \
  tests/test_bundle_validation.py \
  -q
```

Latest result: `22 passed`.

## Continuation Update - Metric Store / Claim Compiler

The next durable truth layer is now implemented and verified.

- Metric-store contract added to `scripts/build_deck_truth_packet.py`.
- Claim compiler now emits `metric_id -> claim_id -> deck_element_name`.
- Deck sidecars now embed `claim_contract` and `claim_ids` for every numeric sidecar metric.
- Truth packet now writes `metrics`, `claims`, and `deck_sidecar_claim_refs`.
- think-cell output now includes `MetricStoreTable` and `ClaimRegistryTable` in addition to the existing truth/status tables.
- `run_sales_director_monthly_master_builder.py` now has a post-gate deck truth packet stage with `--skip-deck-truth-packet` and `--require-deck-truth-packet`.

Latest truth packet after continuation:

- Status: `ok`
- Directors: `9`
- Metrics: `222`
- Claims: `222`
- Sidecar claim refs: `9 / 9` embedded
- High blockers: `0`
- Tie-out mismatches: `0`
- Remaining warning: `dan-peppett` immaterial negative ARR source adjustment

Latest focused verification after continuation:

```bash
python3 -m ruff check \
  scripts/extract_director_live.py \
  scripts/build_deck_from_excel.py \
  scripts/build_deck_truth_packet.py \
  scripts/run_sales_director_monthly_master_builder.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py

python3 -m py_compile \
  scripts/extract_director_live.py \
  scripts/build_deck_from_excel.py \
  scripts/build_deck_truth_packet.py \
  scripts/run_sales_director_monthly_master_builder.py

python3 -m pytest \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  tests/test_director_gold_analytics.py \
  tests/test_audit_director_etl_intelligence.py \
  tests/test_excel_renderer.py \
  tests/test_bundle_validation.py \
  -q
```

Latest result: `25 passed`.

## Continuation Update - Monthly Source Contract

The first monthly conveyor-belt layer is now implemented.

- Added `scripts/build_monthly_source_contract.py`.
- It reads `config/sd_monthly_territories.json`.
- It resolves per-territory/director historical trending report IDs for prior, current, and forward quarters.
- It resolves current and forward Pipeline Inspection list-view sources.
- It folds in existing `output/source_contract_audit/<date>/source_contract_audit.json` probe status when present.
- It reads `output/director_bundles/<date>/*.json` when available to decide whether each territory displays current-quarter pipeline, forward-quarter fallback, or an empty state.
- It writes:
  - `output/monthly_source_contract/<date>/monthly_source_contract.json`
  - `output/monthly_source_contract/<date>/summary.md`
- Quarter policy is explicit in the manifest as `calendar_quarter` so Salesforce report labels, workbook tabs, and fallback behavior cannot drift silently.

Live artifact check against the clean `2026-04-23` chain:

```bash
python3 scripts/build_monthly_source_contract.py \
  --snapshot-date 2026-04-23 \
  --require-bundles \
  --json
```

Latest `2026-04-23` result:

- Status: `ok`
- Territories: `9`
- Historical report IDs resolved: `27`
- Missing report IDs: `0`
- Source probe issues: `0`
- Missing bundles: `0`
- Current-quarter empty territories: `5`
- Forward-quarter fallback territories: `4`

April month-end planning mode:

```bash
python3 scripts/build_monthly_source_contract.py \
  --snapshot-date 2026-04-30 \
  --json
```

Latest `2026-04-30` result is `warning` only because the month-end bundles do not exist yet:

- Territories: `9`
- Historical report IDs resolved: `27`
- Missing report IDs: `0`
- Missing bundles: `9`
- Warning findings: `9`

Focused verification:

```bash
python3 -m ruff check \
  scripts/build_monthly_source_contract.py \
  tests/test_build_monthly_source_contract.py \
  scripts/build_deck_truth_packet.py \
  tests/test_build_deck_truth_packet.py

python3 -m py_compile \
  scripts/build_monthly_source_contract.py \
  scripts/build_deck_truth_packet.py

python3 -m pytest \
  tests/test_build_monthly_source_contract.py \
  tests/test_audit_sales_director_source_contract.py \
  tests/test_extract_historical_trending_period.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  -q
```

Latest result: `29 passed`.

Known unrelated test drift surfaced during broader period testing:

- `tests/test_sales_director_monthly_period.py::test_cadence_resolves_snapshot_date_when_omitted`
- Failure: stale test namespace lacks `deck_source`, now required by `run_sales_director_monthly_cadence.builder_command_args`.

## Continuation Update - Local Storage Substrate

The local lift-shift plan is now recorded and Phase 1 substrate is implemented.

- Added `docs/2026-04-24-local-python-lift-shift-analysis.md`.
- Added `scripts/monthly_platform/contracts.py`.
- Added `scripts/monthly_platform/storage.py`.
- Added `tests/test_monthly_storage.py`.
- Updated `requirements.txt` with `pyarrow`, `duckdb`, and `pydantic`.

What this gives the platform:

- Pydantic control-plane contracts for run manifests, stage results, source extracts, artifact refs, and findings.
- A local monthly storage manager that writes raw source JSON, normalized Parquet tables, and a SQLite artifact ledger.
- Optional DuckDB view registration when `duckdb` is installed.
- No behavior change to extraction, workbooks, decks, or publish gates yet.

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

## Continuation Update - Modular Source Requirements

The source layer now has a declarative requirements registry so reporting requirements can be added/removed without editing extraction/rendering code first.

- Added `config/monthly_source_requirements.json`.
- Added `scripts/monthly_platform/source_requirements.py`.
- Added `tests/test_source_requirements.py`.

What this gives the platform:

- Separates "what data the monthly deck platform needs" from territory-specific report/list-view IDs.
- Models required Salesforce source type, period roles, field contracts, row-count policy, consumers, and fallback policy.
- Resolves requirements against `config/sd_monthly_territories.json` and `PeriodContext`.
- Blocks explicitly when a required source ID is missing.
- Supports disabling/removing a requirement through config.

Live registry check for `2026-04-30`:

- Status: `ok`
- Configured source needs: `45`
- Salesforce reports: `27`
- Salesforce list views: `18`
- Missing source IDs: `0`

Verification:

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

## Continuation Update - Report-First Salesforce Extractor

The first report-first extractor is now implemented and live-smoked.

Added:

- `scripts/monthly_platform/salesforce_auth.py`.
- `scripts/monthly_platform/salesforce_reports.py`.
- `scripts/extract_salesforce_sources.py`.
- `tests/test_salesforce_sources.py`.

Updated:

- `scripts/monthly_platform/storage.py` now accepts raw Salesforce payloads and writes scalar-safe Parquet while preserving raw JSON.

What this gives the platform:

- Shared Salesforce auth/session helper instead of duplicated `sf org display` logic.
- Salesforce Reports API extraction into normalized rows.
- Salesforce UI API list-view extraction into normalized rows.
- Registry-driven extraction from `config/monthly_source_requirements.json`.
- Raw JSON + Parquet + SQLite ledger output through `MonthlyStorage`.
- Dry-run source planning without Salesforce calls.

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

- Report smoke: `APAC` / `00OTb000008g11VMAQ` / `15` rows / status `ok`.
- List-view smoke: `APAC` / `00BTb00000Ksa4bMAB` / `21` rows / status `ok`.

Verification:

```bash
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

## Continuation Update - Source Bundle Transform

The first extract-to-bundle transform is now implemented. This is a deliberate transitional layer: it converts stored Salesforce source extracts into territory-level source bundles and explicit current/forward-quarter display decisions before we map everything into the older `DirectorBundle` business model.

Added:

- `scripts/monthly_platform/source_bundles.py`.
- `scripts/build_source_bundles_from_extracts.py`.
- `tests/test_source_bundles.py`.

What this gives the platform:

- Reads the `MonthlyRunManifest`, persisted source plan, Parquet rows, and raw extract metadata from `MonthlyStorage`.
- Groups historical-trending report extracts and Pipeline Inspection list-view extracts by territory.
- Produces one JSON source bundle per territory plus a source-bundle manifest.
- Encodes the quarter label used for pipeline display.
- Applies the forward-quarter fallback rule only when current-quarter active pipeline is empty and forward-quarter active pipeline exists.
- Supports `--require-complete` so missing selected extracts block downstream bundle generation.

APAC live source extraction:

```bash
python3 scripts/extract_salesforce_sources.py \
  --snapshot-date 2026-04-30 \
  --only-territory APAC \
  --run-id live-apac-sources \
  --json
```

Latest extraction result:

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

Latest bundle result:

- Status: `ok`
- Territory bundles: `1`
- Selected sources: `5`
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

## Continuation Update - DirectorBundle Adapter

The first source-bundle-to-`DirectorBundle` adapter is now implemented. It is intentionally conservative: it maps only source-backed datasets and leaves legacy datasets empty until the registry adds their Salesforce source requirements. No workbook tabs and no fabricated pipeline fields are used.

Added:

- `scripts/monthly_platform/director_bundle_builder.py`.
- `scripts/build_director_bundles_from_sources.py`.
- `tests/test_director_bundle_builder.py`.

What this gives the platform:

- Converts `TerritorySourceBundle` JSON into the existing `monthly_platform.models.DirectorBundle` contract.
- Maps Pipeline Inspection current/forward list-view rows into `PIDeal`.
- Converts historical-trending report rows into `snapshot_trend` entries by snapshot date.
- Preserves source provenance in `SourceContract`.
- Writes a `director_bundle_manifest.json` with unsupported legacy datasets called out explicitly.
- Validates every generated bundle with `validate_bundle`.

APAC DirectorBundle build from live source bundle:

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
- Unsupported legacy datasets remain explicitly empty: `pipeline_open`, `won_lost`, `renewals`, `approvals`, `activity`, `commit_items`, field-history/movement datasets.

Verification:

```bash
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

## Continuation Update - DirectorBundle Coverage Contract

The source-first `DirectorBundle` adapter now has a config-governed dataset coverage contract. Empty legacy datasets are no longer just hardcoded in the builder; each dataset is either source-backed or explicitly marked optional-empty in config.

Added:

- `config/monthly_director_bundle_contract.json`.
- `scripts/monthly_platform/director_bundle_contract.py`.
- `tests/test_director_bundle_contract.py`.

Updated:

- `scripts/monthly_platform/director_bundle_builder.py` now validates generated bundles against the dataset coverage contract.
- `scripts/build_director_bundles_from_sources.py` now loads the contract through `--contract`, defaulting to `config/monthly_director_bundle_contract.json`.

What this gives the platform:

- Every `DirectorBundle` dataset must have an explicit coverage policy.
- Source-backed publish datasets must have `SourceContract` coverage.
- Optional-empty datasets cannot accidentally be marked required for publish.
- Manifest output now shows source-backed datasets, optional-empty datasets, publish-required datasets, source requirement IDs, and row counts.
- Unsupported legacy datasets are visible by config, not hidden in code.

Current source-backed datasets:

- `pi_current` from `sd_pipeline_inspection`.
- `pi_forward` from `sd_pipeline_inspection`.
- `snapshot_trend` from `sd_historical_trending`.

Current optional-empty datasets:

- `pipeline_open`, `won_lost`, `renewals`, `approvals`, `activity`, `commit_items`, `stage_events`, `forecast_category_events`, `close_date_events`, `movement_prior`, `movement_current`.

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

## Continuation Update - Dataset Promotion Readiness Audit

The platform now has a pre-promotion audit so a dataset cannot be flipped from `optional_empty` to `source_backed` unless the current extracted sources actually contain the required fields.

Added:

- `scripts/monthly_platform/dataset_readiness.py`.
- `scripts/audit_director_dataset_readiness.py`.
- `tests/test_dataset_readiness.py`.

What this gives the platform:

- Audits a target `DirectorBundle` dataset against actual extracted source columns.
- Starts with `pipeline_open`, because this is the most tempting dataset to fake from Pipeline Inspection.
- Blocks promotion when required fields are missing.
- Writes a reusable JSON artifact through `--output-path`.

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
- Decision: do not promote `pipeline_open` from current Pipeline Inspection extracts.

Canonical artifact:

- `output/monthly_dataset_readiness/2026-04-30/live-apac-sources/pipeline_open_readiness.json`

Verification:

```bash
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

## Continuation Update - APAC Pipeline Open Promotion

`pipeline_open` is now source-backed for the APAC pilot. The current Pipeline Inspection views were not promoted; a dedicated private Opportunity list view was created with the missing unweighted ARR and Probability fields.

Salesforce source created:

- Label: `SD Monthly Pipeline Open APAC FY26`
- API name: `SD_Monthly_Pipeline_Open_APAC_FY26`
- List view ID: `00BTb00000LJNODMA5`
- Visibility: private
- Filters: `Account_Unit_Group__c = SC Asia`, open stages `1-6`, `Type = Land`, forecast category `Pipeline/Best Case/Commit`, close date `THIS FISCAL YEAR`
- Core columns: account, opportunity, owner, stage, forecast category, close date, unweighted ARR, weighted ARR, probability, type, created date, activity/next-step/modified/push fields

Updated:

- `config/monthly_source_requirements.json` adds `sd_pipeline_open`, scoped to `territories: ["APAC"]`.
- `config/sd_monthly_territories.json` adds APAC `pipeline_open_list_view_id` and label.
- `config/monthly_director_bundle_contract.json` flips `pipeline_open` to `source_backed` / publish-required.
- `scripts/monthly_platform/source_requirements.py` adds requirement-level territory allow-listing.
- `scripts/monthly_platform/source_bundles.py` adds `PipelineOpenRow` and `pipeline_open` normalization.
- `scripts/monthly_platform/director_bundle_builder.py` maps source-bundle `pipeline_open` rows into legacy `PipelineDeal`.
- `scripts/monthly_platform/dataset_readiness.py` audits `pipeline_open` against the new `pipeline_open` source dataset.

APAC end-to-end gate:

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

- Source registry: `46` configured sources, `0` missing source IDs.
- APAC extraction: `6` selected / `6` executed / `0` findings.
- Source bundle: `pipeline_open_rows=13`, `pi_current_rows=21`, `pi_forward_rows=4`, `snapshot_trend=60`.
- DirectorBundle: status `ok`, findings `0`.
- Readiness audit: `pipeline_open` status `ready`, missing fields `[]`.
- Source-backed publish datasets: `pipeline_open`, `pi_current`, `pi_forward`, `snapshot_trend`.

Canonical artifacts:

- `output/monthly_salesforce_sources/2026-04-30/live-apac-sources-pipeline-open/run_manifest.json`
- `output/monthly_source_bundles/2026-04-30/live-apac-sources-pipeline-open/source_bundle_manifest.json`
- `output/monthly_director_bundles_from_sources/2026-04-30/live-apac-sources-pipeline-open/director_bundle_manifest.json`
- `output/monthly_dataset_readiness/2026-04-30/live-apac-sources-pipeline-open/pipeline_open_readiness.json`

Verification:

```bash
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

Latest result: `33 passed`.

## CRM Rules For Terminal Codex

- Use Salesforce CLI/API only. Do not use Salesforce MCP or CRM Analytics MCP tools.
- Target org: `apro@simcorp.com` on `simcorp.my.salesforce.com`, Wave API `v66.0`.
- Do not commit `.env` or secrets.
- The repo is intentionally dirty with many historical/untracked files. Do not clean unrelated files.
- The meaningful current work is in the files listed above plus generated `output/` artifacts.

## Next Best Moves

1. Replicate the `pipeline_open` list-view source for the other 8 territories:
   - keep the same 15-column core because Salesforce list views cap at 15 columns
   - use territory-native filters from `config/sd_monthly_territories.json`
   - prefer Sales Region filters where the business rule requires Sales Region instead of Sales Director Book
   - add IDs to `config/sd_monthly_territories.json`
   - expand or remove the `territories` allow-list in `sd_pipeline_open`

2. Add a full-run bundle completeness gate:
   - all `46` configured source needs resolved
   - all selected extracts present
   - no stale schemas
   - explicit skipped/optional reasons for every unsupported legacy dataset

3. Generate the analyst workbook from bundles/metric tables:
   - source extracts tab
   - metric store tab
   - deal exceptions tab
   - region narrative inputs
   - human-style analyst notes constrained to claim IDs

4. Build a real think-cell template lane:
   - Named template elements: `TruthStatus`, `RegionalRollupsTable`, `DirectorKpiTable`, `PublishBlockersTable`
   - Use `thinkcell_source.xlsx` for Excel-linked workflow.
   - Use `thinkcell_data.ppttc` for JSON automation workflow on Windows / think-cell Server.

5. Add visual regression:
   - export PPT to PDF/images
   - check no placeholder text
   - check required slide titles
   - check key values appear
   - check no blank charts/tables

## Suggested Terminal Codex Prompt

```text
You are in /Users/test/crm-analytics. Read docs/2026-04-24-codex-terminal-handoff.md first.

Continue from the publish-clean 2026-04-23 Sales Director monthly deck truth chain.
Do not redo discovery. Verify the existing gates, then implement the next durable layer:
replicate the APAC `pipeline_open` list-view source pattern for the remaining territories and add their IDs to config without hardcoding source IDs in Python.

Respect CRM rules: use sf CLI/API only; do not use Salesforce/CRM Analytics MCP tools.
Do not clean unrelated dirty repo files.

Start with:
python3 scripts/extract_salesforce_sources.py --snapshot-date 2026-04-30 --only-territory APAC --run-id live-apac-sources --json
python3 scripts/build_source_bundles_from_extracts.py --snapshot-date 2026-04-30 --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-apac-sources --run-id live-apac-sources --require-complete --json
python3 scripts/build_director_bundles_from_sources.py --snapshot-date 2026-04-30 --source-bundle-dir output/monthly_source_bundles/2026-04-30/live-apac-sources --run-id live-apac-sources-contract --require-valid --json
python3 scripts/audit_director_dataset_readiness.py --snapshot-date 2026-04-30 --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-apac-sources --dataset pipeline_open --output-path output/monthly_dataset_readiness/2026-04-30/live-apac-sources/pipeline_open_readiness.json --json
python3 scripts/extract_salesforce_sources.py --snapshot-date 2026-04-30 --only-territory APAC --run-id live-apac-sources-pipeline-open --json
```

## Continuation Update - Full Pipeline Open Promotion

Completed after the APAC pilot: `pipeline_open` is now source-backed for all 9 Sales Director territories.

What changed:

- Created/updated private Opportunity list views for all territories via `scripts/manage_pipeline_open_list_views.py`.
- Promoted `sd_pipeline_open` from APAC-only to all territories in `config/monthly_source_requirements.json`.
- Added global report reference `sd_pipeline_open_reference` using report `00OTb000008fzirMAA` (`SD Pipeline Open FY26`) to enrich list-view rows with `Industry` / `Sales Region`.
- Fixed the NA vertical split: Opportunity list views cannot filter or display `Account.Industry`; NA AM and P&I now use the FY26 Pipeline Open report as a deterministic Industry reference while retaining list-view detail fields.
- Added source-backed analyst workbook lane and think-cell source lane.
- Added full source-backed publish validator: `scripts/validate_monthly_source_backed_run.py`.

Canonical 2026-04-30 run:

```bash
python3 scripts/extract_salesforce_sources.py \
  --snapshot-date 2026-04-30 \
  --run-id live-all-sources-pipeline-open-v3 \
  --json

python3 scripts/build_source_bundles_from_extracts.py \
  --snapshot-date 2026-04-30 \
  --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v3 \
  --run-id live-all-sources-pipeline-open-v3 \
  --require-complete \
  --json

python3 scripts/build_director_bundles_from_sources.py \
  --snapshot-date 2026-04-30 \
  --source-bundle-dir output/monthly_source_bundles/2026-04-30/live-all-sources-pipeline-open-v3 \
  --run-id live-all-sources-pipeline-open-v3 \
  --require-valid \
  --json

python3 scripts/audit_director_dataset_readiness.py \
  --snapshot-date 2026-04-30 \
  --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v3 \
  --dataset pipeline_open \
  --output-path output/monthly_dataset_readiness/2026-04-30/live-all-sources-pipeline-open-v3/pipeline_open_readiness.json \
  --json

python3 scripts/build_source_backed_analyst_workbook.py \
  --manifest output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3/director_bundle_manifest.json \
  --output-path output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_analyst_workbook.xlsx \
  --json

python3 scripts/build_thinkcell_source_from_bundles.py \
  --manifest output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3/director_bundle_manifest.json \
  --output-dir output/thinkcell_source_from_bundles/2026-04-30/live-all-sources-pipeline-open-v3 \
  --json

python3 scripts/validate_monthly_source_backed_run.py \
  --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v3 \
  --source-bundle-dir output/monthly_source_bundles/2026-04-30/live-all-sources-pipeline-open-v3 \
  --director-bundle-dir output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3 \
  --readiness-dir output/monthly_dataset_readiness/2026-04-30/live-all-sources-pipeline-open-v3
```

Latest truth:

- Source registry: `55` configured sources, `0` missing IDs, `0` findings.
- Extraction: `55` selected / `55` executed / `0` failed / `0` findings.
- Source bundles: `9` territories, `0` findings.
- DirectorBundles: `9` bundles, status `ok`.
- Pipeline readiness: `ready`, missing required fields `[]`, source row counts `pipeline_open=88`, `pipeline_inspection=139`.
- Publish gate: status `ok`, `55` selected extracts present, `9` DirectorBundle coverage checks, `0` findings.
- Analyst workbook: `source_backed_analyst_workbook.xlsx`, row counts `126/198/197/90/36` across coverage, metric, exceptions, narrative, notes seed.
- Think-cell source: `thinkcell_source.xlsx` + `thinkcell_data.ppttc`, `36` metrics, `848` source-backed rows.

Territory pipeline counts / quarter behavior:

- APAC: `pipeline_open=13`, display `Q2 2026` current quarter.
- Canada: `pipeline_open=3`, display `Q3 2026` forward fallback.
- Central Europe: `pipeline_open=14`, display `Q2 2026` current quarter.
- Middle East & Africa: `pipeline_open=11`, display `Q2 2026` current quarter.
- NA Asset Management: `pipeline_open=4`, display `Q3 2026` forward fallback.
- NL & Nordics: `pipeline_open=3`, display `Q3 2026` forward fallback.
- Pension & Insurance: `pipeline_open=9`, display `Q3 2026` forward fallback.
- Southern Europe: `pipeline_open=2`, display `Q2 2026`; no active current/forward PI rows.
- UK & Ireland: `pipeline_open=14`, display `Q2 2026` current quarter.

Final verification:

```bash
python3 -m ruff check \
  scripts/manage_pipeline_open_list_views.py \
  scripts/monthly_platform/source_requirements.py \
  scripts/monthly_platform/source_bundles.py \
  scripts/monthly_platform/director_bundle_builder.py \
  scripts/monthly_platform/dataset_readiness.py \
  scripts/monthly_platform/analyst_workbook.py \
  scripts/monthly_platform/thinkcell_source.py \
  scripts/build_source_backed_analyst_workbook.py \
  scripts/build_thinkcell_source_from_bundles.py \
  scripts/validate_monthly_source_backed_run.py \
  tests/test_manage_pipeline_open_list_views.py \
  tests/test_source_requirements.py \
  tests/test_salesforce_sources.py \
  tests/test_source_bundles.py \
  tests/test_director_bundle_builder.py \
  tests/test_dataset_readiness.py \
  tests/test_source_backed_analyst_workbook.py \
  tests/test_thinkcell_source_from_bundles.py \
  tests/test_validate_monthly_source_backed_run.py

python3 -m pytest \
  tests/test_manage_pipeline_open_list_views.py \
  tests/test_source_requirements.py \
  tests/test_salesforce_sources.py \
  tests/test_source_bundles.py \
  tests/test_director_bundle_builder.py \
  tests/test_director_bundle_contract.py \
  tests/test_dataset_readiness.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  tests/test_source_backed_analyst_workbook.py \
  tests/test_thinkcell_source_from_bundles.py \
  tests/test_validate_monthly_source_backed_run.py \
  -q
```

Result: ruff passed; pytest passed (`46` focused tests).

Next best moves:

1. Wire `source_backed_analyst_workbook.xlsx` into the existing deck/fact-packet path.
2. Extend deck truth packet to point at `live-all-sources-pipeline-open-v3` as the source-backed artifact root.
3. Add a PowerPoint visual regression gate over the think-cell/template output.
4. Decide whether Southern Europe's `empty_current_and_forward_quarter` should remain publish-ok or become a medium publish warning.

## Continuation Update - Source-Backed Deck Truth Bridge

The source-backed lane now feeds the deck truth/fact-packet path instead of stopping at DirectorBundles.

Added:

- `scripts/build_director_gold_analytics.py` now ignores `director_bundle_manifest.json` when batch-building gold packs from a DirectorBundle directory.
- `scripts/build_deck_truth_packet.py` accepts `--analyst-workbook` so the single source-backed analyst workbook can be used as workbook evidence for every director.
- `scripts/run_sales_director_monthly_master_builder.py` deck-truth gate now supports source-backed mode with `--deck-truth-analyst-workbook` and `--deck-truth-optional-decks-tieout`.

Canonical source-backed deck-truth commands:

```bash
python3 scripts/build_director_gold_analytics.py \
  --bundle-dir output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3 \
  --output-root output/director_gold_analytics_from_sources \
  --json

python3 scripts/build_deck_truth_packet.py \
  --snapshot-date 2026-04-30 \
  --gold-root output/director_gold_analytics_from_sources \
  --bundle-dir output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3 \
  --workbook-dir output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3 \
  --analyst-workbook output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_analyst_workbook.xlsx \
  --output-root output/deck_truth_packets_from_sources \
  --json
```

Latest result:

- Gold packs from source-backed DirectorBundles: `9` directors, status `ok`.
- Source-backed deck truth packet: status `ok`, `0` high blockers, `0` tie-out mismatches, `60` metrics, `60` claims.
- Remaining medium blockers: missing ETL intelligence audits for the new `2026-04-30` source-backed run. These do not block publish in the truth packet but should be generated before final leadership packaging.

Canonical artifacts:

- `output/director_gold_analytics_from_sources/2026-04-30/manifest.json`
- `output/deck_truth_packets_from_sources/2026-04-30/deck_truth_packet.json`
- `output/deck_truth_packets_from_sources/2026-04-30/thinkcell_source.xlsx`
- `output/deck_truth_packets_from_sources/2026-04-30/thinkcell_data.ppttc`
- `output/deck_truth_packets_from_sources/2026-04-30/rag_corpus.jsonl`
- `output/deck_truth_packets_from_sources/2026-04-30/summary.md`

Verification:

```bash
python3 -m ruff check \
  scripts/run_sales_director_monthly_master_builder.py \
  scripts/build_deck_truth_packet.py \
  scripts/build_director_gold_analytics.py \
  tests/test_deck_truth_packet_gate.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_director_gold_analytics.py

python3 -m pytest \
  tests/test_deck_truth_packet_gate.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_director_gold_analytics.py \
  -q
```

Result: ruff passed; focused deck-truth tests passed.

## Continuation Update - Zero-Blocker Source-Backed Truth Packet

The prior source-backed deck truth bridge is now tightened: the source-backed publish gate is persisted and attached to the deck truth packet, replacing the legacy per-director ETL-audit warning for this source-backed lane.

Added:

- `scripts/validate_monthly_source_backed_run.py --output-path` writes a canonical publish-gate artifact.
- `scripts/build_deck_truth_packet.py --source-backed-publish-gate` records the source-backed gate as truth evidence and suppresses missing legacy ETL-audit warnings when the source-backed gate is `ok`.
- `scripts/run_sales_director_monthly_master_builder.py` passes `--deck-truth-source-backed-publish-gate` through the deck-truth gate.

Canonical publish gate artifact:

- `output/monthly_source_backed_publish_gate/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_publish_gate.json`

Canonical zero-blocker deck truth result:

- `output/deck_truth_packets_from_sources/2026-04-30/deck_truth_packet.json`
- Status: `ok`
- Directors: `9`
- Metrics: `60`
- Claims: `60`
- Blockers: `0`
- High blockers: `0`
- Tie-out mismatches: `0`
- Source-backed publish gate status: `ok`

Master gate live call also passes:

- Output root: `output/deck_truth_packets_from_sources_gate/2026-04-30/`
- Status: `ok`
- Return code: `0`
- High blockers: `0`

Final focused verification after this bridge:

```bash
python3 -m ruff check \
  scripts/manage_pipeline_open_list_views.py \
  scripts/monthly_platform/source_requirements.py \
  scripts/monthly_platform/source_bundles.py \
  scripts/monthly_platform/director_bundle_builder.py \
  scripts/monthly_platform/dataset_readiness.py \
  scripts/monthly_platform/analyst_workbook.py \
  scripts/monthly_platform/thinkcell_source.py \
  scripts/build_source_backed_analyst_workbook.py \
  scripts/build_thinkcell_source_from_bundles.py \
  scripts/validate_monthly_source_backed_run.py \
  scripts/build_director_gold_analytics.py \
  scripts/build_deck_truth_packet.py \
  scripts/run_sales_director_monthly_master_builder.py \
  tests/test_manage_pipeline_open_list_views.py \
  tests/test_source_requirements.py \
  tests/test_salesforce_sources.py \
  tests/test_source_bundles.py \
  tests/test_director_bundle_builder.py \
  tests/test_dataset_readiness.py \
  tests/test_source_backed_analyst_workbook.py \
  tests/test_thinkcell_source_from_bundles.py \
  tests/test_validate_monthly_source_backed_run.py \
  tests/test_director_gold_analytics.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py

python3 -m pytest \
  tests/test_manage_pipeline_open_list_views.py \
  tests/test_source_requirements.py \
  tests/test_salesforce_sources.py \
  tests/test_source_bundles.py \
  tests/test_director_bundle_builder.py \
  tests/test_director_bundle_contract.py \
  tests/test_dataset_readiness.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  tests/test_source_backed_analyst_workbook.py \
  tests/test_thinkcell_source_from_bundles.py \
  tests/test_validate_monthly_source_backed_run.py \
  tests/test_director_gold_analytics.py \
  -q
```

Result: ruff passed; pytest passed (`52` focused tests).

## Continuation Update - Source-Backed PPTX Deck Lane

The visual deck lane is now implemented on top of the source-backed truth packet.

Added:

- `scripts/build_source_backed_deck.py`
- `scripts/validate_source_backed_deck_visuals.py`
- `tests/test_build_source_backed_deck.py`
- `tests/test_validate_source_backed_deck_visuals.py`

Canonical live artifacts:

- `output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_monthly_review.pptx`
- `output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_deck_manifest.json`
- `output/source_backed_deck_visuals/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_deck_visual_audit.json`

Live result:

- Deck status: `ok`
- Slide count: `6`
- Visual/package audit status: `ok`
- Audit findings: `0`
- Tables: `5`
- Native chart objects: `1`
- PPTX package checked: `true`
- Rendered PNG checked: `false`
- PowerPoint parity checked: `false`

Current deck structure:

1. Monthly Sales Director Operating Review
2. Publish gate status / evidence chain
3. Regional open pipeline rollup
4. Director book pipeline/risk/tie-out table
5. Quarter display and fallback decisions
6. Leadership handoff to think-cell / standard deck production

Live commands:

```bash
python3 scripts/build_source_backed_deck.py \
  --truth-packet output/deck_truth_packets_from_sources/2026-04-30/deck_truth_packet.json

python3 scripts/validate_source_backed_deck_visuals.py \
  --deck-path output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_monthly_review.pptx \
  --truth-packet output/deck_truth_packets_from_sources/2026-04-30/deck_truth_packet.json \
  --manifest-path output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_deck_manifest.json
```

Final focused verification after deck lane:

```bash
python3 -m ruff check \
  scripts/build_source_backed_deck.py \
  scripts/validate_source_backed_deck_visuals.py \
  tests/test_build_source_backed_deck.py \
  tests/test_validate_source_backed_deck_visuals.py

python3 -m pytest \
  tests/test_build_source_backed_deck.py \
  tests/test_validate_source_backed_deck_visuals.py \
  tests/test_manage_pipeline_open_list_views.py \
  tests/test_source_requirements.py \
  tests/test_salesforce_sources.py \
  tests/test_source_bundles.py \
  tests/test_director_bundle_builder.py \
  tests/test_director_bundle_contract.py \
  tests/test_dataset_readiness.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_deck_truth_packet_gate.py \
  tests/test_source_backed_analyst_workbook.py \
  tests/test_thinkcell_source_from_bundles.py \
  tests/test_validate_monthly_source_backed_run.py \
  tests/test_director_gold_analytics.py \
  -q
```

Result: ruff passed; focused pytest passed (`55` tests).

## Continuation Update - Master Gate Source-Backed Deck Integration

The monthly master builder now has a post-truth source-backed deck stage.

Added to `scripts/run_sales_director_monthly_master_builder.py`:

- `run_source_backed_deck_gate(...)`
- `--skip-source-backed-deck`
- `--require-source-backed-deck`
- `--source-backed-deck-output-root`
- `--source-backed-deck-visual-output-root`
- `--source-backed-deck-source-bundle-manifest`

Behavior:

- Runs only after `deck_truth_packet` is available.
- Builds `source_backed_monthly_review.pptx` from the truth packet.
- Runs `validate_source_backed_deck_visuals.py`.
- Marks the master run `partial` only when `--require-source-backed-deck` is set and the deck/audit stage is not `ok`.
- Skips by default when source-backed inputs were not provided, so legacy workbook/deck runs do not accidentally invoke this lane.

Live master seam result:

- Truth gate: `ok`
- Truth packet: `output/deck_truth_packets_from_sources_gate/2026-04-30/deck_truth_packet.json`
- Source-backed deck stage: `ok`
- Visual audit: `ok`
- Deck: `output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_monthly_review.pptx`
- Audit: `output/source_backed_deck_visuals/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_deck_visual_audit.json`

Final integration verification:

```bash
python3 -m ruff check \
  scripts/run_sales_director_monthly_master_builder.py \
  scripts/build_source_backed_deck.py \
  scripts/validate_source_backed_deck_visuals.py \
  tests/test_deck_truth_packet_gate.py \
  tests/test_build_source_backed_deck.py \
  tests/test_validate_source_backed_deck_visuals.py

python3 -m pytest \
  tests/test_build_source_backed_deck.py \
  tests/test_validate_source_backed_deck_visuals.py \
  tests/test_deck_truth_packet_gate.py \
  tests/test_manage_pipeline_open_list_views.py \
  tests/test_source_requirements.py \
  tests/test_salesforce_sources.py \
  tests/test_source_bundles.py \
  tests/test_director_bundle_builder.py \
  tests/test_director_bundle_contract.py \
  tests/test_dataset_readiness.py \
  tests/test_monthly_storage.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_build_deck_truth_packet.py \
  tests/test_source_backed_analyst_workbook.py \
  tests/test_thinkcell_source_from_bundles.py \
  tests/test_validate_monthly_source_backed_run.py \
  tests/test_director_gold_analytics.py \
  -q
```

Result: ruff passed; focused pytest passed (`57` tests).

Next best move is adding a reliable headless PPTX render snapshot for this source-backed deck once the local render dependency is confirmed.

## Continuation Update - PI List View Repair And Dead-Field Cleanup

Clarification: the prior fix made the source-backed extraction truth path safe, but the live director-facing PI/list-view definitions and legacy builders still needed cleanup so they could not drift back to deleted fields.

Live Salesforce repair:

- Patched `27` config-pinned Opportunity list views through the Salesforce UI API.
- Covered current PI scope, Pipeline Open FY26, and forward-quarter Q3 Land views for all `9` territories.
- Replaced deleted `Sales_Director_Book__c` filters with the canonical Sales Region / Account Unit policy.
- Verified live list-view filters from Salesforce after patch: `status=ok`, `view_count=27`, `finding_count=0`.
- Repair artifact: `output/pi_list_view_filter_repair_2026-04-24.json`
- Audit artifact: `output/pi_list_view_filter_audit_2026-04-24.json`

Important carveout:

- Opportunity list views cannot filter `Account.Industry`, `INDUSTRY`, or `Industry`; this was tested through UI API and rejected with `Invalid field criteria filter columns`.
- NA Asset Management and Pension & Insurance are now safe from all-org leakage via `Sales_Region__c contains North America` + `Account_Unit__c = SC USA`, but the exact NA vertical split remains an ETL/report-level split using the FY26 Pipeline Open report Industry reference.

Local cleanup:

- Removed active `Sales_Director_Book__c` references from:
  - `scripts/director_data_helpers.py`
  - `scripts/extract_director_workbook_snapshot.py`
  - `scripts/build_director_workbooks.py`
  - `scripts/manage_pipeline_open_list_views.py`
  - `scripts/create_pi_land_forecast_views.py`
- Removed obsolete uptick fields from `build_dashboard.py`.
- Deleted `deploy_uptick_fields.py`.
- Remaining `Sales_Director_Book__c` grep hit is archived only: `scripts/_archive/extract_director_data.py`.

Verification:

```bash
python3 -m ruff check \
  build_dashboard.py \
  scripts/director_data_helpers.py \
  scripts/extract_director_workbook_snapshot.py \
  scripts/build_director_workbooks.py \
  scripts/manage_pipeline_open_list_views.py \
  scripts/create_pi_land_forecast_views.py \
  tests/test_manage_pipeline_open_list_views.py

python3 -m pytest \
  tests/test_manage_pipeline_open_list_views.py \
  tests/test_salesforce_sources.py \
  tests/test_source_requirements.py \
  tests/test_build_monthly_source_contract.py \
  tests/test_source_bundles.py \
  tests/test_director_bundle_builder.py \
  tests/test_director_bundle_contract.py \
  -q
```

Result: ruff passed; focused pytest passed (`26` tests).

## Continuation Update - PI List View Audit Gate

The one-off PI/list-view repair is now a reusable monthly gate.

Added:

- `scripts/audit_pi_list_view_filters.py`
- `tests/test_audit_pi_list_view_filters.py`
- `scripts/validate_monthly_source_backed_run.py --list-view-audit`

Behavior:

- Reads `config/sd_monthly_territories.json`.
- Audits current PI, Pipeline Open FY26, and forward-quarter PI list views.
- Fails on deleted/invalid fields including `Sales_Director_Book__c`, `Account.Industry`, `INDUSTRY`, and `Industry`.
- Requires territory scope filters, common open Land filters, and expected close-date literals for Pipeline Open / forward-quarter views.
- Feeds the audit artifact into the source-backed publish gate so list-view scope drift blocks publish.

Canonical live audit:

- `output/pi_list_view_filter_audit/2026-04-30/live-all-sources-pipeline-open-v3/pi_list_view_filter_audit.json`
- Status: `ok`
- Configured views: `27`
- Audited views: `27`
- Findings: `0`

Updated publish gate:

- `output/monthly_source_backed_publish_gate/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_publish_gate.json`
- Status: `ok`
- `list_view_audit_view_count`: `27`
- `list_view_audit_finding_count`: `0`
- Total findings: `0`

Live commands:

```bash
python3 scripts/audit_pi_list_view_filters.py \
  --target-org apro@simcorp.com \
  --territory-config config/sd_monthly_territories.json \
  --output-path output/pi_list_view_filter_audit/2026-04-30/live-all-sources-pipeline-open-v3/pi_list_view_filter_audit.json

python3 scripts/validate_monthly_source_backed_run.py \
  --source-run-dir output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v3 \
  --source-bundle-dir output/monthly_source_bundles/2026-04-30/live-all-sources-pipeline-open-v3 \
  --director-bundle-dir output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v3 \
  --readiness-dir output/monthly_dataset_readiness/2026-04-30/live-all-sources-pipeline-open-v3 \
  --list-view-audit output/pi_list_view_filter_audit/2026-04-30/live-all-sources-pipeline-open-v3/pi_list_view_filter_audit.json \
  --output-path output/monthly_source_backed_publish_gate/2026-04-30/live-all-sources-pipeline-open-v3/source_backed_publish_gate.json
```

Verification:

- Ruff passed on the audit/gate/list-view/extraction changes.
- Focused pytest passed; `68` tests collected in the current source-backed regression slice.

Next best move: put this audit command into the monthly master/cadence runner before extraction, so scope drift is caught before any Salesforce source rows are pulled.
