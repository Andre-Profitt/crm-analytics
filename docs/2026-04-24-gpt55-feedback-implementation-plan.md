# GPT-5.5 Feedback Implementation Plan — Source-Backed Monthly Platform

Date: 2026-04-24
Baseline reviewed: `live-all-sources-pipeline-open-v18`
Intent: turn the external architecture critique into executable repo work without derailing the green source-backed lane.

## Verdict

Keep the direction, revise the operating model.

The source-backed lane is directionally right: Salesforce sources, explicit periods, source contracts, DirectorBundles, deterministic gates, Excel/PPT outputs, and SharePoint publishing all align with the monthly Sales Director deck goal. The next work is not a Power BI/Fabric/Airflow replacement. The next work is hardening this local-first reporting factory into a small production system.

## Highest-Risk Gaps

1. Calendar-vs-fiscal quarter ambiguity can make a green run business-wrong.
2. Salesforce report/list-view IDs need governance, fingerprints, freshness, and row-bound checks.
3. Deterministic validators can create false comfort about executive-grade quality.
4. The 23-stage runner needs resume/retry/cache semantics before it becomes fragile.
5. Workbooks, think-cell, truth packet, and deck need a canonical metric store between bundles and outputs.
6. Artifact sprawl needs a local run catalog and one operator-facing release summary.
7. YAML authoring needs to evolve from wrapped JSON into modular source contracts.
8. think-cell and PPT generation need a shared presentation/component contract.
9. Optional-empty datasets need owners, due dates, and disclosure rules.
10. Local-first still needs production runbooks, credential checks, notifications, and recovery.

## Implementation Tracks

### Track 1 — Quarter Policy Split

Status: phase 1 implemented in `live-all-sources-pipeline-open-v18`; release packets now block publish unless the business/source/display quarter mapping is present and approved.

Add separate business and source-registry quarter labels:

- `business_calendar`
- `business_current_quarter`
- `business_prior_quarter`
- `business_forward_quarter`
- `source_quarter_label_style`
- `source_current_quarter`
- `source_prior_quarter`
- `source_forward_quarter`
- `quarter_mapping_approved`

Publish should fail if the mapping is missing or unapproved. Do not silently flip the live source registry from calendar to fiscal.

### Track 2 — Source Governance

Add a pre-extraction source fingerprint stage:

- Verify Salesforce org/user.
- Verify report/list-view existence.
- Capture label/API name/folder/owner where available.
- Capture report/list-view columns and filters/query fingerprints.
- Capture row counts and source freshness metadata where available.
- Compare to expected config.
- Emit `source_fingerprint_manifest.json`.

### Track 3 — Config Authoring

Keep YAML authoring and deterministic JSON runtime, but split the YAML:

```text
config/source_contracts/sales_director_monthly/
  index.yaml
  period_policy.yaml
  requirements/
  territories/
  field_packs/
  source_registry/
  bundle_contract.yaml
  metric_contract.yaml
  deck_contract.yaml
  publish_policy.yaml
```

Compile into:

```text
config/runtime_compiled/
  monthly_source_requirements.json
  sd_monthly_territories.json
  monthly_director_bundle_contract.json
  monthly_metric_contract.json
  monthly_deck_contract.json
  source_registry_fingerprints.json
```

### Track 4 — Runner Evolution

Keep local Python, but refactor stage definitions into a lightweight internal DAG:

- Stage dependencies.
- Declared inputs/outputs.
- Input and output hashes.
- Retry policy.
- Timeout policy.
- Cache policy.
- Resume from failed stage.
- `--only`, `--from`, `--until`, `--resume`, `--use-cached-extracts`, `--no-upload`, `--dry-run`, `--explain-plan`.

### Track 5 — Canonical Metric Store

Add a local metric layer between DirectorBundles and every output:

- Prefer DuckDB or an equivalent local structured metric store.
- Workbooks, think-cell, truth packet, and deck become projections.
- Every deck claim maps to metric ID and source fingerprint.

### Track 6 — Presentation Contract

Add a deck/table/component contract:

- Slide IDs.
- Shape/table IDs.
- Named ranges.
- Required metrics.
- Expected dimensions.
- Formatting tokens.
- Numeric formats.
- Render and visual regression expectations.

### Track 7 — Release Catalog

Add a local run catalog:

- Runs.
- Stages.
- Sources.
- Artifacts.
- Metrics.
- Claims.
- SharePoint URLs.
- Checksums.
- Human approval status.

Publish a `release_summary.md` or simple SharePoint index with the operator-facing truth.

## 1–2 Day Work Queue

1. Implement quarter label split in period context and release packet. Done for phase 1: `FY26 Q1` business period maps to `Q2 2026` source/display period with approval metadata and a `quarter_mapping_approved` release check.
2. Add a source fingerprint preflight scaffold with metadata capture for reports/list views.
3. Add row-count policy fields to source config, initially warn-only.
4. Emit `release_summary.md` into release bundles and SharePoint uploads.
5. Add a manual human review checklist gate.
6. Add `--no-upload`, `--resume`, and `--from-stage` aliases if missing.
7. Document source promotion rules for future quarters.

## First Files To Change

- `scripts/monthly_platform/period.py`
- `config/source_contracts/sales_director_monthly.yaml`
- `scripts/compile_monthly_source_contract_config.py`
- `scripts/build_monthly_source_contract.py`
- `scripts/lint_monthly_source_contract.py`
- `scripts/extract_salesforce_sources.py`
- `scripts/monthly_platform/salesforce_reports.py`
- `scripts/monthly_platform/storage.py`
- `scripts/run_source_backed_monthly_pipeline.py`
- `scripts/build_director_gold_analytics.py`
- `scripts/build_deck_truth_packet.py`
- `scripts/monthly_platform/analyst_workbook.py`
- `scripts/monthly_platform/thinkcell_source.py`
- `scripts/build_source_backed_deck.py`
- `scripts/validate_source_backed_deck_visuals.py`
- `scripts/build_source_backed_release_bundle.py`
- `scripts/upload_sales_deck_release_to_sharepoint.py`

## GitHub Goal

Publish the source-backed monthly platform slice so external review tools can inspect real code, not just the architecture brief.

The initial branch should include:

- v18 source-backed runner and configs.
- YAML authoring compiler and tests.
- Period/quarter policy changes.
- Source-backed extraction, bundle, workbook, think-cell, deck, release, and upload scripts.
- Monthly platform modules used by that lane.
- Focused tests and architecture docs.
