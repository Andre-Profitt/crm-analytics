# GPT Pro Feedback Implementation Plan — v2 Cycle

Date: 2026-04-25 (revised same-day to absorb v3 architecture vision)
Baseline reviewed: `live-all-sources-pipeline-open-v20c` (snapshot `2026-04-30`)
GPT Pro response (verbatim): [`docs/2026-04-25-gpt-pro-review-response.md`](./2026-04-25-gpt-pro-review-response.md)
Brief that triggered review: [`docs/2026-04-25-gpt-pro-handoff-v2-etl-build-sharpness.md`](./2026-04-25-gpt-pro-handoff-v2-etl-build-sharpness.md)
v3 architecture vision (north star): [`docs/2026-04-25-gpt-pro-v3-architecture-vision.md`](./2026-04-25-gpt-pro-v3-architecture-vision.md)
Prior cycle plan (for context): [`docs/2026-04-24-gpt55-feedback-implementation-plan.md`](./2026-04-24-gpt55-feedback-implementation-plan.md)

## Cycle Status (live)

| Track | Subject                                            | Status           | Commit                                                                       |
| ----- | -------------------------------------------------- | ---------------- | ---------------------------------------------------------------------------- |
| A     | Cron cutover to source-backed runner               | ✅ DONE          | [`dc0488a`](https://github.com/Andre-Profitt/crm-analytics/commit/dc0488a)   |
| B     | Per-axis source-quality policy actions             | ✅ DONE          | [`5303a5f`](https://github.com/Andre-Profitt/crm-analytics/commit/5303a5f)   |
| C     | Source-quality baselines calibrator                | ✅ DONE          | merged in PR #2                                                              |
| D     | Distribution checks for pipeline sources           | ✅ DONE          | merged in PR #4; activation slice (calibrator + first opt-in) on this branch |
| **H** | **DuckDB / Parquet warehouse layer (v3)**          | 🟡 SKELETON      | (this branch — Track H skeleton PR)                                          |
| **I** | **Pandera + JSON Schema dataframe contracts (v3)** | NEW              | —                                                                            |
| E     | `deck_contract.yaml` (deck API)                    | pending          | —                                                                            |
| F     | Template-first builder + brand fingerprint         | pending          | —                                                                            |
| **J** | **OpenLineage events + slide-to-source map (v3)**  | NEW              | —                                                                            |
| **K** | **Release catalog + waiver system (v3)**           | NEW              | —                                                                            |
| **L** | **Reusable workflows + composite actions (v3)**    | NEW              | —                                                                            |
| G     | v20d evidence pass                                 | gated on E/F/H/I | —                                                                            |

Tracks H/I/J/K/L are the v3 polish layer. They sit between the original A–G to convert the system from "a pipeline that happens to make a deck" into a contract-driven analytics product factory: every source, transform, metric, slide, artifact, and waiver declared in a contract, validated by code, linked by lineage, reproducible from a snapshot date.

## Verdict

**Keep the source-backed lane. Harden it. Do not redesign.**

GPT Pro confirms v20c is publish-green but not change-safe. The largest hidden risk is not that the deck fails to render — it is that a green run can still be green for the wrong source slice. Two of the most damaging gaps are also the cheapest to close: the scheduled GitHub Actions cron still points at the legacy lane, and the source-backed deck builder doesn't actually load the SimCorp template.

## Highest-Risk Gaps (Top 3)

1. **Scheduled cron on the wrong lane.** `.github/workflows/monthly-review.yml` invokes `scripts/run_monthly_director_review.py` (legacy `datetime.now()`-based path). Anyone re-running the workflow on a different day silently shifts the business scope.
2. **Extract quality "0 high" is partly theater.** `min_rows=0`, `allow_zero=True`, no historical baselines, no distribution checks. A filter could silently drop a stage and the gate would still pass.
3. **Source-backed builder is not template-first.** `Presentation()` (no-arg) plus hard-coded RGB constants. Brand fidelity is incidental, not enforced.

## Implementation Tracks

### Track A — Cron Cutover To Source-Backed Runner (Day 1)

Status: ✅ **DONE** — commit [`dc0488a`](https://github.com/Andre-Profitt/crm-analytics/commit/dc0488a). Workflow now invokes `run_source_backed_monthly_pipeline.py` with explicit `--snapshot-date` + `--run-id`; cron resolves snapshot to last day of prior month (no `date +%Y-%m-%d` fallback); legacy lane is reachable only via `inputs.legacy_only=true`. 7/7 cutover tests pass.

Files:

- `.github/workflows/monthly-review.yml`
- `scripts/run_source_backed_monthly_pipeline.py` (verify CLI accepts `--snapshot-date`, `--run-id`)

Changes:

- Replace `python3 scripts/run_monthly_director_review.py --date $DATE` with `python3 scripts/run_source_backed_monthly_pipeline.py --snapshot-date "$SNAPSHOT_DATE" --run-id "$RUN_ID"`.
- Require explicit `SNAPSHOT_DATE` workflow input; fail fast if empty (no `datetime.now()` defaulting in CI).
- Move legacy invocation behind a `legacy_only: true` workflow-dispatch input (off by default).
- Add CI lint that grep-fails on `datetime.now()` inside scripts called by scheduled workflows.

Tests:

- `tests/test_monthly_workflow_cutover.py` — assert workflow YAML invokes the source-backed runner and rejects empty snapshot date.

Risk before / after: see Track 2 in the response.

### Track B — Source Policy Action Separation (Day 2)

Status: ✅ **DONE** — commit [`5303a5f`](https://github.com/Andre-Profitt/crm-analytics/commit/5303a5f). Each axis (`zero_row_action`, `min_rows_action`, `max_rows_action`, `max_records_action`, `null_threshold_action` via `required_field_null_action`, `distribution_action`) carries its own policy. Severity is no longer derived from `zero_row_action`. New `expected_empty_conditions` propagates predicate annotations (PI policy: `territory_has_no_forward_pipeline`). 13/13 new tests + 39 adjacent = 52 pass.

Files:

- `config/source_contracts/sales_director_monthly.yaml`
- `scripts/extract_salesforce_sources.py`
- `scripts/monthly_platform/source_requirements.py` (policy parsing)

Changes:

- Add to source quality policy schema:
  - `zero_row_action` (info | warn | block) — already conceptually present, formalize.
  - `min_rows_action` — separate from zero-row.
  - `max_rows_action`
  - `null_threshold_action`
  - `distribution_action`
  - `expected_empty_conditions` — predicate that legitimizes empty extraction (e.g., territory has no Q3 forward pipeline).
- Update YAML compiler + JSON runtime contract.
- Update audit emitter to honor each action distinctly.

### Track C — Source Quality Baselines Calibrator (Day 3)

Status: **NEW**. Read-only first; explicit promotion required.

Files (new):

- `scripts/calibrate_source_quality_baselines.py`
- `config/source_quality_baselines/<source_key>.json` (one per source, hand-promoted)
- `scripts/monthly_platform/source_quality_baselines.py` (loader + comparator)

Files (touched):

- `scripts/extract_salesforce_sources.py` — wire baseline comparator into the audit step.

Behavior:

- Read v20c manifest + prior approved runs as input.
- Emit per-source-key/dataset/period_role/territory: median row count, p95, expected stage mix, expected null rates.
- Default mode: read-only — gate emits drift findings as `info`.
- `--promote-baselines` flag required to update `config/source_quality_baselines/`.
- Block release on > N% deviation from approved baselines (threshold per policy).

### Track D — Distribution Checks For Pipeline Sources (Day 4)

Status: **NEW**. Builds on Track C.

Files (new):

- `scripts/monthly_platform/distribution_audit.py`

Files (touched):

- `scripts/extract_salesforce_sources.py`
- `scripts/build_source_bundles_from_extracts.py` (so distribution applies post-bundle, not just raw)

Checks per pipeline source:

- Stage mix delta vs baseline
- Quarter mix delta
- Territory mix delta
- Open/closed segmentation delta
- Owner concentration drift

Thresholds: warn at X% delta, block at Y% delta. Calibrate from baselines (Track C output).

### Track H — DuckDB / Parquet Warehouse Layer (v3 insert, Day 5–6)

Status: **NEW** (v3). Sits between extraction and deck/workbook outputs so every downstream artifact reads from the same canonical tables instead of ad-hoc JSON.

Files (new):

- `output/source_backed_warehouse/{snapshot_date}/{run_id}/raw/*.parquet` — direct extraction tables (one per source type).
- `output/source_backed_warehouse/{snapshot_date}/{run_id}/staged/*.parquet` — typed/cleaned (opportunities, accounts, territories).
- `output/source_backed_warehouse/{snapshot_date}/{run_id}/marts/*.parquet` — `director_book`, `pipeline_coverage`, `source_quality_findings`, `release_metrics`.
- `scripts/monthly_platform/warehouse.py` — DuckDB-backed reader/writer, snapshot/run-scoped paths, schema-aware.
- `scripts/build_source_backed_warehouse.py` — pipeline stage that materializes raw → staged → marts.

Files (touched):

- `scripts/run_source_backed_monthly_pipeline.py` — insert warehouse stage between bundle build and deck/workbook stages.
- `scripts/build_source_backed_analyst_workbook.py`, `scripts/build_thinkcell_source_from_bundles.py`, `scripts/build_source_backed_deck.py` — read from marts via `warehouse.py`, not raw bundle JSON.
- `requirements.txt` — add `duckdb`, `pyarrow`.

Behavior:

- Raw layer: 1 Parquet per `(snapshot_date, run_id, source_type)`; row-for-row preservation of extraction output.
- Staged layer: dtype-coerced, currency normalized to EUR, period-role tagged.
- Mart layer: business-grain joins (director × territory × period), one Parquet per consumer surface.
- All reads via SQL through DuckDB (`read_parquet(...)`) so future tools (Evidence cockpit, ad-hoc analyst SQL, dashboards) consume the same numbers the deck does.

### Track I — Pandera + JSON Schema Dataframe Contracts (v3 insert, Day 6)

Status: **NEW** (v3). Builds on Track H. Adds two-tier schema validation: runtime dataframe checks (Pandera) and portable artifact contracts (Frictionless Table Schema / JSON Schema).

Files (new):

- `schemas/pandera/<table>.py` — one Pandera schema per raw / staged / mart table.
- `schemas/table_schema/<table>.schema.json` — portable JSON-expressible schema for the same tables (Frictionless Table Schema vocabulary).
- `scripts/monthly_platform/dataframe_contracts.py` — loader, validator, fail-fast wrapper.
- `tests/fixtures/bad_extracts/` — negative-control fixtures (`stage_5_missing/`, `stale_source/`, `report_hit_salesforce_cap/`, `territory_dropped/`, `null_rate_spike/`, `duplicate_opportunities/`, `wrong_quarter_mapping/`). Each fixture proves a quality gate fails — turning "we think the gate works" into "we have a regression test."

Files (touched):

- `scripts/build_source_backed_warehouse.py` — Pandera-validate every dataframe before write.
- `scripts/extract_salesforce_sources.py` — emit Frictionless schema alongside raw Parquet so downstream consumers can self-describe.
- `requirements.txt` — add `pandera`, `frictionless`.

### Track E — `deck_contract.yaml` (Day 5)

Status: **NOT STARTED**. Highest leverage on the build side.

Files (new):

- `config/deck_contract.yaml` (schema per GPT Pro response §"Proposed deck_contract.yaml shape")
- `scripts/monthly_platform/deck_contract.py` (loader + validator + binding resolver)

Files (refactored to read from contract, not constants):

- `scripts/build_source_backed_deck.py` (build slides from `slides[]`)
- `scripts/validate_source_backed_deck_table_contract.py` (validate against contract, not hardcoded list)
- `scripts/validate_source_backed_deck_visuals.py`
- `scripts/validate_source_backed_deck_semantics.py`
- `scripts/validate_source_backed_deck_render.py`
- `scripts/build_thinkcell_source_from_bundles.py` (emit named ranges from contract `thinkcell.range_name`)

Schema must include: `schema_version`, `brand`, `data_bindings`, `slides[]` (id, slide_number, title, layout, purpose, required_text, tables[], render_expectations, thinkcell).

### Track F — Template-First Builder + Brand Enforcement (Day 6+)

Status: **NOT STARTED**. Depends on Track E being usable.

File: `scripts/build_source_backed_deck.py`

Changes:

- Replace `Presentation()` with `Presentation(deck_contract.brand.template)` (defaults to `assets/SimCorp_PPT_Template.pptx`).
- Assert required layouts present (e.g. `simcorp_content`, `simcorp_title`, `simcorp_section_break`); fail with named missing-layouts error.
- Assert design tokens (colors, fonts) match `deck_contract.brand` expectation; fail on mismatch.
- Remove all hard-coded `RGBColor(...)` constants from builder body — read from contract.
- Add brand-inheritance gate to `validate_source_backed_deck_visuals.py`: theme-token diff vs contract.

### Track G — v20d Evidence Pass (Day 7)

Status: **NOT STARTED**. Validates Tracks A–F.

Run:

```bash
python3 scripts/run_source_backed_monthly_pipeline.py \
  --snapshot-date 2026-04-30 \
  --run-id live-all-sources-pipeline-open-v20d
```

Compare v20d to v20c on:

- 24 stages — must be 24/24 ok.
- Sources — 55/55 selected/extracted, 0 high fingerprint findings.
- Extract quality — non-zero finding count expected (baselines now active); confirm findings are explainable, not regressions.
- Quarter mapping — unchanged.
- Deck contract compliance — new `deck_contract.yaml` validator must pass.
- Brand inheritance — new gate must pass.
- Render — 6/6 slides.
- Release packet — 23+ artifacts, SharePoint plan clean.

Sign off if green and parity to v20c on the unchanged dimensions.

### Track J — OpenLineage Events + Slide-To-Source Map (v3 insert, Week 2)

Status: **NEW** (v3). Lightweight lineage layer — emit OpenLineage-compatible JSON locally; backend (Marquez or other) is optional later.

Files (new):

- `scripts/monthly_platform/lineage.py` — emit OpenLineage `RUN_START` / `RUN_COMPLETE` events per stage.
- `output/source_backed_monthly_pipeline_runs/{snapshot}/{run_id}/lineage_events/*.json` — one event per stage.
- `output/source_backed_monthly_pipeline_runs/{snapshot}/{run_id}/lineage_index.json` — aggregated DAG view of inputs / outputs / facets across all stages.
- `output/source_backed_monthly_pipeline_runs/{snapshot}/{run_id}/slide_to_source_map.json` — for each slide / table cell, the chain back to source key, list-view ID, owner, snapshot date.

Files (touched):

- `scripts/run_source_backed_monthly_pipeline.py` — wrap each stage in lineage emission.
- `scripts/build_source_backed_deck.py` — emit slide-to-source bindings as it builds.

Goal: when a future VP asks "Why did the April deck say pipeline was down?" → trace from slide → metric → mart → bundle → Salesforce extract → source key → owner → snapshot date in one query.

### Track K — Release Catalog + Waiver System (v3 insert, Week 3)

Status: **NEW** (v3). Standardize release packets as a signed, dual-format catalog (human + machine).

Files (new):

- `scripts/build_release_summary.py` — emits both formats from release packet + lineage index + quality findings.
- `output/.../release_summary.md` — operator-facing one-pager.
- `output/.../release_summary.json` — machine-readable.
- `output/.../release_decision.json` — final go/no-go with approver.
- `output/.../waivers.json` — active waivers consulted during this run.
- `output/.../artifact_index.json`, `lineage_index.json`, `quality_findings.json`, `deck_contract_results.json`.
- `config/waivers/` — waiver registry (one YAML per waiver id, e.g. `WV-2026-04-001.yaml`).
- `scripts/monthly_platform/waivers.py` — loader + expiry/owner enforcement.

Waiver rules (enforced by `waivers.py`):

- No anonymous waivers (owner required).
- No permanent waivers (`expires_on` required).
- No waiver without explicit reason and approved-by.
- No waiver for hard truth/tie-out blockers unless `release_policy.allow_truth_waiver: true`.
- `allowed_runs` whitelists which run IDs may consume the waiver — others ignore it.

`release_summary.md` answers: what changed, what data was used, what passed, what failed, what was waived (by whom, expiring when), which artifacts were produced, which sources fed each slide.

### Track L — Reusable Workflows + Composite Actions (v3 insert, Week 4)

Status: **NEW** (v3). Track A cut over the workflow; this modularizes it.

Files (new):

- `.github/workflows/reusable-source-backed-pipeline.yml` — `on: workflow_call`, accepts `snapshot_date`, `run_id`, `legacy_only`, `azure-secrets`.
- `.github/actions/setup-crm-analytics/action.yml` — composite: install deps, cache env, validate repo paths.
- `.github/actions/run-release-gates/action.yml` — composite: pytest + contract validators + deck validators.

Files (touched):

- `.github/workflows/monthly-review.yml` — collapses to a thin wrapper that calls `reusable-source-backed-pipeline.yml`.
- Future: PR / release / nightly workflows can call the same reusable core.

Goal: scheduled, manual, PR, and release workflows all funnel through one declared core lane — no copy-paste drift across workflow files.

## Mapping To GPT Pro's Top-10 ETL/Build Asks

| GPT Pro ask                                                       | Track                              |
| ----------------------------------------------------------------- | ---------------------------------- |
| ETL #1 — Cut over scheduled workflow                              | A                                  |
| ETL #2 — Calibrated source-quality baselines                      | C                                  |
| ETL #3 — Distribution checks for business-critical fields         | D                                  |
| ETL #4 — Separate policy actions                                  | B                                  |
| ETL #5 — Source owner/freshness promotion evidence                | (deferred to Week 2 — see 1-month) |
| Build #1 — `deck_contract.yaml` as single source of truth         | E                                  |
| Build #2 — Template-first builder                                 | F                                  |
| Build #3 — Drive table generation + validation from same contract | E                                  |
| Build #4 — Golden visual regression                               | (deferred to Week 4)               |
| Build #5 — Collapse builder set                                   | (deferred to Week 3)               |

## 1-Week Work Queue (Revised — v3 Sequence)

The original 7-day plan packed Tracks A–G into one week. The v3 vision inserts a warehouse + schemas layer between data and deck, so the week is rebalanced:

1. **Day 1 — Track A.** ✅ Done. Cron repointed, snapshot-date locked, legacy frozen.
2. **Day 2 — Track B.** ✅ Done. Per-axis policy actions in YAML + extractor.
3. **Day 3 — Track C.** ✅ Done. Read-only baseline calibrator, loader+comparator wired into extract audit, 55 v20c-derived baselines committed under `config/source_quality_baselines/`. Drift defaults to `info` severity; contracts opt up to `warning`/`blocked` via `RowCountPolicy.baseline_drift_action`.
4. **Day 4 — Track D.** ✅ Done. Per-dimension distribution audit (required-category presence, disappeared category, share drift, concentration drift) plus named slice sentinels. Hand-crafted negative-control fixtures cover all seven required scenarios. See [`docs/2026-04-26-track-d-distribution-audit-design.md`](./2026-04-26-track-d-distribution-audit-design.md).
5. **Day 5–6 — Tracks H + I.** DuckDB/Parquet warehouse (raw → staged → marts) + Pandera + Frictionless schemas + negative-control fixtures.
6. **Day 6 — Track E.** `deck_contract.yaml` schema + first slide spec; refactor table validator to consume it (now reads from marts, not bundle JSON).
7. **Day 7 — Track F + G.** Template-first builder, brand-inheritance gate, then v20d evidence pass and parity diff against v20c.

## 1-Month Plan (Revised — v3 Sequence)

| Week | Outcome                                                                                                                                                                                                                                                             |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | Tracks A ✅ + B ✅ + C + D + H + I + E + F + G. v20d signed off, with marts-driven deck and schema-validated dataframes.                                                                                                                                            |
| 2    | Track J — OpenLineage events emitted per stage; `lineage_index.json` + `slide_to_source_map.json` make slide-to-source traceability one query. ETL #5 — source owner / freshness / promotion-state added to registry.                                               |
| 3    | Track K — release catalog (`release_summary.md` + `.json`, `release_decision.json`, `waivers.json`, `artifact_index.json`) with full waiver enforcement. Deprecate `build_sales_director_monthly_shell.py`. Freeze `build_deck_from_excel.py` after one parity run. |
| 4    | Track L — reusable workflow + composite actions, so scheduled / manual / PR / release all funnel through one core lane. Golden visual regression harness (frozen vs dynamic regions). Optional: Evidence/Quarto QA cockpit reading the same marts.                  |

## Hard-Blocker / Advisory Gate Spec (per GPT Pro)

### Hard Blockers (release-stop)

1. Artifact completeness
2. Contract compliance (slide IDs, titles, tables, headers, named ranges, source notes match `deck_contract.yaml`)
3. Data binding (every metric → JSON path in truth/release packet)
4. Brand inheritance (template hash + theme tokens)
5. Layout integrity (no overflow, no placeholder remnants, footer present)
6. Render integrity (every slide → image/PDF success)
7. Executive readability (title, takeaway, action owner present where required)

### Advisory LLM Critique (non-blocking initially)

1. "Can a VP understand the action in 30 seconds?"
2. "Audit artifact vs decision surface?"
3. "Unexplained abbreviations?"
4. "False precision / unsupported confidence?"

Promote to blocker after the LLM critique stabilizes (low false-positive rate over 2–3 monthly cycles).

## Theater Audit Outcomes (per GPT Pro) → Closed By

| Gate                        | Verdict                  | Closing Track                     |
| --------------------------- | ------------------------ | --------------------------------- |
| Quarter mapping             | REAL                     | —                                 |
| Truth / tie-out             | REAL                     | —                                 |
| Salesforce extraction count | useful but incomplete    | C, D, ETL#5                       |
| Fingerprints                | useful but underpowered  | ETL#5 (owner/freshness/promotion) |
| Extract quality             | PARTLY THEATER           | B, C, D                           |
| Visual/polish/table gates   | smoke tests only         | E, F, golden-render (Week 4)      |
| Semantic score 100          | false comfort risk       | E (data-binding contract)         |
| Render gate                 | necessary not sufficient | F + golden-render (Week 4)        |
| Release artifacts/upload    | REAL for ops             | —                                 |

## First Files To Change (in order)

Track A ✅ + B ✅ already done; remaining work below.

1. `scripts/calibrate_source_quality_baselines.py` (new) — Track C
2. `config/source_quality_baselines/` (new dir + first JSONs from v20c) — Track C
3. `scripts/monthly_platform/source_quality_baselines.py` (new) — Track C
4. `scripts/monthly_platform/distribution_audit.py` (new) — Track D
5. `scripts/extract_salesforce_sources.py` — wire baselines + distribution checks (Tracks C + D)
6. `scripts/monthly_platform/warehouse.py` (new, DuckDB) — Track H
7. `scripts/build_source_backed_warehouse.py` (new) — Track H
8. `output/source_backed_warehouse/` (new directory schema) — Track H
9. `requirements.txt` — add `duckdb`, `pyarrow`, `pandera`, `frictionless` — Tracks H + I
10. `schemas/pandera/<table>.py` (new) — Track I
11. `schemas/table_schema/<table>.schema.json` (new) — Track I
12. `tests/fixtures/bad_extracts/` (new, 7 negative-control fixtures) — Track I
13. `scripts/monthly_platform/dataframe_contracts.py` (new) — Track I
14. `config/deck_contract.yaml` (new) — Track E
15. `scripts/monthly_platform/deck_contract.py` (new) — Track E
16. `scripts/validate_source_backed_deck_table_contract.py` — Track E refactor (read from contract; mart-bind via warehouse)
17. `scripts/build_source_backed_deck.py` — Tracks E + F (template-first, contract-driven, mart-sourced)
18. `scripts/build_thinkcell_source_from_bundles.py` — Track E refactor (named ranges from contract)
19. `scripts/run_source_backed_monthly_pipeline.py` — wire warehouse + lineage stages — Tracks H + J
20. `scripts/monthly_platform/lineage.py` (new, OpenLineage events) — Track J
21. `scripts/build_release_summary.py` (new) — Track K
22. `config/waivers/` (new dir + first waivers) — Track K
23. `scripts/monthly_platform/waivers.py` (new) — Track K
24. `.github/workflows/reusable-source-backed-pipeline.yml` (new) — Track L
25. `.github/actions/setup-crm-analytics/action.yml` (new composite) — Track L
26. `.github/actions/run-release-gates/action.yml` (new composite) — Track L

## Out Of Scope For This Cycle

- Power BI / Fabric replatform (already ruled out in v1).
- Switching the runner to Airflow / Prefect / Dagster (v3 confirms: not now).
- Migrating from calendar to fiscal quarter labels at the source registry (settled in v1; we ship business-period mapping as approved metadata).
- Replacing python-pptx.
- dbt + dbt tests / contracts (v3: maybe later, when transformations become SQL-model-heavy).
- MetricFlow / full semantic layer (v3: later, only when reusable metrics make hand-coded bindings painful).
- Evidence.dev / Quarto QA cockpit (v3: later, optional companion).
- MCP server for AI-assisted review (v3: optional, guarded; do not let an AI tool run extraction or upload).

## In Scope (v3 confirms — was previously out)

- DuckDB + Parquet warehouse (Track H) — local stable storage layer between extraction and consumers.
- Pandera + Frictionless Table Schema (Track I) — runtime + portable dataframe contracts.
- OpenLineage JSON events (Track J) — lightweight; no Marquez backend yet.
- Reusable workflows + composite actions (Track L) — modular CI without new orchestrator.
- Waiver system (Track K) — owner + expiry + allowed_runs enforced.

## Open Decisions

1. ~~Approval to modify `.github/workflows/monthly-review.yml`~~ — settled, Track A merged.
2. ~~Approval to mark legacy scripts as legacy-only~~ — settled in Track A (`legacy_only=true` opt-in).
3. Confirm v20d run-ID naming convention (proposed: `live-all-sources-pipeline-open-v20d`).
4. **NEW**: When does Track A+B merge to `main`? Currently on `codex/source-backed-v17-github-review`; the May 1 cron will fire from `main` and miss the cutover unless merged.
5. **NEW**: Are negative-control fixtures (Track I) generated from real v20c data or hand-crafted? Real-derived gives higher fidelity; hand-crafted gives clearer test intent.

## Strongest Single Recommendation (from GPT Pro, restated)

> Do the workflow cutover and quality-baseline work **before** touching more deck polish. The biggest hidden risk is not that the deck fails to render; it is that a green run can still be green for the wrong source slice.

## v3 North Star (refresher)

> Every source, transformation, metric, slide, table, chart, artifact, upload, and waiver should be declared in a contract, validated by code, linked by lineage, and reproducible from a snapshot date.

Each Track (A–L) maps to one slice of that sentence. See [`docs/2026-04-25-gpt-pro-v3-architecture-vision.md`](./2026-04-25-gpt-pro-v3-architecture-vision.md) for the end-state architecture diagram.
