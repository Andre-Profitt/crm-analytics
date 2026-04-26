# GPT Pro Feedback Implementation Plan — v2 Cycle

Date: 2026-04-25
Baseline reviewed: `live-all-sources-pipeline-open-v20c` (snapshot `2026-04-30`)
GPT Pro response (verbatim): [`docs/2026-04-25-gpt-pro-review-response.md`](./2026-04-25-gpt-pro-review-response.md)
Brief that triggered review: [`docs/2026-04-25-gpt-pro-handoff-v2-etl-build-sharpness.md`](./2026-04-25-gpt-pro-handoff-v2-etl-build-sharpness.md)
Prior cycle plan (for context): [`docs/2026-04-24-gpt55-feedback-implementation-plan.md`](./2026-04-24-gpt55-feedback-implementation-plan.md)

## Verdict

**Keep the source-backed lane. Harden it. Do not redesign.**

GPT Pro confirms v20c is publish-green but not change-safe. The largest hidden risk is not that the deck fails to render — it is that a green run can still be green for the wrong source slice. Two of the most damaging gaps are also the cheapest to close: the scheduled GitHub Actions cron still points at the legacy lane, and the source-backed deck builder doesn't actually load the SimCorp template.

## Highest-Risk Gaps (Top 3)

1. **Scheduled cron on the wrong lane.** `.github/workflows/monthly-review.yml` invokes `scripts/run_monthly_director_review.py` (legacy `datetime.now()`-based path). Anyone re-running the workflow on a different day silently shifts the business scope.
2. **Extract quality "0 high" is partly theater.** `min_rows=0`, `allow_zero=True`, no historical baselines, no distribution checks. A filter could silently drop a stage and the gate would still pass.
3. **Source-backed builder is not template-first.** `Presentation()` (no-arg) plus hard-coded RGB constants. Brand fidelity is incidental, not enforced.

## Implementation Tracks

### Track A — Cron Cutover To Source-Backed Runner (Day 1)

Status: **NOT STARTED**. Lowest cost, highest priority per GPT Pro.

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

Status: **NOT STARTED**. Cheap, unblocks Track C + D.

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

## 1-Week Work Queue (Day-By-Day)

1. **Day 1 — Track A.** Repoint cron, lock snapshot-date input, freeze legacy path.
2. **Day 2 — Track B.** Policy action separation in YAML + extractor.
3. **Day 3 — Track C.** Baseline calibrator, read-only mode, baseline JSONs from v20c.
4. **Day 4 — Track D.** Distribution audit module wired into extract path.
5. **Day 5 — Track E.** `deck_contract.yaml` schema + first slide spec; refactor table validator to consume it.
6. **Day 6 — Track F.** Template-first builder; brand-inheritance gate; remove hard-coded RGB.
7. **Day 7 — Track G.** v20d evidence pass and parity diff against v20c.

## 1-Month Plan

| Week | Outcome                                                                                                                                                                                                                            |
| ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | Tracks A–G complete; v20d signed off.                                                                                                                                                                                              |
| 2    | Refactor builder + all validators to fully consume `deck_contract.yaml`. Add think-cell named-range generation from contract. ETL #5 — source owner / freshness / promotion-state added to registry.                               |
| 3    | Deprecate `scripts/build_sales_director_monthly_shell.py` (cadence shell). Freeze `scripts/build_deck_from_excel.py` after one parity run vs source-backed v20d.                                                                   |
| 4    | Golden visual regression harness (render-to-image + diff vs approved baseline). Source promotion approvals + owner notifications wired in. `release_summary.md` catalog: what changed, what passed, what was waived, who approved. |

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

1. `.github/workflows/monthly-review.yml` — Track A
2. `scripts/run_source_backed_monthly_pipeline.py` — Track A (CLI args)
3. `tests/test_monthly_workflow_cutover.py` (new) — Track A
4. `config/source_contracts/sales_director_monthly.yaml` — Track B
5. `scripts/monthly_platform/source_requirements.py` — Track B
6. `scripts/extract_salesforce_sources.py` — Tracks B + D
7. `scripts/calibrate_source_quality_baselines.py` (new) — Track C
8. `config/source_quality_baselines/` (new dir + first JSONs) — Track C
9. `scripts/monthly_platform/source_quality_baselines.py` (new) — Track C
10. `scripts/monthly_platform/distribution_audit.py` (new) — Track D
11. `config/deck_contract.yaml` (new) — Track E
12. `scripts/monthly_platform/deck_contract.py` (new) — Track E
13. `scripts/validate_source_backed_deck_table_contract.py` — Track E refactor
14. `scripts/build_source_backed_deck.py` — Tracks E + F
15. `scripts/build_thinkcell_source_from_bundles.py` — Track E refactor

## Out Of Scope For This Cycle

- Power BI / Fabric replatform (already ruled out in v1).
- Switching the runner to Airflow / Prefect / Dagster (already ruled out in v1).
- Migrating from calendar to fiscal quarter labels at the source registry (settled in v1; we ship business-period mapping as approved metadata).
- Replacing python-pptx.

## Open Decisions Needed Before Day 1 Starts

1. Approval to modify `.github/workflows/monthly-review.yml` (production scheduler).
2. Approval to mark `scripts/run_monthly_director_review.py` and `scripts/build_deck_from_excel.py` as legacy-only (no functional removal yet).
3. Confirm v20d run-ID naming convention (proposed: `live-all-sources-pipeline-open-v20d`).

## Strongest Single Recommendation (from GPT Pro, restated)

> Do the workflow cutover and quality-baseline work **before** touching more deck polish. The biggest hidden risk is not that the deck fails to render; it is that a green run can still be green for the wrong source slice.
