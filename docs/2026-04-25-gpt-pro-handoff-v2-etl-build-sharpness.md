# GPT Pro Review v2 — SD Monthly ETL + Build Sharpness Pass

Date: 2026-04-25
Repo: `/Users/test/crm-analytics`
Branch: `codex/source-backed-v17-github-review`
Live baseline: `live-all-sources-pipeline-open-v20c` (snapshot `2026-04-30`)
Prior review: [`docs/2026-04-24-gpt55-architecture-review-brief.md`](./2026-04-24-gpt55-architecture-review-brief.md) (context only — do not re-litigate)
Prior plan: [`docs/2026-04-24-gpt55-feedback-implementation-plan.md`](./2026-04-24-gpt55-feedback-implementation-plan.md)

## 0. Why This Brief Exists (1 paragraph)

The architecture review was already done (v1 brief, 2026-04-24). Tracks 1-3 of the response are merged and proven green at v20c. This second pass is **narrower and sharper**: review the ETL and the build, point at specific code, and tell us what to harden, simplify, or kill. We are **not** asking for another architecture redesign, another orchestration-engine debate, or another "consider Power BI" suggestion. We want a polish pass against the live source-backed lane.

## 1. Paste-Ready Prompt (for GPT Pro)

> You are reviewing a **specific slice** of a Salesforce → Excel → PowerPoint reporting pipeline that produces 9 monthly Sales Director decks. The architecture has already been reviewed once and the response is partially implemented. Your job is to sharpen the **ETL** (Salesforce extraction, source contracts, source bundles, period semantics) and the **build** (analyst workbook, think-cell source, deck generation, validator gates).
>
> Read this brief plus the reference files listed in §7. Then return: (a) the top 5 specific ETL code changes, (b) the top 5 specific build code changes, (c) a "polished deck" rubric we can encode into a deterministic gate, (d) a 1-week and 1-month action plan, and (e) honest calls on which existing gates are theater vs. real.
>
> Be direct. Cite file:line. Avoid restating the architecture. Do not propose Airflow, Prefect, Dagster, Power BI, Fabric, or any cloud replatform — those are out of scope for this pass.

## 2. What Changed Since v1 (4 Commits)

```text
021dad0 Add extraction quality audit gate            ← Track 3 phase 1
f3bfa7a Add Salesforce source fingerprint preflight  ← Track 2 phase 1
80f17b9 Add quarter mapping release gate             ← Track 1 phase 1
aadf434 Publish source-backed monthly review platform← public surface
```

That is the entire delta from the v1 review baseline. Everything below is what is **still open**.

## 3. Current Proof — v20c Gate Summary

From `output/source_backed_monthly_pipeline_runs/latest.md`:

| Gate                                                 | Result                                                                   |
| ---------------------------------------------------- | ------------------------------------------------------------------------ |
| Stages                                               | 24 / 24                                                                  |
| YAML authoring sync                                  | 3 targets, 0 drift                                                       |
| Salesforce sources                                   | 55 / 55 selected, 55 / 55 extracted                                      |
| Source fingerprint findings                          | 0 high                                                                   |
| Extract quality findings                             | 0 high, 1 allowed fallback warning                                       |
| Source bundles → DirectorBundles                     | 9 → 9                                                                    |
| Quarter mapping                                      | business `FY26 Q1` → source `Q2 2026` → display `Q2 2026`, approved=True |
| Truth blockers / tie-out mismatches                  | 0 / 0                                                                    |
| Visual / polish / table / semantic / render findings | 0 / 0 / 0 / 0 / 0                                                        |
| Semantic score                                       | 100                                                                      |
| Rendered slides                                      | 6                                                                        |
| Release artifacts                                    | 23, 0 missing                                                            |
| SharePoint upload                                    | 5 planned, 5 uploaded, 0 skipped                                         |

Translation: every deterministic gate the system currently knows how to ask is green. **That is exactly the kind of result we trust GPT Pro to be skeptical of.**

## 4. The Five Open Sharpness Tracks

Each track has a **claim**, a **target file or files**, and a **review question**. These are the only places we want GPT Pro to attack in this pass.

### 4.1 ETL — Source Registry Governance Beyond Fingerprints

**What we did (Track 2 phase 1):** pre-extraction `source_fingerprint_manifest.json` captures org, list-view/report ID, label, owner, columns, filters, query hash for all 55 sources. Hash drift fails the run.

**What is still missing:**

- Row-bound expectations are warn-only (Track 3 phase 1 emits `1` allowed fallback warning, not a fail).
- No SLA on source freshness ("modified within N days of snapshot").
- No registry promotion workflow when a quarter rolls (Q2 → Q3 list views): today this is manual config edits.
- No "owner notification" if a fingerprint changes — silent humans, loud system.

**Files:**

- `scripts/extract_salesforce_sources.py`
- `scripts/monthly_platform/salesforce_reports.py`
- `scripts/monthly_platform/storage.py`
- `config/sd_monthly_territories.json` (manual list-view ID registry, 9 territories × 4-7 IDs each)

**Review question:** Where on the spectrum between "config-only edits" and "self-healing source registry" should this live for a 9-territory monthly cadence run by one operator? What is the smallest change that gets us a credible source-promotion process for Q3 2026 → Q4 2026 → FY27 Q1?

### 4.2 ETL — Period Semantics Still Split Across Two Lanes

**Source-backed lane (canonical, v20c green):** uses `scripts/monthly_platform/period.py` + `scripts/build_monthly_source_contract.py` + the new quarter-mapping release gate. Calendar quarter is locked; fiscal mapping carried as approved metadata.

**Legacy lane (still wired to GitHub Actions cron):** `scripts/run_monthly_director_review.py` → `scripts/build_deck_from_excel.py`. Period state is computed at import:

- `scripts/build_deck_from_excel.py:89` — `token = str(report_date or datetime.now().date())[:10]`
- `scripts/build_deck_from_excel.py:127` — `report_date = datetime.now().strftime("%Y-%m-%d")`
- `scripts/build_deck_from_excel.py:4084` — output path uses `datetime.now().year` interpolated at runtime

**Risk:** any contributor running the legacy lane on a different day than the snapshot date silently shifts business scope. The audit doc [`2026-04-22-sd-monthly-enterprise-etl-handoff.md`](./2026-04-22-sd-monthly-enterprise-etl-handoff.md) §2 names this; it's still live.

**Review question:** What is the cleanest cut-over plan from the legacy lane to source-backed? Concretely — should we (a) delete the legacy lane now that v20c is publish-clean, (b) freeze it as `legacy/` and forward-only-bug-fix it, or (c) keep both with a mandatory parity gate? We don't want to maintain a 4,129-line `build_deck_from_excel.py` forever, but it has 8+ months of business trust on it.

### 4.3 ETL — Extraction Quality Audit Needs Teeth

**Current behavior:** `source_extract_quality_audit.json` checks required fields, row-count zero/min/max, null thresholds, max-record caps. Today it has 0 high findings — likely because thresholds are conservative.

**What we don't know:**

- Are the row-count bounds calibrated against historical truth, or were they set to whatever didn't fail v20c?
- Does the audit catch the failure mode where a Salesforce filter silently includes/excludes a stage? (Smells like no — only describe-time fingerprint catches filter changes, and only against the **last** run.)
- Optional-empty datasets (`won_lost`, `renewals`, `approvals`, `activity`, `commit_items`, `stage_events`, `forecast_category_events`, `close_date_events`, `movement_prior`, `movement_current`) — when do they become "missing" instead of "optionally empty"?

**Files:**

- `scripts/extract_salesforce_sources.py`
- `output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v20c/audits/source_extract_quality_audit.json` (live evidence, ~~3 KB)
- `config/monthly_director_bundle_contract.json` (defines optional-empty policy)

**Review question:** Define a row-count and null-rate calibration policy that is honest, not theatrical. Where should historical baselines be stored, how should they be updated, and what is the smallest gate that would catch a "filter silently dropped Stage 5 deals" regression?

### 4.4 Build — Presentation Contract Is Implicit

**Today:** the deck is built by `scripts/build_source_backed_deck.py` (931 LOC). Validators check visuals, tables, semantics, render. They pass. But there is **no externalized contract** that says "slide 3 must contain table T2 with columns C1-C5 sourced from metric M9." The contract is encoded inside the validator code, which is encoded inside the builder.

**Symptoms:**

- A semantic score of 100 with `0` findings is not the same as "executive-grade." It is "no rule the validator knows how to ask was violated."
- Adding a new slide requires editing both the builder and at least 4 validators.
- think-cell source (`thinkcell_source.xlsx` + `.ppttc`) is generated by a parallel script (`scripts/build_thinkcell_source_from_bundles.py`) with no shared component contract. They could drift silently.

**Files:**

- `scripts/build_source_backed_deck.py`
- `scripts/polish_source_backed_deck_language.py`
- `scripts/validate_source_backed_deck_visuals.py`
- `scripts/validate_source_backed_deck_table_contract.py`
- `scripts/validate_source_backed_deck_semantics.py`
- `scripts/validate_source_backed_deck_render.py`
- `scripts/build_thinkcell_source_from_bundles.py`
- `scripts/monthly_platform/thinkcell_source.py`

**Review question:** Specify a `deck_contract.yaml` (slide IDs, shape/table IDs, named ranges, required metric IDs, formatting tokens, numeric formats, render expectations). It should drive both the builder and the validators, and it should be the single thing think-cell and python-pptx both project from. Give us the schema and one full slide spec.

### 4.5 Build — Director Render Branding & The Two-Builder Problem

**Issue 1 — branding drift:** [`docs/2026-04-22-sd-monthly-enterprise-etl-handoff.md`](./2026-04-22-sd-monthly-enterprise-etl-handoff.md) §1 reports that `scripts/build_sales_director_monthly_shell.py` (222 LOC, cadence-lane render) does not faithfully inherit the SimCorp master template — it implements its own palette, font, and table styling. The legacy `scripts/build_deck_from_excel.py` does inherit, but is the 4,129-LOC monolith we're trying to retire.

**Issue 2 — two builders:** the source-backed lane currently uses `scripts/build_source_backed_deck.py` (931 LOC) for the canonical 6-slide deck, but `build_sales_director_monthly_shell.py` is still around for the per-director cadence-lane render. If both stay, we have three deck builders (counting legacy). If only one stays, which one and why?

**Files:**

- `scripts/build_source_backed_deck.py` (canonical, source-backed)
- `scripts/build_sales_director_monthly_shell.py` (cadence-lane, branding-drift suspect)
- `scripts/build_deck_from_excel.py` (legacy, true template inheritance, 4129 LOC)
- `assets/SimCorp_PPT_Template.pptx`

**Review question:** Recommend the single deck builder to keep, the template-inheritance pattern that guarantees brand fidelity (palette + font + table style), and the deprecation order for the other two. Bonus: a deterministic visual regression check against a golden render of the canonical SimCorp first slide.

## 5. Cross-Cutting Asks (Lower Priority, Address If Time)

These are real, but secondary to §4. Skip if you're running short.

| #   | Ask                                                                                                                                                                                                                                                            | Files                                                                                                                                      |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| 5.1 | **Runner — minimal DAG without Airflow.** 24-stage procedural script needs `--resume`, `--from-stage`, `--use-cached-extracts`, stage-level retry, and declared inputs/outputs without adopting a workflow framework. Track 5 from v1.                         | `scripts/run_source_backed_monthly_pipeline.py`                                                                                            |
| 5.2 | **Metric store — DuckDB between bundles and outputs.** Today, every output script re-derives metrics from DirectorBundles. A canonical metric layer would let truth packet, workbook, think-cell, and deck all project from the same numbers. Track 6 from v1. | `scripts/build_director_gold_analytics.py`, `scripts/monthly_platform/analyst_workbook.py`, `scripts/monthly_platform/thinkcell_source.py` |
| 5.3 | **Release catalog + `release_summary.md`.** Add an operator-facing one-page release summary that lists run ID, status, sources, gate failures, SharePoint URLs, and a manual-review checklist. Track 8 from v1.                                                | `scripts/build_source_backed_release_bundle.py`                                                                                            |
| 5.4 | **YAML config split.** The single `config/source_contracts/sales_director_monthly.yaml` should split into modular YAML (territories/, requirements/, field_packs/) and compile to JSON. Track 4 from v1.                                                       | `config/source_contracts/sales_director_monthly.yaml`, `scripts/compile_monthly_source_contract_config.py`                                 |

## 6. Deliverable Shape (What We Want Back)

For each of the five §4 tracks, return:

1. **Verdict** — keep, sharpen, replace, delete.
2. **Concrete code changes** — name the file, name the function, give the patch concept (not full diffs unless small).
3. **Risk before / risk after** — what failure mode you're closing.
4. **Cost** — small / medium / large.

Then a synthesis:

5. **Top 5 ETL code changes** ordered by ROI.
6. **Top 5 build code changes** ordered by ROI.
7. **"Polished deck" rubric** — encode it as a checklist that could become a deterministic validator and an LLM-side critique.
8. **1-week action plan** — what one operator can ship in 7 days.
9. **1-month action plan** — what we should aim for in 30 days.
10. **Theater audit** — for each existing gate (visuals / polish / table contract / semantics / render / fingerprints / extract quality / quarter mapping), one sentence: real signal or false comfort, and how to know.

## 7. Reference Files (curated, short)

Read these in this order. Anything else can be inferred.

```text
README.md                                                        ← repo top (if present)
CLAUDE.md                                                        ← operating rules
docs/architecture.md                                             ← lane map
docs/2026-04-22-sd-monthly-enterprise-etl-handoff.md             ← legacy-lane tech debt
docs/2026-04-24-gpt55-architecture-review-brief.md               ← v1 brief (do not re-litigate)
docs/2026-04-24-gpt55-feedback-implementation-plan.md            ← v1 response, what's done
output/source_backed_monthly_pipeline_runs/latest.md             ← v20c proof
output/source_backed_monthly_pipeline_runs/latest.json           ← v20c machine truth
config/source_contracts/sales_director_monthly.yaml              ← authoring layer
config/sd_monthly_territories.json                               ← 9-territory source registry
config/monthly_source_requirements.json                          ← source requirement spec
config/monthly_director_bundle_contract.json                     ← bundle policy
scripts/run_source_backed_monthly_pipeline.py                    ← canonical runner (24 stages)
scripts/extract_salesforce_sources.py                            ← ETL entrypoint
scripts/monthly_platform/period.py                               ← period resolver + quarter policy
scripts/monthly_platform/salesforce_reports.py                   ← SF API surface
scripts/monthly_platform/director_bundle_builder.py              ← bundle assembly
scripts/build_source_backed_analyst_workbook.py                  ← Excel build entrypoint
scripts/build_thinkcell_source_from_bundles.py                   ← think-cell payload
scripts/build_source_backed_deck.py                              ← canonical deck builder (931 LOC)
scripts/validate_source_backed_deck_table_contract.py            ← table contract validator
scripts/build_source_backed_release_bundle.py                    ← release packaging
scripts/upload_sales_deck_release_to_sharepoint.py               ← SharePoint upload
```

Tests (skim if you want to evaluate test quality, not exhaustively):

```text
tests/test_compile_monthly_source_contract_config.py
tests/test_run_source_backed_monthly_pipeline.py
tests/test_build_monthly_source_contract.py
tests/test_validate_source_backed_deck_table_contract.py
tests/test_validate_source_backed_deck_render.py
```

Live artifacts to inspect (real evidence, not docs):

```text
output/source_backed_monthly_pipeline_runs/2026-04-30/live-all-sources-pipeline-open-v20c/pipeline_run_manifest.json
output/monthly_review_release_packets/2026-04-30/live-all-sources-pipeline-open-v20c/source_backed_release_packet.json
output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v20c/source_backed_monthly_review.pptx
output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v20c/source_backed_analyst_workbook.xlsx
output/thinkcell_source_from_bundles/2026-04-30/live-all-sources-pipeline-open-v20c/thinkcell_source.xlsx
output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v20c/audits/source_extract_quality_audit.json
```

## 8. Out Of Scope (Do Not Re-Litigate)

GPT Pro should **skip** these. They were settled in v1 or are explicitly off the table for this pass.

- Replacing python-pptx with another deck library.
- Replacing the procedural runner with Airflow / Prefect / Dagster.
- Moving to Power BI, Fabric, or any cloud BI.
- Moving Excel calculation to Office Scripts / Excel Online.
- Adopting a relational data warehouse upstream of Salesforce.
- Re-arguing whether the SimCorp deck should look different.
- Whether to migrate from calendar-quarter to fiscal-quarter labels in production (decision: stay calendar-locked at the source registry, expose fiscal as approved business-period metadata; v20c implements this).

## 9. Constraints That Survived Round One

- Target Salesforce org: `apro@simcorp.com` / `simcorp.my.salesforce.com` / API v66.0.
- Auth: `sf` CLI only — no `.env`, no service principal in this lane (Microsoft Graph upload uses GitHub Actions secrets only).
- Output surface: PowerPoint + Excel + think-cell + SharePoint. No Power BI.
- Operator: one person, monthly cadence, must be runnable cold after a 30-day gap.
- Local-first: prefer DuckDB over Snowflake, prefer python-pptx over Office Scripts.

## 10. Bottom Line For The Reviewer

The v1 review told us our architecture was directionally right and gave us 8 tracks. We did the safety-critical 3 and proved them green at v20c. We are now in the **polish** phase: every remaining track is about making the system harder to break, easier to extend, and more obviously trustworthy without adding complexity that one operator can't run.

The system passes its own gates. **Tell us where the gates are wrong.**
