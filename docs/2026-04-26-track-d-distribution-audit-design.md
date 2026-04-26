# Track D — Distribution Audit Design

**Date:** 2026-04-26
**Branch:** `codex/track-d-distribution-audit`
**Plan reference:** [`docs/2026-04-25-gpt-pro-feedback-implementation-plan.md`](./2026-04-25-gpt-pro-feedback-implementation-plan.md) §Track D

## Why Track D Exists

Row counts and extract success do not prove the right business slice survived.

Tracks A and B make sure we run the source-backed pipeline and that each per-axis policy
violation (zero-row, min-rows, max-rows, max-records, null-rate) is severity-tagged honestly.
Track C adds calibrated row-count and null-rate baselines so a quietly shrinking source
fires drift findings. None of those checks notice the original GPT Pro v2 hidden-risk example:

> A Salesforce report can extract successfully, satisfy Track B row-count policies, and
> match the Track C row-count baseline while a specific stage / quarter / territory / owner
> segment silently drops to zero rows.

Track D adds the missing axis: **distribution coverage**. The audit notices when the
share of a category materially shifts versus a calibrated seed, when a category that
existed in the seed disappears entirely, when a contract-required category produces
zero rows, or when one owner / territory / director suddenly dominates the source.

## Failure Mode Track D Protects Against

- "Stage 5 deals quietly disappeared." A `WHERE` clause typo or a Salesforce report-filter
  edit drops one category but row counts elsewhere absorb the slack. Old gates pass.
- "Japan territory disappeared." A territory rename or scope edit removes a region from
  the slice but the report still extracts, just smaller.
- "All deals are owned by one rep now." An accidental owner-filter collapse reduces
  the source to a single owner; total row count looks plausible because the others were
  always small.
- "Q2 forecast disappeared just before quarter close." A close-date filter shifts to a
  different quarter and the deck shows the wrong period without a single row-count gate
  firing.

## What Track D Does

For each source the contract opts in (via `distribution_policy` on
`SourceRequirement`), the audit evaluates four independent axes per declared dimension
plus an optional named-sentinel layer:

| Axis                       | Default action | Inputs needed       | Catches                                           |
| -------------------------- | -------------- | ------------------- | ------------------------------------------------- |
| Required-category presence | `warning`      | contract list       | A category the contract names is empty.           |
| Disappeared category       | `warning`      | seed shares         | A seed-observed category is now empty.            |
| Share drift                | `info`         | seed shares + delta | Any category share moved > `max_abs_share_delta`. |
| Concentration drift        | `ok` (off)     | top share threshold | Top-1 category share exceeds the cap.             |
| Slice sentinel             | per-sentinel   | (field, category)   | Named guardrail (e.g. `stage_5_presence`).        |

Each axis has its own action so a contract can e.g. **block** on a vanished stage but
only **warn** on share drift. Severities follow the established mapping —
`ok` → no finding, `info` → `info`, `warning` → `medium`, `blocked` → `high`. The
existing extract-stage gate already escalates run status to `blocked` on any `high`
finding, so contract opt-up is the single switch operators flip when calibration ends.

Each per-source audit emits a payload that includes the top-N categories by count and
share so a reviewer can answer "what changed?" without opening the raw extract.

### Slice sentinels — named guardrails for high-signal failure modes

A sentinel is a thin wrapper around the same (field, category) presence check the main
loop already runs. Its only job is to give the most expensive failure modes a friendly
ID so they show up as `stage_5_presence sentinel failed` in audit evidence instead of
a generic `source_distribution_required_category_missing`. The first sentinel ships
with the `sd_pipeline_open` contract:

```yaml
slice_sentinels:
  - id: stage_5_presence
    field: StageName
    category: "5 - Negotiating"
    action: warning
    reason: "Stage 5 disappearance is a high-signal accidental filter/scope failure."
```

## What Track D Does NOT Do

Track D is intentionally narrow:

- It does **not** create DuckDB / Parquet marts. That is Track H.
- It does **not** alter deck generation, slide content, or template loading.
  Tracks E (`deck_contract.yaml`) and F (template-first builder) are separate.
- It does **not** compute new release visuals, dashboards, or PowerPoint output.
- It does **not** introduce OpenLineage events (Track J) or release waivers (Track K).
- It does **not** rewrite the source contract YAML — `distribution_policy` is an
  additive optional field on `SourceRequirement`.
- It does **not** auto-promote distribution seeds; like Track C, seed promotion is
  hand-driven and read-only at runtime. (A future calibrator script will mirror
  `scripts/calibrate_source_quality_baselines.py`.)

## Module Layout

```
scripts/monthly_platform/source_requirements.py
  + DistributionAction          (Literal type for policy actions)
  + DimensionPolicy             (per-dimension policy with 4 independent action axes)
  + SliceSentinel               (named (field, category) guardrail)
  + DistributionPolicy          (container; lives on SourceRequirement)
  + distribution_action_to_severity()

scripts/monthly_platform/source_distribution_audit.py
  + DimensionSeed               (calibrated category shares for one dimension)
  + SourceDistributionSeed      (per (requirement, territory, period_role) seed)
  + load_distribution_seeds()   (reads config/source_distribution_baselines/)
  + baseline_key_for_item()     (mirrors Track C's key shape)
  + audit_distribution()        (pure per-source audit; never mutates inputs)
  + compare_run_distributions() (run-level summary)

scripts/extract_salesforce_sources.py
  + per-source audit_distribution() call inside the existing extraction loop
  + run-level distribution_comparison block injected into the audit JSON
  + CLI flags: --distribution-seeds-dir, --no-distribution

config/source_distribution_baselines/<baseline_key>.json
  Hand-promoted seeds, one per (requirement_id, territory, period_role).

tests/fixtures/source_distribution/
  Hand-crafted negative-control fixtures (one failure mode per file).

tests/test_source_distribution_audit.py
  21 tests covering the 7 required scenarios plus action-mapping plumbing,
  comparator purity, dotted-field extraction, and the run-level summary.
```

## Hand-Crafted Negative-Control Fixtures

Per the GPT Pro v2 review, Track I (Pandera + JSON Schema dataframes) will use real
v20c-derived fixtures later. Track D deliberately starts with hand-crafted minimal
extracts (20 rows each) so each scenario isolates exactly one failure mode and the
share arithmetic is obvious by inspection.

| Fixture file                          | Scenario it isolates                                        |
| ------------------------------------- | ----------------------------------------------------------- |
| `rows_normal_stage_mix.json`          | Control: current matches seed within `max_abs_share_delta`. |
| `rows_stage_5_missing.json`           | Stage 5 seen in seed → 0 rows now.                          |
| `rows_territory_dropped.json`         | "Japan" disappeared from territory mix.                     |
| `rows_quarter_missing.json`           | Required `Q2` produced 0 rows.                              |
| `rows_owner_concentration_spike.json` | Top owner share = 0.70 > `max_top_category_share=0.60`.     |

Two scenarios reuse fixtures with different policy / seed inputs:

- **missing_distribution_seed** — `rows_normal_stage_mix.json` with `seed=None`.
  Seed-dependent axes (disappeared, share drift) skip; required-category and
  concentration axes still evaluate. Net result: zero findings, proving absence of
  a seed is not itself a release blocker.
- **contract_opt_up_blocked** — `rows_stage_5_missing.json` with the contract's
  `disappeared_category_action="blocked"`. The same data that defaults to `medium`
  severity now produces `high`, escalating the run status to `blocked`.

## Acceptance Criteria

- [x] Track D catches missing Stage 5 even when total row count remains acceptable.
- [x] Distribution findings appear in extract quality audit output (`baseline_comparison`
      and `distribution_comparison` are siblings under the audit JSON).
- [x] Default severity is non-blocking unless source contract opts up.
- [x] Contract override can make category disappearance blocking.
- [x] All seven required scenarios have explicit tests.
- [x] No DuckDB / Parquet, deck-contract, or template work in this PR.
- [x] Track A+B+C+D + adjacent regression: 91 passed, ruff clean on Track D files.

## Where Track D Sits In The Sequence

Track D extends Track C. Track C added the row-count and null-rate baseline lane;
Track D adds the per-dimension distribution lane that catches what row-count drift
cannot. Together they answer two distinct questions about every source:

- **Track C — "is the volume right?"** Calibrated row-count envelope plus null-rate
  ceiling per required field.
- **Track D — "is the slice right?"** Required-category presence, disappeared
  categories, share drift, concentration, named slice sentinels.

Tracks H (DuckDB warehouse) and I (Pandera + Frictionless schemas) follow next, but
both are deliberately out of scope for this PR. The audit JSON under
`output/.../audits/source_extract_quality_audit.json` is the only artifact Track D
needs to land.
