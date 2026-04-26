# v20d ETL spine checkpoint — 2026-04-26

A frozen evidence snapshot of the source-backed monthly review platform's
ETL spine, taken right before Track I (Pandera + JSON Schema dataframe
contracts) work begins. Per GPT Pro's plan: Track I schemas should be
written against the _actual_ warehouse table shapes and parity behavior,
so a quick ETL evidence checkpoint will expose schema gaps before
they're frozen into Pandera/Frictionless.

## Scope

ETL spine only. **Not** a Track G v20d evidence pass (which covers deck +
template + release semantics). This is the data-layer baseline.

| Track                            | Included? | Notes                                                       |
| -------------------------------- | --------- | ----------------------------------------------------------- |
| A (cron cutover)                 | ✅        | Snapshot date, run id discipline already on main            |
| B (per-axis policy actions)      | ✅        | row_count_policy / required_field_null_action               |
| C (baselines)                    | ✅        | 55 v20c-derived baselines under config/                     |
| D (distribution audit framework) | ✅        | quality_audit.distribution_comparison block                 |
| D activation                     | ✅        | sd_pipeline_open opt-in + 9 calibrated seeds                |
| H (warehouse)                    | ✅        | 7 Parquet tables with typed schema (BIGINT/BOOLEAN/VARCHAR) |
| I (schemas)                      | ❌        | This checkpoint is the input for Track I                    |
| E (deck contract)                | ❌        | Deferred                                                    |
| F (template-first builder)       | ❌        | Deferred                                                    |
| G (v20d evidence pass)           | ❌        | Separate, deck-layer scope                                  |
| J (OpenLineage)                  | ❌        | Deferred                                                    |
| K (waiver system)                | ❌        | Deferred                                                    |
| L (reusable workflows)           | ❌        | Deferred                                                    |

## Source evidence

The checkpoint warehouse was built from the latest approved live-evidence
run on disk:

- **Snapshot date:** 2026-04-30
- **Run id:** `live-all-sources-pipeline-open-v20c`
- **Source plan:** `output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v20c/plans/source_requirement_plan.json`
- **Quality audit:** `output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v20c/audits/source_extract_quality_audit.json`
- **Contract registry:** `config/monthly_source_requirements.json` (head of main)

## How to reproduce

```sh
python3 scripts/build_source_backed_warehouse.py \
    --snapshot-date 2026-04-30 \
    --run-id live-all-sources-pipeline-open-v20c \
    --warehouse-root docs/checkpoints/v20d-etl-spine-checkpoint-2026-04-26/warehouse
```

Exit code 0; parity report status `pass` with 8/8 checks green.

## Frozen artifacts

```
docs/checkpoints/v20d-etl-spine-checkpoint-2026-04-26/
├── CHECKPOINT.md                  ← this file
├── observed_schemas.json          ← per-table column types + row counts
└── warehouse/
    └── 2026-04-30/
        └── live-all-sources-pipeline-open-v20c/
            ├── warehouse_manifest.json    ← sha256s + row counts + columns
            ├── parity_report.json         ← input vs warehouse row-count parity
            ├── raw/
            │   ├── salesforce_extract_plan.parquet      (55 rows)
            │   └── source_quality_audit.parquet         (55 rows)
            ├── staged/
            │   ├── source_requirements.parquet          (4 rows)
            │   ├── source_quality_findings.parquet      (1 row)
            │   └── distribution_findings.parquet        (0 rows)
            └── marts/
                ├── director_source_health.parquet       (10 rows)
                └── source_run_summary.parquet           (1 row)
```

## Parity summary

All 8 row-count parity checks pass:

```
✓ raw_salesforce_extract_plan vs source_requirement_plan.items:    55 == 55
✓ raw_source_quality_audit    vs audit.sources:                    55 == 55
✓ staged_source_requirements  vs registry.requirements:             4 ==  4
✓ staged_source_quality_findings vs non-distribution findings:      1 ==  1
✓ staged_distribution_findings   vs distribution_* findings:        0 ==  0
✓ all-track findings           vs audit.findings:                   1 ==  1
✓ mart_director_source_health  rows == distinct directors:         10 == 10
✓ mart_source_run_summary      rows == 1:                           1 ==  1
```

## What this unlocks for Track I

Track I (Pandera + JSON Schema dataframe contracts) needs concrete table
shapes to write schemas against. With this checkpoint:

1. **Pandera schemas** — one per table_id, anchored to the column names
   and DuckDB types in `observed_schemas.json`. Use `pa.Column(int,
nullable=...)`, `pa.Column(str, ...)`, `pa.Column(bool, ...)` to
   mirror the BIGINT / VARCHAR / BOOLEAN typed schema the warehouse now
   emits (post Codex P2 fix on PR #6).
2. **Frictionless / JSON Schema** — portable artifact contracts that
   ride alongside each Parquet file. Same column shapes, expressed in a
   tool-agnostic format so non-Python consumers (BI tooling, Excel
   ingestion, future deck contract) can validate without depending on
   pandas.
3. **Negative-control fixtures** — Track I should ship hand-crafted
   fixtures that _fail_ validation (one per intentional defect: wrong
   column type, missing required column, null in required field, etc.)
   following the pattern Track D used. The seven warehouse table_ids
   give Track I seven natural fixture buckets.

## Known data peculiarities at this checkpoint

These aren't bugs — they're real-world signal Track I should accommodate:

- **`staged_distribution_findings.parquet` has 0 rows** because the v20c
  audit ran before the Track D opt-in for `sd_pipeline_open`. Once the
  next monthly run lands, the table will have real per-source-per-axis
  rows and Track I should validate against both shapes.
- **`staged_source_quality_findings.parquet` has 1 row** — a single
  warning from one source (Pension & Insurance current-quarter
  pipeline_open had zero rows, expected for that territory). Track I
  schema should accept zero or many rows; severity / issue / evidence
  / track / owner are the required columns.
- **`mart_source_run_summary.parquet` is always 1 row** per (snapshot,
  run_id). Track I schema can mark this as `unique=True` on the
  composite key (`snapshot_date`, `run_id`).

## Stop signs (do NOT add to this checkpoint)

- No deck artifacts. Track G owns deck-layer evidence.
- No release packet. PR #11 (cadence test) and the build_release_packet
  helper run later in the pipeline; out of scope here.
- No live SharePoint upload. The checkpoint is offline / read-only.
- No `harness_registry.json` cleanup. That's filed as its own ticket
  (`docs/2026-04-26-harness-registry-rebuild-ticket.md`) and explicitly
  deferred until after Track I.
