# Source distribution baselines (Track D)

Hand-promoted distribution seeds, one per `(requirement_id, territory, period_role)`
tuple. Each seed records the calibrated `share_by_category` for every dimension a
contract has opted into via `distribution_policy.dimensions`.

The runtime audit (`scripts/extract_salesforce_sources.py` →
`scripts/monthly_platform/source_distribution_audit.py`) reads these seeds in
read-only mode and emits drift findings against them. Seeds are **never** written
during a normal monthly extract — only `scripts/calibrate_source_distribution_baselines.py`
writes here, and only when `--promote-baselines` is passed.

## Files

| File                  | What it is                                                                          |
| --------------------- | ----------------------------------------------------------------------------------- |
| `<baseline_key>.json` | One calibrated seed per source (e.g. `sd_pipeline_open.apac.current_quarter.json`). |
| `promotions.jsonl`    | Append-only ledger; one JSON line per `--promote-baselines` run.                    |

## Promotion

Calibrate from a recent approved monthly run:

```sh
# Dry-run (default): inspect what would be promoted.
python3 scripts/calibrate_source_distribution_baselines.py \
    --evidence-run output/monthly_salesforce_sources/<snapshot>/<run_id>

# Promote into config/source_distribution_baselines/.
python3 scripts/calibrate_source_distribution_baselines.py \
    --evidence-run output/monthly_salesforce_sources/<snapshot>/<run_id> \
    --promote-baselines
```

Multiple `--evidence-run` flags merge samples across runs the same way the C
calibrator merges row-count observations. The contract at
`config/monthly_source_requirements.json` is the source-of-truth for which
dimensions to compute; the calibrator overlays that policy onto the historical
plan items so old runs become re-callable when a new dimension is added.

## Promotion ledger

`promotions.jsonl` is append-only. A wrong or stale promotion is corrected by
adding a new line, never by editing or deleting prior lines. Each entry carries:

- `promoted_at` (ISO timestamp)
- `actor` (`$USER` by default; override via `--actor`)
- `evidence_runs` (paths used to derive the seeds)
- `baseline_keys` (which seeds were written)
- `dimension_count`
- `snapshot_dates`, `run_ids`

## What's calibrated today

Track D activation (2026-04-26): the first live opt-in is `sd_pipeline_open` with
three dimensions (`StageName`, `ForecastCategoryName`, `Owner.Name`) plus a
`stage_5_presence` slice sentinel. Default actions are `info` while the share
envelopes mature; operators escalate per dimension via
`DimensionPolicy.{disappeared_category_action, share_drift_action,
concentration_action, missing_seed_action}` once the calibration is mature.

Single-run baselines have small `sample_count` values; envelopes will widen
naturally as additional approved monthly runs are merged in.
