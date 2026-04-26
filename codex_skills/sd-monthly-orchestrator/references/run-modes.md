# Run Modes

## Plan

Use this before any live Office automation:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --plan-only
```

## One Director Pilot

Use this before running all 9 directors:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --director "Sarah Pittroff"
```

## Batch

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --fallback-workbook-deck
```

## Artifacts To Inspect

- `output/sales_director_monthly_master_builder/<date>/<timestamp>/manifest.json`
- per-director `validated_bridge/validated-fact-pack.md`
- per-director `validated_bridge/validation-report.json`
- per-director `powerpoint_review/powerpoint-message.txt`

## Failure Policy

- keep the run moving when one director fails
- preserve transcripts and manifests
- treat Excel-Claude and PowerPoint-Claude failures as lane failures, not data-truth failures
