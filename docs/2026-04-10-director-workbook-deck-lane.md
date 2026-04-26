# Director Workbook Deck Lane

Date: 2026-04-10

## Purpose

This lane turns the corrected Sales Director Excel workbooks into workbook-native PowerPoint decks.

It is intended as the reproducible deck ETL path:

1. Excel workbook -> normalized JSON snapshot
2. normalized JSON snapshot -> branded PowerPoint deck
3. optional render -> PDF / PNG montage for review

This keeps deck generation deterministic and factual, while leaving room to add Claude/Office add-ins later for narrative refinement or PowerPoint QA.

## Files

- [extract_director_workbook_snapshot.py](/Users/test/crm-analytics/scripts/extract_director_workbook_snapshot.py)
- [run_director_workbook_decks.py](/Users/test/crm-analytics/scripts/run_director_workbook_decks.py)
- [build_director_workbook_deck.js](/Users/test/crm-analytics/output/director_workbook_deck_2026-04-10/build_director_workbook_deck.js)

Workspace dependencies are symlinked from the existing March deck workspace:

- [output/director_workbook_deck_2026-04-10](/Users/test/crm-analytics/output/director_workbook_deck_2026-04-10)

## Commands

Build one deck and render review artifacts:

```bash
python3 scripts/run_director_workbook_decks.py \
  --snapshot-date 2026-04-10 \
  --director "Adam Steinhaus" \
  --render
```

Build all 9 decks:

```bash
python3 scripts/run_director_workbook_decks.py \
  --snapshot-date 2026-04-10
```

Extract snapshots only:

```bash
python3 scripts/extract_director_workbook_snapshot.py \
  --snapshot-date 2026-04-10
```

## Outputs

JSON snapshots:

- [output/director_workbook_snapshots/2026-04-10](/Users/test/crm-analytics/output/director_workbook_snapshots/2026-04-10)

Decks:

- [output/director_workbook_deck_runs/2026-04-10](/Users/test/crm-analytics/output/director_workbook_deck_runs/2026-04-10)

Batch manifest:

- [manifest.json](/Users/test/crm-analytics/output/director_workbook_deck_runs/2026-04-10/manifest.json)

Validated example render artifacts for Adam:

- [adam-steinhaus.pptx](/Users/test/crm-analytics/output/director_workbook_deck_runs/2026-04-10/adam-steinhaus.pptx)
- [adam-steinhaus.summary.json](/Users/test/crm-analytics/output/director_workbook_deck_runs/2026-04-10/adam-steinhaus.summary.json)
- [adam-steinhaus.pdf](/Users/test/crm-analytics/output/director_workbook_deck_runs/2026-04-10/libreoffice/adam-steinhaus.pdf)
- [adam-steinhaus_montage.png](/Users/test/crm-analytics/output/director_workbook_deck_runs/2026-04-10/adam-steinhaus_montage.png)

## Current Deck Shape

Each deck is currently 8 slides:

1. Cover / executive readout
2. Open pipeline by stage and top opportunities
3. Q2 forecast mix and CRO tie-out
4. Commercial approval state and named exceptions
5. Renewals and retention facts
6. Risk register and hygiene backlog
7. Rep pipeline concentration and recent outcomes
8. Definitions, sources, and open data gaps

The deck is workbook-native rather than dashboard-native:

- source-of-truth is the Excel workbook tabs
- metrics keep the corrected EUR-converted ARR / ACV semantics
- Q2 tie-out uses the workbook’s CRO section rather than recomputing ad hoc in the deck layer

Important Q1 nuance:

- the workbook `Q1 Review` tab contains a global `ForecastingItem` block and a global pushed-deals list copied from shared cache files
- `extract_director_workbook_snapshot.py` now rebuilds director-scoped Q1 actuals, slipped deals, and forecast-category movements from the hidden `.cache` directory
- use `snapshot["q1_review"]["actuals"]`, `snapshot["q1_review"]["pushed_deals"]`, and `snapshot["q1_review"]["forecast_movement_summary"]` for deck logic
- treat `snapshot["q1_review"]["workbook_forecast_vs_actual"]` as global reference only, not as the director promise baseline

## Validation Status

Completed:

- `py_compile` passes for the new Python scripts
- `node --check` passes for the new JS builder
- one full run with render artifacts completed for Adam
- one batch run completed for all 9 directors
- font substitution check was clean for Adam

Known rough edge:

- the `warnIfSlideElementsOutOfBounds()` helper emits systematic warnings on table slides
- these look like false positives from how PptxGenJS stores table geometry
- Adam’s rendered montage looked visually acceptable despite those warnings

## Residual Risk

This lane is functional, but not final-polish quality yet.

Main residual risks:

- only Adam was visually reviewed via montage in this session
- the layout checker produces noisy false positives on tables
- slide density may need tuning for the largest books, especially Sarah and Christian
- the current deck is factual and structured, but still deterministic rather than Claude-shaped
- Q1 promise-vs-delivery still needs a final territory-safe promise definition before it becomes a production slide headline

## Recommended Next Tranche

1. visually review at least Sarah and Christian montages
2. tighten any dense table layouts
3. decide whether Claude should be used for:
   - executive-summary rewrite only
   - slide notes / speaker notes
   - PowerPoint review and wording polish
4. if Claude is added, keep this workbook snapshot as the hard factual contract
