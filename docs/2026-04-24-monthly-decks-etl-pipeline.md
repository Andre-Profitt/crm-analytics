# Monthly Decks ETL Pipeline - 2026-04-24

## Operating Target

The monthly deck platform needs a real ETL lane, not a workbook-first script pile.

Target contract:

1. **Extract / Bronze**: immutable live pulls with query provenance, row counts, timings, source ids, and raw response shape where useful.
2. **Transform / Silver**: typed `DirectorBundle` JSON is the canonical semantic contract. Excel is never the source of truth.
3. **Analytics / Gold**: derived fact packs, deal-risk indices, owner coaching metrics, churn summaries, and deck-ready narrative inputs.
4. **Render**: workbook/deck/sharepoint outputs consume Gold facts and cite source-contract ids.
5. **Observe**: every run writes validation, row parity, field coverage, workbook render coverage, and high-severity intelligence gaps.

## Gates

`scripts/audit_director_etl_intelligence.py` audits a DirectorBundle JSON against its rendered workbook.

Run:

```bash
python3 scripts/audit_director_etl_intelligence.py \
  --bundle output/director_bundles/2026-04-23/jesper-tyrer.json \
  --workbook output/director_live_workbooks/2026-04-23/jesper-tyrer.xlsx
```

Output:

- `output/etl_intelligence_audit/<date>/<director>/etl_intelligence_audit.json`
- `output/etl_intelligence_audit/<date>/<director>/summary.md`

The gate detects:

- JSON datasets with no workbook surface.
- Workbook sheet row-count drift versus the bundle.
- Populated JSON fields omitted from Excel.
- Joined deal-risk rows from activity, approval, push, stage, forecast, and close-date-history signals.
- Owner coaching metrics and churn summaries.

## Live Finding From Jesper 2026-04-23

The live JSON bundle contains 2,336 materialized rows across 14 datasets. After the Gold-sheet renderer update, the workbook has 15 sheets.

Original high-severity finding:

- `close_date_events` has 785 JSON rows and is not rendered into the workbook.

Implemented workbook promotion:

- `Close Date History` sheet renders the 785 close-date history events.
- `Deal Risk Index` sheet renders joined risk scoring from open pipeline + activity + approvals + push count + stage history + forecast category history + close-date churn.

Current audit status after re-render:

- High-severity gaps: `0`
- Coverage gaps: `6`
- Deal risk rows: `25`

P1 analytics:

- Snapshot trend is modeled but empty in this live bundle.
- Coverage appendix should expose populated fields omitted from Excel, especially `age_days`, `quarter`, `opportunity_id`, and closed/won flags.

## Deck Truth Bridge

`scripts/build_deck_truth_packet.py` now builds the grounded bridge from Gold facts to deck automation:

- `deck_truth_packet.json`: stable claim IDs, source artifact paths, source JSON paths, blockers, and publish truth status.
- `rag_corpus.jsonl`: one retrieval-ready row per validated numeric claim. AI narrative can retrieve this, but numeric claims still have to cite a claim ID.
- `thinkcell_source.xlsx`: Excel interface workbook for think-cell links.
- `thinkcell_data.ppttc`: think-cell JSON automation payload for templates with named elements.

Run:

```bash
python3 scripts/build_deck_truth_packet.py --snapshot-date 2026-04-23 --json
```

Current 2026-04-23 result after extractor, deck, and regional workbook refresh:

- Status: `ok`
- Directors: `9`
- Claims: `60`
- High blockers: `0`
- Tie-out mismatches: `0`
- Publish warnings: `1` medium source-data warning for one immaterial negative ARR adjustment in Dan Peppett's Q4 open pipeline.

The gate now certifies the Excel/deck/SF/regional tie-out while still surfacing the remaining source-data warning.

## Next Implementation Steps

1. Add a real think-cell template with named elements matching `TruthStatus`, `RegionalRollupsTable`, `DirectorKpiTable`, and `PublishBlockersTable`.
2. Wire `build_deck_truth_packet.py` into the master builder after deck delivery validation and tie-out.
3. Add a metric-store contract so each claim ID maps to a formula, grain, horizon, currency, and omitted-stage rule.
4. Use the RAG corpus for narrative drafting only; keep deterministic claim IDs as the numeric source of truth.

Status as of this pass:

- Close-date history, deal-risk Excel rendering, ETL audit promotion, Gold analytics, all-director Gold batch generation, regional Gold rollups, deck truth packet, and four-source tie-out are implemented.
- The current deck truth status is publish-clean with one medium source-data warning.
