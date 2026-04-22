# SD Monthly Deck Pipeline — Session Handoff

**Date:** 2026-04-22
**Branch:** main (18 commits this session)

## What was done

### Pipeline rebuild + delivery

- Full ETL from live SF (Apr 22 extract): 9 director workbooks + 10 analytics workbooks + 10 decks
- All decks + workbooks uploaded to SharePoint (Q1 2026 folder, "Sales Director Monthly - {Region} - March 2026" naming)
- Slide count trimmed to 17-18 (removed churn placeholder, pushed deals detail table, approvals narrative)

### Q3 extension

- 5 directors had zero Q2 Land pipeline (P&I, NA AM, Canada, NL & Nordics, Southern Europe)
- Deck builder now shows Q3 outlook + forward look when Q2 ARR is zero, with subtitle note explaining the swap
- Extract script adds Q2 Movement sheet (slip/push classification parallel to Q1)
- Scope gate extended from Q1-Q2 to Q1-Q3 across builder, validator, and sidecar

### SF reporting infrastructure

- 18 Pipeline Forecast Review reports patched with Type=Land (were missing the filter)
- 9 PI ARR Forecast list views patched with Type=Land via Codex/Playwright
- 9 Q3 PI list views created (Land-only, NEXT FISCAL QUARTER scoped)
- 9 Q3 Historical Trending reports created (Jul-Sep date range)
- Territory mapping updated: NA Sales AM & Insurance split into separate entries
- IDs saved in config/q3_2026_reporting_ids.json

### Text audit

- Removed SF API field name from visible footnote (APTS_Opportunity_ARR\_\_c)
- Eliminated all em dashes from slide text (11 instances)
- Replaced technical subtitles with director-friendly language (deal risk scoring, owner coaching, forward look)
- Standardized ARR column headers to "ARR (mEUR)" across all tables
- Title-cased table headers, standardized title separators to comma
- Cleaned weak/passive language in empty states and placeholder slides

### Validation

- Tie-out: 7/9 directors pass clean, 2 have pre-existing conditional approval classification edge case (SF counts 1 conditional, extract counts 0 — classification boundary, not a bug)
- Deck scope audit: 0 drift flags
- Data quality audit: 36/36 checks pass

## What's pending

1. **PI surface attachment for Q3 views** — Codex/Playwright task. Runbook at `docs/2026-04-22-q3-pi-surface-attachment-runbook.md`. The 9 list views work for our pipeline but won't appear in Pipeline Inspection UI for directors until attached via Setup.

2. **Conditional approval tie-out edge case** — Jesper and Mourad show SF=1 conditional vs Extract=0. The extract's 4-state approval model classifies differently than the validator's raw SOQL. Not blocking; could align the two classification methods if it matters.

3. **Southern Europe** — Zero pipeline across both Q2 and Q3. Francois's deck shows empty Q2 outlook. No action unless Andre wants to suppress the outlook slide when both quarters are empty.

## Key files changed

| File                                   | Lines changed | What                                    |
| -------------------------------------- | ------------- | --------------------------------------- |
| scripts/build_deck_from_excel.py       | +280/-160     | Q3 fallback, text audit, slide trimming |
| scripts/extract_director_live.py       | +45/-1        | Q2 Movement sheet, PI Land filter       |
| scripts/validate_tie_out.py            | +5/-5         | Q1-Q3 scope alignment                   |
| scripts/run_monthly_director_review.py | +1/-1         | Remove stale --skip-samples flag        |
| config/q3_2026_reporting_ids.json      | new           | Q3 PI view + report IDs                 |
| config/territory_mappings.json         | +2/-1         | NA AM / Insurance split                 |
