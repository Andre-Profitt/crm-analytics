# Forecast Subregion Tie-Out Bug Handoff

Date: 2026-04-10

## Problem

The current sales-director workbook/deck pipeline mixes two different territory concepts:

1. director book slices
2. Salesforce forecast rollup territories

That mix causes the top-level pipeline and forecast numbers to diverge in ways that look like data errors, especially once you try to reconcile to the CRO forecast page.

The most important structural bug is:

- `Middle East & Africa` is currently grouped with `APAC` in several dashboard/spec artifacts
- but in the live Salesforce forecast hierarchy, `ME & AFR Sales` rolls up under `EMEA`, not `APAC`

This is not a cosmetic issue. It changes region totals and makes the deck/workbook worldview inconsistent with the CRO forecast tree.

## Verified Facts

### 1. Director workbook slices

The director presets define the 9 workbook slices by `Account.Region__c` plus account-unit filters.

See:
- [sales_director_md1_presets.json](/Users/test/crm-analytics/config/sales_director_md1_presets.json#L9)

Relevant rows:
- Jesper Tyrer -> `APAC` + `SC Asia`
- Sarah Pittroff -> `Central Europe` + `SC EMEA`
- Francois Thaury -> `Southwestern Europe` + `SC EMEA`
- Dan Peppett -> `United Kingdom & Ireland` + `SC EMEA`
- Christian Ebbesen -> `Northern Europe` + `SC EMEA`
- Mourad Essofi -> `Middle East & Africa` + `SC EMEA`
- Megan Miceli / Patrick Gaughan / Adam Steinhaus -> `North America` + `SC North America` with country/industry splits

This is a director-book model, not a forecast-hierarchy model.

### 2. Current dashboard/spec assumption is wrong for forecast tie-out

The repo explicitly groups `Middle East & Africa` with `APAC` in multiple places.

Examples:
- [phase2_7_build.py](/Users/test/crm-analytics/scripts/phase2_7_build.py#L86)
- [2026-04-08-phase2-7-dashboard1-missing-widgets.md](/Users/test/crm-analytics/docs/2026-04-08-phase2-7-dashboard1-missing-widgets.md#L16)
- [sales-director-monthly-dashboard-spec.md](/Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md#L60)

Concrete bad assumption in code:

```python
EMEA_REGIONS = "United Kingdom & Ireland,Central Europe,Northern Europe,Southwestern Europe"
APAC_REGIONS = "APAC,Middle East & Africa"
```

This assumption is the main structural reason the regional rollups do not align with Salesforce forecast.

### 3. Live Salesforce forecast hierarchy

Verified live against Salesforce on 2026-04-10 using the CRO forecast page:

- `forecastingOwnerId = 0055700000747OgAAI` -> `Oliver Johnson`
- `forecastingTerritoryId = 0MI7S0000008XL2WAM` -> `CRO`
- `ForecastingTypeId = 0Db7S000000zDaMSAU` -> `Opportunity ARR`

CRO direct child forecast territories:
- `APAC` -> `0MI7S0000008XKuWAM`
- `EMEA` -> `0MI7S0000008XL4WAM`
- `North America` -> `0MI7S0000008XLVWA2`

Relevant deeper sales nodes:

Under `EMEA`:
- `EMEA Sales` -> `0MIQA00000005UL4AY`
- `ME & AFR Sales` -> `0MIQA00000005m54AA`

Under `EMEA Sales`:
- `EMEA Sales CE` -> `0MIQA00000005Vx4AI`
- `EMEA Sales NE` -> `0MIQA00000005ZB4AY`
- `EMEA Sales SWE` -> `0MIQA00000005cP4AQ`
- `EMEA Sales UK & IE` -> `0MIQA00000005fd4AA`

Under `APAC`:
- `APAC LAND` -> `0MI7S0000008XKyWAM`
- `APAC Expansion` -> `0MIQA0000000Jtt4AE`

Under `North America`:
- `NA Sales AM` -> `0MITb0000000JM1OAM`
- `NA Sales Insurance` -> `0MITb0000000dvpOAA`
- `NA Sales Canada` -> `0MIQA00000004wT4AQ`
- `NA Sales Pension` -> `0MIQA000000052v4AA`

Conclusion:

- `Middle East & Africa` is part of the `EMEA` forecast tree
- not the `APAC` forecast tree

## Why The Numbers Felt Wrong

There were two independent problems:

### A. Currency bug

The earlier workbook headline for Christian Ebbesen looked like `€432.8M`, but that was a mixed-currency sum being formatted as EUR.

This has already been fixed by pulling `convertCurrency(...)` fields into the extractor and using them in workbook calculations.

Relevant files:
- [director_data_helpers.py](/Users/test/crm-analytics/scripts/director_data_helpers.py#L160)
- [extract_director_data.py](/Users/test/crm-analytics/scripts/extract_director_data.py#L153)
- [build_director_workbooks.py](/Users/test/crm-analytics/scripts/build_director_workbooks.py#L103)

### B. Territory rollup bug

Even after fixing EUR conversion and excluding `Omitted`, a regional tie-out to CRO forecast still fails if you keep grouping `ME&A` into `APAC`.

That is because:
- director workbooks are sliced by book/subregion
- CRO forecast is rolled up by forecast territories
- those are related but not identical structures

## Correct Conceptual Model

The system needs two separate mapping layers.

### 1. Director book mapping

Use this when building per-director workbooks and decks:

- Jesper -> APAC
- Sarah -> Central Europe
- Francois -> Southwestern Europe
- Dan -> UK & Ireland
- Christian -> Northern Europe
- Mourad -> Middle East & Africa
- Megan -> Canada
- Patrick -> NA Asset Management
- Adam -> Pension & Insurance

### 2. Forecast rollup mapping

Use this when tying to CRO forecast or any `ForecastingItem` hierarchy:

- `APAC` = APAC only
- `EMEA` = Central Europe + Northern Europe + Southwestern Europe + United Kingdom & Ireland + Middle East & Africa
- `North America` = Canada + Asset Management + Pension/Insurance

Important:

- `APAC` must no longer include `Middle East & Africa`
- `EMEA` must include `Middle East & Africa`

## Current Data Contract Status

The workbook extraction/rendering layer is now using EUR-converted measures and excludes `Omitted` from active pipeline metrics.

Relevant lines:
- Open pipeline extraction: [extract_director_data.py](/Users/test/crm-analytics/scripts/extract_director_data.py#L155)
- Converted opportunity fields: [director_data_helpers.py](/Users/test/crm-analytics/scripts/director_data_helpers.py#L160)
- Active pipeline logic: [build_director_workbooks.py](/Users/test/crm-analytics/scripts/build_director_workbooks.py#L139)
- Scorecard headline: [build_director_workbooks.py](/Users/test/crm-analytics/scripts/build_director_workbooks.py#L302)

Current intended metric semantics:
- new business / expansion pipeline = `ARR (EUR converted)`
- renewals = `ACV (EUR converted)`
- active headline pipeline excludes `Omitted`

## Verified Forecast Numbers

For `FQ2 FY 2026` under CRO on 2026-04-10, `ForecastingItem` totals were:

- `Pipeline`: `€5,649,304.43`
- `Best Case`: `€15,393,921.28`
- `Commit`: `€5,954,839.01`
- `Closed`: `€1,498,435.23`

Open forecast total:

- `€26,998,064.72` = `Pipeline + Best Case + Commit`

Child territories tied exactly to the CRO total:

- `APAC`
- `EMEA`
- `North America`

That hierarchy is internally consistent.

## Why Opportunity Pipeline Still Won't Equal Forecast Exactly

Even after the mapping fix, opportunity-side totals will still not equal `ForecastingItem` exactly.

That is expected because `ForecastingItem` is a forecast submission/rollup surface, not just a raw sum of opportunity rows.

Differences can come from:
- manager adjustments / overrides
- territory-level submissions
- `Closed` bucket treatment
- forecast category logic not matching raw row-level filters exactly

So the goal is not “make opportunity totals equal forecast totals.”
The goal is:

1. make the regional structure consistent
2. make the metrics consistently labeled
3. make the residual delta explainable

## Bugs To Fix

### Bug 1. Wrong top-level region mapping in specs/code

Files to update:
- [phase2_7_build.py](/Users/test/crm-analytics/scripts/phase2_7_build.py#L86)
- [2026-04-08-phase2-7-dashboard1-missing-widgets.md](/Users/test/crm-analytics/docs/2026-04-08-phase2-7-dashboard1-missing-widgets.md#L16)
- [sales-director-monthly-dashboard-spec.md](/Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md#L60)
- [report-1-source-contract.md](/Users/test/crm-analytics/docs/specs/report-1-source-contract.md#L67)
- any other file that defines APAC filters as `('APAC','Middle East & Africa')`

Required change:
- `APAC` filter should only use `Sales_Region__c = 'APAC'`
- `EMEA` filter should include `Middle East & Africa`

### Bug 2. No explicit distinction between book rollup and forecast rollup

The codebase needs a formal mapping artifact, not scattered assumptions.

Recommended fix:
- create a shared config file for territory mappings
- define both:
  - `director_book_rollup`
  - `forecast_rollup`

Every downstream deck/dashboard/report should explicitly choose one.

### Bug 3. Deck/workbook pipeline views are too easy to misread as forecast

Decks need separate labels for:
- `Active Open Pipeline ARR`
- `In-Quarter Active Pipeline ARR`
- `Forecast Pipeline`
- `Forecast Best Case`
- `Forecast Commit`

The system should never label one of these generically as just `Pipeline`.

## Suggested Implementation Plan

1. Add a shared mapping file
   - include director-to-book mapping
   - include book/subregion-to-forecast-rollup mapping

2. Refactor dashboard/spec builders
   - stop hardcoding `APAC,Middle East & Africa`
   - generate filters from the shared mapping file

3. Build a reconciliation artifact
   - one table by subregion, region, and CRO total
   - columns:
     - active open opp ARR
     - omitted ARR
     - in-quarter active ARR
     - forecast pipeline
     - forecast best case
     - forecast commit
     - forecast closed
     - delta

4. Update deck/workbook prompts/contracts
   - distinguish book views from forecast views
   - keep strict factual reporting
   - always label unit and horizon

## Acceptance Criteria

The bug is fixed when all of the following are true:

1. No spec or builder still groups `Middle East & Africa` under `APAC`.
2. `EMEA` forecast rollups include `Middle East & Africa`.
3. A single source-of-truth mapping file exists for both book and forecast hierarchies.
4. CRO/APAC/EMEA/North America reconciliation is reproducible from code, not manual analysis.
5. Decks and workbook summaries label:
   - metric type: ARR vs ACV
   - currency: EUR converted
   - horizon: all-open vs in-quarter vs forecast

## Notes For The Next Agent

Do not collapse everything into one “correct region” concept.

There are at least two valid and necessary region models:
- director book slices
- forecast rollup territories

The fix is to represent both explicitly and use each intentionally.
