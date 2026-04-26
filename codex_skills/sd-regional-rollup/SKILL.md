---
name: sd-regional-rollup
description: Use when reconciling Sales Director books to regional or CRO rollups in this repo. Covers forecast hierarchy, subregion mapping, MEA under EMEA, and opportunity-versus-forecast tie-out logic.
---

# SD Regional Rollup

Use this skill for CRO, region, or subregion tie-outs.

## Start Here

1. Read `references/rollup-rules.md`.
2. Separate director-book logic from forecast-hierarchy logic.
3. Be explicit about whether the number comes from open opportunities or ForecastingItem.

## Non-Negotiables

- do not group MEA under APAC
- distinguish book reporting from manager reporting
- distinguish open-opportunity rollups from ForecastingItem rollups
- do not assume opportunity totals should equal forecast totals

## Primary Repo References

- `docs/2026-04-10-forecast-subregion-bug-handoff.md`
- `config/sales_director_md1_presets.json`
- corrected workbook snapshots and scorecards
