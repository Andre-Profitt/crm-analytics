# D1 MD-1 Preset Validation - 2026-04-09

Validates the current live `Sales Directors Monthly` dashboard (`01ZTb00000FSP7hMAH`) against the 9 named MD-1 territory presets.

Method:

- Executed the 8 current live D1 source reports through `POST /analytics/reports/{id}/instances?includeDetails=true`
- Applied temporary `reportMetadata.reportFilters` overrides per MD-1 preset
- Did not mutate any saved report or dashboard metadata
- Artifact paths:
  - `/tmp/d1_md1_preset_validation.json`
  - `/tmp/d1_md1_preset_validation.md`

## Result

- Presets validated: `9`
- Live D1 reports validated per preset: `8`
- Total report executions: `72`
- Execution failures: `0`

Conclusion:

- The current live 8-widget D1 surface is genuinely MD-1 executable.
- The North America split is real, not aspirational.
- Remaining zeros are data-sparsity outcomes, not filter-architecture failures.

## Filter Resolution Locked In

The validation pass resolved the actual Salesforce filter values behind the user-facing dashboard labels:

- `Legal Country = Canada` is implemented as `ADDRESS1_COUNTRY_CODE = CA`
- `Legal Country = Exclude Canada` is implemented as `ADDRESS1_COUNTRY_CODE notEqual CA`
- Multi-industry splits work with comma-separated picklist values:
  - Patrick: `Asset Management,Bank,Wealth Management,Asset Servicer,Other`
  - Adam: `Pension,Insurance`

These values were proven live against `00OTb000008fBfdMAE` and then reused across the full D1 report set.

## High-Signal Findings

1. All 9 MD-1 presets executed successfully against all 8 current D1 sources. There are no live filter-binding failures in the current D1 architecture.

2. The Patrick vs Adam North America split works correctly at the live report layer. On `Pipeline Overview by Stage`, Patrick returns `31` detail rows and Adam returns `9`; on `Commercial Approval Approved YTD (Land)`, Patrick returns `0` and Adam returns `4`. That is the strongest proof that the current Industry split is doing real work.

3. The renewal surfaces are sparse for North America right now. `Renewal Likelihood by Probability` and `Renewal Pipeline This Quarter` both return `0` rows for Megan, Patrick, and Adam. Mourad also has `0` rows on both renewal surfaces.

4. `Commercial Approval Approved YTD (Land)` is sparse for several books, but not broken. It returns `0` rows for Sarah, Dan, Christian, Megan, and Patrick, while remaining non-zero for Jesper, Francois, Mourad, and Adam.

5. The consistently non-zero MD-1 surfaces are:
   - `Pipeline Overview by Stage`
   - `Commercial Approval Current State`
   - `Business At Risk`
   - `Commercial Approval Candidates by Stage`
   - `Close Date Slipped by Stage`

These are the most reliable current-month building blocks for the per-MD1 pack.

## Zero-Row Surfaces By MD-1

| MD-1 | Zero-row reports |
| --- | --- |
| Jesper Tyrer | none |
| Sarah Pittroff | `Commercial Approval Approved YTD (Land)` |
| Francois Thaury | none |
| Dan Peppett | `Commercial Approval Approved YTD (Land)` |
| Christian Ebbesen | `Commercial Approval Approved YTD (Land)` |
| Mourad Essofi | `Renewal Likelihood by Probability`, `Renewal Pipeline This Quarter` |
| Megan Miceli | `Renewal Likelihood by Probability`, `Commercial Approval Approved YTD (Land)`, `Renewal Pipeline This Quarter` |
| Patrick Gaughan | `Renewal Likelihood by Probability`, `Commercial Approval Approved YTD (Land)`, `Renewal Pipeline This Quarter` |
| Adam Steinhaus | `Renewal Likelihood by Probability`, `Renewal Pipeline This Quarter` |

## Operational Implication

The next D1 work should optimize for insight density, not preset plumbing.

- Preset plumbing is now proven.
- The main remaining issue is that some MD-1 books have naturally sparse approved-deals and renewals surfaces in the current quarter.
- That means deck logic and dashboard narrative need to handle zero-row states intentionally rather than treating them as defects.
