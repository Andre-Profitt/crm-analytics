# Claude Handoff — Pipeline Inspection to Decks

Date: 2026-04-10
Repo: `/Users/test/crm-analytics`
Org: `apro@simcorp.com`
Scope: Pipeline Inspection territory views are live. Next step is deck generation from the live Salesforce data.

## What Changed Today

### 7 new PI territory views created via Lightning UI clone-and-filter

All cloned from the Global ARR CFQ Forecast seed (`00BTb00000Ic82DMAR`), filtered with `Sales_Director_Book__c` (or Account_Unit_Group + Sales_Region for CE).

| View | ListViewId | Filter |
|---|---|---|
| PI ARR Forecast CE | `00BTb00000Kr3YvMAJ` | Account_Unit_Group = SC EMEA + Sales_Region like Central Europe |
| PI ARR Forecast SWE | `00BTb00000Kr3sHMAR` | Sales_Director_Book = Southern Europe |
| PI ARR Forecast UKI | `00BTb00000Kr3yjMAB` | Sales_Director_Book = UK & Ireland |
| PI ARR Forecast NE | `00BTb00000Kr4DFMAZ` | Sales_Director_Book = NL & Nordics |
| PI ARR Forecast Canada | `00BTb00000Kr4ErMAJ` | Sales_Director_Book = Canada |
| PI ARR Forecast NA AM | `00BTb00000Kr4JhMAJ` | Sales_Director_Book = NA Asset Management |
| PI ARR Forecast P&I | `00BTb00000Kr4OXMAZ` | Sales_Director_Book = Pension & Insurance |

### Pre-existing PI seeds (unchanged)

| View | ListViewId |
|---|---|
| Global ARR CFQ Forecast | `00BTb00000Ic82DMAR` |
| APAC ARR CFQ Forecast | `00BTb00000Ic7kTMAR` |
| EMEA ARR CFQ Forecast | `00BTb00000Ic77lMAB` |
| NA ARR CFQ Forecast | `00BTb00000Ic6JlMAJ` |
| ACV Forecast EMEA_SC_Middle East | `00BQA00000GXOf32AH` |

### Key findings that overturned prior assumptions

1. `Sales_Director_Book__c` **IS visible** in the PI filter picker. The prior handoff was wrong.
2. Formula vs stored field is irrelevant — both `Sales_Region__c` and `Sales_Director_Book__c` are formula fields and both work in PI.
3. PI views can **only** be created through the Lightning UI (Pipeline Settings → Clone or Create New View). No REST API path works — UI API-created list views are fundamentally incompatible with PI binding.
4. Once a view is cloned from a PI-native seed, filters can be added and saved normally through the PI filter panel.

### Unfinished cleanup on the PI views

- **Rename needed**: Current names are abbreviated (PI ARR Forecast CE, etc.). Should be expanded to full territory names (e.g., "Central Europe ARR CFQ Forecast"). Use Pipeline Settings → Rename in each view.
- **Chart default**: Waterfall chart should be set as the default view. Check Metrics Settings or Chart tab persistence.

## Product Definition (unchanged from prior handoff)

### Report 1 — Monthly Sales Director Pack (PowerPoint)

One deck per MD-1. Fixed 6-slide structure:

| Slide | Module | Scope |
|---|---|---|
| 1 | Pipeline overview with quarterly focus | MD-1 specific |
| 2 | Commercial approval overview | Global |
| 3 | Missing commercial approval candidates | MD-1 specific |
| 4 | Renewals tracking (ACV + likelihood) | MD-1 specific |
| 5 | Churn risk and trends | MD-1 specific |
| 6 | Slipped deals analysis | MD-1 specific |

### Report 2 — Quarterly Sales Ops Pack (PowerPoint + KPI Dashboard)

4 KPI families: CRM data quality, process compliance, forecast accuracy, pipeline hygiene.

## MD-1 Book Map

| MD-1 | Territory | Sales_Director_Book__c value |
|---|---|---|
| Megan Miceli | Canada | `Canada` |
| Patrick Gaughan | NA Asset Management | `NA Asset Management` |
| Jesper Tyrer | APAC | `APAC` |
| Sarah Pittroff | Central Europe | `Central Europe` |
| Francois Thaury | Southern Europe | `Southern Europe` |
| Dan Peppett | UK & Ireland | `UK & Ireland` |
| Christian Ebbesen | NL & Nordics | `NL & Nordics` |
| Mourad Essofi | Middle East & Africa | `Middle East & Africa` |
| Adam Steinhaus | Pension & Insurance | `Pension & Insurance` |

## Salesforce Source Map for Report 1 Slides

These are the live reports that feed each slide module. All were upgraded and validated in prior sessions.

| Module | Report ID | Report Name |
|---|---|---|
| Pipeline overview | `00OTb000008fBfdMAE` | Pipeline Overview by Stage |
| Commercial approval (global) | `00OTb000008fBEDMA2` | Commercial Approval Global |
| Commercial approval 2x2 | `00OTb000008fQ6nMAE` | Commercial Approval 2x2 Matrix |
| Approved deals ref | `00OTb000008aTtJMAU` | Commercial Approval approved 2026 |
| Missing approval candidates | `00OTb000008ekltMAA` | Land Stage 3 Missing Approval |
| Missing approval grouped | `00OTb000008ekp7MAA` | Commercial Approval Candidates by Stage |
| Renewals this quarter | `00OTb000008ektxMAA` | Renewal Pipeline This Quarter |
| Renewal likelihood | `00OTb000008fBULMA2` | Renewal Likelihood This Qtr |
| Churn / risk | `00OTb000008Ta9xMAC` | Business At Risk |
| Slipped deals | `00OTb000008eknVMAQ` | Close Date Slipped by Stage |

All reports support MD-1 filtering via the preset combinations in `config/sales_director_md1_presets.json` (validated 72/72 on 2026-04-09).

## Dashboard State

- **Dashboard 1** `01ZTb00000FSP7hMAH` — 8 widgets, 0 flags. Operating surface behind Report 1.
- **Dashboard 2** `01ZTb00000FSP9JMAX` — 18 widgets, 0 active flags, 1 deferred. KPI surface behind Report 2.

## What the Next Session Should Do

The PI territory layer is done. The next step is **deck generation** — pulling data from the live Salesforce reports and assembling PowerPoint slides per the Report 1 / Report 2 spec.

### Recommended approach

1. Read `config/sales_director_md1_presets.json` for the 9 MD-1 filter definitions.
2. For each MD-1, query each Report 1 source report with the appropriate filters using:
   ```bash
   sf data query --target-org apro@simcorp.com --json -q "SELECT ... FROM Opportunity WHERE ..."
   ```
   Or hit the Analytics API report endpoint with filter overrides.
3. Assemble the 6-slide structure per MD-1 into PowerPoint using `python-pptx`.
4. For Report 2, query the D2 KPI reports and build the quarterly pack.

### Key constraints

- `Customize Application = true`, `Modify All Data = false`, Metadata API blocked.
- Auth via `sf org display` — no `.env` files.
- PATCH not PUT for any live dashboard changes.
- Never change viz types (funnel, donut, bar, etc.) without explicit approval.
- Preserve all 6 upgraded reports listed above.

### Files to read first

1. This file.
2. `docs/2026-04-09-spec-locked-report1-report2-execution-plan.md` — the full product spec.
3. `config/sales_director_md1_presets.json` — MD-1 filter definitions.
4. `docs/2026-04-09-pipeline-inspection-api-knowledge-corpus.md` — PI reference (now mostly resolved).

### Useful scripts

- `scripts/validate_d1_md1_presets.py` — validates MD-1 preset coverage.
- `scripts/manage_pi_views.py` — lists PI views.
- `scripts/manage_opportunity_list_views.py` — manages list views.
