# Sales Deck Family Source Map

Date: 2026-04-11

## Decision

The reporting system has two primary products and one internal rollup layer:

1. **Director primary deck**  
   One deck per MD-1. This is the main product.
2. **Global summary deck**  
   One executive summary deck with **one operating slide per region** plus global control summary slides.
3. **Regional rollup layer**  
   An internal deterministic rollup for `APAC`, `EMEA`, and `North America` that feeds the global summary deck.

The current repo had drifted toward the regional deck as the main artifact. That is wrong.  
The correct product hierarchy is:

- director deck first
- global summary second
- regional rollup supports the system but is not a primary deliverable by default

## Named Director Scope

The director deck family must cover:

- `Megan Miceli (Canada)`
- `Patrick Gaughan (NA Asset Management)`
- `Jesper Tyrer (APAC)`
- `Sarah Pittroff (Central Europe)`
- `Francois Thaury (Southern Europe)`
- `Dan Peppett (UK & Ireland)`
- `Christian Ebbesen (NL & Nordics)`
- `Mourad Essofi (Middle East & Africa)`
- `Adam Steinhaus (Pension & Insurance)`

## Director Primary Deck

This is the operating review deck. It should be fact-driven, Salesforce-oriented, and monthly-repeatable.

### Required slides

| Slide | Why it exists | Current support | Primary source |
|---|---|---:|---|
| Executive Summary | Leadership position, risk, and action | Strong | `scorecard`, `renewals.summary_metrics`, derived action/risk |
| Q1 Promised vs Delivered | What was promised vs delivered vs slipped | Qualified | `q1_review.actuals`, `q1_review.promise_baseline`, `q1_review.pushed_deals` |
| Quarterly Pipeline and Forecast | Current-quarter active view and forecast mix | Strong | `scorecard.pipeline-health`, `q2_outlook.breakdown` |
| Commercial Approval Overview | Approved vs pending vs missing stage 3+ | Strong | `commercial_approval.summary` |
| Missing Commercial Approval Candidates | Action list of stage 3+ land opps | Strong | `commercial_approval.missing_candidates` |
| Renewals and Retention | Current-quarter renewals, value, and risk | Qualified | `renewals.summary_metrics`, `renewals.q2_open_renewals`, `renewals.risk_levels` |
| Slipped Deals and Follow-up | Validated slips plus owner follow-up need | Qualified | `q1_review.pushed_deals`, `q1_review.forecast_movement_summary` |
| Salesforce Hygiene and Activity Controls | Process hygiene and seller execution signals | Strong / partial | `data_quality`, `risk_register`, `rep_performance` |
| Appendix and Notes | Definitions, lineage, caveats | Strong | `sources`, `scorecard`, `data_quality`, `q1_review.scope_warning` |

### Additional director controls explicitly requested

These should be added to the director shell as explicit slides or panels, not left as ad hoc bullets.

| Control | Rule | Current support | Action |
|---|---|---:|---|
| Missing Win/Loss Reason | `0 - No Opportunity` with no reason is acceptable | Available from `won_lost` | Add derived control |
| Overdue Close Date Open Opps | Sort by largest record count, not owner | Available from `data_quality` + `pipeline_detail.records` | Add derived control |
| KYC Missing | Accounts without KYC Approval | Not in workbook snapshot | Add new Salesforce source |
| Activity / No Activity | Show activity gap as a management control | Available in `data_quality` and `risk_register.Activity Days Ago` | Add derived control |
| Rep concentration / execution risk | Which reps hold pipeline and risk | Available in `rep_performance` | Add derived control |

### Safe derived insights from current Excel data

These are safe to include because the director snapshot already carries the underlying facts.

- **Rep concentration risk**
  - top reps by open pipeline
  - reps who also carry stale deals, pushed deals, or missing approvals
- **Risk-register outliers**
  - largest ARR deals with high `Push Count`
  - largest ARR deals with extreme `Activity Days Ago`
  - largest ARR deals with long `Days In Stage`
  - deals with `Backward Moves`
- **Hygiene hot spots**
  - `No Activity`
  - `Overdue Close`
  - `Missing Next Step`
  - `Missing Amount`
  - `Missing Approval`
- **Win/loss themes**
  - competitor patterns
  - loss reasons / sub-reasons
  - sales cycle outliers
- **Renewal pressure**
  - Q2 renewal watchlist
  - sparse risk-tagging caveat

### Things that still need new sources

- `Accounts without KYC Approval | Salesforce`
- Finance-owned churn trends / current churn risk inputs
- true target / quota coverage
- broader seller activity layer if we want tasks/calls/meetings, not just no-activity flags

## Regional Rollup Layer

This layer summarizes each top-level region without becoming the primary product. It exists mainly to support the global summary deck and to keep the rollup logic explicit.

### Required slides

| Slide | Why it exists | Current support | Primary source |
|---|---|---:|---|
| Regional Executive Summary | Regional position and top action | Strong | regional scorecard rollup |
| Pipeline Overview | One region-level quarterly pipeline slide | Strong | regional `q2_outlook` + scorecard |
| Regional Book Breakdown | Books inside the region | Strong | `component_books` |
| Commercial Approval Overview | Region-level approval control | Qualified | regional approval rollup |
| Missing Approval Candidates | Region-level action list | Strong | regional missing candidates |
| Renewals Tracking | Regional open/Q2 renewal ACV and watchlist | Qualified | regional renewals |
| Slipped Deals and Follow-up | Regional slipped exposure | Qualified | regional `q1_review` |
| Appendix | Notes and caveats | Strong | regional notes |

### Regional requirement from the brief

The brief specifically calls for:

- **one pipeline overview slide per region**

That rollup belongs here as data and slide logic, and it primarily feeds the global summary deck.

## Global Summary Deck

This deck should not try to replace the director deck. It is an executive synthesis deck built from regional rollups.

### Required slides

| Slide | Why it exists | Current support | Primary source |
|---|---|---:|---|
| Global Executive Summary | Global top risk and action | Derived | regional rollups |
| APAC Slide | Single operating slide for APAC | Strong / qualified | APAC rollup |
| EMEA Slide | Single operating slide for EMEA | Strong / qualified | EMEA rollup |
| North America Slide | Single operating slide for North America | Strong / qualified | North America rollup |
| Global Commercial Approval Overview | Approved 2026 and missing candidates by region | Qualified | regional approval rollups + explicit SF lists |
| Global Appendix | Definitions and limitations | Strong | architecture rules + regional notes |

### Global requirement from the brief

The brief specifically calls for:

- a **global commercial approval overview** slide
- a **list of candidates by region** slide

Those belong in the global deck, not duplicated into every director deck.

## Source Map By Current Repo Contract

### Already in director workbook snapshots

These are already exposed in `output/director_workbook_snapshots/<date>/<director>.json`.

- `scorecard`
- `pipeline_detail.records`
- `pipeline_detail.stage_breakdown`
- `pipeline_detail.top_opportunities`
- `q2_outlook.breakdown`
- `q2_outlook.commit_deals`
- `q2_outlook.best_case_deals`
- `q2_outlook.top_q2_active_opportunities`
- `commercial_approval.summary`
- `commercial_approval.missing_candidates`
- `commercial_approval.approved_ytd`
- `renewals.open_renewals`
- `renewals.q2_open_renewals`
- `renewals.risk_levels`
- `renewals.summary_metrics`
- `rep_performance.records`
- `risk_register.records`
- `data_quality.total`
- `data_quality.top_issues`
- `won_lost.won`
- `won_lost.lost`

### Not yet promoted into the deck contract strongly enough

- rep concentration / execution burden
- overdue close-date control
- missing win/loss reason control
- risk-register outlier surfacing
- no-activity management view
- sales-cycle / competitor theme summary

## Build Order

### Phase 1: Director shell becomes the primary shell

Add or revise director-shell slides for:

- `salesforce-hygiene-activity`
- `missing-win-loss-reason`
- `overdue-close-open-opps`
- optional `rep-concentration-and-execution-risk`

### Phase 2: Director structured payload

Build a director equivalent of the regional structured payload:

- `validated-fact-pack.md`
- `powerpoint-fill-payload.json`
- `powerpoint-build-prompt.txt`

### Phase 3: Global summary deck

Build a deterministic global summary payload and shell:

- one slide per region
- one global approval-control slide
- one appendix slide

### Phase 4: New source additions

Add explicit non-workbook sources for:

- `Accounts without KYC Approval | Salesforce`
- `Commercial Approval candidates | Salesforce`
- `Commercial Approval approved 2026 | Salesforce`
- Finance-owned churn reporting

## Non-Negotiables

- Pipeline must always declare horizon and unit.
- Renewals must stay `ACV`.
- `Omitted` must remain visible and separate.
- Q1 promise baseline must remain qualified.
- Missing win/loss reason logic must treat `0 - No Opportunity` as acceptable.
- If a control is not in the data contract, it should not appear as a fact in the deck.
- Standalone regional decks are optional; regional rollups must always support the global summary deck.
