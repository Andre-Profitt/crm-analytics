# Sales Director And Sales Ops Reporting Spec

This document turns the two requested reporting asks into a build contract for the current CRM Analytics repo.

It does not assume the final product is "just a dashboard."

Current recommendation:

- Report 1 is a hybrid product: CRM Analytics and Salesforce report source layers feeding a PowerPoint deck.
- Report 2 is a dashboard-first product: a quarterly Sales Ops CRMA dashboard plus a shorter quarterly PowerPoint readout derived from that dashboard.

## What Exists Today

### Live Or Supporting Source Surfaces

- `Forecast & Revenue Motions` is live and supporting in [config/context_registry.json](config/context_registry.json).
- `Revenue Retention & Health` is live and supporting in [config/context_registry.json](config/context_registry.json).
- `Commercial Rhythm Control Tower`, `Executive Revenue Source Truth`, and `Account Intelligence KPIs` are tracked as target surfaces in [config/dashboard_autopilot_queue.json](config/dashboard_autopilot_queue.json).

### Relevant Verified Surface Evidence

- `Forecast & Revenue Motions` already includes trajectory, bridge, quality, and exception-table widgets, plus record-context queues, in [quality_refresh_2026-03-11 forecast audit](../output/autopilot/runs/quality_refresh_2026-03-11T07-29-25/forecast_revenue_motions/audit/audit.md).
- `Executive Revenue Source Truth` already includes executive KPI story, a regional table, and a one-page executive layout in [quality_refresh_2026-03-11 executive revenue audit](../output/autopilot/runs/quality_refresh_2026-03-11T07-29-25/executive_revenue_source_truth/audit/audit.md).
- `Revenue Retention & Health` already includes `Retention Summary`, `Trends`, `Renewal Pipeline`, and `Churn Analysis` in [continuous_full retention audit](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/revenue_retention_health/audit/audit.md).
- `Account Intelligence KPIs` already includes a `Data Quality` page in [continuous_full account intelligence audit](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/account_intelligence/audit/audit.md).
- `Commercial Rhythm Control Tower` already has an intended `Summary`, `Ownership & Handoffs`, and `Process Quality` contract in [scripts/audit_commercial_rhythm_control_tower.py](scripts/audit_commercial_rhythm_control_tower.py).

### Relevant KPI Contracts Already Cataloged

- `Forecast Accuracy` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)
- `Commercial approval to close time` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)
- `Commercial Approvals - Coverage tracking` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)
- `Forecasting Standards - Deal cycle compliance` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)
- `Opportunity Data Management - Stuck opp flags` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)
- `Passed Due Monthly/Quarterly` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)
- `Data Cleanup - Unit Group Population` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)
- `Opportunity Creation - Path standardization` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)
- `Potential Termination Risk Management` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)
- `Lost ARR By Quarter - with reason` in [kpi_catalog.json](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/kpi_catalog/kpi_catalog.json)

### Live Field Validation Snapshot

Validated with read-only Salesforce CLI queries against `apro@simcorp.com` on March 31, 2026.

- Commercial approval fields are real on `Opportunity`:
  - `Stage_20_Approval__c`
  - `Stage_20_Approval_Date__c`
  - `Approval_Status__c`
- Commercial approval population is non-trivial:
  - `Stage_20_Approval__c = true`: `543`
  - `Approval_Status__c = No Approval Necessary`: `34,434`
  - `Approval_Status__c = Needs Approval`: `51`
  - `Approval_Status__c = Approved`: `13`
  - `Approval_Status__c = Rejected`: `2`
- Data quality fields are real:
  - `Reason_Won_Lost__c` on `Opportunity`
  - `DUNS_No__c` and `Unit_Group__c` on `Account`
- Current rough population snapshot:
  - closed opportunities with `Reason_Won_Lost__c`: `16,063 / 46,079` (`34.9%`)
  - accounts with `DUNS_No__c`: `10,282 / 13,543` (`75.9%`)
- Explicit process helper fields from the KPI catalog were not found under these exact `Opportunity` API names:
  - `Deal_Cycle_Compliant__c`
  - `Stuck_Flag__c`
  - `All_Required_Fields_Complete__c`
  - `Creation_Playbook_Completed__c`
- Implication:
  - commercial approval is buildable now
  - data quality completeness is buildable now
  - process compliance still needs a field-mapping pass instead of assuming the KPI catalog field names are the live source of truth

## Report 1

Goal: monthly forward-looking PowerPoint for Sales Directors, driven by quarterly pipeline and risk insight rather than raw dashboard screenshots.

### Product Decision

- Final deliverable: PowerPoint deck.
- Source system: hybrid.
- Primary CRM Analytics sources:
  - `Executive Revenue Source Truth`
  - `Forecast & Revenue Motions`
  - `Revenue Retention & Health`
  - `Commercial Rhythm Control Tower`
- Optional Salesforce report support:
  - commercial approval candidate lists
  - slipped-deal owner lists
- Current implementation status:
  - first draft deck and live snapshot runner now exist in [output/sales_director_monthly_deck_2026-03-31/README.md](../output/sales_director_monthly_deck_2026-03-31/README.md)
  - one-command monthly packaging now exists in [scripts/run_sales_director_monthly_report.py](../scripts/run_sales_director_monthly_report.py)

### Proposed Slide Outline

#### Slide 1: Executive Summary

- Purpose: summarize what changed this month, where the quarter is exposed, and which regions need intervention.
- Source surfaces:
  - `Executive Revenue Source Truth`
  - `Forecast & Revenue Motions`
- Status: can assemble quickly.
- Current state:
  - implemented in the first-draft monthly deck workspace
- Notes:
  - Use only 3-5 findings.
  - Do not use a raw dashboard screenshot as the slide.

#### Slide 2: EMEA Quarterly Pipeline Overview

- Purpose: pipeline shape, gap-to-target, low-confidence pipeline, promotion need.
- Source surfaces:
  - `Executive Revenue Source Truth`
  - `Forecast & Revenue Motions`
- Status: available now.
- Current state:
  - implemented in the first-draft monthly deck workspace
- Existing evidence:
  - regional columns already exist in `Executive Revenue Source Truth`.

#### Slide 3: North America Quarterly Pipeline Overview

- Purpose: same structure as EMEA.
- Source surfaces:
  - `Executive Revenue Source Truth`
  - `Forecast & Revenue Motions`
- Status: available now.
- Current state:
  - implemented in the first-draft monthly deck workspace

#### Slide 4: Asia Quarterly Pipeline Overview

- Purpose: same structure as EMEA.
- Source surfaces:
  - `Executive Revenue Source Truth`
  - `Forecast & Revenue Motions`
- Status: available now.
- Current state:
  - implemented in the first-draft monthly deck workspace

#### Slide 5: Commercial Approval Overview

- Purpose:
  - total approvals this quarter
  - approval compliance
  - approval-to-close time
  - list of Stage 3 land deals missing approval
- Source surfaces:
  - `Commercial Rhythm Control Tower`
  - operations design in [docs/DASHBOARD_PAGE_PLAN.md](docs/DASHBOARD_PAGE_PLAN.md)
- Status: can assemble quickly if the approval fields are real and populated.
- Current state:
  - implemented in the first-draft monthly deck workspace
  - current live snapshot on March 31, 2026 returned `0` Stage 3 land approval candidates, so the deck uses a compact overview instead of region appendices
- Live validation:
  - `Stage_20_Approval__c`, `Stage_20_Approval_Date__c`, and `Approval_Status__c` are confirmed live in the org.
- Known contract:
  - see `Commercial Approvals` section in [docs/DASHBOARD_PAGE_PLAN.md](docs/DASHBOARD_PAGE_PLAN.md).
- Data risk:
  - field existence is confirmed, but eligibility logic for "should have had approval" still needs a precise business rule.

#### Slides 6-8: Regional Commercial Approval Candidate Lists

- Purpose: stage-3 land opportunities with missing commercial approval, grouped by region.
- Format:
  - one slide per region if the monthly candidate list is material
  - otherwise one shared appendix slide split into region sections
- Source surfaces:
  - Salesforce report or CRMA action queue
- Status: can assemble quickly.
- Current state:
  - deferred because the current live candidate queue is empty
- Build note:
  - this is better as a queue/list surface than a story dashboard tile.

#### Slide 9: Renewals Tracking This Quarter

- Purpose:
  - what renewals are due this quarter
  - renewal value
  - risk band
  - owner
  - likely outcome proxy
- Source surfaces:
  - `Revenue Retention & Health`
  - `Commercial Rhythm Control Tower`
- Status: available now for due renewals and risk; can assemble quickly for "likely outcome" proxy.
- Current state:
  - implemented in the first-draft monthly deck workspace using CRM-side risk and timing proxies
- Existing evidence:
  - `Revenue Retention & Health` already has renewal pipeline and risk fields.
- Caveat:
  - "likelihood of renewing" is not yet a formal model here. Use `RiskLevel`, `ForecastCategory`, timing, and owner context as a proxy until a more explicit contract exists.

#### Slide 10: Churn Risk And Trend

- Purpose:
  - trend of churn
  - concentration of risk
  - any finance-sourced churn view that differs from CRM
- Source surfaces:
  - `Revenue Retention & Health`
  - finance feed not yet integrated
- Status:
  - CRM-side churn trend is available now.
  - Finance-enriched churn risk is a new data dependency.
- Current state:
  - CRM-side churn slide is implemented in the first-draft monthly deck workspace
  - Finance enrichment remains pending
- Required external step:
  - define who owns the Finance report, how it is delivered, and how it joins to the CRM layer.

#### Slide 11: Slipped Deals Analysis

- Purpose:
  - count and value of slipped deals
  - regional concentration
  - root-cause commentary
- Source surfaces:
  - `Forecast & Revenue Motions`
  - owner outreach or structured reason capture
- Status:
  - slip identification can assemble quickly.
  - root-cause commentary needs a new contract.
- Current state:
  - slip quantification and escalation queue are implemented in the first-draft monthly deck workspace
  - root-cause commentary is still pending owner input / contract
- Required new contract:
  - define standard slip reason categories and owner commentary workflow.

### Report 1 Build Buckets

#### Available Now

- Quarterly pipeline overview by region
- executive target/gap/pipeline ladder
- renewal pipeline and risk
- churn trend and churn detail

#### Can Assemble Quickly

- monthly deck narrative from current dashboard and KPI sources
- commercial approval overview if approval fields validate
- regional missing-approval candidate lists
- renewal likelihood proxy
- slipped-deal counts and regional exposure

#### Implemented Now

- reusable live snapshot refresher for Report 1
- reusable monthly Sales Directors PowerPoint generator
- one-command monthly run wrapper that writes snapshot, deck, summary, and thumbnail artifacts
- optional overlay JSON contract for Finance churn and slipped-deal commentary so external inputs can be appended without deck code changes

#### Needs New Contract Or Data

- live Finance-owned churn risk feed and recurring delivery path
- slipped-deal root-cause commentary collection process
- formal renewal-likelihood model
- publishable commercial approval section if approval fields are missing or incomplete

## Report 2

Goal: quarterly Sales Ops reporting product with a dashboard as the system of record and a shorter quarterly PowerPoint readout derived from it.

### Product Decision

- Final products:
  - CRMA dashboard
  - quarterly PowerPoint summary
- Dashboard is the primary build target.
- PowerPoint is downstream packaging, not the source of truth.
- Detailed page-level implementation contract lives in [sales-ops-dashboard-implementation-contract.md](sales-ops-dashboard-implementation-contract.md).

### Proposed Dashboard Pages

#### Page 1: Quarterly Sales Ops Summary

- Purpose:
  - one-page scoreboard for the quarter
  - highlight where data quality, compliance, forecast accuracy, or hygiene is off-track
- KPIs:
  - CRM completeness score
  - process compliance rate
  - forecast accuracy
  - past-due pipeline rate
  - stuck opportunity rate
- Source surfaces:
  - `Account Intelligence KPIs`
  - `Forecast & Revenue Motions`
  - `Commercial Rhythm Control Tower`
- Status: can assemble quickly.

#### Page 2: CRM Data Quality

- Purpose:
  - completeness by field
  - completeness by owner
  - action queues for missing values
- Source surfaces:
  - `Account Intelligence KPIs`
  - `Data Quality & Governance` design in [docs/DASHBOARD_PAGE_PLAN.md](docs/DASHBOARD_PAGE_PLAN.md)
- Status: available now for completeness-oriented measures.
- KPI contracts already present:
  - DUNS population
  - Unit Group population
  - win/loss commentary coverage
- Live validation:
  - `DUNS_No__c` and `Reason_Won_Lost__c` are confirmed live and populated enough for a first draft, though commentary coverage is only `34.9%` of closed opportunities in the current snapshot.

#### Page 3: Process Compliance

- Purpose:
  - deal-cycle compliance
  - path standardization
  - owner hygiene
  - renewal semantic confidence
- Source surfaces:
  - `Commercial Rhythm Control Tower`
  - KPI contracts from the workbook catalog
- Status: can assemble quickly if the compliance fields validate.
- Current state:
  - the explicit KPI helper field names from the workbook are not confirmed in the live org under those exact API names.
- Existing contract:
  - `Process Quality` expectations are already embedded in [scripts/audit_commercial_rhythm_control_tower.py](scripts/audit_commercial_rhythm_control_tower.py).

#### Page 4: Forecast Accuracy

- Purpose:
  - prior-quarter forecast accuracy
  - coverage status
  - promotion need
  - exception queue for low-confidence pipeline
- Source surfaces:
  - `Executive Revenue Source Truth`
  - `Forecast & Revenue Motions`
- Status: available now.
- Existing design contract:
  - forecast accuracy KPI already exists in [docs/DASHBOARD_PAGE_PLAN.md](docs/DASHBOARD_PAGE_PLAN.md).

#### Page 5: Pipeline Hygiene

- Purpose:
  - stuck opps
  - past-due opps
  - stage progression quality
  - owner-level gap queues
- Source surfaces:
  - `Forecast & Revenue Motions`
  - KPI contracts from the workbook catalog
- Status: available now for most measures.
- Existing evidence:
  - exception queues with record context already exist.

#### Page 6: Action Queue And Root Causes

- Purpose:
  - the dashboard must terminate in owner/accountable action queues
  - not just rollups
- Source surfaces:
  - `Forecast & Revenue Motions`
  - `Commercial Rhythm Control Tower`
  - `Account Intelligence KPIs`
- Status: can assemble quickly.

### Quarterly Deck Derived From Report 2 Dashboard

#### Slide 1: Quarterly KPI Summary

- Source page: Page 1
- Status: can assemble quickly.

#### Slide 2: CRM Data Quality

- Source page: Page 2
- Status: can assemble quickly.

#### Slide 3: Process Compliance

- Source page: Page 3
- Status: can assemble quickly once compliance fields validate.

#### Slide 4: Forecast Accuracy And Pipeline Hygiene

- Source pages:
  - Page 4
  - Page 5
- Status: available now for a first draft.

#### Slide 5: Root Causes And Next Actions

- Source page: Page 6
- Status: can assemble quickly.

### Report 2 Build Buckets

#### Available Now

- forecast accuracy source layer
- pipeline hygiene source layer
- data-quality completeness source layer
- action queues with record context

#### Can Assemble Quickly

- quarterly Sales Ops dashboard v1
- dashboard-driven quarterly deck
- owner-level hygiene and compliance review

#### Needs New Contract Or Data

- accuracy measures that require authoritative comparison logic, not just completeness
- any compliance KPI whose underlying fields are not yet validated in the live org

## Immediate Build Order

1. Build Report 2 dashboard first.
2. Use that dashboard to define the quarterly Sales Ops deck.
3. Build the Report 1 deck on top of the existing forecast, renewal, and approval source layers.
4. Add the live Finance churn feed and slipped-deal commentary collection workflow after the first draft deck exists.

## First Execution Slice

### Slice A

- Validate the commercial approval field contract in the live org.
- Validate the process-compliance field contract in the live org.
- Confirm the exact region breakdown for the deck.

### Slice B

- Draft the Report 2 dashboard page contract in repo-native terms.
- Keep it limited to:
  - data quality
  - process compliance
  - forecast accuracy
  - pipeline hygiene
  - action queue

### Slice C

- Draft the Report 1 PowerPoint slide contract with source queries and regional cuts.
- Mark Finance churn and slipped-deal commentary as explicit external inputs, not hidden assumptions.
- Keep those external inputs on a durable overlay contract so monthly runs stay reproducible.
