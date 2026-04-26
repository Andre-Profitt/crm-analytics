# Design Spec: Sales Director Data Dump Workbooks

Date: 2026-04-10
Status: Draft

## Purpose

Build a data extraction pipeline that produces one Excel workbook per MD-1 Sales Director, containing all the data a director needs for their monthly review. The workbook is the handoff artifact for Claude in Excel/PowerPoint to analyze and produce a polished, narrative-driven presentation.

The script does NOT generate slides. It generates the raw + computed data layer. Presentation is handled downstream.

## Architecture

Two-phase pipeline:

```
Phase 1: Extract          Phase 2: Transform
sf CLI / REST API  --->   JSON cache   --->   Excel workbook (one per director)
  SOQL queries              /tmp/                 openpyxl
  Reports API               director/             per-tab formatting
  Wave/SAQL                  *.json                conditional formatting
  ForecastingItem                                  data validation
  OpportunityFieldHistory
```

### Why two phases

- Phase 1 is fragile (auth, rate limits, API changes). Caching raw JSON means Phase 2 can iterate without re-hitting Salesforce.
- When quota/target data arrives from Finance, drop a `quotas.json` into the cache and re-run Phase 2 only.
- Debugging: inspect the JSON to verify data before blaming the Excel layer.

## Data Sources

### SOQL Queries (via `sf data query` or REST API)

| Query                | What it provides                            | Filter                                                                                                |
| -------------------- | ------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Open Pipeline        | All open opps with full field set           | `IsClosed = false AND {director_where}`                                                               |
| Won This Quarter     | Closed-won deals                            | `StageName = '8 - Won' AND CloseDate = THIS_QUARTER AND {director_where}`                             |
| Lost This Quarter    | Closed-lost deals                           | `StageName = '0 - Lost' AND CloseDate = THIS_QUARTER AND {director_where}`                            |
| Won Q1               | Q1 closed-won                               | `StageName = '8 - Won' AND CloseDate >= 2026-01-01 AND CloseDate <= 2026-03-31 AND {director_where}`  |
| Lost Q1              | Q1 closed-lost                              | `StageName = '0 - Lost' AND CloseDate >= 2026-01-01 AND CloseDate <= 2026-03-31 AND {director_where}` |
| Pushed Deals         | Open opps with PushCount > 0                | `IsClosed = false AND PushCount > 0 AND {director_where}`                                             |
| Forecast Categories  | Open opps by forecast category this quarter | `IsClosed = false AND CloseDate = THIS_QUARTER AND {director_where} GROUP BY ForecastCategoryName`    |
| New Pipeline Created | Opps created this quarter                   | `CreatedDate = THIS_QUARTER AND {director_where}`                                                     |

### Opportunity Field Set (for Pipeline Detail tab)

Selected from the org's available fields (verified via describe):

```
Name, Account.Name, Owner.Name, StageName, CloseDate,
APTS_Opportunity_ARR__c, Opportunity_Average_ACV__c, APTS_Forecast_ARR__c,
ForecastCategoryName, Probability, Type, APTS_Opportunity_Sub_Type__c,
PushCount, AgeInDays, LastStageChangeInDays, LastActivityDate, LastActivityInDays,
Sales_Director_Book__c, Sales_Region__c, Account_Unit_Group__c,
Account.Industry, Account.BillingCountryCode,
Stage_20_Approval__c, Stage_20_Approval_Date__c, Approval_Status__c,
Risk_Assessment_Level__c, Risk_Assessment_Comment__c,
NextStep, CreatedDate, Calculated_Close_Date__c,
New_Stage_10_created_Date__c, New_Stage_15_Date__c, New_Stage_20_Date__c,
New_Stage_30_Date__c, New_Stage_40_Date__c, New_Stage_6_Date__c,
New_Stage_7_Date__c, New_Stage_50_Date__c,
Reason_Won_Lost__c, Sub_Reason__c, Lost_to_Competitor__c, Lost_Comments__c,
Contract__c, APTS_Contract_Start_Date__c, APTS_Contract_End_Date__c,
HasOpenActivity, HasOverdueTask, IqScore
```

### Forecasting Module (Collaborative Forecasting)

5 active forecast types in the org:

| Type                         | ID                   | Measures                 |
| ---------------------------- | -------------------- | ------------------------ |
| Opportunity ACV              | `0Db7S000000zDaCSAU` | Annual Contract Value    |
| Opportunity ARR              | `0Db7S000000zDaMSAU` | Annual Recurring Revenue |
| Opportunity Quota Retirement | `0Db7S000000zDaHSAU` | Commission-basis amount  |
| Product Family ACV           | `0DbQA0000004j8D0AQ` | ACV by product family    |
| Renewal ACV                  | `0DbQA0000009vrt0AA` | Renewal-specific ACV     |

Queries per type per period:

| Object                | Query                                                | Purpose                                                          |
| --------------------- | ---------------------------------------------------- | ---------------------------------------------------------------- |
| ForecastingItem       | `PeriodId = {period} AND ForecastingTypeId = {type}` | Forecast grid: owner × category × amount for each of the 5 types |
| ForecastingFact       | `PeriodId = {period}`                                | Links forecast items to actual Opportunity IDs                   |
| ForecastingAdjustment | `PeriodId = {period}`                                | Manager overrides with notes (currently 0 records — not in use)  |
| ForecastingQuota      | `StartDate in range`                                 | Quotas (only 2023 data — no 2026 quotas loaded)                  |

Period IDs (resolved):

- Q1 2026: `0267S000000v3sKQAQ` (2026-01-01 to 2026-03-31)
- Q2 2026: `0267S000000v3sLQAQ` (2026-04-01 to 2026-06-30)

Cross-validation: the Forecasting module's `Closed` category totals should match SOQL `StageName = '8 - Won'` totals. Discrepancies flag data integrity issues.

Q1 baseline (for reference): ACV EUR 337.9M closed, ARR EUR 106.5M closed, Renewal ACV EUR 236.4M closed.
Q2 current: ACV EUR 112M commit + EUR 303.5M best case, ARR EUR 104M commit + EUR 276M best case.

### OpportunityFieldHistory

| Query                                                  | Purpose                                        |
| ------------------------------------------------------ | ---------------------------------------------- |
| `Field = 'CloseDate' AND CreatedDate in Q1`            | Close date movements during Q1 (1,411 records) |
| `Field = 'ForecastCategoryName' AND CreatedDate in Q1` | Forecast category changes during Q1            |
| `Field = 'StageName' AND CreatedDate in Q1`            | Stage progression/regression during Q1         |

Note: `OldValue` is not filterable in SOQL — must pull all Q1 changes and filter client-side for "moved out of Q1" (OldValue in Q1 range, NewValue >= Q2).

### D1 Dashboard (filtered per director)

Dashboard `01ZTb00000FSP7hMAH` — 8 components, 4 filters.

Fetch via PUT (to apply filters) then GET. Extract `componentData[].reportResult` for each widget.

Director filter params use dashboard filter option IDs (Industry, Legal Country, Sales Region, Account Unit Group).

### D2 Dashboard (global — no per-director filters)

Dashboard `01ZTb00000FSP9JMAX` — 15 components, no filters.

Fetch once, extract all component data. Director-level breakdown comes from the underlying report groupings (by Owner, by Sales Region).

### CRMA Dashboards (via Wave `/wave/query` endpoint with SAQL)

10 datasets across 5 dashboards. Director filtering via `SalesRegion`, `UnitGroup`, or equivalent dataset dimension.

#### Dataset 1: `Revenue_Retention_Health` (0FbTb000001A8DRKA0)

Source: Revenue Retention & Health dashboard. RowType-partitioned.

| Metric                         | SAQL Pattern                                                    | Value           |
| ------------------------------ | --------------------------------------------------------------- | --------------- |
| GRR (Gross Revenue Retention)  | `(StartingARR - ChurnARR) / StartingARR * 100`                  | Target: 95%     |
| NRR (Net Revenue Retention)    | `(StartingARR + ExpansionARR - ChurnARR) / StartingARR * 100`   | Target: 110%    |
| ARR Waterfall                  | Starting → Renewal Won → Expansion → New Logos → Churn → Ending | Annual          |
| Churn by Owner                 | `sum(ChurnARR)` grouped by owner                                | Top 15          |
| Renewal Pipeline by Risk Level | Segmented by `RiskLevel` (Overdue/Critical/High/Medium/Low)     | ARR + count     |
| Renewal Confidence             | `CoveredDeals / RenewalDeals * 100` per owner                   | Coverage rate   |
| Renewal Save Queue             | Priority-scored at-risk renewals                                | Actionable list |

Pre-computed fields: `StartingARR`, `RenewalWonARR`, `ExpansionARR`, `ChurnARR`, `EndingARR`, `NewLogoARR`, `RiskLevel`, `DaysUntilClose`, `AtRiskRenewalValue`, `CoverageRate`.

#### Dataset 2: `Sales_Velocity_Annual` (0FbTb000001BPTxKAO)

Source: Sales Velocity Annual dashboard. RowType-partitioned.

| Metric                   | SAQL Pattern                                                     | Value              |
| ------------------------ | ---------------------------------------------------------------- | ------------------ |
| Sales Velocity (EUR/day) | `sum(SalesVelocityEURPerDay)` where RowType = "qualified_stage3" | Per region         |
| Win Rate by Industry     | `sum(WonQualifiedCount) / sum(ClosedQualifiedCount) * 100`       | By industry        |
| Win Rate by Product      | Same formula, RowType = "win_rate_product"                       | By product family  |
| Stage Conversion Funnel  | `sum(AdvancedCount) / sum(EnteredCount) * 100` per stage pair    | S1→S2, S2→S3, etc. |
| Cohort Maturity          | Quarterly cohorts with maturity %, win rates                     | YoY comparison     |
| Avg Deal Size            | `sum(QualifiedValueEUR) / sum(QualifiedOppCount)`                | By segment/region  |

Pre-computed fields: `QualifiedOppCount`, `SalesVelocityEURPerDay`, `QualifiedValueEUR`, `AppliedWinRatePct`, `AdvancedCount`, `EnteredCount`, `RegionGroup`, `Segment`.

#### Dataset 3: `Forecast_Revenue_Motions` (0FbTb000001A0NxKAK) + Weekly Snapshots

Source: Forecast & Revenue Motions dashboard. Also loads `Weekly_Forecast_Summary` and `Weekly_Forecast_Opps`.

| Metric                 | SAQL Pattern                                | Value                       |
| ---------------------- | ------------------------------------------- | --------------------------- |
| Gap-to-Plan            | `Plan - Closed - Commit - BestCase`         | Per owner                   |
| Forecast Accuracy      | `actual_closed / forecast_amount * 100`     | vs prior forecast           |
| Stage Aging Pressure   | `avg(DaysInCurrentStage)` by stage          | Identifies bottlenecks      |
| WoW Forecast Timeline  | Weekly commit and best-case evolution       | Historical snapshots        |
| WoW Push Tracking      | Deal-level close date pushes week-over-week | From `Weekly_Forecast_Opps` |
| WoW Category Migration | Deals that changed forecast category WoW    | Movement analysis           |
| Owner Gap Analysis     | Pipeline by owner vs plan target            | Per-rep gap                 |
| Deal Review Candidates | CASE-based flagging for manager review      | Actionable list             |

#### Dataset 4: `Pipeline_Opportunity_Operations` (0FbTb000001A0KjKAK)

Source: Sales Ops Data Quality & Forecast Accuracy dashboard.

Pre-computed per-opp fields NOT available in standard SOQL:

- `PastDueCount` — number of times close date went past
- `StaleCount` — staleness indicator
- `BackwardMoveCount` — stage regression count
- `DaysInCurrentStage` — computed stage duration
- Pipeline Hygiene Rate: `(clean_deals / total) * 100` where clean = no past-due, no stale, no backward move, PushCount < 2

#### Dataset 5: `Account_Intelligence` (undeclared, loaded via SAQL)

| Metric               | Field                                               |
| -------------------- | --------------------------------------------------- |
| Data Quality Score   | `DataQualityScore` (numeric, band: Low/Medium/High) |
| DUNS Fill Rate       | `HasDUNS == "true"`                                 |
| Unit Group Fill Rate | `HasUnitGroup == "true"`                            |
| Axioma ID Fill Rate  | `HasAxiomaId == "true"`                             |
| KYC Status           | `KYCStatus`                                         |

#### Dataset 6: `Opp_Mgmt_KPIs` (0FbTb0000019llVKAQ)

Source: Sales Process Compliance KPIs dashboard.

| Metric            | SAQL Pattern                                                                                            |
| ----------------- | ------------------------------------------------------------------------------------------------------- |
| Velocity Z-Score  | `avg(DaysInStage)`, `stddev(DaysInStage)`, `max(DaysInStage)` per stage — statistical outlier detection |
| Stuck ARR Trend   | Deals in S3/S4 with DaysInStage > 30, YoY comparison                                                    |
| Win/Loss Velocity | `avg(DaysToClose)` by Type (Land/Expand/Renewal)                                                        |
| Slip Distribution | Close date slip bucketed                                                                                |
| Weighted Score    | Per-opp weighted score, top 25                                                                          |

#### Dataset 7: `Commercial_Rhythm_Control_Tower` (undeclared, loaded via SAQL)

| Metric              | Field                    |
| ------------------- | ------------------------ |
| Renewal Coverage    | `CoveredRenewalOppCount` |
| Next Step Coverage  | `NextStepCoveredFlag`    |
| Ownership Alignment | `OwnershipAlignedFlag`   |

## Workbook Structure (per director)

### Tab 1: Scorecard

**Director's question: "Where do I stand?"**

Summary KPIs computed from all other tabs + CRMA datasets. Two-column layout: KPI name | Value.

Pipeline Health:

- Total open pipeline ARR
- Deal count (open)
- Average deal size (ARR / deal count)
- Weighted pipeline (sum of Probability \* ARR for each opp)
- Pipeline coverage ratio (total pipeline / remaining target) — placeholder until quotas arrive
- New pipeline created this quarter (ARR of opps where CreatedDate = THIS_QUARTER)
- Pipeline by stage (mini table: stage, count, ARR)
- Pipeline hygiene rate (from `Pipeline_Opportunity_Operations`: % clean deals)

Execution:

- Won this quarter — count + ARR
- Lost this quarter — count + ARR
- Win rate by count: won / (won + lost)
- Win rate by ARR: won ARR / (won ARR + lost ARR)
- Average sales cycle — days by type: Land / Expand / Renewal (from `Opp_Mgmt_KPIs`)
- Sales velocity — EUR/day (from `Sales_Velocity_Annual`)
- Stage conversion rates — S1→S2, S2→S3, etc. (from `Sales_Velocity_Annual`)

Retention:

- GRR — Gross Revenue Retention % (from `Revenue_Retention_Health`, target: 95%)
- NRR — Net Revenue Retention % (from `Revenue_Retention_Health`, target: 110%)
- Renewal pipeline ARR — open renewals this quarter
- Renewal coverage rate — covered / total renewal deals (from `Commercial_Rhythm_Control_Tower`)
- Churn ARR — churned this year

Forecast:

- Forecast accuracy — actual closed / prior forecast \* 100 (from `Forecast_Revenue_Motions`)
- Gap-to-plan — Plan - Closed - Commit - BestCase (placeholder until quotas arrive)
- WoW commit change — week-over-week commit delta (from `Weekly_Forecast_Summary`)

Risk:

- Stale deals (30d no activity) — count + ARR (per D2 report threshold)
- High-value stale (60d, EUR 1M+ ARR) — count + ARR (per D2 report threshold)
- Pushed 5+ times — count + ARR
- Overdue close date — count + ARR
- Aging 365+ days — count + ARR
- Backward stage moves — count (from `Pipeline_Opportunity_Operations`)

Process Compliance:

- Commercial approval rate (approved / total at stage 3+)
- Missing approval candidates — count + ARR
- Missing quote type — count
- Missing next step (mid-stage) — count
- Missing amount — count
- Data quality score — avg across territory accounts (from `Account_Intelligence`)

### Tab 2: Q1 Review

**Director's question: "Did we deliver last quarter?"**

Section A — Q1 Forecast vs Actual:

- ForecastingItem data for Q1 period: Commit, Best Case, Pipeline totals
- Actual Closed-Won: count + ARR
- Actual Closed-Lost: count + ARR
- Delta: Commit forecast minus Closed-Won = miss/hit

Section B — Deals Pushed Out of Q1:

- OpportunityFieldHistory where Field = 'CloseDate', OldValue in Q1 range, NewValue >= Q2
- Columns: Account, Opportunity, Owner, ARR, Old Close Date, New Close Date, Current Stage

Section C — Forecast Category Movement:

- OpportunityFieldHistory where Field = 'ForecastCategoryName' during Q1
- Columns: Account, Opportunity, Owner, ARR, Old Category, New Category, Date

Section D — Q1 Won Deals:

- Full detail of won deals: Account, Opportunity, Owner, ARR, ACV, Close Date, Sales Cycle Days

Section E — Q1 Lost Deals:

- Full detail: Account, Opportunity, Owner, ARR, Close Date, Reason, Sub-Reason, Competitor

### Tab 3: Q2 Outlook

**Director's question: "Will we hit this quarter?"**

- Current Q2 forecast by category (from ForecastingItem Q2 period + SOQL)
- Open pipeline with Q2 close dates by stage
- Commit deals detail (table)
- Best Case deals detail (table)
- Coverage: pipeline total / quota target (placeholder)
- Deals expected to close this month
- Gap analysis: target - closed-won - commit = gap to fill

### Tab 4: Rep Performance

**Director's question: "Who needs help?"**

One row per rep (Opportunity Owner) within the director's territory:

| Rep | Open Pipeline ARR | Deal Count | Avg Deal Size | Won ARR (Q) | Lost ARR (Q) | Win Rate | Stale Deals | Pushed Deals | Last Activity (latest) | Missing Approvals |

Sorted by Open Pipeline ARR descending.

### Tab 5: Pipeline Detail

**Director's question: "Show me every deal."**

All open opportunities for this director, full field set (see Opportunity Field Set above).
Sorted by ARR descending.
Pre-applied conditional formatting:

- Red: PushCount >= 5
- Amber: LastActivityInDays > 30
- Red: LastActivityInDays > 60 AND ARR >= 1,000,000
- Red: CloseDate < TODAY (overdue)

### Tab 6: Won-Lost

**Director's question: "What's our win pattern?"**

All closed deals (won + lost) this quarter + Q1.
Columns: Account, Opportunity, Owner, Type, Stage, ARR, ACV, Close Date, Created Date, Sales Cycle Days, Reason Won/Lost, Sub-Reason, Competitor, Lost Comments.

### Tab 7: Commercial Approval

**Director's question: "What's stuck in process?"**

Section A — Approval State Summary:

- Count + ARR: Approved, Pending, No Approval Necessary

Section B — Missing Approval Candidates:

- Opps at Stage 3+ where Stage_20_Approval\_\_c = false
- Columns: Account, Opportunity, Owner, Stage, ARR, Close Date, Next Step

Section C — Approved YTD:

- Approved deals this year with detail

### Tab 8: Renewals & Retention

**Director's question: "What's due, are we covered, and what's churning?"**

Section A — Retention KPIs (from `Revenue_Retention_Health`):

- GRR % (target: 95%)
- NRR % (target: 110%)
- ARR Waterfall: Starting → Renewal Won → Expansion → New Logos → Churn → Ending
- Churn ARR this year + churn rate

Section B — Renewal Pipeline by Risk Level (from `Revenue_Retention_Health`):

- Segmented by RiskLevel: Overdue / Critical / High / Medium / Low
- Columns: Account, Opportunity, Owner, ACV, RiskLevel, DaysUntilClose, ForecastCategory

Section C — Renewal Confidence by Owner (from `Commercial_Rhythm_Control_Tower`):

- CoveredDeals, MissingMetricDeals, AtRiskRenewalValue, CoverageRate per rep

Section D — Renewal Pipeline Detail (from D1 reports + CRMA enrichment):

- Columns: Account, Opportunity, Owner, ACV, Probability, Close Date, Stage, RiskLevel, OwnershipAlignment

Section E — Churn Detail (from `Revenue_Retention_Health`):

- Lost renewal deals with root cause classification

### Tab 9: Risk Register

**Director's question: "What keeps me up at night?"**

Combined view of all risk signals, one row per opp, with a composite risk score:

| Account | Opportunity | Owner | ARR | Risk Flags | Risk Score |

Risk flags (each adds to composite score):

- Pushed 5+ times (+3)
- High-value stale (60d, EUR 1M+) (+3)
- Overdue close date (+2)
- Stale 30d+ (+1)
- Aging 365+ days (+2)
- No activity ever (+3)
- Missing approval at stage 3+ (+1)
- Low probability (<50%) in quarter (+1)
- Backward stage move (from `Pipeline_Opportunity_Operations`) (+2)
- Velocity z-score outlier — deal in stage significantly longer than avg (from `Opp_Mgmt_KPIs`) (+2)
- Deal flagged for manager review (from `Forecast_Revenue_Motions` deal review candidates) (+1)

Sorted by Risk Score descending. Only opps with score > 0 included.

Additional CRMA-enriched columns on each row:

- `DaysInCurrentStage` (from `Pipeline_Opportunity_Operations`)
- `BackwardMoveCount` (from `Pipeline_Opportunity_Operations`)
- `StaleCount` (from `Pipeline_Opportunity_Operations`)
- `PastDueCount` (from `Pipeline_Opportunity_Operations`)

### Tab 10: Data Quality

**Director's question: "Is my team keeping CRM clean?"**

Per-rep breakdown of data quality issues from D2 reports:

| Rep | Stale 30d | High-Value Stale | No Activity Ever | Overdue Close | Missing Amount | Missing Quote Type | Missing Next Step | Missing Approval Flow | Aging 365+ | Total Issues |

Plus a summary row for the territory total.

### Tab 11: Quota & Targets

**Placeholder tab.**

Structure:
| Period | Territory | Quota ARR | Closed Won ARR | Remaining | Coverage Ratio |

Pre-filled with territory name and period labels. Quota ARR column left blank — to be populated when Finance delivers regional targets.

### Tab 12: Sources & Lineage

**Purpose: trace any data point back to its origin for debugging and audit.**

This tab has three sections:

**Section A — Source Registry:**
One row per data source used in the workbook.

| Source ID | Source Type             | Name                 | API Endpoint / Query                                                | Record Count | Extracted At         |
| --------- | ----------------------- | -------------------- | ------------------------------------------------------------------- | ------------ | -------------------- |
| S1        | SOQL                    | Open Pipeline        | `SELECT ... FROM Opportunity WHERE IsClosed = false AND {where}`    | 147          | 2026-04-10T14:32:00Z |
| S2        | SOQL                    | Won Q1               | `SELECT ... FROM Opportunity WHERE StageName = '8 - Won' AND ...`   | 23           | 2026-04-10T14:32:05Z |
| S3        | ForecastingItem         | Q1 ACV Forecast      | `ForecastingTypeId = 0Db7S...DaCSAU, PeriodId = 0267S...sKQAQ`      | 284          | 2026-04-10T14:32:10Z |
| S4        | D1 Dashboard            | Pipeline Overview    | `PUT/GET /analytics/dashboards/01ZTb00000FSP7hMAH` with filter3=... | 1            | 2026-04-10T14:32:30Z |
| S5        | D2 Dashboard            | Stale Opportunities  | Report `00OTb000008TZgvMAG` via D2 component `01aTb00000CmjwvIAB`   | 45           | 2026-04-10T14:32:35Z |
| S6        | CRMA/SAQL               | Revenue Retention    | `load "Revenue_Retention_Health"; filter ... group by ...`          | 12           | 2026-04-10T14:32:40Z |
| S7        | OpportunityFieldHistory | Q1 CloseDate Changes | `Field = 'CloseDate' AND CreatedDate in Q1`                         | 1411         | 2026-04-10T14:32:45Z |
| ...       | ...                     | ...                  | ...                                                                 | ...          | ...                  |

**Section B — Tab-to-Source Map:**
Links each workbook tab (and section within tab) to the source(s) that feed it.

| Tab                  | Section            | Source IDs                 | Notes                                        |
| -------------------- | ------------------ | -------------------------- | -------------------------------------------- |
| Scorecard            | Pipeline Health    | S1                         | Computed from open pipeline SOQL             |
| Scorecard            | GRR / NRR          | S6                         | From Revenue_Retention_Health CRMA dataset   |
| Scorecard            | Win Rate           | S1, S2                     | Won / (Won + Lost) from SOQL                 |
| Scorecard            | Velocity           | S6 (Sales_Velocity_Annual) | Pre-computed in CRMA dataset                 |
| Q1 Review            | Forecast vs Actual | S3, S2                     | ForecastingItem vs Won SOQL                  |
| Q1 Review            | Deals Pushed Out   | S7                         | OpportunityFieldHistory client-side filtered |
| Pipeline Detail      | (all)              | S1                         | Direct SOQL output                           |
| Risk Register        | Risk Score         | S1, S5, S6                 | Composite from SOQL + D2 + CRMA fields       |
| Renewals & Retention | GRR/NRR/Waterfall  | S6                         | Revenue_Retention_Health dataset             |
| Data Quality         | (all)              | S5                         | D2 dashboard component data                  |
| ...                  | ...                | ...                        | ...                                          |

**Section C — Column Lineage:**
For every column in every tab, documents the field origin.

| Tab             | Column          | Source   | Field / Computation                                       |
| --------------- | --------------- | -------- | --------------------------------------------------------- |
| Pipeline Detail | ARR             | S1       | `APTS_Opportunity_ARR__c`                                 |
| Pipeline Detail | Days In Stage   | S6       | `DaysInCurrentStage` from Pipeline_Opportunity_Operations |
| Scorecard       | Avg Deal Size   | S1       | `SUM(APTS_Opportunity_ARR__c) / COUNT(Id)`                |
| Risk Register   | Risk Score      | S1+S5+S6 | Composite: see Risk Register tab spec for weights         |
| Q1 Review       | Forecast Commit | S3       | `ForecastAmount WHERE ForecastCategoryName = 'Commit'`    |
| ...             | ...             | ...      | ...                                                       |

**Implementation:** The extract script auto-populates Section A at extraction time (source ID, query, record count, timestamp). Section B and C are static metadata written once during workbook generation and updated if the tab structure changes.

## Director Filter Logic

Each director's data is filtered using the preset definitions from `config/sales_director_md1_presets.json`.

SOQL WHERE clause built by `_soql_where()` which maps:

- `Account.Region__c` — Region filter
- `Account.Industry` — Industry filter (comma-separated for IN clause)
- `Account.BillingCountryCode` — Legal Country filter
- `Account_Unit_Group__c` — Account Unit Group filter (on Opportunity, not Account)

D1 dashboard filters use option IDs per `DIRECTOR_D1_FILTERS` mapping.

CRMA/Wave queries filter on `Sales_Director_Book__c` or equivalent dataset dimension.

## Key Thresholds (from org — do not modify)

| Metric               | Threshold                                               | Source Report                |
| -------------------- | ------------------------------------------------------- | ---------------------------- |
| General staleness    | LAST_ACTIVITY < LAST 30 DAYS                            | Stale Opportunities - CFQ    |
| High-value staleness | LAST_ACTIVITY < LAST 60 DAYS AND Forecast ARR >= EUR 1M | High Value Stale Deals       |
| Never-contacted      | LAST_ACTIVITY = (empty)                                 | Active Opps: No Activity     |
| Pipeline aging       | AGE > 365 days                                          | Aging Pipeline 365 Plus Days |
| Overdue close        | CLOSE_DATE < TODAY                                      | Overdue Opportunities        |
| Low probability      | PROBABILITY < 50%                                       | Low Probability In Quarter   |

## Authority Policy Thresholds (for flagging)

| ACV Range       | Required Authority     |
| --------------- | ---------------------- |
| > EUR 150,000   | Director/Manager (F/G) |
| > EUR 250,000   | VP/AVP/Sr Director (E) |
| > EUR 800,000   | SVP/CVP (D)            |
| > EUR 2,000,000 | EMB/ExCo (B/C)         |
| > EUR 3,000,000 | CEO (A)                |

Discount thresholds: >5% needs Director+, >15% needs VP+, >25% needs SVP+, >40% needs EMB+.

## Output

```
output/director_data_dumps/2026-04-10/
  Sales Director Data - Jesper Tyrer (APAC).xlsx
  Sales Director Data - Sarah Pittroff (Central Europe).xlsx
  Sales Director Data - Francois Thaury (Southern Europe).xlsx
  Sales Director Data - Dan Peppett (UK & Ireland).xlsx
  Sales Director Data - Christian Ebbesen (NL & Nordics).xlsx
  Sales Director Data - Mourad Essofi (Middle East & Africa).xlsx
  Sales Director Data - Megan Miceli (Canada).xlsx
  Sales Director Data - Patrick Gaughan (NA Asset Management).xlsx
  Sales Director Data - Adam Steinhaus (Pension & Insurance).xlsx
```

## Dependencies

- Python 3.13
- `openpyxl` (Excel writing with formatting)
- `requests` (REST API calls)
- `sf` CLI (auth via `sf org display`)
- `config/sales_director_md1_presets.json` (director filter definitions)

## Knowledge Corpus Reference

All thresholds, field names, report configurations, and dashboard metadata documented in:
`docs/2026-04-10-dashboard-report-knowledge-corpus.md`

## What This Does NOT Cover

- Slide generation (downstream: Claude in Excel/PPT)
- Quota/target numbers (pending from Finance)
- Finance-backed churn data (pending from Alex P)
- Owner commentary on slipped deals (process dependency)
- Calendar-quarter regrouping (schema dependency)
