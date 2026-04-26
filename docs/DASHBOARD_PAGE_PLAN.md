# CRM Analytics Dashboard Page Plan v4

Comprehensive page-by-page specification for the 5-dashboard suite.
Revised after 5 rounds of expert review + dataset reuse audit mapping existing org datasets to planned data sources.

**Changes from v3**: Replaced 6 of 7 planned recipes with existing Python-builder datasets (already active, refreshed daily). Only Weekly_Forecast_Summary requires a new build (from OpportunityFieldHistory — Opportunity_Snapshot\_\_c is dead, 2012-2015 data only). Updated all page data source references to use actual dataset names. Added ML-enriched fields (WinScore, SlipRiskScore, MarkovWinProb, etc.) available from existing datasets. Resolved Opp_Geo_Map status (upload succeeded but dataset not persisted — rebuild needed).

**Changes from v2**: Fixed Rep_Scorecard grain mismatch (expanded to Owner×Quarter×ProductFamily). Stored ratio components instead of pre-computed ratios. Added ARR_Bridge + Pipeline_Health recipes (5→7). Fixed Account_360 Cartesian explosion. Added missing Isolation sections to 13 pages, missing Interactions to 6 pages. Fixed Page 1.3 isolation contradiction, Page 3.1 interaction bug, Page 3.3 toggle binding. Added cross-dashboard drills. Corrected summary table (178→172 widgets).

**Changes from v1**: Merged 3 pages (1.3+1.9, 1.6+1.7, 3.1+3.4). Merged Dashboard 5→2. Relocated 1.5→5.1, 4.3→5.4. Removed 12 unbuildable widgets. Fixed 7 viz anti-patterns.

---

## DATA BACKBONE (build BEFORE dashboards)

### Existing Datasets (from Python builder pipeline — actively refreshed via CSV upload)

All 25 datasets are refreshed daily via the existing Python builder upload pipeline. **6 of 7 originally planned recipes are already covered** by these datasets, which include ML-enriched fields not in our original plan.

| Existing Dataset                    | Replaces Planned Recipe       | Key Fields Beyond Original Plan                                                    | Pages Served                        |
| ----------------------------------- | ----------------------------- | ---------------------------------------------------------------------------------- | ----------------------------------- |
| **Executive_Revenue_Forecast**      | Rep_Scorecard                 | ARR, QuotaAmount, CommitCallARR, BestCaseOpenARR, ConfidenceWeightedARR, WinScore  | 1.1, 1.5, 1.6, 1.7                  |
| **Forecast_Revenue_Motions**        | Rep_Scorecard (extended)      | 57 dims, 37 meas: MotionType, RiskScore, SalesCycleDuration, RegressionForecastARR | 1.1, 1.4, 1.5, 2.2                  |
| **Pipeline_Analytics**              | Pipeline_Health               | PushCount, PushBucket, AgeBucket, DaysInCurrentStage, MarkovWinProb, SlipRiskScore | 1.1, 1.2                            |
| **Pipeline_Opportunity_Operations** | Opp_Stage_Transitions         | DaysInStage, PushCount, BackwardMoveCount, SlipRiskScore, StaleCount, AtRiskARR    | 1.1, 1.2, 1.3                       |
| **Pipeline_Transitions**            | Opp_Stage_Transitions (basic) | FromStage, ToStage, Probability                                                    | 1.2 (stage conversion rates)        |
| **Revenue_Retention_Health**        | ARR_Bridge                    | NRR, GRR, ChurnARR, StartingARR, EndingARR, ExpansionARR, NewLogoARR               | 2.1, 2.3, 2.4                       |
| **Customer_Account_Health**         | Account_360                   | HealthScore, NRRProxy, DataQualityScore, KYCStatus, AuM, RenewalRiskScore          | 4.1, 4.2, 5.4                       |
| **Lead_Funnel**                     | Lead_Funnel                   | 55 dims, 31 meas: ConversionPropensityScore, DaysToConvert, SLABreachCount         | 3.1, 3.3                            |
| **BDR_Operating_Rhythm**            | (no planned recipe)           | 81 dims, 39 meas — complete BDR analytics                                          | 3.1, 3.2, 3.3                       |
| **BDR_Lead_Attribution**            | (no planned recipe)           | SourceType, ContactRole, ResponseTimeBand, OppOutcome, FirstTouchHours             | 3.1                                 |
| **Product_Portfolio_Whitespace**    | (no planned recipe)           | WhitespaceARR, WhitespaceScore, ExpansionScore, BillingCountry, ProductFamily      | 2.5, (potential geo source for 2.1) |
| **Contract_Operations_Renewals**    | (no planned recipe)           | Contract lifecycle metrics                                                         | 2.3, 5.3                            |
| **KPI_Scorecard**                   | (no planned recipe)           | Pre-computed KPI values with RAG status                                            | KPI strip backup                    |

#### Additional ML/Analytics Datasets (bonus capabilities)

| Dataset                          | What It Adds                                                  |
| -------------------------------- | ------------------------------------------------------------- |
| **Pipeline_Trendlines**          | 18-month trend with regression lines (R², upper/lower bounds) |
| **Pipeline_Survival**            | Kaplan-Meier survival probability curves by stage group       |
| **Pipeline_Monte_Carlo**         | Scenario simulation: projected revenue, VaR 5%, CVaR 5%       |
| **Whitespace_Propensity_Scores** | ML whitespace scoring for cross-sell targeting                |
| **Next_Family_Recommendations**  | Product family recommendation engine output                   |
| **Lead_Conversion_Scores**       | ML lead conversion probability                                |
| **Renewal_Risk_Scores**          | ML renewal risk predictions                                   |
| **Data_Freshness_Monitor**       | Meta-dataset tracking data staleness                          |
| **ML_Model_Monitor**             | Meta-dataset tracking ML model health                         |

### New Dataset Required: Weekly_Forecast_Summary (BUILD)

**Source**: OpportunityFieldHistory (100K records, Sep 2024-today). NOT Opportunity_Snapshot**c (dead — 2012-2015 data only, Execution_Date**c entirely NULL).

**Builder script**: `/Users/test/crm-analytics/scripts/build_weekly_forecast_summary.py`

**Output (a): `Weekly_Forecast_Summary`** — 1 row per Week × ForecastCategory
Columns: Week, ForecastCategory, TotalARR, OppCount, AvgARR

**Output (b): `Weekly_Forecast_Opps`** — 1 row per Opportunity × Week
Columns: Week, OpportunityId, OpportunityName, OwnerName, Region, ForecastCategory, CloseDate, ARR, PushCount (cumulative pushes — CloseDate moved later)

**Logic**: Reconstruct each opportunity's ForecastCategory and CloseDate at end-of-each-ISO-week by starting from current values and rolling back changes that happened after that week. ~700-1200 forecast-relevant field changes per month.

### Geographic Dataset (REBUILD)

- **Opp_Geo_Map**: Upload job succeeded historically but dataset is NOT in current org — needs rebuild
- Source: Account.BillingCountry joined with Opportunity ARR, or leverage `Product_Portfolio_Whitespace.BillingCountry` dimension
- Country field must have geographic XMD metadata: `"isGeographic": true`, `"geoType": "country"`

### Replicated Datasets (connected objects — auto-sync)

Primary: Opportunity, Account, Contact, Lead, Task, Event, Contract, Campaign, CampaignMember, User, Product2, OpportunityLineItem, OpportunityHistory, OpportunityFieldHistory, ForecastingQuota, ForecastingItem, Adoption_Score\_\_c, NPS_Survey\_\_c, Customer_Intelligence\_\_c

Note: Opportunity_Snapshot\_\_c removed — dead data (2012-2015), not useful for any current widget.

### XMD Configuration

- Record links: Account.Name, Opportunity.Name (linkTemplateEnabled: true)
- Currency formatting: all ARR fields → `"[$#,##0",1]"`
- Stage colors: Won=#04844B, Lost=#D4504C, Pipeline=#1589EE
- Forecast category colors: Commit=#04844B, Best Case=#FFB75D, Pipeline=#1589EE, Omitted=#CCCCCC

---

## DASHBOARD 1: SALES PIPELINE & FORECAST

**Audience**: CRO, VPs of Sales, Regional Heads, Deal Desk, Sales Managers, AEs
**Cadence**: Weekly forecast call, daily pipeline review
**Global Filters**: Period (dateselector on FiscalQuarter), Region (pillbox: SC EMEA | SC North America | SC Asia), Type (pillbox: Land | Expand | Renewal)

### Page 1.1 — Revenue Command Center

**Story**: "Where do we stand against target, and what changed this week?"

| #   | Widget                        | Type         | Viz       | Metric                                                                 | Source                                     | Isolation            |
| --- | ----------------------------- | ------------ | --------- | ---------------------------------------------------------------------- | ------------------------------------------ | -------------------- |
| 1   | Quota Attainment              | number       | KPI       | Closed Won / Quota × 100                                               | ARR, ForecastingQuota                      | scoped: date, region |
| 2   | Closed Won ARR                | number       | KPI       | Total Closed Won ARR this period                                       | APTS_Opportunity_ARR\_\_c WHERE IsWon      | scoped: date, region |
| 3   | Pipeline Value                | number       | KPI       | Total open pipeline ARR                                                | APTS_Opportunity_ARR\_\_c WHERE stages 1-6 | scoped: date, region |
| 4   | Pipeline Coverage             | number       | KPI       | Pipeline ÷ Remaining Quota (target: 3×)                                | Calculated                                 | scoped: date, region |
| 5   | Win Rate                      | number       | KPI       | Won ÷ (Won + Lost) trailing 12mo                                       | Calculated from stage                      | **global benchmark** |
| 6   | Revenue Waterfall             | chart        | waterfall | Starting → +Created → +Pulled In → -Pushed Out → -Won → -Lost → Ending | Pipeline_Opportunity_Operations dataset    | scoped: date         |
| 7   | Pipeline by Forecast Category | chart        | stackhbar | Pipeline $ by Omitted/Pipeline/BestCase/Commit/Closed                  | ForecastCategory, ARR                      | all                  |
| 8   | Pipeline by Region            | chart        | hbar      | Pipeline $ by Account_Unit_Group\_\_c                                  | Region, ARR                                | all                  |
| 9   | Deals Changed This Week       | comparetable | table     | Opps with stage/date/amount change in last 7 days                      | OpportunityFieldHistory                    | all                  |
| F1  | Period Selector               | dateselector | -         | FiscalQuarter / FiscalYear                                             | CloseDate                                  | broadcasts           |
| F2  | Region Filter                 | pillbox      | -         | Account_Unit_Group\_\_c                                                | Account_Unit_Group\_\_c                    | broadcasts           |

**Interactions**:

- Click waterfall segment → filters Deals Changed table to those movement categories
- Click region bar → filters forecast category chart and deals table to that region
- Click forecast category bar → filters region chart and deals table
- Period/Region selectors drive all scoped widgets; Win Rate is immune (global benchmark)
- Deals Changed table has record links to Salesforce (XMD linkTemplateEnabled)

**Isolation**:

- Win Rate (#5): `receiveFacetSource: {mode: "none"}`, `useGlobal: false`, `broadcastFacet: false`, `selectMode: "none"`
- All other KPIs: `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}`

**Navigation**: Link widgets to → Page 1.2 (Pipeline Health), Page 1.3 (Forecast Analysis), Page 1.6 (Commit Calculator). **Cross-dashboard**: Deals Changed table row → drill to Page 4.2 (Account 360) via record link on Account Name.

**KPI Variance**: Each KPI shows value + variance vs prior period (e.g., "+12% QoQ") via window function `[-1..-1]` in the step query.

---

### Page 1.2 — Pipeline Health & Velocity

**Story**: "Is our pipeline healthy enough to hit target, and where is it stuck?"

| #   | Widget                       | Type         | Viz       | Metric                                                    | Source                          | Target   |
| --- | ---------------------------- | ------------ | --------- | --------------------------------------------------------- | ------------------------------- | -------- |
| 1   | Open Pipeline ARR            | number       | KPI       | Total stages 1-6                                          | APTS_Opportunity_ARR\_\_c       |          |
| 2   | Avg Deal Size (Won)          | number       | KPI       | Avg ARR of Closed Won                                     | APTS_Opportunity_ARR\_\_c       | >€100K   |
| 3   | Avg Sales Cycle              | number       | KPI       | Avg days Created→Won                                      | CloseDate - CreatedDate         | <90 days |
| 4   | Stale Pipeline %             | number       | KPI       | % of open opps >120 days old                              | Pipeline_Analytics dataset      | <20%     |
| 5   | Pipeline Funnel              | chart        | funnel    | ARR by stage (1→2→3→4→5→6)                                | StageName, ARR                  |          |
| 6   | Stage Conversion Rates       | chart        | hbar      | % converting from each stage to next                      | Pipeline_Transitions dataset    | >70%     |
| 7   | Time in Stage                | chart        | hbar      | Avg days in each stage, Land vs Expand                    | Pipeline_Opportunity_Operations | Baseline |
| 8   | Pipeline Creation vs Closure | chart        | combo     | Created pipeline $ (bars) vs Closed Won $ (line) by month | CreatedDate, CloseDate          |          |
| 9   | Aging Distribution           | chart        | stackhbar | Open opps by age: 0-30d, 31-60d, 61-90d, 91-120d, 120d+   | Pipeline_Analytics (AgeBucket)  |          |
| 10  | Stale Deals Table            | comparetable | table     | Open opps >90 days same stage, sorted by ARR desc         | Pipeline_Opportunity_Operations |          |

**Interactions**:

- Click funnel stage → aging distribution + stale deals filter to that stage
- Click age bucket → stale deals table shows those opps
- Stale Pipeline % KPI is scoped to date only — does NOT react to stage clicks
- Record links on stale deals table → opens Salesforce record

**Isolation**:

- KPIs (#1-4): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}`
- Charts (#5-9): `receiveFacetSource: {mode: "all"}` — react to page filters AND each other
- Stale Deals Table (#10): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 1.1 (Command Center), Page 1.3 (Forecast Analysis)

---

### Page 1.3 — Forecast Analysis (MERGED: old 1.3 + 1.9)

**Story**: "How reliable is our forecast, how has it changed, and what deals make up the commit?"

| #   | Widget                      | Type         | Viz       | Metric                                                                      | Source                          | Target |
| --- | --------------------------- | ------------ | --------- | --------------------------------------------------------------------------- | ------------------------------- | ------ |
| 1   | Commit WoW Change           | number       | KPI       | Commit amount delta vs last week                                            | Weekly_Forecast_Summary dataset |        |
| 2   | Best Case WoW Change        | number       | KPI       | Best Case delta vs last week                                                | Weekly_Forecast_Summary dataset |        |
| 3   | Deals Pushed This Week      | number       | KPI       | Count of opps with CloseDate pushed                                         | Weekly_Forecast_Opps dataset    | 0      |
| 4   | Forecast Accuracy (Prior Q) | number       | KPI       | Actual vs Forecasted prior quarter                                          | Weekly_Forecast_Summary dataset | ±5%    |
| 5   | Forecast Change Timeline    | chart        | time      | Weekly Commit/Best Case/Pipeline amounts                                    | Weekly_Forecast_Summary dataset |        |
| 6   | YoY Velocity Comparison     | chart        | combo     | Current Q pace vs same Q last year (line=LY, bars=TY)                       | Historical closed won by week   |        |
| 7   | Category Migration          | chart        | stackhbar | Deals that moved between forecast categories this period                    | Weekly_Forecast_Opps dataset    |        |
| 8   | Push Count Distribution     | chart        | hbar      | Opps by push count (0, 1, 2, 3+), red highlight for 3+                      | Weekly_Forecast_Opps dataset    |        |
| 9   | Commit List                 | comparetable | table     | All opps in Commit/Best Case with ARR, stage, close date, owner, push count | Forecast_Revenue_Motions        |        |
| 10  | Pushed Deals Table          | comparetable | table     | Opps with 3+ pushes, sorted by ARR desc                                     | Weekly_Forecast_Opps dataset    |        |

**Interactions**:

- Click push count bar → Pushed Deals table filters to that bucket
- Click category migration bar → Commit List filters to deals in that movement
- YoY Velocity is a global benchmark — immune to page facets
- Record links on both tables

**Isolation**:

- KPIs (#1-3): scoped to date + region via `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}`
- Forecast Accuracy (#4): **global benchmark** — full isolation quad (reports prior quarter historical fact — must NOT react to filters)
- YoY Velocity (#6): **global benchmark** — full isolation quad
- Charts (#5, #7, #8): `receiveFacetSource: {mode: "all"}` — react to page filters and each other
- Tables (#9, #10): `receiveFacetSource: {mode: "all"}` — cascading drill targets

**Navigation**: Link widgets to → Page 1.1 (Command Center), Page 1.6 (Commit Calculator)

---

### Page 1.4 — Deal Composition & Sizing

**Story**: "What's our deal mix, and are we winning the right deals?"

| #   | Widget                     | Type         | Viz       | Metric                                                  | Source                               |
| --- | -------------------------- | ------------ | --------- | ------------------------------------------------------- | ------------------------------------ |
| 1   | Won Deals by Value Tier    | chart        | stackhbar | Closed Won count: <€50K, €50-100K, €100-500K, €500K+    | ARR bucketed                         |
| 2   | Avg Deal Size Trend        | chart        | time      | Monthly avg ARR of Closed Won                           | APTS_Opportunity_ARR\_\_c            |
| 3   | Land vs Expand Mix         | chart        | stackvbar | Won ARR split by Type (Land/Expand) by quarter          | Type, ARR                            |
| 4   | Product Family Breakdown   | chart        | hbar      | Won ARR by product family                               | OLI.APTS_Product_Family\_\_c         |
| 5   | Source Effectiveness       | chart        | hbar      | Win rate by LeadSource                                  | LeadSource, IsWon                    |
| 6   | New Opps Created by Region | chart        | stackvbar | New opp count by region by month                        | CreatedDate, Account_Unit_Group\_\_c |
| 7   | Deals Below Avg Size       | comparetable | table     | Won deals below trailing-12mo avg size, with Win Reason | ARR, Reason_Won_Lost\_\_c            |

**Interactions**:

- Click product family bar → filters source effectiveness and deals table
- Click value tier → filters deals table
- Record links on deals table

**Isolation**:

- All charts (#1-6): `receiveFacetSource: {mode: "all"}` — react to page filters and each other
- Deals table (#7): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 1.1, Page 1.5 (Performance Analysis)

**Removed from v1**: Synergy Deals Won (#5), Synergy Pipeline (#6) — no M&A tag field exists. Partner Pipeline % (#7) — vague source. **Phase 2**: Create `Synergy_Flag__c` field, back-populate, then add these widgets.

---

### Page 1.5 — Performance Analysis (MERGED: old 1.6 + 1.7)

**Story**: "Compare any metric across any dimension — who are our top performers and where are we strong?"

| #   | Widget             | Type         | Viz       | Metric                                                     | Source                                                | Notes                   |
| --- | ------------------ | ------------ | --------- | ---------------------------------------------------------- | ----------------------------------------------------- | ----------------------- |
| F1  | Metric Selector    | listselector | -         | 15-metric toggle                                           | staticflex                                            | `broadcastFacet: false` |
| F2  | Dimension Selector | listselector | -         | Owner, Region, Product Family, Industry, Lead Source, Type | staticflex                                            | `broadcastFacet: false` |
| 1   | Primary Breakdown  | chart        | hbar      | Selected metric grouped by selected dimension, ranked desc | Executive_Revenue_Forecast + Forecast_Revenue_Motions | Main visualization      |
| 2   | Trend View         | chart        | stackvbar | Selected metric by dimension, quarterly trend              | Executive_Revenue_Forecast + Forecast_Revenue_Motions | Top 10 only             |
| 3   | Cross-Tab Detail   | comparetable | table     | Selected metric × selected dimension full table            | Executive_Revenue_Forecast + Forecast_Revenue_Motions | Exportable              |

**Architecture** (solves the 15-metric binding problem):

`Executive_Revenue_Forecast` has grain **Owner × FiscalQuarter × ProductFamily** with ARR, QuotaAmount, CommitCallARR, BestCaseOpenARR, ConfidenceWeightedARR, WinScore. `Forecast_Revenue_Motions` has 57 dimensions including MotionType, RiskScore, SalesCycleDuration — covers deal-level metrics. Both datasets have component fields that support ratio computation at runtime.

At runtime:

1. Dimension selector → SAQL `group by {{cell(dim_selector.selection, [0], "value").asGrouping()}}`
2. Metric selector → binding selects the appropriate SAQL expression:
   - Additive metrics (Open_Pipeline, Closed_Won, etc.): `sum('{{...}}')`
   - Ratio metrics: `sum('Won_Count') / (sum('Won_Count') + sum('Lost_Count')) * 100` (metric selector maps to full expression via staticflex `formula` column)
3. **Dimension coverage**: Executive_Revenue_Forecast has OwnerName, UnitGroup, ProductFamily, SalesRegion, FYLabel, CloseQuarter. Forecast_Revenue_Motions adds Industry, LeadSource (via Opportunity-level grain). All 6 planned dimensions are served by existing datasets — no runtime joins needed.

For dimension = "Owner", this produces a leaderboard (the old Page 1.6 use case).
For dimension = "Region", this produces a breakdown (the old Page 1.7 use case).

**Interactions**:

- Both selectors use explicit SAQL bindings only (broadcastFacet: false)
- Click dimension bar → table filters to that segment
- Click rep name in table → record link or navigate to filtered Page 1.7 (Rep Command Center)
- Trend chart limited to top 10 values to prevent chart overflow with high-cardinality dimensions

**Isolation**:

- Metric Selector (F1): `broadcastFacet: false` — drives charts via explicit SAQL binding only
- Dimension Selector (F2): `broadcastFacet: false` — drives charts via explicit SAQL binding only
- Primary Breakdown (#1): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}` — scoped to global filters, driven by selectors via binding
- Trend View (#2): same as #1
- Cross-Tab Detail (#3): `receiveFacetSource: {mode: "all"}` — receives global filters + bar clicks from Primary Breakdown

**Navigation**: Link widgets to → Page 1.7 (Rep Command Center), Page 1.1

---

### Page 1.6 — Commit Calculator

**Story**: "What-if scenario modeling — what's my number if I include/exclude specific deals?"

| #   | Widget               | Type         | Viz   | Metric                             | Source                                                                | Notes                                      |
| --- | -------------------- | ------------ | ----- | ---------------------------------- | --------------------------------------------------------------------- | ------------------------------------------ |
| 1   | Current Commit       | number       | KPI   | Sum ARR in Commit category         | ForecastCategory, ARR                                                 | **Global benchmark** — immune to selection |
| 2   | Scenario Total       | number       | KPI   | Sum ARR of selected opportunities  | Binding from selection                                                | Updates real-time                          |
| 3   | Gap to Quota         | number       | KPI   | Quota - Scenario Total             | Calculated (chained binding)                                          | Shows remaining shortfall                  |
| 4   | Pipeline Coverage    | number       | KPI   | Remaining open pipeline / Gap      | Calculated                                                            | Shows ability to fill gap                  |
| 5   | Opportunity Selector | comparetable | table | Open pipeline opps with checkboxes | Name, ARR, Stage, CloseDate, Owner, ForecastCategory, **Id (hidden)** | `selectMode: "multi"`                      |
| F1  | Period Selector      | dateselector | -     | Fiscal Quarter                     | CloseDate                                                             | Scopes opp list                            |
| F2  | Owner Filter         | listselector | -     | Opportunity Owner                  | Owner.Name                                                            | Optional scope                             |

**Architecture**:

1. `step_opp_list`: aggregateflex on Opportunity, `selectMode: "multi"`, includes hidden Id column
2. `step_scenario_sum`: SAQL → `filter q by 'Id' in {{column(step_opp_list.selection, ["Id"]).asEquality()}}; group q by all; foreach q generate sum('ARR') as 'Scenario_Total'`
3. `step_gap`: SAQL → uses `{{cell(step_quota.result, [0], "Quota")}}` minus `{{cell(step_scenario_sum.result, [0], "Scenario_Total")}}`
4. Current Commit step: full isolation quad (immune to everything)

**Isolation**:

- Current Commit (#1): `receiveFacetSource: none`, `useGlobal: false`, `broadcastFacet: false`, `selectMode: "none"`
- Opportunity Selector (#5): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_owner"]}`
- Scenario/Gap/Coverage KPIs: receive from step_scenario_sum only via results binding

**UX Note**: When the user changes Period or Owner filters, the opportunity table re-queries and the multi-select state is lost — the scenario disappears without warning. Consider adding a text widget: "Changing filters will reset your scenario selections."

**Navigation**: Link widgets to → Page 1.1, Page 1.3 (Forecast Analysis)

---

### Page 1.7 — Sales Rep Command Center

**Story**: "My personal dashboard — what should I work on today?"

| #   | Widget                 | Type         | Viz         | Metric                                                                    | Source                        | Notes                                                        |
| --- | ---------------------- | ------------ | ----------- | ------------------------------------------------------------------------- | ----------------------------- | ------------------------------------------------------------ |
| 1   | My Quota Attainment    | chart        | bulletgraph | Personal closed won vs quota target — bar = actual, reference line = 100% | ForecastingQuota, ARR         | `columnMap: {plots: ["Closed_Won"], targetPlots: ["Quota"]}` |
| 2   | My Pipeline Coverage   | number       | KPI         | Personal open pipeline / gap to quota                                     | ARR, Quota                    | Target: 3×                                                   |
| 3   | My Pipeline by Stage   | chart        | funnel      | My open pipeline ARR by stage                                             | StageName, ARR                | columnMap: null                                              |
| 4   | My Stagnating Deals    | comparetable | table       | My open opps stuck >30d OR no activity >14d, sorted by priority score     | DaysInStage, LastActivityDate | Priority = ARR × days_stale                                  |
| 5   | My Upcoming Activities | comparetable | table       | My next 10 tasks/events, sorted by due date                               | Task/Event, ActivityDate      | SOQL step type                                               |
| 6   | My Deal Highlights     | comparetable | table       | My opps with recent changes (amount, stage, close date)                   | OpportunityFieldHistory       | Green/red change indicators                                  |

**Interactions**:

- All steps filtered by `Owner.Id = "{{App.User.Id}}"` — scoped to logged-in user
- Stagnating deals sorted by priority score (ARR × days_stale) so rep works highest-value stuck deals first
- Record links on all three tables
- Global filter bar still applies (user can scope to a specific quarter) — this is intentional

**Isolation**:

- All steps scoped to logged-in user via `Owner.Id = "{{App.User.Id}}"` — this is an implicit filter, NOT a global filter bar facet
- KPIs (#1-2): also receive period filter via `receiveFacetSource: {mode: "include", steps: ["step_period"]}`
- Tables (#4-6): `receiveFacetSource: {mode: "include", steps: ["step_period"]}` — user can scope to quarter but region filter is irrelevant (user sees only their own data)

**Removed from v1**: `flatgauge` → replaced with KPI number + conditional color (anti-pattern fix). Forecast Trend chart → removed (serves weekly cadence, not "what to do today"). Reduced from 7 to 6 widgets for cleaner focus on action.

**Navigation**: Link widgets to → Page 1.1, Page 1.5 (Performance Analysis)

---

## DASHBOARD 2: ARR & REVENUE MANAGEMENT

**Audience**: CRO, CFO, VP Finance, RevOps, Product Marketing
**Cadence**: Monthly/Quarterly business review, weekly pipeline planning
**Global Filters**: Period (dateselector), Region (pillbox)

### Page 2.1 — ARR Overview

**Story**: "What's our recurring revenue base, how is it growing, and where are the risks?"

| #   | Widget                  | Type         | Viz        | Metric                                                       | Source                             | Target     |
| --- | ----------------------- | ------------ | ---------- | ------------------------------------------------------------ | ---------------------------------- | ---------- |
| 1   | Total ARR               | number       | KPI        | Sum current ARR across active contracts                      | APTS_Opportunity_ARR\_\_c (Won)    |            |
| 2   | Net New ARR (QTD)       | number       | KPI        | New + Expansion - Churn                                      | Calculated                         |            |
| 3   | NRR                     | number       | KPI        | Net Revenue Retention %                                      | End ARR ÷ Start ARR for cohort     | >100%      |
| 4   | Churn Rate              | number       | KPI        | Churned ARR ÷ Starting ARR                                   | Lost opps, cancellations           | <5% annual |
| 5   | ARR Bridge              | chart        | waterfall  | Starting ARR → +ILF → +ALF → -Churn → -Downsell → Ending ARR | Revenue_Retention_Health dataset   |            |
| 6   | ARR by Product Family   | chart        | stackvbar  | Quarterly ARR stacked by product family                      | OLI.APTS_Product_Family\_\_c       |            |
| 7   | ARR by Geography        | chart        | choropleth | ARR by country on world map, colored by ARR intensity        | Opp_Geo_Map dataset (60 countries) |            |
| 8   | ARR Trend               | chart        | time       | Monthly ARR with YoY comparison                              | Historical won data                |            |
| 9   | Top ARR Growth Accounts | comparetable | table      | Accounts with largest ARR increase this period               | Account, ARR delta                 |            |

**Removed from v1**: Indexation component from ARR Bridge — no indexation field exists. **Phase 2**: Create `Indexation_Amount__c` on Contract, populate from finance, then add back.

**Interactions**:

- Click product family → filters geography map, ARR trend, and growth table
- Click country on choropleth → filters product family chart and growth table
- Choropleth requires geographic XMD metadata on Country field (`isGeographic: true`) and `columnMap: {locations: ["Country"], color: ["sum_ARR"], trellis: [], dimensionAxis: [], plots: []}`
- NRR and Churn Rate are **global benchmarks** — full isolation quad
- Record links on growth table

**Navigation**: Link widgets to → Page 2.2, 2.3, 2.4, 2.5, 2.6 (SaaS Transition)

---

### Page 2.2 — Revenue Streams

**Story**: "How do our revenue streams (ILF, ALF, PS) compare and trend?"

| #   | Widget                     | Type         | Viz       | Metric                                        | Source                                  |
| --- | -------------------------- | ------------ | --------- | --------------------------------------------- | --------------------------------------- | --- |
| 1   | Total ARR                  | number       | KPI       | Sum all revenue streams                       | Multiple                                |     |
| 2   | Revenue Mix Change         | number       | KPI       | Shift in stream proportions vs prior year     | Calculated                              |
| 3   | PS ARR                     | number       | KPI       | Professional Services recurring               | RH_PS_Annual_Recurring_Revenue_ARR\_\_c |
| 4   | Revenue Stream Trend       | chart        | stackarea | Quarterly revenue by stream: ILF, ALF, PS     | Type + PS field                         |
| 5   | Revenue Stream Growth Rate | chart        | hbar      | YoY growth rate per stream, ranked            | Calculated                              |
| 6   | Revenue Stream Detail      | comparetable | table     | Stream breakdown with ARR, growth, % of total | Multiple                                |

**Removed from v1**: SaaS ARR KPI + SaaS vs On-Prem Trend (duplicates Page 2.6). Cross-Sell to Acquired (no M&A tag). One-Off Revenue (vague tagging). Reduced from 8 to 6 widgets.

**Design note**: Total ARR (#1) is intentionally the same metric as Page 2.1 #1. Consider replacing with **ILF ARR** or **ALF ARR** to make the KPI strip symmetric across all three streams (ILF, ALF, PS) and differentiate from Page 2.1.

**Interactions**:

- Click Revenue Stream Trend area → Revenue Stream Detail table filters to that stream
- Click Growth Rate bar → Detail table filters to that stream
- Revenue Stream Detail table has record links → drill to underlying opportunities

**Isolation**:

- KPIs (#1-3): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}`
- Charts (#4-5): `receiveFacetSource: {mode: "all"}` — react to page filters and each other
- Detail table (#6): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 2.1, Page 2.6 (SaaS Transition)

---

### Page 2.3 — Renewals Management

**Story**: "What's our renewal health, and which renewals need intervention?"

| #   | Widget                    | Type         | Viz       | Metric                                                       | Source                                             | Target |
| --- | ------------------------- | ------------ | --------- | ------------------------------------------------------------ | -------------------------------------------------- | ------ |
| 1   | Renewal Rate              | number       | KPI       | Renewed ARR ÷ Renewal-due ARR                                | Type=Renewal                                       | 95%    |
| 2   | Upcoming Renewals (90d)   | number       | KPI       | ARR of renewals due in next 90 days                          | CloseDate, Type=Renewal                            |        |
| 3   | Renewal Coverage by Owner | chart        | hbar      | Quoted renewals ÷ Due renewals by account exec               | Quote + Renewal data                               | 90%    |
| 4   | Renewals by Quarter       | chart        | stackvbar | Renewal count by term length (3yr, 5yr, 7yr)                 | APTS_Subscription_Term\_\_c                        |        |
| 5   | Renewals Month-over-Year  | chart        | time      | Monthly renewal ARR, current vs prior year                   | CloseDate, ARR                                     |        |
| 6   | At-Risk Renewals          | comparetable | table     | Upcoming renewals with risk flags, owner, ARR, last activity | Risk_of_Potential_Termination\_\_c, CloseDate, ARR |        |

**Removed from v1**: Existing ARR Indexed (#6), Indexation Contribution (#7) — no indexation data exists. Replaced with Renewal Coverage by Owner (accountability) and moved Renewal Coverage KPI from number to chart.

**Interactions**:

- Click quarter bar → At-Risk table filters to that quarter
- Click owner bar → At-Risk table filters to that owner's renewals
- Record links on At-Risk table

**Isolation**:

- Renewal Rate (#1): scoped to period + region — shows filtered view (if VP filters to EMEA, see EMEA renewal rate)
- Upcoming Renewals (#2): scoped to region only — always shows next 90d regardless of period filter
- Charts (#3-5): `receiveFacetSource: {mode: "all"}`
- At-Risk table (#6): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 2.1, Page 2.4 (Churn)

---

### Page 2.4 — Churn & Cancellation

**Story**: "Where are we losing revenue and why?"

| #   | Widget                  | Type         | Viz       | Metric                                                              | Source                             | Target      |
| --- | ----------------------- | ------------ | --------- | ------------------------------------------------------------------- | ---------------------------------- | ----------- |
| 1   | Lost ARR (QTD)          | number       | KPI       | Total ARR lost this quarter                                         | Lost opps ARR                      | <5% annual  |
| 2   | Business At Risk        | number       | KPI       | ARR of at-risk flagged accounts                                     | Risk_of_Potential_Termination\_\_c | <10% of ARR |
| 3   | Lost ARR by Reason      | chart        | hbar      | ARR lost bucketed by Reason_Won_Lost\_\_c                           | Reason_Won_Lost\_\_c, ARR          |             |
| 4   | Churn by Product Family | chart        | hbar      | Lost ARR by product family                                          | OLI, Lost opps                     |             |
| 5   | Churn Trend             | chart        | time      | Monthly churn rate over time                                        | Lost date, ARR                     |             |
| 6   | Business At Risk Trend  | chart        | stackarea | Quarterly at-risk ARR by risk level                                 | Risk_of_Potential_Termination\_\_c |             |
| 7   | At-Risk Account Table   | comparetable | table     | At-risk accounts with ARR, risk level, reason, last activity, owner | Multiple account fields            |             |

**Removed from v1**: Save Rate (#3) — no cancellation save tracking process exists. **Phase 2**: Build save-offer tracking, then add. **Added**: Churn by Product Family — moves from symptom to diagnosis.

**Interactions**:

- Click reason bar → At-Risk table filters to that reason
- Click product family bar → At-Risk table filters
- Record links on At-Risk table → opens Account in Salesforce

**Isolation**:

- Lost ARR (#1): scoped to date + region
- Business At Risk (#2): scoped to region only — always shows current risk regardless of period
- Charts (#3-6): `receiveFacetSource: {mode: "all"}`
- At-Risk table (#7): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 2.1, Page 4.1 (Account Health)

---

### Page 2.5 — Product Penetration Matrix (was Dashboard 5, Page 5.1)

**Story**: "Where are the gaps in our install base?"

| #   | Widget                       | Type         | Viz     | Metric                                                                        | Source                                    |
| --- | ---------------------------- | ------------ | ------- | ----------------------------------------------------------------------------- | ----------------------------------------- |
| 1   | Whitespace Opportunities     | number       | KPI     | Count of accounts with <3 product families                                    | Product count per account                 |
| 2   | Cross-Sell Pipeline          | number       | KPI     | Pipeline ARR for Expand type                                                  | Type=Expand, ARR                          |
| 3   | Product Attach Rate          | chart        | hbar    | % of customers with each product family                                       | OLI.Product_Family, Account.Type=Customer |
| 4   | Account × Product Matrix     | chart        | heatmap | Top 25 accounts × Product families, cell = ARR                                | Account, Product, ARR                     |
| 5   | Expansion Velocity           | chart        | time    | Time from land to first expansion by cohort                                   | Type=Land → first Expand                  |
| 6   | Top Whitespace Opportunities | comparetable | table   | Accounts ranked by whitespace ARR potential (high current ARR + few products) | Calculated                                |

**Changed from v1**: Added "Top Whitespace Opportunities" ranked table — turns "here are gaps" into "here is where to hunt." Removed "SimCorp One Deals" (unverified custom flag). Capped heatmap to **top 25 accounts by current ARR** (highest-value accounts have most expansion potential) via `limit` + `order by sum(ARR) desc`.

**Interactions**:

- Click heatmap cell → Whitespace table filters to that Account × Product Family intersection
- Click Product Attach Rate bar → heatmap and table filter to that product family
- Expansion Velocity chart does NOT broadcast (trend chart, not a filter source)
- Record links on Whitespace table → drill to Page 4.2 (Account 360)

**Isolation**:

- KPIs (#1-2): `receiveFacetSource: {mode: "include", steps: ["step_region"]}`
- Charts (#3-5): `receiveFacetSource: {mode: "all"}`
- Whitespace table (#6): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 4.2 (Account 360), Page 2.1

---

### Page 2.6 — SaaS Transition (was Dashboard 5, Page 5.2)

**Story**: "How is the SaaS migration progressing?"

| #   | Widget                                  | Type         | Viz   | Metric                                                | Source                         | Target   |
| --- | --------------------------------------- | ------------ | ----- | ----------------------------------------------------- | ------------------------------ | -------- |
| 1   | SaaS ARR                                | number       | KPI   | Total SaaS ARR                                        | ASP\_\_c=Yes, ARR              | >20% YoY |
| 2   | SaaS Customer Count                     | number       | KPI   | Accounts with ASP\_\_c=Yes                            | ASP\_\_c                       |          |
| 3   | SBL Conversion Pipeline                 | number       | KPI   | Open SBL Conversion opps                              | APTS_Opportunity_Sub_Type\_\_c |          |
| 4   | SaaS vs On-Prem Ratio                   | chart        | combo | Quarterly SaaS ARR % of total                         | ASP\_\_c, ARR                  |          |
| 5   | SaaS Migration Status                   | chart        | hbar  | Count/ARR by status: Potential, In Progress, Migrated | ASP\_\_c values                |          |
| 6   | SaaS by Region                          | chart        | hbar  | SaaS adoption rate by region                          | Region, ASP\_\_c               |          |
| 7   | Contract Type Distribution              | chart        | hbar  | MBL vs SBL vs PPL count                               | APTS_Contract_Type\_\_c        |          |
| 8   | On-Prem Accounts Eligible for Migration | comparetable | table | On-prem accounts ranked by ARR for SaaS conversion    | ASP\_\_c=No, ARR               |          |

**Changed from v1**: SaaS Migration Funnel → `hbar` (not a true conversion — status distribution where goal is 100%). Contract Type Mix → `hbar` (pie anti-pattern). Added action table: On-Prem Accounts Eligible for Migration.

**Interactions**:

- Click SaaS Migration Status bar → On-Prem table filters to that status
- Click Region bar → filters Migration Status chart and table
- Contract Type Distribution does NOT filter other charts (`broadcastFacet: false` — tangential to SaaS story)
- Record links on On-Prem table

**Isolation**:

- KPIs (#1-3): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}`
- Charts (#4-7): `receiveFacetSource: {mode: "all"}` except Contract Type (#7) which has `broadcastFacet: false`
- On-Prem table (#8): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 2.1, Page 2.2

---

## DASHBOARD 3: BDR OPERATING

**Audience**: BDR Leaders, SDRs/BDRs, Marketing Ops
**Cadence**: Daily standup, weekly review
**Global Filters**: Period (dateselector), BDR Team/Owner (listselector)

### Page 3.1 — Lead Funnel & Campaign Performance (MERGED: old 3.1 + 3.4)

**Story**: "Are we responding fast enough, converting effectively, and which campaigns drive quality pipeline?"

| #   | Widget                    | Type         | Viz    | Metric                                                | Source                    | Target              |
| --- | ------------------------- | ------------ | ------ | ----------------------------------------------------- | ------------------------- | ------------------- |
| 1   | Leads Created (Period)    | number       | KPI    | Count of new leads                                    | Lead.CreatedDate          |                     |
| 2   | Response Time (Median)    | number       | KPI    | Median minutes lead created to first BDR touch        | Lead_Funnel dataset       | <5 min (Contact Me) |
| 3   | Lead→Opp Time             | number       | KPI    | Median days from lead to opp creation                 | Lead_Funnel dataset       | Baseline            |
| 4   | Campaign-Sourced Pipeline | number       | KPI    | Pipeline $ from campaign-sourced opps                 | Lead_Funnel dataset       |                     |
| 5   | Lead Funnel               | chart        | funnel | Created → Qualified → Converted → Meeting → Opp → Won | Lead_Funnel dataset       |                     |
| 6   | Lead Source Attribution   | chart        | hbar   | Lead count by source, colored by conversion rate      | LeadSource                |                     |
| 7   | Campaign ROI              | chart        | hbar   | Pipeline $ per campaign, sorted by ROI                | Lead_Funnel dataset       |                     |
| 8   | Campaign Detail Table     | comparetable | table  | Campaign Name, Leads, Pipeline Created, Opps Won, ROI | Lead_Funnel dataset       |                     |
| 9   | Contact Me Tasks          | comparetable | table  | Open Contact Me tasks with age, assignee, account     | Task with Contact Me type |                     |

**Merged from v1**: Absorbed Campaign Performance (old 3.4) widgets — Campaign ROI, Campaign-Sourced Pipeline. Removed Source Mix pie chart (duplicated Lead Source Attribution). Added Campaign Detail Table (was missing action table on old 3.4). Removed Contact Me Response SLA gauge → response time is already in KPI #2.

**Interactions**:

- Click funnel stage → **Contact Me Tasks table** filters to leads in that stage (NOT Campaign Detail — campaigns span multiple stages and cannot accept a stage filter)
- Click campaign bar → Contact Me Tasks table filters to that campaign's leads
- Click Campaign Detail table row → drills to campaign record in Salesforce
- Record links on both tables

**Isolation**:

- KPIs (#1-4): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_bdr"]}`
- Lead Funnel (#5): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_bdr"]}` — broadcasts stage selection
- Campaign charts (#6-7): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_bdr"]}` — do NOT receive funnel stage facet
- Campaign Detail (#8): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_bdr"]}` — receives campaign bar click
- Contact Me Tasks (#9): `receiveFacetSource: {mode: "all"}` — receives funnel stage AND campaign clicks

**Navigation**: Link widgets to → Page 3.2 (Activity), Page 3.3 (Scoring)

---

### Page 3.2 — Activity & Engagement

**Story**: "Are BDRs doing enough of the right activities?"

| #   | Widget                 | Type   | Viz       | Metric                                                                                      | Source                   | Target |
| --- | ---------------------- | ------ | --------- | ------------------------------------------------------------------------------------------- | ------------------------ | ------ |
| 1   | Activities Logged      | number | KPI       | Total tasks/events logged this period                                                       | Task + Event count       |        |
| 2   | Activity Logging Rate  | number | KPI       | % of contacts with logged activity                                                          | Contact.LastActivityDate | 80%    |
| 3   | Leads Qualified        | number | KPI       | Leads moved to qualified status                                                             | Lead.Status              |        |
| 4   | Leads Disqualified     | number | KPI       | Leads disqualified with %                                                                   | Lead.Status              |        |
| 5   | BDR Leaderboard        | chart  | hbar      | Activities per BDR (calls, emails, meetings)                                                | Task.OwnerId, Type       |        |
| 6   | Activity vs Conversion | chart  | combo     | BDRs on x-axis, activities as bars, win rate as line overlay                                | Calculated               |        |
| 7   | Contact Me Task Aging  | chart  | stackhbar | Open tasks by age: <1d, 1-3d, 3-7d, 7d+                                                     | Task.CreatedDate         |        |
| 8   | Activity Calendar      | chart  | calendar  | Daily activity volume heatmap — reveals gaps in weekend/holiday coverage and daily patterns | Task.ActivityDate, count |        |

**Changed from v1**: Activity→Conversion scatter → `combo` chart (too few data points for scatter with 10-30 BDRs). Removed Contacts Created Over Time and Contacts Linked to Opps (moved to Page 5.4 Data Quality). **Added**: Activity Calendar (calendar heatmap) for daily pattern visibility.

**Interactions**:

- Click BDR name on leaderboard → Task Aging table filters to that BDR
- BDR Leaderboard broadcasts, combo chart does NOT broadcast (broadcastFacet: false on scatter/combo)

**Isolation**:

- KPIs (#1-4): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_bdr"]}`
- BDR Leaderboard (#5): `receiveFacetSource: {mode: "include", steps: ["step_period"]}` — NOT scoped by BDR filter (shows all BDRs for comparison)
- Activity vs Conversion (#6): `receiveFacetSource: {mode: "include", steps: ["step_period"]}`, `broadcastFacet: false`
- Task Aging (#7): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 3.1, Page 3.3

---

### Page 3.3 — Lead Scoring & Qualification

**Story**: "Is our scoring working, and where do leads stall in the funnel?"

| #   | Widget                        | Type         | Viz       | Metric                                                                      | Source                             | Target                                          |
| --- | ----------------------------- | ------------ | --------- | --------------------------------------------------------------------------- | ---------------------------------- | ----------------------------------------------- |
| 1   | MQL→SQL Conversion            | number       | KPI       | % of MQLs that become SQLs                                                  | Lead.Status transitions            | Baseline                                        |
| 2   | SQL→SAL Conversion            | number       | KPI       | % of SQLs accepted by sales                                                 | Opportunity_Stage_Status\_\_c      | Baseline                                        |
| 3   | Score Validation              | number       | KPI       | High-score (>80) conversion rate vs low-score (<40) delta                   | engagio**qualification_score**c    |                                                 |
| F1  | Lead Type Toggle              | pillbox      | -         | All Leads / Hot Leads (Contact Me) only                                     | staticflex                         | `broadcastFacet: false` — explicit binding only |
| 4   | Qualification Funnel          | chart        | funnel    | Lead → MQL → SQL → SAL → Meeting → Opp → Won (toggleable: all vs hot)       | Lead_Funnel dataset                |                                                 |
| 5   | Demandbase Score Distribution | chart        | hbar      | Accounts by pipeline_predict_score buckets                                  | engagio**pipeline_predict_score**c |                                                 |
| 6   | Score vs Win Rate             | chart        | hbar      | Win rate per qualification score bucket (0-20, 20-40, 40-60, 60-80, 80-100) | engagio**qualification_score**c    |                                                 |
| 7   | Lead Aging by Status          | chart        | stackhbar | Open leads bucketed by age and current status                               | Lead.CreatedDate, Status           |                                                 |
| 8   | Leads Needing Action          | comparetable | table     | Open leads >14 days in current status, sorted by score desc                 | Lead_Funnel dataset                |                                                 |

**Changed from v1**: Merged two funnels into one with Lead Type Toggle filter. Replaced Qualification Score scatter → `hbar` (bucketed score ranges — clearer answer to "do higher scores convert more?"). Added Score Validation KPI (headline answer). Added Leads Needing Action table (was missing action table). Removed Disqualification Reasons (diagnostic, moved to secondary position or drill-down). Reduced from 10 to 8 widgets + 1 filter.

**Interactions**:

- Lead Type Toggle switches funnel between all leads and Contact Me only — uses explicit SAQL binding (`broadcastFacet: false` prevents unintended faceting of score/aging charts)
- Click score bucket → Leads Needing Action table filters to that score range
- Two funnels NO LONGER compete for faceting (merged into one)
- Record links on Leads Needing Action table

**Isolation**:

- KPIs (#1-3): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_bdr"]}`
- Lead Type Toggle (F1): `broadcastFacet: false` — drives funnel via explicit binding only; does NOT facet score distribution or aging charts
- Qualification Funnel (#4): receives Toggle via binding + period/BDR filters
- Score charts (#5-6): `receiveFacetSource: {mode: "include", steps: ["step_period"]}` — immune to Toggle and BDR filter (org-wide scoring validation)
- Lead Aging (#7): `receiveFacetSource: {mode: "all"}`
- Leads Needing Action (#8): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 3.1, Page 3.2

---

## DASHBOARD 4: ACCOUNT INTELLIGENCE

**Audience**: Account Executives, Customer Success, Sales Managers
**Cadence**: Ad-hoc, pre-meeting prep, QBR prep
**Global Filters**: Region (pillbox), Account Segment (listselector)

### Page 4.1 — Account Health Overview

**Story**: "Which accounts need attention and which are thriving?"

| #   | Widget                  | Type         | Viz     | Metric                                                                                             | Source                                           | Target |
| --- | ----------------------- | ------------ | ------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------ | ------ |
| 1   | Total Customers         | number       | KPI     | Active customer count                                                                              | Account.Type=Customer                            |        |
| 2   | Avg Health Score        | number       | KPI     | Mean adoption score across customers                                                               | Overall_Adoption_Score\_\_c                      |        |
| 3   | Health Score Coverage   | number       | KPI     | % of customers with health score                                                                   | Adoption_Score\_\_c                              | 90%    |
| 4   | At-Risk Accounts        | number       | KPI     | Count with termination risk flag                                                                   | Risk_of_Potential_Termination\_\_c               |        |
| 5   | QBR Completion          | number       | KPI     | % of required QBRs completed on time                                                               | Last_QBR\_\_c                                    | 95%    |
| 6   | Customer Health Heatmap | chart        | heatmap | Region × Segment, colored by avg health score                                                      | Region, Segment, Health                          |        |
| 7   | Account Risk Landscape  | chart        | bubble  | X=Health Score, Y=ARR, Size=Product Count, Color=Risk Level — shows where big at-risk accounts sit | Overall_Adoption_Score, ARR, Product_Count, Risk |        |
| 8   | Health Score Trend      | chart        | time    | Avg health score over time (are accounts getting healthier?)                                       | Adoption_Score\_\_c (historical since 2020)      |        |
| 9   | Termination Risk Table  | comparetable | table   | At-risk accounts with ARR, risk level, health score, last QBR, last activity                       | Multiple                                         |        |

**Added from v1**: Health Score Trend (#7) — historical data exists from 2020 in Adoption_Score\_\_c. Uses this to answer "are we improving?"

**Interactions**:

- Click heatmap cell → Risk Table filters to that Region × Segment
- Click bubble → Risk Table filters to that account (bubble chart supports `selectMode: "single"`)
- Bubble chart uses `columnMap: null` (auto-detect for scatter/bubble types)
- Record links on Risk Table → opens Account in Salesforce

**Isolation**:

- KPIs (#1-5): `receiveFacetSource: {mode: "include", steps: ["step_region", "step_segment"]}`
- Health Heatmap (#6): `receiveFacetSource: {mode: "include", steps: ["step_region"]}` — broadcasts Region×Segment selection
- Charts (#7-8): `receiveFacetSource: {mode: "all"}`
- Risk Table (#9): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 4.2 (Account 360), Page 2.4 (Churn)

---

### Page 4.2 — Account 360 (Drill-Down)

**Story**: "Everything I need to know about this account before a meeting."

| #   | Widget              | Type         | Viz    | Metric                                                       | Source                            |
| --- | ------------------- | ------------ | ------ | ------------------------------------------------------------ | --------------------------------- |
| F1  | Account Selector    | listselector | filter | Select account (exposes AccountId for cross-dataset binding) | Account.Name                      |
| 1   | Account ARR         | number       | KPI    | Total ARR for selected account                               | Opp.ARR rolled up                 |
| 2   | Health Score        | number       | KPI    | Current adoption score with trend indicator                  | Overall_Adoption_Score\_\_c       |
| 3   | AuM                 | number       | KPI    | Assets under management                                      | AuM_m\_\_c                        |
| 4   | Products Owned      | chart        | hbar   | Products/modules owned                                       | OLI product families              |
| 5   | NPS Trend           | chart        | time   | NPS scores over time for this account                        | NPS_Survey\_\_c                   |
| 6   | Contract Timeline   | chart        | time   | Contract start/end/renewal dates                             | APTS_Contract_Start/End_Date\_\_c |
| 7   | Open Pipeline       | comparetable | table  | Open opps for this account                                   | Opportunity                       |
| 8   | Win/Loss History    | comparetable | table  | Closed opps for this account (won + lost) with reason        | Opportunity, Reason_Won_Lost\_\_c |
| 9   | Buying Group        | comparetable | table  | Contacts with roles, last activity                           | Contact, OCR                      |
| 10  | Engagement Timeline | chart        | time   | Activities (meetings, calls) over time                       | Task + Event by month             |

**Added from v1**: Health Score (#2), NPS Trend (#5), Win/Loss History (#8) — all data confirmed available (11K adoption records, 23K NPS records). Without these the "360" label was misleading.

**Cross-Dataset Bindings** (critical):

- Account Selector exposes `AccountId` field
- Task/Event step: `filter q by 'AccountId' in {{column(step_account.selection, ["AccountId"]).asEquality()}}`
- Contact step: same cross-dataset binding pattern
- NPS step: `filter q by 'Account__c' in {{column(step_account.selection, ["AccountId"]).asEquality()}}`

**Interactions**:

- Account Selector (F1) drives ALL widgets via cross-dataset bindings — selecting an account re-queries every widget
- Click Products Owned bar → Win/Loss History and Open Pipeline tables filter to that product family
- Click Engagement Timeline data point → no drill (informational trend)
- Record links on Open Pipeline, Win/Loss History, and Buying Group tables → open Salesforce records

**Isolation**:

- Account Selector (F1): `broadcastFacet: false` — drives all widgets via explicit cross-dataset bindings, NOT faceting
- KPIs (#1-3): receive from Account Selector binding only — global Region/Segment filters do NOT apply (user has already selected a specific account)
- All charts and tables (#4-10): receive from Account Selector binding only — `receiveFacetSource: {mode: "none"}`, `useGlobal: false` — this page is fully account-scoped, global filters are irrelevant
- Products Owned (#4): `broadcastFacet: true` — broadcasts product family selection to tables

**Navigation**: Link widgets to → Page 4.1, Page 2.5 (Product Penetration filtered to this account)

---

## DASHBOARD 5: OPERATIONS

**Audience**: Deal Desk, Legal, Finance, Sales Ops, RevOps
**Cadence**: Daily operations, weekly review
**Global Filters**: Period (dateselector), Region (pillbox)

### Page 5.1 — Commercial Approvals (was Dashboard 1, Page 1.5)

**Story**: "Is our deal governance working, or is it creating drag?"

| #   | Widget                             | Type         | Viz   | Metric                                               | Source                                  | Target   |
| --- | ---------------------------------- | ------------ | ----- | ---------------------------------------------------- | --------------------------------------- | -------- |
| 1   | Stage 3 Approvals This Month       | number       | KPI   | Count of opps passing Stage 3                        | Stage_20_Approval\_\_c                  |          |
| 2   | ACV of Stage 3 Approvals           | number       | KPI   | Sum ACV of approved deals                            | APTS_Forecast_ACV_AVG\_\_c              |          |
| 3   | Commercial Approval to Close       | number       | KPI   | Avg days from Stage 3 approval to Close              | Stage_20_Approval_Date\_\_c → CloseDate | <30 days |
| 4   | Approval Compliance                | number       | KPI   | % of eligible opps with commercial approval          | Stage_20_Approval\_\_c                  | 100%     |
| 5   | Stage 3 Approvals by Month         | chart        | vbar  | Monthly count of Stage 3 approvals                   | Stage_20_Approval_Date\_\_c             |          |
| 6   | Approval → Close Time Distribution | chart        | hbar  | Bucket: <7d, 7-14d, 15-30d, 30-60d, 60d+             | Calculated                              |          |
| 7   | Approval Effectiveness             | chart        | hbar  | Win Rate of approved vs non-approved deals           | Calculated                              |          |
| 8   | Bottleneck Analysis                | comparetable | table | Opps in Stage 3 or 4 >30 days, with owner and reason | Stage, duration                         |          |

**Added**: Approval Effectiveness (#7) — shows whether governance adds value. Record links on Bottleneck table.

**Data Risk**: `Stage_20_Approval__c` and `Stage_20_Approval_Date__c` fields are NOT confirmed in CRM Architecture. **Action**: Run `SELECT count(Id) FROM Opportunity WHERE Stage_20_Approval__c != null` before building. If fields don't exist, this entire page is deferred.

**Interactions**:

- Click Approvals by Month bar → Bottleneck table filters to that month
- Click Approval→Close Time bucket → Bottleneck table filters to opps in that duration range
- Approval Effectiveness chart does NOT broadcast (`broadcastFacet: false` — informational only)
- Record links on Bottleneck table

**Isolation**:

- KPIs (#1-4): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}`
- Charts (#5-7): `receiveFacetSource: {mode: "all"}`
- Bottleneck table (#8): `receiveFacetSource: {mode: "all"}` — cascading drill target

**Navigation**: Link widgets to → Page 5.2 (Quoting), Page 5.3 (Contracts)

---

### Page 5.2 — Quoting Performance (was Dashboard 6, Page 6.1)

**Story**: "How efficient is our quoting process and what's stuck?"

| #   | Widget                            | Type         | Viz   | Metric                                                             | Source                            | Target    |
| --- | --------------------------------- | ------------ | ----- | ------------------------------------------------------------------ | --------------------------------- | --------- |
| 1   | Quotes Created (Period)           | number       | KPI   | Count of quotes                                                    | Apttus_Proposal**Proposal**c      |           |
| 2   | First Quote→Close Time            | number       | KPI   | Avg days from first quote to close                                 | Quote.CreatedDate → Opp.CloseDate | Baseline  |
| 3   | First-Quote Win Rate              | number       | KPI   | Won on first quote ÷ total quoted                                  | Quote sequence                    | >30%      |
| 4   | Approval Cycle Time               | number       | KPI   | Avg hours for all approvals                                        | Approval timestamps               | <48 hours |
| 5   | Quote Complexity Distribution     | chart        | hbar  | Quotes by # of line items/products                                 | OLI count per quote               |           |
| 6   | Value Variance                    | chart        | hbar  | (First_Quote_Amount - Final_Opp_Amount) / First_Quote_Amount × 100 | Quote vs Opp Amount at close      | <5%       |
| 7   | Quotes Pending Approval           | comparetable | table | Quotes awaiting approval >48 hours with owner, age, amount         | Apttus approval data              |           |
| 8   | Quotes Awaiting Customer Response | comparetable | table | Quotes sent to customer >7d with no response                       | Quote status, age                 |           |

**Removed from v1**: Quote Accuracy (#2) — no error tracking field. Guided Selling Usage (#8) — no tracking flag. **Added**: Quotes Pending Approval and Quotes Awaiting Customer Response action tables (Deal Desk's #1 daily need).

**Data Risk**: Apttus managed package objects may not be available as replicated datasets. **Action**: Verify `Apttus_Proposal__Proposal__c` can be added as connected object. If not, build a dataflow.

**Interactions**:

- Click Quote Complexity bar → both action tables filter to quotes with that line item count
- Click Value Variance bar → Quotes Pending table filters to that variance bucket
- Record links on both tables

**Isolation**:

- KPIs (#1-4): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}`
- Charts (#5-6): `receiveFacetSource: {mode: "all"}`
- Tables (#7-8): `receiveFacetSource: {mode: "all"}` — cascading drill targets

**Navigation**: Link widgets to → Page 5.1, Page 5.3

---

### Page 5.3 — Contract Lifecycle (was Dashboard 6, Page 6.2)

**Story**: "Where are contracts stuck and what's the KYC status?"

| #   | Widget                       | Type         | Viz   | Metric                                                          | Source                        | Target |
| --- | ---------------------------- | ------------ | ----- | --------------------------------------------------------------- | ----------------------------- | ------ |
| 1   | Contract Cycle Time          | number       | KPI   | Avg days from request to signature                              | Contract dates                | Track  |
| 2   | Fulfillment Tracking         | number       | KPI   | % of signed contracts fully fulfilled                           | Fulfillment status            | 100%   |
| 3   | KYC Outstanding              | number       | KPI   | Opps with unresolved KYC                                        | KYC_Approval_Status\_\_c      | 0      |
| 4   | KYC Duration                 | chart        | hbar  | Avg KYC completion time bucketed                                | KYC dates                     |        |
| 5   | Contract Status Distribution | chart        | hbar  | Count per status: Requested, Drafted, Review, Signed, Fulfilled | Contract.Status               |        |
| 6   | Contracts in Review >14 Days | comparetable | table | Stuck contracts with owner, age, amount                         | Contract status + dates       |        |
| 7   | KYC Upcoming                 | comparetable | table | Accounts with KYC expiring in next 90d                          | KYC_Approval_Expiry_Date\_\_c |        |

**Removed from v1**: NDA Turnaround — no NDA tracking data. Redline Tracking — no redline flag. Contract Status Pipeline funnel → `hbar` (status queue, not conversion — funnel implies progressive dropout which is wrong for contracts). **Added**: Contracts in Review >14 Days action table.

**Data Risk**: Fulfillment Tracking (#2) assumes a fulfillment status field exists on Contract. **Action**: Verify `Contract.Fulfillment_Status__c` or equivalent exists before building. If not, replace with Contract Signature Rate (% signed within SLA).

**Interactions**:

- Click Contract Status bar → both tables filter to contracts in that status
- Click KYC Duration bucket → KYC Upcoming table filters to that duration range
- Record links on both tables

**Isolation**:

- KPIs (#1-3): `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}`
- Charts (#4-5): `receiveFacetSource: {mode: "all"}`
- Tables (#6-7): `receiveFacetSource: {mode: "all"}` — cascading drill targets

**Navigation**: Link widgets to → Page 5.1, Page 5.2

---

### Page 5.4 — Data Quality & Governance (was Dashboard 4, Page 4.3)

**Story**: "How clean is our CRM data, and whose data needs fixing?"

| #   | Widget                       | Type         | Viz   | Metric                                                                     | Source                         | Target   |
| --- | ---------------------------- | ------------ | ----- | -------------------------------------------------------------------------- | ------------------------------ | -------- |
| 1   | DUNS Population              | number       | KPI   | % of customers with DUNS                                                   | DUNS_No\_\_c                   | 95%      |
| 2   | Unit Group Population        | number       | KPI   | % of accounts with Unit Group                                              | Account_Unit_Group\_\_c on Opp | 100%     |
| 3   | Win/Loss Commentary          | number       | KPI   | % of closed opps with reason                                               | Reason_Won_Lost\_\_c           | Baseline |
| 4   | Past Due Opps                | number       | KPI   | Opps with CloseDate < TODAY                                                | CloseDate                      | 0        |
| 5   | Data Completeness by Field   | chart        | hbar  | Population % for key fields, ranked worst first                            | Multiple                       |          |
| 6   | Data Quality by Owner        | chart        | hbar  | Completeness score per sales rep (% of their records with required fields) | Multiple                       |          |
| 7   | Accounts Missing DUNS        | comparetable | table | Customer accounts without DUNS, with owner                                 | DUNS_No\_\_c, Owner            |          |
| 8   | Opps Missing Win/Loss Reason | comparetable | table | Closed opps without Reason_Won_Lost\_\_c, with owner                       | Reason_Won_Lost\_\_c           |          |

**Changed from v1**: Relocated from Dashboard 4 (Account Intelligence) — Data Quality is a RevOps concern, not an AE concern. Added owner-level breakdowns (#6) and action tables (#7, #8) — aggregate KPIs without accountability are useless.

**Interactions**:

- Click Data Completeness by Field bar → action tables filter to records missing that specific field
- Click Data Quality by Owner bar → action tables filter to that owner's incomplete records
- Record links on both tables

**Isolation**: All KPIs (#1-4) are **global benchmarks** — full isolation quad. Data quality must show the full picture regardless of filter state. Charts (#5-6) and tables (#7-8) also use full isolation quad — data quality is org-wide, never filtered by period or region.

**Navigation**: Link widgets to → Page 5.1, Page 5.2

---

## CROSS-DASHBOARD DESIGN RULES

### Global Filter Bar (on every dashboard)

- Period: FiscalYear / FiscalQuarter (dateselector) — on every page
- Region: Account_Unit_Group\_\_c (pillbox: SC EMEA | SC North America | SC Asia) — on every page
- Both filters appear at the top of every page — not just the first page
- All dashboards support drill: Unit Group → Region → Account Unit

### KPI Strip Pattern (every page)

- 3-5 KPIs across the top in number widgets
- Each KPI shows: **value + variance vs prior period + conditional color**
- Variance source: window function `sum(sum('ARR')) over ([-1..-1] partition by all order by ('FiscalQuarter'))` to get prior period value
- Green: improvement or on target. Red: decline or off target.
- Global benchmarks (Win Rate, NRR, Churn) → full isolation quad: `receiveFacetSource: none`, `useGlobal: false`, `broadcastFacet: false`, `selectMode: "none"`
- Scoped KPIs (Pipeline, Won ARR) → `receiveFacetSource: {mode: "include", steps: ["step_period", "step_region"]}`

### Isolation Quad (for global benchmark KPIs)

Every step backing a "global benchmark" widget MUST have ALL FOUR:

```json
{
  "broadcastFacet": false,
  "selectMode": "none",
  "receiveFacetSource": { "mode": "none", "steps": [] },
  "useGlobal": false
}
```

Missing ANY ONE allows facet or global filter leakage.

### Interaction Model

1. **Facet** — clicking a chart segment filters the page (`broadcastFacet: true`)
2. **Drill** — clicking a bar in Region chart → shows Units within that region
3. **Record Link** — every comparetable has XMD record links (`linkTemplateEnabled: true`)
4. **Toggle** — `staticflex` + SAQL binding to switch measures or dimensions. Always set `broadcastFacet: false` on toggle steps.
5. **Navigation** — `link` widgets on every page for inter-page movement

### Navigation Requirements

- Every page has link widgets to its sibling pages within the dashboard
- Cross-dashboard links for natural drill paths:
  - Pipeline → Account 360 (from deal table)
  - Churn → Account Health (from at-risk table)
  - Product Matrix → Account 360 (from heatmap)
  - Performance Analysis → Rep Command Center (from leaderboard)

### Data Backbone

- Primary: Replicated Datasets for all objects (auto-sync, no manual dataflows)
- 13 existing datasets + 2 new datasets to build (see DATA BACKBONE section above)
- XMD: Record links on Account.Name, Opportunity.Name; currency formatting on all ARR fields
- `enableAutomaticLinking: true` on all dashboards (auto-links by matching field names)

---

## DATA AVAILABLE (confirmed in org)

- **Quota**: ForecastingQuota.QuotaAmount — confirmed populated
- **Pipeline snapshots**: Opportunity_Snapshot\_\_c — 60,168 records. **DEAD DATA**: all from 2012-2015, Execution_Date\_\_c entirely NULL. Not usable for any current widget.
- **OpportunityFieldHistory**: 100,193 records (Sep 2024-today). Tracks ForecastCategoryName (4,669), CloseDate (7,070), StageName (6,630). **Replacement for dead snapshots** — powers Weekly_Forecast_Summary.
- **Adoption/Health Score**: Adoption_Score\_\_c — 11,083 records, 67 fields. Historical 2020-2025. Overall_Adoption_Score\_\_c on 221 Accounts
- **Customer Intelligence**: Customer_Intelligence\_\_c — 958 records, 86 fields
- **Termination Risk**: Risk_of_Potential_Termination\_\_c — 584 accounts (76 High, 60 Medium, 448 Low)
- **NPS**: NPS_Survey\_\_c — 23,588 records
- **EBIT/Profitability**: EBIT_Calculation\_\_c — 10,917 records
- **OpportunityHistory**: 882,386 records
- **25 CRM Analytics datasets**: All actively refreshed (modified within last 4 days). Powered by Python builder CSV upload pipeline. See DATA BACKBONE for full mapping.

## DATA NOT AVAILABLE (Phase 2 — requires new fields/processes)

| Missing Data             | Widgets Affected    | Action Required                                                                                                                             |
| ------------------------ | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| **Synergy/M&A tag**      | (deferred)          | Create `Synergy_Flag__c` picklist on Opportunity                                                                                            |
| **Indexation amounts**   | (deferred)          | Create `Indexation_Amount__c` on Contract, populate from finance                                                                            |
| **Save offer outcomes**  | (deferred)          | Build cancellation save tracking process                                                                                                    |
| **NDA tracking**         | (deferred)          | Integrate CLM tool or create NDA tracking object                                                                                            |
| **Redline tracking**     | (deferred)          | Same CLM integration                                                                                                                        |
| **Quote error tracking** | (deferred)          | Create validation count field on Apttus Quote                                                                                               |
| **Guided selling flag**  | (deferred)          | Check Apttus CPQ for existing flag                                                                                                          |
| **Opp_Geo_Map**          | Page 2.1 choropleth | Rebuild — upload job succeeded but dataset not persisted. Source from Account.BillingCountry or Product_Portfolio_Whitespace.BillingCountry |

## FIELDS TO VERIFY BEFORE BUILDING

Run these SOQL queries to resolve YELLOW data dependencies:

```sql
-- Page 5.1 (entire page depends on these)
SELECT count(Id) FROM Opportunity WHERE Stage_20_Approval__c != null
SELECT count(Id) FROM Opportunity WHERE Stage_20_Approval_Date__c != null

-- Page 4.2, 5.3 (KYC widgets)
SELECT count(Id) FROM Account WHERE KYC_Approval_Status__c != null
SELECT count(Id) FROM Account WHERE KYC_Approval_Expiry_Date__c != null

-- Page 2.4 (churn reasons)
SELECT count(Id) FROM Opportunity WHERE Reason_Won_Lost__c != null

-- Page 3.3 (Demandbase scoring)
SELECT count(Id) FROM Account WHERE engagio__pipeline_predict_score__c != null
SELECT count(Id) FROM Account WHERE engagio__qualification_score__c != null

-- Page 5.2 (Apttus replication)
SELECT count(Id) FROM Apttus_Proposal__Proposal__c
```

---

## TOTAL WIDGET COUNT (verified Round 4)

| Dashboard               | Pages  | KPIs   | Charts | Tables | Filters | Total    |
| ----------------------- | ------ | ------ | ------ | ------ | ------- | -------- |
| 1. Pipeline & Forecast  | 7      | 20     | 19     | 9      | 7       | **55**   |
| 2. ARR & Revenue        | 6      | 16     | 16     | 7      | 2       | **41**   |
| 3. BDR Operating        | 3      | 11     | 10     | 4      | 1       | **26**   |
| 4. Account Intelligence | 2      | 7      | 8      | 4      | 1       | **20**   |
| 5. Operations           | 4      | 15     | 8      | 8      | 0       | **31**   |
| **TOTAL**               | **22** | **69** | **61** | **32** | **11**  | **~173** |

### Viz Type Distribution (13 types used)

| Type                 | Count | Notes                                              |
| -------------------- | ----- | -------------------------------------------------- |
| KPI (number)         | 69    | Includes conditional color, variance               |
| hbar                 | 30    | Horizontal bar — workhorse                         |
| table (comparetable) | 32    | Action tables with record links                    |
| time                 | 10    | Time series trends                                 |
| stackhbar            | 5     | Stacked horizontal bar                             |
| stackvbar            | 5     | Stacked vertical bar                               |
| funnel               | 4     | Conversion processes only                          |
| combo                | 4     | Dual-axis bars + line                              |
| waterfall            | 2     | ARR bridge, pipeline movement                      |
| stackarea            | 2     | Revenue stream trends                              |
| heatmap              | 2     | Region×Segment, Account×Product                    |
| **choropleth**       | 1     | ARR by country (Page 2.1) — uses Opp_Geo_Map       |
| **bubble**           | 1     | Account risk landscape (Page 4.1) — 4 dimensions   |
| **bulletgraph**      | 1     | Rep quota attainment (Page 1.7) — actual vs target |
| **calendar**         | 1     | Activity patterns (Page 3.2) — daily heatmap       |

**Changes from v3**: Replaced 6/7 planned recipes with 13 existing datasets (Python builder pipeline, actively refreshed). Only Weekly_Forecast_Summary + Opp_Geo_Map need building. Updated all page Source columns to reference actual dataset names. Removed dead Opportunity_Snapshot\_\_c dependency. Added ML/analytics bonus datasets table. Resolved Snapshot data risk (CRITICAL → RESOLVED via OpportunityFieldHistory).

**Changes from v2**: Recipe count 5→7 (added ARR_Bridge, Pipeline_Health). Rep_Scorecard expanded to 16 columns with component storage. Fixed summary table (178→173 after recount). Added Isolation to 13 pages, Interactions to 6 pages. Fixed 3 interaction bugs. Added 4 new viz types: choropleth, bubble, bulletgraph, calendar.

**Changes from v1**: 25→22 pages (-3 merges). ~192→~172 widgets (-12 RED removed, -10 redundant cut, +8 action tables added). 6→5 dashboards (merged Whitespace into ARR).

---

## PHASE 0 BUILD STEPS (before dashboard construction)

| Step | Action                                                     | Status          | Notes                                                         |
| ---- | ---------------------------------------------------------- | --------------- | ------------------------------------------------------------- |
| 0a   | Build Weekly_Forecast_Summary from OpportunityFieldHistory | **IN PROGRESS** | Script: `scripts/build_weekly_forecast_summary.py`            |
| 0b   | Rebuild Opp_Geo_Map dataset                                | TODO            | Source from Account.BillingCountry + Opportunity ARR          |
| 0c   | Run SOQL verification queries (see above)                  | TODO            | Resolve YELLOW dependencies for Pages 5.1, 4.2, 5.3, 3.3, 5.2 |
| 0d   | Verify existing dataset field names match page specs       | TODO            | Spot-check 3-4 pages against actual dataset schemas           |

## PHASE 1-5 BUILD ORDER

| Phase | Dashboard                 | Pages | Complexity | Notes                                    |
| ----- | ------------------------- | ----- | ---------- | ---------------------------------------- |
| 1     | Sales Pipeline & Forecast | 7     | Highest    | Pilot — uses most datasets, most widgets |
| 2     | ARR & Revenue Management  | 6     | High       | Depends on Opp_Geo_Map for choropleth    |
| 3     | BDR Operating             | 3     | Medium     | Self-contained, BDR datasets are rich    |
| 4     | Account Intelligence      | 2     | Medium     | Customer_Account_Health covers most      |
| 5     | Operations                | 4     | Medium     | Some YELLOW dependencies to resolve      |

## PHASE 6+ ROADMAP (after initial build)

| Phase | Scope                                                           | Dependency                                                      |
| ----- | --------------------------------------------------------------- | --------------------------------------------------------------- |
| 6a    | Add Synergy/M&A widgets to Page 1.4                             | Create `Synergy_Flag__c`, back-populate                         |
| 6b    | Add Indexation to ARR Bridge (Page 2.1) and Renewals (Page 2.3) | Create `Indexation_Amount__c`, finance integration              |
| 6c    | Add Save Rate to Churn page (Page 2.4)                          | Build cancellation save tracking process                        |
| 6d    | Add NDA/Redline to Contracts (Page 5.3)                         | CLM integration                                                 |
| 6e    | Add Quote Accuracy/Guided Selling to Quoting (Page 5.2)         | Apttus field verification                                       |
| 6f    | Mobile layouts                                                  | Add gridLayout with numColumns: 2, selectors: ["maxWidth(599)"] |
| 6g    | Integrate ML bonus datasets into dashboards                     | Pipeline_Trendlines, Pipeline_Monte_Carlo, Survival curves      |
