# Revenue Intelligence — Complete Feature Reference

Compiled March 9, 2026. Purpose: Build all Revenue Intelligence features as custom CRM Analytics dashboards using SimCorp's data model.

---

## 1. REVENUE INTELLIGENCE COMPONENT MAP

Revenue Intelligence is NOT a single product — it's a bundle of 10 integrated components:

| #   | Component                               | Type                               | What It Does                                                                      |
| --- | --------------------------------------- | ---------------------------------- | --------------------------------------------------------------------------------- |
| 1   | **Pipeline Inspection**                 | Lightning Component                | Enhanced opportunity list view with inline editing, change highlights, push count |
| 2   | **Pipeline Flow (Sankey)**              | Lightning Component                | Visualizes deal movement between forecast categories over time                    |
| 3   | **Revenue Insights: Overview**          | CRM Analytics Dashboard            | On-track/off-track chart, quota attainment, opportunities needing attention       |
| 4   | **Revenue Insights: Team Performance**  | CRM Analytics Dashboard            | 15 toggleable metrics, top performer leaderboard                                  |
| 5   | **Revenue Insights: Sales Performance** | CRM Analytics Dashboard            | Cross-dimensional metric comparison (by lead source, type, etc.)                  |
| 6   | **Commit Calculator**                   | CRM Analytics Dashboard            | What-if scenario modeling for forecast calls                                      |
| 7   | **Forecast Insights Tab**               | CRM Analytics Dashboard            | YoY velocity analysis, forecast change tracking                                   |
| 8   | **Einstein Account Management**         | CRM Analytics + Einstein Discovery | Whitespace analysis, account potential scoring, cross-sell identification         |
| 9   | **Product Insights Dashboard**          | CRM Analytics Dashboard            | Product success rates, what works by industry/opportunity type                    |
| 10  | **Sales Rep Command Center**            | CRM Analytics Dashboard            | Personal pipeline coverage, what-if calculator, stagnating deals                  |

**We already have:** Pipeline Inspection (#1) — active with 1,568 licenses.
**We need to custom-build:** #2-#10 as CRM Analytics dashboards using our existing Tableau Einstein Included App licenses (2,552 seats).

---

## 2. REVENUE INSIGHTS: OVERVIEW PAGE

### Purpose

"Are we on track this quarter?" — the CRO/VP Sales morning check.

### Widgets

| Widget                              | Type            | Description                                                                                |
| ----------------------------------- | --------------- | ------------------------------------------------------------------------------------------ |
| **On Track / Off Track Chart**      | Gauge/indicator | Visual indicator of quota attainment progress — are we likely to hit the quarterly number? |
| **Quota Attainment**                | KPI number      | Current closed won vs. quota with variance                                                 |
| **Pipeline Coverage**               | KPI number      | Open pipeline / remaining gap to quota                                                     |
| **Commit Amount**                   | KPI number      | Total in Commit forecast category                                                          |
| **Best Case Amount**                | KPI number      | Total in Best Case forecast category                                                       |
| **"What Should I Focus On?"**       | Section header  | Three sub-widgets below:                                                                   |
| **Opportunities Needing Attention** | Table           | Deals with: no recent/upcoming activities, OR stuck in same stage too long                 |
| **Opportunity Changes**             | Table/list      | Recent changes to Amount, Close Date, Forecast Category + new/closed deals                 |
| **Top Open Opportunities**          | Table           | Deals by forecast status: Commit, Most Likely, Best Case, Pipeline                         |

### Data Sources

- `ForecastingItem` — forecast amounts by category
- `ForecastingQuota` — quota targets
- `Opportunity` — pipeline data, stage, amount, close date
- `Task`/`Event` — activity recency

### SimCorp Adaptation

- Replace `Amount` with `APTS_Opportunity_ARR__c` as primary measure
- 5 active forecast types: Opportunity ACV, ARR, Quota Retirement, Product Family ACV, Renewal ACV
- "Stuck" threshold: use median days-in-stage from our org's historical data

---

## 3. REVENUE INSIGHTS: TEAM PERFORMANCE PAGE

### Purpose

Manager drill-down: "How is each rep performing across key metrics?"

### Layout

- **Up to 4 metrics displayed simultaneously** as bar charts/KPI cards
- Each metric shows a **ranked leaderboard** of reps
- Click any metric to **swap it** from a dropdown of 15 available metrics
- Top performers highlighted

### The 15 Metrics (reconstructed from all sources)

| #   | Metric                    | Description                       | SimCorp Field                                            |
| --- | ------------------------- | --------------------------------- | -------------------------------------------------------- |
| 1   | **Open Pipeline**         | Total open pipeline value         | sum(APTS_Opportunity_ARR\_\_c) where IsClosed=false      |
| 2   | **Closed Won**            | Total won value this period       | sum(APTS_Opportunity_ARR\_\_c) where StageName='8 - Won' |
| 3   | **Quota**                 | Assigned quota amount             | ForecastingQuota.QuotaAmount                             |
| 4   | **Quota Attainment %**    | Closed Won / Quota \* 100         | Calculated                                               |
| 5   | **Gap to Quota**          | Quota - Closed Won                | Calculated                                               |
| 6   | **Commit**                | Total in Commit forecast category | ForecastingItem where ForecastCategoryName='Commit'      |
| 7   | **Best Case**             | Total in Best Case category       | ForecastingItem where ForecastCategoryName='Best Case'   |
| 8   | **Pipeline Coverage**     | Open Pipeline / Gap to Quota      | Calculated ratio                                         |
| 9   | **Average Deal Size**     | Avg opportunity value             | avg(APTS_Opportunity_ARR\_\_c)                           |
| 10  | **Average Days to Close** | Avg sales cycle length            | avg(days between CreatedDate and CloseDate) for won      |
| 11  | **Win Rate**              | Won / (Won + Lost) \* 100         | Calculated                                               |
| 12  | **Activities Completed**  | Count of completed tasks/events   | count(Task) where Status='Completed'                     |
| 13  | **Activities Open**       | Count of open/overdue activities  | count(Task) where Status!='Completed'                    |
| 14  | **New Pipeline Created**  | New opps created this period      | sum(ARR) where CreatedDate in period                     |
| 15  | **Closed Lost**           | Total lost value this period      | sum(ARR) where StageName like '%Lost%'                   |

### Implementation Pattern

- Use `staticflex` step for metric selector (15 values)
- Each metric = separate `aggregateflex` step with `receiveFacetSource: {mode: "include", steps: ["metric_selector"]}`
- Use binding to dynamically switch the displayed chart
- Leaderboard = horizontal bar chart grouped by Owner, ordered by selected metric desc

---

## 4. REVENUE INSIGHTS: SALES PERFORMANCE PAGE

### Purpose

"Compare metrics across dimensions" — progressive disclosure for deeper analysis.

### Layout

- Primary metric selection (same 15 as Team Performance)
- **Comparison dimension selector**: Lead Source, Opportunity Type, Product Family, Region, Industry
- Stacked/grouped bar chart showing metric broken down by selected dimension
- Table below with full cross-tab data

### SimCorp Adaptation

- Comparison dimensions: `Account_Unit_Group__c` (3 regions), `Region__c` (7), `APTS_RH_Product_Family__c`, `Industry__c`, `LeadSource`, `Type`
- Use `staticflex` for dimension selector + binding into SAQL `group by`

---

## 5. PIPELINE FLOW (SANKEY CHART)

### Purpose

"How has pipeline shifted since [date]?" — visualize deal movement between forecast categories.

### What It Shows

- Left side: forecast category distribution at start date
- Right side: current forecast category distribution
- Bands connecting them: deals that moved between categories
- Click a band to filter the opportunity list below

### Forecast Categories Tracked

| Category      | Description                     |
| ------------- | ------------------------------- |
| Closed Won    | Deals won in period             |
| Commit        | High confidence to close        |
| Most Likely   | Good probability                |
| Best Case     | Possible but not certain        |
| Open Pipeline | In early stages                 |
| Closed Lost   | Deals lost in period            |
| Moved Out     | Close date pushed beyond period |

### SimCorp Implementation

- Requires `Opportunity_Snapshot__c` data (confirmed: 60K records exist)
- Compare snapshot at period start vs. current `ForecastCategoryName`
- Visualization: CRM Analytics doesn't have native Sankey — use **stacked waterfall** or **alluvial-style grouped bars** as alternative
- Or build as an **LWC custom component** embedded in the dashboard

---

## 6. PIPELINE INSPECTION METRICS (for custom recreation)

### Pipeline Changes View (Waterfall Chart)

| Metric                 | Description                         | Color |
| ---------------------- | ----------------------------------- | ----- |
| **Opening Pipeline**   | Open pipeline at period start       | Blue  |
| **+ New**              | New opportunities created in period | Green |
| **+ Moved In**         | Close date pulled into period       | Green |
| **+ Increased**        | Amount increased on existing opps   | Green |
| **- Decreased**        | Amount decreased on existing opps   | Red   |
| **- Moved Out**        | Close date pushed out of period     | Red   |
| **- Won**              | Closed won (removed from open)      | Blue  |
| **- Lost**             | Closed lost (removed from open)     | Red   |
| **= Closing Pipeline** | Current open pipeline               | Blue  |

### Forecast Categories View

| Metric        | Description                |
| ------------- | -------------------------- |
| Total         | Sum across all categories  |
| Closed Won    | Already won                |
| Commit        | Sales rep commits to close |
| Most Likely   | High probability           |
| Best Case     | Stretch target             |
| Open Pipeline | Early stage                |
| Closed Lost   | Already lost               |

### Change Highlights (inline on opps)

- **Green arrow up**: Amount increased
- **Red arrow down**: Amount decreased
- **Red clock**: Next Step not updated in 7+ days
- **Push Count**: How many times close date pushed by a calendar month

### SimCorp Implementation

- Waterfall chart: use `waterfall` visualization type (with `columnMap: null`)
- Snapshot comparison: `Opportunity_Snapshot__c` vs current Opportunity
- Push Count: calculate from snapshot history (count months where CloseDate moved later)

---

## 7. COMMIT CALCULATOR

### Purpose

"What-if scenario modeling for forecast calls" — interactive, non-destructive.

### How It Works

1. Start with current Commit amount
2. **Add/remove individual opportunities** to model scenarios
3. See real-time impact on:
   - Total commit amount
   - Gap to quota
   - Pipeline coverage
4. Run multiple scenarios without affecting real data
5. Compare scenarios side-by-side

### SimCorp Implementation

- Interactive table with checkboxes (include/exclude opps)
- Running total KPI that updates as user selects/deselects
- Use `selectMode: "multi"` on opportunity list step
- Binding from selection → sum calculation step
- Store no state — purely ephemeral

---

## 8. FORECAST INSIGHTS TAB

### Purpose

"Drive the right conversations in forecast calls with clear metrics and historical comparison."

### Key Widgets

| Widget                          | Description                                            |
| ------------------------------- | ------------------------------------------------------ |
| **Forecast Amount by Category** | Stacked bar showing Commit, Best Case, Pipeline        |
| **Forecast Change Timeline**    | How forecast has trended week-over-week                |
| **YoY Velocity Analysis**       | Current quarter performance vs. same quarter last year |
| **Category Migration**          | Deals that moved between forecast categories           |
| **Close Date Push Analysis**    | Which deals have been pushed and how many times        |

### SimCorp Implementation

- YoY: compare current FY Q vs. prior FY Q using `Opportunity_Snapshot__c`
- Forecast change: weekly snapshots showing commit/best case progression
- Push analysis: group opps by Push Count range (0, 1, 2, 3+)

---

## 9. EINSTEIN ACCOUNT MANAGEMENT

### Purpose

"Identify whitespace and cross-sell/upsell opportunities at the account level."

### Components

#### Account Scoring

- Einstein Discovery model scores accounts on **potential**
- Shows factors driving the score (positive and negative)
- Benchmark vs. similar accounts

#### Whitespace View

- Matrix: Accounts (rows) × Products (columns)
- Cells show: purchased (filled) vs. not purchased (whitespace)
- Breakdowns by:
  - Product Name
  - Product Family
  - Industry
  - Region
- Highlights accounts with highest whitespace opportunity

#### Account Inspector

- Accessed from Account list view
- Score + explanation
- Recommended actions
- Comparison to peer accounts

### SimCorp Implementation

- Product families from `APTS_RH_Product_Family__c`: SimCorp Dimension, Coric, Axioma, Data Management, IBOR, Front Office, Back Office, etc.
- Whitespace = accounts that have some products but not others
- Cross-sell scoring: accounts similar to those that upgraded (use historical patterns)
- Map to our **Dashboard 4: Whitespace & Cross-Sell Action Center**

---

## 10. PRODUCT INSIGHTS DASHBOARD

### Purpose

"Which products are succeeding? What works by industry/opportunity type?"

### Key Widgets

| Widget                        | Description                                           |
| ----------------------------- | ----------------------------------------------------- |
| **Product Revenue Breakdown** | ARR by product family, trending over time             |
| **Win Rate by Product**       | Which products have highest/lowest win rates          |
| **Product × Industry Matrix** | Where does each product sell best                     |
| **Product Attach Rate**       | How often is this product part of multi-product deals |
| **Top Deals by Product**      | Largest recent wins per product family                |

### SimCorp Implementation

- Maps to our **Dashboard 3: Executive Product Mix & Industry**
- Source: OpportunityLineItem joined with Product2
- Product family from OLI (string field, groupable — unlike the Opportunity multipicklist)

---

## 11. SALES REP COMMAND CENTER

### Purpose

"Personal dashboard for frontline reps — what should I do today?"

### Key Widgets

| Widget                        | Description                                           |
| ----------------------------- | ----------------------------------------------------- |
| **My Quota Attainment**       | Personal closed won vs. quota with gauge              |
| **My Pipeline Coverage**      | Personal open pipeline / gap to quota                 |
| **Forecast Change Over Time** | How my forecast has trended this quarter              |
| **What-If Calculator**        | Personal version of Commit Calculator                 |
| **Stagnating Opportunities**  | My deals stuck in stage or with no activity           |
| **Upcoming Activities**       | Next actions due                                      |
| **Opportunity Highlights**    | Deals with recent changes (amount, stage, close date) |

### SimCorp Implementation

- Use `$User.Id` binding to scope to logged-in user
- Maps to embedded dashboard on the Home page or as dedicated "My Pipeline" page within Dashboard 1

---

## 12. PREREQUISITES WE ALREADY HAVE

| Prerequisite                 | Status        | Notes                                                    |
| ---------------------------- | ------------- | -------------------------------------------------------- |
| CRM Analytics Platform       | Active        | Tableau Einstein Included App (2,552 licenses)           |
| Pipeline Inspection          | Active        | 1,568 licenses, 1 assigned                               |
| Collaborative Forecasting    | Active        | 5 forecast types configured                              |
| ForecastingQuota data        | Confirmed     | Quota objects populated                                  |
| ForecastingItem data         | Confirmed     | Forecast items populated                                 |
| Opportunity_Snapshot\_\_c    | Confirmed     | 60K records                                              |
| Historical Trending          | Likely active | Required by Pipeline Inspection                          |
| Einstein Opportunity Scoring | TBD           | Check if enabled — may need Sales Cloud Einstein license |
| Einstein Activity Capture    | TBD           | Check if enabled — requires mail server connection       |
| Einstein Discovery           | TBD           | Check if available for account scoring models            |

---

## 13. MAPPING TO OUR 6 CUSTOM DASHBOARDS

| RI Component                        | Our Dashboard                          | Page                                   |
| ----------------------------------- | -------------------------------------- | -------------------------------------- |
| Revenue Insights: Overview          | Dashboard 1: Sales Pipeline & Forecast | Page 1.1                               |
| Revenue Insights: Team Performance  | Dashboard 1                            | Page 1.2 (NEW)                         |
| Revenue Insights: Sales Performance | Dashboard 1                            | Page 1.3 (NEW)                         |
| Pipeline Flow (Sankey)              | Dashboard 1                            | Page 1.1 (integrated)                  |
| Commit Calculator                   | Dashboard 1                            | Page 1.4 (NEW)                         |
| Forecast Insights                   | Dashboard 1                            | Page 1.5 (NEW)                         |
| Pipeline Inspection Metrics         | Dashboard 1                            | Page 1.1 (waterfall chart)             |
| Einstein Account Management         | Dashboard 4: Whitespace & Cross-Sell   | Pages 4.1-4.2                          |
| Product Insights                    | Dashboard 3: Executive Product Mix     | Pages 3.1-3.2                          |
| Sales Rep Command Center            | Dashboard 1                            | Page 1.6 (NEW) — or embedded Home page |

---

## SOURCES

- [Salesforce Ben: Revenue Intelligence In-Depth](https://www.salesforceben.com/complete-revenue-intelligence-for-salesforce-in-depth-overview/)
- [Salesforce Ben: Top 4 Features](https://www.salesforceben.com/salesforce-revenue-intelligence/)
- [Atrium: Getting Started](https://atrium.ai/resources/getting-started-with-salesforce-revenue-intelligence/)
- [Trailhead: Revenue Insights](https://trailhead.salesforce.com/content/learn/modules/introduction-to-revenue-intelligence-for-sales-cloud/check-performance-with-revenue-insights)
- [Trailhead: Pipeline Inspection](https://trailhead.salesforce.com/content/learn/modules/sell-smarter-with-pipeline-inspection/understand-pipeline-health-with-metrics-and-charts)
- [Salesforce Ben: Pipeline Inspection Guide](https://www.salesforceben.com/ultimate-guide-to-salesforce-pipeline-inspection/)
- [Sweet Potato Tec: Enabling RI](https://www.sweetpotatotec.com/enabling-revenue-intelligence-in-salesforce/)
- [Gettectonic: Revenue Intelligence](https://gettectonic.com/salesforce-revenue-intelligence/)
