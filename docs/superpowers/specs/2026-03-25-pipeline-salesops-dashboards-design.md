# Pipeline Reporting & Sales Ops Dashboards — Design Spec

**Date:** 2026-03-25
**Audience:** Sales Directors (Report 1, monthly), Sales Ops (Report 2, quarterly)
**Platform:** Salesforce CRM Analytics (Wave API PATCH)
**Pattern:** One Python builder per dashboard, same architecture as existing 18+ builders

---

## Deliverables

1. **Dashboard 1: Pipeline Reporting & Insights** — `build_pipeline_reporting.py`
2. **Dashboard 2: Sales Ops Data Quality & Forecast Accuracy** — `build_sales_ops_reporting.py`
3. **New Dataset: Retention Product Analysis** — `build_retention_product_analysis.py`

Existing dashboards for compliance (`build_sales_compliance.py`) and pipeline hygiene (`build_pipeline_opportunity_operations.py`) are reused — no duplication.

---

## Shared Infrastructure

### Filter Bar (all pages, both dashboards)

| #   | Filter       | Type             | Field          | Default |
| --- | ------------ | ---------------- | -------------- | ------- |
| 1   | Fiscal Year  | pillbox (single) | `FYLabel`      | FY2026  |
| 2   | Quarter      | pillbox (multi)  | `CloseQuarter` | All     |
| 3   | Month        | pillbox (multi)  | `MonthLabel`   | All     |
| 4   | Unit Group   | pillbox (multi)  | `UnitGroup`    | All     |
| 5   | Sales Region | pillbox (multi)  | `SalesRegion`  | All     |

- KPIs respond to all 5 filters (KPI_FACET_SCOPE)
- Charts respond to all 5 filters (cross-filter + pillbox)
- Tables respond to all 5 filters
- Quarter-to-Month cascading binding: selecting Q2 scopes Month picker to Apr/May/Jun
- On per-region pages, SalesRegion is pre-set and locked; other 4 filters remain active

---

## Dashboard 1: Pipeline Reporting & Insights

**Builder:** `build_pipeline_reporting.py`
**Total:** 8 pages, ~80 widgets (excluding section labels, filters, nav links)

### Page 1.0 — Global Pipeline Summary

| #   | Widget                      | Viz Type             | Dataset                         | SAQL Logic                                                                                                                                                                                                                                                                                                                                                                                                                       |
| --- | --------------------------- | -------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Total Open Pipeline ARR     | KPI                  | Pipeline_Opportunity_Operations | `sum(WeightedOpenARR) filter IsClosed="false"` — include YoY delta subtitle                                                                                                                                                                                                                                                                                                                                                      |
| 2   | Commit + Best Case ARR      | KPI                  | Pipeline_Opportunity_Operations | `sum(ARR) filter ForecastCategory in ("Commit","Best Case"), IsClosed="false"` — include YoY delta subtitle                                                                                                                                                                                                                                                                                                                      |
| 3   | Pipeline Coverage vs Quota  | KPI                  | Pipeline_Opportunity_Operations | `sum(WeightedOpenARR) / sum(QuotaContrib)` — format as ratio (e.g., "2.3x")                                                                                                                                                                                                                                                                                                                                                      |
| 4   | At-Risk ARR                 | KPI                  | Pipeline_Opportunity_Operations | `sum(AtRiskARR)` — red if > 20% of total open, include YoY delta subtitle                                                                                                                                                                                                                                                                                                                                                        |
| 5   | Deals Needing Approval      | KPI                  | Pipeline_Opportunity_Operations | `sum(MissingApprovalCount)`                                                                                                                                                                                                                                                                                                                                                                                                      |
| 6   | Pipeline Movement Waterfall | Waterfall            | Pipeline_Opportunity_Operations | `Start of Quarter Pipeline → New Created → Pulled In → Won → Lost → Pushed Out → Current Pipeline` — columnMap = null (auto-detect). Requires SAQL union of: (a) start = sum(ARR) at quarter start, (b) created = sum(ARR) where CreatedDate in quarter, (c) won = sum(ActualARR), (d) lost = sum(LostARR where IsClosed and not IsWon), (e) pushed = sum(ARR) of deals pushed out of quarter. Final bar = current open pipeline |
| 7   | Stage Conversion Sankey     | Sankey               | Pipeline_Opportunity_Operations | `group by StageBand, next StageBand; sum(ARR)` — flow from Qualify → Shape → Validate → Commit → Won / Lost. Uses stage_history RecordType rows. Width = ARR. Shows where deals leak out of the funnel. columnMap = null (auto-detect)                                                                                                                                                                                           |
| 8   | Pipeline by Region          | Horizontal bar       | Pipeline_Opportunity_Operations | `group by SalesRegion; sum(WeightedOpenARR) filter IsClosed="false"; order desc`                                                                                                                                                                                                                                                                                                                                                 |
| 9   | Quarterly Pipeline Trend    | Stacked area         | Pipeline_Opportunity_Operations | `group by CloseQuarter, ForecastCategory; sum(ARR) filter IsClosed="false"` — stacked area shows composition + trajectory more smoothly than column                                                                                                                                                                                                                                                                              |
| 10  | Deal Landscape              | Bubble               | Pipeline_Opportunity_Operations | `group by Id; x=DaysInStage, y=ARR, size=Probability, color=RiskBand filter IsClosed="false", RecordType="detail"` — top-right = large deal stuck long. Instant visual of where biggest/riskiest deals sit. columnMap = null (auto-detect)                                                                                                                                                                                       |
| 11  | Risk Distribution           | Stacked bar (single) | Pipeline_Opportunity_Operations | `group by RiskBand; sum(WeightedOpenARR) filter IsClosed="false"` — Critical/High/Medium/Low, preserves ordinal ordering                                                                                                                                                                                                                                                                                                         |

### Page 1.1 — Regional Comparison (replaces 7 separate pages)

Single page showing all 7 regions side by side. No pre-filtering — comparison is the point.

| #   | Widget                      | Viz Type       | Dataset                         | SAQL Logic                                                                                                                                                                                                                                                          |
| --- | --------------------------- | -------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Regional KPI Heatmap        | Heatmap        | Pipeline_Opportunity_Operations | `group by SalesRegion; foreach generate OpenPipelineARR, WonARR_YTD, WinRatePct, AtRiskARR` — rows = 7 regions, columns = 4 KPIs, color intensity = value. Requires pivoting 4 measures into a union step with MetricName dimension. columnMap = null (auto-detect) |
| 2   | Pipeline by Region × Stage  | Stacked hbar   | Pipeline_Opportunity_Operations | `group by SalesRegion, StageBand; sum(ARR) filter IsClosed="false"` — each bar = one region, stacks = stage bands. Shows where each region's pipeline sits in the funnel                                                                                            |
| 3   | Pipeline by Region × Motion | Stacked hbar   | Pipeline_Opportunity_Operations | `group by SalesRegion, MotionType; sum(ARR) filter IsClosed="false"` — shows Land/Expand/Renewal mix per region                                                                                                                                                     |
| 4   | At-Risk ARR by Region       | Horizontal bar | Pipeline_Opportunity_Operations | `group by SalesRegion; sum(AtRiskARR); order desc` — red-toned, highlights problem regions                                                                                                                                                                          |

### Page 1.2 — Regional Detail (filterable, single page)

One page with the SalesRegion filter active. User selects a region to drill in. Replaces 7 identical pages.

| #   | Widget                   | Viz Type       | Dataset                         | SAQL Logic                                                                                                                                                                                                                                   |
| --- | ------------------------ | -------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Region Open Pipeline ARR | KPI            | Pipeline_Opportunity_Operations | `sum(WeightedOpenARR) filter IsClosed="false"` — responds to SalesRegion filter                                                                                                                                                              |
| 2   | Region Won ARR (YTD)     | KPI            | Pipeline_Opportunity_Operations | `sum(ActualARR) filter FYLabel="FY2026"` — responds to SalesRegion filter                                                                                                                                                                    |
| 3   | Region Win Rate          | KPI            | Pipeline_Opportunity_Operations | Count-based: `sum(case IsWon="true" then 1 else 0) / count() filter IsClosed="true" * 100`                                                                                                                                                   |
| 4   | Region At-Risk ARR       | KPI            | Pipeline_Opportunity_Operations | `sum(AtRiskARR)`                                                                                                                                                                                                                             |
| 5   | Pipeline by Stage        | Horizontal bar | Pipeline_Opportunity_Operations | `group by StageBand; sum(ARR) filter IsClosed="false"` — Qualify, Shape, Validate, Commit                                                                                                                                                    |
| 6   | Pipeline by Motion       | Stacked column | Pipeline_Opportunity_Operations | `group by MotionType; sum(ARR) filter IsClosed="false"` — Land/Expand/Renewal/Services, responds to SalesRegion filter                                                                                                                       |
| 7   | Top 10 Deals             | Compare table  | Pipeline_Opportunity_Operations | `filter IsClosed="false"; order by ARR desc; limit 10` — responds to SalesRegion filter. Columns: OpportunityName (record action → navigateToRecord via Id), AccountName, OwnerName, ARR, StageName, ForecastCategory, DaysInStage, RiskBand |

### Page 2.0 — Commercial Approval Overview (Global)

Uses two datasets: `Pipeline_Opportunity_Operations` for structural gaps (NeedsApproval), `Forecast_Revenue_Motions` for process bottlenecks (CommercialApprovalFlag, StaleCommercialApprovalFlag).

| #   | Widget                    | Viz Type       | Dataset                                                    | SAQL Logic                                                                                                                                                                                                                                                                             |
| --- | ------------------------- | -------------- | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Approved Deal ARR         | KPI            | Pipeline_Opportunity_Operations                            | `sum(ARR) filter IsClosed="false", NeedsApproval="false", StageOrder >= 3`                                                                                                                                                                                                             |
| 2   | Compliance %              | KPI            | Pipeline_Opportunity_Operations                            | `count(NeedsApproval="false" and StageOrder>=3) / count(StageOrder>=3) * 100`                                                                                                                                                                                                          |
| 3   | Stale >14d (Count)        | KPI            | Forecast_Revenue_Motions                                   | `sum(StaleCommercialApprovalFlag)` — red highlight, escalation trigger                                                                                                                                                                                                                 |
| 4   | Approval Funnel           | Funnel         | Pipeline_Opportunity_Operations + Forecast_Revenue_Motions | Three-stage funnel: `Not Submitted` (stage 3+ with no submit date) → `Pending` (CommercialApprovalFlag=1) → `Approved` (NeedsApproval="false" and StageOrder>=3). SAQL union step combining counts from both datasets. Width narrows as deals progress. columnMap = null (auto-detect) |
| 5   | Approval Status by Region | Stacked hbar   | Pipeline_Opportunity_Operations                            | `group by SalesRegion; count() filter StageOrder >= 3, IsClosed="false"` — split by NeedsApproval (Approved / Missing)                                                                                                                                                                 |
| 6   | Approval Status by Motion | Stacked column | Pipeline_Opportunity_Operations                            | `group by MotionType; count() filter StageOrder >= 3, IsClosed="false"` — split by NeedsApproval                                                                                                                                                                                       |
| 7   | Approval Trend (Monthly)  | Line           | Pipeline_Opportunity_Operations                            | `group by MonthLabel; sum(MissingApprovalCount)` — trailing 6 months                                                                                                                                                                                                                   |

### Page 2.1 — Commercial Approval Candidates (filterable, single page)

Single page with SalesRegion filter. Replaces 7 identical pages.

| #   | Widget                 | Viz Type      | Dataset                         | SAQL Logic                                                                                                                                                                                                                                                          |
| --- | ---------------------- | ------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Missing Approval Count | KPI           | Pipeline_Opportunity_Operations | `sum(MissingApprovalCount)` — responds to SalesRegion filter                                                                                                                                                                                                        |
| 2   | Missing Approval ARR   | KPI           | Pipeline_Opportunity_Operations | `sum(MissingApprovalARR)` — responds to SalesRegion filter                                                                                                                                                                                                          |
| 3   | Candidates Table       | Compare table | Pipeline_Opportunity_Operations | `filter NeedsApproval="true", IsClosed="false"; order by ARR desc` — responds to SalesRegion filter. Columns: OpportunityName (record action → navigateToRecord via Id), AccountName, OwnerName, ARR, StageName, ForecastCategory, DaysInStage, PushCount, NextStep |

**Builder change required:** Add `NextStep` to `Pipeline_Opportunity_Operations` SOQL query (currently only in `Forecast_Revenue_Motions`).

### Page 3.0 — Renewals Tracking

| #   | Widget                           | Viz Type       | Dataset                  | SAQL Logic                                                                                                                                                                                                                                                  |
| --- | -------------------------------- | -------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Open Renewal ARR                 | KPI            | Revenue_Retention_Health | `sum(OpenRenewalValue) filter OppType="Renewal"`                                                                                                                                                                                                            |
| 2   | At-Risk Renewal ARR              | KPI            | Revenue_Retention_Health | `sum(AtRiskRenewalValue)` — red if > 30% of open                                                                                                                                                                                                            |
| 3   | Renewal Count (This Quarter)     | KPI            | Revenue_Retention_Health | `count() filter OppType="Renewal", IsClosed=0, QuarterLabel="{current_quarter}"`                                                                                                                                                                            |
| 4   | Renewal Win Rate (Prior Quarter) | KPI            | Revenue_Retention_Health | `sum(IsWon) / count() filter OppType="Renewal", IsClosed=1, QuarterLabel="{prior_quarter}" * 100`                                                                                                                                                           |
| 5   | GRR                              | KPI            | Revenue_Retention_Health | `filter RecordType="yearly_metric", YearLabel="{current_year}"; foreach generate GRR` — target 90%                                                                                                                                                          |
| 6   | Renewals by Risk Level           | Horizontal bar | Revenue_Retention_Health | `group by RiskLevel; sum(OpenRenewalValue) filter OppType="Renewal", IsClosed=0` — ordered by severity (Overdue first)                                                                                                                                      |
| 7   | Renewal Wall Heatmap             | Heatmap        | Revenue_Retention_Health | `group by MonthLabel, RiskLevel; sum(OpenRenewalValue) filter OppType="Renewal", IsClosed=0` — x-axis = next 12 months, y-axis = risk level, color = ARR concentration. Shows exactly when/where the renewal pressure peaks. columnMap = null (auto-detect) |
| 8   | Renewals by Owner                | Horizontal bar | Revenue_Retention_Health | `group by OwnerName; sum(OpenRenewalValue) filter OppType="Renewal", IsClosed=0; order desc; limit 15`                                                                                                                                                      |
| 9   | At-Risk Renewals Table           | Compare table  | Revenue_Retention_Health | `filter OppType="Renewal", IsClosed=0, RiskLevel in ("Overdue","Critical","High"); order by DaysUntilClose asc` — columns: OppName, AccountName, OwnerName, ManagerName, RecurringValue, RiskLevel, DaysUntilClose, ProductFamily, Outcome                  |

**Value field:** Header-level `APTS_Renewal_ACV__c` — confirmed correct by profiling work. OLI blend is for the churn methodology (different use case).

### Page 4.0 — Churn Risk & Trends

**Requires new dataset:** `Retention_Product_Analysis` (see below).

| #   | Widget                     | Viz Type           | Dataset                    | SAQL Logic                                                                                                                                                                                                                                                         |
| --- | -------------------------- | ------------------ | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | Churn ARR (Current Year)   | KPI                | Retention_Product_Analysis | `sum(ChurnARR) filter YearLabel="2026"`                                                                                                                                                                                                                            |
| 2   | Churn Rate                 | KPI                | Retention_Product_Analysis | `sum(ChurnARR) / sum(InstalledARR) filter YearLabel="2025" * 100` (prior year base)                                                                                                                                                                                |
| 3   | NRR                        | KPI                | Retention_Product_Analysis | Pre-computed in yearly_metric rows: `(RetainedARR + ExpansionARR) / InstalledARR * 100 filter YearLabel="{current_year}"` — green if > 100%                                                                                                                        |
| 4   | GRR                        | KPI                | Retention_Product_Analysis | Pre-computed in yearly_metric rows: `RetainedARR / InstalledARR * 100 filter YearLabel="{current_year}"` — target 90%                                                                                                                                              |
| 5   | Protected ARR              | KPI                | Retention_Product_Analysis | `sum(ChurnARR) filter EffectiveRetentionFlag="Protected"` — shows "saved" churn                                                                                                                                                                                    |
| 6   | Churn Risk Landscape       | Bubble             | Retention_Product_Analysis | `group by AccountName; x=ChurnRate, y=InstalledARR, size=ChurnARR, color=SalesRegion filter ChurnARR > 0` — top-right = high churn rate on large installed base (worst). Shows region, magnitude, and severity in one viz. columnMap = null (auto-detect)          |
| 7   | Churn by Product Family    | Horizontal bar     | Retention_Product_Analysis | `group by ProductFamily; sum(ChurnARR); order desc` — which products are churning most                                                                                                                                                                             |
| 8   | Churn by Region x Industry | Heatmap            | Retention_Product_Analysis | `group by SalesRegion, IndustryGroup; sum(ChurnARR)` — rows = regions (7), columns = industry groups (8 via `_industry_group()`), color = churn ARR. Shows the intersection of where and what is churning. columnMap = null (auto-detect)                          |
| 9   | Churn Trend (Quarterly)    | Combo (bar + line) | Retention_Product_Analysis | `group by QuarterLabel; bar=sum(ChurnARR), line=NRR%` — trailing 8 quarters, dual axes. Requires quarterly-grain metric rows in dataset                                                                                                                            |
| 10  | Churned Accounts Table     | Compare table      | Retention_Product_Analysis | `filter ChurnARR > 0, YearLabel="2026"; order by ChurnARR desc; limit 20` — columns: AccountName (record action → navigateToRecord via AccountId), UnitGroup, SalesRegion, IndustryGroup, ProductFamily, InstalledARR, ChurnARR, ChurnRate, EffectiveRetentionFlag |

### Page 5.0 — Slipped Deals Analysis

| #   | Widget                       | Viz Type       | Dataset                         | SAQL Logic                                                                                                                                                                                                                                                            |
| --- | ---------------------------- | -------------- | ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Slipped Deal Count           | KPI            | Pipeline_Opportunity_Operations | `count() filter PushCount >= 1, IsClosed="false"`                                                                                                                                                                                                                     |
| 2   | Slipped Deal ARR             | KPI            | Pipeline_Opportunity_Operations | `sum(ARR) filter PushCount >= 1, IsClosed="false"`                                                                                                                                                                                                                    |
| 3   | Avg Push Count               | KPI            | Pipeline_Opportunity_Operations | `avg(PushCount) filter PushCount >= 1, IsClosed="false"`                                                                                                                                                                                                              |
| 4   | Avg Days Pushed              | KPI            | Pipeline_Opportunity_Operations | `avg(PushDays) filter PushCount >= 1, IsClosed="false"`                                                                                                                                                                                                               |
| 5   | Repeat Offenders (3+ pushes) | KPI            | Pipeline_Opportunity_Operations | `count() filter PushCount >= 3, IsClosed="false"` — red highlight                                                                                                                                                                                                     |
| 6   | Push Count Distribution      | Column         | Pipeline_Opportunity_Operations | `group by PushCount; count() filter PushCount >= 1, IsClosed="false"` — x-axis: 1, 2, 3, 4, 5+                                                                                                                                                                        |
| 7   | Slipped ARR by Region        | Horizontal bar | Pipeline_Opportunity_Operations | `group by SalesRegion; sum(ARR) filter PushCount >= 1, IsClosed="false"; order desc`                                                                                                                                                                                  |
| 8   | Slippage Trend (Monthly)     | Line           | Pipeline_Opportunity_Operations | `group by EventMonth; count() filter RecordType="field_history", EventField="CloseDate"` — trailing 6 months                                                                                                                                                          |
| 9   | Slipped Deals Table          | Compare table  | Pipeline_Opportunity_Operations | `filter PushCount >= 1, IsClosed="false"; order by ARR desc; limit 25` — columns: OpportunityName (record action → navigateToRecord via Id), AccountName, OwnerName, ARR, StageName, MotionType, PushCount, PushDays, DaysInStage, RiskBand, RootCauseHypothesis      |
| 10  | Slipped by Motion Type       | Stacked column | Pipeline_Opportunity_Operations | `group by MotionType; count() filter PushCount >= 1, IsClosed="false"` — Land/Expand/Renewal/Services, shows which motion type is slipping most                                                                                                                       |
| 11  | Slippage Risk Landscape      | Bubble         | Pipeline_Opportunity_Operations | `group by Id; x=PushCount, y=ARR, size=DaysInStage, color=RiskBand filter PushCount >= 1, IsClosed="false", RecordType="detail"` — top-right = high-value deal with many pushes, large bubbles = long stage dwell. Visual urgency map. columnMap = null (auto-detect) |

**Root Cause Hypothesis:** Computed dimension in the builder. Uses empirical stage dwell thresholds (P50/P75/P90 by MotionType x Stage, computed at build time from closed-won opps). Priority-ordered logic:

1. `PushCount >= 3 AND DaysInStage > P90` → "Stalled — {PushCount} pushes, {DaysInStage}d in {Stage} (P90 for {Motion}: {P90}d)"
2. `NeedsApproval AND DaysInStage > P75` → "Approval bottleneck — no commercial approval, {DaysInStage}d (P75: {P75}d)"
3. `DaysInStage > P90` → "Stage stall — {DaysInStage}d in {Stage} (P90 for {Motion}: {P90}d)"
4. `ForecastDowngradeCount >= 1 AND PushCount >= 1` → "Weakening conviction — forecast downgraded {N}x with {PushCount} push(es)"
5. `BackwardMoveCount >= 1` → "Deal regression — moved backward {N}x, likely re-qualifying"
6. `IsPastDue` → "Past due — close date {days}d overdue"
7. `DaysInStage > P75` → "Slow — {DaysInStage}d exceeds P75 ({P75}d for {Motion})"
8. `PushCount >= 1` → "Monitor — {PushCount} push(es), {DaysInStage}d in {Stage} ({Motion} P50: {P50}d)"

**Prerequisite:** Profile PushCount, ForecastDowngradeCount, BackwardMoveCount, NeedsApproval, and IsPastDue distributions before finalizing priority order. Hypothesis logic will be adjusted based on what signals are actually populated and discriminating.

---

## Dashboard 2: Sales Ops Data Quality & Forecast Accuracy

**Builder:** `build_sales_ops_reporting.py`
**Total:** 3 pages, ~28 widgets (excluding section labels, filters, nav links)

### Page 1.0 — Account Data Quality

| #   | Widget                     | Viz Type       | Dataset                 | SAQL Logic                                                                                                                                                           |
| --- | -------------------------- | -------------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Avg Data Quality Score     | KPI            | Customer_Account_Health | `avg(DataQualityScore)` (0-100 scale) — green >=80, amber >=60, red <60                                                                                              |
| 2   | DUNS Fill Rate             | KPI            | Account_Intelligence    | `count() filter HasDUNS="true" / count() * 100` — target 95%                                                                                                         |
| 3   | Unit Group Fill Rate       | KPI            | Account_Intelligence    | `count() filter HasUnitGroup="true" / count() * 100` — target 100%                                                                                                   |
| 4   | KYC Approved %             | KPI            | Customer_Account_Health | `count() filter KYCStatus="Approved" / count() * 100`                                                                                                                |
| 5   | Poor Quality Accounts      | KPI            | Customer_Account_Health | `count() filter DataQualityBand="Poor"` — red highlight                                                                                                              |
| 6   | Data Completeness by Field | Horizontal bar | Account_Intelligence    | `count(HasDUNS="true"), count(HasUnitGroup="true"), count(HasAxiomaId="true")` as % of total — ranked worst-first                                                    |
| 7   | Data Quality by Unit Group | Stacked column | Customer_Account_Health | `group by UnitGroup; count() group by DataQualityBand` — stacks: Good/Fair/Poor                                                                                      |
| 8   | Quality Trend (Quarterly)  | Line           | Customer_Account_Health | `group by MonthLabel; avg(DataQualityScore)` from portfolio_trend rows — trailing 4 quarters                                                                         |
| 9   | Worst Accounts Table       | Compare table  | Customer_Account_Health | `filter DataQualityBand="Poor"; order by TotalWonARR desc; limit 20` — columns: AccountName, OwnerName, UnitGroup, DataQualityScore, HasDUNS, KYCStatus, TotalWonARR |

**Data quality scoring note:** Composite score (KPIs, trend) uses `Customer_Account_Health` (0-100 scale). Field-level booleans (completeness bar) use `Account_Intelligence`. The 0-5 integer score from `Account_Intelligence` is never displayed directly to avoid confusion with the 0-100 scale.

### Page 1.1 — Opportunity Data Hygiene

| #   | Widget                                   | Viz Type       | Dataset                         | SAQL Logic                                                                                                                                                                                                                                                                |
| --- | ---------------------------------------- | -------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Win/Loss Reason Fill Rate                | KPI            | Opp_Mgmt_KPIs                   | `count() filter IsClosed="true", WonLostReason != "" / count() filter IsClosed="true" * 100`                                                                                                                                                                              |
| 2   | Past Due Open Opps                       | KPI            | Pipeline_Opportunity_Operations | `sum(PastDueCount)` — red highlight                                                                                                                                                                                                                                       |
| 3   | Past Due ARR                             | KPI            | Pipeline_Opportunity_Operations | `sum(PastDueARR)`                                                                                                                                                                                                                                                         |
| 4   | Forecast Category Set %                  | KPI            | Opp_Mgmt_KPIs                   | `count() filter ForecastCategory != "Pipeline", IsClosed="false" / count() filter IsClosed="false" * 100`                                                                                                                                                                 |
| 5   | Stale Pipeline %                         | KPI            | Pipeline_Opportunity_Operations | `sum(StaleCount) / count() filter IsClosed="false" * 100` — target <20%                                                                                                                                                                                                   |
| 6   | Past Due by Owner                        | Horizontal bar | Pipeline_Opportunity_Operations | `group by OwnerName; sum(PastDueCount); order desc; limit 15`                                                                                                                                                                                                             |
| 7   | Missing Win/Loss Reason by Month         | Line           | Opp_Mgmt_KPIs                   | `group by CloseMonth; count() filter IsClosed="true", WonLostReason=""` — trailing 6 months                                                                                                                                                                               |
| 8   | Hygiene Heatmap                          | Heatmap        | Pipeline_Opportunity_Operations | `group by SalesRegion, ExceptionType; count()` — regions on y-axis (~7), exception types on x-axis (~8, excluding "Monitor")                                                                                                                                              |
| 9   | Opps Missing Win/Loss Reason             | Compare table  | Opp_Mgmt_KPIs                   | `filter IsClosed="true", WonLostReason=""; order by ARR desc; limit 20` — columns: Name, OwnerName, AccountName, ARR, StageName, CloseMonth                                                                                                                               |
| 10  | Past Due Opps Table (with record action) | Compare table  | Pipeline_Opportunity_Operations | `filter IsPastDue="true", IsClosed="false"; order by DaysToClose asc` — columns: OpportunityName (record action → navigateToRecord via Id), AccountName, OwnerName, ARR, StageName, DaysToClose (absolute, labeled "days overdue"), PushCount, ForecastCategory, RiskBand |

### Page 2.0 — Forecast Accuracy

| #   | Widget                         | Viz Type           | Dataset               | SAQL Logic                                                                                                                                                                                                           |
| --- | ------------------------------ | ------------------ | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Forecast Accuracy (Prior Q)    | KPI                | Forecast_Intelligence | `sum(ClosedWonARR) / sum(WeightedForecast) * 100 filter CloseQuarter="{prior_q}"` — green 90-110%, amber 80-120%, red outside                                                                                        |
| 2   | Quota Attainment (Prior Q)     | KPI                | Forecast_Intelligence | `sum(ClosedWonARR) / sum(QuotaAmount) * 100 filter CloseQuarter="{prior_q}"`                                                                                                                                         |
| 3   | Commit Conversion Rate         | KPI                | Forecast_Intelligence | `sum(ClosedWonARR) / sum(CommitARR + ClosedWonARR) * 100 filter CloseQuarter="{prior_q}"`                                                                                                                            |
| 4   | Forecast Bias                  | KPI                | Forecast_Intelligence | `(sum(WeightedForecast) - sum(ClosedWonARR)) / sum(ClosedWonARR) * 100 filter CloseQuarter="{prior_q}"` — positive = over-forecasting, negative = sandbagging                                                        |
| 5   | Current Quarter Coverage       | KPI                | Forecast_Intelligence | `sum(CommitARR + BestCaseARR + PipelineARR + ClosedWonARR) / sum(QuotaAmount) filter CloseQuarter="{current_q}"` — format as ratio                                                                                   |
| 6   | Accuracy by Unit Group         | Grouped column     | Forecast_Intelligence | `group by UnitGroup; sum(ClosedWonARR), sum(WeightedForecast) filter CloseQuarter="{prior_q}"` — two bars: Actual vs Forecast                                                                                        |
| 7   | Accuracy by Quarter (Trailing) | Combo (bar + line) | Forecast_Intelligence | `group by CloseQuarter; bar=sum(ClosedWonARR), line=accuracy%` — trailing 4 quarters, dual axes                                                                                                                      |
| 8   | Rep Forecast Accuracy          | Bubble             | Forecast_Intelligence | `group by OwnerName; x=WeightedForecast, y=ClosedWonARR, size=OppCount filter CloseQuarter="{prior_q}", OppCount >= 3` — 45-degree line = perfect accuracy                                                           |
| 9   | Rep Accuracy Table             | Compare table      | Forecast_Intelligence | `group by OwnerName; order by abs(accuracy-100) desc; limit 20 filter CloseQuarter="{prior_q}"` — columns: OwnerName, UnitGroup, ClosedWonARR, WeightedForecast, QuotaAmount, Accuracy%, Attainment%, Bias, OppCount |

**Cross-dashboard nav:** Rep Accuracy table links to Forecast & Revenue Motions dashboard (`0FKTb0000000JCLOA2`), pre-filtered by OwnerName and CloseQuarter, for opp-level drill-through.

**Deferred (pending Weekly_Forecast_Summary dataset):**

- WoW Commit Delta line chart
- Category Migration Sankey

---

## New Dataset: Retention Product Analysis

**Builder:** `build_retention_product_analysis.py`
**Grain:** Account x Product Family x Fiscal Year
**Purpose:** Churn analysis at account, region, product, and industry level using OLI-level ACV blend methodology

### Sources

- `Opportunity` — Type IN (Land, Expand, Renewal), CloseDate >= 2022-01-01
- `OpportunityLineItem` — ACV blend: `APTS_ACV_1st_Year__c` -> `APTS_Forecast_ACV_AVG__c` -> 0
- `Account` — `Unit_Group__c`, `Region__c`, `Industry`, `BillingCountry`

### Churn/Retention Logic

Reuses motion classification from `build_revenue_retention_health.py`:

- Won Renewal = Retained
- Lost Renewal = Churned
- Won Expand = Expansion
- Won Land = New Logo
- Effective Retention: lost renewal on account where Expand was won within 90 days on same account = "Protected" (validated cases: Nykredit, PFA, Finanz Informatik)

### Dimensions

| Field                  | Source                                                             |
| ---------------------- | ------------------------------------------------------------------ |
| AccountId              | Opportunity.AccountId                                              |
| AccountName            | Account.Name                                                       |
| UnitGroup              | Account.Unit_Group\_\_c                                            |
| SalesRegion            | Account.Region\_\_c (7 values)                                     |
| IndustryGroup          | `_industry_group(Account.Industry)` — 8 groups                     |
| BillingCountry         | Account.BillingCountry                                             |
| ProductFamily          | OLI Product2.Family or APTS_ProductArea\_\_c                       |
| Segment                | Enterprise / Mid-Market / Growth (from AuM/ARR)                    |
| YearLabel              | Fiscal year string                                                 |
| QuarterLabel           | YYYY-QN                                                            |
| Outcome                | Won / Lost / Churned / Open                                        |
| Motion                 | Retained / Churned / Expanded / Contraction / New Logo             |
| EffectiveRetentionFlag | "Protected" if lost renewal but same-account Expand won within 90d |

### Measures

| Field        | Formula                                                       |
| ------------ | ------------------------------------------------------------- |
| InstalledARR | Sum of won OLI ACV blend per account x product x year         |
| ChurnARR     | Lost Renewal OLI ACV per account x product x year             |
| RetainedARR  | Won Renewal OLI ACV                                           |
| ExpansionARR | Won Expand OLI ACV                                            |
| NewLogoARR   | Won Land OLI ACV                                              |
| ChurnRate    | ChurnARR / prior year InstalledARR \* 100                     |
| NRR          | (RetainedARR + ExpansionARR) / prior year InstalledARR \* 100 |
| GRR          | RetainedARR / prior year InstalledARR \* 100                  |

### Reference Files

- `build_revenue_retention_health.py` — churn math, motion classification, NRR/GRR formulas
- `build_product_portfolio_dashboard.py` — ACV blend (`_commercial_value()`), OLI join, `InstalledARR` definition
- `scripts/profile_retention_product_grain.py` — coverage findings (which grain to trust per opp type)
- `docs/generated/retention_account_validation_2026-03-11.md` — semantic validation and protection-match rules

---

## Validate Before Building

These items require data profiling before implementation. Run SAQL or SOQL queries against the live org.

### 1. Contract Coverage Gap

Query contracts expiring in 90 days with no open Renewal opp on the same account. If count is meaningful (10+), add a "Contracts Without Renewal Opp" widget to Page 3.0. If near zero, skip.

### 2. Root Cause Hypothesis Inputs

Profile against `Pipeline_Opportunity_Operations`:

- PushCount distribution (% with 1, 2, 3+ pushes)
- ForecastDowngradeCount (populated vs zero)
- BackwardMoveCount (how frequent)
- NeedsApproval population rate (is Stage_20_Approval\_\_c populated)
- IsPastDue rate on open pipeline (% past due)

Adjust hypothesis priority order based on which signals are actually populated and discriminating.

### 3. Stage Dwell Benchmarks

Query `Opp_Mgmt_KPIs` for closed-won opps:

- P50, P75, P90 of DaysInStage by MotionType x StageOrder
- These empirical thresholds replace all hardcoded SLA values in the hypothesis logic

### 4. Stage_20_Approval\_\_c Field Check

`SELECT count(Id) FROM Opportunity WHERE Stage_20_Approval__c != null`
If null everywhere, defer approval widgets and flag for admin team.

---

## Builder Changes to Existing Code

| Change                                     | File                                       | Detail                                                                                                                                  |
| ------------------------------------------ | ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| Add `NextStep` to SOQL                     | `build_pipeline_opportunity_operations.py` | Add `NextStep` field to Opportunity SOQL query and include as dimension in dataset                                                      |
| Add `RootCauseHypothesis` dimension        | `build_pipeline_opportunity_operations.py` | Pre-computed dimension using empirical P50/P75/P90 thresholds. Cannot be expressed as inline SAQL — requires builder-level computation  |
| Move `_industry_group()` to shared helpers | `crm_analytics_helpers.py`                 | Currently only in `build_bdr_operating_dashboards.py`. Define exact output groups (resolve 4 vs 8 group question during implementation) |
| No other changes                           | —                                          | All other datasets are consumed as-is                                                                                                   |

---

## Design Decisions Log

| Decision                                                                       | Rationale                                                                                                                      |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| Stacked bar over donut for risk                                                | Risk is ordinal — stacked bar preserves Critical→Low ordering                                                                  |
| Horizontal bar over treemap for industry churn                                 | Raw Industry has 25+ values; `_industry_group()` reduces to 8 readable groups                                                  |
| Count-based win rate (not ARR-weighted)                                        | Standard across all existing SimCorp dashboards (Dashboard 1, Sales Velocity)                                                  |
| Bubble over scatter for rep accuracy                                           | Existing pattern in `build_forecasting.py`; OppCount as size adds dimension; min filter `opp_count >= 3`                       |
| Three-state approval (Not Submitted / Pending / Stale)                         | Separates rep compliance, process bottleneck, and escalation trigger                                                           |
| Cross-dashboard nav for forecast drill-through                                 | `Forecast_Intelligence` is rep x quarter grain; opp-level detail lives in `Forecast_Revenue_Motions`                           |
| Customer_Account_Health for composite score, Account_Intelligence for booleans | Avoids 0-5 vs 0-100 confusion; each dataset used for its strength                                                              |
| Empirical stage thresholds over hardcoded SLAs                                 | Both existing SLA systems (14-24d and 21-60d) were our invention, not SimCorp-defined; data-driven P50/P75/P90 is ground truth |
| OLI-level ACV blend for churn                                                  | Header-level Renewal ACV is correct for renewal tracking; OLI blend gives product-level churn granularity                      |
| Pipeline waterfall over stacked column                                         | Waterfall shows pipeline _movement_ (created, won, lost, pushed) — answers "what happened?" not just "what's the snapshot"     |
| Stage Sankey on global page                                                    | Shows where deals leak out of the funnel — width = ARR. Uses existing stage_history RecordType rows                            |
| Deal landscape bubble on global page                                           | x=DaysInStage, y=ARR, size=Probability, color=RiskBand — instant visual radar of where biggest risks sit                       |
| Regional comparison heatmap over 7 identical pages                             | One page with 4 KPIs x 7 regions as heatmap + one filterable detail page. Enables cross-region comparison, cuts 7→2 pages      |
| Approval funnel over 3 separate KPI numbers                                    | Not Submitted → Pending → Approved is a natural funnel progression. Width narrows as deals advance                             |
| Renewal wall heatmap over quarterly stacked column                             | Month x RiskLevel heatmap shows exactly when/where renewal pressure peaks — more actionable than quarterly bars                |
| Churn risk bubble over 3 horizontal bars                                       | x=ChurnRate, y=InstalledARR, size=ChurnARR, color=Region — shows 3 dimensions in one viz, dangerous quadrant jumps out         |
| Region x Industry churn heatmap over separate industry bar                     | Shows the intersection of where and what is churning — 7x8 matrix, color = churn ARR                                           |
| Slippage risk bubble on slipped deals page                                     | x=PushCount, y=ARR, size=DaysInStage, color=RiskBand — visual urgency map, biggest risks visible instantly                     |
| Stacked area over stacked column for pipeline trend                            | Area shows composition + trajectory more smoothly over 4+ quarters                                                             |
| Effective Retention flag                                                       | Catches "lost renewal + same-account Expand" pattern validated on named accounts                                               |

---

## Implementation Notes

### Dataset field type differences

- `Pipeline_Opportunity_Operations`: `IsClosed` is a **string dimension** (`"true"` / `"false"`)
- `Revenue_Retention_Health`: `IsClosed` is a **numeric measure** (`0` / `1`)
- `Pipeline_Opportunity_Operations`: opportunity name field is `OpportunityName`
- `Revenue_Retention_Health`: opportunity name field is `OppName`
- `InstalledARR` in `Retention_Product_Analysis` corresponds to `StartingARR` in `Revenue_Retention_Health`

Do not copy SAQL patterns between datasets without checking field types.

### Visualization type clarifications

- "Grouped column" (Page 2.0 widget 6) = standard `column` chart type with two measures per group dimension (Actual and Forecast bars side by side)
- "Churn Trend" (Page 4.0 widget 9) = `combo` chart (bar + line), not `line` — dual axes required for ChurnARR (bar) + NRR% (line). Requires quarterly-grain churn data in the new dataset, not just yearly.
- Cross-dashboard nav (Page 2.0 widget 9) = cell-level link using `nav_link_external()` pattern, pre-filtered by OwnerName and CloseQuarter

### Filter bar scoping per page

Not all datasets carry all 5 filter dimensions:

| Dataset                          | FYLabel         | CloseQuarter        | MonthLabel | UnitGroup | SalesRegion |
| -------------------------------- | --------------- | ------------------- | ---------- | --------- | ----------- |
| Pipeline_Opportunity_Operations  | yes             | yes                 | yes        | yes       | yes         |
| Forecast_Revenue_Motions         | yes             | yes                 | yes        | yes       | yes         |
| Revenue_Retention_Health         | yes (YearLabel) | yes                 | yes        | no        | no          |
| Customer_Account_Health          | no              | no                  | yes        | yes       | no          |
| Account_Intelligence             | no              | no                  | no         | yes       | no          |
| Opp_Mgmt_KPIs                    | yes             | yes (FiscalQuarter) | CloseMonth | yes       | yes         |
| Forecast_Intelligence            | yes             | yes                 | no         | yes       | no          |
| Retention_Product_Analysis (new) | yes             | yes                 | no         | yes       | yes         |

Pages using datasets without certain filter fields will have those filters silently inactive. This is acceptable — the filter pillbox simply won't affect those widgets. Alternative: scope filter bar per-page at build time to only show applicable filters.

### Widget count correction

Dashboard 1: 8 pages, ~80 widgets (excluding section labels, filters, nav links)
Dashboard 2: 3 pages, ~28 widgets (excluding section labels, filters, nav links)
Total: 11 pages across 2 dashboards

### Filter field name mapping

Datasets use different field names for the same concept. The builder must map filter bindings per dataset:

| Filter  | Pipeline_Opportunity_Operations | Revenue_Retention_Health | Opp_Mgmt_KPIs         | Forecast_Intelligence |
| ------- | ------------------------------- | ------------------------ | --------------------- | --------------------- |
| Year    | `FYLabel`                       | `YearLabel`              | `FYLabel`             | `FYLabel`             |
| Quarter | `CloseQuarter`                  | `QuarterLabel`           | `FiscalQuarter` (int) | `CloseQuarter`        |
| Month   | `MonthLabel`                    | `MonthLabel`             | `CloseMonth`          | n/a                   |

Each step's SAQL must use the correct field name for its dataset. The filter bar pillbox binds to the step, not the dataset directly.

### Record actions on all actionable tables

Every compare table with opportunity or account data must have a record action on the name column:

- Opportunity tables: `OpportunityName` / `OppName` → `navigateToRecord` via `Id` / `OppId`
- Account tables: `AccountName` → `navigateToRecord` via `AccountId`

### Section labels

Every page should include `section_label()` widgets to separate KPI strip, chart zone, and table zone — standard pattern from all existing builders.

---

## Upgrade Opportunities (implement if time permits)

### YoY deltas on Global Pipeline KPIs (Page 1.0)

Add a YoY comparison subtitle to KPIs 1-4: `(current - prior_year) / prior_year * 100`. Shows trend direction without a separate chart. Requires SAQL that loads two fiscal years and computes the delta. Pattern exists in `build_dashboard_1.py` (Forecast Accuracy Prior Q benchmark).

### QoQ delta on Data Quality Score (Dashboard 2, Page 1.0)

Add `vs last quarter` delta to widget 1 (Avg Data Quality Score). Uses the portfolio_trend rows already in `Customer_Account_Health`. Provides accountability signal — is data quality improving or not.

### Renewal Coverage % (Page 3.0)

New KPI: "What % of accounts with expiring contracts in the next 90 days have an open Renewal opp?" Ties directly to the contract coverage gap validation item. If validation shows a gap, this becomes a permanent KPI. If not, skip.

### Forecast Accuracy by Unit Group trend (Dashboard 2, Page 2.0)

The trailing accuracy combo chart (widget 7) is global only. Allow the UnitGroup filter to slice it, or add a small multiples breakout showing each region's accuracy trajectory over 4 quarters.
