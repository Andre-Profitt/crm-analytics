---
title: SimCorp Salesforce Data Model — Overview & Map
type: reference
audience: engineers / analysts building on this org
org: apro@simcorp.com (simcorp.my.salesforce.com)
generated: 2026-04-16
---

# SimCorp Salesforce Data Model — High-Level Map

This is the "what's actually in the org" reference. 1,966 objects total, grouped below so you know where to look.

## Top-level counts

| Bucket                                                          | Count | Notes                                                                                                                       |
| --------------------------------------------------------------- | ----: | --------------------------------------------------------------------------------------------------------------------------- |
| Standard Salesforce objects                                     |   840 | Lead, Account, Opportunity, Campaign, User, Task, Event, Quote, Product2, Order, Contract, Asset, Case, Forecasting\*, etc. |
| Custom objects (`__c`, SimCorp-owned)                           |    87 | The business-specific extension. See full inventory below.                                                                  |
| Managed-package objects                                         |   329 | Dominated by Apttus CPQ (258) + marketing/events. See packages.                                                             |
| Setup/system (Share, History, Feed, Tag, ChangeEvent, Revision) |   710 | Framework plumbing; not queried in normal reporting.                                                                        |

The real "working set" for sales analytics is **core standard objects + ~30 custom objects + Apttus CPQ quoting objects**. Everything else is admin / archival / integration plumbing.

## Managed packages

| Namespace         | Objects | What it is                                                          |
| ----------------- | ------: | ------------------------------------------------------------------- |
| `Apttus_Config2`  |     171 | Apttus CPQ — quote line items, product configuration, pricing rules |
| `Apttus`          |      58 | Apttus CPQ core objects                                             |
| `Apttus_Approval` |      19 | Apttus approval workflows (ties to our Stage_20_Approval\_\_c work) |
| `CventEvents`     |      15 | Event management (conference registrations, etc.)                   |
| `rh2`             |      12 | (Unknown — probably reporting/hierarchy tooling; low priority)      |
| `Apttus_QPConfig` |      11 | Quote-plus-proposal config                                          |
| `pi`              |      10 | Pardot — marketing automation, email engagement                     |
| `cncrg`           |       9 | (Unknown — ConcurSolutions-related?)                                |
| `Apttus_Proposal` |       6 | Proposal documents                                                  |
| `Apttus_Base2`    |       5 | Apttus base utilities                                               |

**Implication:** the Apttus CPQ stack is the quote/pricing engine. Any deep-dive into how a deal's `APTS_Opportunity_ARR__c` / `APTS_Forecast_ARR__c` is computed goes through Apttus objects — particularly `Apttus_Config2__LineItem__c`, `Apttus_Proposal__Proposal__c`, and the QPConfig pricing rules. `APTS_Primary_Quote__c` on Opportunity is the pointer into this graph.

## The core sales graph (what the SD Monthly ETL actually uses)

```
                            ┌──────────────┐
                            │  Campaign    │
                            │  (Pardot)    │
                            └──────┬───────┘
                                   │
                                   ↓
┌──────────────┐            ┌─────────────┐         ┌──────────────────┐
│  Lead        │───convert──│  Account    │ ←──────│ Installed_Competitor
└──────────────┘            └──┬──┬────┬──┘         │  _Product__c      │
                               │  │    │            └──────────────────┘
                               │  │    │  ┌──────────────────────┐
         ┌─────────────────────┘  │    └──│ Account_EI_Snapshot__c│
         ↓                        │       └──────────────────────┘
   ┌──────────────┐           ┌──────────┐
   │   Contact    │           │  Asset   │ (installed products)
   └──────────────┘           └──────────┘
                                   │
                                   ↓
                          ┌──────────────────┐
                          │   Opportunity    │
                          │                  │
                          │ APTS_Primary_Quote ─→ Apttus Proposal/Line Items
                          │ APTS_Opportunity_ARR__c   (measured ARR)
                          │ APTS_Forecast_ARR__c      (weighted ARR)
                          │ Stage_20_Approval__c      (commercial approval gate)
                          │ Type: Land/Expand/Renewal
                          │ PushCount                 (derived)
                          └──────┬────┬─────┬──────┘
                                 │    │     │
                  ┌──────────────┘    │     └───────────┐
                  ↓                   ↓                 ↓
         ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐
         │ ForecastingItem  │  │ Task + Event │  │ Field History    │
         │ (per owner/qtr)  │  │ (activity)   │  │ (CloseDate, Stage│
         └──────────────────┘  └──────────────┘  │  ForecastCat)    │
                                                 └──────────────────┘
```

## Custom objects — 87 total, grouped by purpose

### Pricing & quoting (integrates with Apttus)

- `APTS_Account_Asset_Line_Item__c`
- `APTS_Pricing_Criteria_for_MBL__c`
- `APTS_Product_Child__c`, `APTS_Product_Parent__c`
- `APTS_SimCorp_Pricing_Details__c`
- `APTS_Support_Products_Pricing__c`
- `APTS_SimCorp_Version__c`, `APTS_Version_History__c`, `Version__c`
- `Quote_Comparator__c`, `Quote_Team__c`

### Competitor & win-loss intelligence

- `Competitor__c`, `Competitor_Product__c`
- `Installed_Competitor_Product__c` ← tracks which competitor products are installed at an account

### Customer health & engagement intelligence

- `Account_Engagement_Intelligence__c`, `Contact_Engagement_Intelligence__c`, `Engagement_Intelligence__c`, `Engagement_Intelligence_Activity__c`
- `Account_EI_Snapshot__c`, `Contact_EI_Snapshot__c`
- `Adoption_Score__c`, `Adoption_Score_Contact__c`
- `CX_Persona_Mapping__c`, `CX_Persona_Product_Mapping__c`
- `Customer_Intelligence__c`
- `Customer_Value_Chain__c`
- `Customer_Reference__c`
- `NPS_Survey__c`

### Financial modeling (deal profitability)

- `EBIT_Calculation__c`, `EBIT_Calculation_Revenue_Split__c`
- `EBIT_Key_Data__c`, `EBIT_Key_Data_Business_Unit__c`
- `EBIT_Simulation__c`
- `Commercial_Growth_Driver__c`

### Opportunity outcomes & snapshots

- `Opportunity_Snapshot__c`
- `Opty_Weekly_Snapshot__c` ← likely feeds historical-trend-like analysis alongside standard `OpportunityFieldHistory`
- `Universe_Snapshot__c`
- `Outcome_Opportunity__c`, `Outcomes__c`

### Service & delivery

- `SimCorp_Service_Cloud__c`
- `Installation__c`
- `Project__c` (Service Cloud)
- `Service_Level_Agreement__c`
- `Remote_Connection_Contact__c`
- `Management_Escalation__c`, `Root_Cause_Analysis__c`
- `Support_Instructions__c`
- Remedyforce bridge: `RemedyForce_Request_Definitions__c`, `Remedyforce_IDs__c`, `Remedyforce_Priority__c`, `RemedyForce_Templates__c`, `Remedyforce_URLs__c`, `Remedyforce_profiles__c`

### Partner ecosystem

- `Partner_Offer__c`, `Partner_Product__c`

### Success planning & post-sale

- `Success_Planning_Activity__c`, `Success_Planning_Activity_Relation__c`
- `Ex_Customer__c` (churn marker)

### Legal & compliance

- `Legal_Classification_Summary__c`
- `IUCM__c`

### Marketing & website signals

- `Sitecore_PageEvents__c`, `Sitecore_PageEventDefinitions__c`
- `GeneralCampaignSettings__c`

### Financial scoring utilities

- `SBL_Discount_Calculation_Settings__c`, `SBL_Discount_Tiers__c`

### Admin / plumbing (generally ignore)

- `BypassProcedurePlanHooks__c`, `DisableFlow__c`, `DisabledTriggers__c`
- `ITSM_ProcessDeactivation__c`
- `Chatter_Delete_Settings__c`
- `Default_Team__c`, `Deleted_Object__c`
- `FlowPersonalConfiguration__c`, `FlowTableViewDefinition__c`
- `LoggerSettings__c`, `LoopioConnectorSetting__c`
- `RCA_Ramp_Wizard_Settings__c`
- `Related_Line_Item__c`
- `SF_Product_WD_Sales_Item_Mapping__c`
- `SystemSettings__c`, `System_Preferences__c`, `TestConfig__c`
- `Whitespace_Report_Config__c`
- `Unit_Group_Queue_Mappings__c`
- `Contact_User_Field_Mappings__c`, `Custom_Field_Tracking__c`
- `Collaboration_lookup_junction__c`

### Account segmentation / categorization (sample of heavy Account customization)

`Account` has **454 fields** (347 custom). The custom fields cluster into:

- **Contract lifecycle per product**: `APTS_Contract_Start_Date__c`, `APTS_Contract_End_Date__c`, `APTS_Coric_Contract_End_Date__c`, `APTS_Gain_Contract_End_Date__c`, `APTS_Open_INS_Expiry_Date__c`, `APTS_Subscription_Term__c`
- **Product ownership flags (~40 booleans)**: `AMS_Client__c`, `ASP__c` (SCDaaS Client), `Axioma_Risk_SaaS_Client__c`, `Axioma_Portfolio_Analytics_Client__c`, `CaaS_Client__c` (Client Reporting Cloud), `Channel_Play_Client__c`, `Asset_Service_Hub_Client__c`, etc.
- **Territory / segmentation**: `Account_Unit_Group__c`, `Sales_Region__c`, `Account.Unit__c`, `Account.Region__c`, `Tier_Calculation__c`, `Industry`, `Buy_Sell_Side__c`, `Classification__c`
- **Financial scale**: `AuM_m__c` (assets under management), `AuM_Currency__c`, `AuM_Currency_Rate__c`, `AuM_Universe_Calc__c`
- **License / price point economics**: `APTS_Number_of_Licenses_Owned__c`, `APTS_Number_of_Price_Points_Owned__c`, `APTS_Price_per_User_License__c`, `APTS_Total_Licenses_Owned__c`
- **Marketing / ABM**: `ABM_Campaign_1st_Priority__c`, `C_H_Level_Form_Fillouts_Last_12_Months__c`, `C_Level_Personas__c`
- **Engagement / health**: `Account_Engagement_Intelligence__c` (lookup), `Adoption_Score__c` (lookup), `CHS_Weight__c`, `Account_Latest_Activity_Date__c`
- **KYC / compliance**: `KYC_Approval_Status__c`
- **Legacy IDs**: `Account_ID__c`, `Bloomberg_ID__c`, `CIK__c`, `Axioma_External_Id__c`
- **Flags**: `Ex_Customer__c`, `AMS_Client__c`, `NDA_Signed__c`

## Opportunity — the 369-field monster

Opportunity is the single most-customized object in the org.

- **50 standard** fields (Name, StageName, CloseDate, Amount, ForecastCategoryName, OwnerId, AccountId, Probability, Type, …)
- **284 SimCorp custom** fields
- **35 managed-package** fields (Apttus CPQ extensions)

Custom-field families (≈284 fields total, grouped):

| Family                             | Purpose                                                                                                                                                                     | Key representative fields                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **APTS\_\* pricing (≈180 fields)** | Per-product-line pricing: SBL, MBL/PPL, CDD, Coric (Client Reporting), Gain (DM Software), DataCare, Oracle, RUS (Axioma/BPaaS), PSO/PSA, 3rd Party, SaaS, Extended Support | `APTS_Opportunity_ARR__c`, `APTS_Forecast_ARR__c`, `APTS_Renewal_ACV__c`, `APTS_Forecast_NPP_Display__c` (Order Inflow), `APTS_Forecast_Quota_Retirement__c`, `APTS_CDD_TCV_Display__c`, `APTS_Coric_TCV_Display__c`, `APTS_Gain_TCV_Display__c`, `APTS_SBL_Annual_Total__c`, `APTS_Contract_Start_Date__c` / `_End_Date__c`, `APTS_Contract_Selling_Term__c`, `APTS_Contract_Total_GP1perc__c` (margin %) |
| **Commercial approval gate**       | Two-level approval workflow                                                                                                                                                 | `Stage_20_Approval__c`, `Stage_20_Approval_Date__c`, `Submit_for_Stage_20_Review__c`, `Submit_for_Stage_20_Review_Date__c`, `Approval_Status__c`                                                                                                                                                                                                                                                           |
| **Territory / assignment**         | Director/team routing                                                                                                                                                       | `Account_Unit_Group__c`, `Sales_Region__c`, `Lead_Scope__c`, `Account.Unit__c` (parent ref)                                                                                                                                                                                                                                                                                                                |
| **Win/Loss diagnostics**           | Closed-deal analysis                                                                                                                                                        | `Reason_Won_Lost__c`, `Lost_to_Competitor__c`                                                                                                                                                                                                                                                                                                                                                              |
| **Primary quote link**             | Apttus binding                                                                                                                                                              | `APTS_Primary_Quote__c` (reference), `APTS_Primary_Quote_Type__c`, `APTS_Primary_Quote_Pricing_Criteria__c`                                                                                                                                                                                                                                                                                                |
| **Push/slip tracking**             | Pipeline hygiene                                                                                                                                                            | `PushCount` (system-maintained)                                                                                                                                                                                                                                                                                                                                                                            |
| **Forecast/commit**                | Rollup to FinanceOps forecast page                                                                                                                                          | `APTS_Forecast_ARR__c`, `APTS_Forecast_ACV_AVG__c`, `APTS_Forecast_Renewal_ACV__c`, `APTS_Forecast_NPP_UWT__c`                                                                                                                                                                                                                                                                                             |
| **Classification**                 | Deal typing                                                                                                                                                                 | `Type` (Land/Expand/Renewal), `APTS_Opportunity_Sub_Type__c`                                                                                                                                                                                                                                                                                                                                               |

## The forecast layer

Standard Salesforce Forecasting objects are in play:

- `ForecastingType`, `ForecastingItem`, `ForecastingQuota`, `ForecastingAdjustment`, `ForecastingFact`
- `Period` (fiscal periods)
- `ForecastingItem` rolls up per `(OwnerId, PeriodId, ForecastingTypeId, ForecastCategoryName)` — **not** per-Opportunity. Our `APTS_Opportunity_ARR__c` / `APTS_Forecast_ARR__c` on Opportunity IS the per-deal commit signal.

The SimCorp forecast type of interest: `0Db7S000000zDaMSAU` — "Opportunity ARR".

## History / trending objects

- `OpportunityFieldHistory` — tracks field-level changes (CloseDate, StageName, ForecastCategoryName, Amount). We use this heavily for Q1 slip analysis.
- Historical Trending Reports — 18 reports maintained in the org for the Sales Director Monthly flow (9 directors × Q1+Q2). See `scripts/extract_historical_trending.py`.
- `Opty_Weekly_Snapshot__c` — **custom** weekly snapshot. Interesting — could be an alternative to HT reports. Needs investigation.
- `Opportunity_Snapshot__c`, `Universe_Snapshot__c` — more snapshot objects. Scope unclear without deeper probe.

## Activity objects

- `Task` (WhatId → Opportunity) — calls, emails, manual logs
- `Event` (WhatId → Opportunity) — meetings
- `EmailMessage` — actual email bodies
- `Opportunity.LastActivityDate` — SF-computed roll-up across Task+Event+EmailMessage

We use Task/Event `COUNT(Id) GROUP BY WhatId` for the new **Activity Volume** sheet; `MAX(ActivityDate)` is **not** supported in SOQL (gotcha), so we lean on `Opportunity.LastActivityDate` for "last touched" timing.

## Data model gotchas relevant to the SD Monthly ETL

1. **`Opportunity.PushCount` is a derived field that can't be used as a SOQL/REST filter** — even though you can read it. Affects report-builder automation. (Discovered 2026-04-16.)
2. **`ForecastingItem` has no `OpportunityId`** — it rolls up per (owner, period). For per-deal commit, use `Opportunity.ForecastCategoryName` + `APTS_Forecast_ARR__c` directly.
3. **`Period.StartDate` is a relationship-field filter that doesn't always validate** — if you filter `ForecastingItem` by period dates, use `PeriodId` with a subquery instead.
4. **Currency conversion must be explicit** via `convertCurrency()` in SOQL — otherwise ARR comes back in the deal's native currency. Aliased back to original field name so downstream parsing doesn't need to change.
5. **`Opportunity.Type` in reports is named `TYPE`** (not `Opportunity.Type`) — standard field macro.
6. **`FISCAL_QUARTER` is a first-class filter in reports** — use it rather than computing from CloseDate.
7. **`Account_Unit_Group__c` vs `Account.Unit__c`** — two separate fields. The former is org-level (SC Asia / SC EMEA / SC North America); the latter is on Account (SC USA / SC Canada). Territory routing uses both.

## Objects we've directly referenced in the ETL

- `Opportunity` — 6 SOQL queries in extract_director_live.py, plus Q1 movement
- `OpportunityFieldHistory` — Field-history extraction (now captures CloseDate + StageName + ForecastCategoryName)
- `Account` + `Account_Unit_Group__c` + `Sales_Region__c` + `Account.Industry` + `Account.Unit__c` + `Account.Region__c` — territory routing + reporting filters
- `Task`, `Event` — activity volume per deal
- `ForecastingItem` — forecast page reconciliation (rollup only; per-deal is via Opportunity)
- `Period` — fiscal-period lookup
- `User` (via `Owner.Name`) — owner identification
- Historical Trending reports × 18 — trending time series

## Objects we could exploit but don't yet

- **`Installed_Competitor_Product__c`** — could surface "accounts with competitor products" during whitespace/upsell analysis. Would feed Competitive Win/Loss tab with deeper context ("we're at accounts with X installed — win-back opportunities").
- **`Account_Engagement_Intelligence__c` / `Adoption_Score__c`** — account health signals. Could feed a "Health-at-Risk" section to pair with renewals.
- **`Customer_Reference__c`** — sales enablement. Not directly pipeline-relevant but worth knowing about.
- **`EBIT_Calculation__c` / `EBIT_Simulation__c`** — deal profitability. Could replace `APTS_Forecast_ARR__c` with margin-adjusted ARR for a "quality of pipeline" view.
- **`Opty_Weekly_Snapshot__c`** — if this is maintained, it could replace our HT-report snapshot pipeline with a pure SOQL query. **Worth probing** as a simpler alternative to Analytics Reports API.
- **Apttus quote-line-item detail** — could expose product-mix per deal (SBL vs MBL vs SaaS vs Services). Currently we aggregate at the opportunity level; the deal's product composition is one level deeper.

## What a "complete" exec surface would add

Using only objects already in the org:

1. **Product mix per director** — groupby Apttus line items. "Who's selling Axioma vs SBL vs DMS?"
2. **Installed-base whitespace** — `Account` has 40+ `_Client__c` boolean flags and `Installed_Competitor_Product__c`. Combine: "Axioma clients who don't yet have DMS = upsell queue."
3. **Account engagement correlation** — does Adoption_Score / C_H_Level_Form_Fillouts correlate with opportunity win rate? If yes, it's a leading indicator.
4. **EBIT/margin-weighted pipeline** — currently we report ARR; adding `APTS_Contract_Total_GP1perc__c` gives a quality-of-pipeline dimension.
5. **Quote cycle time** — `APTS_Primary_Quote__c` + Apttus proposal lifecycle gives time-from-quote-to-close as a separate metric from opp-level cycle time.

---

## Using this doc

- **New analyst onboarding**: read sections 1-3 (buckets, packages, core sales graph). That's the org in 10 minutes.
- **Adding a new KPI to the SD Monthly flow**: check "Objects we could exploit but don't yet" (§ above) — it's sized at the opportunities, not vague ideas.
- **Troubleshooting a report/SOQL error**: check "Data model gotchas" (§). Most field-filter rejections come from those 7 issues.
