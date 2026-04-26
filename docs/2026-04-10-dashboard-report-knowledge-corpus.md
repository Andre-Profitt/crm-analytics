# Knowledge Corpus — Salesforce Dashboards & Reports

Extracted 2026-04-10 from `apro@simcorp.com` via Analytics Reports API `/describe`.
This is the single source of truth for what data exists in the org.
Do NOT invent thresholds, metrics, or field names — everything here is as-configured.

---

## Dashboard 1: Sales Directors Monthly Pipeline and Insights

- **ID:** `01ZTb00000FSP7hMAH`
- **Name:** Sales Directors Monthly Pipeline and Insights
- **Components:** 8
- **Dashboard Filters:** 4
  - **Industry** on `?` — 7 options
    - `0ICTb0000007DbdOAE` = Asset Management
    - `0ICTb0000007DbeOAE` = Bank
    - `0ICTb0000007DbfOAE` = Insurance
    - `0ICTb0000007DbgOAE` = Pension
    - `0ICTb0000007DbhOAE` = Wealth Management
    - `0ICTb0000007DbiOAE` = Asset Servicer
    - `0ICTb0000007DbjOAE` = Other
  - **Legal Country** on `?` — 2 options
    - `0ICTb0000007DgTOAU` = CA
    - `0ICTb0000007DgUOAU` = CA
  - **Sales Region** on `?` — 7 options
    - `0ICTb0000007DbnOAE` = APAC
    - `0ICTb0000007DboOAE` = Central Europe
    - `0ICTb0000007DbpOAE` = Middle East & Africa
    - `0ICTb0000007DbqOAE` = North America
    - `0ICTb0000007DbrOAE` = Northern Europe
    - `0ICTb0000007DbsOAE` = Southwestern Europe
    - `0ICTb0000007DbtOAE` = United Kingdom & Ireland
  - **Account Unit Group** on `?` — 3 options
    - `0ICTb0000007Di5OAE` = SC North America
    - `0ICTb0000007Di6OAE` = SC Asia
    - `0ICTb0000007Di7OAE` = SC EMEA

### D1 Components

#### Pipeline Overview by Stage
- Component ID: `01aTb00000Cn9mPIAR`
- Report ID: `00OTb000008fBfdMAE`
- Report Name: Pipeline Global CFQ
- Format: SUMMARY

#### Commercial Approval Candidates by Stage
- Component ID: `01aTb00000Cn85jIAB`
- Report ID: `00OTb000008ekp7MAA`
- Report Name: Commercial Approval Candidates
- Format: SUMMARY

#### Commercial Approval Current State
- Component ID: `01aTb00000Cn9mQIAR`
- Report ID: `00OTb000008fBEDMA2`
- Report Name: Commercial Approval Global
- Format: SUMMARY

#### Renewal Likelihood by Probability
- Component ID: `01aTb00000Cn85bIAB`
- Report ID: `00OTb000008fBULMA2`
- Report Name: P2.7 Renewal Likelihood This Qtr
- Format: SUMMARY

#### Business At Risk
- Component ID: `01aTb00000Cn85dIAB`
- Report ID: `00OTb000008Ta9xMAC`
- Report Name: Business At Risk
- Format: SUMMARY

#### Renewal Pipeline This Quarter
- Component ID: `01aTb00000Cn85ZIAR`
- Report ID: `00OTb000008ektxMAA`
- Report Name: Renewal Pipeline This Quarter
- Format: TABULAR

#### Close Date Slipped by Stage
- Component ID: `01aTb00000Cn85lIAB`
- Report ID: `00OTb000008eknVMAQ`
- Report Name: Close Date Slipped CFQ Aging
- Format: TABULAR

#### Commercial Approval Approved YTD (Land)
- Component ID: `01aTb00000Cn85aIAB`
- Report ID: `00OTb000008aTtJMAU`
- Report Name: Commercial Approval approved 2026
- Format: SUMMARY

---

## Dashboard 2: Sales Ops Quarterly KPI Dashboard

- **ID:** `01ZTb00000FSP9JMAX`
- **Name:** Sales Ops Quarterly KPI Dashboard
- **Components:** 15
- **Dashboard Filters:** 0

### D2 Components

#### Low Probability In Quarter
- Component ID: `01aTb00000CmjwzIAB`
- Report ID: `00OTb000008RfKDMA0`
- Report Name: Low Probability In Quarter
- Format: SUMMARY

#### Stale Opportunities - CFQ
- Component ID: `01aTb00000CmjwvIAB`
- Report ID: `00OTb000008TZgvMAG`
- Report Name: Stale Opportunities - CFQ
- Format: SUMMARY

#### No Activity 30 Plus Days
- Component ID: `01aTb00000CmjwyIAB`
- Report ID: `00OTb000008TaEnMAK`
- Report Name: No Activity 30+ Days - Open Opps
- Format: SUMMARY

#### Active Opps: No Activity
- Component ID: `01aTb00000Cn9PrIAJ`
- Report ID: `00OTb000008fAmnMAE`
- Report Name: Active Opps: No Activity
- Format: SUMMARY

#### High Value Stale Deals
- Component ID: `01aTb00000Cmjx0IAB`
- Report ID: `00OTb000008Ti97MAC`
- Report Name: High Value Stale Deals
- Format: TABULAR

#### Accounts without KYC Approval
- Component ID: `01aTb00000CnRbFIAV`
- Report ID: `00OQA000004OLk92AG`
- Report Name: Accounts without KYC Approval
- Format: SUMMARY

#### Missing Quote Type
- Component ID: `01aTb00000CmjwrIAB`
- Report ID: `00OTb000008ekynMAA`
- Report Name: Missing Quote Type
- Format: SUMMARY

#### Aging Pipeline 365 Plus Days
- Component ID: `01aTb00000CmjwqIAB`
- Report ID: `00OTb000008Ti7VMAS`
- Report Name: Aging Pipeline 365 Plus Days
- Format: SUMMARY

#### Aging Pipeline 365 Plus Days
- Component ID: `01aTb00000CnRUnIAN`
- Report ID: `00OTb000008Ti7VMAS`
- Report Name: Aging Pipeline 365 Plus Days
- Format: SUMMARY

#### Overdue Opportunities
- Component ID: `01aTb00000CmjwoIAB`
- Report ID: `00OTb000008SrmLMAS`
- Report Name: Overdue Opportunities
- Format: SUMMARY

#### Land Deals Lacking Commercial Approval Flow
- Component ID: `01aTb00000Cn9PqIAJ`
- Report ID: `00OTb000008fAlBMAU`
- Report Name: Land: No Approval Flow
- Format: SUMMARY

#### Overdue Opportunities
- Component ID: `01aTb00000CmjwtIAB`
- Report ID: `00OTb000008SrmLMAS`
- Report Name: Overdue Opportunities
- Format: SUMMARY

#### Missing Amount on Open Opps
- Component ID: `01aTb00000CmjwwIAB`
- Report ID: `00OTb000008TZqcMAG`
- Report Name: Missing Amount
- Format: TABULAR

#### Mid-Stage Opps Lacking NextStep
- Component ID: `01aTb00000Cn9PpIAJ`
- Report ID: `00OTb000008fAjZMAU`
- Report Name: Mid-Stage: No NextStep
- Format: SUMMARY

#### Active Opps: No Activity
- Component ID: `01aTb00000CnRWPIA3`
- Report ID: `00OTb000008fAmnMAE`
- Report Name: Active Opps: No Activity
- Format: SUMMARY

---

## Report Detail — All 22 Source Reports

### Accounts without KYC Approval
- **Report ID:** `00OQA000004OLk92AG`
- **Format:** SUMMARY
- **Report Type:** KYC with Account

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `Account.Name` | Account Name | string |
| `Account.Account_Number__c` | Account Number | string |
| `Account.Owner.Name` | Account Owner: Full Name | string |
| `Account.KYC_Approval_Expiry_Date__c` | KYC Approval Expiry Date | date |
| `Account.DUNS_No__c` | DUNS No. | string |
| `Account.Unit_Group__c` | Unit Group | string |
| `Account.Unit__c` | Unit | picklist |
| `Account.Finance_Client__c` | Official Client | boolean |
| `Account.Axioma_External_Id__c` | Axioma External Id | string |
| `Account.Inactive__c` | Inactive | boolean |

**Groupings:**
- Row group: `Account.Type` (Asc)
- Row group: `Account.Heat_Map_Open_Opportunities__c` (Asc)
- Row group: `Account.KYC_Approval_Status__c` (Asc)

**Aggregates:**
- `s!Account.Finance_Client__c` — Sum of Official Client (int)
- `s!Account.Inactive__c` — Sum of Inactive (int)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `Account.Type` | equals | `Prospect` |
| 2 | `Account.KYC_Approval_Status__c` | notEqual | `Approved,On Hold` |
| 3 | `Account.Heat_Map_Open_Opportunities__c` | greaterOrEqual | `1` |
| 4 | `Account.Name` | notContain | `test,simcorp,delete` |
| 5 | `Account.Type` | equals | `Customer` |
| 6 | `Account.Inactive__c` | equals | `False` |

**Filter Logic:** `((1 AND 2 AND 3 AND 4) OR (5 AND 2 AND 4)) AND 6`

**Scope:** organization

---

### Low Probability In Quarter
- **Report ID:** `00OTb000008RfKDMA0`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `ACCOUNT_NAME` | Account Name | string |
| `STAGE_NAME` | Stage | picklist |
| `PROBABILITY` | Probability (%) | percent |
| `CLOSE_DATE` | Close Date | date |
| `Opportunity.APTS_Forecast_ARR__c.CONVERT` | Forecast ARR (converted) | currency |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |
| `STAGE_DURATION` | Stage Duration | double |
| `AGE` | Age | double |

**Groupings:**
- Row group: `Opportunity.Sales_Region__c` (Asc)
- Row group: `TYPE` (Asc)
- Row group: `FULL_NAME` (Asc)

**Aggregates:**
- `s!Opportunity.APTS_Forecast_ARR__c.CONVERT` — Sum of Forecast ARR (converted) (currency)
- `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` — Sum of Opportunity ARR (converted) (currency)
- `s!STAGE_DURATION` — Sum of Stage Duration (int)
- `s!AGE` — Sum of Age (int)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `PROBABILITY` | lessThan | `50` |

**Scope:** organization

---

### Overdue Opportunities
- **Report ID:** `00OTb000008SrmLMAS`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `STAGE_NAME` | Stage | picklist |
| `TYPE` | Type | picklist |
| `FULL_NAME` | Opportunity Owner | string |
| `CLOSE_DATE` | Close Date | date |
| `CDF1` | Overdue Days | double |
| `Account.Region__c` | Sales Region | picklist |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |
| `Opportunity.APTS_Forecast_ARR__c.CONVERT` | Forecast ARR (converted) | currency |
| `Opportunity.APTS_Forecast_ACV_AVG__c.CONVERT` | Forecast ACV (converted) | currency |
| `INDUSTRY` | Industry | picklist |
| `Account.Tier_Calculation__c` | Tier | string |
| `ADDRESS1_COUNTRY` | Legal Country (text only) | string |

**Groupings:**
- Row group: `Opportunity.Sales_Region__c` (Asc)
- Row group: `FISCAL_QUARTER` (Asc)

**Aggregates:**
- `s!CDF1` — Sum of Overdue Days (double)
- `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` — Sum of Opportunity ARR (converted) (currency)
- `s!Opportunity.APTS_Forecast_ARR__c.CONVERT` — Sum of Forecast ARR (converted) (currency)
- `s!Opportunity.APTS_Forecast_ACV_AVG__c.CONVERT` — Sum of Forecast ACV (converted) (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `ACCOUNT_NAME` | notContain | `simcorp,delete` |
| 2 | `TYPE` | equals | `,Land,Expand,Renewal` |
| 3 | `FULL_NAME` | notContain | `Sabiniewicz,Profit` |
| 4 | `STAGE_NAME` | notEqual | `,8 - Won,0 - Lost,0 - No Opportunity,Quota` |
| 5 | `CLOSE_DATE` | lessThan | `TODAY` |
| 6 | `OPPORTUNITY_NAME` | notContain | `test,simcorp,delete` |

**Scope:** organization

---

### Stale Opportunities - CFQ
- **Report ID:** `00OTb000008TZgvMAG`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `Opportunity.Opportunity_Average_ACV__c` | Opportunity ACV | currency |
| `CREATED_DATE` | Created Date | date |
| `CLOSE_DATE` | Close Date | date |
| `AGE` | Age | double |
| `STAGE_NAME` | Stage | picklist |

**Groupings:**
- Row group: `FULL_NAME` (Asc)

**Aggregates:**
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `ACCOUNT_NAME` | notContain | `test,SC,Simcorp` |
| 3 | `LAST_ACTIVITY` | lessThan | `LAST 30 DAYS` |

**Scope:** organization

---

### Missing Amount
- **Report ID:** `00OTb000008TZqcMAG`
- **Format:** TABULAR
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `Opportunity.Opportunity_Average_ACV__c` | Opportunity ACV | currency |
| `AMOUNT` | Amount | currency |

**Aggregates:**
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `Opportunity.APTS_Opportunity_ARR__c` | equals | `EUR 0` |

**Scope:** organization

---

### Business At Risk
- **Report ID:** `00OTb000008Ta9xMAC`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `Opportunity.Opportunity_Average_ACV__c` | Opportunity ACV | currency |
| `Account.Termination_Risk_Last_Updated__c` | Termination Risk Last Updated | date |

**Groupings:**
- Row group: `Account.Risk_of_Potential_Termination__c` (Asc)
- Row group: `Account.Tier_Calculation__c` (Asc)

**Aggregates:**
- `s!Opportunity.Opportunity_Average_ACV__c` — Sum of Opportunity ACV (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |

**Scope:** organization

---

### No Activity 30+ Days - Open Opps
- **Report ID:** `00OTb000008TaEnMAK`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `LAST_ACTIVITY` | Last Activity | date |
| `AGE` | Age | double |
| `AMOUNT` | Amount | currency |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |
| `Opportunity.APTS_Opportunity_ARR__c` | Opportunity ARR | currency |

**Groupings:**
- Row group: `FULL_NAME` (Asc)

**Aggregates:**
- `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` — Sum of Opportunity ARR (converted) (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `LAST_ACTIVITY` | lessThan | `LAST 30 DAYS` |

**Scope:** organization

---

### Aging Pipeline 365 Plus Days
- **Report ID:** `00OTb000008Ti7VMAS`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `AMOUNT` | Amount | currency |
| `CLOSE_DATE` | Close Date | date |
| `AGE` | Age | double |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |

**Groupings:**
- Row group: `STAGE_NAME` (Asc)

**Aggregates:**
- `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` — Sum of Opportunity ARR (converted) (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `AGE` | greaterThan | `365` |

**Scope:** organization

---

### High Value Stale Deals
- **Report ID:** `00OTb000008Ti97MAC`
- **Format:** TABULAR
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `Opportunity.APTS_Forecast_ARR__c.CONVERT` | Forecast ARR (converted) | currency |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |
| `LAST_ACTIVITY` | Last Activity | date |
| `CLOSE_DATE` | Close Date | date |
| `STAGE_NAME` | Stage | picklist |
| `AGE` | Age | double |

**Aggregates:**
- `s!Opportunity.APTS_Forecast_ARR__c.CONVERT` — Sum of Forecast ARR (converted) (currency)
- `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` — Sum of Opportunity ARR (converted) (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `LAST_ACTIVITY` | lessThan | `LAST 60 DAYS` |
| 3 | `Opportunity.APTS_Forecast_ARR__c` | greaterOrEqual | `EUR 1.000.000` |

**Scope:** organization

---

### Commercial Approval approved 2026
- **Report ID:** `00OTb000008aTtJMAU`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `Opportunity.Stage_20_Approval_Date__c` | Commercial Approval Date | date |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `CREATED` | Created By | string |
| `CREATED_DATE` | Created Date | date |
| `Opportunity.Stage_20_Approval__c` | Commercial Approval | boolean |
| `CLOSE_DATE` | Close Date | date |
| `STAGE_NAME` | Stage | picklist |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |
| `Opportunity.APTS_Forecast_ARR__c.CONVERT` | Forecast ARR (converted) | currency |
| `AGE` | Age | double |
| `FULL_NAME` | Opportunity Owner | string |
| `Opportunity.Lead_Scope__c` | Opportunity Scope | multipicklist |
| `Opportunity.APTS_RH_Product_Family__c` | Product Family | multipicklist |
| `ACCOUNT_NAME` | Account Name | string |
| `INDUSTRY` | Industry | picklist |
| `FISCAL_QUARTER` | Fiscal Period | string |
| `Opportunity.New_Stage_15_Date__c` | New Stage 2 Date | date |
| `Opportunity.New_Stage_20_Date__c` | New Stage 3 Date | date |
| `Account.Region__c` | Sales Region | picklist |
| `Opportunity.APTS_Opportunity_Sub_Type__c` | Opportunity Sub-Type | picklist |

**Groupings:**
- Row group: `Opportunity.Sales_Region__c` (Asc)

**Aggregates:**
- `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` — Sum of Opportunity ARR (converted) (currency)
- `s!Opportunity.APTS_Forecast_ARR__c.CONVERT` — Sum of Forecast ARR (converted) (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `TYPE` | equals | `,Land` |
| 2 | `Opportunity.Stage_20_Approval_Date__c` | greaterThan | `2026-01-01` |
| 3 | `Opportunity.Stage_20_Approval__c` | equals | `True` |

**Scope:** organization

---

### Land Stage 3 Missing Approval
- **Report ID:** `00OTb000008ekltMAA`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `CLOSE_DATE` | Close Date | date |
| `NEXT_STEP` | Next Step | string |
| `Opportunity.APTS_Forecast_ARR__c.CONVERT` | Forecast ARR (converted) | currency |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |

**Groupings:**
- Row group: `ROLLUP_DESCRIPTION` (Desc)

**Aggregates:**
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `STAGE_NAME` | equals | `3 - Engagement` |
| 2 | `TYPE` | equals | `Land` |
| 3 | `Opportunity.Stage_20_Approval__c` | equals | `False` |
| 4 | `OPPORTUNITY_NAME` | notContain | `Channel` |

**Scope:** organization

---

### Close Date Slipped CFQ Aging
- **Report ID:** `00OTb000008eknVMAQ`
- **Format:** TABULAR
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `AGE` | Age | double |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |

**Aggregates:**
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `AGE` | greaterThan | `90` |

**Scope:** organization

---

### Commercial Approval Candidates
- **Report ID:** `00OTb000008ekp7MAA`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `CLOSE_DATE` | Close Date | date |
| `NEXT_STEP` | Next Step | string |
| `Opportunity.APTS_Forecast_ARR__c.CONVERT` | Forecast ARR (converted) | currency |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |

**Groupings:**
- Row group: `Opportunity.Sales_Region__c` (Asc)
- Row group: `STAGE_NAME` (Desc)

**Aggregates:**
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `TYPE` | equals | `Land` |
| 2 | `STAGE_NAME` | equals | `3 - Engagement` |
| 3 | `Opportunity.Stage_20_Approval__c` | equals | `False` |

**Scope:** organization

---

### Renewal Pipeline This Quarter
- **Report ID:** `00OTb000008ektxMAA`
- **Format:** TABULAR
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `Opportunity.APTS_Renewal_ACV__c.CONVERT` | Renewal ACV (converted) | currency |

**Aggregates:**
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `TYPE` | equals | `Renewal` |
| 2 | `CLOSED` | equals | `False` |

**Scope:** organization

---

### Missing Quote Type
- **Report ID:** `00OTb000008ekynMAA`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `Opportunity.APTS_Primary_Quote_Type__c` | Primary Quote Type | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `AMOUNT` | Amount | currency |

**Groupings:**
- Row group: `FULL_NAME` (Asc)

**Aggregates:**
- `s!AMOUNT` — Sum of Amount (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `Opportunity.APTS_Primary_Quote_Type__c` | equals | `` |
| 3 | `STAGE_NAME` | equals | `4 - Shortlisted,5 - Preferred,6 - Contracting` |

**Scope:** organization

---

### Mid-Stage: No NextStep
- **Report ID:** `00OTb000008fAjZMAU`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `FULL_NAME` | Opportunity Owner | string |
| `NEXT_STEP` | Next Step | string |

**Groupings:**
- Row group: `Opportunity.Sales_Region__c` (Asc)

**Aggregates:**
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `STAGE_NAME` | equals | `3 - Engagement,4 - Shortlisted,5 - Preferred,6 - Contracting` |
| 3 | `ACCOUNT_NAME` | notContain | `Simcorp,SC,Test` |
| 4 | `NEXT_STEP` | equals | `` |

**Scope:** organization

---

### Land: No Approval Flow
- **Report ID:** `00OTb000008fAlBMAU`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `FULL_NAME` | Opportunity Owner | string |

**Groupings:**
- Row group: `Opportunity.Sales_Region__c` (Asc)

**Aggregates:**
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `TYPE` | equals | `Land` |
| 2 | `CLOSED` | equals | `False` |
| 3 | `STAGE_NAME` | equals | `4 - Shortlisted,5 - Preferred,6 - Contracting` |
| 4 | `Opportunity.Stage_20_Approval__c` | equals | `False` |
| 5 | `Opportunity.Submit_for_Stage_20_Review__c` | equals | `False` |

**Scope:** organization

---

### Active Opps: No Activity
- **Report ID:** `00OTb000008fAmnMAE`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `FULL_NAME` | Opportunity Owner | string |
| `ACCOUNT_LAST_ACTIVITY` | Account: Last Activity | date |
| `LAST_ACTIVITY` | Last Activity | date |
| `Opportunity.Consensus__cDaysSinceLastActivity__c` | Days Since Last Activity | double |

**Groupings:**
- Row group: `Opportunity.Sales_Region__c` (Asc)

**Aggregates:**
- `s!Opportunity.Consensus__cDaysSinceLastActivity__c` — Sum of Days Since Last Activity (double)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `STAGE_NAME` | equals | `1 - Prospecting,2 - Discovery,3 - Engagement,4 - Shortlisted,5 - Preferred,6 - Contracting` |
| 3 | `LAST_ACTIVITY` | equals | `` |
| 4 | `ACCOUNT_NAME` | notContain | `SC,Simcorp,Test` |

**Scope:** organization

---

### Commercial Approval Global
- **Report ID:** `00OTb000008fBEDMA2`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |

**Groupings:**
- Row group: `Opportunity.Stage_20_Approval__c` (Asc)

**Aggregates:**
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `STAGE_NAME` | notEqual | `,1 - Prospecting,2 - Discovery,8 - Won,0 - Lost,0 - No Opportunity,Quota` |

**Scope:** organization

---

### P2.7 Renewal Likelihood This Qtr
- **Report ID:** `00OTb000008fBULMA2`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `STAGE_NAME` | Stage | picklist |
| `CLOSE_DATE` | Close Date | date |
| `FULL_NAME` | Opportunity Owner | string |
| `Opportunity.APTS_Renewal_ACV__c.CONVERT` | Renewal ACV (converted) | currency |

**Groupings:**
- Row group: `PROBABILITY` (Asc)

**Aggregates:**
- `s!Opportunity.APTS_Renewal_ACV__c.CONVERT` — Sum of Renewal ACV (converted) (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |
| 2 | `TYPE` | equals | `Renewal` |

**Scope:** organization

---

### Pipeline Global CFQ
- **Report ID:** `00OTb000008fBfdMAE`
- **Format:** SUMMARY
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `FULL_NAME` | Opportunity Owner | string |
| `CLOSE_DATE` | Close Date | date |
| `NEXT_STEP` | Next Step | string |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |

**Groupings:**
- Row group: `STAGE_NAME` (Asc)
- Row group: `Opportunity.Sales_Region__c` (Asc)

**Aggregates:**
- `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` — Sum of Opportunity ARR (converted) (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |

**Scope:** organization

---

### Commercial Approval 2x2 Matrix
- **Report ID:** `00OTb000008fQ6nMAE`
- **Format:** MATRIX
- **Report Type:** Opportunities

**Detail Columns:**

| API Name | Label | Type |
|---|---|---|
| `ACCOUNT_NAME` | Account Name | string |
| `OPPORTUNITY_NAME` | Opportunity Name | string |
| `CLOSE_DATE` | Close Date | date |
| `Opportunity.Sales_Region__c` | Sales Region | string |
| `Opportunity.APTS_Opportunity_ARR__c.CONVERT` | Opportunity ARR (converted) | currency |

**Groupings:**
- Row group: `STAGE_NAME` (Asc)
- Column group: `Opportunity.Stage_20_Approval__c` (Asc)

**Aggregates:**
- `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` — Sum of Opportunity ARR (converted) (currency)
- `RowCount` — Record Count (int)

**Report Filters (as-configured):**

| # | Column | Operator | Value |
|---|---|---|---|
| 1 | `CLOSED` | equals | `False` |

**Scope:** organization

---

## Appendix: Complete Field Inventory Per Report

### Accounts without KYC Approval (`00OQA000004OLk92AG`)
Format: SUMMARY

**Filters:**
  1. `Account.Type` equals `Prospect`
  2. `Account.KYC_Approval_Status__c` notEqual `Approved,On Hold`
  3. `Account.Heat_Map_Open_Opportunities__c` greaterOrEqual `1`
  4. `Account.Name` notContain `test,simcorp,delete`
  5. `Account.Type` equals `Customer`
  6. `Account.Inactive__c` equals `False`
  Logic: `((1 AND 2 AND 3 AND 4) OR (5 AND 2 AND 4)) AND 6`

Row group: `Account.Type` sort=Asc
Row group: `Account.Heat_Map_Open_Opportunities__c` sort=Asc
Row group: `Account.KYC_Approval_Status__c` sort=Asc

**Columns:**
  - `Account.Name` → Account Name (string)
  - `Account.Account_Number__c` → Account Number (string)
  - `Account.Owner.Name` → Account Owner: Full Name (string)
  - `Account.KYC_Approval_Expiry_Date__c` → KYC Approval Expiry Date (date)
  - `Account.DUNS_No__c` → DUNS No. (string)
  - `Account.Unit_Group__c` → Unit Group (string)
  - `Account.Unit__c` → Unit (picklist)
  - `Account.Finance_Client__c` → Official Client (boolean)
  - `Account.Axioma_External_Id__c` → Axioma External Id (string)
  - `Account.Inactive__c` → Inactive (boolean)

**Aggregates:**
  - `s!Account.Finance_Client__c` → Sum of Official Client
  - `s!Account.Inactive__c` → Sum of Inactive
  - `RowCount` → Record Count

---

### Low Probability In Quarter (`00OTb000008RfKDMA0`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`
  2. `PROBABILITY` lessThan `50`

Row group: `Opportunity.Sales_Region__c` sort=Asc
Row group: `TYPE` sort=Asc
Row group: `FULL_NAME` sort=Asc

**Columns:**
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `ACCOUNT_NAME` → Account Name (string)
  - `STAGE_NAME` → Stage (picklist)
  - `PROBABILITY` → Probability (%) (percent)
  - `CLOSE_DATE` → Close Date (date)
  - `Opportunity.APTS_Forecast_ARR__c.CONVERT` → Forecast ARR (converted) (currency)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)
  - `STAGE_DURATION` → Stage Duration (double)
  - `AGE` → Age (double)

**Aggregates:**
  - `s!Opportunity.APTS_Forecast_ARR__c.CONVERT` → Sum of Forecast ARR (converted)
  - `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Sum of Opportunity ARR (converted)
  - `s!STAGE_DURATION` → Sum of Stage Duration
  - `s!AGE` → Sum of Age
  - `RowCount` → Record Count

---

### Overdue Opportunities (`00OTb000008SrmLMAS`)
Format: SUMMARY

**Filters:**
  1. `ACCOUNT_NAME` notContain `simcorp,delete`
  2. `TYPE` equals `,Land,Expand,Renewal`
  3. `FULL_NAME` notContain `Sabiniewicz,Profit`
  4. `STAGE_NAME` notEqual `,8 - Won,0 - Lost,0 - No Opportunity,Quota`
  5. `CLOSE_DATE` lessThan `TODAY`
  6. `OPPORTUNITY_NAME` notContain `test,simcorp,delete`

Row group: `Opportunity.Sales_Region__c` sort=Asc
Row group: `FISCAL_QUARTER` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `STAGE_NAME` → Stage (picklist)
  - `TYPE` → Type (picklist)
  - `FULL_NAME` → Opportunity Owner (string)
  - `CLOSE_DATE` → Close Date (date)
  - `CDF1` → Overdue Days (double)
  - `Account.Region__c` → Sales Region (picklist)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)
  - `Opportunity.APTS_Forecast_ARR__c.CONVERT` → Forecast ARR (converted) (currency)
  - `Opportunity.APTS_Forecast_ACV_AVG__c.CONVERT` → Forecast ACV (converted) (currency)
  - `INDUSTRY` → Industry (picklist)
  - `Account.Tier_Calculation__c` → Tier (string)
  - `ADDRESS1_COUNTRY` → Legal Country (text only) (string)

**Aggregates:**
  - `s!CDF1` → Sum of Overdue Days
  - `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Sum of Opportunity ARR (converted)
  - `s!Opportunity.APTS_Forecast_ARR__c.CONVERT` → Sum of Forecast ARR (converted)
  - `s!Opportunity.APTS_Forecast_ACV_AVG__c.CONVERT` → Sum of Forecast ACV (converted)
  - `RowCount` → Record Count

---

### Stale Opportunities - CFQ (`00OTb000008TZgvMAG`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`
  2. `ACCOUNT_NAME` notContain `test,SC,Simcorp`
  3. `LAST_ACTIVITY` lessThan `LAST 30 DAYS`

Row group: `FULL_NAME` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `Opportunity.Opportunity_Average_ACV__c` → Opportunity ACV (currency)
  - `CREATED_DATE` → Created Date (date)
  - `CLOSE_DATE` → Close Date (date)
  - `AGE` → Age (double)
  - `STAGE_NAME` → Stage (picklist)

**Aggregates:**
  - `RowCount` → Record Count

---

### Missing Amount (`00OTb000008TZqcMAG`)
Format: TABULAR

**Filters:**
  1. `CLOSED` equals `False`
  2. `Opportunity.APTS_Opportunity_ARR__c` equals `EUR 0`


**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `Opportunity.Opportunity_Average_ACV__c` → Opportunity ACV (currency)
  - `AMOUNT` → Amount (currency)

**Aggregates:**
  - `RowCount` → Record Count

---

### Business At Risk (`00OTb000008Ta9xMAC`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`

Row group: `Account.Risk_of_Potential_Termination__c` sort=Asc
Row group: `Account.Tier_Calculation__c` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `Opportunity.Opportunity_Average_ACV__c` → Opportunity ACV (currency)
  - `Account.Termination_Risk_Last_Updated__c` → Termination Risk Last Updated (date)

**Aggregates:**
  - `s!Opportunity.Opportunity_Average_ACV__c` → Sum of Opportunity ACV
  - `RowCount` → Record Count

---

### No Activity 30+ Days - Open Opps (`00OTb000008TaEnMAK`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`
  2. `LAST_ACTIVITY` lessThan `LAST 30 DAYS`

Row group: `FULL_NAME` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `LAST_ACTIVITY` → Last Activity (date)
  - `AGE` → Age (double)
  - `AMOUNT` → Amount (currency)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)
  - `Opportunity.APTS_Opportunity_ARR__c` → Opportunity ARR (currency)

**Aggregates:**
  - `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Sum of Opportunity ARR (converted)
  - `RowCount` → Record Count

---

### Aging Pipeline 365 Plus Days (`00OTb000008Ti7VMAS`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`
  2. `AGE` greaterThan `365`

Row group: `STAGE_NAME` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `AMOUNT` → Amount (currency)
  - `CLOSE_DATE` → Close Date (date)
  - `AGE` → Age (double)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)

**Aggregates:**
  - `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Sum of Opportunity ARR (converted)
  - `RowCount` → Record Count

---

### High Value Stale Deals (`00OTb000008Ti97MAC`)
Format: TABULAR

**Filters:**
  1. `CLOSED` equals `False`
  2. `LAST_ACTIVITY` lessThan `LAST 60 DAYS`
  3. `Opportunity.APTS_Forecast_ARR__c` greaterOrEqual `EUR 1.000.000`


**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `Opportunity.APTS_Forecast_ARR__c.CONVERT` → Forecast ARR (converted) (currency)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)
  - `LAST_ACTIVITY` → Last Activity (date)
  - `CLOSE_DATE` → Close Date (date)
  - `STAGE_NAME` → Stage (picklist)
  - `AGE` → Age (double)

**Aggregates:**
  - `s!Opportunity.APTS_Forecast_ARR__c.CONVERT` → Sum of Forecast ARR (converted)
  - `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Sum of Opportunity ARR (converted)
  - `RowCount` → Record Count

---

### Commercial Approval approved 2026 (`00OTb000008aTtJMAU`)
Format: SUMMARY

**Filters:**
  1. `TYPE` equals `,Land`
  2. `Opportunity.Stage_20_Approval_Date__c` greaterThan `2026-01-01`
  3. `Opportunity.Stage_20_Approval__c` equals `True`

Row group: `Opportunity.Sales_Region__c` sort=Asc

**Columns:**
  - `Opportunity.Stage_20_Approval_Date__c` → Commercial Approval Date (date)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `CREATED` → Created By (string)
  - `CREATED_DATE` → Created Date (date)
  - `Opportunity.Stage_20_Approval__c` → Commercial Approval (boolean)
  - `CLOSE_DATE` → Close Date (date)
  - `STAGE_NAME` → Stage (picklist)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)
  - `Opportunity.APTS_Forecast_ARR__c.CONVERT` → Forecast ARR (converted) (currency)
  - `AGE` → Age (double)
  - `FULL_NAME` → Opportunity Owner (string)
  - `Opportunity.Lead_Scope__c` → Opportunity Scope (multipicklist)
  - `Opportunity.APTS_RH_Product_Family__c` → Product Family (multipicklist)
  - `ACCOUNT_NAME` → Account Name (string)
  - `INDUSTRY` → Industry (picklist)
  - `FISCAL_QUARTER` → Fiscal Period (string)
  - `Opportunity.New_Stage_15_Date__c` → New Stage 2 Date (date)
  - `Opportunity.New_Stage_20_Date__c` → New Stage 3 Date (date)
  - `Account.Region__c` → Sales Region (picklist)
  - `Opportunity.APTS_Opportunity_Sub_Type__c` → Opportunity Sub-Type (picklist)

**Aggregates:**
  - `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Sum of Opportunity ARR (converted)
  - `s!Opportunity.APTS_Forecast_ARR__c.CONVERT` → Sum of Forecast ARR (converted)
  - `RowCount` → Record Count

---

### Land Stage 3 Missing Approval (`00OTb000008ekltMAA`)
Format: SUMMARY

**Filters:**
  1. `STAGE_NAME` equals `3 - Engagement`
  2. `TYPE` equals `Land`
  3. `Opportunity.Stage_20_Approval__c` equals `False`
  4. `OPPORTUNITY_NAME` notContain `Channel`

Row group: `ROLLUP_DESCRIPTION` sort=Desc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `CLOSE_DATE` → Close Date (date)
  - `NEXT_STEP` → Next Step (string)
  - `Opportunity.APTS_Forecast_ARR__c.CONVERT` → Forecast ARR (converted) (currency)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)

**Aggregates:**
  - `RowCount` → Record Count

---

### Close Date Slipped CFQ Aging (`00OTb000008eknVMAQ`)
Format: TABULAR

**Filters:**
  1. `CLOSED` equals `False`
  2. `AGE` greaterThan `90`


**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `AGE` → Age (double)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)

**Aggregates:**
  - `RowCount` → Record Count

---

### Commercial Approval Candidates (`00OTb000008ekp7MAA`)
Format: SUMMARY

**Filters:**
  1. `TYPE` equals `Land`
  2. `STAGE_NAME` equals `3 - Engagement`
  3. `Opportunity.Stage_20_Approval__c` equals `False`

Row group: `Opportunity.Sales_Region__c` sort=Asc
Row group: `STAGE_NAME` sort=Desc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `CLOSE_DATE` → Close Date (date)
  - `NEXT_STEP` → Next Step (string)
  - `Opportunity.APTS_Forecast_ARR__c.CONVERT` → Forecast ARR (converted) (currency)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)

**Aggregates:**
  - `RowCount` → Record Count

---

### Renewal Pipeline This Quarter (`00OTb000008ektxMAA`)
Format: TABULAR

**Filters:**
  1. `TYPE` equals `Renewal`
  2. `CLOSED` equals `False`


**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `Opportunity.APTS_Renewal_ACV__c.CONVERT` → Renewal ACV (converted) (currency)

**Aggregates:**
  - `RowCount` → Record Count

---

### Missing Quote Type (`00OTb000008ekynMAA`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`
  2. `Opportunity.APTS_Primary_Quote_Type__c` equals ``
  3. `STAGE_NAME` equals `4 - Shortlisted,5 - Preferred,6 - Contracting`

Row group: `FULL_NAME` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `Opportunity.APTS_Primary_Quote_Type__c` → Primary Quote Type (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `AMOUNT` → Amount (currency)

**Aggregates:**
  - `s!AMOUNT` → Sum of Amount
  - `RowCount` → Record Count

---

### Mid-Stage: No NextStep (`00OTb000008fAjZMAU`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`
  2. `STAGE_NAME` equals `3 - Engagement,4 - Shortlisted,5 - Preferred,6 - Contracting`
  3. `ACCOUNT_NAME` notContain `Simcorp,SC,Test`
  4. `NEXT_STEP` equals ``

Row group: `Opportunity.Sales_Region__c` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `FULL_NAME` → Opportunity Owner (string)
  - `NEXT_STEP` → Next Step (string)

**Aggregates:**
  - `RowCount` → Record Count

---

### Land: No Approval Flow (`00OTb000008fAlBMAU`)
Format: SUMMARY

**Filters:**
  1. `TYPE` equals `Land`
  2. `CLOSED` equals `False`
  3. `STAGE_NAME` equals `4 - Shortlisted,5 - Preferred,6 - Contracting`
  4. `Opportunity.Stage_20_Approval__c` equals `False`
  5. `Opportunity.Submit_for_Stage_20_Review__c` equals `False`

Row group: `Opportunity.Sales_Region__c` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `FULL_NAME` → Opportunity Owner (string)

**Aggregates:**
  - `RowCount` → Record Count

---

### Active Opps: No Activity (`00OTb000008fAmnMAE`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`
  2. `STAGE_NAME` equals `1 - Prospecting,2 - Discovery,3 - Engagement,4 - Shortlisted,5 - Preferred,6 - Contracting`
  3. `LAST_ACTIVITY` equals ``
  4. `ACCOUNT_NAME` notContain `SC,Simcorp,Test`

Row group: `Opportunity.Sales_Region__c` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `FULL_NAME` → Opportunity Owner (string)
  - `ACCOUNT_LAST_ACTIVITY` → Account: Last Activity (date)
  - `LAST_ACTIVITY` → Last Activity (date)
  - `Opportunity.Consensus__cDaysSinceLastActivity__c` → Days Since Last Activity (double)

**Aggregates:**
  - `s!Opportunity.Consensus__cDaysSinceLastActivity__c` → Sum of Days Since Last Activity
  - `RowCount` → Record Count

---

### Commercial Approval Global (`00OTb000008fBEDMA2`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`
  2. `STAGE_NAME` notEqual `,1 - Prospecting,2 - Discovery,8 - Won,0 - Lost,0 - No Opportunity,Quota`

Row group: `Opportunity.Stage_20_Approval__c` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)

**Aggregates:**
  - `RowCount` → Record Count

---

### P2.7 Renewal Likelihood This Qtr (`00OTb000008fBULMA2`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`
  2. `TYPE` equals `Renewal`

Row group: `PROBABILITY` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `STAGE_NAME` → Stage (picklist)
  - `CLOSE_DATE` → Close Date (date)
  - `FULL_NAME` → Opportunity Owner (string)
  - `Opportunity.APTS_Renewal_ACV__c.CONVERT` → Renewal ACV (converted) (currency)

**Aggregates:**
  - `s!Opportunity.APTS_Renewal_ACV__c.CONVERT` → Sum of Renewal ACV (converted)
  - `RowCount` → Record Count

---

### Pipeline Global CFQ (`00OTb000008fBfdMAE`)
Format: SUMMARY

**Filters:**
  1. `CLOSED` equals `False`

Row group: `STAGE_NAME` sort=Asc
Row group: `Opportunity.Sales_Region__c` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `FULL_NAME` → Opportunity Owner (string)
  - `CLOSE_DATE` → Close Date (date)
  - `NEXT_STEP` → Next Step (string)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)

**Aggregates:**
  - `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Sum of Opportunity ARR (converted)
  - `RowCount` → Record Count

---

### Commercial Approval 2x2 Matrix (`00OTb000008fQ6nMAE`)
Format: MATRIX

**Filters:**
  1. `CLOSED` equals `False`

Row group: `STAGE_NAME` sort=Asc
Col group: `Opportunity.Stage_20_Approval__c` sort=Asc

**Columns:**
  - `ACCOUNT_NAME` → Account Name (string)
  - `OPPORTUNITY_NAME` → Opportunity Name (string)
  - `CLOSE_DATE` → Close Date (date)
  - `Opportunity.Sales_Region__c` → Sales Region (string)
  - `Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Opportunity ARR (converted) (currency)

**Aggregates:**
  - `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` → Sum of Opportunity ARR (converted)
  - `RowCount` → Record Count

---

---

## Pipeline Inspection Views

All cloned from Global ARR CFQ Forecast seed (`00BTb00000Ic82DMAR`).

| View | ListViewId | Filter Field | Filter Value |
|---|---|---|---|
| PI ARR Forecast CE | `00BTb00000Kr3YvMAJ` | Account_Unit_Group + Sales_Region | SC EMEA + Central Europe |
| PI ARR Forecast SWE | `00BTb00000Kr3sHMAR` | Sales_Director_Book__c | Southern Europe |
| PI ARR Forecast UKI | `00BTb00000Kr3yjMAB` | Sales_Director_Book__c | UK & Ireland |
| PI ARR Forecast NE | `00BTb00000Kr4DFMAZ` | Sales_Director_Book__c | NL & Nordics |
| PI ARR Forecast Canada | `00BTb00000Kr4ErMAJ` | Sales_Director_Book__c | Canada |
| PI ARR Forecast NA AM | `00BTb00000Kr4JhMAJ` | Sales_Director_Book__c | NA Asset Management |
| PI ARR Forecast P&I | `00BTb00000Kr4OXMAZ` | Sales_Director_Book__c | Pension & Insurance |

Pre-existing seeds:

| View | ListViewId |
|---|---|
| Global ARR CFQ Forecast | `00BTb00000Ic82DMAR` |
| APAC ARR CFQ Forecast | `00BTb00000Ic7kTMAR` |
| EMEA ARR CFQ Forecast | `00BTb00000Ic77lMAB` |
| NA ARR CFQ Forecast | `00BTb00000Ic6JlMAJ` |

---

## D1 Dashboard Filter Options (exact IDs)

### Filter 1: Industry (7 options)
| ID | Value |
|---|---|
| `0ICTb0000007DbdOAE` | Asset Management |
| `0ICTb0000007DbeOAE` | Bank |
| `0ICTb0000007DbfOAE` | Insurance |
| `0ICTb0000007DbgOAE` | Pension |
| `0ICTb0000007DbhOAE` | Wealth Management |
| `0ICTb0000007DbiOAE` | Asset Servicer |
| `0ICTb0000007DbjOAE` | Other |

### Filter 2: Legal Country (2 options)
| ID | Value |
|---|---|
| `0ICTb0000007DgTOAU` | CA (include) |
| `0ICTb0000007DgUOAU` | CA (exclude) |

### Filter 3: Sales Region (7 options)
| ID | Value |
|---|---|
| `0ICTb0000007DbnOAE` | APAC |
| `0ICTb0000007DboOAE` | Central Europe |
| `0ICTb0000007DbpOAE` | Middle East & Africa |
| `0ICTb0000007DbqOAE` | North America |
| `0ICTb0000007DbrOAE` | Northern Europe |
| `0ICTb0000007DbsOAE` | Southwestern Europe |
| `0ICTb0000007DbtOAE` | United Kingdom & Ireland |

### Filter 4: Account Unit Group (3 options)
| ID | Value |
|---|---|
| `0ICTb0000007Di5OAE` | SC North America |
| `0ICTb0000007Di6OAE` | SC Asia |
| `0ICTb0000007Di7OAE` | SC EMEA |

---

## Key Thresholds (as-configured in reports)

These are the exact filter thresholds configured in the org. Do not modify or assume different values.

| Metric | Report | Threshold | Notes |
|---|---|---|---|
| General staleness | Stale Opportunities - CFQ | LAST_ACTIVITY < LAST 30 DAYS | Excludes test/SC/Simcorp accounts |
| High-value staleness | High Value Stale Deals | LAST_ACTIVITY < LAST 60 DAYS AND Forecast ARR >= EUR 1M | 60-day window, EUR 1M+ only |
| Never-contacted | Active Opps: No Activity | LAST_ACTIVITY = (empty) | Stages 1-6 only, excludes SC/Simcorp/Test accounts |
| Pipeline aging | Aging Pipeline 365 Plus Days | AGE > 365 days | Open deals only |
| Overdue close | Overdue Opportunities | CLOSE_DATE < TODAY | Excludes won/lost/no-opp/quota stages |
| Low probability | Low Probability In Quarter | PROBABILITY < 50% | Open deals only |
| Missing approval | Land Stage 3 Missing Approval | Stage = 3 AND Commercial Approval = false | Land type only |
| Missing approval flow | Land: No Approval Flow | Approval Status != Approved/No Approval Necessary | Land type, stages 3-6 |


---

## SimCorp Sales Process (from Sales Handbook V4)

### Sales Stages

| Stage | Name | Description | Key Exit Gate |
|---|---|---|---|
| 1 | Prospecting | BDRs working leads with highest engagement scores. Prospect has listened. | Interest assessed, current systems identified, need identified |
| 2 | Discovery | Prospect is active, meetings happening. Price guidance given, scoping project. | Compelling event identified, PAIC done, on long list, technical validation |
| 3 | Engagement | SM driving forward. Due diligence, RFP, DD continues. Decision makers identified. | On short-list, quote created, competitive position established. **Commercial Approval gate (Go/No-Go)** |
| 4 | Shortlisted | Close plan created/validated with prospect. Scope finalized for commercial negotiation. Still in competition. | Written specs, scope agreed, preferred vendor status |
| 5 | Preferred | Named preferred vendor. No longer in competition. Receive full redlining. | Full redlining received |
| 6 | Contracting | Finalize legal review and price. Agree terms and implementation. | Contract fully aligned, final price agreed |
| 7 | Opt-out | Won but opt-out clause in contract. Stays here until opt-out expires. | Opt-out period expires |
| 8 | Won | Contract signed. INSfile generated. Handover to delivery/SaaS/CSM. | Delivery team aligned |
| 0 - Lost | Lost | Deal lost to competitor, incumbent, or stopped | — |
| 0 - No Opportunity | No Opportunity | Not a valid opportunity | — |

### Deal Review Gates (Slide 12)

| Gate | When | What |
|---|---|---|
| Commercial Approval (Go/No-Go) | Stage 3 entry | LAND = all deals. Decision whether SimCorp resources should engage. |
| Margin Review | Before any price proposal | For each iteration of scope, discount & payment schedule |
| Deal Services Design | Early stage | Review of implementation costs, risks, timelines |
| Final Review | Stage 5-6 | Before proceeding to final contracting |

### Opportunity Types (LAER Model)

| Type | Meaning |
|---|---|
| Land | First sale to a new customer + initial implementation |
| Expand | Cross-selling and upselling to existing customers |
| Renewal | Ensuring customer renews contract(s) |
| Amendment | Contract modification |
| Cancellation | Contract termination |

### PAIC Qualification Framework

Used in early-stage (Stage 1-2) to evaluate opportunities:
- **P**riority — strategic importance within target account
- **A**uthority — key decision-makers identified and engaged
- **I**mpact — potential benefit to customer's business
- **C**ritical Timeline — urgency and implementation timeline

### ARR Calculation Rules (Slides 24-25)

- ARR = recurring part of Annual Contract Value (ACV)
- Criteria: Price Type = Recurring, Selling Term >= 1 year, Sold Status != "trial"
- **Included:** Subscription fees, maintenance, CDD, SaaS/PaaS/BPaaS, recurring PS
- **Excluded:** Implementation services, non-embedded onboarding, ad hoc extended support
- **Renewals:** Only net new ARR shown under "Opportunity ARR"
- **SBL conversions:** Only additional license + SaaS (net new ARR), Opp Subtype must be "New Revenue"
- ARR hits calculation in the month of signing

### Forecasting (Slide 17)

- Default forecast types assigned by stage
- Opportunity owner can change probability and forecast category
- MDs, Sales Directors, and Heads of CS responsible for forecast accuracy
- Two questions answered: How much? When?

---

## Authority Policy (November 2024)

All amounts are net, EUR. Four-eyes principle applies (two signatories required).

### Authority Levels

| Level | Function |
|---|---|
| A | CEO |
| B | Member of EMB |
| C | Members of ExCo |
| D | Senior Vice President / Corporate Vice President |
| E | Vice President / Associate Vice President / Senior Director |
| F | Director / Manager |
| G | Other employees within sales |
| H | Project Manager / Invoice Manager |

### Subscription, SaaS, BPaaS — Approval Thresholds

| Authority Level | Annual Fees (up to) | Discounts (up to) | Credit Invoices (up to) |
|---|---|---|---|
| A (CEO) | EUR 3,000,000 | Above 40% | EUR 500,000 |
| B and C (EMB/ExCo) | EUR 2,000,000 | 40% | EUR 400,000 |
| D (SVP/CVP) | EUR 800,000 | 25% | EUR 100,000 |
| E (VP/AVP/Sr Director) | EUR 250,000 | 15% | EUR 10,000 |
| F and G (Director/Manager/Sales) | EUR 150,000 | 5% | No |
| H (Project/Invoice Manager) | No | No | No |

### Professional Services — Approval Thresholds

| Authority Level | T&M (up to) | Recurring Services / Fixed Fee (up to) |
|---|---|---|
| A (CEO) | EUR 5,000,000 | EUR 4,000,000 |
| B and C (EMB/ExCo) | EUR 4,000,000 | EUR 3,000,000 |
| D (SVP/CVP) | EUR 1,500,000 | EUR 1,000,000 |
| E, F and G (VP down to Sales) | EUR 250,000 | EUR 250,000 |
| H (Project/Invoice Manager) | No | No |

### Key Implications for Data Dump

- Deals with ACV > EUR 250K need VP+ approval
- Deals with ACV > EUR 800K need SVP+ approval
- Deals with ACV > EUR 2M need ExCo/EMB approval
- Discounts > 15% need VP+, > 25% need SVP+, > 40% need EMB+
- All agreements reviewed by Group Legal (exception: existing terms < EUR 100K)

