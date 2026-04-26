# SimCorp CRM Architecture Reference

Extracted from Salesforce org `simcorp.my.salesforce.com` on March 9, 2026.

## 1. SALES PROCESS

### Opportunity Stages (numbered sales process)

| Stage              |  Count |     % | Notes                    |
| ------------------ | -----: | ----: | ------------------------ |
| 0 - No Opportunity | 21,566 | 45.1% | Placeholder/disqualified |
| 8 - Won            | 15,518 | 32.5% | Closed won               |
| 0 - Lost           |  8,938 | 18.7% | Closed lost              |
| 2 - Discovery      |    729 |  1.5% | Active pipeline          |
| 3 - Engagement     |    430 |  0.9% | Active pipeline          |
| 1 - Prospecting    |    354 |  0.7% | Active pipeline          |
| 4 - Shortlisted    |    116 |  0.2% | Active pipeline          |
| 5 - Preferred      |     44 |  0.1% | Active pipeline          |
| 6 - Contracting    |     41 |  0.1% | Active pipeline          |
| Quota              |     12 |  0.0% | Quota placeholder        |

**Active pipeline = stages 1-6 = ~1,714 records (3.6% of total)**

### Forecast Categories

| Category |  Count |
| -------- | -----: |
| Omitted  | 31,457 |
| Closed   | 15,518 |
| Pipeline |    574 |
| BestCase |    118 |
| Forecast |     81 |

### Opportunity Types

| Type           |  Count | Description                            |
| -------------- | -----: | -------------------------------------- |
| Expand         | 29,503 | Upsell/cross-sell to existing customer |
| Land           | 11,632 | New logo acquisition                   |
| PS             |  3,166 | Professional services                  |
| Fast track ALF |  1,615 | Fast-track annual license fee          |
| Fast track PS  |    856 | Fast-track professional services       |
| Renewal        |    691 | Contract renewal                       |
| Coric ILF      |    229 | Client Reporting initial license       |
| Coric ALF      |     55 | Client Reporting annual license        |
| Coric PS       |      1 | Client Reporting prof. services        |

### Opportunity Sub-Types

| Sub-Type       |  Count |
| -------------- | -----: |
| New Revenue    | 29,208 |
| Renewal        |    691 |
| Concession     |    376 |
| SBL Conversion |    259 |
| Channel Play   |    158 |

### Record Types (Opportunity)

| Record Type     | Active | Default |
| --------------- | ------ | ------- |
| Opportunity     | Yes    | No      |
| QtC Opportunity | Yes    | **Yes** |
| Quota           | Yes    | No      |

**QtC = Quote-to-Cash (Apttus/Conga CPQ integration)**

### Opportunity Stage Status (BDR handoff)

| Status                     | Count |
| -------------------------- | ----: |
| In Dealmaker               |   266 |
| Qualified by Marketing     |   245 |
| Open                       |   145 |
| With Telemarketing         |    39 |
| Accepted by Sales Director |    28 |
| Disqualified by Marketing  |    12 |
| Sales Handshake            |     4 |
| Rejected by Sales Director |     3 |
| Accepted by AM/SM          |     2 |

---

## 2. PRODUCT MODEL

### Product Families (APTS_RH_Product_Family\_\_c — multipicklist)

| Product Family            | Count (2K sample) | Description                     |
| ------------------------- | ----------------: | ------------------------------- |
| SCD Software              |             1,557 | Core SimCorp Dimension platform |
| SCD Operational Services  |               316 | Managed operations              |
| XaaS                      |               252 | Anything-as-a-Service           |
| SCD Consulting            |               216 | Professional services           |
| 3rd party products        |               168 | Third-party integrations        |
| Data Management           |                96 | GAIN/data management software   |
| SimCorp SaaS              |                69 | Cloud-hosted SCD                |
| White Label Products      |                52 | White-label solutions           |
| Client Driven Development |                52 | Custom development              |
| Client Communications     |                51 | Coric/client reporting          |
| Data Management Services  |                25 | DMS BPaaS                       |
| Regulatory Services       |                12 | Compliance/regulatory           |
| Digital Engagement        |                 5 | Digital portal                  |
| Analytics Services        |                 3 | Analytics/reporting             |

### Contract/Licensing Models

| Contract Type | Count | Description                |
| ------------- | ----: | -------------------------- |
| SBL           |   291 | Subscription-based license |
| MBL           |    72 | Module-based license       |
| PPL           |    11 | Price-per-license          |

### Subscription Terms (years)

| Term      | Count |
| --------- | ----: |
| 5 years   | 3,684 |
| 7 years   | 1,830 |
| 3 years   |   375 |
| 10 years  |   254 |
| 6-9 years |   829 |

### SaaS Indicator (ASP\_\_c)

| Value     | Count |
| --------- | ----: |
| No        | 1,375 |
| Yes       |   136 |
| Potential |   127 |

### Project Services (PSI_PSA_PSO\_\_c)

| Type                          | Count |
| ----------------------------- | ----: |
| Project Services              | 6,254 |
| Repeatable Services           | 3,135 |
| Project & Repeatable Services |    34 |

### Key Revenue Fields on Opportunity

| Field                                   | Label                       | Type     |
| --------------------------------------- | --------------------------- | -------- |
| Amount                                  | Amount                      | currency |
| APTS_Opportunity_ARR\_\_c               | Opportunity ARR             | currency |
| APTS_Forecast_ARR\_\_c                  | Forecast ARR                | currency |
| APTS_Forecast_ACV_AVG\_\_c              | Forecast ACV                | currency |
| APTS_Total_ACV_AVG\_\_c                 | Total ACV (avg)             | currency |
| APTS_Total_ACV_Final\_\_c               | Total ACV (final)           | currency |
| APTS_Renewal_ACV\_\_c                   | Renewal ACV                 | currency |
| APTS_Forecast_Renewal_ACV\_\_c          | Forecast Renewal ACV        | currency |
| APTS_Forecast_Consulting_ACV\_\_c       | Forecast Consulting ACV     | currency |
| APTS_Forecast_Consulting_Revenue\_\_c   | Forecast Consulting Revenue | currency |
| APTS_Forecast_NPP_Display\_\_c          | Forecast OI (Order Inflow)  | currency |
| APTS_Forecast_Quota_Retirement\_\_c     | Quota Retirement (Forecast) | currency |
| ATPS_Total_Commission_Basis\_\_c        | Quota Retirement            | currency |
| RH_PS_Annual_Recurring_Revenue_ARR\_\_c | PS Recurring ACV EBIT       | currency |

### OpportunityLineItem Key Custom Fields

| Field                             | Label                   |
| --------------------------------- | ----------------------- |
| APTS_Product_Family\_\_c          | Product Family          |
| APTS_Product_Name\_\_c            | Product Name            |
| APTS_Product_Option_Parent\_\_c   | Parent Offer            |
| APTS_Revenue_Stream\_\_c          | Revenue Stream          |
| APTS_ChargeType\_\_c              | Charge Type             |
| APTS_NetPrice\_\_c                | Net Price               |
| APTS_ACV_1st_Year\_\_c            | ACV 1st Year            |
| APTS_ACV_2nd_Year\_\_c            | ACV 2nd Year            |
| APTS_Average_ACV\_\_c             | Average ACV             |
| APTS_Opportunity_Product_ARR\_\_c | Opportunity Product ARR |
| Product_Family\_\_c               | Product Family (alt)    |
| Revenue_Stream\_\_c               | Revenue Stream          |
| Agreement_Type\_\_c               | Agreement Type          |

---

## 3. GEOGRAPHIC & ORGANIZATIONAL MODEL

### Sales Regions (Account.Region\_\_c)

| Region                   | Count |
| ------------------------ | ----: |
| Central Europe           | 3,450 |
| North America            | 3,052 |
| APAC                     | 2,042 |
| United Kingdom & Ireland | 1,649 |
| Southwestern Europe      | 1,633 |
| Northern Europe          |   561 |
| Middle East & Africa     |   516 |

### Account Unit Groups (3 super-regions)

| Unit Group       |  Count |
| ---------------- | -----: |
| SC EMEA          | 33,640 |
| SC North America | 10,189 |
| SC Asia          |  3,872 |

### Account Units (legal entities)

| Unit               |  Count | Region  |
| ------------------ | -----: | ------- |
| SC USA             |  7,763 | NA      |
| SC GmbH            |  6,925 | CE      |
| SC Ltd.            |  4,176 | UK      |
| SC Denmark Markets |  4,118 | NE      |
| SC Netherlands     |  2,908 | SE      |
| SC France          |  2,891 | SE      |
| SC Sweden          |  2,814 | NE      |
| SC Norway          |  2,578 | NE      |
| SC BS AG           |  2,445 | CE      |
| SC Canada          |  2,426 | NA      |
| SC Asia            |  2,362 | APAC    |
| SC Singapore       |  1,127 | APAC    |
| SC Austria         |  1,046 | CE      |
| SC Middle East     |  1,041 | MEA     |
| SC Benelux         |    966 | SE      |
| SC Finland         |    842 | NE      |
| + 7 more           | ~1,320 | Various |

### Currencies (on Opportunities)

| Currency |  Count |
| -------- | -----: |
| EUR      | 19,637 |
| USD      | 10,229 |
| GBP      |  3,440 |
| DKK      |  3,037 |
| SEK      |  2,716 |
| NOK      |  2,416 |
| CHF      |  2,076 |
| AUD      |  1,695 |
| CAD      |  1,647 |
| SGD      |    663 |

---

## 4. ACCOUNT MODEL

### Account Types

| Type       |  Count |
| ---------- | -----: |
| Prospect   | 10,063 |
| Partner    |  1,902 |
| Customer   |    958 |
| Affiliate  |    487 |
| Competitor |     64 |
| Internal   |     17 |

### Account Record Types

| Record Type          | Active        |
| -------------------- | ------------- |
| Competitor / Partner | Yes           |
| Customer             | Yes           |
| Prospect / Affiliate | Yes (default) |
| Global Ultimate      | No            |

### Customer Segments (Account.Customer_Segment\_\_c)

| Segment                | Count |
| ---------------------- | ----: |
| E                      |    80 |
| C                      |    46 |
| Sofia only             |    44 |
| Client Comm. only      |    39 |
| A                      |    29 |
| D                      |    28 |
| Data Mgmt Service only |    24 |
| B                      |    20 |
| F                      |    12 |
| IAS only               |     2 |

**A-F = tiered segmentation. Product-only segments = single-product customers.**

### Industries (Account.Industry)

| Industry              | Count |
| --------------------- | ----: |
| Asset Management      | 4,268 |
| Bank                  | 1,605 |
| Insurance             | 1,335 |
| Pension               | 1,058 |
| Wealth Management     |   991 |
| Other                 |   879 |
| Fund                  |   584 |
| Asset Servicer        |   391 |
| Central Bank          |   154 |
| Sovereign Wealth Fund |    93 |
| Service Provider      |    93 |
| Asset Owner           |    61 |

### Account Hierarchy

- `ParentId` — standard parent-child relationship
- `Global_Ultimate__c` — reference to top-level parent (GU DUNS-linked)
- `Is_Global_Ultimate__c` — boolean flag
- `GU_Type__c` — Account Hierarchy Type
- `Hierarchy_Level__c` — numeric hierarchy level
- `Finance_Client__c` — "Official Client" boolean

### Key Account Fields for Dashboards

| Field                       | Label                   | Use                       |
| --------------------------- | ----------------------- | ------------------------- |
| Region\_\_c                 | Sales Region            | Geographic slicing        |
| Unit\_\_c                   | Unit                    | Legal entity              |
| Industry                    | Industry                | Industry vertical         |
| Customer_Segment\_\_c       | Customer Segment        | Tier segmentation         |
| Type                        | Account Type            | Customer/Prospect/Partner |
| AuM_m\_\_c                  | AuM (b)                 | Assets under management   |
| ASP\_\_c                    | SCDaaS Client           | SaaS indicator            |
| Axioma_Client\_\_c          | Axioma Client           | Product adoption          |
| Gain_Client\_\_c            | Data Management Client  | Product adoption          |
| Client_Coric\_\_c           | Client Reporting Client | Product adoption          |
| SaaS_Client\_\_c            | SaaS Client             | SaaS adoption             |
| Finance_Client\_\_c         | Official Client         | Is active customer        |
| Inside_Sales_Owner\_\_c     | BDR                     | BDR assignment            |
| Sales_Manager\_\_c          | Sales Manager           | Manager assignment        |
| Top_Account\_\_c            | Top Account             | Strategic flag            |
| Overall_Adoption_Score\_\_c | Overall Adoption Score  | Health metric             |

---

## 5. ROLE / TEAM HIERARCHY

### Owner Roles (on active opportunities)

| Role                                  | Count | Function                            |
| ------------------------------------- | ----: | ----------------------------------- |
| SC NE CX                              | 2,873 | Northern Europe Customer Experience |
| SC CE CX                              | 2,760 | Central Europe Customer Experience  |
| SC EMEA Sales                         | 1,941 | EMEA Sales                          |
| SC Resigned                           | 1,480 | Former employees                    |
| SC NA CX                              | 1,426 | North America CX                    |
| SC SE CX                              | 1,139 | Southwestern Europe CX              |
| SC Management                         | 1,042 | Management                          |
| SC EMEA Consulting Opportunity Owners |   975 | EMEA Consulting                     |
| SC UK & ME CX                         |   629 | UK & Middle East CX                 |
| SC EMEA PSO Opportunity Owners        |   478 | EMEA PSO                            |
| SC NA Sales                           |   350 | North America Sales                 |
| SC NA Consulting Opportunity Owners   |   312 | NA Consulting                       |
| SC Asia CX                            |   302 | Asia CX                             |
| SC Asia Sales                         |   198 | Asia Sales                          |

**Pattern**: `SC {Region} {Function}` — CX = Account Management, Sales = New Business

---

## 6. MANAGED PACKAGES / INTEGRATIONS

### Apttus/Conga CPQ (Quote-to-Cash)

- **506 custom objects** — majority are `Apttus_Config2__*`, `Apttus_Proposal__*`, `Apttus_Approval__*`
- `QtC Opportunity` is the default record type
- Line items flow: Opportunity → Apttus Quote → Apttus Order → Apttus Asset
- Key CPQ fields on OLI: `APTS_NetPrice__c`, `APTS_ChargeType__c`, `APTS_PriceType__c`, `APTS_Frequency__c`
- Pricing: base price → adjustments → net price → ACV calculation

### Altify (ALTF\_\_)

- Account planning, opportunity management, relationship maps
- `ALTF__Account_Plan__c`, `ALTF__Opportunity__c`, `ALTF__Contact_Map_Details__c`
- Fields on Opportunity: `Altify_Plan_Quality__c`, `Altify_Plan_Status__c`, `Altify_Sales_Process_Status__c`

### Zimit (ZIMIT\_\_)

- Services quoting/estimation
- `ZIMIT__zRevenue__c` = Zimit Amount, `ZIMIT__zHours__c` = Hours, `ZIMIT__zGM__c` = Margin %

### Demandbase/Engagio (engagio\_\_)

- ABM platform, intent data
- `engagio__pipeline_predict_score__c`, `engagio__qualification_score__c`
- `engagio__Status__c` = Demandbase Journey Stage

### Pardot (pi\_\_)

- Marketing automation
- Campaign influence, lead scoring

### Consensus (Consensus\_\_)

- Demo automation
- `Consensus__cTotalDBViews__c`, `Consensus__cTotalDBWatchTime__c`

### Rollup Helper (rh2\_\_)

- Declarative rollup summaries
- Powers many `RH_*` calculated fields on Opportunity

---

## 7. OBJECT RELATIONSHIP MAP

```
Account (13,491)
  ├── Opportunity (47,748) via AccountId
  │     ├── OpportunityLineItem (29,046 opps have them) via OpportunityId
  │     ├── OpportunityContactRole via OpportunityId
  │     ├── OpportunityTeamMember via OpportunityId
  │     ├── OpportunityHistory via OpportunityId
  │     ├── Apttus_Proposal__Proposal__c via Opportunity__c
  │     ├── Apttus_Config2__Order__c via RelatedOpportunityId
  │     ├── EBIT_Calculation__c via Opportunity__c
  │     ├── Opportunity_Snapshot__c via Opportunity__c
  │     ├── ALTF__Opportunity__c via Opportunity__c
  │     ├── Task via WhatId
  │     └── Event via WhatId
  ├── Contact (153,571) via AccountId
  ├── Contract (17,553) via AccountId
  ├── Case via AccountId
  ├── ALTF__Account_Plan__c via Account__c
  └── Adoption_Score__c via Account__c

Lead (51,465)
  └── Campaign via CampaignId (Campaign Member)

User
  ├── Opportunity via OwnerId
  ├── Account via OwnerId
  └── UserRole (hierarchy)
```

---

## 8. KEY INSIGHTS FOR DASHBOARD BUILDING

1. **Filter on active pipeline**: Always exclude "0 - No Opportunity" and "0 - Lost" unless specifically analyzing win/loss. Only ~1,714 records are active pipeline.

2. **ARR is the spine**: `APTS_Opportunity_ARR__c` (5,532 records with value > 0) and `APTS_Forecast_ARR__c` (2,123 records) are the primary revenue metrics.

3. **Multi-currency**: 10 currencies in play. Always use `CurrencyIsoCode` and converted currency fields where available.

4. **Product family is multipicklist**: Can't GROUP BY directly in SOQL. In CRM Analytics, need to parse with SAQL or use line item `APTS_Product_Family__c` (string, groupable).

5. **Expand dominates**: 61.7% of opportunities are "Expand" type — the business is overwhelmingly upsell/cross-sell, not new logos (24.3%).

6. **Global Ultimate hierarchy**: Use `Global_Ultimate__c` for account family views, not just `ParentId`.

7. **CX vs Sales roles**: "CX" roles are account managers (existing customers), "Sales" roles are new business. Key for BDR/pipeline dashboards.

8. **Apttus/Conga CPQ is deeply embedded**: Most pricing/quoting flows through CPQ. Line item data is rich but complex.

9. **Region hierarchy**: Account_Unit_Group**c (3) → Region**c (7) → Account_Unit\_\_c (23) — three levels of geographic drill-down.

10. **Customer vs Prospect**: Only 958 accounts are type "Customer" vs 10,063 "Prospect" — most account records are prospects.
