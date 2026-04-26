---
title: Salesforce Knowledge Corpus — Reports, Dashboards, and Data-Quality Gaps
type: reference / audit findings
org: apro@simcorp.com (simcorp.my.salesforce.com)
generated: 2026-04-16
scope: FY26 open pipeline + FY26 Q1 closed + account-level whitespace signals
---

# Salesforce Knowledge Corpus

Companion to `2026-04-16-salesforce-data-model-overview.md` — that doc explains the **structure**, this doc explains **what's in it, what's being reported on, and what's broken**.

---

## Part 1 — Reports & Dashboards inventory

### Top-level counts

| Artifact                             |      Count | Organized in |
| ------------------------------------ | ---------: | ------------ |
| Reports (non-private)                | **20,879** | 759 folders  |
| Dashboards (non-private)             |    **894** | 139 folders  |
| Reports in sales-adjacent folders    |  **2,052** | 65 folders   |
| Dashboards in sales-adjacent folders |    **110** | 9 folders    |

### Report folders by volume (top 20)

```
2935  Public Reports          (catch-all)
 583  Commonly Accessed
 542  Group Marketing
 477  NA MKT
 471  Archive
 438  Asia Marketing
 367  Course Booking
 344  Marketing Archived Reports
 286  Global Sales Reports    ← canonical sales reports
 284  Victor Misc
 267  Adhoc
 248  GTM Reports
 247  Product Management
 244  PaaS - Dashboards       (misnamed — these are reports)
 241  CE Sales                ← EMEA Central Europe sales reports
 221  EMEA ABM Reports
 218  Revenue Operations      ← revops-governed reports
 202  Email Distribution Lists - EMEA
 194  Client
 192  Support Lines
```

Plus niche folders: `Sales KPI tracking` (43), `SalesOps Cockpit` (20), `Sales Review` (19), `Forecast/Pipeline Quality` (11), `Renewals` (8), `KPIs vs Targets` (9), `Scorecard KPIs` (11), `Commitment Reports` (12).

### Dashboard folders by volume (top 15)

```
 91  Company Dashboards
 52  Measurement & Performance
 39  Revenue Operations       ← has the AMER / APAC / EMEA / NA / CE / TS regional Sales Mgmt Dashboards
 35  Support Lines
 32  NA Dashboards
 26  Adhoc
 24  LIVE Dashboards
 23  Andre                    ← personal experimentation (the 5 SD Monthly clones live here)
 21  Archive
 21  Unified Service Desk
 20  CE Dashboards
 20  PaaS Dashboards
 19  Client
 18  2021 Performance KPIs
 17  Lightning Communities Dashboards
```

### Canonical sales-leadership dashboards

The most production-ready dashboards for sales directors / regional leaders:

| ID                   | Folder                  | Title                                                                     |
| -------------------- | ----------------------- | ------------------------------------------------------------------------- |
| `01ZTb00000FSP7hMAH` | Sales KPI Tracking      | **Sales Directors Monthly Pipeline and Insights** (the one we redesigned) |
| `01ZTb00000Dz8fVMAR` | Revenue Operations      | AMER Sales Mgmt — Pipeline review 2026                                    |
| `01ZQA000000e5Yr2AI` | Revenue Operations      | APAC Sales Mgmt — Pipeline review 2026                                    |
| `01ZTb00000DNFBlMAP` | Revenue Operations      | NA Sales Mgmt — Pipeline review 2026                                      |
| `01Z7S000000PortUAC` | Revenue Operations      | EMEA Mgmt — Pipeline review                                               |
| `01Z7S000000PocdUAC` | Revenue Operations      | EMEA Sales Mgt — Pipeline Analytics                                       |
| `01ZTb00000CcCHNMA3` | Revenue Operations      | Client Accounts — 2026-2027 Pipeline review                               |
| `01Z7S000000PoryUAC` | Revenue Operations      | EMEA Mgmt — Pipeline Analytics                                            |
| `01Z7S000000H3u7UAC` | Revenue Operations      | **Data Hygiene Dashboard** (critical — underused)                         |
| `01Z7S000000bvUQUAY` | Revenue Operations      | Quota — User Percentage                                                   |
| `01ZTb00000FMnflMAD` | Revenue Operations      | MQA Command Center                                                        |
| `01Z2o000000ahHcEAI` | Global Sales Dashboards | GTM Forecast                                                              |

Plus **regional drill-downs**: 13+ "Pipeline Reporting & Insights" CRMA dashboards (one per region/territory), most registered as managed via the harness registry.

### Reports already invoked by the SD Monthly ETL (source-of-truth reports)

| ID                   | Name                                                      | Used by                                                |
| -------------------- | --------------------------------------------------------- | ------------------------------------------------------ |
| `00OTb000008fzirMAA` | SD Pipeline Open FY26                                     | extract_director_live.py via SOQL (field set template) |
| `00OTb000008fzkTMAQ` | SD Won Lost FY26                                          | extract_director_live.py (field set template)          |
| `00OTb000008fzm5MAA` | SD Top Deals FY26                                         | UI only (dashboard component)                          |
| `00OTb000008gUrVMAU` | SD Win Rate by Stage                                      | build_sharepoint_analysis.py (Win Rate tab)            |
| `00OTb000008gUt7MAE` | SD Days in Stage                                          | build_sharepoint_analysis.py (Days in Stage tab)       |
| 18 × HT reports      | Historical Trending Q1+Q2, one per director               | extract_historical_trending.py                         |
| `01ZTb00000FSP7hMAH` | Sales Directors Monthly Pipeline and Insights (dashboard) | build_dashboard_analysis_excel.py                      |
| `01ZTb00000FSP9JMAX` | Sales Ops Quarterly KPI (dashboard)                       | build_dashboard_analysis_excel.py                      |

### Duplicates / cruft worth cleaning

- Multiple "Pipeline Reporting & Insights" CRMA dashboards (13+ of them) — likely one-per-region but no naming convention makes it obvious which belongs to whom
- Multiple "Sales Directors Monthly Pipeline and Insights" Lightning clones in the Andre folder (5 clones) — drafts / experiments
- `Archive` folder has 471 reports + 21 dashboards — nominal retirement path but may contain orphans
- `Copy of APAC Sales Mgmt Dashboard - Pipeline review 2024`, `Copy of AS NA Sales Mgmt Dashboard - Pipeline review 2026`, etc. — editor-created duplicates that were never cleaned up
- Private Reports folder(s) excluded from this audit

---

## Part 2 — Opportunity data-quality gaps

Scope: **FY26 open pipeline, Land + Expand + Renewal (1,278 deals).** All counts are as-of today.

### Blockers — deals that cannot be managed without action

| Finding                                                   | Count | % of pipeline | Impact                                           |
| --------------------------------------------------------- | ----: | ------------: | ------------------------------------------------ |
| Deals at Stage 3+ with **no NextStep**                    |   327 |           26% | Owners can't progress; manager can't coach       |
| Land deals at Stage 3+ missing approval AND not submitted |    16 |          1.3% | Commercial-review queue gap                      |
| Stage 1-2 with CloseDate inside next 30 days              |    80 |            6% | Unrealistic commits — sandbagging or misforecast |

### Activity hygiene — leading indicator of deal health

| Finding                                          | Count |       % | What it means                                                                     |
| ------------------------------------------------ | ----: | ------: | --------------------------------------------------------------------------------- |
| **LastActivityDate NULL** (no touch ever logged) |   983 | **77%** | Pipeline is massively under-logged. The built-in "silent deal" signal can't fire. |
| LastActivityDate > 60 days ago                   |   230 |     18% | Genuinely stale (on top of the 983 that have NO activity at all)                  |
| LastActivityDate > 90 days ago                   |   209 |     16% | Very stale; dead-deal candidates                                                  |

This is the single biggest data-quality finding — 77% of open pipeline has **never** had a logged activity. Either integrations aren't capturing emails/meetings, or reps aren't logging. Either way, our Deal Risk Scoring `STALE` rule (no activity 60+ days) and Activity Volume sheet (silent deals) are both crippled by this.

### Push / slip patterns

| Finding                  | Count |   % |
| ------------------------ | ----: | --: |
| PushCount >= 3           |   349 | 27% |
| PushCount >= 5 (chronic) |   178 | 14% |

### Attribution / routing

| Finding                         | Count |                                                          % |
| ------------------------------- | ----: | ---------------------------------------------------------: |
| Missing `Lead_Scope__c`         |   102 |                                                         8% |
| Missing `APTS_Forecast_ARR__c`  |    23 |                                                         2% |
| Missing `APTS_Primary_Quote__c` |   463 | **36%** — a third of pipeline has no Apttus quote attached |

### Lifecycle / aging

| Finding                 | Count |                                                                         % |
| ----------------------- | ----: | ------------------------------------------------------------------------: |
| Pipeline aged 365+ days |   464 | **36%** — more than a third of open pipeline has been sitting over a year |

### Closed Q1 2026 deal hygiene (Land + Expand, 478 closed)

| Finding                                                 | Count |                                                                             % |
| ------------------------------------------------------- | ----: | ----------------------------------------------------------------------------: |
| "No Opportunity" (disqualified) missing Reason_Won_Lost |   208 |                                        43% — can't analyse why these fell out |
| Lost deals without competitor attribution               |    94 |                                          20% — lost but we don't know to whom |
| Won with zero ARR                                       |   103 | 22% — likely renewals/bookings with different amount path, but worth auditing |

### What to do

| Priority | Gap                                          | Fix path                                                                                                                                          |
| -------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| 🔥       | 77% open pipeline has no logged activity     | Integration audit: Outlook + email sync. Rep coaching / mandated logging rule. Possibly a flow that blocks stage advancement without an activity. |
| 🔥       | 327 deals Stage 3+ with no NextStep          | Validation rule on Stage 3+ save; dashboard red-flag component.                                                                                   |
| ⚡       | 208 Q1 "No Opportunity" deals missing Reason | Validation rule on close; back-fill campaign.                                                                                                     |
| ⚡       | 464 deals >365 days old                      | Monthly auto-review queue; owner must reset CloseDate or mark No Opp.                                                                             |
| 📊       | 463 deals with no Apttus primary quote       | Process question — when is a quote expected? Stage-gate rule.                                                                                     |
| 📊       | 103 Won deals with zero ARR                  | Audit these specifically — data entry gap or intentional (like-for-like renewal)?                                                                 |

---

## Part 3 — Account data-quality gaps

Scope: **462 accounts with FY26 open pipeline.**

### Blockers

| Finding                                     | Count |                                   % |
| ------------------------------------------- | ----: | ----------------------------------: |
| KYC not Approved (including blank/null)     |     3 | 0.6% — excellent compliance hygiene |
| Contract_End_Date in past (renewal overdue) |    19 |                4% — renewal backlog |

### Attribution / segmentation

| Finding                  |   Count |                                       % |
| ------------------------ | ------: | --------------------------------------: |
| Missing AccountSource    |     106 |                                     23% |
| Missing Classification   | **461** | **99.8%** — field is essentially unused |
| Missing Industry         |       0 |                                       ✓ |
| Missing BillingCountry   |       0 |                                       ✓ |
| Missing Tier_Calculation |       0 |                                       ✓ |

### Legal/commercial readiness

| Finding            | Count |                                                             % |
| ------------------ | ----: | ------------------------------------------------------------: |
| NDA_Signed = false |   213 | 46% — nearly half of pipeline accounts don't have NDA flagged |

### What to do

| Priority | Gap                                | Fix path                                                       |
| -------- | ---------------------------------- | -------------------------------------------------------------- |
| 🔥       | 19 accounts with contract past end | Immediate renewal queue → Renewals team                        |
| ⚡       | 213 accounts flagged NDA unsigned  | Legal ops audit — is the flag accurate, or is it stale?        |
| 📊       | 461 missing Classification         | Either retire the field or bulk-populate via segmentation rule |
| 📊       | 106 missing AccountSource          | Attribution gap — integrate with marketing sourcing            |

---

## Part 4 — Whitespace / upsell signals (accounts with no open pipeline)

Using the custom flags on Account, we can surface accounts that are commercially interesting but have nothing in pipeline:

| Signal                                                |     Count | What it means                                                                                                                                        |
| ----------------------------------------------------- | --------: | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Installed competitor product, no open opp**         | **1,772** | Win-back opportunity queue — these accounts have a SimCorp-competitor product installed and we have no deal in play. Biggest single opportunity set. |
| Axioma client but no DMS (DM Software) client         |       413 | Cross-sell: Axioma-using accounts without Gain/DMS products                                                                                          |
| SCDaaS clients (`ASP__c` = true)                      |       104 | Existing cloud clients — natural upsell base                                                                                                         |
| AMS clients                                           |        14 | Managed-services clients — service-led upsell                                                                                                        |
| Current SimCorp client with `APTS_Open_INS__c` = true |         3 | Active implementation — renewal timing needs tracking                                                                                                |
| High AuM (>10B), zero open opps                       |         2 | Whale accounts we're not pursuing                                                                                                                    |

**Implication**: the Installed Competitor signal alone (1,772 accounts) represents a TAM bigger than the current pipeline (1,278 open deals). This is the most actionable unused signal in the org.

### Where to build this surface

- New Excel tab **Account Whitespace**, fed by SOQL joining `Account` to `Installed_Competitor_Product__c` and subquery on open Opportunity
- New Lightning report **SD Accounts Without Pipeline — Competitor Products Installed** (Account-report-type with the filters above), add as dashboard component on the SD Monthly dashboard v2
- SOQL (copy-paste reference):
  ```sql
  SELECT Account.Name, Account.Industry, Account.AuM_m__c, Account.BillingCountry,
         Account.Account_Unit_Group__c, Competitor_Product__r.Name
  FROM Installed_Competitor_Product__c
  WHERE Account.Id NOT IN (
    SELECT AccountId FROM Opportunity
    WHERE IsClosed = false AND Type IN ('Land','Expand','Renewal')
  )
  ORDER BY Account.AuM_m__c DESC NULLS LAST
  ```

---

## Part 5 — Reference SOQL catalog

Paste-ready queries for the gap scans above. Run via `sf data query --query "..." --target-org apro@simcorp.com`.

### Opportunity health

```sql
-- Open FY26 deals missing NextStep at Stage 3+
SELECT Id, Account.Name, Name, Owner.Name, StageName, CloseDate,
       APTS_Opportunity_ARR__c, NextStep
FROM Opportunity
WHERE IsClosed = false
  AND Type IN ('Land','Expand','Renewal')
  AND CloseDate >= 2026-01-01 AND CloseDate <= 2026-12-31
  AND StageName IN ('3 - Engagement','4 - Shortlisted','5 - Preferred','6 - Contracting')
  AND (NextStep = NULL OR NextStep = '')

-- Deals with no logged activity ever
SELECT Id, Account.Name, Name, Owner.Name, StageName, CreatedDate
FROM Opportunity
WHERE IsClosed = false
  AND LastActivityDate = NULL
  AND CloseDate >= 2026-01-01

-- Unrealistic Stage 1-2 commits
SELECT Id, Account.Name, Name, Owner.Name, StageName, CloseDate, Probability
FROM Opportunity
WHERE IsClosed = false
  AND StageName IN ('1 - Prospecting','2 - Discovery')
  AND CloseDate <= NEXT_N_DAYS:30
```

### Account health

```sql
-- Accounts with past contract end dates (renewal backlog)
SELECT Id, Name, Industry, APTS_Contract_End_Date__c, OwnerId
FROM Account
WHERE APTS_Contract_End_Date__c != NULL
  AND APTS_Contract_End_Date__c < TODAY
  AND Id IN (SELECT AccountId FROM Opportunity WHERE IsClosed = false)

-- Whales with no open pipeline
SELECT Id, Name, Industry, AuM_m__c, BillingCountry, OwnerId
FROM Account
WHERE AuM_m__c > 10000
  AND Id NOT IN (SELECT AccountId FROM Opportunity WHERE IsClosed = false)
```

### Whitespace / competitor signals

```sql
-- Installed competitor products at accounts we're not actively pursuing
SELECT Account__c, Account__r.Name, Account__r.Industry,
       Competitor_Product__r.Name
FROM Installed_Competitor_Product__c
WHERE Account__c NOT IN (
  SELECT AccountId FROM Opportunity WHERE IsClosed = false
)

-- Axioma clients without DMS (DM Software)
SELECT Id, Name, Industry, BillingCountry, OwnerId
FROM Account
WHERE Axioma_Client__c = true
  AND Id NOT IN (
    SELECT AccountId FROM Opportunity
    WHERE StageName LIKE '%Won%' AND APTS_RH_Product_Family__c INCLUDES ('DM Software')
  )
```

### Closed-deal hygiene

```sql
-- Lost deals without competitor attribution
SELECT Id, Account.Name, Name, Owner.Name, CloseDate,
       APTS_Opportunity_ARR__c, Reason_Won_Lost__c
FROM Opportunity
WHERE IsClosed = true
  AND StageName = '0 - Lost'
  AND CloseDate >= 2026-01-01 AND CloseDate <= 2026-03-31
  AND (Lost_to_Competitor__c = NULL OR Lost_to_Competitor__c = '')

-- Won with zero ARR (tracking anomalies)
SELECT Id, Account.Name, Name, Owner.Name, CloseDate, Type,
       APTS_Opportunity_ARR__c, Amount
FROM Opportunity
WHERE IsClosed = true
  AND StageName = '8 - Won'
  AND CloseDate >= 2026-01-01
  AND (APTS_Opportunity_ARR__c = 0 OR APTS_Opportunity_ARR__c = NULL)
```

---

## Part 6 — Recommendations ordered by impact

### Act now

1. **Activity-logging integration audit** — 77% of open pipeline has zero activity records. Either Outlook/email sync is broken or reps are bypassing logging. Without this fixed, every "silent deal" / "stale pipeline" analytic is misleading.
2. **NextStep validation rule at Stage 3+** — 327 deals stuck with blank NextStep at mid/late stages. A save-blocker rule on Stage ≥ 3 would fix this forever.
3. **Build the Account Whitespace surface** — 1,772 accounts with installed competitor products and no open opp. Biggest single unexploited asset in the data.
4. **Renewal backlog queue** — 19 accounts with contract past end. Immediate Renewals team action.
5. **Stage 1-2 + near-term close date cleanup** — 80 unrealistic commits. Monthly sanity review.

### Ratchet over the quarter

6. **Close-reason completeness** — 208 "No Opportunity" deals have no Reason. Validation rule on close-stage save.
7. **Aging pipeline review** — 464 deals >365 days. Monthly queue that forces owner action (close, extend, or DQ).
8. **Apttus quote attachment** — 463 open deals without a primary quote. Why? Stage-gate or process misalignment?
9. **Classification field** — 99.8% unused. Either retire it or run a bulk-categorization sprint. Don't leave dead fields cluttering the schema.
10. **Lost-to-Competitor completeness** — 94 lost deals without competitor attribution. Dashboard callout + validation rule.

### Longer-term

11. **Dashboards cleanup sprint** — 13+ near-duplicate "Pipeline Reporting & Insights" CRMA dashboards; 5 SD Monthly Lightning clones in Andre folder. Consolidate to one-per-region with a naming convention.
12. **Report archive vs active separation** — 471 reports in "Archive" folder. Either purge or clearly mark the active set that matters.
13. **Installed Competitor Product data-model audit** — 1,772 hits is promising but needs freshness check. How often is this object updated?

---

## Part 7 — Data quality tracking

This corpus should re-run monthly to track whether hygiene improves. Suggested operational runbook:

1. `python3 /tmp/corpus_4b.py` and `python3 /tmp/corpus_5_acct_gaps.py` (the scripts used to build this)
2. Emit results to `output/data_quality/2026-MM-DD/opportunity-gaps.json` + `account-gaps.json`
3. Track trend: "Deals with no NextStep at Stage 3+ went from 327 to 280 this month" = real improvement
4. Surface the delta on the SD Monthly dashboard next cycle

**Next action**: promote these scripts to `scripts/audit_data_quality.py` and wire into `run_monthly_director_review.py` as a new stage `1c_data_quality_audit`.
