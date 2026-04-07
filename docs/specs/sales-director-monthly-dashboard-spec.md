# Sales Director Monthly Dashboard - Expected Widgets Spec

> The permanent contract for `01ZTb00000FSP7hMAH` (Sales Directors Monthly - Pipeline and Insights). Distilled from the KPI brief in `docs/2026-04-06-deck-and-dashboard-verification-handoff.md`. Every future audit or rebuild of this dashboard grades against this file.

## Dashboard identity

- **Dashboard ID:** `01ZTb00000FSP7hMAH`
- **Name:** Sales Directors Monthly - Pipeline and Insights
- **Lightning URL:** https://simcorp.my.salesforce.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view
- **Audience:** Sales Directors (level below Managing Directors)
- **Cadence:** Monthly
- **Format in which it is surfaced:** PowerPoint deck (Option D recipe pulls from the CRMA equivalent)

## KPI brief (verbatim from Andre)

Monthly report to the Sales Directors (level below MDs). Forward looking, insight-driven, PowerPoint format.

- A pipeline overview with quarterly focus (one slide per region)
- Commercial Approval overview - which deals have been approved and a list of any Land stage 3 deals with no commercial approval. Global overview (one slide) + list of candidates by region (one slide)
- Renewals tracking - what renewals are coming up this quarter, what is the value and likelihood of renewing
- Churn Risk and trends - difficult for now, but try to build a slide of what we can get from Finance. Andre is to reach out to Alex P. The current snapshot has `finance_feed_status: pending`.
- Slipped deals analysis (root cause commentary) - start with slipped deals; root cause commentary requires reaching out to the opportunity owner. Andre will structure the outreach.

## Hard rules applied to this dashboard

1. **Calendar year only.** No FY labels. Use `April 2026` or `Q2 2026` (calendar).
2. **ACV for renewals.** Land and expand pipeline stays in ARR. This is a SimCorp methodology quirk.
3. **Type field is canonical for renewal / land / expand.** The `APTS_Primary_Quote_Type__c` field is stale; the org migrated to `SBL`, `MBL`, `PPL`. Use `Type='Renewal'`, `Type='Land'`, `Type='Expand'`.
4. **No em-dashes** anywhere in widget labels. Use hyphens, periods, or rephrase.
5. **No gauges or donuts** in widget visuals. Bullet charts and ranked bars only.

## Expected widgets table

Each row is a widget the dashboard should contain. The audit grades against this table.

| #   | Widget ID                      | KPI bullet                                          | Type   | Grain      | Required filters                                                                                                                                | Aggregation | Grouping                           | Drilldown report | Notes                                                                                                                                |
| --- | ------------------------------ | --------------------------------------------------- | ------ | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | ---------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | `pipeline_overview_global`     | Pipeline overview with quarterly focus              | metric | global     | `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER`                                                                                       | `sum(ARR)`  | n/a                                | TBD              | Single global number. Calendar quarter, not fiscal.                                                                                  |
| 2   | `pipeline_overview_emea`       | Pipeline overview with quarterly focus              | chart  | per_region | `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `SalesRegion IN ('UKI','Central Europe','Northern Europe','Southwestern Europe')` | `sum(ARR)`  | `Stage`                            | TBD              | Stacked bar by stage, one per EMEA sub-region                                                                                        |
| 3   | `pipeline_overview_nam`        | Pipeline overview with quarterly focus              | chart  | per_region | `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `SalesRegion='North America'`                                                     | `sum(ARR)`  | `Stage`                            | TBD              | Stacked bar by stage                                                                                                                 |
| 4   | `pipeline_overview_apac`       | Pipeline overview with quarterly focus              | chart  | per_region | `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `SalesRegion IN ('APAC','Middle East & Africa')`                                  | `sum(ARR)`  | `Stage`                            | TBD              | Stacked bar by stage                                                                                                                 |
| 5   | `commercial_approval_global`   | Commercial Approval overview (global)               | metric | global     | `Commercial_Approval_Status__c IN ('Approved','Pending')`                                                                                       | `count`     | `Commercial_Approval_Status__c`    | TBD              | Count of approved vs pending, global                                                                                                 |
| 6   | `land_stage3_no_approval_emea` | Commercial Approval - Land Stage 3 missing approval | table  | per_region | `Type='Land'` AND `Stage='3'` AND `Commercial_Approval_Status__c=null` AND `SalesRegion IN (EMEA)`                                              | `count`     | n/a                                | TBD              | List of opps; displayed columns: Account, Opp Name, ARR, Stage entered, Owner                                                        |
| 7   | `land_stage3_no_approval_nam`  | Commercial Approval - Land Stage 3 missing approval | table  | per_region | `Type='Land'` AND `Stage='3'` AND `Commercial_Approval_Status__c=null` AND `SalesRegion='North America'`                                        | `count`     | n/a                                | TBD              | Same columns as EMEA version                                                                                                         |
| 8   | `land_stage3_no_approval_apac` | Commercial Approval - Land Stage 3 missing approval | table  | per_region | `Type='Land'` AND `Stage='3'` AND `Commercial_Approval_Status__c=null` AND `SalesRegion IN ('APAC','Middle East & Africa')`                     | `count`     | n/a                                | TBD              | Same columns as EMEA version                                                                                                         |
| 9   | `renewal_acv_this_quarter`     | Renewals tracking                                   | metric | global     | `Type='Renewal'` AND `CloseDate IN THIS_CALENDAR_QUARTER`                                                                                       | `sum(ACV)`  | n/a                                | TBD              | **ACV, not ARR**                                                                                                                     |
| 10  | `renewal_likelihood`           | Renewals tracking                                   | chart  | global     | `Type='Renewal'` AND `CloseDate IN THIS_CALENDAR_QUARTER`                                                                                       | `sum(ACV)`  | `Probability` bucket               | TBD              | **Open question: what is "likelihood" - opportunity probability, stage-based proxy, or custom field?**                               |
| 11  | `renewal_upcoming_list`        | Renewals tracking                                   | table  | global     | `Type='Renewal'` AND `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER`                                                                  | n/a         | n/a                                | TBD              | Displayed columns: Account, Opp Name, ACV, Close Date, Probability, Owner                                                            |
| 12  | `churn_risk_placeholder`       | Churn Risk and trends                               | metric | global     | n/a                                                                                                                                             | n/a         | n/a                                | TBD              | **Placeholder.** Finance feed is pending from Alex P. Label: "Awaiting Finance feed"                                                 |
| 13  | `slipped_deals_root_cause`     | Slipped deals analysis                              | table  | global     | `CloseDate_Changed__c=true` AND `Previous_CloseDate__c < Current_CloseDate__c`                                                                  | `count`     | `Stage`                            | TBD              | Displayed columns: Account, Opp Name, Previous Close Date, New Close Date, Days slipped, Owner, Root cause (pending - blank for now) |
| 14  | `slipped_deals_trend`          | Slipped deals analysis                              | chart  | global     | `CloseDate_Changed__c=true`                                                                                                                     | `count`     | `Month(CloseDate_Changed_Date__c)` | TBD              | Line chart, trailing 6 calendar months                                                                                               |

**Total expected: 14 widgets.** The live dashboard has 16. The 2 extras, if they exist, are tagged `ORPHAN` by the audit and a decision is needed (keep / drop / fold into spec).

## Implied widget count

| KPI bullet                             | Expected widgets                                                                 | Count  |
| -------------------------------------- | -------------------------------------------------------------------------------- | ------ |
| Pipeline overview with quarterly focus | 1 global metric plus 3 per-region charts                                         | 4      |
| Commercial Approval overview           | 1 global summary plus 3 per-region Land Stage 3 missing-approval lists           | 4      |
| Renewals tracking                      | 1 ACV-this-quarter metric plus 1 likelihood chart plus 1 upcoming-renewals table | 3      |
| Churn risk and trends                  | 1 placeholder (Finance feed pending)                                             | 1      |
| Slipped deals analysis                 | 1 root cause table plus 1 trend chart                                            | 2      |
| **Total expected**                     |                                                                                  | **14** |

## Open questions

These are gaps in the brief. Andre must resolve them before the audit runs, or the spec records the assumption and the audit notes it.

1. **Renewals "likelihood"** - is that opportunity `Probability` (%), a stage-based proxy (`Stage` maps to rough %), or a custom field? _Current assumption: opportunity `Probability`._
2. **Churn placeholder wording** - what does widget 12 say while the Finance feed is pending? _Current assumption: "Awaiting Finance feed (Alex P)"._
3. **Slipped deals root cause column** - opportunity-owner commentary is pending. Until then, is the column blank, "Pending", or dropped? _Current assumption: column present but blank, label "Root cause (pending)"._
4. **"Quarterly focus" scope** - current calendar quarter only, rolling 4 quarters, or current plus next? _Current assumption: current calendar quarter only._
5. **Commercial approval status field name** - the spec uses `Commercial_Approval_Status__c` as a placeholder. _Current assumption: this field exists; if not, the audit will find the actual field during report describe._
6. **Slipped deals field names** - the spec uses `CloseDate_Changed__c`, `Previous_CloseDate__c`, `Current_CloseDate__c`, `CloseDate_Changed_Date__c` as placeholders. _Current assumption: these fields exist in some form; if not, the audit will surface the gap and we fall back to `LastModifiedDate`-based slip detection._
7. **Pipeline "stage" grouping field** - the spec uses bare `Stage` but the org may use `StageName`. _Current assumption: `StageName` is the Salesforce standard field name; `Stage` in the table is shorthand._
8. **SalesRegion field name** - the spec uses bare `SalesRegion`. _Current assumption: there is a `SalesRegion__c` (custom) or `Region__c` field on Opportunity; if not, we fall back to `Account.BillingCountry` mapping._

## Changelog

- 2026-04-06: Initial draft distilled from the KPI brief by Claude during the Phase 1 brainstorm session. 14 expected widgets. 8 open questions at the bottom with current assumptions. Awaiting Andre review.
