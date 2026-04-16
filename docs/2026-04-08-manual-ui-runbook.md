# Manual UI Runbook — Sales Director Monthly + Sales Ops Quarterly

Date: 2026-04-08
Updated: 2026-04-09
For: the UI-only dashboard / report work the REST API cannot do. Section 1 was completed live on 2026-04-09 and is now the recovery/edit procedure for D1 rather than an unmet handoff.

Every step below is something the REST API either cannot do or silently refuses to do. The issues are documented in the session handoffs (`feedback_sf_classic_dashboard_lightning_save.md`) and the deep audit.

## Prerequisites

- Logged into `simcorp.my.salesforce.com` as `apro@simcorp.com` (or a user with edit access to both dashboards + Opportunity metadata)
- Dashboards folder: `005QA000003DUwWYAW`
- Dashboard 1 URL: `https://simcorp.lightning.force.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view`
- Dashboard 2 URL: `https://simcorp.lightning.force.com/lightning/r/Dashboard/01ZTb00000FSP9JMAX/view`

## Section 1 — Dashboard 1 filters + running user (current live state + recovery procedure)

D1 now has 4 saved native dashboard filters. Use this section if you need to recreate them, change their value lists, or recover from a future save/regression. Filter creation remains UI-only because the Analytics REST API cannot CREATE dashboard filters and `PATCH canChangeRunningUser` still returns 200 but silently ignores the change.

### Step 1.1 — Open Dashboard 1 in edit mode

1. Navigate to `https://simcorp.lightning.force.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view`
2. Click the **Edit** button (top-right toolbar)
3. If prompted "Convert this dashboard to Lightning?", click **Convert** and confirm

### Step 1.2 — Add filter 1: Sales Region

1. Click **+ Filter** in the top toolbar
2. Filter field: `Opportunity: Sales Region` (the `Opportunity.Sales_Region__c` field)
3. Display label: `Sales Region`
4. Values to add (check all):
   - APAC
   - Central Europe
   - Northern Europe
   - Southwestern Europe
   - United Kingdom & Ireland
   - Middle East & Africa
   - North America
5. Click **Add**

### Step 1.3 — Add filter 2: Legal Country

1. Click **+ Filter** again
2. Filter field: `Account: Legal Country` (this is the field exposed in the Opportunity report type; internally `ADDRESS1_COUNTRY_CODE` under the Account object, but shown as "Legal Country" in the UI)
3. Display label: `Legal Country`
4. Values to add:
   - Canada
   - Exclude Canada
5. Click **Add**

### Step 1.4 — Add filter 3: Industry

The correct live field is the standard `Account: Industry` filter. This was verified on 2026-04-09; the custom `ZIMIT__zIndustry__c` field is not the right one for the D1 operating model.

1. Click **+ Filter**
2. Filter field: `Account: Industry`
3. Values:
   - Asset Management
   - Bank
   - Insurance
   - Pension
   - Wealth Management
   - Asset Servicer
   - Other
4. Click **Add**

### Step 1.5 — Add filter 4: Account Unit Group

1. Click **+ Filter**
2. Filter field: `Opportunity: Account Unit Group` (`Opportunity.Account_Unit_Group__c`)
3. Display label: `Account Unit Group`
4. Values:
   - SC North America
   - SC Asia
   - SC EMEA
5. Click **Add**

### Step 1.6 — Optional: flip `canChangeRunningUser`

1. Click the **gear icon** (Edit Dashboard Properties)
2. Under "View Dashboard As", select **The dashboard viewer** (equivalent to `LoggedInUser`)
3. Check the box: **Let dashboard viewers choose whom they view the dashboard as** (this is the `canChangeRunningUser = true` flag)
4. Click **Save** inside the properties modal

### Step 1.7 — Save the dashboard

1. Click **Save** in the top toolbar (NOT "Save As")
2. Click **Done** to exit edit mode

**⚠️ Known issue:** this save path was flaky during earlier classic→Lightning automation attempts, but the 2026-04-09 filter/layout pass did persist successfully once the dashboard was already in the Lightning editor. Manual click is still the safe fallback if automation stalls.

### Step 1.8 — Verify

1. Reload `https://simcorp.lightning.force.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view`
2. Confirm 4 filters appear in the top filter bar: `Industry`, `Legal Country`, `Sales Region`, `Account Unit Group`
3. If you changed running-user mode, confirm there's a "View As" dropdown in the top-right
4. Try applying **Sales Region = APAC** + **Account Unit Group = SC Asia** → verify Jesper Tyrer's pipeline slice appears

### The 9 Director preset filter combos

Once filters exist, the 9 Directors each open the dashboard and apply their combo:

| Director                       | Filter combo                                                                                                                     |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| Jesper Tyrer (APAC)            | Sales Region = APAC AND Account Unit Group = SC Asia                                                                             |
| Sarah Pittroff (CE)            | Sales Region = Central Europe AND Account Unit Group = SC EMEA                                                                   |
| Francois Thaury (SE)           | Sales Region = Southwestern Europe AND Account Unit Group = SC EMEA                                                              |
| Dan Peppett (UK&I)             | Sales Region = United Kingdom & Ireland AND Account Unit Group = SC EMEA                                                         |
| Christian Ebbesen (NL&Nordics) | Sales Region = Northern Europe AND Account Unit Group = SC EMEA                                                                  |
| Mourad Essofi (MEA)            | Sales Region = Middle East & Africa AND Account Unit Group = SC EMEA                                                             |
| Megan Miceli (Canada)          | Sales Region = North America AND Legal Country = Canada AND Account Unit Group = SC North America                                |
| Patrick Gaughan (NA rest)      | Sales Region = North America AND Legal Country = Exclude Canada AND Account Unit Group = SC North America                       |
| Adam Steinhaus (NA P&I)        | Sales Region = North America AND Legal Country = Exclude Canada AND Account Unit Group = SC North America AND Industry ∈ (Pension, Insurance) |

If the dashboard UI only allows one Industry value at a time for Adam, use 2 saved states or bookmarks: one for `Pension`, one for `Insurance`.

## Section 2 — Pipeline Inspection list views (manual, ~15 minutes)

Required for the 4 forecast accuracy widgets on Dashboard 2 + the slipped deals widgets on Dashboard 1.

### PI list view 1 — Slipped Deals (Pipeline Changes with Slipped filter)

Context: `PipelineInspectionListView` in the SOQL schema has no `SlipFilter` field. The "Pipeline Changes" view with "Slipped" change type is a Lightning UI-only construct.

1. Open Pipeline Inspection in Lightning: **App Launcher → Pipeline Inspection**
2. Click **New List View** (or the "+ List View" button)
3. Name: `Slipped Deals — Current Quarter`
4. API Name: `Slipped_Deals_Current_Quarter`
5. Filter settings:
   - **Change Type:** Slipped (specific to Pipeline Changes view)
   - **Change Period:** `Start of the period` (or whatever maps to "since start of the quarter")
   - **Date Literal:** `THIS_QUARTER` on Close Date
6. Visible columns: Opportunity Name, Account Name, Sales Region, Close Date, Amount (or ARR), Previous Close Date, Change Reason
7. Sharing: Everyone (or at least the 9 named Directors + Sales Ops)
8. Save

### PI list view 2 — 26-week forecast change window

Context: PI's `ChangePeriodLiteralType` enum in this org currently has only `START_OF_THE_PERIOD` and `THIS_WEEK`. A 26-week (6-month) trailing window requires a custom configuration.

1. Open Pipeline Inspection: same entry as above
2. New List View
3. Name: `Forecast Change Volatility — 26 Weeks`
4. Change Period: if `Last 26 Weeks` or `Last 6 Months` is available, pick it; otherwise document the gap and flag to Salesforce support
5. Summary field: ARR
6. Save

## Section 3 — Industry field verification (completed 2026-04-09)

Result from the live continuation:

1. The correct D1 filter field is the standard `Account: Industry` field.
2. The live D1 filter values are the SimCorp values the Directors actually use: `Asset Management`, `Bank`, `Insurance`, `Pension`, `Wealth Management`, `Asset Servicer`, `Other`.
3. The custom `ZIMIT__zIndustry__c` path is not the field to use for the dashboard operating model.

If this ever drifts and you need to re-probe, the old SOQL workflow is still valid:

```bash
sf data query --query "SELECT QualifiedApiName, Label FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName = 'Account' AND (QualifiedApiName LIKE '%ndustry%' OR Label LIKE '%ndustry%')" --target-org apro@simcorp.com --json
```

## Section 4 — Calendar-quarter grouping schema change (metadata deploy)

Context: the Reports API cannot accept `CALENDAR_QUARTER` or `CLOSE_DATE_CALENDAR_QUARTER` as grouping column names (verified 2026-04-08). 4 widgets are blocked on this:

- D1 `00OTb000008ekxBMAQ` — Renewal ACV by Quarter
- D1 `00OTb000008TZsDMAW` — Forecast Accuracy
- D1 `00OTb000008eksLMAQ` — Renewals by Quarter
- D2 `00OTb000008SrmLMAS` — Overdue Opportunities

### Option A — per-report bucket field (cheapest)

For each of the 4 reports:

1. Open the report in Lightning (`/lightning/r/Report/<id>/view` → Edit)
2. Remove the existing `Fiscal Quarter` grouping
3. Click **Add Bucket Column** (under the **Outline** or **Fields** panel)
4. Source field: `CloseDate`
5. Bucket definition: quarters defined by calendar month ranges (Q1 = Jan-Mar, Q2 = Apr-Jun, Q3 = Jul-Sep, Q4 = Oct-Dec), year component from `CloseDate`. E.g.:
   - `2026-Q1`: CloseDate in January 2026, February 2026, March 2026
   - `2026-Q2`: CloseDate in April 2026, May 2026, June 2026
   - ... etc.
6. Add the bucket column as the new grouping row
7. Save

### Option B — custom formula field on Opportunity (more reusable)

Create a custom formula text field `Calendar_Quarter__c` on Opportunity:

```
TEXT(YEAR(CloseDate)) & "-Q" & TEXT(CEILING(MONTH(CloseDate) / 3))
```

Returns strings like `2026-Q1`, `2026-Q2`, etc.

Deploy via Metadata API or sfdx (not the UI — formula fields are metadata, not click-configurable for complex cases).

Then update each of the 4 reports to replace the `FISCAL_QUARTER` grouping with `Calendar_Quarter__c`. This can be done via the Reports API PATCH once the field exists.

**Recommendation:** use Option B. One-time metadata deploy, reusable across every future report that needs calendar-quarter grouping, and PATCHable after that.

## Section 5 — Stakeholder decisions pending

These are not UI clicks — they're conversations that need to happen before certain widgets can progress:

### D1 — Churn risk Finance feed (Alex P)

- Current state: `Business At Risk` widget is a CRM-side proxy on `Risk_Assessment_Level__c`
- Pending: Finance feed from Alex P — who in Finance produces this, what's the data shape, how does it land in SF or CRMA
- Action: reach out to Alex P, ask which Finance team member produces churn risk, get a recurring feed set up

### D2 — `dq_missing_quote_type` retire vs repurpose

- Current state: `APTS_Primary_Quote_Type__c` field has zero active picklist values (empty after migration)
- Decision needed: (a) retire the widget entirely, or (b) replace with a widget grading the canonical `Type` field (Land/Expand/Renewal)
- Action: ask Sales Ops product team

### D2 — `ph_probability_mismatch_by_stage` threshold

- Current state: `Under Construction` marker on live widget
- Decision needed: per-stage probability thresholds from Sales Handbook V4 slide 17
- Action: ask Sales Ops

## Section 6 — Cumulative-changes notifications (external)

Stakeholder mentioned: Sales Directors should get alerts on cumulative changes to their book. This is not a dashboard widget — it's a notification workflow. Out of scope for the dashboards themselves; tracked separately.

## Verification

After completing §1-§4, run this as a sanity check:

```bash
cd /Users/test/crm-analytics
node /tmp/full-state-dump.mjs  # script from the 2026-04-08 session — may need to be re-parameterized
```

A "pristine" pass is 0 flags across both dashboards. After the schema change in §4, all 4 `fiscal-grouping` flags clear; all other flags should already be clear as of this session's API pass.

## Contact for issues

Issues with any step: escalate to the session handoff author (Andre) or to `apro@simcorp.com`.
