# Manual UI Runbook — Sales Director Monthly + Sales Ops Quarterly

Date: 2026-04-08
For: the 5-minute UI handoff that unblocks per-Director scoping, plus the deferred PI list view work and schema change.

Every step below is something the REST API either cannot do or silently refuses to do. The issues are documented in the session handoffs (`feedback_sf_classic_dashboard_lightning_save.md`) and the deep audit.

## Prerequisites

- Logged into `simcorp.my.salesforce.com` as `apro@simcorp.com` (or a user with edit access to both dashboards + Opportunity metadata)
- Dashboards folder: `005QA000003DUwWYAW`
- Dashboard 1 URL: `https://simcorp.lightning.force.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view`
- Dashboard 2 URL: `https://simcorp.lightning.force.com/lightning/r/Dashboard/01ZTb00000FSP9JMAX/view`

## Section 1 — Dashboard 1 filters + running user (5 minutes)

This is the single highest-leverage manual step. Every step is UI-only because the Analytics REST API cannot CREATE dashboard filters and `PATCH canChangeRunningUser` returns 200 but silently ignores the change.

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
   - United States
5. Click **Add**

### Step 1.4 — Add filter 3: Industry (if possible)

**Warning:** the Lightning filter popover for `Account.Industry` shows generic SF industries (Accommodations, Banking, ...) but the actual SimCorp data uses custom values (Asset Management, Pension, Insurance, Bank, Asset Servicer, ...). This is the **Industry picklist mismatch** noted in the handoff.

Options:

- **(A)** Use `Account: Industry` anyway, add the generic picklist values you see, and note that Patrick + Adam's NAM sub-cuts will not work cleanly.
- **(B)** Skip this filter; address after the Industry investigation (see §Section 3).

If you choose (A):

1. Click **+ Filter**
2. Filter field: `Account: Industry`
3. Values: whatever is shown (Asset Management, Pension, Insurance, Bank, Wealth Management if present, Asset Servicer, Other)
4. Click **Add**

### Step 1.5 — Flip `canChangeRunningUser`

1. Click the **gear icon** (Edit Dashboard Properties)
2. Under "View Dashboard As", select **The dashboard viewer** (equivalent to `LoggedInUser`)
3. Check the box: **Let dashboard viewers choose whom they view the dashboard as** (this is the `canChangeRunningUser = true` flag)
4. Click **Save** inside the properties modal

### Step 1.6 — Save the dashboard

1. Click **Save** in the top toolbar (NOT "Save As")
2. Click **Done** to exit edit mode

**⚠️ Known issue:** the Save button on SF Classic dashboards running in Lightning Experience can silently fail under browser automation (Playwright, Puppeteer, CDP click simulation). Manual click works. If the Save button appears to do nothing on manual click, reload the page and try again — this has been observed once per session occasionally.

### Step 1.7 — Verify

1. Reload `https://simcorp.lightning.force.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view`
2. Confirm 2 or 3 filters appear in the top filter bar (depending on whether you added Industry)
3. Confirm there's a "View As" dropdown in the top-right (the running-user selector)
4. Try applying **Sales Region = APAC** → verify Jesper Tyrer's pipeline slice appears

### The 9 Director preset filter combos

Once filters exist, the 9 Directors each open the dashboard and apply their combo:

| Director                       | Filter combo                                                                                       |
| ------------------------------ | -------------------------------------------------------------------------------------------------- |
| Jesper Tyrer (APAC)            | Sales Region = APAC                                                                                |
| Sarah Pittroff (CE)            | Sales Region = Central Europe                                                                      |
| Francois Thaury (SE)           | Sales Region = Southwestern Europe                                                                 |
| Dan Peppett (UK&I)             | Sales Region = United Kingdom & Ireland                                                            |
| Christian Ebbesen (NL&Nordics) | Sales Region = Northern Europe                                                                     |
| Mourad Essofi (MEA)            | Sales Region = Middle East & Africa                                                                |
| Megan Miceli (Canada)          | Legal Country = Canada                                                                             |
| Patrick Gaughan (NA rest)      | Sales Region = North America AND Legal Country = United States                                     |
| Adam Steinhaus (NA P&I)        | Sales Region = North America AND Legal Country = United States AND Industry ∈ (Pension, Insurance) |

Note: Patrick and Adam will be indistinguishable until the Industry filter is correctly set up (see §Section 3).

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

## Section 3 — Industry picklist investigation (~30 minutes)

Context: `Account.Industry` in the Lightning filter popover shows generic SF standard industries (Accommodations, Banking, Construction, ...) but the actual SimCorp data contains custom values (Asset Management, Pension, Insurance, Bank, Asset Servicer, ...). Need to identify which field holds the SimCorp values.

### Investigation steps

1. SOQL: list all Industry-shaped fields on Account:
   ```bash
   sf data query --query "SELECT QualifiedApiName, Label FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName = 'Account' AND (QualifiedApiName LIKE '%ndustry%' OR Label LIKE '%ndustry%')" --target-org apro@simcorp.com --json
   ```
2. For each candidate field (expect: `Industry`, `ZIMIT__zIndustry__c`, `Industry_Classification__c`, or similar), run a distinct-values query:
   ```bash
   sf data query --query "SELECT <field>, COUNT(Id) FROM Account WHERE <field> != NULL GROUP BY <field> LIMIT 50" --target-org apro@simcorp.com --json
   ```
3. The field whose distinct values contain "Asset Management", "Pension", "Insurance", "Bank", "Asset Servicer", "Wealth Management", "Other" is the one the Directors care about.
4. Once identified, **update the Dashboard 1 filter** in the Lightning UI: remove the generic `Account: Industry` filter (if you added it per §1.4 Option A) and replace with a filter on the correct field.
5. **Update `report-1-source-contract.md`** §Phase 2.8 amendment §9 Sales Directors table with the correct Industry filter field name.

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
