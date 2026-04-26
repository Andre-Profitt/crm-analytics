# Historical Trending Report Builder Handoff

## Objective

Create a Salesforce "Opportunities with Historical Trending" report that compares pipeline snapshots across 3 dates:

- **2026-01-01** — Q1 Open (what pipeline looked like at start of Q1)
- **2026-03-31** — Q1 Close (end of Q1)
- **2026-04-13** — Current state

This lets you see deal-by-deal: what stage was it in, what was the close date, what was the amount — at each point in time. You can immediately identify Q1 slips, close date pushes, stage changes, and dollar movement.

## Why API Can't Do This

The Historical Trending report type (`Opportunities_with_Historical_Trending__c`) requires the Lightning Report Builder UI. The Analytics REST API:

- Returns 0 columns on `GET /analytics/report-types/.../describe`
- Rejects all standard column names (`OPPORTUNITY_NAME`, `ACCOUNT_NAME`, `STAGE_NAME`, etc.) when POSTing
- Requires `historicalSnapshotDates` array but then can't accept any detail columns

This is a known Salesforce limitation — Historical Trending reports must be built interactively.

## Step-by-Step Build Instructions

### 1. Navigate

- Go to Salesforce Lightning → **Reports** tab
- Click **New Report**

### 2. Select Report Type

- In the search box, type **"Historical Trending"**
- Select **"Opportunities with Historical Trending"**
- Click **Start Report**

### 3. Set Snapshot Dates

- In the report builder, find the **Historical Snapshots** section (usually at the top or in the filters panel)
- Add these 3 snapshot dates:
  - `2026-01-01`
  - `2026-03-31`
  - `2026-04-13`

### 4. Add Columns

Add these columns (each will automatically generate historical versions for each snapshot date):

| Column                                                 | Why                                            |
| ------------------------------------------------------ | ---------------------------------------------- |
| **Opportunity Name**                                   | Identify the deal                              |
| **Account Name**                                       | Account context                                |
| **Owner**                                              | Who owns it                                    |
| **Stage**                                              | See stage progression/regression between dates |
| **Close Date**                                         | The key field — see close date pushes          |
| **Amount** (or `APTS_Opportunity_ARR__c` if available) | Dollar impact of changes                       |
| **Forecast Category**                                  | Commit/Best Case/Pipeline movement             |
| **Type**                                               | Land/Expand/Renewal                            |
| **Probability**                                        | Probability changes                            |
| **Account Unit Group**                                 | For regional filtering                         |

Once added, the builder should show columns like:

- `Close Date` (current)
- `Close Date as of 2026-01-01`
- `Close Date as of 2026-03-31`

This is the snapshot comparison view.

### 5. Add Filters

| Filter          | Operator         | Value                   |
| --------------- | ---------------- | ----------------------- |
| Account Name    | does not contain | `simcorp, test, delete` |
| Type            | equals           | `Land, Expand, Renewal` |
| Owner Full Name | does not contain | `Sabiniewicz, Profit`   |

Optional: Add `Account Unit Group = SC Asia` for APAC-only view, or leave global and filter at runtime.

### 6. Add Grouping

- Group by **Stage** (to see pipeline by stage at each snapshot)
- Optionally add a second grouping by **Account Unit Group** for regional breakdown

### 7. Save

- **Name**: `SD Q1 Pipeline Snapshot Comparison`
- **Folder**: `Sales KPI Tracking` (folder ID: `00lD0000001PccnIAC`)
- **Description**: "Compare pipeline at Q1 open (Jan 1) vs Q1 close (Mar 31) vs current. Shows close date pushes, stage changes, and dollar movement."

### 8. Verify

Once saved, the report should show a table where each row is an opportunity with its values at all 3 dates. Key things to look for:

- **Close Date as of Jan 1** = `2026-03-15` but **Close Date** (current) = `2026-09-30` → deal slipped Q1
- **Stage as of Jan 1** = `3 - Engagement` but **Stage as of Mar 31** = `0 - Lost` → deal lost in Q1
- **Amount as of Jan 1** = `€2M` but **Amount** = `€3M` → deal value increased

### 9. Wire to Dashboard (Optional)

If you want this on the Sales Directors Monthly dashboard (`01ZTb00000FSP7hMAH`):

- Edit the dashboard in Lightning
- Add Component → select this report
- Choose FlexTable visualization
- The dashboard's existing filters (Sales Region, Account Unit Group, Industry, Legal Country) will cascade to it

## Report ID Tracking

Once created, record the report ID here for reference:

- **Report ID**: `________________`
- **URL**: `https://simcorp.lightning.force.com/lightning/r/Report/________________/view`

## What We Already Have (SOQL-based alternative)

The live ETL pipeline already captures Q1 movement data via `OpportunityFieldHistory` SOQL:

- **Excel**: `output/director_live_workbooks/2026-04-13/jesper-tyrer.xlsx` → "Q1 Movement" sheet
- **Deck**: Slide 6 "Q1 Movement" shows top 12 deals by ARR with old/new close dates
- **Data**: 47 Q1 slips (EUR 54.1M) + 15 post-Q1 pushes (EUR 12.3M) for APAC

The SF report gives the native Salesforce view of the same data, plus the ability to see the actual values at each snapshot date (not just the change events).

## Org Details

- **Org**: `apro@simcorp.com`
- **Instance**: `simcorp.my.salesforce.com`
- **API version**: v66.0
- **Dashboard folder**: Sales KPI Tracking (`00lD0000001PccdIAC`)
- **Report folder**: Sales KPI tracking (`00lD0000001PccnIAC`)
