---
title: Sales Directors Monthly Pipeline & Insights — Dashboard Redesign Runbook
type: runbook
audience: operator (UI)
target: 01ZTb00000FSP7hMAH (canonical) — but build v2 as a sibling first
prep_time: ~90 minutes
---

# Runbook — SD Monthly Dashboard v2 (UI execution)

You're rebuilding the Lightning dashboard at **Sales KPI Tracking > Sales Directors Monthly Pipeline and Insights** (`01ZTb00000FSP7hMAH`). Don't touch the canonical dashboard while building — clone first, validate, then decide whether to swap.

## Principles

- **Build in your personal folder first.** Clone the existing dashboard to the `Andre` folder, validate there, then move to Sales KPI Tracking when it lands cleanly.
- **Reuse existing reports where possible.** The folder already has the reports we need for most components; only 4-5 reports are genuinely new.
- **Director-centric filter first.** The single biggest UX win: add a `Director` (Owner.Name picklist) filter so a director scopes to their own book in one click instead of compound Region + Unit Group.
- **KPI strip at the top.** Big numbers first, diagnostic second, action tables last.

---

## Step 0 — Clone the canonical dashboard

1. Open `01ZTb00000FSP7hMAH` in the Lightning app.
2. Click the gear → **Save As** → Name: `Sales Directors Monthly Pipeline and Insights — v2`, folder: `Andre`. Save.
3. Open the new clone in the editor. You'll work from here.

---

## Step 1 — Verify / create the reports you'll need

Check each report exists in **Sales KPI Tracking**. If a row says "✓ exists", you're set; if "NEW", create it with the spec given.

| Status | Report label                            | ID (if exists)       | Specification                                                                                                                                                                                                                                                                  |
| ------ | --------------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| ✓      | SD Pipeline Open FY26                   | `00OTb000008fzirMAA` | Used by KPI cards + funnel                                                                                                                                                                                                                                                     |
| ✓      | SD Won Lost FY26                        | `00OTb000008fzkTMAQ` | Used by Q1 Won/Lost KPI + Win Rate                                                                                                                                                                                                                                             |
| ✓      | SD Top Deals FY26                       | `00OTb000008fzm5MAA` | Top N deals table                                                                                                                                                                                                                                                              |
| ✓      | Pipeline Global CFQ                     | `00OTb000008fBfdMAE` | Existing funnel                                                                                                                                                                                                                                                                |
| ✓      | Close Date Slipped YTD                  | `00OTb000008eknVMAQ` | Slip analysis                                                                                                                                                                                                                                                                  |
| ✓      | Renewals By Stager CFQ                  | `00OTb000008fBULMA2` | Renewals                                                                                                                                                                                                                                                                       |
| ✓      | SD Win Rate by Stage                    | `00OTb000008gUrVMAU` | Win rate chart                                                                                                                                                                                                                                                                 |
| ✓      | SD Days in Stage                        | `00OTb000008gUt7MAE` | Stage age                                                                                                                                                                                                                                                                      |
| ✓      | Commercial Approval Candidates          | `00OTb000008ekp7MAA` | Approvals                                                                                                                                                                                                                                                                      |
| ✓      | Commercial Approval Global              | `00OTb000008fBEDMA2` | Approvals donut                                                                                                                                                                                                                                                                |
| ✓      | Commercial Approval Approved YTD (Land) | `00OTb000008aTtJMAU` | Approvals approved table                                                                                                                                                                                                                                                       |
| ✓      | Won FY26                                | `00OTb000008gHZJMA2` | Closed won list                                                                                                                                                                                                                                                                |
| ✓      | Business At Risk (Renewals)             | `00OTb000008Ta9xMAC` | Renewal risk                                                                                                                                                                                                                                                                   |
| ✓      | High Value Stale Deals                  | `00OTb000008Ti97MAC` | Existing deal-risk proxy                                                                                                                                                                                                                                                       |
| ✓      | Active Opps: No Activity                | `00OTb000008fAmnMAE` | Silent deal signal                                                                                                                                                                                                                                                             |
| ✓      | Aging Pipeline 365 Plus Days            | `00OTb000008Ti7VMAS` | Old pipeline                                                                                                                                                                                                                                                                   |
| ✓      | Land: No Approval Flow                  | `00OTb000008fAlBMAU` | Approval gap                                                                                                                                                                                                                                                                   |
| ✓      | Mid-Stage: No NextStep                  | `00OTb000008fAjZMAU` | Activity hygiene                                                                                                                                                                                                                                                               |
| NEW    | **SD Pushed Deals FY26**                | —                    | Land + Q1-Q2 close + PushCount >= 3 + IsClosed = false. Columns: Opportunity, Account, Owner, Stage, Close Date, PushCount, ARR Unwtd. Grouping: Owner. Sort: PushCount desc. Needed for coaching block.                                                                       |
| NEW    | **SD Q1 Loss Reasons**                  | —                    | Opportunity + FY26 Q1 close date + Type = Land + StageName = '0 - Lost' or '0 - No Opportunity'. Columns: Reason_Won_Lost**c, ARR Unwtd, Opportunity, Account, Owner. Grouping: Reason_Won_Lost**c. Summary: COUNT + SUM(ARR). Feeds the Loss Reasons donut.                   |
| NEW    | **SD Competitive Win/Loss**             | —                    | Opportunity + FY26 + Type IN (Land, Expand) + IsClosed = true + Lost_to_Competitor**c != null. Columns: Lost_to_Competitor**c, Stage (Won/Lost), ARR Unwtd. Grouping: Lost_to_Competitor\_\_c, then Stage. Feeds the competitor chart.                                         |
| NEW    | **SD Account Penetration (Multi-Opp)**  | —                    | Opportunity + IsClosed = false + Type = Land + FY26. Columns: Account.Name, COUNT(Id), SUM(ARR Unwtd). Grouping: Account.Name. Filter: COUNT(Id) >= 2. Sort: COUNT desc. Surfaces cross-sell.                                                                                  |
| NEW    | **SD Deals at Risk (Score proxy)**      | —                    | Opportunity + Land + IsClosed = false + Q1-Q2 close AND (PushCount >= 3 OR LastActivityDate < TODAY-60). Columns: Opportunity, Account, Owner, Stage, Close Date, PushCount, ARR Unwtd, LastActivityDate. Sort: PushCount desc. SF-native proxy for the Deal Risk Scoring tab. |

**Create the 5 NEW reports** using Report Builder. Report type: `Opportunities` (for all five). Save each to **Sales KPI Tracking** folder so the dashboard editor can see them.

Tip: duplicate an existing report that's structurally close (e.g. clone `Close Date Slipped YTD` to start a new Opportunity-based one) and adjust filters — saves 10+ min per report.

---

## Step 2 — Lay out the v2 dashboard

Dashboard grid in Lightning is 12 columns wide. Recommended sizes below are column × row counts.

### Row 1 — KPI strip (5 components, 1 row tall each, ~2 cols wide)

Lightning "Metric" chart type, each fed by a Report + summary field.

| #   | Title                     | Source report                        | Metric                         | Size |
| --- | ------------------------- | ------------------------------------ | ------------------------------ | ---- |
| 1   | Open Land ARR             | SD Pipeline Open FY26                | `SUM(APTS_Opportunity_ARR__c)` | 2×1  |
| 2   | Q1 Won ARR                | SD Won Lost FY26 (filter: Q1 + Won)  | `SUM(APTS_Opportunity_ARR__c)` | 2×1  |
| 3   | Q1 Lost ARR               | SD Won Lost FY26 (filter: Q1 + Lost) | `SUM(APTS_Opportunity_ARR__c)` | 2×1  |
| 4   | Deals Pushed 3+           | SD Pushed Deals FY26                 | `COUNT(Id)`                    | 2×1  |
| 5   | Missing Stage 3+ Approval | Land: No Approval Flow               | `COUNT(Id)`                    | 2×1  |
| —   | (optional 12th col)       | RichText: "Snapshot: {today}"        | static                         | 2×1  |

Styling: use a color convention — **blue** for pipeline, **green** for wins, **red** for losses/risk, **amber** for approvals pending.

### Row 2 — Momentum (3 components)

| #   | Title             | Source               | Viz type            | Notes           | Size |
| --- | ----------------- | -------------------- | ------------------- | --------------- | ---- |
| 6   | Pipeline by Stage | Pipeline Global CFQ  | **Funnel**          | Existing. Keep. | 4×3  |
| 7   | Win Rate by Stage | SD Win Rate by Stage | **Bar, horizontal** | Existing. Keep. | 4×3  |
| 8   | Avg Days in Stage | SD Days in Stage     | **Bar, horizontal** | Existing. Keep. | 4×3  |

Two-and-a-half-month trend lines aren't currently available natively. Leave velocity out of this dashboard; it lives in the Excel workbook.

### Row 3 — Diagnostics (3 components)

| #   | Title                | Source                         | Viz type        | Notes                         | Size |
| --- | -------------------- | ------------------------------ | --------------- | ----------------------------- | ---- |
| 9   | Top Deals at Risk    | SD Deals at Risk (Score proxy) | **FlexTable**   | New report, Q1-Q2 scope       | 6×4  |
| 10  | Q1 Loss Reasons      | SD Q1 Loss Reasons             | **Donut**       | New report; 8 wedges          | 3×4  |
| 11  | Competitive Win/Loss | SD Competitive Win/Loss        | **Stacked Bar** | New report; stack on Won/Lost | 3×4  |

### Row 4 — Coaching (3 components)

| #   | Title                         | Source                           | Viz type      | Notes                               | Size |
| --- | ----------------------------- | -------------------------------- | ------------- | ----------------------------------- | ---- |
| 12  | Deals Pushed by Owner         | SD Pushed Deals FY26             | **FlexTable** | Group by Owner, sort PushCount desc | 4×4  |
| 13  | Deals with No Next Step       | Mid-Stage: No NextStep           | **FlexTable** | Existing                            | 4×4  |
| 14  | Active Opps, No Activity 30d+ | No Activity 30+ Days - Open Opps | **FlexTable** | Existing                            | 4×4  |

### Row 5 — Accounts + renewals (3 components)

| #   | Title                          | Source                             | Viz type      | Notes      | Size |
| --- | ------------------------------ | ---------------------------------- | ------------- | ---------- | ---- |
| 15  | Cross-sell: Multi-Opp Accounts | SD Account Penetration (Multi-Opp) | **FlexTable** | New report | 4×4  |
| 16  | Renewals Due This Quarter      | Renewal Pipeline This Quarter      | **FlexTable** | Existing   | 4×4  |
| 17  | Business At Risk (Renewals)    | Business At Risk (Renewals)        | **Bar**       | Existing   | 4×4  |

### Row 6 — Closed & approvals (3 components)

| #   | Title                               | Source                                  | Viz type      | Notes    | Size |
| --- | ----------------------------------- | --------------------------------------- | ------------- | -------- | ---- |
| 18  | Closed Won FY26                     | Won FY26                                | **FlexTable** | Existing | 4×4  |
| 19  | Commercial Approval — Approved 2026 | Commercial Approval Approved YTD (Land) | **FlexTable** | Existing | 4×4  |
| 20  | Approval Pipeline State             | Commercial Approval Global              | **Donut**     | Existing | 4×4  |

Drop the duplicate "Close Date Slipped YTD" component (it's on the canonical twice).

---

## Step 3 — Filters

In the Lightning dashboard editor, **Edit Dashboard Filters** (top-right gear on the canvas).

Configure 3 filters, **in this order** so directors see the most-useful one first:

| #   | Field                               | Operator          | Display                | Notes                                                                                                                                                                                                                         |
| --- | ----------------------------------- | ----------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `Opportunity.Owner.Name`            | equals (picklist) | **Director**           | The 1-click "it's about me" filter. Pre-populate with the 9 MD-1 director names: Jesper Tyrer, Sarah Pittroff, Dan Peppett, Christian Ebbesen, Francois Thaury, Mourad Essofi, Patrick Gaughan, Megan Miceli, Adam Steinhaus. |
| 2   | `Opportunity.Account_Unit_Group__c` | equals (picklist) | **Account Unit Group** | Existing                                                                                                                                                                                                                      |
| 3   | `Opportunity.Sales_Region__c`       | equals (picklist) | **Sales Region**       | Existing                                                                                                                                                                                                                      |

Remove Industry and Legal Country filters — they're cross-cutting slices that directors use rarely and clutter the filter bar.

Each component that's scoped to an individual (pushed deals, deals at risk, stale, missing activity) should have the Director filter mapped to `Opportunity.Owner.Name` in the component's "Filters" → "Linked dashboard filters" panel. That's what makes the filter actually scope everything at once.

---

## Step 4 — Styling + description

- **Dashboard title**: `Sales Directors Monthly — Pipeline & Insights v2`
- **Description** (visible on Sales KPI Tracking folder): "Monthly sales director review. Filter by Director (top-left) for your own book. 20 components across KPI / momentum / diagnostics / coaching / accounts / approvals."
- **Color palette**: `Corporate` (matches other SimCorp dashboards)
- **Chart theme**: Wave (default). Don't try to match the PowerPoint template — they're different media.

---

## Step 5 — Pre-ship checks

Before moving to Sales KPI Tracking:

- [ ] All 20 components render data for at least one Director value (pick any director, verify)
- [ ] Filter each director in turn (9 directors), confirm KPI values are non-zero and differ per director
- [ ] Remove the old "Close Date Slipped YTD" duplicate if it made it across in the clone
- [ ] Verify Row 1 KPI values match the Excel workbook's `Summary` tab row for the same director (tie-out sanity)
- [ ] Check mobile view — Lightning dashboards reflow; KPI strip should stack vertically, not wrap mid-row

---

## Step 6 — Ship

1. **Save As** → overwrite `Sales Directors Monthly Pipeline and Insights — v2` in Andre folder with your final version.
2. Open the canonical `01ZTb00000FSP7hMAH`. Click **Save As** with identical name but target `Sales KPI Tracking` → rename the current one to `Sales Directors Monthly Pipeline and Insights (v1 archive)`.
3. Save your v2 into `Sales KPI Tracking` with name `Sales Directors Monthly Pipeline and Insights`.
4. Notify the 9 directors via email (template: "New filter — pick your name from the Director dropdown; reach out if anything looks off").

---

## Reference — reports you added

When you create the 5 new reports, capture their Ids here for the next runbook:

```
SD Pushed Deals FY26           →  [paste id]
SD Q1 Loss Reasons             →  [paste id]
SD Competitive Win/Loss        →  [paste id]
SD Account Penetration (Multi) →  [paste id]
SD Deals at Risk (Score proxy) →  [paste id]
```

---

## Fallback — if Lightning dashboard editor gets stuck

Per the team's memory (`feedback_sf_classic_dashboard_lightning_save.md`), Classic dashboard saves can hang in the Lightning editor. If Save never fires:

1. Export the dashboard via **Setup → Metadata API** (`Dashboard` type, `FolderName/DashboardName.dashboard`).
2. Edit the XML locally (each component is a `<dashboardComponent>` block).
3. Re-deploy via `sf project deploy start -x manifest/sd-dashboard-v2.package.xml`.

This is slower but works around the editor bug.
