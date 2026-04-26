---
title: Sales Ops Quarterly KPI Dashboard — Hygiene Integration Runbook
type: runbook
target: 01ZTb00000FSP9JMAX (Sales KPI Tracking > Sales Ops Quarterly KPI Dashboard)
prep_time: ~45 minutes
dependencies: scripts/audit_data_quality.py (stage 1c of the monthly pipeline)
---

# Sales Ops Quarterly KPI Dashboard — Integration Runbook

The dashboard at `01ZTb00000FSP9JMAX` already covers most hygiene signals. This runbook tightens it + integrates the new data-quality audit output + closes the three weaknesses: no KPI cards, duplicate tiles, no trend.

## Current state (baseline)

15 components, no KPI cards, 3 pairs of duplicates:

- #4 Active Opps: No Activity == #15 (same report)
- #8 Aging Pipeline 365+ == #9 (same report)
- #10 Overdue Opportunities (bar) + #11 (flex table) (same report, intentional?)

## Target state

22 components across 4 rows: KPI strip → Pipeline hygiene → Accounts hygiene → Closed-deal hygiene. Each non-zero count is actionable; each box maps to a fix path.

## Step 0 — Clone and back up

1. Open `01ZTb00000FSP9JMAX`.
2. Save As → **Sales Ops Quarterly KPI Dashboard — v2** in your `Andre` folder.
3. Work from the clone.

## Step 1 — Drop the duplicates

Open the v2 clone. Delete these components:

- **Remove:** #15 "Active Opps: No Activity" (duplicate of #4)
- **Remove:** #9 "Aging Pipeline 365 Plus Days" (duplicate of #8)
- **Keep both:** #10 Overdue (bar) + #11 Overdue (flex) — **intentional** dual-view, leave alone

You're now at 13 components.

## Step 2 — Add KPI strip (Row 1, 5 metric cards)

Lightning metric chart type. Each card = a COUNT from an existing report with a scope filter. Source reports listed below are all already in Sales KPI tracking folder.

| #   | Title                              | Source report                                                                                                                                                                                                                  | Filter                  | Color  |
| --- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------- | ------ |
| 1   | Total open FY26 pipeline           | SD Pipeline Open FY26 (`00OTb000008fzirMAA`)                                                                                                                                                                                   | FY26 only (already set) | Blue   |
| 2   | Stage 3+ w/ no NextStep            | Mid-Stage: No NextStep (`00OTb000008fAjZMAU`)                                                                                                                                                                                  | (as-is)                 | 🔴 Red |
| 3   | No activity 60d+                   | **NEW** — create `No Activity 60 Plus Days - Open Opps` by cloning `00OTb000008TaEnMAK` (which is `No Activity 30 Plus Days - Open Opps`), change LAST_ACTIVITY filter from 30 to 60                                           | Orange                  |
| 4   | Chronic slip (Push ≥ 5)            | **NEW** — must be created in UI due to REST restriction on PushCount filter. See Step 5 below.                                                                                                                                 | Orange                  |
| 5   | Contract expired (renewal overdue) | **NEW** — Account-type report `Contract End in Past (Active Pipeline)`. Filter: `APTS_Contract_End_Date__c != NULL AND APTS_Contract_End_Date__c < TODAY` + `Id IN (SELECT AccountId FROM Opportunity WHERE IsClosed = false)` | 🔴 Red                  |

Each card should show just the count, no chart axis. Size: 2 cols × 1 row.

## Step 3 — Hygiene flex tables (Row 2-3, existing + 2 new)

Keep these existing components (they're covering the right signals):

- Stale Opportunities - CFQ (`00OTb000008TZgvMAG`)
- No Activity 30 Plus Days (`00OTb000008TaEnMAK`)
- Active Opps: No Activity (`00OTb000008fAmnMAE`)
- High Value Stale Deals (`00OTb000008Ti97MAC`)
- Missing Quote Type (`00OTb000008ekynMAA`)
- Aging Pipeline 365 Plus Days (`00OTb000008Ti7VMAS`)
- Overdue Opportunities (bar + flex)
- Land Deals Lacking Commercial Approval Flow (`00OTb000008fAlBMAU`)
- Missing Amount on Open Opps (`00OTb000008TZqcMAG`)
- Mid-Stage Opps Lacking NextStep (`00OTb000008fAjZMAU`)

**Add two new:**

- **Stage 1-2 w/ Close Date ≤ 30d (unrealistic)** — Clone `Mid-Stage: No NextStep`, change StageName filter to `('1 - Prospecting','2 - Discovery')`, add `CloseDate <= NEXT_N_DAYS:30`.
- **Q1 "No Opportunity" without Reason** — Clone `Close Date Slipped YTD` to a closed-deal report, filter: `IsClosed = true AND StageName = '0 - No Opportunity' AND CloseDate >= 2026-01-01 AND CloseDate <= 2026-03-31 AND (Reason_Won_Lost__c = NULL OR Reason_Won_Lost__c = '')`.

## Step 4 — Add Account-level block (Row 4, 3 new components)

These signals live in `Account`, not Opportunity — Lightning mixes object types fine on a dashboard.

- **Accounts without KYC Approval** — already exists at component #6 (`00OQA000004OLk92AG`). Keep.
- **NEW: Accounts NDA unsigned (with active pipeline)** — Account-type report filtered `APTS_NDA_Signed__c = FALSE AND Id IN (SELECT AccountId FROM Opportunity WHERE IsClosed = false)`.
- **NEW: Installed Competitor, No Open Opp (whitespace)** — Account-type report filtered `Id IN (SELECT Account__c FROM Installed_Competitor_Product__c) AND Id NOT IN (SELECT AccountId FROM Opportunity WHERE IsClosed = false)` — **this surfaces the 1,772-account winback queue**.
- **NEW: Contract Expired (with active pipeline)** — feeds the KPI card #5 and also gives a drillable table.

## Step 5 — PushCount-filter reports (UI only)

Because REST rejects `Opportunity.PushCount` as a filter, these must be built in the UI:

- **Chronic Slip (Push ≥ 5)**: Clone any Opportunity report. In UI Report Builder, add Cross Filter → `Opportunity.PushCount ≥ 5`. Save.
- **Pushed Deals by Owner**: Add PushCount as a COLUMN (not filter). UI only.

## Step 6 — Trend widget (data-quality ledger integration)

Once `scripts/audit_data_quality.py` has run for 2+ months, the file `output/data_quality/history.json` has MoM deltas. Three options to surface in Lightning:

**Option A (manual, fastest)**: Screenshot the "Stage 3+ with no NextStep" delta line from the Obsidian summary each month; paste as a rich-text component at the top of the dashboard. Five-minute operator task.

**Option B (semi-automated)**: Upload `history.json` to a SharePoint folder; embed as a link on the dashboard. Ten minutes once.

**Option C (programmatic)**: Write a nightly Flow that reads `output/data_quality/summary.md` from an inbound integration and posts deltas to a custom `Sales_Ops_Hygiene_Trend__c` object. A dashboard component can chart from there. This is a week of work — defer unless deltas become a monthly talking point.

**Start with Option A.** It's fine.

## Step 7 — Filters

Add:

- `Opportunity.Account_Unit_Group__c` (equals picklist) — the only cross-cutting dimension for sales ops
- `Opportunity.CloseDate` (date range) — bounded to `2026-01-01` → `2026-12-31`

Remove any global filters that aren't referenced by most components (Industry, Legal Country — they're cross-cutting noise for ops hygiene).

## Step 8 — Ship

1. Save as v2 in Andre folder.
2. Open canonical `01ZTb00000FSP9JMAX`. Rename to `Sales Ops Quarterly KPI Dashboard (v1 archive)`.
3. Save As your v2 to `Sales KPI Tracking` with the canonical name.
4. Announce to the ops team.

## Reference — new reports to create (total: 5)

| Name                                       | Source clone           | Key filters                                                                    |
| ------------------------------------------ | ---------------------- | ------------------------------------------------------------------------------ |
| `No Activity 60 Plus Days - Open Opps`     | `00OTb000008TaEnMAK`   | Change LAST_ACTIVITY from LAST 30 DAYS → LAST 60 DAYS                          |
| `Chronic Slip - Push 5 Plus`               | Any Opp report         | Cross filter: Opportunity has PushCount ≥ 5                                    |
| `Stage 1-2 with Imminent Close`            | Mid-Stage No NextStep  | StageName IN (1,2); CloseDate ≤ NEXT 30 DAYS                                   |
| `Q1 No Opportunity Without Reason`         | Close Date Slipped YTD | StageName = '0 - No Opportunity'; Reason empty; CloseDate Q1 2026              |
| `Accounts NDA Unsigned with Open Pipeline` | Account report clone   | APTS_NDA_Signed\_\_c = FALSE + subquery on Opportunity                         |
| `Installed Competitor No Open Opp`         | Account report clone   | subquery on Installed_Competitor_Product\_\_c + NOT IN subquery on Opportunity |
| `Contract Expired with Open Pipeline`      | Account report clone   | APTS_Contract_End_Date\_\_c < TODAY + active pipeline subquery                 |

Paste the Ids here as you create them:

```
No Activity 60 Plus Days           →  [paste id]
Chronic Slip Push 5+               →  [paste id]
Stage 1-2 Imminent Close           →  [paste id]
Q1 No Opportunity Without Reason   →  [paste id]
Accounts NDA Unsigned              →  [paste id]
Installed Competitor No Open Opp   →  [paste id]
Contract Expired Active Pipeline   →  [paste id]
```

## Operational ritual — monthly

Every month after `run_monthly_director_review.py` finishes:

1. Open `obsidian/Monthly/YYYY-MM/` (soon: `output/data_quality/YYYY-MM-DD/summary.md` too).
2. Review the DQ summary — look at deltas (⚠ = worse, ✓ = improving).
3. Biggest regressions go to the **Biggest 3 hygiene issues** rich-text block on the Sales Ops dashboard (Option A above).
4. Assign owners in the ops standup for the top 3.

The goal isn't a pretty dashboard — it's that **"77% of deals have never had an activity logged" stops being true because we're measuring it every month.**
