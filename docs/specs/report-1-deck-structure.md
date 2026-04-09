# Report 1: Sales Director Monthly Deck — Structure Spec

Canonical slide layout for the Sales Director Monthly PowerPoint deck (`Report 1: Pipeline Reporting & Insights`). One deck is rendered per MD-1 Sales Director using the filter presets in `config/sales_director_md1_presets.json`.

## Design principles (confirmed 2026-04-09)

1. **Clear top-level numbered sections.** Six sections, numbered 1-6, each with its own section divider slide.
2. **Separate tables under each section.** Each sub-item (1.1, 1.2, etc.) is its own slide with a single chart or table. No combined views, no opinionated exec-summary synthesis.
3. **One deck per MD-1 Sales Director.** 9 directors → 9 decks per monthly run.
4. **Forward-looking, insight-driven.** The deck is a monthly leadership view; titles and subtitles lead with "what is the state" / "what is at risk" / "what action is needed".

## Slide sequence (21 slides per director)

| #   | Slide                                                          | Type            | Source                                                       |
| --- | -------------------------------------------------------------- | --------------- | ------------------------------------------------------------ |
| 1   | Cover — director name, territory, snapshot date, quarter focus | cover           | director preset + snapshot meta                              |
| 2   | Executive summary — 1 paragraph + 4 metrics                    | metric grid     | pipeline + renewals + slipped_deals summaries                |
| 3   | **1. Pipeline Overview — quarterly focus** (section divider)   | section divider | static                                                       |
| 4   | 1.1 Pipeline by Stage (ARR, current calendar quarter)          | bar chart       | `pipeline_overview_global`                                   |
| 5   | 1.2 Pipeline by Region (stacked, per director scope)           | stacked bar     | `Pipeline_Overview_by_Stage`                                 |
| 6   | 1.3 Top Opportunities in Pipeline (list)                       | table           | custom SOQL filtered to director scope                       |
| 7   | **2. Commercial Approval** (section divider)                   | section divider | static                                                       |
| 8   | 2.1 Current State Overview (approved vs not-approved)          | bar chart       | `Commercial_Approval_Current_State`                          |
| 9   | 2.2 YTD Approved Deals (2026 to date)                          | table           | `Commercial_Approval_approved_2026_qbd` `00OTb000008aTtJMAU` |
| 10  | 2.3 Land Stage 3 Missing Approval Candidates                   | table           | `Commercial_Approval_candidates_cdi` `00OTb000008d6ovMAA`    |
| 11  | 2.4 Missing Commercial Approval Opportunities (list)           | table           | D2 #13 `00OTb000008fAlBMAU`                                  |
| 12  | **3. Renewals Tracking** (section divider)                     | section divider | static                                                       |
| 13  | 3.1 Renewal ACV This Quarter (metric)                          | metric          | `Renewal_Pipeline_This_Quarter` `00OTb000008ektxMAA` (ACV)   |
| 14  | 3.2 Renewal Likelihood by Probability                          | bar chart       | `Renewal_Likelihood_by_Probability` `00OTb000008fBULMA2`     |
| 15  | 3.3 Upcoming Renewals List (this quarter)                      | table           | `Renewal_Pipeline_This_Quarter`                              |
| 16  | **4. Churn Risk & Trends** (section divider)                   | section divider | static                                                       |
| 17  | 4.1 Churn Risk Placeholder — awaiting Finance feed from Alex P | placeholder     | `finance_overlay` (pending)                                  |
| 18  | **5. Slipped Deals Analysis** (section divider)                | section divider | static                                                       |
| 19  | 5.1 Close Date Slipped by Stage                                | table           | `Close_Date_Slipped_by_Stage` `00OTb000008eknVMAQ`           |
| 20  | 5.2 Slipped Deals Trend (6-month)                              | line chart      | Pipeline Inspection native (link-out) or SOQL recompute      |
| 21  | 5.3 Slipped Deals Root Cause Commentary                        | table           | Owner outreach commentary overlay (pending)                  |
| 22  | **6. Data Quality** (section divider)                          | section divider | static                                                       |
| 23  | 6.1 Missing Win/Loss Reason (excludes 0-No-Opportunity)        | table           | `00OTb000008el0PMAQ` (patched 2026-04-09)                    |
| 24  | 6.2 Overdue Close Date Open Opps (sorted by record count desc) | table           | `00OTb000008TaBZMA0` (patched 2026-04-09)                    |
| 25  | 6.3 Accounts without KYC Approval                              | table           | `KYC_Approval_Status` `00OTb000007BvlJMAS`                   |
| 26  | Closing — source URLs + refresh cadence                        | closing         | static                                                       |

**Total: 26 slides per director × 9 directors = 234 slides per monthly run.**

## 9 MD-1 Sales Directors (from `config/sales_director_md1_presets.json`)

| #   | Director          | Territory                  | Scope                                         |
| --- | ----------------- | -------------------------- | --------------------------------------------- |
| 1   | Megan Miceli      | Canada                     | NAM + Canada                                  |
| 2   | Patrick Gaughan   | NA Asset Management        | NAM ex-Canada + AM/Bank/Wealth/Servicer/Other |
| 3   | Jesper Tyrer      | APAC                       | APAC                                          |
| 4   | Sarah Pittroff    | Central Europe             | Central Europe                                |
| 5   | Francois Thaury   | Southern Europe            | Southwestern Europe                           |
| 6   | Dan Peppett       | UK & Ireland               | United Kingdom & Ireland                      |
| 7   | Christian Ebbesen | NL & Nordics               | Northern Europe                               |
| 8   | Mourad Essofi     | Middle East & Africa       | Middle East & Africa                          |
| 9   | Adam Steinhaus    | **US** Pension & Insurance | NAM ex-Canada + Pension/Insurance             |

## Output convention

- **Path:** `~/crm-analytics/output/sales_director_monthly_runs/{YYYY-MM-DD}/{director-slug}.pptx`
- **Slug format:** `lastname-firstname` lowercased, e.g. `peppett-dan.pptx`
- **Runbook:** `scripts/build_sd_monthly_deck_v2.py --director "Dan Peppett" --snapshot-date 2026-04-09`
- **Batch:** `scripts/build_sd_monthly_deck_v2.py --all --snapshot-date 2026-04-09`

## Data source strategy

**Preferred:** Query SF Reports API with per-director `reportFilters` override (ephemeral filter at run time). Fast, no snapshot rework, accurate per director.

**Fallback:** Read the existing global snapshot and filter client-side by `sales_region`, `owner_name`, and `industry`. Works for most directors but misses NAM sub-splits (Megan, Patrick, Adam) because the existing snapshot doesn't carry account-level legal country or industry.

**Ideal (v3, later):** Retrofit `refresh_sales_director_monthly_snapshot.py` to emit per-director snapshots with preset names baked in.

v2 uses the preferred strategy (report filter overrides).
