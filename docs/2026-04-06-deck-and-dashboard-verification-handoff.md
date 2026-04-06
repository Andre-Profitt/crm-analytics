# Next Session Handoff — Deck + Dashboard Accuracy Verification

> **For the next Claude session.** This entire file is the load-bearing context for picking up the Sales Director Monthly + Sales Ops Quarterly work cleanly. Read it before doing anything.

---

## Who you are and what just happened

You're picking up a long, painful session that built three things in parallel:

1. **Standard Salesforce dashboards + reports** for Report 1 and Report 2
2. **CRMA (Wave) dashboards** for the same KPIs (legacy/secondary surface)
3. **A SimCorp-branded PowerPoint deck** for Report 1, with multiple iterations

The session ended with all three artifacts roughly built but **not yet verified for accuracy against the original KPI brief**. Your job in the next session is to **verify everything against the brief, fix the gaps, and ship clean**.

The session also proved out **Option D** — pulling chart data directly from CRMA dashboard SAQL steps via the Wave API and rendering it as a native PowerPoint chart. This is the cleanest path to "live data, native PPTX charts" and is the recommended approach for the rebuild.

## The KPI brief (from Andre, verbatim)

This is the **source of truth** for what each report must contain. Verify every artifact against it.

### Report 1: Pipeline Reporting & Insights

> Monthly report to the Sales Directors (level below MDs). Forward looking, insight-driven, PowerPoint format.
>
> - **A pipeline overview with quarterly focus** (one slide per region)
> - **Commercial Approval overview** — which deals have been approved and a list of any Land stage 3 deals with no commercial approval. Global overview (one slide) + list of candidates by region (one slide)
> - **Renewals tracking** — what renewals are coming up this quarter, what is the value and likelihood of renewing
> - **Churn Risk and trends** — difficult for now, but try to build a slide of what we can get from Finance. Andre is to reach out to Alex P. The current snapshot has `finance_feed_status: pending`.
> - **Slipped deals analysis** (root cause commentary) — start with slipped deals; root cause commentary requires reaching out to the opportunity owner. Andre will structure the outreach.

### Report 2: Sales Ops Quarterly Report

> Quarterly PowerPoint that follows from a CRMA dashboard. Track and report:
>
> - **CRM data quality** (completeness, accuracy)
> - **Process compliance rates**
> - **Forecast accuracy**
> - **Pipeline hygiene** (aging, stage progression)
>
> Report 2 should also have a CRMA dashboard with the same KPIs.

## State of the artifacts

### Standard Salesforce dashboards (just-built, NEED ACCURACY VERIFICATION)

| Dashboard                                         | ID                   | Lightning URL                                                                     | Widgets | Status                                                                       |
| ------------------------------------------------- | -------------------- | --------------------------------------------------------------------------------- | ------- | ---------------------------------------------------------------------------- |
| **Sales Directors Monthly — Pipeline & Insights** | `01ZTb00000FSP7hMAH` | `https://simcorp.my.salesforce.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view` | 16      | All 16/16 widgets returning data; some data values look off vs the KPI brief |
| **Sales Ops Quarterly — KPI Dashboard**           | `01ZTb00000FSP9JMAX` | `https://simcorp.my.salesforce.com/lightning/r/Dashboard/01ZTb00000FSP9JMAX/view` | 14      | All 14/14 widgets returning data                                             |

These were built mid-session by cloning working components from the existing BOB (`01ZTb00000DoGYLMA3`) and RTB (`01ZTb00000E1e50MAB`) dashboards in the org. **The clone was syntactic — we didn't verify each widget's underlying report filter logic against the KPI brief.**

**Known issue: stale picklist filters in inherited reports.** Several BOB reports filter on `APTS_Primary_Quote_Type__c equals 'Quote'` or `contains 'Renewal'`, but the org's picklist values were migrated to `SBL`, `MBL`, `PPL`. The correct field for renewal/land/expand identification is **`Type`** (`Type='Renewal'` = 109 opps, `Type='Land'` = 391, `Type='Expand'` = 1,292).

We replaced 4 broken reports on Report 1 dashboard with corrected summary clones using the `Type` field:

- `00OTb000008ekqjMAA` New Customers (Land) by Region
- `00OTb000008eksLMAQ` Renewals by Fiscal Quarter
- `00OTb000008ektxMAA` Renewal Pipeline This Quarter
- `00OTb000008ekxBMAQ` Renewal ACV by Quarter
- `00OTb000008ekltMAA` Land Stage 3 Missing Approval (by Region)
- `00OTb000008eknVMAQ` Close Date Slipped (by Stage)
- `00OTb000008ekp7MAA` Commercial Approval Candidates (by Stage)
- `00OTb000008el21MAA` Top Accounts by ARR CFY (replaced broken TABULAR BOB version)
- `00OTb000008ekynMAA` Missing Quote Type (with field shown in detail columns)
- `00OTb000008el0PMAQ` Missing Decision Reason (with field shown in detail columns)

**These 10 reports were created mid-session and have NOT been audited line-by-line against the KPI brief.**

### CRMA (Wave) dashboards (refreshed earlier in session)

All 9 CRMA dashboards were refreshed against `apro@simcorp.com` on 2026-04-06 via the modernized Python builders (Builder Modernization 1A plumbing iteration, see the earlier handoff at `crm-analytics/docs/2026-04-06-builder-modernization-1a-baselines.md`):

| Dashboard                                  | ID                   | Folder | Last Modified |
| ------------------------------------------ | -------------------- | ------ | ------------- |
| Sales Ops Data Quality & Forecast Accuracy | `0FKTb0000000K5BOAU` | B2B_MA | 2026-04-06    |
| Pipeline & Opportunity Operations          | `0FKTb0000000KwPOAU` | B2B_MA | 2026-04-06    |
| Forecast & Revenue Motions                 | `0FKTb0000000JCLOA2` | B2B_MA | 2026-04-06    |
| Executive Revenue Source Truth             | `0FKTb0000000IxpOAE` | B2B_MA | 2026-04-06    |
| Revenue Retention & Health                 | `0FKTb0000000J97OAE` | B2B_MA | 2026-04-06    |
| Forecast Intelligence                      | `0FKTb0000000Jc9OAE` | B2B_MA | 2026-04-06    |
| Account Intelligence KPIs                  | `0FKTb0000000J7VOAU` | B2B_MA | 2026-04-06    |
| Customer & Account Health                  | `0FKTb0000000KunOAE` | B2B_MA | 2026-04-06    |
| Commercial Rhythm Control Tower            | `0FKTb0000000JPFOA2` | B2B_MA | 2026-04-06    |

The 9 underlying datasets are also refreshed with current org data via the Builder Modernization 1A iteration.

### PowerPoint decks (Report 1, multiple iterations)

All in `~/crm-analytics/output/sales_director_monthly_runs/2026-04-06T20-00-11Z_2026-04-01/`:

| File                                                                | Purpose                                                                                                                                                                                                                          | Status                                                                                                                                                             |
| ------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `sales_director_monthly_pipeline_insights_2026-04-01.pptx` (812 KB) | **Canonical baseline.** Generated by the existing pptxgenjs builder (`output/sales_director_monthly_deck_2026-03-31/build_sales_director_monthly_deck.js`). Microsoft default styling. Andre called this "decent". DO NOT touch. | Baseline of truth for content                                                                                                                                      |
| `sales_director_monthly_simcorp_v3.pptx`                            | Most recent SimCorp-branded version. 10 slides. 5 layout variants (card strip, hero column, horizontal bars, hero stat + mini row, table rows). Calendar year labels, no FY. Magenta accent for renewals/churn.                  | **Andre's reaction: "not enough intel from the KPIs" and asked to think through visuals**. Use this as the layout reference but rebuild to use Option D for charts |
| `sample_slide_crma_chart.pptx`                                      | **Option D proof of concept** — 1 slide with a stacked bar chart pulled live from CRMA dashboard `0FKTb0000000KwPOAU` step `s_region_hygiene`, rendered as a native PowerPoint chart. **This pattern should drive the rebuild.** | Working POC                                                                                                                                                        |
| `sales_director_monthly_simcorp_hybrid_polished.pptx`               | Earlier transplant attempt (canonical shapes copied onto SimCorp master). Has the colored bars removed via XML pass. Less polished than v3.                                                                                      | Reference only                                                                                                                                                     |
| Several other intermediate `_simcorp*` and `_simcorp_v2` files      | Failed iterations during the session                                                                                                                                                                                             | DELETE on next session cleanup                                                                                                                                     |

**The recommended starting point** for the next session's deck rebuild is the **Option D proof of concept pattern** in `sample_slide_crma_chart.pptx`, applied across 10 slides matching the v3 structure but using live CRMA chart data.

### Scripts in `~/crm-analytics/scripts/` (uncommitted, but proven working)

| Script                                   | What it does                                                                                                                                                                                                                                                                            | Reusable?                                           |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| `simcorp_crma_chart_sample.py`           | **Option D — KEY SCRIPT.** Pulls a chart step from a live CRMA dashboard, runs the SAQL via Wave API, renders as a native PPTX bar chart. Includes the proven 3-step recipe (auth → dashboard state → step query → strip Mustache bindings → run SAQL → CategoryChartData → add_chart). | YES — generalize this into the deck builder         |
| `simcorp_full_deck_v3.py`                | Full 10-slide builder with 5 layout variants, calendar labels, Magenta accent for renewal/churn, etc. Reads `report1_snapshot.json`.                                                                                                                                                    | YES — extend it to ingest CRMA chart data per slide |
| `simcorp_master_transplant.py`           | Transplants shapes from a canonical pptx onto the SimCorp template's Blank layout (used for the hybrid approach)                                                                                                                                                                        | Reference only                                      |
| `simcorp_color_font_remap.py`            | XML pass that swaps canonical hex colors to SimCorp production colors and `Avenir Next` → `Microsoft Sans Serif`                                                                                                                                                                        | Useful pattern                                      |
| `simcorp_polish.py`                      | Removes top blue bars, normAutofit scaling, etc. via lxml                                                                                                                                                                                                                               | Reference only                                      |
| `simcorp_remove_bars.py`                 | Sets `showMasterSp="0"` and strips Blank layout pictures                                                                                                                                                                                                                                | Reference only                                      |
| `simcorp_final_polish.py`                | Removes corner bars, untruncates text via snapshot lookup, adds appendix slide                                                                                                                                                                                                          | Reference only                                      |
| `simcorp_single_slide_v2.py`             | Single-slide v2 polish demo                                                                                                                                                                                                                                                             | Reference only                                      |
| `simcorp_hybrid_build.py`                | Hybrid (real cover + transplanted slides 2-9)                                                                                                                                                                                                                                           | Reference only                                      |
| `simcorp_full_deck_v2.py`                | v2 of the full deck (had FY27 labels — wrong)                                                                                                                                                                                                                                           | DELETE                                              |
| `convert_decks_to_simcorp.py`            | Failed extraction-based converter                                                                                                                                                                                                                                                       | DELETE                                              |
| `build_simcorp_branded_decks.py`         | First failed v1 attempt                                                                                                                                                                                                                                                                 | DELETE                                              |
| `build_report1_simcorp_from_snapshot.py` | Second failed attempt                                                                                                                                                                                                                                                                   | DELETE                                              |
| `simcorp_path_b_sample.py`               | Path B exploration                                                                                                                                                                                                                                                                      | DELETE                                              |
| `simcorp_single_slide_sample.py`         | Earlier single-slide sample                                                                                                                                                                                                                                                             | DELETE                                              |

**None of these scripts are committed to git yet.**

## State of the data

- **Snapshot file**: `output/sales_director_monthly_runs/2026-04-06T20-00-11Z_2026-04-01/report1_snapshot.json` — contains all the structured data for Report 1 (regions, watchlists, renewals, churn, slipped). 207 string candidates (account names, owner names, etc.).
- **Snapshot date**: 2026-04-01
- **Today's reference date**: 2026-04-06 (or whatever today is when the next session runs)
- **Calendar year convention**: ALWAYS calendar year, NEVER fiscal year. No "FY26", no "FY27", no "Q1 FY26". Use `April 2026` or `Q1 2026` (calendar) or just `2026`.
- **Quarter mismatch caveat**: the pptxgenjs source builder used SimCorp fiscal Q1 (Feb–Apr) as its quarter window. The data values are scoped to that window. When labeling slides, use generic time references (`April 2026 monthly view`) instead of `Q1` or `Q2` to avoid misrepresenting the data period.

## Andre's hard rules (do not violate)

1. **No em-dashes anywhere.** Use ASCII hyphens, periods, or rephrase. Em-dashes have been stripped from the deck pipeline; keep them out.
2. **Calendar year labels only.** No `FY26`, no `FY27`, no `Q1 FY26`.
3. **Renewals use ACV, not ARR.** Land and expand pipeline stays in ARR. This is a SimCorp methodology quirk.
4. **No MCP tools.** Use `sf` CLI, `curl`, `python3 requests` directly. CLI to CLI.
5. **Never use `git add .` / `-A` / `-u`.** Stage by exact path only. Working tree has many user WIP files (DO NOT touch them).
6. **Never push to origin** unless Andre explicitly says so.
7. **Do not produce basic/elementary work.** Andre wants consultant-grade output. Be Steve-Jobs-level obsessed.
8. **Don't summarize what was just done.** Andre reads diffs.
9. **Stop when something feels fundamentally wrong.** Iterating in the wrong direction wastes time. Plan with Andre, then execute.
10. **Auth via `sf org display --target-org apro@simcorp.com --json`.** No `.env` files.

## The proven Option D recipe (USE THIS for the deck rebuild)

The script `~/crm-analytics/scripts/simcorp_crma_chart_sample.py` proves a clean 3-step recipe for embedding CRMA dashboard charts as native PowerPoint charts. The pattern:

```python
# 1. Get the chart step's SAQL from a live CRMA dashboard
state = requests.get(f"{inst}/services/data/v66.0/wave/dashboards/{dashboard_id}", ...).json()["state"]
step = state["steps"][step_name]
saql = html.unescape(step["query"])  # may be a dict; check for inner "query" key

# 2. Make the SAQL runnable: qualify the dataset + strip Mustache filter bindings
saql = re.sub(r'q\s*=\s*load\s*"[^"]+"', f'q = load "{dataset_id}/{version_id}"', saql, count=1)
saql = "\n".join(line for line in saql.splitlines() if "{{" not in line)

# 3. Run + render
records = requests.post(f"{inst}/services/data/v66.0/wave/query", json={"query": saql}, ...).json()["results"]["records"]
# Build CategoryChartData, call slide.shapes.add_chart(XL_CHART_TYPE.BAR_STACKED, ...)
```

**Source of truth = the live CRMA dashboard step.** Every chart in the deck should be a direct read of a CRMA step. This guarantees the deck data and the dashboard data match, because they ARE the same query.

Use this recipe for every chart. Do NOT hand-draw shapes for data points.

## What needs to be verified (the actual job for next session)

### Phase 1: Audit standard SF dashboards against the KPI brief

For both `01ZTb00000FSP7hMAH` (Report 1) and `01ZTb00000FSP9JMAX` (Report 2), iterate every widget:

1. **Does each widget map to a specific bullet in the KPI brief?** Document the mapping.
2. **Does each widget's underlying report filter make sense?** Specifically check for:
   - Stale picklist values (the `APTS_Primary_Quote_Type__c` migration issue)
   - Wrong field references (e.g., `Type` vs `APTS_Primary_Quote_Type__c` for renewal/land/expand identification)
   - Date filters that don't reflect calendar quarters
   - Aggregations on the wrong field
3. **For "Missing X" reports**, does the displayed column include the field being filtered on? (We fixed Missing Quote Type and Missing Decision Reason; verify the rest.)
4. **For "Top N" reports**, is the report actually grouped + sorted? (Top 20 Accounts by ARR was a TABULAR report with 1 row before we fixed it; verify there are no other broken Top N reports.)
5. **Cross-check**: does the SF dashboard widget value match the same metric on the corresponding CRMA dashboard? They should. If they don't, figure out which is right.

Output: a delta table per dashboard listing every widget, its KPI brief mapping, its current value, and any discovered issue.

### Phase 2: Audit CRMA dashboards against the KPI brief

For each of the 9 CRMA dashboards in folder `B2B_MA`, identify which steps map to which KPI brief bullet. The Sales Ops Data Quality & Forecast Accuracy dashboard (`0FKTb0000000K5BOAU`) is the **direct match for Report 2**. The others are supporting dashboards used by Report 1.

Specifically verify:

- **Report 2 KPIs**: data completeness, process compliance, forecast accuracy, pipeline hygiene — each must have a matching CRMA step on `0FKTb0000000K5BOAU`. Confirm the values shown match what's in the source dataset.
- **Report 1 backing**: pipeline coverage, commercial approvals, renewals, churn, slipped — each must have a matching CRMA step on the appropriate B2B_MA dashboard. Document the dashboard ID + step name for each.

### Phase 3: Cross-check SF standard dashboards vs CRMA dashboards

For the same KPI (e.g., "Renewal ACV due this quarter"), compare:

1. The standard SF dashboard widget value
2. The CRMA dashboard step value
3. The pptxgenjs `report1_snapshot.json` value

These three numbers should agree. If they don't, the SF dashboard widget is the most likely culprit (the SF reports we cloned have the stale picklist issue).

### Phase 4: Rebuild the Sales Director Monthly deck using Option D

Once the dashboard accuracy is verified, rebuild the Sales Director Monthly deck so that **every chart on every slide is a direct read of a CRMA dashboard step** via the Option D recipe.

Slide structure (from the brief):

| #   | Slide                             | KPI brief mapping            | CRMA source                                                                    |
| --- | --------------------------------- | ---------------------------- | ------------------------------------------------------------------------------ |
| 1   | Cover                             | —                            | —                                                                              |
| 2   | EMEA pipeline outlook             | Pipeline overview by region  | Pipeline & Opportunity Operations dashboard, region-filtered step              |
| 3   | North America pipeline outlook    | Pipeline overview by region  | Same                                                                           |
| 4   | APAC pipeline outlook             | Pipeline overview by region  | Same                                                                           |
| 5   | Commercial approval global        | Commercial Approval global   | Commercial Rhythm Control Tower or a step that returns approved/pending counts |
| 6   | Land Stage 3 candidates by region | Commercial Approval regional | Same                                                                           |
| 7   | Renewal pipeline and risk         | Renewals tracking            | Revenue Retention & Health                                                     |
| 8   | Churn risk and trends             | Churn Risk and trends        | Customer & Account Health (until Finance feed lands)                           |
| 9   | Slipped deals analysis            | Slipped deals                | Pipeline & Opportunity Operations slip steps                                   |
| 10  | Appendix                          | —                            | Salesforce dashboard URL list                                                  |

Every chart on slides 2-9 should be a `slide.shapes.add_chart(...)` with data from a live SAQL query.

### Phase 5: Build Report 2 (Sales Ops Quarterly) using the same pattern

Same approach: 6-7 slides, each backed by a CRMA step on `0FKTb0000000K5BOAU`. Output: `sales_ops_quarterly_simcorp_v1.pptx`.

## Known issues to address

1. **The canonical pptxgenjs deck truncates account/owner names with `…`.** A snapshot-lookup untruncation pass is in `simcorp_final_polish.py` but only catches single-prefix matches. If you keep using the canonical content, run this. If you switch fully to Option D, the issue goes away (CRMA queries return raw values).
2. **Slipped deals root cause commentary is pending** from opportunity owners. Andre will structure outreach. The slide should currently say "pending" and link to the live drilldown.
3. **Churn Finance feed is pending** from Alex P. The slide should currently say "CRM signal only" and label the Finance overlay as pending.
4. **The pptxgenjs source builder uses fiscal-Q logic** (Feb-Apr quarter window) but Andre wants calendar-year labels. The cleanest fix is to skip the pptxgenjs snapshot for the rebuild and pull all data from CRMA via Option D, where the queries can be filtered by calendar quarter.
5. **`days_remaining` from the snapshot was 29** (April 1 → April 30, fiscal Q1 end). This metric is wrong under calendar-year framing and should be dropped or recomputed against calendar Q2 end (June 30).
6. **Pipeline Opportunity Operations dashboard had a `conditionalFormatting` widget bug** earlier in the session that was patched in `crm_analytics_helpers.py` (commit `0d63f16`). The fix added `conditionalFormatting` to the banned-fields strip and made `deploy_dashboard` strip number widget patch fields by default.

## Recommended first actions for next session

1. **Read this handoff doc** end to end before doing anything.
2. **Read the KPI brief at the top of this doc** and treat it as the source of truth.
3. **Run the Option D POC** to confirm it still works: `python3 ~/crm-analytics/scripts/simcorp_crma_chart_sample.py`. Open the result. Verify the chart looks right.
4. **Phase 1 audit** of `01ZTb00000FSP7hMAH` widgets. Output a delta table.
5. **Phase 2 audit** of the CRMA dashboards. Output a step-to-KPI-brief mapping.
6. **Brainstorm with Andre** before rebuilding the deck. Propose a clean slide-by-slide plan based on the audit findings, and confirm before writing code.
7. **Once the plan is approved**, rebuild the deck using Option D for every chart. Save as `sales_director_monthly_simcorp_v4.pptx`.
8. **Commit the proven scripts** (`simcorp_crma_chart_sample.py` plus the final v4 builder) to `crm-analytics/scripts/` and commit the handoff doc.

## How NOT to waste the session (lessons from this one)

- **Do not iterate blindly on visuals.** Eight failed deck variants happened because I kept rebuilding without confirming what Andre wanted. Plan first, then execute.
- **Do not assume the data is right.** Stale picklist filters silently corrupted multiple BOB reports for months. Verify EVERY filter against the current org schema.
- **Do not use fiscal year labels.** Andre wants calendar year. Period.
- **Do not duplicate the SimCorp logo.** The template provides it; the canonical pptxgenjs source also embedded it on every slide. Drop pictures during transplants.
- **Do not use the SimCorp_PPT_Template.pptx** at `~/archive/simcorp-deck-agent-backup/reference-decks/SimCorp_PPT_Template.pptx`. Use the **Commercial Update - Dec 2025.pptx** template at `~/archive/simcorp-deck-agent-backup/reference-decks/Commercial Update - Dec 2025.pptx` instead. It has 50 production layouts (vs 34) including `1_Picture (small) right`, `SC-Master Gradient_Title`, `2_SC-Blue_Divider`, etc.
- **Use real production colors.** The brand JSON colors (`#083EA7`, `#7DE6E0`) are NOT what production decks actually use. Production uses `#0E3788` (primary blue), `#6FCCDD` (the dominant aqua), `#9D2E7B` (magenta), `#011946` (dark navy), `#E6EEFE` (light blue panel). See the side-by-side comparison earlier in the session for full details.
- **Check rate limits**. This session burned through Claude Code rate limits multiple times due to volume of subagent dispatches and large file reads.

## File paths recap

**Templates**:

- Production template (USE THIS): `/Users/test/archive/simcorp-deck-agent-backup/reference-decks/Commercial Update - Dec 2025.pptx`
- Brand JSON: `/Users/test/archive/simcorp-deck-agent-backup/brand-themes/simcorp-2024.json`
- Reference QBR for additional layouts: `/Users/test/archive/simcorp-deck-agent-backup/reference-decks/QBR, Apr. 2025 - AMERICAS.pptx`

**Source data**:

- `/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-06T20-00-11Z_2026-04-01/report1_snapshot.json` — Report 1 structured snapshot
- `/Users/test/crm-analytics/output/sales_ops_quarterly_deck_2026-03-31/sales_ops_quarterly_review_2026-04-01.summary.json` — Report 2 summary

**Live dashboards** (linked in URLs above):

- 2 standard SF dashboards (`01ZTb00000FSP7hMAH`, `01ZTb00000FSP9JMAX`)
- 9 CRMA dashboards in folder `B2B_MA` on `apro@simcorp.com`

**Working scripts** (the ones to keep):

- `/Users/test/crm-analytics/scripts/simcorp_crma_chart_sample.py` — Option D recipe POC
- `/Users/test/crm-analytics/scripts/simcorp_full_deck_v3.py` — v3 full builder reference (replace its data sources with Option D in v4)
- `/Users/test/crm-analytics/scripts/simcorp_color_font_remap.py` — XML color/font remap helper
- `/Users/test/crm-analytics/scripts/simcorp_polish.py` — XML element removal helper

**Output artifacts**:

- `/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-06T20-00-11Z_2026-04-01/sales_director_monthly_pipeline_insights_2026-04-01.pptx` — canonical baseline
- `/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-06T20-00-11Z_2026-04-01/sales_director_monthly_simcorp_v3.pptx` — v3 styled deck
- `/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-06T20-00-11Z_2026-04-01/sample_slide_crma_chart.pptx` — Option D POC
- `/Users/test/crm-analytics/output/sales_ops_quarterly_deck_2026-03-31/sales_ops_quarterly_review_2026-04-01.pptx` — Report 2 baseline (pptxgenjs)

## One sentence

Verify every standard SF dashboard widget and CRMA step against the KPI brief, then rebuild the Sales Director Monthly deck as v4 with native PowerPoint charts pulled live from CRMA dashboard SAQL steps using the Option D recipe in `simcorp_crma_chart_sample.py`.
