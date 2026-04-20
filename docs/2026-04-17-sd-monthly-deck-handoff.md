# Sales Director Monthly Deck — Full Handoff

**Date:** 2026-04-17
**Owner:** Andre Profitt (apro@simcorp.com)
**Repo:** `/Users/test/crm-analytics/`
**Branch:** `main` (346 uncommitted files — bulk of work is uncommitted)

---

## 1. What This Is

An end-to-end automated pipeline that produces **9 per-director PowerPoint decks + 1 exec rollup** for SimCorp's monthly Sales Director review. The pipeline:

```
Salesforce (live SOQL + PI API + Forecast API)
  → extract_director_live.py        (per-director Excel workbook)
  → build_sharepoint_analysis.py    (consolidated + 9 regional analytics workbooks)
  → build_deck_from_excel.py        (per-director SimCorp-branded deck)
  → build_exec_rollup_deck.py       (CRO-level rollup deck)
  → generate_obsidian_notes.py      (knowledge base + snapshot history ledger)
  → validate_tie_out.py             (4-way reconciliation)
  → audit_data_quality.py           (35+ severity-tiered hygiene checks)
  → audit_deck_scope.py             (title-level numeric claim validation)
```

**Orchestrator:** `scripts/run_monthly_director_review.py` runs all stages in sequence.
**CI/CD:** `.github/workflows/monthly-review.yml` runs at midnight UTC on the 1st of each month.

---

## 2. The 9 Directors + Territories

Config lives in `config/sd_monthly_territories.json`. Each entry has a SOQL `WHERE` clause and a Pipeline Inspection list view ID.

| #   | Director          | Territory              | Unit Group       | Region Filter                   |
| --- | ----------------- | ---------------------- | ---------------- | ------------------------------- |
| 1   | Jesper Tyrer      | APAC                   | SC Asia          | (all)                           |
| 2   | Sarah Pittroff    | Central Europe         | SC EMEA          | Central Europe                  |
| 3   | Dan Peppett       | UK & Ireland           | SC EMEA          | United Kingdom & Ireland        |
| 4   | Francois Thaury   | Southern Europe        | SC EMEA          | Southwestern Europe             |
| 5   | Christian Ebbesen | NL & Nordics           | SC EMEA          | Northern Europe                 |
| 6   | Mourad Essofi     | Middle East & Africa   | SC EMEA          | Middle East & Africa            |
| 7   | Megan Miceli      | Canada                 | SC North America | Unit = SC Canada                |
| 8   | Patrick Gaughan   | NA Asset Management    | SC North America | Unit = SC USA, Industry = AM/WM |
| 9   | Adam Steinhaus    | NA Pension & Insurance | SC North America | Unit = SC USA, Industry = P&I   |

Adam is US-only, not global. Patrick gets Asset Mgmt + Wealth Mgmt; Adam gets Pension + Insurance. No overlap.

---

## 3. Core Scripts (15,500 LOC total)

### 3a. `scripts/extract_director_live.py` (1,143 lines)

**What:** Pulls live Salesforce data via SOQL REST API into a per-director Excel workbook.

**Sheets created per workbook:**

- Summary (KPIs, metadata)
- Pipeline Open FY26 (stages 1-6, FY26 close date)
- Won Lost FY26 (stages 0/8, FY26 close date)
- Commercial Approval (approval status for stage 3+ deals)
- Renewals FY26 (Type=Renewal)
- Pipeline Inspection (from PI list views via ui-api)
- Q1 Movement (deals that moved in/out of Q1)
- Activity Volume (LastActivityDate-based activity signals)
- Commit Items (ForecastCategoryName = Commit/CloseDate)
- Stage History (OpportunityFieldHistory for StageName)
- Forecast Category History (OpportunityFieldHistory for ForecastCategoryName)
- Close Date History (OpportunityFieldHistory for CloseDate)

**Key methodology (Alex P):**

- ARR field: `APTS_Opportunity_ARR__c` (unweighted), wrapped in `convertCurrency()` for EUR normalization
- Forecast ARR: `APTS_Forecast_ARR__c` (weighted, ~3-7x lower — Rebekka uses this one)
- Filters: exclude simcorp/test/delete accounts, exclude Sabiniewicz/Profit owners
- Types: Land, Expand, Renewal
- Stages: 1-6 open, 0/8 won/lost
- FY scope: FY26 only (2026-01-01 to 2026-12-31)

**Approval model (4-state):**

- `Approval_Status__c` = "No Approval Necessary" → exempt (not flagged)
- `Approval_Status__c` = "Approved" → approved
- `Submit_for_Stage_20_Review__c` = true but not yet approved → "Pending Approval"
- Stage 3+ with none of the above → "Missing Approval"

**Auth:** `sf org display --target-org apro@simcorp.com --json` → accessToken + instanceUrl. No .env file.

**Parallelization:** Uses `ThreadPoolExecutor` for per-territory extraction when `--all` flag is passed. Retry with `_sf_get_with_retry` (3 attempts, exponential backoff).

**Output:** `output/director_live_workbooks/{date}/{director-slug}.xlsx`

### 3b. `scripts/build_sharepoint_analysis.py` (5,720 lines)

**What:** Builds the consolidated analytics workbook from all 9 director workbooks + forecast API data.

**42 tabs** including: Executive Insights, Forecast Reconciliation, Approval tracking (4 tabs), Q1/Q2 Trend Consolidated, Win Rate, Days in Stage, Pipeline Velocity, ARR Concentration, Deal Risk Scoring, Forecast Variance, Commit Accuracy, Owner Scorecard, Competitive Win/Loss, Stage Conversion, Sales Velocity, Deal Age Distribution, Account Penetration, Forecast Bias, and more.

**Regional workbooks:** Pass `--territory "APAC"` to produce a territory-scoped workbook. Regional workbooks exclude global-only tabs (Win Rate, Days in Stage) and scope all data to that territory. Directors receive their regional workbook alongside their deck.

**DIRECTORS tuple:** Hardcoded list at top of file — name, short territory label, filename, SF user ID, forecast type ID. Must match `sd_monthly_territories.json`.

**Output:**

- `output/sharepoint/FY26 Pipeline Review, All Territories.xlsx` (master)
- `output/sharepoint/FY26 Pipeline Review, {territory}.xlsx` (per-region)
- `output/sharepoint/Dashboard and Q1 Analysis.xlsx`

### 3c. `scripts/build_deck_from_excel.py` (3,852 lines)

**What:** Reads a director's Excel workbook + the consolidated analytics workbook and renders a SimCorp-branded PowerPoint deck.

**Template:** `~/archive/simcorp-deck-agent-backup/reference-decks/SimCorp_PPT_Template.pptx`

**Deck structure (v3, ~16-17 slides depending on data availability):**

| #   | Slide                    | Function                         | Notes                                                                                                              |
| --- | ------------------------ | -------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| 1   | Cover                    | `slide_cover`                    | Director name, territory, reporting period                                                                         |
| 2   | Executive Summary        | `slide_executive_summary`        | KPI strip + insight bullets injected                                                                               |
| 3   | Month over Month         | `slide_month_over_month`         | Skipped if no prior snapshot in ledger                                                                             |
| 4   | Q1 Promised vs Delivered | `slide_q1_promised_vs_delivered` | Won/lost/pipeline decomposition                                                                                    |
| 5   | Q1 Forecast Variance     | `slide_forecast_variance`        | Bucket decomposition (conditional)                                                                                 |
| 6   | Why We Lost              | `slide_win_loss_diagnostic`      | Reason codes, competitor breakdown, stage-at-loss                                                                  |
| 7   | Q2 Outlook               | `slide_q2_outlook`               | Summary numbers for Q2                                                                                             |
| 8   | Q2 Forward Look          | `slide_q2_forward_look`          | **Live SF enrichment** — per-deal readiness grid with activity, AuM, competitor, forecast momentum, stage advances |
| 9   | Top Deals                | `slide_top_deals`                | Top 10 by ARR                                                                                                      |
| 10  | Deal Risk Scoring        | `slide_deal_risk_scoring`        | From analytics workbook (conditional)                                                                              |
| 11  | Owner Coaching           | `slide_owner_coaching`           | Slip owners + reason code labels (conditional)                                                                     |
| 12  | Pushed Deals & PI        | `slide_pushed_deals_with_link`   | Merged pushed + PI with link to list view                                                                          |
| 13  | Q1 Movement              | `slide_q1_movement`              | Deals that entered/exited Q1                                                                                       |
| 14  | Forecast Accuracy        | `slide_forecast_combined`        | Accuracy + breakdown combined                                                                                      |
| 15  | Commercial Approvals     | `slide_commercial_approvals`     | 4-table layout: YTD + FY Targets + Pending + Candidates                                                            |
| 16  | Renewals                 | `slide_renewals`                 | Q2 renewals                                                                                                        |
| 17  | Churn Risk               | `slide_churn`                    | Placeholder — waiting on Finance feed from Alex P                                                                  |
| 18  | End Slide                | `slide_end`                      | SimCorp branded closer                                                                                             |

**Key flags:**

- `--land-only` — filters to Type=Land, Q1-Q2 close dates (used for all director decks)
- `--analytics-workbook` — path to the consolidated workbook for Deal Risk, Forecast Variance, Owner Coaching

**Sidecar JSON:** Every deck emits a `.json` sidecar with the headline numbers it was built from, consumed by `validate_tie_out.py`.

**Rebekka's style (integrated):**

- Dark navy #1A1D31 headers, white text, alternating row colors
- mEUR format in table cells
- 16pt header, 10pt data
- Data-forward titles ("APAC Pipeline: 5 deals, EUR 4.5M" not "Pipeline Overview")
- No viz type changes without explicit approval

**REASON_CODE_LABELS dict:** Translates internal codes to director-friendly text (PUSH_HIGH → "Pushed 5+ times", STALE → "No activity 60+ days", etc.)

**MoM slide:** Reads from `obsidian/snapshot_history.json` ledger. Title: "Since last review ({date}): what moved" with FX rate note.

### 3d. `scripts/build_exec_rollup_deck.py` (620 lines)

Builds the CRO-level rollup deck from all 9 director sidecar JSONs. Data-forward titles + sidecar JSON output.

### 3e. `scripts/generate_obsidian_notes.py` (1,357 lines)

Writes Obsidian vault notes per director + monthly summary. Maintains `obsidian/snapshot_history.json` ledger for MoM deltas. Includes Deal Risk, Forecast Variance, Q1 losses, churn references.

### 3f. Other pipeline scripts

- `scripts/extract_historical_trending.py` (233 lines) — Appends historical trending data to director workbooks
- `scripts/validate_tie_out.py` (679 lines) — 4-way reconciliation: SF ↔ Extract ↔ Regional workbook ↔ Deck sidecar
- `scripts/audit_data_quality.py` (1,228 lines) — 35+ severity-tiered checks (Critical/Important/Domain, Hooman Hashemi governance concept). Writes Excel + JSON + Obsidian summary. Has `--write-to-sf` flag for Hygiene_Snapshot\_\_c writeback (blocked by permissions)
- `scripts/audit_deck_scope.py` (333 lines) — Validates numeric claims in deck slide titles against source data

---

## 4. Salesforce Org Details

- **Org:** `apro@simcorp.com`
- **Instance:** `simcorp.my.salesforce.com`
- **API version:** v66.0
- **Auth method:** `sf org display --target-org apro@simcorp.com --json`
- **CRM Analytics App:** B2B_MA
- **Key objects:** Opportunity, Account, OpportunityFieldHistory, ForecastingItem
- **Key fields:** `APTS_Opportunity_ARR__c`, `APTS_Forecast_ARR__c`, `Approval_Status__c`, `Stage_20_Approval__c`, `Submit_for_Stage_20_Review__c`, `Account_Unit_Group__c`, `Sales_Region__c`, `Lost_to_Competitor__c`, `Lead_Scope__c`, `PushCount`
- **Multi-currency:** Yes — `convertCurrency()` normalizes to EUR

---

## 5. Output Structure

```
output/
  director_live_workbooks/{date}/       # 9 Excel workbooks (extract)
    jesper-tyrer.xlsx
    sarah-pittroff.xlsx
    ...
  sharepoint/                           # Analytics workbooks
    FY26 Pipeline Review, All Territories.xlsx
    FY26 Pipeline Review, APAC.xlsx
    FY26 Pipeline Review, EMEA Central.xlsx
    ... (9 regional + 1 master + 1 dashboard analysis)
  simcorp_director_decks/{date}/
    land-only/                          # Director decks + sidecar JSON
      jesper-tyrer-LAND.pptx + .json
      adam-steinhaus-LAND.pptx + .json
      ...
      Exec Rollup.pptx + .json
  pipeline_logs/{date}/                 # Per-step execution logs
  data_quality/{date}/                  # Hygiene audit output
obsidian/
  Monthly/{YYYY-MM}/                    # Per-month vault notes
  Directors/                            # Standing director pages
  snapshot_history.json                 # MoM ledger
```

---

## 6. How to Run

### Full pipeline (all 9 directors + analysis + decks + obsidian)

```bash
python3 scripts/run_monthly_director_review.py --date 2026-04-17
```

### Single director extract

```bash
python3 scripts/extract_director_live.py --territory APAC
```

### Single deck build

```bash
python3 scripts/build_deck_from_excel.py \
  --workbook output/director_live_workbooks/2026-04-16/jesper-tyrer.xlsx \
  --output output/simcorp_director_decks/2026-04-16/land-only/jesper-tyrer-LAND.pptx \
  --land-only
```

### Regional analytics workbook

```bash
python3 scripts/build_sharepoint_analysis.py \
  --workbooks-dir output/director_live_workbooks/2026-04-16 \
  --territory "APAC"
```

### Audits

```bash
python3 scripts/audit_data_quality.py --date 2026-04-16
python3 scripts/audit_deck_scope.py --date 2026-04-16
```

---

## 7. Known Issues & Remaining Work

### Must Fix

1. **Multi-slide overflow** — Pushed Deals, Commercial Approvals, and Forecast Accuracy functions each produce 2 slides when data is large. Deck ends up ~21 actual slides instead of ~17. Need overflow truncation or pagination.
2. **Churn slide is placeholder** — `slide_churn()` renders static text. Waiting on Finance feed from Alex P for real data.
3. **Per-director churn screenshots** — Only APAC has a churn screenshot asset. Other 8 territories need user-provided assets.
4. **Git state** — 346 uncommitted files. Needs a structured commit (core scripts, configs, outputs, docs separately).

### Blocked on Admin

5. **Hygiene_Snapshot\_\_c CRUD permissions** — Custom object exists in SF (created via Playwright UI automation) but CRUD permission grant needs an admin (Ken Bryce Tagimacruz). `audit_data_quality.py --write-to-sf` will work once permissions are granted.
6. **4 remaining Hygiene custom objects** — Deal_Flag, Account_Flag, Installation_Flag, Quote_Flag not yet created.

### Nice to Have

7. **Forecast ARR reconciliation** — Rebekka uses `APTS_Forecast_ARR__c` (weighted), pipeline uses `APTS_Opportunity_ARR__c` (unweighted). Gap is documented but not resolved.
8. **Full rebuild for all 9 directors** — Latest code changes (approval fix, MoM fix, Q2 enrichment) haven't been rebuilt for all directors. Only APAC (Jesper) was rebuilt last.

---

## 8. Data Methodology Deep Dive

### ARR Definition

- **Opportunity ARR** (`APTS_Opportunity_ARR__c`): unweighted, what our pipeline uses
- **Forecast ARR** (`APTS_Forecast_ARR__c`): weighted, what Rebekka's reference deck uses
- Both wrapped in `convertCurrency()` for EUR normalization across multi-currency deals

### Q1 vs Q2 Scoping

- Q1 = 2026-01 through 2026-03
- Q2 = 2026-04 through 2026-06
- `--land-only` flag on deck builder filters to Type=Land + Q1-Q2 close dates
- Pipeline Inspection keeps full Land universe regardless of close date

### Approval Process

Four-state model discovered during this session:

1. **No Approval Necessary** — `Approval_Status__c = 'No Approval Necessary'` — 34,530 deals are exempt. These are NOT flagged as missing.
2. **Approved** — `Stage_20_Approval__c = true` or `Approval_Status__c = 'Approved'`
3. **Pending Approval** — `Submit_for_Stage_20_Review__c = true` but not yet approved
4. **Missing Approval** — Stage 3+ with none of the above

### Forecast Category Momentum (Q2 Forward Look)

- Reads from Forecast Category History sheet (OpportunityFieldHistory)
- Classifies into upgrades (Pipeline→BestCase→Commit→Closed) and downgrades (reverse)
- Last 30 days shown on slide with named deal movers

### Stage Advances (Q2 Forward Look)

- Reads from Stage History sheet
- Shows recent stage advances with named deals

---

## 9. Key Files Reference

| File                                                                            | Purpose                                            |
| ------------------------------------------------------------------------------- | -------------------------------------------------- |
| `config/sd_monthly_territories.json`                                            | Territory → director + SOQL + PI list view mapping |
| `config/sales_director_md1_presets.json`                                        | 9 MD-1 filter preset combos                        |
| `obsidian/snapshot_history.json`                                                | MoM snapshot ledger                                |
| `obsidian/runbook.md`                                                           | How to run the pipeline                            |
| `obsidian/methodology.md`                                                       | ARR definitions, Alex P scope filters              |
| `.github/workflows/monthly-review.yml`                                          | Monthly CI/CD at midnight 1st of month             |
| `~/archive/simcorp-deck-agent-backup/reference-decks/SimCorp_PPT_Template.pptx` | Deck template                                      |

---

## 10. CLAUDE.md Rules (Non-Negotiable)

These are enforced by the project's `CLAUDE.md`:

1. **CLI-first** — All SF work via `sf` CLI + `curl` + `requests`. No MCP tools.
2. **Never edit build\_\*.py files** — Patch live dashboards via Wave API instead.
3. **PATCH not PUT** for Wave API — PUT returns 405.
4. **HTML unescape** all step queries before PATCH — or the round-trip corrupts filters.
5. **Never change viz types** without explicit approval from Andre.
6. **Auth via `sf org display`** — no .env files, no hardcoded tokens.

---

## 11. Session History (What Was Done 2026-04-13 through 2026-04-17)

Major work completed in the prior session:

1. Built per-director regional workbooks with `--territory` flag
2. Added 11 new analytics tabs (Owner Scorecard, Competitive W/L, Sales Velocity, Deal Age, Account Penetration, Forecast Bias, etc.)
3. Added enterprise insights: MoM deltas, Why We Lost, Owner Coaching, Deal Risk, Q2 Forward Look
4. Fixed approval classification (No Approval Necessary exempt status)
5. Fixed MoM slide math/labeling (FX rate note, correct title format)
6. Added REASON_CODE_LABELS for director-friendly coaching text
7. Wired in unused SF data sources (Forecast Category momentum, Stage advances, Q2 velocity)
8. Trimmed deck from 24 → ~17 slides matching Rebekka's style
9. Added live SF enrichment to Q2 Forward Look (activity signals, AuM, competitor presence, forecast momentum)
10. Built GitHub Actions CI/CD workflow
11. Mapped full SF data model (1,966 objects, 20K reports, 894 dashboards)
12. Built data-quality audit system (35+ checks, severity-tiered)
13. Created Hygiene_Snapshot\_\_c custom object in SF via Playwright
14. Ran full ETL audits (code quality, performance, security)
15. Removed `Sales_Cycle_Duration__c` from DF Revenue Motions dataflow (Johan's Altify cleanup request)

---

## 12. Immediate Next Steps (Priority Order)

1. **Full rebuild** — Run `run_monthly_director_review.py` with all latest code changes for all 9 directors
2. **Git commit** — Structure the 346 uncommitted files into logical commits
3. **Multi-slide overflow fix** — Cap slides that produce extras
4. **Churn data** — Follow up with Alex P on Finance feed
5. **Hygiene permissions** — Follow up with admin for Hygiene_Snapshot\_\_c CRUD

---

## 13. Altify Cleanup (Separate Task, Done)

Johan F. Sonesson requested removal of `Sales_Cycle_Duration__c` from the DF Revenue Motions dataflow to unblock Altify package uninstall.

- **Dataflow:** DF Revenue Motions (`02KTb000006bkd7MAA`)
- **Node:** Extract_Opps (sfdcDigest on Opportunity)
- **Field removed:** `Sales_Cycle_Duration__c` (was index 22 of 23 fields)
- **Downstream impact:** None — no other node referenced it
- **Status:** Done in Prod. Preprod and QA are Johan's team's responsibility.
- **Also found:** 47 SF reports with "Altify" in their names — flagged for cleanup but not blocking.
