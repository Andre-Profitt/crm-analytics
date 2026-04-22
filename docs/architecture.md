# SD Monthly Deck Pipeline — Architecture

## Read this first: repo layout

There are 161 Python scripts in `scripts/`. Most of them are not part of the production pipeline. Here is what matters and what to ignore.

### What runs in production today (11 scripts)

These are the **legacy production lane**. This is the pipeline that currently produces decks for monthly reviews. It has CI (GitHub Actions on the 1st of each month). When someone says "run the monthly pipeline," they mean this.

| Script                              | What it does                                             |
| ----------------------------------- | -------------------------------------------------------- |
| `run_monthly_director_review.py`    | **Orchestrator.** Runs all stages in sequence.           |
| `extract_director_live.py`          | SF → 9 Excel workbooks (SOQL + PI API)                   |
| `extract_historical_trending.py`    | Appends trend snapshot sheets to workbooks               |
| `audit_data_quality.py`             | 36 hygiene checks against live SF                        |
| `build_sharepoint_analysis.py`      | 1 master + 9 regional analytics workbooks (42 tabs)      |
| `build_dashboard_analysis_excel.py` | Q1 analysis workbook                                     |
| `build_deck_from_excel.py`          | Excel → 9 SimCorp-branded PowerPoint decks               |
| `build_exec_rollup_deck.py`         | CRO rollup deck (aggregates all 9)                       |
| `validate_tie_out.py`               | 4-way reconciliation (SF vs extract vs regional vs deck) |
| `audit_deck_scope.py`               | Validates slide-level numeric claims against sidecar     |
| `generate_obsidian_notes.py`        | Updates Obsidian vault + MoM snapshot ledger             |

### What is being built but is not production yet (37 scripts)

These are the **modular lane**. This is the intended future-state architecture. It uses JSON snapshots, Claude-assisted briefs, validated fact packs, and pluggable deck renderers. It does NOT have a CI scheduler and has NOT been used to produce decks for a live review.

| Key scripts                                     | What they do                                                       |
| ----------------------------------------------- | ------------------------------------------------------------------ |
| `run_sales_director_monthly_master_builder.py`  | **Orchestrator.** JSON snapshot → Claude brief → fact pack → deck. |
| `build_validated_director_brief.py`             | Claude-assisted monthly brief with fact validation                 |
| `extract_director_workbook_snapshot.py`         | Workbook → JSON snapshot                                           |
| `build_sales_director_monthly_shell.py`         | Shell/template for director monthly reports                        |
| `validate_sales_director_shell_contract.py`     | Contract validation for shell output                               |
| `run_sales_director_canonical_shell_builder.py` | Canonical shell builder orchestrator                               |
| `build_sd_monthly_deck_v2.py`                   | v2 deck builder (modular lane)                                     |
| `contract_lint.py`                              | Source contract linter                                             |
| + 29 more                                       | Regional/global variants, author prompts, snapshot builders        |

**Do not** wire these into the production pipeline without finishing the scheduler and validating parity with the legacy lane.

### What is a different workstream entirely (22 scripts)

These are **CRM Analytics dashboard builders** — they patch live Salesforce dashboards via the Wave API. They are NOT part of the SD Monthly deck pipeline. They pre-date it.

Examples: `wave_patch_executor.py`, `phase2_5_core_patch.py`, `salesforce_dashboard_executor.py`, `dashboard_state_dump.py`

### Shared infrastructure (22 scripts)

PI list view management, territory mapping, analytics intelligence, Codex skill packaging, Obsidian hygiene, export utilities. Used by both lanes.

### Dead / experimental / one-off (69 scripts)

Prior iterations, BDR profiling, SimCorp deck prototypes, one-time fixes. **Ignore these.** Examples: `simcorp_full_deck_v3.py`, `profile_bdr_activity_model.py`, `fix_legacy_engagement_history_columnmaps.py`, `build_nam_deck.py`

---

## Legacy production lane (detailed)

### What it does

Produces 9 per-director PowerPoint decks + 1 exec rollup for SimCorp's monthly Sales Director review. Fully automated: live Salesforce data in, branded decks out. Runs on the 1st of each month via GitHub Actions, or on-demand.

## Pipeline flow

```
Salesforce (live SOQL + PI API + Forecast API)
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│  Stage 1: EXTRACT                                       │
│  extract_director_live.py        → 9 Excel workbooks    │
│  extract_historical_trending.py  → appends trend sheets │
│  audit_data_quality.py           → 36 hygiene checks    │
└─────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│  Stage 2: ANALYZE                                       │
│  build_sharepoint_analysis.py    → 1 master + 9 regional│
│  build_dashboard_analysis_excel.py → Q1 analysis wb     │
└─────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│  Stage 3: BUILD DECKS                                   │
│  build_deck_from_excel.py        → 9 director decks     │
│  build_exec_rollup_deck.py       → 1 CRO rollup         │
└─────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│  Stage 4: VALIDATE                                      │
│  validate_tie_out.py             → 4-way reconciliation │
│  audit_deck_scope.py             → slide-level claim    │
│                                    verification         │
└─────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│  Stage 5: KNOWLEDGE BASE                                │
│  generate_obsidian_notes.py      → Obsidian vault +     │
│                                    MoM snapshot ledger  │
└─────────────────────────────────────────────────────────┘
```

**Orchestrator (legacy):** `run_monthly_director_review.py` runs all stages in sequence. Exit code 0 = all steps passed.

### Two orchestrators

The repo contains two orchestrators for the SD Monthly pipeline:

|                    | Legacy lane                                                                 | Modular lane                                                                                              |
| ------------------ | --------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| **Script**         | `run_monthly_director_review.py` (370 lines)                                | `run_sales_director_monthly_master_builder.py` (1,459 lines)                                              |
| **Status**         | Production. Has CI scheduler.                                               | In development. No committed scheduler.                                                                   |
| **Approach**       | Monolithic: extract → analyze → deck → validate → obsidian, all in one run. | Modular: JSON snapshots → Claude-assisted brief → validated fact pack → deck render (pluggable backends). |
| **Deck rendering** | `build_deck_from_excel.py` directly.                                        | Can delegate to the legacy builder or to a Claude-assisted renderer.                                      |
| **Scheduling**     | `.github/workflows/monthly-review.yml` (midnight 1st of month).             | None committed. Scheduling is an open item.                                                               |

If you are operating the pipeline today, use the legacy lane. The modular lane is the intended future-state architecture but is not yet production-ready.

## Scripts

### Stage 1: Extract (Salesforce → Excel)

| Script                           | Lines | What it does                                                                                                                                                                                                                                                                                                   |
| -------------------------------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `extract_director_live.py`       | 1,208 | Pulls live Salesforce data via SOQL REST API into one Excel workbook per director. 12 sheets per workbook: Pipeline Open, Won/Lost, Commercial Approval, Renewals, Pipeline Inspection, Q1 Movement, Q2 Movement, Activity Volume, Commit Items, Stage History, Forecast Category History, Close Date History. |
| `extract_historical_trending.py` | 233   | Appends Historical Trending snapshot data to each director workbook. Uses the Forecast API for point-in-time ARR and stage comparisons.                                                                                                                                                                        |
| `audit_data_quality.py`          | 1,228 | Runs 36 data hygiene checks against live SF (missing fields, stale deals, zero-ARR wins, etc). Writes per-run JSON + Obsidian summary + rolling history ledger. Severity-tiered: Critical, Important, Domain.                                                                                                  |

**Auth:** `sf org display --target-org apro@simcorp.com --json` returns accessToken + instanceUrl. No .env file, no stored credentials.

**Territory config:** `config/sd_monthly_territories.json` defines the 9 directors, their SOQL WHERE clauses, and PI list view IDs.

### Stage 2: Analyze (Excel → SharePoint workbooks)

| Script                              | Lines | What it does                                                                                                                                                                                                                                                                                                                                               |
| ----------------------------------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `build_sharepoint_analysis.py`      | 5,720 | Builds the consolidated analytics workbook from all 9 director workbooks. 42 tabs: Executive Insights, Forecast Reconciliation, Approval tracking, Win Rate, Days in Stage, Pipeline Velocity, ARR Concentration, Deal Risk Scoring, Owner Scorecard, Competitive Win/Loss, and more. Also builds 9 regional workbooks (same structure, territory-scoped). |
| `build_dashboard_analysis_excel.py` | 1,741 | Builds a "Dashboard and Q1 Analysis" workbook with cross-territory comparisons and Q1 retrospective analysis.                                                                                                                                                                                                                                              |

### Stage 3: Build decks (Excel → PowerPoint)

| Script                      | Lines | What it does                                                                                                                                                                                                                                                                                                                                                                                                                       |
| --------------------------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `build_deck_from_excel.py`  | 3,956 | Core deck builder. Reads a director's Excel workbook + fires live SOQL for enrichment data, produces a 17-18 slide SimCorp-branded deck. Slides: cover, exec summary, MoM delta, Q1 retrospective, forecast variance, loss diagnostic, quarter outlook, forward look (per-deal readiness), key deals, deal risk scoring, owner coaching, pushed deals, Q1 movement, forecast breakdown, commercial approvals, renewals, end slide. |
| `build_exec_rollup_deck.py` | 620   | Aggregates all 9 director workbooks into a single CRO-level rollup deck (7 slides). Global pipeline summary, territory concentration, approvals overview.                                                                                                                                                                                                                                                                          |

**Template:** `~/archive/simcorp-deck-agent-backup/reference-decks/SimCorp_PPT_Template.pptx` (34 master layouts)

**Quarter logic:** `build_deck_from_excel.py` computes fiscal quarters dynamically from `datetime.now()` at import time via the `FQ` dict. Running in July gives Q2 retro + Q3 outlook + Q4 forward look. When the current quarter has no pipeline ARR for a director, the forward quarter is shown instead with a subtitle note explaining the substitution. **Caveat:** `build_sharepoint_analysis.py` still contains hardcoded Q1 2026 / Q2 FY26 references (lines 551, 1107, 1656+). The dynamic quarter logic only covers the deck builder, not the SharePoint analytics workbooks.

### Stage 4: Validate

| Script                | Lines | What it does                                                                                                                                                                                              |
| --------------------- | ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `validate_tie_out.py` | 704   | 4-way reconciliation: Salesforce (live SOQL) vs Extract (workbook) vs Regional (SharePoint workbook) vs Deck (sidecar JSON). 10 metrics per director, 90 total. Writes Obsidian-formatted tie-out report. |
| `audit_deck_scope.py` | 335   | Opens each .pptx, extracts numeric claims from slide titles, compares against the deck's sidecar JSON. Flags scope drift (e.g., a slide claiming 12 deals when the sidecar says 8).                       |

**Sidecar JSON:** Each deck gets a companion `.json` file with the canonical numbers it was built from. This is the contract between the builder and the validator.

### Stage 5: Knowledge base

| Script                       | Lines | What it does                                                                                                                                                                 |
| ---------------------------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `generate_obsidian_notes.py` | 1,357 | Writes per-director Obsidian notes + a cross-run snapshot ledger for month-over-month comparison. The MoM slide reads from this ledger to show deltas vs the prior snapshot. |

### Shared modules

| Module                                   | Lines | What it does                                                                                                                                                                         |
| ---------------------------------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `monthly_platform/policy.py`             | 104   | Shared filtering rules: active forecast categories, approval classification, reporting scope. Used by both the deck builder and the tie-out validator so they apply identical logic. |
| `monthly_platform/period.py`             | 144   | Fiscal period utilities (quarter boundaries, date range helpers).                                                                                                                    |
| `monthly_platform/quarterly_pipeline.py` | 270   | Quarter-scoped pipeline filtering and aggregation.                                                                                                                                   |

## Config files

| File                                              | Purpose                                                    |
| ------------------------------------------------- | ---------------------------------------------------------- |
| `config/sd_monthly_territories.json`              | Director → territory → SOQL WHERE clause + PI list view ID |
| `config/sales_director_md1_presets.json`          | 9 preset filter combos for deck generation                 |
| `config/sales_deck_execution_plan.json`           | Deck slide sequence and feature flags                      |
| `config/sales_deck_external_source_contract.json` | Source-of-truth contract (which field comes from where)    |
| `config/q3_2026_reporting_ids.json`               | Q3 PI list view + Historical Trending report IDs           |

## Output structure

```
output/
├── director_live_workbooks/
│   └── 2026-04-22/              # per-date snapshot
│       ├── jesper-tyrer.xlsx
│       ├── sarah-pittroff.xlsx
│       └── ...                  # 9 workbooks
├── simcorp_director_decks/
│   └── 2026-04-22/
│       └── land-only/
│           ├── jesper-tyrer-LAND.pptx
│           ├── jesper-tyrer-LAND.json   # sidecar
│           ├── Exec Rollup.pptx
│           └── ...              # 10 decks + 10 sidecars
├── sharepoint/
│   ├── FY26 Pipeline Review, All Territories.xlsx   # master (42 tabs)
│   ├── FY26 Pipeline Review, APAC.xlsx              # regional
│   └── ...                      # 9 regional workbooks
├── data_quality/
│   ├── 2026-04-22/              # per-run audit
│   └── history.json             # rolling ledger
└── pipeline_logs/
    └── 2026-04-22/
        ├── manifest.json        # step durations, exit codes, file inventory
        └── *.log                # per-step stdout
```

## CI/CD

`.github/workflows/monthly-review.yml` runs at midnight UTC on the 1st of every month:

1. Checkout + install Python 3.13 + sf CLI
2. Authenticate to SF via `SFDX_AUTH_URL` secret
3. Run full pipeline (`run_monthly_director_review.py`)
4. Run data quality + scope audits
5. Verify tie-out clean
6. Upload decks + workbooks as GitHub artifacts (90-day retention)
7. Commit updated Obsidian vault + ledgers

Manual trigger via `workflow_dispatch` with optional date override.

**SharePoint upload** is currently a manual post-pipeline step using Microsoft Graph API (`az account get-access-token --resource https://graph.microsoft.com`). Target folder: `Sales Excellence > General > Book of Business > Sales Director Reporting > Q1 2026`.

## How to run

```bash
# Full pipeline (extract + analyze + decks + validate + obsidian)
python3 scripts/run_monthly_director_review.py --date 2026-05-01

# Skip stages
python3 scripts/run_monthly_director_review.py --date 2026-05-01 --skip-extract
python3 scripts/run_monthly_director_review.py --date 2026-05-01 --skip-analysis
python3 scripts/run_monthly_director_review.py --date 2026-05-01 --skip-decks

# Individual scripts
python3 scripts/audit_data_quality.py --date 2026-05-01
python3 scripts/audit_deck_scope.py --date 2026-05-01
python3 scripts/validate_tie_out.py --date 2026-05-01
```

## Key design decisions

1. **Excel as intermediate format.** Directors can review/adjust data before the deck is rendered. The workbook is the single source of truth for the deck.

2. **Sidecar JSON contract.** Every deck gets a companion JSON with the exact numbers it was built from. The validator compares sidecar vs workbook vs regional vs live SF. This catches drift at any layer.

3. **Dynamic fiscal quarters (deck builder only).** `build_deck_from_excel.py` derives quarter boundaries from `datetime.now()` at import. The deck builder works for any month without code changes. `build_sharepoint_analysis.py` and `validate_tie_out.py` still have hardcoded FY26 quarter dates that will need updating for FY27.

4. **Omitted deals excluded.** ForecastCategoryName = Omitted deals are filtered at the scope gate. These are deals directors have marked as not closing and should not appear in the pipeline review.

5. **Quarter fallback.** When the current quarter has zero pipeline ARR for a director, the forward quarter is shown instead. A subtitle note explains the substitution.

6. **4-state approval model.** No Approval Necessary (exempt), Approved, Pending Approval, Missing Approval. "Conditionally Approved" was renamed to "Pending Approval" per Andre's feedback.

## SF org details

- Target: `apro@simcorp.com`
- Instance: `simcorp.my.salesforce.com`
- API: v66.0
- Auth: `sf org display` (no .env)
- ARR field: `APTS_Opportunity_ARR__c` (unweighted, convertCurrency for EUR)
- Type filter: Land only for pipeline/deck (extract pulls Land + Expand + Renewal for analytics)
