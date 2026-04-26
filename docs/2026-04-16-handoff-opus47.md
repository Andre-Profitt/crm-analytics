# Handoff — Sales Director Monthly ETL, 2026-04-16

**Previous model:** Claude Opus 4.6 (1M context)
**Prepared for:** Opus 4.7 or later
**Working dir:** `/Users/test/crm-analytics`
**Run date in use:** `2026-04-16`

Pick up here. You have a working end-to-end pipeline from live Salesforce through Excel analysis to per-director SimCorp-branded PowerPoint decks plus an Obsidian knowledge base. Every stage runs from one command and reconciles 0 mismatches. The user is now focused on tightening the deck visually and selectively adding the highest-value analyses that the workbook already computes.

---

## 1. What exists and runs clean today

One command rebuilds everything:

```bash
python3 scripts/run_monthly_director_review.py --date 2026-04-16
```

That runs 16 stages, all `ok` on the most recent run:

| Stage                            | Script                                      | Purpose                                    |
| -------------------------------- | ------------------------------------------- | ------------------------------------------ |
| `1a_extract_salesforce`          | `scripts/extract_director_live.py --all`    | Pull 9 director workbooks from live SF     |
| `1b_extract_historical_trending` | `scripts/extract_historical_trending.py`    | 18 HT reports → Q1/Q2 Snapshot Trend tabs  |
| `2a_analyze_consolidated_review` | `scripts/build_sharepoint_analysis.py`      | FY26 Pipeline Review, 30 tabs              |
| `2b_analyze_dashboard_q1`        | `scripts/build_dashboard_analysis_excel.py` | Dashboard + Q1 Analysis, 38 tabs           |
| `3_ship_deck_{slug}` × 9         | `scripts/build_deck_from_excel.py`          | Per-director SimCorp deck                  |
| `3_ship_exec_rollup`             | `scripts/build_exec_rollup_deck.py`         | Cross-territory exec deck                  |
| `4_validate_tie_out`             | `scripts/validate_tie_out.py`               | SF vs Excel vs Deck sidecar reconciliation |
| `5_update_obsidian_notes`        | `scripts/generate_obsidian_notes.py`        | Vault regeneration                         |

Outputs:

- `output/director_live_workbooks/2026-04-16/*.xlsx` — 9 per-director workbooks
- `output/sharepoint/FY26 Pipeline Review, All Territories.xlsx` — consolidated analytics
- `output/sharepoint/Dashboard and Q1 Analysis.xlsx` — dashboard widget raw + analytics
- `output/simcorp_director_decks/2026-04-16/land-only/*.pptx` — 9 director decks + `Exec Rollup.pptx`
- `obsidian/Monthly/2026-04/` — README, per-director `.auto.md`, `tie-out.md`

Most recent tie-out: **9 directors × 90 metrics, 0 mismatches.** See `obsidian/Monthly/2026-04/tie-out.md`.

---

## 2. Analytics tabs in the consolidated workbook (this session's work)

`FY26 Pipeline Review, All Territories.xlsx` now has an audit-ready analytics layer:

| Tab                                 | Purpose                                                                                                | Auditability                                                                                                                                                                                                                             |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Executive Insights** (position 0) | Analyst briefing table: # / Finding / Metric / Detail / Source                                         | Every Metric is a live formula (SUMIFS / MAX / COUNTIF). Source column is a real `HYPERLINK()` that jumps to evidence.                                                                                                                   |
| **Parameters** (position 1)         | Every risk weight and insight threshold with Defined Names (`RiskWeight_*`, `Thresh_*`)                | Named cells are addressable from formulas. Changing a value changes what downstream formulas see.                                                                                                                                        |
| Pipeline Pivot                      | Stage × Director stacked matrix + chart                                                                |                                                                                                                                                                                                                                          |
| ARR Concentration                   | Top 20 deals with cumulative % Pareto                                                                  |                                                                                                                                                                                                                                          |
| Pipeline Velocity                   | Q1/Q2 ARR at each Historical Trending snapshot date, per director                                      |                                                                                                                                                                                                                                          |
| Slip Risk by Owner                  | Top 25 by push count with ARR exposure                                                                 |                                                                                                                                                                                                                                          |
| Territory Scorecard                 | 12-KPI cross-director heatmap                                                                          |                                                                                                                                                                                                                                          |
| **Deal Risk Scoring**               | Composite 0-100+ score per open Land deal                                                              | New **Proof** column shows rule-by-rule contribution (e.g. `PUSH_HIGH(+40) + STALE(+15) + LOW_FCST(+15) + HIGH_VALUE_PUSH(+10) = 80`). Weights trace back to `RiskWeight_*` on Parameters.                                               |
| **Forecast Variance**               | Q1 pipeline bucket decomposition (Won / Lost / Added / RevisedUp / RevisedDown)                        | Every cell is a `SUMIFS('Q1 Trend Consolidated'!..., Territory, B5, Bucket, "Won")`. Helper columns on Q1 Trend Consolidated classify each deal; buckets are mutually exclusive; Check column reconciles to 0 per director and at TOTAL. |
| Q1 / Q2 Trend Consolidated          | 504 / 110 rows from HT reports                                                                         | Now carries helper columns: Initial ARR, Final ARR, Initial Stage, Final Stage, Bucket                                                                                                                                                   |
| Methodology                         | Expanded — every risk code, every threshold, bucket classification rules, "how to change" instructions |                                                                                                                                                                                                                                          |

`Dashboard and Q1 Analysis.xlsx` has 38 tabs including:

- Dashboard Overview (Sales Directors Monthly + Sales Ops Quarterly KPI — both dashboards)
- Q1 History Raw (6196 OpportunityFieldHistory events)
- Stage Transition Matrix, Stage Conversion, Time in Stage
- Loss Reasons, Stage at Loss
- Pipeline Inspection Raw + PI Summary
- Methodology

---

## 3. Per-director decks (18 slides each, APAC example)

`build_deck_from_excel.py` produces decks with the full slide sequence. New analytics slides introduced this session are read from `FY26 Pipeline Review, All Territories.xlsx` via `read_director_analytics(analytics_path, director_name)`:

| #   | Slide                                                           | Source                                                                                                                                    |
| --- | --------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Cover                                                           |                                                                                                                                           |
| 2   | **Executive Insights** (3-5 findings, territory-scoped prose)   | `_compute_director_insights()` reads per-director slices + compares thresholds                                                            |
| 3   | Executive Summary                                               | Director workbook                                                                                                                         |
| 4   | Q1 Promised vs Delivered                                        | Won/Lost FY26 Q1 Land subset                                                                                                              |
| 5   | **Q1 Forecast Variance** (bucket breakdown)                     | Q1 Trend Consolidated helper columns (SF formulas would be empty pre-open, so reader recomputes from Bucket column)                       |
| 6   | Q2 Outlook                                                      |                                                                                                                                           |
| 7   | Pipeline Overview & Stage 3+                                    |                                                                                                                                           |
| 8   | Top Deals                                                       |                                                                                                                                           |
| 9   | **Top Q2 Deals at Risk** (filtered to Apr-Jun 2026 close dates) | Deal Risk Scoring tab, per-director + Q2 scope                                                                                            |
| 10  | Pushed Deals & PI Link                                          |                                                                                                                                           |
| 11  | Q1 Movement                                                     |                                                                                                                                           |
| 12  | Forecast Accuracy & Breakdown                                   |                                                                                                                                           |
| 13  | Commercial Approvals                                            |                                                                                                                                           |
| 14  | Missing Approval Detail                                         |                                                                                                                                           |
| 15  | Renewals                                                        |                                                                                                                                           |
| 16  | Churn Risk                                                      | Auto-embeds `assets/rebekka-screenshots/{slug}-churn.png` if present; APAC has one (apac-churn.png), others fall back to the status table |
| 17  | Definitions                                                     |                                                                                                                                           |
| 18  | End                                                             |                                                                                                                                           |

**Template:** `~/archive/simcorp-deck-agent-backup/reference-decks/SimCorp_PPT_Template.pptx`
**Layouts:** `LY_TITLE_1=0`, `LY_TITLE_CONTENT=6`, `LY_2COL_GRAD=10`, `LY_4COL_GRAD=12`, `LY_END_SLIDE=31`

---

## 4. Recent fixes you should know about

**Q1 slip filter, 2026 only.** `extract_director_live.py` line ~592 now requires `change_date >= "2026-01-01"` in addition to the existing "old close in Q1 + new close after Q1" rule. Legacy deals that had a Q1 2026 close date set in 2025 and got pushed before FY26 even started are now excluded. User asked for this explicitly because they want to see what slipped _this year_.

**Zero truncation in the deck.** Every `_trunc(...)` call in `build_deck_from_excel.py` was removed. `_add_table()` now sets `cell.text_frame.word_wrap = True` on every cell so long values wrap instead of clipping. Verified with a regex scan: 0 ellipses across all 10 decks.

**APAC churn screenshot.** Extracted Rebekka's Churn Risk slide 10 picture from `/Users/test/Downloads/Sales Director Monthly - Jesper Tyrer (APAC).pptx` to `assets/rebekka-screenshots/apac-churn.png`. `slide_churn(prs, territory=...)` embeds it when a matching `{slug}-churn.png` exists. Same mechanism would work for other territories if you have their screenshots.

**Obsidian synthesis.** `generate_obsidian_notes.py` now reads from the FY26 Pipeline Review workbook and pushes analytics findings into `Monthly/YYYY-MM/README.md` (ARR concentration, velocity, slip top 10, transitions) and per-director `.auto.md` files (Q1/Q2 velocity trajectory).

---

## 5. Open items the user raised, in order

### a. Deck is too long, compared to Rebekka's 16-slide version

Our current deck is 18 slides. Rebekka's key differences:

- Her slide titles ARE the finding ("Pipeline: EUR 6.6M open. 41% in 6 - Contracting — conversion to later stages is key.")
- Our titles are category labels ("Q1 Forecast Variance", "Top Deals")
- She threads findings through existing slides; we add standalone slides

**User has not yet chosen a merge path.** The open proposal is:

1. Delete standalone Executive Insights slide, fold bullets into Executive Summary headline
2. Merge Forecast Variance INTO Q1 Promised vs Delivered
3. Merge Deal Risk Scoring INTO Top Deals as extra columns
4. Rewrite every slide title as a data-forward sentence, Rebekka-style

If you're picking this up, ask the user which of those 4 moves they want. Don't assume all four.

### b. Missing high-value analyses

User asked what high-value data from the Excel didn't make it to the deck. Answer:

**High value, not in deck:**

- **Loss Reasons + Stage at Loss** — answers the #1 exec question ("why are we losing?") that nothing else on the deck covers. For Jesper Q1 Land: 14 losses, 5.5M EUR; 9 of 14 have no reason code (hygiene fail); real exposure is 4 deals at Discovery+ totalling 5.3M.
- **Slip Risk by Owner** (territory-filtered) — management actionable.
- **Stage Conversion Funnel** — shows Prospecting is the leakiest stage (28% advance, 2% win).

**Preview deck already built:** `output/simcorp_director_decks/2026-04-16/preview/apac-missing-analyses.pptx` (5 slides: cover + 3 analyses + end).

**Scope contract:** preview now reconciles to main deck — Q1 2026 close date + Land only + one director. Jesper shows **14 losses, 5.5M** matching slide 4 of his deck exactly.

**User has not yet decided** whether to wire these into the main deck.

### c. Extraction of per-territory churn screenshots

Only APAC has one. If the user wants the other 8 directors to carry similar screenshots from their prior monthly packs, add the PNG files to `assets/rebekka-screenshots/` using the slug convention `{territory-slug}-churn.png` where the slug is lowercased and hyphenated (e.g. `emea-central-churn.png`, `uk-ireland-churn.png`). `slide_churn()` picks them up automatically.

### d. Obsidian notes are incomplete — missing analytics wiring

The Obsidian generator `scripts/generate_obsidian_notes.py` pushes SOME of the new analytics into the vault but not all. User asked for this to be finished. Specific gaps and where to fix them:

**In `Monthly/YYYY-MM/README.md`** — `write_monthly_summary()` currently writes ARR concentration / pipeline-by-stage / pipeline velocity / slip risk top 10 / stage transitions. **Missing sections to add:**

1. **Deal Risk top 10** — top scored deals from `Deal Risk Scoring` tab in FY26 Pipeline Review, with Proof column contents so the score rationale is in the markdown. Include the triage-threshold count (COUNTIF-equivalent).
2. **Forecast Variance summary** — total Initial, Final, Net Delta, and the Won/Lost/Added/Revised bucket totals from the `Forecast Variance` TOTAL row. Include a one-line "Q1 pipeline shrank EUR Xm, loss-driven / win-driven / balanced" narrative from the thresholds on Parameters.
3. **Q1 Land losses by reason** — aggregated across all 9 directors: reason code × count × ARR. This is the global view of "Why We Lost" that the preview deck shows per-director.
4. **Parameters tab link** — one line pointing readers to the audit surface: "Risk weights and insight thresholds: `output/sharepoint/FY26 Pipeline Review, All Territories.xlsx` → Parameters tab."

**In `Monthly/YYYY-MM/{slug}.auto.md`** — `write_monthly_director()` currently has Headline / Top open deals / Push concentration / Pipeline velocity. **Missing sections to add:**

1. **Top Q2 deals at risk** — Deal Risk Scoring filtered to this director AND Q2 close date (same scope as deck slide 9). Each entry with Score, Account, Opportunity, ARR, Reasons.
2. **Q1 Land losses by reason + stage at loss** — reuse the logic from `build_missing_analyses_preview.py` (`_q1_land_losses_for_director` + `_highest_stage_per_opp`). Output two tables: Loss Reason / Count / Lost ARR, and Stage Reached / Count / Lost ARR.
3. **Churn screenshot reference** — if `assets/rebekka-screenshots/{slug}-churn.png` exists, add a line: "Churn view imported from prior monthly pack — see deck slide 16."

**In `obsidian/methodology.md`** — add two paragraphs:

1. **Q1 slip filter change (2026-04-16):** document that Q1 Slipped classification in `extract_director_live.py` now requires the push CreatedDate to be on or after 2026-01-01. Legacy deals that had a Q1 2026 close date set in 2025 and got pushed before FY26 started are excluded. Reason: the review is about what slipped THIS year.
2. **Audit-ready workbook model:** point to the Parameters tab for all risk weights and thresholds (Defined Names `RiskWeight_*` and `Thresh_*`), explain that Executive Insights Metric column is live formulas (SUMIFS / MAX / COUNTIF), Source column is HYPERLINK, Forecast Variance is SUMIFS against Q1 Trend Consolidated Bucket helper column, Deal Risk Scoring has a Proof column showing each triggered rule.

**Implementation pointers:**

- The FY26 Pipeline Review workbook path is already available in `generate_obsidian_notes.py` via `SHAREPOINT` constant.
- `_read_analytics_workbook()` already reads top deals / concentration / velocity / slip / scorecard / transitions. Extend it to also read Deal Risk Scoring (rows 5-34, 14 columns including Proof) and Forecast Variance (TOTAL row).
- For per-director Q1 losses, import `_q1_land_losses_for_director` from `build_missing_analyses_preview.py` OR inline the logic (read Won Lost FY26, filter Land + Q1 2026 close + not-Won).
- For stage-at-loss per director, cross-reference `Q1 History Raw` in the Dashboard workbook — sample pattern is in `build_missing_analyses_preview.py::_highest_stage_per_opp`.

**Estimated work:** ~30 min. Three edits to `generate_obsidian_notes.py`, one edit to `obsidian/methodology.md`. Then re-run `python3 scripts/generate_obsidian_notes.py --date 2026-04-16` to regenerate. No pipeline rebuild needed since the underlying workbooks already contain all the data.

---

## 6. User preferences and working style

Read these before you respond.

**Default to action, not questions.** User has explicitly corrected me multiple times for asking "should I..." when I should just pick and execute. Pick the most reasonable move, do it, report briefly. No A/B/C/D option menus unless the cost of picking wrong is high.

**No AI-voice output.** User flagged when the Excel looked "AI-generated." Response was to:

- Move all thresholds to a visible Parameters tab with Defined Names
- Make every derived number a live formula (SUMIFS, MAX, COUNTIF, HYPERLINK)
- Add Proof column to Deal Risk Scoring so scores are traceable
- Label everything with rationale in Methodology

If you add new analytics, follow this pattern: **thresholds in Parameters, formulas in cells, hyperlinks on references, rationale in Methodology**. Do not hardcode magic numbers in the prose.

**No truncation anywhere.** User pasted "!!!!" when they saw `...` in a deck cell. Global rule: no `_trunc()` in deck builders. If a column is too narrow, widen it; if the table won't fit, drop a column — don't clip.

**Scope must reconcile.** If a new slide uses a different time/type/geography filter than the rest of the deck, numbers will disagree with adjacent slides. User caught this immediately when the preview deck showed 45 losses vs the main deck's 14 losses. **Always match main deck scope (Land / Q1 / one director) unless you explicitly label the new scope.**

**Codebase instructions:** `CLAUDE.md` at repo root has CLI-first workflow rules, PATCH-not-PUT Wave API gotchas, SAQL quirks. You will rarely touch those since this project is Excel/PowerPoint ETL, but know they exist.

**Memory system at:** `/Users/test/.claude/projects/-Users-test/memory/` — user has persistent notes across sessions. Check `MEMORY.md` on startup; it has entries like "Never change viz types without explicit approval," "Extract before designing," "Verify handoff/prior-session claims before acting."

---

## 7. Running the pipeline for a new snapshot

```bash
cd /Users/test/crm-analytics

# Full rebuild (extract + analyze + decks + tie-out + obsidian)
python3 scripts/run_monthly_director_review.py --date $(date +%Y-%m-%d)

# Analyze-only (skip SF extract, use today's data on disk)
python3 scripts/run_monthly_director_review.py --date 2026-04-16 --skip-extract

# Just decks (if analytics already fresh)
python3 scripts/run_monthly_director_review.py --date 2026-04-16 --skip-extract --skip-analysis

# Single deck
python3 scripts/build_deck_from_excel.py \
  --workbook output/director_live_workbooks/2026-04-16/jesper-tyrer.xlsx \
  --output output/simcorp_director_decks/2026-04-16/land-only/jesper-tyrer-LAND.pptx \
  --land-only

# Preview deck (the 3 missing analyses)
python3 scripts/build_missing_analyses_preview.py
```

Auth: `sf org display --target-org apro@simcorp.com --json` — no `.env` required.

---

## 8. Files you'll edit most often

- `scripts/build_sharepoint_analysis.py` (3800 lines) — all FY26 consolidated analytics + new tabs. This is where Exec Insights, Deal Risk, Forecast Variance, Parameters, Methodology live.
- `scripts/build_deck_from_excel.py` (2800 lines) — all per-director deck slides. Helpers `_add_table`, `_set_ph`, `_fmt_eur`, `_meur`, `_unw`, `read_director_analytics`, `_compute_director_insights`.
- `scripts/build_dashboard_analysis_excel.py` (1600 lines) — second workbook builder.
- `scripts/extract_director_live.py` — SOQL extractor. `TERRITORIES` dict at line 35 is the config knob; Q1 Movement at line 537.
- `scripts/build_missing_analyses_preview.py` — standalone preview generator for the un-merged analyses.
- `scripts/generate_obsidian_notes.py` — vault regeneration.
- `obsidian/sf-reports-index.md` — every report ID and PI list view mapped to a director.
- `obsidian/methodology.md` / `obsidian/runbook.md` — operator docs.

---

## 9. Suggested first prompt to verify state

Ask the user: _"Pipeline state confirmed: 16/16 ok, 0 mismatches, 0 truncations. Three open decisions from prior session: (a) Rebekka-style slide-merge to shorten the deck, (b) whether to wire the Loss Reasons / Stage at Loss / Slip Risk Owners slides into the per-director deck, (c) per-director churn screenshots for non-APAC territories. Which do you want to tackle first?"_

Do not re-run the pipeline unless asked. The user just wants you to pick up the conversation with full context. Verify any claim in this doc against the filesystem before acting on it (`git log`, `ls`, reading files) — handoff docs can lie.
