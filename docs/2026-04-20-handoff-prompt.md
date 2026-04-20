# Handoff Prompt — Paste This to Start the Next Session

---

You're picking up the Sales Director Monthly deck pipeline for SimCorp. This is an end-to-end automated ETL: Salesforce → Excel workbooks → PowerPoint decks → Obsidian knowledge base, producing 9 per-director decks + 1 exec rollup for monthly sales reviews.

## Context

Read these files first (in order):

1. `docs/2026-04-20-final-review-handoff.md` — current state, what needs finishing, review checklist
2. `docs/2026-04-17-sd-monthly-deck-handoff.md` — full architecture, all scripts, data methodology, SF org details, session history
3. `CLAUDE.md` — project rules (CLI-first, no MCP, PATCH not PUT, auth via sf org display)
4. `config/sd_monthly_territories.json` — the 9 directors and their territory filters

## What You Need to Do

### Step 1: Fresh rebuild

Run the full pipeline with fresh SF data:

```bash
python3 scripts/run_monthly_director_review.py --date 2026-04-20
```

This runs: extract (9 directors from live SF) → analytics workbooks (master + 9 regional) → decks (9 + exec rollup) → obsidian notes → data quality audit. Takes ~10 minutes. Watch for errors in the pipeline logs at `output/pipeline_logs/2026-04-20/`.

### Step 2: Review every deck

Open each of the 9 decks in `output/simcorp_director_decks/2026-04-20/land-only/` and verify against the checklist in `docs/2026-04-20-final-review-handoff.md`. Key things:

- Approval numbers correct (exempt "No Approval Necessary" deals not flagged as missing)
- "Pending Approval" label (not "Conditionally Approved")
- MoM slide title says "Since last review ({date}): what moved" with FX note
- Owner Coaching table uses human-readable labels (not raw codes like PUSH_HIGH)
- Q2 Forward Look has forecast momentum + stage advances section
- No blank slides, no broken layouts, slide count ~16-18

### Step 3: Run audits

```bash
python3 scripts/audit_deck_scope.py --date 2026-04-20
python3 scripts/validate_tie_out.py
python3 scripts/audit_data_quality.py --date 2026-04-20
```

All three should pass clean. If tie-out shows mismatches, investigate — the sidecar JSON next to each deck has the numbers the deck was built from.

### Step 4: Fix any issues found in review

Common issues from prior sessions:

- Multi-slide overflow: Pushed Deals, Commercial Approvals, Forecast Accuracy can each produce 2 slides. If deck is >20 slides, truncate table rows in those functions.
- Churn slide is placeholder (static text) — leave it or cut it, Andre's call.
- If a director has zero Q2 deals, Q2 Forward Look will be skipped — that's correct behavior.

### Step 5: Commit

The repo has 346 uncommitted files. All core pipeline scripts are untracked. Structure commits logically:

1. Core pipeline scripts (extract, build, audit, validate, orchestrator)
2. Config files (territories, presets, registry)
3. Docs (handoff docs, runbooks, audits)
4. CI/CD (GitHub Actions workflow)
5. Output files if Andre wants them tracked (usually not)

## Important Context

- **Auth**: `sf org display --target-org apro@simcorp.com --json` gives you accessToken + instanceUrl. No .env file.
- **Template**: `~/archive/simcorp-deck-agent-backup/reference-decks/SimCorp_PPT_Template.pptx`
- **ARR field**: `APTS_Opportunity_ARR__c` (unweighted, wrapped in convertCurrency for EUR). Rebekka uses `APTS_Forecast_ARR__c` (weighted, 3-7x lower) — gap documented but not resolved.
- **Approval model**: 4 states — No Approval Necessary (exempt), Approved, Pending Approval, Missing Approval. The fix for this is already in the code.
- **Directors**: Jesper (APAC), Sarah (CE), Dan (UKI), Francois (SW), Christian (NE), Mourad (MEA), Megan (Canada), Patrick (NA AM), Adam (NA P&I — US only, not global).
- **Andre's style**: He wants action, not questions. Do the work, report briefly. No option menus. No "should I...". If something breaks, fix it and say what you did in one sentence.
- **Never change viz types** (funnel, donut, bar, etc.) without Andre explicitly asking.
- **Verify, don't trust**: Prior session claims about scores, file contents, or job state can be wrong. Always check the actual files.

## Definition of Done

- All 9 decks + exec rollup rebuilt with latest code from fresh SF extract
- Every deck visually reviewed — no broken slides, correct numbers, correct labels
- Audit scripts pass clean (scope audit, tie-out, data quality)
- Regional SharePoint workbooks match their deck numbers
- Core scripts committed to git
- Andre signs off after reviewing 1-2 sample decks
