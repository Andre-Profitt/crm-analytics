# SD Monthly Deck — Final Review & Rebuild Handoff

**Date:** 2026-04-20
**Goal:** Do one final review pass and rebuild all 9 director decks + exec rollup + SharePoint workbooks with latest code, then close out the workstream.

---

## Current State

### What's Done

- Full ETL pipeline built and working: SF → Excel → Deck → Obsidian
- 9 per-director decks + 1 exec rollup generated (Apr 16 run)
- 10 regional + 1 master SharePoint analytics workbooks generated (Apr 17)
- Data quality audit system (35+ checks), tie-out validation, deck scope audit
- GitHub Actions CI/CD at midnight 1st of each month
- Rebekka's style feedback integrated (table formatting, data-forward titles, mEUR, trimmed slide count)

### What Needs Finishing

**1. Full rebuild required.** Only Jesper/APAC was rebuilt after the final code fixes. The other 8 directors + exec rollup are stale (built Apr 16 19:28-19:30, BEFORE these fixes):

| Fix                                                                               | Files Changed                                                                   | Impact                                      |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------- |
| Approval classification: "No Approval Necessary" exempt, "Pending Approval" label | `extract_director_live.py`, `build_deck_from_excel.py`, `audit_data_quality.py` | Approval counts were wrong on 8 decks       |
| MoM slide title/subtitle fix + FX rate note                                       | `build_deck_from_excel.py`                                                      | MoM title was confusing                     |
| REASON_CODE_LABELS for owner coaching                                             | `build_deck_from_excel.py`                                                      | Coaching table had raw codes not human text |
| Q2 Forward Look: forecast momentum + stage advances                               | `build_deck_from_excel.py`                                                      | New data on Q2 slide                        |

**2. Review pass.** After rebuild, open each deck and verify:

- Slide count is ~16-18 (some overflow to ~21 due to multi-slide functions)
- Approval numbers look correct (no massive "Missing" counts)
- MoM slide renders correctly (or is skipped cleanly if no prior snapshot)
- Q2 Forward Look has enrichment data (activity, AuM, competitor, momentum)
- Owner Coaching table uses human-readable reason labels
- No blank slides or broken layouts
- Regional workbooks match their deck's numbers (4-way tie-out)

**3. SharePoint workbooks rebuild.** Regional workbooks were built Apr 17 but may need refresh if extract data is stale. The master workbook should be rebuilt from fresh extracts.

**4. Git commit.** 346 uncommitted files. All core pipeline scripts are `??` (untracked). Need structured commits.

---

## How to Rebuild Everything

### Option A: Full pipeline (extract + analyze + decks, ~10 min)

```bash
python3 scripts/run_monthly_director_review.py --date 2026-04-20
```

### Option B: Skip extract, rebuild from existing workbooks (~3 min)

```bash
# Use the Apr 16 extracts (data is 4 days old but code fixes are what matter)
python3 scripts/run_monthly_director_review.py --date 2026-04-16 --skip-extract
```

### Option C: Fresh extract + rebuild (recommended for final delivery)

```bash
python3 scripts/run_monthly_director_review.py --date 2026-04-20
```

Then review output at:

- Decks: `output/simcorp_director_decks/2026-04-20/land-only/`
- SharePoint: `output/sharepoint/`
- Logs: `output/pipeline_logs/2026-04-20/`

---

## Review Checklist (Per Deck)

For each of the 9 directors, open the `.pptx` and check:

- [ ] **Cover** — correct director name, territory, reporting period
- [ ] **Exec Summary** — KPI numbers populated, insight bullets present
- [ ] **MoM** — either shows deltas from prior snapshot or is cleanly skipped
- [ ] **Q1 Promised vs Delivered** — won/lost/pipeline decomposition makes sense
- [ ] **Why We Lost** — reason codes and competitor breakdown populated
- [ ] **Q2 Outlook** — summary numbers for Q2
- [ ] **Q2 Forward Look** — per-deal readiness grid, forecast momentum, stage advances
- [ ] **Top Deals** — top 10 by ARR, no zero-ARR entries
- [ ] **Pushed Deals** — PI link works, pushed deal tiers (Critical/Watch/Early)
- [ ] **Commercial Approvals** — "Pending Approval" label (not "Conditionally Approved"), exempt deals not flagged
- [ ] **Renewals** — Q2 renewals populated
- [ ] **No blank/broken slides**

### Cross-Check

- [ ] Run `python3 scripts/audit_deck_scope.py --date 2026-04-20` — all claims pass
- [ ] Run `python3 scripts/validate_tie_out.py` — 0 mismatches
- [ ] Spot-check 2-3 regional workbooks against their deck sidecar JSON

---

## Known Issues to Decide On

1. **Multi-slide overflow** — Pushed Deals, Commercial Approvals, Forecast Accuracy can produce 2 slides each. Deck ends up ~21 instead of ~17. Options: truncate table rows, or accept the extra slides.
2. **Churn slide is placeholder** — Static text, no real data. Alex P's Finance feed is the blocker. Options: keep as-is, or cut it entirely.
3. **Forecast ARR vs Opportunity ARR** — Pipeline uses unweighted Opp ARR; Rebekka's reference uses weighted Forecast ARR. Numbers differ 3-7x. Not reconciled. For final delivery, pick one and document.

---

## Key Files

| File                                                                            | Lines | Purpose                                               |
| ------------------------------------------------------------------------------- | ----- | ----------------------------------------------------- |
| `scripts/extract_director_live.py`                                              | 1,143 | SF → Excel extract                                    |
| `scripts/build_sharepoint_analysis.py`                                          | 5,720 | Consolidated + regional analytics workbooks (42 tabs) |
| `scripts/build_deck_from_excel.py`                                              | 3,852 | Excel → SimCorp deck (16-18 slides)                   |
| `scripts/build_exec_rollup_deck.py`                                             | 620   | CRO rollup deck                                       |
| `scripts/run_monthly_director_review.py`                                        | 371   | Orchestrator                                          |
| `scripts/generate_obsidian_notes.py`                                            | 1,357 | Obsidian vault + MoM ledger                           |
| `scripts/audit_data_quality.py`                                                 | 1,228 | 35+ hygiene checks                                    |
| `scripts/validate_tie_out.py`                                                   | 679   | 4-way reconciliation                                  |
| `config/sd_monthly_territories.json`                                            | —     | Territory → director + SOQL + PI config               |
| `~/archive/simcorp-deck-agent-backup/reference-decks/SimCorp_PPT_Template.pptx` | —     | Deck template                                         |

---

## Rules (from CLAUDE.md + Andre's feedback)

- **CLI-first**: `sf` CLI + `curl` + `requests`. No MCP tools for CRM Analytics.
- **Never change viz types** without explicit approval.
- **Never edit build\_\*.py** in the old sense — but `build_deck_from_excel.py` and `build_sharepoint_analysis.py` ARE the current pipeline (confusing naming, they're fine to edit).
- **PATCH not PUT** for Wave API.
- **Auth**: `sf org display --target-org apro@simcorp.com --json` — no .env.
- **Verify handoff claims** — don't trust prior session claims about scores/file contents. Check the actual files.
- **Dashboard filters > report overrides** — set at dashboard level, SF cascades.
- **Extract before designing** — build from actual org metadata, don't invent thresholds.
