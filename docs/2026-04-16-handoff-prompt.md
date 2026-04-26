# Handoff prompt — paste this into the new session

Copy the block below into Opus 4.7 as the first message. It's self-contained; the new model will read the full handoff doc before acting.

---

You're taking over a Sales Director Monthly ETL project from Opus 4.6. Working directory is `/Users/test/crm-analytics`. Current run date in use is `2026-04-16`.

**First step: read `docs/2026-04-16-handoff-opus47.md` in full.** It documents the pipeline, every analytics tab, the deck structure, recent fixes, open decisions, and user preferences. Do not skip it.

**Current state (verified at handoff time):**

- Pipeline: 16/16 stages `ok`, tie-out 0 mismatches, 0 truncation across all 10 decks
- 9 per-director decks + 1 exec rollup in `output/simcorp_director_decks/2026-04-16/land-only/`
- Two consolidated workbooks in `output/sharepoint/` with audit-ready analytics (Parameters tab with Defined Names, SUMIFS formulas, HYPERLINK jumps, Proof column on Deal Risk Scoring)
- APAC deck has a real churn screenshot on slide 16 (sourced from Rebekka's prior monthly pack)
- Preview deck for un-merged analyses at `output/simcorp_director_decks/2026-04-16/preview/apac-missing-analyses.pptx`

**Four open decisions waiting for the user** (see handoff §5):

1. Rebekka-style deck shortening — 4 merge moves proposed (Exec Insights into Exec Summary, Forecast Variance into Q1 Promised, Deal Risk into Top Deals, rewrite titles as data-forward sentences). None chosen yet.
2. Whether to wire the preview deck's 3 analyses (Loss Reasons + Stage at Loss, Slip Risk Owners, Stage Conversion Funnel) into the main per-director deck.
3. Per-director churn screenshots for the other 8 territories (asset drop to `assets/rebekka-screenshots/{slug}-churn.png`).
4. **Obsidian wiring is incomplete** — §5d has a 30-min punch list to finish it (Deal Risk / Forecast Variance / Q1 loss reasons into Monthly README; Q2 at-risk deals + Q1 loss breakdown + churn reference into per-director auto.md; two methodology paragraphs).

**User preferences to internalize before you respond:**

- Default to action, not questions. No A/B/C/D menus. Pick the reasonable move, do it, report briefly.
- No AI-voice output. Thresholds go to visible Parameters tab; numbers are live formulas; insights cite sources via HYPERLINK; rationale lives in Methodology. Don't hardcode magic numbers in prose.
- No truncation anywhere. Never use `_trunc()` in deck builders.
- Scope must reconcile. If a new slide uses a different time/type/geography filter than adjacent slides, numbers disagree and the user will catch it.
- Verify handoff claims against the filesystem before acting on them. Handoff docs can hallucinate.
- User's auto-memory lives at `/Users/test/.claude/projects/-Users-test/memory/MEMORY.md` — loaded into your context automatically.

**Suggested first response to the user:**

> "Pipeline state confirmed: 16/16 ok, 0 mismatches, 0 truncations. Four open decisions from prior session: (a) Rebekka-style slide-merge to shorten deck, (b) wire preview-deck analyses into main deck, (c) per-director churn screenshots for non-APAC territories, (d) finish Obsidian wiring (~30 min punch list in handoff §5d). Which do you want to tackle first?"

Do not re-run the pipeline unless asked. Do not take autonomous action on the four open decisions — confirm direction first, then execute in full.
