# Next Session Handoff — KPI Reports Refresh + Builder Modernization

> **For the next Claude session.** Paste this entire file contents into the first message. Everything you need to pick up cleanly is here. Do NOT re-do the discovery work that's already captured in the artifacts below — read them first.

---

## Who you are and what just happened

You're picking up a Salesforce KPI reports refresh task on `~/crm-analytics`. The prior session (2026-04-06) did a lot of investigation and built scaffolding, but the actual data refresh is paused because the underlying KPI dataset producers (Python builders) need modernization first.

The next thing you should do is **brainstorm the builder modernization spec** using `superpowers:brainstorming`. Do not start coding before that brainstorm produces a spec and plan.

## Required reading (in this order)

Read these BEFORE doing anything else. They contain the load-bearing context:

1. `~/.claude/projects/-Users-test/memory/project_kpi_reports_refresh_2026-04-06.md` — full session state (auto-loaded via MEMORY.md)
2. `~/.claude/projects/-Users-test/memory/feedback_no_python_builders.md` — Andre's hard rule about builder files
3. `~/.claude/projects/-Users-test/memory/user_goals_and_style.md` — Andre's working style and what NOT to do
4. `~/crm-analytics/docs/2026-04-06-builder-assessment.md` — read-only assessment of the 8 KPI dataset producers (THE input for your brainstorm)
5. `~/crm-analytics/docs/adr/ADR-0001-kpi-reports-data-backbone.md` — architectural decision (CRMA primary, SF reports as link layer)
6. `~/crm-analytics/docs/superpowers/specs/2026-04-06-kpi-reports-refresh-design.md` — the refresh design that's currently paused
7. `~/crm-analytics/docs/superpowers/plans/2026-04-06-kpi-reports-refresh.md` — the implementation plan that's currently paused
8. `~/crm-analytics/CLAUDE.md` — repo conventions (Wave API gotchas, SAQL rules, no MCP / no Python builders)

## State of the world

### Branch + commits

- **Branch:** `main`
- **Position:** 16 commits ahead of `origin/main`, none pushed
- **Working tree:** has 13+ user-modified files and 70+ untracked files that are NOT mine — DO NOT touch them, DO NOT stage them
- **Recent commits to verify with `git log --oneline -16`:**
  - `f7385cb docs: builder assessment for the 8 KPI dataset producers`
  - `8876d7e fix: harden run_report2_quarterly_default.sh against JSON injection`
  - `bf76691 feat: add run_report2_quarterly_default.sh wrapper`
  - `3866674 fix: code quality cleanup on run_report2_saql_refresh.py`
  - `1fedb6a fix: drop unused pytest import + pyright suppress for runtime sys.path`
  - `f3a69ce feat: add run_report2_saql_refresh.py for Sales Ops SAQL refresh`
  - `7036ead chore: gitignore .worktrees/ for isolated feature work`
  - `0537776 plan: KPI reports refresh implementation plan — snapshot 2026-04-01`
  - `2c212cb spec: KPI reports refresh design — snapshot 2026-04-01`
  - `d9dd349 adr: ADR-0001 KPI reports data backbone — CRMA primary, SF reports as link layer`

### Org-side state (durable, not in repo)

The prior session fixed real org issues that were unrelated to the deck refresh but were silently breaking the data sync layer:

- ✅ **Opportunity (Replication) data sync** is now passing. Was failing every 8 hours since 2026-04-03 because 4 deleted custom fields (`Competitors__c`, `IPP_Score__c`, `Public_Tender__c`, `Pipeline_Category__c`) were still in the replication field list. Fixed via `PATCH /wave/replicatedDatasets/0Iu57000000fxVCCAY/fields`.
- ✅ **Campaign (SFDC_LOCAL)** same fix pattern. 3 deleted fields removed via `PATCH /wave/replicatedDatasets/0Iu2o000000fxWwCAI/fields`.
- ✅ **DF_Opp_Mgmt, DF_Revenue_Motions, DF_Contract_Operations** dataflows triggered and ran successfully via Wave API. Only `Opp_Mgmt_KPIs` dataset was actually refreshed (the others produce datasets that aren't consumed by either KPI deck).
- ⏳ **Account/Contact/Lead/LandingPage/MarketingLink replications** are still cascade-cancelled from the original failure. They'll recover on the next 22:00 UTC scheduled run (no action needed).

### Why the refresh is blocked

The 8 KPI datasets the decks consume are NOT produced by CRMA dataflows or recipes — they're produced by Python builder scripts that fetch via SOQL and upload via the External Data API. Andre has a hard rule against me touching `build_*.py` files. He authorized a one-time read-only assessment (saved at `docs/2026-04-06-builder-assessment.md`) and explicitly chose to plan modernization first rather than running the existing builders as-is.

### Key dataset staleness as of 2026-04-06

```
Customer_Account_Health           2026-03-06  → build_customer_account_health.py
Pipeline_Opportunity_Operations   2026-03-08  → build_pipeline_opportunity_operations.py
Commercial_Rhythm_Control_Tower   2026-03-12  → build_commercial_rhythm_control_tower.py
Revenue_Retention_Health          2026-03-12  → build_revenue_retention_health.py
Account_Intelligence              2026-03-16  → build_account_intelligence.py
Opp_Mgmt_KPIs                     2026-04-06  → DF_Opp_Mgmt (refreshed last session)
Forecast_Intelligence             2026-03-24  → build_forecasting.py
Forecast_Revenue_Motions          2026-03-24  → build_forecast_revenue_motions.py
Executive_Revenue_Source_Truth    2026-03-24  → scripts/build_source_truth_executive_revenue.py
```

## Andre's hard rules (from feedback memory)

Do not violate any of these:

1. **NEVER edit `build_*.py` files.** Read them only when explicitly authorized for assessment. Modernization work happens via brainstorm → spec → plan and the user authorizes file changes individually.
2. **NEVER use MCP tools.** Use `sf` CLI, `curl`, and `python3 requests` directly. CLI to CLI.
3. **NEVER use `git add .` / `git add -A` / `git add -u`.** The working tree has many user-WIP files. Stage by exact path only.
4. **NEVER push to origin** unless Andre explicitly says so.
5. **Don't summarize what was just done** (Andre reads the diff). Be terse, lead with action and decisions.
6. **Don't propose research harness plumbing when the user wants product work.**
7. **Auth via `sf org display --target-org apro@simcorp.com --json`** — no `.env`, no hardcoded credentials.

## Your immediate task

**Brainstorm the builder modernization spec.** Invoke `superpowers:brainstorming` as your first action after reading the required artifacts.

The brainstorm input is the prioritized shopping list at the bottom of `docs/2026-04-06-builder-assessment.md`. The summary of what needs to happen:

### Builder modernization scope (the prior session's recommended priority order)

1. **Centralize SOQL field references** into a `simcorp_fields.py` constants module with a startup `describe` check that fails fast if the org schema diverges. Eliminates the next deleted-field crash class.
2. **Add a shared `RunSummary` dataclass** that every builder writes to `runs/<dataset>/<timestamp>.json` with `row_count`, `byte_count`, `runtime_s`, `dataset_id`, `dataset_version_id`, `errors`. Operator audit trail.
3. **Replace `print()` with `logging.getLogger(__name__)`** + a default `logging.basicConfig` in helpers. Levels, timestamps, structured.
4. **Fix the 2 SimCorp fiscal-quarter bugs:**
   - `build_forecast_revenue_motions.py:157-163` — `_quarter_from_month_key()` uses calendar quarters; affects `record_type=trend` rows in `Forecast_Revenue_Motions`.
   - `scripts/build_source_truth_executive_revenue.py:168-169` — calendar quarters AND wrong-FY month-date stamps; affects every row in `Executive_Revenue_Source_Truth`.
5. **Decompose the 600+ line monsters** in `build_pipeline_opportunity_operations.py`, `build_forecast_revenue_motions.py`, `build_revenue_retention_health.py`, `build_account_intelligence.py`, `build_customer_account_health.py`. Extract per-page or per-record-type sub-builders.
6. **Delete dead code:** `legacy_*` functions in `build_pipeline_opportunity_operations.py` and the duplicate `build_steps` defs in `build_customer_account_health.py:1396` vs `:1803`.
7. **Migrate `build_revenue_retention_health.py:110-123`** from its custom `run_soql()` to the shared `_soql()` helper for retry parity.
8. **Remove the hardcoded `sys.path.insert(0, "/Users/test/crm-analytics")`** at `build_revenue_retention_health.py:23`.
9. **Add tests** — pure transformer functions (`_stage_band`, `_risk_band`, `_health_score`, `_quarter_from_month_key`, etc.) are easy to extract and unit-test.
10. **Replace `urllib` with `requests.Session`** in `crm_analytics_helpers.py` for connection pooling and better error messages.
11. **Add token refresh** — re-auth on 401 mid-run for long builders (`customer_account_health` makes 4 SOQL pulls).
12. **Add a `make refresh-kpi-data` target** so the operator runs one command instead of remembering 8 file paths.

The brainstorm should help Andre decide which subset of these to tackle in the first iteration. Items 1-4 are likely the highest-leverage starting set. Items 5-12 are real follow-ups.

### Brainstorm process per the skill

1. Invoke `superpowers:brainstorming`
2. Don't create a new design from scratch — use the assessment as your starting point
3. Ask Andre clarifying questions ONE AT A TIME via `AskUserQuestion`:
   - Scope: which subset of items 1-12 in this iteration?
   - Constraint: are we modifying ALL 8 builders or carving out a "safe to touch first" subset?
   - Sequencing: can we land item 1 (field constants) standalone or does it need item 2 (RunSummary)?
   - Test strategy: pure-function unit tests via pytest, integration tests against the live org, both?
4. Propose 2-3 approaches with trade-offs
5. Present the design, get approval
6. Write the spec to `crm-analytics/docs/superpowers/specs/2026-04-XX-builder-modernization-design.md`
7. Self-review, get user approval
8. Transition to `superpowers:writing-plans`

## What NOT to do

- ❌ Do NOT re-investigate the CRMA dataflow status. The prior session already did the full forensics and the dataflow remediation is durable. Read the memory file for the findings.
- ❌ Do NOT try to refresh the KPI datasets via `/wave/dataflowjobs` triggers. Already proven to NOT work — only `Opp_Mgmt_KPIs` is dataflow-produced.
- ❌ Do NOT execute any `build_*.py` script. The user explicitly chose modernization-first.
- ❌ Do NOT touch any `build_*.py` file before brainstorming → spec → plan → approval.
- ❌ Do NOT try to fix the Page 6 SAQL schema mismatch — that's downstream of the modernization decision, not blocking.
- ❌ Do NOT create another worktree. The prior session tried and the worktree pattern doesn't work for this repo (too many gitignored dependencies in `output/` and untracked scripts in `scripts/`). Work directly on `main` with strict staging discipline.
- ❌ Do NOT distribute either of the existing 2026-04-01 deck artifacts. Report 1's pptx is labelled 2026-04-01 but contains 2026-03-08-ish data; it's misleading. Either delete it or keep it ONLY as a rendering pipeline test.
- ❌ Do NOT rerun the refresh plan from `docs/superpowers/plans/2026-04-06-kpi-reports-refresh.md` until builder modernization is complete.

## Two queued follow-ups (do NOT start until builder modernization lands)

These are scope items Andre flagged AFTER the refresh works. Track them but don't start them now:

1. **SimCorp branding on both decks** — use the `simcorp-presentation-style` skill. The decks currently render with default `pptxgenjs` styling. Bring them onto SimCorp's official PPT template (34 master layouts available via the skill).
2. **Data lineage appendix slide** for each deck — one slide at the back of each deck listing every chart → source dataset → SAQL step alias → query/dashboard ID. Self-contained, doesn't change main slide design.

## How to start

Your first message in the new session should look something like:

```
I'm picking up the KPI reports refresh thread from 2026-04-06. Reading the
required artifacts first (memory + ADR + builder assessment), then I'll
invoke superpowers:brainstorming for the builder modernization spec per
the handoff doc at crm-analytics/docs/2026-04-06-next-session-handoff.md.
```

Then:

1. Read all 8 required-reading items
2. Run `cd ~/crm-analytics && git log --oneline -16` to confirm branch state matches the handoff
3. Run `git status` and confirm 13+ unstaged files + many untracked — DO NOT touch them
4. Confirm current date (`date`) and ask Andre if anything changed since 2026-04-06
5. Invoke `superpowers:brainstorming` and start the modernization scope conversation

## One sentence

Brainstorm builder modernization based on `docs/2026-04-06-builder-assessment.md`, prioritizing items 1-4 (field constants, run summaries, logging, fiscal-quarter bugs), without touching any `build_*.py` file until a spec + plan are approved.
