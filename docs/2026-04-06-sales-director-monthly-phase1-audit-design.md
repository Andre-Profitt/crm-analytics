# Sales Director Monthly Dashboard - Phase 1 Audit Design

> Brainstormed design for Phase 1 of the Deck and Dashboard Verification work described in `docs/2026-04-06-deck-and-dashboard-verification-handoff.md`. This file is the contract for the next session's audit work. Phase 1 covers the standard Salesforce dashboard `01ZTb00000FSP7hMAH` only.

## One sentence

Distill the KPI brief into a permanent expected-widgets spec, then run a notebook-style audit script that grades all 16 widgets on `01ZTb00000FSP7hMAH` against that spec via the Analytics REST API and writes a two-table delta report (executive summary plus full appendix) to `docs/audits/`.

## Context and motivation

The handoff doc records that the standard Salesforce dashboard `01ZTb00000FSP7hMAH` (Sales Directors Monthly - Pipeline and Insights, 16 widgets) was built mid-session by syntactic clone from the BOB and RTB dashboards. The clone was structural; no widget was verified against the KPI brief. Several BOB-inherited reports use stale picklist values on `APTS_Primary_Quote_Type__c` (the org migrated to `SBL`, `MBL`, `PPL`; the correct field for renewal/land/expand identification is `Type`). 10 reports were already replaced mid-session with corrected summary clones using the `Type` field, but those have not been audited line-by-line either.

The deck rebuild (Phase 4) cannot start until we know which widgets are correct, which are wrong, and how. Phase 1 is the foundation: produce that knowledge.

## Goal and scope

**In scope:**

1. Produce `docs/specs/sales-director-monthly-dashboard-spec.md` containing the expected widget list distilled from the KPI brief. This becomes the permanent contract for the dashboard.
2. Produce `docs/audits/<rundate>-sales-director-monthly-audit.md` containing a two-table delta report grading every widget on `01ZTb00000FSP7hMAH` against the spec.
3. Produce `scripts/audit_sales_director_monthly_dashboard.py`, a notebook-style audit tool. Tool, not committed unless Andre asks.

**Out of scope:**

- Phase 2 (audit of `01ZTb00000FSP9JMAX`, the Sales Ops Quarterly KPI Dashboard).
- Phase 3 (cross-check standard SF widget values against CRMA dashboard step values and `report1_snapshot.json`).
- Phase 4 (rebuild the Sales Director Monthly deck using Option D).
- Fixing any of the issues the audit identifies. Phase 1 names problems; fixes are a separate brainstorm and plan after Andre reviews the audit.

## Decisions captured during brainstorm

| #   | Question             | Decision                                                                                                           |
| --- | -------------------- | ------------------------------------------------------------------------------------------------------------------ |
| 1   | Audit depth          | Metadata plus current values. No CRMA cross-check (deferred to Phase 3).                                           |
| 2   | Comparison reference | Build an expected-widgets spec first; the audit grades against the spec, not the prose brief.                      |
| 3   | Delta table shape    | Two tables: short executive summary (sortable by severity) plus full appendix with all per-widget metadata.        |
| 4   | Tooling              | Notebook-style Python file with `# %%` cell markers. Re-runnable by section.                                       |
| 5   | Spec location        | Standalone permanent spec at `docs/specs/sales-director-monthly-dashboard-spec.md`. Not embedded in the audit doc. |

## Deliverables

1. `~/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md` - the expected-widgets spec, committed by exact path.
2. `~/crm-analytics/docs/audits/<rundate>-sales-director-monthly-audit.md` - the delta report, committed by exact path. `<rundate>` is the date the audit script is run, in `YYYY-MM-DD` form.
3. `~/crm-analytics/scripts/audit_sales_director_monthly_dashboard.py` - the audit tool. Same naming and rules as the existing `audit_*.py` family in `scripts/`. Not committed to git unless Andre says so.

## The expected-widgets spec format

`docs/specs/sales-director-monthly-dashboard-spec.md` is structured as follows:

### 1. Header

- Purpose, dashboard ID, Lightning URL, owner, audit cadence.
- The KPI brief verbatim.
- Hard rules from the handoff doc, applied to this dashboard: calendar year only (no FY labels), ACV for renewals (ARR for everything else), no em-dashes in any widget label, `Type` field is canonical for renewal/land/expand.

### 2. Expected widgets table

One row per widget the dashboard should contain:

| Column           | Meaning                                                                 |
| ---------------- | ----------------------------------------------------------------------- |
| Widget ID        | Stable internal name (snake_case), e.g. `pipeline_overview_emea`        |
| KPI bullet       | The exact bullet from the brief this widget implements                  |
| Type             | `metric` / `chart` / `table`                                            |
| Grain            | `global` / `per_region` / `per_stage` / `per_quarter`                   |
| Required filters | Field plus operator plus value(s), in canonical form                    |
| Aggregation      | Field plus aggregation function (`sum(ARR)`, `sum(ACV)`, `count`, etc.) |
| Grouping         | Group-by fields if any                                                  |
| Drilldown report | Standard SF report ID once we have it; can start blank                  |
| Notes            | Edge cases, calendar-vs-fiscal caveats, why this exists                 |

### 3. Implied widget count (first pass)

Based on the brief:

| KPI bullet                             | Expected widgets                                                                             | Count  |
| -------------------------------------- | -------------------------------------------------------------------------------------------- | ------ |
| Pipeline overview with quarterly focus | 1 global metric plus 3 per-region charts                                                     | 4      |
| Commercial Approval overview           | 1 global summary plus 3 per-region Land Stage 3 missing-approval lists                       | 4      |
| Renewals tracking                      | 1 ACV-this-quarter metric plus 1 likelihood/probability chart plus 1 upcoming-renewals table | 3      |
| Churn risk and trends                  | 1 placeholder marked `pending Finance feed`                                                  | 1      |
| Slipped deals analysis                 | 1 root cause table plus 1 trend chart                                                        | 2      |
| **Total expected**                     |                                                                                              | **14** |

The live dashboard has 16 widgets. The 2 extras may be legitimate header context widgets (title metric, navigation tile) or cruft from the BOB/RTB clone. The audit answers which.

### 4. Open questions distilled from the brief

These are gaps the spec records and the audit notes. Andre resolves them before the audit runs if they are blockers; otherwise the spec records the assumption.

- **Renewals "likelihood"** - is that opportunity probability, a stage-based proxy, or a custom field?
- **Churn placeholder** - what does the widget say while the Finance feed is pending? "CRM signal only" or "Awaiting Finance feed (Alex P)"?
- **Slipped deals root cause** - opportunity-owner commentary is pending. Until then, what does the widget grade against? Stage-change history? Close-date slip count?
- **Quarterly focus** - current calendar quarter only, rolling 4 quarters, or current plus next?

## The audit methodology

`scripts/audit_sales_director_monthly_dashboard.py` is a single notebook-style Python file. Cells separated by `# %%` markers so each section can be re-run independently in VSCode interactive or via `python3 -i`.

### Cell sequence

1. **Auth** - shell out to `sf org display --target-org apro@simcorp.com --json`, extract `instanceUrl` and `accessToken`. Same pattern as the working Option D POC at `scripts/simcorp_crma_chart_sample.py`. No `.env` files.
2. **Load expected spec** - read `docs/specs/sales-director-monthly-dashboard-spec.md`, parse the expected widgets table into a Python dict keyed by KPI bullet (and by widget ID for direct lookup).
3. **Pull dashboard** - `GET /services/data/v66.0/analytics/dashboards/{dashboardId}/describe`. Extract the 16 components, each with its `reportId`, title, type (chart/table/metric), and any grouping or filter overrides.
4. **Pull each report's metadata** - for each unique report ID, `GET /services/data/v66.0/analytics/reports/{reportId}/describe`. Capture: filters (field plus operator plus value), groupings, summarized fields, displayed columns, report format (`TABULAR` / `SUMMARY` / `MATRIX`).
5. **Run each report** - `POST /services/data/v66.0/analytics/reports/{reportId}/instances?includeDetails=true` (synchronous). Capture the top-line `factMap` value or row count. 30 second timeout per report; on timeout the entry is recorded as BLOCKING and the audit continues.
6. **Static rule scan** - flag widgets where the underlying report:
   - filters on `APTS_Primary_Quote_Type__c` with stale picklist values (`Quote`, `Renewal`, etc.)
   - has `TABULAR` format on a "Top N" widget (the broken-Top-N pattern)
   - has a "Missing X" name but does not show field X in detail columns
   - has a date filter that does not align with calendar quarters (uses fiscal Q logic)
   - aggregates renewal pipeline on `ARR` instead of `ACV`
7. **Compare against spec (bidirectional)** - run two passes:
   - **Spec to dashboard:** for each expected widget, find the matching dashboard widget by KPI bullet mapping. Diff: expected filter vs actual, expected field vs actual, expected grouping vs actual, expected aggregation vs actual.
   - **Dashboard to spec:** for each dashboard widget, check whether it maps to any expected widget. Unmatched widgets are "orphans" (dashboard has it, the spec does not).
     Tag every entry (expected, present, orphan) with severity:
   - `BLOCKING` - wrong field, stale picklist, no data returned, or a required-by-spec widget is missing entirely
   - `WRONG-DATA` - filters partially right, value plausible but suspect
   - `ORPHAN` - widget exists on the dashboard but maps to no spec entry. May be legitimate (header context, navigation tile) or BOB/RTB-clone cruft. Recorded with a recommendation: keep, drop, or fold into spec
   - `COSMETIC` - label or column-order issue, em-dash in title, etc.
   - `OK` - matches spec
8. **Render markdown** - write the two-table delta report to `docs/audits/<rundate>-sales-director-monthly-audit.md`. Print a one-line tally to stdout (`16 widgets - N BLOCKING - M WRONG-DATA - O ORPHAN - K COSMETIC - J OK`).

### Configuration

Two constants at the top of the script, no CLI args (matches the existing `audit_*.py` script convention):

```python
DASHBOARD_ID = "01ZTb00000FSP7hMAH"
SPEC_PATH = "/Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md"
```

The same script with a different `DASHBOARD_ID` and `SPEC_PATH` runs the Phase 2 audit on `01ZTb00000FSP9JMAX` later.

### Failure modes handled inline

- **Auth failure:** script exits with the `sf` CLI error verbatim.
- **Dashboard 404:** exits with a clear message naming the dashboard ID. Cannot continue.
- **Picklist freshness assertion failure:** exits. If the org's `APTS_Primary_Quote_Type__c` values no longer match the audit's expectations (`SBL`, `MBL`, `PPL` present; `Quote`, `Renewal` absent), the stale-picklist rule cannot be trusted, and a continued run would produce wrong severities. The audit must be reviewed and updated before re-running.
- **Report 404 (broken reference) on describe (cell 4):** recorded as a BLOCKING delta entry, audit continues.
- **Report describe failure (any reason):** recorded as a BLOCKING delta entry, audit continues. Cell 5 is skipped for that report.
- **Report run timeout (cell 5):** recorded as a BLOCKING delta entry, audit continues.
- **Report run failure (any reason other than timeout):** recorded as a BLOCKING delta entry, audit continues.
- **Picklist value lookup at startup:** `APTS_Primary_Quote_Type__c` values fetched once at the top via `sf sobject describe`, cached for the run.

### Logging

Stdout only. Andre can pipe to a file if needed. No log files written from inside the script.

## The delta report format

`docs/audits/<rundate>-sales-director-monthly-audit.md` structure:

### Header block

- Dashboard ID, Lightning URL, dashboard `LastModifiedDate` from the describe call (so future sessions can detect drift), audit run date, spec doc reference, audit script reference, exact reproduction command, and a one-line tally:

  > `16 widgets - 4 BLOCKING - 6 WRONG-DATA - 2 ORPHAN - 1 COSMETIC - 3 OK`

### Table 1: Executive summary

Sorted by severity, then by KPI bullet. Fits on one screen. This is the table Andre reads first.

| Severity   | Widget                               | KPI bullet                   | Issue                                                                 | Recommended fix                                                     |
| ---------- | ------------------------------------ | ---------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------- |
| BLOCKING   | Renewal ACV by Quarter               | Renewals tracking            | Filter on stale `APTS_Primary_Quote_Type__c='Renewal'` returns 0 rows | Switch report filter to `Type='Renewal'`, aggregate `ACV` not `ARR` |
| WRONG-DATA | Top 20 Accounts by ARR               | (context)                    | Report is `TABULAR` not `SUMMARY`; widget shows 1 row                 | Convert to `SUMMARY` grouped by Account, sum `ARR` desc             |
| COSMETIC   | Slipped Deals by Stage               | Slipped deals                | Em-dash in widget title                                               | Replace with hyphen                                                 |
| OK         | Land Stage 3 Missing Approval (EMEA) | Commercial approval regional | Filter and field both correct                                         | n/a                                                                 |

Severity meaning:

- **BLOCKING** must be fixed before deck rebuild.
- **WRONG-DATA** must be triaged before any of this is shown to Sales Directors.
- **ORPHAN** widget exists on the dashboard but maps to no spec entry. Decision needed: keep, drop, or fold into spec.
- **COSMETIC** can ship as a follow-up.
- **OK** means the widget matches the spec.

### Table 2: Full appendix

Every one of the 16 widgets, expanded:

| #   | Widget name | Type | Component | Report ID | Report format | Filters (raw) | Group/agg | Cols shown | Current value | Expected per spec | KPI bullet | Severity | Notes |
| --- | ----------- | ---- | --------- | --------- | ------------- | ------------- | --------- | ---------- | ------------- | ----------------- | ---------- | -------- | ----- |

This is the full evidence trail. Greppable for any specific widget. Hand it to a future session as the complete state-of-play before the rebuild.

### Bottom of doc

- **Spec gaps surfaced during audit** - anything the audit had to assume because the brief was ambiguous.
- **Phase 2 / 3 / 4 implications** - cross-references to the deferred phases. Example: "the renewal ACV widget value should match step `s_renewal_acv` on `0FKTb0000000J97OAE` in the Phase 3 cross-check."
- **Reproducibility** - exact command to re-run the audit, plus the spec commit hash the audit was graded against.

## Validation strategy

The audit script is judging dashboards, so it must be trustworthy itself. Three checks:

1. **Spot-check 2 widgets manually before running the full pass** - run cells 1-5 against one widget known to be correct (one of the 10 corrected reports Andre swapped in last session, e.g. `00OTb000008eksLMAQ` Renewals by Fiscal Quarter) and one known to be broken (a stale-picklist BOB clone). If the script flags the corrected one as `OK` and flags the broken one as `BLOCKING` with the right reason, the rules are wired correctly. If not, fix the rules before running the full pass.
2. **Picklist freshness assertion** - at the top of the run, fetch the actual current `APTS_Primary_Quote_Type__c` values via `sf sobject describe` and assert that `SBL`, `MBL`, `PPL` are present and `Quote`, `Renewal` are not. If the picklist has shifted again since the handoff doc was written, the audit's stale-picklist rule needs to know.
3. **Sanity totals** - after the report-run cell, print the sum of all aggregated values across the 16 widgets. If that total is wildly different from expectations (e.g., total open ARR shown across regions is not equal to total open ARR from the global metric), the audit flags it for review.

## Risks

- **Report run timeouts** - `/analytics/reports/{id}/instances` can be slow on big reports. 30 second per-report timeout. Worst case 16 \* 30s = 8 minutes, realistically much less.
- **API rate limits** - 16 dashboard widgets \* 2 calls each (describe plus run) plus the dashboard describe is roughly 33 calls. Well under any rate limit on the apro@simcorp.com org.
- **The expected-widgets spec embeds an interpretation of the brief** - if that interpretation is wrong, the audit grades against a wrong contract and produces false positives. Mitigation: the spec lists open questions explicitly (see "Open questions distilled from the brief" above), and Andre signs off on the spec doc before the audit script is invoked. Same gate as a code review.
- **Live org changes** - the audit captures a point-in-time state. If the dashboard is edited between the audit and the deck rebuild, the audit goes stale. Mitigation: the audit doc records the dashboard's `LastModifiedDate` from the describe call so we can detect drift before the rebuild.
- **Spec drift between sessions** - if Phase 1 lands and Phase 2 modifies the spec format, the two audits become inconsistent. Mitigation: the spec format is locked here in this design doc and `sales-ops-quarterly-dashboard-spec.md` reuses the same template.

## Exit criteria

Phase 1 is done when all four of the following are true:

1. `docs/specs/sales-director-monthly-dashboard-spec.md` exists, is committed by exact path, and Andre has signed off on it.
2. `scripts/audit_sales_director_monthly_dashboard.py` exists. Uncommitted by default, same convention as the rest of the `audit_*.py` family.
3. `docs/audits/<rundate>-sales-director-monthly-audit.md` exists, is committed by exact path, contains the two-table delta report with all 16 widgets graded.
4. Andre has read the audit doc and confirmed the next move (which fixes to take in Phase 1.5, what to defer, what to escalate).

## Commit discipline

- Each file staged by exact path. No `git add .`, no `-A`, no `-u`.
- No push to origin unless Andre explicitly says so.
- Commit messages reference the dashboard ID and the spec date.
- The audit script (`scripts/audit_sales_director_monthly_dashboard.py`) stays uncommitted unless Andre asks otherwise.

## Reference - file paths

**Inputs**

- KPI brief: in `docs/2026-04-06-deck-and-dashboard-verification-handoff.md` under "The KPI brief (from Andre, verbatim)"
- Live dashboard: `01ZTb00000FSP7hMAH` at `https://simcorp.my.salesforce.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view`
- 10 corrected reports already swapped in: listed in the handoff doc under "Standard Salesforce dashboards"
- Reference auth pattern: `scripts/simcorp_crma_chart_sample.py` (working Option D POC, just re-run and confirmed alive)
- Existing audit script convention: `scripts/audit_account_intelligence.py`, `scripts/audit_forecast_revenue_motions.py`, etc.

**Outputs**

- `docs/specs/sales-director-monthly-dashboard-spec.md` (committed)
- `docs/audits/<rundate>-sales-director-monthly-audit.md` (committed at run time)
- `scripts/audit_sales_director_monthly_dashboard.py` (uncommitted by default)

**Cross-references**

- Handoff doc that triggered this work: `docs/2026-04-06-deck-and-dashboard-verification-handoff.md`
- Project rules: `~/crm-analytics/CLAUDE.md`
- Andre's hard rules summary: handoff doc, "Andre's hard rules" section
