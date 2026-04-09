# Dashboard Pristine Pass — Session Handoff (2026-04-08, second pass)

> Second-pass improvement + audit session. Picks up where `2026-04-08-sales-director-monthly-handoff.md` left off (commit `2550fa7`). This session verified all prior fixes, applied 8 new improvements, proved 4 more items require schema change (not API), ported the audit logic into a reusable `scripts/dashboard_state_dump.py`, and wrote the pristine-state design + manual-UI runbook docs.

## TL;DR

**Both dashboards now audit as `✓ Pristine state: 0 active flags, 4 deferred flags.`**

The 4 deferred flags are all `fiscal-grouping:FISCAL_QUARTER` and all require a schema change (bucket field or `Calendar_Quarter__c` formula field on Opportunity). Empirically verified that the Reports API rejects `CALENDAR_QUARTER` and `CLOSE_DATE_CALENDAR_QUARTER` as grouping column names, confirming the deep audit's deferral rationale.

## Delta since the 2550fa7 handoff

### Verified intact (drift check)

All 9 inline fixes from the deep audit still hold in live state:

| Report                                                   | Expected fix                                    | Status |
| -------------------------------------------------------- | ----------------------------------------------- | ------ |
| `00OTb000008ekltMAA` (D1 Land Stage 3 Missing Approval)  | `APTS_Opportunity_ARR__c.CONVERT` in aggregates | ✓      |
| `00OTb000008ekp7MAA` (D1 Commercial Approval Candidates) | `APTS_Opportunity_ARR__c.CONVERT`               | ✓      |
| `00OTb000008eknVMAQ` (D1 Close Date Slipped CFQ)         | `APTS_Opportunity_ARR__c.CONVERT`               | ✓      |
| `00OTb000008eksLMAQ` (D1 Renewals by Quarter)            | name does not contain "Fiscal"                  | ✓      |
| `00OTb000008Ta9xMAC` (D1 Business At Risk)               | CUSTOM date filter                              | ✓      |
| `00OTb000008TZc5MAG` (D1 Pipeline Coverage)              | non-fiscal date filter                          | ✓      |
| `00OTb000008SqblMAC` (D2 Won/Loss Missing CFQ)           | filter excludes "0 - No Opportunity"            | ✓      |
| `00OTb000008TaBZMA0` (D2 Overdue Close Date)             | SUMMARY grouped by FULL_NAME                    | ✓      |
| `00OTb000008TaEnMAK` (D2 No Activity 30+)                | CUSTOM date filter                              | ✓      |

### 8 new improvements applied

All via Analytics REST API PATCH, all verified via inline GET after PATCH:

**Dashboard 1 — Sales Directors Monthly**

1. **Forecast Accuracy** (`00OTb000008TZsDMAW`) — widget binding `APTS_Forecast_ARR__c` → `APTS_Forecast_ARR__c.CONVERT`. Source report updated (added `.CONVERT` to aggregates + detail columns, then retired the non-`.CONVERT` form).
2. **Pipeline Coverage by Stage** (`00OTb000008TZc5MAG`) — same pattern as #1.
3. **Renewal ACV by Quarter** (`00OTb000008ekxBMAQ`) — widget binding + source report ACV field → `.CONVERT` form.
4. **Renewal Pipeline This Quarter** (`00OTb000008ektxMAA`) — same pattern.
5. **Renewals by Quarter** (`00OTb000008eksLMAQ`) — same pattern.

**Dashboard 2 — Sales Ops Quarterly KPI**

6. **No Activity 30 Plus Days** (`00OTb000008TaEnMAK`) — widget binding `s!AMOUNT` → `APTS_Opportunity_ARR__c.CONVERT`. **Load-bearing finding: FlexTable widgets store the aggregate in TWO places** — `properties.aggregates[]` AND `properties.visualizationProperties.tableColumns[]` (rows with `type: "aggregate"`). Both must be updated in the same PATCH or Salesforce silently reverts the change (200 OK but no state change). Earlier PATCHes were silently no-op'd because only `properties.aggregates` was updated.
7. **High Value Stale Deals** (`00OTb000008Ti97MAC`) — source report `Forecast_ARR` → `.CONVERT` form; non-`.CONVERT` retired.

**Cleanup**

8. **Retired 7 stale non-`.CONVERT` aggregates** across source reports that had both forms after the earlier PATCHes.

### Locked-in finding: calendar-quarter grouping requires schema change

Tested both `CALENDAR_QUARTER` and `CLOSE_DATE_CALENDAR_QUARTER` as grouping column name replacements for `FISCAL_QUARTER` on all 4 deferred reports (`00OTb000008ekxBMAQ`, `00OTb000008TZsDMAW`, `00OTb000008eksLMAQ`, `00OTb000008SrmLMAS`). All 8 attempts rejected by Salesforce:

```
"The column CALENDAR_QUARTER is not a valid column for groupings."
"The column CLOSE_DATE_CALENDAR_QUARTER is not a valid column for groupings."
```

Confirms the deep audit's schema-change deferral. The fix requires one of:

- **Bucket field** per report: map `CloseDate` to calendar quarters in the report definition. Cheapest. Not reusable across reports.
- **Custom formula field on Opportunity** (`Calendar_Quarter__c`): `TEXT(YEAR(CloseDate)) & "-Q" & TEXT(CEILING(MONTH(CloseDate) / 3))`. Reusable everywhere. One-time metadata deploy. Recommended.

Once either lands, the 4 widgets can switch groupings in a single PATCH.

## New documentation produced

1. **`docs/2026-04-08-pristine-dashboard-design.md`** — the target-state design covering revenue aggregate hierarchy (ARR/ACV/Forecast_ARR with `.CONVERT`), calendar framing rules, widget naming, source-contract pinning, one-filterable-dashboard principle, running-user semantics, and a widget-by-widget goal coverage map for Dashboard 1. Lockable as the ongoing pristine bar.
2. **`docs/2026-04-08-manual-ui-runbook.md`** — step-by-step UI runbook for the 5-minute handoff (add 2-3 dashboard filters, flip `canChangeRunningUser`, save, verify), the PI list view creation (Slipped Deals + 26-week forecast change), Industry picklist investigation, calendar-quarter schema change, and stakeholder decision items. Every step that the REST API cannot do or silently refuses is documented here.
3. **`docs/2026-04-08-dashboard-pristine-pass-handoff.md`** (this file).

## New reusable tool

**`scripts/dashboard_state_dump.py`** — a read-only Python audit script that walks both dashboards and every source report, cross-references widget bindings with report aggregates, and emits structured drift flags. Uses sf CLI for auth + curl for HTTP (matches the CLAUDE.md "CLI-first, no MCP, no builders" convention). Key flags:

- `no-convert:<field>` — revenue aggregate missing `.CONVERT`
- `amount-not-arr-on-widget` — widget binding uses `s!AMOUNT`
- `fiscal-date-filter:<form>` — fiscal framing on report standardDateFilter
- `fiscal-grouping:<col>` — fiscal framing on groupingsDown

Deferred flags (allow-listed schema changes) are shown separately (`🔶`) from active flags (`⚠️`). A set of deferred report IDs is hard-coded at the top of the script and can be audited against in the future.

```bash
# Markdown + JSON
python3 scripts/dashboard_state_dump.py

# Markdown only to file
python3 scripts/dashboard_state_dump.py --format markdown --out-md /tmp/state.md

# CI gate — fail if any active flag
python3 scripts/dashboard_state_dump.py --fail-if-drifted
```

**Recommendation:** wire `--fail-if-drifted` into a nightly job or CI pre-commit. The Frontier OS `overnight-review` watcher pattern is a natural host for this — it can run the script, parse the JSON output, and emit an alert when drift appears.

## Final audit output

```
## Sales Directors Monthly (01ZTb00000FSP7hMAH)
- dashboardType: SpecifiedUser
- canChangeRunningUser: false
- runningUser: Andre Profitt
- filters: 0
- components: 15 / 20

## Sales Ops Quarterly KPI (01ZTb00000FSP9JMAX)
- dashboardType: SpecifiedUser
- canChangeRunningUser: false
- runningUser: Andre Profitt
- filters: 0
- components: 18 / 20

## Summary
- Sales Directors Monthly: 15 widgets, 0 flagged, 0 active flag(s), 3 deferred flag(s)
- Sales Ops Quarterly KPI: 18 widgets, 0 flagged, 0 active flag(s), 1 deferred flag(s)

✓ Pristine state: 0 active flags, 4 deferred flags.
```

## Remaining work (priority order, carried forward from 2550fa7 + updates)

1. **5-minute UI handoff** — add the 2-3 Dashboard 1 filters, flip `canChangeRunningUser` to `true`, save. Runbook in `docs/2026-04-08-manual-ui-runbook.md` §1. Unblocks the 9 Director preset filter combos.
2. **Calendar-quarter schema change** — add `Calendar_Quarter__c` formula field on Opportunity (recommended) or per-report bucket fields. Runbook §4. Unblocks the 4 deferred fiscal-grouping widgets.
3. **Industry picklist investigation** — SOQL probe to identify the SimCorp-values industry field. Runbook §3. Unblocks Patrick/Adam NAM sub-cuts.
4. **Pipeline Inspection list view creation** — 2 PI list views for the 4 forecast accuracy widgets on Dashboard 2 and the slipped-deals widgets on Dashboard 1. Runbook §2.
5. **Stakeholder decisions (3 items)** — `dq_missing_quote_type` retire/repurpose, `ph_probability_mismatch_by_stage` threshold, Finance churn feed handshake (Alex P). Runbook §5.
6. **Phase 3 or Phase 4 deck rebuild** — source contracts are now clean. Phase 3 is a safer validation pass; Phase 4 is the actual deck builder against `output/sales_director_monthly_deck_2026-03-31/` infrastructure.
7. **Wire `scripts/dashboard_state_dump.py --fail-if-drifted` into CI** — so future drift is caught automatically.

## Files changed in this session (to be committed)

```
M  docs/2026-04-08-pristine-dashboard-design.md        (new)
M  docs/2026-04-08-manual-ui-runbook.md                (new)
M  docs/2026-04-08-dashboard-pristine-pass-handoff.md  (new, this file)
M  scripts/dashboard_state_dump.py                     (new)
```

Plus live Salesforce state changes (no git track, but recorded in this doc):

- 2 dashboards PATCHed (D1, D2)
- 7 reports aggregate/detailColumns upgraded to `.CONVERT`
- 7 reports had stale aggregates retired after upgrade

## Files to read first (next session)

- This doc: `docs/2026-04-08-dashboard-pristine-pass-handoff.md`
- Previous handoff: `docs/2026-04-08-sales-director-monthly-handoff.md`
- Pristine design: `docs/2026-04-08-pristine-dashboard-design.md`
- UI runbook: `docs/2026-04-08-manual-ui-runbook.md`
- Source contracts: `docs/specs/report-1-source-contract.md` + `docs/specs/report-2-source-contract.md`
- Audit tool: `scripts/dashboard_state_dump.py`

## Contact

Session author: Andre (apro@simcorp.com). Questions: escalate via project memory at `~/.claude/projects/-Users-test-crm-analytics/memory/`.
