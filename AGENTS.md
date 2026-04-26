# CRM Analytics — Codex Review Instructions

## What This Repo Is

Salesforce CRM Analytics dashboard suite for SimCorp. All dashboard changes go through direct Wave API calls (curl/requests) and sf CLI — not Python builders. Builder files (`build_*.py`) are legacy and should not be modified or referenced unless explicitly asked.

## Review Focus

### SAQL Steps

- Verify field quoting: bare names in `sum()`, `avg()`, `max()` — single-quoted names inside aggregates are invalid
- Check `order by` uses single-column sort (multi-column fails at query endpoint)
- Verify `group by` precedes `foreach` for aggregations
- No `sum(case when...)` — must pre-compute with `foreach` first
- `matches` is invalid — use `like "%value%"`

### columnMap

- If provided: ALL 4 keys required (`dimensionAxis`, `plots`, `trellis`, `split`)
- Funnel, waterfall, treemap: must be `null`
- Missing any key = white-screen crash in production

### Dashboard JSON

- No `label` or `url` in step dataset entries (breaks PATCH)
- No `isFacet` on aggregateflex steps
- Number widgets: no `numberFormat`, `compact`, or `title` in parameters
- `gridLayouts[].pages[].name` must match nav link `destinationLink.name`

### Dataset Integrity (validate via SAQL, not builder files)

- Builder files (`build_*.py`) are legacy — do not review or modify them
- Verify stage cascade via SAQL: WonFromStage at stage N must be >= WonFromStage at stage N+1
- Verify cohort definitions are labeled (cohort-based vs period-based)
- Check population consistency across RowTypes (industry total should match cross-tab total)

## Org Details

- Target: `apro@simcorp.com` / `simcorp.my.salesforce.com`
- API: v66.0
- Fiscal year starts February 1
