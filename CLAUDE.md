# CRM Analytics — Claude Code Instructions

## Knowledge base

Project methodology, runbook, ADRs, data dictionary and monthly snapshots live in the Obsidian vault at `obsidian/`. Start there for context on _why_ things are built the way they are (the code shows _what_ and _how_). Key entry points:

- `obsidian/README.md` (vault map of content)
- `obsidian/runbook.md` (how to run the monthly pipeline)
- `obsidian/architecture.md` (ETL layout)
- `obsidian/methodology.md` (ARR definitions, Alex P scope filters)
- `obsidian/Decisions/` (ADRs for non-obvious choices)

## CLI-First Workflow

All Salesforce and CRM Analytics work goes through direct CLI and API calls. **No MCP tools. No Python builders.**

### How to Work

1. **Auth**: `sf org display --target-org apro@simcorp.com --json` → extract `accessToken` and `instanceUrl`
2. **SOQL**: `sf data query --query "SELECT ..." --target-org apro@simcorp.com --json` or `curl` with REST API
3. **SAQL**: `curl -X POST .../wave/query` with `{"query": "..."}`
4. **Read dashboards**: `curl .../wave/dashboards/{id}`
5. **Patch dashboards**: `curl -X PATCH .../wave/dashboards/{id}` with `{"state": {...}}`
6. **Read datasets**: `curl .../wave/datasets` and `.../wave/query`
7. **Upload datasets**: External Data API via `curl`

### Wave API PATCH Gotchas (hard-won)

- **PATCH not PUT** — PUT returns 405
- **HTML encoding round-trip** — GET returns HTML entities (`&quot;`, `&#39;`). Always `html.unescape()` ALL step queries before PATCH
- **Strip read-only fields** — remove `label` and `url` from `steps.*.datasets[]` entries, remove `label` from `gridLayouts[].pages[]`
- **Filter step queries** — compact JSON filters (e.g. `{"measures":...}`) are nested: `step.query.query` is the inner string that also needs unescaping
- **columnMap** — if you provide one, ALL 4 keys required (`dimensionAxis`, `plots`, `trellis`, `split`). Missing any = white-screen crash
- **Funnel/waterfall/treemap** — always `columnMap: null`
- **SAQL field quoting** — bare names inside `sum()`, `avg()`, `max()`. Single-quoted names inside aggregates = error
- **SAQL `order by`** — multi-column sort (`col1 asc, col2 asc`) may fail at query endpoint; use single-column sort
- **Window functions** — double-nested aggregates, `[..0]`, `partition by all`
- **`isFacet`** — rejected on aggregateflex step PATCH. Valid fields: `type`, `query`, `datasets`, `broadcastFacet`, `isGlobal`, `selectMode`, `receiveFacetSource`
- **Number widgets** — `numberFormat`, `compact`, `title` all rejected on PATCH. Format via dataset XMD or `number_to_string()` in SAQL
- **Compare table `formatRules`** — WORKS on PATCH. Structure: `[{"type":"threshold","field":"name","rules":[{"value":70,"color":"#D4504C","operator":"gte"}]}]`
- **`sum(case when...)` is INVALID** — pre-compute with `foreach` before `group by`, then `sum()` the result
- **`count()` takes ZERO args** — `count('field')` is invalid

### Rules

1. **Never use MCP tools** — use `curl` / `sf` CLI / `python3 requests` directly
2. **Never edit Python builder files** (`build_*.py`) — patch live dashboards via Wave API
3. **Test SAQL via `/wave/query` endpoint** before embedding in dashboard steps
4. **Always unescape HTML** before PATCH — or the round-trip will corrupt filter steps
5. **Use subagents** for parallel SOQL/SAQL validation work

## Subagents & Skills

- **Use `crm-analytics-specialist` subagent** for SAQL validation, dashboard audits, and parallel SOQL work
- **Use `salesforce-expert` subagent** for field name lookups and SOQL design
- **Invoke `crm-analytics-dashboard-building` skill** when building new widgets or pages — it has columnMap rules, binding patterns, and SimCorp field names

## Dashboard Design (see skill for full reference)

- Classify metrics: global benchmark vs scoped KPI vs exploratory detail
- Page structure: KPI strip → primary viz → drill → action table
- Anti-patterns: no gauges (use bullet), no donuts (use ranked bar), no global filters on everything

## Target Org

- Org: `apro@simcorp.com`
- Instance: `simcorp.my.salesforce.com`
- App: `B2B_MA`
- API: v66.0
- Auth: `sf org display` (no .env files)

## Verification

```bash
make verify          # lint + compile + contracts
make readiness       # org readiness scan
make api-smoke       # API smoke matrix
make security-audit  # security coverage
```
