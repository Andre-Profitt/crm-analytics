# Sales Director Monthly - Phase 2 CRMA Audit Design

> Design doc for Phase 2 of the Sales Director Monthly deck workstream. Phase 1 audited the standard SF dashboard `01ZTb00000FSP7hMAH` and decided to treat it as legacy. Phase 2 audits the 9 B2B_MA CRMA dashboards collectively against the same 14-widget spec, producing the input contract for the Phase 4 Option D deck rebuild. This doc is the output of a brainstorm session on 2026-04-07 and the input for `superpowers:writing-plans`.

## One sentence

Build `scripts/audit_crma_sales_director_monthly.py` as a notebook-style audit that fetches all 9 B2B_MA CRMA dashboards via the Wave API, runs every CRMA widget's SAQL via `/wave/query`, grades each widget against `docs/specs/sales-director-monthly-dashboard-spec.md`, and produces a single combined coverage matrix at `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md` listing the top-3 candidate steps for each of the 14 spec widgets, plus orphan and unused-step appendices.

## Context

Phase 1 (2026-04-06) committed:

- `docs/specs/sales-director-monthly-dashboard-spec.md` - the 14-widget spec, KPI-bullet keyed, field-agnostic enough to grade any dashboard surface.
- `docs/audits/2026-04-06-sales-director-monthly-audit.md` - the Phase 1 delta report. 25 entries: 12 BLOCKING, 10 WRONG-DATA, 1 ORPHAN, 2 OK on `01ZTb00000FSP7hMAH`.
- `docs/2026-04-06-sales-director-monthly-phase1-done-handoff.md` - handoff to this session.

Phase 1 also produced `scripts/audit_sales_director_monthly_dashboard.py` (uncommitted, by convention - tools live local). That script is the seed for the Phase 2 adapter. It is structured as 10 notebook cells, each independently callable, each with inline `assert`-based tests.

The decision out of Phase 1 was to treat the standard SF dashboard as legacy and rebuild the deck CRMA-first via Option D. The 9 B2B_MA CRMA dashboards in scope are:

| Dashboard                                  | ID                   |
| ------------------------------------------ | -------------------- |
| Sales Ops Data Quality & Forecast Accuracy | `0FKTb0000000K5BOAU` |
| Pipeline & Opportunity Operations          | `0FKTb0000000KwPOAU` |
| Forecast & Revenue Motions                 | `0FKTb0000000JCLOA2` |
| Executive Revenue Source Truth             | `0FKTb0000000IxpOAE` |
| Revenue Retention & Health                 | `0FKTb0000000J97OAE` |
| Forecast Intelligence                      | `0FKTb0000000Jc9OAE` |
| Account Intelligence KPIs                  | `0FKTb0000000J7VOAU` |
| Customer & Account Health                  | `0FKTb0000000KunOAE` |
| Commercial Rhythm Control Tower            | `0FKTb0000000JPFOA2` |

The Option D POC at `scripts/simcorp_crma_chart_sample.py` was re-run on 2026-04-07 against `0FKTb0000000KwPOAU` step `s_region_hygiene` and is confirmed alive (7 regions, fresh dataset version `0FcTb0000096QHBKA2`). It is the reference recipe for cell 6 of the Phase 2 script.

## Goal

Produce a single markdown audit at `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md` that the deck-builder can read in spec order to decide, for each of the 14 spec widgets, which CRMA step (across the 9 dashboards) feeds the corresponding deck slide. Surface graded candidates, not pre-decided assignments.

## Non-goals

- Not a CRMA dashboard rebuild. The audit reads dashboards via GET; it never PATCHes anything.
- Not a deck rebuild. Phase 4 consumes this audit; it does not produce slides.
- Not a fix-it pass. Phase 2.5 (separate session, optional) handles fixes to bad CRMA steps surfaced here.
- Not a manual hint file (Approach 3 in the brainstorm). Pure auto-matching with top-N candidates is what we're building first. If a Phase 2 dry run reveals more than two false-`(NO MATCH)` rows that look like genuine matches under different vocabulary, we add an override YAML as a Phase 2.5 patch.
- Not run against any non-B2B_MA dashboard.

## Constraints (non-negotiable)

From `crm-analytics/CLAUDE.md` and the Phase 1 hard rules:

1. CLI-first. `sf` CLI for auth, `requests` for API calls. No MCP. No `build_*.py`. No `.env`.
2. Auth via `sf org display --target-org apro@simcorp.com --json`.
3. API version `v66.0`.
4. Calendar year only. Fiscal date filters get flagged.
5. Renewals use ACV (`APTS_Renewal_ACV__c`); land/expand uses ARR (`APTS_Opportunity_ARR__c`).
6. Type field is canonical for renewal/land/expand; `APTS_Primary_Quote_Type__c` is empty in the org.
7. No em-dashes in any output. ASCII hyphens, periods, or rephrase.
8. Stage exact paths only. Never `git add .` / `-A` / `-u`.
9. Audit script stays uncommitted (matches `scripts/audit_*.py` convention from Phase 1).
10. Audit output committed by exact path.

## Approach (locked)

**Approach 2** from the brainstorm: combined audit at the widget level, top-3 candidates per spec widget, no manual override file. Reused matcher from Phase 1 cell 8 with stem-based scoring against the KPI bullet text.

## Architecture: cell-by-cell

New script: `scripts/audit_crma_sales_director_monthly.py`. Notebook-style `# %%` cells. Constants block at top:

```python
DASHBOARD_IDS = {
    "0FKTb0000000K5BOAU": "Sales Ops Data Quality & Forecast Accuracy",
    "0FKTb0000000KwPOAU": "Pipeline & Opportunity Operations",
    "0FKTb0000000JCLOA2": "Forecast & Revenue Motions",
    "0FKTb0000000IxpOAE": "Executive Revenue Source Truth",
    "0FKTb0000000J97OAE": "Revenue Retention & Health",
    "0FKTb0000000Jc9OAE": "Forecast Intelligence",
    "0FKTb0000000J7VOAU": "Account Intelligence KPIs",
    "0FKTb0000000KunOAE": "Customer & Account Health",
    "0FKTb0000000JPFOA2": "Commercial Rhythm Control Tower",
}
SPEC_PATH = Path("/Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md")
AUDIT_OUTPUT_DIR = Path("/Users/test/crm-analytics/docs/audits")
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"
SAQL_QUERY_TIMEOUT_SECONDS = 60
TOP_N_CANDIDATES = 3
MATCHER_THRESHOLD = 1.0  # same as Phase 1
```

| Cell                                            | Status                      | Behavior                                                                                                                                                                                                                                                                                                                                                                                            |
| ----------------------------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Auth**                                     | Reuse verbatim from Phase 1 | Shell `sf org display`, return `(inst, tok)`.                                                                                                                                                                                                                                                                                                                                                       |
| **2. Picklist freshness**                       | Reuse verbatim              | Assert `APTS_Primary_Quote_Type__c` has zero active values on Opportunity. Same fail-closed pattern.                                                                                                                                                                                                                                                                                                |
| **3. Spec loader**                              | Reuse verbatim              | Parse the markdown table in the spec into `dict[widget_id -> row]`.                                                                                                                                                                                                                                                                                                                                 |
| **4. Dashboard fetch (REPLACE)**                | New                         | Loop the 9 dashboard IDs. For each: `GET /services/data/v66.0/wave/dashboards/{id}`. Walk `state.widgets` and follow each widget's `step` reference into `state.steps`. Emit a uniform widget record. Capture dashboard `name` and `lastModifiedDate`. Side-product: `unused_steps` list per dashboard.                                                                                             |
| **5. Report describes (DELETE)**                | Removed                     | All step config inline in `state.steps`. Nothing useful left.                                                                                                                                                                                                                                                                                                                                       |
| **6. Run SAQL (REPLACE)**                       | New                         | For each unique `(dashboard_id, step_name)` pair: take the SAQL, `html.unescape()`, swap `load "DatasetName"` for `load "datasetId/versionId"` (Option D recipe), strip Mustache `{{...}}` filter bindings, `POST /wave/query`. Cache dataset name -> `(id, version)` once per run via `GET /wave/datasets`. Return `{row_count, top_value, _failed, _timeout, error}`. 60-second per-step timeout. |
| **7. Static rules (EXTEND)**                    | Mostly reuse                | 8 rules from Phase 1 minus 2 SF-only rules (5, 6) plus 3 new SAQL-only rules (9, 10, 11). Full table below.                                                                                                                                                                                                                                                                                         |
| **8. Compare (REPLACE wrapper, reuse matcher)** | Inner matcher reused        | `match_widget_to_spec()` reused as-is - operates on widget title. Wrapper rewritten to: pick top-N candidates per spec widget across all 9 dashboards, emit one row per `(spec_widget, candidate_rank)`, emit `(NO MATCH) BLOCKING` for spec widgets with zero candidates above `MATCHER_THRESHOLD`, list orphan widgets separately.                                                                |
| **9. Markdown render (EXTEND)**                 | Mostly reuse                | Four sections instead of two. See output structure.                                                                                                                                                                                                                                                                                                                                                 |
| **10. Composition (ADAPT)**                     | Reuse helpers               | Wire cells, loop the 9 dashboards, write single combined file.                                                                                                                                                                                                                                                                                                                                      |

### Cell 4 widget-extraction shape

Each emitted widget record has:

```python
{
    "dashboard_id": str,
    "dashboard_name": str,
    "widget_id": str,            # key in state.widgets
    "widget_type": str,          # "chart", "number", "list", "pillbox", etc
    "title": str,                # state.widgets[*].parameters.title (matcher input)
    "step_name": str,            # state.widgets[*].parameters.step
    "step_query": str,           # html.unescape() of state.steps[step_name]["query"], unwrapping the dict shape if compact-JSON
    "step_query_is_compact_json": bool,  # true if the step.query was a dict, not a SAQL string
    "dataset_name": str | None,  # parsed from state.steps[step_name]["datasets"]
    "raw_widget": dict,          # full state.widgets[widget_id] for debugging
    "raw_step": dict,            # full state.steps[step_name] for debugging
}
```

Steps with no widget reference go in `unused_steps[dashboard_id]: list[step_name]`.

Widgets that reference a missing step get tagged with a special record carrying `step_query=None` and contribute a `BLOCKING broken_step_reference` static issue downstream.

### Cell 6 SAQL preparation

Cell 4 has already done the dict-unwrap and `html.unescape()` when it built the widget record's `step_query` field. Cell 6 starts from that already-unescaped string. This matches the Option D POC's split: extraction unescapes once, the load-rewrite helper does swap + strip only.

Helper `prepare_saql(unescaped_query, dataset_versions) -> str | None` does:

1. Find the dataset reference. Try regex `q\s*=\s*load\s*"([^"]+)"`. If the captured name has no `/` (i.e. it's a name, not an `id/version` pair), look up `dataset_versions[name]` and substitute the load string with `q = load "{id}/{version}"`. If the captured name already has a `/`, leave it.
2. Strip lines containing `{{` and `}}` (Mustache binding filters).
3. Return the rewritten SAQL string. Return `None` if step 1 cannot find the dataset in the cache.

The helper is pure - no network calls, takes the dataset cache as a parameter. Tested in cell 6's inline tests with at least 4 fixtures: a clean SAQL with named load (assert load swap), a SAQL with already-resolved `id/version` load (assert no change to the load line), a SAQL with Mustache filter lines (assert strip), and a SAQL whose dataset is missing from the cache (assert returns `None`).

Dataset cache is built lazily: first time cell 6 needs a name not in the cache, walk `GET /wave/datasets?pageSize=200` (paginated like the Option D POC) once.

### Cell 7 static rules (full set)

| #   | Rule                           | Source                                                                      | Severity   | Phase 1 | Phase 2                                                             |
| --- | ------------------------------ | --------------------------------------------------------------------------- | ---------- | ------- | ------------------------------------------------------------------- |
| 1   | `stale_picklist`               | substring `apts_primary_quote_type` in SAQL                                 | BLOCKING   | yes     | rewritten - source changed from filter column list to SAQL string   |
| 2   | `fiscal_date_filter`           | substring `THIS_FISCAL_*` / `LAST_FISCAL_*` / `fiscal_year` in SAQL         | WRONG-DATA | yes     | rewritten to scan SAQL string                                       |
| 3   | `renewal_uses_amount_not_acv`  | renewal hint AND SAQL aggregates Amount, missing `APTS_Renewal_ACV__c`      | BLOCKING   | yes     | rewritten to regex on SAQL `sum(...)`                               |
| 4   | `pipeline_uses_amount_not_arr` | pipeline hint AND SAQL aggregates Amount, missing `APTS_Opportunity_ARR__c` | WRONG-DATA | yes     | rewritten to regex on SAQL `sum(...)`                               |
| 5   | `tabular_top_n`                | SF report format                                                            | WRONG-DATA | yes     | DROPPED (no CRMA equivalent)                                        |
| 6   | `missing_field_not_shown`      | SF detail columns                                                           | WRONG-DATA | yes     | DROPPED (no CRMA equivalent)                                        |
| 7   | `em_dash_in_title`             | widget title char scan                                                      | COSMETIC   | yes     | reused (source = `state.widgets[*].parameters.title`)               |
| 8   | `fiscal_in_title`              | widget title substring                                                      | COSMETIC   | yes     | reused (same source)                                                |
| 9   | `saql_invalid_count_with_arg`  | regex `count\s*\(\s*['"][^'"]+['"]\s*\)`                                    | BLOCKING   | NEW     | invalid SAQL per CLAUDE.md                                          |
| 10  | `saql_sum_case_when`           | substring `sum(case when` (case-insensitive)                                | WRONG-DATA | NEW     | invalid SAQL per CLAUDE.md                                          |
| 11  | `saql_unbound_dataset_load`    | regex `q\s*=\s*load\s+"[^"/]+"` (no slash in load)                          | COSMETIC   | NEW     | informational - normal at runtime, audit-relevant for the load swap |

The hint helper (`_hint_for(widget)`) is reused unchanged - walks the spec's KPI bullets and returns the first one with a 4+ char word that appears in the widget title.

**Which SAQL string the static rules see.** Static rules operate on `widget["step_query"]` from the cell 4 emit shape - i.e. the post-`html.unescape()` SAQL string with the original `load "DatasetName"` line still in place and the Mustache `{{...}}` filter bindings still present. They do NOT see the cell 6 `prepare_saql()` output (which has the load line rewritten and Mustache stripped for runnability). Rule 11 (`saql_unbound_dataset_load`) is meaningful precisely because we test against the pre-rewrite version.

### Cell 8 compare wrapper rewrite

New shape:

```python
def compare_crma(
    spec: dict[str, dict[str, Any]],
    widgets: list[dict[str, Any]],
    static_issues_by_widget: dict[str, list[dict[str, str]]],
    saql_run_by_step: dict[tuple[str, str], dict[str, Any]],
    top_n: int = TOP_N_CANDIDATES,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Returns (coverage_rows, orphan_rows).
    coverage_rows: ~14 * top_n rows, one per (spec_widget, candidate_rank).
    orphan_rows: every widget that did not land in any spec widget's top-N.
    """
```

**Matcher signature change.** The Phase 1 `match_widget_to_spec(widget, spec) -> str | None` returns only the best spec widget id, not the score. Phase 2 needs both. The cleanest refactor is to extract the scoring inner loop into `score_widget_against_spec(widget, spec) -> dict[spec_wid, float]` (returns the full score map) and rewrite Phase 1's `match_widget_to_spec` as a one-line wrapper that returns the argmax. This preserves the Phase 1 callsite shape (still `str | None`) while letting Phase 2's `compare_crma` consume the full map. The score helper, the stem extractor, and the stopword list all stay shared.

Algorithm:

1. For each widget in `widgets`, call `score_widget_against_spec(widget, spec)` and pick the single highest-scoring `spec_widget_id` (must be above `MATCHER_THRESHOLD`); record the widget plus its winning `(spec_widget_id, score)` pair, or mark it unmatched.
2. Group widgets by their winning `spec_widget_id`. Each group is the candidate pool for that spec widget. Unmatched widgets go to a holding list.
3. For each `spec_widget_id` in `spec`:
   - Sort its candidate pool by score desc.
   - Take the top `top_n`.
   - For each candidate, build a coverage row including: spec_widget, rank, source dashboard, step name, widget title, score, severity (worst from static rules + SAQL run failure), sample value, row count, issue text.
   - If the pool is empty, emit one row with rank `-`, severity `BLOCKING`, issue `(NO MATCH) - no CRMA widget across the 9 dashboards scored above MATCHER_THRESHOLD against the KPI bullet`.
4. Any widget that landed in a spec widget's pool but was bumped out of the top-N by higher-scoring siblings AND the unmatched holding list both go in `orphan_rows`, sorted by source dashboard.
5. The matcher's `_MATCH_STOPWORDS` may need 1-2 CRMA-specific additions on first run; defer until we see real false-orphans.

### Cell 9 markdown render

Same `render_markdown(...)` shape as Phase 1, returning a string. Sections written in order:

1. **Header** - run date, audit script path, spec path + commit hash, list of 9 dashboards with `lastModifiedDate`, dataset cache size, tally line.
2. **Section A: Spec coverage matrix** - sorted by spec widget order from `spec.keys()`, then by rank within each spec widget. Columns: Spec widget, Rank, Source dashboard, Step name, Widget title, Score, Severity, Sample value, Row count, Issue.
3. **Section B: Static rule appendix** - every audited widget, sorted by source dashboard then by widget title. Columns: Dashboard, Step name, Widget title, Widget type, Dataset, Severity, Issue list, Top value, Row count.
4. **Section C: Orphan widgets** - widgets that did not land in any top-N, sorted by source dashboard. Columns: Dashboard, Step name, Widget title, Severity, Recommendation slot (blank).
5. **Section D: Unused steps appendix** - one mini-table per dashboard that has any unused steps. Columns: Step name, Dataset, SAQL byte count.
6. **Severity legend** - BLOCKING / WRONG-DATA / ORPHAN / COSMETIC / OK plus the new pseudo-severity `(NO MATCH)`.
7. **Phase 2.5 / Phase 4 implications** - one paragraph each.
8. **Reproducibility block** - shell command to re-run.
9. **Spec commit hash footer**.

Section A's column header for `Row count` says "Rows when SAQL run with no dashboard context" so the deck-builder doesn't conflate it with the live dashboard's row count.

### Cell 10 composition

```python
def main():
    inst, tok = _cell1_main()
    _cell2_main(inst, tok)
    spec = _cell3_main()
    all_widgets, dashboard_meta, unused_steps_by_dashboard = _cell4_main(inst, tok)
    saql_run_by_step = _cell6_main(inst, tok, all_widgets)
    static_issues_by_widget = _cell7_main(all_widgets, saql_run_by_step, spec)
    coverage_rows, orphan_rows = _cell8_main(spec, all_widgets, static_issues_by_widget, saql_run_by_step)
    md = _cell9_main(
        coverage_rows=coverage_rows,
        orphan_rows=orphan_rows,
        unused_steps_by_dashboard=unused_steps_by_dashboard,
        dashboard_meta=dashboard_meta,
        spec_commit_hash=_get_spec_commit_hash(),
    )
    out_path = AUDIT_OUTPUT_DIR / "2026-04-07-sales-director-monthly-crma-audit.md"
    out_path.write_text(md)
    print(f"Wrote {out_path}")
```

## Output

Single file: `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md`. Sections detailed above. Idempotent within a day - re-running overwrites. A run on a different day writes a new file.

## Testing

Inline tests in every cell using small in-memory fixtures. Tests run on every script invocation, no separate harness.

- **Cell 4**: fake `state` dicts covering: 1 step + 2 widgets referencing it (assert 2 widget records, same `step_name`); 1 step + 0 widgets (assert it lands in `unused_steps`); 1 widget referencing a missing step (assert tagged broken, not silently dropped); 1 step with compact-JSON dict `query` containing HTML-encoded entities (assert unwrapped AND unescaped in `step_query`); 1 step with a SAQL string `query` containing HTML-encoded entities (assert unescaped in `step_query`).
- **Cell 6**: `prepare_saql()` fixtures: clean SAQL with named load (assert load swap), SAQL with already-resolved `id/version` load (assert no change to load line), SAQL with Mustache filter lines (assert strip), SAQL whose dataset is missing from the cache (assert returns `None`).
- **Cell 7**: 3 new fixtures for rules 9, 10, 11. Regression fixtures for rewritten rules 2, 3, 4 against SAQL strings.
- **Cell 8**: fake spec with 2 widgets, fake widgets list of 5 candidates spanning 2 dashboards. Assert each spec widget gets exactly its top-N, orphans land in `orphan_rows`, zero-candidate spec widget gets a `(NO MATCH) BLOCKING` row.
- **Cells 1, 2, 3, 9, 10**: existing inline tests reused verbatim where applicable.

## Error handling

- **Auth or token failure**: cell 1 propagates the `subprocess.CalledProcessError` and the script exits.
- **Picklist regression**: cell 2 prints an error and `sys.exit(1)`.
- **Dashboard 404**: cell 4 prints the missing ID and `sys.exit(1)`.
- **Step run failure or timeout**: cell 6 records the failure on the `(dashboard_id, step_name)` key but continues. Downstream the audit row gets BLOCKING severity with the error text.
- **Dataset not found in cache after a full walk**: cell 6 marks the step `_failed=True, error="dataset_not_found"` and continues. Audit row downstream gets BLOCKING.
- **Compact-JSON shape**: handled silently in cell 4 + cell 6's `prepare_saql()`.
- **Broken step reference from a widget**: tagged with a special static issue at cell 4 emit time and surfaces as BLOCKING in the coverage matrix.

## Risks (called out, not blockers)

1. **Dataset version drift.** The 9 dashboards refreshed 2026-04-06; if a `currentVersionId` has rolled, the audit numbers may not match what a real user sees. Mitigation: header captures both the dashboard `lastModifiedDate` and the dataset `currentVersionId` from the cache.
2. **Mustache binding strip changes counts.** Stripping `{{...}}` filter lines gives an unbound query that may return different counts than the live, dashboard-bound version. The audit grades the step shape, not reproduces live numbers. Section A column header calls this out explicitly.
3. **Matcher false-orphans.** Stem matcher may miss vocabulary mismatches (e.g. "Forecast Slippage" vs spec "Slipped deals analysis"). Mitigation: eyeball the `(NO MATCH)` rows after the first run; if there are >2 genuine misses, escalate to Phase 2.5 with an override YAML.
4. **Wall-time.** ~9 dashboards \* ~10 steps = ~90 SAQL runs. At ~1-2 sec each, 1.5-3 minutes per run. Acceptable.
5. **CRMA dashboard ID not in B2B_MA app.** Out of scope - audit fails closed if any of the 9 IDs returns 404.

## Acceptance criteria

The Phase 2 work is done when all of the following are true:

1. `scripts/audit_crma_sales_director_monthly.py` exists, all inline cell tests pass on a fresh run.
2. The script runs end-to-end against the live org without manual intervention beyond `sf` CLI auth.
3. `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md` exists, contains all four sections (A, B, C, D), and is committed by exact path.
4. Section A has exactly 14 spec widgets represented (one block per spec widget), each block having 0-3 candidate rows.
5. Tally line in the header matches the actual row counts in Sections A, B, C.
6. No more than 2 spec widgets land at `(NO MATCH)` after eyeballing - if more, escalate to Phase 2.5 with an override file before declaring Phase 2 done.
7. The two safe SF widgets called out in the Phase 1 audit (`commercial_approval_global` candidates and `land_stage3_no_approval_*` candidates) have at least one OK CRMA candidate each in Section A.
8. Spec commit hash is recorded in the audit footer.
9. Audit script stays uncommitted; audit output is committed by exact path.

## File paths

- New script (uncommitted): `scripts/audit_crma_sales_director_monthly.py`
- Output (commit by exact path): `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md`
- This design doc (commit): `docs/2026-04-07-sales-director-monthly-phase2-audit-design.md`
- Implementation plan (next, commit): `docs/2026-04-07-sales-director-monthly-phase2-audit-plan.md`
- Spec graded against: `docs/specs/sales-director-monthly-dashboard-spec.md`
- Reference: `scripts/simcorp_crma_chart_sample.py` (Option D POC)
- Reference: `scripts/audit_sales_director_monthly_dashboard.py` (Phase 1 audit, seed for the adapter)

## Handoff to writing-plans

After this design doc lands and Andre approves it, the next step is to invoke `superpowers:writing-plans` and produce a bite-sized cell-by-cell implementation plan at `docs/2026-04-07-sales-director-monthly-phase2-audit-plan.md`, modeled on the Phase 1 17-task plan. The plan should slice work along cell boundaries so each task is independently runnable and committable.
