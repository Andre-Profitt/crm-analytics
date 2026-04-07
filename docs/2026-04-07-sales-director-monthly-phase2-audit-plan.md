# Sales Director Monthly - Phase 2 CRMA Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `scripts/audit_crma_sales_director_monthly.py` (notebook-style, 10 cells) by adapting the Phase 1 audit script to fetch the 9 B2B_MA CRMA dashboards via the Wave API, run every saql-typed step's SAQL via `/wave/query`, grade against the 14-widget spec, and commit a single combined coverage matrix at `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md`.

**Architecture:** Copy `scripts/audit_sales_director_monthly_dashboard.py` to `scripts/audit_crma_sales_director_monthly.py`, then surgically replace cells 4 and 6, delete cell 5, extend cell 7 (drop 2 SF rules, rewrite 3 for SAQL, add 3 new SAQL rules), refactor cell 8's matcher to expose a score helper, replace cell 8's compare wrapper with a top-N coverage builder, extend cell 9 from 2 to 4 markdown sections, and adapt cell 10 plus `__main__` to loop the 9 dashboards. Cells 1, 2, 3 reused verbatim. The Phase 1 script stays untouched at its current path. Each cell carries inline `assert`-based tests in its `_cellN_main()` function (Phase 1 pattern).

**Tech Stack:** Python 3.13, `requests`, `subprocess` (for `sf org display`), `re`, `html`, `json`, `pathlib`, `datetime`. No pytest. No mocking. No MCP. No `build_*.py`. Auth via `sf org display --target-org apro@simcorp.com --json`. API v66.0. Same pattern as the Option D POC at `scripts/simcorp_crma_chart_sample.py` (re-confirmed alive 2026-04-07).

**Design doc (input):** `docs/2026-04-07-sales-director-monthly-phase2-audit-design.md` (commit `dc418b9`). Read it first if you have not.

**Spec graded against:** `docs/specs/sales-director-monthly-dashboard-spec.md` (committed in Phase 1; pin the commit hash at run time via `_get_spec_commit_hash()`).

---

## Ground truth from a 2026-04-07 dashboard probe

Confirmed by `GET /services/data/v66.0/wave/dashboards/0FKTb0000000KwPOAU` (Pipeline & Opportunity Operations):

- `state.steps[name]` is a dict with keys: `broadcastFacet, datasets, query, receiveFacetSource, selectMode, start, type, useExternalFilters, useGlobal`.
- Step types in the wild: `saql` (18 in this dashboard, string-typed `query`) and `aggregateflex` (3, dict-typed `query` for filter/facet helpers like `f_unit`). **Phase 2 audits only `type == "saql"` steps.** aggregateflex steps are filter helpers, not KPI-bearing, and get filtered out at cell 4.
- `state.steps[name]["datasets"]` is a list. Each entry has `id`, `name`, `label`, `url`. Phase 2 reads `datasets[0]["name"]` as the cache key and the cache returns `(id, currentVersionId)` for the load swap.
- `state.widgets[wid]` has only `parameters` and `type`. KPI-bearing widget types are `chart`, `number`. Filter widgets are `listselector` (their `parameters.step` resolves to an aggregateflex step). Header/label widgets are `text` (no `parameters.step`). Navigation widgets are `link` (no `parameters.step`).
- `parameters.title` is the human-readable label. `parameters.step` is the step name reference.
- SAQL strings come back HTML-entity-encoded (`&quot;` for `"`) and contain Mustache filter bindings like `{{coalesce(column(f_unit.selection, [&quot;UnitGroup&quot;]), column(f_unit.results, [&quot;UnitGroup&quot;]))}}`.

**Cell 4's widget filter (locked by ground truth):** keep a widget iff `parameters.step` is set AND `state.steps[parameters.step]["type"] == "saql"`. This naturally excludes filter widgets, headers, and navigation links.

**Wall-time estimate:** ~22 KPI widgets per dashboard \* 9 dashboards = ~200 SAQL runs at ~1-2 sec each = ~3-6 minutes per full audit run. Acceptable.

---

## File structure

```
crm-analytics/
|-- docs/
|   |-- 2026-04-07-sales-director-monthly-phase2-audit-design.md  (already committed at dc418b9)
|   |-- 2026-04-07-sales-director-monthly-phase2-audit-plan.md     (this file)
|   `-- audits/
|       `-- 2026-04-07-sales-director-monthly-crma-audit.md         (Task 12, NEW, committed by exact path at run time)
`-- scripts/
    |-- audit_sales_director_monthly_dashboard.py                   (Phase 1, UNTOUCHED)
    |-- audit_crma_sales_director_monthly.py                        (Tasks 1-10, NEW, uncommitted by convention)
    `-- simcorp_crma_chart_sample.py                                (Option D POC, reference only, untouched)
```

Responsibilities:

- **`scripts/audit_crma_sales_director_monthly.py`** - The Phase 2 audit tool. Notebook-style cells (`# %%` markers). Mirrors Phase 1's structure but adapted for CRMA's `state.steps` model and the Wave API endpoints. Stays uncommitted by `audit_*.py` convention.
- **`docs/audits/2026-04-07-sales-director-monthly-crma-audit.md`** - The combined coverage report. Four sections: spec coverage matrix (A), static rule appendix (B), orphan widgets (C), unused steps (D). Plus header, severity legend, Phase 2.5/4 implications, reproducibility footer. Committed by exact path.

Cell decomposition for the Phase 2 script:

| Cell | Purpose                                                                                                                                | Pure / impure  | Inline tests? | Phase 1 status                                    |
| ---- | -------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ------------- | ------------------------------------------------- |
| 0    | Imports + constants                                                                                                                    | pure           | no            | replaced (new constants)                          |
| 1    | `get_auth()` - shell out to `sf org display`                                                                                           | impure (shell) | no            | reused verbatim                                   |
| 2    | `assert_picklist_fresh()` - APTS_Primary_Quote_Type\_\_c must be empty                                                                 | impure (API)   | no            | reused verbatim                                   |
| 3    | `load_expected_spec(path)` - parse markdown spec                                                                                       | pure           | YES           | reused verbatim                                   |
| 4    | `extract_crma_widgets(state, dashboard_id, dashboard_name)` + dashboard fetch loop                                                     | mixed          | YES           | REPLACED                                          |
| 5    | (deleted - no CRMA equivalent)                                                                                                         | -              | -             | DELETED                                           |
| 6    | `prepare_saql()` + `run_saql()` + dataset cache walker                                                                                 | mixed          | YES           | REPLACED                                          |
| 7    | `apply_static_rules()` - 9 rules (1, 2, 3, 4, 7, 8, 9, 10, 11)                                                                         | pure           | YES           | EXTENDED (drop 5/6, rewrite 1/2/3/4, add 9/10/11) |
| 8    | `score_widget_against_spec()` (NEW helper) + `match_widget_to_spec()` (refactored thin wrapper) + `compare_crma()` (NEW top-N wrapper) | pure           | YES           | REFACTORED                                        |
| 9    | `render_markdown()` - 4 sections                                                                                                       | pure           | YES           | EXTENDED                                          |
| 10   | Composition - loops the 9 dashboards                                                                                                   | impure         | no            | ADAPTED                                           |
| -    | `if __name__ == "__main__":` block                                                                                                     | impure         | no            | REWIRED                                           |

---

## Task 0: Verify prerequisites

**Files:** none modified. Read-only checks.

- [ ] **Step 1: Verify Phase 1 audit script exists and is intact**

```bash
test -f ~/crm-analytics/scripts/audit_sales_director_monthly_dashboard.py && \
  wc -l ~/crm-analytics/scripts/audit_sales_director_monthly_dashboard.py
```

Expected: file exists, line count is 1369.

- [ ] **Step 2: Verify the Option D POC exists**

```bash
test -f ~/crm-analytics/scripts/simcorp_crma_chart_sample.py && \
  wc -l ~/crm-analytics/scripts/simcorp_crma_chart_sample.py
```

Expected: file exists, ~310 lines.

- [ ] **Step 3: Verify the spec exists and is at the expected commit**

```bash
test -f ~/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md && \
  cd ~/crm-analytics && git log -1 --format=%h -- docs/specs/sales-director-monthly-dashboard-spec.md
```

Expected: file exists. Hash should be `06713fb` (the field-resolved version from Phase 1) or later.

- [ ] **Step 4: Verify docs/audits/ exists from Phase 1**

```bash
test -d ~/crm-analytics/docs/audits && ls ~/crm-analytics/docs/audits/
```

Expected: directory exists, contains at least `2026-04-06-sales-director-monthly-audit.md`.

- [ ] **Step 5: Re-confirm Option D recipe alive against the live org**

```bash
cd ~/crm-analytics && python3 scripts/simcorp_crma_chart_sample.py 2>&1 | tail -10
```

Expected: prints `Auth OK`, `Rows returned: 7`, lists 7 sales regions, prints `Saved: ...sample_slide_crma_chart.pptx`. If this fails, STOP and surface the error - the Phase 2 script will hit the same auth/Wave API path.

- [ ] **Step 6: No commit**

Read-only checks. Nothing to commit.

---

## Task 1: Copy Phase 1 script and update the constants block

**Files:**

- Create: `scripts/audit_crma_sales_director_monthly.py` (by copy from Phase 1)
- Modify: `scripts/audit_crma_sales_director_monthly.py` lines 1-55 (header docstring + constants)

- [ ] **Step 1: Copy the Phase 1 script to the Phase 2 path**

```bash
cd ~/crm-analytics && cp scripts/audit_sales_director_monthly_dashboard.py scripts/audit_crma_sales_director_monthly.py
```

- [ ] **Step 2: Replace the module docstring**

In `scripts/audit_crma_sales_director_monthly.py`, replace the entire docstring at the top of the file (lines 2-17 in the Phase 1 script) with:

```python
"""Sales Director Monthly - Phase 2 CRMA Audit.

Audits the 9 B2B_MA CRM Analytics dashboards collectively against the
expected-widgets spec at docs/specs/sales-director-monthly-dashboard-spec.md.
For each of the 14 spec widgets, surfaces the top-3 best-matching CRMA
widgets across all 9 dashboards, with severity from static SAQL rules and
sample SAQL run results.

Output: a four-section coverage report at
docs/audits/2026-04-07-sales-director-monthly-crma-audit.md.

Notebook style: cells separated by `# %%` markers. Re-run any cell
independently in VSCode interactive or via `python3 -i`.

Design doc: docs/2026-04-07-sales-director-monthly-phase2-audit-design.md
Plan doc:   docs/2026-04-07-sales-director-monthly-phase2-audit-plan.md
"""
```

- [ ] **Step 3: Replace the constants block**

Find the `# %% Constants` cell and replace its body with:

```python
# %% Constants

DASHBOARD_IDS: dict[str, str] = {
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
SPEC_PATH = Path(
    "/Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md"
)
AUDIT_OUTPUT_DIR = Path("/Users/test/crm-analytics/docs/audits")
AUDIT_OUTPUT_FILENAME = "2026-04-07-sales-director-monthly-crma-audit.md"
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"
SAQL_QUERY_TIMEOUT_SECONDS = 60
TOP_N_CANDIDATES = 3
MATCHER_THRESHOLD = 1.0  # same as Phase 1

# Picklist the audit asserts is current (unchanged from Phase 1)
EXPECTED_PICKLIST_FIELD = "APTS_Primary_Quote_Type__c"
EXPECTED_PICKLIST_VALUES_PRESENT: set[str] = set()
EXPECTED_PICKLIST_VALUES_ABSENT = {"Quote", "Renewal"}

# Severity ranking - lexicographic max/min would be wrong, always go through this list.
SEVERITY_ORDER = ["BLOCKING", "WRONG-DATA", "ORPHAN", "COSMETIC", "OK"]
```

The `DASHBOARD_ID = "01ZTb00000FSP7hMAH"` line from Phase 1 is REMOVED entirely. The Phase 2 script does not have a single dashboard ID.

- [ ] **Step 4: Verify the file still parses as Python**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/audit_crma_sales_director_monthly.py').read()); print('parse OK')"
```

Expected: `parse OK` and exit 0. If parse fails, fix the syntax error before proceeding. Note: the rest of the Phase 1 script body still references `DASHBOARD_ID` in cells 4, 7, and 10 - those will fail at runtime (NameError) until later tasks replace them. The parse-only check at this step is sufficient.

- [ ] **Step 5: No commit**

Audit script stays uncommitted by convention.

---

## Task 2: Refactor cell 8's matcher to expose a score helper

**Files:**

- Modify: `scripts/audit_crma_sales_director_monthly.py` cell 8, lines containing `_MATCH_STOPWORDS`, `_stem`, and `match_widget_to_spec` (in the Phase 1 source these are around lines 553-617)

The Phase 1 `match_widget_to_spec(widget, spec) -> str | None` returns only the best `spec_widget_id`. Phase 2 needs the full `dict[spec_wid, float]` score map so the new `compare_crma` wrapper can pick top-N candidates per spec widget. The cleanest refactor: extract the inner scoring loop into `score_widget_against_spec()`, and rewrite `match_widget_to_spec()` as a one-line argmax wrapper. Keeps the Phase 1 callsite shape intact (still `str | None`). The score helper, the stem extractor, and the stopword list are shared.

- [ ] **Step 1: Replace the matcher helpers with the refactored version**

In cell 8, replace the existing `_MATCH_STOPWORDS`, `_stem`, and `match_widget_to_spec` definitions with:

```python
# %% Cell 8: Bidirectional comparison


# Matcher stopwords: stems that are too generic to be a strong signal on their
# own. A title that ONLY shares these with a bullet is not enough to match.
_MATCH_STOPWORDS = {
    "stage",
    "quarter",
    "quarterli",
    "rate",
    "date",
    "month",
    "region",
    "year",
    "data",
    "with",
    "from",
    "analysi",
    "overview",
    "track",
}


def _stem(word: str) -> str:
    """Very naive stemmer: drop trailing 's'/'es' for words longer than 4 chars."""
    w = word.lower()
    if len(w) > 5 and w.endswith("es"):
        return w[:-2]
    if len(w) > 4 and w.endswith("s"):
        return w[:-1]
    return w


def _title_stems(title: str) -> set[str]:
    """Extract stemmed words of length >= 4 from a title string."""
    return {_stem(w) for w in re.findall(r"\w+", title or "") if len(w) >= 4}


def score_widget_against_spec(
    widget: dict[str, Any],
    spec: dict[str, dict[str, Any]],
) -> dict[str, float]:
    """Return the full score map: spec_widget_id -> score.

    For each spec widget, score = sum over overlapping stems between the
    widget's title and the spec widget's KPI bullet, where each stem
    contributes 1.0 if it is not a stopword and 0.2 if it is. Spec widgets
    with zero overlap are NOT included in the returned dict.
    """
    title = widget.get("title") or ""
    title_stems = _title_stems(title)
    if not title_stems:
        return {}
    scores: dict[str, float] = {}
    for wid, row in spec.items():
        bullet = (row.get("KPI bullet") or "").lower()
        bullet_stems = {_stem(w) for w in re.findall(r"\w+", bullet) if len(w) >= 4}
        if not bullet_stems:
            continue
        overlap = title_stems & bullet_stems
        if not overlap:
            continue
        score = sum(0.2 if s in _MATCH_STOPWORDS else 1.0 for s in overlap)
        if score > 0:
            scores[wid] = score
    return scores


def match_widget_to_spec(
    widget: dict[str, Any],
    spec: dict[str, dict[str, Any]],
) -> str | None:
    """Return the single best-scoring spec_widget_id, or None if no match
    scores at or above MATCHER_THRESHOLD. Thin wrapper around
    score_widget_against_spec for callsites that only want the argmax.
    """
    scores = score_widget_against_spec(widget, spec)
    if not scores:
        return None
    best_wid, best_score = max(scores.items(), key=lambda kv: kv[1])
    return best_wid if best_score >= MATCHER_THRESHOLD else None
```

Note: `score_widget_against_spec` reads `widget.get("title")` (the Phase 2 widget record uses `title` as the matcher input). The Phase 1 matcher read `widget.get("display_title")` which was a fallback chain over `header/title`. Phase 2's cell 4 sets `title` directly from `state.widgets[*].parameters.title` so a single key suffices.

- [ ] **Step 2: Add inline tests for the score helper to `_cell8_main`**

In `_cell8_main`, BEFORE the existing `_tiny_spec / _tiny_dashboard / _tiny_static / _tiny_runs / _tiny_metas` fixture block (which is the Phase 1 compare test), add a new test block:

```python
    # Test score_widget_against_spec returns the full map
    _score_spec = {
        "pipe_a": {"KPI bullet": "Pipeline overview with quarterly focus"},
        "renew_a": {"KPI bullet": "Renewals tracking"},
        "noise": {"KPI bullet": "Stage data overview"},  # all stopwords
    }
    _scored = score_widget_against_spec(
        {"title": "Pipeline overview EMEA"}, _score_spec
    )
    assert "pipe_a" in _scored, f"Expected pipe_a in scores, got {_scored}"
    assert _scored["pipe_a"] >= 1.0, f"pipe_a should beat threshold, got {_scored}"
    assert "renew_a" not in _scored, f"renew_a should not match Pipeline title: {_scored}"

    # Test match_widget_to_spec is unchanged in shape
    _match = match_widget_to_spec({"title": "Pipeline overview EMEA"}, _score_spec)
    assert _match == "pipe_a", f"Expected pipe_a, got {_match}"

    _match_none = match_widget_to_spec({"title": "Random unrelated tile"}, _score_spec)
    assert _match_none is None, f"Expected None, got {_match_none}"

    # Test that a widget title sharing only stopwords does NOT match
    _match_stopword_only = match_widget_to_spec(
        {"title": "Stage data overview"}, _score_spec
    )
    # All three stems (stage, data, overview) are stopwords, contributing 0.6 total < 1.0
    assert _match_stopword_only is None, (
        f"Stopword-only match should fail threshold, got {_match_stopword_only}"
    )

    print("Score helper tests: PASS")
```

- [ ] **Step 3: Update the existing Phase 1 cell 8 test fixtures to use `title` not `display_title`**

In the same `_cell8_main`, the `_tiny_dashboard` fixture currently uses `display_title`. Phase 2 uses `title`. Change both occurrences:

```python
    _tiny_dashboard = [
        {
            "component_id": "c1",
            "title": "Pipeline overview EMEA quarterly",
            "type": "Report",
            "report_id": "r1",
        },
        {
            "component_id": "c2",
            "title": "Random unrelated tile",
            "type": "Report",
            "report_id": "r2",
        },
    ]
```

Note: `component_id` and `report_id` here are Phase 1 test scaffolding for the legacy `compare()` function, which Task 7 will replace. They stay in this task because we are only refactoring the matcher in Task 2; the legacy `compare()` and its asserts get deleted in Task 7.

- [ ] **Step 4: Smoke-test cell 8 in isolation**

Temporarily edit the bottom-of-file `__main__` block to call only cell 8:

```python
if __name__ == "__main__":
    spec = _cell3_main()
    _cell8_main(spec, [], {}, {}, {})
```

Then run:

```bash
cd ~/crm-analytics && python3 scripts/audit_crma_sales_director_monthly.py 2>&1 | tail -20
```

Expected output includes `Score helper tests: PASS` and `Cell 8 tests: PASS` (the existing Phase 1 tests should still pass against the refactored matcher because `match_widget_to_spec`'s external behavior is unchanged for the existing fixtures).

- [ ] **Step 5: Revert the temporary `__main__` change**

Restore the Phase 1 `__main__` block (Task 9 will rewrite it for real). For now, just keep the file in a state that parses cleanly. You can either restore the Phase 1 block exactly OR comment it out entirely - both are fine since the next task doesn't depend on `__main__`.

- [ ] **Step 6: No commit**

---

## Task 3: Replace cell 4 with the CRMA dashboard fetch + widget extractor

**Files:**

- Modify: `scripts/audit_crma_sales_director_monthly.py` cell 4 (replace `get_dashboard_describe`, `extract_widgets`, and `_cell4_main` entirely)

Cell 4 in Phase 2 fetches each of the 9 dashboards, walks `state.widgets`, follows each widget's `parameters.step` reference into `state.steps`, and emits one record per KPI-bearing widget (i.e. the referenced step has `type == "saql"`). Side-product: per-dashboard list of unused saql steps (saql steps not referenced by any widget). Returns three things: the flat list of widget records, a `dashboard_meta` dict, and `unused_steps_by_dashboard`.

- [ ] **Step 1: Delete the Phase 1 cell 4 implementation**

Delete the entire Phase 1 cell 4 block, which spans from `# %% Cell 4: Dashboard describe` through the end of the existing `_cell4_main` function. This includes `get_dashboard_describe`, `extract_widgets`, and `_cell4_main`.

- [ ] **Step 2: Add the new cell 4 header and the dashboard fetcher**

Insert after cell 3:

```python
# %% Cell 4: CRMA dashboard fetch + widget extraction


def get_crma_dashboard_state(
    inst: str, tok: str, dashboard_id: str
) -> dict[str, Any]:
    """GET /wave/dashboards/{id}, return the dashboard payload (not just state).

    Caller reads `payload["state"]` for steps/widgets and `payload["name"]` /
    `payload["lastModifiedDate"]` for header metadata.
    """
    url = f"{inst}/services/data/{API_VERSION}/wave/dashboards/{dashboard_id}"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    if r.status_code == 404:
        print(f"ERROR: CRMA dashboard {dashboard_id} not found (404)")
        sys.exit(1)
    r.raise_for_status()
    return r.json()
```

- [ ] **Step 3: Add the widget extractor**

Append to cell 4:

```python
def extract_crma_widgets(
    payload: dict[str, Any],
    dashboard_id: str,
    dashboard_name: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Walk state.widgets, follow each widget's parameters.step reference into
    state.steps, and emit one record per KPI-bearing widget.

    A widget is KPI-bearing iff:
    - it has parameters.step set
    - the referenced step exists in state.steps
    - the referenced step has type == "saql"

    Returns (widget_records, unused_saql_steps) where unused_saql_steps lists
    the names of saql-typed steps that no widget references.
    """
    state = payload.get("state", {}) or {}
    steps = state.get("steps", {}) or {}
    widgets = state.get("widgets", {}) or {}

    referenced_step_names: set[str] = set()
    widget_records: list[dict[str, Any]] = []

    for widget_id, w in widgets.items():
        params = w.get("parameters", {}) or {}
        step_name = params.get("step")
        if not step_name:
            continue
        step = steps.get(step_name)
        if step is None:
            # Broken reference - record it as a special widget so the audit
            # surfaces the dangling pointer rather than dropping it silently.
            widget_records.append(
                {
                    "dashboard_id": dashboard_id,
                    "dashboard_name": dashboard_name,
                    "widget_id": widget_id,
                    "widget_type": w.get("type", ""),
                    "title": params.get("title") or "",
                    "step_name": step_name,
                    "step_type": None,
                    "step_query": None,
                    "step_query_is_compact_json": False,
                    "dataset_name": None,
                    "broken_step_reference": True,
                    "raw_widget": w,
                    "raw_step": None,
                }
            )
            referenced_step_names.add(step_name)  # still counts as referenced
            continue
        step_type = step.get("type")
        referenced_step_names.add(step_name)
        if step_type != "saql":
            # aggregateflex / staticflex / etc are filter helpers, not KPI-bearing.
            # Skip them. They are not "unused" since a widget references them; they
            # are just not the kind of widget the audit grades.
            continue
        raw_query = step.get("query")
        is_compact_json = isinstance(raw_query, dict)
        if is_compact_json:
            inner = raw_query.get("query") if isinstance(raw_query, dict) else ""
            step_query = html.unescape(inner or "")
        else:
            step_query = html.unescape(raw_query or "")
        datasets_list = step.get("datasets") or []
        dataset_name = (
            datasets_list[0].get("name") if datasets_list else None
        )
        widget_records.append(
            {
                "dashboard_id": dashboard_id,
                "dashboard_name": dashboard_name,
                "widget_id": widget_id,
                "widget_type": w.get("type", ""),
                "title": params.get("title") or "",
                "step_name": step_name,
                "step_type": step_type,
                "step_query": step_query,
                "step_query_is_compact_json": is_compact_json,
                "dataset_name": dataset_name,
                "broken_step_reference": False,
                "raw_widget": w,
                "raw_step": step,
            }
        )

    # Unused saql steps: saql steps not referenced by any widget.
    unused_saql_steps = sorted(
        name
        for name, step in steps.items()
        if step.get("type") == "saql" and name not in referenced_step_names
    )
    return widget_records, unused_saql_steps
```

Note: `import html` must already be in the imports block from the Phase 1 copy. If it is not (Phase 1 may not have imported it), add `import html` to the top-of-file imports. Verify with `grep "^import html" scripts/audit_crma_sales_director_monthly.py`.

- [ ] **Step 4: Add the cell 4 main with inline tests**

Append to cell 4:

```python
def _cell4_main(
    inst: str, tok: str
) -> tuple[
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, list[str]],
]:
    """Run cell 4: fetch all 9 dashboards, extract KPI-bearing widgets, and
    return (all_widgets, dashboard_meta, unused_steps_by_dashboard)."""

    # Inline tests for extract_crma_widgets

    # Test 1: a widget referencing a saql step
    _payload_saql = {
        "name": "Test Dashboard",
        "state": {
            "steps": {
                "s_real": {
                    "type": "saql",
                    "query": "q = load &quot;Foo&quot;;\nq = group q by Region;",
                    "datasets": [{"id": "0Fb1", "name": "Foo"}],
                },
            },
            "widgets": {
                "chart_a": {
                    "type": "chart",
                    "parameters": {"title": "Pipeline by Region", "step": "s_real"},
                },
            },
        },
    }
    _widgets, _unused = extract_crma_widgets(_payload_saql, "TESTID", "Test Dashboard")
    assert len(_widgets) == 1, f"Expected 1 widget record, got {len(_widgets)}"
    assert _widgets[0]["title"] == "Pipeline by Region"
    assert _widgets[0]["step_name"] == "s_real"
    assert _widgets[0]["step_type"] == "saql"
    assert _widgets[0]["dataset_name"] == "Foo"
    assert "&quot;" not in _widgets[0]["step_query"], "html.unescape did not run"
    assert "Foo" in _widgets[0]["step_query"]
    assert _widgets[0]["broken_step_reference"] is False
    assert _unused == [], f"Expected no unused steps, got {_unused}"

    # Test 2: a widget referencing an aggregateflex (filter) step is skipped
    _payload_filter = {
        "name": "Test Dashboard",
        "state": {
            "steps": {
                "f_unit": {
                    "type": "aggregateflex",
                    "query": {"query": "{...}", "version": -1.0},
                    "datasets": [{"id": "0Fb1", "name": "Foo"}],
                },
            },
            "widgets": {
                "lsel_a": {
                    "type": "listselector",
                    "parameters": {"title": "Unit Group", "step": "f_unit"},
                },
            },
        },
    }
    _widgets, _unused = extract_crma_widgets(_payload_filter, "TESTID", "Test Dashboard")
    assert _widgets == [], f"Expected filter widget skipped, got {_widgets}"
    assert _unused == [], f"f_unit is referenced (just not by a KPI widget); should not be unused"

    # Test 3: a saql step with no referencing widget lands in unused_saql_steps
    _payload_unused = {
        "name": "Test Dashboard",
        "state": {
            "steps": {
                "s_dead": {
                    "type": "saql",
                    "query": "q = load &quot;Foo&quot;;",
                    "datasets": [{"id": "0Fb1", "name": "Foo"}],
                },
            },
            "widgets": {},
        },
    }
    _widgets, _unused = extract_crma_widgets(_payload_unused, "TESTID", "Test Dashboard")
    assert _widgets == []
    assert _unused == ["s_dead"], f"Expected ['s_dead'], got {_unused}"

    # Test 4: a widget referencing a missing step is recorded as broken
    _payload_broken = {
        "name": "Test Dashboard",
        "state": {
            "steps": {},
            "widgets": {
                "chart_a": {
                    "type": "chart",
                    "parameters": {"title": "Orphaned Chart", "step": "s_missing"},
                },
            },
        },
    }
    _widgets, _unused = extract_crma_widgets(_payload_broken, "TESTID", "Test Dashboard")
    assert len(_widgets) == 1
    assert _widgets[0]["broken_step_reference"] is True
    assert _widgets[0]["step_query"] is None
    assert _widgets[0]["step_type"] is None

    # Test 5: a widget without parameters.step (header, link) is skipped silently
    _payload_text = {
        "name": "Test Dashboard",
        "state": {
            "steps": {},
            "widgets": {
                "hdr": {"type": "text", "parameters": {"title": "Page Header"}},
                "lnk": {"type": "link", "parameters": {}},
            },
        },
    }
    _widgets, _unused = extract_crma_widgets(_payload_text, "TESTID", "Test Dashboard")
    assert _widgets == []
    assert _unused == []

    print("Cell 4 tests: PASS")

    # Real fetch loop
    all_widgets: list[dict[str, Any]] = []
    dashboard_meta: dict[str, dict[str, Any]] = {}
    unused_steps_by_dashboard: dict[str, list[str]] = {}
    for dash_id, dash_label in DASHBOARD_IDS.items():
        payload = get_crma_dashboard_state(inst, tok, dash_id)
        name = payload.get("name") or dash_label
        last_modified = payload.get("lastModifiedDate") or ""
        widgets, unused = extract_crma_widgets(payload, dash_id, name)
        all_widgets.extend(widgets)
        dashboard_meta[dash_id] = {
            "name": name,
            "label": dash_label,
            "lastModifiedDate": last_modified,
            "widget_count": len(widgets),
        }
        unused_steps_by_dashboard[dash_id] = unused
        print(
            f"  {dash_id}  {name[:50]:<50}  widgets={len(widgets):>3}  unused_saql_steps={len(unused)}"
        )

    print(
        f"Cell 4: {len(all_widgets)} KPI-bearing widgets across {len(DASHBOARD_IDS)} dashboards"
    )
    return all_widgets, dashboard_meta, unused_steps_by_dashboard
```

- [ ] **Step 5: Smoke-test cell 4 against the live org**

Temporarily set the bottom `__main__` block to:

```python
if __name__ == "__main__":
    inst, tok = _cell1_main()
    _cell2_main(inst, tok)
    _cell3_main()
    all_widgets, dashboard_meta, unused = _cell4_main(inst, tok)
    print(f"Total widgets: {len(all_widgets)}")
    print(f"Dashboards with unused saql steps: {sum(1 for v in unused.values() if v)}")
```

Then run:

```bash
cd ~/crm-analytics && python3 scripts/audit_crma_sales_director_monthly.py 2>&1 | tail -30
```

Expected: `Cell 4 tests: PASS`, then 9 lines of dashboard summaries (one per ID), then `Cell 4: <N> KPI-bearing widgets across 9 dashboards` where `<N>` is somewhere between roughly 100 and 250 depending on the dashboards' real shape. If any dashboard 404s, the script will exit 1 with the missing ID printed - do NOT proceed; that means a dashboard ID in `DASHBOARD_IDS` is wrong.

- [ ] **Step 6: Revert the temporary `__main__` change**

Restore or comment out the `__main__` block. Task 9 will write the final version.

- [ ] **Step 7: No commit**

---

## Task 4: Delete cell 5 (report describes - no CRMA equivalent)

**Files:**

- Modify: `scripts/audit_crma_sales_director_monthly.py` cell 5 (delete the entire block)

- [ ] **Step 1: Delete the cell 5 block**

Find `# %% Cell 5: Report describes` and delete from that header through the end of `_cell5_main` (Phase 1's cell 5 contains `get_report_describe`, `extract_report_meta`, and `_cell5_main`). Delete all three.

- [ ] **Step 2: Verify nothing in the remaining file references the deleted symbols**

```bash
cd ~/crm-analytics && grep -n "get_report_describe\|extract_report_meta\|_cell5_main\|report_meta_by_id" scripts/audit_crma_sales_director_monthly.py
```

Expected: any references that come up are in cells 7, 8, or 10 - those will be replaced in later tasks. Note their line numbers; they will be removed when those cells are rewritten. The grep is for awareness, not failure.

- [ ] **Step 3: Verify file still parses**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/audit_crma_sales_director_monthly.py').read()); print('parse OK')"
```

Expected: `parse OK`. The file will fail at runtime (NameError on `_cell5_main`, etc.) until Task 9 rewires `__main__`, but that is fine - parse-only is sufficient for this task.

- [ ] **Step 4: No commit**

---

## Task 5: Replace cell 6 with the CRMA SAQL runner + dataset cache

**Files:**

- Modify: `scripts/audit_crma_sales_director_monthly.py` cell 6 (replace `run_report` and `_cell6_main` entirely)

Cell 6 in Phase 2 builds a `(dataset_name -> (id, version))` cache once via `GET /wave/datasets`, then for each unique `(dashboard_id, step_name)` widget pair runs the prepared SAQL via `POST /wave/query` with a 60-second timeout. Returns `dict[(dashboard_id, step_name), result_dict]`.

- [ ] **Step 1: Delete the Phase 1 cell 6 implementation**

Delete from `# %% Cell 6: Run reports` through the end of the existing `_cell6_main`.

- [ ] **Step 2: Add the new cell 6 header and the dataset cache walker**

Insert after cell 4 (or where the old cell 6 was):

```python
# %% Cell 6: SAQL preparation + run via /wave/query


def build_dataset_cache(inst: str, tok: str) -> dict[str, tuple[str, str]]:
    """Walk GET /wave/datasets paginated and return name -> (id, currentVersionId).

    Same pagination pattern as the Option D POC at scripts/simcorp_crma_chart_sample.py.
    """
    cache: dict[str, tuple[str, str]] = {}
    url = f"{inst}/services/data/{API_VERSION}/wave/datasets?pageSize=200"
    while url:
        r = requests.get(
            url, headers={"Authorization": f"Bearer {tok}"}, timeout=30
        )
        r.raise_for_status()
        j = r.json()
        for ds in j.get("datasets", []) or []:
            name = ds.get("name")
            ds_id = ds.get("id")
            ver = ds.get("currentVersionId")
            if name and ds_id and ver:
                cache[name] = (ds_id, ver)
        next_path = j.get("nextPageUrl")
        url = f"{inst}{next_path}" if next_path else None
    return cache
```

- [ ] **Step 3: Add the prepare_saql helper**

Append to cell 6:

```python
def prepare_saql(
    unescaped_query: str,
    dataset_versions: dict[str, tuple[str, str]],
) -> str | None:
    """Take an already-html-unescaped SAQL string and return a runnable version.

    Steps:
    1. Find the load string. Regex `q\\s*=\\s*load\\s*"([^"]+)"`.
       - If the captured value contains a `/`, leave the load line alone (already
         resolved as `id/version`).
       - Otherwise, look up the name in dataset_versions. If missing, return None.
         If found, substitute `q = load "{id}/{version}"`.
    2. Strip every line containing both `{{` and `}}` (Mustache filter bindings).
    3. Return the rewritten SAQL string.

    Pure: no network calls. The dataset cache is passed in.
    """
    if not unescaped_query:
        return None
    m = re.search(r'q\s*=\s*load\s*"([^"]+)"', unescaped_query)
    if not m:
        return None
    load_target = m.group(1)
    if "/" not in load_target:
        # Bare dataset name - look up id/version and substitute.
        if load_target not in dataset_versions:
            return None
        ds_id, ver = dataset_versions[load_target]
        rewritten = re.sub(
            r'q\s*=\s*load\s*"[^"]+"',
            f'q = load "{ds_id}/{ver}"',
            unescaped_query,
            count=1,
        )
    else:
        rewritten = unescaped_query
    out_lines = [
        line for line in rewritten.splitlines() if not ("{{" in line and "}}" in line)
    ]
    return "\n".join(out_lines)
```

- [ ] **Step 4: Add the run_saql helper**

Append to cell 6:

```python
def run_saql(inst: str, tok: str, prepared_saql: str) -> dict[str, Any]:
    """POST /wave/query with the prepared SAQL string. Synchronous.

    Returns:
        {
          "_failed": bool,
          "_timeout": bool,
          "row_count": int | None,
          "top_value": Any,   # first measure of first row, or None
          "error": str | None,
        }
    """
    url = f"{inst}/services/data/{API_VERSION}/wave/query"
    try:
        r = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {tok}",
                "Content-Type": "application/json",
            },
            json={"query": prepared_saql},
            timeout=SAQL_QUERY_TIMEOUT_SECONDS,
        )
    except requests.Timeout:
        return {
            "_failed": True,
            "_timeout": True,
            "row_count": None,
            "top_value": None,
            "error": "timeout",
        }
    except requests.RequestException as e:
        return {
            "_failed": True,
            "_timeout": False,
            "row_count": None,
            "top_value": None,
            "error": str(e),
        }
    if not r.ok:
        return {
            "_failed": True,
            "_timeout": False,
            "row_count": None,
            "top_value": None,
            "error": f"{r.status_code}: {r.text[:200]}",
        }
    payload = r.json()
    records = (payload.get("results", {}) or {}).get("records", []) or []
    row_count = len(records)
    top_value: Any = None
    if row_count == 1 and records[0]:
        # number-widget shape: pull the first non-grouping numeric value
        first = records[0]
        for v in first.values():
            if isinstance(v, (int, float)):
                top_value = v
                break
    return {
        "_failed": False,
        "_timeout": False,
        "row_count": row_count,
        "top_value": top_value,
        "error": None,
    }
```

- [ ] **Step 5: Add `_cell6_main` with inline tests**

Append to cell 6:

```python
def _cell6_main(
    inst: str, tok: str, all_widgets: list[dict[str, Any]]
) -> dict[tuple[str, str], dict[str, Any]]:
    """Build dataset cache, run every unique (dashboard_id, step_name) widget's
    SAQL via /wave/query, return result map keyed by (dashboard_id, step_name).
    """

    # Inline tests for prepare_saql

    # Test 1: bare dataset name -> swap to id/version
    _cache = {"Foo": ("0Fb1", "0Fc1")}
    _saql = 'q = load "Foo";\nq = group q by Region;'
    _out = prepare_saql(_saql, _cache)
    assert _out is not None
    assert 'q = load "0Fb1/0Fc1"' in _out, f"load swap failed: {_out!r}"
    assert "group q by Region" in _out

    # Test 2: already-resolved id/version load -> leave unchanged
    _saql_resolved = 'q = load "0Fb1/0Fc1";\nq = group q by Region;'
    _out = prepare_saql(_saql_resolved, _cache)
    assert _out is not None
    assert 'q = load "0Fb1/0Fc1"' in _out

    # Test 3: Mustache filter lines stripped
    _saql_mustache = (
        'q = load "Foo";\n'
        "q = filter q by {{coalesce(column(f_unit.selection, [\"X\"]), [])}};\n"
        "q = group q by Region;"
    )
    _out = prepare_saql(_saql_mustache, _cache)
    assert _out is not None
    assert "{{" not in _out, f"Mustache not stripped: {_out!r}"
    assert "coalesce" not in _out, f"Mustache filter line not removed: {_out!r}"
    assert "group q by Region" in _out

    # Test 4: dataset missing from cache -> None
    _out_missing = prepare_saql('q = load "Bar";', {"Foo": ("0Fb1", "0Fc1")})
    assert _out_missing is None, f"Expected None, got {_out_missing!r}"

    # Test 5: empty / no-load query -> None
    assert prepare_saql("", _cache) is None
    assert prepare_saql("q = group q by X;", _cache) is None

    print("Cell 6 (prepare_saql) tests: PASS")

    # Real run
    print("Cell 6: building dataset cache...")
    dataset_versions = build_dataset_cache(inst, tok)
    print(f"  cached {len(dataset_versions)} datasets")

    saql_run_by_step: dict[tuple[str, str], dict[str, Any]] = {}
    unique_pairs = sorted(
        {
            (w["dashboard_id"], w["step_name"])
            for w in all_widgets
            if not w.get("broken_step_reference") and w.get("step_query")
        }
    )
    print(f"Cell 6: running {len(unique_pairs)} unique SAQL queries...")

    # Index queries by (dashboard_id, step_name) for lookup
    query_by_pair: dict[tuple[str, str], str] = {}
    for w in all_widgets:
        if w.get("broken_step_reference") or not w.get("step_query"):
            continue
        query_by_pair[(w["dashboard_id"], w["step_name"])] = w["step_query"]

    for pair in unique_pairs:
        raw = query_by_pair[pair]
        prepared = prepare_saql(raw, dataset_versions)
        if prepared is None:
            saql_run_by_step[pair] = {
                "_failed": True,
                "_timeout": False,
                "row_count": None,
                "top_value": None,
                "error": "prepare_saql returned None (dataset not in cache or no load clause)",
            }
            print(f"  {pair[0]}/{pair[1]}  PREP-FAIL")
            continue
        result = run_saql(inst, tok, prepared)
        saql_run_by_step[pair] = result
        if result["_timeout"]:
            print(f"  {pair[0]}/{pair[1]}  TIMEOUT")
        elif result["_failed"]:
            print(f"  {pair[0]}/{pair[1]}  FAIL  {(result['error'] or '')[:60]}")
        else:
            top = result.get("top_value")
            top_str = f"top={top!r}" if top is not None else "top=-"
            print(
                f"  {pair[0]}/{pair[1]}  OK    rows={result['row_count']:>4}  {top_str}"
            )

    return saql_run_by_step
```

- [ ] **Step 6: Smoke-test cell 6 with cell 4 against the live org**

Temporarily set `__main__` to:

```python
if __name__ == "__main__":
    inst, tok = _cell1_main()
    _cell2_main(inst, tok)
    spec = _cell3_main()
    all_widgets, dashboard_meta, unused = _cell4_main(inst, tok)
    saql_run = _cell6_main(inst, tok, all_widgets)
    failed = sum(1 for r in saql_run.values() if r["_failed"])
    print(f"\nSAQL run summary: {len(saql_run)} total, {failed} failed/timeout")
```

Run:

```bash
cd ~/crm-analytics && python3 scripts/audit_crma_sales_director_monthly.py 2>&1 | tail -50
```

Expected: `Cell 6 (prepare_saql) tests: PASS`, dataset cache size printed, then per-step lines (OK / FAIL / TIMEOUT). The summary should show roughly 100-250 unique SAQL runs with most OK. Some FAILs are expected (steps with bindings the matcher cannot strip cleanly, or complex filter chains). Note any hard timeouts - they will become BLOCKING entries downstream.

- [ ] **Step 7: Revert `__main__`**

- [ ] **Step 8: No commit**

---

## Task 6: Extend cell 7 (drop SF rules, rewrite for SAQL, add 3 new SAQL rules)

**Files:**

- Modify: `scripts/audit_crma_sales_director_monthly.py` cell 7 (replace `apply_static_rules` and `_cell7_main` entirely)

The Phase 1 cell 7 grades report metadata. The Phase 2 cell 7 grades the captured SAQL string from cell 4 (the post-`html.unescape`, pre-rewrite version). Drop rules 5 (`tabular_top_n`) and 6 (`missing_field_not_shown`). Rewrite rules 1-4 to scan SAQL strings. Add rules 9, 10, 11.

- [ ] **Step 1: Delete the Phase 1 cell 7 implementation**

Delete from `# %% Cell 7: Static rule scan` through the end of `_cell7_main`. This includes `_filter_column`, `_filter_value`, `_aggregate_field`, `apply_static_rules`, and `_cell7_main`.

- [ ] **Step 2: Add the new cell 7 header and `apply_static_rules`**

Insert in place of the old cell 7:

```python
# %% Cell 7: Static rule scan (SAQL-based)


def apply_static_rules(
    widget: dict[str, Any],
    kpi_bullet_hint: str = "",
) -> list[dict[str, str]]:
    """Return a list of issues found on this CRMA widget.

    Each issue is {"severity": ..., "rule": ..., "detail": ...}.
    Operates on widget["step_query"] (post-html.unescape, pre-Mustache-strip,
    pre-load-rewrite). kpi_bullet_hint is optional context from a coarse
    spec match used by rules 3 and 4.
    """
    issues: list[dict[str, str]] = []

    # Rule: broken step reference (recorded by cell 4)
    if widget.get("broken_step_reference"):
        issues.append(
            {
                "severity": "BLOCKING",
                "rule": "broken_step_reference",
                "detail": f"Widget references step {widget.get('step_name')!r} which does not exist in state.steps.",
            }
        )
        return issues

    saql = widget.get("step_query") or ""
    saql_lower = saql.lower()
    title = widget.get("title") or ""
    title_lower = title.lower()
    bullet_lower = kpi_bullet_hint.lower()

    # Rule 1: stale picklist on APTS_Primary_Quote_Type__c
    if "apts_primary_quote_type" in saql_lower:
        issues.append(
            {
                "severity": "BLOCKING",
                "rule": "stale_picklist",
                "detail": "SAQL references APTS_Primary_Quote_Type__c which is empty in the org. Switch to Type field (Land/Expand/Renewal).",
            }
        )

    # Rule 2: fiscal date filter / token in SAQL
    fiscal_tokens = (
        "this_fiscal_year",
        "this_fiscal_quarter",
        "last_fiscal_year",
        "last_fiscal_quarter",
        "next_fiscal_year",
        "next_fiscal_quarter",
        "fiscal_year",
    )
    if any(tok in saql_lower for tok in fiscal_tokens):
        issues.append(
            {
                "severity": "WRONG-DATA",
                "rule": "fiscal_date_filter",
                "detail": "SAQL uses a fiscal date token. The KPI brief requires calendar year. Switch to calendar-quarter / calendar-year filters.",
            }
        )

    # Rule 3: renewal widgets must aggregate APTS_Renewal_ACV__c, not Amount
    is_renewal_widget = ("renewal" in title_lower) or ("renewal" in bullet_lower)
    if is_renewal_widget:
        if re.search(r"sum\s*\(\s*['\"]?Amount['\"]?\s*\)", saql, re.IGNORECASE):
            if "APTS_Renewal_ACV__c" not in saql:
                issues.append(
                    {
                        "severity": "BLOCKING",
                        "rule": "renewal_uses_amount_not_acv",
                        "detail": "Renewal widget aggregates Amount instead of APTS_Renewal_ACV__c. Brief requires ACV for renewals.",
                    }
                )

    # Rule 4: pipeline widgets should aggregate APTS_Opportunity_ARR__c, not Amount
    is_pipeline_widget = (
        "pipeline" in title_lower or "pipeline" in bullet_lower
    ) and not is_renewal_widget
    if is_pipeline_widget:
        if re.search(r"sum\s*\(\s*['\"]?Amount['\"]?\s*\)", saql, re.IGNORECASE):
            if "APTS_Opportunity_ARR__c" not in saql:
                issues.append(
                    {
                        "severity": "WRONG-DATA",
                        "rule": "pipeline_uses_amount_not_arr",
                        "detail": "Pipeline widget aggregates Amount instead of APTS_Opportunity_ARR__c. Brief requires ARR.",
                    }
                )

    # Rule 7: em-dash in widget title
    if "\u2014" in title or "\u2013" in title:
        issues.append(
            {
                "severity": "COSMETIC",
                "rule": "em_dash_in_title",
                "detail": f"Widget title contains an em-dash or en-dash: {title!r}. Replace with a hyphen.",
            }
        )

    # Rule 8: 'fiscal' in widget title
    if "fiscal" in title_lower:
        issues.append(
            {
                "severity": "COSMETIC",
                "rule": "fiscal_in_title",
                "detail": f"Widget title contains 'fiscal': {title!r}. The brief requires calendar-year framing.",
            }
        )

    # Rule 9: invalid count('field') syntax (CRMA SAQL count() takes zero args)
    if re.search(r"count\s*\(\s*['\"][^'\"]+['\"]\s*\)", saql):
        issues.append(
            {
                "severity": "BLOCKING",
                "rule": "saql_invalid_count_with_arg",
                "detail": "SAQL uses count('field') which is invalid. count() takes zero arguments in CRMA SAQL.",
            }
        )

    # Rule 10: sum(case when ...) antipattern
    if "sum(case when" in saql_lower:
        issues.append(
            {
                "severity": "WRONG-DATA",
                "rule": "saql_sum_case_when",
                "detail": "SAQL uses sum(case when ...) which is invalid in CRMA. Pre-compute via foreach before group by, then sum() the result.",
            }
        )

    # Rule 11: bare dataset load (informational - cell 6 will swap, but worth noting)
    if re.search(r'q\s*=\s*load\s+"[^"/]+"', saql):
        issues.append(
            {
                "severity": "COSMETIC",
                "rule": "saql_unbound_dataset_load",
                "detail": "SAQL uses a bare dataset name in load. Normal at runtime, audit-relevant for the load swap.",
            }
        )

    return issues
```

- [ ] **Step 3: Add `_cell7_main` with inline tests**

Append to cell 7:

```python
def _cell7_main(
    all_widgets: list[dict[str, Any]],
    expected_spec: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, str]]]:
    # Inline tests with fixture data

    _w_stale = {
        "title": "Renewal Pipeline by Quarter",
        "step_query": 'q = load "Foo";\nq = filter q by APTS_Primary_Quote_Type__c == "Renewal";\nq = foreach q generate sum(\'Amount\') as v;',
        "broken_step_reference": False,
    }
    issues = apply_static_rules(_w_stale, "Renewals tracking")
    rules = {i["rule"] for i in issues}
    assert "stale_picklist" in rules, f"Expected stale_picklist, got {rules}"
    assert "renewal_uses_amount_not_acv" in rules, (
        f"Expected renewal_uses_amount_not_acv, got {rules}"
    )

    _w_fiscal = {
        "title": "Renewal pipeline this quarter",
        "step_query": 'q = load "Foo/0Fc1";\nq = filter q by date(\'CloseDate\') in [THIS_FISCAL_QUARTER];\nq = foreach q generate sum(\'APTS_Renewal_ACV__c\') as v;',
        "broken_step_reference": False,
    }
    issues = apply_static_rules(_w_fiscal, "Renewals tracking")
    rules = {i["rule"] for i in issues}
    assert "fiscal_date_filter" in rules, f"Expected fiscal_date_filter, got {rules}"
    assert "renewal_uses_amount_not_acv" not in rules, (
        f"Should not flag amount-vs-acv when ACV is in the SAQL: {rules}"
    )

    _w_count = {
        "title": "Open Opps By Stage",
        "step_query": "q = foreach q generate count('Id') as N;",
        "broken_step_reference": False,
    }
    issues = apply_static_rules(_w_count)
    assert "saql_invalid_count_with_arg" in {i["rule"] for i in issues}

    _w_sumcase = {
        "title": "Pipeline With Conditional",
        "step_query": "q = foreach q generate sum(case when Stage > 3 then 1 else 0 end) as v;",
        "broken_step_reference": False,
    }
    issues = apply_static_rules(_w_sumcase, "Pipeline overview")
    assert "saql_sum_case_when" in {i["rule"] for i in issues}

    _w_clean = {
        "title": "Renewal ACV by Calendar Quarter",
        "step_query": 'q = load "Foo/0Fc1";\nq = group q by Quarter;\nq = foreach q generate sum(\'APTS_Renewal_ACV__c\') as v;',
        "broken_step_reference": False,
    }
    issues = apply_static_rules(_w_clean, "Renewals tracking")
    assert issues == [], f"Expected no issues on clean ACV widget, got {issues}"

    _w_broken = {"broken_step_reference": True, "step_name": "s_missing"}
    issues = apply_static_rules(_w_broken)
    assert any(i["rule"] == "broken_step_reference" for i in issues)

    print("Cell 7 tests: PASS")

    # Build a coarse hint map (same as Phase 1's _hint_for)
    def _hint_for(widget: dict[str, Any]) -> str:
        t = (widget.get("title") or "").lower()
        for wid, row in expected_spec.items():
            bullet = (row.get("KPI bullet") or "").lower()
            words = [w for w in bullet.split() if len(w) >= 4]
            if any(w in t for w in words):
                return row.get("KPI bullet", "")
        return ""

    static_issues_by_widget: dict[str, list[dict[str, str]]] = {}
    for w in all_widgets:
        key = f"{w['dashboard_id']}/{w['widget_id']}"
        hint = _hint_for(w)
        static_issues_by_widget[key] = apply_static_rules(w, hint)

    total_static = sum(len(v) for v in static_issues_by_widget.values())
    rule_counts: dict[str, int] = {}
    for issues in static_issues_by_widget.values():
        for i in issues:
            rule_counts[i["rule"]] = rule_counts.get(i["rule"], 0) + 1
    print(
        f"Cell 7: {total_static} static issues across {len(all_widgets)} widgets"
    )
    for rule, count in sorted(rule_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {rule:<32}  {count}")
    return static_issues_by_widget
```

Note: the static issues key is `f"{dashboard_id}/{widget_id}"` (composite) because widget_ids are only unique within a dashboard. Cell 8's compare wrapper must use the same composite key.

- [ ] **Step 4: Smoke-test cells 4+6+7 together**

Temporarily set `__main__`:

```python
if __name__ == "__main__":
    inst, tok = _cell1_main()
    _cell2_main(inst, tok)
    spec = _cell3_main()
    all_widgets, dashboard_meta, unused = _cell4_main(inst, tok)
    saql_run = _cell6_main(inst, tok, all_widgets)
    static_issues = _cell7_main(all_widgets, spec)
```

Run:

```bash
cd ~/crm-analytics && python3 scripts/audit_crma_sales_director_monthly.py 2>&1 | tail -40
```

Expected: `Cell 7 tests: PASS` then a per-rule tally. Some rule hits are expected (probably some fiscal date filters in the wild). If `Cell 7 tests` fails, fix the regex / fixture mismatch before proceeding.

- [ ] **Step 5: Revert `__main__`**

- [ ] **Step 6: No commit**

---

## Task 7: Replace cell 8's compare wrapper with `compare_crma`

**Files:**

- Modify: `scripts/audit_crma_sales_director_monthly.py` cell 8 (replace the existing `compare()` function and the existing portion of `_cell8_main` that tests it; the matcher refactor from Task 2 stays)

Phase 1's `compare()` returns a flat list of audit entries with one entry per dashboard widget plus one MISSING entry per uncovered spec widget. Phase 2's `compare_crma()` returns two lists: `coverage_rows` (top-N per spec widget) and `orphan_rows` (widgets that did not land in any top-N).

- [ ] **Step 1: Delete the Phase 1 `compare()` function**

In cell 8, find the `def compare(...)` function (between the matcher helpers from Task 2 and the start of `_cell8_main`) and delete it entirely.

- [ ] **Step 2: Add `compare_crma`**

Insert between the matcher helpers and `_cell8_main`:

```python
def compare_crma(
    spec: dict[str, dict[str, Any]],
    widgets: list[dict[str, Any]],
    static_issues_by_widget: dict[str, list[dict[str, str]]],
    saql_run_by_step: dict[tuple[str, str], dict[str, Any]],
    top_n: int = TOP_N_CANDIDATES,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build the top-N coverage matrix per spec widget plus the orphan list.

    Returns (coverage_rows, orphan_rows).
    """

    # Step 1: score every widget against the spec, pick its single winner above threshold
    candidates_by_spec: dict[str, list[dict[str, Any]]] = {wid: [] for wid in spec}
    unmatched_widgets: list[dict[str, Any]] = []
    for w in widgets:
        scores = score_widget_against_spec(w, spec)
        if not scores:
            unmatched_widgets.append(w)
            continue
        best_wid, best_score = max(scores.items(), key=lambda kv: kv[1])
        if best_score < MATCHER_THRESHOLD:
            unmatched_widgets.append(w)
            continue
        candidates_by_spec[best_wid].append({"widget": w, "score": best_score})

    # Helper: pick the worst-severity static issue across all of a widget's issues + saql run state
    def _row_severity_and_issue(w: dict[str, Any]) -> tuple[str, str]:
        key = f"{w['dashboard_id']}/{w['widget_id']}"
        issues = list(static_issues_by_widget.get(key, []))
        run = saql_run_by_step.get((w["dashboard_id"], w["step_name"]), {})
        if run.get("_failed") or run.get("_timeout"):
            err = "timeout" if run.get("_timeout") else (run.get("error") or "unknown")
            issues.append(
                {
                    "severity": "BLOCKING",
                    "rule": "saql_run_failed",
                    "detail": f"SAQL run failed: {err}",
                }
            )
        if not issues:
            return "OK", ""
        worst = min(issues, key=lambda i: SEVERITY_ORDER.index(i["severity"]))
        summary = "; ".join(i["detail"] for i in issues)
        return worst["severity"], summary

    # Step 2: build coverage rows
    coverage_rows: list[dict[str, Any]] = []
    used_widget_keys: set[tuple[str, str]] = set()
    for spec_wid in spec:
        pool = sorted(
            candidates_by_spec[spec_wid], key=lambda c: c["score"], reverse=True
        )
        top = pool[:top_n]
        if not top:
            coverage_rows.append(
                {
                    "spec_widget": spec_wid,
                    "kpi_bullet": spec[spec_wid].get("KPI bullet", ""),
                    "rank": "-",
                    "dashboard_id": "",
                    "dashboard_name": "",
                    "step_name": "",
                    "widget_title": "",
                    "score": "",
                    "severity": "BLOCKING",
                    "sample_top_value": None,
                    "row_count": None,
                    "issue": "(NO MATCH) - no CRMA widget across the 9 dashboards scored above MATCHER_THRESHOLD against the KPI bullet",
                }
            )
            continue
        for rank, cand in enumerate(top, start=1):
            w = cand["widget"]
            sev, issue = _row_severity_and_issue(w)
            run = saql_run_by_step.get((w["dashboard_id"], w["step_name"]), {})
            coverage_rows.append(
                {
                    "spec_widget": spec_wid,
                    "kpi_bullet": spec[spec_wid].get("KPI bullet", ""),
                    "rank": rank,
                    "dashboard_id": w["dashboard_id"],
                    "dashboard_name": w["dashboard_name"],
                    "step_name": w["step_name"],
                    "widget_title": w["title"],
                    "score": round(cand["score"], 2),
                    "severity": sev,
                    "sample_top_value": run.get("top_value"),
                    "row_count": run.get("row_count"),
                    "issue": issue,
                }
            )
            used_widget_keys.add((w["dashboard_id"], w["widget_id"]))

    # Step 3: orphans = unmatched widgets + matched widgets bumped out of top-N
    orphan_rows: list[dict[str, Any]] = []
    for w in unmatched_widgets:
        sev, issue = _row_severity_and_issue(w)
        orphan_rows.append(
            {
                "dashboard_id": w["dashboard_id"],
                "dashboard_name": w["dashboard_name"],
                "step_name": w["step_name"],
                "widget_title": w["title"],
                "widget_type": w.get("widget_type", ""),
                "severity": sev,
                "issue": issue or "Unmatched - no spec widget scored above threshold",
            }
        )
    # Bumped-out matched widgets
    for spec_wid, pool in candidates_by_spec.items():
        sorted_pool = sorted(pool, key=lambda c: c["score"], reverse=True)
        for cand in sorted_pool[top_n:]:
            w = cand["widget"]
            if (w["dashboard_id"], w["widget_id"]) in used_widget_keys:
                continue
            sev, issue = _row_severity_and_issue(w)
            orphan_rows.append(
                {
                    "dashboard_id": w["dashboard_id"],
                    "dashboard_name": w["dashboard_name"],
                    "step_name": w["step_name"],
                    "widget_title": w["title"],
                    "widget_type": w.get("widget_type", ""),
                    "severity": sev,
                    "issue": (
                        f"Bumped from {spec_wid} top-{top_n} (score {cand['score']:.2f}). "
                        + (issue or "")
                    ).strip(),
                }
            )
    orphan_rows.sort(key=lambda r: (r["dashboard_name"], r["widget_title"]))
    return coverage_rows, orphan_rows
```

- [ ] **Step 3: Replace the `_cell8_main` body's Phase 1 compare tests with new fixtures**

In `_cell8_main`, AFTER the score helper tests from Task 2, REPLACE the existing Phase 1 compare test block (the `_tiny_spec`/`_tiny_dashboard`/`compare(...)` calls) with the new compare_crma tests:

```python
    # Test compare_crma top-N + orphan logic
    _spec = {
        "pipe_a": {"KPI bullet": "Pipeline overview with quarterly focus"},
        "renew_a": {"KPI bullet": "Renewals tracking"},
        "missing_one": {"KPI bullet": "Slipped deals analysis"},
    }
    _widgets_compare = [
        {
            "dashboard_id": "DASH1",
            "dashboard_name": "Dash One",
            "widget_id": "w1",
            "widget_type": "chart",
            "title": "Pipeline overview EMEA",
            "step_name": "s1",
            "step_query": "q = load ...",
            "broken_step_reference": False,
        },
        {
            "dashboard_id": "DASH1",
            "dashboard_name": "Dash One",
            "widget_id": "w2",
            "widget_type": "chart",
            "title": "Pipeline overview NAM",
            "step_name": "s2",
            "step_query": "q = load ...",
            "broken_step_reference": False,
        },
        {
            "dashboard_id": "DASH2",
            "dashboard_name": "Dash Two",
            "widget_id": "w3",
            "widget_type": "chart",
            "title": "Pipeline overview APAC",
            "step_name": "s3",
            "step_query": "q = load ...",
            "broken_step_reference": False,
        },
        {
            "dashboard_id": "DASH2",
            "dashboard_name": "Dash Two",
            "widget_id": "w4",
            "widget_type": "chart",
            "title": "Pipeline overview Global",
            "step_name": "s4",
            "step_query": "q = load ...",
            "broken_step_reference": False,
        },
        {
            "dashboard_id": "DASH2",
            "dashboard_name": "Dash Two",
            "widget_id": "w5",
            "widget_type": "number",
            "title": "Renewal ACV This Quarter",
            "step_name": "s5",
            "step_query": "q = load ...",
            "broken_step_reference": False,
        },
        {
            "dashboard_id": "DASH2",
            "dashboard_name": "Dash Two",
            "widget_id": "w6",
            "widget_type": "chart",
            "title": "Random Unrelated Tile",
            "step_name": "s6",
            "step_query": "q = load ...",
            "broken_step_reference": False,
        },
    ]
    _static: dict[str, list[dict[str, str]]] = {
        f"{w['dashboard_id']}/{w['widget_id']}": [] for w in _widgets_compare
    }
    _runs: dict[tuple[str, str], dict[str, Any]] = {
        (w["dashboard_id"], w["step_name"]): {
            "_failed": False,
            "_timeout": False,
            "row_count": 4,
            "top_value": None,
        }
        for w in _widgets_compare
    }
    _coverage, _orphans = compare_crma(_spec, _widgets_compare, _static, _runs, top_n=3)

    # pipe_a should have exactly 3 candidate rows (top-N=3, 4 pipeline widgets exist)
    pipe_rows = [r for r in _coverage if r["spec_widget"] == "pipe_a"]
    assert len(pipe_rows) == 3, f"Expected 3 pipe_a rows, got {len(pipe_rows)}"
    assert all(r["rank"] in (1, 2, 3) for r in pipe_rows)
    assert all(r["severity"] == "OK" for r in pipe_rows)

    # renew_a should have exactly 1 candidate row
    renew_rows = [r for r in _coverage if r["spec_widget"] == "renew_a"]
    assert len(renew_rows) == 1, f"Expected 1 renew_a row, got {len(renew_rows)}"
    assert renew_rows[0]["widget_title"] == "Renewal ACV This Quarter"

    # missing_one should have exactly 1 (NO MATCH) row
    miss_rows = [r for r in _coverage if r["spec_widget"] == "missing_one"]
    assert len(miss_rows) == 1
    assert miss_rows[0]["rank"] == "-"
    assert miss_rows[0]["severity"] == "BLOCKING"
    assert "(NO MATCH)" in miss_rows[0]["issue"]

    # Random Unrelated Tile + the bumped-out 4th pipeline candidate -> orphans
    orphan_titles = {r["widget_title"] for r in _orphans}
    assert "Random Unrelated Tile" in orphan_titles
    assert any(r["widget_title"].startswith("Pipeline overview") for r in _orphans), (
        f"Expected one of the 4 pipeline widgets bumped to orphan, got {orphan_titles}"
    )

    # Test severity escalation: a SAQL-failed run upgrades a widget to BLOCKING
    _runs_failed = dict(_runs)
    _runs_failed[("DASH1", "s1")] = {
        "_failed": True,
        "_timeout": False,
        "row_count": None,
        "top_value": None,
        "error": "400: bad SAQL",
    }
    _coverage_f, _ = compare_crma(_spec, _widgets_compare, _static, _runs_failed, top_n=3)
    s1_rows = [
        r
        for r in _coverage_f
        if r["spec_widget"] == "pipe_a" and r["dashboard_id"] == "DASH1"
    ]
    s1_for_step1 = [r for r in s1_rows if r["step_name"] == "s1"]
    assert len(s1_for_step1) == 1
    assert s1_for_step1[0]["severity"] == "BLOCKING", (
        f"SAQL run failure should escalate to BLOCKING, got {s1_for_step1[0]['severity']}"
    )

    print("Cell 8 (compare_crma) tests: PASS")
```

The Phase 1 `_cell8_main` originally also called `compare(...)` against real data and computed a tally. Phase 2 needs the same, but returning the new `(coverage_rows, orphan_rows)` shape:

- [ ] **Step 4: Update the real-data section of `_cell8_main`**

After the test block above, replace the Phase 1 "Apply to real data" section with:

```python
    coverage_rows, orphan_rows = compare_crma(
        spec, widgets, static_issues_by_widget, saql_run_by_step, top_n=TOP_N_CANDIDATES
    )

    # Tally severities across coverage_rows + orphan_rows
    tally: dict[str, int] = {}
    for r in coverage_rows + orphan_rows:
        sev = r["severity"]
        tally[sev] = tally.get(sev, 0) + 1
    print(
        f"Cell 8: {len(coverage_rows)} coverage rows, {len(orphan_rows)} orphan rows"
    )
    for sev in SEVERITY_ORDER:
        if tally.get(sev):
            print(f"  {sev:<12}  {tally[sev]}")
    return coverage_rows, orphan_rows, tally
```

And update the `_cell8_main` signature to take the new arg shape:

```python
def _cell8_main(
    spec: dict[str, dict[str, Any]],
    widgets: list[dict[str, Any]],
    static_issues_by_widget: dict[str, list[dict[str, str]]],
    saql_run_by_step: dict[tuple[str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
```

- [ ] **Step 5: Smoke-test cells 4+6+7+8**

Temporarily set `__main__`:

```python
if __name__ == "__main__":
    inst, tok = _cell1_main()
    _cell2_main(inst, tok)
    spec = _cell3_main()
    all_widgets, dashboard_meta, unused = _cell4_main(inst, tok)
    saql_run = _cell6_main(inst, tok, all_widgets)
    static_issues = _cell7_main(all_widgets, spec)
    coverage, orphans, tally = _cell8_main(spec, all_widgets, static_issues, saql_run)
    print(f"\nFINAL: {len(coverage)} coverage rows, {len(orphans)} orphans")
```

Run:

```bash
cd ~/crm-analytics && python3 scripts/audit_crma_sales_director_monthly.py 2>&1 | tail -40
```

Expected: all cell test passes, then a coverage/orphan summary. Look at the final two lines for sanity. Coverage row count should be roughly `14 * top_n = 42` minus any spec widgets that have fewer than 3 candidates plus any `(NO MATCH)` rows (which add 1 row each).

- [ ] **Step 6: Revert `__main__`**

- [ ] **Step 7: No commit**

---

## Task 8: Extend cell 9 to render 4 markdown sections

**Files:**

- Modify: `scripts/audit_crma_sales_director_monthly.py` cell 9 (replace `render_markdown` and `_cell9_main` entirely)

The Phase 1 cell 9 renders 2 tables (executive summary + full appendix). Phase 2 renders 4 sections (coverage matrix, static rule appendix, orphans, unused steps) plus a header and footer.

- [ ] **Step 1: Delete the Phase 1 cell 9 implementation**

Delete from `# %% Cell 9: Markdown rendering` through the end of `_cell9_main`. This includes `_fmt_value`, `_escape_md_cell`, `render_markdown`, and `_cell9_main`.

- [ ] **Step 2: Add the new cell 9 helpers**

Insert in place of the old cell 9:

```python
# %% Cell 9: Markdown rendering


def _fmt_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        if abs(v) >= 1_000_000:
            return f"{v / 1_000_000:.2f}M"
        if abs(v) >= 1_000:
            return f"{v / 1_000:.1f}K"
        return f"{v:.0f}"
    return str(v)


def _escape_md_cell(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)
    s = s.replace("|", "\\|").replace("\n", " ")
    return s
```

- [ ] **Step 3: Add the new render_markdown**

Append to cell 9:

````python
def render_markdown(
    coverage_rows: list[dict[str, Any]],
    orphan_rows: list[dict[str, Any]],
    unused_steps_by_dashboard: dict[str, list[str]],
    dashboard_meta: dict[str, dict[str, Any]],
    spec: dict[str, dict[str, Any]],
    static_issues_by_widget: dict[str, list[dict[str, str]]],
    all_widgets: list[dict[str, Any]],
    saql_run_by_step: dict[tuple[str, str], dict[str, Any]],
    dataset_cache_size: int,
    tally: dict[str, int],
    rundate: str,
    spec_commit_hash: str = "",
) -> str:
    lines: list[str] = []
    lines.append(f"# Sales Director Monthly CRMA Audit - {rundate}")
    lines.append("")

    # Header
    lines.append("## Header")
    lines.append("")
    lines.append(f"- **Audit run date:** {rundate}")
    lines.append(
        "- **Audit script:** `scripts/audit_crma_sales_director_monthly.py` (uncommitted by convention)"
    )
    lines.append(
        f"- **Spec graded against:** `docs/specs/sales-director-monthly-dashboard-spec.md`"
        + (f" (commit `{spec_commit_hash}`)" if spec_commit_hash else "")
    )
    lines.append(f"- **Dataset cache size:** {dataset_cache_size}")
    lines.append("- **Dashboards in scope:**")
    for dash_id, meta in dashboard_meta.items():
        lines.append(
            f"  - `{dash_id}` {meta['name']}  (lastModified: {meta.get('lastModifiedDate') or 'unknown'}; KPI widgets: {meta['widget_count']})"
        )
    tally_parts = [f"{tally.get(s, 0)} {s}" for s in SEVERITY_ORDER if tally.get(s, 0)]
    lines.append(
        f"- **Tally:** {sum(tally.values())} entries  -  " + "  -  ".join(tally_parts)
    )
    lines.append("")

    # Section A: Spec coverage matrix
    lines.append("## Section A: Spec coverage matrix")
    lines.append("")
    lines.append(
        "Sorted by spec widget order from the spec file, then by candidate rank within each spec widget. The deck-builder reads this section in spec order to pick the CRMA step that feeds each slide. `Row count` is the number of rows the SAQL returned when run with no dashboard context (Mustache filter bindings stripped); it may differ from the live dashboard count."
    )
    lines.append("")
    lines.append(
        "| Spec widget | Rank | Source dashboard | Step name | Widget title | Score | Severity | Sample value | Row count | Issue |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    # Preserve spec order
    spec_order = list(spec.keys())
    sorted_coverage = sorted(
        coverage_rows,
        key=lambda r: (
            spec_order.index(r["spec_widget"])
            if r["spec_widget"] in spec_order
            else 999,
            r["rank"] if isinstance(r["rank"], int) else 999,
        ),
    )
    for r in sorted_coverage:
        lines.append(
            "| {sw} | {rk} | {dash} | {sn} | {wt} | {sc} | {sev} | {sv} | {rc} | {iss} |".format(
                sw=_escape_md_cell(r["spec_widget"]),
                rk=r["rank"],
                dash=_escape_md_cell(r.get("dashboard_name", "")),
                sn=_escape_md_cell(r.get("step_name", "")),
                wt=_escape_md_cell(r.get("widget_title", "")),
                sc=r.get("score", ""),
                sev=r["severity"],
                sv=_escape_md_cell(_fmt_value(r.get("sample_top_value"))),
                rc=_escape_md_cell(r.get("row_count") if r.get("row_count") is not None else ""),
                iss=_escape_md_cell(r.get("issue", "")),
            )
        )
    lines.append("")

    # Section B: Static rule appendix
    lines.append("## Section B: Static rule appendix")
    lines.append("")
    lines.append(
        "Every audited CRMA widget, sorted by source dashboard then by widget title. Greppable for any specific step or rule."
    )
    lines.append("")
    lines.append(
        "| Dashboard | Step name | Widget title | Type | Dataset | Severity | Issues | Top value | Row count |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    sorted_widgets = sorted(
        all_widgets, key=lambda w: (w.get("dashboard_name", ""), w.get("title", ""))
    )
    for w in sorted_widgets:
        key = f"{w['dashboard_id']}/{w['widget_id']}"
        issues = static_issues_by_widget.get(key, [])
        run = saql_run_by_step.get((w["dashboard_id"], w["step_name"]), {})
        if issues:
            worst = min(issues, key=lambda i: SEVERITY_ORDER.index(i["severity"]))
            sev = worst["severity"]
            issues_text = "; ".join(i["rule"] for i in issues)
        else:
            if run.get("_failed") or run.get("_timeout"):
                sev = "BLOCKING"
                issues_text = "saql_run_failed"
            else:
                sev = "OK"
                issues_text = ""
        lines.append(
            "| {dash} | {sn} | {wt} | {wtype} | {ds} | {sev} | {iss} | {tv} | {rc} |".format(
                dash=_escape_md_cell(w.get("dashboard_name", "")),
                sn=_escape_md_cell(w.get("step_name", "")),
                wt=_escape_md_cell(w.get("title", "")),
                wtype=_escape_md_cell(w.get("widget_type", "")),
                ds=_escape_md_cell(w.get("dataset_name", "")),
                sev=sev,
                iss=_escape_md_cell(issues_text),
                tv=_escape_md_cell(_fmt_value(run.get("top_value"))),
                rc=_escape_md_cell(run.get("row_count") if run.get("row_count") is not None else ""),
            )
        )
    lines.append("")

    # Section C: Orphan widgets
    lines.append("## Section C: Orphan widgets")
    lines.append("")
    lines.append(
        "Widgets that did not land in any spec widget's top-N. Sorted by source dashboard. Each row needs a keep / drop / fold-into-spec decision."
    )
    lines.append("")
    lines.append(
        "| Dashboard | Step name | Widget title | Type | Severity | Issue | Recommendation |"
    )
    lines.append("|---|---|---|---|---|---|---|")
    for r in orphan_rows:
        lines.append(
            "| {dash} | {sn} | {wt} | {wtype} | {sev} | {iss} | |".format(
                dash=_escape_md_cell(r.get("dashboard_name", "")),
                sn=_escape_md_cell(r.get("step_name", "")),
                wt=_escape_md_cell(r.get("widget_title", "")),
                wtype=_escape_md_cell(r.get("widget_type", "")),
                sev=r["severity"],
                iss=_escape_md_cell(r.get("issue", "")),
            )
        )
    lines.append("")

    # Section D: Unused steps appendix
    lines.append("## Section D: Unused saql steps")
    lines.append("")
    lines.append(
        "saql-typed steps that no widget references. Per dashboard. Empty sections are skipped."
    )
    lines.append("")
    has_any_unused = False
    for dash_id, meta in dashboard_meta.items():
        unused = unused_steps_by_dashboard.get(dash_id, [])
        if not unused:
            continue
        has_any_unused = True
        lines.append(f"### {meta['name']} (`{dash_id}`)")
        lines.append("")
        lines.append("| Step name |")
        lines.append("|---|")
        for s in unused:
            lines.append(f"| `{s}` |")
        lines.append("")
    if not has_any_unused:
        lines.append("(no unused saql steps across the 9 dashboards)")
        lines.append("")

    # Severity legend
    lines.append("## Severity legend")
    lines.append("")
    lines.append(
        "- **BLOCKING** - must be fixed before deck rebuild. Wrong field, stale picklist, broken step reference, SAQL run failure, or required-by-spec widget has zero matching CRMA candidates."
    )
    lines.append(
        "- **WRONG-DATA** - filters partially right, value is suspect (e.g. fiscal date filter, sum(case when) antipattern)."
    )
    lines.append(
        "- **ORPHAN** - widget exists on a dashboard but did not land in any spec widget's top-N. Decision needed: keep, drop, or fold into spec."
    )
    lines.append(
        "- **COSMETIC** - can ship as a follow-up. Em-dash, fiscal in title, bare dataset load."
    )
    lines.append("- **OK** - matches a spec widget and passes all static rules.")
    lines.append(
        "- **(NO MATCH)** - special pseudo-severity (renders as BLOCKING in the tally) for spec widgets with zero CRMA candidates above MATCHER_THRESHOLD."
    )
    lines.append("")

    # Phase 2.5 / Phase 4 implications
    lines.append("## Phase 2.5 / Phase 4 implications")
    lines.append("")
    lines.append(
        "- **Phase 2.5 (optional fix-it pass):** any BLOCKING or WRONG-DATA row in Section A whose top candidate is otherwise the right step but for a single fixable issue (e.g. fiscal date filter) should be patched via Wave API PATCH on the source dashboard's step query. See `crm-analytics/CLAUDE.md` for the PATCH gotchas."
    )
    lines.append(
        "- **Phase 4 (deck rebuild via Option D):** for each spec widget in Section A, take the rank-1 candidate (or rank-2 if rank-1 has a BLOCKING severity) and follow the Option D recipe at `scripts/simcorp_crma_chart_sample.py` to pull the SAQL via /wave/query and render as a native PowerPoint chart. Do NOT embed values from any (NO MATCH) row."
    )
    lines.append("")

    # Reproducibility
    lines.append("## Reproducibility")
    lines.append("")
    lines.append("```bash")
    lines.append("cd ~/crm-analytics")
    lines.append("python3 scripts/audit_crma_sales_director_monthly.py")
    lines.append("```")
    lines.append("")
    if spec_commit_hash:
        lines.append(f"Spec commit graded against: `{spec_commit_hash}`")
        lines.append("")
    return "\n".join(lines) + "\n"
````

- [ ] **Step 4: Add `_cell9_main` with inline tests**

Append to cell 9:

```python
def _cell9_main(
    coverage_rows: list[dict[str, Any]],
    orphan_rows: list[dict[str, Any]],
    unused_steps_by_dashboard: dict[str, list[str]],
    dashboard_meta: dict[str, dict[str, Any]],
    spec: dict[str, dict[str, Any]],
    static_issues_by_widget: dict[str, list[dict[str, str]]],
    all_widgets: list[dict[str, Any]],
    saql_run_by_step: dict[tuple[str, str], dict[str, Any]],
    dataset_cache_size: int,
    tally: dict[str, int],
    rundate: str,
    spec_commit_hash: str,
) -> str:
    # Inline tests for render_markdown
    _spec = {
        "p1": {"KPI bullet": "Pipeline overview"},
        "r1": {"KPI bullet": "Renewals tracking"},
    }
    _coverage = [
        {
            "spec_widget": "p1",
            "kpi_bullet": "Pipeline overview",
            "rank": 1,
            "dashboard_id": "DASH1",
            "dashboard_name": "Dash One",
            "step_name": "s1",
            "widget_title": "Pipeline EMEA",
            "score": 2.0,
            "severity": "OK",
            "sample_top_value": 1_500_000,
            "row_count": 1,
            "issue": "",
        },
        {
            "spec_widget": "r1",
            "kpi_bullet": "Renewals tracking",
            "rank": "-",
            "dashboard_id": "",
            "dashboard_name": "",
            "step_name": "",
            "widget_title": "",
            "score": "",
            "severity": "BLOCKING",
            "sample_top_value": None,
            "row_count": None,
            "issue": "(NO MATCH)",
        },
    ]
    _orphans = [
        {
            "dashboard_id": "DASH1",
            "dashboard_name": "Dash One",
            "step_name": "s_extra",
            "widget_title": "Random Tile",
            "widget_type": "chart",
            "severity": "ORPHAN",
            "issue": "no spec match",
        }
    ]
    _unused = {"DASH1": ["s_dead"]}
    _meta = {
        "DASH1": {
            "name": "Dash One",
            "label": "Dash One",
            "lastModifiedDate": "2026-04-06",
            "widget_count": 2,
        }
    }
    _md = render_markdown(
        coverage_rows=_coverage,
        orphan_rows=_orphans,
        unused_steps_by_dashboard=_unused,
        dashboard_meta=_meta,
        spec=_spec,
        static_issues_by_widget={},
        all_widgets=[],
        saql_run_by_step={},
        dataset_cache_size=5,
        tally={"OK": 1, "BLOCKING": 1, "ORPHAN": 1},
        rundate="2026-04-07",
        spec_commit_hash="abcd1234",
    )
    assert "Section A: Spec coverage matrix" in _md
    assert "Section B: Static rule appendix" in _md
    assert "Section C: Orphan widgets" in _md
    assert "Section D: Unused saql steps" in _md
    assert "1.50M" in _md, "value formatter did not fire on 1500000"
    assert "(NO MATCH)" in _md
    assert "abcd1234" in _md
    assert "Random Tile" in _md
    assert "s_dead" in _md

    print("Cell 9 tests: PASS")

    return render_markdown(
        coverage_rows=coverage_rows,
        orphan_rows=orphan_rows,
        unused_steps_by_dashboard=unused_steps_by_dashboard,
        dashboard_meta=dashboard_meta,
        spec=spec,
        static_issues_by_widget=static_issues_by_widget,
        all_widgets=all_widgets,
        saql_run_by_step=saql_run_by_step,
        dataset_cache_size=dataset_cache_size,
        tally=tally,
        rundate=rundate,
        spec_commit_hash=spec_commit_hash,
    )
```

- [ ] **Step 5: Smoke-test cell 9 in isolation**

Temporarily set `__main__`:

```python
if __name__ == "__main__":
    spec = _cell3_main()
    md = _cell9_main(
        coverage_rows=[],
        orphan_rows=[],
        unused_steps_by_dashboard={},
        dashboard_meta={},
        spec=spec,
        static_issues_by_widget={},
        all_widgets=[],
        saql_run_by_step={},
        dataset_cache_size=0,
        tally={},
        rundate="2026-04-07",
        spec_commit_hash="test",
    )
    print(md[:300])
```

Run:

```bash
cd ~/crm-analytics && python3 scripts/audit_crma_sales_director_monthly.py 2>&1 | tail -20
```

Expected: `Cell 9 tests: PASS` then the first 300 chars of an empty render. Confirms render_markdown is wired and the inline tests pass.

- [ ] **Step 6: Revert `__main__`**

- [ ] **Step 7: No commit**

---

## Task 9: Adapt cell 10 composition + rewrite the `__main__` block

**Files:**

- Modify: `scripts/audit_crma_sales_director_monthly.py` cell 10 (replace `_cell10_main`) and the bottom-of-file `if __name__ == "__main__":` block

- [ ] **Step 1: Replace `_cell10_main`**

Find the existing `_cell10_main` (Phase 1 version) and replace it with:

```python
# %% Cell 10: Composition - write the audit markdown to disk


def _get_spec_commit_hash() -> str:
    try:
        r = subprocess.run(
            ["git", "log", "-1", "--format=%h", "--", str(SPEC_PATH)],
            cwd="/Users/test/crm-analytics",
            capture_output=True,
            text=True,
            check=True,
        )
        return r.stdout.strip()
    except subprocess.CalledProcessError:
        return ""


def _cell10_main(
    coverage_rows: list[dict[str, Any]],
    orphan_rows: list[dict[str, Any]],
    unused_steps_by_dashboard: dict[str, list[str]],
    dashboard_meta: dict[str, dict[str, Any]],
    spec: dict[str, dict[str, Any]],
    static_issues_by_widget: dict[str, list[dict[str, str]]],
    all_widgets: list[dict[str, Any]],
    saql_run_by_step: dict[tuple[str, str], dict[str, Any]],
    dataset_cache_size: int,
    tally: dict[str, int],
) -> Path:
    rundate = dt.date.today().isoformat()
    spec_commit_hash = _get_spec_commit_hash()
    md = _cell9_main(
        coverage_rows=coverage_rows,
        orphan_rows=orphan_rows,
        unused_steps_by_dashboard=unused_steps_by_dashboard,
        dashboard_meta=dashboard_meta,
        spec=spec,
        static_issues_by_widget=static_issues_by_widget,
        all_widgets=all_widgets,
        saql_run_by_step=saql_run_by_step,
        dataset_cache_size=dataset_cache_size,
        tally=tally,
        rundate=rundate,
        spec_commit_hash=spec_commit_hash,
    )
    AUDIT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = AUDIT_OUTPUT_DIR / AUDIT_OUTPUT_FILENAME
    out_path.write_text(md)
    print(f"Wrote {out_path}  ({len(md)} bytes)")
    tally_parts = [f"{tally.get(s, 0)} {s}" for s in SEVERITY_ORDER if tally.get(s, 0)]
    print(f"Tally: {sum(tally.values())} entries  -  " + "  -  ".join(tally_parts))
    return out_path
```

- [ ] **Step 2: Rewrite the `__main__` block**

Replace the bottom `if __name__ == "__main__":` block with:

```python
if __name__ == "__main__":
    inst, tok = _cell1_main()
    _cell2_main(inst, tok)
    spec = _cell3_main()
    all_widgets, dashboard_meta, unused_steps_by_dashboard = _cell4_main(inst, tok)
    saql_run_by_step = _cell6_main(inst, tok, all_widgets)
    static_issues_by_widget = _cell7_main(all_widgets, spec)
    coverage_rows, orphan_rows, tally = _cell8_main(
        spec, all_widgets, static_issues_by_widget, saql_run_by_step
    )
    _cell10_main(
        coverage_rows=coverage_rows,
        orphan_rows=orphan_rows,
        unused_steps_by_dashboard=unused_steps_by_dashboard,
        dashboard_meta=dashboard_meta,
        spec=spec,
        static_issues_by_widget=static_issues_by_widget,
        all_widgets=all_widgets,
        saql_run_by_step=saql_run_by_step,
        dataset_cache_size=0,  # see note in step 3
        tally=tally,
    )
```

- [ ] **Step 3: Wire dataset_cache_size into `_cell6_main` return**

The render header shows the dataset cache size. Cell 6 builds it but currently doesn't return it. Update `_cell6_main` to return a tuple of `(saql_run_by_step, dataset_cache_size)` and update the `__main__` chain to capture it:

In `_cell6_main`, change the final `return saql_run_by_step` to:

```python
    return saql_run_by_step, len(dataset_versions)
```

And update its declared return type:

```python
def _cell6_main(
    inst: str, tok: str, all_widgets: list[dict[str, Any]]
) -> tuple[dict[tuple[str, str], dict[str, Any]], int]:
```

And update `__main__`:

```python
    saql_run_by_step, dataset_cache_size = _cell6_main(inst, tok, all_widgets)
```

And pass `dataset_cache_size=dataset_cache_size` instead of `=0` in the `_cell10_main` call.

- [ ] **Step 4: Verify the file parses cleanly**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/audit_crma_sales_director_monthly.py').read()); print('parse OK')"
```

Expected: `parse OK`.

- [ ] **Step 5: No commit**

---

## Task 10: Smoke test against Pipeline & Opportunity Operations only

**Files:** none modified (temporary diagnostic run)

Before running the full 9-dashboard sweep, validate end-to-end against the one dashboard the Option D POC has already exercised. This is the highest-confidence target.

- [ ] **Step 1: Temporarily restrict DASHBOARD_IDS to one entry**

Edit the constants block to comment out 8 of the 9 entries, leaving only:

```python
DASHBOARD_IDS: dict[str, str] = {
    "0FKTb0000000KwPOAU": "Pipeline & Opportunity Operations",
    # "0FKTb0000000K5BOAU": "Sales Ops Data Quality & Forecast Accuracy",
    # ... etc
}
```

- [ ] **Step 2: Run end-to-end against the single dashboard**

```bash
cd ~/crm-analytics && python3 scripts/audit_crma_sales_director_monthly.py 2>&1 | tee /tmp/phase2-smoke.log
```

Expected: full pipeline runs cleanly. All cell test PASS lines printed. Dataset cache built. SAQL queries run with most OK. `Wrote .../docs/audits/2026-04-07-sales-director-monthly-crma-audit.md` printed.

- [ ] **Step 3: Manually inspect the output**

```bash
head -80 ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-crma-audit.md
```

Check:

- Header section has the one dashboard listed correctly with widget count and lastModifiedDate.
- Section A has 14 spec widgets represented (one block per spec widget, possibly with `(NO MATCH)` for many since only 1 dashboard is in scope).
- Section A's `s_region_hygiene` step (the Option D POC's step) appears as a candidate for `pipeline_overview_emea` or one of the pipeline widgets.

If section A is missing widgets or the format looks wrong, fix the bug before unlocking the full 9-dashboard sweep.

- [ ] **Step 4: Restore the full DASHBOARD_IDS**

Uncomment the 8 entries to restore the full 9-dashboard list.

- [ ] **Step 5: Delete the smoke test output**

```bash
rm ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-crma-audit.md
```

The Task 11 full run will produce the canonical version. Don't commit the smoke version.

- [ ] **Step 6: No commit**

---

## Task 11: Run end-to-end against all 9 dashboards

**Files:** generates `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md`

- [ ] **Step 1: Run the full audit**

```bash
cd ~/crm-analytics && time python3 scripts/audit_crma_sales_director_monthly.py 2>&1 | tee /tmp/phase2-full.log
```

Expected: ~3-6 minutes wall time. All cell tests pass. ~150-250 SAQL runs, mostly OK. Final tally line printed. File written to `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md`.

- [ ] **Step 2: Inspect the tally**

```bash
grep -E "^- \*\*Tally" ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-crma-audit.md
```

Note the BLOCKING / WRONG-DATA / OK / `(NO MATCH)` counts.

- [ ] **Step 3: Count `(NO MATCH)` rows**

```bash
grep -c "(NO MATCH)" ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-crma-audit.md
```

Per acceptance criterion 6: must be `<= 2`. If 3 or more, STOP. Read the design doc's risk section: this is a matcher false-orphan situation. Eyeball each `(NO MATCH)` row. If they really are missing in the org, that's information; if they look like the matcher missed a vocabulary mismatch, escalate to Phase 2.5 (override file). Either way, do NOT proceed to commit until Andre weighs in.

- [ ] **Step 4: Inspect Section A coverage**

```bash
sed -n '/^## Section A:/,/^## Section B:/p' ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-crma-audit.md | head -80
```

Eyeball the first 80 lines of Section A. Confirm that the 14 spec widgets each have at least one candidate row (or a `(NO MATCH)` row). Confirm that the `commercial_approval_global` and `land_stage3_no_approval_*` widgets have at least one OK CRMA candidate (acceptance criterion 7).

- [ ] **Step 5: No commit yet**

The next task does the commit + acceptance gate.

---

## Task 12: Verify acceptance criteria and commit the audit output

**Files:**

- Commit by exact path: `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md`

- [ ] **Step 1: Walk the design doc's acceptance criteria one by one**

Open `docs/2026-04-07-sales-director-monthly-phase2-audit-design.md` and check each of the 9 acceptance criteria against the run:

1. Script exists, all inline cell tests pass on a fresh run. (Verify: Task 11 step 1 output.)
2. Runs end-to-end without manual intervention beyond `sf` CLI auth. (Verify: same.)
3. Output file exists and contains all four sections.
4. Section A has exactly 14 spec widget blocks.
5. Tally line matches actual row counts.
6. `<= 2` `(NO MATCH)` rows. (Task 11 step 3.)
7. `commercial_approval_global` and `land_stage3_no_approval_*` candidates each have at least one OK row in Section A.
8. Spec commit hash recorded in the footer.
9. Audit script stays uncommitted.

If any criterion fails, fix the issue (re-run if needed) before committing.

- [ ] **Step 2: Verify the audit script is still uncommitted**

```bash
cd ~/crm-analytics && git status --short scripts/audit_crma_sales_director_monthly.py
```

Expected: `?? scripts/audit_crma_sales_director_monthly.py`. If the file shows `M` (modified) or is staged, something went wrong - the script must stay uncommitted by convention.

- [ ] **Step 3: Stage the audit output file by exact path**

```bash
cd ~/crm-analytics && git add docs/audits/2026-04-07-sales-director-monthly-crma-audit.md
```

Do NOT use `git add .`, `-A`, or `-u`. Stage by exact path only.

- [ ] **Step 4: Verify only the one file is staged**

```bash
cd ~/crm-analytics && git diff --cached --name-only
```

Expected output: exactly one line `docs/audits/2026-04-07-sales-director-monthly-crma-audit.md`. If anything else is staged, run `git restore --staged <file>` for each extra path before committing.

- [ ] **Step 5: Commit by exact path**

```bash
cd ~/crm-analytics && git commit -m "$(cat <<'EOF'
docs: phase 2 crma audit of 9 b2b_ma dashboards against expected-widgets spec

Combined coverage matrix grading the 9 B2B_MA CRMA dashboards
collectively against the 14-widget spec at
docs/specs/sales-director-monthly-dashboard-spec.md (spec commit
recorded in the footer). Output of the Phase 2 audit per the design
at docs/2026-04-07-sales-director-monthly-phase2-audit-design.md
(commit dc418b9) and the plan at
docs/2026-04-07-sales-director-monthly-phase2-audit-plan.md.

Four sections:

- Section A: spec coverage matrix - top-3 candidate CRMA steps per
  spec widget, sorted in spec order. The deck-builder reads this
  section in spec order to pick the step that feeds each slide.
- Section B: static rule appendix - every audited CRMA widget with
  rule hits, top value, and row count. Greppable.
- Section C: orphan widgets - widgets that did not land in any spec
  widget's top-N. Each row needs a keep / drop / fold decision.
- Section D: unused saql steps - dead saql-typed steps with no
  widget reference, per dashboard.

Tally: <FILL IN FROM THE RUN>.

Audit script: scripts/audit_crma_sales_director_monthly.py (uncommitted
by convention, matches audit_*.py family).

Next: Phase 2.5 (optional fix-it pass for any BLOCKING/WRONG-DATA
candidates that are otherwise the right step but for a single fixable
issue) and Phase 4 (deck rebuild via Option D, sourced from the
rank-1 candidates in Section A).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

Replace `<FILL IN FROM THE RUN>` with the actual tally line from the audit file before committing.

- [ ] **Step 6: Verify the commit landed**

```bash
cd ~/crm-analytics && git log -1 --format="%h %s" && git status --short docs/audits/2026-04-07-sales-director-monthly-crma-audit.md
```

Expected: latest commit subject is the Phase 2 audit doc, and `git status` shows nothing for the audit file (clean).

- [ ] **Step 7: Do NOT push**

Per the hard rules from the Phase 1 handoff: never push to origin unless Andre explicitly says so. The commit stays local.

---

## Self-review checklist (run after writing the plan, before handoff)

After the plan is written, walk these checks once before declaring it ready:

- [ ] Every spec section in `docs/2026-04-07-sales-director-monthly-phase2-audit-design.md` has a corresponding task in this plan.
- [ ] No "TBD" / "TODO" / "implement later" / "add error handling" placeholders in any task body.
- [ ] Every code block contains complete, runnable code (no `# ...` ellipses for actual logic).
- [ ] Function and variable names match across tasks (e.g. `compare_crma`, `score_widget_against_spec`, `prepare_saql`, `extract_crma_widgets` are spelled identically everywhere).
- [ ] Cell numbering is consistent: cells 1-4, then no cell 5, then 6-10. The deletion of cell 5 is explicit in Task 4.
- [ ] All file paths are absolute or `~/crm-analytics`-relative; no ambiguous relative paths.
- [ ] Every commit step uses `git add <exact path>`, never `.` or `-A` or `-u`.
- [ ] The audit script is committed nowhere; only the audit OUTPUT is committed (Task 12).
- [ ] No em-dashes in any task body.

---

## Notes for the executor

- **Iteration pattern.** Each cell-replacement task ends with a smoke-test step that temporarily edits the `__main__` block. After the smoke test passes, revert `__main__` so subsequent tasks start from a clean state. Task 9 writes the final `__main__` block.
- **Phase 1 script untouched.** Tasks 1-12 only touch `scripts/audit_crma_sales_director_monthly.py`. The Phase 1 script at `scripts/audit_sales_director_monthly_dashboard.py` is the seed (copied in Task 1) and stays unchanged at its current path.
- **No mocking, no pytest.** Tests are inline `assert` statements at the top of each `_cellN_main` function, using small in-memory fixtures. The Phase 1 pattern. Tests run on every script invocation.
- **Wall time.** A full Task 11 run is ~3-6 minutes against the live org. Plan accordingly.
- **If a task fails.** Stop, surface the error, do not skip ahead. The plan is sequential and downstream tasks assume upstream cells are healthy.
