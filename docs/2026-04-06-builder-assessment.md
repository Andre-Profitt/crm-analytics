# Builder Assessment — 2026-04-06

> **Status:** read-only assessment of the 8 Python builders that produce the KPI datasets feeding the Sales Director Monthly + Sales Ops Quarterly decks. Captured to inform a separate builder modernization brainstorm.
>
> **Authorization:** the user has a hard rule against touching `build_*.py` files. This assessment was explicitly authorized as a one-time read-only exception. No builder was edited or executed.
>
> **Related artifacts in this session:**
>
> - [`docs/adr/ADR-0001-kpi-reports-data-backbone.md`](adr/ADR-0001-kpi-reports-data-backbone.md) — CRMA primary, SF reports as link layer
> - [`docs/superpowers/specs/2026-04-06-kpi-reports-refresh-design.md`](superpowers/specs/2026-04-06-kpi-reports-refresh-design.md) — refresh design
> - [`docs/superpowers/plans/2026-04-06-kpi-reports-refresh.md`](superpowers/plans/2026-04-06-kpi-reports-refresh.md) — refresh implementation plan (paused at builder execution)

## Summary verdict

The 8 builders are **functional, schema-safe today, and SimCorp-correct on field names**, but they are **not "modern best-in-class"**: stdlib-only single-file scripts of 750-3,100 lines each with `print`-based logging, no tests, no per-run JSON summaries, several monster functions, and at least 3 real correctness bugs around SimCorp fiscal-quarter labeling.

**Recommended action: defer all execution until modernization is planned and the 2 fiscal-quarter bugs are fixed.**

## File roster (with naming surprises)

| #   | Dataset                                       | Builder file                                                   | Lines |
| --- | --------------------------------------------- | -------------------------------------------------------------- | ----- |
| 1   | `Pipeline_Opportunity_Operations`             | `build_pipeline_opportunity_operations.py`                     | 2,313 |
| 2   | `Forecast_Revenue_Motions`                    | `build_forecast_revenue_motions.py`                            | 3,110 |
| 3   | `Revenue_Retention_Health`                    | `build_revenue_retention_health.py`                            | 1,537 |
| 4   | `Executive_Revenue_Source_Truth`              | `scripts/build_source_truth_executive_revenue.py` ⚠️ note path | 747   |
| 5   | `Account_Intelligence` (+ `Contact_Coverage`) | `build_account_intelligence.py`                                | 2,099 |
| 6   | `Customer_Account_Health`                     | `build_customer_account_health.py`                             | 1,981 |
| 7   | `Commercial_Rhythm_Control_Tower`             | `build_commercial_rhythm_control_tower.py`                     | 847   |
| 8   | `Forecast_Intelligence`                       | `build_forecasting.py` ⚠️ note name                            | 1,209 |

All 8 share `crm_analytics_helpers.py` (2,517 lines) which provides auth, SOQL pagination, retry/backoff, and the chunked External Data API uploader.

## Per-builder verdicts

| #   | File                                              | Verdict                | Wall time | Top concerns                                                                                                                                |
| --- | ------------------------------------------------- | ---------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `build_pipeline_opportunity_operations.py`        | SAFE_TO_RUN            | ~60s      | 773-line `create_dataset` (line 355-1128); dead `legacy_*` functions at lines 1128/1382/1614                                                |
| 2   | `build_forecast_revenue_motions.py`               | RUN_WITH_PATCHES       | ~120s     | **Fiscal-quarter bug at line 157-163** (`_quarter_from_month_key` uses calendar quarters); three monster functions of 735/839/1006 lines    |
| 3   | `build_revenue_retention_health.py`               | SAFE_TO_RUN (degraded) | ~60s      | Line 23: hardcoded `/Users/test/crm-analytics`; lines 110-123: custom `run_soql()` lacks the shared retry layer (only builder missing it)   |
| 4   | `scripts/build_source_truth_executive_revenue.py` | RUN_WITH_PATCHES       | ~30s      | **Fiscal-quarter bug at line 169** + **wrong-FY month-date stamp at line 168**; line 767: `create_dataset()` return value discarded         |
| 5   | `build_account_intelligence.py`                   | SAFE_TO_RUN            | ~45s      | No type hints anywhere; 724-line `build_widgets`                                                                                            |
| 6   | `build_customer_account_health.py`                | SAFE_TO_RUN            | ~90s      | **Duplicate `build_steps` at lines 1396 + 1803** (Python uses last-one-wins, first is dead code); ~1050-line `create_dataset`               |
| 7   | `build_commercial_rhythm_control_tower.py`        | SAFE_TO_RUN            | ~30s      | Cleanest of the 8; no blocking concerns                                                                                                     |
| 8   | `build_forecasting.py`                            | SAFE_TO_RUN            | ~30s      | Line 116: `LIMIT 1000` on `ForecastingItem` (silent truncation); main() prints + returns instead of raising — silent failure for schedulers |

## SimCorp fiscal-quarter bugs (the 2 RUN_WITH_PATCHES items)

SimCorp fiscal year starts February 1 → SimCorp Q1 = Feb-Apr. Salesforce's `FiscalQuarter` SOQL field already accounts for this.

**Reading `FiscalQuarter` from SOQL is correct. Computing your own from `CloseDate.month` with `((month - 1) // 3) + 1` is wrong** — that gives calendar quarters.

### Bug 1 — `build_forecast_revenue_motions.py:157-163`

```python
def _quarter_from_month_key(month_key: str) -> str:
    ...
    quarter_num = ((month_num - 1) // 3) + 1   # WRONG: calendar Q
    ...
```

Affects the trend record-type rows generated at line 700 → flows into `Forecast_Revenue_Motions` `record_type=trend` rows. **Per-opp rows are unaffected** (they read `FiscalQuarter` from SOQL directly).

### Bug 2 — `scripts/build_source_truth_executive_revenue.py:168-169`

```python
month_date = f"{CURRENT_FY:04d}-{close_dt.month:02d}-01"  # WRONG: stamps current FY on prior-FY rows
close_quarter = f"Q{((close_dt.month - 1) // 3) + 1}"     # WRONG: calendar Q
```

Both bugs flow into every row in `Executive_Revenue_Source_Truth`. Affects KPIs that group by `CloseQuarter` or chart by month.

## Schema robustness against the recently deleted fields

**Verified clean.** Grepped all 8 builders for the 7 fields that broke the data sync layer:

- `Competitors__c`, `IPP_Score__c`, `Public_Tender__c`, `Pipeline_Category__c` (Opportunity)
- `Response__c`, `CAMPAIGNCAL__Exclude_from_Calendar__c`, `CAMPAIGNCAL__Exclude_from_Plan_View__c` (Campaign)

**Zero matches** in any of the 8. The Python builder layer bypasses CRMA data sync entirely — they go straight from SOQL to External Data API. So the 13-day-old data sync outage never affected these builders. They're all safe to run today.

## SimCorp custom field name correctness (per `crm-analytics/CLAUDE.md`)

**All 8 use the correct names:**

- ✅ `Reason_Won_Lost__c` (not `Decision_Reason__c`) — used in builders #1, #2
- ✅ `Lost_to_Competitor__r.Name` (not `Competitor__c`) — used in #2
- ✅ `APTS_Primary_Quote_Type__c` (not `Quote_Type__c`) — not referenced in any of the 8 (only in `build_account_history_360.py`, outside this assessment)

## Cross-cutting issues

Things that affect multiple builders. Severity = impact on a future bug, not on today's run.

| #   | Issue                                                                                      | Severity                          | Affected files                                |
| --- | ------------------------------------------------------------------------------------------ | --------------------------------- | --------------------------------------------- |
| 1   | No `logging` — all use `print()`, no timestamps, no levels                                 | Medium                            | All 8 + helpers.py                            |
| 2   | No per-run JSON summary — no row counts, runtime, dataset version IDs persisted            | Medium                            | All 8                                         |
| 3   | Stringly-typed SOQL field references everywhere — next field deletion crashes the builder  | **High**                          | All 8                                         |
| 4   | Monster `create_dataset` / `build_widgets` functions (600-1000+ lines)                     | High (maintenance)                | #1, #2, #3, #5, #6                            |
| 5   | Dead-code duplication of dashboard builders                                                | Medium (confusion)                | #1 (`legacy_*`), #6 (duplicate `build_steps`) |
| 6   | No retry layer in `build_revenue_retention_health.py` (custom `run_soql()`)                | Medium                            | #3 only                                       |
| 7   | No tests for any of the 8 builders                                                         | Medium                            | All 8                                         |
| 8   | Hardcoded absolute path `/Users/test/crm-analytics`                                        | Low                               | #3 only                                       |
| 9   | No token refresh — single auth at `main()` start; multi-pull builders risk session timeout | Low (today) / Medium (large orgs) | #6 most affected (4 SOQL pulls)               |
| 10  | stdlib `urllib` instead of `requests.Session` — no connection pooling, no nicer errors     | Low                               | All 8 + helpers.py                            |

## Builder dependency / run order

**The 8 builders are independent.** None reads another builder's dataset as a join source. They all pull straight from the live org. They could run in parallel up to API rate limits (recommend max 3 concurrent).

Suggested order if running serially (low-risk first, high-cost last):

1. `build_commercial_rhythm_control_tower.py` — ~30s
2. `build_forecasting.py` — ~30s
3. `scripts/build_source_truth_executive_revenue.py` — ~30s
4. `build_pipeline_opportunity_operations.py` — ~60s
5. `build_account_intelligence.py` — ~45s
6. `build_revenue_retention_health.py` — ~60s
7. `build_forecast_revenue_motions.py` — ~120s
8. `build_customer_account_health.py` — ~90s

**Sequential total: ~7-9 minutes** (dominated by the 80-attempt × 3-second External Data API status poll loop in `upload_dataset()`).

**API call volume estimate:** ~50-100 SOQL queries + ~15-30 InsightsExternalData operations + ~80-200 status polls. Well within the 100K daily API limit.

## Modernization shopping list (for the future brainstorm)

Roughly in priority order:

1. **Centralize SOQL field references** into a `simcorp_fields.py` constants module with a startup `describe` check that fails fast if the org schema diverges. Eliminates the next deleted-field crash.
2. **Add a shared `RunSummary` dataclass** that every builder writes to `runs/<dataset>/<timestamp>.json` with `row_count`, `byte_count`, `runtime_s`, `dataset_id`, `dataset_version_id`, `errors`. Operator audit trail.
3. **Replace `print()` with `logging.getLogger(__name__)`** and add a default `logging.basicConfig` in helpers. Levels, timestamps, structured.
4. **Fix the 2 fiscal-quarter bugs** (build_forecast_revenue_motions.py:157-163 and build_source_truth_executive_revenue.py:168-169) — small targeted patches.
5. **Decompose the 600+ line monsters** in builders #1, #2, #3, #5, #6 by extracting per-page or per-record-type sub-builders.
6. **Delete dead code** — `legacy_*` in #1 and the duplicate `build_steps` in #6.
7. **Migrate `build_revenue_retention_health.py:110-123`** to the shared `_soql()` helper for retry parity.
8. **Remove the hardcoded `sys.path.insert`** at `build_revenue_retention_health.py:23`.
9. **Add tests** — pure transformer functions (`_stage_band`, `_risk_band`, `_health_score`, `_quarter_from_month_key`, etc.) are easy to extract and unit-test.
10. **Replace `urllib` with `requests.Session`** in helpers.py for connection pooling and better error messages.
11. **Add token refresh** — re-auth on 401 mid-run for long builders.
12. **Wire builders into a `make refresh-kpi-data` target** so the operator runs one command instead of remembering 8 file paths.

## What this assessment did NOT cover

- **Helpers.py modernization** — only spot-checked auth, SOQL, and upload_dataset. The other 1,800 lines were not assessed.
- **Other builders in the repo** — only the 8 KPI-relevant ones. There are 30+ other `build_*.py` files in `/Users/test/crm-analytics/` that produce different datasets.
- **The audit/profile scripts** in `tests/` and `scripts/` that consume these datasets after upload.
- **End-to-end correctness** — the assessment looked at code, not at whether the builders' outputs match the deck builders' expectations. A Successful run does not guarantee deck-correct data.
- **Performance under SimCorp's actual data volumes** — runtime estimates are inferred from SOQL query shapes, not measured.

## Files inspected

- `/Users/test/crm-analytics/CLAUDE.md`
- `/Users/test/crm-analytics/.gitignore`
- `/Users/test/crm-analytics/crm_analytics_helpers.py` (partial: auth, soql, upload_dataset)
- `/Users/test/crm-analytics/build_pipeline_opportunity_operations.py`
- `/Users/test/crm-analytics/build_forecast_revenue_motions.py`
- `/Users/test/crm-analytics/build_revenue_retention_health.py`
- `/Users/test/crm-analytics/scripts/build_source_truth_executive_revenue.py`
- `/Users/test/crm-analytics/build_account_intelligence.py`
- `/Users/test/crm-analytics/build_customer_account_health.py`
- `/Users/test/crm-analytics/build_commercial_rhythm_control_tower.py`
- `/Users/test/crm-analytics/build_forecasting.py`
- `/Users/test/crm-analytics/tests/test_audit_revenue_retention_health_cli.py` (sample)
