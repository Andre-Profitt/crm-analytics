# Builder Modernization 1A — Plumbing — Design Spec

**Date:** 2026-04-06
**Author:** Andre P. (Senior Rev Intelligence) + Claude
**Status:** Draft, awaiting user review
**Audience:** internal — operator runbook + implementation contract for the first iteration of the KPI dataset builder modernization
**Architectural decision reference:** [ADR-0001 KPI Reports Data Backbone](../../adr/ADR-0001-kpi-reports-data-backbone.md)
**Related artifacts:**

- [`docs/2026-04-06-builder-assessment.md`](../../2026-04-06-builder-assessment.md) — read-only assessment of the 8 KPI dataset producers (the input for this spec)
- [`docs/superpowers/specs/2026-04-06-kpi-reports-refresh-design.md`](2026-04-06-kpi-reports-refresh-design.md) — paused refresh design that is unblocked by this spec
- [`docs/superpowers/plans/2026-04-06-kpi-reports-refresh.md`](../plans/2026-04-06-kpi-reports-refresh.md) — paused refresh implementation plan
- [`crm-analytics/CLAUDE.md`](../../../CLAUDE.md) — repo conventions (CLI-first, no MCP, no Python builders without explicit authorization)

---

## Goal

Modernize the operational plumbing of the 8 Python KPI dataset builders so that future runs are observable, schema-safe, and structurally consistent — without changing what data they produce or how they query the org. Specifically, this iteration:

1. **Centralizes SOQL field references** in a new `simcorp_fields.py` module with a startup describe-check that fails fast on schema drift
2. **Adds a `RunSummary` audit trail** written by every builder to `runs/<dataset>/<timestamp>.json` on both success and failure
3. **Replaces every `print()` with `logging.getLogger(__name__)`** in the 8 builders and in `crm_analytics_helpers.py`

This is **iteration 1A** of a broader builder modernization sequence. The remaining iterations are deliberately split out into separate brainstorms (see Future Work below).

## Authorization for editing build\_\*.py files

The repo CLAUDE.md and Andre's standing feedback memory both prohibit editing `build_*.py` files. **This spec is the explicit authorization to modify all 8 KPI dataset builders for the changes described herein, AND ONLY these changes.** Each builder modification still goes through the implementation plan's per-file approval gate. No other `build_*.py` modifications are authorized by this spec.

## Why now

- The 2026-04-06 KPI reports refresh attempt (`docs/superpowers/specs/2026-04-06-kpi-reports-refresh-design.md`) was paused at builder execution because the 8 dataset producers have a quality gap warranting modernization before unattended runs.
- The 2026-04-03 dataflow outage (4 deleted Opportunity fields silently broke replication for 13 days) demonstrated that schema drift is a real and recurring failure mode. The describe-check pattern in this spec is designed to make that class of failure fail fast and visible.
- The current builders use `print()` for everything, leave no per-run audit trail, and embed SOQL field strings inline — making operator review of refresh runs strictly post-hoc and reactive.
- The Sales Director monthly + Sales Ops quarterly decks need to refresh on a recurring cadence. The plumbing fixed in this spec is a prerequisite for ANY operator-runnable refresh flow.

## Scope

### In scope

- Create new module `simcorp_fields.py` with per-object field constants and `assert_org_schema()` function
- Create new module `crm_analytics_runtime.py` with `RunSummary` dataclass and `builder_run()` context manager
- Add `logging.basicConfig(...)` to `crm_analytics_helpers.py` and replace internal `print()` calls with `logger.*` calls
- Modify all 8 KPI dataset builders to:
  - Import the new modules
  - Declare `logger = logging.getLogger(__name__)`
  - Wrap `main()` body in `with builder_run(...) as summary:`
  - Call `assert_org_schema(...)` after auth
  - Replace every `print()` with `logger.info / warning / error`
  - Populate `summary.row_count`, `summary.dataset_id`, `summary.dataset_version_id`, `summary.byte_count`
- Create `runs/` directory at repo root with `.gitkeep` and `README.md`; gitignore the JSON files
- Add `tests/test_simcorp_fields.py` and `tests/test_crm_analytics_runtime.py` (~20 tests, <1s wall time)
- Pilot the pattern on `build_commercial_rhythm_control_tower.py` first, validate live, then dispatch parallel subagents to apply the same pattern to the remaining 7 builders

### Out of scope

- **Calendar-quarter migration** — the hard switch from SimCorp fiscal Q to calendar Q across all dataset producers. Deferred to **Spec 1B**.
- **Deck label changes + dashboard SAQL cascade** triggered by the calendar-Q switch. Deferred to **Spec 1C**.
- **Salesforce ops dashboard backed by `Builder_Run__c` custom object.** Deferred to **Spec 1D** (separate session).
- Decomposing 600+ line monster functions in builders #1, #2, #3, #5, #6 (item 5 in the assessment shopping list)
- Deleting `legacy_*` dead code in `build_pipeline_opportunity_operations.py` (item 6)
- Migrating `build_revenue_retention_health.py:110-123` from custom `run_soql()` to the shared helper (item 7)
- Removing the hardcoded `sys.path.insert(0, "/Users/test/crm-analytics")` at `build_revenue_retention_health.py:23` (item 8)
- Adding pure-transformer unit tests beyond what's in this spec (item 9)
- Replacing `urllib` with `requests.Session` in helpers.py (item 10)
- Adding token refresh on 401 (item 11)
- A `make refresh-kpi-data` operator target (item 12)

All deferred items remain on the modernization shopping list in `docs/2026-04-06-builder-assessment.md` for **Spec 1E** or later iterations.

## Architecture

```
crm-analytics/
├── crm_analytics_helpers.py        ← MODIFIED: + logging.basicConfig at module load,
│                                     internal print() → logger.* (existing 2,517 lines)
├── simcorp_fields.py               ← NEW: SOQL field constants + describe-check
├── crm_analytics_runtime.py        ← NEW: RunSummary dataclass + writer + ctx mgr
├── build_*.py × 8                  ← MODIFIED: imports + logger + describe call + print→log
│                                     + RunSummary write on both success and failure paths
├── runs/                           ← NEW: per-run JSON audit trail
│   ├── .gitkeep                    ← committed
│   ├── README.md                   ← committed; ops note + retention policy
│   └── <Dataset_Name>/
│       └── <YYYYMMDDTHHMMSSZ>.json ← gitignored
└── tests/
    ├── test_simcorp_fields.py      ← NEW: ~120 LOC, mocks describe responses
    └── test_crm_analytics_runtime.py ← NEW: ~150 LOC, exercises dataclass + writer
```

**No subpackages — keeps the layout consistent with the current top-level-script convention.**

The two new modules are deliberately split:

- `simcorp_fields.py` is _data_ (constants + a schema-validation function). Depends only on `crm_analytics_helpers.py` for the SOQL describe call.
- `crm_analytics_runtime.py` is _behavior_ (a dataclass + a writer + a context manager). Depends on logging stdlib only.

Splitting them keeps each file under ~200 lines, makes the imports legible in the builders, and lets the unit tests stay focused.

The `runs/` directory lives at the repo root (not under `output/`) because it is operational metadata, not deliverables — separating it from `output/` makes the "what do I delete to clean up" answer obvious.

## Components

### `simcorp_fields.py` (NEW)

Pure-data module: per-object field tuples + a single function that validates them against the live org via the SOQL `Describe` REST endpoint.

```python
"""SimCorp SOQL field constants + startup describe-check.

Centralizes the field name strings that the 8 KPI builders pull from
Salesforce. Lets a single field deletion fail fast at startup with a
clear message instead of crashing mid-builder with a SOQL error.
"""
from __future__ import annotations
import logging
from typing import Iterable

logger = logging.getLogger(__name__)

OPPORTUNITY_FIELDS: tuple[str, ...] = (
    "Id", "Name", "Amount", "CloseDate", "StageName", "OwnerId",
    "AccountId", "ForecastCategoryName", "FiscalQuarter", "FiscalYear",
    "Reason_Won_Lost__c", "Lost_to_Competitor__c",
    "APTS_Primary_Quote_Type__c",
    # ... full list extracted from the 8 builders during implementation Phase 1
)
ACCOUNT_FIELDS: tuple[str, ...] = (...)
USER_FIELDS: tuple[str, ...] = (...)
CAMPAIGN_FIELDS: tuple[str, ...] = (...)
OPPORTUNITY_LINE_ITEM_FIELDS: tuple[str, ...] = (...)
FORECASTING_ITEM_FIELDS: tuple[str, ...] = (...)

SCHEMA: dict[str, tuple[str, ...]] = {
    "Opportunity": OPPORTUNITY_FIELDS,
    "Account": ACCOUNT_FIELDS,
    "User": USER_FIELDS,
    "Campaign": CAMPAIGN_FIELDS,
    "OpportunityLineItem": OPPORTUNITY_LINE_ITEM_FIELDS,
    "ForecastingItem": FORECASTING_ITEM_FIELDS,
}


class SchemaDriftError(RuntimeError):
    """Raised when the live org is missing fields the builders depend on."""


def _describe_object(instance_url: str, access_token: str, obj: str) -> dict:
    """GET /services/data/v66.0/sobjects/<obj>/describe via requests."""
    ...


def assert_org_schema(
    instance_url: str,
    access_token: str,
    objects: Iterable[str] | None = None,
) -> None:
    """For each object in `objects` (default: all keys in SCHEMA), call
    /sobjects/<obj>/describe and confirm every field in SCHEMA[obj]
    exists in the org. Raise SchemaDriftError listing every missing
    field if any are gone, with a message that names the constant
    tuple to edit.
    """
    ...
```

**Why a constants module and not a YAML/JSON config:** type-checked imports (typos become import errors, not runtime errors), editor autocomplete, single source of truth for SOQL formatter strings.

**Why include `objects=` parameter:** lets each builder scope the check to only the objects it queries, keeping the describe-check fast (~200ms per object).

### `crm_analytics_runtime.py` (NEW)

Owns the `RunSummary` dataclass, the JSON writer, and a context manager that wraps a builder run so the summary is written even when the builder raises.

```python
"""Per-run audit trail for CRM Analytics builders.

Every builder writes one JSON file per run to runs/<Dataset>/<ts>.json
capturing what ran, how long, what got uploaded, and any errors.
"""
from __future__ import annotations
import hashlib
import json
import logging
import socket
import time
import traceback
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

RUNS_ROOT = Path(__file__).parent / "runs"


@dataclass
class RunSummary:
    dataset_name: str
    builder_path: str
    started_at: str                              # ISO 8601 UTC
    summary_schema_version: int = 1
    external_id: str = ""                        # populated in __post_init__
    finished_at: str | None = None
    runtime_s: float | None = None
    row_count: int | None = None
    byte_count: int | None = None
    dataset_id: str | None = None
    dataset_version_id: str | None = None
    status: str = "running"                      # "running" | "ok" | "failed"
    errors: list[str] = field(default_factory=list)
    host: str = field(default_factory=socket.gethostname)

    def __post_init__(self) -> None:
        if not self.external_id:
            key = f"{self.dataset_name}|{self.started_at}".encode()
            self.external_id = hashlib.sha256(key).hexdigest()[:18]

    def to_json_path(self) -> Path:
        ts = self.started_at.replace(":", "").replace("-", "")
        return RUNS_ROOT / self.dataset_name / f"{ts}.json"

    def write(self) -> Path:
        path = self.to_json_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2, sort_keys=True))
        return path


@contextmanager
def builder_run(dataset_name: str, builder_path: str) -> Iterator[RunSummary]:
    """Wrap main() so the RunSummary is written on success AND failure.

    Contract: if summary.write() itself fails (disk full, permission),
    the original exception (if any) is still the one that propagates.
    """
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary = RunSummary(
        dataset_name=dataset_name,
        builder_path=builder_path,
        started_at=started,
    )
    t0 = time.monotonic()
    body_exc: Exception | None = None
    try:
        yield summary
        summary.status = "ok"
    except Exception as exc:
        body_exc = exc
        summary.status = "failed"
        summary.errors.append(f"{type(exc).__name__}: {exc}")
        summary.errors.append(traceback.format_exc())
    finally:
        summary.finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        summary.runtime_s = round(time.monotonic() - t0, 2)
        try:
            path = summary.write()
            logger.info(
                "RunSummary written: %s status=%s runtime=%.1fs rows=%s",
                path, summary.status, summary.runtime_s, summary.row_count,
            )
        except OSError as write_exc:
            logger.error("RunSummary write failed: %s", write_exc)
            # do not mask the body exception
        if body_exc is not None:
            raise body_exc
```

### `crm_analytics_helpers.py` (MODIFIED)

Two changes only:

1. **Module-level logging config** at the top of the file (after imports):

   ```python
   import logging
   import os

   logging.basicConfig(
       level=os.environ.get("LOG_LEVEL", "INFO"),
       format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
       datefmt="%Y-%m-%dT%H:%M:%S",
   )
   logger = logging.getLogger(__name__)
   ```

   At module load (not inside a function) so any builder that imports `crm_analytics_helpers` automatically gets logging configured. Standard library convention warns against `basicConfig` in libraries, but `crm_analytics_helpers.py` is the _de facto_ runtime entry point for these builders, so it's the correct place.

2. **Internal `print()` → `logger.*`** — every `print()` call inside `crm_analytics_helpers.py` becomes a `logger.info / warning / error` call. **No public API changes. Function signatures unchanged.**

### `runs/README.md` (NEW)

~30 lines. Explains the directory's purpose, the per-dataset subdirectory layout, the JSON shape, and a one-liner retention policy ("keep last 30 days; older than that get deleted manually"). Gets committed; the JSON files themselves are gitignored.

### The 8 builder files (MODIFIED)

For each `build_*.py`, the diff has 6 mechanical components:

| #   | Change                                                                   | Approx LOC delta       |
| --- | ------------------------------------------------------------------------ | ---------------------- |
| 1   | Add 4-5 imports (`logging`, `simcorp_fields`, `crm_analytics_runtime`)   | +5                     |
| 2   | Add `logger = logging.getLogger(__name__)` after imports                 | +1                     |
| 3   | Wrap `main()` body in `with builder_run(...) as summary:`                | indent + ~5 line moves |
| 4   | Replace every `print(...)` with `logger.info / warning / error(...)`     | ~20-80 per file        |
| 5   | Add `assert_org_schema(...)` call after auth                             | +1                     |
| 6   | Populate `summary.row_count`, `summary.dataset_id`, etc. as builder runs | +4-6                   |

**Net diff per builder: ~30-100 lines.** The 8 builders together total ~14,000 lines, so the modernization touches roughly 2-5% of the codebase.

## Data Flow

### Single builder run (success path)

1. **Module load** — `import crm_analytics_helpers` triggers `logging.basicConfig(...)`. Logging is now configured for the entire process.
2. **Enter `main()`** → enters `with builder_run("Commercial_Rhythm_Control_Tower", __file__) as summary:`. Captures `started_at`, instantiates `RunSummary`, computes `external_id` in `__post_init__`. Status is `"running"`.
3. **Auth** — `instance_url, access_token = auth()` calls `sf org display --target-org apro@simcorp.com --json`.
4. **Schema check** — `assert_org_schema(instance_url, access_token, objects=["Opportunity", "Account"])` hits `/sobjects/<obj>/describe` for each object. Total ~1s.
5. **Data extraction** — Builder runs its existing SOQL queries (now using `simcorp_fields.OPPORTUNITY_FIELDS` for the SELECT list). No semantic change to the queries.
6. **Transformation** — Pure-Python row mapping. Unchanged.
7. **Upload** — `result = helpers.upload_dataset(rows, "Commercial_Rhythm_Control_Tower")`. Builder populates `summary.row_count`, `summary.dataset_id`, `summary.dataset_version_id`, `summary.byte_count`.
8. **Exit context** — `__exit__` sets status `"ok"`, finalizes runtime, writes `runs/Commercial_Rhythm_Control_Tower/<ts>.json`, logs the write.
9. **Process exits 0.**

### Sample log output (success run)

```
2026-04-06T15:47:20 INFO     crm_analytics_helpers: Auth OK for apro@simcorp.com
2026-04-06T15:47:20 INFO     simcorp_fields: Schema check Opportunity (104 fields)
2026-04-06T15:47:21 INFO     simcorp_fields: Schema check Account (38 fields)
2026-04-06T15:47:21 INFO     simcorp_fields: All required fields present
2026-04-06T15:47:21 INFO     __main__: Fetching Commercial Rhythm rows
2026-04-06T15:47:33 INFO     crm_analytics_helpers: SOQL returned 4123 rows in 11.4s
2026-04-06T15:47:33 INFO     __main__: Transforming 4123 rows → dataset
2026-04-06T15:47:34 INFO     crm_analytics_helpers: Upload started (142853 bytes, 5 chunks)
2026-04-06T15:47:47 INFO     crm_analytics_helpers: Upload complete dataset_version_id=0Fc57000000abc1
2026-04-06T15:47:48 INFO     crm_analytics_runtime: RunSummary written: runs/Commercial_Rhythm_Control_Tower/20260406T154720Z.json status=ok runtime=27.8s rows=4123
```

### Sample success-run JSON

```json
{
  "builder_path": "build_commercial_rhythm_control_tower.py",
  "byte_count": 142853,
  "dataset_id": "0Fb57000000abcAAA",
  "dataset_name": "Commercial_Rhythm_Control_Tower",
  "dataset_version_id": "0Fc57000000abc1",
  "errors": [],
  "external_id": "a7f3c8e1d29b40e7c2",
  "finished_at": "2026-04-06T15:47:48Z",
  "host": "Andres-MacBook-Pro.local",
  "row_count": 4123,
  "runtime_s": 27.8,
  "started_at": "2026-04-06T15:47:20Z",
  "status": "ok",
  "summary_schema_version": 1
}
```

### Sample failed-run JSON

```json
{
  "builder_path": "build_pipeline_opportunity_operations.py",
  "byte_count": null,
  "dataset_id": null,
  "dataset_name": "Pipeline_Opportunity_Operations",
  "dataset_version_id": null,
  "errors": [
    "SchemaDriftError: Opportunity is missing 1 field referenced by simcorp_fields.OPPORTUNITY_FIELDS:\n  - Old_Custom_Field__c\n\nEither add it back to the org, or remove it from OPPORTUNITY_FIELDS.",
    "Traceback (most recent call last):\n  File \"build_pipeline_opportunity_operations.py\", line 142, in main\n    assert_org_schema(...)\n  File \"simcorp_fields.py\", line 87, in assert_org_schema\n    raise SchemaDriftError(...)\nsimcorp_fields.SchemaDriftError: ..."
  ],
  "external_id": "f29bd4a1c8e30b9217",
  "finished_at": "2026-04-06T15:48:02Z",
  "host": "Andres-MacBook-Pro.local",
  "row_count": null,
  "runtime_s": 0.9,
  "started_at": "2026-04-06T15:48:01Z",
  "status": "failed",
  "summary_schema_version": 1
}
```

### Parallel fan-out (Phase 3 of implementation)

In Phase 3, the coordinator dispatches 7 subagents in parallel via the `Agent` tool, one per remaining builder. Each subagent:

1. Reads the pilot's diff (`build_commercial_rhythm_control_tower.py` + the new modules) as the pattern reference
2. Applies the same 6-step mechanical change to its assigned builder
3. Runs `pytest tests/test_simcorp_fields.py tests/test_crm_analytics_runtime.py` (no live deps — fast)
4. Runs its modernized builder live: `python3 build_X.py`
5. Reads the resulting `runs/<Dataset>/<ts>.json`
6. Compares `row_count` vs the prior baseline (from `BASELINES.md` the coordinator prepared in Phase 2)
7. If row count is within ±10% → commits with message including the delta
8. If not → reports without committing and waits for coordinator review

7 subagents × ~30-120s wall time per builder = ~120s total wall time (parallel), bounded by `build_forecast_revenue_motions.py` (~120s).

The 8 resulting `runs/*/*.json` files become the Phase 4 audit table for sign-off.

## Error Handling

### Failure modes inside a single builder run

| Failure                              | Where                                                      | Behavior                                                                       | RunSummary state                                                 | Process exit           |
| ------------------------------------ | ---------------------------------------------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------------------------- | ---------------------- |
| `sf org display` no token            | `auth()` BEFORE `with builder_run`                         | `subprocess.CalledProcessError` propagates                                     | n/a — context never entered                                      | non-zero               |
| Schema drift                         | `assert_org_schema()` INSIDE context                       | `SchemaDriftError` → context catches, status `failed`, JSON written, re-raised | `status="failed"`, `row_count=null`, `errors=[msg, traceback]`   | non-zero               |
| SOQL HTTPError                       | `helpers.run_soql()` (existing 3× retry layer fires first) | After retries exhaust, `requests.HTTPError` propagates                         | `status="failed"`, `row_count=null`                              | non-zero               |
| Transformation raises                | Pure-Python row mapping                                    | KeyError/ValueError propagates                                                 | `status="failed"`, `row_count` may be set                        | non-zero               |
| `upload_dataset` fails               | `helpers.upload_dataset()` (3× chunked retry)              | After retries, raises                                                          | `status="failed"`, `row_count=<live>`, `dataset_version_id=null` | non-zero               |
| Upload status poll timeout           | helpers' 80×3s poll loop                                   | Returns `{"status": "polling_timeout"}`                                        | `status="failed"`, `errors=["polling_timeout after 240s"]`       | non-zero               |
| RunSummary write fails (body OK)     | `summary.write()` in `finally`                             | `OSError` caught, logged at `error`, swallowed — exit 0                        | n/a — JSON missing                                               | zero                   |
| RunSummary write fails (body raised) | `summary.write()` in `finally`                             | `OSError` caught, logged at `error`, body exception re-raised unchanged        | n/a — JSON missing                                               | matches body exception |

**Key contracts:**

1. **RunSummary write failures must never suppress the body exception.** The `finally` block catches `OSError` from `write()`, logs it at ERROR, then re-raises the body exception unchanged. If there was no body exception, the write failure is logged and the process exits 0 — a local disk issue must not convert a successful data upload into a failure report.
2. **Body exception takes priority over write exception.** Python would naturally chain them via `__context__`; we avoid that by catching the `OSError` explicitly and not re-raising it when a body exception is in flight.

### Logging error contract

| Level     | Used for                                                                       |
| --------- | ------------------------------------------------------------------------------ |
| `DEBUG`   | Off by default. Per-row transformation details. Enabled via `LOG_LEVEL=DEBUG`. |
| `INFO`    | Normal happy-path progress. Replaces every existing `print()`.                 |
| `WARNING` | Recoverable degradation: SOQL retry fired, partial result returned.            |
| `ERROR`   | About-to-fail or already-failed; always paired with the exception.             |

### Schema drift specifics

`SchemaDriftError` is intentionally a hard failure. Justification:

- The 2026-04-03 outage (4 deleted Opportunity fields) was exactly this class and took 13 days to surface because the dataflows kept silently retrying.
- Schema drift means the builder's output is _wrong_ (missing columns), not just degraded.
- Cost of false positives is low (update the constants in the same PR); cost of false negatives is multi-week silent corruption.

The error message names the exact constant tuple to edit:

```
SchemaDriftError: Opportunity is missing 1 field referenced by
simcorp_fields.OPPORTUNITY_FIELDS:
  - Old_Custom_Field__c

Either add it back to the org, or remove it from OPPORTUNITY_FIELDS.
```

### Failure modes in parallel fan-out (Phase 3)

| Failure                                                  | Behavior                                                                                                                                                                                                                                          |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Subagent's modernized builder exits non-zero on live run | Subagent does NOT commit. Reports failure (RunSummary JSON path + last 50 log lines) to coordinator.                                                                                                                                              |
| Row count delta > ±10%                                   | Same — does not commit, reports to coordinator.                                                                                                                                                                                                   |
| Tests fail                                               | Subagent does not commit. Reports failing tests + modified files.                                                                                                                                                                                 |
| Subagent timeout/hang                                    | Coordinator's `Agent` tool timeout fires; restart that single subagent or take work back into main session.                                                                                                                                       |
| Two subagents race on a shared file                      | Should not happen — pilot commits both new modules in Phase 2 BEFORE fan-out. Subagents only edit their assigned `build_X.py`. The coordinator's prompt to each subagent will explicitly forbid editing any file other than its assigned builder. |

### Failure modes in the pilot (Phase 2)

- **Fix in place** if mechanical (missing import, wrong field name, logging-format-string crash). Re-run.
- **Revert and re-design** if the issue reveals the pattern is wrong (e.g., context manager doesn't compose with the builder's existing exception handling). Stop, revisit Components.
- **Carve out** if the issue is specific to _that_ builder (unlikely for `commercial_rhythm_control_tower` which is the cleanest). Pick a different pilot.

### What we explicitly do NOT do

- **No automatic retry on schema drift.** Drift is a code-and-org coordination problem, not a transient.
- **No automatic rollback** of dataset uploads on failure. The External Data API doesn't support this cleanly, and the prior dataset version remains queryable until the next successful run.
- **No alerting integration in iter 1.** Failure surfaces are: process exit code, stderr, RunSummary JSON, log file. Alerting → spec 1D.
- **No graceful degradation for partial failures.** Builders today already fail-all-or-succeed-all; we don't add new partial-success paths.

## Testing

### Test layout

```
tests/
├── test_simcorp_fields.py          ← NEW: ~120 LOC, ~8 tests, mocks /sobjects/<X>/describe
├── test_crm_analytics_runtime.py   ← NEW: ~150 LOC, ~12 tests, exercises dataclass + writer + ctx mgr
└── test_audit_revenue_retention_health_cli.py   ← existing, untouched
```

Both new files use stdlib `unittest.mock` (no new pytest plugins). Combined wall time <1s. Zero live-org dependency.

### `test_simcorp_fields.py` test list

1. `test_opportunity_fields_is_tuple_of_unique_strings`
2. `test_schema_dict_covers_every_constant_tuple` — catches "added a new constant tuple but forgot to register it"
3. `test_assert_org_schema_passes_when_all_fields_present` — happy path, mocked describe
4. `test_assert_org_schema_raises_on_single_missing_field`
5. `test_assert_org_schema_lists_all_missing_fields_in_one_error`
6. `test_assert_org_schema_walks_multiple_objects` — confirms loop visits each
7. `test_assert_org_schema_default_objects_is_full_schema` — `objects=None` walks all keys
8. `test_schema_drift_error_message_names_the_constant_tuple` — error format contract from Section 4

### `test_crm_analytics_runtime.py` test list

1. `test_run_summary_external_id_is_deterministic`
2. `test_run_summary_external_id_is_18_chars`
3. `test_run_summary_external_id_differs_per_dataset_or_time`
4. `test_run_summary_default_status_is_running`
5. `test_run_summary_schema_version_default_is_1`
6. `test_to_json_path_strips_colons_and_dashes`
7. `test_write_creates_directory` — uses `tmp_path` fixture
8. `test_written_json_is_parseable_and_sorted`
9. `test_builder_run_success_path` — status=ok, JSON written, runtime_s populated
10. `test_builder_run_failure_path_writes_json_and_reraises` — status=failed, traceback in errors, exception propagates
11. `test_builder_run_write_failure_does_not_mask_original_exception` — the contract from Error Handling section
12. `test_builder_run_populates_finished_at_on_both_paths`

### What is NOT in the test layer

| Concern                                              | Where it's tested instead                 |
| ---------------------------------------------------- | ----------------------------------------- |
| Live `/sobjects/<X>/describe` returns expected shape | Pilot Phase 2 + every subsequent live run |
| `crm_analytics_helpers.upload_dataset()` correctness | Pre-existing behavior, not modified       |
| End-to-end builder correctness                       | Phase 2 pilot run + Phase 3 fan-out runs  |
| Logging output format                                | Manual eyeball during pilot run           |
| 8 modernized builders against actual datasets        | Live runs in Phase 3                      |

### Running the tests

```bash
# From crm-analytics/
python3 -m pytest tests/test_simcorp_fields.py tests/test_crm_analytics_runtime.py -v
```

Will run as part of every subagent's "before commit" gate in Phase 3.

### Test coverage targets

- `simcorp_fields.py`: 100% line coverage on `assert_org_schema()` and `SchemaDriftError` formatting
- `crm_analytics_runtime.py`: 100% line coverage on `RunSummary.__post_init__`, `to_json_path()`, `write()`, and `builder_run()` (both success and failure branches)
- Modified `crm_analytics_helpers.py`: no new tests (mechanical change)
- Modified `build_*.py`: no new tests — the live run in Phases 2/3 IS the test

## Success Criteria

This iteration is complete when ALL of these are true:

1. `simcorp_fields.py` exists, has all per-object constant tuples populated from grepping the 8 builders, and `assert_org_schema()` passes against the live org
2. `crm_analytics_runtime.py` exists with `RunSummary` + `builder_run()` context manager
3. `tests/test_simcorp_fields.py` and `tests/test_crm_analytics_runtime.py` exist with the test lists above and all tests pass
4. `crm_analytics_helpers.py` has `logging.basicConfig` at module load and zero remaining `print()` calls
5. All 8 KPI dataset builders have been modernized per the 6-component mechanical change list
6. All 8 builders have been run live and each produced a `runs/<Dataset>/<ts>.json` with `status="ok"`
7. Each builder's row count is within ±10% of the pre-modernization baseline captured in Phase 2's `BASELINES.md`
8. The Phase 4 audit table shows all 8 builders green
9. `runs/README.md` and `runs/.gitkeep` exist; `runs/*/*.json` is gitignored
10. No `build_*.py` file other than the 8 KPI builders has been touched

## Code Touch List

| File                                              | Action                | Approx LOC                                          |
| ------------------------------------------------- | --------------------- | --------------------------------------------------- |
| `simcorp_fields.py`                               | **NEW**               | ~150                                                |
| `crm_analytics_runtime.py`                        | **NEW**               | ~120                                                |
| `tests/test_simcorp_fields.py`                    | **NEW**               | ~120                                                |
| `tests/test_crm_analytics_runtime.py`             | **NEW**               | ~150                                                |
| `runs/.gitkeep`                                   | **NEW**               | 0                                                   |
| `runs/README.md`                                  | **NEW**               | ~30                                                 |
| `.gitignore`                                      | **MODIFY**            | +1 line (`runs/*/*.json`)                           |
| `crm_analytics_helpers.py`                        | **MODIFY**            | +10 lines (basicConfig), ~50 line edits (print→log) |
| `build_commercial_rhythm_control_tower.py`        | **MODIFY** (pilot)    | ~30-50 lines                                        |
| `build_pipeline_opportunity_operations.py`        | **MODIFY** (subagent) | ~80-100 lines                                       |
| `build_forecast_revenue_motions.py`               | **MODIFY** (subagent) | ~80-100 lines                                       |
| `build_revenue_retention_health.py`               | **MODIFY** (subagent) | ~50-80 lines                                        |
| `scripts/build_source_truth_executive_revenue.py` | **MODIFY** (subagent) | ~30-50 lines                                        |
| `build_account_intelligence.py`                   | **MODIFY** (subagent) | ~50-80 lines                                        |
| `build_customer_account_health.py`                | **MODIFY** (subagent) | ~50-80 lines                                        |
| `build_forecasting.py`                            | **MODIFY** (subagent) | ~30-50 lines                                        |

**Total new LOC: ~570. Total modified LOC: ~470-680. Net diff: ~1,000-1,300 lines across ~16 files.**

## Open Questions / Risks

- **Pilot may surface a context-manager-vs-builder-exception-handling incompatibility.** Mitigation: pilot is on the cleanest builder; revert + revisit Components if it happens.
- **Subagent prompt discipline matters.** Each subagent must be explicitly forbidden from editing any file other than its assigned builder. The coordinator's dispatch prompt must include this constraint verbatim.
- **The describe-check adds ~1s of API calls per builder invocation.** Acceptable for KPI builders that run on a daily/weekly cadence. Would need an opt-out env var if any builder ran sub-minute (none currently do).
- **`logging.basicConfig` in helpers.py is unconventional.** Standard library convention warns against it in libraries, but `crm_analytics_helpers.py` is the de facto runtime entry point and not a library in the conventional sense. Documented inline.
- **The constants tuples are populated by hand from grepping the 8 builders.** A bug in the grep (missed field) means the describe-check passes but a real builder run still fails. Mitigation: the live run in Phases 2/3 will surface any missed fields immediately.
- **Pilot row count delta ±10% may not match production cadence reality.** Some datasets swing more day-to-day than others (e.g., `Forecast_Revenue_Motions` updates daily; `Commercial_Rhythm_Control_Tower` is more stable). If any subagent reports a >±10% delta, the coordinator decides whether to accept (with reasoning in the commit message) or treat as regression.

## What this design does NOT do

- Does not fix the 2 fiscal-quarter bugs (`build_forecast_revenue_motions.py:157-163`, `scripts/build_source_truth_executive_revenue.py:168-169`) — that's part of the calendar-Q migration in **Spec 1B**
- Does not change deck label resolution or dashboard SAQL — that's **Spec 1C**
- Does not upload `RunSummary` records to Salesforce as `Builder_Run__c` rows — that's **Spec 1D** (separate session)
- Does not decompose monster functions, delete dead code, or migrate `urllib`→`requests` — those are **Spec 1E** or later
- Does not introduce a `make refresh-kpi-data` operator target
- Does not push to origin

## Future Work

The broader builder modernization sequence:

| Spec          | Scope                                                                                                                                                                                                                                                                                                                                                                                                  | Status      |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| **1A** (this) | Plumbing: `simcorp_fields.py`, `RunSummary`, logging migration on all 8 builders                                                                                                                                                                                                                                                                                                                       | Draft       |
| **1B**        | Hard calendar-Q switch in builders: replace `FiscalQuarter` reads + computed-Q sites with calendar-Q across all 8 builders. Includes the 2 fiscal-quarter bug fixes from the assessment. Brainstorm pending.                                                                                                                                                                                           | Not started |
| **1C**        | Cascade from 1B: deck `quarter_focus` relabel on Sales Director monthly + Sales Ops quarterly, dashboard SAQL audit + patch on `0FKTb0000000K5BOAU`, refresh-design-spec supersession, `ADR-0002-calendar-quarter-convention.md`. Brainstorm pending.                                                                                                                                                  | Not started |
| **1D**        | Salesforce ops dashboard backed by `Builder_Run__c` custom object. Loader script globs `runs/**/*.json` and POSTs to `/sobjects/Builder_Run__c`. Native SF dashboard with freshness strip + runtime trend + row-count trend + failure log. May reuse an existing custom object in `~/code/apps/salesforce-api/` rather than creating a new one (avoids the Metadata API constraint). Separate session. | Not started |
| **1E**        | Items 5-12 from the assessment shopping list: monster-function decomposition, dead code deletion, retention_health helper migration, hardcoded sys.path removal, urllib→requests, token refresh, `make refresh-kpi-data` target, pure-transformer unit tests                                                                                                                                           | Not started |

The `RunSummary` field shape in this spec was intentionally chosen to be backbone-portable: scalar measures, no nested objects, errors-as-joined-string, stable filename format, deterministic 18-char `external_id`. Spec 1D's loader can write to either CRMA or Salesforce without schema changes.

## References

### Repo artifacts

- [`docs/2026-04-06-builder-assessment.md`](../../2026-04-06-builder-assessment.md) — read-only assessment of the 8 KPI dataset builders
- [`docs/adr/ADR-0001-kpi-reports-data-backbone.md`](../../adr/ADR-0001-kpi-reports-data-backbone.md) — CRMA primary, SF reports as link layer for KPI reports
- [`docs/superpowers/specs/2026-04-06-kpi-reports-refresh-design.md`](2026-04-06-kpi-reports-refresh-design.md) — paused refresh design unblocked by this spec
- [`docs/superpowers/plans/2026-04-06-kpi-reports-refresh.md`](../plans/2026-04-06-kpi-reports-refresh.md) — paused refresh implementation plan
- [`crm-analytics/CLAUDE.md`](../../../CLAUDE.md) — repo conventions
- `~/.claude/projects/-Users-test/memory/feedback_no_python_builders.md` — Andre's hard rule about builder files
- `~/crm-analytics/docs/2026-04-06-next-session-handoff.md` — handoff doc that triggered this brainstorm

### Org details

- Target org: `apro@simcorp.com`
- Instance: `simcorp.my.salesforce.com`
- API version: v66.0
- Sales Ops Quarterly dashboard ID: `0FKTb0000000K5BOAU`
- 8 KPI datasets: `Pipeline_Opportunity_Operations`, `Forecast_Revenue_Motions`, `Revenue_Retention_Health`, `Executive_Revenue_Source_Truth`, `Account_Intelligence`, `Customer_Account_Health`, `Commercial_Rhythm_Control_Tower`, `Forecast_Intelligence`

### External

- Salesforce Describe API reference (`/sobjects/<obj>/describe`)
- Python `dataclasses` and `contextlib.contextmanager` standard library reference
- pytest `monkeypatch` and `tmp_path` fixtures
