# Builder Modernization 1A — Plumbing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modernize the operational plumbing of the 8 KPI dataset builders — centralized SOQL field constants with a startup describe-check, a `RunSummary` audit trail, and `print()` → `logging` migration — without changing what data they produce.

**Architecture:** Two new top-level modules (`simcorp_fields.py`, `crm_analytics_runtime.py`) plus a `runs/` directory for per-run audit JSONs. Pilot on the cleanest builder (`build_commercial_rhythm_control_tower.py`), then dispatch 7 parallel subagents to apply the same pattern to the remaining 7 builders. All new code is TDD (tests first). Live describe-check replaces unit-testing the HTTP layer.

**Tech Stack:** Python 3.13 (`dataclasses`, `contextlib`, `logging`, `hashlib`, `pathlib` stdlib + `requests`), `pytest` with `unittest.mock.monkeypatch` + `tmp_path` fixtures, `sf` CLI for auth, Wave Describe REST API (`/services/data/v66.0/sobjects/<obj>/describe`), Wave Query API for baseline row counts.

**Spec:** [`docs/superpowers/specs/2026-04-06-builder-modernization-1a-plumbing-design.md`](../specs/2026-04-06-builder-modernization-1a-plumbing-design.md)

**Out of scope (deferred):** calendar-Q migration → Spec 1B. Deck/dashboard cascade → Spec 1C. SF ops dashboard → Spec 1D. Assessment items 5-12 → Spec 1E.

---

## File Map

| File                                                      | Action            | Responsibility                                                                      |
| --------------------------------------------------------- | ----------------- | ----------------------------------------------------------------------------------- |
| `simcorp_fields.py`                                       | Create            | Per-object SOQL field constant tuples + `assert_org_schema()` describe-check        |
| `crm_analytics_runtime.py`                                | Create            | `RunSummary` dataclass + `builder_run()` context manager + JSON writer              |
| `tests/test_simcorp_fields.py`                            | Create            | 8 unit tests for field-constants module (mocked describe)                           |
| `tests/test_crm_analytics_runtime.py`                     | Create            | 12 unit tests for runtime module (tmp_path fixtures)                                |
| `runs/.gitkeep`                                           | Create            | Keeps the runs dir in git                                                           |
| `runs/README.md`                                          | Create            | Operator note explaining per-dataset subdirs + retention policy                     |
| `.gitignore`                                              | Modify            | Add `runs/*/*.json` so per-run audit JSONs don't get committed                      |
| `crm_analytics_helpers.py`                                | Modify            | + `logging.basicConfig` at module load; internal `print()` → `logger.*`             |
| `build_commercial_rhythm_control_tower.py`                | Modify (pilot)    | Imports + `logger` + `assert_org_schema` + `builder_run` ctx + `print` → `logger`   |
| `build_pipeline_opportunity_operations.py`                | Modify (subagent) | Same 6-step mechanical change as pilot                                              |
| `build_forecast_revenue_motions.py`                       | Modify (subagent) | Same                                                                                |
| `build_revenue_retention_health.py`                       | Modify (subagent) | Same                                                                                |
| `scripts/build_source_truth_executive_revenue.py`         | Modify (subagent) | Same                                                                                |
| `build_account_intelligence.py`                           | Modify (subagent) | Same                                                                                |
| `build_customer_account_health.py`                        | Modify (subagent) | Same                                                                                |
| `build_forecasting.py`                                    | Modify (subagent) | Same                                                                                |
| `docs/2026-04-06-builder-modernization-1a-baselines.md`   | Create at runtime | Pre-modernization row counts per dataset, used by pilot + subagents for delta check |
| `docs/2026-04-06-builder-modernization-1a-audit-table.md` | Create at runtime | Phase 7 final audit table aggregating all 8 RunSummaries                            |

All paths are relative to `/Users/test/crm-analytics/` unless otherwise noted.

---

## Phase 0: Preflight Verification

### Task 0: Verify environment is sane before touching anything

**Files:** read-only checks

- [ ] **Step 0.1: Confirm `sf` CLI is authenticated against the SimCorp org**

Run:

```bash
sf org display --target-org apro@simcorp.com --json | python3 -c "import json,sys; d=json.load(sys.stdin); print('OK' if d.get('result',{}).get('accessToken') else 'NO TOKEN')"
```

Expected: `OK`
If `NO TOKEN`: stop and run `sf org login web --instance-url https://simcorp.my.salesforce.com --alias apro@simcorp.com`, then re-run this step.

- [ ] **Step 0.2: Confirm Python 3.13 and pytest are available**

Run:

```bash
python3 --version && python3 -m pytest --version
```

Expected: `Python 3.13.x` and a pytest version string.
If pytest missing: `python3 -m pip install pytest`.

- [ ] **Step 0.3: Confirm `requests` is importable**

Run:

```bash
python3 -c "import requests; print(requests.__version__)"
```

Expected: a version string (any ≥ 2.0).
If missing: `python3 -m pip install requests`.

- [ ] **Step 0.4: Confirm working tree is in the expected state**

Run:

```bash
cd /Users/test/crm-analytics && git status --short | wc -l && git log --oneline -3
```

Expected: non-zero line count (user has WIP files — DO NOT touch them) and the most recent commit is `95bee24 spec: builder modernization 1A — plumbing design` (or a later commit from this plan).
If the top commit is something else, stop and reconcile with the user before proceeding.

- [ ] **Step 0.5: Confirm the 8 target builder files exist**

Run:

```bash
for f in \
  build_commercial_rhythm_control_tower.py \
  build_pipeline_opportunity_operations.py \
  build_forecast_revenue_motions.py \
  build_revenue_retention_health.py \
  scripts/build_source_truth_executive_revenue.py \
  build_account_intelligence.py \
  build_customer_account_health.py \
  build_forecasting.py; do
  test -f "/Users/test/crm-analytics/$f" && echo "OK $f" || echo "MISSING $f"
done
```

Expected: 8 lines, all `OK`.
If any `MISSING`: stop — the plan cannot proceed with a missing builder.

- [ ] **Step 0.6: Confirm `crm_analytics_helpers.py` exists and has a `print(` call we can later verify we've replaced**

Run:

```bash
test -f /Users/test/crm-analytics/crm_analytics_helpers.py && grep -c "^[^#]*print(" /Users/test/crm-analytics/crm_analytics_helpers.py
```

Expected: a non-zero count (there are `print()` calls to migrate).
Note the count down — Phase 4 will assert the new count is zero.

---

## Phase 1: Capture Pre-Modernization Baselines

### Task 1: Pull current dataset row counts from the live org

**Files:**

- Create: `docs/2026-04-06-builder-modernization-1a-baselines.md`

The pilot and the subagents need pre-modernization row counts to sanity-check their post-modernization runs against (±10% tolerance). We get these counts WITHOUT running any builder — we query the live datasets directly via the Wave Query API.

- [ ] **Step 1.1: Get auth token into shell variables**

Run:

```bash
cd /Users/test/crm-analytics
eval "$(sf org display --target-org apro@simcorp.com --json | python3 -c '
import json, sys
d = json.load(sys.stdin)["result"]
print(f"export SF_INSTANCE={d[\"instanceUrl\"]}")
print(f"export SF_TOKEN={d[\"accessToken\"]}")
')"
echo "${SF_INSTANCE:?}" | head -c 50 && echo "..."
```

Expected: prints the first 50 chars of the instance URL.

- [ ] **Step 1.2: Query row counts for all 8 KPI datasets**

Run (in the same shell with SF_INSTANCE + SF_TOKEN exported):

```bash
python3 <<'PY'
import os, json, requests
instance = os.environ["SF_INSTANCE"]
token = os.environ["SF_TOKEN"]
datasets = [
    "Commercial_Rhythm_Control_Tower",
    "Pipeline_Opportunity_Operations",
    "Forecast_Revenue_Motions",
    "Revenue_Retention_Health",
    "Executive_Revenue_Source_Truth",
    "Account_Intelligence",
    "Customer_Account_Health",
    "Forecast_Intelligence",
]
results = {}
for ds in datasets:
    saql = f'q = load "{ds}"; q = group q by all; q = foreach q generate count() as row_count;'
    r = requests.post(
        f"{instance}/services/data/v66.0/wave/query",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"query": saql},
    )
    if r.status_code == 200:
        records = r.json().get("results", {}).get("records", [])
        results[ds] = records[0]["row_count"] if records else 0
    else:
        results[ds] = f"ERROR {r.status_code}: {r.text[:200]}"
print(json.dumps(results, indent=2))
PY
```

Expected: a JSON object with 8 keys. Each value should be a positive integer (row count).
If any value is an `ERROR ...` string: the dataset may not exist under that exact name, or the token lacks read permission. Stop and resolve before continuing.

Copy the JSON output — Step 1.3 uses it.

- [ ] **Step 1.3: Write the baseline doc**

Create `/Users/test/crm-analytics/docs/2026-04-06-builder-modernization-1a-baselines.md` with this exact structure (filling in the row counts from Step 1.2):

```markdown
# Builder Modernization 1A — Pre-Modernization Baselines

**Captured:** 2026-04-06 (direct dataset row counts via Wave Query API — no builders were executed)
**Purpose:** Sanity-check row counts for post-modernization runs in Phase 5 (pilot) and Phase 6 (subagent fan-out). Each builder's post-run `RunSummary.row_count` must be within ±10% of the baseline below to commit without coordinator review.
**Source of truth:** live `apro@simcorp.com` datasets as of the capture time.
**Note:** These counts reflect the LAST successful builder run before this iteration began (staleness per each dataset is documented in `docs/2026-04-06-builder-assessment.md`). The ±10% tolerance accounts for day-over-day drift between the baseline capture and the post-modernization run.

| Dataset                         | Baseline row count | Builder                                         |
| ------------------------------- | ------------------ | ----------------------------------------------- |
| Commercial_Rhythm_Control_Tower | <N>                | build_commercial_rhythm_control_tower.py        |
| Pipeline_Opportunity_Operations | <N>                | build_pipeline_opportunity_operations.py        |
| Forecast_Revenue_Motions        | <N>                | build_forecast_revenue_motions.py               |
| Revenue_Retention_Health        | <N>                | build_revenue_retention_health.py               |
| Executive_Revenue_Source_Truth  | <N>                | scripts/build_source_truth_executive_revenue.py |
| Account_Intelligence            | <N>                | build_account_intelligence.py                   |
| Customer_Account_Health         | <N>                | build_customer_account_health.py                |
| Forecast_Intelligence           | <N>                | build_forecasting.py                            |

## Delta tolerance rule

A post-modernization run is within tolerance if `abs(new - baseline) / baseline <= 0.10`. If the delta exceeds 10%, the subagent does NOT commit and reports to the coordinator for review. The coordinator may accept a larger delta with reasoning in the commit message (e.g., "Forecast_Revenue_Motions legitimately updated overnight; delta 14% reflects real data drift, not a bug").
```

Replace each `<N>` with the integer from Step 1.2.

- [ ] **Step 1.4: Verify the file is well-formed**

Run:

```bash
cat /Users/test/crm-analytics/docs/2026-04-06-builder-modernization-1a-baselines.md | head -20
```

Expected: the header + first few rows of the table with real numbers.

- [ ] **Step 1.5: Commit**

```bash
cd /Users/test/crm-analytics
git add docs/2026-04-06-builder-modernization-1a-baselines.md
git commit -m "$(cat <<'EOF'
docs: capture pre-modernization row-count baselines for 1A

Live dataset row counts pulled via Wave Query API, used as the ±10%
tolerance reference for the pilot (Phase 5) and parallel fan-out
(Phase 6) of the builder modernization 1A plumbing iteration.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

Expected: commit created; `git status` shows no baselines file remaining.

---

## Phase 2: Enumerate SOQL Fields Per Object

### Task 2: Dispatch a read-only subagent to grep the 8 builders for SOQL field references

**Files:**

- Read only: all 8 builder files + `crm_analytics_helpers.py`
- Create (temporary): `/tmp/field-audit.json`

The `simcorp_fields.py` constants tuples need to mirror the exact field strings the 8 builders currently embed inline in their SOQL queries. Grepping by hand across 14,000 lines is error-prone and context-heavy. Use the Explore subagent to produce the field inventory.

- [ ] **Step 2.1: Dispatch the Explore subagent**

Use the `Agent` tool with `subagent_type=Explore` and this exact prompt:

```
Enumerate every SOQL field reference, per Salesforce object, across
these 8 Python files in /Users/test/crm-analytics/:

1. build_commercial_rhythm_control_tower.py
2. build_pipeline_opportunity_operations.py
3. build_forecast_revenue_motions.py
4. build_revenue_retention_health.py
5. scripts/build_source_truth_executive_revenue.py
6. build_account_intelligence.py
7. build_customer_account_health.py
8. build_forecasting.py

Also include crm_analytics_helpers.py for any SOQL helpers that
hardcode field lists.

For each file:
1. Find every SOQL query string (look for 'SELECT ... FROM <Object>'
   patterns, both triple-quoted and f-string concatenations).
2. Extract the field list for each query.
3. Identify the root Salesforce object (e.g., Opportunity, Account,
   User, Campaign, OpportunityLineItem, ForecastingItem).
4. Ignore relationship traversals (e.g., Account.Name as AccountName) —
   those are not direct Opportunity fields.
5. Ignore aliases (AS x) — just capture the raw field name.

OUTPUT FORMAT: write a single JSON file to /tmp/field-audit.json with
this structure:

{
  "Opportunity": ["Id", "Name", "Amount", ...],
  "Account": ["Id", "Name", ...],
  "User": [...],
  "Campaign": [...],
  "OpportunityLineItem": [...],
  "ForecastingItem": [...]
}

Field lists must be:
- Deduplicated (a set, but written as a sorted JSON array)
- Sorted alphabetically
- Include BOTH standard fields and custom fields (ending in __c)
- Case-preserved as they appear in the SOQL

Return a one-paragraph summary of what you found (total unique fields
per object) in your response body. Do not modify any file other than
/tmp/field-audit.json.
```

Expected: the subagent returns a short summary. `/tmp/field-audit.json` exists with an object per Salesforce SObject and sorted field arrays.

- [ ] **Step 2.2: Verify the audit JSON is parseable and covers expected objects**

Run:

```bash
python3 -c "
import json
d = json.load(open('/tmp/field-audit.json'))
for obj, fields in sorted(d.items()):
    print(f'{obj}: {len(fields)} fields')
assert 'Opportunity' in d, 'Opportunity missing from audit'
assert len(d['Opportunity']) > 20, f'Opportunity field count suspiciously low: {len(d[\"Opportunity\"])}'
print('AUDIT OK')
"
```

Expected: prints field counts for each object + `AUDIT OK`.
If an assertion fails: the subagent missed something obvious. Re-dispatch with a more specific prompt.

---

## Phase 3: Field Constants Module (TDD)

### Task 3: Write the failing test file for `simcorp_fields.py`

**Files:**

- Create: `tests/test_simcorp_fields.py`

- [ ] **Step 3.1: Create the test file with all 8 tests**

Create `/Users/test/crm-analytics/tests/test_simcorp_fields.py` with this exact content:

```python
"""Unit tests for simcorp_fields.py.

All tests mock _describe_object so no live-org calls are made. The
describe-check is tested by verifying the logic that compares the
mocked describe response against SCHEMA.
"""
from __future__ import annotations

import pytest

import simcorp_fields
from simcorp_fields import (
    ACCOUNT_FIELDS,
    OPPORTUNITY_FIELDS,
    SCHEMA,
    SchemaDriftError,
    assert_org_schema,
)


# --- Constants invariants -------------------------------------------------


def test_opportunity_fields_is_tuple_of_unique_strings():
    assert isinstance(OPPORTUNITY_FIELDS, tuple)
    assert len(set(OPPORTUNITY_FIELDS)) == len(OPPORTUNITY_FIELDS)
    assert all(isinstance(f, str) for f in OPPORTUNITY_FIELDS)
    assert len(OPPORTUNITY_FIELDS) > 0


def test_schema_dict_covers_every_constant_tuple():
    # Every constant tuple exported by the module must be registered
    # in SCHEMA. Catches "added a new constant tuple but forgot to
    # register it."
    declared_constants = {
        name: value
        for name, value in vars(simcorp_fields).items()
        if name.endswith("_FIELDS") and isinstance(value, tuple)
    }
    registered_values = set(SCHEMA.values())
    for name, tpl in declared_constants.items():
        assert tpl in registered_values, (
            f"{name} is a *_FIELDS tuple but is not in SCHEMA"
        )


# --- assert_org_schema happy path ----------------------------------------


def test_assert_org_schema_passes_when_all_fields_present(monkeypatch):
    # Mock the describe helper to return every field the constants demand.
    def fake_describe(instance_url, access_token, obj):
        return {f: {} for f in SCHEMA[obj]}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    # Should not raise.
    assert_org_schema("https://x", "tok", objects=["Opportunity"])


# --- assert_org_schema failure paths -------------------------------------


def test_assert_org_schema_raises_on_single_missing_field(monkeypatch):
    missing_field = OPPORTUNITY_FIELDS[0]
    fake_org_fields = set(OPPORTUNITY_FIELDS) - {missing_field}

    def fake_describe(instance_url, access_token, obj):
        return {f: {} for f in fake_org_fields}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    with pytest.raises(SchemaDriftError, match=missing_field):
        assert_org_schema("https://x", "tok", objects=["Opportunity"])


def test_assert_org_schema_lists_all_missing_fields_in_one_error(monkeypatch):
    missing = set(list(OPPORTUNITY_FIELDS)[:2])  # first 2 fields
    fake_org_fields = set(OPPORTUNITY_FIELDS) - missing

    def fake_describe(instance_url, access_token, obj):
        return {f: {} for f in fake_org_fields}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    with pytest.raises(SchemaDriftError) as exc_info:
        assert_org_schema("https://x", "tok", objects=["Opportunity"])
    msg = str(exc_info.value)
    for f in missing:
        assert f in msg, f"Missing field {f} not named in error: {msg}"


def test_assert_org_schema_walks_multiple_objects(monkeypatch):
    seen = []

    def fake_describe(instance_url, access_token, obj):
        seen.append(obj)
        return {f: {} for f in SCHEMA[obj]}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    assert_org_schema("https://x", "tok", objects=["Opportunity", "Account"])
    assert seen == ["Opportunity", "Account"]


def test_assert_org_schema_default_objects_is_full_schema(monkeypatch):
    seen = []

    def fake_describe(instance_url, access_token, obj):
        seen.append(obj)
        return {f: {} for f in SCHEMA[obj]}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    assert_org_schema("https://x", "tok")  # objects=None
    assert set(seen) == set(SCHEMA.keys())


def test_schema_drift_error_message_names_the_constant_tuple(monkeypatch):
    # Contract from Error Handling section 4.3 of the spec: the error
    # message must tell the operator WHICH constant tuple to edit.
    missing_field = OPPORTUNITY_FIELDS[0]
    fake_org_fields = set(OPPORTUNITY_FIELDS) - {missing_field}

    def fake_describe(instance_url, access_token, obj):
        return {f: {} for f in fake_org_fields}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    with pytest.raises(SchemaDriftError) as exc_info:
        assert_org_schema("https://x", "tok", objects=["Opportunity"])
    msg = str(exc_info.value)
    assert "OPPORTUNITY_FIELDS" in msg, (
        f"Error message must name the constant tuple to edit; got: {msg}"
    )
```

- [ ] **Step 3.2: Run the test file — expect ImportError**

Run:

```bash
cd /Users/test/crm-analytics
python3 -m pytest tests/test_simcorp_fields.py -v 2>&1 | head -30
```

Expected: `ModuleNotFoundError: No module named 'simcorp_fields'` or similar.
This confirms the tests exist and fail for the right reason.

---

### Task 4: Create `simcorp_fields.py`

**Files:**

- Create: `simcorp_fields.py`
- Read: `/tmp/field-audit.json` (from Task 2)

- [ ] **Step 4.1: Generate the constants portion from the field audit**

Run this helper to produce the Python literal text for each `*_FIELDS` tuple:

```bash
python3 <<'PY'
import json
audit = json.load(open("/tmp/field-audit.json"))
for obj, fields in sorted(audit.items()):
    const_name = f"{obj.upper()}_FIELDS" if not obj[-1].isupper() else obj.upper() + "_FIELDS"
    # Normalize the constant name: CamelCase → SNAKE_CASE
    import re
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", obj).upper() + "_FIELDS"
    print(f"{snake}: tuple[str, ...] = (")
    for f in sorted(set(fields)):
        print(f'    "{f}",')
    print(")\n")
PY
```

Keep the stdout — you'll paste it into the module in Step 4.2.

- [ ] **Step 4.2: Create `/Users/test/crm-analytics/simcorp_fields.py`**

Create the file with this exact structure (replace the `# PASTE HERE` marker with the Step 4.1 output):

```python
"""SimCorp SOQL field constants + startup describe-check.

Centralizes the field name strings that the 8 KPI builders pull from
Salesforce. Lets a single field deletion fail fast at startup with a
clear message instead of crashing mid-builder with a SOQL error.

Part of Builder Modernization 1A — see
docs/superpowers/specs/2026-04-06-builder-modernization-1a-plumbing-design.md
"""
from __future__ import annotations

import logging
from typing import Iterable

import requests

logger = logging.getLogger(__name__)


# --- Per-object field tuples ---------------------------------------------
# PASTE HERE: the output from Step 4.1, one tuple per Salesforce object.
# Example of what one tuple looks like:
#
# OPPORTUNITY_FIELDS: tuple[str, ...] = (
#     "AccountId",
#     "Amount",
#     "CloseDate",
#     ...
# )


# --- Constant-name → tuple registry --------------------------------------
# Every *_FIELDS tuple declared above must be registered here under the
# exact Salesforce SObject name (not the snake-cased constant name).
SCHEMA: dict[str, tuple[str, ...]] = {
    "Opportunity": OPPORTUNITY_FIELDS,
    "Account": ACCOUNT_FIELDS,
    "User": USER_FIELDS,
    "Campaign": CAMPAIGN_FIELDS,
    "OpportunityLineItem": OPPORTUNITY_LINE_ITEM_FIELDS,
    "ForecastingItem": FORECASTING_ITEM_FIELDS,
}


# --- Constant-name lookup: tuple identity → constant name ----------------
# Used to name the constant tuple in SchemaDriftError messages. Built
# automatically from SCHEMA so the two can't drift.
_TUPLE_TO_CONSTANT_NAME: dict[str, str] = {
    "Opportunity": "OPPORTUNITY_FIELDS",
    "Account": "ACCOUNT_FIELDS",
    "User": "USER_FIELDS",
    "Campaign": "CAMPAIGN_FIELDS",
    "OpportunityLineItem": "OPPORTUNITY_LINE_ITEM_FIELDS",
    "ForecastingItem": "FORECASTING_ITEM_FIELDS",
}


class SchemaDriftError(RuntimeError):
    """Raised when the live org is missing fields the builders depend on."""


def _describe_object(
    instance_url: str, access_token: str, obj: str
) -> dict[str, dict]:
    """GET /services/data/v66.0/sobjects/<obj>/describe and return a
    dict mapping field.name → field metadata. The inner dict contents
    are opaque to this module; only the keys are checked."""
    url = f"{instance_url}/services/data/v66.0/sobjects/{obj}/describe"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    fields = response.json().get("fields", [])
    return {f["name"]: f for f in fields}


def assert_org_schema(
    instance_url: str,
    access_token: str,
    objects: Iterable[str] | None = None,
) -> None:
    """For each object in `objects` (default: all keys in SCHEMA), call
    /sobjects/<obj>/describe and confirm every field in SCHEMA[obj]
    exists in the org.

    Raises SchemaDriftError listing every missing field if any are
    gone, with a message that names the constant tuple to edit.
    """
    target_objects = list(objects) if objects is not None else list(SCHEMA.keys())
    all_missing: list[str] = []

    for obj in target_objects:
        expected = SCHEMA[obj]
        logger.info("Schema check %s (%d fields)", obj, len(expected))
        org_fields = set(_describe_object(instance_url, access_token, obj).keys())
        missing = [f for f in expected if f not in org_fields]
        if missing:
            const_name = _TUPLE_TO_CONSTANT_NAME[obj]
            all_missing.append(
                f"{obj} is missing {len(missing)} field(s) referenced by "
                f"simcorp_fields.{const_name}:\n"
                + "\n".join(f"  - {f}" for f in missing)
                + f"\n\nEither add it back to the org, or remove it from {const_name}."
            )

    if all_missing:
        raise SchemaDriftError("\n\n".join(all_missing))

    logger.info("All required fields present")
```

- [ ] **Step 4.3: Paste the Step 4.1 output into the `# PASTE HERE` region**

Open the file, replace the `# PASTE HERE: ...` comment block with the literal tuples produced in Step 4.1. Make sure every tuple name referenced in `SCHEMA` and `_TUPLE_TO_CONSTANT_NAME` actually exists — if the audit produced a different set of objects than the 6 listed in the scaffold, add or remove rows in both dicts accordingly.

- [ ] **Step 4.4: Verify the module imports cleanly**

Run:

```bash
cd /Users/test/crm-analytics && python3 -c "
import simcorp_fields
print(f'Module loaded: {len(simcorp_fields.SCHEMA)} objects registered')
for obj, fields in sorted(simcorp_fields.SCHEMA.items()):
    print(f'  {obj}: {len(fields)} fields')
"
```

Expected: prints the object count and a summary of each.
If `NameError` on a constant: a tuple referenced by `SCHEMA` doesn't exist — fix the mismatch.

- [ ] **Step 4.5: Run the unit tests — expect all pass**

Run:

```bash
cd /Users/test/crm-analytics && python3 -m pytest tests/test_simcorp_fields.py -v
```

Expected: 8 tests, all PASS.
If any fail: fix and re-run until green.

- [ ] **Step 4.6: Run the describe-check against the live org as a smoke test**

Run:

```bash
cd /Users/test/crm-analytics && python3 <<'PY'
import os, subprocess, json
r = subprocess.run(
    ["sf", "org", "display", "--target-org", "apro@simcorp.com", "--json"],
    capture_output=True, text=True, check=True,
)
d = json.loads(r.stdout)["result"]

import simcorp_fields
simcorp_fields.assert_org_schema(d["instanceUrl"], d["accessToken"])
print("LIVE DESCRIBE CHECK: all fields present")
PY
```

Expected: prints `LIVE DESCRIBE CHECK: all fields present`.
If a `SchemaDriftError` is raised: the audit in Task 2 included a field that doesn't exist in the live org. Either:

- **Remove the missing field from the constant tuple** if the audit over-captured (e.g., a field referenced inside a comment or dead code).
- **Keep it and investigate** — if the field genuinely should exist, something is wrong with the org and this is exactly the class of failure the describe-check is designed to catch.

Re-run until the live check passes.

- [ ] **Step 4.7: Commit**

```bash
cd /Users/test/crm-analytics
git add simcorp_fields.py tests/test_simcorp_fields.py
git commit -m "$(cat <<'EOF'
feat: add simcorp_fields.py with SOQL constants + describe-check

Centralizes SimCorp Salesforce field references for the 8 KPI dataset
builders. assert_org_schema() hits /sobjects/<obj>/describe per object
and raises SchemaDriftError listing missing fields + the constant tuple
to edit. Verified live against apro@simcorp.com.

Part of Builder Modernization 1A plumbing iteration.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 4: Runtime Module (TDD)

### Task 5: Write the failing test file for `crm_analytics_runtime.py`

**Files:**

- Create: `tests/test_crm_analytics_runtime.py`

- [ ] **Step 5.1: Create the test file with all 12 tests**

Create `/Users/test/crm-analytics/tests/test_crm_analytics_runtime.py` with this exact content:

```python
"""Unit tests for crm_analytics_runtime.py.

All tests use tmp_path to isolate the runs/ dir. No live-org calls.
"""
from __future__ import annotations

import json

import pytest

import crm_analytics_runtime
from crm_analytics_runtime import RunSummary, builder_run


# --- Dataclass + __post_init__ -------------------------------------------


def test_run_summary_external_id_is_deterministic():
    a = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    b = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    assert a.external_id == b.external_id


def test_run_summary_external_id_is_18_chars():
    s = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    assert len(s.external_id) == 18
    assert all(c in "0123456789abcdef" for c in s.external_id)


def test_run_summary_external_id_differs_per_dataset_or_time():
    a = RunSummary(
        dataset_name="A", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    b = RunSummary(
        dataset_name="B", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    c = RunSummary(
        dataset_name="A", builder_path="x.py", started_at="2026-04-06T00:00:01Z"
    )
    assert len({a.external_id, b.external_id, c.external_id}) == 3


def test_run_summary_default_status_is_running():
    s = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    assert s.status == "running"


def test_run_summary_schema_version_default_is_1():
    s = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    assert s.summary_schema_version == 1


# --- Filename generation -------------------------------------------------


def test_to_json_path_strips_colons_and_dashes(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    s = RunSummary(
        dataset_name="My_Dataset",
        builder_path="x.py",
        started_at="2026-04-06T15:47:20Z",
    )
    path = s.to_json_path()
    assert path.name == "20260406T154720Z.json"
    assert path.parent.name == "My_Dataset"
    assert path.parent.parent == tmp_path


# --- Writer --------------------------------------------------------------


def test_write_creates_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    s = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    path = s.write()
    assert path.exists()
    assert path.parent.name == "X"


def test_written_json_is_parseable_and_sorted(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    s = RunSummary(
        dataset_name="X",
        builder_path="x.py",
        started_at="2026-04-06T00:00:00Z",
        row_count=42,
    )
    path = s.write()
    raw = path.read_text()
    parsed = json.loads(raw)
    keys = list(parsed.keys())
    assert keys == sorted(keys), f"Keys not sorted: {keys}"
    assert parsed["row_count"] == 42
    assert parsed["summary_schema_version"] == 1
    assert parsed["external_id"] == s.external_id


# --- Context manager -----------------------------------------------------


def test_builder_run_success_path(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    with builder_run("X", "x.py") as summary:
        summary.row_count = 100
        summary.dataset_id = "0Fb000000000000"
        summary.dataset_version_id = "0Fc000000000000"
    json_files = list(tmp_path.glob("X/*.json"))
    assert len(json_files) == 1
    parsed = json.loads(json_files[0].read_text())
    assert parsed["status"] == "ok"
    assert parsed["row_count"] == 100
    assert parsed["dataset_id"] == "0Fb000000000000"
    assert parsed["runtime_s"] is not None
    assert parsed["runtime_s"] >= 0
    assert parsed["finished_at"] is not None
    assert parsed["errors"] == []


def test_builder_run_failure_path_writes_json_and_reraises(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    with pytest.raises(ValueError, match="boom"):
        with builder_run("X", "x.py") as summary:
            summary.row_count = 50
            raise ValueError("boom")
    json_files = list(tmp_path.glob("X/*.json"))
    assert len(json_files) == 1
    parsed = json.loads(json_files[0].read_text())
    assert parsed["status"] == "failed"
    assert parsed["row_count"] == 50  # was set before the raise
    assert any("ValueError: boom" in e for e in parsed["errors"])
    assert any("Traceback" in e for e in parsed["errors"])


def test_builder_run_write_failure_does_not_mask_original_exception(
    tmp_path, monkeypatch
):
    # Contract from Error Handling Section 4 of the spec: when the body
    # raises AND summary.write() also fails, the body exception must be
    # the one that propagates.
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)

    def boom_write(self):
        raise OSError("disk full")

    monkeypatch.setattr(RunSummary, "write", boom_write)
    with pytest.raises(ValueError, match="original error"):
        with builder_run("X", "x.py"):
            raise ValueError("original error")


def test_builder_run_populates_finished_at_on_both_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    # success
    with builder_run("X", "x.py") as s:
        pass
    parsed_ok = json.loads(next(tmp_path.glob("X/*.json")).read_text())
    assert parsed_ok["finished_at"] is not None
    # failure (different dataset to avoid filename collision)
    with pytest.raises(ValueError):
        with builder_run("Y", "y.py") as s:
            raise ValueError("fail")
    parsed_fail = json.loads(next(tmp_path.glob("Y/*.json")).read_text())
    assert parsed_fail["finished_at"] is not None
```

- [ ] **Step 5.2: Run the test file — expect ImportError**

Run:

```bash
cd /Users/test/crm-analytics
python3 -m pytest tests/test_crm_analytics_runtime.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'crm_analytics_runtime'`.

---

### Task 6: Create `crm_analytics_runtime.py`

**Files:**

- Create: `crm_analytics_runtime.py`

- [ ] **Step 6.1: Create the module file**

Create `/Users/test/crm-analytics/crm_analytics_runtime.py` with this exact content:

```python
"""Per-run audit trail for CRM Analytics builders.

Every builder writes one JSON file per run to runs/<Dataset>/<ts>.json
capturing what ran, how long, what got uploaded, and any errors.

Part of Builder Modernization 1A — see
docs/superpowers/specs/2026-04-06-builder-modernization-1a-plumbing-design.md
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
    started_at: str  # ISO 8601 UTC with Z suffix
    summary_schema_version: int = 1
    external_id: str = ""  # populated in __post_init__
    finished_at: str | None = None
    runtime_s: float | None = None
    row_count: int | None = None
    byte_count: int | None = None
    dataset_id: str | None = None
    dataset_version_id: str | None = None
    status: str = "running"  # "running" | "ok" | "failed"
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
    """Wrap a builder's main() body so the RunSummary is written on
    both success and failure paths.

    Contracts (from spec Error Handling section):
    1. RunSummary write failures must never suppress the body exception.
    2. If the body raised AND the write failed, the body exception is
       the one that propagates.
    3. If the body succeeded but the write failed, the write failure is
       logged at ERROR and the process still exits 0 — a local disk
       issue must not convert a successful data upload into a failure.
    """
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary = RunSummary(
        dataset_name=dataset_name,
        builder_path=builder_path,
        started_at=started,
    )
    t0 = time.monotonic()
    body_exc: BaseException | None = None
    try:
        yield summary
        summary.status = "ok"
    except BaseException as exc:
        body_exc = exc
        summary.status = "failed"
        summary.errors.append(f"{type(exc).__name__}: {exc}")
        summary.errors.append(traceback.format_exc())
    finally:
        summary.finished_at = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        summary.runtime_s = round(time.monotonic() - t0, 2)
        try:
            path = summary.write()
            logger.info(
                "RunSummary written: %s status=%s runtime=%.1fs rows=%s",
                path,
                summary.status,
                summary.runtime_s,
                summary.row_count,
            )
        except OSError as write_exc:
            logger.error("RunSummary write failed: %s", write_exc)
            # Intentionally swallow: must not mask the body exception,
            # and a write failure on a successful body should not turn
            # a successful run into a failure.
        if body_exc is not None:
            raise body_exc
```

- [ ] **Step 6.2: Run the unit tests — expect all pass**

Run:

```bash
cd /Users/test/crm-analytics && python3 -m pytest tests/test_crm_analytics_runtime.py -v
```

Expected: 12 tests, all PASS.
If any fail: fix and re-run until green.

- [ ] **Step 6.3: Commit**

```bash
cd /Users/test/crm-analytics
git add crm_analytics_runtime.py tests/test_crm_analytics_runtime.py
git commit -m "$(cat <<'EOF'
feat: add crm_analytics_runtime.py with RunSummary + builder_run ctx

RunSummary dataclass (stable 18-char external_id, summary_schema_version
for future spec 1D compatibility, Z-suffixed timestamps) + builder_run
context manager that writes the summary JSON on both success and
failure paths without masking body exceptions.

Part of Builder Modernization 1A plumbing iteration.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 5: runs/ Directory and gitignore

### Task 7: Create the runs/ directory structure

**Files:**

- Create: `runs/.gitkeep`
- Create: `runs/README.md`
- Modify: `.gitignore`

- [ ] **Step 7.1: Create the runs/ directory with .gitkeep**

Run:

```bash
cd /Users/test/crm-analytics && mkdir -p runs && touch runs/.gitkeep
```

- [ ] **Step 7.2: Create `runs/README.md`**

Create `/Users/test/crm-analytics/runs/README.md` with this exact content:

````markdown
# `runs/` — Builder Audit Trail

Per-run JSON summaries written by every KPI dataset builder at the end of its `main()`. Produced by `crm_analytics_runtime.builder_run()`. Part of Builder Modernization 1A.

## Structure

```
runs/
├── README.md                        ← this file (committed)
├── .gitkeep                         ← keeps the dir in git (committed)
└── <Dataset_Name>/                  ← per-dataset subdir
    └── <YYYYMMDDTHHMMSSZ>.json      ← one per run (gitignored)
```

The JSON filename encodes the UTC start time with colons and dashes stripped, Z-suffixed. Example: `runs/Commercial_Rhythm_Control_Tower/20260406T154720Z.json`.

## JSON shape

See `crm_analytics_runtime.RunSummary` for the authoritative dataclass. Every file contains:

- `dataset_name`, `builder_path` — what ran
- `started_at`, `finished_at`, `runtime_s` — when + how long
- `row_count`, `byte_count` — what got uploaded
- `dataset_id`, `dataset_version_id` — what dataset version the upload produced
- `status` — `"ok"`, `"failed"`, or `"running"` (only if the process was killed mid-run)
- `errors` — exception messages + tracebacks, joined. Empty list on success.
- `external_id` — deterministic 18-char sha256 over `dataset_name|started_at`. Used by future spec 1D's Salesforce upsert loader.
- `summary_schema_version` — `1` for this iteration; future specs that change the shape will bump it.
- `host` — hostname of the machine that ran the builder.

## Retention

Per-run JSONs are gitignored. Keep the last 30 days locally; older than that, delete manually:

```bash
find runs -name "*.json" -mtime +30 -delete
```

## Future: spec 1D

Spec 1D (Salesforce ops dashboard) adds a loader script that globs `runs/**/*.json` and uploads each file as a `Builder_Run__c` record via the REST API, using `external_id` as the upsert key. When that lands, this README gets a "How 1D consumes this dir" section.
````

- [ ] **Step 7.3: Add `runs/*/*.json` to `.gitignore`**

Run:

```bash
cd /Users/test/crm-analytics && grep -q "^runs/\*/\*\.json$" .gitignore || echo "runs/*/*.json" >> .gitignore
```

Verify:

```bash
cd /Users/test/crm-analytics && grep "runs" .gitignore
```

Expected: at least one line matching `runs/*/*.json`.

- [ ] **Step 7.4: Verify the gitignore works**

Create a test JSON and confirm git ignores it, then clean up:

```bash
cd /Users/test/crm-analytics
mkdir -p runs/_sanity_check
echo '{}' > runs/_sanity_check/20260406T000000Z.json
git status --short runs/ | grep sanity && echo "BUG: json is tracked" || echo "OK: json is gitignored"
rm -rf runs/_sanity_check
```

Expected: `OK: json is gitignored`.

- [ ] **Step 7.5: Commit**

```bash
cd /Users/test/crm-analytics
git add runs/.gitkeep runs/README.md .gitignore
git commit -m "$(cat <<'EOF'
chore: add runs/ dir for RunSummary audit JSONs + gitignore rule

Per-run audit JSONs land in runs/<Dataset>/<YYYYMMDDTHHMMSSZ>.json.
README committed, .gitkeep committed, per-run JSONs gitignored.

Part of Builder Modernization 1A plumbing iteration.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 6: Modernize `crm_analytics_helpers.py`

### Task 8: Add logging config and replace `print()` calls

**Files:**

- Modify: `crm_analytics_helpers.py`

- [ ] **Step 8.1: Read the current top of `crm_analytics_helpers.py`**

Run:

```bash
cd /Users/test/crm-analytics && head -40 crm_analytics_helpers.py
```

Note where the existing imports end. The `logging.basicConfig(...)` block goes immediately after the last import.

- [ ] **Step 8.2: Add the logging block after the imports**

Add these lines (using the Edit tool, targeting the last existing import statement) immediately after the last import in `crm_analytics_helpers.py`:

```python

# --- Logging config (Builder Modernization 1A) ---------------------------
# basicConfig at module load so any builder importing this module gets
# logging configured automatically. crm_analytics_helpers.py is the de
# facto runtime entry point for the 8 KPI builders, so basicConfig
# belongs here despite stdlib warnings against it in general libraries.
import logging
import os

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)
```

If `import logging` or `import os` already exist above, don't duplicate them — just remove them from the block above and add the remaining lines.

- [ ] **Step 8.3: Enumerate the `print()` call sites in `crm_analytics_helpers.py`**

Run:

```bash
cd /Users/test/crm-analytics && grep -n "^[^#]*\bprint(" crm_analytics_helpers.py
```

This lists every line number with a `print(` call. Note the count — it should match what you noted in Step 0.6.

- [ ] **Step 8.4: Replace each `print()` with the appropriate `logger.*` level**

For each line printed by Step 8.3, use the Edit tool to replace the `print(...)` call with:

- `logger.info(...)` for normal progress messages
- `logger.warning(...)` for recoverable degradation or retries
- `logger.error(...)` for about-to-fail or already-failed messages (these should be paired with raising the exception; don't let the error be the only signal)

**Format string conversion:** Python logging uses `%`-style format strings by default and defers interpolation until the log level check passes. Convert:

- `print(f"Fetched {n} rows in {dt:.1f}s")` → `logger.info("Fetched %d rows in %.1fs", n, dt)`
- `print(f"Retry {i}/{max}: {exc}")` → `logger.warning("Retry %d/%d: %s", i, max, exc)`
- `print(f"ERROR: {exc}")` → `logger.error("ERROR: %s", exc)`

Plain-string prints without interpolation just become `logger.info("message text")`.

Do each replacement one at a time and re-run the grep from Step 8.3 to track your progress.

- [ ] **Step 8.5: Verify zero remaining `print()` calls**

Run:

```bash
cd /Users/test/crm-analytics && grep -cn "^[^#]*\bprint(" crm_analytics_helpers.py
```

Expected: `0`.
If non-zero: either you missed one (re-run Step 8.3 to find them) or one is intentional. Intentional `print()` should be the exception — almost always you want `logger.info`. Don't leave any unexplained `print()` calls.

- [ ] **Step 8.6: Verify the module still imports cleanly**

Run:

```bash
cd /Users/test/crm-analytics && python3 -c "
import logging
import crm_analytics_helpers
print('Module loaded OK')
print(f'Logger name: {crm_analytics_helpers.logger.name}')
print(f'Root level: {logging.getLogger().level} (20 = INFO)')
"
```

Expected: `Module loaded OK`, logger name is `crm_analytics_helpers`, root level is 20.
If ImportError: a syntax mistake from the edits; fix and re-run.

- [ ] **Step 8.7: Run the full existing test suite to confirm nothing broke**

Run:

```bash
cd /Users/test/crm-analytics && python3 -m pytest tests/ -v 2>&1 | tail -30
```

Expected: all existing tests still pass, plus the 20 new tests from Phases 3-4.
If a pre-existing test broke: investigate whether it was depending on the `print()` output (e.g., via `capsys`). Fix only the minimum needed — don't rewrite tests beyond restoring the prior behavior.

- [ ] **Step 8.8: Commit**

```bash
cd /Users/test/crm-analytics
git add crm_analytics_helpers.py
git commit -m "$(cat <<'EOF'
refactor: add logging.basicConfig + print→logger in crm_analytics_helpers

Module-level logging config so any builder importing this module gets
structured logging automatically. All internal print() calls migrated
to logger.info/warning/error with %-style format strings.

Part of Builder Modernization 1A plumbing iteration.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 7: Pilot on `build_commercial_rhythm_control_tower.py`

### Task 9: Apply the 6-step mechanical change to the pilot builder

**Files:**

- Modify: `build_commercial_rhythm_control_tower.py`

This is the pattern-lock step. The pilot is intentionally on the cleanest builder (847 lines, "no blocking concerns" per the assessment) so that failures here are almost certainly pattern bugs, not file-specific bugs.

- [ ] **Step 9.1: Read the current top of the builder and locate `main()`**

Run:

```bash
cd /Users/test/crm-analytics && head -30 build_commercial_rhythm_control_tower.py && echo "---" && grep -n "^def main" build_commercial_rhythm_control_tower.py && echo "---" && grep -cn "^[^#]*\bprint(" build_commercial_rhythm_control_tower.py
```

Note: the line where imports end, the line where `main()` starts, and the count of `print()` calls.

- [ ] **Step 9.2: Add the new imports**

Use the Edit tool to add these lines after the last existing import:

```python
import logging

from simcorp_fields import assert_org_schema
from crm_analytics_runtime import builder_run

logger = logging.getLogger(__name__)
```

If any of these imports already exist (unlikely for the three new ones), don't duplicate them.

- [ ] **Step 9.3: Identify which SObjects this builder queries**

Run:

```bash
cd /Users/test/crm-analytics && grep -oE "FROM [A-Z][A-Za-z_]+" build_commercial_rhythm_control_tower.py | sort -u
```

Note the list. For `build_commercial_rhythm_control_tower.py` expect probably `FROM Account`, `FROM Opportunity`, `FROM User`. Use these SObject names in the `assert_org_schema(..., objects=[...])` call in Step 9.4.

- [ ] **Step 9.4: Wrap `main()` body in `with builder_run(...):` and add the describe-check**

Read the current `main()` function, then use Edit to transform it. Before:

```python
def main():
    instance_url, access_token = auth()
    rows = fetch_rows(instance_url, access_token)
    result = upload_dataset(rows, "Commercial_Rhythm_Control_Tower")
    print(f"Uploaded {len(rows)} rows; dataset_version_id={result['dataset_version_id']}")
```

After:

```python
def main():
    with builder_run("Commercial_Rhythm_Control_Tower", __file__) as summary:
        instance_url, access_token = auth()
        assert_org_schema(
            instance_url,
            access_token,
            objects=["Account", "Opportunity", "User"],  # replace with Step 9.3 list
        )
        rows = fetch_rows(instance_url, access_token)
        summary.row_count = len(rows)
        result = upload_dataset(rows, "Commercial_Rhythm_Control_Tower")
        summary.dataset_id = result.get("dataset_id")
        summary.dataset_version_id = result.get("dataset_version_id")
        summary.byte_count = result.get("byte_count")
        logger.info(
            "Uploaded %d rows; dataset_version_id=%s",
            len(rows),
            result.get("dataset_version_id"),
        )
```

**Important:** the real `main()` is longer and has more intermediate steps. Transform the real one, not this scaffolded example — preserve every line of the existing logic, just indented into the `with` block and with `summary.*` populated at the appropriate points.

**Two specific invariants:**

- The `assert_org_schema(...)` call MUST come AFTER auth and BEFORE the first SOQL query.
- The `summary.row_count = ...` assignment MUST come BEFORE `upload_dataset(...)` so that a failed upload still leaves an accurate row count in the JSON.

- [ ] **Step 9.5: Replace every `print()` with `logger.*`**

Same pattern as Step 8.4 in Phase 6:

- `print(f"...")` → `logger.info("...", *args)` with `%`-style format
- Error prints → `logger.error`
- Retry/warning prints → `logger.warning`

Run the grep again to verify:

```bash
cd /Users/test/crm-analytics && grep -cn "^[^#]*\bprint(" build_commercial_rhythm_control_tower.py
```

Expected: `0`.

- [ ] **Step 9.6: Check upload_dataset return shape**

The pilot assumes `upload_dataset()` returns a dict with `dataset_id`, `dataset_version_id`, `byte_count` keys. Verify this against the helpers module:

```bash
cd /Users/test/crm-analytics && grep -n "def upload_dataset\|return {" crm_analytics_helpers.py | head -10
```

If the return shape differs (e.g., returns a tuple, or uses different key names), update the `summary.dataset_id = ...` assignments in Step 9.4 to match. The goal is: `summary.dataset_id`, `summary.dataset_version_id`, `summary.byte_count` all end up with real values, not None, on a successful upload.

If `upload_dataset()` doesn't return enough information, update its return to include a dict of `{dataset_id, dataset_version_id, byte_count}` (a compatible change — existing callers that don't read the return value are unaffected). Note this as a helper change.

- [ ] **Step 9.7: Run pytest one more time to make sure the pilot edits don't break the test suite**

Run:

```bash
cd /Users/test/crm-analytics && python3 -m pytest tests/test_simcorp_fields.py tests/test_crm_analytics_runtime.py -v
```

Expected: 20 tests PASS.

- [ ] **Step 9.8: Run the modernized pilot builder live**

Run:

```bash
cd /Users/test/crm-analytics && python3 build_commercial_rhythm_control_tower.py 2>&1 | tee /tmp/pilot-run.log
```

Expected: the builder runs to completion, exits 0. The log should show:

- `Auth OK` at the top
- `Schema check Account (... fields)`, `Schema check Opportunity (... fields)`, `Schema check User (... fields)`
- `All required fields present`
- The builder's existing progress messages (now at `logger.info` level)
- `Upload complete dataset_version_id=...`
- `RunSummary written: runs/Commercial_Rhythm_Control_Tower/<timestamp>.json status=ok runtime=...s rows=...`

If the builder raises a `SchemaDriftError`: the field audit missed a field this builder uses. Add it to the appropriate `*_FIELDS` tuple in `simcorp_fields.py`, re-run the `simcorp_fields` tests, then re-run the pilot.

If the builder raises a different error (ImportError, AttributeError, NameError): fix the mechanical transform — something in the `main()` edit is malformed.

- [ ] **Step 9.9: Verify the RunSummary JSON exists and has the expected shape**

Run:

```bash
cd /Users/test/crm-analytics && ls runs/Commercial_Rhythm_Control_Tower/ && cat runs/Commercial_Rhythm_Control_Tower/*.json
```

Expected:

- One JSON file named `<YYYYMMDDTHHMMSSZ>.json`
- `status: "ok"`
- `row_count` is non-null and positive
- `dataset_id` and `dataset_version_id` are populated
- `byte_count` is populated
- `errors` is `[]`
- `external_id` is 18 hex chars
- `summary_schema_version: 1`

- [ ] **Step 9.10: Check the row count is within ±10% of the baseline**

Run:

```bash
cd /Users/test/crm-analytics && python3 <<'PY'
import json, glob
runs = sorted(glob.glob("runs/Commercial_Rhythm_Control_Tower/*.json"))
latest = json.load(open(runs[-1]))
with open("docs/2026-04-06-builder-modernization-1a-baselines.md") as f:
    for line in f:
        if "Commercial_Rhythm_Control_Tower" in line and "|" in line:
            cells = [c.strip() for c in line.split("|") if c.strip()]
            baseline = int(cells[1])
            break
new = latest["row_count"]
delta = (new - baseline) / baseline
print(f"Baseline: {baseline}")
print(f"Post-run: {new}")
print(f"Delta:    {delta*100:+.2f}%")
if abs(delta) <= 0.10:
    print("WITHIN TOLERANCE")
else:
    print("OUT OF TOLERANCE — investigate before committing")
PY
```

Expected: `WITHIN TOLERANCE`.
If out of tolerance: do NOT commit. Investigate. Possible causes:

- Data legitimately changed (verify by re-running the SAQL count query from Phase 1)
- The builder logic was broken by the transform (diff against the pre-modernization version)
- The baseline was wrong in Phase 1 (unlikely but possible)

- [ ] **Step 9.11: Commit the pilot**

```bash
cd /Users/test/crm-analytics
git add build_commercial_rhythm_control_tower.py
git commit -m "$(cat <<'EOF'
refactor: modernize build_commercial_rhythm_control_tower (pilot)

Apply the 6-step plumbing pattern from Builder Modernization 1A:
- Import simcorp_fields.assert_org_schema and crm_analytics_runtime.builder_run
- Add logger = logging.getLogger(__name__)
- Wrap main() body in `with builder_run(...) as summary:`
- Call assert_org_schema after auth, before first SOQL
- Populate summary.{row_count,dataset_id,dataset_version_id,byte_count}
- Replace all print() with logger.{info,warning,error}

Verified live: run exits 0, RunSummary JSON written, row count within
±10% of the 2026-04-06 baseline.

Pilot for the parallel subagent fan-out in Phase 8.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 8: Parallel Subagent Fan-Out

### Task 10: Dispatch 7 parallel subagents to apply the pattern to the remaining 7 builders

**Files:**

- Modify (via subagents, in parallel): the 7 remaining builders

The 7 remaining builders are independent (the assessment confirms no builder reads another builder's dataset as a join source). Dispatch all 7 subagents in a single parallel batch. Each subagent operates on its single assigned builder and is explicitly forbidden from editing any other file.

- [ ] **Step 10.1: Prepare the subagent dispatch prompt template**

Each subagent gets a prompt with the structure below. Fill in the four `{{...}}` placeholders per subagent.

**Template (do not dispatch yet — Step 10.2 uses it):**

```
You are a subagent in the Builder Modernization 1A parallel fan-out.
Your job is to apply a mechanical plumbing transform to exactly one
Python file, run it live against the SimCorp Salesforce org, verify
the result, and commit — or report back without committing if
anything fails.

## Your assignment

- ASSIGNED FILE: /Users/test/crm-analytics/{{BUILDER_PATH}}
- DATASET NAME: {{DATASET_NAME}}
- BASELINE ROW COUNT: {{BASELINE_COUNT}} (from docs/2026-04-06-builder-modernization-1a-baselines.md)
- TOLERANCE: ±10%

## Rules (hard — violations cause the whole iteration to roll back)

1. You may ONLY modify the ASSIGNED FILE. You may NOT modify any other
   file in the repo, under any circumstance. If you find a bug in
   simcorp_fields.py, crm_analytics_runtime.py, or crm_analytics_helpers.py,
   STOP and report it instead of fixing it yourself.
2. You may NOT use `git add .`, `git add -A`, or `git add -u`. Stage
   by exact path only: `git add {{BUILDER_PATH}}`.
3. You may NOT push to origin.
4. You may NOT touch any file under .worktrees/, .playwright-cli/, or
   any config/*.json file.

## Reference artifacts you should read first

1. /Users/test/crm-analytics/docs/superpowers/specs/2026-04-06-builder-modernization-1a-plumbing-design.md
   (the design spec; read the Components and Data Flow sections)
2. /Users/test/crm-analytics/build_commercial_rhythm_control_tower.py
   (the pilot; this is the pattern you're replicating)
3. /Users/test/crm-analytics/simcorp_fields.py (new module you will use)
4. /Users/test/crm-analytics/crm_analytics_runtime.py (new module you will use)

## The 6-step transform (apply to ASSIGNED FILE only)

1. Add these imports after the last existing import:
     import logging
     from simcorp_fields import assert_org_schema
     from crm_analytics_runtime import builder_run
   Then add: logger = logging.getLogger(__name__)

2. Identify every Salesforce SObject queried in this file:
     grep -oE "FROM [A-Z][A-Za-z_]+" {{BUILDER_PATH}} | sort -u

3. Wrap the body of main() in:
     with builder_run("{{DATASET_NAME}}", __file__) as summary:
         ...
   (preserve every existing line of main(), just indented one level)

4. Immediately after auth() returns (instance_url + access_token) but
   BEFORE the first SOQL query, call:
     assert_org_schema(
         instance_url,
         access_token,
         objects=[<list from step 2>],
     )

5. After the upload_dataset(...) call, populate:
     summary.row_count = <rows>
     summary.dataset_id = <result['dataset_id']>
     summary.dataset_version_id = <result['dataset_version_id']>
     summary.byte_count = <result['byte_count']>
   The row_count assignment should come BEFORE upload_dataset when
   possible, so that a failed upload still records the extracted row
   count.

6. Replace every print() call with logger.info, logger.warning, or
   logger.error, using %-style format strings (not f-strings inside
   the logger call). Verify with:
     grep -cn "^[^#]*\bprint(" {{BUILDER_PATH}}
   Must print 0.

## Validation

After applying the transform:

1. Run the unit tests:
     cd /Users/test/crm-analytics
     python3 -m pytest tests/test_simcorp_fields.py tests/test_crm_analytics_runtime.py -v
   Expected: 20 tests PASS. If any fail: STOP and report.

2. Run the modernized builder live:
     cd /Users/test/crm-analytics
     python3 {{BUILDER_PATH}} 2>&1 | tee /tmp/{{DATASET_NAME}}-run.log
   Expected: exits 0, log contains "RunSummary written: runs/{{DATASET_NAME}}/..." line.

3. Inspect the RunSummary:
     cat runs/{{DATASET_NAME}}/*.json
   Expected: status="ok", row_count set, dataset_id set, dataset_version_id set, errors=[].

4. Compute the delta:
     python3 -c "
     import json, glob
     runs = sorted(glob.glob('runs/{{DATASET_NAME}}/*.json'))
     latest = json.load(open(runs[-1]))
     baseline = {{BASELINE_COUNT}}
     new = latest['row_count']
     delta = (new - baseline) / baseline
     print(f'baseline={baseline} new={new} delta={delta*100:+.2f}%')
     if abs(delta) > 0.10:
         raise SystemExit(f'OUT OF TOLERANCE: {delta*100:+.2f}%')
     print('WITHIN TOLERANCE')
     "
   Expected: WITHIN TOLERANCE.

## Commit (only if all validations pass)

    cd /Users/test/crm-analytics
    git add {{BUILDER_PATH}}
    git commit -m "$(cat <<'EOF'
    refactor: modernize {{BUILDER_STEM}} via 1A plumbing pattern

    Apply the 6-step transform from Builder Modernization 1A:
    simcorp_fields.assert_org_schema after auth, builder_run context
    manager around main(), RunSummary population on upload, print()
    → logger.* migration.

    Verified live: exit 0, RunSummary JSON written with status=ok,
    row count delta {DELTA_PERCENT}% vs baseline {{BASELINE_COUNT}}.

    Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
    EOF
    )"

Replace {DELTA_PERCENT} in your commit message with the actual delta
from validation step 4.

## Reporting back

Your response should contain exactly these fields:

- ASSIGNED FILE: {{BUILDER_PATH}}
- STATUS: OK | FAILED
- COMMIT SHA: <sha> (if OK)
- ROW COUNT DELTA: <percent> (if OK)
- FAILURE DETAILS: <log snippet + which validation step failed> (if FAILED)

Do NOT include any other commentary. The coordinator will aggregate
the 7 reports into an audit table.
```

- [ ] **Step 10.2: Dispatch the 7 subagents in parallel (single message, 7 Agent tool calls)**

In a single tool-use batch, invoke the `Agent` tool 7 times, one per builder below. Use `subagent_type=general-purpose` for each. Fill in the template from Step 10.1 per builder.

| Builder                                           | Dataset name                      | Baseline (from Phase 1) |
| ------------------------------------------------- | --------------------------------- | ----------------------- |
| `build_pipeline_opportunity_operations.py`        | `Pipeline_Opportunity_Operations` | from baselines doc      |
| `build_forecast_revenue_motions.py`               | `Forecast_Revenue_Motions`        | from baselines doc      |
| `build_revenue_retention_health.py`               | `Revenue_Retention_Health`        | from baselines doc      |
| `scripts/build_source_truth_executive_revenue.py` | `Executive_Revenue_Source_Truth`  | from baselines doc      |
| `build_account_intelligence.py`                   | `Account_Intelligence`            | from baselines doc      |
| `build_customer_account_health.py`                | `Customer_Account_Health`         | from baselines doc      |
| `build_forecasting.py`                            | `Forecast_Intelligence`           | from baselines doc      |

The baseline counts come from `/Users/test/crm-analytics/docs/2026-04-06-builder-modernization-1a-baselines.md` — read it and substitute the real numbers into each subagent's prompt before dispatch.

Expected: 7 parallel responses, each with a `STATUS: OK` + commit SHA + row count delta, or `STATUS: FAILED` + failure details.

- [ ] **Step 10.3: Review each subagent's result**

For each of the 7 reports:

- **If `STATUS: OK`:** record the commit SHA and delta in a scratchpad for the Phase 9 audit table.
- **If `STATUS: FAILED`:** read the failure details. Do not silently re-dispatch. Common failure modes and the right response:
  - **Schema drift error:** the field audit in Task 2 missed a field this builder uses. Add it to the appropriate `*_FIELDS` tuple in `simcorp_fields.py`, re-run the `simcorp_fields` tests, then re-dispatch THAT subagent only (not all 7).
  - **Delta > 10%:** investigate whether the change is legitimate. If yes (e.g., `Forecast_Revenue_Motions` is known to swing daily), manually commit the builder change with an explicit delta justification in the commit message. If no: revert the subagent's changes and re-dispatch with a fresh prompt.
  - **Tests failed:** diff the subagent's builder changes against the pilot pattern. The subagent likely deviated — re-dispatch with an explicit "follow the pilot line-by-line" instruction.
  - **Import error / NameError:** mechanical bug in the transform. Re-dispatch with a sharper prompt that quotes the pilot's exact `with builder_run(...)` block.

- [ ] **Step 10.4: Verify all 8 builders now have RunSummary JSONs**

Run:

```bash
cd /Users/test/crm-analytics && for ds in \
  Commercial_Rhythm_Control_Tower \
  Pipeline_Opportunity_Operations \
  Forecast_Revenue_Motions \
  Revenue_Retention_Health \
  Executive_Revenue_Source_Truth \
  Account_Intelligence \
  Customer_Account_Health \
  Forecast_Intelligence; do
  latest=$(ls -t runs/$ds/*.json 2>/dev/null | head -1)
  if [ -n "$latest" ]; then
    status=$(python3 -c "import json; print(json.load(open('$latest'))['status'])")
    rows=$(python3 -c "import json; print(json.load(open('$latest'))['row_count'])")
    echo "$ds: $status rows=$rows"
  else
    echo "$ds: MISSING"
  fi
done
```

Expected: 8 lines, all with `status=ok` and positive `rows` counts.

- [ ] **Step 10.5: Verify all 8 modernized builders compile (syntax check)**

Run:

```bash
cd /Users/test/crm-analytics && for f in \
  build_commercial_rhythm_control_tower.py \
  build_pipeline_opportunity_operations.py \
  build_forecast_revenue_motions.py \
  build_revenue_retention_health.py \
  scripts/build_source_truth_executive_revenue.py \
  build_account_intelligence.py \
  build_customer_account_health.py \
  build_forecasting.py; do
  python3 -m py_compile "$f" && echo "OK $f" || echo "SYNTAX ERROR $f"
done
```

Expected: 8 lines, all `OK`.

- [ ] **Step 10.6: Verify zero remaining `print()` calls in any of the 8 builders**

Run:

```bash
cd /Users/test/crm-analytics && for f in \
  build_commercial_rhythm_control_tower.py \
  build_pipeline_opportunity_operations.py \
  build_forecast_revenue_motions.py \
  build_revenue_retention_health.py \
  scripts/build_source_truth_executive_revenue.py \
  build_account_intelligence.py \
  build_customer_account_health.py \
  build_forecasting.py; do
  count=$(grep -c "^[^#]*\bprint(" "$f" || echo 0)
  echo "$f: $count"
done
```

Expected: 8 lines, all with `0`.

---

## Phase 9: Final Audit Table and Wrap-Up

### Task 11: Produce the Phase 4-equivalent audit table and commit it

**Files:**

- Create: `docs/2026-04-06-builder-modernization-1a-audit-table.md`

- [ ] **Step 11.1: Generate the audit table**

Run:

```bash
cd /Users/test/crm-analytics && python3 <<'PY'
import json, glob, os, subprocess

datasets = [
    "Commercial_Rhythm_Control_Tower",
    "Pipeline_Opportunity_Operations",
    "Forecast_Revenue_Motions",
    "Revenue_Retention_Health",
    "Executive_Revenue_Source_Truth",
    "Account_Intelligence",
    "Customer_Account_Health",
    "Forecast_Intelligence",
]

# Parse the baselines doc to get the baseline counts.
baselines = {}
with open("docs/2026-04-06-builder-modernization-1a-baselines.md") as f:
    for line in f:
        if "|" in line:
            for ds in datasets:
                if line.strip().startswith(f"| {ds}"):
                    cells = [c.strip() for c in line.split("|") if c.strip()]
                    baselines[ds] = int(cells[1])

# Read the latest RunSummary per dataset.
print("# Builder Modernization 1A — Post-Run Audit Table")
print()
print("**Date:** 2026-04-06")
print("**Iteration:** 1A (plumbing only — simcorp_fields, RunSummary, logging)")
print()
print("| Dataset | Status | Runtime | Rows | Baseline | Δ | dataset_version_id |")
print("| --- | --- | --- | --- | --- | --- | --- |")
for ds in datasets:
    runs = sorted(glob.glob(f"runs/{ds}/*.json"))
    if not runs:
        print(f"| {ds} | MISSING | — | — | {baselines.get(ds, '?')} | — | — |")
        continue
    s = json.load(open(runs[-1]))
    base = baselines.get(ds, 0)
    delta = (s["row_count"] - base) / base * 100 if base else 0
    print(
        f"| {ds} | {s['status']} | {s['runtime_s']}s | {s['row_count']} | "
        f"{base} | {delta:+.2f}% | {s.get('dataset_version_id', '—')} |"
    )
PY
```

Pipe the output into the file:

```bash
cd /Users/test/crm-analytics && python3 <<'PY' > docs/2026-04-06-builder-modernization-1a-audit-table.md
# ... (copy the script above here, or extract it to a reusable helper)
PY
```

(Alternatively, copy the script's stdout manually into the file.)

- [ ] **Step 11.2: Verify the table looks right**

Run:

```bash
cat /Users/test/crm-analytics/docs/2026-04-06-builder-modernization-1a-audit-table.md
```

Expected: 8 rows, all `status=ok`, all deltas within ±10% (or with explicit commit-message justification if not).
If any row shows `MISSING` or `failed`: stop and resolve before committing the audit table.

- [ ] **Step 11.3: Run the full test suite one last time**

Run:

```bash
cd /Users/test/crm-analytics && python3 -m pytest tests/ -v 2>&1 | tail -30
```

Expected: all tests PASS, including the 20 new ones.

- [ ] **Step 11.4: Commit the audit table**

```bash
cd /Users/test/crm-analytics
git add docs/2026-04-06-builder-modernization-1a-audit-table.md
git commit -m "$(cat <<'EOF'
docs: post-run audit table for Builder Modernization 1A

All 8 KPI dataset builders have been modernized with the 1A plumbing
pattern (simcorp_fields + RunSummary + logging) and run live against
apro@simcorp.com. This table captures the final row counts, runtimes,
and dataset version IDs as the signoff artifact for iteration 1A.

Next iterations (see spec Future Work section):
- 1B: Hard calendar-Q switch across all 8 builders
- 1C: Deck relabel + dashboard SAQL cascade + ADR-0002
- 1D: Salesforce ops dashboard backed by Builder_Run__c
- 1E: Assessment items 5-12 (decomposition, dead code, etc.)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 11.5: Summarize the commit graph for the whole iteration**

Run:

```bash
cd /Users/test/crm-analytics && git log --oneline 95bee24..HEAD
```

Expected: a linear list of commits from the spec commit (`95bee24`) through all the Phase 1-9 commits. Should be roughly 14-16 commits:

- 1 spec commit (95bee24 from before this plan)
- 1 baselines doc commit (Phase 1)
- 1 simcorp_fields commit (Phase 3)
- 1 crm_analytics_runtime commit (Phase 4)
- 1 runs/ dir commit (Phase 5)
- 1 crm_analytics_helpers commit (Phase 6)
- 1 pilot builder commit (Phase 7)
- 7 subagent builder commits (Phase 8)
- 1 audit table commit (Phase 9)

- [ ] **Step 11.6: Do NOT push to origin**

Per Andre's hard rule, no push is ever done without explicit instruction. The branch stays local. When Andre is ready to push, he'll say so explicitly.

---

## Success Criteria Checklist (from spec)

Verify each criterion before declaring the iteration complete:

- [ ] `simcorp_fields.py` exists, all per-object constant tuples populated, `assert_org_schema()` passes against the live org (Task 4.6)
- [ ] `crm_analytics_runtime.py` exists with `RunSummary` + `builder_run()` (Task 6)
- [ ] `tests/test_simcorp_fields.py` (8 tests) and `tests/test_crm_analytics_runtime.py` (12 tests) exist and all pass (Task 11.3)
- [ ] `crm_analytics_helpers.py` has `logging.basicConfig` at module load and zero remaining `print()` calls (Task 8.5)
- [ ] All 8 KPI dataset builders have been modernized per the 6-component mechanical change list (Task 10.5, 10.6)
- [ ] All 8 builders have been run live and each produced a `runs/<Dataset>/<ts>.json` with `status="ok"` (Task 10.4)
- [ ] Each builder's row count is within ±10% of the pre-modernization baseline (or explicitly justified in the commit message) (Task 11.1)
- [ ] The audit table shows all 8 builders green (Task 11.2)
- [ ] `runs/README.md` and `runs/.gitkeep` exist; `runs/*/*.json` is gitignored (Task 7)
- [ ] No `build_*.py` file other than the 8 KPI builders has been touched (verify with `git diff 95bee24..HEAD --stat -- 'build_*.py' 'scripts/build_*.py'`)
