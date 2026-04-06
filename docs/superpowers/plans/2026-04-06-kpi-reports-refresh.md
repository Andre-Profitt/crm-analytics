# KPI Reports Refresh — Snapshot 2026-04-01 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run a clean refresh of Report 1 (Sales Director monthly) and Report 2 (Sales Ops quarterly) PowerPoint decks against snapshot date 2026-04-01, with PowerPoint as the authoritative WYSIWYG validator and LibreOffice dropped on both reports.

**Architecture:** Sequential execution. Report 1 reuses the existing locked operator wrapper with `--snapshot-date 2026-04-01 --skip-validation`. Report 2 gets two small new helper scripts (a Python SAQL refresher and a bash wrapper) that mirror the Report 1 operator pattern, then builds + PowerPoint-validates the deck. CRMA primary backbone per ADR-0001 — no architectural change.

**Tech Stack:** Python 3.13 (`requests`, `argparse`, standard library), Bash, Node.js (`pptxgenjs` via existing `build_sales_ops_quarterly_deck.js`), `sf` CLI for auth, Wave API v66.0 (`/services/data/v66.0/wave/query`), Microsoft PowerPoint for PDF export, `pytest` for unit tests.

**Spec:** [`docs/superpowers/specs/2026-04-06-kpi-reports-refresh-design.md`](../specs/2026-04-06-kpi-reports-refresh-design.md)
**ADR:** [`docs/adr/ADR-0001-kpi-reports-data-backbone.md`](../../adr/ADR-0001-kpi-reports-data-backbone.md)

---

## File Map

| File                                                                                                             | Action             | Responsibility                                                                                                                                                                      |
| ---------------------------------------------------------------------------------------------------------------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/run_report2_saql_refresh.py`                                                                            | Create             | Read each `live_saql_validation.json`, re-execute its SAQL queries against the live org via Wave API, atomically write back updated `sample_records` + `row_count` + `validated_at` |
| `scripts/run_report2_quarterly_default.sh`                                                                       | Create             | Operator wrapper for Report 2: preflight pkill → SAQL refresh → deck build → PowerPoint review → summary                                                                            |
| `tests/test_run_report2_saql_refresh.py`                                                                         | Create             | Pytest unit tests for the JSON merge logic in `run_report2_saql_refresh.py` (uses fixture, mocks Wave API HTTP layer)                                                               |
| `tests/fixtures/sales_ops_live_saql_validation_sample.json`                                                      | Create             | Minimal fixture mirroring the real `live_saql_validation.json` structure for unit tests                                                                                             |
| `output/sales_ops_quarterly_runs/.gitkeep`                                                                       | Create             | New run-dir tree mirroring `output/sales_director_monthly_runs/`                                                                                                                    |
| `output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01/`                                    | Created at runtime | Report 1 run output (created by the existing Report 1 wrapper)                                                                                                                      |
| `output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/`                                       | Created at runtime | Report 2 run output (created by the new Report 2 wrapper)                                                                                                                           |
| `scripts/run_sales_director_monthly_report.py`                                                                   | Read only — verify | The `--skip-validation` flag exists at line 113; do not modify                                                                                                                      |
| `scripts/run_report1_monthly_default.sh`                                                                         | Read only — verify | Passes args through to runner; do not modify                                                                                                                                        |
| `scripts/export_powerpoint_pdf.py`                                                                               | Read only — verify | Already exists; both reports use it                                                                                                                                                 |
| `output/sales_ops_quarterly_deck_2026-03-31/build_sales_ops_quarterly_deck.js`                                   | Read only — verify | Accepts `--output`, `--snapshot-date`, `--deck-title`, `--page1-json`, etc.                                                                                                         |
| `output/sales_ops_pageN_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json` (N ∈ {1,4,5,6}) | Modify in place    | The SAQL refresh helper rewrites these atomically with fresh results                                                                                                                |

All paths are relative to `/Users/test/crm-analytics/`.

---

## Phase 0: Environment Verification

### Task 0: Verify the environment is sane before touching anything

**Files:**

- Read only: `scripts/run_sales_director_monthly_report.py`
- Read only: `scripts/run_report1_monthly_default.sh`
- Read only: `scripts/export_powerpoint_pdf.py`

- [ ] **Step 0.1: Confirm `sf` CLI is authenticated against the SimCorp org**

Run:

```bash
sf org display --target-org apro@simcorp.com --json | python3 -c "import json,sys; d=json.load(sys.stdin); print('OK' if d.get('result',{}).get('accessToken') else 'NO TOKEN')"
```

Expected output: `OK`
If output is `NO TOKEN`, stop and run `sf org login web --instance-url https://simcorp.my.salesforce.com --alias apro@simcorp.com` to re-auth, then re-run this step.

- [ ] **Step 0.2: Confirm Microsoft PowerPoint is installed at the expected location**

Run:

```bash
test -d "/Applications/Microsoft PowerPoint.app" && echo OK || echo MISSING
```

Expected output: `OK`
If `MISSING`, stop — the PowerPoint review path requires the macOS Microsoft PowerPoint app at this exact path.

- [ ] **Step 0.3: Confirm `--skip-validation` flag exists in the Report 1 runner**

Run:

```bash
grep -n "skip-validation" /Users/test/crm-analytics/scripts/run_sales_director_monthly_report.py
```

Expected: at least one line returned, e.g. `113:        "--skip-validation",`
If no output, stop — the spec assumes this flag exists; if it has been removed, the plan needs to be revised.

- [ ] **Step 0.4: Confirm the four Sales Ops live SAQL JSONs exist**

Run:

```bash
for n in 1 4 5 6; do
  test -f "/Users/test/crm-analytics/output/sales_ops_page${n}_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json" && echo "page${n}: OK" || echo "page${n}: MISSING"
done
```

Expected output:

```
page1: OK
page4: OK
page5: OK
page6: OK
```

If any are MISSING, stop and report which one — the spec assumes all four exist.

- [ ] **Step 0.5: Confirm the `.venv_slides` Python venv exists with `render_slides.py`**

Run:

```bash
test -x /Users/test/crm-analytics/.venv_slides/bin/python && \
test -f /Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/scripts/render_slides.py && \
test -f /Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/scripts/create_montage.py && \
echo OK || echo MISSING
```

Expected output: `OK`
If `MISSING`, stop — the PowerPoint review path needs these helpers to convert the exported PDF to page images and a montage.

- [ ] **Step 0.6: Confirm the Report 2 deck workspace has its npm dependencies installed**

Run:

```bash
test -d /Users/test/crm-analytics/output/sales_ops_quarterly_deck_2026-03-31/node_modules && echo OK || echo MISSING
```

Expected output: `OK`
If `MISSING`, run `cd /Users/test/crm-analytics/output/sales_ops_quarterly_deck_2026-03-31 && npm install` and re-check.

- [ ] **Step 0.7: Capture the prior 2026-03-31 baseline KPIs for delta comparison later**

Run:

```bash
cat /Users/test/crm-analytics/output/sales_ops_quarterly_deck_2026-03-31/sales_ops_quarterly_review_2026-03-31.summary.json
```

Note the values of: `data_completeness_score`, `process_compliance_rate`, `forecast_accuracy`, `pipeline_hygiene_rate`, `top_exception_queue_area`, `top_exception_queue_count`. These are the prior baseline. Store them mentally (or paste into a scratch note) — the post-refresh summary in Phase 5 will compare against these.

Also run:

```bash
cat /Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/sales_director_monthly_pipeline_insights_2026-03-31.summary.json
```

Note: `quarter_focus`, `biggest_gap_region`, `biggest_gap_arr`, `weakest_confidence_region`, `weakest_confidence_pct`, `total_open_renewal_pipeline`, `critical_renewal_arr`, `biggest_slipped_region`, `biggest_slipped_arr`. Same purpose — post-refresh delta.

---

## Phase 1: Build the SAQL Refresh Helper (`run_report2_saql_refresh.py`)

This phase uses TDD on the JSON merge logic — the algorithmic core of the script. The HTTP layer is wired in after the merge logic is unit-tested.

### Task 1: Create the test fixture

**Files:**

- Create: `tests/fixtures/sales_ops_live_saql_validation_sample.json`

- [ ] **Step 1.1: Create the fixture file**

This fixture mirrors the real `live_saql_validation.json` structure with two minimal results so the merge logic can be tested without hitting the live org.

Create `/Users/test/crm-analytics/tests/fixtures/sales_ops_live_saql_validation_sample.json` with this exact content:

```json
{
  "artifact_type": "sales_ops_test_live_saql_validation",
  "validated_at": "2026-03-31",
  "results": [
    {
      "step_alias": "test_kpi_one",
      "metric": "Test KPI One",
      "status": "ok",
      "query": "q = load \"Account_Intelligence\";\nq = group q by all;\nq = foreach q generate count() as total;",
      "row_count": 1,
      "sample_records": [{ "total": 100 }]
    },
    {
      "step_alias": "test_kpi_two",
      "metric": "Test KPI Two",
      "status": "ok",
      "query": "q = load \"Account_Intelligence\";\nq = filter q by HasDUNS == \"true\";\nq = group q by all;\nq = foreach q generate count() as duns_count;",
      "row_count": 1,
      "sample_records": [{ "duns_count": 50 }]
    }
  ]
}
```

- [ ] **Step 1.2: Verify the fixture is valid JSON**

Run:

```bash
python3 -c "import json; json.load(open('/Users/test/crm-analytics/tests/fixtures/sales_ops_live_saql_validation_sample.json')); print('OK')"
```

Expected output: `OK`

### Task 2: Write the failing test for `merge_query_results()`

**Files:**

- Create: `tests/test_run_report2_saql_refresh.py`

- [ ] **Step 2.1: Write the test file**

Create `/Users/test/crm-analytics/tests/test_run_report2_saql_refresh.py` with this exact content:

```python
"""Unit tests for run_report2_saql_refresh.py."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from run_report2_saql_refresh import merge_query_results, load_validation_json

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "sales_ops_live_saql_validation_sample.json"


def test_load_validation_json_returns_expected_structure():
    payload = load_validation_json(FIXTURE_PATH)
    assert payload["artifact_type"] == "sales_ops_test_live_saql_validation"
    assert len(payload["results"]) == 2
    assert payload["results"][0]["step_alias"] == "test_kpi_one"


def test_merge_query_results_updates_sample_records_and_row_count():
    original = load_validation_json(FIXTURE_PATH)
    new_results_by_alias = {
        "test_kpi_one": [{"total": 200}, {"total": 250}],
        "test_kpi_two": [{"duns_count": 75}],
    }
    merged = merge_query_results(original, new_results_by_alias, validated_at="2026-04-01")

    assert merged["validated_at"] == "2026-04-01"
    assert merged["results"][0]["sample_records"] == [{"total": 200}, {"total": 250}]
    assert merged["results"][0]["row_count"] == 2
    assert merged["results"][0]["status"] == "ok"
    assert merged["results"][1]["sample_records"] == [{"duns_count": 75}]
    assert merged["results"][1]["row_count"] == 1


def test_merge_query_results_marks_missing_aliases_as_error():
    original = load_validation_json(FIXTURE_PATH)
    # Only update one of the two aliases
    new_results_by_alias = {
        "test_kpi_one": [{"total": 999}],
    }
    merged = merge_query_results(original, new_results_by_alias, validated_at="2026-04-01")

    assert merged["results"][0]["status"] == "ok"
    assert merged["results"][0]["sample_records"] == [{"total": 999}]
    # Missing alias keeps its prior sample_records but flips status to "error"
    assert merged["results"][1]["status"] == "error"
    assert merged["results"][1]["sample_records"] == [{"duns_count": 50}]


def test_merge_query_results_preserves_query_and_metric_fields():
    original = load_validation_json(FIXTURE_PATH)
    new_results_by_alias = {
        "test_kpi_one": [{"total": 1}],
        "test_kpi_two": [{"duns_count": 1}],
    }
    merged = merge_query_results(original, new_results_by_alias, validated_at="2026-04-01")

    assert merged["results"][0]["query"] == original["results"][0]["query"]
    assert merged["results"][0]["metric"] == "Test KPI One"
    assert merged["results"][1]["metric"] == "Test KPI Two"


def test_merge_query_results_does_not_mutate_input():
    original = load_validation_json(FIXTURE_PATH)
    original_copy = json.loads(json.dumps(original))
    _ = merge_query_results(original, {"test_kpi_one": [{"total": 1}]}, validated_at="2026-04-01")
    assert original == original_copy
```

- [ ] **Step 2.2: Run the test to verify it fails for the expected reason**

Run:

```bash
cd /Users/test/crm-analytics && python3 -m pytest tests/test_run_report2_saql_refresh.py -v 2>&1 | tail -20
```

Expected: ImportError or ModuleNotFoundError on `from run_report2_saql_refresh import ...` because the script doesn't exist yet.

### Task 3: Implement the script with the merge logic

**Files:**

- Create: `scripts/run_report2_saql_refresh.py`

- [ ] **Step 3.1: Create the script with the testable functions first**

Create `/Users/test/crm-analytics/scripts/run_report2_saql_refresh.py` with this exact content:

```python
#!/usr/bin/env python3
"""Re-run the SAQL queries embedded in each Sales Ops live_saql_validation.json
file and write the updated results back atomically.

Reads each input JSON, extracts every result's SAQL query, executes it against
the live org via Wave API, and rewrites the JSON with new sample_records,
row_count, status, and validated_at fields. Preserves the original query and
metric fields verbatim.

Auth via `sf org display`. No MCP, no Python builders touched.

Usage:
    python3 scripts/run_report2_saql_refresh.py \\
        --snapshot-date 2026-04-01 \\
        --target-org apro@simcorp.com \\
        --json
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TARGET_ORG = "apro@simcorp.com"
WAVE_API_VERSION = "v66.0"

# Inputs the deck builder consumes — see output/sales_ops_quarterly_deck_2026-03-31/build_sales_ops_quarterly_deck.js
DEFAULT_INPUT_PATHS = [
    REPO_ROOT / "output" / "sales_ops_page1_mutation_prep_2026-03-31" / "live_saql_validation" / "live_saql_validation.json",
    REPO_ROOT / "output" / "sales_ops_page4_mutation_prep_2026-03-31" / "live_saql_validation" / "live_saql_validation.json",
    REPO_ROOT / "output" / "sales_ops_page5_mutation_prep_2026-03-31" / "live_saql_validation" / "live_saql_validation.json",
    REPO_ROOT / "output" / "sales_ops_page6_mutation_prep_2026-03-31" / "live_saql_validation" / "live_saql_validation.json",
]


def load_validation_json(path: Path) -> dict[str, Any]:
    """Load a live_saql_validation.json file from disk."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def merge_query_results(
    original: dict[str, Any],
    new_results_by_alias: dict[str, list[dict[str, Any]]],
    *,
    validated_at: str,
) -> dict[str, Any]:
    """Merge new SAQL results into the original validation payload.

    For each result entry, looks up the new sample_records by step_alias.
    If found, updates sample_records, row_count, and sets status='ok'.
    If not found, leaves sample_records as-is and sets status='error'.
    Preserves query and metric fields verbatim.
    Sets payload-level validated_at.
    Does NOT mutate the input dict.
    """
    merged = copy.deepcopy(original)
    merged["validated_at"] = validated_at
    for entry in merged["results"]:
        alias = entry["step_alias"]
        if alias in new_results_by_alias:
            new_records = new_results_by_alias[alias]
            entry["sample_records"] = new_records
            entry["row_count"] = len(new_records)
            entry["status"] = "ok"
        else:
            entry["status"] = "error"
    return merged


def get_auth(target_org: str) -> tuple[str, str]:
    """Return (access_token, instance_url) from `sf org display`."""
    proc = subprocess.run(
        ["sf", "org", "display", "--target-org", target_org, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout)
    result = payload.get("result", {})
    token = result.get("accessToken")
    instance = result.get("instanceUrl")
    if not token or not instance:
        raise RuntimeError(f"sf org display did not return accessToken and instanceUrl for {target_org}")
    return token, instance


def execute_saql(query: str, *, access_token: str, instance_url: str) -> list[dict[str, Any]]:
    """Execute one SAQL query via Wave API and return its records."""
    url = f"{instance_url}/services/data/{WAVE_API_VERSION}/wave/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    response = requests.post(url, headers=headers, json={"query": query}, timeout=120)
    response.raise_for_status()
    payload = response.json()
    return payload.get("results", {}).get("records", [])


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON to path atomically via tempfile + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=False)
            f.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def refresh_one_file(
    path: Path,
    *,
    access_token: str,
    instance_url: str,
    validated_at: str,
) -> dict[str, Any]:
    """Refresh a single live_saql_validation.json. Returns a per-file summary."""
    original = load_validation_json(path)
    new_by_alias: dict[str, list[dict[str, Any]]] = {}
    failures: list[dict[str, str]] = []
    for entry in original["results"]:
        alias = entry["step_alias"]
        query = entry["query"]
        try:
            records = execute_saql(query, access_token=access_token, instance_url=instance_url)
            new_by_alias[alias] = records
        except Exception as e:
            failures.append({"step_alias": alias, "error": str(e)})
    merged = merge_query_results(original, new_by_alias, validated_at=validated_at)
    atomic_write_json(path, merged)
    return {
        "path": str(path),
        "step_count": len(original["results"]),
        "refreshed_count": len(new_by_alias),
        "failure_count": len(failures),
        "failures": failures,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Snapshot date in YYYY-MM-DD format. Used as validated_at in the output JSONs.",
    )
    parser.add_argument(
        "--target-org",
        default=DEFAULT_TARGET_ORG,
        help=f"sf CLI target org alias. Defaults to {DEFAULT_TARGET_ORG}.",
    )
    parser.add_argument(
        "--input",
        action="append",
        type=Path,
        default=None,
        help="Input live_saql_validation.json path. Repeat for multiple. Defaults to all 4 Sales Ops pages.",
    )
    parser.add_argument("--json", action="store_true", help="Print a JSON summary payload.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inputs = args.input if args.input else DEFAULT_INPUT_PATHS
    access_token, instance_url = get_auth(args.target_org)
    summaries: list[dict[str, Any]] = []
    overall_failures = 0
    for path in inputs:
        summary = refresh_one_file(
            path,
            access_token=access_token,
            instance_url=instance_url,
            validated_at=args.snapshot_date,
        )
        summaries.append(summary)
        overall_failures += summary["failure_count"]
    result = {
        "artifact_type": "report2_saql_refresh_summary",
        "snapshot_date": args.snapshot_date,
        "target_org": args.target_org,
        "input_count": len(inputs),
        "total_failures": overall_failures,
        "files": summaries,
    }
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        for s in summaries:
            print(f"{s['path']}: {s['refreshed_count']}/{s['step_count']} steps refreshed, {s['failure_count']} failures")
        print(f"TOTAL FAILURES: {overall_failures}")
    return 0 if overall_failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3.2: Make the script executable**

Run:

```bash
chmod +x /Users/test/crm-analytics/scripts/run_report2_saql_refresh.py
```

- [ ] **Step 3.3: Run the unit tests to verify they pass**

Run:

```bash
cd /Users/test/crm-analytics && python3 -m pytest tests/test_run_report2_saql_refresh.py -v 2>&1 | tail -30
```

Expected: 5 tests pass:

```
test_load_validation_json_returns_expected_structure PASSED
test_merge_query_results_updates_sample_records_and_row_count PASSED
test_merge_query_results_marks_missing_aliases_as_error PASSED
test_merge_query_results_preserves_query_and_metric_fields PASSED
test_merge_query_results_does_not_mutate_input PASSED
```

If any test fails, read the failure, fix the script (NOT the test), and re-run.

- [ ] **Step 3.4: Smoke-test the script's CLI parsing without hitting the org**

Run:

```bash
python3 /Users/test/crm-analytics/scripts/run_report2_saql_refresh.py --help 2>&1 | head -20
```

Expected: argparse help output mentioning `--snapshot-date`, `--target-org`, `--input`, `--json` flags. No errors.

- [ ] **Step 3.5: Commit the test fixture, test file, and script**

Run:

```bash
cd /Users/test/crm-analytics && \
git add tests/fixtures/sales_ops_live_saql_validation_sample.json \
        tests/test_run_report2_saql_refresh.py \
        scripts/run_report2_saql_refresh.py && \
git status tests/ scripts/run_report2_saql_refresh.py
```

Expected: three new files staged, no other changes.

```bash
cd /Users/test/crm-analytics && git commit -m "$(cat <<'EOF'
feat: add run_report2_saql_refresh.py for Sales Ops SAQL refresh

New helper script that re-executes the SAQL queries embedded in each
Sales Ops live_saql_validation.json file (pages 1, 4, 5, 6) against the
live org via Wave API and writes the refreshed results back atomically.

Used by the new Report 2 quarterly wrapper to refresh the deck inputs
before each build. TDD: 5 unit tests cover the merge logic and JSON
roundtrip; HTTP layer is integration-tested via the wrapper.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 2: Build the Report 2 Quarterly Wrapper Script

### Task 4: Create the Report 2 wrapper

**Files:**

- Create: `scripts/run_report2_quarterly_default.sh`

- [ ] **Step 4.1: Create the wrapper script**

Create `/Users/test/crm-analytics/scripts/run_report2_quarterly_default.sh` with this exact content:

```bash
#!/usr/bin/env bash
# Operator wrapper for Report 2 (Sales Ops Quarterly).
#
# Refreshes the Sales Ops SAQL inputs, builds the deck via pptxgenjs, exports
# to PDF via Microsoft PowerPoint, generates a montage, writes a Quick Look
# thumbnail, and prints a per-run summary.
#
# Mirrors the operator pattern of scripts/run_report1_monthly_default.sh.
#
# Usage:
#   scripts/run_report2_quarterly_default.sh default --snapshot-date 2026-04-01 --json

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE="$ROOT_DIR/output/sales_ops_quarterly_deck_2026-03-31"
BUILD_SCRIPT="$WORKSPACE/build_sales_ops_quarterly_deck.js"
SAQL_REFRESH="$ROOT_DIR/scripts/run_report2_saql_refresh.py"
EXPORT_HELPER="$ROOT_DIR/scripts/export_powerpoint_pdf.py"
RENDER_SCRIPT="$ROOT_DIR/output/sales_director_monthly_deck_2026-03-31/scripts/render_slides.py"
MONTAGE_SCRIPT="$ROOT_DIR/output/sales_director_monthly_deck_2026-03-31/scripts/create_montage.py"
SLIDES_VENV_PY="$ROOT_DIR/.venv_slides/bin/python"
TARGET_ORG="${REPORT2_TARGET_ORG:-apro@simcorp.com}"

usage() {
  cat <<'EOF'
Usage:
  scripts/run_report2_quarterly_default.sh <mode> [options...]

Modes:
  default   Run the main Sales Ops quarterly deck flow.

Options:
  --snapshot-date YYYY-MM-DD   Defaults to today (UTC date).
  --output-dir PATH            Defaults to output/sales_ops_quarterly_runs/<runid>.
  --skip-saql-refresh          Skip the SAQL refresh phase (use existing JSONs as-is).
  --json                       Print a JSON summary at the end.

Examples:
  scripts/run_report2_quarterly_default.sh default --snapshot-date 2026-04-01 --json
EOF
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

MODE="$1"
shift

case "$MODE" in
  -h|--help|help)
    usage
    exit 0
    ;;
  default)
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    usage >&2
    exit 1
    ;;
esac

SNAPSHOT_DATE="$(date -u +%Y-%m-%d)"
OUTPUT_DIR=""
SKIP_SAQL_REFRESH=0
WANT_JSON=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot-date)
      SNAPSHOT_DATE="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --skip-saql-refresh)
      SKIP_SAQL_REFRESH=1
      shift
      ;;
    --json)
      WANT_JSON=1
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

RUN_DATE="$(date -u +%Y-%m-%d)"
if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="$ROOT_DIR/output/sales_ops_quarterly_runs/${RUN_DATE}T_refresh_snapshot_${SNAPSHOT_DATE}"
fi
mkdir -p "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/powerpoint_review"
mkdir -p "$OUTPUT_DIR/ql_thumb"

DECK_OUT="$OUTPUT_DIR/sales_ops_quarterly_review_${SNAPSHOT_DATE}.pptx"
SUMMARY_OUT="$OUTPUT_DIR/sales_ops_quarterly_review_${SNAPSHOT_DATE}.summary.json"
PDF_OUT="$OUTPUT_DIR/powerpoint_review/sales_ops_quarterly_review_${SNAPSHOT_DATE}.pdf"
RENDERED_DIR="$OUTPUT_DIR/powerpoint_review/rendered"
MONTAGE_OUT="$OUTPUT_DIR/powerpoint_review/montage.png"
QL_THUMB_OUT="$OUTPUT_DIR/ql_thumb/sales_ops_quarterly_review_${SNAPSHOT_DATE}.pptx.png"

PHASE_PKILL_STATUS="ok"
PHASE_AUTH_STATUS="ok"
PHASE_SAQL_STATUS="skipped"
PHASE_BUILD_STATUS="pending"
PHASE_EXPORT_STATUS="pending"
PHASE_MONTAGE_STATUS="pending"
PHASE_THUMB_STATUS="pending"
EXPORT_RETRIED=0

# --- Phase 0: preflight ---
pkill -9 'Microsoft PowerPoint' 2>/dev/null || true

if ! sf org display --target-org "$TARGET_ORG" --json > /dev/null 2>&1; then
  PHASE_AUTH_STATUS="fail"
  echo "ERROR: sf org display failed for $TARGET_ORG" >&2
  exit 1
fi

# --- Phase 1: SAQL refresh ---
if [[ "$SKIP_SAQL_REFRESH" -eq 0 ]]; then
  if python3 "$SAQL_REFRESH" --snapshot-date "$SNAPSHOT_DATE" --target-org "$TARGET_ORG" --json > "$OUTPUT_DIR/saql_refresh_summary.json" 2> "$OUTPUT_DIR/saql_refresh_stderr.log"; then
    PHASE_SAQL_STATUS="ok"
  else
    PHASE_SAQL_STATUS="fail"
    echo "ERROR: SAQL refresh failed; see $OUTPUT_DIR/saql_refresh_stderr.log" >&2
    exit 2
  fi
fi

# --- Phase 2: build deck ---
cd "$WORKSPACE"
if node "$BUILD_SCRIPT" \
     --output "$DECK_OUT" \
     --snapshot-date "$SNAPSHOT_DATE" \
     --deck-title "Quarterly Sales Ops Review" \
     --summary-json "$SUMMARY_OUT" \
     > "$OUTPUT_DIR/build_stdout.log" 2> "$OUTPUT_DIR/build_stderr.log"; then
  PHASE_BUILD_STATUS="ok"
else
  PHASE_BUILD_STATUS="fail"
  echo "ERROR: deck build failed; see $OUTPUT_DIR/build_stderr.log" >&2
  exit 3
fi
cd "$ROOT_DIR"

# --- Phase 3: PowerPoint PDF export with retry-once ---
do_export() {
  python3 "$EXPORT_HELPER" \
    --input "$DECK_OUT" \
    --output "$PDF_OUT" \
    --timeout-seconds 90 \
    --json \
    > "$OUTPUT_DIR/export_stdout.log" 2> "$OUTPUT_DIR/export_stderr.log"
}

if do_export; then
  PHASE_EXPORT_STATUS="ok"
else
  EXPORT_RETRIED=1
  pkill -9 'Microsoft PowerPoint' 2>/dev/null || true
  sleep 5
  if do_export; then
    PHASE_EXPORT_STATUS="ok_after_retry"
  else
    PHASE_EXPORT_STATUS="warn"
  fi
fi

# --- Phase 4: PDF -> page images -> montage (only if export succeeded) ---
if [[ "$PHASE_EXPORT_STATUS" == "ok" || "$PHASE_EXPORT_STATUS" == "ok_after_retry" ]]; then
  if "$SLIDES_VENV_PY" "$RENDER_SCRIPT" "$PDF_OUT" --output_dir "$RENDERED_DIR" \
       > "$OUTPUT_DIR/render_stdout.log" 2> "$OUTPUT_DIR/render_stderr.log"; then
    if PYTHONPATH="$ROOT_DIR/output/sales_director_monthly_deck_2026-03-31/scripts" \
         "$SLIDES_VENV_PY" "$MONTAGE_SCRIPT" --input_dir "$RENDERED_DIR" --output_file "$MONTAGE_OUT" \
         > "$OUTPUT_DIR/montage_stdout.log" 2> "$OUTPUT_DIR/montage_stderr.log"; then
      PHASE_MONTAGE_STATUS="ok"
    else
      PHASE_MONTAGE_STATUS="warn"
    fi
  else
    PHASE_MONTAGE_STATUS="warn"
  fi
else
  PHASE_MONTAGE_STATUS="skipped"
fi

# --- Phase 5: Quick Look thumbnail ---
if qlmanage -t -s 1200 -o "$OUTPUT_DIR/ql_thumb" "$DECK_OUT" \
     > "$OUTPUT_DIR/ql_thumb_stdout.log" 2> "$OUTPUT_DIR/ql_thumb_stderr.log"; then
  PHASE_THUMB_STATUS="ok"
else
  PHASE_THUMB_STATUS="warn"
fi

# --- Summary ---
SUMMARY_JSON=$(cat <<EOF
{
  "artifact_type": "report2_quarterly_run_summary",
  "run_date": "$RUN_DATE",
  "snapshot_date": "$SNAPSHOT_DATE",
  "target_org": "$TARGET_ORG",
  "output_dir": "$OUTPUT_DIR",
  "deck_path": "$DECK_OUT",
  "deck_summary_path": "$SUMMARY_OUT",
  "pdf_path": "$PDF_OUT",
  "montage_path": "$MONTAGE_OUT",
  "ql_thumb_path": "$QL_THUMB_OUT",
  "phases": {
    "preflight_pkill": "$PHASE_PKILL_STATUS",
    "auth": "$PHASE_AUTH_STATUS",
    "saql_refresh": "$PHASE_SAQL_STATUS",
    "deck_build": "$PHASE_BUILD_STATUS",
    "powerpoint_export": "$PHASE_EXPORT_STATUS",
    "powerpoint_export_retried": $EXPORT_RETRIED,
    "powerpoint_montage": "$PHASE_MONTAGE_STATUS",
    "ql_thumb": "$PHASE_THUMB_STATUS"
  }
}
EOF
)

echo "$SUMMARY_JSON" > "$OUTPUT_DIR/run_summary.json"

if [[ "$WANT_JSON" -eq 1 ]]; then
  echo "$SUMMARY_JSON"
else
  echo "Run complete: $OUTPUT_DIR"
  echo "Deck:    $DECK_OUT"
  echo "PDF:     $PDF_OUT"
  echo "Montage: $MONTAGE_OUT"
  echo "Thumb:   $QL_THUMB_OUT"
  echo "Phases:  build=$PHASE_BUILD_STATUS export=$PHASE_EXPORT_STATUS montage=$PHASE_MONTAGE_STATUS thumb=$PHASE_THUMB_STATUS"
fi
```

- [ ] **Step 4.2: Make the wrapper executable**

Run:

```bash
chmod +x /Users/test/crm-analytics/scripts/run_report2_quarterly_default.sh
```

- [ ] **Step 4.3: Smoke-test the wrapper's `--help` output**

Run:

```bash
/Users/test/crm-analytics/scripts/run_report2_quarterly_default.sh --help 2>&1 | head -20
```

Expected: usage block listing `default` mode and `--snapshot-date`, `--output-dir`, `--skip-saql-refresh`, `--json` options. No errors.

- [ ] **Step 4.4: Verify the build script accepts `--summary-json`**

The wrapper passes `--summary-json` to `build_sales_ops_quarterly_deck.js`. Verify the build script accepts this flag.

Run:

```bash
grep -n "summary-json\|summaryJson" /Users/test/crm-analytics/output/sales_ops_quarterly_deck_2026-03-31/build_sales_ops_quarterly_deck.js | head -10
```

Expected: a match at line 124 referencing `args["summary-json"]`. This was pre-verified during plan authoring; the step exists as a sanity check in case the build script changes between plan and execution. If no matches are found, stop and report — the wrapper will need adjustment.

- [ ] **Step 4.5: Commit the wrapper**

Run:

```bash
cd /Users/test/crm-analytics && \
git add scripts/run_report2_quarterly_default.sh && \
git commit -m "$(cat <<'EOF'
feat: add run_report2_quarterly_default.sh wrapper

Operator wrapper for Report 2 (Sales Ops Quarterly) that mirrors the
Report 1 pattern: preflight pkill, sf auth check, SAQL refresh via the
new run_report2_saql_refresh.py helper, deck build via pptxgenjs,
PowerPoint PDF export with retry-once, PDF -> page images -> montage,
Quick Look thumbnail, and a per-run summary JSON.

Output dir defaults to output/sales_ops_quarterly_runs/<run-date>T_refresh_snapshot_<snapshot-date>/.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 3: Set up the Report 2 Run Directory Tree

### Task 5: Create the Report 2 runs directory

**Files:**

- Create: `output/sales_ops_quarterly_runs/.gitkeep`

- [ ] **Step 5.1: Create the directory and gitkeep**

Run:

```bash
mkdir -p /Users/test/crm-analytics/output/sales_ops_quarterly_runs && \
touch /Users/test/crm-analytics/output/sales_ops_quarterly_runs/.gitkeep
```

- [ ] **Step 5.2: Verify the .gitignore does not exclude this dir**

Run:

```bash
cd /Users/test/crm-analytics && git check-ignore -v output/sales_ops_quarterly_runs/.gitkeep 2>&1 || echo "NOT IGNORED (good)"
```

Expected: `NOT IGNORED (good)` — the .gitkeep should NOT match a gitignore pattern.

If it IS ignored, the runs dir is gitignored as a tree (which is fine for the actual run outputs, but we still want the empty directory tracked). In that case skip the `.gitkeep` and don't commit it; the directory will still be created by the wrapper at runtime. Move on to Step 5.3.

- [ ] **Step 5.3: Commit the .gitkeep IF not ignored**

If Step 5.2 said `NOT IGNORED (good)`:

```bash
cd /Users/test/crm-analytics && \
git add output/sales_ops_quarterly_runs/.gitkeep && \
git commit -m "feat: scaffold output/sales_ops_quarterly_runs/ tree

New runs directory mirroring output/sales_director_monthly_runs/ for the
Report 2 quarterly wrapper. Run-specific subdirs are created at runtime.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

If Step 5.2 said the path is gitignored, skip this commit. The directory will exist on disk only.

---

## Phase 4: Execute Report 1

### Task 6: Run the Report 1 wrapper against snapshot 2026-04-01

**Files:**

- Created at runtime: `output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01/`

- [ ] **Step 6.1: Final preflight pkill before Report 1**

Run:

```bash
pkill -9 'Microsoft PowerPoint' 2>/dev/null || true
sleep 2
```

- [ ] **Step 6.2: Run the Report 1 wrapper**

Run:

```bash
cd /Users/test/crm-analytics && \
scripts/run_report1_monthly_default.sh default \
  --snapshot-date 2026-04-01 \
  --skip-validation \
  --output-dir output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01 \
  --json \
  > /tmp/report1_run_stdout.log 2> /tmp/report1_run_stderr.log &
WRAPPER_PID=$!
echo "Report 1 wrapper PID: $WRAPPER_PID"
wait $WRAPPER_PID
echo "Report 1 exit code: $?"
```

Expected: exit code 0.
Expected wall time: 3-7 minutes.

If exit code is non-zero, read `/tmp/report1_run_stderr.log` and `/tmp/report1_run_stdout.log` to diagnose. Common failure modes:

- `sf` token expired → re-auth and retry
- PowerPoint export timeout → check `output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01/powerpoint_review/` for partial artifacts
- SAQL query failure → check `output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01/snapshot_refresh_stderr.log`

- [ ] **Step 6.3: Verify Report 1 outputs exist**

Run:

```bash
RUN_DIR=/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01
ls "$RUN_DIR/sales_director_monthly_pipeline_insights_2026-04-01.pptx" && \
ls "$RUN_DIR/sales_director_monthly_pipeline_insights_2026-04-01.summary.json" && \
ls "$RUN_DIR/publish_checklist.md" && \
ls "$RUN_DIR/ql_thumb/" && \
echo "Report 1 outputs OK"
```

Expected: all paths exist, ending with `Report 1 outputs OK`.

If the pptx filename has a different snapshot date (e.g., 2026-03-31 instead of 2026-04-01), the wrapper did not honor `--snapshot-date`. Stop and inspect why.

- [ ] **Step 6.4: Read the Report 1 summary.json and verify quarter focus**

Run:

```bash
cat /Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01/sales_director_monthly_pipeline_insights_2026-04-01.summary.json
```

Verify these fields:

- `snapshot_date` = `"2026-04-01"`
- `quarter_focus` = `"Q1"` (since fiscal Q1 = Feb-Apr at SimCorp; 2026-04-01 is mid-Q1)
- `slide_count` >= 8
- `biggest_slipped_arr` should be **non-zero and likely larger than the 2026-03-31 baseline of ~52,952,540** (the whole point of the April-1 snapshot is to see post-month-end slip pressure)

If `quarter_focus` is "Q2" (wrong), the runner mis-resolved the fiscal quarter and the deck is incorrect. Stop and flag this — it likely needs a code fix in the snapshot refresher.

If `biggest_slipped_arr` is 0 or null, the SAQL queries did not find slipped data — possibly because the dataflow has not refreshed since 2026-04-01 closed. Note this in the post-run report.

- [ ] **Step 6.5: Read the Report 1 publish_checklist and confirm expected blockers**

Run:

```bash
cat /Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01/publish_checklist.md
```

Verify:

- 2 `blocked` items: `Finance churn overlay publishable` and `Slipped commentary publishable`
- The `Rendered validation bundle generated` row shows status `skipped` (NOT `pass`) because we passed `--skip-validation`
- The `PowerPoint-first review bundle generated` row shows `pass` or `warn` (warn is acceptable per the spec's failure handling)

If new blockers appear that did not exist on the 2026-03-31 baseline, note them in the post-run report — they indicate a regression.

---

## Phase 5: Execute Report 2

### Task 7: Run the Report 2 wrapper against snapshot 2026-04-01

**Files:**

- Created at runtime: `output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/`
- Modified at runtime: `output/sales_ops_pageN_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json` (N ∈ {1,4,5,6})

- [ ] **Step 7.1: Re-preflight pkill (Report 1 may have left PowerPoint state)**

Run:

```bash
pkill -9 'Microsoft PowerPoint' 2>/dev/null || true
sleep 2
```

- [ ] **Step 7.2: Run the Report 2 wrapper**

Run:

```bash
cd /Users/test/crm-analytics && \
scripts/run_report2_quarterly_default.sh default \
  --snapshot-date 2026-04-01 \
  --output-dir output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01 \
  --json \
  > /tmp/report2_run_stdout.log 2> /tmp/report2_run_stderr.log &
WRAPPER_PID=$!
echo "Report 2 wrapper PID: $WRAPPER_PID"
wait $WRAPPER_PID
echo "Report 2 exit code: $?"
```

Expected: exit code 0.
Expected wall time: 4-8 minutes (SAQL refresh dominates because each query is a Wave API round trip).

If exit code is non-zero:

- Exit code 1: `sf` auth failed; re-auth and retry
- Exit code 2: SAQL refresh failed; read `output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/saql_refresh_stderr.log`
- Exit code 3: deck build failed; read `output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/build_stderr.log`

- [ ] **Step 7.3: Verify Report 2 outputs exist**

Run:

```bash
RUN_DIR=/Users/test/crm-analytics/output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01
ls "$RUN_DIR/sales_ops_quarterly_review_2026-04-01.pptx" && \
ls "$RUN_DIR/run_summary.json" && \
ls "$RUN_DIR/saql_refresh_summary.json" && \
ls "$RUN_DIR/powerpoint_review/" && \
ls "$RUN_DIR/ql_thumb/" && \
echo "Report 2 outputs OK"
```

Expected: all paths exist, ending with `Report 2 outputs OK`.

The deck builder writes its own `<deck>.summary.json` next to the deck — verify it exists too:

```bash
ls "$RUN_DIR/sales_ops_quarterly_review_2026-04-01.summary.json" 2>/dev/null || echo "deck summary missing — check build_stdout.log"
```

- [ ] **Step 7.4: Read the Report 2 run_summary.json and verify all phases passed**

Run:

```bash
cat /Users/test/crm-analytics/output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/run_summary.json
```

Verify the `phases` block:

- `preflight_pkill` = `"ok"`
- `auth` = `"ok"`
- `saql_refresh` = `"ok"`
- `deck_build` = `"ok"`
- `powerpoint_export` = `"ok"` or `"ok_after_retry"` (warn is acceptable but flag it)
- `powerpoint_montage` = `"ok"` (warn or skipped is acceptable but flag it)
- `ql_thumb` = `"ok"`

- [ ] **Step 7.5: Read the SAQL refresh summary and verify zero failures**

Run:

```bash
cat /Users/test/crm-analytics/output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/saql_refresh_summary.json
```

Verify:

- `total_failures` = `0`
- `input_count` = `4`
- For each entry in `files`: `failure_count` = `0` and `refreshed_count` equals `step_count`

If `total_failures > 0`, list which step_aliases failed and why. A query failure means the underlying dataset has a schema change since 2026-03-31 — flag for follow-up but do not block the run (the deck still builds with the stale results for the failing steps; their `status` is now `error`).

- [ ] **Step 7.6: Read the deck summary and verify the four headline KPIs are populated**

Run:

```bash
cat /Users/test/crm-analytics/output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/sales_ops_quarterly_review_2026-04-01.summary.json
```

Verify the `key_metrics` block has non-null numeric values for:

- `data_completeness_score`
- `process_compliance_rate`
- `forecast_accuracy`
- `pipeline_hygiene_rate`
- `top_exception_queue_count`

Compare to the 2026-03-31 baseline you captured in Step 0.7. Deltas of 0–10 percentage points are normal for a 1-day shift. Deltas of >20 points or null values indicate either a meaningful org-state change or a SAQL refresh failure — flag in the post-run report.

---

## Phase 6: Post-Run Summary and Cleanup

### Task 8: Produce a unified post-run report

**Files:**

- Read only: both run dirs from Phases 4 and 5
- Create (optional): `output/2026-04-06T_kpi_refresh_post_run_summary.md` — operator-facing summary

- [ ] **Step 8.1: Collect both summary JSONs into one view**

Run:

```bash
echo "===== Report 1 summary ====="
cat /Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01/sales_director_monthly_pipeline_insights_2026-04-01.summary.json
echo ""
echo "===== Report 2 summary ====="
cat /Users/test/crm-analytics/output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/sales_ops_quarterly_review_2026-04-01.summary.json
echo ""
echo "===== Report 2 run summary ====="
cat /Users/test/crm-analytics/output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/run_summary.json
```

- [ ] **Step 8.2: Verify both .pptx files open without error in PowerPoint (manual check)**

This is a manual eyeball check, not automated:

```bash
open /Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01/sales_director_monthly_pipeline_insights_2026-04-01.pptx
open /Users/test/crm-analytics/output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/sales_ops_quarterly_review_2026-04-01.pptx
```

Look at slide 1 of each. The cover should show snapshot date 2026-04-01. If it shows 2026-03-31 the wrapper did not pass the date through correctly.

Close PowerPoint when done (the wrapper preflight pkill in any future run will close it anyway, but cleaner to close manually).

- [ ] **Step 8.3: Compute KPI deltas vs the 2026-03-31 baseline**

Compare these specific values manually using the summaries from Step 8.1 and the baselines you captured in Step 0.7:

| Metric                            | 2026-03-31 baseline | 2026-04-01 refresh | Delta  |
| --------------------------------- | ------------------- | ------------------ | ------ |
| Report 2: data_completeness_score | _from Step 0.7_     | _from new summary_ | _diff_ |
| Report 2: process_compliance_rate | _from Step 0.7_     | _from new summary_ | _diff_ |
| Report 2: forecast_accuracy       | _from Step 0.7_     | _from new summary_ | _diff_ |
| Report 2: pipeline_hygiene_rate   | _from Step 0.7_     | _from new summary_ | _diff_ |
| Report 1: biggest_slipped_arr     | _from Step 0.7_     | _from new summary_ | _diff_ |
| Report 1: biggest_gap_arr         | _from Step 0.7_     | _from new summary_ | _diff_ |

Expected: small deltas on data quality / process compliance / forecast accuracy (1-day shift). LARGER deltas (positive) on `biggest_slipped_arr` and possibly on `biggest_gap_arr` since the April-1 snapshot is the first time post-month-end slip pressure shows up.

If any delta is suspicious (sign reversal, magnitude > 50% on a stable KPI), flag in the post-run report.

- [ ] **Step 8.4: Decide whether to commit the modified live_saql_validation.json files**

The Report 2 SAQL refresh modified four `live_saql_validation.json` files in place. These now have `validated_at: "2026-04-01"` and updated `sample_records`. Decide:

- **Option A: commit them** as a fresh baseline. Rationale: future builds default to these inputs and will produce 2026-04-01-grounded decks.
- **Option B: don't commit them** and treat the 2026-03-31 versions as the canonical baseline. Rationale: keeps the named directory's date label honest.

Run:

```bash
cd /Users/test/crm-analytics && git status output/sales_ops_page1_mutation_prep_2026-03-31/live_saql_validation/ output/sales_ops_page4_mutation_prep_2026-03-31/live_saql_validation/ output/sales_ops_page5_mutation_prep_2026-03-31/live_saql_validation/ output/sales_ops_page6_mutation_prep_2026-03-31/live_saql_validation/
```

If the operator chooses Option A:

```bash
cd /Users/test/crm-analytics && \
git add output/sales_ops_page1_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json \
        output/sales_ops_page4_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json \
        output/sales_ops_page5_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json \
        output/sales_ops_page6_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json && \
git commit -m "$(cat <<'EOF'
chore: refresh Sales Ops live SAQL validation results to 2026-04-01

Updated sample_records, row_count, and validated_at fields on the four
Sales Ops live_saql_validation.json files via run_report2_saql_refresh.py
against snapshot date 2026-04-01. Underlying SAQL queries are unchanged.

This makes future Report 2 deck builds default to 2026-04-01-grounded
inputs without re-running the SAQL refresh.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

If Option B, leave them in the working tree as uncommitted modifications and note in the post-run report.

- [ ] **Step 8.5: Final task list**

Surface to the user:

1. **Both .pptx files** with their absolute paths
2. **KPI delta summary** table from Step 8.3
3. **Validation status** per phase (from `run_summary.json` for Report 2 and `publish_checklist.md` for Report 1)
4. **Carried-forward Report 1 blockers** (Finance churn pending, slipped commentary pending) — confirmed unchanged
5. **Any phase that ended in `warn`** with the path to the relevant log file
6. **Total wall time** (sum of Phase 4 and Phase 5 elapsed)
7. **What's NOT done** (per the spec): Finance churn unblock, slipped commentary unblock, Sales Ops dashboard `0FKTb0000000K5BOAU` promotion from dry_run, standard-reports link layer wiring (ADR-0001 follow-up #1)
8. **Whether the live SAQL JSONs were committed** (Option A) or left as working-tree modifications (Option B)

---

## Self-Review Checklist (run after completing all tasks)

- [ ] Both `.pptx` files exist in their `2026-04-06T_refresh_snapshot_2026-04-01/` dirs
- [ ] Both PowerPoint PDF exports completed (status `ok` or `ok_after_retry`)
- [ ] Both PowerPoint montages exist
- [ ] Both Quick Look thumbnails exist
- [ ] Report 1 publish_checklist still shows the same 2 blockers (Finance churn, slipped commentary), no new blockers
- [ ] Report 1 publish_checklist `Rendered validation bundle generated` row is `skipped` (because of `--skip-validation`)
- [ ] Report 2 SAQL refresh summary shows `total_failures: 0`
- [ ] Report 2 deck `key_metrics` block has non-null values for all four headline KPIs
- [ ] `quarter_focus` on Report 1 deck reads `Q1`
- [ ] No code touches to `build_*.py` files (verify with `git status build_*.py`)
- [ ] No new commits to origin (push was not requested)
- [ ] All five new files created in this plan are committed: `tests/fixtures/sales_ops_live_saql_validation_sample.json`, `tests/test_run_report2_saql_refresh.py`, `scripts/run_report2_saql_refresh.py`, `scripts/run_report2_quarterly_default.sh`, optionally `output/sales_ops_quarterly_runs/.gitkeep`
- [ ] All five unit tests in `tests/test_run_report2_saql_refresh.py` pass
- [ ] Spec coverage: every section of `docs/superpowers/specs/2026-04-06-kpi-reports-refresh-design.md` has a corresponding task above
