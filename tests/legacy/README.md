# Legacy / quarantined tests

These tests are **not** part of the source-backed default lane. Default
`pytest tests/` skips this directory via `tests/conftest.py`:

```python
collect_ignore_glob = ["legacy/*"]
```

To run them explicitly:

```sh
pytest tests/legacy/
```

## Why each file is here

The quarantine separates two failure shapes that existed before Track A so
that CI can fail loudly on real source-backed regressions without hiding
them behind known-bad legacy noise.

### Collection errors — missing scripts

- `test_sales_director_monthly_master_builder.py`
  Imports `scripts.claude_office_etl`, which has been removed. The test
  file documents an integration with a no-longer-shipping ETL.
- `test_salesforce_report_executor_cli.py`
  Imports `scripts.salesforce_report_executor`, which was renamed/removed.

### Runtime failures — legacy lane being deprecated

- `test_run_monthly_director_review.py` (8 failing cases)
  Tests for `scripts/run_monthly_director_review.py`, the legacy monthly
  runner Track A explicitly froze behind `inputs.legacy_only=true` on the
  scheduled workflow. Failures are mostly fixture-path mismatches
  (`config/sd_monthly_territories.json` not present in the test temp dir)
  and would be repaired by either updating the fixture wiring or rewriting
  the tests against the new source-backed runner.
- `test_run_report_autopilot.py` (2 cases)
  Tests for the report-autopilot lane that pre-dates the source-backed
  pipeline.
- `test_validate_sharepoint_analysis_contract.py` (2 cases)
  Tests for the legacy SharePoint analysis contract validator.

### Runtime failures — missing scripts

- `test_profile_renewal_outcomes_cli.py`
- `test_profile_renewal_ownership_cli.py`
- `test_profile_retention_owner_validation_cli.py`
- `test_profile_retention_product_grain_cli.py`
- `test_profile_retention_semantics_cli.py`
- `test_profile_role_structure_cli.py`

  These reference `scripts/profile_*.py` modules that no longer exist
  (`FileNotFoundError` at test setup). Either restore the scripts or
  retire the test files; either way they are not blocking source-backed
  CI.

## What is NOT here

The source-backed lane keeps any test that exercises code we still ship,
even if it currently fails. This quarantine PR is **narrow**: it removes
files where the underlying module/script no longer exists OR the lane
itself is being deprecated. It does NOT promote every failing source-backed
test into legacy.

Specifically:

- `test_sales_director_monthly_cadence.py` — one failing case is a fixture
  brittleness (path expectation in
  `test_command_monthly_run_builds_region_and_global_release_inputs`),
  not a missing module. Stays in the default lane to be fixed.
- `test_source_bundles.py` / `test_salesforce_sources.py` — failures
  observed in this branch's transient state were caused by a DuckDB ≥ 1.5
  prepared-parameter bug in `scripts/monthly_platform/storage.py` that
  Track H's PR (#6) fixes; not legacy.
- `test_ai_os_browser_cli.py`, `test_analytics_intelligence.py`, and other
  source-backed tests with pre-existing failures — those exercise live code
  paths and need targeted fixes per-suite. They are out of scope for this
  PR. The quarantine's job was to remove **import-error-or-missing-script**
  noise so the _shape_ of source-backed failures is visible without being
  buried under ModuleNotFoundError / FileNotFoundError.

## How to retire a quarantined file

When the underlying script is restored or the test is rewritten against
the source-backed runner:

1. `git mv tests/legacy/<file>.py tests/<file>.py`
2. Run `pytest tests/<file>.py` to confirm green.
3. Update this README.

Track A's principle stands: the source-backed lane is the contract;
legacy is a holding bay until the corresponding tests are either retired
or rewritten.
