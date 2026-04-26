# Track H — recorded follow-ups (do not start in this PR)

These tickets came out of the Track D activation review and the Track H
warehouse skeleton work. They are intentionally **not** scoped into this
PR — record them so they don't get lost, and pick them up in narrow
follow-up PRs.

## Track D escalation evaluation (post-v20d)

The first live `distribution_policy` opt-in (`sd_pipeline_open`) ships with
all actions defaulted to `info` while share envelopes mature. After the
first clean v20d evidence pass, evaluate escalating per dimension:

- `stage_5_presence` slice sentinel: `info → warning`.
- `StageName.disappeared_category_action`: keep `warning`.
- `ForecastCategoryName` missing `Pipeline`: consider `warning → blocked`
  (a sd_pipeline_open run with zero `Pipeline` rows is structurally
  broken regardless of territory size).

Acceptance: at least 2 monthly snapshots have been merged into the seeds
via repeated `--promote-baselines` runs, so `sample_count` per dimension
is meaningful.

## P1 — legacy / missing-module test quarantine

`tests/test_sales_director_monthly_master_builder.py` errors on collection
because `scripts.claude_office_etl` is missing. A handful of legacy
`run_monthly_director_review.py` and `run_report_autopilot.py` tests fail
with environment / fixture issues that pre-date Track A.

Goal: make "source-backed suite green" trivially separable from legacy
failures so a CI run can fail loudly on real regressions instead of
hiding them behind known-bad legacy noise.

Suggested approach (one PR):

- Add `tests/legacy/` directory and move the failing legacy tests into it.
- Add `pytest.ini` markers: `source_backed` (default) and `legacy`.
- Default `pytest` invocation excludes `legacy`; the legacy lane is
  invoked explicitly when needed.
- Either restore `scripts/claude_office_etl.py` or skip the test that
  imports it with `pytest.importorskip`.

## P2 — `extract_salesforce_sources.py` ruff E402 cleanup

The extractor has long-standing `E402` warnings because `sys.path` is
adjusted before the imports. Track A through Track H have all touched
this file; the central integration-point complexity makes the E402
wallpaper especially likely to mask a real import-order bug eventually.

Suggested approach:

- Move the `sys.path` shim into a small `scripts/_bootstrap.py` that the
  extractor imports first.
- Or: package the project so absolute imports work without the shim
  (would require `pyproject.toml`, which the repo does not yet have —
  bigger change, defer to Track L when reusable workflows arrive).

## DuckDB 1.5+ compatibility (incidentally fixed in Track H)

Installing `duckdb>=1.1.0` (which `requirements.txt` declared but the
environment hadn't materialized) surfaced a pre-existing bug in
`scripts/monthly_platform/storage.py:_register_duckdb_table`: it used
`read_parquet(?)` with a prepared parameter, which DuckDB 1.5+ rejects
for DDL statements. Fix landed alongside Track H since the warehouse
skeleton requires DuckDB to be installed; the storage.py fix is a one-line
inline-quote change with no caller-controlled SQL exposure.

## Things explicitly NOT to do in the next PR

- No Track I schemas (Pandera / JSON Schema).
- No `deck_contract.yaml`.
- No template-first builder.
- No OpenLineage events (Track J).
- No release waivers (Track K).
- No reusable workflows / composite actions (Track L).
- No "small" warehouse refactors that pull in deck or release logic — the
  warehouse is read-only of upstream evidence by design.
