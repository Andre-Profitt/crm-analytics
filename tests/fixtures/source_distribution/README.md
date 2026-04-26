# Track D — hand-crafted distribution audit fixtures

Each `rows_*.json` is a minimal hand-crafted Salesforce list-view extract used
to drive one Track D failure mode. `seed_pipeline_open_apac_current_quarter.json`
is the matching baseline seed; tests pair it with rows that legitimately match
it (negative control) and with rows that breach a single distribution axis at
a time (the named scenarios below).

| Fixture                               | Failure mode it isolates                                                |
| ------------------------------------- | ----------------------------------------------------------------------- |
| `rows_normal_stage_mix.json`          | None — control case; current matches seed within `max_abs_share_delta`. |
| `rows_stage_5_missing.json`           | Stage 5 had >0 share in seed but 0 rows in current run.                 |
| `rows_territory_dropped.json`         | Territory category present in seed disappeared from current run.        |
| `rows_quarter_missing.json`           | Required `CloseQuarter` category produced 0 rows.                       |
| `rows_owner_concentration_spike.json` | Top owner now > `max_top_category_share`.                               |

Two scenarios reuse fixtures with different policies:

- **missing_distribution_seed** — `rows_normal_stage_mix.json` audited with
  `seed=None`. Should emit zero findings (seed-dependent axes skip; no
  required categories or concentration breach).
- **contract_opt_up_blocked** — `rows_stage_5_missing.json` audited with the
  same seed but a contract policy that escalates
  `disappeared_category_action` to `blocked`. Should emit a `high`-severity
  finding and a failed `stage_5_presence` slice sentinel.

Fixtures intentionally use 10–20 rows so the share arithmetic is obvious by
inspection. Real v20c-derived extracts are deliberately NOT the first Track D
fixture — minimal hand-crafted data proves intent more cleanly.
