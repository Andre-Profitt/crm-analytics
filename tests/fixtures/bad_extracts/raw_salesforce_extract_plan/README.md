# Negative-control fixtures — `raw_salesforce_extract_plan`

Slice 4 of Track I. Mirror of `source_requirement_plan.json items[]` —
one row per `(requirement_id, territory, period_role, quarter_label)`
tuple.

| Fixture                      | Defect introduced                                           |
| ---------------------------- | ----------------------------------------------------------- |
| `unknown_source_system.json` | `source_system` is `"looker"` (only `salesforce` allowed).  |
| `unknown_source_type.json`   | `source_type` is `"tableau_view"`.                          |
| `unknown_scope.json`         | `scope` is `"regional"` (only `territory` / `global`).      |
| `unknown_period_role.json`   | `period_role` is `"next_quarter"`.                          |
| `unknown_status.json`        | `status` is `"in_progress"`.                                |
| `bad_quarter_label.json`     | `quarter_label` is `"Q5"` (regex `^Q[1-4]$`).               |
| `bad_snapshot_date.json`     | `snapshot_date` is `"2026-4-30"` (regex needs zero-padded). |
| `empty_requirement_id.json`  | `requirement_id` is `""` (must be non-empty).               |
| `missing_dataset.json`       | `dataset` column omitted entirely.                          |

Cross-field defects (Pandera frame-level checks; Frictionless Table
Schema can't express these):

| Fixture                                  | Defect introduced                                                               |
| ---------------------------------------- | ------------------------------------------------------------------------------- |
| `territory_scope_missing_territory.json` | `scope="territory"` but `territory` is null. Frame-level check rejects the row. |
| `territory_scope_missing_director.json`  | `scope="territory"` but `director` is empty.                                    |
| `configured_missing_source_id.json`      | `status="configured"` but `source_id` is empty. An extract-time silent failure. |

The `region` field is deliberately NOT part of the territory-scope
metadata-completeness check. v20c live evidence has `region` null on
every territory row because region is denormalized metadata joined in
downstream by the warehouse builder, not carried on the plan items
themselves. Tightening would create a false-fail against real data.

Two positive controls so the per-scope nullability split is exercised:

- `good.json` — two rows: `scope="territory"` (APAC pipeline_open) and
  `scope="global"` (pipeline_open_reference, with
  `territory`/`director`/`region` legitimately `null`).
- `good_missing_source_id.json` — one row with `status="missing_source_id"`
  and empty `source_id`/`source_label`. The schema deliberately does NOT
  enforce a min-length on those columns; the extract step records the
  empty row so downstream auditing emits a finding instead of dropping
  it silently.
