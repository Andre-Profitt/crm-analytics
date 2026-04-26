# Negative-control fixtures — `staged_source_requirements`

Slice 5 of Track I. Flattened projection of
`config/monthly_source_requirements.json requirements[]`.

| Fixture                                | Defect introduced                                                       |
| -------------------------------------- | ----------------------------------------------------------------------- |
| `duplicate_requirement_id.json`        | Two rows share the same `requirement_id` (must be unique).              |
| `unknown_source_system.json`           | `source_system` is `"looker"` (only `salesforce`).                      |
| `unknown_source_type.json`             | `source_type` is `"tableau"`.                                           |
| `unknown_scope.json`                   | `scope` is `"team"` (only `territory` / `global`).                      |
| `negative_min_rows.json`               | `min_rows` is `-3` (must be ≥ 0).                                       |
| `zero_max_rows.json`                   | `max_rows` is `0` — RowCountPolicy validator requires ≥ 1 when present. |
| `negative_count.json`                  | `distribution_dimension_count` is `-1`.                                 |
| `empty_owner.json`                     | `owner` is `""` (must be non-empty).                                    |
| `missing_has_distribution_policy.json` | `has_distribution_policy` column omitted entirely.                      |

`good.json` carries two rows that exercise the nullable-bound case:

- `sd_pipeline_open` (territory scope, Track D opt-in: 3 dims, 1
  sentinel, `max_rows = None`).
- `sd_pipeline_open_reference` (global scope, no distribution policy,
  capped `max_rows = 50000`).
