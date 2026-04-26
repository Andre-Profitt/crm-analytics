# Negative-control fixtures — `raw_source_quality_audit`

Hand-crafted bad inputs that the Pandera + Frictionless schemas must
reject. One fixture per intentional defect, following the same
"isolate one failure mode per file" pattern Track D established.

| Fixture                     | Defect introduced                                                     |
| --------------------------- | --------------------------------------------------------------------- |
| `wrong_quality_hash.json`   | `quality_hash` is the wrong length / not hex (must be 64-char SHA).   |
| `negative_row_count.json`   | `row_count` is negative (must be ≥ 0).                                |
| `unknown_status.json`       | `status` is `"???"` (must be in {ok, warning, blocked}).              |
| `missing_source_key.json`   | `source_key` field is omitted entirely.                               |
| `bad_period_role.json`      | `period_role` is `"future_quarter"` (only the 3 valid roles allowed). |
| `duplicate_source_key.json` | Two rows share the same `source_key` (must be unique).                |

`good.json` is a one-row positive control that all schemas must accept
unchanged. It mirrors a real pipeline_open APAC current-quarter source.

These are JSON fixtures (one row per file, or multiple rows for the
duplicate-key case). Tests load them, convert to a pandas DataFrame,
and assert `validate_pandera` returns `status=fail` with at least one
finding mentioning the targeted column or invariant.
