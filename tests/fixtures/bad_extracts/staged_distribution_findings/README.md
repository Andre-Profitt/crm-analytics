# Negative-control fixtures — `staged_distribution_findings`

Slice 3 of Track I. Same template as
`bad_extracts/staged_source_quality_findings/`, with the routing rule
inverted: this table accepts ONLY `track="D"` and ONLY issues in the
`source_distribution_*` namespace.

| Fixture                              | Defect introduced                                                          |
| ------------------------------------ | -------------------------------------------------------------------------- |
| `track_b_in_distribution_table.json` | `track` is `"B"` — Track B findings belong in the quality-findings table.  |
| `track_c_in_distribution_table.json` | `track` is `"C"` — Track C findings belong in the quality-findings table.  |
| `unknown_track.json`                 | `track` is `"X"` (must be exactly `"D"`).                                  |
| `unknown_severity.json`              | `severity` is `"critical"` (must be in {high, medium, low, info}).         |
| `non_distribution_issue.json`        | `issue` is `"source_extract_failed"` — Track B issue, wrong namespace.     |
| `issue_with_uppercase.json`          | `issue` is `"source_distribution_BAD"` (regex requires lowercase).         |
| `issue_with_whitespace.json`         | `issue` is `"source_distribution_share drift"` (regex rejects whitespace). |
| `bad_snapshot_date.json`             | `snapshot_date` is `"04/30/2026"` (must be YYYY-MM-DD).                    |
| `missing_run_id.json`                | `run_id` column omitted entirely.                                          |

`good.json` is a one-row positive control — a realistic
`source_distribution_share_drift` finding for the APAC `StageName`
dimension. It uses a numeric stage label (`5 - Preferred`) in the
evidence string and the issue regex `^source_distribution_[a-z0-9_]+$`
permits digits, so future sentinel-id-bearing issues (e.g.
`source_distribution_sentinel_stage_5_presence_failed`) parse cleanly
without a schema migration.

The cross-table contract is the most important guarantee here: the
two `track_b_in_distribution_table` and `track_c_in_distribution_table`
fixtures pair a forbidden `track` with an _otherwise valid_
`source_distribution_*` issue. Catching them is what prevents the
warehouse builder from silently routing a B/C finding into this table.
