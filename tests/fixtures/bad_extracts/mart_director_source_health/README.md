# Negative-control fixtures — `mart_director_source_health`

Slice 6 of Track I. Per-director aggregate of source-quality health.

| Fixture                             | Defect introduced                                                     |
| ----------------------------------- | --------------------------------------------------------------------- |
| `duplicate_director.json`           | Same `(snapshot_date, run_id, director)` repeated — uniqueness break. |
| `buckets_exceed_source_count.json`  | `ok+warning+blocked > source_count` (5+1+1=7 vs 5) — wide-row check.  |
| `negative_source_count.json`        | `source_count` is `-1`.                                               |
| `negative_total_row_count.json`     | `total_row_count` is `-10`.                                           |
| `negative_total_finding_count.json` | `total_finding_count` is `-5`.                                        |
| `bad_snapshot_date.json`            | `snapshot_date` is `"2026/04/30"`.                                    |
| `empty_run_id.json`                 | `run_id` is `""`.                                                     |
| `missing_director.json`             | `director` column omitted entirely.                                   |

`good.json` carries 4 director rows mirroring the v20c shape (Adam
Steinhouse, Catherine Howard, Jesper Tyrer, plus the empty-string
"global" director).

The wide-row "buckets within source_count" check is the contract that
matters here — without it the warehouse could emit nonsensical
aggregates (e.g. 5 ok sources reported under a director with 3 actual
sources) and downstream BI would unknowingly multiply the gap.
