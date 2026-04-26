# Negative-control fixtures — `staged_source_quality_findings`

Hand-crafted bad inputs for the Track I schema's second slice. Same
pattern as `bad_extracts/raw_source_quality_audit/`: one defect per
file, paired with a single positive control.

| Fixture                                    | Defect introduced                                                             |
| ------------------------------------------ | ----------------------------------------------------------------------------- |
| `unknown_track.json`                       | `track` is `"X"` (must be in {B, C, unknown}).                                |
| `distribution_track_in_quality_table.json` | `track` is `"D"` — distribution findings belong in the other staged table.    |
| `unknown_severity.json`                    | `severity` is `"critical"` (must be in {high, medium, low, info}).            |
| `non_source_namespace_issue.json`          | `issue` is `"random_code"` (must match `^source_[a-z_]+$`).                   |
| `issue_with_uppercase.json`                | `issue` is `"source_BAD_CODE"` (regex requires lowercase + underscores only). |
| `issue_with_whitespace.json`               | `issue` is `"source_extract failed"` (regex rejects whitespace).              |
| `bad_snapshot_date.json`                   | `snapshot_date` is `"04/30/2026"` (must be YYYY-MM-DD).                       |
| `missing_run_id.json`                      | `run_id` column is omitted entirely.                                          |

`good.json` is a one-row positive control with a real-shape Track B
warning (zero-row fallback in Southern Europe forward_quarter — the
exact shape that flowed through the v20d checkpoint).

`distribution_track_in_quality_table.json` deserves the most callouts:
it pairs a forbidden `track="D"` with an `issue` that _would_ be valid
for this table on its own. Catching this in the schema is the contract
that prevents the warehouse from silently routing a finding into the
wrong table.
