# Negative-control fixtures — `mart_source_run_summary`

Slice 7 (final) of Track I. One row per (snapshot, run) tuple — the
top-level run summary mart that downstream BI / deck contract / waiver
work will read first.

| Fixture                                 | Defect introduced                                                   |
| --------------------------------------- | ------------------------------------------------------------------- |
| `duplicate_run.json`                    | Two rows for the same `(snapshot_date, run_id)` — uniqueness break. |
| `unknown_status.json`                   | `status` is `"in_progress"`.                                        |
| `buckets_exceed_source_count.json`      | `ok+warning+blocked > source_count`.                                |
| `high_medium_exceed_finding_count.json` | `high + medium > finding_count`.                                    |
| `baseline_high_exceeds_drift.json`      | `baseline_high_finding_count > baseline_drift_finding_count`.       |
| `distribution_high_exceeds_total.json`  | `distribution_high_finding_count > distribution_finding_count`.     |
| `negative_finding_count.json`           | `finding_count` is `-1`.                                            |
| `bad_generated_at.json`                 | `generated_at` is `"April 26, 2026"` (not ISO).                     |
| `missing_distribution_count.json`       | `distribution_finding_count` column omitted entirely.               |

`good.json` mirrors the v20c run summary shape: 55 sources, 1 medium
warning finding (Southern Europe forward_quarter zero-row fallback),
no Track C drift or Track D distribution findings.

The four "high subset of total" wide-row checks are the contract that
matters most here. Without them the warehouse could emit summaries
where high-severity counts exceed their parent total — every
downstream gate (release packet, deck contract, waiver evaluation)
would see corrupt severity arithmetic.
