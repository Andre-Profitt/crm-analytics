# v20d ETL spine freeze — release evidence summary

- **Snapshot:** 2026-04-30
- **Run id:** `v20d-etl-spine-freeze`
- **Source evidence:** rebuilt from `live-all-sources-pipeline-open-v20c` plan + audit (the same v20c live evidence the contract suite anchors against).
- **Status:** PASS — 7/7 tables validate.

## What this freeze proves

After Track I slices 1-7 landed (PRs #14, #15, #16, #17, #18, #19, #20) every warehouse table on the source-backed lane has a strict Pandera schema and a Frictionless Table Schema descriptor. The freeze runs the warehouse builder end-to-end against frozen v20c inputs, then validates every output Parquet with both validators. Result: 7/7 tables pass both, the parity report is green, and the column-by-column Parquet types match the contracts (see `observed_schemas.json` next to this doc).

This is the ETL 1.0 baseline. Subsequent milestones (Track E deck contract, Track F template-first builder, Track G full release pass) anchor against this freeze.

## Tables validated

| Table | Rows | Pandera | Frictionless |
| --- | ---: | --- | --- |
| `raw_salesforce_extract_plan` | 55 | pass | pass |
| `raw_source_quality_audit` | 55 | pass | pass |
| `staged_source_requirements` | 4 | pass | pass |
| `staged_source_quality_findings` | 1 | pass | pass |
| `staged_distribution_findings` | 0 | pass | pass |
| `mart_director_source_health` | 10 | pass | pass |
| `mart_source_run_summary` | 1 | pass | pass |

## Artifacts

```
output/source_backed_warehouse/2026-04-30/v20d-etl-spine-freeze/
  raw/salesforce_extract_plan.parquet
  raw/source_quality_audit.parquet
  staged/source_requirements.parquet
  staged/source_quality_findings.parquet
  staged/distribution_findings.parquet
  marts/director_source_health.parquet
  marts/source_run_summary.parquet
  warehouse_manifest.json
  parity_report.json
  dataframe_contract_report.json
  observed_schemas.json
```

## Notes

- The freeze surfaced one Frictionless gap: `staged_source_requirements.max_rows` is a nullable BIGINT in DuckDB and was not declared as missing-value-tolerant in the schema. Live data has `max_rows` null on all 4 requirements. Patched by adding `missingValues: ["", "nan", "NaN", "<NA>"]` at the schema level so the descriptor matches Pandera's permissive view of the field. No data shape change.
- The contract test suite (96 tests across 7 slices) was rerun after the schema patch and remains green.

## What this freeze does NOT cover (deliberately)

Per the milestone plan, the freeze is ETL only. It does not run the deck builder, deck contract, brand fingerprint, render gates, visual regression, release catalog, waivers, OpenLineage events, or SharePoint upload. Those are Milestones 1+ and start with Track E (`config/deck_contract.yaml`) on a single integration branch.
