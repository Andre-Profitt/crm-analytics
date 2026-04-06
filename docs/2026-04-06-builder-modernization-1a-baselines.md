# Builder Modernization 1A — Pre-Modernization Baselines

**Captured:** 2026-04-06 (direct dataset row counts via Wave Query API — no builders were executed)
**Purpose:** Sanity-check row counts for post-modernization runs in Phase 7 (pilot) and Phase 8 (subagent fan-out). Each builder's post-run `RunSummary.row_count` must be within ±10% of the baseline below to commit without coordinator review.
**Source of truth:** live `apro@simcorp.com` datasets (folder `B2B_MA`) as of the capture time.
**Note:** These counts reflect the LAST successful builder run before this iteration began (staleness per each dataset is documented in `docs/2026-04-06-builder-assessment.md`). The ±10% tolerance accounts for day-over-day drift between the baseline capture and the post-modernization run.
**Method:** `POST /services/data/v66.0/wave/query` with `q = load "<datasetId>/<currentVersionId>"; q = group q by all; q = foreach q generate count() as row_count;` per dataset.

| Dataset                         | Baseline row count | Dataset ID         | Current version ID | Builder                                         |
| ------------------------------- | ------------------ | ------------------ | ------------------ | ----------------------------------------------- |
| Commercial_Rhythm_Control_Tower | 9117               | 0FbTb000001ASVVKA4 | 0FcTb000008xw4zKAA | build_commercial_rhythm_control_tower.py        |
| Pipeline_Opportunity_Operations | 4224               | 0FbTb000001A0KjKAK | 0FcTb000008wmxJKAQ | build_pipeline_opportunity_operations.py        |
| Forecast_Revenue_Motions        | 11695              | 0FbTb000001A0NxKAK | 0FcTb0000092CEPKA2 | build_forecast_revenue_motions.py               |
| Revenue_Retention_Health        | 27578              | 0FbTb000001A8DRKA0 | 0FcTb000008xvk1KAA | build_revenue_retention_health.py               |
| Executive_Revenue_Source_Truth  | 2396               | 0FbTb000001AMrVKAW | 0FcTb0000092ChRKAU | scripts/build_source_truth_executive_revenue.py |
| Account_Intelligence            | 2216               | 0FbTb0000019o1pKAA | 0FcTb000008zX5hKAE | build_account_intelligence.py                   |
| Customer_Account_Health         | 604                | 0FbTb000001A0W1KAK | 0FcTb000008wAX3KAM | build_customer_account_health.py                |
| Forecast_Intelligence           | 787                | 0FbTb0000019oeXKAQ | 0FcTb0000092CPhKAM | build_forecasting.py                            |

## Delta tolerance rule

A post-modernization run is within tolerance if `abs(new - baseline) / baseline <= 0.10`. If the delta exceeds 10%, the subagent does NOT commit and reports to the coordinator for review. The coordinator may accept a larger delta with reasoning in the commit message (e.g., "Forecast_Revenue_Motions legitimately updated overnight; delta 14% reflects real data drift, not a bug").

## Per-dataset reasonability notes

- **Commercial_Rhythm_Control_Tower (9,117):** opportunity-grain dataset covering recent commercial activity. Stable over short windows.
- **Pipeline_Opportunity_Operations (4,224):** open pipeline by region/stage. Swings with deal close events, ±5% day-over-day is normal.
- **Forecast_Revenue_Motions (11,695):** per-opp rows + trend rows. Updates daily as reps adjust forecasts.
- **Revenue_Retention_Health (27,578):** account-grain with a long lookback. Should be the most stable of the 8.
- **Executive_Revenue_Source_Truth (2,396):** executive rollup, fewer rows, tight FY filter.
- **Account_Intelligence (2,216):** strategic account subset.
- **Customer_Account_Health (604):** customer-only slice; smallest of the 8.
- **Forecast_Intelligence (787):** capped at `LIMIT 1000` in the SOQL (known silent truncation per the assessment) — post-run should be close to 787 or possibly still at the 1000 cap.
