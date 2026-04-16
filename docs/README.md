# CRM Analytics Developer Intelligence

Hard-won patterns and pitfalls from building 10 CRM Analytics dashboards programmatically via the Wave API.

## Documents

| Document                                              | Description                                                                                         |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| [columnMap Reference](columnmap-reference.md)         | The #1 crash source — which chart types need explicit columnMap, which need null, which auto-detect |
| [Chart Styling Reference](chart-styling-reference.md) | Styling parameters, axis config, gauge bands, combo plotConfiguration, universal properties         |
| [SAQL Patterns](saql-patterns.md)                     | Field quoting, window functions, timeseries forecasting, CASE expressions, coalesce filters         |
| [Sales Director And Sales Ops Reporting Spec](sales-director-and-sales-ops-reporting-spec.md) | Build contract for the Sales Director monthly deck and the Sales Ops quarterly dashboard plus deck. |
| [Sales Director Monthly Deck Handoff](SALES_DIRECTOR_MONTHLY_DECK_HANDOFF_2026-03-31.md) | Restart-safe handoff for the Report 1 deck lane, including latest artifacts, branding status, current execution tranche, and the session-sensitive Downloads access note. |
| [Sales Director Monthly Deck Execution Plan](2026-04-01-sales-director-monthly-deck-execution-plan.md) | 50-step execution plan for taking the Report 1 monthly deck from runnable baseline to repeatable leadership-grade product. |
| [D1 MD-1 Preset Validation](audits/2026-04-09-d1-md1-preset-validation.md) | Live proof that the current 8-widget D1 source set executes cleanly across all 9 MD-1 territory presets, including the North America split. |
| [Sales Director Default Operator Flow](2026-04-01-sales-director-default-operator-flow.md) | Locked default operating path for the Report 1 monthly PowerPoint lane, including the primary baseline, repeatability proof run, and wrapper entrypoint modes. |
| [Sales Director Operator Publish Contract](2026-04-01-sales-director-operator-publish-contract.md) | Decision record for which monthly deck inputs stay live versus manual, plus the exact publish gate for the operator workflow. |
| [Sales Director Reference Pattern Audit](2026-04-01-sales-director-reference-pattern-audit.md) | Audit of recent SimCorp SharePoint decks used to decide which body-slide patterns are worth porting into the monthly Sales Director deck. |
| [Sales Director Autonomous Tightening Plan](2026-04-01-sales-director-autonomous-tightening-plan.md) | Authoritative 90-120 minute autonomous operating plan for the Report 1 monthly deck, including session-start protocol, tranche order, validation closeout, and restart-safe commands. |
| [Sales Directors Monthly Deck Workspace](../output/sales_director_monthly_deck_2026-03-31/README.md) | Reusable Report 1 deck workspace with the live snapshot refresher, deck generator, one-command monthly runner, and optional overlay contract for Finance churn and slipped-deal commentary. |
| [Sales Ops Dashboard Implementation Contract](sales-ops-dashboard-implementation-contract.md) | Page-by-page implementation contract for the quarterly Sales Ops dashboard, grounded in live field validation. |
| [Sales Ops Page 1 Quarterly Summary Contract](sales-ops-page1-quarterly-summary-contract.md) | Exact Page 1 executive landing-page contract for the quarterly Sales Ops dashboard, grounded in the validated page seams. |
| [Sales Ops Page 2 And Page 3 Step Reuse Contract](sales-ops-page23-step-reuse-contract.md) | Exact step reuse map for the data-quality and process-compliance pages, including the small missing SAQL additions. |
| [Sales Ops Page 3 Process Compliance Contract](sales-ops-page3-process-compliance-contract.md) | Exact Page 3 KPI and queue contract for process compliance, grounded in the live Commercial Rhythm export. |
| [Sales Ops Page 4 Forecast Accuracy Contract](sales-ops-page4-forecast-accuracy-contract.md) | Exact Page 4 KPI, bridge, weekly trend, and action-queue contract grounded in the fresh live Forecast & Revenue Motions export. |
| [Sales Ops Page 5 Pipeline Hygiene Contract](sales-ops-page5-pipeline-hygiene-contract.md) | Exact Page 5 KPI and queue contract for pipeline hygiene, grounded in the fresh live pipeline-operations export. |
| [Sales Ops Page 6 Action Queue Contract](sales-ops-page6-action-queue-contract.md) | Exact Page 6 normalized action-queue contract, grounded in the live account, rhythm, forecast, and pipeline seams. |
| [Sales Ops Page 1 Mutation Prep](generated/sales_ops_page1_mutation_prep/README.md) | Patch-ready Page 1 payload plus exact step contract for the quarterly Sales Ops landing page. |
| [Sales Ops Page 2 Mutation Prep](generated/sales_ops_page2_mutation_prep/README.md) | Patch-ready Page 2 payload plus exact step contract for the first real Sales Ops dashboard mutation slice. |
| [Sales Ops Page 3 Mutation Prep](generated/sales_ops_page3_mutation_prep/README.md) | Patch-ready Page 3 payload plus exact step contract for the rhythm-backed process-compliance slice. |
| [Sales Ops Page 4 Mutation Prep](generated/sales_ops_page4_mutation_prep/README.md) | Patch-ready Page 4 payload plus exact step contract for the forecast-trust and promotion-pressure slice. |
| [Sales Ops Page 5 Mutation Prep](generated/sales_ops_page5_mutation_prep/README.md) | Patch-ready Page 5 payload plus exact step contract, live preview, and read-only SAQL validation for the pipeline-hygiene slice. |
| [Sales Ops Page 6 Mutation Prep](generated/sales_ops_page6_mutation_prep/README.md) | Patch-ready Page 6 payload plus exact step contract for the normalized action-queue and root-cause slice. |

## Quick Rules

1. **columnMap** — if you provide one, ALL 4 keys must be present (`dimensionAxis`, `plots`, `trellis`, `split`). Missing any one = white-screen crash.
2. **Funnel/waterfall/treemap** — always `columnMap: null`. Explicit keys crash them.
3. **SAQL field quoting** — bare names inside `sum()`, `avg()`, `max()`. Single quotes inside aggregates = "Wrong argument type" error.
4. **Window functions** — double-nested aggregates, `[..0]`, `partition by all`, parenthesized `order by`.
5. **Timeseries** — requires `fill` first, and separate Year/Month columns (not combined strings).
