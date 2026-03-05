# CRM Analytics Developer Intelligence

Hard-won patterns and pitfalls from building 10 CRM Analytics dashboards programmatically via the Wave API.

## Documents

| Document                                              | Description                                                                                         |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| [columnMap Reference](columnmap-reference.md)         | The #1 crash source — which chart types need explicit columnMap, which need null, which auto-detect |
| [Chart Styling Reference](chart-styling-reference.md) | Styling parameters, axis config, gauge bands, combo plotConfiguration, universal properties         |
| [SAQL Patterns](saql-patterns.md)                     | Field quoting, window functions, timeseries forecasting, CASE expressions, coalesce filters         |

## Quick Rules

1. **columnMap** — if you provide one, ALL 4 keys must be present (`dimensionAxis`, `plots`, `trellis`, `split`). Missing any one = white-screen crash.
2. **Funnel/waterfall/treemap** — always `columnMap: null`. Explicit keys crash them.
3. **SAQL field quoting** — bare names inside `sum()`, `avg()`, `max()`. Single quotes inside aggregates = "Wrong argument type" error.
4. **Window functions** — double-nested aggregates, `[..0]`, `partition by all`, parenthesized `order by`.
5. **Timeseries** — requires `fill` first, and separate Year/Month columns (not combined strings).
