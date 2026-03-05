# SAQL (Salesforce Analytics Query Language) — Patterns & Pitfalls

Hard-won lessons from building 10 CRM Analytics dashboards programmatically.

## Field Name Quoting

- **Single quotes** = field identifiers (e.g., `'Field Name'`)
- **Double quotes** = string literals (e.g., `"value"`)
- **Bare field names** work for simple names without spaces (e.g., `Amount`, `StageName`)
- **CRITICAL:** Inside aggregate functions (`sum()`, `avg()`, `max()`, `min()`, `count()`), use **bare field names**, NOT single-quoted names. Single quotes inside aggregates cause: `"Wrong argument type: require field in sum"`

```saql
-- CORRECT
q = foreach q generate sum(Amount) as TotalAmount;

-- WRONG — causes "require field in sum"
q = foreach q generate sum('Amount') as TotalAmount;

-- OK for grouping/filtering (outside aggregates)
q = group q by 'StageName';
q = filter q by 'StageName' == "Closed Won";
```

## Window Functions

Window functions enable running totals, moving averages, and rankings directly in SAQL.

### Syntax

```saql
aggregate_function(aggregate_function(field)) over (
  [..0] partition by all order by (DimensionField)
)
```

### Key Rules

1. **Double-nested aggregates**: `sum(sum(Field))` — the inner aggregate matches the `foreach`, the outer is the window
2. **`[..0]`** not `[..]` — specifies cumulative window from start to current row
3. **`partition by all`** — required when there's no natural partition dimension
4. **Parenthesized order by**: `order by (MonthLabel)` — the dimension must be in parentheses
5. Bare field names inside the aggregates (no single quotes)

### Running Total (YTD)

```saql
q = load "Dataset";
q = filter q by IsForecast == "false";
q = group q by MonthLabel;
q = foreach q generate MonthLabel,
    sum(ClosedWonAmount) as WonAmount,
    sum(sum(ClosedWonAmount)) over ([..0] partition by all order by (MonthLabel)) as YTDWon;
q = order q by MonthLabel asc;
```

### 3-Month Moving Average

```saql
q = load "Dataset";
q = filter q by IsForecast == "false";
q = group q by MonthLabel;
q = foreach q generate MonthLabel,
    max(TotalPipeline) as Pipeline,
    avg(max(TotalPipeline)) over (
      [..0] partition by all order by (MonthLabel)
      rows between 2 preceding and current row
    ) as MA3;
q = order q by MonthLabel asc;
```

### Ranking

```saql
q = load "Dataset";
q = group q by (OwnerName, FiscalYear);
q = foreach q generate OwnerName, FiscalYear,
    sum(Amount) as TotalRevenue,
    rank() over ([..0] partition by FiscalYear order by sum(Amount) desc) as RepRank;
```

## Timeseries (Native Forecasting)

SAQL has built-in time series forecasting (Holt-Winters / Holt's Linear Trend).

### Syntax

```saql
q = fill q by (dateCols=('Date_Year', 'Date_Month', "Y-M"));
q = timeseries q generate 'Measure' as 'ForecastMeasure'
    with (length=6, dateCols=('Date_Year', 'Date_Month', "Y-M"), predictionInterval=95);
```

### Critical Requirements

1. **`fill` MUST precede `timeseries`** — fills gaps in the time series
2. **`dateCols` requires SEPARATE date part columns** — e.g., `Date_Year` (int) and `Date_Month` (int), NOT a combined `"2026-01"` string
3. The format string (`"Y-M"`) tells SAQL how to interpret the columns
4. **Output columns**: `ForecastMeasure`, `ForecastMeasure_high_95`, `ForecastMeasure_low_95`
5. Auto model selection: Holt-Winters for seasonal data, Holt's Linear Trend otherwise
6. `predictionInterval=95` gives 95% prediction intervals

### Workaround (when you don't have separate date columns)

If your dataset only has a combined month field like `MonthLabel` = `"2026-01"`, compute trendlines in Python and upload as dataset fields (`TrendPipeline`, `TrendPipelineUpper`, `TrendPipelineLower`). Then just query them:

```saql
q = load "Dataset";
q = group q by MonthLabel;
q = foreach q generate MonthLabel,
    max(TotalPipeline) as TotalPipeline,
    max(TrendPipeline) as TrendPipeline,
    max(TrendPipelineUpper) as TrendUpper,
    max(TrendPipelineLower) as TrendLower;
q = order q by MonthLabel asc;
```

## CASE Expressions

```saql
sum(case when IsWon == "true" then Amount else 0 end) as ClosedWon
```

- `case when ... then ... else ... end` — standard SQL-like syntax
- Can be nested inside aggregates
- String comparisons use double quotes: `== "true"`

## Coalesce Filters

For dashboard interactivity — filter only when user selects a value:

```saql
q = filter q by 'StageName' in {{column(step.selection, ["StageName"]).asEquality()}}
    || "all" in {{column(step.selection, ["StageName"]).asEquality()}};
```

Or using a helper function:

```python
def coalesce_filter(step_name, field, all_value='"all"'):
    return (
        f"q = filter q by '{field}' in "
        f'{{{{column({step_name}.selection, ["{field}"]).asEquality()}}}} '
        f"|| {all_value} in "
        f'{{{{column({step_name}.selection, ["{field}"]).asEquality()}}}};\n'
    )
```

## Common Patterns

### Aggregate with Multiple Measures

```saql
q = load "Dataset";
q = group q by StageName;
q = foreach q generate StageName,
    sum(Amount) as TotalAmount,
    count() as DealCount,
    avg(Amount) as AvgDeal,
    sum(Amount) / sum(Amount) as WinRate;
q = order q by TotalAmount desc;
q = limit q 10;
```

### Multi-Dataset Join (Compact Form)

```saql
a = load "Dataset1";
a = group a by Id;
a = foreach a generate Id, sum(Amount) as Total;
b = load "Dataset2";
b = group b by OppId;
b = foreach b generate OppId as Id, count() as Activities;
q = cogroup a by Id, b by Id;
q = foreach q generate a.Id, a.Total, b.Activities;
```

## Dashboard API

- **Endpoint**: `PATCH /services/data/v66.0/wave/dashboards/{DASHBOARD_ID}`
- **Auth**: Via Salesforce CLI (`sf org display --json` → access token + instance URL)
- **Payload**: Full dashboard JSON with `state` object containing `steps`, `widgets`, `layouts`
- **XMD**: Set via `PATCH /services/data/v66.0/wave/datasets/{id}/versions/{vid}/xmds/user` for record links, colors, formatting
