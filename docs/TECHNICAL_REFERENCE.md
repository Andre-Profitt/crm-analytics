# CRM Analytics Technical Reference — Complete Knowledge Base

Compiled March 9, 2026 from: Dashboard JSON Guide, SAQL Guide, XMD Guide, REST API Guide, Bindings Guide, and production experience.

## 1. DASHBOARD STATE SCHEMA

Dashboard state is pure JSON, deployed via:

```
PATCH /services/data/v66.0/wave/dashboards/{id}
Body: {"state": { steps, widgets, gridLayouts, filters, dataSourceLinksInfo, parameters, widgetStyle }}
```

### Grid Layout

```json
{
  "gridLayouts": [
    {
      "name": "Default",
      "numColumns": 12,
      "rowHeight": "fine",
      "style": {
        "backgroundColor": "#FFFFFF",
        "gutterColor": "#C5D3E0",
        "cellSpacingX": 8,
        "cellSpacingY": 8
      },
      "pages": [
        {
          "label": "Page 1",
          "widgets": [
            {
              "name": "widget_1",
              "row": 0,
              "column": 0,
              "colspan": 6,
              "rowspan": 4
            }
          ]
        }
      ],
      "selectors": [],
      "version": 1
    }
  ]
}
```

### Widget Style (cascading: global → per-widget)

```json
"widgetStyle": {
  "backgroundColor": "#FFFFFF",
  "borderColor": "#E4E4E4",
  "borderEdges": ["all"],
  "borderRadius": 4,
  "borderWidth": 1
}
```

### Global Filters

```json
"filters": [{
  "label": "Region Filter",
  "fields": ["Region"],
  "operator": "in",
  "locked": false,
  "dataset": {"name": "DatasetName"},
  "value": ["West", "East"]
}]
```

Operators: `in`, `not in`, `matches`, `is null`, `is not null`, `==`, `!=`, `<`, `>`, `<=`, `>=`, `>=<=` (between)

### dataSourceLinksInfo (v57.0+)

```json
"dataSourceLinksInfo": { "enableAutomaticLinking": true }
```

Replaces deprecated `dataSourceLinks`. Auto-links datasets by matching field names.

---

## 2. STEP TYPES

### aggregateflex (RECOMMENDED — structured, safe)

```json
{
  "type": "aggregateflex",
  "query": {
    "measures": [
      ["count", "*"],
      ["sum", "Amount"]
    ],
    "groups": ["StageName"],
    "filters": [],
    "order": [[-1, { "ascending": false }]],
    "limit": 2000
  },
  "datasets": [
    { "name": "DatasetName", "url": "/services/data/v66.0/wave/datasets/ID" }
  ],
  "broadcastFacet": true,
  "selectMode": "singlerequired",
  "receiveFacetSource": { "mode": "all", "steps": [] }
}
```

### saql (raw query — use when aggregateflex can't express the logic)

```json
{
  "type": "saql",
  "query": "q = load \"Dataset\"; q = group q by 'Stage'; q = foreach q generate 'Stage', sum('Amount') as 'Total'; q = order q by 'Total' desc;",
  "strings": ["Stage"],
  "numbers": ["Total"],
  "groups": ["Stage"],
  "broadcastFacet": true
}
```

### soql (query Salesforce objects DIRECTLY — no dataset needed!)

```json
{
  "type": "soql",
  "query": "SELECT Name, Amount FROM Opportunity WHERE IsClosed = true",
  "strings": ["Name"],
  "numbers": ["Amount"],
  "groups": ["Name"]
}
```

### staticflex (hardcoded selector values)

```json
{
  "type": "staticflex",
  "values": [
    { "display": "Last 30 Days", "value": "30" },
    { "display": "Last 90 Days", "value": "90" }
  ],
  "broadcastFacet": true,
  "selectMode": "singlerequired",
  "start": { "display": ["Last 30 Days"], "value": ["30"] }
}
```

### Other step types

- `grain` — pre-aggregated/detail rows
- `source` — cogroup/union of other steps
- `apex` — custom Apex controller

---

## 3. WIDGET TYPES (16 total)

| Type            | Purpose                         | Key Parameters                                                  |
| --------------- | ------------------------------- | --------------------------------------------------------------- |
| `chart`         | Visualizations (27 chart types) | `step`, `visualizationType`, `columnMap`, `legend`, `trellis`   |
| `number`        | KPI tile                        | `step`, `measureField`, `compact`, `numberColor`, `numberSize`  |
| `comparetable`  | Table with sparklines/formulas  | `step`, `columns`, `columnProperties`                           |
| `table`         | Basic data table                | `step`, `columns`, `totals`, `pivoted`                          |
| `valuestable`   | Vertical KPI table              | `step`, `measureField`, `multiMetrics`                          |
| `listselector`  | Dropdown filter                 | `step`, `instant`, `itemsPerRow`                                |
| `pillbox`       | Pill toggle filter              | `step`, `instant`                                               |
| `dateselector`  | Date picker                     | `step`, `compact`, `absoluteModeEnabled`, `relativeModeEnabled` |
| `rangeselector` | Range slider                    | `step`, `instant`                                               |
| `globalfilters` | Global filter panel             | `step`, `instant`, `filterItemOptions`                          |
| `container`     | Groups child widgets            | `containedWidgets`                                              |
| `link`          | Navigation button               | `destination`, `destinationType`, `includeState`, `text`        |
| `text`          | Static text                     | `text`, `textAlignment`, `fontSize`, `textColor`                |
| `image`         | Static image                    | `image`, `fit`                                                  |
| `url`           | Embedded iframe                 | `url`, `videoSize`                                              |
| `filterpanel`   | Deprecated filter               | `step`, `instant`                                               |

---

## 4. VISUALIZATION TYPES (27 total)

### Charts requiring explicit columnMap

Safe with `{dimensionAxis, plots, trellis, split}`:

- `hbar`, `vbar`, `stackhbar`, `stackvbar`, `pie` (donut)

### Charts requiring columnMap: null (auto-detect only)

- `funnel`, `waterfall`, `stackwaterfall`, `treemap` — explicit keys CRASH these

### Charts with NO columnMap (auto-detect)

- `scatter`, `combo`, `time`, `time-bar`, `time-combo`, `heatmap`, `calheatmap`, `matrix`, `parallelcoords`, `line`, `area`, `stackarea`

### Special columnMap

- `flatgauge` / `polargauge`: `{trellis: [], plots: ["measure"]}` — no dimensionAxis
- `choropleth`: `{locations: ["geo"], color: ["measure"], trellis: [], dimensionAxis: ["geo"], plots: ["measure"]}`

### Additional types

- `hdot`, `vdot` — dot plots
- `pyramid`, `stackpyramid` — pyramid charts
- `rating` — star rating
- `pivottable` — pivot table (inside chart widget)

### CRITICAL columnMap RULES

1. **NEVER provide partial columnMap** — all 4 keys required or it crashes
2. `columnMap: null` = safe auto-detect fallback for ANY chart type
3. Funnel/waterfall/treemap crash even with all 4 standard keys — use null
4. Missing ANY ONE key → `_buildColorConfig` TypeError → white screen

---

## 5. SAQL REFERENCE

### Syntax Fundamentals

```saql
q = load "DatasetName";
q = filter q by 'Stage' == "Closed Won";
q = filter q by 'Amount' > 100000 && 'Region' == "AMER";
q = filter q by 'Type' in ["New", "Renewal"];
q = filter q by date('Year', 'Month', 'Day') in ["current quarter" .. "current quarter"];
q = group q by 'Owner';
q = group q by ('Year', 'Quarter');
q = group q by all;
q = group q by rollup('Type', 'Source');
q = foreach q generate 'Owner', sum('Amount') as 'Total', count() as 'Count';
q = order q by 'Total' desc;
q = limit q 2000;
```

### QUOTING RULES (the #1 bug source)

- **Single quotes** = field identifiers: `'StageName'`, `'Amount'`
- **Double quotes** = string literals: `"Closed Won"`, `"dataset_name"`
- **Inside aggregates**: `sum('Amount')` ← SINGLE QUOTES for field names
- **count()** takes ZERO arguments — `count('Amount')` is INVALID
- **Keywords MUST be lowercase**: `load`, `filter`, `group`, `foreach`, `order`, `limit`

### Aggregate Functions

| Function             | Syntax                                            | Notes                     |
| -------------------- | ------------------------------------------------- | ------------------------- |
| `sum`                | `sum('field')`                                    |                           |
| `avg`                | `avg('field')`                                    |                           |
| `count`              | `count()`                                         | NO arguments              |
| `min` / `max`        | `min('field')`                                    | Works on dimensions too   |
| `median`             | `median('field')`                                 |                           |
| `unique`             | `unique('field')`                                 | Count distinct            |
| `stddev` / `stddevp` | `stddev('field')`                                 | Sample / population       |
| `percentile_cont`    | `percentile_cont(0.95) within (order by 'field')` |                           |
| `regr_slope`         | `regr_slope('y', 'x')`                            | Native linear regression! |
| `regr_intercept`     | `regr_intercept('y', 'x')`                        |                           |
| `regr_r2`            | `regr_r2('y', 'x')`                               | R-squared                 |

### Window Functions

```saql
-- Running total
sum(sum('Amount')) over ([..0] partition by all order by ('Month')) as YTD

-- Moving average (3 periods)
avg(sum('Amount')) over ([-2..0] partition by all order by ('Month')) as MA3

-- Rank
rank() over ([..] partition by 'Region' order by sum('Amount') desc) as Rank

-- Previous period
sum(sum('Amount')) over ([-1..-1] partition by 'Quarter' order by ('Year')) as PrevYear

-- % of total
(sum('Amount') * 100) / sum(sum('Amount')) over ([..] partition by 'Year') as PctOfYear
```

Row range: `[..0]` = cumulative, `[-2..0]` = 3-row window, `[..]` = entire partition, `[-1..-1]` = prev row

### Timeseries

```saql
q = fill q by (dateCols=('Year', 'Month', "Y-M"));
q = timeseries q generate ('Revenue' as 'Forecast')
    with (dateCols=('Year', 'Month', "Y-M"), length=6, predictionInterval=0.95);
-- Outputs: Forecast, Forecast_high_95, Forecast_low_95
```

### Relative Date Filters (no hardcoding!)

```saql
q = filter q by date('Year', 'Month', 'Day') in ["current year" .. "current year"];
q = filter q by date('Year', 'Month', 'Day') in ["3 months ago" .. "current day"];
q = filter q by date('Year', 'Month', 'Day') in ["current quarter" .. "current quarter"];
```

### Joins

```saql
-- Inner cogroup (join)
result = cogroup a by 'AccountId', b by 'AccountId';
result = foreach result generate a.'Account', sum(a.'Amount') as 'Total', sum(b.'Activities') as 'Acts';

-- Left outer
result = cogroup a by 'Id' left, b by 'Id';

-- Semi-join (keep only matching)
a = join a by (id) semi, b by (id);

-- Anti-join (keep only non-matching)
a = join a by (id) anti, b by (id);
```

### Useful Functions

- `number_to_string('Amount', "$#,###.00")` — in-query formatting
- `coalesce(value1, value2)` — null handling
- `case when condition then value else default end` — conditional logic
- `rollup('A', 'B')` + `grouping('A')` — subtotals

### Limits

- Projection mode (no group): max 100 rows
- Aggregation mode (with group): max 10,000 rows

---

## 6. XMD REFERENCE

### API Endpoints

```
GET/PUT /services/data/v66.0/wave/datasets/{id}/versions/{vid}/xmds/user
```

Each PUT **overwrites** the entire XMD (not a merge).

### Dimension XMD (labels, colors, actions, record links)

```json
{
  "field": "Account.Name",
  "label": "Account",
  "linkTemplateEnabled": true,
  "linkTooltip": "Open record",
  "recordIdField": "Id",
  "recordDisplayFields": ["Name", "Account.Name", "Owner.Name"],
  "salesforceActionsEnabled": true,
  "salesforceActions": [],
  "members": [
    { "member": "Closed Won", "label": "Won", "color": "#04844B" },
    { "member": "Closed Lost", "label": "Lost", "color": "#D4504C" }
  ]
}
```

### Measure XMD (formatting)

```json
{
  "field": "Amount",
  "label": "Amount",
  "format": {
    "customFormat": "[\"$#,##0\",1]"
  }
}
```

Format patterns:
| Goal | customFormat |
|------|-------------|
| `$500,000` | `"[\"$###0\",1]"` |
| `$500,000.00` | `"[\"$###,#00.00\",1]"` |
| `45.5%` | `"[\"##.##%\",100]"` |
| Negative in parens | `"[\"$#,###.##;($#,###.##)\",1]"` |

### Color Mapping

Via `conditionalFormatting.chartColor`:

```json
"conditionalFormatting": {
  "chartColor": {
    "parameters": {
      "values": [
        {"formatValue": "#04844B", "value": "Won"},
        {"formatValue": "#D4504C", "value": "Lost"}
      ]
    },
    "referenceField": "Stage",
    "type": "categories"
  }
}
```

---

## 7. BINDINGS & FACETING

### Selection Bindings (in SAQL queries)

```
{{cell(stepName.selection, [0], "columnName")}}     — single cell from selection
{{column(stepName.selection, ["columnName"])}}        — all values from column
{{row(stepName.selection, [0])}}                      — entire row
{{coalesce(binding1, binding2)}}                      — first non-null
```

### Results Bindings

```
{{cell(stepName.result, [0], "columnName")}}          — cell from query results
{{column(stepName.result, ["columnName"])}}            — column from results
```

### receiveFacetSource (controls inter-widget filtering)

```json
"receiveFacetSource": {
  "mode": "all",        // all | none | include | exclude
  "steps": []           // only relevant for include/exclude
}
```

| Mode      | Behavior                                              |
| --------- | ----------------------------------------------------- |
| `all`     | Receives facets from ALL broadcasting steps (default) |
| `none`    | Ignores all facets (benchmark/fixed KPI pattern)      |
| `include` | Receives ONLY from named steps                        |
| `exclude` | Receives from all EXCEPT named steps                  |

### Step Properties

- `broadcastFacet: true` — step sends its selection to other steps
- `selectMode`: `none`, `single`, `singlerequired`, `multi`, `multirequired`
- `start` — default selection on dashboard load

### Binding Serialization Functions

| Function          | Output                           | Use Case                |
| ----------------- | -------------------------------- | ----------------------- |
| `.asString()`     | `"value"`                        | Text display, titles    |
| `.asObject()`     | `{"field": "value"}`             | Passing structured data |
| `.asEquality()`   | `'field' == "value"`             | SAQL filter equality    |
| `.asGrouping()`   | `'field'`                        | SAQL group by injection |
| `.asProjection()` | `'field' as 'field'`             | SAQL foreach injection  |
| `.asOrder()`      | `'field' asc`                    | SAQL order by injection |
| `.asRange()`      | `'field' >= 0 && 'field' <= 100` | Numeric range filter    |
| `.asDateRange()`  | `date('Y','M','D') in [...]`     | Date range filter       |

### Coalesce Filter Pattern (filter only when user selects)

```saql
q = filter q by 'Stage' in {{column(step_filter.selection, ["Stage"]).asEquality()}}
    || "all" in {{column(step_filter.selection, ["Stage"]).asEquality()}};
```

### Truly Isolated Benchmark Step (completely immune to all faceting)

```json
{
  "type": "aggregateflex",
  "broadcastFacet": false,
  "selectMode": "none",
  "receiveFacetSource": { "mode": "none", "steps": [] },
  "useGlobal": false,
  "query": { ... }
}
```

All four properties are required — missing any one allows facet leakage.

### Measure/Dimension Switching via staticflex + Bindings

```json
{
  "step_metric_toggle": {
    "type": "staticflex",
    "values": [
      { "display": "ARR", "value": "APTS_Opportunity_ARR__c" },
      { "display": "Count", "value": "count()" }
    ],
    "broadcastFacet": true,
    "selectMode": "singlerequired",
    "start": { "display": ["ARR"], "value": ["APTS_Opportunity_ARR__c"] }
  }
}
```

Then in SAQL step: `sum({{cell(step_metric_toggle.selection, [0], "value").asString()}})`

### Cross-Dataset Filtering via Bindings

When two steps use different datasets, faceting doesn't auto-link. Use explicit bindings:

```saql
-- Step on Dataset B, filtered by selection from Step A (Dataset A)
q = load "DatasetB";
q = filter q by 'AccountId' in {{column(step_on_A.selection, ["AccountId"]).asEquality()}};
```

---

## 8. CHART STYLING QUICK REFERENCE

### Universal Chart Properties

- `theme: "wave"` — on every chart
- `autoFitMode: "fit"` — on every chart
- `title: {label, fontSize: 14, subtitleLabel, subtitleFontSize: 11, align: "center"}`
- `exploreLink: true` — show explore icon
- `showActionMenu: true` — show action menu
- `applyConditionalFormatting: true` — enable XMD formatting

### Axis Configuration

```json
"measureAxis1": {
  "showTitle": true, "showAxis": true, "title": "Amount ($)",
  "sqrtScale": false, "customDomain": {"showDomain": false},
  "numberFormat": "$#,##0"
}
```

### Legend

```json
"legend": { "show": true, "showHeader": true, "position": "right-top", "inside": false, "customSize": "auto" }
```

### Trellis (Small Multiples)

```json
"trellis": { "enable": true, "showGridLines": true, "type": "auto", "chartsPerLine": 3 }
```

### Combo Chart

```json
"combo": {"plotConfiguration": [
  {"series": "sum_Amount", "chartType": "column"},
  {"series": "win_rate", "chartType": "line"}
]}
```

### Reference Lines

```json
"referenceLines": [{"value": 1000000, "label": "Target", "color": "#04844B"}]
```

---

## 9. ORG STATE (SimCorp — as of March 9, 2026)

### Key Assets

- **25 dashboards** in B2B_MA app (all created Mar 5-9, 2026)
- **25 datasets** (~347K total rows)
- **30 replicated datasets** (connected objects) — Account, Opportunity, Contact, Lead, Contract, Campaign, Task, Event, User, and more
- **13 dataflows** (5 custom with NO schedule, 8 legacy)
- **0 recipes**
- **Limits**: 17.8% row capacity used

### Data Volumes

- Opportunities: 47,748 (but 45% are "No Opportunity" stage — real pipeline ~1,714)
- Accounts: 13,491
- Leads: 51,465
- Contacts: 153,571
- Contracts: 17,553
- Products: 2,417

### Key Custom Fields

- `APTS_RH_Product_Family__c` — Product family
- `APTS_Forecast_ARR__c` — Forecast ARR
- `APTS_Opportunity_ARR__c` — Opportunity ARR
- `RH_PS_Annual_Recurring_Revenue_ARR__c` — PS ARR

### Critical Issues

- **10 datasets referenced by dashboards don't exist** (broken dashboards)
- **5 custom dataflows have no schedule** (manual-only)
- **pdMultiAttrib3 is 4.5 years stale**

---

## 10. DESIGN RULES (from research + production experience)

### Metric Classification (classify BEFORE building)

| Type               | Should React To              | Pattern                                               |
| ------------------ | ---------------------------- | ----------------------------------------------------- |
| Global benchmark   | Only date/org context        | `receiveFacetSource: {mode: "none"}`                  |
| Scoped KPI         | Selected dims (date, region) | `receiveFacetSource: {mode: "include", steps: [...]}` |
| Exploratory detail | All filters and interactions | `receiveFacetSource: {mode: "all"}`                   |

### Page Structure (progressive disclosure)

1. KPI strip — key numbers with variance
2. Primary visualization — answers the page's main question
3. Secondary drill — appears on selection
4. Action table — exception queue with record links

### Interaction Hierarchy (least disruptive first)

1. Facet/highlight — comparison without recalculation
2. Filter — when views should truly shrink
3. Toggle/parameter — swap measures, dimensions
4. Navigation — tier transitions
5. Record action — open Salesforce record

### Anti-Patterns

- Gauges → use bullet charts
- Donuts for comparison → use ranked bars or 100% stacked bars
- Summary-only widgets → add variance + sparkline
- Duplicate pages → use toggle/parameter controls
- Global filters on everything → scope with receiveFacetSource
