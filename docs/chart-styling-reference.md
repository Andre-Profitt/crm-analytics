# CRM Analytics Chart Styling Reference

## Chart Types & columnMap Safety

**Require explicit columnMap:** hbar, column, donut, stackhbar, stackcolumn, pie, vbar, stackvbar
**Auto-detect (no columnMap):** timeline, scatter, line, area, stackarea, bubble, sankey, heatmap, bullet
**columnMap: null (force auto):** funnel, waterfall, treemap
**Special columnMap:** gauge `{trellis:[], plots:[field]}`, choropleth `{locations, color, trellis, dimensionAxis, plots}`
**combo:** requires columnMap + `combo.plotConfiguration` array (not `measures` dict)

## Key Styling Parameters

| Parameter                    | Description                                                                         | Chart Types                                                               |
| ---------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `applyConditionalFormatting` | Enable XMD-defined conditional formatting                                           | column, hbar, stack\*, donut, comparisontable, line, area, combo, scatter |
| `showValues`                 | Data labels on bars/slices                                                          | hbar, column, stack\*, donut, pie, waterfall                              |
| `normalize`                  | 100% stacked (all bars sum to 100%)                                                 | stack\* types                                                             |
| `referenceLines`             | Array of `{value, label, color}` threshold lines                                    | bar, line, waterfall                                                      |
| `axisMode`                   | `"sync"` (bullet), `"multi"` (dual-axis combo), `"single"` (default)                | bullet, combo                                                             |
| `tooltip`                    | `{showDimensions, showMeasures, showPercentage, customizeTooltip}`                  | all chart types                                                           |
| `trellis`                    | Small multiples: `{enable, type, chartsPerLine}`                                    | all major types                                                           |
| `widgetStyle`                | Container: `{backgroundColor, borderColor, borderEdges, borderRadius, borderWidth}` | all widgets                                                               |

## Axis Configuration

- `measureAxis1`: `{showAxis, showTitle, title, sqrtScale, customDomain: {showDomain, low, high}}`
- `measureAxis2`: same schema (for dual-axis combo, scatter Y-axis)
- `dimensionAxis`: `{showAxis, showTitle, title, customSize: "auto", icons: {useIcons, iconProps: {fit, column, type}}}`

## Gauge Bands

```json
"gauge": {"min": 0, "max": 100, "bands": [
  {"start": 0, "stop": 40, "color": "#D4504C"},
  {"start": 40, "stop": 70, "color": "#FFB75D"},
  {"start": 70, "stop": 100, "color": "#04844B"}
]}
```

## combo.plotConfiguration

```json
"combo": {"plotConfiguration": [
  {"series": "sum_Amount", "chartType": "column"},
  {"series": "cnt", "chartType": "line"}
]}
```

Valid chartType values: "column", "bar", "line", "area"

## Colors

- Per-series colors → XMD `dimensions[].members[].color` (not dashboard JSON)
- Choropleth: `lowColor`, `highColor` on widget
- Number formatting → XMD `measures[].format.customFormat: ["#,##0", 1]`

## Universal Chart Properties

These are applied to every chart in our dashboard builders:

- `theme: "wave"` — universally on every chart
- `autoFitMode: "fit"` — universally on every chart
- Title: `fontSize: 14`, `subtitleFontSize: 11`, `align: "center"` universally

## Production Dashboard Observations (4 dashboards, 187 widgets)

- `bullet`: 9 instances — actual vs target KPIs, `axisMode: "sync"`
- `sankey`: 4 instances — stage-to-outcome flow
- `heatmap`: 8 instances — 2D cross-tabulation
- `area`: 9 instances — running totals
- `bubble`: distinct from scatter (uses `visualizationType: "bubble"`)
- `origami`, `parallelcoords`: NOT found in any production dashboard
- All 17 chart types used: area, bubble, bullet, column, combo, comparisontable, funnel, gauge, hbar, heatmap, line, sankey, scatter, stackcolumn, timeline, treemap, waterfall
