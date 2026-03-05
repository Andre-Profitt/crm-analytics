# CRM Analytics Dashboard JSON — columnMap Reference

## Standard columnMap Keys

```json
{
  "dimensionAxis": ["FieldName"],
  "plots": ["MeasureName"],
  "trellis": [],
  "split": []
}
```

## CRITICAL RULES (learned the hard way — do NOT violate)

1. **NEVER provide partial columnMap** — if you include a columnMap object, ALL 4 keys MUST be present: `dimensionAxis`, `plots`, `trellis`, `split`. Missing ANY ONE key crashes `_buildColorConfig` (`TypeError: Cannot read properties of undefined (reading 'length')`)
2. `columnMap: null` = auto-detect (safe fallback for any chart type)
3. Omitting columnMap entirely = similar to null but less predictable
4. For **funnel/waterfall**: use `columnMap: null` — they crash even with all 4 standard keys
5. For **gauge**: `{trellis: [], plots: [field]}` is the minimum — no dimensionAxis or split needed
6. For **choropleth**: needs BOTH standard keys AND `locations`/`color` keys (UI adds both sets)
7. Do NOT add `scatter`, `line`, `area`, `stackarea`, `combo` to COLUMNMAP_TYPES — they crash. Leave as auto-detect.
8. Safe COLUMNMAP_TYPES: `hbar`, `column`, `donut`, `stackhbar`, `stackcolumn`, `pie`, `vbar`, `stackvbar`
9. Auto-detect types (no columnMap needed): `comparisontable`, `heatmap`, `combo`, `area`, `stackarea`, `line`, `scatter`

## Crash History

- Gauge with `{plots}` only (missing `trellis`) → **CRASH**
- Funnel with `{dimensionAxis, plots, trellis}` (missing `split`) → **CRASH**
- Waterfall with `{dimensionAxis, plots, trellis}` (missing `split`) → **CRASH**
- Adding `scatter`/`line`/`area` to COLUMNMAP_TYPES → **CRASH**
- Gauge with `{trellis: [], plots: [...]}` → works
- Funnel/waterfall with `columnMap: null` → works
- Choropleth with `{locations, color, trellis, dimensionAxis, plots}` → works (user validated in UI)

## By Visualization Type

### Bar/Donut/Pie (in COLUMNMAP_TYPES)

Full 4-key columnMap: `dimensionAxis` + `plots` + `trellis` + `split`

### Funnel (`funnel`)

`columnMap: null` — auto-detect only. Do NOT provide explicit keys.

### Waterfall (`waterfall`)

`columnMap: null` — auto-detect only.
Sibling properties (outside columnMap): `totalValue: "computeTotal"`, `positiveColor`, `negativeColor`, `startColor`, `totalColor`, `showValues: true`

### Treemap (`treemap`)

`columnMap: null` — auto-detect only.

### Choropleth (`choropleth`)

`columnMap: {locations: [geo], color: [measure], trellis: [], dimensionAxis: [geo], plots: [measure]}`
Sibling properties: `map: "World"`, `binValues`, `lowColor`, `highColor`

### Scatter/Bubble (`scatter`)

No columnMap — auto-detect. NOT in COLUMNMAP_TYPES.

### Gauge (`gauge`)

`columnMap: {trellis: [], plots: [field]}` — no dimensionAxis/split needed
Sibling properties: `gauge: {min, max, bands: [{start, stop, color}]}`

### Line/Area/StackArea/Combo

No columnMap — auto-detect. NOT in COLUMNMAP_TYPES.

## Measure Count Constraints

- Choropleth: exactly 1 grouping + 1 measure
- Scatter: 1-3 measures + 0-4 groupings
- Funnel: 1 dimension + 1 measure (ordered largest→smallest)
