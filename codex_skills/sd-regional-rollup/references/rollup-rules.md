# Rollup Rules

## Two Territory Models

- `director book`: workbook and MD-1 territory slice
- `forecast rollup`: Salesforce forecast hierarchy under CRO

These are not interchangeable.

## Current Regional Mapping

- APAC = APAC only
- EMEA = Central Europe + Northern Europe + Southern Europe + UK & Ireland + Middle East & Africa
- North America = Canada + NA Asset Management + Pension & Insurance

## Tie-Out Rules

- if comparing to CRO forecast page, use ForecastingItem logic
- if comparing to workbook pipeline, use corrected opportunity logic
- call the delta what it is; do not force a false match

## Common Confusions

- Christian book total versus Northern Europe forecast total
- mixed all-open pipeline versus in-quarter forecast
- omitted-stage amounts buried in pipeline
