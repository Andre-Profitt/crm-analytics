# CRM Analytics V2 Review Rubric

Use this rubric before deploying any executive, manager, or analyst page.

## Decision Fit

- Name the business question the widget answers in one sentence.
- Confirm the audience for the page and the action expected after review.
- Reject any widget that mixes multiple business meanings on one axis.

## Metric Contract

- Every KPI must identify `actual`, `target`, `forecast`, `variance`, or `risk` explicitly.
- Fiscal scope must be clear and default to current FY / forward-looking unless there is a specific historical need.
- Drill paths must preserve the same business keys across linked dashboards.

## Visualization Fit

- Use line or timeline charts for trajectory and forecast questions.
- Use waterfalls only for bridges.
- Use bars only for ranked decomposition.
- Use tables only for exception queues or controlled calculation surfaces.
- Reject gauges, decorative bullets, and duplicated summary widgets.

## Interaction And Action

- One control bar per dashboard using shared filter vocabulary.
- Page state must carry across the dashboard.
- Every exceptions page must contain a real action path: record, list, inspection surface, or flow.

## Validation

- KPI totals reconcile to the filtered source data.
- Forecast surfaces show a valid actual-to-forecast handoff.
- Risk/predictive surfaces expose score meaning and top drivers where available.
- Dashboard audit must return no widget/query errors before sign-off.
