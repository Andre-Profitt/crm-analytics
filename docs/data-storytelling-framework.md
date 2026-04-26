# Data Storytelling Framework for CRM Analytics Dashboards

## Source: IBCS, Tufte, Cole Nussbaumer Knaflic, McKinsey/BCG/Bain patterns

---

## The 5-Second Test

Show the dashboard to someone for 5 seconds, then take it away. They should tell you:

1. What it's about
2. Whether things are going well or badly
3. What area needs the most attention

If they can't → the narrative structure has failed.

---

## Page Structure: Situation → Complication → Resolution

### Row 1 — Situation (KPI Strip)

- 4-6 summary KPIs with comparison indicators
- Every number needs: current value + vs target + vs prior period + sparkline trend
- Large font (28-36px) for the value, small font for context

### Row 2 — Complication (Primary Visualization)

- The main chart showing the problem/opportunity/trend
- Title states the FINDING, not the axis labels
  - ❌ "Revenue by Region"
  - ✅ "EMEA revenue declined 12% while APAC grew 8%"

### Row 3 — Resolution (Action Table)

- Specific items to act on, sorted by impact
- Record links to Salesforce for immediate action
- Color-coded severity

---

## IBCS Notation Standards

| Scenario           | Visual Treatment              |
| ------------------ | ----------------------------- |
| Actual (AC)        | Solid fill, dark (#333333)    |
| Plan (PL)          | Outlined/hollow (border only) |
| Forecast (FC)      | Hatched/striped fill          |
| Previous Year (PY) | Light gray solid (#C9C9C9)    |

### IBCS Variance Charts

Instead of showing Actual and Plan as two bars, show:

- One bar for actual
- A small delta bar (green if positive, red if negative) for variance
- This is directly actionable — execs see WHERE the gap is instantly

---

## Color Rules

### Structural Colors (always present)

- Dark gray `#333333` — text and data labels
- Medium gray `#999999` — secondary text, axes, gridlines
- Light gray `#E5E5E5` — backgrounds, inactive elements
- White `#FFFFFF` — card backgrounds

### Semantic Colors (meaning-bearing)

- Red `#C23934` — negative variance, missed target, risk
- Green `#2E844A` — positive variance, met target, healthy
- Amber `#DD7A01` — warning, approaching threshold
- Blue `#0070D2` — neutral highlight, selected state

### The "Gray Everything, Then Highlight" Rule

Make ALL data series gray. Color ONLY the one series you want noticed.
This creates instant visual hierarchy.

### Never encode meaning with color alone

Always pair with: icon, text label, or pattern (for accessibility)

---

## Typography Scale (exactly 4 levels)

| Level   | Use                   | Size    | Weight   | Color   |
| ------- | --------------------- | ------- | -------- | ------- |
| Display | KPI values            | 28-36px | Bold     | #333333 |
| Title   | Widget/section titles | 16-18px | Semibold | #333333 |
| Body    | Labels, descriptions  | 13-14px | Regular  | #666666 |
| Caption | Axes, footnotes       | 11-12px | Regular  | #999999 |

---

## Number Formatting

- Abbreviate: $4.2M not $4,200,000
- Match precision to significance (1 decimal for exec views)
- Consistent decimal places across all KPIs
- Always show units ($, %, etc.)
- Comparison format: "+12%" or "-$400K" with directional indicators

---

## McKinsey Chart Rules

1. Action title (the "so what?") — complete sentence stating the finding
2. Single chart per section — never two charts competing
3. Direct labeling — no legends, labels on/next to data points
4. Minimal decoration — no gridlines, borders, 3D, gradients
5. Footnotes for caveats, sources, methodology

---

## Dashboard Archetypes

### Strategic (Executive) — scan in 30 seconds

- 5-9 KPIs, large type, sparklines, red/yellow/green
- Minimal interaction
- Purpose: "Do I need to intervene?"

### Operational (Manager) — filter and drill in 2 minutes

- Dense, data-rich, thresholds, alerts
- Moderate interaction (filter by team, region, time)
- Purpose: "What needs my attention right now?"

### Analytical (Analyst) — explore for 10 minutes

- Flexible, exploratory, small multiples
- Heavy interaction (pivot, drill, compare)
- Purpose: "Why is this happening?"

**Fatal mistake: building analytical and calling it executive.**

---

## Anti-Patterns to Avoid

| Anti-Pattern                                            | Fix                                          |
| ------------------------------------------------------- | -------------------------------------------- |
| Rainbow Dashboard (7 colors for 7 regions)              | Monochromatic palette + 1 accent color       |
| Gauge Collection                                        | Bullet charts (same info, 1/5th the space)   |
| 3D Pie Chart                                            | Horizontal bar sorted by value               |
| Dual-Axis Chart                                         | Two separate charts or indexed chart         |
| Dashboard of Dashboards (12 small charts, no narrative) | KPI strip → main chart → supporting details  |
| Filter Bar of Doom (15 dropdowns)                       | Max 3-5 filters, show active values          |
| Orphan Number (no context)                              | Always show vs target + vs prior + delta     |
| Scroll of Death                                         | Paginate into focused pages with tab nav     |
| Kitchen Sink (VP + Manager + Analyst combined)          | Separate views per audience                  |
| Vanity Metric (always goes up)                          | Show rate metrics that can indicate problems |

---

## Pre-Build Checklist (every page)

1. [ ] Write the action title FIRST — what is this page trying to tell the viewer?
2. [ ] Classify every metric — global benchmark, scoped KPI, or exploratory
3. [ ] Apply narrative arc — KPI strip (situation), primary chart (complication), action table (resolution)
4. [ ] Color check — max 3 semantic colors; gray for everything else
5. [ ] Five-second test — can viewer identify key message in 5 seconds?
6. [ ] Comparison audit — every number has context (vs target, vs prior, vs peer)
7. [ ] Chart type audit — right type for the question?
8. [ ] Clutter sweep — remove borders, unnecessary gridlines, legends (label directly)
9. [ ] Accessibility — nothing relies on color alone? Contrast ratios met?
10. [ ] Action pathway — can viewer go from insight to record-level action?
