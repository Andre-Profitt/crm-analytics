# BDR Chart Formatting Upgrade — Self-Evident Insights

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every BDR chart deliver its insight at a glance — no hover required. Rewrite titles to be metric-focused, add reference lines where targets exist, ensure all bar charts show data labels.

**Architecture:** Edit-only changes to widget definitions in `build_bdr_operating_dashboards.py`. No new steps, no layout changes, no new imports. All changes are to `title`, `subtitle`, `reference_lines`, and `show_values` parameters on existing `rich_chart()`, `line_chart()`, and `heatmap_chart()` calls.

**Tech Stack:** Python, `crm_analytics_helpers.py` (rich_chart, line_chart, heatmap_chart), Salesforce CRM Analytics

**Research basis:** Consulting-grade BDR dashboard standards from McKinsey/BCG formatting rules, 6sense/Gradient Works/Operatix/Bridge Group BDR benchmarks, and the CRM Analytics Widget Decision Library.

---

## Formatting Principles (from research)

1. **10-second rule** — every chart understandable without hover
2. **Metric-focused declarative titles** — include the metric name, dimension, and timeframe. Max 8 words. No generic labels like "Activity Overview"
3. **Data labels on all bar charts** — `show_values=True`. The viewer should never trace from bar tip to axis
4. **Reference lines for instant benchmarking** — horizontal line at target/threshold. Dashed style, muted color, labeled
5. **No prescriptive subtitles** — subtitles only for data scope caveats (e.g., "Excludes partner-sourced"). Remove subtitles that restate the title
6. **One insight per widget** — if a chart answers two questions, it answers neither clearly

---

## Chart Inventory & Treatment Map

### Manager Dashboard (5 pages, 6 charts)

| Widget                  | Type    | Current Title                                         | New Title                                          | Add reference_lines                              | Add show_values                          | Notes |
| ----------------------- | ------- | ----------------------------------------------------- | -------------------------------------------------- | ------------------------------------------------ | ---------------------------------------- | ----- |
| `p1_ch_story`           | line    | "FY2026 Weekly Leads, Meetings, and Stage 3 Handoffs" | "Weekly Leads, Meetings & Handoffs — FY2026"       | No (no single meaningful target across 3 series) | No (line chart — noisy with weekly data) | —     |
| `p2_ch_activity`        | line    | "Weekly Calls, Emails, and Meetings"                  | "Weekly Calls, Emails & Meetings by Team"          | No                                               | No                                       | —     |
| `p2_ch_activity_mix`    | hbar    | "Activity Mix by BDR"                                 | "Activity Volume by Rep — Calls, Emails, Meetings" | No                                               | Already has                              | —     |
| `p3_ch_product`         | hbar    | "Campaign Product Response & Handoff"                 | "Responses & Handoffs by Campaign Product"         | No                                               | Already has                              | —     |
| `p3_ch_week`            | line    | "Monthly Open BDR Pipeline & Handoffs"                | "Monthly Pipeline & Handoffs — BDR Sourced"        | No                                               | No                                       | —     |
| `p4s_ch_target_heatmap` | heatmap | "Industry x Known Product Pockets"                    | "Industry × Product Signal Coverage"               | N/A                                              | N/A (heatmap)                            | —     |
| `p4s_ch_heatmap`        | heatmap | "Industry x Opportunity Product Truth"                | "Industry × Opportunity Product Mix"               | N/A                                              | N/A                                      | —     |

### Rep Dashboard (4 pages, 6 charts)

| Widget          | Type         | Current Title                                         | New Title                                  | Add reference_lines                                           | Add show_values | Notes                                                       |
| --------------- | ------------ | ----------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------- | --------------- | ----------------------------------------------------------- |
| `p1_ch_mix`     | hbar         | "Today's Work Mix"                                    | "Today's Workload — Leads by Queue"        | No                                                            | Already has     | —                                                           |
| `p2_b_sla`      | hbar (1-row) | "Direct Lead Touch <24h"                              | "Direct Lead Touch <24h vs 100% SLA"       | `[{"value": 100, "label": "SLA: 100%", "color": "#54698D"}]`  | Already has     | Reference line makes target visible as line, not just title |
| `p2_b_assoc`    | hbar (1-row) | "Associated Prospect Response <24h"                   | "Associated Response <24h vs 80% Target"   | `[{"value": 80, "label": "Target: 80%", "color": "#54698D"}]` | Already has     | —                                                           |
| `p2_b_leadlink` | hbar (1-row) | "Lead-Linked Activity"                                | "Lead-Linked Activity vs 50% Target"       | `[{"value": 50, "label": "Target: 50%", "color": "#54698D"}]` | Already has     | —                                                           |
| `p2_ch_weekly`  | line         | "Weekly Touches, Meetings, and Stage 2 -> 3 Handoffs" | "Weekly Touches, Meetings & Handoffs"      | No                                                            | No              | —                                                           |
| `p3_ch_product` | hbar         | "Campaign Product Response & Handoff"                 | "Responses & Handoffs by Campaign Product" | No                                                            | Already has     | —                                                           |
| `p3_ch_heatmap` | heatmap      | "Role / Title x Industry Opportunity Rate"            | "Role × Industry — Opportunity Rate"       | N/A                                                           | N/A             | —                                                           |

### Control Dashboard (5 pages, 5 charts)

| Widget                  | Type       | Current Title                                | New Title                                                   | Add reference_lines | Other changes                    |
| ----------------------- | ---------- | -------------------------------------------- | ----------------------------------------------------------- | ------------------- | -------------------------------- |
| `p1_ch_weekly`          | line_chart | "Weekly Campaign, Response & Handoff Rhythm" | "Weekly Leads, Responses & Handoffs — Last 26 Weeks"        | No                  | Remove subtitle (restates title) |
| `p1_ch_monthly`         | line_chart | "Monthly GTM Seasonality"                    | "Monthly GTM Volume — Leads, Responses, Meetings, Handoffs" | No                  | Remove subtitle (restates title) |
| `p1_ch_product`         | hbar       | "Campaign Product Response & Handoff"        | "Responses & Handoffs by Campaign Product"                  | No                  | Already has show_values          |
| `p2_ch_heatmap`         | heatmap    | "Role / Title x Industry Opportunity Rate"   | "Role × Industry — Opportunity Rate"                        | N/A                 | —                                |
| `p2_ch_monthly_product` | heatmap    | "Monthly Product Momentum"                   | "Monthly Product Engagement Momentum"                       | N/A                 | —                                |

---

## Chunk 1: Manager Dashboard Chart Titles & Formatting

### Task 1: Rewrite Manager chart titles

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py`

- [ ] **Step 1: Update p1_ch_story title (Manager p1 — line chart)**

Find in `_manager_widgets()`:

```python
            "FY2026 Weekly Leads, Meetings, and Stage 3 Handoffs",
```

Replace with:

```python
            "Weekly Leads, Meetings & Handoffs — FY2026",
```

- [ ] **Step 2: Update p2_ch_activity title (Manager p2 — line chart)**

Find:

```python
            "Weekly Calls, Emails, and Meetings",
```

Replace with:

```python
            "Weekly Calls, Emails & Meetings by Team",
```

- [ ] **Step 3: Update p2_ch_activity_mix title (Manager p2 — hbar)**

Find:

```python
            "Activity Mix by BDR",
```

Replace with:

```python
            "Activity Volume by Rep — Calls, Emails, Meetings",
```

- [ ] **Step 4: Update p3_ch_product title (Manager p3 — hbar)**

Find in manager widgets:

```python
            "Campaign Product Response & Handoff",
```

Replace with:

```python
            "Responses & Handoffs by Campaign Product",
```

- [ ] **Step 5: Update p3_ch_week title (Manager p3 — line chart)**

Find:

```python
            "Monthly Open BDR Pipeline & Handoffs",
```

Replace with:

```python
            "Monthly Pipeline & Handoffs — BDR Sourced",
```

- [ ] **Step 6: Update p4s heatmap titles (Manager p4)**

Find:

```python
            "Industry x Known Product Pockets",
```

Replace with:

```python
            "Industry × Product Signal Coverage",
```

Find:

```python
            "Industry x Opportunity Product Truth",
```

Replace with:

```python
            "Industry × Opportunity Product Mix",
```

- [ ] **Step 7: Verify compile**

Run: `cd /Users/test/crm-analytics && python3 -m py_compile build_bdr_operating_dashboards.py`
Expected: No errors

- [ ] **Step 8: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): rewrite manager chart titles to metric-focused declarative format"
```

---

## Chunk 2: Rep Dashboard Chart Titles, Reference Lines & Formatting

### Task 2: Rewrite Rep chart titles and add SLA reference lines

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py`

- [ ] **Step 1: Update p1_ch_mix title (Rep p1 — hbar)**

Find in `_rep_widgets()`:

```python
            "Today's Work Mix",
```

Replace with:

```python
            "Today's Workload — Leads by Queue",
```

- [ ] **Step 2: Add reference_lines to p2_b_sla (Rep p2 — single-row SLA bar)**

Find:

```python
        "p2_b_sla": rich_chart(
            "s_sla_bar",
            "hbar",
            "Direct Lead Touch <24h",
```

Replace the title AND add reference_lines. The full call should become:

```python
        "p2_b_sla": rich_chart(
            "s_sla_bar",
            "hbar",
            "Direct Lead Touch <24h vs 100% SLA",
```

Then find the closing `)` of this rich_chart call and add `reference_lines` before it:

```python
            reference_lines=[
                {"value": 100, "label": "SLA: 100%", "color": "#54698D"},
            ],
```

- [ ] **Step 3: Add reference_lines to p2_b_assoc (Rep p2 — single-row bar)**

Same pattern. Update title to:

```python
            "Associated Response <24h vs 80% Target",
```

Add:

```python
            reference_lines=[
                {"value": 80, "label": "Target: 80%", "color": "#54698D"},
            ],
```

- [ ] **Step 4: Add reference_lines to p2_b_leadlink (Rep p2 — single-row bar)**

Update title to:

```python
            "Lead-Linked Activity vs 50% Target",
```

Add:

```python
            reference_lines=[
                {"value": 50, "label": "Target: 50%", "color": "#54698D"},
            ],
```

- [ ] **Step 5: Update p2_ch_weekly title (Rep p2 — line chart)**

Find:

```python
            "Weekly Touches, Meetings, and Stage 2 -> 3 Handoffs",
```

Replace with:

```python
            "Weekly Touches, Meetings & Handoffs",
```

- [ ] **Step 6: Update p3_ch_product title (Rep p3 — hbar)**

Find in rep widgets:

```python
            "Campaign Product Response & Handoff",
```

Replace with:

```python
            "Responses & Handoffs by Campaign Product",
```

- [ ] **Step 7: Update p3_ch_heatmap title (Rep p3 — heatmap)**

Find:

```python
            "Role / Title x Industry Opportunity Rate",
```

Replace with:

```python
            "Role × Industry — Opportunity Rate",
```

Note: This title appears in both Rep and Control dashboards. Use unique context (surrounding widget keys) to target the correct instance.

- [ ] **Step 8: Verify compile**

Run: `cd /Users/test/crm-analytics && python3 -m py_compile build_bdr_operating_dashboards.py`
Expected: No errors

- [ ] **Step 9: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): rewrite rep chart titles, add SLA reference lines"
```

---

## Chunk 3: Control Dashboard Chart Titles & Subtitle Cleanup

### Task 3: Rewrite Control chart titles and remove stale subtitles

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py`

- [ ] **Step 1: Update p1_ch_weekly title and remove subtitle (Control p1 — line_chart)**

Find in `_control_widgets()`:

```python
        "p1_ch_weekly": line_chart(
            "s_weekly_rhythm",
            "Weekly Campaign, Response & Handoff Rhythm",
```

Replace title:

```python
            "Weekly Leads, Responses & Handoffs — Last 26 Weeks",
```

Find the subtitle parameter and set it to empty:

```python
            subtitle="",
```

- [ ] **Step 2: Update p1_ch_monthly title and remove subtitle (Control p1 — line_chart)**

Find:

```python
            "Monthly GTM Seasonality",
```

Replace with:

```python
            "Monthly GTM Volume — Leads, Responses, Meetings, Handoffs",
```

Set subtitle to empty:

```python
            subtitle="",
```

- [ ] **Step 3: Update p1_ch_product title (Control p1 — hbar)**

Find in control widgets:

```python
            "Campaign Product Response & Handoff",
```

Replace with:

```python
            "Responses & Handoffs by Campaign Product",
```

- [ ] **Step 4: Update p2 heatmap titles (Control p2)**

Find:

```python
            "Role / Title x Industry Opportunity Rate",
```

Replace with:

```python
            "Role × Industry — Opportunity Rate",
```

Find:

```python
            "Monthly Product Momentum",
```

Replace with:

```python
            "Monthly Product Engagement Momentum",
```

- [ ] **Step 5: Verify compile**

Run: `cd /Users/test/crm-analytics && python3 -m py_compile build_bdr_operating_dashboards.py`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): rewrite control chart titles, remove stale subtitles"
```

---

## Chunk 4: Validation

### Task 4: Full compile and pattern verification

- [ ] **Step 1: Verify compile**

Run: `cd /Users/test/crm-analytics && python3 -m py_compile build_bdr_operating_dashboards.py`
Expected: No errors

- [ ] **Step 2: Verify all chart titles are updated**

Run: `grep -n "rich_chart\|line_chart\|heatmap_chart" /Users/test/crm-analytics/build_bdr_operating_dashboards.py | head -30`

Scan titles — none should contain:

- "x" (lowercase) for cross-axis (should be "×")
- "and" (should be "&" for brevity)
- Generic labels like "Overview", "Summary", "Chart"

- [ ] **Step 3: Verify reference_lines added to Rep SLA bars**

Run: `grep -A2 "reference_lines" /Users/test/crm-analytics/build_bdr_operating_dashboards.py`

Expected: 3 reference_line entries on Rep p2 SLA bars (100%, 80%, 50%)

- [ ] **Step 4: Verify subtitles removed from Control**

Run: `grep "subtitle" /Users/test/crm-analytics/build_bdr_operating_dashboards.py`

Expected: No non-empty subtitle strings remaining (all should be `subtitle=""` or absent)

---

## Summary of Changes

| Change Type             | Count                        | Scope                |
| ----------------------- | ---------------------------- | -------------------- |
| Title rewrites          | 17 charts                    | All 3 dashboards     |
| Reference lines added   | 3 (Rep SLA bars)             | Rep dashboard p2     |
| Stale subtitles removed | 2                            | Control dashboard p1 |
| show_values additions   | 0 (all bars already have it) | —                    |
| Layout changes          | 0                            | —                    |
| New imports             | 0                            | —                    |
| New steps               | 0                            | —                    |

**Not changed (by design):**

- Line chart `show_values` — too noisy with weekly data points; reference lines + good titles are sufficient
- Heatmap cell labels — CRM Analytics heatmaps show values by default
- Prescriptive subtitles — data changes daily; titles frame the question, data provides the answer
