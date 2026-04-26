# BDR Consulting-Grade Upgrade Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply all 10 consulting-grade patterns to the BDR Operating System builder (Manager, Rep, Control dashboards) to match the standard already landed in 7 other builders.

**Architecture:** Mechanical upgrade of `build_bdr_operating_dashboards.py` — add imports, define KPI_FACET_SCOPE constants per dashboard, upgrade 28 `num()` calls with `tier=`/`widget_style=`, add `format_rules=` to ~40 comparison tables, add `section_label()` dividers, add `reference_lines=` to benchmark charts, and pass `bg_color`/`cell_spacing`/`widget_style` to all 3 `build_dashboard_state()` calls.

**Tech Stack:** Python, `crm_analytics_helpers.py` functions, Salesforce CRM Analytics Wave API

**Reference builders:** `build_dashboard_1.py` (all 10 patterns), `build_executive_pipeline_risk_process.py` (compare_table + section_label)

---

## Chunk 1: Imports, Constants, and KPI Upgrades

### Task 1: Add imports and define KPI_FACET_SCOPE constants

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:17-43` (imports)
- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:46-54` (after constants)

- [ ] **Step 1: Add missing imports**

Add `kpi_style`, `KPI_CARD_STYLE`, and `section_label` to the import block from `crm_analytics_helpers`:

```python
from crm_analytics_helpers import (
    _date,
    _dim,
    _measure,
    _soql,
    KPI_CARD_STYLE,
    add_table_action,
    af,
    build_dashboard_state,
    coalesce_filter,
    create_dashboard_if_needed,
    deploy_dashboard,
    flat_gauge,
    get_auth,
    get_dataset_id,
    hdr,
    heatmap_chart,
    kpi_style,
    line_chart,
    nav_link,
    nav_row,
    num,
    pg,
    pillbox,
    rich_chart,
    section_label,
    set_record_links_xmd,
    sq,
    upload_dataset,
)
```

Note: `compare_table` is NOT imported — the BDR builder uses `rich_chart(..., "comparisontable", ...)` + `add_table_action()` for all tables, which is the correct inline-table pattern. `compare_table()` is reserved for exec-style standalone queues. Keep the existing pattern.

- [ ] **Step 2: Define KPI_FACET_SCOPE constants**

Add after line 54 (after `FY2027_START`). All three dashboards use `f_team`, `f_owner`, `f_source` as filter steps. The Control dashboard also has `f_year`.

```python
# -- Consulting-grade KPI isolation: KPIs respond to filters only, not chart cross-clicks --
MANAGER_KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_team", "f_owner", "f_source"],
    },
}

REP_KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_team", "f_owner"],
    },
}

CONTROL_KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_team", "f_owner", "f_source", "f_year"],
    },
}
```

- [ ] **Step 3: Verify file still parses**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`
Expected: No errors (clean import)

- [ ] **Step 4: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): add consulting-grade imports and KPI_FACET_SCOPE constants"
```

---

### Task 2: Upgrade Manager KPIs (6 num() calls + 3 product KPIs)

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:5092-5097` (p1 KPIs)
- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:5274-5276` (p4s KPIs)

- [ ] **Step 1: Upgrade NA Rhythm KPIs (lines 5092-5097)**

Replace the 6 hero KPIs on the overview page with `tier=` and `widget_style=`:

```python
        "p1_n_prospect": num("s_summary", "prospect_accounts", "Prospect Accounts", "#8E030F", compact=True, tier="primary", widget_style=kpi_style("card")),
        "p1_n_former": num("s_summary", "former_client_accounts", "Former Client Accounts", "#5F2C83", compact=True, tier="primary", widget_style=kpi_style("card")),
        "p1_n_persona": num("s_summary", "persona_contacts", "Persona Contacts", "#032D60", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p1_n_open_opp": num("s_summary", "open_opportunities", "Open Opportunities", "#0176D3", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p1_n_stage2": num("s_summary", "discovery_handoffs", "Stage 2 Discovery", "#2E844A", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p1_n_stage3": num("s_summary", "stage3_handoffs", "Stage 3 Handoffs", "#032D60", compact=True, tier="secondary", widget_style=kpi_style("card")),
```

Tier logic: Prospect Accounts and Former Clients are the two hero account-based KPIs (primary=40pt). The rest are supporting (secondary=32pt).

- [ ] **Step 2: Upgrade Product Signal KPIs (lines 5274-5276)**

```python
        "p4s_n_known_product": num("s_product_signal_summary", "KnownProductAccounts", "Accounts With Product Signal", "#2E844A", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p4s_n_missing_product": num("s_product_signal_summary", "MissingProductAccounts", "Accounts Missing Product Signal", "#BA0517", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p4s_n_known_pct": num("s_product_signal_summary", "KnownProductCoveragePct", "Known Product Coverage %", "#032D60", compact=True, tier="secondary", widget_style=kpi_style("card")),
```

- [ ] **Step 3: Apply KPI_FACET_SCOPE to Manager KPI steps**

In `_manager_steps()` (starts line 4124), add `.update(MANAGER_KPI_FACET_SCOPE)` to the KPI steps. Find the return statement of `_manager_steps()` and add before it:

```python
    # Apply KPI facet isolation — KPIs respond to filters, not chart cross-clicks
    for key in ("s_summary", "s_sla_bullet", "s_integrity_bullet", "s_source_bullet", "s_product_signal_summary"):
        if key in steps:
            steps[key].update(MANAGER_KPI_FACET_SCOPE)
```

- [ ] **Step 4: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`
Expected: No errors

- [ ] **Step 5: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): upgrade manager KPIs with tier, kpi_style, and facet isolation"
```

---

### Task 3: Upgrade Rep KPIs (10 num() calls)

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:5955-5960` (p1 KPIs)
- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:6122-6125` (p4 KPIs)

- [ ] **Step 1: Upgrade Rep My Day KPIs (lines 5955-5960)**

```python
        "p1_n_open": num("s_summary", "open_leads", "Open Leads", "#032D60", compact=True, tier="primary", widget_style=kpi_style("card")),
        "p1_n_mql": num("s_summary", "open_mql_leads", "Open MQL Leads", "#0176D3", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p1_n_sql": num("s_summary", "open_sql_leads", "Open SQL / Hot Leads", "#8E030F", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p1_n_response": num("s_summary", "responder_queue_count", "Responders Awaiting Action", "#BA0517", compact=True, tier="primary", widget_style=kpi_style("accent")),
        "p1_n_meetings": num("s_summary", "upcoming_meetings", "Upcoming Meetings", "#0176D3", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p1_n_reengage": num("s_summary", "reengage_count", "Re-engagement Targets", "#2E844A", compact=True, tier="secondary", widget_style=kpi_style("card")),
```

Tier logic: Open Leads is the hero count (primary). Responders Awaiting Action is the urgency callout (primary + accent style). Rest are supporting.

- [ ] **Step 2: Upgrade Target & Handoff KPIs (lines 6122-6125)**

```python
        "p4_n_target": num("s_target_summary", "matched_targets", "Matched Targets", "#032D60", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p4_n_tier1": num("s_target_summary", "tier1_targets", "Tier 1 Targets", "#0176D3", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p4_n_reengage": num("s_target_summary", "target_reengage", "Strategic Re-engage", "#BA0517", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p4_n_handoff": num("s_handoff_summary", "handoff_queue_count", "Stage 2 -> 3 Handoffs", "#5F2C83", compact=True, tier="primary", widget_style=kpi_style("accent")),
```

- [ ] **Step 3: Apply KPI_FACET_SCOPE to Rep KPI steps**

In `_rep_steps()` (starts line 5479), add before the return:

```python
    # Apply KPI facet isolation
    for key in ("s_summary", "s_target_summary", "s_handoff_summary"):
        if key in steps:
            steps[key].update(REP_KPI_FACET_SCOPE)
```

- [ ] **Step 4: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`

- [ ] **Step 5: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): upgrade rep KPIs with tier, kpi_style, and facet isolation"
```

---

### Task 4: Upgrade Control KPIs (9 num() calls)

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:6727-6731` (p1 KPIs)
- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:6815-6818` (p3 KPIs)

- [ ] **Step 1: Upgrade Campaign Performance KPIs (lines 6727-6731)**

```python
        "p1_n_leads": num("s_summary", "campaign_leads", "Campaign Leads", "#032D60", compact=True, tier="primary", widget_style=kpi_style("card")),
        "p1_n_response": num("s_summary", "campaign_responders", "Responders", "#0176D3", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p1_n_meetings": num("s_summary", "meetings_held", "Meetings Held", "#2E844A", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p1_n_handoffs": num("s_summary", "opportunity_handoffs", "Opp Handoffs", "#032D60", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p1_n_arr": num("s_summary", "known_attributed_arr", "Known Attributed ARR", "#5F2C83", compact=True, tier="primary", widget_style=kpi_style("accent")),
```

- [ ] **Step 2: Upgrade Cohort KPIs (lines 6815-6818)**

```python
        "p3_n_former": num("s_cohort_summary", "former_client_2y_open", "Former Client >2Y", "#8E030F", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p3_n_current": num("s_cohort_summary", "tm_handback_open", "TM Hand-back Open", "#032D60", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p3_n_untouched": num("s_cohort_summary", "untouched_open", "Untouched Open", "#BA0517", compact=True, tier="secondary", widget_style=kpi_style("card")),
        "p3_n_cold": num("s_cohort_summary", "cold_open", "Cold Open", "#5F2C83", compact=True, tier="secondary", widget_style=kpi_style("card")),
```

- [ ] **Step 3: Apply KPI_FACET_SCOPE to Control KPI steps**

In `_control_steps()` (starts line 6237), add before the return:

```python
    # Apply KPI facet isolation
    for key in ("s_summary", "s_cohort_summary"):
        if key in steps:
            steps[key].update(CONTROL_KPI_FACET_SCOPE)
```

- [ ] **Step 4: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`

- [ ] **Step 5: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): upgrade control KPIs with tier, kpi_style, and facet isolation"
```

---

## Chunk 2: Format Rules on Comparison Tables

### Task 5: Add format_rules to Manager comparison tables (20 tables)

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:5137-5365` (manager widgets)

The RAG color standard: `#D4504C` = red, `#FFB75D` = amber, `#04844B` = green.

- [ ] **Step 1: Add format_rules to diagnostic/scorecard tables**

These tables have numeric fields that benefit from RAG coloring. Apply `format_rules=` to each `rich_chart(..., "comparisontable", ...)` call. Rules by table:

**p1_tbl_rep** (Rep Account Universe) — line 5153:

```python
        "p1_tbl_rep": rich_chart(
            "s_rep_table",
            "comparisontable",
            "Rep Account Universe, Contact Coverage & Handoff Load",
            ["OwnerName", "BDRTeam"],
            ["AccountCount", "ProspectAccountCount", "FormerClientAccountCount", "CurrentClientAccountCount", "ContactCount", "PersonaContactCount", "OpenOpportunityCount", "DiscoveryHandoffCount", "AccountStage3Count", "PendingStage3ReviewCount"],
            show_legend=False,
            format_rules=[
                {"type": "threshold", "field": "PendingStage3ReviewCount", "rules": [
                    {"value": 3, "color": "#D4504C", "operator": "gte"},
                    {"value": 1, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
        ),
```

**p2_tbl_exec** (Rep Execution Scorecard) — line 5186:

```python
            format_rules=[
                {"type": "threshold", "field": "AvgDaysToFirstMeeting", "rules": [
                    {"value": 14, "color": "#D4504C", "operator": "gte"},
                    {"value": 7, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "LeadToMeetingPct", "rules": [
                    {"value": 15, "color": "#04844B", "operator": "gte"},
                    {"value": 8, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p2_tbl_coach** (Rep Handoff Scorecard) — line 5194:

```python
            format_rules=[
                {"type": "threshold", "field": "AvgStage2To3Days", "rules": [
                    {"value": 90, "color": "#D4504C", "operator": "gte"},
                    {"value": 45, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p2_tbl_integrity** (Logging Integrity) — line 5202:

```python
            format_rules=[
                {"type": "threshold", "field": "DirectLeadTouch24hPct", "rules": [
                    {"value": 80, "color": "#04844B", "operator": "gte"},
                    {"value": 50, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "LeadLinkedActivityPct", "rules": [
                    {"value": 70, "color": "#04844B", "operator": "gte"},
                    {"value": 40, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p3_tbl_source** (Source Quality) — line 5243:

```python
            format_rules=[
                {"type": "threshold", "field": "ResponseRatePct", "rules": [
                    {"value": 20, "color": "#04844B", "operator": "gte"},
                    {"value": 10, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "LeadToMeetingPct", "rules": [
                    {"value": 10, "color": "#04844B", "operator": "gte"},
                    {"value": 5, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p3_tbl_campaign** (Campaign Quality) — line 5251:

```python
            format_rules=[
                {"type": "threshold", "field": "ResponseRatePct", "rules": [
                    {"value": 20, "color": "#04844B", "operator": "gte"},
                    {"value": 10, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "LeadToMeetingPct", "rules": [
                    {"value": 10, "color": "#04844B", "operator": "gte"},
                    {"value": 5, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p4s_tbl_coverage** (Product Signal Coverage) — line 5277:

```python
            format_rules=[
                {"type": "threshold", "field": "KnownProductCoveragePct", "rules": [
                    {"value": 70, "color": "#04844B", "operator": "gte"},
                    {"value": 40, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p4s_tbl_persona** (Persona x Product) — line 5287:

```python
            format_rules=[
                {"type": "threshold", "field": "ResponseRatePct", "rules": [
                    {"value": 20, "color": "#04844B", "operator": "gte"},
                    {"value": 10, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "ContactToOppPct", "rules": [
                    {"value": 5, "color": "#04844B", "operator": "gte"},
                    {"value": 2, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p4s_tbl_industry** (Industry Outreach) — line 5295:

```python
            format_rules=[
                {"type": "threshold", "field": "ActiveCoveragePct", "rules": [
                    {"value": 50, "color": "#04844B", "operator": "gte"},
                    {"value": 25, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p4_tbl_upcoming** (Stage 2->3 Queue) — line 5342:

```python
            format_rules=[
                {"type": "threshold", "field": "Stage2To3Days", "rules": [
                    {"value": 90, "color": "#D4504C", "operator": "gte"},
                    {"value": 45, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p4_tbl_priority** (Manager Priorities) — line 5326:

```python
            format_rules=[
                {"type": "threshold", "field": "DaysSinceLastTouch", "rules": [
                    {"value": 14, "color": "#D4504C", "operator": "gte"},
                    {"value": 7, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p4_tbl_response** (Responders) — line 5334:

```python
            format_rules=[
                {"type": "threshold", "field": "DaysSinceLastTouch", "rules": [
                    {"value": 14, "color": "#D4504C", "operator": "gte"},
                    {"value": 7, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p4_tbl_reengage** (Re-engagement) — line 5358:

```python
            format_rules=[
                {"type": "threshold", "field": "DaysSinceLastTouch", "rules": [
                    {"value": 30, "color": "#D4504C", "operator": "gte"},
                    {"value": 14, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

Tables that do NOT get format_rules (no meaningful RAG threshold): `p1_tbl_account_mix` (client-base split), `p1_tbl_yoy` (year comparison), `p2_tbl_mix` (outreach mix), `p3_tbl_source_product`, `p4s_tbl_segment`, `p4s_tbl_stage`, `p4_tbl_target`.

- [ ] **Step 2: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`

- [ ] **Step 3: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): add RAG format_rules to manager comparison tables"
```

---

### Task 6: Add format_rules to Rep comparison tables (10 tables)

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:5961-6168` (rep widgets)

- [ ] **Step 1: Add format_rules to Rep tables**

**p1_tbl_priority** (Today's Priorities) — line 5980:

```python
            format_rules=[
                {"type": "threshold", "field": "DaysSinceLastTouch", "rules": [
                    {"value": 14, "color": "#D4504C", "operator": "gte"},
                    {"value": 7, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p2_tbl_sla** (SLA & Stale) — line 6052:

```python
            format_rules=[
                {"type": "threshold", "field": "DaysToFirstTouch", "rules": [
                    {"value": 3, "color": "#D4504C", "operator": "gte"},
                    {"value": 1, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "DaysSinceLastTouch", "rules": [
                    {"value": 14, "color": "#D4504C", "operator": "gte"},
                    {"value": 7, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p2_tbl_integrity** (Logging Integrity) — line 6060:

```python
            format_rules=[
                {"type": "threshold", "field": "DirectLeadTouch24hPct", "rules": [
                    {"value": 80, "color": "#04844B", "operator": "gte"},
                    {"value": 50, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "LeadLinkedActivityPct", "rules": [
                    {"value": 70, "color": "#04844B", "operator": "gte"},
                    {"value": 40, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p3_tbl_source** (Campaign Quality) — line 6084:

```python
            format_rules=[
                {"type": "threshold", "field": "ResponseRatePct", "rules": [
                    {"value": 20, "color": "#04844B", "operator": "gte"},
                    {"value": 10, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p3_tbl_campaign** (Persona x Product) — line 6092:

```python
            format_rules=[
                {"type": "threshold", "field": "ResponseRatePct", "rules": [
                    {"value": 20, "color": "#04844B", "operator": "gte"},
                    {"value": 10, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p3_tbl_response** (Campaign Responders) — line 6044:

```python
            format_rules=[
                {"type": "threshold", "field": "DaysSinceLastTouch", "rules": [
                    {"value": 14, "color": "#D4504C", "operator": "gte"},
                    {"value": 7, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p4_tbl_stage3** (Stage 2->3 Queue — Rep view) — line 6102, only if present. Check step ref `s_stage3_queue`:

```python
            format_rules=[
                {"type": "threshold", "field": "Stage2To3Days", "rules": [
                    {"value": 90, "color": "#D4504C", "operator": "gte"},
                    {"value": 45, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

Tables without format_rules: `p1_tbl_yoy` (year comparison), `p2_tbl_upcoming` (meetings — no RAG field), `p4_tbl_target_segment`.

- [ ] **Step 2: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`

- [ ] **Step 3: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): add RAG format_rules to rep comparison tables"
```

---

### Task 7: Add format_rules to Control comparison tables (~16 tables)

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:6753-6938` (control widgets)

- [ ] **Step 1: Add format_rules to Control tables**

**p1_tbl_campaign** (Campaign Performance) — line 6753:

```python
            format_rules=[
                {"type": "threshold", "field": "ResponseRatePct", "rules": [
                    {"value": 20, "color": "#04844B", "operator": "gte"},
                    {"value": 10, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "LeadToMeetingPct", "rules": [
                    {"value": 10, "color": "#04844B", "operator": "gte"},
                    {"value": 5, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p2_tbl_persona_product** — line 6783:

```python
            format_rules=[
                {"type": "threshold", "field": "ResponseRatePct", "rules": [
                    {"value": 20, "color": "#04844B", "operator": "gte"},
                    {"value": 10, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "LeadToOppPct", "rules": [
                    {"value": 5, "color": "#04844B", "operator": "gte"},
                    {"value": 2, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p2_tbl_owner_success** — line 6791:

```python
            format_rules=[
                {"type": "threshold", "field": "LeadToOppPct", "rules": [
                    {"value": 5, "color": "#04844B", "operator": "gte"},
                    {"value": 2, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**Action queue tables** (p3_tbl_former, p3_tbl_current, p3_tbl_untouched, p3_tbl_cold, p4_tbl_response, p4_tbl_cold, p5_tbl_named, p5_tbl_former, p5_tbl_cold) — all have `DaysSinceLastTouch` or `DaysToFirstTouch`:

For tables with `DaysSinceLastTouch`:

```python
            format_rules=[
                {"type": "threshold", "field": "DaysSinceLastTouch", "rules": [
                    {"value": 30, "color": "#D4504C", "operator": "gte"},
                    {"value": 14, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

For `p3_tbl_untouched` (has `DaysToFirstTouch` instead):

```python
            format_rules=[
                {"type": "threshold", "field": "DaysToFirstTouch", "rules": [
                    {"value": 3, "color": "#D4504C", "operator": "gte"},
                    {"value": 1, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p4_tbl_stage3** (Stage 2->3 Queue — Control view):

```python
            format_rules=[
                {"type": "threshold", "field": "Stage2To3Days", "rules": [
                    {"value": 90, "color": "#D4504C", "operator": "gte"},
                    {"value": 45, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

**p5_tbl_pockets** and **p5_tbl_role_product** — rate-based:

```python
            format_rules=[
                {"type": "threshold", "field": "ResponseRatePct", "rules": [
                    {"value": 20, "color": "#04844B", "operator": "gte"},
                    {"value": 10, "color": "#FFB75D", "operator": "gte"},
                ]},
                {"type": "threshold", "field": "LeadToOppPct", "rules": [
                    {"value": 5, "color": "#04844B", "operator": "gte"},
                    {"value": 2, "color": "#FFB75D", "operator": "gte"},
                ]},
            ],
```

Tables without format_rules: `p1_tbl_source_product` (no clear threshold), `p2_tbl_client_product` (mix view).

- [ ] **Step 2: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`

- [ ] **Step 3: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): add RAG format_rules to control comparison tables"
```

---

## Chunk 3: Section Labels, Reference Lines, and Build State

### Task 8: Add section_label() dividers to Manager layout

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:5083-5385` (manager widgets)
- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:5388-5476` (manager layout)

- [ ] **Step 1: Add section_label widgets to \_manager_widgets()**

Add these entries to the `widgets` dict in `_manager_widgets()`:

```python
        # Section dividers
        "p1_sec_scorecard": section_label("Account Scorecard"),
        "p1_sec_rhythm": section_label("FY Rhythm & Account Universe"),
        "p2_sec_activity": section_label("Activity Trends"),
        "p2_sec_scorecard": section_label("Rep Execution Scorecard"),
        "p3_sec_charts": section_label("Campaign & Product Trends"),
        "p3_sec_tables": section_label("Source & Campaign Quality"),
        "p4s_sec_coverage": section_label("Product Signal Coverage"),
        "p4s_sec_diagnostics": section_label("Industry & Segment Diagnostics"),
        "p4_sec_queues": section_label("Manager Action Queues"),
```

- [ ] **Step 2: Insert section_label entries into \_manager_layout()**

In each page layout list, insert the section label widget with `rowspan=1` at the boundary between widget groups. Shift subsequent rows down by 1 to accommodate.

For p1 (NA Rhythm), insert after gauges (row 12) and before the charts:

```python
        {"name": "p1_sec_rhythm", "row": 12, "column": 0, "colspan": 12, "rowspan": 1},
```

Shift `p1_ch_story`, `p1_tbl_account_mix`, `p1_tbl_yoy` down by 1 (row 12→13).
Shift `p1_tbl_rep` down by 1 (row 19→20).

For p2 (Rep Cadence), insert after filters (row 4) before charts, and at row 11 before tables:

```python
        {"name": "p2_sec_activity", "row": 4, "column": 0, "colspan": 12, "rowspan": 1},
```

Shift charts down by 1 (row 4→5).

```python
        {"name": "p2_sec_scorecard", "row": 12, "column": 0, "colspan": 12, "rowspan": 1},
```

Shift tables down by 2 total.

Similar pattern for p3, p4, p5 — each gets 1 section label before the diagnostic/table blocks.

- [ ] **Step 3: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`

- [ ] **Step 4: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): add section_label dividers to manager dashboard layout"
```

---

### Task 9: Add section_label() to Rep and Control layouts

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:5947-6168` (rep widgets + layout)
- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:6717-6954` (control widgets + layout)

- [ ] **Step 1: Add section_label widgets to \_rep_widgets()**

```python
        "p1_sec_mix": section_label("Today's Work Queue"),
        "p2_sec_sla": section_label("SLA & Logging Integrity"),
        "p2_sec_tables": section_label("Follow-up & Activity Audit"),
        "p3_sec_campaign": section_label("Campaign Performance"),
        "p4_sec_target": section_label("Target Accounts & Handoffs"),
```

- [ ] **Step 2: Insert into \_rep_layout() with row shifts**

Same pattern as manager — add `rowspan=1` entries at group boundaries, shift downstream rows.

- [ ] **Step 3: Add section_label widgets to \_control_widgets()**

```python
        "p1_sec_charts": section_label("Campaign Rhythm"),
        "p1_sec_tables": section_label("Campaign & Source Quality"),
        "p2_sec_heatmap": section_label("Persona & Product Heatmaps"),
        "p2_sec_tables": section_label("Persona & Segment Diagnostics"),
        "p3_sec_cohorts": section_label("GTM Cohort Queues"),
        "p4_sec_queues": section_label("Activation Queues"),
        "p5_sec_lists": section_label("Strategic Target Lists"),
```

- [ ] **Step 4: Insert into \_control_layout() with row shifts**

- [ ] **Step 5: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`

- [ ] **Step 6: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): add section_label dividers to rep and control layouts"
```

---

### Task 10: Add reference_lines to benchmark charts and show_values to hbar charts

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py` (manager + rep + control widget defs)

- [ ] **Step 1: Add show_values=True to all hbar charts missing it**

Manager `p2_ch_activity_mix` (line 5177) and `p3_ch_product` (line 5225):

```python
            show_values=True,
```

Control `p1_ch_product` (line 6744):

```python
            show_values=True,
```

Rep `p3_ch_product` (line 6074):

```python
            show_values=True,
```

- [ ] **Step 2: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`

- [ ] **Step 3: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): add show_values to hbar charts"
```

---

### Task 11: Upgrade build_dashboard_state() calls

**Files:**

- Modify: `/Users/test/crm-analytics/build_bdr_operating_dashboards.py:7050-7063` (main)

- [ ] **Step 1: Add bg_color, cell_spacing, widget_style to all 3 build_dashboard_state calls**

Replace lines 7050, 7055, 7060:

```python
    manager_state = build_dashboard_state(
        _manager_steps(ds_id), _manager_widgets(), _manager_layout(),
        bg_color="#F4F6F9", cell_spacing=8, widget_style=KPI_CARD_STYLE,
    )
```

```python
    rep_state = build_dashboard_state(
        _rep_steps(ds_id), _rep_widgets(), _rep_layout(),
        bg_color="#F4F6F9", cell_spacing=8, widget_style=KPI_CARD_STYLE,
    )
```

```python
    control_state = build_dashboard_state(
        _control_steps(ds_id), _control_widgets(), _control_layout(),
        bg_color="#F4F6F9", cell_spacing=8, widget_style=KPI_CARD_STYLE,
    )
```

- [ ] **Step 2: Verify parse**

Run: `cd /Users/test/crm-analytics && python3 -c "import build_bdr_operating_dashboards"`

- [ ] **Step 3: Commit**

```bash
cd /Users/test/crm-analytics && git add build_bdr_operating_dashboards.py && git commit -m "feat(bdr): add consulting-grade bg_color, cell_spacing, and widget_style to all dashboards"
```

---

## Chunk 4: Validation

### Task 12: Run existing tests and BDR audit

- [ ] **Step 1: Run BDR audit**

```bash
cd /Users/test/crm-analytics && python3 scripts/audit_bdr_operating_system.py 2>&1 | tail -40
```

Expected: All existing checks still PASS (26/26). No regressions.

- [ ] **Step 2: Verify the builder can produce JSON without runtime errors**

```bash
cd /Users/test/crm-analytics && python3 -c "
from build_bdr_operating_dashboards import _manager_steps, _manager_widgets, _manager_layout, _rep_steps, _rep_widgets, _rep_layout, _control_steps, _control_widgets, _control_layout
from crm_analytics_helpers import build_dashboard_state, KPI_CARD_STYLE
import json

# Validate manager
mw = _manager_widgets()
ml = _manager_layout()
print(f'Manager: {len(mw)} widgets, {len(ml[\"pages\"])} pages')

# Validate rep
rw = _rep_widgets()
rl = _rep_layout()
print(f'Rep: {len(rw)} widgets, {len(rl[\"pages\"])} pages')

# Validate control
cw = _control_widgets()
cl = _control_layout()
print(f'Control: {len(cw)} widgets, {len(cl[\"pages\"])} pages')

# Check consulting-grade patterns are present
assert any('widget_style' in str(v) for v in mw.values()), 'Missing widget_style in manager'
print('All consulting-grade patterns verified')
"
```

- [ ] **Step 3: Final commit with all validation passing**

```bash
cd /Users/test/crm-analytics && git add -A && git commit -m "feat(bdr): complete consulting-grade upgrade — 10 patterns across 3 dashboards"
```

---

## Summary of Changes

| Pattern                        | Count                           | Applied To                                     |
| ------------------------------ | ------------------------------- | ---------------------------------------------- |
| `tier=`                        | 28 `num()` calls                | All 3 dashboards                               |
| `widget_style=kpi_style()`     | 28 `num()` calls                | All 3 dashboards                               |
| `KPI_FACET_SCOPE`              | 3 constants + step updates      | Manager, Rep, Control                          |
| `format_rules=`                | ~35 comparison tables           | All diagnostic/scorecard/queue tables          |
| `section_label()`              | ~21 dividers                    | All 3 dashboards, all pages                    |
| `show_values=True`             | 4 additional hbar charts        | Manager, Rep, Control                          |
| `build_dashboard_state()` args | 3 calls                         | `bg_color`, `cell_spacing`, `widget_style`     |
| `reference_lines=`             | 0 (no benchmark data available) | N/A — BDR SLA targets use `flat_gauge` already |

Note: `reference_lines=` is not applicable here because the BDR builder already uses `flat_gauge()` for SLA targets (24h touch, associated response, source completeness). The `flat_gauge` widget IS the BDR benchmark pattern. No charts have external benchmark values that warrant a reference line.
