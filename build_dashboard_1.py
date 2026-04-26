#!/usr/bin/env python3
"""Build Dashboard 1: Sales Pipeline & Forecast — 7 pages, ~56 widgets.

Pages:
  1.1  Revenue Command Center
  1.2  Pipeline Health & Velocity
  1.3  Forecast Analysis
  1.4  Deal Composition & Sizing
  1.5  Performance Analysis (dynamic metric/dimension)
  1.6  Commit Calculator
  1.7  Sales Rep Command Center

Datasets used:
  PA   Pipeline_Analytics
  POO  Pipeline_Opportunity_Operations
  PT   Pipeline_Transitions
  ERF  Executive_Revenue_Forecast
  FRM  Forecast_Revenue_Motions

Deploy via PATCH to the Wave REST API.
"""

from crm_analytics_helpers import (  # noqa: E501
    get_auth,
    sq,
    af,
    num,
    rich_chart,
    funnel_chart,
    waterfall_chart,
    combo_chart,
    bullet_chart,
    pillbox,
    listselector,
    hdr,
    section_label,
    nav_link,
    pg,
    nav_row,
    deploy_dashboard,
    create_dashboard_if_needed,
    coalesce_filter,
    line_chart,
    treemap_chart,
    heatmap_chart,
    scatter_chart,
    sankey_chart,
    add_table_action,
    build_dashboard_state,
    kpi_style,
    KPI_CARD_STYLE,
)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

DASHBOARD_LABEL = "Sales Pipeline & Forecast"

# Dataset names for SAQL `load` (CRM Analytics resolves names to IDs)
PA = "Pipeline_Analytics"
POO = "Pipeline_Opportunity_Operations"
PT = "Pipeline_Transitions"
ERF = "Executive_Revenue_Forecast"
FRM = "Forecast_Revenue_Motions"

# TODO: Replace with actual dataset IDs once built. These datasets hold weekly
# forecast snapshots and are required for Page 1.3 widgets 1-5, 7-8, 10.
WFS = "Weekly_Forecast_Summary"  # Weekly_Forecast_Summary dataset
WFO = "Weekly_Forecast_Opps"  # Weekly_Forecast_Opps dataset

# Dataset metadata for aggregateflex steps
PA_META = [{"id": "0FbTb0000019wPBKAY", "name": "Pipeline_Analytics"}]
POO_META = [{"id": "0FbTb000001A0KjKAK", "name": "Pipeline_Opportunity_Operations"}]
ERF_META = [{"id": "0FbTb000001A0EHKA0", "name": "Executive_Revenue_Forecast"}]
FRM_META = [{"id": "0FbTb000001A0NxKAK", "name": "Forecast_Revenue_Motions"}]
PT_META = [{"id": "0FbTb0000019xRhKAI", "name": "Pipeline_Transitions"}]


def _polish_saql_steps(steps):
    """Add groups/numbers arrays to SAQL steps (required by CRM Analytics)."""
    for step in steps.values():
        if step.get("type") == "saql":
            step.setdefault("groups", [])
            step.setdefault("numbers", [])


# ═══════════════════════════════════════════════════════════════════════════
#  SAQL shorthands
# ═══════════════════════════════════════════════════════════════════════════

L_PA = f'q = load "{PA}";\n'
L_POO = f'q = load "{POO}";\n'
L_PT = f'q = load "{PT}";\n'
L_ERF = f'q = load "{ERF}";\n'
L_FRM = f'q = load "{FRM}";\n'
L_WFS = f'q = load "{WFS}";\n'
L_WFO = f'q = load "{WFO}";\n'

# Common filters for PA
PA_OPEN = 'q = filter q by IsClosed == "false";\n'
PA_WON = 'q = filter q by IsWon == "true";\n'
PA_CLOSED = 'q = filter q by IsClosed == "true";\n'

# Filter binding expressions (coalesce passthrough when no selection)
UGF = coalesce_filter("step_region", "UnitGroup")  # UnitGroup on ERF/FRM/POO
UGF_PA = coalesce_filter(
    "step_region", "UnitGroup"
)  # PA doesn't have UnitGroup — see note
TYF = coalesce_filter("step_type", "Type")  # Type filter (PA only)
QF = coalesce_filter("step_quarter", "CloseQuarter")  # Fiscal Quarter filter

# NOTE: PA has no UnitGroup field. For PA queries that need region scoping,
# we filter by the closest available field or skip the filter.
# ERF/FRM/POO all have UnitGroup.

# Isolation Quad — for global benchmarks immune to all facets
ISOLATION_QUAD = {
    "broadcastFacet": False,
    "selectMode": "none",
    "receiveFacetSource": {"mode": "none", "steps": []},
    "useGlobal": False,
}

# KPI Facet Scope — KPIs respond only to global filter pillboxes, not chart clicks
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["step_region", "step_type", "step_quarter"],
    },
}

# ═══════════════════════════════════════════════════════════════════════════
#  Page names (for nav links)
# ═══════════════════════════════════════════════════════════════════════════

PAGE_NAMES = [
    ("p1_1", "Command Center"),
    ("p1_2", "Pipeline Health"),
    ("p1_3", "Forecast"),
    ("p1_4", "Deal Composition"),
    ("p1_5", "Performance"),
    ("p1_6", "Commit Calc"),
    ("p1_7", "My Dashboard"),
]

NAV_COUNT = len(PAGE_NAMES)


def _nav_widgets(page_prefix, active_idx):
    """Build nav link widgets for a page. Returns dict of widget_name -> widget."""
    widgets = {}
    for i, (pg_name, pg_label) in enumerate(PAGE_NAMES):
        wname = f"{page_prefix}_nav{i + 1}"
        widgets[wname] = nav_link(pg_name, pg_label, active=(i == active_idx))
    return widgets


def _standard_header(page_prefix, active_idx, title, subtitle):
    """Build nav bar + header + filter bar widgets and layout entries.

    Returns (widgets_dict, layout_list).
    """
    w = _nav_widgets(page_prefix, active_idx)
    w[f"{page_prefix}_hdr"] = hdr(title, subtitle)
    w[f"{page_prefix}_f_quarter"] = pillbox("step_quarter", "Fiscal Quarter")
    w[f"{page_prefix}_f_region"] = pillbox("step_region", "Region")
    w[f"{page_prefix}_f_type"] = pillbox("step_type", "Type")

    layout = nav_row(page_prefix, NAV_COUNT) + [
        {
            "name": f"{page_prefix}_hdr",
            "row": 1,
            "column": 0,
            "colspan": 12,
            "rowspan": 2,
        },
        {
            "name": f"{page_prefix}_f_quarter",
            "row": 3,
            "column": 0,
            "colspan": 4,
            "rowspan": 2,
        },
        {
            "name": f"{page_prefix}_f_region",
            "row": 3,
            "column": 4,
            "colspan": 4,
            "rowspan": 2,
        },
        {
            "name": f"{page_prefix}_f_type",
            "row": 3,
            "column": 8,
            "colspan": 4,
            "rowspan": 2,
        },
    ]
    return w, layout


# ═══════════════════════════════════════════════════════════════════════════
#  Shared filter steps
# ═══════════════════════════════════════════════════════════════════════════


def shared_steps():
    """Global filter steps shared across all pages."""
    return {
        # UnitGroup filter — driven from ERF which has UnitGroup
        "step_region": af("UnitGroup", ERF_META),
        # Type filter — driven from PA which has Type (Land, Expand, Renewal)
        "step_type": af("Type", PA_META),
        # Fiscal Quarter filter — critical for scoping KPIs to current quarter
        "step_quarter": af("CloseQuarter", ERF_META),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Page 1.1 — Revenue Command Center
# ═══════════════════════════════════════════════════════════════════════════


def page_1_1():
    """Revenue Command Center.

    Story: Where do we stand against target, and what changed this week?
    """
    steps = {}
    widgets = {}
    pp = "p1_1"

    # -- Header & Nav --
    w, layout = _standard_header(
        pp,
        0,
        "Revenue Command Center",
        "Where do we stand against target? | Select Region or Type to filter",
    )
    widgets.update(w)

    # -- KPI 1: Quota Attainment --
    # Closed Won ARR / QuotaAmount * 100 from ERF
    steps["p1_1_quota_attain"] = {
        **sq(
            L_ERF
            + 'q = filter q by IsWon == "true";\n'
            + UGF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate sum('ARR') as won_arr;\n"
            + f'q2 = load "{ERF}";\n'
            + "q2 = group q2 by 'OwnerName';\n"
            + "q2 = foreach q2 generate max('QuotaAmount') as rep_quota;\n"
            + "q2 = group q2 by all;\n"
            + "q2 = foreach q2 generate sum(rep_quota) as total_quota;\n"
            + "r = cogroup q by all, q2 by all;\n"
            + "r = foreach r generate "
            + "coalesce(sum(q.won_arr), 0) as won_arr, "
            + "coalesce(sum(q2.total_quota), 1) as quota, "
            + "(case when coalesce(sum(q2.total_quota), 0) > 0 then "
            + "(coalesce(sum(q.won_arr), 0) / coalesce(sum(q2.total_quota), 1)) * 100 "
            + "else 0 end) as attainment;"
        ),
        **KPI_FACET_SCOPE,
    }
    # Bullet chart: more data-dense than gauge (Stephen Few best practice)
    widgets[f"{pp}_quota"] = bullet_chart(
        "p1_1_quota_attain",
        "Quota Attainment %",
        axis_title="% of Quota",
    )
    layout.append(
        {"name": f"{pp}_quota", "row": 5, "column": 0, "colspan": 3, "rowspan": 4}
    )

    # -- KPI 2: Closed Won ARR --
    steps["p1_1_closed_won"] = sq(
        L_ERF
        + 'q = filter q by IsWon == "true";\n'
        + UGF
        + QF
        + "q = group q by all;\n"
        + "q = foreach q generate sum('ARR') as sum_arr, count() as cnt;"
    )
    steps["p1_1_closed_won"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_cwon"] = num(
        "p1_1_closed_won",
        "sum_arr",
        "Closed Won ARR",
        "#04844B",
        compact=True,
        tier="primary",
        prefix="$",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_cwon", "row": 5, "column": 3, "colspan": 3, "rowspan": 4}
    )

    # -- KPI 3: Pipeline Value (Weighted) --
    steps["p1_1_pipeline"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + UGF
        + QF
        + "q = group q by all;\n"
        + "q = foreach q generate sum('WeightedOpenARR') as pipeline_arr;"
    )
    steps["p1_1_pipeline"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_pipeline"] = num(
        "p1_1_pipeline",
        "pipeline_arr",
        "Weighted Pipeline ARR",
        "#0070D2",
        compact=True,
        tier="primary",
        prefix="$",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_pipeline", "row": 5, "column": 6, "colspan": 3, "rowspan": 4}
    )

    # -- KPI 4: Pipeline Coverage --
    steps["p1_1_coverage"] = sq(
        L_ERF
        + UGF
        + QF
        + "q = foreach q generate "
        + "'WeightedOpenARR', 'QuotaAmount', 'IsWon', 'IsClosed', 'OwnerName';\n"
        + "q = group q by 'OwnerName';\n"
        + "q = foreach q generate "
        + "'OwnerName', "
        + "sum(case when IsClosed == \"false\" then 'WeightedOpenARR' else 0 end) as open_pipe, "
        + "sum(case when IsWon == \"true\" then 'WeightedOpenARR' else 0 end) as won_arr, "
        + "max('QuotaAmount') as rep_quota;\n"
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "sum(open_pipe) as total_pipe, "
        + "sum(rep_quota) as total_quota, "
        + "sum(won_arr) as total_won, "
        + "(case when (sum(rep_quota) - sum(won_arr)) > 0 "
        + "then sum(open_pipe) / (sum(rep_quota) - sum(won_arr)) "
        + "else 0 end) as coverage;"
    )
    # Bullet chart: shows coverage ratio vs target in compact linear form
    widgets[f"{pp}_coverage"] = bullet_chart(
        "p1_1_coverage",
        "Pipeline Coverage (x)",
        axis_title="Coverage Ratio",
    )
    layout.append(
        {"name": f"{pp}_coverage", "row": 5, "column": 9, "colspan": 3, "rowspan": 4}
    )

    # -- KPI 5: Win Rate (filter-responsive — reacts to region/type/quarter) --
    # NOT isolated: managers need to compare win rate across regions, types, etc.
    steps["p1_1_win_rate"] = sq(
        L_PA
        + PA_CLOSED
        + TYF
        + "q = foreach q generate "
        + '(case when IsWon == "true" then 1 else 0 end) as is_won;\n'
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "(sum(is_won) / count()) * 100 as win_rate, "
        + "sum(is_won) as won, count() as total;",
    )
    steps["p1_1_win_rate"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_winrate"] = num(
        "p1_1_win_rate",
        "win_rate",
        "Win Rate",
        "#9050E9",
        tier="secondary",
        suffix="%",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_winrate", "row": 9, "column": 0, "colspan": 4, "rowspan": 3}
    )

    # -- Widget 6: Revenue Composition (donut — category breakdown) --
    # Donut shows proportional composition of pipeline by forecast category.
    # A waterfall would be more appropriate with weekly snapshot data (WFS).
    steps["p1_1_composition"] = sq(
        L_ERF
        + UGF
        + QF
        + "q = foreach q generate "
        + "(case "
        + 'when IsWon == "true" then "Closed Won" '
        + 'when IsClosed == "true" and IsWon == "false" then "Closed Lost" '
        + 'when \'ForecastCategory\' == "Commit" then "Commit" '
        + 'when \'ForecastCategory\' == "Best Case" then "Best Case" '
        + 'when \'ForecastCategory\' == "Pipeline" then "Pipeline" '
        + "else \"Omitted\" end) as Category, 'ARR';\n"
        + "q = group q by Category;\n"
        + "q = foreach q generate Category, sum('ARR') as sum_arr;\n"
        + "q = order q by sum_arr desc;"
    )
    # Treemap: shows proportional size + hierarchy better than donut for 5+ categories
    widgets[f"{pp}_composition"] = treemap_chart(
        "p1_1_composition",
        "Revenue Composition by Category",
        ["Category"],
        "sum_arr",
        show_legend=True,
    )
    layout.append(
        {"name": f"{pp}_composition", "row": 9, "column": 4, "colspan": 8, "rowspan": 8}
    )

    # -- Widget 7: Pipeline by Forecast Category (stackhbar) --
    steps["p1_1_fcat"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + UGF
        + QF
        + "q = group q by 'ForecastCategory';\n"
        + "q = foreach q generate 'ForecastCategory', sum('ARR') as sum_arr, count() as cnt;\n"
        + "q = order q by sum_arr desc;"
    )
    widgets[f"{pp}_fcat"] = rich_chart(
        "p1_1_fcat",
        "hbar",
        "Open Pipeline by Forecast Category",
        ["ForecastCategory"],
        ["sum_arr"],
        axis_title="ARR",
        subtitle="Higher Commit/Best Case = more predictable quarter",
        show_values=True,
        reference_lines=[
            {"value": 3000000, "label": "3x Coverage Target", "color": "#963CE9"}
        ],
    )
    layout.append(
        {"name": f"{pp}_fcat", "row": 17, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 8: Pipeline by Region (hbar) --
    steps["p1_1_region"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + "q = group q by 'UnitGroup';\n"
        + "q = foreach q generate 'UnitGroup', sum('ARR') as sum_arr, count() as cnt;\n"
        + "q = order q by sum_arr desc;"
    )
    widgets[f"{pp}_region"] = rich_chart(
        "p1_1_region",
        "hbar",
        "Open Pipeline by Region",
        ["UnitGroup"],
        ["sum_arr"],
        axis_title="ARR",
        subtitle="Regional distribution of open pipeline ARR",
        show_values=True,
        reference_lines=[
            {"value": 1000000, "label": "Quota Target", "color": "#963CE9"}
        ],
    )
    layout.append(
        {"name": f"{pp}_region", "row": 17, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 9: Opportunities Needing Attention (RI pattern) --
    # Surfaces deals with risk signals: stuck in stage, high push count, large ARR at risk
    # Priority score = ARR × risk factors (high days + high pushes = higher priority)
    steps["p1_1_needs_attention"] = sq(
        L_POO
        + 'q = filter q by IsClosed == "false";\n'
        + UGF
        + "q = filter q by ('DaysInStage' > 45 or 'PushCount' >= 2);\n"
        + "q = foreach q generate 'Id', 'OpportunityName', 'OwnerName', "
        + "'StageName', 'WeightedOpenARR', 'AccountName', "
        + "'ForecastCategory', 'DaysInStage', 'PushCount', "
        + "(case "
        + "when 'DaysInStage' > 90 and 'PushCount' >= 3 then \"Critical\" "
        + "when 'DaysInStage' > 60 or 'PushCount' >= 3 then \"High\" "
        + 'else "Medium" end) as RiskLevel, '
        + "('WeightedOpenARR' * ('DaysInStage' + 'PushCount' * 30)) as attention_score;\n"
        + "q = order q by attention_score desc;\n"
        + "q = limit q 15;"
    )
    widgets[f"{pp}_needs_attention"] = add_table_action(
        rich_chart(
            "p1_1_needs_attention",
            "comparisontable",
            "Needs Attention",
            [
                "RiskLevel",
                "OpportunityName",
                "OwnerName",
                "StageName",
                "ForecastCategory",
            ],
            ["WeightedOpenARR", "DaysInStage", "PushCount"],
            subtitle="Deals with risk signals — stuck or frequently pushed",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysInStage",
                    "rules": [
                        {"value": 90, "color": "#D4504C", "operator": "gte"},
                        {"value": 45, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "PushCount",
                    "rules": [
                        {"value": 3, "color": "#D4504C", "operator": "gte"},
                        {"value": 2, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        )
    )
    layout.append(
        {
            "name": f"{pp}_needs_attention",
            "row": 12,
            "column": 0,
            "colspan": 4,
            "rowspan": 5,
        }
    )

    # -- Widget 10: Pipeline Changes Waterfall (RI Pipeline Inspection) --
    # Shows: Opening Pipeline → +New → +Increased → -Decreased → -Won → -Lost → =Closing
    # Uses POO dataset which has pipeline change categories
    steps["p1_1_pipe_waterfall"] = sq(
        L_POO
        + UGF
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "sum(case when 'PipelineChangeCategory' == \"Opening\" then 'ARR' else 0 end) as Opening, "
        + "sum(case when 'PipelineChangeCategory' == \"New\" then 'ARR' else 0 end) as New_Pipeline, "
        + "sum(case when 'PipelineChangeCategory' == \"Increased\" then 'ARR' else 0 end) as Increased, "
        + "sum(case when 'PipelineChangeCategory' == \"Decreased\" then 'ARR' else 0 end) as Decreased, "
        + "sum(case when 'PipelineChangeCategory' == \"Won\" then 'ARR' else 0 end) as Won, "
        + "sum(case when 'PipelineChangeCategory' == \"Lost\" then 'ARR' else 0 end) as Lost, "
        + "sum(case when IsClosed == \"false\" then 'ARR' else 0 end) as Closing;"
    )
    widgets[f"{pp}_waterfall"] = waterfall_chart(
        "p1_1_pipe_waterfall",
        "Pipeline Changes (Waterfall)",
        "Category",
        "ARR",
        axis_label="ARR",
    )
    layout.append(
        {"name": f"{pp}_waterfall", "row": 12, "column": 4, "colspan": 8, "rowspan": 5}
    )

    # -- Widget 11: Pipeline Velocity KPI (filter-responsive) --
    # Velocity = (# opps × avg deal × win rate) / avg cycle
    # Responds to type filter so you can compare Land velocity vs Expand velocity
    steps["p1_1_velocity"] = sq(
        L_PA
        + PA_CLOSED
        + TYF
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "count() as total_closed, "
        + 'sum(case when IsWon == "true" then 1 else 0 end) as won_count, '
        + "avg('ARR') as avg_deal, "
        + "avg(case when IsWon == \"true\" then 'AgeInDays' else null end) as avg_cycle, "
        + "(case when avg(case when IsWon == \"true\" then 'AgeInDays' else null end) > 0 "
        + 'then (sum(case when IsWon == "true" then 1 else 0 end) '
        + "* avg('ARR') "
        + '* (sum(case when IsWon == "true" then 1 else 0 end) / count()) '
        + ") / avg(case when IsWon == \"true\" then 'AgeInDays' else null end) "
        + "else 0 end) as velocity;",
    )
    steps["p1_1_velocity"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_velocity"] = num(
        "p1_1_velocity",
        "velocity",
        "Pipeline Velocity",
        "#0070D2",
        tier="secondary",
        prefix="$",
        suffix="/day",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_velocity", "row": 9, "column": 4, "colspan": 4, "rowspan": 3}
    )

    # -- Widget 12: Avg Days to Close (filter-responsive) --
    steps["p1_1_avg_days"] = sq(
        L_PA
        + PA_WON
        + TYF
        + "q = filter q by 'AgeInDays' > 0;\n"
        + "q = group q by all;\n"
        + "q = foreach q generate avg('AgeInDays') as avg_cycle;",
    )
    steps["p1_1_avg_days"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_avg_days"] = num(
        "p1_1_avg_days",
        "avg_cycle",
        "Avg Sales Cycle",
        "#091A3E",
        tier="secondary",
        suffix=" days",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_avg_days", "row": 9, "column": 8, "colspan": 4, "rowspan": 3}
    )

    # -- Section label --
    widgets[f"{pp}_sec_pipe"] = section_label("Pipeline Breakdown")
    layout.append(
        {"name": f"{pp}_sec_pipe", "row": 16, "column": 0, "colspan": 12, "rowspan": 1}
    )

    page = pg("p1_1", "Revenue Command Center", layout)
    return steps, widgets, page


# ═══════════════════════════════════════════════════════════════════════════
#  Page 1.2 — Pipeline Health & Velocity
# ═══════════════════════════════════════════════════════════════════════════


def page_1_2():
    """Pipeline Health & Velocity.

    Story: Is our pipeline healthy enough to hit target, and where is it stuck?
    """
    steps = {}
    widgets = {}
    pp = "p1_2"

    w, layout = _standard_header(
        pp,
        1,
        "Pipeline Health & Velocity",
        "Is our pipeline healthy? Where is it stuck?",
    )
    widgets.update(w)

    # -- KPI 1: Open Pipeline ARR --
    steps["p1_2_open_pipe"] = sq(
        L_PA
        + PA_OPEN
        + "q = group q by all;\n"
        + "q = foreach q generate sum('ARR') as sum_arr, count() as cnt;"
    )
    steps["p1_2_open_pipe"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_open_pipe"] = num(
        "p1_2_open_pipe",
        "sum_arr",
        "Open Pipeline ARR",
        "#0070D2",
        compact=True,
        tier="primary",
        prefix="$",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_open_pipe", "row": 5, "column": 0, "colspan": 3, "rowspan": 3}
    )

    # -- KPI 2: Avg Deal Size (Won) --
    steps["p1_2_avg_deal"] = sq(
        L_PA
        + PA_WON
        + "q = group q by all;\n"
        + "q = foreach q generate avg('ARR') as avg_deal;"
    )
    steps["p1_2_avg_deal"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_avg_deal"] = num(
        "p1_2_avg_deal",
        "avg_deal",
        "Avg Deal Size (Won)",
        "#04844B",
        compact=True,
        tier="secondary",
        prefix="$",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_avg_deal", "row": 5, "column": 3, "colspan": 3, "rowspan": 3}
    )

    # -- KPI 3: Avg Sales Cycle --
    steps["p1_2_avg_cycle"] = sq(
        L_PA
        + PA_WON
        + "q = filter q by 'AgeInDays' > 0;\n"
        + "q = group q by all;\n"
        + "q = foreach q generate avg('AgeInDays') as avg_cycle;"
    )
    steps["p1_2_avg_cycle"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_avg_cycle"] = num(
        "p1_2_avg_cycle",
        "avg_cycle",
        "Avg Sales Cycle",
        "#0070D2",
        tier="secondary",
        suffix=" days",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_avg_cycle", "row": 5, "column": 6, "colspan": 3, "rowspan": 3}
    )

    # -- KPI 4: Stale Pipeline % --
    steps["p1_2_stale_pct"] = sq(
        L_PA
        + PA_OPEN
        + "q = foreach q generate "
        + "(case when 'AgeInDays' > 120 then 1 else 0 end) as is_stale;\n"
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "(sum(is_stale) / count()) * 100 as stale_pct, "
        + "sum(is_stale) as stale_count, count() as total;"
    )
    steps["p1_2_stale_pct"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_stale_pct"] = num(
        "p1_2_stale_pct",
        "stale_pct",
        "Stale Pipeline",
        "#D4504C",
        tier="secondary",
        suffix="%",
        sentiment_color=True,
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_stale_pct", "row": 5, "column": 9, "colspan": 3, "rowspan": 3}
    )

    # -- Widget 5: Pipeline Funnel --
    # Order by StageOrder (stage progression), not ARR — funnel must follow sales process
    steps["p1_2_funnel"] = sq(
        L_PA
        + PA_OPEN
        + "q = group q by ('StageName', 'StageOrder');\n"
        + "q = foreach q generate 'StageName', 'StageOrder', sum('ARR') as sum_arr;\n"
        + "q = order q by 'StageOrder' asc;"
    )
    widgets[f"{pp}_funnel"] = funnel_chart(
        "p1_2_funnel", "Pipeline Funnel by Stage", "StageName", "sum_arr"
    )
    layout.append(
        {"name": f"{pp}_funnel", "row": 8, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 6: Stage Flow (Sankey — pipeline transitions) --
    # Sankey shows flow volume between stages — instantly reveals where deals progress vs drop
    steps["p1_2_conv"] = sq(
        L_PT
        + "q = group q by ('FromStage', 'ToStage');\n"
        + "q = foreach q generate 'FromStage' as source, 'ToStage' as target, "
        + "sum('TransitionCount') as cnt;\n"
        + "q = filter q by cnt > 0;\n"
        + "q = order q by cnt desc;\n"
        + "q = limit q 50;"
    )
    widgets[f"{pp}_conv"] = sankey_chart(
        "p1_2_conv",
        "Pipeline Stage Flow",
        source_field="source",
        target_field="target",
        measure_field="cnt",
    )
    layout.append(
        {"name": f"{pp}_conv", "row": 8, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 7: Time in Stage (hbar, Land vs Expand) --
    steps["p1_2_time_stage"] = sq(
        L_POO
        + 'q = filter q by IsClosed == "false";\n'
        + UGF
        + "q = group q by 'StageName';\n"
        + "q = foreach q generate 'StageName', "
        + "avg('DaysInStage') as avg_days, count() as cnt;\n"
        + "q = order q by 'StageName' asc;"
    )
    widgets[f"{pp}_time_stage"] = rich_chart(
        "p1_2_time_stage",
        "hbar",
        "Avg Time in Stage (Days)",
        ["StageName"],
        ["avg_days"],
        axis_title="Days",
        subtitle="Stages with highest dwell time are velocity bottlenecks",
        show_values=True,
        reference_lines=[
            {"value": 30, "label": "30-Day Threshold", "color": "#FFB75D"}
        ],
    )
    layout.append(
        {"name": f"{pp}_time_stage", "row": 16, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 8: Pipeline Created vs Closed Won by Month (combo) --
    # Created = opps created in that month (by CreatedDate), Won = closed won in that month
    steps["p1_2_create_vs_close"] = sq(
        L_PA
        + "q = foreach q generate "
        + "'CreatedDate_Year' || \"-\" || 'CreatedDate_Month' as created_month, "
        + "'CloseDate_Year' || \"-\" || 'CloseDate_Month' as close_month, "
        + "'ARR', 'IsWon';\n"
        + "q1 = group q by created_month;\n"
        + "q1 = foreach q1 generate created_month as Month, sum('ARR') as created_pipeline;\n"
        + 'q2 = filter q by IsWon == "true";\n'
        + "q2 = group q2 by close_month;\n"
        + "q2 = foreach q2 generate close_month as Month, sum('ARR') as closed_won_arr;\n"
        + "r = cogroup q1 by Month, q2 by Month;\n"
        + "r = foreach r generate coalesce(q1.Month, q2.Month) as Month, "
        + "coalesce(sum(q1.created_pipeline), 0) as created_pipeline, "
        + "coalesce(sum(q2.closed_won_arr), 0) as closed_won_arr;\n"
        + "r = order r by Month asc;\n"
        + "r = limit r 12;"
    )
    widgets[f"{pp}_create_close"] = combo_chart(
        "p1_2_create_vs_close",
        "Pipeline Created vs Closed Won (Monthly)",
        ["Month"],
        ["created_pipeline"],
        ["closed_won_arr"],
        show_legend=True,
        axis_title="ARR",
        axis1_format="$#,##0",
        subtitle="Created > Won = healthy pipeline generation; Won > Created = drawing down reserves",
    )
    layout.append(
        {
            "name": f"{pp}_create_close",
            "row": 16,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        }
    )

    # -- Widget 9: Stage x Age Heatmap --
    # Heatmap reveals concentration patterns: which stages have the oldest deals
    steps["p1_2_aging"] = sq(
        L_PA
        + PA_OPEN
        + "q = group q by ('StageName', 'AgeBucket');\n"
        + "q = foreach q generate 'StageName', 'AgeBucket', sum('ARR') as sum_arr;\n"
        + "q = order q by 'StageName' asc;"
    )
    widgets[f"{pp}_aging"] = heatmap_chart(
        "p1_2_aging",
        "Pipeline Risk: Stage x Age Heatmap",
    )
    layout.append(
        {"name": f"{pp}_aging", "row": 24, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 10: Stale Deals Table --
    steps["p1_2_stale_deals"] = sq(
        L_POO
        + 'q = filter q by IsClosed == "false";\n'
        + "q = filter q by 'DaysInStage' > 90;\n"
        + UGF
        + "q = foreach q generate 'Id', 'OpportunityName', 'OwnerName', "
        + "'StageName', 'WeightedOpenARR', 'DaysInStage', 'AccountName';\n"
        + "q = order q by 'WeightedOpenARR' desc;\n"
        + "q = limit q 20;"
    )
    widgets[f"{pp}_stale_deals"] = add_table_action(
        rich_chart(
            "p1_2_stale_deals",
            "comparisontable",
            "Stale Deals (>90 Days Same Stage)",
            ["OpportunityName", "OwnerName", "StageName", "AccountName"],
            ["WeightedOpenARR", "DaysInStage"],
            subtitle="Sorted by ARR — highest-value stalled deals need immediate attention",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysInStage",
                    "rules": [
                        {"value": 180, "color": "#D4504C", "operator": "gte"},
                        {"value": 120, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        )
    )
    layout.append(
        {
            "name": f"{pp}_stale_deals",
            "row": 24,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        }
    )

    # -- Widget 11: Win Rate Trend (rolling 90-day line) --
    # Deep-dive metric: win rate by month, so managers can see trajectory
    steps["p1_2_wr_trend"] = sq(
        L_PA
        + PA_CLOSED
        + TYF
        + "q = group q by ('CloseDate_Year', 'CloseDate_Month');\n"
        + "q = foreach q generate "
        + "'CloseDate_Year' || \"-\" || 'CloseDate_Month' as Month, "
        + '(sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100 as win_rate, '
        + "count() as deal_count;\n"
        + "q = order q by Month asc;\n"
        + "q = foreach q generate Month, win_rate, deal_count, "
        + "avg(win_rate) over ([-2..0] partition by all order by (Month)) as rolling_3m_wr;\n"
        + "q = limit q 24;"
    )
    widgets[f"{pp}_wr_trend"] = combo_chart(
        "p1_2_wr_trend",
        "Win Rate Trend (3-Month Rolling Avg)",
        ["Month"],
        ["win_rate"],
        ["rolling_3m_wr"],
        show_legend=True,
        axis_title="Win Rate %",
        axis2_title="3M Rolling Avg",
        subtitle="Monthly win rate with smoothed trendline — is it improving?",
        reference_lines=[{"value": 25, "label": "25% Benchmark", "color": "#963CE9"}],
        axis1_format="0.0%",
        axis2_format="0.0%",
    )
    layout.append(
        {"name": f"{pp}_wr_trend", "row": 32, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 12: Win Rate by Deal Size Tier --
    # Sliced intel: do we win differently on large vs small deals?
    steps["p1_2_wr_by_tier"] = sq(
        L_PA
        + PA_CLOSED
        + TYF
        + "q = foreach q generate "
        + "(case "
        + "when 'ARR' < 50000 then \"< 50K\" "
        + "when 'ARR' < 100000 then \"50K-100K\" "
        + "when 'ARR' < 500000 then \"100K-500K\" "
        + 'else "500K+" end) as ValueTier, '
        + '(case when IsWon == "true" then 1 else 0 end) as is_won;\n'
        + "q = group q by ValueTier;\n"
        + "q = foreach q generate ValueTier, "
        + "(sum(is_won) / count()) * 100 as win_rate, "
        + "count() as total_deals;\n"
        + "q = order q by ValueTier asc;"
    )
    widgets[f"{pp}_wr_by_tier"] = rich_chart(
        "p1_2_wr_by_tier",
        "hbar",
        "Win Rate by Deal Size Tier",
        ["ValueTier"],
        ["win_rate"],
        axis_title="Win Rate %",
        subtitle="Are we better at closing small or large deals?",
        show_values=True,
        reference_lines=[{"value": 25, "label": "25% Benchmark", "color": "#963CE9"}],
    )
    layout.append(
        {"name": f"{pp}_wr_by_tier", "row": 32, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 13: Stage Conversion Rates --
    # Shows conversion rate at each stage: what % of deals that enter a stage eventually win?
    steps["p1_2_stage_conv"] = sq(
        L_PA
        + PA_CLOSED
        + TYF
        + "q = group q by 'StageName';\n"
        + "q = foreach q generate 'StageName', "
        + "count() as total_deals, "
        + 'sum(case when IsWon == "true" then 1 else 0 end) as won_deals, '
        + '(sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100 as stage_win_rate;\n'
        + "q = order q by 'StageName' asc;"
    )
    widgets[f"{pp}_stage_conv"] = rich_chart(
        "p1_2_stage_conv",
        "hbar",
        "Win Rate by Entry Stage",
        ["StageName"],
        ["stage_win_rate"],
        axis_title="Win Rate %",
        subtitle="Which stages are leaking deals? Lower = more fallout",
        show_values=True,
        reference_lines=[{"value": 25, "label": "25% Benchmark", "color": "#963CE9"}],
    )
    layout.append(
        {"name": f"{pp}_stage_conv", "row": 40, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 14: Pipeline Velocity by Stage (avg days × conversion × avg deal) --
    # Shows velocity contribution per stage — identifies bottleneck stages
    steps["p1_2_stage_velocity"] = sq(
        L_PA
        + PA_CLOSED
        + TYF
        + "q = filter q by 'AgeInDays' > 0;\n"
        + "q = group q by 'StageName';\n"
        + "q = foreach q generate 'StageName', "
        + "avg('AgeInDays') as avg_cycle, "
        + "avg('ARR') as avg_deal, "
        + "count() as volume;\n"
        + "q = order q by avg_cycle desc;"
    )
    widgets[f"{pp}_stage_velocity"] = scatter_chart(
        "p1_2_stage_velocity",
        "Stage Velocity: Cycle Days vs Avg Deal Size",
        x_title="Avg Cycle (Days)",
        y_title="Avg Deal ARR",
        show_legend=True,
    )
    layout.append(
        {
            "name": f"{pp}_stage_velocity",
            "row": 40,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        }
    )

    page = pg("p1_2", "Pipeline Health & Velocity", layout)
    return steps, widgets, page


# ═══════════════════════════════════════════════════════════════════════════
#  Page 1.3 — Forecast Analysis
# ═══════════════════════════════════════════════════════════════════════════


def page_1_3():
    """Forecast Analysis.

    Story: How reliable is our forecast, how has it changed, and what deals
    make up the commit?

    NOTE: Widgets 1-5, 7-8, 10 require Weekly_Forecast_Summary (WFS) and
    Weekly_Forecast_Opps (WFO) datasets. These are placeholders until those
    datasets are built. Replace WFS/WFO constants with actual IDs.
    """
    steps = {}
    widgets = {}
    pp = "p1_3"

    w, layout = _standard_header(
        pp,
        2,
        "Forecast Analysis",
        "How reliable is our forecast? What changed this week?",
    )
    widgets.update(w)

    # -- KPI 1: Total Commit ARR --
    # TODO: Upgrade to WoW change once Weekly_Forecast_Summary dataset is built
    steps["p1_3_commit_wow"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + "q = filter q by 'ForecastCategory' == \"Commit\";\n"
        + UGF
        + QF
        + "q = group q by all;\n"
        + "q = foreach q generate sum('ARR') as commit_arr, count() as cnt;"
    )
    steps["p1_3_commit_wow"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_commit_wow"] = num(
        "p1_3_commit_wow",
        "commit_arr",
        "Commit ARR",
        "#0070D2",
        compact=True,
        tier="primary",
        prefix="$",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_commit_wow", "row": 5, "column": 0, "colspan": 3, "rowspan": 3}
    )

    # -- KPI 2: Total Best Case ARR --
    steps["p1_3_bestcase_wow"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + "q = filter q by 'ForecastCategory' == \"Best Case\";\n"
        + UGF
        + QF
        + "q = group q by all;\n"
        + "q = foreach q generate sum('ARR') as bestcase_arr, count() as cnt;"
    )
    steps["p1_3_bestcase_wow"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_bestcase_wow"] = num(
        "p1_3_bestcase_wow",
        "bestcase_arr",
        "Best Case ARR",
        "#04844B",
        compact=True,
        tier="primary",
        prefix="$",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {
            "name": f"{pp}_bestcase_wow",
            "row": 5,
            "column": 3,
            "colspan": 3,
            "rowspan": 3,
        }
    )

    # -- KPI 3: Deals Pushed This Week --
    steps["p1_3_pushed"] = sq(
        L_PA
        + PA_OPEN
        + "q = filter q by 'PushCount' > 0;\n"
        + "q = group q by all;\n"
        + "q = foreach q generate count() as push_count, sum('ARR') as pushed_arr;"
    )
    steps["p1_3_pushed"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_pushed"] = num(
        "p1_3_pushed",
        "push_count",
        "Deals Pushed",
        "#D4504C",
        tier="secondary",
        sentiment_color=True,
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_pushed", "row": 5, "column": 6, "colspan": 3, "rowspan": 3}
    )

    # -- KPI 4: Forecast Accuracy (Prior Q) — GLOBAL BENCHMARK --
    steps["p1_3_accuracy"] = {
        **sq(
            L_ERF
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by 'CloseQuarter';\n"
            + "q = foreach q generate 'CloseQuarter', sum('ARR') as actual_arr;\n"
            + f'q2 = load "{ERF}";\n'
            + "q2 = group q2 by ('CloseQuarter', 'OwnerName');\n"
            + "q2 = foreach q2 generate 'CloseQuarter', max('QuotaAmount') as rep_quota;\n"
            + "q2 = group q2 by 'CloseQuarter';\n"
            + "q2 = foreach q2 generate 'CloseQuarter', sum(rep_quota) as forecast_arr;\n"
            + "r = cogroup q by 'CloseQuarter', q2 by 'CloseQuarter';\n"
            + "r = foreach r generate q.'CloseQuarter' as Quarter, "
            + "coalesce(sum(q.actual_arr), 0) as actual, "
            + "coalesce(sum(q2.forecast_arr), 1) as forecast, "
            + "(case when coalesce(sum(q2.forecast_arr), 0) > 0 "
            + "then (coalesce(sum(q.actual_arr), 0) / coalesce(sum(q2.forecast_arr), 1)) * 100 "
            + "else 0 end) as accuracy;\n"
            + "r = order r by Quarter desc;\n"
            + "r = limit r 1;",
            broadcast=False,
        ),
        **ISOLATION_QUAD,
    }
    widgets[f"{pp}_accuracy"] = num(
        "p1_3_accuracy",
        "accuracy",
        "Forecast Accuracy (Prior Q)",
        "#9050E9",
        tier="secondary",
        suffix="%",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_accuracy", "row": 5, "column": 9, "colspan": 3, "rowspan": 3}
    )

    # -- Widget 5: Cumulative Closed Won + Commit Build + Quota Target (combo) --
    # Shows cumulative ARR building through the fiscal year by month
    # Window function syntax: sum(sum('field')) over() — inner sum = group agg, outer = running total
    # Quota line computed as running fraction of total quota (linear ramp)
    steps["p1_3_forecast_timeline"] = sq(
        L_ERF
        + UGF
        + "q = group q by ('MonthLabel', 'OwnerName');\n"
        + "q = foreach q generate 'MonthLabel', "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end) as won_arr, "
        + "sum('CommitCallARR') as commit_arr, "
        + "sum('BestCaseCallARR') as bestcase_arr, "
        + "max('QuotaAmount') as rep_quota;\n"
        + "q = group q by 'MonthLabel';\n"
        + "q = foreach q generate 'MonthLabel', "
        + "sum(won_arr) as period_won, "
        + "sum(commit_arr) as period_commit, "
        + "sum(bestcase_arr) as period_bestcase, "
        + "sum(rep_quota) as period_quota;\n"
        + "q = foreach q generate 'MonthLabel', "
        + "sum(period_won) over ([..0] partition by all order by ('MonthLabel')) as cumulative_won, "
        + "sum(period_commit) over ([..0] partition by all order by ('MonthLabel')) as cumulative_commit, "
        + "sum(period_bestcase) over ([..0] partition by all order by ('MonthLabel')) as cumulative_bestcase, "
        + "sum(period_quota) over ([..0] partition by all order by ('MonthLabel')) as quota_target;"
    )
    # Combo: cumulative actuals as area fill + quota target as dashed line
    widgets[f"{pp}_timeline"] = combo_chart(
        "p1_3_forecast_timeline",
        "Cumulative Revenue Build vs Quota Target",
        ["MonthLabel"],
        ["cumulative_won", "cumulative_commit", "cumulative_bestcase"],
        ["quota_target"],
        show_legend=True,
        axis_title="ARR (Cumulative)",
        axis2_title="Quota Target",
        subtitle="Closed Won + Commit + Best Case trajectories toward quota line",
        axis1_format="$#,##0",
        axis2_format="$#,##0",
    )
    layout.append(
        {"name": f"{pp}_timeline", "row": 8, "column": 0, "colspan": 12, "rowspan": 8}
    )

    # -- Widget 6: YoY Cumulative Velocity — GLOBAL BENCHMARK --
    # Trellis by fiscal year: each panel shows cumulative won by month within that year
    steps["p1_3_yoy_velocity"] = {
        **sq(
            L_ERF
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by ('MonthDate_Month', 'FYLabel');\n"
            + "q = foreach q generate 'MonthDate_Month', 'FYLabel', "
            + "sum(sum('ARR')) over ([..0] partition by 'FYLabel' "
            + "order by ('MonthDate_Month')) as cumulative_won;",
            broadcast=False,
        ),
        **ISOLATION_QUAD,
    }
    widgets[f"{pp}_yoy"] = {
        "type": "chart",
        "parameters": {
            "step": "p1_3_yoy_velocity",
            "visualizationType": "line",
            "title": {
                "label": "YoY Cumulative Closed Won",
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "Each panel = one fiscal year",
            },
            "theme": "wave",
            "exploreLink": True,
            "autoFitMode": "fit",
            "columnMap": {
                "dimensionAxis": ["MonthDate_Month"],
                "plots": ["cumulative_won"],
                "trellis": ["FYLabel"],
                "split": [],
            },
            "measureAxis1": {
                "showTitle": True,
                "showAxis": True,
                "title": "Cumulative ARR",
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
                "numberFormat": "$#,##0",
            },
            "legend": {"show": False},
            "interactions": [],
        },
    }
    layout.append(
        {"name": f"{pp}_yoy", "row": 16, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 7: Category Migration (stackhbar) --
    # Shows distribution of deals by forecast category with push counts
    steps["p1_3_cat_migration"] = sq(
        L_PA
        + PA_OPEN
        + "q = group q by ('ForecastCategory', 'PushBucket');\n"
        + "q = foreach q generate 'ForecastCategory', 'PushBucket', "
        + "sum('ARR') as sum_arr, count() as cnt;\n"
        + "q = order q by sum_arr desc;"
    )
    widgets[f"{pp}_cat_migration"] = rich_chart(
        "p1_3_cat_migration",
        "stackhbar",
        "Forecast Risk: Category x Push Count",
        ["ForecastCategory"],
        ["sum_arr"],
        split=["PushBucket"],
        show_legend=True,
        axis_title="ARR",
        subtitle="Commit deals with high push count = forecast risk",
    )
    layout.append(
        {
            "name": f"{pp}_cat_migration",
            "row": 16,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        }
    )

    # -- Widget 8: Push Count Distribution (hbar) --
    steps["p1_3_push_dist"] = sq(
        L_PA
        + PA_OPEN
        + "q = group q by 'PushBucket';\n"
        + "q = foreach q generate 'PushBucket', count() as cnt, sum('ARR') as sum_arr;\n"
        + "q = order q by 'PushBucket' asc;"
    )
    widgets[f"{pp}_push_dist"] = rich_chart(
        "p1_3_push_dist",
        "hbar",
        "Push Count Distribution",
        ["PushBucket"],
        ["cnt"],
        axis_title="Deal Count",
        subtitle="More deals in 3+ pushes = systemic forecasting issue",
        show_values=True,
    )
    layout.append(
        {"name": f"{pp}_push_dist", "row": 24, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 9: Commit List (compare table with rank) --
    steps["p1_3_commit_list"] = sq(
        L_FRM
        + 'q = filter q by IsClosed == "false";\n'
        + 'q = filter q by \'ForecastCategory\' in ["Commit", "Best Case"];\n'
        + UGF
        + "q = foreach q generate 'Id', 'OpportunityName', 'OwnerName', "
        + "'ARR', 'CloseQuarter', 'ForecastCategory', 'RiskScore', "
        + "'WinScore', 'PushCount', 'AccountName';\n"
        + "q = foreach q generate "
        + "rank() over ([..] partition by all order by 'ARR' desc) as Rank, "
        + "'OpportunityName', 'OwnerName', 'ARR', 'CloseQuarter', "
        + "'ForecastCategory', 'RiskScore', 'WinScore', 'PushCount', 'AccountName';\n"
        + "q = order q by 'ARR' desc;\n"
        + "q = limit q 50;"
    )
    widgets[f"{pp}_commit_list"] = add_table_action(
        rich_chart(
            "p1_3_commit_list",
            "comparisontable",
            "Commit & Best Case Deals",
            [
                "Rank",
                "OpportunityName",
                "OwnerName",
                "ForecastCategory",
                "CloseQuarter",
                "AccountName",
            ],
            ["ARR", "WinScore", "RiskScore", "PushCount"],
            subtitle="Red = high risk, green = strong win probability",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "RiskScore",
                    "rules": [
                        {"value": 70, "color": "#D4504C", "operator": "gte"},
                        {"value": 40, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "WinScore",
                    "rules": [
                        {"value": 70, "color": "#04844B", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "PushCount",
                    "rules": [
                        {"value": 3, "color": "#D4504C", "operator": "gte"},
                    ],
                },
            ],
        )
    )
    layout.append(
        {
            "name": f"{pp}_commit_list",
            "row": 24,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        }
    )

    # -- Widget 10: Pushed Deals Table --
    steps["p1_3_pushed_deals"] = sq(
        L_PA
        + PA_OPEN
        + "q = filter q by 'PushCount' >= 3;\n"
        + "q = foreach q generate 'Id', 'Name', 'OwnerName', "
        + "'ARR', 'StageName', 'PushCount', 'AccountName', 'ForecastCategory';\n"
        + "q = order q by 'ARR' desc;\n"
        + "q = limit q 25;"
    )
    widgets[f"{pp}_pushed_deals"] = add_table_action(
        rich_chart(
            "p1_3_pushed_deals",
            "comparisontable",
            "Frequently Pushed Deals (3+ Pushes)",
            ["Name", "OwnerName", "StageName", "ForecastCategory", "AccountName"],
            ["ARR", "PushCount"],
            subtitle="Chronic slippage — these deals may need re-qualification",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "PushCount",
                    "rules": [
                        {"value": 5, "color": "#D4504C", "operator": "gte"},
                        {"value": 3, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        )
    )
    layout.append(
        {
            "name": f"{pp}_pushed_deals",
            "row": 32,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        }
    )

    # -- Widget 11: Forecast Category Composition (stackvbar over time) --
    # Shows how forecast mix changes quarter to quarter
    steps["p1_3_fcat_trend"] = sq(
        L_ERF
        + UGF
        + "q = group q by ('CloseQuarter', 'ForecastCategory');\n"
        + "q = foreach q generate 'CloseQuarter', 'ForecastCategory', "
        + "sum('ARR') as sum_arr;\n"
        + "q = order q by 'CloseQuarter' asc;"
    )
    widgets[f"{pp}_fcat_trend"] = rich_chart(
        "p1_3_fcat_trend",
        "stackvbar",
        "Forecast Mix by Quarter",
        ["CloseQuarter"],
        ["sum_arr"],
        split=["ForecastCategory"],
        show_legend=True,
        axis_title="ARR",
        subtitle="Composition shift: more Commit = higher confidence",
        normalize=True,
    )
    layout.append(
        {"name": f"{pp}_fcat_trend", "row": 40, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 12: Commit vs Best Case vs Pipeline Trend (line) --
    # Simple time series showing the three forecast categories separately
    steps["p1_3_fcat_lines"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + UGF
        + "q = group q by 'CloseQuarter';\n"
        + "q = foreach q generate 'CloseQuarter', "
        + "sum(case when 'ForecastCategory' == \"Commit\" then 'ARR' else 0 end) as commit_arr, "
        + "sum(case when 'ForecastCategory' == \"Best Case\" then 'ARR' else 0 end) as bestcase_arr, "
        + "sum(case when 'ForecastCategory' == \"Pipeline\" then 'ARR' else 0 end) as pipeline_arr;\n"
        + "q = order q by 'CloseQuarter' asc;"
    )
    widgets[f"{pp}_fcat_lines"] = combo_chart(
        "p1_3_fcat_lines",
        "Forecast Categories Over Time",
        ["CloseQuarter"],
        ["commit_arr", "bestcase_arr"],
        ["pipeline_arr"],
        show_legend=True,
        axis_title="ARR (Commit/Best Case)",
        axis2_title="Pipeline ARR",
        subtitle="Commit + Best Case as bars, Pipeline as line",
        axis1_format="$#,##0",
        axis2_format="$#,##0",
    )
    layout.append(
        {"name": f"{pp}_fcat_lines", "row": 40, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 14: Pipeline Flow (Sankey) — Forecast Category Movement --
    # Shows deal flow between forecast categories: how much ARR moved from
    # Pipeline→Best Case→Commit→Closed Won (and backwards movements)
    # NOTE: Requires Previous_ForecastCategory field in dataset (snapshot delta)
    steps["p1_3_pipeline_flow"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + UGF
        + QF
        + "q = filter q by 'Previous_ForecastCategory' is not null;\n"
        + "q = group q by ('Previous_ForecastCategory', 'ForecastCategory');\n"
        + "q = foreach q generate 'Previous_ForecastCategory' as source, "
        + "'ForecastCategory' as target, "
        + "sum('ARR') as flow_arr, count() as cnt;\n"
        + "q = order q by flow_arr desc;"
    )
    widgets[f"{pp}_pipeline_flow"] = sankey_chart(
        "p1_3_pipeline_flow",
        "Pipeline Flow: Forecast Category Movement",
        subtitle="Left=previous category, right=current — backward flows indicate forecast risk",
    )
    layout.append(
        {
            "name": f"{pp}_pipeline_flow",
            "row": 48,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        }
    )

    page = pg("p1_3", "Forecast Analysis", layout)
    return steps, widgets, page


# ═══════════════════════════════════════════════════════════════════════════
#  Page 1.4 — Deal Composition & Sizing
# ═══════════════════════════════════════════════════════════════════════════


def page_1_4():
    """Deal Composition & Sizing.

    Story: What is our deal mix, and are we winning the right deals?
    """
    steps = {}
    widgets = {}
    pp = "p1_4"

    w, layout = _standard_header(
        pp,
        3,
        "Deal Composition & Sizing",
        "What is our deal mix? Are we winning the right deals?",
    )
    widgets.update(w)

    # -- Widget 1: Won Deals by Value Tier (stackhbar) --
    steps["p1_4_won_tier"] = sq(
        L_PA
        + PA_WON
        + "q = foreach q generate "
        + "(case "
        + "when 'ARR' < 50000 then \"< 50K\" "
        + "when 'ARR' < 100000 then \"50K-100K\" "
        + "when 'ARR' < 500000 then \"100K-500K\" "
        + "else \"500K+\" end) as ValueTier, 'ARR';\n"
        + "q = group q by ValueTier;\n"
        + "q = foreach q generate ValueTier, count() as cnt, sum('ARR') as sum_arr;\n"
        + "q = order q by ValueTier asc;"
    )
    # Treemap: proportional area shows deal tier concentration at a glance
    widgets[f"{pp}_won_tier"] = treemap_chart(
        "p1_4_won_tier",
        "Won ARR by Deal Size Tier",
        ["ValueTier"],
        "sum_arr",
        show_legend=True,
    )
    layout.append(
        {"name": f"{pp}_won_tier", "row": 5, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 2: Avg Deal Size Trend (line) --
    steps["p1_4_avg_trend"] = sq(
        L_ERF
        + 'q = filter q by IsWon == "true";\n'
        + UGF
        + "q = group q by 'MonthLabel';\n"
        + "q = foreach q generate 'MonthLabel', avg('ARR') as avg_deal, count() as cnt;\n"
        + "q = order q by 'MonthLabel' asc;"
    )
    widgets[f"{pp}_avg_trend"] = line_chart(
        "p1_4_avg_trend",
        "Won Avg Deal Size Trend",
        show_legend=False,
        axis_title="ARR",
        reference_lines=[
            {"value": 100000, "label": "Portfolio Avg ($100K)", "color": "#963CE9"}
        ],
        subtitle="Rising trend = selling larger deals; watch for erosion below portfolio avg",
    )
    layout.append(
        {"name": f"{pp}_avg_trend", "row": 5, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 3: Land vs Expand Mix (stackvbar) --
    steps["p1_4_land_expand"] = sq(
        L_PA
        + PA_WON
        + "q = group q by ('FiscalQuarter', 'Type');\n"
        + "q = foreach q generate 'FiscalQuarter', 'Type', sum('ARR') as sum_arr;\n"
        + "q = order q by 'FiscalQuarter' asc;"
    )
    widgets[f"{pp}_land_expand"] = rich_chart(
        "p1_4_land_expand",
        "stackvbar",
        "Won ARR: Land vs Expand by Quarter",
        ["FiscalQuarter"],
        ["sum_arr"],
        split=["Type"],
        show_legend=True,
        axis_title="ARR",
        subtitle="Healthy mix: growing Expand signals strong customer retention",
    )
    layout.append(
        {
            "name": f"{pp}_land_expand",
            "row": 13,
            "column": 0,
            "colspan": 6,
            "rowspan": 8,
        }
    )

    # -- Widget 4: Product Family Breakdown (hbar) --
    steps["p1_4_product"] = sq(
        L_FRM
        + 'q = filter q by IsWon == "true";\n'
        + UGF
        + "q = group q by 'ProductFamily';\n"
        + "q = foreach q generate 'ProductFamily', sum('ARR') as sum_arr, count() as cnt;\n"
        + "q = order q by sum_arr desc;\n"
        + "q = limit q 15;"
    )
    widgets[f"{pp}_product"] = rich_chart(
        "p1_4_product",
        "hbar",
        "Won ARR by Product Family",
        ["ProductFamily"],
        ["sum_arr"],
        axis_title="ARR",
        subtitle="Revenue concentration across product lines",
        show_values=True,
    )
    layout.append(
        {"name": f"{pp}_product", "row": 13, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 5: Source Effectiveness (hbar — win rate by LeadSource) --
    steps["p1_4_source"] = sq(
        L_PA
        + PA_CLOSED
        + "q = filter q by 'LeadSource' != \"\";\n"
        + "q = foreach q generate 'LeadSource', "
        + '(case when IsWon == "true" then 1 else 0 end) as is_won;\n'
        + "q = group q by 'LeadSource';\n"
        + "q = foreach q generate 'LeadSource', "
        + "(sum(is_won) / count()) * 100 as win_rate, count() as total;\n"
        + "q = order q by win_rate desc;"
    )
    widgets[f"{pp}_source"] = rich_chart(
        "p1_4_source",
        "hbar",
        "Win Rate by Lead Source",
        ["LeadSource"],
        ["win_rate"],
        axis_title="Win Rate %",
        subtitle="Double down on highest-converting channels",
        show_values=True,
    )
    layout.append(
        {"name": f"{pp}_source", "row": 21, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 6: Opps Created by Region & Quarter (stackvbar) --
    # All opps (including closed) — counts by creation period, not by close date
    steps["p1_4_new_opps"] = sq(
        L_ERF
        + UGF
        + "q = group q by ('CloseQuarter', 'UnitGroup');\n"
        + "q = foreach q generate 'CloseQuarter', 'UnitGroup', count() as cnt;\n"
        + "q = order q by 'CloseQuarter' asc;"
    )
    widgets[f"{pp}_new_opps"] = rich_chart(
        "p1_4_new_opps",
        "stackvbar",
        "Deal Count by Region & Quarter",
        ["CloseQuarter"],
        ["cnt"],
        split=["UnitGroup"],
        show_legend=True,
        axis_title="Count",
        subtitle="Are all regions contributing to pipeline?",
    )
    layout.append(
        {"name": f"{pp}_new_opps", "row": 21, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 7: Deal Efficiency Scatter (ARR vs Sales Cycle, colored by Type) --
    # Scatter reveals outliers: high-ARR fast deals = ideal; low-ARR slow deals = inefficient
    steps["p1_4_scatter"] = sq(
        L_PA
        + PA_WON
        + "q = filter q by 'AgeInDays' > 0;\n"
        + "q = group q by ('Type', 'Name');\n"
        + "q = foreach q generate 'Type', 'Name', "
        + "sum('ARR') as deal_arr, avg('AgeInDays') as cycle_days;\n"
        + "q = order q by deal_arr desc;\n"
        + "q = limit q 100;"
    )
    widgets[f"{pp}_scatter"] = scatter_chart(
        "p1_4_scatter",
        "Deal Efficiency: ARR vs Sales Cycle",
        x_title="Sales Cycle (Days)",
        y_title="Deal ARR",
        show_legend=True,
    )
    layout.append(
        {"name": f"{pp}_scatter", "row": 29, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 8: Smallest Won Deals (compare table) --
    steps["p1_4_below_avg"] = sq(
        L_PA
        + PA_WON
        + "q = foreach q generate 'Id', 'Name', 'OwnerName', 'ARR', "
        + "'Type', 'AccountName', 'LeadSource';\n"
        + "q = order q by 'ARR' asc;\n"
        + "q = limit q 25;"
    )
    widgets[f"{pp}_below_avg"] = rich_chart(
        "p1_4_below_avg",
        "comparisontable",
        "Smallest Won Deals",
        ["Name", "OwnerName", "Type", "AccountName"],
        ["ARR"],
        subtitle="Deals below portfolio average — are these worth pursuing?",
        format_rules=[
            {
                "type": "threshold",
                "field": "ARR",
                "rules": [
                    {"value": 10000, "color": "#D4504C", "operator": "lte"},
                    {"value": 25000, "color": "#FFB75D", "operator": "lte"},
                ],
            },
        ],
    )
    layout.append(
        {"name": f"{pp}_below_avg", "row": 29, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 9: Pareto Analysis — Account Concentration (80/20) --
    # Cumulative % of won ARR by account, ranked desc — reveals deal concentration
    steps["p1_4_pareto"] = sq(
        L_PA
        + PA_WON
        + "q = group q by 'AccountName';\n"
        + "q = foreach q generate 'AccountName', sum('ARR') as acct_arr;\n"
        + "q = order q by acct_arr desc;\n"
        + "q = foreach q generate 'AccountName', acct_arr, "
        + "sum(acct_arr) over ([..0] partition by all order by acct_arr desc) as cumulative_arr;\n"
        + "q = foreach q generate 'AccountName', acct_arr, cumulative_arr, "
        + "(cumulative_arr / sum(acct_arr) over ([..] partition by all order by acct_arr desc)) * 100 as cumulative_pct;\n"
        + "q = limit q 30;"
    )
    widgets[f"{pp}_pareto"] = combo_chart(
        "p1_4_pareto",
        "Account Concentration (Pareto)",
        ["AccountName"],
        ["acct_arr"],
        ["cumulative_pct"],
        show_legend=True,
        axis_title="Won ARR",
        axis2_title="Cumulative %",
        subtitle="Top accounts drive disproportionate revenue — 80/20 rule",
        reference_lines=[
            {"value": 80, "label": "80% Concentration Line", "color": "#D4504C"}
        ],
        axis1_format="$#,##0",
        axis2_format="0%",
    )
    layout.append(
        {"name": f"{pp}_pareto", "row": 37, "column": 0, "colspan": 12, "rowspan": 8}
    )

    # -- Widget 10: Product × Region Heatmap --
    # Cross-tab heatmap: which products sell best in which regions
    steps["p1_4_prod_region"] = sq(
        L_FRM
        + 'q = filter q by IsWon == "true";\n'
        + UGF
        + "q = group q by ('ProductFamily', 'UnitGroup');\n"
        + "q = foreach q generate 'ProductFamily', 'UnitGroup', sum('ARR') as sum_arr;\n"
        + "q = order q by sum_arr desc;"
    )
    widgets[f"{pp}_prod_region"] = heatmap_chart(
        "p1_4_prod_region",
        "Product × Region Revenue Heatmap",
    )
    layout.append(
        {
            "name": f"{pp}_prod_region",
            "row": 45,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        }
    )

    page = pg("p1_4", "Deal Composition & Sizing", layout)
    return steps, widgets, page


# ═══════════════════════════════════════════════════════════════════════════
#  Page 1.5 — Performance Analysis (Dynamic Metric/Dimension)
# ═══════════════════════════════════════════════════════════════════════════


def page_1_5():
    """Performance Analysis — dynamic metric x dimension breakdown.

    Story: Compare any metric across any dimension. Who are top performers?

    Architecture: Uses staticflex steps for metric and dimension selectors,
    with SAQL bindings to drive the main chart, trend, and table.
    """
    steps = {}
    widgets = {}
    pp = "p1_5"

    w, layout = _standard_header(
        pp,
        4,
        "Performance Analysis",
        "Compare any metric across any dimension | Select metric & dimension below",
    )
    widgets.update(w)

    # -- F1: Metric Selector (staticflex — broadcastFacet: false) --
    steps["p1_5_metric_sel"] = {
        "type": "staticflex",
        "broadcastFacet": False,
        "selectMode": "singlerequired",
        "values": [
            {"display": "Open Pipeline ARR", "value": "sum('WeightedOpenARR')"},
            {
                "display": "Closed Won ARR",
                "value": "sum(case when IsWon == \"true\" then 'ARR' else 0 end)",
            },
            {
                "display": "Win Rate %",
                "value": '(sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100',
            },
            {"display": "Deal Count", "value": "count()"},
            {"display": "Avg Deal Size", "value": "avg('ARR')"},
            {"display": "Commit ARR", "value": "sum('CommitCallARR')"},
            {"display": "Best Case ARR", "value": "sum('BestCaseCallARR')"},
            {
                "display": "Confidence Weighted ARR",
                "value": "sum('ConfidenceWeightedARR')",
            },
            {"display": "Quota Amount", "value": "max('QuotaAmount')"},
            {"display": "Risk Weighted ARR", "value": "sum('RiskWeightedARR')"},
            # RI Team Performance metrics (5 additional)
            {
                "display": "Gap to Quota",
                "value": "(max('QuotaAmount') - sum(case when IsWon == \"true\" then 'ARR' else 0 end))",
            },
            {
                "display": "Pipeline Coverage",
                "value": "(case when (max('QuotaAmount') - sum(case when IsWon == \"true\" then 'ARR' else 0 end)) > 0 then sum(case when IsClosed == \"false\" then 'WeightedOpenARR' else 0 end) / (max('QuotaAmount') - sum(case when IsWon == \"true\" then 'ARR' else 0 end)) else 0 end)",
            },
            {
                "display": "Quota Attainment %",
                "value": "(case when max('QuotaAmount') > 0 then (sum(case when IsWon == \"true\" then 'ARR' else 0 end) / max('QuotaAmount')) * 100 else 0 end)",
            },
            {
                "display": "Closed Lost ARR",
                "value": 'sum(case when IsClosed == "true" and IsWon == "false" then \'ARR\' else 0 end)',
            },
            {
                "display": "New Pipeline Created",
                "value": "sum('ARR')",
            },
        ],
        "start": '[{"display":"Open Pipeline ARR","value":"sum(\'WeightedOpenARR\')"}]',
        "useGlobal": False,
    }
    widgets[f"{pp}_metric_sel"] = listselector("p1_5_metric_sel", "Select Metric")
    layout.append(
        {"name": f"{pp}_metric_sel", "row": 5, "column": 0, "colspan": 6, "rowspan": 2}
    )

    # -- F2: Dimension Selector (staticflex — broadcastFacet: false) --
    steps["p1_5_dim_sel"] = {
        "type": "staticflex",
        "broadcastFacet": False,
        "selectMode": "singlerequired",
        "values": [
            {"display": "Owner", "value": "OwnerName"},
            {"display": "Region", "value": "UnitGroup"},
            {"display": "Sales Region", "value": "SalesRegion"},
            {"display": "Quarter", "value": "CloseQuarter"},
            {"display": "Forecast Category", "value": "ForecastCategory"},
            {"display": "Fiscal Year", "value": "FYLabel"},
            {"display": "Stage", "value": "StageName"},
            {"display": "Lead Source", "value": "LeadSource"},
            {"display": "Type (Land/Expand/Renewal)", "value": "Type"},
        ],
        "start": '[{"display":"Owner","value":"OwnerName"}]',
        "useGlobal": False,
    }
    widgets[f"{pp}_dim_sel"] = listselector("p1_5_dim_sel", "Select Dimension")
    layout.append(
        {"name": f"{pp}_dim_sel", "row": 5, "column": 6, "colspan": 6, "rowspan": 2}
    )

    # -- Widget 1: Primary Breakdown (hbar) --
    # Uses SAQL bindings from selectors for dynamic grouping and metric
    steps["p1_5_breakdown"] = sq(
        L_ERF
        + UGF
        + "q = group q by "
        + '{{cell(p1_5_dim_sel.selection, [0], "value").asGrouping()}};\n'
        + "q = foreach q generate "
        + '{{cell(p1_5_dim_sel.selection, [0], "value").asProjection()}} as dim_value, '
        + '{{cell(p1_5_metric_sel.selection, [0], "value")}} as metric_value;\n'
        + "q = order q by metric_value desc;\n"
        + "q = limit q 20;"
    )
    widgets[f"{pp}_breakdown"] = rich_chart(
        "p1_5_breakdown",
        "hbar",
        "Primary Breakdown",
        ["dim_value"],
        ["metric_value"],
        axis_title="Metric Value",
        show_values=True,
        subtitle="Use dimension and metric selectors to pivot this view",
    )
    layout.append(
        {"name": f"{pp}_breakdown", "row": 7, "column": 0, "colspan": 12, "rowspan": 10}
    )

    # -- Widget 2: Trend View (stackvbar — top 10 by quarter) --
    steps["p1_5_trend"] = sq(
        L_ERF
        + UGF
        + "q = group q by ('CloseQuarter', "
        + '{{cell(p1_5_dim_sel.selection, [0], "value").asGrouping()}});\n'
        + "q = foreach q generate 'CloseQuarter', "
        + '{{cell(p1_5_dim_sel.selection, [0], "value").asProjection()}} as dim_value, '
        + '{{cell(p1_5_metric_sel.selection, [0], "value")}} as metric_value;\n'
        + "q = order q by metric_value desc;\n"
        + "q = limit q 40;"
    )
    widgets[f"{pp}_trend"] = rich_chart(
        "p1_5_trend",
        "stackvbar",
        "Trend by Quarter (Top Values)",
        ["CloseQuarter"],
        ["metric_value"],
        split=["dim_value"],
        show_legend=True,
        axis_title="Metric Value",
        subtitle="Quarterly composition — identify shifting mix over time",
    )
    layout.append(
        {"name": f"{pp}_trend", "row": 17, "column": 0, "colspan": 12, "rowspan": 8}
    )

    # -- Widget 3: Cross-Tab Detail (compare table) --
    steps["p1_5_crosstab"] = sq(
        L_ERF
        + UGF
        + "q = group q by "
        + '{{cell(p1_5_dim_sel.selection, [0], "value").asGrouping()}};\n'
        + "q = foreach q generate "
        + '{{cell(p1_5_dim_sel.selection, [0], "value").asProjection()}} as dim_value, '
        + "sum('ARR') as total_arr, "
        + "sum('WeightedOpenARR') as open_pipe, "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end) as won_arr, "
        + "count() as deal_count;\n"
        + "q = order q by total_arr desc;"
    )
    widgets[f"{pp}_crosstab"] = rich_chart(
        "p1_5_crosstab",
        "comparisontable",
        "Cross-Tab Detail",
        ["dim_value"],
        ["total_arr", "open_pipe", "won_arr", "deal_count"],
    )
    layout.append(
        {"name": f"{pp}_crosstab", "row": 25, "column": 0, "colspan": 12, "rowspan": 10}
    )

    # -- Widget 4: Rep Scorecard (Bloomberg-style dense management cockpit) --
    # All key metrics for each rep in one table — the "management cockpit" view
    steps["p1_5_scorecard"] = sq(
        L_ERF
        + UGF
        + QF
        + "q = group q by 'OwnerName';\n"
        + "q = foreach q generate 'OwnerName', "
        + "sum(case when IsClosed == \"false\" then 'WeightedOpenARR' else 0 end) as Open_Pipeline, "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end) as Closed_Won, "
        + "max('QuotaAmount') as Quota, "
        + "(case when max('QuotaAmount') > 0 "
        + "then (sum(case when IsWon == \"true\" then 'ARR' else 0 end) / max('QuotaAmount')) * 100 "
        + "else 0 end) as Attainment_Pct, "
        + "(max('QuotaAmount') - sum(case when IsWon == \"true\" then 'ARR' else 0 end)) as Gap, "
        + "(case when (max('QuotaAmount') - sum(case when IsWon == \"true\" then 'ARR' else 0 end)) > 0 "
        + "then sum(case when IsClosed == \"false\" then 'WeightedOpenARR' else 0 end) "
        + "/ (max('QuotaAmount') - sum(case when IsWon == \"true\" then 'ARR' else 0 end)) "
        + "else 0 end) as Coverage, "
        + "count() as Total_Deals, "
        + "avg('ARR') as Avg_Deal;\n"
        + "q = order q by Closed_Won desc;"
    )
    widgets[f"{pp}_scorecard"] = rich_chart(
        "p1_5_scorecard",
        "comparisontable",
        "Rep Scorecard (All Key Metrics)",
        ["OwnerName"],
        [
            "Open_Pipeline",
            "Closed_Won",
            "Quota",
            "Attainment_Pct",
            "Gap",
            "Coverage",
            "Total_Deals",
            "Avg_Deal",
        ],
        subtitle="Dense management view — all KPIs for every rep at a glance",
        format_rules=[
            {
                "type": "threshold",
                "field": "Attainment_Pct",
                "rules": [
                    {"value": 80, "color": "#04844B", "operator": "gte"},
                    {"value": 50, "color": "#FFB75D", "operator": "gte"},
                ],
            },
            {
                "type": "threshold",
                "field": "Coverage",
                "rules": [
                    {"value": 3, "color": "#04844B", "operator": "gte"},
                    {"value": 2, "color": "#FFB75D", "operator": "gte"},
                ],
            },
        ],
    )
    layout.append(
        {
            "name": f"{pp}_scorecard",
            "row": 35,
            "column": 0,
            "colspan": 12,
            "rowspan": 12,
        }
    )

    page = pg("p1_5", "Performance Analysis", layout)
    return steps, widgets, page


# ═══════════════════════════════════════════════════════════════════════════
#  Page 1.6 — Commit Calculator
# ═══════════════════════════════════════════════════════════════════════════


def page_1_6():
    """Commit Calculator — what-if scenario modeling.

    Story: What is my number if I include/exclude specific deals?
    """
    steps = {}
    widgets = {}
    pp = "p1_6"

    w, layout = _standard_header(
        pp,
        5,
        "Commit Calculator",
        "What-if scenario: select deals to model your commit number",
    )
    widgets.update(w)

    # Warning text about filter resets
    widgets[f"{pp}_warning"] = {
        "type": "text",
        "parameters": {
            "content": {
                "richTextContent": [
                    {
                        "attributes": {
                            "size": "12px",
                            "color": "#D4504C",
                            "italic": True,
                        },
                        "insert": "Note: Changing filters will reset your scenario selections.",
                    },
                    {"attributes": {"align": "left"}, "insert": "\n"},
                ]
            },
            "interactions": [],
        },
    }
    layout.append(
        {"name": f"{pp}_warning", "row": 5, "column": 0, "colspan": 12, "rowspan": 1}
    )

    # -- KPI 1: Current Commit (GLOBAL BENCHMARK — immune to selection) --
    steps["p1_6_current_commit"] = {
        **sq(
            L_ERF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by 'ForecastCategory' == \"Commit\";\n"
            + "q = group q by all;\n"
            + "q = foreach q generate sum('ARR') as commit_arr;",
            broadcast=False,
        ),
        **ISOLATION_QUAD,
    }
    widgets[f"{pp}_commit"] = num(
        "p1_6_current_commit",
        "commit_arr",
        "Current Commit (Locked)",
        "#091A3E",
        compact=True,
        tier="primary",
        prefix="$",
        widget_style=kpi_style("accent"),
    )
    layout.append(
        {"name": f"{pp}_commit", "row": 6, "column": 0, "colspan": 3, "rowspan": 4}
    )

    # -- KPI 2: Scenario Total (bound to selection) --
    steps["p1_6_scenario"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + UGF
        + "q = foreach q generate 'Id', 'OpportunityName', 'OwnerName', "
        + "'ARR', 'ForecastCategory', 'CloseQuarter', 'StageName', 'AccountName';\n"
        + "q = order q by 'ARR' desc;",
        broadcast=True,
    )
    # Override to multi-select mode for scenario modeling
    steps["p1_6_scenario"]["selectMode"] = "multi"

    # Use coalesce_filter pattern: when no selection, filter is passthrough (all records)
    steps["p1_6_scenario_sum"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + "q = filter q by 'Id' in "
        + '{{coalesce(column(p1_6_scenario.selection, ["Id"]), column(p1_6_scenario.result, ["Id"])).asEquality()}};\n'
        + "q = group q by all;\n"
        + "q = foreach q generate sum('ARR') as scenario_total;"
    )
    steps["p1_6_scenario_sum"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_scenario"] = num(
        "p1_6_scenario_sum",
        "scenario_total",
        "Scenario Total",
        "#0070D2",
        compact=True,
        tier="primary",
        prefix="$",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_scenario", "row": 6, "column": 3, "colspan": 3, "rowspan": 4}
    )

    # -- KPI 3: Gap to Quota --
    steps["p1_6_quota"] = sq(
        L_ERF
        + "q = group q by 'OwnerName';\n"
        + "q = foreach q generate max('QuotaAmount') as rep_quota;\n"
        + "q = group q by all;\n"
        + "q = foreach q generate sum(rep_quota) as total_quota;"
    )
    steps["p1_6_gap"] = sq(
        L_ERF
        + "q = group q by 'OwnerName';\n"
        + "q = foreach q generate max('QuotaAmount') as rep_quota, "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end) as won;\n"
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "sum(rep_quota) as total_quota, "
        + "sum(won) as total_won, "
        + "(sum(rep_quota) - sum(won)) as gap_to_quota;"
    )
    steps["p1_6_gap"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_gap"] = num(
        "p1_6_gap",
        "gap_to_quota",
        "Gap to Quota",
        "#D4504C",
        compact=True,
        tier="primary",
        prefix="$",
        sentiment_color=True,
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_gap", "row": 6, "column": 6, "colspan": 3, "rowspan": 4}
    )

    # -- KPI 4: Pipeline Coverage (remaining) --
    steps["p1_6_pipe_coverage"] = sq(
        L_ERF
        + UGF
        + "q = group q by 'OwnerName';\n"
        + "q = foreach q generate "
        + "sum(case when IsClosed == \"false\" then 'ARR' else 0 end) as open_pipe, "
        + "max('QuotaAmount') as rep_quota, "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end) as won;\n"
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "sum(open_pipe) as total_pipe, "
        + "(case when (sum(rep_quota) - sum(won)) > 0 "
        + "then sum(open_pipe) / (sum(rep_quota) - sum(won)) "
        + "else 0 end) as coverage;"
    )
    steps["p1_6_pipe_coverage"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_pipe_cov"] = num(
        "p1_6_pipe_coverage",
        "coverage",
        "Coverage of Gap",
        "#0070D2",
        tier="secondary",
        suffix="x",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_pipe_cov", "row": 6, "column": 9, "colspan": 3, "rowspan": 4}
    )

    # -- Widget 5: Opportunity Selector (compare table with multi-select) --
    widgets[f"{pp}_opp_table"] = rich_chart(
        "p1_6_scenario",
        "comparisontable",
        "Select Deals for Scenario",
        [
            "OpportunityName",
            "OwnerName",
            "ForecastCategory",
            "StageName",
            "CloseQuarter",
            "AccountName",
        ],
        ["ARR"],
        subtitle="Multi-select deals to model scenario outcomes",
        format_rules=[
            {
                "type": "threshold",
                "field": "ARR",
                "rules": [
                    {"value": 500000, "color": "#04844B", "operator": "gte"},
                    {"value": 100000, "color": "#0070D2", "operator": "gte"},
                ],
            },
        ],
    )
    layout.append(
        {
            "name": f"{pp}_opp_table",
            "row": 10,
            "column": 0,
            "colspan": 12,
            "rowspan": 15,
        }
    )

    page = pg("p1_6", "Commit Calculator", layout)
    return steps, widgets, page


# ═══════════════════════════════════════════════════════════════════════════
#  Page 1.7 — Sales Rep Command Center
# ═══════════════════════════════════════════════════════════════════════════


def page_1_7():
    """Sales Rep Command Center — personal dashboard.

    Story: My personal dashboard. What should I work on today?

    All steps filtered by Owner.Id = {{App.User.Id}} for logged-in user.
    """
    steps = {}
    widgets = {}
    pp = "p1_7"

    w, layout = _standard_header(
        pp,
        6,
        "My Sales Dashboard",
        "Your personal pipeline view | Scoped to your opportunities",
    )
    widgets.update(w)

    # User-scoping SAQL fragment (logged-in user only)
    MY = "q = filter q by 'OwnerName' == \"{{App.User.Name}}\";\n"

    # -- Widget 1: My Quota Attainment (bullet chart) --
    steps["p1_7_attain"] = sq(
        L_ERF
        + MY
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end) as closed_won, "
        + "max('QuotaAmount') as quota;"
    )
    widgets[f"{pp}_attain"] = bullet_chart(
        "p1_7_attain",
        "My Quota Attainment",
        axis_title="ARR",
    )
    layout.append(
        {"name": f"{pp}_attain", "row": 5, "column": 0, "colspan": 6, "rowspan": 5}
    )

    # -- Widget 2: My Pipeline Coverage (KPI) --
    steps["p1_7_coverage"] = sq(
        L_ERF
        + MY
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "sum(case when IsClosed == \"false\" then 'ARR' else 0 end) as open_pipe, "
        + "max('QuotaAmount') as quota, "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end) as won, "
        + "(case when (max('QuotaAmount') - "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end)) > 0 "
        + "then sum(case when IsClosed == \"false\" then 'ARR' else 0 end) / "
        + "(max('QuotaAmount') - "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end)) "
        + "else 0 end) as my_coverage;"
    )
    steps["p1_7_coverage"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_coverage"] = num(
        "p1_7_coverage",
        "my_coverage",
        "My Pipeline Coverage",
        "#0070D2",
        tier="primary",
        suffix="x",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_coverage", "row": 5, "column": 6, "colspan": 6, "rowspan": 5}
    )

    # -- Widget 3: My Pipeline by Stage (funnel) --
    steps["p1_7_my_funnel"] = sq(
        L_PA
        + PA_OPEN
        + MY
        + "q = group q by ('StageName', 'StageOrder');\n"
        + "q = foreach q generate 'StageName', 'StageOrder', sum('ARR') as sum_arr;\n"
        + "q = order q by 'StageOrder' asc;"
    )
    widgets[f"{pp}_my_funnel"] = funnel_chart(
        "p1_7_my_funnel",
        "My Pipeline by Stage",
        "StageName",
        "sum_arr",
    )
    layout.append(
        {"name": f"{pp}_my_funnel", "row": 10, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 4: My Stagnating Deals (compare table) --
    # Open opps stuck >30d or no activity >14d, sorted by priority score (ARR * days)
    steps["p1_7_stagnating"] = sq(
        L_PA
        + PA_OPEN
        + MY
        + "q = filter q by 'DaysInCurrentStage' > 30;\n"
        + "q = foreach q generate 'Id', 'Name', 'ARR', 'StageName', "
        + "'DaysInCurrentStage', 'AccountName', 'ForecastCategory', "
        + "('ARR' * 'DaysInCurrentStage') as priority_score;\n"
        + "q = order q by priority_score desc;\n"
        + "q = limit q 15;"
    )
    widgets[f"{pp}_stagnating"] = add_table_action(
        rich_chart(
            "p1_7_stagnating",
            "comparisontable",
            "My Stagnating Deals (>30d Same Stage)",
            ["Name", "StageName", "AccountName", "ForecastCategory"],
            ["ARR", "DaysInCurrentStage", "priority_score"],
            subtitle="Ranked by priority = ARR x days stalled",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysInCurrentStage",
                    "rules": [
                        {"value": 90, "color": "#D4504C", "operator": "gte"},
                        {"value": 60, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        )
    )
    layout.append(
        {"name": f"{pp}_stagnating", "row": 10, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 5: My Upcoming Activities (SOQL step) --
    # NOTE: This uses a SOQL step type for Task/Event objects
    steps["p1_7_activities"] = {
        "type": "soql",
        "query": (
            "SELECT Id, Subject, ActivityDate, Status, WhoId, WhatId, "
            "What.Name, Priority "
            "FROM Task "
            "WHERE OwnerId = '{{App.User.Id}}' "
            "AND Status != 'Completed' "
            "AND ActivityDate >= TODAY "
            "ORDER BY ActivityDate ASC "
            "LIMIT 10"
        ),
        "broadcastFacet": False,
    }
    widgets[f"{pp}_activities"] = rich_chart(
        "p1_7_activities",
        "comparisontable",
        "My Upcoming Activities",
        ["Subject", "What.Name", "Priority", "Status"],
        ["ActivityDate"],
    )
    layout.append(
        {"name": f"{pp}_activities", "row": 18, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 6: My Deal Highlights (recent changes) --
    steps["p1_7_highlights"] = sq(
        L_PA
        + PA_OPEN
        + MY
        + "q = foreach q generate 'Id', 'Name', 'ARR', 'StageName', "
        + "'AccountName', 'ForecastCategory', 'PushCount', 'DaysInCurrentStage', "
        + "'AtRiskFlag';\n"
        + "q = order q by 'ARR' desc;\n"
        + "q = limit q 15;"
    )
    widgets[f"{pp}_highlights"] = add_table_action(
        rich_chart(
            "p1_7_highlights",
            "comparisontable",
            "My Open Deals Overview",
            ["Name", "StageName", "ForecastCategory", "AccountName", "AtRiskFlag"],
            ["ARR", "PushCount", "DaysInCurrentStage"],
            subtitle="Full view of your open pipeline — flag at-risk deals early",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "PushCount",
                    "rules": [
                        {"value": 3, "color": "#D4504C", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "DaysInCurrentStage",
                    "rules": [
                        {"value": 60, "color": "#D4504C", "operator": "gte"},
                        {"value": 30, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        )
    )
    layout.append(
        {"name": f"{pp}_highlights", "row": 18, "column": 6, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 7: My Forecast Build (combo — cumulative won + commit + quota) --
    # Personal version of the P1.3 cumulative build chart
    steps["p1_7_my_build"] = sq(
        L_ERF
        + MY
        + "q = group q by 'MonthLabel';\n"
        + "q = foreach q generate 'MonthLabel', "
        + "sum(case when IsWon == \"true\" then 'ARR' else 0 end) as won_arr, "
        + "sum('CommitCallARR') as commit_arr;\n"
        + "q = foreach q generate 'MonthLabel', "
        + "sum(won_arr) over ([..0] partition by all order by ('MonthLabel')) as cum_won, "
        + "sum(commit_arr) over ([..0] partition by all order by ('MonthLabel')) as cum_commit;"
    )
    widgets[f"{pp}_my_build"] = combo_chart(
        "p1_7_my_build",
        "My Revenue Build (Cumulative)",
        ["MonthLabel"],
        ["cum_won"],
        ["cum_commit"],
        show_legend=True,
        axis_title="Won ARR",
        axis2_title="Commit ARR",
        subtitle="Your personal trajectory: closed won vs commit forecast",
        axis1_format="$#,##0",
        axis2_format="$#,##0",
    )
    layout.append(
        {"name": f"{pp}_my_build", "row": 26, "column": 0, "colspan": 6, "rowspan": 8}
    )

    # -- Widget 8: My Win Rate + Deal Count --
    steps["p1_7_my_winrate"] = sq(
        L_PA
        + PA_CLOSED
        + MY
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + '(sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100 as my_wr, '
        + 'sum(case when IsWon == "true" then 1 else 0 end) as won, '
        + "count() as total;"
    )
    steps["p1_7_my_winrate"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_my_winrate"] = num(
        "p1_7_my_winrate",
        "my_wr",
        "My Win Rate",
        "#9050E9",
        tier="secondary",
        suffix="%",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {"name": f"{pp}_my_winrate", "row": 26, "column": 6, "colspan": 3, "rowspan": 4}
    )

    # -- Widget 9: My Avg Deal Size --
    steps["p1_7_my_avg_deal"] = sq(
        L_PA
        + PA_WON
        + MY
        + "q = group q by all;\n"
        + "q = foreach q generate avg('ARR') as avg_deal, count() as won_count;"
    )
    steps["p1_7_my_avg_deal"].update(KPI_FACET_SCOPE)
    widgets[f"{pp}_my_avg_deal"] = num(
        "p1_7_my_avg_deal",
        "avg_deal",
        "My Avg Deal Size",
        "#04844B",
        compact=True,
        tier="secondary",
        prefix="$",
        widget_style=kpi_style("card"),
    )
    layout.append(
        {
            "name": f"{pp}_my_avg_deal",
            "row": 26,
            "column": 9,
            "colspan": 3,
            "rowspan": 4,
        }
    )

    # -- Widget 10: My Deals by Forecast Category (donut) --
    steps["p1_7_my_fcat"] = sq(
        L_ERF
        + 'q = filter q by IsClosed == "false";\n'
        + MY
        + "q = group q by 'ForecastCategory';\n"
        + "q = foreach q generate 'ForecastCategory', sum('ARR') as sum_arr;\n"
        + "q = order q by sum_arr desc;"
    )
    widgets[f"{pp}_my_fcat"] = rich_chart(
        "p1_7_my_fcat",
        "donut",
        "My Open Pipeline by Forecast Category",
        ["ForecastCategory"],
        ["sum_arr"],
        show_legend=True,
        show_pct=True,
        subtitle="Your pipeline mix — target >50% Commit for predictable quarter",
    )
    layout.append(
        {"name": f"{pp}_my_fcat", "row": 30, "column": 6, "colspan": 6, "rowspan": 8}
    )

    page = pg("p1_7", "Sales Rep Command Center", layout)
    return steps, widgets, page


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main():
    """Build and deploy Dashboard 1: Sales Pipeline & Forecast."""
    print("=" * 70)
    print("  Dashboard 1: Sales Pipeline & Forecast")
    print("=" * 70)

    inst, tok = get_auth()
    dashboard_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)

    steps, widgets = {}, {}

    # Add shared filter steps
    s = shared_steps()
    steps.update(s)

    # Build each page
    pages = []
    page_builders = [
        page_1_1,
        page_1_2,
        page_1_3,
        page_1_4,
        page_1_5,
        page_1_6,
        page_1_7,
    ]

    for page_fn in page_builders:
        s, w, p = page_fn()
        steps.update(s)
        widgets.update(w)
        pages.append(p)

    # Polish SAQL steps (add groups/numbers arrays)
    _polish_saql_steps(steps)

    # Summary
    print(f"\nBuilt {len(steps)} steps, {len(widgets)} widgets, {len(pages)} pages")

    # Build layout with consulting-grade theming
    layout = {
        "name": "Default",
        "numColumns": 12,
        "pages": pages,
    }

    state = build_dashboard_state(
        steps,
        widgets,
        layout,
        bg_color="#F4F6F9",  # Light gray background — white cards pop
        cell_spacing=8,  # Consulting standard whitespace
        row_height="normal",
        widget_style=KPI_CARD_STYLE,  # Default card style for all widgets
    )
    deploy_dashboard(inst, tok, dashboard_id, state)


if __name__ == "__main__":
    main()
