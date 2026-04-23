#!/usr/bin/env python3
"""Upgrade the live Executive Revenue & Forecast dashboard in place.

This script uses the live dashboard state as the baseline, then applies a
targeted upgrade:
1. Repair stale internal page links.
2. Convert the page-2 "action queue" hbars into compare tables.
3. Add explicit NextBestAction logic to the queue SAQL.

It intentionally avoids regenerating the full dashboard from local builders.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from crm_analytics_helpers import (  # noqa: E402
    combo_chart,
    compare_table,
    get_auth,
    hdr,
    normalize_dashboard_state_for_patch,
    rich_chart,
)


DASHBOARD_ID = "0FKTb0000000HqTOAU"
TODAY = date.today()
FORECAST_START_KEY = f"{TODAY.year:04d}-{TODAY.month:02d}"


def shift_month_key(month_key: str, offset: int) -> str:
    year, month = map(int, month_key.split("-"))
    month = month + offset
    while month <= 0:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    return f"{year:04d}-{month:02d}"


LAST_COMPLETE_KEY = shift_month_key(FORECAST_START_KEY, -1)
LAST_COMPLETE_LABEL = date.fromisoformat(f"{LAST_COMPLETE_KEY}-01").strftime("%b %Y")
FORECAST_START_LABEL = date.fromisoformat(f"{FORECAST_START_KEY}-01").strftime("%b %Y")
CURRENT_FY_LABEL = f"FY{TODAY.year}"
PRIOR_FY_LABEL = f"FY{TODAY.year - 1}"

def get_dashboard(instance_url, token, dashboard_id):
    req = urllib.request.Request(
        f"{instance_url}/services/data/v66.0/wave/dashboards/{dashboard_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

def patch_dashboard(instance_url, token, dashboard_id, state):
    body = json.dumps({"state": state}).encode("utf-8")
    req = urllib.request.Request(
        f"{instance_url}/services/data/v66.0/wave/dashboards/{dashboard_id}",
        data=body,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return resp.status


def ref(name, row, column, colspan, rowspan):
    return {
        "name": name,
        "row": row,
        "column": column,
        "colspan": colspan,
        "rowspan": rowspan,
        "widgetStyle": {"borderEdges": []},
    }


def fix_internal_nav(widgets):
    link_specs = {
        "p1_nav1": ("revenue", "Revenue & Pacing", "#091A3E"),
        "p1_nav2": ("pipeline", "Pipeline Coverage", "#0070D2"),
        "p2_nav1": ("revenue", "Revenue & Pacing", "#0070D2"),
        "p2_nav2": ("pipeline", "Pipeline Coverage", "#091A3E"),
    }
    for widget_name, (page_name, label, color) in link_specs.items():
        widget = widgets[widget_name]
        params = widget["parameters"]
        params["destinationType"] = "page"
        params["destinationLink"] = {"name": page_name}
        params["includeState"] = False
        params["text"] = label
        params["textColor"] = color


def monthly_forecast_saql():
    return f"""
q = load "Executive_Revenue_Forecast";
q = filter q by RecordType == "forecast_chart";
q = filter q by {{{{coalesce(column(f_unit_group.selection, ["UnitGroup"]), column(f_unit_group.result, ["UnitGroup"])).asEquality('UnitGroup')}}}};
q = filter q by {{{{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}}}};
q = filter q by {{{{coalesce(column(f_fy.selection, ["FYLabel"]), column(f_fy.result, ["FYLabel"])).asEquality('FYLabel')}}}};
q = group q by (MonthDate, MonthLabel);
q = foreach q generate
    MonthDate,
    MonthLabel,
    (case when MonthLabel <= "{LAST_COMPLETE_KEY}" then sum(RevenueForecastARR) else null end) as ActualARR,
    (case when MonthLabel >= "{FORECAST_START_KEY}" then sum(RevenueForecastARR) else null end) as ForecastARR,
    (case when MonthLabel >= "{FORECAST_START_KEY}" then sum(RevenueForecastARR_high_95) else null end) as ForecastARR_high_95,
    (case when MonthLabel >= "{FORECAST_START_KEY}" then sum(RevenueForecastARR_low_95) else null end) as ForecastARR_low_95,
    sum(sum(RevenueForecastARR)) over ([..0] partition by all order by MonthDate asc) as CumulativeRevenue,
    avg(sum(RevenueForecastARR)) over ([-2..0] partition by all order by MonthDate asc) as MA3_Revenue;
q = order q by MonthDate asc;
""".strip()


def executive_story_saql():
    return """
q = load "Executive_Revenue_Forecast";
q = filter q by {{coalesce(column(f_unit_group.selection, ["UnitGroup"]), column(f_unit_group.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q = filter q by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q = filter q by {{coalesce(column(f_fy.selection, ["FYLabel"]), column(f_fy.result, ["FYLabel"])).asEquality('FYLabel')}};
q = filter q by RecordType in ["detail", "forecast_chart"];
q = foreach q generate
    MonthDate,
    MonthLabel,
    (case when RecordType == "detail" and IsWon == "true" then ARR else 0 end) as ClosedWonARR,
    (case when RecordType == "forecast_chart" then RevenueForecastARR else 0 end) as ModelForecastARR,
    (case when RecordType == "forecast_chart" then CommitCallARR else 0 end) as CommitCallARR,
    (case when RecordType == "forecast_chart" then BestCaseCallARR else 0 end) as BestCaseCallARR;
q = group q by (MonthDate, MonthLabel);
q = foreach q generate
    MonthDate,
    MonthLabel,
    sum(ClosedWonARR) as ClosedWonARR,
    sum(ModelForecastARR) as ModelForecastARR,
    sum(CommitCallARR) as CommitCallARR,
    sum(BestCaseCallARR) as BestCaseCallARR,
    sum(sum(ClosedWonARR)) over ([..0] partition by all order by MonthDate asc) as CumulativeClosedWon,
    sum(sum(ModelForecastARR)) over ([..0] partition by all order by MonthDate asc) as CumulativeModel,
    sum(sum(CommitCallARR)) over ([..0] partition by all order by MonthDate asc) as CumulativeCommitCall,
    sum(sum(BestCaseCallARR)) over ([..0] partition by all order by MonthDate asc) as CumulativeBestCaseCall;
q = order q by MonthDate asc;
""".strip()


def yoy_variance_saql():
    return f"""
q_cur = load "Executive_Revenue_Forecast";
q_cur = filter q_cur by RecordType == "detail";
q_cur = filter q_cur by {{{{coalesce(column(f_unit_group.selection, ["UnitGroup"]), column(f_unit_group.result, ["UnitGroup"])).asEquality('UnitGroup')}}}};
q_cur = filter q_cur by {{{{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}}}};
q_cur = filter q_cur by FYLabel == "{CURRENT_FY_LABEL}";
q_cur = foreach q_cur generate
    (case when IsWon == "true" then ARR else 0 end) as _actual,
    (case when IsClosed == "false" then WeightedOpenARR else 0 end) as _weighted;
q_cur = group q_cur by all;
q_cur = foreach q_cur generate
    sum(_actual) as current_arr,
    (sum(_actual) + sum(_weighted)) as projected_cur;
q_pri = load "Executive_Revenue_Forecast";
q_pri = filter q_pri by RecordType == "detail";
q_pri = filter q_pri by {{{{coalesce(column(f_unit_group.selection, ["UnitGroup"]), column(f_unit_group.result, ["UnitGroup"])).asEquality('UnitGroup')}}}};
q_pri = filter q_pri by {{{{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}}}};
q_pri = filter q_pri by FYLabel == "{PRIOR_FY_LABEL}";
q_pri = foreach q_pri generate
    (case when IsWon == "true" then ARR else 0 end) as _actual,
    (case when IsClosed == "false" then WeightedOpenARR else 0 end) as _weighted;
q_pri = group q_pri by all;
q_pri = foreach q_pri generate
    sum(_actual) as prior_arr,
    (sum(_actual) + sum(_weighted)) as projected_pri;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate
    coalesce(sum(q_cur.current_arr), 0) as current_arr,
    coalesce(sum(q_pri.prior_arr), 0) as prior_arr,
    (coalesce(sum(q_cur.current_arr), 0) - coalesce(sum(q_pri.prior_arr), 0)) as yoy_delta,
    (case when coalesce(sum(q_pri.prior_arr), 0) > 0 then ((coalesce(sum(q_cur.current_arr), 0) - coalesce(sum(q_pri.prior_arr), 0)) / coalesce(sum(q_pri.prior_arr), 0)) * 100 else 0 end) as yoy_pct,
    coalesce(sum(q_cur.projected_cur), 0) as projected_cur,
    coalesce(sum(q_pri.projected_pri), 0) as projected_pri,
    (case when coalesce(sum(q_pri.projected_pri), 0) > 0 then ((coalesce(sum(q_cur.projected_cur), 0) - coalesce(sum(q_pri.projected_pri), 0)) / coalesce(sum(q_pri.projected_pri), 0)) * 100 else 0 end) as projected_yoy_pct;
""".strip()


def region_plan_gap_saql():
    return """
q1 = load "Executive_Revenue_Forecast";
q1 = filter q1 by RecordType == "detail";
q1 = filter q1 by {{coalesce(column(f_unit_group.selection, ["UnitGroup"]), column(f_unit_group.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q1 = filter q1 by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q1 = filter q1 by {{coalesce(column(f_fy.selection, ["FYLabel"]), column(f_fy.result, ["FYLabel"])).asEquality('FYLabel')}};
q1 = foreach q1 generate
    SalesRegion as SalesRegion,
    (case when IsWon == "true" then ARR else 0 end) as _actual,
    (case when IsClosed == "false" then WeightedOpenARR else 0 end) as _weighted,
    (case when IsClosed == "false" then AtRiskCommitARR else 0 end) as _riskcommit;
q1 = group q1 by SalesRegion;
q1 = foreach q1 generate
    SalesRegion,
    sum(_actual) as ClosedARR,
    (sum(_actual) + sum(_weighted)) as ProjectedARR,
    sum(_weighted) as OpenARR,
    sum(_riskcommit) as AtRiskCommitARR;
q2 = load "Executive_Revenue_Forecast";
q2 = filter q2 by RecordType == "quota_month";
q2 = filter q2 by {{coalesce(column(f_unit_group.selection, ["UnitGroup"]), column(f_unit_group.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q2 = filter q2 by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q2 = filter q2 by {{coalesce(column(f_fy.selection, ["FYLabel"]), column(f_fy.result, ["FYLabel"])).asEquality('FYLabel')}};
q2 = group q2 by SalesRegion;
q2 = foreach q2 generate SalesRegion, sum(PlanARR) as PlanARR;
q = cogroup q1 by SalesRegion, q2 by SalesRegion;
q = foreach q generate
    coalesce(first(q1.SalesRegion), first(q2.SalesRegion)) as SalesRegion,
    coalesce(sum(q1.ProjectedARR), 0) as ProjectedARR,
    coalesce(sum(q1.ClosedARR), 0) as ClosedARR,
    coalesce(sum(q1.OpenARR), 0) as OpenARR,
    coalesce(sum(q1.AtRiskCommitARR), 0) as AtRiskCommitARR,
    coalesce(sum(q2.PlanARR), 0) as PlanARR,
    (coalesce(sum(q1.ProjectedARR), 0) - coalesce(sum(q2.PlanARR), 0)) as GapToPlan;
q = order q by GapToPlan asc;
""".strip()


def risk_queue_saql():
    return """
q = load "Pipeline_Opportunity_Operations";
q = filter q by RecordType == "detail";
q = filter q by {{coalesce(column(f_unit_group.selection, ["UnitGroup"]), column(f_unit_group.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q = filter q by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q = filter q by {{coalesce(column(f_fy.selection, ["FYLabel"]), column(f_fy.result, ["FYLabel"])).asEquality('FYLabel')}};
q = filter q by IsClosed == "false";
q = filter q by AtRiskARR > 0;
q = group q by (OpportunityName, AccountName, OwnerName, ForecastCategory, StageName, ExceptionType, Id);
q = foreach q generate
    OpportunityName,
    AccountName,
    ForecastCategory,
    ExceptionType,
    (case
        when max(TotalRiskScore) >= 80 then "Executive review commit credibility"
        when max(PushCount) >= 3 then "Requalify close date and commit call"
        when max(SlipRiskScore) >= 60 then "Pressure-test deal milestone plan"
        else "Inspect risk driver and next milestone"
     end) as NextStep,
    max(WeightedOpenARR) as WeightedOpenARR,
    max(TotalRiskScore) as TotalRiskScore,
    max(PushCount) as PushCount,
    Id;
q = order q by TotalRiskScore desc;
q = limit q 15;
""".strip()


def process_queue_saql():
    return """
q = load "Pipeline_Opportunity_Operations";
q = filter q by RecordType == "detail";
q = filter q by {{coalesce(column(f_unit_group.selection, ["UnitGroup"]), column(f_unit_group.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q = filter q by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q = filter q by {{coalesce(column(f_fy.selection, ["FYLabel"]), column(f_fy.result, ["FYLabel"])).asEquality('FYLabel')}};
q = filter q by IsClosed == "false";
q = filter q by (PastDueCount > 0) or (MissingApprovalCount > 0) or (StaleCount > 0) or (BackwardMoveCount > 1);
q = group q by (OpportunityName, AccountName, OwnerName, StageName, ExceptionType, Id);
q = foreach q generate
    OpportunityName,
    AccountName,
    StageName,
    ExceptionType,
    (case
        when max(MissingApprovalCount) > 0 then "Clear approval blocker"
        when max(BackwardMoveCount) > 1 then "Recover stage regression"
        when max(DaysInStage) >= 90 then "Escalate stage stall with manager"
        when max(PushCount) >= 3 then "Re-cut close plan and customer steps"
        else "Inspect process exception"
     end) as NextStep,
    max(WeightedOpenARR) as WeightedOpenARR,
    max(DaysInStage) as DaysInStage,
    max(MissingApprovalCount) as MissingApprovalCount,
    Id;
q = order q by WeightedOpenARR desc;
q = limit q 15;
""".strip()


def build_queue_widgets():
    risk_widget = compare_table(
        "s_pipe_top_risk",
        "Executive Risk Queue",
        columns=[
            "OpportunityName",
            "NextStep",
            "AccountName",
            "ForecastCategory",
            "WeightedOpenARR",
            "TotalRiskScore",
            "PushCount",
        ],
        column_properties={
            "OpportunityName": {"width": 180, "alignment": "left"},
            "NextStep": {"width": 260, "alignment": "left"},
            "AccountName": {"width": 140, "alignment": "left"},
            "ForecastCategory": {"width": 90, "alignment": "left"},
            "WeightedOpenARR": {"width": 110, "alignment": "right"},
            "TotalRiskScore": {"width": 80, "alignment": "right"},
            "PushCount": {"width": 70, "alignment": "right"},
        },
        show_totals=False,
        vertical_padding=6,
        min_col_width=70,
        max_col_width=260,
        format_rules=[
            {
                "type": "threshold",
                "field": "TotalRiskScore",
                "rules": [
                    {"value": 80, "color": "#D4504C", "operator": "gte"},
                    {"value": 60, "color": "#FFB75D", "operator": "gte"},
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
        subtitle="Largest open risks with direct record links; Next Best Action states the immediate executive intervention.",
    )

    process_widget = compare_table(
        "s_pipe_top_process",
        "Executive Process Escalation Queue",
        columns=[
            "OpportunityName",
            "NextStep",
            "AccountName",
            "StageName",
            "WeightedOpenARR",
            "DaysInStage",
            "MissingApprovalCount",
        ],
        column_properties={
            "OpportunityName": {"width": 180, "alignment": "left"},
            "NextStep": {"width": 260, "alignment": "left"},
            "AccountName": {"width": 140, "alignment": "left"},
            "StageName": {"width": 110, "alignment": "left"},
            "WeightedOpenARR": {"width": 110, "alignment": "right"},
            "DaysInStage": {"width": 80, "alignment": "right"},
            "MissingApprovalCount": {"width": 90, "alignment": "right"},
        },
        show_totals=False,
        vertical_padding=6,
        min_col_width=70,
        max_col_width=260,
        format_rules=[
            {
                "type": "threshold",
                "field": "DaysInStage",
                "rules": [
                    {"value": 90, "color": "#D4504C", "operator": "gte"},
                    {"value": 60, "color": "#FFB75D", "operator": "gte"},
                ],
            },
            {
                "type": "threshold",
                "field": "MissingApprovalCount",
                "rules": [
                    {"value": 1, "color": "#D4504C", "operator": "gte"},
                ],
            },
        ],
        subtitle="Large deals with stage-aging or approval breakdowns; use the linked record and action column to clear the blocker fast.",
    )

    risk_widget["parameters"]["exploreLink"] = False
    process_widget["parameters"]["exploreLink"] = False
    risk_widget["parameters"]["numberOfLines"] = 2
    process_widget["parameters"]["numberOfLines"] = 2
    return risk_widget, process_widget


def build_story_widgets():
    timeline = rich_chart(
        "s_monthly_forecast",
        "line",
        "Cumulative Closed + Forecast Call Ladder",
        ["MonthLabel"],
        [
            "CumulativeClosedWon",
            "CumulativeModel",
            "CumulativeCommitCall",
            "CumulativeBestCaseCall",
        ],
        show_legend=True,
        axis_title="ARR (EUR)",
        subtitle=(
            f"Closed won accumulates through {LAST_COMPLETE_LABEL}; model, commit, and best-case ladders show the year-end path from the live forecast objects."
        ),
    )

    bridge = rich_chart(
        "s_plan_bridge",
        "column",
        "Year-End Build Mix and Remaining Gap",
        ["BridgeStep"],
        ["BridgeARR"],
        axis_title="ARR (EUR)",
        show_values=True,
        subtitle=(
            "Closed won, commit, best case, pipeline, and the remaining miss are laid out in the order the executive team should discuss them."
        ),
    )

    region_gap = rich_chart(
        "s_region_plan_gap",
        "hbar",
        "Projected ARR vs Plan by Region",
        ["SalesRegion"],
        ["ProjectedARR", "PlanARR"],
        show_legend=True,
        axis_title="ARR (EUR)",
        show_values=True,
        subtitle="Regions are ordered by gap to plan so the executive miss is obvious.",
    )

    call_ladder = rich_chart(
        "s_sales_call_view",
        "line",
        "Salesforce Call Ladder vs Model",
        ["MonthLabel"],
        ["ModelForecastARR", "CommitCallARR", "BestCaseCallARR", "PipelineCallARR"],
        show_legend=True,
        axis_title="ARR (EUR)",
        subtitle="Read Commit, Best Case, and Pipeline call layers against the model to judge forecast credibility.",
    )

    quarter_mix = rich_chart(
        "s_pipe_quarter_confidence",
        "stackvbar",
        "Open Pipeline Mix by Quarter",
        ["CloseQuarter"],
        ["CommitARR", "BestCaseARR", "PipelineARR"],
        show_legend=True,
        axis_title="ARR (EUR)",
        show_values=True,
        subtitle="This shows how much of each quarter sits in Commit, Best Case, and Pipeline.",
    )

    stage_velocity = combo_chart(
        "s_pipe_stage_velocity",
        "Stage Aging vs SLA",
        ["StageName"],
        ["AvgDaysInStage"],
        ["StageSlaDays"],
        show_legend=True,
        axis_title="Avg Days In Stage",
        axis2_title="SLA Days",
        subtitle="Stages above the SLA line are where forecast quality and execution are degrading.",
        axis1_format="#,##0",
        axis2_format="#,##0",
    )

    region_risk = rich_chart(
        "s_pipe_region_pressure",
        "hbar",
        "At-Risk ARR by Region",
        ["SalesRegion"],
        ["AtRiskARR"],
        show_legend=False,
        axis_title="ARR (EUR)",
        show_values=True,
        subtitle="Use this to see where executive escalation should concentrate first.",
    )

    push_combo = combo_chart(
        "s_pipe_push_trend",
        "Close Date Pushes and Avg Push Days",
        ["EventMonthDate"],
        ["PushCount"],
        ["AvgPushDays"],
        show_legend=True,
        axis_title="Push Count",
        axis2_title="Avg Push Days",
        subtitle="Bars show push volume; the line shows how severe the slippage is becoming.",
        axis1_format="#,##0",
        axis2_format="#,##0",
    )

    return {
        "p1_ch_timeline": timeline,
        "p1_ch_bridge": bridge,
        "p1_ch_unit": region_gap,
        "w_pacing_combo": call_ladder,
        "p2_ch_quarter": quarter_mix,
        "p2_ch_velocity": stage_velocity,
        "p2_ch_region": region_risk,
        "p2_ch_push": push_combo,
        "w_bullet_rev_kpis": rich_chart(
            "s_bullet_rev_kpis",
            "hbar",
            "Executive KPI Scorecard",
            ["KPI_Name"],
            ["actual", "target"],
            show_legend=True,
            axis_title="% of Target",
            show_values=True,
            subtitle="Actual versus target across attainment, coverage, and forecast support metrics.",
            number_format="#,##0.0",
        ),
    }


def rebuild_page_layouts(state):
    revenue_page = None
    pipeline_page = None
    for page in state["gridLayouts"][0]["pages"]:
        if page.get("name") == "revenue":
            revenue_page = page
        elif page.get("name") == "pipeline":
            pipeline_page = page

    if revenue_page is None or pipeline_page is None:
        raise RuntimeError("Expected revenue and pipeline pages in dashboard layout")

    revenue_page["widgets"] = [
        ref("p1_nav1", 0, 0, 6, 1),
        ref("p1_nav2", 0, 6, 6, 1),
        ref("p1_hdr", 1, 0, 12, 2),
        ref("p1_f_unit", 3, 0, 4, 2),
        ref("p1_f_region", 3, 4, 4, 2),
        ref("p1_f_fy", 3, 8, 4, 2),
        ref("p1_n_actual", 5, 0, 3, 4),
        ref("p1_n_projected", 5, 3, 3, 4),
        ref("p1_n_gap", 5, 6, 3, 4),
        ref("p1_n_confidence", 5, 9, 3, 4),
        ref("p1_ch_timeline", 9, 0, 8, 9),
        ref("p1_ch_bridge", 9, 8, 4, 9),
        ref("p1_ch_unit", 18, 0, 5, 7),
        ref("w_pacing_combo", 18, 5, 7, 7),
        ref("w_bullet_rev_kpis", 25, 0, 12, 5),
    ]

    pipeline_page["widgets"] = [
        ref("p2_nav1", 0, 0, 6, 1),
        ref("p2_nav2", 0, 6, 6, 1),
        ref("p2_hdr", 1, 0, 12, 2),
        ref("p2_f_unit", 3, 0, 4, 2),
        ref("p2_f_region", 3, 4, 4, 2),
        ref("p2_f_fy", 3, 8, 4, 2),
        ref("p2_ch_quarter", 5, 0, 6, 6),
        ref("p2_ch_velocity", 5, 6, 6, 6),
        ref("p2_ch_region", 11, 0, 5, 6),
        ref("p2_ch_push", 11, 5, 7, 6),
        ref("p2_tbl_risk", 17, 0, 12, 7),
        ref("p2_tbl_process", 24, 0, 12, 7),
    ]

    return state


def apply_upgrade(state):
    widgets = state["widgets"]
    steps = state["steps"]

    fix_internal_nav(widgets)
    if "f_fy" in steps:
        steps["f_fy"]["start"] = json.dumps([CURRENT_FY_LABEL])
    steps.pop("s_exec_cume_story", None)
    steps["s_monthly_forecast"]["query"] = executive_story_saql()
    steps["s_yoy_variance"]["query"] = yoy_variance_saql()
    steps["s_region_plan_gap"] = {"type": "saql", "query": region_plan_gap_saql()}
    steps["s_pipe_top_risk"]["query"] = risk_queue_saql()
    steps["s_pipe_top_process"]["query"] = process_queue_saql()

    widgets["p1_hdr"] = hdr(
        "Executive Revenue & Forecast",
        "Start here: projected ARR versus plan, the 10% YoY growth target, forecast credibility, and the regions driving the annual gap.",
    )
    widgets["p2_hdr"] = hdr(
        "Pipeline Coverage",
        "Use this page to direct executive escalation across quarter mix, stage aging, close-date slippage, and the exception queues.",
    )

    widgets["p1_n_actual"]["parameters"]["title"] = "Closed Won ARR"
    widgets["p1_n_projected"]["parameters"]["title"] = "Projected ARR"
    widgets["p1_n_gap"]["parameters"]["title"] = "Gap vs Plan"
    widgets["p1_n_confidence"]["parameters"]["step"] = "s_yoy_variance"
    widgets["p1_n_confidence"]["parameters"]["measureField"] = "projected_yoy_pct"
    widgets["p1_n_confidence"]["parameters"]["title"] = "Projected YoY %"

    story_widgets = build_story_widgets()
    widgets.update(story_widgets)

    risk_widget, process_widget = build_queue_widgets()
    widgets["p2_tbl_risk"] = risk_widget
    widgets["p2_tbl_process"] = process_widget
    return rebuild_page_layouts(state)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dashboard-id", default=DASHBOARD_ID)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    instance_url, token = get_auth()
    dashboard = get_dashboard(instance_url, token, args.dashboard_id)
    state = normalize_dashboard_state_for_patch(
        dashboard["state"],
        strip_page_labels=False,
    )
    upgraded = apply_upgrade(state)

    if args.dry_run:
        print(json.dumps(upgraded["gridLayouts"][0]["pages"], indent=2)[:6000])
        print("---")
        print(json.dumps(upgraded["widgets"]["p1_ch_timeline"], indent=2)[:4000])
        return

    status = patch_dashboard(instance_url, token, args.dashboard_id, upgraded)
    print(f"PATCH {status} — Executive Revenue & Forecast upgraded")


if __name__ == "__main__":
    main()
