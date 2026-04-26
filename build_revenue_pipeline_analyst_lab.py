#!/usr/bin/env python3
"""Build the Revenue/Pipeline Analyst Lab dashboard.

This analyst surface reuses the live Pipeline_Opportunity_Operations dataset so
the exploratory pages stay aligned with the operating pipeline model.
"""

from __future__ import annotations

from crm_analytics_helpers import (
    add_table_action,
    af,
    build_dashboard_state,
    coalesce_filter,
    combo_chart,
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    hdr,
    heatmap_chart,
    nav_link,
    nav_row,
    pg,
    pillbox,
    rich_chart,
    sankey_chart,
    set_record_links_xmd,
    sq,
    treemap_chart,
)

DS = "Pipeline_Opportunity_Operations"
DASHBOARD_LABEL = "Revenue/Pipeline Analyst Lab"


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build analyst dashboard steps on top of the manager pipeline dataset."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_fy = coalesce_filter("f_fy", "FYLabel")
    filter_region = coalesce_filter("f_region", "SalesRegion")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )
    trend = (
        load
        + 'q = filter q by RecordType == "trend";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )
    field_events = (
        load
        + 'q = filter q by RecordType == "field_history";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )
    stage_events = (
        load
        + 'q = filter q by RecordType == "stage_event";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )

    return {
        "f_unit": af("UnitGroup", ds_meta),
        "f_fy": af("FYLabel", ds_meta),
        "f_region": af("SalesRegion", ds_meta),
        "s_stage_forecast_heatmap": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (StageName, ForecastCategory);\n"
            + "q = foreach q generate StageName, ForecastCategory, sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by AtRiskARR desc;"
        ),
        "s_motion_region_treemap": sq(
            detail
            + "q = group q by (SalesRegion, MotionType);\n"
            + "q = foreach q generate SalesRegion, MotionType, "
            + "(sum(ActualARR) + sum(WeightedOpenARR)) as ProjectedARR;\n"
            + "q = order q by ProjectedARR desc;"
        ),
        "s_stage_exception": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (StageOrder, StageName);\n"
            + "q = foreach q generate StageOrder, StageName, "
            + "sum(AtRiskARR) as AtRiskARR, "
            + "sum(CriticalExceptionCount) as CriticalExceptions;\n"
            + "q = order q by StageOrder asc;"
        ),
        "s_owner_outliers": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(AtRiskARR) as AtRiskARR, "
            + "avg(TotalRiskScore) as TotalRiskScore, "
            + "sum(CriticalExceptionCount) as CriticalExceptions;\n"
            + "q = order q by AtRiskARR desc;\n"
            + "q = limit q 15;"
        ),
        "s_stage_flow": sq(
            stage_events
            + 'q = filter q by PrevStage != "";\n'
            + 'q = filter q by StageName != "";\n'
            + "q = group q by (PrevStage, StageName);\n"
            + "q = foreach q generate PrevStage as source, StageName as target, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        "s_scenario_timeline": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(RiskWeightedARR) as RiskWeightedARR, "
            + "sum(TargetARR) as TargetARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_push_trend": sq(
            field_events
            + 'q = filter q by EventField == "CloseDate";\n'
            + "q = group q by (EventMonthDate, EventMonth);\n"
            + "q = foreach q generate EventMonthDate, EventMonth, "
            + "sum(PushCount) as PushCount, "
            + "avg(PushDays) as AvgPushDays;\n"
            + "q = order q by EventMonthDate asc;"
        ),
        "s_stage_velocity": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by StageOrder != "00";\n'
            + "q = group q by (StageOrder, StageName);\n"
            + "q = foreach q generate StageOrder, StageName, "
            + "avg(DaysInStage) as AvgDaysInStage, "
            + "avg(StageSlaDays) as StageSlaDays, "
            + "avg(TotalRiskScore) as AvgTotalRiskScore;\n"
            + "q = order q by StageOrder asc;"
        ),
        "s_risk_band_profile": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by RiskBand;\n"
            + "q = foreach q generate RiskBand, "
            + "count() as OpportunityCount, "
            + "avg(SlipRiskScore) as AvgSlipRiskScore, "
            + "avg(TotalRiskScore) as AvgTotalRiskScore, "
            + "avg(WinScore) as AvgWinScore;\n"
            + "q = order q by AvgTotalRiskScore desc;"
        ),
        "s_stage_risk_heatmap": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (StageBand, RiskBand);\n"
            + "q = foreach q generate StageBand, RiskBand, count() as OpportunityCount;\n"
            + "q = order q by OpportunityCount desc;"
        ),
        "s_exception_profile": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by ExceptionType != "";\n'
            + "q = group q by ExceptionType;\n"
            + "q = foreach q generate ExceptionType, "
            + "count() as OpportunityCount, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "avg(TotalRiskScore) as AvgTotalRiskScore;\n"
            + "q = order q by WeightedOpenARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_outlier_deals": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by TotalRiskScore > 70 || SlipRiskScore > 65 || WinScore > 75;\n"
            + "q = foreach q generate OpportunityName, AccountName, OwnerName, StageName, RiskBand, "
            + "WeightedOpenARR, TotalRiskScore, SlipRiskScore, WinScore, Id;\n"
            + "q = order q by TotalRiskScore desc;\n"
            + "q = limit q 15;"
        ),
    }


def build_widgets() -> dict[str, dict]:
    """Build the 3-page analyst dashboard widgets."""
    widgets = {
        "p1_nav1": nav_link("exploration", "Exploration", active=True),
        "p1_nav2": nav_link("scenarios", "Cohorts & Scenarios"),
        "p1_nav3": nav_link("qa", "Model QA"),
        "p1_hdr": hdr(
            "Revenue/Pipeline Analyst Lab",
            "Exploratory surface for pipeline structure, exposure concentration, and analyst drill paths.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p1_f_region": pillbox("f_region", "Region"),
        "p1_ch_heatmap": heatmap_chart(
            "s_stage_forecast_heatmap",
            "Stage x Forecast Category Risk Heatmap",
        ),
        "p1_ch_treemap": treemap_chart(
            "s_motion_region_treemap",
            "Region / Motion Exposure Map",
            ["SalesRegion", "MotionType"],
            "ProjectedARR",
            show_legend=True,
        ),
        "p1_ch_stage": rich_chart(
            "s_stage_exception",
            "stackcolumn",
            "Stage Risk and Exception Pressure",
            ["StageName"],
            ["AtRiskARR", "CriticalExceptions"],
            show_legend=True,
            axis_title="ARR / Exceptions",
            show_values=True,
        ),
        "p1_tbl_owner": rich_chart(
            "s_owner_outliers",
            "comparisontable",
            "Owner Exposure Outliers",
            ["OwnerName"],
            ["WeightedOpenARR", "AtRiskARR", "TotalRiskScore", "CriticalExceptions"],
            show_legend=False,
        ),
        "p2_nav1": nav_link("exploration", "Exploration"),
        "p2_nav2": nav_link("scenarios", "Cohorts & Scenarios", active=True),
        "p2_nav3": nav_link("qa", "Model QA"),
        "p2_hdr": hdr(
            "Cohorts & Scenarios",
            "How stage movement, push behavior, and forecast scenarios evolve through the pipeline.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_ch_flow": sankey_chart(
            "s_stage_flow",
            "Stage Transition Flow",
        ),
        "p2_ch_scenario": combo_chart(
            "s_scenario_timeline",
            "Weighted vs Risk-Weighted Forecast vs Target",
            ["MonthDate"],
            ["WeightedOpenARR", "RiskWeightedARR"],
            ["TargetARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            axis2_title="Target ARR",
        ),
        "p2_ch_push": combo_chart(
            "s_push_trend",
            "Push Count and Average Push Days",
            ["EventMonthDate"],
            ["PushCount"],
            ["AvgPushDays"],
            show_legend=True,
            axis_title="Push Count",
            axis2_title="Average Push Days",
        ),
        "p2_ch_velocity": rich_chart(
            "s_stage_velocity",
            "hbar",
            "Stage Velocity vs SLA and Risk",
            ["StageName"],
            ["AvgDaysInStage", "StageSlaDays", "AvgTotalRiskScore"],
            show_legend=True,
            axis_title="Days / Score",
            show_values=True,
        ),
        "p3_nav1": nav_link("exploration", "Exploration"),
        "p3_nav2": nav_link("scenarios", "Cohorts & Scenarios"),
        "p3_nav3": nav_link("qa", "Model QA", active=True),
        "p3_hdr": hdr(
            "Model QA",
            "Score distributions, exception clustering, and the specific deals that do not fit the expected pattern.",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p3_f_region": pillbox("f_region", "Region"),
        "p3_ch_risk_band": rich_chart(
            "s_risk_band_profile",
            "hbar",
            "Risk Band Score Profile",
            ["RiskBand"],
            [
                "OpportunityCount",
                "AvgSlipRiskScore",
                "AvgTotalRiskScore",
                "AvgWinScore",
            ],
            show_legend=True,
            axis_title="Count / Score",
            show_values=True,
        ),
        "p3_ch_heatmap": heatmap_chart(
            "s_stage_risk_heatmap",
            "Stage Band x Risk Band Density",
        ),
        "p3_ch_exception": rich_chart(
            "s_exception_profile",
            "stackcolumn",
            "Exception Type Exposure Profile",
            ["ExceptionType"],
            ["OpportunityCount", "WeightedOpenARR", "AvgTotalRiskScore"],
            show_legend=True,
            axis_title="Count / ARR / Score",
            show_values=True,
        ),
        "p3_tbl_outliers": rich_chart(
            "s_outlier_deals",
            "comparisontable",
            "Analyst Outlier Deals",
            ["OpportunityName", "AccountName", "OwnerName", "StageName", "RiskBand"],
            ["WeightedOpenARR", "TotalRiskScore", "SlipRiskScore", "WinScore"],
            show_legend=False,
        ),
    }

    widgets["p2_ch_scenario"]["parameters"].pop("columnMap", None)
    widgets["p2_ch_push"]["parameters"].pop("columnMap", None)
    add_table_action(
        widgets["p3_tbl_outliers"], "salesforceActions", "Opportunity", "Id"
    )
    return widgets


def build_layout() -> dict:
    """Build grid layout for the 3-page analyst dashboard."""
    p1 = nav_row("p1", 3) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_ch_heatmap", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p1_ch_treemap", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p1_ch_stage", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p1_tbl_owner", "row": 12, "column": 6, "colspan": 6, "rowspan": 8},
    ]

    p2 = nav_row("p2", 3) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_flow", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_scenario", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_push", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_velocity", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 3) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_ch_risk_band", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_heatmap", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_exception", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_tbl_outliers", "row": 12, "column": 6, "colspan": 6, "rowspan": 8},
    ]

    return {
        "name": "RevenuePipelineAnalystLab",
        "numColumns": 12,
        "pages": [
            pg("exploration", "Exploration", p1),
            pg("scenarios", "Cohorts & Scenarios", p2),
            pg("qa", "Model QA", p3),
        ],
    }


def main() -> None:
    """Deploy the analyst pipeline dashboard using the manager pipeline dataset."""
    instance_url, token = get_auth()
    dataset_id = get_dataset_id(instance_url, token, DS)
    if not dataset_id:
        raise SystemExit(
            "Could not resolve dataset id for Pipeline_Opportunity_Operations. "
            "Deploy build_pipeline_opportunity_operations.py first."
        )

    steps = build_steps(dataset_id)
    widgets = build_widgets()
    layout = build_layout()
    state = build_dashboard_state(steps, widgets, layout)

    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    print(f"\n=== Deploying {DASHBOARD_LABEL} ===")
    deploy_dashboard(instance_url, token, dashboard_id, state)

    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "OpportunityName", "id_field": "Id", "label": "Opportunity"},
            {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
        ],
    )


if __name__ == "__main__":
    main()
