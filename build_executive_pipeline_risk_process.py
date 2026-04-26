#!/usr/bin/env python3
"""Build the Executive Pipeline Risk & Process dashboard.

This executive surface reuses the live Pipeline_Opportunity_Operations dataset so
the drill path and metric contracts stay aligned with the manager dashboard.
"""

from __future__ import annotations

from crm_analytics_helpers import (
    add_table_action,
    af,
    build_dashboard_state,
    bullet_chart,
    coalesce_filter,
    combo_chart,
    compare_table,
    create_dashboard_if_needed,
    deploy_dashboard,
    funnel_chart,
    get_auth,
    get_dataset_id,
    hdr,
    KPI_CARD_STYLE,
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
)

DS = "Pipeline_Opportunity_Operations"
DASHBOARD_LABEL = "Executive Pipeline Risk & Process"

# ── Consulting-grade patterns ─────────────────────────────────────────────
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_unit", "f_fy", "f_region"],
    },
}


def _rebind(binding: str, alias: str) -> str:
    """Retarget a coalesce_filter binding to a specific SAQL alias."""
    return (
        binding.replace("q =", f"{alias} =")
        .replace("q by", f"{alias} by")
        .replace("q generate", f"{alias} generate")
    )


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build executive dashboard steps on top of the manager pipeline dataset."""
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

    q1_filters = (
        _rebind(filter_unit, "q1")
        + _rebind(filter_fy, "q1")
        + _rebind(filter_region, "q1")
    )
    q2_filters = (
        _rebind(filter_unit, "q2")
        + _rebind(filter_fy, "q2")
        + _rebind(filter_region, "q2")
    )
    q3_filters = (
        _rebind(filter_unit, "q3")
        + _rebind(filter_fy, "q3")
        + _rebind(filter_region, "q3")
    )

    summary = (
        f'q1 = load "{DS}";\n'
        'q1 = filter q1 by RecordType == "detail";\n'
        + q1_filters
        + 'q1 = filter q1 by IsWon == "true";\n'
        + "q1 = group q1 by all;\n"
        + "q1 = foreach q1 generate sum(ActualARR) as actual_closed;\n"
        + f'q2 = load "{DS}";\n'
        + 'q2 = filter q2 by RecordType == "detail";\n'
        + q2_filters
        + 'q2 = filter q2 by IsClosed == "false";\n'
        + "q2 = group q2 by all;\n"
        + "q2 = foreach q2 generate sum(WeightedOpenARR) as weighted_open, sum(AtRiskARR) as at_risk_arr;\n"
        + f'q3 = load "{DS}";\n'
        + 'q3 = filter q3 by RecordType == "detail";\n'
        + q3_filters
        + "q3 = group q3 by (OwnerName, FYLabel);\n"
        + "q3 = foreach q3 generate max(QuotaAmount) as owner_quota;\n"
        + "q3 = group q3 by all;\n"
        + "q3 = foreach q3 generate sum(owner_quota) as total_quota;\n"
        + "q = cogroup q1 by all, q2 by all, q3 by all;\n"
        + "q = foreach q generate "
        + "coalesce(sum(q1.actual_closed), 0) as actual_closed, "
        + "coalesce(sum(q2.weighted_open), 0) as weighted_open, "
        + "(coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) as projected, "
        + "coalesce(sum(q2.at_risk_arr), 0) as at_risk_arr, "
        + "coalesce(sum(q3.total_quota), 0) as target, "
        + "((coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) - coalesce(sum(q3.total_quota), 0)) as gap_to_plan, "
        + "(coalesce(sum(q3.total_quota), 0) * 0.90) as good, "
        + "(coalesce(sum(q3.total_quota), 0) * 0.75) as satisfactory;"
    )

    # Apply KPI facet scoping
    s_summary = sq(summary)
    s_summary.update(KPI_FACET_SCOPE)

    return {
        "f_unit": af("UnitGroup", ds_meta),
        "f_fy": af("FYLabel", ds_meta),
        "f_region": af("SalesRegion", ds_meta),
        "s_summary": s_summary,
        "s_process_summary": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(CriticalExceptionCount) as critical_exceptions, "
            + "sum(PastDueCount) as past_due_count, "
            + "sum(StaleCount) as stale_count, "
            + "sum(MissingApprovalCount) as missing_approval_count, "
            + "(sum(PastDueARR) + sum(StaleARR) + sum(MissingApprovalARR)) as stuck_arr;"
        ),
        "s_monthly_trajectory": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ActualARR) as ActualARR, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(RegressionForecastARR) as RegressionForecastARR, "
            + "sum(RegressionUpperARR) as RegressionUpperARR, "
            + "sum(RegressionLowerARR) as RegressionLowerARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_stage_funnel": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (StageOrder, StageName);\n"
            + "q = foreach q generate StageOrder, StageName, sum(WeightedOpenARR) as WeightedOpenARR;\n"
            + "q = order q by StageOrder asc;"
        ),
        "s_quarter_confidence": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = foreach q generate CloseQuarter, "
            + 'case when ForecastCategory == "Commit" then WeightedOpenARR else 0 end as CommitARR, '
            + 'case when ForecastCategory == "Best Case" then WeightedOpenARR else 0 end as BestCaseARR, '
            + 'case when ForecastCategory == "Pipeline" then WeightedOpenARR else 0 end as PipelineARR;\n'
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(CommitARR) as CommitARR, "
            + "sum(BestCaseARR) as BestCaseARR, "
            + "sum(PipelineARR) as PipelineARR;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        "s_stage_velocity": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by StageOrder != "00";\n'
            + "q = group q by (StageOrder, StageName);\n"
            + "q = foreach q generate StageOrder, StageName, "
            + "avg(DaysInStage) as AvgDaysInStage, "
            + "avg(StageSlaDays) as StageSlaDays;\n"
            + "q = order q by StageOrder asc;"
        ),
        "s_region_pressure": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by SalesRegion;\n"
            + "q = foreach q generate SalesRegion, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by AtRiskARR desc;"
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
        "s_top_risk": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by AtRiskARR > 0;\n"
            + "q = group q by (OpportunityName, AccountName, OwnerName, StageName, ExceptionType, Id);\n"
            + "q = foreach q generate OpportunityName, AccountName, OwnerName, StageName, ExceptionType, "
            + "max(WeightedOpenARR) as WeightedOpenARR, "
            + "max(TotalRiskScore) as TotalRiskScore, "
            + "max(SlipRiskScore) as SlipRiskScore, "
            + "max(PushCount) as PushCount, "
            + "Id;\n"
            + "q = order q by TotalRiskScore desc;\n"
            + "q = limit q 15;"
        ),
        "s_top_process": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by (PastDueCount > 0) or (MissingApprovalCount > 0) or (StaleCount > 0) or (BackwardMoveCount > 1);\n"
            + "q = group q by (OpportunityName, AccountName, OwnerName, StageName, ExceptionType, Id);\n"
            + "q = foreach q generate OpportunityName, AccountName, OwnerName, StageName, ExceptionType, "
            + "max(WeightedOpenARR) as WeightedOpenARR, "
            + "max(DaysInStage) as DaysInStage, "
            + "max(PushCount) as PushCount, "
            + "max(BackwardMoveCount) as BackwardMoveCount, "
            + "max(MissingApprovalCount) as MissingApprovalCount, "
            + "Id;\n"
            + "q = order q by WeightedOpenARR desc;\n"
            + "q = limit q 15;"
        ),
    }


def build_widgets() -> dict[str, dict]:
    """Build the 2-page executive dashboard widgets."""
    widgets = {
        "p1_nav1": nav_link("summary", "Summary", active=True),
        "p1_nav2": nav_link("drivers", "Drivers & Risks"),
        "p1_hdr": hdr(
            "Executive Pipeline Risk & Process",
            "Executive view of plan exposure, forecast confidence, process breakdowns, and drill paths into the operating pipeline.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p1_f_region": pillbox("f_region", "Region"),
        "p1_n_projected": num(
            "s_summary",
            "projected",
            "Projected ARR",
            "#032D60",
            compact=True,
            tier="primary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_gap": num(
            "s_summary",
            "gap_to_plan",
            "Gap To Plan",
            "#BA0517",
            compact=True,
            tier="secondary",
            prefix="€",
            sentiment_color=True,
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_atrisk": num(
            "s_summary",
            "at_risk_arr",
            "At-Risk ARR",
            "#8E030F",
            compact=True,
            tier="secondary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_ch_timeline": rich_chart(
            "s_monthly_trajectory",
            "line",
            "Pipeline Trajectory: Actual vs Weighted Open vs Forecast",
            ["MonthLabel"],
            [
                "ActualARR",
                "WeightedOpenARR",
                "RegressionForecastARR",
                "RegressionUpperARR",
                "RegressionLowerARR",
            ],
            show_legend=True,
            axis_title="ARR (EUR)",
            subtitle="Actual = closed won | Weighted Open = probability-adjusted pipeline | Shaded = regression forecast with 95% confidence band",
        ),
        "p1_ch_bullet": bullet_chart(
            "s_summary",
            "Projected ARR vs Plan",
            axis_title="ARR (EUR)",
        ),
        "p1_ch_stage": funnel_chart(
            "s_stage_funnel",
            "Open Pipeline by Stage",
            "StageName",
            "WeightedOpenARR",
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("drivers", "Drivers & Risks", active=True),
        "p2_hdr": hdr(
            "Drivers & Risks",
            "Where forecast confidence breaks down, where process pressure is building, and which deals require executive escalation.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_ch_quarter": rich_chart(
            "s_quarter_confidence",
            "stackcolumn",
            "Quarterly Forecast Mix by Confidence",
            ["CloseQuarter"],
            ["CommitARR", "BestCaseARR", "PipelineARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
            subtitle="Commit = high-confidence pipeline | Best Case = moderate confidence | Pipeline = early stage",
        ),
        "p2_section_pipeline": section_label("Pipeline Category & Velocity"),
        "p2_ch_velocity": combo_chart(
            "s_stage_velocity",
            "Days in Stage vs SLA",
            ["StageName"],
            ["AvgDaysInStage"],
            ["StageSlaDays"],
            show_legend=True,
            axis_title="Days",
            subtitle="Bars exceeding the SLA line indicate stages where deals are aging beyond expected cycle time",
            reference_lines=[
                {"value": 30, "label": "30-Day Threshold", "color": "#FFB75D"}
            ],
            axis1_format="#,##0",
        ),
        "p2_ch_region": rich_chart(
            "s_region_pressure",
            "stackhbar",
            "Open vs At-Risk ARR by Region",
            ["SalesRegion"],
            ["WeightedOpenARR", "AtRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
            subtitle="At-Risk = deals flagged by slip risk model (>50% slip probability) or process exceptions",
        ),
        "p2_section_risk": section_label("Regional Risk & Close Date Drift"),
        "p2_ch_push": line_chart(
            "s_push_trend",
            "Close Date Push Trend",
            show_legend=True,
            axis_title="Pushes / Days",
            subtitle="Rising trend signals forecast degradation from close date drift across the pipeline",
            reference_lines=[
                {"value": 5, "label": "Push Threshold", "color": "#D4504C"}
            ],
        ),
        "p2_section_deals": section_label("Executive Action Queue"),
        "p2_tbl_risk": compare_table(
            "s_top_risk",
            "Top Pipeline Risk Deals",
            [
                "OpportunityName",
                "AccountName",
                "OwnerName",
                "StageName",
                "ExceptionType",
            ],
            ["WeightedOpenARR", "TotalRiskScore", "SlipRiskScore", "PushCount"],
            subtitle="Risk Score (0-100) = slip risk (push count, age, backward moves) + exception flags (SLA breach, stale)",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "TotalRiskScore",
                    "rules": [
                        {"value": 75, "color": "#D4504C", "operator": "gte"},
                        {"value": 50, "color": "#FFB75D", "operator": "gte"},
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
        ),
        "p2_tbl_process": compare_table(
            "s_top_process",
            "Top Process Exception Deals",
            [
                "OpportunityName",
                "AccountName",
                "OwnerName",
                "StageName",
                "ExceptionType",
            ],
            [
                "WeightedOpenARR",
                "DaysInStage",
                "PushCount",
                "BackwardMoveCount",
                "MissingApprovalCount",
            ],
            subtitle="Flagged for: past close date, >90d stale, missing approval, or >1 backward stage move",
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
                    "field": "BackwardMoveCount",
                    "rules": [
                        {"value": 2, "color": "#D4504C", "operator": "gte"},
                    ],
                },
            ],
        ),
    }

    # Subtitles for Summary page helpers that don't natively support them
    widgets["p1_ch_bullet"]["parameters"]["title"]["subtitleLabel"] = (
        "Green = on track (≥90% of plan) | Yellow = monitor (75-90%) | Projected = Closed Won + Weighted Open"
    )
    widgets["p1_ch_stage"]["parameters"]["title"]["subtitleLabel"] = (
        "Weighted by probability — narrowing funnel shows conversion yield through the pipeline"
    )

    add_table_action(widgets["p2_tbl_risk"], "salesforceActions", "Opportunity", "Id")
    add_table_action(
        widgets["p2_tbl_process"], "salesforceActions", "Opportunity", "Id"
    )
    return widgets


def build_layout() -> dict:
    """Build grid layout for the 2-page executive dashboard."""
    p1 = nav_row("p1", 2) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_projected", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_gap", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_atrisk", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_ch_timeline", "row": 9, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_bullet", "row": 17, "column": 0, "colspan": 5, "rowspan": 6},
        {"name": "p1_ch_stage", "row": 17, "column": 5, "colspan": 7, "rowspan": 6},
    ]

    p2 = nav_row("p2", 2) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {
            "name": "p2_section_pipeline",
            "row": 5,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p2_ch_quarter", "row": 6, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p2_ch_velocity", "row": 6, "column": 6, "colspan": 6, "rowspan": 6},
        {
            "name": "p2_section_risk",
            "row": 12,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p2_ch_region", "row": 13, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p2_ch_push", "row": 13, "column": 6, "colspan": 6, "rowspan": 6},
        {
            "name": "p2_section_deals",
            "row": 19,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p2_tbl_risk", "row": 20, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p2_tbl_process", "row": 27, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "ExecutivePipelineRiskProcess",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("drivers", "Drivers & Risks", p2),
        ],
    }


def main() -> None:
    """Deploy the executive pipeline dashboard using the manager pipeline dataset."""
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
    state = build_dashboard_state(
        steps,
        widgets,
        layout,
        bg_color="#F4F6F9",
        cell_spacing=8,
        row_height="normal",
    )

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
