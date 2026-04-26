#!/usr/bin/env python3
"""Build the Executive Customer Risk & Growth dashboard.

This executive surface reuses the live Customer_Account_Health dataset so the
drill path and scoring contracts stay aligned with the manager dashboard.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from crm_analytics_helpers import (
    add_table_action,
    af,
    build_dashboard_state,
    bullet_chart,
    coalesce_filter,
    compare_table,
    create_dashboard_if_needed,
    deploy_dashboard,
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

DS = "Customer_Account_Health"
DASHBOARD_LABEL = "Executive Customer Risk & Growth"

# ── Consulting-grade patterns ─────────────────────────────────────────────
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_unit", "f_segment", "f_health"],
    },
}


def _shift_month_key(month_key_value: str, offset: int) -> str:
    """Add offset months to a YYYY-MM string."""
    dt = datetime.strptime(f"{month_key_value}-01", "%Y-%m-%d")
    month = dt.month - 1 + offset
    year = dt.year + month // 12
    month = month % 12 + 1
    return f"{year:04d}-{month:02d}"


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build executive dashboard steps on top of the manager customer dataset."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    current_month = datetime.now(UTC).strftime("%Y-%m")
    forecast_anchor_key = _shift_month_key(current_month, -1)
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_segment = coalesce_filter("f_segment", "Segment")
    filter_health = coalesce_filter("f_health", "HealthBand")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_segment
        + filter_health
    )
    customer_forecast = (
        load
        + 'q = filter q by RecordType == "executive_forecast";\n'
        + filter_unit
        + filter_segment
        + filter_health
        + "q = group q by (MonthDate, MonthLabel);\n"
        + "q = foreach q generate MonthDate, MonthLabel, "
        + f'(case when MonthLabel <= "{forecast_anchor_key}" then sum(ExecutiveForecastARR) else null end) as ActualARR, '
        + f'(case when MonthLabel >= "{forecast_anchor_key}" then sum(ExecutiveForecastARR) else null end) as ForecastARR, '
        + f'(case when MonthLabel == "{forecast_anchor_key}" then sum(ExecutiveForecastARR) '
        + f'when MonthLabel >= "{current_month}" then sum(ExecutiveForecastARR_high_95) else null end) as ForecastARR_high_95, '
        + f'(case when MonthLabel == "{forecast_anchor_key}" then sum(ExecutiveForecastARR) '
        + f'when MonthLabel >= "{current_month}" then sum(ExecutiveForecastARR_low_95) else null end) as ForecastARR_low_95;\n'
        + "q = order q by MonthDate asc;"
    )
    renewal_trend = (
        load
        + 'q = filter q by RecordType == "renewal_trend";\n'
        + filter_unit
        + filter_segment
        + filter_health
    )

    s_summary = sq(
        detail
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "sum(TotalWonARR) as customer_arr, "
        + "sum(ExpandPipelineARR) as expansion_pipe_arr, "
        + "sum(RenewalRiskARR) as renewal_risk_arr, "
        + "avg(HealthScore) as avg_health, "
        + "70 as target, "
        + "80 as good, "
        + "60 as satisfactory;"
    )
    s_summary.update(KPI_FACET_SCOPE)

    return {
        "f_unit": af("UnitGroup", ds_meta),
        "f_segment": af("Segment", ds_meta),
        "f_health": af("HealthBand", ds_meta),
        "s_summary": s_summary,
        "s_customer_forecast": sq(customer_forecast),
        "s_segment_mix": sq(
            detail
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, "
            + "sum(TotalWonARR) as CustomerARR, "
            + "sum(ExpandPipelineARR) as ExpandPipelineARR, "
            + "sum(RenewalRiskARR) as RenewalRiskARR;\n"
            + "q = order q by CustomerARR desc;"
        ),
        "s_renewal_outlook": sq(
            renewal_trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ExpiringAccounts) as ExpiringAccounts, "
            + "sum(RenewalRiskAccountCount) as RenewalRiskAccounts, "
            + "sum(RenewalRiskARR) as RenewalRiskARR, "
            + "sum(RenewalPipelineTrendARR) as RenewalPipelineARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_lifecycle_mix": sq(
            detail
            + "q = group q by LifecycleStage;\n"
            + "q = foreach q generate LifecycleStage, "
            + "sum(TotalWonARR) as CustomerARR, "
            + "sum(ExpandPipelineARR) as ExpandPipelineARR, "
            + "sum(RenewalRiskARR) as RenewalRiskARR;\n"
            + "q = order q by CustomerARR desc;"
        ),
        "s_gap_by_unit": sq(
            detail
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(KycGapCount) as KycGapCount, "
            + "sum(DataQualityGapCount) as DataQualityGapCount, "
            + "sum(UnderEngagedCount) as UnderEngagedCount, "
            + "sum(KycGapCount) + sum(DataQualityGapCount) + sum(UnderEngagedCount) as TotalGaps;\n"
            + "q = order q by TotalGaps desc;"
        ),
        "s_top_risk": sq(
            detail
            + "q = filter q by AtRiskAccountCount > 0;\n"
            + "q = foreach q generate AccountName, OwnerName, Segment, HealthBand, LifecycleStage, "
            + "RenewalRiskARR, RenewalRiskScore, HealthScore, OperatingGapScore, AccountId;\n"
            + "q = order q by RenewalRiskScore desc;\n"
            + "q = limit q 15;"
        ),
        "s_top_growth": sq(
            detail
            + "q = filter q by ExpansionScore >= 50 || ExpandPipelineARR > 0;\n"
            + "q = foreach q generate AccountName, OwnerName, Segment, ExpansionBand, LifecycleStage, "
            + "ExpandPipelineARR, ExpansionScore, NRRProxy, HealthScore, AccountId;\n"
            + "q = order q by ExpandPipelineARR desc;\n"
            + "q = limit q 15;"
        ),
    }


def build_widgets() -> dict[str, dict]:
    """Build the 2-page executive dashboard widgets."""
    forecast_start_key = datetime.now(UTC).strftime("%Y-%m")
    last_complete_key = _shift_month_key(forecast_start_key, -1)
    last_complete_label = date.fromisoformat(f"{last_complete_key}-01").strftime(
        "%b %Y"
    )
    forecast_start_label = date.fromisoformat(f"{forecast_start_key}-01").strftime(
        "%b %Y"
    )
    current_year_label = datetime.now(UTC).strftime("%Y")

    customer_forecast_chart = rich_chart(
        "s_customer_forecast",
        "line",
        "Customer ARR Trend & Forecast",
        ["MonthLabel"],
        ["ActualARR", "ForecastARR", "ForecastARR_high_95", "ForecastARR_low_95"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    customer_forecast_chart["parameters"]["title"]["subtitleLabel"] = (
        f"Current {current_year_label} cumulative ARR won through {last_complete_label} | Model forecast begins {forecast_start_label}"
    )

    widgets = {
        "p1_nav1": nav_link("summary", "Summary", active=True),
        "p1_nav2": nav_link("drivers", "Drivers & Risks"),
        "p1_hdr": hdr(
            "Executive Customer Risk & Growth",
            "Executive view of customer value, expansion capacity, renewal exposure, and the accounts driving growth or risk.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_segment": pillbox("f_segment", "Segment"),
        "p1_f_health": pillbox("f_health", "Health"),
        "p1_n_arr": num(
            "s_summary",
            "customer_arr",
            "Customer ARR",
            "#032D60",
            compact=True,
            tier="primary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_expand": num(
            "s_summary",
            "expansion_pipe_arr",
            "Expansion Pipeline ARR",
            "#0176D3",
            compact=True,
            tier="secondary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_renewal": num(
            "s_summary",
            "renewal_risk_arr",
            "Renewal Risk ARR",
            "#8E030F",
            compact=True,
            tier="secondary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_ch_timeline": customer_forecast_chart,
        "p1_ch_bullet": bullet_chart(
            "s_summary",
            "Average Health Score vs Target",
            axis_title="Score",
        ),
        "p1_ch_segment": rich_chart(
            "s_segment_mix",
            "stackhbar",
            "Customer ARR, Expansion, and Renewal Risk by Segment",
            ["Segment"],
            ["CustomerARR", "ExpandPipelineARR", "RenewalRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
            subtitle="Segments with high Renewal Risk relative to Customer ARR need immediate retention focus",
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("drivers", "Drivers & Risks", active=True),
        "p2_hdr": hdr(
            "Drivers & Risks",
            "Where renewal pressure, lifecycle imbalance, and operating gaps are concentrated, and which accounts need escalation.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_segment": pillbox("f_segment", "Segment"),
        "p2_f_health": pillbox("f_health", "Health"),
        "p2_section_renewal": section_label("Renewal Pressure & Lifecycle"),
        "p2_ch_renewal": line_chart(
            "s_renewal_outlook",
            "Renewal Outlook: Expiring Accounts, Risk Accounts, and Renewal Risk ARR",
            show_legend=True,
            axis_title="Accounts / ARR",
            subtitle="Tracks renewal exposure by month — spikes indicate concentrated renewal risk windows",
        ),
        "p2_ch_lifecycle": rich_chart(
            "s_lifecycle_mix",
            "stackcolumn",
            "Customer ARR, Expansion, and Risk by Lifecycle",
            ["LifecycleStage"],
            ["CustomerARR", "ExpandPipelineARR", "RenewalRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
            subtitle="Lifecycle stages: Onboarding → Growing → Mature → Declining (based on tenure and engagement trend)",
        ),
        "p2_ch_gap": rich_chart(
            "s_gap_by_unit",
            "stackhbar",
            "Operating Gaps by Unit Group",
            ["UnitGroup"],
            ["KycGapCount", "DataQualityGapCount", "UnderEngagedCount"],
            show_legend=True,
            axis_title="Accounts",
            show_values=True,
            subtitle="KYC = incomplete know-your-customer | Data Quality = missing critical fields | Under-Engaged = below peer benchmark",
        ),
        "p2_section_accounts": section_label("Account Action Queues"),
        "p2_tbl_risk": compare_table(
            "s_top_risk",
            "Top Customer Risk Accounts",
            ["AccountName", "OwnerName", "Segment", "HealthBand", "LifecycleStage"],
            ["RenewalRiskARR", "RenewalRiskScore", "HealthScore", "OperatingGapScore"],
            subtitle="Renewal Risk = contract proximity x health decline x engagement drop | Health = NPS + adoption + support",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "RenewalRiskScore",
                    "rules": [
                        {"value": 75, "color": "#D4504C", "operator": "gte"},
                        {"value": 50, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "HealthScore",
                    "rules": [
                        {"value": 60, "color": "#D4504C", "operator": "lte"},
                        {"value": 80, "color": "#FFB75D", "operator": "lte"},
                    ],
                },
            ],
        ),
        "p2_tbl_growth": compare_table(
            "s_top_growth",
            "Top Growth Accounts",
            ["AccountName", "OwnerName", "Segment", "ExpansionBand", "LifecycleStage"],
            ["ExpandPipelineARR", "ExpansionScore", "NRRProxy", "HealthScore"],
            subtitle="Expansion Score = whitespace x engagement growth x adoption velocity | NRR = 12-month net revenue retention",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ExpansionScore",
                    "rules": [
                        {"value": 80, "color": "#04844B", "operator": "gte"},
                        {"value": 50, "color": "#0176D3", "operator": "gte"},
                    ],
                },
            ],
        ),
    }

    # Subtitle for bullet chart on Summary page
    widgets["p1_ch_bullet"]["parameters"]["title"]["subtitleLabel"] = (
        "Green ≥80 (healthy) | Yellow 60-80 (monitor) | Target = 70 | Health = NPS + adoption + support engagement"
    )

    add_table_action(
        widgets["p2_tbl_risk"], "salesforceActions", "Account", "AccountId"
    )
    add_table_action(
        widgets["p2_tbl_growth"], "salesforceActions", "Account", "AccountId"
    )
    return widgets


def build_layout() -> dict:
    """Build grid layout for the 2-page executive dashboard."""
    p1 = nav_row("p1", 2) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_arr", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_expand", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_renewal", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_ch_timeline", "row": 9, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_bullet", "row": 17, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p1_ch_segment", "row": 17, "column": 4, "colspan": 8, "rowspan": 6},
    ]

    p2 = nav_row("p2", 2) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {
            "name": "p2_section_renewal",
            "row": 5,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p2_ch_renewal", "row": 6, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p2_ch_lifecycle", "row": 6, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p2_ch_gap", "row": 12, "column": 0, "colspan": 6, "rowspan": 6},
        {
            "name": "p2_section_accounts",
            "row": 12,
            "column": 6,
            "colspan": 6,
            "rowspan": 1,
        },
        {"name": "p2_tbl_risk", "row": 13, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p2_tbl_growth", "row": 21, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    return {
        "name": "ExecutiveCustomerRiskGrowth",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("drivers", "Drivers & Risks", p2),
        ],
    }


def main() -> None:
    """Deploy the executive customer dashboard using the manager customer dataset."""
    instance_url, token = get_auth()
    dataset_id = get_dataset_id(instance_url, token, DS)
    if not dataset_id:
        raise SystemExit(
            "Could not resolve dataset id for Customer_Account_Health. "
            "Deploy build_customer_account_health.py first."
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
            {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
        ],
    )


if __name__ == "__main__":
    main()
