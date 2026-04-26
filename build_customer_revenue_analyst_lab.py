#!/usr/bin/env python3
"""Build the Customer/Revenue Analyst Lab dashboard.

This analyst surface reuses the live Customer_Account_Health dataset so the
exploratory pages stay aligned with the operating customer health model.
"""

from __future__ import annotations

from crm_analytics_helpers import (
    add_table_action,
    af,
    build_dashboard_state,
    coalesce_filter,
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
    set_record_links_xmd,
    sq,
    timeline_chart,
    treemap_chart,
)

DS = "Customer_Account_Health"
DASHBOARD_LABEL = "Customer/Revenue Analyst Lab"


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build analyst dashboard steps on top of the manager customer dataset."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_segment = coalesce_filter("f_segment", "Segment")
    filter_health = coalesce_filter("f_health", "HealthBand")

    detail = load + 'q = filter q by RecordType == "detail";\n' + filter_unit + filter_segment + filter_health
    revenue_trend = (
        load + 'q = filter q by RecordType == "revenue_trend";\n' + filter_unit + filter_segment + filter_health
    )
    renewal_trend = (
        load + 'q = filter q by RecordType == "renewal_trend";\n' + filter_unit + filter_segment + filter_health
    )

    return {
        "f_unit": af("UnitGroup", ds_meta),
        "f_segment": af("Segment", ds_meta),
        "f_health": af("HealthBand", ds_meta),
        "s_segment_health_heatmap": sq(
            detail
            + "q = group q by (Segment, HealthBand);\n"
            + "q = foreach q generate Segment, HealthBand, sum(TotalWonARR) as CustomerARR;\n"
            + "q = order q by CustomerARR desc;"
        ),
        "s_lifecycle_treemap": sq(
            detail
            + "q = group q by (LifecycleStage, ExpansionBand);\n"
            + "q = foreach q generate LifecycleStage, ExpansionBand, sum(TotalWonARR) as CustomerARR;\n"
            + "q = order q by CustomerARR desc;"
        ),
        "s_score_by_segment": sq(
            detail
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, "
            + "avg(HealthScore) as HealthScore, "
            + "avg(ExpansionScore) as ExpansionScore, "
            + "avg(RenewalRiskScore) as RenewalRiskScore;\n"
            + "q = order q by HealthScore desc;"
        ),
        "s_owner_outliers": sq(
            detail
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(TotalWonARR) as CustomerARR, "
            + "sum(ExpandPipelineARR) as ExpandPipelineARR, "
            + "sum(RenewalRiskARR) as RenewalRiskARR, "
            + "avg(OperatingGapScore) as OperatingGapScore;\n"
            + "q = order q by RenewalRiskARR desc;\n"
            + "q = limit q 15;"
        ),
        "s_revenue_trajectory": sq(
            revenue_trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ActualARR) as ActualARR, "
            + "sum(OpenExpansionARR) as OpenExpansionARR, "
            + "sum(RegressionForecastARR) as RegressionForecastARR, "
            + "sum(RegressionUpperARR) as RegressionUpperARR, "
            + "sum(RegressionLowerARR) as RegressionLowerARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_retention_cohort": sq(
            detail
            + 'q = filter q by CreatedDate != "";\n'
            + "q = group q by substr(CreatedDate, 1, 4);\n"
            + "q = foreach q generate substr(CreatedDate, 1, 4) as CohortYear, "
            + "avg(NRRProxy) as NRRProxy, "
            + "avg(GRRProxy) as GRRProxy, "
            + "count() as CustomerCount;\n"
            + "q = order q by CohortYear asc;"
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
        "s_gap_by_unit": sq(
            detail
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(KycGapCount) as KycGapCount, "
            + "sum(DataQualityGapCount) as DataQualityGapCount, "
            + "sum(UnderEngagedCount) as UnderEngagedCount;\n"
            + "q = order q by (KycGapCount + DataQualityGapCount + UnderEngagedCount) desc;"
        ),
        "s_term_health_heatmap": sq(
            detail
            + "q = group q by (TermBucket, HealthBand);\n"
            + "q = foreach q generate TermBucket, HealthBand, sum(RenewalRiskARR) as RenewalRiskARR;\n"
            + "q = order q by RenewalRiskARR desc;"
        ),
        "s_kyc_profile": sq(
            detail
            + "q = group q by KYCStatus;\n"
            + "q = foreach q generate KYCStatus, "
            + "count() as AccountCount, "
            + "sum(TotalWonARR) as CustomerARR, "
            + "sum(RenewalRiskARR) as RenewalRiskARR;\n"
            + "q = order q by AccountCount desc;"
        ),
        "s_lifecycle_profile": sq(
            detail
            + "q = group q by LifecycleStage;\n"
            + "q = foreach q generate LifecycleStage, "
            + "avg(HealthScore) as HealthScore, "
            + "avg(ExpansionScore) as ExpansionScore, "
            + "avg(RenewalRiskScore) as RenewalRiskScore;\n"
            + "q = order q by HealthScore desc;"
        ),
        "s_outlier_accounts": sq(
            detail
            + "q = filter q by RenewalRiskScore > 65 || OperatingGapScore > 50 || ExpansionScore > 60;\n"
            + "q = foreach q generate AccountName, OwnerName, Segment, HealthBand, ExpansionBand, "
            + "HealthScore, ExpansionScore, RenewalRiskScore, OperatingGapScore, AccountId;\n"
            + "q = order q by RenewalRiskScore desc;\n"
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
            "Customer/Revenue Analyst Lab",
            "Exploratory surface for customer value concentration, health segmentation, and ownership outliers.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_segment": pillbox("f_segment", "Segment"),
        "p1_f_health": pillbox("f_health", "Health"),
        "p1_ch_heatmap": heatmap_chart(
            "s_segment_health_heatmap",
            "Segment x Health Band ARR Heatmap",
        ),
        "p1_ch_treemap": treemap_chart(
            "s_lifecycle_treemap",
            "Lifecycle / Expansion Exposure Map",
            ["LifecycleStage", "ExpansionBand"],
            "CustomerARR",
            show_legend=True,
        ),
        "p1_ch_scores": rich_chart(
            "s_score_by_segment",
            "hbar",
            "Health, Expansion, and Renewal Risk by Segment",
            ["Segment"],
            ["HealthScore", "ExpansionScore", "RenewalRiskScore"],
            show_legend=True,
            axis_title="Score",
            show_values=True,
        ),
        "p1_tbl_owner": rich_chart(
            "s_owner_outliers",
            "comparisontable",
            "Owner Exposure Outliers",
            ["OwnerName"],
            ["CustomerARR", "ExpandPipelineARR", "RenewalRiskARR", "OperatingGapScore"],
            show_legend=False,
        ),
        "p2_nav1": nav_link("exploration", "Exploration"),
        "p2_nav2": nav_link("scenarios", "Cohorts & Scenarios", active=True),
        "p2_nav3": nav_link("qa", "Model QA"),
        "p2_hdr": hdr(
            "Cohorts & Scenarios",
            "Revenue trajectory, retention cohorts, renewal timing, and unit-level operating scenarios.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_segment": pillbox("f_segment", "Segment"),
        "p2_f_health": pillbox("f_health", "Health"),
        "p2_ch_timeline": timeline_chart(
            "s_revenue_trajectory",
            "Customer Revenue and Expansion Trajectory",
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        "p2_ch_cohort": rich_chart(
            "s_retention_cohort",
            "line",
            "NRR and GRR by Customer Cohort",
            ["CohortYear"],
            ["NRRProxy", "GRRProxy"],
            show_legend=True,
            axis_title="Retention %",
        ),
        "p2_ch_renewal": rich_chart(
            "s_renewal_outlook",
            "line",
            "Renewal Outlook: Expiring Accounts, Risk Accounts, and Risk ARR",
            ["MonthDate"],
            ["ExpiringAccounts", "RenewalRiskAccounts", "RenewalRiskARR"],
            show_legend=True,
            axis_title="Accounts / ARR",
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
        ),
        "p3_nav1": nav_link("exploration", "Exploration"),
        "p3_nav2": nav_link("scenarios", "Cohorts & Scenarios"),
        "p3_nav3": nav_link("qa", "Model QA", active=True),
        "p3_hdr": hdr(
            "Model QA",
            "Health model diagnostics, term-risk clustering, and the specific accounts that do not fit the expected pattern.",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_segment": pillbox("f_segment", "Segment"),
        "p3_f_health": pillbox("f_health", "Health"),
        "p3_ch_heatmap": heatmap_chart(
            "s_term_health_heatmap",
            "Term Bucket x Health Band Risk Heatmap",
        ),
        "p3_ch_kyc": rich_chart(
            "s_kyc_profile",
            "stackcolumn",
            "KYC Status Profile",
            ["KYCStatus"],
            ["AccountCount", "CustomerARR", "RenewalRiskARR"],
            show_legend=True,
            axis_title="Accounts / ARR",
            show_values=True,
        ),
        "p3_ch_lifecycle": rich_chart(
            "s_lifecycle_profile",
            "hbar",
            "Health, Expansion, and Renewal Risk by Lifecycle",
            ["LifecycleStage"],
            ["HealthScore", "ExpansionScore", "RenewalRiskScore"],
            show_legend=True,
            axis_title="Score",
            show_values=True,
        ),
        "p3_tbl_outliers": rich_chart(
            "s_outlier_accounts",
            "comparisontable",
            "Analyst Outlier Accounts",
            ["AccountName", "OwnerName", "Segment", "HealthBand", "ExpansionBand"],
            ["HealthScore", "ExpansionScore", "RenewalRiskScore", "OperatingGapScore"],
            show_legend=False,
        ),
    }

    add_table_action(widgets["p3_tbl_outliers"], "salesforceActions", "Account", "AccountId")
    return widgets


def build_layout() -> dict:
    """Build grid layout for the 3-page analyst dashboard."""
    p1 = nav_row("p1", 3) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_ch_heatmap", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p1_ch_treemap", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p1_ch_scores", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p1_tbl_owner", "row": 12, "column": 6, "colspan": 6, "rowspan": 8},
    ]

    p2 = nav_row("p2", 3) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_timeline", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_cohort", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_renewal", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_gap", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 3) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_ch_heatmap", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_kyc", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_lifecycle", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_tbl_outliers", "row": 12, "column": 6, "colspan": 6, "rowspan": 8},
    ]

    return {
        "name": "CustomerRevenueAnalystLab",
        "numColumns": 12,
        "pages": [
            pg("exploration", "Exploration", p1),
            pg("scenarios", "Cohorts & Scenarios", p2),
            pg("qa", "Model QA", p3),
        ],
    }


def main() -> None:
    """Deploy the analyst customer dashboard using the manager customer dataset."""
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
    state = build_dashboard_state(steps, widgets, layout)

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
