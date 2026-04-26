#!/usr/bin/env python3
"""Build the Executive Product Mix & Industry dashboard.

This is the first product-suite dashboard aligned to the deeper research plan.
It focuses on industry concentration, product mix, delivery-model mix, and
industry-level whitespace using the shared product dataset.
"""

from __future__ import annotations

from build_product_portfolio_dashboard import DS, create_dataset
from crm_analytics_helpers import (
    add_table_action,
    af,
    build_dashboard_state,
    choropleth_chart,
    coalesce_filter,
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    hdr,
    heatmap_chart,
    num,
    pg,
    pillbox,
    rich_chart,
    set_record_links_xmd,
    sq,
)

DASHBOARD_LABEL = "Executive Product Mix & Industry"


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_industry = coalesce_filter("f_industry", "IndustryVertical")
    filter_segment = coalesce_filter("f_segment", "Segment")
    filter_delivery = coalesce_filter("f_delivery", "DeliveryModel")
    detail = (
        load
        + 'q = filter q by RecordType == "account_detail";\n'
        + filter_industry
        + filter_segment
        + filter_delivery
    )
    opp = (
        load
        + 'q = filter q by RecordType == "opportunity_product";\n'
        + filter_industry
        + filter_segment
        + filter_delivery
    )

    return {
        "f_industry": af("IndustryVertical", ds_meta),
        "f_segment": af("Segment", ds_meta),
        "f_delivery": af("DeliveryModel", ds_meta),
        "s_summary": sq(
            detail
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(AccountCount) as account_count, "
            + "sum(InstalledARR) as installed_arr, "
            + "sum(OpenExpansionARR) as expansion_arr, "
            + "sum(WhitespaceARR) as whitespace_arr, "
            + "(case when sum(InstalledARR) > 0 then (sum(SaaSInstalledARR) / sum(InstalledARR)) * 100 else 0 end) as saas_mix_pct;"
        ),
        "s_mix_share": sq(
            opp
            + "q = filter q by WonARR > 0;\n"
            + 'q = filter q by IndustryVertical != "Other";\n'
            + 'q = filter q by ProductFamily != "Unmapped";\n'
            + "q = group q by (IndustryVertical, ProductFamily);\n"
            + "q = foreach q generate IndustryVertical, ProductFamily, sum(WonARR) as WonARR;\n"
            + "q = order q by IndustryVertical asc;"
        ),
        "s_attach_heatmap": sq(
            opp
            + "q = filter q by WonARR > 0;\n"
            + 'q = filter q by IndustryVertical != "Other";\n'
            + 'q = filter q by ProductFamily != "Unmapped";\n'
            + "q = group q by (IndustryVertical, ProductFamily, AccountId);\n"
            + "q = foreach q generate IndustryVertical, ProductFamily, AccountId, sum(WonARR) as WonARR;\n"
            + "q = group q by (IndustryVertical, ProductFamily);\n"
            + "q = foreach q generate IndustryVertical, ProductFamily, count() as AccountCoverage;\n"
            + "q = order q by AccountCoverage desc;"
        ),
        "s_industry_summary": sq(
            detail
            + 'q = filter q by IndustryVertical != "Other";\n'
            + "q = group q by IndustryVertical;\n"
            + "q = foreach q generate IndustryVertical, "
            + "sum(AccountCount) as AccountCount, "
            + "sum(InstalledARR) as InstalledARR, "
            + "sum(OpenExpansionARR) as OpenExpansionARR, "
            + "sum(WhitespaceARR) as WhitespaceARR, "
            + "(case when sum(InstalledARR) > 0 then (sum(SaaSInstalledARR) / sum(InstalledARR)) * 100 else 0 end) as SaaSMixPct;\n"
            + "q = order q by InstalledARR desc;"
        ),
        "s_segment_heatmap": sq(
            detail
            + 'q = filter q by IndustryVertical != "Other";\n'
            + "q = group q by (IndustryVertical, Segment);\n"
            + "q = foreach q generate IndustryVertical, Segment, sum(InstalledARR) as InstalledARR;\n"
            + "q = order q by InstalledARR desc;"
        ),
        "s_monthly_open": sq(
            opp
            + "q = filter q by OpenARR > 0;\n"
            + 'q = filter q by MonthLabel != "";\n'
            + 'q = filter q by IndustryVertical != "Other";\n'
            + "q = group q by (MonthDate, IndustryVertical);\n"
            + "q = foreach q generate MonthDate, IndustryVertical, sum(WeightedOpenARR) as WeightedOpenARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_delivery_mix": sq(
            detail
            + 'q = filter q by IndustryVertical != "Other";\n'
            + "q = group q by (IndustryVertical, DeliveryModel);\n"
            + "q = foreach q generate IndustryVertical, DeliveryModel, sum(InstalledARR) as InstalledARR;\n"
            + "q = order q by IndustryVertical asc;"
        ),
        "s_industry_gap": sq(
            detail
            + 'q = filter q by IndustryVertical != "Other";\n'
            + "q = group q by IndustryVertical;\n"
            + "q = foreach q generate IndustryVertical, "
            + "sum(InstalledARR) as InstalledARR, "
            + "sum(OpenExpansionARR) as OpenExpansionARR, "
            + "sum(WhitespaceARR) as WhitespaceARR;\n"
            + "q = order q by WhitespaceARR desc;"
        ),
        "s_country_arr": sq(
            detail
            + 'q = filter q by BillingCountry != "";\n'
            + 'q = filter q by BillingCountry != "Unknown";\n'
            + "q = group q by BillingCountry;\n"
            + "q = foreach q generate BillingCountry as Country, "
            + "sum(InstalledARR) as InstalledARR, "
            + "sum(WhitespaceARR) as WhitespaceARR;\n"
            + "q = order q by InstalledARR desc;"
        ),
        "s_top_accounts": sq(
            detail
            + 'q = filter q by IndustryVertical != "Other";\n'
            + "q = filter q by InstalledARR > 0;\n"
            + "q = foreach q generate AccountName, IndustryVertical, OwnerName, Segment, "
            + "InstalledARR, OpenExpansionARR, WhitespaceScore, ExpansionScore, AccountId;\n"
            + "q = order q by WhitespaceScore desc;\n"
            + "q = limit q 25;"
        ),
    }


def build_widgets() -> dict[str, dict]:
    """Build dashboard widgets."""
    widgets = {
        "p1_hdr": hdr(
            "Executive Product Mix & Industry",
            "Where SimCorp is concentrated today, where cross-sell whitespace is real, and which industry-product combinations deserve GTM focus next. KPI counts reflect the current filtered portfolio scope, not the full account master.",
        ),
        "p1_f_industry": pillbox("f_industry", "Industry"),
        "p1_f_segment": pillbox("f_segment", "Segment"),
        "p1_f_delivery": pillbox("f_delivery", "Delivery Model"),
        "p1_n_accounts": num("s_summary", "account_count", "Accounts in Scope", "#032D60", compact=True),
        "p1_n_installed": num("s_summary", "installed_arr", "Installed ARR", "#2E844A", compact=True),
        "p1_n_expansion": num("s_summary", "expansion_arr", "Open Expansion ARR", "#0176D3", compact=True),
        "p1_n_whitespace": num("s_summary", "whitespace_arr", "Whitespace ARR", "#BA0517", compact=True),
        "p1_n_saas": num("s_summary", "saas_mix_pct", "SaaS Mix %", "#9050E9", compact=True),
        "p1_ch_mix": rich_chart(
            "s_mix_share",
            "stackhbar",
            "Industry Product Mix Concentration",
            ["IndustryVertical"],
            ["WonARR"],
            split=["ProductFamily"],
            show_legend=True,
            axis_title="Share of ARR",
            normalize=True,
            show_values=True,
        ),
        "p1_ch_heatmap": heatmap_chart(
            "s_attach_heatmap",
            "Cross-Sell Penetration Heatmap",
            show_legend=True,
        ),
        "p1_tbl_industry": rich_chart(
            "s_industry_summary",
            "comparisontable",
            "Industry GTM Scorecard",
            ["IndustryVertical"],
            ["AccountCount", "InstalledARR", "OpenExpansionARR", "WhitespaceARR", "SaaSMixPct"],
            show_legend=False,
        ),
        "p2_hdr": hdr(
            "Industry Breakdown",
            "Which industries have scale, which have whitespace, and where delivery-model mix suggests the strongest GTM push.",
        ),
        "p2_f_industry": pillbox("f_industry", "Industry"),
        "p2_f_segment": pillbox("f_segment", "Segment"),
        "p2_f_delivery": pillbox("f_delivery", "Delivery Model"),
        "p2_ch_segment": heatmap_chart(
            "s_segment_heatmap",
            "Installed ARR by Industry x Segment",
            show_legend=True,
        ),
        "p2_ch_geo": choropleth_chart(
            "s_country_arr",
            "Global Installed ARR Footprint",
            "Country",
            "InstalledARR",
        ),
        "p2_ch_delivery": rich_chart(
            "s_delivery_mix",
            "stackhbar",
            "Delivery Model Mix by Industry",
            ["IndustryVertical"],
            ["InstalledARR"],
            split=["DeliveryModel"],
            show_legend=True,
            axis_title="Share of ARR",
            normalize=True,
            show_values=True,
        ),
        "p2_ch_gap": rich_chart(
            "s_industry_gap",
            "stackhbar",
            "Industry Growth and Whitespace Balance",
            ["IndustryVertical"],
            ["InstalledARR", "OpenExpansionARR", "WhitespaceARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p2_tbl_accounts": rich_chart(
            "s_top_accounts",
            "comparisontable",
            "Priority Cross-Sell Accounts",
            ["AccountName", "IndustryVertical", "OwnerName", "Segment"],
            ["InstalledARR", "OpenExpansionARR", "WhitespaceScore", "ExpansionScore"],
            show_legend=False,
        ),
    }
    add_table_action(widgets["p2_tbl_accounts"], "salesforceActions", "Account", "AccountId")
    return widgets


def build_layout() -> dict:
    """Build the 2-page executive dashboard layout."""
    p1 = [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_industry", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_delivery", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_accounts", "row": 5, "column": 0, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_installed", "row": 5, "column": 2, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_expansion", "row": 5, "column": 4, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_whitespace", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_saas", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p1_ch_mix", "row": 9, "column": 0, "colspan": 7, "rowspan": 7},
        {"name": "p1_ch_heatmap", "row": 9, "column": 7, "colspan": 5, "rowspan": 7},
        {"name": "p1_tbl_industry", "row": 16, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    p2 = [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_industry", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_delivery", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_segment", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_geo", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_delivery", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_gap", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_tbl_accounts", "row": 19, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "ExecutiveProductMixIndustry",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("industry", "Industry Breakdown", p2),
        ],
    }


def main() -> None:
    """Build dataset and deploy dashboard."""
    inst, tok = get_auth()
    if not create_dataset(inst, tok):
        raise SystemExit("Dataset upload failed")

    ds_id = get_dataset_id(inst, tok, DS)
    if not ds_id:
        raise SystemExit(f"Could not resolve dataset id for {DS}")

    state = build_dashboard_state(build_steps(ds_id), build_widgets(), build_layout())
    dashboard_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
    print(f"\n=== Deploying {DASHBOARD_LABEL} ===")
    deploy_dashboard(inst, tok, dashboard_id, state)

    set_record_links_xmd(
        inst,
        tok,
        DS,
        [
            {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
        ],
    )


if __name__ == "__main__":
    main()
