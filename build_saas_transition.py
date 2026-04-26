#!/usr/bin/env python3
"""SaaS Transition & Delivery Model Dashboard Builder.

Tracks SimCorp's strategic shift from on-prem (SCD Software/License)
to cloud-native (SimCorp SaaS/XaaS) and Managed Business Services.
Uses OpportunityLineItem data to build account-level delivery model mix,
yearly transition trends, and migration candidate identification.

Pages:
  1. Transition Overview — SaaS share trend, delivery mix KPIs, bridge chart
  2. Industry Deep Dive — SaaS adoption by industry, trellised comparisons
  3. Account Migration — account-level mix, transition candidates, top movers
  4. MBS & Services — Managed Business Services pipeline and attach analysis
"""

import csv
import io
import json
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, "/Users/test/crm-analytics")
from crm_analytics_helpers import (
    get_auth,
    upload_dataset,
    get_dataset_id,
    build_dashboard_state,
    deploy_dashboard,
    create_dashboard_if_needed,
    set_record_links_xmd,
    sq,
    af,
    num,
    rich_chart,
    combo_chart,
    compare_table,
    line_chart,
    section_label,
    KPI_CARD_STYLE,
    hdr,
    listselector,
    nav_link,
    nav_row,
    pg,
    _dim,
    _measure,
)

DS = "SaaS_Transition_Delivery"
DS_LABEL = "SaaS Transition & Delivery Model"
DASHBOARD_LABEL = "SaaS Transition & Delivery Model"

# Consulting-grade faceting: KPIs respond to filter pillboxes
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_year", "f_industry", "f_account", "f_family"],
    },
}

# Delivery model classification
SAAS_FAMILIES = {"SimCorp SaaS", "XaaS"}
ONPREM_FAMILIES = {"SCD Software", "License"}
MBS_FAMILIES = {"MBS"}
SERVICES_FAMILIES = {
    "SCD Operational Services",
    "SCD Consulting",
    "Analytics Services",
    "Data Management Services",
    "Regulatory Services",
    "Client Communications",
    "Data Management",
}

# All families we care about for the transition story
ALL_TRACKED = SAAS_FAMILIES | ONPREM_FAMILIES | MBS_FAMILIES | SERVICES_FAMILIES

LINE_ITEM_SOQL = (
    "SELECT Id, Opportunity.Id, Opportunity.Name, Opportunity.CloseDate, "
    "Opportunity.Type, Opportunity.StageName, Opportunity.IsWon, Opportunity.IsClosed, "
    "Opportunity.Account.Id, Opportunity.Account.Name, Opportunity.Account.Industry, "
    "Opportunity.Owner.Name, "
    "Product2.Family, Product2.Name, "
    "Quantity, UnitPrice "
    "FROM OpportunityLineItem "
    "WHERE Opportunity.CloseDate >= 2022-01-01 "
    "AND Opportunity.IsWon = true "
    "ORDER BY Opportunity.CloseDate"
)


def run_soql(inst, tok, query):
    """Run a SOQL query with auto-pagination."""
    all_records = []
    url = f"{inst}/services/data/v66.0/query?q={urllib.parse.quote(query)}"
    while url:
        req = urllib.request.Request(
            url if url.startswith("http") else f"{inst}{url}",
            headers={"Authorization": f"Bearer {tok}"},
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
        all_records.extend(data.get("records", []))
        url = data.get("nextRecordsUrl")
    return all_records


def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def classify_delivery(family):
    """Classify a product family into a delivery model."""
    if family in SAAS_FAMILIES:
        return "Cloud (SaaS/XaaS)"
    elif family in ONPREM_FAMILIES:
        return "On-Premise"
    elif family in MBS_FAMILIES:
        return "Managed Services"
    elif family in SERVICES_FAMILIES:
        return "Professional Services"
    else:
        return "Other"


def create_dataset(inst, tok):
    """Build the SaaS transition dataset from OpportunityLineItem data."""
    print("  Querying OpportunityLineItem records...")
    items = run_soql(inst, tok, LINE_ITEM_SOQL)
    print(f"  → {len(items)} won line items")

    rows = []

    # Track yearly aggregates for summary rows
    yearly_delivery = defaultdict(
        lambda: defaultdict(float)
    )  # yr -> delivery -> revenue
    yearly_family = defaultdict(lambda: defaultdict(float))  # yr -> family -> revenue
    industry_delivery = defaultdict(
        lambda: defaultdict(float)
    )  # industry -> delivery -> revenue
    account_year_delivery = defaultdict(
        lambda: defaultdict(lambda: defaultdict(float))
    )  # acct -> yr -> delivery -> revenue

    for item in items:
        opp = item.get("Opportunity") or {}
        acct = opp.get("Account") or {}
        owner = opp.get("Owner") or {}
        prod = item.get("Product2") or {}

        family = prod.get("Family", "Other") or "Other"
        price = safe_float(item.get("UnitPrice"))
        qty = safe_float(item.get("Quantity")) or 1

        # Only track families we care about
        if family not in ALL_TRACKED:
            continue

        delivery = classify_delivery(family)

        close_date = opp.get("CloseDate", "")
        try:
            cd = datetime.strptime(close_date, "%Y-%m-%d")
            yr = cd.year
            qtr = (cd.month - 1) // 3 + 1
            qtr_label = f"{yr}-Q{qtr}"
        except (ValueError, TypeError):
            yr, qtr, qtr_label = 0, 0, "Unknown"

        acct_name = acct.get("Name", "Unknown")
        acct_id = acct.get("Id", "")
        industry = acct.get("Industry", "Unknown") or "Unknown"

        row = {
            "RecordType": "line_item",
            "LineItemId": item.get("Id", ""),
            "OppId": opp.get("Id", ""),
            "OppName": opp.get("Name", ""),
            "AccountId": acct_id,
            "AccountName": acct_name,
            "Industry": industry,
            "OwnerName": owner.get("Name", "Unknown"),
            "ProductFamily": family,
            "ProductName": prod.get("Name", ""),
            "DeliveryModel": delivery,
            "OppType": opp.get("Type", ""),
            "Revenue": price,
            "Quantity": qty,
            "Year": yr,
            "Quarter": qtr,
            "QuarterLabel": qtr_label,
            "CloseDate": close_date,
            # Metric columns (zeroed for detail rows)
            "SaaSRevenue": 0,
            "OnPremRevenue": 0,
            "MBSRevenue": 0,
            "ServicesRevenue": 0,
            "SaaSPct": 0,
            "OnPremPct": 0,
            "TotalRevenue": 0,
        }
        rows.append(row)

        # Accumulate aggregates
        yearly_delivery[yr][delivery] += price
        yearly_family[yr][family] += price
        industry_delivery[industry][delivery] += price
        account_year_delivery[acct_name][yr][delivery] += price

    # ── Yearly summary rows ──
    for yr in sorted(yearly_delivery.keys()):
        d = yearly_delivery[yr]
        saas = d.get("Cloud (SaaS/XaaS)", 0)
        onprem = d.get("On-Premise", 0)
        mbs = d.get("Managed Services", 0)
        services = d.get("Professional Services", 0)
        total = saas + onprem + mbs + services
        saas_pct = (saas / total * 100) if total > 0 else 0
        onprem_pct = (onprem / total * 100) if total > 0 else 0

        rows.append(
            {
                "RecordType": "yearly_summary",
                "LineItemId": "",
                "OppId": "",
                "OppName": f"FY{yr}",
                "AccountId": "",
                "AccountName": "",
                "Industry": "",
                "OwnerName": "",
                "ProductFamily": "",
                "ProductName": "",
                "DeliveryModel": "",
                "OppType": "",
                "Revenue": total,
                "Quantity": 0,
                "Year": yr,
                "Quarter": 0,
                "QuarterLabel": str(yr),
                "CloseDate": f"{yr}-12-31",
                "SaaSRevenue": round(saas, 2),
                "OnPremRevenue": round(onprem, 2),
                "MBSRevenue": round(mbs, 2),
                "ServicesRevenue": round(services, 2),
                "SaaSPct": round(saas_pct, 2),
                "OnPremPct": round(onprem_pct, 2),
                "TotalRevenue": round(total, 2),
            }
        )

    # ── Account transition rows (for account-level mix analysis) ──
    for acct_name in account_year_delivery:
        for yr in sorted(account_year_delivery[acct_name].keys()):
            d = account_year_delivery[acct_name][yr]
            saas = d.get("Cloud (SaaS/XaaS)", 0)
            onprem = d.get("On-Premise", 0)
            mbs = d.get("Managed Services", 0)
            services = d.get("Professional Services", 0)
            total = saas + onprem + mbs + services
            saas_pct = (saas / total * 100) if total > 0 else 0
            onprem_pct = (onprem / total * 100) if total > 0 else 0

            # Only include accounts with meaningful revenue
            if total < 100000:
                continue

            rows.append(
                {
                    "RecordType": "account_year",
                    "LineItemId": "",
                    "OppId": "",
                    "OppName": "",
                    "AccountId": "",
                    "AccountName": acct_name,
                    "Industry": "",
                    "OwnerName": "",
                    "ProductFamily": "",
                    "ProductName": "",
                    "DeliveryModel": "",
                    "OppType": "",
                    "Revenue": total,
                    "Quantity": 0,
                    "Year": yr,
                    "Quarter": 0,
                    "QuarterLabel": str(yr),
                    "CloseDate": f"{yr}-12-31",
                    "SaaSRevenue": round(saas, 2),
                    "OnPremRevenue": round(onprem, 2),
                    "MBSRevenue": round(mbs, 2),
                    "ServicesRevenue": round(services, 2),
                    "SaaSPct": round(saas_pct, 2),
                    "OnPremPct": round(onprem_pct, 2),
                    "TotalRevenue": round(total, 2),
                }
            )

    line_items = sum(1 for r in rows if r["RecordType"] == "line_item")
    yearly = sum(1 for r in rows if r["RecordType"] == "yearly_summary")
    acct_yr = sum(1 for r in rows if r["RecordType"] == "account_year")
    print(
        f"  → {len(rows)} total rows ({line_items} line items, {yearly} yearly, {acct_yr} account-year)"
    )

    # ── Build CSV ──
    fields = list(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8")

    fields_meta = [
        _dim("RecordType"),
        _dim("LineItemId"),
        _dim("OppId"),
        _dim("OppName"),
        _dim("AccountId"),
        _dim("AccountName"),
        _dim("Industry"),
        _dim("OwnerName"),
        _dim("ProductFamily"),
        _dim("ProductName"),
        _dim("DeliveryModel"),
        _dim("OppType"),
        _measure("Revenue"),
        _measure("Quantity", precision=18, scale=0),
        _measure("Year", precision=18, scale=0),
        _measure("Quarter", precision=18, scale=0),
        _dim("QuarterLabel"),
        _dim("CloseDate"),
        _measure("SaaSRevenue"),
        _measure("OnPremRevenue"),
        _measure("MBSRevenue"),
        _measure("ServicesRevenue"),
        _measure("SaaSPct"),
        _measure("OnPremPct"),
        _measure("TotalRevenue"),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


# ── Steps ─────────────────────────────────────────────────────────────────
def build_steps(ds_id):
    ds_meta = [{"id": ds_id, "name": DS}]
    detail = f'q = load "{DS}";\nq = filter q by RecordType == "line_item";\n'
    summary = f'q = load "{DS}";\nq = filter q by RecordType == "yearly_summary";\n'
    acct = f'q = load "{DS}";\nq = filter q by RecordType == "account_year";\n'

    # Build the summary step and apply facet scope so KPIs respond to filters
    s_summary = sq(summary + "q = order q by Year desc;\nq = limit q 1;")
    s_summary.update(KPI_FACET_SCOPE)

    return {
        # Filters
        "f_year": af("QuarterLabel", ds_meta),
        "f_industry": af("Industry", ds_meta),
        "f_account": af("AccountName", ds_meta),
        "f_family": af("ProductFamily", ds_meta),
        # ── Page 1: Transition Overview ──
        "s_latest_summary": s_summary,
        "s_yearly_trend": sq(
            summary
            + "q = foreach q generate QuarterLabel as Year, "
            + "SaaSRevenue, OnPremRevenue, MBSRevenue, ServicesRevenue, "
            + "SaaSPct, OnPremPct, TotalRevenue;\n"
            + "q = order q by Year asc;"
        ),
        "s_delivery_mix": sq(
            detail
            + "q = group q by DeliveryModel;\n"
            + "q = foreach q generate DeliveryModel, "
            + "sum(Revenue) as Revenue, count() as Items;\n"
            + "q = order q by Revenue desc;"
        ),
        "s_family_revenue": sq(
            detail
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, "
            + "sum(Revenue) as Revenue, count() as Items;\n"
            + "q = order q by Revenue desc;"
        ),
        "s_saas_qtr_trend": sq(
            detail
            + "q = group q by (QuarterLabel, DeliveryModel);\n"
            + "q = foreach q generate QuarterLabel, DeliveryModel, "
            + "sum(Revenue) as Revenue;\n"
            + "q = order q by QuarterLabel asc;"
        ),
        # ── Page 2: Industry Deep Dive ──
        "s_industry_delivery": sq(
            detail
            + "q = group q by (Industry, DeliveryModel);\n"
            + "q = foreach q generate Industry, DeliveryModel, "
            + "sum(Revenue) as Revenue;\n"
            + "q = order q by Revenue desc;"
        ),
        "s_industry_saas_pct": sq(
            detail
            + "q = group q by Industry;\n"
            + "q = foreach q generate Industry, "
            + "sum(Revenue) as TotalRevenue, "
            + 'sum(case when DeliveryModel == "Cloud (SaaS/XaaS)" then Revenue else 0 end) as SaaSRevenue;\n'
            + "q = order q by TotalRevenue desc;"
        ),
        "s_industry_trend": sq(
            detail
            + 'q = filter q by DeliveryModel == "Cloud (SaaS/XaaS)";\n'
            + "q = group q by (QuarterLabel, Industry);\n"
            + "q = foreach q generate QuarterLabel, Industry, "
            + "sum(Revenue) as SaaSRevenue;\n"
            + "q = order q by QuarterLabel asc;"
        ),
        # ── Page 3: Account Migration ──
        "s_account_mix": sq(
            acct
            + "q = filter q by Year >= 2024;\n"
            + "q = group q by AccountName;\n"
            + "q = foreach q generate AccountName, "
            + "sum(SaaSRevenue) as SaaSRevenue, "
            + "sum(OnPremRevenue) as OnPremRevenue, "
            + "sum(MBSRevenue) as MBSRevenue, "
            + "sum(TotalRevenue) as TotalRevenue;\n"
            + "q = order q by TotalRevenue desc;\n"
            + "q = limit q 30;"
        ),
        "s_saas_leaders": sq(
            acct
            + "q = filter q by Year == 2025;\n"
            + "q = foreach q generate AccountName, "
            + "SaaSRevenue, OnPremRevenue, SaaSPct, TotalRevenue;\n"
            + "q = order q by SaaSRevenue desc;\n"
            + "q = limit q 20;"
        ),
        "s_onprem_heavy": sq(
            acct
            + "q = filter q by Year >= 2024 and OnPremRevenue > 1000000;\n"
            + "q = foreach q generate AccountName, Year as FiscalYear, "
            + "OnPremRevenue, SaaSRevenue, SaaSPct, TotalRevenue;\n"
            + "q = order q by OnPremRevenue desc;\n"
            + "q = limit q 25;"
        ),
        "s_acct_total": sq(
            acct
            + "q = filter q by Year >= 2024;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate sum(SaaSRevenue) as TotalSaaS, "
            + "sum(OnPremRevenue) as TotalOnPrem, "
            + "sum(TotalRevenue) as GrandTotal, "
            + "count() as AccountYears;"
        ),
        # ── Page 4: MBS & Services ──
        "s_mbs_trend": sq(
            detail
            + 'q = filter q by DeliveryModel == "Managed Services";\n'
            + "q = group q by QuarterLabel;\n"
            + "q = foreach q generate QuarterLabel, "
            + "sum(Revenue) as MBSRevenue, count() as Items;\n"
            + "q = order q by QuarterLabel asc;"
        ),
        "s_mbs_accounts": sq(
            detail
            + 'q = filter q by DeliveryModel == "Managed Services";\n'
            + "q = group q by AccountName;\n"
            + "q = foreach q generate AccountName, "
            + "sum(Revenue) as MBSRevenue, count() as Items;\n"
            + "q = order q by MBSRevenue desc;\n"
            + "q = limit q 15;"
        ),
        "s_services_mix": sq(
            detail
            + 'q = filter q by DeliveryModel == "Professional Services";\n'
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, "
            + "sum(Revenue) as Revenue, count() as Items;\n"
            + "q = order q by Revenue desc;"
        ),
        "s_mbs_summary": sq(
            detail
            + 'q = filter q by DeliveryModel == "Managed Services" and Year >= 2024;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(Revenue) as MBSTotal, count() as Items;"
        ),
        "s_services_summary": sq(
            detail
            + 'q = filter q by DeliveryModel == "Professional Services" and Year >= 2024;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(Revenue) as ServicesTotal, count() as Items;"
        ),
    }


# ── Widgets ───────────────────────────────────────────────────────────────
def build_widgets():
    w = {}

    # ═══ Page 1: Transition Overview ═══
    w["p1_hdr"] = hdr(
        "SaaS Transition & Delivery Model",
        "Strategic shift from on-premise to cloud-native — tracking adoption, revenue mix, and migration velocity",
    )
    w["p1_n_saas_pct"] = num(
        "s_latest_summary",
        "SaaSPct",
        "Cloud Share %",
        "#0070D2",
        compact=False,
        tier="primary",
        suffix="%",
        widget_style=KPI_CARD_STYLE,
    )
    w["p1_n_saas_rev"] = num(
        "s_latest_summary",
        "SaaSRevenue",
        "Cloud Revenue",
        "#04844B",
        compact=True,
        tier="secondary",
        prefix="\u20ac",
        widget_style=KPI_CARD_STYLE,
    )
    w["p1_n_onprem_rev"] = num(
        "s_latest_summary",
        "OnPremRevenue",
        "On-Prem Revenue",
        "#D4504C",
        compact=True,
        tier="secondary",
        prefix="\u20ac",
        widget_style=KPI_CARD_STYLE,
    )
    w["p1_n_total"] = num(
        "s_latest_summary",
        "TotalRevenue",
        "Total Tracked Revenue",
        "#9050E9",
        compact=True,
        tier="secondary",
        prefix="\u20ac",
        widget_style=KPI_CARD_STYLE,
    )

    w["p1_sec_trend"] = section_label("Revenue Trend & Delivery Mix")

    w["p1_ch_trend"] = combo_chart(
        "s_yearly_trend",
        "Cloud vs On-Prem Revenue by Year",
        ["Year"],
        bar_measures=["SaaSRevenue", "OnPremRevenue", "MBSRevenue"],
        line_measures=["SaaSPct"],
        show_legend=True,
        axis_title="Revenue (\u20ac)",
        axis2_title="Cloud Share %",
        subtitle="Bar = absolute revenue by delivery model | Line = cloud share percentage \u2014 the crossover happened in 2023",
        axis1_format="\u20ac#,##0",
        axis2_format="0.0%",
        reference_lines=[{"value": 50, "label": "50% Cloud Share", "color": "#FFB75D"}],
    )
    w["p1_sec_breakdown"] = section_label("Delivery Model Breakdown")

    w["p1_ch_delivery"] = rich_chart(
        "s_delivery_mix",
        "hbar",
        "Revenue by Delivery Model (All Time)",
        ["DeliveryModel"],
        ["Revenue"],
        subtitle="Total revenue allocation across cloud, on-premise, managed services, and professional services",
    )
    w["p1_ch_family"] = rich_chart(
        "s_family_revenue",
        "hbar",
        "Revenue by Product Family",
        ["ProductFamily"],
        ["Revenue"],
        subtitle="Granular product-level view — SimCorp SaaS and XaaS are the cloud families",
    )
    w["p1_ch_qtr"] = rich_chart(
        "s_saas_qtr_trend",
        "stackcolumn",
        "Quarterly Revenue by Delivery Model",
        ["QuarterLabel", "DeliveryModel"],
        ["Revenue"],
        show_legend=True,
        axis_title="Revenue (€)",
        subtitle="Quarterly stack — watch for cloud proportion growth and on-prem decline quarter over quarter",
    )

    # ═══ Page 2: Industry Deep Dive ═══
    w["p2_hdr"] = hdr(
        "Industry SaaS Adoption",
        "Which buy-side segments are adopting cloud fastest — and where is on-prem still dominant",
    )
    w["p2_ch_industry"] = rich_chart(
        "s_industry_delivery",
        "stackcolumn",
        "Revenue by Industry & Delivery Model",
        ["Industry", "DeliveryModel"],
        ["Revenue"],
        show_legend=True,
        axis_title="Revenue (€)",
        subtitle="Stacked breakdown per industry — asset managers lead cloud adoption, insurers and pensions mixed",
    )
    w["p2_sec_detail"] = section_label("Adoption Detail & Trends")

    w["p2_ch_saas_pct"] = compare_table(
        "s_industry_saas_pct",
        "Industry Cloud Adoption",
        columns=["Industry", "SaaSRevenue", "TotalRevenue"],
        subtitle="Cloud revenue and total by industry \u2014 calculate cloud share to identify lagging segments",
        format_rules=[
            {
                "measure": "SaaSRevenue",
                "ranges": [
                    {"min": 0, "max": 500000, "color": "#D4504C"},
                    {"min": 500000, "max": 2000000, "color": "#FFB75D"},
                    {"min": 2000000, "color": "#04844B"},
                ],
            },
        ],
    )
    w["p2_ch_trend"] = line_chart(
        "s_industry_trend",
        "Cloud Revenue by Industry Over Time",
        show_legend=True,
        axis_title="Cloud Revenue (\u20ac)",
        subtitle="Quarterly cloud adoption curves by segment \u2014 steep lines = accelerating transition",
        reference_lines=[{"value": 25, "label": "25% Benchmark", "color": "#963CE9"}],
    )

    # ═══ Page 3: Account Migration ═══
    w["p3_hdr"] = hdr(
        "Account-Level Transition",
        "Individual account delivery mix — identify migration candidates and track account-level shift",
    )
    w["p3_n_saas"] = num(
        "s_acct_total",
        "TotalSaaS",
        "Cloud Revenue (2024+)",
        "#0070D2",
        compact=True,
        tier="primary",
        prefix="\u20ac",
        widget_style=KPI_CARD_STYLE,
    )
    w["p3_n_onprem"] = num(
        "s_acct_total",
        "TotalOnPrem",
        "On-Prem Revenue (2024+)",
        "#D4504C",
        compact=True,
        tier="secondary",
        prefix="\u20ac",
        widget_style=KPI_CARD_STYLE,
    )
    w["p3_n_total"] = num(
        "s_acct_total",
        "GrandTotal",
        "Total (2024+)",
        "#9050E9",
        compact=True,
        tier="secondary",
        prefix="\u20ac",
        widget_style=KPI_CARD_STYLE,
    )
    w["p3_sec_mix"] = section_label("Account Delivery Mix")

    w["p3_ch_mix"] = rich_chart(
        "s_account_mix",
        "stackcolumn",
        "Top 30 Accounts by Delivery Mix (2024+)",
        ["AccountName"],
        ["SaaSRevenue", "OnPremRevenue", "MBSRevenue"],
        show_legend=True,
        axis_title="Revenue (€)",
        subtitle="Stacked per account — pure cloud on left, heavy on-prem on right — migration targets are mixed accounts",
    )
    w["p3_sec_tables"] = section_label("Migration Candidates & Leaders")

    w["p3_ch_leaders"] = compare_table(
        "s_saas_leaders",
        "Top 20 Cloud Accounts (2025)",
        columns=[
            "AccountName",
            "SaaSRevenue",
            "OnPremRevenue",
            "SaaSPct",
            "TotalRevenue",
        ],
        subtitle="Sorted by cloud revenue \u2014 high SaaS% accounts are fully transitioned, low SaaS% are candidates",
        format_rules=[
            {
                "measure": "SaaSPct",
                "ranges": [
                    {"min": 0, "max": 25, "color": "#D4504C"},
                    {"min": 25, "max": 75, "color": "#FFB75D"},
                    {"min": 75, "color": "#04844B"},
                ],
            },
        ],
    )
    w["p3_ch_onprem"] = compare_table(
        "s_onprem_heavy",
        "On-Prem Heavy Accounts (Migration Candidates)",
        columns=[
            "AccountName",
            "FiscalYear",
            "OnPremRevenue",
            "SaaSRevenue",
            "SaaSPct",
            "TotalRevenue",
        ],
        subtitle="Accounts with >\u20ac1M on-prem \u2014 prioritize those with some SaaS (hybrid) for migration push",
        format_rules=[
            {
                "measure": "SaaSPct",
                "ranges": [
                    {"min": 0, "max": 10, "color": "#D4504C"},
                    {"min": 10, "max": 40, "color": "#FFB75D"},
                    {"min": 40, "color": "#04844B"},
                ],
            },
        ],
    )

    # ═══ Page 4: MBS & Services ═══
    w["p4_hdr"] = hdr(
        "Managed Business Services & Professional Services",
        "MBS attach and services revenue — the service layer that drives stickiness and transition support",
    )
    w["p4_n_mbs"] = num(
        "s_mbs_summary",
        "MBSTotal",
        "MBS Revenue (2024+)",
        "#0070D2",
        compact=True,
        tier="primary",
        prefix="\u20ac",
        widget_style=KPI_CARD_STYLE,
    )
    w["p4_n_services"] = num(
        "s_services_summary",
        "ServicesTotal",
        "Services Revenue (2024+)",
        "#04844B",
        compact=True,
        tier="primary",
        prefix="\u20ac",
        widget_style=KPI_CARD_STYLE,
    )
    w["p4_sec_charts"] = section_label("MBS & Services Breakdown")

    w["p4_ch_mbs_trend"] = rich_chart(
        "s_mbs_trend",
        "column",
        "MBS Revenue by Quarter",
        ["QuarterLabel"],
        ["MBSRevenue"],
        axis_title="MBS Revenue (€)",
        subtitle="Quarterly MBS bookings — growing MBS signals successful managed services strategy",
    )
    w["p4_ch_mbs_accts"] = rich_chart(
        "s_mbs_accounts",
        "hbar",
        "Top MBS Accounts",
        ["AccountName"],
        ["MBSRevenue"],
        subtitle="Which accounts are buying managed services — cross-reference with on-prem heavy for transition plays",
    )
    w["p4_ch_services"] = rich_chart(
        "s_services_mix",
        "hbar",
        "Professional Services by Family",
        ["ProductFamily"],
        ["Revenue"],
        subtitle="Services revenue distribution — consulting, analytics, operational, data, and regulatory services",
    )

    # Navigation
    pages = ["overview", "industry", "accounts", "services"]
    labels = [
        "Transition Overview",
        "Industry Deep Dive",
        "Account Migration",
        "MBS & Services",
    ]
    for pg_idx in range(4):
        for nav_idx in range(4):
            name = f"p{pg_idx + 1}_nav{nav_idx + 1}"
            w[name] = nav_link(
                pages[nav_idx], labels[nav_idx], active=(pg_idx == nav_idx)
            )

    # Filters
    w["f_year_w"] = listselector("f_year", "Period")
    w["f_industry_w"] = listselector("f_industry", "Industry")
    w["f_account_w"] = listselector("f_account", "Account")
    w["f_family_w"] = listselector("f_family", "Product Family")

    return w


# ── Layout ────────────────────────────────────────────────────────────────
def build_layout():
    filt = [
        {"name": "f_year_w", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "f_industry_w", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "f_account_w", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "f_family_w", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
    ]

    p1 = (
        nav_row("p1", 4)
        + [{"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2}]
        + filt
        + [
            {
                "name": "p1_n_saas_pct",
                "row": 5,
                "column": 0,
                "colspan": 3,
                "rowspan": 4,
            },
            {
                "name": "p1_n_saas_rev",
                "row": 5,
                "column": 3,
                "colspan": 3,
                "rowspan": 4,
            },
            {
                "name": "p1_n_onprem_rev",
                "row": 5,
                "column": 6,
                "colspan": 3,
                "rowspan": 4,
            },
            {"name": "p1_n_total", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
            {
                "name": "p1_sec_trend",
                "row": 9,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p1_ch_trend",
                "row": 10,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
            {
                "name": "p1_sec_breakdown",
                "row": 18,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p1_ch_delivery",
                "row": 19,
                "column": 0,
                "colspan": 4,
                "rowspan": 8,
            },
            {
                "name": "p1_ch_family",
                "row": 19,
                "column": 4,
                "colspan": 4,
                "rowspan": 8,
            },
            {"name": "p1_ch_qtr", "row": 19, "column": 8, "colspan": 4, "rowspan": 8},
        ]
    )

    p2 = (
        nav_row("p2", 4)
        + [{"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2}]
        + filt
        + [
            {
                "name": "p2_ch_industry",
                "row": 5,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
            {
                "name": "p2_sec_detail",
                "row": 13,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p2_ch_saas_pct",
                "row": 14,
                "column": 0,
                "colspan": 6,
                "rowspan": 10,
            },
            {
                "name": "p2_ch_trend",
                "row": 14,
                "column": 6,
                "colspan": 6,
                "rowspan": 10,
            },
        ]
    )

    p3 = (
        nav_row("p3", 4)
        + [{"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2}]
        + filt
        + [
            {"name": "p3_n_saas", "row": 5, "column": 0, "colspan": 4, "rowspan": 3},
            {"name": "p3_n_onprem", "row": 5, "column": 4, "colspan": 4, "rowspan": 3},
            {"name": "p3_n_total", "row": 5, "column": 8, "colspan": 4, "rowspan": 3},
            {"name": "p3_sec_mix", "row": 8, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p3_ch_mix", "row": 9, "column": 0, "colspan": 12, "rowspan": 8},
            {
                "name": "p3_sec_tables",
                "row": 17,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p3_ch_leaders",
                "row": 18,
                "column": 0,
                "colspan": 6,
                "rowspan": 10,
            },
            {
                "name": "p3_ch_onprem",
                "row": 18,
                "column": 6,
                "colspan": 6,
                "rowspan": 10,
            },
        ]
    )

    p4 = (
        nav_row("p4", 4)
        + [{"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2}]
        + filt
        + [
            {"name": "p4_n_mbs", "row": 5, "column": 0, "colspan": 6, "rowspan": 3},
            {
                "name": "p4_n_services",
                "row": 5,
                "column": 6,
                "colspan": 6,
                "rowspan": 3,
            },
            {
                "name": "p4_sec_charts",
                "row": 8,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p4_ch_mbs_trend",
                "row": 9,
                "column": 0,
                "colspan": 6,
                "rowspan": 8,
            },
            {
                "name": "p4_ch_mbs_accts",
                "row": 9,
                "column": 6,
                "colspan": 6,
                "rowspan": 8,
            },
            {
                "name": "p4_ch_services",
                "row": 17,
                "column": 0,
                "colspan": 12,
                "rowspan": 7,
            },
        ]
    )

    return {
        "name": "saas_transition",
        "numColumns": 12,
        "pages": [
            pg("overview", "Transition Overview", p1),
            pg("industry", "Industry Deep Dive", p2),
            pg("accounts", "Account Migration", p3),
            pg("services", "MBS & Services", p4),
        ],
    }


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Building: SaaS Transition & Delivery Model Dashboard")
    print("=" * 60)

    inst, tok = get_auth()

    print("\n[1/4] Creating dataset...")
    ok = create_dataset(inst, tok)
    if not ok:
        print("FAILED: Dataset upload failed.")
        sys.exit(1)

    print("\n[2/4] Resolving dataset ID...")
    ds_id = get_dataset_id(inst, tok, DS)
    if not ds_id:
        print("FAILED: Could not find dataset.")
        sys.exit(1)
    print(f"  Dataset ID: {ds_id}")

    print("\n[3/4] Building dashboard state...")
    steps = build_steps(ds_id)
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

    print("\n[4/4] Deploying dashboard...")
    dash_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
    deploy_dashboard(inst, tok, dash_id, state)
    print(f"  Dashboard ID: {dash_id}")

    print("\n  Setting record links...")
    set_record_links_xmd(
        inst,
        tok,
        DS,
        [
            {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
            {"field": "OppName", "id_field": "OppId", "label": "Opportunity"},
        ],
    )

    print("\n✓ SaaS Transition & Delivery Model dashboard deployed!")
    print(f"  Open: https://simcorp.lightning.force.com/analytics/dashboard/{dash_id}")


if __name__ == "__main__":
    main()
