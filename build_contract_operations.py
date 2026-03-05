#!/usr/bin/env python3
"""Build the Contract Operations KPI dashboard (AP 1.6, 1.8).

Features:
  - 4 pages: Active Book, Renewal Pipeline, Agreement Types, Fulfillment
  - Interactive nav bar across all pages
  - Global 4-filter bar (Unit Group, Agreement Type, Renewal Window, Status)
  - Dataset built from Contract SOQL with computed fields
  - Gauge, funnel, waterfall, area, donut, hbar, column, stackhbar, stackcolumn
  - Comparison tables for at-risk expirations and contract detail
"""

import csv
import io
import sys
from datetime import datetime

from crm_analytics_helpers import (
    get_auth,
    _soql,
    _dim,
    _measure,
    _date,
    upload_dataset,
    get_dataset_id,
    create_dashboard_if_needed,
    sq,
    af,
    pillbox,
    coalesce_filter,
    num,
    num_with_trend,
    trend_step,
    rich_chart,
    gauge,
    funnel_chart,
    waterfall_chart,
    hdr,
    section_label,
    nav_link,
    pg,
    nav_row,
    build_dashboard_state,
    deploy_dashboard,
    treemap_chart,
    area_chart,
    heatmap_chart,
    bullet_chart,
    sankey_chart,
    bubble_chart,
    create_dataflow,
    run_dataflow,
    set_record_links_xmd,  # noqa: F401
)

DS = "Contract_Operations"
DS_LABEL = "Contract Operations"
DASHBOARD_LABEL = "Contract Operations KPIs"

SOQL = (
    "SELECT Id, ContractNumber, AccountId, Account.Name, Status, StartDate, EndDate, "
    "ContractTerm, CreatedDate, Agreement_Type__c, "
    "Account.Unit_Group__c, Account.Risk_of_Potential_Termination__c "
    "FROM Contract WHERE CreatedDate >= 2022-01-01T00:00:00Z"
)

# ── Filter binding constants (coalesce = passthrough when nothing selected) ──
UF = coalesce_filter("f_unit", "UnitGroup")
TF = coalesce_filter("f_type", "AgreementType")
WF = coalesce_filter("f_window", "RenewalWindow")
SF = coalesce_filter("f_status", "Status")


# =========================================================================
#  Dataset creation
# =========================================================================


def _renewal_window(days_to_expiry):
    """Classify days-to-expiry into renewal window bands."""
    if days_to_expiry <= 0:
        return ""
    if days_to_expiry <= 30:
        return "0-30d"
    if days_to_expiry <= 90:
        return "31-90d"
    if days_to_expiry <= 180:
        return "91-180d"
    return "180d+"


def _term_band(term_months):
    """Classify contract term into bands for distribution charts."""
    if term_months < 12:
        return "<12mo"
    if term_months == 12:
        return "12mo"
    if term_months <= 24:
        return "24mo"
    if term_months <= 36:
        return "36mo"
    return "36mo+"


def create_dataset(inst, tok):
    """Query Contract records, compute fields, upload CSV dataset."""
    print("\n=== Building Contract Operations dataset ===")

    contracts = _soql(inst, tok, SOQL)
    print(f"  Queried {len(contracts)} contracts")

    today_str = datetime.now().strftime("%Y-%m-%d")
    today = datetime.now()

    fields = [
        "Id",
        "ContractNumber",
        "AccountId",
        "AccountName",
        "Status",
        "AgreementType",
        "UnitGroup",
        "RiskLevel",
        "IsActive",
        "StartDate",
        "EndDate",
        "CreatedDate",
        "ContractTermNum",
        "DaysToExpiry",
        "ExpiryMonth",
        "RenewalWindow",
        "CreatedMonth",
        "StartMonth",
        "TermBand",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()

    for c in contracts:
        acct = c.get("Account") or {}
        status = c.get("Status") or ""
        end_date = c.get("EndDate") or ""
        start_date = c.get("StartDate") or ""
        created_date = (c.get("CreatedDate") or "")[:10]
        contract_term = c.get("ContractTerm") or 0

        # Compute DaysToExpiry
        days_to_expiry = 0
        if end_date:
            try:
                end_dt = datetime.strptime(end_date[:10], "%Y-%m-%d")
                days_to_expiry = (end_dt - today).days
            except ValueError:
                days_to_expiry = 0

        agreement_type = c.get("Agreement_Type__c") or "Unknown"
        risk_level = acct.get("Risk_of_Potential_Termination__c") or "Low"

        writer.writerow(
            {
                "Id": c.get("Id", ""),
                "ContractNumber": c.get("ContractNumber", ""),
                "AccountId": c.get("AccountId", ""),
                "AccountName": (acct.get("Name") or "")[:255],
                "Status": status,
                "AgreementType": agreement_type,
                "UnitGroup": acct.get("Unit_Group__c") or "",
                "RiskLevel": risk_level,
                "IsActive": "true" if status == "Activated" else "false",
                "StartDate": start_date[:10] if start_date else "",
                "EndDate": end_date[:10] if end_date else "",
                "CreatedDate": created_date,
                "ContractTermNum": contract_term or 0,
                "DaysToExpiry": days_to_expiry,
                "ExpiryMonth": end_date[:7] if end_date else "",
                "RenewalWindow": _renewal_window(days_to_expiry),
                "CreatedMonth": created_date[:7] if created_date else "",
                "StartMonth": start_date[:7] if start_date else "",
                "TermBand": _term_band(contract_term or 0),
            }
        )

    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes, {len(contracts)} rows")

    fields_meta = [
        _dim("Id", "Contract ID"),
        _dim("ContractNumber", "Contract Number"),
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account Name"),
        _dim("Status", "Status"),
        _dim("AgreementType", "Agreement Type"),
        _dim("UnitGroup", "Unit Group"),
        _dim("RiskLevel", "Risk Level"),
        _dim("IsActive", "Is Active"),
        _date("StartDate", "Start Date"),
        _date("EndDate", "End Date"),
        _date("CreatedDate", "Created Date"),
        _measure("ContractTermNum", "Contract Term (Months)", scale=0, precision=6),
        _measure("DaysToExpiry", "Days to Expiry", scale=0, precision=6),
        _dim("ExpiryMonth", "Expiry Month"),
        _dim("RenewalWindow", "Renewal Window"),
        _dim("CreatedMonth", "Created Month"),
        _dim("StartMonth", "Start Month"),
        _dim("TermBand", "Term Band"),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


# =========================================================================
#  SAQL steps
# =========================================================================


def build_steps(ds_id):
    DS_META = [{"id": ds_id, "name": DS}]

    L = f'q = load "{DS}";\n'
    ACTIVE = 'q = filter q by IsActive == "true";\n'
    EXPIRING = "q = filter q by DaysToExpiry > 0;\n"
    NEXT12 = "q = filter q by DaysToExpiry > 0 && DaysToExpiry <= 365;\n"

    return {
        # ── Filter steps (aggregateflex) ──
        "f_unit": af("UnitGroup", DS_META),
        "f_type": af("AgreementType", DS_META),
        "f_window": af("RenewalWindow", DS_META),
        "f_status": af("Status", DS_META),
        # ===== PAGE 1: Active Book =====
        # Active contract count
        "s_active_count": sq(
            L
            + ACTIVE
            + UF
            + TF
            + WF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Total contract count (for active rate gauge)
        "s_total_count": sq(
            L
            + UF
            + TF
            + WF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as total;"
        ),
        # Active rate: active / total * 100
        "s_active_rate": sq(
            L
            + UF
            + TF
            + WF
            + SF
            + "q = foreach q generate "
            + '(case when IsActive == "true" then 1 else 0 end) as is_active;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(sum(is_active) / count()) * 100 as active_rate;"
        ),
        # Donut: by agreement type  (no TF — groups by AgreementType)
        "s_by_type": sq(
            L
            + ACTIVE
            + UF
            + WF
            + SF
            + "q = group q by AgreementType;\n"
            + "q = foreach q generate AgreementType, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Hbar: by UnitGroup  (no UF — groups by UnitGroup)
        "s_by_unit": sq(
            L
            + ACTIVE
            + TF
            + WF
            + SF
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Column: term distribution (using TermBand)
        "s_term_dist": sq(
            L
            + ACTIVE
            + UF
            + TF
            + WF
            + SF
            + "q = group q by TermBand;\n"
            + "q = foreach q generate TermBand, count() as cnt;\n"
            + "q = order q by TermBand asc;"
        ),
        # ===== PAGE 2: Renewal Pipeline =====
        # Area: expiring by month (next 12 months)
        "s_expiry_month": sq(
            L
            + NEXT12
            + UF
            + TF
            + WF
            + SF
            + "q = group q by ExpiryMonth;\n"
            + "q = foreach q generate ExpiryMonth, count() as cnt;\n"
            + "q = order q by ExpiryMonth asc;"
        ),
        # Funnel: by renewal window band  (no WF — groups by RenewalWindow)
        "s_renewal_funnel": sq(
            L
            + EXPIRING
            + 'q = filter q by RenewalWindow != "";\n'
            + UF
            + TF
            + SF
            + "q = group q by RenewalWindow;\n"
            + "q = foreach q generate RenewalWindow, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Comparisontable: at-risk expirations (High/Medium risk, <=90 days)
        "s_at_risk": sq(
            L
            + "q = filter q by DaysToExpiry > 0 && DaysToExpiry <= 90;\n"
            + 'q = filter q by RiskLevel in ["High", "Medium"];\n'
            + UF
            + TF
            + WF
            + SF
            + "q = foreach q generate Id, ContractNumber, AccountName, "
            + "AgreementType, RiskLevel, DaysToExpiry, ExpiryMonth;\n"
            + "q = order q by DaysToExpiry asc;\n"
            + "q = limit q 25;"
        ),
        # Gauge: renewal coverage (% with DaysToExpiry > 90)
        "s_renewal_coverage": sq(
            L
            + EXPIRING
            + UF
            + TF
            + WF
            + SF
            + "q = foreach q generate "
            + "(case when DaysToExpiry > 90 then 1 else 0 end) as has_coverage;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(sum(has_coverage) / count()) * 100 as coverage_pct;"
        ),
        # ===== PAGE 3: Agreement Types =====
        # Donut: type distribution  (no TF — groups by AgreementType)
        "s_type_dist": sq(
            L
            + UF
            + WF
            + SF
            + "q = group q by AgreementType;\n"
            + "q = foreach q generate AgreementType, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Stackcolumn: monthly new by type  (no TF — groups by AgreementType)
        "s_monthly_new_type": sq(
            L
            + 'q = filter q by CreatedMonth >= "2024-01";\n'
            + UF
            + WF
            + SF
            + "q = group q by (CreatedMonth, AgreementType);\n"
            + "q = foreach q generate CreatedMonth, AgreementType, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Number: legacy agreements count
        "s_legacy_count": sq(
            L
            + 'q = filter q by AgreementType == "Legacy";\n'
            + UF
            + TF
            + WF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Waterfall: contract status transitions (new month over month)
        "s_status_waterfall": sq(
            L
            + 'q = filter q by CreatedMonth >= "2024-01";\n'
            + UF
            + TF
            + WF
            + SF
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # ===== PAGE 4: Fulfillment =====
        # Stackhbar: status distribution stacked by agreement type
        "s_status_type": sq(
            L
            + UF
            + TF
            + WF
            + SF
            + "q = group q by (Status, AgreementType);\n"
            + "q = foreach q generate Status, AgreementType, count() as cnt;\n"
            + "q = order q by Status asc;"
        ),
        # Comparisontable: contract detail (top 50 by DaysToExpiry ascending)
        "s_contract_detail": sq(
            L
            + "q = filter q by DaysToExpiry > 0;\n"
            + UF
            + TF
            + WF
            + SF
            + "q = foreach q generate Id, ContractNumber, AccountName, "
            + "Status, AgreementType, UnitGroup, RiskLevel, "
            + "DaysToExpiry, ExpiryMonth, ContractTermNum;\n"
            + "q = order q by DaysToExpiry asc;\n"
            + "q = limit q 50;"
        ),
        # ===== Trend steps (YoY by CreatedDate) =====
        "s_active_trend": trend_step(
            DS,
            base_filters=('q = filter q by IsActive == "true";\n' + UF + TF + WF + SF),
            current_filter='q = filter q by CreatedDate_Year == "2026";\n',
            prior_filter='q = filter q by CreatedDate_Year == "2025";\n',
            group_field="all",
            measure_expr="count()",
            measure_alias="cnt",
        ),
        "s_legacy_trend": trend_step(
            DS,
            base_filters=(
                'q = filter q by AgreementType == "Legacy";\n' + UF + TF + WF + SF
            ),
            current_filter='q = filter q by CreatedDate_Year == "2026";\n',
            prior_filter='q = filter q by CreatedDate_Year == "2025";\n',
            group_field="all",
            measure_expr="count()",
            measure_alias="cnt",
        ),
        # ═══ ITERATION 3: Missing viz types + Contract Aging (Additive CRO) ═══
        # LINE: Monthly activation trend (contracts activated by month)
        "s_monthly_activation": sq(
            L
            + ACTIVE
            + 'q = filter q by StartMonth != "";\n'
            + UF
            + TF
            + WF
            + SF
            + "q = group q by StartMonth;\n"
            + "q = foreach q generate StartMonth, count() as cnt;\n"
            + "q = order q by StartMonth asc;"
        ),
        # NUM: Total contract count
        "s_total_num": sq(
            L
            + UF
            + TF
            + WF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as total;"
        ),
        # NUM: Average contract term in months
        "s_avg_term": sq(
            L
            + ACTIVE
            + UF
            + TF
            + WF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate avg(ContractTermNum) as avg_term;"
        ),
        # NUM: Contracts expiring in next 90 days
        "s_expiring_90d": sq(
            L
            + "q = filter q by DaysToExpiry > 0 && DaysToExpiry <= 90;\n"
            + UF
            + TF
            + WF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # COMBO: New contracts vs expirations by month (additive CRO: contract lifecycle)
        "s_new_vs_expiry": sq(
            L
            + UF
            + TF
            + WF
            + SF
            + 'q = filter q by CreatedMonth >= "2024-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as new_cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # HBAR: Contract Aging — vintage year cohorts (Additive CRO #5)
        "s_contract_aging": sq(
            L
            + ACTIVE
            + UF
            + TF
            + WF
            + SF
            + 'q = filter q by StartDate != "";\n'
            + "q = foreach q generate substr(StartDate, 1, 4) as VintageYear, "
            + "DaysToExpiry, ContractTermNum;\n"
            + "q = group q by VintageYear;\n"
            + "q = foreach q generate VintageYear, count() as cnt, "
            + "avg(ContractTermNum) as avg_term, avg(DaysToExpiry) as avg_days_left;\n"
            + "q = order q by VintageYear asc;"
        ),
        # STACKHBAR: Aging cohort by risk level
        "s_aging_risk": sq(
            L
            + ACTIVE
            + UF
            + TF
            + WF
            + SF
            + 'q = filter q by StartDate != "";\n'
            + "q = foreach q generate substr(StartDate, 1, 4) as VintageYear, RiskLevel;\n"
            + "q = group q by (VintageYear, RiskLevel);\n"
            + "q = foreach q generate VintageYear, RiskLevel, count() as cnt;\n"
            + "q = order q by VintageYear asc;"
        ),
        # ═══ V2: Advanced Visualizations ═══
        # Treemap: Active contracts by AgreementType → Status
        "s_treemap_portfolio": sq(
            L
            + ACTIVE
            + UF
            + WF
            + 'q = filter q by AgreementType != "";\n'
            + "q = group q by (AgreementType, Status);\n"
            + "q = foreach q generate AgreementType, Status, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Area: Monthly contract expiry horizon (next 12 months)
        "s_area_expiry": sq(
            L
            + ACTIVE
            + UF
            + TF
            + SF
            + "q = filter q by DaysToExpiry > 0 && DaysToExpiry <= 365;\n"
            + 'q = filter q by EndDate != "";\n'
            + "q = foreach q generate substr(EndDate, 1, 7) as ExpiryMonth, "
            + "DaysToExpiry, ContractTermNum;\n"
            + "q = group q by ExpiryMonth;\n"
            + "q = foreach q generate ExpiryMonth, count() as cnt, "
            + "avg(ContractTermNum) as avg_term;\n"
            + "q = order q by ExpiryMonth asc;"
        ),
        # Heatmap: Agreement Type × Renewal Window
        "s_heatmap_type_window": sq(
            L
            + ACTIVE
            + UF
            + SF
            + 'q = filter q by AgreementType != "" && RenewalWindow != "";\n'
            + "q = group q by (AgreementType, RenewalWindow);\n"
            + "q = foreach q generate AgreementType, RenewalWindow, count() as cnt;\n"
            + "q = order q by AgreementType asc;"
        ),
        # ═══ V2 Phase 6: Bullet Chart ═══
        "s_bullet_cycle": sq(
            L
            + ACTIVE
            + UF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate avg(ContractTermNum) as avg_term, 24 as target;"
        ),
        # ═══ V2 Phase 8: Statistical Analysis ═══
        "s_stat_term_dist": sq(
            L
            + ACTIVE
            + UF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "min(ContractTermNum) as min_term, "
            + "percentile_disc(0.25) within group (order by ContractTermNum) as p25, "
            + "percentile_disc(0.50) within group (order by ContractTermNum) as median_term, "
            + "percentile_disc(0.75) within group (order by ContractTermNum) as p75, "
            + "max(ContractTermNum) as max_term, "
            + "avg(ContractTermNum) as mean_term, stddev(ContractTermNum) as std_dev, "
            + "count() as contract_count;"
        ),
        "s_stat_type_summary": sq(
            L
            + ACTIVE
            + UF
            + SF
            + 'q = filter q by AgreementType != "";\n'
            + "q = group q by AgreementType;\n"
            + "q = foreach q generate AgreementType, count() as cnt, "
            + "avg(ContractTermNum) as avg_term, avg(DaysToExpiry) as avg_days_expiry;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══ V2 Phase 10: Sankey ═══
        "s_sankey_type_status": sq(
            L
            + UF
            + TF
            + WF
            + SF
            + "q = group q by (AgreementType, Status);\n"
            + "q = foreach q generate AgreementType, Status, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══ V2 Phase 10: Bubble ═══
        "s_bubble_contracts": sq(
            L
            + ACTIVE
            + UF
            + TF
            + WF
            + SF
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "count() as cnt, "
            + "avg(ContractTermNum) as avg_term, "
            + "avg(DaysToExpiry) as avg_expiry;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══ V2 Gap Fill: Running total ═══
        "s_running_contracts": sq(
            L
            + UF
            + TF
            + WF
            + SF
            + 'q = filter q by StartMonth != "";\n'
            + "q = group q by StartMonth;\n"
            + "q = foreach q generate StartMonth, count() as monthly_new;\n"
            + "q = order q by StartMonth asc;\n"
            + "q = foreach q generate StartMonth, monthly_new, "
            + "sum(monthly_new) over (order by StartMonth "
            + "rows unbounded preceding) as cumul_contracts;"
        ),
    }


# =========================================================================
#  Widgets
# =========================================================================


def build_widgets():
    w = {
        # ===== PAGE 1: Active Book =====
        "p1_nav1": nav_link("active_book", "Active Book", active=True),
        "p1_nav2": nav_link("renewal", "Renewal Pipeline"),
        "p1_nav3": nav_link("agreements", "Agreements"),
        "p1_nav4": nav_link("fulfillment", "Fulfillment"),
        "p1_nav5": nav_link("aging", "Contract Aging"),
        "p1_hdr": hdr(
            "Active Book",
            "Contract Operations | Active contracts, types, terms",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_type": pillbox("f_type", "Agreement Type"),
        "p1_f_window": pillbox("f_window", "Renewal Window"),
        "p1_f_status": pillbox("f_status", "Status"),
        "p1_active_cnt": num_with_trend(
            "s_active_trend",
            "cnt",
            "Active Contracts",
            "#04844B",
            compact=False,
            size=28,
        ),
        "p1_ch_type": rich_chart(
            "s_by_type",
            "donut",
            "Active Contracts by Agreement Type",
            ["AgreementType"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        "p1_ch_unit": rich_chart(
            "s_by_unit",
            "hbar",
            "Active Contracts by Unit Group",
            ["UnitGroup"],
            ["cnt"],
            axis_title="Count",
        ),
        "p1_ch_term": rich_chart(
            "s_term_dist",
            "column",
            "Term Distribution",
            ["TermBand"],
            ["cnt"],
            axis_title="Count",
        ),
        "p1_gauge": gauge(
            "s_active_rate",
            "active_rate",
            "Active Rate (%)",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 60, "color": "#D4504C"},
                {"start": 60, "stop": 80, "color": "#FFB75D"},
                {"start": 80, "stop": 100, "color": "#04844B"},
            ],
        ),
        # ===== PAGE 2: Renewal Pipeline =====
        "p2_nav1": nav_link("active_book", "Active Book"),
        "p2_nav2": nav_link("renewal", "Renewal Pipeline", active=True),
        "p2_nav3": nav_link("agreements", "Agreements"),
        "p2_nav4": nav_link("fulfillment", "Fulfillment"),
        "p2_nav5": nav_link("aging", "Contract Aging"),
        "p2_hdr": hdr(
            "Renewal Pipeline",
            "Contract Operations | Upcoming expirations & risk",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_type": pillbox("f_type", "Agreement Type"),
        "p2_f_window": pillbox("f_window", "Renewal Window"),
        "p2_f_status": pillbox("f_status", "Status"),
        "p2_sec_expiry": section_label("Expiring Contracts (Next 12 Months)"),
        "p2_ch_expiry": rich_chart(
            "s_expiry_month",
            "area",
            "Contracts Expiring by Month",
            ["ExpiryMonth"],
            ["cnt"],
            axis_title="Count",
        ),
        "p2_ch_funnel": funnel_chart(
            "s_renewal_funnel",
            "Renewal Window Distribution",
            "RenewalWindow",
            "cnt",
        ),
        "p2_sec_risk": section_label("At-Risk Expirations (High/Medium, <=90 Days)"),
        "p2_ch_at_risk": rich_chart(
            "s_at_risk",
            "comparisontable",
            "At-Risk Contracts Expiring Soon",
            ["ContractNumber", "AccountName", "AgreementType", "RiskLevel"],
            ["DaysToExpiry"],
        ),
        "p2_gauge": gauge(
            "s_renewal_coverage",
            "coverage_pct",
            "Renewal Coverage (% with >90d Runway)",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 70, "color": "#D4504C"},
                {"start": 70, "stop": 90, "color": "#FFB75D"},
                {"start": 90, "stop": 100, "color": "#04844B"},
            ],
        ),
        # ===== PAGE 3: Agreement Types =====
        "p3_nav1": nav_link("active_book", "Active Book"),
        "p3_nav2": nav_link("renewal", "Renewal Pipeline"),
        "p3_nav3": nav_link("agreements", "Agreements", active=True),
        "p3_nav4": nav_link("fulfillment", "Fulfillment"),
        "p3_nav5": nav_link("aging", "Contract Aging"),
        "p3_hdr": hdr(
            "Agreement Types",
            "Contract Operations | Type distribution, trends & transitions",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_type": pillbox("f_type", "Agreement Type"),
        "p3_f_window": pillbox("f_window", "Renewal Window"),
        "p3_f_status": pillbox("f_status", "Status"),
        "p3_ch_dist": rich_chart(
            "s_type_dist",
            "donut",
            "Contract Type Distribution",
            ["AgreementType"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        "p3_ch_monthly": rich_chart(
            "s_monthly_new_type",
            "stackcolumn",
            "Monthly New Contracts by Type",
            ["CreatedMonth"],
            ["cnt"],
            split=["AgreementType"],
            show_legend=True,
            axis_title="Count",
        ),
        "p3_legacy_cnt": num_with_trend(
            "s_legacy_trend",
            "cnt",
            "Legacy Agreements",
            "#D4504C",
            compact=False,
            size=28,
        ),
        "p3_ch_waterfall": waterfall_chart(
            "s_status_waterfall",
            "New Contracts Month over Month",
            "CreatedMonth",
            "cnt",
            axis_label="Count",
        ),
        # ===== PAGE 4: Fulfillment =====
        "p4_nav1": nav_link("active_book", "Active Book"),
        "p4_nav2": nav_link("renewal", "Renewal Pipeline"),
        "p4_nav3": nav_link("agreements", "Agreements"),
        "p4_nav4": nav_link("fulfillment", "Fulfillment", active=True),
        "p4_nav5": nav_link("aging", "Contract Aging"),
        "p4_hdr": hdr(
            "Fulfillment",
            "Contract Operations | Status breakdown & contract detail",
        ),
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_type": pillbox("f_type", "Agreement Type"),
        "p4_f_window": pillbox("f_window", "Renewal Window"),
        "p4_f_status": pillbox("f_status", "Status"),
        "p4_ch_status": rich_chart(
            "s_status_type",
            "stackhbar",
            "Status Distribution by Agreement Type",
            ["Status"],
            ["cnt"],
            split=["AgreementType"],
            show_legend=True,
            axis_title="Count",
        ),
        "p4_sec_detail": section_label("Contract Detail (Most Urgent First)"),
        "p4_ch_detail": rich_chart(
            "s_contract_detail",
            "comparisontable",
            "Contract Detail - Top 50 by Days to Expiry",
            [
                "ContractNumber",
                "AccountName",
                "Status",
                "AgreementType",
                "UnitGroup",
                "RiskLevel",
            ],
            ["DaysToExpiry", "ContractTermNum"],
        ),
    }

    # ═══ ITERATION 3: New widgets (line, combo, num, aging) ═══
    # Page 1 — Hero KPI nums
    w["p1_n_total"] = num("s_total_num", "total", "Total Contracts", "#2A2F3A")
    w["p1_n_avg_term"] = num("s_avg_term", "avg_term", "Avg Term (Months)", "#0070D2")
    # Page 2 — Expiring 90d num + line chart
    w["p2_n_expiring_90"] = num(
        "s_expiring_90d", "cnt", "Expiring Next 90 Days", "#D4504C"
    )
    # Page 3 — Activation trend (LINE — fills missing viz type)
    w["p3_sec_activation"] = section_label("Monthly Contract Activations")
    w["p3_ch_activation"] = rich_chart(
        "s_monthly_activation",
        "line",
        "Contracts Activated by Month",
        ["StartMonth"],
        ["cnt"],
        axis_title="Count",
    )
    # Page 4 — New vs Expiry combo (COMBO — fills missing viz type)
    w["p4_sec_lifecycle"] = section_label("Contract Lifecycle Analysis")
    w["p4_ch_new_vs_expiry"] = rich_chart(
        "s_new_vs_expiry",
        "combo",
        "New Contracts by Month",
        ["CreatedMonth"],
        ["new_cnt"],
        axis_title="Count",
    )
    # Page 5 (NEW) — Contract Aging (Additive CRO #5)
    w["p5_nav1"] = nav_link("active_book", "Active Book")
    w["p5_nav2"] = nav_link("renewal", "Renewal Pipeline")
    w["p5_nav3"] = nav_link("agreements", "Agreements")
    w["p5_nav4"] = nav_link("fulfillment", "Fulfillment")
    w["p5_nav5"] = nav_link("aging", "Contract Aging", active=True)
    w["p5_hdr"] = hdr(
        "Contract Aging",
        "Contract Operations | Vintage cohort analysis & risk by age",
    )
    w["p5_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p5_f_type"] = pillbox("f_type", "Agreement Type")
    w["p5_f_window"] = pillbox("f_window", "Renewal Window")
    w["p5_f_status"] = pillbox("f_status", "Status")
    w["p5_sec_vintage"] = section_label("Contract Vintage Cohorts")
    w["p5_ch_aging"] = rich_chart(
        "s_contract_aging",
        "column",
        "Active Contracts by Vintage Year",
        ["VintageYear"],
        ["cnt"],
        axis_title="Count",
    )
    w["p5_ch_aging_term"] = rich_chart(
        "s_contract_aging",
        "hbar",
        "Avg Contract Term by Vintage Year",
        ["VintageYear"],
        ["avg_term"],
        axis_title="Months",
    )
    w["p5_ch_aging_risk"] = rich_chart(
        "s_aging_risk",
        "stackhbar",
        "Vintage Cohort by Risk Level",
        ["VintageYear"],
        ["cnt"],
        split=["RiskLevel"],
        show_legend=True,
        axis_title="Count",
    )

    # ── Phase 6: Reference lines ──────────────────────────────────────────
    from crm_analytics_helpers import add_reference_line

    add_reference_line(w["p2_ch_expiry"], 5, "Avg Monthly", "#D4504C", "dashed")

    # ── Phase 7: Embedded table actions ──────────────────────────────────
    from crm_analytics_helpers import add_table_action

    add_table_action(w["p2_ch_at_risk"], "salesforceActions", "Contract", "Id")
    add_table_action(w["p4_ch_detail"], "salesforceActions", "Contract", "Id")

    # ═══ V2 PAGE 6: Advanced Analytics ═══
    w["p6_nav1"] = nav_link("overview", "Active Book")
    w["p6_nav2"] = nav_link("renewals", "Renewals")
    w["p6_nav3"] = nav_link("types", "Types")
    w["p6_nav4"] = nav_link("fulfillment", "Fulfillment")
    w["p6_nav5"] = nav_link("aging", "Aging")
    w["p6_nav6"] = nav_link("advanalytics", "Advanced", active=True)
    w["p6_hdr"] = hdr(
        "Advanced Analytics",
        "Portfolio Composition | Expiry Horizon | Type × Window Matrix",
    )
    w["p6_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p6_f_type"] = pillbox("f_type", "Agreement Type")
    w["p6_f_window"] = pillbox("f_window", "Renewal Window")
    w["p6_f_status"] = pillbox("f_status", "Status")
    # Treemap: Portfolio composition
    w["p6_sec_treemap"] = section_label("Contract Portfolio Composition")
    w["p6_ch_treemap"] = treemap_chart(
        "s_treemap_portfolio",
        "Active Contracts by Type & Status",
        ["AgreementType", "Status"],
        "cnt",
    )
    # Area: Expiry horizon
    w["p6_sec_area"] = section_label("Contract Expiry Horizon")
    w["p6_ch_area"] = area_chart(
        "s_area_expiry",
        "Contracts Expiring Over Next 12 Months",
        stacked=False,
        show_legend=False,
    )
    # Heatmap: Type × Window
    w["p6_sec_heatmap"] = section_label("Renewal Window Matrix")
    w["p6_ch_heatmap"] = heatmap_chart(
        "s_heatmap_type_window", "Contracts by Type × Renewal Window"
    )
    # Sankey: Agreement Type → Status
    w["p6_sec_sankey"] = section_label("Contract Flow: Type → Status")
    w["p6_ch_sankey"] = sankey_chart(
        "s_sankey_type_status", "Agreement Type → Contract Status"
    )
    # Bubble: Unit Group contract profile
    w["p6_sec_bubble"] = section_label("Contract Profile by Unit Group")
    w["p6_ch_bubble"] = bubble_chart(
        "s_bubble_contracts", "Contracts: Count vs Avg Term vs Avg Days to Expiry"
    )

    # ═══ V2 PAGE 7: Bullet Charts & Statistical Analysis ═══
    w["p7_nav1"] = nav_link("overview", "Active Book")
    w["p7_nav2"] = nav_link("renewals", "Renewals")
    w["p7_nav3"] = nav_link("types", "Types")
    w["p7_nav4"] = nav_link("fulfillment", "Fulfillment")
    w["p7_nav5"] = nav_link("aging", "Aging")
    w["p7_nav6"] = nav_link("advanalytics", "Advanced")
    w["p7_nav7"] = nav_link("contractstats", "Statistics", active=True)
    w["p7_hdr"] = hdr(
        "Contract Statistical Analysis",
        "Contract Cycle Target | Term Distribution | Type Summary",
    )
    w["p7_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p7_f_type"] = pillbox("f_type", "Agreement Type")
    w["p7_f_window"] = pillbox("f_window", "Renewal Window")
    w["p7_f_status"] = pillbox("f_status", "Status")
    # Bullet: Avg contract cycle
    w["p7_sec_bullet"] = section_label("Contract Cycle Target")
    w["p7_bullet_cycle"] = bullet_chart(
        "s_bullet_cycle", "Avg Term Months (Target: 24)", axis_title="Months"
    )
    # Stats: Term distribution
    w["p7_sec_term_dist"] = section_label("Contract Term Distribution")
    w["p7_stat_term_dist"] = rich_chart(
        "s_stat_term_dist",
        "comparisontable",
        "Contract Term Percentiles (P25/Median/P75/Max)",
        [],
        [
            "min_term",
            "p25",
            "median_term",
            "p75",
            "max_term",
            "mean_term",
            "std_dev",
            "contract_count",
        ],
    )
    # Stats: Type summary
    w["p7_sec_type_summary"] = section_label("Agreement Type Summary")
    w["p7_stat_type_summary"] = rich_chart(
        "s_stat_type_summary",
        "comparisontable",
        "Contract Count, Avg Term & Days to Expiry by Type",
        ["AgreementType"],
        ["cnt", "avg_term", "avg_days_expiry"],
    )
    # Cumulative contracts running total
    w["p7_sec_running"] = section_label("Cumulative Contracts Over Time")
    w["p7_ch_running"] = area_chart(
        "s_running_contracts",
        "Cumulative New Contracts by Month",
        axis_title="Contracts",
    )

    # Add nav6 (Advanced) to pages 1-5
    for px in range(1, 6):
        w[f"p{px}_nav6"] = nav_link("advanalytics", "Advanced")
    # Add nav7 (Statistics) to pages 1-6
    for px in range(1, 7):
        w[f"p{px}_nav7"] = nav_link("contractstats", "Statistics")

    return w


# =========================================================================
#  Layout
# =========================================================================


def _filter_row(prefix):
    """Return 4 filter-bar layout entries at row 3 for the given page prefix."""
    return [
        {"name": f"{prefix}_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": f"{prefix}_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {
            "name": f"{prefix}_f_window",
            "row": 3,
            "column": 6,
            "colspan": 3,
            "rowspan": 2,
        },
        {
            "name": f"{prefix}_f_status",
            "row": 3,
            "column": 9,
            "colspan": 3,
            "rowspan": 2,
        },
    ]


def build_layout():
    # Page 1: Active Book  (content rows shifted +2 to make room for filter bar)
    p1 = (
        nav_row("p1", 7)
        + [
            {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        ]
        + _filter_row("p1")
        + [
            # Hero KPI
            {
                "name": "p1_active_cnt",
                "row": 5,
                "column": 0,
                "colspan": 3,
                "rowspan": 4,
            },
            {"name": "p1_gauge", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
            # Donut: agreement type
            {"name": "p1_ch_type", "row": 5, "column": 6, "colspan": 6, "rowspan": 8},
            # Hbar: unit group
            {"name": "p1_ch_unit", "row": 9, "column": 0, "colspan": 6, "rowspan": 8},
            # Column: term distribution
            {"name": "p1_ch_term", "row": 13, "column": 6, "colspan": 6, "rowspan": 8},
            # Iteration 3: num tiles
            {"name": "p1_n_total", "row": 17, "column": 0, "colspan": 3, "rowspan": 4},
            {
                "name": "p1_n_avg_term",
                "row": 17,
                "column": 3,
                "colspan": 3,
                "rowspan": 4,
            },
        ]
    )

    # Page 2: Renewal Pipeline
    p2 = (
        nav_row("p2", 7)
        + [
            {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        ]
        + _filter_row("p2")
        + [
            {
                "name": "p2_sec_expiry",
                "row": 5,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            # Area: expiring by month
            {"name": "p2_ch_expiry", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
            # Funnel: renewal window
            {"name": "p2_ch_funnel", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
            # Gauge: renewal coverage
            {"name": "p2_gauge", "row": 14, "column": 0, "colspan": 4, "rowspan": 5},
            # Iteration 3: expiring 90d num
            {
                "name": "p2_n_expiring_90",
                "row": 19,
                "column": 0,
                "colspan": 4,
                "rowspan": 4,
            },
            # At-risk section
            {"name": "p2_sec_risk", "row": 14, "column": 4, "colspan": 8, "rowspan": 1},
            {
                "name": "p2_ch_at_risk",
                "row": 15,
                "column": 4,
                "colspan": 8,
                "rowspan": 10,
            },
        ]
    )

    # Page 3: Agreement Types
    p3 = (
        nav_row("p3", 7)
        + [
            {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        ]
        + _filter_row("p3")
        + [
            # Donut + Legacy count side by side
            {"name": "p3_ch_dist", "row": 5, "column": 0, "colspan": 6, "rowspan": 8},
            {
                "name": "p3_legacy_cnt",
                "row": 5,
                "column": 6,
                "colspan": 6,
                "rowspan": 4,
            },
            # Stackcolumn: monthly new by type
            {
                "name": "p3_ch_monthly",
                "row": 9,
                "column": 6,
                "colspan": 6,
                "rowspan": 8,
            },
            # Waterfall: status transitions
            {
                "name": "p3_ch_waterfall",
                "row": 13,
                "column": 0,
                "colspan": 6,
                "rowspan": 8,
            },
            # Iteration 3: activation line chart
            {
                "name": "p3_sec_activation",
                "row": 21,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p3_ch_activation",
                "row": 22,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
        ]
    )

    # Page 4: Fulfillment
    p4 = (
        nav_row("p4", 7)
        + [
            {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        ]
        + _filter_row("p4")
        + [
            # Stackhbar: status by agreement type
            {
                "name": "p4_ch_status",
                "row": 5,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
            # Contract detail table
            {
                "name": "p4_sec_detail",
                "row": 13,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p4_ch_detail",
                "row": 14,
                "column": 0,
                "colspan": 12,
                "rowspan": 12,
            },
            # Iteration 3: lifecycle combo chart
            {
                "name": "p4_sec_lifecycle",
                "row": 26,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p4_ch_new_vs_expiry",
                "row": 27,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
        ]
    )

    # Page 5: Contract Aging (Additive CRO #5)
    p5 = (
        nav_row("p5", 7)
        + [
            {"name": "p5_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        ]
        + _filter_row("p5")
        + [
            {
                "name": "p5_sec_vintage",
                "row": 5,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            # Column: vintage cohort counts
            {"name": "p5_ch_aging", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
            # Hbar: avg term by vintage
            {
                "name": "p5_ch_aging_term",
                "row": 6,
                "column": 6,
                "colspan": 6,
                "rowspan": 8,
            },
            # Stackhbar: vintage by risk level
            {
                "name": "p5_ch_aging_risk",
                "row": 14,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
        ]
    )

    p6 = nav_row("p6", 7) + [
        {"name": "p6_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p6_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_window", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_status", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Treemap
        {"name": "p6_sec_treemap", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_treemap", "row": 6, "column": 0, "colspan": 12, "rowspan": 10},
        # Area
        {"name": "p6_sec_area", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_area", "row": 17, "column": 0, "colspan": 12, "rowspan": 8},
        # Heatmap
        {"name": "p6_sec_heatmap", "row": 25, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_heatmap", "row": 26, "column": 0, "colspan": 12, "rowspan": 10},
        # Sankey: Type → Status
        {"name": "p6_sec_sankey", "row": 36, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_sankey", "row": 37, "column": 0, "colspan": 12, "rowspan": 10},
        # Bubble: Unit Group contracts
        {"name": "p6_sec_bubble", "row": 47, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_bubble", "row": 48, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    p7 = nav_row("p7", 7) + [
        {"name": "p7_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p7_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_window", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_status", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Bullet
        {"name": "p7_sec_bullet", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_bullet_cycle", "row": 6, "column": 0, "colspan": 12, "rowspan": 5},
        # Term distribution
        {
            "name": "p7_sec_term_dist",
            "row": 11,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p7_stat_term_dist",
            "row": 12,
            "column": 0,
            "colspan": 12,
            "rowspan": 5,
        },
        # Type summary
        {
            "name": "p7_sec_type_summary",
            "row": 17,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p7_stat_type_summary",
            "row": 18,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Cumulative contracts
        {"name": "p7_sec_running", "row": 26, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_running", "row": 27, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("active_book", "Active Book", p1),
            pg("renewal", "Renewal Pipeline", p2),
            pg("agreements", "Agreements", p3),
            pg("fulfillment", "Fulfillment", p4),
            pg("aging", "Contract Aging", p5),
            pg("advanalytics", "Advanced Analytics", p6),
            pg("contractstats", "Statistical Analysis", p7),
        ],
    }


# =========================================================================
#  Main
# =========================================================================


def create_dataflow_definition():
    """Return a CRM Analytics dataflow definition for Contract_Operations."""
    return {
        "Extract_Contracts": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Contract",
                "fields": [
                    {"name": "Id"},
                    {"name": "ContractNumber"},
                    {"name": "AccountId"},
                    {"name": "Status"},
                    {"name": "StartDate"},
                    {"name": "EndDate"},
                    {"name": "ContractTerm"},
                    {"name": "CreatedDate"},
                    {"name": "Agreement_Type__c"},
                ],
            },
        },
        "Extract_Accounts": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Account",
                "fields": [{"name": "Id"}, {"name": "Name"}],
            },
        },
        "Augment_Account": {
            "action": "augment",
            "parameters": {
                "left": "Extract_Contracts",
                "left_key": ["AccountId"],
                "relationship": "Account",
                "right": "Extract_Accounts",
                "right_key": ["Id"],
                "right_select": ["Name"],
            },
        },
        "Register_Dataset": {
            "action": "sfdcRegister",
            "parameters": {
                "source": "Augment_Account",
                "name": "Contract_Operations",
                "alias": "Contract_Operations",
                "label": "Contract Operations",
            },
        },
    }


def main():
    instance_url, token = get_auth()

    if "--create-dataflow" in sys.argv:
        print("\n=== Creating/updating dataflow ===")
        df_def = create_dataflow_definition()
        df_id = create_dataflow(instance_url, token, "DF_Contract_Operations", df_def)
        if df_id and "--run-dataflow" in sys.argv:
            run_dataflow(instance_url, token, df_id)
        return

    # 1. Build and upload dataset
    ds_ok = create_dataset(instance_url, token)
    if not ds_ok:
        print("ERROR: Dataset upload failed - aborting")
        return

    # Set record navigation links via XMD
    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "ContractNumber", "sobject": "Contract", "id_field": "Id"},
            {"field": "AccountName", "sobject": "Account", "id_field": "AccountId"},
        ],
    )

    # 2. Look up dataset ID (needed for filter steps)
    ds_id = get_dataset_id(instance_url, token, DS)
    if ds_id:
        print(f"  Dataset ID: {ds_id}")
    else:
        print("WARNING: Could not look up dataset ID - dashboard may still deploy")

    # 3. Create or find dashboard
    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)

    # 4. Build and deploy
    steps = build_steps(ds_id)
    widgets = build_widgets()
    layout = build_layout()
    state = build_dashboard_state(steps, widgets, layout)
    deploy_dashboard(instance_url, token, dashboard_id, state)


if __name__ == "__main__":
    main()
