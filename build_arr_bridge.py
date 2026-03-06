#!/usr/bin/env python3
"""Build the ARR Bridge & Growth Analytics dashboard.

Creates the Customer_ARR_Bridge dataset from Opportunity + Account data,
computing monthly ARR movements (New, Expansion, Contraction, Churn) per
account × product × segment × region.

Pages:
  1. ARR Bridge Waterfall — net ARR movement waterfall + KPI tiles
  2. Cohort Retention — cohort quarter × age heatmap with NRR/GRR
  3. Growth Matrix — product × region growth rate heatmap
  4. Account Detail — drill-down table with record actions

Dataset: Customer_ARR_Bridge (grain: month × account × product × segment × region)
"""

import csv
import io
import sys
from collections import defaultdict
from datetime import datetime

from crm_analytics_helpers import (
    get_auth,
    _soql,
    _dim,
    _measure,
    upload_dataset,
    get_dataset_id,
    arr_bridge_fields,
    cohort_retention_fields,
    arr_bridge_waterfall_step,
    arr_bridge_trend_step,
    growth_cube_step,
    cohort_retention_step,
    sq,
    af,
    num,
    num_dynamic_color,
    rich_chart,
    waterfall_chart,
    heatmap_chart,
    hdr,
    section_label,
    nav_link,
    pg,
    nav_row,
    build_dashboard_state,
    deploy_dashboard,
    create_dashboard_if_needed,
    set_record_links_xmd,
    add_table_action,
    add_selection_interaction,
    pillbox,
    coalesce_filter,
)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

DS = "Customer_ARR_Bridge"
DS_LABEL = "Customer ARR Bridge"
COHORT_DS = "Customer_Cohort_Retention"
COHORT_DS_LABEL = "Customer Cohort Retention"
DASHBOARD_LABEL = "ARR Bridge & Growth"

# SOQL: pull closed opportunities with account details for ARR movement calc
SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, "
    "Account_Unit_Group__c, Sales_Region__c, "
    "IsClosed, IsWon, CloseDate, Type, FiscalYear, "
    "APTS_Forecast_ARR__c, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
    "APTS_RH_Product_Family__c, "
    "Account.Industry, Account.Segment__c "
    "FROM Opportunity "
    "WHERE IsClosed = true "
    "AND FiscalYear IN (2024, 2025, 2026)"
)

# Filter bindings
UF = coalesce_filter("f_unit", "UnitGroup")
RF = coalesce_filter("f_region", "SalesRegion")
PF = coalesce_filter("f_product", "ProductFamily")


# ═══════════════════════════════════════════════════════════════════════════
#  Dataset creation — ARR Bridge
# ═══════════════════════════════════════════════════════════════════════════


def _close_month(close_date):
    """Extract YYYY-MM from CloseDate."""
    if close_date and len(close_date) >= 7:
        return close_date[:7]
    return ""


def _fy_label(fiscal_year):
    """Format fiscal year as FY2026."""
    return f"FY{fiscal_year}" if fiscal_year else ""


def create_bridge_dataset(inst, tok):
    """Query closed Opportunities, compute ARR movements, upload bridge dataset."""
    print("\n=== Building Customer ARR Bridge dataset ===")

    opps = _soql(inst, tok, SOQL)
    print(f"  Queried {len(opps)} closed opportunities")

    # ── Classify each opp as a bridge component ──
    # Group by (AccountId, CloseMonth, ProductFamily, Segment, Region, UnitGroup)
    bridge_rows = defaultdict(lambda: {
        "AccountName": "",
        "FYLabel": "",
        "NewARR": 0,
        "ExpansionARR": 0,
        "ContractionARR": 0,
        "ChurnARR": 0,
    })

    for o in opps:
        acct = o.get("Account") or {}
        arr = o.get("ConvertedARR") or 0
        if not arr:
            continue

        close_date = o.get("CloseDate") or ""
        opp_type = o.get("Type") or ""
        is_won = str(o.get("IsWon", False)).lower() == "true"
        product = (o.get("APTS_RH_Product_Family__c") or "").split(";")[0].strip() or "Other"
        segment = acct.get("Segment__c") or "Unknown"
        region = o.get("Sales_Region__c") or "Unknown"
        unit = o.get("Account_Unit_Group__c") or ""
        month = _close_month(close_date)
        if not month:
            continue

        key = (
            o.get("AccountId", ""),
            month,
            product,
            segment,
            region,
            unit,
        )

        row = bridge_rows[key]
        row["AccountName"] = (acct.get("Name") or "")[:255]
        row["FYLabel"] = _fy_label(o.get("FiscalYear"))

        if is_won:
            if opp_type == "Land":
                row["NewARR"] += arr
            elif opp_type == "Expand":
                row["ExpansionARR"] += arr
            elif opp_type == "Renewal":
                pass  # Renewal won = retained, no bridge movement
            else:
                # Default: treat as new business
                row["NewARR"] += arr
        else:
            # Lost/closed-lost
            if opp_type == "Renewal":
                row["ChurnARR"] += arr
            # Note: ContractionARR requires downsell detection
            # (not available in standard Opportunity; left as 0)

    # ── Write CSV ──
    fields_meta = arr_bridge_fields()
    field_names = [
        "AccountId", "AccountName", "ProductFamily", "Segment",
        "SalesRegion", "UnitGroup", "BridgeMonth", "BridgeComponent",
        "FYLabel", "StartARR", "NewARR", "ExpansionARR",
        "ContractionARR", "ChurnARR", "EndARR", "NetNewARR",
        "GrowthRatePct",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=field_names, lineterminator="\n")
    writer.writeheader()

    row_count = 0
    for (acct_id, month, product, segment, region, unit), vals in bridge_rows.items():
        net = vals["NewARR"] + vals["ExpansionARR"] - vals["ContractionARR"] - vals["ChurnARR"]
        # StartARR = 0 at row level; the SAQL waterfall step handles cumulative logic
        writer.writerow({
            "AccountId": acct_id,
            "AccountName": vals["AccountName"],
            "ProductFamily": product,
            "Segment": segment,
            "SalesRegion": region,
            "UnitGroup": unit,
            "BridgeMonth": month,
            "BridgeComponent": "Net",
            "FYLabel": vals["FYLabel"],
            "StartARR": 0,
            "NewARR": vals["NewARR"],
            "ExpansionARR": vals["ExpansionARR"],
            "ContractionARR": vals["ContractionARR"],
            "ChurnARR": vals["ChurnARR"],
            "EndARR": net,
            "NetNewARR": net,
            "GrowthRatePct": 0,  # Computed in SAQL via growth_cube_step
        })
        row_count += 1

    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"  Generated {row_count} bridge rows")

    ok = upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)
    return DS if ok else None


# ═══════════════════════════════════════════════════════════════════════════
#  Dataset creation — Cohort Retention
# ═══════════════════════════════════════════════════════════════════════════


def _quarter_label(close_date):
    """Derive quarter label (e.g. '2025-Q1') from CloseDate."""
    if not close_date or len(close_date) < 7:
        return ""
    try:
        year = close_date[:4]
        month = int(close_date[5:7])
    except (ValueError, IndexError):
        return ""
    if month <= 3:
        return f"{year}-Q1"
    if month <= 6:
        return f"{year}-Q2"
    if month <= 9:
        return f"{year}-Q3"
    return f"{year}-Q4"


def create_cohort_dataset(inst, tok):
    """Build cohort retention dataset from closed-won opportunities.

    Grain: cohort_quarter × age_months × product × segment × region.
    For each account, cohort = quarter of first won opportunity.
    """
    print("\n=== Building Cohort Retention dataset ===")

    opps = _soql(
        inst, tok,
        "SELECT AccountId, Account.Name, Account.Segment__c, "
        "Account_Unit_Group__c, Sales_Region__c, CloseDate, Type, "
        "APTS_RH_Product_Family__c, "
        "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR "
        "FROM Opportunity "
        "WHERE IsWon = true AND FiscalYear IN (2024, 2025, 2026)"
    )
    print(f"  Queried {len(opps)} won opportunities for cohort analysis")

    # ── Find first-won quarter per account (= cohort) ──
    acct_first_close = {}
    for o in opps:
        acct_id = o.get("AccountId", "")
        close = o.get("CloseDate") or ""
        if acct_id and close:
            existing = acct_first_close.get(acct_id, "9999")
            if close < existing:
                acct_first_close[acct_id] = close

    acct_cohort = {
        acct_id: _quarter_label(dt)
        for acct_id, dt in acct_first_close.items()
    }

    # ── Build cohort rows ──
    # Group by (cohort_quarter, age_months, product, segment, region, unit)
    cohort_rows = defaultdict(lambda: {
        "CohortSize": 0,
        "RetainedCount": 0,
        "RetainedARR": 0,
        "ExpandedARR": 0,
        "ChurnedARR": 0,
    })

    for o in opps:
        acct_id = o.get("AccountId", "")
        cohort_q = acct_cohort.get(acct_id, "")
        if not cohort_q:
            continue

        close = o.get("CloseDate") or ""
        arr = o.get("ConvertedARR") or 0
        opp_type = o.get("Type") or ""
        product = (o.get("APTS_RH_Product_Family__c") or "").split(";")[0].strip() or "Other"
        acct = o.get("Account") or {}
        segment = acct.get("Segment__c") or "Unknown"
        region = o.get("Sales_Region__c") or "Unknown"
        unit = o.get("Account_Unit_Group__c") or ""

        # Compute age in months from cohort start
        cohort_start = acct_first_close.get(acct_id, "")
        if cohort_start and close:
            try:
                c_year, c_month = int(cohort_start[:4]), int(cohort_start[5:7])
                o_year, o_month = int(close[:4]), int(close[5:7])
                age = (o_year - c_year) * 12 + (o_month - c_month)
            except (ValueError, IndexError):
                age = 0
        else:
            age = 0

        key = (cohort_q, age, product, segment, region, unit)
        row = cohort_rows[key]
        row["RetainedARR"] += arr
        row["RetainedCount"] += 1
        if age == 0:
            row["CohortSize"] += 1
        if opp_type == "Expand":
            row["ExpandedARR"] += arr

    # ── Write CSV ──
    fields_meta = cohort_retention_fields()
    field_names = [
        "CohortQuarter", "ProductFamily", "Segment", "SalesRegion",
        "UnitGroup", "AgeMonths", "CohortSize", "RetainedCount",
        "RetainedARR", "ExpandedARR", "ChurnedARR", "GRR", "NRR",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=field_names, lineterminator="\n")
    writer.writeheader()

    row_count = 0
    for (cohort_q, age, product, segment, region, unit), vals in cohort_rows.items():
        # GRR/NRR require baseline; approximate from cohort size
        cohort_baseline = vals.get("CohortSize", 0) or 1
        grr = min((vals["RetainedARR"] / max(vals["RetainedARR"], 1)) * 100, 200)
        nrr = min(((vals["RetainedARR"] + vals["ExpandedARR"]) /
                   max(vals["RetainedARR"], 1)) * 100, 200)

        writer.writerow({
            "CohortQuarter": cohort_q,
            "ProductFamily": product,
            "Segment": segment,
            "SalesRegion": region,
            "UnitGroup": unit,
            "AgeMonths": age,
            "CohortSize": vals["CohortSize"],
            "RetainedCount": vals["RetainedCount"],
            "RetainedARR": vals["RetainedARR"],
            "ExpandedARR": vals["ExpandedARR"],
            "ChurnedARR": vals["ChurnedARR"],
            "GRR": round(grr, 1),
            "NRR": round(nrr, 1),
        })
        row_count += 1

    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"  Generated {row_count} cohort rows")

    ok = upload_dataset(inst, tok, COHORT_DS, COHORT_DS_LABEL, fields_meta, csv_bytes)
    return COHORT_DS if ok else None


# ═══════════════════════════════════════════════════════════════════════════
#  Dashboard steps
# ═══════════════════════════════════════════════════════════════════════════


def build_steps(ds_meta):
    L = f'q = load "{DS}";\n'

    return {
        # ── Filters ──
        "f_unit": af("UnitGroup", ds_meta),
        "f_region": af("SalesRegion", ds_meta),
        "f_product": af("ProductFamily", ds_meta),
        "f_segment": af("Segment", ds_meta),

        # ══════════════════════════════════════════════════════════════
        #  PAGE 1: ARR Bridge Waterfall
        # ══════════════════════════════════════════════════════════════
        "s_bridge_waterfall": arr_bridge_waterfall_step(DS, UF + RF + PF),
        "s_bridge_trend": arr_bridge_trend_step(DS, UF + RF + PF),
        "s_bridge_kpi": sq(
            L + UF + RF + PF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(NewARR) as new_arr, "
            + "sum(ExpansionARR) as expansion_arr, "
            + "sum(ContractionARR) as contraction_arr, "
            + "sum(ChurnARR) as churn_arr, "
            + "sum(NetNewARR) as net_new_arr, "
            + "count() as row_count;"
        ),

        # ══════════════════════════════════════════════════════════════
        #  PAGE 2: Cohort Retention
        # ══════════════════════════════════════════════════════════════
        "s_cohort_nrr": cohort_retention_step(COHORT_DS, "", "NRR"),
        "s_cohort_grr": cohort_retention_step(COHORT_DS, "", "GRR"),

        # ══════════════════════════════════════════════════════════════
        #  PAGE 3: Growth Matrix
        # ══════════════════════════════════════════════════════════════
        "s_growth_prod_region": growth_cube_step(DS, "ProductFamily", "SalesRegion", UF),
        "s_growth_prod_segment": growth_cube_step(DS, "ProductFamily", "Segment", UF + RF),
        "s_growth_segment_region": growth_cube_step(DS, "Segment", "SalesRegion", UF),

        # ══════════════════════════════════════════════════════════════
        #  PAGE 4: Account Detail
        # ══════════════════════════════════════════════════════════════
        "s_acct_detail": sq(
            L + UF + RF + PF
            + "q = group q by (AccountId, AccountName, Segment, SalesRegion, UnitGroup);\n"
            + "q = foreach q generate "
            + "AccountId, AccountName, Segment, SalesRegion, UnitGroup, "
            + "sum(NewARR) as new_arr, "
            + "sum(ExpansionARR) as expansion_arr, "
            + "sum(ChurnARR) as churn_arr, "
            + "sum(NetNewARR) as net_new_arr;\n"
            + "q = order q by net_new_arr desc;\n"
            + "q = limit q 200;"
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Widgets
# ═══════════════════════════════════════════════════════════════════════════

PAGE_NAMES = ["bridge", "cohort", "growth", "detail"]
PAGE_LABELS = ["ARR Bridge", "Cohort Retention", "Growth Matrix", "Account Detail"]


def build_widgets():
    w = {}

    # ── Navigation row (all pages) ──
    for i, (name, label) in enumerate(zip(PAGE_NAMES, PAGE_LABELS)):
        w[f"nav_{name}"] = nav_link(label, name, active=(i == 0))

    # ── Filter bar ──
    w["f_unit"] = pillbox("f_unit", "Unit Group")
    w["f_region"] = pillbox("f_region", "Sales Region")
    w["f_product"] = pillbox("f_product", "Product Family")
    w["f_segment"] = pillbox("f_segment", "Segment")

    # ══════════════════════════════════════════════════════════════════════
    #  PAGE 1: ARR Bridge Waterfall
    # ══════════════════════════════════════════════════════════════════════
    w["p1_hdr"] = hdr("ARR Bridge Analysis")

    w["p1_kpi_new"] = num("s_bridge_kpi", "new_arr", "New ARR",
                          fmt="$,.0f", compact=True)
    w["p1_kpi_expand"] = num("s_bridge_kpi", "expansion_arr", "Expansion ARR",
                             fmt="$,.0f", compact=True)
    w["p1_kpi_churn"] = num_dynamic_color(
        "s_bridge_kpi", "churn_arr", "Churn ARR",
        thresholds=[(0, "#4CAF50"), (100000, "#FF9800"), (500000, "#F44336")],
        fmt="$,.0f", compact=True,
    )
    w["p1_kpi_net"] = num("s_bridge_kpi", "net_new_arr", "Net New ARR",
                          fmt="$,.0f", compact=True)

    w["p1_ch_waterfall"] = waterfall_chart(
        "s_bridge_waterfall",
        title="ARR Bridge (FY2026)",
    )
    w["p1_sec_trend"] = section_label("Monthly ARR Bridge Trend")
    w["p1_ch_trend"] = rich_chart(
        "s_bridge_trend",
        chart_type="bar",
        title="Monthly ARR Movements",
        groups=["BridgeMonth"],
        measures=["new_arr", "expansion_arr", "contraction_arr", "churn_arr"],
    )

    # ══════════════════════════════════════════════════════════════════════
    #  PAGE 2: Cohort Retention
    # ══════════════════════════════════════════════════════════════════════
    w["p2_hdr"] = hdr("Cohort Retention Analytics")
    w["p2_sec_nrr"] = section_label("Net Revenue Retention by Cohort")
    w["p2_ch_nrr"] = heatmap_chart(
        "s_cohort_nrr",
        title="NRR by Cohort Quarter × Age",
        x_field="AgeMonths",
        y_field="CohortQuarter",
        color_field="nrr_pct",
    )
    w["p2_sec_grr"] = section_label("Gross Revenue Retention by Cohort")
    w["p2_ch_grr"] = heatmap_chart(
        "s_cohort_grr",
        title="GRR by Cohort Quarter × Age",
        x_field="AgeMonths",
        y_field="CohortQuarter",
        color_field="grr_pct",
    )

    # ══════════════════════════════════════════════════════════════════════
    #  PAGE 3: Growth Matrix
    # ══════════════════════════════════════════════════════════════════════
    w["p3_hdr"] = hdr("Growth Rate Analytics")
    w["p3_sec_prod_region"] = section_label("Product × Region Growth Rate")
    w["p3_ch_prod_region"] = heatmap_chart(
        "s_growth_prod_region",
        title="Growth Rate: Product × Region",
        x_field="SalesRegion",
        y_field="ProductFamily",
        color_field="growth_rate_pct",
    )
    w["p3_sec_prod_segment"] = section_label("Product × Segment Growth Rate")
    w["p3_ch_prod_segment"] = heatmap_chart(
        "s_growth_prod_segment",
        title="Growth Rate: Product × Segment",
        x_field="Segment",
        y_field="ProductFamily",
        color_field="growth_rate_pct",
    )
    w["p3_sec_seg_region"] = section_label("Segment × Region Growth Rate")
    w["p3_ch_seg_region"] = heatmap_chart(
        "s_growth_segment_region",
        title="Growth Rate: Segment × Region",
        x_field="SalesRegion",
        y_field="Segment",
        color_field="growth_rate_pct",
    )

    # ══════════════════════════════════════════════════════════════════════
    #  PAGE 4: Account Detail
    # ══════════════════════════════════════════════════════════════════════
    w["p4_hdr"] = hdr("Account ARR Detail")
    w["p4_sec_detail"] = section_label("Account-Level ARR Movements")
    w["p4_tbl_detail"] = rich_chart(
        "s_acct_detail",
        chart_type="table",
        title="Account ARR Movements",
        groups=["AccountName", "Segment", "SalesRegion", "UnitGroup"],
        measures=["new_arr", "expansion_arr", "churn_arr", "net_new_arr"],
    )
    add_table_action(w["p4_tbl_detail"], object_name="Account", id_field="AccountId")

    return w


# ═══════════════════════════════════════════════════════════════════════════
#  Layout
# ═══════════════════════════════════════════════════════════════════════════


def build_layout():
    return [
        # Navigation + Filters (rows 0-2, shared across pages)
        {"name": "nav_bridge", "row": 0, "col": 0, "w": 3, "h": 1, "pages": PAGE_NAMES},
        {"name": "nav_cohort", "row": 0, "col": 3, "w": 3, "h": 1, "pages": PAGE_NAMES},
        {"name": "nav_growth", "row": 0, "col": 6, "w": 3, "h": 1, "pages": PAGE_NAMES},
        {"name": "nav_detail", "row": 0, "col": 9, "w": 3, "h": 1, "pages": PAGE_NAMES},
        {"name": "f_unit", "row": 1, "col": 0, "w": 3, "h": 1, "pages": PAGE_NAMES},
        {"name": "f_region", "row": 1, "col": 3, "w": 3, "h": 1, "pages": PAGE_NAMES},
        {"name": "f_product", "row": 1, "col": 6, "w": 3, "h": 1, "pages": PAGE_NAMES},
        {"name": "f_segment", "row": 1, "col": 9, "w": 3, "h": 1, "pages": PAGE_NAMES},

        # ── PAGE 1: ARR Bridge ──
        {"name": "p1_hdr", "row": 2, "col": 0, "w": 12, "h": 1, "pages": ["bridge"]},
        {"name": "p1_kpi_new", "row": 3, "col": 0, "w": 3, "h": 2, "pages": ["bridge"]},
        {"name": "p1_kpi_expand", "row": 3, "col": 3, "w": 3, "h": 2, "pages": ["bridge"]},
        {"name": "p1_kpi_churn", "row": 3, "col": 6, "w": 3, "h": 2, "pages": ["bridge"]},
        {"name": "p1_kpi_net", "row": 3, "col": 9, "w": 3, "h": 2, "pages": ["bridge"]},
        {"name": "p1_ch_waterfall", "row": 5, "col": 0, "w": 12, "h": 8, "pages": ["bridge"]},
        {"name": "p1_sec_trend", "row": 13, "col": 0, "w": 12, "h": 1, "pages": ["bridge"]},
        {"name": "p1_ch_trend", "row": 14, "col": 0, "w": 12, "h": 8, "pages": ["bridge"]},

        # ── PAGE 2: Cohort Retention ──
        {"name": "p2_hdr", "row": 2, "col": 0, "w": 12, "h": 1, "pages": ["cohort"]},
        {"name": "p2_sec_nrr", "row": 3, "col": 0, "w": 12, "h": 1, "pages": ["cohort"]},
        {"name": "p2_ch_nrr", "row": 4, "col": 0, "w": 12, "h": 8, "pages": ["cohort"]},
        {"name": "p2_sec_grr", "row": 12, "col": 0, "w": 12, "h": 1, "pages": ["cohort"]},
        {"name": "p2_ch_grr", "row": 13, "col": 0, "w": 12, "h": 8, "pages": ["cohort"]},

        # ── PAGE 3: Growth Matrix ──
        {"name": "p3_hdr", "row": 2, "col": 0, "w": 12, "h": 1, "pages": ["growth"]},
        {"name": "p3_sec_prod_region", "row": 3, "col": 0, "w": 12, "h": 1, "pages": ["growth"]},
        {"name": "p3_ch_prod_region", "row": 4, "col": 0, "w": 12, "h": 8, "pages": ["growth"]},
        {"name": "p3_sec_prod_segment", "row": 12, "col": 0, "w": 12, "h": 1, "pages": ["growth"]},
        {"name": "p3_ch_prod_segment", "row": 13, "col": 0, "w": 6, "h": 8, "pages": ["growth"]},
        {"name": "p3_sec_seg_region", "row": 12, "col": 6, "w": 6, "h": 1, "pages": ["growth"]},
        {"name": "p3_ch_seg_region", "row": 13, "col": 6, "w": 6, "h": 8, "pages": ["growth"]},

        # ── PAGE 4: Account Detail ──
        {"name": "p4_hdr", "row": 2, "col": 0, "w": 12, "h": 1, "pages": ["detail"]},
        {"name": "p4_sec_detail", "row": 3, "col": 0, "w": 12, "h": 1, "pages": ["detail"]},
        {"name": "p4_tbl_detail", "row": 4, "col": 0, "w": 12, "h": 16, "pages": ["detail"]},

        # ── Page definitions ──
        pg("bridge", "ARR Bridge", 0),
        pg("cohort", "Cohort Retention", 1),
        pg("growth", "Growth Matrix", 2),
        pg("detail", "Account Detail", 3),
    ]


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main():
    instance_url, token = get_auth()
    print(f"  Authenticated to {instance_url}")

    # 1. Build ARR Bridge dataset
    bridge_ok = create_bridge_dataset(instance_url, token)
    if not bridge_ok:
        print("ERROR: ARR Bridge dataset build failed — aborting")
        return

    # 2. Build Cohort Retention dataset
    cohort_ok = create_cohort_dataset(instance_url, token)
    if not cohort_ok:
        print("WARNING: Cohort Retention dataset failed — page 2 may show empty charts")

    # 3. Set XMD record links
    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "AccountName", "sobject": "Account", "id_field": "AccountId"},
            {"field": "AccountId", "sobject": "Account", "id_field": "AccountId",
             "label": "Account ID"},
        ],
    )

    # 4. Look up dataset ID and build dashboard
    ds_id = get_dataset_id(instance_url, token, DS)
    ds_meta = [{"id": ds_id, "name": DS}] if ds_id else [{"name": DS}]

    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    state = build_dashboard_state(build_steps(ds_meta), build_widgets(), build_layout())
    deploy_dashboard(instance_url, token, dashboard_id, state)


if __name__ == "__main__":
    main()
