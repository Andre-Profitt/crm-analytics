#!/usr/bin/env python3
"""Build the Revenue Motions KPI dashboard — RW 1.8, 1.9, 1.10, 1.14 — ML-Forward upgrade with ARR bridge.

Pages:
  1. Pipeline by Motion — Land/Expand/Renewal funnels, waterfall, combo, gauges
  2. Renewals (RW 1.8) — term-based stacks, YoY area, indexation, at-risk table
  3. Conversion / ILF / ALF (RW 1.9) — ILF/ALF combos, cross-sell, funnel
  4. Cancellation & Churn (RW 1.10) — lost ARR, risk dist, at-risk, net ARR
  5. Product/Pricing (RW 1.14) — SaaS ARR, penetration, product mix
  6. Competitive Intelligence — competitor frequency, win rate, reason analysis
  7. Advanced Analytics — Sankey, treemap, heatmap, bubble, area
  8. Statistical Analysis — bullet charts, percentiles, distributions
  9. ARR Bridge & Growth (ML-Forward) — ARR bridge waterfall, growth rate analytics, expected churn computation

Dataset: Revenue_Motions (Opportunity + Account fields)

Visualization Upgrade:
  - Motion → Stage/Outcome Sankey for deal flow visualization
  - NRR bridge waterfall (Start → Renewed → Expanded → Churned)
  - Competitor × Segment heatmap for competitive pattern discovery
  - Renewal cohort timeline view
  - Dynamic KPI tiles with threshold-based coloring
"""

import csv
import io
import sys

from crm_analytics_helpers import (
    get_auth,
    _soql,
    _dim,
    _measure,
    _date,
    upload_dataset,
    get_dataset_id,
    sq,
    af,
    num,
    num_dynamic_color,
    num_with_trend,
    trend_step,
    rich_chart,
    gauge,
    funnel_chart,
    waterfall_chart,
    coalesce_filter,
    pillbox,
    hdr,
    section_label,
    nav_link,
    pg,
    nav_row,
    build_dashboard_state,
    deploy_dashboard,
    create_dashboard_if_needed,
    precompute_scoring_stats,
    compute_win_score,
    sankey_chart,
    treemap_chart,
    area_chart,
    heatmap_chart,
    bullet_chart,
    bubble_chart,
    choropleth_chart,
    combo_chart,
    timeline_chart,
    create_dataflow,
    run_dataflow,
    set_record_links_xmd,  # noqa: F401
    add_table_action,
    arr_bridge_waterfall_step,
    growth_cube_step,
)

DS = "Revenue_Motions"
DS_LABEL = "Revenue Motions"
DASHBOARD_LABEL = "Revenue Motions KPIs"

# ═══════════════════════════════════════════════════════════════════════════
#  Dataset creation
# ═══════════════════════════════════════════════════════════════════════════

SOQL = (
    "SELECT Id, Name, Owner.Name, AccountId, Account.Name, "
    "Account_Unit_Group__c, Sales_Region__c, ForecastCategoryName, "
    "IsClosed, IsWon, CloseDate, StageName, Type, CreatedDate, "
    "FiscalYear, FiscalQuarter, "
    "APTS_Forecast_ARR__c, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
    "Amount, Probability, AgeInDays, Sales_Cycle_Duration__c, "
    "Reason_Won_Lost__c, Sub_Reason__c, "
    "Lost_to_Competitor__r.Name, "
    "Account.SaaS_Client__c, Account.Axioma_Client__c, "
    "Account.Risk_of_Potential_Termination__c, "
    "Account.APTS_Subscription_Term__c, "
    "APTS_RH_Product_Family__c "
    "FROM Opportunity "
    "WHERE FiscalYear IN (2025, 2026, 2027)"
)

CSV_FIELDS = [
    "Id",
    "Name",
    "OwnerName",
    "AccountId",
    "AccountName",
    "UnitGroup",
    "SalesRegion",
    "ForecastCategory",
    "IsClosed",
    "IsWon",
    "CloseDate",
    "StageName",
    "Type",
    "CreatedDate",
    "FiscalYear",
    "FiscalQuarter",
    "ARR",
    "Amount",
    "Probability",
    "AgeInDays",
    "SalesCycleDuration",
    "WonLostReason",
    "Competitor",
    "SubReason",
    # Computed fields
    "IsRenewal",
    "IsExpand",
    "IsLand",
    "RenewalARR",
    "ExpandARR",
    "LandARR",
    "SaaSClient",
    "AxiomaClient",
    "RiskOfTermination",
    "IsAtRisk",
    "SubscriptionTerm",
    "CloseQuarter",
    "CloseMonth",
    "FYLabel",
    "WinScore",
    "WinScoreBand",
    "ProductFamily",
    "IsPS",
    "PSARR",
    "OneOffARR",
]


def _close_quarter(close_date):
    """Derive quarter label from CloseDate month."""
    if not close_date or len(close_date) < 7:
        return ""
    try:
        month = int(close_date[5:7])
    except ValueError:
        return ""
    if month <= 3:
        return "Q1"
    if month <= 6:
        return "Q2"
    if month <= 9:
        return "Q3"
    return "Q4"


def create_dataset(inst, tok):
    """Query Opportunity + Account fields, compute motion columns, upload CSV."""
    print("\n=== Building Revenue Motions dataset ===")

    opps = _soql(inst, tok, SOQL)
    print(f"  Queried {len(opps)} opportunities")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()

    type_win_rates, avg_deal_size = precompute_scoring_stats(opps)

    for o in opps:
        acct = o.get("Account") or {}
        owner = o.get("Owner") or {}
        opp_type = o.get("Type") or ""
        arr = o.get("ConvertedARR") or 0
        risk = acct.get("Risk_of_Potential_Termination__c") or ""
        close_date = o.get("CloseDate") or ""
        competitor = (
            o.get("Lost_to_Competitor__r", {}).get("Name", "")
            if o.get("Lost_to_Competitor__r")
            else ""
        )
        sub_reason = o.get("Sub_Reason__c") or ""
        win_score, win_band = compute_win_score(o, type_win_rates, avg_deal_size)

        writer.writerow(
            {
                "Id": o.get("Id", ""),
                "Name": (o.get("Name") or "")[:255],
                "OwnerName": (owner.get("Name") or "")[:255],
                "AccountId": o.get("AccountId", ""),
                "AccountName": (acct.get("Name") or "")[:255],
                "UnitGroup": o.get("Account_Unit_Group__c", ""),
                "SalesRegion": o.get("Sales_Region__c", ""),
                "ForecastCategory": o.get("ForecastCategoryName", ""),
                "IsClosed": str(o.get("IsClosed", False)).lower(),
                "IsWon": str(o.get("IsWon", False)).lower(),
                "CloseDate": close_date,
                "StageName": o.get("StageName", ""),
                "Type": opp_type,
                "CreatedDate": (o.get("CreatedDate") or "")[:10],
                "FiscalYear": o.get("FiscalYear", ""),
                "FiscalQuarter": o.get("FiscalQuarter", ""),
                "ARR": arr,
                "Amount": o.get("Amount") or 0,
                "Probability": o.get("Probability") or 0,
                "AgeInDays": o.get("AgeInDays") or 0,
                "SalesCycleDuration": o.get("Sales_Cycle_Duration__c") or 0,
                "WonLostReason": o.get("Reason_Won_Lost__c") or "",
                "Competitor": competitor,
                "SubReason": sub_reason,
                # Computed motion fields
                "IsRenewal": "true" if opp_type == "Renewal" else "false",
                "IsExpand": "true" if opp_type == "Expand" else "false",
                "IsLand": "true" if opp_type == "Land" else "false",
                "RenewalARR": arr if opp_type == "Renewal" else 0,
                "ExpandARR": arr if opp_type == "Expand" else 0,
                "LandARR": arr if opp_type == "Land" else 0,
                "SaaSClient": acct.get("SaaS_Client__c") or "false",
                "AxiomaClient": acct.get("Axioma_Client__c") or "false",
                "RiskOfTermination": risk,
                "IsAtRisk": ("true" if risk in ("High", "Medium") else "false"),
                "SubscriptionTerm": acct.get("APTS_Subscription_Term__c") or 0,
                "CloseQuarter": _close_quarter(close_date),
                "CloseMonth": close_date[:7] if close_date else "",
                "FYLabel": f"FY{o.get('FiscalYear', '')}",
                "WinScore": win_score,
                "WinScoreBand": win_band,
                "ProductFamily": (o.get("APTS_RH_Product_Family__c") or "")
                .split(";")[0]
                .strip(),
                "IsPS": "true"
                if opp_type in ("PS", "Fast track PS", "Coric PS")
                else "false",
                "PSARR": arr if opp_type in ("PS", "Fast track PS", "Coric PS") else 0,
                "OneOffARR": arr
                if opp_type in ("PS", "Fast track PS", "Coric PS")
                else 0,
            }
        )

    # ── Phase 9: Python-precomputed analytics ──
    # 9E: Gini coefficient of revenue concentration
    won_arrs_by_acct = {}
    for o in opps:
        if str(o.get("IsWon", "")).lower() == "true" and o.get("FiscalYear") == 2026:
            acct_name = (o.get("Account") or {}).get("Name") or "Unknown"
            won_arrs_by_acct[acct_name] = won_arrs_by_acct.get(acct_name, 0) + (
                o.get("ConvertedARR") or 0
            )
    sorted_arrs = sorted(won_arrs_by_acct.values())
    n = len(sorted_arrs)
    if n > 1 and sum(sorted_arrs) > 0:
        cum = 0
        area_under = 0
        total = sum(sorted_arrs)
        for i, v in enumerate(sorted_arrs):
            cum += v
            area_under += cum / total
        gini = 1 - (2 * area_under / n) + (1 / n)
    else:
        gini = 0
    gini_rounded = round(gini, 4)
    print(f"  Gini coefficient: {gini_rounded} (n={n} accounts)")

    # 9C: Cohort quarter (first CloseDate quarter per account)
    acct_first_quarter = {}
    for o in opps:
        if str(o.get("IsWon", "")).lower() == "true":
            acct_id = o.get("AccountId", "")
            cd = o.get("CloseDate") or ""
            if acct_id and cd:
                cq = _close_quarter(cd)
                if (
                    acct_id not in acct_first_quarter
                    or cq < acct_first_quarter[acct_id]
                ):
                    acct_first_quarter[acct_id] = cq

    # Re-write CSV with precomputed fields
    buf2 = io.StringIO()
    csv_fields_ext = CSV_FIELDS + ["GiniIndex", "CohortQuarter"]
    writer2 = csv.DictWriter(buf2, fieldnames=csv_fields_ext, lineterminator="\n")
    writer2.writeheader()
    buf.seek(0)
    reader = csv.DictReader(buf)
    for row in reader:
        row["GiniIndex"] = gini_rounded
        row["CohortQuarter"] = acct_first_quarter.get(row.get("AccountId", ""), "")
        writer2.writerow(row)

    csv_bytes = buf2.getvalue().encode("utf-8")
    print(
        f"  CSV: {len(csv_bytes):,} bytes, {len(opps)} rows (with Gini + CohortQuarter)"
    )

    fields_meta = [
        _dim("Id", "Opportunity ID"),
        _dim("Name", "Opportunity Name"),
        _dim("OwnerName", "Owner"),
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account Name"),
        _dim("UnitGroup", "Unit Group"),
        _dim("SalesRegion", "Sales Region"),
        _dim("ForecastCategory", "Forecast Category"),
        _dim("IsClosed", "Is Closed"),
        _dim("IsWon", "Is Won"),
        _date("CloseDate", "Close Date"),
        _dim("StageName", "Stage"),
        _dim("Type", "Opportunity Type"),
        _date("CreatedDate", "Created Date"),
        _measure("FiscalYear", "Fiscal Year", scale=0, precision=4),
        _measure("FiscalQuarter", "Fiscal Quarter", scale=0, precision=1),
        _measure("ARR", "ARR (EUR)"),
        _measure("Amount", "Amount"),
        _measure("Probability", "Probability", scale=0, precision=3),
        _measure("AgeInDays", "Age (Days)", scale=0, precision=5),
        _measure("SalesCycleDuration", "Sales Cycle Duration", scale=0, precision=6),
        _dim("WonLostReason", "Won/Lost Reason"),
        _dim("Competitor", "Competitor"),
        _dim("SubReason", "Sub-Reason"),
        # Computed motion fields
        _dim("IsRenewal", "Is Renewal"),
        _dim("IsExpand", "Is Expand"),
        _dim("IsLand", "Is Land"),
        _measure("RenewalARR", "Renewal ARR"),
        _measure("ExpandARR", "Expand ARR"),
        _measure("LandARR", "Land ARR"),
        _dim("SaaSClient", "SaaS Client"),
        _dim("AxiomaClient", "Axioma Client"),
        _dim("RiskOfTermination", "Risk of Termination"),
        _dim("IsAtRisk", "Is At Risk"),
        _measure("SubscriptionTerm", "Subscription Term"),
        _dim("CloseQuarter", "Close Quarter"),
        _dim("CloseMonth", "Close Month"),
        _dim("FYLabel", "Fiscal Year Label"),
        _measure("WinScore", "Win Score", scale=0, precision=3),
        _dim("WinScoreBand", "Win Score Band"),
        _dim("ProductFamily", "Product Family"),
        _dim("IsPS", "Is Professional Services"),
        _measure("PSARR", "PS ARR"),
        _measure("OneOffARR", "One-Off Revenue ARR"),
        _measure("GiniIndex", "Gini Index", scale=4, precision=6),
        _dim("CohortQuarter", "Cohort Quarter"),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


# ═══════════════════════════════════════════════════════════════════════════
#  SAQL helpers
# ═══════════════════════════════════════════════════════════════════════════

# Base load
L = f'q = load "{DS}";\n'

# Common filters
FY26 = "q = filter q by FiscalYear == 2026;\n"
OPEN = 'q = filter q by IsClosed == "false";\n'
WON = 'q = filter q by IsWon == "true";\n'
CLOSED = 'q = filter q by IsClosed == "true";\n'
RENEWAL = 'q = filter q by Type == "Renewal";\n'
EXPAND = 'q = filter q by Type == "Expand";\n'
LAND = 'q = filter q by Type == "Land";\n'
AT_RISK = 'q = filter q by IsAtRisk == "true";\n'

# Quarter case expression (from CloseDate)
QTR = (
    '(case when substr(CloseDate, 6, 2) in ["01","02","03"] then "Q1" '
    'when substr(CloseDate, 6, 2) in ["04","05","06"] then "Q2" '
    'when substr(CloseDate, 6, 2) in ["07","08","09"] then "Q3" '
    'else "Q4" end)'
)

# Subscription term bucket expression
TERM_BUCKET = (
    '(case when SubscriptionTerm <= 12 then "1yr" '
    'when SubscriptionTerm <= 24 then "2yr" '
    'when SubscriptionTerm <= 36 then "3yr" '
    'else "4yr+" end)'
)

# Filter binding: unit group filter
UF = (
    "q = filter q by "
    '{{coalesce(column(f_unit.selection, ["UnitGroup"]), '
    "column(f_unit.result, [\"UnitGroup\"])).asEquality('UnitGroup')}};\n"
)
TF = coalesce_filter("f_type", "Type")
QF = coalesce_filter("f_qtr", "CloseQuarter")
RF = coalesce_filter("f_region", "SalesRegion")


# ═══════════════════════════════════════════════════════════════════════════
#  Steps
# ═══════════════════════════════════════════════════════════════════════════


def build_steps(ds_id):
    DS_META = [{"id": ds_id, "name": DS}]

    return {
        # ═══ FILTER STEPS ═══
        "f_unit": af("UnitGroup", DS_META),
        "f_type": af("Type", DS_META),
        "f_qtr": af("CloseQuarter", DS_META),
        "f_region": af("SalesRegion", DS_META),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 1 — Pipeline by Motion
        # ═══════════════════════════════════════════════════════════════════
        # KPI: Land pipeline ARR
        "s_land_pipe": sq(
            L
            + FY26
            + OPEN
            + LAND
            + UF
            + TF
            + QF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_arr;"
        ),
        # KPI: Expand pipeline ARR
        "s_expand_pipe": sq(
            L
            + FY26
            + OPEN
            + EXPAND
            + UF
            + TF
            + QF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_arr;"
        ),
        # KPI: Renewal pipeline ARR
        "s_renewal_pipe": sq(
            L
            + FY26
            + OPEN
            + RENEWAL
            + UF
            + TF
            + QF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_arr;"
        ),
        # Funnel: Pipeline by motion (Land > Expand > Renewal, ordered by ARR desc)
        "s_motion_funnel": sq(
            L
            + FY26
            + OPEN
            + UF
            + QF
            + RF
            + "q = group q by Type;\n"
            + "q = foreach q generate Type, sum(ARR) as sum_arr;\n"
            + "q = order q by sum_arr desc;"
        ),
        # Waterfall: QoQ pipeline change by motion type
        "s_motion_waterfall": sq(
            L
            + FY26
            + UF
            + TF
            + QF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, Type, "
            + '(case when IsClosed == "false" then ARR else 0 end) as pipe_arr, '
            + '(case when IsWon == "true" then ARR else 0 end) as won_arr, '
            + '(case when IsClosed == "true" and IsWon == "false" then -ARR else 0 end) as lost_arr;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, "
            + "sum(pipe_arr) + sum(won_arr) + sum(lost_arr) as net_change;\n"
            + "q = order q by Quarter asc;"
        ),
        # Combo: Pipeline by Qtr x Type (columns) + cumulative total (line)
        "s_qtr_type_pipe": sq(
            L
            + FY26
            + OPEN
            + UF
            + QF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, Type, ARR;\n"
            + "q = group q by (Quarter, Type);\n"
            + "q = foreach q generate Quarter, Type, sum(ARR) as sum_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # Combo: Pipeline by Qtr — pipe columns + cumul line
        "s_qtr_pipe_cumul": sq(
            L
            + FY26
            + OPEN
            + UF
            + TF
            + QF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, ARR;\n"
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, sum(ARR) as pipe_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # Gauge: Win rate Land
        "s_wr_land": sq(
            L
            + FY26
            + CLOSED
            + LAND
            + UF
            + TF
            + QF
            + RF
            + 'q = foreach q generate (case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_won) / count()) * 100 as win_rate;"
        ),
        # Gauge: Win rate Expand
        "s_wr_expand": sq(
            L
            + FY26
            + CLOSED
            + EXPAND
            + UF
            + TF
            + QF
            + RF
            + 'q = foreach q generate (case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_won) / count()) * 100 as win_rate;"
        ),
        # Gauge: Win rate Renewal
        "s_wr_renewal": sq(
            L
            + FY26
            + CLOSED
            + RENEWAL
            + UF
            + TF
            + QF
            + RF
            + 'q = foreach q generate (case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_won) / count()) * 100 as win_rate;"
        ),
        # Hbar: Sales cycle by Type
        "s_cycle_type": sq(
            L
            + FY26
            + WON
            + UF
            + QF
            + RF
            + "q = group q by Type;\n"
            + "q = foreach q generate Type, avg(SalesCycleDuration) as avg_cycle;\n"
            + "q = order q by avg_cycle desc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 2 — Renewals (RW 1.8)
        # ═══════════════════════════════════════════════════════════════════
        # Stackcolumn: Renewals per Qtr by term length bucket
        "s_ren_qtr_term": sq(
            L
            + FY26
            + RENEWAL
            + UF
            + TF
            + QF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + f"{TERM_BUCKET} as TermBucket, ARR;\n"
            + "q = group q by (Quarter, TermBucket);\n"
            + "q = foreach q generate Quarter, TermBucket, sum(ARR) as sum_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # Area: Renewals by month YoY (split by FYLabel)
        "s_ren_month_yoy": sq(
            L
            + RENEWAL
            + UF
            + TF
            + QF
            + RF
            + "q = foreach q generate CloseMonth, FYLabel, ARR;\n"
            + "q = group q by (CloseMonth, FYLabel);\n"
            + "q = foreach q generate CloseMonth, FYLabel, sum(ARR) as sum_arr;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # Waterfall: Existing ARR indexed by FY (renewal won ARR)
        "s_ren_fy_waterfall": sq(
            L
            + RENEWAL
            + WON
            + UF
            + TF
            + QF
            + RF
            + "q = group q by FYLabel;\n"
            + "q = foreach q generate FYLabel, sum(ARR) as sum_arr;\n"
            + "q = order q by FYLabel asc;"
        ),
        # Line: Indexation growth quarterly (renewal won ARR by quarter)
        "s_ren_qtr_index": sq(
            L
            + FY26
            + RENEWAL
            + WON
            + UF
            + TF
            + QF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, ARR;\n"
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, sum(ARR) as sum_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # Comparisontable: Renewals at risk (Risk=High/Medium, top 25 by ARR)
        "s_ren_at_risk": sq(
            L
            + FY26
            + RENEWAL
            + AT_RISK
            + UF
            + TF
            + QF
            + RF
            + "q = foreach q generate Id, Name, AccountName, RiskOfTermination, "
            + "ARR, CloseQuarter, StageName;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 25;"
        ),
        # Gauge: Renewal win rate
        "s_ren_wr": sq(
            L
            + FY26
            + CLOSED
            + RENEWAL
            + UF
            + TF
            + QF
            + RF
            + 'q = foreach q generate (case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_won) / count()) * 100 as win_rate;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 3 — Conversion / ILF / ALF (RW 1.9)
        # ═══════════════════════════════════════════════════════════════════
        # Combo: ILF (Land) ARR by Qtr — column=pipe, line=won
        "s_ilf_qtr": sq(
            L
            + FY26
            + LAND
            + UF
            + TF
            + QF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + '(case when IsClosed == "false" then ARR else 0 end) as pipe_arr, '
            + '(case when IsWon == "true" then ARR else 0 end) as won_arr;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, sum(pipe_arr) as pipeline, sum(won_arr) as closed_won;\n"
            + "q = order q by Quarter asc;"
        ),
        # Combo: ALF (Expand) ARR by Qtr — column=pipe, line=won
        "s_alf_qtr": sq(
            L
            + FY26
            + EXPAND
            + UF
            + TF
            + QF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + '(case when IsClosed == "false" then ARR else 0 end) as pipe_arr, '
            + '(case when IsWon == "true" then ARR else 0 end) as won_arr;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, sum(pipe_arr) as pipeline, sum(won_arr) as closed_won;\n"
            + "q = order q by Quarter asc;"
        ),
        # Stackcolumn: New customers by month x region (Type=Land + Won)
        "s_new_cust_region": sq(
            L
            + FY26
            + LAND
            + WON
            + UF
            + TF
            + QF
            + "q = foreach q generate CloseMonth, SalesRegion, ARR;\n"
            + "q = group q by (CloseMonth, SalesRegion);\n"
            + "q = foreach q generate CloseMonth, SalesRegion, count() as cnt;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # Waterfall: Cross-sell ARR quarterly adds from Axioma cross-sell
        "s_cross_sell_wf": sq(
            L
            + FY26
            + EXPAND
            + WON
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by AxiomaClient == "true";\n'
            + f"q = foreach q generate {QTR} as Quarter, ARR;\n"
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, sum(ARR) as sum_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # Number: Synergy won (Type=Land + SaaSClient=true + Won)
        "s_synergy_won": sq(
            L
            + FY26
            + LAND
            + WON
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by SaaSClient == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_arr, count() as cnt;"
        ),
        # Number: Synergy pipeline (Type=Land + SaaSClient=true + Open)
        "s_synergy_pipe": sq(
            L
            + FY26
            + LAND
            + OPEN
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by SaaSClient == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_arr, count() as cnt;"
        ),
        # Funnel: Land -> Expand conversion flow
        # Shows how many Land wins convert to Expand opportunities on same account
        "s_land_expand_funnel": sq(
            L
            + FY26
            + UF
            + QF
            + RF
            + "q = group q by Type;\n"
            + 'q = filter q by Type in ["Land", "Expand"];\n'
            + "q = foreach q generate Type, sum(ARR) as sum_arr;\n"
            + "q = order q by sum_arr desc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 4 — Cancellation & Churn (RW 1.10)
        # ═══════════════════════════════════════════════════════════════════
        # Stackcolumn: Lost ARR by Qtr + reason
        "s_lost_qtr_reason": sq(
            L
            + FY26
            + CLOSED
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by IsWon == "false";\n'
            + f"q = foreach q generate {QTR} as Quarter, WonLostReason, ARR;\n"
            + "q = group q by (Quarter, WonLostReason);\n"
            + "q = foreach q generate Quarter, WonLostReason, sum(ARR) as sum_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # Donut: Risk level distribution
        "s_risk_dist": sq(
            L
            + FY26
            + OPEN
            + RENEWAL
            + UF
            + TF
            + QF
            + RF
            + "q = group q by RiskOfTermination;\n"
            + "q = foreach q generate RiskOfTermination, sum(ARR) as sum_arr, count() as cnt;\n"
            + "q = order q by sum_arr desc;"
        ),
        # Gauge: % of pipe at risk
        "s_pct_at_risk": sq(
            L
            + FY26
            + OPEN
            + RENEWAL
            + UF
            + TF
            + QF
            + RF
            + "q = foreach q generate "
            + '(case when IsAtRisk == "true" then ARR else 0 end) as risk_arr, ARR;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(case when sum(ARR) > 0 then (sum(risk_arr) / sum(ARR)) * 100 else 0 end) as pct_at_risk;"
        ),
        # Comparisontable: At-risk deals (top 25)
        "s_at_risk_list": sq(
            L
            + FY26
            + OPEN
            + AT_RISK
            + UF
            + TF
            + QF
            + RF
            + "q = foreach q generate Id, Name, AccountName, Type, "
            + "RiskOfTermination, ARR, StageName, CloseQuarter;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 25;"
        ),
        # Area: Churn trend (renewal losses by month)
        "s_churn_month": sq(
            L
            + FY26
            + CLOSED
            + RENEWAL
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by IsWon == "false";\n'
            + "q = foreach q generate CloseMonth, ARR;\n"
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, sum(ARR) as lost_arr;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # Waterfall: Net ARR movement (renewals won - renewals lost per quarter)
        "s_net_arr_qtr": sq(
            L
            + FY26
            + CLOSED
            + RENEWAL
            + UF
            + TF
            + QF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + '(case when IsWon == "true" then ARR else -ARR end) as net_arr;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, sum(net_arr) as net_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 5 — Product/Pricing (RW 1.14)
        # ═══════════════════════════════════════════════════════════════════
        # Combo: SaaS ARR by Qtr (column=SaaS, line=non-SaaS)
        "s_saas_qtr": sq(
            L
            + FY26
            + OPEN
            + UF
            + TF
            + QF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + '(case when SaaSClient == "true" then ARR else 0 end) as saas_arr, '
            + '(case when SaaSClient != "true" then ARR else 0 end) as non_saas_arr;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, "
            + "sum(saas_arr) as saas_arr, sum(non_saas_arr) as non_saas_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # Gauge: SaaS penetration %
        "s_saas_pct": sq(
            L
            + FY26
            + WON
            + UF
            + TF
            + QF
            + RF
            + "q = foreach q generate "
            + '(case when SaaSClient == "true" then ARR else 0 end) as saas_arr, ARR;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(case when sum(ARR) > 0 then (sum(saas_arr) / sum(ARR)) * 100 else 0 end) as saas_pct;"
        ),
        # Stackhbar: Product by UnitGroup x SaaS
        "s_unit_saas": sq(
            L
            + FY26
            + OPEN
            + UF
            + TF
            + QF
            + RF
            + "q = group q by (UnitGroup, SaaSClient);\n"
            + "q = foreach q generate UnitGroup, SaaSClient, sum(ARR) as sum_arr;\n"
            + "q = order q by sum_arr desc;"
        ),
        # Number: SaaS won ARR
        "s_saas_won": sq(
            L
            + FY26
            + WON
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by SaaSClient == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_arr, count() as cnt;"
        ),
        # Number: SaaS % of won
        "s_saas_won_pct": sq(
            L
            + FY26
            + WON
            + UF
            + TF
            + QF
            + RF
            + "q = foreach q generate "
            + '(case when SaaSClient == "true" then ARR else 0 end) as saas_arr, ARR;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(case when sum(ARR) > 0 then (sum(saas_arr) / sum(ARR)) * 100 else 0 end) as saas_pct;"
        ),
        # ═══ PS ARR ATTACH + ONE-OFF + SaaS YoY (RW 1.14 gaps) ═══
        "s_ps_attach": sq(
            L
            + FY26
            + WON
            + UF
            + TF
            + QF
            + RF
            + "q = foreach q generate "
            + '(case when IsPS == "true" then ARR else 0 end) as ps_arr, ARR;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ps_arr) as ps_arr, sum(ARR) as total_arr, "
            + "(case when sum(ARR) > 0 then (sum(ps_arr) / sum(ARR)) * 100 else 0 end) as attach_rate;"
        ),
        "s_ps_qtr": sq(
            L
            + FY26
            + UF
            + RF
            + 'q = filter q by IsPS == "true";\n'
            + f"q = foreach q generate {QTR} as Quarter, "
            + '(case when IsWon == "true" then ARR else 0 end) as won_arr, '
            + '(case when IsClosed == "false" then ARR else 0 end) as pipe_arr;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, sum(won_arr) as won_arr, sum(pipe_arr) as pipe_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        "s_oneoff_qtr": sq(
            L
            + UF
            + RF
            + 'q = filter q by Type in ["PS", "Fast track PS", "Coric PS"];\n'
            + 'q = filter q by IsWon == "true";\n'
            + "q = foreach q generate FiscalYear as FY, "
            + f"{QTR} as Quarter, ARR;\n"
            + "q = group q by (FY, Quarter);\n"
            + "q = foreach q generate FY, Quarter, sum(ARR) as sum_arr;\n"
            + "q = order q by FY asc, Quarter asc;"
        ),
        "s_saas_yoy": sq(
            L
            + WON
            + UF
            + RF
            + 'q = filter q by SaaSClient == "true";\n'
            + "q = foreach q generate FiscalYear as FY, ARR;\n"
            + "q = group q by FY;\n"
            + "q = foreach q generate FY, sum(ARR) as sum_arr;\n"
            + "q = order q by FY asc;"
        ),
        # ═══ TREND STEPS (Phase 3) — FY2026 vs FY2025 ═══
        "s_land_pipe_t": trend_step(
            DS,
            OPEN + LAND + UF + RF,
            "q = filter q by FiscalYear == 2026;\n",
            "q = filter q by FiscalYear == 2025;\n",
            "all",
            "sum(ARR)",
            "sum_acv",
        ),
        "s_expand_pipe_t": trend_step(
            DS,
            OPEN + EXPAND + UF + RF,
            "q = filter q by FiscalYear == 2026;\n",
            "q = filter q by FiscalYear == 2025;\n",
            "all",
            "sum(ARR)",
            "sum_acv",
        ),
        "s_renewal_pipe_t": trend_step(
            DS,
            OPEN + RENEWAL + UF + RF,
            "q = filter q by FiscalYear == 2026;\n",
            "q = filter q by FiscalYear == 2025;\n",
            "all",
            "sum(ARR)",
            "sum_acv",
        ),
        # ═══ YoY MOTION COMPARISON STEPS ═══
        "s_land_yoy": sq(
            L
            + LAND
            + UF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + "FiscalYear as FY, ARR;\n"
            + "q = group q by (Quarter, FY);\n"
            + "q = foreach q generate Quarter, FY, sum(ARR) as sum_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        "s_expand_yoy": sq(
            L
            + EXPAND
            + UF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + "FiscalYear as FY, ARR;\n"
            + "q = group q by (Quarter, FY);\n"
            + "q = foreach q generate Quarter, FY, sum(ARR) as sum_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        "s_renewal_yoy": sq(
            L
            + RENEWAL
            + UF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + "FiscalYear as FY, ARR;\n"
            + "q = group q by (Quarter, FY);\n"
            + "q = foreach q generate Quarter, FY, sum(ARR) as sum_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 6 — Competitive Intelligence
        # ═══════════════════════════════════════════════════════════════════
        # Hbar: Top competitors by deal count
        "s_comp_frequency": sq(
            L
            + UF
            + RF
            + 'q = filter q by Competitor != "";\n'
            + "q = group q by Competitor;\n"
            + "q = foreach q generate Competitor, count() as cnt, sum(ARR) as total_arr;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 15;"
        ),
        # Comparisontable: Win rate vs competitors
        "s_comp_wr": sq(
            L
            + CLOSED
            + UF
            + RF
            + 'q = filter q by Competitor != "";\n'
            + "q = group q by Competitor;\n"
            + "q = foreach q generate Competitor, count() as total, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as won_cnt;\n'
            + "q = foreach q generate Competitor, total, won_cnt, "
            + "(won_cnt / total) * 100 as win_rate;\n"
            + "q = order q by total desc;\n"
            + "q = limit q 15;"
        ),
        # Hbar: Win/loss reason distribution
        "s_reason_dist": sq(
            L
            + CLOSED
            + UF
            + RF
            + 'q = filter q by WonLostReason != "";\n'
            + "q = group q by WonLostReason;\n"
            + "q = foreach q generate WonLostReason, count() as cnt;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 15;"
        ),
        # Stackhbar: Reasons by Won vs Lost
        "s_reason_won_lost": sq(
            L
            + CLOSED
            + UF
            + RF
            + 'q = filter q by WonLostReason != "";\n'
            + "q = foreach q generate WonLostReason, IsWon;\n"
            + "q = group q by (WonLostReason, IsWon);\n"
            + "q = foreach q generate WonLostReason, IsWon, count() as cnt;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 30;"
        ),
        # Hbar: Loss sub-reasons by ARR impact
        "s_sub_reason": sq(
            L
            + CLOSED
            + UF
            + RF
            + 'q = filter q by SubReason != "";\n'
            + 'q = filter q by IsWon == "false";\n'
            + "q = group q by SubReason;\n"
            + "q = foreach q generate SubReason, count() as cnt, sum(ARR) as lost_arr;\n"
            + "q = order q by lost_arr desc;\n"
            + "q = limit q 15;"
        ),
        # ═══ ITERATION 4: Closing RW KPI gaps ═══
        # KPI 31: SaaS ARR YoY growth % (cogroup current vs prior)
        "s_saas_growth": sq(
            L
            + WON
            + UF
            + RF
            + 'q = filter q by SaaSClient == "true";\n'
            + "q = foreach q generate FiscalYear, ARR;\n"
            + "q = group q by FiscalYear;\n"
            + "q = foreach q generate FiscalYear, sum(ARR) as total_arr;\n"
            + "q = order q by FiscalYear asc;"
        ),
        # KPI 27: Lost ARR as % of total pipeline (denominator)
        "s_lost_pct": sq(
            L
            + FY26
            + CLOSED
            + UF
            + TF
            + QF
            + RF
            + "q = foreach q generate ARR, "
            + '(case when IsWon == "false" then ARR else 0 end) as lost_arr;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(lost_arr) as total_lost, sum(ARR) as total_closed, "
            + "(sum(lost_arr) / sum(ARR)) * 100 as lost_pct;"
        ),
        # KPI 23: New customer by month with ARR value
        "s_new_cust_value": sq(
            L
            + FY26
            + LAND
            + WON
            + UF
            + TF
            + QF
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, count() as cnt, sum(ARR) as total_arr;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # ═══ V2: Advanced Visualizations ═══
        # Sankey: Revenue flow — Type → Product Family
        "s_sankey_flow": sq(
            L
            + FY26
            + WON
            + UF
            + QF
            + RF
            + 'q = filter q by ProductFamily != "";\n'
            + "q = group q by (Type, ProductFamily);\n"
            + "q = foreach q generate Type as source, ProductFamily as target, "
            + "sum(ARR) as total_arr, count() as cnt;\n"
            + "q = order q by total_arr desc;"
        ),
        # Treemap: Won ARR by UnitGroup → Type
        "s_treemap_won": sq(
            L
            + FY26
            + WON
            + UF
            + QF
            + RF
            + 'q = filter q by UnitGroup != "";\n'
            + "q = group q by (UnitGroup, Type);\n"
            + "q = foreach q generate UnitGroup, Type, sum(ARR) as total_arr;\n"
            + "q = order q by total_arr desc;"
        ),
        # Stacked area: Revenue composition by motion over months
        "s_area_motion": sq(
            L
            + FY26
            + WON
            + UF
            + RF
            + "q = group q by (CloseMonth, Type);\n"
            + "q = foreach q generate CloseMonth, Type, sum(ARR) as monthly_arr;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # Heatmap: Product × Quarter revenue
        "s_heatmap_prod_qtr": sq(
            L
            + FY26
            + WON
            + UF
            + RF
            + 'q = filter q by ProductFamily != "";\n'
            + "q = group q by (ProductFamily, CloseQuarter);\n"
            + "q = foreach q generate ProductFamily, CloseQuarter, sum(ARR) as total_arr;\n"
            + "q = order q by ProductFamily asc;"
        ),
        # ═══ V2 Phase 6: Bullet Charts ═══
        "s_bullet_ilf": sq(
            L
            + FY26
            + LAND
            + UF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as actual, 5000000 as target;"
        ),
        "s_bullet_alf": sq(
            L
            + FY26
            + EXPAND
            + UF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as actual, 3000000 as target;"
        ),
        "s_bullet_retention": sq(
            L
            + FY26
            + RENEWAL
            + CLOSED
            + UF
            + RF
            + 'q = foreach q generate (case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_won) / count()) * 100 as retention_rate, 95 as target;"
        ),
        # ═══ V2 Phase 8: Statistical Analysis ═══
        "s_stat_motion_wr": sq(
            L
            + FY26
            + CLOSED
            + UF
            + RF
            + "q = group q by Type;\n"
            + "q = foreach q generate Type, count() as total, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as won, '
            + '(sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100 as win_rate;\n'
            + "q = order q by win_rate desc;"
        ),
        # ═══ V2 Phase 9: Python-precomputed viz ═══
        "s_gini_display": sq(
            L
            + FY26
            + UF
            + TF
            + QF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate avg(GiniIndex) as gini_value;"
        ),
        "s_cohort_arr": sq(
            L
            + FY26
            + WON
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by CohortQuarter != "";\n'
            + "q = group q by CohortQuarter;\n"
            + "q = foreach q generate CohortQuarter, sum(ARR) as cohort_arr, count() as deal_count;\n"
            + "q = order q by CohortQuarter asc;"
        ),
        # ═══ V2 Phase 7: Choropleth ═══
        "s_geo_won": sq(
            'q = load "Opp_Geo_Map";\n'
            + "q = group q by Country;\n"
            + "q = foreach q generate Country, sum(won_arr) as total_won;\n"
            + "q = order q by total_won desc;"
        ),
        # ═══ Win/Loss by Competitor Detail ═══
        # Hbar: Loss count by Competitor
        "s_competitor_loss": sq(
            L
            + FY26
            + CLOSED
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by IsWon == "false";\n'
            + 'q = filter q by Competitor != "";\n'
            + "q = group q by Competitor;\n"
            + "q = foreach q generate Competitor, count() as lost_count, "
            + "sum(ARR) as lost_arr;\n"
            + "q = order q by lost_arr desc;\n"
            + "q = limit q 15;"
        ),
        # Competitor win rate comparison
        "s_competitor_winrate": sq(
            L
            + FY26
            + CLOSED
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by Competitor != "";\n'
            + "q = group q by Competitor;\n"
            + "q = foreach q generate Competitor, count() as total, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as won, '
            + 'sum(case when IsWon == "false" then 1 else 0 end) as lost, '
            + "sum(ARR) as total_arr, "
            + '(sum(case when IsWon == "true" then 1 else 0 end) * 100 / count()) as win_rate;\n'
            + "q = order q by total desc;\n"
            + "q = limit q 15;"
        ),
        # Competitor losses by deal type
        "s_competitor_by_type": sq(
            L
            + FY26
            + CLOSED
            + UF
            + QF
            + RF
            + 'q = filter q by IsWon == "false";\n'
            + 'q = filter q by Competitor != "";\n'
            + "q = group q by (Type, Competitor);\n"
            + "q = foreach q generate Type, Competitor, count() as cnt, sum(ARR) as lost_arr;\n"
            + "q = order q by Type asc, lost_arr desc;"
        ),
        # Top lost deals to competitors (detail table)
        "s_competitor_detail": sq(
            L
            + FY26
            + CLOSED
            + UF
            + TF
            + QF
            + RF
            + 'q = filter q by IsWon == "false";\n'
            + 'q = filter q by Competitor != "";\n'
            + "q = foreach q generate Id, Name, AccountName, Competitor, Type, "
            + "ARR, WonLostReason, SalesRegion;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 25;"
        ),
        # ═══ V2 Phase 10: Bubble Chart ═══
        "s_bubble_motion": sq(
            L
            + FY26
            + CLOSED
            + UF
            + TF
            + QF
            + RF
            + "q = group q by Type;\n"
            + "q = foreach q generate Type, "
            + "count() as deal_count, "
            + "avg(ARR) as avg_arr, "
            + "avg(SalesCycleDuration) as avg_cycle, "
            + "sum(ARR) as total_arr;\n"
            + "q = order q by total_arr desc;"
        ),
        # ═══ V2 Phase 10: Statistical Analysis ═══
        "s_stat_arr_percentiles": sq(
            L
            + FY26
            + WON
            + UF
            + TF
            + QF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as cnt, "
            + "avg(ARR) as mean_arr, "
            + "stddev(ARR) as std_dev, "
            + "percentile_disc(0.25) within group (order by ARR) as p25, "
            + "percentile_disc(0.50) within group (order by ARR) as median_arr, "
            + "percentile_disc(0.75) within group (order by ARR) as p75;"
        ),
        "s_stat_cycle_percentiles": sq(
            L
            + FY26
            + WON
            + UF
            + TF
            + QF
            + RF
            + "q = group q by Type;\n"
            + "q = foreach q generate Type, "
            + "count() as cnt, "
            + "avg(SalesCycleDuration) as avg_cycle, "
            + "stddev(SalesCycleDuration) as std_cycle, "
            + "percentile_disc(0.50) within group (order by SalesCycleDuration) as median_cycle, "
            + "percentile_disc(0.90) within group (order by SalesCycleDuration) as p90_cycle;\n"
            + "q = order q by avg_cycle desc;"
        ),
        # ═══ VIZ UPGRADE: Motion → Stage Flow Sankey ═══
        "s_motion_flow": sq(
            f'q1 = load "{DS}";\n'
            + "q1 = filter q1 by FiscalYear == 2026;\n"
            + UF.replace("q ", "q1 ").replace("q by", "q1 by")
            + RF.replace("q ", "q1 ").replace("q by", "q1 by")
            + 'q1 = filter q1 by IsLand == "true";\n'
            + "q1 = group q1 by StageName;\n"
            + 'q1 = foreach q1 generate "Land" as source, StageName as target, sum(ARR) as arr;\n'
            + f'q2 = load "{DS}";\n'
            + "q2 = filter q2 by FiscalYear == 2026;\n"
            + UF.replace("q ", "q2 ").replace("q by", "q2 by")
            + RF.replace("q ", "q2 ").replace("q by", "q2 by")
            + 'q2 = filter q2 by IsExpand == "true";\n'
            + "q2 = group q2 by StageName;\n"
            + 'q2 = foreach q2 generate "Expand" as source, StageName as target, sum(ARR) as arr;\n'
            + f'q3 = load "{DS}";\n'
            + "q3 = filter q3 by FiscalYear == 2026;\n"
            + UF.replace("q ", "q3 ").replace("q by", "q3 by")
            + RF.replace("q ", "q3 ").replace("q by", "q3 by")
            + 'q3 = filter q3 by IsRenewal == "true";\n'
            + "q3 = group q3 by StageName;\n"
            + 'q3 = foreach q3 generate "Renewal" as source, StageName as target, sum(ARR) as arr;\n'
            + "q = union q1, q2, q3;\n"
            + "q = order q by source asc, arr desc;"
        ),
        # ═══ VIZ UPGRADE: NRR Bridge Waterfall ═══
        "s_nrr_bridge": sq(
            L
            + FY26
            + UF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + 'sum(case when IsRenewal == "true" && IsWon == "true" then ARR else 0 end) as renewed_arr, '
            + 'sum(case when IsExpand == "true" && IsWon == "true" then ARR else 0 end) as expanded_arr, '
            + 'sum(case when IsClosed == "true" && IsWon == "false" && IsRenewal == "true" then ARR else 0 end) as churned_arr, '
            + 'sum(case when IsLand == "true" && IsWon == "true" then ARR else 0 end) as new_arr;'
        ),
        # ═══ VIZ UPGRADE: Competitor × Segment Heatmap ═══
        "s_competitor_segment": sq(
            L
            + FY26
            + CLOSED
            + UF
            + RF
            + 'q = filter q by CompetitorName != "" && CompetitorName != "null";\n'
            + "q = group q by (CompetitorName, UnitGroup);\n"
            + "q = foreach q generate CompetitorName, UnitGroup, "
            + "count() as deal_count, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as won_count, '
            + '(sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100 as win_rate;\n'
            + "q = order q by deal_count desc;"
        ),
        # ═══ VIZ UPGRADE: Dynamic KPI Thresholds ═══
        "s_rm_kpi_thresh": sq(
            L
            + FY26
            + UF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + '(sum(case when IsRenewal == "true" && IsWon == "true" then ARR else 0 end) * 100 / '
            + 'case when sum(case when IsRenewal == "true" then ARR else 0 end) > 0 then '
            + 'sum(case when IsRenewal == "true" then ARR else 0 end) else 1 end) as renewal_rate, '
            + '(sum(case when IsClosed == "true" && IsWon == "false" && IsRenewal == "true" then ARR else 0 end) * 100 / '
            + 'case when sum(case when IsRenewal == "true" then ARR else 0 end) > 0 then '
            + 'sum(case when IsRenewal == "true" then ARR else 0 end) else 1 end) as churn_rate;'
        ),
        # ═══ PAGE 9: ARR Bridge & Growth (ML-Forward) ═══
        # ARR bridge waterfall: motion-based decomposition
        "s_arr_bridge": sq(
            L
            + UF
            + RF
            + "q = filter q by FiscalYear == 2026;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(case when IsRenewal == \"true\" && IsWon == \"true\" then ARR else 0 end) as renewed_arr, "
            + "sum(case when IsExpand == \"true\" && IsWon == \"true\" then ARR else 0 end) as expanded_arr, "
            + "sum(case when IsLand == \"true\" && IsWon == \"true\" then ARR else 0 end) as new_arr, "
            + "sum(case when IsClosed == \"true\" && IsWon == \"false\" && IsRenewal == \"true\" then ARR else 0 end) as churned_arr, "
            + "sum(case when IsWon == \"true\" then ARR else 0 end) as total_won;"
        ),
        # Growth rate by product × region
        "s_growth_prod_region": sq(
            L
            + UF
            + "q = filter q by IsClosed == \"true\";\n"
            + "q = group q by (ProductFamily, SalesRegion);\n"
            + "q = foreach q generate ProductFamily, SalesRegion, "
            + "sum(case when IsWon == \"true\" then ARR else 0 end) as won_arr, "
            + "(sum(case when IsWon == \"true\" then 1 else 0 end) * 100 / count()) as win_rate, "
            + "count() as deal_count;\n"
            + "q = order q by ProductFamily asc, SalesRegion asc;"
        ),
        # Expected churn: at-risk renewal pipeline
        "s_expected_churn": sq(
            L
            + UF
            + RF
            + "q = filter q by IsAtRisk == \"true\" || RiskOfTermination != \"\";\n"
            + "q = group q by SalesRegion;\n"
            + "q = foreach q generate SalesRegion, "
            + "sum(ARR) as at_risk_arr, "
            + "count() as at_risk_count;\n"
            + "q = order q by at_risk_arr desc;"
        ),
        # Net ARR bridge KPIs
        "s_arr_bridge_kpi": sq(
            L
            + UF
            + RF
            + "q = filter q by FiscalYear == 2026;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(case when IsWon == \"true\" then ARR else 0 end) as total_won, "
            + "sum(case when IsLand == \"true\" && IsWon == \"true\" then ARR else 0 end) as new_arr, "
            + "sum(case when IsExpand == \"true\" && IsWon == \"true\" then ARR else 0 end) as expand_arr, "
            + "sum(case when IsClosed == \"true\" && IsWon == \"false\" && IsRenewal == \"true\" then ARR else 0 end) as churn_arr;"
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Widgets
# ═══════════════════════════════════════════════════════════════════════════

# Page names for nav links
PAGE_NAMES = ["motions", "renewals", "conversion", "churn", "product", "competitive"]
PAGE_LABELS = ["Motions", "Renewals", "Conversion", "Churn", "Product", "Competitive"]

# Combo config reused across ILF/ALF/SaaS combos
PIPE_WON_COMBO = {
    "plotConfiguration": [
        {"series": "pipeline", "chartType": "column"},
        {"series": "closed_won", "chartType": "line"},
    ]
}

PIPE_CUMUL_COMBO = {
    "plotConfiguration": [
        {"series": "pipe_arr", "chartType": "column"},
        {"series": "cumul_arr", "chartType": "line"},
    ]
}

SAAS_COMBO = {
    "plotConfiguration": [
        {"series": "saas_arr", "chartType": "column"},
        {"series": "non_saas_arr", "chartType": "line"},
    ]
}

# Win rate gauge bands per motion type
WR_BANDS_MOTION = [
    {"start": 0, "stop": 15, "color": "#D4504C"},
    {"start": 15, "stop": 30, "color": "#FFB75D"},
    {"start": 30, "stop": 100, "color": "#04844B"},
]
WR_BANDS_RENEWAL = [
    {"start": 0, "stop": 70, "color": "#D4504C"},
    {"start": 70, "stop": 90, "color": "#FFB75D"},
    {"start": 90, "stop": 100, "color": "#04844B"},
]
SAAS_BANDS = [
    {"start": 0, "stop": 15, "color": "#D4504C"},
    {"start": 15, "stop": 30, "color": "#FFB75D"},
    {"start": 30, "stop": 100, "color": "#04844B"},
]
RISK_BANDS = [
    {"start": 0, "stop": 10, "color": "#04844B"},
    {"start": 10, "stop": 25, "color": "#FFB75D"},
    {"start": 25, "stop": 100, "color": "#D4504C"},
]


def _page_nav(page_idx):
    """Return nav link widgets for all 6 pages, marking page_idx as active."""
    widgets = {}
    prefix = f"p{page_idx + 1}"
    for i, (pname, plabel) in enumerate(zip(PAGE_NAMES, PAGE_LABELS)):
        widgets[f"{prefix}_nav{i + 1}"] = nav_link(
            pname, plabel, active=(i == page_idx)
        )
    return widgets


def build_widgets():
    w = {}

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 1 — Pipeline by Motion
    # ═══════════════════════════════════════════════════════════════════
    w.update(_page_nav(0))
    w["p1_hdr"] = hdr(
        "Pipeline by Motion",
        "FY2026 | Land / Expand / Renewal | All values in EUR",
    )
    # Filter bar
    w["p1_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p1_f_type"] = pillbox("f_type", "Type")
    w["p1_f_qtr"] = pillbox("f_qtr", "Quarter")
    w["p1_f_region"] = pillbox("f_region", "Region")
    # KPI tiles (Phase 3: YoY trend indicators — FY2026 vs FY2025)
    w["p1_land_arr"] = num_with_trend(
        "s_land_pipe_t", "sum_acv", "Land Pipeline (EUR)", "#0070D2", compact=True
    )
    w["p1_expand_arr"] = num_with_trend(
        "s_expand_pipe_t", "sum_acv", "Expand Pipeline (EUR)", "#FF6600", compact=True
    )
    w["p1_renewal_arr"] = num_with_trend(
        "s_renewal_pipe_t", "sum_acv", "Renewal Pipeline (EUR)", "#04844B", compact=True
    )
    # Funnel
    w["p1_sec_funnel"] = section_label("Pipeline Flow")
    w["p1_funnel"] = funnel_chart(
        "s_motion_funnel", "Pipeline by Motion (ARR)", "Type", "sum_arr"
    )
    # Waterfall: QoQ pipeline change
    w["p1_waterfall"] = waterfall_chart(
        "s_motion_waterfall",
        "QoQ Pipeline Change",
        "Quarter",
        "net_change",
    )
    # Combo: Pipeline by Qtr + cumulative
    w["p1_sec_qtr"] = section_label("Quarterly Pipeline")
    w["p1_combo_qtr"] = rich_chart(
        "s_qtr_pipe_cumul",
        "column",
        "Quarterly Pipeline ARR",
        ["Quarter"],
        ["pipe_arr"],
        axis_title="ARR (EUR)",
    )
    # Gauges: Win Rate by Type
    w["p1_sec_wr"] = section_label("Win Rate by Motion")
    w["p1_wr_land"] = gauge(
        "s_wr_land",
        "win_rate",
        "Land Win Rate %",
        min_val=0,
        max_val=100,
        bands=WR_BANDS_MOTION,
    )
    w["p1_wr_expand"] = gauge(
        "s_wr_expand",
        "win_rate",
        "Expand Win Rate %",
        min_val=0,
        max_val=100,
        bands=WR_BANDS_MOTION,
    )
    w["p1_wr_renewal"] = gauge(
        "s_wr_renewal",
        "win_rate",
        "Renewal Win Rate %",
        min_val=0,
        max_val=100,
        bands=WR_BANDS_RENEWAL,
    )
    # Hbar: Sales Cycle by Type
    w["p1_cycle"] = rich_chart(
        "s_cycle_type",
        "hbar",
        "Avg Sales Cycle by Type (Days)",
        ["Type"],
        ["avg_cycle"],
        axis_title="Days",
    )

    # YoY Motion Comparison
    w["p1_sec_yoy"] = section_label("Year-over-Year Motion Comparison")
    w["p1_yoy_land"] = rich_chart(
        "s_land_yoy",
        "area",
        "Land ARR by Quarter (YoY)",
        ["Quarter"],
        ["sum_arr"],
        split=["FY"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    w["p1_yoy_expand"] = rich_chart(
        "s_expand_yoy",
        "area",
        "Expand ARR by Quarter (YoY)",
        ["Quarter"],
        ["sum_arr"],
        split=["FY"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    w["p1_yoy_renewal"] = rich_chart(
        "s_renewal_yoy",
        "area",
        "Renewal ARR by Quarter (YoY)",
        ["Quarter"],
        ["sum_arr"],
        split=["FY"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 2 — Renewals (RW 1.8)
    # ═══════════════════════════════════════════════════════════════════
    w.update(_page_nav(1))
    w["p2_hdr"] = hdr(
        "Renewals (RW 1.8)",
        "FY2026 | Renewal pipeline, indexation, at-risk analysis",
    )
    # Filter bar
    w["p2_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p2_f_type"] = pillbox("f_type", "Type")
    w["p2_f_qtr"] = pillbox("f_qtr", "Quarter")
    w["p2_f_region"] = pillbox("f_region", "Region")
    # Stackcolumn: Renewals per Qtr by term length
    w["p2_sec_term"] = section_label("Renewals by Term Length")
    w["p2_ren_term"] = rich_chart(
        "s_ren_qtr_term",
        "stackcolumn",
        "Renewal ARR by Quarter & Term",
        ["Quarter"],
        ["sum_arr"],
        split=["TermBucket"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    # Area: Renewals by month YoY
    w["p2_ren_yoy"] = rich_chart(
        "s_ren_month_yoy",
        "area",
        "Renewal ARR by Month (YoY)",
        ["CloseMonth"],
        ["sum_arr"],
        split=["FYLabel"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    # Waterfall: Existing ARR by FY
    w["p2_sec_index"] = section_label("Indexation & Growth")
    w["p2_ren_fy_wf"] = waterfall_chart(
        "s_ren_fy_waterfall",
        "Renewal Won ARR by Fiscal Year",
        "FYLabel",
        "sum_arr",
    )
    # Line: Indexation growth quarterly
    w["p2_ren_index"] = rich_chart(
        "s_ren_qtr_index",
        "line",
        "Renewal Won ARR by Quarter",
        ["Quarter"],
        ["sum_arr"],
        axis_title="ARR (EUR)",
    )
    # Comparisontable: Renewals at risk
    w["p2_sec_risk"] = section_label("At-Risk Renewals")
    w["p2_ren_risk_tbl"] = rich_chart(
        "s_ren_at_risk",
        "comparisontable",
        "Top 25 At-Risk Renewals",
        ["Name", "AccountName", "RiskOfTermination", "CloseQuarter", "StageName"],
        ["ARR"],
    )
    # Gauge: Renewal win rate
    w["p2_ren_wr"] = gauge(
        "s_ren_wr",
        "win_rate",
        "Renewal Win Rate %",
        min_val=0,
        max_val=100,
        bands=WR_BANDS_RENEWAL,
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 3 — Conversion / ILF / ALF (RW 1.9)
    # ═══════════════════════════════════════════════════════════════════
    w.update(_page_nav(2))
    w["p3_hdr"] = hdr(
        "Conversion / ILF / ALF (RW 1.9)",
        "FY2026 | Initial Land Flow, Account Land Flow, Cross-Sell",
    )
    # Filter bar
    w["p3_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p3_f_type"] = pillbox("f_type", "Type")
    w["p3_f_qtr"] = pillbox("f_qtr", "Quarter")
    w["p3_f_region"] = pillbox("f_region", "Region")
    # Combo: ILF (Land) ARR by Qtr
    w["p3_sec_ilf"] = section_label("Initial Land Flow (ILF)")
    w["p3_ilf_combo"] = rich_chart(
        "s_ilf_qtr",
        "combo",
        "Land ARR by Quarter: Pipeline vs Won",
        ["Quarter"],
        ["pipeline", "closed_won"],
        show_legend=True,
        axis_title="ARR (EUR)",
        combo_config=PIPE_WON_COMBO,
    )
    # Combo: ALF (Expand) ARR by Qtr
    w["p3_sec_alf"] = section_label("Account Land Flow (ALF)")
    w["p3_alf_combo"] = rich_chart(
        "s_alf_qtr",
        "combo",
        "Expand ARR by Quarter: Pipeline vs Won",
        ["Quarter"],
        ["pipeline", "closed_won"],
        show_legend=True,
        axis_title="ARR (EUR)",
        combo_config=PIPE_WON_COMBO,
    )
    # Stackcolumn: New customers by month x region
    w["p3_sec_new"] = section_label("New Customer Acquisition")
    w["p3_new_cust"] = rich_chart(
        "s_new_cust_region",
        "stackcolumn",
        "New Customers by Month & Region",
        ["CloseMonth"],
        ["cnt"],
        split=["SalesRegion"],
        show_legend=True,
        axis_title="Count",
    )
    # Waterfall: Cross-sell ARR quarterly adds
    w["p3_cross_sell"] = waterfall_chart(
        "s_cross_sell_wf",
        "Axioma Cross-Sell ARR (Quarterly)",
        "Quarter",
        "sum_arr",
    )
    # Number: Synergy tiles (KPIs 25/26: show count, target 10/qtr won, 30/qtr pipe)
    w["p3_sec_synergy"] = section_label("SaaS Synergy (Land + SaaS Client)")
    w["p3_synergy_won"] = num("s_synergy_won", "cnt", "Synergy Won (Count)", "#04844B")
    w["p3_synergy_won_arr"] = num(
        "s_synergy_won", "sum_arr", "Synergy Won ARR", "#04844B", True, 28
    )
    w["p3_synergy_pipe"] = num(
        "s_synergy_pipe", "cnt", "Synergy Pipeline (Count)", "#0070D2"
    )
    w["p3_synergy_pipe_arr"] = num(
        "s_synergy_pipe", "sum_arr", "Synergy Pipeline ARR", "#0070D2", True, 28
    )
    # Funnel: Land -> Expand
    w["p3_le_funnel"] = funnel_chart(
        "s_land_expand_funnel",
        "Land to Expand Conversion",
        "Type",
        "sum_arr",
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 4 — Cancellation & Churn (RW 1.10)
    # ═══════════════════════════════════════════════════════════════════
    w.update(_page_nav(3))
    w["p4_hdr"] = hdr(
        "Cancellation & Churn (RW 1.10)",
        "FY2026 | Lost ARR, risk distribution, net retention",
    )
    # Filter bar
    w["p4_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p4_f_type"] = pillbox("f_type", "Type")
    w["p4_f_qtr"] = pillbox("f_qtr", "Quarter")
    w["p4_f_region"] = pillbox("f_region", "Region")
    # Stackcolumn: Lost ARR by Qtr + reason
    w["p4_sec_lost"] = section_label("Lost ARR Analysis")
    w["p4_lost_qtr"] = rich_chart(
        "s_lost_qtr_reason",
        "stackcolumn",
        "Lost ARR by Quarter & Reason",
        ["Quarter"],
        ["sum_arr"],
        split=["WonLostReason"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    # Donut: Risk level distribution
    w["p4_sec_risk"] = section_label("Risk Distribution")
    w["p4_risk_donut"] = rich_chart(
        "s_risk_dist",
        "donut",
        "Renewal Risk Distribution",
        ["RiskOfTermination"],
        ["sum_arr"],
        show_legend=True,
        show_pct=True,
    )
    # Gauge: % of pipe at risk
    w["p4_risk_gauge"] = gauge(
        "s_pct_at_risk",
        "pct_at_risk",
        "% Renewal Pipeline At Risk",
        min_val=0,
        max_val=100,
        bands=RISK_BANDS,
    )
    # Comparisontable: At-risk deals
    w["p4_sec_deals"] = section_label("At-Risk Deals")
    w["p4_risk_tbl"] = rich_chart(
        "s_at_risk_list",
        "comparisontable",
        "Top 25 At-Risk Deals",
        [
            "Name",
            "AccountName",
            "Type",
            "RiskOfTermination",
            "StageName",
            "CloseQuarter",
        ],
        ["ARR"],
    )
    # Area: Churn trend
    w["p4_sec_churn"] = section_label("Churn Trend")
    w["p4_churn_area"] = rich_chart(
        "s_churn_month",
        "area",
        "Renewal Losses by Month",
        ["CloseMonth"],
        ["lost_arr"],
        axis_title="ARR (EUR)",
    )
    # Waterfall: Net ARR movement
    w["p4_net_arr"] = waterfall_chart(
        "s_net_arr_qtr",
        "Net Renewal ARR (Won - Lost) by Quarter",
        "Quarter",
        "net_arr",
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 5 — Product/Pricing (RW 1.14)
    # ═══════════════════════════════════════════════════════════════════
    w.update(_page_nav(4))
    w["p5_hdr"] = hdr(
        "Product & Pricing (RW 1.14)",
        "FY2026 | SaaS penetration, product mix, pricing trends",
    )
    # Filter bar
    w["p5_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p5_f_type"] = pillbox("f_type", "Type")
    w["p5_f_qtr"] = pillbox("f_qtr", "Quarter")
    w["p5_f_region"] = pillbox("f_region", "Region")
    # Combo: SaaS ARR by Qtr
    w["p5_sec_saas"] = section_label("SaaS Pipeline")
    w["p5_saas_combo"] = rich_chart(
        "s_saas_qtr",
        "combo",
        "SaaS vs Non-SaaS Pipeline by Quarter",
        ["Quarter"],
        ["saas_arr", "non_saas_arr"],
        show_legend=True,
        axis_title="ARR (EUR)",
        combo_config=SAAS_COMBO,
    )
    # Gauge: SaaS penetration %
    w["p5_saas_gauge"] = gauge(
        "s_saas_pct",
        "saas_pct",
        "SaaS Penetration %",
        min_val=0,
        max_val=100,
        bands=SAAS_BANDS,
    )
    # Stackhbar: Product by UnitGroup x SaaS
    w["p5_sec_product"] = section_label("Product Mix")
    w["p5_unit_saas"] = rich_chart(
        "s_unit_saas",
        "stackhbar",
        "Unit Group ARR: SaaS vs Non-SaaS",
        ["UnitGroup"],
        ["sum_arr"],
        split=["SaaSClient"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    # Number tiles: SaaS won, total won, SaaS %
    w["p5_sec_kpi"] = section_label("Won ARR Breakdown")
    w["p5_saas_won"] = num("s_saas_won", "sum_arr", "SaaS Won ARR", "#04844B", True, 28)
    w["p5_saas_pct_num"] = num(
        "s_saas_won_pct", "saas_pct", "SaaS % of Won", "#9050E9", False, 28
    )

    # ═══ PS ARR & ONE-OFF REVENUE (RW 1.14 gaps) ═══
    w["p5_sec_ps"] = section_label("Professional Services & One-Off Revenue")
    w["p5_ps_gauge"] = gauge(
        "s_ps_attach",
        "attach_rate",
        "PS Attach Rate %",
        min_val=0,
        max_val=30,
        bands=[
            {"start": 0, "stop": 5, "color": "#D4504C"},
            {"start": 5, "stop": 15, "color": "#FFB75D"},
            {"start": 15, "stop": 30, "color": "#04844B"},
        ],
    )
    w["p5_ps_qtr"] = rich_chart(
        "s_ps_qtr",
        "combo",
        "PS Pipeline vs Won by Quarter",
        ["Quarter"],
        ["won_arr", "pipe_arr"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    w["p5_oneoff_qtr"] = rich_chart(
        "s_oneoff_qtr",
        "column",
        "One-Off Revenue (PS) by Quarter & FY",
        ["Quarter"],
        ["sum_arr"],
        split=["FY"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    # ═══ SaaS YoY GROWTH (RW 1.14 gap) ═══
    w["p5_sec_yoy"] = section_label("SaaS YoY Growth")
    w["p5_saas_yoy"] = rich_chart(
        "s_saas_yoy",
        "column",
        "SaaS Won ARR by Fiscal Year",
        ["FY"],
        ["sum_arr"],
        axis_title="ARR (EUR)",
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 6 — Competitive Intelligence
    # ═══════════════════════════════════════════════════════════════════
    w.update(_page_nav(5))
    w["p6_hdr"] = hdr(
        "Competitive Intelligence",
        "FY2026 | Win/Loss Analysis by Competitor",
    )
    # Filter bar
    w["p6_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p6_f_type"] = pillbox("f_type", "Type")
    w["p6_f_qtr"] = pillbox("f_qtr", "Quarter")
    w["p6_f_region"] = pillbox("f_region", "Region")
    # Hbar: Top competitors by deal count
    w["p6_sec_comp"] = section_label("Competitor Frequency")
    w["p6_ch_comp_freq"] = rich_chart(
        "s_comp_frequency",
        "hbar",
        "Top Competitors by Deal Count",
        ["Competitor"],
        ["cnt"],
        axis_title="Deal Count",
    )
    # Comparisontable: Win rate vs competitors
    w["p6_sec_wr"] = section_label("Win Rate vs Competitors")
    w["p6_ch_comp_wr"] = rich_chart(
        "s_comp_wr",
        "comparisontable",
        "Win Rate vs Competitors",
        ["Competitor"],
        ["total", "won_cnt", "win_rate"],
    )
    # Hbar: Win/loss reason distribution
    w["p6_sec_reason"] = section_label("Win/Loss Reasons")
    w["p6_ch_reason"] = rich_chart(
        "s_reason_dist",
        "hbar",
        "Win/Loss Reasons",
        ["WonLostReason"],
        ["cnt"],
        axis_title="Count",
    )
    # Stackhbar: Reasons by Won vs Lost
    w["p6_ch_reason_wl"] = rich_chart(
        "s_reason_won_lost",
        "stackhbar",
        "Reasons by Won vs Lost",
        ["WonLostReason"],
        ["cnt"],
        split=["IsWon"],
        show_legend=True,
        axis_title="Count",
    )
    # Hbar: Loss sub-reasons by ARR impact
    w["p6_sec_sub"] = section_label("Loss Sub-Reasons")
    w["p6_ch_sub_reason"] = rich_chart(
        "s_sub_reason",
        "hbar",
        "Loss Sub-Reasons by ARR Impact",
        ["SubReason"],
        ["lost_arr"],
        axis_title="Lost ARR (EUR)",
    )

    # ═══ Win/Loss by Competitor Detail (Enhancement 11) ═══
    w["p6_sec_comp_loss"] = section_label("Competitor Loss Impact")
    w["p6_ch_comp_loss"] = rich_chart(
        "s_competitor_loss",
        "hbar",
        "Top 15 Competitors by Lost ARR",
        ["Competitor"],
        ["lost_arr", "lost_count"],
        show_legend=True,
        axis_title="Lost ARR (EUR)",
    )
    w["p6_sec_comp_wr2"] = section_label("Competitor Win Rate Analysis")
    w["p6_tbl_comp_wr"] = rich_chart(
        "s_competitor_winrate",
        "comparisontable",
        "Win Rate vs Competitors (Detailed)",
        ["Competitor"],
        ["total", "won", "lost", "total_arr", "win_rate"],
    )
    w["p6_sec_comp_type"] = section_label("Losses by Type × Competitor")
    w["p6_ch_comp_type"] = rich_chart(
        "s_competitor_by_type",
        "stackhbar",
        "Competitor Losses by Deal Type",
        ["Type"],
        ["lost_arr"],
        split=["Competitor"],
        show_legend=True,
        axis_title="Lost ARR (EUR)",
    )
    w["p6_sec_comp_detail"] = section_label("Top 25 Lost Deals to Competitors")
    w["p6_tbl_comp_detail"] = rich_chart(
        "s_competitor_detail",
        "comparisontable",
        "Lost Deal Detail by Competitor",
        ["Name", "AccountName", "Competitor", "Type", "WonLostReason", "SalesRegion"],
        ["ARR"],
    )

    # ═══ ITERATION 4: New KPI widgets ═══
    # KPI 27: Lost ARR % gauge (Page 4)
    w["p4_lost_pct"] = gauge(
        "s_lost_pct",
        "lost_pct",
        "Lost ARR % of Closed",
        min_val=0,
        max_val=30,
        bands=[
            {"start": 0, "stop": 5, "color": "#04844B"},
            {"start": 5, "stop": 15, "color": "#FFB75D"},
            {"start": 15, "stop": 30, "color": "#D4504C"},
        ],
    )
    # KPI 23: New customer ARR by month (combo with count + ARR)
    w["p3_ch_new_value"] = rich_chart(
        "s_new_cust_value",
        "combo",
        "New Customer Wins: Count & ARR by Month",
        ["CloseMonth"],
        ["cnt", "total_arr"],
        show_legend=True,
        axis_title="Count / ARR",
        combo_config={
            "plotConfiguration": [
                {"series": "cnt", "chartType": "column"},
                {"series": "total_arr", "chartType": "line"},
            ],
        },
    )

    # ── Phase 6: Reference lines ──────────────────────────────────────────
    from crm_analytics_helpers import add_reference_line

    add_reference_line(w["p3_ilf_combo"], 5000000, "€5M Target", "#D4504C", "dashed")
    add_reference_line(w["p3_alf_combo"], 3000000, "€3M Target", "#D4504C", "dashed")
    add_reference_line(w["p3_cross_sell"], 2000000, "€2M Target", "#04844B", "dashed")

    # ── Phase 7: Embedded table actions ──────────────────────────────────
    from crm_analytics_helpers import add_table_action

    add_table_action(w["p2_ren_risk_tbl"], "salesforceActions", "Opportunity", "Id")
    add_table_action(w["p4_risk_tbl"], "salesforceActions", "Opportunity", "Id")
    add_table_action(w["p6_tbl_comp_detail"], "salesforceActions", "Opportunity", "Id")

    # ═══ V2 PAGE 7: Advanced Analytics ═══
    w["p7_nav1"] = nav_link("motions", "Motions")
    w["p7_nav2"] = nav_link("renewals", "Renewals")
    w["p7_nav3"] = nav_link("conversion", "Conversion")
    w["p7_nav4"] = nav_link("churn", "Churn")
    w["p7_nav5"] = nav_link("product", "Product")
    w["p7_nav6"] = nav_link("competitive", "Competitive")
    w["p7_nav7"] = nav_link("advanalytics", "Advanced", active=True)
    w["p7_hdr"] = hdr(
        "Advanced Analytics",
        "Revenue Flow | Composition | Product Heatmap",
    )
    w["p7_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p7_f_type"] = pillbox("f_type", "Type")
    w["p7_f_qtr"] = pillbox("f_qtr", "Quarter")
    w["p7_f_region"] = pillbox("f_region", "Region")
    # Sankey: Revenue flow Type → Product
    w["p7_sec_sankey"] = section_label("Revenue Flow: Type → Product Family")
    w["p7_ch_sankey"] = sankey_chart("s_sankey_flow", "Won Revenue: Motion → Product")
    # Treemap: Won ARR by UnitGroup → Type
    w["p7_sec_treemap"] = section_label("Revenue Composition")
    w["p7_ch_treemap"] = treemap_chart(
        "s_treemap_won",
        "Won ARR by Unit Group & Type",
        ["UnitGroup", "Type"],
        "total_arr",
    )
    # Stacked area: Revenue by motion over time
    w["p7_sec_area"] = section_label("Revenue Trend by Motion")
    w["p7_ch_area"] = area_chart(
        "s_area_motion",
        "Monthly Won ARR by Type",
        stacked=True,
        show_legend=True,
        axis_title="EUR",
    )
    # Heatmap: Product × Quarter
    w["p7_sec_heatmap"] = section_label("Product × Quarter Revenue Matrix")
    w["p7_ch_heatmap"] = heatmap_chart(
        "s_heatmap_prod_qtr", "Revenue by Product Family × Quarter"
    )
    # Choropleth: Won revenue by country
    w["p7_sec_geo"] = section_label("Won Revenue by Country")
    w["p7_ch_geo"] = choropleth_chart(
        "s_geo_won", "Won ARR by Country", "Country", "total_won"
    )

    # ═══ V2 PAGE 8: Bullet Charts & Statistical Analysis ═══
    w["p8_nav1"] = nav_link("motions", "Motions")
    w["p8_nav2"] = nav_link("renewals", "Renewals")
    w["p8_nav3"] = nav_link("conversion", "Conversion")
    w["p8_nav4"] = nav_link("churn", "Churn")
    w["p8_nav5"] = nav_link("product", "Product")
    w["p8_nav6"] = nav_link("competitive", "Competitive")
    w["p8_nav7"] = nav_link("advanalytics", "Advanced")
    w["p8_nav8"] = nav_link("revstats", "Statistics", active=True)
    w["p8_hdr"] = hdr(
        "Revenue Statistical Analysis",
        "Target vs Actual | Revenue Concentration | Win Rate by Motion",
    )
    w["p8_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p8_f_type"] = pillbox("f_type", "Type")
    w["p8_f_qtr"] = pillbox("f_qtr", "Quarter")
    w["p8_f_region"] = pillbox("f_region", "Region")
    # Bullet charts
    w["p8_sec_bullet"] = section_label("Target vs Actual KPIs")
    w["p8_bullet_ilf"] = bullet_chart(
        "s_bullet_ilf", "ILF ARR (Target: €5M)", axis_title="EUR"
    )
    w["p8_bullet_alf"] = bullet_chart(
        "s_bullet_alf", "ALF ARR (Target: €3M)", axis_title="EUR"
    )
    w["p8_bullet_retention"] = bullet_chart(
        "s_bullet_retention", "Renewal Retention (Target: 95%)", axis_title="%"
    )
    # Stats: Win rate by motion type
    w["p8_sec_motion_wr"] = section_label("Win Rate by Revenue Motion")
    w["p8_stat_motion_wr"] = rich_chart(
        "s_stat_motion_wr",
        "hbar",
        "Win Probability by Type (Land/Expand/Renewal)",
        ["Type"],
        ["win_rate"],
        axis_title="Win Rate %",
    )

    # Phase 9: Python-precomputed widgets
    w["p8_sec_gini"] = section_label("Revenue Concentration (Gini Coefficient)")
    w["p8_gini_value"] = num("s_gini_display", "gini_value", "Gini Index", "#4a90d9")
    w["p8_sec_cohort"] = section_label("Won ARR by Acquisition Cohort")
    w["p8_cohort_chart"] = rich_chart(
        "s_cohort_arr",
        "column",
        "Won ARR by Account Cohort Quarter",
        ["CohortQuarter"],
        ["cohort_arr"],
        axis_title="ARR (EUR)",
    )

    # ═══ V2 Phase 10: Bubble chart on p7 ═══
    w["p7_sec_bubble"] = section_label("Deal Size vs Cycle Time by Motion")
    w["p7_ch_bubble"] = bubble_chart(
        "s_bubble_motion",
        "Deal Volume vs Avg Deal Size vs Cycle Time",
    )
    # ═══ V2 Phase 10: Stats on p8 ═══
    w["p8_sec_percentiles"] = section_label("ARR Distribution (Percentiles)")
    w["p8_tbl_percentiles"] = rich_chart(
        "s_stat_arr_percentiles",
        "comparisonTable",
        "Won Deal ARR Percentiles",
        ["cnt", "mean_arr", "std_dev", "p25", "median_arr", "p75"],
        [],
    )
    w["p8_sec_cycle_stats"] = section_label("Sales Cycle Statistics by Motion")
    w["p8_tbl_cycle_stats"] = rich_chart(
        "s_stat_cycle_percentiles",
        "comparisonTable",
        "Cycle Duration Stats (days) by Revenue Motion",
        ["Type", "cnt", "avg_cycle", "std_cycle", "median_cycle", "p90_cycle"],
        [],
    )

    # Add nav7 to pages 1-6
    for px in range(1, 7):
        w[f"p{px}_nav7"] = nav_link("advanalytics", "Advanced")
    # Add nav8 to pages 1-7
    for px in range(1, 8):
        w[f"p{px}_nav8"] = nav_link("revstats", "Statistics")

    # ═══ VIZ UPGRADE: Motion → Stage Flow Sankey ═══
    w["p7_sec_motion_flow"] = section_label("Revenue Motion → Stage Flow")
    w["p7_ch_motion_flow"] = sankey_chart(
        "s_motion_flow", "Deal Flow: Land / Expand / Renewal → Stage",
        source_field="source", target_field="target", measure_field="arr",
    )

    # ═══ VIZ UPGRADE: NRR Bridge Waterfall ═══
    w["p4_sec_nrr_bridge"] = section_label("Net Revenue Retention Bridge")
    w["p4_ch_nrr_bridge"] = rich_chart(
        "s_nrr_bridge",
        "comparisontable",
        "NRR Components: Renewed + Expanded − Churned + New",
        [],
        ["renewed_arr", "expanded_arr", "churned_arr", "new_arr"],
    )

    # ═══ VIZ UPGRADE: Competitor × Segment Heatmap ═══
    w["p6_sec_comp_heat"] = section_label("Competitive Win Rate × Unit Group")
    w["p6_ch_comp_heat"] = heatmap_chart(
        "s_competitor_segment", "Competitor × Segment Win Rate Matrix"
    )

    # ═══ VIZ UPGRADE: Dynamic KPI Tiles ═══
    w["p1_renewal_rate_dynamic"] = num_dynamic_color(
        "s_rm_kpi_thresh",
        "renewal_rate",
        "Renewal Win Rate %",
        thresholds=[(70, "#D4504C"), (90, "#FFB75D"), (100, "#04844B")],
        size=28,
    )
    w["p1_churn_rate_dynamic"] = num_dynamic_color(
        "s_rm_kpi_thresh",
        "churn_rate",
        "Churn Rate %",
        thresholds=[(10, "#04844B"), (25, "#FFB75D"), (100, "#D4504C")],
        size=28,
    )

    # ═══ PAGE 9: ARR Bridge & Growth (ML-Forward) ═══
    w["p9_hdr"] = hdr(
        "ARR Bridge & Growth Analytics",
        "Motion-based ARR decomposition, product × region growth, expected churn",
    )
    w["p9_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p9_f_region"] = pillbox("f_region", "Region")
    # ARR bridge KPIs
    w["p9_kpi_new"] = num("s_arr_bridge_kpi", "new_arr", "New ARR", "#04844B", compact=True)
    w["p9_kpi_expand"] = num("s_arr_bridge_kpi", "expand_arr", "Expansion ARR", "#0070D2", compact=True)
    w["p9_kpi_churn"] = num("s_arr_bridge_kpi", "churn_arr", "Churned ARR", "#D4504C", compact=True)
    # ARR bridge waterfall
    w["p9_sec_bridge"] = section_label("ARR Bridge: Land + Expand − Churn = Net")
    w["p9_ch_bridge"] = waterfall_chart(
        "s_arr_bridge", "FY2026 ARR Bridge by Motion",
        "Component", "arr", axis_label="ARR (EUR)",
    )
    # Growth heatmap
    w["p9_sec_growth"] = section_label("Won ARR by Product × Region")
    w["p9_ch_growth"] = heatmap_chart(
        "s_growth_prod_region", "Won ARR: Product Family × Sales Region"
    )
    # Expected churn by region
    w["p9_sec_churn"] = section_label("Expected Churn by Region")
    w["p9_ch_churn"] = rich_chart(
        "s_expected_churn", "hbar",
        "At-Risk ARR by Region",
        ["SalesRegion"], ["at_risk_arr"],
        axis_title="At-Risk ARR (EUR)",
    )

    return w


# ═══════════════════════════════════════════════════════════════════════════
#  Layout
# ═══════════════════════════════════════════════════════════════════════════


def build_layout():
    # ── Page 1: Pipeline by Motion ──
    p1 = nav_row("p1", 9) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_qtr", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # KPI tiles
        {"name": "p1_land_arr", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_expand_arr", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_renewal_arr", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        # Funnel + Waterfall
        {"name": "p1_sec_funnel", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_funnel", "row": 10, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_waterfall", "row": 10, "column": 6, "colspan": 6, "rowspan": 8},
        # Combo: Qtr pipeline + cumulative
        {"name": "p1_sec_qtr", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_combo_qtr", "row": 19, "column": 0, "colspan": 12, "rowspan": 8},
        # Gauges: Win Rate by Type
        {"name": "p1_sec_wr", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_wr_land", "row": 28, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p1_wr_expand", "row": 28, "column": 4, "colspan": 4, "rowspan": 6},
        {"name": "p1_wr_renewal", "row": 28, "column": 8, "colspan": 4, "rowspan": 6},
        # Hbar: Sales Cycle
        {"name": "p1_cycle", "row": 34, "column": 0, "colspan": 12, "rowspan": 8},
        # YoY Motion Comparison
        {"name": "p1_sec_yoy", "row": 61, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_yoy_land", "row": 62, "column": 0, "colspan": 4, "rowspan": 8},
        {"name": "p1_yoy_expand", "row": 62, "column": 4, "colspan": 4, "rowspan": 8},
        {"name": "p1_yoy_renewal", "row": 62, "column": 8, "colspan": 4, "rowspan": 8},
        # VIZ UPGRADE: Dynamic KPI Tiles
        {"name": "p1_renewal_rate_dynamic", "row": 70, "column": 0, "colspan": 6, "rowspan": 4},
        {"name": "p1_churn_rate_dynamic", "row": 70, "column": 6, "colspan": 6, "rowspan": 4},
    ]

    # ── Page 2: Renewals (RW 1.8) ──
    p2 = nav_row("p2", 9) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_qtr", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Stackcolumn: Renewals by Qtr & term
        {"name": "p2_sec_term", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ren_term", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
        # Area: Renewals by month YoY
        {"name": "p2_ren_yoy", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
        # Indexation section
        {"name": "p2_sec_index", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ren_fy_wf", "row": 15, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_ren_index", "row": 15, "column": 6, "colspan": 6, "rowspan": 8},
        # At-risk renewals
        {"name": "p2_sec_risk", "row": 23, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p2_ren_risk_tbl",
            "row": 24,
            "column": 0,
            "colspan": 9,
            "rowspan": 10,
        },
        {"name": "p2_ren_wr", "row": 24, "column": 9, "colspan": 3, "rowspan": 6},
    ]

    # ── Page 3: Conversion / ILF / ALF (RW 1.9) ──
    p3 = nav_row("p3", 9) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_qtr", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # ILF combo
        {"name": "p3_sec_ilf", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ilf_combo", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
        # ALF combo
        {"name": "p3_sec_alf", "row": 5, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p3_alf_combo", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
        # New customers by month x region
        {"name": "p3_sec_new", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_new_cust", "row": 15, "column": 0, "colspan": 6, "rowspan": 8},
        # Waterfall: Cross-sell ARR
        {"name": "p3_cross_sell", "row": 15, "column": 6, "colspan": 6, "rowspan": 8},
        # Synergy tiles (count + ARR for each)
        {"name": "p3_sec_synergy", "row": 23, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_synergy_won", "row": 24, "column": 0, "colspan": 3, "rowspan": 4},
        {
            "name": "p3_synergy_won_arr",
            "row": 24,
            "column": 3,
            "colspan": 3,
            "rowspan": 4,
        },
        {"name": "p3_synergy_pipe", "row": 28, "column": 0, "colspan": 3, "rowspan": 4},
        {
            "name": "p3_synergy_pipe_arr",
            "row": 28,
            "column": 3,
            "colspan": 3,
            "rowspan": 4,
        },
        # Funnel: Land -> Expand
        {"name": "p3_le_funnel", "row": 24, "column": 6, "colspan": 6, "rowspan": 8},
        # Iteration 4: New customer value combo (KPI 23)
        {
            "name": "p3_ch_new_value",
            "row": 32,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
    ]

    # ── Page 4: Cancellation & Churn (RW 1.10) ──
    p4 = nav_row("p4", 9) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_qtr", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Lost ARR by Qtr + reason
        {"name": "p4_sec_lost", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_lost_qtr", "row": 6, "column": 0, "colspan": 12, "rowspan": 8},
        # Risk dist + gauge
        {"name": "p4_sec_risk", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_risk_donut", "row": 15, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p4_risk_gauge", "row": 15, "column": 6, "colspan": 6, "rowspan": 6},
        # At-risk deals table
        {"name": "p4_sec_deals", "row": 23, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_risk_tbl", "row": 24, "column": 0, "colspan": 12, "rowspan": 10},
        # Churn trend + net ARR waterfall
        {"name": "p4_sec_churn", "row": 34, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_churn_area", "row": 35, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p4_net_arr", "row": 35, "column": 6, "colspan": 6, "rowspan": 8},
        # Iteration 4: Lost ARR % gauge (KPI 27)
        {"name": "p4_lost_pct", "row": 21, "column": 6, "colspan": 6, "rowspan": 6},
        # VIZ UPGRADE: NRR Bridge
        {"name": "p4_sec_nrr_bridge", "row": 43, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_nrr_bridge", "row": 44, "column": 0, "colspan": 12, "rowspan": 6},
    ]

    # ── Page 5: Product/Pricing (RW 1.14) ──
    p5 = nav_row("p5", 9) + [
        {"name": "p5_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p5_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_qtr", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # SaaS ARR combo + gauge
        {"name": "p5_sec_saas", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_saas_combo", "row": 6, "column": 0, "colspan": 8, "rowspan": 8},
        {"name": "p5_saas_gauge", "row": 6, "column": 8, "colspan": 4, "rowspan": 6},
        # Product mix
        {"name": "p5_sec_product", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_unit_saas", "row": 15, "column": 0, "colspan": 12, "rowspan": 8},
        # KPI tiles
        {"name": "p5_sec_kpi", "row": 23, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_saas_won", "row": 24, "column": 0, "colspan": 6, "rowspan": 4},
        {"name": "p5_saas_pct_num", "row": 24, "column": 6, "colspan": 6, "rowspan": 4},
        # PS ARR & One-Off Revenue
        {"name": "p5_sec_ps", "row": 29, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ps_gauge", "row": 30, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p5_ps_qtr", "row": 30, "column": 4, "colspan": 8, "rowspan": 6},
        {"name": "p5_oneoff_qtr", "row": 36, "column": 0, "colspan": 12, "rowspan": 8},
        # SaaS YoY Growth
        {"name": "p5_sec_yoy", "row": 45, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_saas_yoy", "row": 46, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    # ── Page 6: Competitive Intelligence ──
    p6 = nav_row("p6", 9) + [
        {"name": "p6_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p6_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_qtr", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Competitor frequency hbar
        {"name": "p6_sec_comp", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_comp_freq", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
        # Win rate comparisontable
        {"name": "p6_sec_wr", "row": 5, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p6_ch_comp_wr", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
        # Win/Loss reason distribution
        {"name": "p6_sec_reason", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_reason", "row": 15, "column": 0, "colspan": 6, "rowspan": 8},
        # Reasons by won vs lost stackhbar
        {"name": "p6_ch_reason_wl", "row": 15, "column": 6, "colspan": 6, "rowspan": 8},
        # Loss sub-reasons by ARR impact
        {"name": "p6_sec_sub", "row": 23, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p6_ch_sub_reason",
            "row": 24,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Win/Loss by Competitor Detail
        {
            "name": "p6_sec_comp_loss",
            "row": 32,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p6_ch_comp_loss",
            "row": 33,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        {
            "name": "p6_sec_comp_wr2",
            "row": 41,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p6_tbl_comp_wr", "row": 42, "column": 0, "colspan": 12, "rowspan": 8},
        {
            "name": "p6_sec_comp_type",
            "row": 50,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p6_ch_comp_type",
            "row": 51,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        {
            "name": "p6_sec_comp_detail",
            "row": 59,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p6_tbl_comp_detail",
            "row": 60,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # VIZ UPGRADE: Competitor × Segment Heatmap
        {"name": "p6_sec_comp_heat", "row": 70, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_comp_heat", "row": 71, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    p7 = nav_row("p7", 9) + [
        {"name": "p7_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p7_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_qtr", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Sankey
        {"name": "p7_sec_sankey", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_sankey", "row": 6, "column": 0, "colspan": 12, "rowspan": 10},
        # Treemap
        {"name": "p7_sec_treemap", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_treemap", "row": 17, "column": 0, "colspan": 12, "rowspan": 10},
        # Stacked area
        {"name": "p7_sec_area", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_area", "row": 28, "column": 0, "colspan": 12, "rowspan": 8},
        # Heatmap
        {"name": "p7_sec_heatmap", "row": 36, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_heatmap", "row": 37, "column": 0, "colspan": 12, "rowspan": 10},
        # Choropleth: Won revenue by country
        {"name": "p7_sec_geo", "row": 47, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_geo", "row": 48, "column": 0, "colspan": 12, "rowspan": 10},
        # Bubble: Motion deal size vs cycle time
        {"name": "p7_sec_bubble", "row": 58, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_bubble", "row": 59, "column": 0, "colspan": 12, "rowspan": 10},
        # VIZ UPGRADE: Motion Flow Sankey
        {"name": "p7_sec_motion_flow", "row": 69, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_motion_flow", "row": 70, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    p8 = nav_row("p8", 9) + [
        {"name": "p8_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p8_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p8_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p8_f_qtr", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p8_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Bullet charts
        {"name": "p8_sec_bullet", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p8_bullet_ilf", "row": 6, "column": 0, "colspan": 4, "rowspan": 5},
        {"name": "p8_bullet_alf", "row": 6, "column": 4, "colspan": 4, "rowspan": 5},
        {
            "name": "p8_bullet_retention",
            "row": 6,
            "column": 8,
            "colspan": 4,
            "rowspan": 5,
        },
        # Win rate by motion
        {
            "name": "p8_sec_motion_wr",
            "row": 20,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p8_stat_motion_wr",
            "row": 21,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Phase 9: Gini + Cohort
        {"name": "p8_sec_gini", "row": 29, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p8_gini_value", "row": 30, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p8_sec_cohort", "row": 30, "column": 4, "colspan": 8, "rowspan": 1},
        {"name": "p8_cohort_chart", "row": 31, "column": 4, "colspan": 8, "rowspan": 8},
        # ARR Percentiles table
        {
            "name": "p8_sec_percentiles",
            "row": 39,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p8_tbl_percentiles",
            "row": 40,
            "column": 0,
            "colspan": 12,
            "rowspan": 6,
        },
        # Cycle time stats by motion
        {
            "name": "p8_sec_cycle_stats",
            "row": 55,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p8_tbl_cycle_stats",
            "row": 56,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
    ]

    p9 = nav_row("p9", 9) + [
        {"name": "p9_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p9_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p9_f_region", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p9_kpi_new", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p9_kpi_expand", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p9_kpi_churn", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p9_sec_bridge", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p9_ch_bridge", "row": 10, "column": 0, "colspan": 12, "rowspan": 10},
        {"name": "p9_sec_growth", "row": 20, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p9_ch_growth", "row": 21, "column": 0, "colspan": 12, "rowspan": 10},
        {"name": "p9_sec_churn", "row": 31, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p9_ch_churn", "row": 32, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("motions", "Pipeline by Motion", p1),
            pg("renewals", "Renewals", p2),
            pg("conversion", "Conversion / ILF / ALF", p3),
            pg("churn", "Cancellation & Churn", p4),
            pg("product", "Product & Pricing", p5),
            pg("competitive", "Competitive Intelligence", p6),
            pg("advanalytics", "Advanced Analytics", p7),
            pg("revstats", "Statistical Analysis", p8),
            pg("arrbridge", "ARR Bridge", p9),
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def create_dataflow_definition():
    """Return a CRM Analytics dataflow definition for Revenue_Motions.

    NOTE: The Python upload path uses convertCurrency(APTS_Forecast_ARR__c)
    to get EUR-converted ARR values. sfdcDigest does NOT support
    convertCurrency(), so this dataflow creates a basic version without
    currency conversion as a fallback for automated daily refresh.
    """
    return {
        "Extract_Opps": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Opportunity",
                "fields": [
                    {"name": "Id"},
                    {"name": "Name"},
                    {"name": "OwnerId"},
                    {"name": "AccountId"},
                    {"name": "Account_Unit_Group__c"},
                    {"name": "Sales_Region__c"},
                    {"name": "ForecastCategoryName"},
                    {"name": "IsClosed"},
                    {"name": "IsWon"},
                    {"name": "CloseDate"},
                    {"name": "StageName"},
                    {"name": "Type"},
                    {"name": "CreatedDate"},
                    {"name": "FiscalYear"},
                    {"name": "FiscalQuarter"},
                    {"name": "APTS_Forecast_ARR__c"},
                    {"name": "Amount"},
                    {"name": "Probability"},
                    {"name": "AgeInDays"},
                    {"name": "Reason_Won_Lost__c"},
                    {"name": "Sub_Reason__c"},
                    {"name": "Lost_to_Competitor__c"},
                    {"name": "Sales_Cycle_Duration__c"},
                ],
            },
        },
        "Extract_Users": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "User",
                "fields": [{"name": "Id"}, {"name": "Name"}],
            },
        },
        "Extract_Accounts": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Account",
                "fields": [{"name": "Id"}, {"name": "Name"}],
            },
        },
        "Augment_Owner": {
            "action": "augment",
            "parameters": {
                "left": "Extract_Opps",
                "left_key": ["OwnerId"],
                "relationship": "Owner",
                "right": "Extract_Users",
                "right_key": ["Id"],
                "right_select": ["Name"],
            },
        },
        "Augment_Account": {
            "action": "augment",
            "parameters": {
                "left": "Augment_Owner",
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
                "name": "Revenue_Motions",
                "alias": "Revenue_Motions",
                "label": "Revenue Motions",
            },
        },
    }


def main():
    print("=" * 60)
    print("Revenue Motions KPI Dashboard Builder")
    print("=" * 60)

    # 1. Authenticate
    instance_url, token = get_auth()
    print(f"  Authenticated: {instance_url}")

    if "--create-dataflow" in sys.argv:
        print("\n=== Creating/updating dataflow ===")
        df_def = create_dataflow_definition()
        df_id = create_dataflow(instance_url, token, "DF_Revenue_Motions", df_def)
        if df_id and "--run-dataflow" in sys.argv:
            run_dataflow(instance_url, token, df_id)
        return

    # 2. Build and upload dataset
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
            {"field": "Name", "sobject": "Opportunity", "id_field": "Id"},
            {"field": "AccountName", "sobject": "Account", "id_field": "AccountId"},
        ],
    )

    # 3. Look up dataset ID
    ds_id = get_dataset_id(instance_url, token, DS)
    if not ds_id:
        print(f"ERROR: Could not find dataset '{DS}' - aborting")
        return
    print(f"  Dataset ID: {ds_id}")

    # 4. Create or find existing dashboard
    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    print(f"  Dashboard ID: {dashboard_id}")

    # 5. Build and deploy
    steps = build_steps(ds_id)
    widgets = build_widgets()
    layout = build_layout()

    state = build_dashboard_state(steps, widgets, layout)
    print("\n=== Deploying dashboard ===")
    deploy_dashboard(instance_url, token, dashboard_id, state)

    print("\nDone.")


if __name__ == "__main__":
    main()
