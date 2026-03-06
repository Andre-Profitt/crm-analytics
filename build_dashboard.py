#!/usr/bin/env python3
"""Build the Opp Mgmt KPI dashboard — v7 COO/CRO KPI framework.

Features:
  - Interactive UnitGroup listselector filter (aggregateflex step)
  - Interactive Quarter listselector filter
  - Selection bindings: clicking a filter updates all data widgets
  - broadcastFacet on chart steps: clicking a bar cross-filters
  - Rich chart configs: columnMap, axis labels, legends, tooltips
  - widgetStyle for consistent look
  - 8 pages: Executive Overview, Time Trends, Cross-Dimensional, Geographic,
    Sales Ops, Product Mix, Advanced Analytics, Statistical Analysis
  - 16 KPI framework with conditional formatting
  - Win/Loss analysis, Forecast Accuracy, Pipeline Velocity
  - Stage conversion funnel and time-in-stage analytics

  Visualization Upgrade (v7):
  - Dynamic KPI tiles with threshold-based coloring (results interactions)
  - Stage-to-stage transition Sankey for deal flow visualization
  - Win/Loss reason heatmap (reason × segment matrix)
  - Win/Loss reason waterfall (net ARR impact)
  - Time-in-stage distribution trellis (small multiples by stage)
  - Geographic map toggle views (World → US → EMEA drill)
  - Cross-dimensional trellis (UnitGroup × Region small multiples)
  - Table actions (View Record / Create Task) on drill-down tables
"""

import csv
import io
import sys

from crm_analytics_helpers import (
    _date_diff,
    get_auth,
    _sf_api,
    _soql,
    _dim,
    _measure,
    _date,
    upload_dataset,
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
    choropleth_chart,
    pillbox,
    hdr,
    section_label,
    nav_link,
    pg,
    nav_row,
    build_dashboard_state,
    deploy_dashboard,
    create_dashboard_if_needed,
    coalesce_filter,
    precompute_scoring_stats,
    compute_win_score,
    sankey_chart,
    treemap_chart,
    bubble_chart,
    heatmap_chart,
    area_chart,
    bullet_chart,
    combo_chart,
    line_chart,
    create_dataflow,
    run_dataflow,
    set_record_links_xmd,
    add_selection_interaction,
    add_table_action,
    stage_transition_step,
    # ML-Forward additions
    scatter_chart,
    growth_cube_step,
    get_dataset_id,
)

DASHBOARD_LABEL = "Opp Management"
DS = "Opp_Mgmt_KPIs"
DS_LABEL = "Opportunity Management KPIs"

GEO_DS = "Opp_Geo_Map"
GEO_DS_LABEL = "Opportunity Geographic Map"


def create_main_dataset(inst, tok):
    """Rebuild Opp_Mgmt_KPIs with convertCurrency(Forecast ARR) already in EUR."""
    print("\n=== Rebuilding main dataset (with converted ARR) ===")

    # ── 1. Query all opportunities with converted ARR ──
    opps = _soql(
        inst,
        tok,
        "SELECT Id, Name, Owner.Name, AccountId, Account.Name, "
        "Account_Unit_Group__c, Sales_Region__c, ForecastCategoryName, "
        "IsClosed, IsWon, CloseDate, StageName, Type, LeadSource, "
        "CreatedDate, CurrencyIsoCode, "
        "FiscalYear, FiscalQuarter, "
        "APTS_Forecast_ARR__c, "
        "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
        "APTS_Forecast_ACV_AVG__c, "
        "Expand_Forecast_ACV__c, Renewal_Forecast_ACV__c, "
        "Amount, Probability, AgeInDays, LastStageChangeInDays, "
        "Sales_Cycle_Duration__c, "
        "Stage_20_Approval__c, Stage_20_Approval_Date__c, "
        "New_Stage_10_created_Date__c, New_Stage_15_Date__c, "
        "New_Stage_20_Date__c, New_Stage_30_Date__c, "
        "New_Stage_40_Date__c, New_Stage_50_Date__c, "
        "New_Stage_6_Date__c, New_Stage_7_Date__c, "
        "Reason_Won_Lost__c, Sub_Reason__c, Quota_Amount__c, "
        "APTS_RH_Product_Family__c "
        "FROM Opportunity "
        "WHERE FiscalYear IN (2025, 2026, 2027)",
    )
    print(f"  Queried {len(opps)} opportunities")

    # ── 2. Build CSV ──
    fields = [
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
        "LeadSource",
        "CreatedDate",
        "CurrencyCode",
        "FiscalYear",
        "FiscalQuarter",
        "ForecastARR",
        "ARR",
        "ForecastACV",
        "ExpandForecastACV",
        "RenewalForecastACV",
        "Amount",
        "Probability",
        "AgeInDays",
        "DaysInStage",
        "SalesCycleDuration",
        "CommercialApproval",
        "CommercialApprovalDate",
        "Stage1Date",
        "Stage2Date",
        "Stage3Date",
        "Stage4Date",
        "Stage5Date",
        "Stage6Date",
        "Stage7Date",
        "Stage8Date",
        # ── v6 computed fields ──
        "WonLostReason",
        "SubReason",
        "QuotaAmount",
        "CloseMonth",
        "CreatedMonth",
        "HitStage1",
        "HitStage2",
        "HitStage3",
        "HitStage4",
        "HitStage5",
        "HitStage6",
        "Stage1to2Days",
        "Stage2to3Days",
        "Stage3to4Days",
        "Stage4to5Days",
        "Stage5to6Days",
        "ApprovalToCloseDays",
        "WeightedARR",
        "FYLabel",
        "WinScore",
        "WinScoreBand",
        "ProductFamily",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()

    type_win_rates, avg_deal_size = precompute_scoring_stats(opps)

    for o in opps:
        acct = o.get("Account") or {}
        owner = o.get("Owner") or {}
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
                "CloseDate": o.get("CloseDate", ""),
                "StageName": o.get("StageName", ""),
                "Type": o.get("Type", ""),
                "LeadSource": o.get("LeadSource", ""),
                "CreatedDate": (o.get("CreatedDate") or "")[:10],
                "CurrencyCode": o.get("CurrencyIsoCode", ""),
                "FiscalYear": o.get("FiscalYear", ""),
                "FiscalQuarter": o.get("FiscalQuarter", ""),
                "ForecastARR": o.get("APTS_Forecast_ARR__c") or 0,
                "ARR": o.get("ConvertedARR") or 0,
                "ForecastACV": o.get("APTS_Forecast_ACV_AVG__c") or 0,
                "ExpandForecastACV": o.get("Expand_Forecast_ACV__c") or 0,
                "RenewalForecastACV": o.get("Renewal_Forecast_ACV__c") or 0,
                "Amount": o.get("Amount") or 0,
                "Probability": o.get("Probability") or 0,
                "AgeInDays": o.get("AgeInDays") or 0,
                "DaysInStage": o.get("LastStageChangeInDays") or 0,
                "SalesCycleDuration": o.get("Sales_Cycle_Duration__c") or 0,
                "CommercialApproval": str(o.get("Stage_20_Approval__c", False)).lower(),
                "CommercialApprovalDate": o.get("Stage_20_Approval_Date__c") or "",
                "Stage1Date": (o.get("New_Stage_10_created_Date__c") or ""),
                "Stage2Date": (o.get("New_Stage_15_Date__c") or ""),
                "Stage3Date": (o.get("New_Stage_20_Date__c") or ""),
                "Stage4Date": (o.get("New_Stage_30_Date__c") or ""),
                "Stage5Date": (o.get("New_Stage_40_Date__c") or ""),
                "Stage6Date": (o.get("New_Stage_6_Date__c") or ""),
                "Stage7Date": (o.get("New_Stage_7_Date__c") or ""),
                "Stage8Date": (o.get("New_Stage_50_Date__c") or ""),
                # ── v6 computed fields ──
                "WonLostReason": o.get("Reason_Won_Lost__c") or "",
                "SubReason": o.get("Sub_Reason__c") or "",
                "QuotaAmount": o.get("Quota_Amount__c") or 0,
                "CloseMonth": (o.get("CloseDate") or "")[:7],
                "CreatedMonth": (o.get("CreatedDate") or "")[:10][:7],
                "HitStage1": "true"
                if o.get("New_Stage_10_created_Date__c")
                else "false",
                "HitStage2": "true" if o.get("New_Stage_15_Date__c") else "false",
                "HitStage3": "true" if o.get("New_Stage_20_Date__c") else "false",
                "HitStage4": "true" if o.get("New_Stage_30_Date__c") else "false",
                "HitStage5": "true" if o.get("New_Stage_40_Date__c") else "false",
                "HitStage6": "true" if o.get("New_Stage_6_Date__c") else "false",
                "Stage1to2Days": _date_diff(
                    o.get("New_Stage_10_created_Date__c"),
                    o.get("New_Stage_15_Date__c"),
                ),
                "Stage2to3Days": _date_diff(
                    o.get("New_Stage_15_Date__c"),
                    o.get("New_Stage_20_Date__c"),
                ),
                "Stage3to4Days": _date_diff(
                    o.get("New_Stage_20_Date__c"),
                    o.get("New_Stage_30_Date__c"),
                ),
                "Stage4to5Days": _date_diff(
                    o.get("New_Stage_30_Date__c"),
                    o.get("New_Stage_40_Date__c"),
                ),
                "Stage5to6Days": _date_diff(
                    o.get("New_Stage_40_Date__c"),
                    o.get("New_Stage_6_Date__c"),
                ),
                "ApprovalToCloseDays": (
                    _date_diff(
                        o.get("Stage_20_Approval_Date__c"),
                        o.get("CloseDate"),
                    )
                    if str(o.get("IsWon", False)).lower() == "true"
                    else 0
                ),
                "WeightedARR": round(
                    (o.get("ConvertedARR") or 0) * (o.get("Probability") or 0) / 100, 2
                ),
                "FYLabel": f"FY{o.get('FiscalYear', '')}",
                "WinScore": win_score,
                "WinScoreBand": win_band,
                "ProductFamily": (o.get("APTS_RH_Product_Family__c") or "Unknown")
                .split(";")[0]
                .strip(),
            }
        )
    # ── Phase 9: Python-precomputed DealVelocityBand ──
    # Compute percentiles of SalesCycleDuration for closed-won deals
    won_durations = sorted(
        [
            o.get("Sales_Cycle_Duration__c") or 0
            for o in opps
            if str(o.get("IsWon", False)).lower() == "true"
            and (o.get("Sales_Cycle_Duration__c") or 0) > 0
        ]
    )
    if len(won_durations) >= 4:
        p33 = won_durations[len(won_durations) // 3]
        p66 = won_durations[2 * len(won_durations) // 3]
    else:
        p33, p66 = 60, 120  # defaults
    print(f"  Velocity bands: Fast<{p33}d, Normal {p33}-{p66}d, Slow>{p66}d")

    buf2 = io.StringIO()
    ext_fields = fields + ["DealVelocityBand"]
    writer2 = csv.DictWriter(buf2, fieldnames=ext_fields, lineterminator="\n")
    writer2.writeheader()
    buf.seek(0)
    reader = csv.DictReader(buf)
    band_counts = {"Fast": 0, "Normal": 0, "Slow": 0, "N/A": 0}
    for row in reader:
        dur = int(float(row.get("SalesCycleDuration") or 0))
        if dur <= 0:
            band = "N/A"
        elif dur < p33:
            band = "Fast"
        elif dur <= p66:
            band = "Normal"
        else:
            band = "Slow"
        row["DealVelocityBand"] = band
        band_counts[band] = band_counts.get(band, 0) + 1
        writer2.writerow(row)
    csv_bytes = buf2.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes, {len(opps)} rows (with DealVelocityBand)")
    print(f"  Velocity bands: {band_counts}")

    # ── 3. Build metadata & upload ──
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
        _dim("LeadSource", "Lead Source"),
        _date("CreatedDate", "Created Date"),
        _dim("CurrencyCode", "Currency"),
        _measure("FiscalYear", "Fiscal Year", scale=0, precision=4),
        _measure("FiscalQuarter", "Fiscal Quarter", scale=0, precision=1),
        _measure("ForecastARR", "Forecast ARR (local)"),
        _measure("ARR", "ARR (EUR)"),
        _measure("ForecastACV", "Forecast ACV (local)"),
        _measure("ExpandForecastACV", "Expand Forecast ACV"),
        _measure("RenewalForecastACV", "Renewal Forecast ACV"),
        _measure("Amount", "Amount"),
        _measure("Probability", "Probability", scale=0, precision=3),
        _measure("AgeInDays", "Age (Days)", scale=0, precision=5),
        _measure("DaysInStage", "Days In Current Stage", scale=0, precision=5),
        _measure(
            "SalesCycleDuration",
            "Sales Cycle Duration",
            scale=0,
            precision=6,
        ),
        _dim("CommercialApproval", "Commercial Approval"),
        _date("CommercialApprovalDate", "Commercial Approval Date"),
        _date("Stage1Date", "Stage 1 Date"),
        _date("Stage2Date", "Stage 2 Date"),
        _date("Stage3Date", "Stage 3 Date"),
        _date("Stage4Date", "Stage 4 Date"),
        _date("Stage5Date", "Stage 5 Date"),
        _date("Stage6Date", "Stage 6 Date"),
        _date("Stage7Date", "Stage 7 Date"),
        _date("Stage8Date", "Stage 8 Date"),
        # ── v6 computed fields ──
        _dim("WonLostReason", "Won/Lost Reason"),
        _dim("SubReason", "Sub Reason"),
        _measure("QuotaAmount", "Quota Amount"),
        _dim("CloseMonth", "Close Month"),
        _dim("CreatedMonth", "Created Month"),
        _dim("HitStage1", "Hit Stage 1"),
        _dim("HitStage2", "Hit Stage 2"),
        _dim("HitStage3", "Hit Stage 3"),
        _dim("HitStage4", "Hit Stage 4"),
        _dim("HitStage5", "Hit Stage 5"),
        _dim("HitStage6", "Hit Stage 6"),
        _measure("Stage1to2Days", "Stage 1→2 Days", scale=0, precision=5),
        _measure("Stage2to3Days", "Stage 2→3 Days", scale=0, precision=5),
        _measure("Stage3to4Days", "Stage 3→4 Days", scale=0, precision=5),
        _measure("Stage4to5Days", "Stage 4→5 Days", scale=0, precision=5),
        _measure("Stage5to6Days", "Stage 5→6 Days", scale=0, precision=5),
        _measure(
            "ApprovalToCloseDays",
            "Approval to Close Days",
            scale=0,
            precision=5,
        ),
        _measure("WeightedARR", "Weighted ARR (EUR)"),
        _dim("FYLabel", "Fiscal Year Label"),
        _measure("WinScore", "Win Score", scale=0, precision=3),
        _dim("WinScoreBand", "Win Score Band"),
        _dim("ProductFamily", "Product Family"),
        _dim("DealVelocityBand", "Deal Velocity Band"),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


def create_geo_dataset(inst, tok):
    """Create/refresh the Opp_Geo_Map dataset with BillingCountry data (EUR).

    Uses convertCurrency() for Salesforce-native EUR conversion.
    """
    print("\n=== Building geographic dataset (EUR via convertCurrency) ===")

    # ── 1. Query country-level pipeline data (converted to corporate EUR) ──
    pipe_rows = _soql(
        inst,
        tok,
        "SELECT Account.BillingCountry country, "
        "SUM(convertCurrency(APTS_Forecast_ARR__c)) acv, COUNT(Id) cnt "
        "FROM Opportunity "
        "WHERE FiscalYear = 2026 AND IsClosed = false "
        "AND Account.BillingCountry != null "
        "GROUP BY Account.BillingCountry "
        "ORDER BY Account.BillingCountry",
    )
    print(f"  Pipeline: {len(pipe_rows)} countries")

    won_rows = _soql(
        inst,
        tok,
        "SELECT Account.BillingCountry country, "
        "SUM(convertCurrency(APTS_Forecast_ARR__c)) acv, COUNT(Id) cnt "
        "FROM Opportunity "
        "WHERE FiscalYear = 2026 AND IsWon = true "
        "AND Account.BillingCountry != null "
        "GROUP BY Account.BillingCountry",
    )

    # ── 2. Merge into country-level rows ──
    won_map = {r["country"]: r for r in won_rows}

    rows = []
    for r in pipe_rows:
        country = r["country"]
        w = won_map.get(country, {})
        pipe_acv = r.get("acv") or 0
        pipe_cnt = r.get("cnt") or 0
        won_arr = w.get("acv") or 0
        won_cnt = w.get("cnt") or 0
        avg_deal = pipe_acv / pipe_cnt if pipe_cnt else 0
        win_rate = (won_cnt / (pipe_cnt + won_cnt) * 100) if (pipe_cnt + won_cnt) else 0
        rows.append(
            {
                "Country": country,
                "pipeline_arr": round(pipe_acv, 2),
                "won_arr": round(won_arr, 2),
                "opp_count": pipe_cnt + won_cnt,
                "won_count": won_cnt,
                "avg_deal": round(avg_deal, 2),
                "win_rate": round(win_rate, 1),
            }
        )
    print(
        f"  Merged: {len(rows)} countries, "
        f"\u20ac{sum(r['pipeline_arr'] for r in rows):,.0f} total pipeline (EUR)"
    )

    # ── 3. Generate CSV ──
    buf = io.StringIO()
    fields = [
        "Country",
        "pipeline_arr",
        "won_arr",
        "opp_count",
        "won_count",
        "avg_deal",
        "win_rate",
    ]
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8")

    # ── 4. Build metadata & upload ──
    fields_meta = [
        _dim("Country", "Country"),
        _measure("pipeline_arr", "Pipeline ARR"),
        _measure("won_arr", "Won ARR"),
        _measure("opp_count", "Opportunities", scale=0, precision=10),
        _measure("won_count", "Won Deals", scale=0, precision=10),
        _measure("avg_deal", "Avg Deal Size"),
        _measure("win_rate", "Win Rate %", scale=1, precision=5),
    ]
    ok = upload_dataset(
        inst, tok, GEO_DS, GEO_DS_LABEL, fields_meta, csv_bytes, poll_attempts=20
    )
    return GEO_DS if ok else None


def _set_geo_xmd(inst, tok):
    """Mark the Country field as geographic in the Opp_Geo_Map XMD.

    This must run after each dataset rebuild because uploading new data
    resets the extended metadata, wiping any geographic field config.
    """
    # Find the dataset and its current version
    ds_list = _sf_api(
        inst, tok, "GET", "/services/data/v66.0/wave/datasets?q=Opp_Geo_Map"
    )
    datasets = ds_list.get("datasets", [])
    if not datasets:
        print("  XMD: Opp_Geo_Map dataset not found — skipping")
        return
    ds = datasets[0]
    ds_id = ds["id"]
    vid = ds.get("currentVersionId", "")
    if not vid:
        print("  XMD: No current version — skipping")
        return

    # Build clean XMD body — only writable fields (the API rejects read-only ones)
    # Read from /xmds/main (system), write to /xmds/user (editable)
    xmd_read = f"/services/data/v66.0/wave/datasets/{ds_id}/versions/{vid}/xmds/main"
    xmd_write = f"/services/data/v66.0/wave/datasets/{ds_id}/versions/{vid}/xmds/user"
    xmd = _sf_api(inst, tok, "GET", xmd_read)

    # Clean measures: keep only writable fields
    clean_measures = []
    for m in xmd.get("measures", []):
        cm = {"field": m["field"], "label": m.get("label", m["field"])}
        if m.get("format"):
            cm["format"] = m["format"]
        clean_measures.append(cm)

    body = {
        "dimensions": [
            {
                "field": "Country",
                "label": "Country",
                "origin": "Country",
                "showInExplorer": True,
                "customActionsEnabled": True,
                "salesforceActionsEnabled": True,
                "linkTemplateEnabled": True,
            }
        ],
        "measures": clean_measures,
        "derivedDimensions": [],
        "derivedMeasures": [],
        "dates": [],
        "organizations": [],
        "showDetailsDefaultFields": [],
    }

    # PUT the updated XMD (non-fatal — dashboard deploys even if this fails)
    try:
        _sf_api(inst, tok, "PUT", xmd_write, body)
        print("  XMD: Country field metadata applied")
    except RuntimeError as e:
        print(f"  XMD WARNING: {e}")


# ── Quarter case expression ──
QTR = (
    '(case when substr(CloseDate, 6, 2) in ["01","02","03"] then "Q1" '
    'when substr(CloseDate, 6, 2) in ["04","05","06"] then "Q2" '
    'when substr(CloseDate, 6, 2) in ["07","08","09"] then "Q3" '
    'else "Q4" end)'
)

# ── Filter binding expressions ──
# These resolve to nothing when no selection is made (passthrough)
UF = 'q = filter q by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality(\'UnitGroup\')}};\n'
QF = 'q = filter q by {{coalesce(column(f_qtr.selection, ["Quarter"]), column(f_qtr.result, ["Quarter"])).asEquality(\'Quarter\')}};\n'
FYF = coalesce_filter("f_fy", "FYLabel")
RF = coalesce_filter("f_region", "SalesRegion")
TF = coalesce_filter("f_type", "Type")


def build_steps(ds_meta):
    L = f'q = load "{DS}";\n'
    # ARR is already in EUR (converted at dataset-build time via convertCurrency())
    FY = "q = filter q by FiscalYear == 2026;\n"
    OPEN = 'q = filter q by IsClosed == "false";\n'
    WON = 'q = filter q by IsWon == "true";\n'
    CLOSED = 'q = filter q by IsClosed == "true";\n'

    return {
        # ═══ FILTER STEPS (aggregateflex — powers listselector widgets) ═══
        "f_unit": af("UnitGroup", ds_meta),
        "f_type": af("Type", ds_meta),
        "f_fy": af("FYLabel", ds_meta),
        "f_region": af("SalesRegion", ds_meta),
        # Quarter filter needs SAQL since Quarter is computed
        "f_qtr": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, ARR;\n"
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, count() as cnt;\n"
            + "q = order q by Quarter asc;",
            broadcast=True,
        ),
        # ═══ PAGE 1: Executive Overview ═══
        "s_cwon": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_acv, count() as cnt;"
        ),
        "s_pipe": sq(
            L
            + FY
            + UF
            + FYF
            + RF
            + "q = foreach q generate ARR, "
            + '(case when IsClosed == "false" then ARR else 0 end) as pipe_acv, '
            + '(case when IsWon == "true" then ARR else 0 end) as won_arr;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(pipe_acv) as sum_acv, "
            + "(case when sum(won_arr) > 0 then sum(pipe_acv) / sum(won_arr) else 0 end) as coverage;"
        ),
        "s_wr": sq(
            L
            + FY
            + CLOSED
            + UF
            + FYF
            + RF
            + 'q = foreach q generate (case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_won) / count()) * 100 as win_rate, "
            + "sum(is_won) as won, count() as total;"
        ),
        "s_age": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = foreach q generate AgeInDays, "
            + "(case when AgeInDays > 120 then 1 else 0 end) as is_stale;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(AgeInDays) as avg_age, sum(is_stale) as stale;"
        ),
        "s_cyc": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate avg(SalesCycleDuration) as avg_cycle;"
        ),
        "s_apv": sq(
            L
            + FY
            + UF
            + FYF
            + RF
            + 'q = filter q by CommercialApproval == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_acv, count() as cnt;"
        ),
        "s_deal": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate avg(ARR) as avg_deal;"
        ),
        # Pipeline breakdowns
        "s_stg": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, sum(ARR) as sum_acv, count() as cnt;\n"
            + "q = order q by StageName asc;"
        ),
        "s_unit": sq(
            L
            + FY
            + OPEN
            + FYF
            + RF
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, sum(ARR) as sum_acv, count() as cnt;\n"
            + "q = order q by sum_acv desc;"
        ),
        "s_fcat": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = group q by ForecastCategory;\n"
            + "q = foreach q generate ForecastCategory, sum(ARR) as sum_acv, count() as cnt;"
        ),
        "s_type": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = group q by Type;\n"
            + "q = foreach q generate Type, sum(ARR) as sum_acv, count() as cnt;"
        ),
        # ═══ PAGE 2: Time Trends ═══
        "s_month_pipe": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = foreach q generate substr(CloseDate, 1, 7) as CloseMonth, ARR;\n"
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, sum(ARR) as sum_acv, count() as cnt;\n"
            + "q = order q by CloseMonth asc;"
        ),
        "s_month_won": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + "q = foreach q generate substr(CloseDate, 1, 7) as CloseMonth, ARR;\n"
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, sum(ARR) as sum_acv, count() as cnt;\n"
            + "q = order q by CloseMonth asc;"
        ),
        "s_qtr": sq(
            L
            + FY
            + UF
            + FYF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + '(case when IsClosed == "false" then ARR else 0 end) as pipe_acv, '
            + '(case when IsWon == "true" then ARR else 0 end) as won_arr;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, sum(pipe_acv) as pipeline, "
            + "sum(won_arr) as closed_won, count() as cnt;\n"
            + "q = order q by Quarter asc;"
        ),
        "s_qtr_wr": sq(
            L
            + FY
            + CLOSED
            + UF
            + FYF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, "
            + '(case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, (sum(is_won) / count()) * 100 as win_rate, "
            + "count() as total;\n"
            + "q = order q by Quarter asc;"
        ),
        # New opps created: filter by CreatedMonth (not FiscalYear which is CloseDate-based)
        "s_month_new": sq(
            L
            + UF
            + FYF
            + RF
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # ═══ PAGE 3: Cross-Dimensional ═══
        "s_deal_unit": sq(
            L
            + FY
            + OPEN
            + FYF
            + RF
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, avg(ARR) as avg_deal, "
            + "sum(ARR) as total_acv, count() as cnt;\n"
            + "q = order q by total_acv desc;"
        ),
        "s_wr_type": sq(
            L
            + FY
            + CLOSED
            + UF
            + FYF
            + RF
            + 'q = foreach q generate Type, (case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by Type;\n"
            + "q = foreach q generate Type, (sum(is_won) / count()) * 100 as win_rate, count() as total;"
        ),
        "s_wr_unit": sq(
            L
            + FY
            + CLOSED
            + FYF
            + RF
            + 'q = foreach q generate UnitGroup, (case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, (sum(is_won) / count()) * 100 as win_rate, count() as total;"
        ),
        "s_stg_unit": sq(
            L
            + FY
            + OPEN
            + FYF
            + RF
            + "q = group q by (StageName, UnitGroup);\n"
            + "q = foreach q generate StageName, UnitGroup, sum(ARR) as sum_acv;\n"
            + "q = order q by StageName asc;"
        ),
        "s_unit_reg": sq(
            L
            + FY
            + OPEN
            + FYF
            + "q = group q by (UnitGroup, SalesRegion);\n"
            + "q = foreach q generate UnitGroup, SalesRegion, "
            + "sum(ARR) as sum_acv, count() as cnt, avg(ARR) as avg_deal;\n"
            + "q = order q by sum_acv desc;"
        ),
        "s_cyc_unit": sq(
            L
            + FY
            + WON
            + FYF
            + RF
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, avg(SalesCycleDuration) as avg_cycle, count() as cnt;"
        ),
        "s_tier": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = foreach q generate "
            + '(case when ARR < 50000 then "< 50K" '
            + 'when ARR < 250000 then "50K-250K" '
            + 'when ARR < 1000000 then "250K-1M" '
            + 'else "> 1M" end) as Tier, ARR;\n'
            + "q = group q by Tier;\n"
            + "q = foreach q generate Tier, count() as cnt, sum(ARR) as sum_acv;"
        ),
        # Top 10 opportunities by ARR
        "s_top10": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = foreach q generate Id, Name, AccountId, AccountName, StageName, "
            + "ARR, AgeInDays;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 10;"
        ),
        # ═══ PAGE 4: Geographic ═══
        "s_geo_country": sq(
            f'q = load "{GEO_DS}";\n'
            + "q = group q by Country;\n"
            + "q = foreach q generate Country, "
            + "sum(pipeline_arr) as pipeline_arr;\n"
            + "q = order q by pipeline_arr desc;"
        ),
        "s_geo_region": sq(
            f'q = load "{DS}";\n'
            + "q = filter q by FiscalYear == 2026;\n"
            + 'q = filter q by IsClosed == "false";\n'
            + UF
            + FYF
            + "q = group q by SalesRegion;\n"
            + "q = foreach q generate SalesRegion, "
            + "sum(ARR) as pipeline_arr, "
            + "count() as opp_count;\n"
            + "q = order q by pipeline_arr desc;"
        ),
        "s_geo_scatter": sq(
            f'q = load "{GEO_DS}";\n'
            + "q = group q by Country;\n"
            + "q = foreach q generate Country, "
            + "sum(pipeline_arr) as pipeline_arr, "
            + "sum(opp_count) as opp_count, "
            + "sum(avg_deal) as avg_deal;\n"
            + "q = order q by pipeline_arr desc;\n"
            + "q = limit q 20;"
        ),
        "s_geo_top_countries": sq(
            f'q = load "{GEO_DS}";\n'
            + "q = group q by Country;\n"
            + "q = foreach q generate Country, "
            + "sum(pipeline_arr) as pipeline_arr, "
            + "sum(won_arr) as won_arr, "
            + "sum(opp_count) as opp_count, "
            + "sum(won_count) as won_count, "
            + "sum(avg_deal) as avg_deal, "
            + "avg(win_rate) as win_rate;\n"
            + "q = order q by pipeline_arr desc;\n"
            + "q = limit q 15;"
        ),
        # ═══ PAGE 1 additions: YoY + Weighted ═══
        "s_cwon_yoy": sq(
            L
            + WON
            + UF
            + FYF
            + RF
            + "q = group q by FYLabel;\n"
            + "q = foreach q generate FYLabel, sum(ARR) as sum_acv, count() as cnt;\n"
            + "q = order q by FYLabel asc;"
        ),
        "s_pipe_yoy": sq(
            L
            + OPEN
            + UF
            + FYF
            + RF
            + "q = group q by FYLabel;\n"
            + "q = foreach q generate FYLabel, sum(ARR) as sum_acv, count() as cnt;\n"
            + "q = order q by FYLabel asc;"
        ),
        "s_weighted": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(WeightedARR) as weighted, sum(ARR) as total;"
        ),
        # ═══ PAGE 2 additions: Monthly Deal, Approvals, Region, Won Tier ═══
        "s_month_deal": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, avg(ARR) as avg_deal, count() as cnt;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # Monthly approvals: group by actual approval date, not close date
        "s_month_apv": sq(
            L
            + FY
            + UF
            + FYF
            + RF
            + 'q = filter q by CommercialApproval == "true";\n'
            + "q = foreach q generate substr(CommercialApprovalDate, 1, 7) as ApprovalMonth, ARR;\n"
            + 'q = filter q by ApprovalMonth != "";\n'
            + "q = group q by ApprovalMonth;\n"
            + "q = foreach q generate ApprovalMonth, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by ApprovalMonth asc;"
        ),
        # New opps by region: filter by CreatedDate year, not FiscalYear
        "s_month_region": sq(
            L
            + UF
            + FYF
            + RF
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by (CreatedMonth, SalesRegion);\n"
            + "q = foreach q generate CreatedMonth, SalesRegion, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        "s_won_tier": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + "q = foreach q generate "
            + '(case when ARR < 50000 then "< 50K" '
            + 'when ARR < 100000 then "50K-100K" '
            + 'when ARR < 250000 then "100K-250K" '
            + 'when ARR < 1000000 then "250K-1M" '
            + 'else "> 1M" end) as Tier, ARR;\n'
            + "q = group q by Tier;\n"
            + "q = foreach q generate Tier, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by Tier asc;"
        ),
        # ═══ PAGE 3 additions: Conversion + Stage Duration + Type Effectiveness ═══
        "s_conv": sq(
            L
            + FY
            + UF
            + FYF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as total, "
            + '(sum(case when HitStage1 == "true" then 1 else 0 end) / count()) * 100 as stage1_pct, '
            + '(sum(case when HitStage2 == "true" then 1 else 0 end) / count()) * 100 as stage2_pct, '
            + '(sum(case when HitStage3 == "true" then 1 else 0 end) / count()) * 100 as stage3_pct, '
            + '(sum(case when HitStage4 == "true" then 1 else 0 end) / count()) * 100 as stage4_pct, '
            + '(sum(case when HitStage5 == "true" then 1 else 0 end) / count()) * 100 as stage5_pct, '
            + '(sum(case when HitStage6 == "true" then 1 else 0 end) / count()) * 100 as stage6_pct;'
        ),
        "s_type_eff": sq(
            L
            + FY
            + CLOSED
            + UF
            + FYF
            + RF
            + "q = foreach q generate Type, ARR, SalesCycleDuration, "
            + '(case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by Type;\n"
            + "q = foreach q generate Type, "
            + "(sum(is_won) / count()) * 100 as win_rate, "
            + "avg(ARR) as avg_deal, "
            + "avg(SalesCycleDuration) as avg_cycle, "
            + "count() as total;"
        ),
        # ═══ PAGE 5: Sales Operations ═══
        # Forecast accuracy: deduplicate quota per rep (max per owner)
        # then sum distinct quotas vs sum won ARR
        "s_forecast_acc": sq(
            L
            + UF
            + FYF
            + RF
            + "q = foreach q generate OwnerName, FYLabel, ARR, QuotaAmount, "
            + '(case when IsWon == "true" then ARR else 0 end) as won_arr;\n'
            + "q = group q by (OwnerName, FYLabel);\n"
            + "q = foreach q generate OwnerName, FYLabel, "
            + "sum(won_arr) as rep_won, max(QuotaAmount) as rep_quota;\n"
            + "q = group q by FYLabel;\n"
            + "q = foreach q generate FYLabel, "
            + "sum(rep_won) as closed_won, "
            + "sum(rep_quota) as quota, "
            + "(case when sum(rep_quota) > 0 then "
            + "(sum(rep_won) / sum(rep_quota)) * 100 else 0 end) as accuracy;\n"
            + "q = order q by FYLabel asc;"
        ),
        # Avg days to close: use ApprovalToCloseDays where available,
        # fall back to SalesCycleDuration for all won deals
        "s_apv_close": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + "q = foreach q generate "
            + "(case when ApprovalToCloseDays > 0 then ApprovalToCloseDays "
            + "else SalesCycleDuration end) as days_to_close;\n"
            + "q = filter q by days_to_close > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(days_to_close) as avg_days, "
            + "count() as cnt;"
        ),
        "s_aging": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = foreach q generate "
            + '(case when AgeInDays <= 30 then "0-30d" '
            + 'when AgeInDays <= 60 then "31-60d" '
            + 'when AgeInDays <= 90 then "61-90d" '
            + 'when AgeInDays <= 120 then "91-120d" '
            + 'else "120d+" end) as AgeBand, ARR;\n'
            + "q = group q by AgeBand;\n"
            + "q = foreach q generate AgeBand, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by AgeBand asc;"
        ),
        # Pipeline velocity = (won_count × avg_won_deal × win_rate) / avg_cycle
        # Use CLOSED deals only for consistent population
        "s_vel": sq(
            L
            + FY
            + CLOSED
            + UF
            + FYF
            + RF
            + "q = foreach q generate ARR, SalesCycleDuration, "
            + '(case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(is_won) as won_cnt, "
            + "count() as total, "
            + "avg(ARR) as avg_deal, "
            + "(case when count() > 0 then sum(is_won) / count() else 0 end) as win_rate, "
            + "(case when avg(SalesCycleDuration) > 0 then avg(SalesCycleDuration) else 1 end) as avg_cycle, "
            + "(case when count() > 0 and avg(SalesCycleDuration) > 0 then "
            + "(sum(is_won) * avg(ARR) * (sum(is_won) / count())) / avg(SalesCycleDuration) "
            + "else 0 end) as velocity;"
        ),
        "s_stale_list": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = filter q by AgeInDays > 120;\n"
            + "q = foreach q generate Id, Name, AccountName, StageName, ARR, AgeInDays;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 15;"
        ),
        # ═══ ADVANCED CHART STEPS ═══
        # Pipeline by stage ordered desc for funnel shape
        "s_stg_funnel": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, sum(ARR) as sum_acv;\n"
            + "q = order q by sum_acv desc;"
        ),
        # Conversion funnel: % of opps that hit each stage, as rows (UNION approach)
        "s_conv_funnel": sq(
            f'q1 = load "{DS}";\n'
            + "q1 = filter q1 by FiscalYear == 2026;\n"
            + 'q1 = foreach q1 generate (case when HitStage1 == "true" then 1 else 0 end) as h;\n'
            + "q1 = group q1 by all;\n"
            + 'q1 = foreach q1 generate "Stage 1" as StageName, (sum(h) / count()) * 100 as conv_pct;\n'
            + f'q2 = load "{DS}";\n'
            + "q2 = filter q2 by FiscalYear == 2026;\n"
            + 'q2 = foreach q2 generate (case when HitStage2 == "true" then 1 else 0 end) as h;\n'
            + "q2 = group q2 by all;\n"
            + 'q2 = foreach q2 generate "Stage 2" as StageName, (sum(h) / count()) * 100 as conv_pct;\n'
            + f'q3 = load "{DS}";\n'
            + "q3 = filter q3 by FiscalYear == 2026;\n"
            + 'q3 = foreach q3 generate (case when HitStage3 == "true" then 1 else 0 end) as h;\n'
            + "q3 = group q3 by all;\n"
            + 'q3 = foreach q3 generate "Stage 3" as StageName, (sum(h) / count()) * 100 as conv_pct;\n'
            + f'q4 = load "{DS}";\n'
            + "q4 = filter q4 by FiscalYear == 2026;\n"
            + 'q4 = foreach q4 generate (case when HitStage4 == "true" then 1 else 0 end) as h;\n'
            + "q4 = group q4 by all;\n"
            + 'q4 = foreach q4 generate "Stage 4" as StageName, (sum(h) / count()) * 100 as conv_pct;\n'
            + f'q5 = load "{DS}";\n'
            + "q5 = filter q5 by FiscalYear == 2026;\n"
            + 'q5 = foreach q5 generate (case when HitStage5 == "true" then 1 else 0 end) as h;\n'
            + "q5 = group q5 by all;\n"
            + 'q5 = foreach q5 generate "Stage 5" as StageName, (sum(h) / count()) * 100 as conv_pct;\n'
            + f'q6 = load "{DS}";\n'
            + "q6 = filter q6 by FiscalYear == 2026;\n"
            + 'q6 = foreach q6 generate (case when HitStage6 == "true" then 1 else 0 end) as h;\n'
            + "q6 = group q6 by all;\n"
            + 'q6 = foreach q6 generate "Stage 6" as StageName, (sum(h) / count()) * 100 as conv_pct;\n'
            + "q = union q1, q2, q3, q4, q5, q6;\n"
            + "q = order q by conv_pct desc;"
        ),
        # Monthly pipeline waterfall: created vs closed
        "s_waterfall": sq(
            L
            + FY
            + UF
            + FYF
            + RF
            + "q = foreach q generate "
            + "substr(CloseDate, 1, 7) as Month, "
            + '(case when IsClosed == "false" then ARR else 0 end) as added, '
            + '(case when IsClosed == "true" and IsWon == "false" then -ARR else 0 end) as lost, '
            + '(case when IsWon == "true" then ARR else 0 end) as won;\n'
            + "q = group q by Month;\n"
            + "q = foreach q generate Month, "
            + "sum(added) + sum(lost) + sum(won) as net_change;\n"
            + "q = order q by Month asc;"
        ),
        # Geographic bubble: country + pipeline + count + avg deal
        "s_geo_bubble": sq(
            f'q = load "{GEO_DS}";\n'
            + "q = group q by Country;\n"
            + "q = foreach q generate Country, "
            + "sum(opp_count) as opp_count, "
            + "avg(win_rate) as win_rate;\n"
            + "q = order q by opp_count desc;\n"
            + "q = limit q 20;"
        ),
        # ═══ TREND STEPS (Phase 3) — FY2026 vs FY2025 ═══
        "s_pipe_t": trend_step(
            DS,
            OPEN + UF + RF,
            "q = filter q by FiscalYear == 2026;\n",
            "q = filter q by FiscalYear == 2025;\n",
            "all",
            "sum(ARR)",
            "sum_acv",
        ),
        "s_cwon_t": trend_step(
            DS,
            WON + UF + RF,
            "q = filter q by FiscalYear == 2026;\n",
            "q = filter q by FiscalYear == 2025;\n",
            "all",
            "sum(ARR)",
            "sum_acv",
        ),
        "s_cwon_cnt_t": trend_step(
            DS,
            WON + UF + RF,
            "q = filter q by FiscalYear == 2026;\n",
            "q = filter q by FiscalYear == 2025;\n",
            "all",
            "count()",
            "cnt",
        ),
        "s_deal_t": trend_step(
            DS,
            OPEN + UF + RF,
            "q = filter q by FiscalYear == 2026;\n",
            "q = filter q by FiscalYear == 2025;\n",
            "all",
            "avg(ARR)",
            "avg_deal",
        ),
        # ═══ REP ATTAINMENT (Sales Ops page) ═══
        "s_rep_attain": sq(
            L
            + WON
            + FY
            + UF
            + TF
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, sum(ARR) as won_arr;\n"
            + "q = foreach q generate "
            + '(case when won_arr >= 2000000 then "200%+" '
            + 'when won_arr >= 1500000 then "150-200%" '
            + 'when won_arr >= 1000000 then "100-150%" '
            + 'when won_arr >= 500000 then "50-100%" '
            + 'else "< 50%" end) as AttainBand;\n'
            + "q = group q by AttainBand;\n"
            + "q = foreach q generate AttainBand, count() as cnt;\n"
            + "q = order q by AttainBand asc;"
        ),
        # ═══ PAGE 6: Product Mix ═══
        "s_product_pipe": sq(
            L
            + OPEN
            + FY
            + UF
            + TF
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, sum(ARR) as sum_arr, count() as cnt;\n"
            + "q = order q by sum_arr desc;\n"
            + "q = limit q 15;"
        ),
        "s_product_won": sq(
            L
            + WON
            + FY
            + UF
            + TF
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, sum(ARR) as sum_arr, count() as cnt;\n"
            + "q = order q by sum_arr desc;\n"
            + "q = limit q 15;"
        ),
        "s_product_wr": sq(
            L
            + CLOSED
            + FY
            + UF
            + TF
            + "q = foreach q generate ProductFamily, IsWon, ARR;\n"
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, count() as total, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as won_cnt, '
            + 'sum(case when IsWon == "true" then ARR else 0 end) as won_arr;\n'
            + "q = foreach q generate ProductFamily, total, won_cnt, "
            + "(won_cnt / total) * 100 as win_rate, won_arr;\n"
            + "q = order q by win_rate desc;"
        ),
        "s_product_type": sq(
            L
            + OPEN
            + FY
            + UF
            + "q = group q by (ProductFamily, Type);\n"
            + "q = foreach q generate ProductFamily, Type, sum(ARR) as sum_arr;\n"
            + "q = order q by sum_arr desc;\n"
            + "q = limit q 30;"
        ),
        "s_product_trend": sq(
            L
            + FY
            + UF
            + TF
            + f"q = foreach q generate {QTR} as Quarter, ProductFamily, ARR;\n"
            + "q = group q by (Quarter, ProductFamily);\n"
            + "q = foreach q generate Quarter, ProductFamily, sum(ARR) as sum_arr;\n"
            + "q = order q by Quarter asc;"
        ),
        # ═══ SOURCE EFFECTIVENESS & PARTNER (RW KPIs #6, #16) ═══
        # NOTE: LeadSource is very sparse (~25 records total). Use all-time data for
        # meaningful charts rather than FY-filtered (which yields ~5 records).
        "s_wr_source": sq(
            L
            + CLOSED
            + UF
            + RF
            + 'q = filter q by LeadSource != "";\n'
            + 'q = foreach q generate LeadSource, (case when IsWon == "true" then 1 else 0 end) as is_won, ARR;\n'
            + "q = group q by LeadSource;\n"
            + "q = foreach q generate LeadSource, count() as total, sum(is_won) as won, "
            + "(sum(is_won) / count()) * 100 as win_rate, sum(ARR) as sum_acv;\n"
            + "q = order q by sum_acv desc;\n"
            + "q = limit q 15;"
        ),
        "s_source_pipe": sq(
            L
            + OPEN
            + UF
            + RF
            + 'q = filter q by LeadSource != "";\n'
            + "q = group q by LeadSource;\n"
            + "q = foreach q generate LeadSource, sum(ARR) as sum_acv, count() as cnt;\n"
            + "q = order q by sum_acv desc;\n"
            + "q = limit q 15;"
        ),
        "s_partner_pipe": sq(
            L
            + UF
            + RF
            + 'q = foreach q generate (case when LeadSource != "" and LeadSource != "null" then "Sourced" else "No Source" end) as SourceBucket, ARR;\n'
            + "q = group q by SourceBucket;\n"
            + "q = foreach q generate SourceBucket, sum(ARR) as sum_acv, count() as cnt;\n"
            + "q = order q by sum_acv desc;"
        ),
        # ═══ PIPELINE VELOCITY INDEX by Quarter (Additive CRO) ═══
        "s_vel_qtr": sq(
            L
            + FY
            + CLOSED
            + UF
            + FYF
            + RF
            + f"q = foreach q generate {QTR} as Quarter, ARR, SalesCycleDuration, "
            + '(case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, "
            + "sum(is_won) as won_cnt, count() as total, avg(ARR) as avg_deal, "
            + "(case when count() > 0 then (sum(is_won) / count()) * 100 else 0 end) as win_rate, "
            + "(case when avg(SalesCycleDuration) > 0 then "
            + "(sum(is_won) * avg(ARR) * (sum(is_won) / count())) / avg(SalesCycleDuration) "
            + "else 0 end) as velocity;\n"
            + "q = order q by Quarter asc;"
        ),
        # ═══ V2: Advanced Visualizations ═══
        # Sankey: Stage → Final Outcome flow
        "s_sankey_stage": sq(
            L
            + FY
            + CLOSED
            + UF
            + TF
            + RF
            + "q = foreach q generate StageName as source, "
            + '(case when IsWon == "true" then "Won" else "Lost" end) as target, ARR;\n'
            + "q = group q by (source, target);\n"
            + "q = foreach q generate source, target, count() as cnt, sum(ARR) as total_arr;\n"
            + "q = order q by cnt desc;"
        ),
        # Treemap: Revenue by Region → Type
        "s_treemap_rev": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + 'q = filter q by SalesRegion != "";\n'
            + "q = group q by (SalesRegion, Type);\n"
            + "q = foreach q generate SalesRegion, Type, sum(ARR) as total_arr;\n"
            + "q = order q by total_arr desc;"
        ),
        # Heatmap: Stage × Region win rate matrix
        "s_heatmap_wr": sq(
            L
            + FY
            + CLOSED
            + UF
            + TF
            + FYF
            + 'q = filter q by SalesRegion != "";\n'
            + "q = group q by (StageName, SalesRegion);\n"
            + "q = foreach q generate StageName, SalesRegion, "
            + '(sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100 as win_rate;\n'
            + "q = order q by StageName asc;"
        ),
        # Bubble: Deal Age × WinScore × ARR (open pipeline)
        "s_bubble_deals": sq(
            L
            + FY
            + OPEN
            + UF
            + TF
            + RF
            + "q = foreach q generate Name, AgeInDays, WinScore, ARR, Type;\n"
            + "q = group q by (Name, AgeInDays, WinScore, Type);\n"
            + "q = foreach q generate Name, avg(AgeInDays) as deal_age, "
            + "avg(WinScore) as win_score, sum(ARR) as deal_arr, Type;\n"
            + "q = order q by deal_arr desc;\n"
            + "q = limit q 100;"
        ),
        # Area: Cumulative won ARR by month
        "s_area_cumul": sq(
            L
            + FY
            + WON
            + UF
            + TF
            + RF
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, sum(ARR) as monthly_arr;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # ═══ V2 Phase 6: Bullet Charts (target vs actual) ═══
        "s_bullet_coverage": sq(
            L
            + FY
            + UF
            + FYF
            + RF
            + 'q = foreach q generate ARR, (case when IsClosed == "false" then ARR else 0 end) as pipe, '
            + '(case when IsWon == "true" then ARR else 0 end) as won;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(case when sum(won) > 0 then sum(pipe) / sum(won) else 0 end) as coverage, "
            + "3 as target;"
        ),
        "s_bullet_winrate": sq(
            L
            + FY
            + CLOSED
            + UF
            + FYF
            + RF
            + 'q = foreach q generate (case when IsWon == "true" then 1 else 0 end) as is_won;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_won) / count()) * 100 as win_rate, 25 as target;"
        ),
        "s_bullet_avg_deal": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate avg(ARR) as avg_deal, 150000 as target;"
        ),
        # ═══ V2 Phase 8: Statistical Analysis ═══
        # 8A: Pipeline percentiles (box-plot data)
        "s_stat_percentiles": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "min(ARR) as min_arr, "
            + "percentile_disc(0.25) within group (order by ARR) as p25, "
            + "percentile_disc(0.50) within group (order by ARR) as median_arr, "
            + "percentile_disc(0.75) within group (order by ARR) as p75, "
            + "max(ARR) as max_arr, "
            + "avg(ARR) as mean_arr, stddev(ARR) as std_dev, count() as deal_count;"
        ),
        # 8B: Moving average on monthly pipeline
        "s_stat_ma_pipeline": sq(
            L
            + FY
            + OPEN
            + UF
            + FYF
            + RF
            + 'q = filter q by CreatedMonth >= "2024-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, sum(ARR) as monthly_arr, count() as deal_cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # 8D: Win rate regression (deal size effect)
        "s_stat_regression": sq(
            L
            + FY
            + CLOSED
            + UF
            + FYF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + 'avg(case when IsWon == "true" then 1 else 0 end) as avg_wr, '
            + "avg(ARR) as avg_arr, "
            + 'stddev(case when IsWon == "true" then 1 else 0 end) as std_wr, '
            + "stddev(ARR) as std_arr, "
            + "count() as pair_count;"
        ),
        # 8E: Revenue concentration (Pareto / cumulative)
        "s_stat_pareto": sq(
            L
            + FY
            + WON
            + UF
            + FYF
            + RF
            + "q = group q by AccountName;\n"
            + "q = foreach q generate AccountName, sum(ARR) as acct_arr;\n"
            + "q = order q by acct_arr desc;\n"
            + "q = limit q 50;"
        ),
        # ═══ V2 Phase 9: Python-precomputed velocity band ═══
        "s_velocity_band": sq(
            L
            + FY
            + CLOSED
            + UF
            + TF
            + RF
            + 'q = filter q by DealVelocityBand != "N/A";\n'
            + "q = group q by DealVelocityBand;\n"
            + "q = foreach q generate DealVelocityBand, count() as deal_count, "
            + "sum(ARR) as total_arr, avg(SalesCycleDuration) as avg_days;\n"
            + "q = order q by avg_days asc;"
        ),
        # ═══ Sales Performance Enhancements ═══
        # Decomposed Sales Velocity Formula: (won_count × win_rate × avg_deal) / avg_cycle
        "s_velocity_formula": sq(
            L
            + FY
            + CLOSED
            + UF
            + TF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as total_closed, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as won_count, '
            + 'sum(case when IsWon == "true" then ARR else 0 end) as won_arr, '
            + 'sum(case when IsWon == "true" then SalesCycleDuration else 0 end) as won_cycle_sum;\n'
            + "q = foreach q generate total_closed, won_count, "
            + "(case when total_closed > 0 then (won_count * 100 / total_closed) else 0 end) as win_rate, "
            + "(case when won_count > 0 then won_arr / won_count else 0 end) as avg_deal, "
            + "(case when won_count > 0 then won_cycle_sum / won_count else 0 end) as avg_cycle, "
            + "(case when (total_closed > 0 and won_cycle_sum > 0) then "
            + "(won_count * won_count * won_arr) / (total_closed * won_cycle_sum) "
            + "else 0 end) as velocity;"
        ),
        # Time-to-Close Distribution (histogram)
        "s_close_dist": sq(
            L
            + FY
            + WON
            + UF
            + TF
            + RF
            + "q = foreach q generate "
            + "(case "
            + 'when SalesCycleDuration < 30 then "a_0-30d" '
            + 'when SalesCycleDuration < 60 then "b_30-60d" '
            + 'when SalesCycleDuration < 90 then "c_60-90d" '
            + 'when SalesCycleDuration < 180 then "d_90-180d" '
            + 'when SalesCycleDuration < 365 then "e_180-365d" '
            + 'else "f_365d+" end) as CloseBand, ARR;\n'
            + "q = group q by CloseBand;\n"
            + "q = foreach q generate CloseBand, count() as deal_count, sum(ARR) as total_arr;\n"
            + "q = order q by CloseBand asc;"
        ),
        # Stale Deal Alerts (open deals stuck 30+ days in current stage)
        "s_stale_deals": sq(
            L
            + FY
            + OPEN
            + UF
            + TF
            + RF
            + "q = filter q by DaysInStage > 30;\n"
            + "q = foreach q generate Id, OppName, OwnerName, StageName, ARR, DaysInStage, AccountName;\n"
            + "q = order q by DaysInStage desc;\n"
            + "q = limit q 25;"
        ),
        # Stale deals KPI (count + ARR at risk)
        "s_stale_count": sq(
            L
            + FY
            + OPEN
            + UF
            + TF
            + RF
            + "q = filter q by DaysInStage > 30;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate count() as stale_count, sum(ARR) as stale_arr;"
        ),
        # Pipeline Movement (won vs lost ARR by month)
        "s_pipeline_movement": sq(
            L
            + FY
            + CLOSED
            + UF
            + TF
            + RF
            + 'q = filter q by CloseMonth >= "2025-07";\n'
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, "
            + 'sum(case when IsWon == "true" then ARR else 0 end) as won_arr, '
            + 'sum(case when IsWon == "false" then ARR else 0 end) as lost_arr;\n'
            + "q = order q by CloseMonth asc;"
        ),
        # Cohort-Based Conversion Funnel (by fiscal year)
        "s_cohort_conversion": sq(
            L
            + UF
            + TF
            + RF
            + "q = group q by FYLabel;\n"
            + "q = foreach q generate FYLabel, "
            + "count() as total, "
            + 'sum(case when HitStage1 == "Y" then 1 else 0 end) as hit_s1, '
            + 'sum(case when HitStage2 == "Y" then 1 else 0 end) as hit_s2, '
            + 'sum(case when HitStage3 == "Y" then 1 else 0 end) as hit_s3, '
            + 'sum(case when HitStage4 == "Y" then 1 else 0 end) as hit_s4, '
            + 'sum(case when HitStage5 == "Y" then 1 else 0 end) as hit_s5, '
            + 'sum(case when HitStage6 == "Y" then 1 else 0 end) as hit_s6;\n'
            + "q = order q by FYLabel asc;"
        ),
        # ═══ VIZ UPGRADE: Stage-to-Stage Transition Sankey ═══
        # Tracks deal flow between consecutive stages using HitStage flags
        "s_stage_transition": stage_transition_step(DS, FY + UF + RF),
        # ═══ VIZ UPGRADE: Win/Loss Reason Heatmap ═══
        # Reason × UnitGroup matrix colored by count for pattern discovery
        "s_reason_heatmap": sq(
            L
            + FY
            + CLOSED
            + UF
            + RF
            + 'q = filter q by WonLostReason != "" && WonLostReason != "null";\n'
            + "q = group q by (WonLostReason, UnitGroup);\n"
            + "q = foreach q generate WonLostReason, UnitGroup, count() as cnt, "
            + "sum(ARR) as total_arr;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══ VIZ UPGRADE: Win/Loss Reason Waterfall ═══
        # Shows impact of each reason on pipeline (positive=won reasons, negative=lost)
        "s_reason_waterfall": sq(
            L
            + FY
            + CLOSED
            + UF
            + RF
            + 'q = filter q by WonLostReason != "" && WonLostReason != "null";\n'
            + "q = group q by WonLostReason;\n"
            + "q = foreach q generate WonLostReason, "
            + 'sum(case when IsWon == "true" then ARR else -ARR end) as net_impact;\n'
            + "q = order q by net_impact desc;\n"
            + "q = limit q 15;"
        ),
        # ═══ VIZ UPGRADE: Dynamic KPI Threshold Metrics ═══
        # Computes KPI values + threshold indicators for dynamic coloring
        "s_kpi_thresholds": sq(
            L
            + FY
            + UF
            + FYF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + 'sum(case when IsWon == "true" then ARR else 0 end) as won_arr, '
            + 'sum(case when IsClosed == "false" then ARR else 0 end) as pipe_arr, '
            + "(case when count() > 0 then "
            + 'sum(case when IsWon == "true" then 1 else 0 end) * 100 / count() '
            + "else 0 end) as win_rate_pct, "
            + "avg(AgeInDays) as avg_age, "
            + "(case when sum(case when IsWon == \"true\" then ARR else 0 end) > 0 then "
            + "sum(case when IsClosed == \"false\" then ARR else 0 end) / "
            + "sum(case when IsWon == \"true\" then ARR else 0 end) else 0 end) as coverage_ratio;"
        ),
        # ═══ VIZ UPGRADE: Time-in-Stage Small Multiples ═══
        # DaysInStage distribution per StageName for bottleneck analysis
        "s_stage_time_dist": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + "q = foreach q generate StageName, "
            + "(case "
            + 'when DaysInStage < 7 then "a_0-7d" '
            + 'when DaysInStage < 14 then "b_7-14d" '
            + 'when DaysInStage < 30 then "c_14-30d" '
            + 'when DaysInStage < 60 then "d_30-60d" '
            + 'else "e_60d+" end) as TimeBand, ARR;\n'
            + "q = group q by (StageName, TimeBand);\n"
            + "q = foreach q generate StageName, TimeBand, count() as deal_count, sum(ARR) as total_arr;\n"
            + "q = order q by StageName asc, TimeBand asc;"
        ),
        # ═══ VIZ UPGRADE: Geographic Map with Region Toggle ═══
        # US state-level drill from geo dataset
        "s_geo_us": sq(
            f'q = load "{GEO_DS}";\n'
            + 'q = filter q by Country == "United States";\n'
            + "q = group q by State;\n"
            + "q = foreach q generate State, "
            + "sum(pipeline_arr) as pipeline_arr, "
            + "sum(opp_count) as opp_count;\n"
            + "q = order q by pipeline_arr desc;"
        ),
        # EMEA regional view
        "s_geo_emea": sq(
            f'q = load "{GEO_DS}";\n'
            + 'q = filter q by Region == "EMEA";\n'
            + "q = group q by Country;\n"
            + "q = foreach q generate Country, "
            + "sum(pipeline_arr) as pipeline_arr, "
            + "sum(opp_count) as opp_count;\n"
            + "q = order q by pipeline_arr desc;"
        ),
        # ═══ VIZ UPGRADE: Cross-Dimensional Trellis (metric × segment) ═══
        # Pipeline by UnitGroup × Region small multiples
        "s_trellis_unit_region": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + "q = group q by (UnitGroup, SalesRegion);\n"
            + "q = foreach q generate UnitGroup, SalesRegion, "
            + "sum(ARR) as sum_arr, count() as cnt;\n"
            + "q = order q by sum_arr desc;"
        ),
        # ═══ PAGE 9: Quantitative Growth & Risk (ML-Forward) ═══
        # Product × Region pipeline heatmap
        "s_growth_prod_region": sq(
            L
            + FY
            + OPEN
            + UF
            + "q = group q by (ProductFamily, SalesRegion);\n"
            + "q = foreach q generate ProductFamily, SalesRegion, "
            + "sum(ARR) as pipeline_arr, "
            + "count() as opp_count, "
            + "avg(WinScore) as avg_win_score;\n"
            + "q = order q by ProductFamily asc, SalesRegion asc;"
        ),
        # Win rate by ProductFamily × UnitGroup
        "s_growth_wr_matrix": sq(
            L
            + FY
            + CLOSED
            + UF
            + RF
            + "q = group q by (ProductFamily, UnitGroup);\n"
            + "q = foreach q generate ProductFamily, UnitGroup, "
            + "(sum(case when IsWon == \"true\" then 1 else 0 end) * 100 / count()) as win_rate, "
            + "sum(ARR) as total_arr, "
            + "count() as deal_count;\n"
            + "q = order q by ProductFamily asc, UnitGroup asc;"
        ),
        # Coverage bullet by ProductFamily
        "s_growth_coverage_bullet": sq(
            L
            + FY
            + UF
            + RF
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, "
            + "sum(case when IsClosed == \"false\" then ARR else 0 end) as pipeline, "
            + "sum(case when IsWon == \"true\" then ARR else 0 end) as won, "
            + "(case when sum(case when IsWon == \"true\" then ARR else 0 end) > 0 then "
            + "sum(case when IsClosed == \"false\" then ARR else 0 end) / "
            + "sum(case when IsWon == \"true\" then ARR else 0 end) else 0 end) as actual, "
            + "3 as target;\n"
            + "q = order q by pipeline desc;\n"
            + "q = limit q 10;"
        ),
        # Deal risk distribution by WinScoreBand
        "s_growth_risk_dist": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + "q = group q by WinScoreBand;\n"
            + "q = foreach q generate WinScoreBand, "
            + "count() as opp_count, "
            + "sum(ARR) as total_arr;\n"
            + "q = order q by (case "
            + "when WinScoreBand == \"Low\" then 1 "
            + "when WinScoreBand == \"Medium\" then 2 "
            + "else 3 end) asc;"
        ),
        # At-risk deals (low win score, high ARR)
        "s_growth_atrisk_deals": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + "q = filter q by WinScore < 40;\n"
            + "q = foreach q generate Name, OwnerName, ProductFamily, "
            + "SalesRegion, UnitGroup, StageName, "
            + "WinScore, ARR, AgeInDays, DaysInStage;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 20;"
        ),
        # Growth KPIs
        "s_growth_kpi": sq(
            L
            + FY
            + UF
            + RF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(case when IsClosed == \"false\" then ARR else 0 end) as pipeline_arr, "
            + "sum(case when IsWon == \"true\" then ARR else 0 end) as won_arr, "
            + "avg(WinScore) as avg_win_score, "
            + "sum(case when IsClosed == \"false\" && WinScore < 40 then ARR else 0 end) as at_risk_arr;"
        ),
    }


# Widget builders imported from crm_analytics_helpers


def build_widgets():
    # Shorthand for simple charts (backward compat)
    def ch(step, viz, title):
        return rich_chart(step, viz, title, [], [])

    w = {
        # ═══ PAGE 1: Executive Overview ═══
        "p1_nav1": nav_link("overview", "Overview", active=True),
        "p1_nav2": nav_link("trends", "Trends"),
        "p1_nav3": nav_link("crossdim", "Cross-Dim"),
        "p1_nav4": nav_link("geo", "Geographic"),
        "p1_nav5": nav_link("salesops", "Sales Ops"),
        "p1_nav6": nav_link("productmix", "Product Mix"),
        "p1_hdr": hdr(
            "Opportunity Management KPIs",
            "FY2026 | All values in EUR | Select a Unit Group or Type to filter",
        ),
        # Filter bar
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_type": pillbox("f_type", "Type"),
        "p1_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p1_f_region": pillbox("f_region", "Region"),
        # Hero KPIs (with conditional formatting)
        "p1_pipe": num_with_trend(
            "s_pipe_t",
            "sum_acv",
            "Open Pipeline (EUR)",
            "#0070D2",
            compact=True,
            size=28,
        ),
        "p1_cwon_arr": num_with_trend(
            "s_cwon_t",
            "sum_acv",
            "Closed Won ARR (EUR)",
            "#04844B",
            compact=True,
            size=28,
        ),
        "p1_wr": gauge(
            "s_wr",
            "win_rate",
            "Win Rate %",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 15, "color": "#D4504C"},
                {"start": 15, "stop": 25, "color": "#FFB75D"},
                {"start": 25, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p1_coverage": gauge(
            "s_pipe",
            "coverage",
            "Pipeline Coverage (x)",
            min_val=0,
            max_val=10,
            bands=[
                {"start": 0, "stop": 2, "color": "#D4504C"},
                {"start": 2, "stop": 3, "color": "#FFB75D"},
                {"start": 3, "stop": 10, "color": "#04844B"},
            ],
        ),
        # Secondary KPIs
        "p1_cwon_cnt": num_with_trend("s_cwon_cnt_t", "cnt", "Won Deals", "#04844B"),
        "p1_deal": num_with_trend(
            "s_deal_t", "avg_deal", "Avg Deal Size (EUR)", "#04844B", compact=True
        ),
        "p1_age": num("s_age", "avg_age", "Avg Opp Age (Days)", "#FF6600"),
        "p1_stale": num("s_age", "stale", "Stale Opps (>120d)", "#D4504C"),
        "p1_cycle": num("s_cyc", "avg_cycle", "Sales Cycle (Days)", "#0070D2"),
        "p1_apv_cnt": num("s_apv", "cnt", "Stage 3 Approvals", "#9050E9"),
        "p1_apv_acv": num("s_apv", "sum_acv", "Approved ARR (EUR)", "#9050E9", True),
        # Charts
        "p1_sec": section_label("Pipeline Breakdown"),
        "p1_ch_stg": funnel_chart(
            "s_stg_funnel", "Pipeline by Stage", "StageName", "sum_acv"
        ),
        "p1_ch_unit": rich_chart(
            "s_unit",
            "hbar",
            "Pipeline by Unit Group",
            ["UnitGroup"],
            ["sum_acv"],
            axis_title="ARR (EUR)",
        ),
        "p1_ch_fcat": rich_chart(
            "s_fcat",
            "comparisontable",
            "Forecast Category",
            ["ForecastCategory"],
            ["sum_acv", "cnt"],
        ),
        "p1_ch_type": rich_chart(
            "s_type",
            "donut",
            "Pipeline by Opp Type",
            ["Type"],
            ["sum_acv"],
            show_legend=True,
            show_pct=True,
        ),
        "p1_top10": rich_chart(
            "s_top10",
            "comparisontable",
            "Top 10 Opportunities",
            ["Name", "AccountName", "StageName"],
            ["ARR"],
        ),
        # YoY comparison
        "p1_sec_yoy": section_label("Year-over-Year Comparison"),
        "p1_yoy_cwon": rich_chart(
            "s_cwon_yoy",
            "column",
            "Closed Won ARR by FY",
            ["FYLabel"],
            ["sum_acv"],
            axis_title="ARR (EUR)",
        ),
        "p1_yoy_pipe": rich_chart(
            "s_pipe_yoy",
            "column",
            "Open Pipeline by FY",
            ["FYLabel"],
            ["sum_acv"],
            axis_title="ARR (EUR)",
        ),
        "p1_weighted": num(
            "s_weighted", "weighted", "Weighted Pipeline (EUR)", "#0070D2", True, 28
        ),
        # ═══ PAGE 2: Time Trends ═══
        "p2_nav1": nav_link("overview", "Overview"),
        "p2_nav2": nav_link("trends", "Trends", active=True),
        "p2_nav3": nav_link("crossdim", "Cross-Dim"),
        "p2_nav4": nav_link("geo", "Geographic"),
        "p2_nav5": nav_link("salesops", "Sales Ops"),
        "p2_nav6": nav_link("productmix", "Product Mix"),
        "p2_hdr": hdr(
            "Time Trends", "FY2026 | Monthly & Quarterly | Filters apply from Page 1"
        ),
        # Filter bar
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_type": pillbox("f_type", "Type"),
        "p2_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_sec_m": section_label("Monthly Trends"),
        "p2_ch_mpipe": rich_chart(
            "s_month_pipe",
            "area",
            "Monthly Open Pipeline",
            ["CloseMonth"],
            ["sum_acv"],
            axis_title="ARR (EUR)",
        ),
        "p2_ch_mwon": rich_chart(
            "s_month_won",
            "column",
            "Monthly Closed Won",
            ["CloseMonth"],
            ["sum_acv", "cnt"],
            show_legend=True,
            axis_title="ARR (EUR) / Count",
        ),
        "p2_ch_mnew": rich_chart(
            "s_month_new",
            "column",
            "New Opps Created (by Month)",
            ["CreatedMonth"],
            ["cnt", "sum_acv"],
            show_legend=True,
            axis_title="Count / ARR (EUR)",
        ),
        "p2_sec_q": section_label("Quarterly Trends"),
        "p2_ch_qtr": rich_chart(
            "s_qtr",
            "combo",
            "Quarterly Pipeline vs Won",
            ["Quarter"],
            ["pipeline", "closed_won"],
            show_legend=True,
            axis_title="ARR (EUR)",
            combo_config={
                "plotConfiguration": [
                    {"series": "pipeline", "chartType": "column"},
                    {"series": "closed_won", "chartType": "line"},
                ]
            },
        ),
        "p2_ch_qwr": rich_chart(
            "s_qtr_wr",
            "line",
            "Win Rate % by Quarter",
            ["Quarter"],
            ["win_rate"],
            axis_title="Win Rate %",
        ),
        # New monthly analytics
        "p2_sec_deal": section_label("Deal & Approval Analytics"),
        "p2_ch_mdeal": rich_chart(
            "s_month_deal",
            "line",
            "Won Avg Deal Size by Month",
            ["CloseMonth"],
            ["avg_deal"],
            axis_title="ARR (EUR)",
        ),
        "p2_ch_mapv": rich_chart(
            "s_month_apv",
            "column",
            "Commercial Approvals by Month",
            ["ApprovalMonth"],
            ["cnt"],
            axis_title="Count",
        ),
        "p2_ch_mregion": rich_chart(
            "s_month_region",
            "stackcolumn",
            "New Opps by Region (Monthly)",
            ["CreatedMonth"],
            ["cnt"],
            split=["SalesRegion"],
            show_legend=True,
            axis_title="Count",
        ),
        "p2_ch_won_tier": rich_chart(
            "s_won_tier",
            "column",
            "Won Deals by Value Tier",
            ["Tier"],
            ["cnt", "sum_acv"],
            show_legend=True,
            axis_title="Count / ARR",
        ),
        # Waterfall: Monthly pipeline change
        "p2_sec_waterfall": section_label("Pipeline Movement"),
        "p2_ch_waterfall": waterfall_chart(
            "s_waterfall", "Monthly Pipeline Change", "Month", "net_change"
        ),
        # ═══ PAGE 3: Cross-Dimensional ═══
        "p3_nav1": nav_link("overview", "Overview"),
        "p3_nav2": nav_link("trends", "Trends"),
        "p3_nav3": nav_link("crossdim", "Cross-Dim", active=True),
        "p3_nav4": nav_link("geo", "Geographic"),
        "p3_nav5": nav_link("salesops", "Sales Ops"),
        "p3_nav6": nav_link("productmix", "Product Mix"),
        "p3_hdr": hdr(
            "Cross-Dimensional Analysis", "FY2026 | Unit Group & Type Deep Dive"
        ),
        # Filter bar
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_type": pillbox("f_type", "Type"),
        "p3_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p3_f_region": pillbox("f_region", "Region"),
        "p3_sec_unit": section_label("By Unit Group"),
        "p3_ch_deal_unit": rich_chart(
            "s_deal_unit",
            "hbar",
            "Avg Deal Size",
            ["UnitGroup"],
            ["avg_deal"],
            axis_title="ARR (EUR)",
        ),
        "p3_ch_wr_unit": rich_chart(
            "s_wr_unit",
            "hbar",
            "Win Rate %",
            ["UnitGroup"],
            ["win_rate"],
            axis_title="%",
        ),
        "p3_ch_cyc_unit": rich_chart(
            "s_cyc_unit",
            "hbar",
            "Sales Cycle (Days)",
            ["UnitGroup"],
            ["avg_cycle"],
            axis_title="Days",
        ),
        "p3_ch_stg_unit": rich_chart(
            "s_stg_unit",
            "heatmap",
            "Pipeline: Stage x Unit Group",
            ["StageName"],
            ["sum_acv"],
            show_legend=True,
        ),
        "p3_sec_type": section_label("By Opp Type"),
        "p3_ch_wr_type": rich_chart(
            "s_wr_type",
            "column",
            "Win Rate % by Type",
            ["Type"],
            ["win_rate"],
            axis_title="%",
        ),
        "p3_ch_tier": rich_chart(
            "s_tier",
            "column",
            "Deal Value Tiers",
            ["Tier"],
            ["cnt", "sum_acv"],
            show_legend=True,
            axis_title="Count / ARR",
        ),
        "p3_sec_drill": section_label("Region Drill-Down"),
        "p3_ch_unit_reg": rich_chart(
            "s_unit_reg",
            "comparisontable",
            "Unit Group → Region",
            ["UnitGroup", "SalesRegion"],
            ["sum_acv", "cnt", "avg_deal"],
        ),
        # Conversion & Velocity section
        "p3_sec_conv": section_label("Conversion & Velocity"),
        "p3_ch_conv": funnel_chart(
            "s_conv_funnel", "Stage Conversion Funnel", "StageName", "conv_pct"
        ),
        "p3_ch_type_scatter": rich_chart(
            "s_type_eff",
            "scatter",
            "Type Effectiveness (Win Rate vs Avg Deal)",
            ["Type"],
            ["win_rate", "avg_deal"],
        ),
        "p3_ch_type_eff": rich_chart(
            "s_type_eff",
            "comparisontable",
            "Type Effectiveness Detail",
            ["Type"],
            ["win_rate", "avg_deal", "avg_cycle", "total"],
        ),
        # ═══ PAGE 4: Geographic ═══
        "p4_nav1": nav_link("overview", "Overview"),
        "p4_nav2": nav_link("trends", "Trends"),
        "p4_nav3": nav_link("crossdim", "Cross-Dim"),
        "p4_nav4": nav_link("geo", "Geographic", active=True),
        "p4_nav5": nav_link("salesops", "Sales Ops"),
        "p4_nav6": nav_link("productmix", "Product Mix"),
        "p4_hdr": hdr(
            "Geographic Pipeline View",
            "FY2026 | Pipeline by Country & Sales Region",
        ),
        # Filter bar
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_type": pillbox("f_type", "Type"),
        "p4_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p4_f_region": pillbox("f_region", "Region"),
        "p4_sec_map": section_label("Pipeline by Country"),
        "p4_ch_country": choropleth_chart(
            "s_geo_country",
            "Pipeline ARR by Country",
            "Country",
            "pipeline_arr",
        ),
        "p4_ch_region": rich_chart(
            "s_geo_region",
            "donut",
            "Pipeline by Sales Region",
            ["SalesRegion"],
            ["pipeline_arr"],
            show_legend=True,
            show_pct=True,
        ),
        "p4_sec_detail": section_label("Country Detail"),
        "p4_ch_scatter": rich_chart(
            "s_geo_bubble",
            "scatter",
            "Country Bubble: Deal Count vs Win Rate",
            ["Country"],
            ["opp_count", "win_rate"],
            show_legend=True,
        ),
        "p4_ch_table": rich_chart(
            "s_geo_top_countries",
            "comparisontable",
            "Top 15 Countries — Full Metrics",
            ["Country"],
            [
                "pipeline_arr",
                "won_arr",
                "opp_count",
                "won_count",
                "avg_deal",
                "win_rate",
            ],
        ),
        # ═══ PAGE 5: Sales Operations ═══
        "p5_nav1": nav_link("overview", "Overview"),
        "p5_nav2": nav_link("trends", "Trends"),
        "p5_nav3": nav_link("crossdim", "Cross-Dim"),
        "p5_nav4": nav_link("geo", "Geographic"),
        "p5_nav5": nav_link("salesops", "Sales Ops", active=True),
        "p5_nav6": nav_link("productmix", "Product Mix"),
        "p5_hdr": hdr(
            "Sales Operations",
            "FY2026 | Forecast Accuracy, Pipeline Health, Win/Loss Analysis",
        ),
        # Filter bar
        "p5_f_unit": pillbox("f_unit", "Unit Group"),
        "p5_f_type": pillbox("f_type", "Type"),
        "p5_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p5_f_region": pillbox("f_region", "Region"),
        # Forecast & Quota section
        "p5_sec_forecast": section_label("Forecast & Quota"),
        "p5_acc": gauge(
            "s_forecast_acc",
            "accuracy",
            "Forecast Accuracy %",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 90, "color": "#D4504C"},
                {"start": 90, "stop": 95, "color": "#FFB75D"},
                {"start": 95, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p5_acc_chart": rich_chart(
            "s_forecast_acc",
            "column",
            "Forecast Accuracy by FY",
            ["FYLabel"],
            ["accuracy"],
            axis_title="Accuracy %",
        ),
        "p5_apv_time": num(
            "s_apv_close", "avg_days", "Avg Days to Close (Won)", "#04844B"
        ),
        "p5_vel": num(
            "s_vel", "velocity", "Pipeline Velocity (EUR/day)", "#0070D2", True
        ),
        # Pipeline Health section
        "p5_sec_health": section_label("Pipeline Health"),
        "p5_aging": rich_chart(
            "s_aging",
            "stackhbar",
            "Pipeline Aging Bands",
            ["AgeBand"],
            ["cnt", "sum_acv"],
            show_legend=True,
            axis_title="Count / ARR",
        ),
        "p5_stale": rich_chart(
            "s_stale_list",
            "comparisontable",
            "Top 15 Stale Opportunities (>120 Days)",
            ["Name", "AccountName", "StageName"],
            ["ARR", "AgeInDays"],
        ),
        # Cumulative Performance section
        "p5_sec_cumul": section_label("Cumulative Performance"),
        "p5_apv_trend": rich_chart(
            "s_month_apv",
            "line",
            "Monthly Approval Trend",
            ["ApprovalMonth"],
            ["cnt"],
            axis_title="Count",
        ),
        # ═══ REP PERFORMANCE & ATTAINMENT ═══
        "p5_sec_attain": section_label("Rep Performance & Attainment"),
        "p5_ch_attain": rich_chart(
            "s_rep_attain",
            "column",
            "Quota Attainment Distribution",
            ["AttainBand"],
            ["cnt"],
            axis_title="Number of Reps",
        ),
        # ═══ PIPELINE VELOCITY INDEX (Additive CRO) ═══
        "p5_sec_vel_idx": section_label("Pipeline Velocity Index (CRO)"),
        "p5_ch_vel_qtr": rich_chart(
            "s_vel_qtr",
            "combo",
            "Velocity Index by Quarter (EUR/day)",
            ["Quarter"],
            ["velocity", "win_rate"],
            show_legend=True,
            axis_title="Velocity / Win Rate",
            combo_config={
                "plotConfiguration": [
                    {"series": "velocity", "chartType": "column"},
                    {"series": "win_rate", "chartType": "line"},
                ]
            },
        ),
        # ═══ SOURCE EFFECTIVENESS & PARTNER (RW KPIs #6, #16) ═══
        "p5_sec_source": section_label("Source Effectiveness (RW 1.4.6)"),
        "p5_ch_wr_source": rich_chart(
            "s_wr_source",
            "hbar",
            "Win Rate by Lead Source (All-Time)",
            ["LeadSource"],
            ["win_rate"],
            axis_title="Win Rate %",
        ),
        "p5_ch_source_pipe": rich_chart(
            "s_source_pipe",
            "column",
            "Open Pipeline ARR by Lead Source (All-Time)",
            ["LeadSource"],
            ["sum_acv"],
            axis_title="ARR (EUR)",
        ),
        "p5_ch_source_tbl": rich_chart(
            "s_wr_source",
            "comparisontable",
            "Source Effectiveness (All-Time) — Win Rate, ARR, Deal Count",
            ["LeadSource"],
            ["win_rate", "sum_acv", "total", "won"],
        ),
        "p5_sec_partner": section_label("Partner & Channel (RW 1.4.16)"),
        "p5_ch_partner_donut": rich_chart(
            "s_partner_pipe",
            "donut",
            "Pipeline: Sourced vs Unsourced",
            ["SourceBucket"],
            ["sum_acv"],
            show_legend=True,
            show_pct=True,
        ),
        # ═══ SALES PERFORMANCE ENHANCEMENTS ═══
        # Decomposed Velocity Formula (4 component KPIs + velocity)
        "p5_sec_vel_formula": section_label("Decomposed Sales Velocity Formula"),
        "p5_vel_won_count": num(
            "s_velocity_formula", "won_count", "Won Deals", "#0070D2", compact=False
        ),
        "p5_vel_win_rate": num(
            "s_velocity_formula", "win_rate", "Win Rate %", "#04844B", compact=False
        ),
        "p5_vel_avg_deal": num(
            "s_velocity_formula",
            "avg_deal",
            "Avg Deal Size (EUR)",
            "#54698D",
            compact=True,
        ),
        "p5_vel_avg_cycle": num(
            "s_velocity_formula",
            "avg_cycle",
            "Avg Cycle (Days)",
            "#FFB75D",
            compact=False,
        ),
        "p5_vel_velocity": num(
            "s_velocity_formula",
            "velocity",
            "Velocity (EUR/day)",
            "#091A3E",
            compact=True,
        ),
        # Time-to-Close Distribution
        "p5_sec_close_dist": section_label("Time-to-Close Distribution"),
        "p5_ch_close_dist": rich_chart(
            "s_close_dist",
            "column",
            "Deal Close Duration Distribution (Won Deals)",
            ["CloseBand"],
            ["deal_count", "total_arr"],
            show_legend=True,
            axis_title="Count / ARR",
        ),
        # Stale Deal Alerts
        "p5_sec_stale": section_label("Stale Deal Alerts (30+ Days in Stage)"),
        "p5_stale_count": num(
            "s_stale_count", "stale_count", "Stale Deals", "#D4504C", compact=False
        ),
        "p5_stale_arr": num(
            "s_stale_count", "stale_arr", "At-Risk ARR (EUR)", "#D4504C", compact=True
        ),
        "p5_tbl_stale": rich_chart(
            "s_stale_deals",
            "comparisontable",
            "Top 25 Stale Deals (Open, 30+ Days in Stage)",
            ["OppName"],
            ["OwnerName", "StageName", "ARR", "DaysInStage", "AccountName"],
        ),
        # Pipeline Movement (Won vs Lost by Month)
        "p5_sec_pipe_move": section_label("Pipeline Movement (Won vs Lost by Month)"),
        "p5_ch_pipe_move": rich_chart(
            "s_pipeline_movement",
            "column",
            "Monthly Won vs Lost ARR",
            ["CloseMonth"],
            ["won_arr", "lost_arr"],
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        # Cohort Conversion Funnel
        "p5_sec_cohort": section_label("Cohort Conversion Funnel (by Fiscal Year)"),
        "p5_ch_cohort": rich_chart(
            "s_cohort_conversion",
            "column",
            "Stage Progression by FY Cohort",
            ["FYLabel"],
            ["total", "hit_s1", "hit_s2", "hit_s3", "hit_s4", "hit_s5", "hit_s6"],
            show_legend=True,
            axis_title="Deal Count",
        ),
        # ═══ PAGE 6: Product Mix ═══
        "p6_nav1": nav_link("overview", "Overview"),
        "p6_nav2": nav_link("trends", "Trends"),
        "p6_nav3": nav_link("crossdim", "Cross-Dim"),
        "p6_nav4": nav_link("geo", "Geographic"),
        "p6_nav5": nav_link("salesops", "Sales Ops"),
        "p6_nav6": nav_link("productmix", "Product Mix", active=True),
        "p6_hdr": hdr(
            "Product Mix Analysis",
            "FY2026 | Revenue by Product Family",
        ),
        # Filter bar
        "p6_f_unit": pillbox("f_unit", "Unit Group"),
        "p6_f_type": pillbox("f_type", "Type"),
        "p6_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p6_f_region": pillbox("f_region", "Region"),
        "p6_ch_pipe_product": rich_chart(
            "s_product_pipe",
            "donut",
            "Open Pipeline by Product Family",
            ["ProductFamily"],
            ["sum_arr"],
            show_legend=True,
            show_pct=True,
        ),
        "p6_ch_won_product": rich_chart(
            "s_product_won",
            "hbar",
            "Closed Won ARR by Product Family",
            ["ProductFamily"],
            ["sum_arr"],
            axis_title="ARR (EUR)",
        ),
        "p6_ch_wr_product": rich_chart(
            "s_product_wr",
            "comparisontable",
            "Win Rate by Product Family",
            ["ProductFamily"],
            ["total", "won_cnt", "win_rate", "won_arr"],
        ),
        "p6_ch_product_type": rich_chart(
            "s_product_type",
            "stackhbar",
            "Product x Deal Type",
            ["ProductFamily"],
            ["sum_arr"],
            split=["Type"],
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        "p6_ch_product_trend": rich_chart(
            "s_product_trend",
            "stackcolumn",
            "Product Family Pipeline by Quarter",
            ["Quarter"],
            ["sum_arr"],
            split=["ProductFamily"],
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
    }

    # ── Phase 6: Reference lines ──────────────────────────────────────────
    from crm_analytics_helpers import add_reference_line

    add_reference_line(w["p2_ch_mpipe"], 500000, "Monthly Target", "#D4504C", "dashed")
    add_reference_line(w["p2_ch_qwr"], 25, "Target 25%", "#D4504C", "dashed")
    # KPI 11: New opps 100/mo target
    add_reference_line(w["p2_ch_mnew"], 100, "100/mo Target", "#04844B", "dashed")

    # ── Phase 7: Embedded table actions ──────────────────────────────────
    from crm_analytics_helpers import add_table_action

    add_table_action(w["p1_top10"], "salesforceActions", "Opportunity", "Id")
    add_table_action(w["p5_stale"], "salesforceActions", "Opportunity", "Id")
    add_table_action(w["p5_tbl_stale"], "salesforceActions", "Opportunity", "Id")

    # ═══ V2 PAGE 7: Advanced Analytics ═══
    w["p7_nav1"] = nav_link("overview", "Overview")
    w["p7_nav2"] = nav_link("trends", "Trends")
    w["p7_nav3"] = nav_link("crossdim", "Cross-Dim")
    w["p7_nav4"] = nav_link("geo", "Geographic")
    w["p7_nav5"] = nav_link("salesops", "Sales Ops")
    w["p7_nav6"] = nav_link("productmix", "Product Mix")
    w["p7_nav7"] = nav_link("advanalytics", "Advanced", active=True)
    w["p7_hdr"] = hdr(
        "Advanced Analytics",
        "Stage Flow | Revenue Composition | Risk Matrix | Deal Intelligence",
    )
    w["p7_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p7_f_type"] = pillbox("f_type", "Type")
    w["p7_f_fy"] = pillbox("f_fy", "Fiscal Year")
    w["p7_f_region"] = pillbox("f_region", "Region")
    # Sankey: Stage → Won/Lost flow
    w["p7_sec_sankey"] = section_label("Stage Outcome Flow")
    w["p7_ch_sankey"] = sankey_chart("s_sankey_stage", "Deal Flow: Stage → Won/Lost")
    # Treemap: Revenue by Region → Type
    w["p7_sec_treemap"] = section_label("Revenue Composition")
    w["p7_ch_treemap"] = treemap_chart(
        "s_treemap_rev",
        "Won Revenue by Region & Type",
        ["SalesRegion", "Type"],
        "total_arr",
    )
    # Heatmap: Stage × Region win rate
    w["p7_sec_heatmap"] = section_label("Win Rate Matrix")
    w["p7_ch_heatmap_wr"] = heatmap_chart(
        "s_heatmap_wr", "Win Rate % by Stage × Region"
    )
    # Bubble: Deal intelligence
    w["p7_sec_bubble"] = section_label("Deal Intelligence")
    w["p7_ch_bubble"] = bubble_chart(
        "s_bubble_deals", "Open Deals: Age vs WinScore (size = ARR)"
    )
    # Area: Cumulative won ARR
    w["p7_sec_area"] = section_label("Cumulative Won ARR")
    w["p7_ch_area"] = area_chart(
        "s_area_cumul",
        "Cumulative Closed Won ARR by Month",
        stacked=False,
        show_legend=False,
        axis_title="EUR",
    )

    # ═══ V2 PAGE 8: Bullet Charts & Statistical Analysis ═══
    w["p8_nav1"] = nav_link("overview", "Overview")
    w["p8_nav2"] = nav_link("trends", "Trends")
    w["p8_nav3"] = nav_link("crossdim", "Cross-Dim")
    w["p8_nav4"] = nav_link("geo", "Geographic")
    w["p8_nav5"] = nav_link("salesops", "Sales Ops")
    w["p8_nav6"] = nav_link("productmix", "Product Mix")
    w["p8_nav7"] = nav_link("advanalytics", "Advanced")
    w["p8_nav8"] = nav_link("statsanalysis", "Statistics", active=True)
    w["p8_hdr"] = hdr(
        "Statistical Analysis & Targets",
        "Bullet KPIs | Percentiles | Moving Averages | Regression | Pareto",
    )
    w["p8_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p8_f_type"] = pillbox("f_type", "Type")
    w["p8_f_fy"] = pillbox("f_fy", "Fiscal Year")
    w["p8_f_region"] = pillbox("f_region", "Region")
    # Bullet charts
    w["p8_sec_bullet"] = section_label("Target vs Actual KPIs")
    w["p8_bullet_coverage"] = bullet_chart(
        "s_bullet_coverage",
        "Pipeline Coverage (Target: 3x)",
        axis_title="Coverage Ratio",
    )
    w["p8_bullet_winrate"] = bullet_chart(
        "s_bullet_winrate", "Win Rate (Target: 25%)", axis_title="Win Rate %"
    )
    w["p8_bullet_avg_deal"] = bullet_chart(
        "s_bullet_avg_deal", "Avg Deal Size (Target: €150K)", axis_title="EUR"
    )
    # Stats: Percentile summary
    w["p8_sec_stats"] = section_label("Pipeline Distribution Statistics")
    w["p8_stat_pct"] = rich_chart(
        "s_stat_percentiles",
        "comparisontable",
        "Pipeline ARR Percentiles (P25/Median/P75/Max)",
        [],
        [
            "min_arr",
            "p25",
            "median_arr",
            "p75",
            "max_arr",
            "mean_arr",
            "std_dev",
            "deal_count",
        ],
    )
    # Stats: Moving average chart
    w["p8_sec_ma"] = section_label("Monthly Pipeline Trend")
    w["p8_stat_ma"] = rich_chart(
        "s_stat_ma_pipeline",
        "combo",
        "Monthly Pipeline ARR & Deal Count",
        ["CreatedMonth"],
        ["monthly_arr", "deal_cnt"],
        axis_title="ARR (EUR)",
    )
    # Stats: Regression summary
    w["p8_stat_regression"] = rich_chart(
        "s_stat_regression",
        "comparisontable",
        "Win Rate vs Deal Size Statistics",
        [],
        ["avg_wr", "avg_arr", "std_wr", "std_arr", "pair_count"],
    )
    # Stats: Pareto / revenue concentration
    w["p8_sec_pareto"] = section_label("Revenue Concentration (Pareto)")
    w["p8_stat_pareto"] = rich_chart(
        "s_stat_pareto",
        "hbar",
        "Won ARR by Account (Top 50)",
        ["AccountName"],
        ["acct_arr"],
        axis_title="ARR (EUR)",
    )

    # Phase 9: Python-precomputed velocity band visualization
    w["p8_sec_velocity"] = section_label("Deal Velocity Bands (Python-Computed)")
    w["p8_velocity_bar"] = rich_chart(
        "s_velocity_band",
        "column",
        "Closed Deals by Velocity Band (Fast/Normal/Slow)",
        ["DealVelocityBand"],
        ["deal_count"],
        axis_title="Deal Count",
    )
    w["p8_velocity_arr"] = rich_chart(
        "s_velocity_band",
        "donut",
        "ARR by Velocity Band",
        ["DealVelocityBand"],
        ["total_arr"],
    )

    # Add nav7 link (Advanced) to pages 1-6
    for px in range(1, 7):
        w[f"p{px}_nav7"] = nav_link("advanalytics", "Advanced")
    # Add nav8 link (Statistics) to pages 1-7
    for px in range(1, 8):
        w[f"p{px}_nav8"] = nav_link("statsanalysis", "Statistics")

    # ═══ VIZ UPGRADE: Dynamic KPI Tiles with Threshold-Based Coloring ═══
    # Win Rate tile with dynamic red/amber/green based on percentage
    w["p1_wr_dynamic"] = num_dynamic_color(
        "s_kpi_thresholds",
        "win_rate_pct",
        "Win Rate %",
        thresholds=[(15, "#D4504C"), (25, "#FFB75D"), (100, "#04844B")],
        compact=False,
        size=28,
    )
    # Coverage ratio with dynamic coloring
    w["p1_coverage_dynamic"] = num_dynamic_color(
        "s_kpi_thresholds",
        "coverage_ratio",
        "Pipeline Coverage (x)",
        thresholds=[(2, "#D4504C"), (3, "#FFB75D"), (10, "#04844B")],
        compact=False,
        size=28,
    )
    # Average deal age with dynamic coloring (lower is better)
    w["p1_age_dynamic"] = num_dynamic_color(
        "s_kpi_thresholds",
        "avg_age",
        "Avg Opp Age (Days)",
        thresholds=[(60, "#04844B"), (120, "#FFB75D"), (999, "#D4504C")],
        compact=False,
        size=24,
    )

    # ═══ VIZ UPGRADE: Stage Transition Sankey (Advanced Analytics page) ═══
    w["p7_sec_stage_flow"] = section_label("Stage-to-Stage Deal Flow")
    w["p7_ch_stage_flow"] = sankey_chart(
        "s_stage_transition", "Deal Flow: Stage Transitions"
    )

    # ═══ VIZ UPGRADE: Win/Loss Reason Analysis ═══
    w["p7_sec_reasons"] = section_label("Win/Loss Reason Analysis")
    w["p7_ch_reason_heatmap"] = heatmap_chart(
        "s_reason_heatmap", "Win/Loss Reasons × Unit Group"
    )
    w["p7_ch_reason_waterfall"] = waterfall_chart(
        "s_reason_waterfall",
        "Win/Loss Reason Impact (Net ARR)",
        "WonLostReason",
        "net_impact",
        axis_label="Net ARR Impact (EUR)",
    )

    # ═══ VIZ UPGRADE: Time-in-Stage Distribution (trellis by stage) ═══
    w["p7_sec_stage_time"] = section_label("Time-in-Stage Distribution")
    w["p7_ch_stage_time"] = rich_chart(
        "s_stage_time_dist",
        "stackcolumn",
        "Deal Count by Time Band (per Stage)",
        ["TimeBand"],
        ["deal_count"],
        trellis=["StageName"],
        show_legend=True,
        axis_title="Deals",
    )

    # ═══ VIZ UPGRADE: Geographic Map Toggle Views ═══
    w["p4_sec_map_us"] = section_label("US State Pipeline View")
    w["p4_ch_map_us"] = choropleth_chart(
        "s_geo_us", "US Pipeline by State", "State", "pipeline_arr", map_type="USA"
    )
    w["p4_sec_map_emea"] = section_label("EMEA Regional Pipeline View")
    w["p4_ch_map_emea"] = choropleth_chart(
        "s_geo_emea", "EMEA Pipeline by Country", "Country", "pipeline_arr", map_type="Europe"
    )

    # ═══ VIZ UPGRADE: Cross-Dimensional Trellis ═══
    w["p3_sec_trellis"] = section_label("Pipeline by Unit Group × Region")
    w["p3_ch_trellis"] = rich_chart(
        "s_trellis_unit_region",
        "column",
        "Pipeline Distribution (Small Multiples)",
        ["SalesRegion"],
        ["sum_arr"],
        trellis=["UnitGroup"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )

    # ═══ VIZ UPGRADE: Add table actions on drill tables ═══
    # Enable View Record / Create Task on Top 10 Opps table
    add_table_action(w["p1_top10"])
    # Enable actions on stale deals table
    if "p5_tbl_stale" in w:
        add_table_action(w["p5_tbl_stale"])

    # ═══ PAGE 9: Quantitative Growth & Risk (ML-Forward) ═══
    w["p9_hdr"] = hdr(
        "Quantitative Growth & Risk",
        "Product × Region growth heatmaps, win probability risk, coverage analysis",
    )
    w["p9_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p9_f_region"] = pillbox("f_region", "Region")
    # Growth KPIs
    w["p9_kpi_pipeline"] = num(
        "s_growth_kpi", "pipeline_arr", "Open Pipeline", "#0070D2", compact=True, size=28,
    )
    w["p9_kpi_won"] = num(
        "s_growth_kpi", "won_arr", "Won ARR", "#04844B", compact=True, size=28,
    )
    w["p9_kpi_risk"] = num_dynamic_color(
        "s_growth_kpi", "at_risk_arr", "At-Risk Pipeline ARR",
        thresholds=[(500000, "#04844B"), (2000000, "#FFB75D"), (100000000, "#D4504C")],
        compact=True, size=28,
    )
    # Product × Region heatmap
    w["p9_sec_prod_region"] = section_label("Pipeline by Product × Region")
    w["p9_ch_prod_region"] = heatmap_chart(
        "s_growth_prod_region", "Pipeline ARR: Product Family × Sales Region"
    )
    # Win rate matrix
    w["p9_sec_wr_matrix"] = section_label("Win Rate by Product × Unit Group")
    w["p9_ch_wr_matrix"] = heatmap_chart(
        "s_growth_wr_matrix", "Win Rate %: Product Family × Unit Group"
    )
    # Coverage bullet by product
    w["p9_sec_coverage"] = section_label("Pipeline Coverage by Product (vs 3x Target)")
    w["p9_ch_coverage"] = bullet_chart(
        "s_growth_coverage_bullet",
        "Coverage Ratio by Product Family",
        axis_title="Coverage Ratio",
    )
    # Risk distribution
    w["p9_sec_risk_dist"] = section_label("Deal Risk Distribution (Win Score)")
    w["p9_ch_risk_dist"] = rich_chart(
        "s_growth_risk_dist", "donut",
        "Open Pipeline by Win Score Band",
        ["WinScoreBand"], ["total_arr"],
        show_legend=True,
    )
    # At-risk deals table
    w["p9_sec_atrisk"] = section_label("At-Risk Deals (Win Score < 40)")
    w["p9_tbl_atrisk"] = rich_chart(
        "s_growth_atrisk_deals", "comparisontable",
        "Low Win Probability Deals",
        ["Name", "OwnerName", "ProductFamily", "SalesRegion", "StageName"],
        ["WinScore", "ARR", "AgeInDays", "DaysInStage"],
    )
    add_table_action(w["p9_tbl_atrisk"])

    return w


def build_layout():
    p1 = nav_row("p1", 8) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_fy", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Hero KPIs
        {"name": "p1_pipe", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_cwon_arr", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_wr", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_coverage", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        # Secondary KPIs
        {"name": "p1_cwon_cnt", "row": 9, "column": 0, "colspan": 2, "rowspan": 3},
        {"name": "p1_deal", "row": 9, "column": 2, "colspan": 2, "rowspan": 3},
        {"name": "p1_age", "row": 9, "column": 4, "colspan": 2, "rowspan": 3},
        {"name": "p1_stale", "row": 9, "column": 6, "colspan": 2, "rowspan": 3},
        {"name": "p1_cycle", "row": 9, "column": 8, "colspan": 2, "rowspan": 3},
        {"name": "p1_apv_cnt", "row": 9, "column": 10, "colspan": 2, "rowspan": 3},
        {"name": "p1_apv_acv", "row": 12, "column": 0, "colspan": 6, "rowspan": 3},
        {"name": "p1_weighted", "row": 12, "column": 6, "colspan": 3, "rowspan": 3},
        # Charts
        {"name": "p1_sec", "row": 15, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_stg", "row": 16, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_ch_unit", "row": 16, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p1_ch_fcat", "row": 24, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_ch_type", "row": 24, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p1_top10", "row": 32, "column": 0, "colspan": 12, "rowspan": 8},
        # YoY section
        {"name": "p1_sec_yoy", "row": 40, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_yoy_cwon", "row": 41, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_yoy_pipe", "row": 41, "column": 6, "colspan": 6, "rowspan": 8},
        # VIZ UPGRADE: Dynamic KPI Tiles with Threshold-Based Coloring
        {"name": "p1_wr_dynamic", "row": 49, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_coverage_dynamic", "row": 49, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_age_dynamic", "row": 49, "column": 8, "colspan": 4, "rowspan": 4},
    ]

    p2 = nav_row("p2", 8) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_fy", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p2_sec_m", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_mpipe", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_mwon", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_mnew", "row": 14, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p2_sec_q", "row": 22, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_qtr", "row": 23, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_qwr", "row": 23, "column": 6, "colspan": 6, "rowspan": 8},
        # New: Deal & Approval analytics
        {"name": "p2_sec_deal", "row": 31, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_mdeal", "row": 32, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_mapv", "row": 32, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_mregion", "row": 40, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_won_tier", "row": 40, "column": 6, "colspan": 6, "rowspan": 8},
        # Waterfall section
        {
            "name": "p2_sec_waterfall",
            "row": 48,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p2_ch_waterfall",
            "row": 49,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
    ]

    p3 = nav_row("p3", 8) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_fy", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p3_sec_unit", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_deal_unit", "row": 6, "column": 0, "colspan": 4, "rowspan": 8},
        {"name": "p3_ch_wr_unit", "row": 6, "column": 4, "colspan": 4, "rowspan": 8},
        {"name": "p3_ch_cyc_unit", "row": 6, "column": 8, "colspan": 4, "rowspan": 8},
        {"name": "p3_ch_stg_unit", "row": 14, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p3_sec_type", "row": 22, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_wr_type", "row": 23, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p3_ch_tier", "row": 23, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p3_sec_drill", "row": 31, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_unit_reg", "row": 32, "column": 0, "colspan": 12, "rowspan": 8},
        # Conversion & Velocity section (funnel + hbar replaces 11 number tiles)
        {"name": "p3_sec_conv", "row": 40, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_conv", "row": 41, "column": 0, "colspan": 12, "rowspan": 8},
        {
            "name": "p3_ch_type_scatter",
            "row": 49,
            "column": 0,
            "colspan": 6,
            "rowspan": 8,
        },
        {"name": "p3_ch_type_eff", "row": 49, "column": 6, "colspan": 6, "rowspan": 8},
        # VIZ UPGRADE: Cross-Dimensional Trellis (Small Multiples)
        {"name": "p3_sec_trellis", "row": 57, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_trellis", "row": 58, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    p4 = nav_row("p4", 8) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_fy", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p4_sec_map", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_country", "row": 6, "column": 0, "colspan": 6, "rowspan": 12},
        {"name": "p4_ch_region", "row": 6, "column": 6, "colspan": 6, "rowspan": 12},
        {"name": "p4_sec_detail", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_scatter", "row": 19, "column": 0, "colspan": 12, "rowspan": 10},
        {"name": "p4_ch_table", "row": 29, "column": 0, "colspan": 12, "rowspan": 10},
        # VIZ UPGRADE: Regional Map Drill-Down Views
        {"name": "p4_sec_map_us", "row": 39, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_map_us", "row": 40, "column": 0, "colspan": 6, "rowspan": 12},
        {"name": "p4_sec_map_emea", "row": 39, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p4_ch_map_emea", "row": 40, "column": 6, "colspan": 6, "rowspan": 12},
    ]

    p5 = nav_row("p5", 8) + [
        {"name": "p5_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p5_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_fy", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Forecast & Quota
        {"name": "p5_sec_forecast", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_acc", "row": 6, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p5_acc_chart", "row": 6, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p5_apv_time", "row": 6, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p5_vel", "row": 6, "column": 9, "colspan": 3, "rowspan": 4},
        # Pipeline Health
        {"name": "p5_sec_health", "row": 10, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_aging", "row": 11, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p5_stale", "row": 11, "column": 6, "colspan": 6, "rowspan": 8},
        # Cumulative Performance
        {"name": "p5_sec_cumul", "row": 38, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_apv_trend", "row": 39, "column": 0, "colspan": 12, "rowspan": 8},
        # Rep Performance & Attainment
        {"name": "p5_sec_attain", "row": 65, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_attain", "row": 66, "column": 0, "colspan": 12, "rowspan": 8},
        # Pipeline Velocity Index (Additive CRO)
        {"name": "p5_sec_vel_idx", "row": 75, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_vel_qtr", "row": 76, "column": 0, "colspan": 12, "rowspan": 8},
        # Source Effectiveness & Partner
        {"name": "p5_sec_source", "row": 85, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_wr_source", "row": 86, "column": 0, "colspan": 6, "rowspan": 8},
        {
            "name": "p5_ch_source_pipe",
            "row": 86,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        },
        {
            "name": "p5_ch_source_tbl",
            "row": 94,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        {
            "name": "p5_sec_partner",
            "row": 105,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p5_ch_partner_donut",
            "row": 106,
            "column": 0,
            "colspan": 6,
            "rowspan": 8,
        },
        # ═══ Sales Performance Enhancements ═══
        # Decomposed Velocity Formula
        {
            "name": "p5_sec_vel_formula",
            "row": 115,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p5_vel_won_count",
            "row": 116,
            "column": 0,
            "colspan": 2,
            "rowspan": 4,
        },
        {
            "name": "p5_vel_win_rate",
            "row": 116,
            "column": 2,
            "colspan": 2,
            "rowspan": 4,
        },
        {
            "name": "p5_vel_avg_deal",
            "row": 116,
            "column": 4,
            "colspan": 3,
            "rowspan": 4,
        },
        {
            "name": "p5_vel_avg_cycle",
            "row": 116,
            "column": 7,
            "colspan": 2,
            "rowspan": 4,
        },
        {
            "name": "p5_vel_velocity",
            "row": 116,
            "column": 9,
            "colspan": 3,
            "rowspan": 4,
        },
        # Time-to-Close Distribution
        {
            "name": "p5_sec_close_dist",
            "row": 120,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p5_ch_close_dist",
            "row": 121,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Stale Deal Alerts
        {"name": "p5_sec_stale", "row": 129, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_stale_count", "row": 130, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p5_stale_arr", "row": 130, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p5_tbl_stale", "row": 134, "column": 0, "colspan": 12, "rowspan": 10},
        # Pipeline Movement
        {
            "name": "p5_sec_pipe_move",
            "row": 144,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p5_ch_pipe_move",
            "row": 145,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Cohort Conversion Funnel
        {"name": "p5_sec_cohort", "row": 153, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_cohort", "row": 154, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    p6 = nav_row("p6", 8) + [
        {"name": "p6_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p6_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_fy", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Charts
        {
            "name": "p6_ch_pipe_product",
            "row": 5,
            "column": 0,
            "colspan": 6,
            "rowspan": 8,
        },
        {
            "name": "p6_ch_won_product",
            "row": 5,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        },
        {
            "name": "p6_ch_wr_product",
            "row": 13,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        {
            "name": "p6_ch_product_type",
            "row": 23,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        {
            "name": "p6_ch_product_trend",
            "row": 31,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
    ]

    p7 = nav_row("p7", 8) + [
        {"name": "p7_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p7_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_fy", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Sankey
        {"name": "p7_sec_sankey", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_sankey", "row": 6, "column": 0, "colspan": 12, "rowspan": 10},
        # Treemap
        {"name": "p7_sec_treemap", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_treemap", "row": 17, "column": 0, "colspan": 12, "rowspan": 10},
        # Heatmap: Win Rate
        {"name": "p7_sec_heatmap", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p7_ch_heatmap_wr",
            "row": 28,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # Bubble
        {"name": "p7_sec_bubble", "row": 38, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_bubble", "row": 39, "column": 0, "colspan": 12, "rowspan": 10},
        # Area
        {"name": "p7_sec_area", "row": 49, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_area", "row": 50, "column": 0, "colspan": 12, "rowspan": 8},
        # VIZ UPGRADE: Stage-to-Stage Deal Flow Sankey
        {"name": "p7_sec_stage_flow", "row": 58, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_stage_flow", "row": 59, "column": 0, "colspan": 12, "rowspan": 10},
        # VIZ UPGRADE: Win/Loss Reason Analysis
        {"name": "p7_sec_reasons", "row": 69, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_reason_heatmap", "row": 70, "column": 0, "colspan": 6, "rowspan": 10},
        {"name": "p7_ch_reason_waterfall", "row": 70, "column": 6, "colspan": 6, "rowspan": 10},
        # VIZ UPGRADE: Time-in-Stage Distribution (trellis)
        {"name": "p7_sec_stage_time", "row": 80, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_stage_time", "row": 81, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    p8 = nav_row("p8", 8) + [
        {"name": "p8_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p8_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p8_f_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p8_f_fy", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p8_f_region", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Bullet charts (3 side-by-side)
        {"name": "p8_sec_bullet", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p8_bullet_coverage",
            "row": 6,
            "column": 0,
            "colspan": 4,
            "rowspan": 5,
        },
        {
            "name": "p8_bullet_winrate",
            "row": 6,
            "column": 4,
            "colspan": 4,
            "rowspan": 5,
        },
        {
            "name": "p8_bullet_avg_deal",
            "row": 6,
            "column": 8,
            "colspan": 4,
            "rowspan": 5,
        },
        # Percentile summary table
        {"name": "p8_sec_stats", "row": 11, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p8_stat_pct", "row": 12, "column": 0, "colspan": 12, "rowspan": 5},
        # Moving average chart
        {"name": "p8_sec_ma", "row": 17, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p8_stat_ma", "row": 18, "column": 0, "colspan": 12, "rowspan": 8},
        # Regression
        {
            "name": "p8_stat_regression",
            "row": 26,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Pareto / revenue concentration
        {"name": "p8_sec_pareto", "row": 35, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p8_stat_pareto", "row": 36, "column": 0, "colspan": 12, "rowspan": 8},
        # Phase 9: Velocity band
        {
            "name": "p8_sec_velocity",
            "row": 44,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p8_velocity_bar", "row": 45, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p8_velocity_arr", "row": 45, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p9 = nav_row("p9", 9) + [
        {"name": "p9_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p9_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p9_f_region", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # KPI tiles
        {"name": "p9_kpi_pipeline", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p9_kpi_won", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p9_kpi_risk", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        # Product × Region heatmap
        {"name": "p9_sec_prod_region", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p9_ch_prod_region", "row": 10, "column": 0, "colspan": 12, "rowspan": 10},
        # Win rate matrix
        {"name": "p9_sec_wr_matrix", "row": 20, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p9_ch_wr_matrix", "row": 21, "column": 0, "colspan": 12, "rowspan": 10},
        # Coverage bullet + risk distribution
        {"name": "p9_sec_coverage", "row": 31, "column": 0, "colspan": 6, "rowspan": 1},
        {"name": "p9_ch_coverage", "row": 32, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p9_sec_risk_dist", "row": 31, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p9_ch_risk_dist", "row": 32, "column": 6, "colspan": 6, "rowspan": 8},
        # At-risk deals table
        {"name": "p9_sec_atrisk", "row": 40, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p9_tbl_atrisk", "row": 41, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("overview", "Executive Overview", p1),
            pg("trends", "Time Trends", p2),
            pg("crossdim", "Cross-Dimensional", p3),
            pg("geo", "Geographic", p4),
            pg("salesops", "Sales Operations", p5),
            pg("productmix", "Product Mix", p6),
            pg("advanalytics", "Advanced Analytics", p7),
            pg("statsanalysis", "Statistical Analysis", p8),
            pg("growth", "Growth & Risk", p9),
        ],
    }


def create_dataflow_definition():
    """Return a CRM Analytics dataflow definition for Opp_Mgmt_KPIs.

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
                    {"name": "LeadSource"},
                    {"name": "CreatedDate"},
                    {"name": "CurrencyIsoCode"},
                    {"name": "FiscalYear"},
                    {"name": "FiscalQuarter"},
                    {"name": "APTS_Forecast_ARR__c"},
                    {"name": "Amount"},
                    {"name": "Probability"},
                    {"name": "AgeInDays"},
                    {"name": "Stage_20_Approval__c"},
                    {"name": "APTS_RH_Product_Family__c"},
                ],
            },
        },
        "Extract_Users": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "User",
                "fields": [
                    {"name": "Id"},
                    {"name": "Name"},
                ],
            },
        },
        "Extract_Accounts": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Account",
                "fields": [
                    {"name": "Id"},
                    {"name": "Name"},
                ],
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
                "name": "Opp_Mgmt_KPIs",
                "alias": "Opp_Mgmt_KPIs",
                "label": "Opportunity Management KPIs",
            },
        },
    }


def main():
    instance_url, token = get_auth()

    if "--create-dataflow" in sys.argv:
        print("\n=== Creating/updating dataflow ===")
        df_def = create_dataflow_definition()
        df_id = create_dataflow(instance_url, token, "DF_Opp_Mgmt", df_def)
        if df_id and "--run-dataflow" in sys.argv:
            run_dataflow(instance_url, token, df_id)
        return

    # Rebuild main dataset with convertCurrency(ARR) already in EUR
    ds_result = create_main_dataset(instance_url, token)
    if not ds_result:
        print("ERROR: Main dataset rebuild failed — aborting")
        return

    # Look up dataset ID dynamically
    ds_id = get_dataset_id(instance_url, token, DS)
    ds_meta = [{"id": ds_id, "name": DS}] if ds_id else [{"name": DS}]

    # Set record navigation links via XMD
    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "Name", "id_field": "Id"},
            {"field": "Id", "id_field": "Id", "label": "Opportunity ID"},
            {"field": "AccountName", "id_field": "AccountId"},
            {"field": "AccountId", "id_field": "AccountId", "label": "Account ID"},
        ],
    )

    # Create/refresh the geographic dataset from live SOQL data
    geo_result = create_geo_dataset(instance_url, token)
    if not geo_result:
        print("WARNING: Geo dataset failed — page 4 may show empty charts")
    else:
        _set_geo_xmd(instance_url, token)

    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    state = build_dashboard_state(build_steps(ds_meta), build_widgets(), build_layout())
    deploy_dashboard(instance_url, token, dashboard_id, state)


if __name__ == "__main__":
    main()
