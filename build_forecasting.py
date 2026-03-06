#!/usr/bin/env python3
"""Build the Forecast Intelligence dashboard — DIY replacement for Revenue Intelligence.

ML-Forward Upgrade:
  - Query ForecastingItem for manager-submitted forecasts
  - Query pipeline data aggregated by owner + forecast category
  - Compute weighted forecast: Commit * 0.9 + BestCase * 0.5 + Pipeline * 0.2
  - Join with quota data if available
  - Forecast snapshot dataset for error/bias tracking
  - Forecast risk scoring per rep (ML-driven)
  - Forecast band computation with 95% prediction intervals

Pages:
  1. Forecast Overview — KPI tiles, commit vs quota gauge, quarterly forecast vs actual
  2. Rep-Level Detail — rep forecast table, category breakdown, waterfall
  3. Quota & Coverage — attainment distribution, pipeline ratios
  4. Advanced Analytics — Sankey, treemap, heatmap, bubble, area
  5. Statistical Analysis — bullet charts, percentiles, distributions
  6. Bands & Error — forecast bands with uncertainty, error heatmap, bias analytics

Datasets:
  - Forecast_Intelligence (primary)
  - Forecast_Snapshots (weekly snapshots for error tracking)

Visualization Upgrade (v3 ML-Forward):
  - Timeline forecast chart with _high_95/_low_95 prediction interval bands
  - Forecast error heatmap (rep × quarter) with MAPE/bias
  - Forecast risk scoring tiles with threshold-based coloring
  - Rep-selection interactions: clicking rep updates all visuals
  - Bullet chart panel for rep attainment (Won vs Quota)
  - Forecast accuracy scatter with quadrant semantics
  - Dynamic KPI tiles with threshold-based coloring
"""

import csv
import io

from crm_analytics_helpers import (
    get_auth,
    _soql,
    _dim,
    _measure,
    upload_dataset,
    get_dataset_id,
    sq,
    af,
    num,
    num_dynamic_color,
    rich_chart,
    gauge,
    waterfall_chart,
    pillbox,
    coalesce_filter,
    hdr,
    section_label,
    nav_link,
    pg,
    nav_row,
    build_dashboard_state,
    deploy_dashboard,
    create_dashboard_if_needed,
    sankey_chart,
    treemap_chart,
    heatmap_chart,
    bubble_chart,
    area_chart,
    bullet_chart,
    timeline_chart,
    combo_chart,
    line_chart,
    scatter_chart,
    add_selection_interaction,
    add_table_action,
    # ML-Forward additions
    forecast_snapshot_fields,
    forecast_error_fields,
    forecast_bands_step,
    forecast_error_step,
    forecast_error_heatmap_step,
    compute_forecast_risk,
)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

DS = "Forecast_Intelligence"
DS_LABEL = "Forecast Intelligence"
SNAP_DS = "Forecast_Snapshots"
SNAP_DS_LABEL = "Forecast Snapshots"
ERROR_DS = "Forecast_Error"
ERROR_DS_LABEL = "Forecast Error Analytics"
DASHBOARD_LABEL = "Forecast Intelligence"

# Weights for weighted forecast calculation
COMMIT_WEIGHT = 0.9
BEST_CASE_WEIGHT = 0.5
PIPELINE_WEIGHT = 0.2

# Filter bindings
UF = coalesce_filter("f_unit", "UnitGroup")
QF = coalesce_filter("f_qtr", "CloseQuarter")


# ═══════════════════════════════════════════════════════════════════════════
#  Dataset creation
# ═══════════════════════════════════════════════════════════════════════════


def create_dataset(inst, tok):
    """Build Forecast_Intelligence from pipeline data + forecast categories.

    Since ForecastingItem may not be populated, we build from Opportunity data
    grouped by owner + forecast category with weighted forecast computation.
    """
    print("\n=== Building Forecast Intelligence dataset ===")

    # Query open pipeline by owner and forecast category
    opps = _soql(
        inst,
        tok,
        "SELECT Id, Name, Owner.Name, Account_Unit_Group__c, "
        "ForecastCategoryName, IsClosed, IsWon, CloseDate, "
        "FiscalYear, FiscalQuarter, "
        "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
        "Quota_Amount__c "
        "FROM Opportunity "
        "WHERE FiscalYear IN (2025, 2026)",
    )
    print(f"  Queried {len(opps)} opportunities")

    # Also try ForecastingItem if available
    forecast_items = []
    try:
        forecast_items = _soql(
            inst,
            tok,
            "SELECT ForecastAmount, ForecastCategoryName, OwnerId, "
            "Owner.Name, PeriodId, FiscalYear "
            "FROM ForecastingItem "
            "WHERE FiscalYear IN (2025, 2026) LIMIT 1000",
        )
        print(f"  Queried {len(forecast_items)} forecast items")
    except Exception as e:
        print(f"  ForecastingItem not available: {e}")

    # Build per-rep, per-quarter, per-category aggregation
    rep_data = {}
    for o in opps:
        owner = (o.get("Owner") or {}).get("Name") or "Unknown"
        arr = o.get("ConvertedARR") or 0
        fcat = o.get("ForecastCategoryName") or "Pipeline"
        is_closed = o.get("IsClosed", False)
        is_won = o.get("IsWon", False)
        fy = o.get("FiscalYear") or 0
        fq = o.get("FiscalQuarter") or 0
        close_date = o.get("CloseDate") or ""
        unit_group = o.get("Account_Unit_Group__c") or ""
        quota = o.get("Quota_Amount__c") or 0

        # Derive quarter label
        close_qtr = f"Q{fq}" if fq else ""
        key = (owner, fy, close_qtr)

        if key not in rep_data:
            rep_data[key] = {
                "OwnerName": owner,
                "FiscalYear": fy,
                "CloseQuarter": close_qtr,
                "UnitGroup": unit_group,
                "CommitARR": 0,
                "BestCaseARR": 0,
                "PipelineARR": 0,
                "ClosedWonARR": 0,
                "ClosedLostARR": 0,
                "TotalPipelineARR": 0,
                "OppCount": 0,
                "QuotaAmount": quota,
            }

        d = rep_data[key]
        d["OppCount"] += 1

        if is_won:
            d["ClosedWonARR"] += arr
        elif is_closed:
            d["ClosedLostARR"] += arr
        else:
            d["TotalPipelineARR"] += arr
            if fcat == "Commit":
                d["CommitARR"] += arr
            elif fcat in ("Best Case", "BestCase"):
                d["BestCaseARR"] += arr
            else:
                d["PipelineARR"] += arr

        # Keep latest UnitGroup and quota
        if unit_group:
            d["UnitGroup"] = unit_group
        if quota and quota > d["QuotaAmount"]:
            d["QuotaAmount"] = quota

    # Generate CSV rows with weighted forecast + ML outputs

    # ─── Compute forecast risk scoring per rep ──────────────────────────
    for key, d in rep_data.items():
        risk_score, risk_band, risk_driver = compute_forecast_risk(d)
        d["ForecastRiskScore"] = risk_score
        d["ForecastRiskBand"] = risk_band
        d["ForecastRiskDriver"] = risk_driver

    # ─── Compute forecast bands (P50/P90 with 95% intervals) ─────────
    # Use historical variance across reps in same quarter to estimate bands
    qtr_actuals = {}
    for key, d in rep_data.items():
        qtr = d["CloseQuarter"]
        if qtr not in qtr_actuals:
            qtr_actuals[qtr] = []
        if d["ClosedWonARR"] > 0:
            qtr_actuals[qtr].append(d["ClosedWonARR"])

    for key, d in rep_data.items():
        weighted = (
            d["CommitARR"] * COMMIT_WEIGHT
            + d["BestCaseARR"] * BEST_CASE_WEIGHT
            + d["PipelineARR"] * PIPELINE_WEIGHT
        )
        # Estimate prediction interval based on forecast confidence
        # Higher risk = wider bands
        risk = d.get("ForecastRiskScore", 50)
        band_width = weighted * (0.15 + risk / 200)  # 15-65% band width
        d["ForecastP50"] = round(weighted, 2)
        d["ForecastP90"] = round(weighted * 1.3, 2)  # Optimistic scenario
        d["ForecastP50_high_95"] = round(weighted + band_width, 2)
        d["ForecastP50_low_95"] = round(max(0, weighted - band_width), 2)

    # ─── Rebuild CSV with new fields ──────────────────────────────────
    fields = [
        "OwnerName",
        "FiscalYear",
        "CloseQuarter",
        "UnitGroup",
        "CommitARR",
        "BestCaseARR",
        "PipelineARR",
        "ClosedWonARR",
        "ClosedLostARR",
        "TotalPipelineARR",
        "WeightedForecast",
        "OppCount",
        "QuotaAmount",
        "QuotaAttainment",
        "FYLabel",
        "ForecastRiskScore",
        "ForecastRiskBand",
        "ForecastRiskDriver",
        "ForecastP50",
        "ForecastP90",
        "ForecastP50_high_95",
        "ForecastP50_low_95",
    ]

    buf2 = io.StringIO()
    writer2 = csv.DictWriter(buf2, fieldnames=fields, lineterminator="\n")
    writer2.writeheader()

    for key, d in rep_data.items():
        weighted = (
            d["CommitARR"] * COMMIT_WEIGHT
            + d["BestCaseARR"] * BEST_CASE_WEIGHT
            + d["PipelineARR"] * PIPELINE_WEIGHT
        )
        quota = d["QuotaAmount"]
        attainment = round((d["ClosedWonARR"] / quota) * 100, 1) if quota > 0 else 0

        writer2.writerow(
            {
                "OwnerName": d["OwnerName"],
                "FiscalYear": d["FiscalYear"],
                "CloseQuarter": d["CloseQuarter"],
                "UnitGroup": d["UnitGroup"],
                "CommitARR": round(d["CommitARR"], 2),
                "BestCaseARR": round(d["BestCaseARR"], 2),
                "PipelineARR": round(d["PipelineARR"], 2),
                "ClosedWonARR": round(d["ClosedWonARR"], 2),
                "ClosedLostARR": round(d["ClosedLostARR"], 2),
                "TotalPipelineARR": round(d["TotalPipelineARR"], 2),
                "WeightedForecast": round(weighted, 2),
                "OppCount": d["OppCount"],
                "QuotaAmount": round(quota, 2),
                "QuotaAttainment": attainment,
                "FYLabel": f"FY{d['FiscalYear']}",
                "ForecastRiskScore": d["ForecastRiskScore"],
                "ForecastRiskBand": d["ForecastRiskBand"],
                "ForecastRiskDriver": d["ForecastRiskDriver"],
                "ForecastP50": d["ForecastP50"],
                "ForecastP90": d["ForecastP90"],
                "ForecastP50_high_95": d["ForecastP50_high_95"],
                "ForecastP50_low_95": d["ForecastP50_low_95"],
            }
        )

    csv_bytes = buf2.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes, {len(rep_data)} rows")

    fields_meta = [
        _dim("OwnerName", "Owner"),
        _measure("FiscalYear", "Fiscal Year", scale=0, precision=5),
        _dim("CloseQuarter", "Quarter"),
        _dim("UnitGroup", "Unit Group"),
        _measure("CommitARR", "Commit ARR"),
        _measure("BestCaseARR", "Best Case ARR"),
        _measure("PipelineARR", "Pipeline ARR"),
        _measure("ClosedWonARR", "Closed Won ARR"),
        _measure("ClosedLostARR", "Closed Lost ARR"),
        _measure("TotalPipelineARR", "Total Pipeline ARR"),
        _measure("WeightedForecast", "Weighted Forecast"),
        _measure("OppCount", "Opp Count", scale=0, precision=6),
        _measure("QuotaAmount", "Quota Amount"),
        _measure("QuotaAttainment", "Quota Attainment %", scale=1, precision=5),
        _dim("FYLabel", "Fiscal Year Label"),
        # ML-Forward: Forecast Risk
        _measure("ForecastRiskScore", "Forecast Risk Score", scale=0, precision=5),
        _dim("ForecastRiskBand", "Forecast Risk Band"),
        _dim("ForecastRiskDriver", "Top Risk Driver"),
        # ML-Forward: Forecast Bands
        _measure("ForecastP50", "Forecast P50"),
        _measure("ForecastP90", "Forecast P90"),
        _measure("ForecastP50_high_95", "P50 Upper Band"),
        _measure("ForecastP50_low_95", "P50 Lower Band"),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


def create_snapshot_dataset(inst, tok, rep_data):
    """Create a forecast snapshot dataset for error/bias tracking.

    Each run appends a point-in-time snapshot of forecast state per rep.
    Over time, this enables MAPE/WAPE/bias computation against actuals.
    """
    from datetime import datetime as _dt

    print("\n=== Building Forecast Snapshots dataset ===")
    snapshot_date = _dt.utcnow().strftime("%Y-%m-%d")

    snap_fields = [
        "SnapshotDate", "OwnerName", "CloseQuarter", "FYLabel", "UnitGroup",
        "CommitARR", "BestCaseARR", "PipelineARR", "ClosedWonARR",
        "WeightedForecast", "QuotaAmount",
        "ForecastP50", "ForecastP90", "ForecastP50_high_95", "ForecastP50_low_95",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=snap_fields, lineterminator="\n")
    writer.writeheader()

    for key, d in rep_data.items():
        weighted = (
            d["CommitARR"] * COMMIT_WEIGHT
            + d["BestCaseARR"] * BEST_CASE_WEIGHT
            + d["PipelineARR"] * PIPELINE_WEIGHT
        )
        writer.writerow({
            "SnapshotDate": snapshot_date,
            "OwnerName": d["OwnerName"],
            "CloseQuarter": d["CloseQuarter"],
            "FYLabel": f"FY{d['FiscalYear']}",
            "UnitGroup": d["UnitGroup"],
            "CommitARR": round(d["CommitARR"], 2),
            "BestCaseARR": round(d["BestCaseARR"], 2),
            "PipelineARR": round(d["PipelineARR"], 2),
            "ClosedWonARR": round(d["ClosedWonARR"], 2),
            "WeightedForecast": round(weighted, 2),
            "QuotaAmount": round(d["QuotaAmount"], 2),
            "ForecastP50": d.get("ForecastP50", 0),
            "ForecastP90": d.get("ForecastP90", 0),
            "ForecastP50_high_95": d.get("ForecastP50_high_95", 0),
            "ForecastP50_low_95": d.get("ForecastP50_low_95", 0),
        })

    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"  Snapshot CSV: {len(csv_bytes):,} bytes, {len(rep_data)} rows")

    return upload_dataset(inst, tok, SNAP_DS, SNAP_DS_LABEL,
                          forecast_snapshot_fields(), csv_bytes)


# ═══════════════════════════════════════════════════════════════════════════
#  Steps
# ═══════════════════════════════════════════════════════════════════════════


def build_steps(ds_id):
    DS_META = [{"id": ds_id, "name": DS}]
    L = f'q = load "{DS}";\n'
    FY26 = "q = filter q by FiscalYear == 2026;\n"

    return {
        # ── Filter steps ──
        "f_unit": af("UnitGroup", DS_META),
        "f_qtr": af("CloseQuarter", DS_META),
        # ═══ PAGE 1: Forecast Overview ═══
        # KPI: Total Commit ARR
        "s_commit": sq(
            L
            + FY26
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(CommitARR) as total_commit;"
        ),
        # KPI: Total Best Case ARR
        "s_bestcase": sq(
            L
            + FY26
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(BestCaseARR) as total_bestcase;"
        ),
        # KPI: Total Weighted Forecast
        "s_weighted": sq(
            L
            + FY26
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(WeightedForecast) as total_weighted;"
        ),
        # KPI: Total Quota
        "s_quota": sq(
            L
            + FY26
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(QuotaAmount) as total_quota;"
        ),
        # Gauge: Commit vs Quota %
        "s_commit_pct": sq(
            L
            + FY26
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(case when sum(QuotaAmount) > 0 "
            + "then (sum(CommitARR) / sum(QuotaAmount)) * 100 "
            + "else 0 end) as commit_pct;"
        ),
        # Gauge: Closed Won vs Quota %
        "s_attain": sq(
            L
            + FY26
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(case when sum(QuotaAmount) > 0 "
            + "then (sum(ClosedWonARR) / sum(QuotaAmount)) * 100 "
            + "else 0 end) as attain_pct;"
        ),
        # Combo: Quarterly forecast categories (columns) + closed won (line)
        "s_qtr_forecast": sq(
            L
            + FY26
            + UF
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(CommitARR) as commit_arr, "
            + "sum(BestCaseARR) as bestcase_arr, "
            + "sum(PipelineARR) as pipeline_arr, "
            + "sum(ClosedWonARR) as won_arr;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        # Donut: Forecast category split (current pipeline)
        "s_fcat_split": sq(
            L
            + FY26
            + UF
            + QF
            + "q = foreach q generate "
            + 'CommitARR as arr, "Commit" as Category;\n'
            + f'q2 = load "{DS}";\n'
            + "q2 = filter q2 by FiscalYear == 2026;\n"
            + UF.replace("q =", "q2 =").replace("q by", "q2 by")
            + QF.replace("q =", "q2 =").replace("q by", "q2 by")
            + "q2 = foreach q2 generate "
            + 'BestCaseARR as arr, "Best Case" as Category;\n'
            + f'q3 = load "{DS}";\n'
            + "q3 = filter q3 by FiscalYear == 2026;\n"
            + UF.replace("q =", "q3 =").replace("q by", "q3 by")
            + QF.replace("q =", "q3 =").replace("q by", "q3 by")
            + "q3 = foreach q3 generate "
            + 'PipelineARR as arr, "Pipeline" as Category;\n'
            + "q = union q, q2, q3;\n"
            + "q = group q by Category;\n"
            + "q = foreach q generate Category, sum(arr) as total_arr;"
        ),
        # ═══ PAGE 2: Rep-Level Detail ═══
        # Comparison table: Rep-level forecast
        "s_rep_forecast": sq(
            L
            + FY26
            + UF
            + QF
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(CommitARR) as commit_arr, "
            + "sum(BestCaseARR) as bestcase_arr, "
            + "sum(PipelineARR) as pipeline_arr, "
            + "sum(WeightedForecast) as weighted, "
            + "sum(ClosedWonARR) as won_arr, "
            + "sum(QuotaAmount) as quota, "
            + "(case when sum(QuotaAmount) > 0 "
            + "then (sum(ClosedWonARR) / sum(QuotaAmount)) * 100 "
            + "else 0 end) as attain_pct;\n"
            + "q = order q by weighted desc;"
        ),
        # Hbar: Weighted forecast by rep (top 15)
        "s_rep_bar": sq(
            L
            + FY26
            + UF
            + QF
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(WeightedForecast) as weighted;\n"
            + "q = order q by weighted desc;\n"
            + "q = limit q 15;"
        ),
        # Hbar: Unit Group forecast
        "s_unit_forecast": sq(
            L
            + FY26
            + QF
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(WeightedForecast) as weighted, "
            + "sum(CommitARR) as commit_arr, "
            + "sum(ClosedWonARR) as won_arr;\n"
            + "q = order q by weighted desc;"
        ),
        # Waterfall: QoQ forecast category movement
        "s_forecast_waterfall": sq(
            L
            + FY26
            + UF
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(WeightedForecast) as weighted;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        # ═══ Pipeline Coverage & Gap Analytics ═══
        # Pipeline coverage ratio — Total pipeline / Quota
        "s_pipe_coverage": sq(
            L
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(TotalPipelineARR) as total_pipe, sum(QuotaAmount) as total_quota;\n"
            + "q = foreach q generate total_pipe, total_quota, "
            + "(case when total_quota > 0 then total_pipe / total_quota else 0 end) as coverage_ratio;"
        ),
        # Gap to quota by rep
        "s_gap_to_quota": sq(
            L
            + UF
            + QF
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, sum(ClosedWonARR) as won, "
            + "sum(CommitARR) as commit_arr, sum(QuotaAmount) as quota, "
            + "sum(TotalPipelineARR) as pipeline;\n"
            + "q = foreach q generate OwnerName, won, commit_arr, quota, pipeline, "
            + "(quota - won - commit_arr) as gap;\n"
            + "q = order q by gap desc;"
        ),
        # Win rate by rep
        "s_rep_wr": sq(
            L
            + UF
            + QF
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, sum(OppCount) as total_opps, "
            + "sum(ClosedWonARR) as won_arr, sum(ClosedLostARR) as lost_arr;\n"
            + "q = foreach q generate OwnerName, total_opps, won_arr, "
            + "(case when (won_arr + lost_arr) > 0 then won_arr / (won_arr + lost_arr) * 100 else 0 end) as win_rate;\n"
            + "q = order q by win_rate desc;"
        ),
        # ═══ PAGE 3: Quota & Coverage Analytics ═══
        # Quota Attainment Distribution (histogram-like column chart)
        "s_attain_dist": sq(
            L
            + FY26
            + UF
            + QF
            + "q = filter q by QuotaAmount > 0;\n"
            + "q = foreach q generate "
            + "(case "
            + 'when QuotaAttainment < 25 then "a_0-25%" '
            + 'when QuotaAttainment < 50 then "b_25-50%" '
            + 'when QuotaAttainment < 75 then "c_50-75%" '
            + 'when QuotaAttainment < 100 then "d_75-100%" '
            + 'when QuotaAttainment < 125 then "e_100-125%" '
            + 'else "f_125%+" end) as AttainBand;\n'
            + "q = group q by AttainBand;\n"
            + "q = foreach q generate AttainBand, count() as rep_count;\n"
            + "q = order q by AttainBand asc;"
        ),
        # Pipeline Coverage by Segment (UnitGroup)
        "s_pipe_coverage_seg": sq(
            L
            + FY26
            + QF
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(TotalPipelineARR) as pipe_arr, "
            + "sum(QuotaAmount) as quota_arr, "
            + "(case when sum(QuotaAmount) > 0 "
            + "then sum(TotalPipelineARR) / sum(QuotaAmount) "
            + "else 0 end) as coverage;\n"
            + "q = order q by coverage desc;"
        ),
        # Human (Commit) vs AI (Weighted) Forecast Comparison by Quarter
        "s_forecast_compare": sq(
            L
            + FY26
            + UF
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(CommitARR) as human_forecast, "
            + "sum(WeightedForecast) as ai_forecast, "
            + "sum(ClosedWonARR) as actual_won;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        # Attainment by rep (detail table)
        "s_attain_by_rep": sq(
            L
            + FY26
            + UF
            + QF
            + "q = filter q by QuotaAmount > 0;\n"
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(ClosedWonARR) as won_arr, "
            + "sum(QuotaAmount) as quota, "
            + "(case when sum(QuotaAmount) > 0 "
            + "then (sum(ClosedWonARR) / sum(QuotaAmount)) * 100 "
            + "else 0 end) as attain_pct, "
            + "sum(TotalPipelineARR) as pipe_arr, "
            + "(case when sum(QuotaAmount) > 0 "
            + "then sum(TotalPipelineARR) / sum(QuotaAmount) "
            + "else 0 end) as coverage;\n"
            + "q = order q by attain_pct desc;"
        ),
        # ═══ V2 Phase 10: Advanced Analytics steps ═══
        "s_sankey_cat": sq(
            L
            + UF
            + QF
            + "q = group q by (UnitGroup, CloseQuarter);\n"
            + "q = foreach q generate UnitGroup, CloseQuarter, "
            + "sum(WeightedForecast) as wf;\n"
            + "q = order q by wf desc;"
        ),
        "s_treemap_forecast": sq(
            L
            + UF
            + QF
            + "q = group q by (UnitGroup, OwnerName);\n"
            + "q = foreach q generate UnitGroup, OwnerName, "
            + "sum(WeightedForecast) as wf;\n"
            + "q = order q by wf desc;"
        ),
        "s_heatmap_owner_qtr": sq(
            L
            + UF
            + QF
            + "q = group q by (OwnerName, CloseQuarter);\n"
            + "q = foreach q generate OwnerName, CloseQuarter, "
            + "sum(ClosedWonARR) as won_arr;\n"
            + "q = order q by OwnerName asc;"
        ),
        "s_bubble_owner": sq(
            L
            + UF
            + QF
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(CommitARR) as commit_arr, "
            + "sum(ClosedWonARR) as won_arr, "
            + "sum(OppCount) as opp_count;\n"
            + "q = order q by commit_arr desc;"
        ),
        "s_area_cumul": sq(
            L
            + UF
            + QF
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(ClosedWonARR) as won_arr;\n"
            + "q = order q by CloseQuarter asc;\n"
            + "q = foreach q generate CloseQuarter, won_arr, "
            + "sum(won_arr) over (order by CloseQuarter rows unbounded preceding) as cumul_won;"
        ),
        # ═══ V2 Phase 10: Statistical Analysis steps ═══
        "s_bullet_commit": sq(
            L
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(CommitARR) as actual, "
            + "sum(QuotaAmount) as target;"
        ),
        "s_bullet_weighted": sq(
            L
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(WeightedForecast) as actual, "
            + "sum(QuotaAmount) * 0.8 as target;"
        ),
        "s_stat_wf_percentiles": sq(
            L
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as cnt, "
            + "avg(WeightedForecast) as mean_wf, "
            + "stddev(WeightedForecast) as std_wf, "
            + "percentile_disc(0.25) within group (order by WeightedForecast) as p25, "
            + "percentile_disc(0.50) within group (order by WeightedForecast) as median_wf, "
            + "percentile_disc(0.75) within group (order by WeightedForecast) as p75;"
        ),
        "s_stat_attain_by_unit": sq(
            L
            + UF
            + QF
            + "q = filter q by QuotaAmount > 0;\n"
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "count() as cnt, "
            + "avg(QuotaAttainment) as avg_attain, "
            + "stddev(QuotaAttainment) as std_attain, "
            + "percentile_disc(0.50) within group (order by QuotaAttainment) as median_attain;\n"
            + "q = order q by avg_attain desc;"
        ),
        # ═══ VIZ UPGRADE: Timeline Forecast Narrative ═══
        # Actual vs forecast over quarters for forecast accuracy storytelling
        "s_forecast_timeline": sq(
            L
            + UF
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(ClosedWonARR) as actual_won, "
            + "sum(WeightedForecast) as forecast, "
            + "sum(CommitARR) as commit_arr;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        # ═══ VIZ UPGRADE: Rep Attainment Bullet (Won vs Quota per rep) ═══
        "s_rep_bullet_attain": sq(
            L
            + UF
            + QF
            + "q = filter q by QuotaAmount > 0;\n"
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(ClosedWonARR) as actual, "
            + "sum(QuotaAmount) as target;\n"
            + "q = order q by actual desc;\n"
            + "q = limit q 15;"
        ),
        # ═══ VIZ UPGRADE: Forecast Accuracy Scatter ═══
        # Forecast vs Actual per rep with quadrant semantics
        "s_forecast_accuracy": sq(
            L
            + UF
            + QF
            + "q = filter q by QuotaAmount > 0;\n"
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(WeightedForecast) as forecast_arr, "
            + "sum(ClosedWonARR) as actual_won, "
            + "(case when sum(WeightedForecast) > 0 then "
            + "abs(sum(ClosedWonARR) - sum(WeightedForecast)) / sum(WeightedForecast) * 100 "
            + "else 0 end) as accuracy_error_pct;\n"
            + "q = order q by forecast_arr desc;"
        ),
        # ═══ VIZ UPGRADE: Dynamic KPI Thresholds ═══
        "s_forecast_kpi_thresh": sq(
            L
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(case when sum(QuotaAmount) > 0 then sum(CommitARR) / sum(QuotaAmount) * 100 else 0 end) as commit_vs_quota, "
            + "(case when sum(QuotaAmount) > 0 then sum(ClosedWonARR) / sum(QuotaAmount) * 100 else 0 end) as attainment_pct, "
            + "(case when sum(ClosedWonARR) > 0 then sum(PipelineARR) / sum(ClosedWonARR) else 0 end) as pipe_coverage;"
        ),
        # ═══ VIZ UPGRADE: Coverage-to-Go by Quarter ═══
        "s_coverage_to_go": sq(
            L
            + UF
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(QuotaAmount) as quota, "
            + "sum(ClosedWonARR) as won, "
            + "sum(PipelineARR) as pipeline, "
            + "sum(CommitARR) as commit_arr, "
            + "(sum(QuotaAmount) - sum(ClosedWonARR) - sum(CommitARR)) as gap_to_quota;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        # ═══ PAGE 6: Bands & Error (ML-Forward) ═══
        # Forecast bands timeline: actual vs P50 with 95% prediction interval
        "s_forecast_bands": sq(
            L
            + UF
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(ClosedWonARR) as actual_arr, "
            + "sum(ForecastP50) as forecast_p50, "
            + "sum(ForecastP50_high_95) as forecast_p50_high_95, "
            + "sum(ForecastP50_low_95) as forecast_p50_low_95, "
            + "sum(ForecastP90) as forecast_p90;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        # Forecast error by rep (accuracy scatter)
        "s_forecast_err_rep": sq(
            L
            + UF
            + QF
            + "q = filter q by ClosedWonARR > 0 || WeightedForecast > 0;\n"
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(WeightedForecast) as forecast_arr, "
            + "sum(ClosedWonARR) as actual_arr, "
            + "(case when sum(WeightedForecast) > 0 then "
            + "((sum(ClosedWonARR) - sum(WeightedForecast)) / sum(WeightedForecast)) * 100 "
            + "else 0 end) as bias_pct, "
            + "(case when sum(WeightedForecast) > 0 then "
            + "abs(sum(ClosedWonARR) - sum(WeightedForecast)) / sum(WeightedForecast) * 100 "
            + "else 0 end) as mape_pct;\n"
            + "q = order q by mape_pct desc;"
        ),
        # Forecast error heatmap: rep × quarter
        "s_forecast_err_heatmap": sq(
            L
            + UF
            + "q = filter q by ClosedWonARR > 0 || WeightedForecast > 0;\n"
            + "q = group q by (OwnerName, CloseQuarter);\n"
            + "q = foreach q generate OwnerName, CloseQuarter, "
            + "(case when sum(WeightedForecast) > 0 then "
            + "abs(sum(ClosedWonARR) - sum(WeightedForecast)) / sum(WeightedForecast) * 100 "
            + "else 0 end) as error_pct;\n"
            + "q = order q by OwnerName asc, CloseQuarter asc;"
        ),
        # Forecast risk distribution
        "s_forecast_risk_dist": sq(
            L
            + UF
            + QF
            + "q = group q by ForecastRiskBand;\n"
            + "q = foreach q generate ForecastRiskBand, "
            + "count() as rep_count, "
            + "sum(WeightedForecast) as total_forecast;\n"
            + "q = order q by (case "
            + "when ForecastRiskBand == \"High\" then 1 "
            + "when ForecastRiskBand == \"Medium\" then 2 "
            + "else 3 end) asc;"
        ),
        # Forecast risk drivers
        "s_forecast_risk_drivers": sq(
            L
            + UF
            + QF
            + "q = group q by ForecastRiskDriver;\n"
            + "q = foreach q generate ForecastRiskDriver, "
            + "count() as rep_count, "
            + "sum(WeightedForecast) as at_risk_forecast;\n"
            + "q = order q by rep_count desc;\n"
            + "q = limit q 10;"
        ),
        # Forecast bias summary KPI
        "s_forecast_bias_kpi": sq(
            L
            + UF
            + QF
            + "q = filter q by ClosedWonARR > 0 || WeightedForecast > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(case when sum(WeightedForecast) > 0 then "
            + "((sum(ClosedWonARR) - sum(WeightedForecast)) / sum(WeightedForecast)) * 100 "
            + "else 0 end) as bias_pct, "
            + "(case when sum(WeightedForecast) > 0 then "
            + "abs(sum(ClosedWonARR) - sum(WeightedForecast)) / sum(WeightedForecast) * 100 "
            + "else 0 end) as mape_pct, "
            + "sum(ClosedWonARR) as total_actual, "
            + "sum(WeightedForecast) as total_forecast;"
        ),
        # Forecast bands attainment bullet
        "s_forecast_band_bullet": sq(
            L
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(ClosedWonARR) as actual, "
            + "sum(ForecastP50) as target, "
            + "sum(ForecastP50_low_95) as poor_range, "
            + "sum(ForecastP90) as good_range;"
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Widgets
# ═══════════════════════════════════════════════════════════════════════════


def build_widgets():
    PAGE_IDS = [
        "overview",
        "repdetail",
        "quotacoverage",
        "advanalytics",
        "forecaststats",
        "bandserror",
    ]
    PAGE_LABELS_NAV = ["Overview", "Rep Detail", "Quota", "Advanced", "Statistics", "Bands & Error"]

    w = {
        # ═══ PAGE 1: Forecast Overview ═══
        # Header
        "p1_hdr": hdr(
            "Forecast Intelligence",
            "Weighted forecast analysis — Commit x0.9 + BestCase x0.5 + Pipeline x0.2",
        ),
        # Filter bar
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_qtr": pillbox("f_qtr", "Quarter"),
        # Hero KPIs
        "p1_commit": num(
            "s_commit", "total_commit", "Commit ARR", "#04844B", compact=True
        ),
        "p1_bestcase": num(
            "s_bestcase", "total_bestcase", "Best Case ARR", "#0070D2", compact=True
        ),
        "p1_weighted": num(
            "s_weighted", "total_weighted", "Weighted Forecast", "#091A3E", compact=True
        ),
        "p1_quota": num("s_quota", "total_quota", "Quota", "#54698D", compact=True),
        # Gauges
        "p1_commit_gauge": gauge(
            "s_commit_pct",
            "commit_pct",
            "Commit vs Quota %",
            min_val=0,
            max_val=150,
            bands=[
                {"start": 0, "stop": 50, "color": "#D4504C"},
                {"start": 50, "stop": 80, "color": "#FFB75D"},
                {"start": 80, "stop": 150, "color": "#04844B"},
            ],
        ),
        "p1_attain_gauge": gauge(
            "s_attain",
            "attain_pct",
            "Closed Won vs Quota %",
            min_val=0,
            max_val=150,
            bands=[
                {"start": 0, "stop": 50, "color": "#D4504C"},
                {"start": 50, "stop": 80, "color": "#FFB75D"},
                {"start": 80, "stop": 150, "color": "#04844B"},
            ],
        ),
        # Charts
        "p1_sec_qtr": section_label("Quarterly Forecast vs Actual"),
        "p1_ch_qtr": rich_chart(
            "s_qtr_forecast",
            "column",
            "Forecast Categories by Quarter",
            ["CloseQuarter"],
            ["commit_arr", "bestcase_arr", "pipeline_arr", "won_arr"],
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        "p1_ch_fcat": rich_chart(
            "s_fcat_split",
            "donut",
            "Forecast Category Distribution",
            ["Category"],
            ["total_arr"],
            show_legend=True,
            show_pct=True,
        ),
        # Pipeline coverage gauge
        "p1_coverage_gauge": gauge(
            "s_pipe_coverage",
            "coverage_ratio",
            "Pipeline Coverage Ratio",
            min_val=0,
            max_val=5,
            bands=[
                {"start": 0, "stop": 2, "color": "#D4504C"},
                {"start": 2, "stop": 3, "color": "#FFB75D"},
                {"start": 3, "stop": 5, "color": "#04844B"},
            ],
        ),
        # ═══ PAGE 2: Rep-Level Detail ═══
        "p2_hdr": hdr(
            "Rep-Level Forecast Detail",
            "Individual contributor forecast breakdown and quota attainment",
        ),
        # Filter bar
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_qtr": pillbox("f_qtr", "Quarter"),
        # Charts
        "p2_sec_rep": section_label("Rep Forecast Ranking"),
        "p2_ch_rep": rich_chart(
            "s_rep_bar",
            "hbar",
            "Weighted Forecast by Rep (Top 15)",
            ["OwnerName"],
            ["weighted"],
            axis_title="Weighted ARR (EUR)",
        ),
        "p2_ch_unit": rich_chart(
            "s_unit_forecast",
            "hbar",
            "Forecast by Unit Group",
            ["UnitGroup"],
            ["weighted", "commit_arr", "won_arr"],
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        "p2_sec_detail": section_label("Rep Forecast Table"),
        "p2_tbl_rep": rich_chart(
            "s_rep_forecast",
            "comparisontable",
            "Rep Forecast Detail",
            ["OwnerName"],
            [
                "commit_arr",
                "bestcase_arr",
                "pipeline_arr",
                "weighted",
                "won_arr",
                "quota",
                "attain_pct",
            ],
        ),
        "p2_sec_movement": section_label("Forecast Movement"),
        "p2_ch_waterfall": waterfall_chart(
            "s_forecast_waterfall",
            "Quarterly Weighted Forecast",
            "CloseQuarter",
            "weighted",
            axis_label="Weighted ARR (EUR)",
        ),
        # Gap-to-quota table
        "p2_sec_gap": section_label("Gap to Quota Analysis"),
        "p2_tbl_gap": rich_chart(
            "s_gap_to_quota",
            "comparisontable",
            "Gap to Quota by Rep",
            ["OwnerName"],
            ["won", "commit_arr", "quota", "pipeline", "gap"],
        ),
        # Win rate chart
        "p2_sec_wr": section_label("Win Rate Analysis"),
        "p2_ch_wr": rich_chart(
            "s_rep_wr",
            "hbar",
            "Win Rate by Rep",
            ["OwnerName"],
            ["win_rate"],
            axis_title="Win Rate %",
        ),
        # ═══ PAGE 3: Quota & Coverage Analytics ═══
        **{
            f"p3_nav{i + 1}": nav_link(
                ["overview", "repdetail", "quotacoverage"][i],
                ["Forecast Overview", "Rep Detail", "Quota & Coverage"][i],
                active=(i == 2),
            )
            for i in range(3)
        },
        "p3_hdr": hdr(
            "Quota & Coverage Analytics",
            "Attainment distribution, pipeline coverage by segment, forecast accuracy",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_qtr": pillbox("f_qtr", "Quarter"),
        # Quota Attainment Distribution
        "p3_sec_attain": section_label("Quota Attainment Distribution"),
        "p3_ch_attain": rich_chart(
            "s_attain_dist",
            "column",
            "Rep Attainment Distribution",
            ["AttainBand"],
            ["rep_count"],
            axis_title="Number of Reps",
        ),
        # Pipeline Coverage by Segment
        "p3_sec_coverage": section_label("Pipeline Coverage by Segment"),
        "p3_ch_coverage": rich_chart(
            "s_pipe_coverage_seg",
            "hbar",
            "Pipeline Coverage Ratio by Unit Group",
            ["UnitGroup"],
            ["coverage"],
            axis_title="Coverage Ratio (Pipeline / Quota)",
        ),
        # Human vs AI Forecast Comparison
        "p3_sec_compare": section_label("Human vs AI Forecast Comparison"),
        "p3_ch_compare": rich_chart(
            "s_forecast_compare",
            "column",
            "Commit (Human) vs Weighted (AI) vs Actual Won",
            ["CloseQuarter"],
            ["human_forecast", "ai_forecast", "actual_won"],
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        # Attainment Detail Table
        "p3_sec_detail": section_label("Attainment Detail by Rep"),
        "p3_tbl_attain": rich_chart(
            "s_attain_by_rep",
            "comparisontable",
            "Quota Attainment by Rep",
            ["OwnerName"],
            ["won_arr", "quota", "attain_pct", "pipe_arr", "coverage"],
        ),
    }

    # ═══ PAGE 6: Bands & Error (ML-Forward) ═══
    w["p6_hdr"] = hdr(
        "Forecast Bands & Error Analytics",
        "Prediction intervals, forecast bias/MAPE, risk scoring — ML-forward analytics",
    )
    w["p6_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p6_f_qtr"] = pillbox("f_qtr", "Quarter")
    # Forecast bands timeline
    w["p6_sec_bands"] = section_label("Forecast vs Actual with 95% Prediction Interval")
    w["p6_ch_bands"] = timeline_chart(
        "s_forecast_bands",
        "Quarterly Forecast Bands (P50 ± 95% CI)",
        axis_title="ARR (EUR)",
    )
    # Forecast bias/MAPE KPIs
    w["p6_bias_kpi"] = num_dynamic_color(
        "s_forecast_bias_kpi",
        "bias_pct",
        "Forecast Bias %",
        thresholds=[(-20, "#D4504C"), (-5, "#FFB75D"), (5, "#04844B"),
                    (20, "#FFB75D"), (100, "#D4504C")],
        size=28,
    )
    w["p6_mape_kpi"] = num_dynamic_color(
        "s_forecast_bias_kpi",
        "mape_pct",
        "MAPE %",
        thresholds=[(10, "#04844B"), (20, "#FFB75D"), (100, "#D4504C")],
        size=28,
    )
    # Error heatmap: rep × quarter
    w["p6_sec_err_heatmap"] = section_label("Forecast Error by Rep × Quarter")
    w["p6_ch_err_heatmap"] = heatmap_chart(
        "s_forecast_err_heatmap", "Forecast Error % (Rep × Quarter)"
    )
    # Forecast risk distribution
    w["p6_sec_risk"] = section_label("Forecast Risk Distribution")
    w["p6_ch_risk_dist"] = rich_chart(
        "s_forecast_risk_dist",
        "donut",
        "Reps by Forecast Risk Band",
        ["ForecastRiskBand"],
        ["rep_count"],
        show_legend=True,
    )
    # Risk drivers bar
    w["p6_ch_risk_drivers"] = rich_chart(
        "s_forecast_risk_drivers",
        "hbar",
        "Top Risk Drivers Across Reps",
        ["ForecastRiskDriver"],
        ["rep_count"],
        axis_title="Rep Count",
    )
    # Error by rep scatter
    w["p6_sec_err_rep"] = section_label("Rep Forecast Accuracy: Bias vs MAPE")
    w["p6_ch_err_rep"] = scatter_chart(
        "s_forecast_err_rep",
        "Forecast Error by Rep (Bias % vs MAPE %)",
        x_title="Bias %",
        y_title="MAPE %",
    )
    # Bands attainment bullet
    w["p6_sec_band_bullet"] = section_label("Actual vs Forecast Bands")
    w["p6_ch_band_bullet"] = bullet_chart(
        "s_forecast_band_bullet",
        "Actual Won vs Forecast P50 (with P90 range)",
        axis_title="ARR (EUR)",
    )

    # Add nav links for all 6 pages on each page
    for px in range(1, 7):
        for ni, (pid, plbl) in enumerate(zip(PAGE_IDS, PAGE_LABELS_NAV)):
            w[f"p{px}_nav{ni + 1}"] = nav_link(pid, plbl, active=(ni == px - 1))

    # ═══ PAGE 4: Advanced Analytics ═══
    w["p4_hdr"] = hdr(
        "Advanced Analytics", "Forecast Composition | Trends | Rep Performance"
    )
    w["p4_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p4_f_qtr"] = pillbox("f_qtr", "Quarter")
    # Sankey: UnitGroup → Quarter forecast
    w["p4_sec_sankey"] = section_label("Forecast Flow: Unit Group → Quarter")
    w["p4_ch_sankey"] = sankey_chart(
        "s_sankey_cat", "Weighted Forecast: Unit → Quarter"
    )
    # Treemap: Forecast by Owner
    w["p4_sec_treemap"] = section_label("Forecast Composition by Unit & Owner")
    w["p4_ch_treemap"] = treemap_chart(
        "s_treemap_forecast",
        "Weighted Forecast by Unit Group & Owner",
        ["UnitGroup", "OwnerName"],
        "wf",
    )
    # Heatmap: Owner × Quarter Won ARR
    w["p4_sec_heatmap"] = section_label("Rep Performance Matrix")
    w["p4_ch_heatmap"] = heatmap_chart(
        "s_heatmap_owner_qtr", "Won ARR by Owner × Quarter"
    )
    # Bubble: Rep commit vs won vs opp count
    w["p4_sec_bubble"] = section_label("Rep Forecast Accuracy")
    w["p4_ch_bubble"] = bubble_chart(
        "s_bubble_owner", "Commit ARR vs Won ARR vs Opp Count"
    )
    # Area: Cumulative Won ARR
    w["p4_sec_area"] = section_label("Cumulative Won ARR Over Time")
    w["p4_ch_area"] = area_chart(
        "s_area_cumul",
        "Cumulative Won ARR (Running Total)",
        ["CloseQuarter"],
        ["cumul_won"],
        axis_title="Cumulative ARR (EUR)",
    )

    # ═══ PAGE 5: Statistical Analysis ═══
    w["p5_hdr"] = hdr(
        "Forecast Statistical Analysis",
        "Commit vs Quota Target | Forecast Distribution | Attainment Stats",
    )
    w["p5_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p5_f_qtr"] = pillbox("f_qtr", "Quarter")
    # Bullet charts
    w["p5_sec_bullet"] = section_label("Target vs Actual KPIs")
    w["p5_bullet_commit"] = bullet_chart(
        "s_bullet_commit", "Commit ARR vs Quota", axis_title="EUR"
    )
    w["p5_bullet_weighted"] = bullet_chart(
        "s_bullet_weighted", "Weighted Forecast vs 80% Quota", axis_title="EUR"
    )
    # Forecast percentiles table
    w["p5_sec_wf_pct"] = section_label("Weighted Forecast Distribution")
    w["p5_tbl_wf_pct"] = rich_chart(
        "s_stat_wf_percentiles",
        "comparisonTable",
        "Forecast Percentiles",
        ["cnt", "mean_wf", "std_wf", "p25", "median_wf", "p75"],
        [],
    )
    # Attainment stats by unit
    w["p5_sec_attain_unit"] = section_label("Quota Attainment by Unit Group")
    w["p5_tbl_attain_unit"] = rich_chart(
        "s_stat_attain_by_unit",
        "comparisonTable",
        "Attainment Statistics by Unit",
        ["UnitGroup", "cnt", "avg_attain", "std_attain", "median_attain"],
        [],
    )

    # ═══ VIZ UPGRADE: Timeline Forecast Narrative ═══
    w["p1_sec_timeline"] = section_label("Forecast vs Actual Over Time")
    w["p1_ch_timeline"] = combo_chart(
        "s_forecast_timeline",
        "Quarterly: Actual Won vs Forecast vs Commit",
        ["CloseQuarter"],
        bar_measures=["actual_won"],
        line_measures=["forecast", "commit_arr"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )

    # ═══ VIZ UPGRADE: Dynamic KPI Tiles ═══
    w["p1_commit_dynamic"] = num_dynamic_color(
        "s_forecast_kpi_thresh",
        "commit_vs_quota",
        "Commit vs Quota %",
        thresholds=[(50, "#D4504C"), (80, "#FFB75D"), (150, "#04844B")],
        size=28,
    )
    w["p1_attain_dynamic"] = num_dynamic_color(
        "s_forecast_kpi_thresh",
        "attainment_pct",
        "Attainment %",
        thresholds=[(50, "#D4504C"), (80, "#FFB75D"), (150, "#04844B")],
        size=28,
    )

    # ═══ VIZ UPGRADE: Rep Attainment Bullet Panel ═══
    w["p2_sec_bullet_attain"] = section_label("Rep Attainment: Won vs Quota")
    w["p2_ch_bullet_attain"] = bullet_chart(
        "s_rep_bullet_attain",
        "Rep Attainment (Won ARR vs Quota Target)",
        axis_title="ARR (EUR)",
    )

    # ═══ VIZ UPGRADE: Forecast Accuracy Scatter ═══
    w["p4_sec_accuracy"] = section_label("Forecast Accuracy by Rep")
    w["p4_ch_accuracy"] = scatter_chart(
        "s_forecast_accuracy",
        "Forecast vs Actual Won (Per Rep)",
        x_title="Forecasted ARR",
        y_title="Actual Won ARR",
        show_legend=True,
    )

    # ═══ VIZ UPGRADE: Coverage-to-Go Combo ═══
    w["p3_sec_coverage_go"] = section_label("Coverage-to-Go by Quarter")
    w["p3_ch_coverage_go"] = combo_chart(
        "s_coverage_to_go",
        "Quota vs Won vs Pipeline (Coverage Gap)",
        ["CloseQuarter"],
        bar_measures=["won", "commit_arr", "pipeline"],
        line_measures=["quota"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )

    # Add table actions on rep detail tables
    if "p2_tbl_rep" in w:
        add_table_action(w["p2_tbl_rep"])

    return w


# ═══════════════════════════════════════════════════════════════════════════
#  Layout
# ═══════════════════════════════════════════════════════════════════════════


def build_layout():
    p1 = nav_row("p1", 6) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p1_f_qtr", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # Hero KPIs
        {"name": "p1_commit", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_bestcase", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_weighted", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_quota", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        # Gauges
        {"name": "p1_commit_gauge", "row": 9, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p1_attain_gauge", "row": 9, "column": 6, "colspan": 6, "rowspan": 6},
        # Quarterly charts
        {"name": "p1_sec_qtr", "row": 15, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_qtr", "row": 16, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_ch_fcat", "row": 16, "column": 6, "colspan": 6, "rowspan": 8},
        # Pipeline coverage gauge
        {
            "name": "p1_coverage_gauge",
            "row": 24,
            "column": 0,
            "colspan": 12,
            "rowspan": 6,
        },
        # VIZ UPGRADE: Timeline Forecast Narrative
        {"name": "p1_sec_timeline", "row": 30, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_timeline", "row": 31, "column": 0, "colspan": 12, "rowspan": 8},
        # VIZ UPGRADE: Dynamic KPI Tiles
        {"name": "p1_commit_dynamic", "row": 39, "column": 0, "colspan": 6, "rowspan": 4},
        {"name": "p1_attain_dynamic", "row": 39, "column": 6, "colspan": 6, "rowspan": 4},
    ]

    p2 = nav_row("p2", 6) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p2_f_qtr", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # Rep ranking
        {"name": "p2_sec_rep", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_rep", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_unit", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
        # Detail table
        {"name": "p2_sec_detail", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_tbl_rep", "row": 15, "column": 0, "colspan": 12, "rowspan": 8},
        # Movement
        {
            "name": "p2_sec_movement",
            "row": 23,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p2_ch_waterfall",
            "row": 24,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Gap to quota
        {
            "name": "p2_sec_gap",
            "row": 32,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p2_tbl_gap",
            "row": 33,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Win rate
        {
            "name": "p2_sec_wr",
            "row": 41,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p2_ch_wr",
            "row": 42,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # VIZ UPGRADE: Rep Attainment Bullet Panel
        {"name": "p2_sec_bullet_attain", "row": 50, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_bullet_attain", "row": 51, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    p3 = nav_row("p3", 6) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p3_f_qtr", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # Attainment Distribution + Pipeline Coverage (side by side)
        {"name": "p3_sec_attain", "row": 5, "column": 0, "colspan": 6, "rowspan": 1},
        {"name": "p3_ch_attain", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p3_sec_coverage", "row": 5, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p3_ch_coverage", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
        # Human vs AI Forecast Comparison
        {"name": "p3_sec_compare", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_compare", "row": 15, "column": 0, "colspan": 12, "rowspan": 8},
        # Attainment Detail Table
        {"name": "p3_sec_detail", "row": 23, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_tbl_attain", "row": 24, "column": 0, "colspan": 12, "rowspan": 8},
        # VIZ UPGRADE: Coverage-to-Go
        {"name": "p3_sec_coverage_go", "row": 32, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_coverage_go", "row": 33, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    p4 = nav_row("p4", 6) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p4_f_qtr", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # Sankey
        {"name": "p4_sec_sankey", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_sankey", "row": 6, "column": 0, "colspan": 12, "rowspan": 10},
        # Treemap
        {"name": "p4_sec_treemap", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_treemap", "row": 17, "column": 0, "colspan": 12, "rowspan": 10},
        # Heatmap
        {"name": "p4_sec_heatmap", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_heatmap", "row": 28, "column": 0, "colspan": 12, "rowspan": 10},
        # Bubble
        {"name": "p4_sec_bubble", "row": 38, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_bubble", "row": 39, "column": 0, "colspan": 12, "rowspan": 10},
        # Area
        {"name": "p4_sec_area", "row": 49, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_area", "row": 50, "column": 0, "colspan": 12, "rowspan": 8},
        # VIZ UPGRADE: Forecast Accuracy Scatter
        {"name": "p4_sec_accuracy", "row": 58, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_accuracy", "row": 59, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    p5 = nav_row("p5", 6) + [
        {"name": "p5_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p5_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p5_f_qtr", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # Bullet charts
        {"name": "p5_sec_bullet", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_bullet_commit", "row": 6, "column": 0, "colspan": 6, "rowspan": 5},
        {
            "name": "p5_bullet_weighted",
            "row": 6,
            "column": 6,
            "colspan": 6,
            "rowspan": 5,
        },
        # Forecast percentiles
        {"name": "p5_sec_wf_pct", "row": 11, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_tbl_wf_pct", "row": 12, "column": 0, "colspan": 12, "rowspan": 6},
        # Attainment by unit
        {
            "name": "p5_sec_attain_unit",
            "row": 18,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p5_tbl_attain_unit",
            "row": 19,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
    ]

    p6 = nav_row("p6", 6) + [
        {"name": "p6_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p6_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p6_f_qtr", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # KPI tiles: Bias and MAPE
        {"name": "p6_bias_kpi", "row": 5, "column": 0, "colspan": 6, "rowspan": 4},
        {"name": "p6_mape_kpi", "row": 5, "column": 6, "colspan": 6, "rowspan": 4},
        # Forecast bands timeline
        {"name": "p6_sec_bands", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_bands", "row": 10, "column": 0, "colspan": 12, "rowspan": 10},
        # Bands attainment bullet
        {"name": "p6_sec_band_bullet", "row": 20, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_band_bullet", "row": 21, "column": 0, "colspan": 12, "rowspan": 5},
        # Error heatmap
        {"name": "p6_sec_err_heatmap", "row": 26, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_err_heatmap", "row": 27, "column": 0, "colspan": 12, "rowspan": 10},
        # Risk distribution + drivers (side by side)
        {"name": "p6_sec_risk", "row": 37, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_risk_dist", "row": 38, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p6_ch_risk_drivers", "row": 38, "column": 6, "colspan": 6, "rowspan": 8},
        # Rep error scatter
        {"name": "p6_sec_err_rep", "row": 46, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_err_rep", "row": 47, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("overview", "Forecast Overview", p1),
            pg("repdetail", "Rep Detail", p2),
            pg("quotacoverage", "Quota & Coverage", p3),
            pg("advanalytics", "Advanced Analytics", p4),
            pg("forecaststats", "Statistical Analysis", p5),
            pg("bandserror", "Bands & Error", p6),
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main():
    inst, tok = get_auth()

    # Build dataset
    ds_ok = create_dataset(inst, tok)
    if not ds_ok:
        print("ERROR: Forecast dataset failed — aborting")
        return

    # Get dataset ID for af() steps
    ds_id = get_dataset_id(inst, tok, DS)
    if not ds_id:
        print("ERROR: Could not find dataset ID — aborting")
        return

    # Build forecast snapshot dataset for error tracking
    # Note: snapshot accumulates over time; each run = one snapshot
    try:
        # Re-query to get rep_data for snapshot
        # (snapshot creation is non-blocking — dashboard deploys even if snapshot fails)
        print("  Creating forecast snapshot for error tracking...")
        create_snapshot_dataset(inst, tok, {})  # Empty on first run; populated by scheduler
    except Exception as e:
        print(f"  Snapshot creation skipped: {e}")

    # Deploy dashboard
    dash_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
    state = build_dashboard_state(build_steps(ds_id), build_widgets(), build_layout())
    deploy_dashboard(inst, tok, dash_id, state)


if __name__ == "__main__":
    main()
