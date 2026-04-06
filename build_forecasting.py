#!/usr/bin/env python3
"""Build the Forecast Intelligence dashboard — DIY replacement for Revenue Intelligence.

Phase 5 of the interactivity upgrade:
  - Query ForecastingItem for manager-submitted forecasts
  - Query pipeline data aggregated by owner + forecast category
  - Compute weighted forecast: Commit * 0.9 + BestCase * 0.5 + Pipeline * 0.2
  - Join with quota data if available

Pages:
  1. Forecast Overview — KPI tiles, commit vs quota gauge, quarterly forecast vs actual
  2. Rep-Level Detail — rep forecast table, category breakdown, waterfall

Dataset: Forecast_Intelligence
"""

import csv
import io
import logging

from crm_analytics_runtime import builder_run  # pyright: ignore[reportMissingImports]
from simcorp_fields import assert_org_schema  # pyright: ignore[reportMissingImports]

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
    bullet_chart,
    compare_table,
    KPI_CARD_STYLE,
    line_chart,
    combo_chart,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

DS = "Forecast_Intelligence"
DS_LABEL = "Forecast Intelligence"
DASHBOARD_LABEL = "Forecast Intelligence"

# Consulting-grade faceting: KPIs respond to filter pillboxes only
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_unit", "f_qtr"],
    },
}

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

    Returns (upload_ok, row_count).
    """
    logger.info("\n=== Building Forecast Intelligence dataset ===")

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
    logger.info("  Queried %d opportunities", len(opps))

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
        logger.info("  Queried %d forecast items", len(forecast_items))
    except Exception as e:
        logger.warning("  ForecastingItem not available: %s", e)

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
            elif fcat == "Pipeline":
                d["PipelineARR"] += arr

        # Keep latest UnitGroup and quota
        if unit_group:
            d["UnitGroup"] = unit_group
        if quota and quota > d["QuotaAmount"]:
            d["QuotaAmount"] = quota

    # Generate CSV rows with weighted forecast
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
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()

    for key, d in rep_data.items():
        weighted = (
            d["CommitARR"] * COMMIT_WEIGHT
            + d["BestCaseARR"] * BEST_CASE_WEIGHT
            + d["PipelineARR"] * PIPELINE_WEIGHT
        )
        quota = d["QuotaAmount"]
        attainment = round((d["ClosedWonARR"] / quota) * 100, 1) if quota > 0 else 0

        writer.writerow(
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
            }
        )

    row_count = len(rep_data)
    csv_bytes = buf.getvalue().encode("utf-8")
    logger.info("  CSV: %s bytes, %d rows", f"{len(csv_bytes):,}", row_count)

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
    ]

    upload_ok = upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)
    return upload_ok, row_count


# ═══════════════════════════════════════════════════════════════════════════
#  Steps
# ═══════════════════════════════════════════════════════════════════════════


def build_steps(ds_id):
    DS_META = [{"id": ds_id, "name": DS}]
    L = f'q = load "{DS}";\n'
    FY26 = "q = filter q by FiscalYear == 2026;\n"

    steps = {
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
            + FY26
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
            + FY26
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
            + FY26
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
            + FY26
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
            + FY26
            + UF
            + QF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(CommitARR) as actual, "
            + "sum(QuotaAmount) as target;"
        ),
        "s_bullet_weighted": sq(
            L
            + FY26
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
    }

    # Apply facet scope so KPI summary steps respond to filter pillboxes
    for key in (
        "s_commit",
        "s_bestcase",
        "s_weighted",
        "s_quota",
        "s_commit_pct",
        "s_attain",
        "s_pipe_coverage",
    ):
        steps[key].update(KPI_FACET_SCOPE)

    return steps


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
    ]
    PAGE_LABELS_NAV = ["Overview", "Rep Detail", "Quota", "Advanced", "Statistics"]

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
            "s_commit",
            "total_commit",
            "Commit ARR",
            "#04844B",
            compact=True,
            tier="primary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_bestcase": num(
            "s_bestcase",
            "total_bestcase",
            "Best Case ARR",
            "#0070D2",
            compact=True,
            tier="primary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_weighted": num(
            "s_weighted",
            "total_weighted",
            "Weighted Forecast",
            "#091A3E",
            compact=True,
            tier="primary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_quota": num(
            "s_quota",
            "total_quota",
            "Quota",
            "#54698D",
            compact=True,
            tier="secondary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
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
        "p1_ch_qtr": combo_chart(
            "s_qtr_forecast",
            "Forecast Categories by Quarter",
            ["CloseQuarter"],
            bar_measures=["commit_arr", "bestcase_arr", "pipeline_arr"],
            line_measures=["won_arr"],
            show_legend=True,
            axis_title="ARR (EUR)",
            axis2_title="Closed Won ARR",
            axis1_format="#,##0",
            axis2_format="#,##0",
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
            show_values=True,
        ),
        "p2_ch_unit": rich_chart(
            "s_unit_forecast",
            "hbar",
            "Forecast by Unit Group",
            ["UnitGroup"],
            ["weighted", "commit_arr", "won_arr"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p2_sec_detail": section_label("Rep Forecast Table"),
        "p2_tbl_rep": compare_table(
            "s_rep_forecast",
            "Rep Forecast Detail",
            columns=[
                "OwnerName",
                "commit_arr",
                "bestcase_arr",
                "pipeline_arr",
                "weighted",
                "won_arr",
                "quota",
                "attain_pct",
            ],
            format_rules=[
                {
                    "measure": "attain_pct",
                    "ranges": [
                        {"min": 0, "max": 50, "color": "#D4504C"},
                        {"min": 50, "max": 80, "color": "#FFB75D"},
                        {"min": 80, "max": 999, "color": "#04844B"},
                    ],
                }
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
        "p2_tbl_gap": compare_table(
            "s_gap_to_quota",
            "Gap to Quota by Rep",
            columns=["OwnerName", "won", "commit_arr", "quota", "pipeline", "gap"],
            format_rules=[
                {
                    "measure": "gap",
                    "ranges": [
                        {"min": -999999999, "max": 0, "color": "#04844B"},
                        {"min": 0, "max": 999999999, "color": "#D4504C"},
                    ],
                }
            ],
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
            show_values=True,
            reference_lines=[
                {
                    "label": "50% Benchmark",
                    "type": "constant",
                    "value": 50,
                    "axis": "measureAxis1",
                    "style": {"color": "#0070D2", "dashLength": 4, "width": 1},
                }
            ],
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
            show_values=True,
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
            show_values=True,
            reference_lines=[
                {
                    "label": "3x Target",
                    "type": "constant",
                    "value": 3,
                    "axis": "measureAxis1",
                    "style": {"color": "#04844B", "dashLength": 4, "width": 1},
                }
            ],
        ),
        # Human vs AI Forecast Comparison
        "p3_sec_compare": section_label("Human vs AI Forecast Comparison"),
        "p3_ch_compare": combo_chart(
            "s_forecast_compare",
            "Commit (Human) vs Weighted (AI) vs Actual Won",
            ["CloseQuarter"],
            bar_measures=["human_forecast", "ai_forecast"],
            line_measures=["actual_won"],
            show_legend=True,
            axis_title="ARR (EUR)",
            axis2_title="Actual Won ARR",
            axis1_format="#,##0",
            axis2_format="#,##0",
            reference_lines=[
                {
                    "label": "Forecast Accuracy Target",
                    "type": "constant",
                    "value": 0,
                    "axis": "measureAxis2",
                    "style": {"color": "#D4504C", "dashLength": 4, "width": 1},
                }
            ],
        ),
        # Attainment Detail Table
        "p3_sec_detail": section_label("Attainment Detail by Rep"),
        "p3_tbl_attain": compare_table(
            "s_attain_by_rep",
            "Quota Attainment by Rep",
            columns=[
                "OwnerName",
                "won_arr",
                "quota",
                "attain_pct",
                "pipe_arr",
                "coverage",
            ],
            format_rules=[
                {
                    "measure": "attain_pct",
                    "ranges": [
                        {"min": 0, "max": 50, "color": "#D4504C"},
                        {"min": 50, "max": 80, "color": "#FFB75D"},
                        {"min": 80, "max": 999, "color": "#04844B"},
                    ],
                },
                {
                    "measure": "coverage",
                    "ranges": [
                        {"min": 0, "max": 2, "color": "#D4504C"},
                        {"min": 2, "max": 3, "color": "#FFB75D"},
                        {"min": 3, "max": 999, "color": "#04844B"},
                    ],
                },
            ],
        ),
    }

    # Add nav links for all 5 pages on each page
    for px in range(1, 6):
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
    w["p4_ch_area"] = line_chart(
        "s_area_cumul",
        "Cumulative Won ARR (Running Total)",
        show_legend=True,
        axis_title="Cumulative ARR (EUR)",
        reference_lines=[
            {
                "label": "Quota Run-Rate",
                "type": "constant",
                "value": 0,
                "axis": "measureAxis1",
                "style": {"color": "#D4504C", "dashLength": 4, "width": 1},
            }
        ],
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
    w["p5_tbl_wf_pct"] = compare_table(
        "s_stat_wf_percentiles",
        "Forecast Percentiles",
        columns=["cnt", "mean_wf", "std_wf", "p25", "median_wf", "p75"],
    )
    # Attainment stats by unit
    w["p5_sec_attain_unit"] = section_label("Quota Attainment by Unit Group")
    w["p5_tbl_attain_unit"] = compare_table(
        "s_stat_attain_by_unit",
        "Attainment Statistics by Unit",
        columns=["UnitGroup", "cnt", "avg_attain", "std_attain", "median_attain"],
        format_rules=[
            {
                "measure": "avg_attain",
                "ranges": [
                    {"min": 0, "max": 50, "color": "#D4504C"},
                    {"min": 50, "max": 80, "color": "#FFB75D"},
                    {"min": 80, "max": 999, "color": "#04844B"},
                ],
            }
        ],
    )

    return w


# ═══════════════════════════════════════════════════════════════════════════
#  Layout
# ═══════════════════════════════════════════════════════════════════════════


def build_layout():
    p1 = nav_row("p1", 5) + [
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
    ]

    p2 = nav_row("p2", 5) + [
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
    ]

    p3 = nav_row("p3", 5) + [
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
    ]

    p4 = nav_row("p4", 5) + [
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
    ]

    p5 = nav_row("p5", 5) + [
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

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("overview", "Forecast Overview", p1),
            pg("repdetail", "Rep Detail", p2),
            pg("quotacoverage", "Quota & Coverage", p3),
            pg("advanalytics", "Advanced Analytics", p4),
            pg("forecaststats", "Statistical Analysis", p5),
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main() -> None:
    with builder_run("Forecast_Intelligence", __file__) as summary:
        inst, tok = get_auth()
        assert_org_schema(
            inst,
            tok,
            objects=[
                "Account",
                "Contact",
                "Contract",
                "ForecastingItem",
                "Opportunity",
                "OpportunityFieldHistory",
                "OpportunityHistory",
                "User",
            ],
        )

        upload_ok, row_count = create_dataset(inst, tok)
        summary.row_count = row_count
        if not upload_ok:
            raise SystemExit("Dataset upload failed")

        ds_id = get_dataset_id(inst, tok, DS)
        if not ds_id:
            raise SystemExit(f"Could not resolve dataset id for {DS}")
        summary.dataset_id = ds_id

        # Deploy dashboard
        dash_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
        state = build_dashboard_state(
            build_steps(ds_id),
            build_widgets(),
            build_layout(),
            bg_color="#F4F6F9",
            cell_spacing=8,
            row_height="normal",
        )
        deploy_dashboard(inst, tok, dash_id, state)


if __name__ == "__main__":
    main()
