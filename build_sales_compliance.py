#!/usr/bin/env python3
"""Build the Sales Process Compliance (AP 1.4) dashboard.

Reuses the existing Opp_Mgmt_KPIs dataset — no dataset upload needed.
8 pages: Stage Bottlenecks, Stuck & Past-Due, Velocity by Type,
         Close Date Hygiene, Win/Loss Analysis, Advanced Analytics,
         Statistical Analysis, Forecast Integrity (ML-Forward).

Fields available in Opp_Mgmt_KPIs:
  dim:  Id, Name, OwnerName, AccountName, UnitGroup, SalesRegion,
        IsClosed, IsWon, StageName, Type, ForecastCategory,
        CloseMonth, CreatedMonth, FYLabel, HitStage1-6,
        WonLostReason, SubReason, WinScoreBand
  date: CloseDate, CreatedDate
  msr:  FiscalYear, FiscalQuarter, ARR, Amount, Probability,
        AgeInDays, DaysInStage, SalesCycleDuration,
        Stage1to2Days..Stage5to6Days, WinScore

Visualization upgrade (v3):
  - Dynamic KPI tiles with threshold-based coloring (num_dynamic_color)
  - Compliance scorecard (stuck_rate, avg_days_in_stage)
  - Bottleneck distributions (DaysInStage distribution per stage)
  - Table actions on stuck/past-due tables
  - Selection interactions for cross-filtering

ML-Forward upgrade (v4):
  - Forecast integrity scoring (ForecastCategory vs WinScore alignment)
  - Dynamic date handling for past-due calculations (no more static dates)
  - Notification readiness for mismatch alerts
"""

import sys
from datetime import datetime as _dt

from crm_analytics_helpers import (
    af,
    build_dashboard_state,
    coalesce_filter,
    create_dashboard_if_needed,
    deploy_dashboard,
    gauge,
    get_auth,
    hdr,
    nav_link,
    nav_row,
    num,
    num_with_trend,
    num_dynamic_color,
    pg,
    pillbox,
    rich_chart,
    section_label,
    sq,
    trend_step,
    waterfall_chart,
    heatmap_chart,
    bubble_chart,
    bullet_chart,
    sankey_chart,
    treemap_chart,
    area_chart,
    combo_chart,
    add_table_action,
    add_selection_interaction,
    compliance_scorecard_step,
    forecast_integrity_step,
    forecast_integrity_heatmap_step,
    scatter_chart,
)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

DS = "Opp_Mgmt_KPIs"
DS_ID = "0FbTb0000019llVKAQ"
DS_META = [{"id": DS_ID, "name": DS}]
DASHBOARD_LABEL = "Sales Process Compliance KPIs"

# Today's date for past-due comparisons (SAQL string comparison on yyyy-MM-dd)
TODAY = _dt.utcnow().strftime("%Y-%m-%d")

# ═══════════════════════════════════════════════════════════════════════════
#  SAQL fragments
# ═══════════════════════════════════════════════════════════════════════════

L = f'q = load "{DS}";\n'
FY = "q = filter q by FiscalYear == 2026;\n"
OPEN = 'q = filter q by IsClosed == "false";\n'
WON = 'q = filter q by IsWon == "true";\n'
CLOSED = 'q = filter q by IsClosed == "true";\n'
PAST_DUE = f'q = filter q by CloseDate < "{TODAY}";\n'

# ── Coalesce filter bindings ──────────────────────────────────────────────
UF = coalesce_filter("f_unit", "UnitGroup")
RF = coalesce_filter("f_region", "SalesRegion")
TF = coalesce_filter("f_type", "Type")
SGF = coalesce_filter("f_stage", "StageName")

# ── Trend period filters (FY2026 vs FY2025) ──────────────────────────────
_CURRENT_FY = "q = filter q by FiscalYear == 2026;\n"
_PRIOR_FY = "q = filter q by FiscalYear == 2025;\n"


# ═══════════════════════════════════════════════════════════════════════════
#  Steps
# ═══════════════════════════════════════════════════════════════════════════


def build_steps():
    return {
        # ── Filter steps (aggregateflex) ──────────────────────────────────
        "f_unit": af("UnitGroup", DS_META),
        "f_region": af("SalesRegion", DS_META),
        "f_type": af("Type", DS_META),
        "f_stage": af("StageName", DS_META),
        # ══════════════════════════════════════════════════════════════════
        #  PAGE 1: Stage Bottlenecks
        # ══════════════════════════════════════════════════════════════════
        # Number: Count of open opps stuck in Stage 3/4 (DaysInStage > 30)
        "s_stuck_cnt": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = filter q by DaysInStage > 30;\n"
            + 'q = filter q by (StageName like "%3%" or StageName like "%4%");\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Number: ARR of those stuck deals
        "s_stuck_arr": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = filter q by DaysInStage > 30;\n"
            + 'q = filter q by (StageName like "%3%" or StageName like "%4%");\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_acv;"
        ),
        # Stackhbar: Stage aging distribution (age band within each stage)
        # (groups by StageName — omit SGF)
        "s_stg_aging": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + "q = foreach q generate StageName, "
            + '(case when DaysInStage <= 30 then "0-30d" '
            + 'when DaysInStage <= 60 then "31-60d" '
            + 'when DaysInStage <= 90 then "61-90d" '
            + 'else "90d+" end) as AgeBand, ARR;\n'
            + "q = group q by (StageName, AgeBand);\n"
            + "q = foreach q generate StageName, AgeBand, "
            + "count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by StageName asc;"
        ),
        # Comparisontable: Stuck deals (DaysInStage > 30), top 25
        "s_stuck_list": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = filter q by DaysInStage > 30;\n"
            + "q = foreach q generate Id, Name, AccountName, StageName, "
            + "OwnerName, DaysInStage, ARR;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 25;"
        ),
        # Waterfall: Stage dropoff — net opp count at each stage
        # (groups by StageName — omit SGF)
        "s_stg_waterfall": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, count() as cnt;\n"
            + "q = order q by StageName asc;"
        ),
        # ══════════════════════════════════════════════════════════════════
        #  PAGE 2: Stuck & Past-Due
        # ══════════════════════════════════════════════════════════════════
        # Number: Count of past-due opps (open + CloseDate < today)
        "s_pastdue_cnt": sq(
            L
            + FY
            + OPEN
            + PAST_DUE
            + UF
            + RF
            + TF
            + SGF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Number: ARR of past-due opps
        "s_pastdue_arr": sq(
            L
            + FY
            + OPEN
            + PAST_DUE
            + UF
            + RF
            + TF
            + SGF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as sum_acv;"
        ),
        # Gauge: Pipeline hygiene score (% of open opps with future close dates)
        "s_hygiene": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = foreach q generate "
            + f'(case when CloseDate >= "{TODAY}" then 1 else 0 end) as is_future;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(sum(is_future) / count()) * 100 as hygiene_pct;"
        ),
        # Hbar: Past-due by owner (count of past-due opps per owner)
        "s_pastdue_owner": sq(
            L
            + FY
            + OPEN
            + PAST_DUE
            + UF
            + RF
            + TF
            + SGF
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by cnt desc;"
        ),
        # Comparisontable: Stuck opps (DaysInStage > 30), top 25
        "s_stuck_detail": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = filter q by DaysInStage > 30;\n"
            + "q = foreach q generate Id, Name, StageName, OwnerName, "
            + "DaysInStage, ARR;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 25;"
        ),
        # ══════════════════════════════════════════════════════════════════
        #  PAGE 3: Velocity by Type
        # ══════════════════════════════════════════════════════════════════
        # Gauge: Land velocity (avg SalesCycleDuration for Type = "Land")
        "s_vel_land": sq(
            L
            + FY
            + WON
            + UF
            + RF
            + TF
            + SGF
            + 'q = filter q by Type == "Land";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(SalesCycleDuration) as avg_cycle;"
        ),
        # Gauge: Expand velocity
        "s_vel_expand": sq(
            L
            + FY
            + WON
            + UF
            + RF
            + TF
            + SGF
            + 'q = filter q by Type == "Expand";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(SalesCycleDuration) as avg_cycle;"
        ),
        # Gauge: Renewal velocity
        "s_vel_renewal": sq(
            L
            + FY
            + WON
            + UF
            + RF
            + TF
            + SGF
            + 'q = filter q by Type == "Renewal";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(SalesCycleDuration) as avg_cycle;"
        ),
        # Hbar: Cycle time comparison by Type
        # (groups by Type — omit TF)
        "s_vel_type": sq(
            L
            + FY
            + WON
            + UF
            + RF
            + SGF
            + "q = group q by Type;\n"
            + "q = foreach q generate Type, avg(SalesCycleDuration) as avg_cycle, "
            + "count() as cnt;\n"
            + "q = order q by avg_cycle desc;"
        ),
        # Combo: Monthly velocity trend (columns = deal count, line = avg cycle)
        # Won deals grouped by CloseMonth
        "s_vel_monthly": sq(
            L
            + FY
            + WON
            + UF
            + RF
            + TF
            + SGF
            + "q = foreach q generate "
            + "substr(CloseDate, 1, 7) as CloseMonth, "
            + "SalesCycleDuration;\n"
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, "
            + "count() as deal_count, "
            + "avg(SalesCycleDuration) as avg_cycle;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # Velocity trend: avg days in stage by quarter (grouped by stage)
        "s_velocity_trend": sq(
            L
            + UF
            + RF
            + "q = filter q by DaysInStage is not null;\n"
            + "q = filter q by FiscalYear == 2026;\n"
            + 'q = foreach q generate (case when substr(CloseDate, 6, 2) in ["01","02","03"] then "Q1" '
            + 'when substr(CloseDate, 6, 2) in ["04","05","06"] then "Q2" '
            + 'when substr(CloseDate, 6, 2) in ["07","08","09"] then "Q3" '
            + 'else "Q4" end) as Quarter, StageName, DaysInStage;\n'
            + "q = group q by (Quarter, StageName);\n"
            + "q = foreach q generate Quarter, StageName, avg(DaysInStage) as avg_days;\n"
            + "q = order q by Quarter asc;"
        ),
        # Stage conversion rates: won / total per stage
        "s_stage_conversion": sq(
            L
            + UF
            + RF
            + "q = filter q by FiscalYear == 2026;\n"
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, count() as total, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as won_cnt;\n'
            + "q = foreach q generate StageName, total, won_cnt, "
            + "(won_cnt / total) * 100 as conv_rate;\n"
            + "q = order q by conv_rate desc;"
        ),
        # ══════════════════════════════════════════════════════════════════
        #  PAGE 4: Close Date Hygiene
        # ══════════════════════════════════════════════════════════════════
        # Donut: Close date distribution (past / this quarter / next quarter / beyond)
        "s_close_dist": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = foreach q generate "
            + f'(case when CloseDate < "{TODAY}" then "Past Due" '
            + 'when CloseDate < "2026-04-01" then "This Quarter" '
            + 'when CloseDate < "2026-07-01" then "Next Quarter" '
            + 'else "Beyond" end) as CloseBucket, ARR;\n'
            + "q = group q by CloseBucket;\n"
            + "q = foreach q generate CloseBucket, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by CloseBucket asc;"
        ),
        # Number: Pushed-out count (past-due open deals)
        "s_pushed_cnt": sq(
            L
            + FY
            + OPEN
            + PAST_DUE
            + UF
            + RF
            + TF
            + SGF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt, sum(ARR) as sum_acv;"
        ),
        # Waterfall: Monthly close date movements (opp count closing each month)
        "s_close_monthly": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = foreach q generate substr(CloseDate, 1, 7) as CloseMonth, ARR;\n"
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, count() as cnt;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # Comparisontable: Past close date opps, top 25
        "s_pastdue_list": sq(
            L
            + FY
            + OPEN
            + PAST_DUE
            + UF
            + RF
            + TF
            + SGF
            + "q = foreach q generate Id, Name, AccountName, OwnerName, "
            + "StageName, CloseDate, ARR;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 25;"
        ),
        # ══════════════════════════════════════════════════════════════════
        #  WIN SCORING (Phase 2)
        # ══════════════════════════════════════════════════════════════════
        "s_ws_avg": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = group q by all;\n"
            + "q = foreach q generate avg(WinScore) as avg_score;"
        ),
        "s_ws_band": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = group q by WinScoreBand;\n"
            + "q = foreach q generate WinScoreBand, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by WinScoreBand asc;"
        ),
        "s_ws_top25": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = foreach q generate Name, AccountName, StageName, OwnerName, "
            + "WinScore, WinScoreBand, ARR, DaysInStage;\n"
            + "q = order q by WinScore desc;\n"
            + "q = limit q 25;"
        ),
        # Groups by StageName — omit SGF
        "s_ws_stage": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, avg(WinScore) as avg_score, count() as cnt;\n"
            + "q = order q by avg_score desc;"
        ),
        # ══════════════════════════════════════════════════════════════════
        #  KPI TREND STEPS (Phase 3) — FY2026 vs FY2025
        # ══════════════════════════════════════════════════════════════════
        # Stuck count trend: open opps in Stage 3/4 with DaysInStage > 30
        "s_stuck_cnt_t": trend_step(
            DS,
            OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = filter q by DaysInStage > 30;\n"
            + 'q = filter q by (StageName like "%3%" or StageName like "%4%");\n',
            _CURRENT_FY,
            _PRIOR_FY,
            "all",
            "count()",
            "cnt",
        ),
        # Stuck ARR trend
        "s_stuck_arr_t": trend_step(
            DS,
            OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = filter q by DaysInStage > 30;\n"
            + 'q = filter q by (StageName like "%3%" or StageName like "%4%");\n',
            _CURRENT_FY,
            _PRIOR_FY,
            "all",
            "sum(ARR)",
            "sum_acv",
        ),
        # Past-due count trend: open opps with CloseDate < today
        "s_pastdue_cnt_t": trend_step(
            DS,
            OPEN + PAST_DUE + UF + RF + TF + SGF,
            _CURRENT_FY,
            _PRIOR_FY,
            "all",
            "count()",
            "cnt",
        ),
        # ═══ FORECAST CATEGORY COMPLIANCE (AP 1.4) ═══
        "s_fcat_dist": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + "q = group q by ForecastCategory;\n"
            + "q = foreach q generate ForecastCategory, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by sum_acv desc;"
        ),
        "s_fcat_trend": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + "q = foreach q generate CloseMonth, ForecastCategory, ARR;\n"
            + "q = group q by (CloseMonth, ForecastCategory);\n"
            + "q = foreach q generate CloseMonth, ForecastCategory, sum(ARR) as sum_acv;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # ═══ PER-STAGE SLA (AP 1.4) ═══
        "s_stage_sla": sq(
            L
            + FY
            + UF
            + RF
            + TF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "avg(Stage1to2Days) as avg_1to2, "
            + "avg(Stage2to3Days) as avg_2to3, "
            + "avg(Stage3to4Days) as avg_3to4, "
            + "avg(Stage4to5Days) as avg_4to5, "
            + "avg(Stage5to6Days) as avg_5to6;"
        ),
        "s_stage_sla_rows": sq(
            L
            + FY
            + UF
            + RF
            + TF
            + "q = group q by all;\n"
            + 'q = foreach q generate "1→2" as StageTransition, avg(Stage1to2Days) as avg_days;\n'
            + "q = union "
            + f'(q = load "{DS}"; {FY}{UF}{RF}{TF}q = group q by all; '
            + 'q = foreach q generate "2→3" as StageTransition, avg(Stage2to3Days) as avg_days), '
            + f'(q = load "{DS}"; {FY}{UF}{RF}{TF}q = group q by all; '
            + 'q = foreach q generate "3→4" as StageTransition, avg(Stage3to4Days) as avg_days), '
            + f'(q = load "{DS}"; {FY}{UF}{RF}{TF}q = group q by all; '
            + 'q = foreach q generate "4→5" as StageTransition, avg(Stage4to5Days) as avg_days), '
            + f'(q = load "{DS}"; {FY}{UF}{RF}{TF}q = group q by all; '
            + 'q = foreach q generate "5→6" as StageTransition, avg(Stage5to6Days) as avg_days);\n'
            + "q = order q by StageTransition asc;"
        ),
        # ═══ ITERATION 3: Win/Loss Reason (Additive CRO #3) + Stuck Owner Leaderboard ═══
        # Per-owner stuck leaderboard (for Page 2 enhancement)
        "s_stuck_by_owner": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = filter q by DaysInStage > 30;\n"
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, count() as stuck_cnt, "
            + "sum(ARR) as stuck_arr, avg(DaysInStage) as avg_days_stuck;\n"
            + "q = order q by stuck_cnt desc;\n"
            + "q = limit q 20;"
        ),
        # Win/Loss Reason distribution (closed deals only)
        "s_wonlost_reason": sq(
            L
            + FY
            + CLOSED
            + UF
            + RF
            + TF
            + 'q = filter q by WonLostReason != "";\n'
            + "q = group q by WonLostReason;\n"
            + "q = foreach q generate WonLostReason, count() as cnt, sum(ARR) as sum_acv;\n"
            + "q = order q by cnt desc;"
        ),
        # Loss reasons only (Won=false)
        "s_loss_reasons": sq(
            L
            + FY
            + CLOSED
            + UF
            + RF
            + TF
            + 'q = filter q by IsWon == "false";\n'
            + 'q = filter q by WonLostReason != "";\n'
            + "q = group q by WonLostReason;\n"
            + "q = foreach q generate WonLostReason, count() as cnt, sum(ARR) as lost_arr;\n"
            + "q = order q by lost_arr desc;"
        ),
        # Loss reasons by Type (Land/Expand/Renewal)
        "s_loss_by_type": sq(
            L
            + FY
            + CLOSED
            + UF
            + RF
            + 'q = filter q by IsWon == "false";\n'
            + 'q = filter q by WonLostReason != "";\n'
            + "q = group q by (Type, WonLostReason);\n"
            + "q = foreach q generate Type, WonLostReason, count() as cnt;\n"
            + "q = order q by Type asc;"
        ),
        # Win reasons (Won=true)
        "s_win_reasons": sq(
            L
            + FY
            + CLOSED
            + UF
            + RF
            + TF
            + 'q = filter q by IsWon == "true";\n'
            + 'q = filter q by WonLostReason != "";\n'
            + "q = group q by WonLostReason;\n"
            + "q = foreach q generate WonLostReason, count() as cnt, sum(ARR) as won_arr;\n"
            + "q = order q by won_arr desc;"
        ),
        # Lost deal detail (top 25 by ARR)
        "s_lost_detail": sq(
            L
            + FY
            + CLOSED
            + UF
            + RF
            + TF
            + 'q = filter q by IsWon == "false";\n'
            + 'q = filter q by WonLostReason != "";\n'
            + "q = foreach q generate Name, AccountName, OwnerName, Type, "
            + "WonLostReason, SubReason, ARR, SalesCycleDuration;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 25;"
        ),
        # ═══ V2: Advanced Visualizations ═══
        # Heatmap: Owner × Month pipeline
        "s_heatmap_owner_month": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + 'q = filter q by OwnerName != "";\n'
            + "q = group q by (OwnerName, CloseMonth);\n"
            + "q = foreach q generate OwnerName, CloseMonth, sum(ARR) as pipe_arr;\n"
            + "q = order q by CloseMonth asc;"
        ),
        # Bubble: Rep performance — Pipeline × Win Rate × Deal Count
        "s_bubble_rep": sq(
            L
            + FY
            + UF
            + RF
            + TF
            + 'q = filter q by OwnerName != "";\n'
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(ARR) as total_pipe, count() as deal_cnt, "
            + '(sum(case when IsWon == "true" then 1 else 0 end) / '
            + '(case when sum(case when IsClosed == "true" then 1 else 0 end) > 0 '
            + 'then sum(case when IsClosed == "true" then 1 else 0 end) '
            + "else 1 end)) * 100 as win_rate;\n"
            + "q = order q by total_pipe desc;\n"
            + "q = limit q 30;"
        ),
        # Heatmap: Stage × Type days-in-stage
        "s_heatmap_stage_type": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + 'q = filter q by Type != "";\n'
            + "q = group q by (StageName, Type);\n"
            + "q = foreach q generate StageName, Type, avg(DaysInStage) as avg_days;\n"
            + "q = order q by StageName asc;"
        ),
        # ═══ V2 Phase 6: Bullet Chart ═══
        "s_bullet_approval": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + 'q = filter q by StageName in ["3 - Propose", "4 - Negotiate", "5 - Close"];\n'
            + 'q = foreach q generate (case when CommercialApproval == "true" then 1 else 0 end) as approved;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(approved) / count()) * 100 as approval_rate, 100 as target;"
        ),
        # ═══ V2 Phase 8: Statistical Analysis ═══
        "s_stat_velocity_zscore": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, avg(DaysInStage) as stage_avg, "
            + "stddev(DaysInStage) as stage_std, max(DaysInStage) as max_days, count() as cnt;\n"
            + "q = foreach q generate StageName, stage_avg, stage_std, max_days, cnt, "
            + "(case when stage_std > 0 then (max_days - stage_avg) / stage_std else 0 end) as z_score;\n"
            + "q = order q by z_score desc;"
        ),
        # ═══ Deal Slippage Tracker ═══
        # Slippage distribution by days past close date
        "s_slip_dist": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + PAST_DUE
            + "q = foreach q generate "
            + "(case "
            + 'when CloseDate >= "2026-02-01" then "a_1-30d past" '
            + 'when CloseDate >= "2026-01-02" then "b_31-60d past" '
            + 'when CloseDate >= "2025-12-03" then "c_61-90d past" '
            + 'else "d_90d+ past" end) as SlipBand, ARR;\n'
            + "q = group q by SlipBand;\n"
            + "q = foreach q generate SlipBand, count() as deal_count, sum(ARR) as slip_arr;\n"
            + "q = order q by SlipBand asc;"
        ),
        # Slippage by owner (who has the most slipped deals)
        "s_slip_by_owner": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + PAST_DUE
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, count() as slip_count, sum(ARR) as slip_arr;\n"
            + "q = order q by slip_arr desc;\n"
            + "q = limit q 20;"
        ),
        # ═══ V2 Phase 10: Sankey ═══
        "s_sankey_stage_fc": sq(
            L
            + FY
            + CLOSED
            + UF
            + RF
            + TF
            + "q = group q by (StageName, ForecastCategory);\n"
            + "q = foreach q generate StageName, ForecastCategory, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══ V2 Phase 10: Treemap ═══
        "s_treemap_arr": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + "q = group q by (UnitGroup, Type);\n"
            + "q = foreach q generate UnitGroup, Type, sum(ARR) as total_arr;\n"
            + "q = order q by total_arr desc;"
        ),
        # ═══ V2 Gap Fill: area_chart + percentile_disc + running total ═══
        # Running total: cumulative stuck deal count by CloseMonth
        "s_running_stuck": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + PAST_DUE
            + "q = group q by CloseMonth;\n"
            + "q = foreach q generate CloseMonth, count() as monthly_stuck;\n"
            + "q = order q by CloseMonth asc;\n"
            + "q = foreach q generate CloseMonth, monthly_stuck, "
            + "sum(monthly_stuck) over (order by CloseMonth "
            + "rows unbounded preceding) as cumul_stuck;"
        ),
        # Percentile distribution of DaysInStage
        "s_stat_stage_pctiles": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, count() as cnt, "
            + "avg(DaysInStage) as avg_days, "
            + "stddev(DaysInStage) as std_days, "
            + "percentile_disc(0.25) within group "
            + "(order by DaysInStage) as p25_days, "
            + "percentile_disc(0.50) within group "
            + "(order by DaysInStage) as p50_days, "
            + "percentile_disc(0.75) within group "
            + "(order by DaysInStage) as p75_days;\n"
            + "q = order q by StageName asc;"
        ),
        # ═══ V3 VIZ UPGRADE ═══
        # Compliance scorecard: stuck_rate, avg_days_in_stage per stage
        "s_compliance_scorecard": compliance_scorecard_step(
            DS, base_filters=FY + OPEN + UF + RF + TF,
        ),
        # KPI thresholds for dynamic coloring
        "s_sc_kpi_thresh": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + SGF
            + "q = foreach q generate "
            + "(case when DaysInStage > 30 then 1 else 0 end) as is_stuck, "
            + f'(case when CloseDate >= "{TODAY}" then 1 else 0 end) as is_future;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(sum(is_stuck) / count()) * 100 as stuck_rate, "
            + "(sum(is_future) / count()) * 100 as hygiene_pct;"
        ),
        # Bottleneck distribution: DaysInStage distribution per stage (5 time bands)
        "s_bottleneck_dist": sq(
            L
            + FY
            + OPEN
            + UF
            + RF
            + TF
            + "q = foreach q generate StageName, "
            + '(case when DaysInStage <= 7 then "0-7d" '
            + 'when DaysInStage <= 14 then "8-14d" '
            + 'when DaysInStage <= 30 then "15-30d" '
            + 'when DaysInStage <= 60 then "31-60d" '
            + 'else "60d+" end) as TimeBand;\n'
            + "q = group q by (StageName, TimeBand);\n"
            + "q = foreach q generate StageName, TimeBand, count() as cnt;\n"
            + "q = order q by StageName asc;"
        ),
        # ═══ PAGE 8: Forecast Integrity (ML-Forward) ═══
        # Forecast integrity: ForecastCategory vs WinScore misalignment
        "s_integrity_by_rep": forecast_integrity_step(DS, UF + TF + RF),
        # Integrity heatmap: rep × category
        "s_integrity_heatmap": forecast_integrity_heatmap_step(DS, UF + RF),
        # Forecast integrity KPIs
        "s_integrity_kpi": sq(
            L
            + UF
            + RF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(sum(case when "
            + '(ForecastCategory == "Commit" && WinScore < 40) || '
            + '(ForecastCategory == "Pipeline" && WinScore > 70) '
            + "then 1 else 0 end) * 100 / count()) as mismatch_pct, "
            + "sum(case when ForecastCategory == \"Commit\" && WinScore < 40 then ARR else 0 end) as risky_commit_arr, "
            + "count() as total_open;"
        ),
        # Past-due deals (dynamic date)
        "s_pastdue_dynamic": sq(
            L
            + UF
            + RF
            + 'q = filter q by IsClosed == "false" && CloseDate < "' + TODAY + '";\n'
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "count() as pastdue_count, "
            + "sum(ARR) as pastdue_arr;\n"
            + "q = order q by pastdue_arr desc;\n"
            + "q = limit q 20;"
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Widgets
# ═══════════════════════════════════════════════════════════════════════════


def build_widgets():
    w = {
        # ══════════════════════════════════════════════════════════════════
        #  PAGE 1: Stage Bottlenecks
        # ══════════════════════════════════════════════════════════════════
        "p1_nav1": nav_link("bottlenecks", "Bottlenecks", active=True),
        "p1_nav2": nav_link("stuck", "Stuck Deals"),
        "p1_nav3": nav_link("velocity", "Velocity"),
        "p1_nav4": nav_link("closedates", "Close Dates"),
        "p1_nav5": nav_link("winloss", "Win/Loss"),
        "p1_hdr": hdr(
            "Stage Bottlenecks",
            "FY2026 | Identifying pipeline friction points | Stuck = DaysInStage > 30",
        ),
        # Filter bar
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_region": pillbox("f_region", "Region"),
        "p1_f_type": pillbox("f_type", "Type"),
        "p1_f_stage": pillbox("f_stage", "Stage"),
        # KPI tiles — with YoY trend (Phase 3)
        "p1_stuck_cnt": num_with_trend(
            "s_stuck_cnt_t",
            "cnt",
            "Stage 3/4 Stuck Count",
            "#D4504C",
        ),
        "p1_stuck_arr": num_with_trend(
            "s_stuck_arr_t",
            "sum_acv",
            "Stage 3/4 Stuck ARR (EUR)",
            "#D4504C",
            compact=True,
            size=28,
        ),
        # Stackhbar: Stage aging distribution
        "p1_stg_aging": rich_chart(
            "s_stg_aging",
            "stackhbar",
            "Stage Aging Distribution",
            ["StageName"],
            ["cnt"],
            split=["AgeBand"],
            show_legend=True,
            axis_title="Opportunity Count",
        ),
        # Section label
        "p1_sec_stuck": section_label("Stuck Deal Inventory (DaysInStage > 30)"),
        # Comparisontable: Stuck deals
        "p1_stuck_table": rich_chart(
            "s_stuck_list",
            "comparisontable",
            "Stuck Deals - Top 25 by ARR",
            ["Name", "AccountName", "StageName", "OwnerName"],
            ["DaysInStage", "ARR"],
        ),
        # Waterfall: Stage dropoff rate
        "p1_waterfall": waterfall_chart(
            "s_stg_waterfall",
            "Stage Dropoff (Opp Count by Stage)",
            "StageName",
            "cnt",
            axis_label="Opportunity Count",
        ),
        # ══════════════════════════════════════════════════════════════════
        #  PAGE 2: Stuck & Past-Due
        # ══════════════════════════════════════════════════════════════════
        "p2_nav1": nav_link("bottlenecks", "Bottlenecks"),
        "p2_nav2": nav_link("stuck", "Stuck Deals", active=True),
        "p2_nav3": nav_link("velocity", "Velocity"),
        "p2_nav4": nav_link("closedates", "Close Dates"),
        "p2_nav5": nav_link("winloss", "Win/Loss"),
        "p2_hdr": hdr(
            "Stuck & Past-Due Opportunities",
            f"FY2026 | Past-due = CloseDate < {TODAY} and still open",
        ),
        # Filter bar
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_f_type": pillbox("f_type", "Type"),
        "p2_f_stage": pillbox("f_stage", "Stage"),
        # KPI tiles — with YoY trend (Phase 3)
        "p2_pastdue_cnt": num_with_trend(
            "s_pastdue_cnt_t",
            "cnt",
            "Past-Due Opp Count",
            "#D4504C",
        ),
        "p2_pastdue_arr": num(
            "s_pastdue_arr",
            "sum_acv",
            "Past-Due ARR (EUR)",
            "#D4504C",
            True,
            28,
        ),
        # Gauge: Pipeline hygiene score
        "p2_hygiene": gauge(
            "s_hygiene",
            "hygiene_pct",
            "Pipeline Hygiene Score",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 70, "color": "#D4504C"},
                {"start": 70, "stop": 90, "color": "#FFB75D"},
                {"start": 90, "stop": 100, "color": "#04844B"},
            ],
        ),
        # Hbar: Past-due by owner
        "p2_pastdue_owner": rich_chart(
            "s_pastdue_owner",
            "hbar",
            "Past-Due Opps by Owner",
            ["OwnerName"],
            ["cnt"],
            axis_title="Count",
        ),
        # Section label
        "p2_sec_stuck": section_label("Stuck Opportunities (DaysInStage > 30)"),
        # Comparisontable: Stuck ops detail
        "p2_stuck_table": rich_chart(
            "s_stuck_detail",
            "comparisontable",
            "Stuck Opportunities - Top 25 by ARR",
            ["Name", "StageName", "OwnerName"],
            ["DaysInStage", "ARR"],
        ),
        # ══════════════════════════════════════════════════════════════════
        #  PAGE 3: Velocity by Type
        # ══════════════════════════════════════════════════════════════════
        "p3_nav1": nav_link("bottlenecks", "Bottlenecks"),
        "p3_nav2": nav_link("stuck", "Stuck Deals"),
        "p3_nav3": nav_link("velocity", "Velocity", active=True),
        "p3_nav4": nav_link("closedates", "Close Dates"),
        "p3_nav5": nav_link("winloss", "Win/Loss"),
        "p3_hdr": hdr(
            "Sales Velocity by Opportunity Type",
            "FY2026 | Won deals | Lower cycle time = better",
        ),
        # Filter bar
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_region": pillbox("f_region", "Region"),
        "p3_f_type": pillbox("f_type", "Type"),
        "p3_f_stage": pillbox("f_stage", "Stage"),
        # Gauge x3: Land, Expand, Renewal velocity
        # Reversed bands: <60d green, 60-120d amber, >120d red
        "p3_vel_land": gauge(
            "s_vel_land",
            "avg_cycle",
            "Land Velocity (Days)",
            min_val=0,
            max_val=200,
            bands=[
                {"start": 0, "stop": 60, "color": "#04844B"},
                {"start": 60, "stop": 120, "color": "#FFB75D"},
                {"start": 120, "stop": 200, "color": "#D4504C"},
            ],
        ),
        "p3_vel_expand": gauge(
            "s_vel_expand",
            "avg_cycle",
            "Expand Velocity (Days)",
            min_val=0,
            max_val=200,
            bands=[
                {"start": 0, "stop": 60, "color": "#04844B"},
                {"start": 60, "stop": 120, "color": "#FFB75D"},
                {"start": 120, "stop": 200, "color": "#D4504C"},
            ],
        ),
        "p3_vel_renewal": gauge(
            "s_vel_renewal",
            "avg_cycle",
            "Renewal Velocity (Days)",
            min_val=0,
            max_val=200,
            bands=[
                {"start": 0, "stop": 60, "color": "#04844B"},
                {"start": 60, "stop": 120, "color": "#FFB75D"},
                {"start": 120, "stop": 200, "color": "#D4504C"},
            ],
        ),
        # Hbar: Cycle time comparison
        "p3_sec_compare": section_label("Cycle Time Comparison"),
        "p3_vel_type": rich_chart(
            "s_vel_type",
            "hbar",
            "Avg Sales Cycle by Type",
            ["Type"],
            ["avg_cycle"],
            axis_title="Days",
        ),
        # Combo: Monthly velocity trend
        "p3_vel_monthly": rich_chart(
            "s_vel_monthly",
            "combo",
            "Monthly Velocity Trend (Won Deals)",
            ["CloseMonth"],
            ["deal_count", "avg_cycle"],
            show_legend=True,
            axis_title="Count / Days",
            combo_config={
                "plotConfiguration": [
                    {"series": "deal_count", "chartType": "column"},
                    {"series": "avg_cycle", "chartType": "line"},
                ],
            },
        ),
        # Velocity trend & stage conversion (new analytics)
        "p3_sec_trend": section_label("Velocity Trends & Stage Conversion"),
        "p3_ch_vel_trend": rich_chart(
            "s_velocity_trend",
            "column",
            "Avg Days in Stage by Quarter",
            ["Quarter"],
            ["avg_days"],
            split=["StageName"],
            show_legend=True,
            axis_title="Days",
        ),
        "p3_ch_stage_conv": rich_chart(
            "s_stage_conversion",
            "hbar",
            "Stage Conversion Rates",
            ["StageName"],
            ["conv_rate"],
            axis_title="Win Rate %",
        ),
        # ══════════════════════════════════════════════════════════════════
        #  PAGE 4: Close Date Hygiene
        # ══════════════════════════════════════════════════════════════════
        "p4_nav1": nav_link("bottlenecks", "Bottlenecks"),
        "p4_nav2": nav_link("stuck", "Stuck Deals"),
        "p4_nav3": nav_link("velocity", "Velocity"),
        "p4_nav4": nav_link("closedates", "Close Dates", active=True),
        "p4_nav5": nav_link("winloss", "Win/Loss"),
        "p4_hdr": hdr(
            "Close Date Hygiene",
            f"FY2026 | Tracking close date accuracy | Today = {TODAY}",
        ),
        # Filter bar
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_region": pillbox("f_region", "Region"),
        "p4_f_type": pillbox("f_type", "Type"),
        "p4_f_stage": pillbox("f_stage", "Stage"),
        # Donut: Close date distribution
        "p4_close_dist": rich_chart(
            "s_close_dist",
            "donut",
            "Close Date Distribution",
            ["CloseBucket"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        # Number: Pushed-out count
        "p4_pushed_cnt": num(
            "s_pushed_cnt",
            "cnt",
            "Pushed-Out Deals (Past Close Date)",
            "#FF6600",
        ),
        "p4_pushed_arr": num(
            "s_pushed_cnt",
            "sum_acv",
            "Pushed-Out ARR (EUR)",
            "#FF6600",
            True,
            28,
        ),
        # Section: Monthly view
        "p4_sec_monthly": section_label("Monthly Close Date Distribution"),
        # Waterfall: Monthly close date movements
        "p4_waterfall": waterfall_chart(
            "s_close_monthly",
            "Monthly Close Date Movements (Opp Count)",
            "CloseMonth",
            "cnt",
            axis_label="Opportunity Count",
        ),
        # Section: Detail
        "p4_sec_detail": section_label("Past Close Date Opportunities"),
        # Comparisontable: Past close date opps
        "p4_pastdue_table": rich_chart(
            "s_pastdue_list",
            "comparisontable",
            "Past Close Date Deals - Top 25 by ARR",
            ["Name", "AccountName", "OwnerName", "StageName", "CloseDate"],
            ["ARR"],
        ),
        # ══════════════════════════════════════════════════════════════════
        #  FORECAST CATEGORY COMPLIANCE (AP 1.4) — on Page 4
        # ══════════════════════════════════════════════════════════════════
        "p4_sec_fcat": section_label("Forecast Category Compliance"),
        "p4_fcat_donut": rich_chart(
            "s_fcat_dist",
            "donut",
            "Open Pipeline by Forecast Category",
            ["ForecastCategory"],
            ["sum_acv"],
            show_legend=True,
            show_pct=True,
        ),
        "p4_fcat_trend": rich_chart(
            "s_fcat_trend",
            "stackarea",
            "Forecast Category Mix Over Time",
            ["CloseMonth"],
            ["sum_acv"],
            split=["ForecastCategory"],
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        # ══════════════════════════════════════════════════════════════════
        #  PER-STAGE SLA (AP 1.4) — on Page 3
        # ══════════════════════════════════════════════════════════════════
        "p3_sec_sla": section_label("Stage Transition SLA"),
        "p3_stage_sla": rich_chart(
            "s_stage_sla_rows",
            "hbar",
            "Avg Days per Stage Transition",
            ["StageTransition"],
            ["avg_days"],
            axis_title="Days",
        ),
        # ══════════════════════════════════════════════════════════════════
        #  WIN SCORING (Phase 2) — on Page 1
        # ══════════════════════════════════════════════════════════════════
        "p1_sec_ws": section_label("Win Probability Scoring"),
        "p1_ws_gauge": gauge(
            "s_ws_avg",
            "avg_score",
            "Avg Win Score",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 40, "color": "#D4504C"},
                {"start": 40, "stop": 70, "color": "#FFB75D"},
                {"start": 70, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p1_ws_donut": rich_chart(
            "s_ws_band",
            "donut",
            "Pipeline by Win Score Band",
            ["WinScoreBand"],
            ["sum_acv"],
            show_legend=True,
            show_pct=True,
        ),
        "p1_ws_top25": rich_chart(
            "s_ws_top25",
            "comparisontable",
            "Top 25 Deals by Win Score",
            ["Name", "AccountName", "StageName", "OwnerName", "WinScoreBand"],
            ["WinScore", "ARR", "DaysInStage"],
        ),
        "p1_ws_stage": rich_chart(
            "s_ws_stage",
            "hbar",
            "Avg Win Score by Stage",
            ["StageName"],
            ["avg_score"],
            axis_title="Win Score",
        ),
    }

    # ═══ Deal Slippage Tracker (Page 4 extension) ═══
    w["p4_sec_slip"] = section_label("Deal Slippage Tracker")
    w["p4_ch_slip_dist"] = rich_chart(
        "s_slip_dist",
        "column",
        "Past-Due Deals by Slippage Duration",
        ["SlipBand"],
        ["deal_count", "slip_arr"],
        show_legend=True,
        axis_title="Count / ARR",
    )
    w["p4_ch_slip_owner"] = rich_chart(
        "s_slip_by_owner",
        "hbar",
        "Slippage by Owner (Top 20 by At-Risk ARR)",
        ["OwnerName"],
        ["slip_arr", "slip_count"],
        show_legend=True,
        axis_title="ARR (EUR) / Count",
    )

    # ═══ ITERATION 3: Stuck Owner Leaderboard (Page 2) + Win/Loss Analysis (Page 5) ═══
    # Page 2: Stuck owner leaderboard
    w["p2_sec_owner_stuck"] = section_label("Stuck Deal Leaderboard by Owner")
    w["p2_ch_stuck_owner"] = rich_chart(
        "s_stuck_by_owner",
        "comparisontable",
        "Owner Stuck Leaderboard",
        ["OwnerName"],
        ["stuck_cnt", "stuck_arr", "avg_days_stuck"],
    )
    # Page 5 (NEW): Win/Loss Reason Analysis (Additive CRO #3)
    w["p5_nav1"] = nav_link("bottlenecks", "Bottlenecks")
    w["p5_nav2"] = nav_link("stuck", "Stuck Deals")
    w["p5_nav3"] = nav_link("velocity", "Velocity")
    w["p5_nav4"] = nav_link("closedates", "Close Dates")
    w["p5_nav5"] = nav_link("winloss", "Win/Loss", active=True)
    w["p5_hdr"] = hdr(
        "Win/Loss Reason Analysis",
        "FY2026 | Understanding why deals are won and lost",
    )
    w["p5_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p5_f_region"] = pillbox("f_region", "Region")
    w["p5_f_type"] = pillbox("f_type", "Type")
    w["p5_f_stage"] = pillbox("f_stage", "Stage")
    w["p5_sec_loss"] = section_label("Loss Reason Analysis")
    w["p5_ch_loss_donut"] = rich_chart(
        "s_loss_reasons",
        "donut",
        "Loss Reasons (Count)",
        ["WonLostReason"],
        ["cnt"],
        show_legend=True,
        show_pct=True,
    )
    w["p5_ch_loss_arr"] = rich_chart(
        "s_loss_reasons",
        "hbar",
        "Lost ARR by Reason",
        ["WonLostReason"],
        ["lost_arr"],
        axis_title="ARR (EUR)",
    )
    w["p5_ch_loss_type"] = rich_chart(
        "s_loss_by_type",
        "stackhbar",
        "Loss Reasons by Deal Type",
        ["Type"],
        ["cnt"],
        split=["WonLostReason"],
        show_legend=True,
        axis_title="Count",
    )
    w["p5_sec_win"] = section_label("Win Reason Analysis")
    w["p5_ch_win_reasons"] = rich_chart(
        "s_win_reasons",
        "hbar",
        "Win Reasons by ARR",
        ["WonLostReason"],
        ["won_arr"],
        axis_title="ARR (EUR)",
    )
    w["p5_sec_detail"] = section_label("Lost Deal Detail")
    w["p5_ch_lost_detail"] = rich_chart(
        "s_lost_detail",
        "comparisontable",
        "Top 25 Lost Deals by ARR",
        ["Name", "AccountName", "OwnerName", "Type", "WonLostReason", "SubReason"],
        ["ARR", "SalesCycleDuration"],
    )

    # ── Phase 6: Reference lines ──────────────────────────────────────────
    from crm_analytics_helpers import add_reference_line

    add_reference_line(w["p3_vel_type"], 60, "60-Day Target", "#D4504C", "dashed")
    add_reference_line(w["p3_vel_monthly"], 60, "60-Day Target", "#D4504C", "dashed")

    # ── Phase 7: Embedded table actions ──────────────────────────────────
    add_table_action(w["p1_stuck_table"], "salesforceActions", "Opportunity", "Id")
    add_table_action(w["p2_stuck_table"], "salesforceActions", "Opportunity", "Id")
    add_table_action(w["p4_pastdue_table"], "salesforceActions", "Opportunity", "Id")

    # ═══ V2 PAGE 6: Advanced Analytics ═══
    w["p6_nav1"] = nav_link("bottlenecks", "Bottlenecks")
    w["p6_nav2"] = nav_link("stuck", "Stuck & Past-Due")
    w["p6_nav3"] = nav_link("velocity", "Velocity")
    w["p6_nav4"] = nav_link("closedate", "Close Date")
    w["p6_nav5"] = nav_link("wonlost", "Win/Loss")
    w["p6_nav6"] = nav_link("advanalytics", "Advanced", active=True)
    w["p6_hdr"] = hdr(
        "Advanced Analytics",
        "Rep Performance | Pipeline Heatmap | Stage Velocity Matrix",
    )
    w["p6_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p6_f_region"] = pillbox("f_region", "Region")
    w["p6_f_type"] = pillbox("f_type", "Type")
    w["p6_f_stage"] = pillbox("f_stage", "Stage")
    # Heatmap: Owner × Month pipeline
    w["p6_sec_heatmap"] = section_label("Rep Pipeline Consistency")
    w["p6_ch_heatmap"] = heatmap_chart(
        "s_heatmap_owner_month", "Pipeline ARR by Rep × Month"
    )
    # Bubble: Rep performance
    w["p6_sec_bubble"] = section_label("Rep Performance Quadrant")
    w["p6_ch_bubble"] = bubble_chart(
        "s_bubble_rep", "Reps: Pipeline × Win Rate (size = Deal Count)"
    )
    # Heatmap: Stage × Type days in stage
    w["p6_sec_heatmap2"] = section_label("Stage Velocity Matrix")
    w["p6_ch_heatmap2"] = heatmap_chart(
        "s_heatmap_stage_type", "Avg Days in Stage by Stage × Type"
    )
    # Sankey: Stage → Forecast Category
    w["p6_sec_sankey"] = section_label("Stage → Forecast Category Flow")
    w["p6_ch_sankey"] = sankey_chart(
        "s_sankey_stage_fc", "Deal Flow: Stage → Forecast Category"
    )
    # Treemap: Pipeline ARR composition
    w["p6_sec_treemap"] = section_label("Pipeline ARR Composition")
    w["p6_ch_treemap"] = treemap_chart(
        "s_treemap_arr",
        "Open Pipeline by Unit Group & Type",
        ["UnitGroup", "Type"],
        "total_arr",
    )

    # ═══ V2 PAGE 7: Bullet Charts & Statistical Analysis ═══
    w["p7_nav1"] = nav_link("bottlenecks", "Bottlenecks")
    w["p7_nav2"] = nav_link("stuck", "Stuck & Past-Due")
    w["p7_nav3"] = nav_link("velocity", "Velocity")
    w["p7_nav4"] = nav_link("closedate", "Close Date")
    w["p7_nav5"] = nav_link("wonlost", "Win/Loss")
    w["p7_nav6"] = nav_link("advanalytics", "Advanced")
    w["p7_nav7"] = nav_link("compstats", "Statistics", active=True)
    w["p7_hdr"] = hdr(
        "Compliance Statistical Analysis",
        "Approval Target | Velocity Anomalies | Rep Percentile Ranking",
    )
    w["p7_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p7_f_region"] = pillbox("f_region", "Region")
    w["p7_f_type"] = pillbox("f_type", "Type")
    w["p7_f_stage"] = pillbox("f_stage", "Stage")
    # Bullet: Approval rate
    w["p7_sec_bullet"] = section_label("Approval Rate Target")
    w["p7_bullet_approval"] = bullet_chart(
        "s_bullet_approval", "Commercial Approval Rate (Target: 100%)", axis_title="%"
    )
    # Stats: Z-score anomaly detection (deals much slower than stage average)
    w["p7_sec_zscore"] = section_label("Velocity Anomalies by Stage")
    w["p7_stat_zscore"] = rich_chart(
        "s_stat_velocity_zscore",
        "comparisontable",
        "Stage Duration Z-Score Analysis",
        ["StageName"],
        ["stage_avg", "stage_std", "max_days", "z_score", "cnt"],
    )
    # Area: Cumulative stuck deals over time
    w["p7_sec_running"] = section_label("Cumulative Stuck Deals Over Time")
    w["p7_ch_running"] = area_chart(
        "s_running_stuck",
        "Cumulative Past-Due Deals by Month",
        axis_title="Deals",
    )
    # Percentile distribution of DaysInStage
    w["p7_sec_pctiles"] = section_label("Stage Duration Percentile Distribution")
    w["p7_tbl_pctiles"] = rich_chart(
        "s_stat_stage_pctiles",
        "comparisontable",
        "Days-in-Stage Percentiles by Stage",
        ["StageName"],
        ["cnt", "avg_days", "std_days", "p25_days", "p50_days", "p75_days"],
    )

    # Add nav6 (Advanced) to pages 1-5
    for px in range(1, 6):
        w[f"p{px}_nav6"] = nav_link("advanalytics", "Advanced")
    # Add nav7 (Statistics) to pages 1-6
    for px in range(1, 7):
        w[f"p{px}_nav7"] = nav_link("compstats", "Statistics")

    # ═══ V3 VIZ UPGRADE ═══
    # Dynamic KPI tiles on Page 1
    w["p1_stuck_rate_dynamic"] = num_dynamic_color(
        "s_sc_kpi_thresh",
        "stuck_rate",
        "Stuck Rate %",
        [(10, "#04844B"), (25, "#FFB75D"), (100, "#D4504C")],
        size=28,
    )
    w["p1_hygiene_dynamic"] = num_dynamic_color(
        "s_sc_kpi_thresh",
        "hygiene_pct",
        "Pipeline Hygiene %",
        [(70, "#D4504C"), (90, "#FFB75D"), (100, "#04844B")],
        size=28,
    )

    # Compliance scorecard on Page 1
    w["p1_sec_scorecard"] = section_label("Compliance Scorecard by Stage")
    w["p1_ch_scorecard"] = rich_chart(
        "s_compliance_scorecard",
        "comparisontable",
        "Stage Compliance: Stuck Rate & Avg Days",
        ["StageName"],
        ["stuck_rate", "avg_days_in_stage", "total_cnt"],
    )

    # Bottleneck distribution on Page 1
    w["p1_sec_bottleneck"] = section_label("Bottleneck Distribution")
    w["p1_ch_bottleneck"] = rich_chart(
        "s_bottleneck_dist",
        "stackcolumn",
        "Days-in-Stage Distribution by Stage",
        ["StageName"],
        ["cnt"],
        split=["TimeBand"],
        show_legend=True,
        axis_title="Opportunity Count",
    )

    # Table action on lost deal detail (p5)
    add_table_action(w["p5_ch_lost_detail"], "salesforceActions", "Opportunity", "Id")

    # Selection interaction: unit filter drives scorecard
    add_selection_interaction(
        w["p1_ch_scorecard"], "f_unit", "UnitGroup", ["s_compliance_scorecard"]
    )

    # ═══ PAGE 8: Forecast Integrity (ML-Forward) ═══
    w["p8_hdr"] = hdr(
        "Forecast Integrity & Hygiene",
        "ForecastCategory vs WinScore alignment — identify misclassified deals",
    )
    w["p8_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p8_f_region"] = pillbox("f_region", "Region")
    # Integrity KPIs
    w["p8_mismatch_kpi"] = num_dynamic_color(
        "s_integrity_kpi", "mismatch_pct", "Forecast Mismatch %",
        thresholds=[(5, "#04844B"), (15, "#FFB75D"), (100, "#D4504C")],
        size=28,
    )
    w["p8_risky_commit"] = num(
        "s_integrity_kpi", "risky_commit_arr", "Risky Commit ARR",
        "#D4504C", compact=True, size=28,
    )
    # Integrity heatmap
    w["p8_sec_heatmap"] = section_label("Forecast Integrity: Rep × Category Mismatch Rate")
    w["p8_ch_heatmap"] = heatmap_chart(
        "s_integrity_heatmap", "Mismatch Rate by Rep × Forecast Category"
    )
    # Integrity detail table
    w["p8_sec_detail"] = section_label("Rep Forecast Integrity Detail")
    w["p8_tbl_detail"] = rich_chart(
        "s_integrity_by_rep", "comparisontable",
        "Forecast Category vs Win Score Alignment",
        ["OwnerName", "ForecastCategory"],
        ["avg_win_score", "opp_count", "total_arr", "mismatch_pct"],
    )
    # Past-due with dynamic date
    w["p8_sec_pastdue"] = section_label("Past-Due Open Deals (Dynamic Date)")
    w["p8_tbl_pastdue"] = rich_chart(
        "s_pastdue_dynamic", "hbar",
        "Past-Due ARR by Rep",
        ["OwnerName"], ["pastdue_arr"],
        axis_title="Past-Due ARR (EUR)",
    )

    return w


# ═══════════════════════════════════════════════════════════════════════════
#  Layout
# ═══════════════════════════════════════════════════════════════════════════


def build_layout():
    # ── Page 1: Stage Bottlenecks ─────────────────────────────────────────
    p1 = nav_row("p1", 7) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_type", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_stage", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # KPI tiles
        {"name": "p1_stuck_cnt", "row": 5, "column": 0, "colspan": 6, "rowspan": 3},
        {"name": "p1_stuck_arr", "row": 5, "column": 6, "colspan": 6, "rowspan": 3},
        # Stackhbar: Stage aging (full width)
        {"name": "p1_stg_aging", "row": 8, "column": 0, "colspan": 12, "rowspan": 10},
        # Section + Table
        {"name": "p1_sec_stuck", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p1_stuck_table",
            "row": 19,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # Waterfall
        {"name": "p1_waterfall", "row": 29, "column": 0, "colspan": 12, "rowspan": 8},
        # Win Scoring section (Phase 2)
        {"name": "p1_sec_ws", "row": 37, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ws_gauge", "row": 38, "column": 0, "colspan": 3, "rowspan": 6},
        {"name": "p1_ws_donut", "row": 38, "column": 3, "colspan": 3, "rowspan": 6},
        {"name": "p1_ws_stage", "row": 38, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p1_ws_top25", "row": 44, "column": 0, "colspan": 12, "rowspan": 10},
        # V3: Dynamic KPI tiles
        {"name": "p1_stuck_rate_dynamic", "row": 54, "column": 0, "colspan": 6, "rowspan": 5},
        {"name": "p1_hygiene_dynamic", "row": 54, "column": 6, "colspan": 6, "rowspan": 5},
        # V3: Compliance scorecard
        {"name": "p1_sec_scorecard", "row": 59, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_scorecard", "row": 60, "column": 0, "colspan": 12, "rowspan": 8},
        # V3: Bottleneck distribution
        {"name": "p1_sec_bottleneck", "row": 68, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_bottleneck", "row": 69, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    # ── Page 2: Stuck & Past-Due ──────────────────────────────────────────
    p2 = nav_row("p2", 7) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_type", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_stage", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # KPI row: count, ARR, hygiene gauge
        {"name": "p2_pastdue_cnt", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p2_pastdue_arr", "row": 5, "column": 3, "colspan": 5, "rowspan": 4},
        {"name": "p2_hygiene", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        # Hbar: Past-due by owner
        {
            "name": "p2_pastdue_owner",
            "row": 9,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # Section + Table
        {"name": "p2_sec_stuck", "row": 19, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p2_stuck_table",
            "row": 20,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # Iteration 3: Stuck owner leaderboard
        {
            "name": "p2_sec_owner_stuck",
            "row": 30,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p2_ch_stuck_owner",
            "row": 31,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
    ]

    # ── Page 3: Velocity by Type ──────────────────────────────────────────
    p3 = nav_row("p3", 7) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_region", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_type", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_stage", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Gauge x3
        {"name": "p3_vel_land", "row": 5, "column": 0, "colspan": 4, "rowspan": 5},
        {"name": "p3_vel_expand", "row": 5, "column": 4, "colspan": 4, "rowspan": 5},
        {"name": "p3_vel_renewal", "row": 5, "column": 8, "colspan": 4, "rowspan": 5},
        # Section + Hbar
        {"name": "p3_sec_compare", "row": 10, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_vel_type", "row": 11, "column": 0, "colspan": 12, "rowspan": 8},
        # Combo: Monthly velocity trend
        {
            "name": "p3_vel_monthly",
            "row": 19,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # Velocity trend & stage conversion
        {"name": "p3_sec_trend", "row": 29, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_vel_trend", "row": 30, "column": 0, "colspan": 6, "rowspan": 8},
        {
            "name": "p3_ch_stage_conv",
            "row": 30,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        },
        # Stage Transition SLA
        {"name": "p3_sec_sla", "row": 39, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_stage_sla", "row": 40, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    # ── Page 4: Close Date Hygiene ────────────────────────────────────────
    p4 = nav_row("p4", 7) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_region", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_type", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_stage", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Donut + KPI tiles
        {"name": "p4_close_dist", "row": 5, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p4_pushed_cnt", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p4_pushed_arr", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        # Section + Waterfall
        {"name": "p4_sec_monthly", "row": 13, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_waterfall", "row": 14, "column": 0, "colspan": 12, "rowspan": 8},
        # Section + Table
        {"name": "p4_sec_detail", "row": 22, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p4_pastdue_table",
            "row": 23,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # Forecast Category Compliance
        {"name": "p4_sec_fcat", "row": 34, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_fcat_donut", "row": 35, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p4_fcat_trend", "row": 35, "column": 6, "colspan": 6, "rowspan": 8},
        # Deal Slippage Tracker
        {"name": "p4_sec_slip", "row": 43, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_slip_dist", "row": 44, "column": 0, "colspan": 6, "rowspan": 8},
        {
            "name": "p4_ch_slip_owner",
            "row": 44,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        },
    ]

    # ── Page 5: Win/Loss Reason Analysis (Additive CRO #3) ────────────────
    p5 = nav_row("p5", 7) + [
        {"name": "p5_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p5_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_region", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_type", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_stage", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Loss reasons
        {"name": "p5_sec_loss", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_loss_donut", "row": 6, "column": 0, "colspan": 4, "rowspan": 8},
        {"name": "p5_ch_loss_arr", "row": 6, "column": 4, "colspan": 4, "rowspan": 8},
        {"name": "p5_ch_loss_type", "row": 6, "column": 8, "colspan": 4, "rowspan": 8},
        # Win reasons
        {"name": "p5_sec_win", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p5_ch_win_reasons",
            "row": 15,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Lost deal detail
        {"name": "p5_sec_detail", "row": 23, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p5_ch_lost_detail",
            "row": 24,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
    ]

    p6 = nav_row("p6", 7) + [
        {"name": "p6_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p6_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_region", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_type", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_stage", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Heatmap: Owner × Month pipeline
        {"name": "p6_sec_heatmap", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_heatmap", "row": 6, "column": 0, "colspan": 12, "rowspan": 10},
        # Bubble: Rep performance
        {"name": "p6_sec_bubble", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_bubble", "row": 17, "column": 0, "colspan": 12, "rowspan": 10},
        # Heatmap: Stage × Type
        {
            "name": "p6_sec_heatmap2",
            "row": 27,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p6_ch_heatmap2",
            "row": 28,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # Sankey: Stage → Forecast Category
        {"name": "p6_sec_sankey", "row": 38, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_sankey", "row": 39, "column": 0, "colspan": 12, "rowspan": 10},
        # Treemap: Pipeline ARR
        {"name": "p6_sec_treemap", "row": 49, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_treemap", "row": 50, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    p7 = nav_row("p7", 7) + [
        {"name": "p7_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p7_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_region", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_type", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_stage", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Bullet
        {"name": "p7_sec_bullet", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p7_bullet_approval",
            "row": 6,
            "column": 0,
            "colspan": 12,
            "rowspan": 5,
        },
        # Z-score anomaly table
        {"name": "p7_sec_zscore", "row": 11, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_stat_zscore", "row": 12, "column": 0, "colspan": 12, "rowspan": 8},
        # Cumulative stuck area chart (row 20)
        {"name": "p7_sec_running", "row": 20, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_running", "row": 21, "column": 0, "colspan": 12, "rowspan": 8},
        # Stage duration percentile table (row 29)
        {"name": "p7_sec_pctiles", "row": 29, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_tbl_pctiles", "row": 30, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    p8 = nav_row("p8", 8) + [
        {"name": "p8_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p8_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p8_f_region", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p8_mismatch_kpi", "row": 5, "column": 0, "colspan": 6, "rowspan": 4},
        {"name": "p8_risky_commit", "row": 5, "column": 6, "colspan": 6, "rowspan": 4},
        {"name": "p8_sec_heatmap", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p8_ch_heatmap", "row": 10, "column": 0, "colspan": 12, "rowspan": 10},
        {"name": "p8_sec_detail", "row": 20, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p8_tbl_detail", "row": 21, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p8_sec_pastdue", "row": 29, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p8_tbl_pastdue", "row": 30, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("bottlenecks", "Stage Bottlenecks", p1),
            pg("stuck", "Stuck & Past-Due", p2),
            pg("velocity", "Velocity by Type", p3),
            pg("closedates", "Close Date Hygiene", p4),
            pg("winloss", "Win/Loss Analysis", p5),
            pg("advanalytics", "Advanced Analytics", p6),
            pg("compstats", "Statistical Analysis", p7),
            pg("integrity", "Forecast Integrity", p8),
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main():
    if "--create-dataflow" in sys.argv:
        print(
            "Sales Compliance reuses Opp_Mgmt_KPIs — "
            "run build_dashboard.py --create-dataflow instead"
        )
        return

    print("=== Sales Process Compliance Dashboard (AP 1.4) ===")

    # 1. Authenticate
    instance_url, token = get_auth()
    print(f"  Authenticated to {instance_url}")

    # 2. No dataset upload — reuses Opp_Mgmt_KPIs
    print(f"  Using existing dataset: {DS} ({DS_ID})")

    # 3. Create or find dashboard
    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)

    # 4. Build state
    steps = build_steps()
    widgets = build_widgets()
    layout = build_layout()
    state = build_dashboard_state(steps, widgets, layout)

    # 5. Deploy
    print(f"\n=== Deploying {DASHBOARD_LABEL} ===")
    deploy_dashboard(instance_url, token, dashboard_id, state)


if __name__ == "__main__":
    main()
