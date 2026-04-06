#!/usr/bin/env python3
"""Build a source-truth executive revenue dashboard from live Opportunity data.

This bypasses the stale legacy revenue datasets and creates a new CRM Analytics
dataset directly from current/prior FY Opportunity rows.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from crm_analytics_helpers import (  # noqa: E402
    _date,
    _dim,
    _measure,
    _soql,
    af,
    build_dashboard_state,
    choropleth_chart,
    compare_table,
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    hdr,
    num,
    pg,
    pillbox,
    rich_chart,
    set_record_links_xmd,
    sq,
    upload_dataset,
    waterfall_chart,
)
from crm_analytics_runtime import builder_run  # noqa: E402,F401  # pyright: ignore[reportMissingImports]
from simcorp_fields import assert_org_schema  # noqa: E402,F401  # pyright: ignore[reportMissingImports]

logger = logging.getLogger(__name__)

DS = "Executive_Revenue_Source_Truth"
DS_LABEL = "Executive Revenue Source Truth"
GEO_DS = "Executive_Revenue_Source_Geo"
GEO_DS_LABEL = "Executive Revenue Source Geo"
DASHBOARD_LABEL = "Executive Revenue Source Truth"
TODAY = date.today()
CURRENT_FY = TODAY.year
PRIOR_FY = CURRENT_FY - 1
EXEC_QUEUE_MIN_ARR = 900000
COUNTRY_TRANSLATION_PATH = REPO_ROOT / "config" / "country_translation_table.json"
COUNTRY_TRANSLATIONS = json.loads(COUNTRY_TRANSLATION_PATH.read_text())
COUNTRY_TRANSLATION_INDEX = {row["source_country"]: row for row in COUNTRY_TRANSLATIONS}

SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, Account.BillingCountry, Owner.Name, "
    "Sales_Region__c, Account_Unit_Group__c, StageName, NextStep, FiscalYear, "
    "ForecastCategoryName, IsWon, IsClosed, CloseDate, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedForecastARR "
    "FROM Opportunity "
    f"WHERE FiscalYear IN ({PRIOR_FY}, {CURRENT_FY}) "
    "AND CloseDate != null "
    "AND (IsWon = true OR IsClosed = false)"
)


def _safe_float(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _risk_bucket(days_to_close: int, is_closed: bool) -> str:
    if is_closed:
        return "Closed"
    if days_to_close < 0:
        return "Past Due"
    if days_to_close <= 30:
        return "0-30 Days"
    if days_to_close <= 60:
        return "31-60 Days"
    if days_to_close <= 120:
        return "61-120 Days"
    return "120+ Days"


def _normalize_country(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "Unknown"
    row = COUNTRY_TRANSLATION_INDEX.get(raw)
    if row:
        return row.get("canonical_country") or raw
    return raw


def _map_country_label(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    row = COUNTRY_TRANSLATION_INDEX.get(raw)
    if row:
        if not row.get("map_supported", True):
            return ""
        return row.get("map_country_label") or row.get("canonical_country") or raw
    return raw


def _recommended_action(
    category: str, days_to_close: int, next_step: str, is_closed: bool
) -> str:
    if is_closed:
        return ""
    if days_to_close < 0:
        return "Past due: rephase close date or close out"
    if category == "Commit":
        return "Validate commit plan and exec sponsor"
    if category == "Best Case":
        return "Pull to commit or reset forecast"
    if category == "Pipeline":
        return "Qualification review and next-step hygiene"
    if category == "Omitted":
        return "Set forecast category or disqualify"
    if next_step:
        return "Pressure-test next step and owner plan"
    return "Define next step and owner plan"


def _priority_score(
    category: str, days_to_close: int, amount: float, is_closed: bool
) -> int:
    if is_closed:
        return 0

    if days_to_close < 0:
        urgency_points = 50
    elif days_to_close <= 30:
        urgency_points = 40
    elif days_to_close <= 60:
        urgency_points = 25
    elif days_to_close <= 120:
        urgency_points = 10
    else:
        urgency_points = 0

    category_points = {
        "Commit": 30,
        "Best Case": 20,
        "Pipeline": 10,
        "Omitted": 5,
    }.get(category, 0)
    arr_points = min(25, int(amount / 250000))
    return urgency_points + category_points + arr_points


def create_dataset(inst: str, tok: str) -> int:
    opps = _soql(inst, tok, SOQL)
    rows: list[dict[str, object]] = []
    geo_totals: dict[tuple[str, str], float] = {}

    for opp in opps:
        close_date_text = (opp.get("CloseDate") or "")[:10]
        if len(close_date_text) != 10:
            continue
        close_dt = datetime.strptime(close_date_text, "%Y-%m-%d").date()
        month_date = f"{CURRENT_FY:04d}-{close_dt.month:02d}-01"
        close_quarter = f"Q{((close_dt.month - 1) // 3) + 1}"
        fiscal_year = int(_safe_float(opp.get("FiscalYear")))
        amount = round(_safe_float(opp.get("ConvertedForecastARR")), 2)
        is_won = bool(opp.get("IsWon"))
        is_closed = bool(opp.get("IsClosed"))
        category = (opp.get("ForecastCategoryName") or "").strip() or "Omitted"
        next_step = (opp.get("NextStep") or "").strip()
        days_to_close = (close_dt - TODAY).days

        actual_closed = amount if fiscal_year == CURRENT_FY and is_won else 0.0
        open_commit = (
            amount
            if fiscal_year == CURRENT_FY and (not is_closed) and category == "Commit"
            else 0.0
        )
        open_best_case = (
            amount
            if fiscal_year == CURRENT_FY and (not is_closed) and category == "Best Case"
            else 0.0
        )
        open_pipeline = (
            amount
            if fiscal_year == CURRENT_FY and (not is_closed) and category == "Pipeline"
            else 0.0
        )
        open_omitted = (
            amount
            if fiscal_year == CURRENT_FY and (not is_closed) and category == "Omitted"
            else 0.0
        )
        prior_year_actual = amount if fiscal_year == PRIOR_FY and is_won else 0.0
        best_case_call = round(actual_closed + open_commit + open_best_case, 2)
        geo_country = _map_country_label(
            (opp.get("Account") or {}).get("BillingCountry") or ""
        )
        if fiscal_year == CURRENT_FY and geo_country:
            geo_key = (geo_country, close_quarter)
            geo_totals[geo_key] = round(
                geo_totals.get(geo_key, 0.0) + best_case_call, 2
            )

        rows.append(
            {
                "OpportunityId": opp.get("Id") or "",
                "OpportunityName": (opp.get("Name") or "")[:255],
                "AccountId": opp.get("AccountId") or "",
                "AccountName": ((opp.get("Account") or {}).get("Name") or "")[:255],
                "BillingCountry": (
                    ((opp.get("Account") or {}).get("BillingCountry")) or "Unknown"
                )[:255],
                "Country": geo_country[:255] or "Unknown",
                "OwnerName": ((opp.get("Owner") or {}).get("Name") or "Unknown")[:255],
                "SalesRegion": (opp.get("Sales_Region__c") or "Unassigned")[:255],
                "AccountUnitGroup": (opp.get("Account_Unit_Group__c") or "Unassigned")[
                    :255
                ],
                "StageName": (opp.get("StageName") or "")[:255],
                "NextStep": next_step[:255],
                "ForecastCategory": category[:255],
                "FiscalYear": fiscal_year,
                "CloseQuarter": close_quarter,
                "CloseDate": close_date_text,
                "MonthDate": month_date,
                "MonthLabel": month_date[:7],
                "DaysToClose": days_to_close,
                "RiskBucket": _risk_bucket(days_to_close, is_closed)[:255],
                "RecommendedAction": _recommended_action(
                    category, days_to_close, next_step, is_closed
                )[:255],
                "PriorityScore": _priority_score(
                    category, days_to_close, amount, is_closed
                ),
                "ARR": amount,
                "ActualClosedWonARR": round(actual_closed, 2),
                "OpenCommitARR": round(open_commit, 2),
                "OpenBestCaseARR": round(open_best_case, 2),
                "OpenPipelineARR": round(open_pipeline, 2),
                "OpenOmittedARR": round(open_omitted, 2),
                "CommitCallARR": round(actual_closed + open_commit, 2),
                "BestCaseCallARR": best_case_call,
                "PipelineCallARR": round(best_case_call + open_pipeline, 2),
                "PriorYearActualARR": round(prior_year_actual, 2),
                "YoY10TargetARR": round(prior_year_actual * 1.10, 2),
                "OpenOpportunityFlag": 1
                if (fiscal_year == CURRENT_FY and not is_closed)
                else 0,
            }
        )

    buffer = io.StringIO()
    fieldnames = [
        "OpportunityId",
        "OpportunityName",
        "AccountId",
        "AccountName",
        "BillingCountry",
        "Country",
        "OwnerName",
        "SalesRegion",
        "AccountUnitGroup",
        "StageName",
        "NextStep",
        "ForecastCategory",
        "FiscalYear",
        "CloseQuarter",
        "CloseDate",
        "MonthDate",
        "MonthLabel",
        "DaysToClose",
        "RiskBucket",
        "RecommendedAction",
        "PriorityScore",
        "ARR",
        "ActualClosedWonARR",
        "OpenCommitARR",
        "OpenBestCaseARR",
        "OpenPipelineARR",
        "OpenOmittedARR",
        "CommitCallARR",
        "BestCaseCallARR",
        "PipelineCallARR",
        "PriorYearActualARR",
        "YoY10TargetARR",
        "OpenOpportunityFlag",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)

    fields_meta = [
        _dim("OpportunityId", "Opportunity Id"),
        _dim("OpportunityName", "Opportunity"),
        _dim("AccountId", "Account Id"),
        _dim("AccountName", "Account"),
        _dim("BillingCountry", "Billing Country"),
        _dim("Country", "Country"),
        _dim("OwnerName", "Owner"),
        _dim("SalesRegion", "Sales Region"),
        _dim("AccountUnitGroup", "Account Unit Group"),
        _dim("StageName", "Stage"),
        _dim("NextStep", "Next Step"),
        _dim("ForecastCategory", "Forecast Category"),
        _measure("FiscalYear", "Fiscal Year", scale=0, precision=4),
        _dim("CloseQuarter", "Close Quarter"),
        _date("CloseDate", "Close Date"),
        _date("MonthDate", "Month"),
        _dim("MonthLabel", "Month Label"),
        _measure("DaysToClose", "Days To Close", scale=0, precision=5),
        _dim("RiskBucket", "Risk Bucket"),
        _dim("RecommendedAction", "Recommended Action"),
        _measure("PriorityScore", "Priority Score", scale=0, precision=3),
        _measure("ARR", "ARR"),
        _measure("ActualClosedWonARR", "Actual Closed Won ARR"),
        _measure("OpenCommitARR", "Open Commit ARR"),
        _measure("OpenBestCaseARR", "Open Best Case ARR"),
        _measure("OpenPipelineARR", "Open Pipeline ARR"),
        _measure("OpenOmittedARR", "Open Omitted ARR"),
        _measure("CommitCallARR", "Commit Call ARR"),
        _measure("BestCaseCallARR", "Best Case Call ARR"),
        _measure("PipelineCallARR", "Pipeline Call ARR"),
        _measure("PriorYearActualARR", "Prior Year Actual ARR"),
        _measure("YoY10TargetARR", "10% YoY Target ARR"),
        _measure("OpenOpportunityFlag", "Open Opportunity Flag", scale=0, precision=1),
    ]

    if not upload_dataset(
        inst,
        tok,
        DS,
        DS_LABEL,
        fields_meta,
        buffer.getvalue().encode("utf-8"),
    ):
        raise RuntimeError("Dataset upload failed")

    geo_buffer = io.StringIO()
    geo_writer = csv.DictWriter(
        geo_buffer,
        fieldnames=["Country", "CloseQuarter", "BestCaseCallARR"],
        lineterminator="\n",
    )
    geo_writer.writeheader()
    for (country, close_quarter), value in sorted(geo_totals.items()):
        geo_writer.writerow(
            {
                "Country": country,
                "CloseQuarter": close_quarter,
                "BestCaseCallARR": round(value, 2),
            }
        )

    geo_fields_meta = [
        _dim("Country", "Country"),
        _dim("CloseQuarter", "Close Quarter"),
        _measure("BestCaseCallARR", "Best Case Call ARR"),
    ]
    if not upload_dataset(
        inst,
        tok,
        GEO_DS,
        GEO_DS_LABEL,
        geo_fields_meta,
        geo_buffer.getvalue().encode("utf-8"),
    ):
        raise RuntimeError("Geo dataset upload failed")

    return len(rows)


def build_steps(ds_id: str) -> dict[str, dict]:
    ds_meta = [{"id": ds_id, "name": DS}]
    quarter_filter = (
        "q = filter q by "
        '{{coalesce(column(f_quarter.selection, ["CloseQuarter"]), '
        "column(f_quarter.result, [\"CloseQuarter\"])).asEquality('CloseQuarter')}};\n"
    )

    return {
        "f_region": af("SalesRegion", ds_meta, select_mode="multi"),
        "f_unit": af("AccountUnitGroup", ds_meta, select_mode="multi"),
        "f_quarter": af("CloseQuarter", ds_meta, select_mode="single"),
        "s_kpi_actual": sq(
            f'q = load "{DS}";\n'
            "q = group q by all;\n"
            "q = foreach q generate sum(ActualClosedWonARR) as ActualClosedWonARR;\n"
        ),
        "s_kpi_commit": sq(
            f'q = load "{DS}";\n'
            "q = group q by all;\n"
            "q = foreach q generate sum(CommitCallARR) as CommitCallARR;\n"
        ),
        "s_kpi_best": sq(
            f'q = load "{DS}";\n'
            "q = group q by all;\n"
            "q = foreach q generate sum(BestCaseCallARR) as BestCaseCallARR;\n"
        ),
        "s_kpi_pipeline": sq(
            f'q = load "{DS}";\n'
            "q = group q by all;\n"
            "q = foreach q generate sum(PipelineCallARR) as PipelineCallARR;\n"
        ),
        "s_kpi_target": sq(
            f'q = load "{DS}";\n'
            "q = group q by all;\n"
            "q = foreach q generate sum(YoY10TargetARR) as YoY10TargetARR;\n"
        ),
        "s_kpi_needed": sq(
            f'q = load "{DS}";\n'
            "q = group q by all;\n"
            "q = foreach q generate "
            "(case when (sum(YoY10TargetARR) - sum(BestCaseCallARR)) > 0 "
            "then (sum(YoY10TargetARR) - sum(BestCaseCallARR)) else 0 end) "
            "as NeededFromPipelineARR;\n"
        ),
        "s_call_ladder": sq(
            f'q = load "{DS}";\n'
            "q = group q by (MonthDate, MonthLabel);\n"
            "q = foreach q generate "
            "MonthDate as MonthDate, "
            "MonthLabel as MonthLabel, "
            "sum(sum(ActualClosedWonARR)) over ([..0] partition by all order by (MonthDate)) as Actual, "
            "sum(sum(CommitCallARR)) over ([..0] partition by all order by (MonthDate)) as Commit, "
            "sum(sum(BestCaseCallARR)) over ([..0] partition by all order by (MonthDate)) as BestCase, "
            "sum(sum(PipelineCallARR)) over ([..0] partition by all order by (MonthDate)) as Pipeline, "
            "sum(sum(YoY10TargetARR)) over ([..0] partition by all order by (MonthDate)) as Target;\n"
            "q = order q by MonthDate asc;\n"
        ),
        "s_forecast_bridge": sq(
            f'base = load "{DS}";\n'
            "base = group base by all;\n"
            'a = foreach base generate "01 Closed Won" as BridgeStage, sum(ActualClosedWonARR) as BridgeValue;\n'
            'b = foreach base generate "02 Commit Open" as BridgeStage, sum(OpenCommitARR) as BridgeValue;\n'
            'c = foreach base generate "03 Best Case Open" as BridgeStage, sum(OpenBestCaseARR) as BridgeValue;\n'
            'd = foreach base generate "04 Gap to 10% YoY" as BridgeStage, (sum(YoY10TargetARR) - sum(BestCaseCallARR)) as BridgeValue;\n'
            "q = union a, b, c, d;\n"
        ),
        "s_region_pressure": sq(
            (
                f'q = load "{DS}";\n'
                + quarter_filter
                + 'q = filter q by SalesRegion != "Unassigned";\n'
                + "q = group q by SalesRegion;\n"
                + "q = foreach q generate "
                + "SalesRegion as SalesRegion, "
                + "(case "
                + "when sum(BestCaseCallARR) >= sum(YoY10TargetARR) then 0 "
                + "when (sum(PipelineCallARR) - sum(BestCaseCallARR)) > 0 then "
                + "((case when (sum(YoY10TargetARR) - sum(BestCaseCallARR)) > 0 "
                + "then (sum(YoY10TargetARR) - sum(BestCaseCallARR)) else 0 end) / "
                + "(sum(PipelineCallARR) - sum(BestCaseCallARR))) "
                + "else 1 end) as PromotionNeedPct;\n"
                + "q = order q by PromotionNeedPct desc;\n"
                + "q = limit q 12;\n"
            )
        ),
        "s_region_matrix": sq(
            (
                f'q = load "{DS}";\n'
                + quarter_filter
                + 'q = filter q by SalesRegion != "Unassigned";\n'
                + "q = group q by SalesRegion;\n"
                + "q = foreach q generate "
                + "SalesRegion as SalesRegion, "
                + "(case "
                + 'when sum(BestCaseCallARR) >= sum(YoY10TargetARR) then "Covered" '
                + 'when sum(PipelineCallARR) < sum(YoY10TargetARR) then "Undercovered" '
                + 'when (sum(PipelineCallARR) - sum(BestCaseCallARR)) <= 0 then "No Headroom" '
                + 'when ((sum(YoY10TargetARR) - sum(BestCaseCallARR)) / (sum(PipelineCallARR) - sum(BestCaseCallARR))) <= 0.35 then "Light Promotion" '
                + 'when ((sum(YoY10TargetARR) - sum(BestCaseCallARR)) / (sum(PipelineCallARR) - sum(BestCaseCallARR))) <= 0.70 then "Heavy Promotion" '
                + 'else "Low Confidence" '
                + "end) as CoverageStatus, "
                + "(case "
                + "when (sum(PipelineCallARR) - sum(BestCaseCallARR)) > 0 then "
                + "((case when (sum(YoY10TargetARR) - sum(BestCaseCallARR)) > 0 "
                + "then (sum(YoY10TargetARR) - sum(BestCaseCallARR)) else 0 end) / "
                + "(sum(PipelineCallARR) - sum(BestCaseCallARR))) "
                + "else 0 end) as PromotionNeedPct, "
                + "(case when (sum(YoY10TargetARR) - sum(BestCaseCallARR)) > 0 "
                + "then (sum(YoY10TargetARR) - sum(BestCaseCallARR)) else 0 end) "
                + "as NeededFromPipelineARR, "
                + "(sum(PipelineCallARR) - sum(BestCaseCallARR)) as LowConfidencePipelineARR, "
                + "(sum(BestCaseCallARR) - sum(YoY10TargetARR)) as BestCaseGapARR;\n"
                + "q = order q by BestCaseGapARR asc;\n"
                + "q = limit q 12;\n"
            )
        ),
        "s_unit_gap": sq(
            (
                f'q = load "{DS}";\n'
                + quarter_filter
                + 'q = filter q by AccountUnitGroup != "Unassigned";\n'
                + "q = group q by AccountUnitGroup;\n"
                + "q = foreach q generate AccountUnitGroup, "
                + "(sum(BestCaseCallARR) - sum(YoY10TargetARR)) as GapToTarget;\n"
                + "q = order q by GapToTarget asc;\n"
                + "q = limit q 12;\n"
            )
        ),
        "s_country_best_case_map": sq(
            (
                f'q = load "{GEO_DS}";\n'
                + quarter_filter
                + 'q = filter q by Country != "" && Country != "Unknown";\n'
                + "q = group q by Country;\n"
                + "q = foreach q generate Country as Country, sum(BestCaseCallARR) as BestCaseCallARR;\n"
                + "q = order q by BestCaseCallARR desc;\n"
                + "q = limit q 80;\n"
            )
        ),
        "s_forecast_category_trend": sq(
            (
                f'q = load "{DS}";\n'
                + "q = filter q by OpenOpportunityFlag == 1 && ARR > 0;\n"
                + quarter_filter
                + "q = group q by (MonthDate, MonthLabel);\n"
                + "q = foreach q generate "
                + "MonthDate as MonthDate, "
                + "MonthLabel as MonthLabel, "
                + "sum(sum(OpenCommitARR)) over ([..0] partition by all order by (MonthDate)) as 'Commit', "
                + "sum(sum(OpenBestCaseARR)) over ([..0] partition by all order by (MonthDate)) as 'Best Case', "
                + "sum(sum(OpenPipelineARR)) over ([..0] partition by all order by (MonthDate)) as 'Pipeline', "
                + "sum(sum(OpenOmittedARR)) over ([..0] partition by all order by (MonthDate)) as 'Omitted', "
                + "sum(sum(ARR)) over ([..0] partition by all order by (MonthDate)) as 'Total ARR Universe';\n"
                + "q = order q by MonthDate asc;\n"
            )
        ),
        "s_risk_queue": sq(
            (
                f'q = load "{DS}";\n'
                + f'q = filter q by OpenOpportunityFlag == 1 && ARR >= {EXEC_QUEUE_MIN_ARR} && DaysToClose <= 120 && ForecastCategory != "Omitted";\n'
                + quarter_filter
                + "q = foreach q generate "
                + "OpportunityName as OpportunityName, "
                + "AccountName as AccountName, "
                + "OwnerName as OwnerName, "
                + "SalesRegion as SalesRegion, "
                + "AccountUnitGroup as AccountUnitGroup, "
                + "ForecastCategory as ForecastCategory, "
                + "StageName as StageName, "
                + "CloseDate as CloseDate, "
                + "DaysToClose as DaysToClose, "
                + "PriorityScore as PriorityScore, "
                + "ARR as ARR, "
                + "CloseQuarter as CloseQuarter, "
                + "(case "
                + 'when ForecastCategory == "Commit" && DaysToClose < 0 then "Commit | Past due" '
                + 'when ForecastCategory == "Commit" && DaysToClose <= 30 then "Commit | Next 30 days" '
                + 'when ForecastCategory == "Commit" then "Commit" '
                + 'when ForecastCategory == "Best Case" && DaysToClose < 0 then "Best Case | Past due" '
                + 'when ForecastCategory == "Best Case" && DaysToClose <= 30 then "Best Case | Next 30 days" '
                + 'when ForecastCategory == "Best Case" then "Best Case" '
                + 'when ForecastCategory == "Pipeline" && DaysToClose < 0 then "Pipeline | Past due" '
                + 'when ForecastCategory == "Pipeline" && DaysToClose <= 30 then "Pipeline | Next 30 days" '
                + 'when ForecastCategory == "Pipeline" && NextStep == "" then "Pipeline | No next step" '
                + 'when ForecastCategory == "Pipeline" then "Pipeline" '
                + "else ForecastCategory "
                + "end) as ForecastPulse, "
                + "(case "
                + 'when ForecastCategory == "Commit" && DaysToClose <= 30 then "Protect commit in next 30 days" '
                + 'when ForecastCategory == "Commit" then "Validate commit plan" '
                + 'when ForecastCategory == "Best Case" && DaysToClose <= 30 then "Promote to commit in next 30 days" '
                + 'when ForecastCategory == "Best Case" then "Pull to commit or reset" '
                + 'when ForecastCategory == "Pipeline" && DaysToClose <= 30 then "Promote to best case in next 30 days" '
                + 'when ForecastCategory == "Pipeline" && NextStep == "" then "Set next step or reclassify" '
                + 'when ForecastCategory == "Pipeline" then "Qualify or reclassify" '
                + 'when DaysToClose < 0 then "Rephase or close out" '
                + 'else "Clarify forecast" '
                + "end) as LeadershipAsk, "
                + "OpportunityId as OpportunityId, "
                + "AccountId as AccountId;\n"
                + "q = order q by PriorityScore desc;\n"
                + "q = limit q 10;\n"
            )
        ),
    }


def build_widgets() -> dict[str, dict]:
    return {
        "p1_hdr": hdr(
            "Revenue Story",
            f"FY{CURRENT_FY} actuals, forecast scenarios, and target gap from live Opportunity source only.",
        ),
        "p1_f_region": pillbox("f_region", "Sales Region"),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_quarter": pillbox("f_quarter", "Close Quarter"),
        "p1_kpi_actual": num(
            "s_kpi_actual",
            "ActualClosedWonARR",
            "Actual Closed Won ARR",
            "#0B5CAB",
            compact=True,
            tier="tertiary",
        ),
        "p1_kpi_commit": num(
            "s_kpi_commit",
            "CommitCallARR",
            "Commit Forecast ARR",
            "#0B5CAB",
            compact=True,
            tier="tertiary",
        ),
        "p1_kpi_best": num(
            "s_kpi_best",
            "BestCaseCallARR",
            "Best Case Forecast ARR",
            "#0B5CAB",
            compact=True,
            tier="tertiary",
        ),
        "p1_kpi_pipeline": num(
            "s_kpi_pipeline",
            "PipelineCallARR",
            "Pipeline Envelope ARR",
            "#0B5CAB",
            compact=True,
            tier="tertiary",
        ),
        "p1_kpi_target": num(
            "s_kpi_target",
            "YoY10TargetARR",
            "10% YoY Target ARR",
            "#0B5CAB",
            compact=True,
            tier="tertiary",
        ),
        "p1_kpi_needed": num(
            "s_kpi_needed",
            "NeededFromPipelineARR",
            "Needed Promotion ARR",
            "#0B5CAB",
            compact=True,
            tier="tertiary",
        ),
        "p1_ch_ladder": rich_chart(
            "s_call_ladder",
            "line",
            f"FY{CURRENT_FY} Forecast Scenario Ladder",
            ["MonthLabel"],
            [
                "Actual",
                "Commit",
                "BestCase",
                "Pipeline",
                "Target",
            ],
            show_legend=True,
            legend_pos="bottom",
            axis_title="ARR (EUR)",
            subtitle=(
                "Cumulative scenarios: Commit Forecast = Closed + Commit, Best Case Forecast = Closed + Commit + Best Case, "
                "and Pipeline Envelope = Closed + Commit + Best Case + Pipeline. Target is prior-year actuals plus 10%."
            ),
            number_format="$#,##0",
        ),
        "p1_ch_bridge": waterfall_chart(
            "s_forecast_bridge",
            "Forecast Build to 10% YoY Target",
            "BridgeStage",
            "BridgeValue",
        ),
        "p1_ch_region": rich_chart(
            "s_region_pressure",
            "hbar",
            "Promotion Pressure by Region",
            ["SalesRegion"],
            ["PromotionNeedPct"],
            show_legend=False,
            axis_title="Promotion Need %",
            subtitle="Share of low-confidence pipeline that must move up to hit target. Higher values mean a harder promotion ask.",
            number_format="0%",
        ),
        "p1_tbl_region": compare_table(
            "s_region_matrix",
            "Regional Promotion Pressure Table",
            columns=[
                "SalesRegion",
                "CoverageStatus",
                "PromotionNeedPct",
                "NeededFromPipelineARR",
                "LowConfidencePipelineARR",
                "BestCaseGapARR",
            ],
            column_properties={
                "SalesRegion": {"width": 120},
                "CoverageStatus": {"width": 135},
                "PromotionNeedPct": {"width": 105, "alignment": "right"},
                "NeededFromPipelineARR": {"width": 120, "alignment": "right"},
                "LowConfidencePipelineARR": {"width": 125, "alignment": "right"},
                "BestCaseGapARR": {"width": 110, "alignment": "right"},
            },
            row_limit=12,
            show_totals=False,
            min_col_width=60,
            max_col_width=180,
            subtitle="Regional readout of coverage, promotion required, low-confidence headroom, and remaining best-case shortfall.",
        ),
        "p2_hdr": hdr(
            "Exceptions",
            "Where open ARR is sitting, where confidence is weakest, and which deals need leadership attention now.",
        ),
        "p2_ch_category_mix": rich_chart(
            "s_forecast_category_trend",
            "line",
            "Cumulative Open ARR by Forecast Category",
            ["MonthLabel"],
            ["Commit", "Best Case", "Pipeline", "Omitted", "Total ARR Universe"],
            show_legend=True,
            legend_pos="bottom",
            axis_title="Open ARR (EUR)",
            subtitle=(
                "Quarter-aware cumulative view of open ARR by raw forecast category. "
                "Use this to reconcile the forecast scenarios with Pipeline Inspection while keeping Omitted visible but secondary. "
                "Total ARR Universe is the full open-opportunity reference line."
            ),
            number_format="$#,##0",
        ),
        "p2_map_geo_v2": choropleth_chart(
            "s_country_best_case_map",
            "Global Best Case ARR Footprint",
            "Country",
            "BestCaseCallARR",
        ),
        "p2_tbl_risk": compare_table(
            "s_risk_queue",
            "Top Promotion Candidates",
            columns=[
                "OpportunityName",
                "AccountName",
                "ARR",
                "ForecastPulse",
                "LeadershipAsk",
                "CloseQuarter",
                "OwnerName",
            ],
            column_properties={
                "OpportunityName": {"width": 200},
                "AccountName": {"width": 175},
                "ARR": {"width": 105, "alignment": "right"},
                "ForecastPulse": {"width": 135},
                "LeadershipAsk": {"width": 180},
                "CloseQuarter": {"width": 85},
                "OwnerName": {"width": 120},
            },
            row_limit=10,
            show_totals=False,
            min_col_width=60,
            max_col_width=220,
            subtitle=f"Highest-priority open deals inside 120 days with ARR >= {EXEC_QUEUE_MIN_ARR:,.0f}, showing forecast pulse and the one leadership ask.",
        ),
    }


def build_layout() -> dict:
    return {
        "name": "Executive Revenue Layout",
        "numColumns": 12,
        "pages": [
            pg(
                "overview",
                "Overview",
                [
                    {
                        "name": "p1_hdr",
                        "row": 0,
                        "column": 0,
                        "colspan": 12,
                        "rowspan": 2,
                    },
                    {
                        "name": "p1_f_region",
                        "row": 2,
                        "column": 0,
                        "colspan": 4,
                        "rowspan": 2,
                    },
                    {
                        "name": "p1_f_unit",
                        "row": 2,
                        "column": 4,
                        "colspan": 4,
                        "rowspan": 2,
                    },
                    {
                        "name": "p1_f_quarter",
                        "row": 2,
                        "column": 8,
                        "colspan": 4,
                        "rowspan": 2,
                    },
                    {
                        "name": "p1_kpi_actual",
                        "row": 4,
                        "column": 0,
                        "colspan": 2,
                        "rowspan": 3,
                    },
                    {
                        "name": "p1_kpi_commit",
                        "row": 4,
                        "column": 2,
                        "colspan": 2,
                        "rowspan": 3,
                    },
                    {
                        "name": "p1_kpi_best",
                        "row": 4,
                        "column": 4,
                        "colspan": 2,
                        "rowspan": 3,
                    },
                    {
                        "name": "p1_kpi_pipeline",
                        "row": 4,
                        "column": 6,
                        "colspan": 2,
                        "rowspan": 3,
                    },
                    {
                        "name": "p1_kpi_target",
                        "row": 4,
                        "column": 8,
                        "colspan": 2,
                        "rowspan": 3,
                    },
                    {
                        "name": "p1_kpi_needed",
                        "row": 4,
                        "column": 10,
                        "colspan": 2,
                        "rowspan": 3,
                    },
                    {
                        "name": "p1_ch_ladder",
                        "row": 7,
                        "column": 0,
                        "colspan": 7,
                        "rowspan": 8,
                    },
                    {
                        "name": "p1_ch_bridge",
                        "row": 7,
                        "column": 7,
                        "colspan": 5,
                        "rowspan": 8,
                    },
                    {
                        "name": "p1_ch_region",
                        "row": 15,
                        "column": 0,
                        "colspan": 5,
                        "rowspan": 8,
                    },
                    {
                        "name": "p1_tbl_region",
                        "row": 15,
                        "column": 5,
                        "colspan": 7,
                        "rowspan": 8,
                    },
                    {
                        "name": "p2_hdr",
                        "row": 23,
                        "column": 0,
                        "colspan": 12,
                        "rowspan": 2,
                    },
                    {
                        "name": "p2_ch_category_mix",
                        "row": 25,
                        "column": 0,
                        "colspan": 6,
                        "rowspan": 8,
                    },
                    {
                        "name": "p2_map_geo_v2",
                        "row": 25,
                        "column": 6,
                        "colspan": 6,
                        "rowspan": 8,
                    },
                    {
                        "name": "p2_tbl_risk",
                        "row": 33,
                        "column": 0,
                        "colspan": 12,
                        "rowspan": 12,
                    },
                ],
            ),
        ],
    }


def main() -> None:
    with builder_run("Executive_Revenue_Source_Truth", __file__) as summary:
        inst, tok = get_auth()
        assert_org_schema(inst, tok, objects=["Opportunity"])
        summary.row_count = create_dataset(inst, tok)
        ds_id = get_dataset_id(inst, tok, DS)
        if not ds_id:
            raise RuntimeError("Dataset ID lookup failed after upload")
        summary.dataset_id = ds_id
        if not get_dataset_id(inst, tok, GEO_DS):
            raise RuntimeError("Geo dataset ID lookup failed after upload")

        dashboard_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
        state = build_dashboard_state(
            build_steps(ds_id), build_widgets(), build_layout()
        )
        deploy_dashboard(inst, tok, dashboard_id, state)
        set_record_links_xmd(
            inst,
            tok,
            DS,
            [
                {
                    "field": "OpportunityName",
                    "id_field": "OpportunityId",
                    "label": "Opportunity",
                },
                {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
            ],
        )
        logger.info("%s/analytics/dashboard/%s", inst, dashboard_id)


if __name__ == "__main__":
    main()
