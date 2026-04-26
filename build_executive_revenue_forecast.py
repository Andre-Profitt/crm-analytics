#!/usr/bin/env python3
"""Build the Executive Revenue & Forecast dashboard.

Executive redesign principles:
- native monthly forecast view with prediction bands
- explicit plan bridge instead of a weak bullet chart
- quarter and unit accountability views
- sharper risk concentration and action queues

Dataset:
  - Executive_Revenue_Forecast
"""

from __future__ import annotations

import csv
import io
from datetime import date

from crm_analytics_helpers import (
    _date,
    _dim,
    _measure,
    _soql,
    add_table_action,
    af,
    build_dashboard_state,
    coalesce_filter,
    combo_chart,
    compare_table,
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    hdr,
    KPI_CARD_STYLE,
    line_chart,
    nav_link,
    nav_row,
    num,
    pg,
    pillbox,
    precompute_scoring_stats,
    compute_win_score,
    rich_chart,
    section_label,
    set_record_links_xmd,
    sq,
    upload_dataset,
    waterfall_chart,
)
from portfolio_foundation import (
    coerce_bool,
    fiscal_label,
    forecast_weight,
    least_squares,
    month_key,
    month_sequence,
    month_start,
    normalize_motion,
    prediction_interval,
    risk_level_to_score,
    safe_float,
)

DS = "Executive_Revenue_Forecast"
DS_LABEL = "Executive Revenue Forecast"
DASHBOARD_LABEL = "Executive Revenue & Forecast"
PIPELINE_DS = "Pipeline_Opportunity_Operations"
TODAY = date.today()

# ── Consulting-grade patterns ─────────────────────────────────────────────
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_unit", "f_region"],
    },
}

ISOLATION_QUAD = {
    "broadcastFacet": False,
    "selectMode": "none",
    "receiveFacetSource": {"mode": "none"},
    "useGlobal": False,
}
TODAY_YEAR = TODAY.year
TODAY_MONTH = TODAY.month

SOQL = (
    "SELECT Id, Name, Owner.Name, Owner.Annual_Revenue_Goal__c, AccountId, Account.Name, "
    "Account_Unit_Group__c, Sales_Region__c, ForecastCategoryName, "
    "IsClosed, IsWon, CloseDate, StageName, Type, CreatedDate, "
    "FiscalYear, FiscalQuarter, "
    "APTS_Forecast_ARR__c, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
    "Amount, Probability, AgeInDays, Sales_Cycle_Duration__c, "
    "Quota_Amount__c, Reason_Won_Lost__c, "
    "Account.Risk_of_Potential_Termination__c "
    "FROM Opportunity "
    "WHERE FiscalYear IN (2025, 2026, 2027)"
)


def _rebind(binding: str, alias: str) -> str:
    """Retarget a coalesce_filter binding to a specific SAQL alias."""
    return (
        binding.replace("q =", f"{alias} =")
        .replace("q by", f"{alias} by")
        .replace("q generate", f"{alias} generate")
    )


def _risk_score(
    win_score: float, risk_level: str, age_in_days: float, forecast_category: str
) -> float:
    """Blend account renewal risk and deal-level confidence into one score."""
    score = max(risk_level_to_score(risk_level), 100.0 - win_score)
    category = (forecast_category or "").strip().lower().replace(" ", "")
    if category in {"pipeline", "omitted"}:
        score += 8.0
    elif category == "bestcase":
        score += 3.0
    if age_in_days >= 180:
        score += 8.0
    elif age_in_days >= 90:
        score += 4.0
    return round(max(0.0, min(100.0, score)), 1)


def _priority_band(risk_score: float, upside_arr: float, weighted_open: float) -> str:
    """Operational priority label for queues."""
    if risk_score >= 80 and weighted_open >= 100000:
        return "Critical Risk"
    if risk_score >= 65 and weighted_open >= 50000:
        return "High Risk"
    if upside_arr >= 100000:
        return "High Upside"
    if upside_arr >= 50000:
        return "Upside"
    return "Monitor"


def _month_parts(close_month: str) -> tuple[int, int]:
    """Split YYYY-MM into (year, month)."""
    if not close_month or len(close_month) != 7:
        return 0, 0
    return int(close_month[:4]), int(close_month[5:7])


def _shift_month_key(month_key_value: str, offset: int) -> str:
    """Shift a YYYY-MM month key by N months."""
    year, month = _month_parts(month_key_value)
    if not year or not month:
        return month_key_value
    absolute = (year * 12 + (month - 1)) + offset
    shifted_year = absolute // 12
    shifted_month = (absolute % 12) + 1
    return f"{shifted_year:04d}-{shifted_month:02d}"


def create_dataset(inst: str, tok: str) -> bool:
    """Build the executive dashboard dataset."""
    print(f"\n=== Building {DS_LABEL} dataset ===")
    opps = _soql(inst, tok, SOQL)
    print(f"  Queried {len(opps)} opportunities")

    type_win_rates, avg_deal_size = precompute_scoring_stats(opps)

    detail_rows: list[dict[str, object]] = []
    owner_quota: dict[tuple[str, int], dict[str, object]] = {}
    segment_keys: set[tuple[str, str]] = set()
    actual_arr_by_segment_month: dict[tuple[str, str], dict[str, float]] = {}
    weighted_open_by_segment_month: dict[tuple[str, str], dict[str, float]] = {}
    sales_call_open_by_segment_month: dict[tuple[str, str], dict[str, float]] = {}
    commit_open_by_segment_month: dict[tuple[str, str], dict[str, float]] = {}
    best_case_open_by_segment_month: dict[tuple[str, str], dict[str, float]] = {}
    pipeline_open_by_segment_month_series: dict[tuple[str, str], dict[str, float]] = {}
    current_month_key = f"{TODAY_YEAR:04d}-{TODAY_MONTH:02d}"
    last_complete_month_key = _shift_month_key(current_month_key, -1)
    history_start_key = f"{TODAY_YEAR:04d}-01"
    forecast_end_key = f"{TODAY_YEAR:04d}-12"

    for opp in opps:
        acct = opp.get("Account") or {}
        owner = opp.get("Owner") or {}

        owner_name = (owner.get("Name") or "Unknown")[:255]
        account_name = (acct.get("Name") or "")[:255]
        opp_name = (opp.get("Name") or "")[:255]
        unit_group = (
            opp.get("Account_Unit_Group__c") or "Unassigned"
        ).strip() or "Unassigned"
        sales_region = (
            opp.get("Sales_Region__c") or "Unassigned"
        ).strip() or "Unassigned"
        motion = normalize_motion(opp.get("Type") or "")
        forecast_category = opp.get("ForecastCategoryName") or "Pipeline"
        close_date = opp.get("CloseDate") or ""
        close_month = month_key(close_date)
        month_year, month_month = _month_parts(close_month)

        fiscal_year = int(safe_float(opp.get("FiscalYear"), 0))
        fiscal_quarter = int(safe_float(opp.get("FiscalQuarter"), 0))
        fy_label = fiscal_label(fiscal_year)
        arr = safe_float(opp.get("ConvertedARR") or opp.get("APTS_Forecast_ARR__c"))
        probability = safe_float(opp.get("Probability"))
        age_in_days = safe_float(opp.get("AgeInDays"))
        cycle_days = safe_float(opp.get("Sales_Cycle_Duration__c"))
        quota_amount = safe_float(
            opp.get("Quota_Amount__c") or owner.get("Annual_Revenue_Goal__c")
        )
        is_closed = coerce_bool(opp.get("IsClosed"))
        is_won = coerce_bool(opp.get("IsWon"))
        risk_level = (
            acct.get("Risk_of_Potential_Termination__c") or ""
        ).strip() or "Low"

        win_score, _ = compute_win_score(opp, type_win_rates, avg_deal_size)
        weight = (
            1.0
            if is_won
            else (0.0 if is_closed else forecast_weight(forecast_category, probability))
        )
        weighted_open = 0.0 if is_closed else round(arr * weight, 2)
        risk_score = _risk_score(win_score, risk_level, age_in_days, forecast_category)
        forecast_confidence = round(max(0.0, 100.0 - risk_score), 1)
        confidence_weighted_arr = round(weighted_open * forecast_confidence / 100.0, 2)
        expected_arr = round(
            arr
            if is_won
            else (0.0 if is_closed else arr * max(probability, 15.0) / 100.0),
            2,
        )
        risk_weighted_arr = round(weighted_open * (risk_score / 100.0), 2)
        upside_arr = round(
            weighted_open * (win_score / 100.0) * (1.0 - min(risk_score, 85.0) / 140.0),
            2,
        )
        priority_band = _priority_band(risk_score, upside_arr, weighted_open)

        category_key = (forecast_category or "").strip().lower().replace(" ", "")
        commit_open = weighted_open if category_key == "commit" else 0.0
        best_case_open = weighted_open if category_key == "bestcase" else 0.0
        pipeline_open = (
            weighted_open if category_key in {"pipeline", "omitted"} else 0.0
        )
        at_risk_commit = (
            weighted_open
            if category_key in {"commit", "bestcase"} and risk_score >= 65.0
            else 0.0
        )

        segment_key = (unit_group, sales_region)
        segment_keys.add(segment_key)
        if close_month:
            if is_won:
                monthly_actual = actual_arr_by_segment_month.setdefault(segment_key, {})
                monthly_actual[close_month] = round(
                    monthly_actual.get(close_month, 0.0) + arr, 2
                )
            elif not is_closed:
                monthly_open = weighted_open_by_segment_month.setdefault(
                    segment_key, {}
                )
                monthly_open[close_month] = round(
                    monthly_open.get(close_month, 0.0) + weighted_open,
                    2,
                )
                if category_key in {"commit", "bestcase", "pipeline"}:
                    monthly_sales_call = sales_call_open_by_segment_month.setdefault(
                        segment_key, {}
                    )
                    monthly_sales_call[close_month] = round(
                        monthly_sales_call.get(close_month, 0.0) + arr,
                        2,
                    )
                if category_key == "commit":
                    monthly_commit = commit_open_by_segment_month.setdefault(
                        segment_key, {}
                    )
                    monthly_commit[close_month] = round(
                        monthly_commit.get(close_month, 0.0) + arr,
                        2,
                    )
                elif category_key == "bestcase":
                    monthly_best_case = best_case_open_by_segment_month.setdefault(
                        segment_key, {}
                    )
                    monthly_best_case[close_month] = round(
                        monthly_best_case.get(close_month, 0.0) + arr,
                        2,
                    )
                elif category_key == "pipeline":
                    monthly_pipeline = pipeline_open_by_segment_month_series.setdefault(
                        segment_key, {}
                    )
                    monthly_pipeline[close_month] = round(
                        monthly_pipeline.get(close_month, 0.0) + arr,
                        2,
                    )

        detail_rows.append(
            {
                "RecordType": "detail",
                "Id": opp.get("Id", ""),
                "OpportunityName": opp_name,
                "AccountId": opp.get("AccountId", ""),
                "AccountName": account_name,
                "OwnerName": owner_name,
                "UnitGroup": unit_group,
                "SalesRegion": sales_region,
                "MotionType": motion,
                "ForecastCategory": forecast_category,
                "StageName": opp.get("StageName") or "",
                "RiskLevel": risk_level,
                "PriorityBand": priority_band,
                "IsClosed": str(is_closed).lower(),
                "IsWon": str(is_won).lower(),
                "CloseDate": close_date,
                "CreatedDate": (opp.get("CreatedDate") or "")[:10],
                "MonthDate": month_start(close_date) if close_date else "",
                "MonthLabel": close_month,
                "MonthYear": month_year,
                "MonthMonth": month_month,
                "FiscalYear": fiscal_year,
                "FiscalQuarter": fiscal_quarter,
                "FYLabel": fy_label,
                "ARR": round(arr, 2),
                "Amount": round(safe_float(opp.get("Amount")), 2),
                "Probability": round(probability, 1),
                "AgeInDays": round(age_in_days, 1),
                "SalesCycleDuration": round(cycle_days, 1),
                "QuotaAmount": round(quota_amount, 2),
                "PlanARR": 0.0,
                "WinScore": round(float(win_score), 1),
                "RiskScore": risk_score,
                "ForecastConfidenceScore": forecast_confidence,
                "ConfidenceWeightedARR": confidence_weighted_arr,
                "WeightedOpenARR": weighted_open,
                "CommitOpenARR": round(commit_open, 2),
                "BestCaseOpenARR": round(best_case_open, 2),
                "PipelineOpenARR": round(pipeline_open, 2),
                "AtRiskCommitARR": round(at_risk_commit, 2),
                "ExpectedARR": expected_arr,
                "RiskWeightedARR": risk_weighted_arr,
                "UpsideARR": upside_arr,
                "ActualARR": round(arr, 2) if is_won else 0.0,
                "RenewalRiskARR": round(weighted_open, 2)
                if motion == "Renewal" and risk_score >= 60
                else 0.0,
            }
        )

        if fiscal_year and quota_amount > 0:
            key = (owner_name, fiscal_year)
            existing = owner_quota.get(key)
            if existing is None or quota_amount > float(existing["QuotaAmount"]):
                owner_quota[key] = {
                    "QuotaAmount": round(quota_amount, 2),
                    "UnitGroup": unit_group,
                    "SalesRegion": sales_region,
                }

    quota_rows: list[dict[str, object]] = []
    for (_, fiscal_year), quota_context in owner_quota.items():
        segment_keys.add(
            (str(quota_context["UnitGroup"]), str(quota_context["SalesRegion"]))
        )
        monthly_plan = round(float(quota_context["QuotaAmount"]) / 12.0, 2)
        fy_label = fiscal_label(fiscal_year)
        for month_num in range(1, 13):
            month_label = f"{fiscal_year:04d}-{month_num:02d}"
            month_date = f"{month_label}-01"
            quota_rows.append(
                {
                    "RecordType": "quota_month",
                    "Id": "",
                    "OpportunityName": "",
                    "AccountId": "",
                    "AccountName": "",
                    "OwnerName": "",
                    "UnitGroup": quota_context["UnitGroup"],
                    "SalesRegion": quota_context["SalesRegion"],
                    "MotionType": "",
                    "ForecastCategory": "",
                    "StageName": "",
                    "RiskLevel": "",
                    "PriorityBand": "",
                    "IsClosed": "false",
                    "IsWon": "false",
                    "CloseDate": "",
                    "CreatedDate": "",
                    "MonthDate": month_date,
                    "MonthLabel": month_label,
                    "MonthYear": fiscal_year,
                    "MonthMonth": month_num,
                    "FiscalYear": fiscal_year,
                    "FiscalQuarter": ((month_num - 1) // 3) + 1,
                    "FYLabel": fy_label,
                    "ARR": 0.0,
                    "Amount": 0.0,
                    "Probability": 0.0,
                    "AgeInDays": 0.0,
                    "SalesCycleDuration": 0.0,
                    "QuotaAmount": round(float(quota_context["QuotaAmount"]), 2),
                    "PlanARR": monthly_plan,
                    "WinScore": 0.0,
                    "RiskScore": 0.0,
                    "ForecastConfidenceScore": 0.0,
                    "ConfidenceWeightedARR": 0.0,
                    "WeightedOpenARR": 0.0,
                    "CommitOpenARR": 0.0,
                    "BestCaseOpenARR": 0.0,
                    "PipelineOpenARR": 0.0,
                    "AtRiskCommitARR": 0.0,
                    "ExpectedARR": 0.0,
                    "RiskWeightedARR": 0.0,
                    "UpsideARR": 0.0,
                    "ActualARR": 0.0,
                    "RenewalRiskARR": 0.0,
                }
            )

    forecast_chart_rows: list[dict[str, object]] = []
    chart_months = month_sequence(history_start_key, forecast_end_key)
    historical_months = [
        month for month in chart_months if month <= last_complete_month_key
    ]

    for unit_group, sales_region in sorted(segment_keys):
        segment_key = (unit_group, sales_region)
        actual_series = actual_arr_by_segment_month.get(segment_key, {})
        open_series = weighted_open_by_segment_month.get(segment_key, {})
        sales_call_series = sales_call_open_by_segment_month.get(segment_key, {})
        commit_series = commit_open_by_segment_month.get(segment_key, {})
        best_case_series = best_case_open_by_segment_month.get(segment_key, {})
        pipeline_series = pipeline_open_by_segment_month_series.get(segment_key, {})
        history_values = [
            round(actual_series.get(month, 0.0), 2) for month in historical_months
        ]

        if not any(history_values) and not any(
            open_series.get(month, 0.0) for month in chart_months
        ):
            continue

        fit = least_squares(history_values)
        running_actual = 0.0
        running_forecast = 0.0
        running_sales_call = 0.0
        running_commit_call = 0.0
        running_best_case_call = 0.0
        running_pipeline_call = 0.0
        forecast_variance = 0.0
        for index, month_label in enumerate(chart_months):
            month_date = f"{month_label}-01"
            month_year, month_month = _month_parts(month_label)
            fy_label = fiscal_label(month_year)
            monthly_actual_value = round(actual_series.get(month_label, 0.0), 2)
            trend_value = max(0.0, fit["intercept"] + fit["slope"] * index)

            if month_label <= last_complete_month_key:
                running_actual = round(running_actual + monthly_actual_value, 2)
                running_forecast = running_actual
                running_sales_call = running_actual
                running_commit_call = running_actual
                running_best_case_call = running_actual
                running_pipeline_call = running_actual
                chart_value = running_actual
                sales_call_value = running_sales_call
                commit_call_value = running_commit_call
                best_case_call_value = running_best_case_call
                pipeline_call_value = running_pipeline_call
                band_high = chart_value
                band_low = chart_value
            else:
                pipeline_close_month_arr = round(open_series.get(month_label, 0.0), 2)
                monthly_sales_call_value = round(
                    sales_call_series.get(month_label, 0.0), 2
                )
                monthly_commit_call_value = round(
                    commit_series.get(month_label, 0.0), 2
                )
                monthly_best_case_call_value = round(
                    best_case_series.get(month_label, 0.0), 2
                )
                monthly_pipeline_call_value = round(
                    pipeline_series.get(month_label, 0.0), 2
                )
                if pipeline_close_month_arr > 0 and trend_value > 0:
                    monthly_forecast_value = round(
                        (pipeline_close_month_arr * 0.7) + (trend_value * 0.3),
                        2,
                    )
                else:
                    monthly_forecast_value = round(
                        pipeline_close_month_arr
                        if pipeline_close_month_arr > 0
                        else trend_value,
                        2,
                    )

                monthly_interval = min(
                    max(
                        prediction_interval(fit, index),
                        abs(monthly_forecast_value - trend_value) * 0.5,
                        monthly_forecast_value * 0.08
                        if monthly_forecast_value > 0
                        else 0.0,
                    ),
                    monthly_forecast_value * 0.3 if monthly_forecast_value > 0 else 0.0,
                )
                running_forecast = round(running_forecast + monthly_forecast_value, 2)
                running_sales_call = round(
                    running_sales_call + monthly_sales_call_value, 2
                )
                running_commit_call = round(
                    running_commit_call + monthly_commit_call_value, 2
                )
                running_best_case_call = round(
                    running_best_case_call
                    + monthly_commit_call_value
                    + monthly_best_case_call_value,
                    2,
                )
                running_pipeline_call = round(
                    running_pipeline_call
                    + monthly_commit_call_value
                    + monthly_best_case_call_value
                    + monthly_pipeline_call_value,
                    2,
                )
                forecast_variance += monthly_interval * monthly_interval
                cumulative_interval = round(forecast_variance**0.5, 2)
                chart_value = running_forecast
                sales_call_value = running_sales_call
                commit_call_value = running_commit_call
                best_case_call_value = running_best_case_call
                pipeline_call_value = running_pipeline_call
                band_high = round(chart_value + cumulative_interval, 2)
                band_low = round(max(0.0, chart_value - cumulative_interval), 2)

            forecast_chart_rows.append(
                {
                    "RecordType": "forecast_chart",
                    "Id": "",
                    "OpportunityName": "",
                    "AccountId": "",
                    "AccountName": "",
                    "OwnerName": "",
                    "UnitGroup": unit_group,
                    "SalesRegion": sales_region,
                    "MotionType": "",
                    "ForecastCategory": "",
                    "StageName": "",
                    "RiskLevel": "",
                    "PriorityBand": "",
                    "IsClosed": "false",
                    "IsWon": "false",
                    "CloseDate": "",
                    "CreatedDate": "",
                    "MonthDate": month_date,
                    "MonthLabel": month_label,
                    "MonthYear": month_year,
                    "MonthMonth": month_month,
                    "FiscalYear": month_year,
                    "FiscalQuarter": ((month_month - 1) // 3) + 1,
                    "FYLabel": fy_label,
                    "ARR": 0.0,
                    "Amount": 0.0,
                    "Probability": 0.0,
                    "AgeInDays": 0.0,
                    "SalesCycleDuration": 0.0,
                    "QuotaAmount": 0.0,
                    "PlanARR": 0.0,
                    "WinScore": 0.0,
                    "RiskScore": 0.0,
                    "ForecastConfidenceScore": 0.0,
                    "ConfidenceWeightedARR": 0.0,
                    "WeightedOpenARR": 0.0,
                    "CommitOpenARR": 0.0,
                    "BestCaseOpenARR": 0.0,
                    "PipelineOpenARR": 0.0,
                    "AtRiskCommitARR": 0.0,
                    "ExpectedARR": 0.0,
                    "RiskWeightedARR": 0.0,
                    "UpsideARR": 0.0,
                    "ActualARR": 0.0,
                    "RenewalRiskARR": 0.0,
                    "RevenueForecastARR": chart_value,
                    "RevenueForecastARR_high_95": band_high,
                    "RevenueForecastARR_low_95": band_low,
                    "SalesCallForecastARR": sales_call_value,
                    "CommitCallARR": commit_call_value,
                    "BestCaseCallARR": best_case_call_value,
                    "PipelineCallARR": pipeline_call_value,
                }
            )

    rows = detail_rows + quota_rows + forecast_chart_rows
    print(f"  Detail rows: {len(detail_rows)}")
    print(f"  Quota rows: {len(quota_rows)}")
    print(f"  Forecast chart rows: {len(forecast_chart_rows)}")

    field_names = [
        "RecordType",
        "Id",
        "OpportunityName",
        "AccountId",
        "AccountName",
        "OwnerName",
        "UnitGroup",
        "SalesRegion",
        "MotionType",
        "ForecastCategory",
        "StageName",
        "RiskLevel",
        "PriorityBand",
        "IsClosed",
        "IsWon",
        "CloseDate",
        "CreatedDate",
        "MonthDate",
        "MonthLabel",
        "MonthYear",
        "MonthMonth",
        "FiscalYear",
        "FiscalQuarter",
        "FYLabel",
        "ARR",
        "Amount",
        "Probability",
        "AgeInDays",
        "SalesCycleDuration",
        "QuotaAmount",
        "PlanARR",
        "WinScore",
        "RiskScore",
        "ForecastConfidenceScore",
        "ConfidenceWeightedARR",
        "WeightedOpenARR",
        "CommitOpenARR",
        "BestCaseOpenARR",
        "PipelineOpenARR",
        "AtRiskCommitARR",
        "ExpectedARR",
        "RiskWeightedARR",
        "UpsideARR",
        "ActualARR",
        "RenewalRiskARR",
        "RevenueForecastARR",
        "RevenueForecastARR_high_95",
        "RevenueForecastARR_low_95",
        "SalesCallForecastARR",
        "CommitCallARR",
        "BestCaseCallARR",
        "PipelineCallARR",
    ]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=field_names, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buffer.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes, {len(rows)} rows")

    fields_meta = [
        _dim("RecordType", "Record Type"),
        _dim("Id", "Opportunity ID"),
        _dim("OpportunityName", "Opportunity"),
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account"),
        _dim("OwnerName", "Owner"),
        _dim("UnitGroup", "Unit Group"),
        _dim("SalesRegion", "Sales Region"),
        _dim("MotionType", "Motion"),
        _dim("ForecastCategory", "Forecast Category"),
        _dim("StageName", "Stage"),
        _dim("RiskLevel", "Risk Level"),
        _dim("PriorityBand", "Priority"),
        _dim("IsClosed", "Is Closed"),
        _dim("IsWon", "Is Won"),
        _date("CloseDate", "Close Date"),
        _date("CreatedDate", "Created Date"),
        _date("MonthDate", "Month"),
        _dim("MonthLabel", "Month Label"),
        _measure("MonthYear", "Month Year", scale=0, precision=4),
        _measure("MonthMonth", "Month Number", scale=0, precision=2),
        _measure("FiscalYear", "Fiscal Year", scale=0, precision=4),
        _measure("FiscalQuarter", "Fiscal Quarter", scale=0, precision=2),
        _dim("FYLabel", "Fiscal Year Label"),
        _measure("ARR", "ARR (EUR)"),
        _measure("Amount", "Amount"),
        _measure("Probability", "Probability", scale=1, precision=5),
        _measure("AgeInDays", "Age (Days)", scale=1, precision=6),
        _measure("SalesCycleDuration", "Sales Cycle Duration", scale=1, precision=6),
        _measure("QuotaAmount", "Quota Amount"),
        _measure("PlanARR", "Monthly Plan ARR"),
        _measure("WinScore", "Win Score", scale=1, precision=5),
        _measure("RiskScore", "Risk Score", scale=1, precision=5),
        _measure(
            "ForecastConfidenceScore", "Forecast Confidence Score", scale=1, precision=5
        ),
        _measure("ConfidenceWeightedARR", "Confidence-Weighted ARR"),
        _measure("WeightedOpenARR", "Weighted Open ARR"),
        _measure("CommitOpenARR", "Commit Open ARR"),
        _measure("BestCaseOpenARR", "Best Case Open ARR"),
        _measure("PipelineOpenARR", "Pipeline Open ARR"),
        _measure("AtRiskCommitARR", "At-Risk Commit ARR"),
        _measure("ExpectedARR", "Expected ARR"),
        _measure("RiskWeightedARR", "Risk-Weighted ARR"),
        _measure("UpsideARR", "Upside ARR"),
        _measure("ActualARR", "Actual Closed Won ARR"),
        _measure("RenewalRiskARR", "Renewal Risk ARR"),
        _measure("RevenueForecastARR", "Revenue Forecast ARR"),
        _measure("RevenueForecastARR_high_95", "Revenue Forecast ARR High 95"),
        _measure("RevenueForecastARR_low_95", "Revenue Forecast ARR Low 95"),
        _measure("SalesCallForecastARR", "Sales Call Forecast ARR"),
        _measure("CommitCallARR", "Commit Call ARR"),
        _measure("BestCaseCallARR", "Best Case Call ARR"),
        _measure("PipelineCallARR", "Pipeline Call ARR"),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    pipeline_load = f'q = load "{PIPELINE_DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_region = coalesce_filter("f_region", "SalesRegion")
    current_fy_label = fiscal_label(TODAY_YEAR)
    filter_fy = f'q = filter q by FYLabel == "{current_fy_label}";\n'
    forecast_start_key = f"{TODAY_YEAR:04d}-{TODAY_MONTH:02d}"
    forecast_anchor_key = _shift_month_key(forecast_start_key, -1)

    pipeline_detail = (
        pipeline_load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_region
        + filter_fy
    )
    pipeline_field_events = (
        pipeline_load
        + 'q = filter q by RecordType == "field_history";\n'
        + filter_unit
        + filter_region
        + filter_fy
    )
    q1_filters = (
        _rebind(filter_unit, "q1")
        + _rebind(filter_region, "q1")
        + _rebind(filter_fy, "q1")
    )
    q2_filters = (
        _rebind(filter_unit, "q2")
        + _rebind(filter_region, "q2")
        + _rebind(filter_fy, "q2")
    )
    q3_filters = (
        _rebind(filter_unit, "q3")
        + _rebind(filter_region, "q3")
        + _rebind(filter_fy, "q3")
    )

    summary = (
        f'q1 = load "{DS}";\n'
        + 'q1 = filter q1 by RecordType == "detail";\n'
        + q1_filters
        + 'q1 = filter q1 by IsWon == "true";\n'
        + "q1 = group q1 by all;\n"
        + "q1 = foreach q1 generate sum(ARR) as actual_closed;\n"
        + f'q2 = load "{DS}";\n'
        + 'q2 = filter q2 by RecordType == "detail";\n'
        + q2_filters
        + 'q2 = filter q2 by IsClosed == "false";\n'
        + "q2 = group q2 by all;\n"
        + "q2 = foreach q2 generate "
        + "sum(WeightedOpenARR) as weighted_open, "
        + "sum(CommitOpenARR) as commit_open, "
        + "sum(BestCaseOpenARR) as best_case_open, "
        + "sum(PipelineOpenARR) as pipeline_open, "
        + "sum(AtRiskCommitARR) as at_risk_commit, "
        + "sum(RiskWeightedARR) as risk_arr, "
        + "sum(UpsideARR) as upside_arr, "
        + "sum(ConfidenceWeightedARR) as confidence_weighted;\n"
        + f'q3 = load "{DS}";\n'
        + 'q3 = filter q3 by RecordType == "quota_month";\n'
        + q3_filters
        + "q3 = group q3 by all;\n"
        + "q3 = foreach q3 generate sum(PlanARR) as target;\n"
        + "q = cogroup q1 by all, q2 by all, q3 by all;\n"
        + "q = foreach q generate "
        + "coalesce(sum(q1.actual_closed), 0) as actual_closed, "
        + "coalesce(sum(q2.weighted_open), 0) as weighted_open, "
        + "coalesce(sum(q2.commit_open), 0) as commit_open, "
        + "coalesce(sum(q2.best_case_open), 0) as best_case_open, "
        + "coalesce(sum(q2.pipeline_open), 0) as pipeline_open, "
        + "coalesce(sum(q2.at_risk_commit), 0) as at_risk_commit, "
        + "coalesce(sum(q2.risk_arr), 0) as risk_arr, "
        + "coalesce(sum(q2.upside_arr), 0) as upside_arr, "
        + "(coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) as projected, "
        + "coalesce(sum(q3.target), 0) as target, "
        + "((coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) - coalesce(sum(q3.target), 0)) as gap_to_plan, "
        + "(case when (coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) > 0 "
        + "then ((coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.confidence_weighted), 0)) / "
        + "(coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0))) * 100 "
        + "else 0 end) as forecast_confidence, "
        + "(case when coalesce(sum(q3.target), 0) > 0 "
        + "then ((coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) / coalesce(sum(q3.target), 0)) * 100 "
        + "else 0 end) as attainment_pct;"
    )

    plan_bridge = (
        f'q1 = load "{DS}";\n'
        + 'q1 = filter q1 by RecordType == "detail";\n'
        + q1_filters
        + 'q1 = filter q1 by IsWon == "true";\n'
        + "q1 = group q1 by all;\n"
        + "q1 = foreach q1 generate sum(ARR) as closed_arr;\n"
        + 'q1l = foreach q1 generate "1 Closed Won" as BridgeStep, 1 as BridgeOrder, closed_arr as BridgeARR;\n'
        + f'q2 = load "{DS}";\n'
        + 'q2 = filter q2 by RecordType == "detail";\n'
        + q2_filters
        + 'q2 = filter q2 by IsClosed == "false";\n'
        + "q2 = group q2 by all;\n"
        + "q2 = foreach q2 generate sum(CommitOpenARR) as commit_arr;\n"
        + 'q2l = foreach q2 generate "2 Commit Open" as BridgeStep, 2 as BridgeOrder, commit_arr as BridgeARR;\n'
        + f'q3 = load "{DS}";\n'
        + 'q3 = filter q3 by RecordType == "detail";\n'
        + q3_filters
        + 'q3 = filter q3 by IsClosed == "false";\n'
        + "q3 = group q3 by all;\n"
        + "q3 = foreach q3 generate sum(BestCaseOpenARR) as best_case_arr;\n"
        + 'q3l = foreach q3 generate "3 Best Case Open" as BridgeStep, 3 as BridgeOrder, best_case_arr as BridgeARR;\n'
        + f'q4 = load "{DS}";\n'
        + 'q4 = filter q4 by RecordType == "detail";\n'
        + q1_filters.replace("q1", "q4")
        + 'q4 = filter q4 by IsClosed == "false";\n'
        + "q4 = group q4 by all;\n"
        + "q4 = foreach q4 generate sum(PipelineOpenARR) as pipeline_arr;\n"
        + 'q4l = foreach q4 generate "4 Pipeline Open" as BridgeStep, 4 as BridgeOrder, pipeline_arr as BridgeARR;\n'
        + f'q5 = load "{DS}";\n'
        + 'q5 = filter q5 by RecordType == "quota_month";\n'
        + q3_filters.replace("q3", "q5")
        + "q5 = group q5 by all;\n"
        + "q5 = foreach q5 generate sum(PlanARR) as target_arr;\n"
        + "q6 = cogroup q1 by all, q2 by all, q3 by all, q4 by all, q5 by all;\n"
        + "q6 = foreach q6 generate "
        + '"5 Gap To Plan" as BridgeStep, '
        + "5 as BridgeOrder, "
        + "(coalesce(sum(q5.target_arr), 0) - "
        + "(coalesce(sum(q1.closed_arr), 0) + coalesce(sum(q2.commit_arr), 0) + coalesce(sum(q3.best_case_arr), 0) + "
        + "coalesce(sum(q4.pipeline_arr), 0))) as BridgeARR;\n"
        + "q = union q1l, q2l, q3l, q4l, q6;\n"
        + "q = order q by BridgeOrder asc;"
    )

    monthly_forecast = (
        load
        + 'q = filter q by RecordType == "forecast_chart";\n'
        + filter_unit
        + filter_region
        + filter_fy
        + "q = group q by (MonthDate, MonthLabel);\n"
        + "q = foreach q generate MonthDate, MonthLabel, "
        + f'(case when MonthLabel <= "{forecast_anchor_key}" then sum(RevenueForecastARR) else null end) as ActualARR, '
        + f'(case when MonthLabel >= "{forecast_anchor_key}" then sum(RevenueForecastARR) else null end) as ForecastARR, '
        + f'(case when MonthLabel == "{forecast_anchor_key}" then sum(RevenueForecastARR) '
        + f'when MonthLabel >= "{forecast_start_key}" then sum(RevenueForecastARR_high_95) else null end) as ForecastARR_high_95, '
        + f'(case when MonthLabel == "{forecast_anchor_key}" then sum(RevenueForecastARR) '
        + f'when MonthLabel >= "{forecast_start_key}" then sum(RevenueForecastARR_low_95) else null end) as ForecastARR_low_95;\n'
        + "q = order q by MonthDate asc;"
    )

    sales_call_view = (
        load
        + 'q = filter q by RecordType == "forecast_chart";\n'
        + filter_unit
        + filter_region
        + filter_fy
        + "q = group q by (MonthDate, MonthLabel);\n"
        + "q = foreach q generate MonthDate, MonthLabel, "
        + "sum(RevenueForecastARR) as ModelForecastARR, "
        + "sum(CommitCallARR) as CommitCallARR, "
        + "sum(BestCaseCallARR) as BestCaseCallARR, "
        + "sum(PipelineCallARR) as PipelineCallARR;\n"
        + "q = order q by MonthDate asc;"
    )

    pipe_quarter_confidence = (
        pipeline_detail
        + 'q = filter q by IsClosed == "false";\n'
        + "q = foreach q generate CloseQuarter, "
        + '(case when ForecastCategory == "Commit" then WeightedOpenARR else 0 end) as _commit, '
        + '(case when ForecastCategory == "Best Case" then WeightedOpenARR else 0 end) as _bestcase, '
        + '(case when ForecastCategory == "Pipeline" then WeightedOpenARR else 0 end) as _pipeline;\n'
        + "q = group q by CloseQuarter;\n"
        + "q = foreach q generate CloseQuarter, "
        + "sum(_commit) as CommitARR, "
        + "sum(_bestcase) as BestCaseARR, "
        + "sum(_pipeline) as PipelineARR;\n"
        + "q = order q by CloseQuarter asc;"
    )

    pipe_stage_velocity = (
        pipeline_detail
        + 'q = filter q by IsClosed == "false";\n'
        + 'q = filter q by StageOrder != "00";\n'
        + "q = group q by StageName;\n"
        + "q = foreach q generate StageName, "
        + "avg(DaysInStage) as AvgDaysInStage, "
        + "avg(StageSlaDays) as StageSlaDays;\n"
        + "q = order q by StageName asc;"
    )

    pipe_region_pressure = (
        pipeline_detail
        + 'q = filter q by IsClosed == "false";\n'
        + "q = group q by SalesRegion;\n"
        + "q = foreach q generate SalesRegion, "
        + "sum(WeightedOpenARR) as WeightedOpenARR, "
        + "sum(AtRiskARR) as AtRiskARR;\n"
        + "q = order q by AtRiskARR desc;"
    )

    pipe_push_trend = (
        pipeline_field_events
        + 'q = filter q by EventField == "CloseDate";\n'
        + "q = group q by (EventMonthDate, EventMonth);\n"
        + "q = foreach q generate EventMonthDate, EventMonth, "
        + "sum(PushCount) as PushCount, "
        + "avg(PushDays) as AvgPushDays;\n"
        + "q = order q by EventMonthDate asc;"
    )

    pipe_top_risk = (
        pipeline_detail
        + 'q = filter q by IsClosed == "false";\n'
        + "q = filter q by AtRiskARR > 0;\n"
        + "q = group q by (OpportunityName, AccountName, OwnerName, StageName, ExceptionType, Id);\n"
        + "q = foreach q generate OpportunityName, AccountName, OwnerName, StageName, ExceptionType, "
        + "max(WeightedOpenARR) as WeightedOpenARR, "
        + "max(TotalRiskScore) as TotalRiskScore, "
        + "max(SlipRiskScore) as SlipRiskScore, "
        + "max(PushCount) as PushCount, "
        + "Id;\n"
        + "q = order q by TotalRiskScore desc;\n"
        + "q = limit q 15;"
    )

    pipe_top_process = (
        pipeline_detail
        + 'q = filter q by IsClosed == "false";\n'
        + "q = filter q by (PastDueCount > 0) or (MissingApprovalCount > 0) or (StaleCount > 0) or (BackwardMoveCount > 1);\n"
        + "q = group q by (OpportunityName, AccountName, OwnerName, StageName, ExceptionType, Id);\n"
        + "q = foreach q generate OpportunityName, AccountName, OwnerName, StageName, ExceptionType, "
        + "max(WeightedOpenARR) as WeightedOpenARR, "
        + "max(DaysInStage) as DaysInStage, "
        + "max(PushCount) as PushCount, "
        + "max(BackwardMoveCount) as BackwardMoveCount, "
        + "max(MissingApprovalCount) as MissingApprovalCount, "
        + "Id;\n"
        + "q = order q by WeightedOpenARR desc;\n"
        + "q = limit q 15;"
    )

    # Apply KPI facet scoping — KPIs respond only to filter pillboxes
    s_summary = sq(summary)
    s_summary.update(KPI_FACET_SCOPE)

    return {
        "f_unit": af("UnitGroup", ds_meta),
        "f_region": af("SalesRegion", ds_meta),
        "s_summary": s_summary,
        "s_monthly_forecast": sq(monthly_forecast),
        "s_sales_call_view": sq(sales_call_view),
        "s_plan_bridge": sq(plan_bridge),
        "s_pipe_quarter_confidence": sq(pipe_quarter_confidence),
        "s_pipe_stage_velocity": sq(pipe_stage_velocity),
        "s_pipe_region_pressure": sq(pipe_region_pressure),
        "s_pipe_push_trend": sq(pipe_push_trend),
        "s_pipe_top_risk": sq(pipe_top_risk),
        "s_pipe_top_process": sq(pipe_top_process),
    }


def build_widgets() -> dict[str, dict]:
    """Build dashboard widgets."""
    forecast_start_key = f"{TODAY_YEAR:04d}-{TODAY_MONTH:02d}"
    last_complete_key = _shift_month_key(forecast_start_key, -1)
    last_complete_label = date.fromisoformat(f"{last_complete_key}-01").strftime(
        "%b %Y"
    )
    forecast_start_label = date.fromisoformat(f"{forecast_start_key}-01").strftime(
        "%b %Y"
    )
    current_fy_label = fiscal_label(TODAY_YEAR)

    timeline = rich_chart(
        "s_monthly_forecast",
        "line",
        "Revenue Trend & Forecast",
        ["MonthLabel"],
        ["ActualARR", "ForecastARR", "ForecastARR_high_95", "ForecastARR_low_95"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    timeline["parameters"]["title"]["subtitleLabel"] = (
        f"Current FY cumulative ARR won through {last_complete_label} | Model forecast begins {forecast_start_label}"
    )
    sales_call_chart = rich_chart(
        "s_sales_call_view",
        "line",
        "Salesforce Call Ladder vs Model",
        ["MonthLabel"],
        ["ModelForecastARR", "CommitCallARR", "BestCaseCallARR", "PipelineCallARR"],
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    sales_call_chart["parameters"]["title"]["subtitleLabel"] = (
        "Closed Won anchor, then cumulative Commit, Best Case, and Pipeline call layers against the model view"
    )

    widgets = {
        "p1_nav1": nav_link("summary", "Summary", active=True),
        "p1_nav2": nav_link("drivers", "Drivers & Risks"),
        "p1_hdr": hdr(
            "Executive Revenue & Forecast",
            f"{current_fy_label} forecast, plan bridge, and the executive drivers of revenue delivery.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_region": pillbox("f_region", "Region"),
        # ── KPI strip: tier-sized with card styling ──
        "p1_n_actual": num(
            "s_summary",
            "actual_closed",
            "Closed Won ARR",
            "#04844B",
            compact=True,
            tier="primary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_projected": num(
            "s_summary",
            "projected",
            "Projected ARR",
            "#032D60",
            compact=True,
            tier="primary",
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_gap": num(
            "s_summary",
            "gap_to_plan",
            "Gap To Plan",
            "#BA0517",
            compact=True,
            tier="secondary",
            prefix="€",
            sentiment_color=True,
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_confidence": num(
            "s_summary",
            "forecast_confidence",
            "Forecast Confidence",
            "#0176D3",
            compact=True,
            tier="secondary",
            suffix="%",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_section_forecast": section_label("Revenue Forecast & Plan Bridge"),
        "p1_ch_timeline": timeline,
        "p1_ch_bridge": waterfall_chart(
            "s_plan_bridge",
            "Projected ARR to Plan Bridge",
            "BridgeStep",
            "BridgeARR",
            axis_label="ARR (EUR)",
        ),
        "p1_ch_unit": sales_call_chart,
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("drivers", "Drivers & Risks", active=True),
        "p2_hdr": hdr(
            "Drivers & Risks",
            "Pipeline category pressure, stage aging, close-date drift, and the deals that need executive escalation.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_section_pipeline": section_label("Pipeline Category & Velocity"),
        "p2_ch_quarter": rich_chart(
            "s_pipe_quarter_confidence",
            "stackcolumn",
            "Quarterly Forecast Category Mix",
            ["CloseQuarter"],
            ["CommitARR", "BestCaseARR", "PipelineARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
            subtitle="Commit = high-confidence pipeline | Best Case = moderate confidence | Pipeline = early stage",
        ),
        "p2_ch_velocity": combo_chart(
            "s_pipe_stage_velocity",
            "Days in Stage vs SLA",
            ["StageName"],
            ["AvgDaysInStage"],
            ["StageSlaDays"],
            show_legend=True,
            axis_title="Days",
            subtitle="Bars exceeding the SLA line indicate stages where deals are aging beyond expected cycle time",
            reference_lines=[
                {"value": 30, "label": "30-Day Threshold", "color": "#FFB75D"}
            ],
            axis1_format="#,##0",
        ),
        "p2_section_risk": section_label("Regional Risk & Close Date Drift"),
        "p2_ch_region": rich_chart(
            "s_pipe_region_pressure",
            "stackhbar",
            "Open vs At-Risk ARR by Region",
            ["SalesRegion"],
            ["WeightedOpenARR", "AtRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
            subtitle="At-Risk = deals flagged by slip risk model (>50% slip probability) or process exceptions",
        ),
        "p2_ch_push": line_chart(
            "s_pipe_push_trend",
            "Close Date Push Trend",
            show_legend=True,
            axis_title="Pushes / Days",
            subtitle="Rising trend signals forecast degradation from close date drift across the pipeline",
            reference_lines=[
                {"value": 5, "label": "Push Threshold", "color": "#D4504C"}
            ],
        ),
        "p2_section_deals": section_label("Executive Action Queue"),
        "p2_tbl_risk": compare_table(
            "s_pipe_top_risk",
            "Top Pipeline Risk Deals",
            [
                "OpportunityName",
                "AccountName",
                "OwnerName",
                "StageName",
                "ExceptionType",
            ],
            ["WeightedOpenARR", "TotalRiskScore", "SlipRiskScore", "PushCount"],
            subtitle="Risk Score (0-100) = slip risk (push count, age, backward moves) + exception flags (SLA breach, stale)",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "TotalRiskScore",
                    "rules": [
                        {"value": 75, "color": "#D4504C", "operator": "gte"},
                        {"value": 50, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "PushCount",
                    "rules": [
                        {"value": 3, "color": "#D4504C", "operator": "gte"},
                        {"value": 2, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p2_tbl_process": compare_table(
            "s_pipe_top_process",
            "Top Process Exception Deals",
            [
                "OpportunityName",
                "AccountName",
                "OwnerName",
                "StageName",
                "ExceptionType",
            ],
            [
                "WeightedOpenARR",
                "DaysInStage",
                "PushCount",
                "BackwardMoveCount",
                "MissingApprovalCount",
            ],
            subtitle="Flagged for: past close date, >90d stale, missing approval, or >1 backward stage move",
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysInStage",
                    "rules": [
                        {"value": 90, "color": "#D4504C", "operator": "gte"},
                        {"value": 60, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "BackwardMoveCount",
                    "rules": [
                        {"value": 2, "color": "#D4504C", "operator": "gte"},
                    ],
                },
            ],
        ),
    }

    # Subtitle for waterfall bridge on Summary page
    widgets["p1_ch_bridge"]["parameters"]["title"]["subtitleLabel"] = (
        "Decomposes gap: Plan → Closed Won → +Weighted Open → -At Risk Commit → = Projected"
    )

    add_table_action(widgets["p2_tbl_risk"], "salesforceActions", "Opportunity", "Id")
    add_table_action(
        widgets["p2_tbl_process"], "salesforceActions", "Opportunity", "Id"
    )
    return widgets


def build_layout() -> dict:
    """Build grid layout for the 2-page executive dashboard."""
    p1 = nav_row("p1", 2) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p1_n_actual", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_projected", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_gap", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_confidence", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        {
            "name": "p1_section_forecast",
            "row": 9,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p1_ch_timeline", "row": 10, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_bridge", "row": 18, "column": 0, "colspan": 5, "rowspan": 6},
        {"name": "p1_ch_unit", "row": 18, "column": 5, "colspan": 7, "rowspan": 6},
    ]

    p2 = nav_row("p2", 2) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        {
            "name": "p2_section_pipeline",
            "row": 5,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p2_ch_quarter", "row": 6, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p2_ch_velocity", "row": 6, "column": 6, "colspan": 6, "rowspan": 6},
        {
            "name": "p2_section_risk",
            "row": 12,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p2_ch_region", "row": 13, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p2_ch_push", "row": 13, "column": 6, "colspan": 6, "rowspan": 6},
        {
            "name": "p2_section_deals",
            "row": 19,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p2_tbl_risk", "row": 20, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p2_tbl_process", "row": 27, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "ExecutiveRevenueForecast",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("drivers", "Drivers & Risks", p2),
        ],
    }


def main() -> None:
    instance_url, token = get_auth()
    if not create_dataset(instance_url, token):
        raise SystemExit("Dataset upload failed")

    dataset_id = get_dataset_id(instance_url, token, DS)
    if not dataset_id:
        raise SystemExit(f"Could not resolve dataset id for {DS}")
    if not get_dataset_id(instance_url, token, PIPELINE_DS):
        raise SystemExit(f"Could not resolve dataset id for {PIPELINE_DS}")

    steps = build_steps(dataset_id)
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

    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    print(f"\n=== Deploying {DASHBOARD_LABEL} ===")
    deploy_dashboard(instance_url, token, dashboard_id, state)

    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "OpportunityName", "id_field": "Id", "label": "Opportunity"},
            {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
        ],
    )


if __name__ == "__main__":
    main()
