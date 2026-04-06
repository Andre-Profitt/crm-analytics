#!/usr/bin/env python3
"""Build the Pipeline & Opportunity Operations dashboard.

This is the Wave 1 manager dashboard that consolidates:
- Opp Management
- Advanced Pipeline Analytics
- Sales Process Compliance KPIs
- Pipeline History

Design goals:
- 4-page manager surface with operational depth
- native forecast trajectory with confidence bands
- stage velocity, close-date push, and backward-move analytics
- exception queues with direct Salesforce record actions
"""

from __future__ import annotations

import csv
import io
import re
from collections import defaultdict
from datetime import UTC, date, datetime

from crm_analytics_helpers import (
    KPI_CARD_STYLE,
    _date,
    _dim,
    _measure,
    _soql,
    add_table_action,
    af,
    bullet_chart,
    bubble_chart,
    build_dashboard_state,
    coalesce_filter,
    combo_chart,
    compare_table,
    create_dashboard_if_needed,
    deploy_dashboard,
    funnel_chart,
    get_auth,
    get_dataset_id,
    hdr,
    heatmap_chart,
    line_chart,
    nav_link,
    nav_row,
    num,
    pg,
    pillbox,
    precompute_scoring_stats,
    compute_win_score,
    rich_chart,
    sankey_chart,
    section_label,
    set_record_links_xmd,
    sq,
    timeline_chart,
    upload_dataset,
    waterfall_chart,
)
from portfolio_foundation import (
    coerce_bool,
    current_fy_label,
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
from crm_analytics_runtime import builder_run  # pyright: ignore[reportMissingImports]
from simcorp_fields import assert_org_schema  # pyright: ignore[reportMissingImports]

import logging

logger = logging.getLogger(__name__)

DS = "Pipeline_Opportunity_Operations"
DS_LABEL = "Pipeline Opportunity Operations"
DASHBOARD_LABEL = "Pipeline & Opportunity Operations"
CURRENT_FY_LABEL = current_fy_label()

KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_unit", "f_fy", "f_region"],
    },
}

SOQL = (
    "SELECT Id, Name, Owner.Name, AccountId, Account.Name, "
    "Account_Unit_Group__c, Sales_Region__c, ForecastCategoryName, "
    "IsClosed, IsWon, CloseDate, StageName, Type, LeadSource, CreatedDate, "
    "FiscalYear, FiscalQuarter, "
    "APTS_Forecast_ARR__c, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
    "Amount, Probability, AgeInDays, LastStageChangeInDays, "
    "Sales_Cycle_Duration__c, "
    "Stage_20_Approval__c, Stage_20_Approval_Date__c, "
    "Reason_Won_Lost__c, Sub_Reason__c, Quota_Amount__c, "
    "Account.Risk_of_Potential_Termination__c "
    "FROM Opportunity "
    "WHERE FiscalYear IN (2025, 2026, 2027)"
)


def _add_months(month_key_value: str, offset: int) -> str:
    """Add offset months to a YYYY-MM string."""
    dt = datetime.strptime(f"{month_key_value}-01", "%Y-%m-%d")
    month = dt.month - 1 + offset
    year = dt.year + month // 12
    month = month % 12 + 1
    return f"{year:04d}-{month:02d}"


def _rebind(binding: str, alias: str) -> str:
    """Retarget a coalesce_filter binding to a specific SAQL alias."""
    return (
        binding.replace("q =", f"{alias} =")
        .replace("q by", f"{alias} by")
        .replace("q generate", f"{alias} generate")
    )


def _parse_date(value: str) -> date | None:
    """Parse the YYYY-MM-DD prefix from an ISO-ish value."""
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _days_between(start_value: str, end_value: str) -> int:
    """Return whole days between two ISO dates."""
    start_dt = _parse_date(start_value)
    end_dt = _parse_date(end_value)
    if not start_dt or not end_dt:
        return 0
    return max(0, (end_dt - start_dt).days)


def _days_until(close_date: str, today: date) -> int:
    """Return days from today until close date."""
    close_dt = _parse_date(close_date)
    if not close_dt:
        return 0
    return (close_dt - today).days


def _stage_order(stage_name: str) -> str:
    """Extract the numeric prefix from the org's stage labels.

    Returns a zero-padded string so it works as a SAQL group-by dimension.
    """
    value = (stage_name or "").strip()
    if not value:
        return "00"
    match = re.match(r"^(\d+)", value)
    if match:
        return f"{int(match.group(1)):02d}"
    lowered = value.lower()
    if "won" in lowered:
        return "08"
    if "lost" in lowered or "no opportunity" in lowered:
        return "00"
    return "00"


def _stage_band(stage_order: str | int) -> str:
    """Compress stage depth into a smaller operating vocabulary."""
    order = int(stage_order)
    if order <= 1:
        return "Qualify"
    if order <= 3:
        return "Shape"
    if order <= 5:
        return "Validate"
    if order <= 7:
        return "Commit"
    return "Closed"


def _stage_sla_days(stage_order: str | int) -> int:
    """Expected maximum dwell time by stage."""
    order = int(stage_order)
    return {
        0: 0,
        1: 14,
        2: 18,
        3: 21,
        4: 24,
        5: 21,
        6: 18,
        7: 14,
        8: 0,
    }.get(order, 21)


def _risk_band(score: float) -> str:
    """Map risk score to an operating band."""
    if score >= 80:
        return "Critical"
    if score >= 65:
        return "High"
    if score >= 45:
        return "Medium"
    return "Low"


def _category_rank(category: str) -> int:
    """Order forecast categories from strongest to weakest."""
    value = (category or "").strip().lower().replace(" ", "")
    if value == "closed":
        return 5
    if value == "commit":
        return 4
    if value == "bestcase":
        return 3
    if value == "pipeline":
        return 2
    if value == "omitted":
        return 1
    return 0


def _slip_risk_score(
    stage_order: int,
    stage_sla_days: int,
    days_in_stage: float,
    days_to_close: int,
    forecast_category: str,
    age_in_days: float,
    push_count: int,
) -> float:
    """Heuristic close-date slip risk."""
    score = 0.0
    if days_to_close < 0:
        score += 40.0
    elif days_to_close <= 14 and stage_order < 5:
        score += 25.0
    elif days_to_close <= 30 and stage_order < 4:
        score += 15.0

    if stage_sla_days > 0:
        score += min(25.0, max(0.0, days_in_stage - stage_sla_days) * 1.4)

    value = (forecast_category or "").strip().lower().replace(" ", "")
    if value == "omitted":
        score += 15.0
    elif value == "pipeline":
        score += 10.0

    if age_in_days >= 180:
        score += 15.0
    elif age_in_days >= 120:
        score += 8.0

    if push_count >= 3:
        score += 15.0
    elif push_count >= 1:
        score += 8.0

    return round(max(0.0, min(100.0, score)), 1)


def _process_risk_score(
    stage_order: int,
    stage_sla_days: int,
    days_in_stage: float,
    needs_approval: bool,
    backward_moves: int,
    forecast_downgrades: int,
) -> float:
    """Heuristic process-compliance risk."""
    score = 0.0
    if needs_approval:
        score += 35.0
    if stage_sla_days > 0 and days_in_stage >= stage_sla_days * 1.5:
        score += 20.0
    if backward_moves >= 2:
        score += 20.0
    elif backward_moves == 1:
        score += 12.0
    if forecast_downgrades >= 2:
        score += 18.0
    elif forecast_downgrades == 1:
        score += 10.0
    if stage_order <= 2 and days_in_stage > 30:
        score += 8.0
    return round(max(0.0, min(100.0, score)), 1)


def _exception_type(
    is_past_due: bool,
    needs_approval: bool,
    is_stale: bool,
    push_count: int,
    backward_moves: int,
    slip_risk: float,
    process_risk: float,
) -> str:
    """Select the most important operational exception for the record."""
    if is_past_due:
        return "Past Due Close"
    if needs_approval:
        return "Approval Gap"
    if is_stale:
        return "Stage Stale"
    if backward_moves >= 2:
        return "Backward Movement"
    if push_count >= 2:
        return "Repeated Pushes"
    if slip_risk >= 70:
        return "Slip Risk"
    if process_risk >= 65:
        return "Process Risk"
    return "Monitor"


def _opp_context(opp: dict) -> dict[str, object]:
    """Return the shared dimension values for an opportunity."""
    acct = opp.get("Account") or {}
    owner = opp.get("Owner") or {}
    stage_name = opp.get("StageName") or ""
    fiscal_year = int(safe_float(opp.get("FiscalYear"), 0))
    fiscal_quarter = int(safe_float(opp.get("FiscalQuarter"), 0))
    stage_order = _stage_order(stage_name)
    return {
        "Id": opp.get("Id", ""),
        "OpportunityName": (opp.get("Name") or "")[:255],
        "AccountId": opp.get("AccountId", ""),
        "AccountName": (acct.get("Name") or "")[:255],
        "OwnerName": (owner.get("Name") or "Unknown")[:255],
        "UnitGroup": (
            (opp.get("Account_Unit_Group__c") or "Unassigned").strip() or "Unassigned"
        )[:255],
        "SalesRegion": (
            (opp.get("Sales_Region__c") or "Unassigned").strip() or "Unassigned"
        )[:255],
        "MotionType": normalize_motion(opp.get("Type") or ""),
        "ForecastCategory": (opp.get("ForecastCategoryName") or "Pipeline")[:255],
        "StageName": stage_name[:255],
        "StageBand": _stage_band(stage_order),
        "WonLostReason": (opp.get("Reason_Won_Lost__c") or "")[:255],
        "FYLabel": fiscal_label(fiscal_year),
        "CloseQuarter": f"Q{fiscal_quarter}" if fiscal_quarter else "",
        "FiscalYear": fiscal_year,
        "FiscalQuarter": fiscal_quarter,
        "StageOrder": stage_order,
    }


def create_dataset(inst: str, tok: str) -> bool:
    """Build the unified pipeline operations dataset."""
    logger.info("\n=== Building %s dataset ===", DS_LABEL)
    opps = _soql(inst, tok, SOQL)
    logger.info("  Queried %d opportunities", len(opps))
    if not opps:
        raise RuntimeError(
            "No opportunities returned for the pipeline operations dataset."
        )

    opp_by_id = {opp.get("Id"): opp for opp in opps if opp.get("Id")}
    type_win_rates, avg_deal_size = precompute_scoring_stats(opps)
    current_month = datetime.now(UTC).strftime("%Y-%m")
    today = datetime.now(UTC).date()

    push_stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {"push_count": 0.0, "push_days": 0.0, "downgrades": 0.0}
    )
    stage_stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {"backward": 0.0, "transition_days": 0.0, "transition_count": 0.0}
    )
    field_event_rows: list[dict[str, object]] = []
    stage_event_rows: list[dict[str, object]] = []

    try:
        field_history = _soql(
            inst,
            tok,
            "SELECT OpportunityId, Field, OldValue, NewValue, CreatedDate "
            "FROM OpportunityFieldHistory "
            "WHERE CreatedDate >= 2025-01-01T00:00:00Z "
            "AND Field IN ('CloseDate', 'ForecastCategoryName')",
        )
        logger.info("  Queried %d opportunity field history rows", len(field_history))
        for record in field_history:
            opp_id = record.get("OpportunityId")
            opp = opp_by_id.get(opp_id)
            if not opp:
                continue

            field_name = record.get("Field") or ""
            old_value = str(record.get("OldValue") or "")[:255]
            new_value = str(record.get("NewValue") or "")[:255]
            created_date = (record.get("CreatedDate") or "")[:10]
            event_month = created_date[:7]
            event_month_date = f"{event_month}-01" if len(event_month) == 7 else ""

            push_count = 0
            push_days = 0.0
            downgrade_count = 0
            exception_type = ""

            if field_name == "CloseDate" and old_value and new_value:
                old_dt = _parse_date(old_value)
                new_dt = _parse_date(new_value)
                if old_dt and new_dt:
                    delta_days = (new_dt - old_dt).days
                    if delta_days > 0:
                        push_count = 1
                        push_days = float(delta_days)
                        push_stats[opp_id]["push_count"] += 1.0
                        push_stats[opp_id]["push_days"] += float(delta_days)
                        exception_type = "Close Date Push"

            if field_name == "ForecastCategoryName":
                if _category_rank(new_value) < _category_rank(old_value):
                    downgrade_count = 1
                    push_stats[opp_id]["downgrades"] += 1.0
                    exception_type = "Forecast Downgrade"

            if not push_count and not downgrade_count:
                continue

            context = _opp_context(opp)
            field_event_rows.append(
                {
                    "RecordType": "field_history",
                    **context,
                    "RiskBand": "",
                    "WinScoreBand": "",
                    "ExceptionType": exception_type,
                    "EventField": field_name,
                    "PrevStage": "",
                    "IsClosed": str(coerce_bool(opp.get("IsClosed"))).lower(),
                    "IsWon": str(coerce_bool(opp.get("IsWon"))).lower(),
                    "IsPastDue": "false",
                    "IsStale": "false",
                    "NeedsApproval": "false",
                    "CloseDate": (opp.get("CloseDate") or "")[:10],
                    "CreatedDate": (opp.get("CreatedDate") or "")[:10],
                    "MonthDate": "",
                    "MonthLabel": "",
                    "EventDate": created_date,
                    "EventMonth": event_month,
                    "EventMonthDate": event_month_date,
                    "StageSlaDays": 0,
                    "ARR": 0.0,
                    "WeightedOpenARR": 0.0,
                    "ExpectedARR": 0.0,
                    "ActualARR": 0.0,
                    "QuotaAmount": 0.0,
                    "QuotaContrib": 0.0,
                    "Probability": 0.0,
                    "AgeInDays": 0.0,
                    "DaysInStage": 0.0,
                    "SalesCycleDuration": 0.0,
                    "DaysToClose": 0.0,
                    "WinScore": 0.0,
                    "SlipRiskScore": 0.0,
                    "ProcessRiskScore": 0.0,
                    "TotalRiskScore": 0.0,
                    "HygieneScore": 0.0,
                    "RiskWeightedARR": 0.0,
                    "AtRiskARR": 0.0,
                    "PastDueARR": 0.0,
                    "StaleARR": 0.0,
                    "MissingApprovalARR": 0.0,
                    "PastDueCount": 0,
                    "StaleCount": 0,
                    "MissingApprovalCount": 0,
                    "PushCount": push_count,
                    "PushDays": round(push_days, 1),
                    "ForecastDowngradeCount": downgrade_count,
                    "BackwardMoveCount": 0,
                    "TransitionDays": 0.0,
                    "CriticalExceptionCount": push_count + downgrade_count,
                    "OpportunityCount": 1,
                    "RegressionForecastARR": 0.0,
                    "RegressionUpperARR": 0.0,
                    "RegressionLowerARR": 0.0,
                    "TargetARR": 0.0,
                }
            )
    except Exception as exc:
        logger.warning(
            "  OpportunityFieldHistory unavailable, skipping push analytics: %s", exc
        )

    try:
        stage_history = _soql(
            inst,
            tok,
            "SELECT OpportunityId, StageName, Amount, CloseDate, CreatedDate "
            "FROM OpportunityHistory "
            "WHERE CreatedDate >= 2025-01-01T00:00:00Z "
            "ORDER BY OpportunityId, CreatedDate ASC",
        )
        logger.info("  Queried %d opportunity history rows", len(stage_history))
        prev_by_opp: dict[str, dict[str, object]] = {}
        for record in stage_history:
            opp_id = record.get("OpportunityId")
            opp = opp_by_id.get(opp_id)
            if not opp:
                continue

            stage_name = (record.get("StageName") or "")[:255]
            created_date = (record.get("CreatedDate") or "")[:10]
            stage_num = _stage_order(stage_name)
            prev = prev_by_opp.get(opp_id)
            if prev:
                transition_days = _days_between(str(prev["created_date"]), created_date)
                backward = (
                    1
                    if stage_num < int(prev["stage_num"]) and int(prev["stage_num"]) > 0
                    else 0
                )
                if backward:
                    stage_stats[opp_id]["backward"] += 1.0
                if transition_days > 0:
                    stage_stats[opp_id]["transition_days"] += float(transition_days)
                    stage_stats[opp_id]["transition_count"] += 1.0

                event_month = created_date[:7]
                event_month_date = f"{event_month}-01" if len(event_month) == 7 else ""
                context = _opp_context(opp)
                stage_event_rows.append(
                    {
                        "RecordType": "stage_history",
                        **context,
                        "RiskBand": "",
                        "WinScoreBand": "",
                        "ExceptionType": "Backward Move"
                        if backward
                        else "Stage Transition",
                        "EventField": "StageName",
                        "PrevStage": str(prev["stage"])[:255],
                        "IsClosed": str(coerce_bool(opp.get("IsClosed"))).lower(),
                        "IsWon": str(coerce_bool(opp.get("IsWon"))).lower(),
                        "IsPastDue": "false",
                        "IsStale": "false",
                        "NeedsApproval": "false",
                        "CloseDate": (opp.get("CloseDate") or "")[:10],
                        "CreatedDate": (opp.get("CreatedDate") or "")[:10],
                        "MonthDate": "",
                        "MonthLabel": "",
                        "EventDate": created_date,
                        "EventMonth": event_month,
                        "EventMonthDate": event_month_date,
                        "StageSlaDays": 0,
                        "ARR": 0.0,
                        "WeightedOpenARR": 0.0,
                        "ExpectedARR": 0.0,
                        "ActualARR": 0.0,
                        "QuotaAmount": 0.0,
                        "QuotaContrib": 0.0,
                        "Probability": 0.0,
                        "AgeInDays": 0.0,
                        "DaysInStage": 0.0,
                        "SalesCycleDuration": 0.0,
                        "DaysToClose": 0.0,
                        "WinScore": 0.0,
                        "SlipRiskScore": 0.0,
                        "ProcessRiskScore": 0.0,
                        "TotalRiskScore": 0.0,
                        "HygieneScore": 0.0,
                        "RiskWeightedARR": 0.0,
                        "AtRiskARR": 0.0,
                        "PastDueARR": 0.0,
                        "StaleARR": 0.0,
                        "MissingApprovalARR": 0.0,
                        "PastDueCount": 0,
                        "StaleCount": 0,
                        "MissingApprovalCount": 0,
                        "PushCount": 0,
                        "PushDays": 0.0,
                        "ForecastDowngradeCount": 0,
                        "BackwardMoveCount": backward,
                        "TransitionDays": transition_days,
                        "CriticalExceptionCount": backward,
                        "OpportunityCount": 1,
                        "RegressionForecastARR": 0.0,
                        "RegressionUpperARR": 0.0,
                        "RegressionLowerARR": 0.0,
                        "TargetARR": 0.0,
                    }
                )

            prev_by_opp[opp_id] = {
                "stage": stage_name,
                "stage_num": stage_num,
                "created_date": created_date,
            }
    except Exception as exc:
        logger.warning(
            "  OpportunityHistory unavailable, skipping backward-move analytics: %s",
            exc,
        )

    detail_rows: list[dict[str, object]] = []
    grouped_monthly: dict[tuple[str, str, str], dict[str, dict[str, float]]] = (
        defaultdict(
            lambda: defaultdict(
                lambda: {
                    "ActualARR": 0.0,
                    "WeightedOpenARR": 0.0,
                    "RiskWeightedARR": 0.0,
                    "PastDueARR": 0.0,
                    "StaleARR": 0.0,
                }
            )
        )
    )
    owner_quota: dict[tuple[str, int, str, str], float] = {}
    min_month = ""
    max_month = current_month

    for opp in opps:
        context = _opp_context(opp)
        opp_id = str(context["Id"])
        close_date = (opp.get("CloseDate") or "")[:10]
        close_month = month_key(close_date)
        if close_month:
            min_month = close_month if not min_month else min(min_month, close_month)
            max_month = max(max_month, close_month)

        stage_order = int(context["StageOrder"])
        stage_sla_days = _stage_sla_days(stage_order)
        arr = round(
            safe_float(opp.get("ConvertedARR") or opp.get("APTS_Forecast_ARR__c")), 2
        )
        probability = round(safe_float(opp.get("Probability")), 1)
        age_in_days = round(safe_float(opp.get("AgeInDays")), 1)
        days_in_stage = round(safe_float(opp.get("LastStageChangeInDays")), 1)
        sales_cycle = round(safe_float(opp.get("Sales_Cycle_Duration__c")), 1)
        quota_amount = round(safe_float(opp.get("Quota_Amount__c")), 2)
        is_closed = coerce_bool(opp.get("IsClosed"))
        is_won = coerce_bool(opp.get("IsWon"))
        days_to_close = _days_until(close_date, today) if close_date else 0
        needs_approval = (
            not is_closed
            and stage_order >= 3
            and not coerce_bool(opp.get("Stage_20_Approval__c"))
        )
        is_past_due = not is_closed and close_date and days_to_close < 0
        is_stale = (
            not is_closed
            and stage_sla_days > 0
            and days_in_stage > stage_sla_days * 1.35
        )
        risk_level = (
            (opp.get("Account") or {}).get("Risk_of_Potential_Termination__c") or ""
        ).strip() or "Low"

        push_count = int(push_stats[opp_id]["push_count"])
        push_days = round(push_stats[opp_id]["push_days"], 1)
        forecast_downgrades = int(push_stats[opp_id]["downgrades"])
        backward_moves = int(stage_stats[opp_id]["backward"])

        win_score, win_band = compute_win_score(opp, type_win_rates, avg_deal_size)
        weight = (
            1.0
            if is_won
            else (
                0.0
                if is_closed
                else forecast_weight(str(context["ForecastCategory"]), probability)
            )
        )
        weighted_open = round(0.0 if is_closed else arr * weight, 2)
        expected_arr = round(
            arr
            if is_won
            else (0.0 if is_closed else arr * max(probability, 10.0) / 100.0),
            2,
        )
        slip_risk = _slip_risk_score(
            stage_order,
            stage_sla_days,
            days_in_stage,
            days_to_close,
            str(context["ForecastCategory"]),
            age_in_days,
            push_count,
        )
        process_risk = _process_risk_score(
            stage_order,
            stage_sla_days,
            days_in_stage,
            needs_approval,
            backward_moves,
            forecast_downgrades,
        )
        total_risk = round(
            max(
                risk_level_to_score(risk_level),
                100.0 - float(win_score),
                slip_risk,
                process_risk,
            ),
            1,
        )
        hygiene_penalty = (slip_risk * 0.35) + (process_risk * 0.30)
        if is_past_due:
            hygiene_penalty += 20.0
        if is_stale:
            hygiene_penalty += 12.0
        hygiene_score = round(max(0.0, min(100.0, 100.0 - hygiene_penalty)), 1)

        at_risk_arr = round(weighted_open if total_risk >= 65.0 else 0.0, 2)
        risk_weighted_arr = round(weighted_open * total_risk / 100.0, 2)
        past_due_arr = round(weighted_open if is_past_due else 0.0, 2)
        stale_arr = round(weighted_open if is_stale else 0.0, 2)
        missing_approval_arr = round(weighted_open if needs_approval else 0.0, 2)
        exception_type = _exception_type(
            is_past_due,
            needs_approval,
            is_stale,
            push_count,
            backward_moves,
            slip_risk,
            process_risk,
        )
        critical_exception = (
            1
            if (not is_closed and (total_risk >= 80.0 or is_past_due or needs_approval))
            else 0
        )

        detail_rows.append(
            {
                "RecordType": "detail",
                **context,
                "RiskBand": _risk_band(total_risk),
                "WinScoreBand": win_band,
                "ExceptionType": exception_type,
                "EventField": "",
                "PrevStage": "",
                "IsClosed": str(is_closed).lower(),
                "IsWon": str(is_won).lower(),
                "IsPastDue": str(is_past_due).lower(),
                "IsStale": str(is_stale).lower(),
                "NeedsApproval": str(needs_approval).lower(),
                "CloseDate": close_date,
                "CreatedDate": (opp.get("CreatedDate") or "")[:10],
                "MonthDate": month_start(close_date),
                "MonthLabel": close_month,
                "EventDate": "",
                "EventMonth": "",
                "EventMonthDate": "",
                "StageSlaDays": stage_sla_days,
                "ARR": arr,
                "WeightedOpenARR": weighted_open,
                "ExpectedARR": expected_arr,
                "ActualARR": arr if is_won else 0.0,
                "QuotaAmount": quota_amount,
                "Probability": probability,
                "AgeInDays": age_in_days,
                "DaysInStage": days_in_stage,
                "SalesCycleDuration": sales_cycle,
                "DaysToClose": float(days_to_close),
                "WinScore": round(float(win_score), 1),
                "SlipRiskScore": slip_risk,
                "ProcessRiskScore": process_risk,
                "TotalRiskScore": total_risk,
                "HygieneScore": hygiene_score,
                "RiskWeightedARR": risk_weighted_arr,
                "AtRiskARR": at_risk_arr,
                "PastDueARR": past_due_arr,
                "StaleARR": stale_arr,
                "MissingApprovalARR": missing_approval_arr,
                "PastDueCount": 1 if is_past_due else 0,
                "StaleCount": 1 if is_stale else 0,
                "MissingApprovalCount": 1 if needs_approval else 0,
                "PushCount": push_count,
                "PushDays": push_days,
                "ForecastDowngradeCount": forecast_downgrades,
                "BackwardMoveCount": backward_moves,
                "TransitionDays": round(
                    stage_stats[opp_id]["transition_days"]
                    / stage_stats[opp_id]["transition_count"],
                    1,
                )
                if stage_stats[opp_id]["transition_count"] > 0
                else 0.0,
                "CriticalExceptionCount": critical_exception,
                "OpportunityCount": 1,
                "RegressionForecastARR": 0.0,
                "RegressionUpperARR": 0.0,
                "RegressionLowerARR": 0.0,
                "TargetARR": 0.0,
            }
        )

        if context["FiscalYear"]:
            quota_key = (
                str(context["OwnerName"]),
                int(context["FiscalYear"]),
                str(context["UnitGroup"]),
                str(context["SalesRegion"]),
            )
            if quota_amount > owner_quota.get(quota_key, 0.0):
                owner_quota[quota_key] = quota_amount

        if close_month:
            trend_key = (
                str(context["UnitGroup"]),
                str(context["SalesRegion"]),
                str(context["FYLabel"]),
            )
            bucket = grouped_monthly[trend_key][close_month]
            if is_won:
                bucket["ActualARR"] += arr
            elif not is_closed:
                bucket["WeightedOpenARR"] += weighted_open
                bucket["RiskWeightedARR"] += risk_weighted_arr
                bucket["PastDueARR"] += past_due_arr
                bucket["StaleARR"] += stale_arr

    # ----- Deduplicate quota: only first opp per (owner, FY, unit, region) -----
    # contributes quota; rest get 0.  This makes sum(QuotaContrib) correct.
    _seen_quota_keys: set[tuple[str, str, str, str]] = set()
    for row in detail_rows:
        qk = (
            str(row["OwnerName"]),
            str(row.get("FiscalYear", "")),
            str(row["UnitGroup"]),
            str(row["SalesRegion"]),
        )
        if qk not in _seen_quota_keys and row.get("FiscalYear"):
            fy_int = int(row["FiscalYear"]) if row["FiscalYear"] else 0
            row["QuotaContrib"] = owner_quota.get(
                (
                    str(row["OwnerName"]),
                    fy_int,
                    str(row["UnitGroup"]),
                    str(row["SalesRegion"]),
                ),
                0.0,
            )
            _seen_quota_keys.add(qk)
        else:
            row["QuotaContrib"] = 0.0

    if not min_month:
        raise RuntimeError(
            "No opportunity close-month data available for the pipeline operations dataset."
        )

    quota_by_segment: dict[tuple[str, str, str], float] = defaultdict(float)
    for (
        owner_name,
        fiscal_year,
        unit_group,
        sales_region,
    ), quota in owner_quota.items():
        del owner_name
        quota_by_segment[(unit_group, sales_region, fiscal_label(fiscal_year))] += quota

    trend_rows: list[dict[str, object]] = []
    forecast_end_month = _add_months(max(max_month, current_month), 3)
    months = month_sequence(min_month, forecast_end_month)
    historical_months = [month for month in months if month <= current_month]

    for (unit_group, sales_region, fy_label), monthly_data in grouped_monthly.items():
        actual_series = [
            monthly_data[month]["ActualARR"] for month in historical_months
        ]
        fit = least_squares(actual_series)
        target_monthly = round(
            quota_by_segment.get((unit_group, sales_region, fy_label), 0.0) / 12.0, 2
        )

        for index, month in enumerate(months):
            values = monthly_data[month]
            forecast = max(0.0, fit["intercept"] + fit["slope"] * index)
            interval = prediction_interval(fit, index)
            year_value = int(month[:4])

            trend_rows.append(
                {
                    "RecordType": "trend",
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
                    "StageBand": "",
                    "WonLostReason": "",
                    "RiskBand": "",
                    "WinScoreBand": "",
                    "ExceptionType": "",
                    "EventField": "",
                    "PrevStage": "",
                    "IsClosed": "false",
                    "IsWon": "false",
                    "IsPastDue": "false",
                    "IsStale": "false",
                    "NeedsApproval": "false",
                    "CloseDate": "",
                    "CreatedDate": "",
                    "MonthDate": f"{month}-01",
                    "MonthLabel": month,
                    "EventDate": "",
                    "EventMonth": "",
                    "EventMonthDate": "",
                    "FYLabel": fy_label,
                    "CloseQuarter": "",
                    "FiscalYear": year_value,
                    "FiscalQuarter": 0,
                    "StageOrder": "00",
                    "StageSlaDays": 0,
                    "ARR": 0.0,
                    "WeightedOpenARR": round(values["WeightedOpenARR"], 2),
                    "ExpectedARR": 0.0,
                    "ActualARR": round(values["ActualARR"], 2),
                    "QuotaAmount": 0.0,
                    "QuotaContrib": 0.0,
                    "Probability": 0.0,
                    "AgeInDays": 0.0,
                    "DaysInStage": 0.0,
                    "SalesCycleDuration": 0.0,
                    "DaysToClose": 0.0,
                    "WinScore": 0.0,
                    "SlipRiskScore": 0.0,
                    "ProcessRiskScore": 0.0,
                    "TotalRiskScore": 0.0,
                    "HygieneScore": 0.0,
                    "RiskWeightedARR": round(values["RiskWeightedARR"], 2),
                    "AtRiskARR": round(values["RiskWeightedARR"], 2),
                    "PastDueARR": round(values["PastDueARR"], 2),
                    "StaleARR": round(values["StaleARR"], 2),
                    "MissingApprovalARR": 0.0,
                    "PastDueCount": 0,
                    "StaleCount": 0,
                    "MissingApprovalCount": 0,
                    "PushCount": 0,
                    "PushDays": 0.0,
                    "ForecastDowngradeCount": 0,
                    "BackwardMoveCount": 0,
                    "TransitionDays": 0.0,
                    "CriticalExceptionCount": 0,
                    "OpportunityCount": 0,
                    "RegressionForecastARR": round(forecast, 2),
                    "RegressionUpperARR": round(max(0.0, forecast + interval), 2),
                    "RegressionLowerARR": round(max(0.0, forecast - interval), 2),
                    "TargetARR": target_monthly,
                }
            )

    rows = detail_rows + trend_rows + field_event_rows + stage_event_rows
    logger.info("  Detail rows: %d", len(detail_rows))
    logger.info("  Trend rows: %d", len(trend_rows))
    logger.info("  Field event rows: %d", len(field_event_rows))
    logger.info("  Stage event rows: %d", len(stage_event_rows))
    logger.info("  Total rows: %d", len(rows))

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
        "StageBand",
        "WonLostReason",
        "RiskBand",
        "WinScoreBand",
        "ExceptionType",
        "EventField",
        "PrevStage",
        "IsClosed",
        "IsWon",
        "IsPastDue",
        "IsStale",
        "NeedsApproval",
        "CloseDate",
        "CreatedDate",
        "MonthDate",
        "MonthLabel",
        "EventDate",
        "EventMonth",
        "EventMonthDate",
        "FYLabel",
        "CloseQuarter",
        "FiscalYear",
        "FiscalQuarter",
        "StageOrder",
        "StageSlaDays",
        "ARR",
        "WeightedOpenARR",
        "ExpectedARR",
        "ActualARR",
        "QuotaAmount",
        "QuotaContrib",
        "Probability",
        "AgeInDays",
        "DaysInStage",
        "SalesCycleDuration",
        "DaysToClose",
        "WinScore",
        "SlipRiskScore",
        "ProcessRiskScore",
        "TotalRiskScore",
        "HygieneScore",
        "RiskWeightedARR",
        "AtRiskARR",
        "PastDueARR",
        "StaleARR",
        "MissingApprovalARR",
        "PastDueCount",
        "StaleCount",
        "MissingApprovalCount",
        "PushCount",
        "PushDays",
        "ForecastDowngradeCount",
        "BackwardMoveCount",
        "TransitionDays",
        "CriticalExceptionCount",
        "OpportunityCount",
        "RegressionForecastARR",
        "RegressionUpperARR",
        "RegressionLowerARR",
        "TargetARR",
    ]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=field_names, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    csv_bytes = buffer.getvalue().encode("utf-8")
    logger.info("  CSV: %s bytes", f"{len(csv_bytes):,}")

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
        _dim("StageBand", "Stage Band"),
        _dim("WonLostReason", "Won/Lost Reason"),
        _dim("RiskBand", "Risk Band"),
        _dim("WinScoreBand", "Win Score Band"),
        _dim("ExceptionType", "Exception Type"),
        _dim("EventField", "Event Field"),
        _dim("PrevStage", "Previous Stage"),
        _dim("IsClosed", "Is Closed"),
        _dim("IsWon", "Is Won"),
        _dim("IsPastDue", "Is Past Due"),
        _dim("IsStale", "Is Stale"),
        _dim("NeedsApproval", "Needs Approval"),
        _date("CloseDate", "Close Date"),
        _date("CreatedDate", "Created Date"),
        _date("MonthDate", "Month"),
        _dim("MonthLabel", "Month Label"),
        _date("EventDate", "Event Date"),
        _dim("EventMonth", "Event Month"),
        _date("EventMonthDate", "Event Month Date"),
        _dim("FYLabel", "Fiscal Year Label"),
        _dim("CloseQuarter", "Close Quarter"),
        _measure("FiscalYear", "Fiscal Year", scale=0, precision=5),
        _measure("FiscalQuarter", "Fiscal Quarter", scale=0, precision=3),
        _dim("StageOrder", "Stage Order"),
        _measure("StageSlaDays", "Stage SLA Days", scale=0, precision=4),
        _measure("ARR", "ARR"),
        _measure("WeightedOpenARR", "Weighted Open ARR"),
        _measure("ExpectedARR", "Expected ARR"),
        _measure("ActualARR", "Actual ARR"),
        _measure("QuotaAmount", "Quota Amount"),
        _measure("QuotaContrib", "Quota Contribution"),
        _measure("Probability", "Probability", scale=1, precision=5),
        _measure("AgeInDays", "Age In Days", scale=1, precision=6),
        _measure("DaysInStage", "Days In Stage", scale=1, precision=6),
        _measure("SalesCycleDuration", "Sales Cycle Duration", scale=1, precision=6),
        _measure("DaysToClose", "Days To Close", scale=0, precision=6),
        _measure("WinScore", "Win Score", scale=1, precision=5),
        _measure("SlipRiskScore", "Slip Risk Score", scale=1, precision=5),
        _measure("ProcessRiskScore", "Process Risk Score", scale=1, precision=5),
        _measure("TotalRiskScore", "Total Risk Score", scale=1, precision=5),
        _measure("HygieneScore", "Hygiene Score", scale=1, precision=5),
        _measure("RiskWeightedARR", "Risk-Weighted ARR"),
        _measure("AtRiskARR", "At-Risk ARR"),
        _measure("PastDueARR", "Past Due ARR"),
        _measure("StaleARR", "Stale ARR"),
        _measure("MissingApprovalARR", "Missing Approval ARR"),
        _measure("PastDueCount", "Past Due Count", scale=0, precision=6),
        _measure("StaleCount", "Stale Count", scale=0, precision=6),
        _measure(
            "MissingApprovalCount", "Missing Approval Count", scale=0, precision=6
        ),
        _measure("PushCount", "Push Count", scale=0, precision=6),
        _measure("PushDays", "Push Days", scale=1, precision=6),
        _measure(
            "ForecastDowngradeCount", "Forecast Downgrade Count", scale=0, precision=6
        ),
        _measure("BackwardMoveCount", "Backward Move Count", scale=0, precision=6),
        _measure("TransitionDays", "Transition Days", scale=1, precision=6),
        _measure(
            "CriticalExceptionCount", "Critical Exception Count", scale=0, precision=6
        ),
        _measure("OpportunityCount", "Opportunity Count", scale=0, precision=6),
        _measure("RegressionForecastARR", "Regression Forecast ARR"),
        _measure("RegressionUpperARR", "Regression Upper ARR"),
        _measure("RegressionLowerARR", "Regression Lower ARR"),
        _measure("TargetARR", "Target ARR"),
    ]

    ok = upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)
    return ok, len(rows)


def legacy_build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_fy = coalesce_filter("f_fy", "FYLabel")
    filter_region = coalesce_filter("f_region", "SalesRegion")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )
    trend = (
        load
        + 'q = filter q by RecordType == "trend";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )
    field_events = (
        load
        + 'q = filter q by RecordType == "field_history";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )

    q1_filters = (
        _rebind(filter_unit, "q1")
        + _rebind(filter_fy, "q1")
        + _rebind(filter_region, "q1")
    )
    q2_filters = (
        _rebind(filter_unit, "q2")
        + _rebind(filter_fy, "q2")
        + _rebind(filter_region, "q2")
    )
    q3_filters = (
        _rebind(filter_unit, "q3")
        + _rebind(filter_fy, "q3")
        + _rebind(filter_region, "q3")
    )

    summary = (
        f'q1 = load "{DS}";\n'
        'q1 = filter q1 by RecordType == "detail";\n'
        + q1_filters
        + 'q1 = filter q1 by IsWon == "true";\n'
        + "q1 = group q1 by all;\n"
        + "q1 = foreach q1 generate sum(ActualARR) as actual_closed;\n"
        + f'q2 = load "{DS}";\n'
        + 'q2 = filter q2 by RecordType == "detail";\n'
        + q2_filters
        + 'q2 = filter q2 by IsClosed == "false";\n'
        + "q2 = group q2 by all;\n"
        + "q2 = foreach q2 generate sum(WeightedOpenARR) as weighted_open, sum(AtRiskARR) as at_risk_arr;\n"
        + f'q3 = load "{DS}";\n'
        + 'q3 = filter q3 by RecordType == "detail";\n'
        + q3_filters
        + "q3 = group q3 by (OwnerName, FYLabel);\n"
        + "q3 = foreach q3 generate max(QuotaAmount) as owner_quota;\n"
        + "q3 = group q3 by all;\n"
        + "q3 = foreach q3 generate sum(owner_quota) as total_quota;\n"
        + "q = cogroup q1 by all, q2 by all, q3 by all;\n"
        + "q = foreach q generate "
        + "coalesce(sum(q1.actual_closed), 0) as actual_closed, "
        + "coalesce(sum(q2.weighted_open), 0) as weighted_open, "
        + "(coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) as projected, "
        + "coalesce(sum(q2.at_risk_arr), 0) as at_risk_arr, "
        + "coalesce(sum(q3.total_quota), 0) as target, "
        + "((coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) - coalesce(sum(q3.total_quota), 0)) as gap_to_plan, "
        + "(coalesce(sum(q3.total_quota), 0) * 0.90) as good, "
        + "(coalesce(sum(q3.total_quota), 0) * 0.75) as satisfactory;"
    )

    return {
        "f_unit": af("UnitGroup", ds_meta),
        "f_fy": af("FYLabel", ds_meta),
        "f_region": af("SalesRegion", ds_meta),
        "s_summary": sq(summary),
        "s_exception_summary": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(CriticalExceptionCount) as critical_exceptions, "
            + "sum(PastDueCount) as past_due_count, "
            + "sum(StaleCount) as stale_count, "
            + "(sum(PastDueARR) + sum(StaleARR) + sum(MissingApprovalARR)) as stuck_arr;"
        ),
        "s_monthly_trajectory": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ActualARR) as ActualARR, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(RegressionForecastARR) as RegressionForecastARR, "
            + "sum(RegressionUpperARR) as RegressionUpperARR, "
            + "sum(RegressionLowerARR) as RegressionLowerARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_stage_funnel": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, sum(WeightedOpenARR) as WeightedOpenARR;\n"
            + "q = order q by StageName asc;"
        ),
        "s_unit_risk": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by AtRiskARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_quarter_confidence": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = foreach q generate CloseQuarter, "
            + '(case when ForecastCategory == "Commit" then WeightedOpenARR else 0 end) as _commit, '
            + '(case when ForecastCategory == "Best Case" then WeightedOpenARR else 0 end) as _bestcase, '
            + '(case when ForecastCategory == "Pipeline" then WeightedOpenARR else 0 end) as _pipeline, '
            + "AtRiskARR as _atrisk;\n"
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(_commit) as CommitARR, "
            + "sum(_bestcase) as BestCaseARR, "
            + "sum(_pipeline) as PipelineARR, "
            + "sum(_atrisk) as AtRiskARR;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        "s_hygiene_trend": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(PastDueARR) as PastDueARR, "
            + "sum(StaleARR) as StaleARR, "
            + "sum(RiskWeightedARR) as RiskWeightedARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_push_trend": sq(
            field_events
            + 'q = filter q by EventField == "CloseDate";\n'
            + "q = group q by (EventMonthDate, EventMonth);\n"
            + "q = foreach q generate EventMonthDate, EventMonth, "
            + "sum(PushCount) as PushCount, "
            + "avg(PushDays) as AvgPushDays;\n"
            + "q = order q by EventMonthDate asc;"
        ),
        "s_stage_velocity": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by StageOrder != "00";\n'
            + "q = group q by (StageOrder, StageName);\n"
            + "q = foreach q generate StageOrder, StageName, "
            + "avg(DaysInStage) as AvgDaysInStage, "
            + "avg(StageSlaDays) as StageSlaDays;\n"
            + "q = order q by StageOrder asc;"
        ),
        "s_projection_by_motion": sq(
            detail
            + "q = group q by MotionType;\n"
            + "q = foreach q generate MotionType, "
            + "sum(ActualARR) as ActualARR, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by (ActualARR + WeightedOpenARR) desc;"
        ),
        "s_region_hygiene": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by SalesRegion;\n"
            + "q = foreach q generate SalesRegion, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by WeightedOpenARR desc;"
        ),
        "s_loss_driver": sq(
            detail
            + 'q = filter q by IsClosed == "true";\n'
            + 'q = filter q by IsWon == "false";\n'
            + 'q = filter q by WonLostReason != "";\n'
            + "q = group q by WonLostReason;\n"
            + "q = foreach q generate WonLostReason, count() as OppCount, sum(ARR) as LostARR;\n"
            + "q = order q by LostARR desc;\n"
            + "q = limit q 10;"
        ),
        "s_stage_success": sq(
            detail
            + 'q = filter q by StageOrder != "00";\n'
            + "q = group q by (StageOrder, StageName);\n"
            + "q = foreach q generate StageOrder, StageName, "
            + "avg(WinScore) as WinScore, "
            + "avg(HygieneScore) as HygieneScore;\n"
            + "q = order q by StageOrder asc;"
        ),
        "s_owner_queue": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(AtRiskARR) as AtRiskARR, "
            + "sum(CriticalExceptionCount) as CriticalExceptions;\n"
            + "q = order q by AtRiskARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_exception_by_stage": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (StageOrder, StageName);\n"
            + "q = foreach q generate StageOrder, StageName, "
            + "sum(CriticalExceptionCount) as CriticalExceptions, "
            + "sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by StageOrder asc;"
        ),
        "s_top_risk": sq(
            detail
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
        ),
        "s_top_process": sq(
            detail
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
        ),
    }


def legacy_build_widgets() -> dict[str, dict]:
    """Build dashboard widgets."""
    widgets = {
        "p1_nav1": nav_link("summary", "Summary", active=True),
        "p1_nav2": nav_link("trend", "Trend & Forecast"),
        "p1_nav3": nav_link("drivers", "Drivers & Segments"),
        "p1_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p1_hdr": hdr(
            "Pipeline & Opportunity Operations",
            "Manager operating view for forecast delivery, stage health, and exception concentration.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p1_f_region": pillbox("f_region", "Region"),
        "p1_n_projected": num(
            "s_summary", "projected", "Projected ARR", "#032D60", compact=True
        ),
        "p1_n_gap": num(
            "s_summary", "gap_to_plan", "Gap To Plan", "#BA0517", compact=True
        ),
        "p1_n_atrisk": num(
            "s_summary", "at_risk_arr", "At-Risk ARR", "#8E030F", compact=True
        ),
        "p1_ch_timeline": timeline_chart(
            "s_monthly_trajectory",
            "Monthly Revenue Trajectory",
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        "p1_ch_bullet": bullet_chart(
            "s_summary", "Projected ARR vs Plan", axis_title="ARR (EUR)"
        ),
        "p1_ch_stage": funnel_chart(
            "s_stage_funnel", "Open Pipeline by Stage", "StageName", "WeightedOpenARR"
        ),
        "p1_ch_unit": rich_chart(
            "s_unit_risk",
            "stackhbar",
            "Weighted Open vs At-Risk ARR by Unit",
            ["UnitGroup"],
            ["WeightedOpenARR", "AtRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("trend", "Trend & Forecast", active=True),
        "p2_nav3": nav_link("drivers", "Drivers & Segments"),
        "p2_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p2_hdr": hdr(
            "Trend & Forecast",
            "Forecast confidence, hygiene pressure, and close-date movement over time.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_ch_quarter": rich_chart(
            "s_quarter_confidence",
            "stackcolumn",
            "Quarterly Forecast Mix by Confidence",
            ["CloseQuarter"],
            ["CommitARR", "BestCaseARR", "PipelineARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p2_ch_hygiene": rich_chart(
            "s_hygiene_trend",
            "line",
            "Monthly Hygiene Pressure",
            ["MonthDate"],
            ["PastDueARR", "StaleARR", "RiskWeightedARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        "p2_ch_push": combo_chart(
            "s_push_trend",
            "Close Date Push Trend",
            ["EventMonthDate"],
            ["PushCount"],
            ["AvgPushDays"],
            show_legend=True,
            axis_title="Push Count",
            axis2_title="Avg Push Days",
        ),
        "p2_ch_velocity": rich_chart(
            "s_stage_velocity",
            "hbar",
            "Avg Days in Stage vs SLA",
            ["StageName"],
            ["AvgDaysInStage", "StageSlaDays"],
            show_legend=True,
            axis_title="Days",
            show_values=True,
        ),
        "p3_nav1": nav_link("summary", "Summary"),
        "p3_nav2": nav_link("trend", "Trend & Forecast"),
        "p3_nav3": nav_link("drivers", "Drivers & Segments", active=True),
        "p3_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p3_hdr": hdr(
            "Drivers & Segments",
            "Which motions, regions, and owners are driving performance or concentrating risk.",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p3_f_region": pillbox("f_region", "Region"),
        "p3_ch_motion": rich_chart(
            "s_projection_by_motion",
            "stackcolumn",
            "Actual, Open, and At-Risk ARR by Motion",
            ["MotionType"],
            ["ActualARR", "WeightedOpenARR", "AtRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p3_ch_region": rich_chart(
            "s_region_hygiene",
            "stackhbar",
            "Open vs At-Risk ARR by Region",
            ["SalesRegion"],
            ["WeightedOpenARR", "AtRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p3_ch_loss": rich_chart(
            "s_loss_driver",
            "column",
            "Closed-Lost ARR by Reason",
            ["WonLostReason"],
            ["LostARR"],
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p3_ch_stage": rich_chart(
            "s_stage_success",
            "hbar",
            "Win Score and Hygiene by Stage",
            ["StageName"],
            ["WinScore", "HygieneScore"],
            show_legend=True,
            axis_title="Score",
            show_values=True,
        ),
        "p3_tbl_owner": rich_chart(
            "s_owner_queue",
            "comparisontable",
            "Owner Risk Queue",
            ["OwnerName"],
            ["WeightedOpenARR", "AtRiskARR", "CriticalExceptions"],
            show_legend=False,
        ),
        "p4_nav1": nav_link("summary", "Summary"),
        "p4_nav2": nav_link("trend", "Trend & Forecast"),
        "p4_nav3": nav_link("drivers", "Drivers & Segments"),
        "p4_nav4": nav_link("exceptions", "Exceptions & Actions", active=True),
        "p4_hdr": hdr(
            "Exceptions & Actions",
            "The deals and stage clusters that require manager intervention now.",
        ),
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p4_f_region": pillbox("f_region", "Region"),
        "p4_n_critical": num(
            "s_exception_summary",
            "critical_exceptions",
            "Critical Exceptions",
            "#8E030F",
            compact=True,
        ),
        "p4_n_stuck": num(
            "s_exception_summary",
            "stuck_arr",
            "Stuck / Breach ARR",
            "#BA0517",
            compact=True,
        ),
        "p4_ch_stage": rich_chart(
            "s_exception_by_stage",
            "stackcolumn",
            "Critical Exceptions by Stage",
            ["StageName"],
            ["CriticalExceptions", "AtRiskARR"],
            show_legend=True,
            axis_title="Exceptions / ARR",
            show_values=True,
        ),
        "p4_tbl_risk": rich_chart(
            "s_top_risk",
            "comparisontable",
            "Top At-Risk Opportunities",
            [
                "OpportunityName",
                "AccountName",
                "OwnerName",
                "StageName",
                "ExceptionType",
            ],
            ["WeightedOpenARR", "TotalRiskScore", "SlipRiskScore", "PushCount"],
            show_legend=False,
        ),
        "p4_tbl_process": rich_chart(
            "s_top_process",
            "comparisontable",
            "Top Process Exceptions",
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
            show_legend=False,
        ),
    }

    widgets["p2_ch_push"]["parameters"].pop("columnMap", None)
    add_table_action(widgets["p4_tbl_risk"], "salesforceActions", "Opportunity", "Id")
    add_table_action(
        widgets["p4_tbl_process"], "salesforceActions", "Opportunity", "Id"
    )
    return widgets


def legacy_build_layout() -> dict:
    """Build the 4-page manager dashboard layout."""
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_projected", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_gap", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_atrisk", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_ch_timeline", "row": 9, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_bullet", "row": 17, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p1_ch_stage", "row": 17, "column": 4, "colspan": 3, "rowspan": 6},
        {"name": "p1_ch_unit", "row": 17, "column": 7, "colspan": 5, "rowspan": 6},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_quarter", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_hygiene", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_push", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_velocity", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_ch_motion", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_region", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_loss", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_stage", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_tbl_owner", "row": 19, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p4_n_critical", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_stuck", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p4_ch_stage", "row": 5, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p4_tbl_risk", "row": 11, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p4_tbl_process", "row": 18, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "PipelineOpportunityOperations",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("trend", "Trend & Forecast", p2),
            pg("drivers", "Drivers & Segments", p3),
            pg("exceptions", "Exceptions & Actions", p4),
        ],
    }


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_fy = coalesce_filter("f_fy", "FYLabel")
    filter_region = coalesce_filter("f_region", "SalesRegion")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )
    trend = (
        load
        + 'q = filter q by RecordType == "trend";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )
    field_events = (
        load
        + 'q = filter q by RecordType == "field_history";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )
    stage_events = (
        load
        + 'q = filter q by RecordType == "stage_history";\n'
        + filter_unit
        + filter_fy
        + filter_region
    )

    q1_filters = (
        _rebind(filter_unit, "q1")
        + _rebind(filter_fy, "q1")
        + _rebind(filter_region, "q1")
    )
    q2_filters = (
        _rebind(filter_unit, "q2")
        + _rebind(filter_fy, "q2")
        + _rebind(filter_region, "q2")
    )
    q3_filters = (
        _rebind(filter_unit, "q3")
        + _rebind(filter_fy, "q3")
        + _rebind(filter_region, "q3")
    )

    # Single-load summary using project-then-aggregate pattern.
    # SAQL's sum() only accepts bare field references, not expressions.
    # Step 1: project case-when computed fields
    # Step 2: group + sum the projected fields
    # This avoids cogroup+rebind which breaks number widget bindings.
    # QuotaContrib is pre-deduped in the dataset (one per owner/FY/unit/region).
    summary = (
        detail
        + "q = foreach q generate "
        + '(case when IsWon == "true" then ActualARR else 0 end) as _actual, '
        + '(case when IsClosed == "false" then WeightedOpenARR else 0 end) as _weighted, '
        + '(case when IsClosed == "false" then AtRiskARR else 0 end) as _atrisk, '
        + '(case when IsClosed == "false" and ForecastCategory == "Commit" then ARR else 0 end) as _commit, '
        + '(case when IsClosed == "false" and ForecastCategory == "Best Case" then ARR else 0 end) as _bestcase, '
        + '(case when IsClosed == "false" and ForecastCategory == "Pipeline" then ARR else 0 end) as _pipeline, '
        + "QuotaContrib as _quota;\n"
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "sum(_actual) as actual_closed, "
        + "sum(_weighted) as weighted_open, "
        + "(sum(_actual) + sum(_weighted)) as projected, "
        + "sum(_atrisk) as at_risk_arr, "
        + "sum(_commit) as commit_open, "
        + "sum(_bestcase) as best_case_open, "
        + "sum(_pipeline) as pipeline_open, "
        + "sum(_quota) as target, "
        + "((sum(_actual) + sum(_weighted)) - sum(_quota)) as gap_to_plan, "
        + "(sum(_quota) * 0.90) as good, "
        + "(sum(_quota) * 0.75) as satisfactory;"
    )

    # Waterfall charts require exactly 1 dimension + 1 measure.
    # BridgeStep labels are prefixed "1 ", "2 " etc for natural sort order.
    plan_bridge = (
        f'q1 = load "{DS}";\n'
        + 'q1 = filter q1 by RecordType == "detail";\n'
        + q1_filters
        + 'q1 = filter q1 by IsWon == "true";\n'
        + "q1 = group q1 by all;\n"
        + "q1 = foreach q1 generate sum(ActualARR) as closed_arr;\n"
        + 'q1l = foreach q1 generate "1 Closed Won" as BridgeStep, closed_arr as BridgeARR;\n'
        + f'q2 = load "{DS}";\n'
        + 'q2 = filter q2 by RecordType == "detail";\n'
        + q2_filters
        + 'q2 = filter q2 by IsClosed == "false";\n'
        + 'q2 = filter q2 by ForecastCategory == "Commit";\n'
        + "q2 = group q2 by all;\n"
        + "q2 = foreach q2 generate sum(ARR) as commit_arr;\n"
        + 'q2l = foreach q2 generate "2 Commit Open" as BridgeStep, commit_arr as BridgeARR;\n'
        + f'q3 = load "{DS}";\n'
        + 'q3 = filter q3 by RecordType == "detail";\n'
        + q3_filters
        + 'q3 = filter q3 by IsClosed == "false";\n'
        + 'q3 = filter q3 by ForecastCategory == "Best Case";\n'
        + "q3 = group q3 by all;\n"
        + "q3 = foreach q3 generate sum(ARR) as best_case_arr;\n"
        + 'q3l = foreach q3 generate "3 Best Case Open" as BridgeStep, best_case_arr as BridgeARR;\n'
        + f'q4 = load "{DS}";\n'
        + 'q4 = filter q4 by RecordType == "detail";\n'
        + q1_filters.replace("q1", "q4")
        + 'q4 = filter q4 by IsClosed == "false";\n'
        + 'q4 = filter q4 by ForecastCategory == "Pipeline";\n'
        + "q4 = group q4 by all;\n"
        + "q4 = foreach q4 generate sum(ARR) as pipeline_arr;\n"
        + 'q4l = foreach q4 generate "4 Pipeline Open" as BridgeStep, pipeline_arr as BridgeARR;\n'
        + f'q5 = load "{DS}";\n'
        + 'q5 = filter q5 by RecordType == "detail";\n'
        + q3_filters.replace("q3", "q5")
        + "q5 = group q5 by (OwnerName, FYLabel);\n"
        + "q5 = foreach q5 generate max(QuotaAmount) as owner_quota;\n"
        + "q5 = group q5 by all;\n"
        + "q5 = foreach q5 generate sum(owner_quota) as target_arr;\n"
        + "q6 = cogroup q1 by all, q2 by all, q3 by all, q4 by all, q5 by all;\n"
        + "q6 = foreach q6 generate "
        + '"5 Gap To Plan" as BridgeStep, '
        + "(coalesce(sum(q5.target_arr), 0) - "
        + "(coalesce(sum(q1.closed_arr), 0) + coalesce(sum(q2.commit_arr), 0) + coalesce(sum(q3.best_case_arr), 0) + coalesce(sum(q4.pipeline_arr), 0))) as BridgeARR;\n"
        + "q = union q1l, q2l, q3l, q4l, q6;\n"
        + "q = order q by BridgeStep asc;"
    )

    steps = {
        "f_unit": af("UnitGroup", ds_meta),
        "f_fy": af(
            "FYLabel", ds_meta
        ),  # No default start — breaks number widget bindings
        "f_region": af("SalesRegion", ds_meta),
        "s_summary": sq(summary),
        "s_exception_summary": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(CriticalExceptionCount) as critical_exceptions, "
            + "sum(PastDueCount) as past_due_count, "
            + "sum(StaleCount) as stale_count, "
            + "(sum(PastDueARR) + sum(StaleARR) + sum(MissingApprovalARR)) as stuck_arr;"
        ),
        "s_monthly_trajectory": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ActualARR) as ActualARR, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(RegressionForecastARR) as RegressionForecastARR, "
            + "sum(RegressionUpperARR) as RegressionUpperARR, "
            + "sum(RegressionLowerARR) as RegressionLowerARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_plan_bridge": sq(plan_bridge),
        "s_stage_funnel": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, sum(WeightedOpenARR) as WeightedOpenARR;\n"
            + "q = order q by StageName asc;"
        ),
        "s_unit_risk": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by AtRiskARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_quarter_confidence": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = foreach q generate CloseQuarter, "
            + '(case when ForecastCategory == "Commit" then WeightedOpenARR else 0 end) as _commit, '
            + '(case when ForecastCategory == "Best Case" then WeightedOpenARR else 0 end) as _bestcase, '
            + '(case when ForecastCategory == "Pipeline" then WeightedOpenARR else 0 end) as _pipeline, '
            + "AtRiskARR as _atrisk;\n"
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(_commit) as CommitARR, "
            + "sum(_bestcase) as BestCaseARR, "
            + "sum(_pipeline) as PipelineARR, "
            + "sum(_atrisk) as AtRiskARR;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        "s_hygiene_trend": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(PastDueARR) as PastDueARR, "
            + "sum(StaleARR) as StaleARR, "
            + "sum(RiskWeightedARR) as RiskWeightedARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_push_trend": sq(
            field_events
            + 'q = filter q by EventField == "CloseDate";\n'
            + "q = group q by (EventMonthDate, EventMonth);\n"
            + "q = foreach q generate EventMonthDate, EventMonth, "
            + "sum(PushCount) as PushCount, "
            + "avg(PushDays) as AvgPushDays;\n"
            + "q = order q by EventMonthDate asc;"
        ),
        "s_stage_velocity": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by StageOrder != "00";\n'
            + "q = group q by (StageOrder, StageName);\n"
            + "q = foreach q generate StageOrder, StageName, "
            + "avg(DaysInStage) as AvgDaysInStage, "
            + "avg(StageSlaDays) as StageSlaDays;\n"
            + "q = order q by StageOrder asc;"
        ),
        "s_stage_transition_heatmap": sq(
            stage_events
            + 'q = filter q by PrevStage != "";\n'
            + "q = group q by (PrevStage, StageName);\n"
            + "q = foreach q generate PrevStage, StageName, count() as TransitionCount;\n"
            + "q = filter q by TransitionCount >= 3;\n"
            + "q = order q by PrevStage asc, StageName asc;"
        ),
        "s_stage_flow": sq(
            stage_events
            + 'q = filter q by PrevStage != "";\n'
            + "q = group q by (PrevStage, StageName);\n"
            + "q = foreach q generate PrevStage as source, StageName as target, count() as flow;\n"
            + "q = order q by flow desc;\n"
            + "q = limit q 30;"
        ),
        "s_projection_by_motion": sq(
            detail
            + "q = group q by MotionType;\n"
            + "q = foreach q generate MotionType, "
            + "sum(ActualARR) as ActualARR, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by (ActualARR + WeightedOpenARR) desc;"
        ),
        "s_region_hygiene": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by SalesRegion;\n"
            + "q = foreach q generate SalesRegion, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by WeightedOpenARR desc;"
        ),
        "s_slip_scatter": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by AtRiskARR > 0;\n"
            + "q = group q by (OpportunityName, StageName, RiskBand, Id);\n"
            + "q = foreach q generate OpportunityName, StageName, RiskBand, "
            + "max(SlipRiskScore) as SlipRiskScore, "
            + "max(WinScore) as WinScore, "
            + "max(WeightedOpenARR) as WeightedOpenARR, "
            + "Id;\n"
            + "q = order q by SlipRiskScore desc;\n"
            + "q = limit q 25;"
        ),
        "s_exception_by_stage": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (StageOrder, StageName);\n"
            + "q = foreach q generate StageOrder, StageName, "
            + "sum(CriticalExceptionCount) as CriticalExceptions, "
            + "sum(AtRiskARR) as AtRiskARR;\n"
            + "q = order q by StageOrder asc;"
        ),
        "s_top_risk": sq(
            detail
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
        ),
        "s_top_process": sq(
            detail
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
        ),
    }

    # Apply KPI facet scope so KPI number widgets respond to filter steps
    for step_name in ("s_summary", "s_exception_summary"):
        if step_name in steps:
            steps[step_name].update(KPI_FACET_SCOPE)

    return steps


def build_widgets() -> dict[str, dict]:
    """Build dashboard widgets."""
    widgets = {
        "p1_nav1": nav_link("summary", "Summary", active=True),
        "p1_nav2": nav_link("trend", "Trend & Forecast"),
        "p1_nav3": nav_link("drivers", "Drivers & Segments"),
        "p1_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p1_hdr": hdr(
            "Pipeline & Opportunity Operations",
            "Manager operating view for stage health, coverage quality, and exception concentration.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p1_f_region": pillbox("f_region", "Region"),
        "p1_n_projected": num(
            "s_summary",
            "projected",
            "Weighted Projected ARR",
            "#032D60",
            compact=True,
            tier="primary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_gap": num(
            "s_summary",
            "gap_to_plan",
            "Gap To Plan",
            "#BA0517",
            compact=True,
            tier="primary",
            prefix="\u20ac",
            sentiment_color=True,
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_atrisk": num(
            "s_summary",
            "at_risk_arr",
            "At-Risk ARR",
            "#8E030F",
            compact=True,
            tier="primary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_commit": num(
            "s_summary",
            "commit_open",
            "Commit Open ARR",
            "#0176D3",
            compact=True,
            tier="primary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_sec_forecast": section_label("Forecast Trajectory"),
        "p1_ch_timeline": timeline_chart(
            "s_monthly_trajectory",
            "Monthly Actual vs Weighted Model Forecast",
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        "p1_sec_coverage": section_label("Pipeline Coverage & Composition"),
        "p1_ch_bridge": waterfall_chart(
            "s_plan_bridge",
            "Pipeline to Plan Bridge",
            "BridgeStep",
            "BridgeARR",
            axis_label="ARR (EUR)",
        ),
        "p1_ch_stage": funnel_chart(
            "s_stage_funnel", "Open Pipeline by Stage", "StageName", "WeightedOpenARR"
        ),
        "p1_ch_unit": rich_chart(
            "s_unit_risk",
            "stackhbar",
            "Weighted Open vs At-Risk ARR by Unit",
            ["UnitGroup"],
            ["WeightedOpenARR", "AtRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("trend", "Trend & Forecast", active=True),
        "p2_nav3": nav_link("drivers", "Drivers & Segments"),
        "p2_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p2_hdr": hdr(
            "Trend & Forecast",
            "Forecast mix, hygiene pressure, push behavior, and stage velocity over time.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_ch_quarter": rich_chart(
            "s_quarter_confidence",
            "stackcolumn",
            "Quarterly Forecast Mix by Confidence",
            ["CloseQuarter"],
            ["CommitARR", "BestCaseARR", "PipelineARR", "AtRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p2_ch_hygiene": line_chart(
            "s_hygiene_trend",
            "Monthly Hygiene Pressure",
            show_legend=True,
            axis_title="ARR (EUR)",
            reference_lines=[
                {"label": "Risk Threshold", "value": 0, "color": "#C23934"},
            ],
        ),
        "p2_ch_push": combo_chart(
            "s_push_trend",
            "Close Date Push Trend",
            ["EventMonthDate"],
            ["PushCount"],
            ["AvgPushDays"],
            show_legend=True,
            axis_title="Push Count",
            axis2_title="Avg Push Days",
            axis1_format="#,##0",
            axis2_format="0.0",
            reference_lines=[
                {"label": "SLA (14 days)", "value": 14, "color": "#FF9E2C"},
            ],
        ),
        "p2_ch_velocity": rich_chart(
            "s_stage_velocity",
            "hbar",
            "Avg Days in Stage vs SLA",
            ["StageName"],
            ["AvgDaysInStage", "StageSlaDays"],
            show_legend=True,
            axis_title="Days",
            show_values=True,
        ),
        "p2_sec_velocity": section_label("Stage Velocity & Push Trends"),
        "p3_nav1": nav_link("summary", "Summary"),
        "p3_nav2": nav_link("trend", "Trend & Forecast"),
        "p3_nav3": nav_link("drivers", "Drivers & Segments", active=True),
        "p3_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p3_hdr": hdr(
            "Drivers & Segments",
            "Which stage transitions, motions, and regions are shaping pipeline quality.",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p3_f_region": pillbox("f_region", "Region"),
        "p3_ch_flow": sankey_chart(
            "s_stage_flow",
            "Stage Flow: Previous Stage -> Current Stage",
        ),
        "p3_ch_heatmap": heatmap_chart(
            "s_stage_transition_heatmap",
            "Stage Transition Matrix",
            show_legend=True,
        ),
        "p3_sec_segments": section_label("Motion & Region Segments"),
        "p3_ch_motion": rich_chart(
            "s_projection_by_motion",
            "stackcolumn",
            "Actual, Open, and At-Risk ARR by Motion",
            ["MotionType"],
            ["ActualARR", "WeightedOpenARR", "AtRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p3_ch_region": rich_chart(
            "s_region_hygiene",
            "stackhbar",
            "Open vs At-Risk ARR by Region",
            ["SalesRegion"],
            ["WeightedOpenARR", "AtRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p4_nav1": nav_link("summary", "Summary"),
        "p4_nav2": nav_link("trend", "Trend & Forecast"),
        "p4_nav3": nav_link("drivers", "Drivers & Segments"),
        "p4_nav4": nav_link("exceptions", "Exceptions & Actions", active=True),
        "p4_hdr": hdr(
            "Exceptions & Actions",
            "The deals and stage clusters that require manager intervention now.",
        ),
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p4_f_region": pillbox("f_region", "Region"),
        "p4_n_critical": num(
            "s_exception_summary",
            "critical_exceptions",
            "Critical Exceptions",
            "#8E030F",
            compact=True,
            tier="secondary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p4_n_stuck": num(
            "s_exception_summary",
            "stuck_arr",
            "Stuck / Breach ARR",
            "#BA0517",
            compact=True,
            tier="secondary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p4_sec_exceptions": section_label("Exception Concentration"),
        "p4_ch_stage": rich_chart(
            "s_exception_by_stage",
            "stackcolumn",
            "Critical Exceptions by Stage",
            ["StageName"],
            ["CriticalExceptions", "AtRiskARR"],
            show_legend=True,
            axis_title="Exceptions / ARR",
            show_values=True,
        ),
        "p4_ch_scatter": bubble_chart(
            "s_slip_scatter",
            "Slip Risk vs Win Score",
            show_legend=False,
        ),
        "p4_sec_risk_queue": section_label("Risk & Process Queues"),
        "p4_tbl_risk": compare_table(
            "s_top_risk",
            "Top At-Risk Opportunities",
            columns=[
                "OpportunityName",
                "AccountName",
                "OwnerName",
                "StageName",
                "ExceptionType",
                "WeightedOpenARR",
                "TotalRiskScore",
                "SlipRiskScore",
                "PushCount",
            ],
            format_rules=[
                {
                    "measure": "TotalRiskScore",
                    "ranges": [
                        {"color": "#C23934", "min": 80},
                        {"color": "#FF9E2C", "min": 65, "max": 80},
                        {"color": "#4BCA81", "max": 65},
                    ],
                },
                {
                    "measure": "SlipRiskScore",
                    "ranges": [
                        {"color": "#C23934", "min": 70},
                        {"color": "#FF9E2C", "min": 45, "max": 70},
                        {"color": "#4BCA81", "max": 45},
                    ],
                },
            ],
        ),
        "p4_tbl_process": compare_table(
            "s_top_process",
            "Top Process Exceptions",
            columns=[
                "OpportunityName",
                "AccountName",
                "OwnerName",
                "StageName",
                "ExceptionType",
                "WeightedOpenARR",
                "DaysInStage",
                "PushCount",
                "BackwardMoveCount",
                "MissingApprovalCount",
            ],
            format_rules=[
                {
                    "measure": "DaysInStage",
                    "ranges": [
                        {"color": "#C23934", "min": 30},
                        {"color": "#FF9E2C", "min": 14, "max": 30},
                        {"color": "#4BCA81", "max": 14},
                    ],
                },
                {
                    "measure": "BackwardMoveCount",
                    "ranges": [
                        {"color": "#C23934", "min": 2},
                        {"color": "#FF9E2C", "min": 1, "max": 2},
                    ],
                },
            ],
        ),
    }

    widgets["p2_ch_push"]["parameters"].pop("columnMap", None)
    add_table_action(widgets["p4_tbl_risk"], "salesforceActions", "Opportunity", "Id")
    add_table_action(
        widgets["p4_tbl_process"], "salesforceActions", "Opportunity", "Id"
    )
    return widgets


def build_layout() -> dict:
    """Build the 4-page manager dashboard layout."""
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_projected", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_gap", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_atrisk", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_commit", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p1_sec_forecast", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_timeline", "row": 10, "column": 0, "colspan": 12, "rowspan": 8},
        {
            "name": "p1_sec_coverage",
            "row": 18,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p1_ch_bridge", "row": 19, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p1_ch_stage", "row": 19, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p1_ch_unit", "row": 25, "column": 0, "colspan": 12, "rowspan": 6},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_quarter", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_hygiene", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {
            "name": "p2_sec_velocity",
            "row": 12,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p2_ch_push", "row": 13, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_velocity", "row": 13, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_ch_flow", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_heatmap", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {
            "name": "p3_sec_segments",
            "row": 12,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p3_ch_motion", "row": 13, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_region", "row": 13, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_region", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p4_n_critical", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_stuck", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {
            "name": "p4_sec_exceptions",
            "row": 5,
            "column": 6,
            "colspan": 6,
            "rowspan": 1,
        },
        {"name": "p4_ch_stage", "row": 6, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p4_ch_scatter", "row": 12, "column": 0, "colspan": 12, "rowspan": 6},
        {
            "name": "p4_sec_risk_queue",
            "row": 18,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p4_tbl_risk", "row": 19, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p4_tbl_process", "row": 26, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "PipelineOpportunityOperations",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("trend", "Trend & Forecast", p2),
            pg("drivers", "Drivers & Segments", p3),
            pg("exceptions", "Exceptions & Actions", p4),
        ],
    }


def main() -> None:
    """Build dataset and deploy dashboard."""
    with builder_run(DS, __file__) as summary:
        instance_url, token = get_auth()
        assert_org_schema(
            instance_url,
            token,
            objects=["Opportunity", "OpportunityFieldHistory", "OpportunityHistory"],
        )

        ok, row_count = create_dataset(instance_url, token)
        if not ok:
            raise SystemExit("Dataset upload failed")
        summary.row_count = row_count

        dataset_id = get_dataset_id(instance_url, token, DS)
        if not dataset_id:
            raise SystemExit(f"Could not resolve dataset id for {DS}")
        summary.dataset_id = dataset_id

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
        logger.info("\n=== Deploying %s ===", DASHBOARD_LABEL)
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
