#!/usr/bin/env python3
"""Build the Forecast & Revenue Motions dashboard.

This is the Wave 1 manager dashboard that consolidates:
- Forecast Intelligence
- Revenue Motions KPIs

Design goals:
- 4-page manager surface for forecast delivery and motion quality
- native forecast trajectories with motion-aware time series
- renewals/churn pressure, product and competitor drivers
- actionable owner and opportunity queues
"""

from __future__ import annotations

import csv
import io
import logging
from collections import defaultdict
from datetime import UTC, datetime

from crm_analytics_helpers import (
    KPI_CARD_STYLE,
    _date,
    _dim,
    _measure,
    _soql,
    add_table_action,
    af,
    build_dashboard_state,
    coalesce_filter,
    compare_table,
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    hdr,
    heatmap_chart,
    line_chart,
    nav_link,
    nav_link_external,
    num,
    pg,
    pillbox,
    precompute_scoring_stats,
    compute_win_score,
    section_label,
    set_record_links_xmd,
    sq,
    upload_dataset,
    waterfall_chart,
)
from commercial_operating_model import (
    ownership_alignment,
    primary_motion_persona,
    role_dimension_row,
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


logger = logging.getLogger(__name__)

DS = "Forecast_Revenue_Motions"
DS_LABEL = "Forecast Revenue Motions"
DASHBOARD_LABEL = "Forecast & Revenue Motions"
WFS = "Weekly_Forecast_Summary"
WFO = "Weekly_Forecast_Opps"
SALES_MANAGER_DASHBOARD_ID = "0FKTb0000000JCLOA2"
CSM_MANAGER_DASHBOARD_ID = "0FKTb0000000J97OAE"
ACCOUNT_360_DASHBOARD_ID = "0FKTb0000000JNdOAM"

# ---------------------------------------------------------------------------
# Consulting-grade constants
# ---------------------------------------------------------------------------
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_unit", "f_fy", "f_region", "f_quarter", "f_manager"],
    },
}
CURRENT_FY_LABEL = current_fy_label()

SOQL = (
    "SELECT Id, Name, OwnerId, Owner.Name, AccountId, Account.Name, "
    "Owner.ManagerId, Owner.Manager.Name, "
    "Account_Unit_Group__c, Sales_Region__c, ForecastCategoryName, "
    "IsClosed, IsWon, CloseDate, StageName, NextStep, Type, CreatedDate, "
    "FiscalYear, FiscalQuarter, "
    "APTS_Forecast_ARR__c, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
    "Amount, Probability, AgeInDays, Sales_Cycle_Duration__c, "
    "Quota_Amount__c, "
    "Deal_Shaping_Approved__c, Submit_for_Stage_20_Review__c, "
    "Submit_for_Stage_20_Review_Date__c, Stage_20_Approval__c, "
    "Stage_20_Approval_Date__c, Approval_Status__c, HasOverdueTask, "
    "Reason_Won_Lost__c, Sub_Reason__c, "
    "Lost_to_Competitor__r.Name, "
    "Account.SaaS_Client__c, Account.Axioma_Client__c, "
    "Account.Risk_of_Potential_Termination__c, "
    "Account.APTS_Subscription_Term__c, "
    "APTS_RH_Product_Family__c "
    "FROM Opportunity "
    "WHERE FiscalYear IN (2025, 2026, 2027)"
)

USER_DIM_FIELDS = (
    "Id, Name, Title, Department, Division, UserRole.Name, ManagerId, Manager.Name"
)


def _chunked(items: list[str], size: int = 150) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _fetch_user_dimensions(
    inst: str, tok: str, user_ids: set[str]
) -> dict[str, dict[str, object]]:
    """Load live user metadata for persona and ownership alignment."""
    clean_ids = sorted({user_id for user_id in user_ids if user_id})
    dims: dict[str, dict[str, object]] = {}
    for chunk in _chunked(clean_ids):
        quoted_ids = ",".join(f"'{user_id}'" for user_id in chunk)
        query = f"SELECT {USER_DIM_FIELDS} FROM User WHERE Id IN ({quoted_ids})"
        for row in _soql(inst, tok, query):
            dims[row.get("Id", "")] = role_dimension_row(
                owner_id=row.get("Id", ""),
                owner_name=row.get("Name", "") or "",
                title=row.get("Title") or "",
                user_role=((row.get("UserRole") or {}).get("Name")) or "",
                department=row.get("Department") or "",
                division=row.get("Division") or "",
                manager_id=row.get("ManagerId") or "",
                manager_name=((row.get("Manager") or {}).get("Name")) or "",
            )
    return dims


def _add_months(month_key_value: str, offset: int) -> str:
    """Add offset months to a YYYY-MM string."""
    dt = datetime.strptime(f"{month_key_value}-01", "%Y-%m-%d")
    month = dt.month - 1 + offset
    year = dt.year + month // 12
    month = month % 12 + 1
    return f"{year:04d}-{month:02d}"


def _quarter_from_month_key(month_key_value: str) -> tuple[int, str]:
    """Return fiscal-quarter number and label for a YYYY-MM month key."""
    if not month_key_value:
        return 0, ""
    month_num = int(month_key_value[5:7])
    quarter_num = ((month_num - 1) // 3) + 1
    return quarter_num, f"Q{quarter_num}"


def _date_only(value: object) -> str:
    """Normalize Salesforce datetime/date strings to YYYY-MM-DD."""
    return str(value or "")[:10]


def _days_since(start_date: str, end_date: datetime) -> float:
    """Return days between a YYYY-MM-DD string and a datetime anchor."""
    if not start_date:
        return 0.0
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        return 0.0
    return max((end_date.date() - start).days, 0)


def _rebind(binding: str, alias: str) -> str:
    """Retarget a coalesce_filter binding to a specific SAQL alias."""
    return (
        binding.replace("q =", f"{alias} =")
        .replace("q by", f"{alias} by")
        .replace("q generate", f"{alias} generate")
    )


def _term_bucket(term_value: float) -> str:
    """Bucket subscription terms into manager-friendly ranges."""
    if term_value <= 0:
        return "Unknown"
    if term_value <= 12:
        return "0-12m"
    if term_value <= 24:
        return "13-24m"
    if term_value <= 36:
        return "25-36m"
    return "36m+"


def _stage_progression_label(stage_name: str) -> str:
    """Map free-form Salesforce stages into a stable process-ordered label."""
    normalized = (stage_name or "").strip().lower().replace("-", "").replace(" ", "")
    if not normalized:
        return "9 Other"
    if normalized.startswith("prospect"):
        return "1 Prospecting"
    if normalized.startswith("discover"):
        return "2 Discovery"
    if normalized.startswith("engage"):
        return "3 Engagement"
    if normalized.startswith("short"):
        return "4 Shortlisted"
    if normalized.startswith("prefer"):
        return "5 Preferred"
    if normalized.startswith("contract"):
        return "6 Contracting"
    if normalized in {"optout", "opt_out"}:
        return "7 Opt-out"
    if normalized in {"won", "closedwon", "closed"}:
        return "8 Won"
    return f"9 Other: {stage_name.strip()[:40]}"


def _risk_band(score: float) -> str:
    """Map numeric risk score into an operating band."""
    if score >= 80:
        return "Critical"
    if score >= 65:
        return "High"
    if score >= 45:
        return "Medium"
    return "Low"


def _motion_risk_score(
    motion: str,
    risk_level: str,
    forecast_category: str,
    age_in_days: float,
    subscription_term: float,
    win_score: float,
) -> float:
    """Blend account, motion, and forecast confidence into one score."""
    score = max(risk_level_to_score(risk_level), 100.0 - win_score)
    category = (forecast_category or "").strip().lower().replace(" ", "")
    if category == "omitted":
        score += 12.0
    elif category == "pipeline":
        score += 8.0

    if age_in_days >= 180:
        score += 10.0
    elif age_in_days >= 120:
        score += 5.0

    if motion == "Renewal":
        if subscription_term <= 12 and subscription_term > 0:
            score += 10.0
        elif subscription_term <= 24:
            score += 5.0
    elif motion == "Expand":
        score += 2.0
    elif motion == "Land":
        score += 4.0

    return round(max(0.0, min(100.0, score)), 1)


def _opp_context(opp: dict) -> dict[str, object]:
    """Return shared dimension values for an opportunity."""
    acct = opp.get("Account") or {}
    owner = opp.get("Owner") or {}
    product = (opp.get("APTS_RH_Product_Family__c") or "").split(";")[
        0
    ].strip() or "Unknown"
    fiscal_year = int(safe_float(opp.get("FiscalYear"), 0))
    fiscal_quarter = int(safe_float(opp.get("FiscalQuarter"), 0))
    competitor = (
        ((opp.get("Lost_to_Competitor__r") or {}).get("Name") or "").strip()
        if opp.get("Lost_to_Competitor__r")
        else ""
    )
    motion = normalize_motion(opp.get("Type") or "")
    stage_name = (opp.get("StageName") or "").strip()
    return {
        "Id": opp.get("Id", ""),
        "OpportunityName": (opp.get("Name") or "")[:255],
        "AccountId": opp.get("AccountId", ""),
        "AccountName": ((acct.get("Name") or "")[:255]),
        "OwnerName": ((owner.get("Name") or "Unknown")[:255]),
        "ManagerId": owner.get("ManagerId") or "",
        "ManagerName": (
            (((owner.get("Manager") or {}).get("Name")) or "Unassigned")[:255]
        ),
        "UnitGroup": (
            (opp.get("Account_Unit_Group__c") or "Unassigned").strip() or "Unassigned"
        )[:255],
        "SalesRegion": (
            (opp.get("Sales_Region__c") or "Unassigned").strip() or "Unassigned"
        )[:255],
        "MotionType": motion,
        "ForecastCategory": (opp.get("ForecastCategoryName") or "Pipeline")[:255],
        "StageName": stage_name[:255],
        "StageProgression": _stage_progression_label(stage_name)[:255],
        "ProductFamily": product[:255],
        "Competitor": competitor[:255],
        "WonLostReason": (opp.get("Reason_Won_Lost__c") or "")[:255],
        "SubReason": (opp.get("Sub_Reason__c") or "")[:255],
        "FYLabel": fiscal_label(fiscal_year),
        "CloseQuarter": f"Q{fiscal_quarter}" if fiscal_quarter else "",
        "FiscalYear": fiscal_year,
        "FiscalQuarter": fiscal_quarter,
    }


def create_dataset(inst: str, tok: str) -> tuple[bool, int]:
    """Build the merged forecast and revenue motions dataset."""
    logger.info("\n=== Building %s dataset ===", DS_LABEL)
    opps = _soql(inst, tok, SOQL)
    logger.info("  Queried %d opportunities", len(opps))
    if not opps:
        raise RuntimeError(
            "No opportunities returned for the forecast and revenue motions dataset."
        )

    user_ids = {opp.get("OwnerId") or "" for opp in opps if opp.get("OwnerId")}
    user_ids.update(
        (opp.get("Owner") or {}).get("ManagerId") or ""
        for opp in opps
        if (opp.get("Owner") or {}).get("ManagerId")
    )
    user_dimensions = _fetch_user_dimensions(inst, tok, user_ids)

    type_win_rates, avg_deal_size = precompute_scoring_stats(opps)
    now_utc = datetime.now(UTC)
    current_month = now_utc.strftime("%Y-%m")

    detail_rows: list[dict[str, object]] = []
    grouped_monthly: dict[tuple[str, ...], dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(
            lambda: {
                "ActualARR": 0.0,
                "WeightedOpenARR": 0.0,
                "ProjectedARR": 0.0,
                "RenewalRiskARR": 0.0,
                "RiskyCommitARR": 0.0,
                "LostARR": 0.0,
                "CompetitiveLossARR": 0.0,
            }
        )
    )
    owner_quota: dict[tuple[str, ...], float] = {}
    min_month = ""
    max_month = current_month

    for opp in opps:
        context = _opp_context(opp)
        acct = opp.get("Account") or {}
        motion = str(context["MotionType"])
        owner_id = opp.get("OwnerId") or ""
        owner_dim = user_dimensions.get(
            owner_id,
            role_dimension_row(
                owner_id=owner_id,
                owner_name=str(context["OwnerName"]),
                manager_id=str(context["ManagerId"]),
                manager_name=str(context["ManagerName"]),
            ),
        )
        manager_dim = user_dimensions.get(
            str(context["ManagerId"]),
            role_dimension_row(
                owner_id=str(context["ManagerId"]),
                owner_name=str(context["ManagerName"]),
            ),
        )
        close_date = _date_only(opp.get("CloseDate"))
        close_month = month_key(close_date)
        if close_month:
            min_month = close_month if not min_month else min(min_month, close_month)
            max_month = max(max_month, close_month)

        arr = round(
            safe_float(opp.get("ConvertedARR") or opp.get("APTS_Forecast_ARR__c")), 2
        )
        probability = round(safe_float(opp.get("Probability")), 1)
        age_in_days = round(safe_float(opp.get("AgeInDays")), 1)
        sales_cycle = round(safe_float(opp.get("Sales_Cycle_Duration__c")), 1)
        quota_amount = round(safe_float(opp.get("Quota_Amount__c")), 2)
        subscription_term = round(safe_float(acct.get("APTS_Subscription_Term__c")), 1)
        is_closed = coerce_bool(opp.get("IsClosed"))
        is_won = coerce_bool(opp.get("IsWon"))
        risk_level = (
            acct.get("Risk_of_Potential_Termination__c") or ""
        ).strip() or "Low"
        forecast_category = str(context["ForecastCategory"])
        owner_persona = str(owner_dim["Persona"])
        manager_persona = str(manager_dim["Persona"])
        motion_primary_persona = primary_motion_persona(motion)
        alignment = ownership_alignment(owner_persona, motion)

        win_score, win_band = compute_win_score(opp, type_win_rates, avg_deal_size)
        weight = (
            1.0
            if is_won
            else (0.0 if is_closed else forecast_weight(forecast_category, probability))
        )
        weighted_open = round(0.0 if is_closed else arr * weight, 2)
        actual_arr = round(arr if is_won else 0.0, 2)
        projected_arr = round(actual_arr + weighted_open, 2)
        lost_arr = round(arr if is_closed and not is_won else 0.0, 2)
        risk_score = _motion_risk_score(
            motion,
            risk_level,
            forecast_category,
            age_in_days,
            subscription_term,
            float(win_score),
        )
        renewal_risk_arr = round(
            weighted_open if motion == "Renewal" and risk_score >= 60 else 0.0, 2
        )
        risky_commit_arr = round(
            weighted_open
            if forecast_category in {"Commit", "Best Case"} and risk_score >= 65
            else 0.0,
            2,
        )
        competitive_loss_arr = round(lost_arr if str(context["Competitor"]) else 0.0, 2)
        omitted_arr = round(
            arr if (not is_closed and forecast_category == "Omitted") else 0.0, 2
        )
        commit_open_arr = round(
            arr if (not is_closed and forecast_category == "Commit") else 0.0, 2
        )
        best_case_open_arr = round(
            arr if (not is_closed and forecast_category == "Best Case") else 0.0, 2
        )
        pipeline_open_arr = round(
            arr if (not is_closed and forecast_category == "Pipeline") else 0.0, 2
        )
        expected_arr = round(
            arr
            if is_won
            else (0.0 if is_closed else arr * max(probability, 10.0) / 100.0),
            2,
        )
        no_next_step_flag = 1 if not (opp.get("NextStep") or "").strip() else 0
        submit_for_review_date = _date_only(
            opp.get("Submit_for_Stage_20_Review_Date__c")
        )
        commercial_approval_date = _date_only(opp.get("Stage_20_Approval_Date__c"))
        deal_review_approved_flag = (
            1 if coerce_bool(opp.get("Deal_Shaping_Approved__c")) else 0
        )
        commercial_approval_flag = (
            1 if submit_for_review_date and not commercial_approval_date else 0
        )
        commercial_approval_age_days = round(
            _days_since(submit_for_review_date, now_utc),
            1,
        )
        stale_commercial_approval_flag = (
            1 if commercial_approval_flag and commercial_approval_age_days >= 14 else 0
        )
        overdue_task_flag = 1 if coerce_bool(opp.get("HasOverdueTask")) else 0
        late_stage_flag = (
            1 if str(context["StageProgression"]).startswith(("4 ", "5 ", "6 ")) else 0
        )
        omitted_flag = 1 if omitted_arr > 0 else 0
        open_arr = round(projected_arr if not is_closed else 0.0, 2)
        review_candidate_flag = (
            1
            if (
                not is_closed
                and (late_stage_flag or forecast_category in {"Commit", "Best Case"})
                and (no_next_step_flag == 1 or risk_score >= 65)
            )
            else 0
        )
        needs_review_ownership_flag = 1 if alignment == "Needs Review" else 0
        needs_review_ownership_arr = round(
            open_arr if needs_review_ownership_flag else 0.0,
            2,
        )

        detail_rows.append(
            {
                "RecordType": "detail",
                **context,
                "OwnerId": owner_id,
                "OwnerTitle": str(owner_dim["Title"])[:255],
                "OwnerRole": str(owner_dim["UserRole"])[:255],
                "OwnerDivision": str(owner_dim["Division"])[:255],
                "OwnerPersona": owner_persona[:255],
                "ManagerPersona": manager_persona[:255],
                "MotionPrimaryPersona": motion_primary_persona[:255],
                "OwnershipAlignment": alignment[:255],
                "RiskLevel": risk_level,
                "RiskBand": _risk_band(risk_score),
                "WinScoreBand": win_band,
                "TermBucket": _term_bucket(subscription_term),
                "SaaSClient": str(coerce_bool(acct.get("SaaS_Client__c"))).lower(),
                "AxiomaClient": str(coerce_bool(acct.get("Axioma_Client__c"))).lower(),
                "IsClosed": str(is_closed).lower(),
                "IsWon": str(is_won).lower(),
                "IsRenewal": str(motion == "Renewal").lower(),
                "IsExpand": str(motion == "Expand").lower(),
                "IsLand": str(motion == "Land").lower(),
                "IsServices": str(motion == "Services").lower(),
                "CloseDate": close_date,
                "CreatedDate": (opp.get("CreatedDate") or "")[:10],
                "NextStep": (opp.get("NextStep") or "")[:255],
                "NoNextStepFlag": no_next_step_flag,
                "DealReviewApprovedFlag": deal_review_approved_flag,
                "SubmitForCommercialApprovalDate": submit_for_review_date,
                "CommercialApprovalDate": commercial_approval_date,
                "CommercialApprovalFlag": commercial_approval_flag,
                "CommercialApprovalAgeDays": commercial_approval_age_days,
                "StaleCommercialApprovalFlag": stale_commercial_approval_flag,
                "OverdueTaskFlag": overdue_task_flag,
                "MonthDate": month_start(close_date),
                "MonthLabel": close_month,
                "SubscriptionTerm": subscription_term,
                "QuotaAmount": quota_amount,
                "ARR": arr,
                "Probability": probability,
                "AgeInDays": age_in_days,
                "SalesCycleDuration": sales_cycle,
                "WinScore": round(float(win_score), 1),
                "RiskScore": risk_score,
                "ActualARR": actual_arr,
                "WeightedOpenARR": weighted_open,
                "ProjectedARR": projected_arr,
                "ExpectedARR": expected_arr,
                "LostARR": lost_arr,
                "OmittedARR": omitted_arr,
                "CommitOpenARR": commit_open_arr,
                "BestCaseOpenARR": best_case_open_arr,
                "PipelineOpenARR": pipeline_open_arr,
                "RenewalRiskARR": renewal_risk_arr,
                "RiskyCommitARR": risky_commit_arr,
                "CompetitiveLossARR": competitive_loss_arr,
                "OpenARR": open_arr,
                "LandARR": round(projected_arr if motion == "Land" else 0.0, 2),
                "ExpandARR": round(projected_arr if motion == "Expand" else 0.0, 2),
                "RenewalARR": round(projected_arr if motion == "Renewal" else 0.0, 2),
                "ServicesARR": round(projected_arr if motion == "Services" else 0.0, 2),
                "SaaSARR": round(
                    projected_arr if coerce_bool(acct.get("SaaS_Client__c")) else 0.0, 2
                ),
                "PSARR": round(projected_arr if motion == "Services" else 0.0, 2),
                "LandOppCount": 1 if motion == "Land" else 0,
                "LandWonCount": 1 if motion == "Land" and is_won else 0,
                "ExpandOppCount": 1 if motion == "Expand" else 0,
                "ExpandWonCount": 1 if motion == "Expand" and is_won else 0,
                "RenewalOppCount": 1 if motion == "Renewal" else 0,
                "RenewalWonCount": 1 if motion == "Renewal" and is_won else 0,
                "RenewalRiskCount": 1 if renewal_risk_arr > 0 else 0,
                "RiskyCommitCount": 1 if risky_commit_arr > 0 else 0,
                "CompetitiveLossCount": 1 if competitive_loss_arr > 0 else 0,
                "OmittedOppCount": omitted_flag,
                "LateStageOppCount": late_stage_flag,
                "ReviewCandidateCount": review_candidate_flag,
                "NeedsReviewOwnershipCount": needs_review_ownership_flag,
                "NeedsReviewOwnershipARR": needs_review_ownership_arr,
                "PendingApprovalARR": round(
                    open_arr if commercial_approval_flag else 0.0,
                    2,
                ),
                "RegressionForecastARR": 0.0,
                "RegressionUpperARR": 0.0,
                "RegressionLowerARR": 0.0,
                "TargetARR": 0.0,
            }
        )

        if context["FiscalYear"]:
            quota_key = (
                owner_id,
                str(context["OwnerName"]),
                int(context["FiscalYear"]),
                str(context["UnitGroup"]),
                str(context["SalesRegion"]),
                str(context["ManagerId"]),
                str(context["ManagerName"]),
            )
            if quota_amount > owner_quota.get(quota_key, 0.0):
                owner_quota[quota_key] = quota_amount

        if close_month:
            trend_key = (
                str(context["UnitGroup"]),
                str(context["SalesRegion"]),
                str(context["FYLabel"]),
                motion,
                str(context["ManagerId"]),
                str(context["ManagerName"]),
                manager_persona,
                owner_id,
                str(context["OwnerName"]),
                str(owner_dim["Title"]),
                str(owner_dim["UserRole"]),
                str(owner_dim["Division"]),
                owner_persona,
                alignment,
            )
            bucket = grouped_monthly[trend_key][close_month]
            bucket["ActualARR"] += actual_arr
            bucket["WeightedOpenARR"] += weighted_open
            bucket["ProjectedARR"] += projected_arr
            bucket["RenewalRiskARR"] += renewal_risk_arr
            bucket["RiskyCommitARR"] += risky_commit_arr
            bucket["LostARR"] += lost_arr
            bucket["CompetitiveLossARR"] += competitive_loss_arr

    if not min_month:
        raise RuntimeError(
            "No opportunity close-month data available for the forecast and revenue motions dataset."
        )

    quota_by_segment: dict[tuple[str, ...], float] = defaultdict(float)
    for (
        owner_id,
        owner_name,
        fiscal_year,
        unit_group,
        sales_region,
        manager_id,
        manager_name,
    ), quota in owner_quota.items():
        quota_by_segment[
            (
                unit_group,
                sales_region,
                fiscal_label(fiscal_year),
                manager_id,
                manager_name,
                owner_id,
                owner_name,
            )
        ] += quota

    trend_rows: list[dict[str, object]] = []
    forecast_end_month = _add_months(max(max_month, current_month), 3)
    months = month_sequence(min_month, forecast_end_month)

    for (
        unit_group,
        sales_region,
        fy_label,
        motion,
        manager_id,
        manager_name,
        manager_persona,
        owner_id,
        owner_name,
        owner_title,
        owner_role,
        owner_division,
        owner_persona,
        alignment,
    ), monthly_data in grouped_monthly.items():
        fy_months = [
            month for month in months if fiscal_label(int(month[:4])) == fy_label
        ]
        historical_months = [month for month in fy_months if month <= current_month]
        if not fy_months:
            continue
        actual_series = [
            monthly_data[month]["ActualARR"] for month in historical_months
        ]
        fit = least_squares(actual_series)
        annual_quota = round(
            quota_by_segment.get(
                (
                    unit_group,
                    sales_region,
                    fy_label,
                    manager_id,
                    manager_name,
                    owner_id,
                    owner_name,
                ),
                0.0,
            ),
            2,
        )
        target_monthly = round(annual_quota / 12.0, 2)

        for index, month in enumerate(fy_months):
            values = monthly_data[month]
            forecast = max(0.0, fit["intercept"] + fit["slope"] * index)
            interval = prediction_interval(fit, index)
            year_value = int(month[:4])
            fiscal_quarter, close_quarter = _quarter_from_month_key(month)

            trend_rows.append(
                {
                    "RecordType": "trend",
                    "Id": "",
                    "OpportunityName": "",
                    "AccountId": "",
                    "AccountName": "",
                    "OwnerId": owner_id,
                    "OwnerName": owner_name[:255],
                    "OwnerTitle": owner_title[:255],
                    "OwnerRole": owner_role[:255],
                    "OwnerDivision": owner_division[:255],
                    "OwnerPersona": owner_persona[:255],
                    "ManagerId": manager_id,
                    "ManagerName": manager_name,
                    "ManagerPersona": manager_persona[:255],
                    "UnitGroup": unit_group,
                    "SalesRegion": sales_region,
                    "MotionType": motion,
                    "MotionPrimaryPersona": primary_motion_persona(motion)[:255],
                    "OwnershipAlignment": alignment[:255],
                    "ForecastCategory": "",
                    "StageName": "",
                    "ProductFamily": "",
                    "Competitor": "",
                    "WonLostReason": "",
                    "SubReason": "",
                    "RiskLevel": "",
                    "RiskBand": "",
                    "WinScoreBand": "",
                    "TermBucket": "",
                    "SaaSClient": "false",
                    "AxiomaClient": "false",
                    "IsClosed": "false",
                    "IsWon": "false",
                    "IsRenewal": "false",
                    "IsExpand": "false",
                    "IsLand": "false",
                    "IsServices": "false",
                    "CloseDate": "",
                    "CreatedDate": "",
                    "NextStep": "",
                    "NoNextStepFlag": 0,
                    "DealReviewApprovedFlag": 0,
                    "CommercialApprovalFlag": 0,
                    "OverdueTaskFlag": 0,
                    "MonthDate": f"{month}-01",
                    "MonthLabel": month,
                    "FYLabel": fy_label,
                    "CloseQuarter": close_quarter,
                    "FiscalYear": year_value,
                    "FiscalQuarter": fiscal_quarter,
                    "SubscriptionTerm": 0.0,
                    "QuotaAmount": annual_quota,
                    "ARR": 0.0,
                    "Probability": 0.0,
                    "AgeInDays": 0.0,
                    "SalesCycleDuration": 0.0,
                    "WinScore": 0.0,
                    "RiskScore": 0.0,
                    "ActualARR": round(values["ActualARR"], 2),
                    "WeightedOpenARR": round(values["WeightedOpenARR"], 2),
                    "ProjectedARR": round(values["ProjectedARR"], 2),
                    "ExpectedARR": 0.0,
                    "LostARR": round(values["LostARR"], 2),
                    "OmittedARR": 0.0,
                    "CommitOpenARR": 0.0,
                    "BestCaseOpenARR": 0.0,
                    "PipelineOpenARR": 0.0,
                    "RenewalRiskARR": round(values["RenewalRiskARR"], 2),
                    "RiskyCommitARR": round(values["RiskyCommitARR"], 2),
                    "CompetitiveLossARR": round(values["CompetitiveLossARR"], 2),
                    "OpenARR": 0.0,
                    "LandARR": round(
                        values["ProjectedARR"] if motion == "Land" else 0.0, 2
                    ),
                    "ExpandARR": round(
                        values["ProjectedARR"] if motion == "Expand" else 0.0, 2
                    ),
                    "RenewalARR": round(
                        values["ProjectedARR"] if motion == "Renewal" else 0.0, 2
                    ),
                    "ServicesARR": 0.0,
                    "SaaSARR": 0.0,
                    "PSARR": 0.0,
                    "LandOppCount": 0,
                    "LandWonCount": 0,
                    "ExpandOppCount": 0,
                    "ExpandWonCount": 0,
                    "RenewalOppCount": 0,
                    "RenewalWonCount": 0,
                    "RenewalRiskCount": 0,
                    "RiskyCommitCount": 0,
                    "CompetitiveLossCount": 0,
                    "OmittedOppCount": 0,
                    "LateStageOppCount": 0,
                    "ReviewCandidateCount": 0,
                    "NeedsReviewOwnershipCount": 0,
                    "NeedsReviewOwnershipARR": 0.0,
                    "RegressionForecastARR": round(forecast, 2),
                    "RegressionUpperARR": round(max(0.0, forecast + interval), 2),
                    "RegressionLowerARR": round(max(0.0, forecast - interval), 2),
                    "TargetARR": target_monthly,
                }
            )

    rows = detail_rows + trend_rows
    logger.info("  Detail rows: %d", len(detail_rows))
    logger.info("  Trend rows: %d", len(trend_rows))
    logger.info("  Total rows: %d", len(rows))

    field_names = [
        "RecordType",
        "Id",
        "OpportunityName",
        "AccountId",
        "AccountName",
        "OwnerId",
        "OwnerName",
        "OwnerTitle",
        "OwnerRole",
        "OwnerDivision",
        "OwnerPersona",
        "ManagerId",
        "ManagerName",
        "ManagerPersona",
        "UnitGroup",
        "SalesRegion",
        "MotionType",
        "MotionPrimaryPersona",
        "OwnershipAlignment",
        "ForecastCategory",
        "StageName",
        "StageProgression",
        "ProductFamily",
        "Competitor",
        "WonLostReason",
        "SubReason",
        "RiskLevel",
        "RiskBand",
        "WinScoreBand",
        "TermBucket",
        "SaaSClient",
        "AxiomaClient",
        "IsClosed",
        "IsWon",
        "IsRenewal",
        "IsExpand",
        "IsLand",
        "IsServices",
        "CloseDate",
        "CreatedDate",
        "NextStep",
        "NoNextStepFlag",
        "DealReviewApprovedFlag",
        "SubmitForCommercialApprovalDate",
        "CommercialApprovalDate",
        "CommercialApprovalFlag",
        "CommercialApprovalAgeDays",
        "StaleCommercialApprovalFlag",
        "OverdueTaskFlag",
        "MonthDate",
        "MonthLabel",
        "FYLabel",
        "CloseQuarter",
        "FiscalYear",
        "FiscalQuarter",
        "SubscriptionTerm",
        "QuotaAmount",
        "ARR",
        "Probability",
        "AgeInDays",
        "SalesCycleDuration",
        "WinScore",
        "RiskScore",
        "ActualARR",
        "WeightedOpenARR",
        "ProjectedARR",
        "ExpectedARR",
        "LostARR",
        "OmittedARR",
        "CommitOpenARR",
        "BestCaseOpenARR",
        "PipelineOpenARR",
        "RenewalRiskARR",
        "RiskyCommitARR",
        "CompetitiveLossARR",
        "OpenARR",
        "LandARR",
        "ExpandARR",
        "RenewalARR",
        "ServicesARR",
        "SaaSARR",
        "PSARR",
        "LandOppCount",
        "LandWonCount",
        "ExpandOppCount",
        "ExpandWonCount",
        "RenewalOppCount",
        "RenewalWonCount",
        "RenewalRiskCount",
        "RiskyCommitCount",
        "CompetitiveLossCount",
        "OmittedOppCount",
        "LateStageOppCount",
        "ReviewCandidateCount",
        "NeedsReviewOwnershipCount",
        "NeedsReviewOwnershipARR",
        "PendingApprovalARR",
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
    logger.info("  CSV: %d bytes", len(csv_bytes))

    fields_meta = [
        _dim("RecordType", "Record Type"),
        _dim("Id", "Opportunity ID"),
        _dim("OpportunityName", "Opportunity"),
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account"),
        _dim("OwnerId", "Owner ID"),
        _dim("OwnerName", "Owner"),
        _dim("OwnerTitle", "Owner Title"),
        _dim("OwnerRole", "Owner Role"),
        _dim("OwnerDivision", "Owner Division"),
        _dim("OwnerPersona", "Owner Persona"),
        _dim("ManagerId", "Manager ID"),
        _dim("ManagerName", "Manager"),
        _dim("ManagerPersona", "Manager Persona"),
        _dim("UnitGroup", "Unit Group"),
        _dim("SalesRegion", "Sales Region"),
        _dim("MotionType", "Motion"),
        _dim("MotionPrimaryPersona", "Motion Primary Persona"),
        _dim("OwnershipAlignment", "Ownership Alignment"),
        _dim("ForecastCategory", "Forecast Category"),
        _dim("StageName", "Stage"),
        _dim("StageProgression", "Stage Progression"),
        _dim("ProductFamily", "Product Family"),
        _dim("Competitor", "Competitor"),
        _dim("WonLostReason", "Won/Lost Reason"),
        _dim("SubReason", "Sub-Reason"),
        _dim("RiskLevel", "Risk Level"),
        _dim("RiskBand", "Risk Band"),
        _dim("WinScoreBand", "Win Score Band"),
        _dim("TermBucket", "Term Bucket"),
        _dim("SaaSClient", "SaaS Client"),
        _dim("AxiomaClient", "Axioma Client"),
        _dim("IsClosed", "Is Closed"),
        _dim("IsWon", "Is Won"),
        _dim("IsRenewal", "Is Renewal"),
        _dim("IsExpand", "Is Expand"),
        _dim("IsLand", "Is Land"),
        _dim("IsServices", "Is Services"),
        _date("CloseDate", "Close Date"),
        _date("CreatedDate", "Created Date"),
        _dim("NextStep", "Next Step"),
        _measure("NoNextStepFlag", "No Next Step Flag", scale=0, precision=6),
        _measure("DealReviewApprovedFlag", "GS Deal Review Flag", scale=0, precision=6),
        _date("SubmitForCommercialApprovalDate", "Submit for Commercial Approval Date"),
        _date("CommercialApprovalDate", "Commercial Approval Date"),
        _measure(
            "CommercialApprovalFlag", "Commercial Approval Flag", scale=0, precision=6
        ),
        _measure(
            "CommercialApprovalAgeDays",
            "Commercial Approval Age (Days)",
            scale=1,
            precision=6,
        ),
        _measure(
            "StaleCommercialApprovalFlag",
            "Stale Commercial Approval Flag",
            scale=0,
            precision=6,
        ),
        _measure("OverdueTaskFlag", "Overdue Task Flag", scale=0, precision=6),
        _date("MonthDate", "Month"),
        _dim("MonthLabel", "Month Label"),
        _dim("FYLabel", "Fiscal Year Label"),
        _dim("CloseQuarter", "Close Quarter"),
        _measure("FiscalYear", "Fiscal Year", scale=0, precision=5),
        _measure("FiscalQuarter", "Fiscal Quarter", scale=0, precision=3),
        _measure("SubscriptionTerm", "Subscription Term", scale=1, precision=6),
        _measure("QuotaAmount", "Quota Amount"),
        _measure("ARR", "ARR"),
        _measure("Probability", "Probability", scale=1, precision=5),
        _measure("AgeInDays", "Age In Days", scale=1, precision=6),
        _measure("SalesCycleDuration", "Sales Cycle Duration", scale=1, precision=6),
        _measure("WinScore", "Win Score", scale=1, precision=5),
        _measure("RiskScore", "Risk Score", scale=1, precision=5),
        _measure("ActualARR", "Actual ARR"),
        _measure("WeightedOpenARR", "Weighted Open ARR"),
        _measure("ProjectedARR", "Projected ARR"),
        _measure("ExpectedARR", "Expected ARR"),
        _measure("LostARR", "Lost ARR"),
        _measure("OmittedARR", "Omitted ARR"),
        _measure("CommitOpenARR", "Commit Open ARR"),
        _measure("BestCaseOpenARR", "Best Case Open ARR"),
        _measure("PipelineOpenARR", "Pipeline Open ARR"),
        _measure("RenewalRiskARR", "Renewal Risk ARR"),
        _measure("RiskyCommitARR", "Risky Commit ARR"),
        _measure("CompetitiveLossARR", "Competitive Loss ARR"),
        _measure("OpenARR", "Open ARR"),
        _measure("LandARR", "Land ARR"),
        _measure("ExpandARR", "Expand ARR"),
        _measure("RenewalARR", "Renewal ARR"),
        _measure("ServicesARR", "Services ARR"),
        _measure("SaaSARR", "SaaS ARR"),
        _measure("PSARR", "PS ARR"),
        _measure("LandOppCount", "Land Opportunity Count", scale=0, precision=6),
        _measure("LandWonCount", "Land Won Count", scale=0, precision=6),
        _measure("ExpandOppCount", "Expand Opportunity Count", scale=0, precision=6),
        _measure("ExpandWonCount", "Expand Won Count", scale=0, precision=6),
        _measure("RenewalOppCount", "Renewal Opportunity Count", scale=0, precision=6),
        _measure("RenewalWonCount", "Renewal Won Count", scale=0, precision=6),
        _measure("RenewalRiskCount", "Renewal Risk Count", scale=0, precision=6),
        _measure("RiskyCommitCount", "Risky Commit Count", scale=0, precision=6),
        _measure(
            "CompetitiveLossCount", "Competitive Loss Count", scale=0, precision=6
        ),
        _measure("OmittedOppCount", "Omitted Opportunity Count", scale=0, precision=6),
        _measure(
            "LateStageOppCount", "Late Stage Opportunity Count", scale=0, precision=6
        ),
        _measure(
            "ReviewCandidateCount", "Review Candidate Count", scale=0, precision=6
        ),
        _measure(
            "NeedsReviewOwnershipCount",
            "Needs Review Ownership Count",
            scale=0,
            precision=6,
        ),
        _measure("NeedsReviewOwnershipARR", "Needs Review Ownership ARR"),
        _measure("PendingApprovalARR", "Pending Approval ARR"),
        _measure("RegressionForecastARR", "Regression Forecast ARR"),
        _measure("RegressionUpperARR", "Regression Upper ARR"),
        _measure("RegressionLowerARR", "Regression Lower ARR"),
        _measure("TargetARR", "Target ARR"),
    ]

    upload_ok = upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)
    return upload_ok, len(rows)


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_fy = coalesce_filter("f_fy", "FYLabel")
    filter_region = coalesce_filter("f_region", "SalesRegion")
    filter_quarter = coalesce_filter("f_quarter", "CloseQuarter")
    filter_manager = coalesce_filter("f_manager", "ManagerName")
    filter_owner = coalesce_filter("f_owner", "OwnerName")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_fy
        + filter_region
        + filter_quarter
        + filter_manager
        + filter_owner
    )
    trend = (
        load
        + 'q = filter q by RecordType == "trend";\n'
        + filter_unit
        + filter_fy
        + filter_region
        + filter_quarter
        + filter_manager
        + filter_owner
    )

    q1_filters = (
        _rebind(filter_unit, "q1")
        + _rebind(filter_fy, "q1")
        + _rebind(filter_region, "q1")
        + _rebind(filter_quarter, "q1")
        + _rebind(filter_manager, "q1")
        + _rebind(filter_owner, "q1")
    )
    q2_filters = (
        _rebind(filter_unit, "q2")
        + _rebind(filter_fy, "q2")
        + _rebind(filter_region, "q2")
        + _rebind(filter_quarter, "q2")
        + _rebind(filter_manager, "q2")
        + _rebind(filter_owner, "q2")
    )
    q3_filters = (
        _rebind(filter_unit, "q3")
        + _rebind(filter_fy, "q3")
        + _rebind(filter_region, "q3")
        + _rebind(filter_quarter, "q3")
        + _rebind(filter_manager, "q3")
        + _rebind(filter_owner, "q3")
    )
    weekly_filters = (
        _rebind(filter_unit, "q")
        + _rebind(filter_fy, "q")
        + _rebind(filter_region, "q")
        + _rebind(filter_quarter, "q")
        + _rebind(filter_manager, "q")
        + _rebind(filter_owner, "q")
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
        + "q2 = foreach q2 generate "
        + "sum(WeightedOpenARR) as weighted_open, "
        + "sum(RenewalRiskARR) as renewal_risk_arr;\n"
        + f'q3 = load "{DS}";\n'
        + 'q3 = filter q3 by RecordType == "detail";\n'
        + q3_filters
        + 'q3 = filter q3 by IsClosed == "false";\n'
        + 'q3 = filter q3 by ForecastCategory == "Commit";\n'
        + "q3 = group q3 by all;\n"
        + "q3 = foreach q3 generate sum(ARR) as commit_open;\n"
        + f'q4 = load "{DS}";\n'
        + 'q4 = filter q4 by RecordType == "detail";\n'
        + q3_filters.replace("q3", "q4")
        + 'q4 = filter q4 by IsClosed == "false";\n'
        + 'q4 = filter q4 by ForecastCategory == "Best Case";\n'
        + "q4 = group q4 by all;\n"
        + "q4 = foreach q4 generate sum(ARR) as best_case_open;\n"
        + f'q5 = load "{DS}";\n'
        + 'q5 = filter q5 by RecordType == "detail";\n'
        + q3_filters.replace("q3", "q5")
        + 'q5 = filter q5 by IsClosed == "false";\n'
        + 'q5 = filter q5 by ForecastCategory == "Pipeline";\n'
        + "q5 = group q5 by all;\n"
        + "q5 = foreach q5 generate sum(ARR) as pipeline_open;\n"
        + f'q6 = load "{DS}";\n'
        + 'q6 = filter q6 by RecordType == "detail";\n'
        + q3_filters.replace("q3", "q6")
        + "q6 = group q6 by (OwnerName, FYLabel);\n"
        + "q6 = foreach q6 generate max(QuotaAmount) as owner_quota;\n"
        + "q6 = group q6 by all;\n"
        + "q6 = foreach q6 generate sum(owner_quota) as total_quota;\n"
        + "q = cogroup q1 by all, q2 by all, q3 by all, q4 by all, q5 by all, q6 by all;\n"
        + "q = foreach q generate "
        + "coalesce(sum(q1.actual_closed), 0) as actual_closed, "
        + "coalesce(sum(q2.weighted_open), 0) as weighted_open, "
        + "(coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) as projected, "
        + "coalesce(sum(q2.renewal_risk_arr), 0) as renewal_risk_arr, "
        + "coalesce(sum(q3.commit_open), 0) as commit_open, "
        + "coalesce(sum(q4.best_case_open), 0) as best_case_open, "
        + "coalesce(sum(q5.pipeline_open), 0) as pipeline_open, "
        + "(coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q3.commit_open), 0)) as commit_forecast, "
        + "(coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q3.commit_open), 0) + coalesce(sum(q4.best_case_open), 0)) as best_case_forecast, "
        + "(coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q3.commit_open), 0) + coalesce(sum(q4.best_case_open), 0) + coalesce(sum(q5.pipeline_open), 0)) as field_call_total, "
        + "coalesce(sum(q6.total_quota), 0) as target, "
        + "((coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) - coalesce(sum(q6.total_quota), 0)) as gap_to_plan, "
        + "(case when (coalesce(sum(q6.total_quota), 0) - (coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q3.commit_open), 0) + coalesce(sum(q4.best_case_open), 0))) > 0 "
        + "then (coalesce(sum(q6.total_quota), 0) - (coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q3.commit_open), 0) + coalesce(sum(q4.best_case_open), 0))) else 0 end) as needed_promotion, "
        + "(case when (coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q3.commit_open), 0) + coalesce(sum(q4.best_case_open), 0) + coalesce(sum(q5.pipeline_open), 0)) > 0 "
        + "then ((coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q2.weighted_open), 0)) / "
        + "(coalesce(sum(q1.actual_closed), 0) + coalesce(sum(q3.commit_open), 0) + coalesce(sum(q4.best_case_open), 0) + coalesce(sum(q5.pipeline_open), 0))) * 100 "
        + "else 0 end) as forecast_confidence;"
    )

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
        + q3_filters.replace("q3", "q4")
        + "q4 = group q4 by (OwnerName, FYLabel);\n"
        + "q4 = foreach q4 generate max(QuotaAmount) as owner_quota;\n"
        + "q4 = group q4 by all;\n"
        + "q4 = foreach q4 generate sum(owner_quota) as target_arr;\n"
        + "q5 = cogroup q1 by all, q2 by all, q3 by all, q4 by all;\n"
        + "q5 = foreach q5 generate "
        + '"4 Needed Promotion" as BridgeStep, '
        + "(coalesce(sum(q4.target_arr), 0) - "
        + "(coalesce(sum(q1.closed_arr), 0) + coalesce(sum(q2.commit_arr), 0) + coalesce(sum(q3.best_case_arr), 0))) as BridgeARR;\n"
        + "q = union q1l, q2l, q3l, q5;\n"
        + "q = order q by BridgeStep asc;"
    )

    steps = {
        "f_unit": af("UnitGroup", ds_meta),
        "f_fy": af("FYLabel", ds_meta, start=f'["{CURRENT_FY_LABEL}"]'),
        "f_region": af("SalesRegion", ds_meta),
        "f_quarter": af("CloseQuarter", ds_meta),
        "f_manager": af("ManagerName", ds_meta),
        "f_owner": af("OwnerName", ds_meta),
        "s_summary": sq(summary),
        "s_exception_summary": sq(
            f'q1 = load "{DS}";\n'
            + 'q1 = filter q1 by RecordType == "detail";\n'
            + q1_filters
            + 'q1 = filter q1 by IsClosed == "false";\n'
            + 'q1 = filter q1 by (ForecastCategory == "Commit") or (ForecastCategory == "Best Case");\n'
            + "q1 = filter q1 by RiskyCommitARR > 0;\n"
            + "q1 = group q1 by all;\n"
            + "q1 = foreach q1 generate "
            + "count() as risky_commit_count, "
            + "sum(RiskyCommitARR) as risky_commit_arr;\n"
            + f'q2 = load "{DS}";\n'
            + 'q2 = filter q2 by RecordType == "detail";\n'
            + q2_filters
            + 'q2 = filter q2 by IsClosed == "false";\n'
            + 'q2 = filter q2 by ForecastCategory == "Omitted";\n'
            + "q2 = group q2 by all;\n"
            + "q2 = foreach q2 generate sum(ARR) as omitted_arr;\n"
            + f'q3 = load "{DS}";\n'
            + 'q3 = filter q3 by RecordType == "detail";\n'
            + q3_filters
            + 'q3 = filter q3 by IsClosed == "false";\n'
            + "q3 = filter q3 by ARR >= 500000;\n"
            + 'q3 = filter q3 by (ForecastCategory == "Omitted") or (RiskyCommitARR > 0) or ((ForecastCategory == "Best Case") and (AgeInDays >= 60)) or ((ForecastCategory == "Pipeline") and ((StageProgression == "4 Shortlisted") or (StageProgression == "5 Preferred") or (StageProgression == "6 Contracting"))) or (CommercialApprovalFlag > 0) or (OverdueTaskFlag > 0);\n'
            + "q3 = group q3 by all;\n"
            + "q3 = foreach q3 generate count() as review_candidate_count;\n"
            + "q = cogroup q1 by all, q2 by all, q3 by all;\n"
            + "q = foreach q generate "
            + "coalesce(sum(q1.risky_commit_count), 0) as risky_commit_count, "
            + "coalesce(sum(q1.risky_commit_arr), 0) as risky_commit_arr, "
            + "coalesce(sum(q2.omitted_arr), 0) as omitted_arr, "
            + "coalesce(sum(q3.review_candidate_count), 0) as review_candidate_count;"
        ),
        "s_monthly_trajectory": sq(
            trend
            + "q = group q by MonthDate;\n"
            + "q = foreach q generate MonthDate, "
            + "sum(ActualARR) as ActualARR, "
            + "sum(WeightedOpenARR) as WeightedOpenARR, "
            + "sum(RegressionForecastARR) as RegressionForecastARR, "
            + "sum(RegressionUpperARR) as RegressionUpperARR, "
            + "sum(RegressionLowerARR) as RegressionLowerARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_plan_bridge": sq(plan_bridge),
        "s_manager_action_queue": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by ((ForecastCategory == "Commit") and ARR >= 500000) or ((ForecastCategory == "Best Case") and ARR >= 500000) or ((ForecastCategory == "Pipeline") and ARR >= 750000) or ((ForecastCategory == "Omitted") and ARR >= 250000);\n'
            + "q = group q by (OpportunityName, AccountName, OwnerName, MotionType, ForecastCategory, CloseQuarter, StageProgression, NextStep, Id);\n"
            + "q = foreach q generate OpportunityName, AccountName, OwnerName, MotionType, CloseQuarter, StageProgression, "
            + "max(ARR) as ARR, "
            + "max(AgeInDays) as AgeInDays, "
            + "max(NoNextStepFlag) as NoNextStepFlag, "
            + "max(NeedsReviewOwnershipCount) as NeedsReviewOwnershipCount, "
            + "max(CommercialApprovalFlag) as CommercialApprovalFlag, "
            + "max(StaleCommercialApprovalFlag) as StaleCommercialApprovalFlag, "
            + "max(OverdueTaskFlag) as OverdueTaskFlag, "
            + '(case when ForecastCategory == "Omitted" then 4 when ForecastCategory == "Commit" then 3 when ForecastCategory == "Best Case" then 2 else 1 end) as ForecastPriority, '
            + '((max(NeedsReviewOwnershipCount) * 1000000) + (max(StaleCommercialApprovalFlag) * 800000) + (max(CommercialApprovalFlag) * 400000) + (max(OverdueTaskFlag) * 200000) + (max(NoNextStepFlag) * 100000) + ((case when ForecastCategory == "Omitted" then 4 when ForecastCategory == "Commit" then 3 when ForecastCategory == "Best Case" then 2 else 1 end) * 10000) + max(AgeInDays) + (max(ARR) / 1000000)) as ActionPriorityScore, '
            + '(case when max(NeedsReviewOwnershipCount) > 0 then ("Ownership review | " + StageProgression) '
            + 'when max(StaleCommercialApprovalFlag) > 0 then ("Approval pending >14d | " + StageProgression) '
            + 'when max(CommercialApprovalFlag) > 0 then ("Approval pending | " + StageProgression) '
            + 'when max(OverdueTaskFlag) > 0 then ("Overdue task | " + StageProgression) '
            + 'when ForecastCategory == "Omitted" then ("Omitted | " + StageProgression) '
            + 'when max(NoNextStepFlag) > 0 then (ForecastCategory + " | Missing next step") '
            + 'when ForecastCategory == "Commit" then ("Commit | " + StageProgression) '
            + 'when ForecastCategory == "Best Case" then ("Best Case | " + StageProgression) '
            + 'else ("Pipeline | " + StageProgression) end) as DealPulse, '
            + '(case when max(NeedsReviewOwnershipCount) > 0 then "Confirm owner and handoff" '
            + 'when max(StaleCommercialApprovalFlag) > 0 then "Escalate stalled approval" '
            + 'when max(CommercialApprovalFlag) > 0 then "Unblock commercial approval" '
            + 'when max(OverdueTaskFlag) > 0 then "Clear overdue task now" '
            + 'when ForecastCategory == "Omitted" then "Classify or close" '
            + 'when max(NoNextStepFlag) > 0 then "Set next step before review" '
            + 'when ForecastCategory == "Commit" then "Confirm close plan" '
            + 'when ForecastCategory == "Best Case" then "Approve promotion evidence" '
            + 'else "Decide quarter and category" end) as Escalation, '
            + '(case when max(NextStep) == "" then "-" else max(NextStep) end) as NextStep, '
            + "Id;\n"
            + "q = order q by ActionPriorityScore desc;\n"
            + "q = limit q 10;"
        ),
        "s_forecast_quality": sq(
            f'q1 = load "{DS}";\n'
            + 'q1 = filter q1 by RecordType == "detail";\n'
            + q1_filters
            + 'q1 = filter q1 by IsClosed == "false";\n'
            + 'q1 = filter q1 by ForecastCategory == "Commit";\n'
            + "q1 = group q1 by CloseQuarter;\n"
            + "q1 = foreach q1 generate CloseQuarter, sum(ARR) as CommitARR;\n"
            + f'q2 = load "{DS}";\n'
            + 'q2 = filter q2 by RecordType == "detail";\n'
            + q2_filters
            + 'q2 = filter q2 by IsClosed == "false";\n'
            + 'q2 = filter q2 by ForecastCategory == "Best Case";\n'
            + "q2 = group q2 by CloseQuarter;\n"
            + "q2 = foreach q2 generate CloseQuarter, sum(ARR) as BestCaseARR;\n"
            + f'q3 = load "{DS}";\n'
            + 'q3 = filter q3 by RecordType == "detail";\n'
            + q3_filters
            + 'q3 = filter q3 by IsClosed == "false";\n'
            + 'q3 = filter q3 by ForecastCategory == "Pipeline";\n'
            + "q3 = group q3 by CloseQuarter;\n"
            + "q3 = foreach q3 generate CloseQuarter, sum(ARR) as PipelineARR;\n"
            + f'q4 = load "{DS}";\n'
            + 'q4 = filter q4 by RecordType == "detail";\n'
            + q3_filters.replace("q3", "q4")
            + 'q4 = filter q4 by IsClosed == "false";\n'
            + 'q4 = filter q4 by ForecastCategory == "Omitted";\n'
            + "q4 = group q4 by CloseQuarter;\n"
            + "q4 = foreach q4 generate CloseQuarter, sum(ARR) as OmittedARR;\n"
            + f'q5 = load "{DS}";\n'
            + 'q5 = filter q5 by RecordType == "detail";\n'
            + q3_filters.replace("q3", "q5")
            + 'q5 = filter q5 by IsClosed == "false";\n'
            + "q5 = group q5 by CloseQuarter;\n"
            + "q5 = foreach q5 generate CloseQuarter, sum(RiskyCommitARR) as RiskyCommitARR;\n"
            + "q = cogroup q1 by CloseQuarter full, q2 by CloseQuarter full, q3 by CloseQuarter full, q4 by CloseQuarter full, q5 by CloseQuarter;\n"
            + "q = foreach q generate coalesce(q1.CloseQuarter, q2.CloseQuarter, q3.CloseQuarter, q4.CloseQuarter, q5.CloseQuarter) as CloseQuarter, "
            + "coalesce(sum(q1.CommitARR), 0) as CommitARR, "
            + "coalesce(sum(q2.BestCaseARR), 0) as BestCaseARR, "
            + "coalesce(sum(q3.PipelineARR), 0) as PipelineARR, "
            + "coalesce(sum(q4.OmittedARR), 0) as OmittedARR, "
            + "coalesce(sum(q5.RiskyCommitARR), 0) as RiskyCommitARR;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        "s_process_compliance": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (ManagerName, OwnerName, OwnerPersona);\n"
            + "q = foreach q generate ManagerName, OwnerName, OwnerPersona, "
            + "sum(OpenARR) as OpenARR, "
            + "sum(ReviewCandidateCount) as ReviewCandidateCount, "
            + "sum(NoNextStepFlag) as NoNextStepCount, "
            + "sum(OmittedOppCount) as OmittedOppCount, "
            + "sum(LateStageOppCount) as LateStageOppCount, "
            + "sum(DealReviewApprovedFlag) as DealReviewCount, "
            + "sum(CommercialApprovalFlag) as PendingApprovalCount, "
            + "sum(StaleCommercialApprovalFlag) as StaleApprovalCount, "
            + "sum(PendingApprovalARR) as PendingApprovalARR, "
            + "sum(OverdueTaskFlag) as OverdueTaskCount, "
            + "sum(NeedsReviewOwnershipCount) as OwnershipReviewCount, "
            + "sum(NeedsReviewOwnershipARR) as OwnershipReviewARR, "
            + "((sum(NeedsReviewOwnershipCount) * 1000000) + "
            + "(sum(StaleCommercialApprovalFlag) * 800000) + "
            + "(sum(CommercialApprovalFlag) * 400000) + "
            + "(sum(OverdueTaskFlag) * 200000) + "
            + "(sum(NoNextStepFlag) * 100000) + "
            + "(sum(ReviewCandidateCount) * 10000) + "
            + "(sum(OpenARR) / 1000000)) as ProcessPressureScore;\n"
            + "q = order q by ProcessPressureScore desc;\n"
            + "q = limit q 20;"
        ),
        "s_process_issue_trend": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by CloseQuarter;\n"
            + "q = foreach q generate CloseQuarter, "
            + "sum(ReviewCandidateCount) as ReviewCandidateCount, "
            + "sum(NoNextStepFlag) as NoNextStepCount, "
            + "sum(OmittedOppCount) as OmittedOppCount, "
            + "sum(CommercialApprovalFlag) as PendingApprovalCount, "
            + "sum(OverdueTaskFlag) as OverdueTaskCount, "
            + "sum(NeedsReviewOwnershipCount) as OwnershipReviewCount;\n"
            + "q = order q by CloseQuarter asc;"
        ),
        "s_stage_aging_pressure": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (ManagerName, OwnerName);\n"
            + "q = foreach q generate ManagerName, OwnerName, "
            + "count() as OpenOppCount, "
            + "sum(ARR) as OpenARR, "
            + "avg(AgeInDays) as AvgAgeInDays, "
            + "sum(NoNextStepFlag) as NoNextStepCount, "
            + "sum(CommitOpenARR) as CommitARR, "
            + "sum(BestCaseOpenARR) as BestCaseARR, "
            + "sum(OmittedARR) as OmittedARR;\n"
            + "q = order q by AvgAgeInDays desc;\n"
            + "q = limit q 10;"
        ),
        "s_closed_win_rate": sq(
            detail
            + 'q = filter q by IsClosed == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as ClosedOppCount, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as WonOppCount, '
            + '(case when count() > 0 then (sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100 else 0 end) as WinRatePct;'
        ),
        "s_advanced_stage_conversion": sq(
            detail
            + 'q = filter q by (StageProgression == "4 Shortlisted") or (StageProgression == "5 Preferred") or (StageProgression == "6 Contracting") or (StageProgression == "8 Won");\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as AdvancedStageOppCount, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as AdvancedStageWonCount, '
            + '(case when count() > 0 then (sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100 else 0 end) as AdvancedStageConversionPct;'
        ),
        "s_stage_conversion_table": sq(
            detail
            + 'q = filter q by StageProgression != "";\n'
            + "q = group q by StageProgression;\n"
            + "q = foreach q generate StageProgression, "
            + "count() as OppCount, "
            + 'sum(case when IsWon == "true" then 1 else 0 end) as WonOppCount, '
            + '(case when count() > 0 then (sum(case when IsWon == "true" then 1 else 0 end) / count()) * 100 else 0 end) as ConversionPct, '
            + "sum(OpenARR) as OpenARR, "
            + "sum(ActualARR) as ClosedWonARR;\n"
            + "q = order q by StageProgression asc;\n"
            + "q = limit q 8;"
        ),
        "s_region_mix": sq(
            detail
            + "q = group q by SalesRegion;\n"
            + "q = foreach q generate SalesRegion, "
            + "sum(ProjectedARR) as ProjectedARR, "
            + "sum(RiskyCommitARR) as RiskyCommitARR, "
            + "sum(OmittedARR) as OmittedARR, "
            + "sum(ReviewCandidateCount) as ReviewCandidateCount, "
            + "sum(NoNextStepFlag) as NoNextStepCount, "
            + "sum(NeedsReviewOwnershipARR) as OwnershipReviewARR;\n"
            + "q = order q by ProjectedARR desc;"
        ),
        "s_product_heatmap": sq(
            detail
            + 'q = filter q by ProductFamily != "";\n'
            + "q = group q by (ProductFamily, CloseQuarter);\n"
            + "q = foreach q generate ProductFamily, CloseQuarter, sum(ProjectedARR) as ProjectedARR;\n"
            + "q = order q by CloseQuarter asc, ProductFamily asc;"
        ),
        "s_stage_forecast_heatmap": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (StageProgression, ForecastCategory);\n"
            + "q = foreach q generate StageProgression, ForecastCategory, sum(ARR) as OpenARR;\n"
            + "q = order q by StageProgression asc;"
        ),
        "s_owner_confidence": sq(
            detail
            + "q = group q by (ManagerName, OwnerName);\n"
            + "q = foreach q generate ManagerName, OwnerName, "
            + "count() as OpenOppCount, "
            + "(case when max(QuotaAmount) > 0 then ((sum(ActualARR) + sum(WeightedOpenARR)) / max(QuotaAmount)) * 100 else 0 end) as AttainmentPct, "
            + "(case when sum(ARR) > 0 then (sum(WeightedOpenARR) / sum(ARR)) * 100 else 0 end) as ConfidencePct, "
            + "(sum(ActualARR) + sum(WeightedOpenARR)) as ProjectedARR, "
            + "sum(NoNextStepFlag) as NoNextStepCount, "
            + "sum(OmittedARR) as OmittedARR, "
            + "sum(RiskyCommitARR) as CommitRiskARR;\n"
            + "q = order q by ProjectedARR desc;\n"
            + "q = limit q 10;"
        ),
        "s_product_pressure": sq(
            detail
            + 'q = filter q by ProductFamily != "";\n'
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, "
            + "sum(ProjectedARR) as ProjectedARR, "
            + "sum(RiskyCommitARR) as CommitRiskARR, "
            + "sum(OmittedARR) as OmittedARR, "
            + "sum(ReviewCandidateCount) as ReviewCandidateCount, "
            + "sum(NoNextStepFlag) as NoNextStepCount, "
            + "sum(NeedsReviewOwnershipARR) as OwnershipReviewARR;\n"
            + "q = order q by ProjectedARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_motion_issue_trend": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = foreach q generate FiscalQuarter, CloseQuarter, "
            + '(case when MotionType == "Land" then ReviewCandidateCount else 0 end) as LandReviewCount, '
            + '(case when MotionType == "Expand" then ReviewCandidateCount else 0 end) as ExpandReviewCount, '
            + '(case when MotionType == "Renewal" then ReviewCandidateCount else 0 end) as RenewalReviewCount;\n'
            + "q = group q by (FiscalQuarter, CloseQuarter);\n"
            + "q = foreach q generate FiscalQuarter, CloseQuarter, "
            + "sum(LandReviewCount) as LandReviewCount, "
            + "sum(ExpandReviewCount) as ExpandReviewCount, "
            + "sum(RenewalReviewCount) as RenewalReviewCount;\n"
            + "q = order q by FiscalQuarter asc;"
        ),
        "s_owner_gap": sq(
            f'q1 = load "{DS}";\n'
            + 'q1 = filter q1 by RecordType == "detail";\n'
            + q1_filters
            + "q1 = group q1 by (ManagerName, OwnerName);\n"
            + "q1 = foreach q1 generate ManagerName, OwnerName, "
            + "sum(ActualARR) as ClosedWonARR, "
            + "(sum(ActualARR) + sum(WeightedOpenARR)) as WeightedModelARR, "
            + "max(QuotaAmount) as QuotaAmount, "
            + "sum(ReviewCandidateCount) as ReviewCandidateCount, "
            + "sum(NoNextStepFlag) as NoNextStepCount, "
            + "sum(NeedsReviewOwnershipARR) as OwnershipReviewARR;\n"
            + f'q2 = load "{DS}";\n'
            + 'q2 = filter q2 by RecordType == "detail";\n'
            + q2_filters
            + 'q2 = filter q2 by ForecastCategory == "Commit";\n'
            + "q2 = group q2 by (ManagerName, OwnerName);\n"
            + "q2 = foreach q2 generate ManagerName, OwnerName, sum(ARR) as CommitARR;\n"
            + f'q3 = load "{DS}";\n'
            + 'q3 = filter q3 by RecordType == "detail";\n'
            + q3_filters
            + 'q3 = filter q3 by ForecastCategory == "Best Case";\n'
            + "q3 = group q3 by (ManagerName, OwnerName);\n"
            + "q3 = foreach q3 generate ManagerName, OwnerName, sum(ARR) as BestCaseARR;\n"
            + "q = cogroup q1 by (ManagerName, OwnerName) full, q2 by (ManagerName, OwnerName) full, q3 by (ManagerName, OwnerName);\n"
            + "q = foreach q generate coalesce(q1.ManagerName, q2.ManagerName, q3.ManagerName) as ManagerName, "
            + "coalesce(q1.OwnerName, q2.OwnerName, q3.OwnerName) as OwnerName, "
            + "coalesce(sum(q1.ClosedWonARR), 0) as ClosedWonARR, "
            + "coalesce(sum(q1.WeightedModelARR), 0) as WeightedModelARR, "
            + "(coalesce(sum(q1.ClosedWonARR), 0) + coalesce(sum(q2.CommitARR), 0)) as CommitForecastARR, "
            + "(coalesce(sum(q1.ClosedWonARR), 0) + coalesce(sum(q2.CommitARR), 0) + coalesce(sum(q3.BestCaseARR), 0)) as BestCaseForecastARR, "
            + "coalesce(max(q1.QuotaAmount), 0) as QuotaAmount, "
            + "coalesce(sum(q1.ReviewCandidateCount), 0) as ReviewCandidateCount, "
            + "coalesce(sum(q1.NoNextStepCount), 0) as NoNextStepCount, "
            + "coalesce(sum(q1.OwnershipReviewARR), 0) as OwnershipReviewARR, "
            + "(coalesce(max(q1.QuotaAmount), 0) - (coalesce(sum(q1.ClosedWonARR), 0) + coalesce(sum(q2.CommitARR), 0) + coalesce(sum(q3.BestCaseARR), 0))) as NeededPromotionARR;\n"
            + "q = order q by NeededPromotionARR desc;\n"
            + "q = limit q 10;"
        ),
        "s_top_commit_protection": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by ForecastCategory == "Commit";\n'
            + "q = filter q by (ARR >= 500000) or (RiskyCommitARR > 0) or (AgeInDays >= 60);\n"
            + "q = group q by (OpportunityName, AccountName, OwnerName, CloseQuarter, StageProgression, NextStep, Id);\n"
            + "q = foreach q generate OpportunityName, AccountName, OwnerName, CloseQuarter, StageProgression, "
            + "max(ARR) as ARR, "
            + "max(AgeInDays) as AgeInDays, "
            + "max(CommercialApprovalAgeDays) as ApprovalAgeDays, "
            + "max(NoNextStepFlag) as NoNextStepFlag, "
            + "max(CommercialApprovalFlag) as CommercialApprovalFlag, "
            + "max(StaleCommercialApprovalFlag) as StaleCommercialApprovalFlag, "
            + "max(OverdueTaskFlag) as OverdueTaskFlag, "
            + "((max(StaleCommercialApprovalFlag) * 1000000) + (max(CommercialApprovalFlag) * 500000) + (max(OverdueTaskFlag) * 250000) + (case when max(AgeInDays) >= 90 then 150000 when max(AgeInDays) >= 60 then 75000 else 0 end) + (max(NoNextStepFlag) * 50000) + (max(ARR) / 1000000)) as QueuePriorityScore, "
            + '(case when max(StaleCommercialApprovalFlag) > 0 then ("Approval pending >14d | " + StageProgression) '
            + 'when max(CommercialApprovalFlag) > 0 then ("Approval pending | " + StageProgression) '
            + 'when max(OverdueTaskFlag) > 0 then ("Overdue task | " + StageProgression) '
            + 'else ("Commit | " + StageProgression) end) as DealPulse, '
            + '(case when max(StaleCommercialApprovalFlag) > 0 then "Manager: escalate stalled approval" '
            + 'when max(CommercialApprovalFlag) > 0 then "Manager: unblock commercial approval" '
            + 'when max(OverdueTaskFlag) > 0 then "Manager: clear overdue task now" '
            + 'when max(AgeInDays) >= 90 then "Manager: run close-plan review now" '
            + 'when max(NextStep) == "" then "Manager: update next step before call" '
            + 'else "Manager: validate commit evidence" end) as Escalation, '
            + '(case when max(NextStep) == "" then "-" else max(NextStep) end) as NextStep, '
            + "Id;\n"
            + "q = order q by QueuePriorityScore desc;\n"
            + "q = limit q 10;"
        ),
        "s_top_forecast_risk": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by (ForecastCategory == "Best Case") or (ForecastCategory == "Pipeline");\n'
            + 'q = filter q by (ARR >= 500000) and ((ForecastCategory == "Best Case") or ((ForecastCategory == "Pipeline") and ((StageProgression == "4 Shortlisted") or (StageProgression == "5 Preferred") or (StageProgression == "6 Contracting"))));\n'
            + "q = group q by (OpportunityName, AccountName, OwnerName, ForecastCategory, CloseQuarter, StageProgression, NextStep, Id);\n"
            + "q = foreach q generate OpportunityName, AccountName, OwnerName, CloseQuarter, StageProgression, "
            + "max(ARR) as ARR, "
            + "max(AgeInDays) as AgeInDays, "
            + "max(CommercialApprovalAgeDays) as ApprovalAgeDays, "
            + "max(NoNextStepFlag) as NoNextStepFlag, "
            + "max(CommercialApprovalFlag) as CommercialApprovalFlag, "
            + "max(StaleCommercialApprovalFlag) as StaleCommercialApprovalFlag, "
            + "max(OverdueTaskFlag) as OverdueTaskFlag, "
            + '(case when ForecastCategory == "Best Case" then 2 else 1 end) as ForecastPriority, '
            + '((max(StaleCommercialApprovalFlag) * 1000000) + (max(CommercialApprovalFlag) * 500000) + (max(OverdueTaskFlag) * 250000) + ((case when ForecastCategory == "Best Case" then 2 else 1 end) * 100000) + (max(NoNextStepFlag) * 50000) + max(AgeInDays) + (max(ARR) / 1000000)) as QueuePriorityScore, '
            + '(case when max(StaleCommercialApprovalFlag) > 0 then ("Approval pending >14d | " + StageProgression) '
            + 'when max(CommercialApprovalFlag) > 0 then ("Approval pending | " + StageProgression) '
            + 'when max(OverdueTaskFlag) > 0 then ("Overdue task | " + StageProgression) '
            + 'when ForecastCategory == "Best Case" then ("Best Case | " + StageProgression) else ("Pipeline | " + StageProgression) end) as DealPulse, '
            + '(case when max(StaleCommercialApprovalFlag) > 0 then "Manager: escalate stalled approval" '
            + 'when max(CommercialApprovalFlag) > 0 then "Manager: unblock commercial approval" '
            + 'when max(OverdueTaskFlag) > 0 then "Manager: clear overdue task now" '
            + 'when ForecastCategory == "Best Case" then "Manager: approve pull to commit" else "Manager: force promote or push out" end) as Escalation, '
            + '(case when max(NextStep) == "" then "-" else max(NextStep) end) as NextStep, '
            + "Id;\n"
            + "q = order q by QueuePriorityScore desc;\n"
            + "q = limit q 10;"
        ),
        "s_top_omitted": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by ForecastCategory == "Omitted";\n'
            + "q = filter q by ARR >= 250000;\n"
            + "q = group q by (OpportunityName, AccountName, OwnerName, CloseQuarter, StageProgression, NextStep, Id);\n"
            + "q = foreach q generate OpportunityName, AccountName, OwnerName, CloseQuarter, StageProgression, max(ARR) as ARR, max(AgeInDays) as AgeInDays, max(NoNextStepFlag) as NoNextStepFlag, max(OverdueTaskFlag) as OverdueTaskFlag, ((max(OverdueTaskFlag) * 1000000) + (case when max(AgeInDays) >= 120 then 500000 when max(AgeInDays) >= 60 then 250000 else 0 end) + (max(NoNextStepFlag) * 100000) + (max(ARR) / 1000000)) as QueuePriorityScore, "
            + '(case when max(OverdueTaskFlag) > 0 then ("Overdue task | " + StageProgression) else ("Omitted | " + StageProgression) end) as DealPulse, '
            + '(case when max(OverdueTaskFlag) > 0 then "Manager: clear overdue task and classify" when max(AgeInDays) >= 120 then "Manager: reclassify or close now" when max(AgeInDays) >= 60 then "Manager: assign category this week" else "Manager: confirm forecast category" end) as Escalation, '
            + '(case when max(NextStep) == "" then "-" else max(NextStep) end) as NextStep, '
            + "Id;\n"
            + "q = order q by QueuePriorityScore desc;\n"
            + "q = limit q 10;"
        ),
        "s_deal_review_candidates": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by ARR >= 500000;\n"
            + 'q = filter q by (ForecastCategory == "Omitted") or (RiskyCommitARR > 0) or ((ForecastCategory == "Best Case") and (AgeInDays >= 60)) or ((ForecastCategory == "Pipeline") and ((StageProgression == "4 Shortlisted") or (StageProgression == "5 Preferred") or (StageProgression == "6 Contracting"))) or (CommercialApprovalFlag > 0) or (OverdueTaskFlag > 0);\n'
            + "q = group q by (OpportunityName, AccountName, OwnerName, ForecastCategory, CloseQuarter, StageProgression, NextStep, Id);\n"
            + "q = foreach q generate OpportunityName, AccountName, OwnerName, CloseQuarter, StageProgression, "
            + "max(ARR) as ARR, "
            + "max(RiskScore) as RiskScore, "
            + "max(AgeInDays) as AgeInDays, "
            + "max(CommercialApprovalAgeDays) as ApprovalAgeDays, "
            + "max(NoNextStepFlag) as NoNextStepFlag, "
            + "max(NeedsReviewOwnershipCount) as NeedsReviewOwnershipCount, "
            + "max(CommercialApprovalFlag) as CommercialApprovalFlag, "
            + "max(StaleCommercialApprovalFlag) as StaleCommercialApprovalFlag, "
            + "max(OverdueTaskFlag) as OverdueTaskFlag, "
            + "((max(StaleCommercialApprovalFlag) * 1000000) + (max(CommercialApprovalFlag) * 500000) + (max(OverdueTaskFlag) * 250000) + (max(NeedsReviewOwnershipCount) * 200000) + (max(NoNextStepFlag) * 100000) + max(RiskScore) + max(AgeInDays) + (max(ARR) / 1000000)) as QueuePriorityScore, "
            + '(case when max(StaleCommercialApprovalFlag) > 0 then ("Approval pending >14d | " + StageProgression) '
            + 'when max(CommercialApprovalFlag) > 0 then ("Approval pending | " + StageProgression) '
            + 'when max(OverdueTaskFlag) > 0 then ("Overdue task | " + StageProgression) '
            + 'when ForecastCategory == "Omitted" then ("Omitted | " + StageProgression) '
            + 'when ForecastCategory == "Commit" then ("Commit | " + StageProgression) '
            + 'when ForecastCategory == "Best Case" then ("Best Case | " + StageProgression) else ("Pipeline | " + StageProgression) end) as DealPulse, '
            + '(case when max(StaleCommercialApprovalFlag) > 0 then "Manager: escalate stalled approval" '
            + 'when max(CommercialApprovalFlag) > 0 then "Manager: unblock commercial approval" '
            + 'when max(OverdueTaskFlag) > 0 then "Manager: clear overdue task now" '
            + 'when ForecastCategory == "Omitted" then "Manager: review in forecast call" '
            + 'when ForecastCategory == "Commit" then "Manager: run close-plan review" '
            + 'when ForecastCategory == "Best Case" then "Manager: run T&I on evidence" else "Manager: decide promote or push out" end) as Escalation, '
            + '(case when max(NextStep) == "" then "-" else max(NextStep) end) as NextStep, '
            + "Id;\n"
            + "q = order q by QueuePriorityScore desc;\n"
            + "q = limit q 10;"
        ),
        "s_wow_commit": sq(
            f'q1 = load "{WFS}";\n'
            + _rebind(filter_unit, "q1")
            + _rebind(filter_fy, "q1")
            + _rebind(filter_region, "q1")
            + _rebind(filter_quarter, "q1")
            + _rebind(filter_manager, "q1")
            + _rebind(filter_owner, "q1")
            + 'q1 = filter q1 by ForecastCategory == "Commit";\n'
            + "q1 = filter q1 by CurrentWeekFlag == 1;\n"
            + "q1 = group q1 by all;\n"
            + "q1 = foreach q1 generate sum(TotalARR) as CurrentCommitARR;\n"
            + f'q2 = load "{WFS}";\n'
            + _rebind(filter_unit, "q2")
            + _rebind(filter_fy, "q2")
            + _rebind(filter_region, "q2")
            + _rebind(filter_quarter, "q2")
            + _rebind(filter_manager, "q2")
            + _rebind(filter_owner, "q2")
            + 'q2 = filter q2 by ForecastCategory == "Commit";\n'
            + "q2 = filter q2 by PreviousWeekFlag == 1;\n"
            + "q2 = group q2 by all;\n"
            + "q2 = foreach q2 generate sum(TotalARR) as PrevCommitARR;\n"
            + "q = cogroup q1 by all, q2 by all;\n"
            + "q = foreach q generate "
            + "coalesce(sum(q1.CurrentCommitARR), 0) as CurrentCommitARR, "
            + "coalesce(sum(q2.PrevCommitARR), 0) as PrevCommitARR, "
            + "(coalesce(sum(q1.CurrentCommitARR), 0) - coalesce(sum(q2.PrevCommitARR), 0)) as CommitWoWChange;"
        ),
        "s_wow_best_case": sq(
            f'q1 = load "{WFS}";\n'
            + _rebind(filter_unit, "q1")
            + _rebind(filter_fy, "q1")
            + _rebind(filter_region, "q1")
            + _rebind(filter_quarter, "q1")
            + _rebind(filter_manager, "q1")
            + _rebind(filter_owner, "q1")
            + 'q1 = filter q1 by ForecastCategory == "Best Case";\n'
            + "q1 = filter q1 by CurrentWeekFlag == 1;\n"
            + "q1 = group q1 by all;\n"
            + "q1 = foreach q1 generate sum(TotalARR) as CurrentBestCaseARR;\n"
            + f'q2 = load "{WFS}";\n'
            + _rebind(filter_unit, "q2")
            + _rebind(filter_fy, "q2")
            + _rebind(filter_region, "q2")
            + _rebind(filter_quarter, "q2")
            + _rebind(filter_manager, "q2")
            + _rebind(filter_owner, "q2")
            + 'q2 = filter q2 by ForecastCategory == "Best Case";\n'
            + "q2 = filter q2 by PreviousWeekFlag == 1;\n"
            + "q2 = group q2 by all;\n"
            + "q2 = foreach q2 generate sum(TotalARR) as PrevBestCaseARR;\n"
            + "q = cogroup q1 by all, q2 by all;\n"
            + "q = foreach q generate "
            + "coalesce(sum(q1.CurrentBestCaseARR), 0) as CurrentBestCaseARR, "
            + "coalesce(sum(q2.PrevBestCaseARR), 0) as PrevBestCaseARR, "
            + "(coalesce(sum(q1.CurrentBestCaseARR), 0) - coalesce(sum(q2.PrevBestCaseARR), 0)) as BestCaseWoWChange;"
        ),
        "s_wow_pushes": sq(
            f'q = load "{WFO}";\n'
            + weekly_filters
            + "q = filter q by CurrentWeekFlag == 1;\n"
            + "q = filter q by PushThisWeekFlag == 1 or PushedOutOfQuarterFlag == 1;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate count() as PushedDeals, sum(ARR) as PushedARR;"
        ),
        "s_wow_big_bets": sq(
            f'q = load "{WFO}";\n'
            + weekly_filters
            + "q = filter q by CurrentWeekFlag == 1;\n"
            + "q = filter q by BigBetFlag == 1;\n"
            + "q = filter q by ChangedThisWeekFlag == 1;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate count() as BigBetCount, sum(ARR) as BigBetARR;"
        ),
        "s_wow_timeline": sq(
            f'q1 = load "{WFS}";\n'
            + _rebind(filter_unit, "q1")
            + _rebind(filter_fy, "q1")
            + _rebind(filter_region, "q1")
            + _rebind(filter_quarter, "q1")
            + _rebind(filter_manager, "q1")
            + _rebind(filter_owner, "q1")
            + 'q1 = filter q1 by ForecastCategory == "Commit";\n'
            + "q1 = group q1 by WeekEndDate;\n"
            + "q1 = foreach q1 generate WeekEndDate, sum(TotalARR) as CommitARR;\n"
            + f'q2 = load "{WFS}";\n'
            + _rebind(filter_unit, "q2")
            + _rebind(filter_fy, "q2")
            + _rebind(filter_region, "q2")
            + _rebind(filter_quarter, "q2")
            + _rebind(filter_manager, "q2")
            + _rebind(filter_owner, "q2")
            + 'q2 = filter q2 by ForecastCategory == "Best Case";\n'
            + "q2 = group q2 by WeekEndDate;\n"
            + "q2 = foreach q2 generate WeekEndDate, sum(TotalARR) as BestCaseARR;\n"
            + f'q3 = load "{WFS}";\n'
            + _rebind(filter_unit, "q3")
            + _rebind(filter_fy, "q3")
            + _rebind(filter_region, "q3")
            + _rebind(filter_quarter, "q3")
            + _rebind(filter_manager, "q3")
            + _rebind(filter_owner, "q3")
            + 'q3 = filter q3 by ForecastCategory == "Pipeline";\n'
            + "q3 = group q3 by WeekEndDate;\n"
            + "q3 = foreach q3 generate WeekEndDate, sum(TotalARR) as PipelineARR;\n"
            + f'q4 = load "{WFS}";\n'
            + _rebind(filter_unit, "q4")
            + _rebind(filter_fy, "q4")
            + _rebind(filter_region, "q4")
            + _rebind(filter_quarter, "q4")
            + _rebind(filter_manager, "q4")
            + _rebind(filter_owner, "q4")
            + 'q4 = filter q4 by ForecastCategory == "Omitted";\n'
            + "q4 = group q4 by WeekEndDate;\n"
            + "q4 = foreach q4 generate WeekEndDate, sum(TotalARR) as OmittedARR;\n"
            + "q = cogroup q1 by WeekEndDate full, q2 by WeekEndDate full, q3 by WeekEndDate full, q4 by WeekEndDate;\n"
            + "q = foreach q generate "
            + "coalesce(q1.WeekEndDate, q2.WeekEndDate, q3.WeekEndDate, q4.WeekEndDate) as WeekEndDate, "
            + "coalesce(sum(q1.CommitARR), 0) as CommitARR, "
            + "coalesce(sum(q2.BestCaseARR), 0) as BestCaseARR, "
            + "coalesce(sum(q3.PipelineARR), 0) as PipelineARR, "
            + "coalesce(sum(q4.OmittedARR), 0) as OmittedARR;\n"
            + "q = order q by WeekEndDate asc;"
        ),
        "s_wow_migration": sq(
            f'q = load "{WFO}";\n'
            + weekly_filters
            + "q = filter q by CurrentWeekFlag == 1;\n"
            + "q = filter q by ChangedThisWeekFlag == 1;\n"
            + "q = group q by (MovementPair, WeekChangeStory);\n"
            + "q = foreach q generate MovementPair, WeekChangeStory, "
            + "sum(ARR) as ARR, "
            + "count() as DealCount, "
            + "sum(PromotionThisWeekFlag) as PromotionCount, "
            + "sum(DemotionThisWeekFlag) as DemotionCount, "
            + "sum(PushedOutOfQuarterFlag) as PushOutCount;\n"
            + "q = order q by ARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_wow_big_bet_summary": sq(
            f'q = load "{WFO}";\n'
            + weekly_filters
            + "q = filter q by CurrentWeekFlag == 1;\n"
            + "q = filter q by BigBetFlag == 1;\n"
            + "q = filter q by ChangedThisWeekFlag == 1;\n"
            + "q = group q by (OpportunityName, ForecastCategory, WeekChangeStory, CloseQuarter, OpportunityId);\n"
            + "q = foreach q generate OpportunityName, "
            + "max(ARR) as ARR, "
            + '(case when max(PushedOutOfQuarterFlag) > 0 then "Pushed out of quarter" '
            + 'when max(DemotionThisWeekFlag) > 0 then "Moved down category" '
            + 'when max(PromotionThisWeekFlag) > 0 then "Promoted category" '
            + 'when max(PushThisWeekFlag) > 0 then "Close date pushed" '
            + 'when max(PullInThisWeekFlag) > 0 then "Pulled into quarter" '
            + 'when max(NewThisWeekFlag) > 0 then "Created this week" '
            + "else WeekChangeStory end) as ChangeSignal, "
            + '(case when max(PushedOutOfQuarterFlag) > 0 then "Protect quarter or reclassify" '
            + 'when max(DemotionThisWeekFlag) > 0 then "Rebuild evidence or reset" '
            + 'when max(PromotionThisWeekFlag) > 0 then "Validate promotion evidence" '
            + 'when max(PushThisWeekFlag) > 0 then "Confirm new close plan" '
            + 'when max(PullInThisWeekFlag) > 0 then "Validate pull-in plan" '
            + 'when max(NewThisWeekFlag) > 0 then "Qualify new big bet" '
            + 'else "Review week-over-week change" end) as LeadershipAsk, '
            + "CloseQuarter, OpportunityId, "
            + "((max(PushedOutOfQuarterFlag) * 1000000) + (max(DemotionThisWeekFlag) * 750000) + (max(PromotionThisWeekFlag) * 500000) + (max(PushThisWeekFlag) * 250000) + (max(PullInThisWeekFlag) * 150000) + (max(NewThisWeekFlag) * 100000) + (max(ARR) / 1000000)) as ChangePriorityScore;\n"
            + "q = order q by ChangePriorityScore desc, ARR desc;\n"
            + "q = limit q 5;"
        ),
        "s_wow_big_bet_table": sq(
            f'q = load "{WFO}";\n'
            + weekly_filters
            + "q = filter q by CurrentWeekFlag == 1;\n"
            + "q = filter q by BigBetFlag == 1;\n"
            + "q = filter q by ChangedThisWeekFlag == 1;\n"
            + "q = group q by (AccountName, OpportunityName, ForecastCategory, WeekChangeStory, CloseQuarter, OwnerName, OpportunityId, AccountId);\n"
            + "q = foreach q generate AccountName, OpportunityName, "
            + "max(ARR) as ARR, "
            + "ForecastCategory, "
            + '(case when max(PushedOutOfQuarterFlag) > 0 then "Pushed out of quarter" '
            + 'when max(DemotionThisWeekFlag) > 0 then "Moved down category" '
            + 'when max(PromotionThisWeekFlag) > 0 then "Promoted category" '
            + 'when max(PushThisWeekFlag) > 0 then "Close date pushed" '
            + 'when max(PullInThisWeekFlag) > 0 then "Pulled into quarter" '
            + 'when max(NewThisWeekFlag) > 0 then "Created this week" '
            + "else WeekChangeStory end) as ChangeSignal, "
            + '(case when max(PushedOutOfQuarterFlag) > 0 then "Protect quarter or reclassify" '
            + 'when max(DemotionThisWeekFlag) > 0 then "Rebuild evidence or reset" '
            + 'when max(PromotionThisWeekFlag) > 0 then "Validate promotion evidence" '
            + 'when max(PushThisWeekFlag) > 0 then "Confirm new close plan" '
            + 'when max(PullInThisWeekFlag) > 0 then "Validate pull-in plan" '
            + 'when max(NewThisWeekFlag) > 0 then "Qualify new big bet" '
            + 'else "Review week-over-week change" end) as LeadershipAsk, '
            + "CloseQuarter, OwnerName, OpportunityId, AccountId, "
            + "((max(PushedOutOfQuarterFlag) * 1000000) + (max(DemotionThisWeekFlag) * 750000) + (max(PromotionThisWeekFlag) * 500000) + (max(PushThisWeekFlag) * 250000) + (max(PullInThisWeekFlag) * 150000) + (max(NewThisWeekFlag) * 100000) + (max(ARR) / 1000000)) as ChangePriorityScore;\n"
            + "q = order q by ChangePriorityScore desc, ARR desc;\n"
            + "q = limit q 10;"
        ),
        "s_wow_push_table": sq(
            f'q = load "{WFO}";\n'
            + weekly_filters
            + "q = filter q by CurrentWeekFlag == 1;\n"
            + "q = filter q by PushThisWeekFlag == 1 or PushedOutOfQuarterFlag == 1;\n"
            + "q = filter q by ARR >= 500000;\n"
            + "q = group q by (AccountName, OpportunityName, ForecastCategory, WeekChangeStory, CloseQuarter, OwnerName, OpportunityId, AccountId);\n"
            + "q = foreach q generate AccountName, OpportunityName, "
            + "max(ARR) as ARR, "
            + "ForecastCategory, "
            + '(case when max(PushedOutOfQuarterFlag) > 0 then "Pushed out of quarter" '
            + 'when max(PushThisWeekFlag) > 0 then "Close date pushed" '
            + "else WeekChangeStory end) as ChangeSignal, "
            + '(case when max(PushedOutOfQuarterFlag) > 0 then "Backfill coverage or reclassify" '
            + 'when max(PushThisWeekFlag) > 0 then "Reset close plan this week" '
            + 'else "Review close-date change" end) as LeadershipAsk, '
            + "CloseQuarter, OwnerName, OpportunityId, AccountId, "
            + "((max(PushedOutOfQuarterFlag) * 1000000) + (max(PushThisWeekFlag) * 250000) + (max(ARR) / 1000000)) as ChangePriorityScore;\n"
            + "q = order q by ChangePriorityScore desc, ARR desc;\n"
            + "q = limit q 10;"
        ),
    }

    # Apply KPI facet scope so summary tiles respond to filter interactions
    for step_name in (
        "s_summary",
        "s_exception_summary",
        "s_wow_commit",
        "s_wow_best_case",
        "s_wow_pushes",
        "s_wow_big_bets",
    ):
        steps[step_name].update(KPI_FACET_SCOPE)

    return steps


def build_widgets() -> dict[str, dict]:
    """Build dashboard widgets."""
    widgets = {
        "p1_tab_sales": nav_link_external(
            SALES_MANAGER_DASHBOARD_ID, "Sales Manager", include_state=False
        ),
        "p1_tab_csm": nav_link_external(
            CSM_MANAGER_DASHBOARD_ID, "CSM Manager", include_state=False
        ),
        "p1_nav1": nav_link("summary", "Summary", active=True),
        "p1_nav2": nav_link("trend", "Trend & Forecast"),
        "p1_nav3": nav_link("drivers", "Drivers & Segments"),
        "p1_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p1_hdr": hdr(
            "Forecast & Revenue Motions",
            "Sales manager operating view for scenario conversion, weighted-model divergence, and promotion pressure.",
        ),
        "p1_f_region": pillbox("f_region", "Region"),
        "p1_f_quarter": pillbox("f_quarter", "Close Quarter"),
        "p1_f_manager": pillbox("f_manager", "Manager"),
        "p1_f_owner": pillbox("f_owner", "Owner"),
        "p1_sec_kpis": section_label("Forecast KPIs"),
        "p1_n_actual": num(
            "s_summary",
            "actual_closed",
            "Closed Won ARR",
            "#2E844A",
            compact=True,
            tier="primary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_projected": num(
            "s_summary",
            "commit_forecast",
            "Commit Forecast ARR",
            "#032D60",
            compact=True,
            tier="primary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_gap": num(
            "s_summary",
            "needed_promotion",
            "Needed Promotion ARR",
            "#BA0517",
            compact=True,
            tier="primary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_conf": num(
            "s_summary",
            "best_case_forecast",
            "Best Case Forecast ARR",
            "#0176D3",
            compact=True,
            tier="secondary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_sec_trajectory": section_label("Trajectory, Bridge & Weekly Change"),
        "p1_ch_timeline": line_chart(
            "s_monthly_trajectory",
            "Monthly Actual vs Weighted Model Trajectory",
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        "p1_tbl_weekly": compare_table(
            "s_wow_migration",
            "Weekly Forecast Movement Snapshot",
            columns=[
                "MovementPair",
                "WeekChangeStory",
                "ARR",
                "DealCount",
                "PushOutCount",
            ],
            row_limit=5,
            subtitle="Top category shifts, push-outs, and promotions under the current manager filters.",
            column_properties={
                "MovementPair": {"width": 120},
                "WeekChangeStory": {"width": 170},
                "ARR": {"width": 95, "alignment": "right"},
                "DealCount": {"width": 70, "alignment": "right"},
                "PushOutCount": {"width": 78, "alignment": "right"},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "PushOutCount",
                        "operator": "greaterThanOrEqual",
                        "value": 1,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
                {
                    "condition": {
                        "column": "ARR",
                        "operator": "greaterThanOrEqual",
                        "value": 1000000,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
            ],
        ),
        "p1_ch_bridge": waterfall_chart(
            "s_plan_bridge",
            "Closed -> Commit -> Best Case -> Needed Promotion",
            "BridgeStep",
            "BridgeARR",
            axis_label="ARR (EUR)",
        ),
        "p1_tbl_big_bets": compare_table(
            "s_wow_big_bet_summary",
            "Big Bets Requiring Attention",
            columns=[
                "OpportunityName",
                "ARR",
                "ChangeSignal",
                "LeadershipAsk",
                "CloseQuarter",
            ],
            row_limit=5,
            subtitle="Largest week-over-week deal changes with the management intervention each one now needs.",
            column_properties={
                "OpportunityName": {"width": 210},
                "ARR": {"width": 95, "alignment": "right"},
                "ChangeSignal": {"width": 145},
                "LeadershipAsk": {"width": 185},
                "CloseQuarter": {"width": 80},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "ARR",
                        "operator": "greaterThanOrEqual",
                        "value": 1000000,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
            ],
        ),
        "p1_sec_actions": section_label("Leadership Queue"),
        "p1_tbl_actions": compare_table(
            "s_manager_action_queue",
            "Leadership Priorities This Week",
            columns=[
                "OpportunityName",
                "AccountName",
                "ARR",
                "Escalation",
                "NextStep",
                "OwnerName",
                "CloseQuarter",
            ],
            row_limit=8,
            subtitle="Highest-leverage interventions for the next forecast call: owner clarity, approvals, stale deals, and category cleanup.",
            column_properties={
                "OpportunityName": {"width": 210},
                "AccountName": {"width": 165},
                "ARR": {"width": 110, "alignment": "right"},
                "Escalation": {"width": 180},
                "NextStep": {"width": 180},
                "OwnerName": {"width": 120},
                "CloseQuarter": {"width": 85},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "ARR",
                        "operator": "greaterThanOrEqual",
                        "value": 1000000,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
            ],
        ),
        "p2_tab_sales": nav_link_external(
            SALES_MANAGER_DASHBOARD_ID, "Sales Manager", include_state=False
        ),
        "p2_tab_csm": nav_link_external(
            CSM_MANAGER_DASHBOARD_ID, "CSM Manager", include_state=False
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("trend", "Trend & Forecast", active=True),
        "p2_nav3": nav_link("drivers", "Drivers & Segments"),
        "p2_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p2_hdr": hdr(
            "Trend & Forecast",
            "Sales manager view of pacing, raw category pressure, approval bottlenecks, and rep coaching that needs intervention before forecast calls.",
        ),
        "p2_f_quarter": pillbox("f_quarter", "Close Quarter"),
        "p2_f_manager": pillbox("f_manager", "Manager"),
        "p2_f_owner": pillbox("f_owner", "Owner"),
        "p2_sec_compliance": section_label("Process Compliance & Forecast Quality"),
        "p2_tbl_process": compare_table(
            "s_process_compliance",
            "Rep Hygiene, Ownership & Approval Risk",
            columns=[
                "ManagerName",
                "OwnerName",
                "OwnerPersona",
                "OpenARR",
                "OwnershipReviewARR",
                "PendingApprovalARR",
                "ReviewCandidateCount",
                "NoNextStepCount",
                "StaleApprovalCount",
                "OverdueTaskCount",
                "DealReviewCount",
            ],
            row_limit=12,
            subtitle="Use this table to spot reps blocked by approval queues, ownership gaps, and missing next steps before forecast reviews.",
            column_properties={
                "ManagerName": {"width": 120},
                "OwnerName": {"width": 125},
                "OwnerPersona": {"width": 90},
                "OpenARR": {"width": 110, "alignment": "right"},
                "OwnershipReviewARR": {"width": 120, "alignment": "right"},
                "PendingApprovalARR": {"width": 120, "alignment": "right"},
                "ReviewCandidateCount": {"width": 80, "alignment": "right"},
                "NoNextStepCount": {"width": 80, "alignment": "right"},
                "StaleApprovalCount": {"width": 80, "alignment": "right"},
                "OverdueTaskCount": {"width": 80, "alignment": "right"},
                "DealReviewCount": {"width": 80, "alignment": "right"},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "OwnershipReviewARR",
                        "operator": "greaterThan",
                        "value": 0,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
                {
                    "condition": {
                        "column": "NoNextStepCount",
                        "operator": "greaterThan",
                        "value": 0,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
                {
                    "condition": {
                        "column": "StaleApprovalCount",
                        "operator": "greaterThan",
                        "value": 0,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
            ],
        ),
        "p2_ch_forecast_quality": line_chart(
            "s_forecast_quality",
            "Quarterly Open ARR by Forecast Category",
            show_legend=True,
            axis_title="ARR (EUR)",
            subtitle="Raw category pressure by quarter, including omitted dollars and risky commit exposure.",
        ),
        "p2_sec_aging": section_label("Stage Aging & Confidence"),
        "p2_tbl_stage_age": compare_table(
            "s_stage_aging_pressure",
            "Rep Stage Aging & Follow-Up",
            columns=[
                "ManagerName",
                "OwnerName",
                "OpenARR",
                "AvgAgeInDays",
                "OpenOppCount",
                "NoNextStepCount",
                "CommitARR",
                "OmittedARR",
            ],
            row_limit=12,
            subtitle="Use this to find reps carrying old late-stage deals, missing follow-up, or too much omitted pressure.",
            column_properties={
                "ManagerName": {"width": 120},
                "OwnerName": {"width": 125},
                "OpenARR": {"width": 110, "alignment": "right"},
                "AvgAgeInDays": {"width": 90, "alignment": "right"},
                "OpenOppCount": {"width": 80, "alignment": "right"},
                "NoNextStepCount": {"width": 80, "alignment": "right"},
                "CommitARR": {"width": 105, "alignment": "right"},
                "OmittedARR": {"width": 105, "alignment": "right"},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "AvgAgeInDays",
                        "operator": "greaterThanOrEqual",
                        "value": 90,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
            ],
        ),
        "p2_tbl_owner_conf": compare_table(
            "s_owner_confidence",
            "Rep Forecast Confidence & Coverage",
            columns=[
                "ManagerName",
                "OwnerName",
                "ProjectedARR",
                "ConfidencePct",
                "AttainmentPct",
                "CommitRiskARR",
                "OpenOppCount",
                "NoNextStepCount",
            ],
            row_limit=12,
            subtitle="Compare rep forecast quality, risk concentration, and missing follow-up before commit reviews.",
            column_properties={
                "ManagerName": {"width": 120},
                "OwnerName": {"width": 125},
                "ProjectedARR": {"width": 110, "alignment": "right"},
                "ConfidencePct": {"width": 85, "alignment": "right"},
                "AttainmentPct": {"width": 85, "alignment": "right"},
                "CommitRiskARR": {"width": 110, "alignment": "right"},
                "OpenOppCount": {"width": 80, "alignment": "right"},
                "NoNextStepCount": {"width": 80, "alignment": "right"},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "ConfidencePct",
                        "operator": "lessThan",
                        "value": 35,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
                {
                    "condition": {
                        "column": "CommitRiskARR",
                        "operator": "greaterThan",
                        "value": 500000,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
                {
                    "condition": {
                        "column": "NoNextStepCount",
                        "operator": "greaterThan",
                        "value": 0,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
            ],
        ),
        "p2_sec_conversion": section_label("Win Rate & Stage Conversion"),
        "p2_n_win_rate": num(
            "s_closed_win_rate",
            "WinRatePct",
            "Closed Win Rate",
            "#2E844A",
            compact=True,
            tier="secondary",
            suffix="%",
            widget_style=KPI_CARD_STYLE,
        ),
        "p2_n_adv_stage_conv": num(
            "s_advanced_stage_conversion",
            "AdvancedStageConversionPct",
            "Advanced Stage Conversion",
            "#0176D3",
            compact=True,
            tier="secondary",
            suffix="%",
            widget_style=KPI_CARD_STYLE,
        ),
        "p2_tbl_stage_conv": compare_table(
            "s_stage_conversion_table",
            "Stage Conversion by Progression",
            columns=[
                "StageProgression",
                "OppCount",
                "WonOppCount",
                "ConversionPct",
                "OpenARR",
                "ClosedWonARR",
            ],
            row_limit=8,
            subtitle="RW KPI cut: stage-level conversion signal plus open coverage still sitting in each stage.",
            column_properties={
                "StageProgression": {"width": 130},
                "OppCount": {"width": 70, "alignment": "right"},
                "WonOppCount": {"width": 78, "alignment": "right"},
                "ConversionPct": {"width": 85, "alignment": "right"},
                "OpenARR": {"width": 95, "alignment": "right"},
                "ClosedWonARR": {"width": 105, "alignment": "right"},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "ConversionPct",
                        "operator": "lessThan",
                        "value": 20,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
                {
                    "condition": {
                        "column": "OpenARR",
                        "operator": "greaterThanOrEqual",
                        "value": 1000000,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
            ],
        ),
        "p2_sec_pressure": section_label("Issue Pressure Trend"),
        "p2_ch_issue_pressure": line_chart(
            "s_process_issue_trend",
            "Quarterly Process Pressure by Issue",
            show_legend=True,
            axis_title="Issue Count",
            subtitle="Review candidates, missing next steps, pending approvals, overdue tasks, and ownership-review issues by quarter.",
        ),
        "p3_tab_sales": nav_link_external(
            SALES_MANAGER_DASHBOARD_ID, "Sales Manager", include_state=False
        ),
        "p3_tab_csm": nav_link_external(
            CSM_MANAGER_DASHBOARD_ID, "CSM Manager", include_state=False
        ),
        "p3_nav1": nav_link("summary", "Summary"),
        "p3_nav2": nav_link("trend", "Trend & Forecast"),
        "p3_nav3": nav_link("drivers", "Drivers & Segments", active=True),
        "p3_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p3_hdr": hdr(
            "Drivers & Segments",
            "Sales manager view of which stages, products, regions, and motions are driving coaching pressure, category drift, and forecast cleanup.",
        ),
        "p3_f_region": pillbox("f_region", "Region"),
        "p3_f_quarter": pillbox("f_quarter", "Close Quarter"),
        "p3_f_manager": pillbox("f_manager", "Manager"),
        "p3_f_owner": pillbox("f_owner", "Owner"),
        "p3_sec_product": section_label("Product & Region Pressure"),
        "p3_tbl_product_pressure": compare_table(
            "s_product_pressure",
            "Product Family Pressure & Cleanup",
            columns=[
                "ProductFamily",
                "ProjectedARR",
                "CommitRiskARR",
                "OmittedARR",
                "ReviewCandidateCount",
                "NoNextStepCount",
                "OwnershipReviewARR",
            ],
            row_limit=12,
            subtitle="Shows where specific product families are driving review pressure, omitted cleanup, or ownership mismatches.",
            column_properties={
                "ProductFamily": {"width": 180},
                "ProjectedARR": {"width": 110, "alignment": "right"},
                "CommitRiskARR": {"width": 110, "alignment": "right"},
                "OmittedARR": {"width": 105, "alignment": "right"},
                "ReviewCandidateCount": {"width": 80, "alignment": "right"},
                "NoNextStepCount": {"width": 80, "alignment": "right"},
                "OwnershipReviewARR": {"width": 120, "alignment": "right"},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "CommitRiskARR",
                        "operator": "greaterThan",
                        "value": 500000,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
                {
                    "condition": {
                        "column": "OwnershipReviewARR",
                        "operator": "greaterThan",
                        "value": 0,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
            ],
        ),
        "p3_ch_heatmap": heatmap_chart(
            "s_product_heatmap",
            "Product Family x Quarter Projected ARR",
            show_legend=True,
        ),
        "p3_sec_region": section_label("Stage & Regional Detail"),
        "p3_tbl_region": compare_table(
            "s_region_mix",
            "Regional Pressure & Cleanup",
            columns=[
                "SalesRegion",
                "ProjectedARR",
                "RiskyCommitARR",
                "OmittedARR",
                "ReviewCandidateCount",
                "NoNextStepCount",
                "OwnershipReviewARR",
            ],
            row_limit=10,
            subtitle="Use this to compare where risk, omitted dollars, and ownership-review pressure are concentrated by region.",
            column_properties={
                "SalesRegion": {"width": 150},
                "ProjectedARR": {"width": 110, "alignment": "right"},
                "RiskyCommitARR": {"width": 110, "alignment": "right"},
                "OmittedARR": {"width": 105, "alignment": "right"},
                "ReviewCandidateCount": {"width": 80, "alignment": "right"},
                "NoNextStepCount": {"width": 80, "alignment": "right"},
                "OwnershipReviewARR": {"width": 120, "alignment": "right"},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "RiskyCommitARR",
                        "operator": "greaterThan",
                        "value": 1000000,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
                {
                    "condition": {
                        "column": "OmittedARR",
                        "operator": "greaterThan",
                        "value": 500000,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
                {
                    "condition": {
                        "column": "OwnershipReviewARR",
                        "operator": "greaterThan",
                        "value": 0,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
            ],
        ),
        "p3_ch_stage_heatmap": heatmap_chart(
            "s_stage_forecast_heatmap",
            "Stage x Forecast Category Pressure",
            show_legend=True,
        ),
        "p3_sec_motion": section_label("Motion Coaching Trend"),
        "p3_ch_motion_trend": line_chart(
            "s_motion_issue_trend",
            "Quarterly Coaching Pressure by Motion",
            show_legend=True,
            axis_title="Issue Count",
            subtitle="Review candidates by motion so managers can see whether land, expand, or renewal coaching is driving the quarter.",
        ),
        "p4_tab_sales": nav_link_external(
            SALES_MANAGER_DASHBOARD_ID, "Sales Manager", include_state=False
        ),
        "p4_tab_csm": nav_link_external(
            CSM_MANAGER_DASHBOARD_ID, "CSM Manager", include_state=False
        ),
        "p4_link_account360": nav_link_external(
            ACCOUNT_360_DASHBOARD_ID,
            "Open Account 360 & History",
            include_state=False,
            font_size=13,
        ),
        "p4_nav1": nav_link("summary", "Summary"),
        "p4_nav2": nav_link("trend", "Trend & Forecast"),
        "p4_nav3": nav_link("drivers", "Drivers & Segments"),
        "p4_nav4": nav_link("exceptions", "Exceptions & Actions", active=True),
        "p4_hdr": hdr(
            "Exceptions & Actions",
            "Sales manager view of deal review, approval bottlenecks, commit protection, promotion pressure, and omitted cleanup that needs action now.",
        ),
        "p4_f_quarter": pillbox("f_quarter", "Close Quarter"),
        "p4_f_manager": pillbox("f_manager", "Manager"),
        "p4_f_owner": pillbox("f_owner", "Owner"),
        "p4_sec_summary": section_label("Exception Summary"),
        "p4_n_review_count": num(
            "s_exception_summary",
            "review_candidate_count",
            "Review / Approval Candidates",
            "#8E030F",
            compact=True,
            tier="secondary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p4_n_commit_arr": num(
            "s_exception_summary",
            "risky_commit_arr",
            "Commit / Best Case At Risk ARR",
            "#BA0517",
            compact=True,
            tier="secondary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p4_n_omitted_arr": num(
            "s_exception_summary",
            "omitted_arr",
            "Omitted ARR",
            "#5C5C5C",
            compact=True,
            tier="secondary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p4_tbl_owner": compare_table(
            "s_owner_gap",
            "Rep Promotion & Hygiene Pressure",
            columns=[
                "ManagerName",
                "OwnerName",
                "WeightedModelARR",
                "BestCaseForecastARR",
                "NeededPromotionARR",
                "ReviewCandidateCount",
                "NoNextStepCount",
                "OwnershipReviewARR",
            ],
            row_limit=12,
            subtitle="Shows which reps need forecast promotion, better hygiene, or ownership cleanup before the next review.",
            column_properties={
                "ManagerName": {"width": 120},
                "OwnerName": {"width": 125},
                "WeightedModelARR": {"width": 110, "alignment": "right"},
                "BestCaseForecastARR": {"width": 110, "alignment": "right"},
                "NeededPromotionARR": {"width": 120, "alignment": "right"},
                "ReviewCandidateCount": {"width": 80, "alignment": "right"},
                "NoNextStepCount": {"width": 80, "alignment": "right"},
                "OwnershipReviewARR": {"width": 120, "alignment": "right"},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "NeededPromotionARR",
                        "operator": "greaterThan",
                        "value": 0,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#BA0517",
                },
                {
                    "condition": {
                        "column": "NeededPromotionARR",
                        "operator": "lessThanOrEqual",
                        "value": 0,
                    },
                    "backgroundColor": "#EFF6EE",
                    "fontColor": "#2E844A",
                },
                {
                    "condition": {
                        "column": "OwnershipReviewARR",
                        "operator": "greaterThan",
                        "value": 0,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
            ],
        ),
        "p4_sec_queues": section_label("Deal Queues"),
        "p4_tbl_review": compare_table(
            "s_deal_review_candidates",
            "Review & Approval Queue",
            columns=[
                "AccountName",
                "OpportunityName",
                "ARR",
                "DealPulse",
                "ApprovalAgeDays",
                "Escalation",
                "OwnerName",
                "CloseQuarter",
                "NextStep",
            ],
            row_limit=8,
            subtitle="Use this queue for deals blocked by review readiness, commercial approval, or missing execution detail.",
            column_properties={
                "AccountName": {"width": 170},
                "OpportunityName": {"width": 220},
                "ARR": {"width": 110, "alignment": "right"},
                "DealPulse": {"width": 150},
                "ApprovalAgeDays": {"width": 85, "alignment": "right"},
                "Escalation": {"width": 180},
                "OwnerName": {"width": 120},
                "CloseQuarter": {"width": 85},
                "NextStep": {"width": 180},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "ARR",
                        "operator": "greaterThanOrEqual",
                        "value": 1000000,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
                {
                    "condition": {
                        "column": "ApprovalAgeDays",
                        "operator": "greaterThanOrEqual",
                        "value": 14,
                    },
                    "backgroundColor": "#FFF7E6",
                    "fontColor": "#8B5D00",
                },
            ],
        ),
        "p4_tbl_commit": compare_table(
            "s_top_commit_protection",
            "Commit Protection Queue",
            columns=[
                "AccountName",
                "OpportunityName",
                "ARR",
                "DealPulse",
                "ApprovalAgeDays",
                "Escalation",
                "OwnerName",
                "CloseQuarter",
                "NextStep",
            ],
            row_limit=8,
            subtitle="Highest-value commit deals that need manager intervention to protect the quarter.",
            column_properties={
                "AccountName": {"width": 170},
                "OpportunityName": {"width": 220},
                "ARR": {"width": 110, "alignment": "right"},
                "DealPulse": {"width": 150},
                "ApprovalAgeDays": {"width": 85, "alignment": "right"},
                "Escalation": {"width": 180},
                "OwnerName": {"width": 120},
                "CloseQuarter": {"width": 85},
                "NextStep": {"width": 180},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "ARR",
                        "operator": "greaterThanOrEqual",
                        "value": 1000000,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
            ],
        ),
        "p4_sec_promotion": section_label("Promotion & Cleanup"),
        "p4_tbl_forecast": compare_table(
            "s_top_forecast_risk",
            "Promotion Candidates",
            columns=[
                "AccountName",
                "OpportunityName",
                "ARR",
                "DealPulse",
                "ApprovalAgeDays",
                "Escalation",
                "OwnerName",
                "CloseQuarter",
                "NextStep",
            ],
            row_limit=8,
            subtitle="Best-case and pipeline deals that need evidence, cleanup, or a promotion decision before the next call.",
            column_properties={
                "AccountName": {"width": 170},
                "OpportunityName": {"width": 220},
                "ARR": {"width": 110, "alignment": "right"},
                "DealPulse": {"width": 150},
                "ApprovalAgeDays": {"width": 85, "alignment": "right"},
                "Escalation": {"width": 180},
                "OwnerName": {"width": 120},
                "CloseQuarter": {"width": 85},
                "NextStep": {"width": 180},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "ARR",
                        "operator": "greaterThanOrEqual",
                        "value": 1000000,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
            ],
        ),
        "p4_tbl_omitted": compare_table(
            "s_top_omitted",
            "Omitted Cleanup Queue",
            columns=[
                "AccountName",
                "OpportunityName",
                "ARR",
                "AgeInDays",
                "DealPulse",
                "Escalation",
                "OwnerName",
                "CloseQuarter",
                "NextStep",
            ],
            row_limit=8,
            subtitle="Deals that still need category classification or closure before they distort the manager forecast view.",
            column_properties={
                "AccountName": {"width": 170},
                "OpportunityName": {"width": 220},
                "ARR": {"width": 110, "alignment": "right"},
                "AgeInDays": {"width": 85, "alignment": "right"},
                "DealPulse": {"width": 150},
                "Escalation": {"width": 180},
                "OwnerName": {"width": 120},
                "CloseQuarter": {"width": 85},
                "NextStep": {"width": 180},
            },
            format_rules=[
                {
                    "condition": {
                        "column": "AgeInDays",
                        "operator": "greaterThanOrEqual",
                        "value": 120,
                    },
                    "backgroundColor": "#FEF0EF",
                    "fontColor": "#8E030F",
                },
            ],
        ),
        "p5_tab_sales": nav_link_external(
            SALES_MANAGER_DASHBOARD_ID, "Sales Manager", include_state=False
        ),
        "p5_tab_csm": nav_link_external(
            CSM_MANAGER_DASHBOARD_ID, "CSM Manager", include_state=False
        ),
        "p5_link_account360": nav_link_external(
            ACCOUNT_360_DASHBOARD_ID,
            "Open Account 360 & History",
            include_state=False,
            font_size=13,
        ),
        "p5_hdr": hdr(
            "Week over Week",
            "Sales manager control room for forecast movement, pushed deals, and big bets that changed this week.",
        ),
        "p5_f_region": pillbox("f_region", "Region"),
        "p5_f_quarter": pillbox("f_quarter", "Close Quarter"),
        "p5_f_manager": pillbox("f_manager", "Manager"),
        "p5_f_owner": pillbox("f_owner", "Owner"),
        "p5_sec_kpis": section_label("Forecast & Coverage Movement"),
        "p5_n_commit": num(
            "s_wow_commit",
            "CommitWoWChange",
            "Commit WoW Change",
            "#032D60",
            compact=True,
            tier="primary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p5_n_best_case": num(
            "s_wow_best_case",
            "BestCaseWoWChange",
            "Best Case WoW Change",
            "#0176D3",
            compact=True,
            tier="primary",
            prefix="\u20ac",
            widget_style=KPI_CARD_STYLE,
        ),
        "p5_n_pushes": num(
            "s_wow_pushes",
            "PushedDeals",
            "Deals Pushed This Week",
            "#BA0517",
            compact=True,
            tier="secondary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p5_n_big_bets": num(
            "s_wow_big_bets",
            "BigBetCount",
            "Big Bets Changed",
            "#8E030F",
            compact=True,
            tier="secondary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p5_ch_timeline": line_chart(
            "s_wow_timeline",
            "Weekly Forecast & Pipeline Mix",
            show_legend=True,
            axis_title="Open ARR (EUR)",
            subtitle="Track forecast and pipeline coverage mix week over week under the current manager filters.",
        ),
        "p5_sec_movement": section_label("Forecast Movement & Big Bets"),
        "p5_tbl_migration": compare_table(
            "s_wow_migration",
            "Forecast Category Movement This Week",
            columns=[
                "MovementPair",
                "WeekChangeStory",
                "ARR",
                "DealCount",
                "PromotionCount",
                "PushOutCount",
            ],
            row_limit=12,
            subtitle="Use this to see what moved categories, what was pushed, and where the week changed shape.",
            column_properties={
                "MovementPair": {"width": 160},
                "WeekChangeStory": {"width": 170},
                "ARR": {"width": 110, "alignment": "right"},
                "DealCount": {"width": 80, "alignment": "right"},
                "PromotionCount": {"width": 80, "alignment": "right"},
                "PushOutCount": {"width": 80, "alignment": "right"},
            },
        ),
        "p5_tbl_big_bets": compare_table(
            "s_wow_big_bet_table",
            "Big Bets Requiring Attention",
            columns=[
                "AccountName",
                "OpportunityName",
                "ARR",
                "ChangeSignal",
                "LeadershipAsk",
                "CloseQuarter",
                "OwnerName",
            ],
            row_limit=10,
            subtitle="Million-plus deals whose week-over-week change now needs a specific leadership intervention.",
            column_properties={
                "AccountName": {"width": 170},
                "OpportunityName": {"width": 220},
                "ARR": {"width": 110, "alignment": "right"},
                "ChangeSignal": {"width": 145},
                "LeadershipAsk": {"width": 180},
                "CloseQuarter": {"width": 85},
                "OwnerName": {"width": 120},
            },
        ),
        "p5_sec_pushes": section_label("Pipeline Coverage Risk"),
        "p5_tbl_pushes": compare_table(
            "s_wow_push_table",
            "Pushed Deals Hitting Coverage",
            columns=[
                "AccountName",
                "OpportunityName",
                "ARR",
                "ChangeSignal",
                "LeadershipAsk",
                "CloseQuarter",
                "OwnerName",
            ],
            row_limit=10,
            subtitle="Highest-value push-outs and late slips that now threaten quarter coverage.",
            column_properties={
                "AccountName": {"width": 170},
                "OpportunityName": {"width": 220},
                "ARR": {"width": 110, "alignment": "right"},
                "ChangeSignal": {"width": 145},
                "LeadershipAsk": {"width": 180},
                "CloseQuarter": {"width": 85},
                "OwnerName": {"width": 120},
            },
        ),
    }

    add_table_action(
        widgets["p1_tbl_actions"], "salesforceActions", "Opportunity", "Id"
    )
    add_table_action(
        widgets["p1_tbl_big_bets"], "salesforceActions", "Opportunity", "OpportunityId"
    )
    add_table_action(widgets["p4_tbl_review"], "salesforceActions", "Opportunity", "Id")
    add_table_action(widgets["p4_tbl_commit"], "salesforceActions", "Opportunity", "Id")
    add_table_action(
        widgets["p4_tbl_forecast"], "salesforceActions", "Opportunity", "Id"
    )
    add_table_action(
        widgets["p4_tbl_omitted"], "salesforceActions", "Opportunity", "Id"
    )
    add_table_action(
        widgets["p5_tbl_big_bets"], "salesforceActions", "Opportunity", "OpportunityId"
    )
    add_table_action(
        widgets["p5_tbl_pushes"], "salesforceActions", "Opportunity", "OpportunityId"
    )
    return {name: widget for name, widget in widgets.items() if "_nav" not in name}


def build_layout() -> dict:
    """Build the manager dashboard layout."""
    p1 = [
        {"name": "p1_tab_sales", "row": 1, "column": 0, "colspan": 6, "rowspan": 1},
        {"name": "p1_tab_csm", "row": 1, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p1_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 0, "colspan": 4, "rowspan": 1},
        {"name": "p1_f_quarter", "row": 3, "column": 4, "colspan": 4, "rowspan": 1},
        {"name": "p1_f_manager", "row": 3, "column": 8, "colspan": 4, "rowspan": 1},
        {"name": "p1_f_owner", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_sec_kpis", "row": 6, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_n_actual", "row": 7, "column": 0, "colspan": 3, "rowspan": 3},
        {"name": "p1_n_projected", "row": 7, "column": 3, "colspan": 3, "rowspan": 3},
        {"name": "p1_n_gap", "row": 7, "column": 6, "colspan": 3, "rowspan": 3},
        {"name": "p1_n_conf", "row": 7, "column": 9, "colspan": 3, "rowspan": 3},
        {
            "name": "p1_sec_trajectory",
            "row": 10,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p1_ch_timeline", "row": 11, "column": 0, "colspan": 12, "rowspan": 5},
        {"name": "p1_tbl_weekly", "row": 16, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_ch_bridge", "row": 16, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_tbl_big_bets", "row": 16, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_sec_actions", "row": 20, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_tbl_actions", "row": 21, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    p2 = [
        {"name": "p2_tab_sales", "row": 1, "column": 0, "colspan": 6, "rowspan": 1},
        {"name": "p2_tab_csm", "row": 1, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p2_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_quarter", "row": 4, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p2_f_manager", "row": 4, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p2_f_owner", "row": 6, "column": 0, "colspan": 12, "rowspan": 2},
        {
            "name": "p2_sec_compliance",
            "row": 8,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p2_tbl_process",
            "row": 9,
            "column": 0,
            "colspan": 6,
            "rowspan": 7,
        },
        {
            "name": "p2_ch_forecast_quality",
            "row": 9,
            "column": 6,
            "colspan": 6,
            "rowspan": 7,
        },
        {"name": "p2_sec_aging", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p2_tbl_stage_age",
            "row": 17,
            "column": 0,
            "colspan": 6,
            "rowspan": 7,
        },
        {
            "name": "p2_tbl_owner_conf",
            "row": 17,
            "column": 6,
            "colspan": 6,
            "rowspan": 7,
        },
        {
            "name": "p2_sec_conversion",
            "row": 24,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p2_n_win_rate", "row": 25, "column": 0, "colspan": 3, "rowspan": 3},
        {
            "name": "p2_n_adv_stage_conv",
            "row": 25,
            "column": 3,
            "colspan": 3,
            "rowspan": 3,
        },
        {
            "name": "p2_tbl_stage_conv",
            "row": 25,
            "column": 6,
            "colspan": 6,
            "rowspan": 6,
        },
        {
            "name": "p2_sec_pressure",
            "row": 31,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p2_ch_issue_pressure",
            "row": 32,
            "column": 0,
            "colspan": 12,
            "rowspan": 6,
        },
    ]

    p3 = [
        {"name": "p3_tab_sales", "row": 1, "column": 0, "colspan": 6, "rowspan": 1},
        {"name": "p3_tab_csm", "row": 1, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p3_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_region", "row": 4, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_quarter", "row": 4, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_manager", "row": 4, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_owner", "row": 6, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_sec_product", "row": 8, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p3_tbl_product_pressure",
            "row": 9,
            "column": 0,
            "colspan": 6,
            "rowspan": 7,
        },
        {"name": "p3_ch_heatmap", "row": 9, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_sec_region", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_tbl_region", "row": 17, "column": 0, "colspan": 6, "rowspan": 7},
        {
            "name": "p3_ch_stage_heatmap",
            "row": 17,
            "column": 6,
            "colspan": 6,
            "rowspan": 7,
        },
        {"name": "p3_sec_motion", "row": 24, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p3_ch_motion_trend",
            "row": 25,
            "column": 0,
            "colspan": 12,
            "rowspan": 7,
        },
    ]

    p4 = [
        {"name": "p4_tab_sales", "row": 1, "column": 0, "colspan": 4, "rowspan": 1},
        {"name": "p4_tab_csm", "row": 1, "column": 4, "colspan": 4, "rowspan": 1},
        {
            "name": "p4_link_account360",
            "row": 1,
            "column": 8,
            "colspan": 4,
            "rowspan": 1,
        },
        {"name": "p4_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_quarter", "row": 4, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p4_f_manager", "row": 4, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p4_f_owner", "row": 6, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_sec_summary", "row": 8, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p4_n_review_count",
            "row": 9,
            "column": 0,
            "colspan": 2,
            "rowspan": 4,
        },
        {"name": "p4_n_commit_arr", "row": 9, "column": 2, "colspan": 2, "rowspan": 4},
        {"name": "p4_n_omitted_arr", "row": 9, "column": 4, "colspan": 2, "rowspan": 4},
        {"name": "p4_tbl_owner", "row": 9, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p4_sec_queues", "row": 15, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p4_tbl_review",
            "row": 16,
            "column": 0,
            "colspan": 6,
            "rowspan": 7,
        },
        {
            "name": "p4_tbl_commit",
            "row": 16,
            "column": 6,
            "colspan": 6,
            "rowspan": 7,
        },
        {
            "name": "p4_sec_promotion",
            "row": 23,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p4_tbl_forecast",
            "row": 24,
            "column": 0,
            "colspan": 6,
            "rowspan": 7,
        },
        {"name": "p4_tbl_omitted", "row": 24, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p5 = [
        {"name": "p5_tab_sales", "row": 1, "column": 0, "colspan": 4, "rowspan": 1},
        {"name": "p5_tab_csm", "row": 1, "column": 4, "colspan": 4, "rowspan": 1},
        {
            "name": "p5_link_account360",
            "row": 1,
            "column": 8,
            "colspan": 4,
            "rowspan": 1,
        },
        {"name": "p5_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p5_f_region", "row": 4, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p5_f_quarter", "row": 4, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p5_f_manager", "row": 6, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p5_f_owner", "row": 6, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p5_sec_kpis", "row": 8, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_n_commit", "row": 9, "column": 0, "colspan": 3, "rowspan": 3},
        {"name": "p5_n_best_case", "row": 9, "column": 3, "colspan": 3, "rowspan": 3},
        {"name": "p5_n_pushes", "row": 9, "column": 6, "colspan": 3, "rowspan": 3},
        {"name": "p5_n_big_bets", "row": 9, "column": 9, "colspan": 3, "rowspan": 3},
        {"name": "p5_ch_timeline", "row": 12, "column": 0, "colspan": 12, "rowspan": 6},
        {
            "name": "p5_sec_movement",
            "row": 18,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p5_tbl_migration",
            "row": 19,
            "column": 0,
            "colspan": 6,
            "rowspan": 6,
        },
        {"name": "p5_tbl_big_bets", "row": 19, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p5_sec_pushes", "row": 25, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_tbl_pushes", "row": 26, "column": 0, "colspan": 12, "rowspan": 6},
    ]

    return {
        "name": "ForecastRevenueMotions",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("trend", "Trend & Forecast", p2),
            pg("drivers", "Drivers & Segments", p3),
            pg("exceptions", "Exceptions & Actions", p4),
            pg("wow", "Week over Week", p5),
        ],
    }


def main() -> None:
    """Build dataset and deploy dashboard."""
    with builder_run("Forecast_Revenue_Motions", __file__) as summary:
        instance_url, token = get_auth()
        assert_org_schema(instance_url, token, objects=["Opportunity", "User"])
        upload_ok, row_count = create_dataset(instance_url, token)
        summary.row_count = row_count
        if not upload_ok:
            raise SystemExit("Dataset upload failed")

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
