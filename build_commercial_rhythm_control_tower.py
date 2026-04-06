#!/usr/bin/env python3
"""Build the Commercial Rhythm Control Tower dashboard.

Cross-suite operating surface for:
- motion ownership alignment
- sales to CX handoff quality
- forecast/process hygiene
- renewal semantic confidence
"""

from __future__ import annotations

import csv
import io
import logging
from collections import defaultdict
from datetime import UTC, datetime

from commercial_operating_model import (
    ownership_alignment,
    primary_motion_persona,
    role_dimension_row,
)
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
    nav_link_external,
    num,
    pg,
    pillbox,
    set_record_links_xmd,
    sq,
    upload_dataset,
)
from crm_analytics_runtime import builder_run  # pyright: ignore[reportMissingImports]
from portfolio_foundation import current_fy_label, fiscal_label, safe_float
from simcorp_fields import assert_org_schema  # pyright: ignore[reportMissingImports]

logger = logging.getLogger(__name__)

DS = "Commercial_Rhythm_Control_Tower"
DS_LABEL = "Commercial Rhythm Control Tower"
DASHBOARD_LABEL = "Commercial Rhythm Control Tower"
CURRENT_FY_LABEL = current_fy_label()
SALES_MANAGER_DASHBOARD_ID = "0FKTb0000000JCLOA2"
CSM_MANAGER_DASHBOARD_ID = "0FKTb0000000J97OAE"
ACCOUNT_360_DASHBOARD_ID = "0FKTb0000000JNdOAM"

OPP_SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, "
    "Account.OwnerId, Account.Owner.Name, Account.Owner.Title, Account.Owner.Department, "
    "Account.Owner.Division, Account.Owner.UserRole.Name, "
    "Account.Owner.ManagerId, Account.Owner.Manager.Name, "
    "OwnerId, Owner.Name, Owner.Title, Owner.Department, Owner.Division, "
    "Owner.UserRole.Name, Owner.ManagerId, Owner.Manager.Name, "
    "Type, StageName, ForecastCategoryName, IsClosed, IsWon, CloseDate, CreatedDate, "
    "FiscalYear, FiscalQuarter, NextStep, AgeInDays, Sales_Region__c, Account_Unit_Group__c, "
    "APTS_Forecast_ARR__c, convertCurrency(APTS_Forecast_ARR__c) ConvertedForecastARR, "
    "APTS_Renewal_ACV__c "
    "FROM Opportunity "
    "WHERE FiscalYear IN (2024, 2025, 2026, 2027)"
)


def _stage_progression_label(stage_name: str) -> str:
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
    if normalized in {"won", "closedwon", "closed"}:
        return "8 Won"
    if normalized in {"optout", "opt_out"}:
        return "7 Opt-out"
    return f"9 Other: {(stage_name or '').strip()[:40]}"


def _handoff_state(motion: str, opp_persona: str, acct_persona: str) -> str:
    if motion == "Land":
        if opp_persona == "Sales":
            return "Sales-led"
        if opp_persona == "Marketing":
            return "Marketing-assisted"
        return "Needs Sales owner"
    if motion in {"Expand", "Renewal", "Contraction", "Churn"}:
        if opp_persona == "CX" and acct_persona == "CX":
            return "CX-owned"
        if opp_persona == "CX":
            return "Sales to CX handoff"
        if opp_persona == "Services":
            return "Services-supported"
        if opp_persona == "Sales":
            return "Needs CX handoff"
        return "Needs CX owner"
    return "Unclassified"


def _review_pulse(
    motion: str,
    handoff_state: str,
    forecast_category: str,
    no_next_step: int,
    zero_value_renewal: int,
    renewal_metric_missing: int,
) -> str:
    if handoff_state.startswith("Needs"):
        return f"{motion} | Ownership review"
    if zero_value_renewal:
        return "Renewal | Zero value"
    if renewal_metric_missing:
        return "Renewal | Missing ACV"
    if forecast_category == "Omitted":
        return f"{motion} | Omitted"
    if no_next_step:
        return f"{motion} | No next step"
    return f"{motion} | Monitor"


def _leadership_ask(
    handoff_state: str,
    forecast_category: str,
    no_next_step: int,
    zero_value_renewal: int,
    renewal_metric_missing: int,
) -> str:
    if handoff_state.startswith("Needs"):
        return "Confirm owner and commercial handoff path"
    if zero_value_renewal or renewal_metric_missing:
        return "Fix renewal value semantics before forecasting"
    if forecast_category == "Omitted":
        return "Assign category or close out"
    if no_next_step:
        return "Update next step and review date"
    return "Monitor in weekly rhythm review"


def _commercial_value(
    opp_type: str, renewal_acv: float, forecast_arr: float
) -> tuple[float, str, str]:
    if opp_type == "Renewal":
        if renewal_acv > 0:
            return round(renewal_acv, 2), "Renewal ACV", "High"
        return 0.0, "Missing Renewal ACV", "Low"
    if forecast_arr > 0:
        return round(forecast_arr, 2), "Forecast ARR", "High"
    return 0.0, "Missing Forecast ARR", "Low"


def create_dataset(inst: str, tok: str) -> tuple[bool, int]:
    logger.info("\n=== Building %s dataset ===", DS_LABEL)
    opps = _soql(inst, tok, OPP_SOQL)
    logger.info("  Queried %d opportunities", len(opps))
    if not opps:
        raise RuntimeError(
            "No opportunities returned for Commercial Rhythm Control Tower."
        )

    current_month = datetime.now(UTC).strftime("%Y-%m")
    detail_rows: list[dict[str, object]] = []
    grouped_monthly: dict[tuple[str, str, str, str, str], dict[str, float]] = (
        defaultdict(
            lambda: {
                "OpenValue": 0.0,
                "OwnershipReviewValue": 0.0,
                "OmittedValue": 0.0,
                "AtRiskRenewalValue": 0.0,
                "NoNextStepCount": 0.0,
                "OwnershipReviewCount": 0.0,
                "ZeroValueRenewalCount": 0.0,
            }
        )
    )

    for opp in opps:
        account = opp.get("Account") or {}
        opp_owner = opp.get("Owner") or {}
        account_owner = account.get("Owner") or {}
        opp_owner_dim = role_dimension_row(
            owner_id=opp.get("OwnerId") or "",
            owner_name=opp_owner.get("Name") or "Unassigned",
            title=opp_owner.get("Title") or "",
            user_role=((opp_owner.get("UserRole") or {}).get("Name")) or "",
            department=opp_owner.get("Department") or "",
            division=opp_owner.get("Division") or "",
            manager_id=opp_owner.get("ManagerId") or "",
            manager_name=((opp_owner.get("Manager") or {}).get("Name")) or "",
        )
        account_owner_dim = role_dimension_row(
            owner_id=account.get("OwnerId") or "",
            owner_name=account_owner.get("Name") or "Unassigned",
            title=account_owner.get("Title") or "",
            user_role=((account_owner.get("UserRole") or {}).get("Name")) or "",
            department=account_owner.get("Department") or "",
            division=account_owner.get("Division") or "",
            manager_id=account_owner.get("ManagerId") or "",
            manager_name=((account_owner.get("Manager") or {}).get("Name")) or "",
        )

        motion = (opp.get("Type") or "").strip() or "Unknown"
        motion_primary_persona = primary_motion_persona(motion)
        opp_owner_persona = str(opp_owner_dim["Persona"])
        account_owner_persona = str(account_owner_dim["Persona"])
        opp_alignment = ownership_alignment(opp_owner_persona, motion)
        account_alignment = ownership_alignment(account_owner_persona, motion)
        handoff_state = _handoff_state(motion, opp_owner_persona, account_owner_persona)

        close_date = (opp.get("CloseDate") or "")[:10]
        month_label = close_date[:7] if close_date else current_month
        forecast_arr = safe_float(
            opp.get("ConvertedForecastARR") or opp.get("APTS_Forecast_ARR__c")
        )
        renewal_acv = safe_float(opp.get("APTS_Renewal_ACV__c"))
        commercial_value, value_source, semantic_confidence = _commercial_value(
            motion,
            renewal_acv,
            forecast_arr,
        )
        is_closed = str(bool(opp.get("IsClosed"))).lower()
        is_won = str(bool(opp.get("IsWon"))).lower()
        no_next_step_flag = 1 if not (opp.get("NextStep") or "").strip() else 0
        omitted_flag = (
            1
            if (opp.get("ForecastCategoryName") or "") == "Omitted"
            and is_closed == "false"
            else 0
        )
        omitted_value = round(commercial_value if omitted_flag else 0.0, 2)
        late_stage_flag = (
            1
            if _stage_progression_label(opp.get("StageName") or "").startswith(
                ("4 ", "5 ", "6 ")
            )
            else 0
        )
        renewal_metric_missing = 1 if motion == "Renewal" and renewal_acv <= 0 else 0
        zero_value_renewal = 1 if motion == "Renewal" and commercial_value <= 0 else 0
        at_risk_renewal_value = round(
            commercial_value
            if motion == "Renewal"
            and is_closed == "false"
            and (safe_float(opp.get("AgeInDays")) >= 30 or omitted_flag)
            else 0.0,
            2,
        )
        open_value = round(commercial_value if is_closed == "false" else 0.0, 2)
        ownership_review_flag = (
            1 if (opp_alignment == "Needs Review" or renewal_metric_missing) else 0
        )
        ownership_review_value = round(open_value if ownership_review_flag else 0.0, 2)
        renewal_opp_count = 1 if motion == "Renewal" else 0
        covered_renewal_opp_count = 1 if motion == "Renewal" and renewal_acv > 0 else 0
        review_candidate_count = (
            1
            if is_closed == "false"
            and (
                ownership_review_flag
                or omitted_flag
                or no_next_step_flag
                or late_stage_flag
            )
            else 0
        )
        review_pulse = _review_pulse(
            motion,
            handoff_state,
            opp.get("ForecastCategoryName") or "",
            no_next_step_flag,
            zero_value_renewal,
            renewal_metric_missing,
        )
        leadership_ask = _leadership_ask(
            handoff_state,
            opp.get("ForecastCategoryName") or "",
            no_next_step_flag,
            zero_value_renewal,
            renewal_metric_missing,
        )

        row = {
            "RecordType": "detail",
            "Id": opp.get("Id") or "",
            "OpportunityName": (opp.get("Name") or "")[:255],
            "AccountId": opp.get("AccountId") or "",
            "AccountName": (account.get("Name") or "Unknown")[:255],
            "OppOwnerId": opp.get("OwnerId") or "",
            "OppOwnerName": str(opp_owner_dim["OwnerName"])[:255],
            "OppOwnerPersona": opp_owner_persona[:255],
            "OppOwnerRole": str(opp_owner_dim["UserRole"])[:255],
            "OppManagerId": str(opp_owner_dim["ManagerId"])[:255],
            "OppManagerName": str(opp_owner_dim["ManagerName"])[:255],
            "AccountOwnerId": account.get("OwnerId") or "",
            "AccountOwnerName": str(account_owner_dim["OwnerName"])[:255],
            "AccountOwnerPersona": account_owner_persona[:255],
            "AccountManagerName": str(account_owner_dim["ManagerName"])[:255],
            "MotionType": motion[:255],
            "MotionPrimaryPersona": motion_primary_persona[:255],
            "OppOwnershipAlignment": opp_alignment[:255],
            "AccountOwnershipAlignment": account_alignment[:255],
            "HandoffState": handoff_state[:255],
            "SalesRegion": (
                (opp.get("Sales_Region__c") or "Unassigned").strip() or "Unassigned"
            )[:255],
            "UnitGroup": (
                (opp.get("Account_Unit_Group__c") or "Unassigned").strip()
                or "Unassigned"
            )[:255],
            "ForecastCategory": (opp.get("ForecastCategoryName") or "")[:255],
            "StageName": (opp.get("StageName") or "")[:255],
            "StageProgression": _stage_progression_label(opp.get("StageName") or "")[
                :255
            ],
            "NextStep": (opp.get("NextStep") or "")[:255],
            "ReviewPulse": review_pulse[:255],
            "LeadershipAsk": leadership_ask[:255],
            "ValueSource": value_source[:255],
            "SemanticConfidence": semantic_confidence[:255],
            "IsClosed": is_closed,
            "IsWon": is_won,
            "CloseDate": close_date,
            "MonthDate": f"{month_label}-01",
            "MonthLabel": month_label,
            "FYLabel": fiscal_label(int(safe_float(opp.get("FiscalYear"), 0)))
            if safe_float(opp.get("FiscalYear"), 0)
            else "",
            "CloseQuarter": f"Q{int(safe_float(opp.get('FiscalQuarter'), 0))}"
            if safe_float(opp.get("FiscalQuarter"), 0)
            else "",
            "CommercialValue": commercial_value,
            "OpenValue": open_value,
            "OwnershipReviewValue": ownership_review_value,
            "OmittedValue": omitted_value,
            "AtRiskRenewalValue": at_risk_renewal_value,
            "NoNextStepCount": no_next_step_flag,
            "OwnershipReviewCount": ownership_review_flag,
            "OmittedCount": omitted_flag,
            "LateStageCount": late_stage_flag,
            "ZeroValueRenewalCount": zero_value_renewal,
            "RenewalOppCount": renewal_opp_count,
            "CoveredRenewalOppCount": covered_renewal_opp_count,
            "ReviewCandidateCount": review_candidate_count,
        }
        detail_rows.append(row)

        trend_key = (
            row["MonthLabel"],
            row["FYLabel"],
            row["SalesRegion"],
            row["MotionType"],
            row["OppOwnerPersona"],
            row["OppManagerName"],
        )
        bucket = grouped_monthly[trend_key]
        bucket["OpenValue"] += open_value
        bucket["OwnershipReviewValue"] += ownership_review_value
        bucket["OmittedValue"] += omitted_value
        bucket["AtRiskRenewalValue"] += at_risk_renewal_value
        bucket["NoNextStepCount"] += no_next_step_flag
        bucket["OwnershipReviewCount"] += ownership_review_flag
        bucket["ZeroValueRenewalCount"] += zero_value_renewal

    trend_rows = []
    for (
        month_label,
        fy_label,
        sales_region,
        motion,
        owner_persona,
        opp_manager_name,
    ), values in grouped_monthly.items():
        trend_rows.append(
            {
                "RecordType": "trend",
                "Id": "",
                "OpportunityName": "",
                "AccountId": "",
                "AccountName": "",
                "OppOwnerId": "",
                "OppOwnerName": "",
                "OppOwnerPersona": owner_persona,
                "OppOwnerRole": "",
                "OppManagerId": "",
                "OppManagerName": opp_manager_name,
                "AccountOwnerId": "",
                "AccountOwnerName": "",
                "AccountOwnerPersona": "",
                "AccountManagerName": "",
                "MotionType": motion,
                "MotionPrimaryPersona": primary_motion_persona(motion),
                "OppOwnershipAlignment": "",
                "AccountOwnershipAlignment": "",
                "HandoffState": "",
                "SalesRegion": sales_region,
                "UnitGroup": "",
                "ForecastCategory": "",
                "StageName": "",
                "StageProgression": "",
                "NextStep": "",
                "ReviewPulse": "",
                "LeadershipAsk": "",
                "ValueSource": "",
                "SemanticConfidence": "",
                "IsClosed": "false",
                "IsWon": "false",
                "CloseDate": "",
                "MonthDate": f"{month_label}-01",
                "MonthLabel": month_label,
                "FYLabel": fy_label,
                "CloseQuarter": "",
                "CommercialValue": 0.0,
                "OpenValue": round(values["OpenValue"], 2),
                "OwnershipReviewValue": round(values["OwnershipReviewValue"], 2),
                "OmittedValue": round(values["OmittedValue"], 2),
                "AtRiskRenewalValue": round(values["AtRiskRenewalValue"], 2),
                "NoNextStepCount": int(values["NoNextStepCount"]),
                "OwnershipReviewCount": int(values["OwnershipReviewCount"]),
                "OmittedCount": 0,
                "LateStageCount": 0,
                "ZeroValueRenewalCount": int(values["ZeroValueRenewalCount"]),
                "RenewalOppCount": 0,
                "CoveredRenewalOppCount": 0,
                "ReviewCandidateCount": 0,
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
        "OppOwnerId",
        "OppOwnerName",
        "OppOwnerPersona",
        "OppOwnerRole",
        "OppManagerId",
        "OppManagerName",
        "AccountOwnerId",
        "AccountOwnerName",
        "AccountOwnerPersona",
        "AccountManagerName",
        "MotionType",
        "MotionPrimaryPersona",
        "OppOwnershipAlignment",
        "AccountOwnershipAlignment",
        "HandoffState",
        "SalesRegion",
        "UnitGroup",
        "ForecastCategory",
        "StageName",
        "StageProgression",
        "NextStep",
        "ReviewPulse",
        "LeadershipAsk",
        "ValueSource",
        "SemanticConfidence",
        "IsClosed",
        "IsWon",
        "CloseDate",
        "MonthDate",
        "MonthLabel",
        "FYLabel",
        "CloseQuarter",
        "CommercialValue",
        "OpenValue",
        "OwnershipReviewValue",
        "OmittedValue",
        "AtRiskRenewalValue",
        "NoNextStepCount",
        "OwnershipReviewCount",
        "OmittedCount",
        "LateStageCount",
        "ZeroValueRenewalCount",
        "RenewalOppCount",
        "CoveredRenewalOppCount",
        "ReviewCandidateCount",
    ]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=field_names, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    csv_bytes = buffer.getvalue().encode("utf-8")

    fields_meta = [
        _dim("RecordType", "Record Type"),
        _dim("Id", "Opportunity ID"),
        _dim("OpportunityName", "Opportunity"),
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account"),
        _dim("OppOwnerId", "Opportunity Owner ID"),
        _dim("OppOwnerName", "Opportunity Owner"),
        _dim("OppOwnerPersona", "Opportunity Owner Persona"),
        _dim("OppOwnerRole", "Opportunity Owner Role"),
        _dim("OppManagerId", "Opportunity Manager ID"),
        _dim("OppManagerName", "Opportunity Manager"),
        _dim("AccountOwnerId", "Account Owner ID"),
        _dim("AccountOwnerName", "Account Owner"),
        _dim("AccountOwnerPersona", "Account Owner Persona"),
        _dim("AccountManagerName", "Account Manager"),
        _dim("MotionType", "Motion"),
        _dim("MotionPrimaryPersona", "Motion Primary Persona"),
        _dim("OppOwnershipAlignment", "Opportunity Ownership Alignment"),
        _dim("AccountOwnershipAlignment", "Account Ownership Alignment"),
        _dim("HandoffState", "Handoff State"),
        _dim("SalesRegion", "Sales Region"),
        _dim("UnitGroup", "Unit Group"),
        _dim("ForecastCategory", "Forecast Category"),
        _dim("StageName", "Stage"),
        _dim("StageProgression", "Stage Progression"),
        _dim("NextStep", "Next Step"),
        _dim("ReviewPulse", "Review Pulse"),
        _dim("LeadershipAsk", "Leadership Ask"),
        _dim("ValueSource", "Value Source"),
        _dim("SemanticConfidence", "Semantic Confidence"),
        _dim("IsClosed", "Is Closed"),
        _dim("IsWon", "Is Won"),
        _date("CloseDate", "Close Date"),
        _date("MonthDate", "Month"),
        _dim("MonthLabel", "Month Label"),
        _dim("FYLabel", "Fiscal Year"),
        _dim("CloseQuarter", "Close Quarter"),
        _measure("CommercialValue", "Commercial Value"),
        _measure("OpenValue", "Open Value"),
        _measure("OwnershipReviewValue", "Ownership Review Value"),
        _measure("OmittedValue", "Omitted Value"),
        _measure("AtRiskRenewalValue", "At-Risk Renewal Value"),
        _measure("NoNextStepCount", "No Next Step Count", scale=0, precision=8),
        _measure(
            "OwnershipReviewCount", "Ownership Review Count", scale=0, precision=8
        ),
        _measure("OmittedCount", "Omitted Count", scale=0, precision=8),
        _measure("LateStageCount", "Late Stage Count", scale=0, precision=8),
        _measure(
            "ZeroValueRenewalCount", "Zero Value Renewal Count", scale=0, precision=8
        ),
        _measure("RenewalOppCount", "Renewal Opportunity Count", scale=0, precision=8),
        _measure(
            "CoveredRenewalOppCount",
            "Covered Renewal Opportunity Count",
            scale=0,
            precision=8,
        ),
        _measure(
            "ReviewCandidateCount", "Review Candidate Count", scale=0, precision=8
        ),
    ]
    result = upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)
    return result, len(rows)


def build_steps(ds_id: str) -> dict[str, dict]:
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    fy = coalesce_filter("f_fy", "FYLabel")
    region = coalesce_filter("f_region", "SalesRegion")
    motion = coalesce_filter("f_motion", "MotionType")
    persona = coalesce_filter("f_persona", "OppOwnerPersona")
    manager = coalesce_filter("f_manager", "OppManagerName")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + fy
        + region
        + motion
        + persona
        + manager
    )
    trend = (
        load
        + 'q = filter q by RecordType == "trend";\n'
        + fy
        + region
        + motion
        + persona
        + manager
    )

    return {
        "f_fy": af("FYLabel", ds_meta, start=f'["{CURRENT_FY_LABEL}"]'),
        "f_region": af("SalesRegion", ds_meta),
        "f_motion": af("MotionType", ds_meta),
        "f_persona": af("OppOwnerPersona", ds_meta),
        "f_manager": af("OppManagerName", ds_meta),
        "s_summary": sq(
            detail
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(OpenValue) as OpenValue, "
            + "sum(OwnershipReviewValue) as OwnershipReviewValue, "
            + "sum(NoNextStepCount) as NoNextStepCount, "
            + "sum(OmittedValue) as OmittedValue, "
            + "sum(CoveredRenewalOppCount) as CoveredRenewalOppCount, "
            + "sum(RenewalOppCount) as RenewalOppCount;\n"
            + "q = foreach q generate OpenValue, OwnershipReviewValue, NoNextStepCount, OmittedValue, "
            + "case when RenewalOppCount > 0 then (CoveredRenewalOppCount / RenewalOppCount) * 100 else 100 end as RenewalCoveragePct;"
        ),
        "s_value_trend": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(OwnershipReviewValue) as OwnershipReviewValue, "
            + "sum(OmittedValue) as OmittedValue, "
            + "sum(AtRiskRenewalValue) as AtRiskRenewalValue;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_count_trend": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(NoNextStepCount) as NoNextStepCount, "
            + "sum(OwnershipReviewCount) as OwnershipReviewCount, "
            + "sum(ZeroValueRenewalCount) as ZeroValueRenewalCount;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_breach_queue": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by ReviewCandidateCount > 0;\n"
            + "q = group q by (OpportunityName, AccountName, MotionType, OppOwnerName, OppOwnerPersona, HandoffState, ReviewPulse, LeadershipAsk, Id, AccountId);\n"
            + "q = foreach q generate OpportunityName, AccountName, MotionType, OppOwnerName, OppOwnerPersona, HandoffState, ReviewPulse, LeadershipAsk, max(OpenValue) as OpenValue, Id, AccountId;\n"
            + "q = order q by OpenValue desc;\n"
            + "q = limit q 12;"
        ),
        "s_motion_owner_matrix": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (MotionType, OppOwnerPersona);\n"
            + "q = foreach q generate MotionType, OppOwnerPersona, sum(OpenValue) as OpenValue;\n"
            + "q = order q by OpenValue desc;"
        ),
        "s_handoff_pressure": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by OppManagerName;\n"
            + "q = foreach q generate OppManagerName, "
            + "sum(OpenValue) as OpenValue, "
            + "sum(OwnershipReviewValue) as OwnershipReviewValue, "
            + "sum(AtRiskRenewalValue) as AtRiskRenewalValue, "
            + "sum(NoNextStepCount) as NoNextStepCount, "
            + "sum(ReviewCandidateCount) as ReviewCandidateCount;\n"
            + "q = order q by OwnershipReviewValue desc, AtRiskRenewalValue desc;\n"
            + "q = limit q 15;"
        ),
        "s_ownership_review_queue": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by OwnershipReviewCount > 0;\n"
            + "q = group q by (OpportunityName, AccountName, MotionType, OppOwnerName, OppOwnerPersona, HandoffState, LeadershipAsk, Id, AccountId);\n"
            + "q = foreach q generate OpportunityName, AccountName, MotionType, OppOwnerName, OppOwnerPersona, HandoffState, LeadershipAsk, max(OpenValue) as OpenValue, Id, AccountId;\n"
            + "q = order q by OpenValue desc;\n"
            + "q = limit q 12;"
        ),
        "s_renewal_confidence": sq(
            detail
            + 'q = filter q by MotionType == "Renewal";\n'
            + "q = group q by (OppManagerName, OppOwnerName, OppOwnerPersona, OppOwnershipAlignment);\n"
            + "q = foreach q generate OppManagerName, OppOwnerName, OppOwnerPersona, OppOwnershipAlignment, "
            + "sum(RenewalOppCount) as RenewalOppCount, "
            + "sum(CoveredRenewalOppCount) as CoveredRenewalOppCount, "
            + "sum(AtRiskRenewalValue) as AtRiskRenewalValue, "
            + "sum(ZeroValueRenewalCount) as ZeroValueRenewalCount;\n"
            + "q = foreach q generate OppManagerName, OppOwnerName, OppOwnerPersona, OppOwnershipAlignment, "
            + "RenewalOppCount, CoveredRenewalOppCount, AtRiskRenewalValue, ZeroValueRenewalCount, "
            + "case when RenewalOppCount > 0 then (CoveredRenewalOppCount / RenewalOppCount) * 100 else 0 end as RenewalCoveragePct;\n"
            + "q = order q by RenewalCoveragePct asc, ZeroValueRenewalCount desc, AtRiskRenewalValue desc;\n"
            + "q = limit q 15;"
        ),
        "s_process_hygiene": sq(
            detail
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by (OppManagerName, OppOwnerName, OppOwnerPersona);\n"
            + "q = foreach q generate OppManagerName, OppOwnerName, OppOwnerPersona, "
            + "sum(OpenValue) as OpenValue, "
            + "sum(NoNextStepCount) as NoNextStepCount, "
            + "sum(OmittedCount) as OmittedCount, "
            + "sum(LateStageCount) as LateStageCount, "
            + "sum(ReviewCandidateCount) as ReviewCandidateCount;\n"
            + "q = order q by ReviewCandidateCount desc, OpenValue desc;\n"
            + "q = limit q 15;"
        ),
        "s_zero_value_renewals": sq(
            detail
            + 'q = filter q by MotionType == "Renewal";\n'
            + "q = filter q by ZeroValueRenewalCount > 0;\n"
            + "q = group q by (OpportunityName, AccountName, OppOwnerName, OppOwnerPersona, HandoffState, LeadershipAsk, Id, AccountId);\n"
            + "q = foreach q generate OpportunityName, AccountName, OppOwnerName, OppOwnerPersona, HandoffState, LeadershipAsk, max(CloseDate) as CloseDate, Id, AccountId;\n"
            + "q = order q by CloseDate desc;\n"
            + "q = limit q 15;"
        ),
    }


def build_widgets() -> dict[str, dict]:
    widgets = {
        "p1_link_sales": nav_link_external(
            SALES_MANAGER_DASHBOARD_ID, "Sales Manager", include_state=False
        ),
        "p1_link_csm": nav_link_external(
            CSM_MANAGER_DASHBOARD_ID, "CSM Manager", include_state=False
        ),
        "p1_link_account": nav_link_external(
            ACCOUNT_360_DASHBOARD_ID, "Account 360", include_state=False
        ),
        "p1_hdr": hdr(
            "Commercial Rhythm Control Tower",
            "Cross-suite operating view of motion ownership, handoff quality, forecast hygiene, and renewal semantic confidence.",
        ),
        "p1_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p1_f_region": pillbox("f_region", "Region"),
        "p1_f_motion": pillbox("f_motion", "Motion"),
        "p1_f_persona": pillbox("f_persona", "Owner Persona"),
        "p1_f_manager": pillbox("f_manager", "Manager"),
        "p1_n_open": num(
            "s_summary",
            "OpenValue",
            "Open Commercial Value",
            "#032D60",
            compact=True,
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_review": num(
            "s_summary",
            "OwnershipReviewValue",
            "Ownership Review Value",
            "#BA0517",
            compact=True,
            prefix="€",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_next": num(
            "s_summary",
            "NoNextStepCount",
            "No Next Step Deals",
            "#8B5D00",
            compact=False,
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_coverage": num(
            "s_summary",
            "RenewalCoveragePct",
            "Renewal Coverage %",
            "#0176D3",
            compact=False,
            suffix="%",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_ch_value": line_chart(
            "s_value_trend",
            "Value at Risk Over Time",
            axis_title="Value (EUR)",
            subtitle="Ownership review value, omitted value, and at-risk renewal value by month.",
        ),
        "p1_ch_count": line_chart(
            "s_count_trend",
            "Process Pressure Counts Over Time",
            axis_title="Deal Count",
            subtitle="Counts of missing next step, ownership-review, and zero-value renewal issues by month.",
        ),
        "p1_tbl_queue": compare_table(
            "s_breach_queue",
            "Top Rhythm Breach Queue",
            columns=[
                "OpportunityName",
                "AccountName",
                "MotionType",
                "OppOwnerName",
                "OppOwnerPersona",
                "HandoffState",
                "ReviewPulse",
                "LeadershipAsk",
                "OpenValue",
            ],
        ),
        "p2_link_sales": nav_link_external(
            SALES_MANAGER_DASHBOARD_ID, "Sales Manager", include_state=False
        ),
        "p2_link_csm": nav_link_external(
            CSM_MANAGER_DASHBOARD_ID, "CSM Manager", include_state=False
        ),
        "p2_link_account": nav_link_external(
            ACCOUNT_360_DASHBOARD_ID, "Account 360", include_state=False
        ),
        "p2_hdr": hdr(
            "Ownership & Handoffs",
            "Which motions are owned by the right persona, where handoffs are healthy, and where ownership needs intervention.",
        ),
        "p2_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_f_motion": pillbox("f_motion", "Motion"),
        "p2_f_persona": pillbox("f_persona", "Owner Persona"),
        "p2_f_manager": pillbox("f_manager", "Manager"),
        "p2_ch_matrix": heatmap_chart(
            "s_motion_owner_matrix", "Motion x Owner Persona Open Value"
        ),
        "p2_tbl_pressure": compare_table(
            "s_handoff_pressure",
            "Manager Handoff Pressure",
            columns=[
                "OppManagerName",
                "OpenValue",
                "OwnershipReviewValue",
                "AtRiskRenewalValue",
                "NoNextStepCount",
                "ReviewCandidateCount",
            ],
        ),
        "p2_tbl_review": compare_table(
            "s_ownership_review_queue",
            "Ownership Review Queue",
            columns=[
                "OpportunityName",
                "AccountName",
                "MotionType",
                "OppOwnerName",
                "OppOwnerPersona",
                "HandoffState",
                "LeadershipAsk",
                "OpenValue",
            ],
        ),
        "p3_link_sales": nav_link_external(
            SALES_MANAGER_DASHBOARD_ID, "Sales Manager", include_state=False
        ),
        "p3_link_csm": nav_link_external(
            CSM_MANAGER_DASHBOARD_ID, "CSM Manager", include_state=False
        ),
        "p3_link_account": nav_link_external(
            ACCOUNT_360_DASHBOARD_ID, "Account 360", include_state=False
        ),
        "p3_hdr": hdr(
            "Process Quality",
            "Renewal semantic confidence and operating hygiene checks that determine whether the suite can be trusted.",
        ),
        "p3_f_fy": pillbox("f_fy", "Fiscal Year"),
        "p3_f_region": pillbox("f_region", "Region"),
        "p3_f_motion": pillbox("f_motion", "Motion"),
        "p3_f_persona": pillbox("f_persona", "Owner Persona"),
        "p3_f_manager": pillbox("f_manager", "Manager"),
        "p3_tbl_conf": compare_table(
            "s_renewal_confidence",
            "Renewal Semantic Confidence by Owner",
            columns=[
                "OppManagerName",
                "OppOwnerName",
                "OppOwnerPersona",
                "OppOwnershipAlignment",
                "RenewalCoveragePct",
                "ZeroValueRenewalCount",
                "AtRiskRenewalValue",
            ],
        ),
        "p3_tbl_hygiene": compare_table(
            "s_process_hygiene",
            "Owner Process Hygiene",
            columns=[
                "OppManagerName",
                "OppOwnerName",
                "OppOwnerPersona",
                "OpenValue",
                "NoNextStepCount",
                "OmittedCount",
                "LateStageCount",
                "ReviewCandidateCount",
            ],
        ),
        "p3_tbl_zero": compare_table(
            "s_zero_value_renewals",
            "Zero-Value Renewal Anomalies",
            columns=[
                "OpportunityName",
                "AccountName",
                "OppOwnerName",
                "OppOwnerPersona",
                "HandoffState",
                "LeadershipAsk",
                "CloseDate",
            ],
        ),
    }
    add_table_action(widgets["p1_tbl_queue"], "salesforceActions", "Opportunity", "Id")
    add_table_action(widgets["p2_tbl_review"], "salesforceActions", "Opportunity", "Id")
    add_table_action(widgets["p3_tbl_zero"], "salesforceActions", "Opportunity", "Id")
    return widgets


def build_layout() -> dict:
    p1 = [
        {"name": "p1_link_sales", "row": 1, "column": 0, "colspan": 4, "rowspan": 1},
        {"name": "p1_link_csm", "row": 1, "column": 4, "colspan": 4, "rowspan": 1},
        {"name": "p1_link_account", "row": 1, "column": 8, "colspan": 4, "rowspan": 1},
        {"name": "p1_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_fy", "row": 4, "column": 0, "colspan": 2, "rowspan": 2},
        {"name": "p1_f_region", "row": 4, "column": 2, "colspan": 2, "rowspan": 2},
        {"name": "p1_f_motion", "row": 4, "column": 4, "colspan": 2, "rowspan": 2},
        {"name": "p1_f_persona", "row": 4, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_manager", "row": 4, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p1_n_open", "row": 6, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_review", "row": 6, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_next", "row": 6, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_coverage", "row": 6, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p1_ch_value", "row": 10, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p1_ch_count", "row": 10, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p1_tbl_queue", "row": 17, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    p2 = [
        {"name": "p2_link_sales", "row": 1, "column": 0, "colspan": 4, "rowspan": 1},
        {"name": "p2_link_csm", "row": 1, "column": 4, "colspan": 4, "rowspan": 1},
        {"name": "p2_link_account", "row": 1, "column": 8, "colspan": 4, "rowspan": 1},
        {"name": "p2_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_fy", "row": 4, "column": 0, "colspan": 2, "rowspan": 2},
        {"name": "p2_f_region", "row": 4, "column": 2, "colspan": 2, "rowspan": 2},
        {"name": "p2_f_motion", "row": 4, "column": 4, "colspan": 2, "rowspan": 2},
        {"name": "p2_f_persona", "row": 4, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_manager", "row": 4, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p2_ch_matrix", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_tbl_pressure", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p2_tbl_review", "row": 14, "column": 0, "colspan": 12, "rowspan": 9},
    ]

    p3 = [
        {"name": "p3_link_sales", "row": 1, "column": 0, "colspan": 4, "rowspan": 1},
        {"name": "p3_link_csm", "row": 1, "column": 4, "colspan": 4, "rowspan": 1},
        {"name": "p3_link_account", "row": 1, "column": 8, "colspan": 4, "rowspan": 1},
        {"name": "p3_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_fy", "row": 4, "column": 0, "colspan": 2, "rowspan": 2},
        {"name": "p3_f_region", "row": 4, "column": 2, "colspan": 2, "rowspan": 2},
        {"name": "p3_f_motion", "row": 4, "column": 4, "colspan": 2, "rowspan": 2},
        {"name": "p3_f_persona", "row": 4, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_manager", "row": 4, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p3_tbl_conf", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p3_tbl_hygiene", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p3_tbl_zero", "row": 14, "column": 0, "colspan": 12, "rowspan": 9},
    ]

    return {
        "name": "CommercialRhythmControlTower",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("ownership", "Ownership & Handoffs", p2),
            pg("quality", "Process Quality", p3),
        ],
    }


def main() -> None:
    with builder_run("Commercial_Rhythm_Control_Tower", __file__) as summary:
        inst, tok = get_auth()
        assert_org_schema(
            inst,
            tok,
            objects=["Opportunity"],
        )
        upload_ok, row_count = create_dataset(inst, tok)
        summary.row_count = row_count
        if not upload_ok:
            raise SystemExit("Dataset upload failed")

        ds_id = get_dataset_id(inst, tok, DS)
        if not ds_id:
            raise SystemExit(f"Could not resolve dataset id for {DS}")
        summary.dataset_id = ds_id

        steps = build_steps(ds_id)
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

        dashboard_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
        logger.info("\n=== Deploying %s ===", DASHBOARD_LABEL)
        deploy_dashboard(inst, tok, dashboard_id, state)
        set_record_links_xmd(
            inst,
            tok,
            DS,
            [
                {"field": "OpportunityName", "id_field": "Id", "label": "Opportunity"},
                {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
            ],
        )


if __name__ == "__main__":
    main()
