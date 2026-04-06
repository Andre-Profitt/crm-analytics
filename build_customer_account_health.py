#!/usr/bin/env python3
"""Build the Customer & Account Health dashboard.

This is the Wave 2 manager dashboard that consolidates:
- Customer Intelligence
- Account Intelligence KPIs

Design goals:
- 4-page manager surface for customer health, renewal pressure, and account hygiene
- account-level action queues with record links
- stronger time-series views for customer additions, revenue trajectory, and renewal risk
"""

from __future__ import annotations

import csv
import io
import logging
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta


from crm_analytics_runtime import builder_run  # pyright: ignore[reportMissingImports]
from simcorp_fields import assert_org_schema  # pyright: ignore[reportMissingImports]

from crm_analytics_helpers import (
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
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    hdr,
    heatmap_chart,
    nav_link,
    nav_row,
    num,
    pg,
    pillbox,
    rich_chart,
    sankey_chart,
    set_record_links_xmd,
    sq,
    timeline_chart,
    treemap_chart,
    upload_dataset,
)
from portfolio_foundation import (
    least_squares,
    month_key,
    month_sequence,
    prediction_interval,
    risk_level_to_score,
    safe_float,
)

logger = logging.getLogger(__name__)

DS = "Customer_Account_Health"
DS_LABEL = "Customer Account Health"
DASHBOARD_LABEL = "Customer & Account Health"


ACCOUNT_SOQL = (
    "SELECT Id, Name, Owner.Name, Type, CreatedDate, BillingCountry, "
    "Industry, Unit_Group__c, SaaS_Client__c, Axioma_Client__c, "
    "Risk_of_Potential_Termination__c, KYC_Approval_Status__c, "
    "DUNS_No__c, Partner_Engagement_Level__c, "
    "APTS_Subscription_Term__c, Termination_Date__c, "
    "Expected_Termination_Date__c, Termination_Reason__c, "
    "AuM_m__c, NumberOfEmployees "
    "FROM Account "
    "WHERE CreatedDate >= 2020-01-01T00:00:00Z"
)

OPP_SOQL = (
    "SELECT Id, AccountId, Type, IsClosed, IsWon, "
    "FiscalYear, ForecastCategoryName, Probability, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
    "APTS_RH_Product_Family__c, CloseDate "
    "FROM Opportunity "
    "WHERE FiscalYear IN (2024, 2025, 2026, 2027)"
)

CONTRACT_SOQL = (
    "SELECT Id, AccountId, Status, StartDate, EndDate, "
    "ContractTerm, Agreement_Type__c "
    "FROM Contract "
    "WHERE CreatedDate >= 2022-01-01T00:00:00Z"
)

CONTACT_SOQL = (
    "SELECT Id, AccountId, Title, LastActivityDate "
    "FROM Contact "
    "WHERE CreatedDate >= 2022-01-01T00:00:00Z"
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
    """Parse an ISO-style date string."""
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


def _segment(aum: float, arr: float) -> str:
    """Classify account into segments based on AuM and ARR."""
    if aum > 100000 or arr > 500000:
        return "Enterprise"
    if aum > 10000 or arr > 100000:
        return "Mid-Market"
    if arr > 0:
        return "Growth"
    return "Prospect"


def _data_quality_band(score: float) -> str:
    """Map data quality score to a band."""
    if score >= 80:
        return "Good"
    if score >= 60:
        return "Fair"
    return "Poor"


def _health_band(score: float) -> str:
    """Map health score to a band."""
    if score >= 70:
        return "Healthy"
    if score >= 40:
        return "At Risk"
    return "Critical"


def _term_bucket(term_months: float) -> str:
    """Bucket contract terms into manager-friendly ranges."""
    if term_months <= 0:
        return "Unknown"
    if term_months <= 12:
        return "0-12m"
    if term_months <= 24:
        return "13-24m"
    if term_months <= 36:
        return "25-36m"
    return "36m+"


def _health_score(metrics: dict[str, object], today: date) -> float:
    """Compute account health score."""
    score = 0.0

    data_quality = safe_float(metrics.get("DataQualityScore"))
    if data_quality >= 80:
        score += 15.0
    elif data_quality >= 60:
        score += 10.0
    else:
        score += 4.0

    active_contracts = safe_float(metrics.get("ActiveContracts"))
    expiring_90 = safe_float(metrics.get("ExpiringContracts90d"))
    if active_contracts > 0 and expiring_90 == 0:
        score += 20.0
    elif active_contracts > 0:
        score += 10.0
    elif safe_float(metrics.get("TotalContracts")) > 0:
        score += 5.0

    won_26 = safe_float(metrics.get("WonARR_FY26"))
    won_25 = safe_float(metrics.get("WonARR_FY25"))
    if won_25 > 0 and won_26 >= won_25:
        score += 15.0
    elif won_26 > 0:
        score += 12.0
    elif safe_float(metrics.get("TotalWonARR")) > 0:
        score += 6.0

    contacts = safe_float(metrics.get("ContactCount"))
    c_level = safe_float(metrics.get("CLevelContacts"))
    recent_activity = safe_float(metrics.get("RecentActivityCount"))
    if contacts >= 5 and c_level >= 1 and recent_activity >= 2:
        score += 18.0
    elif contacts >= 3 and recent_activity >= 1:
        score += 12.0
    elif contacts >= 1:
        score += 6.0

    product_count = safe_float(metrics.get("ProductCount"))
    if product_count >= 3:
        score += 12.0
    elif product_count >= 2:
        score += 8.0
    elif product_count >= 1:
        score += 4.0

    kyc_status = (metrics.get("KYCStatus") or "").strip()
    if kyc_status == "Approved":
        score += 10.0
    elif kyc_status in {"Approval Requested", "On Hold"}:
        score += 4.0

    risk_level = (metrics.get("RiskLevel") or "").strip()
    score += max(0.0, 15.0 - (risk_level_to_score(risk_level) / 7.5))

    last_activity = _parse_date(str(metrics.get("LastActivityDate") or ""))
    if last_activity:
        days_since = (today - last_activity).days
        if days_since <= 30:
            score += 10.0
        elif days_since <= 90:
            score += 7.0
        elif days_since <= 180:
            score += 4.0

    return round(max(0.0, min(100.0, score)), 1)


def _expansion_score(metrics: dict[str, object]) -> float:
    """Compute expansion opportunity score."""
    score = 0.0

    expand_arr = safe_float(metrics.get("ExpandPipelineARR"))
    if expand_arr > 100000:
        score += 25.0
    elif expand_arr > 0:
        score += 15.0

    product_count = safe_float(metrics.get("ProductCount"))
    is_saas = str(metrics.get("IsSaaS") or "false").lower() == "true"
    is_axioma = str(metrics.get("IsAxioma") or "false").lower() == "true"
    if product_count < 2 and (is_saas or is_axioma):
        score += 20.0
    elif product_count < 3:
        score += 10.0

    expiring_90 = safe_float(metrics.get("ExpiringContracts90d"))
    if expiring_90 > 0:
        score += 15.0

    contacts = safe_float(metrics.get("ContactCount"))
    if contacts >= 5:
        score += 15.0
    elif contacts >= 2:
        score += 10.0

    expand_won = safe_float(metrics.get("ExpandWonARR"))
    if expand_won > 0:
        score += 25.0
    elif safe_float(metrics.get("TotalWonARR")) > 0:
        score += 10.0

    return round(max(0.0, min(100.0, score)), 1)


def _renewal_risk_score(metrics: dict[str, object]) -> float:
    """Compute renewal risk score."""
    score = risk_level_to_score(str(metrics.get("RiskLevel") or ""))

    expiring_90 = safe_float(metrics.get("ExpiringContracts90d"))
    expiring_180 = safe_float(metrics.get("ExpiringContracts180d"))
    if expiring_90 > 0:
        score += 25.0
    elif expiring_180 > 0:
        score += 10.0

    renewal_pipeline = safe_float(metrics.get("RenewalPipelineARR"))
    if renewal_pipeline > 0:
        score += 12.0

    health_score = safe_float(metrics.get("HealthScore"))
    if health_score < 40:
        score += 20.0
    elif health_score < 60:
        score += 10.0

    recent_activity = safe_float(metrics.get("RecentActivityCount"))
    if recent_activity == 0:
        score += 10.0

    kyc_status = (metrics.get("KYCStatus") or "").strip()
    if kyc_status != "Approved":
        score += 8.0

    return round(max(0.0, min(100.0, score)), 1)


def _contact_tier(contact_count: int) -> str:
    """Bucket contact coverage into engagement tiers."""
    if contact_count >= 6:
        return "Over-indexed"
    if contact_count >= 3:
        return "Adequate"
    if contact_count >= 1:
        return "Under-indexed"
    return "Dark"


def _expansion_band(score: float) -> str:
    """Map expansion score to a band."""
    if score >= 60:
        return "High"
    if score >= 30:
        return "Medium"
    return "Low"


def _safe_avg(total: float, count: float) -> float:
    """Return average, guarding against divide-by-zero."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)


def create_dataset(inst: str, tok: str) -> tuple[bool, int]:
    """Build merged customer and account health dataset."""
    logger.info("\n=== Building %s dataset ===", DS_LABEL)
    accounts = _soql(inst, tok, ACCOUNT_SOQL)
    opps = _soql(inst, tok, OPP_SOQL)
    contracts = _soql(inst, tok, CONTRACT_SOQL)
    contacts = _soql(inst, tok, CONTACT_SOQL)
    logger.info("  Queried %d accounts", len(accounts))
    logger.info("  Queried %d opportunities", len(opps))
    logger.info("  Queried %d contracts", len(contracts))
    logger.info("  Queried %d contacts", len(contacts))

    today = datetime.now(UTC).date()
    recent_activity_cutoff = today - timedelta(days=180)
    current_month = datetime.now(UTC).strftime("%Y-%m")
    last_complete_month = _add_months(current_month, -1)
    recent_start_month = _add_months(current_month, -23)
    forecast_end_month = _add_months(current_month, 3)
    renewal_end_month = _add_months(current_month, 12)
    current_year_start_month = f"{today.year:04d}-01"
    current_year_end_month = f"{today.year:04d}-12"

    acct_opps: dict[str, dict[str, object]] = {}
    for opp in opps:
        account_id = opp.get("AccountId")
        if not account_id:
            continue

        bucket = acct_opps.setdefault(
            account_id,
            {
                "TotalWonARR": 0.0,
                "WonARR_FY24": 0.0,
                "WonARR_FY25": 0.0,
                "WonARR_FY26": 0.0,
                "TotalLostARR": 0.0,
                "OpenPipelineARR": 0.0,
                "ExpandPipelineARR": 0.0,
                "RenewalPipelineARR": 0.0,
                "LandPipelineARR": 0.0,
                "ExpandWonARR": 0.0,
                "RenewalWonARR": 0.0,
                "LandWonARR": 0.0,
                "WonCount": 0,
                "LostCount": 0,
                "OpenCount": 0,
                "Products": set(),
                "LastWonDate": "",
                "LastLostDate": "",
                "FirstLandWonDate": "",
                "FirstExpandWonDate": "",
            },
        )

        arr = safe_float(opp.get("ConvertedARR"))
        is_closed = str(opp.get("IsClosed") or "").lower() == "true"
        is_won = str(opp.get("IsWon") or "").lower() == "true"
        fiscal_year = int(safe_float(opp.get("FiscalYear"), 0))
        opp_type = (opp.get("Type") or "").strip()
        close_date = (opp.get("CloseDate") or "")[:10]
        product = (opp.get("APTS_RH_Product_Family__c") or "").split(";")[0].strip()

        if product:
            cast_products = bucket["Products"]
            if isinstance(cast_products, set):
                cast_products.add(product)

        if is_won:
            bucket["TotalWonARR"] = safe_float(bucket["TotalWonARR"]) + arr
            bucket["WonCount"] = int(safe_float(bucket["WonCount"])) + 1
            if close_date > str(bucket["LastWonDate"]):
                bucket["LastWonDate"] = close_date
            if fiscal_year == 2024:
                bucket["WonARR_FY24"] = safe_float(bucket["WonARR_FY24"]) + arr
            elif fiscal_year == 2025:
                bucket["WonARR_FY25"] = safe_float(bucket["WonARR_FY25"]) + arr
            elif fiscal_year == 2026:
                bucket["WonARR_FY26"] = safe_float(bucket["WonARR_FY26"]) + arr
            if opp_type == "Expand":
                bucket["ExpandWonARR"] = safe_float(bucket["ExpandWonARR"]) + arr
                if not bucket["FirstExpandWonDate"] or close_date < str(
                    bucket["FirstExpandWonDate"]
                ):
                    bucket["FirstExpandWonDate"] = close_date
            elif opp_type == "Renewal":
                bucket["RenewalWonARR"] = safe_float(bucket["RenewalWonARR"]) + arr
            elif opp_type == "Land":
                bucket["LandWonARR"] = safe_float(bucket["LandWonARR"]) + arr
                if not bucket["FirstLandWonDate"] or close_date < str(
                    bucket["FirstLandWonDate"]
                ):
                    bucket["FirstLandWonDate"] = close_date
        elif is_closed:
            bucket["TotalLostARR"] = safe_float(bucket["TotalLostARR"]) + arr
            bucket["LostCount"] = int(safe_float(bucket["LostCount"])) + 1
            if close_date > str(bucket["LastLostDate"]):
                bucket["LastLostDate"] = close_date
        else:
            bucket["OpenPipelineARR"] = safe_float(bucket["OpenPipelineARR"]) + arr
            bucket["OpenCount"] = int(safe_float(bucket["OpenCount"])) + 1
            if opp_type == "Expand":
                bucket["ExpandPipelineARR"] = (
                    safe_float(bucket["ExpandPipelineARR"]) + arr
                )
            elif opp_type == "Renewal":
                bucket["RenewalPipelineARR"] = (
                    safe_float(bucket["RenewalPipelineARR"]) + arr
                )
            elif opp_type == "Land":
                bucket["LandPipelineARR"] = safe_float(bucket["LandPipelineARR"]) + arr

    acct_contracts: dict[str, dict[str, object]] = {}
    for contract in contracts:
        account_id = contract.get("AccountId")
        if not account_id:
            continue

        bucket = acct_contracts.setdefault(
            account_id,
            {
                "TotalContracts": 0,
                "ActiveContracts": 0,
                "ExpiringContracts90d": 0,
                "ExpiringContracts180d": 0,
                "TermSum": 0.0,
                "MultiYearCount": 0,
                "NextEndDate": "",
                "LatestEndDate": "",
            },
        )

        bucket["TotalContracts"] = int(safe_float(bucket["TotalContracts"])) + 1
        status = (contract.get("Status") or "").strip()
        end_date = (contract.get("EndDate") or "")[:10]
        contract_term = safe_float(contract.get("ContractTerm"))

        if status in {"Activated", "Active"}:
            bucket["ActiveContracts"] = int(safe_float(bucket["ActiveContracts"])) + 1
        if end_date:
            if end_date > str(bucket["LatestEndDate"]):
                bucket["LatestEndDate"] = end_date
            expiry = _parse_date(end_date)
            if expiry:
                days_to = (expiry - today).days
                if status in {"Activated", "Active"} and days_to >= 0:
                    if not bucket["NextEndDate"] or end_date < str(
                        bucket["NextEndDate"]
                    ):
                        bucket["NextEndDate"] = end_date
                if 0 < days_to <= 90:
                    bucket["ExpiringContracts90d"] = (
                        int(safe_float(bucket["ExpiringContracts90d"])) + 1
                    )
                if 0 < days_to <= 180:
                    bucket["ExpiringContracts180d"] = (
                        int(safe_float(bucket["ExpiringContracts180d"])) + 1
                    )

        if contract_term > 0:
            bucket["TermSum"] = safe_float(bucket["TermSum"]) + contract_term
            if contract_term > 12:
                bucket["MultiYearCount"] = int(safe_float(bucket["MultiYearCount"])) + 1

    acct_contacts: dict[str, dict[str, object]] = {}
    for contact in contacts:
        account_id = contact.get("AccountId")
        if not account_id:
            continue

        bucket = acct_contacts.setdefault(
            account_id,
            {
                "ContactCount": 0,
                "CLevelContacts": 0,
                "LastActivityDate": "",
                "RecentActivityCount": 0,
            },
        )

        bucket["ContactCount"] = int(safe_float(bucket["ContactCount"])) + 1
        title = (contact.get("Title") or "").lower()
        if any(
            value in title
            for value in {
                "chief",
                "ceo",
                "cfo",
                "cto",
                "coo",
                "cio",
                "president",
                "vice president",
                "vp",
                "managing director",
                "head of",
            }
        ):
            bucket["CLevelContacts"] = int(safe_float(bucket["CLevelContacts"])) + 1

        activity_date = (contact.get("LastActivityDate") or "")[:10]
        if activity_date > str(bucket["LastActivityDate"]):
            bucket["LastActivityDate"] = activity_date
        parsed_activity = _parse_date(activity_date)
        if parsed_activity and parsed_activity >= recent_activity_cutoff:
            bucket["RecentActivityCount"] = (
                int(safe_float(bucket["RecentActivityCount"])) + 1
            )

    detail_rows: list[dict[str, object]] = []
    account_lookup: dict[str, dict[str, object]] = {}

    for account in accounts:
        account_id = account.get("Id")
        if not account_id:
            continue

        opp_metrics = acct_opps.get(account_id, {})
        contract_metrics = acct_contracts.get(account_id, {})
        contact_metrics = acct_contacts.get(account_id, {})

        total_won = safe_float(opp_metrics.get("TotalWonARR"))
        open_pipeline = safe_float(opp_metrics.get("OpenPipelineARR"))
        total_contracts = safe_float(contract_metrics.get("TotalContracts"))
        if total_won == 0 and open_pipeline == 0 and total_contracts == 0:
            continue

        products = opp_metrics.get("Products") or set()
        product_count = len(products) if isinstance(products, set) else 0

        unit_group = (
            (account.get("Unit_Group__c") or "Unassigned").strip() or "Unassigned"
        )[:255]
        billing_country = (
            (account.get("BillingCountry") or "Unknown").strip() or "Unknown"
        )[:255]
        industry = ((account.get("Industry") or "Unknown").strip() or "Unknown")[:255]
        account_type = ((account.get("Type") or "Unknown").strip() or "Unknown")[:255]
        owner_name = ((account.get("Owner") or {}).get("Name") or "Unknown")[:255]
        created_date = (account.get("CreatedDate") or "")[:10]
        customer_since = created_date[:4]
        is_saas = str(account.get("SaaS_Client__c") or False).lower()
        is_axioma = str(account.get("Axioma_Client__c") or False).lower()
        risk_level = (
            (account.get("Risk_of_Potential_Termination__c") or "").strip() or "Low"
        )[:255]
        kyc_status = (
            (account.get("KYC_Approval_Status__c") or "Not Started").strip()
            or "Not Started"
        )[:255]
        partner_level = ((account.get("Partner_Engagement_Level__c") or "").strip())[
            :255
        ]
        termination_date = (account.get("Termination_Date__c") or "")[:10]
        expected_termination_date = (account.get("Expected_Termination_Date__c") or "")[
            :10
        ]
        next_end_date = (contract_metrics.get("NextEndDate") or "")[:10]
        latest_end_date = (contract_metrics.get("LatestEndDate") or "")[:10]
        future_dates = []
        for candidate in (next_end_date, expected_termination_date, termination_date):
            parsed = _parse_date(candidate)
            if parsed and parsed >= today:
                future_dates.append(candidate)
        renewal_date = (
            min(future_dates)
            if future_dates
            else (
                next_end_date
                or expected_termination_date
                or termination_date
                or latest_end_date
            )
        )

        has_duns = "true" if account.get("DUNS_No__c") else "false"
        has_unit_group = "true" if account.get("Unit_Group__c") else "false"
        has_axioma_id = "true" if is_axioma == "true" else "false"

        data_quality_score = float(
            (20 if has_duns == "true" else 0)
            + (20 if has_unit_group == "true" else 0)
            + (20 if industry != "Unknown" else 0)
            + (20 if billing_country != "Unknown" else 0)
            + (20 if account_type != "Unknown" else 0)
        )

        won_25 = safe_float(opp_metrics.get("WonARR_FY25"))
        won_26 = safe_float(opp_metrics.get("WonARR_FY26"))
        expand_pipeline = safe_float(opp_metrics.get("ExpandPipelineARR"))
        renewal_pipeline = safe_float(opp_metrics.get("RenewalPipelineARR"))
        expand_won = safe_float(opp_metrics.get("ExpandWonARR"))
        renewal_won = safe_float(opp_metrics.get("RenewalWonARR"))
        recent_activity_count = int(
            safe_float(contact_metrics.get("RecentActivityCount"))
        )
        contact_count = int(safe_float(contact_metrics.get("ContactCount")))
        c_level_contacts = int(safe_float(contact_metrics.get("CLevelContacts")))
        aum = round(safe_float(account.get("AuM_m__c")), 2)
        employees = int(safe_float(account.get("NumberOfEmployees")))

        scoring = {
            "DataQualityScore": data_quality_score,
            "ActiveContracts": contract_metrics.get("ActiveContracts", 0),
            "ExpiringContracts90d": contract_metrics.get("ExpiringContracts90d", 0),
            "ExpiringContracts180d": contract_metrics.get("ExpiringContracts180d", 0),
            "TotalContracts": contract_metrics.get("TotalContracts", 0),
            "WonARR_FY25": won_25,
            "WonARR_FY26": won_26,
            "TotalWonARR": total_won,
            "ContactCount": contact_count,
            "CLevelContacts": c_level_contacts,
            "RecentActivityCount": recent_activity_count,
            "LastActivityDate": contact_metrics.get("LastActivityDate", ""),
            "ProductCount": product_count,
            "KYCStatus": kyc_status,
            "RiskLevel": risk_level,
            "ExpandPipelineARR": expand_pipeline,
            "IsSaaS": is_saas,
            "IsAxioma": is_axioma,
            "ExpandWonARR": expand_won,
        }

        health_score = _health_score(scoring, today)
        scoring["HealthScore"] = health_score
        expansion_score = _expansion_score(scoring)
        renewal_risk_score = _renewal_risk_score(scoring)

        data_quality_band = _data_quality_band(data_quality_score)
        health_band = _health_band(health_score)
        expansion_band = _expansion_band(expansion_score)
        segment = _segment(aum, total_won)
        nrr_proxy = round((won_26 / won_25) * 100, 1) if won_25 > 0 else 0.0
        grr_proxy = (
            round(min((renewal_won / won_25) * 100, 200), 1) if won_25 > 0 else 0.0
        )
        avg_term_months = _safe_avg(
            safe_float(contract_metrics.get("TermSum")),
            safe_float(contract_metrics.get("TotalContracts")),
        )
        contact_tier = _contact_tier(contact_count)
        product_combo = (
            "|".join(sorted(products))
            if isinstance(products, set) and len(products) >= 2
            else ""
        )

        first_land = str(opp_metrics.get("FirstLandWonDate") or "")
        first_expand = str(opp_metrics.get("FirstExpandWonDate") or "")
        land_to_expand_days = (
            _days_between(first_land, first_expand)
            if first_land and first_expand
            else 0
        )

        if health_score < 40 and total_won > 0:
            lifecycle_stage = "At-Risk"
        elif safe_float(opp_metrics.get("TotalLostARR")) > 0 and won_26 == 0:
            lifecycle_stage = "Churning"
        elif customer_since and int(customer_since) >= 2025:
            lifecycle_stage = "Onboarding"
        elif expand_won > 0:
            lifecycle_stage = "Growing"
        elif customer_since and int(customer_since) <= 2022:
            lifecycle_stage = "Mature"
        else:
            lifecycle_stage = "Stable"

        data_quality_gap = 1 if data_quality_score < 60 else 0
        kyc_gap = 1 if kyc_status != "Approved" else 0
        under_engaged = (
            1
            if recent_activity_count == 0 or contact_count < 3 or c_level_contacts == 0
            else 0
        )
        renewal_risk_arr = 0.0
        if (
            renewal_risk_score >= 65
            or int(safe_float(contract_metrics.get("ExpiringContracts90d"))) > 0
        ):
            renewal_risk_arr = (
                renewal_pipeline if renewal_pipeline > 0 else round(total_won * 0.25, 2)
            )
        at_risk_account = (
            1
            if renewal_risk_score >= 65
            or health_score < 40
            or risk_level in {"High", "Critical"}
            else 0
        )
        operating_gap_score = min(
            100.0,
            (30.0 if kyc_gap else 0.0)
            + (30.0 if data_quality_gap else 0.0)
            + (20.0 if under_engaged else 0.0)
            + (20.0 if contact_tier == "Dark" else 0.0),
        )

        row = {
            "RecordType": "detail",
            "AccountId": account_id,
            "AccountName": (account.get("Name") or "")[:255],
            "OwnerName": owner_name,
            "UnitGroup": unit_group,
            "Segment": segment,
            "HealthBand": health_band,
            "Industry": industry,
            "BillingCountry": billing_country,
            "AccountType": account_type,
            "RiskLevel": risk_level,
            "KYCStatus": kyc_status,
            "PartnerLevel": partner_level,
            "LifecycleStage": lifecycle_stage,
            "DataQualityBand": data_quality_band,
            "ExpansionBand": expansion_band,
            "ContactTier": contact_tier,
            "TermBucket": _term_bucket(
                avg_term_months or safe_float(account.get("APTS_Subscription_Term__c"))
            ),
            "ProductCombo": product_combo,
            "CreatedDate": created_date,
            "RenewalDate": renewal_date,
            "MonthDate": "",
            "MonthLabel": "",
            "TrendCategory": "",
            "IsSaaS": is_saas,
            "IsAxioma": is_axioma,
            "HasDUNS": has_duns,
            "HasUnitGroup": has_unit_group,
            "HasAxiomaId": has_axioma_id,
            "DataQualityScore": round(data_quality_score, 1),
            "HealthScore": health_score,
            "ExpansionScore": expansion_score,
            "RenewalRiskScore": renewal_risk_score,
            "OperatingGapScore": round(operating_gap_score, 1),
            "TotalWonARR": round(total_won, 2),
            "WonARR_FY25": round(won_25, 2),
            "WonARR_FY26": round(won_26, 2),
            "OpenPipelineARR": round(open_pipeline, 2),
            "ExpandPipelineARR": round(expand_pipeline, 2),
            "RenewalPipelineARR": round(renewal_pipeline, 2),
            "ExpandWonARR": round(expand_won, 2),
            "RenewalWonARR": round(renewal_won, 2),
            "TotalLostARR": round(safe_float(opp_metrics.get("TotalLostARR")), 2),
            "ActiveContracts": int(safe_float(contract_metrics.get("ActiveContracts"))),
            "TotalContracts": int(safe_float(contract_metrics.get("TotalContracts"))),
            "ExpiringContracts90d": int(
                safe_float(contract_metrics.get("ExpiringContracts90d"))
            ),
            "ExpiringContracts180d": int(
                safe_float(contract_metrics.get("ExpiringContracts180d"))
            ),
            "AvgTermMonths": round(avg_term_months, 1),
            "MultiYearCount": int(safe_float(contract_metrics.get("MultiYearCount"))),
            "ContactCount": contact_count,
            "CLevelContacts": c_level_contacts,
            "RecentActivityCount": recent_activity_count,
            "AuM": aum,
            "Employees": employees,
            "NRRProxy": nrr_proxy,
            "GRRProxy": grr_proxy,
            "LandToExpandDays": land_to_expand_days,
            "RenewalRiskARR": round(renewal_risk_arr, 2),
            "CustomerCount": 1,
            "HealthScoreTotal": health_score,
            "DataQualityScoreTotal": round(data_quality_score, 1),
            "KycGapCount": kyc_gap,
            "DataQualityGapCount": data_quality_gap,
            "UnderEngagedCount": under_engaged,
            "AtRiskAccountCount": at_risk_account,
            "RenewalRiskAccountCount": 1 if renewal_risk_arr > 0 else 0,
            "RegressionForecastARR": 0.0,
            "RegressionUpperARR": 0.0,
            "RegressionLowerARR": 0.0,
            "TargetValue": 0.0,
            "ActualARR": 0.0,
            "OpenExpansionARR": 0.0,
            "ExpiringAccounts": 0,
            "RenewalPipelineTrendARR": 0.0,
        }

        detail_rows.append(row)
        account_lookup[account_id] = row

    revenue_grouped: dict[tuple[str, str, str], dict[str, dict[str, float]]] = (
        defaultdict(
            lambda: defaultdict(
                lambda: {
                    "ActualARR": 0.0,
                    "OpenExpansionARR": 0.0,
                    "RenewalRiskARR": 0.0,
                }
            )
        )
    )
    for opp in opps:
        account_id = opp.get("AccountId")
        detail = account_lookup.get(account_id)
        if not detail:
            continue

        close_date = (opp.get("CloseDate") or "")[:10]
        close_month = month_key(close_date)
        if (
            not close_month
            or close_month < recent_start_month
            or close_month > forecast_end_month
        ):
            continue

        key = (
            str(detail["UnitGroup"]),
            str(detail["Segment"]),
            str(detail["HealthBand"]),
        )
        bucket = revenue_grouped[key][close_month]
        arr = safe_float(opp.get("ConvertedARR"))
        probability = safe_float(opp.get("Probability"))
        weight = (
            1.0
            if str(opp.get("IsWon") or "").lower() == "true"
            else max(0.05, min(1.0, probability / 100 if probability > 0 else 0.2))
        )
        weighted_arr = round(arr * weight, 2)
        opp_type = (opp.get("Type") or "").strip()
        is_closed = str(opp.get("IsClosed") or "").lower() == "true"
        is_won = str(opp.get("IsWon") or "").lower() == "true"

        if is_won:
            bucket["ActualARR"] += arr
        elif not is_closed and opp_type == "Expand":
            bucket["OpenExpansionARR"] += weighted_arr
        elif (
            not is_closed
            and opp_type == "Renewal"
            and safe_float(detail["RenewalRiskScore"]) >= 65
        ):
            bucket["RenewalRiskARR"] += (
                weighted_arr
                if weighted_arr > 0
                else safe_float(detail["RenewalRiskARR"])
            )

    portfolio_grouped: dict[tuple[str, str, str], dict[str, dict[str, float]]] = (
        defaultdict(
            lambda: defaultdict(
                lambda: {
                    "CustomerCount": 0.0,
                    "HealthScoreTotal": 0.0,
                    "DataQualityScoreTotal": 0.0,
                    "KycGapCount": 0.0,
                    "UnderEngagedCount": 0.0,
                    "AtRiskAccountCount": 0.0,
                }
            )
        )
    )
    for detail in detail_rows:
        created_month = month_key(str(detail["CreatedDate"]))
        if (
            not created_month
            or created_month < recent_start_month
            or created_month > current_month
        ):
            continue
        key = (
            str(detail["UnitGroup"]),
            str(detail["Segment"]),
            str(detail["HealthBand"]),
        )
        bucket = portfolio_grouped[key][created_month]
        bucket["CustomerCount"] += 1.0
        bucket["HealthScoreTotal"] += safe_float(detail["HealthScore"])
        bucket["DataQualityScoreTotal"] += safe_float(detail["DataQualityScore"])
        bucket["KycGapCount"] += safe_float(detail["KycGapCount"])
        bucket["UnderEngagedCount"] += safe_float(detail["UnderEngagedCount"])
        bucket["AtRiskAccountCount"] += safe_float(detail["AtRiskAccountCount"])

    renewal_grouped: dict[tuple[str, str, str], dict[str, dict[str, float]]] = (
        defaultdict(
            lambda: defaultdict(
                lambda: {
                    "ExpiringAccounts": 0.0,
                    "RenewalRiskAccountCount": 0.0,
                    "RenewalRiskARR": 0.0,
                    "RenewalPipelineTrendARR": 0.0,
                }
            )
        )
    )
    for detail in detail_rows:
        renewal_month = month_key(str(detail["RenewalDate"]))
        if (
            not renewal_month
            or renewal_month < current_month
            or renewal_month > renewal_end_month
        ):
            continue
        key = (
            str(detail["UnitGroup"]),
            str(detail["Segment"]),
            str(detail["HealthBand"]),
        )
        bucket = renewal_grouped[key][renewal_month]
        if safe_float(detail["ActiveContracts"]) > 0:
            bucket["ExpiringAccounts"] += 1.0
        bucket["RenewalRiskAccountCount"] += safe_float(
            detail["RenewalRiskAccountCount"]
        )
        bucket["RenewalRiskARR"] += safe_float(detail["RenewalRiskARR"])
        bucket["RenewalPipelineTrendARR"] += safe_float(detail["RenewalPipelineARR"])

    revenue_trend_rows: list[dict[str, object]] = []
    revenue_months = month_sequence(recent_start_month, forecast_end_month)
    historical_revenue_months = [
        month for month in revenue_months if month <= current_month
    ]
    for (unit_group, segment, health_band), monthly_data in revenue_grouped.items():
        actual_series = [
            monthly_data[month]["ActualARR"] for month in historical_revenue_months
        ]
        fit = least_squares(actual_series)
        for index, month in enumerate(revenue_months):
            values = monthly_data[month]
            forecast = max(0.0, fit["intercept"] + fit["slope"] * index)
            interval = prediction_interval(fit, index)
            revenue_trend_rows.append(
                {
                    "RecordType": "revenue_trend",
                    "AccountId": "",
                    "AccountName": "",
                    "OwnerName": "",
                    "UnitGroup": unit_group,
                    "Segment": segment,
                    "HealthBand": health_band,
                    "Industry": "",
                    "BillingCountry": "",
                    "AccountType": "",
                    "RiskLevel": "",
                    "KYCStatus": "",
                    "PartnerLevel": "",
                    "LifecycleStage": "",
                    "DataQualityBand": "",
                    "ExpansionBand": "",
                    "ContactTier": "",
                    "TermBucket": "",
                    "ProductCombo": "",
                    "CreatedDate": "",
                    "RenewalDate": "",
                    "MonthDate": f"{month}-01",
                    "MonthLabel": month,
                    "TrendCategory": "Revenue",
                    "IsSaaS": "false",
                    "IsAxioma": "false",
                    "HasDUNS": "false",
                    "HasUnitGroup": "false",
                    "HasAxiomaId": "false",
                    "DataQualityScore": 0.0,
                    "HealthScore": 0.0,
                    "ExpansionScore": 0.0,
                    "RenewalRiskScore": 0.0,
                    "OperatingGapScore": 0.0,
                    "TotalWonARR": 0.0,
                    "WonARR_FY25": 0.0,
                    "WonARR_FY26": 0.0,
                    "OpenPipelineARR": 0.0,
                    "ExpandPipelineARR": 0.0,
                    "RenewalPipelineARR": 0.0,
                    "ExpandWonARR": 0.0,
                    "RenewalWonARR": 0.0,
                    "TotalLostARR": 0.0,
                    "ActiveContracts": 0,
                    "TotalContracts": 0,
                    "ExpiringContracts90d": 0,
                    "ExpiringContracts180d": 0,
                    "AvgTermMonths": 0.0,
                    "MultiYearCount": 0,
                    "ContactCount": 0,
                    "CLevelContacts": 0,
                    "RecentActivityCount": 0,
                    "AuM": 0.0,
                    "Employees": 0,
                    "NRRProxy": 0.0,
                    "GRRProxy": 0.0,
                    "LandToExpandDays": 0,
                    "RenewalRiskARR": round(values["RenewalRiskARR"], 2),
                    "CustomerCount": 0,
                    "HealthScoreTotal": 0.0,
                    "DataQualityScoreTotal": 0.0,
                    "KycGapCount": 0,
                    "DataQualityGapCount": 0,
                    "UnderEngagedCount": 0,
                    "AtRiskAccountCount": 0,
                    "RenewalRiskAccountCount": 0,
                    "RegressionForecastARR": round(forecast, 2),
                    "RegressionUpperARR": round(max(0.0, forecast + interval), 2),
                    "RegressionLowerARR": round(max(0.0, forecast - interval), 2),
                    "TargetValue": 0.0,
                    "ActualARR": round(values["ActualARR"], 2),
                    "OpenExpansionARR": round(values["OpenExpansionARR"], 2),
                    "ExpiringAccounts": 0,
                    "RenewalPipelineTrendARR": 0.0,
                }
            )

    executive_forecast_rows: list[dict[str, object]] = []
    executive_months = month_sequence(current_year_start_month, current_year_end_month)
    fit_months = month_sequence(recent_start_month, last_complete_month)
    segment_keys = {
        (str(row["UnitGroup"]), str(row["Segment"]), str(row["HealthBand"]))
        for row in detail_rows
        if row.get("UnitGroup") or row.get("Segment") or row.get("HealthBand")
    }
    for unit_group, segment, health_band in sorted(segment_keys):
        monthly_data = revenue_grouped[(unit_group, segment, health_band)]
        history_values = [monthly_data[month]["ActualARR"] for month in fit_months]
        current_year_actuals = [
            monthly_data[month]["ActualARR"]
            for month in executive_months
            if month <= last_complete_month
        ]
        if not any(history_values) and not any(current_year_actuals):
            continue

        fit = least_squares(history_values)
        running_actual = 0.0
        running_forecast = 0.0
        forecast_variance = 0.0
        future_offset = 0
        for month in executive_months:
            if month <= last_complete_month:
                running_actual = round(
                    running_actual + monthly_data[month]["ActualARR"], 2
                )
                running_forecast = running_actual
                chart_value = running_actual
                band_high = running_actual
                band_low = running_actual
            else:
                x_idx = len(fit_months) + future_offset
                monthly_forecast = max(0.0, fit["intercept"] + fit["slope"] * x_idx)
                monthly_interval = prediction_interval(fit, x_idx)
                running_forecast = round(running_forecast + monthly_forecast, 2)
                forecast_variance += monthly_interval * monthly_interval
                cumulative_interval = round(forecast_variance**0.5, 2)
                chart_value = running_forecast
                band_high = round(chart_value + cumulative_interval, 2)
                band_low = round(max(0.0, chart_value - cumulative_interval), 2)
                future_offset += 1

            executive_forecast_rows.append(
                {
                    "RecordType": "executive_forecast",
                    "AccountId": "",
                    "AccountName": "",
                    "OwnerName": "",
                    "UnitGroup": unit_group,
                    "Segment": segment,
                    "HealthBand": health_band,
                    "Industry": "",
                    "BillingCountry": "",
                    "AccountType": "",
                    "RiskLevel": "",
                    "KYCStatus": "",
                    "PartnerLevel": "",
                    "LifecycleStage": "",
                    "DataQualityBand": "",
                    "ExpansionBand": "",
                    "ContactTier": "",
                    "TermBucket": "",
                    "ProductCombo": "",
                    "CreatedDate": "",
                    "RenewalDate": "",
                    "MonthDate": f"{month}-01",
                    "MonthLabel": month,
                    "TrendCategory": "Executive Forecast",
                    "IsSaaS": "false",
                    "IsAxioma": "false",
                    "HasDUNS": "false",
                    "HasUnitGroup": "false",
                    "HasAxiomaId": "false",
                    "DataQualityScore": 0.0,
                    "HealthScore": 0.0,
                    "ExpansionScore": 0.0,
                    "RenewalRiskScore": 0.0,
                    "OperatingGapScore": 0.0,
                    "TotalWonARR": 0.0,
                    "WonARR_FY25": 0.0,
                    "WonARR_FY26": 0.0,
                    "OpenPipelineARR": 0.0,
                    "ExpandPipelineARR": 0.0,
                    "RenewalPipelineARR": 0.0,
                    "ExpandWonARR": 0.0,
                    "RenewalWonARR": 0.0,
                    "TotalLostARR": 0.0,
                    "ActiveContracts": 0,
                    "TotalContracts": 0,
                    "ExpiringContracts90d": 0,
                    "ExpiringContracts180d": 0,
                    "AvgTermMonths": 0.0,
                    "MultiYearCount": 0,
                    "ContactCount": 0,
                    "CLevelContacts": 0,
                    "RecentActivityCount": 0,
                    "AuM": 0.0,
                    "Employees": 0,
                    "NRRProxy": 0.0,
                    "GRRProxy": 0.0,
                    "LandToExpandDays": 0,
                    "RenewalRiskARR": 0.0,
                    "CustomerCount": 0,
                    "HealthScoreTotal": 0.0,
                    "DataQualityScoreTotal": 0.0,
                    "KycGapCount": 0,
                    "DataQualityGapCount": 0,
                    "UnderEngagedCount": 0,
                    "AtRiskAccountCount": 0,
                    "RenewalRiskAccountCount": 0,
                    "RegressionForecastARR": 0.0,
                    "RegressionUpperARR": 0.0,
                    "RegressionLowerARR": 0.0,
                    "TargetValue": 0.0,
                    "ActualARR": 0.0,
                    "OpenExpansionARR": 0.0,
                    "ExpiringAccounts": 0,
                    "RenewalPipelineTrendARR": 0.0,
                    "ExecutiveForecastARR": round(chart_value, 2),
                    "ExecutiveForecastARR_high_95": round(band_high, 2),
                    "ExecutiveForecastARR_low_95": round(band_low, 2),
                }
            )

    portfolio_trend_rows: list[dict[str, object]] = []
    portfolio_months = month_sequence(recent_start_month, current_month)
    for (unit_group, segment, health_band), monthly_data in portfolio_grouped.items():
        for month in portfolio_months:
            values = monthly_data[month]
            portfolio_trend_rows.append(
                {
                    "RecordType": "portfolio_trend",
                    "AccountId": "",
                    "AccountName": "",
                    "OwnerName": "",
                    "UnitGroup": unit_group,
                    "Segment": segment,
                    "HealthBand": health_band,
                    "Industry": "",
                    "BillingCountry": "",
                    "AccountType": "",
                    "RiskLevel": "",
                    "KYCStatus": "",
                    "PartnerLevel": "",
                    "LifecycleStage": "",
                    "DataQualityBand": "",
                    "ExpansionBand": "",
                    "ContactTier": "",
                    "TermBucket": "",
                    "ProductCombo": "",
                    "CreatedDate": "",
                    "RenewalDate": "",
                    "MonthDate": f"{month}-01",
                    "MonthLabel": month,
                    "TrendCategory": "Portfolio",
                    "IsSaaS": "false",
                    "IsAxioma": "false",
                    "HasDUNS": "false",
                    "HasUnitGroup": "false",
                    "HasAxiomaId": "false",
                    "DataQualityScore": 0.0,
                    "HealthScore": 0.0,
                    "ExpansionScore": 0.0,
                    "RenewalRiskScore": 0.0,
                    "OperatingGapScore": 0.0,
                    "TotalWonARR": 0.0,
                    "WonARR_FY25": 0.0,
                    "WonARR_FY26": 0.0,
                    "OpenPipelineARR": 0.0,
                    "ExpandPipelineARR": 0.0,
                    "RenewalPipelineARR": 0.0,
                    "ExpandWonARR": 0.0,
                    "RenewalWonARR": 0.0,
                    "TotalLostARR": 0.0,
                    "ActiveContracts": 0,
                    "TotalContracts": 0,
                    "ExpiringContracts90d": 0,
                    "ExpiringContracts180d": 0,
                    "AvgTermMonths": 0.0,
                    "MultiYearCount": 0,
                    "ContactCount": 0,
                    "CLevelContacts": 0,
                    "RecentActivityCount": 0,
                    "AuM": 0.0,
                    "Employees": 0,
                    "NRRProxy": 0.0,
                    "GRRProxy": 0.0,
                    "LandToExpandDays": 0,
                    "RenewalRiskARR": 0.0,
                    "CustomerCount": int(values["CustomerCount"]),
                    "HealthScoreTotal": round(values["HealthScoreTotal"], 2),
                    "DataQualityScoreTotal": round(values["DataQualityScoreTotal"], 2),
                    "KycGapCount": int(values["KycGapCount"]),
                    "DataQualityGapCount": 0,
                    "UnderEngagedCount": int(values["UnderEngagedCount"]),
                    "AtRiskAccountCount": int(values["AtRiskAccountCount"]),
                    "RenewalRiskAccountCount": 0,
                    "RegressionForecastARR": 0.0,
                    "RegressionUpperARR": 0.0,
                    "RegressionLowerARR": 0.0,
                    "TargetValue": 0.0,
                    "ActualARR": 0.0,
                    "OpenExpansionARR": 0.0,
                    "ExpiringAccounts": 0,
                    "RenewalPipelineTrendARR": 0.0,
                }
            )

    renewal_trend_rows: list[dict[str, object]] = []
    renewal_months = month_sequence(current_month, renewal_end_month)
    for (unit_group, segment, health_band), monthly_data in renewal_grouped.items():
        for month in renewal_months:
            values = monthly_data[month]
            renewal_trend_rows.append(
                {
                    "RecordType": "renewal_trend",
                    "AccountId": "",
                    "AccountName": "",
                    "OwnerName": "",
                    "UnitGroup": unit_group,
                    "Segment": segment,
                    "HealthBand": health_band,
                    "Industry": "",
                    "BillingCountry": "",
                    "AccountType": "",
                    "RiskLevel": "",
                    "KYCStatus": "",
                    "PartnerLevel": "",
                    "LifecycleStage": "",
                    "DataQualityBand": "",
                    "ExpansionBand": "",
                    "ContactTier": "",
                    "TermBucket": "",
                    "ProductCombo": "",
                    "CreatedDate": "",
                    "RenewalDate": "",
                    "MonthDate": f"{month}-01",
                    "MonthLabel": month,
                    "TrendCategory": "Renewal",
                    "IsSaaS": "false",
                    "IsAxioma": "false",
                    "HasDUNS": "false",
                    "HasUnitGroup": "false",
                    "HasAxiomaId": "false",
                    "DataQualityScore": 0.0,
                    "HealthScore": 0.0,
                    "ExpansionScore": 0.0,
                    "RenewalRiskScore": 0.0,
                    "OperatingGapScore": 0.0,
                    "TotalWonARR": 0.0,
                    "WonARR_FY25": 0.0,
                    "WonARR_FY26": 0.0,
                    "OpenPipelineARR": 0.0,
                    "ExpandPipelineARR": 0.0,
                    "RenewalPipelineARR": 0.0,
                    "ExpandWonARR": 0.0,
                    "RenewalWonARR": 0.0,
                    "TotalLostARR": 0.0,
                    "ActiveContracts": 0,
                    "TotalContracts": 0,
                    "ExpiringContracts90d": 0,
                    "ExpiringContracts180d": 0,
                    "AvgTermMonths": 0.0,
                    "MultiYearCount": 0,
                    "ContactCount": 0,
                    "CLevelContacts": 0,
                    "RecentActivityCount": 0,
                    "AuM": 0.0,
                    "Employees": 0,
                    "NRRProxy": 0.0,
                    "GRRProxy": 0.0,
                    "LandToExpandDays": 0,
                    "RenewalRiskARR": round(values["RenewalRiskARR"], 2),
                    "CustomerCount": 0,
                    "HealthScoreTotal": 0.0,
                    "DataQualityScoreTotal": 0.0,
                    "KycGapCount": 0,
                    "DataQualityGapCount": 0,
                    "UnderEngagedCount": 0,
                    "AtRiskAccountCount": 0,
                    "RenewalRiskAccountCount": int(values["RenewalRiskAccountCount"]),
                    "RegressionForecastARR": 0.0,
                    "RegressionUpperARR": 0.0,
                    "RegressionLowerARR": 0.0,
                    "TargetValue": 0.0,
                    "ActualARR": 0.0,
                    "OpenExpansionARR": 0.0,
                    "ExpiringAccounts": int(values["ExpiringAccounts"]),
                    "RenewalPipelineTrendARR": round(
                        values["RenewalPipelineTrendARR"], 2
                    ),
                }
            )

    rows = (
        detail_rows
        + revenue_trend_rows
        + executive_forecast_rows
        + portfolio_trend_rows
        + renewal_trend_rows
    )
    logger.info("  Detail rows: %d", len(detail_rows))
    logger.info("  Revenue trend rows: %d", len(revenue_trend_rows))
    logger.info("  Executive forecast rows: %d", len(executive_forecast_rows))
    logger.info("  Portfolio trend rows: %d", len(portfolio_trend_rows))
    logger.info("  Renewal trend rows: %d", len(renewal_trend_rows))
    logger.info("  Total rows: %d", len(rows))

    field_names = [
        "RecordType",
        "AccountId",
        "AccountName",
        "OwnerName",
        "UnitGroup",
        "Segment",
        "HealthBand",
        "Industry",
        "BillingCountry",
        "AccountType",
        "RiskLevel",
        "KYCStatus",
        "PartnerLevel",
        "LifecycleStage",
        "DataQualityBand",
        "ExpansionBand",
        "ContactTier",
        "TermBucket",
        "ProductCombo",
        "CreatedDate",
        "RenewalDate",
        "MonthDate",
        "MonthLabel",
        "TrendCategory",
        "IsSaaS",
        "IsAxioma",
        "HasDUNS",
        "HasUnitGroup",
        "HasAxiomaId",
        "DataQualityScore",
        "HealthScore",
        "ExpansionScore",
        "RenewalRiskScore",
        "OperatingGapScore",
        "TotalWonARR",
        "WonARR_FY25",
        "WonARR_FY26",
        "OpenPipelineARR",
        "ExpandPipelineARR",
        "RenewalPipelineARR",
        "ExpandWonARR",
        "RenewalWonARR",
        "TotalLostARR",
        "ActiveContracts",
        "TotalContracts",
        "ExpiringContracts90d",
        "ExpiringContracts180d",
        "AvgTermMonths",
        "MultiYearCount",
        "ContactCount",
        "CLevelContacts",
        "RecentActivityCount",
        "AuM",
        "Employees",
        "NRRProxy",
        "GRRProxy",
        "LandToExpandDays",
        "RenewalRiskARR",
        "CustomerCount",
        "HealthScoreTotal",
        "DataQualityScoreTotal",
        "KycGapCount",
        "DataQualityGapCount",
        "UnderEngagedCount",
        "AtRiskAccountCount",
        "RenewalRiskAccountCount",
        "RegressionForecastARR",
        "RegressionUpperARR",
        "RegressionLowerARR",
        "TargetValue",
        "ActualARR",
        "OpenExpansionARR",
        "ExpiringAccounts",
        "RenewalPipelineTrendARR",
        "ExecutiveForecastARR",
        "ExecutiveForecastARR_high_95",
        "ExecutiveForecastARR_low_95",
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
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account"),
        _dim("OwnerName", "Owner"),
        _dim("UnitGroup", "Unit Group"),
        _dim("Segment", "Segment"),
        _dim("HealthBand", "Health Band"),
        _dim("Industry", "Industry"),
        _dim("BillingCountry", "Country"),
        _dim("AccountType", "Account Type"),
        _dim("RiskLevel", "Risk Level"),
        _dim("KYCStatus", "KYC Status"),
        _dim("PartnerLevel", "Partner Level"),
        _dim("LifecycleStage", "Lifecycle Stage"),
        _dim("DataQualityBand", "Data Quality Band"),
        _dim("ExpansionBand", "Expansion Band"),
        _dim("ContactTier", "Contact Tier"),
        _dim("TermBucket", "Term Bucket"),
        _dim("ProductCombo", "Product Combination"),
        _date("CreatedDate", "Created Date"),
        _date("RenewalDate", "Renewal Date"),
        _date("MonthDate", "Month"),
        _dim("MonthLabel", "Month Label"),
        _dim("TrendCategory", "Trend Category"),
        _dim("IsSaaS", "SaaS Client"),
        _dim("IsAxioma", "Axioma Client"),
        _dim("HasDUNS", "Has DUNS"),
        _dim("HasUnitGroup", "Has Unit Group"),
        _dim("HasAxiomaId", "Has Axioma ID"),
        _measure("DataQualityScore", "Data Quality Score", scale=1, precision=5),
        _measure("HealthScore", "Health Score", scale=1, precision=5),
        _measure("ExpansionScore", "Expansion Score", scale=1, precision=5),
        _measure("RenewalRiskScore", "Renewal Risk Score", scale=1, precision=5),
        _measure("OperatingGapScore", "Operating Gap Score", scale=1, precision=5),
        _measure("TotalWonARR", "Customer ARR"),
        _measure("WonARR_FY25", "Won ARR FY25"),
        _measure("WonARR_FY26", "Won ARR FY26"),
        _measure("OpenPipelineARR", "Open Pipeline ARR"),
        _measure("ExpandPipelineARR", "Expand Pipeline ARR"),
        _measure("RenewalPipelineARR", "Renewal Pipeline ARR"),
        _measure("ExpandWonARR", "Expand Won ARR"),
        _measure("RenewalWonARR", "Renewal Won ARR"),
        _measure("TotalLostARR", "Total Lost ARR"),
        _measure("ActiveContracts", "Active Contracts", scale=0, precision=6),
        _measure("TotalContracts", "Total Contracts", scale=0, precision=6),
        _measure(
            "ExpiringContracts90d", "Expiring Contracts 90d", scale=0, precision=6
        ),
        _measure(
            "ExpiringContracts180d", "Expiring Contracts 180d", scale=0, precision=6
        ),
        _measure("AvgTermMonths", "Average Term Months", scale=1, precision=5),
        _measure("MultiYearCount", "Multi-Year Contracts", scale=0, precision=6),
        _measure("ContactCount", "Contact Count", scale=0, precision=6),
        _measure("CLevelContacts", "C-Level Contacts", scale=0, precision=6),
        _measure("RecentActivityCount", "Recent Activity Count", scale=0, precision=6),
        _measure("AuM", "AuM (M)", scale=2, precision=10),
        _measure("Employees", "Employees", scale=0, precision=10),
        _measure("NRRProxy", "NRR Proxy", scale=1, precision=6),
        _measure("GRRProxy", "GRR Proxy", scale=1, precision=6),
        _measure("LandToExpandDays", "Land To Expand Days", scale=0, precision=6),
        _measure("RenewalRiskARR", "Renewal Risk ARR"),
        _measure("CustomerCount", "Customer Count", scale=0, precision=6),
        _measure("HealthScoreTotal", "Health Score Total", scale=1, precision=8),
        _measure(
            "DataQualityScoreTotal", "Data Quality Score Total", scale=1, precision=8
        ),
        _measure("KycGapCount", "KYC Gap Count", scale=0, precision=6),
        _measure("DataQualityGapCount", "Data Quality Gap Count", scale=0, precision=6),
        _measure("UnderEngagedCount", "Under-Engaged Count", scale=0, precision=6),
        _measure("AtRiskAccountCount", "At-Risk Account Count", scale=0, precision=6),
        _measure(
            "RenewalRiskAccountCount",
            "Renewal Risk Account Count",
            scale=0,
            precision=6,
        ),
        _measure("RegressionForecastARR", "Regression Forecast ARR"),
        _measure("RegressionUpperARR", "Regression Upper ARR"),
        _measure("RegressionLowerARR", "Regression Lower ARR"),
        _measure("TargetValue", "Target Value"),
        _measure("ActualARR", "Actual ARR"),
        _measure("OpenExpansionARR", "Open Expansion ARR"),
        _measure("ExpiringAccounts", "Expiring Accounts", scale=0, precision=6),
        _measure("RenewalPipelineTrendARR", "Renewal Pipeline Trend ARR"),
        _measure("ExecutiveForecastARR", "Executive Forecast ARR"),
        _measure("ExecutiveForecastARR_high_95", "Executive Forecast ARR High 95"),
        _measure("ExecutiveForecastARR_low_95", "Executive Forecast ARR Low 95"),
    ]

    ok = upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)
    return ok, len(rows)


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_segment = coalesce_filter("f_segment", "Segment")
    filter_health = coalesce_filter("f_health", "HealthBand")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_segment
        + filter_health
    )
    revenue_trend = (
        load
        + 'q = filter q by RecordType == "revenue_trend";\n'
        + filter_unit
        + filter_segment
        + filter_health
    )
    portfolio_trend = (
        load
        + 'q = filter q by RecordType == "portfolio_trend";\n'
        + filter_unit
        + filter_segment
        + filter_health
    )
    renewal_trend = (
        load
        + 'q = filter q by RecordType == "renewal_trend";\n'
        + filter_unit
        + filter_segment
        + filter_health
    )

    return {
        "f_unit": af("UnitGroup", ds_meta),
        "f_segment": af("Segment", ds_meta),
        "f_health": af("HealthBand", ds_meta),
        "s_summary": sq(
            detail
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(TotalWonARR) as customer_arr, "
            + "sum(ExpandPipelineARR) as expansion_pipe_arr, "
            + "sum(RenewalRiskARR) as renewal_risk_arr, "
            + "avg(HealthScore) as avg_health, "
            + "70 as target, "
            + "80 as good, "
            + "60 as satisfactory;"
        ),
        "s_revenue_trajectory": sq(
            revenue_trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ActualARR) as ActualARR, "
            + "sum(OpenExpansionARR) as OpenExpansionARR, "
            + "sum(RenewalRiskARR) as RenewalRiskARR, "
            + "sum(RegressionForecastARR) as RegressionForecastARR, "
            + "sum(RegressionUpperARR) as RegressionUpperARR, "
            + "sum(RegressionLowerARR) as RegressionLowerARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_segment_mix": sq(
            detail
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, "
            + "sum(TotalWonARR) as CustomerARR, "
            + "sum(ExpandPipelineARR) as ExpansionPipelineARR;\n"
            + "q = order q by CustomerARR desc;"
        ),
        "s_new_customer_trend": sq(
            portfolio_trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(CustomerCount) as CustomerCount, "
            + "sum(UnderEngagedCount) as UnderEngagedCount;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_score_trend": sq(
            portfolio_trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "case when sum(CustomerCount) > 0 then sum(HealthScoreTotal) / sum(CustomerCount) else 0 end as AvgHealthScore, "
            + "case when sum(CustomerCount) > 0 then sum(DataQualityScoreTotal) / sum(CustomerCount) else 0 end as AvgDataQualityScore, "
            + "sum(KycGapCount) as KycGapCount;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_renewal_outlook": sq(
            renewal_trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ExpiringAccounts) as ExpiringAccounts, "
            + "sum(RenewalRiskAccountCount) as RenewalRiskAccounts, "
            + "sum(RenewalRiskARR) as RenewalRiskARR, "
            + "sum(RenewalPipelineTrendARR) as RenewalPipelineARR;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_retention_cohort": sq(
            detail
            + 'q = filter q by CreatedDate != "";\n'
            + "q = group q by substr(CreatedDate, 1, 4);\n"
            + "q = foreach q generate substr(CreatedDate, 1, 4) as CohortYear, "
            + "avg(NRRProxy) as NRRProxy, "
            + "avg(GRRProxy) as GRRProxy, "
            + "count() as CustomerCount;\n"
            + "q = order q by CohortYear asc;"
        ),
        "s_kyc_by_unit": sq(
            detail
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + 'sum(case when KYCStatus == "Approved" then 1 else 0 end) as Approved, '
            + 'sum(case when KYCStatus == "Not Started" then 1 else 0 end) as NotStarted, '
            + 'sum(case when KYCStatus == "Approval Requested" then 1 else 0 end) as ApprovalRequested, '
            + 'sum(case when KYCStatus == "On Hold" then 1 else 0 end) as OnHold;\n'
            + "q = order q by (NotStarted + ApprovalRequested + OnHold) desc;"
        ),
        "s_product_coverage": sq(
            detail
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + 'sum(case when IsSaaS == "true" then 1 else 0 end) as SaaSAccounts, '
            + 'sum(case when IsAxioma == "true" then 1 else 0 end) as AxiomaAccounts, '
            + 'sum(case when ProductCombo != "" then 1 else 0 end) as MultiProductAccounts;\n'
            + "q = order q by (SaaSAccounts + AxiomaAccounts) desc;"
        ),
        "s_scores_by_segment": sq(
            detail
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, "
            + "avg(HealthScore) as HealthScore, "
            + "avg(DataQualityScore) as DataQualityScore, "
            + "avg(ExpansionScore) as ExpansionScore;\n"
            + "q = order q by HealthScore desc;"
        ),
        "s_arr_by_industry": sq(
            detail
            + "q = group q by Industry;\n"
            + "q = foreach q generate Industry, "
            + "sum(TotalWonARR) as CustomerARR, "
            + "sum(RenewalRiskARR) as RenewalRiskARR;\n"
            + "q = order q by CustomerARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_exception_summary": sq(
            detail
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(AtRiskAccountCount) as AtRiskAccounts, "
            + "(sum(KycGapCount) + sum(DataQualityGapCount)) as HygieneGaps, "
            + "sum(UnderEngagedCount) as UnderEngagedAccounts, "
            + "sum(RenewalRiskARR) as RenewalRiskARR;"
        ),
        "s_risk_by_term": sq(
            detail
            + "q = group q by TermBucket;\n"
            + "q = foreach q generate TermBucket, "
            + "sum(TotalWonARR) as CustomerARR, "
            + "sum(RenewalRiskARR) as RenewalRiskARR;\n"
            + "q = order q by RenewalRiskARR desc;"
        ),
        "s_top_at_risk": sq(
            detail
            + "q = filter q by AtRiskAccountCount > 0;\n"
            + "q = foreach q generate AccountName, OwnerName, Segment, HealthBand, "
            + "RenewalRiskARR, RenewalRiskScore, HealthScore, ExpiringContracts90d, AccountId;\n"
            + "q = order q by RenewalRiskScore desc;\n"
            + "q = limit q 15;"
        ),
        "s_top_gaps": sq(
            detail
            + "q = filter q by KycGapCount > 0 || DataQualityGapCount > 0 || UnderEngagedCount > 0;\n"
            + "q = foreach q generate AccountName, OwnerName, Segment, KYCStatus, DataQualityBand, "
            + "OperatingGapScore, ContactCount, RecentActivityCount, AccountId;\n"
            + "q = order q by OperatingGapScore desc;\n"
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
            "Customer & Account Health",
            "Manager operating view for customer value, renewal pressure, and account hygiene.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_segment": pillbox("f_segment", "Segment"),
        "p1_f_health": pillbox("f_health", "Health"),
        "p1_n_arr": num(
            "s_summary", "customer_arr", "Customer ARR", "#032D60", compact=True
        ),
        "p1_n_expand": num(
            "s_summary",
            "expansion_pipe_arr",
            "Expansion Pipeline ARR",
            "#0176D3",
            compact=True,
        ),
        "p1_n_renewal": num(
            "s_summary", "renewal_risk_arr", "Renewal Risk ARR", "#8E030F", compact=True
        ),
        "p1_n_health": num(
            "s_summary", "avg_health", "Avg Health Score", "#2E844A", compact=True
        ),
        "p1_ch_timeline": timeline_chart(
            "s_revenue_trajectory",
            "Customer Revenue Trajectory",
            show_legend=True,
            axis_title="ARR (EUR)",
        ),
        "p1_ch_bullet": bullet_chart(
            "s_summary", "Avg Health Score vs Target", axis_title="Score"
        ),
        "p1_ch_segment": rich_chart(
            "s_segment_mix",
            "stackhbar",
            "Customer ARR vs Renewal Risk by Segment",
            ["Segment"],
            ["CustomerARR", "RenewalRiskARR"],
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
            "Customer additions, health quality, and renewal pressure over time.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_segment": pillbox("f_segment", "Segment"),
        "p2_f_health": pillbox("f_health", "Health"),
        "p2_ch_customers": combo_chart(
            "s_new_customer_trend",
            "New Customers vs Under-Engaged Accounts",
            ["MonthDate"],
            ["CustomerCount"],
            ["UnderEngagedCount"],
            show_legend=True,
            axis_title="Customers",
            axis2_title="Under-Engaged",
        ),
        "p2_ch_scores": rich_chart(
            "s_score_trend",
            "line",
            "Health and Data Quality Trend",
            ["MonthDate"],
            ["AvgHealthScore", "AvgDataQualityScore"],
            show_legend=True,
            axis_title="Score",
        ),
        "p2_ch_renewal": combo_chart(
            "s_renewal_outlook",
            "Renewal Outlook",
            ["MonthDate"],
            ["ExpiringAccounts", "RenewalRiskAccounts"],
            ["RenewalRiskARR"],
            show_legend=True,
            axis_title="Accounts",
            axis2_title="ARR (EUR)",
        ),
        "p2_ch_retention": rich_chart(
            "s_retention_cohort",
            "line",
            "NRR and GRR by Customer Cohort",
            ["CohortYear"],
            ["NRRProxy", "GRRProxy"],
            show_legend=True,
            axis_title="Retention %",
        ),
        "p3_nav1": nav_link("summary", "Summary"),
        "p3_nav2": nav_link("trend", "Trend & Forecast"),
        "p3_nav3": nav_link("drivers", "Drivers & Segments", active=True),
        "p3_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p3_hdr": hdr(
            "Drivers & Segments",
            "The customer cohorts, product patterns, and hygiene drivers behind performance.",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_segment": pillbox("f_segment", "Segment"),
        "p3_f_health": pillbox("f_health", "Health"),
        "p3_ch_kyc": rich_chart(
            "s_kyc_by_unit",
            "stackhbar",
            "KYC Backlog by Unit Group",
            ["UnitGroup"],
            ["Approved", "NotStarted", "ApprovalRequested", "OnHold"],
            show_legend=True,
            axis_title="Accounts",
            show_values=True,
        ),
        "p3_ch_products": rich_chart(
            "s_product_coverage",
            "stackhbar",
            "Product Coverage by Unit Group",
            ["UnitGroup"],
            ["SaaSAccounts", "AxiomaAccounts", "MultiProductAccounts"],
            show_legend=True,
            axis_title="Accounts",
            show_values=True,
        ),
        "p3_ch_scores": rich_chart(
            "s_scores_by_segment",
            "hbar",
            "Health, Data Quality, and Expansion by Segment",
            ["Segment"],
            ["HealthScore", "DataQualityScore", "ExpansionScore"],
            show_legend=True,
            axis_title="Score",
            show_values=True,
        ),
        "p3_ch_industry": rich_chart(
            "s_arr_by_industry",
            "stackhbar",
            "Customer ARR vs Renewal Risk by Industry",
            ["Industry"],
            ["CustomerARR", "RenewalRiskARR"],
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
            "Accounts that require intervention because of renewal pressure, weak health, or operating gaps.",
        ),
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_segment": pillbox("f_segment", "Segment"),
        "p4_f_health": pillbox("f_health", "Health"),
        "p4_n_risk": num(
            "s_exception_summary",
            "AtRiskAccounts",
            "At-Risk Accounts",
            "#8E030F",
            compact=True,
        ),
        "p4_n_gaps": num(
            "s_exception_summary",
            "HygieneGaps",
            "KYC / Data Quality Gaps",
            "#BA0517",
            compact=True,
        ),
        "p4_ch_term": rich_chart(
            "s_risk_by_term",
            "stackcolumn",
            "Renewal Risk by Term Bucket",
            ["TermBucket"],
            ["CustomerARR", "RenewalRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p4_tbl_risk": rich_chart(
            "s_top_at_risk",
            "comparisontable",
            "Top At-Risk Accounts",
            ["AccountName", "OwnerName", "Segment", "HealthBand"],
            [
                "RenewalRiskARR",
                "RenewalRiskScore",
                "HealthScore",
                "ExpiringContracts90d",
            ],
            show_legend=False,
        ),
        "p4_tbl_gaps": rich_chart(
            "s_top_gaps",
            "comparisontable",
            "Top Operating Gaps",
            ["AccountName", "OwnerName", "Segment", "KYCStatus", "DataQualityBand"],
            ["OperatingGapScore", "ContactCount", "RecentActivityCount"],
            show_legend=False,
        ),
    }

    widgets["p2_ch_customers"]["parameters"].pop("columnMap", None)
    widgets["p2_ch_renewal"]["parameters"].pop("columnMap", None)
    add_table_action(
        widgets["p4_tbl_risk"], "salesforceActions", "Account", "AccountId"
    )
    add_table_action(
        widgets["p4_tbl_gaps"], "salesforceActions", "Account", "AccountId"
    )
    return widgets


def legacy_build_layout() -> dict:
    """Build the 4-page manager dashboard layout."""
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_arr", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_expand", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_renewal", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_health", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p1_ch_timeline", "row": 9, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_bullet", "row": 17, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p1_ch_segment", "row": 17, "column": 4, "colspan": 8, "rowspan": 6},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_customers", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_scores", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_renewal", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_retention", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_ch_kyc", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_products", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_scores", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_industry", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p4_n_risk", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_gaps", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p4_ch_term", "row": 5, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p4_tbl_risk", "row": 11, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p4_tbl_gaps", "row": 18, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "CustomerAccountHealth",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("trend", "Trend & Forecast", p2),
            pg("drivers", "Drivers & Segments", p3),
            pg("exceptions", "Exceptions & Actions", p4),
        ],
    }


_base_build_steps = build_steps


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    steps = _base_build_steps(ds_id)
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_segment = coalesce_filter("f_segment", "Segment")
    filter_health = coalesce_filter("f_health", "HealthBand")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_segment
        + filter_health
    )
    portfolio_trend = (
        load
        + 'q = filter q by RecordType == "portfolio_trend";\n'
        + filter_unit
        + filter_segment
        + filter_health
    )

    steps.update(
        {
            "s_renewal_arr_outlook": sq(
                portfolio_trend
                + "q = group q by (MonthDate, MonthLabel);\n"
                + "q = foreach q generate MonthDate, MonthLabel, "
                + "sum(AtRiskAccountCount) as AtRiskAccountCount, "
                + "sum(UnderEngagedCount) as UnderEngagedCount, "
                + "sum(KycGapCount) as KycGapCount;\n"
                + "q = order q by MonthDate asc;"
            ),
            "s_health_scatter": sq(
                detail
                + "q = group q by (AccountName, Segment, HealthBand, AccountId);\n"
                + "q = foreach q generate AccountName, Segment, HealthBand, "
                + "max(HealthScore) as HealthScore, "
                + "max(ExpansionScore) as ExpansionScore, "
                + "max(TotalWonARR) as CustomerARR, "
                + "AccountId;\n"
                + "q = order q by CustomerARR desc;\n"
                + "q = limit q 25;"
            ),
            "s_health_heatmap": sq(
                detail
                + "q = group q by (UnitGroup, HealthBand);\n"
                + "q = foreach q generate UnitGroup, HealthBand, count() as AccountCount;\n"
                + "q = order q by AccountCount desc;"
            ),
            "s_health_segment_flow": sq(
                detail
                + "q = group q by (HealthBand, Segment);\n"
                + "q = foreach q generate HealthBand as source, Segment as target, count() as flow;\n"
                + "q = order q by flow desc;"
            ),
            "s_industry_treemap": sq(
                detail
                + "q = group q by (Industry, Segment);\n"
                + "q = foreach q generate Industry, Segment, sum(TotalWonARR) as CustomerARR;\n"
                + "q = order q by CustomerARR desc;\n"
                + "q = limit q 30;"
            ),
        }
    )
    steps["f_unit"] = af("UnitGroup", ds_meta)
    steps["f_segment"] = af("Segment", ds_meta)
    steps["f_health"] = af("HealthBand", ds_meta)
    return steps


def build_widgets() -> dict[str, dict]:
    """Build dashboard widgets."""
    widgets = {
        "p1_nav1": nav_link("summary", "Summary", active=True),
        "p1_nav2": nav_link("trend", "Trend & Forecast"),
        "p1_nav3": nav_link("drivers", "Drivers & Segments"),
        "p1_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p1_hdr": hdr(
            "Customer & Account Health",
            "Manager operating view for renewal exposure, customer health, and expansion headroom.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_segment": pillbox("f_segment", "Segment"),
        "p1_f_health": pillbox("f_health", "Health"),
        "p1_n_arr": num(
            "s_summary", "customer_arr", "Customer ARR", "#032D60", compact=True
        ),
        "p1_n_expand": num(
            "s_summary",
            "expansion_pipe_arr",
            "Expansion Pipeline ARR",
            "#0176D3",
            compact=True,
        ),
        "p1_n_renewal": num(
            "s_exception_summary",
            "AtRiskAccounts",
            "At-Risk Accounts",
            "#8E030F",
            compact=True,
        ),
        "p1_n_health": num(
            "s_summary", "avg_health", "Avg Health Score", "#2E844A", compact=True
        ),
        "p1_ch_renewal": rich_chart(
            "s_renewal_arr_outlook",
            "line",
            "Account Risk Outlook",
            ["MonthDate"],
            ["AtRiskAccountCount", "UnderEngagedCount", "KycGapCount"],
            show_legend=True,
            axis_title="Accounts",
        ),
        "p1_ch_segment": rich_chart(
            "s_segment_mix",
            "stackhbar",
            "Customer ARR vs Expansion Pipeline by Segment",
            ["Segment"],
            ["CustomerARR", "ExpansionPipelineARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p1_ch_heatmap": heatmap_chart(
            "s_health_heatmap",
            "Unit Group x Health Band Account Count",
            show_legend=True,
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("trend", "Trend & Forecast", active=True),
        "p2_nav3": nav_link("drivers", "Drivers & Segments"),
        "p2_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p2_hdr": hdr(
            "Trend & Forecast",
            "How customer count, health quality, and renewal pressure are moving over time.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_segment": pillbox("f_segment", "Segment"),
        "p2_f_health": pillbox("f_health", "Health"),
        "p2_ch_customers": combo_chart(
            "s_new_customer_trend",
            "New Customers vs Under-Engaged Accounts",
            ["MonthDate"],
            ["CustomerCount"],
            ["UnderEngagedCount"],
            show_legend=True,
            axis_title="Customers",
            axis2_title="Under-Engaged",
        ),
        "p2_ch_scores": rich_chart(
            "s_score_trend",
            "line",
            "Health and Data Quality Trend",
            ["MonthDate"],
            ["AvgHealthScore", "AvgDataQualityScore"],
            show_legend=True,
            axis_title="Score",
        ),
        "p2_ch_renewal_trend": rich_chart(
            "s_renewal_arr_outlook",
            "line",
            "Customer Risk Outlook",
            ["MonthDate"],
            ["AtRiskAccountCount", "UnderEngagedCount", "KycGapCount"],
            show_legend=True,
            axis_title="Accounts",
        ),
        "p2_ch_retention": rich_chart(
            "s_retention_cohort",
            "line",
            "NRR and GRR by Customer Cohort",
            ["CohortYear"],
            ["NRRProxy", "GRRProxy"],
            show_legend=True,
            axis_title="Retention %",
        ),
        "p3_nav1": nav_link("summary", "Summary"),
        "p3_nav2": nav_link("trend", "Trend & Forecast"),
        "p3_nav3": nav_link("drivers", "Drivers & Segments", active=True),
        "p3_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p3_hdr": hdr(
            "Drivers & Segments",
            "Where health, expansion potential, and risk are concentrated across the portfolio.",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_segment": pillbox("f_segment", "Segment"),
        "p3_f_health": pillbox("f_health", "Health"),
        "p3_ch_scatter": bubble_chart(
            "s_health_scatter",
            "Health Score vs Expansion Score",
            show_legend=False,
        ),
        "p3_ch_flow": sankey_chart(
            "s_health_segment_flow",
            "Customer Flow: Health Band -> Segment",
        ),
        "p3_ch_treemap": treemap_chart(
            "s_industry_treemap",
            "Customer ARR Composition by Industry and Segment",
            ["Industry", "Segment"],
            "CustomerARR",
            show_legend=False,
        ),
        "p3_ch_products": rich_chart(
            "s_product_coverage",
            "stackhbar",
            "Product Coverage by Unit Group",
            ["UnitGroup"],
            ["SaaSAccounts", "AxiomaAccounts", "MultiProductAccounts"],
            show_legend=True,
            axis_title="Accounts",
            show_values=True,
        ),
        "p4_nav1": nav_link("summary", "Summary"),
        "p4_nav2": nav_link("trend", "Trend & Forecast"),
        "p4_nav3": nav_link("drivers", "Drivers & Segments"),
        "p4_nav4": nav_link("exceptions", "Exceptions & Actions", active=True),
        "p4_hdr": hdr(
            "Exceptions & Actions",
            "Accounts that need intervention because of renewal pressure, weak health, or operating gaps.",
        ),
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_segment": pillbox("f_segment", "Segment"),
        "p4_f_health": pillbox("f_health", "Health"),
        "p4_n_risk": num(
            "s_exception_summary",
            "AtRiskAccounts",
            "At-Risk Accounts",
            "#8E030F",
            compact=True,
        ),
        "p4_n_gaps": num(
            "s_exception_summary",
            "HygieneGaps",
            "KYC / Data Quality Gaps",
            "#BA0517",
            compact=True,
        ),
        "p4_ch_term": rich_chart(
            "s_risk_by_term",
            "stackcolumn",
            "Renewal Risk by Term Bucket",
            ["TermBucket"],
            ["CustomerARR", "RenewalRiskARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p4_tbl_risk": rich_chart(
            "s_top_at_risk",
            "comparisontable",
            "Top At-Risk Accounts",
            ["AccountName", "OwnerName", "Segment", "HealthBand"],
            [
                "RenewalRiskARR",
                "RenewalRiskScore",
                "HealthScore",
                "ExpiringContracts90d",
            ],
            show_legend=False,
        ),
        "p4_tbl_gaps": rich_chart(
            "s_top_gaps",
            "comparisontable",
            "Top Operating Gaps",
            ["AccountName", "OwnerName", "Segment", "KYCStatus", "DataQualityBand"],
            ["OperatingGapScore", "ContactCount", "RecentActivityCount"],
            show_legend=False,
        ),
    }

    widgets["p2_ch_customers"]["parameters"].pop("columnMap", None)
    add_table_action(
        widgets["p4_tbl_risk"], "salesforceActions", "Account", "AccountId"
    )
    add_table_action(
        widgets["p4_tbl_gaps"], "salesforceActions", "Account", "AccountId"
    )
    return widgets


def build_layout() -> dict:
    """Build the 4-page manager dashboard layout."""
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_arr", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_expand", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_renewal", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_health", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p1_ch_renewal", "row": 9, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_segment", "row": 17, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p1_ch_heatmap", "row": 17, "column": 6, "colspan": 6, "rowspan": 6},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_customers", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_scores", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {
            "name": "p2_ch_renewal_trend",
            "row": 12,
            "column": 0,
            "colspan": 6,
            "rowspan": 7,
        },
        {"name": "p2_ch_retention", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_ch_scatter", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_flow", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_treemap", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_products", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_segment", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_health", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p4_n_risk", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_gaps", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p4_ch_term", "row": 5, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p4_tbl_risk", "row": 11, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p4_tbl_gaps", "row": 18, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "CustomerAccountHealth",
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
    with builder_run("Customer_Account_Health", __file__) as summary:
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

        dataset_id = get_dataset_id(inst, tok, DS)
        if not dataset_id:
            raise SystemExit(f"Could not resolve dataset id for {DS}")
        summary.dataset_id = dataset_id

        steps = build_steps(dataset_id)
        widgets = build_widgets()
        layout = build_layout()
        state = build_dashboard_state(steps, widgets, layout)

        dashboard_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
        logger.info("\n=== Deploying %s ===", DASHBOARD_LABEL)
        deploy_dashboard(inst, tok, dashboard_id, state)

        set_record_links_xmd(
            inst,
            tok,
            DS,
            [{"field": "AccountName", "id_field": "AccountId", "label": "Account"}],
        )


if __name__ == "__main__":
    main()
