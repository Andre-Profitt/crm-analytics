#!/usr/bin/env python3
"""Build the Contract Operations & Renewals dashboard.

This is the Wave 3 manager dashboard that consolidates:
- Contract Operations KPIs

Design goals:
- 4-page manager surface for runway coverage, activation throughput, renewal risk, and backlog intervention
- stronger monthly trend and forecast views using the last 24 complete months
- contract- and account-owner-level action queues with Salesforce record actions
"""

from __future__ import annotations

import csv
import io
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
    compare_table,
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
    section_label,
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
    safe_float,
)

DS = "Contract_Operations_Renewals"
DS_LABEL = "Contract Operations Renewals"
DASHBOARD_LABEL = "Contract Operations & Renewals"

KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_unit", "f_agreement", "f_status"],
    },
}

CONTRACT_SOQL = (
    "SELECT Id, ContractNumber, AccountId, Account.Name, Status, StartDate, EndDate, "
    "ContractTerm, CreatedDate, Agreement_Type__c, "
    "Account.Unit_Group__c, Account.Risk_of_Potential_Termination__c, "
    "Account.Owner.Name, Account.BillingCountry "
    "FROM Contract "
    "WHERE CreatedDate >= 2022-01-01T00:00:00Z"
)

EMEA_COUNTRIES = {
    "austria",
    "bahrain",
    "belgium",
    "denmark",
    "finland",
    "france",
    "germany",
    "ireland",
    "italy",
    "luxembourg",
    "netherlands",
    "norway",
    "qatar",
    "saudi arabia",
    "south africa",
    "spain",
    "sweden",
    "switzerland",
    "uae",
    "united arab emirates",
    "united kingdom",
}
APAC_COUNTRIES = {
    "australia",
    "china",
    "hong kong",
    "india",
    "indonesia",
    "japan",
    "korea",
    "malaysia",
    "new zealand",
    "philippines",
    "singapore",
    "taiwan",
    "thailand",
    "vietnam",
}
LATAM_COUNTRIES = {
    "argentina",
    "brazil",
    "chile",
    "colombia",
    "mexico",
    "peru",
}


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


def _region(country: str) -> str:
    """Map billing country to broad operating regions."""
    normalized = (country or "").strip().lower()
    if not normalized:
        return "Unknown"
    if normalized in {"united states", "usa", "us", "canada"}:
        return "North America"
    if normalized in EMEA_COUNTRIES:
        return "EMEA"
    if normalized in APAC_COUNTRIES:
        return "APAC"
    if normalized in LATAM_COUNTRIES:
        return "LATAM"
    return "EMEA" if normalized in {"israel", "turkey"} else "Other"


def _agreement_group(value: str) -> str:
    """Cluster contract agreement types into operating groups."""
    lowered = (value or "").strip().lower()
    if not lowered:
        return "Other"
    if "legacy" in lowered:
        return "Legacy"
    if any(
        token in lowered
        for token in (
            "statement of work",
            "sow",
            "master service agreement",
            "master consulting agreement",
        )
    ):
        return "Services"
    if any(
        token in lowered
        for token in (
            "license",
            "order form",
            "pricing",
            "change addon",
            "master agreement",
        )
    ):
        return "Commercial"
    return "Other"


def _status_group(status: str) -> str:
    """Map raw contract status values to operating buckets."""
    lowered = (status or "").strip().lower()
    if lowered in {"active", "activated"}:
        return "Active"
    if lowered in {"created", "draft"}:
        return "Pipeline"
    if lowered == "archived":
        return "Archived"
    return "Other"


def _renewal_window(days_to_expiry: int) -> str:
    """Bucket runway into manager-friendly windows."""
    if days_to_expiry < 0:
        return "Expired"
    if days_to_expiry == 0:
        return "No End Date"
    if days_to_expiry <= 30:
        return "0-30d"
    if days_to_expiry <= 90:
        return "31-90d"
    if days_to_expiry <= 180:
        return "91-180d"
    if days_to_expiry <= 365:
        return "181-365d"
    return "365d+"


def _term_band(term_months: float) -> str:
    """Bucket contract term."""
    if term_months <= 0:
        return "Unknown"
    if term_months < 12:
        return "<12mo"
    if term_months == 12:
        return "12mo"
    if term_months <= 24:
        return "13-24mo"
    if term_months <= 36:
        return "25-36mo"
    return "36mo+"


def _renewal_risk_score(
    status_group: str,
    risk_level: str,
    days_to_expiry: int,
    agreement_group: str,
    has_end_date: bool,
) -> float:
    """Score renewal timing and account risk pressure."""
    score = 0.0

    risk_map = {
        "high": 30.0,
        "medium": 18.0,
        "low": 8.0,
    }
    score += risk_map.get((risk_level or "").strip().lower(), 10.0)

    if status_group == "Active":
        if 0 < days_to_expiry <= 30:
            score += 40.0
        elif 0 < days_to_expiry <= 90:
            score += 28.0
        elif 0 < days_to_expiry <= 180:
            score += 15.0
        elif days_to_expiry < 0:
            score += 20.0

    if agreement_group == "Legacy":
        score += 10.0
    if status_group == "Active" and not has_end_date:
        score += 15.0

    return round(max(0.0, min(100.0, score)), 1)


def _cycle_risk_score(
    status_group: str,
    contract_age_days: int,
    days_to_start: int,
    has_start_date: bool,
    agreement_group: str,
) -> float:
    """Score pipeline and activation cycle pressure."""
    score = 0.0

    if status_group == "Pipeline":
        if contract_age_days > 90:
            score += 40.0
        elif contract_age_days > 45:
            score += 25.0
        elif contract_age_days > 30:
            score += 15.0

        if not has_start_date:
            score += 20.0
        elif days_to_start > 60:
            score += 20.0
        elif days_to_start > 30:
            score += 10.0

    if agreement_group == "Legacy":
        score += 10.0

    return round(max(0.0, min(100.0, score)), 1)


def _next_best_action(
    status_group: str,
    days_to_expiry: int,
    renewal_risk_score: float,
    cycle_risk_score: float,
    agreement_group: str,
    has_start_date: bool,
) -> str:
    """Recommend a manager-facing next action."""
    if (
        status_group == "Active"
        and 0 < days_to_expiry <= 30
        and renewal_risk_score >= 65
    ):
        return "Escalate renewal with account owner"
    if status_group == "Active" and 0 < days_to_expiry <= 90:
        return "Lock renewal path"
    if status_group == "Pipeline" and cycle_risk_score >= 60:
        return "Unblock legal/commercial review"
    if status_group == "Pipeline" and not has_start_date:
        return "Confirm start date and activation owner"
    if agreement_group == "Legacy":
        return "Modernize agreement structure"
    return "Monitor"


def create_dataset(inst: str, tok: str) -> bool:
    """Build the consolidated contract operations dataset."""
    print(f"\n=== Building {DS_LABEL} dataset ===")
    contracts = _soql(inst, tok, CONTRACT_SOQL)
    print(f"  Queried {len(contracts)} contracts")

    today = datetime.now(UTC).date()
    today_iso = today.isoformat()
    current_month = today.strftime("%Y-%m")
    last_complete_month = _add_months(current_month, -1)
    recent_start_month = _add_months(last_complete_month, -23)
    forecast_end_month = _add_months(current_month, 12)

    prepared: list[dict[str, object]] = []
    monthly_grouped: dict[tuple[str, str, str], dict[str, dict[str, float]]] = (
        defaultdict(
            lambda: defaultdict(
                lambda: {
                    "CreatedCount": 0.0,
                    "StartedCount": 0.0,
                    "ExpiringCount": 0.0,
                    "RenewalRiskCount": 0.0,
                    "ActivationBacklogCount": 0.0,
                    "StartLagTotal": 0.0,
                }
            )
        )
    )
    detail_rows: list[dict[str, object]] = []

    for contract in contracts:
        account = contract.get("Account") or {}
        created_date = (contract.get("CreatedDate") or "")[:10]
        if not created_date:
            continue

        start_date = (contract.get("StartDate") or "")[:10]
        end_date = (contract.get("EndDate") or "")[:10]
        created_month = month_key(created_date)
        start_month = month_key(start_date)
        end_month = month_key(end_date)

        status = ((contract.get("Status") or "Unknown").strip() or "Unknown")[:255]
        status_group = _status_group(status)
        agreement_type = (
            (contract.get("Agreement_Type__c") or "Unknown").strip() or "Unknown"
        )[:255]
        agreement_group = _agreement_group(agreement_type)
        risk_level = (
            (account.get("Risk_of_Potential_Termination__c") or "Low").strip() or "Low"
        )[:255]
        unit_group = (
            (account.get("Unit_Group__c") or "Unassigned").strip() or "Unassigned"
        )[:255]
        account_name = ((account.get("Name") or "").strip() or "(No Account)")[:255]
        account_owner = (
            (((account.get("Owner") or {}).get("Name")) or "Unassigned").strip()
            or "Unassigned"
        )[:255]
        country = ((account.get("BillingCountry") or "Unknown").strip() or "Unknown")[
            :255
        ]
        region = _region(country)

        contract_term = round(safe_float(contract.get("ContractTerm")), 1)
        contract_age_days = _days_between(created_date, today_iso)
        days_to_start = _days_between(created_date, start_date) if start_date else 0
        days_to_expiry = 0
        if end_date:
            expiry_date = _parse_date(end_date)
            if expiry_date:
                days_to_expiry = (expiry_date - today).days

        renewal_window = _renewal_window(days_to_expiry)
        term_band = _term_band(contract_term)
        renewal_risk_score = _renewal_risk_score(
            status_group,
            risk_level,
            days_to_expiry,
            agreement_group,
            bool(end_date),
        )
        cycle_risk_score = _cycle_risk_score(
            status_group,
            contract_age_days,
            days_to_start,
            bool(start_date),
            agreement_group,
        )

        active_count = 1 if status_group == "Active" else 0
        expiring_90_count = 1 if active_count and 0 < days_to_expiry <= 90 else 0
        expiring_180_count = 1 if active_count and 0 < days_to_expiry <= 180 else 0
        activation_backlog_count = (
            1 if status_group == "Pipeline" and contract_age_days > 30 else 0
        )
        renewal_risk_count = 1 if active_count and renewal_risk_score >= 65 else 0
        safe_runway_count = 1 if active_count and days_to_expiry > 90 else 0
        legacy_count = 1 if agreement_group == "Legacy" else 0

        key = (unit_group, agreement_group, status_group)
        if created_month and recent_start_month <= created_month <= current_month:
            monthly_grouped[key][created_month]["CreatedCount"] += 1.0
            monthly_grouped[key][created_month]["ActivationBacklogCount"] += (
                activation_backlog_count
            )
        if start_month and recent_start_month <= start_month <= current_month:
            monthly_grouped[key][start_month]["StartedCount"] += 1.0
            monthly_grouped[key][start_month]["StartLagTotal"] += days_to_start
        if end_month and recent_start_month <= end_month <= forecast_end_month:
            monthly_grouped[key][end_month]["ExpiringCount"] += 1.0
            monthly_grouped[key][end_month]["RenewalRiskCount"] += renewal_risk_count

        detail_rows.append(
            {
                "RecordType": "detail",
                "Id": contract.get("Id") or "",
                "ContractNumber": (contract.get("ContractNumber") or "")[:255],
                "AccountId": contract.get("AccountId") or "",
                "AccountName": account_name,
                "AccountOwnerName": account_owner,
                "UnitGroup": unit_group,
                "Region": region,
                "Status": status,
                "StatusGroup": status_group,
                "AgreementType": agreement_type,
                "AgreementGroup": agreement_group,
                "RiskLevel": risk_level,
                "RenewalWindow": renewal_window,
                "TermBand": term_band,
                "NextBestAction": _next_best_action(
                    status_group,
                    days_to_expiry,
                    renewal_risk_score,
                    cycle_risk_score,
                    agreement_group,
                    bool(start_date),
                ),
                "CreatedDate": created_date,
                "StartDate": start_date,
                "EndDate": end_date,
                "MonthDate": "",
                "MonthLabel": "",
                "ContractTermNum": contract_term,
                "ContractAgeDays": contract_age_days,
                "DaysToStart": days_to_start,
                "DaysToExpiry": days_to_expiry,
                "RenewalRiskScore": renewal_risk_score,
                "CycleRiskScore": cycle_risk_score,
                "ActiveCount": active_count,
                "Expiring90Count": expiring_90_count,
                "Expiring180Count": expiring_180_count,
                "ActivationBacklogCount": activation_backlog_count,
                "RenewalRiskCount": renewal_risk_count,
                "SafeRunwayCount": safe_runway_count,
                "LegacyCount": legacy_count,
                "CreatedCount": 1,
                "StartedCount": 1 if start_date else 0,
                "ExpiringCount": 1 if end_date else 0,
                "StartLagTotal": float(days_to_start if start_date else 0),
                "ForecastStartedCount": 0.0,
                "ForecastStartedCount_high_95": 0.0,
                "ForecastStartedCount_low_95": 0.0,
            }
        )

        prepared.append(
            {
                "UnitGroup": unit_group,
                "AgreementGroup": agreement_group,
                "StatusGroup": status_group,
            }
        )

    trend_rows: list[dict[str, object]] = []
    trend_months = month_sequence(recent_start_month, forecast_end_month)
    historical_months = [
        month for month in trend_months if month <= last_complete_month
    ]

    for (
        unit_group,
        agreement_group,
        status_group,
    ), monthly_data in monthly_grouped.items():
        started_series = [
            monthly_data[month]["StartedCount"] for month in historical_months
        ]
        fit = least_squares(started_series)

        for index, month in enumerate(trend_months):
            values = monthly_data[month]
            forecast = max(0.0, fit["intercept"] + fit["slope"] * index)
            interval = prediction_interval(fit, index)
            trend_rows.append(
                {
                    "RecordType": "trend",
                    "Id": "",
                    "ContractNumber": "",
                    "AccountId": "",
                    "AccountName": "",
                    "AccountOwnerName": "",
                    "UnitGroup": unit_group,
                    "Region": "",
                    "Status": "",
                    "StatusGroup": status_group,
                    "AgreementType": "",
                    "AgreementGroup": agreement_group,
                    "RiskLevel": "",
                    "RenewalWindow": "",
                    "TermBand": "",
                    "NextBestAction": "",
                    "CreatedDate": "",
                    "StartDate": "",
                    "EndDate": "",
                    "MonthDate": f"{month}-01",
                    "MonthLabel": month,
                    "ContractTermNum": 0.0,
                    "ContractAgeDays": 0,
                    "DaysToStart": 0,
                    "DaysToExpiry": 0,
                    "RenewalRiskScore": 0.0,
                    "CycleRiskScore": 0.0,
                    "ActiveCount": 0,
                    "Expiring90Count": 0,
                    "Expiring180Count": 0,
                    "ActivationBacklogCount": int(values["ActivationBacklogCount"]),
                    "RenewalRiskCount": int(values["RenewalRiskCount"]),
                    "SafeRunwayCount": 0,
                    "LegacyCount": 0,
                    "CreatedCount": int(values["CreatedCount"]),
                    "StartedCount": int(values["StartedCount"]),
                    "ExpiringCount": int(values["ExpiringCount"]),
                    "StartLagTotal": round(values["StartLagTotal"], 2),
                    "ForecastStartedCount": round(forecast, 2),
                    "ForecastStartedCount_high_95": round(
                        max(0.0, forecast + interval), 2
                    ),
                    "ForecastStartedCount_low_95": round(
                        max(0.0, forecast - interval), 2
                    ),
                }
            )

    rows = detail_rows + trend_rows
    print(f"  Detail rows: {len(detail_rows)}")
    print(f"  Trend rows: {len(trend_rows)}")
    print(f"  Total rows: {len(rows)}")

    field_names = [
        "RecordType",
        "Id",
        "ContractNumber",
        "AccountId",
        "AccountName",
        "AccountOwnerName",
        "UnitGroup",
        "Region",
        "Status",
        "StatusGroup",
        "AgreementType",
        "AgreementGroup",
        "RiskLevel",
        "RenewalWindow",
        "TermBand",
        "NextBestAction",
        "CreatedDate",
        "StartDate",
        "EndDate",
        "MonthDate",
        "MonthLabel",
        "ContractTermNum",
        "ContractAgeDays",
        "DaysToStart",
        "DaysToExpiry",
        "RenewalRiskScore",
        "CycleRiskScore",
        "ActiveCount",
        "Expiring90Count",
        "Expiring180Count",
        "ActivationBacklogCount",
        "RenewalRiskCount",
        "SafeRunwayCount",
        "LegacyCount",
        "CreatedCount",
        "StartedCount",
        "ExpiringCount",
        "StartLagTotal",
        "ForecastStartedCount",
        "ForecastStartedCount_high_95",
        "ForecastStartedCount_low_95",
    ]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=field_names, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    csv_bytes = buffer.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes")

    fields_meta = [
        _dim("RecordType", "Record Type"),
        _dim("Id", "Contract ID"),
        _dim("ContractNumber", "Contract Number"),
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account"),
        _dim("AccountOwnerName", "Account Owner"),
        _dim("UnitGroup", "Unit Group"),
        _dim("Region", "Region"),
        _dim("Status", "Status"),
        _dim("StatusGroup", "Status Group"),
        _dim("AgreementType", "Agreement Type"),
        _dim("AgreementGroup", "Agreement Group"),
        _dim("RiskLevel", "Risk Level"),
        _dim("RenewalWindow", "Renewal Window"),
        _dim("TermBand", "Term Band"),
        _dim("NextBestAction", "Next Best Action"),
        _date("CreatedDate", "Created Date"),
        _date("StartDate", "Start Date"),
        _date("EndDate", "End Date"),
        _date("MonthDate", "Month"),
        _dim("MonthLabel", "Month Label"),
        _measure("ContractTermNum", "Contract Term", scale=1, precision=6),
        _measure("ContractAgeDays", "Contract Age (Days)", scale=0, precision=6),
        _measure("DaysToStart", "Days to Start", scale=0, precision=6),
        _measure("DaysToExpiry", "Days to Expiry", scale=0, precision=6),
        _measure("RenewalRiskScore", "Renewal Risk Score", scale=1, precision=5),
        _measure("CycleRiskScore", "Cycle Risk Score", scale=1, precision=5),
        _measure("ActiveCount", "Active Count", scale=0, precision=6),
        _measure("Expiring90Count", "Expiring 90 Count", scale=0, precision=6),
        _measure("Expiring180Count", "Expiring 180 Count", scale=0, precision=6),
        _measure(
            "ActivationBacklogCount", "Activation Backlog Count", scale=0, precision=6
        ),
        _measure("RenewalRiskCount", "Renewal Risk Count", scale=0, precision=6),
        _measure("SafeRunwayCount", "Safe Runway Count", scale=0, precision=6),
        _measure("LegacyCount", "Legacy Count", scale=0, precision=6),
        _measure("CreatedCount", "Created Count", scale=0, precision=6),
        _measure("StartedCount", "Started Count", scale=0, precision=6),
        _measure("ExpiringCount", "Expiring Count", scale=0, precision=6),
        _measure("StartLagTotal", "Start Lag Total", scale=2, precision=8),
        _measure(
            "ForecastStartedCount", "Forecast Started Count", scale=2, precision=8
        ),
        _measure(
            "ForecastStartedCount_high_95",
            "Forecast Started Count High 95",
            scale=2,
            precision=8,
        ),
        _measure(
            "ForecastStartedCount_low_95",
            "Forecast Started Count Low 95",
            scale=2,
            precision=8,
        ),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_agreement = coalesce_filter("f_agreement", "AgreementGroup")
    filter_status = coalesce_filter("f_status", "StatusGroup")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_agreement
        + filter_status
    )
    trend = (
        load
        + 'q = filter q by RecordType == "trend";\n'
        + filter_unit
        + filter_agreement
        + filter_status
    )

    return {
        "f_unit": af("UnitGroup", ds_meta),
        "f_agreement": af("AgreementGroup", ds_meta),
        "f_status": af("StatusGroup", ds_meta),
        "s_summary": sq(
            detail
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(ActiveCount) as active_contracts, "
            + "sum(Expiring180Count) as expiring_180, "
            + "sum(ActivationBacklogCount) as activation_backlog, "
            + "case when sum(ActiveCount) > 0 then (sum(SafeRunwayCount) / sum(ActiveCount)) * 100 else 0 end as runway_coverage, "
            + "85 as target_coverage, "
            + "75 as good, "
            + "60 as satisfactory;"
        ),
        "s_monthly_trajectory": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ExpiringCount) as ExpiringCount, "
            + "sum(StartedCount) as StartedCount, "
            + "sum(ForecastStartedCount) as ForecastStartedCount, "
            + "sum(ForecastStartedCount_high_95) as ForecastStartedCount_high_95, "
            + "sum(ForecastStartedCount_low_95) as ForecastStartedCount_low_95;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_agreement_mix": sq(
            detail
            + "q = group q by AgreementGroup;\n"
            + "q = foreach q generate AgreementGroup, "
            + "sum(ActiveCount) as ActiveCount, "
            + "sum(RenewalRiskCount) as RenewalRiskCount, "
            + "sum(ActivationBacklogCount) as ActivationBacklogCount;\n"
            + "q = order q by ActiveCount desc;"
        ),
        "s_unit_mix": sq(
            detail
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(ActiveCount) as ActiveCount, "
            + "sum(RenewalRiskCount) as RenewalRiskCount, "
            + "sum(ActivationBacklogCount) as ActivationBacklogCount;\n"
            + "q = order q by ActiveCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_expiry_trend": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ExpiringCount) as ExpiringCount, "
            + "sum(RenewalRiskCount) as RenewalRiskCount;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_throughput": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(CreatedCount) as CreatedCount, "
            + "sum(StartedCount) as StartedCount, "
            + "sum(ForecastStartedCount) as ForecastStartedCount, "
            + "sum(ForecastStartedCount_high_95) as ForecastStartedCount_high_95, "
            + "sum(ForecastStartedCount_low_95) as ForecastStartedCount_low_95;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_cycle_trend": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(ActivationBacklogCount) as ActivationBacklogCount, "
            + "case when sum(StartedCount) > 0 then sum(StartLagTotal) / sum(StartedCount) else 0 end as AvgActivationLag;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_window_mix": sq(
            detail
            + "q = group q by RenewalWindow;\n"
            + "q = foreach q generate RenewalWindow, "
            + "count() as ContractCount, "
            + "sum(RenewalRiskCount) as RenewalRiskCount;\n"
            + "q = order q by ContractCount desc;"
        ),
        "s_term_mix": sq(
            detail
            + "q = group q by TermBand;\n"
            + "q = foreach q generate TermBand, "
            + "sum(ActiveCount) as ActiveCount, "
            + "sum(Expiring180Count) as Expiring180Count, "
            + "sum(RenewalRiskCount) as RenewalRiskCount;\n"
            + "q = order q by ActiveCount desc;"
        ),
        "s_region_mix": sq(
            detail
            + "q = group q by Region;\n"
            + "q = foreach q generate Region, "
            + "sum(ActiveCount) as ActiveCount, "
            + "sum(RenewalRiskCount) as RenewalRiskCount, "
            + "sum(ActivationBacklogCount) as ActivationBacklogCount;\n"
            + "q = order q by ActiveCount desc;"
        ),
        "s_exception_summary": sq(
            detail
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(Expiring90Count) as at_risk_expiring_90, "
            + "sum(ActivationBacklogCount) as activation_backlog;"
        ),
        "s_owner_queue": sq(
            detail
            + "q = group q by AccountOwnerName;\n"
            + "q = foreach q generate AccountOwnerName, "
            + "sum(RenewalRiskCount) as RenewalRiskCount, "
            + "sum(ActivationBacklogCount) as ActivationBacklogCount, "
            + "sum(Expiring90Count) as Expiring90Count;\n"
            + "q = order q by RenewalRiskCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_top_expiring": sq(
            detail
            + 'q = filter q by StatusGroup == "Active";\n'
            + "q = filter q by Expiring90Count > 0 or RenewalRiskCount > 0;\n"
            + "q = group q by (ContractNumber, AccountName, AccountOwnerName, UnitGroup, AgreementGroup, NextBestAction, Id);\n"
            + "q = foreach q generate ContractNumber, AccountName, AccountOwnerName, UnitGroup, AgreementGroup, NextBestAction, "
            + "max(DaysToExpiry) as DaysToExpiry, "
            + "max(RenewalRiskScore) as RenewalRiskScore, "
            + "max(ContractTermNum) as ContractTermNum, "
            + "Id;\n"
            + "q = order q by RenewalRiskScore desc;\n"
            + "q = limit q 15;"
        ),
        "s_top_backlog": sq(
            detail
            + 'q = filter q by StatusGroup == "Pipeline";\n'
            + "q = filter q by ActivationBacklogCount > 0;\n"
            + "q = group q by (ContractNumber, AccountName, AccountOwnerName, UnitGroup, AgreementGroup, NextBestAction, Id);\n"
            + "q = foreach q generate ContractNumber, AccountName, AccountOwnerName, UnitGroup, AgreementGroup, NextBestAction, "
            + "max(ContractAgeDays) as ContractAgeDays, "
            + "max(CycleRiskScore) as CycleRiskScore, "
            + "max(DaysToStart) as DaysToStart, "
            + "Id;\n"
            + "q = order q by CycleRiskScore desc;\n"
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
            "Contract Operations & Renewals",
            "Manager surface for runway coverage, activation throughput, renewal risk, and contract backlog intervention.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_agreement": pillbox("f_agreement", "Agreement Group"),
        "p1_f_status": pillbox("f_status", "Status Group"),
        "p1_n_active": num(
            "s_summary", "active_contracts", "Active Contracts", "#032D60", compact=True
        ),
        "p1_n_expiring": num(
            "s_summary",
            "expiring_180",
            "Expiring Next 180d",
            "#0176D3",
            compact=True,
        ),
        "p1_n_backlog": num(
            "s_summary",
            "activation_backlog",
            "Activation Backlog",
            "#BA0517",
            compact=True,
        ),
        "p1_ch_timeline": timeline_chart(
            "s_monthly_trajectory",
            "Expiry Load and Activation Throughput Forecast",
            show_legend=True,
            axis_title="Contracts",
        ),
        "p1_ch_bullet": bullet_chart(
            "s_summary",
            "Runway Coverage vs Target",
            axis_title="Coverage %",
        ),
        "p1_ch_agreement": rich_chart(
            "s_agreement_mix",
            "stackcolumn",
            "Agreement Group Mix: Active, Risk, Backlog",
            ["AgreementGroup"],
            ["ActiveCount", "RenewalRiskCount", "ActivationBacklogCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p1_ch_unit": rich_chart(
            "s_unit_mix",
            "stackhbar",
            "Unit Group Risk and Backlog Mix",
            ["UnitGroup"],
            ["ActiveCount", "RenewalRiskCount", "ActivationBacklogCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("trend", "Trend & Forecast", active=True),
        "p2_nav3": nav_link("drivers", "Drivers & Segments"),
        "p2_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p2_hdr": hdr(
            "Trend & Forecast",
            "How expiry load, activation throughput, backlog pressure, and runway windows are moving over time.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_agreement": pillbox("f_agreement", "Agreement Group"),
        "p2_f_status": pillbox("f_status", "Status Group"),
        "p2_ch_expiry": rich_chart(
            "s_expiry_trend",
            "line",
            "Monthly Expiry Load and Renewal Risk",
            ["MonthDate"],
            ["ExpiringCount", "RenewalRiskCount"],
            show_legend=True,
            axis_title="Contracts",
        ),
        "p2_ch_throughput": timeline_chart(
            "s_throughput",
            "Created vs Started Contracts with Throughput Forecast",
            show_legend=True,
            axis_title="Contracts",
        ),
        "p2_ch_cycle": rich_chart(
            "s_cycle_trend",
            "line",
            "Activation Backlog and Average Activation Lag",
            ["MonthDate"],
            ["ActivationBacklogCount", "AvgActivationLag"],
            show_legend=True,
            axis_title="Contracts / Days",
        ),
        "p2_ch_window": rich_chart(
            "s_window_mix",
            "stackcolumn",
            "Runway Window Distribution",
            ["RenewalWindow"],
            ["ContractCount", "RenewalRiskCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p3_nav1": nav_link("summary", "Summary"),
        "p3_nav2": nav_link("trend", "Trend & Forecast"),
        "p3_nav3": nav_link("drivers", "Drivers & Segments", active=True),
        "p3_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p3_hdr": hdr(
            "Drivers & Segments",
            "Where contract risk, runway exposure, and backlog pressure are concentrated across the estate.",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_agreement": pillbox("f_agreement", "Agreement Group"),
        "p3_f_status": pillbox("f_status", "Status Group"),
        "p3_ch_agreement": rich_chart(
            "s_agreement_mix",
            "stackhbar",
            "Agreement Group Risk and Backlog Mix",
            ["AgreementGroup"],
            ["ActiveCount", "RenewalRiskCount", "ActivationBacklogCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p3_ch_term": rich_chart(
            "s_term_mix",
            "stackcolumn",
            "Term Band Exposure",
            ["TermBand"],
            ["ActiveCount", "Expiring180Count", "RenewalRiskCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p3_ch_region": rich_chart(
            "s_region_mix",
            "stackhbar",
            "Region Risk and Backlog Mix",
            ["Region"],
            ["ActiveCount", "RenewalRiskCount", "ActivationBacklogCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p3_ch_unit": rich_chart(
            "s_unit_mix",
            "stackhbar",
            "Unit Group Risk and Backlog Mix",
            ["UnitGroup"],
            ["ActiveCount", "RenewalRiskCount", "ActivationBacklogCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p4_nav1": nav_link("summary", "Summary"),
        "p4_nav2": nav_link("trend", "Trend & Forecast"),
        "p4_nav3": nav_link("drivers", "Drivers & Segments"),
        "p4_nav4": nav_link("exceptions", "Exceptions & Actions", active=True),
        "p4_hdr": hdr(
            "Exceptions & Actions",
            "The renewals and backlog contracts that need intervention before timing and risk degrade further.",
        ),
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_agreement": pillbox("f_agreement", "Agreement Group"),
        "p4_f_status": pillbox("f_status", "Status Group"),
        "p4_n_risk": num(
            "s_exception_summary",
            "at_risk_expiring_90",
            "At-Risk Expiring 90d",
            "#8E030F",
            compact=True,
        ),
        "p4_n_backlog": num(
            "s_exception_summary",
            "activation_backlog",
            "Backlog Over 30d",
            "#BA0517",
            compact=True,
        ),
        "p4_ch_owner": rich_chart(
            "s_owner_queue",
            "stackhbar",
            "Account Owner Action Queue",
            ["AccountOwnerName"],
            ["RenewalRiskCount", "ActivationBacklogCount", "Expiring90Count"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p4_tbl_expiring": rich_chart(
            "s_top_expiring",
            "comparisontable",
            "Top Expiring / At-Risk Contracts",
            [
                "ContractNumber",
                "AccountName",
                "AccountOwnerName",
                "UnitGroup",
                "AgreementGroup",
                "NextBestAction",
            ],
            ["DaysToExpiry", "RenewalRiskScore", "ContractTermNum"],
            show_legend=False,
        ),
        "p4_tbl_backlog": rich_chart(
            "s_top_backlog",
            "comparisontable",
            "Top Activation Backlog Contracts",
            [
                "ContractNumber",
                "AccountName",
                "AccountOwnerName",
                "UnitGroup",
                "AgreementGroup",
                "NextBestAction",
            ],
            ["ContractAgeDays", "CycleRiskScore", "DaysToStart"],
            show_legend=False,
        ),
    }

    add_table_action(widgets["p4_tbl_expiring"], "salesforceActions", "Contract", "Id")
    add_table_action(widgets["p4_tbl_backlog"], "salesforceActions", "Contract", "Id")
    return widgets


def legacy_build_layout() -> dict:
    """Build the 4-page manager dashboard layout."""
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_agreement", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_status", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_active", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_expiring", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_backlog", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_ch_timeline", "row": 9, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_bullet", "row": 17, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p1_ch_agreement", "row": 17, "column": 4, "colspan": 4, "rowspan": 6},
        {"name": "p1_ch_unit", "row": 17, "column": 8, "colspan": 4, "rowspan": 6},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_agreement", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_status", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_expiry", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_throughput", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_cycle", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_window", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_agreement", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_status", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_ch_agreement", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_term", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_region", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_unit", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_agreement", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_status", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p4_n_risk", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_backlog", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p4_ch_owner", "row": 5, "column": 6, "colspan": 6, "rowspan": 6},
        {
            "name": "p4_tbl_expiring",
            "row": 11,
            "column": 0,
            "colspan": 12,
            "rowspan": 7,
        },
        {"name": "p4_tbl_backlog", "row": 18, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "ContractOperationsRenewals",
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
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_agreement = coalesce_filter("f_agreement", "AgreementGroup")
    filter_status = coalesce_filter("f_status", "StatusGroup")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + filter_unit
        + filter_agreement
        + filter_status
    )

    # Apply KPI_FACET_SCOPE to KPI steps (respond to filter pillboxes only)
    for s_name in ("s_summary", "s_exception_summary"):
        if s_name in steps:
            steps[s_name].update(KPI_FACET_SCOPE)

    steps.update(
        {
            "s_window_heatmap": sq(
                detail
                + "q = group q by (AgreementGroup, RenewalWindow);\n"
                + "q = foreach q generate AgreementGroup, RenewalWindow, sum(ContractCount) as ContractCount;\n"
                + "q = order q by ContractCount desc;"
            ),
            "s_type_status_flow": sq(
                detail
                + "q = group q by (AgreementGroup, StatusGroup);\n"
                + "q = foreach q generate AgreementGroup as source, StatusGroup as target, count() as flow;\n"
                + "q = order q by flow desc;\n"
                + "q = limit q 30;"
            ),
            "s_portfolio_treemap": sq(
                detail
                + "q = group q by (UnitGroup, AgreementGroup);\n"
                + "q = foreach q generate UnitGroup, AgreementGroup, sum(ActiveCount) as ActiveCount;\n"
                + "q = order q by ActiveCount desc;\n"
                + "q = limit q 30;"
            ),
            "s_cycle_scatter": sq(
                detail
                + 'q = filter q by StatusGroup == "Pipeline";\n'
                + "q = group q by (ContractNumber, AccountName, AccountOwnerName, AgreementGroup, Id);\n"
                + "q = foreach q generate ContractNumber, AccountName, AccountOwnerName, AgreementGroup, "
                + "max(ContractAgeDays) as ContractAgeDays, "
                + "max(CycleRiskScore) as CycleRiskScore, "
                + "max(DaysToStart) as DaysToStart, "
                + "Id;\n"
                + "q = order q by CycleRiskScore desc;\n"
                + "q = limit q 25;"
            ),
        }
    )
    return steps


def build_widgets() -> dict[str, dict]:
    """Build dashboard widgets."""
    widgets = {
        "p1_nav1": nav_link("summary", "Summary", active=True),
        "p1_nav2": nav_link("trend", "Trend & Forecast"),
        "p1_nav3": nav_link("drivers", "Drivers & Segments"),
        "p1_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p1_hdr": hdr(
            "Contract Operations & Renewals",
            "Manager operating view for runway coverage, renewal pressure, and activation backlog intervention.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_agreement": pillbox("f_agreement", "Agreement Group"),
        "p1_f_status": pillbox("f_status", "Status Group"),
        "p1_section": section_label("Contract Operations Summary"),
        "p1_n_active": num(
            "s_summary",
            "active_contracts",
            "Active Contracts",
            "#032D60",
            compact=True,
            tier="primary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_expiring": num(
            "s_summary",
            "expiring_180",
            "Expiring Next 180d",
            "#0176D3",
            compact=True,
            tier="primary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_n_backlog": num(
            "s_summary",
            "activation_backlog",
            "Activation Backlog",
            "#BA0517",
            compact=True,
            tier="primary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_ch_timeline": timeline_chart(
            "s_monthly_trajectory",
            "Expiry Load and Activation Throughput Forecast",
            show_legend=True,
            axis_title="Contracts",
        ),
        "p1_ch_agreement": rich_chart(
            "s_agreement_mix",
            "stackcolumn",
            "Agreement Group Mix: Active, Risk, Backlog",
            ["AgreementGroup"],
            ["ActiveCount", "RenewalRiskCount", "ActivationBacklogCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
            reference_lines=[
                {"value": 50, "label": "Attention Threshold", "color": "#FFB75D"},
            ],
        ),
        "p1_ch_window": heatmap_chart(
            "s_window_heatmap",
            "Agreement Group x Renewal Window",
            show_legend=True,
        ),
        "p1_ch_unit": rich_chart(
            "s_unit_mix",
            "stackhbar",
            "Unit Group Risk and Backlog Mix",
            ["UnitGroup"],
            ["ActiveCount", "RenewalRiskCount", "ActivationBacklogCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("trend", "Trend & Forecast", active=True),
        "p2_nav3": nav_link("drivers", "Drivers & Segments"),
        "p2_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p2_hdr": hdr(
            "Trend & Forecast",
            "How expiry load, activation throughput, backlog pressure, and runway windows are moving over time.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_agreement": pillbox("f_agreement", "Agreement Group"),
        "p2_f_status": pillbox("f_status", "Status Group"),
        "p2_section": section_label("Trend & Forecast Analysis"),
        "p2_ch_expiry": rich_chart(
            "s_expiry_trend",
            "line",
            "Monthly Expiry Load and Renewal Risk",
            ["MonthDate"],
            ["ExpiringCount", "RenewalRiskCount"],
            show_legend=True,
            axis_title="Contracts",
        ),
        "p2_ch_throughput": timeline_chart(
            "s_throughput",
            "Created vs Started Contracts with Throughput Forecast",
            show_legend=True,
            axis_title="Contracts",
        ),
        "p2_ch_cycle": rich_chart(
            "s_cycle_trend",
            "line",
            "Activation Backlog and Average Activation Lag",
            ["MonthDate"],
            ["ActivationBacklogCount", "AvgActivationLag"],
            show_legend=True,
            axis_title="Contracts / Days",
        ),
        "p2_ch_window_dist": rich_chart(
            "s_window_mix",
            "stackcolumn",
            "Runway Window Distribution",
            ["RenewalWindow"],
            ["ContractCount", "RenewalRiskCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p3_nav1": nav_link("summary", "Summary"),
        "p3_nav2": nav_link("trend", "Trend & Forecast"),
        "p3_nav3": nav_link("drivers", "Drivers & Segments", active=True),
        "p3_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p3_hdr": hdr(
            "Drivers & Segments",
            "Where contract risk, runway exposure, and backlog pressure are concentrated across the estate.",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_agreement": pillbox("f_agreement", "Agreement Group"),
        "p3_f_status": pillbox("f_status", "Status Group"),
        "p3_section": section_label("Drivers & Segment Analysis"),
        "p3_ch_treemap": treemap_chart(
            "s_portfolio_treemap",
            "Contract Portfolio Composition",
            ["UnitGroup", "AgreementGroup"],
            "ActiveCount",
            show_legend=False,
        ),
        "p3_ch_heatmap": heatmap_chart(
            "s_window_heatmap",
            "Agreement Group x Renewal Window",
            show_legend=True,
        ),
        "p3_ch_flow": sankey_chart(
            "s_type_status_flow",
            "Agreement Group -> Contract Status",
        ),
        "p3_ch_region": rich_chart(
            "s_region_mix",
            "stackhbar",
            "Region Risk and Backlog Mix",
            ["Region"],
            ["ActiveCount", "RenewalRiskCount", "ActivationBacklogCount"],
            show_legend=True,
            axis_title="Contracts",
            show_values=True,
        ),
        "p4_nav1": nav_link("summary", "Summary"),
        "p4_nav2": nav_link("trend", "Trend & Forecast"),
        "p4_nav3": nav_link("drivers", "Drivers & Segments"),
        "p4_nav4": nav_link("exceptions", "Exceptions & Actions", active=True),
        "p4_hdr": hdr(
            "Exceptions & Actions",
            "The renewals and backlog contracts that need intervention before timing and risk degrade further.",
        ),
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_agreement": pillbox("f_agreement", "Agreement Group"),
        "p4_f_status": pillbox("f_status", "Status Group"),
        "p4_section": section_label("Exception Queue & Actions"),
        "p4_n_risk": num(
            "s_exception_summary",
            "at_risk_expiring_90",
            "At-Risk Expiring 90d",
            "#8E030F",
            compact=True,
            tier="secondary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p4_n_backlog": num(
            "s_exception_summary",
            "activation_backlog",
            "Backlog Over 30d",
            "#BA0517",
            compact=True,
            tier="secondary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p4_ch_scatter": bubble_chart(
            "s_cycle_scatter",
            "Cycle Risk vs Contract Age",
            show_legend=False,
        ),
        "p4_tbl_expiring": compare_table(
            "s_top_expiring",
            "Top Expiring / At-Risk Contracts",
            row_limit=15,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "RenewalRiskScore",
                    "rules": [
                        {"value": 65, "color": "#D4504C", "operator": "gte"},
                        {"value": 40, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "DaysToExpiry",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "lte"},
                        {"value": 90, "color": "#FFB75D", "operator": "lte"},
                    ],
                },
            ],
        ),
        "p4_tbl_backlog": compare_table(
            "s_top_backlog",
            "Top Activation Backlog Contracts",
            row_limit=15,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "CycleRiskScore",
                    "rules": [
                        {"value": 60, "color": "#D4504C", "operator": "gte"},
                        {"value": 35, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "ContractAgeDays",
                    "rules": [
                        {"value": 90, "color": "#D4504C", "operator": "gte"},
                        {"value": 45, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
    }

    add_table_action(widgets["p4_tbl_expiring"], "salesforceActions", "Contract", "Id")
    add_table_action(widgets["p4_tbl_backlog"], "salesforceActions", "Contract", "Id")
    return widgets


def build_layout() -> dict:
    """Build the 4-page manager dashboard layout."""
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_agreement", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_status", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_section", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_n_active", "row": 6, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_expiring", "row": 6, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_backlog", "row": 6, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_ch_timeline", "row": 10, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_agreement", "row": 18, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p1_ch_window", "row": 18, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p1_ch_unit", "row": 24, "column": 0, "colspan": 12, "rowspan": 6},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_agreement", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_status", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_section", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_expiry", "row": 6, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_throughput", "row": 6, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_cycle", "row": 13, "column": 0, "colspan": 6, "rowspan": 7},
        {
            "name": "p2_ch_window_dist",
            "row": 13,
            "column": 6,
            "colspan": 6,
            "rowspan": 7,
        },
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_agreement", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_status", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_section", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_treemap", "row": 6, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_heatmap", "row": 6, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_flow", "row": 13, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_region", "row": 13, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_agreement", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_status", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p4_section", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_n_risk", "row": 6, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_backlog", "row": 6, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p4_ch_scatter", "row": 6, "column": 6, "colspan": 6, "rowspan": 6},
        {
            "name": "p4_tbl_expiring",
            "row": 12,
            "column": 0,
            "colspan": 12,
            "rowspan": 7,
        },
        {"name": "p4_tbl_backlog", "row": 19, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "ContractOperationsRenewals",
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
    instance_url, token = get_auth()
    if not create_dataset(instance_url, token):
        raise SystemExit("Dataset upload failed")

    dataset_id = get_dataset_id(instance_url, token, DS)
    if not dataset_id:
        raise SystemExit(f"Could not resolve dataset id for {DS}")

    steps = build_steps(dataset_id)
    widgets = build_widgets()
    layout = build_layout()
    state = build_dashboard_state(
        steps,
        widgets,
        layout,
        bg_color="#F4F6F9",
        cell_spacing=8,
        row_height="fine",
    )

    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    print(f"\n=== Deploying {DASHBOARD_LABEL} ===")
    deploy_dashboard(instance_url, token, dashboard_id, state)

    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "ContractNumber", "id_field": "Id", "label": "Contract"},
            {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
        ],
    )


if __name__ == "__main__":
    main()
