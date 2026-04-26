#!/usr/bin/env python3
"""Build the Lead Funnel dashboard.

This is the Wave 2 manager dashboard that consolidates:
- Lead Management KPIs

Design goals:
- 4-page manager surface for lead intake, qualification, conversion, and response risk
- stronger monthly trend and forecast views using the last 24 complete months
- owner-level action queues with Salesforce record actions
"""

from __future__ import annotations

import csv
import io
from collections import defaultdict
from datetime import UTC, date, datetime

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
    combo_chart,
    coalesce_filter,
    create_dashboard_if_needed,
    deploy_dashboard,
    funnel_chart,
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
    upload_dataset,
)
from portfolio_foundation import (
    least_squares,
    month_key,
    month_sequence,
    prediction_interval,
    safe_float,
)

DS = "Lead_Funnel"
DS_LABEL = "Lead Funnel"
DASHBOARD_LABEL = "Lead Funnel"
CURRENT_YEAR = date.today().year
CURRENT_MONTH_START = f"{CURRENT_YEAR}-01"
CURRENT_MONTH_END = f"{CURRENT_YEAR}-12"

LEAD_SOQL = (
    "SELECT Id, Name, Owner.Name, Status, LeadSource, CreatedDate, "
    "LastActivityDate, ConvertedDate, IsConverted, Company, Country, Industry, "
    "NumberOfEmployees, pi__score__c, pi__campaign__c, pi__utm_campaign__c, "
    "Disqualified_Reason__c "
    "FROM Lead "
    "WHERE CreatedDate >= 2024-01-01T00:00:00Z"
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


def _normalize_source(value: str) -> str:
    """Normalize noisy lead source values into stable labels."""
    source = (value or "").strip()
    if not source:
        return "Unknown"

    lowered = source.lower()
    if lowered in {"unknown", "none", "null"}:
        return "Unknown"
    if lowered == "pardot":
        return "Pardot"
    if lowered == "campaign":
        return "Campaign"
    if lowered in {"tradeshow", "trade show"}:
        return "Trade Show"
    if lowered == "www.simcorp.com":
        return "Website"
    if lowered == "web":
        return "Web"
    return source


def _source_group(source: str) -> str:
    """Classify lead sources into manager-friendly channel groups."""
    lowered = source.lower()
    if source == "Unknown":
        return "Unknown"
    if any(
        token in lowered
        for token in (
            "trade show",
            "consensus",
            "cvent",
            "seminar",
            "webinar",
            "wbr",
        )
    ):
        return "Events"
    if any(
        token in lowered
        for token in (
            "google",
            "bing",
            "linkedin",
            "website",
            "web",
            "pardot",
            "campaign",
        )
    ):
        return "Digital"
    if any(
        token in lowered
        for token in (
            "partner",
            "promowise",
            "public relations",
        )
    ):
        return "Partner / PR"
    if lowered == "other":
        return "Other"
    return "Field / Other"


def _region(country: str) -> str:
    """Map country to broad operating regions."""
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


def _lifecycle_stage(status: str, is_converted: bool) -> str:
    """Map raw lead status values to a manager lifecycle."""
    if is_converted:
        return "Converted"

    lowered = (status or "").strip().lower()
    if "disqualified" in lowered:
        return "Disqualified"
    if "hot" in lowered:
        return "Hot"
    if "qualified" in lowered:
        return "Qualified"
    if "contact" in lowered or "working" in lowered:
        return "Worked"
    return "Open"


def _stage_label(stage: str) -> str:
    """Return a sort-stable lifecycle label."""
    mapping = {
        "Open": "1 - Open",
        "Worked": "2 - Worked",
        "Qualified": "3 - Qualified",
        "Hot": "4 - Hot",
        "Converted": "5 - Converted",
        "Disqualified": "6 - Disqualified",
    }
    return mapping.get(stage, "7 - Other")


def _priority_band(score: float, stage: str) -> str:
    """Bucket lead priority into manager-friendly bands."""
    if stage == "Converted":
        return "Closed"
    if stage == "Disqualified":
        return "Disqualified"
    if stage == "Hot" or score >= 70:
        return "High"
    if stage == "Qualified" or score >= 40:
        return "Medium"
    return "Low"


def _employee_band(employees: int) -> str:
    """Classify lead company size."""
    if employees >= 1000:
        return "Enterprise"
    if employees >= 200:
        return "Mid-Market"
    if employees > 0:
        return "Growth"
    return "Unknown"


def _safe_rate(numerator: float, denominator: float) -> float:
    """Return a guarded rate as a fraction."""
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _conversion_propensity(
    stage: str,
    lead_score: float,
    source_rate_pct: float,
    region_rate_pct: float,
    age_days: int,
    days_since_activity: int,
    has_activity: bool,
) -> float:
    """Compute a lightweight conversion propensity score."""
    if stage == "Converted":
        return 100.0
    if stage == "Disqualified":
        return 0.0

    score = 0.35 * source_rate_pct + 0.15 * region_rate_pct
    score += min(20.0, max(0.0, lead_score / 3.5))
    score += {
        "Hot": 30.0,
        "Qualified": 18.0,
        "Worked": 8.0,
        "Open": 0.0,
    }.get(stage, 0.0)

    if has_activity:
        score += 10.0
    if days_since_activity <= 2:
        score += 8.0
    elif days_since_activity <= 7:
        score += 4.0
    elif days_since_activity > 14:
        score -= 10.0

    if age_days > 90:
        score -= 15.0
    elif age_days > 45:
        score -= 8.0

    return round(max(0.0, min(100.0, score)), 1)


def _response_risk(
    stage: str,
    lead_score: float,
    age_days: int,
    days_since_activity: int,
    has_activity: bool,
) -> float:
    """Score response and aging risk for open leads."""
    if stage in {"Converted", "Disqualified"}:
        return 0.0

    risk = {
        "Hot": 35.0,
        "Qualified": 22.0,
        "Worked": 10.0,
        "Open": 5.0,
    }.get(stage, 0.0)

    if lead_score >= 70:
        risk += 20.0
    elif lead_score >= 40:
        risk += 10.0

    if not has_activity:
        risk += 25.0

    if days_since_activity > 30:
        risk += 25.0
    elif days_since_activity > 14:
        risk += 18.0
    elif days_since_activity > 7:
        risk += 12.0
    elif stage == "Hot" and days_since_activity > 2:
        risk += 15.0

    if age_days > 90:
        risk += 20.0
    elif age_days > 45:
        risk += 10.0
    elif age_days > 21:
        risk += 5.0

    return round(max(0.0, min(100.0, risk)), 1)


def _next_best_action(
    stage: str,
    response_risk: float,
    days_since_activity: int,
    source_group: str,
    has_activity: bool,
) -> str:
    """Recommend a manager-facing next action."""
    if stage == "Converted":
        return "No action"
    if stage == "Disqualified":
        return "Review disqualification pattern"
    if stage == "Hot" and days_since_activity > 2:
        return "Immediate SDR follow-up"
    if response_risk >= 80:
        return "Escalate owner outreach today"
    if not has_activity:
        return "Launch first-touch outreach"
    if source_group == "Unknown":
        return "Repair source attribution"
    if stage == "Qualified":
        return "Book qualification call"
    if days_since_activity > 14:
        return "Recycle or re-sequence"
    return "Progress nurture sequence"


def create_dataset(inst: str, tok: str) -> bool:
    """Build the consolidated lead funnel dataset."""
    print(f"\n=== Building {DS_LABEL} dataset ===")
    leads = _soql(inst, tok, LEAD_SOQL)
    print(f"  Queried {len(leads)} leads")

    today = datetime.now(UTC).date()
    today_iso = today.isoformat()
    current_month = today.strftime("%Y-%m")
    last_complete_month = _add_months(current_month, -1)
    recent_start_month = _add_months(last_complete_month, -23)
    forecast_end_month = _add_months(current_month, 3)

    prepared: list[dict[str, object]] = []
    source_stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {"total": 0.0, "converted": 0.0}
    )
    region_stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {"total": 0.0, "converted": 0.0}
    )

    for lead in leads:
        created_date = (lead.get("CreatedDate") or "")[:10]
        if not created_date:
            continue

        converted_date = (lead.get("ConvertedDate") or "")[:10]
        last_activity_date = (lead.get("LastActivityDate") or "")[:10]
        is_converted = str(lead.get("IsConverted") or "").lower() == "true"
        status = (lead.get("Status") or "").strip()
        source = _normalize_source(str(lead.get("LeadSource") or ""))
        source_group = _source_group(source)
        country = ((lead.get("Country") or "Unknown").strip() or "Unknown")[:255]
        region = _region(country)
        lifecycle_stage = _lifecycle_stage(status, is_converted)
        company = ((lead.get("Company") or "").strip() or "(No Company)")[:255]
        industry = ((lead.get("Industry") or "Unknown").strip() or "Unknown")[:255]
        owner_name = (((lead.get("Owner") or {}).get("Name") or "Unassigned").strip() or "Unassigned")[:255]
        lead_name = ((lead.get("Name") or "Unnamed Lead").strip() or "Unnamed Lead")[:255]
        employees = int(safe_float(lead.get("NumberOfEmployees")))
        employee_band = _employee_band(employees)
        lead_score = round(safe_float(lead.get("pi__score__c")), 1)
        created_month = month_key(created_date)
        converted_month = month_key(converted_date)
        age_days = _days_between(created_date, today_iso)
        days_since_activity = _days_between(last_activity_date or created_date, today_iso)
        days_to_convert = _days_between(created_date, converted_date) if is_converted else 0
        has_activity = bool(last_activity_date)
        disqualified_reason = ((lead.get("Disqualified_Reason__c") or "").strip())[:255]
        campaign = (
            (
                lead.get("pi__campaign__c")
                or lead.get("pi__utm_campaign__c")
                or ""
            ).strip()
        )[:255]

        prepared_row = {
            "Id": lead.get("Id") or "",
            "LeadName": lead_name,
            "OwnerName": owner_name,
            "Company": company,
            "Country": country,
            "Region": region,
            "Industry": industry,
            "LeadSource": source,
            "SourceGroup": source_group,
            "Status": status[:255] if status else "Unknown",
            "LifecycleStage": lifecycle_stage,
            "StageLabel": _stage_label(lifecycle_stage),
            "EmployeeBand": employee_band,
            "DisqualifiedReason": disqualified_reason,
            "Campaign": campaign,
            "CreatedDate": created_date,
            "CreatedMonth": created_month,
            "LastActivityDate": last_activity_date,
            "ConvertedDate": converted_date,
            "ConvertedMonth": converted_month,
            "LeadScore": lead_score,
            "LeadAgeDays": age_days,
            "DaysSinceActivity": days_since_activity,
            "DaysToConvert": days_to_convert,
            "HasActivity": has_activity,
            "Employees": employees,
            "IsConverted": is_converted,
        }
        prepared.append(prepared_row)

        source_stats[source_group]["total"] += 1.0
        region_stats[region]["total"] += 1.0
        if is_converted:
            source_stats[source_group]["converted"] += 1.0
            region_stats[region]["converted"] += 1.0

    monthly_grouped: dict[tuple[str, str, str], dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(
            lambda: {
                "CreatedCount": 0.0,
                "EngagedCount": 0.0,
                "QualifiedCount": 0.0,
                "HotLeadCount": 0.0,
                "OpenLeadCount": 0.0,
                "ConvertedCount": 0.0,
                "DisqualifiedCount": 0.0,
                "SLABreachCount": 0.0,
                "AtRiskLeadCount": 0.0,
            }
        )
    )
    detail_rows: list[dict[str, object]] = []

    for lead in prepared:
        region = str(lead["Region"])
        source_group = str(lead["SourceGroup"])
        lifecycle_stage = str(lead["LifecycleStage"])
        created_month = str(lead["CreatedMonth"] or "")
        converted_month = str(lead["ConvertedMonth"] or "")
        key = (region, source_group, lifecycle_stage)

        source_rate_pct = round(
            _safe_rate(
                source_stats[source_group]["converted"],
                source_stats[source_group]["total"],
            )
            * 100,
            1,
        )
        region_rate_pct = round(
            _safe_rate(
                region_stats[region]["converted"],
                region_stats[region]["total"],
            )
            * 100,
            1,
        )
        lead_score = safe_float(lead["LeadScore"])
        lead_age_days = int(safe_float(lead["LeadAgeDays"]))
        days_since_activity = int(safe_float(lead["DaysSinceActivity"]))
        has_activity = bool(lead["HasActivity"])

        conversion_propensity = _conversion_propensity(
            lifecycle_stage,
            lead_score,
            source_rate_pct,
            region_rate_pct,
            lead_age_days,
            days_since_activity,
            has_activity,
        )
        response_risk = _response_risk(
            lifecycle_stage,
            lead_score,
            lead_age_days,
            days_since_activity,
            has_activity,
        )
        priority_band = _priority_band(conversion_propensity, lifecycle_stage)

        horizon_weight = {
            "Hot": 0.90,
            "Qualified": 0.70,
            "Worked": 0.50,
            "Open": 0.30,
            "Converted": 0.0,
            "Disqualified": 0.0,
        }.get(lifecycle_stage, 0.0)
        if days_since_activity > 14:
            horizon_weight *= 0.6
        elif days_since_activity > 7:
            horizon_weight *= 0.8
        if lead_age_days > 90:
            horizon_weight *= 0.6
        elif lead_age_days > 45:
            horizon_weight *= 0.8

        expected_conversion_90d = round(
            (conversion_propensity / 100.0) * horizon_weight,
            3,
        )

        engaged_count = 1 if has_activity or lifecycle_stage in {"Worked", "Qualified", "Hot", "Converted"} else 0
        qualified_count = 1 if lifecycle_stage in {"Qualified", "Hot", "Converted"} else 0
        hot_lead_count = 1 if lifecycle_stage == "Hot" else 0
        converted_count = 1 if lifecycle_stage == "Converted" else 0
        disqualified_count = 1 if lifecycle_stage == "Disqualified" else 0
        open_lead_count = 1 if lifecycle_stage not in {"Converted", "Disqualified"} else 0

        sla_limit = 2 if lifecycle_stage == "Hot" else 5 if lifecycle_stage == "Qualified" else 7
        sla_breach_count = 1 if open_lead_count and days_since_activity > sla_limit else 0
        at_risk_lead_count = 1 if open_lead_count and response_risk >= 70 else 0

        if created_month and recent_start_month <= created_month <= current_month:
            bucket = monthly_grouped[key][created_month]
            bucket["CreatedCount"] += 1.0
            bucket["EngagedCount"] += engaged_count
            bucket["QualifiedCount"] += qualified_count
            bucket["HotLeadCount"] += hot_lead_count
            bucket["OpenLeadCount"] += open_lead_count
            bucket["DisqualifiedCount"] += disqualified_count
            bucket["SLABreachCount"] += sla_breach_count
            bucket["AtRiskLeadCount"] += at_risk_lead_count

        if converted_count and converted_month and recent_start_month <= converted_month <= current_month:
            monthly_grouped[key][converted_month]["ConvertedCount"] += 1.0

        detail_rows.append(
            {
                "RecordType": "detail",
                "Id": lead["Id"],
                "LeadName": lead["LeadName"],
                "OwnerName": lead["OwnerName"],
                "Company": lead["Company"],
                "Country": lead["Country"],
                "Region": region,
                "Industry": lead["Industry"],
                "LeadSource": lead["LeadSource"],
                "SourceGroup": source_group,
                "Status": lead["Status"],
                "LifecycleStage": lifecycle_stage,
                "StageLabel": lead["StageLabel"],
                "PriorityBand": priority_band,
                "EmployeeBand": lead["EmployeeBand"],
                "DisqualifiedReason": lead["DisqualifiedReason"],
                "Campaign": lead["Campaign"],
                "NextBestAction": _next_best_action(
                    lifecycle_stage,
                    response_risk,
                    days_since_activity,
                    source_group,
                    has_activity,
                ),
                "CreatedDate": lead["CreatedDate"],
                "LastActivityDate": lead["LastActivityDate"],
                "ConvertedDate": lead["ConvertedDate"],
                "MonthDate": "",
                "MonthLabel": "",
                "LeadScore": lead_score,
                "LeadAgeDays": lead_age_days,
                "DaysSinceActivity": days_since_activity,
                "DaysToConvert": int(safe_float(lead["DaysToConvert"])),
                "Employees": int(safe_float(lead["Employees"])),
                "SourceConversionRate": source_rate_pct,
                "RegionConversionRate": region_rate_pct,
                "ConversionPropensityScore": conversion_propensity,
                "ResponseRiskScore": response_risk,
                "ExpectedConversion90d": expected_conversion_90d,
                "Target90d": 0.0,
                "CreatedCount": 1,
                "EngagedCount": engaged_count,
                "QualifiedCount": qualified_count,
                "HotLeadCount": hot_lead_count,
                "OpenLeadCount": open_lead_count,
                "ConvertedCount": converted_count,
                "DisqualifiedCount": disqualified_count,
                "SLABreachCount": sla_breach_count,
                "AtRiskLeadCount": at_risk_lead_count,
                "ForecastConverted": 0.0,
                "ForecastConverted_high_95": 0.0,
                "ForecastConverted_low_95": 0.0,
            }
        )

    trend_rows: list[dict[str, object]] = []
    trend_months = month_sequence(recent_start_month, forecast_end_month)
    historical_months = [month for month in trend_months if month <= last_complete_month]
    recent_target_months = historical_months[-3:] if len(historical_months) >= 3 else historical_months

    for (region, source_group, lifecycle_stage), monthly_data in monthly_grouped.items():
        converted_series = [monthly_data[month]["ConvertedCount"] for month in historical_months]
        fit = least_squares(converted_series)
        target_90d = round(sum(monthly_data[month]["ConvertedCount"] for month in recent_target_months), 2)

        for index, month in enumerate(trend_months):
            values = monthly_data[month]
            forecast = max(0.0, fit["intercept"] + fit["slope"] * index)
            interval = prediction_interval(fit, index)
            trend_rows.append(
                {
                    "RecordType": "trend",
                    "Id": "",
                    "LeadName": "",
                    "OwnerName": "",
                    "Company": "",
                    "Country": "",
                    "Region": region,
                    "Industry": "",
                    "LeadSource": "",
                    "SourceGroup": source_group,
                    "Status": "",
                    "LifecycleStage": lifecycle_stage,
                    "StageLabel": _stage_label(lifecycle_stage),
                    "PriorityBand": "",
                    "EmployeeBand": "",
                    "DisqualifiedReason": "",
                    "Campaign": "",
                    "NextBestAction": "",
                    "CreatedDate": "",
                    "LastActivityDate": "",
                    "ConvertedDate": "",
                    "MonthDate": f"{month}-01",
                    "MonthLabel": month,
                    "LeadScore": 0.0,
                    "LeadAgeDays": 0,
                    "DaysSinceActivity": 0,
                    "DaysToConvert": 0,
                    "Employees": 0,
                    "SourceConversionRate": 0.0,
                    "RegionConversionRate": 0.0,
                    "ConversionPropensityScore": 0.0,
                    "ResponseRiskScore": 0.0,
                    "ExpectedConversion90d": 0.0,
                    "Target90d": target_90d if month == current_month else 0.0,
                    "CreatedCount": int(values["CreatedCount"]),
                    "EngagedCount": int(values["EngagedCount"]),
                    "QualifiedCount": int(values["QualifiedCount"]),
                    "HotLeadCount": int(values["HotLeadCount"]),
                    "OpenLeadCount": int(values["OpenLeadCount"]),
                    "ConvertedCount": int(values["ConvertedCount"]),
                    "DisqualifiedCount": int(values["DisqualifiedCount"]),
                    "SLABreachCount": int(values["SLABreachCount"]),
                    "AtRiskLeadCount": int(values["AtRiskLeadCount"]),
                    "ForecastConverted": round(forecast, 2),
                    "ForecastConverted_high_95": round(max(0.0, forecast + interval), 2),
                    "ForecastConverted_low_95": round(max(0.0, forecast - interval), 2),
                }
            )

    rows = detail_rows + trend_rows
    print(f"  Detail rows: {len(detail_rows)}")
    print(f"  Trend rows: {len(trend_rows)}")
    print(f"  Total rows: {len(rows)}")

    field_names = [
        "RecordType",
        "Id",
        "LeadName",
        "OwnerName",
        "Company",
        "Country",
        "Region",
        "Industry",
        "LeadSource",
        "SourceGroup",
        "Status",
        "LifecycleStage",
        "StageLabel",
        "PriorityBand",
        "EmployeeBand",
        "DisqualifiedReason",
        "Campaign",
        "NextBestAction",
        "CreatedDate",
        "LastActivityDate",
        "ConvertedDate",
        "MonthDate",
        "MonthLabel",
        "LeadScore",
        "LeadAgeDays",
        "DaysSinceActivity",
        "DaysToConvert",
        "Employees",
        "SourceConversionRate",
        "RegionConversionRate",
        "ConversionPropensityScore",
        "ResponseRiskScore",
        "ExpectedConversion90d",
        "Target90d",
        "CreatedCount",
        "EngagedCount",
        "QualifiedCount",
        "HotLeadCount",
        "OpenLeadCount",
        "ConvertedCount",
        "DisqualifiedCount",
        "SLABreachCount",
        "AtRiskLeadCount",
        "ForecastConverted",
        "ForecastConverted_high_95",
        "ForecastConverted_low_95",
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
        _dim("Id", "Lead ID"),
        _dim("LeadName", "Lead"),
        _dim("OwnerName", "Owner"),
        _dim("Company", "Company"),
        _dim("Country", "Country"),
        _dim("Region", "Region"),
        _dim("Industry", "Industry"),
        _dim("LeadSource", "Lead Source"),
        _dim("SourceGroup", "Source Group"),
        _dim("Status", "Status"),
        _dim("LifecycleStage", "Lifecycle Stage"),
        _dim("StageLabel", "Stage Label"),
        _dim("PriorityBand", "Priority Band"),
        _dim("EmployeeBand", "Employee Band"),
        _dim("DisqualifiedReason", "Disqualified Reason"),
        _dim("Campaign", "Campaign"),
        _dim("NextBestAction", "Next Best Action"),
        _date("CreatedDate", "Created Date"),
        _date("LastActivityDate", "Last Activity Date"),
        _date("ConvertedDate", "Converted Date"),
        _date("MonthDate", "Month"),
        _dim("MonthLabel", "Month Label"),
        _measure("LeadScore", "Lead Score", scale=1, precision=5),
        _measure("LeadAgeDays", "Lead Age (Days)", scale=0, precision=6),
        _measure("DaysSinceActivity", "Days Since Activity", scale=0, precision=6),
        _measure("DaysToConvert", "Days to Convert", scale=0, precision=6),
        _measure("Employees", "Employees", scale=0, precision=8),
        _measure("SourceConversionRate", "Source Conversion Rate", scale=1, precision=5),
        _measure("RegionConversionRate", "Region Conversion Rate", scale=1, precision=5),
        _measure("ConversionPropensityScore", "Conversion Propensity Score", scale=1, precision=5),
        _measure("ResponseRiskScore", "Response Risk Score", scale=1, precision=5),
        _measure("ExpectedConversion90d", "Expected Conversion 90d", scale=3, precision=8),
        _measure("Target90d", "Target 90d", scale=1, precision=8),
        _measure("CreatedCount", "Created Count", scale=0, precision=6),
        _measure("EngagedCount", "Engaged Count", scale=0, precision=6),
        _measure("QualifiedCount", "Qualified Count", scale=0, precision=6),
        _measure("HotLeadCount", "Hot Lead Count", scale=0, precision=6),
        _measure("OpenLeadCount", "Open Lead Count", scale=0, precision=6),
        _measure("ConvertedCount", "Converted Count", scale=0, precision=6),
        _measure("DisqualifiedCount", "Disqualified Count", scale=0, precision=6),
        _measure("SLABreachCount", "SLA Breach Count", scale=0, precision=6),
        _measure("AtRiskLeadCount", "At-Risk Lead Count", scale=0, precision=6),
        _measure("ForecastConverted", "Forecast Converted", scale=2, precision=8),
        _measure("ForecastConverted_high_95", "Forecast Converted High 95", scale=2, precision=8),
        _measure("ForecastConverted_low_95", "Forecast Converted Low 95", scale=2, precision=8),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_region = coalesce_filter("f_region", "Region")
    filter_source = coalesce_filter("f_source", "SourceGroup")
    filter_stage = coalesce_filter("f_stage", "LifecycleStage")

    detail = (
        load
        + 'q = filter q by RecordType == "detail";\n'
        + f'q = filter q by MonthLabel >= "{CURRENT_MONTH_START}" and MonthLabel <= "{CURRENT_MONTH_END}";\n'
        + filter_region
        + filter_source
        + filter_stage
    )
    trend = (
        load
        + 'q = filter q by RecordType == "trend";\n'
        + f'q = filter q by MonthLabel >= "{CURRENT_MONTH_START}" and MonthLabel <= "{CURRENT_MONTH_END}";\n'
        + filter_region
        + filter_source
        + filter_stage
    )

    q1_filters = _rebind(filter_region, "q1") + _rebind(filter_source, "q1") + _rebind(filter_stage, "q1")
    q2_filters = _rebind(filter_region, "q2") + _rebind(filter_source, "q2") + _rebind(filter_stage, "q2")
    q3_filters = _rebind(filter_region, "q3") + _rebind(filter_source, "q3") + _rebind(filter_stage, "q3")
    q4_filters = _rebind(filter_region, "q4") + _rebind(filter_source, "q4") + _rebind(filter_stage, "q4")

    summary = (
        f'q1 = load "{DS}";\n'
        'q1 = filter q1 by RecordType == "detail";\n'
        + q1_filters
        + "q1 = group q1 by all;\n"
        + "q1 = foreach q1 generate "
        + "sum(OpenLeadCount) as open_leads, "
        + "sum(ExpectedConversion90d) as projected_90d, "
        + "sum(SLABreachCount) as sla_breaches;\n"
        + f'q2 = load "{DS}";\n'
        + 'q2 = filter q2 by RecordType == "trend";\n'
        + q2_filters
        + "q2 = group q2 by all;\n"
        + "q2 = foreach q2 generate sum(Target90d) as target_90d;\n"
        + "q = cogroup q1 by all, q2 by all;\n"
        + "q = foreach q generate "
        + "coalesce(sum(q1.open_leads), 0) as open_leads, "
        + "coalesce(sum(q1.projected_90d), 0) as projected_90d, "
        + "coalesce(sum(q1.sla_breaches), 0) as sla_breaches, "
        + "case when coalesce(sum(q2.target_90d), 0) > 0 then coalesce(sum(q2.target_90d), 0) else coalesce(sum(q1.projected_90d), 0) end as target_90d, "
        + "(case when coalesce(sum(q2.target_90d), 0) > 0 then coalesce(sum(q2.target_90d), 0) else coalesce(sum(q1.projected_90d), 0) end) * 0.90 as good, "
        + "(case when coalesce(sum(q2.target_90d), 0) > 0 then coalesce(sum(q2.target_90d), 0) else coalesce(sum(q1.projected_90d), 0) end) * 0.75 as satisfactory;"
    )

    funnel = (
        f'q1 = load "{DS}";\n'
        'q1 = filter q1 by RecordType == "detail";\n'
        + q1_filters
        + "q1 = group q1 by all;\n"
        + 'q1 = foreach q1 generate "1 - Created" as Stage, sum(CreatedCount) as LeadCount;\n'
        + f'q2 = load "{DS}";\n'
        + 'q2 = filter q2 by RecordType == "detail";\n'
        + q2_filters
        + "q2 = group q2 by all;\n"
        + 'q2 = foreach q2 generate "2 - Engaged" as Stage, sum(EngagedCount) as LeadCount;\n'
        + f'q3 = load "{DS}";\n'
        + 'q3 = filter q3 by RecordType == "detail";\n'
        + q3_filters
        + "q3 = group q3 by all;\n"
        + 'q3 = foreach q3 generate "3 - Qualified" as Stage, sum(QualifiedCount) as LeadCount;\n'
        + f'q4 = load "{DS}";\n'
        + 'q4 = filter q4 by RecordType == "detail";\n'
        + q4_filters
        + "q4 = group q4 by all;\n"
        + 'q4 = foreach q4 generate "4 - Converted" as Stage, sum(ConvertedCount) as LeadCount;\n'
        + "q = union q1, q2, q3, q4;\n"
        + "q = order q by LeadCount desc;"
    )

    return {
        "f_region": af("Region", ds_meta),
        "f_source": af("SourceGroup", ds_meta),
        "f_stage": af("LifecycleStage", ds_meta),
        "s_summary": sq(summary),
        "s_funnel": sq(funnel),
        "s_monthly_trajectory": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(CreatedCount) as CreatedCount, "
            + "sum(ConvertedCount) as ConvertedCount, "
            + "sum(ForecastConverted) as ForecastConverted, "
            + "sum(ForecastConverted_high_95) as ForecastConverted_high_95, "
            + "sum(ForecastConverted_low_95) as ForecastConverted_low_95;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_source_mix": sq(
            detail
            + "q = group q by SourceGroup;\n"
            + "q = foreach q generate SourceGroup, "
            + "sum(CreatedCount) as CreatedCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(ConvertedCount) as ConvertedCount;\n"
            + "q = order q by CreatedCount desc;\n"
            + "q = limit q 8;"
        ),
        "s_conversion_volume": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(CreatedCount) as CreatedCount, "
            + "sum(EngagedCount) as EngagedCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(ConvertedCount) as ConvertedCount;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_response_trend": sq(
            trend
            + "q = group q by (MonthDate, MonthLabel);\n"
            + "q = foreach q generate MonthDate, MonthLabel, "
            + "sum(OpenLeadCount) as OpenLeadCount, "
            + "sum(SLABreachCount) as SLABreachCount, "
            + "sum(AtRiskLeadCount) as AtRiskLeadCount;\n"
            + "q = order q by MonthDate asc;"
        ),
        "s_stage_dwell": sq(
            detail
            + "q = group q by StageLabel;\n"
            + "q = foreach q generate StageLabel, "
            + "avg(LeadAgeDays) as AvgLeadAgeDays, "
            + "avg(DaysSinceActivity) as AvgDaysSinceActivity, "
            + "case when sum(ConvertedCount) > 0 then sum(DaysToConvert) / sum(ConvertedCount) else 0 end as AvgDaysToConvert;\n"
            + "q = order q by StageLabel asc;"
        ),
        "s_source_performance": sq(
            detail
            + "q = group q by SourceGroup;\n"
            + "q = foreach q generate SourceGroup, "
            + "sum(CreatedCount) as CreatedCount, "
            + "sum(ConvertedCount) as ConvertedCount, "
            + "case when sum(CreatedCount) > 0 then (sum(ConvertedCount) / sum(CreatedCount)) * 100 else 0 end as ConversionRate;\n"
            + "q = order q by CreatedCount desc;\n"
            + "q = limit q 8;"
        ),
        "s_region_performance": sq(
            detail
            + "q = group q by Region;\n"
            + "q = foreach q generate Region, "
            + "sum(OpenLeadCount) as OpenLeadCount, "
            + "sum(ConvertedCount) as ConvertedCount, "
            + "sum(AtRiskLeadCount) as AtRiskLeadCount;\n"
            + "q = order q by OpenLeadCount desc;"
        ),
        "s_priority_performance": sq(
            detail
            + 'q = filter q by LifecycleStage != "Converted";\n'
            + 'q = filter q by LifecycleStage != "Disqualified";\n'
            + "q = group q by PriorityBand;\n"
            + "q = foreach q generate PriorityBand, "
            + "sum(OpenLeadCount) as OpenLeadCount, "
            + "sum(AtRiskLeadCount) as AtRiskLeadCount, "
            + "sum(ExpectedConversion90d) as ExpectedConversion90d;\n"
            + "q = order q by ExpectedConversion90d desc;"
        ),
        "s_dq_reason": sq(
            detail
            + 'q = filter q by LifecycleStage == "Disqualified";\n'
            + 'q = filter q by DisqualifiedReason != "";\n'
            + "q = group q by DisqualifiedReason;\n"
            + "q = foreach q generate DisqualifiedReason, count() as LeadCount;\n"
            + "q = order q by LeadCount desc;\n"
            + "q = limit q 10;"
        ),
        "s_exception_summary": sq(
            detail
            + 'q = filter q by LifecycleStage != "Converted";\n'
            + 'q = filter q by LifecycleStage != "Disqualified";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(case when ResponseRiskScore >= 70 and ConversionPropensityScore >= 50 then 1 else 0 end) as high_risk_high_potential, "
            + 'sum(case when LifecycleStage == "Hot" and DaysSinceActivity > 2 then 1 else 0 end) as hot_no_activity, '
            + "sum(SLABreachCount) as sla_breach_count;"
        ),
        "s_owner_queue": sq(
            detail
            + 'q = filter q by LifecycleStage != "Converted";\n'
            + 'q = filter q by LifecycleStage != "Disqualified";\n'
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(SLABreachCount) as SLABreachCount, "
            + "sum(AtRiskLeadCount) as AtRiskLeadCount, "
            + "sum(ExpectedConversion90d) as ExpectedConversion90d;\n"
            + "q = order q by AtRiskLeadCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_top_response_risk": sq(
            detail
            + 'q = filter q by LifecycleStage != "Converted";\n'
            + 'q = filter q by LifecycleStage != "Disqualified";\n'
            + "q = filter q by ResponseRiskScore >= 70;\n"
            + "q = group q by (LeadName, Company, OwnerName, Region, PriorityBand, NextBestAction, Id);\n"
            + "q = foreach q generate LeadName, Company, OwnerName, Region, PriorityBand, NextBestAction, "
            + "max(DaysSinceActivity) as DaysSinceActivity, "
            + "max(ResponseRiskScore) as ResponseRiskScore, "
            + "max(ConversionPropensityScore) as ConversionPropensityScore, "
            + "Id;\n"
            + "q = order q by ResponseRiskScore desc;\n"
            + "q = limit q 15;"
        ),
        "s_top_stalled": sq(
            detail
            + 'q = filter q by LifecycleStage != "Converted";\n'
            + 'q = filter q by LifecycleStage != "Disqualified";\n'
            + "q = filter q by ConversionPropensityScore >= 50;\n"
            + "q = filter q by (LeadAgeDays > 14) or (SLABreachCount > 0);\n"
            + "q = group q by (LeadName, Company, OwnerName, SourceGroup, PriorityBand, NextBestAction, Id);\n"
            + "q = foreach q generate LeadName, Company, OwnerName, SourceGroup, PriorityBand, NextBestAction, "
            + "max(LeadAgeDays) as LeadAgeDays, "
            + "max(DaysSinceActivity) as DaysSinceActivity, "
            + "max(ConversionPropensityScore) as ConversionPropensityScore, "
            + "max(ExpectedConversion90d) as ExpectedConversion90d, "
            + "Id;\n"
            + "q = order q by ConversionPropensityScore desc;\n"
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
            "Lead Funnel",
            "Manager surface for lead intake, qualification, forecasted conversion, and response risk.",
        ),
        "p1_f_region": pillbox("f_region", "Region"),
        "p1_f_source": pillbox("f_source", "Source Group"),
        "p1_f_stage": pillbox("f_stage", "Lifecycle Stage"),
        "p1_n_open": num("s_summary", "open_leads", "Open Leads", "#032D60", compact=True),
        "p1_n_projected": num(
            "s_summary",
            "projected_90d",
            "Projected 90d Conversions",
            "#0176D3",
            compact=True,
        ),
        "p1_n_sla": num(
            "s_summary",
            "sla_breaches",
            "SLA Breaches",
            "#BA0517",
            compact=True,
        ),
        "p1_ch_funnel": funnel_chart("s_funnel", "Lead Funnel", "Stage", "LeadCount"),
        "p1_ch_bullet": bullet_chart(
            "s_summary",
            "Projected 90d Conversions vs Recent Benchmark",
            axis_title="Leads",
        ),
        "p1_ch_source": rich_chart(
            "s_source_mix",
            "stackcolumn",
            "Source Mix: Created, Qualified, Converted",
            ["SourceGroup"],
            ["CreatedCount", "QualifiedCount", "ConvertedCount"],
            show_legend=True,
            axis_title="Leads",
            show_values=True,
        ),
        "p1_ch_timeline": timeline_chart(
            "s_monthly_trajectory",
            "Monthly Intake, Conversion, and Forecast",
            show_legend=True,
            axis_title="Leads",
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("trend", "Trend & Forecast", active=True),
        "p2_nav3": nav_link("drivers", "Drivers & Segments"),
        "p2_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p2_hdr": hdr(
            "Trend & Forecast",
            "How intake, qualification, response pressure, and expected conversion are moving over time.",
        ),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_f_source": pillbox("f_source", "Source Group"),
        "p2_f_stage": pillbox("f_stage", "Lifecycle Stage"),
        "p2_ch_volume": rich_chart(
            "s_conversion_volume",
            "stackcolumn",
            "Monthly Volume Through Funnel",
            ["MonthDate"],
            ["CreatedCount", "EngagedCount", "QualifiedCount", "ConvertedCount"],
            show_legend=True,
            axis_title="Leads",
        ),
        "p2_ch_forecast": timeline_chart(
            "s_monthly_trajectory",
            "Converted Leads with Forecast Band",
            show_legend=True,
            axis_title="Leads",
        ),
        "p2_ch_response": rich_chart(
            "s_response_trend",
            "line",
            "Open Backlog, SLA Breaches, and Response Risk",
            ["MonthDate"],
            ["OpenLeadCount", "SLABreachCount", "AtRiskLeadCount"],
            show_legend=True,
            axis_title="Leads",
        ),
        "p2_ch_dwell": rich_chart(
            "s_stage_dwell",
            "hbar",
            "Stage Dwell and Response Latency",
            ["StageLabel"],
            ["AvgLeadAgeDays", "AvgDaysSinceActivity", "AvgDaysToConvert"],
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
            "Where conversion quality, backlog risk, and disqualification pressure are concentrated.",
        ),
        "p3_f_region": pillbox("f_region", "Region"),
        "p3_f_source": pillbox("f_source", "Source Group"),
        "p3_f_stage": pillbox("f_stage", "Lifecycle Stage"),
        "p3_ch_source_perf": combo_chart(
            "s_source_performance",
            "Source Performance: Volume and Conversion Rate",
            ["SourceGroup"],
            ["CreatedCount", "ConvertedCount"],
            ["ConversionRate"],
            show_legend=True,
            axis_title="Leads",
            axis2_title="Conversion Rate %",
        ),
        "p3_ch_region": rich_chart(
            "s_region_performance",
            "stackhbar",
            "Region Backlog, Conversion, and Risk",
            ["Region"],
            ["OpenLeadCount", "ConvertedCount", "AtRiskLeadCount"],
            show_legend=True,
            axis_title="Leads",
            show_values=True,
        ),
        "p3_ch_priority": rich_chart(
            "s_priority_performance",
            "stackcolumn",
            "Open Pipeline by Priority Band",
            ["PriorityBand"],
            ["OpenLeadCount", "AtRiskLeadCount", "ExpectedConversion90d"],
            show_legend=True,
            axis_title="Leads",
            show_values=True,
        ),
        "p3_ch_dq": rich_chart(
            "s_dq_reason",
            "hbar",
            "Top Disqualification Reasons",
            ["DisqualifiedReason"],
            ["LeadCount"],
            axis_title="Leads",
            show_values=True,
        ),
        "p4_nav1": nav_link("summary", "Summary"),
        "p4_nav2": nav_link("trend", "Trend & Forecast"),
        "p4_nav3": nav_link("drivers", "Drivers & Segments"),
        "p4_nav4": nav_link("exceptions", "Exceptions & Actions", active=True),
        "p4_hdr": hdr(
            "Exceptions & Actions",
            "The leads and owners that need intervention before conversion probability decays further.",
        ),
        "p4_f_region": pillbox("f_region", "Region"),
        "p4_f_source": pillbox("f_source", "Source Group"),
        "p4_f_stage": pillbox("f_stage", "Lifecycle Stage"),
        "p4_n_risk": num(
            "s_exception_summary",
            "high_risk_high_potential",
            "High-Risk High-Potential Leads",
            "#8E030F",
            compact=True,
        ),
        "p4_n_hot": num(
            "s_exception_summary",
            "hot_no_activity",
            "Hot Leads Without Recent Activity",
            "#BA0517",
            compact=True,
        ),
        "p4_ch_owner": rich_chart(
            "s_owner_queue",
            "stackhbar",
            "Owner Action Queue",
            ["OwnerName"],
            ["SLABreachCount", "AtRiskLeadCount", "ExpectedConversion90d"],
            show_legend=True,
            axis_title="Leads",
            show_values=True,
        ),
        "p4_tbl_risk": rich_chart(
            "s_top_response_risk",
            "comparisontable",
            "Top Response-Risk Leads",
            ["LeadName", "Company", "OwnerName", "Region", "PriorityBand", "NextBestAction"],
            ["DaysSinceActivity", "ResponseRiskScore", "ConversionPropensityScore"],
            show_legend=False,
        ),
        "p4_tbl_stalled": rich_chart(
            "s_top_stalled",
            "comparisontable",
            "Top Stalled High-Potential Leads",
            ["LeadName", "Company", "OwnerName", "SourceGroup", "PriorityBand", "NextBestAction"],
            ["LeadAgeDays", "DaysSinceActivity", "ConversionPropensityScore", "ExpectedConversion90d"],
            show_legend=False,
        ),
    }

    widgets["p3_ch_source_perf"]["parameters"].pop("columnMap", None)
    add_table_action(widgets["p4_tbl_risk"], "salesforceActions", "Lead", "Id")
    add_table_action(widgets["p4_tbl_stalled"], "salesforceActions", "Lead", "Id")
    return widgets


def legacy_build_layout() -> dict:
    """Build the 4-page manager dashboard layout."""
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_source", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_stage", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_open", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_projected", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_sla", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_ch_funnel", "row": 9, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p1_ch_bullet", "row": 9, "column": 4, "colspan": 4, "rowspan": 6},
        {"name": "p1_ch_source", "row": 9, "column": 8, "colspan": 4, "rowspan": 6},
        {"name": "p1_ch_timeline", "row": 15, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_source", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_stage", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_volume", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_forecast", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_response", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_dwell", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_region", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_source", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_stage", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_ch_source_perf", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_region", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_priority", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_dq", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_region", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_source", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_stage", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p4_n_risk", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_hot", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p4_ch_owner", "row": 5, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p4_tbl_risk", "row": 11, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p4_tbl_stalled", "row": 18, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "LeadFunnel",
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
    filter_region = coalesce_filter("f_region", "Region")
    filter_source = coalesce_filter("f_source", "SourceGroup")
    filter_stage = coalesce_filter("f_stage", "LifecycleStage")

    detail = load + 'q = filter q by RecordType == "detail";\n' + filter_region + filter_source + filter_stage
    trend = load + 'q = filter q by RecordType == "trend";\n' + filter_region + filter_source + filter_stage

    steps.update(
        {
            "s_source_status_flow": sq(
                detail
                + "q = group q by (SourceGroup, LifecycleStage);\n"
                + "q = foreach q generate SourceGroup as source, LifecycleStage as target, count() as flow;\n"
                + "q = order q by flow desc;\n"
                + "q = limit q 30;"
            ),
            "s_source_month_heatmap": sq(
                trend
                + "q = group q by (SourceGroup, MonthLabel);\n"
                + "q = foreach q generate SourceGroup, MonthLabel, sum(CreatedCount) as LeadCount;\n"
                + "q = order q by LeadCount desc;"
            ),
            "s_priority_scatter": sq(
                detail
                + 'q = filter q by LifecycleStage != "Converted";\n'
                + 'q = filter q by LifecycleStage != "Disqualified";\n'
                + "q = group q by (LeadName, OwnerName, PriorityBand, Id);\n"
                + "q = foreach q generate LeadName, OwnerName, PriorityBand, "
                + "max(ConversionPropensityScore) as ConversionPropensityScore, "
                + "max(ResponseRiskScore) as ResponseRiskScore, "
                + "max(ExpectedConversion90d) as ExpectedConversion90d, "
                + "Id;\n"
                + "q = order q by ResponseRiskScore desc;\n"
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
            "Lead Funnel",
            f"{CURRENT_YEAR} manager operating view for lead flow, forecasted conversion, and response risk.",
        ),
        "p1_f_region": pillbox("f_region", "Region"),
        "p1_f_source": pillbox("f_source", "Source Group"),
        "p1_f_stage": pillbox("f_stage", "Lifecycle Stage"),
        "p1_n_open": num("s_summary", "open_leads", "Open Leads", "#032D60", compact=True),
        "p1_n_projected": num(
            "s_summary",
            "projected_90d",
            "Projected 90d Conversions",
            "#0176D3",
            compact=True,
        ),
        "p1_n_sla": num("s_summary", "sla_breaches", "SLA Breaches", "#BA0517", compact=True),
        "p1_ch_funnel": funnel_chart("s_funnel", f"{CURRENT_YEAR} Lead Funnel", "Stage", "LeadCount"),
        "p1_ch_source": rich_chart(
            "s_source_mix",
            "stackcolumn",
            "Source Mix: Created, Qualified, Converted",
            ["SourceGroup"],
            ["CreatedCount", "QualifiedCount", "ConvertedCount"],
            show_legend=True,
            axis_title="Leads",
            show_values=True,
        ),
        "p1_ch_timeline": timeline_chart(
            "s_monthly_trajectory",
            f"{CURRENT_YEAR} Monthly Intake, Conversion, and Forecast",
            show_legend=True,
            axis_title="Leads",
        ),
        "p1_ch_flow": sankey_chart(
            "s_source_status_flow",
            "Lead Flow: Source -> Lifecycle Stage",
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("trend", "Trend & Forecast", active=True),
        "p2_nav3": nav_link("drivers", "Drivers & Segments"),
        "p2_nav4": nav_link("exceptions", "Exceptions & Actions"),
        "p2_hdr": hdr(
            "Trend & Forecast",
            f"How {CURRENT_YEAR} intake, qualification, response pressure, and expected conversion are moving over time.",
        ),
        "p2_f_region": pillbox("f_region", "Region"),
        "p2_f_source": pillbox("f_source", "Source Group"),
        "p2_f_stage": pillbox("f_stage", "Lifecycle Stage"),
        "p2_ch_volume": combo_chart(
            "s_conversion_volume",
            f"{CURRENT_YEAR} Intake, Qualified, and Converted",
            ["MonthDate"],
            ["CreatedCount", "QualifiedCount"],
            ["ConvertedCount"],
            show_legend=True,
            axis_title="Leads",
            axis2_title="Converted Leads",
            subtitle="Columns show monthly intake and qualified throughput; line shows converted output.",
        ),
        "p2_ch_forecast": timeline_chart(
            "s_monthly_trajectory",
            f"{CURRENT_YEAR} Converted Leads with Forecast Band",
            show_legend=True,
            axis_title="Leads",
        ),
        "p2_ch_response": rich_chart(
            "s_response_trend",
            "line",
            "Open Backlog, SLA Breaches, and Response Risk",
            ["MonthDate"],
            ["OpenLeadCount", "SLABreachCount", "AtRiskLeadCount"],
            show_legend=True,
            axis_title="Leads",
        ),
        "p2_ch_dwell": rich_chart(
            "s_stage_dwell",
            "hbar",
            "Stage Dwell and Response Latency",
            ["StageLabel"],
            ["AvgLeadAgeDays", "AvgDaysSinceActivity", "AvgDaysToConvert"],
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
            "Where source quality, backlog risk, and disqualification pressure are concentrated.",
        ),
        "p3_f_region": pillbox("f_region", "Region"),
        "p3_f_source": pillbox("f_source", "Source Group"),
        "p3_f_stage": pillbox("f_stage", "Lifecycle Stage"),
        "p3_ch_source_perf": combo_chart(
            "s_source_performance",
            "Source Performance: Volume and Conversion Rate",
            ["SourceGroup"],
            ["CreatedCount", "ConvertedCount"],
            ["ConversionRate"],
            show_legend=True,
            axis_title="Leads",
            axis2_title="Conversion Rate %",
        ),
        "p3_ch_heatmap": heatmap_chart(
            "s_source_month_heatmap",
            "Lead Volume by Source x Month",
            show_legend=True,
        ),
        "p3_ch_region": rich_chart(
            "s_region_performance",
            "stackhbar",
            "Region Backlog, Conversion, and Risk",
            ["Region"],
            ["OpenLeadCount", "ConvertedCount", "AtRiskLeadCount"],
            show_legend=True,
            axis_title="Leads",
            show_values=True,
        ),
        "p3_ch_dq": rich_chart(
            "s_dq_reason",
            "hbar",
            "Top Disqualification Reasons",
            ["DisqualifiedReason"],
            ["LeadCount"],
            axis_title="Leads",
            show_values=True,
        ),
        "p4_nav1": nav_link("summary", "Summary"),
        "p4_nav2": nav_link("trend", "Trend & Forecast"),
        "p4_nav3": nav_link("drivers", "Drivers & Segments"),
        "p4_nav4": nav_link("exceptions", "Exceptions & Actions", active=True),
        "p4_hdr": hdr(
            "Exceptions & Actions",
            "The leads and owners that need intervention before conversion probability decays further.",
        ),
        "p4_f_region": pillbox("f_region", "Region"),
        "p4_f_source": pillbox("f_source", "Source Group"),
        "p4_f_stage": pillbox("f_stage", "Lifecycle Stage"),
        "p4_n_risk": num(
            "s_exception_summary",
            "high_risk_high_potential",
            "High-Risk High-Potential Leads",
            "#8E030F",
            compact=True,
        ),
        "p4_n_hot": num(
            "s_exception_summary",
            "hot_no_activity",
            "Hot Leads Without Recent Activity",
            "#BA0517",
            compact=True,
        ),
        "p4_ch_scatter": bubble_chart(
            "s_priority_scatter",
            "Conversion Propensity vs Response Risk",
            show_legend=False,
        ),
        "p4_tbl_risk": rich_chart(
            "s_top_response_risk",
            "comparisontable",
            "Top Response-Risk Leads",
            ["LeadName", "Company", "OwnerName", "Region", "PriorityBand", "NextBestAction"],
            ["DaysSinceActivity", "ResponseRiskScore", "ConversionPropensityScore"],
            show_legend=False,
        ),
        "p4_tbl_stalled": rich_chart(
            "s_top_stalled",
            "comparisontable",
            "Top Stalled High-Potential Leads",
            ["LeadName", "Company", "OwnerName", "SourceGroup", "PriorityBand", "NextBestAction"],
            ["LeadAgeDays", "DaysSinceActivity", "ConversionPropensityScore", "ExpectedConversion90d"],
            show_legend=False,
        ),
    }

    widgets["p3_ch_source_perf"]["parameters"].pop("columnMap", None)
    add_table_action(widgets["p4_tbl_risk"], "salesforceActions", "Lead", "Id")
    add_table_action(widgets["p4_tbl_stalled"], "salesforceActions", "Lead", "Id")
    return widgets


def build_layout() -> dict:
    """Build the 4-page manager dashboard layout."""
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_region", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_source", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_stage", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p1_n_open", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_projected", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_n_sla", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_ch_funnel", "row": 9, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p1_ch_source", "row": 9, "column": 4, "colspan": 8, "rowspan": 6},
        {"name": "p1_ch_timeline", "row": 15, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_flow", "row": 23, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_region", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_source", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_stage", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p2_ch_volume", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_forecast", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_response", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_dwell", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_region", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_source", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_stage", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p3_ch_source_perf", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_heatmap", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_region", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_dq", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_region", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_source", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p4_f_stage", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        {"name": "p4_n_risk", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_hot", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p4_ch_scatter", "row": 5, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p4_tbl_risk", "row": 11, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p4_tbl_stalled", "row": 18, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "LeadFunnel",
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
    state = build_dashboard_state(steps, widgets, layout)

    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    print(f"\n=== Deploying {DASHBOARD_LABEL} ===")
    deploy_dashboard(instance_url, token, dashboard_id, state)

    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "LeadName", "id_field": "Id", "label": "Lead"},
        ],
    )


if __name__ == "__main__":
    main()
