#!/usr/bin/env python3
"""Build the BDR Manager and BDR Rep Queue dashboards.

Uses the org's explicit Business Development titles plus the regional
AMERS/APAC/EMEA BDR task queues to define the BDR operating population.
The dataset blends lead, task, event, campaign member, and converted
opportunity data into BDR-specific operating views.
"""

from __future__ import annotations

import csv
import io
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

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
    create_dashboard_if_needed,
    deploy_dashboard,
    flat_gauge,
    get_auth,
    get_dataset_id,
    hdr,
    heatmap_chart,
    kpi_style,
    line_chart,
    nav_link,
    nav_row,
    num,
    pg,
    pillbox,
    rich_chart,
    section_label,
    set_record_links_xmd,
    sq,
    upload_dataset,
)
from portfolio_foundation import safe_float

DS = "BDR_Operating_Rhythm"
DS_LABEL = "BDR Operating Rhythm"
MANAGER_LABEL = "BDR Manager"
REP_LABEL = "BDR Rep Queue"
CONTROL_LABEL = "BDR Campaign & Target Control"
START_DATE = "2025-01-01T00:00:00Z"
FY2025_START = "2025-01-01"
FY2026_START = "2026-01-01"
FY2027_START = "2027-01-01"

# -- Consulting-grade KPI isolation: KPIs respond to filters only, not chart cross-clicks --
MANAGER_KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_team", "f_owner", "f_source"],
    },
}

REP_KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_team", "f_owner"],
    },
}

CONTROL_KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_team", "f_owner", "f_source", "f_year"],
    },
}

BDR_USER_SOQL = (
    "SELECT Id, Name, Title, Department, Manager.Name "
    "FROM User "
    "WHERE IsActive = true AND ("
    "Title LIKE '%Business Development Representative%' OR "
    "Title LIKE '%Senior Business Development Representative%' OR "
    "Title LIKE '%Lead Business Development Representative%' OR "
    "Title LIKE '%Senior Manager Business Development%' OR "
    "Title LIKE '%Sr.Manager Business Development%') "
    "ORDER BY Department, Title, Name"
)

QUEUE_SOQL = (
    "SELECT Id, Name, Type "
    "FROM Group "
    "WHERE Name IN ('AMERS BDR Tasks', 'APAC BDR Tasks', 'EMEA BDR Tasks')"
)

LEAD_BASE_SOQL = (
    "SELECT Id, Name, Title, OwnerId, Owner.Name, CreatedById, CreatedBy.Name, CreatedDate, ConvertedDate, IsConverted, "
    "ConvertedContactId, ConvertedAccountId, ConvertedOpportunityId, Company, Country, Industry, Status, LeadSource, Dimension_Persona__c, "
    "pi__score__c, pi__campaign__c, pi__utm_campaign__c, "
    "engagio__Matched_Account__c, engagio__Matched_Account_Name__c, engagio__Matched_Account_Industry__c, "
    "engagio__Matched_Account__r.Region__c, engagio__Matched_Account__r.TAM_Universe_Segment__c, "
    "engagio__Matched_Account__r.Tier_Calculation__c "
    "FROM Lead "
)

CONTACT_SOQL = (
    "SELECT Id, Name, Title, Official_Title__c, Main_Client_Contact__c, Dimension_Persona__c, AccountId, Account.Name, "
    "Account.OwnerId, Account.Owner.Name, "
    "Account.Type, Account.Region__c, Account.TAM_Universe_Segment__c, Account.Tier_Calculation__c, "
    "Account.Finance_Client__c, Account.Ex_Customer__c, Account.SaaS_Client__c, Account.Axioma_Client__c "
    "FROM Contact "
)

ACCOUNT_SOQL = (
    "SELECT Id, Name, OwnerId, Owner.Name, Type, Industry, Region__c, TAM_Universe_Segment__c, Tier_Calculation__c, "
    "Finance_Client__c, Ex_Customer__c, SaaS_Client__c, Axioma_Client__c, Customer_Segment__c, "
    "Product_Opportunity__c, Product_Mainline__c, Heat_Map_Red_Lostdate__c, TM_Account_Status__c, "
    "Ex_Customer_Prospecting_Date__c, C_Level_Personas__c, H_Level_Personas__c, Persona_Contacts__c, Unique_Personas__c, Non_Persona_Contacts__c "
    "FROM Account "
)

OPP_SOQL = (
    "SELECT Id, Name, AccountId, LeadSource, StageName, ForecastCategoryName, Type, IsClosed, IsWon, "
    "CreatedDate, CreatedById, CreatedBy.Name, CreatedBy.Title, "
    "OwnerId, Owner.Name, Owner.Title, "
    "CloseDate, convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
    "New_Stage_15_Date__c, New_Stage_20_Date__c, "
    "New_Stage_15_Score__c, New_Stage_20_Score__c, "
    "Submit_for_Stage_20_Review__c, Submit_for_Stage_20_Review_Date__c, "
    "Stage_20_Approval__c, Stage_20_Approval_Date__c, "
    "HasOverdueTask, LastStageChangeInDays, Stage_with_Product_Scope__c, APTS_RH_Product_Family__c "
    "FROM Opportunity "
)


def _parse_date(value: object) -> date | None:
    if not value:
        return None
    text = str(value)[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _days_between(start_value: object, end_value: object) -> int:
    start_dt = _parse_date(start_value)
    end_dt = _parse_date(end_value)
    if not start_dt or not end_dt:
        return 0
    return max(0, (end_dt - start_dt).days)


def _coerce_bool(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _week_start(value: object) -> str:
    dt = _parse_date(value)
    if not dt:
        return ""
    monday = dt - timedelta(days=dt.weekday())
    return monday.isoformat()


def _month_start(value: object) -> str:
    dt = _parse_date(value)
    if not dt:
        return ""
    return dt.replace(day=1).isoformat()


def _month_label(value: object) -> str:
    dt = _parse_date(value)
    if not dt:
        return ""
    return dt.strftime("%Y-%m")


def _fy_label(value: object) -> str:
    dt = _parse_date(value)
    if not dt:
        return ""
    return f"FY{dt.year}"


def _normalize_source(value: object) -> str:
    source = (str(value or "")).strip()
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
    return source[:255]


def _source_group(source: str) -> str:
    lowered = source.lower()
    if source == "Unknown":
        return "Unknown"
    if any(
        token in lowered
        for token in ("trade show", "consensus", "cvent", "seminar", "webinar", "wbr")
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
    if any(token in lowered for token in ("partner", "promowise", "public relations")):
        return "Partner / PR"
    if lowered == "other":
        return "Other"
    return "Field / Other"


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clean_multivalue_text(value: object) -> list[str]:
    raw = _clean_text(value)
    if not raw:
        return []
    parts: list[str] = []
    for chunk in raw.replace("|", ";").split(";"):
        text = chunk.strip()
        if text:
            parts.append(text)
    return parts


def _normalize_product_token(value: object) -> str:
    token = _clean_text(value)
    if not token:
        return ""
    lowered = token.lower()
    if lowered in {"unknown", "all", "null", "-"}:
        return ""
    if "simcorp saas" in lowered or lowered == "xaas" or " xaas" in lowered:
        return "SimCorp SaaS / XaaS"
    if "scd software" in lowered:
        return "SCD Software"
    if "analytics services" in lowered:
        return "Analytics Services"
    if "data management services" in lowered:
        return "Data Management Services"
    if "white label" in lowered:
        return "White Label"
    if "3rd party" in lowered or "third party" in lowered:
        return "3rd Party"
    if lowered == "mbo":
        return "MBO"
    if lowered == "ims":
        return "IMS Mainline"
    return token[:255]


def _product_priority(token: str) -> int:
    lowered = token.lower()
    if "simcorp saas / xaas" in lowered:
        return 100
    if "scd software" in lowered:
        return 95
    if "data management services" in lowered:
        return 90
    if "analytics services" in lowered:
        return 85
    if lowered == "mbo":
        return 80
    if "ims mainline" in lowered:
        return 20
    if "white label" in lowered:
        return 10
    if "3rd party" in lowered:
        return 5
    return 60


def _primary_product_signal(value: object) -> str:
    normalized: list[str] = []
    for token in _clean_multivalue_text(value):
        clean = _normalize_product_token(token)
        if clean and clean not in normalized:
            normalized.append(clean)
    if not normalized:
        return ""
    ranked = sorted(normalized, key=lambda item: (-_product_priority(item), item))
    if (
        len(ranked) > 1
        and _product_priority(ranked[0]) >= 80
        and _product_priority(ranked[1]) >= 80
    ):
        return f"{ranked[0]} +"
    return ranked[0]


def _all_product_signals(value: object) -> list[str]:
    normalized: list[str] = []
    for token in _clean_multivalue_text(value):
        clean = _normalize_product_token(token)
        if clean and clean not in normalized:
            normalized.append(clean)
    return sorted(normalized, key=lambda item: (-_product_priority(item), item))


def _product_source_rank(source: str) -> int:
    return {
        "Opportunity Product": 5,
        "Opportunity Stage Scope": 4,
        "Account Opportunity History": 4,
        "Campaign Product": 3,
        "Account Opportunity Product": 2,
        "Account Mainline": 1,
        "Unknown": 0,
    }.get(source, 0)


def _primary_product_source(sources: set[str]) -> str:
    if not sources:
        return "Unknown"
    return sorted(sources, key=lambda item: (-_product_source_rank(item), item))[0]


def _product_signal_confidence(source: str) -> str:
    if source == "Opportunity Product":
        return "High"
    if source in {"Opportunity Stage Scope", "Account Opportunity History"}:
        return "Medium"
    if source in {"Campaign Product", "Account Opportunity Product"}:
        return "Low"
    return "Unknown"


def _product_signal_confidence_score(source: str) -> int:
    return {
        "High": 3,
        "Medium": 2,
        "Low": 1,
        "Unknown": 0,
    }.get(_product_signal_confidence(source), 0)


def _targeted_product_signal(
    campaign_product: object,
    sourced_product: object,
    opportunity_stage_scope: object,
    account_product_opportunity: object,
    account_product_mainline: object,
) -> tuple[str, str]:
    campaign = _primary_product_signal(campaign_product)
    if campaign:
        return campaign, "Campaign Product"
    opp = _primary_product_signal(sourced_product)
    if opp:
        return opp, "Opportunity Product"
    scope = _stage_scope_product_signal(opportunity_stage_scope)
    if scope:
        return scope, "Opportunity Stage Scope"
    account_specific = _primary_product_signal(account_product_opportunity)
    if account_specific:
        return account_specific, "Account Opportunity Product"
    return "Unknown", "Unknown"


def _opportunity_product_signal(
    sourced_product: object,
    account_product_opportunity: object,
    account_product_mainline: object,
) -> tuple[str, str]:
    opp = _primary_product_signal(sourced_product)
    if opp:
        return opp, "Opportunity Product"
    return "Unknown", "Unknown"


def _stage_scope_product_signal(value: object) -> str:
    token = _clean_text(value)
    if not token:
        return ""
    if " - " in token:
        _stage, token = token.split(" - ", 1)
    token = token.strip()
    if not token:
        return ""
    mapping = {
        "SCD": "SCD Scope",
        "Gain": "Gain Scope",
    }
    return mapping.get(token, f"{token} Scope")[:255]


def _industry_group(industry: object) -> str:
    text = _clean_text(industry)
    lowered = text.lower()
    if not text:
        return "Unknown"
    if lowered in {"asset management", "wealth management", "fund"}:
        return "Asset / Wealth"
    if lowered in {"pension", "asset owner"}:
        return "Asset Owner / Pension"
    if lowered in {"bank", "asset servicer"}:
        return "Bank / Servicer"
    if lowered == "insurance":
        return "Insurance"
    return text[:255]


def _team_from_department(value: object) -> str:
    department = str(value or "").lower()
    if "apac" in department:
        return "APAC"
    if "emea" in department or "value advisory" in department:
        return "EMEA"
    if (
        "na sales" in department
        or "north america" in department
        or "us (" in department
    ):
        return "AMERS"
    if "axioma" in department:
        return "Axioma"
    return "Shared BDR"


def _queue_team(queue_name: str) -> str:
    upper = queue_name.upper()
    if "APAC" in upper:
        return "APAC"
    if "EMEA" in upper:
        return "EMEA"
    if "AMERS" in upper or "NA" in upper:
        return "AMERS"
    return "Shared BDR"


def _role_level(title: str) -> str:
    if "manager" in title.lower():
        return "Manager"
    return "Rep"


def _activity_kind(task_type: str, subject: str, is_event: bool) -> str:
    if is_event:
        return "Meeting"
    type_lower = task_type.lower()
    subject_lower = subject.lower()
    if "email" in type_lower or subject_lower.startswith("email:"):
        return "Email"
    if "call" in type_lower or subject_lower == "call":
        return "Call"
    return "Other"


def _priority_band(queue_score: float) -> str:
    if queue_score >= 80:
        return "Critical"
    if queue_score >= 60:
        return "High"
    if queue_score >= 35:
        return "Medium"
    return "Low"


def _next_best_action(
    lifecycle_stage: str,
    sla_breach: bool,
    days_since_touch: int,
    has_meeting: bool,
    has_response: bool,
) -> str:
    if lifecycle_stage == "Qualified / Opportunity":
        return "Confirm AE handoff and pipeline follow-up"
    if has_response and days_since_touch > 3:
        return "Respond to engaged lead now"
    if has_meeting and days_since_touch > 7:
        return "Prep meeting or drive next-step follow-up"
    if lifecycle_stage == "Meeting Held":
        return "Convert meeting into qualified opportunity plan"
    if lifecycle_stage == "Meeting Set":
        return "Confirm attendance and prep the meeting brief"
    if sla_breach:
        return "Launch first-touch cadence today"
    if not has_response and days_since_touch > 14:
        return "Run re-engagement cadence"
    if lifecycle_stage == "Touched":
        return "Push for first meeting"
    return "Start new-touch outreach cadence"


def _suggested_tool(
    lifecycle_stage: str,
    sla_breach: bool,
    days_since_touch: int,
    has_meeting: bool,
    has_response: bool,
) -> str:
    if lifecycle_stage == "Qualified / Opportunity":
        return "AE Handoff"
    if has_meeting:
        return "Salesforce Meeting Prep"
    if has_response:
        return "Salesforce Engage"
    if sla_breach or days_since_touch > 14:
        return "Sales Engagement Basic"
    return "Salesforce Engage"


def _account_next_best_action(
    client_base_class: str,
    former_client_age_band: str,
    days_since_last_touch: int,
    open_discovery_count: int,
    pending_stage3_review_count: int,
    persona_contact_count: int,
) -> str:
    if pending_stage3_review_count > 0 or open_discovery_count > 0:
        return "Drive Stage 2 -> 3 handoff review"
    if client_base_class == "Former Client" and former_client_age_band == "2+ Years":
        return "Launch former-client re-entry play"
    if persona_contact_count == 0:
        return "Build buying-group coverage"
    if days_since_last_touch > 30:
        return "Reactivate account outreach"
    return "Run account-based outreach"


def _account_suggested_tool(
    client_base_class: str,
    former_client_age_band: str,
    open_discovery_count: int,
    pending_stage3_review_count: int,
    days_since_last_touch: int,
) -> str:
    if pending_stage3_review_count > 0 or open_discovery_count > 0:
        return "AE Handoff"
    if client_base_class == "Former Client" and former_client_age_band == "2+ Years":
        return "Salesforce Engage"
    if days_since_last_touch > 30:
        return "Sales Engagement Basic"
    return "Salesforce Engage"


def _contact_next_best_action(
    days_since_last_touch: int, meeting_held_count: int, client_base_class: str
) -> str:
    if meeting_held_count > 0:
        return "Advance follow-up with AE"
    if days_since_last_touch > 30:
        return "Re-engage dormant contact"
    if client_base_class == "Former Client":
        return "Run former-client re-entry outreach"
    return "Start persona outreach"


def _contact_suggested_tool(days_since_last_touch: int, meeting_held_count: int) -> str:
    if meeting_held_count > 0:
        return "Salesforce Meeting Prep"
    if days_since_last_touch > 30:
        return "Sales Engagement Basic"
    return "Salesforce Engage"


def _queue_score(
    lead_score: float,
    sla_breach: bool,
    days_since_touch: int,
    meeting_booked_count: int,
    converted: bool,
    has_response: bool,
) -> float:
    if converted:
        return 0.0
    score = lead_score * 0.5
    if sla_breach:
        score += 30.0
    if meeting_booked_count > 0:
        score -= 10.0
    if not has_response:
        score += 10.0
    if days_since_touch > 14:
        score += 20.0
    elif days_since_touch > 7:
        score += 10.0
    return round(max(0.0, min(100.0, score)), 1)


def _chunked(values: list[str], size: int = 150) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _quoted_ids(values: list[str]) -> str:
    return ",".join(f"'{value}'" for value in values)


def _fetch_contacts(inst: str, tok: str, contact_ids: list[str]) -> list[dict]:
    rows: list[dict] = []
    for chunk in _chunked(contact_ids):
        query = CONTACT_SOQL + f"WHERE Id IN ({_quoted_ids(chunk)})"
        rows.extend(_soql(inst, tok, query))
    return rows


def _fetch_accounts(inst: str, tok: str, account_ids: list[str]) -> list[dict]:
    rows: list[dict] = []
    for chunk in _chunked(account_ids):
        query = ACCOUNT_SOQL + f"WHERE Id IN ({_quoted_ids(chunk)})"
        rows.extend(_soql(inst, tok, query))
    return rows


def _account_base_class(account: dict | None) -> str:
    if not account:
        return "Unmatched"
    account_type = (account.get("Type") or "").strip()
    finance_client = _coerce_bool(account.get("Finance_Client__c"))
    ex_customer = _coerce_bool(account.get("Ex_Customer__c"))
    product_client = _coerce_bool(account.get("SaaS_Client__c")) or _coerce_bool(
        account.get("Axioma_Client__c")
    )
    lost_date = _parse_date(account.get("Heat_Map_Red_Lostdate__c"))
    if account_type in {"Partner", "Affiliate", "Competitor", "Internal"}:
        return account_type
    if finance_client or product_client:
        return "Current Client"
    if ex_customer or lost_date:
        return "Former Client"
    if account_type == "Customer":
        return "Current Client"
    if account_type == "Prospect":
        return "Prospect"
    return account_type or "Unclassified"


def _former_client_age_band(account: dict | None, today: date) -> str:
    if not account:
        return ""
    lost_date = _parse_date(account.get("Heat_Map_Red_Lostdate__c"))
    if not lost_date:
        return ""
    age_days = max(0, (today - lost_date).days)
    if age_days >= 365 * 2:
        return "2+ Years"
    if age_days >= 365:
        return "1-2 Years"
    if age_days >= 180:
        return "6-12 Months"
    return "<6 Months"


def _stage_rank(stage_name: str) -> int:
    text = (stage_name or "").strip()
    if not text:
        return 0
    try:
        return int(text.split(" - ", 1)[0])
    except (ValueError, IndexError):
        return 0


def _handoff_quality_band(opp: dict[str, object]) -> str:
    if not opp:
        return "No Handoff"
    stage_rank = _stage_rank(str(opp.get("StageName") or ""))
    hit_stage3 = bool(opp.get("New_Stage_20_Date__c")) or stage_rank >= 3
    submitted = _coerce_bool(opp.get("Submit_for_Stage_20_Review__c"))
    approved = _coerce_bool(opp.get("Stage_20_Approval__c"))
    overdue = _coerce_bool(opp.get("HasOverdueTask"))
    if hit_stage3 and (approved or submitted) and not overdue:
        return "Strong"
    if hit_stage3:
        return "Stage 3+"
    if bool(opp.get("New_Stage_15_Date__c")) or stage_rank == 2:
        return "Discovery"
    if stage_rank > 0:
        return "Early / Weak"
    return "No Handoff"


def _fetch_campaign_members(inst: str, tok: str, lead_ids: list[str]) -> list[dict]:
    rows: list[dict] = []
    for chunk in _chunked(lead_ids):
        query = (
            "SELECT Id, LeadId, Campaign.Name, Campaign.Type, Campaign.Campaign_Product__c, "
            "Campaign.Lead_Scope_Type__c, Campaign.Campaign_Purpose__c, Status, HasResponded, CreatedDate "
            "FROM CampaignMember "
            f"WHERE LeadId IN ({_quoted_ids(chunk)})"
        )
        rows.extend(_soql(inst, tok, query))
    return rows


def _fetch_opportunities(inst: str, tok: str, opp_ids: list[str]) -> list[dict]:
    rows: list[dict] = []
    for chunk in _chunked(opp_ids):
        query = OPP_SOQL + f"WHERE Id IN ({_quoted_ids(chunk)})"
        rows.extend(_soql(inst, tok, query))
    return rows


def _fetch_bdr_created_opportunities(
    inst: str, tok: str, creator_ids: list[str]
) -> list[dict]:
    rows: list[dict] = []
    for chunk in _chunked(creator_ids):
        query = (
            OPP_SOQL
            + f"WHERE CreatedDate >= {START_DATE} AND CreatedById IN ({_quoted_ids(chunk)})"
        )
        rows.extend(_soql(inst, tok, query))
    return rows


def _fetch_account_opportunities(
    inst: str, tok: str, account_ids: list[str]
) -> list[dict]:
    rows: list[dict] = []
    for chunk in _chunked(account_ids, 100):
        query = OPP_SOQL + f"WHERE AccountId IN ({_quoted_ids(chunk)})"
        rows.extend(_soql(inst, tok, query))
    return rows


def create_dataset(inst: str, tok: str) -> bool:
    """Build the BDR operating dataset."""
    print(f"\n=== Building {DS_LABEL} dataset ===")
    today = datetime.now(UTC).date()
    today_iso = today.isoformat()

    users = _soql(inst, tok, BDR_USER_SOQL)
    queues = _soql(inst, tok, QUEUE_SOQL)
    queue_ids = [row["Id"] for row in queues if row.get("Id")]
    queue_by_id = {row["Id"]: row for row in queues if row.get("Id")}
    print(f"  BDR users: {len(users)}")
    print(f"  BDR queues: {len(queues)}")

    queue_members = _soql(
        inst,
        tok,
        f"SELECT GroupId, UserOrGroupId FROM GroupMember WHERE GroupId IN ({_quoted_ids(queue_ids)})",
    )

    membership_by_user: dict[str, list[str]] = defaultdict(list)
    for row in queue_members:
        group_id = row.get("GroupId")
        user_id = row.get("UserOrGroupId")
        if group_id and user_id and str(user_id).startswith("005"):
            membership_by_user[user_id].append(group_id)

    user_by_id: dict[str, dict[str, str]] = {}
    bdr_user_ids: list[str] = []
    for row in users:
        user_id = row.get("Id")
        if not user_id:
            continue
        title = (row.get("Title") or "").strip()
        department = row.get("Department") or ""
        queue_memberships = membership_by_user.get(user_id, [])
        if not department and not queue_memberships:
            continue

        team = _team_from_department(department)
        if len(queue_memberships) == 1:
            team = _queue_team(queue_by_id[queue_memberships[0]]["Name"])

        user_by_id[user_id] = {
            "OwnerName": (row.get("Name") or "")[:255],
            "Title": title[:255],
            "Department": str(department)[:255],
            "ManagerName": (((row.get("Manager") or {}).get("Name")) or "")[:255],
            "BDRTeam": team[:255],
            "BDRRole": _role_level(title),
            "QueueGroup": queue_by_id[queue_memberships[0]]["Name"][:255]
            if len(queue_memberships) == 1
            else "",
        }
        bdr_user_ids.append(user_id)

    owner_ids = bdr_user_ids + queue_ids
    owner_filter = _quoted_ids(owner_ids)

    tasks = _soql(
        inst,
        tok,
        "SELECT Id, Subject, Type, Status, ActivityDate, CreatedDate, WhoId, WhatId, OwnerId, Owner.Name "
        "FROM Task "
        f"WHERE CreatedDate >= {START_DATE} AND OwnerId IN ({owner_filter})",
    )
    events = _soql(
        inst,
        tok,
        "SELECT Id, Subject, ActivityDate, CreatedDate, WhoId, WhatId, OwnerId, Owner.Name "
        "FROM Event "
        f"WHERE CreatedDate >= {START_DATE} AND OwnerId IN ({owner_filter})",
    )
    print(f"  BDR tasks: {len(tasks)}")
    print(f"  BDR events: {len(events)}")

    activity_lead_ids = {
        str(row.get("WhoId"))
        for row in tasks + events
        if str(row.get("WhoId") or "").startswith("00Q")
    }

    base_leads = _soql(
        inst,
        tok,
        LEAD_BASE_SOQL
        + f"WHERE CreatedDate >= {START_DATE} OR OwnerId IN ({owner_filter})",
    )

    leads_by_id = {row["Id"]: row for row in base_leads if row.get("Id")}
    missing_lead_ids = sorted(activity_lead_ids - set(leads_by_id))
    if missing_lead_ids:
        for chunk in _chunked(missing_lead_ids):
            query = LEAD_BASE_SOQL + f"WHERE Id IN ({_quoted_ids(chunk)})"
            for row in _soql(inst, tok, query):
                if row.get("Id"):
                    leads_by_id[row["Id"]] = row

    candidate_lead_ids = sorted(
        lead_id
        for lead_id, row in leads_by_id.items()
        if row.get("OwnerId") in owner_ids
        or row.get("CreatedById") in bdr_user_ids
        or lead_id in activity_lead_ids
    )
    print(f"  Candidate BDR leads: {len(candidate_lead_ids)}")

    campaign_members = (
        _fetch_campaign_members(inst, tok, candidate_lead_ids)
        if candidate_lead_ids
        else []
    )
    print(f"  Campaign members: {len(campaign_members)}")

    converted_opp_ids = sorted(
        {
            str(row.get("ConvertedOpportunityId"))
            for lead_id, row in leads_by_id.items()
            if lead_id in candidate_lead_ids and row.get("ConvertedOpportunityId")
        }
    )
    converted_opps = (
        _fetch_opportunities(inst, tok, converted_opp_ids) if converted_opp_ids else []
    )
    bdr_created_opps = (
        _fetch_bdr_created_opportunities(inst, tok, bdr_user_ids)
        if bdr_user_ids
        else []
    )
    opp_by_id = {
        row["Id"]: row for row in converted_opps + bdr_created_opps if row.get("Id")
    }
    print(f"  Converted opportunities: {len(converted_opps)}")
    print(f"  BDR-created opportunities: {len(bdr_created_opps)}")

    converted_contact_ids = sorted(
        {
            str(row.get("ConvertedContactId"))
            for lead_id, row in leads_by_id.items()
            if lead_id in candidate_lead_ids and row.get("ConvertedContactId")
        }
    )
    activity_contact_ids = sorted(
        {
            str(row.get("WhoId"))
            for row in tasks + events
            if str(row.get("WhoId") or "").startswith("003")
        }
    )
    all_contact_ids = sorted(set(converted_contact_ids) | set(activity_contact_ids))
    contacts = _fetch_contacts(inst, tok, all_contact_ids) if all_contact_ids else []
    contact_by_id = {row["Id"]: row for row in contacts if row.get("Id")}
    print(f"  Related contacts: {len(contacts)}")

    direct_accounts = (
        _soql(
            inst, tok, ACCOUNT_SOQL + f"WHERE OwnerId IN ({_quoted_ids(bdr_user_ids)})"
        )
        if bdr_user_ids
        else []
    )

    converted_account_ids = sorted(
        {
            str(row.get("ConvertedAccountId"))
            for lead_id, row in leads_by_id.items()
            if lead_id in candidate_lead_ids and row.get("ConvertedAccountId")
        }
    )
    matched_account_ids = sorted(
        {
            str(row.get("engagio__Matched_Account__c"))
            for lead_id, row in leads_by_id.items()
            if lead_id in candidate_lead_ids and row.get("engagio__Matched_Account__c")
        }
    )
    activity_account_ids = sorted(
        {
            str(row.get("WhatId"))
            for row in tasks + events
            if str(row.get("WhatId") or "").startswith("001")
        }
    )
    contact_account_ids = sorted(
        {str(row.get("AccountId")) for row in contacts if row.get("AccountId")}
    )
    all_account_ids = sorted(
        set(converted_account_ids)
        | set(matched_account_ids)
        | set(activity_account_ids)
        | set(contact_account_ids)
        | {
            str(row.get("AccountId"))
            for row in opp_by_id.values()
            if row.get("AccountId")
        }
        | {str(row.get("Id")) for row in direct_accounts if row.get("Id")}
    )
    accounts = _fetch_accounts(inst, tok, all_account_ids) if all_account_ids else []
    account_by_id = {
        row["Id"]: row for row in accounts + direct_accounts if row.get("Id")
    }
    print(f"  Related accounts: {len(accounts)}")

    direct_account_ids = [
        str(row.get("Id") or "") for row in direct_accounts if row.get("Id")
    ]
    account_history_opps = (
        _fetch_account_opportunities(inst, tok, direct_account_ids)
        if direct_account_ids
        else []
    )
    print(f"  Opportunities on direct BDR accounts: {len(account_history_opps)}")

    direct_contacts = (
        _soql(
            inst,
            tok,
            CONTACT_SOQL + f"WHERE Account.OwnerId IN ({_quoted_ids(bdr_user_ids)})",
        )
        if bdr_user_ids
        else []
    )
    contact_by_id.update({row["Id"]: row for row in direct_contacts if row.get("Id")})
    print(f"  Direct BDR contacts: {len(direct_contacts)}")

    def lead_context(lead_row: dict[str, object]) -> dict[str, object]:
        matched_account_id = str(lead_row.get("engagio__Matched_Account__c") or "")
        converted_account_id = str(lead_row.get("ConvertedAccountId") or "")
        converted_contact_id = str(lead_row.get("ConvertedContactId") or "")
        contact_account_id = str(
            (contact_by_id.get(converted_contact_id) or {}).get("AccountId") or ""
        )

        context_account_id = ""
        context_account_source = ""
        if matched_account_id and matched_account_id in account_by_id:
            context_account_id = matched_account_id
            context_account_source = "Matched Account"
        elif converted_account_id and converted_account_id in account_by_id:
            context_account_id = converted_account_id
            context_account_source = "Converted Account"
        elif contact_account_id and contact_account_id in account_by_id:
            context_account_id = contact_account_id
            context_account_source = "Converted Contact"

        account = account_by_id.get(context_account_id, {})
        contact = contact_by_id.get(converted_contact_id, {})
        persona = (
            (lead_row.get("Dimension_Persona__c") or "")
            or (contact.get("Dimension_Persona__c") or "")
            or "Unknown"
        )
        title = (
            (lead_row.get("Title") or "")
            or (contact.get("Official_Title__c") or contact.get("Title") or "")
            or ""
        )[:255]
        contact_title = (
            (contact.get("Official_Title__c") or "") or (contact.get("Title") or "")
        )[:255]
        return {
            "ContextAccountId": context_account_id,
            "ContextAccountSource": context_account_source[:255],
            "ContextAccount": account,
            "ContextAccountName": (
                account.get("Name")
                or lead_row.get("engagio__Matched_Account_Name__c")
                or ""
            )[:255],
            "ContextAccountType": (account.get("Type") or "")[:255],
            "ContextAccountIndustry": (
                account.get("Industry")
                or lead_row.get("engagio__Matched_Account_Industry__c")
                or ""
            )[:255],
            "ContextAccountRegion": (
                account.get("Region__c")
                or (
                    (lead_row.get("engagio__Matched_Account__r") or {}).get("Region__c")
                )
                or ""
            )[:255],
            "ContextAccountSegment": (
                account.get("TAM_Universe_Segment__c")
                or (
                    (lead_row.get("engagio__Matched_Account__r") or {}).get(
                        "TAM_Universe_Segment__c"
                    )
                )
                or ""
            )[:255],
            "ContextAccountTier": (
                account.get("Tier_Calculation__c")
                or (
                    (lead_row.get("engagio__Matched_Account__r") or {}).get(
                        "Tier_Calculation__c"
                    )
                )
                or ""
            )[:255],
            "ContextCustomerSegment": (account.get("Customer_Segment__c") or "")[:255],
            "ContextProductOpportunity": (account.get("Product_Opportunity__c") or "")[
                :255
            ],
            "ContextProductMainline": (account.get("Product_Mainline__c") or "")[:255],
            "ClientBaseClass": _account_base_class(account)[:255],
            "FormerClientLostDate": str(
                (account.get("Heat_Map_Red_Lostdate__c") or "")
            )[:10],
            "FormerClientAgeBand": _former_client_age_band(account, today)[:255],
            "TelemarketingStatus": (account.get("TM_Account_Status__c") or "")[:255],
            "Persona": persona[:255],
            "LeadTitle": title,
            "ContactOfficialTitle": contact_title,
        }

    lead_context_by_id = {
        lead_id: lead_context(leads_by_id[lead_id]) for lead_id in candidate_lead_ids
    }

    def _lead_priority(lead_id: str) -> tuple[int, str, str]:
        lead_row = leads_by_id[lead_id]
        status_lower = str(lead_row.get("Status") or "").strip().lower()
        is_disqualified = status_lower in {
            "disqualified by marketing",
            "disqualified by sales",
        }
        is_converted = bool(lead_row.get("IsConverted"))
        open_score = (
            2
            if (not is_converted and not is_disqualified)
            else 1
            if is_converted
            else 0
        )
        return (
            open_score,
            str(lead_row.get("ConvertedDate") or ""),
            str(lead_row.get("CreatedDate") or ""),
        )

    contact_primary_lead: dict[str, str] = {}
    for contact_id in converted_contact_ids:
        lead_ids = [
            lead_id
            for lead_id in candidate_lead_ids
            if str(leads_by_id[lead_id].get("ConvertedContactId") or "") == contact_id
        ]
        if lead_ids:
            contact_primary_lead[contact_id] = max(lead_ids, key=_lead_priority)

    opp_primary_lead: dict[str, str] = {}
    for opp_id in converted_opp_ids:
        lead_ids = [
            lead_id
            for lead_id in candidate_lead_ids
            if str(leads_by_id[lead_id].get("ConvertedOpportunityId") or "") == opp_id
        ]
        if lead_ids:
            opp_primary_lead[opp_id] = max(lead_ids, key=_lead_priority)

    account_primary_lead: dict[str, str] = {}
    account_candidates: dict[str, list[str]] = defaultdict(list)
    for lead_id in candidate_lead_ids:
        context_account_id = str(
            lead_context_by_id[lead_id].get("ContextAccountId") or ""
        )
        if context_account_id:
            account_candidates[context_account_id].append(lead_id)
    for account_id, lead_ids in account_candidates.items():
        account_primary_lead[account_id] = max(lead_ids, key=_lead_priority)

    queue_backlog: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "QueueTaskCount": 0.0,
            "OverdueTaskCount": 0.0,
            "DueTodayTaskCount": 0.0,
        }
    )
    for row in tasks:
        owner_id = row.get("OwnerId")
        if owner_id not in queue_by_id:
            continue
        queue_name = queue_by_id[owner_id]["Name"]
        metrics = queue_backlog[queue_name]
        status = (row.get("Status") or "").lower()
        if status == "completed":
            continue
        metrics["QueueTaskCount"] += 1.0
        due_date = _parse_date(row.get("ActivityDate"))
        if due_date == today:
            metrics["DueTodayTaskCount"] += 1.0
        elif due_date and due_date < today:
            metrics["OverdueTaskCount"] += 1.0

    lead_activity: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "TouchCount": 0.0,
            "CallCount": 0.0,
            "EmailCount": 0.0,
            "MeetingBookedCount": 0.0,
            "MeetingHeldCount": 0.0,
            "LeadTouchCount": 0.0,
            "ContactTouchCount": 0.0,
            "AccountTouchCount": 0.0,
            "OpportunityTouchCount": 0.0,
            "FirstTouchDate": "",
            "FirstTouchPath": "",
            "FirstMeetingDate": "",
            "DirectLeadFirstTouchDate": "",
            "AssociatedFirstTouchDate": "",
            "LastTouchDate": "",
            "NextMeetingDate": "",
            "LatestMeetingDate": "",
            "ReplyCount": 0.0,
            "Owners": defaultdict(float),
            "LatestOwnerId": "",
            "LatestOwnerDate": "",
        }
    )
    account_activity: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "TouchCount": 0.0,
            "CallCount": 0.0,
            "EmailCount": 0.0,
            "MeetingBookedCount": 0.0,
            "MeetingHeldCount": 0.0,
            "LastTouchDate": "",
            "NextMeetingDate": "",
        }
    )
    contact_activity: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "TouchCount": 0.0,
            "CallCount": 0.0,
            "EmailCount": 0.0,
            "MeetingBookedCount": 0.0,
            "MeetingHeldCount": 0.0,
            "LastTouchDate": "",
            "NextMeetingDate": "",
        }
    )

    rep_week: dict[tuple[str, str, str, str, str, str], dict[str, object]] = (
        defaultdict(
            lambda: {
                "TouchCount": 0.0,
                "CallCount": 0.0,
                "EmailCount": 0.0,
                "MeetingBookedCount": 0.0,
                "MeetingHeldCount": 0.0,
                "LeadCreatedCount": 0.0,
                "SLAMetCount": 0.0,
                "SLABreachCount": 0.0,
                "QualifiedCount": 0.0,
                "SourcedARR": 0.0,
                "ResponseCount": 0.0,
                "Companies": set(),
            }
        )
    )
    owner_week: dict[tuple[str, str, str, str, str], dict[str, object]] = defaultdict(
        lambda: {
            "TouchCount": 0.0,
            "CallCount": 0.0,
            "EmailCount": 0.0,
            "MeetingBookedCount": 0.0,
            "MeetingHeldCount": 0.0,
            "QualifiedCount": 0.0,
            "TotalActivityCount": 0.0,
            "LeadLinkedActivityCount": 0.0,
            "ContactLinkedActivityCount": 0.0,
            "AccountLinkedActivityCount": 0.0,
            "OpportunityLinkedActivityCount": 0.0,
            "ProspectActivityCount": 0.0,
            "CurrentClientActivityCount": 0.0,
            "FormerClientActivityCount": 0.0,
            "PartnerActivityCount": 0.0,
            "UnclassifiedActivityCount": 0.0,
            "Companies": set(),
        }
    )
    source_week: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
        lambda: {
            "LeadCreatedCount": 0.0,
            "MeetingHeldCount": 0.0,
            "QualifiedCount": 0.0,
            "SourcedARR": 0.0,
            "ResponseCount": 0.0,
        }
    )
    campaign_summary: dict[
        tuple[str, str, str, str, str, str, str, str], dict[str, float]
    ] = defaultdict(
        lambda: {
            "LeadCount": 0.0,
            "MQLLeadCount": 0.0,
            "SQLLeadCount": 0.0,
            "DisqualifiedLeadCount": 0.0,
            "MarketingDisqualifiedLeadCount": 0.0,
            "SalesDisqualifiedLeadCount": 0.0,
            "OpportunityHandoffCount": 0.0,
            "ResponseCount": 0.0,
            "MeetingBookedCount": 0.0,
            "MeetingHeldCount": 0.0,
            "QualifiedCount": 0.0,
            "SourcedARR": 0.0,
        }
    )
    owner_integrity: dict[tuple[str, str, str, str], dict[str, float]] = defaultdict(
        lambda: {
            "LeadCount": 0.0,
            "LeadTouch24hCount": 0.0,
            "LeadTouchAnyCount": 0.0,
            "AssociatedTouch24hCount": 0.0,
            "AssociatedTouchAnyCount": 0.0,
            "TotalActivityCount": 0.0,
            "LeadLinkedActivityCount": 0.0,
            "ContactLinkedActivityCount": 0.0,
            "AccountLinkedActivityCount": 0.0,
            "OpportunityLinkedActivityCount": 0.0,
            "ProspectActivityCount": 0.0,
            "CurrentClientActivityCount": 0.0,
            "FormerClientActivityCount": 0.0,
            "PartnerActivityCount": 0.0,
            "UnclassifiedActivityCount": 0.0,
        }
    )

    def owner_context(owner_id: str) -> dict[str, str]:
        if owner_id in user_by_id:
            return user_by_id[owner_id]
        if owner_id in queue_by_id:
            queue_name = queue_by_id[owner_id]["Name"]
            return {
                "OwnerName": queue_name[:255],
                "Title": "Queue",
                "Department": queue_name[:255],
                "ManagerName": "",
                "BDRTeam": _queue_team(queue_name)[:255],
                "BDRRole": "Queue",
                "QueueGroup": queue_name[:255],
            }
        return {
            "OwnerName": "Unknown",
            "Title": "",
            "Department": "",
            "ManagerName": "",
            "BDRTeam": "Shared BDR",
            "BDRRole": "Rep",
            "QueueGroup": "",
        }

    def add_rep_week(
        owner_id: str,
        activity_date: str,
        source_group: str,
        company: str,
        **metrics: float,
    ) -> None:
        if not activity_date:
            return
        context = owner_context(owner_id)
        week_start = _week_start(activity_date)
        if not week_start:
            return
        key = (
            context["OwnerName"],
            context["BDRTeam"],
            context["BDRRole"],
            context["ManagerName"],
            source_group[:255],
            week_start,
        )
        bucket = rep_week[key]
        for metric_name, metric_value in metrics.items():
            bucket[metric_name] = safe_float(bucket.get(metric_name)) + safe_float(
                metric_value
            )
        if company:
            companies = bucket["Companies"]
            if isinstance(companies, set):
                companies.add(company)

        source_key = (context["BDRTeam"], source_group[:255], week_start)
        source_bucket = source_week[source_key]
        for metric_name in (
            "LeadCreatedCount",
            "MeetingHeldCount",
            "QualifiedCount",
            "SourcedARR",
            "ResponseCount",
        ):
            if metric_name in metrics:
                source_bucket[metric_name] += safe_float(metrics[metric_name])

    def _lead_activity_path(row: dict[str, object]) -> list[tuple[str, str]]:
        who_id = str(row.get("WhoId") or "")
        what_id = str(row.get("WhatId") or "")
        mapped: dict[str, str] = {}
        if who_id.startswith("00Q") and who_id in candidate_lead_ids:
            mapped[who_id] = "Lead"
        elif who_id.startswith("003"):
            contact_lead = contact_primary_lead.get(who_id, "")
            if contact_lead:
                mapped[contact_lead] = "Converted Contact"
            else:
                contact_account_id = str(
                    (contact_by_id.get(who_id) or {}).get("AccountId") or ""
                )
                account_lead = account_primary_lead.get(contact_account_id, "")
                if account_lead:
                    mapped[account_lead] = "Context Account"
        if what_id.startswith("006"):
            opp_lead = opp_primary_lead.get(what_id, "")
            if opp_lead and opp_lead not in mapped:
                mapped[opp_lead] = "Opportunity"
        elif what_id.startswith("001"):
            account_lead = account_primary_lead.get(what_id, "")
            if account_lead and account_lead not in mapped:
                mapped[account_lead] = "Context Account"
        return list(mapped.items())

    def _activity_client_class(
        row: dict[str, object], mapped_items: list[tuple[str, str]]
    ) -> tuple[str, str]:
        who_id = str(row.get("WhoId") or "")
        what_id = str(row.get("WhatId") or "")
        if who_id.startswith("003"):
            contact = contact_by_id.get(who_id, {})
            account = account_by_id.get(str(contact.get("AccountId") or ""), {})
            return _account_base_class(account), (account.get("Name") or "")[:255]
        if what_id.startswith("001"):
            account = account_by_id.get(what_id, {})
            return _account_base_class(account), (account.get("Name") or "")[:255]
        if mapped_items:
            lead_id = mapped_items[0][0]
            context_row = lead_context_by_id.get(lead_id, {})
            return (
                str(context_row.get("ClientBaseClass") or "Unclassified"),
                str(
                    context_row.get("ContextAccountName")
                    or leads_by_id.get(lead_id, {}).get("Company")
                    or ""
                )[:255],
            )
        return "Unclassified", ""

    def _owner_week_key(
        owner_id: str, activity_date: str
    ) -> tuple[str, str, str, str, str] | None:
        week_start = _week_start(activity_date)
        if not week_start:
            return None
        context = owner_context(owner_id)
        return (
            context["OwnerName"],
            context["BDRTeam"],
            context["BDRRole"],
            context["ManagerName"],
            week_start,
        )

    def _apply_entity_touch(
        bucket: dict[str, object], touch_date: str, kind: str, meeting_date: str = ""
    ) -> None:
        bucket["TouchCount"] = safe_float(bucket["TouchCount"]) + 1.0
        if kind == "Call":
            bucket["CallCount"] = safe_float(bucket["CallCount"]) + 1.0
        elif kind == "Email":
            bucket["EmailCount"] = safe_float(bucket["EmailCount"]) + 1.0
        if touch_date and (
            not bucket["LastTouchDate"] or touch_date > str(bucket["LastTouchDate"])
        ):
            bucket["LastTouchDate"] = touch_date
        if meeting_date:
            bucket["MeetingBookedCount"] = (
                safe_float(bucket["MeetingBookedCount"]) + 1.0
            )
            if meeting_date >= today_iso:
                if not bucket["NextMeetingDate"] or meeting_date < str(
                    bucket["NextMeetingDate"]
                ):
                    bucket["NextMeetingDate"] = meeting_date
            if meeting_date <= today_iso:
                bucket["MeetingHeldCount"] = (
                    safe_float(bucket["MeetingHeldCount"]) + 1.0
                )

    def _increment_owner_week(
        owner_id: str,
        activity_date: str,
        kind: str,
        client_class: str,
        company: str,
        *,
        lead_linked: bool,
        contact_linked: bool,
        account_linked: bool,
        opportunity_linked: bool,
        meeting_booked: bool = False,
        meeting_held: bool = False,
    ) -> None:
        key = _owner_week_key(owner_id, activity_date)
        if not key:
            return
        bucket = owner_week[key]
        bucket["TotalActivityCount"] = safe_float(bucket["TotalActivityCount"]) + 1.0
        bucket["TouchCount"] = safe_float(bucket["TouchCount"]) + 1.0
        if kind == "Call":
            bucket["CallCount"] = safe_float(bucket["CallCount"]) + 1.0
        elif kind == "Email":
            bucket["EmailCount"] = safe_float(bucket["EmailCount"]) + 1.0
        if meeting_booked:
            bucket["MeetingBookedCount"] = (
                safe_float(bucket["MeetingBookedCount"]) + 1.0
            )
        if meeting_held:
            bucket["MeetingHeldCount"] = safe_float(bucket["MeetingHeldCount"]) + 1.0
        if lead_linked:
            bucket["LeadLinkedActivityCount"] = (
                safe_float(bucket["LeadLinkedActivityCount"]) + 1.0
            )
        if contact_linked:
            bucket["ContactLinkedActivityCount"] = (
                safe_float(bucket["ContactLinkedActivityCount"]) + 1.0
            )
        if account_linked:
            bucket["AccountLinkedActivityCount"] = (
                safe_float(bucket["AccountLinkedActivityCount"]) + 1.0
            )
        if opportunity_linked:
            bucket["OpportunityLinkedActivityCount"] = (
                safe_float(bucket["OpportunityLinkedActivityCount"]) + 1.0
            )
        if client_class == "Current Client":
            bucket["CurrentClientActivityCount"] = (
                safe_float(bucket["CurrentClientActivityCount"]) + 1.0
            )
        elif client_class == "Former Client":
            bucket["FormerClientActivityCount"] = (
                safe_float(bucket["FormerClientActivityCount"]) + 1.0
            )
        elif client_class == "Prospect":
            bucket["ProspectActivityCount"] = (
                safe_float(bucket["ProspectActivityCount"]) + 1.0
            )
        elif client_class in {"Partner", "Affiliate"}:
            bucket["PartnerActivityCount"] = (
                safe_float(bucket["PartnerActivityCount"]) + 1.0
            )
        else:
            bucket["UnclassifiedActivityCount"] = (
                safe_float(bucket["UnclassifiedActivityCount"]) + 1.0
            )
        companies = bucket["Companies"]
        if company and isinstance(companies, set):
            companies.add(company)

    def _add_owner_week_metrics(
        owner_id: str, activity_date: str, **metrics: float
    ) -> None:
        key = _owner_week_key(owner_id, activity_date)
        if not key:
            return
        bucket = owner_week[key]
        for metric_name, metric_value in metrics.items():
            bucket[metric_name] = safe_float(bucket.get(metric_name)) + safe_float(
                metric_value
            )

    def _apply_touch(
        activity: dict[str, object],
        owner_id: str,
        touch_date: str,
        kind: str,
        path: str,
        meeting_date: str = "",
    ) -> None:
        activity["Owners"][owner_id] = safe_float(activity["Owners"][owner_id]) + 1.0
        if touch_date and (
            not activity["LatestOwnerDate"]
            or touch_date >= str(activity["LatestOwnerDate"])
        ):
            activity["LatestOwnerId"] = owner_id
            activity["LatestOwnerDate"] = touch_date
        activity["TouchCount"] = safe_float(activity["TouchCount"]) + 1.0
        if kind == "Call":
            activity["CallCount"] = safe_float(activity["CallCount"]) + 1.0
        elif kind == "Email":
            activity["EmailCount"] = safe_float(activity["EmailCount"]) + 1.0

        if path == "Lead":
            activity["LeadTouchCount"] = safe_float(activity["LeadTouchCount"]) + 1.0
            if touch_date and (
                not activity["DirectLeadFirstTouchDate"]
                or touch_date < str(activity["DirectLeadFirstTouchDate"])
            ):
                activity["DirectLeadFirstTouchDate"] = touch_date
        elif path == "Converted Contact":
            activity["ContactTouchCount"] = (
                safe_float(activity["ContactTouchCount"]) + 1.0
            )
        elif path == "Opportunity":
            activity["OpportunityTouchCount"] = (
                safe_float(activity["OpportunityTouchCount"]) + 1.0
            )
        else:
            activity["AccountTouchCount"] = (
                safe_float(activity["AccountTouchCount"]) + 1.0
            )

        if (
            path != "Lead"
            and touch_date
            and (
                not activity["AssociatedFirstTouchDate"]
                or touch_date < str(activity["AssociatedFirstTouchDate"])
            )
        ):
            activity["AssociatedFirstTouchDate"] = touch_date

        if touch_date and (
            not activity["FirstTouchDate"]
            or touch_date < str(activity["FirstTouchDate"])
        ):
            activity["FirstTouchDate"] = touch_date
            activity["FirstTouchPath"] = path
        if touch_date and (
            not activity["LastTouchDate"] or touch_date > str(activity["LastTouchDate"])
        ):
            activity["LastTouchDate"] = touch_date
        if meeting_date:
            activity["MeetingBookedCount"] = (
                safe_float(activity["MeetingBookedCount"]) + 1.0
            )
            if meeting_date >= today_iso:
                if not activity["NextMeetingDate"] or meeting_date < str(
                    activity["NextMeetingDate"]
                ):
                    activity["NextMeetingDate"] = meeting_date
            if meeting_date <= today_iso:
                activity["MeetingHeldCount"] = (
                    safe_float(activity["MeetingHeldCount"]) + 1.0
                )
                if not activity["FirstMeetingDate"] or meeting_date < str(
                    activity["FirstMeetingDate"]
                ):
                    activity["FirstMeetingDate"] = meeting_date
                if not activity["LatestMeetingDate"] or meeting_date > str(
                    activity["LatestMeetingDate"]
                ):
                    activity["LatestMeetingDate"] = meeting_date

    def _process_activity_row(row: dict[str, object], *, is_event: bool) -> None:
        owner_id = row.get("OwnerId") or ""
        context = owner_context(owner_id)
        integrity_key = (
            context["OwnerName"],
            context["ManagerName"],
            context["BDRTeam"],
            context["BDRRole"],
        )
        metrics = owner_integrity[integrity_key]
        metrics["TotalActivityCount"] += 1.0
        who_prefix = str(row.get("WhoId") or "")[:3]
        what_prefix = str(row.get("WhatId") or "")[:3]
        lead_linked = who_prefix == "00Q"
        contact_linked = who_prefix == "003"
        account_linked = what_prefix == "001"
        opportunity_linked = what_prefix == "006"
        if lead_linked:
            metrics["LeadLinkedActivityCount"] += 1.0
        elif contact_linked:
            metrics["ContactLinkedActivityCount"] += 1.0
        if account_linked:
            metrics["AccountLinkedActivityCount"] += 1.0
        elif opportunity_linked:
            metrics["OpportunityLinkedActivityCount"] += 1.0

        mapped_items = _lead_activity_path(row)
        client_class, company_name = _activity_client_class(row, mapped_items)
        if client_class == "Current Client":
            metrics["CurrentClientActivityCount"] += 1.0
        elif client_class == "Former Client":
            metrics["FormerClientActivityCount"] += 1.0
        elif client_class == "Prospect":
            metrics["ProspectActivityCount"] += 1.0
        elif client_class in {"Partner", "Affiliate"}:
            metrics["PartnerActivityCount"] += 1.0
        else:
            metrics["UnclassifiedActivityCount"] += 1.0

        created_date = (row.get("CreatedDate") or "")[:10]
        activity_date = (row.get("ActivityDate") or "")[:10]
        touch_date = created_date or activity_date
        kind = _activity_kind(
            str(row.get("Type") or ""), str(row.get("Subject") or ""), is_event
        )
        status = (row.get("Status") or "").lower()
        completed_task = not is_event and status == "completed"
        meeting_booked = bool(is_event)
        meeting_held = bool(is_event and activity_date and activity_date <= today_iso)

        if (is_event or completed_task) and touch_date:
            _increment_owner_week(
                owner_id,
                touch_date,
                kind,
                client_class,
                company_name,
                lead_linked=lead_linked,
                contact_linked=contact_linked,
                account_linked=account_linked,
                opportunity_linked=opportunity_linked,
                meeting_booked=meeting_booked,
                meeting_held=False,
            )
        if meeting_held and activity_date:
            _increment_owner_week(
                owner_id,
                activity_date,
                "Meeting",
                client_class,
                company_name,
                lead_linked=lead_linked,
                contact_linked=contact_linked,
                account_linked=account_linked,
                opportunity_linked=opportunity_linked,
                meeting_booked=False,
                meeting_held=True,
            )

        if not mapped_items:
            mapped_items = []

        account_ids: set[str] = set()
        contact_ids: set[str] = set()
        if contact_linked:
            contact_id = str(row.get("WhoId") or "")
            if contact_id:
                contact_ids.add(contact_id)
                contact_account_id = str(
                    (contact_by_id.get(contact_id) or {}).get("AccountId") or ""
                )
                if contact_account_id:
                    account_ids.add(contact_account_id)
        if account_linked:
            account_id = str(row.get("WhatId") or "")
            if account_id:
                account_ids.add(account_id)
        if opportunity_linked:
            opp_account_id = str(
                (opp_by_id.get(str(row.get("WhatId") or "")) or {}).get("AccountId")
                or ""
            )
            if opp_account_id:
                account_ids.add(opp_account_id)
        for lead_id, _path in mapped_items:
            context_account_id = str(
                (lead_context_by_id.get(lead_id) or {}).get("ContextAccountId") or ""
            )
            if context_account_id:
                account_ids.add(context_account_id)

        if is_event or completed_task:
            for account_id in account_ids:
                _apply_entity_touch(
                    account_activity[account_id],
                    touch_date,
                    kind,
                    activity_date if is_event else "",
                )
            for contact_id in contact_ids:
                _apply_entity_touch(
                    contact_activity[contact_id],
                    touch_date,
                    kind,
                    activity_date if is_event else "",
                )

        if not mapped_items:
            return

        for lead_id, path in mapped_items:
            if lead_id not in candidate_lead_ids:
                continue
            lead_row = leads_by_id.get(lead_id, {})
            source_group = _source_group(_normalize_source(lead_row.get("LeadSource")))
            company = (lead_row.get("Company") or company_name or "")[:255]
            activity = lead_activity[lead_id]
            if is_event or completed_task:
                _apply_touch(
                    activity,
                    owner_id,
                    touch_date,
                    kind,
                    path,
                    activity_date if is_event else "",
                )
                add_rep_week(
                    owner_id,
                    touch_date,
                    source_group,
                    company,
                    TouchCount=1,
                    CallCount=1 if kind == "Call" else 0,
                    EmailCount=1 if kind == "Email" else 0,
                    MeetingBookedCount=1 if is_event else 0,
                )
                if str(row.get("Subject") or "").startswith("Email: Re:"):
                    activity["ReplyCount"] = safe_float(activity["ReplyCount"]) + 1.0
            if is_event and meeting_held:
                add_rep_week(
                    owner_id, activity_date, source_group, company, MeetingHeldCount=1
                )

    for row in tasks:
        _process_activity_row(row, is_event=False)

    for row in events:
        _process_activity_row(row, is_event=True)

    campaign_rows_by_lead: dict[str, list[dict]] = defaultdict(list)
    for row in campaign_members:
        lead_id = row.get("LeadId")
        if lead_id:
            campaign_rows_by_lead[lead_id].append(row)

    detail_rows: list[dict[str, object]] = []
    for lead_id in candidate_lead_ids:
        lead = leads_by_id[lead_id]
        context_row = lead_context_by_id.get(lead_id, {})
        owner_id = lead.get("OwnerId") or ""
        current_owner_is_bdr = owner_id in owner_ids
        activity = lead_activity.get(lead_id, {})
        if current_owner_is_bdr:
            attributed_owner_id = owner_id
        else:
            touch_owners = activity.get("Owners") or {}
            ranked = sorted(
                (
                    (oid, safe_float(count))
                    for oid, count in touch_owners.items()
                    if oid in owner_ids
                ),
                key=lambda item: item[1],
                reverse=True,
            )
            attributed_owner_id = (
                ranked[0][0]
                if ranked
                else str(
                    activity.get("LatestOwnerId") or lead.get("CreatedById") or owner_id
                )
            )

        context = owner_context(attributed_owner_id)
        created_date = (lead.get("CreatedDate") or "")[:10]
        converted_date = (lead.get("ConvertedDate") or "")[:10]
        source = _normalize_source(lead.get("LeadSource"))
        source_group = _source_group(source)
        company = (lead.get("Company") or "")[:255]
        persona = str(context_row.get("Persona") or "Unknown")[:255]
        industry = ((lead.get("Industry") or "Unknown") or "Unknown")[:255]
        country = ((lead.get("Country") or "Unknown") or "Unknown")[:255]
        lead_score = round(safe_float(lead.get("pi__score__c")), 1)
        first_touch_date = str(activity.get("FirstTouchDate") or "")
        first_touch_path = str(activity.get("FirstTouchPath") or "")
        first_meeting_date = str(activity.get("FirstMeetingDate") or "")
        direct_first_touch_date = str(activity.get("DirectLeadFirstTouchDate") or "")
        associated_first_touch_date = str(
            activity.get("AssociatedFirstTouchDate") or ""
        )
        last_touch_date = str(activity.get("LastTouchDate") or "")
        next_meeting_date = str(activity.get("NextMeetingDate") or "")
        meeting_booked_count = int(safe_float(activity.get("MeetingBookedCount")))
        meeting_held_count = int(safe_float(activity.get("MeetingHeldCount")))
        touch_count = int(safe_float(activity.get("TouchCount")))
        call_count = int(safe_float(activity.get("CallCount")))
        email_count = int(safe_float(activity.get("EmailCount")))
        lead_touch_count = int(safe_float(activity.get("LeadTouchCount")))
        contact_touch_count = int(safe_float(activity.get("ContactTouchCount")))
        account_touch_count = int(safe_float(activity.get("AccountTouchCount")))
        opportunity_touch_count = int(safe_float(activity.get("OpportunityTouchCount")))
        associated_touch_count = (
            contact_touch_count + account_touch_count + opportunity_touch_count
        )
        days_to_first_touch = (
            _days_between(created_date, first_touch_date) if first_touch_date else 0
        )
        days_to_first_meeting = (
            _days_between(created_date, first_meeting_date) if first_meeting_date else 0
        )
        days_since_last_touch = (
            _days_between(last_touch_date, today_iso)
            if last_touch_date
            else _days_between(created_date, today_iso)
        )
        sla_breach = (
            not direct_first_touch_date and _days_between(created_date, today_iso) > 2
        ) or (
            direct_first_touch_date
            and _days_between(created_date, direct_first_touch_date) > 2
        )
        sla_met = (
            bool(direct_first_touch_date)
            and _days_between(created_date, direct_first_touch_date) <= 2
        )
        response_count = int(
            sum(
                1
                for row in campaign_rows_by_lead.get(lead_id, [])
                if row.get("HasResponded")
            )
            + safe_float(activity.get("ReplyCount"))
        )
        campaign_count = len(campaign_rows_by_lead.get(lead_id, []))
        primary_campaign = ""
        primary_campaign_type = ""
        primary_campaign_product = ""
        primary_campaign_scope_type = ""
        primary_campaign_purpose = ""
        if campaign_rows_by_lead.get(lead_id):
            latest_row = max(
                campaign_rows_by_lead[lead_id],
                key=lambda item: str(item.get("CreatedDate") or ""),
            )
            campaign_obj = latest_row.get("Campaign") or {}
            primary_campaign = ((campaign_obj.get("Name")) or "")[:255]
            primary_campaign_type = ((campaign_obj.get("Type")) or "Unknown")[:255]
            primary_campaign_product = (
                (campaign_obj.get("Campaign_Product__c")) or "Unknown"
            )[:255]
            primary_campaign_scope_type = (
                (campaign_obj.get("Lead_Scope_Type__c")) or "Unknown"
            )[:255]
            primary_campaign_purpose = (
                (campaign_obj.get("Campaign_Purpose__c")) or "Unknown"
            )[:255]

        matched_account_obj = lead.get("engagio__Matched_Account__r") or {}
        matched_account_name = (
            (lead.get("engagio__Matched_Account_Name__c") or "") or ""
        )[:255]
        matched_account_industry = (
            (lead.get("engagio__Matched_Account_Industry__c") or "") or ""
        )[:255]
        matched_account_region = ((matched_account_obj.get("Region__c") or "") or "")[
            :255
        ]
        matched_account_segment = (
            (matched_account_obj.get("TAM_Universe_Segment__c") or "") or ""
        )[:255]
        matched_account_tier = (
            (matched_account_obj.get("Tier_Calculation__c") or "") or ""
        )[:255]
        target_account_flag = str(bool(lead.get("engagio__Matched_Account__c"))).lower()

        converted_flag = str(bool(lead.get("IsConverted"))).lower()
        sourced_opp_id = str(lead.get("ConvertedOpportunityId") or "")
        sourced_opp = opp_by_id.get(sourced_opp_id, {})
        targeted_product, targeted_product_source = _targeted_product_signal(
            primary_campaign_product,
            sourced_opp.get("APTS_RH_Product_Family__c"),
            sourced_opp.get("Stage_with_Product_Scope__c"),
            context_row.get("ContextProductOpportunity"),
            context_row.get("ContextProductMainline"),
        )
        opportunity_product_raw = _primary_product_signal(
            sourced_opp.get("APTS_RH_Product_Family__c")
        )
        opportunity_product, opportunity_product_source = _opportunity_product_signal(
            sourced_opp.get("APTS_RH_Product_Family__c"),
            context_row.get("ContextProductOpportunity"),
            context_row.get("ContextProductMainline"),
        )
        sourced_arr = round(safe_float(sourced_opp.get("ConvertedARR")), 2)
        stage2_date = str(sourced_opp.get("New_Stage_15_Date__c") or "")[:10]
        stage3_date = str(sourced_opp.get("New_Stage_20_Date__c") or "")[:10]
        handoff_quality_band = _handoff_quality_band(sourced_opp)
        pending_stage3_review = _coerce_bool(
            sourced_opp.get("Submit_for_Stage_20_Review__c")
        ) and not _coerce_bool(sourced_opp.get("Stage_20_Approval__c"))
        stage3_approved = _coerce_bool(sourced_opp.get("Stage_20_Approval__c"))
        strong_handoff = handoff_quality_band in {"Strong", "Stage 3+"}
        discovery_handoff = handoff_quality_band == "Discovery"
        status = (lead.get("Status") or "")[:255]
        status_lower = status.lower()
        is_mql = status_lower == "qualified by marketing"
        is_sql = status_lower == "hot lead"
        is_marketing_disqualified = status_lower == "disqualified by marketing"
        is_sales_disqualified = status_lower == "disqualified by sales"
        is_disqualified = is_marketing_disqualified or is_sales_disqualified
        is_open_actionable = converted_flag == "false" and not is_disqualified

        if converted_flag == "true":
            lifecycle_stage = "Qualified / Opportunity"
            stage_order = 5
        elif meeting_held_count > 0:
            lifecycle_stage = "Meeting Held"
            stage_order = 4
        elif meeting_booked_count > 0:
            lifecycle_stage = "Meeting Set"
            stage_order = 3
        elif touch_count > 0 or response_count > 0:
            lifecycle_stage = "Touched"
            stage_order = 2
        else:
            lifecycle_stage = "New"
            stage_order = 1

        queue_score = _queue_score(
            lead_score,
            sla_breach,
            days_since_last_touch,
            meeting_booked_count,
            converted_flag == "true",
            response_count > 0,
        )
        direct_lead_touch_any = bool(direct_first_touch_date)
        direct_lead_touch_24h = (
            bool(direct_first_touch_date)
            and _days_between(created_date, direct_first_touch_date) <= 1
        )
        associated_touch_any = bool(associated_first_touch_date)
        associated_touch_24h = (
            bool(associated_first_touch_date)
            and _days_between(created_date, associated_first_touch_date) <= 1
        )
        next_best_action = _next_best_action(
            lifecycle_stage,
            sla_breach,
            days_since_last_touch,
            meeting_booked_count > 0,
            response_count > 0,
        )
        suggested_tool = _suggested_tool(
            lifecycle_stage,
            sla_breach,
            days_since_last_touch,
            meeting_booked_count > 0,
            response_count > 0,
        )

        detail_rows.append(
            {
                "RecordType": "lead_detail",
                "LeadId": lead_id,
                "LeadName": (lead.get("Name") or "")[:255],
                "Company": company,
                "OwnerId": attributed_owner_id,
                "OwnerName": context["OwnerName"],
                "ManagerName": context["ManagerName"],
                "BDRTeam": context["BDRTeam"],
                "BDRRole": context["BDRRole"],
                "QueueGroup": context["QueueGroup"],
                "Department": context["Department"],
                "LeadTitle": str(context_row.get("LeadTitle") or ""),
                "ContactOfficialTitle": str(
                    context_row.get("ContactOfficialTitle") or ""
                ),
                "Persona": persona,
                "Industry": industry,
                "IndustryGroup": _industry_group(industry),
                "Country": country,
                "LeadSource": source[:255],
                "SourceGroup": source_group[:255],
                "Campaign": primary_campaign,
                "CampaignType": primary_campaign_type,
                "CampaignProduct": primary_campaign_product,
                "CampaignScopeType": primary_campaign_scope_type,
                "CampaignPurpose": primary_campaign_purpose,
                "YearLabel": _fy_label(created_date),
                "MatchedAccountName": matched_account_name,
                "MatchedAccountIndustry": matched_account_industry,
                "MatchedAccountRegion": matched_account_region,
                "MatchedAccountSegment": matched_account_segment,
                "MatchedAccountTier": matched_account_tier,
                "ContextAccountId": str(context_row.get("ContextAccountId") or ""),
                "ContextAccountName": str(context_row.get("ContextAccountName") or ""),
                "ContextAccountSource": str(
                    context_row.get("ContextAccountSource") or ""
                ),
                "ContextAccountType": str(context_row.get("ContextAccountType") or ""),
                "ContextAccountIndustry": str(
                    context_row.get("ContextAccountIndustry") or ""
                ),
                "ContextAccountRegion": str(
                    context_row.get("ContextAccountRegion") or ""
                ),
                "ContextAccountSegment": str(
                    context_row.get("ContextAccountSegment") or ""
                ),
                "ContextAccountTier": str(context_row.get("ContextAccountTier") or ""),
                "ContextCustomerSegment": str(
                    context_row.get("ContextCustomerSegment") or ""
                ),
                "ContextProductOpportunity": str(
                    context_row.get("ContextProductOpportunity") or ""
                ),
                "ContextProductMainline": str(
                    context_row.get("ContextProductMainline") or ""
                ),
                "TargetedProduct": targeted_product[:255],
                "TargetedProductSource": targeted_product_source[:255],
                "OpportunityProductRaw": opportunity_product_raw[:255],
                "OpportunityProductRawSource": "Opportunity Product"
                if opportunity_product_raw
                else "",
                "OpportunityProduct": opportunity_product[:255],
                "OpportunityProductSource": opportunity_product_source[:255],
                "ClientBaseClass": str(
                    context_row.get("ClientBaseClass") or "Unclassified"
                ),
                "FormerClientLostDate": str(
                    context_row.get("FormerClientLostDate") or ""
                ),
                "FormerClientAgeBand": str(
                    context_row.get("FormerClientAgeBand") or ""
                ),
                "TelemarketingStatus": str(
                    context_row.get("TelemarketingStatus") or ""
                ),
                "TargetAccountFlag": target_account_flag,
                "Status": status,
                "QualificationStage": (
                    "Opportunity Handoff"
                    if converted_flag == "true"
                    else "SQL / Hot Lead"
                    if is_sql
                    else "MQL"
                    if is_mql
                    else "Marketing Disqualified"
                    if is_marketing_disqualified
                    else "Sales Disqualified"
                    if is_sales_disqualified
                    else "Other Open"
                ),
                "LifecycleStage": lifecycle_stage,
                "PriorityBand": _priority_band(queue_score),
                "NextBestAction": next_best_action[:255],
                "SuggestedTool": suggested_tool[:255],
                "ConvertedFlag": converted_flag,
                "SLAMetFlag": str(sla_met).lower(),
                "SLABreachFlag": str(sla_breach).lower(),
                "HasResponseFlag": str(response_count > 0).lower(),
                "HasTouchFlag": str(touch_count > 0).lower(),
                "HasMeetingFlag": str(meeting_booked_count > 0).lower(),
                "HasMeetingHeldFlag": str(meeting_held_count > 0).lower(),
                "DirectLeadTouch24hFlag": str(direct_lead_touch_24h).lower(),
                "DirectLeadTouchAnyFlag": str(direct_lead_touch_any).lower(),
                "AssociatedTouch24hFlag": str(associated_touch_24h).lower(),
                "AssociatedTouchAnyFlag": str(associated_touch_any).lower(),
                "FirstTouchPath": first_touch_path[:255],
                "SourcedOpportunityId": sourced_opp_id,
                "SourcedOpportunityName": (sourced_opp.get("Name") or "")[:255],
                "SourcedOpportunityStage": (sourced_opp.get("StageName") or "")[:255],
                "SourcedForecastCategory": (
                    sourced_opp.get("ForecastCategoryName") or ""
                )[:255],
                "SourcedOpportunityType": (sourced_opp.get("Type") or "")[:255],
                "SourcedProductFamily": (
                    sourced_opp.get("APTS_RH_Product_Family__c") or ""
                )[:255],
                "HandoffQualityBand": handoff_quality_band[:255],
                "Stage2Date": stage2_date,
                "Stage3Date": stage3_date,
                "CreatedDate": created_date,
                "ConvertedDate": converted_date,
                "MonthStartDate": _month_start(created_date),
                "FirstTouchDate": first_touch_date,
                "FirstMeetingDate": first_meeting_date,
                "LastTouchDate": last_touch_date,
                "NextMeetingDate": next_meeting_date,
                "UpcomingMeetingCount": 1 if next_meeting_date else 0,
                "WeekStartDate": _week_start(created_date),
                "PriorityScore": queue_score,
                "LeadScore": lead_score,
                "LeadAgeDays": _days_between(created_date, today_iso),
                "DaysToFirstTouch": days_to_first_touch,
                "DaysToFirstMeeting": days_to_first_meeting,
                "DaysSinceLastTouch": days_since_last_touch,
                "Stage2To3Days": _days_between(stage2_date, stage3_date),
                "TouchCount": touch_count,
                "CallCount": call_count,
                "EmailCount": email_count,
                "LeadTouchCount": lead_touch_count,
                "ContactTouchCount": contact_touch_count,
                "AccountTouchCount": account_touch_count,
                "OpportunityTouchCount": opportunity_touch_count,
                "AssociatedTouchCount": associated_touch_count,
                "MeetingBookedCount": meeting_booked_count,
                "MeetingHeldCount": meeting_held_count,
                "ResponseCount": response_count,
                "CampaignCount": campaign_count,
                "SourcedARR": sourced_arr,
                "SourcedOpportunityCount": 1 if sourced_opp_id else 0,
                "LeadCount": 1,
                "SLAEligibleCount": 1,
                "SLAMetCount": 1 if sla_met else 0,
                "SLABreachCount": 1 if sla_breach else 0,
                "ConnectedLeadCount": 1
                if (touch_count > 0 or response_count > 0)
                else 0,
                "MeetingBookedLeadCount": 1 if meeting_booked_count > 0 else 0,
                "MeetingHeldLeadCount": 1 if meeting_held_count > 0 else 0,
                "MeetingTimedLeadCount": 1 if days_to_first_meeting > 0 else 0,
                "QualifiedCount": 1 if converted_flag == "true" else 0,
                "OpenLeadCount": 1 if is_open_actionable else 0,
                "MQLLeadCount": 1 if is_mql else 0,
                "SQLLeadCount": 1 if is_sql else 0,
                "DisqualifiedLeadCount": 1 if is_disqualified else 0,
                "MarketingDisqualifiedLeadCount": 1 if is_marketing_disqualified else 0,
                "SalesDisqualifiedLeadCount": 1 if is_sales_disqualified else 0,
                "OpportunityHandoffCount": 1 if converted_flag == "true" else 0,
                "OpenMQLLeadCount": 1 if (is_open_actionable and is_mql) else 0,
                "OpenSQLLeadCount": 1 if (is_open_actionable and is_sql) else 0,
                "DirectLeadTouch24hCount": 1 if direct_lead_touch_24h else 0,
                "DirectLeadTouchAnyCount": 1 if direct_lead_touch_any else 0,
                "AssociatedTouch24hCount": 1 if associated_touch_24h else 0,
                "AssociatedTouchAnyCount": 1 if associated_touch_any else 0,
                "CompanyTouchCount": 1 if company else 0,
                "ProspectLeadCount": 1
                if str(context_row.get("ClientBaseClass") or "") == "Prospect"
                else 0,
                "CurrentClientLeadCount": 1
                if str(context_row.get("ClientBaseClass") or "") == "Current Client"
                else 0,
                "FormerClientLeadCount": 1
                if str(context_row.get("ClientBaseClass") or "") == "Former Client"
                else 0,
                "PartnerLeadCount": 1
                if str(context_row.get("ClientBaseClass") or "")
                in {"Partner", "Affiliate"}
                else 0,
                "StrongHandoffCount": 1 if strong_handoff else 0,
                "DiscoveryHandoffCount": 1 if discovery_handoff else 0,
                "PendingStage3ReviewCount": 1 if pending_stage3_review else 0,
                "Stage3ApprovedCount": 1 if stage3_approved else 0,
                "QueueTaskCount": 0,
                "OverdueTaskCount": 0,
                "DueTodayTaskCount": 0,
                "TotalActivityCount": 0,
                "LeadLinkedActivityCount": 0,
                "ContactLinkedActivityCount": 0,
                "AccountLinkedActivityCount": 0,
                "OpportunityLinkedActivityCount": 0,
                "StageOrder": stage_order,
            }
        )

        integrity_key = (
            context["OwnerName"],
            context["ManagerName"],
            context["BDRTeam"],
            context["BDRRole"],
        )
        owner_integrity[integrity_key]["LeadCount"] += 1.0
        owner_integrity[integrity_key]["LeadTouch24hCount"] += (
            1.0 if direct_lead_touch_24h else 0.0
        )
        owner_integrity[integrity_key]["LeadTouchAnyCount"] += (
            1.0 if direct_lead_touch_any else 0.0
        )
        owner_integrity[integrity_key]["AssociatedTouch24hCount"] += (
            1.0 if associated_touch_24h else 0.0
        )
        owner_integrity[integrity_key]["AssociatedTouchAnyCount"] += (
            1.0 if associated_touch_any else 0.0
        )
        client_base_class = str(context_row.get("ClientBaseClass") or "")
        if client_base_class == "Current Client":
            owner_integrity[integrity_key]["CurrentClientActivityCount"] += 0.0
        elif client_base_class == "Former Client":
            owner_integrity[integrity_key]["FormerClientActivityCount"] += 0.0
        elif client_base_class == "Prospect":
            owner_integrity[integrity_key]["ProspectActivityCount"] += 0.0
        elif client_base_class in {"Partner", "Affiliate"}:
            owner_integrity[integrity_key]["PartnerActivityCount"] += 0.0
        else:
            owner_integrity[integrity_key]["UnclassifiedActivityCount"] += 0.0

        add_rep_week(
            attributed_owner_id,
            created_date,
            source_group,
            company,
            LeadCreatedCount=1,
            SLAMetCount=1 if sla_met else 0,
            SLABreachCount=1 if sla_breach else 0,
        )
        if converted_date and converted_flag == "true":
            add_rep_week(
                attributed_owner_id,
                converted_date,
                source_group,
                company,
                QualifiedCount=1,
                SourcedARR=sourced_arr,
            )
            _add_owner_week_metrics(
                attributed_owner_id, converted_date, QualifiedCount=1
            )

        for campaign_row in campaign_rows_by_lead.get(lead_id, []):
            campaign_obj = campaign_row.get("Campaign") or {}
            campaign_name = ((campaign_obj.get("Name")) or "Unmapped Campaign")[:255]
            if campaign_name == "Unmapped Campaign":
                continue
            campaign_key = (
                context["BDRTeam"],
                context["OwnerName"],
                context["ManagerName"],
                _fy_label(created_date),
                campaign_name,
                source_group[:255],
                ((campaign_obj.get("Type")) or "Unknown")[:255],
                ((campaign_obj.get("Campaign_Product__c")) or "Unknown")[:255],
                ((campaign_obj.get("Lead_Scope_Type__c")) or "Unknown")[:255],
            )
            campaign_summary[campaign_key]["LeadCount"] += 1.0
            campaign_summary[campaign_key]["MQLLeadCount"] += 1.0 if is_mql else 0.0
            campaign_summary[campaign_key]["SQLLeadCount"] += 1.0 if is_sql else 0.0
            campaign_summary[campaign_key]["DisqualifiedLeadCount"] += (
                1.0 if is_disqualified else 0.0
            )
            campaign_summary[campaign_key]["OpportunityHandoffCount"] += (
                1.0 if converted_flag == "true" else 0.0
            )
            campaign_summary[campaign_key]["MeetingBookedCount"] += meeting_booked_count
            campaign_summary[campaign_key]["MeetingHeldCount"] += meeting_held_count
            if campaign_row.get("HasResponded"):
                campaign_summary[campaign_key]["ResponseCount"] += 1.0
                add_rep_week(
                    attributed_owner_id,
                    (campaign_row.get("CreatedDate") or "")[:10],
                    source_group,
                    company,
                    ResponseCount=1,
                )
            if converted_flag == "true":
                campaign_summary[campaign_key]["QualifiedCount"] += 1.0
                campaign_summary[campaign_key]["SourcedARR"] += sourced_arr

    for opp in bdr_created_opps:
        opp_id = str(opp.get("Id") or "")
        if not opp_id:
            continue

        attributed_owner_id = str(opp.get("CreatedById") or "")
        if attributed_owner_id not in user_by_id:
            fallback_owner_id = str(opp.get("OwnerId") or "")
            if fallback_owner_id in user_by_id:
                attributed_owner_id = fallback_owner_id
            else:
                continue

        context = owner_context(attributed_owner_id)
        stage_name = str(opp.get("StageName") or "")
        stage_rank = _stage_rank(stage_name)
        stage2_date = str(opp.get("New_Stage_15_Date__c") or "")[:10]
        stage3_date = str(opp.get("New_Stage_20_Date__c") or "")[:10]
        created_date = str(opp.get("CreatedDate") or "")[:10]
        source = _normalize_source(opp.get("LeadSource"))
        source_group = _source_group(source)
        if source == "Unknown":
            source_group = "Opportunity Created"

        account = account_by_id.get(str(opp.get("AccountId") or ""), {})
        company = ((account.get("Name") or opp.get("Name") or "") or "")[:255]
        industry = ((account.get("Industry") or "") or "")[:255]
        country = (
            (account.get("BillingCountry") or account.get("ShippingCountry") or "")
            or ""
        )[:255]
        client_base_class = _account_base_class(account)[:255]
        targeted_product, targeted_product_source = _targeted_product_signal(
            "",
            opp.get("APTS_RH_Product_Family__c"),
            opp.get("Stage_with_Product_Scope__c"),
            account.get("Product_Opportunity__c"),
            account.get("Product_Mainline__c"),
        )
        opportunity_product_raw = _primary_product_signal(
            opp.get("APTS_RH_Product_Family__c")
        )
        opportunity_product, opportunity_product_source = _opportunity_product_signal(
            opp.get("APTS_RH_Product_Family__c"),
            account.get("Product_Opportunity__c"),
            account.get("Product_Mainline__c"),
        )
        handoff_quality_band = _handoff_quality_band(opp)
        pending_stage3_review = _coerce_bool(
            opp.get("Submit_for_Stage_20_Review__c")
        ) and not _coerce_bool(opp.get("Stage_20_Approval__c"))
        stage3_approved = _coerce_bool(opp.get("Stage_20_Approval__c"))
        strong_handoff = handoff_quality_band in {"Strong", "Stage 3+"}
        discovery_handoff = handoff_quality_band == "Discovery"
        stage2_discovery = bool(stage2_date) or stage_rank == 2
        stage3_engagement = bool(stage3_date) or stage_rank >= 3
        sourced_arr = round(safe_float(opp.get("ConvertedARR")), 2)
        qualification_stage = (
            "Stage 3 Engagement"
            if stage3_engagement
            else "Stage 2 Discovery"
            if stage2_discovery
            else "Opportunity Created"
        )
        next_best_action = (
            "Confirm sales acceptance and engagement plan"
            if stage3_engagement
            else "Advance discovery to engagement gate"
            if stage2_discovery
            else "Drive first discovery with AE"
        )
        suggested_tool = "AE Handoff" if stage3_engagement else "Salesforce Engage"

        detail_rows.append(
            {
                "RecordType": "opportunity_detail",
                "LeadId": "",
                "LeadName": "",
                "Company": company,
                "OwnerId": attributed_owner_id,
                "OwnerName": context["OwnerName"],
                "ManagerName": context["ManagerName"],
                "BDRTeam": context["BDRTeam"],
                "BDRRole": context["BDRRole"],
                "QueueGroup": context["QueueGroup"],
                "Department": context["Department"],
                "LeadTitle": "",
                "ContactOfficialTitle": "",
                "Persona": "",
                "Industry": industry,
                "IndustryGroup": _industry_group(industry),
                "Country": country,
                "LeadSource": source[:255],
                "SourceGroup": source_group[:255],
                "Campaign": "",
                "CampaignType": "",
                "CampaignProduct": "",
                "CampaignScopeType": "",
                "CampaignPurpose": "",
                "YearLabel": _fy_label(stage3_date or stage2_date or created_date),
                "MatchedAccountName": company,
                "MatchedAccountIndustry": industry,
                "MatchedAccountRegion": (account.get("Region__c") or "")[:255],
                "MatchedAccountSegment": (account.get("TAM_Universe_Segment__c") or "")[
                    :255
                ],
                "MatchedAccountTier": (account.get("Tier_Calculation__c") or "")[:255],
                "ContextAccountId": str(opp.get("AccountId") or ""),
                "ContextAccountName": company,
                "ContextAccountSource": "Opportunity Account",
                "ContextAccountType": (account.get("Type") or "")[:255],
                "ContextAccountIndustry": industry,
                "ContextAccountRegion": (account.get("Region__c") or "")[:255],
                "ContextAccountSegment": (account.get("TAM_Universe_Segment__c") or "")[
                    :255
                ],
                "ContextAccountTier": (account.get("Tier_Calculation__c") or "")[:255],
                "ContextCustomerSegment": (account.get("Customer_Segment__c") or "")[
                    :255
                ],
                "ContextProductOpportunity": (
                    account.get("Product_Opportunity__c") or ""
                )[:255],
                "ContextProductMainline": (account.get("Product_Mainline__c") or "")[
                    :255
                ],
                "TargetedProduct": targeted_product[:255],
                "TargetedProductSource": targeted_product_source[:255],
                "OpportunityProductRaw": opportunity_product_raw[:255],
                "OpportunityProductRawSource": "Opportunity Product"
                if opportunity_product_raw
                else "",
                "OpportunityProduct": opportunity_product[:255],
                "OpportunityProductSource": opportunity_product_source[:255],
                "ClientBaseClass": client_base_class,
                "FormerClientLostDate": str(
                    (account.get("Heat_Map_Red_Lostdate__c") or "")
                )[:10],
                "FormerClientAgeBand": _former_client_age_band(account, today)[:255],
                "TelemarketingStatus": (account.get("TM_Account_Status__c") or "")[
                    :255
                ],
                "TargetAccountFlag": "false",
                "Status": "",
                "QualificationStage": qualification_stage,
                "LifecycleStage": qualification_stage,
                "PriorityBand": "High"
                if stage3_engagement or pending_stage3_review
                else "Medium",
                "NextBestAction": next_best_action[:255],
                "SuggestedTool": suggested_tool[:255],
                "ConvertedFlag": "false",
                "SLAMetFlag": "false",
                "SLABreachFlag": "false",
                "HasResponseFlag": "false",
                "HasTouchFlag": "false",
                "HasMeetingFlag": "false",
                "HasMeetingHeldFlag": "false",
                "DirectLeadTouch24hFlag": "false",
                "DirectLeadTouchAnyFlag": "false",
                "AssociatedTouch24hFlag": "false",
                "AssociatedTouchAnyFlag": "false",
                "FirstTouchPath": "Opportunity Created",
                "SourcedOpportunityId": opp_id,
                "SourcedOpportunityName": (opp.get("Name") or "")[:255],
                "SourcedOpportunityStage": stage_name[:255],
                "SourcedForecastCategory": (opp.get("ForecastCategoryName") or "")[
                    :255
                ],
                "SourcedOpportunityType": (opp.get("Type") or "")[:255],
                "SourcedProductFamily": opportunity_product[:255],
                "HandoffQualityBand": handoff_quality_band[:255],
                "Stage2Date": stage2_date,
                "Stage3Date": stage3_date,
                "CreatedDate": created_date,
                "ConvertedDate": "",
                "MonthStartDate": _month_start(
                    stage3_date or stage2_date or created_date
                ),
                "FirstTouchDate": "",
                "LastTouchDate": "",
                "NextMeetingDate": "",
                "UpcomingMeetingCount": 0,
                "WeekStartDate": _week_start(created_date),
                "PriorityScore": 90.0
                if stage3_engagement
                else 60.0
                if stage2_discovery
                else 30.0,
                "LeadScore": 0.0,
                "LeadAgeDays": _days_between(created_date, today_iso),
                "DaysToFirstTouch": 0,
                "DaysSinceLastTouch": 0,
                "Stage2To3Days": _days_between(stage2_date, stage3_date),
                "TouchCount": 0,
                "CallCount": 0,
                "EmailCount": 0,
                "LeadTouchCount": 0,
                "ContactTouchCount": 0,
                "AccountTouchCount": 0,
                "OpportunityTouchCount": 0,
                "AssociatedTouchCount": 0,
                "MeetingBookedCount": 0,
                "MeetingHeldCount": 0,
                "ResponseCount": 0,
                "CampaignCount": 0,
                "SourcedARR": sourced_arr,
                "SourcedOpportunityCount": 1,
                "LeadCount": 0,
                "SLAEligibleCount": 0,
                "SLAMetCount": 0,
                "SLABreachCount": 0,
                "ConnectedLeadCount": 0,
                "MeetingBookedLeadCount": 0,
                "MeetingHeldLeadCount": 0,
                "QualifiedCount": 1 if stage3_engagement else 0,
                "OpenLeadCount": 0,
                "MQLLeadCount": 0,
                "SQLLeadCount": 0,
                "DisqualifiedLeadCount": 0,
                "MarketingDisqualifiedLeadCount": 0,
                "SalesDisqualifiedLeadCount": 0,
                "OpportunityHandoffCount": 1 if stage3_engagement else 0,
                "OpenMQLLeadCount": 0,
                "OpenSQLLeadCount": 0,
                "DirectLeadTouch24hCount": 0,
                "DirectLeadTouchAnyCount": 0,
                "AssociatedTouch24hCount": 0,
                "AssociatedTouchAnyCount": 0,
                "CompanyTouchCount": 1 if company else 0,
                "ProspectLeadCount": 1 if client_base_class == "Prospect" else 0,
                "CurrentClientLeadCount": 1
                if client_base_class == "Current Client"
                else 0,
                "FormerClientLeadCount": 1
                if client_base_class == "Former Client"
                else 0,
                "PartnerLeadCount": 1
                if client_base_class in {"Partner", "Affiliate"}
                else 0,
                "StrongHandoffCount": 1 if strong_handoff else 0,
                "DiscoveryHandoffCount": 1
                if discovery_handoff or stage2_discovery
                else 0,
                "PendingStage3ReviewCount": 1 if pending_stage3_review else 0,
                "Stage3ApprovedCount": 1 if stage3_approved else 0,
                "QueueTaskCount": 0,
                "OverdueTaskCount": 0,
                "DueTodayTaskCount": 0,
                "TotalActivityCount": 0,
                "LeadLinkedActivityCount": 0,
                "ContactLinkedActivityCount": 0,
                "AccountLinkedActivityCount": 0,
                "OpportunityLinkedActivityCount": 0,
                "StageOrder": stage_rank,
            }
        )

        if stage2_date:
            add_rep_week(attributed_owner_id, stage2_date, source_group, company)
        if stage3_date:
            add_rep_week(
                attributed_owner_id,
                stage3_date,
                source_group,
                company,
                QualifiedCount=1,
                SourcedARR=sourced_arr,
            )
            _add_owner_week_metrics(attributed_owner_id, stage3_date, QualifiedCount=1)

    account_leads_by_account: dict[str, list[str]] = defaultdict(list)
    for lead_id, context_row in lead_context_by_id.items():
        account_id = str(context_row.get("ContextAccountId") or "")
        if account_id:
            account_leads_by_account[account_id].append(lead_id)

    opps_by_account: dict[str, list[dict[str, object]]] = defaultdict(list)
    for opp in bdr_created_opps:
        account_id = str(opp.get("AccountId") or "")
        if account_id:
            opps_by_account[account_id].append(opp)

    account_history_opps_by_account: dict[str, list[dict[str, object]]] = defaultdict(
        list
    )
    for opp in account_history_opps:
        account_id = str(opp.get("AccountId") or "")
        if account_id:
            account_history_opps_by_account[account_id].append(opp)

    contacts_by_account: dict[str, list[dict[str, object]]] = defaultdict(list)
    for contact in contact_by_id.values():
        account_id = str(contact.get("AccountId") or "")
        if account_id:
            contacts_by_account[account_id].append(contact)

    detail_rows_account: list[dict[str, object]] = []
    for account in direct_accounts:
        account_id = str(account.get("Id") or "")
        owner_id = str(account.get("OwnerId") or "")
        if not account_id or owner_id not in user_by_id:
            continue
        context = owner_context(owner_id)
        client_base_class = _account_base_class(account)
        former_client_age_band = _former_client_age_band(account, today)
        account_leads = account_leads_by_account.get(account_id, [])
        account_contacts = contacts_by_account.get(account_id, [])
        account_opps = opps_by_account.get(account_id, [])
        activity = account_activity.get(account_id, {})
        personas = {
            str(c.get("Dimension_Persona__c") or "").strip()
            for c in account_contacts
            if str(c.get("Dimension_Persona__c") or "").strip()
        }
        open_lead_count = 0
        mql_count = 0
        sql_count = 0
        response_count = 0
        meeting_held_count = 0
        qualified_count = 0
        sourced_arr = 0.0
        campaign_count = 0
        for lead_id in account_leads:
            lead = leads_by_id.get(lead_id, {})
            status_lower = str(lead.get("Status") or "").strip().lower()
            is_marketing_disqualified = status_lower == "disqualified by marketing"
            is_sales_disqualified = status_lower == "disqualified by sales"
            if not lead.get("IsConverted") and not (
                is_marketing_disqualified or is_sales_disqualified
            ):
                open_lead_count += 1
            if status_lower == "qualified by marketing":
                mql_count += 1
            if status_lower == "hot lead":
                sql_count += 1
            lead_act = lead_activity.get(lead_id, {})
            response_count += int(
                sum(
                    1
                    for row in campaign_rows_by_lead.get(lead_id, [])
                    if row.get("HasResponded")
                )
                + safe_float(lead_act.get("ReplyCount"))
            )
            meeting_held_count += int(safe_float(lead_act.get("MeetingHeldCount")))
            campaign_count += len(campaign_rows_by_lead.get(lead_id, []))
            qualified_count += 1 if lead.get("ConvertedOpportunityId") else 0
        discovery_count = 0
        stage3_count = 0
        pending_stage3_review_count = 0
        stage3_approved_count = 0
        for opp in account_opps:
            stage_name = str(opp.get("StageName") or "")
            stage_rank = _stage_rank(stage_name)
            if bool(opp.get("New_Stage_15_Date__c")) or stage_rank == 2:
                discovery_count += 1
            if bool(opp.get("New_Stage_20_Date__c")) or stage_rank >= 3:
                stage3_count += 1
            pending_stage3_review_count += (
                1
                if (
                    _coerce_bool(opp.get("Submit_for_Stage_20_Review__c"))
                    and not _coerce_bool(opp.get("Stage_20_Approval__c"))
                )
                else 0
            )
            stage3_approved_count += (
                1 if _coerce_bool(opp.get("Stage_20_Approval__c")) else 0
            )
            sourced_arr += round(safe_float(opp.get("ConvertedARR")), 2)
        last_touch_date = str(activity.get("LastTouchDate") or "")
        days_since_last_touch = (
            _days_between(last_touch_date, today_iso) if last_touch_date else 999
        )
        next_best_action = _account_next_best_action(
            client_base_class,
            former_client_age_band,
            days_since_last_touch,
            discovery_count,
            pending_stage3_review_count,
            int(safe_float(account.get("Persona_Contacts__c"))) or len(personas),
        )
        suggested_tool = _account_suggested_tool(
            client_base_class,
            former_client_age_band,
            discovery_count,
            pending_stage3_review_count,
            days_since_last_touch,
        )
        targeted_product, targeted_product_source = _targeted_product_signal(
            "",
            "",
            "",
            account.get("Product_Opportunity__c"),
            account.get("Product_Mainline__c"),
        )
        opportunity_product, opportunity_product_source = _opportunity_product_signal(
            "",
            account.get("Product_Opportunity__c"),
            account.get("Product_Mainline__c"),
        )
        detail_rows_account.append(
            {
                "RecordType": "account_universe",
                "LeadId": "",
                "LeadName": "",
                "ContactId": "",
                "ContactName": "",
                "Company": (account.get("Name") or "")[:255],
                "OwnerId": owner_id,
                "OwnerName": context["OwnerName"],
                "ManagerName": context["ManagerName"],
                "BDRTeam": context["BDRTeam"],
                "BDRRole": context["BDRRole"],
                "QueueGroup": context["QueueGroup"],
                "Department": context["Department"],
                "LeadTitle": "",
                "ContactOfficialTitle": "",
                "Persona": "",
                "Industry": (account.get("Industry") or "")[:255],
                "IndustryGroup": _industry_group(account.get("Industry")),
                "Country": "",
                "LeadSource": "",
                "SourceGroup": "Account Universe",
                "Campaign": "",
                "CampaignType": "",
                "CampaignProduct": "",
                "CampaignScopeType": "",
                "CampaignPurpose": "",
                "YearLabel": "FY2026",
                "MatchedAccountName": (account.get("Name") or "")[:255],
                "MatchedAccountIndustry": (account.get("Industry") or "")[:255],
                "MatchedAccountRegion": (account.get("Region__c") or "")[:255],
                "MatchedAccountSegment": (account.get("TAM_Universe_Segment__c") or "")[
                    :255
                ],
                "MatchedAccountTier": (account.get("Tier_Calculation__c") or "")[:255],
                "ContextAccountId": account_id,
                "ContextAccountName": (account.get("Name") or "")[:255],
                "ContextAccountSource": "Owned Account",
                "ContextAccountType": (account.get("Type") or "")[:255],
                "ContextAccountIndustry": (account.get("Industry") or "")[:255],
                "ContextAccountRegion": (account.get("Region__c") or "")[:255],
                "ContextAccountSegment": (account.get("TAM_Universe_Segment__c") or "")[
                    :255
                ],
                "ContextAccountTier": (account.get("Tier_Calculation__c") or "")[:255],
                "ContextCustomerSegment": (account.get("Customer_Segment__c") or "")[
                    :255
                ],
                "ContextProductOpportunity": (
                    account.get("Product_Opportunity__c") or ""
                )[:255],
                "ContextProductMainline": (account.get("Product_Mainline__c") or "")[
                    :255
                ],
                "TargetedProduct": targeted_product[:255],
                "TargetedProductSource": targeted_product_source[:255],
                "OpportunityProductRaw": "",
                "OpportunityProductRawSource": "",
                "OpportunityProduct": opportunity_product[:255],
                "OpportunityProductSource": opportunity_product_source[:255],
                "ClientBaseClass": client_base_class[:255],
                "FormerClientLostDate": str(
                    (account.get("Heat_Map_Red_Lostdate__c") or "")
                )[:10],
                "FormerClientAgeBand": former_client_age_band[:255],
                "TelemarketingStatus": (account.get("TM_Account_Status__c") or "")[
                    :255
                ],
                "ExCustomerProspectingDate": str(
                    (account.get("Ex_Customer_Prospecting_Date__c") or "")
                )[:10],
                "TargetAccountFlag": "true",
                "Status": "",
                "QualificationStage": "Account Universe",
                "LifecycleStage": "Account Universe",
                "PriorityBand": "High"
                if next_best_action
                in {
                    "Drive Stage 2 -> 3 handoff review",
                    "Launch former-client re-entry play",
                }
                else "Medium",
                "NextBestAction": next_best_action[:255],
                "SuggestedTool": suggested_tool[:255],
                "ConvertedFlag": "false",
                "SLAMetFlag": "false",
                "SLABreachFlag": "false",
                "HasResponseFlag": str(response_count > 0).lower(),
                "HasTouchFlag": str(safe_float(activity.get("TouchCount")) > 0).lower(),
                "HasMeetingFlag": str(
                    safe_float(activity.get("MeetingBookedCount")) > 0
                ).lower(),
                "HasMeetingHeldFlag": str(
                    safe_float(activity.get("MeetingHeldCount")) > 0
                ).lower(),
                "DirectLeadTouch24hFlag": "false",
                "DirectLeadTouchAnyFlag": "false",
                "AssociatedTouch24hFlag": "false",
                "AssociatedTouchAnyFlag": str(
                    safe_float(activity.get("TouchCount")) > 0
                ).lower(),
                "FirstTouchPath": "Account",
                "SourcedOpportunityId": "",
                "SourcedOpportunityName": "",
                "SourcedOpportunityStage": "",
                "SourcedForecastCategory": "",
                "SourcedOpportunityType": "",
                "SourcedProductFamily": "",
                "HandoffQualityBand": "",
                "Stage2Date": "",
                "Stage3Date": "",
                "CreatedDate": "",
                "ConvertedDate": "",
                "MonthStartDate": "",
                "FirstTouchDate": "",
                "LastTouchDate": last_touch_date,
                "NextMeetingDate": str(activity.get("NextMeetingDate") or ""),
                "UpcomingMeetingCount": 1
                if str(activity.get("NextMeetingDate") or "")
                else 0,
                "WeekStartDate": "",
                "PriorityScore": 0.0,
                "LeadScore": 0.0,
                "LeadAgeDays": 0,
                "DaysToFirstTouch": 0,
                "DaysSinceLastTouch": 0
                if days_since_last_touch == 999
                else days_since_last_touch,
                "Stage2To3Days": 0,
                "TouchCount": int(safe_float(activity.get("TouchCount"))),
                "CallCount": int(safe_float(activity.get("CallCount"))),
                "EmailCount": int(safe_float(activity.get("EmailCount"))),
                "LeadTouchCount": 0,
                "ContactTouchCount": 0,
                "AccountTouchCount": int(safe_float(activity.get("TouchCount"))),
                "OpportunityTouchCount": 0,
                "AssociatedTouchCount": int(safe_float(activity.get("TouchCount"))),
                "MeetingBookedCount": int(
                    safe_float(activity.get("MeetingBookedCount"))
                ),
                "MeetingHeldCount": int(safe_float(activity.get("MeetingHeldCount")))
                + meeting_held_count,
                "LeadCreatedCount": 0,
                "ResponseCount": response_count,
                "CampaignCount": campaign_count,
                "SourcedARR": round(sourced_arr, 2),
                "SourcedOpportunityCount": len(account_opps),
                "LeadCount": len(account_leads),
                "SLAEligibleCount": 0,
                "SLAMetCount": 0,
                "SLABreachCount": 0,
                "ConnectedLeadCount": len(account_leads),
                "MeetingBookedLeadCount": 0,
                "MeetingHeldLeadCount": meeting_held_count,
                "QualifiedCount": qualified_count,
                "OpenLeadCount": open_lead_count,
                "MQLLeadCount": mql_count,
                "SQLLeadCount": sql_count,
                "DisqualifiedLeadCount": 0,
                "MarketingDisqualifiedLeadCount": 0,
                "SalesDisqualifiedLeadCount": 0,
                "OpportunityHandoffCount": stage3_count,
                "OpenMQLLeadCount": 0,
                "OpenSQLLeadCount": 0,
                "DirectLeadTouch24hCount": 0,
                "DirectLeadTouchAnyCount": 0,
                "AssociatedTouch24hCount": 0,
                "AssociatedTouchAnyCount": 0,
                "CompanyTouchCount": 1,
                "ProspectLeadCount": 1 if client_base_class == "Prospect" else 0,
                "CurrentClientLeadCount": 1
                if client_base_class == "Current Client"
                else 0,
                "FormerClientLeadCount": 1
                if client_base_class == "Former Client"
                else 0,
                "PartnerLeadCount": 1
                if client_base_class in {"Partner", "Affiliate"}
                else 0,
                "StrongHandoffCount": stage3_count,
                "DiscoveryHandoffCount": discovery_count,
                "PendingStage3ReviewCount": pending_stage3_review_count,
                "Stage3ApprovedCount": stage3_approved_count,
                "QueueTaskCount": 0,
                "OverdueTaskCount": 0,
                "DueTodayTaskCount": 0,
                "TotalActivityCount": int(safe_float(activity.get("TouchCount"))),
                "LeadLinkedActivityCount": 0,
                "ContactLinkedActivityCount": 0,
                "AccountLinkedActivityCount": int(
                    safe_float(activity.get("TouchCount"))
                ),
                "OpportunityLinkedActivityCount": 0,
                "ProspectActivityCount": int(safe_float(activity.get("TouchCount")))
                if client_base_class == "Prospect"
                else 0,
                "CurrentClientActivityCount": int(
                    safe_float(activity.get("TouchCount"))
                )
                if client_base_class == "Current Client"
                else 0,
                "FormerClientActivityCount": int(safe_float(activity.get("TouchCount")))
                if client_base_class == "Former Client"
                else 0,
                "PartnerActivityCount": int(safe_float(activity.get("TouchCount")))
                if client_base_class in {"Partner", "Affiliate"}
                else 0,
                "UnclassifiedActivityCount": int(safe_float(activity.get("TouchCount")))
                if client_base_class
                not in {
                    "Prospect",
                    "Current Client",
                    "Former Client",
                    "Partner",
                    "Affiliate",
                }
                else 0,
                "ContactCount": len(account_contacts),
                "PersonaContactCount": int(
                    safe_float(account.get("Persona_Contacts__c"))
                )
                if safe_float(account.get("Persona_Contacts__c"))
                else len(
                    [c for c in account_contacts if c.get("Dimension_Persona__c")]
                ),
                "UniquePersonaCount": int(safe_float(account.get("Unique_Personas__c")))
                if safe_float(account.get("Unique_Personas__c"))
                else len(personas),
                "CLevelPersonaCount": int(
                    safe_float(account.get("C_Level_Personas__c"))
                ),
                "HLevelPersonaCount": int(
                    safe_float(account.get("H_Level_Personas__c"))
                ),
                "NonPersonaContactCount": int(
                    safe_float(account.get("Non_Persona_Contacts__c"))
                ),
                "OpenOpportunityCount": len(account_opps),
                "StageOrder": 0,
            }
        )

    detail_rows_contact: list[dict[str, object]] = []
    for contact in direct_contacts:
        contact_id = str(contact.get("Id") or "")
        account_id = str(contact.get("AccountId") or "")
        owner_id = str(((contact.get("Account") or {}).get("OwnerId")) or "")
        if not contact_id or not account_id or owner_id not in user_by_id:
            continue
        account = account_by_id.get(account_id, {})
        context = owner_context(owner_id)
        activity = contact_activity.get(contact_id, {})
        lead_id = contact_primary_lead.get(contact_id, "")
        lead = leads_by_id.get(lead_id, {})
        source = _normalize_source(lead.get("LeadSource"))
        source_group = _source_group(source) if lead_id else "Contact Coverage"
        campaign_name = ""
        campaign_type = ""
        campaign_product = ""
        campaign_scope_type = ""
        campaign_purpose = ""
        if lead_id and campaign_rows_by_lead.get(lead_id):
            latest_row = max(
                campaign_rows_by_lead[lead_id],
                key=lambda item: str(item.get("CreatedDate") or ""),
            )
            campaign_obj = latest_row.get("Campaign") or {}
            campaign_name = ((campaign_obj.get("Name")) or "")[:255]
            campaign_type = ((campaign_obj.get("Type")) or "Unknown")[:255]
            campaign_product = ((campaign_obj.get("Campaign_Product__c")) or "Unknown")[
                :255
            ]
            campaign_scope_type = (
                (campaign_obj.get("Lead_Scope_Type__c")) or "Unknown"
            )[:255]
            campaign_purpose = ((campaign_obj.get("Campaign_Purpose__c")) or "Unknown")[
                :255
            ]
        targeted_product, targeted_product_source = _targeted_product_signal(
            campaign_product,
            "",
            "",
            account.get("Product_Opportunity__c"),
            account.get("Product_Mainline__c"),
        )
        opportunity_product, opportunity_product_source = _opportunity_product_signal(
            "",
            account.get("Product_Opportunity__c"),
            account.get("Product_Mainline__c"),
        )
        last_touch_date = str(activity.get("LastTouchDate") or "")
        days_since_last_touch = (
            _days_between(last_touch_date, today_iso) if last_touch_date else 999
        )
        client_base_class = _account_base_class(account)
        meeting_held_count = int(safe_float(activity.get("MeetingHeldCount")))
        next_best_action = _contact_next_best_action(
            days_since_last_touch, meeting_held_count, client_base_class
        )
        suggested_tool = _contact_suggested_tool(
            days_since_last_touch, meeting_held_count
        )
        detail_rows_contact.append(
            {
                "RecordType": "contact_coverage",
                "LeadId": lead_id,
                "LeadName": (lead.get("Name") or "")[:255],
                "ContactId": contact_id,
                "ContactName": (contact.get("Name") or "")[:255],
                "Company": (account.get("Name") or "")[:255],
                "OwnerId": owner_id,
                "OwnerName": context["OwnerName"],
                "ManagerName": context["ManagerName"],
                "BDRTeam": context["BDRTeam"],
                "BDRRole": context["BDRRole"],
                "QueueGroup": context["QueueGroup"],
                "Department": context["Department"],
                "LeadTitle": (lead.get("Title") or "")[:255],
                "ContactOfficialTitle": (
                    (contact.get("Official_Title__c") or "")
                    or (contact.get("Title") or "")
                )[:255],
                "Persona": (contact.get("Dimension_Persona__c") or "Unknown")[:255],
                "Industry": (account.get("Industry") or "")[:255],
                "IndustryGroup": _industry_group(account.get("Industry")),
                "Country": "",
                "LeadSource": source[:255],
                "SourceGroup": source_group[:255],
                "Campaign": campaign_name,
                "CampaignType": campaign_type,
                "CampaignProduct": campaign_product,
                "CampaignScopeType": campaign_scope_type,
                "CampaignPurpose": campaign_purpose,
                "YearLabel": "FY2026",
                "MatchedAccountName": (account.get("Name") or "")[:255],
                "MatchedAccountIndustry": (account.get("Industry") or "")[:255],
                "MatchedAccountRegion": (account.get("Region__c") or "")[:255],
                "MatchedAccountSegment": (account.get("TAM_Universe_Segment__c") or "")[
                    :255
                ],
                "MatchedAccountTier": (account.get("Tier_Calculation__c") or "")[:255],
                "ContextAccountId": account_id,
                "ContextAccountName": (account.get("Name") or "")[:255],
                "ContextAccountSource": "Owned Contact Account",
                "ContextAccountType": (account.get("Type") or "")[:255],
                "ContextAccountIndustry": (account.get("Industry") or "")[:255],
                "ContextAccountRegion": (account.get("Region__c") or "")[:255],
                "ContextAccountSegment": (account.get("TAM_Universe_Segment__c") or "")[
                    :255
                ],
                "ContextAccountTier": (account.get("Tier_Calculation__c") or "")[:255],
                "ContextCustomerSegment": (account.get("Customer_Segment__c") or "")[
                    :255
                ],
                "ContextProductOpportunity": (
                    account.get("Product_Opportunity__c") or ""
                )[:255],
                "ContextProductMainline": (account.get("Product_Mainline__c") or "")[
                    :255
                ],
                "TargetedProduct": targeted_product[:255],
                "TargetedProductSource": targeted_product_source[:255],
                "OpportunityProductRaw": "",
                "OpportunityProductRawSource": "",
                "OpportunityProduct": opportunity_product[:255],
                "OpportunityProductSource": opportunity_product_source[:255],
                "ClientBaseClass": client_base_class[:255],
                "FormerClientLostDate": str(
                    (account.get("Heat_Map_Red_Lostdate__c") or "")
                )[:10],
                "FormerClientAgeBand": _former_client_age_band(account, today)[:255],
                "TelemarketingStatus": (account.get("TM_Account_Status__c") or "")[
                    :255
                ],
                "ExCustomerProspectingDate": str(
                    (account.get("Ex_Customer_Prospecting_Date__c") or "")
                )[:10],
                "TargetAccountFlag": "true",
                "Status": (lead.get("Status") or "")[:255],
                "QualificationStage": "Contact Coverage",
                "LifecycleStage": "Contact Coverage",
                "PriorityBand": "High" if days_since_last_touch > 30 else "Medium",
                "NextBestAction": next_best_action[:255],
                "SuggestedTool": suggested_tool[:255],
                "ConvertedFlag": str(bool(lead.get("IsConverted"))).lower()
                if lead_id
                else "false",
                "SLAMetFlag": "false",
                "SLABreachFlag": "false",
                "HasResponseFlag": str(
                    bool(lead_id and campaign_rows_by_lead.get(lead_id))
                ).lower(),
                "HasTouchFlag": str(safe_float(activity.get("TouchCount")) > 0).lower(),
                "HasMeetingFlag": str(
                    safe_float(activity.get("MeetingBookedCount")) > 0
                ).lower(),
                "HasMeetingHeldFlag": str(meeting_held_count > 0).lower(),
                "DirectLeadTouch24hFlag": "false",
                "DirectLeadTouchAnyFlag": "false",
                "AssociatedTouch24hFlag": "false",
                "AssociatedTouchAnyFlag": str(
                    safe_float(activity.get("TouchCount")) > 0
                ).lower(),
                "FirstTouchPath": "Contact",
                "SourcedOpportunityId": str(lead.get("ConvertedOpportunityId") or ""),
                "SourcedOpportunityName": "",
                "SourcedOpportunityStage": "",
                "SourcedForecastCategory": "",
                "SourcedOpportunityType": "",
                "SourcedProductFamily": opportunity_product[:255],
                "HandoffQualityBand": "",
                "Stage2Date": "",
                "Stage3Date": "",
                "CreatedDate": "",
                "ConvertedDate": str(lead.get("ConvertedDate") or "")[:10]
                if lead_id
                else "",
                "MonthStartDate": "",
                "FirstTouchDate": "",
                "LastTouchDate": last_touch_date,
                "NextMeetingDate": str(activity.get("NextMeetingDate") or ""),
                "UpcomingMeetingCount": 1
                if str(activity.get("NextMeetingDate") or "")
                else 0,
                "WeekStartDate": "",
                "PriorityScore": 0.0,
                "LeadScore": round(safe_float(lead.get("pi__score__c")), 1)
                if lead_id
                else 0.0,
                "LeadAgeDays": 0,
                "DaysToFirstTouch": 0,
                "DaysSinceLastTouch": 0
                if days_since_last_touch == 999
                else days_since_last_touch,
                "Stage2To3Days": 0,
                "TouchCount": int(safe_float(activity.get("TouchCount"))),
                "CallCount": int(safe_float(activity.get("CallCount"))),
                "EmailCount": int(safe_float(activity.get("EmailCount"))),
                "LeadTouchCount": 0,
                "ContactTouchCount": int(safe_float(activity.get("TouchCount"))),
                "AccountTouchCount": 0,
                "OpportunityTouchCount": 0,
                "AssociatedTouchCount": int(safe_float(activity.get("TouchCount"))),
                "MeetingBookedCount": int(
                    safe_float(activity.get("MeetingBookedCount"))
                ),
                "MeetingHeldCount": meeting_held_count,
                "LeadCreatedCount": 0,
                "ResponseCount": 1
                if (lead_id and campaign_rows_by_lead.get(lead_id))
                else 0,
                "CampaignCount": len(campaign_rows_by_lead.get(lead_id, []))
                if lead_id
                else 0,
                "SourcedARR": 0.0,
                "SourcedOpportunityCount": 1
                if lead.get("ConvertedOpportunityId")
                else 0,
                "LeadCount": 0,
                "SLAEligibleCount": 0,
                "SLAMetCount": 0,
                "SLABreachCount": 0,
                "ConnectedLeadCount": 0,
                "MeetingBookedLeadCount": 0,
                "MeetingHeldLeadCount": 0,
                "QualifiedCount": 1 if lead.get("ConvertedOpportunityId") else 0,
                "OpenLeadCount": 0,
                "MQLLeadCount": 0,
                "SQLLeadCount": 0,
                "DisqualifiedLeadCount": 0,
                "MarketingDisqualifiedLeadCount": 0,
                "SalesDisqualifiedLeadCount": 0,
                "OpportunityHandoffCount": 0,
                "OpenMQLLeadCount": 0,
                "OpenSQLLeadCount": 0,
                "DirectLeadTouch24hCount": 0,
                "DirectLeadTouchAnyCount": 0,
                "AssociatedTouch24hCount": 0,
                "AssociatedTouchAnyCount": 0,
                "CompanyTouchCount": 1 if account_id else 0,
                "ProspectLeadCount": 1 if client_base_class == "Prospect" else 0,
                "CurrentClientLeadCount": 1
                if client_base_class == "Current Client"
                else 0,
                "FormerClientLeadCount": 1
                if client_base_class == "Former Client"
                else 0,
                "PartnerLeadCount": 1
                if client_base_class in {"Partner", "Affiliate"}
                else 0,
                "StrongHandoffCount": 0,
                "DiscoveryHandoffCount": 0,
                "PendingStage3ReviewCount": 0,
                "Stage3ApprovedCount": 0,
                "QueueTaskCount": 0,
                "OverdueTaskCount": 0,
                "DueTodayTaskCount": 0,
                "TotalActivityCount": int(safe_float(activity.get("TouchCount"))),
                "LeadLinkedActivityCount": 0,
                "ContactLinkedActivityCount": int(
                    safe_float(activity.get("TouchCount"))
                ),
                "AccountLinkedActivityCount": 0,
                "OpportunityLinkedActivityCount": 0,
                "ProspectActivityCount": int(safe_float(activity.get("TouchCount")))
                if client_base_class == "Prospect"
                else 0,
                "CurrentClientActivityCount": int(
                    safe_float(activity.get("TouchCount"))
                )
                if client_base_class == "Current Client"
                else 0,
                "FormerClientActivityCount": int(safe_float(activity.get("TouchCount")))
                if client_base_class == "Former Client"
                else 0,
                "PartnerActivityCount": int(safe_float(activity.get("TouchCount")))
                if client_base_class in {"Partner", "Affiliate"}
                else 0,
                "UnclassifiedActivityCount": int(safe_float(activity.get("TouchCount")))
                if client_base_class
                not in {
                    "Prospect",
                    "Current Client",
                    "Former Client",
                    "Partner",
                    "Affiliate",
                }
                else 0,
                "ContactCount": 1,
                "PersonaContactCount": 1
                if str(contact.get("Dimension_Persona__c") or "").strip()
                else 0,
                "UniquePersonaCount": 1
                if str(contact.get("Dimension_Persona__c") or "").strip()
                else 0,
                "CLevelPersonaCount": 0,
                "HLevelPersonaCount": 0,
                "NonPersonaContactCount": 1
                if not str(contact.get("Dimension_Persona__c") or "").strip()
                else 0,
                "OpenOpportunityCount": 0,
                "StageOrder": 0,
            }
        )

    detail_rows_account_product: list[dict[str, object]] = []
    detail_rows_account_persona_product: list[dict[str, object]] = []
    for account in direct_accounts:
        account_id = str(account.get("Id") or "")
        owner_id = str(account.get("OwnerId") or "")
        if not account_id or owner_id not in user_by_id:
            continue

        context = owner_context(owner_id)
        account_leads = account_leads_by_account.get(account_id, [])
        account_contacts = contacts_by_account.get(account_id, [])
        account_opps = opps_by_account.get(account_id, [])
        account_history_opps_for_product = account_history_opps_by_account.get(
            account_id, []
        )
        account_activity_row = account_activity.get(account_id, {})
        client_base_class = _account_base_class(account)
        former_client_age_band = _former_client_age_band(account, today)
        persona_contacts = [
            contact
            for contact in account_contacts
            if str(contact.get("Dimension_Persona__c") or "").strip()
        ]
        active_contacts = [
            contact
            for contact in account_contacts
            if safe_float(
                (contact_activity.get(str(contact.get("Id") or ""), {}) or {}).get(
                    "TouchCount"
                )
            )
            > 0
        ]
        active_persona_contacts = [
            contact
            for contact in active_contacts
            if str(contact.get("Dimension_Persona__c") or "").strip()
        ]
        account_touch_count = int(safe_float(account_activity_row.get("TouchCount")))
        account_personas = {
            str(contact.get("Dimension_Persona__c") or "").strip()
            for contact in persona_contacts
            if str(contact.get("Dimension_Persona__c") or "").strip()
        }
        persona_contact_counts: dict[str, int] = defaultdict(int)
        active_persona_contact_counts: dict[str, int] = defaultdict(int)
        persona_touch_counts: dict[str, int] = defaultdict(int)
        persona_meeting_counts: dict[str, int] = defaultdict(int)
        for contact in account_contacts:
            persona = str(contact.get("Dimension_Persona__c") or "").strip()
            if not persona:
                continue
            persona_contact_counts[persona] += 1
            c_activity = contact_activity.get(str(contact.get("Id") or ""), {})
            touch_count = int(safe_float(c_activity.get("TouchCount")))
            meeting_count = int(safe_float(c_activity.get("MeetingHeldCount")))
            persona_touch_counts[persona] += touch_count
            persona_meeting_counts[persona] += meeting_count
            if touch_count > 0:
                active_persona_contact_counts[persona] += 1

        product_metrics: dict[str, dict[str, object]] = defaultdict(
            lambda: {
                "sources": set(),
                "campaign_signal_count": 0,
                "opportunity_signal_count": 0,
                "opportunity_stage_scope_signal_count": 0,
                "account_opportunity_history_signal_count": 0,
                "account_opportunity_signal_count": 0,
                "account_mainline_signal_count": 0,
                "lead_ids": set(),
                "response_lead_ids": set(),
                "meeting_lead_ids": set(),
                "qualified_lead_ids": set(),
                "opportunity_ids": set(),
                "open_opportunity_ids": set(),
                "stage2_opportunity_ids": set(),
                "stage3_opportunity_ids": set(),
                "known_arr": 0.0,
            }
        )

        for product in _all_product_signals(account.get("Product_Opportunity__c")):
            metrics = product_metrics[product]
            metrics["sources"].add("Account Opportunity Product")
            metrics["account_opportunity_signal_count"] += 1

        for lead_id in account_leads:
            lead = leads_by_id.get(lead_id, {})
            lead_activity_row = lead_activity.get(lead_id, {})
            meeting_held_count = int(
                safe_float(lead_activity_row.get("MeetingHeldCount"))
            )
            converted = bool(lead.get("ConvertedOpportunityId"))
            for campaign_row in campaign_rows_by_lead.get(lead_id, []):
                campaign_obj = campaign_row.get("Campaign") or {}
                for product in _all_product_signals(
                    campaign_obj.get("Campaign_Product__c")
                ):
                    metrics = product_metrics[product]
                    metrics["sources"].add("Campaign Product")
                    metrics["campaign_signal_count"] += 1
                    metrics["lead_ids"].add(lead_id)
                    if campaign_row.get("HasResponded"):
                        metrics["response_lead_ids"].add(lead_id)
                    if meeting_held_count > 0:
                        metrics["meeting_lead_ids"].add(lead_id)
                    if converted:
                        metrics["qualified_lead_ids"].add(lead_id)

        for opp in account_opps:
            opp_id = str(opp.get("Id") or "")
            stage_name = str(opp.get("StageName") or "")
            stage_rank = _stage_rank(stage_name)
            stage2_discovery = bool(opp.get("New_Stage_15_Date__c")) or stage_rank == 2
            stage3_engagement = bool(opp.get("New_Stage_20_Date__c")) or stage_rank >= 3
            is_open = not bool(opp.get("IsClosed"))
            raw_products = _all_product_signals(opp.get("APTS_RH_Product_Family__c"))
            for product in raw_products:
                metrics = product_metrics[product]
                metrics["sources"].add("Opportunity Product")
                metrics["opportunity_signal_count"] += 1
                metrics["opportunity_ids"].add(opp_id)
                if is_open:
                    metrics["open_opportunity_ids"].add(opp_id)
                if stage2_discovery:
                    metrics["stage2_opportunity_ids"].add(opp_id)
                if stage3_engagement:
                    metrics["stage3_opportunity_ids"].add(opp_id)
                metrics["known_arr"] += round(safe_float(opp.get("ConvertedARR")), 2)
            if not raw_products:
                scope_product = _stage_scope_product_signal(
                    opp.get("Stage_with_Product_Scope__c")
                )
                if scope_product:
                    metrics = product_metrics[scope_product]
                    metrics["sources"].add("Opportunity Stage Scope")
                    metrics["opportunity_stage_scope_signal_count"] += 1
                    metrics["opportunity_ids"].add(opp_id)
                    if is_open:
                        metrics["open_opportunity_ids"].add(opp_id)
                    if stage2_discovery:
                        metrics["stage2_opportunity_ids"].add(opp_id)
                    if stage3_engagement:
                        metrics["stage3_opportunity_ids"].add(opp_id)
                    metrics["known_arr"] += round(
                        safe_float(opp.get("ConvertedARR")), 2
                    )

        for opp in account_history_opps_for_product:
            for product in _all_product_signals(opp.get("APTS_RH_Product_Family__c")):
                metrics = product_metrics[product]
                metrics["sources"].add("Account Opportunity History")
                metrics["account_opportunity_history_signal_count"] += 1

        account_mainline_products = _all_product_signals(
            account.get("Product_Mainline__c")
        )
        if not product_metrics:
            product_metrics["Unknown"]["sources"].add("Unknown")
            product_metrics["Unknown"]["account_mainline_signal_count"] = len(
                account_mainline_products
            )

        for product, metrics in sorted(
            product_metrics.items(), key=lambda item: item[0]
        ):
            sources = metrics["sources"]
            primary_source = _primary_product_source(sources)
            source_set = (
                " | ".join(
                    sorted(
                        sources, key=lambda item: (-_product_source_rank(item), item)
                    )
                )
                or "Unknown"
            )
            campaign_signal_count = int(metrics["campaign_signal_count"])
            opportunity_signal_count = int(metrics["opportunity_signal_count"])
            opportunity_stage_scope_signal_count = int(
                metrics["opportunity_stage_scope_signal_count"]
            )
            account_opportunity_history_signal_count = int(
                metrics["account_opportunity_history_signal_count"]
            )
            account_opportunity_signal_count = int(
                metrics["account_opportunity_signal_count"]
            )
            account_mainline_signal_count = int(
                metrics["account_mainline_signal_count"]
            )
            product_signal_confidence = _product_signal_confidence(primary_source)
            product_signal_confidence_score = _product_signal_confidence_score(
                primary_source
            )
            signal_strength = (
                opportunity_signal_count * 4
                + opportunity_stage_scope_signal_count * 3
                + account_opportunity_history_signal_count * 3
                + campaign_signal_count * 3
                + account_opportunity_signal_count * 2
                + account_mainline_signal_count
            )
            detail_rows_account_product.append(
                {
                    "RecordType": "account_product_target",
                    "LeadId": "",
                    "LeadName": "",
                    "ContactId": "",
                    "ContactName": "",
                    "Company": (account.get("Name") or "")[:255],
                    "OwnerId": owner_id,
                    "OwnerName": context["OwnerName"],
                    "ManagerName": context["ManagerName"],
                    "BDRTeam": context["BDRTeam"],
                    "BDRRole": context["BDRRole"],
                    "QueueGroup": context["QueueGroup"],
                    "Department": context["Department"],
                    "LeadTitle": "",
                    "ContactOfficialTitle": "",
                    "Persona": "",
                    "Industry": (account.get("Industry") or "")[:255],
                    "IndustryGroup": _industry_group(account.get("Industry")),
                    "Country": "",
                    "LeadSource": "",
                    "SourceGroup": "Account Product Target",
                    "Campaign": "",
                    "CampaignType": "",
                    "CampaignProduct": "",
                    "CampaignScopeType": "",
                    "CampaignPurpose": "",
                    "YearLabel": "FY2026",
                    "MatchedAccountName": (account.get("Name") or "")[:255],
                    "MatchedAccountIndustry": (account.get("Industry") or "")[:255],
                    "MatchedAccountRegion": (account.get("Region__c") or "")[:255],
                    "MatchedAccountSegment": (
                        account.get("TAM_Universe_Segment__c") or ""
                    )[:255],
                    "MatchedAccountTier": (account.get("Tier_Calculation__c") or "")[
                        :255
                    ],
                    "ContextAccountId": account_id,
                    "ContextAccountName": (account.get("Name") or "")[:255],
                    "ContextAccountSource": "Owned Account",
                    "ContextAccountType": (account.get("Type") or "")[:255],
                    "ContextAccountIndustry": (account.get("Industry") or "")[:255],
                    "ContextAccountRegion": (account.get("Region__c") or "")[:255],
                    "ContextAccountSegment": (
                        account.get("TAM_Universe_Segment__c") or ""
                    )[:255],
                    "ContextAccountTier": (account.get("Tier_Calculation__c") or "")[
                        :255
                    ],
                    "ContextCustomerSegment": (
                        account.get("Customer_Segment__c") or ""
                    )[:255],
                    "ContextProductOpportunity": (
                        account.get("Product_Opportunity__c") or ""
                    )[:255],
                    "ContextProductMainline": (
                        account.get("Product_Mainline__c") or ""
                    )[:255],
                    "TargetedProduct": product[:255],
                    "TargetedProductSource": primary_source[:255],
                    "ProductSignalConfidence": product_signal_confidence[:255],
                    "OpportunityProductRaw": "",
                    "OpportunityProductRawSource": "",
                    "OpportunityProduct": "",
                    "OpportunityProductSource": "",
                    "ProductSourceSet": source_set[:255],
                    "ClientBaseClass": client_base_class[:255],
                    "FormerClientLostDate": str(
                        (account.get("Heat_Map_Red_Lostdate__c") or "")
                    )[:10],
                    "FormerClientAgeBand": former_client_age_band[:255],
                    "TelemarketingStatus": (account.get("TM_Account_Status__c") or "")[
                        :255
                    ],
                    "ExCustomerProspectingDate": str(
                        (account.get("Ex_Customer_Prospecting_Date__c") or "")
                    )[:10],
                    "TargetAccountFlag": "true",
                    "Status": "",
                    "QualificationStage": "Account Product Target",
                    "LifecycleStage": "Account Product Target",
                    "PriorityBand": "High"
                    if signal_strength >= 6
                    else "Medium"
                    if signal_strength >= 3
                    else "Low",
                    "NextBestAction": "Validate product hypothesis and activate outreach"[
                        :255
                    ],
                    "SuggestedTool": "Campaign / Target List Planning"[:255],
                    "ConvertedFlag": "false",
                    "SLAMetFlag": "false",
                    "SLABreachFlag": "false",
                    "HasResponseFlag": str(bool(metrics["response_lead_ids"])).lower(),
                    "HasTouchFlag": str(account_touch_count > 0).lower(),
                    "HasMeetingFlag": str(bool(metrics["meeting_lead_ids"])).lower(),
                    "HasMeetingHeldFlag": str(
                        bool(metrics["meeting_lead_ids"])
                    ).lower(),
                    "DirectLeadTouch24hFlag": "false",
                    "DirectLeadTouchAnyFlag": "false",
                    "AssociatedTouch24hFlag": "false",
                    "AssociatedTouchAnyFlag": "false",
                    "FirstTouchPath": "Account Product Target",
                    "SourcedOpportunityId": "",
                    "SourcedOpportunityName": "",
                    "SourcedOpportunityStage": "",
                    "SourcedForecastCategory": "",
                    "SourcedOpportunityType": "",
                    "SourcedProductFamily": "",
                    "HandoffQualityBand": "",
                    "Stage2Date": "",
                    "Stage3Date": "",
                    "CreatedDate": "",
                    "ConvertedDate": "",
                    "MonthStartDate": "",
                    "FirstTouchDate": "",
                    "FirstMeetingDate": "",
                    "LastTouchDate": str(
                        account_activity_row.get("LastTouchDate") or ""
                    ),
                    "NextMeetingDate": str(
                        account_activity_row.get("NextMeetingDate") or ""
                    ),
                    "UpcomingMeetingCount": 1
                    if str(account_activity_row.get("NextMeetingDate") or "")
                    else 0,
                    "WeekStartDate": "",
                    "PriorityScore": float(signal_strength),
                    "LeadScore": 0.0,
                    "LeadAgeDays": 0,
                    "DaysToFirstTouch": 0,
                    "DaysToFirstMeeting": 0,
                    "DaysSinceLastTouch": _days_between(
                        str(account_activity_row.get("LastTouchDate") or ""), today_iso
                    )
                    if str(account_activity_row.get("LastTouchDate") or "")
                    else 0,
                    "Stage2To3Days": 0,
                    "TouchCount": account_touch_count,
                    "CallCount": 0,
                    "EmailCount": 0,
                    "LeadTouchCount": 0,
                    "ContactTouchCount": 0,
                    "AccountTouchCount": account_touch_count,
                    "OpportunityTouchCount": 0,
                    "AssociatedTouchCount": account_touch_count,
                    "MeetingBookedCount": 0,
                    "MeetingHeldCount": len(metrics["meeting_lead_ids"]),
                    "LeadCreatedCount": len(metrics["lead_ids"]),
                    "ResponseCount": len(metrics["response_lead_ids"]),
                    "CampaignCount": campaign_signal_count,
                    "SourcedARR": round(float(metrics["known_arr"]), 2),
                    "SourcedOpportunityCount": len(metrics["opportunity_ids"]),
                    "LeadCount": len(metrics["lead_ids"]),
                    "SLAEligibleCount": 0,
                    "SLAMetCount": 0,
                    "SLABreachCount": 0,
                    "ConnectedLeadCount": len(metrics["response_lead_ids"]),
                    "MeetingBookedLeadCount": 0,
                    "MeetingHeldLeadCount": len(metrics["meeting_lead_ids"]),
                    "MeetingTimedLeadCount": 0,
                    "QualifiedCount": len(metrics["qualified_lead_ids"]),
                    "OpenLeadCount": 0,
                    "MQLLeadCount": 0,
                    "SQLLeadCount": 0,
                    "DisqualifiedLeadCount": 0,
                    "MarketingDisqualifiedLeadCount": 0,
                    "SalesDisqualifiedLeadCount": 0,
                    "OpportunityHandoffCount": len(metrics["stage3_opportunity_ids"]),
                    "OpenMQLLeadCount": 0,
                    "OpenSQLLeadCount": 0,
                    "DirectLeadTouch24hCount": 0,
                    "DirectLeadTouchAnyCount": 0,
                    "AssociatedTouch24hCount": 0,
                    "AssociatedTouchAnyCount": 0,
                    "CompanyTouchCount": 1,
                    "ProspectLeadCount": 1 if client_base_class == "Prospect" else 0,
                    "CurrentClientLeadCount": 1
                    if client_base_class == "Current Client"
                    else 0,
                    "FormerClientLeadCount": 1
                    if client_base_class == "Former Client"
                    else 0,
                    "PartnerLeadCount": 1
                    if client_base_class in {"Partner", "Affiliate"}
                    else 0,
                    "StrongHandoffCount": len(metrics["stage3_opportunity_ids"]),
                    "DiscoveryHandoffCount": len(metrics["stage2_opportunity_ids"]),
                    "PendingStage3ReviewCount": 0,
                    "Stage3ApprovedCount": 0,
                    "QueueTaskCount": 0,
                    "OverdueTaskCount": 0,
                    "DueTodayTaskCount": 0,
                    "TotalActivityCount": account_touch_count,
                    "LeadLinkedActivityCount": 0,
                    "ContactLinkedActivityCount": 0,
                    "AccountLinkedActivityCount": account_touch_count,
                    "OpportunityLinkedActivityCount": 0,
                    "ProspectActivityCount": account_touch_count
                    if client_base_class == "Prospect"
                    else 0,
                    "CurrentClientActivityCount": account_touch_count
                    if client_base_class == "Current Client"
                    else 0,
                    "FormerClientActivityCount": account_touch_count
                    if client_base_class == "Former Client"
                    else 0,
                    "PartnerActivityCount": account_touch_count
                    if client_base_class in {"Partner", "Affiliate"}
                    else 0,
                    "UnclassifiedActivityCount": account_touch_count
                    if client_base_class
                    not in {
                        "Prospect",
                        "Current Client",
                        "Former Client",
                        "Partner",
                        "Affiliate",
                    }
                    else 0,
                    "ContactCount": len(account_contacts),
                    "PersonaContactCount": len(persona_contacts),
                    "UniquePersonaCount": len(account_personas),
                    "CLevelPersonaCount": int(
                        safe_float(account.get("C_Level_Personas__c"))
                    ),
                    "HLevelPersonaCount": int(
                        safe_float(account.get("H_Level_Personas__c"))
                    ),
                    "NonPersonaContactCount": int(
                        safe_float(account.get("Non_Persona_Contacts__c"))
                    ),
                    "OpenOpportunityCount": len(metrics["open_opportunity_ids"]),
                    "ActiveContactCount": len(active_contacts),
                    "ActivePersonaContactCount": len(active_persona_contacts),
                    "SignalSourceCount": len(sources),
                    "ProductSignalStrength": signal_strength,
                    "ProductSignalConfidenceScore": product_signal_confidence_score,
                    "CampaignProductSignalCount": campaign_signal_count,
                    "OpportunityProductSignalCount": opportunity_signal_count,
                    "AccountOpportunityHistorySignalCount": account_opportunity_history_signal_count,
                    "AccountProductSignalCount": account_opportunity_signal_count,
                    "AccountMainlineSignalCount": account_mainline_signal_count,
                    "StageOrder": 0,
                }
            )
            if product != "Unknown":
                for persona, persona_contact_count in sorted(
                    persona_contact_counts.items()
                ):
                    detail_rows_account_persona_product.append(
                        {
                            "RecordType": "account_persona_product_target",
                            "LeadId": "",
                            "LeadName": "",
                            "ContactId": "",
                            "ContactName": "",
                            "Company": (account.get("Name") or "")[:255],
                            "OwnerId": owner_id,
                            "OwnerName": context["OwnerName"],
                            "ManagerName": context["ManagerName"],
                            "BDRTeam": context["BDRTeam"],
                            "BDRRole": context["BDRRole"],
                            "QueueGroup": context["QueueGroup"],
                            "Department": context["Department"],
                            "LeadTitle": "",
                            "ContactOfficialTitle": "",
                            "Persona": persona[:255],
                            "Industry": (account.get("Industry") or "")[:255],
                            "IndustryGroup": _industry_group(account.get("Industry")),
                            "Country": "",
                            "LeadSource": "",
                            "SourceGroup": "Persona Product Target",
                            "Campaign": "",
                            "CampaignType": "",
                            "CampaignProduct": "",
                            "CampaignScopeType": "",
                            "CampaignPurpose": "",
                            "YearLabel": "FY2026",
                            "MatchedAccountName": (account.get("Name") or "")[:255],
                            "MatchedAccountIndustry": (account.get("Industry") or "")[
                                :255
                            ],
                            "MatchedAccountRegion": (account.get("Region__c") or "")[
                                :255
                            ],
                            "MatchedAccountSegment": (
                                account.get("TAM_Universe_Segment__c") or ""
                            )[:255],
                            "MatchedAccountTier": (
                                account.get("Tier_Calculation__c") or ""
                            )[:255],
                            "ContextAccountId": account_id,
                            "ContextAccountName": (account.get("Name") or "")[:255],
                            "ContextAccountSource": "Account Persona Product Target",
                            "ContextAccountType": (account.get("Type") or "")[:255],
                            "ContextAccountIndustry": (account.get("Industry") or "")[
                                :255
                            ],
                            "ContextAccountRegion": (account.get("Region__c") or "")[
                                :255
                            ],
                            "ContextAccountSegment": (
                                account.get("TAM_Universe_Segment__c") or ""
                            )[:255],
                            "ContextAccountTier": (
                                account.get("Tier_Calculation__c") or ""
                            )[:255],
                            "ContextCustomerSegment": (
                                account.get("Customer_Segment__c") or ""
                            )[:255],
                            "ContextProductOpportunity": (
                                account.get("Product_Opportunity__c") or ""
                            )[:255],
                            "ContextProductMainline": (
                                account.get("Product_Mainline__c") or ""
                            )[:255],
                            "TargetedProduct": product[:255],
                            "TargetedProductSource": primary_source[:255],
                            "ProductSignalConfidence": product_signal_confidence[:255],
                            "OpportunityProductRaw": "",
                            "OpportunityProductRawSource": "",
                            "OpportunityProduct": "",
                            "OpportunityProductSource": "",
                            "ProductSourceSet": source_set[:255],
                            "ClientBaseClass": client_base_class[:255],
                            "FormerClientLostDate": str(
                                (account.get("Heat_Map_Red_Lostdate__c") or "")
                            )[:10],
                            "FormerClientAgeBand": former_client_age_band[:255],
                            "TelemarketingStatus": (
                                account.get("TM_Account_Status__c") or ""
                            )[:255],
                            "ExCustomerProspectingDate": str(
                                (account.get("Ex_Customer_Prospecting_Date__c") or "")
                            )[:10],
                            "TargetAccountFlag": "true",
                            "Status": "",
                            "QualificationStage": "Persona Product Target",
                            "LifecycleStage": "Persona Product Target",
                            "PriorityBand": "Medium",
                            "NextBestAction": "Expand persona coverage on targeted account",
                            "SuggestedTool": "Salesforce Engage",
                            "ConvertedFlag": "false",
                            "SLAMetFlag": "false",
                            "SLABreachFlag": "false",
                            "HasResponseFlag": "false",
                            "HasTouchFlag": "true"
                            if persona_touch_counts[persona] > 0
                            else "false",
                            "HasMeetingFlag": "true"
                            if persona_meeting_counts[persona] > 0
                            else "false",
                            "HasMeetingHeldFlag": "true"
                            if persona_meeting_counts[persona] > 0
                            else "false",
                            "DirectLeadTouch24hFlag": "false",
                            "DirectLeadTouchAnyFlag": "false",
                            "AssociatedTouch24hFlag": "false",
                            "AssociatedTouchAnyFlag": "false",
                            "FirstTouchPath": "",
                            "SourcedOpportunityId": "",
                            "SourcedOpportunityName": "",
                            "SourcedOpportunityStage": "",
                            "SourcedForecastCategory": "",
                            "SourcedOpportunityType": "",
                            "SourcedProductFamily": product[:255],
                            "HandoffQualityBand": "",
                            "Stage2Date": "",
                            "Stage3Date": "",
                            "CreatedDate": "",
                            "ConvertedDate": "",
                            "MonthStartDate": "",
                            "FirstTouchDate": "",
                            "FirstMeetingDate": "",
                            "LastTouchDate": "",
                            "NextMeetingDate": "",
                            "UpcomingMeetingCount": 0,
                            "WeekStartDate": "",
                            "PriorityScore": float(signal_strength),
                            "LeadScore": 0.0,
                            "LeadAgeDays": 0,
                            "DaysToFirstTouch": 0,
                            "DaysToFirstMeeting": 0,
                            "DaysSinceLastTouch": 0,
                            "Stage2To3Days": 0,
                            "TouchCount": persona_touch_counts[persona],
                            "CallCount": 0,
                            "EmailCount": 0,
                            "LeadTouchCount": 0,
                            "ContactTouchCount": persona_touch_counts[persona],
                            "AccountTouchCount": account_touch_count,
                            "OpportunityTouchCount": 0,
                            "AssociatedTouchCount": persona_touch_counts[persona],
                            "MeetingBookedCount": 0,
                            "MeetingHeldCount": persona_meeting_counts[persona],
                            "LeadCreatedCount": 0,
                            "ResponseCount": len(metrics["response_lead_ids"]),
                            "CampaignCount": campaign_signal_count,
                            "SourcedARR": round(safe_float(metrics["known_arr"]), 2),
                            "SourcedOpportunityCount": len(metrics["opportunity_ids"]),
                            "LeadCount": 0,
                            "SLAEligibleCount": 0,
                            "SLAMetCount": 0,
                            "SLABreachCount": 0,
                            "ConnectedLeadCount": len(metrics["lead_ids"]),
                            "MeetingBookedLeadCount": 0,
                            "MeetingHeldLeadCount": len(metrics["meeting_lead_ids"]),
                            "MeetingTimedLeadCount": 0,
                            "QualifiedCount": len(metrics["qualified_lead_ids"]),
                            "OpenLeadCount": 0,
                            "MQLLeadCount": 0,
                            "SQLLeadCount": 0,
                            "DisqualifiedLeadCount": 0,
                            "MarketingDisqualifiedLeadCount": 0,
                            "SalesDisqualifiedLeadCount": 0,
                            "OpportunityHandoffCount": len(
                                metrics["stage3_opportunity_ids"]
                            ),
                            "OpenMQLLeadCount": 0,
                            "OpenSQLLeadCount": 0,
                            "DirectLeadTouch24hCount": 0,
                            "DirectLeadTouchAnyCount": 0,
                            "AssociatedTouch24hCount": 0,
                            "AssociatedTouchAnyCount": 0,
                            "CompanyTouchCount": 1,
                            "ProspectLeadCount": 1
                            if client_base_class == "Prospect"
                            else 0,
                            "CurrentClientLeadCount": 1
                            if client_base_class == "Current Client"
                            else 0,
                            "FormerClientLeadCount": 1
                            if client_base_class == "Former Client"
                            else 0,
                            "PartnerLeadCount": 1
                            if client_base_class in {"Partner", "Affiliate"}
                            else 0,
                            "StrongHandoffCount": len(
                                metrics["stage3_opportunity_ids"]
                            ),
                            "DiscoveryHandoffCount": len(
                                metrics["stage2_opportunity_ids"]
                            ),
                            "PendingStage3ReviewCount": 0,
                            "Stage3ApprovedCount": 0,
                            "QueueTaskCount": 0,
                            "OverdueTaskCount": 0,
                            "DueTodayTaskCount": 0,
                            "TotalActivityCount": persona_touch_counts[persona],
                            "LeadLinkedActivityCount": 0,
                            "ContactLinkedActivityCount": persona_touch_counts[persona],
                            "AccountLinkedActivityCount": account_touch_count,
                            "OpportunityLinkedActivityCount": 0,
                            "ProspectActivityCount": persona_touch_counts[persona]
                            if client_base_class == "Prospect"
                            else 0,
                            "CurrentClientActivityCount": persona_touch_counts[persona]
                            if client_base_class == "Current Client"
                            else 0,
                            "FormerClientActivityCount": persona_touch_counts[persona]
                            if client_base_class == "Former Client"
                            else 0,
                            "PartnerActivityCount": persona_touch_counts[persona]
                            if client_base_class in {"Partner", "Affiliate"}
                            else 0,
                            "UnclassifiedActivityCount": persona_touch_counts[persona]
                            if client_base_class
                            not in {
                                "Prospect",
                                "Current Client",
                                "Former Client",
                                "Partner",
                                "Affiliate",
                            }
                            else 0,
                            "ContactCount": persona_contact_count,
                            "PersonaContactCount": persona_contact_count,
                            "UniquePersonaCount": 1,
                            "CLevelPersonaCount": 0,
                            "HLevelPersonaCount": 0,
                            "NonPersonaContactCount": 0,
                            "OpenOpportunityCount": len(
                                metrics["open_opportunity_ids"]
                            ),
                            "SignalSourceCount": len(sources),
                            "ProductSignalStrength": signal_strength,
                            "ProductSignalConfidenceScore": product_signal_confidence_score,
                            "CampaignProductSignalCount": campaign_signal_count,
                            "OpportunityProductSignalCount": opportunity_signal_count,
                            "AccountOpportunityHistorySignalCount": account_opportunity_history_signal_count,
                            "AccountProductSignalCount": account_opportunity_signal_count,
                            "AccountMainlineSignalCount": account_mainline_signal_count,
                            "StageOrder": 0,
                        }
                    )

    rep_week_rows: list[dict[str, object]] = []
    for (
        owner_name,
        team,
        role,
        manager_name,
        source_group,
        week_start,
    ), metrics in rep_week.items():
        rep_week_rows.append(
            {
                "RecordType": "rep_week",
                "LeadId": "",
                "LeadName": "",
                "Company": "",
                "OwnerId": "",
                "OwnerName": owner_name,
                "ManagerName": manager_name,
                "BDRTeam": team,
                "BDRRole": role,
                "QueueGroup": "",
                "Department": "",
                "Persona": "",
                "Industry": "",
                "Country": "",
                "LeadSource": "",
                "SourceGroup": source_group,
                "Campaign": "",
                "CampaignType": "",
                "CampaignProduct": "",
                "CampaignScopeType": "",
                "CampaignPurpose": "",
                "YearLabel": _fy_label(week_start),
                "MatchedAccountName": "",
                "MatchedAccountIndustry": "",
                "MatchedAccountRegion": "",
                "MatchedAccountSegment": "",
                "MatchedAccountTier": "",
                "TargetAccountFlag": "false",
                "Status": "",
                "QualificationStage": "",
                "LifecycleStage": "",
                "PriorityBand": "",
                "NextBestAction": "",
                "SuggestedTool": "",
                "ConvertedFlag": "false",
                "SLAMetFlag": "false",
                "SLABreachFlag": "false",
                "HasResponseFlag": "false",
                "HasTouchFlag": "false",
                "HasMeetingFlag": "false",
                "HasMeetingHeldFlag": "false",
                "SourcedOpportunityId": "",
                "SourcedOpportunityName": "",
                "SourcedOpportunityStage": "",
                "SourcedForecastCategory": "",
                "CreatedDate": "",
                "ConvertedDate": "",
                "MonthStartDate": _month_start(week_start),
                "FirstTouchDate": "",
                "LastTouchDate": "",
                "NextMeetingDate": "",
                "UpcomingMeetingCount": 0,
                "WeekStartDate": week_start,
                "PriorityScore": 0.0,
                "LeadScore": 0.0,
                "LeadAgeDays": 0,
                "DaysToFirstTouch": 0,
                "DaysSinceLastTouch": 0,
                "TouchCount": int(safe_float(metrics["TouchCount"])),
                "CallCount": int(safe_float(metrics["CallCount"])),
                "EmailCount": int(safe_float(metrics["EmailCount"])),
                "MeetingBookedCount": int(safe_float(metrics["MeetingBookedCount"])),
                "MeetingHeldCount": int(safe_float(metrics["MeetingHeldCount"])),
                "ResponseCount": int(safe_float(metrics["ResponseCount"])),
                "CampaignCount": 0,
                "SourcedARR": round(safe_float(metrics["SourcedARR"]), 2),
                "SourcedOpportunityCount": 0,
                "LeadCreatedCount": int(safe_float(metrics["LeadCreatedCount"])),
                "LeadCount": int(safe_float(metrics["LeadCreatedCount"])),
                "SLAEligibleCount": 0,
                "SLAMetCount": int(safe_float(metrics["SLAMetCount"])),
                "SLABreachCount": int(safe_float(metrics["SLABreachCount"])),
                "ConnectedLeadCount": 0,
                "MeetingBookedLeadCount": 0,
                "MeetingHeldLeadCount": 0,
                "QualifiedCount": int(safe_float(metrics["QualifiedCount"])),
                "OpenLeadCount": int(safe_float(metrics["LeadCreatedCount"])),
                "MQLLeadCount": 0,
                "SQLLeadCount": 0,
                "DisqualifiedLeadCount": 0,
                "MarketingDisqualifiedLeadCount": 0,
                "SalesDisqualifiedLeadCount": 0,
                "OpportunityHandoffCount": 0,
                "OpenMQLLeadCount": 0,
                "OpenSQLLeadCount": 0,
                "DirectLeadTouch24hCount": 0,
                "DirectLeadTouchAnyCount": 0,
                "CompanyTouchCount": len(metrics["Companies"])
                if isinstance(metrics["Companies"], set)
                else 0,
                "QueueTaskCount": 0,
                "OverdueTaskCount": 0,
                "DueTodayTaskCount": 0,
                "StageOrder": 0,
            }
        )

    owner_week_rows: list[dict[str, object]] = []
    for (
        owner_name,
        team,
        role,
        manager_name,
        week_start,
    ), metrics in owner_week.items():
        owner_week_rows.append(
            {
                "RecordType": "owner_week",
                "OwnerName": owner_name,
                "ManagerName": manager_name,
                "BDRTeam": team,
                "BDRRole": role,
                "WeekStartDate": week_start,
                "TouchCount": int(safe_float(metrics["TouchCount"])),
                "CallCount": int(safe_float(metrics["CallCount"])),
                "EmailCount": int(safe_float(metrics["EmailCount"])),
                "MeetingBookedCount": int(safe_float(metrics["MeetingBookedCount"])),
                "MeetingHeldCount": int(safe_float(metrics["MeetingHeldCount"])),
                "QualifiedCount": int(safe_float(metrics["QualifiedCount"])),
                "CompanyTouchCount": len(metrics["Companies"])
                if isinstance(metrics["Companies"], set)
                else 0,
                "TotalActivityCount": int(safe_float(metrics["TotalActivityCount"])),
                "LeadLinkedActivityCount": int(
                    safe_float(metrics["LeadLinkedActivityCount"])
                ),
                "ContactLinkedActivityCount": int(
                    safe_float(metrics["ContactLinkedActivityCount"])
                ),
                "AccountLinkedActivityCount": int(
                    safe_float(metrics["AccountLinkedActivityCount"])
                ),
                "OpportunityLinkedActivityCount": int(
                    safe_float(metrics["OpportunityLinkedActivityCount"])
                ),
                "ProspectActivityCount": int(
                    safe_float(metrics["ProspectActivityCount"])
                ),
                "CurrentClientActivityCount": int(
                    safe_float(metrics["CurrentClientActivityCount"])
                ),
                "FormerClientActivityCount": int(
                    safe_float(metrics["FormerClientActivityCount"])
                ),
                "PartnerActivityCount": int(
                    safe_float(metrics["PartnerActivityCount"])
                ),
                "UnclassifiedActivityCount": int(
                    safe_float(metrics["UnclassifiedActivityCount"])
                ),
            }
        )

    source_week_rows: list[dict[str, object]] = []
    for (team, source_group, week_start), metrics in source_week.items():
        source_week_rows.append(
            {
                "RecordType": "source_week",
                "LeadId": "",
                "LeadName": "",
                "Company": "",
                "OwnerId": "",
                "OwnerName": "",
                "ManagerName": "",
                "BDRTeam": team,
                "BDRRole": "",
                "QueueGroup": "",
                "Department": "",
                "Persona": "",
                "Industry": "",
                "Country": "",
                "LeadSource": "",
                "SourceGroup": source_group,
                "Campaign": "",
                "CampaignType": "",
                "CampaignProduct": "",
                "CampaignScopeType": "",
                "CampaignPurpose": "",
                "YearLabel": _fy_label(week_start),
                "MatchedAccountName": "",
                "MatchedAccountIndustry": "",
                "MatchedAccountRegion": "",
                "MatchedAccountSegment": "",
                "MatchedAccountTier": "",
                "TargetAccountFlag": "false",
                "Status": "",
                "LifecycleStage": "",
                "PriorityBand": "",
                "NextBestAction": "",
                "SuggestedTool": "",
                "ConvertedFlag": "false",
                "SLAMetFlag": "false",
                "SLABreachFlag": "false",
                "HasResponseFlag": "false",
                "HasTouchFlag": "false",
                "HasMeetingFlag": "false",
                "HasMeetingHeldFlag": "false",
                "SourcedOpportunityId": "",
                "SourcedOpportunityName": "",
                "SourcedOpportunityStage": "",
                "SourcedForecastCategory": "",
                "CreatedDate": "",
                "ConvertedDate": "",
                "MonthStartDate": _month_start(week_start),
                "FirstTouchDate": "",
                "LastTouchDate": "",
                "NextMeetingDate": "",
                "UpcomingMeetingCount": 0,
                "WeekStartDate": week_start,
                "PriorityScore": 0.0,
                "LeadScore": 0.0,
                "LeadAgeDays": 0,
                "DaysToFirstTouch": 0,
                "DaysSinceLastTouch": 0,
                "TouchCount": 0,
                "CallCount": 0,
                "EmailCount": 0,
                "MeetingBookedCount": 0,
                "MeetingHeldCount": int(safe_float(metrics["MeetingHeldCount"])),
                "ResponseCount": int(safe_float(metrics["ResponseCount"])),
                "CampaignCount": 0,
                "SourcedARR": round(safe_float(metrics["SourcedARR"]), 2),
                "SourcedOpportunityCount": 0,
                "LeadCount": int(safe_float(metrics["LeadCreatedCount"])),
                "SLAEligibleCount": 0,
                "SLAMetCount": 0,
                "SLABreachCount": 0,
                "ConnectedLeadCount": 0,
                "MeetingBookedLeadCount": 0,
                "MeetingHeldLeadCount": 0,
                "QualifiedCount": int(safe_float(metrics["QualifiedCount"])),
                "OpenLeadCount": 0,
                "MQLLeadCount": 0,
                "SQLLeadCount": 0,
                "DisqualifiedLeadCount": 0,
                "MarketingDisqualifiedLeadCount": 0,
                "SalesDisqualifiedLeadCount": 0,
                "OpportunityHandoffCount": 0,
                "OpenMQLLeadCount": 0,
                "OpenSQLLeadCount": 0,
                "DirectLeadTouch24hCount": 0,
                "DirectLeadTouchAnyCount": 0,
                "CompanyTouchCount": 0,
                "QueueTaskCount": 0,
                "OverdueTaskCount": 0,
                "DueTodayTaskCount": 0,
                "TotalActivityCount": 0,
                "LeadLinkedActivityCount": 0,
                "ContactLinkedActivityCount": 0,
                "AccountLinkedActivityCount": 0,
                "OpportunityLinkedActivityCount": 0,
                "StageOrder": 0,
            }
        )

    campaign_rows: list[dict[str, object]] = []
    for (
        team,
        owner_name,
        manager_name,
        year_label,
        campaign_name,
        source_group,
        campaign_type,
        campaign_product,
        campaign_scope_type,
    ), metrics in campaign_summary.items():
        campaign_rows.append(
            {
                "RecordType": "campaign_summary",
                "LeadId": "",
                "LeadName": "",
                "Company": "",
                "OwnerId": "",
                "OwnerName": owner_name,
                "ManagerName": manager_name,
                "BDRTeam": team,
                "BDRRole": "",
                "QueueGroup": "",
                "Department": "",
                "Persona": "",
                "Industry": "",
                "Country": "",
                "LeadSource": "",
                "SourceGroup": source_group,
                "Campaign": campaign_name,
                "CampaignType": campaign_type,
                "CampaignProduct": campaign_product,
                "CampaignScopeType": campaign_scope_type,
                "CampaignPurpose": "",
                "YearLabel": year_label,
                "MatchedAccountName": "",
                "MatchedAccountIndustry": "",
                "MatchedAccountRegion": "",
                "MatchedAccountSegment": "",
                "MatchedAccountTier": "",
                "TargetAccountFlag": "false",
                "Status": "",
                "LifecycleStage": "",
                "PriorityBand": "",
                "NextBestAction": "",
                "SuggestedTool": "",
                "ConvertedFlag": "false",
                "SLAMetFlag": "false",
                "SLABreachFlag": "false",
                "HasResponseFlag": "false",
                "HasTouchFlag": "false",
                "HasMeetingFlag": "false",
                "HasMeetingHeldFlag": "false",
                "SourcedOpportunityId": "",
                "SourcedOpportunityName": "",
                "SourcedOpportunityStage": "",
                "SourcedForecastCategory": "",
                "CreatedDate": "",
                "ConvertedDate": "",
                "MonthStartDate": "",
                "FirstTouchDate": "",
                "LastTouchDate": "",
                "NextMeetingDate": "",
                "UpcomingMeetingCount": 0,
                "WeekStartDate": "",
                "PriorityScore": 0.0,
                "LeadScore": 0.0,
                "LeadAgeDays": 0,
                "DaysToFirstTouch": 0,
                "DaysSinceLastTouch": 0,
                "TouchCount": 0,
                "CallCount": 0,
                "EmailCount": 0,
                "MeetingBookedCount": int(safe_float(metrics["MeetingBookedCount"])),
                "MeetingHeldCount": int(safe_float(metrics["MeetingHeldCount"])),
                "ResponseCount": int(safe_float(metrics["ResponseCount"])),
                "CampaignCount": int(safe_float(metrics["LeadCount"])),
                "SourcedARR": round(safe_float(metrics["SourcedARR"]), 2),
                "SourcedOpportunityCount": 0,
                "LeadCount": int(safe_float(metrics["LeadCount"])),
                "SLAEligibleCount": 0,
                "SLAMetCount": 0,
                "SLABreachCount": 0,
                "ConnectedLeadCount": 0,
                "MeetingBookedLeadCount": 0,
                "MeetingHeldLeadCount": 0,
                "QualifiedCount": int(safe_float(metrics["QualifiedCount"])),
                "OpenLeadCount": 0,
                "MQLLeadCount": int(safe_float(metrics["MQLLeadCount"])),
                "SQLLeadCount": int(safe_float(metrics["SQLLeadCount"])),
                "DisqualifiedLeadCount": int(
                    safe_float(metrics["DisqualifiedLeadCount"])
                ),
                "MarketingDisqualifiedLeadCount": int(
                    safe_float(metrics["MarketingDisqualifiedLeadCount"])
                ),
                "SalesDisqualifiedLeadCount": int(
                    safe_float(metrics["SalesDisqualifiedLeadCount"])
                ),
                "OpportunityHandoffCount": int(
                    safe_float(metrics["OpportunityHandoffCount"])
                ),
                "OpenMQLLeadCount": 0,
                "OpenSQLLeadCount": 0,
                "DirectLeadTouch24hCount": 0,
                "DirectLeadTouchAnyCount": 0,
                "CompanyTouchCount": 0,
                "QueueTaskCount": 0,
                "OverdueTaskCount": 0,
                "DueTodayTaskCount": 0,
                "StageOrder": 0,
            }
        )

    queue_rows: list[dict[str, object]] = []
    for queue_name, metrics in queue_backlog.items():
        queue_rows.append(
            {
                "RecordType": "queue_snapshot",
                "LeadId": "",
                "LeadName": "",
                "Company": "",
                "OwnerId": "",
                "OwnerName": queue_name,
                "ManagerName": "",
                "BDRTeam": _queue_team(queue_name),
                "BDRRole": "Queue",
                "QueueGroup": queue_name,
                "Department": queue_name,
                "Persona": "",
                "Industry": "",
                "Country": "",
                "LeadSource": "",
                "SourceGroup": "",
                "Campaign": "",
                "CampaignType": "",
                "CampaignProduct": "",
                "CampaignScopeType": "",
                "CampaignPurpose": "",
                "YearLabel": "",
                "MatchedAccountName": "",
                "MatchedAccountIndustry": "",
                "MatchedAccountRegion": "",
                "MatchedAccountSegment": "",
                "MatchedAccountTier": "",
                "TargetAccountFlag": "false",
                "Status": "",
                "LifecycleStage": "",
                "PriorityBand": "",
                "NextBestAction": "",
                "SuggestedTool": "",
                "ConvertedFlag": "false",
                "SLAMetFlag": "false",
                "SLABreachFlag": "false",
                "HasResponseFlag": "false",
                "HasTouchFlag": "false",
                "HasMeetingFlag": "false",
                "HasMeetingHeldFlag": "false",
                "SourcedOpportunityId": "",
                "SourcedOpportunityName": "",
                "SourcedOpportunityStage": "",
                "SourcedForecastCategory": "",
                "CreatedDate": "",
                "ConvertedDate": "",
                "MonthStartDate": "",
                "FirstTouchDate": "",
                "LastTouchDate": "",
                "NextMeetingDate": "",
                "UpcomingMeetingCount": 0,
                "WeekStartDate": "",
                "PriorityScore": 0.0,
                "LeadScore": 0.0,
                "LeadAgeDays": 0,
                "DaysToFirstTouch": 0,
                "DaysSinceLastTouch": 0,
                "TouchCount": 0,
                "CallCount": 0,
                "EmailCount": 0,
                "MeetingBookedCount": 0,
                "MeetingHeldCount": 0,
                "ResponseCount": 0,
                "CampaignCount": 0,
                "SourcedARR": 0.0,
                "SourcedOpportunityCount": 0,
                "LeadCount": 0,
                "SLAEligibleCount": 0,
                "SLAMetCount": 0,
                "SLABreachCount": 0,
                "ConnectedLeadCount": 0,
                "MeetingBookedLeadCount": 0,
                "MeetingHeldLeadCount": 0,
                "QualifiedCount": 0,
                "OpenLeadCount": 0,
                "DirectLeadTouch24hCount": 0,
                "DirectLeadTouchAnyCount": 0,
                "CompanyTouchCount": 0,
                "QueueTaskCount": int(safe_float(metrics["QueueTaskCount"])),
                "OverdueTaskCount": int(safe_float(metrics["OverdueTaskCount"])),
                "DueTodayTaskCount": int(safe_float(metrics["DueTodayTaskCount"])),
                "StageOrder": 0,
            }
        )

    integrity_rows: list[dict[str, object]] = []
    for (owner_name, manager_name, team, role), metrics in owner_integrity.items():
        integrity_rows.append(
            {
                "RecordType": "owner_integrity",
                "LeadId": "",
                "LeadName": "",
                "Company": "",
                "OwnerId": "",
                "OwnerName": owner_name,
                "ManagerName": manager_name,
                "BDRTeam": team,
                "BDRRole": role,
                "QueueGroup": "",
                "Department": "",
                "Persona": "",
                "Industry": "",
                "Country": "",
                "LeadSource": "",
                "SourceGroup": "",
                "Campaign": "",
                "CampaignType": "",
                "CampaignProduct": "",
                "CampaignScopeType": "",
                "CampaignPurpose": "",
                "YearLabel": "",
                "MatchedAccountName": "",
                "MatchedAccountIndustry": "",
                "MatchedAccountRegion": "",
                "MatchedAccountSegment": "",
                "MatchedAccountTier": "",
                "TargetAccountFlag": "false",
                "Status": "",
                "LifecycleStage": "",
                "PriorityBand": "",
                "NextBestAction": "",
                "SuggestedTool": "",
                "ConvertedFlag": "false",
                "SLAMetFlag": "false",
                "SLABreachFlag": "false",
                "HasResponseFlag": "false",
                "HasTouchFlag": "false",
                "HasMeetingFlag": "false",
                "HasMeetingHeldFlag": "false",
                "DirectLeadTouch24hFlag": "false",
                "DirectLeadTouchAnyFlag": "false",
                "SourcedOpportunityId": "",
                "SourcedOpportunityName": "",
                "SourcedOpportunityStage": "",
                "SourcedForecastCategory": "",
                "CreatedDate": "",
                "ConvertedDate": "",
                "MonthStartDate": "",
                "FirstTouchDate": "",
                "LastTouchDate": "",
                "NextMeetingDate": "",
                "UpcomingMeetingCount": 0,
                "WeekStartDate": "",
                "PriorityScore": 0.0,
                "LeadScore": 0.0,
                "LeadAgeDays": 0,
                "DaysToFirstTouch": 0,
                "DaysSinceLastTouch": 0,
                "TouchCount": 0,
                "CallCount": 0,
                "EmailCount": 0,
                "MeetingBookedCount": 0,
                "MeetingHeldCount": 0,
                "ResponseCount": 0,
                "CampaignCount": 0,
                "SourcedARR": 0.0,
                "SourcedOpportunityCount": 0,
                "LeadCount": int(safe_float(metrics["LeadCount"])),
                "SLAEligibleCount": 0,
                "SLAMetCount": 0,
                "SLABreachCount": 0,
                "ConnectedLeadCount": 0,
                "MeetingBookedLeadCount": 0,
                "MeetingHeldLeadCount": 0,
                "QualifiedCount": 0,
                "OpenLeadCount": 0,
                "MQLLeadCount": 0,
                "SQLLeadCount": 0,
                "DisqualifiedLeadCount": 0,
                "MarketingDisqualifiedLeadCount": 0,
                "SalesDisqualifiedLeadCount": 0,
                "OpportunityHandoffCount": 0,
                "OpenMQLLeadCount": 0,
                "OpenSQLLeadCount": 0,
                "DirectLeadTouch24hCount": int(
                    safe_float(metrics["LeadTouch24hCount"])
                ),
                "DirectLeadTouchAnyCount": int(
                    safe_float(metrics["LeadTouchAnyCount"])
                ),
                "AssociatedTouch24hCount": int(
                    safe_float(metrics["AssociatedTouch24hCount"])
                ),
                "AssociatedTouchAnyCount": int(
                    safe_float(metrics["AssociatedTouchAnyCount"])
                ),
                "CompanyTouchCount": 0,
                "QueueTaskCount": int(safe_float(metrics["TotalActivityCount"])),
                "OverdueTaskCount": int(safe_float(metrics["LeadLinkedActivityCount"])),
                "DueTodayTaskCount": int(
                    safe_float(metrics["ContactLinkedActivityCount"])
                ),
                "TotalActivityCount": int(safe_float(metrics["TotalActivityCount"])),
                "LeadLinkedActivityCount": int(
                    safe_float(metrics["LeadLinkedActivityCount"])
                ),
                "ContactLinkedActivityCount": int(
                    safe_float(metrics["ContactLinkedActivityCount"])
                ),
                "AccountLinkedActivityCount": int(
                    safe_float(metrics["AccountLinkedActivityCount"])
                ),
                "OpportunityLinkedActivityCount": int(
                    safe_float(metrics["OpportunityLinkedActivityCount"])
                ),
                "ProspectActivityCount": int(
                    safe_float(metrics["ProspectActivityCount"])
                ),
                "CurrentClientActivityCount": int(
                    safe_float(metrics["CurrentClientActivityCount"])
                ),
                "FormerClientActivityCount": int(
                    safe_float(metrics["FormerClientActivityCount"])
                ),
                "PartnerActivityCount": int(
                    safe_float(metrics["PartnerActivityCount"])
                ),
                "UnclassifiedActivityCount": int(
                    safe_float(metrics["UnclassifiedActivityCount"])
                ),
                "StageOrder": 0,
            }
        )

    rows = (
        detail_rows
        + detail_rows_account
        + detail_rows_contact
        + detail_rows_account_product
        + detail_rows_account_persona_product
        + rep_week_rows
        + owner_week_rows
        + source_week_rows
        + campaign_rows
        + queue_rows
        + integrity_rows
    )
    print(f"  Lead/opp detail rows: {len(detail_rows)}")
    print(f"  Account universe rows: {len(detail_rows_account)}")
    print(f"  Contact coverage rows: {len(detail_rows_contact)}")
    print(f"  Account product target rows: {len(detail_rows_account_product)}")
    print(
        f"  Account persona-product target rows: {len(detail_rows_account_persona_product)}"
    )
    print(f"  Rep-week rows: {len(rep_week_rows)}")
    print(f"  Owner-week rows: {len(owner_week_rows)}")
    print(f"  Source-week rows: {len(source_week_rows)}")
    print(f"  Campaign summary rows: {len(campaign_rows)}")
    print(f"  Queue rows: {len(queue_rows)}")
    print(f"  Integrity rows: {len(integrity_rows)}")
    print(f"  Total rows: {len(rows)}")

    field_names = [
        "RecordType",
        "LeadId",
        "LeadName",
        "ContactId",
        "ContactName",
        "Company",
        "OwnerId",
        "OwnerName",
        "ManagerName",
        "BDRTeam",
        "BDRRole",
        "QueueGroup",
        "Department",
        "LeadTitle",
        "ContactOfficialTitle",
        "Persona",
        "Industry",
        "IndustryGroup",
        "Country",
        "LeadSource",
        "SourceGroup",
        "Campaign",
        "CampaignType",
        "CampaignProduct",
        "CampaignScopeType",
        "CampaignPurpose",
        "YearLabel",
        "MatchedAccountName",
        "MatchedAccountIndustry",
        "MatchedAccountRegion",
        "MatchedAccountSegment",
        "MatchedAccountTier",
        "ContextAccountId",
        "ContextAccountName",
        "ContextAccountSource",
        "ContextAccountType",
        "ContextAccountIndustry",
        "ContextAccountRegion",
        "ContextAccountSegment",
        "ContextAccountTier",
        "ContextCustomerSegment",
        "ContextProductOpportunity",
        "ContextProductMainline",
        "TargetedProduct",
        "TargetedProductSource",
        "ProductSignalConfidence",
        "OpportunityProductRaw",
        "OpportunityProductRawSource",
        "OpportunityProduct",
        "OpportunityProductSource",
        "ProductSourceSet",
        "ClientBaseClass",
        "FormerClientLostDate",
        "FormerClientAgeBand",
        "TelemarketingStatus",
        "ExCustomerProspectingDate",
        "TargetAccountFlag",
        "Status",
        "QualificationStage",
        "LifecycleStage",
        "PriorityBand",
        "NextBestAction",
        "SuggestedTool",
        "ConvertedFlag",
        "SLAMetFlag",
        "SLABreachFlag",
        "HasResponseFlag",
        "HasTouchFlag",
        "HasMeetingFlag",
        "HasMeetingHeldFlag",
        "DirectLeadTouch24hFlag",
        "DirectLeadTouchAnyFlag",
        "AssociatedTouch24hFlag",
        "AssociatedTouchAnyFlag",
        "FirstTouchPath",
        "SourcedOpportunityId",
        "SourcedOpportunityName",
        "SourcedOpportunityStage",
        "SourcedForecastCategory",
        "SourcedOpportunityType",
        "SourcedProductFamily",
        "HandoffQualityBand",
        "Stage2Date",
        "Stage3Date",
        "CreatedDate",
        "ConvertedDate",
        "MonthStartDate",
        "FirstTouchDate",
        "FirstMeetingDate",
        "LastTouchDate",
        "NextMeetingDate",
        "UpcomingMeetingCount",
        "WeekStartDate",
        "PriorityScore",
        "LeadScore",
        "LeadAgeDays",
        "DaysToFirstTouch",
        "DaysToFirstMeeting",
        "DaysSinceLastTouch",
        "Stage2To3Days",
        "TouchCount",
        "CallCount",
        "EmailCount",
        "LeadTouchCount",
        "ContactTouchCount",
        "AccountTouchCount",
        "OpportunityTouchCount",
        "AssociatedTouchCount",
        "MeetingBookedCount",
        "MeetingHeldCount",
        "LeadCreatedCount",
        "ResponseCount",
        "CampaignCount",
        "SourcedARR",
        "SourcedOpportunityCount",
        "LeadCount",
        "SLAEligibleCount",
        "SLAMetCount",
        "SLABreachCount",
        "ConnectedLeadCount",
        "MeetingBookedLeadCount",
        "MeetingHeldLeadCount",
        "MeetingTimedLeadCount",
        "QualifiedCount",
        "OpenLeadCount",
        "MQLLeadCount",
        "SQLLeadCount",
        "DisqualifiedLeadCount",
        "MarketingDisqualifiedLeadCount",
        "SalesDisqualifiedLeadCount",
        "OpportunityHandoffCount",
        "OpenMQLLeadCount",
        "OpenSQLLeadCount",
        "DirectLeadTouch24hCount",
        "DirectLeadTouchAnyCount",
        "AssociatedTouch24hCount",
        "AssociatedTouchAnyCount",
        "CompanyTouchCount",
        "ProspectLeadCount",
        "CurrentClientLeadCount",
        "FormerClientLeadCount",
        "PartnerLeadCount",
        "StrongHandoffCount",
        "DiscoveryHandoffCount",
        "PendingStage3ReviewCount",
        "Stage3ApprovedCount",
        "QueueTaskCount",
        "OverdueTaskCount",
        "DueTodayTaskCount",
        "TotalActivityCount",
        "LeadLinkedActivityCount",
        "ContactLinkedActivityCount",
        "AccountLinkedActivityCount",
        "OpportunityLinkedActivityCount",
        "ProspectActivityCount",
        "CurrentClientActivityCount",
        "FormerClientActivityCount",
        "PartnerActivityCount",
        "UnclassifiedActivityCount",
        "ContactCount",
        "PersonaContactCount",
        "UniquePersonaCount",
        "CLevelPersonaCount",
        "HLevelPersonaCount",
        "NonPersonaContactCount",
        "OpenOpportunityCount",
        "ActiveContactCount",
        "ActivePersonaContactCount",
        "SignalSourceCount",
        "ProductSignalStrength",
        "ProductSignalConfidenceScore",
        "CampaignProductSignalCount",
        "OpportunityProductSignalCount",
        "AccountOpportunityHistorySignalCount",
        "AccountProductSignalCount",
        "AccountMainlineSignalCount",
        "StageOrder",
    ]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buffer.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes")

    fields_meta = [
        _dim("RecordType", "Record Type"),
        _dim("LeadId", "Lead Id"),
        _dim("LeadName", "Lead"),
        _dim("ContactId", "Contact Id"),
        _dim("ContactName", "Contact"),
        _dim("Company", "Company"),
        _dim("OwnerId", "Owner Id"),
        _dim("OwnerName", "Owner"),
        _dim("ManagerName", "Manager"),
        _dim("BDRTeam", "BDR Team"),
        _dim("BDRRole", "BDR Role"),
        _dim("QueueGroup", "Queue Group"),
        _dim("Department", "Department"),
        _dim("LeadTitle", "Lead Title"),
        _dim("ContactOfficialTitle", "Contact Title"),
        _dim("Persona", "Persona"),
        _dim("Industry", "Industry"),
        _dim("IndustryGroup", "Industry Group"),
        _dim("Country", "Country"),
        _dim("LeadSource", "Lead Source"),
        _dim("SourceGroup", "Source Group"),
        _dim("Campaign", "Campaign"),
        _dim("CampaignType", "Campaign Type"),
        _dim("CampaignProduct", "Campaign Product"),
        _dim("CampaignScopeType", "Campaign Scope Type"),
        _dim("CampaignPurpose", "Campaign Purpose"),
        _dim("YearLabel", "Fiscal Year"),
        _dim("MatchedAccountName", "Matched Account"),
        _dim("MatchedAccountIndustry", "Matched Account Industry"),
        _dim("MatchedAccountRegion", "Matched Account Region"),
        _dim("MatchedAccountSegment", "Matched Account Segment"),
        _dim("MatchedAccountTier", "Matched Account Tier"),
        _dim("ContextAccountId", "Context Account Id"),
        _dim("ContextAccountName", "Context Account"),
        _dim("ContextAccountSource", "Context Account Source"),
        _dim("ContextAccountType", "Context Account Type"),
        _dim("ContextAccountIndustry", "Context Account Industry"),
        _dim("ContextAccountRegion", "Context Account Region"),
        _dim("ContextAccountSegment", "Context Account Segment"),
        _dim("ContextAccountTier", "Context Account Tier"),
        _dim("ContextCustomerSegment", "Context Customer Segment"),
        _dim("ContextProductOpportunity", "Context Product Opportunity"),
        _dim("ContextProductMainline", "Context Product Mainline"),
        _dim("TargetedProduct", "Targeted Product"),
        _dim("TargetedProductSource", "Targeted Product Source"),
        _dim("ProductSignalConfidence", "Product Signal Confidence"),
        _dim("OpportunityProductRaw", "Opportunity Product Raw"),
        _dim("OpportunityProductRawSource", "Opportunity Product Raw Source"),
        _dim("OpportunityProduct", "Opportunity Product"),
        _dim("OpportunityProductSource", "Opportunity Product Source"),
        _dim("ProductSourceSet", "Product Source Set"),
        _dim("ClientBaseClass", "Client / Prospect Class"),
        _date("FormerClientLostDate", "Former Client Lost Date"),
        _dim("FormerClientAgeBand", "Former Client Age Band"),
        _dim("TelemarketingStatus", "Telemarketing Status"),
        _date("ExCustomerProspectingDate", "Ex-Customer Prospecting Date"),
        _dim("TargetAccountFlag", "Target Account"),
        _dim("Status", "Status"),
        _dim("QualificationStage", "Qualification Stage"),
        _dim("LifecycleStage", "Lifecycle Stage"),
        _dim("PriorityBand", "Priority Band"),
        _dim("NextBestAction", "Next Best Action"),
        _dim("SuggestedTool", "Suggested Tool"),
        _dim("ConvertedFlag", "Converted"),
        _dim("SLAMetFlag", "SLA Met"),
        _dim("SLABreachFlag", "SLA Breach"),
        _dim("HasResponseFlag", "Has Response"),
        _dim("HasTouchFlag", "Has Touch"),
        _dim("HasMeetingFlag", "Has Meeting"),
        _dim("HasMeetingHeldFlag", "Has Meeting Held"),
        _dim("DirectLeadTouch24hFlag", "Direct Lead Touch <24h"),
        _dim("DirectLeadTouchAnyFlag", "Direct Lead Touch"),
        _dim("AssociatedTouch24hFlag", "Associated Prospect Touch <24h"),
        _dim("AssociatedTouchAnyFlag", "Associated Prospect Touch"),
        _dim("FirstTouchPath", "First Touch Path"),
        _dim("SourcedOpportunityId", "Sourced Opportunity Id"),
        _dim("SourcedOpportunityName", "Sourced Opportunity"),
        _dim("SourcedOpportunityStage", "Sourced Opportunity Stage"),
        _dim("SourcedForecastCategory", "Sourced Forecast Category"),
        _dim("SourcedOpportunityType", "Sourced Opportunity Type"),
        _dim("SourcedProductFamily", "Sourced Product Family"),
        _dim("HandoffQualityBand", "Handoff Quality"),
        _date("CreatedDate", "Created Date"),
        _date("ConvertedDate", "Converted Date"),
        _date("MonthStartDate", "Month Start"),
        _date("Stage2Date", "Stage 2 Date"),
        _date("Stage3Date", "Stage 3 Date"),
        _date("FirstTouchDate", "First Touch Date"),
        _date("FirstMeetingDate", "First Meeting Date"),
        _date("LastTouchDate", "Last Touch Date"),
        _date("NextMeetingDate", "Next Meeting Date"),
        _measure(
            "UpcomingMeetingCount", "Upcoming Meeting Count", scale=0, precision=6
        ),
        _date("WeekStartDate", "Week Start"),
        _measure("PriorityScore", "Priority Score", scale=1, precision=6),
        _measure("LeadScore", "Lead Score", scale=1, precision=6),
        _measure("LeadAgeDays", "Lead Age Days", scale=0, precision=6),
        _measure("DaysToFirstTouch", "Days To First Touch", scale=0, precision=6),
        _measure("DaysToFirstMeeting", "Days To First Meeting", scale=0, precision=6),
        _measure("DaysSinceLastTouch", "Days Since Last Touch", scale=0, precision=6),
        _measure("Stage2To3Days", "Stage 2 -> 3 Days", scale=0, precision=6),
        _measure("TouchCount", "Touch Count", scale=0, precision=6),
        _measure("CallCount", "Call Count", scale=0, precision=6),
        _measure("EmailCount", "Email Count", scale=0, precision=6),
        _measure("LeadTouchCount", "Lead Touch Count", scale=0, precision=6),
        _measure("ContactTouchCount", "Contact Touch Count", scale=0, precision=6),
        _measure("AccountTouchCount", "Account Touch Count", scale=0, precision=6),
        _measure(
            "OpportunityTouchCount", "Opportunity Touch Count", scale=0, precision=6
        ),
        _measure(
            "AssociatedTouchCount",
            "Associated Prospect Touch Count",
            scale=0,
            precision=6,
        ),
        _measure("MeetingBookedCount", "Meetings Booked", scale=0, precision=6),
        _measure("MeetingHeldCount", "Meetings Held", scale=0, precision=6),
        _measure("LeadCreatedCount", "Lead Created Count", scale=0, precision=6),
        _measure("ResponseCount", "Response Count", scale=0, precision=6),
        _measure("CampaignCount", "Campaign Count", scale=0, precision=6),
        _measure("SourcedARR", "Sourced ARR", scale=2, precision=18),
        _measure(
            "SourcedOpportunityCount", "Sourced Opportunity Count", scale=0, precision=6
        ),
        _measure("LeadCount", "Lead Count", scale=0, precision=6),
        _measure("SLAEligibleCount", "SLA Eligible Count", scale=0, precision=6),
        _measure("SLAMetCount", "SLA Met Count", scale=0, precision=6),
        _measure("SLABreachCount", "SLA Breach Count", scale=0, precision=6),
        _measure("ConnectedLeadCount", "Connected Lead Count", scale=0, precision=6),
        _measure(
            "MeetingBookedLeadCount", "Meeting Booked Lead Count", scale=0, precision=6
        ),
        _measure(
            "MeetingHeldLeadCount", "Meeting Held Lead Count", scale=0, precision=6
        ),
        _measure(
            "MeetingTimedLeadCount", "Meeting Timed Lead Count", scale=0, precision=6
        ),
        _measure("QualifiedCount", "Qualified Count", scale=0, precision=6),
        _measure("OpenLeadCount", "Open Lead Count", scale=0, precision=6),
        _measure("MQLLeadCount", "MQL Lead Count", scale=0, precision=6),
        _measure("SQLLeadCount", "SQL Lead Count", scale=0, precision=6),
        _measure(
            "DisqualifiedLeadCount", "Disqualified Lead Count", scale=0, precision=6
        ),
        _measure(
            "MarketingDisqualifiedLeadCount",
            "Marketing Disqualified Lead Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "SalesDisqualifiedLeadCount",
            "Sales Disqualified Lead Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "OpportunityHandoffCount", "Opportunity Handoff Count", scale=0, precision=6
        ),
        _measure("OpenMQLLeadCount", "Open MQL Lead Count", scale=0, precision=6),
        _measure("OpenSQLLeadCount", "Open SQL Lead Count", scale=0, precision=6),
        _measure(
            "DirectLeadTouch24hCount",
            "Direct Lead Touch <24h Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "DirectLeadTouchAnyCount", "Direct Lead Touch Count", scale=0, precision=6
        ),
        _measure(
            "AssociatedTouch24hCount",
            "Associated Prospect Touch <24h Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "AssociatedTouchAnyCount",
            "Associated Prospect Touch Count",
            scale=0,
            precision=6,
        ),
        _measure("CompanyTouchCount", "Company Touch Count", scale=0, precision=6),
        _measure("ProspectLeadCount", "Prospect Lead Count", scale=0, precision=6),
        _measure(
            "CurrentClientLeadCount", "Current Client Lead Count", scale=0, precision=6
        ),
        _measure(
            "FormerClientLeadCount", "Former Client Lead Count", scale=0, precision=6
        ),
        _measure("PartnerLeadCount", "Partner Lead Count", scale=0, precision=6),
        _measure("StrongHandoffCount", "Strong Handoff Count", scale=0, precision=6),
        _measure(
            "DiscoveryHandoffCount", "Discovery Handoff Count", scale=0, precision=6
        ),
        _measure(
            "PendingStage3ReviewCount",
            "Pending Stage 3 Review Count",
            scale=0,
            precision=6,
        ),
        _measure("Stage3ApprovedCount", "Stage 3 Approved Count", scale=0, precision=6),
        _measure("QueueTaskCount", "Queue Task Count", scale=0, precision=6),
        _measure("OverdueTaskCount", "Overdue Task Count", scale=0, precision=6),
        _measure("DueTodayTaskCount", "Due Today Task Count", scale=0, precision=6),
        _measure("TotalActivityCount", "Total Activity Count", scale=0, precision=6),
        _measure(
            "LeadLinkedActivityCount",
            "Lead-Linked Activity Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "ContactLinkedActivityCount",
            "Contact-Linked Activity Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "AccountLinkedActivityCount",
            "Account-Linked Activity Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "OpportunityLinkedActivityCount",
            "Opportunity-Linked Activity Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "ProspectActivityCount", "Prospect Activity Count", scale=0, precision=6
        ),
        _measure(
            "CurrentClientActivityCount",
            "Current Client Activity Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "FormerClientActivityCount",
            "Former Client Activity Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "PartnerActivityCount", "Partner Activity Count", scale=0, precision=6
        ),
        _measure(
            "UnclassifiedActivityCount",
            "Unclassified Activity Count",
            scale=0,
            precision=6,
        ),
        _measure("ContactCount", "Contact Count", scale=0, precision=6),
        _measure("PersonaContactCount", "Persona Contact Count", scale=0, precision=6),
        _measure("UniquePersonaCount", "Unique Persona Count", scale=0, precision=6),
        _measure("CLevelPersonaCount", "C-Level Persona Count", scale=0, precision=6),
        _measure(
            "HLevelPersonaCount", "Head-Level Persona Count", scale=0, precision=6
        ),
        _measure(
            "NonPersonaContactCount", "Non-Persona Contact Count", scale=0, precision=6
        ),
        _measure(
            "OpenOpportunityCount", "Open Opportunity Count", scale=0, precision=6
        ),
        _measure("ActiveContactCount", "Active Contact Count", scale=0, precision=6),
        _measure(
            "ActivePersonaContactCount",
            "Active Persona Contact Count",
            scale=0,
            precision=6,
        ),
        _measure("SignalSourceCount", "Signal Source Count", scale=0, precision=6),
        _measure(
            "ProductSignalStrength", "Product Signal Strength", scale=0, precision=6
        ),
        _measure(
            "ProductSignalConfidenceScore",
            "Product Signal Confidence Score",
            scale=0,
            precision=6,
        ),
        _measure(
            "CampaignProductSignalCount",
            "Campaign Product Signal Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "OpportunityProductSignalCount",
            "Opportunity Product Signal Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "AccountOpportunityHistorySignalCount",
            "Account Opportunity History Signal Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "AccountProductSignalCount",
            "Account Opportunity Product Signal Count",
            scale=0,
            precision=6,
        ),
        _measure(
            "AccountMainlineSignalCount",
            "Account Mainline Signal Count",
            scale=0,
            precision=6,
        ),
        _measure("StageOrder", "Stage Order", scale=0, precision=6),
    ]

    return upload_dataset(
        inst,
        tok,
        DS,
        DS_LABEL,
        fields_meta,
        csv_bytes,
        poll_attempts=180,
        poll_interval=3,
    )


def _manager_steps(ds_id: str) -> dict[str, dict]:
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_team = coalesce_filter("f_team", "BDRTeam")
    filter_owner = coalesce_filter("f_owner", "OwnerName")
    filter_source = coalesce_filter("f_source", "SourceGroup")

    def scoped_filter(step_name: str, field_name: str, selector_name: str) -> str:
        return (
            f"{step_name} = filter {step_name} by "
            f'{{{{coalesce(column({selector_name}.selection, ["{field_name}"]), '
            f"column({selector_name}.result, [\"{field_name}\"])).asEquality('{field_name}')}}}};\n"
        )

    lead_detail = (
        load
        + 'q = filter q by RecordType == "lead_detail";\n'
        + filter_team
        + filter_owner
        + filter_source
    )
    detail = (
        load
        + 'q = filter q by (RecordType == "lead_detail") || (RecordType == "opportunity_detail");\n'
        + filter_team
        + filter_owner
        + filter_source
    )
    opportunity_detail = (
        load
        + 'q = filter q by RecordType == "opportunity_detail";\n'
        + filter_team
        + filter_owner
        + filter_source
    )
    rep_week = (
        load
        + 'q = filter q by RecordType == "rep_week";\n'
        + filter_team
        + filter_owner
        + filter_source
    )
    owner_week = (
        load
        + 'q = filter q by RecordType == "owner_week";\n'
        + filter_team
        + filter_owner
    )
    source_week = (
        load
        + 'q = filter q by RecordType == "source_week";\n'
        + filter_team
        + filter_source
    )
    campaign = (
        load
        + 'q = filter q by RecordType == "campaign_summary";\n'
        + filter_team
        + filter_owner
        + filter_source
    )
    integrity = (
        load
        + 'q = filter q by RecordType == "owner_integrity";\n'
        + filter_team
        + filter_owner
    )
    account_universe = (
        load
        + 'q = filter q by RecordType == "account_universe";\n'
        + filter_team
        + filter_owner
    )
    contact_coverage = (
        load
        + 'q = filter q by RecordType == "contact_coverage";\n'
        + filter_team
        + filter_owner
    )
    account_persona_product_target = (
        load
        + 'q = filter q by RecordType == "account_persona_product_target";\n'
        + filter_team
        + filter_owner
    )
    account_persona_product_target = (
        load
        + 'q = filter q by RecordType == "account_persona_product_target";\n'
        + filter_team
        + filter_owner
    )
    account_product_target = (
        load
        + 'q = filter q by RecordType == "account_product_target";\n'
        + filter_team
        + filter_owner
    )
    fy26_rep_week = (
        rep_week
        + f'q = filter q by WeekStartDate >= "{FY2026_START}" && WeekStartDate < "{FY2027_START}";\n'
    )
    fy25_26_rep_week = (
        rep_week
        + f'q = filter q by WeekStartDate >= "{FY2025_START}" && WeekStartDate < "{FY2027_START}";\n'
    )
    fy26_owner_week = (
        owner_week
        + f'q = filter q by WeekStartDate >= "{FY2026_START}" && WeekStartDate < "{FY2027_START}";\n'
    )
    fy26_source_week = (
        source_week
        + f'q = filter q by WeekStartDate >= "{FY2026_START}" && WeekStartDate < "{FY2027_START}";\n'
    )
    fy26_lead_detail = lead_detail + 'q = filter q by YearLabel == "FY2026";\n'
    fy26_opportunity_detail = (
        opportunity_detail + 'q = filter q by YearLabel == "FY2026";\n'
    )
    fy26_contact_coverage = (
        contact_coverage + 'q = filter q by YearLabel == "FY2026";\n'
    )
    fy26_account_universe = (
        account_universe + 'q = filter q by YearLabel == "FY2026";\n'
    )
    fy26_account_product_target = (
        account_product_target + 'q = filter q by YearLabel == "FY2026";\n'
    )
    fy26_account_persona_product_target = (
        load
        + 'q = filter q by RecordType == "account_persona_product_target";\n'
        + filter_team
        + filter_owner
        + 'q = filter q by YearLabel == "FY2026";\n'
    )
    product_focus = (
        '(case when TargetedProduct != "" then TargetedProduct else "Unknown" end)'
    )
    product_focus_source = '(case when TargetedProductSource != "" then TargetedProductSource else "Unknown" end)'
    opp_product_focus = '(case when OpportunityProduct != "" then OpportunityProduct else "Unknown" end)'
    opp_product_focus_source = '(case when OpportunityProductSource != "" then OpportunityProductSource else "Unknown" end)'
    role_focus = (
        '(case when ContactOfficialTitle != "" then ContactOfficialTitle '
        'when LeadTitle != "" then LeadTitle '
        'when Persona != "" then Persona '
        'else "Unknown" end)'
    )

    return {
        "f_team": af("BDRTeam", ds_meta, select_mode="single", start='["AMERS"]'),
        "f_owner": af("OwnerName", ds_meta),
        "f_source": af("SourceGroup", ds_meta),
        "s_summary": sq(
            account_universe
            + "q = foreach q generate "
            + '(case when ClientBaseClass == "Prospect" then 1 else 0 end) as ProspectAccountFlag, '
            + '(case when ClientBaseClass == "Former Client" then 1 else 0 end) as FormerClientAccountFlag, '
            + '(case when ClientBaseClass == "Current Client" then 1 else 0 end) as CurrentClientAccountFlag, '
            + "ContactCount as ContactCount, "
            + "PersonaContactCount as PersonaContactCount, "
            + "OpenOpportunityCount as OpenOpportunityCount, "
            + "DiscoveryHandoffCount as DiscoveryHandoffCount, "
            + "OpportunityHandoffCount as OpportunityHandoffCount, "
            + "PendingStage3ReviewCount as PendingStage3ReviewCount, "
            + "Stage3ApprovedCount as Stage3ApprovedCount;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as account_count, "
            + "sum(ProspectAccountFlag) as prospect_accounts, "
            + "sum(FormerClientAccountFlag) as former_client_accounts, "
            + "sum(CurrentClientAccountFlag) as current_client_accounts, "
            + "sum(ContactCount) as contact_count, "
            + "sum(PersonaContactCount) as persona_contacts, "
            + "sum(OpenOpportunityCount) as open_opportunities, "
            + "sum(DiscoveryHandoffCount) as discovery_handoffs, "
            + "sum(OpportunityHandoffCount) as stage3_handoffs, "
            + "sum(PendingStage3ReviewCount) as pending_stage3_review, "
            + "sum(Stage3ApprovedCount) as stage3_approved;"
        ),
        "s_sla_bullet": sq(
            integrity
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + '"Direct Lead Touch <24h" as MetricLabel, '
            + "case when sum(LeadCount) > 0 then (sum(DirectLeadTouch24hCount) * 100) / sum(LeadCount) else 0 end as Actual, "
            + "100 as Target;"
        ),
        "s_integrity_bullet": sq(
            integrity
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + '"Associated Prospect Response <24h" as MetricLabel, '
            + "case when sum(LeadCount) > 0 then (sum(AssociatedTouch24hCount) * 100) / sum(LeadCount) else 0 end as Actual, "
            + "80 as Target;"
        ),
        "s_assoc_integrity": sq(
            integrity
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + '"Lead-Linked Activity" as MetricLabel, '
            + "case when sum(TotalActivityCount) > 0 then (sum(LeadLinkedActivityCount) * 100) / sum(TotalActivityCount) else 0 end as Actual, "
            + "80 as Target;"
        ),
        "s_source_bullet": sq(
            lead_detail
            + "q = foreach q generate "
            + '(case when LeadSource != "" then 1 else 0 end) as has_source;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + '"Lead Source Present" as MetricLabel, '
            + "case when count() > 0 then (sum(has_source) * 100) / count() else 0 end as Actual, "
            + "100 as Target;"
        ),
        "s_story": sq(
            fy26_rep_week
            + "q = group q by WeekStartDate;\n"
            + "q = foreach q generate WeekStartDate, "
            + "sum(LeadCreatedCount) as LeadCreatedCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount;\n"
            + "q = order q by WeekStartDate asc;"
        ),
        "s_yoy_rhythm": sq(
            fy25_26_rep_week
            + "q = foreach q generate "
            + f'(case when WeekStartDate >= "{FY2026_START}" then "FY2026" else "FY2025" end) as FiscalYear, '
            + "LeadCreatedCount as LeadCreatedCount, "
            + "MeetingHeldCount as MeetingHeldCount, "
            + "QualifiedCount as QualifiedCount, "
            + "OpportunityHandoffCount as OpportunityHandoffCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + "q = group q by FiscalYear;\n"
            + "q = foreach q generate FiscalYear, "
            + "sum(LeadCreatedCount) as LeadCreatedCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR;\n"
            + "q = order q by FiscalYear asc;"
        ),
        "s_rep_table": sq(
            detail
            + "q = foreach q generate "
            + "OwnerName, BDRTeam, LeadCount as LeadCount, OpenLeadCount as OpenLeadCount, "
            + "OpenMQLLeadCount as OpenMQLLeadCount, OpenSQLLeadCount as OpenSQLLeadCount, "
            + "MarketingDisqualifiedLeadCount as MarketingDisqualifiedLeadCount, "
            + "SalesDisqualifiedLeadCount as SalesDisqualifiedLeadCount, "
            + "MeetingHeldLeadCount as MeetingHeldLeadCount, "
            + "QualifiedCount as QualifiedCount, "
            + "PendingStage3ReviewCount as PendingStage3ReviewCount, "
            + "Stage3ApprovedCount as Stage3ApprovedCount;\n"
            + "q = group q by (OwnerName, BDRTeam);\n"
            + "q = foreach q generate "
            + "OwnerName, BDRTeam, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(OpenLeadCount) as OpenLeadCount, "
            + "sum(OpenMQLLeadCount) as OpenMQLLeadCount, "
            + "sum(OpenSQLLeadCount) as OpenSQLLeadCount, "
            + "sum(MarketingDisqualifiedLeadCount) as MarketingDisqualifiedLeadCount, "
            + "sum(SalesDisqualifiedLeadCount) as SalesDisqualifiedLeadCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldLeadCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(PendingStage3ReviewCount) as PendingStage3ReviewCount, "
            + "sum(Stage3ApprovedCount) as Stage3ApprovedCount;\n"
            + 'q2 = load "BDR_Operating_Rhythm";\n'
            + 'q2 = filter q2 by RecordType == "account_universe";\n'
            + scoped_filter("q2", "BDRTeam", "f_team")
            + scoped_filter("q2", "OwnerName", "f_owner")
            + "q2 = foreach q2 generate "
            + "OwnerName, BDRTeam, "
            + "1 as AccountCount, "
            + '(case when ClientBaseClass == "Prospect" then 1 else 0 end) as ProspectAccountCount, '
            + '(case when ClientBaseClass == "Former Client" then 1 else 0 end) as FormerClientAccountCount, '
            + '(case when ClientBaseClass == "Current Client" then 1 else 0 end) as CurrentClientAccountCount, '
            + "PersonaContactCount as PersonaContactCount, "
            + "ContactCount as ContactCount, "
            + "OpenOpportunityCount as OpenOpportunityCount, "
            + "DiscoveryHandoffCount as DiscoveryHandoffCount, "
            + "OpportunityHandoffCount as AccountStage3Count, "
            + "PendingStage3ReviewCount as PendingStage3ReviewCount, "
            + "Stage3ApprovedCount as Stage3ApprovedCount;\n"
            + "q2 = group q2 by (OwnerName, BDRTeam);\n"
            + "q2 = foreach q2 generate "
            + "OwnerName, BDRTeam, "
            + "sum(AccountCount) as AccountCount, "
            + "sum(ProspectAccountCount) as ProspectAccountCount, "
            + "sum(FormerClientAccountCount) as FormerClientAccountCount, "
            + "sum(CurrentClientAccountCount) as CurrentClientAccountCount, "
            + "sum(PersonaContactCount) as PersonaContactCount, "
            + "sum(ContactCount) as ContactCount, "
            + "sum(OpenOpportunityCount) as OpenOpportunityCount, "
            + "sum(DiscoveryHandoffCount) as DiscoveryHandoffCount, "
            + "sum(AccountStage3Count) as AccountStage3Count, "
            + "sum(PendingStage3ReviewCount) as PendingStage3ReviewCount, "
            + "sum(Stage3ApprovedCount) as Stage3ApprovedCount;\n"
            + "q = cogroup q by (OwnerName, BDRTeam) full, q2 by (OwnerName, BDRTeam);\n"
            + "q = foreach q generate "
            + "coalesce(q.OwnerName, q2.OwnerName) as OwnerName, "
            + "coalesce(q.BDRTeam, q2.BDRTeam) as BDRTeam, "
            + "coalesce(sum(q.LeadCount), 0) as LeadCount, "
            + "coalesce(sum(q.OpenLeadCount), 0) as OpenLeadCount, "
            + "coalesce(sum(q.OpenMQLLeadCount), 0) as OpenMQLLeadCount, "
            + "coalesce(sum(q.OpenSQLLeadCount), 0) as OpenSQLLeadCount, "
            + "coalesce(sum(q.MarketingDisqualifiedLeadCount), 0) as MarketingDisqualifiedLeadCount, "
            + "coalesce(sum(q.SalesDisqualifiedLeadCount), 0) as SalesDisqualifiedLeadCount, "
            + "coalesce(sum(q.MeetingHeldLeadCount), 0) as MeetingHeldLeadCount, "
            + "coalesce(sum(q2.AccountCount), 0) as AccountCount, "
            + "coalesce(sum(q2.PersonaContactCount), 0) as PersonaContactCount, "
            + "coalesce(sum(q2.OpenOpportunityCount), 0) as OpenOpportunityCount, "
            + "coalesce(sum(q2.DiscoveryHandoffCount), 0) as DiscoveryHandoffCount, "
            + "coalesce(sum(q2.AccountStage3Count), 0) as AccountStage3Count, "
            + "coalesce(sum(q.PendingStage3ReviewCount), 0) + coalesce(sum(q2.PendingStage3ReviewCount), 0) as PendingStage3ReviewCount, "
            + "coalesce(sum(q.Stage3ApprovedCount), 0) + coalesce(sum(q2.Stage3ApprovedCount), 0) as Stage3ApprovedCount, "
            + "case when coalesce(sum(q.LeadCount), 0) > 0 then (coalesce(sum(q.MeetingHeldLeadCount), 0) * 100) / coalesce(sum(q.LeadCount), 0) else 0 end as LeadToMeetingPct, "
            + "case when coalesce(sum(q.LeadCount), 0) > 0 then (coalesce(sum(q.QualifiedCount), 0) * 100) / coalesce(sum(q.LeadCount), 0) else 0 end as LeadToOppPct;\n"
            + "q = order q by AccountCount desc;\n"
            + "q = limit q 15;"
        ),
        "s_account_mix": sq(
            account_universe
            + "q = group q by ClientBaseClass;\n"
            + "q = foreach q generate "
            + "ClientBaseClass, "
            + "count() as AccountCount, "
            + "sum(ContactCount) as ContactCount, "
            + "sum(PersonaContactCount) as PersonaContactCount, "
            + "sum(OpenOpportunityCount) as OpenOpportunityCount, "
            + "sum(DiscoveryHandoffCount) as DiscoveryHandoffCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount;\n"
            + "q = order q by AccountCount desc;\n"
            + "q = limit q 8;"
        ),
        "s_weekly_activity": sq(
            fy26_rep_week
            + "q = group q by WeekStartDate;\n"
            + "q = foreach q generate WeekStartDate, "
            + "sum(CallCount) as CallCount, "
            + "sum(EmailCount) as EmailCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount;\n"
            + "q = order q by WeekStartDate asc;"
        ),
        "s_rep_activity_mix": sq(
            fy26_owner_week
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate "
            + "OwnerName, "
            + "sum(CallCount) as CallCount, "
            + "sum(EmailCount) as EmailCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "(sum(CallCount) + sum(EmailCount) + sum(MeetingHeldCount)) as TotalActivity;\n"
            + "q = order q by TotalActivity desc;\n"
            + "q = limit q 10;"
        ),
        "s_rep_execution": sq(
            fy26_lead_detail
            + "q = group q by (OwnerName, BDRTeam, BDRRole);\n"
            + "q = foreach q generate "
            + "OwnerName, BDRTeam, BDRRole, "
            + "sum(LeadCount) as LeadCreatedCount, "
            + "sum(CallCount) as CallCount, "
            + "sum(EmailCount) as EmailCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldCount, "
            + "sum(SourcedOpportunityCount) as OpenOpportunityCount, "
            + "sum(SourcedARR) as KnownAttributedARR, "
            + "sum(SLABreachCount) as SLABreachCount, "
            + "sum(CompanyTouchCount) as CompanyTouchCount, "
            + "sum(MeetingTimedLeadCount) as MeetingTimedLeadCount, "
            + "case when sum(LeadCount) > 0 then (sum(ResponseCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct, "
            + "case when sum(LeadCount) > 0 then (sum(MeetingHeldLeadCount) * 100) / sum(LeadCount) else 0 end as LeadToMeetingPct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct, "
            + "case when sum(MeetingTimedLeadCount) > 0 then sum(DaysToFirstMeeting) / sum(MeetingTimedLeadCount) else 0 end as AvgDaysToFirstMeeting;\n"
            + "q = order q by OpenOpportunityCount desc;\n"
            + "q = limit q 15;"
        ),
        "s_rep_handoff": sq(
            fy26_opportunity_detail
            + "q = group q by (OwnerName, ManagerName, BDRTeam);\n"
            + "q = foreach q generate "
            + "OwnerName, ManagerName, BDRTeam, "
            + "sum(DiscoveryHandoffCount) as Stage2DiscoveryCount, "
            + "sum(OpportunityHandoffCount) as Stage3EngagementCount, "
            + "sum(PendingStage3ReviewCount) as PendingStage3ReviewCount, "
            + "sum(Stage3ApprovedCount) as Stage3ApprovedCount, "
            + "sum(SourcedARR) as KnownAttributedARR, "
            + "case when sum(OpportunityHandoffCount) > 0 then sum(Stage2To3Days) / sum(OpportunityHandoffCount) else 0 end as AvgStage2To3Days;\n"
            + "q = order q by Stage3EngagementCount desc;\n"
            + "q = limit q 15;"
        ),
        "s_rep_integrity": sq(
            load
            + 'q = filter q by RecordType == "owner_integrity";\n'
            + filter_team
            + filter_owner
            + f'q = filter q by WeekStartDate >= "{FY2026_START}" && WeekStartDate < "{FY2027_START}";\n'
            + "q = group q by (OwnerName, ManagerName, BDRRole);\n"
            + "q = foreach q generate "
            + "OwnerName, ManagerName, BDRRole, "
            + "sum(DirectLeadTouch24hCount) as DirectLeadTouch24hCount, "
            + "sum(DirectLeadTouchAnyCount) as DirectLeadTouchAnyCount, "
            + "sum(AssociatedTouch24hCount) as AssociatedTouch24hCount, "
            + "sum(AssociatedTouchAnyCount) as AssociatedTouchAnyCount, "
            + "sum(TotalActivityCount) as TotalActivityCount, "
            + "sum(LeadLinkedActivityCount) as LeadLinkedActivityCount, "
            + "sum(ContactLinkedActivityCount) as ContactLinkedActivityCount, "
            + "sum(AccountLinkedActivityCount) as AccountLinkedActivityCount, "
            + "sum(OpportunityLinkedActivityCount) as OpportunityLinkedActivityCount, "
            + "sum(ProspectActivityCount) as ProspectActivityCount, "
            + "sum(CurrentClientActivityCount) as CurrentClientActivityCount, "
            + "sum(FormerClientActivityCount) as FormerClientActivityCount, "
            + "case when sum(LeadCount) > 0 then (sum(DirectLeadTouch24hCount) * 100) / sum(LeadCount) else 0 end as DirectLeadTouch24hPct, "
            + "case when sum(LeadCount) > 0 then (sum(DirectLeadTouchAnyCount) * 100) / sum(LeadCount) else 0 end as DirectLeadTouchAnyPct, "
            + "case when sum(LeadCount) > 0 then (sum(AssociatedTouch24hCount) * 100) / sum(LeadCount) else 0 end as AssociatedTouch24hPct, "
            + "case when sum(LeadCount) > 0 then (sum(AssociatedTouchAnyCount) * 100) / sum(LeadCount) else 0 end as AssociatedTouchAnyPct, "
            + "case when sum(TotalActivityCount) > 0 then (sum(LeadLinkedActivityCount) * 100) / sum(TotalActivityCount) else 0 end as LeadLinkedActivityPct, "
            + "case when sum(TotalActivityCount) > 0 then (sum(ContactLinkedActivityCount) * 100) / sum(TotalActivityCount) else 0 end as ContactLinkedActivityPct, "
            + "case when sum(TotalActivityCount) > 0 then (sum(AccountLinkedActivityCount) * 100) / sum(TotalActivityCount) else 0 end as AccountLinkedActivityPct;\n"
            + "q = order q by DirectLeadTouch24hPct asc;\n"
            + "q = limit q 15;"
        ),
        "s_outreach_mix": sq(
            fy26_owner_week
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate "
            + "OwnerName, "
            + "sum(ProspectActivityCount) as ProspectActivityCount, "
            + "sum(CurrentClientActivityCount) as CurrentClientActivityCount, "
            + "sum(FormerClientActivityCount) as FormerClientActivityCount, "
            + "sum(PartnerActivityCount) as PartnerActivityCount, "
            + "sum(UnclassifiedActivityCount) as UnclassifiedActivityCount, "
            + "sum(TotalActivityCount) as TotalActivityCount;\n"
            + "q = order q by TotalActivityCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_source_week": sq(
            fy26_source_week
            + "q = group q by (WeekStartDate, SourceGroup);\n"
            + "q = foreach q generate WeekStartDate, SourceGroup, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount;\n"
            + "q = order q by WeekStartDate asc;"
        ),
        "s_campaign_product": sq(
            campaign
            + 'q = filter q by CampaignProduct != "";\n'
            + "q = group q by CampaignProduct;\n"
            + "q = foreach q generate "
            + "CampaignProduct, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(SourcedARR) as KnownAttributedARR;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_source_quality": sq(
            detail
            + "q = foreach q generate "
            + "SourceGroup, LeadCount as LeadCount, OpenMQLLeadCount as OpenMQLLeadCount, OpenSQLLeadCount as OpenSQLLeadCount, "
            + "MarketingDisqualifiedLeadCount as MarketingDisqualifiedLeadCount, SalesDisqualifiedLeadCount as SalesDisqualifiedLeadCount, MeetingHeldLeadCount as MeetingHeldLeadCount, "
            + "QualifiedCount as QualifiedCount, "
            + "SourcedARR as KnownAttributedARR, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponderLeadCount;\n'
            + "q = group q by SourceGroup;\n"
            + "q = foreach q generate "
            + "SourceGroup, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(OpenMQLLeadCount) as OpenMQLLeadCount, "
            + "sum(OpenSQLLeadCount) as OpenSQLLeadCount, "
            + "sum(MarketingDisqualifiedLeadCount) as MarketingDisqualifiedLeadCount, "
            + "sum(SalesDisqualifiedLeadCount) as SalesDisqualifiedLeadCount, "
            + "sum(ResponderLeadCount) as ResponderLeadCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldLeadCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(ResponderLeadCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct, "
            + "case when sum(LeadCount) > 0 then (sum(MeetingHeldLeadCount) * 100) / sum(LeadCount) else 0 end as LeadToMeetingPct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_campaign_quality": sq(
            campaign
            + 'q = filter q by Campaign != "";\n'
            + 'q = filter q by Campaign != "Unmapped Campaign";\n'
            + "q = group q by (Campaign, CampaignProduct, CampaignScopeType, SourceGroup);\n"
            + "q = foreach q generate Campaign, CampaignProduct, CampaignScopeType, SourceGroup, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(MQLLeadCount) as MQLLeadCount, "
            + "sum(SQLLeadCount) as SQLLeadCount, "
            + "sum(MarketingDisqualifiedLeadCount) as MarketingDisqualifiedLeadCount, "
            + "sum(SalesDisqualifiedLeadCount) as SalesDisqualifiedLeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(SourcedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(ResponseCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct, "
            + "case when sum(LeadCount) > 0 then (sum(MeetingHeldCount) * 100) / sum(LeadCount) else 0 end as LeadToMeetingPct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 20;"
        ),
        "s_source_product": sq(
            detail
            + "q = foreach q generate "
            + f"SourceGroup, {product_focus} as ProductFocus, {product_focus_source} as ProductSource, "
            + "LeadCount as LeadCount, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponseCount, '
            + "MeetingHeldLeadCount as MeetingHeldLeadCount, "
            + "QualifiedCount as QualifiedCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (SourceGroup, ProductFocus, ProductSource);\n"
            + "q = foreach q generate "
            + "SourceGroup, ProductFocus, ProductSource, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "case when sum(LeadCount) > 0 then (sum(MeetingHeldCount) * 100) / sum(LeadCount) else 0 end as LeadToMeetingPct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_pipeline_monthly": sq(
            fy26_opportunity_detail
            + "q = foreach q generate "
            + "MonthStartDate, OpenOpportunityCount as OpenOpportunityCount, DiscoveryHandoffCount as DiscoveryHandoffCount, "
            + "OpportunityHandoffCount as OpportunityHandoffCount, SourcedARR as KnownAttributedARR;\n"
            + "q = filter q by MonthStartDate is not null;\n"
            + "q = group q by MonthStartDate;\n"
            + "q = foreach q generate "
            + "MonthStartDate, "
            + "sum(OpenOpportunityCount) as OpenOpportunityCount, "
            + "sum(DiscoveryHandoffCount) as DiscoveryHandoffCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR;\n"
            + "q = order q by MonthStartDate asc;\n"
            + "q = limit q 18;"
        ),
        "s_industry_outreach": sq(
            'q1 = load "BDR_Operating_Rhythm";\n'
            + 'q1 = filter q1 by RecordType == "contact_coverage";\n'
            + scoped_filter("q1", "BDRTeam", "f_team")
            + scoped_filter("q1", "OwnerName", "f_owner")
            + 'q1 = filter q1 by YearLabel == "FY2026";\n'
            + "q1 = foreach q1 generate Industry, ContactCount as ContactCount, ContactTouchCount as ContactTouchCount, "
            + "MeetingHeldCount as MeetingHeldCount;\n"
            + 'q1 = filter q1 by Industry != "";\n'
            + "q1 = group q1 by Industry;\n"
            + "q1 = foreach q1 generate Industry, "
            + "sum(ContactCount) as ContactCount, "
            + "sum(ContactTouchCount) as ContactTouchCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount;\n"
            + 'q2 = load "BDR_Operating_Rhythm";\n'
            + 'q2 = filter q2 by RecordType == "account_universe";\n'
            + scoped_filter("q2", "BDRTeam", "f_team")
            + scoped_filter("q2", "OwnerName", "f_owner")
            + 'q2 = filter q2 by YearLabel == "FY2026";\n'
            + "q2 = foreach q2 generate Industry, 1 as AccountCount, OpenOpportunityCount as OpenOpportunityCount, OpportunityHandoffCount as OpportunityHandoffCount;\n"
            + 'q2 = filter q2 by Industry != "";\n'
            + "q2 = group q2 by Industry;\n"
            + "q2 = foreach q2 generate Industry, "
            + "sum(AccountCount) as AccountCount, "
            + "sum(OpenOpportunityCount) as OpenOpportunityCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount;\n"
            + "q = cogroup q1 by Industry full, q2 by Industry;\n"
            + "q = foreach q generate "
            + "coalesce(q1.Industry, q2.Industry) as Industry, "
            + "coalesce(sum(q1.ContactCount), 0) as ContactCount, "
            + "coalesce(sum(q1.ContactTouchCount), 0) as ContactTouchCount, "
            + "coalesce(sum(q1.MeetingHeldCount), 0) as MeetingHeldCount, "
            + "coalesce(sum(q2.AccountCount), 0) as AccountCount, "
            + "coalesce(sum(q2.OpenOpportunityCount), 0) as OpenOpportunityCount, "
            + "coalesce(sum(q2.OpportunityHandoffCount), 0) as OpportunityHandoffCount, "
            + "case when coalesce(sum(q1.ContactCount), 0) > 0 then (coalesce(sum(q1.ContactTouchCount), 0) * 100) / coalesce(sum(q1.ContactCount), 0) else 0 end as ActiveCoveragePct;\n"
            + "q = order q by ContactTouchCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_industry_product_heatmap": sq(
            fy26_opportunity_detail
            + "q = foreach q generate "
            + '(case when Industry != "" then Industry else "Unknown Industry" end) as Industry, '
            + f'(case when {opp_product_focus} == "Unknown" then "Missing Product" else {opp_product_focus} end) as ProductFocus, '
            + "1 as OpportunityCount;\n"
            + 'q = filter q by Industry != "Unknown Industry";\n'
            + "q = group q by (Industry, ProductFocus);\n"
            + "q = foreach q generate "
            + "Industry, ProductFocus, "
            + "sum(OpportunityCount) as OpportunityCount;\n"
            + "q = order q by OpportunityCount desc;\n"
            + "q = limit q 60;"
        ),
        "s_product_signal_summary": sq(
            'q1 = load "BDR_Operating_Rhythm";\n'
            + 'q1 = filter q1 by RecordType == "account_universe";\n'
            + scoped_filter("q1", "BDRTeam", "f_team")
            + scoped_filter("q1", "OwnerName", "f_owner")
            + 'q1 = filter q1 by YearLabel == "FY2026";\n'
            + "q1 = group q1 by all;\n"
            + "q1 = foreach q1 generate count() as TotalAccounts;\n"
            + 'q2 = load "BDR_Operating_Rhythm";\n'
            + 'q2 = filter q2 by RecordType == "account_product_target";\n'
            + scoped_filter("q2", "BDRTeam", "f_team")
            + scoped_filter("q2", "OwnerName", "f_owner")
            + 'q2 = filter q2 by YearLabel == "FY2026";\n'
            + 'q2 = filter q2 by TargetedProduct != "Unknown";\n'
            + "q2 = group q2 by 'ContextAccountId';\n"
            + "q2 = foreach q2 generate 'ContextAccountId' as ContextAccountId;\n"
            + "q2 = group q2 by all;\n"
            + "q2 = foreach q2 generate count() as KnownProductAccounts;\n"
            + "q = cogroup q1 by all, q2 by all;\n"
            + "q = foreach q generate "
            + "first(q1.TotalAccounts) as TotalAccounts, "
            + "coalesce(first(q2.KnownProductAccounts), 0) as KnownProductAccounts, "
            + "(first(q1.TotalAccounts) - coalesce(first(q2.KnownProductAccounts), 0)) as MissingProductAccounts, "
            + "case when first(q1.TotalAccounts) > 0 then (coalesce(first(q2.KnownProductAccounts), 0) * 100) / first(q1.TotalAccounts) else 0 end as KnownProductCoveragePct;\n"
        ),
        "s_product_signal_coverage": sq(
            'q1 = load "BDR_Operating_Rhythm";\n'
            + 'q1 = filter q1 by RecordType == "account_universe";\n'
            + scoped_filter("q1", "BDRTeam", "f_team")
            + scoped_filter("q1", "OwnerName", "f_owner")
            + 'q1 = filter q1 by YearLabel == "FY2026";\n'
            + 'q1 = filter q1 by Industry != "";\n'
            + "q1 = group q1 by Industry;\n"
            + "q1 = foreach q1 generate Industry, count() as TotalAccounts;\n"
            + 'q2 = load "BDR_Operating_Rhythm";\n'
            + 'q2 = filter q2 by RecordType == "account_product_target";\n'
            + scoped_filter("q2", "BDRTeam", "f_team")
            + scoped_filter("q2", "OwnerName", "f_owner")
            + 'q2 = filter q2 by YearLabel == "FY2026";\n'
            + 'q2 = filter q2 by Industry != "";\n'
            + 'q2 = filter q2 by TargetedProduct != "Unknown";\n'
            + 'q2 = filter q2 by ProductSignalConfidence == "High";\n'
            + "q2 = group q2 by (Industry, ContextAccountId);\n"
            + "q2 = foreach q2 generate Industry as Industry, ContextAccountId as ContextAccountId;\n"
            + "q2 = group q2 by Industry;\n"
            + "q2 = foreach q2 generate Industry, count() as HighConfidenceAccounts;\n"
            + 'q3 = load "BDR_Operating_Rhythm";\n'
            + 'q3 = filter q3 by RecordType == "account_product_target";\n'
            + scoped_filter("q3", "BDRTeam", "f_team")
            + scoped_filter("q3", "OwnerName", "f_owner")
            + 'q3 = filter q3 by YearLabel == "FY2026";\n'
            + 'q3 = filter q3 by Industry != "";\n'
            + 'q3 = filter q3 by TargetedProduct != "Unknown";\n'
            + 'q3 = filter q3 by ProductSignalConfidence == "Medium";\n'
            + "q3 = group q3 by (Industry, ContextAccountId);\n"
            + "q3 = foreach q3 generate Industry as Industry, ContextAccountId as ContextAccountId;\n"
            + "q3 = group q3 by Industry;\n"
            + "q3 = foreach q3 generate Industry, count() as MediumConfidenceAccounts;\n"
            + 'q4 = load "BDR_Operating_Rhythm";\n'
            + 'q4 = filter q4 by RecordType == "account_product_target";\n'
            + scoped_filter("q4", "BDRTeam", "f_team")
            + scoped_filter("q4", "OwnerName", "f_owner")
            + 'q4 = filter q4 by YearLabel == "FY2026";\n'
            + 'q4 = filter q4 by Industry != "";\n'
            + 'q4 = filter q4 by TargetedProduct != "Unknown";\n'
            + 'q4 = filter q4 by ProductSignalConfidence == "Low";\n'
            + "q4 = group q4 by (Industry, ContextAccountId);\n"
            + "q4 = foreach q4 generate Industry as Industry, ContextAccountId as ContextAccountId;\n"
            + "q4 = group q4 by Industry;\n"
            + "q4 = foreach q4 generate Industry, count() as LowConfidenceAccounts;\n"
            + "q = cogroup q1 by Industry full, q2 by Industry full, q3 by Industry full, q4 by Industry full;\n"
            + "q = foreach q generate "
            + "coalesce(q1.Industry, q2.Industry, q3.Industry, q4.Industry) as Industry, "
            + "coalesce(sum(q1.TotalAccounts), 0) as TotalAccounts, "
            + "coalesce(sum(q2.HighConfidenceAccounts), 0) as HighConfidenceAccounts, "
            + "coalesce(sum(q3.MediumConfidenceAccounts), 0) as MediumConfidenceAccounts, "
            + "coalesce(sum(q4.LowConfidenceAccounts), 0) as LowConfidenceAccounts, "
            + "(coalesce(sum(q2.HighConfidenceAccounts), 0) + coalesce(sum(q3.MediumConfidenceAccounts), 0) + coalesce(sum(q4.LowConfidenceAccounts), 0)) as KnownProductAccounts, "
            + "(coalesce(sum(q1.TotalAccounts), 0) - (coalesce(sum(q2.HighConfidenceAccounts), 0) + coalesce(sum(q3.MediumConfidenceAccounts), 0) + coalesce(sum(q4.LowConfidenceAccounts), 0))) as MissingProductAccounts, "
            + "case when coalesce(sum(q1.TotalAccounts), 0) > 0 then ((coalesce(sum(q2.HighConfidenceAccounts), 0) + coalesce(sum(q3.MediumConfidenceAccounts), 0) + coalesce(sum(q4.LowConfidenceAccounts), 0)) * 100) / coalesce(sum(q1.TotalAccounts), 0) else 0 end as KnownProductCoveragePct;\n"
            + "q = order q by KnownProductAccounts desc;\n"
            + "q = limit q 20;"
        ),
        "s_target_product_heatmap": sq(
            fy26_account_product_target
            + "q = foreach q generate "
            + '(case when Industry != "" then Industry else "Unknown Industry" end) as Industry, '
            + "TargetedProduct as ProductFocus, "
            + "ContextAccountId as ContextAccountId, "
            + "ActivePersonaContactCount as ActivePersonaContactCount, "
            + "OpenOpportunityCount as OpenOpportunityCount, "
            + "ProductSignalStrength as ProductSignalStrength;\n"
            + 'q = filter q by Industry != "Unknown Industry";\n'
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (Industry, ProductFocus);\n"
            + "q = foreach q generate "
            + "Industry, ProductFocus, "
            + "unique(ContextAccountId) as AccountCount, "
            + "sum(ActivePersonaContactCount) as ActivePersonaContactCount, "
            + "sum(OpenOpportunityCount) as OpenOpportunityCount, "
            + "sum(ProductSignalStrength) as PocketStrength;\n"
            + "q = order q by PocketStrength desc;\n"
            + "q = limit q 60;"
        ),
        "s_open_opp_product": sq(
            fy26_opportunity_detail
            + "q = foreach q generate "
            + f"{opp_product_focus} as ProductFocus, {opp_product_focus_source} as ProductSource, "
            + "OpenOpportunityCount as OpenOpportunityCount, DiscoveryHandoffCount as DiscoveryHandoffCount, "
            + "OpportunityHandoffCount as OpportunityHandoffCount, SourcedARR as KnownAttributedARR;\n"
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (ProductFocus, ProductSource);\n"
            + "q = foreach q generate "
            + "ProductFocus, ProductSource, "
            + "sum(OpenOpportunityCount) as OpenOpportunityCount, "
            + "sum(DiscoveryHandoffCount) as DiscoveryHandoffCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR;\n"
            + "q = order q by OpenOpportunityCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_role_industry": sq(
            fy26_contact_coverage
            + "q = foreach q generate "
            + f"{role_focus} as RoleFocus, Industry, "
            + "ContactCount as ContactCount, MeetingHeldCount as MeetingHeldCount, QualifiedCount as QualifiedCount;\n"
            + 'q = filter q by RoleFocus != "Unknown";\n'
            + 'q = filter q by Industry != "";\n'
            + "q = group q by (RoleFocus, Industry);\n"
            + "q = foreach q generate "
            + "RoleFocus, Industry, "
            + "sum(ContactCount) as ContactCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "case when sum(ContactCount) > 0 then (sum(QualifiedCount) * 100) / sum(ContactCount) else 0 end as ContactToOppPct;\n"
            + "q = order q by ContactToOppPct desc;\n"
            + "q = limit q 60;"
        ),
        "s_persona_product": sq(
            fy26_account_persona_product_target
            + "q = foreach q generate "
            + '(case when Persona != "" then Persona else "Unknown Persona" end) as Persona, '
            + "TargetedProduct as ProductFocus, "
            + "ProductSignalConfidence as ProductSignalConfidence, "
            + "ProductSourceSet as ProductSource, "
            + "1 as AccountCount, "
            + "PersonaContactCount as PersonaContactCount, "
            + "ContactTouchCount as ContactTouchCount, "
            + "MeetingHeldCount as MeetingHeldCount, "
            + "OpportunityHandoffCount as OpportunityHandoffCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + 'q = filter q by Persona != "";\n'
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (Persona, ProductFocus, ProductSignalConfidence, ProductSource);\n"
            + "q = foreach q generate "
            + "Persona, ProductFocus, ProductSignalConfidence, ProductSource, "
            + "sum(AccountCount) as AccountCount, "
            + "sum(PersonaContactCount) as ContactCount, "
            + "sum(ContactTouchCount) as ContactTouchCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(PersonaContactCount) > 0 then (sum(ContactTouchCount) * 100) / sum(PersonaContactCount) else 0 end as ResponseRatePct, "
            + "case when sum(PersonaContactCount) > 0 then (sum(OpportunityHandoffCount) * 100) / sum(PersonaContactCount) else 0 end as ContactToOppPct, "
            + "case when sum(AccountCount) > 0 then (sum(OpportunityHandoffCount) * 100) / sum(AccountCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by OpportunityHandoffCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_owner_success": sq(
            fy26_account_universe
            + 'q = filter q by Industry != "";\n'
            + "q = group q by (OwnerName, Industry);\n"
            + "q = foreach q generate "
            + "OwnerName, Industry, "
            + "count() as AccountCount, "
            + "sum(ContactCount) as ContactCount, "
            + "sum(OpenOpportunityCount) as OpenOpportunityCount, "
            + "sum(DiscoveryHandoffCount) as DiscoveryHandoffCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount, "
            + "sum(PendingStage3ReviewCount) as PendingStage3ReviewCount;\n"
            + "q = order q by OpportunityHandoffCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_client_product_mix": sq(
            fy26_account_universe
            + "q = foreach q generate "
            + "ClientBaseClass, "
            + f"{product_focus} as ProductFocus, {product_focus_source} as ProductSource, "
            + "1 as AccountCount, "
            + "ContactCount as ContactCount, "
            + "OpenOpportunityCount as OpenOpportunityCount, "
            + "OpportunityHandoffCount as OpportunityHandoffCount;\n"
            + 'q = filter q by ClientBaseClass != "";\n'
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (ClientBaseClass, ProductFocus, ProductSource);\n"
            + "q = foreach q generate "
            + "ClientBaseClass, ProductFocus, ProductSource, "
            + "sum(AccountCount) as AccountCount, "
            + "sum(ContactCount) as ContactCount, "
            + "sum(OpenOpportunityCount) as OpenOpportunityCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount;\n"
            + "q = order q by OpportunityHandoffCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_persona_success": sq(
            detail
            + "q = foreach q generate "
            + "Persona, LeadCount as LeadCount, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponderLeadCount, '
            + "MeetingHeldLeadCount as MeetingHeldLeadCount, "
            + "QualifiedCount as QualifiedCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + "q = group q by Persona;\n"
            + "q = foreach q generate "
            + "Persona, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponderLeadCount) as ResponderLeadCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldLeadCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(ResponderLeadCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct, "
            + "case when sum(LeadCount) > 0 then (sum(MeetingHeldLeadCount) * 100) / sum(LeadCount) else 0 end as LeadToMeetingPct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_industry_success": sq(
            detail
            + "q = foreach q generate "
            + "Industry, LeadCount as LeadCount, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponderLeadCount, '
            + "MeetingHeldLeadCount as MeetingHeldLeadCount, "
            + "QualifiedCount as QualifiedCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + "q = group q by Industry;\n"
            + "q = foreach q generate "
            + "Industry, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponderLeadCount) as ResponderLeadCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldLeadCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(ResponderLeadCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct, "
            + "case when sum(LeadCount) > 0 then (sum(MeetingHeldLeadCount) * 100) / sum(LeadCount) else 0 end as LeadToMeetingPct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_segment_success": sq(
            account_universe
            + 'q = filter q by TargetAccountFlag == "true";\n'
            + "q = foreach q generate "
            + "MatchedAccountTier, MatchedAccountSegment, 1 as AccountCount, "
            + "ContactCount as ContactCount, "
            + "PersonaContactCount as PersonaContactCount, "
            + "OpenOpportunityCount as OpenOpportunityCount, "
            + "OpportunityHandoffCount as OpportunityHandoffCount;\n"
            + "q = group q by (MatchedAccountTier, MatchedAccountSegment);\n"
            + "q = foreach q generate "
            + "MatchedAccountTier, MatchedAccountSegment, "
            + "sum(AccountCount) as AccountCount, "
            + "sum(ContactCount) as ContactCount, "
            + "sum(PersonaContactCount) as PersonaContactCount, "
            + "sum(OpenOpportunityCount) as OpenOpportunityCount, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount;\n"
            + "q = order q by OpportunityHandoffCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_persona_industry_success": sq(
            detail
            + "q = group q by (Persona, Industry);\n"
            + "q = foreach q generate Persona, Industry, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldLeadCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by LeadToOppPct desc;"
        ),
        "s_known_stage": sq(
            opportunity_detail
            + "q = group q by (OwnerName, HandoffQualityBand);\n"
            + "q = foreach q generate "
            + "OwnerName, HandoffQualityBand, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount, "
            + "sum(StrongHandoffCount) as StrongHandoffCount, "
            + "sum(DiscoveryHandoffCount) as DiscoveryHandoffCount, "
            + "sum(PendingStage3ReviewCount) as PendingStage3ReviewCount, "
            + "sum(Stage3ApprovedCount) as Stage3ApprovedCount, "
            + "sum(SourcedARR) as KnownAttributedARR;\n"
            + "q = order q by KnownAttributedARR desc;\n"
            + "q = limit q 20;"
        ),
        "s_client_mix": sq(
            detail
            + "q = group q by ClientBaseClass;\n"
            + "q = foreach q generate ClientBaseClass, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(DirectLeadTouch24hCount) as DirectLeadTouch24hCount, "
            + "sum(AssociatedTouch24hCount) as AssociatedTouch24hCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldLeadCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(SourcedARR) as KnownAttributedARR;\n"
            + "q = order q by LeadCount desc;\n"
            + "q = limit q 8;"
        ),
        "s_prioritized": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + "q = foreach q generate "
            + "Company, LeadName, OwnerName, Persona, "
            + f"{product_focus} as ProductFocus, {product_focus_source} as ProductSource, "
            + "SourceGroup, Campaign, NextBestAction, SuggestedTool, PriorityBand, LeadId, "
            + "(PriorityScore + "
            + '(case when HasResponseFlag == "true" then 20 else 0 end) + '
            + '(case when DaysSinceLastTouch > 14 && HasTouchFlag == "true" then 15 else 0 end) + '
            + "(case when OpenSQLLeadCount > 0 then 20 else 0 end) + "
            + '(case when TargetAccountFlag == "true" then 15 else 0 end) + '
            + '(case when ClientBaseClass == "Former Client" then 10 when ClientBaseClass == "Current Client" then 5 else 0 end)) as QueueRank, '
            + "PriorityScore, DaysSinceLastTouch;\n"
            + "q = order q by QueueRank desc;\n"
            + "q = limit q 10;"
        ),
        "s_response_queue": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by HasResponseFlag == "true";\n'
            + "q = foreach q generate Company, LeadName, OwnerName, Campaign, Persona, "
            + f"{product_focus} as ProductFocus, {product_focus_source} as ProductSource, "
            + "NextBestAction, SuggestedTool, "
            + "LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by DaysSinceLastTouch desc;\n"
            + "q = limit q 12;"
        ),
        "s_upcoming": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by NextMeetingDate != "";\n'
            + "q = foreach q generate Company, LeadName, OwnerName, SourceGroup, NextMeetingDate, "
            + "NextBestAction, SuggestedTool, LeadScore, LeadId;\n"
            + "q = order q by NextMeetingDate asc;\n"
            + "q = limit q 12;"
        ),
        "s_reengage": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by HasTouchFlag == "true";\n'
            + "q = filter q by DaysSinceLastTouch > 14;\n"
            + "q = foreach q generate Company, LeadName, OwnerName, Persona, "
            + f"{product_focus} as ProductFocus, {product_focus_source} as ProductSource, "
            + "SourceGroup, Campaign, NextBestAction, SuggestedTool, "
            + "LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 12;"
        ),
        "s_stage3_queue": sq(
            fy26_opportunity_detail
            + "q = filter q by (PendingStage3ReviewCount > 0) || (OpportunityHandoffCount > 0) || (DiscoveryHandoffCount > 0);\n"
            + "q = filter q by OpenOpportunityCount > 0;\n"
            + "q = foreach q generate "
            + "Company, OwnerName, SourcedOpportunityName, SourcedOpportunityStage, "
            + f"{opp_product_focus} as ProductFocus, {opp_product_focus_source} as ProductSource, "
            + "HandoffQualityBand, Stage2To3Days, SourcedARR as KnownAttributedARR, "
            + "((case when PendingStage3ReviewCount > 0 then 100 else 0 end) + "
            + '(case when HandoffQualityBand == "Discovery" then 35 else 0 end) + '
            + "(case when Stage2To3Days > 0 then Stage2To3Days else 0 end) + "
            + "(case when SourcedARR > 0 then 50 else 0 end) + "
            + "(case when SourcedARR >= 250000 then 25 when SourcedARR >= 100000 then 10 else 0 end)) as HandoffRank, "
            + "NextBestAction, SuggestedTool, SourcedOpportunityId;\n"
            + "q = order q by HandoffRank desc;\n"
            + "q = limit q 12;"
        ),
        "s_target_queue": sq(
            account_universe
            + 'q = filter q by TargetAccountFlag == "true";\n'
            + "q = filter q by (ContactCount > 0) || (OpenOpportunityCount > 0) || (OpportunityHandoffCount > 0);\n"
            + "q = foreach q generate "
            + "MatchedAccountName, ContextAccountName, ClientBaseClass, "
            + f"{product_focus} as ProductFocus, {product_focus_source} as ProductSource, "
            + "MatchedAccountTier, MatchedAccountSegment, OwnerName, "
            + "NextBestAction, SuggestedTool, DaysSinceLastTouch, ContextAccountId, "
            + "((PersonaContactCount * 5) + (OpenOpportunityCount * 10) + (OpportunityHandoffCount * 20) + "
            + '(case when ClientBaseClass == "Former Client" then 15 when ClientBaseClass == "Current Client" then 5 else 0 end) + '
            + "(case when DaysSinceLastTouch > 30 then 10 when DaysSinceLastTouch > 14 then 5 else 0 end)) as AccountQueueRank, "
            + "PersonaContactCount, ContactCount, OpenOpportunityCount, OpportunityHandoffCount;\n"
            + "q = order q by AccountQueueRank desc;\n"
            + "q = limit q 12;"
        ),
    }


def _manager_widgets() -> dict[str, dict]:
    widgets = {
        "p1_hdr": hdr(
            "North America BDR Manager",
            "Account-based operating view for prospect coverage, persona penetration, Stage 2 creation, and Stage 3 handoff rhythm in AMERS.",
        ),
        "p1_f_team": pillbox("f_team", "BDR Team"),
        "p1_f_owner": pillbox("f_owner", "Owner"),
        "p1_f_source": pillbox("f_source", "Source Group"),
        "p1_n_prospect": num(
            "s_summary",
            "prospect_accounts",
            "Prospect Accounts",
            "#8E030F",
            compact=True,
            tier="primary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_former": num(
            "s_summary",
            "former_client_accounts",
            "Former Client Accounts",
            "#5F2C83",
            compact=True,
            tier="primary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_persona": num(
            "s_summary",
            "persona_contacts",
            "Persona Contacts",
            "#032D60",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_open_opp": num(
            "s_summary",
            "open_opportunities",
            "Open Opportunities",
            "#0176D3",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_stage2": num(
            "s_summary",
            "discovery_handoffs",
            "Stage 2 Discovery",
            "#2E844A",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_stage3": num(
            "s_summary",
            "stage3_handoffs",
            "Stage 3 Handoffs",
            "#032D60",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_b_sla": flat_gauge(
            "s_sla_bullet",
            "Actual",
            "24h Direct Lead Touch SLA (Target: 100%)",
            bands=[
                {"start": 0, "stop": 50, "color": "#D4504C"},
                {"start": 50, "stop": 80, "color": "#FFB75D"},
                {"start": 80, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p1_b_integrity": flat_gauge(
            "s_integrity_bullet",
            "Actual",
            "Associated Prospect Response <24h (Target: 80%)",
            bands=[
                {"start": 0, "stop": 40, "color": "#D4504C"},
                {"start": 40, "stop": 80, "color": "#FFB75D"},
                {"start": 80, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p1_b_source": flat_gauge(
            "s_source_bullet",
            "Actual",
            "Lead Source Present (Target: 100%)",
            bands=[
                {"start": 0, "stop": 70, "color": "#D4504C"},
                {"start": 70, "stop": 90, "color": "#FFB75D"},
                {"start": 90, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p1_ch_story": rich_chart(
            "s_story",
            "line",
            "Weekly Leads, Meetings & Handoffs — FY2026",
            ["WeekStartDate"],
            ["LeadCreatedCount", "MeetingHeldCount", "OpportunityHandoffCount"],
            show_legend=True,
            axis_title="Count",
        ),
        "p1_tbl_account_mix": rich_chart(
            "s_account_mix",
            "comparisontable",
            "Account Universe by Client Base",
            ["ClientBaseClass"],
            [
                "AccountCount",
                "ContactCount",
                "PersonaContactCount",
                "OpenOpportunityCount",
                "DiscoveryHandoffCount",
                "OpportunityHandoffCount",
            ],
            show_legend=False,
        ),
        "p1_tbl_yoy": rich_chart(
            "s_yoy_rhythm",
            "comparisontable",
            "FY2025 vs FY2026 Rhythm",
            ["FiscalYear"],
            [
                "LeadCreatedCount",
                "MeetingHeldCount",
                "OpportunityHandoffCount",
                "KnownAttributedARR",
            ],
            show_legend=False,
        ),
        "p1_tbl_rep": rich_chart(
            "s_rep_table",
            "comparisontable",
            "Rep Account Universe, Contact Coverage & Handoff Load",
            ["OwnerName", "BDRTeam"],
            [
                "AccountCount",
                "ProspectAccountCount",
                "FormerClientAccountCount",
                "CurrentClientAccountCount",
                "ContactCount",
                "PersonaContactCount",
                "OpenOpportunityCount",
                "DiscoveryHandoffCount",
                "AccountStage3Count",
                "PendingStage3ReviewCount",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "PendingStage3ReviewCount",
                    "rules": [
                        {"value": 3, "color": "#D4504C", "operator": "gte"},
                        {"value": 1, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p2_hdr": hdr(
            "Rep Cadence",
            "Coach BDR execution rhythm: lead load, lead-linked cadence, and whether prospecting work is being logged on leads versus contacts or accounts.",
        ),
        "p2_f_team": pillbox("f_team", "BDR Team"),
        "p2_f_owner": pillbox("f_owner", "Owner"),
        "p2_f_source": pillbox("f_source", "Source Group"),
        "p2_ch_activity": rich_chart(
            "s_weekly_activity",
            "line",
            "Weekly Calls, Emails & Meetings by Team",
            ["WeekStartDate"],
            ["CallCount", "EmailCount", "MeetingHeldCount"],
            show_legend=True,
            axis_title="Count",
        ),
        "p2_ch_activity_mix": rich_chart(
            "s_rep_activity_mix",
            "hbar",
            "Activity Volume by Rep — Calls, Emails, Meetings",
            ["OwnerName"],
            ["CallCount", "EmailCount", "MeetingHeldCount"],
            show_legend=True,
            axis_title="Count",
            show_values=True,
        ),
        "p2_tbl_exec": rich_chart(
            "s_rep_execution",
            "comparisontable",
            "Rep Activity, Meeting Speed & Handoff Scorecard",
            ["OwnerName", "BDRTeam", "BDRRole"],
            [
                "LeadCreatedCount",
                "CallCount",
                "EmailCount",
                "ResponseCount",
                "ResponseRatePct",
                "MeetingHeldCount",
                "AvgDaysToFirstMeeting",
                "QualifiedCount",
                "KnownAttributedARR",
                "LeadToMeetingPct",
                "LeadToOppPct",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "AvgDaysToFirstMeeting",
                    "rules": [
                        {"value": 14, "color": "#D4504C", "operator": "gte"},
                        {"value": 7, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "LeadToMeetingPct",
                    "rules": [
                        {"value": 15, "color": "#04844B", "operator": "gte"},
                        {"value": 8, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p2_tbl_coach": rich_chart(
            "s_rep_handoff",
            "comparisontable",
            "Rep Stage 2 -> 3 Handoff Scorecard",
            ["OwnerName", "ManagerName", "BDRTeam"],
            [
                "Stage2DiscoveryCount",
                "Stage3EngagementCount",
                "PendingStage3ReviewCount",
                "Stage3ApprovedCount",
                "AvgStage2To3Days",
                "KnownAttributedARR",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "AvgStage2To3Days",
                    "rules": [
                        {"value": 90, "color": "#D4504C", "operator": "gte"},
                        {"value": 45, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p2_tbl_integrity": rich_chart(
            "s_rep_integrity",
            "comparisontable",
            "Prospect Activity Logging Integrity",
            ["OwnerName", "ManagerName", "BDRRole"],
            [
                "LeadCount",
                "DirectLeadTouch24hPct",
                "AssociatedTouch24hPct",
                "LeadLinkedActivityPct",
                "ContactLinkedActivityPct",
                "AccountLinkedActivityPct",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DirectLeadTouch24hPct",
                    "rules": [
                        {"value": 80, "color": "#04844B", "operator": "gte"},
                        {"value": 50, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "LeadLinkedActivityPct",
                    "rules": [
                        {"value": 70, "color": "#04844B", "operator": "gte"},
                        {"value": 40, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p2_tbl_mix": rich_chart(
            "s_outreach_mix",
            "comparisontable",
            "Client vs Prospect Outreach Mix by BDR",
            ["OwnerName"],
            [
                "ProspectActivityCount",
                "CurrentClientActivityCount",
                "FormerClientActivityCount",
                "PartnerActivityCount",
                "UnclassifiedActivityCount",
            ],
            show_legend=False,
        ),
        "p3_hdr": hdr(
            "Campaign & Product",
            "Which campaigns, products, and source motions are producing the right response quality, meetings, and opportunity handoffs in North America.",
        ),
        "p3_f_team": pillbox("f_team", "BDR Team"),
        "p3_f_owner": pillbox("f_owner", "Owner"),
        "p3_f_source": pillbox("f_source", "Source Group"),
        "p3_ch_product": rich_chart(
            "s_campaign_product",
            "hbar",
            "Responses & Handoffs by Campaign Product",
            ["CampaignProduct"],
            ["ResponseCount", "MeetingHeldCount", "QualifiedCount"],
            show_legend=True,
            axis_title="Count",
            show_values=True,
        ),
        "p3_ch_week": rich_chart(
            "s_pipeline_monthly",
            "line",
            "Monthly Pipeline & Handoffs — BDR Sourced",
            ["MonthStartDate"],
            [
                "OpenOpportunityCount",
                "DiscoveryHandoffCount",
                "OpportunityHandoffCount",
            ],
            show_legend=True,
            axis_title="Count",
        ),
        "p3_tbl_source": rich_chart(
            "s_source_quality",
            "comparisontable",
            "Source Quality, Response & Lifecycle Mix",
            ["SourceGroup"],
            [
                "LeadCount",
                "OpenMQLLeadCount",
                "OpenSQLLeadCount",
                "MarketingDisqualifiedLeadCount",
                "SalesDisqualifiedLeadCount",
                "ResponderLeadCount",
                "ResponseRatePct",
                "MeetingHeldLeadCount",
                "QualifiedCount",
                "KnownAttributedARR",
                "LeadToMeetingPct",
                "LeadToOppPct",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ResponseRatePct",
                    "rules": [
                        {"value": 20, "color": "#04844B", "operator": "gte"},
                        {"value": 10, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "LeadToMeetingPct",
                    "rules": [
                        {"value": 10, "color": "#04844B", "operator": "gte"},
                        {"value": 5, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p3_tbl_campaign": rich_chart(
            "s_campaign_quality",
            "comparisontable",
            "Campaign Response, Mix, Conversion & Known ARR",
            ["Campaign", "CampaignProduct", "CampaignScopeType", "SourceGroup"],
            [
                "LeadCount",
                "MQLLeadCount",
                "SQLLeadCount",
                "MarketingDisqualifiedLeadCount",
                "SalesDisqualifiedLeadCount",
                "ResponseCount",
                "ResponseRatePct",
                "MeetingHeldCount",
                "QualifiedCount",
                "KnownAttributedARR",
                "LeadToMeetingPct",
                "LeadToOppPct",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ResponseRatePct",
                    "rules": [
                        {"value": 20, "color": "#04844B", "operator": "gte"},
                        {"value": 10, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "LeadToMeetingPct",
                    "rules": [
                        {"value": 10, "color": "#04844B", "operator": "gte"},
                        {"value": 5, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p3_tbl_source_product": rich_chart(
            "s_source_product",
            "comparisontable",
            "Source x Targeted Product Conversion",
            ["SourceGroup", "ProductFocus", "ProductSource"],
            [
                "LeadCount",
                "ResponseCount",
                "MeetingHeldCount",
                "QualifiedCount",
                "LeadToMeetingPct",
                "LeadToOppPct",
            ],
            show_legend=False,
        ),
        "p4s_hdr": hdr(
            "Role, Industry & Product",
            "Separate true GTM product pockets from missing product signal and raw opportunity product truth so North America BDR targeting stays honest.",
        ),
        "p4s_f_team": pillbox("f_team", "BDR Team"),
        "p4s_f_owner": pillbox("f_owner", "Owner"),
        "p4s_f_source": pillbox("f_source", "Source Group"),
        "p4s_n_known_product": num(
            "s_product_signal_summary",
            "KnownProductAccounts",
            "Accounts With Product Signal",
            "#2E844A",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p4s_n_missing_product": num(
            "s_product_signal_summary",
            "MissingProductAccounts",
            "Accounts Missing Product Signal",
            "#BA0517",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p4s_n_known_pct": num(
            "s_product_signal_summary",
            "KnownProductCoveragePct",
            "Known Product Coverage %",
            "#032D60",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p4s_tbl_coverage": rich_chart(
            "s_product_signal_coverage",
            "comparisontable",
            "Product Signal Coverage & Confidence by Industry",
            ["Industry"],
            [
                "TotalAccounts",
                "HighConfidenceAccounts",
                "MediumConfidenceAccounts",
                "LowConfidenceAccounts",
                "MissingProductAccounts",
                "KnownProductCoveragePct",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "KnownProductCoveragePct",
                    "rules": [
                        {"value": 70, "color": "#04844B", "operator": "gte"},
                        {"value": 40, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4s_ch_target_heatmap": heatmap_chart(
            "s_target_product_heatmap",
            "Industry × Product Signal Coverage",
            show_legend=True,
        ),
        "p4s_ch_heatmap": heatmap_chart(
            "s_industry_product_heatmap",
            "Industry × Opportunity Product Mix",
            show_legend=True,
        ),
        "p4s_tbl_persona": rich_chart(
            "s_persona_product",
            "comparisontable",
            "Persona x Targeted Product Coverage, Activity & Handoff",
            ["Persona", "ProductFocus", "ProductSignalConfidence", "ProductSource"],
            [
                "AccountCount",
                "ContactCount",
                "ResponseRatePct",
                "MeetingHeldCount",
                "OpportunityHandoffCount",
                "ContactToOppPct",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ResponseRatePct",
                    "rules": [
                        {"value": 20, "color": "#04844B", "operator": "gte"},
                        {"value": 10, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "ContactToOppPct",
                    "rules": [
                        {"value": 5, "color": "#04844B", "operator": "gte"},
                        {"value": 2, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4s_tbl_industry": rich_chart(
            "s_industry_outreach",
            "comparisontable",
            "Industry Outreach, Meetings & Handoffs",
            ["Industry"],
            [
                "AccountCount",
                "ContactCount",
                "ContactTouchCount",
                "ActiveCoveragePct",
                "MeetingHeldCount",
                "OpenOpportunityCount",
                "OpportunityHandoffCount",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ActiveCoveragePct",
                    "rules": [
                        {"value": 50, "color": "#04844B", "operator": "gte"},
                        {"value": 25, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4s_tbl_segment": rich_chart(
            "s_segment_success",
            "comparisontable",
            "Target Segment & Tier Coverage",
            ["MatchedAccountTier", "MatchedAccountSegment"],
            [
                "AccountCount",
                "ContactCount",
                "PersonaContactCount",
                "OpenOpportunityCount",
                "OpportunityHandoffCount",
            ],
            show_legend=False,
        ),
        "p4s_tbl_stage": rich_chart(
            "s_open_opp_product",
            "comparisontable",
            "Open BDR Opportunity Mix by Opportunity Product & ARR",
            ["ProductFocus", "ProductSource"],
            [
                "OpenOpportunityCount",
                "DiscoveryHandoffCount",
                "OpportunityHandoffCount",
                "KnownAttributedARR",
            ],
            show_legend=False,
        ),
        "p4_hdr": hdr(
            "Handoff & Action Center",
            "What the North America BDR manager should act on now: priority leads, campaign responders, Stage 2 -> 3 handoffs, named-account activation, and re-engagement plays. Marketing- and sales-disqualified leads are excluded from workload queues.",
        ),
        "p4_f_team": pillbox("f_team", "BDR Team"),
        "p4_f_owner": pillbox("f_owner", "Owner"),
        "p4_f_source": pillbox("f_source", "Source Group"),
        "p4_tbl_priority": rich_chart(
            "s_prioritized",
            "comparisontable",
            "Manager Weekly Priorities",
            [
                "Company",
                "LeadName",
                "OwnerName",
                "Persona",
                "ProductFocus",
                "ProductSource",
                "Campaign",
                "NextBestAction",
                "SuggestedTool",
                "PriorityBand",
            ],
            ["QueueRank", "PriorityScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 14, "color": "#D4504C", "operator": "gte"},
                        {"value": 7, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4_tbl_response": rich_chart(
            "s_response_queue",
            "comparisontable",
            "Campaign Responders Awaiting Action",
            [
                "Company",
                "LeadName",
                "OwnerName",
                "Campaign",
                "Persona",
                "ProductFocus",
                "ProductSource",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 14, "color": "#D4504C", "operator": "gte"},
                        {"value": 7, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4_tbl_upcoming": rich_chart(
            "s_stage3_queue",
            "comparisontable",
            "High Potential Stage 2 -> 3 Handoff Queue",
            [
                "Company",
                "OwnerName",
                "SourcedOpportunityName",
                "SourcedOpportunityStage",
                "ProductFocus",
                "ProductSource",
                "HandoffQualityBand",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["Stage2To3Days", "KnownAttributedARR"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "Stage2To3Days",
                    "rules": [
                        {"value": 90, "color": "#D4504C", "operator": "gte"},
                        {"value": 45, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4_tbl_target": rich_chart(
            "s_target_queue",
            "comparisontable",
            "Named Account Activation Queue",
            [
                "MatchedAccountName",
                "ContextAccountName",
                "ClientBaseClass",
                "ProductFocus",
                "ProductSource",
                "MatchedAccountTier",
                "MatchedAccountSegment",
                "OwnerName",
                "NextBestAction",
                "SuggestedTool",
            ],
            [
                "AccountQueueRank",
                "DaysSinceLastTouch",
                "PersonaContactCount",
                "ContactCount",
                "OpenOpportunityCount",
                "OpportunityHandoffCount",
            ],
            show_legend=False,
        ),
        "p4_tbl_reengage": rich_chart(
            "s_reengage",
            "comparisontable",
            "Cold / Stale Re-engagement",
            [
                "Company",
                "LeadName",
                "OwnerName",
                "Persona",
                "ProductFocus",
                "ProductSource",
                "SourceGroup",
                "Campaign",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "gte"},
                        {"value": 14, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
    }
    manager_pages = [
        ("overview", "NA Rhythm"),
        ("productivity", "Rep Cadence"),
        ("source", "Campaign & Product"),
        ("segments", "Persona / Industry / Product"),
        ("queue", "Handoff & Action Center"),
    ]
    for page_idx in range(5):
        for nav_idx, (page_name, label) in enumerate(manager_pages):
            widgets[f"p{page_idx + 1}_nav{nav_idx + 1}"] = nav_link(
                page_name, label, active=(page_idx == nav_idx)
            )

    # -- Consulting-grade section labels --
    widgets["p1_sec_rhythm"] = section_label("FY Rhythm & Account Universe")
    widgets["p2_sec_activity"] = section_label("Activity Trends")
    widgets["p2_sec_scorecard"] = section_label("Rep Execution Scorecard")
    widgets["p3_sec_charts"] = section_label("Campaign & Product Trends")
    widgets["p3_sec_tables"] = section_label("Source & Campaign Quality")
    widgets["p4s_sec_diagnostics"] = section_label("Industry & Segment Diagnostics")
    widgets["p4_sec_queues"] = section_label("Manager Action Queues")

    add_table_action(widgets["p4_tbl_priority"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p4_tbl_response"], "salesforceActions", "Lead", "LeadId")
    add_table_action(
        widgets["p4_tbl_upcoming"],
        "salesforceActions",
        "Opportunity",
        "SourcedOpportunityId",
    )
    add_table_action(
        widgets["p4_tbl_target"], "salesforceActions", "Account", "ContextAccountId"
    )
    add_table_action(widgets["p4_tbl_reengage"], "salesforceActions", "Lead", "LeadId")
    return widgets


def _manager_layout() -> dict:
    def shift(items: list[dict], delta: int = 1) -> list[dict]:
        return [{**item, "row": item["row"] + delta} for item in items]

    p1 = nav_row("p1", 5) + shift(
        [
            {"name": "p1_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
            {"name": "p1_f_team", "row": 2, "column": 0, "colspan": 4, "rowspan": 2},
            {"name": "p1_f_owner", "row": 2, "column": 4, "colspan": 4, "rowspan": 2},
            {"name": "p1_f_source", "row": 2, "column": 8, "colspan": 4, "rowspan": 2},
            {
                "name": "p1_n_prospect",
                "row": 4,
                "column": 0,
                "colspan": 2,
                "rowspan": 4,
            },
            {"name": "p1_n_former", "row": 4, "column": 2, "colspan": 2, "rowspan": 4},
            {"name": "p1_n_persona", "row": 4, "column": 4, "colspan": 2, "rowspan": 4},
            {
                "name": "p1_n_open_opp",
                "row": 4,
                "column": 6,
                "colspan": 2,
                "rowspan": 4,
            },
            {"name": "p1_n_stage2", "row": 4, "column": 8, "colspan": 2, "rowspan": 4},
            {"name": "p1_n_stage3", "row": 4, "column": 10, "colspan": 2, "rowspan": 4},
            {"name": "p1_b_sla", "row": 8, "column": 0, "colspan": 4, "rowspan": 4},
            {
                "name": "p1_b_integrity",
                "row": 8,
                "column": 4,
                "colspan": 4,
                "rowspan": 4,
            },
            {"name": "p1_b_source", "row": 8, "column": 8, "colspan": 4, "rowspan": 4},
            {
                "name": "p1_sec_rhythm",
                "row": 12,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {"name": "p1_ch_story", "row": 13, "column": 0, "colspan": 6, "rowspan": 7},
            {
                "name": "p1_tbl_account_mix",
                "row": 13,
                "column": 6,
                "colspan": 3,
                "rowspan": 7,
            },
            {"name": "p1_tbl_yoy", "row": 13, "column": 9, "colspan": 3, "rowspan": 7},
            {"name": "p1_tbl_rep", "row": 20, "column": 0, "colspan": 12, "rowspan": 7},
        ]
    )

    p2 = nav_row("p2", 5) + shift(
        [
            {"name": "p2_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
            {"name": "p2_f_team", "row": 2, "column": 0, "colspan": 4, "rowspan": 2},
            {"name": "p2_f_owner", "row": 2, "column": 4, "colspan": 4, "rowspan": 2},
            {"name": "p2_f_source", "row": 2, "column": 8, "colspan": 4, "rowspan": 2},
            {
                "name": "p2_sec_activity",
                "row": 4,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p2_ch_activity",
                "row": 5,
                "column": 0,
                "colspan": 8,
                "rowspan": 7,
            },
            {
                "name": "p2_ch_activity_mix",
                "row": 5,
                "column": 8,
                "colspan": 4,
                "rowspan": 7,
            },
            {
                "name": "p2_sec_scorecard",
                "row": 12,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {"name": "p2_tbl_exec", "row": 13, "column": 0, "colspan": 4, "rowspan": 7},
            {
                "name": "p2_tbl_coach",
                "row": 13,
                "column": 4,
                "colspan": 4,
                "rowspan": 7,
            },
            {
                "name": "p2_tbl_integrity",
                "row": 13,
                "column": 8,
                "colspan": 4,
                "rowspan": 7,
            },
            {"name": "p2_tbl_mix", "row": 20, "column": 0, "colspan": 12, "rowspan": 6},
        ]
    )

    p3 = nav_row("p3", 5) + shift(
        [
            {"name": "p3_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
            {"name": "p3_f_team", "row": 2, "column": 0, "colspan": 4, "rowspan": 2},
            {"name": "p3_f_owner", "row": 2, "column": 4, "colspan": 4, "rowspan": 2},
            {"name": "p3_f_source", "row": 2, "column": 8, "colspan": 4, "rowspan": 2},
            {
                "name": "p3_sec_charts",
                "row": 4,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p3_ch_product",
                "row": 5,
                "column": 0,
                "colspan": 4,
                "rowspan": 7,
            },
            {"name": "p3_ch_week", "row": 5, "column": 4, "colspan": 8, "rowspan": 7},
            {
                "name": "p3_sec_tables",
                "row": 12,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p3_tbl_source",
                "row": 13,
                "column": 0,
                "colspan": 4,
                "rowspan": 7,
            },
            {
                "name": "p3_tbl_campaign",
                "row": 13,
                "column": 4,
                "colspan": 8,
                "rowspan": 7,
            },
            {
                "name": "p3_tbl_source_product",
                "row": 20,
                "column": 0,
                "colspan": 12,
                "rowspan": 6,
            },
        ]
    )

    p4 = nav_row("p4", 5) + shift(
        [
            {"name": "p4s_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
            {"name": "p4s_f_team", "row": 2, "column": 0, "colspan": 4, "rowspan": 2},
            {"name": "p4s_f_owner", "row": 2, "column": 4, "colspan": 4, "rowspan": 2},
            {"name": "p4s_f_source", "row": 2, "column": 8, "colspan": 4, "rowspan": 2},
            {
                "name": "p4s_n_known_product",
                "row": 4,
                "column": 0,
                "colspan": 2,
                "rowspan": 4,
            },
            {
                "name": "p4s_n_missing_product",
                "row": 4,
                "column": 2,
                "colspan": 2,
                "rowspan": 4,
            },
            {
                "name": "p4s_n_known_pct",
                "row": 4,
                "column": 4,
                "colspan": 2,
                "rowspan": 4,
            },
            {
                "name": "p4s_tbl_coverage",
                "row": 4,
                "column": 6,
                "colspan": 6,
                "rowspan": 4,
            },
            {
                "name": "p4s_ch_target_heatmap",
                "row": 8,
                "column": 0,
                "colspan": 6,
                "rowspan": 7,
            },
            {
                "name": "p4s_ch_heatmap",
                "row": 8,
                "column": 6,
                "colspan": 6,
                "rowspan": 7,
            },
            {
                "name": "p4s_sec_diagnostics",
                "row": 15,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p4s_tbl_persona",
                "row": 16,
                "column": 0,
                "colspan": 6,
                "rowspan": 6,
            },
            {
                "name": "p4s_tbl_industry",
                "row": 16,
                "column": 6,
                "colspan": 6,
                "rowspan": 6,
            },
            {
                "name": "p4s_tbl_segment",
                "row": 22,
                "column": 0,
                "colspan": 6,
                "rowspan": 6,
            },
            {
                "name": "p4s_tbl_stage",
                "row": 22,
                "column": 6,
                "colspan": 6,
                "rowspan": 6,
            },
        ]
    )

    p5 = nav_row("p5", 5) + shift(
        [
            {"name": "p4_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
            {"name": "p4_f_team", "row": 2, "column": 0, "colspan": 4, "rowspan": 2},
            {"name": "p4_f_owner", "row": 2, "column": 4, "colspan": 4, "rowspan": 2},
            {"name": "p4_f_source", "row": 2, "column": 8, "colspan": 4, "rowspan": 2},
            {
                "name": "p4_sec_queues",
                "row": 4,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p4_tbl_priority",
                "row": 5,
                "column": 0,
                "colspan": 12,
                "rowspan": 7,
            },
            {
                "name": "p4_tbl_response",
                "row": 12,
                "column": 0,
                "colspan": 6,
                "rowspan": 6,
            },
            {
                "name": "p4_tbl_upcoming",
                "row": 12,
                "column": 6,
                "colspan": 6,
                "rowspan": 6,
            },
            {
                "name": "p4_tbl_target",
                "row": 18,
                "column": 0,
                "colspan": 6,
                "rowspan": 6,
            },
            {
                "name": "p4_tbl_reengage",
                "row": 18,
                "column": 6,
                "colspan": 6,
                "rowspan": 6,
            },
        ]
    )

    return {
        "name": "BDRManager",
        "numColumns": 12,
        "pages": [
            pg("overview", "NA Rhythm", p1),
            pg("productivity", "Rep Cadence", p2),
            pg("source", "Campaign & Product", p3),
            pg("segments", "Persona, Industry & Product", p4),
            pg("queue", "Handoff & Action Center", p5),
        ],
    }


def _rep_steps(ds_id: str) -> dict[str, dict]:
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_team = coalesce_filter("f_team", "BDRTeam")
    filter_owner = coalesce_filter("f_owner", "OwnerName")

    lead_detail = (
        load
        + 'q = filter q by RecordType == "lead_detail";\n'
        + filter_team
        + filter_owner
    )
    detail = (
        load
        + 'q = filter q by (RecordType == "lead_detail") || (RecordType == "opportunity_detail");\n'
        + filter_team
        + filter_owner
    )
    opportunity_detail = (
        load
        + 'q = filter q by RecordType == "opportunity_detail";\n'
        + filter_team
        + filter_owner
    )
    contact_coverage = (
        load
        + 'q = filter q by RecordType == "contact_coverage";\n'
        + filter_team
        + filter_owner
    )
    account_persona_product_target = (
        load
        + 'q = filter q by RecordType == "account_persona_product_target";\n'
        + filter_team
        + filter_owner
    )
    rep_week = (
        load
        + 'q = filter q by RecordType == "rep_week";\n'
        + filter_team
        + filter_owner
    )
    owner_week = (
        load
        + 'q = filter q by RecordType == "owner_week";\n'
        + filter_team
        + filter_owner
    )
    campaign = (
        load
        + 'q = filter q by RecordType == "campaign_summary";\n'
        + filter_team
        + filter_owner
    )
    integrity = (
        load
        + 'q = filter q by RecordType == "owner_integrity";\n'
        + filter_team
        + filter_owner
    )
    fy26_rep_week = (
        rep_week
        + f'q = filter q by WeekStartDate >= "{FY2026_START}" && WeekStartDate < "{FY2027_START}";\n'
    )
    fy25_26_rep_week = (
        rep_week
        + f'q = filter q by WeekStartDate >= "{FY2025_START}" && WeekStartDate < "{FY2027_START}";\n'
    )
    product_focus = (
        '(case when CampaignProduct != "" then CampaignProduct '
        'when SourcedProductFamily != "" then SourcedProductFamily '
        'when ContextProductOpportunity != "" then ContextProductOpportunity '
        'when ContextProductMainline != "" then ContextProductMainline '
        'else "Unknown" end)'
    )
    role_focus = (
        '(case when ContactOfficialTitle != "" then ContactOfficialTitle '
        'when LeadTitle != "" then LeadTitle '
        'when Persona != "" then Persona '
        'else "Unknown" end)'
    )

    return {
        "f_team": af("BDRTeam", ds_meta, select_mode="single", start='["AMERS"]'),
        "f_owner": af("OwnerName", ds_meta),
        "s_summary": sq(
            detail
            + "q = foreach q generate "
            + "OpenLeadCount as OpenLeadCount, "
            + "OpenMQLLeadCount as OpenMQLLeadCount, "
            + "OpenSQLLeadCount as OpenSQLLeadCount, "
            + '(case when OpenLeadCount > 0 && HasResponseFlag == "true" then 1 else 0 end) as ResponderQueueCount, '
            + "UpcomingMeetingCount as UpcomingMeetingCount, "
            + '(case when OpenLeadCount > 0 && DaysSinceLastTouch > 14 && HasTouchFlag == "true" then 1 else 0 end) as ReengageCount, '
            + '(case when OpenLeadCount > 0 && TargetAccountFlag == "true" then 1 else 0 end) as TargetLeadCount;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(OpenLeadCount) as open_leads, "
            + "sum(OpenMQLLeadCount) as open_mql_leads, "
            + "sum(OpenSQLLeadCount) as open_sql_leads, "
            + "sum(ResponderQueueCount) as responder_queue_count, "
            + "sum(UpcomingMeetingCount) as upcoming_meetings, "
            + "sum(ReengageCount) as reengage_count, "
            + "sum(TargetLeadCount) as target_lead_count;"
        ),
        "s_yoy_rhythm": sq(
            fy25_26_rep_week
            + "q = foreach q generate "
            + f'(case when WeekStartDate >= "{FY2026_START}" then "FY2026" else "FY2025" end) as FiscalYear, '
            + "LeadCreatedCount as LeadCreatedCount, "
            + "MeetingHeldCount as MeetingHeldCount, "
            + "QualifiedCount as QualifiedCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + "q = group q by FiscalYear;\n"
            + "q = foreach q generate FiscalYear, "
            + "sum(LeadCreatedCount) as LeadCreatedCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR;\n"
            + "q = order q by FiscalYear asc;"
        ),
        "s_priority": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + "q = foreach q generate "
            + "Company, LeadName, ClientBaseClass, Persona, "
            + f"{product_focus} as ProductFocus, "
            + "Campaign, PriorityBand, NextBestAction, SuggestedTool, LeadId, "
            + "(PriorityScore + "
            + '(case when HasResponseFlag == "true" then 25 else 0 end) + '
            + '(case when NextMeetingDate != "" then 15 else 0 end) + '
            + '(case when TargetAccountFlag == "true" then 10 else 0 end) + '
            + '(case when ClientBaseClass == "Former Client" then 10 when ClientBaseClass == "Current Client" then 5 else 0 end) + '
            + '(case when DaysSinceLastTouch > 14 && HasTouchFlag == "true" then 10 else 0 end)) as QueueRank, '
            + "LeadScore, DaysSinceLastTouch;\n"
            + "q = order q by QueueRank desc;\n"
            + "q = limit q 15;"
        ),
        "s_priority_mix": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q1 = filter q by HasResponseFlag == "true";\n'
            + "q1 = group q1 by all;\n"
            + 'q1 = foreach q1 generate "Responders" as WorkBucket, count() as LeadCount;\n'
            + 'q2 = filter q by NextMeetingDate != "";\n'
            + "q2 = group q2 by all;\n"
            + 'q2 = foreach q2 generate "Meeting Prep / Follow-up" as WorkBucket, count() as LeadCount;\n'
            + "q3 = filter q by OpenSQLLeadCount > 0;\n"
            + "q3 = group q3 by all;\n"
            + 'q3 = foreach q3 generate "SQL / Hot Leads" as WorkBucket, count() as LeadCount;\n'
            + 'q4 = filter q by HasTouchFlag == "true" && DaysSinceLastTouch > 14;\n'
            + "q4 = group q4 by all;\n"
            + 'q4 = foreach q4 generate "Stale Re-engage" as WorkBucket, count() as LeadCount;\n'
            + 'q5 = filter q by TargetAccountFlag == "true";\n'
            + "q5 = group q5 by all;\n"
            + 'q5 = foreach q5 generate "Target Accounts" as WorkBucket, count() as LeadCount;\n'
            + "q = union q1, q2, q3, q4, q5;\n"
            + "q = order q by LeadCount desc;"
        ),
        "s_sla_bar": sq(
            integrity
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + '"Direct Lead Touch <24h" as MetricLabel, '
            + "case when sum(LeadCount) > 0 then (sum(DirectLeadTouch24hCount) * 100) / sum(LeadCount) else 0 end as Actual, "
            + "100 as Target;"
        ),
        "s_assoc_bar": sq(
            integrity
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + '"Associated Prospect Response <24h" as MetricLabel, '
            + "case when sum(LeadCount) > 0 then (sum(AssociatedTouch24hCount) * 100) / sum(LeadCount) else 0 end as Actual, "
            + "80 as Target;"
        ),
        "s_lead_link_bar": sq(
            integrity
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + '"Lead-Linked Activity" as MetricLabel, '
            + "case when sum(TotalActivityCount) > 0 then (sum(LeadLinkedActivityCount) * 100) / sum(TotalActivityCount) else 0 end as Actual, "
            + "50 as Target;"
        ),
        "s_weekly_rhythm": sq(
            fy26_rep_week
            + "q = group q by WeekStartDate;\n"
            + "q = foreach q generate WeekStartDate, "
            + "sum(TouchCount) as TouchCount, "
            + "sum(MeetingBookedCount) as MeetingBookedCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(OpportunityLinkedActivityCount) as OpportunityLinkedActivityCount;\n"
            + "q = order q by WeekStartDate asc;"
        ),
        "s_upcoming": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by NextMeetingDate != "";\n'
            + "q = foreach q generate "
            + "Company, LeadName, Persona, "
            + f"{product_focus} as ProductFocus, "
            + "Campaign, NextMeetingDate, NextBestAction, SuggestedTool, LeadScore, LeadId;\n"
            + "q = order q by NextMeetingDate asc;\n"
            + "q = limit q 12;"
        ),
        "s_response": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by HasResponseFlag == "true";\n'
            + "q = foreach q generate "
            + "Company, LeadName, Campaign, CampaignProduct, Persona, "
            + f"{product_focus} as ProductFocus, "
            + "NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by DaysSinceLastTouch desc;\n"
            + "q = limit q 12;"
        ),
        "s_sla": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by (SLABreachFlag == "true") || (HasTouchFlag == "true" && DaysSinceLastTouch > 14);\n'
            + "q = foreach q generate "
            + "Company, LeadName, ClientBaseClass, Persona, "
            + f"{product_focus} as ProductFocus, "
            + "NextBestAction, SuggestedTool, LeadScore, DaysToFirstTouch, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by DaysSinceLastTouch desc;\n"
            + "q = limit q 12;"
        ),
        "s_integrity": sq(
            integrity
            + "q = group q by (OwnerName, ManagerName, BDRRole);\n"
            + "q = foreach q generate "
            + "OwnerName, ManagerName, BDRRole, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(DirectLeadTouch24hCount) as DirectLeadTouch24hCount, "
            + "sum(DirectLeadTouchAnyCount) as DirectLeadTouchAnyCount, "
            + "sum(AssociatedTouch24hCount) as AssociatedTouch24hCount, "
            + "sum(AssociatedTouchAnyCount) as AssociatedTouchAnyCount, "
            + "sum(TotalActivityCount) as TotalActivityCount, "
            + "sum(LeadLinkedActivityCount) as LeadLinkedActivityCount, "
            + "sum(ContactLinkedActivityCount) as ContactLinkedActivityCount, "
            + "sum(AccountLinkedActivityCount) as AccountLinkedActivityCount, "
            + "sum(OpportunityLinkedActivityCount) as OpportunityLinkedActivityCount, "
            + "sum(ProspectActivityCount) as ProspectActivityCount, "
            + "sum(CurrentClientActivityCount) as CurrentClientActivityCount, "
            + "sum(FormerClientActivityCount) as FormerClientActivityCount, "
            + "case when sum(LeadCount) > 0 then (sum(DirectLeadTouch24hCount) * 100) / sum(LeadCount) else 0 end as DirectLeadTouch24hPct, "
            + "case when sum(LeadCount) > 0 then (sum(DirectLeadTouchAnyCount) * 100) / sum(LeadCount) else 0 end as DirectLeadTouchAnyPct, "
            + "case when sum(LeadCount) > 0 then (sum(AssociatedTouch24hCount) * 100) / sum(LeadCount) else 0 end as AssociatedTouch24hPct, "
            + "case when sum(LeadCount) > 0 then (sum(AssociatedTouchAnyCount) * 100) / sum(LeadCount) else 0 end as AssociatedTouchAnyPct, "
            + "case when sum(TotalActivityCount) > 0 then (sum(LeadLinkedActivityCount) * 100) / sum(TotalActivityCount) else 0 end as LeadLinkedActivityPct, "
            + "case when sum(TotalActivityCount) > 0 then (sum(ContactLinkedActivityCount) * 100) / sum(TotalActivityCount) else 0 end as ContactLinkedActivityPct, "
            + "case when sum(TotalActivityCount) > 0 then (sum(AccountLinkedActivityCount) * 100) / sum(TotalActivityCount) else 0 end as AccountLinkedActivityPct;\n"
            + "q = limit q 5;"
        ),
        "s_source_quality": sq(
            detail
            + "q = foreach q generate "
            + "SourceGroup, LeadCount as LeadCount, OpenMQLLeadCount as OpenMQLLeadCount, OpenSQLLeadCount as OpenSQLLeadCount, "
            + "MarketingDisqualifiedLeadCount as MarketingDisqualifiedLeadCount, SalesDisqualifiedLeadCount as SalesDisqualifiedLeadCount, MeetingHeldLeadCount as MeetingHeldLeadCount, QualifiedCount as QualifiedCount, SourcedARR as KnownAttributedARR, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponderLeadCount;\n'
            + "q = group q by SourceGroup;\n"
            + "q = foreach q generate "
            + "SourceGroup, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(OpenMQLLeadCount) as OpenMQLLeadCount, "
            + "sum(OpenSQLLeadCount) as OpenSQLLeadCount, "
            + "sum(MarketingDisqualifiedLeadCount) as MarketingDisqualifiedLeadCount, "
            + "sum(SalesDisqualifiedLeadCount) as SalesDisqualifiedLeadCount, "
            + "sum(ResponderLeadCount) as ResponderLeadCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldLeadCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(ResponderLeadCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_campaign_product": sq(
            campaign
            + 'q = filter q by CampaignProduct != "";\n'
            + "q = group q by CampaignProduct;\n"
            + "q = foreach q generate "
            + "CampaignProduct, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_campaign_quality": sq(
            campaign
            + 'q = filter q by Campaign != "";\n'
            + "q = group q by (Campaign, CampaignType, CampaignProduct, CampaignScopeType);\n"
            + "q = foreach q generate Campaign, CampaignType, CampaignProduct, CampaignScopeType, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(MQLLeadCount) as MQLLeadCount, "
            + "sum(SQLLeadCount) as SQLLeadCount, "
            + "sum(MarketingDisqualifiedLeadCount) as MarketingDisqualifiedLeadCount, "
            + "sum(SalesDisqualifiedLeadCount) as SalesDisqualifiedLeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(SourcedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(ResponseCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct;\n"
            + "q = order q by ResponseCount desc;\n"
            + "q = limit q 20;"
        ),
        "s_role_industry": sq(
            detail
            + "q = foreach q generate "
            + f"{role_focus} as RoleFocus, Industry, "
            + "LeadCount as LeadCount, QualifiedCount as QualifiedCount;\n"
            + 'q = filter q by RoleFocus != "Unknown";\n'
            + 'q = filter q by Industry != "";\n'
            + "q = group q by (RoleFocus, Industry);\n"
            + "q = foreach q generate "
            + "RoleFocus, Industry, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by LeadToOppPct desc;\n"
            + "q = limit q 60;"
        ),
        "s_persona_product": sq(
            account_persona_product_target
            + "q = foreach q generate "
            + '(case when Persona != "" then Persona else "Unknown Persona" end) as Persona, '
            + "TargetedProduct as ProductFocus, "
            + "1 as AccountCount, "
            + "PersonaContactCount as ContactCount, "
            + "ContactTouchCount as ResponseCount, "
            + "MeetingHeldCount as MeetingHeldCount, "
            + "OpportunityHandoffCount as QualifiedCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + 'q = filter q by Persona != "";\n'
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (Persona, ProductFocus);\n"
            + "q = foreach q generate "
            + "Persona, ProductFocus, "
            + "sum(AccountCount) as AccountCount, "
            + "sum(ContactCount) as ContactCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(ContactCount) > 0 then (sum(ResponseCount) * 100) / sum(ContactCount) else 0 end as ResponseRatePct, "
            + "case when sum(ContactCount) > 0 then (sum(QualifiedCount) * 100) / sum(ContactCount) else 0 end as ContactToOppPct, "
            + "case when sum(AccountCount) > 0 then (sum(QualifiedCount) * 100) / sum(AccountCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_client_product_mix": sq(
            detail
            + "q = foreach q generate "
            + "ClientBaseClass, "
            + f"{product_focus} as ProductFocus, "
            + "LeadCount as LeadCount, "
            + "ContactCount as ContactCount, "
            + "QualifiedCount as QualifiedCount;\n"
            + 'q = filter q by ClientBaseClass != "";\n'
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (ClientBaseClass, ProductFocus);\n"
            + "q = foreach q generate "
            + "ClientBaseClass, ProductFocus, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_source_product": sq(
            detail
            + "q = foreach q generate "
            + f"SourceGroup, {product_focus} as ProductFocus, "
            + "LeadCount as LeadCount, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponseCount, '
            + "ContactCount as ContactCount, "
            + "QualifiedCount as QualifiedCount;\n"
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (SourceGroup, ProductFocus);\n"
            + "q = foreach q generate "
            + "SourceGroup, ProductFocus, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "case when sum(LeadCount) > 0 then (sum(MeetingHeldCount) * 100) / sum(LeadCount) else 0 end as LeadToMeetingPct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_target_summary": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + "q = foreach q generate "
            + "LeadCount as LeadCount, "
            + '(case when TargetAccountFlag == "true" then 1 else 0 end) as TargetLeadCount, '
            + '(case when TargetAccountFlag == "true" && MatchedAccountTier == "Tier 1" then 1 else 0 end) as Tier1Count, '
            + '(case when ((TargetAccountFlag == "true") || (ClientBaseClass == "Former Client")) && DaysSinceLastTouch > 14 && HasTouchFlag == "true" then 1 else 0 end) as StrategicReengageCount;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(TargetLeadCount) as matched_targets, "
            + "sum(Tier1Count) as tier1_targets, "
            + "sum(StrategicReengageCount) as target_reengage;"
        ),
        "s_handoff_summary": sq(
            opportunity_detail
            + "q = filter q by (PendingStage3ReviewCount > 0) || (OpportunityHandoffCount > 0) || (DiscoveryHandoffCount > 0);\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(OpportunityHandoffCount) as handoff_queue_count, "
            + "sum(PendingStage3ReviewCount) as pending_stage3_review;"
        ),
        "s_target_accounts": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by TargetAccountFlag == "true";\n'
            + "q = foreach q generate "
            + "MatchedAccountName, MatchedAccountTier, MatchedAccountSegment, MatchedAccountIndustry, Company, LeadName, ClientBaseClass, Persona, "
            + f"{product_focus} as ProductFocus, "
            + "NextBestAction, SuggestedTool, LeadId, "
            + "(PriorityScore + "
            + '(case when HasResponseFlag == "true" then 20 else 0 end) + '
            + '(case when DaysSinceLastTouch > 14 && HasTouchFlag == "true" then 15 else 0 end)) as TargetQueueRank, '
            + "LeadScore, DaysSinceLastTouch;\n"
            + "q = order q by TargetQueueRank desc;\n"
            + "q = limit q 15;"
        ),
        "s_target_segment": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by TargetAccountFlag == "true";\n'
            + "q = foreach q generate "
            + "MatchedAccountTier, MatchedAccountSegment, LeadCount as LeadCount, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponderLeadCount, '
            + "QualifiedCount as QualifiedCount;\n"
            + "q = group q by (MatchedAccountTier, MatchedAccountSegment);\n"
            + "q = foreach q generate "
            + "MatchedAccountTier, MatchedAccountSegment, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponderLeadCount) as ResponderLeadCount, "
            + "sum(QualifiedCount) as QualifiedCount;\n"
            + "q = order q by LeadCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_target_reengage": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by (TargetAccountFlag == "true") || (ClientBaseClass == "Former Client");\n'
            + 'q = filter q by DaysSinceLastTouch > 14 && HasTouchFlag == "true";\n'
            + "q = foreach q generate "
            + "MatchedAccountName, Company, LeadName, ClientBaseClass, Persona, "
            + f"{product_focus} as ProductFocus, "
            + "NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 12;"
        ),
        "s_stage3_queue": sq(
            opportunity_detail
            + "q = filter q by (PendingStage3ReviewCount > 0) || (OpportunityHandoffCount > 0) || (DiscoveryHandoffCount > 0);\n"
            + "q = foreach q generate "
            + "Company, SourcedOpportunityName, SourcedOpportunityStage, "
            + f"{product_focus} as ProductFocus, "
            + "HandoffQualityBand, Stage2To3Days, KnownAttributedARR as KnownAttributedARR, "
            + "((case when PendingStage3ReviewCount > 0 then 100 else 0 end) + "
            + '(case when HandoffQualityBand == "Discovery" then 20 else 0 end) + '
            + "Stage2To3Days + "
            + "(case when KnownAttributedARR > 0 then 10 else 0 end)) as HandoffRank, "
            + "NextBestAction, SuggestedTool, SourcedOpportunityId;\n"
            + "q = order q by HandoffRank desc;\n"
            + "q = limit q 12;"
        ),
    }


def _rep_widgets() -> dict[str, dict]:
    widgets = {
        "p1_hdr": hdr(
            "BDR Rep Queue",
            "Start with today’s priorities, then work meetings, campaign responses, and named-account follow-up with clear next actions, tools, and product context.",
        ),
        "p1_f_team": pillbox("f_team", "BDR Team"),
        "p1_f_owner": pillbox("f_owner", "Owner"),
        "p1_n_open": num(
            "s_summary",
            "open_leads",
            "Open Leads",
            "#032D60",
            compact=True,
            tier="primary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_mql": num(
            "s_summary",
            "open_mql_leads",
            "Open MQL Leads",
            "#0176D3",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_sql": num(
            "s_summary",
            "open_sql_leads",
            "Open SQL / Hot Leads",
            "#8E030F",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_response": num(
            "s_summary",
            "responder_queue_count",
            "Responders Awaiting Action",
            "#BA0517",
            compact=True,
            tier="primary",
            widget_style=kpi_style("accent"),
        ),
        "p1_n_meetings": num(
            "s_summary",
            "upcoming_meetings",
            "Upcoming Meetings",
            "#0176D3",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_reengage": num(
            "s_summary",
            "reengage_count",
            "Re-engagement Targets",
            "#2E844A",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_tbl_yoy": rich_chart(
            "s_yoy_rhythm",
            "comparisontable",
            "FY2025 vs FY2026 Rhythm",
            ["FiscalYear"],
            [
                "LeadCreatedCount",
                "MeetingHeldCount",
                "QualifiedCount",
                "KnownAttributedARR",
            ],
            show_legend=False,
        ),
        "p1_ch_mix": rich_chart(
            "s_priority_mix",
            "hbar",
            "Today's Workload — Leads by Queue",
            ["WorkBucket"],
            ["LeadCount"],
            show_legend=False,
            axis_title="Lead Count",
            number_format="#,##0",
            show_values=True,
        ),
        "p1_tbl_priority": rich_chart(
            "s_priority",
            "comparisontable",
            "Today's Priorities",
            [
                "Company",
                "LeadName",
                "ClientBaseClass",
                "Persona",
                "ProductFocus",
                "Campaign",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["QueueRank", "LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 14, "color": "#D4504C", "operator": "gte"},
                        {"value": 7, "color": "#FFB75D", "operator": "gte"},
                    ],
                }
            ],
        ),
        "p2_hdr": hdr(
            "Meetings & Follow-up",
            "Work the week cleanly: prepare for meetings, catch SLA breaches and stale follow-up, and verify whether activity is being logged on the lead versus the broader prospect footprint.",
        ),
        "p2_f_team": pillbox("f_team", "BDR Team"),
        "p2_f_owner": pillbox("f_owner", "Owner"),
        "p2_b_sla": rich_chart(
            "s_sla_bar",
            "hbar",
            "Direct Lead Touch <24h vs 100% SLA",
            ["MetricLabel"],
            ["Actual", "Target"],
            show_legend=True,
            axis_title="%",
            number_format="#,##0",
            show_values=True,
            reference_lines=[
                {"value": 100, "label": "SLA: 100%", "color": "#54698D"},
            ],
        ),
        "p2_b_assoc": rich_chart(
            "s_assoc_bar",
            "hbar",
            "Associated Response <24h vs 80% Target",
            ["MetricLabel"],
            ["Actual", "Target"],
            show_legend=True,
            axis_title="%",
            number_format="#,##0",
            show_values=True,
            reference_lines=[
                {"value": 80, "label": "Target: 80%", "color": "#54698D"},
            ],
        ),
        "p2_b_leadlink": rich_chart(
            "s_lead_link_bar",
            "hbar",
            "Lead-Linked Activity vs 50% Target",
            ["MetricLabel"],
            ["Actual", "Target"],
            show_legend=True,
            axis_title="%",
            number_format="#,##0",
            show_values=True,
            reference_lines=[
                {"value": 50, "label": "Target: 50%", "color": "#54698D"},
            ],
        ),
        "p2_ch_weekly": rich_chart(
            "s_weekly_rhythm",
            "line",
            "Weekly Touches, Meetings & Handoffs",
            ["WeekStartDate"],
            ["TouchCount", "MeetingBookedCount", "MeetingHeldCount", "QualifiedCount"],
            show_legend=True,
            axis_title="Count",
        ),
        "p2_tbl_upcoming": rich_chart(
            "s_upcoming",
            "comparisontable",
            "Upcoming Meetings",
            [
                "Company",
                "LeadName",
                "Persona",
                "ProductFocus",
                "Campaign",
                "NextMeetingDate",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore"],
            show_legend=False,
        ),
        "p3_tbl_response": rich_chart(
            "s_response",
            "comparisontable",
            "Campaign Responders Awaiting Action",
            [
                "Company",
                "LeadName",
                "Campaign",
                "CampaignProduct",
                "Persona",
                "ProductFocus",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 14, "color": "#D4504C", "operator": "gte"},
                        {"value": 7, "color": "#FFB75D", "operator": "gte"},
                    ],
                }
            ],
        ),
        "p2_tbl_sla": rich_chart(
            "s_sla",
            "comparisontable",
            "SLA & Stale Leads",
            [
                "Company",
                "LeadName",
                "ClientBaseClass",
                "Persona",
                "ProductFocus",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysToFirstTouch", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysToFirstTouch",
                    "rules": [
                        {"value": 3, "color": "#D4504C", "operator": "gte"},
                        {"value": 1, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 14, "color": "#D4504C", "operator": "gte"},
                        {"value": 7, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p2_tbl_integrity": rich_chart(
            "s_integrity",
            "comparisontable",
            "Activity Logging & Response Integrity",
            ["OwnerName", "ManagerName", "BDRRole"],
            [
                "LeadCount",
                "DirectLeadTouch24hPct",
                "AssociatedTouch24hPct",
                "LeadLinkedActivityPct",
                "ContactLinkedActivityPct",
                "AccountLinkedActivityPct",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DirectLeadTouch24hPct",
                    "rules": [
                        {"value": 80, "color": "#04844B", "operator": "gte"},
                        {"value": 50, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "LeadLinkedActivityPct",
                    "rules": [
                        {"value": 70, "color": "#04844B", "operator": "gte"},
                        {"value": 40, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p3_hdr": hdr(
            "Campaign Responders",
            "Work campaign follow-up with enough GTM context to know which personas, industries, and products are producing meetings and real handoffs in North America.",
        ),
        "p3_f_team": pillbox("f_team", "BDR Team"),
        "p3_f_owner": pillbox("f_owner", "Owner"),
        "p3_ch_product": rich_chart(
            "s_campaign_product",
            "hbar",
            "Responses & Handoffs by Campaign Product",
            ["CampaignProduct"],
            ["ResponseCount", "MeetingHeldCount", "QualifiedCount"],
            show_legend=True,
            axis_title="Count",
            show_values=True,
        ),
        "p3_ch_heatmap": heatmap_chart(
            "s_role_industry",
            "Role × Industry — Opportunity Rate",
            show_legend=True,
        ),
        "p3_tbl_source": rich_chart(
            "s_campaign_quality",
            "comparisontable",
            "Campaign Response, Mix & Conversion",
            ["Campaign", "CampaignProduct", "CampaignScopeType"],
            [
                "LeadCount",
                "MQLLeadCount",
                "SQLLeadCount",
                "ResponseCount",
                "MeetingHeldCount",
                "QualifiedCount",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ResponseRatePct",
                    "rules": [
                        {"value": 20, "color": "#04844B", "operator": "gte"},
                        {"value": 10, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p3_tbl_campaign": rich_chart(
            "s_persona_product",
            "comparisontable",
            "Persona x Product Coverage & Handoff",
            ["Persona", "ProductFocus"],
            [
                "AccountCount",
                "ContactCount",
                "ResponseRatePct",
                "MeetingHeldCount",
                "QualifiedCount",
                "LeadToOppPct",
                "KnownAttributedARR",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ResponseRatePct",
                    "rules": [
                        {"value": 20, "color": "#04844B", "operator": "gte"},
                        {"value": 10, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p3_tbl_source_product": rich_chart(
            "s_source_product",
            "comparisontable",
            "Source x Product Performance",
            ["SourceGroup", "ProductFocus"],
            [
                "LeadCount",
                "ResponseCount",
                "MeetingHeldCount",
                "QualifiedCount",
                "LeadToMeetingPct",
                "LeadToOppPct",
            ],
            show_legend=False,
        ),
        "p3_tbl_client_product": rich_chart(
            "s_client_product_mix",
            "comparisontable",
            "Client / Prospect x Product Mix",
            ["ClientBaseClass", "ProductFocus"],
            ["LeadCount", "MeetingHeldCount", "QualifiedCount", "LeadToOppPct"],
            show_legend=False,
        ),
        "p4_hdr": hdr(
            "Target Accounts & Handoffs",
            "Use matched-account context, former-client signals, and Stage 2 -> 3 handoffs to work the right accounts and move the right opportunities forward.",
        ),
        "p4_f_team": pillbox("f_team", "BDR Team"),
        "p4_f_owner": pillbox("f_owner", "Owner"),
        "p4_n_target": num(
            "s_target_summary",
            "matched_targets",
            "Matched Targets",
            "#032D60",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p4_n_tier1": num(
            "s_target_summary",
            "tier1_targets",
            "Tier 1 Targets",
            "#0176D3",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p4_n_reengage": num(
            "s_target_summary",
            "target_reengage",
            "Strategic Re-engage",
            "#BA0517",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p4_n_handoff": num(
            "s_handoff_summary",
            "handoff_queue_count",
            "Stage 2 -> 3 Handoffs",
            "#5F2C83",
            compact=True,
            tier="primary",
            widget_style=kpi_style("accent"),
        ),
        "p4_tbl_target": rich_chart(
            "s_target_accounts",
            "comparisontable",
            "Named Account Targets",
            [
                "MatchedAccountName",
                "MatchedAccountTier",
                "MatchedAccountSegment",
                "LeadName",
                "ClientBaseClass",
                "Persona",
                "ProductFocus",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["TargetQueueRank", "LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
        ),
        "p4_tbl_segment": rich_chart(
            "s_target_segment",
            "comparisontable",
            "Target Segment Coverage",
            ["MatchedAccountTier", "MatchedAccountSegment"],
            ["LeadCount", "ResponderLeadCount", "QualifiedCount"],
            show_legend=False,
        ),
        "p4_tbl_handoff": rich_chart(
            "s_stage3_queue",
            "comparisontable",
            "Stage 2 -> 3 Handoff Queue",
            [
                "Company",
                "SourcedOpportunityName",
                "SourcedOpportunityStage",
                "ProductFocus",
                "HandoffQualityBand",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["Stage2To3Days", "KnownAttributedARR"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "Stage2To3Days",
                    "rules": [
                        {"value": 90, "color": "#D4504C", "operator": "gte"},
                        {"value": 45, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4_tbl_reengage": rich_chart(
            "s_target_reengage",
            "comparisontable",
            "Former Client & Target Re-engagement",
            [
                "MatchedAccountName",
                "Company",
                "LeadName",
                "ClientBaseClass",
                "Persona",
                "ProductFocus",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
        ),
        "p1_sec_mix": section_label("Today's Work Queue"),
        "p2_sec_sla": section_label("SLA & Logging Integrity"),
        "p2_sec_tables": section_label("Follow-up & Activity Audit"),
        "p3_sec_campaign": section_label("Campaign Performance"),
        "p4_sec_target": section_label("Target Accounts & Handoffs"),
    }
    add_table_action(widgets["p1_tbl_priority"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p2_tbl_upcoming"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p3_tbl_response"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p2_tbl_sla"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p4_tbl_target"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p4_tbl_reengage"], "salesforceActions", "Lead", "LeadId")
    add_table_action(
        widgets["p4_tbl_handoff"],
        "salesforceActions",
        "Opportunity",
        "SourcedOpportunityId",
    )
    return widgets


def _rep_layout() -> dict:
    p1 = [
        {"name": "p1_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_team", "row": 2, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p1_f_owner", "row": 2, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p1_n_open", "row": 4, "column": 0, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_mql", "row": 4, "column": 2, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_sql", "row": 4, "column": 4, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_response", "row": 4, "column": 6, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_meetings", "row": 4, "column": 8, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_reengage", "row": 4, "column": 10, "colspan": 2, "rowspan": 4},
        {"name": "p1_sec_mix", "row": 8, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_tbl_yoy", "row": 9, "column": 0, "colspan": 3, "rowspan": 7},
        {"name": "p1_ch_mix", "row": 9, "column": 3, "colspan": 3, "rowspan": 7},
        {"name": "p1_tbl_priority", "row": 9, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p2 = [
        {"name": "p2_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_team", "row": 2, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p2_f_owner", "row": 2, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p2_sec_sla", "row": 4, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_b_sla", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p2_b_assoc", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p2_b_leadlink", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p2_ch_weekly", "row": 9, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p2_sec_tables", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_tbl_upcoming", "row": 17, "column": 0, "colspan": 7, "rowspan": 6},
        {"name": "p2_tbl_sla", "row": 17, "column": 7, "colspan": 5, "rowspan": 6},
        {
            "name": "p2_tbl_integrity",
            "row": 23,
            "column": 0,
            "colspan": 12,
            "rowspan": 6,
        },
    ]

    p3 = [
        {"name": "p3_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_team", "row": 2, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p3_f_owner", "row": 2, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p3_tbl_response", "row": 4, "column": 0, "colspan": 12, "rowspan": 6},
        {"name": "p3_ch_product", "row": 10, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p3_ch_heatmap", "row": 10, "column": 4, "colspan": 8, "rowspan": 6},
        {
            "name": "p3_sec_campaign",
            "row": 16,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p3_tbl_source", "row": 17, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_tbl_campaign", "row": 17, "column": 6, "colspan": 6, "rowspan": 7},
        {
            "name": "p3_tbl_client_product",
            "row": 24,
            "column": 0,
            "colspan": 6,
            "rowspan": 6,
        },
        {
            "name": "p3_tbl_source_product",
            "row": 24,
            "column": 6,
            "colspan": 6,
            "rowspan": 6,
        },
    ]

    p4 = [
        {"name": "p4_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_team", "row": 2, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p4_f_owner", "row": 2, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p4_n_target", "row": 4, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_tier1", "row": 4, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_reengage", "row": 4, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p4_n_handoff", "row": 4, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p4_sec_target", "row": 8, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_tbl_target", "row": 9, "column": 0, "colspan": 8, "rowspan": 7},
        {"name": "p4_tbl_segment", "row": 9, "column": 8, "colspan": 4, "rowspan": 7},
        {"name": "p4_tbl_handoff", "row": 16, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p4_tbl_reengage", "row": 16, "column": 6, "colspan": 6, "rowspan": 6},
    ]

    return {
        "name": "BDRRepQueue",
        "numColumns": 12,
        "pages": [
            pg("queue", "My Day", p1),
            pg("meetings", "Meetings & Follow-up", p2),
            pg("responders", "Campaign Responders", p3),
            pg("targets", "Target Accounts & Handoffs", p4),
        ],
    }


def _control_steps(ds_id: str) -> dict[str, dict]:
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_team = coalesce_filter("f_team", "BDRTeam")
    filter_owner = coalesce_filter("f_owner", "OwnerName")
    filter_source = coalesce_filter("f_source", "SourceGroup")
    filter_year = coalesce_filter("f_year", "YearLabel")
    rep_week = (
        load
        + 'q = filter q by RecordType == "rep_week";\n'
        + filter_team
        + filter_owner
        + filter_source
        + filter_year
    )

    lead_detail = (
        load
        + 'q = filter q by RecordType == "lead_detail";\n'
        + filter_team
        + filter_owner
        + filter_source
        + filter_year
    )
    detail = (
        load
        + 'q = filter q by (RecordType == "lead_detail") || (RecordType == "opportunity_detail");\n'
        + filter_team
        + filter_owner
        + filter_source
        + filter_year
    )
    opportunity_detail = (
        load
        + 'q = filter q by RecordType == "opportunity_detail";\n'
        + filter_team
        + filter_owner
        + filter_source
        + filter_year
    )
    campaign = (
        load
        + 'q = filter q by RecordType == "campaign_summary";\n'
        + filter_team
        + filter_owner
        + filter_source
        + filter_year
    )
    account_persona_product_target = (
        load
        + 'q = filter q by RecordType == "account_persona_product_target";\n'
        + filter_team
        + filter_owner
        + filter_source
        + filter_year
    )

    product_focus = (
        '(case when CampaignProduct != "" then CampaignProduct '
        'when ContextProductOpportunity != "" then ContextProductOpportunity '
        'when ContextProductMainline != "" then ContextProductMainline '
        'else "Unknown" end)'
    )
    role_focus = (
        '(case when ContactOfficialTitle != "" then ContactOfficialTitle '
        'when LeadTitle != "" then LeadTitle '
        'when Persona != "" then Persona '
        'else "Unknown" end)'
    )

    steps = {
        "f_team": af("BDRTeam", ds_meta, select_mode="single", start='["AMERS"]'),
        "f_owner": af("OwnerName", ds_meta),
        "f_source": af("SourceGroup", ds_meta),
        "f_year": af("YearLabel", ds_meta, select_mode="single", start='["FY2026"]'),
        "s_summary": sq(
            lead_detail
            + 'q = filter q by Campaign != "";\n'
            + "q = foreach q generate "
            + "LeadCount as LeadCount, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponseCount, '
            + "MeetingHeldLeadCount as MeetingHeldLeadCount, "
            + "QualifiedCount as QualifiedCount, "
            + "SourcedARR as SourcedARR;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(LeadCount) as campaign_leads, "
            + "sum(ResponseCount) as campaign_responders, "
            + "sum(MeetingHeldLeadCount) as meetings_held, "
            + "sum(QualifiedCount) as opportunity_handoffs, "
            + "sum(SourcedARR) as known_attributed_arr;"
        ),
        "s_weekly_rhythm": sq(
            rep_week
            + "q = group q by WeekStartDate;\n"
            + "q = foreach q generate "
            + "WeekStartDate, "
            + "sum(LeadCreatedCount) as LeadCreatedCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount;\n"
            + "q = order q by WeekStartDate asc;\n"
            + "q = limit q 26;"
        ),
        "s_monthly_rhythm": sq(
            'q1 = load "BDR_Operating_Rhythm";\n'
            + 'q1 = filter q1 by RecordType == "lead_detail";\n'
            + 'q1 = filter q1 by {{coalesce(column(f_team.selection, ["BDRTeam"]), column(f_team.result, ["BDRTeam"])).asEquality(\'BDRTeam\')}};\n'
            + 'q1 = filter q1 by {{coalesce(column(f_owner.selection, ["OwnerName"]), column(f_owner.result, ["OwnerName"])).asEquality(\'OwnerName\')}};\n'
            + 'q1 = filter q1 by {{coalesce(column(f_source.selection, ["SourceGroup"]), column(f_source.result, ["SourceGroup"])).asEquality(\'SourceGroup\')}};\n'
            + 'q1 = filter q1 by {{coalesce(column(f_year.selection, ["YearLabel"]), column(f_year.result, ["YearLabel"])).asEquality(\'YearLabel\')}};\n'
            + "q1 = group q1 by MonthStartDate;\n"
            + "q1 = foreach q1 generate "
            + "MonthStartDate, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldCount;\n"
            + 'q2 = load "BDR_Operating_Rhythm";\n'
            + 'q2 = filter q2 by RecordType == "opportunity_detail";\n'
            + 'q2 = filter q2 by {{coalesce(column(f_team.selection, ["BDRTeam"]), column(f_team.result, ["BDRTeam"])).asEquality(\'BDRTeam\')}};\n'
            + 'q2 = filter q2 by {{coalesce(column(f_owner.selection, ["OwnerName"]), column(f_owner.result, ["OwnerName"])).asEquality(\'OwnerName\')}};\n'
            + 'q2 = filter q2 by {{coalesce(column(f_source.selection, ["SourceGroup"]), column(f_source.result, ["SourceGroup"])).asEquality(\'SourceGroup\')}};\n'
            + 'q2 = filter q2 by {{coalesce(column(f_year.selection, ["YearLabel"]), column(f_year.result, ["YearLabel"])).asEquality(\'YearLabel\')}};\n'
            + "q2 = group q2 by MonthStartDate;\n"
            + "q2 = foreach q2 generate "
            + "MonthStartDate, "
            + "sum(OpportunityHandoffCount) as OpportunityHandoffCount;\n"
            + "q = cogroup q1 by MonthStartDate full, q2 by MonthStartDate;\n"
            + "q = foreach q generate "
            + "coalesce(q1.MonthStartDate, q2.MonthStartDate) as MonthStartDate, "
            + "coalesce(sum(q1.LeadCount), 0) as LeadCount, "
            + "coalesce(sum(q1.ResponseCount), 0) as ResponseCount, "
            + "coalesce(sum(q1.MeetingHeldCount), 0) as MeetingHeldCount, "
            + "coalesce(sum(q2.OpportunityHandoffCount), 0) as OpportunityHandoffCount;\n"
            + "q = order q by MonthStartDate asc;\n"
            + "q = limit q 18;"
        ),
        "s_campaign_product": sq(
            campaign
            + 'q = filter q by CampaignProduct != "";\n'
            + "q = group q by CampaignProduct;\n"
            + "q = foreach q generate "
            + "CampaignProduct, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 12;"
        ),
        "s_campaign_quality": sq(
            campaign
            + 'q = filter q by Campaign != "";\n'
            + "q = group q by (Campaign, CampaignProduct, CampaignScopeType, SourceGroup);\n"
            + "q = foreach q generate "
            + "Campaign, CampaignProduct, CampaignScopeType, SourceGroup, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(MQLLeadCount) as MQLLeadCount, "
            + "sum(SQLLeadCount) as SQLLeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(SourcedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(ResponseCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct, "
            + "case when sum(LeadCount) > 0 then (sum(MeetingHeldCount) * 100) / sum(LeadCount) else 0 end as LeadToMeetingPct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 20;"
        ),
        "s_source_product": sq(
            detail
            + "q = foreach q generate "
            + f"SourceGroup, {product_focus} as ProductFocus, "
            + "LeadCount as LeadCount, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponseCount, '
            + "MeetingHeldLeadCount as MeetingHeldLeadCount, "
            + "QualifiedCount as QualifiedCount;\n"
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (SourceGroup, ProductFocus);\n"
            + "q = foreach q generate "
            + "SourceGroup, ProductFocus, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldLeadCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "case when sum(LeadCount) > 0 then (sum(MeetingHeldLeadCount) * 100) / sum(LeadCount) else 0 end as LeadToMeetingPct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_monthly_product_engagement": sq(
            detail
            + "q = foreach q generate "
            + "MonthStartDate, "
            + f"{product_focus} as ProductFocus, "
            + "PersonaContactCount as PersonaContactCount, "
            + "QualifiedCount as QualifiedCount;\n"
            + "q = filter q by MonthStartDate is not null;\n"
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (ProductFocus, MonthStartDate);\n"
            + "q = foreach q generate "
            + "ProductFocus, "
            + "MonthStartDate, "
            + "(sum(MeetingHeldCount) + sum(QualifiedCount)) as EngagementScore;\n"
            + "q = order q by MonthStartDate asc;\n"
            + "q = limit q 120;"
        ),
        "s_role_industry": sq(
            detail
            + "q = foreach q generate "
            + f"{role_focus} as RoleFocus, Industry, "
            + "LeadCount as LeadCount, QualifiedCount as QualifiedCount;\n"
            + 'q = filter q by RoleFocus != "Unknown";\n'
            + 'q = filter q by Industry != "";\n'
            + "q = group q by (RoleFocus, Industry);\n"
            + "q = foreach q generate "
            + "RoleFocus, Industry, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by LeadToOppPct desc;\n"
            + "q = limit q 60;"
        ),
        "s_persona_product": sq(
            account_persona_product_target
            + "q = foreach q generate "
            + '(case when Persona != "" then Persona else "Unknown Persona" end) as Persona, '
            + "TargetedProduct as ProductFocus, "
            + "1 as AccountCount, "
            + "PersonaContactCount as ContactCount, "
            + "ContactTouchCount as ResponseCount, "
            + "MeetingHeldCount as MeetingHeldCount, "
            + "OpportunityHandoffCount as QualifiedCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + 'q = filter q by Persona != "";\n'
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (Persona, ProductFocus);\n"
            + "q = foreach q generate "
            + "Persona, ProductFocus, "
            + "sum(AccountCount) as AccountCount, "
            + "sum(ContactCount) as ContactCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(ContactCount) > 0 then (sum(ResponseCount) * 100) / sum(ContactCount) else 0 end as ResponseRatePct, "
            + "case when sum(AccountCount) > 0 then (sum(QualifiedCount) * 100) / sum(AccountCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_owner_success": sq(
            detail
            + "q = foreach q generate "
            + "OwnerName, Industry, "
            + "LeadCount as LeadCount, "
            + "MeetingHeldLeadCount as MeetingHeldLeadCount, "
            + "QualifiedCount as QualifiedCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + 'q = filter q by Industry != "";\n'
            + "q = group q by (OwnerName, Industry);\n"
            + "q = foreach q generate "
            + "OwnerName, Industry, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_client_product_mix": sq(
            detail
            + "q = foreach q generate "
            + "ClientBaseClass, "
            + f"{product_focus} as ProductFocus, "
            + "LeadCount as LeadCount, QualifiedCount as QualifiedCount, MeetingHeldLeadCount as MeetingHeldLeadCount;\n"
            + 'q = filter q by ClientBaseClass != "";\n'
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (ClientBaseClass, ProductFocus);\n"
            + "q = foreach q generate "
            + "ClientBaseClass, ProductFocus, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(MeetingHeldLeadCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 18;"
        ),
        "s_cohort_summary": sq(
            detail
            + "q = foreach q generate "
            + '(case when OpenLeadCount > 0 && ClientBaseClass == "Former Client" && FormerClientAgeBand == "2+ Years" then 1 else 0 end) as FormerClient2YOpenCount, '
            + '(case when OpenLeadCount > 0 && TelemarketingStatus == "Hand-back from Telemarketing" then 1 else 0 end) as TMHandbackOpenCount, '
            + '(case when OpenLeadCount > 0 && HasTouchFlag == "false" then 1 else 0 end) as UntouchedOpenCount, '
            + '(case when OpenLeadCount > 0 && HasTouchFlag == "true" && DaysSinceLastTouch > 30 then 1 else 0 end) as ColdOpenCount;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(FormerClient2YOpenCount) as former_client_2y_open, "
            + "sum(TMHandbackOpenCount) as tm_handback_open, "
            + "sum(UntouchedOpenCount) as untouched_open, "
            + "sum(ColdOpenCount) as cold_open;"
        ),
        "s_former_client": sq(
            detail
            + 'q = filter q by ClientBaseClass == "Former Client";\n'
            + 'q = filter q by FormerClientAgeBand == "2+ Years";\n'
            + "q = filter q by OpenLeadCount > 0;\n"
            + "q = foreach q generate "
            + "MatchedAccountName, Company, LeadName, Persona, "
            + f"{product_focus} as ProductFocus, "
            + "FormerClientAgeBand, FormerClientLostDate, OwnerName, NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 12;"
        ),
        "s_tm_handback": sq(
            detail
            + 'q = filter q by TelemarketingStatus == "Hand-back from Telemarketing";\n'
            + "q = filter q by OpenLeadCount > 0;\n"
            + "q = foreach q generate "
            + "MatchedAccountName, Company, LeadName, Persona, "
            + f"{product_focus} as ProductFocus, "
            + "TelemarketingStatus, OwnerName, NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 12;"
        ),
        "s_untouched": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by HasTouchFlag == "false";\n'
            + "q = foreach q generate "
            + "Company, LeadName, Persona, Industry, OwnerName, Campaign, NextBestAction, SuggestedTool, LeadScore, DaysToFirstTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 12;"
        ),
        "s_cold_reengage": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by HasTouchFlag == "true";\n'
            + "q = filter q by DaysSinceLastTouch > 30;\n"
            + "q = foreach q generate "
            + "Company, LeadName, Persona, Industry, "
            + f"{product_focus} as ProductFocus, "
            + "OwnerName, NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 12;"
        ),
        "s_responder_queue": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by HasResponseFlag == "true";\n'
            + "q = foreach q generate "
            + "Company, LeadName, Campaign, Persona, OwnerName, "
            + f"{product_focus} as ProductFocus, "
            + "NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by DaysSinceLastTouch desc;\n"
            + "q = limit q 12;"
        ),
        "s_stage3_queue": sq(
            opportunity_detail
            + "q = filter q by (PendingStage3ReviewCount > 0) || (OpportunityHandoffCount > 0) || (DiscoveryHandoffCount > 0);\n"
            + "q = foreach q generate "
            + "Company, OwnerName, SourcedOpportunityName, SourcedOpportunityStage, HandoffQualityBand, "
            + "Stage2To3Days, NextBestAction, SuggestedTool, SourcedOpportunityId;\n"
            + "q = order q by Stage2To3Days desc;\n"
            + "q = limit q 12;"
        ),
        "s_target_queue": sq(
            detail
            + 'q = filter q by TargetAccountFlag == "true";\n'
            + "q = filter q by OpenLeadCount > 0;\n"
            + "q = foreach q generate "
            + "MatchedAccountName, Company, LeadName, Persona, MatchedAccountTier, MatchedAccountSegment, OwnerName, "
            + "NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 12;"
        ),
        "s_reentry_handback": sq(
            detail
            + 'q = filter q by (ClientBaseClass == "Former Client" && FormerClientAgeBand == "2+ Years") || (TelemarketingStatus == "Hand-back from Telemarketing");\n'
            + "q = filter q by OpenLeadCount > 0;\n"
            + "q = foreach q generate "
            + "MatchedAccountName, Company, LeadName, ClientBaseClass, FormerClientAgeBand, TelemarketingStatus, Persona, "
            + f"{product_focus} as ProductFocus, "
            + "OwnerName, NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 12;"
        ),
        "s_industry_product_targets": sq(
            detail
            + "q = foreach q generate "
            + "Industry, "
            + f"{product_focus} as ProductFocus, "
            + "ClientBaseClass, "
            + "LeadCount as LeadCount, "
            + '(case when TargetAccountFlag == "true" then 1 else 0 end) as TargetAccountLeadCount, '
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponseCount, '
            + "PersonaContactCount as PersonaContactCount, "
            + "OpenOpportunityCount as OpenOpportunityCount, "
            + "OpportunityHandoffCount as OpportunityHandoffCount;\n"
            + 'q = filter q by Industry != "";\n'
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (Industry, ProductFocus, ClientBaseClass);\n"
            + "q = foreach q generate "
            + "Industry, ProductFocus, ClientBaseClass, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(TargetAccountLeadCount) as TargetAccountLeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(ResponseCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 30;"
        ),
        "s_role_industry_product": sq(
            detail
            + "q = foreach q generate "
            + f"{role_focus} as RoleFocus, "
            + "Industry, "
            + f"{product_focus} as ProductFocus, "
            + "LeadCount as LeadCount, "
            + '(case when HasResponseFlag == "true" then 1 else 0 end) as ResponseCount, '
            + "ContactCount as ContactCount, "
            + "QualifiedCount as QualifiedCount, "
            + "SourcedARR as KnownAttributedARR;\n"
            + 'q = filter q by RoleFocus != "Unknown";\n'
            + 'q = filter q by Industry != "";\n'
            + 'q = filter q by ProductFocus != "Unknown";\n'
            + "q = group q by (RoleFocus, Industry, ProductFocus);\n"
            + "q = foreach q generate "
            + "RoleFocus, Industry, ProductFocus, "
            + "sum(LeadCount) as LeadCount, "
            + "sum(ResponseCount) as ResponseCount, "
            + "sum(MeetingHeldCount) as MeetingHeldCount, "
            + "sum(QualifiedCount) as QualifiedCount, "
            + "sum(KnownAttributedARR) as KnownAttributedARR, "
            + "case when sum(LeadCount) > 0 then (sum(ResponseCount) * 100) / sum(LeadCount) else 0 end as ResponseRatePct, "
            + "case when sum(LeadCount) > 0 then (sum(QualifiedCount) * 100) / sum(LeadCount) else 0 end as LeadToOppPct;\n"
            + "q = order q by QualifiedCount desc;\n"
            + "q = limit q 30;"
        ),
        "s_named_account_targets_long": sq(
            detail
            + 'q = filter q by TargetAccountFlag == "true";\n'
            + "q = filter q by OpenLeadCount > 0;\n"
            + "q = foreach q generate "
            + "MatchedAccountName, Company, LeadName, Persona, Industry, "
            + f"{product_focus} as ProductFocus, "
            + "MatchedAccountTier, MatchedAccountSegment, ClientBaseClass, OwnerName, "
            + "NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 30;"
        ),
        "s_former_client_long": sq(
            detail
            + 'q = filter q by ClientBaseClass == "Former Client";\n'
            + 'q = filter q by FormerClientAgeBand == "2+ Years";\n'
            + "q = filter q by OpenLeadCount > 0;\n"
            + "q = foreach q generate "
            + "MatchedAccountName, Company, LeadName, Persona, Industry, "
            + f"{product_focus} as ProductFocus, "
            + "FormerClientAgeBand, FormerClientLostDate, OwnerName, NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 30;"
        ),
        "s_cold_prospect_long": sq(
            detail
            + "q = filter q by OpenLeadCount > 0;\n"
            + 'q = filter q by ClientBaseClass == "Prospect" || ClientBaseClass == "Unmatched";\n'
            + 'q = filter q by HasTouchFlag == "true";\n'
            + "q = filter q by DaysSinceLastTouch > 30;\n"
            + "q = foreach q generate "
            + "Company, LeadName, Persona, Industry, "
            + f"{product_focus} as ProductFocus, "
            + "SourceGroup, Campaign, OwnerName, NextBestAction, SuggestedTool, LeadScore, DaysSinceLastTouch, LeadId;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 30;"
        ),
    }
    for key in ("s_summary", "s_cohort_summary"):
        if key in steps:
            steps[key].update(CONTROL_KPI_FACET_SCOPE)
    return steps


def _control_widgets() -> dict[str, dict]:
    widgets = {
        "p1_hdr": hdr(
            "North America BDR Campaign & Target Control",
            "Manage campaign quality, persona and product targeting, and the GTM cohorts that should drive the next North America BDR plays.",
        ),
        "p1_f_team": pillbox("f_team", "BDR Team"),
        "p1_f_owner": pillbox("f_owner", "Owner"),
        "p1_f_source": pillbox("f_source", "Source Group"),
        "p1_f_year": pillbox("f_year", "Fiscal Year"),
        "p1_n_leads": num(
            "s_summary",
            "campaign_leads",
            "Campaign Leads",
            "#032D60",
            compact=True,
            tier="primary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_response": num(
            "s_summary",
            "campaign_responders",
            "Responders",
            "#0176D3",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_meetings": num(
            "s_summary",
            "meetings_held",
            "Meetings Held",
            "#2E844A",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_handoffs": num(
            "s_summary",
            "opportunity_handoffs",
            "Opp Handoffs",
            "#032D60",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p1_n_arr": num(
            "s_summary",
            "known_attributed_arr",
            "Known Attributed ARR",
            "#5F2C83",
            compact=True,
            tier="primary",
            widget_style=kpi_style("accent"),
        ),
        "p1_ch_weekly": line_chart(
            "s_weekly_rhythm",
            "Weekly Leads, Responses & Handoffs — Last 26 Weeks",
            axis_title="Count",
            subtitle="",
        ),
        "p1_ch_monthly": line_chart(
            "s_monthly_rhythm",
            "Monthly GTM Volume — Leads, Responses, Meetings, Handoffs",
            axis_title="Count",
            subtitle="",
        ),
        "p1_ch_product": rich_chart(
            "s_campaign_product",
            "hbar",
            "Responses & Handoffs by Campaign Product",
            ["CampaignProduct"],
            ["ResponseCount", "MeetingHeldCount", "QualifiedCount"],
            show_legend=True,
            axis_title="Count",
            show_values=True,
        ),
        "p1_tbl_campaign": rich_chart(
            "s_campaign_quality",
            "comparisontable",
            "Campaign Performance",
            ["Campaign", "CampaignProduct", "CampaignScopeType", "SourceGroup"],
            [
                "LeadCount",
                "MQLLeadCount",
                "SQLLeadCount",
                "ResponseRatePct",
                "MeetingHeldCount",
                "QualifiedCount",
                "LeadToMeetingPct",
                "LeadToOppPct",
                "KnownAttributedARR",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ResponseRatePct",
                    "rules": [
                        {"value": 20, "color": "#04844B", "operator": "gte"},
                        {"value": 10, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "LeadToMeetingPct",
                    "rules": [
                        {"value": 10, "color": "#04844B", "operator": "gte"},
                        {"value": 5, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p1_tbl_source_product": rich_chart(
            "s_source_product",
            "comparisontable",
            "Source x Product Conversion",
            ["SourceGroup", "ProductFocus"],
            [
                "LeadCount",
                "ResponseCount",
                "MeetingHeldCount",
                "QualifiedCount",
                "LeadToMeetingPct",
                "LeadToOppPct",
            ],
            show_legend=False,
        ),
        "p2_hdr": hdr(
            "Persona & Product",
            "See which roles, industries, and product motions are performing best, and which BDRs are succeeding inside those segment pockets.",
        ),
        "p2_f_team": pillbox("f_team", "BDR Team"),
        "p2_f_owner": pillbox("f_owner", "Owner"),
        "p2_f_source": pillbox("f_source", "Source Group"),
        "p2_f_year": pillbox("f_year", "Fiscal Year"),
        "p2_ch_heatmap": heatmap_chart(
            "s_role_industry",
            "Role × Industry — Opportunity Rate",
            show_legend=True,
        ),
        "p2_ch_monthly_product": heatmap_chart(
            "s_monthly_product_engagement",
            "Monthly Product Engagement Momentum",
            show_legend=True,
        ),
        "p2_tbl_persona_product": rich_chart(
            "s_persona_product",
            "comparisontable",
            "Persona x Product Coverage & Handoff",
            ["Persona", "ProductFocus"],
            [
                "AccountCount",
                "ContactCount",
                "ResponseRatePct",
                "MeetingHeldCount",
                "QualifiedCount",
                "LeadToOppPct",
                "KnownAttributedARR",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ResponseRatePct",
                    "rules": [
                        {"value": 20, "color": "#04844B", "operator": "gte"},
                        {"value": 10, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "LeadToOppPct",
                    "rules": [
                        {"value": 5, "color": "#04844B", "operator": "gte"},
                        {"value": 2, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p2_tbl_owner_success": rich_chart(
            "s_owner_success",
            "comparisontable",
            "BDR Success by Industry",
            ["OwnerName", "Industry"],
            [
                "LeadCount",
                "MeetingHeldCount",
                "QualifiedCount",
                "LeadToOppPct",
                "KnownAttributedARR",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "LeadToOppPct",
                    "rules": [
                        {"value": 5, "color": "#04844B", "operator": "gte"},
                        {"value": 2, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p2_tbl_client_product": rich_chart(
            "s_client_product_mix",
            "comparisontable",
            "Client / Prospect x Product Mix",
            ["ClientBaseClass", "ProductFocus"],
            ["LeadCount", "MeetingHeldCount", "QualifiedCount", "LeadToOppPct"],
            show_legend=False,
        ),
        "p3_hdr": hdr(
            "Cohort Plays",
            "Work the GTM cohorts that matter most now: former clients lost 2+ years ago, telemarketing hand-back accounts, untouched new leads, and colder leads ready for reactivation.",
        ),
        "p3_f_team": pillbox("f_team", "BDR Team"),
        "p3_f_owner": pillbox("f_owner", "Owner"),
        "p3_f_source": pillbox("f_source", "Source Group"),
        "p3_f_year": pillbox("f_year", "Fiscal Year"),
        "p3_n_former": num(
            "s_cohort_summary",
            "former_client_2y_open",
            "Former Client >2Y",
            "#8E030F",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p3_n_current": num(
            "s_cohort_summary",
            "tm_handback_open",
            "TM Hand-back Open",
            "#032D60",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p3_n_untouched": num(
            "s_cohort_summary",
            "untouched_open",
            "Untouched Open",
            "#BA0517",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p3_n_cold": num(
            "s_cohort_summary",
            "cold_open",
            "Cold Open",
            "#5F2C83",
            compact=True,
            tier="secondary",
            widget_style=kpi_style("card"),
        ),
        "p3_tbl_former": rich_chart(
            "s_former_client",
            "comparisontable",
            "Former Client >2Y Re-entry",
            [
                "MatchedAccountName",
                "Company",
                "LeadName",
                "Persona",
                "ProductFocus",
                "FormerClientAgeBand",
                "FormerClientLostDate",
                "OwnerName",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "gte"},
                        {"value": 14, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p3_tbl_current": rich_chart(
            "s_tm_handback",
            "comparisontable",
            "Telemarketing Hand-back",
            [
                "MatchedAccountName",
                "Company",
                "LeadName",
                "Persona",
                "ProductFocus",
                "TelemarketingStatus",
                "OwnerName",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "gte"},
                        {"value": 14, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p3_tbl_untouched": rich_chart(
            "s_untouched",
            "comparisontable",
            "Untouched New Leads",
            [
                "Company",
                "LeadName",
                "Persona",
                "Industry",
                "OwnerName",
                "Campaign",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysToFirstTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysToFirstTouch",
                    "rules": [
                        {"value": 3, "color": "#D4504C", "operator": "gte"},
                        {"value": 1, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p3_tbl_cold": rich_chart(
            "s_cold_reengage",
            "comparisontable",
            "Cold Lead Reactivation",
            [
                "Company",
                "LeadName",
                "Persona",
                "Industry",
                "ProductFocus",
                "OwnerName",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "gte"},
                        {"value": 14, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4_hdr": hdr(
            "Activation Queues",
            "Use the actionable GTM queues to drive campaign response, stage-3 handoff, and named-account activation.",
        ),
        "p4_f_team": pillbox("f_team", "BDR Team"),
        "p4_f_owner": pillbox("f_owner", "Owner"),
        "p4_f_source": pillbox("f_source", "Source Group"),
        "p4_f_year": pillbox("f_year", "Fiscal Year"),
        "p4_tbl_response": rich_chart(
            "s_responder_queue",
            "comparisontable",
            "High-Intent Campaign Responders",
            [
                "Company",
                "LeadName",
                "Campaign",
                "Persona",
                "OwnerName",
                "ProductFocus",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "gte"},
                        {"value": 14, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4_tbl_stage3": rich_chart(
            "s_stage3_queue",
            "comparisontable",
            "Stage 2 -> 3 Handoff Queue",
            [
                "Company",
                "OwnerName",
                "SourcedOpportunityName",
                "SourcedOpportunityStage",
                "HandoffQualityBand",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["Stage2To3Days"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "Stage2To3Days",
                    "rules": [
                        {"value": 90, "color": "#D4504C", "operator": "gte"},
                        {"value": 45, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p4_tbl_target": rich_chart(
            "s_target_queue",
            "comparisontable",
            "Named Account Activation Queue",
            [
                "MatchedAccountName",
                "Company",
                "LeadName",
                "Persona",
                "MatchedAccountTier",
                "MatchedAccountSegment",
                "OwnerName",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
        ),
        "p4_tbl_cold": rich_chart(
            "s_reentry_handback",
            "comparisontable",
            "Re-entry & Hand-back Queue",
            [
                "MatchedAccountName",
                "Company",
                "LeadName",
                "ClientBaseClass",
                "FormerClientAgeBand",
                "TelemarketingStatus",
                "Persona",
                "ProductFocus",
                "OwnerName",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "gte"},
                        {"value": 14, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p5_hdr": hdr(
            "Strategic Target Lists",
            "Build larger campaign and list-building motions from industry, role, and product pockets, then work named accounts, former clients, and colder prospects with intention.",
        ),
        "p5_f_team": pillbox("f_team", "BDR Team"),
        "p5_f_owner": pillbox("f_owner", "Owner"),
        "p5_f_source": pillbox("f_source", "Source Group"),
        "p5_f_year": pillbox("f_year", "Fiscal Year"),
        "p5_tbl_pockets": rich_chart(
            "s_industry_product_targets",
            "comparisontable",
            "Industry x Product Target Pockets",
            ["Industry", "ProductFocus", "ClientBaseClass"],
            [
                "LeadCount",
                "TargetAccountLeadCount",
                "ResponseRatePct",
                "MeetingHeldCount",
                "QualifiedCount",
                "LeadToOppPct",
                "KnownAttributedARR",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ResponseRatePct",
                    "rules": [
                        {"value": 20, "color": "#04844B", "operator": "gte"},
                        {"value": 10, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "LeadToOppPct",
                    "rules": [
                        {"value": 5, "color": "#04844B", "operator": "gte"},
                        {"value": 2, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p5_tbl_role_product": rich_chart(
            "s_role_industry_product",
            "comparisontable",
            "Role / Title x Industry x Product",
            ["RoleFocus", "Industry", "ProductFocus"],
            [
                "LeadCount",
                "ResponseRatePct",
                "MeetingHeldCount",
                "QualifiedCount",
                "LeadToOppPct",
                "KnownAttributedARR",
            ],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "ResponseRatePct",
                    "rules": [
                        {"value": 20, "color": "#04844B", "operator": "gte"},
                        {"value": 10, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
                {
                    "type": "threshold",
                    "field": "LeadToOppPct",
                    "rules": [
                        {"value": 5, "color": "#04844B", "operator": "gte"},
                        {"value": 2, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p5_tbl_named": rich_chart(
            "s_named_account_targets_long",
            "comparisontable",
            "Named Account Strategic Targets",
            [
                "MatchedAccountName",
                "Company",
                "LeadName",
                "Persona",
                "Industry",
                "ProductFocus",
                "MatchedAccountTier",
                "MatchedAccountSegment",
                "ClientBaseClass",
                "OwnerName",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "gte"},
                        {"value": 14, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p5_tbl_former": rich_chart(
            "s_former_client_long",
            "comparisontable",
            "Former Client Re-entry Targets",
            [
                "MatchedAccountName",
                "Company",
                "LeadName",
                "Persona",
                "Industry",
                "ProductFocus",
                "FormerClientAgeBand",
                "FormerClientLostDate",
                "OwnerName",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "gte"},
                        {"value": 14, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p5_tbl_cold": rich_chart(
            "s_cold_prospect_long",
            "comparisontable",
            "Cold Prospect Strategic Targets",
            [
                "Company",
                "LeadName",
                "Persona",
                "Industry",
                "ProductFocus",
                "SourceGroup",
                "Campaign",
                "OwnerName",
                "NextBestAction",
                "SuggestedTool",
            ],
            ["LeadScore", "DaysSinceLastTouch"],
            show_legend=False,
            format_rules=[
                {
                    "type": "threshold",
                    "field": "DaysSinceLastTouch",
                    "rules": [
                        {"value": 30, "color": "#D4504C", "operator": "gte"},
                        {"value": 14, "color": "#FFB75D", "operator": "gte"},
                    ],
                },
            ],
        ),
        "p1_sec_charts": section_label("Campaign Rhythm"),
        "p1_sec_tables": section_label("Campaign & Source Quality"),
        "p2_sec_heatmap": section_label("Persona & Product Heatmaps"),
        "p2_sec_tables": section_label("Persona & Segment Diagnostics"),
        "p3_sec_cohorts": section_label("GTM Cohort Queues"),
        "p4_sec_queues": section_label("Activation Queues"),
        "p5_sec_lists": section_label("Strategic Target Lists"),
    }
    add_table_action(widgets["p3_tbl_former"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p3_tbl_current"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p3_tbl_untouched"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p3_tbl_cold"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p4_tbl_response"], "salesforceActions", "Lead", "LeadId")
    add_table_action(
        widgets["p4_tbl_stage3"],
        "salesforceActions",
        "Opportunity",
        "SourcedOpportunityId",
    )
    add_table_action(widgets["p4_tbl_target"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p4_tbl_cold"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p5_tbl_named"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p5_tbl_former"], "salesforceActions", "Lead", "LeadId")
    add_table_action(widgets["p5_tbl_cold"], "salesforceActions", "Lead", "LeadId")
    return widgets


def _control_layout() -> dict:
    p1 = [
        {"name": "p1_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_team", "row": 2, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_owner", "row": 2, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_source", "row": 2, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_year", "row": 2, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p1_n_leads", "row": 4, "column": 0, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_response", "row": 4, "column": 2, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_meetings", "row": 4, "column": 4, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_handoffs", "row": 4, "column": 6, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_arr", "row": 4, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_sec_charts", "row": 8, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_weekly", "row": 9, "column": 0, "colspan": 6, "rowspan": 6},
        {"name": "p1_ch_monthly", "row": 9, "column": 6, "colspan": 6, "rowspan": 6},
        {"name": "p1_ch_product", "row": 15, "column": 0, "colspan": 6, "rowspan": 6},
        {
            "name": "p1_tbl_source_product",
            "row": 15,
            "column": 6,
            "colspan": 6,
            "rowspan": 6,
        },
        {"name": "p1_sec_tables", "row": 21, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p1_tbl_campaign",
            "row": 22,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
    ]

    p2 = [
        {"name": "p2_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_team", "row": 2, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_owner", "row": 2, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_source", "row": 2, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_year", "row": 2, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p2_sec_heatmap", "row": 4, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_heatmap", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {
            "name": "p2_ch_monthly_product",
            "row": 5,
            "column": 6,
            "colspan": 6,
            "rowspan": 7,
        },
        {"name": "p2_sec_tables", "row": 12, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p2_tbl_persona_product",
            "row": 13,
            "column": 0,
            "colspan": 6,
            "rowspan": 7,
        },
        {
            "name": "p2_tbl_owner_success",
            "row": 13,
            "column": 6,
            "colspan": 6,
            "rowspan": 7,
        },
        {
            "name": "p2_tbl_client_product",
            "row": 20,
            "column": 0,
            "colspan": 12,
            "rowspan": 7,
        },
    ]

    p3 = [
        {"name": "p3_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_team", "row": 2, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_owner", "row": 2, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_source", "row": 2, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_year", "row": 2, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p3_n_former", "row": 4, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p3_n_current", "row": 4, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p3_n_untouched", "row": 4, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p3_n_cold", "row": 4, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p3_sec_cohorts", "row": 8, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_tbl_former", "row": 9, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_tbl_current", "row": 9, "column": 6, "colspan": 6, "rowspan": 7},
        {
            "name": "p3_tbl_untouched",
            "row": 16,
            "column": 0,
            "colspan": 6,
            "rowspan": 7,
        },
        {"name": "p3_tbl_cold", "row": 16, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p4 = [
        {"name": "p4_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_team", "row": 2, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_owner", "row": 2, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_source", "row": 2, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_year", "row": 2, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p4_sec_queues", "row": 4, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_tbl_response", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p4_tbl_stage3", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p4_tbl_target", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p4_tbl_cold", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p5 = [
        {"name": "p5_hdr", "row": 0, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p5_f_team", "row": 2, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_owner", "row": 2, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_source", "row": 2, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_year", "row": 2, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p5_sec_lists", "row": 4, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_tbl_pockets", "row": 5, "column": 0, "colspan": 6, "rowspan": 8},
        {
            "name": "p5_tbl_role_product",
            "row": 5,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        },
        {"name": "p5_tbl_named", "row": 13, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p5_tbl_former", "row": 20, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p5_tbl_cold", "row": 20, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    return {
        "name": "BDRCampaignTargetControl",
        "numColumns": 12,
        "pages": [
            pg("campaigns", "Campaign Performance", p1),
            pg("segments", "Persona & Product", p2),
            pg("cohorts", "Cohort Plays", p3),
            pg("queues", "Activation Queues", p4),
            pg("targetlists", "Strategic Target Lists", p5),
        ],
    }


def main() -> None:
    """Build dataset and deploy both BDR dashboards."""
    inst, tok = get_auth()
    if not create_dataset(inst, tok):
        raise SystemExit("Dataset upload failed")

    ds_id = get_dataset_id(inst, tok, DS)
    if not ds_id:
        raise SystemExit(f"Could not resolve dataset id for {DS}")

    manager_state = build_dashboard_state(
        _manager_steps(ds_id),
        _manager_widgets(),
        _manager_layout(),
        bg_color="#F4F6F9",
        cell_spacing=8,
        widget_style=KPI_CARD_STYLE,
    )
    manager_id = create_dashboard_if_needed(inst, tok, MANAGER_LABEL)
    print(f"\n=== Deploying {MANAGER_LABEL} ===")
    deploy_dashboard(inst, tok, manager_id, manager_state)

    rep_state = build_dashboard_state(
        _rep_steps(ds_id),
        _rep_widgets(),
        _rep_layout(),
        bg_color="#F4F6F9",
        cell_spacing=8,
        widget_style=KPI_CARD_STYLE,
    )
    rep_id = create_dashboard_if_needed(inst, tok, REP_LABEL)
    print(f"\n=== Deploying {REP_LABEL} ===")
    deploy_dashboard(inst, tok, rep_id, rep_state)

    control_state = build_dashboard_state(
        _control_steps(ds_id),
        _control_widgets(),
        _control_layout(),
        bg_color="#F4F6F9",
        cell_spacing=8,
        widget_style=KPI_CARD_STYLE,
    )
    control_id = create_dashboard_if_needed(inst, tok, CONTROL_LABEL)
    print(f"\n=== Deploying {CONTROL_LABEL} ===")
    deploy_dashboard(inst, tok, control_id, control_state)

    set_record_links_xmd(
        inst,
        tok,
        DS,
        [
            {"field": "LeadName", "id_field": "LeadId", "label": "Lead"},
            {
                "field": "SourcedOpportunityName",
                "id_field": "SourcedOpportunityId",
                "label": "Opportunity",
            },
            {
                "field": "MatchedAccountName",
                "id_field": "ContextAccountId",
                "label": "Account",
            },
            {
                "field": "ContextAccountName",
                "id_field": "ContextAccountId",
                "label": "Account",
            },
        ],
    )


if __name__ == "__main__":
    main()
