#!/usr/bin/env python3
"""Revenue Retention & Health Dashboard Builder.

Closes the biggest metric gap: NRR, GRR, Churn Rate, Retention Cohorts.
Creates a unified dataset from Opportunity data (Land/Expand/Renewal)
with pre-computed retention metrics and a renewal risk pipeline.

Pages:
  1. Retention Summary — NRR, GRR, Churn KPIs + waterfall bridge
  2. Trend & Cohort — quarterly NRR/GRR trend + cohort analysis
  3. Renewal Pipeline — upcoming renewals ranked by risk
  4. Churn Analysis — lost renewals, reasons, account detail
"""

import csv
import io
import json
import logging
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime

sys.path.insert(0, "/Users/test/crm-analytics")
from crm_analytics_helpers import (
    KPI_CARD_STYLE,
    _dim,
    _measure,
    af,
    build_dashboard_state,
    bullet_chart,
    combo_chart,
    compare_table,
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    hdr,
    listselector,
    nav_link_external,
    num,
    pg,
    rich_chart,
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
from crm_analytics_runtime import builder_run  # pyright: ignore[reportMissingImports]
from simcorp_fields import assert_org_schema  # pyright: ignore[reportMissingImports]

logger = logging.getLogger(__name__)

DS = "Revenue_Retention_Health"
DS_LABEL = "Revenue Retention & Health"
DASHBOARD_LABEL = "Revenue Retention & Health"
SALES_MANAGER_DASHBOARD_ID = "0FKTb0000000JCLOA2"
CSM_MANAGER_DASHBOARD_ID = "0FKTb0000000J97OAE"
DEFAULT_RETENTION_YEAR = str(date.today().year - 1)

# Consulting-grade facet scope: KPI tiles listen to all filter pillboxes
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_year", "f_manager", "f_owner", "f_account"],
    },
}

# ── SOQL ──────────────────────────────────────────────────────────────────
OPP_SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, Account.OwnerId, Account.Owner.Name, "
    "Account.Owner.ManagerId, Account.Owner.Manager.Name, "
    "OwnerId, Owner.Name, Owner.ManagerId, Owner.Manager.Name, "
    "Type, StageName, Amount, APTS_Opportunity_ARR__c, APTS_Renewal_ACV__c, "
    "APTS_Forecast_ARR__c, APTS_Contract_Start_Date__c, APTS_Contract_End_Date__c, "
    "APTS_RH_Product_Family__c, ForecastCategoryName, IsClosed, IsWon, "
    "CloseDate, CreatedDate "
    "FROM Opportunity "
    "WHERE Type IN ('Land','Expand','Renewal') "
    "AND CloseDate >= 2022-01-01 "
    "ORDER BY CloseDate"
)

USER_DIM_FIELDS = (
    "Id, Name, Title, Department, Division, UserRole.Name, ManagerId, Manager.Name"
)


def _chunked(items, size=150):
    return [items[index : index + size] for index in range(0, len(items), size)]


def fetch_user_dimensions(inst, tok, user_ids):
    clean_ids = sorted({user_id for user_id in user_ids if user_id})
    dims = {}
    for chunk in _chunked(clean_ids):
        quoted_ids = ",".join(f"'{user_id}'" for user_id in chunk)
        query = f"SELECT {USER_DIM_FIELDS} FROM User WHERE Id IN ({quoted_ids})"
        for row in run_soql(inst, tok, query):
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


def run_soql(inst, tok, query):
    """Run a SOQL query and return all records with auto-pagination."""
    all_records = []
    url = f"{inst}/services/data/v66.0/query?q={urllib.parse.quote(query)}"
    while url:
        req = urllib.request.Request(
            url if url.startswith("http") else f"{inst}{url}",
            headers={"Authorization": f"Bearer {tok}"},
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
        all_records.extend(data.get("records", []))
        url = data.get("nextRecordsUrl")
    return all_records


def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def _account_owner_context(opp):
    """Use account ownership for the CSM surface; fall back to opp ownership."""
    account = opp.get("Account") or {}
    account_owner = account.get("Owner") or {}
    opp_owner = opp.get("Owner") or {}
    owner_id = account.get("OwnerId") or opp.get("OwnerId") or ""
    owner_name = account_owner.get("Name") or opp_owner.get("Name") or "Unassigned"
    manager_id = account_owner.get("ManagerId") or opp_owner.get("ManagerId") or ""
    manager_name = (
        ((account_owner.get("Manager") or {}).get("Name"))
        or ((opp_owner.get("Manager") or {}).get("Name"))
        or "Unassigned"
    )
    return owner_id, owner_name, manager_id, manager_name


def create_dataset(inst, tok):
    """Build the retention dataset from Opportunity data."""
    logger.info("  Querying Opportunity records...")
    opps = run_soql(inst, tok, OPP_SOQL)
    logger.info("  -> %d opportunities", len(opps))
    user_ids = set()
    for opp in opps:
        account = opp.get("Account") or {}
        account_owner = account.get("Owner") or {}
        owner = opp.get("Owner") or {}
        user_ids.update(
            filter(
                None,
                [
                    account.get("OwnerId") or "",
                    account_owner.get("ManagerId") or "",
                    opp.get("OwnerId") or "",
                    owner.get("ManagerId") or "",
                ],
            )
        )
    user_dimensions = fetch_user_dimensions(inst, tok, user_ids)

    today = date.today()

    # ── Classify each opp ──
    rows = []
    yearly = {}  # global year -> {land_won, expand_won, renewal_won, renewal_lost, expand_lost}
    account_yearly = {}  # (account, owner, manager, year) -> same metrics

    for o in opps:
        amt = safe_float(o.get("Amount"))
        raw_arr = safe_float(o.get("APTS_Opportunity_ARR__c"))
        arr = raw_arr or amt
        renewal_acv = safe_float(o.get("APTS_Renewal_ACV__c"))
        forecast_arr = safe_float(o.get("APTS_Forecast_ARR__c"))
        otype = o.get("Type", "")
        stage = o.get("StageName", "")
        is_won = o.get("IsWon", False)
        is_closed = o.get("IsClosed", False)
        close_date = o.get("CloseDate", "")
        acct = (o.get("Account") or {}).get("Name", "Unknown")
        owner_id, owner, manager_id, manager = _account_owner_context(o)
        owner_dim = user_dimensions.get(
            owner_id,
            role_dimension_row(
                owner_id=owner_id,
                owner_name=owner,
                manager_id=manager_id,
                manager_name=manager,
            ),
        )
        manager_dim = user_dimensions.get(
            manager_id,
            role_dimension_row(owner_id=manager_id, owner_name=manager),
        )
        forecast = o.get("ForecastCategoryName", "")
        product_family = o.get("APTS_RH_Product_Family__c") or ""
        contract_start = o.get("APTS_Contract_Start_Date__c") or ""
        contract_end = o.get("APTS_Contract_End_Date__c") or ""
        owner_persona = owner_dim.get("Persona", "Other")
        manager_persona = manager_dim.get("Persona", "Other")
        motion_primary_persona = primary_motion_persona(otype)
        ownership_status = ownership_alignment(owner_persona, otype)
        if otype == "Renewal":
            recurring_value = renewal_acv
            value_source = (
                "Renewal ACV"
                if renewal_acv > 0
                else ("Missing Renewal ACV" if amt > 0 else "Missing")
            )
            semantic_confidence = (
                "High" if renewal_acv > 0 else ("Low" if amt > 0 else "Missing")
            )
        else:
            recurring_value = raw_arr
            value_source = (
                "ARR" if raw_arr > 0 else ("Missing ARR" if amt > 0 else "Missing")
            )
            semantic_confidence = (
                "High" if raw_arr > 0 else ("Low" if amt > 0 else "Missing")
            )
        metric_coverage_flag = 1 if recurring_value > 0 else 0
        missing_metric_flag = 1 if recurring_value <= 0 and amt > 0 else 0

        # Parse year/quarter
        try:
            cd = datetime.strptime(close_date, "%Y-%m-%d")
            yr = cd.year
            qtr = (cd.month - 1) // 3 + 1
            year_label = str(yr)
            qtr_label = f"{yr}-Q{qtr}"
            month_label = cd.strftime("%Y-%m")
            days_until = (cd.date() - today).days
        except (ValueError, TypeError):
            yr, qtr, year_label, qtr_label, month_label, days_until = (
                0,
                0,
                "Unknown",
                "Unknown",
                "Unknown",
                0,
            )

        # Determine outcome
        if is_won:
            outcome = "Won"
        elif is_closed and not is_won:
            if "Lost" in stage:
                outcome = "Lost"
            elif "No Opportunity" in stage:
                outcome = "Churned"
            else:
                outcome = "Lost"
        else:
            outcome = "Open"

        # Determine risk level for open renewals
        risk = "N/A"
        if otype == "Renewal" and not is_closed:
            if days_until < 0:
                risk = "Overdue"
            elif days_until <= 30:
                risk = "Critical"
            elif days_until <= 90:
                risk = "High"
            elif days_until <= 180:
                risk = "Medium"
            else:
                risk = "Low"

        # Revenue motion classification
        if otype == "Renewal" and outcome == "Won":
            motion = "Retained"
        elif otype == "Renewal" and outcome in ("Lost", "Churned"):
            motion = "Churned"
        elif otype == "Expand" and outcome == "Won":
            motion = "Expanded"
        elif otype == "Expand" and outcome in ("Lost", "Churned"):
            motion = "Contraction"
        elif otype == "Land" and outcome == "Won":
            motion = "New Logo"
        else:
            motion = "Other"

        # Accumulate yearly metrics (only closed years) — use ARR for retention math
        if is_closed and yr >= 2023 and yr <= 2025:
            if yr not in yearly:
                yearly[yr] = {
                    "land_won": 0,
                    "expand_won": 0,
                    "renewal_won": 0,
                    "renewal_lost": 0,
                    "expand_lost": 0,
                }
            acct_key = (
                o.get("AccountId", ""),
                acct,
                owner_id,
                owner,
                manager_id,
                manager,
                yr,
            )
            if acct_key not in account_yearly:
                account_yearly[acct_key] = {
                    "land_won": 0,
                    "expand_won": 0,
                    "renewal_won": 0,
                    "renewal_lost": 0,
                    "expand_lost": 0,
                }
            if otype == "Land" and is_won:
                yearly[yr]["land_won"] += arr
                account_yearly[acct_key]["land_won"] += arr
            elif otype == "Expand" and is_won:
                yearly[yr]["expand_won"] += arr
                account_yearly[acct_key]["expand_won"] += arr
            elif otype == "Renewal" and is_won:
                yearly[yr]["renewal_won"] += arr
                account_yearly[acct_key]["renewal_won"] += arr
            elif otype == "Renewal" and not is_won:
                yearly[yr]["renewal_lost"] += arr
                account_yearly[acct_key]["renewal_lost"] += arr
            elif otype == "Expand" and not is_won:
                yearly[yr]["expand_lost"] += arr
                account_yearly[acct_key]["expand_lost"] += arr

        # Individual opp row (for pipeline and churn tables)
        row = {
            "RecordType": "opp_detail",
            "OppId": o.get("Id", ""),
            "OppName": o.get("Name", ""),
            "AccountId": o.get("AccountId", ""),
            "AccountName": acct,
            "OwnerId": owner_id,
            "OwnerName": owner,
            "OwnerPersona": owner_persona,
            "OwnerRole": owner_dim.get("UserRole", ""),
            "OwnerTitle": owner_dim.get("Title", ""),
            "ManagerId": manager_id,
            "ManagerName": manager,
            "ManagerPersona": manager_persona,
            "OppType": otype,
            "MotionPrimaryPersona": motion_primary_persona,
            "OwnershipAlignment": ownership_status,
            "Stage": stage,
            "Amount": amt,
            "ARR": arr,
            "RenewalACV": renewal_acv,
            "RecurringValue": recurring_value,
            "ValueSource": value_source,
            "SemanticConfidence": semantic_confidence,
            "MetricCoverageFlag": metric_coverage_flag,
            "MissingMetricFlag": missing_metric_flag,
            "AlignmentNeedsReviewFlag": 1 if ownership_status == "Needs Review" else 0,
            "ForecastARR": forecast_arr,
            "ProductFamily": product_family,
            "ContractStart": contract_start,
            "ContractEnd": contract_end,
            "ForecastCategory": forecast,
            "IsClosed": 1 if is_closed else 0,
            "IsWon": 1 if is_won else 0,
            "CloseDate": close_date,
            "Year": yr,
            "YearLabel": year_label,
            "Quarter": qtr,
            "QuarterLabel": qtr_label,
            "MonthLabel": month_label,
            "Outcome": outcome,
            "Motion": motion,
            "RiskLevel": risk,
            "DaysUntilClose": days_until,
            "OpenRenewalValue": recurring_value
            if otype == "Renewal" and not is_closed
            else 0,
            "AtRiskRenewalValue": (
                recurring_value
                if otype == "Renewal"
                and not is_closed
                and risk in {"Overdue", "Critical", "High"}
                else 0
            ),
            # Metric columns (zeroed for detail rows)
            "StartingARR": 0,
            "RenewalWonARR": 0,
            "ExpansionARR": 0,
            "ChurnARR": 0,
            "EndingARR": 0,
            "NRR": 0,
            "GRR": 0,
            "ChurnRate": 0,
            "NewLogoARR": 0,
        }
        rows.append(row)

    # ── Compute yearly retention metrics ──
    for yr in sorted(yearly.keys()):
        d = yearly[yr]
        # Starting ARR = prior year's (renewal_won + expand_won)
        prior = yearly.get(yr - 1)
        if prior:
            starting = prior["renewal_won"] + prior["expand_won"]
        else:
            starting = d["renewal_won"] + d["renewal_lost"]  # estimate

        expansion = d["expand_won"]
        churn = d["renewal_lost"]
        new_logo = d["land_won"]
        ending = starting + expansion - churn + new_logo

        nrr = ((starting + expansion - churn) / starting * 100) if starting > 0 else 0
        grr = ((starting - churn) / starting * 100) if starting > 0 else 0
        churn_rate = (churn / starting * 100) if starting > 0 else 0

        rows.append(
            {
                "RecordType": "yearly_metric",
                "OppId": "",
                "OppName": f"FY{yr}",
                "AccountId": "",
                "AccountName": "",
                "OwnerId": "",
                "OwnerName": "",
                "OwnerPersona": "",
                "OwnerRole": "",
                "OwnerTitle": "",
                "ManagerId": "",
                "ManagerName": "",
                "ManagerPersona": "",
                "OppType": "",
                "MotionPrimaryPersona": "",
                "OwnershipAlignment": "",
                "Stage": "",
                "Amount": 0,
                "ARR": 0,
                "RenewalACV": 0,
                "RecurringValue": 0,
                "ValueSource": "",
                "SemanticConfidence": "",
                "MetricCoverageFlag": 0,
                "MissingMetricFlag": 0,
                "AlignmentNeedsReviewFlag": 0,
                "ForecastARR": 0,
                "ProductFamily": "",
                "ContractStart": "",
                "ContractEnd": "",
                "ForecastCategory": "",
                "IsClosed": 0,
                "IsWon": 0,
                "CloseDate": f"{yr}-12-31",
                "Year": yr,
                "YearLabel": str(yr),
                "Quarter": 0,
                "QuarterLabel": f"{yr}",
                "MonthLabel": f"{yr}-12",
                "Outcome": "",
                "Motion": "",
                "RiskLevel": "",
                "DaysUntilClose": 0,
                "OpenRenewalValue": 0,
                "AtRiskRenewalValue": 0,
                "StartingARR": round(starting, 2),
                "RenewalWonARR": round(d["renewal_won"], 2),
                "ExpansionARR": round(expansion, 2),
                "ChurnARR": round(churn, 2),
                "EndingARR": round(ending, 2),
                "NRR": round(nrr, 2),
                "GRR": round(grr, 2),
                "ChurnRate": round(churn_rate, 2),
                "NewLogoARR": round(new_logo, 2),
            }
        )

    # ── Compute account-year retention metrics so account/manager filters can drive page 1 ──
    account_histories = {}
    for (
        account_id,
        account_name,
        owner_id,
        owner_name,
        manager_id,
        manager_name,
        yr,
    ), values in account_yearly.items():
        acct_scope = (
            account_id,
            account_name,
            owner_id,
            owner_name,
            manager_id,
            manager_name,
        )
        account_histories.setdefault(acct_scope, {})[yr] = values

    for (
        account_id,
        account_name,
        owner_id,
        owner_name,
        manager_id,
        manager_name,
    ), history in account_histories.items():
        prior_ending = 0.0
        for yr in sorted(history.keys()):
            d = history[yr]
            starting = prior_ending
            expansion = d["expand_won"]
            churn = d["renewal_lost"]
            renewal_won = d["renewal_won"]
            new_logo = d["land_won"]
            ending = starting + expansion + new_logo - churn
            nrr = (
                ((starting + expansion - churn) / starting * 100) if starting > 0 else 0
            )
            grr = ((starting - churn) / starting * 100) if starting > 0 else 0
            churn_rate = (churn / starting * 100) if starting > 0 else 0

            rows.append(
                {
                    "RecordType": "account_year_metric",
                    "OppId": "",
                    "OppName": f"{account_name} FY{yr}",
                    "AccountId": account_id,
                    "AccountName": account_name,
                    "OwnerId": owner_id,
                    "OwnerName": owner_name,
                    "OwnerPersona": "",
                    "OwnerRole": "",
                    "OwnerTitle": "",
                    "ManagerId": manager_id,
                    "ManagerName": manager_name,
                    "ManagerPersona": "",
                    "OppType": "",
                    "MotionPrimaryPersona": "",
                    "OwnershipAlignment": "",
                    "Stage": "",
                    "Amount": 0,
                    "ARR": 0,
                    "RenewalACV": 0,
                    "RecurringValue": 0,
                    "ValueSource": "",
                    "SemanticConfidence": "",
                    "MetricCoverageFlag": 0,
                    "MissingMetricFlag": 0,
                    "AlignmentNeedsReviewFlag": 0,
                    "ForecastARR": 0,
                    "ProductFamily": "",
                    "ContractStart": "",
                    "ContractEnd": "",
                    "ForecastCategory": "",
                    "IsClosed": 0,
                    "IsWon": 0,
                    "CloseDate": f"{yr}-12-31",
                    "Year": yr,
                    "YearLabel": str(yr),
                    "Quarter": 0,
                    "QuarterLabel": f"{yr}",
                    "MonthLabel": f"{yr}-12",
                    "Outcome": "",
                    "Motion": "",
                    "RiskLevel": "",
                    "DaysUntilClose": 0,
                    "OpenRenewalValue": 0,
                    "AtRiskRenewalValue": 0,
                    "StartingARR": round(starting, 2),
                    "RenewalWonARR": round(renewal_won, 2),
                    "ExpansionARR": round(expansion, 2),
                    "ChurnARR": round(churn, 2),
                    "EndingARR": round(ending, 2),
                    "NRR": round(nrr, 2),
                    "GRR": round(grr, 2),
                    "ChurnRate": round(churn_rate, 2),
                    "NewLogoARR": round(new_logo, 2),
                }
            )

            bridge_items = [
                ("Starting ARR", starting, "start"),
                ("Renewal Won", renewal_won, "positive"),
                ("Expansion", expansion, "positive"),
                ("New Logos", new_logo, "positive"),
                ("Churn", -churn, "negative"),
                ("Ending ARR", ending, "total"),
            ]
            for label, val, btype in bridge_items:
                rows.append(
                    {
                        "RecordType": "waterfall_metric",
                        "OppId": "",
                        "OppName": label,
                        "AccountId": account_id,
                        "AccountName": account_name,
                        "OwnerId": owner_id,
                        "OwnerName": owner_name,
                        "OwnerPersona": "",
                        "OwnerRole": "",
                        "OwnerTitle": "",
                        "ManagerId": manager_id,
                        "ManagerName": manager_name,
                        "ManagerPersona": "",
                        "OppType": btype,
                        "MotionPrimaryPersona": "",
                        "OwnershipAlignment": "",
                        "Stage": "",
                        "Amount": round(val, 2),
                        "ARR": 0,
                        "RenewalACV": 0,
                        "RecurringValue": 0,
                        "ValueSource": "",
                        "SemanticConfidence": "",
                        "MetricCoverageFlag": 0,
                        "MissingMetricFlag": 0,
                        "AlignmentNeedsReviewFlag": 0,
                        "ForecastARR": 0,
                        "ProductFamily": "",
                        "ContractStart": "",
                        "ContractEnd": "",
                        "ForecastCategory": "",
                        "IsClosed": 0,
                        "IsWon": 0,
                        "CloseDate": f"{yr}-12-31",
                        "Year": yr,
                        "YearLabel": str(yr),
                        "Quarter": 0,
                        "QuarterLabel": str(yr),
                        "MonthLabel": f"{yr}-12",
                        "Outcome": "",
                        "Motion": label,
                        "RiskLevel": "",
                        "DaysUntilClose": 0,
                        "OpenRenewalValue": 0,
                        "AtRiskRenewalValue": 0,
                        "StartingARR": 0,
                        "RenewalWonARR": 0,
                        "ExpansionARR": 0,
                        "ChurnARR": 0,
                        "EndingARR": 0,
                        "NRR": 0,
                        "GRR": 0,
                        "ChurnRate": 0,
                        "NewLogoARR": 0,
                    }
                )

            prior_ending = ending

    # ── Waterfall bridge rows for latest year ──
    latest_yr = max(yearly.keys())
    d = yearly[latest_yr]
    prior = yearly.get(latest_yr - 1)
    starting = (
        (prior["renewal_won"] + prior["expand_won"])
        if prior
        else (d["renewal_won"] + d["renewal_lost"])
    )

    bridge_items = [
        ("Starting ARR", starting, "start"),
        ("Renewal Won", d["renewal_won"], "positive"),
        ("Expansion", d["expand_won"], "positive"),
        ("New Logos", d["land_won"], "positive"),
        ("Churn", -d["renewal_lost"], "negative"),
        (
            "Ending ARR",
            starting + d["expand_won"] + d["land_won"] - d["renewal_lost"],
            "total",
        ),
    ]
    for label, val, btype in bridge_items:
        rows.append(
            {
                "RecordType": "waterfall",
                "OppId": "",
                "OppName": label,
                "AccountId": "",
                "AccountName": "",
                "OwnerId": "",
                "OwnerName": "",
                "OwnerPersona": "",
                "OwnerRole": "",
                "OwnerTitle": "",
                "ManagerId": "",
                "ManagerName": "",
                "ManagerPersona": "",
                "OppType": btype,
                "MotionPrimaryPersona": "",
                "OwnershipAlignment": "",
                "Stage": "",
                "Amount": round(val, 2),
                "ARR": 0,
                "RenewalACV": 0,
                "RecurringValue": 0,
                "ValueSource": "",
                "SemanticConfidence": "",
                "MetricCoverageFlag": 0,
                "MissingMetricFlag": 0,
                "AlignmentNeedsReviewFlag": 0,
                "ForecastARR": 0,
                "ProductFamily": "",
                "ContractStart": "",
                "ContractEnd": "",
                "ForecastCategory": "",
                "IsClosed": 0,
                "IsWon": 0,
                "CloseDate": f"{latest_yr}-12-31",
                "Year": latest_yr,
                "Quarter": 0,
                "QuarterLabel": str(latest_yr),
                "MonthLabel": f"{latest_yr}-12",
                "Outcome": "",
                "Motion": label,
                "RiskLevel": "",
                "DaysUntilClose": 0,
                "OpenRenewalValue": 0,
                "AtRiskRenewalValue": 0,
                "StartingARR": 0,
                "RenewalWonARR": 0,
                "ExpansionARR": 0,
                "ChurnARR": 0,
                "EndingARR": 0,
                "NRR": 0,
                "GRR": 0,
                "ChurnRate": 0,
                "NewLogoARR": 0,
            }
        )

    logger.info(
        "  -> %d total rows (%d opps, %d account-year metrics, %d waterfall metrics)",
        len(rows),
        sum(1 for r in rows if r["RecordType"] == "opp_detail"),
        sum(1 for r in rows if r["RecordType"] == "account_year_metric"),
        sum(1 for r in rows if r["RecordType"] == "waterfall_metric"),
    )

    # ── Build CSV ──
    fields = list(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8")

    fields_meta = [
        _dim("RecordType"),
        _dim("OppId"),
        _dim("OppName"),
        _dim("AccountId"),
        _dim("AccountName"),
        _dim("OwnerId"),
        _dim("OwnerName"),
        _dim("OwnerPersona"),
        _dim("OwnerRole"),
        _dim("OwnerTitle"),
        _dim("ManagerId"),
        _dim("ManagerName"),
        _dim("ManagerPersona"),
        _dim("OppType"),
        _dim("MotionPrimaryPersona"),
        _dim("OwnershipAlignment"),
        _dim("Stage"),
        _measure("Amount"),
        _measure("ARR"),
        _measure("RenewalACV"),
        _measure("RecurringValue"),
        _dim("ValueSource"),
        _dim("SemanticConfidence"),
        _measure("MetricCoverageFlag", precision=18, scale=0),
        _measure("MissingMetricFlag", precision=18, scale=0),
        _measure("AlignmentNeedsReviewFlag", precision=18, scale=0),
        _measure("ForecastARR"),
        _dim("ProductFamily"),
        _dim("ContractStart"),
        _dim("ContractEnd"),
        _dim("ForecastCategory"),
        _measure("IsClosed", precision=18, scale=0),
        _measure("IsWon", precision=18, scale=0),
        _dim("CloseDate"),
        _measure("Year", precision=18, scale=0),
        _dim("YearLabel"),
        _measure("Quarter", precision=18, scale=0),
        _dim("QuarterLabel"),
        _dim("MonthLabel"),
        _dim("Outcome"),
        _dim("Motion"),
        _dim("RiskLevel"),
        _measure("DaysUntilClose", precision=18, scale=0),
        _measure("OpenRenewalValue"),
        _measure("AtRiskRenewalValue"),
        _measure("StartingARR"),
        _measure("RenewalWonARR"),
        _measure("ExpansionARR"),
        _measure("ChurnARR"),
        _measure("EndingARR"),
        _measure("NRR"),
        _measure("GRR"),
        _measure("ChurnRate"),
        _measure("NewLogoARR"),
    ]

    ok = upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)
    return ok, len(rows)


# ── Steps ─────────────────────────────────────────────────────────────────
def build_steps(ds_id):
    ds_meta = [{"id": ds_id, "name": DS}]
    detail = f'q = load "{DS}";\nq = filter q by RecordType == "opp_detail";\n'
    metric = f'q = load "{DS}";\nq = filter q by RecordType == "account_year_metric";\n'
    wfall = f'q = load "{DS}";\nq = filter q by RecordType == "waterfall_metric";\n'

    return {
        # Filters
        "f_year": af("YearLabel", ds_meta, start=f'["{DEFAULT_RETENTION_YEAR}"]'),
        "f_manager": af("ManagerName", ds_meta),
        "f_owner": af("OwnerName", ds_meta),
        "f_account": af("AccountName", ds_meta),
        # ── Page 1: Retention Summary ──
        "s_latest_metrics": {
            **sq(
                metric
                + "q = group q by YearLabel;\n"
                + "q = foreach q generate YearLabel as YearLabel, "
                + "sum(StartingARR) as StartingARR, "
                + "sum(RenewalWonARR) as RenewalWonARR, "
                + "sum(ExpansionARR) as ExpansionARR, "
                + "sum(ChurnARR) as ChurnARR, "
                + "sum(EndingARR) as EndingARR, "
                + "sum(NewLogoARR) as NewLogoARR;\n"
                + "q = foreach q generate YearLabel, StartingARR, RenewalWonARR, ExpansionARR, ChurnARR, EndingARR, NewLogoARR, "
                + "case when StartingARR > 0 then ((StartingARR + ExpansionARR - ChurnARR) / StartingARR) * 100 else 0 end as NRR, "
                + "case when StartingARR > 0 then ((StartingARR - ChurnARR) / StartingARR) * 100 else 0 end as GRR, "
                + "case when StartingARR > 0 then (ChurnARR / StartingARR) * 100 else 0 end as ChurnRate;\n"
                + "q = order q by YearLabel desc;\n"
                + "q = limit q 1;"
            ),
            **KPI_FACET_SCOPE,
        },
        "s_yearly_trend": sq(
            metric
            + "q = group q by YearLabel;\n"
            + "q = foreach q generate YearLabel as YearLabel, "
            + "sum(StartingARR) as StartingARR, "
            + "sum(ExpansionARR) as ExpansionARR, "
            + "sum(ChurnARR) as ChurnARR, "
            + "sum(EndingARR) as EndingARR, "
            + "sum(NewLogoARR) as NewLogoARR;\n"
            + "q = foreach q generate YearLabel, StartingARR, ExpansionARR, ChurnARR, EndingARR, NewLogoARR, "
            + "case when StartingARR > 0 then ((StartingARR + ExpansionARR - ChurnARR) / StartingARR) * 100 else 0 end as NRR, "
            + "case when StartingARR > 0 then ((StartingARR - ChurnARR) / StartingARR) * 100 else 0 end as GRR, "
            + "case when StartingARR > 0 then (ChurnARR / StartingARR) * 100 else 0 end as ChurnRate;\n"
            + "q = order q by YearLabel asc;"
        ),
        "s_nrr_bullet": {
            **sq(
                metric
                + "q = group q by YearLabel;\n"
                + "q = foreach q generate YearLabel as YearLabel, sum(StartingARR) as StartingARR, sum(ExpansionARR) as ExpansionARR, sum(ChurnARR) as ChurnARR;\n"
                + "q = foreach q generate YearLabel, case when StartingARR > 0 then ((StartingARR + ExpansionARR - ChurnARR) / StartingARR) * 100 else 0 end as NRR, 110 as target;\n"
                + "q = order q by YearLabel desc;\n"
                + "q = limit q 1;"
            ),
            **KPI_FACET_SCOPE,
        },
        "s_grr_bullet": {
            **sq(
                metric
                + "q = group q by YearLabel;\n"
                + "q = foreach q generate YearLabel as YearLabel, sum(StartingARR) as StartingARR, sum(ChurnARR) as ChurnARR;\n"
                + "q = foreach q generate YearLabel, case when StartingARR > 0 then ((StartingARR - ChurnARR) / StartingARR) * 100 else 0 end as GRR, 95 as target;\n"
                + "q = order q by YearLabel desc;\n"
                + "q = limit q 1;"
            ),
            **KPI_FACET_SCOPE,
        },
        "s_waterfall": sq(
            wfall
            + "q = group q by Motion;\n"
            + "q = foreach q generate Motion as Category, sum(Amount) as Amount, "
            + "case "
            + 'when Motion == "Starting ARR" then 1 '
            + 'when Motion == "Renewal Won" then 2 '
            + 'when Motion == "Expansion" then 3 '
            + 'when Motion == "New Logos" then 4 '
            + 'when Motion == "Churn" then 5 '
            + 'when Motion == "Ending ARR" then 6 '
            + "else 7 end as SortOrder;\n"
            + "q = order q by SortOrder asc;"
        ),
        "s_motion_mix": sq(
            detail
            + "q = filter q by IsWon == 1;\n"
            + "q = group q by Motion;\n"
            + "q = foreach q generate Motion, sum(Amount) as Revenue;\n"
            + "q = order q by Revenue desc;"
        ),
        "s_summary_action_queue": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and IsClosed == 0;\n'
            + "q = group q by (OppName, OppId, AccountName, OwnerName, ManagerName, QuarterLabel, RiskLevel, ForecastCategory);\n"
            + "q = foreach q generate OppName, OppId, AccountName, OwnerName, ManagerName, QuarterLabel, "
            + "max(Amount) as Amount, max(DaysUntilClose) as DaysUntilClose, "
            + '(case when RiskLevel == "Overdue" then "Renewal | Overdue" '
            + 'when RiskLevel == "Critical" then "Renewal | Next 30 days" '
            + 'when RiskLevel == "High" then "Renewal | Next 90 days" '
            + 'when ForecastCategory == "Omitted" then "Renewal | Unclassified" '
            + 'else "Renewal | Active" end) as ForecastPulse, '
            + '(case when RiskLevel == "Overdue" then "Lock exec sponsor and save plan now" '
            + 'when RiskLevel == "Critical" then "Confirm commercial path this week" '
            + 'when RiskLevel == "High" then "Review renewal path in 14 days" '
            + 'when ForecastCategory == "Omitted" then "Assign category and close plan" '
            + 'else "Review QBR and renewal path" end) as ManagerAsk;\n'
            + "q = order q by Amount desc;\n"
            + "q = limit q 8;"
        ),
        # ── Page 2: Trend & Cohort ──
        "s_quarterly_revenue": sq(
            detail
            + "q = filter q by IsClosed == 1 and Year >= 2023;\n"
            + "q = group q by (QuarterLabel, OppType, Outcome);\n"
            + "q = foreach q generate QuarterLabel, OppType, Outcome, "
            + "sum(Amount) as Revenue, count() as DealCount;\n"
            + "q = order q by QuarterLabel asc;"
        ),
        "s_retention_by_qtr": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and IsClosed == 1 and Year >= 2023;\n'
            + "q = group q by QuarterLabel;\n"
            + "q = foreach q generate QuarterLabel, "
            + "sum(case when IsWon == 1 then Amount else 0 end) as RenewalWon, "
            + "sum(case when IsWon == 0 then Amount else 0 end) as RenewalLost, "
            + "count(case when IsWon == 1 then true else null end) as WonCount, "
            + "count(case when IsWon == 0 then true else null end) as LostCount;\n"
            + "q = order q by QuarterLabel asc;"
        ),
        "s_expansion_by_qtr": sq(
            detail
            + 'q = filter q by OppType == "Expand" and IsClosed == 1 and Year >= 2023;\n'
            + "q = group q by QuarterLabel;\n"
            + "q = foreach q generate QuarterLabel, "
            + "sum(case when IsWon == 1 then Amount else 0 end) as ExpansionWon, "
            + "sum(case when IsWon == 0 then Amount else 0 end) as ExpansionLost;\n"
            + "q = order q by QuarterLabel asc;"
        ),
        # ── Page 3: Renewal Pipeline ──
        "s_renewal_pipeline": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and IsClosed == 0;\n'
            + "q = foreach q generate OppName, OppId, AccountName, OwnerName, ManagerName, "
            + "Amount, Stage, ForecastCategory, RiskLevel, DaysUntilClose, CloseDate, QuarterLabel;\n"
            + "q = order q by Amount desc;"
        ),
        "s_renewal_save_queue": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and IsClosed == 0;\n'
            + "q = group q by (OppName, OppId, AccountName, OwnerName, ManagerName, QuarterLabel, RiskLevel, ForecastCategory);\n"
            + "q = foreach q generate OppName, OppId, AccountName, OwnerName, ManagerName, QuarterLabel, "
            + "max(Amount) as Amount, max(DaysUntilClose) as DaysUntilClose, "
            + "(case "
            + 'when RiskLevel == "Overdue" then "Renewal | Overdue" '
            + 'when RiskLevel == "Critical" then "Renewal | Next 30 days" '
            + 'when RiskLevel == "High" then "Renewal | Next 90 days" '
            + 'when ForecastCategory == "Omitted" then "Renewal | Unclassified" '
            + 'else "Renewal | Active" end) as ForecastPulse, '
            + "(case "
            + 'when RiskLevel == "Overdue" then "Lock exec sponsor and save plan now" '
            + 'when RiskLevel == "Critical" then "Confirm commercial path this week" '
            + 'when RiskLevel == "High" then "Review renewal path in 14 days" '
            + 'when ForecastCategory == "Omitted" then "Assign category and close plan" '
            + 'else "Review QBR and renewal path" end) as ManagerAsk;\n'
            + "q = order q by Amount desc;\n"
            + "q = limit q 12;"
        ),
        "s_pipeline_by_risk": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and IsClosed == 0;\n'
            + "q = group q by RiskLevel;\n"
            + "q = foreach q generate RiskLevel, sum(Amount) as AtRiskARR, count() as Deals, "
            + "case "
            + 'when RiskLevel == "Overdue" then 1 '
            + 'when RiskLevel == "Critical" then 2 '
            + 'when RiskLevel == "High" then 3 '
            + 'when RiskLevel == "Medium" then 4 '
            + 'when RiskLevel == "Low" then 5 '
            + "else 6 end as SortOrder;\n"
            + "q = order q by SortOrder asc;"
        ),
        "s_pipeline_summary": {
            **sq(
                detail
                + 'q = filter q by OppType == "Renewal" and IsClosed == 0;\n'
                + "q = group q by all;\n"
                + "q = foreach q generate sum(Amount) as TotalRenewalPipeline, "
                + "count() as TotalDeals, "
                + 'sum(case when RiskLevel == "Critical" or RiskLevel == "Overdue" then Amount else 0 end) as CriticalARR;'
            ),
            **KPI_FACET_SCOPE,
        },
        # ── Page 4: Churn Analysis ──
        "s_churn_detail": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and IsClosed == 1 and IsWon == 0;\n'
            + "q = foreach q generate OppName, OppId, AccountName, OwnerName, ManagerName, "
            + "Amount, Stage, QuarterLabel, Outcome;\n"
            + "q = order q by Amount desc;"
        ),
        "s_churn_root_causes": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and IsClosed == 1 and IsWon == 0;\n'
            + "q = group q by (OppName, OppId, AccountName, OwnerName, ManagerName, QuarterLabel, Stage, Outcome);\n"
            + "q = foreach q generate OppName, OppId, AccountName, OwnerName, ManagerName, QuarterLabel, Stage, Outcome, "
            + "max(Amount) as Amount, "
            + "(case "
            + 'when Outcome == "Churned" then "Churned | Recovery postmortem" '
            + 'when Stage matches ".*Lost.*" then "Lost Renewal | Root cause" '
            + 'else "Lost Renewal | Review" end) as ChurnPulse, '
            + "(case "
            + 'when max(Amount) >= 1000000 then "Run exec loss review this week" '
            + 'when max(Amount) >= 250000 then "Confirm root cause and next account plan" '
            + 'else "Capture reason code and handoff" end) as ManagerAsk;\n'
            + "q = order q by Amount desc;\n"
            + "q = limit q 12;"
        ),
        "s_churn_by_qtr": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and IsClosed == 1 and IsWon == 0;\n'
            + "q = group q by QuarterLabel;\n"
            + "q = foreach q generate QuarterLabel, "
            + "sum(Amount) as ChurnedARR, count() as ChurnedDeals;\n"
            + "q = order q by QuarterLabel asc;"
        ),
        "s_churn_by_owner": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and IsClosed == 1 and IsWon == 0 and Year >= 2024;\n'
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(Amount) as ChurnedARR, count() as ChurnedDeals;\n"
            + "q = order q by ChurnedARR desc;\n"
            + "q = limit q 15;"
        ),
        "s_renewal_confidence": sq(
            detail
            + 'q = filter q by OppType == "Renewal" and Year >= 2023;\n'
            + "q = group q by (ManagerName, OwnerName, OwnerPersona, OwnershipAlignment);\n"
            + "q = foreach q generate ManagerName, OwnerName, OwnerPersona, OwnershipAlignment, "
            + "sum(RecurringValue) as RenewalValue, "
            + "sum(MetricCoverageFlag) as CoveredDeals, "
            + "count() as RenewalDeals, "
            + "sum(MissingMetricFlag) as MissingMetricDeals, "
            + "sum(AtRiskRenewalValue) as AtRiskRenewalValue, "
            + "sum(AlignmentNeedsReviewFlag) as OwnershipMismatchDeals;\n"
            + "q = foreach q generate ManagerName, OwnerName, OwnerPersona, OwnershipAlignment, RenewalValue, CoveredDeals, RenewalDeals, MissingMetricDeals, AtRiskRenewalValue, OwnershipMismatchDeals, "
            + "case when RenewalDeals > 0 then (CoveredDeals / RenewalDeals) * 100 else 0 end as MetricCoveragePct;\n"
            + "q = order q by MetricCoveragePct asc, MissingMetricDeals desc, AtRiskRenewalValue desc;\n"
            + "q = limit q 15;"
        ),
        "s_churn_summary": {
            **sq(
                detail
                + 'q = filter q by OppType == "Renewal" and IsClosed == 1 and IsWon == 0 and Year >= 2024;\n'
                + "q = group q by all;\n"
                + "q = foreach q generate sum(Amount) as TotalChurned, count() as TotalDeals;"
            ),
            **KPI_FACET_SCOPE,
        },
    }


# ── Widgets ───────────────────────────────────────────────────────────────
def build_widgets():
    w = {}
    for page_prefix in ("p1", "p2", "p3", "p4"):
        w[f"{page_prefix}_tab_sales"] = nav_link_external(
            SALES_MANAGER_DASHBOARD_ID, "Sales Manager", include_state=False
        )
        w[f"{page_prefix}_tab_csm"] = nav_link_external(
            CSM_MANAGER_DASHBOARD_ID, "CSM Manager", include_state=False
        )

    # ═══ Page 1: Retention Summary ═══
    w["p1_hdr"] = hdr(
        "Revenue Retention & Health",
        "CSM manager view of installed-base ARR, retention targets, and the renewals that need intervention now",
    )
    w["p1_n_nrr"] = num(
        "s_latest_metrics",
        "StartingARR",
        "Starting ARR",
        "#54698D",
        compact=False,
        tier="primary",
        prefix="€",
        widget_style=KPI_CARD_STYLE,
    )
    w["p1_n_grr"] = num(
        "s_latest_metrics",
        "NewLogoARR",
        "New Logo ARR",
        "#2E844A",
        compact=False,
        tier="primary",
        prefix="€",
        widget_style=KPI_CARD_STYLE,
    )
    w["p1_n_churn"] = num(
        "s_latest_metrics",
        "ChurnARR",
        "Churn ARR",
        "#D4504C",
        compact=False,
        tier="secondary",
        prefix="€",
        widget_style=KPI_CARD_STYLE,
    )
    w["p1_n_ending"] = num(
        "s_latest_metrics",
        "EndingARR",
        "Ending ARR",
        "#9050E9",
        compact=True,
        tier="secondary",
        prefix="€",
        widget_style=KPI_CARD_STYLE,
    )

    w["p1_b_nrr"] = bullet_chart(
        "s_nrr_bullet", "NRR vs Target (Existing Base)", axis_title="%"
    )
    w["p1_b_grr"] = bullet_chart(
        "s_grr_bullet", "GRR vs Target (Existing Base)", axis_title="%"
    )
    w["p1_ch_waterfall"] = waterfall_chart(
        "s_waterfall",
        "ARR Bridge — Starting → Ending",
        "Category",
        "Amount",
    )
    w["p1_ch_yearly"] = combo_chart(
        "s_yearly_trend",
        "Installed Base ARR and Retention by Year",
        ["YearLabel"],
        bar_measures=["StartingARR", "EndingARR"],
        line_measures=["NRR", "GRR"],
        show_legend=True,
        axis_title="ARR (€)",
        axis2_title="Retention %",
        subtitle="Bars = ARR base and ending ARR | Lines = NRR and GRR — watch whether the installed base is retaining and growing cleanly",
        axis1_format="€#,##0",
        reference_lines=[
            {"value": 100, "label": "100% Retention", "color": "#54698D"},
        ],
    )
    w["p1_tbl_actions"] = compare_table(
        "s_summary_action_queue",
        "Top Save Actions",
        columns=[
            "AccountName",
            "Amount",
            "ForecastPulse",
            "ManagerAsk",
            "QuarterLabel",
            "OwnerName",
        ],
        column_properties={
            "Amount": {"width": 120, "alignment": "right"},
        },
        subtitle="Highest-value renewals that need a save plan, forecast cleanup, or commercial escalation now.",
        format_rules=[
            {
                "type": "threshold",
                "field": "Amount",
                "rules": [
                    {"value": 1000000, "color": "#D4504C", "operator": "gte"},
                    {"value": 250000, "color": "#FF9A3C", "operator": "gte"},
                ],
            },
        ],
    )

    # ═══ Page 2: Trend & Cohort ═══
    w["p2_hdr"] = hdr(
        "Retention Trends & Cohorts",
        "CSM manager view of quarterly retention, cohort performance, and expansion trajectories",
    )
    w["p2_sec_retention"] = section_label("Cohort Retention & Expansion")
    w["p2_ch_retention"] = combo_chart(
        "s_retention_by_qtr",
        "Renewal Cohort Retention",
        ["QuarterLabel"],
        bar_measures=["RenewalWon", "RenewalLost"],
        line_measures=["WonCount"],
        show_legend=True,
        axis_title="Revenue (€)",
        axis2_title="Deal Count",
        subtitle="Each quarter acts as a renewal cohort: bars show ARR retained vs lost, line shows cohort deal count",
        axis1_format="€#,##0",
    )
    w["p2_ch_expansion"] = combo_chart(
        "s_expansion_by_qtr",
        "Quarterly Expansion Outcome",
        ["QuarterLabel"],
        bar_measures=["ExpansionWon", "ExpansionLost"],
        line_measures=[],
        show_legend=True,
        axis_title="Revenue (€)",
        subtitle="Expansion revenue won vs lost — expansion is the primary NRR driver above 100%",
        axis1_format="€#,##0",
    )
    w["p2_sec_mix"] = section_label("Revenue Mix")
    w["p2_ch_quarterly_rev"] = rich_chart(
        "s_quarterly_revenue",
        "stackcolumn",
        "Won Revenue by Type & Quarter",
        ["QuarterLabel", "OppType"],
        ["Revenue"],
        show_legend=True,
        axis_title="Revenue (€)",
        subtitle="Stacked breakdown: Land (new logos) + Expand + Renewal each quarter — watch for type concentration risk",
    )

    # ═══ Page 3: Renewal Pipeline ═══
    w["p3_hdr"] = hdr(
        "Renewal Pipeline & Risk",
        "CSM manager save surface for upcoming renewals, risk triage, and intervention ownership.",
    )
    w["p3_n_pipeline"] = num(
        "s_pipeline_summary",
        "TotalRenewalPipeline",
        "Total Renewal Pipeline",
        "#0070D2",
        compact=True,
        tier="primary",
        prefix="€",
        widget_style=KPI_CARD_STYLE,
    )
    w["p3_n_deals"] = num(
        "s_pipeline_summary",
        "TotalDeals",
        "Open Renewals",
        "#9050E9",
        tier="secondary",
        widget_style=KPI_CARD_STYLE,
    )
    w["p3_n_critical"] = num(
        "s_pipeline_summary",
        "CriticalARR",
        "Critical / Overdue ARR",
        "#D4504C",
        compact=True,
        tier="secondary",
        prefix="€",
        widget_style=KPI_CARD_STYLE,
    )
    w["p3_sec_risk"] = section_label("Renewal Save Priorities")
    w["p3_ch_risk"] = rich_chart(
        "s_pipeline_by_risk",
        "hbar",
        "Renewal Pipeline by Risk Level",
        ["RiskLevel"],
        ["AtRiskARR"],
        subtitle="Risk bands: Overdue (past due) → Critical (≤30d) → High (≤90d) → Medium (≤180d) → Low (>180d)",
    )
    w["p3_ch_table"] = compare_table(
        "s_renewal_save_queue",
        "Renewal Save Queue",
        columns=[
            "OppName",
            "AccountName",
            "Amount",
            "ForecastPulse",
            "ManagerAsk",
            "QuarterLabel",
            "DaysUntilClose",
            "OwnerName",
        ],
        column_properties={
            "Amount": {"width": 120, "alignment": "right"},
            "DaysUntilClose": {"width": 80, "alignment": "right"},
        },
        subtitle="Highest-value renewals that need save-plan action, commercial escalation, or forecast cleanup.",
        format_rules=[
            {
                "type": "threshold",
                "field": "DaysUntilClose",
                "rules": [
                    {"value": 0, "color": "#D4504C", "operator": "lte"},
                    {"value": 30, "color": "#FF9A3C", "operator": "lte"},
                    {"value": 90, "color": "#FFB75D", "operator": "lte"},
                ],
            },
        ],
    )

    # ═══ Page 4: Churn Analysis ═══
    w["p4_hdr"] = hdr(
        "Churn Analysis",
        "CSM manager root-cause view of lost renewals, ownership patterns, and follow-up actions.",
    )
    w["p4_n_churned"] = num(
        "s_churn_summary",
        "TotalChurned",
        "Total Churned ARR (2024+)",
        "#D4504C",
        compact=True,
        tier="primary",
        prefix="€",
        widget_style=KPI_CARD_STYLE,
    )
    w["p4_n_deals"] = num(
        "s_churn_summary",
        "TotalDeals",
        "Churned Deals",
        "#D4504C",
        tier="secondary",
        widget_style=KPI_CARD_STYLE,
    )
    w["p4_sec_trends"] = section_label("Churn Trends, Ownership & Root Cause")
    w["p4_ch_trend"] = rich_chart(
        "s_churn_by_qtr",
        "column",
        "Quarterly Churn Trend",
        ["QuarterLabel"],
        ["ChurnedARR"],
        axis_title="Churned ARR (€)",
        subtitle="Quarter-over-quarter churn — rising bars indicate accelerating customer loss",
    )
    w["p4_tbl_confidence"] = compare_table(
        "s_renewal_confidence",
        "Renewal Semantic Confidence by Owner",
        columns=[
            "ManagerName",
            "OwnerName",
            "OwnerPersona",
            "OwnershipAlignment",
            "MetricCoveragePct",
            "MissingMetricDeals",
            "OwnershipMismatchDeals",
            "AtRiskRenewalValue",
        ],
        subtitle="Owner-level data quality and ownership confidence for renewal math. Low coverage or role mismatch means coach the process before trusting the KPI.",
        format_rules=[
            {
                "type": "threshold",
                "field": "MetricCoveragePct",
                "rules": [
                    {"value": 60, "color": "#D4504C", "operator": "lt"},
                    {"value": 80, "color": "#FF9A3C", "operator": "lt"},
                ],
            },
            {
                "type": "threshold",
                "field": "OwnershipMismatchDeals",
                "rules": [{"value": 1, "color": "#FF9A3C", "operator": "gte"}],
            },
        ],
    )
    w["p4_ch_detail"] = compare_table(
        "s_churn_root_causes",
        "Churn Root Cause Queue",
        columns=[
            "OppName",
            "AccountName",
            "Amount",
            "ChurnPulse",
            "ManagerAsk",
            "QuarterLabel",
            "OwnerName",
        ],
        column_properties={
            "Amount": {"width": 120, "alignment": "right"},
        },
        subtitle="Largest lost renewals with a clear review ask so the team closes the loop on churn causes.",
        format_rules=[
            {
                "type": "threshold",
                "field": "Amount",
                "rules": [
                    {"value": 100000, "color": "#D4504C", "operator": "gte"},
                    {"value": 50000, "color": "#FF9A3C", "operator": "gte"},
                ],
            },
        ],
    )

    # Filters
    w["f_year_w"] = listselector("f_year", "Year")
    w["f_manager_w"] = listselector("f_manager", "CSM Manager")
    w["f_owner_w"] = listselector("f_owner", "CSM Owner")
    w["f_account_w"] = listselector("f_account", "Account")

    return w


# ── Layout ────────────────────────────────────────────────────────────────
def build_layout():
    filt = [
        {"name": "f_year_w", "row": 5, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "f_manager_w", "row": 5, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "f_owner_w", "row": 5, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "f_account_w", "row": 5, "column": 9, "colspan": 3, "rowspan": 2},
    ]

    p1 = (
        [
            {"name": "p1_tab_sales", "row": 1, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p1_tab_csm", "row": 1, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p1_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        ]
        + filt
        + [
            {"name": "p1_n_nrr", "row": 7, "column": 0, "colspan": 3, "rowspan": 4},
            {"name": "p1_n_grr", "row": 7, "column": 3, "colspan": 3, "rowspan": 4},
            {"name": "p1_n_churn", "row": 7, "column": 6, "colspan": 3, "rowspan": 4},
            {"name": "p1_n_ending", "row": 7, "column": 9, "colspan": 3, "rowspan": 4},
            {"name": "p1_b_nrr", "row": 11, "column": 0, "colspan": 6, "rowspan": 4},
            {"name": "p1_b_grr", "row": 11, "column": 6, "colspan": 6, "rowspan": 4},
            {
                "name": "p1_ch_waterfall",
                "row": 15,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
            {
                "name": "p1_ch_yearly",
                "row": 23,
                "column": 0,
                "colspan": 7,
                "rowspan": 8,
            },
            {
                "name": "p1_tbl_actions",
                "row": 23,
                "column": 7,
                "colspan": 5,
                "rowspan": 8,
            },
        ]
    )

    p2 = (
        [
            {"name": "p2_tab_sales", "row": 1, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p2_tab_csm", "row": 1, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p2_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        ]
        + filt
        + [
            {
                "name": "p2_sec_retention",
                "row": 7,
                "column": 0,
                "colspan": 12,
                "rowspan": 2,
            },
            {
                "name": "p2_ch_retention",
                "row": 9,
                "column": 0,
                "colspan": 6,
                "rowspan": 8,
            },
            {
                "name": "p2_ch_expansion",
                "row": 9,
                "column": 6,
                "colspan": 6,
                "rowspan": 8,
            },
            {
                "name": "p2_sec_mix",
                "row": 17,
                "column": 0,
                "colspan": 12,
                "rowspan": 2,
            },
            {
                "name": "p2_ch_quarterly_rev",
                "row": 19,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
        ]
    )

    p3 = (
        [
            {"name": "p3_tab_sales", "row": 1, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p3_tab_csm", "row": 1, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p3_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        ]
        + filt
        + [
            {
                "name": "p3_n_pipeline",
                "row": 7,
                "column": 0,
                "colspan": 4,
                "rowspan": 3,
            },
            {"name": "p3_n_deals", "row": 7, "column": 4, "colspan": 4, "rowspan": 3},
            {
                "name": "p3_n_critical",
                "row": 7,
                "column": 8,
                "colspan": 4,
                "rowspan": 3,
            },
            {
                "name": "p3_sec_risk",
                "row": 10,
                "column": 0,
                "colspan": 12,
                "rowspan": 2,
            },
            {"name": "p3_ch_risk", "row": 12, "column": 0, "colspan": 4, "rowspan": 8},
            {
                "name": "p3_ch_table",
                "row": 12,
                "column": 4,
                "colspan": 8,
                "rowspan": 14,
            },
        ]
    )

    p4 = (
        [
            {"name": "p4_tab_sales", "row": 1, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p4_tab_csm", "row": 1, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p4_hdr", "row": 2, "column": 0, "colspan": 12, "rowspan": 2},
        ]
        + filt
        + [
            {"name": "p4_n_churned", "row": 7, "column": 0, "colspan": 6, "rowspan": 3},
            {"name": "p4_n_deals", "row": 7, "column": 6, "colspan": 6, "rowspan": 3},
            {
                "name": "p4_sec_trends",
                "row": 10,
                "column": 0,
                "colspan": 12,
                "rowspan": 2,
            },
            {"name": "p4_ch_trend", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
            {
                "name": "p4_tbl_confidence",
                "row": 12,
                "column": 6,
                "colspan": 6,
                "rowspan": 7,
            },
            {
                "name": "p4_ch_detail",
                "row": 19,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
        ]
    )

    return {
        "name": "revenue_retention_health",
        "numColumns": 12,
        "pages": [
            pg("summary", "Retention Summary", p1),
            pg("trends", "Trends", p2),
            pg("pipeline", "Renewal Pipeline", p3),
            pg("churn", "Churn Analysis", p4),
        ],
    }


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    with builder_run("Revenue_Retention_Health", __file__) as summary:
        inst, tok = get_auth()
        assert_org_schema(
            inst,
            tok,
            objects=["Opportunity", "User"],
        )

        logger.info("=" * 60)
        logger.info("Building: Revenue Retention & Health Dashboard")
        logger.info("=" * 60)

        logger.info("\n[1/4] Creating dataset...")
        upload_ok, row_count = create_dataset(inst, tok)
        summary.row_count = row_count
        if not upload_ok:
            raise SystemExit("Dataset upload failed")

        logger.info("\n[2/4] Resolving dataset ID...")
        ds_id = get_dataset_id(inst, tok, DS)
        if not ds_id:
            raise SystemExit(f"Could not find dataset {DS}")
        summary.dataset_id = ds_id
        logger.info("  Dataset ID: %s", ds_id)

        logger.info("\n[3/4] Building dashboard state...")
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

        logger.info("\n[4/4] Deploying dashboard...")
        dash_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
        deploy_dashboard(inst, tok, dash_id, state)
        logger.info("  Dashboard ID: %s", dash_id)

        logger.info("\n  Setting record links...")
        set_record_links_xmd(
            inst,
            tok,
            DS,
            [
                {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
                {"field": "OppName", "id_field": "OppId", "label": "Opportunity"},
            ],
        )

        logger.info("\nRevenue Retention & Health dashboard deployed!")
        logger.info(
            "  Open: https://simcorp.lightning.force.com/analytics/dashboard/%s",
            dash_id,
        )


if __name__ == "__main__":
    main()
