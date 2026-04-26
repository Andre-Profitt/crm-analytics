#!/usr/bin/env python3
"""Shared helpers for the Sales Director data-dump pipeline.

Constants, auth, SOQL/SAQL query runners, dashboard fetcher,
director filter builder, and source lineage helpers.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = "config/sales_director_md1_presets.json"

# ---------------------------------------------------------------------------
# Org / API
# ---------------------------------------------------------------------------
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"

# ---------------------------------------------------------------------------
# Forecast type IDs
# ---------------------------------------------------------------------------
FORECAST_TYPES: dict[str, str] = {
    "ACV": "0Db7S000000zDaCSAU",
    "ARR": "0Db7S000000zDaMSAU",
    "QuotaRetirement": "0Db7S000000zDaHSAU",
    "ProductFamilyACV": "0DbQA0000004j8D0AQ",
    "RenewalACV": "0DbQA0000009vrt0AA",
}

# ---------------------------------------------------------------------------
# Forecast periods
# ---------------------------------------------------------------------------
PERIODS: dict[str, str] = {
    "Q1_2026": "0267S000000v3sKQAQ",
    "Q2_2026": "0267S000000v3sLQAQ",
}

# ---------------------------------------------------------------------------
# Dashboard IDs
# ---------------------------------------------------------------------------
D1_DASHBOARD_ID = "01ZTb00000FSP7hMAH"
D2_DASHBOARD_ID = "01ZTb00000FSP9JMAX"

# ---------------------------------------------------------------------------
# CRMA dataset IDs
# ---------------------------------------------------------------------------
CRMA_DATASETS: dict[str, str] = {
    "Revenue_Retention_Health": "0FbTb000001A8DRKA0",
    "Sales_Velocity_Annual": "0FbTb000001BPTxKAO",
    "Forecast_Revenue_Motions": "0FbTb000001A0NxKAK",
    "Pipeline_Opportunity_Operations": "0FbTb000001A0KjKAK",
    "Opp_Mgmt_KPIs": "0FbTb0000019llVKAQ",
}

# ---------------------------------------------------------------------------
# D1 dashboard filter option IDs
# ---------------------------------------------------------------------------
D1_FILTER_OPTS: dict[str, str] = {
    # Industry
    "ind_asset_mgmt": "0ICTb0000007DbdOAE",
    "ind_bank": "0ICTb0000007DbeOAE",
    "ind_insurance": "0ICTb0000007DbfOAE",
    "ind_pension": "0ICTb0000007DbgOAE",
    "ind_wealth": "0ICTb0000007DbhOAE",
    "ind_servicer": "0ICTb0000007DbiOAE",
    "ind_other": "0ICTb0000007DbjOAE",
    # Legal Country
    "lc_canada": "0ICTb0000007DgTOAU",
    "lc_excl_canada": "0ICTb0000007DgUOAU",
    # Sales Region
    "sr_apac": "0ICTb0000007DbnOAE",
    "sr_central_europe": "0ICTb0000007DboOAE",
    "sr_mea": "0ICTb0000007DbpOAE",
    "sr_nam": "0ICTb0000007DbqOAE",
    "sr_northern_europe": "0ICTb0000007DbrOAE",
    "sr_southwestern_europe": "0ICTb0000007DbsOAE",
    "sr_uki": "0ICTb0000007DbtOAE",
    # Account Unit Group
    "aug_sc_nam": "0ICTb0000007Di5OAE",
    "aug_sc_asia": "0ICTb0000007Di6OAE",
    "aug_sc_emea": "0ICTb0000007Di7OAE",
}

# ---------------------------------------------------------------------------
# Director -> D1 dashboard filter params
#
# Each value is a dict ready to be sent as the filter body on the dashboard
# PUT endpoint.  Keys are filter1..filterN; values are either a single
# option ID string or a list of option ID strings (multi-select).
# ---------------------------------------------------------------------------
DIRECTOR_D1_FILTERS: dict[str, dict[str, Any]] = {
    "Jesper Tyrer": {
        "filter1": D1_FILTER_OPTS["sr_apac"],
        "filter2": D1_FILTER_OPTS["aug_sc_asia"],
    },
    "Sarah Pittroff": {
        "filter1": D1_FILTER_OPTS["sr_central_europe"],
        "filter2": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Francois Thaury": {
        "filter1": D1_FILTER_OPTS["sr_southwestern_europe"],
        "filter2": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Dan Peppett": {
        "filter1": D1_FILTER_OPTS["sr_uki"],
        "filter2": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Christian Ebbesen": {
        "filter1": D1_FILTER_OPTS["sr_northern_europe"],
        "filter2": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Mourad Essofi": {
        "filter1": D1_FILTER_OPTS["sr_mea"],
        "filter2": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Megan Miceli": {
        "filter1": D1_FILTER_OPTS["sr_nam"],
        "filter2": D1_FILTER_OPTS["lc_canada"],
        "filter3": D1_FILTER_OPTS["aug_sc_nam"],
    },
    "Patrick Gaughan": {
        "filter1": [
            D1_FILTER_OPTS["ind_asset_mgmt"],
            D1_FILTER_OPTS["ind_bank"],
            D1_FILTER_OPTS["ind_wealth"],
            D1_FILTER_OPTS["ind_servicer"],
            D1_FILTER_OPTS["ind_other"],
        ],
        "filter2": D1_FILTER_OPTS["sr_nam"],
        "filter3": D1_FILTER_OPTS["lc_excl_canada"],
        "filter4": D1_FILTER_OPTS["aug_sc_nam"],
    },
    "Adam Steinhaus": {
        "filter1": [
            D1_FILTER_OPTS["ind_pension"],
            D1_FILTER_OPTS["ind_insurance"],
        ],
        "filter2": D1_FILTER_OPTS["sr_nam"],
        "filter3": D1_FILTER_OPTS["lc_excl_canada"],
        "filter4": D1_FILTER_OPTS["aug_sc_nam"],
    },
}

# ---------------------------------------------------------------------------
# Opportunity field list for SOQL bulk pulls
# ---------------------------------------------------------------------------
OPP_FIELDS = (
    "Id, Name, CurrencyIsoCode, Account.Name, Account.Industry, "
    "Account.BillingCountryCode, "
    "Owner.Name, OwnerId, StageName, CloseDate, "
    "APTS_Opportunity_ARR__c, convertCurrency(APTS_Opportunity_ARR__c) ConvertedARR, "
    "Opportunity_Average_ACV__c, convertCurrency(Opportunity_Average_ACV__c) ConvertedACV, "
    "APTS_Renewal_ACV__c, convertCurrency(APTS_Renewal_ACV__c) ConvertedRenewalACV, "
    "APTS_Forecast_ARR__c, convertCurrency(APTS_Forecast_ARR__c) ConvertedForecastARR, "
    "APTS_Forecast_ACV_AVG__c, convertCurrency(APTS_Forecast_ACV_AVG__c) ConvertedForecastACV, "
    "ForecastCategoryName, Probability, Type, APTS_Opportunity_Sub_Type__c, "
    "PushCount, AgeInDays, LastStageChangeInDays, "
    "LastActivityDate, LastActivityInDays, "
    "Sales_Region__c, Account_Unit_Group__c, Account_Unit__c, "
    "Stage_20_Approval__c, Stage_20_Approval_Date__c, Approval_Status__c, "
    "Risk_Assessment_Level__c, Risk_Assessment_Comment__c, NextStep, "
    "CreatedDate, Calculated_Close_Date__c, "
    "New_Stage_10_created_Date__c, New_Stage_15_Date__c, "
    "New_Stage_20_Date__c, New_Stage_30_Date__c, "
    "New_Stage_40_Date__c, New_Stage_6_Date__c, "
    "New_Stage_7_Date__c, New_Stage_50_Date__c, "
    "Reason_Won_Lost__c, Sub_Reason__c, Lost_to_Competitor__c, Lost_Comments__c, "
    "Contract__c, APTS_Contract_Start_Date__c, APTS_Contract_End_Date__c, "
    "HasOpenActivity, HasOverdueTask, IqScore"
)


# ===================================================================
# Auth
# ===================================================================


def auth() -> tuple[str, str]:
    """Return (accessToken, instanceUrl) from sf CLI."""
    result = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"sf org display failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    payload = json.loads(result.stdout)
    r = payload["result"]
    return r["accessToken"], r["instanceUrl"]


# ===================================================================
# SOQL
# ===================================================================


def soql_query(token: str, base_url: str, query: str) -> list[dict]:
    """Run a SOQL query via REST API, handling pagination."""
    url = f"{base_url}/services/data/{API_VERSION}/query/"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params={"q": query}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("records", [])
    while not data.get("done", True):
        next_url = f"{base_url}{data['nextRecordsUrl']}"
        resp = requests.get(next_url, headers=headers, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
    return records


def soql_query_all(token: str, base_url: str, query: str) -> list[dict]:
    """Run a SOQL queryAll (includes deleted/archived records)."""
    url = f"{base_url}/services/data/{API_VERSION}/queryAll/"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params={"q": query}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("records", [])
    while not data.get("done", True):
        next_url = f"{base_url}{data['nextRecordsUrl']}"
        resp = requests.get(next_url, headers=headers, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
    return records


# ===================================================================
# SAQL / Wave query
# ===================================================================


def wave_query(token: str, base_url: str, saql: str) -> list[dict]:
    """POST a SAQL query to the Wave API and return result records."""
    url = f"{base_url}/services/data/{API_VERSION}/wave/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, json={"query": saql}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", {}).get("records", [])


# ===================================================================
# Dashboard fetch (with optional filter apply)
# ===================================================================


def fetch_dashboard(
    token: str,
    base_url: str,
    dashboard_id: str,
    filters: dict | None = None,
) -> dict:
    """Fetch a CRM Analytics dashboard.

    If *filters* is provided, PUT them first (applies the filter state),
    wait 6 s for the server to recompute, then GET the result.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    dash_url = f"{base_url}/services/data/{API_VERSION}/wave/dashboards/{dashboard_id}"

    if filters is not None:
        put_resp = requests.put(
            dash_url,
            headers=headers,
            json={"state": {"filters": filters}},
            timeout=120,
        )
        put_resp.raise_for_status()
        time.sleep(6)

    resp = requests.get(dash_url, headers=headers, timeout=120)
    resp.raise_for_status()
    return resp.json()


# ===================================================================
# SOQL WHERE builder from preset filters
# ===================================================================

_COLUMN_MAP = {
    "Account.Region__c": "Account.Region__c",
    "ADDRESS1_COUNTRY_CODE": "Account.BillingCountryCode",
    "INDUSTRY": "Account.Industry",
    "Opportunity.Account_Unit_Group__c": "Account_Unit_Group__c",
}


def soql_where(director: dict) -> str:
    """Build a SOQL WHERE clause from a preset director dict.

    Handles:
      - Account.Region__c  (equals / notEqual)
      - INDUSTRY            (comma-separated -> IN list)
      - ADDRESS1_COUNTRY_CODE -> Account.BillingCountryCode (equals / notEqual)
      - Opportunity.Account_Unit_Group__c -> Account_Unit_Group__c (equals)
    """
    clauses: list[str] = []
    for f in director.get("filters", []):
        col = f["column"]
        op = f["operator"]
        val = f["value"]
        sf_field = _COLUMN_MAP.get(col, col)

        if col == "INDUSTRY" and op == "equals":
            # comma-separated picklist -> IN (...)
            values = [v.strip() for v in val.split(",")]
            in_list = ", ".join(f"'{v}'" for v in values)
            clauses.append(f"{sf_field} IN ({in_list})")
        elif op == "equals":
            clauses.append(f"{sf_field} = '{val}'")
        elif op == "notEqual":
            clauses.append(f"{sf_field} != '{val}'")
        else:
            clauses.append(f"{sf_field} = '{val}'")

    return " AND ".join(clauses) if clauses else "Account.Region__c != null"


# ===================================================================
# Source lineage entry
# ===================================================================


def make_source_entry(
    source_id: str,
    source_type: str,
    name: str,
    query_or_endpoint: str,
    record_count: int,
) -> dict:
    """Create a source-lineage metadata entry."""
    return {
        "source_id": source_id,
        "source_type": source_type,
        "name": name,
        "query_or_endpoint": query_or_endpoint,
        "record_count": record_count,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }


# ===================================================================
# Slugify director name
# ===================================================================


def slugify(name: str) -> str:
    """'Dan Peppett' -> 'peppett-dan'  (last-first, lowercase)."""
    parts = name.strip().split()
    if len(parts) < 2:
        return name.lower().strip()
    return f"{parts[-1].lower()}-{parts[0].lower()}"
