# Sales Director Data Dump — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a two-phase extraction pipeline that produces one Excel workbook per MD-1 Sales Director from live Salesforce data (SOQL + Forecasting + D1/D2 dashboards + CRMA/Wave datasets).

**Architecture:** Phase 1 (`extract_director_data.py`) authenticates, queries all data sources, caches raw JSON per director. Phase 2 (`build_director_workbooks.py`) reads cached JSON, computes derived metrics, writes formatted Excel workbooks with 12 tabs. Both phases use shared helpers from `director_data_helpers.py`.

**Tech Stack:** Python 3.13, `openpyxl` 3.1.5, `requests`, `sf` CLI for auth, existing `native_surface_io.py` for REST helpers, existing `md1_presets.py` for director filter definitions.

**Spec:** `docs/specs/2026-04-10-director-data-dump-design.md`
**Knowledge Corpus:** `docs/2026-04-10-dashboard-report-knowledge-corpus.md`

---

## File Structure

```
scripts/
  director_data_helpers.py    # Shared: auth, SOQL builder, SAQL runner, constants
  extract_director_data.py    # Phase 1: extract all data → JSON cache
  build_director_workbooks.py # Phase 2: JSON cache → Excel workbooks
```

All three files are new. No existing files are modified (we reuse `native_surface_io.get_org_session()` and `md1_presets.load_md1_preset_config()` by import).

Output directory: `output/director_data_dumps/{snapshot_date}/`
Cache directory: `output/director_data_dumps/{snapshot_date}/.cache/`

---

### Task 1: Shared Helpers (`director_data_helpers.py`)

**Files:**

- Create: `scripts/director_data_helpers.py`

- [ ] **Step 1: Create the helpers file with constants and auth**

```python
#!/usr/bin/env python3
"""Shared helpers for the Sales Director data dump pipeline."""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "sales_director_md1_presets.json"
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"

# Forecasting type IDs (from org describe)
FORECAST_TYPES = {
    "ACV": "0Db7S000000zDaCSAU",
    "ARR": "0Db7S000000zDaMSAU",
    "QuotaRetirement": "0Db7S000000zDaHSAU",
    "ProductFamilyACV": "0DbQA0000004j8D0AQ",
    "RenewalACV": "0DbQA0000009vrt0AA",
}

# Period IDs (resolved)
PERIODS = {
    "Q1_2026": "0267S000000v3sKQAQ",
    "Q2_2026": "0267S000000v3sLQAQ",
}

# D1 dashboard filter option IDs
D1_FILTER_OPTS = {
    "ind_asset_mgmt": "0ICTb0000007DbdOAE",
    "ind_bank": "0ICTb0000007DbeOAE",
    "ind_insurance": "0ICTb0000007DbfOAE",
    "ind_pension": "0ICTb0000007DbgOAE",
    "ind_wealth": "0ICTb0000007DbhOAE",
    "ind_servicer": "0ICTb0000007DbiOAE",
    "ind_other": "0ICTb0000007DbjOAE",
    "lc_canada": "0ICTb0000007DgTOAU",
    "lc_excl_canada": "0ICTb0000007DgUOAU",
    "sr_apac": "0ICTb0000007DbnOAE",
    "sr_central_europe": "0ICTb0000007DboOAE",
    "sr_mea": "0ICTb0000007DbpOAE",
    "sr_nam": "0ICTb0000007DbqOAE",
    "sr_northern_europe": "0ICTb0000007DbrOAE",
    "sr_southwestern_europe": "0ICTb0000007DbsOAE",
    "sr_uki": "0ICTb0000007DbtOAE",
    "aug_sc_nam": "0ICTb0000007Di5OAE",
    "aug_sc_asia": "0ICTb0000007Di6OAE",
    "aug_sc_emea": "0ICTb0000007Di7OAE",
}

# D1 per-director dashboard filter params
DIRECTOR_D1_FILTERS: dict[str, dict[str, Any]] = {
    "Megan Miceli": {
        "filter2": D1_FILTER_OPTS["lc_canada"],
        "filter3": D1_FILTER_OPTS["sr_nam"],
        "filter4": D1_FILTER_OPTS["aug_sc_nam"],
    },
    "Patrick Gaughan": {
        "filter1": [
            D1_FILTER_OPTS["ind_asset_mgmt"],
            D1_FILTER_OPTS["ind_bank"],
            D1_FILTER_OPTS["ind_wealth"],
            D1_FILTER_OPTS["ind_servicer"],
            D1_FILTER_OPTS["ind_other"],
        ],
        "filter2": D1_FILTER_OPTS["lc_excl_canada"],
        "filter3": D1_FILTER_OPTS["sr_nam"],
        "filter4": D1_FILTER_OPTS["aug_sc_nam"],
    },
    "Jesper Tyrer": {
        "filter3": D1_FILTER_OPTS["sr_apac"],
        "filter4": D1_FILTER_OPTS["aug_sc_asia"],
    },
    "Sarah Pittroff": {
        "filter3": D1_FILTER_OPTS["sr_central_europe"],
        "filter4": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Francois Thaury": {
        "filter3": D1_FILTER_OPTS["sr_southwestern_europe"],
        "filter4": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Dan Peppett": {
        "filter3": D1_FILTER_OPTS["sr_uki"],
        "filter4": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Christian Ebbesen": {
        "filter3": D1_FILTER_OPTS["sr_northern_europe"],
        "filter4": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Mourad Essofi": {
        "filter3": D1_FILTER_OPTS["sr_mea"],
        "filter4": D1_FILTER_OPTS["aug_sc_emea"],
    },
    "Adam Steinhaus": {
        "filter1": [D1_FILTER_OPTS["ind_pension"], D1_FILTER_OPTS["ind_insurance"]],
        "filter2": D1_FILTER_OPTS["lc_excl_canada"],
        "filter3": D1_FILTER_OPTS["sr_nam"],
        "filter4": D1_FILTER_OPTS["aug_sc_nam"],
    },
}

# Dashboard IDs
D1_DASHBOARD_ID = "01ZTb00000FSP7hMAH"
D2_DASHBOARD_ID = "01ZTb00000FSP9JMAX"

# CRMA dataset IDs
CRMA_DATASETS = {
    "Revenue_Retention_Health": "0FbTb000001A8DRKA0",
    "Sales_Velocity_Annual": "0FbTb000001BPTxKAO",
    "Forecast_Revenue_Motions": "0FbTb000001A0NxKAK",
    "Pipeline_Opportunity_Operations": "0FbTb000001A0KjKAK",
    "Opp_Mgmt_KPIs": "0FbTb0000019llVKAQ",
}

# Opportunity fields to extract (verified against org describe)
OPP_FIELDS = (
    "Id, Name, Account.Name, Account.Industry, Account.BillingCountryCode, "
    "Owner.Name, OwnerId, StageName, CloseDate, "
    "APTS_Opportunity_ARR__c, Opportunity_Average_ACV__c, APTS_Forecast_ARR__c, APTS_Forecast_ACV_AVG__c, "
    "ForecastCategoryName, Probability, Type, APTS_Opportunity_Sub_Type__c, "
    "PushCount, AgeInDays, LastStageChangeInDays, LastActivityDate, LastActivityInDays, "
    "Sales_Director_Book__c, Sales_Region__c, Account_Unit_Group__c, "
    "Stage_20_Approval__c, Stage_20_Approval_Date__c, Approval_Status__c, "
    "Risk_Assessment_Level__c, Risk_Assessment_Comment__c, "
    "NextStep, CreatedDate, Calculated_Close_Date__c, "
    "New_Stage_10_created_Date__c, New_Stage_15_Date__c, New_Stage_20_Date__c, "
    "New_Stage_30_Date__c, New_Stage_40_Date__c, New_Stage_6_Date__c, "
    "New_Stage_7_Date__c, New_Stage_50_Date__c, "
    "Reason_Won_Lost__c, Sub_Reason__c, Lost_to_Competitor__c, Lost_Comments__c, "
    "Contract__c, APTS_Contract_Start_Date__c, APTS_Contract_End_Date__c, "
    "HasOpenActivity, HasOverdueTask, IqScore"
)


def auth() -> tuple[str, str]:
    """Return (access_token, instance_url) via sf CLI."""
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True, text=True, check=True,
    )
    idx = r.stdout.find("{")
    d = json.loads(r.stdout[idx:])["result"]
    return d["accessToken"], d["instanceUrl"].rstrip("/")


def soql_query(token: str, base_url: str, query: str) -> list[dict[str, Any]]:
    """Run a SOQL query via REST API, handle pagination."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{base_url}/services/data/{API_VERSION}/query/"
    resp = requests.get(url, headers=headers, params={"q": query}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("records", [])
    while not data.get("done", True):
        next_url = f"{base_url}{data['nextRecordsUrl']}"
        resp = requests.get(next_url, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
    return records


def soql_query_all(token: str, base_url: str, query: str) -> list[dict[str, Any]]:
    """Run SOQL via queryAll (includes deleted/archived)."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{base_url}/services/data/{API_VERSION}/queryAll/"
    resp = requests.get(url, headers=headers, params={"q": query}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("records", [])
    while not data.get("done", True):
        next_url = f"{base_url}{data['nextRecordsUrl']}"
        resp = requests.get(next_url, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
    return records


def wave_query(token: str, base_url: str, saql: str) -> list[dict[str, Any]]:
    """Run a SAQL query via Wave API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    url = f"{base_url}/services/data/{API_VERSION}/wave/query"
    resp = requests.post(url, headers=headers, json={"query": saql}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", {}).get("records", [])


def fetch_dashboard(token: str, base_url: str, dashboard_id: str,
                    filters: dict[str, Any] | None = None) -> dict[str, Any]:
    """Fetch a SF native dashboard, optionally with filters applied via PUT then GET."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{base_url}/services/data/{API_VERSION}/analytics/dashboards/{dashboard_id}"
    if filters:
        requests.put(url, headers=headers, params=filters, timeout=60)
        time.sleep(6)
    resp = requests.get(url, headers=headers, params=(filters or {}), timeout=60)
    resp.raise_for_status()
    return resp.json()


def soql_where(director: dict[str, Any]) -> str:
    """Build SOQL WHERE fragment from a director's preset filters."""
    clauses: list[str] = []
    for f in director.get("filters", []):
        col = f.get("column", "")
        op = f.get("operator", "equals")
        val = f.get("value", "")
        if col == "Account.Region__c":
            if op == "equals":
                clauses.append(f"Account.Region__c = '{val}'")
            elif op == "notEqual":
                clauses.append(f"Account.Region__c != '{val}'")
        elif col == "INDUSTRY":
            if op == "equals":
                if "," in val:
                    in_list = ", ".join(f"'{v.strip()}'" for v in val.split(","))
                    clauses.append(f"Account.Industry IN ({in_list})")
                else:
                    clauses.append(f"Account.Industry = '{val}'")
        elif col == "ADDRESS1_COUNTRY_CODE":
            if op == "equals":
                clauses.append(f"Account.BillingCountryCode = '{val}'")
            elif op == "notEqual":
                clauses.append(f"Account.BillingCountryCode != '{val}'")
        elif col == "Opportunity.Account_Unit_Group__c":
            if op == "equals":
                clauses.append(f"Account_Unit_Group__c = '{val}'")
    return " AND ".join(clauses) if clauses else "Account.Region__c != null"


def make_source_entry(source_id: str, source_type: str, name: str,
                      query_or_endpoint: str, record_count: int) -> dict[str, Any]:
    """Create a source registry entry for the Sources & Lineage tab."""
    return {
        "source_id": source_id,
        "source_type": source_type,
        "name": name,
        "query_or_endpoint": query_or_endpoint,
        "record_count": record_count,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }


def slugify(name: str) -> str:
    """Convert director name to slug: 'Dan Peppett' -> 'peppett-dan'."""
    parts = name.lower().strip().split()
    return f"{parts[-1]}-{parts[0]}" if len(parts) >= 2 else parts[0]
```

- [ ] **Step 2: Verify imports work**

Run: `cd /Users/test/crm-analytics && python3 -c "import scripts.director_data_helpers as h; t,u = h.auth(); print(f'OK: {u}')"`
Expected: `OK: https://simcorp.my.salesforce.com`

- [ ] **Step 3: Commit**

```bash
git add scripts/director_data_helpers.py
git commit -m "feat: add shared helpers for director data dump pipeline"
```

---

### Task 2: Phase 1 — Extract Script (`extract_director_data.py`)

**Files:**

- Create: `scripts/extract_director_data.py`

This is the largest task. The script authenticates, runs all queries for all 9 directors, and writes raw JSON to a cache directory. Each data source gets its own JSON file per director.

- [ ] **Step 1: Create the extract script skeleton with CLI and per-director loop**

```python
#!/usr/bin/env python3
"""Phase 1: Extract all Salesforce data for Sales Director data dump.

Queries SOQL, Forecasting objects, D1/D2 dashboards, and CRMA datasets.
Writes raw JSON to a cache directory per director.

Usage:
    python3 scripts/extract_director_data.py --all
    python3 scripts/extract_director_data.py --director "Dan Peppett"
    python3 scripts/extract_director_data.py --all --snapshot-date 2026-04-10
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import director_data_helpers as h
from md1_presets import load_md1_preset_config

OUTPUT_ROOT = h.REPO_ROOT / "output" / "director_data_dumps"


def save_json(cache_dir: Path, filename: str, data: Any) -> Path:
    path = cache_dir / filename
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    return path


# ── SOQL Extraction ──────────────────────────────────────────────────────────

def extract_soql(token: str, base_url: str, director: dict[str, Any],
                 cache_dir: Path, sources: list[dict]) -> None:
    """Run all SOQL queries for one director."""
    where = h.soql_where(director)
    territory = director["territory"]
    print(f"  SOQL: Open pipeline...")

    # S1: Open Pipeline
    q = f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE IsClosed = false AND {where} ORDER BY APTS_Opportunity_ARR__c DESC NULLS LAST"
    recs = h.soql_query(token, base_url, q)
    save_json(cache_dir, "soql_open_pipeline.json", recs)
    sources.append(h.make_source_entry("S1", "SOQL", "Open Pipeline", q[:200], len(recs)))
    print(f"    Open pipeline: {len(recs)} records")

    # S2: Won This Quarter
    print(f"  SOQL: Won this quarter...")
    q = f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE StageName = '8 - Won' AND CloseDate = THIS_QUARTER AND {where} ORDER BY APTS_Opportunity_ARR__c DESC NULLS LAST"
    recs = h.soql_query(token, base_url, q)
    save_json(cache_dir, "soql_won_this_quarter.json", recs)
    sources.append(h.make_source_entry("S2", "SOQL", "Won This Quarter", q[:200], len(recs)))
    print(f"    Won this quarter: {len(recs)} records")

    # S3: Lost This Quarter
    print(f"  SOQL: Lost this quarter...")
    q = f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE StageName = '0 - Lost' AND CloseDate = THIS_QUARTER AND {where} ORDER BY APTS_Opportunity_ARR__c DESC NULLS LAST"
    recs = h.soql_query(token, base_url, q)
    save_json(cache_dir, "soql_lost_this_quarter.json", recs)
    sources.append(h.make_source_entry("S3", "SOQL", "Lost This Quarter", q[:200], len(recs)))
    print(f"    Lost this quarter: {len(recs)} records")

    # S4: Won Q1
    print(f"  SOQL: Won Q1...")
    q = f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE StageName = '8 - Won' AND CloseDate >= 2026-01-01 AND CloseDate <= 2026-03-31 AND {where} ORDER BY APTS_Opportunity_ARR__c DESC NULLS LAST"
    recs = h.soql_query(token, base_url, q)
    save_json(cache_dir, "soql_won_q1.json", recs)
    sources.append(h.make_source_entry("S4", "SOQL", "Won Q1", q[:200], len(recs)))
    print(f"    Won Q1: {len(recs)} records")

    # S5: Lost Q1
    print(f"  SOQL: Lost Q1...")
    q = f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE StageName = '0 - Lost' AND CloseDate >= 2026-01-01 AND CloseDate <= 2026-03-31 AND {where} ORDER BY APTS_Opportunity_ARR__c DESC NULLS LAST"
    recs = h.soql_query(token, base_url, q)
    save_json(cache_dir, "soql_lost_q1.json", recs)
    sources.append(h.make_source_entry("S5", "SOQL", "Lost Q1", q[:200], len(recs)))
    print(f"    Lost Q1: {len(recs)} records")

    # S6: Pushed Deals
    print(f"  SOQL: Pushed deals...")
    q = f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE IsClosed = false AND PushCount > 0 AND {where} ORDER BY PushCount DESC"
    recs = h.soql_query(token, base_url, q)
    save_json(cache_dir, "soql_pushed_deals.json", recs)
    sources.append(h.make_source_entry("S6", "SOQL", "Pushed Deals", q[:200], len(recs)))
    print(f"    Pushed deals: {len(recs)} records")

    # S7: New Pipeline Created This Quarter
    print(f"  SOQL: New pipeline this quarter...")
    q = f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE CreatedDate = THIS_QUARTER AND {where} ORDER BY APTS_Opportunity_ARR__c DESC NULLS LAST"
    recs = h.soql_query(token, base_url, q)
    save_json(cache_dir, "soql_new_pipeline.json", recs)
    sources.append(h.make_source_entry("S7", "SOQL", "New Pipeline This Quarter", q[:200], len(recs)))
    print(f"    New pipeline: {len(recs)} records")

    # S8: Forecast Categories This Quarter
    print(f"  SOQL: Forecast categories...")
    q = f"SELECT ForecastCategoryName, SUM(APTS_Opportunity_ARR__c) arr, SUM(Opportunity_Average_ACV__c) acv, COUNT(Id) ct FROM Opportunity WHERE IsClosed = false AND CloseDate = THIS_QUARTER AND {where} GROUP BY ForecastCategoryName"
    recs = h.soql_query(token, base_url, q)
    save_json(cache_dir, "soql_forecast_categories.json", recs)
    sources.append(h.make_source_entry("S8", "SOQL", "Forecast Categories Q2", q[:200], len(recs)))
    print(f"    Forecast categories: {len(recs)} records")


# ── Forecasting Module ───────────────────────────────────────────────────────

def extract_forecasting(token: str, base_url: str, cache_dir: Path,
                        sources: list[dict]) -> None:
    """Extract ForecastingItem and ForecastingFact for Q1 and Q2."""
    print(f"  Forecasting: Items and facts...")

    for period_label, period_id in h.PERIODS.items():
        # ForecastingItem by type
        for type_label, type_id in h.FORECAST_TYPES.items():
            q = (
                f"SELECT Id, OwnerId, Owner.Name, ForecastCategoryName, "
                f"ForecastingItemCategory, ForecastAmount, AmountWithoutAdjustments, "
                f"AmountWithoutManagerAdjustment, HasAdjustment, OwnerOnlyAmount "
                f"FROM ForecastingItem "
                f"WHERE PeriodId = '{period_id}' AND ForecastingTypeId = '{type_id}'"
            )
            recs = h.soql_query(token, base_url, q)
            fname = f"forecast_item_{period_label}_{type_label}.json"
            save_json(cache_dir, fname, recs)
            sid = f"F_{period_label}_{type_label}"
            sources.append(h.make_source_entry(sid, "ForecastingItem", f"{period_label} {type_label}", q[:200], len(recs)))
            print(f"    {period_label} {type_label}: {len(recs)} items")

        # ForecastingFact
        q = (
            f"SELECT Id, OpportunityId, ForecastCategoryName, OwnerId, Owner.Name "
            f"FROM ForecastingFact WHERE PeriodId = '{period_id}'"
        )
        recs = h.soql_query(token, base_url, q)
        save_json(cache_dir, f"forecast_fact_{period_label}.json", recs)
        sources.append(h.make_source_entry(f"FF_{period_label}", "ForecastingFact", f"{period_label} Facts", q[:200], len(recs)))
        print(f"    {period_label} facts: {len(recs)} records")


# ── OpportunityFieldHistory ──────────────────────────────────────────────────

def extract_field_history(token: str, base_url: str, cache_dir: Path,
                          sources: list[dict]) -> None:
    """Extract Q1 field changes (CloseDate, ForecastCategory, Stage)."""
    print(f"  FieldHistory: Q1 changes...")

    for field in ["CloseDate", "ForecastCategoryName", "StageName"]:
        q = (
            f"SELECT OpportunityId, Opportunity.Name, Opportunity.Account.Name, "
            f"Opportunity.Owner.Name, Opportunity.APTS_Opportunity_ARR__c, "
            f"Opportunity.Sales_Director_Book__c, Opportunity.StageName, "
            f"Opportunity.CloseDate, Opportunity.ForecastCategoryName, "
            f"OldValue, NewValue, CreatedDate "
            f"FROM OpportunityFieldHistory "
            f"WHERE Field = '{field}' "
            f"AND CreatedDate >= 2026-01-01T00:00:00Z "
            f"AND CreatedDate < 2026-04-01T00:00:00Z "
            f"ORDER BY CreatedDate DESC"
        )
        recs = h.soql_query_all(token, base_url, q)
        save_json(cache_dir, f"field_history_{field}.json", recs)
        sources.append(h.make_source_entry(f"FH_{field}", "OpportunityFieldHistory", f"Q1 {field} Changes", q[:200], len(recs)))
        print(f"    {field}: {len(recs)} changes")


# ── D1 Dashboard (per director) ─────────────────────────────────────────────

def extract_d1(token: str, base_url: str, director_name: str,
               cache_dir: Path, sources: list[dict]) -> None:
    """Fetch D1 dashboard filtered for this director."""
    filters = h.DIRECTOR_D1_FILTERS.get(director_name, {})
    if not filters:
        print(f"  D1: No filter mapping for {director_name}, skipping")
        return
    print(f"  D1: Fetching filtered dashboard...")
    data = h.fetch_dashboard(token, base_url, h.D1_DASHBOARD_ID, filters)
    save_json(cache_dir, "d1_dashboard.json", data)
    comp_count = len(data.get("componentData", []))
    sources.append(h.make_source_entry("D1", "Dashboard", "D1 Sales Directors Monthly", f"PUT+GET {h.D1_DASHBOARD_ID} with {len(filters)} filters", comp_count))
    print(f"    D1: {comp_count} components")


# ── D2 Dashboard (global) ───────────────────────────────────────────────────

def extract_d2(token: str, base_url: str, cache_dir: Path,
               sources: list[dict]) -> None:
    """Fetch D2 dashboard (no per-director filters)."""
    print(f"  D2: Fetching global dashboard...")
    data = h.fetch_dashboard(token, base_url, h.D2_DASHBOARD_ID)
    save_json(cache_dir, "d2_dashboard.json", data)
    comp_count = len(data.get("componentData", []))
    sources.append(h.make_source_entry("D2", "Dashboard", "D2 Sales Ops Quarterly KPI", f"GET {h.D2_DASHBOARD_ID}", comp_count))
    print(f"    D2: {comp_count} components")


# ── CRMA / Wave Datasets ────────────────────────────────────────────────────

def extract_crma(token: str, base_url: str, director: dict[str, Any],
                 cache_dir: Path, sources: list[dict]) -> None:
    """Extract key metrics from CRMA datasets via SAQL."""
    territory = director["territory"]
    book = territory  # Sales_Director_Book__c value matches territory name
    print(f"  CRMA: Querying Wave datasets...")

    # Revenue Retention & Health — GRR, NRR, waterfall, churn, renewal pipeline
    saql = (
        f'q = load "{h.CRMA_DATASETS["Revenue_Retention_Health"]}";'
        f'q = filter q by \'RecordType\' == "account_year_metric";'
        f'q = group q by \'FiscalYear\';'
        f'q = foreach q generate \'FiscalYear\', '
        f'sum(\'StartingARR\') as \'StartingARR\', '
        f'sum(\'RenewalWonARR\') as \'RenewalWonARR\', '
        f'sum(\'ExpansionARR\') as \'ExpansionARR\', '
        f'sum(\'ChurnARR\') as \'ChurnARR\', '
        f'sum(\'EndingARR\') as \'EndingARR\', '
        f'sum(\'NewLogoARR\') as \'NewLogoARR\';'
        f'q = order q by \'FiscalYear\' desc;'
        f'q = limit q 5;'
    )
    try:
        recs = h.wave_query(token, base_url, saql)
        save_json(cache_dir, "crma_retention_metrics.json", recs)
        sources.append(h.make_source_entry("C1", "CRMA/SAQL", "Revenue Retention Metrics", "Revenue_Retention_Health account_year_metric", len(recs)))
        print(f"    Retention metrics: {len(recs)} years")
    except Exception as e:
        print(f"    Retention metrics: FAILED ({e})")
        save_json(cache_dir, "crma_retention_metrics.json", [])

    # Renewal pipeline by risk level
    saql = (
        f'q = load "{h.CRMA_DATASETS["Revenue_Retention_Health"]}";'
        f'q = filter q by \'RecordType\' == "opp_detail" && \'IsClosed\' == "false" && \'OppType\' == "Renewal";'
        f'q = group q by \'RiskLevel\';'
        f'q = foreach q generate \'RiskLevel\', sum(\'RecurringValue\') as \'ARR\', count() as \'DealCount\';'
        f'q = order q by \'ARR\' desc;'
    )
    try:
        recs = h.wave_query(token, base_url, saql)
        save_json(cache_dir, "crma_renewal_risk.json", recs)
        sources.append(h.make_source_entry("C2", "CRMA/SAQL", "Renewal Pipeline by Risk", "Revenue_Retention_Health opp_detail Renewal", len(recs)))
        print(f"    Renewal risk levels: {len(recs)} buckets")
    except Exception as e:
        print(f"    Renewal risk: FAILED ({e})")
        save_json(cache_dir, "crma_renewal_risk.json", [])

    # Sales Velocity — stage conversion, win rates
    saql = (
        f'q = load "{h.CRMA_DATASETS["Sales_Velocity_Annual"]}";'
        f'q = filter q by \'RowType\' == "full_pipe_stage" && \'FiscalYear\' == "2026";'
        f'q = group q by \'StageLabel\';'
        f'q = foreach q generate \'StageLabel\', '
        f'sum(\'EnteredCount\') as \'Entered\', '
        f'sum(\'AdvancedCount\') as \'Advanced\', '
        f'sum(\'QualifiedValueEUR\') as \'ValueEUR\';'
        f'q = order q by \'StageLabel\' asc;'
    )
    try:
        recs = h.wave_query(token, base_url, saql)
        save_json(cache_dir, "crma_stage_conversion.json", recs)
        sources.append(h.make_source_entry("C3", "CRMA/SAQL", "Stage Conversion 2026", "Sales_Velocity_Annual full_pipe_stage", len(recs)))
        print(f"    Stage conversion: {len(recs)} stages")
    except Exception as e:
        print(f"    Stage conversion: FAILED ({e})")
        save_json(cache_dir, "crma_stage_conversion.json", [])

    # Pipeline Opportunity Operations — per-opp hygiene fields
    saql = (
        f'q = load "{h.CRMA_DATASETS["Pipeline_Opportunity_Operations"]}";'
        f'q = filter q by \'IsClosed\' == "false";'
        f'q = foreach q generate \'OpportunityId\', \'PastDueCount\', \'StaleCount\', '
        f'\'BackwardMoveCount\', \'PushCount\', \'DaysInCurrentStage\', \'ARR\', '
        f'\'OwnerName\', \'SalesRegion\', \'StageName\';'
        f'q = limit q 2000;'
    )
    try:
        recs = h.wave_query(token, base_url, saql)
        save_json(cache_dir, "crma_pipeline_ops.json", recs)
        sources.append(h.make_source_entry("C4", "CRMA/SAQL", "Pipeline Opp Operations", "Pipeline_Opportunity_Operations detail", len(recs)))
        print(f"    Pipeline ops: {len(recs)} opps")
    except Exception as e:
        print(f"    Pipeline ops: FAILED ({e})")
        save_json(cache_dir, "crma_pipeline_ops.json", [])

    # Opp Mgmt KPIs — velocity by type
    saql = (
        f'q = load "{h.CRMA_DATASETS["Opp_Mgmt_KPIs"]}";'
        f'q = filter q by \'IsWon\' == "true" && \'FiscalYear\' == "2026";'
        f'q = group q by \'Type\';'
        f'q = foreach q generate \'Type\', '
        f'avg(\'DaysToClose\') as \'AvgDaysToClose\', '
        f'count() as \'DealCount\', '
        f'sum(\'ARR\') as \'TotalARR\';'
        f'q = order q by \'DealCount\' desc;'
    )
    try:
        recs = h.wave_query(token, base_url, saql)
        save_json(cache_dir, "crma_velocity_by_type.json", recs)
        sources.append(h.make_source_entry("C5", "CRMA/SAQL", "Velocity by Opp Type", "Opp_Mgmt_KPIs won 2026", len(recs)))
        print(f"    Velocity by type: {len(recs)} types")
    except Exception as e:
        print(f"    Velocity by type: FAILED ({e})")
        save_json(cache_dir, "crma_velocity_by_type.json", [])


# ── Main ─────────────────────────────────────────────────────────────────────

def extract_all_for_director(
    director: dict[str, Any],
    token: str,
    base_url: str,
    output_dir: Path,
    d2_cache: dict | None = None,
) -> Path:
    """Run full extraction for one director. Returns cache_dir path."""
    name = director["name"]
    territory = director["territory"]
    slug = h.slugify(name)
    cache_dir = output_dir / ".cache" / slug
    cache_dir.mkdir(parents=True, exist_ok=True)

    sources: list[dict] = []
    print(f"\n{'='*60}")
    print(f"  {name} ({territory})")
    print(f"{'='*60}")

    # SOQL (per-director)
    extract_soql(token, base_url, director, cache_dir, sources)

    # D1 (per-director)
    extract_d1(token, base_url, name, cache_dir, sources)

    # D2 (global — shared across all directors, extracted once)
    if d2_cache is not None:
        save_json(cache_dir, "d2_dashboard.json", d2_cache)
        sources.append(h.make_source_entry("D2", "Dashboard", "D2 (cached)", "shared", 15))
    else:
        extract_d2(token, base_url, cache_dir, sources)

    # Forecasting (global — but we save per director for self-contained cache)
    extract_forecasting(token, base_url, cache_dir, sources)

    # Field History (global — filtered per director client-side in Phase 2)
    extract_field_history(token, base_url, cache_dir, sources)

    # CRMA (filtered by region where possible)
    extract_crma(token, base_url, director, cache_dir, sources)

    # Save source registry
    save_json(cache_dir, "_sources.json", sources)
    print(f"  Sources: {len(sources)} registered")

    return cache_dir


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--director", help="Single director name")
    parser.add_argument("--all", action="store_true", help="Extract for all 9 directors")
    parser.add_argument("--snapshot-date", default=date.today().isoformat())
    args = parser.parse_args()

    if not args.director and not args.all:
        print("Must specify --director NAME or --all", file=sys.stderr)
        return 2

    token, base_url = h.auth()
    config = load_md1_preset_config(h.CONFIG_PATH)
    output_dir = OUTPUT_ROOT / args.snapshot_date

    if args.director:
        targets = [p for p in config.presets if args.director.lower() in p.name.lower()]
        if not targets:
            print(f"No preset matched: {args.director}", file=sys.stderr)
            return 1
        targets = [{"name": t.name, "territory": t.territory, "filters": [dict(f) for f in t.filters]} for t in targets]
    else:
        targets = [{"name": p.name, "territory": p.territory, "filters": [dict(f) for f in p.filters]} for p in config.presets]

    # Extract D2 once (global, no per-director filters)
    print("Extracting D2 (global)...")
    d2_data = h.fetch_dashboard(token, base_url, h.D2_DASHBOARD_ID)

    # Extract forecasting once (global)
    global_cache = output_dir / ".cache" / "_global"
    global_cache.mkdir(parents=True, exist_ok=True)
    global_sources: list[dict] = []
    print("Extracting Forecasting (global)...")
    extract_forecasting(token, base_url, global_cache, global_sources)
    print("Extracting Field History (global)...")
    extract_field_history(token, base_url, global_cache, global_sources)

    # Per-director extraction
    for director in targets:
        start = time.time()
        cache_dir = extract_all_for_director(
            director, token, base_url, output_dir, d2_cache=d2_data,
        )
        # Copy global forecasting + field history into director cache
        for f in global_cache.glob("*.json"):
            if not (cache_dir / f.name).exists():
                (cache_dir / f.name).write_text(f.read_text())
        elapsed = time.time() - start
        print(f"  Done in {elapsed:.1f}s")

    print(f"\nCache written to: {output_dir / '.cache'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Test with one director**

Run: `cd /Users/test/crm-analytics && python3 scripts/extract_director_data.py --director "Dan Peppett" --snapshot-date 2026-04-10`
Expected: Cache files written to `output/director_data_dumps/2026-04-10/.cache/peppett-dan/`, each source logged with record count.

- [ ] **Step 3: Verify cache contents**

Run: `ls output/director_data_dumps/2026-04-10/.cache/peppett-dan/ | wc -l`
Expected: ~25+ JSON files (8 SOQL + 12 forecast items + 2 forecast facts + 3 field history + 1 D1 + 1 D2 + 5 CRMA + 1 sources)

Run: `python3 -c "import json; d=json.load(open('output/director_data_dumps/2026-04-10/.cache/peppett-dan/_sources.json')); print(f'{len(d)} sources registered')"`
Expected: 25+ sources registered

- [ ] **Step 4: Commit**

```bash
git add scripts/extract_director_data.py
git commit -m "feat: add Phase 1 extract script for director data dump"
```

---

### Task 3: Phase 2 — Build Workbooks (`build_director_workbooks.py`)

**Files:**

- Create: `scripts/build_director_workbooks.py`

This script reads the JSON cache and builds Excel workbooks. It's long but mechanical — each tab is a function that reads specific JSON files and writes to a worksheet.

- [ ] **Step 1: Create the workbook builder with the Scorecard + Pipeline Detail tabs**

Start with 2 tabs to validate the approach, then add remaining tabs incrementally.

```python
#!/usr/bin/env python3
"""Phase 2: Build Excel workbooks from cached JSON data.

Reads the JSON cache produced by extract_director_data.py and generates
one Excel workbook per director with 12 tabs.

Usage:
    python3 scripts/build_director_workbooks.py --all --snapshot-date 2026-04-10
    python3 scripts/build_director_workbooks.py --director "Dan Peppett" --snapshot-date 2026-04-10
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side, numbers
from openpyxl.utils import get_column_letter

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import director_data_helpers as h
from md1_presets import load_md1_preset_config

OUTPUT_ROOT = h.REPO_ROOT / "output" / "director_data_dumps"

# Style constants
HEADER_FILL = PatternFill(start_color="003E52", end_color="003E52", fill_type="solid")
HEADER_FONT = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
DATA_FONT = Font(name="Calibri", size=10)
TITLE_FONT = Font(name="Calibri", size=14, bold=True, color="003E52")
KPI_FONT = Font(name="Calibri", size=12, bold=True, color="003E52")
RED_FILL = PatternFill(start_color="FDE8E8", end_color="FDE8E8", fill_type="solid")
AMBER_FILL = PatternFill(start_color="FFF3E0", end_color="FFF3E0", fill_type="solid")
GREEN_FILL = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
THIN_BORDER = Border(
    bottom=Side(style="thin", color="D7E2E8"),
)


def load_cache(cache_dir: Path, filename: str) -> Any:
    path = cache_dir / filename
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def write_header_row(ws, row: int, headers: list[str]) -> None:
    for col, h_text in enumerate(headers, 1):
        cell = ws.cell(row=row, column=col, value=h_text)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="left", vertical="center")


def auto_width(ws, min_width: int = 10, max_width: int = 40) -> None:
    for col_cells in ws.columns:
        max_len = min_width
        col_letter = get_column_letter(col_cells[0].column)
        for cell in col_cells[:100]:
            if cell.value:
                max_len = max(max_len, min(len(str(cell.value)) + 2, max_width))
        ws.column_dimensions[col_letter].width = max_len


def safe_num(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_str(val: Any, max_len: int = 100) -> str:
    if val is None:
        return ""
    s = str(val)
    return s[:max_len] if len(s) > max_len else s


def nested_get(d: dict, *keys: str, default: Any = None) -> Any:
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k, default)
        else:
            return default
    return d


# ── Tab Builders ─────────────────────────────────────────────────────────────

def build_scorecard(wb: Workbook, cache_dir: Path, director: dict) -> None:
    ws = wb.active
    ws.title = "Scorecard"
    territory = director["territory"]

    ws.cell(row=1, column=1, value=f"Sales Director Scorecard — {director['name']} ({territory})").font = TITLE_FONT
    ws.cell(row=2, column=1, value=f"Snapshot: {date.today().isoformat()}").font = DATA_FONT
    ws.merge_cells("A1:D1")

    pipeline = load_cache(cache_dir, "soql_open_pipeline.json")
    won_q = load_cache(cache_dir, "soql_won_this_quarter.json")
    lost_q = load_cache(cache_dir, "soql_lost_this_quarter.json")
    pushed = load_cache(cache_dir, "soql_pushed_deals.json")
    new_pipe = load_cache(cache_dir, "soql_new_pipeline.json")

    # Compute KPIs
    total_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in pipeline)
    deal_count = len(pipeline)
    avg_deal = total_arr / deal_count if deal_count else 0
    weighted = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) * safe_num(r.get("Probability", 0)) / 100 for r in pipeline)
    new_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in new_pipe)
    won_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in won_q)
    won_ct = len(won_q)
    lost_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in lost_q)
    lost_ct = len(lost_q)
    win_rate_ct = (won_ct / (won_ct + lost_ct) * 100) if (won_ct + lost_ct) else 0
    win_rate_arr = (won_arr / (won_arr + lost_arr) * 100) if (won_arr + lost_arr) else 0

    stale_30 = [r for r in pipeline if safe_num(r.get("LastActivityInDays"), 0) > 30]
    stale_60_1m = [r for r in pipeline if safe_num(r.get("LastActivityInDays"), 0) > 60 and safe_num(r.get("APTS_Forecast_ARR__c")) >= 1_000_000]
    pushed_5 = [r for r in pushed if safe_num(r.get("PushCount")) >= 5]
    overdue = [r for r in pipeline if r.get("CloseDate") and r["CloseDate"] < date.today().isoformat()]
    aging_365 = [r for r in pipeline if safe_num(r.get("AgeInDays")) > 365]

    approval_3plus = [r for r in pipeline if r.get("StageName", "") >= "3"]
    approved = [r for r in approval_3plus if r.get("Stage_20_Approval__c") is True]
    missing_approval = [r for r in approval_3plus if r.get("Stage_20_Approval__c") is not True and r.get("Type") == "Land"]

    row = 4
    kpis = [
        ("PIPELINE HEALTH", None),
        ("Total Open Pipeline ARR", f"EUR {total_arr:,.0f}"),
        ("Deal Count (Open)", f"{deal_count:,}"),
        ("Average Deal Size", f"EUR {avg_deal:,.0f}"),
        ("Weighted Pipeline", f"EUR {weighted:,.0f}"),
        ("Pipeline Coverage Ratio", "— (awaiting quota targets)"),
        ("New Pipeline Created This Quarter", f"EUR {new_arr:,.0f}"),
        ("", ""),
        ("EXECUTION", None),
        ("Won This Quarter", f"{won_ct} deals, EUR {won_arr:,.0f}"),
        ("Lost This Quarter", f"{lost_ct} deals, EUR {lost_arr:,.0f}"),
        ("Win Rate (by count)", f"{win_rate_ct:.1f}%"),
        ("Win Rate (by ARR)", f"{win_rate_arr:.1f}%"),
        ("", ""),
        ("RISK", None),
        ("Stale Deals (30d no activity)", f"{len(stale_30)} deals, EUR {sum(safe_num(r.get('APTS_Opportunity_ARR__c')) for r in stale_30):,.0f}"),
        ("High-Value Stale (60d, EUR 1M+)", f"{len(stale_60_1m)} deals, EUR {sum(safe_num(r.get('APTS_Opportunity_ARR__c')) for r in stale_60_1m):,.0f}"),
        ("Pushed 5+ Times", f"{len(pushed_5)} deals, EUR {sum(safe_num(r.get('APTS_Opportunity_ARR__c')) for r in pushed_5):,.0f}"),
        ("Overdue Close Date", f"{len(overdue)} deals"),
        ("Aging 365+ Days", f"{len(aging_365)} deals"),
        ("", ""),
        ("PROCESS COMPLIANCE", None),
        ("Commercial Approval Rate (Stage 3+)", f"{len(approved)}/{len(approval_3plus)} ({len(approved)/len(approval_3plus)*100:.0f}%)" if approval_3plus else "n/a"),
        ("Missing Approval (Land Stage 3+)", f"{len(missing_approval)} deals"),
    ]

    for label, value in kpis:
        if value is None:
            ws.cell(row=row, column=1, value=label).font = KPI_FONT
        else:
            ws.cell(row=row, column=1, value=label).font = DATA_FONT
            ws.cell(row=row, column=2, value=value).font = DATA_FONT
        row += 1

    auto_width(ws)


def build_pipeline_detail(wb: Workbook, cache_dir: Path, director: dict) -> None:
    ws = wb.create_sheet("Pipeline Detail")
    pipeline = load_cache(cache_dir, "soql_open_pipeline.json")

    headers = [
        "Account", "Opportunity", "Owner", "Stage", "Close Date",
        "ARR", "ACV", "Forecast ARR", "Forecast Category", "Probability",
        "Type", "Sub-Type", "Push Count", "Age (Days)", "Days In Stage",
        "Last Activity", "Activity Days Ago", "Risk Level",
        "Approval", "Approval Status", "Next Step",
        "Director Book", "Region", "Industry",
    ]
    write_header_row(ws, 1, headers)

    for i, r in enumerate(pipeline, 2):
        ws.cell(row=i, column=1, value=safe_str(nested_get(r, "Account", "Name")))
        ws.cell(row=i, column=2, value=safe_str(r.get("Name")))
        ws.cell(row=i, column=3, value=safe_str(nested_get(r, "Owner", "Name")))
        ws.cell(row=i, column=4, value=safe_str(r.get("StageName")))
        ws.cell(row=i, column=5, value=safe_str(r.get("CloseDate")))
        ws.cell(row=i, column=6, value=safe_num(r.get("APTS_Opportunity_ARR__c")))
        ws.cell(row=i, column=7, value=safe_num(r.get("Opportunity_Average_ACV__c")))
        ws.cell(row=i, column=8, value=safe_num(r.get("APTS_Forecast_ARR__c")))
        ws.cell(row=i, column=9, value=safe_str(r.get("ForecastCategoryName")))
        ws.cell(row=i, column=10, value=safe_num(r.get("Probability")))
        ws.cell(row=i, column=11, value=safe_str(r.get("Type")))
        ws.cell(row=i, column=12, value=safe_str(r.get("APTS_Opportunity_Sub_Type__c")))
        ws.cell(row=i, column=13, value=safe_num(r.get("PushCount")))
        ws.cell(row=i, column=14, value=safe_num(r.get("AgeInDays")))
        ws.cell(row=i, column=15, value=safe_num(r.get("LastStageChangeInDays")))
        ws.cell(row=i, column=16, value=safe_str(r.get("LastActivityDate")))
        ws.cell(row=i, column=17, value=safe_num(r.get("LastActivityInDays")))
        ws.cell(row=i, column=18, value=safe_str(r.get("Risk_Assessment_Level__c")))
        ws.cell(row=i, column=19, value="Yes" if r.get("Stage_20_Approval__c") else "No")
        ws.cell(row=i, column=20, value=safe_str(r.get("Approval_Status__c")))
        ws.cell(row=i, column=21, value=safe_str(r.get("NextStep"), 200))
        ws.cell(row=i, column=22, value=safe_str(r.get("Sales_Director_Book__c")))
        ws.cell(row=i, column=23, value=safe_str(r.get("Sales_Region__c")))
        ws.cell(row=i, column=24, value=safe_str(nested_get(r, "Account", "Industry")))

        # Conditional formatting
        push = safe_num(r.get("PushCount"))
        activity_days = safe_num(r.get("LastActivityInDays"), 0)
        arr = safe_num(r.get("APTS_Opportunity_ARR__c"))
        close = r.get("CloseDate", "9999")

        row_fill = None
        if push >= 5 or (activity_days > 60 and arr >= 1_000_000) or close < date.today().isoformat():
            row_fill = RED_FILL
        elif activity_days > 30:
            row_fill = AMBER_FILL

        if row_fill:
            for col in range(1, len(headers) + 1):
                ws.cell(row=i, column=col).fill = row_fill

    auto_width(ws)
    ws.auto_filter.ref = ws.dimensions


def build_q1_review(wb: Workbook, cache_dir: Path, director: dict) -> None:
    ws = wb.create_sheet("Q1 Review")
    territory = director["territory"]
    book = territory

    ws.cell(row=1, column=1, value=f"Q1 2026 Review — {territory}").font = TITLE_FONT
    ws.merge_cells("A1:F1")

    # Section A: Forecast vs Actual
    won_q1 = load_cache(cache_dir, "soql_won_q1.json")
    lost_q1 = load_cache(cache_dir, "soql_lost_q1.json")
    forecast_items = load_cache(cache_dir, "forecast_item_Q1_2026_ARR.json")

    won_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in won_q1)
    lost_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in lost_q1)

    # Aggregate forecast by category (global — we'll note it's org-wide)
    commit_total = sum(safe_num(r.get("ForecastAmount")) for r in forecast_items
                       if r.get("ForecastCategoryName") == "Commit" and r.get("ForecastingItemCategory") == "CommitOnly")
    closed_total = sum(safe_num(r.get("ForecastAmount")) for r in forecast_items
                       if r.get("ForecastCategoryName") == "Closed" and r.get("ForecastingItemCategory") == "ClosedOnly")

    row = 3
    ws.cell(row=row, column=1, value="SECTION A: Q1 Forecast vs Actual (ARR)").font = KPI_FONT
    row += 1
    for label, val in [
        ("Q1 Commit Forecast (org-wide)", f"EUR {commit_total:,.0f}"),
        ("Q1 Closed-Won (org-wide)", f"EUR {closed_total:,.0f}"),
        (f"Q1 Won — {territory}", f"EUR {won_arr:,.0f} ({len(won_q1)} deals)"),
        (f"Q1 Lost — {territory}", f"EUR {lost_arr:,.0f} ({len(lost_q1)} deals)"),
    ]:
        ws.cell(row=row, column=1, value=label).font = DATA_FONT
        ws.cell(row=row, column=2, value=val).font = DATA_FONT
        row += 1

    # Section B: Deals Pushed Out of Q1
    row += 1
    ws.cell(row=row, column=1, value="SECTION B: Deals Pushed Out of Q1").font = KPI_FONT
    row += 1
    close_changes = load_cache(cache_dir, "field_history_CloseDate.json")
    pushed_out = [
        r for r in close_changes
        if r.get("OldValue") and r.get("NewValue")
        and "2026-01-" <= str(r["OldValue"]) <= "2026-03-31"
        and str(r["NewValue"]) >= "2026-04"
        and nested_get(r, "Opportunity", "Sales_Director_Book__c") == book
    ]

    headers = ["Account", "Opportunity", "Owner", "ARR", "Old Close", "New Close", "Stage"]
    write_header_row(ws, row, headers)
    row += 1

    for r in pushed_out:
        opp = r.get("Opportunity") or {}
        ws.cell(row=row, column=1, value=safe_str(nested_get(opp, "Account", "Name")))
        ws.cell(row=row, column=2, value=safe_str(opp.get("Name")))
        ws.cell(row=row, column=3, value=safe_str(nested_get(opp, "Owner", "Name")))
        ws.cell(row=row, column=4, value=safe_num(opp.get("APTS_Opportunity_ARR__c")))
        ws.cell(row=row, column=5, value=safe_str(r.get("OldValue")))
        ws.cell(row=row, column=6, value=safe_str(r.get("NewValue")))
        ws.cell(row=row, column=7, value=safe_str(opp.get("StageName")))
        row += 1

    # Section C: Forecast Category Movement
    row += 1
    ws.cell(row=row, column=1, value="SECTION C: Forecast Category Movement (Q1)").font = KPI_FONT
    row += 1
    fc_changes = load_cache(cache_dir, "field_history_ForecastCategoryName.json")
    fc_director = [
        r for r in fc_changes
        if nested_get(r, "Opportunity", "Sales_Director_Book__c") == book
    ]

    headers = ["Opportunity", "Owner", "ARR", "Old Category", "New Category", "Date"]
    write_header_row(ws, row, headers)
    row += 1
    for r in fc_director:
        opp = r.get("Opportunity") or {}
        ws.cell(row=row, column=1, value=safe_str(opp.get("Name")))
        ws.cell(row=row, column=2, value=safe_str(nested_get(opp, "Owner", "Name")))
        ws.cell(row=row, column=3, value=safe_num(opp.get("APTS_Opportunity_ARR__c")))
        ws.cell(row=row, column=4, value=safe_str(r.get("OldValue")))
        ws.cell(row=row, column=5, value=safe_str(r.get("NewValue")))
        ws.cell(row=row, column=6, value=safe_str(r.get("CreatedDate", "")[:10]))
        row += 1

    auto_width(ws)


def build_won_lost(wb: Workbook, cache_dir: Path, director: dict) -> None:
    ws = wb.create_sheet("Won-Lost")
    won = load_cache(cache_dir, "soql_won_this_quarter.json") + load_cache(cache_dir, "soql_won_q1.json")
    lost = load_cache(cache_dir, "soql_lost_this_quarter.json") + load_cache(cache_dir, "soql_lost_q1.json")
    # Deduplicate by Id
    seen = set()
    all_deals = []
    for r in won + lost:
        rid = r.get("Id")
        if rid and rid not in seen:
            seen.add(rid)
            all_deals.append(r)
    all_deals.sort(key=lambda r: safe_num(r.get("APTS_Opportunity_ARR__c")), reverse=True)

    headers = [
        "Account", "Opportunity", "Owner", "Type", "Stage", "ARR", "ACV",
        "Close Date", "Created Date", "Sales Cycle Days",
        "Reason Won/Lost", "Sub-Reason", "Competitor", "Lost Comments",
    ]
    write_header_row(ws, 1, headers)

    for i, r in enumerate(all_deals, 2):
        created = r.get("CreatedDate", "")[:10] if r.get("CreatedDate") else ""
        close = r.get("CloseDate", "")
        cycle_days = ""
        if created and close:
            try:
                from datetime import datetime as dt
                d1 = dt.strptime(created[:10], "%Y-%m-%d")
                d2 = dt.strptime(close[:10], "%Y-%m-%d")
                cycle_days = (d2 - d1).days
            except Exception:
                pass

        ws.cell(row=i, column=1, value=safe_str(nested_get(r, "Account", "Name")))
        ws.cell(row=i, column=2, value=safe_str(r.get("Name")))
        ws.cell(row=i, column=3, value=safe_str(nested_get(r, "Owner", "Name")))
        ws.cell(row=i, column=4, value=safe_str(r.get("Type")))
        ws.cell(row=i, column=5, value=safe_str(r.get("StageName")))
        ws.cell(row=i, column=6, value=safe_num(r.get("APTS_Opportunity_ARR__c")))
        ws.cell(row=i, column=7, value=safe_num(r.get("Opportunity_Average_ACV__c")))
        ws.cell(row=i, column=8, value=safe_str(close))
        ws.cell(row=i, column=9, value=safe_str(created))
        ws.cell(row=i, column=10, value=cycle_days)
        ws.cell(row=i, column=11, value=safe_str(r.get("Reason_Won_Lost__c")))
        ws.cell(row=i, column=12, value=safe_str(r.get("Sub_Reason__c")))
        ws.cell(row=i, column=13, value=safe_str(r.get("Lost_to_Competitor__c")))
        ws.cell(row=i, column=14, value=safe_str(r.get("Lost_Comments__c"), 200))

        if "Won" in str(r.get("StageName", "")):
            for col in range(1, len(headers) + 1):
                ws.cell(row=i, column=col).fill = GREEN_FILL

    auto_width(ws)
    ws.auto_filter.ref = ws.dimensions


def build_rep_performance(wb: Workbook, cache_dir: Path, director: dict) -> None:
    ws = wb.create_sheet("Rep Performance")
    pipeline = load_cache(cache_dir, "soql_open_pipeline.json")
    won = load_cache(cache_dir, "soql_won_this_quarter.json")
    lost = load_cache(cache_dir, "soql_lost_this_quarter.json")
    pushed = load_cache(cache_dir, "soql_pushed_deals.json")

    # Aggregate by rep
    reps: dict[str, dict] = {}
    for r in pipeline:
        owner = nested_get(r, "Owner", "Name") or "Unknown"
        if owner not in reps:
            reps[owner] = {"pipeline_arr": 0, "deal_count": 0, "won_arr": 0, "won_ct": 0,
                           "lost_arr": 0, "lost_ct": 0, "stale": 0, "pushed": 0, "missing_approval": 0}
        reps[owner]["pipeline_arr"] += safe_num(r.get("APTS_Opportunity_ARR__c"))
        reps[owner]["deal_count"] += 1
        if safe_num(r.get("LastActivityInDays"), 0) > 30:
            reps[owner]["stale"] += 1
        if r.get("StageName", "") >= "3" and not r.get("Stage_20_Approval__c") and r.get("Type") == "Land":
            reps[owner]["missing_approval"] += 1

    for r in won:
        owner = nested_get(r, "Owner", "Name") or "Unknown"
        if owner not in reps:
            reps[owner] = {"pipeline_arr": 0, "deal_count": 0, "won_arr": 0, "won_ct": 0,
                           "lost_arr": 0, "lost_ct": 0, "stale": 0, "pushed": 0, "missing_approval": 0}
        reps[owner]["won_arr"] += safe_num(r.get("APTS_Opportunity_ARR__c"))
        reps[owner]["won_ct"] += 1

    for r in lost:
        owner = nested_get(r, "Owner", "Name") or "Unknown"
        if owner not in reps:
            reps[owner] = {"pipeline_arr": 0, "deal_count": 0, "won_arr": 0, "won_ct": 0,
                           "lost_arr": 0, "lost_ct": 0, "stale": 0, "pushed": 0, "missing_approval": 0}
        reps[owner]["lost_arr"] += safe_num(r.get("APTS_Opportunity_ARR__c"))
        reps[owner]["lost_ct"] += 1

    for r in pushed:
        owner = nested_get(r, "Owner", "Name") or "Unknown"
        if owner in reps:
            reps[owner]["pushed"] += 1

    headers = [
        "Rep", "Open Pipeline ARR", "Deal Count", "Avg Deal Size",
        "Won ARR (Q)", "Lost ARR (Q)", "Win Rate %",
        "Stale Deals", "Pushed Deals", "Missing Approvals",
    ]
    write_header_row(ws, 1, headers)

    sorted_reps = sorted(reps.items(), key=lambda x: x[1]["pipeline_arr"], reverse=True)
    for i, (name, d) in enumerate(sorted_reps, 2):
        avg = d["pipeline_arr"] / d["deal_count"] if d["deal_count"] else 0
        total_closed = d["won_ct"] + d["lost_ct"]
        wr = (d["won_ct"] / total_closed * 100) if total_closed else 0

        ws.cell(row=i, column=1, value=name)
        ws.cell(row=i, column=2, value=d["pipeline_arr"])
        ws.cell(row=i, column=3, value=d["deal_count"])
        ws.cell(row=i, column=4, value=round(avg))
        ws.cell(row=i, column=5, value=d["won_arr"])
        ws.cell(row=i, column=6, value=d["lost_arr"])
        ws.cell(row=i, column=7, value=round(wr, 1))
        ws.cell(row=i, column=8, value=d["stale"])
        ws.cell(row=i, column=9, value=d["pushed"])
        ws.cell(row=i, column=10, value=d["missing_approval"])

    auto_width(ws)
    ws.auto_filter.ref = ws.dimensions


def build_sources_lineage(wb: Workbook, cache_dir: Path) -> None:
    ws = wb.create_sheet("Sources & Lineage")
    sources = load_cache(cache_dir, "_sources.json")

    ws.cell(row=1, column=1, value="Source Registry").font = TITLE_FONT
    ws.merge_cells("A1:F1")

    headers = ["Source ID", "Source Type", "Name", "Query / Endpoint", "Record Count", "Extracted At"]
    write_header_row(ws, 3, headers)

    for i, s in enumerate(sources, 4):
        ws.cell(row=i, column=1, value=s.get("source_id", ""))
        ws.cell(row=i, column=2, value=s.get("source_type", ""))
        ws.cell(row=i, column=3, value=s.get("name", ""))
        ws.cell(row=i, column=4, value=safe_str(s.get("query_or_endpoint", ""), 150))
        ws.cell(row=i, column=5, value=s.get("record_count", 0))
        ws.cell(row=i, column=6, value=s.get("extracted_at", ""))

    auto_width(ws)


def build_placeholder_tab(wb: Workbook, title: str, message: str) -> None:
    """Create a placeholder tab with a message."""
    ws = wb.create_sheet(title)
    ws.cell(row=1, column=1, value=title).font = TITLE_FONT
    ws.cell(row=3, column=1, value=message).font = DATA_FONT
    ws.column_dimensions["A"].width = 60


# ── Main ─────────────────────────────────────────────────────────────────────

def build_workbook(director: dict, snapshot_date: str, output_dir: Path) -> Path:
    slug = h.slugify(director["name"])
    cache_dir = output_dir / ".cache" / slug

    if not cache_dir.exists():
        print(f"  ERROR: Cache not found at {cache_dir}")
        return cache_dir

    wb = Workbook()

    print(f"  Building tabs...")
    build_scorecard(wb, cache_dir, director)
    build_pipeline_detail(wb, cache_dir, director)
    build_q1_review(wb, cache_dir, director)

    # Q2 Outlook — uses same pipeline data + forecast
    build_placeholder_tab(wb, "Q2 Outlook", "Q2 forecast detail — computed from ForecastingItem Q2 + open pipeline")

    build_rep_performance(wb, cache_dir, director)
    build_won_lost(wb, cache_dir, director)

    # Commercial Approval, Renewals, Risk Register, Data Quality — placeholders for incremental build
    build_placeholder_tab(wb, "Commercial Approval", "Approval state + candidates from D1 dashboard data")
    build_placeholder_tab(wb, "Renewals & Retention", "GRR/NRR from CRMA + D1 renewal reports")
    build_placeholder_tab(wb, "Risk Register", "Composite risk score from SOQL + CRMA pipeline ops")
    build_placeholder_tab(wb, "Data Quality", "D2 hygiene metrics by rep")
    build_placeholder_tab(wb, "Quota & Targets", "Placeholder — populated when Finance delivers regional targets")

    build_sources_lineage(wb, cache_dir)

    out_path = output_dir / f"Sales Director Data - {director['name']} ({director['territory']}).xlsx"
    wb.save(str(out_path))
    print(f"  Saved: {out_path.name} ({len(wb.sheetnames)} tabs)")
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--director", help="Single director name")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--snapshot-date", default=date.today().isoformat())
    args = parser.parse_args()

    if not args.director and not args.all:
        print("Must specify --director NAME or --all", file=sys.stderr)
        return 2

    config = load_md1_preset_config(h.CONFIG_PATH)
    output_dir = OUTPUT_ROOT / args.snapshot_date

    if args.director:
        targets = [p for p in config.presets if args.director.lower() in p.name.lower()]
        targets = [{"name": t.name, "territory": t.territory, "filters": [dict(f) for f in t.filters]} for t in targets]
    else:
        targets = [{"name": p.name, "territory": p.territory, "filters": [dict(f) for f in p.filters]} for p in config.presets]

    for director in targets:
        print(f"\n=== {director['name']} ({director['territory']}) ===")
        build_workbook(director, args.snapshot_date, output_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Test with one director (requires Phase 1 cache to exist)**

Run: `cd /Users/test/crm-analytics && python3 scripts/build_director_workbooks.py --director "Dan Peppett" --snapshot-date 2026-04-10`
Expected: `Sales Director Data - Dan Peppett (UK & Ireland).xlsx` created with 12 tabs (6 populated, 6 placeholders).

- [ ] **Step 3: Open the file and verify tabs look correct**

Run: `python3 -c "from openpyxl import load_workbook; wb = load_workbook('output/director_data_dumps/2026-04-10/Sales Director Data - Dan Peppett (UK & Ireland).xlsx'); print(wb.sheetnames)"`
Expected: 12 tab names including Scorecard, Pipeline Detail, Q1 Review, Q2 Outlook, Rep Performance, Won-Lost, etc.

- [ ] **Step 4: Commit**

```bash
git add scripts/build_director_workbooks.py
git commit -m "feat: add Phase 2 workbook builder with core tabs + placeholders"
```

---

### Task 4: Run Phase 1 Extract for All Directors

- [ ] **Step 1: Run full extraction**

Run: `cd /Users/test/crm-analytics && python3 scripts/extract_director_data.py --all --snapshot-date 2026-04-10`
Expected: ~5-8 minutes. Cache populated for all 9 directors. Each prints source counts.

- [ ] **Step 2: Verify cache completeness**

Run: `for d in output/director_data_dumps/2026-04-10/.cache/*/; do echo "$(basename $d): $(ls $d/*.json | wc -l) files"; done`
Expected: Each director has 25+ JSON files.

- [ ] **Step 3: Spot-check one cache**

Run: `python3 -c "import json; print(json.dumps(json.load(open('output/director_data_dumps/2026-04-10/.cache/peppett-dan/_sources.json')), indent=2)[:2000])"`
Expected: Source registry with IDs, types, record counts, timestamps.

---

### Task 5: Run Phase 2 Build for All Directors

- [ ] **Step 1: Build all workbooks**

Run: `cd /Users/test/crm-analytics && python3 scripts/build_director_workbooks.py --all --snapshot-date 2026-04-10`
Expected: 9 Excel files created in `output/director_data_dumps/2026-04-10/`.

- [ ] **Step 2: Verify output**

Run: `ls -la output/director_data_dumps/2026-04-10/*.xlsx | wc -l`
Expected: 9 files.

- [ ] **Step 3: Commit**

```bash
git add -A output/director_data_dumps/2026-04-10/.cache/
git commit -m "data: extract + build director data dump workbooks for 2026-04-10"
```

---

### Task 6: Fill Remaining Placeholder Tabs (Incremental)

After Tasks 1-5 produce working workbooks with 6 real tabs + 6 placeholders, fill in the remaining tabs one at a time. Each sub-step below replaces one placeholder with real data.

**Files:**

- Modify: `scripts/build_director_workbooks.py`

- [ ] **Step 1: Q2 Outlook tab** — Replace `build_placeholder_tab(wb, "Q2 Outlook", ...)` with a real function that reads `soql_forecast_categories.json` + `forecast_item_Q2_2026_*.json` + open pipeline with Q2 close dates. Show commit/best case/pipeline breakdown + deal-level detail for commit deals.

- [ ] **Step 2: Commercial Approval tab** — Read `d1_dashboard.json`, extract the approval state and candidates components. Show approval summary + missing approval detail table.

- [ ] **Step 3: Renewals & Retention tab** — Read `crma_retention_metrics.json` + `crma_renewal_risk.json` + D1 renewal components. Show GRR/NRR + ARR waterfall + renewal pipeline by risk level + churn detail.

- [ ] **Step 4: Risk Register tab** — Read `soql_open_pipeline.json` + `crma_pipeline_ops.json`. Compute composite risk score per opp. Show sorted table with risk flags and CRMA-enriched columns.

- [ ] **Step 5: Data Quality tab** — Read `d2_dashboard.json` component data. Extract per-rep hygiene counts (stale, overdue, missing fields, aging). Show per-rep table + territory totals.

- [ ] **Step 6: Commit**

```bash
git add scripts/build_director_workbooks.py
git commit -m "feat: fill remaining workbook tabs (Q2, approval, renewals, risk, data quality)"
```

---

## Execution Summary

| Task | What                                 | Time Est             |
| ---- | ------------------------------------ | -------------------- |
| 1    | Shared helpers                       | Quick                |
| 2    | Phase 1 extract script               | Medium               |
| 3    | Phase 2 workbook builder (core tabs) | Medium               |
| 4    | Run Phase 1 for all 9 directors      | ~5-8 min (API calls) |
| 5    | Run Phase 2 for all 9 directors      | ~30 sec              |
| 6    | Fill placeholder tabs                | Medium               |

Tasks 1-3 are the code. Task 4 is the data collection. Task 5 produces the workbooks. Task 6 polishes.
