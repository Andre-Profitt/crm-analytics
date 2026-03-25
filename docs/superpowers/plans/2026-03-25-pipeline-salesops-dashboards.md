# Pipeline Reporting & Sales Ops Dashboards — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build two CRMA dashboards (Pipeline Reporting 8 pages, Sales Ops 3 pages) plus one new dataset (Retention Product Analysis), using existing builder patterns.

**Architecture:** Python builders that generate dashboard JSON and deploy via Wave API PATCH. Each builder follows the established pattern: constants → SOQL → dataset upload → SAQL steps → widgets → layout → deploy. The new dataset builder creates `Retention_Product_Analysis` by joining Opportunity + OpportunityLineItem + Account.

**Tech Stack:** Python 3, Salesforce CRM Analytics (Wave API v66.0), SAQL, `crm_analytics_helpers.py` helper library, `sf` CLI for auth.

**Spec:** `docs/superpowers/specs/2026-03-25-pipeline-salesops-dashboards-design.md`

---

## File Map

| File                                             | Action | Responsibility                                                    |
| ------------------------------------------------ | ------ | ----------------------------------------------------------------- |
| `build_retention_product_analysis.py`            | Create | New dataset: account × product × year churn analysis              |
| `build_pipeline_reporting.py`                    | Create | Dashboard 1: Pipeline Reporting & Insights (8 pages)              |
| `build_sales_ops_reporting.py`                   | Create | Dashboard 2: Sales Ops Data Quality & Forecast Accuracy (3 pages) |
| `build_pipeline_opportunity_operations.py`       | Modify | Add `NextStep` and `RootCauseHypothesis` fields                   |
| `crm_analytics_helpers.py`                       | Modify | Move `_industry_group()` from BDR builder to shared helpers       |
| `tests/test_build_retention_product_analysis.py` | Create | Unit tests for churn dataset transformation logic                 |
| `tests/test_build_pipeline_reporting.py`         | Create | Smoke tests for steps/widgets/layout generation                   |
| `tests/test_build_sales_ops_reporting.py`        | Create | Smoke tests for steps/widgets/layout generation                   |

All new files live in `/Users/test/azure-storage-optimizer/book-of-business/`.

**Important:** Existing builders for the consumed datasets (e.g., `build_pipeline_opportunity_operations.py`, `build_revenue_retention_health.py`, `build_forecast_revenue_motions.py`, `build_customer_account_health.py`) live in `/Users/test/crm-analytics/`. These builders have already been run and their datasets are deployed to the Salesforce org. The new dashboard builders consume these datasets via SAQL — they do not need to recreate them.

---

## Phase 0: Data Validation (must run before building)

### Task 0: Profile data and validate assumptions

**Files:**

- Read: `build_pipeline_opportunity_operations.py`
- Read: `crm_analytics_helpers.py`

- [ ] **Step 1: Authenticate to Salesforce**

Run: `sf org display --target-org apro@simcorp.com --json`
Expected: JSON with `accessToken` and `instanceUrl`

- [ ] **Step 2: Check Stage_20_Approval\_\_c field population**

```bash
sf data query --query "SELECT count(Id) cnt FROM Opportunity WHERE Stage_20_Approval__c != null" --target-org apro@simcorp.com --json
```

Expected: Count > 0. If zero, log the finding and skip approval widgets in the plan.

- [ ] **Step 3: Profile PushCount distribution on open pipeline**

```bash
sf data query --query "SELECT COUNT(Id) cnt FROM Opportunity WHERE IsClosed = false AND FiscalYear = 2026" --target-org apro@simcorp.com --json
```

Then via Wave API SAQL query:

```
q = load "Pipeline_Opportunity_Operations";
q = filter q by IsClosed == "false";
q = filter q by RecordType == "detail";
q = group q by all;
q = foreach q generate
  count() as total,
  sum(case when PushCount >= 1 then 1 else 0 end) as pushed_1plus,
  sum(case when PushCount >= 3 then 1 else 0 end) as pushed_3plus,
  sum(case when ForecastDowngradeCount >= 1 then 1 else 0 end) as forecast_downgraded,
  sum(case when BackwardMoveCount >= 1 then 1 else 0 end) as backward_moved,
  sum(case when IsPastDue == "true" then 1 else 0 end) as past_due,
  sum(case when NeedsApproval == "true" then 1 else 0 end) as needs_approval;
```

Record the results — they inform the root cause hypothesis priority order.

- [ ] **Step 4: Profile stage dwell times by MotionType**

```
q = load "Opp_Mgmt_KPIs";
q = filter q by IsWon == "true";
q = filter q by FiscalYear in [2025, 2026];
q = group q by (Type, StageName);
q = foreach q generate Type, StageName,
  median(DaysInStage) as P50,
  percentile(DaysInStage, 75) as P75,
  percentile(DaysInStage, 90) as P90,
  count() as n;
q = order q by Type, StageName;
```

Save the output — these become the empirical thresholds for RootCauseHypothesis.

- [ ] **Step 5: Check contract coverage gap**

```
q = load "Contract_Operations";
q = filter q by IsActive == "true";
q = filter q by DaysToExpiry >= 0;
q = filter q by DaysToExpiry <= 90;
q = group q by all;
q = foreach q generate count() as expiring_90d;
```

Then cross-reference with Revenue_Retention_Health to see how many of those accounts have an open Renewal opp. If gap > 10 accounts, flag for a widget on the renewals page.

- [ ] **Step 6: Document findings**

Create a brief `docs/superpowers/plans/2026-03-25-data-validation-results.md` with the profiling results. These inform implementation decisions in subsequent tasks.

- [ ] **Step 7: Commit**

```bash
git add docs/superpowers/plans/2026-03-25-data-validation-results.md
git commit -m "chore: data validation results for pipeline/sales ops dashboards"
```

---

## Phase 1: Shared Infrastructure

### Task 1: Move `_industry_group()` to shared helpers

**Files:**

- Read: `build_bdr_operating_dashboards.py` (source of `_industry_group()`)
- Modify: `crm_analytics_helpers.py`

- [ ] **Step 1: Read the existing `_industry_group()` function**

Find it in `build_bdr_operating_dashboards.py` (around line 415). Copy the function signature and logic.

- [ ] **Step 2: Add `_industry_group()` to `crm_analytics_helpers.py`**

Add at the end of the file, before any `if __name__` block:

```python
def _industry_group(industry: str) -> str:
    """Normalize Account.Industry to 8 consulting-grade groups."""
    ind = (industry or "").strip().lower()
    if any(k in ind for k in ("asset manage", "asset mgmt", "investment manage")):
        return "Asset Management"
    if any(k in ind for k in ("wealth", "private bank")):
        return "Wealth Management"
    if "fund" in ind:
        return "Fund"
    if any(k in ind for k in ("pension", "retirement")):
        return "Pension"
    if any(k in ind for k in ("asset owner", "sovereign", "endowment")):
        return "Asset Owner"
    if any(k in ind for k in ("bank", "credit")):
        return "Bank"
    if any(k in ind for k in ("servicer", "custod", "administrator")):
        return "Asset Servicer"
    if "insurance" in ind:
        return "Insurance"
    return "Other"
```

- [ ] **Step 3: Run existing tests to verify no regression**

Run: `cd /Users/test/azure-storage-optimizer/book-of-business && python3 -m pytest tests/ -v`
Expected: All existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add crm_analytics_helpers.py
git commit -m "feat: move _industry_group() to shared helpers"
```

### Task 2: Add NextStep and RootCauseHypothesis to Pipeline_Opportunity_Operations

**Files:**

- Modify: `build_pipeline_opportunity_operations.py`
- Test: `tests/test_build_pipeline_reporting.py` (created later — for now, manual smoke)

- [ ] **Step 1: Add NextStep to SOQL query**

Find the `OPP_SOQL` constant in `build_pipeline_opportunity_operations.py`. Add `NextStep,` to the SELECT field list (after `Quota_Amount__c`).

- [ ] **Step 2: Add NextStep dimension to row builder**

Find the row-building section where dimensions are assigned (look for `_dim("OpportunityName", ...)`). Add:

```python
_dim("NextStep", opp.get("NextStep", "")),
```

- [ ] **Step 3: Add RootCauseHypothesis computed dimension**

Find the section where `SlipRiskScore` and `ProcessRiskScore` are computed. After those computations, add:

```python
def _root_cause_hypothesis(row, stage_benchmarks):
    """Generate data-driven root cause hypothesis for slipped deals."""
    push = row.get("PushCount", 0)
    dis = row.get("DaysInStage", 0)
    stage = row.get("StageName", "")
    motion = row.get("MotionType", "")
    key = (motion, stage)
    p75 = stage_benchmarks.get(key, {}).get("P75", 999)
    p90 = stage_benchmarks.get(key, {}).get("P90", 999)
    p50 = stage_benchmarks.get(key, {}).get("P50", 0)

    if push >= 3 and dis > p90:
        return f"Stalled — {push} pushes, {dis}d in {stage} (P90 for {motion}: {p90}d)"
    if row.get("NeedsApproval") == "true" and dis > p75:
        return f"Approval bottleneck — no approval, {dis}d in {stage} (P75: {p75}d)"
    if dis > p90:
        return f"Stage stall — {dis}d in {stage} (P90 for {motion}: {p90}d)"
    if row.get("ForecastDowngradeCount", 0) >= 1 and push >= 1:
        return f"Weakening conviction — forecast downgraded {row['ForecastDowngradeCount']}x with {push} push(es)"
    if row.get("BackwardMoveCount", 0) >= 1:
        return f"Deal regression — moved backward {row['BackwardMoveCount']}x"
    if row.get("IsPastDue") == "true":
        days_overdue = abs(int(row.get("DaysToClose", 0)))
        return f"Past due — close date {days_overdue}d overdue"
    if dis > p75:
        return f"Slow — {dis}d exceeds P75 ({p75}d for {motion})"
    if push >= 1:
        return f"Monitor — {push} push(es), {dis}d in {stage} ({motion} P50: {p50}d)"
    return ""
```

Add the `_dim("RootCauseHypothesis", ...)` call in the row builder, passing the benchmarks from Task 0 Step 4.

- [ ] **Step 4: Test the builder runs without error**

Run: `python3 build_pipeline_opportunity_operations.py --dry-run` (if dry-run exists) or verify imports work:

```bash
python3 -c "from build_pipeline_opportunity_operations import build_steps, build_widgets, build_layout; print('OK')"
```

- [ ] **Step 5: Commit**

```bash
git add build_pipeline_opportunity_operations.py
git commit -m "feat: add NextStep and RootCauseHypothesis to Pipeline_Opportunity_Operations"
```

---

## Phase 2: New Dataset — Retention Product Analysis

### Task 3: Build `build_retention_product_analysis.py` — SOQL and row transformation

**Files:**

- Create: `build_retention_product_analysis.py`
- Create: `tests/test_build_retention_product_analysis.py`
- Read: `build_revenue_retention_health.py` (churn math reference)
- Read: `build_product_portfolio_dashboard.py` (ACV blend reference)

- [ ] **Step 1: Write failing test for ACV blend logic**

```python
# tests/test_build_retention_product_analysis.py
import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from build_retention_product_analysis import _commercial_value

class TestACVBlend(unittest.TestCase):
    def test_prefers_acv_1st_year(self):
        self.assertEqual(_commercial_value(100, 200), 100)

    def test_falls_back_to_forecast_acv(self):
        self.assertEqual(_commercial_value(0, 200), 200)
        self.assertEqual(_commercial_value(None, 200), 200)

    def test_falls_back_to_zero(self):
        self.assertEqual(_commercial_value(0, 0), 0)
        self.assertEqual(_commercial_value(None, None), 0)

if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test — verify it fails**

Run: `python3 -m pytest tests/test_build_retention_product_analysis.py -v`
Expected: ImportError — module not found.

- [ ] **Step 3: Create the builder with SOQL, ACV blend, and row transformation**

```python
#!/usr/bin/env python3
"""Retention Product Analysis. Account x Product x Year churn dataset using OLI ACV blend."""

import csv
import io
from collections import defaultdict
from datetime import datetime, timedelta

from crm_analytics_helpers import (
    get_auth, _soql, _dim, _measure, _date, upload_dataset,
    _industry_group,
)

DS = "Retention_Product_Analysis"
DS_LABEL = "Retention Product Analysis"

OPP_SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, Account.Unit_Group__c, "
    "Account.Region__c, Account.Industry, Account.BillingCountry, "
    "Account.AuM_m__c, "
    "OwnerId, Owner.Name, Type, StageName, IsClosed, IsWon, "
    "CloseDate, FiscalYear, FiscalQuarter, "
    "APTS_Opportunity_ARR__c, APTS_Renewal_ACV__c, Amount "
    "FROM Opportunity "
    "WHERE Type IN ('Land','Expand','Renewal') "
    "AND CloseDate >= 2022-01-01T00:00:00Z "
    "ORDER BY CloseDate"
)

OLI_SOQL = (
    "SELECT Id, OpportunityId, Product2.Family, "
    "APTS_ACV_1st_Year__c, APTS_Forecast_ACV_AVG__c, "
    "APTS_ProductArea__c "
    "FROM OpportunityLineItem "
    "WHERE Opportunity.Type IN ('Land','Expand','Renewal') "
    "AND Opportunity.CloseDate >= 2022-01-01T00:00:00Z"
)

def _commercial_value(acv_1st, forecast_acv):
    """ACV blend: APTS_ACV_1st_Year__c -> APTS_Forecast_ACV_AVG__c -> 0."""
    v1 = float(acv_1st or 0)
    v2 = float(forecast_acv or 0)
    return v1 if v1 > 0 else v2

def _segment(aum, total_arr):
    """Account segment from AuM/ARR thresholds."""
    aum = float(aum or 0)
    arr = float(total_arr or 0)
    if aum > 100000 or arr > 500000:
        return "Enterprise"
    if aum > 10000 or arr > 100000:
        return "Mid-Market"
    return "Growth"

def _effective_retention_flag(account_id, lost_renewals, expand_wins):
    """Check if a lost renewal was 'protected' by a same-account Expand win within 90d."""
    for lr in lost_renewals.get(account_id, []):
        lr_date = lr["close_date"]
        for ew in expand_wins.get(account_id, []):
            delta = abs((ew["close_date"] - lr_date).days)
            if delta <= 90:
                return "Protected"
    return ""

def build_rows(opps, olis):
    """Transform raw SOQL results into dataset rows at account x product x year grain."""
    # Index OLIs by OpportunityId
    oli_by_opp = defaultdict(list)
    for oli in olis:
        oli_by_opp[oli["OpportunityId"]].append(oli)

    # Track lost renewals and expand wins for effective retention
    lost_renewals = defaultdict(list)  # account_id -> [{close_date, acv}]
    expand_wins = defaultdict(list)

    # First pass: classify opps
    for opp in opps:
        if not opp.get("IsClosed"):
            continue
        acct = opp["AccountId"]
        close_str = opp.get("CloseDate", "")[:10]
        if not close_str:
            continue
        close_dt = datetime.strptime(close_str, "%Y-%m-%d")
        otype = opp.get("Type", "")
        is_won = str(opp.get("IsWon", "")).lower() == "true"

        if otype == "Renewal" and not is_won:
            lost_renewals[acct].append({"close_date": close_dt})
        if otype == "Expand" and is_won:
            expand_wins[acct].append({"close_date": close_dt})

    # Second pass: build rows at account x product x quarter grain
    # Quarter grain is needed for the churn trend combo chart
    bucket = defaultdict(lambda: defaultdict(float))  # (acct, product, year, quarter) -> {measures}
    acct_info = {}  # account_id -> {dims}

    for opp in opps:
        acct_id = opp["AccountId"]
        otype = opp.get("Type", "")
        is_won = str(opp.get("IsWon", "")).lower() == "true"
        is_closed = str(opp.get("IsClosed", "")).lower() == "true"
        fy = int(opp.get("FiscalYear", 0))
        if fy == 0:
            continue

        # Get account info
        acct_name = (opp.get("Account") or {}).get("Name", "")
        unit_group = (opp.get("Account") or {}).get("Unit_Group__c", "Unassigned")
        region = (opp.get("Account") or {}).get("Region__c", "Unassigned")
        industry = (opp.get("Account") or {}).get("Industry", "Unknown")
        country = (opp.get("Account") or {}).get("BillingCountry", "Unknown")
        aum = (opp.get("Account") or {}).get("AuM_m__c", 0)

        acct_info[acct_id] = {
            "AccountName": acct_name,
            "UnitGroup": unit_group or "Unassigned",
            "SalesRegion": region or "Unassigned",
            "IndustryGroup": _industry_group(industry),
            "BillingCountry": country or "Unknown",
        }

        # Derive quarter from FiscalQuarter or CloseDate
        fq = int(opp.get("FiscalQuarter", 0))
        quarter_label = f"Q{fq}" if fq else ""

        # Get OLIs for this opp
        opp_olis = oli_by_opp.get(opp["Id"], [])

        if opp_olis:
            for oli in opp_olis:
                pf = oli.get("Product2", {}).get("Family", "") or oli.get("APTS_ProductArea__c", "") or "Unknown"
                acv = _commercial_value(
                    oli.get("APTS_ACV_1st_Year__c"),
                    oli.get("APTS_Forecast_ACV_AVG__c"),
                )
                key = (acct_id, pf, fy, quarter_label)

                if is_won and is_closed:
                    if otype == "Land":
                        bucket[key]["NewLogoARR"] += acv
                        bucket[key]["InstalledARR"] += acv
                    elif otype == "Expand":
                        bucket[key]["ExpansionARR"] += acv
                        bucket[key]["InstalledARR"] += acv
                    elif otype == "Renewal":
                        bucket[key]["RetainedARR"] += acv
                        bucket[key]["InstalledARR"] += acv
                elif is_closed and not is_won and otype == "Renewal":
                    bucket[key]["ChurnARR"] += acv
        else:
            # No OLIs — use header-level value
            pf = "Unknown"
            if otype == "Renewal":
                acv = float(opp.get("APTS_Renewal_ACV__c") or 0)
            else:
                acv = float(opp.get("APTS_Opportunity_ARR__c") or opp.get("Amount") or 0)
            key = (acct_id, pf, fy, quarter_label)

            if is_won and is_closed:
                if otype == "Land":
                    bucket[key]["NewLogoARR"] += acv
                    bucket[key]["InstalledARR"] += acv
                elif otype == "Expand":
                    bucket[key]["ExpansionARR"] += acv
                    bucket[key]["InstalledARR"] += acv
                elif otype == "Renewal":
                    bucket[key]["RetainedARR"] += acv
                    bucket[key]["InstalledARR"] += acv
            elif is_closed and not is_won and otype == "Renewal":
                bucket[key]["ChurnARR"] += acv

    # Build output rows
    rows = []
    for (acct_id, pf, fy, qtr), measures in bucket.items():
        info = acct_info.get(acct_id, {})
        churn = measures.get("ChurnARR", 0)
        installed = measures.get("InstalledARR", 0)
        retained = measures.get("RetainedARR", 0)
        expansion = measures.get("ExpansionARR", 0)

        # Prior year installed for rate calculations (sum across all quarters)
        prior_installed = sum(
            v.get("InstalledARR", 0) for k, v in bucket.items()
            if k[0] == acct_id and k[1] == pf and k[2] == fy - 1
        )

        churn_rate = (churn / prior_installed * 100) if prior_installed > 0 else 0
        nrr = ((retained + expansion) / prior_installed * 100) if prior_installed > 0 else 0
        grr = (retained / prior_installed * 100) if prior_installed > 0 else 0

        eff_flag = _effective_retention_flag(acct_id, lost_renewals, expand_wins) if churn > 0 else ""

        rows.append({
            "AccountId": acct_id,
            "AccountName": info.get("AccountName", ""),
            "UnitGroup": info.get("UnitGroup", "Unassigned"),
            "SalesRegion": info.get("SalesRegion", "Unassigned"),
            "IndustryGroup": info.get("IndustryGroup", "Other"),
            "BillingCountry": info.get("BillingCountry", "Unknown"),
            "ProductFamily": pf,
            "Segment": _segment(0, installed),  # simplified
            "YearLabel": str(fy),
            "FYLabel": f"FY{fy}",
            "QuarterLabel": qtr,
            "Outcome": "Churned" if churn > 0 else ("Retained" if retained > 0 else "Active"),
            "Motion": "Churned" if churn > 0 else ("Retained" if retained > 0 else "New Logo" if measures.get("NewLogoARR", 0) > 0 else "Expanded"),
            "EffectiveRetentionFlag": eff_flag,
            "InstalledARR": round(installed, 2),
            "ChurnARR": round(churn, 2),
            "RetainedARR": round(retained, 2),
            "ExpansionARR": round(expansion, 2),
            "NewLogoARR": round(measures.get("NewLogoARR", 0), 2),
            "ChurnRate": round(churn_rate, 2),
            "NRR": round(nrr, 2),
            "GRR": round(grr, 2),
        })

    return rows

FIELDS_META = [
    _dim("AccountId"),
    _dim("AccountName", "Account"),
    _dim("UnitGroup", "Unit Group"),
    _dim("SalesRegion", "Sales Region"),
    _dim("IndustryGroup", "Industry Group"),
    _dim("BillingCountry", "Country"),
    _dim("ProductFamily", "Product Family"),
    _dim("Segment"),
    _dim("YearLabel", "Year"),
    _dim("FYLabel", "Fiscal Year"),
    _dim("QuarterLabel", "Quarter"),
    _dim("Outcome"),
    _dim("Motion"),
    _dim("EffectiveRetentionFlag", "Effective Retention"),
    _measure("InstalledARR", "Installed ARR"),
    _measure("ChurnARR", "Churn ARR"),
    _measure("RetainedARR", "Retained ARR"),
    _measure("ExpansionARR", "Expansion ARR"),
    _measure("NewLogoARR", "New Logo ARR"),
    _measure("ChurnRate", "Churn Rate %"),
    _measure("NRR", "NRR %"),
    _measure("GRR", "GRR %"),
]

def create_dataset(inst, tok):
    """Pull SOQL data, transform, upload dataset."""
    opps = _soql(inst, tok, OPP_SOQL)
    olis = _soql(inst, tok, OLI_SOQL)
    rows = build_rows(opps, olis)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[f["name"] for f in FIELDS_META])
    writer.writeheader()
    writer.writerows(rows)

    upload_dataset(inst, tok, DS, DS_LABEL, FIELDS_META, buf.getvalue().encode("utf-8"))
    return rows

def main():
    inst, tok = get_auth()
    create_dataset(inst, tok)
    print(f"Dataset {DS} uploaded successfully.")

if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test — verify it passes**

Run: `python3 -m pytest tests/test_build_retention_product_analysis.py -v`
Expected: All 3 tests pass.

- [ ] **Step 5: Write additional tests for churn classification and effective retention**

```python
# Add to tests/test_build_retention_product_analysis.py

from build_retention_product_analysis import build_rows, _effective_retention_flag, _segment
from datetime import datetime
from collections import defaultdict

class TestSegment(unittest.TestCase):
    def test_enterprise(self):
        self.assertEqual(_segment(200000, 0), "Enterprise")
        self.assertEqual(_segment(0, 600000), "Enterprise")

    def test_mid_market(self):
        self.assertEqual(_segment(50000, 0), "Mid-Market")

    def test_growth(self):
        self.assertEqual(_segment(0, 0), "Growth")

class TestEffectiveRetention(unittest.TestCase):
    def test_protected_within_90d(self):
        lost = {"A1": [{"close_date": datetime(2025, 6, 1)}]}
        wins = {"A1": [{"close_date": datetime(2025, 7, 15)}]}
        self.assertEqual(_effective_retention_flag("A1", lost, wins), "Protected")

    def test_not_protected_beyond_90d(self):
        lost = {"A1": [{"close_date": datetime(2025, 1, 1)}]}
        wins = {"A1": [{"close_date": datetime(2025, 12, 1)}]}
        self.assertEqual(_effective_retention_flag("A1", lost, wins), "")

    def test_no_expand_win(self):
        lost = {"A1": [{"close_date": datetime(2025, 6, 1)}]}
        wins = {}
        self.assertEqual(_effective_retention_flag("A1", lost, wins), "")
```

- [ ] **Step 6: Run all tests**

Run: `python3 -m pytest tests/test_build_retention_product_analysis.py -v`
Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add build_retention_product_analysis.py tests/test_build_retention_product_analysis.py
git commit -m "feat: Retention_Product_Analysis dataset builder with churn at account x product x year grain"
```

---

## Phase 3: Dashboard 1 — Pipeline Reporting & Insights

### Task 4: Build `build_pipeline_reporting.py` — constants, steps, and filter infrastructure

**Files:**

- Create: `build_pipeline_reporting.py`
- Read: `build_pipeline_opportunity_operations.py` (pattern reference)
- Read: `build_sales_compliance.py` (pattern reference)

- [ ] **Step 1: Create the builder with constants and filter steps**

```python
#!/usr/bin/env python3
"""Pipeline Reporting & Insights. 8 pages: Global Summary, Regional Comparison,
Regional Detail, Approval Overview, Approval Candidates, Renewals, Churn, Slipped Deals."""

from crm_analytics_helpers import (
    get_auth, get_dataset_id, sq, af, num, exec_kpi, pillbox,
    rich_chart, hdr, pg, nav_link, nav_row,
    build_dashboard_state, deploy_dashboard, create_dashboard_if_needed,
    coalesce_filter, section_label, add_table_action,
    heatmap_chart, bubble_chart, sankey_chart, combo_chart,
    waterfall_chart, funnel_chart, area_chart, line_chart,
)

# ═══ Dataset references ═══
DS_PIPE  = "Pipeline_Opportunity_Operations"
DS_FRM   = "Forecast_Revenue_Motions"
DS_RRH   = "Revenue_Retention_Health"
DS_RPA   = "Retention_Product_Analysis"

DASHBOARD_LABEL = "Pipeline Reporting & Insights"
N_PAGES = 8

# ═══ Resolved at runtime ═══
DS_PIPE_ID = None
DS_FRM_ID  = None
DS_RRH_ID  = None
DS_RPA_ID  = None

def _resolve_dataset_ids(inst, tok):
    global DS_PIPE_ID, DS_FRM_ID, DS_RRH_ID, DS_RPA_ID
    DS_PIPE_ID = get_dataset_id(inst, tok, DS_PIPE)
    DS_FRM_ID  = get_dataset_id(inst, tok, DS_FRM)
    DS_RRH_ID  = get_dataset_id(inst, tok, DS_RRH)
    DS_RPA_ID  = get_dataset_id(inst, tok, DS_RPA)

# ═══ SAQL fragments ═══
LP   = f'q = load "{DS_PIPE}";\n'
LFRM = f'q = load "{DS_FRM}";\n'
LRRH = f'q = load "{DS_RRH}";\n'
LRPA = f'q = load "{DS_RPA}";\n'

DETAIL = 'q = filter q by RecordType == "detail";\n'
OPEN   = 'q = filter q by IsClosed == "false";\n'
FY26   = 'q = filter q by FYLabel == "FY2026";\n'

# ═══ Coalesce filters ═══
UF  = coalesce_filter("f_unit",    "UnitGroup")
RF  = coalesce_filter("f_region",  "SalesRegion")
QF  = coalesce_filter("f_quarter", "CloseQuarter")
MF  = coalesce_filter("f_month",   "MonthLabel")
FYF = coalesce_filter("f_fy",      "FYLabel")

def build_steps():
    s = {}
    # ─── Filter steps ───
    pipe_meta = [{"id": DS_PIPE_ID, "name": DS_PIPE}]
    s["f_fy"]      = af("FYLabel",      pipe_meta, select_mode="single")
    s["f_quarter"] = af("CloseQuarter",  pipe_meta)
    s["f_month"]   = af("MonthLabel",    pipe_meta)
    s["f_unit"]    = af("UnitGroup",     pipe_meta)
    s["f_region"]  = af("SalesRegion",   pipe_meta)
    return s

def build_widgets():
    w = {}
    # ─── Filter widgets (shared across pages) ───
    w["f_fy"]      = pillbox("f_fy",      "Fiscal Year")
    w["f_quarter"] = pillbox("f_quarter", "Quarter")
    w["f_month"]   = pillbox("f_month",   "Month")
    w["f_unit"]    = pillbox("f_unit",    "Unit Group")
    w["f_region"]  = pillbox("f_region",  "Sales Region")
    return w

def build_layout():
    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [],  # populated per-page in subsequent tasks
    }
```

- [ ] **Step 2: Verify imports work**

Run: `python3 -c "from build_pipeline_reporting import build_steps, build_widgets; print('OK')"`
Expected: "OK"

- [ ] **Step 3: Commit**

```bash
git add build_pipeline_reporting.py
git commit -m "feat: build_pipeline_reporting.py scaffold with constants, filters, SAQL fragments"
```

### Task 5: Page 1.0 — Global Pipeline Summary (11 widgets)

**Files:**

- Modify: `build_pipeline_reporting.py`

- [ ] **Step 1: Add KPI steps (widgets 1-5)**

Add to `build_steps()`:

```python
    # ─── Page 1.0: Global Pipeline Summary ───
    s["s_p1_open_arr"] = sq(
        LP + DETAIL + OPEN + FYF + UF + RF + QF + MF
        + "q = group q by all;\n"
        + "q = foreach q generate sum(WeightedOpenARR) as val;\n"
    )
    s["s_p1_commit_arr"] = sq(
        LP + DETAIL + OPEN + FYF + UF + RF + QF + MF
        + 'q = filter q by ForecastCategory in ["Commit", "Best Case"];\n'
        + "q = group q by all;\n"
        + "q = foreach q generate sum(ARR) as val;\n"
    )
    s["s_p1_coverage"] = sq(
        LP + DETAIL + OPEN + FYF + UF + RF + QF + MF
        + "q = group q by all;\n"
        + "q = foreach q generate sum(WeightedOpenARR) / sum(QuotaContrib) as val;\n"
    )
    s["s_p1_atrisk"] = sq(
        LP + DETAIL + FYF + UF + RF + QF + MF
        + "q = group q by all;\n"
        + "q = foreach q generate sum(AtRiskARR) as val;\n"
    )
    s["s_p1_approval"] = sq(
        LP + DETAIL + FYF + UF + RF + QF + MF
        + "q = group q by all;\n"
        + "q = foreach q generate sum(MissingApprovalCount) as val;\n"
    )
```

- [ ] **Step 2: Add chart steps (widgets 6-11)**

```python
    # Waterfall (widget 6) — requires union of components
    # This is complex SAQL — build each component as a union
    s["s_p1_waterfall"] = sq(
        LP + DETAIL + FYF + UF + RF + QF + MF
        + 'q = filter q by IsClosed == "false";\n'
        + "q = group q by ForecastCategory;\n"
        + "q = foreach q generate ForecastCategory as label, sum(ARR) as val;\n"
        + "q = order q by val desc;\n"
    )
    # NOTE: True waterfall (start→created→won→lost→pushed→current) requires
    # a multi-load union step. Implement the union pattern during build.

    # Sankey (widget 7)
    s["s_p1_sankey"] = sq(
        LP + 'q = filter q by RecordType == "stage_history";\n'
        + FYF + UF + RF
        + 'q = filter q by PrevStage != "";\n'
        + "q = group q by (PrevStage, StageName);\n"
        + "q = foreach q generate PrevStage as source, StageName as target, sum(ARR) as val;\n"
        + "q = filter q by val > 0;\n"
        + "q = order q by val desc;\n"
    )

    # Pipeline by Region (widget 8)
    s["s_p1_by_region"] = sq(
        LP + DETAIL + OPEN + FYF + UF + RF + QF + MF
        + "q = group q by SalesRegion;\n"
        + "q = foreach q generate SalesRegion, sum(WeightedOpenARR) as val;\n"
        + "q = order q by val desc;\n"
    )

    # Pipeline Trend stacked area (widget 9)
    s["s_p1_trend"] = sq(
        LP + DETAIL + OPEN + FYF + UF + RF
        + "q = group q by (CloseQuarter, ForecastCategory);\n"
        + "q = foreach q generate CloseQuarter, ForecastCategory, sum(ARR) as val;\n"
        + "q = order q by CloseQuarter;\n"
    )

    # Deal Landscape bubble (widget 10)
    s["s_p1_landscape"] = sq(
        LP + DETAIL + OPEN + FYF + UF + RF + QF + MF
        + "q = foreach q generate OpportunityName, DaysInStage, ARR, Probability, RiskBand;\n"
        + "q = order q by ARR desc;\n"
        + "q = limit q 100;\n"
    )

    # Risk Distribution (widget 11)
    s["s_p1_risk"] = sq(
        LP + DETAIL + OPEN + FYF + UF + RF + QF + MF
        + "q = group q by RiskBand;\n"
        + "q = foreach q generate RiskBand, sum(WeightedOpenARR) as val;\n"
    )
```

- [ ] **Step 3: Add widget definitions**

Add to `build_widgets()`:

```python
    # ─── Page 1.0: Global Pipeline Summary ───
    w["p1_hdr"] = hdr("Pipeline Reporting & Insights", "Global Pipeline Summary")
    w["p1_sec_kpi"] = section_label("Pipeline KPIs")
    w["p1_kpi_open"]     = num("s_p1_open_arr",  "val", "Open Pipeline ARR", "#16325C", compact=True)
    w["p1_kpi_commit"]   = num("s_p1_commit_arr", "val", "Commit + Best Case", "#16325C", compact=True)
    w["p1_kpi_coverage"] = num("s_p1_coverage",   "val", "Coverage vs Quota", "#16325C")
    w["p1_kpi_atrisk"]   = num("s_p1_atrisk",     "val", "At-Risk ARR", "#E74C3C", compact=True)
    w["p1_kpi_approval"] = num("s_p1_approval",   "val", "Needs Approval", "#E74C3C")

    w["p1_sec_charts"] = section_label("Pipeline Analysis")
    w["p1_waterfall"] = waterfall_chart("s_p1_waterfall", "Pipeline Movement", "label", "val")
    w["p1_sankey"]    = sankey_chart("s_p1_sankey", "Stage Conversion Flow", "source", "target", "val")
    w["p1_by_region"] = rich_chart("s_p1_by_region", "hbar", "Pipeline by Region",
                                    ["SalesRegion"], ["val"])
    w["p1_trend"]     = area_chart("s_p1_trend", "Pipeline Trend by Quarter", stacked=True,
                                    show_legend=True)
    w["p1_landscape"] = bubble_chart("s_p1_landscape", "Deal Landscape")
    w["p1_risk"]      = rich_chart("s_p1_risk", "stackhbar", "Risk Distribution",
                                    ["RiskBand"], ["val"])
```

- [ ] **Step 4: Add page layout**

```python
    # In build_layout(), add to pages list:
    p1 = nav_row("p1", N_PAGES) + [
        {"name": "p1_hdr",          "row": 1, "column": 0,  "colspan": 12, "rowspan": 2},
        {"name": "f_fy",            "row": 3, "column": 0,  "colspan": 2,  "rowspan": 2},
        {"name": "f_quarter",       "row": 3, "column": 2,  "colspan": 2,  "rowspan": 2},
        {"name": "f_month",         "row": 3, "column": 4,  "colspan": 2,  "rowspan": 2},
        {"name": "f_unit",          "row": 3, "column": 6,  "colspan": 3,  "rowspan": 2},
        {"name": "f_region",        "row": 3, "column": 9,  "colspan": 3,  "rowspan": 2},
        {"name": "p1_sec_kpi",      "row": 5, "column": 0,  "colspan": 12, "rowspan": 1},
        {"name": "p1_kpi_open",     "row": 6, "column": 0,  "colspan": 2,  "rowspan": 3},
        {"name": "p1_kpi_commit",   "row": 6, "column": 2,  "colspan": 3,  "rowspan": 3},
        {"name": "p1_kpi_coverage", "row": 6, "column": 5,  "colspan": 2,  "rowspan": 3},
        {"name": "p1_kpi_atrisk",   "row": 6, "column": 7,  "colspan": 3,  "rowspan": 3},
        {"name": "p1_kpi_approval", "row": 6, "column": 10, "colspan": 2,  "rowspan": 3},
        {"name": "p1_sec_charts",   "row": 9, "column": 0,  "colspan": 12, "rowspan": 1},
        {"name": "p1_waterfall",    "row": 10, "column": 0,  "colspan": 6,  "rowspan": 6},
        {"name": "p1_sankey",       "row": 10, "column": 6,  "colspan": 6,  "rowspan": 6},
        {"name": "p1_by_region",    "row": 16, "column": 0,  "colspan": 4,  "rowspan": 6},
        {"name": "p1_trend",        "row": 16, "column": 4,  "colspan": 4,  "rowspan": 6},
        {"name": "p1_landscape",    "row": 22, "column": 0,  "colspan": 8,  "rowspan": 6},
        {"name": "p1_risk",         "row": 22, "column": 8,  "colspan": 4,  "rowspan": 6},
    ]
```

- [ ] **Step 5: Verify imports and structure**

Run: `python3 -c "from build_pipeline_reporting import build_steps, build_widgets; s = build_steps(); w = build_widgets(); print(f'{len(s)} steps, {len(w)} widgets')"`
Expected: Step and widget counts printed without error.

- [ ] **Step 6: Commit**

```bash
git add build_pipeline_reporting.py
git commit -m "feat: Page 1.0 Global Pipeline Summary — KPIs, waterfall, sankey, bubble, area"
```

### Task 6: Pages 1.1-1.2 — Regional Comparison + Regional Detail

**Files:**

- Modify: `build_pipeline_reporting.py`

Follow the same pattern as Task 5: add steps to `build_steps()`, widgets to `build_widgets()`, layouts to `build_layout()`.

- [ ] **Step 1: Add regional comparison steps (Page 1.1 — heatmap, stacked hbars, at-risk bar)**

4 steps: `s_p1_1_heatmap`, `s_p1_1_region_stage`, `s_p1_1_region_motion`, `s_p1_1_atrisk_region`

- [ ] **Step 2: Add regional detail steps (Page 1.2 — 4 KPIs, stage bar, motion column, top 10 table)**

7 steps: `s_p1_2_open`, `s_p1_2_won_ytd`, `s_p1_2_winrate`, `s_p1_2_atrisk`, `s_p1_2_stage`, `s_p1_2_motion`, `s_p1_2_top10`

- [ ] **Step 3: Add widgets for both pages**

- [ ] **Step 4: Add record action to Top 10 Deals table**

```python
add_table_action(w["p1_2_top10"], "salesforceActions", "Opportunity", "Id")
```

- [ ] **Step 5: Add page layouts**

- [ ] **Step 6: Verify and commit**

```bash
git commit -m "feat: Pages 1.1-1.2 Regional Comparison heatmap + filterable Regional Detail"
```

### Task 7: Pages 2.0-2.1 — Commercial Approval (overview + candidates)

**Files:**

- Modify: `build_pipeline_reporting.py`

- [ ] **Step 1: Add approval steps** — uses both `DS_PIPE` and `DS_FRM` datasets

Key steps: `s_p2_approved_arr`, `s_p2_compliance`, `s_p2_stale`, `s_p2_funnel` (union step), `s_p2_by_region`, `s_p2_by_motion`, `s_p2_trend`, `s_p2_1_candidates`

The funnel step requires a SAQL union:

```python
s["s_p2_funnel"] = sq(
    f'a = load "{DS_PIPE}";\n'
    + 'a = filter a by IsClosed == "false";\n'
    + 'a = filter a by StageOrder >= 3;\n'
    + 'a = group a by NeedsApproval;\n'
    + 'a = foreach a generate NeedsApproval as stage, count() as cnt;\n'
    # ... union with FRM CommercialApprovalFlag counts
)
```

- [ ] **Step 2: Add widgets including funnel**

- [ ] **Step 3: Add record action to candidates table**

- [ ] **Step 4: Add page layouts and commit**

```bash
git commit -m "feat: Pages 2.0-2.1 Commercial Approval funnel + candidates"
```

### Task 8: Page 3.0 — Renewals Tracking

**Files:**

- Modify: `build_pipeline_reporting.py`

- [ ] **Step 1: Add renewal steps** — uses `DS_RRH` dataset

Key steps: `s_p3_open_arr`, `s_p3_atrisk`, `s_p3_count`, `s_p3_winrate`, `s_p3_grr`, `s_p3_by_risk`, `s_p3_wall_heatmap`, `s_p3_by_owner`, `s_p3_atrisk_table`

Note: `IsClosed` in Revenue_Retention_Health is numeric (`0`/`1`), not string.

- [ ] **Step 2: Add widgets including renewal wall heatmap**

```python
w["p3_wall"] = heatmap_chart("s_p3_wall_heatmap", "Renewal Wall — Monthly Risk Concentration")
```

- [ ] **Step 3: Add record action and manager column to at-risk table**

- [ ] **Step 4: Add page layout and commit**

```bash
git commit -m "feat: Page 3.0 Renewals Tracking with renewal wall heatmap"
```

### Task 9: Page 4.0 — Churn Risk & Trends

**Files:**

- Modify: `build_pipeline_reporting.py`

- [ ] **Step 1: Add churn steps** — uses `DS_RPA` dataset

Key steps: `s_p4_churn_arr`, `s_p4_churn_rate`, `s_p4_nrr`, `s_p4_grr`, `s_p4_protected`, `s_p4_landscape` (bubble), `s_p4_by_product`, `s_p4_region_industry` (heatmap), `s_p4_trend` (combo), `s_p4_table`

- [ ] **Step 2: Add widgets including churn risk bubble and region×industry heatmap**

```python
w["p4_landscape"] = bubble_chart("s_p4_landscape", "Churn Risk Landscape")
w["p4_heatmap"]   = heatmap_chart("s_p4_region_industry", "Churn by Region × Industry")
w["p4_trend"]     = combo_chart("s_p4_trend", "Churn Trend & NRR",
                                 ["QuarterLabel"], ["ChurnARR"], ["NRR"])
```

- [ ] **Step 3: Add record action to churned accounts table (links to Account, not Opportunity)**

```python
add_table_action(w["p4_table"], "salesforceActions", "Account", "AccountId")
```

- [ ] **Step 4: Add page layout and commit**

```bash
git commit -m "feat: Page 4.0 Churn Risk with bubble landscape, region×industry heatmap, combo trend"
```

### Task 10: Page 5.0 — Slipped Deals Analysis

**Files:**

- Modify: `build_pipeline_reporting.py`

- [ ] **Step 1: Add slippage steps** — uses `DS_PIPE` dataset

Key steps: `s_p5_count`, `s_p5_arr`, `s_p5_avg_push`, `s_p5_avg_days`, `s_p5_repeat`, `s_p5_distribution`, `s_p5_by_region`, `s_p5_trend`, `s_p5_table`, `s_p5_by_motion`, `s_p5_risk_landscape` (bubble)

- [ ] **Step 2: Add widgets including slippage risk bubble**

```python
w["p5_risk_bubble"] = bubble_chart("s_p5_risk_landscape", "Slippage Risk Landscape")
```

- [ ] **Step 3: Add record action to slipped deals table**

- [ ] **Step 4: Add page layout and commit**

```bash
git commit -m "feat: Page 5.0 Slipped Deals with risk landscape bubble and root cause table"
```

### Task 11: Dashboard 1 — main() and deploy

**Files:**

- Modify: `build_pipeline_reporting.py`
- Create: `tests/test_build_pipeline_reporting.py`

- [ ] **Step 1: Write the `main()` function**

```python
def main():
    inst, tok = get_auth()
    _resolve_dataset_ids(inst, tok)
    dashboard_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)

    steps   = build_steps()
    widgets = build_widgets()
    layout  = build_layout()
    state   = build_dashboard_state(steps, widgets, layout)

    deploy_dashboard(inst, tok, dashboard_id, state)
    print(f"Dashboard '{DASHBOARD_LABEL}' deployed: {dashboard_id}")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write smoke tests**

```python
# tests/test_build_pipeline_reporting.py
import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from build_pipeline_reporting import build_steps, build_widgets, build_layout

class TestPipelineReportingSmoke(unittest.TestCase):
    def test_steps_generated(self):
        # Mock dataset IDs needed for af() steps
        import build_pipeline_reporting as m
        m.DS_PIPE_ID = "fake_pipe_id"
        m.DS_FRM_ID  = "fake_frm_id"
        m.DS_RRH_ID  = "fake_rrh_id"
        m.DS_RPA_ID  = "fake_rpa_id"
        s = build_steps()
        self.assertIn("f_fy", s)
        self.assertIn("s_p1_open_arr", s)
        self.assertGreater(len(s), 30)

    def test_widgets_generated(self):
        w = build_widgets()
        self.assertIn("p1_kpi_open", w)
        self.assertIn("p1_waterfall", w)
        self.assertGreater(len(w), 40)

    def test_layout_has_8_pages(self):
        l = build_layout()
        self.assertEqual(len(l["pages"]), 8)

if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run tests**

Run: `python3 -m pytest tests/test_build_pipeline_reporting.py -v`
Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add build_pipeline_reporting.py tests/test_build_pipeline_reporting.py
git commit -m "feat: Dashboard 1 complete — Pipeline Reporting & Insights, 8 pages, main() + tests"
```

---

## Phase 4: Dashboard 2 — Sales Ops Data Quality & Forecast Accuracy

### Task 12: Build `build_sales_ops_reporting.py` — all 3 pages

**Files:**

- Create: `build_sales_ops_reporting.py`
- Create: `tests/test_build_sales_ops_reporting.py`

This dashboard is smaller (3 pages, ~28 widgets). Build all pages in one task.

- [ ] **Step 1: Create builder with constants, filters, and all steps**

Uses 5 datasets: `Customer_Account_Health`, `Account_Intelligence`, `Pipeline_Opportunity_Operations`, `Opp_Mgmt_KPIs`, `Forecast_Intelligence`.

Key filter field mapping (from spec):

- `Customer_Account_Health`: `MonthLabel`, `UnitGroup` only
- `Account_Intelligence`: `UnitGroup` only
- `Opp_Mgmt_KPIs`: `FYLabel`, `FiscalQuarter` (int), `CloseMonth`, `UnitGroup`, `SalesRegion`
- `Pipeline_Opportunity_Operations`: all 5 filters
- `Forecast_Intelligence`: `FYLabel`, `CloseQuarter`, `UnitGroup`

- [ ] **Step 2: Add Page 1.0 — Account Data Quality (9 widgets)**

Steps for: avg quality score, DUNS fill rate, unit group fill rate, KYC %, poor accounts count, completeness bar, quality by unit group, quality trend, worst accounts table.

- [ ] **Step 3: Add Page 1.1 — Opportunity Data Hygiene (10 widgets)**

Steps for: win/loss fill rate, past due count, past due ARR, forecast category %, stale %, past due by owner, missing reason trend, hygiene heatmap, missing reason table, past due table (with record action).

- [ ] **Step 4: Add Page 2.0 — Forecast Accuracy (9 widgets)**

Steps for: accuracy %, quota attainment, commit conversion, bias, coverage, accuracy by unit group (column), accuracy trend (combo), rep accuracy (bubble), rep accuracy table.

Add cross-dashboard nav link from rep table to Forecast & Revenue Motions dashboard (`0FKTb0000000JCLOA2`). Implementation: add a `nav_link()` widget below the table with URL pattern `#/dashboard/0FKTb0000000JCLOA2?fv0={OwnerName}&fv1={CloseQuarter}`. Alternatively, use a text widget with a static link if dynamic filtering is not feasible.

Note: `SalesRegion` and `MonthLabel` filters should be excluded from Page 2.0 step bindings — `Forecast_Intelligence` does not have these fields.

- [ ] **Step 5: Add main() and page layouts**

- [ ] **Step 6: Write smoke tests**

```python
# tests/test_build_sales_ops_reporting.py
class TestSalesOpsSmoke(unittest.TestCase):
    def test_layout_has_3_pages(self):
        l = build_layout()
        self.assertEqual(len(l["pages"]), 3)
```

- [ ] **Step 7: Run all tests**

Run: `python3 -m pytest tests/ -v`
Expected: All tests across all test files pass.

- [ ] **Step 8: Commit**

```bash
git add build_sales_ops_reporting.py tests/test_build_sales_ops_reporting.py
git commit -m "feat: Dashboard 2 complete — Sales Ops Data Quality & Forecast Accuracy, 3 pages"
```

---

## Phase 5: Deploy and Verify

### Task 13: Deploy datasets and dashboards

**Files:**

- Run: `build_retention_product_analysis.py`
- Run: `build_pipeline_opportunity_operations.py` (to pick up NextStep + RootCauseHypothesis)
- Run: `build_pipeline_reporting.py`
- Run: `build_sales_ops_reporting.py`

- [ ] **Step 1: Upload Retention_Product_Analysis dataset**

```bash
cd /Users/test/azure-storage-optimizer/book-of-business
python3 build_retention_product_analysis.py
```

Expected: "Dataset Retention_Product_Analysis uploaded successfully."

- [ ] **Step 2: Re-upload Pipeline_Opportunity_Operations (with new fields)**

```bash
python3 build_pipeline_opportunity_operations.py
```

- [ ] **Step 3: Deploy Dashboard 1**

```bash
python3 build_pipeline_reporting.py
```

Expected: Dashboard created/updated, ID printed.

- [ ] **Step 4: Deploy Dashboard 2**

```bash
python3 build_sales_ops_reporting.py
```

Expected: Dashboard created/updated, ID printed.

- [ ] **Step 5: Visual verification in Salesforce**

Open each dashboard in CRM Analytics and verify:

- All 8 pages render on Dashboard 1
- All 3 pages render on Dashboard 2
- Filters work (click a region, verify KPIs update)
- Record actions work (click an opportunity name, verify it navigates to SF record)
- Waterfall, Sankey, bubble charts render correctly
- Heatmaps show data (not empty)

- [ ] **Step 6: Record dashboard IDs**

Update `CLAUDE_HANDOFF.md` with the new dashboard IDs.

- [ ] **Step 7: Final commit**

```bash
git add -A
git commit -m "deploy: Pipeline Reporting & Sales Ops dashboards live"
```
