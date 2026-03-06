#!/usr/bin/env python3
"""Build the Customer Intelligence dashboard — unified view of current customers.

ML-Forward Upgrade:
  Joins Account + Opportunity + Contract + Contact data into a single per-account
  dataset with computed health scores, expansion signals, product adoption,
  NRR components, and engagement metrics.

  ML additions:
    - Predictive churn scoring (rule-based → probability + risk band + drivers)
    - Predictive expansion scoring (probability + band + drivers)
    - Expected churn ARR computation (probability × ARR at risk)
    - Cohort retention analytics (GRR/NRR by cohort quarter)

Pages:
  1. Portfolio Overview — Total customers, ARR, NRR/GRR proxy, health distribution
  2. Customer Health & Risk — Composite health score, scatter, at-risk table
  3. Product Adoption — Product penetration, SaaS/Axioma, cross-sell whitespace
  4. Revenue Expansion — Expand pipeline, signal scoring, whitespace accounts
  5. Contract & Renewal — Renewal calendar, term distribution, at-risk renewals
  6. Customer Segmentation — Segment performance, cohort, revenue concentration
  7. Engagement & Contacts — Contact coverage, C-level penetration, activity
  8. Advanced Analytics — Sankey, treemap, heatmap, bubble, area, stats
  9. Retention & Growth — Cohort retention heatmap, expected churn waterfall, at-risk actions

Dataset: Customer_Intelligence (one row per account)
"""

import csv
import io
from datetime import datetime

from crm_analytics_helpers import (
    get_auth,
    _soql,
    _dim,
    _measure,
    upload_dataset,
    get_dataset_id,
    sq,
    af,
    num,
    num_dynamic_color,
    rich_chart,
    gauge,
    treemap_chart,
    bubble_chart,
    heatmap_chart,
    waterfall_chart,
    sankey_chart,
    area_chart,
    bullet_chart,
    timeline_chart,
    combo_chart,
    scatter_chart,
    pillbox,
    coalesce_filter,
    hdr,
    section_label,
    nav_link,
    pg,
    nav_row,
    build_dashboard_state,
    deploy_dashboard,
    create_dashboard_if_needed,
    set_record_links_xmd,  # noqa: F401
    add_selection_interaction,
    add_table_action,
    # ML-Forward additions
    compute_churn_probability,
    compute_expansion_probability,
)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

DS = "Customer_Intelligence"
DS_LABEL = "Customer Intelligence"
DASHBOARD_LABEL = "Customer Intelligence"
TODAY = datetime.now().strftime("%Y-%m-%d")

# Filter bindings
UF = coalesce_filter("f_unit", "UnitGroup")
IF = coalesce_filter("f_industry", "Industry")
HF = coalesce_filter("f_health", "HealthBand")
SF = coalesce_filter("f_segment", "Segment")

# Page nav config
PAGE_IDS = [
    "portfolio",
    "health",
    "product",
    "expansion",
    "contracts",
    "segments",
    "engagement",
    "advanalytics",
    "retention",
]
PAGE_LABELS = [
    "Portfolio",
    "Health & Risk",
    "Product Adoption",
    "Expansion",
    "Contracts",
    "Segmentation",
    "Engagement",
    "Advanced",
    "Retention",
]
NUM_PAGES = len(PAGE_IDS)


# ═══════════════════════════════════════════════════════════════════════════
#  Dataset creation
# ═══════════════════════════════════════════════════════════════════════════


def _health_score(acct):
    """Compute composite health score (0-100) from account attributes.

    Factors:
      - Contract status (0-20): Active contracts = 20, expiring soon = 10
      - Revenue trend (0-20): Won ARR growing vs shrinking
      - Engagement recency (0-15): Recent activity = 15
      - Contact coverage (0-15): Multiple contacts = 15
      - Product adoption (0-15): Multi-product = 15
      - Risk flags (0-15): No termination risk = 15
    """
    score = 0

    # Contract status
    active = acct.get("ActiveContracts", 0)
    expiring_90 = acct.get("ExpiringContracts90d", 0)
    if active > 0 and expiring_90 == 0:
        score += 20
    elif active > 0:
        score += 10
    elif acct.get("TotalContracts", 0) > 0:
        score += 5

    # Revenue trend (compare FY26 won vs FY25 won)
    won_26 = acct.get("WonARR_FY26", 0)
    won_25 = acct.get("WonARR_FY25", 0)
    if won_26 > won_25 and won_25 > 0:
        score += 20
    elif won_26 > 0:
        score += 15
    elif won_25 > 0:
        score += 10

    # Engagement recency
    last_activity = acct.get("LastActivityDate", "")
    if last_activity and last_activity >= (
        datetime.now().strftime("%Y-%m-%d")[:8] + "01"
    ):
        score += 15  # This month
    elif last_activity and last_activity >= "2025-10-01":
        score += 10  # Last 6 months
    elif last_activity:
        score += 5

    # Contact coverage
    contacts = acct.get("ContactCount", 0)
    clevel = acct.get("CLevelContacts", 0)
    if contacts >= 5 and clevel >= 1:
        score += 15
    elif contacts >= 3:
        score += 10
    elif contacts >= 1:
        score += 5

    # Product adoption
    products = acct.get("ProductCount", 0)
    if products >= 3:
        score += 15
    elif products >= 2:
        score += 10
    elif products >= 1:
        score += 5

    # Risk flags
    risk = acct.get("RiskLevel", "")
    term_date = acct.get("TerminationDate", "")
    if not risk and not term_date:
        score += 15
    elif risk in ("Low", ""):
        score += 10
    elif risk == "Medium":
        score += 5

    return max(0, min(100, score))


def _health_band(score):
    if score >= 70:
        return "Healthy"
    elif score >= 40:
        return "At Risk"
    else:
        return "Critical"


def _expansion_score(acct):
    """Compute expansion signal score (0-100).

    Factors: open pipeline, product whitespace, contract renewal timing,
    engagement level, historical expansion success.
    """
    score = 0

    # Open expansion pipeline
    expand_arr = acct.get("ExpandPipelineARR", 0)
    if expand_arr > 100000:
        score += 25
    elif expand_arr > 0:
        score += 15

    # Product whitespace (fewer products = more opportunity)
    products = acct.get("ProductCount", 0)
    is_saas = acct.get("IsSaaS", "false") == "true"
    is_axioma = acct.get("IsAxioma", "false") == "true"
    if products < 2 and (is_saas or is_axioma):
        score += 20  # Major whitespace
    elif products < 3:
        score += 10

    # Renewal timing (approaching renewal = expansion opportunity)
    expiring_90 = acct.get("ExpiringContracts90d", 0)
    if expiring_90 > 0:
        score += 15

    # Engagement level
    contacts = acct.get("ContactCount", 0)
    if contacts >= 5:
        score += 15
    elif contacts >= 2:
        score += 10

    # Historical expansion success
    expand_won = acct.get("ExpandWonARR", 0)
    if expand_won > 0:
        score += 25
    elif acct.get("TotalWonARR", 0) > 0:
        score += 10

    return max(0, min(100, score))


def _segment(aum, arr):
    """Classify account into segments based on AuM and ARR."""
    if aum > 100000 or arr > 500000:
        return "Enterprise"
    elif aum > 10000 or arr > 100000:
        return "Mid-Market"
    elif arr > 0:
        return "Growth"
    else:
        return "Prospect"


def create_dataset(inst, tok):
    """Build Customer_Intelligence from Account + Opportunity + Contract + Contact."""
    print("\n=== Building Customer Intelligence dataset ===")

    # 1. Query Accounts
    accounts = _soql(
        inst,
        tok,
        "SELECT Id, Name, Owner.Name, Type, CreatedDate, BillingCountry, "
        "Industry, Unit__c, Unit_Group__c, SaaS_Client__c, Axioma_Client__c, "
        "Risk_of_Potential_Termination__c, KYC_Approval_Status__c, "
        "DUNS_No__c, Partner_Engagement_Level__c, "
        "APTS_Subscription_Term__c, Termination_Date__c, "
        "Expected_Termination_Date__c, Termination_Reason__c, "
        "AuM_m__c, NumberOfEmployees "
        "FROM Account "
        "WHERE CreatedDate >= 2020-01-01T00:00:00Z",
    )
    print(f"  Queried {len(accounts)} accounts")

    # 2. Query Opportunities (aggregate per account)
    opps = _soql(
        inst,
        tok,
        "SELECT Id, AccountId, Type, IsClosed, IsWon, "
        "FiscalYear, StageName, ForecastCategoryName, "
        "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
        "APTS_RH_Product_Family__c, CloseDate "
        "FROM Opportunity "
        "WHERE FiscalYear IN (2024, 2025, 2026, 2027)",
    )
    print(f"  Queried {len(opps)} opportunities")

    # 3. Query Contracts
    contracts = _soql(
        inst,
        tok,
        "SELECT Id, AccountId, Status, StartDate, EndDate, "
        "ContractTerm, Agreement_Type__c "
        "FROM Contract WHERE CreatedDate >= 2022-01-01T00:00:00Z",
    )
    print(f"  Queried {len(contracts)} contracts")

    # 4. Query Contacts
    contacts = _soql(
        inst,
        tok,
        "SELECT Id, AccountId, Title, LastActivityDate "
        "FROM Contact WHERE CreatedDate >= 2022-01-01T00:00:00Z",
    )
    print(f"  Queried {len(contacts)} contacts")

    # ── Aggregate per account ──

    # Opp aggregation
    acct_opps = {}
    for o in opps:
        aid = o.get("AccountId")
        if not aid:
            continue
        if aid not in acct_opps:
            acct_opps[aid] = {
                "TotalWonARR": 0,
                "WonARR_FY25": 0,
                "WonARR_FY26": 0,
                "WonARR_FY24": 0,
                "TotalLostARR": 0,
                "OpenPipelineARR": 0,
                "ExpandPipelineARR": 0,
                "RenewalPipelineARR": 0,
                "LandPipelineARR": 0,
                "ExpandWonARR": 0,
                "RenewalWonARR": 0,
                "LandWonARR": 0,
                "WonCount": 0,
                "LostCount": 0,
                "OpenCount": 0,
                "Products": set(),
                "LastWonDate": "",
                "LastLostDate": "",
                "FirstLandWonDate": "",
                "FirstExpandWonDate": "",
            }
        d = acct_opps[aid]
        arr = o.get("ConvertedARR") or 0
        is_closed = str(o.get("IsClosed", False)).lower() == "true"
        is_won = str(o.get("IsWon", False)).lower() == "true"
        fy = o.get("FiscalYear") or 0
        opp_type = o.get("Type") or ""
        product = o.get("APTS_RH_Product_Family__c") or ""
        close_date = o.get("CloseDate") or ""

        if product:
            d["Products"].add(product)

        if is_won:
            d["TotalWonARR"] += arr
            d["WonCount"] += 1
            if close_date > d["LastWonDate"]:
                d["LastWonDate"] = close_date
            if fy == 2026:
                d["WonARR_FY26"] += arr
            elif fy == 2025:
                d["WonARR_FY25"] += arr
            elif fy == 2024:
                d["WonARR_FY24"] += arr
            if opp_type == "Expand":
                d["ExpandWonARR"] += arr
                if not d["FirstExpandWonDate"] or close_date < d["FirstExpandWonDate"]:
                    d["FirstExpandWonDate"] = close_date
            elif opp_type == "Renewal":
                d["RenewalWonARR"] += arr
            elif opp_type == "Land":
                d["LandWonARR"] += arr
                if not d["FirstLandWonDate"] or close_date < d["FirstLandWonDate"]:
                    d["FirstLandWonDate"] = close_date
        elif is_closed:
            d["TotalLostARR"] += arr
            d["LostCount"] += 1
            if close_date > d["LastLostDate"]:
                d["LastLostDate"] = close_date
        else:
            d["OpenPipelineARR"] += arr
            d["OpenCount"] += 1
            if opp_type == "Expand":
                d["ExpandPipelineARR"] += arr
            elif opp_type == "Renewal":
                d["RenewalPipelineARR"] += arr
            elif opp_type == "Land":
                d["LandPipelineARR"] += arr

    # Contract aggregation
    acct_contracts = {}
    for c in contracts:
        aid = c.get("AccountId")
        if not aid:
            continue
        if aid not in acct_contracts:
            acct_contracts[aid] = {
                "TotalContracts": 0,
                "ActiveContracts": 0,
                "ExpiringContracts90d": 0,
                "ExpiringContracts180d": 0,
                "AvgTermMonths": 0,
                "TermSum": 0,
                "MultiYearCount": 0,
                "LatestEndDate": "",
            }
        d = acct_contracts[aid]
        d["TotalContracts"] += 1
        status = c.get("Status") or ""
        end_date = c.get("EndDate") or ""
        term = c.get("ContractTerm") or 0

        if status in ("Activated", "Active"):
            d["ActiveContracts"] += 1
        if end_date:
            if end_date > d["LatestEndDate"]:
                d["LatestEndDate"] = end_date
            # Days to expiry
            try:
                exp = datetime.strptime(end_date[:10], "%Y-%m-%d")
                days_to = (exp - datetime.now()).days
                if 0 < days_to <= 90:
                    d["ExpiringContracts90d"] += 1
                if 0 < days_to <= 180:
                    d["ExpiringContracts180d"] += 1
            except (ValueError, TypeError):
                pass
        if term:
            d["TermSum"] += term
            if term > 12:
                d["MultiYearCount"] += 1

    # Contact aggregation
    acct_contacts = {}
    for c in contacts:
        aid = c.get("AccountId")
        if not aid:
            continue
        if aid not in acct_contacts:
            acct_contacts[aid] = {
                "ContactCount": 0,
                "CLevelContacts": 0,
                "LastActivityDate": "",
                "RecentActivityCount": 0,
            }
        d = acct_contacts[aid]
        d["ContactCount"] += 1
        title = (c.get("Title") or "").lower()
        if any(
            t in title
            for t in [
                "chief",
                "ceo",
                "cfo",
                "cto",
                "coo",
                "cio",
                "president",
                "vp",
                "vice president",
                "managing director",
                "head of",
            ]
        ):
            d["CLevelContacts"] += 1
        activity = c.get("LastActivityDate") or ""
        if activity and activity > d["LastActivityDate"]:
            d["LastActivityDate"] = activity
        if activity and activity >= "2025-09-01":
            d["RecentActivityCount"] += 1

    # ── Build per-account rows ──
    fields = [
        "AccountId",
        "AccountName",
        "OwnerName",
        "UnitGroup",
        "Industry",
        "BillingCountry",
        "AccountType",
        "CreatedDate",
        "CustomerSince",
        "IsSaaS",
        "IsAxioma",
        "AuM",
        "Employees",
        "RiskLevel",
        "KYCStatus",
        "TerminationDate",
        # Opportunity metrics
        "TotalWonARR",
        "WonARR_FY24",
        "WonARR_FY25",
        "WonARR_FY26",
        "TotalLostARR",
        "OpenPipelineARR",
        "ExpandPipelineARR",
        "RenewalPipelineARR",
        "LandPipelineARR",
        "ExpandWonARR",
        "RenewalWonARR",
        "LandWonARR",
        "WonCount",
        "LostCount",
        "OpenCount",
        "ProductCount",
        "ProductList",
        "LastWonDate",
        "LastLostDate",
        # Contract metrics
        "TotalContracts",
        "ActiveContracts",
        "ExpiringContracts90d",
        "ExpiringContracts180d",
        "AvgTermMonths",
        "MultiYearCount",
        "LatestEndDate",
        # Contact metrics
        "ContactCount",
        "CLevelContacts",
        "LastActivityDate",
        "RecentActivityCount",
        # Computed scores
        "HealthScore",
        "HealthBand",
        "ExpansionScore",
        "ExpansionBand",
        "Segment",
        "NRR_Proxy",
        # Enhancement fields
        "GRR_Proxy",
        "IsRetained",
        "LifecycleStage",
        "LandToExpandDays",
        "ContactTier",
        "ProductCombo",
        # ML-Forward: Churn prediction
        "ChurnProbability",
        "ChurnRiskBand",
        "ChurnDriver1",
        "ChurnDriver2",
        "ExpectedChurnARR",
        # ML-Forward: Expansion prediction
        "ExpansionProbability",
        "ExpansionPropensityBand",
        "ExpansionDriver1",
        "ExpansionDriver2",
        # ML-Forward: Cohort tracking
        "CohortQuarter",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    row_count = 0

    for a in accounts:
        aid = a.get("Id")
        if not aid:
            continue

        opp_data = acct_opps.get(aid, {})
        con_data = acct_contracts.get(aid, {})
        contact_data = acct_contacts.get(aid, {})

        # Skip accounts with zero revenue and zero pipeline and zero contracts
        total_won = opp_data.get("TotalWonARR", 0)
        open_pipe = opp_data.get("OpenPipelineARR", 0)
        total_contracts = con_data.get("TotalContracts", 0)
        if total_won == 0 and open_pipe == 0 and total_contracts == 0:
            continue

        unit_group = a.get("Unit_Group__c") or ""
        aum = a.get("AuM_m__c") or 0
        created = (a.get("CreatedDate") or "")[:10]

        # Build temp dict for scoring
        scoring_input = {
            "ActiveContracts": con_data.get("ActiveContracts", 0),
            "ExpiringContracts90d": con_data.get("ExpiringContracts90d", 0),
            "TotalContracts": con_data.get("TotalContracts", 0),
            "WonARR_FY26": opp_data.get("WonARR_FY26", 0),
            "WonARR_FY25": opp_data.get("WonARR_FY25", 0),
            "LastActivityDate": contact_data.get("LastActivityDate", ""),
            "ContactCount": contact_data.get("ContactCount", 0),
            "CLevelContacts": contact_data.get("CLevelContacts", 0),
            "ProductCount": len(opp_data.get("Products", set())),
            "RiskLevel": a.get("Risk_of_Potential_Termination__c") or "",
            "TerminationDate": (a.get("Termination_Date__c") or "")[:10],
            "ExpandPipelineARR": opp_data.get("ExpandPipelineARR", 0),
            "IsSaaS": str(a.get("SaaS_Client__c", False)).lower(),
            "IsAxioma": str(a.get("Axioma_Client__c", False)).lower(),
            "ExpandWonARR": opp_data.get("ExpandWonARR", 0),
            "TotalWonARR": total_won,
        }

        health = _health_score(scoring_input)
        expansion = _expansion_score(scoring_input)
        segment = _segment(aum, total_won)

        # NRR proxy: (FY26 won / FY25 won) * 100
        won_25 = opp_data.get("WonARR_FY25", 0)
        won_26 = opp_data.get("WonARR_FY26", 0)
        nrr = round((won_26 / won_25) * 100, 1) if won_25 > 0 else 0

        term_sum = con_data.get("TermSum", 0)
        total_c = con_data.get("TotalContracts", 0)
        avg_term = round(term_sum / total_c, 1) if total_c > 0 else 0

        products = opp_data.get("Products", set())

        # GRR proxy: min(FY26 renewal ARR / FY25 total, 100%)
        fy25_total = won_25
        fy26_renewal = opp_data.get("RenewalWonARR", 0)
        grr = (
            round(min((fy26_renewal / fy25_total) * 100, 200), 1)
            if fy25_total > 0
            else 0
        )

        # Logo retained: FY25 customer has FY26 revenue
        is_retained = "true" if won_25 > 0 and won_26 > 0 else "false"

        # Lifecycle stage
        customer_since = created[:4] if created else ""
        expand_won = opp_data.get("ExpandWonARR", 0)
        lost_arr = opp_data.get("TotalLostARR", 0)
        if health < 40 and total_won > 0:
            lifecycle = "At-Risk"
        elif lost_arr > 0 and won_26 == 0:
            lifecycle = "Churning"
        elif customer_since and int(customer_since) >= 2025:
            lifecycle = "Onboarding"
        elif expand_won > 0:
            lifecycle = "Growing"
        elif customer_since and int(customer_since) <= 2022:
            lifecycle = "Mature"
        else:
            lifecycle = "Stable"

        # Land-to-Expand days
        first_land = opp_data.get("FirstLandWonDate", "")
        first_expand = opp_data.get("FirstExpandWonDate", "")
        land_to_expand = 0
        if first_land and first_expand:
            try:
                ld = datetime.strptime(first_land[:10], "%Y-%m-%d")
                ed = datetime.strptime(first_expand[:10], "%Y-%m-%d")
                land_to_expand = max(0, (ed - ld).days)
            except (ValueError, TypeError):
                pass

        # Contact coverage tier
        contact_count = contact_data.get("ContactCount", 0)
        if contact_count >= 6:
            contact_tier = "Over-indexed"
        elif contact_count >= 3:
            contact_tier = "Adequate"
        elif contact_count >= 1:
            contact_tier = "Under-indexed"
        else:
            contact_tier = "Dark"

        # Product combination (sorted, pipe-delimited)
        product_combo = "|".join(sorted(products)) if len(products) >= 2 else ""

        # ─── ML-Forward: Churn prediction ─────────────────────────────
        churn_input = {
            "HealthScore": health,
            "ExpiringContracts90d": con_data.get("ExpiringContracts90d", 0),
            "ExpiringContracts180d": con_data.get("ExpiringContracts180d", 0),
            "NRR_Proxy": nrr,
            "RecentActivityCount": contact_data.get("RecentActivityCount", 0),
            "RiskLevel": a.get("Risk_of_Potential_Termination__c") or "",
            "ProductCount": len(products),
        }
        churn_prob, churn_band, churn_d1, churn_d2 = compute_churn_probability(
            churn_input
        )

        # ─── ML-Forward: Expansion prediction ────────────────────────
        exp_input = {
            "ExpansionScore": expansion,
            "NRR_Proxy": nrr,
            "ProductCount": len(products),
            "RecentActivityCount": contact_data.get("RecentActivityCount", 0),
            "AuM": aum,
            "MultiYearCount": con_data.get("MultiYearCount", 0),
        }
        exp_prob, exp_band, exp_d1, exp_d2 = compute_expansion_probability(exp_input)

        # ─── ML-Forward: Cohort quarter ───────────────────────────────
        first_won = opp_data.get("FirstLandWonDate", "")
        if first_won:
            try:
                fwd = datetime.strptime(first_won[:10], "%Y-%m-%d")
                q = (fwd.month - 1) // 3 + 1
                cohort_qtr = f"FY{fwd.year} Q{q}"
            except (ValueError, TypeError):
                cohort_qtr = ""
        else:
            cohort_qtr = ""

        writer.writerow(
            {
                "AccountId": aid,
                "AccountName": a.get("Name") or "",
                "OwnerName": (a.get("Owner") or {}).get("Name") or "",
                "UnitGroup": unit_group,
                "Industry": a.get("Industry") or "",
                "BillingCountry": a.get("BillingCountry") or "",
                "AccountType": a.get("Type") or "",
                "CreatedDate": created,
                "CustomerSince": created[:4] if created else "",
                "IsSaaS": str(a.get("SaaS_Client__c", False)).lower(),
                "IsAxioma": str(a.get("Axioma_Client__c", False)).lower(),
                "AuM": round(aum, 2) if aum else 0,
                "Employees": a.get("NumberOfEmployees") or 0,
                "RiskLevel": a.get("Risk_of_Potential_Termination__c") or "",
                "KYCStatus": a.get("KYC_Approval_Status__c") or "",
                "TerminationDate": (a.get("Termination_Date__c") or "")[:10],
                # Opp metrics
                "TotalWonARR": round(total_won, 2),
                "WonARR_FY24": round(opp_data.get("WonARR_FY24", 0), 2),
                "WonARR_FY25": round(won_25, 2),
                "WonARR_FY26": round(won_26, 2),
                "TotalLostARR": round(opp_data.get("TotalLostARR", 0), 2),
                "OpenPipelineARR": round(open_pipe, 2),
                "ExpandPipelineARR": round(opp_data.get("ExpandPipelineARR", 0), 2),
                "RenewalPipelineARR": round(opp_data.get("RenewalPipelineARR", 0), 2),
                "LandPipelineARR": round(opp_data.get("LandPipelineARR", 0), 2),
                "ExpandWonARR": round(opp_data.get("ExpandWonARR", 0), 2),
                "RenewalWonARR": round(opp_data.get("RenewalWonARR", 0), 2),
                "LandWonARR": round(opp_data.get("LandWonARR", 0), 2),
                "WonCount": opp_data.get("WonCount", 0),
                "LostCount": opp_data.get("LostCount", 0),
                "OpenCount": opp_data.get("OpenCount", 0),
                "ProductCount": len(products),
                "ProductList": "|".join(sorted(products)) if products else "",
                "LastWonDate": opp_data.get("LastWonDate", ""),
                "LastLostDate": opp_data.get("LastLostDate", ""),
                # Contract metrics
                "TotalContracts": con_data.get("TotalContracts", 0),
                "ActiveContracts": con_data.get("ActiveContracts", 0),
                "ExpiringContracts90d": con_data.get("ExpiringContracts90d", 0),
                "ExpiringContracts180d": con_data.get("ExpiringContracts180d", 0),
                "AvgTermMonths": avg_term,
                "MultiYearCount": con_data.get("MultiYearCount", 0),
                "LatestEndDate": con_data.get("LatestEndDate", ""),
                # Contact metrics
                "ContactCount": contact_data.get("ContactCount", 0),
                "CLevelContacts": contact_data.get("CLevelContacts", 0),
                "LastActivityDate": contact_data.get("LastActivityDate", ""),
                "RecentActivityCount": contact_data.get("RecentActivityCount", 0),
                # Scores
                "HealthScore": health,
                "HealthBand": _health_band(health),
                "ExpansionScore": expansion,
                "ExpansionBand": (
                    "High"
                    if expansion >= 60
                    else "Medium"
                    if expansion >= 30
                    else "Low"
                ),
                "Segment": segment,
                "NRR_Proxy": nrr,
                # Enhancement fields
                "GRR_Proxy": grr,
                "IsRetained": is_retained,
                "LifecycleStage": lifecycle,
                "LandToExpandDays": land_to_expand,
                "ContactTier": contact_tier,
                "ProductCombo": product_combo,
                # ML-Forward: Churn prediction
                "ChurnProbability": churn_prob,
                "ChurnRiskBand": churn_band,
                "ChurnDriver1": churn_d1,
                "ChurnDriver2": churn_d2,
                "ExpectedChurnARR": round(
                    churn_prob / 100 * max(total_won, open_pipe), 2
                ),
                # ML-Forward: Expansion prediction
                "ExpansionProbability": exp_prob,
                "ExpansionPropensityBand": exp_band,
                "ExpansionDriver1": exp_d1,
                "ExpansionDriver2": exp_d2,
                # ML-Forward: Cohort
                "CohortQuarter": cohort_qtr,
            }
        )
        row_count += 1

    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes, {row_count} rows")

    fields_meta = [
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account"),
        _dim("OwnerName", "Owner"),
        _dim("UnitGroup", "Unit Group"),
        _dim("Industry", "Industry"),
        _dim("BillingCountry", "Country"),
        _dim("AccountType", "Account Type"),
        _dim("CreatedDate", "Created Date"),
        _dim("CustomerSince", "Customer Since"),
        _dim("IsSaaS", "SaaS Client"),
        _dim("IsAxioma", "Axioma Client"),
        _measure("AuM", "AuM (M)", scale=2),
        _measure("Employees", "Employees", scale=0, precision=8),
        _dim("RiskLevel", "Risk Level"),
        _dim("KYCStatus", "KYC Status"),
        _dim("TerminationDate", "Termination Date"),
        # Opp measures
        _measure("TotalWonARR", "Total Won ARR"),
        _measure("WonARR_FY24", "Won ARR FY24"),
        _measure("WonARR_FY25", "Won ARR FY25"),
        _measure("WonARR_FY26", "Won ARR FY26"),
        _measure("TotalLostARR", "Total Lost ARR"),
        _measure("OpenPipelineARR", "Open Pipeline ARR"),
        _measure("ExpandPipelineARR", "Expand Pipeline"),
        _measure("RenewalPipelineARR", "Renewal Pipeline"),
        _measure("LandPipelineARR", "Land Pipeline"),
        _measure("ExpandWonARR", "Expand Won ARR"),
        _measure("RenewalWonARR", "Renewal Won ARR"),
        _measure("LandWonARR", "Land Won ARR"),
        _measure("WonCount", "Won Deals", scale=0, precision=6),
        _measure("LostCount", "Lost Deals", scale=0, precision=6),
        _measure("OpenCount", "Open Deals", scale=0, precision=6),
        _measure("ProductCount", "Product Count", scale=0, precision=3),
        _dim("ProductList", "Products"),
        _dim("LastWonDate", "Last Won Date"),
        _dim("LastLostDate", "Last Lost Date"),
        # Contract measures
        _measure("TotalContracts", "Total Contracts", scale=0, precision=5),
        _measure("ActiveContracts", "Active Contracts", scale=0, precision=5),
        _measure("ExpiringContracts90d", "Expiring 90d", scale=0, precision=5),
        _measure("ExpiringContracts180d", "Expiring 180d", scale=0, precision=5),
        _measure("AvgTermMonths", "Avg Term (Months)", scale=1, precision=5),
        _measure("MultiYearCount", "Multi-Year Contracts", scale=0, precision=5),
        _dim("LatestEndDate", "Latest Contract End"),
        # Contact measures
        _measure("ContactCount", "Contact Count", scale=0, precision=5),
        _measure("CLevelContacts", "C-Level Contacts", scale=0, precision=5),
        _dim("LastActivityDate", "Last Activity Date"),
        _measure("RecentActivityCount", "Recent Activity Count", scale=0, precision=5),
        # Scores
        _measure("HealthScore", "Health Score", scale=0, precision=3),
        _dim("HealthBand", "Health Band"),
        _measure("ExpansionScore", "Expansion Score", scale=0, precision=3),
        _dim("ExpansionBand", "Expansion Signal"),
        _dim("Segment", "Segment"),
        _measure("NRR_Proxy", "NRR Proxy %", scale=1, precision=6),
        # Enhancement fields
        _measure("GRR_Proxy", "GRR Proxy %", scale=1, precision=6),
        _dim("IsRetained", "Is Retained"),
        _dim("LifecycleStage", "Lifecycle Stage"),
        _measure("LandToExpandDays", "Land-to-Expand Days", scale=0, precision=6),
        _dim("ContactTier", "Contact Tier"),
        _dim("ProductCombo", "Product Combination"),
        # ML-Forward: Churn prediction
        _measure("ChurnProbability", "Churn Probability %", scale=1, precision=5),
        _dim("ChurnRiskBand", "Churn Risk Band"),
        _dim("ChurnDriver1", "Churn Top Driver"),
        _dim("ChurnDriver2", "Churn Second Driver"),
        _measure("ExpectedChurnARR", "Expected Churn ARR"),
        # ML-Forward: Expansion prediction
        _measure("ExpansionProbability", "Expansion Probability %", scale=1, precision=5),
        _dim("ExpansionPropensityBand", "Expansion Propensity Band"),
        _dim("ExpansionDriver1", "Expansion Top Driver"),
        _dim("ExpansionDriver2", "Expansion Second Driver"),
        # ML-Forward: Cohort tracking
        _dim("CohortQuarter", "Cohort Quarter"),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


# ═══════════════════════════════════════════════════════════════════════════
#  Steps
# ═══════════════════════════════════════════════════════════════════════════


def build_steps(ds_id):
    DS_META = [{"id": ds_id, "name": DS}]
    L = f'q = load "{DS}";\n'

    return {
        # ── Filter steps ──
        "f_unit": af("UnitGroup", DS_META),
        "f_industry": af("Industry", DS_META),
        "f_health": af("HealthBand", DS_META),
        "f_segment": af("Segment", DS_META),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 1: Portfolio Overview
        # ═══════════════════════════════════════════════════════════════════
        # KPI: Total Customers (accounts with won ARR > 0)
        "s_total_customers": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate count() as total_customers;"
        ),
        # KPI: Total ARR (sum of FY26 won ARR across all accounts)
        "s_total_arr": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(WonARR_FY26) as total_arr;"
        ),
        # KPI: Average Health Score
        "s_avg_health": sq(
            L
            + UF
            + IF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(HealthScore) as avg_health;"
        ),
        # KPI: Average NRR Proxy
        "s_avg_nrr": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by NRR_Proxy > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(NRR_Proxy) as avg_nrr;"
        ),
        # Health band distribution (donut)
        "s_health_dist": sq(
            L
            + UF
            + IF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by HealthBand;\n"
            + "q = foreach q generate HealthBand, count() as acct_count, "
            + "sum(TotalWonARR) as total_arr;\n"
            + "q = order q by HealthBand asc;"
        ),
        # ARR by UnitGroup (treemap)
        "s_arr_by_unit": sq(
            L
            + IF
            + HF
            + SF
            + 'q = filter q by UnitGroup != "";\n'
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(WonARR_FY26) as arr_fy26, count() as acct_count;\n"
            + "q = order q by arr_fy26 desc;"
        ),
        # Segment distribution (donut)
        "s_segment_dist": sq(
            L
            + UF
            + IF
            + HF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, count() as acct_count, "
            + "sum(TotalWonARR) as total_arr;\n"
            + "q = order q by total_arr desc;"
        ),
        # ARR YoY comparison (FY24 vs FY25 vs FY26)
        "s_arr_yoy": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(WonARR_FY24) as arr_fy24, "
            + "sum(WonARR_FY25) as arr_fy25, "
            + "sum(WonARR_FY26) as arr_fy26;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 2: Customer Health & Risk
        # ═══════════════════════════════════════════════════════════════════
        # Health Score gauge (avg)
        "s_health_gauge": sq(
            L
            + UF
            + IF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(HealthScore) as avg_score;"
        ),
        # Scatter: ARR vs Health Score (bubble = AuM)
        "s_health_scatter": sq(
            L
            + UF
            + IF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = foreach q generate AccountName, "
            + "TotalWonARR as arr, HealthScore as health, "
            + "AuM as aum, HealthBand;\n"
            + "q = order q by arr desc;\n"
            + "q = limit q 50;"
        ),
        # At-risk accounts table
        "s_at_risk": sq(
            L
            + UF
            + IF
            + SF
            + 'q = filter q by HealthBand == "Critical" || HealthBand == "At Risk";\n'
            + "q = foreach q generate Id, AccountName, OwnerName, UnitGroup, "
            + "HealthScore, HealthBand, TotalWonARR, "
            + "ActiveContracts, ContactCount, LastActivityDate;\n"
            + "q = order q by TotalWonARR desc;\n"
            + "q = limit q 25;"
        ),
        # Health distribution by UnitGroup (heatmap)
        "s_health_by_unit": sq(
            L
            + IF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + 'q = filter q by UnitGroup != "";\n'
            + "q = group q by (UnitGroup, HealthBand);\n"
            + "q = foreach q generate UnitGroup, HealthBand, "
            + "count() as acct_count;\n"
            + "q = order q by UnitGroup asc;"
        ),
        # Risk breakdown (accounts with termination date or risk flag)
        "s_risk_breakdown": sq(
            L
            + UF
            + IF
            + SF
            + 'q = filter q by RiskLevel != "";\n'
            + "q = group q by RiskLevel;\n"
            + "q = foreach q generate RiskLevel, "
            + "count() as acct_count, sum(TotalWonARR) as at_risk_arr;\n"
            + "q = order q by at_risk_arr desc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 3: Product Adoption
        # ═══════════════════════════════════════════════════════════════════
        # KPI: SaaS adoption rate
        "s_saas_rate": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + 'sum(case when IsSaaS == "true" then 1 else 0 end) as saas_count, '
            + "count() as total_count, "
            + '(case when count() > 0 then sum(case when IsSaaS == "true" then 1 else 0 end) * 100 / count() else 0 end) as saas_pct;'
        ),
        # KPI: Axioma adoption rate
        "s_axioma_rate": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + 'sum(case when IsAxioma == "true" then 1 else 0 end) as axioma_count, '
            + "count() as total_count, "
            + '(case when count() > 0 then sum(case when IsAxioma == "true" then 1 else 0 end) * 100 / count() else 0 end) as axioma_pct;'
        ),
        # Product penetration by UnitGroup (stackhbar)
        "s_product_by_unit": sq(
            L
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + 'q = filter q by UnitGroup != "";\n'
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "avg(ProductCount) as avg_products, "
            + 'sum(case when IsSaaS == "true" then 1 else 0 end) as saas_count, '
            + 'sum(case when IsAxioma == "true" then 1 else 0 end) as axioma_count, '
            + "count() as total_count;\n"
            + "q = order q by total_count desc;"
        ),
        # Product count distribution (column chart)
        "s_product_dist": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = foreach q generate "
            + "(case "
            + 'when ProductCount == 0 then "a_0 Products" '
            + 'when ProductCount == 1 then "b_1 Product" '
            + 'when ProductCount == 2 then "c_2 Products" '
            + 'when ProductCount == 3 then "d_3 Products" '
            + 'else "e_4+ Products" end) as ProductBand;\n'
            + "q = group q by ProductBand;\n"
            + "q = foreach q generate ProductBand, count() as acct_count;\n"
            + "q = order q by ProductBand asc;"
        ),
        # Cross-sell whitespace (accounts with low product count but high ARR)
        "s_whitespace": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = filter q by ProductCount < 2;\n"
            + "q = foreach q generate Id, AccountName, OwnerName, UnitGroup, "
            + "TotalWonARR, ProductCount, IsSaaS, IsAxioma, "
            + "HealthScore, ExpansionScore;\n"
            + "q = order q by TotalWonARR desc;\n"
            + "q = limit q 25;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 4: Revenue Expansion
        # ═══════════════════════════════════════════════════════════════════
        # KPI: Total Expand Pipeline
        "s_expand_pipe": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ExpandPipelineARR) as expand_pipe;"
        ),
        # KPI: Total Expand Won ARR
        "s_expand_won": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ExpandWonARR) as expand_won;"
        ),
        # Expansion score distribution (column)
        "s_expansion_dist": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by ExpansionBand;\n"
            + "q = foreach q generate ExpansionBand, "
            + "count() as acct_count, "
            + "sum(TotalWonARR) as existing_arr;\n"
            + "q = order q by ExpansionBand asc;"
        ),
        # Top expansion opportunity accounts
        "s_expansion_top": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by ExpansionScore >= 30;\n"
            + "q = foreach q generate Id, AccountName, OwnerName, UnitGroup, "
            + "TotalWonARR, ExpandPipelineARR, ExpansionScore, "
            + "ExpansionBand, ProductCount, HealthScore;\n"
            + "q = order q by ExpansionScore desc;\n"
            + "q = limit q 25;"
        ),
        # Expand pipeline by UnitGroup
        "s_expand_by_unit": sq(
            L
            + IF
            + HF
            + SF
            + 'q = filter q by UnitGroup != "";\n'
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(ExpandPipelineARR) as expand_pipe, "
            + "sum(ExpandWonARR) as expand_won;\n"
            + "q = order q by expand_pipe desc;"
        ),
        # NRR by UnitGroup
        "s_nrr_by_unit": sq(
            L
            + IF
            + HF
            + SF
            + 'q = filter q by UnitGroup != "";\n'
            + "q = filter q by WonARR_FY25 > 0;\n"
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "sum(WonARR_FY25) as fy25_arr, "
            + "sum(WonARR_FY26) as fy26_arr, "
            + "(case when sum(WonARR_FY25) > 0 "
            + "then (sum(WonARR_FY26) / sum(WonARR_FY25)) * 100 "
            + "else 0 end) as nrr_pct;\n"
            + "q = order q by nrr_pct desc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 5: Contract & Renewal
        # ═══════════════════════════════════════════════════════════════════
        # KPI: Active Contracts
        "s_active_contracts": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ActiveContracts) as active_count;"
        ),
        # KPI: Expiring Next 90d
        "s_expiring_90": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ExpiringContracts90d) as exp_90;"
        ),
        # KPI: Expiring Next 180d
        "s_expiring_180": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ExpiringContracts180d) as exp_180;"
        ),
        # KPI: Multi-Year Contracts
        "s_multiyear": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(MultiYearCount) as multi_year;"
        ),
        # Avg term by UnitGroup
        "s_term_by_unit": sq(
            L
            + IF
            + HF
            + SF
            + 'q = filter q by UnitGroup != "";\n'
            + "q = filter q by TotalContracts > 0;\n"
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "avg(AvgTermMonths) as avg_term, "
            + "sum(MultiYearCount) as multi_year, "
            + "sum(TotalContracts) as total_contracts;\n"
            + "q = order q by avg_term desc;"
        ),
        # At-risk renewals (expiring soon + low health)
        "s_renewal_risk": sq(
            L
            + UF
            + IF
            + SF
            + "q = filter q by ExpiringContracts90d > 0;\n"
            + "q = foreach q generate Id, AccountName, OwnerName, UnitGroup, "
            + "TotalWonARR, ExpiringContracts90d, HealthScore, "
            + "HealthBand, ContactCount, LastActivityDate;\n"
            + "q = order q by TotalWonARR desc;\n"
            + "q = limit q 25;"
        ),
        # Contract count by Segment
        "s_contracts_by_seg": sq(
            L
            + UF
            + IF
            + HF
            + "q = filter q by TotalContracts > 0;\n"
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, "
            + "sum(TotalContracts) as total_contracts, "
            + "sum(ActiveContracts) as active_contracts, "
            + "sum(ExpiringContracts90d) as exp_90;\n"
            + "q = order q by total_contracts desc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 6: Customer Segmentation
        # ═══════════════════════════════════════════════════════════════════
        # Segment performance (hbar)
        "s_seg_performance": sq(
            L
            + UF
            + IF
            + HF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, "
            + "count() as acct_count, "
            + "sum(TotalWonARR) as total_arr, "
            + "avg(HealthScore) as avg_health;\n"
            + "q = order q by total_arr desc;"
        ),
        # Revenue concentration (top 20 accounts = % of total)
        "s_revenue_concentration": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = foreach q generate AccountName, TotalWonARR, UnitGroup, Segment;\n"
            + "q = order q by TotalWonARR desc;\n"
            + "q = limit q 20;"
        ),
        # Cohort analysis by CustomerSince year
        "s_cohort": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + 'q = filter q by CustomerSince != "";\n'
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by CustomerSince;\n"
            + "q = foreach q generate CustomerSince, "
            + "count() as acct_count, "
            + "sum(TotalWonARR) as cohort_arr, "
            + "avg(HealthScore) as avg_health;\n"
            + "q = order q by CustomerSince asc;"
        ),
        # Industry distribution (treemap)
        "s_industry_treemap": sq(
            L
            + UF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + 'q = filter q by Industry != "";\n'
            + "q = group q by Industry;\n"
            + "q = foreach q generate Industry, "
            + "sum(TotalWonARR) as total_arr, count() as acct_count;\n"
            + "q = order q by total_arr desc;\n"
            + "q = limit q 15;"
        ),
        # Country distribution (hbar)
        "s_country_dist": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + 'q = filter q by BillingCountry != "";\n'
            + "q = group q by BillingCountry;\n"
            + "q = foreach q generate BillingCountry, "
            + "sum(TotalWonARR) as total_arr, count() as acct_count;\n"
            + "q = order q by total_arr desc;\n"
            + "q = limit q 15;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 7: Engagement & Contacts
        # ═══════════════════════════════════════════════════════════════════
        # KPI: Avg contacts per account
        "s_avg_contacts": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(ContactCount) as avg_contacts;"
        ),
        # KPI: Avg C-Level contacts
        "s_avg_clevel": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(CLevelContacts) as avg_clevel;"
        ),
        # Contact coverage by Segment (hbar)
        "s_contact_by_seg": sq(
            L
            + UF
            + IF
            + HF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, "
            + "avg(ContactCount) as avg_contacts, "
            + "avg(CLevelContacts) as avg_clevel, "
            + "count() as acct_count;\n"
            + "q = order q by acct_count desc;"
        ),
        # Multi-threading analysis (contact count buckets)
        "s_threading": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = foreach q generate "
            + "(case "
            + 'when ContactCount == 0 then "a_No Contacts" '
            + 'when ContactCount == 1 then "b_Single Thread" '
            + 'when ContactCount <= 3 then "c_2-3 Contacts" '
            + 'when ContactCount <= 5 then "d_4-5 Contacts" '
            + 'else "e_6+ Contacts" end) as ThreadBand;\n'
            + "q = group q by ThreadBand;\n"
            + "q = foreach q generate ThreadBand, "
            + "count() as acct_count;\n"
            + "q = order q by ThreadBand asc;"
        ),
        # Low-engagement high-ARR accounts (table)
        "s_low_engagement": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = filter q by ContactCount < 3;\n"
            + "q = foreach q generate Id, AccountName, OwnerName, UnitGroup, "
            + "TotalWonARR, ContactCount, CLevelContacts, "
            + "LastActivityDate, HealthScore;\n"
            + "q = order q by TotalWonARR desc;\n"
            + "q = limit q 25;"
        ),
        # C-Level penetration by UnitGroup
        "s_clevel_by_unit": sq(
            L
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + 'q = filter q by UnitGroup != "";\n'
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "avg(CLevelContacts) as avg_clevel, "
            + "avg(ContactCount) as avg_contacts, "
            + "count() as acct_count;\n"
            + "q = order q by avg_clevel desc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  ENHANCEMENT STEPS
        # ═══════════════════════════════════════════════════════════════════
        # ARR Waterfall components (Portfolio page)
        "s_arr_waterfall": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(LandWonARR) as new_logos, "
            + "sum(ExpandWonARR) as expansion, "
            + "sum(RenewalWonARR) as renewals, "
            + "sum(TotalLostARR) as churn;\n"
            + "q = foreach q generate "
            + 'new_logos as value, "1_New Logos" as category;\n'
            + f'q2 = load "{DS}";\n'
            + UF.replace("q =", "q2 =").replace("q by", "q2 by")
            + IF.replace("q =", "q2 =").replace("q by", "q2 by")
            + HF.replace("q =", "q2 =").replace("q by", "q2 by")
            + SF.replace("q =", "q2 =").replace("q by", "q2 by")
            + "q2 = group q2 by all;\n"
            + "q2 = foreach q2 generate "
            + 'sum(ExpandWonARR) as value, "2_Expansion" as category;\n'
            + f'q3 = load "{DS}";\n'
            + UF.replace("q =", "q3 =").replace("q by", "q3 by")
            + IF.replace("q =", "q3 =").replace("q by", "q3 by")
            + HF.replace("q =", "q3 =").replace("q by", "q3 by")
            + SF.replace("q =", "q3 =").replace("q by", "q3 by")
            + "q3 = group q3 by all;\n"
            + "q3 = foreach q3 generate "
            + 'sum(RenewalWonARR) as value, "3_Renewals" as category;\n'
            + f'q4 = load "{DS}";\n'
            + UF.replace("q =", "q4 =").replace("q by", "q4 by")
            + IF.replace("q =", "q4 =").replace("q by", "q4 by")
            + HF.replace("q =", "q4 =").replace("q by", "q4 by")
            + SF.replace("q =", "q4 =").replace("q by", "q4 by")
            + "q4 = group q4 by all;\n"
            + "q4 = foreach q4 generate "
            + '0 - sum(TotalLostARR) as value, "4_Churn" as category;\n'
            + "q = union q, q2, q3, q4;\n"
            + "q = order q by category asc;"
        ),
        # GRR and Logo Retention KPIs
        "s_grr": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by WonARR_FY25 > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(case when sum(WonARR_FY25) > 0 "
            + "then sum(RenewalWonARR) / sum(WonARR_FY25) * 100 "
            + "else 0 end) as grr_pct;"
        ),
        "s_logo_retention": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by WonARR_FY25 > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as fy25_customers, "
            + 'sum(case when IsRetained == "true" then 1 else 0 end) as retained, '
            + '(case when count() > 0 then sum(case when IsRetained == "true" then 1 else 0 end) * 100 / count() else 0 end) as logo_retention;'
        ),
        # Lifecycle distribution
        "s_lifecycle": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by LifecycleStage;\n"
            + "q = foreach q generate LifecycleStage, "
            + "count() as acct_count, sum(TotalWonARR) as stage_arr;\n"
            + "q = order q by stage_arr desc;"
        ),
        # Lifecycle by UnitGroup
        "s_lifecycle_by_unit": sq(
            L
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + 'q = filter q by UnitGroup != "";\n'
            + "q = group q by (UnitGroup, LifecycleStage);\n"
            + "q = foreach q generate UnitGroup, LifecycleStage, "
            + "count() as acct_count;\n"
            + "q = order q by UnitGroup asc;"
        ),
        # Land-to-Expand velocity
        "s_l2e_avg": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by LandToExpandDays > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(LandToExpandDays) as avg_days, "
            + "count() as expanded_count;"
        ),
        "s_l2e_by_segment": sq(
            L
            + UF
            + IF
            + HF
            + "q = filter q by LandToExpandDays > 0;\n"
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, "
            + "avg(LandToExpandDays) as avg_days, "
            + "count() as expanded_count;\n"
            + "q = order q by avg_days asc;"
        ),
        # Contact coverage tier
        "s_contact_tier": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + "q = group q by ContactTier;\n"
            + "q = foreach q generate ContactTier, "
            + "count() as acct_count, sum(TotalWonARR) as tier_arr;\n"
            + "q = order q by ContactTier asc;"
        ),
        # Contact tier by UnitGroup (heatmap)
        "s_tier_by_unit": sq(
            L
            + IF
            + HF
            + SF
            + "q = filter q by TotalWonARR > 0;\n"
            + 'q = filter q by UnitGroup != "";\n'
            + "q = group q by (UnitGroup, ContactTier);\n"
            + "q = foreach q generate UnitGroup, ContactTier, "
            + "count() as acct_count;\n"
            + "q = order q by UnitGroup asc;"
        ),
        # Product combination (top combos for 2+ product accounts)
        "s_product_combos": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + 'q = filter q by ProductCombo != "";\n'
            + "q = group q by ProductCombo;\n"
            + "q = foreach q generate ProductCombo, "
            + "count() as combo_count, sum(TotalWonARR) as combo_arr;\n"
            + "q = order q by combo_count desc;\n"
            + "q = limit q 10;"
        ),
        # ── Page 8: Advanced Analytics steps ──
        # Sankey: HealthBand → Segment flow
        "s_sankey_health_seg": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by (HealthBand, Segment);\n"
            + "q = foreach q generate HealthBand, Segment, "
            + "count() as acct_count;\n"
            + "q = order q by acct_count desc;"
        ),
        # Area: Cumulative ARR by CustomerSince year
        "s_area_arr_cumul": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + 'q = filter q by CustomerSince != "";\n'
            + "q = group q by CustomerSince;\n"
            + "q = foreach q generate CustomerSince, "
            + "sum(TotalWonARR) as period_arr;\n"
            + "q = order q by CustomerSince asc;\n"
            + "q = foreach q generate CustomerSince, period_arr, "
            + "sum(period_arr) over (order by CustomerSince "
            + "rows unbounded preceding) as cumul_arr;"
        ),
        # Bullet: Avg Health Score (target 70)
        "s_bullet_health": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "avg(HealthScore) as current_val, "
            + "70 as target_val;"
        ),
        # Bullet: Avg Expansion Score (target 60)
        "s_bullet_expansion": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "avg(ExpansionScore) as current_val, "
            + "60 as target_val;"
        ),
        # Stats: ARR percentiles (stddev, P25, P50, P75)
        "s_stat_arr_percentiles": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as total_accts, "
            + "avg(TotalWonARR) as avg_arr, "
            + "stddev(TotalWonARR) as stddev_arr, "
            + "percentile_disc(0.25) within group "
            + "(order by TotalWonARR) as p25_arr, "
            + "percentile_disc(0.50) within group "
            + "(order by TotalWonARR) as p50_arr, "
            + "percentile_disc(0.75) within group "
            + "(order by TotalWonARR) as p75_arr;"
        ),
        # Stats: Health Score percentiles by UnitGroup
        "s_stat_health_by_unit": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, "
            + "count() as acct_count, "
            + "avg(HealthScore) as avg_health, "
            + "stddev(HealthScore) as stddev_health, "
            + "percentile_disc(0.25) within group "
            + "(order by HealthScore) as p25_health, "
            + "percentile_disc(0.50) within group "
            + "(order by HealthScore) as p50_health, "
            + "percentile_disc(0.75) within group "
            + "(order by HealthScore) as p75_health;\n"
            + "q = order q by UnitGroup asc;"
        ),
        # ═══ VIZ UPGRADE: Health Transition Sankey ═══
        # Shows movement between health bands (requires PriorHealthBand field)
        "s_health_transition": sq(
            L
            + UF
            + IF
            + SF
            + 'q = filter q by PriorHealthBand != "" && PriorHealthBand != "null";\n'
            + "q = group q by (PriorHealthBand, HealthBand);\n"
            + "q = foreach q generate "
            + "PriorHealthBand as source, "
            + "HealthBand as target, "
            + "count() as cnt, "
            + "sum(TotalWonARR) as total_arr;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══ VIZ UPGRADE: Health Score Driver Waterfall ═══
        # Break down average health score into component contributions
        "s_health_drivers": sq(
            L
            + UF
            + IF
            + HF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "avg(ContractScore) as contract_component, "
            + "avg(RevenueTrendScore) as revenue_component, "
            + "avg(EngagementScore) as engagement_component, "
            + "avg(ContactScore) as contact_component, "
            + "avg(AdoptionScore) as adoption_component, "
            + "avg(RiskScore) as risk_component;"
        ),
        # ═══ VIZ UPGRADE: Renewal Timeline ═══
        # ARR at risk grouped by contract end month
        "s_renewal_timeline": sq(
            L
            + UF
            + IF
            + SF
            + 'q = filter q by ContractEndMonth != "" && ContractEndMonth != "null";\n'
            + "q = group q by ContractEndMonth;\n"
            + "q = foreach q generate ContractEndMonth, "
            + "sum(TotalWonARR) as expiring_arr, "
            + "count() as acct_count, "
            + "avg(HealthScore) as avg_health;\n"
            + "q = order q by ContractEndMonth asc;\n"
            + "q = limit q 24;"
        ),
        # ═══ VIZ UPGRADE: Revenue Concentration Curve ═══
        # Top accounts ranked by ARR for Pareto analysis
        "s_revenue_concentration": sq(
            L
            + UF
            + IF
            + SF
            + "q = group q by AccountName;\n"
            + "q = foreach q generate AccountName, "
            + "sum(TotalWonARR) as acct_arr, "
            + "max(HealthScore) as health;\n"
            + "q = order q by acct_arr desc;\n"
            + "q = limit q 50;"
        ),
        # ═══ VIZ UPGRADE: Adoption × Segment Heatmap ═══
        "s_adoption_segment": sq(
            L
            + UF
            + IF
            + SF
            + "q = group q by (ProductCount, Segment);\n"
            + "q = foreach q generate "
            + 'ProductCount as ProductCount, '
            + "Segment, "
            + "count() as acct_count, "
            + "sum(TotalWonARR) as total_arr;\n"
            + "q = order q by ProductCount asc;"
        ),
        # ═══ VIZ UPGRADE: Dynamic KPI Thresholds ═══
        "s_ci_kpi_thresh": sq(
            L
            + UF
            + IF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "avg(HealthScore) as avg_health, "
            + "(sum(case when HealthBand == \"At-Risk\" then 1 else 0 end) * 100 / count()) as at_risk_pct, "
            + "count() as total_customers;"
        ),
        # ═══ PAGE 9: Retention & Growth (ML-Forward) ═══
        # Cohort retention heatmap: CohortQuarter × NRR proxy
        "s_cohort_nrr": sq(
            L
            + UF
            + SF
            + "q = filter q by CohortQuarter != \"\";\n"
            + "q = group q by CohortQuarter;\n"
            + "q = foreach q generate CohortQuarter, "
            + "count() as cohort_size, "
            + "avg(NRR_Proxy) as avg_nrr, "
            + "avg(GRR_Proxy) as avg_grr, "
            + "sum(case when IsRetained == \"true\" then 1 else 0 end) as retained_count, "
            + "(sum(case when IsRetained == \"true\" then 1 else 0 end) * 100 / count()) as retention_rate;\n"
            + "q = order q by CohortQuarter asc;"
        ),
        # Churn probability distribution
        "s_churn_dist": sq(
            L
            + UF
            + SF
            + "q = group q by ChurnRiskBand;\n"
            + "q = foreach q generate ChurnRiskBand, "
            + "count() as acct_count, "
            + "sum(ExpectedChurnARR) as expected_churn_arr, "
            + "sum(TotalWonARR) as total_arr;\n"
            + "q = order q by (case "
            + "when ChurnRiskBand == \"High\" then 1 "
            + "when ChurnRiskBand == \"Medium\" then 2 "
            + "else 3 end) asc;"
        ),
        # Expected churn by segment (waterfall-like)
        "s_churn_by_segment": sq(
            L
            + UF
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, "
            + "sum(ExpectedChurnARR) as expected_churn, "
            + "sum(TotalWonARR) as total_arr, "
            + "(sum(ExpectedChurnARR) * 100 / sum(TotalWonARR)) as churn_pct;\n"
            + "q = order q by expected_churn desc;"
        ),
        # Churn drivers (top reasons)
        "s_churn_drivers": sq(
            L
            + UF
            + SF
            + "q = group q by ChurnDriver1;\n"
            + "q = foreach q generate ChurnDriver1, "
            + "count() as acct_count, "
            + "sum(ExpectedChurnARR) as expected_churn_arr;\n"
            + "q = order q by expected_churn_arr desc;\n"
            + "q = limit q 10;"
        ),
        # Expansion propensity distribution
        "s_expansion_dist": sq(
            L
            + UF
            + SF
            + "q = group q by ExpansionPropensityBand;\n"
            + "q = foreach q generate ExpansionPropensityBand, "
            + "count() as acct_count, "
            + "sum(ExpandPipelineARR) as expand_pipeline;\n"
            + "q = order q by (case "
            + "when ExpansionPropensityBand == \"High\" then 1 "
            + "when ExpansionPropensityBand == \"Medium\" then 2 "
            + "else 3 end) asc;"
        ),
        # At-risk accounts table (churn probability > 50%)
        "s_churn_at_risk_list": sq(
            L
            + UF
            + SF
            + "q = filter q by ChurnProbability > 50;\n"
            + "q = foreach q generate AccountName, UnitGroup, Segment, "
            + "ChurnProbability, ChurnRiskBand, ChurnDriver1, "
            + "ExpectedChurnARR, TotalWonARR, HealthBand, LifecycleStage;\n"
            + "q = order q by ExpectedChurnARR desc;\n"
            + "q = limit q 25;"
        ),
        # Churn KPIs
        "s_churn_kpi": sq(
            L
            + UF
            + SF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(ExpectedChurnARR) as total_expected_churn, "
            + "avg(ChurnProbability) as avg_churn_prob, "
            + "sum(case when ChurnRiskBand == \"High\" then 1 else 0 end) as high_risk_count, "
            + "count() as total_accounts;"
        ),
        # Churn by product
        "s_churn_by_product": sq(
            L
            + UF
            + SF
            + "q = filter q by ProductCombo != \"\";\n"
            + "q = group q by ProductCombo;\n"
            + "q = foreach q generate ProductCombo, "
            + "avg(ChurnProbability) as avg_churn_prob, "
            + "sum(ExpectedChurnARR) as expected_churn, "
            + "count() as acct_count;\n"
            + "q = order q by expected_churn desc;\n"
            + "q = limit q 10;"
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Widgets
# ═══════════════════════════════════════════════════════════════════════════


def build_widgets():
    w = {}

    # ── Nav widgets for all 7 pages ──
    for p_idx in range(NUM_PAGES):
        prefix = f"p{p_idx + 1}"
        for i in range(NUM_PAGES):
            w[f"{prefix}_nav{i + 1}"] = nav_link(
                PAGE_IDS[i], PAGE_LABELS[i], active=(i == p_idx)
            )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 1: Portfolio Overview
    # ═══════════════════════════════════════════════════════════════════

    w.update(
        {
            "p1_hdr": hdr(
                "Customer Intelligence",
                "360-degree view of current customers — health, revenue, adoption, engagement",
            ),
            "p1_f_unit": pillbox("f_unit", "Unit Group"),
            "p1_f_industry": pillbox("f_industry", "Industry"),
            "p1_f_health": pillbox("f_health", "Health Band"),
            "p1_f_segment": pillbox("f_segment", "Segment"),
            # Hero KPIs
            "p1_kpi_customers": num(
                "s_total_customers",
                "total_customers",
                "Total Customers",
                "#091A3E",
                compact=False,
                size=28,
            ),
            "p1_kpi_arr": num(
                "s_total_arr",
                "total_arr",
                "FY26 Won ARR",
                "#04844B",
                compact=True,
            ),
            "p1_kpi_health": num(
                "s_avg_health",
                "avg_health",
                "Avg Health Score",
                "#0070D2",
                compact=False,
                size=28,
            ),
            "p1_kpi_nrr": num(
                "s_avg_nrr",
                "avg_nrr",
                "Avg NRR Proxy %",
                "#FF5D2D",
                compact=False,
                size=28,
            ),
            # Health distribution donut
            "p1_sec_health": section_label("Customer Health Distribution"),
            "p1_ch_health": rich_chart(
                "s_health_dist",
                "donut",
                "Health Band Distribution",
                ["HealthBand"],
                ["acct_count"],
                show_legend=True,
                show_pct=True,
            ),
            # ARR by UnitGroup treemap
            "p1_sec_unit": section_label("ARR by Unit Group"),
            "p1_ch_unit": treemap_chart(
                "s_arr_by_unit",
                "FY26 ARR by Unit Group",
                ["UnitGroup"],
                "arr_fy26",
                show_legend=True,
            ),
            # Segment distribution donut
            "p1_sec_seg": section_label("Segment Distribution"),
            "p1_ch_seg": rich_chart(
                "s_segment_dist",
                "donut",
                "Customer Segments by ARR",
                ["Segment"],
                ["total_arr"],
                show_legend=True,
                show_pct=True,
            ),
        }
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 2: Customer Health & Risk
    # ═══════════════════════════════════════════════════════════════════

    w.update(
        {
            "p2_hdr": hdr(
                "Customer Health & Risk",
                "Composite health scoring, at-risk identification, early warning signals",
            ),
            "p2_f_unit": pillbox("f_unit", "Unit Group"),
            "p2_f_industry": pillbox("f_industry", "Industry"),
            "p2_f_segment": pillbox("f_segment", "Segment"),
            # Health gauge
            "p2_health_gauge": gauge(
                "s_health_gauge",
                "avg_score",
                "Average Health Score",
                min_val=0,
                max_val=100,
                bands=[
                    {"start": 0, "stop": 40, "color": "#D4504C"},
                    {"start": 40, "stop": 70, "color": "#FFB75D"},
                    {"start": 70, "stop": 100, "color": "#04844B"},
                ],
            ),
            # Health scatter (bubble)
            "p2_sec_scatter": section_label("ARR vs Health Score"),
            "p2_ch_scatter": bubble_chart(
                "s_health_scatter",
                "Customer ARR vs Health (size = AuM)",
            ),
            # Health heatmap by UnitGroup
            "p2_sec_heatmap": section_label("Health Distribution by Unit Group"),
            "p2_ch_heatmap": heatmap_chart(
                "s_health_by_unit",
                "Health Band x Unit Group",
            ),
            # Risk breakdown
            "p2_sec_risk": section_label("Risk Level Breakdown"),
            "p2_ch_risk": rich_chart(
                "s_risk_breakdown",
                "hbar",
                "Accounts by Risk Level",
                ["RiskLevel"],
                ["at_risk_arr"],
                axis_title="At-Risk ARR (EUR)",
            ),
            # At-risk table
            "p2_sec_table": section_label("At-Risk Accounts"),
            "p2_tbl_risk": rich_chart(
                "s_at_risk",
                "comparisontable",
                "At-Risk & Critical Accounts",
                ["AccountName"],
                [
                    "OwnerName",
                    "UnitGroup",
                    "HealthScore",
                    "TotalWonARR",
                    "ActiveContracts",
                    "ContactCount",
                ],
            ),
        }
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 3: Product Adoption
    # ═══════════════════════════════════════════════════════════════════

    w.update(
        {
            "p3_hdr": hdr(
                "Product Adoption",
                "Product penetration, SaaS & Axioma adoption, cross-sell whitespace",
            ),
            "p3_f_unit": pillbox("f_unit", "Unit Group"),
            "p3_f_industry": pillbox("f_industry", "Industry"),
            "p3_f_health": pillbox("f_health", "Health Band"),
            # KPIs
            "p3_kpi_saas": num(
                "s_saas_rate",
                "saas_pct",
                "SaaS Adoption %",
                "#04844B",
                compact=False,
                size=28,
            ),
            "p3_kpi_axioma": num(
                "s_axioma_rate",
                "axioma_pct",
                "Axioma Adoption %",
                "#0070D2",
                compact=False,
                size=28,
            ),
            # Product by UnitGroup
            "p3_sec_unit": section_label("Product Penetration by Unit Group"),
            "p3_ch_unit": rich_chart(
                "s_product_by_unit",
                "hbar",
                "Avg Products & Adoption by Unit Group",
                ["UnitGroup"],
                ["saas_count", "axioma_count"],
                show_legend=True,
                axis_title="Accounts",
            ),
            # Product distribution
            "p3_sec_dist": section_label("Product Count Distribution"),
            "p3_ch_dist": rich_chart(
                "s_product_dist",
                "column",
                "Accounts by Product Count",
                ["ProductBand"],
                ["acct_count"],
                axis_title="Number of Accounts",
            ),
            # Whitespace table
            "p3_sec_ws": section_label("Cross-Sell Whitespace (Low Product, High ARR)"),
            "p3_tbl_ws": rich_chart(
                "s_whitespace",
                "comparisontable",
                "Top Cross-Sell Opportunities",
                ["AccountName"],
                [
                    "OwnerName",
                    "UnitGroup",
                    "TotalWonARR",
                    "ProductCount",
                    "IsSaaS",
                    "IsAxioma",
                    "ExpansionScore",
                ],
            ),
        }
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 4: Revenue Expansion
    # ═══════════════════════════════════════════════════════════════════

    w.update(
        {
            "p4_hdr": hdr(
                "Revenue Expansion",
                "Expansion pipeline, NRR components, upsell/cross-sell opportunity scoring",
            ),
            "p4_f_unit": pillbox("f_unit", "Unit Group"),
            "p4_f_industry": pillbox("f_industry", "Industry"),
            "p4_f_health": pillbox("f_health", "Health Band"),
            # KPIs
            "p4_kpi_pipe": num(
                "s_expand_pipe",
                "expand_pipe",
                "Expand Pipeline",
                "#0070D2",
                compact=True,
            ),
            "p4_kpi_won": num(
                "s_expand_won",
                "expand_won",
                "Expand Won ARR",
                "#04844B",
                compact=True,
            ),
            # Expansion signal distribution
            "p4_sec_signal": section_label("Expansion Signal Distribution"),
            "p4_ch_signal": rich_chart(
                "s_expansion_dist",
                "column",
                "Accounts by Expansion Signal",
                ["ExpansionBand"],
                ["acct_count"],
                axis_title="Accounts",
            ),
            # Expand by UnitGroup
            "p4_sec_unit": section_label("Expansion Pipeline by Unit Group"),
            "p4_ch_unit": rich_chart(
                "s_expand_by_unit",
                "hbar",
                "Expand Pipeline & Won by Unit Group",
                ["UnitGroup"],
                ["expand_pipe", "expand_won"],
                show_legend=True,
                axis_title="ARR (EUR)",
            ),
            # NRR by UnitGroup
            "p4_sec_nrr": section_label("Net Revenue Retention by Unit Group"),
            "p4_ch_nrr": rich_chart(
                "s_nrr_by_unit",
                "hbar",
                "NRR Proxy % by Unit Group",
                ["UnitGroup"],
                ["nrr_pct"],
                axis_title="NRR %",
            ),
            # Top expansion accounts table
            "p4_sec_top": section_label("Top Expansion Opportunities"),
            "p4_tbl_top": rich_chart(
                "s_expansion_top",
                "comparisontable",
                "Highest Expansion Signal Accounts",
                ["AccountName"],
                [
                    "OwnerName",
                    "UnitGroup",
                    "TotalWonARR",
                    "ExpandPipelineARR",
                    "ExpansionScore",
                    "ProductCount",
                    "HealthScore",
                ],
            ),
        }
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 5: Contract & Renewal
    # ═══════════════════════════════════════════════════════════════════

    w.update(
        {
            "p5_hdr": hdr(
                "Contract & Renewal Intelligence",
                "Renewal calendar, contract terms, at-risk renewals",
            ),
            "p5_f_unit": pillbox("f_unit", "Unit Group"),
            "p5_f_industry": pillbox("f_industry", "Industry"),
            "p5_f_health": pillbox("f_health", "Health Band"),
            # KPIs
            "p5_kpi_active": num(
                "s_active_contracts",
                "active_count",
                "Active Contracts",
                "#04844B",
                compact=False,
                size=28,
            ),
            "p5_kpi_exp90": num(
                "s_expiring_90",
                "exp_90",
                "Expiring 90d",
                "#D4504C",
                compact=False,
                size=28,
            ),
            "p5_kpi_exp180": num(
                "s_expiring_180",
                "exp_180",
                "Expiring 180d",
                "#FFB75D",
                compact=False,
                size=28,
            ),
            "p5_kpi_multi": num(
                "s_multiyear",
                "multi_year",
                "Multi-Year",
                "#0070D2",
                compact=False,
                size=28,
            ),
            # Term by UnitGroup
            "p5_sec_term": section_label("Average Contract Term by Unit Group"),
            "p5_ch_term": rich_chart(
                "s_term_by_unit",
                "hbar",
                "Avg Term (Months) by Unit Group",
                ["UnitGroup"],
                ["avg_term"],
                axis_title="Months",
            ),
            # Contracts by Segment
            "p5_sec_seg": section_label("Contract Distribution by Segment"),
            "p5_ch_seg": rich_chart(
                "s_contracts_by_seg",
                "column",
                "Contracts by Customer Segment",
                ["Segment"],
                ["total_contracts", "active_contracts", "exp_90"],
                show_legend=True,
                axis_title="Contracts",
            ),
            # At-risk renewals table
            "p5_sec_risk": section_label("At-Risk Renewals (Expiring 90d)"),
            "p5_tbl_risk": rich_chart(
                "s_renewal_risk",
                "comparisontable",
                "Renewals at Risk",
                ["AccountName"],
                [
                    "OwnerName",
                    "UnitGroup",
                    "TotalWonARR",
                    "ExpiringContracts90d",
                    "HealthScore",
                    "ContactCount",
                    "LastActivityDate",
                ],
            ),
        }
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 6: Customer Segmentation
    # ═══════════════════════════════════════════════════════════════════

    w.update(
        {
            "p6_hdr": hdr(
                "Customer Segmentation",
                "Segment performance, cohort retention, revenue concentration, geography",
            ),
            "p6_f_unit": pillbox("f_unit", "Unit Group"),
            "p6_f_industry": pillbox("f_industry", "Industry"),
            "p6_f_health": pillbox("f_health", "Health Band"),
            # Segment performance
            "p6_sec_seg": section_label("Segment Performance"),
            "p6_ch_seg": rich_chart(
                "s_seg_performance",
                "hbar",
                "ARR & Health by Segment",
                ["Segment"],
                ["total_arr"],
                axis_title="Total Won ARR (EUR)",
            ),
            # Revenue concentration
            "p6_sec_conc": section_label("Revenue Concentration (Top 20)"),
            "p6_ch_conc": rich_chart(
                "s_revenue_concentration",
                "hbar",
                "Top 20 Accounts by ARR",
                ["AccountName"],
                ["TotalWonARR"],
                axis_title="Total Won ARR (EUR)",
            ),
            # Cohort analysis
            "p6_sec_cohort": section_label("Customer Cohort Analysis"),
            "p6_ch_cohort": rich_chart(
                "s_cohort",
                "column",
                "Customers & ARR by Cohort Year",
                ["CustomerSince"],
                ["cohort_arr"],
                axis_title="Cohort ARR (EUR)",
            ),
            # Industry treemap
            "p6_sec_ind": section_label("Industry Distribution"),
            "p6_ch_ind": treemap_chart(
                "s_industry_treemap",
                "ARR by Industry",
                ["Industry"],
                "total_arr",
                show_legend=True,
            ),
            # Country distribution
            "p6_sec_geo": section_label("Geographic Distribution"),
            "p6_ch_geo": rich_chart(
                "s_country_dist",
                "hbar",
                "ARR by Country (Top 15)",
                ["BillingCountry"],
                ["total_arr"],
                axis_title="Total ARR (EUR)",
            ),
        }
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 7: Engagement & Contacts
    # ═══════════════════════════════════════════════════════════════════

    w.update(
        {
            "p7_hdr": hdr(
                "Engagement & Contacts",
                "Contact coverage, C-level penetration, multi-threading, activity recency",
            ),
            "p7_f_unit": pillbox("f_unit", "Unit Group"),
            "p7_f_industry": pillbox("f_industry", "Industry"),
            "p7_f_health": pillbox("f_health", "Health Band"),
            # KPIs
            "p7_kpi_contacts": num(
                "s_avg_contacts",
                "avg_contacts",
                "Avg Contacts/Acct",
                "#091A3E",
                compact=False,
                size=28,
            ),
            "p7_kpi_clevel": num(
                "s_avg_clevel",
                "avg_clevel",
                "Avg C-Level/Acct",
                "#0070D2",
                compact=False,
                size=28,
            ),
            # Contact by segment
            "p7_sec_seg": section_label("Contact Coverage by Segment"),
            "p7_ch_seg": rich_chart(
                "s_contact_by_seg",
                "hbar",
                "Avg Contacts & C-Level by Segment",
                ["Segment"],
                ["avg_contacts", "avg_clevel"],
                show_legend=True,
                axis_title="Avg Count",
            ),
            # Multi-threading
            "p7_sec_thread": section_label("Multi-Threading Analysis"),
            "p7_ch_thread": rich_chart(
                "s_threading",
                "column",
                "Account Threading Distribution",
                ["ThreadBand"],
                ["acct_count"],
                axis_title="Accounts",
            ),
            # C-Level by UnitGroup
            "p7_sec_clevel": section_label("C-Level Penetration by Unit Group"),
            "p7_ch_clevel": rich_chart(
                "s_clevel_by_unit",
                "hbar",
                "Avg C-Level Contacts by Unit Group",
                ["UnitGroup"],
                ["avg_clevel"],
                axis_title="Avg C-Level Contacts",
            ),
            # Low engagement table
            "p7_sec_low": section_label("Low-Engagement High-ARR Accounts"),
            "p7_tbl_low": rich_chart(
                "s_low_engagement",
                "comparisontable",
                "Under-Engaged Accounts",
                ["AccountName"],
                [
                    "OwnerName",
                    "UnitGroup",
                    "TotalWonARR",
                    "ContactCount",
                    "CLevelContacts",
                    "HealthScore",
                ],
            ),
        }
    )

    # ═══════════════════════════════════════════════════════════════════
    #  ENHANCEMENT WIDGETS (added to existing pages)
    # ═══════════════════════════════════════════════════════════════════

    w.update(
        {
            # ── Portfolio page enhancements ──
            "p1_sec_waterfall": section_label(
                "ARR Movement (Land / Expand / Renewal / Churn)"
            ),
            "p1_ch_waterfall": waterfall_chart(
                "s_arr_waterfall",
                "ARR Waterfall",
                "category",
                "value",
                axis_label="ARR (EUR)",
            ),
            "p1_kpi_grr": num(
                "s_grr",
                "grr_pct",
                "GRR Proxy %",
                "#04844B",
                compact=False,
                size=28,
            ),
            "p1_kpi_logo": num(
                "s_logo_retention",
                "logo_retention",
                "Logo Retention %",
                "#0070D2",
                compact=False,
                size=28,
            ),
            # ── Expansion page enhancements ──
            "p4_kpi_l2e": num(
                "s_l2e_avg",
                "avg_days",
                "Avg Land-to-Expand Days",
                "#FF5D2D",
                compact=False,
                size=28,
            ),
            "p4_sec_l2e": section_label("Land-to-Expand Velocity by Segment"),
            "p4_ch_l2e": rich_chart(
                "s_l2e_by_segment",
                "hbar",
                "Avg Days from Land to First Expand",
                ["Segment"],
                ["avg_days"],
                axis_title="Days",
            ),
            # ── Segmentation page enhancements ──
            "p6_sec_lifecycle": section_label("Customer Lifecycle Distribution"),
            "p6_ch_lifecycle": rich_chart(
                "s_lifecycle",
                "donut",
                "Lifecycle Stage Distribution",
                ["LifecycleStage"],
                ["acct_count"],
                show_legend=True,
                show_pct=True,
            ),
            "p6_sec_lc_unit": section_label("Lifecycle by Unit Group"),
            "p6_ch_lc_unit": heatmap_chart(
                "s_lifecycle_by_unit",
                "Lifecycle Stage x Unit Group",
            ),
            # ── Engagement page enhancements ──
            "p7_sec_tier": section_label("Contact Coverage Tier Distribution"),
            "p7_ch_tier": rich_chart(
                "s_contact_tier",
                "donut",
                "Accounts by Contact Tier",
                ["ContactTier"],
                ["acct_count"],
                show_legend=True,
                show_pct=True,
            ),
            "p7_sec_tier_heat": section_label("Contact Tier by Unit Group"),
            "p7_ch_tier_heat": heatmap_chart(
                "s_tier_by_unit",
                "Contact Tier x Unit Group",
            ),
            # ── Product page enhancements ──
            "p3_sec_combos": section_label("Top Product Combinations"),
            "p3_ch_combos": rich_chart(
                "s_product_combos",
                "hbar",
                "Most Common Product Combinations",
                ["ProductCombo"],
                ["combo_count"],
                axis_title="Accounts",
            ),
        }
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 8: Advanced Analytics
    # ═══════════════════════════════════════════════════════════════════
    w.update(
        {
            "p8_hdr": hdr(
                DS_LABEL, "Advanced Analytics — Sankey, Area, Bullet & Stats"
            ),
            "p8_f_unit": pillbox("f_unit", "Unit Group"),
            "p8_f_industry": pillbox("f_industry", "Industry"),
            "p8_f_health": pillbox("f_health", "Health Band"),
            "p8_f_segment": pillbox("f_segment", "Segment"),
            # Sankey: HealthBand → Segment flow
            "p8_sec_sankey": section_label("Customer Flow: Health Band → Segment"),
            "p8_ch_sankey": sankey_chart(
                "s_sankey_health_seg",
                "Health Band to Segment Flow",
            ),
            # Area: Cumulative ARR
            "p8_sec_area": section_label("Cumulative ARR by Customer Vintage"),
            "p8_ch_area": area_chart(
                "s_area_arr_cumul",
                "Cumulative ARR Over Time",
                axis_title="ARR (EUR)",
            ),
            # Bullet: Health Score vs target
            "p8_sec_bullet": section_label("Score vs Target"),
            "p8_ch_bullet_health": bullet_chart(
                "s_bullet_health",
                "Avg Health Score vs Target (70)",
                axis_title="Health Score",
            ),
            "p8_ch_bullet_expand": bullet_chart(
                "s_bullet_expansion",
                "Avg Expansion Score vs Target (60)",
                axis_title="Expansion Score",
            ),
            # Stats: ARR percentile table
            "p8_sec_arr_stats": section_label("ARR Distribution Statistics"),
            "p8_tbl_arr_stats": rich_chart(
                "s_stat_arr_percentiles",
                "comparisontable",
                "ARR Percentile Distribution",
                ["total_accts"],
                ["avg_arr", "stddev_arr", "p25_arr", "p50_arr", "p75_arr"],
            ),
            # Stats: Health Score by UnitGroup table
            "p8_sec_health_stats": section_label(
                "Health Score Statistics by Unit Group"
            ),
            "p8_tbl_health_stats": rich_chart(
                "s_stat_health_by_unit",
                "comparisontable",
                "Health Score Distribution by Unit Group",
                ["UnitGroup"],
                [
                    "acct_count",
                    "avg_health",
                    "stddev_health",
                    "p25_health",
                    "p50_health",
                    "p75_health",
                ],
            ),
        }
    )

    # ── Phase 7: Embedded table actions ──────────────────────────────────
    add_table_action(w["p2_tbl_risk"], "salesforceActions", "Account", "Id")
    add_table_action(w["p3_tbl_ws"], "salesforceActions", "Account", "Id")
    add_table_action(w["p4_tbl_top"], "salesforceActions", "Account", "Id")
    add_table_action(w["p5_tbl_risk"], "salesforceActions", "Account", "Id")
    add_table_action(w["p7_tbl_low"], "salesforceActions", "Account", "Id")

    # ═══ VIZ UPGRADE: Health Transition Sankey ═══
    w["p8_sec_health_flow"] = section_label("Health Band Transitions")
    w["p8_ch_health_flow"] = sankey_chart(
        "s_health_transition", "Account Health Movement (Prior → Current)"
    )

    # ═══ VIZ UPGRADE: Health Score Driver Waterfall ═══
    w["p8_sec_drivers"] = section_label("Health Score Component Breakdown")
    w["p8_ch_drivers"] = rich_chart(
        "s_health_drivers",
        "comparisontable",
        "Avg Score by Component (out of 15-20 each)",
        [],
        [
            "contract_component",
            "revenue_component",
            "engagement_component",
            "contact_component",
            "adoption_component",
            "risk_component",
        ],
    )

    # ═══ VIZ UPGRADE: Renewal Timeline ═══
    w["p5_sec_timeline"] = section_label("Renewal Timeline: Expiring ARR by Month")
    w["p5_ch_timeline"] = combo_chart(
        "s_renewal_timeline",
        "Expiring ARR & Account Count by Month",
        ["ContractEndMonth"],
        bar_measures=["expiring_arr"],
        line_measures=["avg_health"],
        show_legend=True,
        axis_title="ARR (EUR)",
        axis2_title="Avg Health Score",
    )

    # ═══ VIZ UPGRADE: Revenue Concentration ═══
    w["p1_sec_concentration"] = section_label("Revenue Concentration (Top 50)")
    w["p1_ch_concentration"] = rich_chart(
        "s_revenue_concentration",
        "hbar",
        "ARR by Account (Top 50 Customers)",
        ["AccountName"],
        ["acct_arr"],
        axis_title="ARR (EUR)",
    )

    # ═══ VIZ UPGRADE: Adoption × Segment Heatmap ═══
    w["p8_sec_adopt_seg"] = section_label("Product Adoption × Segment")
    w["p8_ch_adopt_seg"] = heatmap_chart(
        "s_adoption_segment", "Accounts by Product Count × Segment"
    )

    # ═══ VIZ UPGRADE: Dynamic KPI Tiles ═══
    w["p1_health_dynamic"] = num_dynamic_color(
        "s_ci_kpi_thresh",
        "avg_health",
        "Avg Health Score",
        thresholds=[(40, "#D4504C"), (70, "#FFB75D"), (100, "#04844B")],
        size=28,
    )
    w["p1_atrisk_dynamic"] = num_dynamic_color(
        "s_ci_kpi_thresh",
        "at_risk_pct",
        "At-Risk Customers %",
        thresholds=[(10, "#04844B"), (25, "#FFB75D"), (100, "#D4504C")],
        size=28,
    )

    # ═══ PAGE 9: Retention & Growth (ML-Forward) ═══
    w["p9_hdr"] = hdr(
        "Retention & Growth Intelligence",
        "Predictive churn/expansion scoring, cohort retention, expected churn impact",
    )
    w["p9_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p9_f_segment"] = pillbox("f_segment", "Segment")
    # Churn KPI tiles
    w["p9_churn_total"] = num(
        "s_churn_kpi", "total_expected_churn", "Expected Churn ARR", "#D4504C",
        compact=True, size=28,
    )
    w["p9_churn_prob"] = num_dynamic_color(
        "s_churn_kpi", "avg_churn_prob", "Avg Churn Probability %",
        thresholds=[(20, "#04844B"), (40, "#FFB75D"), (100, "#D4504C")],
        size=28,
    )
    w["p9_high_risk"] = num(
        "s_churn_kpi", "high_risk_count", "High-Risk Accounts", "#D4504C",
        size=28,
    )
    # Cohort retention bar
    w["p9_sec_cohort"] = section_label("Cohort Retention Analysis")
    w["p9_ch_cohort"] = rich_chart(
        "s_cohort_nrr", "column",
        "NRR & GRR by Cohort Quarter",
        ["CohortQuarter"], ["avg_nrr", "avg_grr"],
        show_legend=True, axis_title="Rate %",
    )
    # Churn distribution
    w["p9_sec_churn_dist"] = section_label("Churn Risk Distribution")
    w["p9_ch_churn_dist"] = rich_chart(
        "s_churn_dist", "donut",
        "Accounts by Churn Risk Band",
        ["ChurnRiskBand"], ["acct_count"],
        show_legend=True,
    )
    # Expected churn by segment
    w["p9_ch_churn_seg"] = rich_chart(
        "s_churn_by_segment", "hbar",
        "Expected Churn ARR by Segment",
        ["Segment"], ["expected_churn"],
        axis_title="Expected Churn ARR (EUR)",
    )
    # Churn drivers
    w["p9_sec_drivers"] = section_label("Top Churn Drivers")
    w["p9_ch_drivers"] = rich_chart(
        "s_churn_drivers", "hbar",
        "Top Churn Risk Factors",
        ["ChurnDriver1"], ["expected_churn_arr"],
        axis_title="Expected Churn ARR (EUR)",
    )
    # Expansion propensity
    w["p9_sec_expansion"] = section_label("Expansion Propensity Distribution")
    w["p9_ch_expansion"] = rich_chart(
        "s_expansion_dist", "donut",
        "Accounts by Expansion Propensity",
        ["ExpansionPropensityBand"], ["acct_count"],
        show_legend=True,
    )
    # At-risk accounts table
    w["p9_sec_atrisk"] = section_label("High-Risk Churn Accounts (Prob > 50%)")
    w["p9_tbl_atrisk"] = rich_chart(
        "s_churn_at_risk_list", "comparisontable",
        "At-Risk Accounts — Recommended Actions",
        ["AccountName", "UnitGroup", "Segment", "ChurnRiskBand", "ChurnDriver1"],
        ["ChurnProbability", "ExpectedChurnARR", "TotalWonARR"],
    )
    add_table_action(w["p9_tbl_atrisk"], object_name="Account", id_field="AccountId")
    # Churn by product
    w["p9_sec_product"] = section_label("Churn Risk by Product Combination")
    w["p9_ch_product"] = heatmap_chart(
        "s_churn_by_product", "Avg Churn Probability by Product Combo"
    )

    return w


# ═══════════════════════════════════════════════════════════════════════════
#  Layout
# ═══════════════════════════════════════════════════════════════════════════


def _std_header(prefix, row_start=1):
    """Standard header + 4-filter bar layout entries."""
    return [
        {
            "name": f"{prefix}_hdr",
            "row": row_start,
            "column": 0,
            "colspan": 12,
            "rowspan": 2,
        },
        {
            "name": f"{prefix}_f_unit",
            "row": row_start + 2,
            "column": 0,
            "colspan": 3,
            "rowspan": 2,
        },
        {
            "name": f"{prefix}_f_industry",
            "row": row_start + 2,
            "column": 3,
            "colspan": 3,
            "rowspan": 2,
        },
        {
            "name": f"{prefix}_f_health",
            "row": row_start + 2,
            "column": 6,
            "colspan": 3,
            "rowspan": 2,
        },
        {
            "name": f"{prefix}_f_segment",
            "row": row_start + 2,
            "column": 9,
            "colspan": 3,
            "rowspan": 2,
        },
    ]


def _std_header_3f(prefix, f3_name, f3_label, row_start=1):
    """Header + 3-filter bar (no segment filter on some pages)."""
    return [
        {
            "name": f"{prefix}_hdr",
            "row": row_start,
            "column": 0,
            "colspan": 12,
            "rowspan": 2,
        },
        {
            "name": f"{prefix}_f_unit",
            "row": row_start + 2,
            "column": 0,
            "colspan": 4,
            "rowspan": 2,
        },
        {
            "name": f"{prefix}_f_industry",
            "row": row_start + 2,
            "column": 4,
            "colspan": 4,
            "rowspan": 2,
        },
        {
            "name": f"{prefix}_f_{f3_name}",
            "row": row_start + 2,
            "column": 8,
            "colspan": 4,
            "rowspan": 2,
        },
    ]


def build_layout():
    # ── PAGE 1: Portfolio Overview ──
    p1 = (
        nav_row("p1", NUM_PAGES)
        + _std_header("p1")
        + [
            # Hero KPIs (row 5)
            {
                "name": "p1_kpi_customers",
                "row": 5,
                "column": 0,
                "colspan": 3,
                "rowspan": 4,
            },
            {"name": "p1_kpi_arr", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
            {
                "name": "p1_kpi_health",
                "row": 5,
                "column": 6,
                "colspan": 3,
                "rowspan": 4,
            },
            {"name": "p1_kpi_nrr", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
            # Health distribution + Segment distribution (row 9)
            {
                "name": "p1_sec_health",
                "row": 9,
                "column": 0,
                "colspan": 6,
                "rowspan": 1,
            },
            {
                "name": "p1_ch_health",
                "row": 10,
                "column": 0,
                "colspan": 6,
                "rowspan": 7,
            },
            {"name": "p1_sec_seg", "row": 9, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p1_ch_seg", "row": 10, "column": 6, "colspan": 6, "rowspan": 7},
            # ARR by Unit Group treemap (row 17)
            {
                "name": "p1_sec_unit",
                "row": 17,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {"name": "p1_ch_unit", "row": 18, "column": 0, "colspan": 12, "rowspan": 8},
            # ARR Waterfall (row 26)
            {
                "name": "p1_sec_waterfall",
                "row": 26,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p1_ch_waterfall",
                "row": 27,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
            # GRR + Logo Retention KPIs (row 35)
            {"name": "p1_kpi_grr", "row": 35, "column": 0, "colspan": 6, "rowspan": 4},
            {"name": "p1_kpi_logo", "row": 35, "column": 6, "colspan": 6, "rowspan": 4},
            # VIZ UPGRADE: Revenue Concentration + Dynamic KPIs
            {"name": "p1_sec_concentration", "row": 39, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p1_ch_concentration", "row": 40, "column": 0, "colspan": 12, "rowspan": 8},
            {"name": "p1_health_dynamic", "row": 48, "column": 0, "colspan": 6, "rowspan": 4},
            {"name": "p1_atrisk_dynamic", "row": 48, "column": 6, "colspan": 6, "rowspan": 4},
        ]
    )

    # ── PAGE 2: Customer Health & Risk ──
    p2 = (
        nav_row("p2", NUM_PAGES)
        + _std_header_3f("p2", "segment", "Segment")
        + [
            # Health gauge (row 5)
            {
                "name": "p2_health_gauge",
                "row": 5,
                "column": 0,
                "colspan": 12,
                "rowspan": 6,
            },
            # Scatter + Heatmap (row 11)
            {
                "name": "p2_sec_scatter",
                "row": 11,
                "column": 0,
                "colspan": 6,
                "rowspan": 1,
            },
            {
                "name": "p2_ch_scatter",
                "row": 12,
                "column": 0,
                "colspan": 6,
                "rowspan": 8,
            },
            {
                "name": "p2_sec_heatmap",
                "row": 11,
                "column": 6,
                "colspan": 6,
                "rowspan": 1,
            },
            {
                "name": "p2_ch_heatmap",
                "row": 12,
                "column": 6,
                "colspan": 6,
                "rowspan": 8,
            },
            # Risk breakdown (row 20)
            {
                "name": "p2_sec_risk",
                "row": 20,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {"name": "p2_ch_risk", "row": 21, "column": 0, "colspan": 12, "rowspan": 6},
            # At-risk table (row 27)
            {
                "name": "p2_sec_table",
                "row": 27,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p2_tbl_risk",
                "row": 28,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
        ]
    )

    # ── PAGE 3: Product Adoption ──
    p3 = (
        nav_row("p3", NUM_PAGES)
        + _std_header_3f("p3", "health", "Health")
        + [
            # KPIs (row 5)
            {"name": "p3_kpi_saas", "row": 5, "column": 0, "colspan": 6, "rowspan": 4},
            {
                "name": "p3_kpi_axioma",
                "row": 5,
                "column": 6,
                "colspan": 6,
                "rowspan": 4,
            },
            # Product by UnitGroup (row 9)
            {"name": "p3_sec_unit", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p3_ch_unit", "row": 10, "column": 0, "colspan": 6, "rowspan": 8},
            # Product distribution (row 9 right)
            {"name": "p3_sec_dist", "row": 9, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p3_ch_dist", "row": 10, "column": 6, "colspan": 6, "rowspan": 8},
            # Whitespace table (row 18)
            {"name": "p3_sec_ws", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p3_tbl_ws", "row": 19, "column": 0, "colspan": 12, "rowspan": 8},
            # Product Combinations (row 27)
            {
                "name": "p3_sec_combos",
                "row": 27,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p3_ch_combos",
                "row": 28,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
        ]
    )

    # ── PAGE 4: Revenue Expansion ──
    p4 = (
        nav_row("p4", NUM_PAGES)
        + _std_header_3f("p4", "health", "Health")
        + [
            # KPIs (row 5)
            {"name": "p4_kpi_pipe", "row": 5, "column": 0, "colspan": 6, "rowspan": 4},
            {"name": "p4_kpi_won", "row": 5, "column": 6, "colspan": 6, "rowspan": 4},
            # Expansion signal + Expand by unit (row 9)
            {
                "name": "p4_sec_signal",
                "row": 9,
                "column": 0,
                "colspan": 6,
                "rowspan": 1,
            },
            {
                "name": "p4_ch_signal",
                "row": 10,
                "column": 0,
                "colspan": 6,
                "rowspan": 8,
            },
            {"name": "p4_sec_unit", "row": 9, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p4_ch_unit", "row": 10, "column": 6, "colspan": 6, "rowspan": 8},
            # NRR by UnitGroup (row 18)
            {"name": "p4_sec_nrr", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p4_ch_nrr", "row": 19, "column": 0, "colspan": 12, "rowspan": 7},
            # Expansion opportunities table (row 26)
            {"name": "p4_sec_top", "row": 26, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p4_tbl_top", "row": 27, "column": 0, "colspan": 12, "rowspan": 8},
            # Land-to-Expand KPI + velocity chart (row 35)
            {"name": "p4_kpi_l2e", "row": 35, "column": 0, "colspan": 12, "rowspan": 4},
            {"name": "p4_sec_l2e", "row": 39, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p4_ch_l2e", "row": 40, "column": 0, "colspan": 12, "rowspan": 8},
        ]
    )

    # ── PAGE 5: Contract & Renewal ──
    p5 = (
        nav_row("p5", NUM_PAGES)
        + _std_header_3f("p5", "health", "Health")
        + [
            # KPIs (row 5)
            {
                "name": "p5_kpi_active",
                "row": 5,
                "column": 0,
                "colspan": 3,
                "rowspan": 4,
            },
            {"name": "p5_kpi_exp90", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
            {
                "name": "p5_kpi_exp180",
                "row": 5,
                "column": 6,
                "colspan": 3,
                "rowspan": 4,
            },
            {"name": "p5_kpi_multi", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
            # Term by UnitGroup + Contracts by Segment (row 9)
            {"name": "p5_sec_term", "row": 9, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p5_ch_term", "row": 10, "column": 0, "colspan": 6, "rowspan": 8},
            {"name": "p5_sec_seg", "row": 9, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p5_ch_seg", "row": 10, "column": 6, "colspan": 6, "rowspan": 8},
            # At-risk renewals table (row 18)
            {
                "name": "p5_sec_risk",
                "row": 18,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p5_tbl_risk",
                "row": 19,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
            # VIZ UPGRADE: Renewal Timeline
            {"name": "p5_sec_timeline", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p5_ch_timeline", "row": 28, "column": 0, "colspan": 12, "rowspan": 8},
        ]
    )

    # ── PAGE 6: Customer Segmentation ──
    p6 = (
        nav_row("p6", NUM_PAGES)
        + _std_header_3f("p6", "health", "Health")
        + [
            # Segment performance + Revenue concentration (row 5)
            {"name": "p6_sec_seg", "row": 5, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p6_ch_seg", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
            {"name": "p6_sec_conc", "row": 5, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p6_ch_conc", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
            # Cohort analysis (row 14)
            {
                "name": "p6_sec_cohort",
                "row": 14,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p6_ch_cohort",
                "row": 15,
                "column": 0,
                "colspan": 12,
                "rowspan": 7,
            },
            # Industry treemap + Country distribution (row 22)
            {"name": "p6_sec_ind", "row": 22, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p6_ch_ind", "row": 23, "column": 0, "colspan": 6, "rowspan": 8},
            {"name": "p6_sec_geo", "row": 22, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p6_ch_geo", "row": 23, "column": 6, "colspan": 6, "rowspan": 8},
            # Customer Lifecycle (row 31)
            {
                "name": "p6_sec_lifecycle",
                "row": 31,
                "column": 0,
                "colspan": 6,
                "rowspan": 1,
            },
            {
                "name": "p6_ch_lifecycle",
                "row": 32,
                "column": 0,
                "colspan": 6,
                "rowspan": 8,
            },
            # Lifecycle by Unit Group heatmap (row 31 right)
            {
                "name": "p6_sec_lc_unit",
                "row": 31,
                "column": 6,
                "colspan": 6,
                "rowspan": 1,
            },
            {
                "name": "p6_ch_lc_unit",
                "row": 32,
                "column": 6,
                "colspan": 6,
                "rowspan": 8,
            },
        ]
    )

    # ── PAGE 7: Engagement & Contacts ──
    p7 = (
        nav_row("p7", NUM_PAGES)
        + _std_header_3f("p7", "health", "Health")
        + [
            # KPIs (row 5)
            {
                "name": "p7_kpi_contacts",
                "row": 5,
                "column": 0,
                "colspan": 6,
                "rowspan": 4,
            },
            {
                "name": "p7_kpi_clevel",
                "row": 5,
                "column": 6,
                "colspan": 6,
                "rowspan": 4,
            },
            # Contact by segment + Multi-threading (row 9)
            {"name": "p7_sec_seg", "row": 9, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p7_ch_seg", "row": 10, "column": 0, "colspan": 6, "rowspan": 8},
            {
                "name": "p7_sec_thread",
                "row": 9,
                "column": 6,
                "colspan": 6,
                "rowspan": 1,
            },
            {
                "name": "p7_ch_thread",
                "row": 10,
                "column": 6,
                "colspan": 6,
                "rowspan": 8,
            },
            # C-Level by UnitGroup (row 18)
            {
                "name": "p7_sec_clevel",
                "row": 18,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p7_ch_clevel",
                "row": 19,
                "column": 0,
                "colspan": 12,
                "rowspan": 7,
            },
            # Low engagement table (row 26)
            {"name": "p7_sec_low", "row": 26, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p7_tbl_low", "row": 27, "column": 0, "colspan": 12, "rowspan": 8},
            # Contact Coverage Tier (row 35)
            {"name": "p7_sec_tier", "row": 35, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p7_ch_tier", "row": 36, "column": 0, "colspan": 6, "rowspan": 8},
            # Contact Tier by Unit heatmap (row 35 right)
            {
                "name": "p7_sec_tier_heat",
                "row": 35,
                "column": 6,
                "colspan": 6,
                "rowspan": 1,
            },
            {
                "name": "p7_ch_tier_heat",
                "row": 36,
                "column": 6,
                "colspan": 6,
                "rowspan": 8,
            },
        ]
    )

    # ── PAGE 8: Advanced Analytics ──
    p8 = (
        nav_row("p8", NUM_PAGES)
        + _std_header("p8")
        + [
            # Sankey: HealthBand → Segment (row 5)
            {
                "name": "p8_sec_sankey",
                "row": 5,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p8_ch_sankey",
                "row": 6,
                "column": 0,
                "colspan": 12,
                "rowspan": 10,
            },
            # Area: Cumulative ARR (row 16)
            {
                "name": "p8_sec_area",
                "row": 16,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {"name": "p8_ch_area", "row": 17, "column": 0, "colspan": 12, "rowspan": 8},
            # Bullet charts (row 25)
            {
                "name": "p8_sec_bullet",
                "row": 25,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p8_ch_bullet_health",
                "row": 26,
                "column": 0,
                "colspan": 6,
                "rowspan": 5,
            },
            {
                "name": "p8_ch_bullet_expand",
                "row": 26,
                "column": 6,
                "colspan": 6,
                "rowspan": 5,
            },
            # ARR Stats table (row 31)
            {
                "name": "p8_sec_arr_stats",
                "row": 31,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p8_tbl_arr_stats",
                "row": 32,
                "column": 0,
                "colspan": 12,
                "rowspan": 6,
            },
            # Health Score Stats table (row 38)
            {
                "name": "p8_sec_health_stats",
                "row": 38,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p8_tbl_health_stats",
                "row": 39,
                "column": 0,
                "colspan": 12,
                "rowspan": 8,
            },
            # VIZ UPGRADE: Health Transition Sankey
            {"name": "p8_sec_health_flow", "row": 47, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p8_ch_health_flow", "row": 48, "column": 0, "colspan": 12, "rowspan": 10},
            # VIZ UPGRADE: Health Score Driver Breakdown
            {"name": "p8_sec_drivers", "row": 58, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p8_ch_drivers", "row": 59, "column": 0, "colspan": 12, "rowspan": 6},
            # VIZ UPGRADE: Adoption × Segment Heatmap
            {"name": "p8_sec_adopt_seg", "row": 65, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p8_ch_adopt_seg", "row": 66, "column": 0, "colspan": 12, "rowspan": 10},
        ]
    )

    # ── PAGE 9: Retention & Growth (ML-Forward) ──
    p9 = (
        nav_row("p9", NUM_PAGES)
        + _std_header("p9")
        + [
            # Filter bar
            {"name": "p9_f_unit", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
            {"name": "p9_f_segment", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
            # Churn KPI tiles (row 5)
            {"name": "p9_churn_total", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
            {"name": "p9_churn_prob", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
            {"name": "p9_high_risk", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
            # Cohort retention (row 9)
            {"name": "p9_sec_cohort", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p9_ch_cohort", "row": 10, "column": 0, "colspan": 12, "rowspan": 8},
            # Churn distribution + by segment (row 18)
            {"name": "p9_sec_churn_dist", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p9_ch_churn_dist", "row": 19, "column": 0, "colspan": 6, "rowspan": 8},
            {"name": "p9_ch_churn_seg", "row": 19, "column": 6, "colspan": 6, "rowspan": 8},
            # Churn drivers + expansion propensity (row 27)
            {"name": "p9_sec_drivers", "row": 27, "column": 0, "colspan": 6, "rowspan": 1},
            {"name": "p9_ch_drivers", "row": 28, "column": 0, "colspan": 6, "rowspan": 8},
            {"name": "p9_sec_expansion", "row": 27, "column": 6, "colspan": 6, "rowspan": 1},
            {"name": "p9_ch_expansion", "row": 28, "column": 6, "colspan": 6, "rowspan": 8},
            # At-risk accounts table (row 36)
            {"name": "p9_sec_atrisk", "row": 36, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p9_tbl_atrisk", "row": 37, "column": 0, "colspan": 12, "rowspan": 10},
            # Churn by product (row 47)
            {"name": "p9_sec_product", "row": 47, "column": 0, "colspan": 12, "rowspan": 1},
            {"name": "p9_ch_product", "row": 48, "column": 0, "colspan": 12, "rowspan": 8},
        ]
    )

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg(PAGE_IDS[0], PAGE_LABELS[0], p1),
            pg(PAGE_IDS[1], PAGE_LABELS[1], p2),
            pg(PAGE_IDS[2], PAGE_LABELS[2], p3),
            pg(PAGE_IDS[3], PAGE_LABELS[3], p4),
            pg(PAGE_IDS[4], PAGE_LABELS[4], p5),
            pg(PAGE_IDS[5], PAGE_LABELS[5], p6),
            pg(PAGE_IDS[6], PAGE_LABELS[6], p7),
            pg(PAGE_IDS[7], PAGE_LABELS[7], p8),
            pg(PAGE_IDS[8], PAGE_LABELS[8], p9),
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main():
    inst, tok = get_auth()

    # Build dataset
    ds_ok = create_dataset(inst, tok)
    if not ds_ok:
        print("ERROR: Customer Intelligence dataset failed — aborting")
        return

    # Set record navigation links via XMD
    set_record_links_xmd(
        inst,
        tok,
        DS,
        [
            {"field": "AccountName", "sobject": "Account", "id_field": "Id"},
        ],
    )

    # Get dataset ID for af() steps
    ds_id = get_dataset_id(inst, tok, DS)
    if not ds_id:
        print("ERROR: Could not find dataset ID — aborting")
        return

    # Deploy dashboard
    dash_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
    steps = build_steps(ds_id)
    widgets = build_widgets()
    layout = build_layout()
    state = build_dashboard_state(steps, widgets, layout)
    deploy_dashboard(inst, tok, dash_id, state)


if __name__ == "__main__":
    main()
