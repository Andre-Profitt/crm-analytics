#!/usr/bin/env python3
"""Build the Account Intelligence KPI dashboard (AP 1.2, 1.3, 1.7).

Pages:
  1. Data Quality    — DUNS/UnitGroup/Axioma fill rates, quality bands, trend
  2. KYC Pipeline    — KYC status funnel, not-started count, by UnitGroup
  3. Customer Health — Risk distribution, churn gauge, termination detail
  4. Cross-sell & Contact — Axioma penetration, SaaS adoption, contact coverage
  5. Segment Analysis — Industry, size band, geography, AuM analytics

Datasets:
  - Account_Intelligence  (Account SOQL + computed fields)
  - Contact_Coverage      (Contact SOQL + computed fields)
"""

import csv
import io
import sys

from crm_analytics_helpers import (
    get_auth,
    _soql,
    _dim,
    _measure,
    _date,
    upload_dataset,
    get_dataset_id,
    create_dashboard_if_needed,
    sq,
    af,
    num,
    num_with_trend,
    trend_step,
    rich_chart,
    gauge,
    funnel_chart,
    waterfall_chart,
    hdr,
    section_label,
    nav_link,
    pg,
    nav_row,
    build_dashboard_state,
    deploy_dashboard,
    coalesce_filter,
    pillbox,
    treemap_chart,
    heatmap_chart,
    bubble_chart,
    bullet_chart,
    sankey_chart,
    area_chart,
    create_dataflow,
    run_dataflow,
    set_record_links_xmd,  # noqa: F401
)

DS = "Account_Intelligence"
DS_LABEL = "Account Intelligence"
CONTACT_DS = "Contact_Coverage"
CONTACT_DS_LABEL = "Contact Coverage"
DASHBOARD_LABEL = "Account Intelligence KPIs"


# ═══════════════════════════════════════════════════════════════════════════
#  Dataset builders
# ═══════════════════════════════════════════════════════════════════════════


def create_account_dataset(inst, tok):
    """Build Account_Intelligence from Account SOQL with computed fields."""
    print("\n=== Building Account Intelligence dataset ===")

    accounts = _soql(
        inst,
        tok,
        "SELECT Id, Name, Owner.Name, Type, CreatedDate, BillingCountry, Industry, "
        "Unit__c, Unit_Group__c, SaaS_Client__c, Axioma_Client__c, "
        "Risk_of_Potential_Termination__c, KYC_Approval_Status__c, DUNS_No__c, "
        "Partner_Engagement_Level__c, APTS_Subscription_Term__c, "
        "Termination_Date__c, Expected_Termination_Date__c, Termination_Reason__c, "
        "AuM_m__c, NumberOfEmployees "
        "FROM Account "
        "WHERE CreatedDate >= 2022-01-01T00:00:00Z",
    )
    print(f"  Queried {len(accounts)} accounts")

    # ── Build CSV ──
    fields = [
        "Id",
        "Name",
        "OwnerName",
        "Type",
        "BillingCountry",
        "Industry",
        "CreatedDate",
        "UnitGroup",
        "IsSaaS",
        "AxiomaClient",
        "KYCStatus",
        "RiskLevel",
        "HasDUNS",
        "HasUnitGroup",
        "HasAxiomaId",
        "DataQualityScore",
        "DataQualityBand",
        "PartnerLevel",
        "TerminationDate",
        "ExpectedTerminationDate",
        "TerminationReason",
        "CreatedMonth",
        "AuM",
        "Employees",
        "SizeBand",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()

    for a in accounts:
        owner = a.get("Owner") or {}

        has_duns = "true" if a.get("DUNS_No__c") else "false"
        has_unit_group = "true" if a.get("Unit_Group__c") else "false"
        has_axioma = (
            "true"
            if str(a.get("Axioma_Client__c", False)).lower() == "true"
            else "false"
        )
        industry = a.get("Industry") or ""
        billing_country = a.get("BillingCountry") or ""
        acct_type = a.get("Type") or ""

        # DataQualityScore: 0-5 count of populated fields
        dq_score = sum(
            [
                has_duns == "true",
                has_unit_group == "true",
                bool(industry),
                bool(billing_country),
                bool(acct_type),
            ]
        )
        if dq_score >= 4:
            dq_band = "Good"
        elif dq_score >= 2:
            dq_band = "Fair"
        else:
            dq_band = "Poor"

        is_saas = str(a.get("SaaS_Client__c", False)).lower() or "false"
        axioma_client = str(a.get("Axioma_Client__c", False)).lower() or "false"
        kyc_status = a.get("KYC_Approval_Status__c") or "Not Started"
        risk_level = a.get("Risk_of_Potential_Termination__c") or "Low"
        created_date = (a.get("CreatedDate") or "")[:10]
        created_month = created_date[:7]

        aum = a.get("AuM_m__c") or 0
        employees = a.get("NumberOfEmployees") or 0
        size_band = (
            "Large"
            if employees >= 1000
            else "Mid-Market"
            if employees >= 100
            else "Small"
            if employees > 0
            else "Unknown"
        )

        writer.writerow(
            {
                "Id": a.get("Id", ""),
                "Name": (a.get("Name") or "")[:255],
                "OwnerName": (owner.get("Name") or "")[:255],
                "Type": acct_type,
                "BillingCountry": billing_country,
                "Industry": industry,
                "CreatedDate": created_date,
                "UnitGroup": a.get("Unit_Group__c") or "",
                "IsSaaS": is_saas,
                "AxiomaClient": axioma_client,
                "KYCStatus": kyc_status,
                "RiskLevel": risk_level,
                "HasDUNS": has_duns,
                "HasUnitGroup": has_unit_group,
                "HasAxiomaId": has_axioma,
                "DataQualityScore": dq_score,
                "DataQualityBand": dq_band,
                "PartnerLevel": a.get("Partner_Engagement_Level__c") or "",
                "TerminationDate": (a.get("Termination_Date__c") or "")[:10],
                "ExpectedTerminationDate": (
                    a.get("Expected_Termination_Date__c") or ""
                )[:10],
                "TerminationReason": a.get("Termination_Reason__c") or "",
                "CreatedMonth": created_month,
                "AuM": aum,
                "Employees": employees,
                "SizeBand": size_band,
            }
        )

    # ── Phase 9: Python-precomputed Segment field ──
    # Rule-based segmentation: Strategic / Growth / Maintain / At-Risk
    buf2 = io.StringIO()
    orig_fields = [
        "Id",
        "Name",
        "OwnerName",
        "Type",
        "BillingCountry",
        "Industry",
        "CreatedDate",
        "UnitGroup",
        "IsSaaS",
        "AxiomaClient",
        "KYCStatus",
        "RiskLevel",
        "HasDUNS",
        "HasUnitGroup",
        "HasAxiomaId",
        "DataQualityScore",
        "DataQualityBand",
        "PartnerLevel",
        "TerminationDate",
        "ExpectedTerminationDate",
        "TerminationReason",
        "CreatedMonth",
        "AuM",
        "Employees",
        "SizeBand",
    ]
    ext_fields = orig_fields + ["Segment"]
    writer2 = csv.DictWriter(buf2, fieldnames=ext_fields, lineterminator="\n")
    writer2.writeheader()
    buf.seek(0)
    reader = csv.DictReader(buf)
    seg_counts = {"Strategic": 0, "Growth": 0, "Maintain": 0, "At-Risk": 0}
    for row in reader:
        aum_val = float(row.get("AuM") or 0)
        risk = row.get("RiskLevel", "Low")
        is_saas = row.get("IsSaaS", "false")
        dq = int(float(row.get("DataQualityScore") or 0))
        if risk in ("High", "Critical"):
            segment = "At-Risk"
        elif aum_val >= 100 and is_saas == "true":
            segment = "Strategic"
        elif aum_val >= 10 or is_saas == "true":
            segment = "Growth"
        else:
            segment = "Maintain"
        row["Segment"] = segment
        seg_counts[segment] = seg_counts.get(segment, 0) + 1
        writer2.writerow(row)
    csv_bytes = buf2.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes, {len(accounts)} rows (with Segment)")
    print(f"  Segments: {seg_counts}")

    # ── Metadata ──
    fields_meta = [
        _dim("Id", "Account ID"),
        _dim("Name", "Account Name"),
        _dim("OwnerName", "Owner"),
        _dim("Type", "Account Type"),
        _dim("BillingCountry", "Billing Country"),
        _dim("Industry", "Industry"),
        _date("CreatedDate", "Created Date"),
        _dim("UnitGroup", "Unit Group"),
        _dim("IsSaaS", "SaaS Client"),
        _dim("AxiomaClient", "Axioma Client"),
        _dim("KYCStatus", "KYC Status"),
        _dim("RiskLevel", "Risk Level"),
        _dim("HasDUNS", "Has DUNS"),
        _dim("HasUnitGroup", "Has Unit Group"),
        _dim("HasAxiomaId", "Has Axioma ID"),
        _measure("DataQualityScore", "Data Quality Score", scale=0, precision=1),
        _dim("DataQualityBand", "Data Quality Band"),
        _dim("PartnerLevel", "Partner Engagement Level"),
        _date("TerminationDate", "Termination Date"),
        _date("ExpectedTerminationDate", "Expected Termination Date"),
        _dim("TerminationReason", "Termination Reason"),
        _dim("CreatedMonth", "Created Month"),
        _measure("AuM", "AuM (Billions)", scale=2, precision=6),
        _measure("Employees", "Employees", scale=0, precision=10),
        _dim("SizeBand", "Size Band"),
        _dim("Segment", "Account Segment"),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


def create_contact_dataset(inst, tok):
    """Build Contact_Coverage from Contact SOQL with computed fields."""
    print("\n=== Building Contact Coverage dataset ===")

    contacts = _soql(
        inst,
        tok,
        "SELECT Id, AccountId, Account.Name, CreatedDate, Title, "
        "Department__c, LastActivityDate "
        "FROM Contact WHERE CreatedDate >= 2024-01-01T00:00:00Z",
    )
    print(f"  Queried {len(contacts)} contacts")

    # ── Build CSV ──
    fields = [
        "Id",
        "AccountId",
        "AccountName",
        "CreatedDate",
        "Title",
        "Department",
        "LastActivityDate",
        "ContactLevel",
        "HasRecentActivity",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()

    c_level_keywords = {"CEO", "CFO", "CTO", "CIO", "COO", "CMO"}

    for c in contacts:
        acct = c.get("Account") or {}
        title = (c.get("Title") or "").strip()
        title_upper = title.upper()

        # Determine ContactLevel from Title
        if any(kw in title_upper for kw in c_level_keywords):
            contact_level = "C-Level"
        elif "VP" in title_upper or "VICE PRESIDENT" in title_upper:
            contact_level = "VP"
        elif "DIRECTOR" in title_upper:
            contact_level = "Director"
        elif "MANAGER" in title_upper:
            contact_level = "Manager"
        else:
            contact_level = "Other"

        last_activity = c.get("LastActivityDate") or ""
        has_recent_activity = "true" if last_activity else "false"

        writer.writerow(
            {
                "Id": c.get("Id", ""),
                "AccountId": c.get("AccountId") or "",
                "AccountName": (acct.get("Name") or "")[:255],
                "CreatedDate": (c.get("CreatedDate") or "")[:10],
                "Title": title[:255],
                "Department": (c.get("Department__c") or "")[:255],
                "LastActivityDate": last_activity[:10] if last_activity else "",
                "ContactLevel": contact_level,
                "HasRecentActivity": has_recent_activity,
            }
        )

    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes, {len(contacts)} rows")

    # ── Metadata ──
    fields_meta = [
        _dim("Id", "Contact ID"),
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account Name"),
        _date("CreatedDate", "Created Date"),
        _dim("Title", "Title"),
        _dim("Department", "Department"),
        _date("LastActivityDate", "Last Activity Date"),
        _dim("ContactLevel", "Contact Level"),
        _dim("HasRecentActivity", "Has Recent Activity"),
    ]

    return upload_dataset(
        inst, tok, CONTACT_DS, CONTACT_DS_LABEL, fields_meta, csv_bytes
    )


# ═══════════════════════════════════════════════════════════════════════════
#  Filter binding expressions
# ═══════════════════════════════════════════════════════════════════════════

UF = coalesce_filter("f_unit", "UnitGroup")
IF = coalesce_filter("f_industry", "Industry")
RKF = coalesce_filter("f_risk", "RiskLevel")
KF = coalesce_filter("f_kyc", "KYCStatus")

# ── YoY period filters for trend_step comparisons ──
# CreatedDate is stored as yyyy-MM-dd; use substr to extract year.
CURRENT_YEAR_FILTER = 'q = filter q by substr(CreatedDate, 1, 4) == "2026";\n'
PRIOR_YEAR_FILTER = 'q = filter q by substr(CreatedDate, 1, 4) == "2025";\n'


# ═══════════════════════════════════════════════════════════════════════════
#  Steps
# ═══════════════════════════════════════════════════════════════════════════


def build_steps(ds_meta, contact_ds_meta):
    L = f'q = load "{DS}";\n'
    CL = f'q = load "{CONTACT_DS}";\n'

    # ── Base filters string (all 4 filter bindings) ──
    base_filters = UF + IF + RKF + KF

    return {
        # ═══ FILTER STEPS ═══
        "f_unit": af("UnitGroup", ds_meta),
        "f_industry": af("Industry", ds_meta),
        "f_risk": af("RiskLevel", ds_meta),
        "f_kyc": af("KYCStatus", ds_meta),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 1 — Data Quality (AP 1.2)
        # ═══════════════════════════════════════════════════════════════════
        # Gauge: DUNS fill rate (% of accounts where HasDUNS == "true")
        "s_duns_rate": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = foreach q generate (case when HasDUNS == "true" then 1 else 0 end) as has_it;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(has_it) / count()) * 100 as fill_rate;"
        ),
        # Gauge: UnitGroup fill rate
        "s_unitgroup_rate": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = foreach q generate (case when HasUnitGroup == "true" then 1 else 0 end) as has_it;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(has_it) / count()) * 100 as fill_rate;"
        ),
        # Gauge: Axioma ID fill rate
        "s_axioma_rate": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = foreach q generate (case when HasAxiomaId == "true" then 1 else 0 end) as has_it;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(has_it) / count()) * 100 as fill_rate;"
        ),
        # Line: Avg data quality score by CreatedMonth
        "s_dq_trend": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by CreatedMonth >= "2024-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, avg(DataQualityScore) as avg_score, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Column: Accounts created monthly
        "s_created_monthly": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by CreatedMonth >= "2024-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Comparisontable: Accounts with DataQualityScore < 3 (top 25)
        "s_dq_poor_list": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = filter q by DataQualityScore < 3;\n"
            + "q = foreach q generate Id, Name, OwnerName, UnitGroup, "
            + "DataQualityScore, DataQualityBand, HasDUNS, HasUnitGroup, HasAxiomaId;\n"
            + "q = order q by DataQualityScore asc;\n"
            + "q = limit q 25;"
        ),
        # Donut: Data quality band distribution
        "s_dq_band": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = group q by DataQualityBand;\n"
            + "q = foreach q generate DataQualityBand, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 2 — KYC Pipeline (AP 1.2)
        # ═══════════════════════════════════════════════════════════════════
        # Funnel: KYC status distribution ordered by count desc
        # (groups by KYCStatus — skip KF)
        "s_kyc_funnel": sq(
            L
            + UF
            + IF
            + RKF
            + "q = group q by KYCStatus;\n"
            + "q = foreach q generate KYCStatus, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Number: Not-started count
        "s_kyc_not_started": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by KYCStatus == "Not Started";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Trend: KYC Not Started YoY (accounts created in 2026 vs 2025)
        "s_kyc_not_started_trend": trend_step(
            DS,
            base_filters + 'q = filter q by KYCStatus == "Not Started";\n',
            CURRENT_YEAR_FILTER,
            PRIOR_YEAR_FILTER,
            "all",
            "count()",
            "current",
        ),
        # Stackhbar: KYC by UnitGroup stacked by KYC status
        # (groups by KYCStatus — skip KF)
        "s_kyc_by_unit": sq(
            L
            + UF
            + IF
            + RKF
            + "q = group q by (UnitGroup, KYCStatus);\n"
            + "q = foreach q generate UnitGroup, KYCStatus, count() as cnt;\n"
            + "q = order q by UnitGroup asc;"
        ),
        # Comparisontable: KYC detail list
        "s_kyc_detail": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = foreach q generate Id, Name, OwnerName, UnitGroup, "
            + "KYCStatus, Type, BillingCountry;\n"
            + "q = order q by KYCStatus asc;\n"
            + "q = limit q 25;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 3 — Customer Health (AP 1.7)
        # ═══════════════════════════════════════════════════════════════════
        # Donut: Risk distribution
        # (groups by RiskLevel — skip RKF)
        "s_risk_dist": sq(
            L
            + UF
            + IF
            + KF
            + "q = group q by RiskLevel;\n"
            + "q = foreach q generate RiskLevel, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Number: At-risk count (High + Medium)
        "s_at_risk": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by RiskLevel in ["High", "Medium"];\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Trend: At-risk accounts YoY (created 2026 vs 2025)
        "s_at_risk_trend": trend_step(
            DS,
            base_filters + 'q = filter q by RiskLevel in ["High", "Medium"];\n',
            CURRENT_YEAR_FILTER,
            PRIOR_YEAR_FILTER,
            "all",
            "count()",
            "current",
        ),
        # Number: Total accounts
        "s_total_accounts": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Trend: Total accounts YoY (created 2026 vs 2025)
        "s_total_accounts_trend": trend_step(
            DS,
            base_filters,
            CURRENT_YEAR_FILTER,
            PRIOR_YEAR_FILTER,
            "all",
            "count()",
            "current",
        ),
        # Gauge: Churn risk score (% of accounts at High risk)
        "s_churn_pct": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = foreach q generate (case when RiskLevel == "High" then 1 else 0 end) as is_high;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_high) / count()) * 100 as churn_pct;"
        ),
        # Stackhbar: Risk by UnitGroup stacked by risk level
        # (groups by RiskLevel — skip RKF)
        "s_risk_by_unit": sq(
            L
            + UF
            + IF
            + KF
            + "q = group q by (UnitGroup, RiskLevel);\n"
            + "q = foreach q generate UnitGroup, RiskLevel, count() as cnt;\n"
            + "q = order q by UnitGroup asc;"
        ),
        # Comparisontable: High-risk accounts
        "s_risk_list": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by RiskLevel in ["High", "Medium"];\n'
            + "q = foreach q generate Id, Name, RiskLevel, UnitGroup, OwnerName, "
            + "TerminationReason, ExpectedTerminationDate;\n"
            + "q = order q by RiskLevel asc;\n"
            + "q = limit q 25;"
        ),
        # Hbar: Partner engagement levels
        "s_partner_levels": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by PartnerLevel != "";\n'
            + "q = group q by PartnerLevel;\n"
            + "q = foreach q generate PartnerLevel, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 4 — Cross-sell & Contact (AP 1.3/1.7)
        # ═══════════════════════════════════════════════════════════════════
        # Number: Axioma accounts count
        "s_axioma_count": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by AxiomaClient == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Gauge: Axioma penetration %
        "s_axioma_pct": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = foreach q generate (case when AxiomaClient == "true" then 1 else 0 end) as is_ax;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_ax) / count()) * 100 as ax_pct;"
        ),
        # Waterfall: Quarterly SaaS adoption adds (new SaaS accounts by quarter)
        "s_saas_quarterly": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by IsSaaS == "true";\n'
            + 'q = filter q by CreatedMonth != "";\n'
            + "q = foreach q generate "
            + '(case when substr(CreatedDate, 6, 2) in ["01","02","03"] then substr(CreatedDate, 1, 4) || "-Q1" '
            + 'when substr(CreatedDate, 6, 2) in ["04","05","06"] then substr(CreatedDate, 1, 4) || "-Q2" '
            + 'when substr(CreatedDate, 6, 2) in ["07","08","09"] then substr(CreatedDate, 1, 4) || "-Q3" '
            + 'else substr(CreatedDate, 1, 4) || "-Q4" end) as Quarter;\n'
            + "q = group q by Quarter;\n"
            + "q = foreach q generate Quarter, count() as cnt;\n"
            + "q = order q by Quarter asc;"
        ),
        # Hbar: Contacts per account (distribution bands) from Contact_Coverage
        # (Contact_Coverage dataset — filter bindings skipped)
        "s_contacts_per_acct": sq(
            CL
            + "q = group q by AccountName;\n"
            + "q = foreach q generate AccountName, count() as contact_cnt;\n"
            + "q = foreach q generate "
            + '(case when contact_cnt == 0 then "0" '
            + 'when contact_cnt <= 2 then "1-2" '
            + 'when contact_cnt <= 5 then "3-5" '
            + 'when contact_cnt <= 10 then "6-10" '
            + 'else "10+" end) as ContactBand;\n'
            + "q = group q by ContactBand;\n"
            + "q = foreach q generate ContactBand, count() as cnt;\n"
            + "q = order q by ContactBand asc;"
        ),
        # Donut: Department coverage (ContactLevel distribution) from Contact_Coverage
        # (Contact_Coverage dataset — filter bindings skipped)
        "s_contact_level": sq(
            CL
            + "q = group q by ContactLevel;\n"
            + "q = foreach q generate ContactLevel, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 5 — Segment Analytics
        # ═══════════════════════════════════════════════════════════════════
        # Donut: Industry distribution
        "s_industry_dist": sq(
            L
            + UF
            + RKF
            + 'q = filter q by Industry != "";\n'
            + "q = group q by Industry;\n"
            + "q = foreach q generate Industry, count() as cnt;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 15;"
        ),
        # Stackhbar: Industry × Risk Level
        "s_industry_risk": sq(
            L
            + UF
            + 'q = filter q by Industry != "" and RiskLevel != "";\n'
            + "q = group q by (Industry, RiskLevel);\n"
            + "q = foreach q generate Industry, RiskLevel, count() as cnt;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 30;"
        ),
        # Comparisontable: AuM by Unit Group
        "s_aum_by_unit": sq(
            L
            + IF
            + RKF
            + KF
            + "q = filter q by AuM > 0;\n"
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, sum(AuM) as total_aum, count() as cnt, avg(AuM) as avg_aum;\n"
            + "q = order q by total_aum desc;"
        ),
        # Donut: Size band distribution
        "s_size_dist": sq(
            L
            + UF
            + IF
            + RKF
            + 'q = filter q by SizeBand != "Unknown";\n'
            + "q = group q by SizeBand;\n"
            + "q = foreach q generate SizeBand, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Hbar: Top 20 countries
        "s_country_top": sq(
            L
            + UF
            + IF
            + RKF
            + 'q = filter q by BillingCountry != "";\n'
            + "q = group q by BillingCountry;\n"
            + "q = foreach q generate BillingCountry, count() as cnt;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 20;"
        ),
        # Stackhbar: Industry × KYC Status
        "s_industry_kyc": sq(
            L
            + UF
            + 'q = filter q by Industry != "" and KYCStatus != "";\n'
            + "q = group q by (Industry, KYCStatus);\n"
            + "q = foreach q generate Industry, KYCStatus, count() as cnt;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 30;"
        ),
        # ═══ CONTACT METRICS & NRR PROXY (AP 1.7 gaps) ═══
        # Contacts created monthly (from Contact_Coverage)
        "s_contact_monthly": sq(
            CL
            + 'q = filter q by CreatedDate != "";\n'
            + "q = foreach q generate substr(CreatedDate, 1, 7) as CreatedMonth;\n"
            + 'q = filter q by CreatedMonth >= "2024-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Contact activity rate gauge (% with recent activity)
        "s_contact_activity_rate": sq(
            CL
            + 'q = foreach q generate (case when HasRecentActivity == "true" then 1 else 0 end) as has_act;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(has_act) / count()) * 100 as activity_rate;"
        ),
        # Retention proxy: % of accounts NOT at High risk (NRR approximation)
        "s_retention_rate": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = foreach q generate (case when RiskLevel != "High" then 1 else 0 end) as retained;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(retained) / count()) * 100 as retention_rate;"
        ),
        # Revenue Concentration: Top 20 accounts by AuM (Pareto / CRO additive)
        "s_rev_concentration": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = filter q by AuM > 0;\n"
            + "q = group q by Name;\n"
            + "q = foreach q generate Name, sum(AuM) as total_aum;\n"
            + "q = order q by total_aum desc;\n"
            + "q = limit q 20;"
        ),
        # ═══ ITERATION 3: Customer Vintage Cohort (Additive CRO #4) ═══
        # Accounts grouped by creation year — cohort analysis
        "s_vintage_cohort": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by CreatedDate != "";\n'
            + "q = foreach q generate substr(CreatedDate, 1, 4) as VintageYear, "
            + "AuM, RiskLevel;\n"
            + "q = group q by VintageYear;\n"
            + "q = foreach q generate VintageYear, count() as acct_cnt, "
            + "sum(AuM) as total_aum, avg(AuM) as avg_aum;\n"
            + "q = order q by VintageYear asc;"
        ),
        # Vintage by risk level
        "s_vintage_risk": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by CreatedDate != "";\n'
            + "q = foreach q generate substr(CreatedDate, 1, 4) as VintageYear, RiskLevel;\n"
            + "q = group q by (VintageYear, RiskLevel);\n"
            + "q = foreach q generate VintageYear, RiskLevel, count() as cnt;\n"
            + "q = order q by VintageYear asc;"
        ),
        # Vintage retention: % not-High-risk by cohort
        "s_vintage_retention": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by CreatedDate != "";\n'
            + "q = foreach q generate substr(CreatedDate, 1, 4) as VintageYear, "
            + '(case when RiskLevel != "High" then 1 else 0 end) as retained;\n'
            + "q = group q by VintageYear;\n"
            + "q = foreach q generate VintageYear, "
            + "(sum(retained) / count()) * 100 as retention_rate, count() as total;\n"
            + "q = order q by VintageYear asc;"
        ),
        # ═══ V2: Advanced Visualizations ═══
        # Treemap: AuM by Industry → UnitGroup
        "s_treemap_aum": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by Industry != "" && UnitGroup != "";\n'
            + "q = group q by (Industry, UnitGroup);\n"
            + "q = foreach q generate Industry, UnitGroup, sum(AuM) as total_aum;\n"
            + "q = order q by total_aum desc;"
        ),
        # Heatmap: Industry × RiskLevel count
        "s_heatmap_risk": sq(
            L
            + UF
            + KF
            + 'q = filter q by Industry != "" && RiskLevel != "";\n'
            + "q = group q by (Industry, RiskLevel);\n"
            + "q = foreach q generate Industry, RiskLevel, count() as cnt;\n"
            + "q = order q by Industry asc;"
        ),
        # Bubble: AuM × DataQuality × ContactCount
        "s_bubble_health": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = group q by (Name, Industry);\n"
            + "q = foreach q generate Name, Industry, "
            + "sum(AuM) as total_aum, avg(DataQualityScore) as dq_score, "
            + "count() as acct_count;\n"
            + "q = filter q by total_aum > 0;\n"
            + "q = order q by total_aum desc;\n"
            + "q = limit q 100;"
        ),
        # ═══ V2 Phase 6: Bullet Chart ═══
        "s_bullet_dq": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = group q by all;\n"
            + "q = foreach q generate avg(DataQualityScore) as dq_score, 80 as target;"
        ),
        # ═══ V2 Phase 8: Statistical Analysis ═══
        "s_stat_dq_dist": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "min(DataQualityScore) as min_dq, "
            + "percentile_disc(0.25) within group (order by DataQualityScore) as p25, "
            + "percentile_disc(0.50) within group (order by DataQualityScore) as median_dq, "
            + "percentile_disc(0.75) within group (order by DataQualityScore) as p75, "
            + "max(DataQualityScore) as max_dq, "
            + "avg(DataQualityScore) as mean_dq, stddev(DataQualityScore) as std_dev, "
            + "count() as acct_count;"
        ),
        "s_stat_industry_aum": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by Industry != "";\n'
            + "q = group q by Industry;\n"
            + "q = foreach q generate Industry, sum(AuM) as total_aum, count() as cnt, "
            + "avg(DataQualityScore) as avg_dq;\n"
            + "q = order q by total_aum desc;\n"
            + "q = limit q 20;"
        ),
        # ═══ V2 Phase 9: Python-precomputed segment ═══
        "s_segment_dist": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by Segment != "";\n'
            + "q = group q by Segment;\n"
            + "q = foreach q generate Segment, count() as cnt, sum(AuM) as total_aum;\n"
            + "q = order q by total_aum desc;"
        ),
        "s_segment_aum_bar": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by Segment != "";\n'
            + "q = group q by (Segment, Industry);\n"
            + "q = foreach q generate Segment, Industry, sum(AuM) as total_aum;\n"
            + "q = order q by Segment asc, total_aum desc;"
        ),
        # ═══ Whitespace Cross-Sell Matrix ═══
        # Heatmap: UnitGroup × Industry showing account count (gaps = whitespace)
        "s_whitespace_matrix": sq(
            L
            + RKF
            + KF
            + 'q = filter q by Industry != "" && UnitGroup != "";\n'
            + "q = group q by (UnitGroup, Industry);\n"
            + "q = foreach q generate UnitGroup, Industry, count() as acct_count, "
            + "sum(AuM) as total_aum;\n"
            + "q = order q by UnitGroup asc;"
        ),
        # Cross-sell opportunity: accounts with single product/service (low contact coverage)
        "s_whitespace_low_coverage": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by AxiomaClient == "false";\n'
            + "q = foreach q generate Id, Name, OwnerName, UnitGroup, Industry, AuM, "
            + "DataQualityScore, RiskLevel;\n"
            + "q = order q by AuM desc;\n"
            + "q = limit q 25;"
        ),
        # Summary: Accounts per UnitGroup with Axioma penetration
        "s_whitespace_unit_summary": sq(
            L
            + IF
            + RKF
            + KF
            + "q = group q by UnitGroup;\n"
            + "q = foreach q generate UnitGroup, count() as total_accts, "
            + 'sum(case when AxiomaClient == "true" then 1 else 0 end) as axioma_accts, '
            + "sum(AuM) as total_aum, "
            + '(sum(case when AxiomaClient == "true" then 1 else 0 end) * 100 / count()) as axioma_pct;\n'
            + "q = order q by total_aum desc;"
        ),
        # ═══ V2 Phase 10: Sankey ═══
        "s_sankey_kyc": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = group q by (KYCStatus, RiskLevel);\n"
            + "q = foreach q generate KYCStatus, RiskLevel, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══ V2 Phase 10: Area ═══
        "s_area_dq_trend": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, "
            + "avg(DataQualityScore) as avg_dq, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # ═══ V2 Gap Fill: Running total ═══
        "s_running_acct_arr": sq(
            L
            + UF
            + IF
            + RKF
            + KF
            + 'q = filter q by CreatedMonth != "";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, sum(AuM) as monthly_aum;\n"
            + "q = order q by CreatedMonth asc;\n"
            + "q = foreach q generate CreatedMonth, monthly_aum, "
            + "sum(monthly_aum) over (order by CreatedMonth "
            + "rows unbounded preceding) as cumul_aum;"
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Widgets
# ═══════════════════════════════════════════════════════════════════════════


def build_widgets():
    w = {
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 1 — Data Quality
        # ═══════════════════════════════════════════════════════════════════
        "p1_nav1": nav_link("dataquality", "Data Quality", active=True),
        "p1_nav2": nav_link("kyc", "KYC Pipeline"),
        "p1_nav3": nav_link("health", "Customer Health"),
        "p1_nav4": nav_link("crosssell", "Cross-sell & Contact"),
        "p1_nav5": nav_link("segments", "Segment Analysis"),
        "p1_hdr": hdr(
            "Data Quality Overview",
            "AP 1.2 | Field completeness & data health",
        ),
        # Filter bar (4 pillbox filters)
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_industry": pillbox("f_industry", "Industry"),
        "p1_f_risk": pillbox("f_risk", "Risk Level"),
        "p1_f_kyc": pillbox("f_kyc", "KYC Status"),
        # Gauges: fill rates
        "p1_g_duns": gauge(
            "s_duns_rate",
            "fill_rate",
            "DUNS Fill Rate %",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 30, "color": "#D4504C"},
                {"start": 30, "stop": 60, "color": "#FFB75D"},
                {"start": 60, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p1_g_unit": gauge(
            "s_unitgroup_rate",
            "fill_rate",
            "Unit Group Fill Rate %",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 30, "color": "#D4504C"},
                {"start": 30, "stop": 60, "color": "#FFB75D"},
                {"start": 60, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p1_g_axioma": gauge(
            "s_axioma_rate",
            "fill_rate",
            "Axioma ID Fill Rate %",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 30, "color": "#D4504C"},
                {"start": 30, "stop": 60, "color": "#FFB75D"},
                {"start": 60, "stop": 100, "color": "#04844B"},
            ],
        ),
        # Line: DQ trend
        "p1_ch_trend": rich_chart(
            "s_dq_trend",
            "line",
            "Data Quality Trend (Avg Score by Month)",
            ["CreatedMonth"],
            ["avg_score"],
            axis_title="Avg Score (0-5)",
        ),
        # Column: Accounts created monthly
        "p1_ch_created": rich_chart(
            "s_created_monthly",
            "column",
            "Accounts Created Monthly",
            ["CreatedMonth"],
            ["cnt"],
            axis_title="Count",
        ),
        # Comparisontable: Missing fields detail
        "p1_sec_detail": section_label("Missing Fields Detail"),
        "p1_tbl_poor": rich_chart(
            "s_dq_poor_list",
            "comparisontable",
            "Accounts with Data Quality Score < 3 (Top 25)",
            [
                "Name",
                "OwnerName",
                "UnitGroup",
                "DataQualityBand",
                "HasDUNS",
                "HasUnitGroup",
                "HasAxiomaId",
            ],
            ["DataQualityScore"],
        ),
        # Donut: Overall data quality distribution
        "p1_ch_band": rich_chart(
            "s_dq_band",
            "donut",
            "Data Quality Distribution",
            ["DataQualityBand"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 2 — KYC Pipeline
        # ═══════════════════════════════════════════════════════════════════
        "p2_nav1": nav_link("dataquality", "Data Quality"),
        "p2_nav2": nav_link("kyc", "KYC Pipeline", active=True),
        "p2_nav3": nav_link("health", "Customer Health"),
        "p2_nav4": nav_link("crosssell", "Cross-sell & Contact"),
        "p2_nav5": nav_link("segments", "Segment Analysis"),
        "p2_hdr": hdr(
            "KYC Pipeline",
            "AP 1.2 | KYC approval status tracking",
        ),
        # Filter bar (4 pillbox filters)
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_industry": pillbox("f_industry", "Industry"),
        "p2_f_risk": pillbox("f_risk", "Risk Level"),
        "p2_f_kyc": pillbox("f_kyc", "KYC Status"),
        # Funnel: KYC status (columnMap: None)
        "p2_ch_funnel": funnel_chart(
            "s_kyc_funnel",
            "KYC Status Distribution",
            "KYCStatus",
            "cnt",
        ),
        # Number+Trend: KYC Not Started (2026 vs 2025 created cohort)
        "p2_n_not_started": num_with_trend(
            "s_kyc_not_started_trend",
            "current",
            "KYC Not Started (YoY)",
            "#D4504C",
            compact=False,
            size=28,
        ),
        # Stackhbar: KYC by UnitGroup
        "p2_ch_kyc_unit": rich_chart(
            "s_kyc_by_unit",
            "stackhbar",
            "KYC Status by Unit Group",
            ["UnitGroup"],
            ["cnt"],
            split=["KYCStatus"],
            show_legend=True,
            axis_title="Count",
        ),
        # Comparisontable: KYC detail list
        "p2_sec_detail": section_label("KYC Account Detail"),
        "p2_tbl_kyc": rich_chart(
            "s_kyc_detail",
            "comparisontable",
            "KYC Account Detail (Top 25)",
            ["Name", "OwnerName", "UnitGroup", "KYCStatus", "Type", "BillingCountry"],
            [],
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 3 — Customer Health
        # ═══════════════════════════════════════════════════════════════════
        "p3_nav1": nav_link("dataquality", "Data Quality"),
        "p3_nav2": nav_link("kyc", "KYC Pipeline"),
        "p3_nav3": nav_link("health", "Customer Health", active=True),
        "p3_nav4": nav_link("crosssell", "Cross-sell & Contact"),
        "p3_nav5": nav_link("segments", "Segment Analysis"),
        "p3_hdr": hdr(
            "Customer Health",
            "AP 1.7 | Risk monitoring & churn prevention",
        ),
        # Filter bar (4 pillbox filters)
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_industry": pillbox("f_industry", "Industry"),
        "p3_f_risk": pillbox("f_risk", "Risk Level"),
        "p3_f_kyc": pillbox("f_kyc", "KYC Status"),
        # Donut: Risk distribution
        "p3_ch_risk": rich_chart(
            "s_risk_dist",
            "donut",
            "Risk Level Distribution",
            ["RiskLevel"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        # Number+Trend: At-risk accounts (2026 vs 2025 created cohort)
        "p3_n_at_risk": num_with_trend(
            "s_at_risk_trend",
            "current",
            "At-Risk Accounts (YoY)",
            "#D4504C",
            compact=False,
            size=28,
        ),
        # Number+Trend: Total accounts (2026 vs 2025 created cohort)
        "p3_n_total": num_with_trend(
            "s_total_accounts_trend",
            "current",
            "New Accounts (YoY)",
            "#0070D2",
            compact=False,
            size=24,
        ),
        # Gauge: Churn risk score
        "p3_g_churn": gauge(
            "s_churn_pct",
            "churn_pct",
            "Churn Risk (% High)",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 5, "color": "#04844B"},
                {"start": 5, "stop": 10, "color": "#FFB75D"},
                {"start": 10, "stop": 100, "color": "#D4504C"},
            ],
        ),
        # Stackhbar: Risk by UnitGroup
        "p3_ch_risk_unit": rich_chart(
            "s_risk_by_unit",
            "stackhbar",
            "Risk by Unit Group",
            ["UnitGroup"],
            ["cnt"],
            split=["RiskLevel"],
            show_legend=True,
            axis_title="Count",
        ),
        # Comparisontable: High-risk accounts
        "p3_sec_detail": section_label("At-Risk Account Detail"),
        "p3_tbl_risk": rich_chart(
            "s_risk_list",
            "comparisontable",
            "High & Medium Risk Accounts (Top 25)",
            [
                "Name",
                "RiskLevel",
                "UnitGroup",
                "OwnerName",
                "ExpectedTerminationDate",
                "TerminationReason",
            ],
            [],
        ),
        # Hbar: Partner engagement levels
        "p3_ch_partner": rich_chart(
            "s_partner_levels",
            "hbar",
            "Partner Engagement Levels",
            ["PartnerLevel"],
            ["cnt"],
            axis_title="Count",
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 4 — Cross-sell & Contact
        # ═══════════════════════════════════════════════════════════════════
        "p4_nav1": nav_link("dataquality", "Data Quality"),
        "p4_nav2": nav_link("kyc", "KYC Pipeline"),
        "p4_nav3": nav_link("health", "Customer Health"),
        "p4_nav4": nav_link("crosssell", "Cross-sell & Contact", active=True),
        "p4_nav5": nav_link("segments", "Segment Analysis"),
        "p4_hdr": hdr(
            "Cross-sell & Contact Coverage",
            "AP 1.3/1.7 | Axioma penetration, SaaS adoption, contact depth",
        ),
        # Filter bar (4 pillbox filters)
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_industry": pillbox("f_industry", "Industry"),
        "p4_f_risk": pillbox("f_risk", "Risk Level"),
        "p4_f_kyc": pillbox("f_kyc", "KYC Status"),
        # Number: Axioma accounts
        "p4_n_axioma": num(
            "s_axioma_count", "cnt", "Axioma Accounts", "#04844B", False, 28
        ),
        # Gauge: Axioma penetration
        "p4_g_axioma_pct": gauge(
            "s_axioma_pct",
            "ax_pct",
            "Axioma Penetration %",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 20, "color": "#D4504C"},
                {"start": 20, "stop": 40, "color": "#FFB75D"},
                {"start": 40, "stop": 100, "color": "#04844B"},
            ],
        ),
        # Waterfall: Quarterly SaaS adoption (columnMap: None)
        "p4_ch_saas": waterfall_chart(
            "s_saas_quarterly",
            "Quarterly SaaS Adoption",
            "Quarter",
            "cnt",
            axis_label="New SaaS Accounts",
        ),
        # Hbar: Contacts per account distribution (Contact_Coverage)
        "p4_sec_contact": section_label("Contact Coverage"),
        "p4_ch_contacts": rich_chart(
            "s_contacts_per_acct",
            "hbar",
            "Contacts per Account (Distribution)",
            ["ContactBand"],
            ["cnt"],
            axis_title="Accounts",
        ),
        # Donut: Contact level distribution (Contact_Coverage)
        "p4_ch_level": rich_chart(
            "s_contact_level",
            "donut",
            "Contact Seniority Mix",
            ["ContactLevel"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        # ── Contact Metrics & NRR Proxy (AP 1.7 gaps) ──
        "p4_sec_metrics": section_label("Contact & Retention Metrics"),
        "p4_g_contact_act": gauge(
            "s_contact_activity_rate",
            "activity_rate",
            "Contact Activity Rate %",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 30, "color": "#D4504C"},
                {"start": 30, "stop": 60, "color": "#FFB75D"},
                {"start": 60, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p4_g_retention": gauge(
            "s_retention_rate",
            "retention_rate",
            "Retention Rate % (NRR Proxy)",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 80, "color": "#D4504C"},
                {"start": 80, "stop": 95, "color": "#FFB75D"},
                {"start": 95, "stop": 100, "color": "#04844B"},
            ],
        ),
        "p4_ch_contact_monthly": rich_chart(
            "s_contact_monthly",
            "area",
            "Contacts Created Monthly",
            ["CreatedMonth"],
            ["cnt"],
            axis_title="Count",
        ),
        # ═══ Whitespace Cross-Sell Matrix ═══
        "p4_sec_whitespace": section_label("Whitespace Cross-Sell Matrix"),
        "p4_ch_whitespace": rich_chart(
            "s_whitespace_matrix",
            "stackhbar",
            "Accounts by Unit Group × Industry (Gaps = Whitespace)",
            ["UnitGroup"],
            ["acct_count"],
            split=["Industry"],
            show_legend=True,
            axis_title="Account Count",
        ),
        "p4_sec_ws_opps": section_label(
            "Cross-Sell Opportunities (Non-Axioma Accounts)"
        ),
        "p4_tbl_ws_opps": rich_chart(
            "s_whitespace_low_coverage",
            "comparisontable",
            "Top 25 Non-Axioma Accounts by AuM",
            ["Name", "OwnerName", "UnitGroup", "Industry", "RiskLevel"],
            ["AuM", "DataQualityScore"],
        ),
        "p4_sec_ws_summary": section_label("Unit Group Penetration Summary"),
        "p4_tbl_ws_summary": rich_chart(
            "s_whitespace_unit_summary",
            "comparisontable",
            "Axioma Penetration by Unit Group",
            ["UnitGroup"],
            ["total_accts", "axioma_accts", "axioma_pct", "total_aum"],
        ),
        # ═══════════════════════════════════════════════════════════════════
        #  PAGE 5 — Segment Analysis
        # ═══════════════════════════════════════════════════════════════════
        "p5_nav1": nav_link("dataquality", "Data Quality"),
        "p5_nav2": nav_link("kyc", "KYC Pipeline"),
        "p5_nav3": nav_link("health", "Customer Health"),
        "p5_nav4": nav_link("crosssell", "Cross-sell & Contact"),
        "p5_nav5": nav_link("segments", "Segment Analysis", active=True),
        "p5_hdr": hdr(
            "Segment Analysis",
            "Customer Base by Industry, Size & Geography",
        ),
        # Filter bar (4 pillbox filters)
        "p5_f_unit": pillbox("f_unit", "Unit Group"),
        "p5_f_industry": pillbox("f_industry", "Industry"),
        "p5_f_risk": pillbox("f_risk", "Risk Level"),
        "p5_f_kyc": pillbox("f_kyc", "KYC Status"),
        # Donut: Industry distribution
        "p5_ch_industry": rich_chart(
            "s_industry_dist",
            "donut",
            "Accounts by Industry",
            ["Industry"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        # Stackhbar: Industry × Risk Level
        "p5_ch_ind_risk": rich_chart(
            "s_industry_risk",
            "stackhbar",
            "Industry \u00d7 Risk Level",
            ["Industry"],
            ["cnt"],
            split=["RiskLevel"],
            show_legend=True,
            axis_title="Count",
        ),
        # Comparisontable: AuM by Unit Group
        "p5_ch_aum": rich_chart(
            "s_aum_by_unit",
            "comparisontable",
            "AuM by Unit Group",
            ["UnitGroup"],
            ["total_aum", "cnt", "avg_aum"],
        ),
        # Donut: Size band distribution
        "p5_ch_size": rich_chart(
            "s_size_dist",
            "donut",
            "Accounts by Size Band",
            ["SizeBand"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        # Hbar: Top 20 countries
        "p5_ch_country": rich_chart(
            "s_country_top",
            "hbar",
            "Top 20 Countries by Account Count",
            ["BillingCountry"],
            ["cnt"],
            axis_title="Count",
        ),
        # Stackhbar: Industry × KYC Status
        "p5_ch_ind_kyc": rich_chart(
            "s_industry_kyc",
            "stackhbar",
            "Industry \u00d7 KYC Status",
            ["Industry"],
            ["cnt"],
            split=["KYCStatus"],
            show_legend=True,
            axis_title="Count",
        ),
        # ── Revenue Concentration (Additive CRO) ──
        "p5_sec_concentration": section_label("Revenue Concentration (Pareto)"),
        "p5_ch_concentration": rich_chart(
            "s_rev_concentration",
            "hbar",
            "Top 20 Accounts by AuM (Revenue Concentration)",
            ["Name"],
            ["total_aum"],
            axis_title="AuM",
        ),
    }

    # ═══ ITERATION 3: Customer Vintage Cohort (Additive CRO #4) on Page 5 ═══
    w["p5_sec_vintage"] = section_label("Customer Vintage Cohort Analysis")
    w["p5_ch_vintage"] = rich_chart(
        "s_vintage_cohort",
        "column",
        "Accounts by Vintage Year",
        ["VintageYear"],
        ["acct_cnt"],
        axis_title="Count",
    )
    w["p5_ch_vintage_aum"] = rich_chart(
        "s_vintage_cohort",
        "hbar",
        "AuM by Vintage Year",
        ["VintageYear"],
        ["total_aum"],
        axis_title="AuM (m)",
    )
    w["p5_ch_vintage_risk"] = rich_chart(
        "s_vintage_risk",
        "stackhbar",
        "Vintage Cohort by Risk Level",
        ["VintageYear"],
        ["cnt"],
        split=["RiskLevel"],
        show_legend=True,
        axis_title="Count",
    )
    w["p5_ch_vintage_ret"] = rich_chart(
        "s_vintage_retention",
        "line",
        "Retention Rate by Vintage Year",
        ["VintageYear"],
        ["retention_rate"],
        axis_title="Retention %",
    )

    # ── Phase 7: Embedded table actions ──────────────────────────────────
    from crm_analytics_helpers import add_table_action

    add_table_action(w["p1_tbl_poor"], "salesforceActions", "Account", "Id")
    add_table_action(w["p2_tbl_kyc"], "salesforceActions", "Account", "Id")
    add_table_action(w["p3_tbl_risk"], "salesforceActions", "Account", "Id")
    add_table_action(w["p4_tbl_ws_opps"], "salesforceActions", "Account", "Id")

    # ═══ V2 PAGE 6: Advanced Analytics ═══
    w["p6_nav1"] = nav_link("dataquality", "Data Quality")
    w["p6_nav2"] = nav_link("kyc", "KYC Pipeline")
    w["p6_nav3"] = nav_link("health", "Customer Health")
    w["p6_nav4"] = nav_link("crosssell", "Cross-sell")
    w["p6_nav5"] = nav_link("segments", "Segments")
    w["p6_nav6"] = nav_link("advanalytics", "Advanced", active=True)
    w["p6_hdr"] = hdr(
        "Advanced Analytics",
        "AuM Composition | Risk Matrix | Account Health",
    )
    w["p6_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p6_f_industry"] = pillbox("f_industry", "Industry")
    w["p6_f_risk"] = pillbox("f_risk", "Risk Level")
    w["p6_f_kyc"] = pillbox("f_kyc", "KYC Status")
    # Treemap: AuM by Industry → UnitGroup
    w["p6_sec_treemap"] = section_label("AuM Composition")
    w["p6_ch_treemap"] = treemap_chart(
        "s_treemap_aum",
        "Assets Under Management by Industry & Unit",
        ["Industry", "UnitGroup"],
        "total_aum",
    )
    # Heatmap: Industry × Risk
    w["p6_sec_heatmap"] = section_label("Risk Concentration Matrix")
    w["p6_ch_heatmap"] = heatmap_chart(
        "s_heatmap_risk", "Account Count by Industry × Risk Level"
    )
    # Bubble: Account health
    w["p6_sec_bubble"] = section_label("Account Health Intelligence")
    w["p6_ch_bubble"] = bubble_chart(
        "s_bubble_health", "Accounts: AuM vs Data Quality (size = Count)"
    )
    # Sankey: KYC Status → Risk Level
    w["p6_sec_sankey"] = section_label("KYC → Risk Level Flow")
    w["p6_ch_sankey"] = sankey_chart(
        "s_sankey_kyc", "Account Flow: KYC Status → Risk Level"
    )
    # Area: DQ Score trend
    w["p6_sec_area"] = section_label("Data Quality Score Trend")
    w["p6_ch_area"] = area_chart(
        "s_area_dq_trend",
        "Average Data Quality Score Over Time",
        ["CreatedMonth"],
        ["avg_dq"],
        axis_title="Avg DQ Score",
    )

    # ═══ V2 PAGE 7: Bullet Charts & Statistical Analysis ═══
    w["p7_nav1"] = nav_link("dataquality", "Data Quality")
    w["p7_nav2"] = nav_link("kyc", "KYC Pipeline")
    w["p7_nav3"] = nav_link("health", "Customer Health")
    w["p7_nav4"] = nav_link("crosssell", "Cross-sell")
    w["p7_nav5"] = nav_link("segments", "Segments")
    w["p7_nav6"] = nav_link("advanalytics", "Advanced")
    w["p7_nav7"] = nav_link("acctstats", "Statistics", active=True)
    w["p7_hdr"] = hdr(
        "Account Statistical Analysis",
        "Data Quality Targets | Percentile Distribution | Industry AuM",
    )
    w["p7_f_unit"] = pillbox("f_unit", "Unit Group")
    w["p7_f_industry"] = pillbox("f_industry", "Industry")
    w["p7_f_risk"] = pillbox("f_risk", "Risk Level")
    w["p7_f_kyc"] = pillbox("f_kyc", "KYC Status")
    # Bullet: Data Quality target
    w["p7_sec_bullet"] = section_label("Data Quality Target")
    w["p7_bullet_dq"] = bullet_chart(
        "s_bullet_dq", "Avg Data Quality Score (Target: 80)", axis_title="Score"
    )
    # Stats: DQ percentile distribution
    w["p7_sec_dq_dist"] = section_label("Data Quality Score Distribution")
    w["p7_stat_dq_dist"] = rich_chart(
        "s_stat_dq_dist",
        "comparisontable",
        "Data Quality Percentiles (P25/Median/P75/Max)",
        [],
        [
            "min_dq",
            "p25",
            "median_dq",
            "p75",
            "max_dq",
            "mean_dq",
            "std_dev",
            "acct_count",
        ],
    )
    # Stats: Industry AuM analysis
    w["p7_sec_industry"] = section_label("Industry AuM & Quality Analysis")
    w["p7_stat_industry"] = rich_chart(
        "s_stat_industry_aum",
        "comparisontable",
        "Top 20 Industries by AuM (with Avg Data Quality)",
        ["Industry"],
        ["total_aum", "cnt", "avg_dq"],
    )

    # Phase 9: Python-precomputed Segment visualization
    w["p7_sec_segment"] = section_label("Account Segmentation (Python-Computed)")
    w["p7_segment_donut"] = rich_chart(
        "s_segment_dist",
        "donut",
        "Accounts by Segment",
        ["Segment"],
        ["cnt"],
    )
    w["p7_segment_bar"] = rich_chart(
        "s_segment_aum_bar",
        "stackhbar",
        "AuM by Segment × Industry",
        ["Segment", "Industry"],
        ["total_aum"],
        axis_title="AuM (Billions)",
    )
    # Cumulative AuM running total
    w["p7_sec_running"] = section_label("Cumulative AuM Over Time")
    w["p7_ch_running"] = area_chart(
        "s_running_acct_arr",
        "Cumulative AuM by Month",
        axis_title="AuM (m)",
    )

    # Add nav6 (Advanced) to pages 1-5
    for px in range(1, 6):
        w[f"p{px}_nav6"] = nav_link("advanalytics", "Advanced")
    # Add nav7 (Statistics) to pages 1-6
    for px in range(1, 7):
        w[f"p{px}_nav7"] = nav_link("acctstats", "Statistics")

    return w


# ═══════════════════════════════════════════════════════════════════════════
#  Layout
# ═══════════════════════════════════════════════════════════════════════════


def build_layout():
    # ── Page 1: Data Quality ──
    p1 = nav_row("p1", 7) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar (4 pillbox filters)
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_industry", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_risk", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_kyc", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Gauges (3 across)
        {"name": "p1_g_duns", "row": 7, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p1_g_unit", "row": 7, "column": 4, "colspan": 4, "rowspan": 4},
        {"name": "p1_g_axioma", "row": 7, "column": 8, "colspan": 4, "rowspan": 4},
        # Line + Column side-by-side
        {"name": "p1_ch_trend", "row": 11, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_ch_created", "row": 11, "column": 6, "colspan": 6, "rowspan": 8},
        # Donut: quality band
        {"name": "p1_ch_band", "row": 19, "column": 0, "colspan": 6, "rowspan": 8},
        # Section + Table
        {"name": "p1_sec_detail", "row": 19, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p1_tbl_poor", "row": 27, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    # ── Page 2: KYC Pipeline ──
    p2 = nav_row("p2", 7) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar (4 pillbox filters)
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_industry", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_risk", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_kyc", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Funnel + Number side-by-side
        {"name": "p2_ch_funnel", "row": 5, "column": 0, "colspan": 8, "rowspan": 8},
        {"name": "p2_n_not_started", "row": 5, "column": 8, "colspan": 4, "rowspan": 4},
        # Stackhbar: KYC by UnitGroup
        {"name": "p2_ch_kyc_unit", "row": 13, "column": 0, "colspan": 12, "rowspan": 8},
        # Detail section
        {"name": "p2_sec_detail", "row": 21, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_tbl_kyc", "row": 22, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    # ── Page 3: Customer Health ──
    p3 = nav_row("p3", 7) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar (4 pillbox filters)
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_industry", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_risk", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_kyc", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Top row: Donut + Numbers + Gauge
        {"name": "p3_ch_risk", "row": 5, "column": 0, "colspan": 4, "rowspan": 6},
        {"name": "p3_n_at_risk", "row": 5, "column": 4, "colspan": 2, "rowspan": 3},
        {"name": "p3_n_total", "row": 5, "column": 6, "colspan": 2, "rowspan": 3},
        {"name": "p3_g_churn", "row": 5, "column": 8, "colspan": 4, "rowspan": 6},
        # Risk by UnitGroup
        {"name": "p3_ch_risk_unit", "row": 11, "column": 0, "colspan": 6, "rowspan": 8},
        # Partner engagement
        {"name": "p3_ch_partner", "row": 11, "column": 6, "colspan": 6, "rowspan": 8},
        # Detail section
        {"name": "p3_sec_detail", "row": 19, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_tbl_risk", "row": 20, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    # ── Page 4: Cross-sell & Contact ──
    p4 = nav_row("p4", 7) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar (4 pillbox filters)
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_industry", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_risk", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_kyc", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Top row: Number + Gauge
        {"name": "p4_n_axioma", "row": 5, "column": 0, "colspan": 4, "rowspan": 4},
        {"name": "p4_g_axioma_pct", "row": 5, "column": 4, "colspan": 4, "rowspan": 4},
        # Waterfall: SaaS quarterly
        {"name": "p4_ch_saas", "row": 9, "column": 0, "colspan": 12, "rowspan": 8},
        # Contact Coverage section
        {"name": "p4_sec_contact", "row": 17, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_contacts", "row": 18, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p4_ch_level", "row": 18, "column": 6, "colspan": 6, "rowspan": 8},
        # Contact Metrics & NRR Proxy
        {"name": "p4_sec_metrics", "row": 26, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p4_g_contact_act",
            "row": 27,
            "column": 0,
            "colspan": 6,
            "rowspan": 4,
        },
        {"name": "p4_g_retention", "row": 27, "column": 6, "colspan": 6, "rowspan": 4},
        {
            "name": "p4_ch_contact_monthly",
            "row": 31,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Whitespace Cross-Sell Matrix
        {
            "name": "p4_sec_whitespace",
            "row": 39,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p4_ch_whitespace",
            "row": 40,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        {"name": "p4_sec_ws_opps", "row": 48, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_tbl_ws_opps", "row": 49, "column": 0, "colspan": 12, "rowspan": 8},
        {
            "name": "p4_sec_ws_summary",
            "row": 57,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p4_tbl_ws_summary",
            "row": 58,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
    ]

    # ── Page 5: Segment Analysis ──
    p5 = nav_row("p5", 7) + [
        {"name": "p5_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar (4 pillbox filters)
        {"name": "p5_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_industry", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_risk", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_kyc", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Row 1: Industry donut + Industry × Risk stackhbar
        {"name": "p5_ch_industry", "row": 5, "column": 0, "colspan": 5, "rowspan": 8},
        {"name": "p5_ch_ind_risk", "row": 5, "column": 5, "colspan": 7, "rowspan": 8},
        # Row 2: AuM table + Size band donut
        {"name": "p5_ch_aum", "row": 13, "column": 0, "colspan": 7, "rowspan": 8},
        {"name": "p5_ch_size", "row": 13, "column": 7, "colspan": 5, "rowspan": 8},
        # Row 3: Country hbar + Industry × KYC stackhbar
        {"name": "p5_ch_country", "row": 21, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p5_ch_ind_kyc", "row": 21, "column": 6, "colspan": 6, "rowspan": 8},
        # Revenue Concentration (Additive CRO)
        {
            "name": "p5_sec_concentration",
            "row": 29,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p5_ch_concentration",
            "row": 30,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # Iteration 3: Customer Vintage Cohort (Additive CRO #4)
        {"name": "p5_sec_vintage", "row": 40, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_vintage", "row": 41, "column": 0, "colspan": 6, "rowspan": 8},
        {
            "name": "p5_ch_vintage_aum",
            "row": 41,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        },
        {
            "name": "p5_ch_vintage_risk",
            "row": 49,
            "column": 0,
            "colspan": 6,
            "rowspan": 8,
        },
        {
            "name": "p5_ch_vintage_ret",
            "row": 49,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        },
    ]

    p6 = nav_row("p6", 7) + [
        {"name": "p6_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p6_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_industry", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_risk", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_kyc", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Treemap
        {"name": "p6_sec_treemap", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_treemap", "row": 6, "column": 0, "colspan": 12, "rowspan": 10},
        # Heatmap
        {"name": "p6_sec_heatmap", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_heatmap", "row": 17, "column": 0, "colspan": 12, "rowspan": 10},
        # Bubble
        {"name": "p6_sec_bubble", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_bubble", "row": 28, "column": 0, "colspan": 12, "rowspan": 10},
        # Sankey: KYC → Risk
        {"name": "p6_sec_sankey", "row": 38, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_sankey", "row": 39, "column": 0, "colspan": 12, "rowspan": 10},
        # Area: DQ Trend
        {"name": "p6_sec_area", "row": 49, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_area", "row": 50, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    p7 = nav_row("p7", 7) + [
        {"name": "p7_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p7_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_industry", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_risk", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p7_f_kyc", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Bullet
        {"name": "p7_sec_bullet", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_bullet_dq", "row": 6, "column": 0, "colspan": 12, "rowspan": 5},
        # DQ percentile table
        {"name": "p7_sec_dq_dist", "row": 11, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p7_stat_dq_dist",
            "row": 12,
            "column": 0,
            "colspan": 12,
            "rowspan": 5,
        },
        # Industry AuM table
        {
            "name": "p7_sec_industry",
            "row": 17,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p7_stat_industry",
            "row": 18,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Phase 9: Segment visualization
        {"name": "p7_sec_segment", "row": 26, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p7_segment_donut",
            "row": 27,
            "column": 0,
            "colspan": 5,
            "rowspan": 8,
        },
        {"name": "p7_segment_bar", "row": 27, "column": 5, "colspan": 7, "rowspan": 8},
        # Cumulative AuM
        {"name": "p7_sec_running", "row": 35, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_running", "row": 36, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("dataquality", "Data Quality", p1),
            pg("kyc", "KYC Pipeline", p2),
            pg("health", "Customer Health", p3),
            pg("crosssell", "Cross-sell & Contact", p4),
            pg("segments", "Segment Analysis", p5),
            pg("advanalytics", "Advanced Analytics", p6),
            pg("acctstats", "Statistical Analysis", p7),
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def create_dataflow_definition():
    """Return a CRM Analytics dataflow definition for Account_Intelligence + Contact_Coverage."""
    return {
        "Extract_Accounts": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Account",
                "fields": [
                    {"name": "Id"},
                    {"name": "Name"},
                    {"name": "OwnerId"},
                    {"name": "Type"},
                    {"name": "CreatedDate"},
                    {"name": "BillingCountry"},
                    {"name": "Industry"},
                    {"name": "Risk_of_Potential_Termination__c"},
                    {"name": "SaaS_Client__c"},
                    {"name": "Axioma_Client__c"},
                    {"name": "AuM_m__c"},
                    {"name": "NumberOfEmployees"},
                ],
            },
        },
        "Extract_Users": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "User",
                "fields": [{"name": "Id"}, {"name": "Name"}],
            },
        },
        "Augment_Acct_Owner": {
            "action": "augment",
            "parameters": {
                "left": "Extract_Accounts",
                "left_key": ["OwnerId"],
                "relationship": "Owner",
                "right": "Extract_Users",
                "right_key": ["Id"],
                "right_select": ["Name"],
            },
        },
        "Register_Accounts": {
            "action": "sfdcRegister",
            "parameters": {
                "source": "Augment_Acct_Owner",
                "name": "Account_Intelligence",
                "alias": "Account_Intelligence",
                "label": "Account Intelligence",
            },
        },
        "Extract_Contacts": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Contact",
                "fields": [
                    {"name": "Id"},
                    {"name": "AccountId"},
                    {"name": "CreatedDate"},
                    {"name": "Title"},
                    {"name": "Email"},
                    {"name": "HasOptedOutOfEmail"},
                ],
            },
        },
        "Extract_Accounts_Lookup": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Account",
                "fields": [{"name": "Id"}, {"name": "Name"}],
            },
        },
        "Augment_Contact_Account": {
            "action": "augment",
            "parameters": {
                "left": "Extract_Contacts",
                "left_key": ["AccountId"],
                "relationship": "Account",
                "right": "Extract_Accounts_Lookup",
                "right_key": ["Id"],
                "right_select": ["Name"],
            },
        },
        "Register_Contacts": {
            "action": "sfdcRegister",
            "parameters": {
                "source": "Augment_Contact_Account",
                "name": "Contact_Coverage",
                "alias": "Contact_Coverage",
                "label": "Contact Coverage",
            },
        },
    }


def main():
    instance_url, token = get_auth()

    if "--create-dataflow" in sys.argv:
        print("\n=== Creating/updating dataflow ===")
        df_def = create_dataflow_definition()
        df_id = create_dataflow(instance_url, token, "DF_Account_Intelligence", df_def)
        if df_id and "--run-dataflow" in sys.argv:
            run_dataflow(instance_url, token, df_id)
        return

    # ── 1. Build & upload Account Intelligence dataset ──
    acct_ok = create_account_dataset(instance_url, token)
    if not acct_ok:
        print("ERROR: Account dataset upload failed -- aborting")
        return

    # Set record navigation links via XMD
    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "AccountName", "sobject": "Account", "id_field": "Id"},
        ],
    )

    # ── 2. Build & upload Contact Coverage dataset ──
    contact_ok = create_contact_dataset(instance_url, token)
    if not contact_ok:
        print(
            "WARNING: Contact dataset upload failed -- page 4 contact charts may be empty"
        )

    # ── 3. Look up dataset IDs ──
    ds_id = get_dataset_id(instance_url, token, DS)
    if not ds_id:
        print(f"ERROR: Could not find dataset ID for {DS}")
        return
    print(f"  {DS} ID: {ds_id}")

    contact_ds_id = get_dataset_id(instance_url, token, CONTACT_DS)
    if contact_ds_id:
        print(f"  {CONTACT_DS} ID: {contact_ds_id}")
    else:
        print(f"WARNING: Could not find dataset ID for {CONTACT_DS}")

    ds_meta = [{"id": ds_id, "name": DS}]
    contact_ds_meta = (
        [{"id": contact_ds_id, "name": CONTACT_DS}] if contact_ds_id else []
    )

    # ── 4. Create or find dashboard ──
    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    print(f"  Dashboard ID: {dashboard_id}")

    # ── 5. Build & deploy ──
    steps = build_steps(ds_meta, contact_ds_meta)
    widgets = build_widgets()
    layout = build_layout()

    state = build_dashboard_state(steps, widgets, layout)
    deploy_dashboard(instance_url, token, dashboard_id, state)


if __name__ == "__main__":
    main()
