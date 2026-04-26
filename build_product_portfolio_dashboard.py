#!/usr/bin/env python3
"""Build the Product Portfolio & Whitespace dashboard.

This product suite builder now uses OpportunityLineItem + Product2 as the
commercial backbone so the dashboards reflect the actual sold, renewal, and
expansion mix instead of the lossy opportunity-header family field.
"""

from __future__ import annotations

import csv
import io
from collections import defaultdict

from crm_analytics_helpers import (
    _date,
    _dim,
    _measure,
    _soql,
    add_table_action,
    af,
    bubble_chart,
    build_dashboard_state,
    coalesce_filter,
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
    treemap_chart,
    upload_dataset,
)
from portfolio_foundation import fiscal_label, forecast_weight, normalize_motion, safe_float

DS = "Product_Portfolio_Whitespace"
DS_LABEL = "Product Portfolio Whitespace"
DASHBOARD_LABEL = "Product Portfolio & Whitespace"

ACCOUNT_SOQL = (
    "SELECT Id, Name, Owner.Name, Type, CreatedDate, BillingCountry, "
    "Industry, Unit_Group__c, SaaS_Client__c, Axioma_Client__c, "
    "Risk_of_Potential_Termination__c, Partner_Engagement_Level__c, "
    "AuM_m__c, NumberOfEmployees "
    "FROM Account "
    "WHERE CreatedDate >= 2020-01-01T00:00:00Z"
)

LINE_ITEM_SOQL = (
    "SELECT Id, OpportunityId, Opportunity.Name, Opportunity.AccountId, "
    "Opportunity.Account.Name, Opportunity.Account.Industry, "
    "Opportunity.Type, Opportunity.IsClosed, Opportunity.IsWon, "
    "Opportunity.CloseDate, Opportunity.FiscalYear, Opportunity.FiscalQuarter, "
    "Opportunity.ForecastCategoryName, Opportunity.Probability, "
    "Product2Id, Product2.Name, Product2.Family, Product2.Classification_Legal__c, "
    "Quantity, UnitPrice, "
    "APTS_ACV_1st_Year__c, APTS_ACV_2nd_Year__c, APTS_Forecast_ACV_AVG__c, "
    "APTS_ProductArea__c, APTS_Category_Level_1__c, APTS_Opportunity_Sub_Type__c "
    "FROM OpportunityLineItem "
    "WHERE Opportunity.FiscalYear IN (2025, 2026, 2027)"
)


def _normalize_industry(value: object) -> str:
    raw = (str(value or "")).strip()
    lowered = raw.lower()
    if not raw or lowered in {"select industry", "other", "computer software"}:
        return "Other"
    mapping = {
        "asset manager": "Asset Management",
        "asset management": "Asset Management",
        "bank": "Bank",
        "central bank": "Central Bank",
        "insurance": "Insurance",
        "pension": "Pension",
        "pension fund": "Pension",
        "asset owner": "Asset Owner",
        "wealth management": "Wealth Management",
        "asset servicer": "Asset Servicer",
        "sovereign wealth fund": "Sovereign Wealth Fund",
        "hedge fund": "Hedge Fund",
        "fund": "Fund",
    }
    if lowered in mapping:
        return mapping[lowered]
    if "financial services" in lowered and "investment management" in lowered:
        return "Asset Management"
    if "service provider" in lowered:
        return "Service Provider"
    return raw[:255]


def _industry_vertical(industry: str) -> str:
    if industry in {"Asset Management", "Fund", "Hedge Fund"}:
        return "Asset Management"
    if industry in {"Bank", "Central Bank"}:
        return "Banking"
    if industry == "Insurance":
        return "Insurance"
    if industry in {"Pension", "Asset Owner", "Sovereign Wealth Fund"}:
        return "Asset Owner / Pension"
    if industry == "Wealth Management":
        return "Wealth Management"
    if industry == "Asset Servicer":
        return "Asset Servicing"
    if industry == "Service Provider":
        return "Service Provider"
    return "Other"


def _delivery_model(is_saas: str) -> str:
    return "SaaS" if is_saas == "true" else "Non-SaaS / Unknown"


def _portfolio_cluster(product_family: str) -> str:
    lowered = product_family.lower()
    if "operational services" in lowered or "data management" in lowered:
        return "Managed Business Services"
    if "consulting" in lowered or "client driven development" in lowered:
        return "Services"
    if "xaas" in lowered or "saas" in lowered:
        return "SaaS / Cloud"
    if "client communications" in lowered:
        return "Client Communications"
    if "software" in lowered:
        return "SimCorp Core Software"
    if "3rd party" in lowered or "white label" in lowered or "regulatory" in lowered:
        return "Adjacencies / Other"
    return "Adjacencies / Other"


def _segment(aum: float, arr: float) -> str:
    if aum > 100000 or arr > 500000:
        return "Enterprise"
    if aum > 10000 or arr > 100000:
        return "Mid-Market"
    if arr > 0:
        return "Growth"
    return "Prospect"


def _product_count_band(count: int) -> str:
    if count <= 0:
        return "0 Products"
    if count == 1:
        return "1 Product"
    if count == 2:
        return "2 Products"
    if count == 3:
        return "3 Products"
    return "4+ Products"


def _whitespace_score(
    installed_arr: float,
    product_count: int,
    is_saas: str,
    is_axioma: str,
    open_expansion_arr: float,
) -> float:
    score = 0.0
    if installed_arr >= 500000:
        score += 30.0
    elif installed_arr >= 100000:
        score += 20.0
    elif installed_arr > 0:
        score += 10.0

    if product_count <= 0:
        score += 25.0
    elif product_count == 1:
        score += 20.0
    elif product_count == 2:
        score += 10.0

    if is_saas == "true" and is_axioma == "false":
        score += 15.0
    if is_axioma == "true" and is_saas == "false":
        score += 15.0

    if open_expansion_arr > 0:
        score += 10.0

    return round(min(100.0, score), 1)


def _expansion_score(
    installed_arr: float,
    open_expansion_arr: float,
    product_count: int,
    is_saas: str,
    is_axioma: str,
) -> float:
    score = 0.0
    if open_expansion_arr >= 250000:
        score += 35.0
    elif open_expansion_arr >= 100000:
        score += 25.0
    elif open_expansion_arr > 0:
        score += 15.0

    if installed_arr >= 500000:
        score += 20.0
    elif installed_arr >= 100000:
        score += 10.0

    if product_count <= 1:
        score += 20.0
    elif product_count == 2:
        score += 10.0

    if is_saas == "true" or is_axioma == "true":
        score += 10.0

    return round(min(100.0, score), 1)


def _mapped_product(value: object) -> str:
    raw = ((value or "") if isinstance(value, str) else str(value or "")).split(";")[0].strip()
    return raw[:255] if raw else "Unmapped"


def _mapped_text(value: object, default: str = "Unknown") -> str:
    raw = ((value or "") if isinstance(value, str) else str(value or "")).strip()
    return raw[:255] if raw else default


def _row_value(row: dict[str, object], *path: str) -> object:
    current: object = row
    for part in path:
        if not isinstance(current, dict):
            return ""
        current = current.get(part)
    return current if current is not None else ""


def _commercial_value(line_item: dict[str, object]) -> float:
    acv_year_1 = safe_float(line_item.get("APTS_ACV_1st_Year__c"))
    if acv_year_1 > 0:
        return round(acv_year_1, 2)
    forecast_acv = safe_float(line_item.get("APTS_Forecast_ACV_AVG__c"))
    if forecast_acv > 0:
        return round(forecast_acv, 2)
    return 0.0


def create_dataset(inst: str, tok: str) -> bool:
    """Build the product portfolio dataset."""
    print(f"\n=== Building {DS_LABEL} dataset ===")
    accounts = _soql(inst, tok, ACCOUNT_SOQL)
    line_items = _soql(inst, tok, LINE_ITEM_SOQL)
    print(f"  Queried {len(accounts)} accounts")
    print(f"  Queried {len(line_items)} opportunity line items")
    if not accounts or not line_items:
        raise RuntimeError("Product dashboard requires both account and opportunity line-item data.")

    acct_rollups: dict[str, dict[str, object]] = {}
    for line_item in line_items:
        account_id = _row_value(line_item, "Opportunity", "AccountId")
        if not account_id:
            continue

        bucket = acct_rollups.setdefault(
            account_id,
            {
                "InstalledARR": 0.0,
                "OpenARR": 0.0,
                "OpenExpansionARR": 0.0,
                "OpenRenewalARR": 0.0,
                "WeightedOpenARR": 0.0,
                "WonProducts": set(),
                "OpenProducts": set(),
                "ProductTotals": defaultdict(float),
                "ClassificationTotals": defaultdict(float),
                "FamilyBuckets": defaultdict(
                    lambda: {
                        "FamilyInstalledARR": 0.0,
                        "FamilyOpenARR": 0.0,
                        "FamilyOpenExpansionARR": 0.0,
                        "FamilyOpenRenewalARR": 0.0,
                        "FamilyWeightedOpenARR": 0.0,
                        "FamilyWonARR": 0.0,
                        "FamilyProjectedARR": 0.0,
                        "ProductClassificationTotals": defaultdict(float),
                        "ProductAreaTotals": defaultdict(float),
                        "CategoryLevelTotals": defaultdict(float),
                        "OpportunitySubTypeTotals": defaultdict(float),
                    }
                ),
            },
        )

        product = _mapped_product(_row_value(line_item, "Product2", "Family"))
        classification = _mapped_text(_row_value(line_item, "Product2", "Classification_Legal__c"))
        product_area = _mapped_text(line_item.get("APTS_ProductArea__c"))
        category_level = _mapped_text(line_item.get("APTS_Category_Level_1__c"), product)
        opp_sub_type = _mapped_text(line_item.get("APTS_Opportunity_Sub_Type__c"))
        arr = _commercial_value(line_item)
        probability = safe_float(_row_value(line_item, "Opportunity", "Probability"))
        forecast_category = str(_row_value(line_item, "Opportunity", "ForecastCategoryName") or "")
        weight = forecast_weight(forecast_category, probability)
        motion = normalize_motion(_row_value(line_item, "Opportunity", "Type") or "")
        is_closed = str(_row_value(line_item, "Opportunity", "IsClosed") or "").lower() == "true"
        is_won = str(_row_value(line_item, "Opportunity", "IsWon") or "").lower() == "true"

        bucket["ProductTotals"][product] += arr
        bucket["ClassificationTotals"][classification] += arr
        family_bucket = bucket["FamilyBuckets"][product]
        family_bucket["ProductClassificationTotals"][classification] += arr
        family_bucket["ProductAreaTotals"][product_area] += arr
        family_bucket["CategoryLevelTotals"][category_level] += arr
        family_bucket["OpportunitySubTypeTotals"][opp_sub_type] += arr

        if is_won:
            bucket["InstalledARR"] = round(safe_float(bucket["InstalledARR"]) + arr, 2)
            cast_products = bucket["WonProducts"]
            if isinstance(cast_products, set):
                cast_products.add(product)
            family_bucket["FamilyInstalledARR"] = round(
                safe_float(family_bucket["FamilyInstalledARR"]) + arr, 2
            )
            family_bucket["FamilyWonARR"] = round(
                safe_float(family_bucket["FamilyWonARR"]) + arr, 2
            )
            family_bucket["FamilyProjectedARR"] = round(
                safe_float(family_bucket["FamilyProjectedARR"]) + arr, 2
            )
        elif not is_closed:
            bucket["OpenARR"] = round(safe_float(bucket["OpenARR"]) + arr, 2)
            bucket["WeightedOpenARR"] = round(safe_float(bucket["WeightedOpenARR"]) + arr * weight, 2)
            cast_products = bucket["OpenProducts"]
            if isinstance(cast_products, set):
                cast_products.add(product)
            family_bucket["FamilyOpenARR"] = round(
                safe_float(family_bucket["FamilyOpenARR"]) + arr, 2
            )
            family_bucket["FamilyWeightedOpenARR"] = round(
                safe_float(family_bucket["FamilyWeightedOpenARR"]) + arr * weight, 2
            )
            family_bucket["FamilyProjectedARR"] = round(
                safe_float(family_bucket["FamilyProjectedARR"]) + arr * weight, 2
            )
            if motion == "Expand":
                bucket["OpenExpansionARR"] = round(safe_float(bucket["OpenExpansionARR"]) + arr, 2)
                family_bucket["FamilyOpenExpansionARR"] = round(
                    safe_float(family_bucket["FamilyOpenExpansionARR"]) + arr, 2
                )
            if motion == "Renewal":
                bucket["OpenRenewalARR"] = round(safe_float(bucket["OpenRenewalARR"]) + arr, 2)
                family_bucket["FamilyOpenRenewalARR"] = round(
                    safe_float(family_bucket["FamilyOpenRenewalARR"]) + arr, 2
                )

    detail_rows: list[dict[str, object]] = []
    account_lookup: dict[str, dict[str, object]] = {}
    for account in accounts:
        account_id = account.get("Id")
        if not account_id:
            continue

        opp_metrics = acct_rollups.get(account_id, {})
        installed_arr = round(safe_float(opp_metrics.get("InstalledARR")), 2)
        open_arr = round(safe_float(opp_metrics.get("OpenARR")), 2)
        if installed_arr <= 0 and open_arr <= 0 and not account.get("SaaS_Client__c") and not account.get("Axioma_Client__c"):
            continue

        won_products = set(opp_metrics.get("WonProducts") or set())
        open_products = set(opp_metrics.get("OpenProducts") or set())
        coverage_products = set(won_products)
        if not coverage_products and account.get("SaaS_Client__c"):
            coverage_products.add("SimCorp SaaS Flag")
        if not coverage_products and account.get("Axioma_Client__c"):
            coverage_products.add("Axioma Flag")
        observed_products = coverage_products | open_products
        product_count = len(coverage_products)
        product_combo = "|".join(sorted(coverage_products)) if len(coverage_products) >= 2 else ""
        product_totals = opp_metrics.get("ProductTotals") or {}
        primary_product = "Unmapped"
        if isinstance(product_totals, dict) and product_totals:
            primary_product = max(product_totals.items(), key=lambda item: safe_float(item[1]))[0]
        classification_totals = opp_metrics.get("ClassificationTotals") or {}
        primary_classification = "Unknown"
        if isinstance(classification_totals, dict) and classification_totals:
            primary_classification = max(
                classification_totals.items(), key=lambda item: safe_float(item[1])
            )[0]

        owner = account.get("Owner") or {}
        unit_group = ((account.get("Unit_Group__c") or "Unassigned").strip() or "Unassigned")[:255]
        account_type = ((account.get("Type") or "Unknown").strip() or "Unknown")[:255]
        industry = ((account.get("Industry") or "Unknown").strip() or "Unknown")[:255]
        industry_normalized = _normalize_industry(industry)
        industry_vertical = _industry_vertical(industry_normalized)
        billing_country = ((account.get("BillingCountry") or "Unknown").strip() or "Unknown")[:255]
        risk_level = ((account.get("Risk_of_Potential_Termination__c") or "Low").strip() or "Low")[:255]
        partner_level = ((account.get("Partner_Engagement_Level__c") or "None").strip() or "None")[:255]
        is_saas = str(bool(account.get("SaaS_Client__c"))).lower()
        is_axioma = str(bool(account.get("Axioma_Client__c"))).lower()
        delivery_model = _delivery_model(is_saas)
        open_expansion_arr = round(safe_float(opp_metrics.get("OpenExpansionARR")), 2)
        whitespace_score = _whitespace_score(installed_arr, product_count, is_saas, is_axioma, open_expansion_arr)
        expansion_score = _expansion_score(installed_arr, open_expansion_arr, product_count, is_saas, is_axioma)
        whitespace_arr = installed_arr if product_count <= 1 else 0.0
        aum = round(safe_float(account.get("AuM_m__c")), 2)
        portfolio_cluster = _portfolio_cluster(primary_product)

        row = {
            "RecordType": "account_detail",
            "Id": account_id,
            "AccountId": account_id,
            "AccountName": (account.get("Name") or "")[:255],
            "OwnerName": ((owner.get("Name") or "Unknown")[:255]),
            "UnitGroup": unit_group,
            "AccountType": account_type,
            "Segment": _segment(aum, installed_arr),
            "Industry": industry,
            "IndustryNormalized": industry_normalized,
            "IndustryVertical": industry_vertical,
            "BillingCountry": billing_country,
            "RiskLevel": risk_level,
            "PartnerLevel": partner_level,
            "ProductFamily": primary_product[:255],
            "ProductClassification": primary_classification[:255],
            "ProductArea": "",
            "CategoryLevel1": "",
            "OpportunitySubType": "",
            "PortfolioCluster": portfolio_cluster[:255],
            "ProductCombo": product_combo[:255],
            "ProductCountBand": _product_count_band(product_count)[:255],
            "MotionType": "",
            "ForecastCategory": "",
            "FYLabel": "",
            "CloseQuarter": "",
            "MonthDate": "",
            "MonthLabel": "",
            "DeliveryModel": delivery_model,
            "IsSaaS": is_saas,
            "IsAxioma": is_axioma,
            "ProductCount": product_count,
            "ObservedProductCount": len(observed_products),
            "InstalledARR": installed_arr,
            "FamilyInstalledARR": installed_arr,
            "SaaSInstalledARR": installed_arr if is_saas == "true" else 0.0,
            "NonSaaSInstalledARR": installed_arr if is_saas != "true" else 0.0,
            "OpenARR": open_arr,
            "FamilyOpenARR": open_arr,
            "OpenExpansionARR": open_expansion_arr,
            "FamilyOpenExpansionARR": open_expansion_arr,
            "OpenRenewalARR": round(safe_float(opp_metrics.get("OpenRenewalARR")), 2),
            "FamilyOpenRenewalARR": round(safe_float(opp_metrics.get("OpenRenewalARR")), 2),
            "WeightedOpenARR": round(safe_float(opp_metrics.get("WeightedOpenARR")), 2),
            "FamilyWeightedOpenARR": round(safe_float(opp_metrics.get("WeightedOpenARR")), 2),
            "ProjectedARR": round(installed_arr + safe_float(opp_metrics.get("WeightedOpenARR")), 2),
            "FamilyProjectedARR": round(installed_arr + safe_float(opp_metrics.get("WeightedOpenARR")), 2),
            "WonARR": installed_arr,
            "FamilyWonARR": installed_arr,
            "RiskExposureARR": installed_arr if risk_level in {"High", "Critical"} else 0.0,
            "WhitespaceARR": round(whitespace_arr, 2),
            "WhitespaceScore": whitespace_score,
            "ExpansionScore": expansion_score,
            "AccountCount": 1,
            "OpportunityCount": 0,
        }
        detail_rows.append(row)
        account_lookup[account_id] = row

    account_product_rows: list[dict[str, object]] = []
    for account_id, detail in account_lookup.items():
        family_buckets = acct_rollups.get(account_id, {}).get("FamilyBuckets") or {}
        if not isinstance(family_buckets, dict):
            continue
        if not family_buckets:
            account_product_rows.append({**detail, "RecordType": "account_product"})
            continue

        for product_family, family_bucket in family_buckets.items():
            classification_totals = family_bucket.get("ProductClassificationTotals") or {}
            product_area_totals = family_bucket.get("ProductAreaTotals") or {}
            category_totals = family_bucket.get("CategoryLevelTotals") or {}
            opp_sub_type_totals = family_bucket.get("OpportunitySubTypeTotals") or {}
            account_product_rows.append(
                {
                    **detail,
                    "RecordType": "account_product",
                    "Id": f"{account_id}-{product_family[:120]}",
                    "ProductFamily": product_family[:255],
                    "ProductClassification": (
                        max(classification_totals.items(), key=lambda item: safe_float(item[1]))[0]
                        if classification_totals
                        else detail["ProductClassification"]
                    )[:255],
                    "ProductArea": (
                        max(product_area_totals.items(), key=lambda item: safe_float(item[1]))[0]
                        if product_area_totals
                        else ""
                    )[:255],
                    "CategoryLevel1": (
                        max(category_totals.items(), key=lambda item: safe_float(item[1]))[0]
                        if category_totals
                        else ""
                    )[:255],
                    "OpportunitySubType": (
                        max(opp_sub_type_totals.items(), key=lambda item: safe_float(item[1]))[0]
                        if opp_sub_type_totals
                        else ""
                    )[:255],
                    "FamilyInstalledARR": round(safe_float(family_bucket.get("FamilyInstalledARR")), 2),
                    "FamilyOpenARR": round(safe_float(family_bucket.get("FamilyOpenARR")), 2),
                    "FamilyOpenExpansionARR": round(
                        safe_float(family_bucket.get("FamilyOpenExpansionARR")), 2
                    ),
                    "FamilyOpenRenewalARR": round(
                        safe_float(family_bucket.get("FamilyOpenRenewalARR")), 2
                    ),
                    "FamilyWeightedOpenARR": round(
                        safe_float(family_bucket.get("FamilyWeightedOpenARR")), 2
                    ),
                    "FamilyProjectedARR": round(
                        safe_float(family_bucket.get("FamilyProjectedARR")), 2
                    ),
                    "FamilyWonARR": round(safe_float(family_bucket.get("FamilyWonARR")), 2),
                }
            )

    product_rows: list[dict[str, object]] = []
    for line_item in line_items:
        account_id = _row_value(line_item, "Opportunity", "AccountId")
        detail = account_lookup.get(account_id)
        if not detail:
            continue

        product = _mapped_product(_row_value(line_item, "Product2", "Family"))
        classification = _mapped_text(_row_value(line_item, "Product2", "Classification_Legal__c"))
        product_area = _mapped_text(line_item.get("APTS_ProductArea__c"))
        category_level = _mapped_text(line_item.get("APTS_Category_Level_1__c"), product)
        opp_sub_type = _mapped_text(line_item.get("APTS_Opportunity_Sub_Type__c"))
        arr = _commercial_value(line_item)
        probability = safe_float(_row_value(line_item, "Opportunity", "Probability"))
        forecast_category = str(_row_value(line_item, "Opportunity", "ForecastCategoryName") or "Pipeline")
        weight = forecast_weight(forecast_category, probability)
        motion = normalize_motion(_row_value(line_item, "Opportunity", "Type") or "")
        is_closed = str(_row_value(line_item, "Opportunity", "IsClosed") or "").lower() == "true"
        is_won = str(_row_value(line_item, "Opportunity", "IsWon") or "").lower() == "true"
        close_date = str(_row_value(line_item, "Opportunity", "CloseDate") or "")[:10]
        month_label = close_date[:7] if close_date else ""
        fiscal_year = int(safe_float(_row_value(line_item, "Opportunity", "FiscalYear")))
        fiscal_quarter = int(safe_float(_row_value(line_item, "Opportunity", "FiscalQuarter")))
        weighted_open_arr = round(arr * weight, 2) if not is_closed else 0.0
        portfolio_cluster = _portfolio_cluster(product)

        product_rows.append(
            {
                "RecordType": "opportunity_product",
                "Id": line_item.get("Id", ""),
                "AccountId": account_id,
                "AccountName": detail["AccountName"],
                "OwnerName": detail["OwnerName"],
                "UnitGroup": detail["UnitGroup"],
                "AccountType": detail["AccountType"],
                "Segment": detail["Segment"],
                "Industry": detail["Industry"],
                "IndustryNormalized": detail["IndustryNormalized"],
                "IndustryVertical": detail["IndustryVertical"],
                "BillingCountry": detail["BillingCountry"],
                "RiskLevel": detail["RiskLevel"],
                "PartnerLevel": detail["PartnerLevel"],
                "ProductFamily": product,
                "ProductClassification": classification[:255],
                "ProductArea": product_area[:255],
                "CategoryLevel1": category_level[:255],
                "OpportunitySubType": opp_sub_type[:255],
                "PortfolioCluster": portfolio_cluster,
                "ProductCombo": detail["ProductCombo"],
                "ProductCountBand": detail["ProductCountBand"],
                "MotionType": motion,
                "ForecastCategory": forecast_category[:255],
                "FYLabel": fiscal_label(fiscal_year),
                "CloseQuarter": f"Q{fiscal_quarter}" if fiscal_quarter else "",
                "MonthDate": f"{month_label}-01" if month_label else "",
                "MonthLabel": month_label,
                "DeliveryModel": detail["DeliveryModel"],
                "IsSaaS": detail["IsSaaS"],
                "IsAxioma": detail["IsAxioma"],
                "ProductCount": detail["ProductCount"],
                "ObservedProductCount": detail["ObservedProductCount"],
                "InstalledARR": detail["InstalledARR"],
                "FamilyInstalledARR": arr if is_won else 0.0,
                "SaaSInstalledARR": detail["SaaSInstalledARR"],
                "NonSaaSInstalledARR": detail["NonSaaSInstalledARR"],
                "OpenARR": arr if not is_closed else 0.0,
                "FamilyOpenARR": arr if not is_closed else 0.0,
                "OpenExpansionARR": arr if (not is_closed and motion == "Expand") else 0.0,
                "FamilyOpenExpansionARR": arr if (not is_closed and motion == "Expand") else 0.0,
                "OpenRenewalARR": arr if (not is_closed and motion == "Renewal") else 0.0,
                "FamilyOpenRenewalARR": arr if (not is_closed and motion == "Renewal") else 0.0,
                "WeightedOpenARR": weighted_open_arr,
                "FamilyWeightedOpenARR": weighted_open_arr,
                "ProjectedARR": (arr if is_won else 0.0) + weighted_open_arr,
                "FamilyProjectedARR": (arr if is_won else 0.0) + weighted_open_arr,
                "WonARR": arr if is_won else 0.0,
                "FamilyWonARR": arr if is_won else 0.0,
                "RiskExposureARR": weighted_open_arr if detail["RiskLevel"] in {"High", "Critical"} else 0.0,
                "WhitespaceARR": detail["WhitespaceARR"],
                "WhitespaceScore": detail["WhitespaceScore"],
                "ExpansionScore": detail["ExpansionScore"],
                "AccountCount": 0,
                "OpportunityCount": 1,
            }
        )

    rows = detail_rows + account_product_rows + product_rows
    print(f"  Account detail rows: {len(detail_rows)}")
    print(f"  Account product rows: {len(account_product_rows)}")
    print(f"  Product opportunity rows: {len(product_rows)}")
    print(f"  Total rows: {len(rows)}")

    field_names = [
        "RecordType",
        "Id",
        "AccountId",
        "AccountName",
        "OwnerName",
        "UnitGroup",
        "AccountType",
        "Segment",
        "Industry",
        "IndustryNormalized",
        "IndustryVertical",
        "BillingCountry",
        "RiskLevel",
        "PartnerLevel",
        "ProductFamily",
        "ProductClassification",
        "ProductArea",
        "CategoryLevel1",
        "OpportunitySubType",
        "PortfolioCluster",
        "ProductCombo",
        "ProductCountBand",
        "MotionType",
        "ForecastCategory",
        "FYLabel",
        "CloseQuarter",
        "MonthDate",
        "MonthLabel",
        "DeliveryModel",
        "IsSaaS",
        "IsAxioma",
        "ProductCount",
        "ObservedProductCount",
        "InstalledARR",
        "FamilyInstalledARR",
        "SaaSInstalledARR",
        "NonSaaSInstalledARR",
        "OpenARR",
        "FamilyOpenARR",
        "OpenExpansionARR",
        "FamilyOpenExpansionARR",
        "OpenRenewalARR",
        "FamilyOpenRenewalARR",
        "WeightedOpenARR",
        "FamilyWeightedOpenARR",
        "ProjectedARR",
        "FamilyProjectedARR",
        "WonARR",
        "FamilyWonARR",
        "RiskExposureARR",
        "WhitespaceARR",
        "WhitespaceScore",
        "ExpansionScore",
        "AccountCount",
        "OpportunityCount",
    ]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buffer.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes")

    fields_meta = [
        _dim("RecordType", "Record Type"),
        _dim("Id", "Id"),
        _dim("AccountId", "Account Id"),
        _dim("AccountName", "Account"),
        _dim("OwnerName", "Owner"),
        _dim("UnitGroup", "Unit Group"),
        _dim("AccountType", "Account Type"),
        _dim("Segment", "Segment"),
        _dim("Industry", "Industry"),
        _dim("IndustryNormalized", "Industry Normalized"),
        _dim("IndustryVertical", "Industry Vertical"),
        _dim("BillingCountry", "Billing Country"),
        _dim("RiskLevel", "Risk Level"),
        _dim("PartnerLevel", "Partner Level"),
        _dim("ProductFamily", "Product Family"),
        _dim("ProductClassification", "Product Classification"),
        _dim("ProductArea", "Product Area"),
        _dim("CategoryLevel1", "Category Level 1"),
        _dim("OpportunitySubType", "Opportunity Sub Type"),
        _dim("PortfolioCluster", "Portfolio Cluster"),
        _dim("ProductCombo", "Product Combination"),
        _dim("ProductCountBand", "Product Count Band"),
        _dim("MotionType", "Motion Type"),
        _dim("ForecastCategory", "Forecast Category"),
        _dim("FYLabel", "Fiscal Year"),
        _dim("CloseQuarter", "Close Quarter"),
        _date("MonthDate", "Month"),
        _dim("MonthLabel", "Month Label"),
        _dim("DeliveryModel", "Delivery Model"),
        _dim("IsSaaS", "SaaS Client"),
        _dim("IsAxioma", "Axioma Client"),
        _measure("ProductCount", "Installed Product Count", scale=0, precision=6),
        _measure("ObservedProductCount", "Observed Product Count", scale=0, precision=6),
        _measure("InstalledARR", "Installed ARR", scale=2, precision=18),
        _measure("FamilyInstalledARR", "Family Installed ARR", scale=2, precision=18),
        _measure("SaaSInstalledARR", "SaaS Installed ARR", scale=2, precision=18),
        _measure("NonSaaSInstalledARR", "Non-SaaS Installed ARR", scale=2, precision=18),
        _measure("OpenARR", "Open ARR", scale=2, precision=18),
        _measure("FamilyOpenARR", "Family Open ARR", scale=2, precision=18),
        _measure("OpenExpansionARR", "Open Expansion ARR", scale=2, precision=18),
        _measure("FamilyOpenExpansionARR", "Family Open Expansion ARR", scale=2, precision=18),
        _measure("OpenRenewalARR", "Open Renewal ARR", scale=2, precision=18),
        _measure("FamilyOpenRenewalARR", "Family Open Renewal ARR", scale=2, precision=18),
        _measure("WeightedOpenARR", "Weighted Open ARR", scale=2, precision=18),
        _measure("FamilyWeightedOpenARR", "Family Weighted Open ARR", scale=2, precision=18),
        _measure("ProjectedARR", "Projected ARR", scale=2, precision=18),
        _measure("FamilyProjectedARR", "Family Projected ARR", scale=2, precision=18),
        _measure("WonARR", "Won ARR", scale=2, precision=18),
        _measure("FamilyWonARR", "Family Won ARR", scale=2, precision=18),
        _measure("RiskExposureARR", "Risk Exposure ARR", scale=2, precision=18),
        _measure("WhitespaceARR", "Whitespace ARR", scale=2, precision=18),
        _measure("WhitespaceScore", "Whitespace Score", scale=1, precision=6),
        _measure("ExpansionScore", "Expansion Score", scale=1, precision=6),
        _measure("AccountCount", "Account Count", scale=0, precision=6),
        _measure("OpportunityCount", "Opportunity Count", scale=0, precision=6),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


def build_steps(ds_id: str) -> dict[str, dict]:
    """Build dashboard steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_type = coalesce_filter("f_account_type", "AccountType")
    filter_segment = coalesce_filter("f_segment", "Segment")
    filter_product = coalesce_filter("f_product", "ProductFamily")

    account_product = (
        load
        + 'q = filter q by RecordType == "account_product";\n'
        + filter_unit
        + filter_type
        + filter_segment
        + filter_product
    )
    opp = (
        load
        + 'q = filter q by RecordType == "opportunity_product";\n'
        + filter_unit
        + filter_type
        + filter_segment
        + filter_product
    )

    return {
        "f_unit": af("UnitGroup", ds_meta),
        "f_account_type": af("AccountType", ds_meta),
        "f_segment": af("Segment", ds_meta),
        "f_product": af("ProductFamily", ds_meta),
        "s_summary": sq(
            account_product
            + "q = group q by AccountId;\n"
            + "q = foreach q generate "
            + "max(InstalledARR) as InstalledARR, "
            + "max(OpenExpansionARR) as OpenExpansionARR, "
            + "max(WhitespaceARR) as WhitespaceARR, "
            + "max(ProductCount) as ProductCount;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(InstalledARR) as installed_arr, "
            + "sum(OpenExpansionARR) as expansion_pipe_arr, "
            + "sum(WhitespaceARR) as whitespace_arr, "
            + 'sum(case when ProductCount >= 2 then 1 else 0 end) as multi_product_accounts;'
        ),
        "s_product_mix": sq(
            opp
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, "
            + "sum(WonARR) as WonARR, "
            + "sum(WeightedOpenARR) as WeightedOpenARR;\n"
            + "q = order q by WonARR desc;\n"
            + "q = limit q 15;"
        ),
        "s_commercial_mix": sq(
            opp
            + "q = group q by (MotionType, ProductFamily);\n"
            + "q = foreach q generate MotionType, ProductFamily, sum(ProjectedARR) as ProjectedARR;\n"
            + "q = order q by MotionType asc;"
        ),
        "s_client_matrix": sq(
            account_product
            + "q = filter q by FamilyInstalledARR > 0;\n"
            + "q = foreach q generate AccountType, ProductFamily, AccountId;\n"
            + "q = group q by (AccountType, ProductFamily);\n"
            + "q = foreach q generate AccountType, ProductFamily, count() as AccountCount;\n"
            + "q = order q by AccountCount desc;"
        ),
        "s_attach_matrix": sq(
            account_product
            + "q = filter q by FamilyInstalledARR > 0;\n"
            + "q = foreach q generate UnitGroup, ProductFamily, AccountId;\n"
            + "q = group q by (UnitGroup, ProductFamily);\n"
            + "q = foreach q generate UnitGroup, ProductFamily, count() as AccountCount;\n"
            + "q = order q by AccountCount desc;"
        ),
        "s_product_count_dist": sq(
            account_product
            + "q = group q by (AccountId, ProductCountBand);\n"
            + "q = foreach q generate AccountId, ProductCountBand, max(InstalledARR) as InstalledARR;\n"
            + "q = group q by ProductCountBand;\n"
            + "q = foreach q generate ProductCountBand, count() as AccountCount, sum(InstalledARR) as InstalledARR;\n"
            + "q = order q by ProductCountBand asc;"
        ),
        "s_whitespace_accounts": sq(
            account_product
            + "q = filter q by ProductCount <= 1;\n"
            + "q = filter q by InstalledARR > 0;\n"
            + "q = group q by (AccountName, OwnerName, UnitGroup, Segment, AccountId);\n"
            + "q = foreach q generate AccountName, OwnerName, UnitGroup, Segment, "
            + "max(InstalledARR) as InstalledARR, "
            + "max(OpenExpansionARR) as OpenExpansionARR, "
            + "max(ProductCount) as ProductCount, "
            + "max(WhitespaceScore) as WhitespaceScore, "
            + "AccountId;\n"
            + "q = order q by WhitespaceScore desc;\n"
            + "q = limit q 20;"
        ),
        "s_product_combos": sq(
            account_product
            + 'q = filter q by ProductCombo != "";\n'
            + "q = group q by (AccountId, ProductCombo);\n"
            + "q = foreach q generate AccountId, ProductCombo, max(InstalledARR) as InstalledARR;\n"
            + "q = group q by ProductCombo;\n"
            + "q = foreach q generate ProductCombo, count() as AccountCount, sum(InstalledARR) as InstalledARR;\n"
            + "q = order q by InstalledARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_expansion_scatter": sq(
            account_product
            + "q = filter q by (ExpansionScore > 0) or (OpenExpansionARR > 0);\n"
            + "q = group q by (AccountName, Segment, AccountType, AccountId);\n"
            + "q = foreach q generate "
            + "max(ProductCount) as ProductCount, "
            + "max(ExpansionScore) as ExpansionScore, "
            + "max(OpenExpansionARR) as OpenExpansionARR, "
            + "max(InstalledARR) as InstalledARR, "
            + "AccountName, Segment, AccountType, AccountId;\n"
            + "q = order q by OpenExpansionARR desc;\n"
            + "q = limit q 25;"
        ),
        "s_expand_by_product": sq(
            opp
            + 'q = filter q by MotionType == "Expand";\n'
            + "q = filter q by OpenARR > 0;\n"
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, sum(OpenARR) as OpenARR, sum(WeightedOpenARR) as WeightedOpenARR;\n"
            + "q = order q by OpenARR desc;"
        ),
        "s_motion_product_flow": sq(
            opp
            + "q = group q by (MotionType, ProductFamily);\n"
            + "q = foreach q generate MotionType as source, ProductFamily as target, sum(ProjectedARR) as flow;\n"
            + "q = order q by flow desc;\n"
            + "q = limit q 24;"
        ),
        "s_top_expansion": sq(
            account_product
            + "q = filter q by (ExpansionScore >= 25) or (OpenExpansionARR > 0);\n"
            + "q = group q by (AccountName, OwnerName, UnitGroup, Segment, AccountId);\n"
            + "q = foreach q generate AccountName, OwnerName, UnitGroup, Segment, "
            + "max(InstalledARR) as InstalledARR, "
            + "max(OpenExpansionARR) as OpenExpansionARR, "
            + "max(ExpansionScore) as ExpansionScore, "
            + "max(ProductCount) as ProductCount, "
            + "AccountId;\n"
            + "q = order q by ExpansionScore desc;\n"
            + "q = limit q 20;"
        ),
        "s_risk_by_product": sq(
            opp
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, "
            + "sum(RiskExposureARR) as RiskExposureARR, "
            + "sum(ProjectedARR) as ProjectedARR;\n"
            + "q = order q by RiskExposureARR desc;"
        ),
        "s_growth_risk_scatter": sq(
            account_product
            + "q = filter q by InstalledARR > 0;\n"
            + "q = group q by (AccountName, Segment, AccountType, AccountId);\n"
            + "q = foreach q generate "
            + "max(WhitespaceScore) as WhitespaceScore, "
            + "max(ExpansionScore) as ExpansionScore, "
            + "max(InstalledARR) as InstalledARR, "
            + "AccountName, Segment, AccountType, AccountId;\n"
            + "q = order q by InstalledARR desc;\n"
            + "q = limit q 25;"
        ),
        "s_low_coverage": sq(
            account_product
            + "q = filter q by ProductCount <= 1;\n"
            + "q = filter q by InstalledARR > 0;\n"
            + "q = group q by (AccountName, OwnerName, UnitGroup, AccountType, AccountId);\n"
            + "q = foreach q generate AccountName, OwnerName, UnitGroup, AccountType, "
            + "max(InstalledARR) as InstalledARR, "
            + "max(ProductCount) as ProductCount, "
            + "max(WhitespaceScore) as WhitespaceScore, "
            + "max(ExpansionScore) as ExpansionScore, "
            + "AccountId;\n"
            + "q = order q by InstalledARR desc;\n"
            + "q = limit q 20;"
        ),
        "s_unmapped_products": sq(
            opp
            + 'q = filter q by ProductFamily == "Unmapped";\n'
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, sum(ProjectedARR) as ProjectedARR, count() as OpportunityCount;\n"
            + "q = order q by OpportunityCount desc;"
        ),
        "s_at_risk_accounts": sq(
            account_product
            + 'q = filter q by RiskLevel == "High" or RiskLevel == "Critical";\n'
            + "q = group q by (AccountName, OwnerName, UnitGroup, ProductFamily, AccountId);\n"
            + "q = foreach q generate AccountName, OwnerName, UnitGroup, ProductFamily, "
            + "max(InstalledARR) as InstalledARR, "
            + "max(OpenExpansionARR) as OpenExpansionARR, "
            + "max(WhitespaceScore) as WhitespaceScore, "
            + "AccountId;\n"
            + "q = order q by InstalledARR desc;\n"
            + "q = limit q 20;"
        ),
    }


def build_widgets() -> dict[str, dict]:
    """Build dashboard widgets."""
    widgets = {
        "p1_nav1": nav_link("portfolio", "Portfolio Mix", active=True),
        "p1_nav2": nav_link("attach", "Attach & Whitespace"),
        "p1_nav3": nav_link("expansion", "Expansion Plays"),
        "p1_nav4": nav_link("risk", "Risk & Actions"),
        "p1_hdr": hdr(
            "Product Portfolio & Whitespace",
            "Commercial product mix, attach gaps, and expansion headroom across the installed base.",
        ),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_account_type": pillbox("f_account_type", "Account Type"),
        "p1_f_segment": pillbox("f_segment", "Segment"),
        "p1_f_product": pillbox("f_product", "Product Family"),
        "p1_n_installed": num("s_summary", "installed_arr", "Installed ARR", "#032D60", compact=True),
        "p1_n_expand": num("s_summary", "expansion_pipe_arr", "Open Expansion ARR", "#0176D3", compact=True),
        "p1_n_whitespace": num("s_summary", "whitespace_arr", "Whitespace ARR", "#BA0517", compact=True),
        "p1_n_multi": num("s_summary", "multi_product_accounts", "Multi-Product Accounts", "#2E844A", compact=True),
        "p1_ch_treemap": treemap_chart(
            "s_product_mix",
            "Installed ARR by Product Family",
            ["ProductFamily"],
            "WonARR",
            show_legend=False,
        ),
        "p1_ch_quarter": rich_chart(
            "s_commercial_mix",
            "stackhbar",
            "Sold, Renewal, and Expansion Mix by Product Family",
            ["MotionType"],
            ["ProjectedARR"],
            split=["ProductFamily"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p1_ch_client": heatmap_chart(
            "s_client_matrix",
            "Account Type x Product Family Installed Accounts",
            show_legend=True,
        ),
        "p2_nav1": nav_link("portfolio", "Portfolio Mix"),
        "p2_nav2": nav_link("attach", "Attach & Whitespace", active=True),
        "p2_nav3": nav_link("expansion", "Expansion Plays"),
        "p2_nav4": nav_link("risk", "Risk & Actions"),
        "p2_hdr": hdr(
            "Attach & Whitespace",
            "Where product penetration is shallow and which accounts represent the highest-value cross-sell gaps.",
        ),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_account_type": pillbox("f_account_type", "Account Type"),
        "p2_f_segment": pillbox("f_segment", "Segment"),
        "p2_f_product": pillbox("f_product", "Product Family"),
        "p2_ch_attach": heatmap_chart(
            "s_attach_matrix",
            "Unit Group x Product Family Installed Accounts",
            show_legend=True,
        ),
        "p2_ch_dist": rich_chart(
            "s_product_count_dist",
            "stackcolumn",
            "Installed Product Count Distribution",
            ["ProductCountBand"],
            ["AccountCount"],
            show_legend=False,
            axis_title="Accounts",
            show_values=True,
        ),
        "p2_ch_combo": rich_chart(
            "s_product_combos",
            "hbar",
            "Top Installed Product Combinations",
            ["ProductCombo"],
            ["InstalledARR"],
            show_legend=False,
            axis_title="Installed ARR (EUR)",
            show_values=True,
        ),
        "p2_tbl_whitespace": rich_chart(
            "s_whitespace_accounts",
            "comparisontable",
            "Top Whitespace Accounts",
            ["AccountName", "OwnerName", "UnitGroup", "Segment"],
            ["InstalledARR", "OpenExpansionARR", "ProductCount", "WhitespaceScore"],
            show_legend=False,
        ),
        "p3_nav1": nav_link("portfolio", "Portfolio Mix"),
        "p3_nav2": nav_link("attach", "Attach & Whitespace"),
        "p3_nav3": nav_link("expansion", "Expansion Plays", active=True),
        "p3_nav4": nav_link("risk", "Risk & Actions"),
        "p3_hdr": hdr(
            "Expansion Plays",
            "Which accounts and product families have the strongest combination of existing footprint and expansion headroom.",
        ),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_account_type": pillbox("f_account_type", "Account Type"),
        "p3_f_segment": pillbox("f_segment", "Segment"),
        "p3_f_product": pillbox("f_product", "Product Family"),
        "p3_ch_scatter": bubble_chart(
            "s_expansion_scatter",
            "Expansion Score vs Product Count",
            show_legend=False,
        ),
        "p3_ch_expand": rich_chart(
            "s_expand_by_product",
            "stackhbar",
            "Open vs Weighted Expansion ARR by Product Family",
            ["ProductFamily"],
            ["OpenARR", "WeightedOpenARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p3_ch_flow": sankey_chart(
            "s_motion_product_flow",
            "Revenue Flow: Motion -> Product Family",
        ),
        "p3_tbl_expansion": rich_chart(
            "s_top_expansion",
            "comparisontable",
            "Top Expansion Plays",
            ["AccountName", "OwnerName", "UnitGroup", "Segment"],
            ["InstalledARR", "OpenExpansionARR", "ExpansionScore", "ProductCount"],
            show_legend=False,
        ),
        "p4_nav1": nav_link("portfolio", "Portfolio Mix"),
        "p4_nav2": nav_link("attach", "Attach & Whitespace"),
        "p4_nav3": nav_link("expansion", "Expansion Plays"),
        "p4_nav4": nav_link("risk", "Risk & Actions", active=True),
        "p4_hdr": hdr(
            "Risk & Actions",
            "Accounts with high-value product gaps, mapping issues, or concentrated commercial risk that need attention.",
        ),
        "p4_f_unit": pillbox("f_unit", "Unit Group"),
        "p4_f_account_type": pillbox("f_account_type", "Account Type"),
        "p4_f_segment": pillbox("f_segment", "Segment"),
        "p4_f_product": pillbox("f_product", "Product Family"),
        "p4_ch_risk": rich_chart(
            "s_risk_by_product",
            "stackhbar",
            "Risk Exposure vs Projected ARR by Product Family",
            ["ProductFamily"],
            ["ProjectedARR", "RiskExposureARR"],
            show_legend=True,
            axis_title="ARR (EUR)",
            show_values=True,
        ),
        "p4_ch_growth_risk": bubble_chart(
            "s_growth_risk_scatter",
            "Whitespace Score vs Expansion Score",
            show_legend=False,
        ),
        "p4_ch_unmapped": rich_chart(
            "s_unmapped_products",
            "hbar",
            "Unmapped Product Opportunities by Owner",
            ["OwnerName"],
            ["OpportunityCount"],
            show_legend=False,
            axis_title="Opportunities",
            show_values=True,
        ),
        "p4_tbl_risk": rich_chart(
            "s_at_risk_accounts",
            "comparisontable",
            "At-Risk Product Accounts",
            ["AccountName", "OwnerName", "UnitGroup", "ProductFamily"],
            ["InstalledARR", "OpenExpansionARR", "WhitespaceScore"],
            show_legend=False,
        ),
        "p4_tbl_low_coverage": rich_chart(
            "s_low_coverage",
            "comparisontable",
            "Low-Coverage High-Value Accounts",
            ["AccountName", "OwnerName", "UnitGroup", "AccountType"],
            ["InstalledARR", "ProductCount", "WhitespaceScore", "ExpansionScore"],
            show_legend=False,
        ),
    }

    add_table_action(widgets["p2_tbl_whitespace"], "salesforceActions", "Account", "AccountId")
    add_table_action(widgets["p3_tbl_expansion"], "salesforceActions", "Account", "AccountId")
    add_table_action(widgets["p4_tbl_risk"], "salesforceActions", "Account", "AccountId")
    add_table_action(widgets["p4_tbl_low_coverage"], "salesforceActions", "Account", "AccountId")
    return widgets


def build_layout() -> dict:
    """Build the 4-page product dashboard layout."""
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_account_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_segment", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_product", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p1_n_installed", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_expand", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_whitespace", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_multi", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p1_ch_treemap", "row": 9, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p1_ch_quarter", "row": 9, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p1_ch_client", "row": 16, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_account_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_segment", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_product", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p2_ch_attach", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_dist", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p2_ch_combo", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p2_tbl_whitespace", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_account_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_segment", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_product", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p3_ch_scatter", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_expand", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p3_ch_flow", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p3_tbl_expansion", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_unit", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_account_type", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_segment", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_product", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p4_ch_risk", "row": 5, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p4_ch_growth_risk", "row": 5, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p4_ch_unmapped", "row": 12, "column": 0, "colspan": 6, "rowspan": 7},
        {"name": "p4_tbl_risk", "row": 12, "column": 6, "colspan": 6, "rowspan": 7},
        {"name": "p4_tbl_low_coverage", "row": 19, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "ProductPortfolioWhitespace",
        "numColumns": 12,
        "pages": [
            pg("portfolio", "Portfolio Mix", p1),
            pg("attach", "Attach & Whitespace", p2),
            pg("expansion", "Expansion Plays", p3),
            pg("risk", "Risk & Actions", p4),
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

    state = build_dashboard_state(build_steps(dataset_id), build_widgets(), build_layout())

    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    print(f"\n=== Deploying {DASHBOARD_LABEL} ===")
    deploy_dashboard(instance_url, token, dashboard_id, state)

    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
        ],
    )


if __name__ == "__main__":
    main()
