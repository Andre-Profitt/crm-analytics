#!/usr/bin/env python3
"""Build a Product ML & Recommendations dashboard.

This dashboard adds a real product-intelligence layer on top of the product
line-item backbone:
- K-means account archetypes
- predictive attach models for product-family adoption
- family-affinity recommendations
- model QA and feature-driver visibility
"""

from __future__ import annotations

import csv
import io
import math
from collections import Counter, defaultdict

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from build_product_portfolio_dashboard import (
    _commercial_value,
    _delivery_model,
    _industry_vertical,
    _mapped_product,
    _mapped_text,
    _normalize_industry,
    _row_value,
    _segment,
)
from crm_analytics_helpers import (
    _dim,
    _measure,
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
    scatter_chart,
    set_record_links_xmd,
    sq,
    upload_dataset,
    _soql,
)
from portfolio_foundation import normalize_motion, safe_float

DS = "Product_ML_Recommendations"
DS_LABEL = "Product ML Recommendations"
DASHBOARD_LABEL = "Product ML & Recommendations"

ML_ACCOUNT_SOQL = (
    "SELECT Id, Name, Owner.Name, Type, CreatedDate, BillingCountry, "
    "Industry, Unit_Group__c, SaaS_Client__c, Axioma_Client__c, "
    "Risk_of_Potential_Termination__c, Partner_Engagement_Level__c, "
    "AuM_m__c, NumberOfEmployees "
    "FROM Account"
)

ML_LINE_ITEM_SOQL = (
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
    "WHERE Opportunity.CloseDate >= 2020-01-01 OR Opportunity.IsClosed = false"
)

MIN_MODEL_POSITIVES = 12
MAX_MODEL_FAMILIES = 10
MAX_FEATURE_FAMILIES = 12
MAX_CLUSTER_FAMILIES = 12
EXCLUDED_RECOMMENDATION_FAMILIES = {
    "Unmapped",
    "3rd party products",
    "White Label Products",
    "SCD Consulting",
}

RISK_ORDER = {"Low": 1, "Medium": 2, "High": 3, "Critical": 4}
PARTNER_ORDER = {"None": 0, "Low": 1, "Medium": 2, "High": 3, "Strategic": 4}


def _safe_log1p(value: object) -> float:
    return math.log1p(max(0.0, safe_float(value)))


def _ml_value(line_item: dict[str, object]) -> float:
    value = _commercial_value(line_item)
    if value > 0:
        return value
    unit_price = safe_float(line_item.get("UnitPrice"))
    if unit_price > 0:
        return round(unit_price, 2)
    return 0.0


def _risk_ordinal(value: object) -> int:
    raw = _mapped_text(value, "Low")
    return RISK_ORDER.get(raw, 1)


def _partner_ordinal(value: object) -> int:
    raw = _mapped_text(value, "None")
    return PARTNER_ORDER.get(raw, 0)


def _short_family(value: str) -> str:
    mapping = {
        "SCD Software": "Core",
        "SimCorp SaaS": "SaaS",
        "XaaS": "XaaS",
        "Client Communications": "Client Comms",
        "Analytics Services": "Analytics",
        "White Label Products": "White Label",
        "Data Management": "Data Mgmt",
        "Data Management Services": "DMS",
        "Regulatory Services": "Reg Services",
        "SCD Operational Services": "Ops Services",
    }
    return mapping.get(value, value[:24])


def _clean_feature_name(name: str) -> str:
    if name.startswith("InstalledFamily__"):
        return f"Installed {_short_family(name.split('__', 1)[1])}"
    if name.startswith("OpenExpandFamily__"):
        return f"Open expansion {_short_family(name.split('__', 1)[1])}"
    if name.startswith("IndustryVertical_"):
        return name.replace("IndustryVertical_", "Industry: ")
    if name.startswith("Segment_"):
        return name.replace("Segment_", "Segment: ")
    if name.startswith("DeliveryModel_"):
        return name.replace("DeliveryModel_", "Delivery: ")
    if name.startswith("AccountType_"):
        return name.replace("AccountType_", "Type: ")
    if name.startswith("UnitGroup_"):
        return name.replace("UnitGroup_", "Unit: ")
    if name.startswith("ArchetypeLabel_"):
        return name.replace("ArchetypeLabel_", "Archetype: ")
    mapping = {
        "LogInstalledARR": "Installed ARR",
        "LogOpenExpansionARR": "Open expansion ARR",
        "LogOpenRenewalARR": "Open renewal ARR",
        "ProductCount": "Installed product count",
        "LogAUM": "AUM",
        "LogEmployees": "Employees",
        "RiskOrdinal": "Termination risk",
        "PartnerOrdinal": "Partner engagement",
        "IsSaaSFlag": "SaaS client",
        "IsAxiomaFlag": "Axioma client",
    }
    return mapping.get(name, name[:64])


def _cluster_label(rows: list[dict[str, object]], top_families: list[str]) -> str:
    industries = Counter(str(row["IndustryVertical"]) for row in rows)
    dominant_industry = industries.most_common(1)[0][0] if industries else "Mixed"
    family_counts = Counter()
    for row in rows:
        installed = row.get("InstalledFamilies", set())
        if isinstance(installed, set):
            for family in installed:
                if family in top_families:
                    family_counts[family] += 1
    dominant_family = family_counts.most_common(1)[0][0] if family_counts else "Mixed"
    avg_products = float(np.mean([safe_float(row["ProductCount"]) for row in rows])) if rows else 0.0
    cloud_share = float(np.mean([1.0 if row["IsSaaS"] == "true" else 0.0 for row in rows])) if rows else 0.0
    depth_tag = "Broad" if avg_products >= 3 else "Focused"
    delivery_tag = "Cloud" if cloud_share >= 0.5 else "Hybrid"
    return f"{dominant_industry} {delivery_tag} {depth_tag} {_short_family(dominant_family)}"[:255]


def _peer_key(row: dict[str, object]) -> tuple[str, str, str]:
    return (
        str(row["IndustryVertical"]),
        str(row["Segment"]),
        str(row["DeliveryModel"]),
    )


def _build_account_rows(
    accounts: list[dict[str, object]], line_items: list[dict[str, object]]
) -> tuple[list[dict[str, object]], dict[str, dict[str, object]]]:
    account_metrics: dict[str, dict[str, object]] = {}
    for account in accounts:
        account_id = account.get("Id")
        if not account_id:
            continue
        owner = account.get("Owner") or {}
        industry = _mapped_text(account.get("Industry"), "Unknown")
        industry_normalized = _normalize_industry(industry)
        industry_vertical = _industry_vertical(industry_normalized)
        base = {
            "AccountId": account_id,
            "AccountName": _mapped_text(account.get("Name"), ""),
            "OwnerName": _mapped_text(owner.get("Name"), "Unknown"),
            "UnitGroup": _mapped_text(account.get("Unit_Group__c"), "Unassigned"),
            "AccountType": _mapped_text(account.get("Type"), "Unknown"),
            "Industry": industry,
            "IndustryVertical": industry_vertical,
            "BillingCountry": _mapped_text(account.get("BillingCountry"), "Unknown"),
            "IsSaaS": str(bool(account.get("SaaS_Client__c"))).lower(),
            "IsAxioma": str(bool(account.get("Axioma_Client__c"))).lower(),
            "DeliveryModel": _delivery_model(str(bool(account.get("SaaS_Client__c"))).lower()),
            "RiskLevel": _mapped_text(account.get("Risk_of_Potential_Termination__c"), "Low"),
            "PartnerLevel": _mapped_text(account.get("Partner_Engagement_Level__c"), "None"),
            "AUM": round(safe_float(account.get("AuM_m__c")), 2),
            "EmployeeCount": round(safe_float(account.get("NumberOfEmployees")), 2),
            "InstalledARR": 0.0,
            "OpenExpansionARR": 0.0,
            "OpenRenewalARR": 0.0,
            "InstalledFamilies": set(),
            "InstalledFamilyARR": defaultdict(float),
            "OpenExpandFamilyARR": defaultdict(float),
            "OpenRenewalFamilyARR": defaultdict(float),
            "ProductClassificationARR": defaultdict(float),
        }
        account_metrics[account_id] = base

    family_stats: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "accounts": set(),
            "installed_arr": 0.0,
            "classifications": defaultdict(float),
        }
    )

    for line_item in line_items:
        account_id = _row_value(line_item, "Opportunity", "AccountId")
        if not account_id or account_id not in account_metrics:
            continue

        family = _mapped_product(_row_value(line_item, "Product2", "Family"))
        classification = _mapped_text(_row_value(line_item, "Product2", "Classification_Legal__c"))
        motion = normalize_motion(_row_value(line_item, "Opportunity", "Type") or "")
        is_closed = str(_row_value(line_item, "Opportunity", "IsClosed") or "").lower() == "true"
        is_won = str(_row_value(line_item, "Opportunity", "IsWon") or "").lower() == "true"
        row = account_metrics[account_id]
        arr = _ml_value(line_item)
        classification_weight = arr if arr > 0 else 1.0

        row["ProductClassificationARR"][classification] += classification_weight
        if is_won:
            if arr > 0:
                row["InstalledARR"] = round(safe_float(row["InstalledARR"]) + arr, 2)
            cast_installed = row["InstalledFamilies"]
            if isinstance(cast_installed, set):
                cast_installed.add(family)
            row["InstalledFamilyARR"][family] += arr
            family_stats[family]["accounts"].add(account_id)
            family_stats[family]["installed_arr"] = round(
                safe_float(family_stats[family]["installed_arr"]) + arr, 2
            )
            family_stats[family]["classifications"][classification] += classification_weight
        elif not is_closed and arr > 0:
            if motion == "Expand":
                row["OpenExpansionARR"] = round(safe_float(row["OpenExpansionARR"]) + arr, 2)
                row["OpenExpandFamilyARR"][family] += arr
            if motion == "Renewal":
                row["OpenRenewalARR"] = round(safe_float(row["OpenRenewalARR"]) + arr, 2)
                row["OpenRenewalFamilyARR"][family] += arr

    account_rows: list[dict[str, object]] = []
    for row in account_metrics.values():
        product_count = len(row["InstalledFamilies"]) if isinstance(row["InstalledFamilies"], set) else 0
        installed_arr = round(safe_float(row["InstalledARR"]), 2)
        open_expand_arr = round(safe_float(row["OpenExpansionARR"]), 2)
        open_renewal_arr = round(safe_float(row["OpenRenewalARR"]), 2)
        if (
            product_count <= 0
            and
            installed_arr <= 0
            and open_expand_arr <= 0
            and open_renewal_arr <= 0
            and row["IsSaaS"] != "true"
            and row["IsAxioma"] != "true"
        ):
            continue
        row["ProductCount"] = product_count
        row["Segment"] = _segment(safe_float(row["AUM"]), installed_arr)
        row["PrimaryClassification"] = (
            max(
                row["ProductClassificationARR"].items(),
                key=lambda item: safe_float(item[1]),
            )[0]
            if row["ProductClassificationARR"]
            else "Unknown"
        )
        row["ArchetypeLabel"] = ""
        row["ClusterIndex"] = 0
        row["TopRecommendationFamily"] = ""
        row["RecommendationScore"] = 0.0
        row["RecommendationPotentialARR"] = 0.0
        row["RecommendationCount"] = 0
        row["HasRecommendation"] = 0
        row["ClusterPCA1"] = 0.0
        row["ClusterPCA2"] = 0.0
        account_rows.append(row)

    return account_rows, family_stats


def _family_lists(family_stats: dict[str, dict[str, object]]) -> tuple[list[str], list[str]]:
    families = sorted(
        (
            (family, len(stats["accounts"]), safe_float(stats["installed_arr"]))
            for family, stats in family_stats.items()
            if family != "Unmapped"
        ),
        key=lambda item: (item[1], item[2]),
        reverse=True,
    )
    feature_families = [family for family, support, _ in families if support >= 8][:MAX_FEATURE_FAMILIES]
    modeled_families = [
        family
        for family, support, _ in families
        if support >= MIN_MODEL_POSITIVES and family not in EXCLUDED_RECOMMENDATION_FAMILIES
    ][:MAX_MODEL_FAMILIES]
    return feature_families, modeled_families


def _cluster_accounts(
    account_rows: list[dict[str, object]], feature_families: list[str]
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[tuple[str, str], float]]:
    if not account_rows:
        return [], [], {}

    cluster_records: list[dict[str, object]] = []
    for row in account_rows:
        record = {
            "LogInstalledARR": _safe_log1p(row["InstalledARR"]),
            "LogOpenExpansionARR": _safe_log1p(row["OpenExpansionARR"]),
            "LogOpenRenewalARR": _safe_log1p(row["OpenRenewalARR"]),
            "ProductCount": safe_float(row["ProductCount"]),
            "LogAUM": _safe_log1p(row["AUM"]),
            "LogEmployees": _safe_log1p(row["EmployeeCount"]),
            "RiskOrdinal": _risk_ordinal(row["RiskLevel"]),
            "PartnerOrdinal": _partner_ordinal(row["PartnerLevel"]),
            "IsSaaSFlag": 1.0 if row["IsSaaS"] == "true" else 0.0,
            "IsAxiomaFlag": 1.0 if row["IsAxioma"] == "true" else 0.0,
            "IndustryVertical": row["IndustryVertical"],
            "Segment": row["Segment"],
            "DeliveryModel": row["DeliveryModel"],
        }
        installed = row["InstalledFamilies"] if isinstance(row["InstalledFamilies"], set) else set()
        for family in feature_families:
            record[f"InstalledFamily__{family}"] = 1.0 if family in installed else 0.0
        cluster_records.append(record)

    feature_df = pd.DataFrame(cluster_records)
    categorical_cols = ["IndustryVertical", "Segment", "DeliveryModel"]
    numeric_df = feature_df.drop(columns=categorical_cols)
    encoded = pd.get_dummies(feature_df[categorical_cols], prefix=categorical_cols, dtype=float)
    X = pd.concat([numeric_df, encoded], axis=1)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_scaled = np.nan_to_num(np.clip(X_scaled, -6.0, 6.0), nan=0.0, posinf=6.0, neginf=-6.0)
    if len(account_rows) >= 3:
        embedding = PCA(n_components=2, svd_solver="randomized", random_state=42).fit_transform(
            X_scaled
        )
    else:
        embedding = np.zeros((len(account_rows), 2), dtype=float)

    if len(account_rows) < 10:
        labels = np.zeros(len(account_rows), dtype=int)
    else:
        k = 5 if len(account_rows) >= 80 else 4
        k = min(k, max(2, len(account_rows) // 8))
        km = KMeans(n_clusters=k, random_state=42, n_init=20, max_iter=400)
        labels = km.fit_predict(X_scaled)

    cluster_members: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row, label, coords in zip(account_rows, labels, embedding):
        row["ClusterIndex"] = int(label) + 1
        row["ClusterPCA1"] = round(float(coords[0]), 6)
        row["ClusterPCA2"] = round(float(coords[1]), 6)
        cluster_members[int(label)].append(row)

    cluster_names: dict[int, str] = {}
    for label, rows in cluster_members.items():
        cluster_names[label] = _cluster_label(rows, feature_families)

    cluster_family_rows: list[dict[str, object]] = []
    cluster_family_rates: dict[tuple[str, str], float] = {}
    for label, rows in cluster_members.items():
        cluster_label = cluster_names[label]
        cluster_size = len(rows)
        for row in rows:
            row["ArchetypeLabel"] = cluster_label
        family_counter = Counter()
        family_arr = defaultdict(float)
        for row in rows:
            installed = row.get("InstalledFamilies", set())
            installed_arrs = row.get("InstalledFamilyARR", {})
            if isinstance(installed, set):
                for family in installed:
                    if family in feature_families[:MAX_CLUSTER_FAMILIES]:
                        family_counter[family] += 1
                        family_arr[family] += safe_float(installed_arrs.get(family))
        for family, count in family_counter.items():
            adoption_rate = round(count / cluster_size, 4) if cluster_size else 0.0
            cluster_family_rates[(cluster_label, family)] = adoption_rate
            cluster_family_rows.append(
                {
                    "RecordType": "cluster_family",
                    "Id": f"{label+1}-{family}"[:255],
                    "AccountId": "",
                    "AccountName": "",
                    "OwnerName": "",
                    "UnitGroup": "",
                    "AccountType": "",
                    "Segment": "",
                    "Industry": "",
                    "IndustryVertical": "",
                    "DeliveryModel": "",
                    "IsSaaS": "",
                    "IsAxioma": "",
                    "RiskLevel": "",
                    "PartnerLevel": "",
                    "ProductFamily": family,
                    "ProductClassification": "",
                    "SourceProductFamily": "",
                    "ArchetypeLabel": cluster_label,
                    "TopRecommendationFamily": "",
                    "Reason1": "",
                    "Reason2": "",
                    "Reason3": "",
                    "TopFeature1": "",
                    "TopFeature2": "",
                    "TopFeature3": "",
                    "FeatureGroup": "",
                    "ModelName": "",
                    "AUM": 0.0,
                    "EmployeeCount": 0.0,
                    "InstalledARR": round(safe_float(family_arr[family]), 2),
                    "OpenExpansionARR": 0.0,
                    "OpenRenewalARR": 0.0,
                    "ProductCount": 0.0,
                    "AccountCount": count,
                    "FamilyInstalledARR": round(safe_float(family_arr[family]), 2),
                    "RecommendationScore": 0.0,
                    "PropensityScore": 0.0,
                    "PeerAttachRate": 0.0,
                    "AffinityScore": 0.0,
                    "Lift": 0.0,
                    "SupportCount": cluster_size,
                    "RecommendationPotentialARR": 0.0,
                    "RankWithinAccount": 0.0,
                    "RecommendationCount": 0.0,
                    "HasRecommendation": 0.0,
                    "CVAUC": 0.0,
                    "CVAvgPrecision": 0.0,
                    "PositiveAccounts": 0.0,
                    "CandidateAccounts": 0.0,
                    "AdoptionRate": adoption_rate,
                    "ImportanceWeight": 0.0,
                    "ClusterIndex": label + 1,
                }
            )

    return account_rows, cluster_family_rows, cluster_family_rates


def _pair_metrics(
    account_rows: list[dict[str, object]], modeled_families: list[str]
) -> tuple[dict[str, float], dict[tuple[str, str], tuple[str, float, float]], dict[tuple[str, str], float], list[dict[str, object]]]:
    total_accounts = max(len(account_rows), 1)
    family_prevalence: dict[str, float] = {}
    source_support = Counter()
    pair_support = Counter()
    for row in account_rows:
        installed = sorted(
            family
            for family in (row.get("InstalledFamilies", set()) if isinstance(row.get("InstalledFamilies"), set) else set())
            if family in modeled_families
        )
        for family in installed:
            source_support[family] += 1
        for source in installed:
            for target in installed:
                if source != target:
                    pair_support[(source, target)] += 1

    for family in modeled_families:
        family_prevalence[family] = source_support[family] / total_accounts if total_accounts else 0.0

    best_affinity: dict[tuple[str, str], tuple[str, float, float]] = {}
    affinity_strength: dict[tuple[str, str], float] = {}
    affinity_rows: list[dict[str, object]] = []
    for (source, target), both in pair_support.items():
        if source_support[source] <= 0 or family_prevalence.get(target, 0.0) <= 0:
            continue
        conditional = both / source_support[source]
        lift = conditional / family_prevalence[target]
        score = conditional * max(1.0, lift)
        affinity_strength[(source, target)] = score
        best_affinity[(source, target)] = (source, conditional, lift)
        if both < 3 or conditional < 0.10:
            continue
        affinity_rows.append(
            {
                "RecordType": "family_affinity",
                "Id": f"{source}->{target}"[:255],
                "AccountId": "",
                "AccountName": "",
                "OwnerName": "",
                "UnitGroup": "",
                "AccountType": "",
                "Segment": "",
                "Industry": "",
                "IndustryVertical": "",
                "DeliveryModel": "",
                "IsSaaS": "",
                "IsAxioma": "",
                "RiskLevel": "",
                "PartnerLevel": "",
                "ProductFamily": target,
                "ProductClassification": "",
                "SourceProductFamily": source,
                "ArchetypeLabel": "",
                "TopRecommendationFamily": "",
                "Reason1": "",
                "Reason2": "",
                "Reason3": "",
                "TopFeature1": "",
                "TopFeature2": "",
                "TopFeature3": "",
                "FeatureGroup": "",
                "ModelName": "",
                "AUM": 0.0,
                "EmployeeCount": 0.0,
                "InstalledARR": 0.0,
                "OpenExpansionARR": 0.0,
                "OpenRenewalARR": 0.0,
                "ProductCount": 0.0,
                "AccountCount": 0.0,
                "FamilyInstalledARR": 0.0,
                "RecommendationScore": round(score * 100, 2),
                "PropensityScore": 0.0,
                "PeerAttachRate": conditional,
                "AffinityScore": conditional,
                "Lift": round(lift, 3),
                "SupportCount": both,
                "RecommendationPotentialARR": round(score * both * 10000, 2),
                "RankWithinAccount": 0.0,
                "RecommendationCount": 0.0,
                "HasRecommendation": 0.0,
                "CVAUC": 0.0,
                "CVAvgPrecision": 0.0,
                "PositiveAccounts": 0.0,
                "CandidateAccounts": 0.0,
                "AdoptionRate": conditional,
                "ImportanceWeight": 0.0,
                "ClusterIndex": 0.0,
            }
        )

    affinity_rows.sort(key=lambda row: safe_float(row["RecommendationScore"]), reverse=True)
    return family_prevalence, best_affinity, affinity_strength, affinity_rows[:30]


def _cohort_rates(account_rows: list[dict[str, object]], modeled_families: list[str]) -> tuple[dict[tuple[str, str, str], int], dict[tuple[tuple[str, str, str], str], int], dict[tuple[str, str], int]]:
    cohort_total = Counter()
    cohort_family = Counter()
    industry_family = Counter()
    for row in account_rows:
        key = _peer_key(row)
        cohort_total[key] += 1
        installed = row.get("InstalledFamilies", set())
        if not isinstance(installed, set):
            continue
        for family in installed:
            if family in modeled_families:
                cohort_family[(key, family)] += 1
                industry_family[(str(row["IndustryVertical"]), family)] += 1
    return cohort_total, cohort_family, industry_family


def _peer_attach_rate(
    row: dict[str, object],
    target_family: str,
    family_prevalence: dict[str, float],
    cohort_total: dict[tuple[str, str, str], int],
    cohort_family: dict[tuple[tuple[str, str, str], str], int],
    industry_family: dict[tuple[str, str], int],
    total_accounts: int,
) -> float:
    key = _peer_key(row)
    cohort_size = cohort_total.get(key, 0)
    if cohort_size >= 5:
        return cohort_family.get((key, target_family), 0) / cohort_size
    industry = str(row["IndustryVertical"])
    industry_total = sum(
        1 for candidate in cohort_total if candidate[0] == industry
    )
    if industry_total >= 3:
        return industry_family.get((industry, target_family), 0) / industry_total
    return family_prevalence.get(target_family, 0.0) if total_accounts else 0.0


def _build_reasons(
    row: dict[str, object],
    target_family: str,
    peer_rate: float,
    affinity_source: str,
    affinity_score: float,
    cluster_rate: float,
    propensity: float,
) -> tuple[str, str, str]:
    reasons: list[str] = []
    if peer_rate >= 0.35:
        reasons.append(
            f"{row['IndustryVertical']} peers attach {_short_family(target_family)} at {peer_rate * 100:.0f}%"
        )
    if affinity_source and affinity_score >= 0.20:
        reasons.append(
            f"{_short_family(affinity_source)} frequently co-sells into {_short_family(target_family)}"
        )
    if cluster_rate >= 0.40:
        reasons.append(
            f"Accounts in the {row['ArchetypeLabel']} archetype often hold {_short_family(target_family)}"
        )
    if safe_float(row["OpenExpansionARR"]) > 0:
        reasons.append("Existing expansion motion raises attach likelihood")
    if safe_float(row["ProductCount"]) <= 1:
        reasons.append("Shallow current product penetration leaves whitespace")
    if propensity >= 0.55:
        reasons.append("Model propensity is materially above baseline")
    while len(reasons) < 3:
        reasons.append("")
    return reasons[0][:255], reasons[1][:255], reasons[2][:255]


def _model_rows(
    account_rows: list[dict[str, object]],
    modeled_families: list[str],
    feature_families: list[str],
    family_stats: dict[str, dict[str, object]],
    family_prevalence: dict[str, float],
    cohort_total: dict[tuple[str, str, str], int],
    cohort_family: dict[tuple[tuple[str, str, str], str], int],
    industry_family: dict[tuple[str, str], int],
    pair_details: dict[tuple[str, str], tuple[str, float, float]],
    affinity_strength: dict[tuple[str, str], float],
    cluster_family_rates: dict[tuple[str, str], float],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    if not account_rows:
        return [], [], []

    total_accounts = len(account_rows)
    feature_records: list[dict[str, object]] = []
    meta_by_account: dict[str, dict[str, object]] = {}
    for row in account_rows:
        account_id = str(row["AccountId"])
        meta_by_account[account_id] = row
        feature_row: dict[str, object] = {
            "AccountId": account_id,
            "IndustryVertical": row["IndustryVertical"],
            "Segment": row["Segment"],
            "DeliveryModel": row["DeliveryModel"],
            "UnitGroup": row["UnitGroup"],
            "AccountType": row["AccountType"],
            "ArchetypeLabel": row["ArchetypeLabel"],
            "LogInstalledARR": _safe_log1p(row["InstalledARR"]),
            "LogOpenExpansionARR": _safe_log1p(row["OpenExpansionARR"]),
            "LogOpenRenewalARR": _safe_log1p(row["OpenRenewalARR"]),
            "ProductCount": safe_float(row["ProductCount"]),
            "LogAUM": _safe_log1p(row["AUM"]),
            "LogEmployees": _safe_log1p(row["EmployeeCount"]),
            "RiskOrdinal": _risk_ordinal(row["RiskLevel"]),
            "PartnerOrdinal": _partner_ordinal(row["PartnerLevel"]),
            "IsSaaSFlag": 1.0 if row["IsSaaS"] == "true" else 0.0,
            "IsAxiomaFlag": 1.0 if row["IsAxioma"] == "true" else 0.0,
        }
        installed = row.get("InstalledFamilies", set()) if isinstance(row.get("InstalledFamilies"), set) else set()
        expand = row.get("OpenExpandFamilyARR", {})
        for family in feature_families:
            feature_row[f"InstalledFamily__{family}"] = 1.0 if family in installed else 0.0
            feature_row[f"OpenExpandFamily__{family}"] = 1.0 if safe_float(expand.get(family)) > 0 else 0.0
        feature_records.append(feature_row)

    feature_df = pd.DataFrame(feature_records).set_index("AccountId")
    categorical_cols = [
        "IndustryVertical",
        "Segment",
        "DeliveryModel",
        "UnitGroup",
        "AccountType",
        "ArchetypeLabel",
    ]
    binary_cols = [
        column
        for column in feature_df.columns
        if column.startswith("InstalledFamily__") or column.startswith("OpenExpandFamily__")
    ]
    numeric_cols = [
        column
        for column in feature_df.columns
        if column not in categorical_cols and column not in binary_cols
    ]
    X_num = feature_df[numeric_cols].astype(float)
    X_bin = feature_df[binary_cols].astype(float)
    X_cat = pd.get_dummies(feature_df[categorical_cols], prefix=categorical_cols, dtype=float)
    X_all = pd.concat([X_num, X_bin, X_cat], axis=1)

    recommendation_candidates: dict[str, list[dict[str, object]]] = defaultdict(list)
    model_metric_rows: list[dict[str, object]] = []
    feature_importance_rows: list[dict[str, object]] = []

    for family in modeled_families:
        target_col = f"InstalledFamily__{family}"
        if target_col not in feature_df.columns:
            continue
        y = feature_df[target_col].astype(int)
        positives = int(y.sum())
        negatives = int(len(y) - positives)
        if positives < MIN_MODEL_POSITIVES or negatives < MIN_MODEL_POSITIVES:
            continue

        X = X_all.drop(columns=[target_col], errors="ignore")
        rf_model = RandomForestClassifier(
            n_estimators=240,
            max_depth=6,
            min_samples_leaf=3,
            class_weight="balanced_subsample",
            random_state=42,
            n_jobs=-1,
        )
        lr_model = make_pipeline(
            StandardScaler(),
            LogisticRegression(
                max_iter=2000,
                class_weight="balanced",
                solver="liblinear",
                random_state=42,
            ),
        )
        n_splits = min(4, positives, negatives)
        if n_splits < 3:
            continue
        cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
        rf_oof = cross_val_predict(rf_model, X, y, cv=cv, method="predict_proba", n_jobs=-1)[:, 1]
        lr_oof = cross_val_predict(lr_model, X, y, cv=cv, method="predict_proba")[:, 1]
        ensemble_oof = (0.55 * rf_oof) + (0.45 * lr_oof)
        auc = float(roc_auc_score(y, ensemble_oof))
        avg_precision = float(average_precision_score(y, ensemble_oof))
        rf_auc = float(roc_auc_score(y, rf_oof))
        lr_auc = float(roc_auc_score(y, lr_oof))
        rf_avg_precision = float(average_precision_score(y, rf_oof))
        lr_avg_precision = float(average_precision_score(y, lr_oof))

        rf_model.fit(X, y)
        lr_model.fit(X, y)
        rf_probabilities = rf_model.predict_proba(X)[:, 1]
        lr_probabilities = lr_model.predict_proba(X)[:, 1]
        probabilities = (0.55 * rf_probabilities) + (0.45 * lr_probabilities)

        rf_importances = np.array(rf_model.feature_importances_, dtype=float)
        if rf_importances.sum() > 0:
            rf_importances = rf_importances / rf_importances.sum()
        lr_coefficients = np.abs(
            lr_model.named_steps["logisticregression"].coef_[0].astype(float)
        )
        if lr_coefficients.sum() > 0:
            lr_coefficients = lr_coefficients / lr_coefficients.sum()
        combined_importances = (0.60 * rf_importances) + (0.40 * lr_coefficients)
        importances = list(zip(X.columns.tolist(), combined_importances.tolist()))
        importances = [item for item in importances if safe_float(item[1]) > 0]
        importances.sort(key=lambda item: item[1], reverse=True)
        top_features = [_clean_feature_name(name) for name, _ in importances[:3]]
        while len(top_features) < 3:
            top_features.append("")

        model_metric_rows.append(
            {
                "RecordType": "model_metric",
                "Id": family[:255],
                "AccountId": "",
                "AccountName": "",
                "OwnerName": "",
                "UnitGroup": "",
                "AccountType": "",
                "Segment": "",
                "Industry": "",
                "IndustryVertical": "",
                "DeliveryModel": "",
                "IsSaaS": "",
                "IsAxioma": "",
                "RiskLevel": "",
                "PartnerLevel": "",
                "ProductFamily": family,
                "ProductClassification": (
                    max(
                        family_stats[family]["classifications"].items(),
                        key=lambda item: safe_float(item[1]),
                    )[0]
                    if family_stats[family]["classifications"]
                    else ""
                )[:255],
                "SourceProductFamily": "",
                "ArchetypeLabel": "",
                "TopRecommendationFamily": "",
                "Reason1": "",
                "Reason2": "",
                "Reason3": "",
                "TopFeature1": top_features[0][:255],
                "TopFeature2": top_features[1][:255],
                "TopFeature3": top_features[2][:255],
                "FeatureGroup": "",
                "ModelName": "AttachEnsemble_v2",
                "AUM": 0.0,
                "EmployeeCount": 0.0,
                "InstalledARR": round(safe_float(family_stats[family]["installed_arr"]), 2),
                "OpenExpansionARR": 0.0,
                "OpenRenewalARR": 0.0,
                "ProductCount": 0.0,
                "AccountCount": 0.0,
                "FamilyInstalledARR": 0.0,
                "RecommendationScore": round(float(np.mean(probabilities[y == 0])) * 100, 2),
                "PropensityScore": 0.0,
                "PeerAttachRate": family_prevalence.get(family, 0.0),
                "AffinityScore": 0.0,
                "Lift": 0.0,
                "SupportCount": positives,
                "RecommendationPotentialARR": 0.0,
                "RankWithinAccount": 0.0,
                "RecommendationCount": 0.0,
                "HasRecommendation": 0.0,
                "CVAUC": round(auc, 4),
                "CVAvgPrecision": round(avg_precision, 4),
                "RFAUC": round(rf_auc, 4),
                "LRAUC": round(lr_auc, 4),
                "RFAvgPrecision": round(rf_avg_precision, 4),
                "LRAvgPrecision": round(lr_avg_precision, 4),
                "PositiveAccounts": positives,
                "CandidateAccounts": negatives,
                "AdoptionRate": family_prevalence.get(family, 0.0),
                "ImportanceWeight": 0.0,
                "ModelContribution": 0.0,
                "PeerContribution": 0.0,
                "AffinityContribution": 0.0,
                "ClusterContribution": 0.0,
                "ClusterIndex": 0.0,
            }
        )

        for feature_name, importance in importances[:10]:
            feature_importance_rows.append(
                {
                    "RecordType": "feature_importance",
                    "Id": f"{family}-{feature_name}"[:255],
                    "AccountId": "",
                    "AccountName": "",
                    "OwnerName": "",
                    "UnitGroup": "",
                    "AccountType": "",
                    "Segment": "",
                    "Industry": "",
                    "IndustryVertical": "",
                    "DeliveryModel": "",
                    "IsSaaS": "",
                    "IsAxioma": "",
                    "RiskLevel": "",
                    "PartnerLevel": "",
                    "ProductFamily": family,
                    "ProductClassification": "",
                    "SourceProductFamily": "",
                    "ArchetypeLabel": "",
                    "TopRecommendationFamily": "",
                    "Reason1": "",
                    "Reason2": "",
                    "Reason3": "",
                    "TopFeature1": "",
                    "TopFeature2": "",
                    "TopFeature3": "",
                    "FeatureGroup": _clean_feature_name(feature_name)[:255],
                    "ModelName": "AttachEnsemble_v2",
                    "AUM": 0.0,
                    "EmployeeCount": 0.0,
                    "InstalledARR": 0.0,
                    "OpenExpansionARR": 0.0,
                    "OpenRenewalARR": 0.0,
                    "ProductCount": 0.0,
                    "AccountCount": 0.0,
                    "FamilyInstalledARR": 0.0,
                    "RecommendationScore": 0.0,
                    "PropensityScore": 0.0,
                    "PeerAttachRate": 0.0,
                    "AffinityScore": 0.0,
                    "Lift": 0.0,
                    "SupportCount": positives,
                    "RecommendationPotentialARR": 0.0,
                    "RankWithinAccount": 0.0,
                    "RecommendationCount": 0.0,
                    "HasRecommendation": 0.0,
                    "CVAUC": round(auc, 4),
                    "CVAvgPrecision": round(avg_precision, 4),
                    "RFAUC": round(rf_auc, 4),
                    "LRAUC": round(lr_auc, 4),
                    "RFAvgPrecision": round(rf_avg_precision, 4),
                    "LRAvgPrecision": round(lr_avg_precision, 4),
                    "PositiveAccounts": positives,
                    "CandidateAccounts": negatives,
                    "AdoptionRate": 0.0,
                    "ImportanceWeight": round(float(importance), 6),
                    "ModelContribution": 0.0,
                    "PeerContribution": 0.0,
                    "AffinityContribution": 0.0,
                    "ClusterContribution": 0.0,
                    "ClusterIndex": 0.0,
                }
            )

        family_avg_arr = safe_float(family_stats[family]["installed_arr"]) / max(positives, 1)
        target_index = list(feature_df.index)
        for account_id, propensity in zip(target_index, probabilities):
            if safe_float(y.loc[account_id]) >= 1.0:
                continue
            row = meta_by_account[account_id]
            peer_rate = _peer_attach_rate(
                row,
                family,
                family_prevalence,
                cohort_total,
                cohort_family,
                industry_family,
                total_accounts,
            )
            installed = row.get("InstalledFamilies", set()) if isinstance(row.get("InstalledFamilies"), set) else set()
            affinity_source = ""
            affinity_value = 0.0
            affinity_lift = 0.0
            for source_family in installed:
                pair = (source_family, family)
                score = affinity_strength.get(pair, 0.0)
                if score > affinity_value:
                    affinity_source = source_family
                    affinity_value = score
                    if pair in pair_details:
                        _, _, affinity_lift = pair_details[pair]
            cluster_rate = cluster_family_rates.get((str(row["ArchetypeLabel"]), family), family_prevalence.get(family, 0.0))
            model_contribution = 55.0 * float(propensity)
            peer_contribution = 20.0 * peer_rate
            affinity_contribution = 15.0 * min(1.0, affinity_value)
            cluster_contribution = 10.0 * cluster_rate
            recommendation_score = (
                model_contribution
                + peer_contribution
                + affinity_contribution
                + cluster_contribution
            )
            if recommendation_score < 18.0:
                continue
            whitespace_multiplier = 1.0
            if safe_float(row["ProductCount"]) <= 1:
                whitespace_multiplier += 0.25
            if safe_float(row["OpenExpansionARR"]) > 0:
                whitespace_multiplier += 0.10
            if str(row["RiskLevel"]) in {"High", "Critical"}:
                whitespace_multiplier -= 0.10
            potential_arr = (
                max(family_avg_arr, safe_float(row["InstalledARR"]) * 0.12, 25000.0)
                * (recommendation_score / 100.0)
                * whitespace_multiplier
            )
            reasons = _build_reasons(
                row,
                family,
                peer_rate,
                affinity_source,
                affinity_value,
                cluster_rate,
                float(propensity),
            )
            recommendation_candidates[account_id].append(
                {
                    "RecordType": "recommendation",
                    "Id": f"{account_id}-{family}"[:255],
                    "AccountId": account_id,
                    "AccountName": row["AccountName"],
                    "OwnerName": row["OwnerName"],
                    "UnitGroup": row["UnitGroup"],
                    "AccountType": row["AccountType"],
                    "Segment": row["Segment"],
                    "Industry": row["Industry"],
                    "IndustryVertical": row["IndustryVertical"],
                    "DeliveryModel": row["DeliveryModel"],
                    "IsSaaS": row["IsSaaS"],
                    "IsAxioma": row["IsAxioma"],
                    "RiskLevel": row["RiskLevel"],
                    "PartnerLevel": row["PartnerLevel"],
                    "ProductFamily": family,
                    "ProductClassification": (
                        max(
                            family_stats[family]["classifications"].items(),
                            key=lambda item: safe_float(item[1]),
                        )[0]
                        if family_stats[family]["classifications"]
                        else ""
                    )[:255],
                    "SourceProductFamily": affinity_source[:255],
                    "ArchetypeLabel": row["ArchetypeLabel"],
                    "TopRecommendationFamily": "",
                    "Reason1": reasons[0],
                    "Reason2": reasons[1],
                    "Reason3": reasons[2],
                    "TopFeature1": top_features[0][:255],
                    "TopFeature2": top_features[1][:255],
                    "TopFeature3": top_features[2][:255],
                    "FeatureGroup": "",
                    "ModelName": "AttachEnsemble_v2",
                    "AUM": round(safe_float(row["AUM"]), 2),
                    "EmployeeCount": round(safe_float(row["EmployeeCount"]), 2),
                    "InstalledARR": round(safe_float(row["InstalledARR"]), 2),
                    "OpenExpansionARR": round(safe_float(row["OpenExpansionARR"]), 2),
                    "OpenRenewalARR": round(safe_float(row["OpenRenewalARR"]), 2),
                    "ProductCount": safe_float(row["ProductCount"]),
                    "AccountCount": 0.0,
                    "FamilyInstalledARR": 0.0,
                    "RecommendationScore": round(recommendation_score, 2),
                    "PropensityScore": round(float(propensity), 4),
                    "PeerAttachRate": round(peer_rate, 4),
                    "AffinityScore": round(affinity_value, 4),
                    "Lift": round(affinity_lift, 4),
                    "SupportCount": positives,
                    "RecommendationPotentialARR": round(potential_arr, 2),
                    "RankWithinAccount": 0.0,
                    "RecommendationCount": 0.0,
                    "HasRecommendation": 1.0,
                    "CVAUC": round(auc, 4),
                    "CVAvgPrecision": round(avg_precision, 4),
                    "RFAUC": round(rf_auc, 4),
                    "LRAUC": round(lr_auc, 4),
                    "RFAvgPrecision": round(rf_avg_precision, 4),
                    "LRAvgPrecision": round(lr_avg_precision, 4),
                    "PositiveAccounts": positives,
                    "CandidateAccounts": negatives,
                    "AdoptionRate": family_prevalence.get(family, 0.0),
                    "ImportanceWeight": 0.0,
                    "ModelContribution": round(model_contribution, 2),
                    "PeerContribution": round(peer_contribution, 2),
                    "AffinityContribution": round(affinity_contribution, 2),
                    "ClusterContribution": round(cluster_contribution, 2),
                    "ClusterIndex": safe_float(row["ClusterIndex"]),
                }
            )

    recommendation_rows: list[dict[str, object]] = []
    for row in account_rows:
        account_id = str(row["AccountId"])
        ranked = sorted(
            recommendation_candidates.get(account_id, []),
            key=lambda candidate: (
                safe_float(candidate["RecommendationPotentialARR"]),
                safe_float(candidate["RecommendationScore"]),
            ),
            reverse=True,
        )[:3]
        row["RecommendationCount"] = len(ranked)
        row["HasRecommendation"] = 1 if ranked else 0
        if ranked:
            top = ranked[0]
            row["TopRecommendationFamily"] = top["ProductFamily"]
            row["RecommendationScore"] = round(safe_float(top["RecommendationScore"]), 2)
            row["RecommendationPotentialARR"] = round(
                safe_float(top["RecommendationPotentialARR"]), 2
            )
        for rank, candidate in enumerate(ranked, start=1):
            candidate["RankWithinAccount"] = rank
            candidate["RecommendationCount"] = len(ranked)
            recommendation_rows.append(candidate)

    return recommendation_rows, model_metric_rows, feature_importance_rows


def create_dataset(inst: str, tok: str) -> bool:
    """Build the product ML dataset."""
    print(f"\n=== Building {DS_LABEL} dataset ===")
    accounts = _soql(inst, tok, ML_ACCOUNT_SOQL)
    line_items = _soql(inst, tok, ML_LINE_ITEM_SOQL)
    print(f"  Queried {len(accounts)} accounts")
    print(f"  Queried {len(line_items)} opportunity line items")
    if not accounts or not line_items:
        raise RuntimeError("Product ML dashboard requires both accounts and opportunity line items.")

    account_rows, family_stats = _build_account_rows(accounts, line_items)
    feature_families, modeled_families = _family_lists(family_stats)
    print(f"  Account ML rows: {len(account_rows)}")
    print(f"  Feature families: {', '.join(feature_families[:8])}")
    print(f"  Modeled families: {', '.join(modeled_families[:8])}")
    if len(account_rows) < 20 or len(modeled_families) < 3:
        raise RuntimeError("Insufficient account/family coverage for product ML dashboard.")

    account_rows, cluster_family_rows, cluster_family_rates = _cluster_accounts(
        account_rows, feature_families
    )
    family_prevalence, pair_details, affinity_strength, affinity_rows = _pair_metrics(
        account_rows, modeled_families
    )
    cohort_total, cohort_family, industry_family = _cohort_rates(
        account_rows, modeled_families
    )
    recommendation_rows, model_metric_rows, feature_importance_rows = _model_rows(
        account_rows,
        modeled_families,
        feature_families,
        family_stats,
        family_prevalence,
        cohort_total,
        cohort_family,
        industry_family,
        pair_details,
        affinity_strength,
        cluster_family_rates,
    )

    account_cluster_rows: list[dict[str, object]] = []
    account_family_rows: list[dict[str, object]] = []
    for row in account_rows:
        account_cluster_rows.append(
            {
                "RecordType": "account_cluster",
                "Id": str(row["AccountId"]),
                "AccountId": str(row["AccountId"]),
                "AccountName": row["AccountName"],
                "OwnerName": row["OwnerName"],
                "UnitGroup": row["UnitGroup"],
                "AccountType": row["AccountType"],
                "Segment": row["Segment"],
                "Industry": row["Industry"],
                "IndustryVertical": row["IndustryVertical"],
                "DeliveryModel": row["DeliveryModel"],
                "IsSaaS": row["IsSaaS"],
                "IsAxioma": row["IsAxioma"],
                "RiskLevel": row["RiskLevel"],
                "PartnerLevel": row["PartnerLevel"],
                "ProductFamily": "",
                "ProductClassification": row["PrimaryClassification"][:255],
                "SourceProductFamily": "",
                "ArchetypeLabel": row["ArchetypeLabel"],
                "TopRecommendationFamily": str(row["TopRecommendationFamily"])[:255],
                "Reason1": "",
                "Reason2": "",
                "Reason3": "",
                "TopFeature1": "",
                "TopFeature2": "",
                "TopFeature3": "",
                "FeatureGroup": "",
                "ModelName": "",
                "AUM": round(safe_float(row["AUM"]), 2),
                "EmployeeCount": round(safe_float(row["EmployeeCount"]), 2),
                "InstalledARR": round(safe_float(row["InstalledARR"]), 2),
                "OpenExpansionARR": round(safe_float(row["OpenExpansionARR"]), 2),
                "OpenRenewalARR": round(safe_float(row["OpenRenewalARR"]), 2),
                "ProductCount": safe_float(row["ProductCount"]),
                "AccountCount": 1.0,
                "FamilyInstalledARR": 0.0,
                "RecommendationScore": round(safe_float(row["RecommendationScore"]), 2),
                "PropensityScore": 0.0,
                "PeerAttachRate": 0.0,
                "AffinityScore": 0.0,
                "Lift": 0.0,
                "SupportCount": 0.0,
                "RecommendationPotentialARR": round(
                    safe_float(row["RecommendationPotentialARR"]), 2
                ),
                "RankWithinAccount": 0.0,
                "RecommendationCount": safe_float(row["RecommendationCount"]),
                "HasRecommendation": safe_float(row["HasRecommendation"]),
                "ClusterPCA1": round(safe_float(row["ClusterPCA1"]), 6),
                "ClusterPCA2": round(safe_float(row["ClusterPCA2"]), 6),
                "CVAUC": 0.0,
                "CVAvgPrecision": 0.0,
                "PositiveAccounts": 0.0,
                "CandidateAccounts": 0.0,
                "AdoptionRate": 0.0,
                "ImportanceWeight": 0.0,
                "ClusterIndex": safe_float(row["ClusterIndex"]),
            }
        )
        installed_family_arr = row.get("InstalledFamilyARR", {})
        installed = sorted(
            family for family in (row.get("InstalledFamilies", set()) if isinstance(row.get("InstalledFamilies"), set) else set())
            if family in feature_families[:MAX_CLUSTER_FAMILIES]
        )
        for family in installed:
            account_family_rows.append(
                {
                    "RecordType": "account_family",
                    "Id": f"{row['AccountId']}-{family}"[:255],
                    "AccountId": str(row["AccountId"]),
                    "AccountName": row["AccountName"],
                    "OwnerName": row["OwnerName"],
                    "UnitGroup": row["UnitGroup"],
                    "AccountType": row["AccountType"],
                    "Segment": row["Segment"],
                    "Industry": row["Industry"],
                    "IndustryVertical": row["IndustryVertical"],
                    "DeliveryModel": row["DeliveryModel"],
                    "IsSaaS": row["IsSaaS"],
                    "IsAxioma": row["IsAxioma"],
                    "RiskLevel": row["RiskLevel"],
                    "PartnerLevel": row["PartnerLevel"],
                    "ProductFamily": family,
                    "ProductClassification": row["PrimaryClassification"][:255],
                    "SourceProductFamily": "",
                    "ArchetypeLabel": row["ArchetypeLabel"],
                    "TopRecommendationFamily": str(row["TopRecommendationFamily"])[:255],
                    "Reason1": "",
                    "Reason2": "",
                    "Reason3": "",
                    "TopFeature1": "",
                    "TopFeature2": "",
                    "TopFeature3": "",
                    "FeatureGroup": "",
                    "ModelName": "",
                    "AUM": round(safe_float(row["AUM"]), 2),
                    "EmployeeCount": round(safe_float(row["EmployeeCount"]), 2),
                    "InstalledARR": round(safe_float(row["InstalledARR"]), 2),
                    "OpenExpansionARR": round(safe_float(row["OpenExpansionARR"]), 2),
                    "OpenRenewalARR": round(safe_float(row["OpenRenewalARR"]), 2),
                    "ProductCount": safe_float(row["ProductCount"]),
                    "AccountCount": 1.0,
                    "FamilyInstalledARR": round(
                        safe_float(installed_family_arr.get(family)), 2
                    ),
                    "RecommendationScore": round(safe_float(row["RecommendationScore"]), 2),
                    "PropensityScore": 0.0,
                    "PeerAttachRate": 0.0,
                    "AffinityScore": 0.0,
                    "Lift": 0.0,
                    "SupportCount": 0.0,
                    "RecommendationPotentialARR": round(
                        safe_float(row["RecommendationPotentialARR"]), 2
                    ),
                    "RankWithinAccount": 0.0,
                    "RecommendationCount": safe_float(row["RecommendationCount"]),
                    "HasRecommendation": safe_float(row["HasRecommendation"]),
                    "ClusterPCA1": round(safe_float(row["ClusterPCA1"]), 6),
                    "ClusterPCA2": round(safe_float(row["ClusterPCA2"]), 6),
                    "CVAUC": 0.0,
                    "CVAvgPrecision": 0.0,
                    "PositiveAccounts": 0.0,
                    "CandidateAccounts": 0.0,
                    "AdoptionRate": 1.0,
                    "ImportanceWeight": 0.0,
                    "ClusterIndex": safe_float(row["ClusterIndex"]),
                }
            )

    rows = (
        account_cluster_rows
        + account_family_rows
        + cluster_family_rows
        + recommendation_rows
        + model_metric_rows
        + feature_importance_rows
        + affinity_rows
    )
    for row in rows:
        row.setdefault("ClusterPCA1", 0.0)
        row.setdefault("ClusterPCA2", 0.0)
    print(f"  Account cluster rows: {len(account_cluster_rows)}")
    print(f"  Account family rows: {len(account_family_rows)}")
    print(f"  Cluster family rows: {len(cluster_family_rows)}")
    print(f"  Recommendation rows: {len(recommendation_rows)}")
    print(f"  Model metric rows: {len(model_metric_rows)}")
    print(f"  Feature importance rows: {len(feature_importance_rows)}")
    print(f"  Affinity rows: {len(affinity_rows)}")
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
        "IndustryVertical",
        "DeliveryModel",
        "IsSaaS",
        "IsAxioma",
        "RiskLevel",
        "PartnerLevel",
        "ProductFamily",
        "ProductClassification",
        "SourceProductFamily",
        "ArchetypeLabel",
        "TopRecommendationFamily",
        "Reason1",
        "Reason2",
        "Reason3",
        "TopFeature1",
        "TopFeature2",
        "TopFeature3",
        "FeatureGroup",
        "ModelName",
        "AUM",
        "EmployeeCount",
        "InstalledARR",
        "OpenExpansionARR",
        "OpenRenewalARR",
        "ProductCount",
        "AccountCount",
        "FamilyInstalledARR",
        "RecommendationScore",
        "PropensityScore",
        "PeerAttachRate",
        "AffinityScore",
        "Lift",
        "SupportCount",
        "RecommendationPotentialARR",
        "RankWithinAccount",
        "RecommendationCount",
        "HasRecommendation",
        "ClusterPCA1",
        "ClusterPCA2",
        "CVAUC",
        "CVAvgPrecision",
        "RFAUC",
        "LRAUC",
        "RFAvgPrecision",
        "LRAvgPrecision",
        "PositiveAccounts",
        "CandidateAccounts",
        "AdoptionRate",
        "ImportanceWeight",
        "ModelContribution",
        "PeerContribution",
        "AffinityContribution",
        "ClusterContribution",
        "ClusterIndex",
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
        _dim("IndustryVertical", "Industry Vertical"),
        _dim("DeliveryModel", "Delivery Model"),
        _dim("IsSaaS", "SaaS Client"),
        _dim("IsAxioma", "Axioma Client"),
        _dim("RiskLevel", "Risk Level"),
        _dim("PartnerLevel", "Partner Level"),
        _dim("ProductFamily", "Product Family"),
        _dim("ProductClassification", "Product Classification"),
        _dim("SourceProductFamily", "Source Product Family"),
        _dim("ArchetypeLabel", "Product Archetype"),
        _dim("TopRecommendationFamily", "Top Recommendation"),
        _dim("Reason1", "Reason 1"),
        _dim("Reason2", "Reason 2"),
        _dim("Reason3", "Reason 3"),
        _dim("TopFeature1", "Top Feature 1"),
        _dim("TopFeature2", "Top Feature 2"),
        _dim("TopFeature3", "Top Feature 3"),
        _dim("FeatureGroup", "Feature"),
        _dim("ModelName", "Model"),
        _measure("AUM", "AuM", scale=2, precision=18),
        _measure("EmployeeCount", "Employees", scale=0, precision=18),
        _measure("InstalledARR", "Installed ARR", scale=2, precision=18),
        _measure("OpenExpansionARR", "Open Expansion ARR", scale=2, precision=18),
        _measure("OpenRenewalARR", "Open Renewal ARR", scale=2, precision=18),
        _measure("ProductCount", "Installed Product Count", scale=0, precision=18),
        _measure("AccountCount", "Account Count", scale=0, precision=18),
        _measure("FamilyInstalledARR", "Family Installed ARR", scale=2, precision=18),
        _measure("RecommendationScore", "Recommendation Score", scale=2, precision=18),
        _measure("PropensityScore", "Propensity Score", scale=4, precision=18),
        _measure("PeerAttachRate", "Peer Attach Rate", scale=4, precision=18),
        _measure("AffinityScore", "Affinity Score", scale=4, precision=18),
        _measure("Lift", "Lift", scale=4, precision=18),
        _measure("SupportCount", "Support Count", scale=0, precision=18),
        _measure("RecommendationPotentialARR", "Recommendation Potential ARR", scale=2, precision=18),
        _measure("RankWithinAccount", "Recommendation Rank", scale=0, precision=18),
        _measure("RecommendationCount", "Recommendation Count", scale=0, precision=18),
        _measure("HasRecommendation", "Has Recommendation", scale=0, precision=18),
        _measure("ClusterPCA1", "Cluster PCA 1", scale=6, precision=18),
        _measure("ClusterPCA2", "Cluster PCA 2", scale=6, precision=18),
        _measure("CVAUC", "CV ROC AUC", scale=4, precision=18),
        _measure("CVAvgPrecision", "CV Average Precision", scale=4, precision=18),
        _measure("RFAUC", "RF ROC AUC", scale=4, precision=18),
        _measure("LRAUC", "LR ROC AUC", scale=4, precision=18),
        _measure("RFAvgPrecision", "RF Avg Precision", scale=4, precision=18),
        _measure("LRAvgPrecision", "LR Avg Precision", scale=4, precision=18),
        _measure("PositiveAccounts", "Positive Accounts", scale=0, precision=18),
        _measure("CandidateAccounts", "Candidate Accounts", scale=0, precision=18),
        _measure("AdoptionRate", "Adoption Rate", scale=4, precision=18),
        _measure("ImportanceWeight", "Importance Weight", scale=6, precision=18),
        _measure("ModelContribution", "Model Contribution", scale=2, precision=18),
        _measure("PeerContribution", "Peer Contribution", scale=2, precision=18),
        _measure("AffinityContribution", "Affinity Contribution", scale=2, precision=18),
        _measure("ClusterContribution", "Cluster Contribution", scale=2, precision=18),
        _measure("ClusterIndex", "Cluster Index", scale=0, precision=18),
    ]
    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


def build_steps(ds_id: str) -> dict[str, dict]:
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    filter_industry = coalesce_filter("f_industry", "IndustryVertical")
    filter_segment = coalesce_filter("f_segment", "Segment")
    filter_unit = coalesce_filter("f_unit", "UnitGroup")
    filter_cluster = coalesce_filter("f_cluster", "ArchetypeLabel")

    account_cluster = (
        load
        + 'q = filter q by RecordType == "account_cluster";\n'
        + filter_industry
        + filter_segment
        + filter_unit
        + filter_cluster
    )
    recommendation = (
        load
        + 'q = filter q by RecordType == "recommendation";\n'
        + filter_industry
        + filter_segment
        + filter_unit
        + filter_cluster
    )
    cluster_family = load + 'q = filter q by RecordType == "cluster_family";\n'
    model_metric = load + 'q = filter q by RecordType == "model_metric";\n'
    feature_importance = load + 'q = filter q by RecordType == "feature_importance";\n'
    family_affinity = load + 'q = filter q by RecordType == "family_affinity";\n'

    return {
        "f_industry": af("IndustryVertical", ds_meta),
        "f_segment": af("Segment", ds_meta),
        "f_unit": af("UnitGroup", ds_meta),
        "f_cluster": af("ArchetypeLabel", ds_meta),
        "s_summary": sq(
            account_cluster
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(AccountCount) as account_count, "
            + "sum(HasRecommendation) as rec_accounts, "
            + "sum(RecommendationPotentialARR) as potential_arr, "
            + "avg(RecommendationScore) as avg_score;"
        ),
        "s_model_overview": sq(
            model_metric
            + "q = group q by all;\n"
            + "q = foreach q generate count() as model_count, avg(CVAUC) as avg_auc;"
        ),
        "s_cluster_embedding": sq(
            account_cluster
            + 'q = filter q by ArchetypeLabel != "";\n'
            + "q = foreach q generate ClusterPCA1, ClusterPCA2, InstalledARR, ArchetypeLabel;\n"
            + "q = order q by InstalledARR desc;\n"
            + "q = limit q 250;"
        ),
        "s_cluster_scatter": sq(
            account_cluster
            + 'q = filter q by ArchetypeLabel != "";\n'
            + "q = foreach q generate ClusterPCA1, ClusterPCA2, ArchetypeLabel;\n"
            + "q = limit q 350;"
        ),
        "s_cluster_mix": sq(
            account_cluster
            + 'q = filter q by ArchetypeLabel != "";\n'
            + "q = group q by ArchetypeLabel;\n"
            + "q = foreach q generate ArchetypeLabel, count() as AccountCount, avg(InstalledARR) as InstalledARR;\n"
            + "q = order q by AccountCount desc;"
        ),
        "s_family_compare": sq(
            recommendation
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, "
            + "sum(RecommendationPotentialARR) as PotentialARR, "
            + "avg(RecommendationScore) as RecommendationScore, "
            + "count() as AccountCount;\n"
            + "q = order q by PotentialARR desc;\n"
            + "q = limit q 15;"
        ),
        "s_family_bubble": sq(
            recommendation
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate avg(RecommendationScore) as RecommendationScore, "
            + "sum(RecommendationPotentialARR) as PotentialARR, count() as AccountCount, ProductFamily;\n"
            + "q = order q by PotentialARR desc;\n"
            + "q = limit q 15;"
        ),
        "s_rec_family": sq(
            recommendation
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, sum(RecommendationPotentialARR) as PotentialARR, avg(RecommendationScore) as RecommendationScore;\n"
            + "q = order q by PotentialARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_score_heatmap": sq(
            recommendation
            + "model = group q by ProductFamily;\n"
            + 'model = foreach model generate ProductFamily, "Model" as ScoreSignal, avg(ModelContribution) as Contribution;\n'
            + "peer = group q by ProductFamily;\n"
            + 'peer = foreach peer generate ProductFamily, "Peer" as ScoreSignal, avg(PeerContribution) as Contribution;\n'
            + "aff = group q by ProductFamily;\n"
            + 'aff = foreach aff generate ProductFamily, "Affinity" as ScoreSignal, avg(AffinityContribution) as Contribution;\n'
            + "cluster = group q by ProductFamily;\n"
            + 'cluster = foreach cluster generate ProductFamily, "Cluster" as ScoreSignal, avg(ClusterContribution) as Contribution;\n'
            + "q = union model, peer, aff, cluster;\n"
            + "q = order q by Contribution desc;"
        ),
        "s_score_decomp": sq(
            recommendation
            + "q = group q by ProductFamily;\n"
            + "q = foreach q generate ProductFamily, "
            + "avg(ModelContribution) as ModelContribution, "
            + "avg(PeerContribution) as PeerContribution, "
            + "avg(AffinityContribution) as AffinityContribution, "
            + "avg(ClusterContribution) as ClusterContribution;\n"
            + "q = order q by ModelContribution desc;\n"
            + "q = limit q 12;"
        ),
        "s_industry_family": sq(
            recommendation
            + 'q = filter q by IndustryVertical != "Other";\n'
            + "q = group q by (IndustryVertical, ProductFamily);\n"
            + "q = foreach q generate IndustryVertical, ProductFamily, sum(RecommendationPotentialARR) as PotentialARR;\n"
            + "q = order q by PotentialARR desc;"
        ),
        "s_top_recs": sq(
            recommendation
            + "q = filter q by RankWithinAccount == 1;\n"
            + "q = foreach q generate AccountName, OwnerName, ProductFamily, ArchetypeLabel, RecommendationScore, RecommendationPotentialARR, AccountId;\n"
            + "q = order q by RecommendationPotentialARR desc;\n"
            + "q = limit q 25;"
        ),
        "s_cluster_profile": sq(
            account_cluster
            + 'q = filter q by ArchetypeLabel != "";\n'
            + "q = group q by ArchetypeLabel;\n"
            + "q = foreach q generate avg(ProductCount) as ProductCount, avg(InstalledARR) as InstalledARR, sum(AccountCount) as AccountCount, ArchetypeLabel;\n"
            + "q = order q by AccountCount desc;"
        ),
        "s_cluster_family": sq(
            cluster_family
            + "q = group q by (ArchetypeLabel, ProductFamily);\n"
            + "q = foreach q generate ArchetypeLabel, ProductFamily, max(AdoptionRate) as AdoptionRate;\n"
            + "q = order q by AdoptionRate desc;"
        ),
        "s_cluster_industry": sq(
            account_cluster
            + 'q = filter q by IndustryVertical != "Other";\n'
            + "q = group q by (ArchetypeLabel, IndustryVertical);\n"
            + "q = foreach q generate ArchetypeLabel, IndustryVertical, count() as AccountCount;\n"
            + "q = order q by ArchetypeLabel asc;"
        ),
        "s_cluster_accounts": sq(
            account_cluster
            + "q = foreach q generate AccountName, OwnerName, ArchetypeLabel, InstalledARR, RecommendationPotentialARR, TopRecommendationFamily, AccountId;\n"
            + "q = order q by RecommendationPotentialARR desc;\n"
            + "q = limit q 20;"
        ),
        "s_affinity_flow": sq(
            family_affinity
            + "q = foreach q generate SourceProductFamily as source, ProductFamily as target, RecommendationPotentialARR as flow;\n"
            + "q = order q by flow desc;\n"
            + "q = limit q 20;"
        ),
        "s_affinity_heatmap": sq(
            family_affinity
            + "q = group q by (SourceProductFamily, ProductFamily);\n"
            + "q = foreach q generate SourceProductFamily, ProductFamily, max(Lift) as Lift;\n"
            + "q = order q by Lift desc;\n"
            + "q = limit q 25;"
        ),
        "s_rec_scatter": sq(
            recommendation
            + "q = foreach q generate PropensityScore, RecommendationPotentialARR, RecommendationScore, ProductFamily;\n"
            + "q = order q by RecommendationPotentialARR desc;\n"
            + "q = limit q 40;"
        ),
        "s_recommendation_table": sq(
            recommendation
            + "q = foreach q generate AccountName, OwnerName, ProductFamily, Reason1, PropensityScore, PeerAttachRate, Lift, RecommendationPotentialARR, AccountId;\n"
            + "q = order q by RecommendationPotentialARR desc;\n"
            + "q = limit q 30;"
        ),
        "s_model_scatter": sq(
            model_metric
            + "q = foreach q generate CVAUC, CVAvgPrecision, PositiveAccounts, ProductFamily;\n"
            + "q = order q by CVAUC desc;"
        ),
        "s_global_features": sq(
            feature_importance
            + "q = group q by FeatureGroup;\n"
            + "q = foreach q generate FeatureGroup, sum(ImportanceWeight) as ImportanceWeight;\n"
            + "q = order q by ImportanceWeight desc;\n"
            + "q = limit q 15;"
        ),
        "s_feature_matrix": sq(
            feature_importance
            + "q = group q by (ProductFamily, FeatureGroup);\n"
            + "q = foreach q generate ProductFamily, FeatureGroup, sum(ImportanceWeight) as ImportanceWeight;\n"
            + "q = order q by ImportanceWeight desc;\n"
            + "q = limit q 75;"
        ),
        "s_model_table": sq(
            model_metric
            + "q = foreach q generate ProductFamily, CVAUC, RFAUC, LRAUC, CVAvgPrecision, PositiveAccounts, TopFeature1, TopFeature2;\n"
            + "q = order q by CVAUC desc;"
        ),
        "s_affinity_table": sq(
            family_affinity
            + "q = foreach q generate SourceProductFamily, ProductFamily, AffinityScore, Lift, SupportCount;\n"
            + "q = order q by Lift desc;\n"
            + "q = limit q 20;"
        ),
    }


def build_widgets() -> dict[str, dict]:
    widgets = {
        "p1_nav1": nav_link("summary", "Summary", active=True),
        "p1_nav2": nav_link("archetypes", "K-Means"),
        "p1_nav3": nav_link("recommendations", "Recommendations"),
        "p1_nav4": nav_link("qa", "Model QA"),
        "p1_hdr": hdr(
            "Product ML & Recommendations",
            "K-means account archetypes, ensemble attach scoring, cross-sell affinity, and a clearer next-best-product operating layer.",
        ),
        "p1_f_industry": pillbox("f_industry", "Industry"),
        "p1_f_segment": pillbox("f_segment", "Segment"),
        "p1_f_unit": pillbox("f_unit", "Unit Group"),
        "p1_f_cluster": pillbox("f_cluster", "Archetype"),
        "p1_n_accounts": num("s_summary", "account_count", "Accounts Scored", "#032D60", compact=True),
        "p1_n_rec_accounts": num("s_summary", "rec_accounts", "Accounts With Recs", "#2E844A", compact=True),
        "p1_n_models": num("s_model_overview", "model_count", "Modeled Families", "#0176D3", compact=True),
        "p1_n_potential": num("s_summary", "potential_arr", "Top Recommendation Potential ARR", "#9050E9", compact=True),
        "p1_n_score": num("s_summary", "avg_score", "Avg Top Recommendation Score", "#BA0517", compact=True),
        "p1_ch_cluster": rich_chart(
            "s_cluster_mix",
            "comparisontable",
            "K-Means Archetype Snapshot",
            ["ArchetypeLabel"],
            ["AccountCount", "InstalledARR"],
            show_legend=False,
        ),
        "p1_ch_family": rich_chart(
            "s_family_compare",
            "comparisontable",
            "Recommendation Opportunity by Product Family",
            ["ProductFamily"],
            ["PotentialARR", "RecommendationScore", "AccountCount"],
            show_legend=False,
        ),
        "p1_ch_heatmap": heatmap_chart(
            "s_industry_family",
            "Recommendation Potential by Industry x Product Family",
            show_legend=True,
        ),
        "p1_tbl_recs": rich_chart(
            "s_top_recs",
            "comparisontable",
            "Top Account Recommendations",
            ["AccountName", "OwnerName", "ProductFamily", "ArchetypeLabel"],
            ["RecommendationScore", "RecommendationPotentialARR"],
            show_legend=False,
        ),
        "p2_nav1": nav_link("summary", "Summary"),
        "p2_nav2": nav_link("archetypes", "K-Means", active=True),
        "p2_nav3": nav_link("recommendations", "Recommendations"),
        "p2_nav4": nav_link("qa", "Model QA"),
        "p2_hdr": hdr(
            "K-Means Product Archetypes",
            "These views make the clustering explicit: where accounts sit in cluster space, what each cluster installs, and which industries dominate each archetype.",
        ),
        "p2_f_industry": pillbox("f_industry", "Industry"),
        "p2_f_segment": pillbox("f_segment", "Segment"),
        "p2_f_unit": pillbox("f_unit", "Unit Group"),
        "p2_f_cluster": pillbox("f_cluster", "Archetype"),
        "p2_ch_profile": scatter_chart(
            "s_cluster_scatter",
            "K-Means Cluster Separation",
            x_title="Principal Component 1",
            y_title="Principal Component 2",
            show_legend=True,
        ),
        "p2_ch_family": heatmap_chart(
            "s_cluster_family",
            "Archetype x Product Family Adoption Rate",
            show_legend=True,
        ),
        "p2_ch_industry": heatmap_chart(
            "s_cluster_industry",
            "Industry x K-Means Archetype Concentration",
            show_legend=True,
        ),
        "p2_tbl_accounts": rich_chart(
            "s_cluster_accounts",
            "comparisontable",
            "Top Accounts by Archetype Potential",
            ["AccountName", "OwnerName", "ArchetypeLabel", "TopRecommendationFamily"],
            ["InstalledARR", "RecommendationPotentialARR"],
            show_legend=False,
        ),
        "p3_nav1": nav_link("summary", "Summary"),
        "p3_nav2": nav_link("archetypes", "K-Means"),
        "p3_nav3": nav_link("recommendations", "Recommendations", active=True),
        "p3_nav4": nav_link("qa", "Model QA"),
        "p3_hdr": hdr(
            "Recommendations",
            "Recommendations are now shown as signal systems: propensity, peer fit, installed-family affinity, and account-level opportunity sizing.",
        ),
        "p3_f_industry": pillbox("f_industry", "Industry"),
        "p3_f_segment": pillbox("f_segment", "Segment"),
        "p3_f_unit": pillbox("f_unit", "Unit Group"),
        "p3_f_cluster": pillbox("f_cluster", "Archetype"),
        "p3_ch_family": heatmap_chart(
            "s_score_heatmap",
            "Recommendation Signal Mix by Product Family",
            show_legend=True,
        ),
        "p3_ch_affinity": sankey_chart(
            "s_affinity_flow",
            "Installed to Recommended Family Flow",
            "source",
            "target",
            "flow",
        ),
        "p3_ch_scatter": bubble_chart(
            "s_rec_scatter",
            "Ensemble Propensity vs Potential ARR",
            show_legend=True,
        ),
        "p3_tbl_recommendations": rich_chart(
            "s_recommendation_table",
            "comparisontable",
            "Recommendation Action Queue",
            ["AccountName", "OwnerName", "ProductFamily", "Reason1"],
            ["PropensityScore", "PeerAttachRate", "Lift", "RecommendationPotentialARR"],
            show_legend=False,
        ),
        "p4_nav1": nav_link("summary", "Summary"),
        "p4_nav2": nav_link("archetypes", "K-Means"),
        "p4_nav3": nav_link("recommendations", "Recommendations"),
        "p4_nav4": nav_link("qa", "Model QA", active=True),
        "p4_hdr": hdr(
            "Model QA",
            "Cross-validated ensemble quality, feature-driver matrices, and affinity diagnostics so the recommender can be challenged, not just consumed.",
        ),
        "p4_ch_model": bubble_chart(
            "s_model_scatter",
            "Family Model Quality: ROC AUC vs Avg Precision",
            show_legend=True,
        ),
        "p4_ch_features": heatmap_chart(
            "s_feature_matrix",
            "Feature Importance by Product Family",
            show_legend=True,
        ),
        "p4_tbl_models": rich_chart(
            "s_model_table",
            "comparisontable",
            "Ensemble vs Base Model Metrics",
            ["ProductFamily", "TopFeature1", "TopFeature2"],
            ["CVAUC", "RFAUC", "LRAUC", "CVAvgPrecision", "PositiveAccounts"],
            show_legend=False,
        ),
        "p4_tbl_affinity": rich_chart(
            "s_affinity_table",
            "comparisontable",
            "Affinity Diagnostic Pairs",
            ["SourceProductFamily", "ProductFamily"],
            ["AffinityScore", "Lift", "SupportCount"],
            show_legend=False,
        ),
    }
    add_table_action(widgets["p1_tbl_recs"], "salesforceActions", "Account", "AccountId")
    add_table_action(widgets["p2_tbl_accounts"], "salesforceActions", "Account", "AccountId")
    add_table_action(widgets["p3_tbl_recommendations"], "salesforceActions", "Account", "AccountId")
    return widgets


def build_layout() -> dict:
    p1 = nav_row("p1", 4) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_industry", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_segment", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_unit", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_cluster", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p1_n_accounts", "row": 5, "column": 0, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_rec_accounts", "row": 5, "column": 2, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_models", "row": 5, "column": 4, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_potential", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_n_score", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p1_ch_cluster", "row": 9, "column": 0, "colspan": 4, "rowspan": 7},
        {"name": "p1_ch_family", "row": 9, "column": 4, "colspan": 4, "rowspan": 7},
        {"name": "p1_ch_heatmap", "row": 9, "column": 8, "colspan": 4, "rowspan": 7},
        {"name": "p1_tbl_recs", "row": 16, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    p2 = nav_row("p2", 4) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_industry", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_segment", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_unit", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_cluster", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p2_ch_profile", "row": 5, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_family", "row": 5, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_industry", "row": 13, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_tbl_accounts", "row": 13, "column": 6, "colspan": 6, "rowspan": 8},
    ]

    p3 = nav_row("p3", 4) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_industry", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_segment", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_unit", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_cluster", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        {"name": "p3_ch_family", "row": 5, "column": 0, "colspan": 4, "rowspan": 8},
        {"name": "p3_ch_affinity", "row": 5, "column": 4, "colspan": 4, "rowspan": 8},
        {"name": "p3_ch_scatter", "row": 5, "column": 8, "colspan": 4, "rowspan": 8},
        {"name": "p3_tbl_recommendations", "row": 13, "column": 0, "colspan": 12, "rowspan": 9},
    ]

    p4 = nav_row("p4", 4) + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_ch_model", "row": 3, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p4_ch_features", "row": 3, "column": 6, "colspan": 6, "rowspan": 8},
        {"name": "p4_tbl_models", "row": 11, "column": 0, "colspan": 12, "rowspan": 7},
        {"name": "p4_tbl_affinity", "row": 18, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    return {
        "name": "ProductMLRecommendations",
        "numColumns": 12,
        "pages": [
            pg("summary", "Summary", p1),
            pg("archetypes", "K-Means", p2),
            pg("recommendations", "Recommendations", p3),
            pg("qa", "Model QA", p4),
        ],
    }


def main() -> None:
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
        [{"field": "AccountName", "id_field": "AccountId", "label": "Account"}],
    )


if __name__ == "__main__":
    main()
