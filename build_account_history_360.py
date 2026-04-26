#!/usr/bin/env python3
"""Build the Account 360 / Account History dashboard.

This dashboard is intentionally account-level:
- Page 1 tells the ARR story over time
- Page 2 shows product contribution + event ledger

Data model:
- account/year metrics for chronology
- opportunity events for classification
- quote line items only as supporting product evidence when present
"""

from __future__ import annotations

import csv
import io
from collections import defaultdict
from datetime import date, datetime, timedelta

from crm_analytics_helpers import (
    _date,
    _dim,
    _measure,
    _soql,
    af,
    build_dashboard_state,
    compare_table,
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    hdr,
    nav_link,
    nav_row,
    num,
    pg,
    pillbox,
    sankey_chart,
    section_label,
    set_record_links_xmd,
    sq,
    timeline_chart,
    upload_dataset,
    waterfall_chart,
    coalesce_filter,
)
from commercial_operating_model import ownership_alignment, primary_motion_persona, role_dimension_row

DS = "Account_History_360"
DS_LABEL = "Account History 360"
DASHBOARD_LABEL = "Account 360 & History"
DEFAULT_YEAR = "2025"
LEGACY_CUTOFF_YEAR = 2018

RENEWALISH_TOKENS = (
    "renewal",
    "extension",
    "opt out",
    "continuous testing",
    "arr increase",
    "contract",
    "prolong",
    "swap",
    "support",
)

OPP_SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, Account.OwnerId, Account.Owner.Name, "
    "Account.Owner.Title, Account.Owner.Department, Account.Owner.Division, Account.Owner.UserRole.Name, "
    "Account.Owner.ManagerId, Account.Owner.Manager.Name, Type, StageName, IsClosed, IsWon, CloseDate, "
    "CreatedDate, Amount, ForecastCategoryName, "
    "APTS_Opportunity_ARR__c, APTS_Renewal_ACV__c, "
    "APTS_Primary_Quote_Type__c, APTS_Opportunity_Sub_Type__c, "
    "APTS_RH_Product_Family__c, APTS_Primary_Quote__c, APTS_Primary_Quote__r.Name, "
    "APTS_Contract_Start_Date__c, APTS_Contract_End_Date__c "
    "FROM Opportunity "
    "WHERE Type IN ('Land','Expand','Renewal') "
    "AND (CloseDate >= 2022-01-01 OR APTS_Opportunity_ARR__c > 0 OR APTS_Renewal_ACV__c > 0) "
    "ORDER BY AccountId, CloseDate"
)

LINE_SOQL = (
    "SELECT Id, Apttus_Proposal__Proposal__r.Name, "
    "Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__c, "
    "Apttus_Proposal__Product__r.Name, APTS_Product_Area__c, "
    "APTS_Strategic_Product__c, APTS_Net_Product_Price__c, "
    "Apttus_QPConfig__NetPrice__c, Apttus_QPConfig__StartDate__c, "
    "Apttus_QPConfig__EndDate__c "
    "FROM Apttus_Proposal__Proposal_Line_Item__c "
    "WHERE Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.Type "
    "IN ('Land','Expand','Renewal') "
    "AND (Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.CloseDate >= 2022-01-01 "
    "OR Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.APTS_Opportunity_ARR__c > 0 "
    "OR Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__r.APTS_Renewal_ACV__c > 0) "
    "ORDER BY Apttus_Proposal__Proposal__r.Apttus_Proposal__Opportunity__c"
)


def safe_float(value) -> float:
    try:
        return float(value) if value not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def iso(value: date | None) -> str:
    return value.isoformat() if value else ""


def split_multi(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(";") if part.strip()]


def year_start(year: int) -> date:
    return date(year, 1, 1)


def year_end(year: int) -> date:
    return date(year, 12, 31)


def days_between(d1: date | None, d2: date | None) -> int:
    if not d1 or not d2:
        return 10_000
    return abs((d2 - d1).days)


def event_value(opp: dict) -> float:
    opp_type = opp.get("Type") or ""
    if opp_type == "Renewal":
        return safe_float(opp.get("APTS_Renewal_ACV__c")) or safe_float(
            opp.get("APTS_Opportunity_ARR__c")
        )
    return safe_float(opp.get("APTS_Opportunity_ARR__c"))


def classify_loss(stage_name: str) -> str:
    if "No Opportunity" in (stage_name or ""):
        return "No Opportunity"
    if "Lost" in (stage_name or ""):
        return "Lost"
    return "Closed Not Won"


def renewalish_name(name: str) -> bool:
    lower = (name or "").lower()
    return any(token in lower for token in RENEWALISH_TOKENS)


def build_quote_line_map(lines: list[dict]) -> dict[str, dict]:
    by_opp: dict[str, dict] = {}
    for line in lines:
        opp_id = ((line.get("Apttus_Proposal__Proposal__r") or {}).get("Apttus_Proposal__Opportunity__c")) or ""
        if not opp_id:
            continue
        bucket = by_opp.setdefault(
            opp_id,
            {
                "start": None,
                "end": None,
                "areas": defaultdict(float),
                "products": defaultdict(set),
                "quote_names": set(),
            },
        )
        start = parse_date(line.get("Apttus_QPConfig__StartDate__c"))
        end = parse_date(line.get("Apttus_QPConfig__EndDate__c"))
        if start and (bucket["start"] is None or start < bucket["start"]):
            bucket["start"] = start
        if end and (bucket["end"] is None or end > bucket["end"]):
            bucket["end"] = end
        area = line.get("APTS_Product_Area__c") or "Unknown"
        price = safe_float(line.get("APTS_Net_Product_Price__c")) or safe_float(
            line.get("Apttus_QPConfig__NetPrice__c")
        )
        bucket["areas"][area] += price
        product_name = ((line.get("Apttus_Proposal__Product__r") or {}).get("Name")) or ""
        if product_name:
            bucket["products"][area].add(product_name)
        quote_name = ((line.get("Apttus_Proposal__Proposal__r") or {}).get("Name")) or ""
        if quote_name:
            bucket["quote_names"].add(quote_name)
    return by_opp


def derive_product_payload(opp: dict, quote_info: dict | None) -> tuple[list[str], str, str, str]:
    opp_groups = split_multi(opp.get("APTS_RH_Product_Family__c"))
    if opp_groups:
        confidence = "High" if quote_info and quote_info.get("areas") else "Medium"
        note = "Opportunity product family" + (
            " + quote line support" if quote_info and quote_info.get("areas") else ""
        )
        return opp_groups, "Opportunity Family", confidence, note

    if quote_info and quote_info.get("areas"):
        groups = sorted([g for g in quote_info["areas"].keys() if g])
        return groups, "Quote Product Area", "Medium", "Quote product area fallback"

    return ["Unspecified"], "None", "Low", "No product evidence"


def derive_contract_span(opp: dict, quote_info: dict | None) -> tuple[date | None, date | None]:
    close_dt = parse_date(opp.get("CloseDate"))
    start = parse_date(opp.get("APTS_Contract_Start_Date__c")) or (
        quote_info.get("start") if quote_info else None
    )
    end = parse_date(opp.get("APTS_Contract_End_Date__c")) or (
        quote_info.get("end") if quote_info else None
    )

    if not start:
        start = close_dt
    if not end and close_dt:
        end = date(close_dt.year, 12, 31)
    if start and end and end < start:
        end = start
    return start, end


def active_on(opp_record: dict, point: date) -> bool:
    if not opp_record["IsWon"] or opp_record["EventValue"] <= 0:
        return False
    start = opp_record["StartDate"]
    end = opp_record["EndDate"]
    if not start:
        return False
    if end and point > end:
        return False
    return point >= start


def build_matched_protection(opps_by_account: dict[str, list[dict]]) -> dict[str, dict]:
    matches: dict[str, dict] = {}
    for account_id, account_opps in opps_by_account.items():
        losses = [
            opp
            for opp in account_opps
            if opp["Type"] == "Renewal"
            and opp["IsClosed"]
            and not opp["IsWon"]
            and opp["EventValue"] > 0
            and opp["CloseDate"]
        ]
        wins = [
            opp
            for opp in account_opps
            if opp["Type"] in {"Land", "Expand"}
            and opp["IsWon"]
            and opp["EventValue"] > 0
            and opp["CloseDate"]
        ]
        used: set[str] = set()

        for loss in losses:
            best = None
            best_rank = None
            for win in wins:
                if win["Id"] in used:
                    continue
                delta = days_between(loss["CloseDate"], win["CloseDate"])
                if delta > 180:
                    continue
                overlap = bool(set(loss["ProductGroups"]) & set(win["ProductGroups"]))
                renewal_name = renewalish_name(win["Name"])
                if overlap:
                    rank = (3, -delta)
                    confidence = "High"
                    note = "Product overlap within renewal window"
                elif renewal_name:
                    rank = (2, -delta)
                    confidence = "Medium"
                    note = "Renewal-like naming within renewal window"
                else:
                    rank = (1, -delta)
                    confidence = "Low"
                    note = "Same account within renewal window"
                if best is None or rank > best_rank:
                    best = (win, confidence, note)
                    best_rank = rank
            if best:
                win, confidence, note = best
                matched_value = min(loss["EventValue"], win["EventValue"])
                matches[win["Id"]] = {
                    "matched_to": loss["Id"],
                    "matched_value": matched_value,
                    "confidence": confidence,
                    "note": note,
                }
                used.add(win["Id"])
    return matches


def build_dataset_rows(opps: list[dict], line_map: dict[str, dict]) -> list[dict]:
    prepared = []
    opps_by_account: dict[str, list[dict]] = defaultdict(list)

    for opp in opps:
        quote_info = line_map.get(opp.get("Id", ""), {})
        groups, evidence_source, evidence_confidence, evidence_note = derive_product_payload(
            opp, quote_info
        )
        start_dt, end_dt = derive_contract_span(opp, quote_info)
        close_dt = parse_date(opp.get("CloseDate"))
        account = opp.get("Account") or {}
        account_owner = account.get("Owner") or {}
        owner_id = account.get("OwnerId") or ""
        owner_dim = role_dimension_row(
            owner_id=owner_id,
            owner_name=account_owner.get("Name") or "Unassigned",
            title=account_owner.get("Title") or "",
            user_role=((account_owner.get("UserRole") or {}).get("Name")) or "",
            department=account_owner.get("Department") or "",
            division=account_owner.get("Division") or "",
            manager_id=account_owner.get("ManagerId") or "",
            manager_name=((account_owner.get("Manager") or {}).get("Name")) or "",
        )
        owner_name = owner_dim["OwnerName"] or "Unassigned"
        manager_name = (
            ((((opp.get("Account") or {}).get("Owner") or {}).get("Manager") or {}).get("Name"))
            or "Unassigned"
        )
        motion_primary_persona = primary_motion_persona(opp.get("Type") or "")
        ownership_status = ownership_alignment(owner_dim.get("Persona"), opp.get("Type") or "")
        record = {
            "Id": opp.get("Id") or "",
            "Name": opp.get("Name") or "",
            "AccountId": opp.get("AccountId") or "",
            "AccountName": account.get("Name") or "Unknown",
            "OwnerId": owner_id,
            "OwnerName": owner_name,
            "ManagerName": manager_name,
            "OwnerPersona": owner_dim.get("Persona", "Other"),
            "OwnerRole": owner_dim.get("UserRole", ""),
            "MotionPrimaryPersona": motion_primary_persona,
            "OwnershipAlignment": ownership_status,
            "Type": opp.get("Type") or "",
            "StageName": opp.get("StageName") or "",
            "IsClosed": bool(opp.get("IsClosed")),
            "IsWon": bool(opp.get("IsWon")),
            "CloseDate": close_dt,
            "CloseYear": close_dt.year if close_dt else None,
            "CreatedDateText": (opp.get("CreatedDate") or "")[:10],
            "CreatedYear": int((opp.get("CreatedDate") or "0")[:4] or 0),
            "LegacyAmount": safe_float(opp.get("Amount")),
            "ForecastCategory": opp.get("ForecastCategoryName") or "",
            "QuoteType": opp.get("APTS_Primary_Quote_Type__c") or "",
            "OpportunitySubType": opp.get("APTS_Opportunity_Sub_Type__c") or "",
            "QuoteName": ((opp.get("APTS_Primary_Quote__r") or {}).get("Name")) or "",
            "ProductGroups": groups,
            "EvidenceSource": evidence_source,
            "EvidenceConfidence": evidence_confidence,
            "EvidenceNote": evidence_note,
            "StartDate": start_dt,
            "EndDate": end_dt,
            "EventValue": event_value(opp),
        }
        prepared.append(record)
        if record["AccountId"]:
            opps_by_account[record["AccountId"]].append(record)

    matched = build_matched_protection(opps_by_account)

    rows: list[dict] = []
    for account_id, account_opps in opps_by_account.items():
        account_opps.sort(key=lambda item: (item["CloseDate"] or date(1900, 1, 1), item["Name"]))
        # Pull in full CRM history, but only start the ARR story once the account
        # has real economic value. Older zero-value legacy opps remain available in
        # CRM, but they should not create years of empty ARR history.
        years = sorted(
            {
                y
                for opp in account_opps
                if opp["EventValue"] > 0
                for y in [
                    (opp["StartDate"].year if opp["StartDate"] else None),
                    (opp["EndDate"].year if opp["EndDate"] else None),
                    opp["CloseYear"],
                ]
                if y
            }
        )
        legacy_years = sorted(
            {
                opp["CloseYear"]
                for opp in account_opps
                if opp["LegacyAmount"] > 0
                and opp["CloseYear"]
                and opp["CloseYear"] < LEGACY_CUTOFF_YEAR
            }
        )
        if not years and not legacy_years:
            continue

        for year in range(min(years), max(years) + 1):
            y_start = year_start(year)
            y_end = year_end(year)
            year_events = [opp for opp in account_opps if opp["CloseYear"] == year]
            starting_arr = sum(opp["EventValue"] for opp in account_opps if active_on(opp, y_start))
            ending_arr = sum(opp["EventValue"] for opp in account_opps if active_on(opp, y_end))
            carryover_base = sum(
                opp["EventValue"]
                for opp in account_opps
                if active_on(opp, y_start) and active_on(opp, y_end)
            )
            strict_renewal = sum(
                opp["EventValue"]
                for opp in year_events
                if opp["Type"] == "Renewal" and opp["IsWon"]
            )
            matched_protection = sum(
                matched.get(opp["Id"], {}).get("matched_value", 0.0)
                for opp in year_events
                if opp["Type"] in {"Land", "Expand"} and opp["IsWon"]
            )
            protected_base = min(starting_arr, carryover_base + strict_renewal + matched_protection)
            growth_arr = max(0.0, ending_arr - protected_base)
            true_churn = max(0.0, starting_arr - protected_base)
            new_logo_arr = (
                sum(
                    opp["EventValue"]
                    for opp in year_events
                    if opp["Type"] == "Land" and opp["IsWon"]
                )
                if starting_arr <= 0
                else 0.0
            )
            sample = account_opps[0]

            rows.append(
                {
                    "RecordType": "year_metric",
                    "AccountId": account_id,
                    "AccountName": sample["AccountName"],
                    "OwnerId": sample["OwnerId"],
                    "OwnerName": sample["OwnerName"],
                    "ManagerName": sample["ManagerName"],
                    "OwnerPersona": sample["OwnerPersona"],
                    "OwnerRole": sample["OwnerRole"],
                    "MotionPrimaryPersona": "",
                    "OwnershipAlignment": "",
                    "YearLabel": str(year),
                    "YearDate": f"{year}-01-01",
                    "StartingARR": round(starting_arr, 2),
                    "ProtectedBaseARR": round(protected_base, 2),
                    "WonGrowthARR": round(growth_arr, 2),
                    "TrueChurnARR": round(true_churn, 2),
                    "EndingARR": round(ending_arr, 2),
                    "NewLogoARR": round(new_logo_arr, 2),
                    "CarryoverBaseARR": round(carryover_base, 2),
                    "StrictRenewalARR": round(strict_renewal, 2),
                    "MatchedProtectionARR": round(matched_protection, 2),
                    "EvidenceConfidence": "Modeled",
                    "EvidenceNote": "Starting/ending ARR from active won opp spans; protection from carryover + renewal + matched protection.",
                }
            )

            for step_order, (step_label, step_value) in enumerate(
                [
                    ("Starting ARR", starting_arr),
                    ("True Churn", -true_churn),
                    ("Won Growth", growth_arr),
                    ("Ending ARR", ending_arr),
                ],
                start=1,
            ):
                rows.append(
                    {
                        "RecordType": "year_waterfall",
                        "AccountId": account_id,
                        "AccountName": sample["AccountName"],
                        "OwnerId": sample["OwnerId"],
                        "OwnerName": sample["OwnerName"],
                        "ManagerName": sample["ManagerName"],
                        "OwnerPersona": sample["OwnerPersona"],
                        "OwnerRole": sample["OwnerRole"],
                        "MotionPrimaryPersona": "",
                        "OwnershipAlignment": "",
                        "YearLabel": str(year),
                        "YearDate": f"{year}-01-01",
                        "StepOrder": step_order,
                        "StepLabel": step_label,
                        "StepValue": round(step_value, 2),
                    }
                )

            prior_ending_arr = starting_arr
            for opp in year_events:
                matched_info = matched.get(opp["Id"], {})
                if opp["Type"] == "Renewal" and opp["IsWon"]:
                    event_class = "Strict Renewal"
                    motion_bucket = "Protected Base"
                elif opp["Type"] == "Renewal" and not opp["IsClosed"]:
                    event_class = "Open Renewal"
                    motion_bucket = "Open Renewal"
                elif opp["Type"] == "Renewal":
                    event_class = f"Churn Event ({classify_loss(opp['StageName'])})"
                    motion_bucket = "Churn / Loss"
                elif matched_info.get("matched_value", 0) > 0:
                    event_class = "Matched Protection Candidate"
                    motion_bucket = "Protected Base"
                elif opp["Type"] == "Land" and opp["IsWon"] and prior_ending_arr <= 0:
                    event_class = "New Logo"
                    motion_bucket = "New Logo"
                elif opp["Type"] in {"Land", "Expand"} and opp["IsWon"]:
                    event_class = "Won Growth"
                    motion_bucket = "Growth"
                elif opp["Type"] in {"Land", "Expand"} and not opp["IsClosed"]:
                    event_class = "Open Growth"
                    motion_bucket = "Open Pipeline"
                else:
                    event_class = "Lost Growth"
                    motion_bucket = "Lost / Deferred"

                product_groups = opp["ProductGroups"] or ["Unspecified"]
                alloc_denominator = len(product_groups)
                for product_group in product_groups:
                    allocated_arr = opp["EventValue"] / alloc_denominator if alloc_denominator else 0.0
                    rows.append(
                        {
                            "RecordType": "product_flow",
                            "AccountId": account_id,
                            "AccountName": opp["AccountName"],
                            "OwnerId": opp["OwnerId"],
                            "OwnerName": opp["OwnerName"],
                            "ManagerName": opp["ManagerName"],
                            "OwnerPersona": opp["OwnerPersona"],
                            "OwnerRole": opp["OwnerRole"],
                            "MotionPrimaryPersona": opp["MotionPrimaryPersona"],
                            "OwnershipAlignment": opp["OwnershipAlignment"],
                            "YearLabel": str(year),
                            "YearDate": f"{year}-01-01",
                            "ProductGroup": product_group,
                            "MotionBucket": motion_bucket,
                            "FlowARR": round(allocated_arr, 2),
                            "OpportunityCount": 1,
                            "EvidenceSource": opp["EvidenceSource"],
                            "EvidenceConfidence": matched_info.get("confidence") or opp["EvidenceConfidence"],
                            "EvidenceNote": matched_info.get("note") or opp["EvidenceNote"],
                        }
                    )

                rows.append(
                    {
                        "RecordType": "event_detail",
                        "AccountId": account_id,
                        "AccountName": opp["AccountName"],
                        "OwnerId": opp["OwnerId"],
                        "OwnerName": opp["OwnerName"],
                        "ManagerName": opp["ManagerName"],
                        "OwnerPersona": opp["OwnerPersona"],
                        "OwnerRole": opp["OwnerRole"],
                        "MotionPrimaryPersona": opp["MotionPrimaryPersona"],
                        "OwnershipAlignment": opp["OwnershipAlignment"],
                        "YearLabel": str(year),
                        "EventDate": iso(opp["CloseDate"]),
                        "OpportunityId": opp["Id"],
                        "OpportunityName": opp["Name"],
                        "OpportunityType": opp["Type"],
                        "StageName": opp["StageName"],
                        "ForecastCategory": opp["ForecastCategory"],
                        "QuoteType": opp["QuoteType"],
                        "OpportunitySubType": opp["OpportunitySubType"],
                        "QuoteName": opp["QuoteName"],
                        "ProductGroup": ", ".join(product_groups),
                        "EventClass": event_class,
                        "EventARR": round(opp["EventValue"], 2),
                        "MatchedProtectionARR": round(matched_info.get("matched_value", 0.0), 2),
                        "EvidenceSource": opp["EvidenceSource"],
                        "EvidenceConfidence": matched_info.get("confidence") or opp["EvidenceConfidence"],
                        "EvidenceNote": matched_info.get("note") or opp["EvidenceNote"],
                    }
                )

        if legacy_years:
            sample = account_opps[0]
            for year in legacy_years:
                legacy_events = [
                    opp
                    for opp in account_opps
                    if opp["CloseYear"] == year and opp["LegacyAmount"] > 0
                ]
                legacy_won = sum(
                    opp["LegacyAmount"]
                    for opp in legacy_events
                    if opp["StageName"] == "8 - Won"
                )
                legacy_lost = sum(
                    opp["LegacyAmount"]
                    for opp in legacy_events
                    if opp["StageName"] != "8 - Won"
                )
                backfilled_count = sum(
                    1 for opp in legacy_events if opp["CreatedYear"] == 2012
                )
                native_count = sum(
                    1 for opp in legacy_events if opp["CreatedYear"] != 2012
                )

                rows.append(
                    {
                        "RecordType": "legacy_year_metric",
                        "AccountId": account_id,
                        "AccountName": sample["AccountName"],
                        "OwnerId": sample["OwnerId"],
                        "OwnerName": sample["OwnerName"],
                        "ManagerName": sample["ManagerName"],
                        "OwnerPersona": sample["OwnerPersona"],
                        "OwnerRole": sample["OwnerRole"],
                        "MotionPrimaryPersona": "",
                        "OwnershipAlignment": "",
                        "YearLabel": str(year),
                        "YearDate": f"{year}-01-01",
                        "LegacyWonAmount": round(legacy_won, 2),
                        "LegacyLostAmount": round(legacy_lost, 2),
                        "LegacyBackfilledCount": backfilled_count,
                        "LegacyNativeCount": native_count,
                        "EvidenceConfidence": "Legacy",
                        "EvidenceNote": "Legacy commercial history in Opportunity Amount. Pre-modern CRM records are not ARR-comparable.",
                    }
                )

                for opp in legacy_events:
                    rows.append(
                        {
                            "RecordType": "legacy_event",
                            "AccountId": account_id,
                            "AccountName": opp["AccountName"],
                            "OwnerId": opp["OwnerId"],
                            "OwnerName": opp["OwnerName"],
                            "ManagerName": opp["ManagerName"],
                            "OwnerPersona": opp["OwnerPersona"],
                            "OwnerRole": opp["OwnerRole"],
                            "MotionPrimaryPersona": opp["MotionPrimaryPersona"],
                            "OwnershipAlignment": opp["OwnershipAlignment"],
                            "YearLabel": str(year),
                            "YearDate": f"{year}-01-01",
                            "EventDate": iso(opp["CloseDate"]),
                            "OpportunityId": opp["Id"],
                            "OpportunityName": opp["Name"],
                            "OpportunityType": opp["Type"],
                            "StageName": opp["StageName"],
                            "ProductGroup": ", ".join(opp["ProductGroups"] or ["Unspecified"]),
                            "LegacyAmount": round(opp["LegacyAmount"], 2),
                            "CreatedDateText": opp["CreatedDateText"],
                            "DataQualityTag": "2012 Backfill"
                            if opp["CreatedYear"] == 2012
                            else "Native / Later Entry",
                            "EvidenceConfidence": "Legacy",
                            "EvidenceNote": "Legacy commercial history in Amount only; not ARR-comparable.",
                        }
                    )

    return rows


def create_dataset(inst: str, tok: str) -> bool:
    print("\n=== Building Account History 360 dataset ===")
    opps = _soql(inst, tok, OPP_SOQL)
    print(f"  Queried {len(opps)} opportunities")
    lines = _soql(inst, tok, LINE_SOQL)
    print(f"  Queried {len(lines)} quote lines")
    line_map = build_quote_line_map(lines)
    rows = build_dataset_rows(opps, line_map)
    print(f"  Built {len(rows)} dataset rows")

    fields = [
        _dim("RecordType"),
        _dim("AccountId"),
        _dim("AccountName"),
        _dim("OwnerId"),
        _dim("OwnerName"),
        _dim("ManagerName"),
        _dim("OwnerPersona"),
        _dim("OwnerRole"),
        _dim("MotionPrimaryPersona"),
        _dim("OwnershipAlignment"),
        _dim("YearLabel"),
        _date("YearDate"),
        _date("EventDate"),
        _dim("StepLabel"),
        _measure("StepOrder", precision=8, scale=0),
        _measure("StepValue"),
        _measure("StartingARR"),
        _measure("ProtectedBaseARR"),
        _measure("WonGrowthARR"),
        _measure("TrueChurnARR"),
        _measure("EndingARR"),
        _measure("NewLogoARR"),
        _measure("CarryoverBaseARR"),
        _measure("StrictRenewalARR"),
        _measure("MatchedProtectionARR"),
        _measure("LegacyWonAmount"),
        _measure("LegacyLostAmount"),
        _measure("LegacyBackfilledCount", precision=8, scale=0),
        _measure("LegacyNativeCount", precision=8, scale=0),
        _measure("LegacyYearCount", precision=8, scale=0),
        _dim("OpportunityId"),
        _dim("OpportunityName"),
        _dim("OpportunityType"),
        _dim("StageName"),
        _dim("CreatedDateText"),
        _dim("DataQualityTag"),
        _dim("ForecastCategory"),
        _dim("QuoteType"),
        _dim("OpportunitySubType"),
        _dim("QuoteName"),
        _dim("ProductGroup"),
        _dim("MotionBucket"),
        _dim("EventClass"),
        _dim("EvidenceSource"),
        _dim("EvidenceConfidence"),
        _dim("EvidenceNote"),
        _measure("EventARR"),
        _measure("LegacyAmount"),
        _measure("FlowARR"),
        _measure("OpportunityCount", precision=8, scale=0),
    ]

    fieldnames = [field["name"] for field in fields]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in fieldnames})
    return upload_dataset(inst, tok, DS, DS_LABEL, fields, buf.getvalue().encode())


def build_steps(ds_id: str) -> dict[str, dict]:
    ds_meta = [{"id": ds_id, "name": DS}]
    load = f'q = load "{DS}";\n'
    fm = coalesce_filter("f_manager", "ManagerName")
    fo = coalesce_filter("f_owner", "OwnerName")
    fa = coalesce_filter("f_account", "AccountName")
    fy = coalesce_filter("f_year", "YearLabel")

    account_scope = load + fm + fo + fa
    account_year_scope = load + fm + fo + fa + fy

    steps = {
        "f_manager": af("ManagerName", ds_meta),
        "f_owner": af("OwnerName", ds_meta),
        "f_account": af("AccountName", ds_meta, select_mode="single"),
        "f_year": af("YearLabel", ds_meta, select_mode="single", start='["2025"]'),
        "s_summary": sq(
            account_year_scope
            + 'q = filter q by RecordType == "year_metric";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(StartingARR) as StartingARR, "
            + "sum(ProtectedBaseARR) as ProtectedBaseARR, "
            + "sum(WonGrowthARR) as WonGrowthARR, "
            + "sum(TrueChurnARR) as TrueChurnARR, "
            + "sum(EndingARR) as EndingARR;"
        ),
        "s_timeline": sq(
            account_scope
            + 'q = filter q by RecordType == "year_metric";\n'
            + "q = group q by (YearDate, YearLabel);\n"
            + "q = foreach q generate YearDate, YearLabel, "
            + "sum(StartingARR) as StartingARR, "
            + "sum(ProtectedBaseARR) as ProtectedBaseARR, "
            + "sum(WonGrowthARR) as WonGrowthARR, "
            + "sum(TrueChurnARR) as TrueChurnARR, "
            + "sum(EndingARR) as EndingARR;\n"
            + "q = order q by YearDate asc;"
        ),
        "s_waterfall": sq(
            account_year_scope
            + 'q = filter q by RecordType == "year_waterfall";\n'
            + "q = group q by (StepOrder, StepLabel);\n"
            + "q = foreach q generate StepOrder, StepLabel, sum(StepValue) as StepValue;\n"
            + "q = order q by StepOrder asc;"
        ),
        "s_product_flow": sq(
            account_year_scope
            + 'q = filter q by RecordType == "product_flow";\n'
            + 'q = filter q by ProductGroup != "Unspecified";\n'
            + "q = group q by (ProductGroup, MotionBucket);\n"
            + "q = foreach q generate ProductGroup as source, MotionBucket as target, sum(FlowARR) as flow;\n"
            + "q = order q by flow desc;"
        ),
        "s_product_table": sq(
            account_year_scope
            + 'q = filter q by RecordType == "product_flow";\n'
            + 'q = filter q by ProductGroup != "Unspecified";\n'
            + "q = group q by (ProductGroup, MotionBucket, EvidenceSource, EvidenceConfidence);\n"
            + "q = foreach q generate ProductGroup, MotionBucket, "
            + "sum(FlowARR) as FlowARR, sum(OpportunityCount) as OpportunityCount, "
            + "EvidenceSource, EvidenceConfidence;\n"
            + "q = order q by FlowARR desc;\n"
            + "q = limit q 15;"
        ),
        "s_handoff": sq(
            account_year_scope
            + 'q = filter q by RecordType == "event_detail";\n'
            + "q = group q by (OpportunityType, OwnerName, OwnerPersona, MotionPrimaryPersona, OwnershipAlignment);\n"
            + "q = foreach q generate OpportunityType, OwnerName, OwnerPersona, MotionPrimaryPersona, OwnershipAlignment, "
            + "sum(EventARR) as EventARR, count() as OpportunityCount;\n"
            + "q = order q by EventARR desc;\n"
            + "q = limit q 12;"
        ),
        "s_events": sq(
            account_year_scope
            + 'q = filter q by RecordType == "event_detail";\n'
            + "q = group q by (EventDate, OpportunityName, OpportunityType, StageName, ForecastCategory, "
            + "QuoteType, OpportunitySubType, QuoteName, ProductGroup, EventClass, "
            + "EvidenceSource, EvidenceConfidence, EvidenceNote, OpportunityId, AccountId, OwnerName);\n"
            + "q = foreach q generate EventDate, OpportunityName, OpportunityType, StageName, "
            + "ForecastCategory, QuoteType, ProductGroup, EventClass, "
            + "sum(EventARR) as EventARR, sum(MatchedProtectionARR) as MatchedProtectionARR, "
            + "EvidenceConfidence, EvidenceSource, EvidenceNote, OwnerName, OpportunityId, AccountId;\n"
            + "q = order q by EventDate desc;\n"
            + "q = limit q 20;"
        ),
        "s_legacy_summary": sq(
            account_scope
            + 'q = filter q by RecordType == "legacy_year_metric";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "sum(LegacyWonAmount) as LegacyWonAmount, "
            + "sum(LegacyLostAmount) as LegacyLostAmount, "
            + "sum(LegacyBackfilledCount) as LegacyBackfilledCount, "
            + "sum(LegacyNativeCount) as LegacyNativeCount, "
            + "count() as LegacyYearCount;"
        ),
        "s_legacy_timeline": sq(
            account_scope
            + 'q = filter q by RecordType == "legacy_year_metric";\n'
            + "q = group q by (YearDate, YearLabel);\n"
            + "q = foreach q generate YearDate, YearLabel, "
            + "sum(LegacyWonAmount) as LegacyWonAmount, "
            + "sum(LegacyLostAmount) as LegacyLostAmount;\n"
            + "q = order q by YearDate asc;"
        ),
        "s_legacy_events": sq(
            account_scope
            + 'q = filter q by RecordType == "legacy_event";\n'
            + "q = group q by (EventDate, OpportunityName, OpportunityType, StageName, "
            + "CreatedDateText, ProductGroup, DataQualityTag, EvidenceNote, OpportunityId, AccountId, OwnerName);\n"
            + "q = foreach q generate EventDate, OpportunityName, OpportunityType, StageName, "
            + "sum(LegacyAmount) as LegacyAmount, CreatedDateText, ProductGroup, DataQualityTag, "
            + "EvidenceNote, OwnerName, OpportunityId, AccountId;\n"
            + "q = order q by LegacyAmount desc;\n"
            + "q = limit q 20;"
        ),
    }
    return steps


def build_widgets() -> dict[str, dict]:
    return {
        "p1_nav1": nav_link("story", "Account Story", active=True),
        "p1_nav2": nav_link("detail", "Product & Events"),
        "p1_nav3": nav_link("legacy", "Legacy History"),
        "p1_hdr": hdr(
            "Account 360 & History",
            "Account-level ARR story using opportunity chronology with quote-line product evidence when available.",
        ),
        "p1_f_manager": pillbox("f_manager", "Manager"),
        "p1_f_owner": pillbox("f_owner", "Owner"),
        "p1_f_account": pillbox("f_account", "Account"),
        "p1_f_year": pillbox("f_year", "Year"),
        "p1_sec_story": section_label("ARR Story"),
        "p1_n_start": num("s_summary", "StartingARR", "Starting ARR", "#032D60", compact=True),
        "p1_n_protected": num("s_summary", "ProtectedBaseARR", "Protected Base ARR", "#2E844A", compact=True),
        "p1_n_growth": num("s_summary", "WonGrowthARR", "Won Growth ARR", "#0176D3", compact=True),
        "p1_n_churn": num("s_summary", "TrueChurnARR", "True Churn ARR", "#BA0517", compact=True),
        "p1_n_end": num("s_summary", "EndingARR", "Ending ARR", "#5C3EAA", compact=True),
        "p1_ch_timeline": timeline_chart(
            "s_timeline",
            "Account ARR Story Over Time",
            show_legend=True,
            axis_title="ARR",
        ),
        "p1_ch_waterfall": waterfall_chart(
            "s_waterfall",
            "Selected Year Bridge: Starting ARR to Ending ARR",
            "StepLabel",
            "StepValue",
            axis_label="ARR",
        ),
        "p2_nav1": nav_link("story", "Account Story"),
        "p2_nav2": nav_link("detail", "Product & Events", active=True),
        "p2_nav3": nav_link("legacy", "Legacy History"),
        "p2_hdr": hdr(
            "Product & Events",
            "How product groups and deals contributed to protection, growth, or churn in the selected year.",
        ),
        "p2_f_manager": pillbox("f_manager", "Manager"),
        "p2_f_owner": pillbox("f_owner", "Owner"),
        "p2_f_account": pillbox("f_account", "Account"),
        "p2_f_year": pillbox("f_year", "Year"),
        "p2_sec_product": section_label("Product Contribution"),
        "p2_ch_flow": sankey_chart(
            "s_product_flow",
            "Product Group -> Motion Bucket",
            subtitle="Selected year view. Uses opportunity product family first, quote product area as fallback evidence.",
        ),
        "p2_tbl_product": compare_table(
            "s_product_table",
            "Product Motion Summary",
            columns=[
                "ProductGroup",
                "MotionBucket",
                "FlowARR",
                "OpportunityCount",
                "EvidenceSource",
                "EvidenceConfidence",
            ],
            row_limit=15,
            show_totals=False,
        ),
        "p2_tbl_handoff": compare_table(
            "s_handoff",
            "Commercial Handoff Context",
            columns=[
                "OpportunityType",
                "OwnerName",
                "OwnerPersona",
                "MotionPrimaryPersona",
                "OwnershipAlignment",
                "EventARR",
            ],
            row_limit=12,
            show_totals=False,
            subtitle="Checks whether account motions are sitting with the expected commercial persona for the selected year.",
        ),
        "p2_sec_events": section_label("Event Ledger"),
        "p2_tbl_events": compare_table(
            "s_events",
            "Account Event Ledger",
            columns=[
                "EventDate",
                "OpportunityName",
                "OpportunityType",
                "EventClass",
                "EventARR",
                "MatchedProtectionARR",
                "ProductGroup",
                "EvidenceConfidence",
                "EvidenceSource",
                "OwnerName",
            ],
            row_limit=20,
            show_totals=False,
            subtitle="Audit view of the selected account/year. Uses ARR or Renewal ACV, never Amount.",
        ),
        "p3_nav1": nav_link("story", "Account Story"),
        "p3_nav2": nav_link("detail", "Product & Events"),
        "p3_nav3": nav_link("legacy", "Legacy History", active=True),
        "p3_hdr": hdr(
            "Legacy Commercial History",
            "Pre-2018 commercial history in Opportunity Amount. Useful for context and relationship age, but not ARR-comparable.",
        ),
        "p3_f_manager": pillbox("f_manager", "Manager"),
        "p3_f_owner": pillbox("f_owner", "Owner"),
        "p3_f_account": pillbox("f_account", "Account"),
        "p3_sec_legacy": section_label("Legacy History"),
        "p3_n_legacy_won": num(
            "s_legacy_summary", "LegacyWonAmount", "Legacy Won Amount", "#2E844A", compact=True
        ),
        "p3_n_legacy_lost": num(
            "s_legacy_summary", "LegacyLostAmount", "Legacy Lost Amount", "#BA0517", compact=True
        ),
        "p3_n_legacy_years": num(
            "s_legacy_summary", "LegacyYearCount", "Legacy Years With Value", "#032D60", compact=False
        ),
        "p3_n_legacy_backfill": num(
            "s_legacy_summary", "LegacyBackfilledCount", "2012 Backfilled Records", "#5C3EAA", compact=False
        ),
        "p3_ch_legacy_timeline": timeline_chart(
            "s_legacy_timeline",
            "Legacy Commercial History Over Time",
            show_legend=True,
            axis_title="Amount",
        ),
        "p3_tbl_legacy_events": compare_table(
            "s_legacy_events",
            "Key Legacy Commercial Events",
            columns=[
                "EventDate",
                "OpportunityName",
                "OpportunityType",
                "StageName",
                "LegacyAmount",
                "DataQualityTag",
                "CreatedDateText",
                "ProductGroup",
            ],
            row_limit=20,
            show_totals=False,
            subtitle="Largest pre-2018 events for the selected account. These are legacy Amount records, not ARR.",
        ),
    }


def build_layout() -> dict:
    p1 = nav_row("p1", 3) + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_manager", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_owner", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_account", "row": 3, "column": 6, "colspan": 4, "rowspan": 2},
        {"name": "p1_f_year", "row": 3, "column": 10, "colspan": 2, "rowspan": 2},
        {"name": "p1_sec_story", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_n_start", "row": 6, "column": 0, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_protected", "row": 6, "column": 2, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_growth", "row": 6, "column": 4, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_churn", "row": 6, "column": 6, "colspan": 2, "rowspan": 4},
        {"name": "p1_n_end", "row": 6, "column": 8, "colspan": 4, "rowspan": 4},
        {"name": "p1_ch_timeline", "row": 10, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p1_ch_waterfall", "row": 18, "column": 0, "colspan": 12, "rowspan": 7},
    ]

    p2 = nav_row("p2", 3) + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_manager", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_owner", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_account", "row": 3, "column": 6, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_year", "row": 3, "column": 10, "colspan": 2, "rowspan": 2},
        {"name": "p2_sec_product", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_flow", "row": 6, "column": 0, "colspan": 7, "rowspan": 8},
        {"name": "p2_tbl_product", "row": 6, "column": 7, "colspan": 5, "rowspan": 4},
        {"name": "p2_tbl_handoff", "row": 10, "column": 7, "colspan": 5, "rowspan": 4},
        {"name": "p2_sec_events", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_tbl_events", "row": 15, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    p3 = nav_row("p3", 3) + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_manager", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_owner", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_account", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        {"name": "p3_sec_legacy", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_n_legacy_won", "row": 6, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p3_n_legacy_lost", "row": 6, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p3_n_legacy_years", "row": 6, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p3_n_legacy_backfill", "row": 6, "column": 9, "colspan": 3, "rowspan": 4},
        {"name": "p3_ch_legacy_timeline", "row": 10, "column": 0, "colspan": 12, "rowspan": 8},
        {"name": "p3_tbl_legacy_events", "row": 18, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    return {
        "name": "AccountHistory360",
        "numColumns": 12,
        "pages": [
            pg("story", "Account Story", p1),
            pg("detail", "Product & Events", p2),
            pg("legacy", "Legacy History", p3),
        ],
    }


def main() -> None:
    inst, tok = get_auth()
    if not create_dataset(inst, tok):
        raise SystemExit("Dataset upload failed")

    ds_id = get_dataset_id(inst, tok, DS)
    if not ds_id:
        raise SystemExit(f"Could not resolve dataset id for {DS}")

    steps = build_steps(ds_id)
    widgets = build_widgets()
    layout = build_layout()
    state = build_dashboard_state(steps, widgets, layout)

    dashboard_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
    print(f"\n=== Deploying {DASHBOARD_LABEL} ===")
    deploy_dashboard(inst, tok, dashboard_id, state)

    set_record_links_xmd(
        inst,
        tok,
        DS,
        [
            {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
            {"field": "OpportunityName", "id_field": "OpportunityId", "label": "Opportunity"},
        ],
    )


if __name__ == "__main__":
    main()
