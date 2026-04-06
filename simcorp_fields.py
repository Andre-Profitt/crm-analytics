"""SimCorp SOQL field constants + startup describe-check.

Centralizes the field name strings that the 8 KPI builders pull from
Salesforce. Lets a single field deletion fail fast at startup with a
clear message instead of crashing mid-builder with a SOQL error.

Part of Builder Modernization 1A — see
docs/superpowers/specs/2026-04-06-builder-modernization-1a-plumbing-design.md
"""

from __future__ import annotations

import logging
from typing import Iterable

import requests

logger = logging.getLogger(__name__)


# --- Per-object field tuples ---------------------------------------------

ACCOUNT_FIELDS: tuple[str, ...] = (
    "APTS_Subscription_Term__c",
    "AuM_m__c",
    "Axioma_Client__c",
    "BillingCountry",
    "CreatedDate",
    "DUNS_No__c",
    "Expected_Termination_Date__c",
    "Id",
    "Industry",
    "KYC_Approval_Status__c",
    "Name",
    "NumberOfEmployees",
    "Partner_Engagement_Level__c",
    "Risk_of_Potential_Termination__c",
    "SaaS_Client__c",
    "Termination_Date__c",
    "Termination_Reason__c",
    "Type",
    "Unit_Group__c",
    "Unit__c",
)

CONTACT_FIELDS: tuple[str, ...] = (
    "AccountId",
    "CreatedDate",
    "Department__c",
    "Id",
    "LastActivityDate",
    "Title",
)

CONTRACT_FIELDS: tuple[str, ...] = (
    "AccountId",
    "Agreement_Type__c",
    "ContractTerm",
    "EndDate",
    "Id",
    "StartDate",
    "Status",
)

FORECASTING_ITEM_FIELDS: tuple[str, ...] = (
    # Note: FiscalYear is queried via SOQL on ForecastingItem but is NOT
    # returned by /sobjects/ForecastingItem/describe (it is a pseudo-field
    # resolved through the related Period object). Omitted here so the
    # describe-check does not false-positive on a valid, active field.
    "ForecastAmount",
    "ForecastCategoryName",
    "OwnerId",
    "PeriodId",
)

OPPORTUNITY_FIELDS: tuple[str, ...] = (
    "APTS_Contract_End_Date__c",
    "APTS_Contract_Start_Date__c",
    "APTS_Forecast_ARR__c",
    "APTS_Opportunity_ARR__c",
    "APTS_RH_Product_Family__c",
    "APTS_Renewal_ACV__c",
    "AccountId",
    "Account_Unit_Group__c",
    "AgeInDays",
    "Amount",
    "Approval_Status__c",
    "CloseDate",
    "CreatedDate",
    "Deal_Shaping_Approved__c",
    "FiscalQuarter",
    "FiscalYear",
    "ForecastCategoryName",
    "HasOverdueTask",
    "Id",
    "IsClosed",
    "IsWon",
    "LastStageChangeInDays",
    "LeadSource",
    "Name",
    "NextStep",
    "OwnerId",
    "Probability",
    "Quota_Amount__c",
    "Reason_Won_Lost__c",
    "Sales_Cycle_Duration__c",
    "Sales_Region__c",
    "StageName",
    "Stage_20_Approval_Date__c",
    "Stage_20_Approval__c",
    "Sub_Reason__c",
    "Submit_for_Stage_20_Review_Date__c",
    "Submit_for_Stage_20_Review__c",
    "Type",
)

OPPORTUNITY_FIELD_HISTORY_FIELDS: tuple[str, ...] = (
    "CreatedDate",
    "Field",
    "NewValue",
    "OldValue",
    "OpportunityId",
)

OPPORTUNITY_HISTORY_FIELDS: tuple[str, ...] = (
    "Amount",
    "CloseDate",
    "CreatedDate",
    "OpportunityId",
    "StageName",
)

USER_FIELDS: tuple[str, ...] = (
    "Department",
    "Division",
    "Id",
    "ManagerId",
    "Name",
    "Title",
)


# --- Constant-name → tuple registry --------------------------------------
# Every *_FIELDS tuple declared above must be registered here under the
# exact Salesforce SObject name (NOT the snake-cased constant name).
SCHEMA: dict[str, tuple[str, ...]] = {
    "Account": ACCOUNT_FIELDS,
    "Contact": CONTACT_FIELDS,
    "Contract": CONTRACT_FIELDS,
    "ForecastingItem": FORECASTING_ITEM_FIELDS,
    "Opportunity": OPPORTUNITY_FIELDS,
    "OpportunityFieldHistory": OPPORTUNITY_FIELD_HISTORY_FIELDS,
    "OpportunityHistory": OPPORTUNITY_HISTORY_FIELDS,
    "User": USER_FIELDS,
}


# --- SObject name → constant name (for SchemaDriftError messages) -------
_TUPLE_TO_CONSTANT_NAME: dict[str, str] = {
    "Account": "ACCOUNT_FIELDS",
    "Contact": "CONTACT_FIELDS",
    "Contract": "CONTRACT_FIELDS",
    "ForecastingItem": "FORECASTING_ITEM_FIELDS",
    "Opportunity": "OPPORTUNITY_FIELDS",
    "OpportunityFieldHistory": "OPPORTUNITY_FIELD_HISTORY_FIELDS",
    "OpportunityHistory": "OPPORTUNITY_HISTORY_FIELDS",
    "User": "USER_FIELDS",
}


class SchemaDriftError(RuntimeError):
    """Raised when the live org is missing fields the builders depend on."""


def _describe_object(instance_url: str, access_token: str, obj: str) -> dict[str, dict]:
    """GET /services/data/v66.0/sobjects/<obj>/describe and return a
    dict mapping field.name → field metadata. The inner dict contents
    are opaque to this module; only the keys are checked."""
    url = f"{instance_url}/services/data/v66.0/sobjects/{obj}/describe"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    fields = response.json().get("fields", [])
    return {f["name"]: f for f in fields}


def assert_org_schema(
    instance_url: str,
    access_token: str,
    objects: Iterable[str] | None = None,
) -> None:
    """For each object in `objects` (default: all keys in SCHEMA), call
    /sobjects/<obj>/describe and confirm every field in SCHEMA[obj]
    exists in the org.

    Raises SchemaDriftError listing every missing field if any are
    gone, with a message that names the constant tuple to edit.
    """
    target_objects = list(objects) if objects is not None else list(SCHEMA.keys())
    all_missing: list[str] = []

    for obj in target_objects:
        expected = SCHEMA[obj]
        logger.info("Schema check %s (%d fields)", obj, len(expected))
        org_fields = set(_describe_object(instance_url, access_token, obj).keys())
        missing = [f for f in expected if f not in org_fields]
        if missing:
            const_name = _TUPLE_TO_CONSTANT_NAME[obj]
            all_missing.append(
                f"{obj} is missing {len(missing)} field(s) referenced by "
                f"simcorp_fields.{const_name}:\n"
                + "\n".join(f"  - {f}" for f in missing)
                + f"\n\nEither add it back to the org, or remove it from {const_name}."
            )

    if all_missing:
        raise SchemaDriftError("\n\n".join(all_missing))

    logger.info("All required fields present")
