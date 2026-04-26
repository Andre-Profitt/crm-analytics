#!/usr/bin/env python3
"""Create/update Sales Director monthly Pipeline Open Opportunity list views."""

from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.manage_opportunity_list_views import (  # noqa: E402
    DEFAULT_TARGET_ORG,
    api_request,
    create_list_info,
    get_auth,
    get_list_info_by_name,
    normalize_api_name,
    update_list_info_by_name,
)


DEFAULT_TERRITORY_CONFIG = ROOT / "config" / "sd_monthly_territories.json"
DEFAULT_OBJECT = "Opportunity"
DEFAULT_FY_LABEL = "FY26"

DISPLAY_COLUMNS = [
    "Name",
    "Account.Name",
    "Owner.Name",
    "StageName",
    "ForecastCategoryName",
    "CloseDate",
    "APTS_Opportunity_ARR__c",
    "APTS_Forecast_ARR__c",
    "Probability",
    "Type",
    "CreatedDate",
    "LastActivityDate",
    "NextStep",
    "LastModifiedDate",
    "PushCount",
]

OPEN_STAGES = [
    "1 - Prospecting",
    "2 - Discovery",
    "3 - Engagement",
    "4 - Shortlisted",
    "5 - Preferred",
    "6 - Contracting",
]

FORECAST_CATEGORIES = ["Pipeline", "Best Case", "Commit"]

TERRITORY_FILTERS: dict[str, list[dict[str, Any]]] = {
    "APAC": [
        {"fieldApiName": "Account_Unit_Group__c", "operandLabels": ["SC Asia"]},
        {"fieldApiName": "Sales_Region__c", "operandLabels": ["APAC"]},
    ],
    "Central Europe": [
        {"fieldApiName": "Account_Unit_Group__c", "operandLabels": ["SC EMEA"]},
        {"fieldApiName": "Sales_Region__c", "operandLabels": ["Central Europe"]},
    ],
    "UK & Ireland": [
        {"fieldApiName": "Account_Unit_Group__c", "operandLabels": ["SC EMEA"]},
        {
            "fieldApiName": "Sales_Region__c",
            "operandLabels": ["United Kingdom & Ireland"],
        },
    ],
    "Southern Europe": [
        {"fieldApiName": "Account_Unit_Group__c", "operandLabels": ["SC EMEA"]},
        {"fieldApiName": "Sales_Region__c", "operandLabels": ["Southwestern Europe"]},
    ],
    "NL & Nordics": [
        {"fieldApiName": "Account_Unit_Group__c", "operandLabels": ["SC EMEA"]},
        {"fieldApiName": "Sales_Region__c", "operandLabels": ["Northern Europe"]},
    ],
    "Middle East & Africa": [
        {"fieldApiName": "Account_Unit_Group__c", "operandLabels": ["SC EMEA"]},
        {"fieldApiName": "Sales_Region__c", "operandLabels": ["Middle East & Africa"]},
    ],
    "Canada": [
        {
            "fieldApiName": "Account_Unit_Group__c",
            "operandLabels": ["SC North America"],
        },
        {"fieldApiName": "Sales_Region__c", "operandLabels": ["North America"]},
        {"fieldApiName": "Account_Unit__c", "operandLabels": ["SC Canada"]},
    ],
    "NA Asset Management": [
        {
            "fieldApiName": "Account_Unit_Group__c",
            "operandLabels": ["SC North America"],
        },
        {"fieldApiName": "Sales_Region__c", "operandLabels": ["North America"]},
        {"fieldApiName": "Account_Unit__c", "operandLabels": ["SC USA"]},
    ],
    "Pension & Insurance": [
        {
            "fieldApiName": "Account_Unit_Group__c",
            "operandLabels": ["SC North America"],
        },
        {"fieldApiName": "Sales_Region__c", "operandLabels": ["North America"]},
        {"fieldApiName": "Account_Unit__c", "operandLabels": ["SC USA"]},
    ],
}

FIELD_FILTER_REASON = {
    "NA Asset Management": (
        "Opportunity list views cannot filter Account.Industry; NA vertical "
        "book split is applied in source-backed ETL using the FY26 Pipeline Open "
        "report Industry reference."
    ),
    "Pension & Insurance": (
        "Opportunity list views cannot filter Account.Industry; NA vertical "
        "book split is applied in source-backed ETL using the FY26 Pipeline Open "
        "report Industry reference."
    ),
}

TERRITORY_LABEL_SUFFIX = {
    "APAC": "APAC",
    "Central Europe": "CE",
    "UK & Ireland": "UKI",
    "Southern Europe": "SWE",
    "NL & Nordics": "NE",
    "Middle East & Africa": "MEA",
    "Canada": "Canada",
    "NA Asset Management": "NA AM",
    "Pension & Insurance": "P&I",
}


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def pipeline_open_label(territory: str, fy_label: str) -> str:
    suffix = TERRITORY_LABEL_SUFFIX.get(territory, territory)
    return f"SD Monthly Pipeline Open {suffix} {fy_label}"


def pipeline_open_api_name(territory: str, fy_label: str) -> str:
    return normalize_api_name(pipeline_open_label(territory, fy_label))


def list_view_filter(field_api_name: str, operand_labels: list[str]) -> dict[str, Any]:
    return {
        "fieldApiName": field_api_name,
        "operator": "Contains" if field_api_name == "Sales_Region__c" else "Equals",
        "operandLabels": operand_labels,
    }


def pipeline_open_filters(territory: str) -> list[dict[str, Any]]:
    if territory not in TERRITORY_FILTERS:
        raise ValueError(f"No pipeline-open filter policy for territory: {territory}")
    filters = [
        list_view_filter(item["fieldApiName"], list(item["operandLabels"]))
        for item in TERRITORY_FILTERS[territory]
    ]
    filters.extend(
        [
            list_view_filter("StageName", OPEN_STAGES),
            list_view_filter("Type", ["Land"]),
            list_view_filter("ForecastCategoryName", FORECAST_CATEGORIES),
            list_view_filter("CloseDate", ["THIS FISCAL YEAR"]),
        ]
    )
    return filters


def build_payload(territory: str, *, fy_label: str) -> dict[str, Any]:
    label = pipeline_open_label(territory, fy_label)
    return {
        "label": label,
        "listViewApiName": pipeline_open_api_name(territory, fy_label),
        "visibility": "Private",
        "scope": {"apiName": "everything"},
        "displayColumns": DISPLAY_COLUMNS,
        "filteredByInfo": pipeline_open_filters(territory),
    }


def validate_filter_fields(
    *,
    instance_url: str,
    token: str,
    object_api_name: str,
    territories: list[str],
) -> dict[str, Any]:
    object_info = api_request(
        method="GET",
        instance_url=instance_url,
        token=token,
        path=f"/ui-api/object-info/{object_api_name}",
    )
    fields = object_info.get("fields") or {}
    used_fields = sorted(
        {
            filter_item["fieldApiName"]
            for territory in territories
            for filter_item in pipeline_open_filters(territory)
        }
    )
    missing = [
        field
        for field in used_fields
        if field not in fields
    ]
    not_filterable = [
        field
        for field in used_fields
        if field in fields and not fields[field].get("filterable", False)
    ]
    return {
        "status": "ok" if not missing and not not_filterable else "blocked",
        "used_fields": used_fields,
        "missing_fields": missing,
        "not_filterable_fields": not_filterable,
        "assumed_filterable_fields": {},
    }


def apply_pipeline_open_views(
    *,
    instance_url: str,
    token: str,
    territory_config_path: Path,
    object_api_name: str,
    fy_label: str,
    update_config: bool,
    only_territory: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    config = load_json(territory_config_path)
    territories = config.get("territories")
    if not isinstance(territories, dict) or not territories:
        raise ValueError(f"{territory_config_path}: expected territories object")

    selected_names = [
        name
        for name in territories
        if (not only_territory or name == only_territory)
    ]
    unknown = [name for name in selected_names if name not in TERRITORY_FILTERS]
    if unknown:
        raise ValueError(f"Missing filter policies for: {', '.join(unknown)}")

    validation = validate_filter_fields(
        instance_url=instance_url,
        token=token,
        object_api_name=object_api_name,
        territories=selected_names,
    )
    if validation["status"] != "ok":
        return {
            "status": "blocked",
            "validation": validation,
            "applied": [],
            "skipped": [],
        }

    applied: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for territory in selected_names:
        payload = build_payload(territory, fy_label=fy_label)
        api_name = payload["listViewApiName"]
        if dry_run:
            skipped.append(
                {
                    "territory": territory,
                    "api_name": api_name,
                    "label": payload["label"],
                    "reason": "dry_run",
                    "filters": payload["filteredByInfo"],
                }
            )
            continue

        existing = get_list_info_by_name(
            instance_url=instance_url,
            token=token,
            object_api_name=object_api_name,
            list_view_api_name=api_name,
        )
        if existing:
            update_payload = dict(payload)
            update_payload.pop("listViewApiName", None)
            response = update_list_info_by_name(
                instance_url=instance_url,
                token=token,
                object_api_name=object_api_name,
                list_view_api_name=api_name,
                payload=update_payload,
            )
            action = "updated"
        else:
            response = create_list_info(
                instance_url=instance_url,
                token=token,
                object_api_name=object_api_name,
                payload=payload,
            )
            action = "created"

        list_reference = response.get("listReference") or {}
        list_view_id = list_reference.get("id")
        if not list_view_id:
            raise ValueError(f"{territory}: Salesforce response omitted list view id")
        response_label = html.unescape(str(response.get("label") or payload["label"]))
        territories[territory]["pipeline_open_list_view_id"] = list_view_id
        territories[territory]["pipeline_open_list_view_label"] = response_label
        applied.append(
            {
                "territory": territory,
                "action": action,
                "id": list_view_id,
                "api_name": api_name,
                "label": response_label,
                "filter_reason": FIELD_FILTER_REASON.get(territory),
                "filters": payload["filteredByInfo"],
            }
        )

    if update_config and applied and not dry_run:
        write_json(territory_config_path, config)

    return {
        "status": "ok",
        "territory_config_path": str(territory_config_path),
        "fy_label": fy_label,
        "object_api_name": object_api_name,
        "config_updated": bool(update_config and applied and not dry_run),
        "validation": validation,
        "applied": applied,
        "skipped": skipped,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG)
    parser.add_argument("--territory-config", type=Path, default=DEFAULT_TERRITORY_CONFIG)
    parser.add_argument("--object-api-name", default=DEFAULT_OBJECT)
    parser.add_argument("--fy-label", default=DEFAULT_FY_LABEL)
    parser.add_argument("--only-territory")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--update-config",
        action="store_true",
        help="Write pipeline_open_list_view_id/label back to the territory config.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    token, instance_url = get_auth(args.target_org)
    result = apply_pipeline_open_views(
        instance_url=instance_url,
        token=token,
        territory_config_path=args.territory_config,
        object_api_name=args.object_api_name,
        fy_label=args.fy_label,
        update_config=args.update_config,
        only_territory=args.only_territory,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2))
    return 1 if result["status"] != "ok" else 0


if __name__ == "__main__":
    sys.exit(main())
