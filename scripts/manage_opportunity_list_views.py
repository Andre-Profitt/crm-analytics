#!/usr/bin/env python3
"""Manage Opportunity list views through Salesforce UI API.

This script exists because Metadata API is unavailable in the target org, but
Opportunity list views are still createable/updateable through the UI API.
It is intentionally org-specific and uses the authenticated `sf` CLI session.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"
DEFAULT_OBJECT = "Opportunity"
DEFAULT_COLUMNS = [
    "Name",
    "Account.Name",
    "StageName",
    "CloseDate",
    "Amount",
    "OpportunityScore.Score",
    "APTS_Forecast_ARR__c",
    "ForecastCategoryName",
    "ZIMIT__zOwner__c",
    "NextStep",
    "Consensus__cDaysSinceLastActivity__c",
    "Account_Unit_Group__c",
    "Account_Unit__c",
    "Territory2.Name",
    "PushCount",
]
SUPPORTED_FILTER_FIELDS = {
    "Account_Unit_Group__c",
    "Account_Unit__c",
    "Sales_Region__c",
    "StageName",
}
DEFAULT_STAGE_FILTER = [
    "1 - Prospecting",
    "2 - Discovery",
    "3 - Engagement",
    "4 - Shortlisted",
    "5 - Preferred",
    "6 - Contracting",
    "8 - Won",
    "0 - Lost",
]


def run(cmd: list[str]) -> str:
    completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return completed.stdout


def get_auth(target_org: str) -> tuple[str, str]:
    payload = json.loads(
        run(["sf", "org", "display", "--target-org", target_org, "--json"])
    )
    result = payload["result"]
    return result["accessToken"], result["instanceUrl"]


def api_request(
    *,
    method: str,
    instance_url: str,
    token: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> Any:
    url = f"{instance_url}/services/data/{API_VERSION}{path}"
    req = urllib.request.Request(url=url, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if body is not None:
        req.add_header("Content-Type", "application/json")
    data = None if body is None else json.dumps(body).encode("utf-8")
    try:
        with urllib.request.urlopen(req, data=data, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        raise SystemExit(f"{method} {path} failed: HTTP {exc.code}: {detail}") from exc


def get_list_info_by_name(
    *,
    instance_url: str,
    token: str,
    object_api_name: str,
    list_view_api_name: str,
) -> dict[str, Any] | None:
    try:
        return api_request(
            method="GET",
            instance_url=instance_url,
            token=token,
            path=f"/ui-api/list-info/{object_api_name}/{list_view_api_name}",
        )
    except SystemExit as exc:
        if "HTTP 404" in str(exc):
            return None
        raise


def search_list_infos(
    *,
    instance_url: str,
    token: str,
    object_api_name: str,
    query: str,
    page_size: int = 100,
) -> dict[str, Any]:
    encoded_query = urllib.parse.quote(query, safe="")
    return api_request(
        method="GET",
        instance_url=instance_url,
        token=token,
        path=f"/ui-api/list-info/{object_api_name}?pageSize={page_size}&q={encoded_query}",
    )


def create_list_info(
    *,
    instance_url: str,
    token: str,
    object_api_name: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return api_request(
        method="POST",
        instance_url=instance_url,
        token=token,
        path=f"/ui-api/list-info/{object_api_name}",
        body=payload,
    )


def update_list_info_by_name(
    *,
    instance_url: str,
    token: str,
    object_api_name: str,
    list_view_api_name: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return api_request(
        method="PATCH",
        instance_url=instance_url,
        token=token,
        path=f"/ui-api/list-info/{object_api_name}/{list_view_api_name}",
        body=payload,
    )


def delete_list_info_by_name(
    *,
    instance_url: str,
    token: str,
    object_api_name: str,
    list_view_api_name: str,
) -> dict[str, Any]:
    return api_request(
        method="DELETE",
        instance_url=instance_url,
        token=token,
        path=f"/ui-api/list-info/{object_api_name}/{list_view_api_name}",
    )


def normalize_api_name(name: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
    return compact[:80]


def build_filter(field_api_name: str, value: Any) -> dict[str, Any]:
    if field_api_name not in SUPPORTED_FILTER_FIELDS:
        raise ValueError(f"Unsupported filter field for UI API list views: {field_api_name}")

    if isinstance(value, dict):
        operator = value.get("operator")
        if operator == "notEqual":
            return {
                "fieldApiName": field_api_name,
                "operator": "Not Equal",
                "operandLabels": [value["value"]],
            }
        if operator == "in":
            return {
                "fieldApiName": field_api_name,
                "operator": "Equals",
                "operandLabels": value["values"],
            }
        raise ValueError(f"Unsupported filter operator for {field_api_name}: {value!r}")

    operator = "Contains" if field_api_name == "Sales_Region__c" else "Equals"
    return {
        "fieldApiName": field_api_name,
        "operator": operator,
        "operandLabels": [value],
    }


def build_payload(territory: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    filters = territory.get("filters", {})
    unsupported: list[str] = []
    filtered_by_info: list[dict[str, Any]] = []
    for field, value in filters.items():
        if field == "scope":
            continue
        if field not in SUPPORTED_FILTER_FIELDS:
            unsupported.append(field)
            continue
        filtered_by_info.append(build_filter(field, value))

    # Every territory view should keep the common open/won/lost stage set.
    if not any(item["fieldApiName"] == "StageName" for item in filtered_by_info):
        filtered_by_info.append(
            {
                "fieldApiName": "StageName",
                "operator": "Equals",
                "operandLabels": DEFAULT_STAGE_FILTER,
            }
        )

    payload = {
        "label": territory["list_view_name"],
        "listViewApiName": territory.get("list_view_api_name")
        or normalize_api_name(territory["list_view_name"]),
        "visibility": "Private",
        "scope": {"apiName": filters.get("scope", "my_team_territory")},
        "displayColumns": list(DEFAULT_COLUMNS),
        "filteredByInfo": filtered_by_info,
    }
    return payload, unsupported


def apply_blueprint(
    *,
    instance_url: str,
    token: str,
    blueprint_path: Path,
    object_api_name: str,
    include_existing: bool,
) -> dict[str, Any]:
    blueprint = json.loads(blueprint_path.read_text())
    results: dict[str, Any] = {"applied": [], "skipped": []}
    for territory in blueprint.get("territories", []):
        if territory.get("status") == "existing" and not include_existing:
            results["skipped"].append(
                {
                    "territory": territory["territory"],
                    "reason": "already-covered-in-blueprint",
                }
            )
            continue

        payload, unsupported = build_payload(territory)
        if unsupported:
            results["skipped"].append(
                {
                    "territory": territory["territory"],
                    "api_name": payload["listViewApiName"],
                    "reason": "unsupported-filters",
                    "unsupported_filters": unsupported,
                }
            )
            continue

        existing = get_list_info_by_name(
            instance_url=instance_url,
            token=token,
            object_api_name=object_api_name,
            list_view_api_name=payload["listViewApiName"],
        )
        if existing:
            updated = update_list_info_by_name(
                instance_url=instance_url,
                token=token,
                object_api_name=object_api_name,
                list_view_api_name=payload["listViewApiName"],
                payload=payload,
            )
            results["applied"].append(
                {
                    "territory": territory["territory"],
                    "action": "updated",
                    "id": updated["listReference"]["id"],
                    "api_name": payload["listViewApiName"],
                    "label": updated["label"],
                }
            )
        else:
            created = create_list_info(
                instance_url=instance_url,
                token=token,
                object_api_name=object_api_name,
                payload=payload,
            )
            results["applied"].append(
                {
                    "territory": territory["territory"],
                    "action": "created",
                    "id": created["listReference"]["id"],
                    "api_name": payload["listViewApiName"],
                    "label": created["label"],
                }
            )
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG)
    parser.add_argument("--object-api-name", default=DEFAULT_OBJECT)
    subparsers = parser.add_subparsers(dest="command", required=True)

    search = subparsers.add_parser("search", help="Search list views by query.")
    search.add_argument("--query", required=True)
    search.add_argument("--page-size", type=int, default=100)

    delete = subparsers.add_parser("delete", help="Delete a list view by API name.")
    delete.add_argument("--list-view-api-name", required=True)

    apply_cmd = subparsers.add_parser(
        "apply-blueprint",
        help="Create or update list views from the PI territory blueprint.",
    )
    apply_cmd.add_argument(
        "--blueprint",
        default="config/pi_territory_blueprint_2026-04-09.json",
    )
    apply_cmd.add_argument(
        "--include-existing",
        action="store_true",
        help="Also update blueprint entries already marked as existing.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    token, instance_url = get_auth(args.target_org)

    if args.command == "search":
        print(
            json.dumps(
                search_list_infos(
                    instance_url=instance_url,
                    token=token,
                    object_api_name=args.object_api_name,
                    query=args.query,
                    page_size=args.page_size,
                ),
                indent=2,
            )
        )
        return 0

    if args.command == "delete":
        print(
            json.dumps(
                delete_list_info_by_name(
                    instance_url=instance_url,
                    token=token,
                    object_api_name=args.object_api_name,
                    list_view_api_name=args.list_view_api_name,
                ),
                indent=2,
            )
        )
        return 0

    if args.command == "apply-blueprint":
        results = apply_blueprint(
            instance_url=instance_url,
            token=token,
            blueprint_path=Path(args.blueprint),
            object_api_name=args.object_api_name,
            include_existing=args.include_existing,
        )
        print(json.dumps(results, indent=2))
        return 0

    parser.error(f"Unhandled command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
