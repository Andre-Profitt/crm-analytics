#!/usr/bin/env python3
"""Inventory and clone Salesforce Pipeline Inspection list views.

This script is intentionally small and org-specific. It uses the authenticated
`sf` CLI session for `apro@simcorp.com` by default, then calls the Salesforce
REST API directly for PipelineInspectionListView CRUD.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"


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
    req.add_header("Content-Type", "application/json")
    data = None if body is None else json.dumps(body).encode("utf-8")
    try:
        with urllib.request.urlopen(req, data=data, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        raise SystemExit(f"{method} {path} failed: HTTP {exc.code}: {detail}") from exc


def soql_query(*, instance_url: str, token: str, soql: str) -> list[dict[str, Any]]:
    encoded = urllib.parse.quote(soql, safe="")
    payload = api_request(
        method="GET",
        instance_url=instance_url,
        token=token,
        path=f"/query?q={encoded}",
    )
    return payload.get("records", [])


def list_views(*, instance_url: str, token: str) -> list[dict[str, Any]]:
    records = soql_query(
        instance_url=instance_url,
        token=token,
        soql=(
            "SELECT Id, ListViewId, DateLiteralType, StartDate, EndDate, "
            "SummaryField, ChangePeriodLiteralType, UserId, MarketSegments, "
            "ViewType, IsSystemManaged FROM PipelineInspectionListView "
            "ORDER BY IsSystemManaged DESC, Id ASC"
        ),
    )
    list_view_ids = sorted(
        {record["ListViewId"] for record in records if record.get("ListViewId")}
    )
    list_views_by_id: dict[str, dict[str, Any]] = {}
    if list_view_ids:
        chunks = [list_view_ids[i : i + 100] for i in range(0, len(list_view_ids), 100)]
        for chunk in chunks:
            in_clause = ",".join(f"'{item}'" for item in chunk)
            rows = soql_query(
                instance_url=instance_url,
                token=token,
                soql=(
                    "SELECT Id, Name, DeveloperName, SobjectType "
                    f"FROM ListView WHERE Id IN ({in_clause})"
                ),
            )
            list_views_by_id.update({row["Id"]: row for row in rows})

    output: list[dict[str, Any]] = []
    for record in records:
        list_view = list_views_by_id.get(record.get("ListViewId") or "", {})
        output.append(
            {
                "pi_id": record.get("Id"),
                "list_view_id": record.get("ListViewId"),
                "list_view_name": list_view.get("Name"),
                "list_view_developer_name": list_view.get("DeveloperName"),
                "date_literal_type": record.get("DateLiteralType"),
                "start_date": record.get("StartDate"),
                "end_date": record.get("EndDate"),
                "summary_field": record.get("SummaryField"),
                "change_period_literal_type": record.get("ChangePeriodLiteralType"),
                "user_id": record.get("UserId"),
                "view_type": record.get("ViewType"),
                "is_system_managed": record.get("IsSystemManaged"),
            }
        )
    return output


def clone_view(
    *,
    instance_url: str,
    token: str,
    template_pi_id: str,
    list_view_id: str,
    date_literal_type: str | None,
    start_date: str | None,
    end_date: str | None,
    summary_field: str | None,
    change_period_literal_type: str | None,
    user_id: str | None,
    market_segments: str | None,
) -> dict[str, Any]:
    template = api_request(
        method="GET",
        instance_url=instance_url,
        token=token,
        path=f"/sobjects/PipelineInspectionListView/{template_pi_id}",
    )
    writable = {
        "ListViewId": list_view_id,
        "DateLiteralType": template.get("DateLiteralType"),
        "StartDate": template.get("StartDate"),
        "EndDate": template.get("EndDate"),
        "SummaryField": template.get("SummaryField"),
        "ChangePeriodLiteralType": template.get("ChangePeriodLiteralType"),
        "UserId": template.get("UserId"),
        "MarketSegments": template.get("MarketSegments"),
    }

    overrides = {
        "DateLiteralType": date_literal_type,
        "StartDate": start_date,
        "EndDate": end_date,
        "SummaryField": summary_field,
        "ChangePeriodLiteralType": change_period_literal_type,
        "UserId": user_id,
        "MarketSegments": market_segments,
    }
    for key, value in overrides.items():
        if value is not None:
            writable[key] = value

    # Date literal and explicit start/end dates are mutually exclusive in practice.
    if writable.get("DateLiteralType"):
        writable["StartDate"] = None
        writable["EndDate"] = None

    payload = {k: v for k, v in writable.items() if v is not None}
    return api_request(
        method="POST",
        instance_url=instance_url,
        token=token,
        path="/sobjects/PipelineInspectionListView",
        body=payload,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List current Pipeline Inspection views.")

    clone = subparsers.add_parser(
        "clone",
        help="Clone a PipelineInspectionListView from an existing template.",
    )
    clone.add_argument("--template-pi-id", required=True)
    clone.add_argument("--list-view-id", required=True)
    clone.add_argument("--date-literal-type")
    clone.add_argument("--start-date")
    clone.add_argument("--end-date")
    clone.add_argument("--summary-field")
    clone.add_argument("--change-period-literal-type")
    clone.add_argument("--user-id")
    clone.add_argument("--market-segments")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    token, instance_url = get_auth(args.target_org)

    if args.command == "list":
        print(json.dumps(list_views(instance_url=instance_url, token=token), indent=2))
        return 0

    if args.command == "clone":
        created = clone_view(
            instance_url=instance_url,
            token=token,
            template_pi_id=args.template_pi_id,
            list_view_id=args.list_view_id,
            date_literal_type=args.date_literal_type,
            start_date=args.start_date,
            end_date=args.end_date,
            summary_field=args.summary_field,
            change_period_literal_type=args.change_period_literal_type,
            user_id=args.user_id,
            market_segments=args.market_segments,
        )
        print(json.dumps(created, indent=2))
        return 0

    parser.error(f"Unhandled command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
