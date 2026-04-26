#!/usr/bin/env python3
"""Create or update quarter-scoped Land PI views for director territories.

This script creates a clean, UI-API-addressable set of Opportunity list views
for PI tie-out work, then attaches PipelineInspectionListView records to them.

Why this exists:
- the legacy `PI ARR Forecast ...` list views in the org are not addressable via
  the standard Lightning list-view APIs
- directors still need a PI surface that is Land-only and quarter-scoped

Output:
- writes a config JSON with the new list view ids and PI ids so the verifier can
  validate the new surfaces without disturbing the canonical monthly config
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

try:
    from manage_pipeline_open_list_views import (  # noqa: E402
        TERRITORY_FILTERS,
        list_view_filter,
    )
except ModuleNotFoundError:  # pragma: no cover
    from scripts.manage_pipeline_open_list_views import (  # noqa: E402
        TERRITORY_FILTERS,
        list_view_filter,
    )


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_TERRITORY_CONFIG_PATH = REPO_ROOT / "config" / "sd_monthly_territories.json"
DEFAULT_OUTPUT_CONFIG_PATH = REPO_ROOT / "output" / "pi_land_forecast_views.json"
API_VERSION = "v66.0"
TARGET_ORG = "apro@simcorp.com"

OPEN_STAGES = [
    "1 - Prospecting",
    "2 - Discovery",
    "3 - Engagement",
    "4 - Shortlisted",
    "5 - Preferred",
    "6 - Contracting",
]
OPEN_FORECAST_CATEGORIES = ["Pipeline", "Best Case", "Commit"]
DISPLAY_COLUMNS = [
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
    "Type",
]


@dataclass(frozen=True)
class PeriodWindow:
    start: str
    end: str
    quarter_code: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target-org",
        default=TARGET_ORG,
        help=f"Salesforce org alias (default: {TARGET_ORG})",
    )
    parser.add_argument(
        "--as-of",
        default=str(date.today()),
        help="Anchor date used to resolve the next fiscal quarter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--close-date-literal",
        default="NEXT FISCAL QUARTER",
        help="CloseDate list-view filter literal",
    )
    parser.add_argument(
        "--output-config-path",
        default=str(DEFAULT_OUTPUT_CONFIG_PATH),
        help="Path to write the generated territory->view id registry JSON",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the generated config JSON to stdout as well",
    )
    return parser.parse_args()


def get_org_session(target_org: str) -> tuple[requests.Session, str]:
    result = subprocess.run(
        ["sf", "org", "display", "--target-org", target_org, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(result.stdout[result.stdout.find("{") :])["result"]
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {payload['accessToken']}"})
    return session, payload["instanceUrl"]


def soql_query(session: requests.Session, instance_url: str, query: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    url = f"{instance_url}/services/data/{API_VERSION}/query"
    params: dict[str, str] | None = {"q": query}
    while True:
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
        records.extend(payload.get("records", []))
        next_url = payload.get("nextRecordsUrl")
        if not next_url:
            return records
        url = f"{instance_url}{next_url}"
        params = None


def ui_api_request(
    session: requests.Session,
    instance_url: str,
    *,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = session.request(
        method,
        f"{instance_url}/services/data/{API_VERSION}{path}",
        json=body,
        timeout=60,
    )
    response.raise_for_status()
    if not response.text:
        return {}
    return response.json()


def sobject_request(
    session: requests.Session,
    instance_url: str,
    *,
    method: str,
    object_name: str,
    record_id: str | None = None,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    suffix = f"/{record_id}" if record_id else ""
    response = session.request(
        method,
        f"{instance_url}/services/data/{API_VERSION}/sobjects/{object_name}{suffix}",
        json=body,
        timeout=60,
    )
    response.raise_for_status()
    if not response.text:
        return {}
    return response.json()


def load_source_config() -> dict[str, Any]:
    return json.loads(SOURCE_TERRITORY_CONFIG_PATH.read_text())


def next_fiscal_quarter(
    session: requests.Session,
    instance_url: str,
    as_of: str,
) -> PeriodWindow:
    rows = soql_query(
        session,
        instance_url,
        "SELECT StartDate, EndDate "
        "FROM Period "
        f"WHERE Type = 'Quarter' AND StartDate > {as_of} "
        "ORDER BY StartDate ASC LIMIT 1",
    )
    if not rows:
        raise SystemExit(f"No next fiscal quarter found after {as_of}.")
    row = rows[0]
    start = row["StartDate"]
    end = row["EndDate"]
    dt = datetime.strptime(start, "%Y-%m-%d")
    quarter_code = f"Q{((dt.month - 1) // 3) + 1} {dt.year}"
    return PeriodWindow(start=start, end=end, quarter_code=quarter_code)


def normalize_api_name(name: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
    return compact[:80]


def get_existing_list_info(
    session: requests.Session,
    instance_url: str,
    api_name: str,
) -> dict[str, Any] | None:
    response = session.get(
        f"{instance_url}/services/data/{API_VERSION}/ui-api/list-info/Opportunity/{api_name}",
        timeout=60,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def build_list_payload(territory_label: str, quarter_code: str, close_date_literal: str) -> dict[str, Any]:
    if territory_label not in TERRITORY_FILTERS:
        raise ValueError(f"No PI list-view filter policy for territory: {territory_label}")
    label = f"PI ARR Forecast {territory_label} {quarter_code} Land"
    api_name = normalize_api_name(label)
    filtered_by_info = [
        list_view_filter(item["fieldApiName"], list(item["operandLabels"]))
        for item in TERRITORY_FILTERS[territory_label]
    ]
    filtered_by_info.extend(
        [
            list_view_filter("StageName", OPEN_STAGES),
            list_view_filter("Type", ["Land"]),
            list_view_filter("ForecastCategoryName", OPEN_FORECAST_CATEGORIES),
            list_view_filter("CloseDate", [close_date_literal]),
        ]
    )
    return {
        "label": label,
        "listViewApiName": api_name,
        "visibility": "Private",
        "scope": {"apiName": "my_team_territory"},
        "displayColumns": DISPLAY_COLUMNS,
        "filteredByInfo": filtered_by_info,
    }


def upsert_list_view(
    session: requests.Session,
    instance_url: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    api_name = payload["listViewApiName"]
    existing = get_existing_list_info(session, instance_url, api_name)
    if existing:
        response = ui_api_request(
            session,
            instance_url,
            method="PATCH",
            path=f"/ui-api/list-info/Opportunity/{api_name}",
            body=payload,
        )
        response["_action"] = "updated"
        return response
    response = ui_api_request(
        session,
        instance_url,
        method="POST",
        path="/ui-api/list-info/Opportunity",
        body=payload,
    )
    response["_action"] = "created"
    return response


def get_pi_template(
    session: requests.Session,
    instance_url: str,
    legacy_list_view_id: str,
) -> dict[str, Any]:
    rows = soql_query(
        session,
        instance_url,
        "SELECT Id, SummaryField, ChangePeriodLiteralType "
        "FROM PipelineInspectionListView "
        f"WHERE ListViewId = '{legacy_list_view_id}' "
        "ORDER BY CreatedDate DESC LIMIT 1",
    )
    if not rows:
        raise SystemExit(
            f"No PipelineInspectionListView template found for legacy list view {legacy_list_view_id}."
        )
    return rows[0]


def upsert_pipeline_inspection_view(
    session: requests.Session,
    instance_url: str,
    *,
    list_view_id: str,
    pi_start_date: str,
    pi_end_date: str,
    summary_field: str,
    change_period_literal_type: str | None,
) -> dict[str, Any]:
    existing = soql_query(
        session,
        instance_url,
        "SELECT Id FROM PipelineInspectionListView "
        f"WHERE ListViewId = '{list_view_id}' "
        "ORDER BY CreatedDate DESC LIMIT 1",
    )
    payload = {
        "StartDate": pi_start_date,
        "EndDate": pi_end_date,
        "SummaryField": summary_field,
        "ChangePeriodLiteralType": change_period_literal_type or "START_OF_THE_PERIOD",
        "DateLiteralType": None,
    }
    api_payload = {key: value for key, value in payload.items() if value is not None}
    if existing:
        sobject_request(
            session,
            instance_url,
            method="PATCH",
            object_name="PipelineInspectionListView",
            record_id=existing[0]["Id"],
            body=api_payload,
        )
        payload["Id"] = existing[0]["Id"]
        payload["_action"] = "updated"
        return payload
    payload["ListViewId"] = list_view_id
    created = sobject_request(
        session,
        instance_url,
        method="POST",
        object_name="PipelineInspectionListView",
        body={key: value for key, value in payload.items() if value is not None},
    )
    payload["Id"] = created.get("id")
    payload["_action"] = "created"
    return payload


def main() -> None:
    args = parse_args()
    session, instance_url = get_org_session(args.target_org)
    source = load_source_config()
    next_period = next_fiscal_quarter(session, instance_url, args.as_of)

    output: dict[str, Any] = {
        "_meta": {
            "source_config": str(SOURCE_TERRITORY_CONFIG_PATH),
            "as_of": args.as_of,
            "close_date_literal": args.close_date_literal,
            "pi_start_date": next_period.start,
            "pi_end_date": next_period.end,
            "quarter_code": next_period.quarter_code,
        },
        "territories": {},
    }

    for territory_label, cfg in source["territories"].items():
        payload = build_list_payload(
            territory_label=territory_label,
            quarter_code=next_period.quarter_code,
            close_date_literal=args.close_date_literal,
        )
        list_info = upsert_list_view(session, instance_url, payload)
        list_view_id = list_info["listReference"]["id"]
        template = get_pi_template(session, instance_url, cfg["pi_list_view_id"])
        pi_record: dict[str, Any]
        try:
            pi_record = upsert_pipeline_inspection_view(
                session,
                instance_url,
                list_view_id=list_view_id,
                pi_start_date=next_period.start,
                pi_end_date=next_period.end,
                summary_field=template["SummaryField"],
                change_period_literal_type=template.get("ChangePeriodLiteralType"),
            )
        except requests.HTTPError as exc:
            detail = exc.response.text if exc.response is not None else str(exc)
            pi_record = {
                "Id": None,
                "_action": "error",
                "error": detail,
            }
        output["territories"][territory_label] = {
            "director": cfg["director"],
            "soql_where": cfg["soql_where"],
            "pi_list_view_id": list_view_id,
            "pi_list_view_label": payload["label"],
            "pi_list_view_api_name": payload["listViewApiName"],
            "pi_list_view_action": list_info["_action"],
            "pipeline_inspection_id": pi_record["Id"],
            "pipeline_inspection_action": pi_record["_action"],
            "pipeline_inspection_error": pi_record.get("error"),
        }

    output_path = Path(args.output_config_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2))
    if args.json:
        print(json.dumps(output, indent=2))
    else:
        print(f"Wrote {output_path}")
        for territory_label, cfg in output["territories"].items():
            print(
                f"{territory_label}: "
                f"{cfg['pi_list_view_api_name']} -> {cfg['pi_list_view_id']} "
                f"({cfg['pi_list_view_action']}), PI {cfg['pipeline_inspection_id']} "
                f"({cfg['pipeline_inspection_action']})"
            )


if __name__ == "__main__":
    main()
