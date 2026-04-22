#!/usr/bin/env python3
"""Verify PI-vs-forecast Land alignment for director territories.

This verifier treats PI as the director action surface and the forecast as the
control surface. For each director territory it reports:

1. PI scope drift:
   - raw PI rows that fall outside the intended forecast-eligible scope
   - missing rows that should be in PI but are not
2. Forecast tie-out:
   - Land opportunities visible in PI but missing positive ForecastingFact rows
   - Land opportunities backed by forecast but missing from PI
   - positive forecast-backed opportunities that are not Type = Land

Default scope is the fiscal quarter containing --as-of. Use --quarter-start to
target a specific quarter like Q3 FY26.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
TERRITORY_CONFIG_PATH = REPO_ROOT / "config" / "sd_monthly_territories.json"
API_VERSION = "v66.0"
TARGET_ORG = "apro@simcorp.com"

ACCOUNT_EXCLUDE = (
    "AND (NOT Account.Name LIKE '%simcorp%') "
    "AND (NOT Account.Name LIKE '%test%') "
    "AND (NOT Account.Name LIKE '%delete%') "
)
OWNER_EXCLUDE = (
    "AND (NOT Owner.Name LIKE '%Sabiniewicz%') "
    "AND (NOT Owner.Name LIKE '%Profit%') "
)
OPEN_FORECAST_CATEGORIES = ("Pipeline", "Best Case", "Commit")


@dataclass(frozen=True)
class PeriodWindow:
    id: str
    start: str
    end: str
    label: str
    fiscal_year_name: str | None


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
        help="Date used to resolve the active quarter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--quarter-start",
        help="Override the target quarter by its start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--territory",
        action="append",
        help="Limit output to one or more territory labels from sd_monthly_territories.json",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of text",
    )
    parser.add_argument(
        "--territory-config-path",
        default=str(TERRITORY_CONFIG_PATH),
        help="Path to a territory config JSON with a top-level territories map",
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


def soql_query(
    session: requests.Session,
    instance_url: str,
    query: str,
) -> list[dict[str, Any]]:
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


def current_quarter_period(
    session: requests.Session,
    instance_url: str,
    *,
    as_of: str | None = None,
    quarter_start: str | None = None,
) -> PeriodWindow:
    if quarter_start:
        query = (
            "SELECT Id, QuarterLabel, StartDate, EndDate, FiscalYearSettings.Name "
            "FROM Period "
            f"WHERE Type = 'Quarter' AND StartDate = {quarter_start} "
            "ORDER BY StartDate DESC LIMIT 1"
        )
    else:
        query = (
            "SELECT Id, QuarterLabel, StartDate, EndDate, FiscalYearSettings.Name "
            "FROM Period "
            f"WHERE Type = 'Quarter' AND StartDate <= {as_of} AND EndDate >= {as_of} "
            "ORDER BY StartDate DESC LIMIT 1"
        )
    rows = soql_query(session, instance_url, query)
    if not rows:
        if quarter_start:
            raise SystemExit(f"No fiscal quarter Period found for start date {quarter_start}.")
        raise SystemExit(f"No active fiscal quarter Period found for as-of date {as_of}.")
    row = rows[0]
    return PeriodWindow(
        id=row["Id"],
        start=row["StartDate"],
        end=row["EndDate"],
        label=row.get("QuarterLabel") or row["StartDate"],
        fiscal_year_name=(row.get("FiscalYearSettings") or {}).get("Name"),
    )


def load_territories(config_path: Path, selected: list[str] | None) -> dict[str, Any]:
    config = json.loads(config_path.read_text())
    territories = config.get("territories", {})
    if not selected:
        return territories
    unknown = sorted(set(selected) - set(territories))
    if unknown:
        raise SystemExit(
            f"Unknown territories {unknown}. Known: {sorted(territories)}"
        )
    return {label: territories[label] for label in selected}


def fetch_pi_records(
    session: requests.Session,
    instance_url: str,
    list_view_id: str,
) -> list[dict[str, Any]]:
    url = f"{instance_url}/services/data/{API_VERSION}/ui-api/list-records/{list_view_id}?pageSize=200"
    records: list[dict[str, Any]] = []
    while url:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        payload = response.json()
        records.extend(payload.get("records", []))
        next_url = payload.get("nextPageUrl")
        url = f"{instance_url}{next_url}" if next_url else None
    return records


def ui_value(record: dict[str, Any], field_name: str) -> Any:
    field = (record.get("fields") or {}).get(field_name)
    if not field:
        return None
    return field.get("value")


def ui_display(record: dict[str, Any], field_name: str) -> str | None:
    field = (record.get("fields") or {}).get(field_name)
    if not field:
        return None
    return field.get("displayValue") or field.get("value")


def pi_exclusion_reason(record: dict[str, Any], period: PeriodWindow) -> str | None:
    record_type = ui_value(record, "Type")
    if record_type != "Land":
        return "non_land"
    if bool(ui_value(record, "IsClosed")):
        return "closed"
    close_date = ui_value(record, "CloseDate")
    if not close_date or close_date < period.start or close_date > period.end:
        return "outside_quarter"
    forecast_category = ui_value(record, "ForecastCategoryName")
    if forecast_category not in OPEN_FORECAST_CATEGORIES:
        return "non_open_forecast_category"
    return None


def build_record_summary(
    *,
    row: dict[str, Any] | None = None,
    record: dict[str, Any] | None = None,
    fact_amount: float | None = None,
) -> dict[str, Any]:
    if row is not None:
        return {
            "id": row["Id"],
            "name": row.get("Name"),
            "type": row.get("Type"),
            "forecast_category": row.get("ForecastCategoryName"),
            "stage": row.get("StageName"),
            "close_date": row.get("CloseDate"),
            "owner": (row.get("Owner") or {}).get("Name"),
            "account": (row.get("Account") or {}).get("Name"),
            "forecast_arr_eur": float(row.get("ForecastARR") or 0),
            "fact_amount_eur": fact_amount,
        }
    assert record is not None
    return {
        "id": record.get("id"),
        "name": ui_value(record, "Name"),
        "type": ui_value(record, "Type"),
        "forecast_category": ui_value(record, "ForecastCategoryName"),
        "stage": ui_value(record, "StageName"),
        "close_date": ui_value(record, "CloseDate"),
        "owner": ui_display(record, "OwnerId"),
        "account": ui_display(record, "AccountId"),
        "forecast_arr_eur": ui_value(record, "APTS_Forecast_ARR__c"),
        "fact_amount_eur": fact_amount,
    }


def query_expected_pi_scope(
    session: requests.Session,
    instance_url: str,
    soql_where: str,
    period: PeriodWindow,
) -> list[dict[str, Any]]:
    query = (
        "SELECT Id, Name, Type, ForecastCategoryName, StageName, CloseDate, "
        "Owner.Name, Account.Name, convertCurrency(APTS_Forecast_ARR__c) ForecastARR "
        "FROM Opportunity "
        f"WHERE {soql_where} "
        "AND IsClosed = false "
        "AND Type = 'Land' "
        "AND ForecastCategoryName IN ('Pipeline', 'Best Case', 'Commit') "
        f"AND CloseDate >= {period.start} AND CloseDate <= {period.end} "
        f"{ACCOUNT_EXCLUDE}{OWNER_EXCLUDE}"
    )
    return soql_query(session, instance_url, query)


def query_forecasting_fact_rows(
    session: requests.Session,
    instance_url: str,
    period: PeriodWindow,
) -> list[dict[str, Any]]:
    query = (
        "SELECT Id, OpportunityId, ForecastCategoryName, TargetValue "
        "FROM ForecastingFact "
        f"WHERE PeriodId = '{period.id}' "
        "AND TargetValue > 0 "
        "AND OpportunityId != null "
    )
    return soql_query(session, instance_url, query)


def query_fact_backed_opportunities(
    session: requests.Session,
    instance_url: str,
    *,
    soql_where: str,
    period: PeriodWindow,
    opportunity_ids: set[str],
    type_filter: str,
) -> list[dict[str, Any]]:
    if not opportunity_ids:
        return []
    ordered = sorted(opportunity_ids)
    rows: list[dict[str, Any]] = []
    for start in range(0, len(ordered), 200):
        chunk = ordered[start : start + 200]
        ids = ",".join(f"'{item}'" for item in chunk)
        query = (
            "SELECT Id, Name, Type, ForecastCategoryName, StageName, CloseDate, "
            "Owner.Name, Account.Name, convertCurrency(APTS_Forecast_ARR__c) ForecastARR "
            "FROM Opportunity "
            f"WHERE Id IN ({ids}) "
            f"AND {soql_where} "
            "AND IsClosed = false "
            f"AND Type {type_filter} "
            "AND ForecastCategoryName IN ('Pipeline', 'Best Case', 'Commit') "
            f"AND CloseDate >= {period.start} AND CloseDate <= {period.end} "
            f"{ACCOUNT_EXCLUDE}{OWNER_EXCLUDE}"
        )
        rows.extend(soql_query(session, instance_url, query))
    return rows


def collect_opportunity_details(
    session: requests.Session,
    instance_url: str,
    opportunity_ids: set[str],
) -> dict[str, dict[str, Any]]:
    if not opportunity_ids:
        return {}
    ordered = sorted(opportunity_ids)
    rows: dict[str, dict[str, Any]] = {}
    for start in range(0, len(ordered), 200):
        chunk = ordered[start : start + 200]
        ids = ",".join(f"'{item}'" for item in chunk)
        query = (
            "SELECT Id, Name, Type, ForecastCategoryName, StageName, CloseDate, "
            "Owner.Name, Account.Name, convertCurrency(APTS_Forecast_ARR__c) ForecastARR "
            f"FROM Opportunity WHERE Id IN ({ids})"
        )
        for row in soql_query(session, instance_url, query):
            rows[row["Id"]] = row
    return rows


def top_examples(
    ids: list[str] | set[str],
    details: dict[str, dict[str, Any]],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for opp_id in sorted(ids):
        row = details.get(opp_id)
        if not row:
            continue
        examples.append(
            build_record_summary(
                row=row,
            )
        )
        if len(examples) >= limit:
            break
    return examples


def top_pi_examples(
    records: list[dict[str, Any]],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for record in records[:limit]:
        out.append(build_record_summary(record=record))
    return out


def verify_territory(
    session: requests.Session,
    instance_url: str,
    territory_label: str,
    territory_cfg: dict[str, Any],
    period: PeriodWindow,
    fact_opportunity_ids: set[str],
) -> dict[str, Any]:
    raw_pi_records = fetch_pi_records(session, instance_url, territory_cfg["pi_list_view_id"])
    pi_raw_ids = {record["id"] for record in raw_pi_records}
    excluded_counter: Counter[str] = Counter()
    excluded_records: dict[str, list[dict[str, Any]]] = {}
    pi_eligible_records: list[dict[str, Any]] = []
    for record in raw_pi_records:
        reason = pi_exclusion_reason(record, period)
        if reason:
            excluded_counter[reason] += 1
            excluded_records.setdefault(reason, []).append(record)
            continue
        pi_eligible_records.append(record)
    pi_eligible_ids = {record["id"] for record in pi_eligible_records}

    expected_rows = query_expected_pi_scope(
        session,
        instance_url,
        territory_cfg["soql_where"],
        period,
    )
    expected_ids = {row["Id"] for row in expected_rows}

    land_fact_rows = query_fact_backed_opportunities(
        session,
        instance_url,
        soql_where=territory_cfg["soql_where"],
        period=period,
        opportunity_ids=fact_opportunity_ids,
        type_filter="= 'Land'",
    )
    forecast_land_ids = {row["Id"] for row in land_fact_rows}
    forecast_land_amount = round(
        sum(float(row.get("ForecastARR") or 0) for row in land_fact_rows),
        2,
    )

    non_land_fact_rows = query_fact_backed_opportunities(
        session,
        instance_url,
        soql_where=territory_cfg["soql_where"],
        period=period,
        opportunity_ids=fact_opportunity_ids,
        type_filter="!= 'Land'",
    )
    forecast_non_land_ids = {row["Id"] for row in non_land_fact_rows}
    forecast_non_land_amount = round(
        sum(float(row.get("ForecastARR") or 0) for row in non_land_fact_rows),
        2,
    )

    detail_ids = expected_ids | pi_raw_ids | forecast_land_ids | forecast_non_land_ids
    opportunity_details = collect_opportunity_details(session, instance_url, detail_ids)

    pi_scope_missing = expected_ids - pi_eligible_ids
    pi_scope_extra = pi_eligible_ids - expected_ids
    in_pi_not_in_forecast = pi_eligible_ids - forecast_land_ids
    in_forecast_not_in_pi = forecast_land_ids - pi_eligible_ids

    return {
        "territory": territory_label,
        "director": territory_cfg["director"],
        "pi_list_view_id": territory_cfg["pi_list_view_id"],
        "period_start": period.start,
        "period_end": period.end,
        "pi_scope": {
            "raw_count": len(raw_pi_records),
            "eligible_count": len(pi_eligible_ids),
            "expected_count": len(expected_ids),
            "raw_excluded_breakdown": dict(sorted(excluded_counter.items())),
            "raw_excluded_examples": {
                reason: top_pi_examples(records)
                for reason, records in excluded_records.items()
            },
            "missing_from_pi_count": len(pi_scope_missing),
            "missing_from_pi_examples": top_examples(pi_scope_missing, opportunity_details),
            "unexpected_in_pi_count": len(pi_scope_extra),
            "unexpected_in_pi_examples": top_examples(pi_scope_extra, opportunity_details),
        },
        "forecast_tieout": {
            "forecast_land_count": len(forecast_land_ids),
            "forecast_land_weighted_arr_eur": forecast_land_amount,
            "forecast_non_land_count": len(forecast_non_land_ids),
            "forecast_non_land_weighted_arr_eur": forecast_non_land_amount,
            "in_pi_not_in_forecast_count": len(in_pi_not_in_forecast),
            "in_pi_not_in_forecast_examples": top_examples(
                in_pi_not_in_forecast,
                opportunity_details,
            ),
            "in_forecast_not_in_pi_count": len(in_forecast_not_in_pi),
            "in_forecast_not_in_pi_examples": top_examples(
                in_forecast_not_in_pi,
                opportunity_details,
            ),
            "forecast_non_land_examples": top_examples(
                forecast_non_land_ids,
                opportunity_details,
            ),
        },
    }


def summarize_results(results: list[dict[str, Any]], period: PeriodWindow) -> str:
    lines = [
        f"PI vs forecast Land alignment for {period.label} {period.fiscal_year_name or ''}".strip(),
        f"Quarter window: {period.start}..{period.end}",
    ]
    totals = {
        "pi_raw": 0,
        "pi_eligible": 0,
        "pi_expected": 0,
        "pi_missing": 0,
        "pi_unexpected": 0,
        "pi_not_in_forecast": 0,
        "forecast_not_in_pi": 0,
        "forecast_non_land": 0,
        "forecast_non_land_amount": 0.0,
    }
    for result in results:
        pi_scope = result["pi_scope"]
        tieout = result["forecast_tieout"]
        totals["pi_raw"] += pi_scope["raw_count"]
        totals["pi_eligible"] += pi_scope["eligible_count"]
        totals["pi_expected"] += pi_scope["expected_count"]
        totals["pi_missing"] += pi_scope["missing_from_pi_count"]
        totals["pi_unexpected"] += pi_scope["unexpected_in_pi_count"]
        totals["pi_not_in_forecast"] += tieout["in_pi_not_in_forecast_count"]
        totals["forecast_not_in_pi"] += tieout["in_forecast_not_in_pi_count"]
        totals["forecast_non_land"] += tieout["forecast_non_land_count"]
        totals["forecast_non_land_amount"] += tieout["forecast_non_land_weighted_arr_eur"]
    lines.append(
        "Topline: "
        f"PI raw={totals['pi_raw']}, eligible={totals['pi_eligible']}, expected={totals['pi_expected']}, "
        f"missing_from_pi={totals['pi_missing']}, unexpected_in_pi={totals['pi_unexpected']}, "
        f"in_pi_not_in_forecast={totals['pi_not_in_forecast']}, "
        f"in_forecast_not_in_pi={totals['forecast_not_in_pi']}, "
        f"forecast_non_land={totals['forecast_non_land']} "
        f"({totals['forecast_non_land_amount']:,.2f} EUR)"
    )
    for result in results:
        pi_scope = result["pi_scope"]
        tieout = result["forecast_tieout"]
        lines.append(
            f"{result['territory']}: "
            f"pi raw={pi_scope['raw_count']} eligible={pi_scope['eligible_count']} expected={pi_scope['expected_count']} | "
            f"pi missing={pi_scope['missing_from_pi_count']} unexpected={pi_scope['unexpected_in_pi_count']} | "
            f"pi-not-forecast={tieout['in_pi_not_in_forecast_count']} "
            f"forecast-not-pi={tieout['in_forecast_not_in_pi_count']} "
            f"forecast-non-land={tieout['forecast_non_land_count']}"
        )
        if pi_scope["raw_excluded_breakdown"]:
            breakdown = ", ".join(
                f"{key}={value}" for key, value in pi_scope["raw_excluded_breakdown"].items()
            )
            lines.append(f"  raw exclusions: {breakdown}")
        if pi_scope["missing_from_pi_examples"]:
            examples = ", ".join(item["name"] for item in pi_scope["missing_from_pi_examples"])
            lines.append(f"  missing from PI: {examples}")
        if pi_scope["unexpected_in_pi_examples"]:
            examples = ", ".join(item["name"] for item in pi_scope["unexpected_in_pi_examples"])
            lines.append(f"  unexpected in PI: {examples}")
        if tieout["in_pi_not_in_forecast_examples"]:
            examples = ", ".join(item["name"] for item in tieout["in_pi_not_in_forecast_examples"])
            lines.append(f"  PI not in forecast: {examples}")
        if tieout["in_forecast_not_in_pi_examples"]:
            examples = ", ".join(item["name"] for item in tieout["in_forecast_not_in_pi_examples"])
            lines.append(f"  forecast not in PI: {examples}")
        if tieout["forecast_non_land_examples"]:
            examples = ", ".join(
                f"{item['name']} ({item['type']})" for item in tieout["forecast_non_land_examples"]
            )
            lines.append(f"  forecast non-Land: {examples}")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    session, instance_url = get_org_session(args.target_org)
    period = current_quarter_period(
        session,
        instance_url,
        as_of=args.as_of,
        quarter_start=args.quarter_start,
    )
    fact_opportunity_ids: set[str] = set()
    for row in query_forecasting_fact_rows(session, instance_url, period):
        opp_id = row.get("OpportunityId")
        if not opp_id:
            continue
        fact_opportunity_ids.add(opp_id)
    territories = load_territories(Path(args.territory_config_path), args.territory)
    results = [
        verify_territory(session, instance_url, label, cfg, period, fact_opportunity_ids)
        for label, cfg in territories.items()
    ]
    output = {
        "period": {
            "id": period.id,
            "label": period.label,
            "fiscal_year_name": period.fiscal_year_name,
            "start": period.start,
            "end": period.end,
        },
        "territories": results,
    }
    if args.json:
        print(json.dumps(output, indent=2))
    else:
        print(summarize_results(results, period))


if __name__ == "__main__":
    main()
