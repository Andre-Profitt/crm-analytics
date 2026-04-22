#!/usr/bin/env python3
"""Probe a Salesforce forecast URL and attempt a scoped opportunity tie-out.

Given a Lightning forecast URL, this script:
1. extracts the forecasting owner / territory / type ids
2. resolves the live quarter containing --as-of
3. queries ForecastingItem and sums OwnerOnlyAmount by category
4. maps the forecast territory into the repo's rollup model when possible
5. if the scope is known, queries open Land opportunities for an in-quarter
   opportunity-side tie-out by ForecastCategoryName

This script is intentionally probe-first. It does not patch Salesforce.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
TERRITORY_CONFIG_PATH = REPO_ROOT / "config" / "sd_monthly_territories.json"
ROLLUP_CONFIG_PATH = REPO_ROOT / "config" / "territory_mappings.json"
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


@dataclass
class ForecastScope:
    owner_id: str
    territory_id: str
    type_id: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe a Salesforce forecast URL and attempt a tie-out."
    )
    parser.add_argument("--forecast-url", required=True, help="Lightning forecast URL")
    parser.add_argument(
        "--as-of",
        default=str(date.today()),
        help="Date used to resolve the active quarter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--target-org",
        default=TARGET_ORG,
        help=f"Salesforce org alias (default: {TARGET_ORG})",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of human text",
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


def parse_forecast_scope(forecast_url: str) -> ForecastScope:
    parsed = urlparse(forecast_url)
    query = parse_qs(parsed.query)
    try:
        return ForecastScope(
            owner_id=query["c__forecastingOwnerId"][0],
            territory_id=query["c__forecastingTerritoryId"][0],
            type_id=query["c__forecastingTypeId"][0],
        )
    except KeyError as exc:
        raise SystemExit(
            "Forecast URL must include c__forecastingOwnerId, "
            "c__forecastingTerritoryId, and c__forecastingTypeId."
        ) from exc


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
    as_of: str,
) -> dict[str, Any]:
    query = (
        "SELECT Id, Number, QuarterLabel, StartDate, EndDate, Type, "
        "FiscalYearSettings.Name "
        "FROM Period "
        f"WHERE Type = 'Quarter' AND StartDate <= {as_of} AND EndDate >= {as_of} "
        "ORDER BY StartDate DESC LIMIT 1"
    )
    rows = soql_query(session, instance_url, query)
    if not rows:
        raise SystemExit(f"No active quarter Period found for as-of date {as_of}.")
    return rows[0]


def resolve_entities(
    session: requests.Session,
    instance_url: str,
    scope: ForecastScope,
) -> dict[str, Any]:
    owner = soql_query(
        session, instance_url, f"SELECT Id, Name FROM User WHERE Id = '{scope.owner_id}'"
    )
    territory = soql_query(
        session,
        instance_url,
        "SELECT Id, Name, ParentTerritory2Id FROM Territory2 "
        f"WHERE Id = '{scope.territory_id}'",
    )
    forecasting_type = soql_query(
        session,
        instance_url,
        "SELECT Id, DeveloperName, MasterLabel FROM ForecastingType "
        f"WHERE Id = '{scope.type_id}'",
    )
    return {
        "owner": owner[0] if owner else {"Id": scope.owner_id, "Name": None},
        "territory": territory[0]
        if territory
        else {"Id": scope.territory_id, "Name": None, "ParentTerritory2Id": None},
        "type": forecasting_type[0]
        if forecasting_type
        else {"Id": scope.type_id, "DeveloperName": None, "MasterLabel": None},
    }


def query_forecasting_items(
    session: requests.Session,
    instance_url: str,
    scope: ForecastScope,
    period_id: str,
) -> list[dict[str, Any]]:
    query = (
        "SELECT Id, ForecastCategoryName, ForecastingItemCategory, "
        "ForecastAmount, convertCurrency(ForecastAmount) ConvertedForecastAmount, "
        "AmountWithoutAdjustments, "
        "convertCurrency(AmountWithoutAdjustments) ConvertedAdjAmount, "
        "AmountWithoutManagerAdjustment, "
        "convertCurrency(AmountWithoutManagerAdjustment) ConvertedNoMgrAdjAmount, "
        "OwnerOnlyAmount, convertCurrency(OwnerOnlyAmount) ConvertedOwnerOnlyAmount, "
        "CurrencyIsoCode, PeriodId, ForecastingTypeId, OwnerId, Territory2Id "
        "FROM ForecastingItem "
        f"WHERE OwnerId = '{scope.owner_id}' "
        f"AND Territory2Id = '{scope.territory_id}' "
        f"AND ForecastingTypeId = '{scope.type_id}' "
        f"AND PeriodId = '{period_id}' "
        "AND CurrencyIsoCode = 'EUR'"
    )
    return soql_query(session, instance_url, query)


def sum_amounts_by_category(
    rows: list[dict[str, Any]],
    *,
    converted_field: str,
    raw_field: str,
) -> dict[str, float]:
    out = {"Pipeline": 0.0, "Best Case": 0.0, "Commit": 0.0, "Closed": 0.0}
    for row in rows:
        category = (row.get("ForecastCategoryName") or "").strip()
        amount_value = row.get(converted_field)
        if amount_value is None:
            amount_value = row.get(raw_field)
        amount = float(amount_value or 0)
        if category in out:
            out[category] += amount
            continue
        lowered = category.lower()
        if "pipeline" in lowered:
            out["Pipeline"] += amount
        elif "bestcase" in lowered or "best case" in lowered:
            out["Best Case"] += amount
        elif "commit" in lowered:
            out["Commit"] += amount
        elif "closed" in lowered:
            out["Closed"] += amount
    out["Open Total"] = out["Pipeline"] + out["Best Case"] + out["Commit"]
    return out


def load_configs() -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        json.loads(TERRITORY_CONFIG_PATH.read_text()),
        json.loads(ROLLUP_CONFIG_PATH.read_text()),
    )


def scope_from_forecast_territory(
    forecast_territory_id: str,
    rollup_config: dict[str, Any],
) -> dict[str, Any] | None:
    forecast_rollup = rollup_config.get("forecast_rollup", {})
    director_books = rollup_config.get("director_book_rollup", {})
    cro = rollup_config.get("cro", {})

    # Known child-territory to director-book mapping.
    child_to_books = {
        "APAC LAND": ["APAC"],
        "APAC Expansion": ["APAC"],
        "EMEA Sales CE": ["Central Europe"],
        "EMEA Sales NE": ["NL & Nordics"],
        "EMEA Sales SWE": ["Southern Europe"],
        "EMEA Sales UK & IE": ["UK & Ireland"],
        "ME & AFR Sales": ["Middle East & Africa"],
        "NA Sales Canada": ["Canada"],
        "NA Sales AM": ["NA Asset Management"],
        "NA Sales Insurance": ["Pension & Insurance"],
        "NA Sales Pension": ["Pension & Insurance"],
    }

    if cro.get("forecasting_territory_id") == forecast_territory_id:
        return {
            "mode": "cro_rollup",
            "label": "CRO",
            "director_book_territories": sorted(
                book_cfg["territory"] for book_cfg in director_books.values()
            ),
        }

    # Top-level forecast territory ids.
    for top_level, cfg in forecast_rollup.items():
        if cfg.get("forecasting_territory_id") == forecast_territory_id:
            sales_regions = set(cfg.get("sales_regions", []))
            matched = sorted(
                book_cfg["territory"]
                for book_cfg in director_books.values()
                if book_cfg.get("sales_region") in sales_regions
            )
            return {
                "mode": "forecast_rollup",
                "label": top_level,
                "director_book_territories": matched,
            }

    # Child territories.
    for top_level, cfg in forecast_rollup.items():
        for child_name, child_id in cfg.get("child_territories", {}).items():
            if child_id == forecast_territory_id:
                return {
                    "mode": "forecast_child",
                    "label": child_name,
                    "rollup": top_level,
                    "director_book_territories": child_to_books.get(child_name, []),
                }
    return None


def query_opportunity_rollup(
    session: requests.Session,
    instance_url: str,
    territory_label: str,
    territory_cfg: dict[str, Any],
    period_start: str,
    period_end: str,
) -> dict[str, dict[str, float]]:
    scope_where = territory_cfg["territories"][territory_label]["soql_where"]
    query = (
        "SELECT ForecastCategoryName, COUNT(Id) dealCount, "
        "SUM(convertCurrency(APTS_Opportunity_ARR__c)) arrUnweighted, "
        "SUM(convertCurrency(APTS_Forecast_ARR__c)) arrWeighted "
        "FROM Opportunity "
        "WHERE IsClosed = false "
        "AND Type = 'Land' "
        "AND ForecastCategoryName != 'Omitted' "
        f"AND CloseDate >= {period_start} AND CloseDate <= {period_end} "
        f"AND {scope_where} "
        f"{ACCOUNT_EXCLUDE}{OWNER_EXCLUDE}"
        "GROUP BY ForecastCategoryName"
    )
    rows = soql_query(session, instance_url, query)
    out: dict[str, dict[str, float]] = {}
    for row in rows:
        cat = row.get("ForecastCategoryName") or "Unspecified"
        out[cat] = {
            "deal_count": float(row.get("dealCount") or 0),
            "arr_unweighted": float(row.get("arrUnweighted") or 0),
            "arr_weighted": float(row.get("arrWeighted") or 0),
        }
    return out


def aggregate_opportunity_rollup(
    session: requests.Session,
    instance_url: str,
    territory_cfg: dict[str, Any],
    territory_labels: list[str],
    period_start: str,
    period_end: str,
) -> dict[str, Any]:
    by_category: dict[str, dict[str, float]] = defaultdict(
        lambda: {"deal_count": 0.0, "arr_unweighted": 0.0, "arr_weighted": 0.0}
    )
    for territory_label in territory_labels:
        territory_rows = query_opportunity_rollup(
            session,
            instance_url,
            territory_label,
            territory_cfg,
            period_start,
            period_end,
        )
        for category, metrics in territory_rows.items():
            for key, value in metrics.items():
                by_category[category][key] += value

    totals = {"deal_count": 0.0, "arr_unweighted": 0.0, "arr_weighted": 0.0}
    for metrics in by_category.values():
        for key in totals:
            totals[key] += metrics[key]
    return {"by_category": dict(by_category), "totals": totals}


def render_text(result: dict[str, Any]) -> str:
    lines = []
    page = result["forecast_page"]
    lines.append(
        f"Forecast page: {page['owner_name']} / {page['territory_name']} / "
        f"{page['forecast_type_label']} / {page['period_start']}..{page['period_end']}"
    )
    lines.append("Forecast page totals from ForecastingItem ForecastAmount (EUR):")
    for key in ("Pipeline", "Best Case", "Commit", "Closed", "Open Total"):
        lines.append(f"  {key}: {result['forecast_page_totals'].get(key, 0):,.2f}")
    lines.append("Direct contribution totals from OwnerOnlyAmount (EUR):")
    for key in ("Pipeline", "Best Case", "Commit", "Closed", "Open Total"):
        lines.append(f"  {key}: {result['forecast_owner_only_totals'].get(key, 0):,.2f}")

    tieout_scope = result.get("tieout_scope")
    if not tieout_scope:
        lines.append(
            "Tie-out scope: none. This forecast territory does not map to a known "
            "director-book or forecast-rollup slice in the repo."
        )
        return "\n".join(lines)

    lines.append(
        f"Tie-out scope: {tieout_scope['mode']} / {tieout_scope['label']} -> "
        f"{', '.join(tieout_scope['director_book_territories']) or 'no mapped books'}"
    )
    opportunity_rollup = result.get("opportunity_rollup")
    if not opportunity_rollup:
        lines.append("Opportunity-side tie-out: unavailable for this scope.")
        return "\n".join(lines)

    totals = opportunity_rollup["totals"]
    lines.append("Open Land opportunity totals for the same quarter window:")
    lines.append(f"  Deal count: {int(totals['deal_count'])}")
    lines.append(f"  ARR unweighted: {totals['arr_unweighted']:,.2f}")
    lines.append(f"  ARR weighted: {totals['arr_weighted']:,.2f}")
    lines.append("By ForecastCategoryName:")
    for category, metrics in sorted(opportunity_rollup["by_category"].items()):
        lines.append(
            f"  {category}: deals={int(metrics['deal_count'])}, "
            f"wtd={metrics['arr_weighted']:,.2f}, "
            f"unwtd={metrics['arr_unweighted']:,.2f}"
        )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    scope = parse_forecast_scope(args.forecast_url)
    session, instance_url = get_org_session(args.target_org)
    period = current_quarter_period(session, instance_url, args.as_of)
    entities = resolve_entities(session, instance_url, scope)
    forecasting_rows = query_forecasting_items(
        session, instance_url, scope, period["Id"]
    )
    forecast_page_totals = sum_amounts_by_category(
        forecasting_rows,
        converted_field="ConvertedForecastAmount",
        raw_field="ForecastAmount",
    )
    forecast_owner_only_totals = sum_amounts_by_category(
        forecasting_rows,
        converted_field="ConvertedOwnerOnlyAmount",
        raw_field="OwnerOnlyAmount",
    )

    territory_cfg, rollup_cfg = load_configs()
    tieout_scope = scope_from_forecast_territory(scope.territory_id, rollup_cfg)
    opportunity_rollup = None
    if tieout_scope and tieout_scope.get("director_book_territories"):
        opportunity_rollup = aggregate_opportunity_rollup(
            session,
            instance_url,
            territory_cfg,
            tieout_scope["director_book_territories"],
            period["StartDate"],
            period["EndDate"],
        )

    result = {
        "forecast_page": {
            "owner_id": scope.owner_id,
            "owner_name": entities["owner"].get("Name"),
            "territory_id": scope.territory_id,
            "territory_name": entities["territory"].get("Name"),
            "territory_parent_id": entities["territory"].get("ParentTerritory2Id"),
            "forecast_type_id": scope.type_id,
            "forecast_type_label": entities["type"].get("MasterLabel"),
            "forecast_type_api_name": entities["type"].get("DeveloperName"),
            "period_id": period["Id"],
            "period_start": period["StartDate"],
            "period_end": period["EndDate"],
            "as_of": args.as_of,
        },
        "forecast_page_totals": forecast_page_totals,
        "forecast_owner_only_totals": forecast_owner_only_totals,
        "forecasting_item_row_count": len(forecasting_rows),
        "tieout_scope": tieout_scope,
        "opportunity_rollup": opportunity_rollup,
    }

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(render_text(result))


if __name__ == "__main__":
    main()
