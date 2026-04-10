#!/usr/bin/env python3
"""Phase 1 extract script — pull raw data for each Sales Director and cache as JSON.

Usage:
    python3 scripts/extract_director_data.py --director "Dan Peppett" --snapshot-date 2026-04-10
    python3 scripts/extract_director_data.py --all --snapshot-date 2026-04-10
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import date
from pathlib import Path

import requests

# Make scripts/ importable regardless of cwd
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

import director_data_helpers as h
from md1_presets import load_md1_preset_config

# ---------------------------------------------------------------------------
# Classic SF dashboard helpers
# D1/D2 are 01Z-prefix dashboards — they live under /analytics/dashboards/,
# NOT /wave/dashboards/.  The helpers module's fetch_dashboard() hits the wrong
# endpoint; we implement the correct flow here.
# ---------------------------------------------------------------------------

# Maps each filter option ID (0ICT…) to its parent filter ID (0IBT…).
# Derived from GET /analytics/dashboards/{D1_ID}/describe
_OPTION_TO_FILTER: dict[str, str] = {
    # Industry
    "0ICTb0000007DbdOAE": "0IBTb0000004LgzOAE",
    "0ICTb0000007DbeOAE": "0IBTb0000004LgzOAE",
    "0ICTb0000007DbfOAE": "0IBTb0000004LgzOAE",
    "0ICTb0000007DbgOAE": "0IBTb0000004LgzOAE",
    "0ICTb0000007DbhOAE": "0IBTb0000004LgzOAE",
    "0ICTb0000007DbiOAE": "0IBTb0000004LgzOAE",
    "0ICTb0000007DbjOAE": "0IBTb0000004LgzOAE",
    # Legal Country
    "0ICTb0000007DgTOAU": "0IBTb0000004Lh0OAE",
    "0ICTb0000007DgUOAU": "0IBTb0000004Lh0OAE",
    # Sales Region
    "0ICTb0000007DbnOAE": "0IBTb0000004Lh1OAE",
    "0ICTb0000007DboOAE": "0IBTb0000004Lh1OAE",
    "0ICTb0000007DbpOAE": "0IBTb0000004Lh1OAE",
    "0ICTb0000007DbqOAE": "0IBTb0000004Lh1OAE",
    "0ICTb0000007DbrOAE": "0IBTb0000004Lh1OAE",
    "0ICTb0000007DbsOAE": "0IBTb0000004Lh1OAE",
    "0ICTb0000007DbtOAE": "0IBTb0000004Lh1OAE",
    # Account Unit Group
    "0ICTb0000007Di5OAE": "0IBTb0000004LnROAU",
    "0ICTb0000007Di6OAE": "0IBTb0000004LnROAU",
    "0ICTb0000007Di7OAE": "0IBTb0000004LnROAU",
}


def _build_dashboard_filter_body(filters: dict) -> list[dict]:
    """Convert DIRECTOR_D1_FILTERS dict (filterN -> option ID or list) to
    dashboardFilters list for the classic analytics PUT endpoint."""
    filter_map: dict[str, list[str]] = {}
    for opt_ids in filters.values():
        if isinstance(opt_ids, str):
            opt_ids = [opt_ids]
        for opt_id in opt_ids:
            parent_id = _OPTION_TO_FILTER.get(opt_id)
            if parent_id:
                filter_map.setdefault(parent_id, []).append(opt_id)
    return [
        {"filterId": fid, "selectedOptions": opts} for fid, opts in filter_map.items()
    ]


def fetch_sf_classic_dashboard(
    token: str,
    base_url: str,
    dashboard_id: str,
    filters: dict | None = None,
) -> dict:
    """Fetch a classic Salesforce dashboard via /analytics/dashboards/.

    If filters are provided, PUT to trigger a filtered refresh, wait 4 s,
    then GET the result.  On 403 rate-limit (1 PUT/min), falls back to GET.
    """
    api_url = (
        f"{base_url}/services/data/{h.API_VERSION}/analytics/dashboards/{dashboard_id}"
    )
    hdr = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    if filters is not None:
        body = {"dashboardFilters": _build_dashboard_filter_body(filters)}
        r = requests.put(api_url, headers=hdr, json=body, timeout=120)
        if r.status_code == 403:
            print("(rate-limited, using last-state GET)", end=" ", flush=True)
        else:
            r.raise_for_status()
            time.sleep(4)  # let server recompute

    resp = requests.get(api_url, headers=hdr, timeout=120)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Output layout
# ---------------------------------------------------------------------------


def cache_root(snapshot_date: str) -> Path:
    return REPO_ROOT / "output" / "director_data_dumps" / snapshot_date / ".cache"


def director_cache_dir(snapshot_date: str, slug: str) -> Path:
    return cache_root(snapshot_date) / slug


def global_cache_dir(snapshot_date: str) -> Path:
    return cache_root(snapshot_date) / "_global"


# ---------------------------------------------------------------------------
# Save helper
# ---------------------------------------------------------------------------


def save(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


# ---------------------------------------------------------------------------
# SOQL queries per director
# ---------------------------------------------------------------------------


def extract_soql_queries(
    token: str,
    base_url: str,
    director: dict,
    out_dir: Path,
    sources: list,
) -> None:
    where = h.soql_where(director)
    name = director["name"]

    queries = [
        (
            "soql_open_pipeline.json",
            f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE IsClosed = false AND {where} ORDER BY APTS_Opportunity_ARR__c DESC NULLS LAST",
            "open_pipeline",
        ),
        (
            "soql_won_this_quarter.json",
            f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE StageName = '8 - Won' AND CloseDate = THIS_QUARTER AND {where}",
            "won_this_quarter",
        ),
        (
            "soql_lost_this_quarter.json",
            f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE StageName = '0 - Lost' AND CloseDate = THIS_QUARTER AND {where}",
            "lost_this_quarter",
        ),
        (
            "soql_won_q1.json",
            f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE StageName = '8 - Won' AND CloseDate >= 2026-01-01 AND CloseDate <= 2026-03-31 AND {where}",
            "won_q1",
        ),
        (
            "soql_lost_q1.json",
            f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE StageName = '0 - Lost' AND CloseDate >= 2026-01-01 AND CloseDate <= 2026-03-31 AND {where}",
            "lost_q1",
        ),
        (
            "soql_pushed_deals.json",
            f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE IsClosed = false AND PushCount > 0 AND {where} ORDER BY PushCount DESC",
            "pushed_deals",
        ),
        (
            "soql_new_pipeline.json",
            f"SELECT {h.OPP_FIELDS} FROM Opportunity WHERE CreatedDate = THIS_QUARTER AND {where}",
            "new_pipeline",
        ),
        (
            "soql_forecast_categories.json",
            f"SELECT ForecastCategoryName, SUM(APTS_Opportunity_ARR__c) arr, SUM(Opportunity_Average_ACV__c) acv, COUNT(Id) ct FROM Opportunity WHERE IsClosed = false AND CloseDate = THIS_QUARTER AND {where} GROUP BY ForecastCategoryName",
            "forecast_categories",
        ),
    ]

    for filename, query, source_id_suffix in queries:
        print(f"  [{name}] {filename} ...", end=" ", flush=True)
        records = h.soql_query(token, base_url, query)
        save(out_dir / filename, records)
        print(f"{len(records)} records")
        sources.append(
            h.make_source_entry(
                source_id=f"{h.slugify(name)}_{source_id_suffix}",
                source_type="soql",
                name=filename.replace(".json", "").replace("soql_", ""),
                query_or_endpoint=query,
                record_count=len(records),
            )
        )


# ---------------------------------------------------------------------------
# D1 dashboard per director
# ---------------------------------------------------------------------------


def extract_d1_dashboard(
    token: str,
    base_url: str,
    director: dict,
    out_dir: Path,
    sources: list,
) -> None:
    name = director["name"]
    filters = h.DIRECTOR_D1_FILTERS.get(name)
    print(f"  [{name}] d1_dashboard.json ...", end=" ", flush=True)
    data = fetch_sf_classic_dashboard(token, base_url, h.D1_DASHBOARD_ID, filters)
    save(out_dir / "d1_dashboard.json", data)
    print("done")
    sources.append(
        h.make_source_entry(
            source_id=f"{h.slugify(name)}_d1_dashboard",
            source_type="dashboard",
            name="D1 Sales Director Monthly Dashboard",
            query_or_endpoint=f"/analytics/dashboards/{h.D1_DASHBOARD_ID}",
            record_count=1,
        )
    )


# ---------------------------------------------------------------------------
# Global: D2 dashboard (extracted once, shared)
# ---------------------------------------------------------------------------


def extract_d2_dashboard(
    token: str,
    base_url: str,
    global_dir: Path,
    sources: list,
) -> None:
    print("  [global] d2_dashboard.json ...", end=" ", flush=True)
    data = fetch_sf_classic_dashboard(token, base_url, h.D2_DASHBOARD_ID)
    save(global_dir / "d2_dashboard.json", data)
    print("done")
    sources.append(
        h.make_source_entry(
            source_id="d2_dashboard_global",
            source_type="dashboard",
            name="D2 Dashboard (global)",
            query_or_endpoint=f"/analytics/dashboards/{h.D2_DASHBOARD_ID}",
            record_count=1,
        )
    )


# ---------------------------------------------------------------------------
# Global: Forecasting (extracted once, shared)
# ---------------------------------------------------------------------------


def extract_forecasting(
    token: str,
    base_url: str,
    global_dir: Path,
    sources: list,
) -> None:
    for period_label, period_id in h.PERIODS.items():
        for type_label, type_id in h.FORECAST_TYPES.items():
            filename = f"forecast_item_{period_label}_{type_label}.json"
            print(f"  [global] {filename} ...", end=" ", flush=True)
            query = (
                f"SELECT Id, OwnerId, Owner.Name, ForecastCategoryName, "
                f"ForecastingItemCategory, ForecastAmount, AmountWithoutAdjustments, "
                f"AmountWithoutManagerAdjustment, HasAdjustment, OwnerOnlyAmount "
                f"FROM ForecastingItem "
                f"WHERE PeriodId = '{period_id}' AND ForecastingTypeId = '{type_id}'"
            )
            records = h.soql_query(token, base_url, query)
            save(global_dir / filename, records)
            print(f"{len(records)} records")
            sources.append(
                h.make_source_entry(
                    source_id=f"forecast_item_{period_label}_{type_label}",
                    source_type="soql",
                    name=f"ForecastingItem {period_label} {type_label}",
                    query_or_endpoint=query,
                    record_count=len(records),
                )
            )

        # ForecastingFact per period
        fact_filename = f"forecast_fact_{period_label}.json"
        print(f"  [global] {fact_filename} ...", end=" ", flush=True)
        fact_query = (
            f"SELECT Id, OpportunityId, ForecastCategoryName, OwnerId, Owner.Name "
            f"FROM ForecastingFact WHERE PeriodId = '{period_id}'"
        )
        fact_records = h.soql_query(token, base_url, fact_query)
        save(global_dir / fact_filename, fact_records)
        print(f"{len(fact_records)} records")
        sources.append(
            h.make_source_entry(
                source_id=f"forecast_fact_{period_label}",
                source_type="soql",
                name=f"ForecastingFact {period_label}",
                query_or_endpoint=fact_query,
                record_count=len(fact_records),
            )
        )


# ---------------------------------------------------------------------------
# Global: OpportunityFieldHistory (extracted once, shared)
# ---------------------------------------------------------------------------


def extract_field_history(
    token: str,
    base_url: str,
    global_dir: Path,
    sources: list,
) -> None:
    fields = ["CloseDate", "ForecastCategoryName", "StageName"]
    for field in fields:
        filename = f"field_history_{field}.json"
        print(f"  [global] {filename} ...", end=" ", flush=True)
        query = (
            f"SELECT OpportunityId, Opportunity.Name, Opportunity.Account.Name, "
            f"Opportunity.Owner.Name, Opportunity.APTS_Opportunity_ARR__c, "
            f"Opportunity.Sales_Director_Book__c, Opportunity.StageName, "
            f"Opportunity.CloseDate, Opportunity.ForecastCategoryName, "
            f"OldValue, NewValue, CreatedDate "
            f"FROM OpportunityFieldHistory "
            f"WHERE Field = '{field}' "
            f"AND CreatedDate >= 2026-01-01T00:00:00Z "
            f"AND CreatedDate < 2026-04-01T00:00:00Z "
            f"ORDER BY CreatedDate DESC"
        )
        records = h.soql_query_all(token, base_url, query)
        save(global_dir / filename, records)
        print(f"{len(records)} records")
        sources.append(
            h.make_source_entry(
                source_id=f"field_history_{field}",
                source_type="soql_all",
                name=f"OpportunityFieldHistory {field}",
                query_or_endpoint=query,
                record_count=len(records),
            )
        )


# ---------------------------------------------------------------------------
# CRMA / Wave SAQL queries (global, extracted once)
# ---------------------------------------------------------------------------


def _get_dataset_ref(token: str, base_url: str, dataset_id: str) -> str:
    """Return 'datasetId/versionId' for use in SAQL load statements."""
    resp = requests.get(
        f"{base_url}/services/data/{h.API_VERSION}/wave/datasets/{dataset_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    ver_id = resp.json().get("currentVersionId", "")
    return f"{dataset_id}/{ver_id}"


def extract_crma_queries(
    token: str,
    base_url: str,
    global_dir: Path,
    sources: list,
) -> None:
    ds = h.CRMA_DATASETS

    # Resolve live dataset references (datasetId/versionId) for SAQL load stmts
    print("  [global] resolving CRMA dataset versions ...", end=" ", flush=True)
    refs: dict[str, str] = {}
    for name, ds_id in ds.items():
        try:
            refs[name] = _get_dataset_ref(token, base_url, ds_id)
        except Exception as exc:
            refs[name] = f"{ds_id}/unknown"
            print(f"\n    WARNING: could not resolve {name}: {exc}", flush=True)
    print("OK")

    rrh = refs["Revenue_Retention_Health"]
    sva = refs["Sales_Velocity_Annual"]
    poo = refs["Pipeline_Opportunity_Operations"]
    omk = refs["Opp_Mgmt_KPIs"]

    saql_queries = [
        (
            "crma_retention_metrics.json",
            "crma_retention_metrics",
            "Revenue Retention Metrics (annual)",
            # RecordType, YearLabel are dims; StartingARR etc. are measures
            f"""q = load "{rrh}";
q = filter q by 'RecordType' == "account_year_metric";
q = group q by 'YearLabel';
q = foreach q generate
    'YearLabel' as 'YearLabel',
    sum('StartingARR') as 'StartingARR',
    sum('RenewalWonARR') as 'RenewalWonARR',
    sum('ExpansionARR') as 'ExpansionARR',
    sum('ChurnARR') as 'ChurnARR',
    sum('EndingARR') as 'EndingARR',
    sum('NewLogoARR') as 'NewLogoARR';
q = order q by 'YearLabel' asc;
q = limit q 100;""",
        ),
        (
            "crma_renewal_risk.json",
            "crma_renewal_risk",
            "Renewal Risk by Level",
            # IsClosed is a numeric measure (0/1), OppType and RiskLevel are dims
            f"""q = load "{rrh}";
q = filter q by 'RecordType' == "opp_detail" && 'IsClosed' == 0 && 'OppType' == "Renewal";
q = group q by 'RiskLevel';
q = foreach q generate
    'RiskLevel' as 'RiskLevel',
    sum('RecurringValue') as 'RecurringValue',
    count() as 'ct';
q = order q by 'RecurringValue' desc;
q = limit q 100;""",
        ),
        (
            "crma_stage_conversion.json",
            "crma_stage_conversion",
            "Stage Conversion 2026",
            # FiscalYear is a numeric measure; FiscalYearLabel is the dim
            # EnteredCount -> StageReachedCount; AdvancedCount -> NextStageReachedCount
            f"""q = load "{sva}";
q = filter q by 'RowType' == "full_pipe_stage" && 'FiscalYearLabel' == "FY2026";
q = group q by 'StageLabel';
q = foreach q generate
    'StageLabel' as 'StageLabel',
    sum('StageReachedCount') as 'EnteredCount',
    sum('NextStageReachedCount') as 'AdvancedCount',
    sum('QualifiedValueEUR') as 'QualifiedValueEUR';
q = order q by 'EnteredCount' desc;
q = limit q 50;""",
        ),
        (
            "crma_pipeline_ops.json",
            "crma_pipeline_ops",
            "Pipeline Opportunity Operations (open)",
            # IsClosed is a dim (string); DaysInCurrentStage -> DaysInStage (measure)
            # Id is the opportunity ID dim; OwnerName, SalesRegion, StageName are dims
            f"""q = load "{poo}";
q = filter q by 'IsClosed' == "false";
q = foreach q generate
    'Id' as 'OpportunityId',
    'PastDueCount' as 'PastDueCount',
    'StaleCount' as 'StaleCount',
    'BackwardMoveCount' as 'BackwardMoveCount',
    'PushCount' as 'PushCount',
    'DaysInStage' as 'DaysInCurrentStage',
    'ARR' as 'ARR',
    'OwnerName' as 'OwnerName',
    'SalesRegion' as 'SalesRegion',
    'StageName' as 'StageName';
q = limit q 2000;""",
        ),
        (
            "crma_velocity_by_type.json",
            "crma_velocity_by_type",
            "Velocity by Opp Type (Won 2026)",
            # IsWon is a dim (string); FiscalYear is a numeric measure (filter == 2026)
            # DaysToClose not available; use AgeInDays. ARR not available; use APTS_Forecast_ARR__c
            f"""q = load "{omk}";
q = filter q by 'IsWon' == "true" && 'CloseDate_Year' == "2026";
q = group q by 'Type';
q = foreach q generate
    'Type' as 'Type',
    avg('AgeInDays') as 'AvgDaysToClose',
    count() as 'ct',
    sum('APTS_Forecast_ARR__c') as 'ARR';
q = order q by 'ARR' desc;
q = limit q 50;""",
        ),
    ]

    for filename, source_id, source_name, saql in saql_queries:
        print(f"  [global] {filename} ...", end=" ", flush=True)
        try:
            records = h.wave_query(token, base_url, saql)
            save(global_dir / filename, records)
            print(f"{len(records)} records")
        except Exception as exc:
            print(f"ERROR: {exc}")
            records = []
            save(global_dir / filename, records)
        sources.append(
            h.make_source_entry(
                source_id=source_id,
                source_type="saql",
                name=source_name,
                query_or_endpoint=saql,
                record_count=len(records),
            )
        )


# ---------------------------------------------------------------------------
# Copy global files into a director's cache dir
# ---------------------------------------------------------------------------

_GLOBAL_STATIC_FILES = [
    "d2_dashboard.json",
    "field_history_CloseDate.json",
    "field_history_ForecastCategoryName.json",
    "field_history_StageName.json",
    "crma_retention_metrics.json",
    "crma_renewal_risk.json",
    "crma_stage_conversion.json",
    "crma_pipeline_ops.json",
    "crma_velocity_by_type.json",
]

_GLOBAL_FORECAST_FILES = [
    f"forecast_item_{p}_{t}.json" for p in h.PERIODS for t in h.FORECAST_TYPES
] + [f"forecast_fact_{p}.json" for p in h.PERIODS]


def copy_global_to_director(global_dir: Path, dir_cache: Path) -> None:
    for fname in _GLOBAL_STATIC_FILES + _GLOBAL_FORECAST_FILES:
        src = global_dir / fname
        if src.exists():
            shutil.copy2(src, dir_cache / fname)


# ---------------------------------------------------------------------------
# Extract one director
# ---------------------------------------------------------------------------


def extract_director(
    token: str,
    base_url: str,
    director: dict,
    snapshot_date: str,
    global_dir: Path,
    global_sources: list,
    director_index: int,
) -> None:
    name = director["name"]
    slug = h.slugify(name)
    out_dir = director_cache_dir(snapshot_date, slug)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== [{director_index}] {name} ({slug}) ===")
    sources: list = []

    # SOQL queries (director-specific)
    extract_soql_queries(token, base_url, director, out_dir, sources)

    # D1 dashboard (director-specific; rate-limited to 1 PUT/min)
    # Sleep 62s between directors so the PUT doesn't 403.
    if director_index > 1:
        print(f"  [{name}] waiting 62s for dashboard rate-limit ...", flush=True)
        time.sleep(62)
    extract_d1_dashboard(token, base_url, director, out_dir, sources)

    # Copy global files into director cache
    copy_global_to_director(global_dir, out_dir)

    # Annotate and merge global sources into director _sources.json
    for gs in global_sources:
        sources.append({**gs, "source_scope": "global"})

    save(out_dir / "_sources.json", sources)
    print(f"  [{name}] _sources.json — {len(sources)} sources registered")


# ---------------------------------------------------------------------------
# Extract global data (once)
# ---------------------------------------------------------------------------


def extract_global(token: str, base_url: str, snapshot_date: str) -> tuple[Path, list]:
    global_dir = global_cache_dir(snapshot_date)
    global_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Global data (extracted once) ===")
    global_sources: list = []

    extract_d2_dashboard(token, base_url, global_dir, global_sources)
    extract_forecasting(token, base_url, global_dir, global_sources)
    extract_field_history(token, base_url, global_dir, global_sources)
    extract_crma_queries(token, base_url, global_dir, global_sources)

    return global_dir, global_sources


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 1: Extract Sales Director data to JSON cache."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--director", metavar="NAME", help="Extract data for one director by name"
    )
    group.add_argument(
        "--all", action="store_true", help="Extract data for all 9 directors"
    )
    parser.add_argument(
        "--snapshot-date",
        default=str(date.today()),
        metavar="YYYY-MM-DD",
        help="Snapshot date (default: today)",
    )
    args = parser.parse_args()

    # Load preset config
    config = load_md1_preset_config(REPO_ROOT / h.CONFIG_PATH)
    presets = config.presets

    # Determine which directors to run
    if args.all:
        directors = [
            {
                "name": p.name,
                "territory": p.territory,
                "filters": [dict(f) for f in p.filters],
            }
            for p in presets
        ]
    else:
        match = next(
            (p for p in presets if p.name.lower() == args.director.lower()), None
        )
        if match is None:
            names = [p.name for p in presets]
            print(
                f"ERROR: Director '{args.director}' not found. Available: {names}",
                file=sys.stderr,
            )
            sys.exit(1)
        directors = [
            {
                "name": match.name,
                "territory": match.territory,
                "filters": [dict(f) for f in match.filters],
            }
        ]

    snapshot_date = args.snapshot_date
    print(f"Snapshot date: {snapshot_date}")
    print(f"Directors: {[d['name'] for d in directors]}")

    # Auth
    print("\nAuthenticating ...", end=" ", flush=True)
    token, instance_url = h.auth()
    base_url = instance_url.rstrip("/")
    print("OK")

    # Extract global data once
    global_dir, global_sources = extract_global(token, base_url, snapshot_date)

    # Extract per-director data
    for idx, director in enumerate(directors, start=1):
        extract_director(
            token,
            base_url,
            director,
            snapshot_date,
            global_dir,
            global_sources,
            director_index=idx,
        )

    print(f"\nDone. Cache at: output/director_data_dumps/{snapshot_date}/.cache/")


if __name__ == "__main__":
    main()
