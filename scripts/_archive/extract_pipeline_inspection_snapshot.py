#!/usr/bin/env python3
"""Extract Pipeline Inspection data from Salesforce list views and save as JSON snapshots."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "pipeline_inspection_snapshots"

# PI list view IDs per territory (from CLAUDE_HANDOFF.md)
PI_VIEWS = {
    "central-europe": {
        "list_view_id": "00BTb00000Kr3YvMAJ",
        "director": "Sarah Pittroff",
        "territory": "Central Europe",
    },
    "southwestern-europe": {
        "list_view_id": "00BTb00000Kr3sHMAR",
        "director": "Francois Thaury",
        "territory": "Southern Europe",
    },
    "uk-ireland": {
        "list_view_id": "00BTb00000Kr3yjMAB",
        "director": "Dan Peppett",
        "territory": "UK & Ireland",
    },
    "northern-europe": {
        "list_view_id": "00BTb00000Kr4DFMAZ",
        "director": "Christian Ebbesen",
        "territory": "NL & Nordics",
    },
    "canada": {
        "list_view_id": "00BTb00000Kr4ErMAJ",
        "director": "Megan Miceli",
        "territory": "Canada",
    },
    "na-asset-management": {
        "list_view_id": "00BTb00000Kr4JhMAJ",
        "director": "Patrick Gaughan",
        "territory": "NA Asset Management",
    },
    "pension-insurance": {
        "list_view_id": "00BTb00000Kr4OXMAZ",
        "director": "Adam Steinhaus",
        "territory": "Pension & Insurance",
    },
}


def get_auth() -> tuple[str, str]:
    result = subprocess.run(
        ["sf", "org", "display", "--target-org", "apro@simcorp.com", "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(result.stdout)["result"]
    return data["accessToken"], data["instanceUrl"]


def fetch_all_records(
    token: str, instance_url: str, list_view_id: str, max_records: int = 2000
) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    url: str | None = (
        f"{instance_url}/services/data/v66.0/ui-api/list-records/{list_view_id}?pageSize=200"
    )
    all_records: list[dict] = []
    while url and len(all_records) < max_records:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            # Some PI views hit pagination limits; return what we have
            break
        d = resp.json()
        all_records.extend(d.get("records", []))
        next_url = d.get("nextPageUrl")
        url = f"{instance_url}{next_url}" if next_url else None
    return all_records


def parse_record(rec: dict) -> dict[str, Any]:
    f = rec.get("fields", {})
    score_obj = f.get("OpportunityScore", {}).get("value")
    score = None
    if isinstance(score_obj, dict):
        score = score_obj.get("fields", {}).get("Score", {}).get("value")

    owner_obj = f.get("Owner", {}).get("value")
    owner = ""
    if isinstance(owner_obj, dict):
        owner = owner_obj.get("fields", {}).get("Name", {}).get("value", "")

    return {
        "id": rec.get("id", ""),
        "name": str(f.get("Name", {}).get("value", "")),
        "account": str(
            (f.get("Account", {}).get("value") or {})
            .get("fields", {})
            .get("Name", {})
            .get("value", "")
        ),
        "owner": owner,
        "stage": str(f.get("StageName", {}).get("value", "")),
        "forecast_category": str(f.get("ForecastCategoryName", {}).get("value", "")),
        "forecast_arr": float(f.get("APTS_Forecast_ARR__c", {}).get("value") or 0),
        "close_date": str(f.get("CloseDate", {}).get("value", "")),
        "is_closed": bool(f.get("IsClosed", {}).get("value", False)),
        "push_count": int(f.get("PushCount", {}).get("value") or 0),
        "score": score,
        "is_priority": bool(f.get("IsPriorityRecord", {}).get("value", False)),
        "last_activity_days": f.get("LastActivityInDays", {}).get("value"),
        "next_step": str(f.get("NextStep", {}).get("value") or ""),
        "currency": str(f.get("CurrencyIsoCode", {}).get("value", "")),
    }


def build_summary(records: list[dict]) -> dict[str, Any]:
    open_records = [
        r
        for r in records
        if not r["is_closed"] and r["forecast_category"] not in ("Omitted", "Closed")
    ]
    fc = defaultdict(lambda: {"count": 0, "forecast_arr": 0.0})
    stage = defaultdict(lambda: {"count": 0, "forecast_arr": 0.0})
    for r in records:
        fc[r["forecast_category"]]["count"] += 1
        fc[r["forecast_category"]]["forecast_arr"] += r["forecast_arr"]
    for r in open_records:
        stage[r["stage"]]["count"] += 1
        stage[r["stage"]]["forecast_arr"] += r["forecast_arr"]

    top_deals = sorted(open_records, key=lambda x: -x["forecast_arr"])[:10]
    high_push = sorted(
        [r for r in open_records if r["push_count"] >= 2],
        key=lambda x: -x["push_count"],
    )[:5]
    priority = [r for r in open_records if r["is_priority"]]

    return {
        "total_records": len(records),
        "open_pipeline_count": len(open_records),
        "open_pipeline_forecast_arr": round(
            sum(r["forecast_arr"] for r in open_records), 2
        ),
        "forecast_category_breakdown": dict(fc),
        "stage_breakdown": dict(stage),
        "top_deals": [
            {k: v for k, v in d.items() if k != "next_step"} for d in top_deals
        ],
        "high_push_deals": [
            {k: v for k, v in d.items() if k != "next_step"} for d in high_push
        ],
        "priority_deals": [
            {k: v for k, v in d.items() if k != "next_step"} for d in priority
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument(
        "--territory", help="Single territory slug (e.g. uk-ireland). Omit for all."
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    token, instance_url = get_auth()
    views = PI_VIEWS
    if args.territory:
        if args.territory not in PI_VIEWS:
            print(f"Unknown territory: {args.territory}", file=sys.stderr)
            print(f"Available: {', '.join(PI_VIEWS.keys())}", file=sys.stderr)
            sys.exit(1)
        views = {args.territory: PI_VIEWS[args.territory]}

    out_dir = args.output_root / args.snapshot_date
    out_dir.mkdir(parents=True, exist_ok=True)

    for slug, view_info in sorted(views.items()):
        print(f"Pulling {slug} ({view_info['director']})...", end=" ", flush=True)
        raw = fetch_all_records(token, instance_url, view_info["list_view_id"])
        records = [parse_record(r) for r in raw]
        summary = build_summary(records)

        snapshot = {
            "source": "pipeline_inspection",
            "territory_slug": slug,
            "director": view_info["director"],
            "territory": view_info["territory"],
            "list_view_id": view_info["list_view_id"],
            "snapshot_date": args.snapshot_date,
            "extracted_at": datetime.now(UTC).isoformat(),
            "summary": summary,
            "records": records,
        }

        out_path = out_dir / f"{slug}.json"
        out_path.write_text(
            json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        print(
            f"{len(records)} records, {summary['open_pipeline_count']} open, EUR {summary['open_pipeline_forecast_arr']:,.0f}"
        )

    print(f"\nSnapshots saved to: {out_dir}")


if __name__ == "__main__":
    main()
