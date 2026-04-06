#!/usr/bin/env python3
"""Re-run the SAQL queries embedded in each Sales Ops live_saql_validation.json
file and write the updated results back atomically.

Reads each input JSON, extracts every result's SAQL query, executes it against
the live org via Wave API, and rewrites the JSON with new sample_records,
row_count, status, and validated_at fields. Preserves the original query and
metric fields verbatim.

Auth via `sf org display`. No MCP, no Python builders touched.

Usage:
    python3 scripts/run_report2_saql_refresh.py \\
        --snapshot-date 2026-04-01 \\
        --target-org apro@simcorp.com \\
        --json
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TARGET_ORG = "apro@simcorp.com"
WAVE_API_VERSION = "v66.0"

DEFAULT_INPUT_PATHS = [
    REPO_ROOT
    / "output"
    / "sales_ops_page1_mutation_prep_2026-03-31"
    / "live_saql_validation"
    / "live_saql_validation.json",
    REPO_ROOT
    / "output"
    / "sales_ops_page4_mutation_prep_2026-03-31"
    / "live_saql_validation"
    / "live_saql_validation.json",
    REPO_ROOT
    / "output"
    / "sales_ops_page5_mutation_prep_2026-03-31"
    / "live_saql_validation"
    / "live_saql_validation.json",
    REPO_ROOT
    / "output"
    / "sales_ops_page6_mutation_prep_2026-03-31"
    / "live_saql_validation"
    / "live_saql_validation.json",
]


def load_validation_json(path: Path) -> dict[str, Any]:
    """Load a live_saql_validation.json file from disk."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def merge_query_results(
    original: dict[str, Any],
    new_results_by_alias: dict[str, list[dict[str, Any]]],
    *,
    validated_at: str,
) -> dict[str, Any]:
    """Merge new SAQL results into the original validation payload.

    For each result entry, looks up the new sample_records by step_alias.
    If found, updates sample_records, row_count, and sets status='ok'.
    If not found, leaves sample_records as-is and sets status='error'.
    Preserves query and metric fields verbatim. Sets payload-level validated_at.
    Does NOT mutate the input dict.
    """
    merged = copy.deepcopy(original)
    merged["validated_at"] = validated_at
    for entry in merged["results"]:
        alias = entry["step_alias"]
        if alias in new_results_by_alias:
            new_records = new_results_by_alias[alias]
            entry["sample_records"] = new_records
            entry["row_count"] = len(new_records)
            entry["status"] = "ok"
        else:
            entry["status"] = "error"
    return merged


def get_auth(target_org: str) -> tuple[str, str]:
    """Return (access_token, instance_url) from `sf org display`."""
    proc = subprocess.run(
        ["sf", "org", "display", "--target-org", target_org, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout)
    result = payload.get("result", {})
    token = result.get("accessToken")
    instance = result.get("instanceUrl")
    if not token or not instance:
        raise RuntimeError(
            f"sf org display did not return accessToken and instanceUrl for {target_org}"
        )
    return token, instance


def execute_saql(
    query: str, *, access_token: str, instance_url: str
) -> list[dict[str, Any]]:
    """Execute one SAQL query via Wave API and return its records."""
    url = f"{instance_url}/services/data/{WAVE_API_VERSION}/wave/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    response = requests.post(url, headers=headers, json={"query": query}, timeout=120)
    response.raise_for_status()
    payload = response.json()
    return payload.get("results", {}).get("records", [])


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON to path atomically via tempfile + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=False)
            f.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def refresh_one_file(
    path: Path,
    *,
    access_token: str,
    instance_url: str,
    validated_at: str,
) -> dict[str, Any]:
    """Refresh a single live_saql_validation.json. Returns a per-file summary."""
    original = load_validation_json(path)
    new_by_alias: dict[str, list[dict[str, Any]]] = {}
    failures: list[dict[str, str]] = []
    for entry in original["results"]:
        alias = entry["step_alias"]
        query = entry["query"]
        try:
            records = execute_saql(
                query, access_token=access_token, instance_url=instance_url
            )
            new_by_alias[alias] = records
        except Exception as e:
            failures.append({"step_alias": alias, "error": str(e)})
    merged = merge_query_results(original, new_by_alias, validated_at=validated_at)
    atomic_write_json(path, merged)
    return {
        "path": str(path),
        "step_count": len(original["results"]),
        "refreshed_count": len(new_by_alias),
        "failure_count": len(failures),
        "failures": failures,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Snapshot date in YYYY-MM-DD format. Used as validated_at in the output JSONs.",
    )
    parser.add_argument(
        "--target-org",
        default=DEFAULT_TARGET_ORG,
        help=f"sf CLI target org alias. Defaults to {DEFAULT_TARGET_ORG}.",
    )
    parser.add_argument(
        "--input",
        action="append",
        type=Path,
        default=None,
        help="Input live_saql_validation.json path. Repeat for multiple. Defaults to all 4 Sales Ops pages.",
    )
    parser.add_argument(
        "--json", action="store_true", help="Print a JSON summary payload."
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inputs = args.input if args.input else DEFAULT_INPUT_PATHS
    access_token, instance_url = get_auth(args.target_org)
    summaries: list[dict[str, Any]] = []
    overall_failures = 0
    for path in inputs:
        summary = refresh_one_file(
            path,
            access_token=access_token,
            instance_url=instance_url,
            validated_at=args.snapshot_date,
        )
        summaries.append(summary)
        overall_failures += summary["failure_count"]
    result = {
        "artifact_type": "report2_saql_refresh_summary",
        "snapshot_date": args.snapshot_date,
        "target_org": args.target_org,
        "input_count": len(inputs),
        "total_failures": overall_failures,
        "files": summaries,
    }
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        for s in summaries:
            print(
                f"{s['path']}: {s['refreshed_count']}/{s['step_count']} steps refreshed, {s['failure_count']} failures"
            )
        print(f"TOTAL FAILURES: {overall_failures}")
    return 0 if overall_failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
