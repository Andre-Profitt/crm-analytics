#!/usr/bin/env python3
"""
validate_d1_md1_presets.py

Read-only validation of the live Dashboard 1 source reports against the 9 MD-1
territory presets.

The script executes the current live D1 source reports through the Salesforce
Reports API using temporary reportMetadata filter overrides. It does not save
or mutate any report or dashboard metadata.

Outputs:
- JSON summary to stdout by default
- optional markdown and/or JSON file artifacts

Usage:
  python3 scripts/validate_d1_md1_presets.py
  python3 scripts/validate_d1_md1_presets.py --format markdown
  python3 scripts/validate_d1_md1_presets.py --out-md /tmp/d1_md1_validation.md --out-json /tmp/d1_md1_validation.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from md1_presets import (
    find_md1_preset,
    load_md1_preset_config,
    md1_preset_config_summary,
)

API_VERSION = "v66.0"
TARGET_ORG_DEFAULT = "apro@simcorp.com"
DASHBOARD_ID = "01ZTb00000FSP7hMAH"
POLL_ATTEMPTS = 20
POLL_SLEEP_SECONDS = 1.0
REPO_ROOT = Path(__file__).resolve().parents[1]
PRESET_CONFIG_PATH = REPO_ROOT / "config" / "sales_director_md1_presets.json"

BANG = chr(33)
FACTMAP_TOTAL_KEY = "T" + BANG + "T"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate live D1 MD-1 presets.")
    parser.add_argument(
        "--target-org",
        default=TARGET_ORG_DEFAULT,
        help=f"Salesforce target org username or alias (default: {TARGET_ORG_DEFAULT})",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Primary stdout format.",
    )
    parser.add_argument(
        "--preset-config",
        default=str(PRESET_CONFIG_PATH),
        help=f"Preset config path (default: {PRESET_CONFIG_PATH})",
    )
    parser.add_argument(
        "--preset-name",
        default=None,
        help="Optional single preset name to validate instead of the full matrix.",
    )
    parser.add_argument("--out-json", help="Optional path to write JSON results.")
    parser.add_argument("--out-md", help="Optional path to write markdown results.")
    return parser.parse_args()


def sh(*args: str) -> str:
    result = subprocess.run(
        list(args),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def auth(target_org: str) -> tuple[str, str]:
    payload = json.loads(sh("sf", "org", "display", "--target-org", target_org, "--json"))
    result = payload["result"]
    return result["instanceUrl"], result["accessToken"]


def http_json(method: str, url: str, token: str, body: dict[str, Any] | None = None) -> dict[str, Any] | list[Any]:
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = {"error": payload}
        raise RuntimeError(f"HTTP {exc.code}: {parsed}") from exc
    except URLError as exc:
        raise RuntimeError(f"network error: {exc}") from exc


def fetch_dashboard_metadata(instance_url: str, token: str) -> dict[str, Any]:
    payload = http_json(
        "GET",
        f"{instance_url}/services/data/{API_VERSION}/analytics/dashboards/{DASHBOARD_ID}",
        token,
    )
    assert isinstance(payload, dict)
    return payload["dashboardMetadata"]


def fetch_report_describe(instance_url: str, token: str, report_id: str) -> dict[str, Any]:
    payload = http_json(
        "GET",
        f"{instance_url}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe",
        token,
    )
    assert isinstance(payload, dict)
    return payload["reportMetadata"]


def build_execution_metadata(report_metadata: dict[str, Any], extra_filters: tuple[dict[str, str], ...]) -> dict[str, Any]:
    return {
        "reportFormat": report_metadata.get("reportFormat"),
        "reportFilters": list(report_metadata.get("reportFilters") or []) + [dict(item) for item in extra_filters],
        "groupingsDown": list(report_metadata.get("groupingsDown") or []),
        "groupingsAcross": list(report_metadata.get("groupingsAcross") or []),
        "detailColumns": list(report_metadata.get("detailColumns") or []),
        "aggregates": list(report_metadata.get("aggregates") or []),
        "standardDateFilter": report_metadata.get("standardDateFilter"),
    }


def run_report_instance(
    instance_url: str,
    token: str,
    report_id: str,
    report_metadata: dict[str, Any],
) -> dict[str, Any]:
    initial = http_json(
        "POST",
        f"{instance_url}/services/data/{API_VERSION}/analytics/reports/{report_id}/instances?includeDetails=true",
        token,
        body={"reportMetadata": report_metadata},
    )
    if isinstance(initial, list):
        raise RuntimeError(f"unexpected list response: {initial}")
    relative_url = initial.get("url")
    if not relative_url:
        raise RuntimeError(f"missing instance url: {initial}")
    for _ in range(POLL_ATTEMPTS):
        payload = http_json("GET", f"{instance_url}{relative_url}?includeDetails=true", token)
        if isinstance(payload, dict) and payload.get("factMap"):
            return payload
        if isinstance(payload, dict) and payload.get("status") == "Error":
            raise RuntimeError(f"instance error: {payload}")
        time.sleep(POLL_SLEEP_SECONDS)
    raise RuntimeError("timed out polling report instance")


def count_detail_rows(fact_map: dict[str, Any]) -> int:
    total = 0
    for bucket in fact_map.values():
        if not isinstance(bucket, dict):
            continue
        rows = bucket.get("rows") or []
        total += len(rows)
    return total


def summarize_result(payload: dict[str, Any]) -> dict[str, Any]:
    fact_map = payload.get("factMap") or {}
    total = fact_map.get(FACTMAP_TOTAL_KEY) or {}
    groupings = (payload.get("groupingsDown") or {}).get("groupings") or []
    return {
        "detail_row_count": count_detail_rows(fact_map),
        "group_labels": [item.get("label") for item in groupings[:5]],
        "aggregate_labels": [item.get("label") for item in (total.get("aggregates") or [])],
    }


def build_results(
    instance_url: str,
    token: str,
    *,
    preset_config_path: str,
    preset_name: str | None,
) -> dict[str, Any]:
    preset_config = load_md1_preset_config(preset_config_path)
    presets = preset_config.presets
    if preset_name:
        selected = find_md1_preset(preset_config, preset_name)
        if selected is None:
            available = ", ".join(preset.name for preset in preset_config.presets)
            raise RuntimeError(
                f"preset not found: {preset_name}. Available presets: {available}"
            )
        presets = (selected,)
    dashboard_metadata = fetch_dashboard_metadata(instance_url, token)
    components = dashboard_metadata.get("components") or []
    current_reports: list[dict[str, Any]] = []
    for component in components:
        if not isinstance(component, dict):
            continue
        header = component.get("header") or component.get("title") or "(untitled)"
        if isinstance(header, dict):
            header = header.get("label") or "(untitled)"
        current_reports.append(
            {
                "header": str(header),
                "report_id": str(component.get("reportId") or ""),
                "visualization_type": str((component.get("properties") or {}).get("visualizationType") or ""),
            }
        )

    presets_results: list[dict[str, Any]] = []
    for preset in presets:
        report_results: list[dict[str, Any]] = []
        for report in current_reports:
            report_id = report["report_id"]
            report_describe = fetch_report_describe(instance_url, token, report_id)
            execution_metadata = build_execution_metadata(report_describe, preset.filters)
            try:
                payload = run_report_instance(instance_url, token, report_id, execution_metadata)
                summary = summarize_result(payload)
                report_results.append(
                    {
                        "header": report["header"],
                        "report_id": report_id,
                        "visualization_type": report["visualization_type"],
                        "status": "ok",
                        **summary,
                    }
                )
            except Exception as exc:
                report_results.append(
                    {
                        "header": report["header"],
                        "report_id": report_id,
                        "visualization_type": report["visualization_type"],
                        "status": "error",
                        "error": str(exc),
                    }
                )
        presets_results.append(
            {
                "preset_name": preset.name,
                "territory": preset.territory,
                "filters": list(preset.filters),
                "reports": report_results,
            }
        )

    return {
        "dashboard_id": DASHBOARD_ID,
        "preset_config": md1_preset_config_summary(preset_config),
        "current_headers": [item["header"] for item in current_reports],
        "presets": presets_results,
    }


def render_markdown(results: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# D1 MD-1 Preset Validation")
    lines.append("")
    lines.append(f"- Dashboard: `{results['dashboard_id']}`")
    lines.append(f"- Preset config: `{results['preset_config']['config_path']}`")
    lines.append(f"- Widgets: `{len(results['current_headers'])}`")
    lines.append("")
    lines.append("## Current D1")
    lines.append("")
    for header in results["current_headers"]:
        lines.append(f"- `{header}`")
    for preset in results["presets"]:
        lines.append("")
        lines.append(f"## {preset['preset_name']} - {preset['territory']}")
        lines.append("")
        lines.append(f"- Filter count: `{len(preset['filters'])}`")
        if preset.get("filters"):
            lines.append(f"- Selected filters: `{md1_preset_summary_from_dict(preset)}`")
        lines.append("| Report | Status | Detail rows | Aggregate labels |")
        lines.append("| --- | --- | ---: | --- |")
        for report in preset["reports"]:
            if report["status"] != "ok":
                lines.append(
                    f"| `{report['header']}` | error | - | `{report['error']}` |"
                )
                continue
            aggregates = ", ".join(report.get("aggregate_labels") or []) or "-"
            lines.append(
                f"| `{report['header']}` | ok | {report['detail_row_count']} | {aggregates} |"
            )
    return "\n".join(lines) + "\n"


def md1_preset_summary_from_dict(preset: dict[str, Any]) -> str:
    return "; ".join(
        f"{item.get('column')} {item.get('operator')} {item.get('value')}"
        for item in (preset.get("filters") or [])
        if isinstance(item, dict)
    )


def main() -> int:
    args = parse_args()
    instance_url, token = auth(args.target_org)
    results = build_results(
        instance_url,
        token,
        preset_config_path=args.preset_config,
        preset_name=args.preset_name,
    )
    markdown = render_markdown(results)
    results_json = json.dumps(results, indent=2)

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as handle:
            handle.write(results_json)
    if args.out_md:
        with open(args.out_md, "w", encoding="utf-8") as handle:
            handle.write(markdown)

    if args.format == "markdown":
        sys.stdout.write(markdown)
    else:
        sys.stdout.write(results_json + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
