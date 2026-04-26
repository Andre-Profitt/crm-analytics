#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import requests

try:
    from monthly_platform.period import resolve_period_context
except ModuleNotFoundError:  # pragma: no cover
    from scripts.monthly_platform.period import resolve_period_context

try:
    from extract_historical_trending import REPORTS as LEGACY_HISTORICAL_REPORTS
    from extract_historical_trending import _resolve_report_plan
except ModuleNotFoundError:  # pragma: no cover
    from scripts.extract_historical_trending import REPORTS as LEGACY_HISTORICAL_REPORTS
    from scripts.extract_historical_trending import _resolve_report_plan


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "output" / "source_contract_audit"
TERRITORY_CONFIG_PATH = ROOT / "config" / "sd_monthly_territories.json"

DASHBOARD_REQUIREMENTS = {
    "01ZTb00000FSP7hMAH": {
        "name": "Sales Directors Monthly Pipeline and Insights",
        "required_report_ids": {
            "00OTb000008fBfdMAE",
            "00OTb000008fBEDMA2",
            "00OTb000008ekp7MAA",
            "00OTb000008fBULMA2",
            "00OTb000008Ta9xMAC",
            "00OTb000008ektxMAA",
            "00OTb000008eknVMAQ",
            "00OTb000008aTtJMAU",
        },
    },
    "01ZTb00000FSP9JMAX": {
        "name": "Sales Ops Quarterly KPI Dashboard",
        "required_report_ids": {
            "00OTb000008fAmnMAE",
            "00OTb000008Ti97MAC",
            "00OQA000004OLk92AG",
            "00OTb000008ekynMAA",
            "00OTb000008Ti7VMAS",
            "00OTb000008SrmLMAS",
            "00OTb000008fAlBMAU",
            "00OTb000008TZqcMAG",
            "00OTb000008fAjZMAU",
        },
    },
}

QUARTER_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}


def _load_json(path: Path) -> dict:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _candidate_lane(payload: dict) -> dict:
    lane = payload.get("candidate_forward_quarter")
    if not isinstance(lane, dict) or not lane:
        lane = payload.get("candidate_q3")
    return lane if isinstance(lane, dict) else {}


def _auth() -> tuple[str, str]:
    data = json.loads(
        subprocess.run(
            ["sf", "org", "display", "--target-org", "apro@simcorp.com", "--json"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )["result"]
    return data["accessToken"], data["instanceUrl"]


def _extract_dashboard_component_report_ids(payload: dict) -> list[str]:
    report_ids: list[str] = []
    for component in payload.get("components", []):
        if component.get("type") != "Report":
            continue
        report_id = (
            component.get("reportMetadata", {}).get("id")
            or component.get("reportId")
            or component.get("sourceReportId")
        )
        if isinstance(report_id, str) and report_id:
            report_ids.append(report_id)
    return sorted(set(report_ids))


def _extract_historical_snapshot_dates(detail_columns: list[str]) -> list[str]:
    dates = set()
    for column in detail_columns or []:
        for token in re.findall(r"\.(\d{4}-\d{2}-\d{2})(?:\.|$)", str(column)):
            dates.add(token)
    return sorted(dates)


def _escape_soql_literal(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def _soql_query(session: requests.Session, instance: str, query: str) -> list[dict]:
    records: list[dict] = []
    url = f"{instance}/services/data/v66.0/query"
    params: dict[str, str] | None = {"q": query}
    while True:
        response = session.get(url, params=params, timeout=30)
        payload = response.json()
        if response.status_code != 200:
            raise RuntimeError(f"SOQL query failed ({response.status_code}): {payload}")
        records.extend(payload.get("records", []))
        next_url = payload.get("nextRecordsUrl")
        if not next_url:
            return records
        url = f"{instance}{next_url}"
        params = None


def _replace_quarter_tokens(
    value: str,
    *,
    source_quarter_label: str,
    source_year: int,
    target_quarter_label: str,
    target_year: int,
) -> str:
    updated = str(value or "")
    updated = updated.replace(
        f"{source_quarter_label}_{source_year}",
        f"{target_quarter_label}_{target_year}",
    )
    updated = re.sub(
        rf"(?<![A-Za-z0-9]){re.escape(source_quarter_label)}\s+{source_year}(?![A-Za-z0-9])",
        f"{target_quarter_label} {target_year}",
        updated,
    )
    updated = re.sub(
        rf"(?<![A-Za-z0-9]){re.escape(source_quarter_label)}(?![A-Za-z0-9])",
        target_quarter_label,
        updated,
    )
    return updated

def _find_reference_quarter_entry(
    quarter_map: dict[str, object],
    *,
    target_quarter_label: str,
) -> tuple[str, object] | None:
    target_number = QUARTER_ORDER[str(target_quarter_label).strip().upper()]
    candidates = []
    for label, value in (quarter_map or {}).items():
        label = str(label).strip().upper()
        if label not in QUARTER_ORDER:
            continue
        if QUARTER_ORDER[label] < target_number and value:
            candidates.append((QUARTER_ORDER[label], label, value))
    if not candidates:
        return None
    _, label, value = sorted(candidates)[-1]
    return label, value


def _discover_forward_quarter_sources(
    session: requests.Session,
    instance: str,
    territory_config: dict[str, dict],
    *,
    quarter_label: str,
    quarter_year: int,
    missing_config: list[dict[str, str]],
) -> dict[str, object]:
    missing_by_territory: dict[str, set[str]] = {}
    for item in missing_config:
        territory = str(item.get("territory") or "").strip()
        source = str(item.get("source") or "").strip()
        if territory and source:
            missing_by_territory.setdefault(territory, set()).add(source)

    discovery_records: list[dict[str, str]] = []
    expected_pi_names: dict[str, str] = {}
    expected_report_names: dict[str, str] = {}
    report_reference_ids: dict[str, str] = {}

    for territory, missing_sources in missing_by_territory.items():
        config = territory_config.get(territory) or {}
        if "forward_quarter_pi_list_views" in missing_sources:
            reference_entry = _find_reference_quarter_entry(
                config.get("forward_quarter_pi_list_views") or {},
                target_quarter_label=quarter_label,
            )
            if reference_entry and isinstance(reference_entry[1], dict):
                source_quarter_label, reference_source = reference_entry
                reference_label = str(
                    reference_source.get("list_view_label")
                    or reference_source.get("label")
                    or ""
                ).strip()
                expected_name = _replace_quarter_tokens(
                    reference_label,
                    source_quarter_label=source_quarter_label,
                    source_year=quarter_year,
                    target_quarter_label=quarter_label,
                    target_year=quarter_year,
                )
                expected_pi_names[territory] = expected_name
            else:
                discovery_records.append(
                    {
                        "territory": territory,
                        "source": "forward_quarter_pi_list_views",
                        "quarter_label": quarter_label,
                        "status": "reference_unavailable",
                    }
                )

        if "forward_quarter_historical_trending_report_ids" in missing_sources:
            reference_entry = _find_reference_quarter_entry(
                config.get("forward_quarter_historical_trending_report_ids") or {},
                target_quarter_label=quarter_label,
            )
            if not reference_entry:
                reference_entry = _find_reference_quarter_entry(
                    config.get("historical_trending_report_ids") or {},
                    target_quarter_label=quarter_label,
                )
            if reference_entry and str(reference_entry[1]).strip():
                report_reference_ids[territory] = str(reference_entry[1]).strip()
            else:
                discovery_records.append(
                    {
                        "territory": territory,
                        "source": "forward_quarter_historical_trending_report_ids",
                        "quarter_label": quarter_label,
                        "status": "reference_unavailable",
                    }
                )

    reference_reports_by_id: dict[str, dict] = {}
    if report_reference_ids:
        ids_clause = ", ".join(
            f"'{_escape_soql_literal(report_id)}'"
            for report_id in sorted(set(report_reference_ids.values()))
        )
        rows = _soql_query(
            session,
            instance,
            "SELECT Id, Name, FolderName FROM Report "
            f"WHERE Id IN ({ids_clause})",
        )
        reference_reports_by_id = {str(row.get("Id") or ""): row for row in rows}

    for territory, report_id in report_reference_ids.items():
        reference_row = reference_reports_by_id.get(report_id)
        if not reference_row:
            discovery_records.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_historical_trending_report_ids",
                    "quarter_label": quarter_label,
                    "status": "reference_missing_in_org",
                    "reference_report_id": report_id,
                }
            )
            continue
        reference_name = str(reference_row.get("Name") or "").strip()
        source_match = re.search(r"\b(Q[1-4])\s+(\d{4})\b", reference_name)
        if not source_match:
            discovery_records.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_historical_trending_report_ids",
                    "quarter_label": quarter_label,
                    "status": "reference_name_unusable",
                    "reference_report_id": report_id,
                    "reference_name": reference_name,
                }
            )
            continue
        expected_report_names[territory] = _replace_quarter_tokens(
            reference_name,
            source_quarter_label=source_match.group(1),
            source_year=int(source_match.group(2)),
            target_quarter_label=quarter_label,
            target_year=quarter_year,
        )

    discovered_pi_list_views: dict[str, dict[str, str]] = {}
    if expected_pi_names:
        names_clause = ", ".join(
            f"'{_escape_soql_literal(name)}'"
            for name in sorted(set(expected_pi_names.values()))
        )
        rows = _soql_query(
            session,
            instance,
            "SELECT Id, Name, DeveloperName, SobjectType FROM ListView "
            "WHERE SobjectType = 'Opportunity' "
            f"AND Name IN ({names_clause})",
        )
        rows_by_name = {str(row.get("Name") or ""): row for row in rows}
        for territory, expected_name in expected_pi_names.items():
            row = rows_by_name.get(expected_name)
            if row:
                discovered_pi_list_views[territory] = {
                    "list_view_id": str(row.get("Id") or "").strip(),
                    "list_view_label": str(row.get("Name") or expected_name).strip(),
                }
                discovery_records.append(
                    {
                        "territory": territory,
                        "source": "forward_quarter_pi_list_views",
                        "quarter_label": quarter_label,
                        "status": "discovered",
                        "expected_name": expected_name,
                        "discovered_id": str(row.get("Id") or "").strip(),
                    }
                )
            else:
                discovery_records.append(
                    {
                        "territory": territory,
                        "source": "forward_quarter_pi_list_views",
                        "quarter_label": quarter_label,
                        "status": "absent_in_org",
                        "expected_name": expected_name,
                    }
                )

    discovered_historical_reports: dict[str, str] = {}
    if expected_report_names:
        names_clause = ", ".join(
            f"'{_escape_soql_literal(name)}'"
            for name in sorted(set(expected_report_names.values()))
        )
        rows = _soql_query(
            session,
            instance,
            "SELECT Id, Name, FolderName FROM Report "
            "WHERE FolderName = 'Revenue Operations' "
            f"AND Name IN ({names_clause})",
        )
        rows_by_name = {str(row.get("Name") or ""): row for row in rows}
        for territory, expected_name in expected_report_names.items():
            row = rows_by_name.get(expected_name)
            if row:
                discovered_historical_reports[territory] = str(row.get("Id") or "").strip()
                discovery_records.append(
                    {
                        "territory": territory,
                        "source": "forward_quarter_historical_trending_report_ids",
                        "quarter_label": quarter_label,
                        "status": "discovered",
                        "expected_name": expected_name,
                        "discovered_id": str(row.get("Id") or "").strip(),
                    }
                )
            else:
                discovery_records.append(
                    {
                        "territory": territory,
                        "source": "forward_quarter_historical_trending_report_ids",
                        "quarter_label": quarter_label,
                        "status": "absent_in_org",
                        "expected_name": expected_name,
                    }
                )

    return {
        "pi_list_views": discovered_pi_list_views,
        "historical_reports": discovered_historical_reports,
        "discovery": discovery_records,
    }


def _quarter_window(year: int, quarter_label: str) -> tuple[str, str]:
    quarter = str(quarter_label).strip().upper()
    starts = {
        "Q1": ("01-01", "03-31"),
        "Q2": ("04-01", "06-30"),
        "Q3": ("07-01", "09-30"),
        "Q4": ("10-01", "12-31"),
    }
    if quarter not in starts:
        raise ValueError(f"Unsupported quarter label: {quarter_label}")
    start_mmdd, end_mmdd = starts[quarter]
    return f"{year}-{start_mmdd}", f"{year}-{end_mmdd}"


def _evaluate_historical_alignment(
    *,
    standard_date_filter: dict,
    detail_columns: list[str],
    expected_start: str,
    expected_end: str,
    run_date: str,
) -> dict[str, object]:
    actual_start = str(standard_date_filter.get("startDate") or "")
    actual_end = str(standard_date_filter.get("endDate") or "")
    snapshot_dates = _extract_historical_snapshot_dates(detail_columns)
    before_window = [
        token for token in snapshot_dates if token < expected_start
    ]
    earliest_snapshot = snapshot_dates[0] if snapshot_dates else ""
    latest_snapshot = snapshot_dates[-1] if snapshot_dates else ""
    run_month = str(run_date)[:7]
    issues: list[str] = []
    if actual_start != expected_start or actual_end != expected_end:
        issues.append("standard_date_filter_mismatch")
    if not snapshot_dates:
        issues.append("snapshot_dates_missing")
    else:
        if latest_snapshot[:7] != run_month:
            issues.append("snapshot_review_month_mismatch")
        if latest_snapshot > str(run_date)[:10]:
            issues.append("snapshot_after_run_date")
    return {
        "expected_start": expected_start,
        "expected_end": expected_end,
        "run_date": str(run_date)[:10],
        "run_month": run_month,
        "actual_start": actual_start,
        "actual_end": actual_end,
        "snapshot_dates": snapshot_dates,
        "earliest_snapshot_date": earliest_snapshot,
        "latest_snapshot_date": latest_snapshot,
        "snapshot_dates_before_window": before_window,
        "issues": issues,
        "aligned": not issues,
    }


def _load_forward_quarter_candidate_registry(
    territory_config: dict[str, dict], quarter_label: str
) -> dict[str, object]:
    pi_list_views: dict[str, dict[str, str]] = {}
    historical_reports: dict[str, str] = {}
    missing_config: list[dict[str, str]] = []
    for territory, config in territory_config.items():
        pi_source = (config.get("forward_quarter_pi_list_views") or {}).get(quarter_label)
        list_view_id = ""
        if isinstance(pi_source, dict):
            list_view_id = str(
                pi_source.get("list_view_id") or pi_source.get("id") or ""
            ).strip()
        if list_view_id:
            pi_list_views[territory] = {
                "list_view_id": list_view_id,
                "list_view_label": str(
                    pi_source.get("list_view_label") or pi_source.get("label") or ""
                ).strip(),
            }
        else:
            missing_config.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_pi_list_views",
                    "quarter_label": quarter_label,
                }
            )

        report_id = str(
            (config.get("forward_quarter_historical_trending_report_ids") or {}).get(
                quarter_label
            )
            or ""
        ).strip()
        if report_id:
            historical_reports[territory] = report_id
        else:
            missing_config.append(
                {
                    "territory": territory,
                    "source": "forward_quarter_historical_trending_report_ids",
                    "quarter_label": quarter_label,
                }
            )
    return {
        "pi_list_views": pi_list_views,
        "historical_reports": historical_reports,
        "missing_config": missing_config,
    }


def _probe_dashboard(session: requests.Session, instance: str, dashboard_id: str) -> dict:
    response = session.get(
        f"{instance}/services/data/v66.0/analytics/dashboards/{dashboard_id}/describe",
        timeout=30,
    )
    payload = response.json()
    component_report_ids = _extract_dashboard_component_report_ids(payload)
    requirement = DASHBOARD_REQUIREMENTS[dashboard_id]
    missing_required = sorted(
        requirement["required_report_ids"] - set(component_report_ids)
    )
    return {
        "dashboard_id": dashboard_id,
        "dashboard_name": requirement["name"],
        "status_code": response.status_code,
        "component_count": len(payload.get("components", [])),
        "component_report_ids": component_report_ids,
        "missing_required_report_ids": missing_required,
        "status": "ok" if response.status_code == 200 and not missing_required else "failed",
    }


def _probe_pi_list_view(
    session: requests.Session,
    instance: str,
    territory: str,
    list_view_id: str,
) -> dict:
    response = session.get(
        f"{instance}/services/data/v66.0/ui-api/list-records/{list_view_id}?pageSize=1",
        timeout=30,
    )
    payload = response.json()
    records = payload.get("records", []) if isinstance(payload, dict) else []
    first_fields = (
        sorted(list(records[0].get("fields", {}).keys()))[:8] if records else []
    )
    return {
        "territory": territory,
        "list_view_id": list_view_id,
        "status_code": response.status_code,
        "row_probe_count": len(records),
        "sample_fields": first_fields,
        "status": "ok" if response.status_code == 200 else "failed",
    }


def _probe_historical_report(
    session: requests.Session,
    instance: str,
    *,
    report_id: str,
    expected_start: str,
    expected_end: str,
    quarter_label: str,
    director_slug: str,
    sheet_name: str,
    run_date: str,
) -> dict:
    response = session.get(
        f"{instance}/services/data/v66.0/analytics/reports/{report_id}/describe",
        timeout=30,
    )
    payload = response.json()
    metadata = payload.get("reportMetadata", {})
    alignment = _evaluate_historical_alignment(
        standard_date_filter=metadata.get("standardDateFilter") or {},
        detail_columns=metadata.get("detailColumns") or [],
        expected_start=expected_start,
        expected_end=expected_end,
        run_date=run_date,
    )
    return {
        "director_slug": director_slug,
        "sheet_name": sheet_name,
        "quarter_label": quarter_label,
        "report_id": report_id,
        "status_code": response.status_code,
        **alignment,
        "status": "ok" if response.status_code == 200 and alignment["aligned"] else "failed",
    }


def _write_summary(path: Path, payload: dict) -> None:
    candidate_lane = _candidate_lane(payload)
    lines = [
        f"# Sales Director Source Contract Audit — {payload['run_date']}",
        "",
        f"- Active lane status: `{payload['active_lane']['status']}`",
        f"- Candidate lane status: `{candidate_lane.get('status', 'unknown')}` "
        f"({candidate_lane.get('quarter_title', 'unknown')})",
        "",
        "## Active lane failures",
        "",
    ]
    active_failures = []
    for section_name in ["dashboards", "pi_list_views", "historical_reports"]:
        for item in payload["active_lane"].get(section_name, []):
            if item.get("status") != "ok":
                active_failures.append((section_name, item))
    if not active_failures:
        lines.append("- none")
    else:
        for section_name, item in active_failures:
            lines.append(f"- `{section_name}` {item}")

    lines.extend(["", f"## Candidate {candidate_lane.get('quarter_title', 'unknown')} warnings", ""])
    candidate_issues = []
    for item in candidate_lane.get("missing_config", []):
        candidate_issues.append(("missing_config", item))
    for item in candidate_lane.get("discovery", []):
        if item.get("status") != "discovered":
            candidate_issues.append(("discovery", item))
    for section_name in ["pi_list_views", "historical_reports"]:
        for item in candidate_lane.get(section_name, []):
            if item.get("status") != "ok":
                candidate_issues.append((section_name, item))
            elif item.get("source_origin") == "discovered":
                candidate_issues.append((section_name, item))
    if not candidate_issues:
        lines.append("- none")
    else:
        for section_name, item in candidate_issues:
            lines.append(f"- `{section_name}` {item}")
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Run date stamp, YYYY-MM-DD. Defaults to today.",
    )
    args = parser.parse_args()

    run_date = str(args.date)[:10]
    output_dir = OUTPUT_ROOT / run_date
    output_dir.mkdir(parents=True, exist_ok=True)

    token, instance = _auth()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    period = resolve_period_context(
        as_of_date=run_date,
        snapshot_date=run_date,
        deck_date=run_date,
    )
    active_lane: dict[str, object] = {
        "status": "ok",
        "dashboards": [],
        "pi_list_views": [],
        "historical_reports": [],
    }

    for dashboard_id in DASHBOARD_REQUIREMENTS:
        result = _probe_dashboard(session, instance, dashboard_id)
        active_lane["dashboards"].append(result)
        if result["status"] != "ok":
            active_lane["status"] = "failed"

    territory_config = _load_json(TERRITORY_CONFIG_PATH).get("territories", {})
    for territory, config in territory_config.items():
        result = _probe_pi_list_view(
            session,
            instance,
            territory,
            str(config["pi_list_view_id"]),
        )
        active_lane["pi_list_views"].append(result)
        if result["status"] != "ok":
            active_lane["status"] = "failed"

    try:
        report_plan = _resolve_report_plan(snapshot_date=run_date)
    except ValueError as exc:
        active_lane["historical_reports"].append(
            {
                "status": "failed",
                "issue": "unsupported_runtime_contract",
                "message": str(exc),
            }
        )
        active_lane["status"] = "failed"
        report_plan = {}

    for director_slug, sheet_plan in report_plan.items():
        for sheet_name, report_id in sheet_plan:
            quarter_label = sheet_name.split(" ", 1)[0]
            expected_start, expected_end = _quarter_window(
                period.current_quarter.year,
                quarter_label,
            )
            result = _probe_historical_report(
                session,
                instance,
                report_id=report_id,
                expected_start=expected_start,
                expected_end=expected_end,
                quarter_label=quarter_label,
                director_slug=director_slug,
                sheet_name=sheet_name,
                run_date=run_date,
            )
            active_lane["historical_reports"].append(result)
            if result["status"] != "ok":
                active_lane["status"] = "failed"

    candidate_q3: dict[str, object] = {
        "status": "ok",
        "quarter_label": period.forward_quarter.label,
        "quarter_title": period.forward_quarter.title,
        "pi_list_views": [],
        "historical_reports": [],
        "missing_config": [],
        "discovery": [],
    }
    candidate_registry = _load_forward_quarter_candidate_registry(
        territory_config, period.forward_quarter.label
    )
    candidate_q3["missing_config"] = candidate_registry["missing_config"]
    discovery_registry = _discover_forward_quarter_sources(
        session,
        instance,
        territory_config,
        quarter_label=period.forward_quarter.label,
        quarter_year=period.forward_quarter.year,
        missing_config=list(candidate_registry["missing_config"]),
    )
    candidate_q3["discovery"] = discovery_registry["discovery"]
    if candidate_q3["missing_config"]:
        candidate_q3["status"] = "warning"
    for territory, config in candidate_registry["pi_list_views"].items():
        result = _probe_pi_list_view(
            session,
            instance,
            territory,
            str(config["list_view_id"]),
        )
        result["quarter_label"] = period.forward_quarter.label
        result["quarter_title"] = period.forward_quarter.title
        result["source_origin"] = "configured"
        candidate_q3["pi_list_views"].append(result)
        if result["status"] != "ok":
            candidate_q3["status"] = "warning"
    for territory, config in discovery_registry["pi_list_views"].items():
        if territory in candidate_registry["pi_list_views"]:
            continue
        result = _probe_pi_list_view(
            session,
            instance,
            territory,
            str(config["list_view_id"]),
        )
        result["quarter_label"] = period.forward_quarter.label
        result["quarter_title"] = period.forward_quarter.title
        result["source_origin"] = "discovered"
        candidate_q3["pi_list_views"].append(result)
        candidate_q3["status"] = "warning"
        if result["status"] != "ok":
            candidate_q3["status"] = "warning"
    expected_start, expected_end = _quarter_window(
        period.forward_quarter.year,
        period.forward_quarter.label,
    )
    for territory, report_id in candidate_registry["historical_reports"].items():
        result = _probe_historical_report(
            session,
            instance,
            report_id=str(report_id),
            expected_start=expected_start,
            expected_end=expected_end,
            quarter_label=period.forward_quarter.label,
            director_slug=territory,
            sheet_name=f"{period.forward_quarter.label} Snapshot Trend",
            run_date=run_date,
        )
        if result["status"] != "ok":
            candidate_q3["status"] = "warning"
        result["source_origin"] = "configured"
        candidate_q3["historical_reports"].append(result)
    for territory, report_id in discovery_registry["historical_reports"].items():
        if territory in candidate_registry["historical_reports"]:
            continue
        result = _probe_historical_report(
            session,
            instance,
            report_id=str(report_id),
            expected_start=expected_start,
            expected_end=expected_end,
            quarter_label=period.forward_quarter.label,
            director_slug=territory,
            sheet_name=f"{period.forward_quarter.label} Snapshot Trend",
            run_date=run_date,
        )
        result["source_origin"] = "discovered"
        candidate_q3["status"] = "warning"
        if result["status"] != "ok":
            candidate_q3["status"] = "warning"
        candidate_q3["historical_reports"].append(result)

    payload = {
        "run_date": run_date,
        "active_lane": active_lane,
        "candidate_forward_quarter": candidate_q3,
        "candidate_q3": candidate_q3,
    }
    (output_dir / "source_contract_audit.json").write_text(
        json.dumps(payload, indent=2) + "\n"
    )
    _write_summary(output_dir / "summary.md", payload)

    print(f"Source contract audit: {active_lane['status']}")
    print(f"Output: {output_dir.relative_to(ROOT)}")
    if candidate_q3["status"] != "ok":
        print(
            f"Candidate {candidate_q3['quarter_title']} IDs contain warnings; see summary.md"
        )

    return 0 if active_lane["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
