#!/usr/bin/env python3
"""Build the monthly Salesforce source contract manifest.

This is the offline-safe control manifest for the monthly Sales Director deck
chain. It resolves configured Salesforce sources, folds in the latest source
audit when present, and records current-vs-forward quarter display decisions
from DirectorBundle data when available.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from scripts.monthly_platform.period import resolve_period_context  # noqa: E402
from scripts.monthly_platform.policy import is_active_forecast_category  # noqa: E402

try:
    from scripts.build_director_gold_analytics import region_for_territory  # noqa: E402
except ModuleNotFoundError:  # pragma: no cover
    from build_director_gold_analytics import region_for_territory  # type: ignore  # noqa: E402

DEFAULT_TERRITORY_CONFIG = ROOT / "config" / "sd_monthly_territories.json"
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "monthly_source_contract"
DEFAULT_SOURCE_AUDIT_ROOT = ROOT / "output" / "source_contract_audit"
DEFAULT_BUNDLE_ROOT = ROOT / "output" / "director_bundles"


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")
    return slug or "unknown"


def bundle_slugify(value: str) -> str:
    slug = "".join(ch if ch.isalnum() else "-" for ch in str(value).lower()).strip("-")
    return slug or "unknown"


def bundle_path_for_territory(
    *,
    bundle_dir: Path,
    territory: str,
    director_slug: str,
) -> Path:
    candidates = [
        bundle_dir / f"{bundle_slugify(territory)}.json",
        bundle_dir / f"{slugify(territory)}.json",
        bundle_dir / f"{director_slug}.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def display_path(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_territory_config(path: Path = DEFAULT_TERRITORY_CONFIG) -> dict[str, Any]:
    payload = load_json(path)
    territories = payload.get("territories")
    if not isinstance(territories, dict) or not territories:
        raise ValueError(f"{path}: expected non-empty territories object")
    return territories


def quarter_window(year: int, quarter_label: str) -> tuple[str, str]:
    quarter = str(quarter_label).strip().upper()
    windows = {
        "Q1": ("01-01", "03-31"),
        "Q2": ("04-01", "06-30"),
        "Q3": ("07-01", "09-30"),
        "Q4": ("10-01", "12-31"),
    }
    if quarter not in windows:
        raise ValueError(f"Unsupported quarter label: {quarter_label}")
    start, end = windows[quarter]
    return f"{year}-{start}", f"{year}-{end}"


def quarter_role(quarter_label: str, period: Any) -> str:
    quarter = str(quarter_label).upper()
    if quarter == period.prior_quarter.label:
        return "prior_quarter"
    if quarter == period.current_quarter.label:
        return "current_quarter"
    if quarter == period.forward_quarter.label:
        return "forward_quarter"
    return "other_quarter"


def quarter_title(quarter_label: str, period: Any) -> str:
    role = quarter_role(quarter_label, period)
    if role == "prior_quarter":
        return period.prior_quarter.title
    if role == "current_quarter":
        return period.current_quarter.title
    if role == "forward_quarter":
        return period.forward_quarter.title
    return f"{str(quarter_label).upper()} {period.current_quarter.year}"


def source_audit_path_for(
    snapshot_date: str,
    *,
    source_audit_root: Path = DEFAULT_SOURCE_AUDIT_ROOT,
    explicit_path: Path | None = None,
) -> Path | None:
    if explicit_path:
        return explicit_path if explicit_path.exists() else None
    candidate = source_audit_root / snapshot_date / "source_contract_audit.json"
    return candidate if candidate.exists() else None


def _index_historical_audit(source_audit: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not source_audit:
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for lane_name in ("active_lane", "candidate_forward_quarter", "candidate_q3"):
        lane = source_audit.get(lane_name) or {}
        for row in lane.get("historical_reports") or []:
            report_id = str(row.get("report_id") or "").strip()
            quarter_label = str(row.get("quarter_label") or "").strip().upper()
            director_slug = str(row.get("director_slug") or "").strip()
            if report_id:
                indexed[f"{report_id}:{quarter_label}:{director_slug}"] = row
                indexed.setdefault(f"{report_id}:{quarter_label}", row)
                indexed.setdefault(report_id, row)
    return indexed


def _index_pi_audit(source_audit: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not source_audit:
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for lane_name in ("active_lane", "candidate_forward_quarter", "candidate_q3"):
        lane = source_audit.get(lane_name) or {}
        for row in lane.get("pi_list_views") or []:
            list_view_id = str(row.get("list_view_id") or "").strip()
            territory = str(row.get("territory") or "").strip()
            quarter_label = str(row.get("quarter_label") or "").strip().upper()
            if list_view_id:
                indexed[list_view_id] = row
                if territory and quarter_label:
                    indexed[f"{territory}:{quarter_label}"] = row
                if territory:
                    indexed.setdefault(territory, row)
    return indexed


def _probe_summary(probe: dict[str, Any] | None) -> dict[str, Any]:
    if not probe:
        return {"status": "not_probed"}
    return {
        "status": probe.get("status") or "unknown",
        "status_code": probe.get("status_code"),
        "issues": probe.get("issues") or [],
        "latest_snapshot_date": probe.get("latest_snapshot_date"),
        "actual_start": probe.get("actual_start"),
        "actual_end": probe.get("actual_end"),
        "row_probe_count": probe.get("row_probe_count"),
        "source_origin": probe.get("source_origin"),
    }


def resolve_historical_sources(
    *,
    snapshot_date: str,
    period: Any,
    territories: dict[str, Any],
    historical_audit_index: dict[str, dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    sources_by_slug: dict[str, list[dict[str, Any]]] = {}
    findings: list[dict[str, Any]] = []
    required_quarters = [
        period.prior_quarter.label,
        period.current_quarter.label,
        period.forward_quarter.label,
    ]
    for territory, config in territories.items():
        director_slug = slugify(str(config.get("director") or ""))
        quarter_ids = {
            str(label).upper(): str(report_id or "").strip()
            for label, report_id in (
                (config.get("historical_trending_report_ids") or {}) | (
                    config.get("forward_quarter_historical_trending_report_ids") or {}
                )
            ).items()
        }
        for quarter_label in required_quarters:
            report_id = quarter_ids.get(quarter_label, "")
            role = quarter_role(quarter_label, period)
            sheet_name = f"{quarter_label} Snapshot Trend"
            if not report_id:
                findings.append(
                    {
                        "severity": "high",
                        "territory": territory,
                        "director_slug": director_slug,
                        "issue": "historical_report_id_missing",
                        "evidence": f"{quarter_label} {role}",
                    }
                )
                sources_by_slug.setdefault(director_slug, []).append(
                    {
                        "source_type": "salesforce_report",
                        "source_lane": "historical_trending",
                        "source_id": "",
                        "sheet_name": sheet_name,
                        "quarter_label": quarter_label,
                        "quarter_title": quarter_title(quarter_label, period),
                        "role": role,
                        "status": "missing_config",
                        "probe": {"status": "not_probed"},
                    }
                )
                continue
            probe = (
                historical_audit_index.get(f"{report_id}:{quarter_label}:{director_slug}")
                or historical_audit_index.get(f"{report_id}:{quarter_label}:{territory}")
                or historical_audit_index.get(f"{report_id}:{quarter_label}")
                or historical_audit_index.get(str(report_id))
            )
            status = (probe or {}).get("status") or "not_probed"
            if status not in ("ok", "not_probed"):
                findings.append(
                    {
                        "severity": "high",
                        "territory": territory,
                        "director_slug": director_slug,
                        "issue": "historical_report_probe_failed",
                        "evidence": f"{report_id} {quarter_label}: {status}",
                    }
                )
            sources_by_slug.setdefault(director_slug, []).append(
                {
                    "source_type": "salesforce_report",
                    "source_lane": "historical_trending",
                    "source_id": str(report_id),
                    "sheet_name": sheet_name,
                    "quarter_label": quarter_label,
                    "quarter_title": quarter_title(quarter_label, period),
                    "role": role,
                    "status": status,
                    "probe": _probe_summary(probe),
                }
            )
    return sources_by_slug, findings


def resolve_pi_sources(
    *,
    territory: str,
    config: dict[str, Any],
    period: Any,
    pi_audit_index: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    current_id = str(config.get("pi_list_view_id") or "").strip()
    current_probe = pi_audit_index.get(current_id) or pi_audit_index.get(territory)
    forward_source = (config.get("forward_quarter_pi_list_views") or {}).get(
        period.forward_quarter.label
    ) or {}
    forward_id = ""
    forward_label = ""
    if isinstance(forward_source, dict):
        forward_id = str(forward_source.get("list_view_id") or forward_source.get("id") or "").strip()
        forward_label = str(
            forward_source.get("list_view_label") or forward_source.get("label") or ""
        ).strip()
    forward_probe = (
        pi_audit_index.get(forward_id)
        or pi_audit_index.get(f"{territory}:{period.forward_quarter.label}")
    )
    return [
        {
            "source_type": "salesforce_list_view",
            "source_lane": "pipeline_inspection_current",
            "source_id": current_id,
            "source_label": str(config.get("pi_list_view_label") or "").strip(),
            "quarter_label": period.current_quarter.label,
            "quarter_title": period.current_quarter.title,
            "role": "current_quarter",
            "status": (current_probe or {}).get("status") or ("configured" if current_id else "missing_config"),
            "probe": _probe_summary(current_probe),
        },
        {
            "source_type": "salesforce_list_view",
            "source_lane": "pipeline_inspection_forward",
            "source_id": forward_id,
            "source_label": forward_label,
            "quarter_label": period.forward_quarter.label,
            "quarter_title": period.forward_quarter.title,
            "role": "forward_quarter",
            "status": (forward_probe or {}).get("status") or ("configured" if forward_id else "missing_config"),
            "probe": _probe_summary(forward_probe),
        },
    ]


def _is_land(row: dict[str, Any]) -> bool:
    return str(row.get("deal_type") or "").strip().lower() == "land"


def _in_window(row: dict[str, Any], start: str, end: str) -> bool:
    token = str(row.get("close_date") or "")[:10]
    return bool(token) and start <= token <= end


def _active_land_rows(rows: list[dict[str, Any]], start: str, end: str) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if _is_land(row)
        and _in_window(row, start, end)
        and is_active_forecast_category(row.get("forecast_category"))
    ]


def bundle_pipeline_decision(
    *,
    bundle_path: Path,
    period: Any,
) -> dict[str, Any]:
    if not bundle_path.exists():
        return {
            "status": "pending_bundle",
            "bundle_path": display_path(bundle_path),
            "current_quarter_active_deals": None,
            "current_quarter_active_arr": None,
            "forward_quarter_active_deals": None,
            "forward_quarter_active_arr": None,
            "display_quarter_label": None,
            "display_quarter_title": None,
            "display_reason": "unknown_until_bundle_exists",
            "requires_forward_quarter_fallback": None,
        }
    payload = load_json(bundle_path)
    rows = ((payload.get("datasets") or {}).get("pipeline_open")) or []
    current_rows = _active_land_rows(
        rows,
        period.current_quarter.start_date,
        period.current_quarter.end_date,
    )
    forward_rows = _active_land_rows(
        rows,
        period.forward_quarter.start_date,
        period.forward_quarter.end_date,
    )
    current_arr = round(sum(float(row.get("arr_unweighted") or 0) for row in current_rows), 2)
    forward_arr = round(sum(float(row.get("arr_unweighted") or 0) for row in forward_rows), 2)
    if current_rows:
        display_quarter = period.current_quarter
        reason = "current_quarter"
        fallback = False
    elif forward_rows:
        display_quarter = period.forward_quarter
        reason = "forward_quarter_fallback"
        fallback = True
    else:
        display_quarter = period.current_quarter
        reason = "empty_current_and_forward_quarter"
        fallback = False
    return {
        "status": "ok",
        "bundle_path": display_path(bundle_path),
        "current_quarter_active_deals": len(current_rows),
        "current_quarter_active_arr": current_arr,
        "forward_quarter_active_deals": len(forward_rows),
        "forward_quarter_active_arr": forward_arr,
        "display_quarter_label": display_quarter.label,
        "display_quarter_title": display_quarter.title,
        "display_reason": reason,
        "requires_forward_quarter_fallback": fallback,
    }


def build_manifest(
    *,
    snapshot_date: str,
    territory_config_path: Path = DEFAULT_TERRITORY_CONFIG,
    bundle_dir: Path | None = None,
    source_audit_path: Path | None = None,
    require_bundles: bool = False,
) -> dict[str, Any]:
    snapshot_date = str(snapshot_date)[:10]
    period = resolve_period_context(
        as_of_date=snapshot_date,
        snapshot_date=snapshot_date,
        deck_date=snapshot_date,
    )
    territories = load_territory_config(territory_config_path)
    resolved_bundle_dir = bundle_dir or DEFAULT_BUNDLE_ROOT / snapshot_date
    resolved_source_audit = source_audit_path_for(
        snapshot_date,
        explicit_path=source_audit_path,
    )
    source_audit = load_json(resolved_source_audit) if resolved_source_audit else None
    historical_audit = _index_historical_audit(source_audit)
    pi_audit = _index_pi_audit(source_audit)
    historical_sources_by_slug, findings = resolve_historical_sources(
        snapshot_date=snapshot_date,
        period=period,
        territories=territories,
        historical_audit_index=historical_audit,
    )

    territory_rows: list[dict[str, Any]] = []
    for territory, config in sorted(territories.items()):
        director = str(config.get("director") or "").strip()
        director_slug = slugify(director)
        bundle_path = bundle_path_for_territory(
            bundle_dir=resolved_bundle_dir,
            territory=territory,
            director_slug=director_slug,
        )
        decision = bundle_pipeline_decision(bundle_path=bundle_path, period=period)
        if require_bundles and decision["status"] != "ok":
            findings.append(
                {
                    "severity": "high",
                    "territory": territory,
                    "director_slug": director_slug,
                    "issue": "bundle_missing_for_quarter_decision",
                    "evidence": display_path(bundle_path),
                }
            )
        elif decision["status"] != "ok":
            findings.append(
                {
                    "severity": "medium",
                    "territory": territory,
                    "director_slug": director_slug,
                    "issue": "bundle_pending_for_quarter_decision",
                    "evidence": display_path(bundle_path),
                }
            )

        pi_sources = resolve_pi_sources(
            territory=territory,
            config=config,
            period=period,
            pi_audit_index=pi_audit,
        )
        for source in pi_sources:
            if source["status"] == "missing_config":
                findings.append(
                    {
                        "severity": "high",
                        "territory": territory,
                        "director_slug": director_slug,
                        "issue": "pi_source_missing_config",
                        "evidence": f"{source['source_lane']} {source['quarter_label']}",
                    }
                )

        historical_sources = historical_sources_by_slug.get(director_slug, [])
        if not historical_sources:
            findings.append(
                {
                    "severity": "high",
                    "territory": territory,
                    "director_slug": director_slug,
                    "issue": "historical_sources_missing",
                    "evidence": "no historical report IDs resolved",
                }
            )

        territory_rows.append(
            {
                "territory": territory,
                "region": region_for_territory(territory),
                "director": director,
                "director_slug": director_slug,
                "source_scope": {
                    "soql_where": str(config.get("soql_where") or "").strip(),
                    "source_filter_policy": (
                        "Configured Salesforce reports/list views must use the same "
                        "regional scope; historical change reports should use Sales Region "
                        "where regional output is required."
                    ),
                },
                "pipeline_display_decision": decision,
                "sources": {
                    "pipeline_inspection": pi_sources,
                    "historical_trending": historical_sources,
                },
            }
        )

    high_findings = [row for row in findings if row.get("severity") == "high"]
    warning_findings = [row for row in findings if row.get("severity") != "high"]
    missing_bundle_count = sum(
        1
        for row in territory_rows
        if row["pipeline_display_decision"]["status"] != "ok"
    )
    fallback_count = sum(
        1
        for row in territory_rows
        if row["pipeline_display_decision"]["requires_forward_quarter_fallback"] is True
    )
    empty_current_count = sum(
        1
        for row in territory_rows
        if row["pipeline_display_decision"]["current_quarter_active_deals"] == 0
    )
    report_count = sum(
        len(row["sources"]["historical_trending"]) for row in territory_rows
    )
    missing_report_id_count = sum(
        1
        for row in territory_rows
        for source in row["sources"]["historical_trending"]
        if not source.get("source_id")
    )
    source_probe_issue_count = sum(
        1
        for row in territory_rows
        for group in row["sources"].values()
        for source in group
        if source.get("status") not in ("ok", "configured", "not_probed")
    )
    status = "blocked" if high_findings else ("warning" if warning_findings else "ok")
    return {
        "artifact_type": "monthly_source_contract",
        "schema_version": "1",
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_date": snapshot_date,
        "status": status,
        "period": period.as_dict(),
        "quarter_policy": period.as_dict()["quarter_policy"],
        "inputs": {
            "territory_config": display_path(territory_config_path),
            "bundle_dir": display_path(resolved_bundle_dir),
            "source_contract_audit": display_path(resolved_source_audit),
        },
        "summary": {
            "territory_count": len(territory_rows),
            "historical_report_count": report_count,
            "missing_report_id_count": missing_report_id_count,
            "source_probe_issue_count": source_probe_issue_count,
            "missing_bundle_count": missing_bundle_count,
            "current_quarter_empty_count": empty_current_count,
            "forward_fallback_count": fallback_count,
            "high_finding_count": len(high_findings),
            "warning_finding_count": len(warning_findings),
        },
        "territories": territory_rows,
        "findings": findings,
    }


def markdown_summary(manifest: dict[str, Any]) -> str:
    summary = manifest["summary"]
    lines = [
        f"# Monthly Source Contract — {manifest['snapshot_date']}",
        "",
        f"- Status: `{manifest['status']}`",
        f"- Quarter policy: `{manifest['quarter_policy']['name']}`",
        f"- Current quarter: `{manifest['period']['current_quarter']['title']}`",
        f"- Forward quarter: `{manifest['period']['forward_quarter']['title']}`",
        f"- Territories: `{summary['territory_count']}`",
        f"- Historical report IDs: `{summary['historical_report_count']}`",
        f"- Forward fallback territories: `{summary['forward_fallback_count']}`",
        f"- Missing bundles: `{summary['missing_bundle_count']}`",
        f"- High findings: `{summary['high_finding_count']}`",
        "",
        "## Quarter Display Decisions",
        "",
        "| Territory | Director | Current Active Deals | Forward Active Deals | Display | Reason |",
        "|---|---|---:|---:|---|---|",
    ]
    for row in manifest["territories"]:
        decision = row["pipeline_display_decision"]
        lines.append(
            f"| {row['territory']} | {row['director']} | "
            f"{decision.get('current_quarter_active_deals') if decision.get('current_quarter_active_deals') is not None else '—'} | "
            f"{decision.get('forward_quarter_active_deals') if decision.get('forward_quarter_active_deals') is not None else '—'} | "
            f"{decision.get('display_quarter_title') or '—'} | {decision.get('display_reason')} |"
        )
    lines.extend(["", "## Findings", ""])
    if not manifest["findings"]:
        lines.append("- none")
    else:
        for finding in manifest["findings"][:40]:
            lines.append(
                f"- `{finding.get('severity')}` `{finding.get('territory', 'global')}` "
                f"{finding.get('issue')}: {finding.get('evidence')}"
            )
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument(
        "--territory-config",
        type=Path,
        default=DEFAULT_TERRITORY_CONFIG,
    )
    parser.add_argument("--bundle-dir", type=Path)
    parser.add_argument("--source-audit-path", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--require-bundles", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot_date = str(args.snapshot_date)[:10]
    manifest = build_manifest(
        snapshot_date=snapshot_date,
        territory_config_path=args.territory_config,
        bundle_dir=args.bundle_dir,
        source_audit_path=args.source_audit_path,
        require_bundles=args.require_bundles,
    )
    output_dir = args.output_root / snapshot_date
    manifest_path = output_dir / "monthly_source_contract.json"
    summary_path = output_dir / "summary.md"
    save_json(manifest_path, manifest)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(markdown_summary(manifest), encoding="utf-8")

    result = {
        "status": manifest["status"],
        "snapshot_date": snapshot_date,
        "manifest_path": display_path(manifest_path),
        "summary_path": display_path(summary_path),
        **manifest["summary"],
    }
    print(json.dumps(result, indent=2) if args.json else result)
    return 2 if manifest["status"] == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
