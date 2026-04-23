#!/usr/bin/env python3
"""Audit the source-truth executive revenue dashboard against hard business gates."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from crm_analytics_helpers import _soql, get_auth  # noqa: E402

CURRENT_FY = date.today().year
PRIOR_FY = CURRENT_FY - 1

SOQL = (
    "SELECT FiscalYear, ForecastCategoryName, IsWon, IsClosed, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedForecastARR "
    "FROM Opportunity "
    f"WHERE FiscalYear IN ({PRIOR_FY}, {CURRENT_FY}) "
    "AND CloseDate != null "
    "AND (IsWon = true OR IsClosed = false)"
)


@dataclass
class AuditCheck:
    category: str
    name: str
    passed: bool
    detail: str


def _make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def _make_artifact(kind: str, path: Path) -> dict[str, str]:
    return {"kind": kind, "path": str(path)}


def _make_result(
    *,
    status: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "audit_source_truth_executive_revenue",
        "lane": "wave_data_validations",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def _safe_sum(rows: list[dict[str, Any]], *, fy: int, won: bool | None = None, closed: bool | None = None, category: str | None = None) -> float:
    total = 0.0
    for row in rows:
        row_fy = int(float(row.get("FiscalYear") or 0))
        if row_fy != fy:
            continue
        if won is not None and bool(row.get("IsWon")) != won:
            continue
        if closed is not None and bool(row.get("IsClosed")) != closed:
            continue
        if category is not None and (row.get("ForecastCategoryName") or "") != category:
            continue
        total += float(row.get("ConvertedForecastARR") or 0)
    return round(total, 2)


def _load_dashboard_json(live_export_dir: Path) -> dict[str, Any]:
    dashboard_path = live_export_dir / "dashboard.json"
    if not dashboard_path.exists():
        candidates = sorted(live_export_dir.glob("*/dashboard.json"))
        if len(candidates) == 1:
            dashboard_path = candidates[0]
        else:
            raise FileNotFoundError(f"Could not resolve dashboard.json under {live_export_dir}")
    return json.loads(dashboard_path.read_text(encoding="utf-8"))


def _widget(dashboard: dict[str, Any], name: str) -> dict[str, Any]:
    return dashboard["state"]["widgets"][name]


def _step(dashboard: dict[str, Any], name: str) -> dict[str, Any]:
    return dashboard["state"]["steps"][name]


def _widget_title(dashboard: dict[str, Any], widget_name: str) -> str:
    title = _widget(dashboard, widget_name).get("parameters", {}).get("title")
    if isinstance(title, str):
        return title
    if isinstance(title, dict):
        return title.get("label", "")
    return ""


def _ladder_measure_names(dashboard: dict[str, Any], widget_name: str) -> list[str]:
    widget = _widget(dashboard, widget_name)
    params = widget.get("parameters", {})
    column_map = params.get("columnMap") or {}
    plots = column_map.get("plots")
    if plots:
        return plots

    step_name = params.get("step")
    if not step_name:
        return []
    query = _step(dashboard, step_name).get("query", "")
    metric_names: list[str] = []
    for alias in ["Actual", "Commit", "BestCase", "Pipeline", "Target"]:
        if f" as {alias}" in query:
            metric_names.append(alias)
    return metric_names


def run_audit(live_export_dir: Path) -> dict[str, Any]:
    dashboard = _load_dashboard_json(live_export_dir)
    inst, tok = get_auth()
    rows = _soql(inst, tok, SOQL)

    fy25_closed = _safe_sum(rows, fy=PRIOR_FY, won=True)
    fy26_closed = _safe_sum(rows, fy=CURRENT_FY, won=True)
    fy26_commit_open = _safe_sum(rows, fy=CURRENT_FY, closed=False, category="Commit")
    fy26_best_open = _safe_sum(rows, fy=CURRENT_FY, closed=False, category="Best Case")
    fy26_pipeline_open = _safe_sum(rows, fy=CURRENT_FY, closed=False, category="Pipeline")

    target = round(fy25_closed * 1.10, 2)
    best_case_call = round(fy26_closed + fy26_commit_open + fy26_best_open, 2)
    pipeline_call = round(best_case_call + fy26_pipeline_open, 2)
    needed_from_pipeline = round(max(target - best_case_call, 0), 2)

    checks: list[AuditCheck] = []

    ladder = _widget(dashboard, "p1_ch_ladder")
    ladder_measures = _ladder_measure_names(dashboard, "p1_ch_ladder")
    checks.append(
        AuditCheck(
            category="metric",
            name="ladder_measure_set",
            passed=ladder_measures == ["Actual", "Commit", "BestCase", "Pipeline", "Target"],
            detail=f"Ladder measures = {ladder_measures}",
        )
    )

    checks.append(
        AuditCheck(
            category="metric",
            name="no_visible_prior_year_series",
            passed="FY2025" not in json.dumps(ladder),
            detail="Ladder widget contains no visible FY2025 label or series reference.",
        )
    )

    bridge_query = _step(dashboard, "s_forecast_bridge")["query"]
    checks.append(
        AuditCheck(
            category="story",
            name="bridge_uses_best_case_gap",
            passed="sum(BestCaseCallARR)" in bridge_query and "sum(PipelineCallARR)" not in bridge_query,
            detail="Forecast bridge closes to Best Case, not Pipeline.",
        )
    )

    region_table = _widget(dashboard, "p1_tbl_region")["parameters"]
    checks.append(
        AuditCheck(
            category="visual",
            name="region_table_columns",
            passed=region_table["columns"]
            == ["SalesRegion", "CoverageStatus", "PromotionNeedPct", "NeededFromPipelineARR", "LowConfidencePipelineARR", "BestCaseGapARR"],
            detail=f"Columns = {region_table['columns']}",
        )
    )

    queue_columns = _widget(dashboard, "p2_tbl_risk")["parameters"]["columns"]
    queue_step = _step(dashboard, "s_risk_queue")["query"]
    checks.append(
        AuditCheck(
            category="action",
            name="action_queue_columns",
            passed={"OpportunityName", "AccountName", "ARR", "ForecastPulse", "LeadershipAsk", "CloseQuarter", "OwnerName"} == set(queue_columns),
            detail=f"Queue columns = {queue_columns}",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="action_queue_exec_arr_threshold",
            passed=("ARR >= 900000" in queue_step) or ("ARR &gt;= 900000" in queue_step),
            detail=f"Queue query = {queue_step}",
        )
    )

    quarter_filter_title = _widget_title(dashboard, "p1_f_quarter") if "p1_f_quarter" in dashboard["state"]["widgets"] else ""
    checks.append(
        AuditCheck(
            category="story",
            name="quarter_filter_present",
            passed=quarter_filter_title == "Close Quarter",
            detail=f"Quarter filter title = {quarter_filter_title}",
        )
    )

    kpi_titles = [
        _widget_title(dashboard, "p1_kpi_actual"),
        _widget_title(dashboard, "p1_kpi_commit"),
        _widget_title(dashboard, "p1_kpi_best"),
        _widget_title(dashboard, "p1_kpi_pipeline"),
        _widget_title(dashboard, "p1_kpi_target"),
        _widget_title(dashboard, "p1_kpi_needed"),
    ]
    checks.append(
        AuditCheck(
            category="story",
            name="kpi_story_complete",
            passed=kpi_titles
            == [
                "Actual Closed Won ARR",
                "Commit Forecast ARR",
                "Best Case Forecast ARR",
                "Pipeline Envelope ARR",
                "10% YoY Target ARR",
                "Needed Promotion ARR",
            ],
            detail=f"KPI titles = {kpi_titles}",
        )
    )

    layout = dashboard["state"]["gridLayouts"][0]["pages"][0]["widgets"]
    checks.append(
        AuditCheck(
            category="persona",
            name="exec_widget_count_reasonable",
            passed=len(layout) <= 18,
            detail=f"Executive page widget count = {len(layout)}",
        )
    )

    widget_types = {name: _widget(dashboard, name)["parameters"].get("visualizationType", _widget(dashboard, name)["type"]) for name in dashboard["state"]["widgets"]}
    checks.append(
        AuditCheck(
            category="visual",
            name="exec_visual_mix_present",
            passed=all(
                required in widget_types.values()
                for required in ["line", "waterfall", "comparisontable", "hbar", "choropleth"]
            ),
            detail=f"Visualization types present = {sorted(set(widget_types.values()))}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="global_arr_map_present",
            passed=(
                ("p2_map_geo_v2" in dashboard["state"]["widgets"] and widget_types.get("p2_map_geo_v2") == "choropleth")
                or ("p2_map_geo" in dashboard["state"]["widgets"] and widget_types.get("p2_map_geo") == "choropleth")
            ),
            detail="Executive revenue surface includes a global ARR choropleth tied to forecast-of-record geography.",
        )
    )
    map_widget_name = "p2_map_geo_v2" if "p2_map_geo_v2" in dashboard["state"]["widgets"] else "p2_map_geo"
    map_widget = _widget(dashboard, map_widget_name)
    map_step = dashboard["state"]["steps"].get(map_widget["parameters"].get("step", ""), {})
    map_column_map = map_widget["parameters"].get("columnMap", {})
    checks.append(
        AuditCheck(
            category="metric",
            name="map_uses_dedicated_geo_dataset",
            passed="Executive_Revenue_Source_Geo" in map_step.get("query", ""),
            detail=f"Map step query = {map_step.get('query', '')}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="map_column_map_is_complete",
            passed=all(key in map_column_map for key in ["locations", "color", "dimensionAxis", "plots", "trellis", "split"]),
            detail=f"Map columnMap keys = {sorted(map_column_map.keys())}",
        )
    )
    category_mix_title = _widget_title(dashboard, "p2_ch_category_mix") if "p2_ch_category_mix" in dashboard["state"]["widgets"] else ""
    category_mix_step = dashboard["state"]["steps"].get("s_forecast_category_trend", {})
    checks.append(
        AuditCheck(
            category="story",
            name="forecast_category_mix_present",
            passed=category_mix_title == "Cumulative Open ARR by Forecast Category",
            detail=f"Category mix title = {category_mix_title}",
        )
    )
    checks.append(
        AuditCheck(
            category="metric",
            name="omitted_category_reconciliation_present",
            passed=(
                "OpenOpportunityFlag == 1" in category_mix_step.get("query", "")
                and (
                    "OpenOmittedARR" in category_mix_step.get("query", "")
                    or "ForecastCategory == &quot;Omitted&quot;" in category_mix_step.get("query", "")
                    or 'ForecastCategory == "Omitted"' in category_mix_step.get("query", "")
                )
            ),
            detail=f"Category mix query = {category_mix_step.get('query', '')}",
        )
    )

    checks.append(
        AuditCheck(
            category="story",
            name="exceptions_section_present",
            passed="Exceptions" in json.dumps(_widget(dashboard, "p2_hdr")),
            detail="Dashboard includes an explicit exceptions section below the summary story.",
        )
    )

    checks.append(
        AuditCheck(
            category="persona",
            name="one_page_exec_surface",
            passed=dashboard["state"]["gridLayouts"][0]["pages"][0]["label"] == "Overview" and len(dashboard["state"]["gridLayouts"][0]["pages"]) == 1,
            detail="Executive surface is a single overview page, not fragmented page navigation.",
        )
    )

    values = {
        "fy25_closed_won_arr": fy25_closed,
        "fy26_closed_won_arr": fy26_closed,
        "fy26_commit_call_arr": round(fy26_closed + fy26_commit_open, 2),
        "fy26_best_case_call_arr": best_case_call,
        "fy26_pipeline_call_arr": pipeline_call,
        "fy26_10pct_yoy_target_arr": target,
        "needed_from_pipeline_arr": needed_from_pipeline,
        "best_case_gap_to_target_arr": round(best_case_call - target, 2),
        "pipeline_gap_to_target_arr": round(pipeline_call - target, 2),
    }

    return {
        "checked_at": date.today().isoformat(),
        "dashboard_label": dashboard["label"],
        "live_export_dir": str(live_export_dir),
        "source_truth_values": values,
        "checks": [asdict(check) for check in checks],
        "pass_count": sum(1 for check in checks if check.passed),
        "fail_count": sum(1 for check in checks if not check.passed),
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        f"# Executive Revenue Audit ({payload['checked_at']})",
        "",
        f"- Dashboard: `{payload['dashboard_label']}`",
        f"- Live export: `{payload['live_export_dir']}`",
        "",
        "## Source Truth Values",
        "",
    ]
    for key, value in payload["source_truth_values"].items():
        lines.append(f"- `{key}`: `{value:,.2f}`")
    lines.extend(["", "## Checks", ""])
    current_category = None
    for check in payload["checks"]:
        if check["category"] != current_category:
            current_category = check["category"]
            lines.extend(["", f"### {current_category.title()}"])
        status = "PASS" if check["passed"] else "FAIL"
        lines.append(f"- `{status}` `{check['name']}`: {check['detail']}")
    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-export-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    return parser


def run_audit_command(
    live_export_dir: Path,
    output_dir: Path,
    *,
    emit_text: bool = True,
) -> tuple[dict[str, Any], int]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = run_audit(live_export_dir.resolve())
    json_path = output_dir / "audit.json"
    md_path = output_dir / "audit.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_markdown(md_path, payload)

    if emit_text:
        print(json_path)
        print(md_path)

    status = "warn" if payload["fail_count"] else "ok"
    messages = [
        _make_message(
            "warn" if payload["fail_count"] else "info",
            "audit_findings" if payload["fail_count"] else "audit_clean",
            (
                f"Audit found {payload['fail_count']} failing check(s) out of "
                f"{len(payload['checks'])}."
            )
            if payload["fail_count"]
            else f"Audit passed {payload['pass_count']} of {len(payload['checks'])} checks.",
        )
    ]
    result = _make_result(
        status=status,
        messages=messages,
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "dashboard": payload["dashboard_label"],
            "pass_count": payload["pass_count"],
            "fail_count": payload["fail_count"],
            "checked_at": payload["checked_at"],
            "output_dir": str(output_dir),
            "source_truth_values": payload["source_truth_values"],
        },
        audit=payload,
    )
    return result, 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result, exit_code = run_audit_command(
        args.live_export_dir.resolve(),
        args.output_dir.resolve(),
        emit_text=not args.json,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
