#!/usr/bin/env python3
"""Audit Revenue Retention & Health against retention-surface rules."""

import argparse
import html
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


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
        "tool": "audit_revenue_retention_health",
        "lane": "export_audits",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _page_labels(dashboard: dict[str, Any]) -> list[str]:
    return [html.unescape(page.get("label", "")) for page in dashboard["state"]["gridLayouts"][0].get("pages", [])]


def _viz_counts(dashboard: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for widget in dashboard["state"]["widgets"].values():
        viz = widget.get("parameters", {}).get("visualizationType", widget.get("type"))
        counts[viz] = counts.get(viz, 0) + 1
    return counts


def _chrome_ratio(dashboard: dict[str, Any]) -> float:
    counts = _viz_counts(dashboard)
    chrome = sum(counts.get(viz, 0) for viz in ("link", "listselector", "text"))
    total = sum(counts.values()) or 1
    return chrome / total


def _widget(dashboard: dict[str, Any], name: str) -> dict[str, Any]:
    return dashboard["state"]["widgets"][name]


def _widget_title(dashboard: dict[str, Any], name: str) -> str:
    title = _widget(dashboard, name).get("parameters", {}).get("title")
    if isinstance(title, dict):
        return html.unescape(title.get("label", ""))
    return html.unescape(title or "")


def _step_query(dashboard: dict[str, Any], name: str) -> str:
    return html.unescape(dashboard["state"]["steps"][name]["query"])


def run_audit(live_export_dir: Path) -> dict[str, Any]:
    dashboard = _load(live_export_dir / "revenue_retention_health" / "dashboard.json")
    page_labels = _page_labels(dashboard)
    chrome_ratio = _chrome_ratio(dashboard)
    viz_counts = _viz_counts(dashboard)

    checks: list[AuditCheck] = []
    checks.append(
        AuditCheck(
            category="persona",
            name="retention_page_set",
            passed=page_labels == ["Retention Summary", "Trends", "Renewal Pipeline", "Churn Analysis"],
            detail=f"Pages = {page_labels}",
        )
    )
    checks.append(
        AuditCheck(
            category="persona",
            name="cross_dashboard_manager_tabs_present",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in (
                    "p1_tab_sales",
                    "p1_tab_csm",
                    "p2_tab_sales",
                    "p2_tab_csm",
                    "p3_tab_sales",
                    "p3_tab_csm",
                    "p4_tab_sales",
                    "p4_tab_csm",
                )
            ),
            detail="Retention surface should expose explicit Sales Manager / CSM Manager tabs on every page.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="core_retention_kpis_present",
            passed=[
                _widget_title(dashboard, "p1_n_nrr"),
                _widget_title(dashboard, "p1_n_grr"),
                _widget_title(dashboard, "p1_n_churn"),
                _widget_title(dashboard, "p1_n_ending"),
            ]
            == ["Starting ARR (€)", "New Logo ARR (€)", "Churn ARR (€)", "Ending ARR (€)"],
            detail=f"KPI titles = {[_widget_title(dashboard, 'p1_n_nrr'), _widget_title(dashboard, 'p1_n_grr'), _widget_title(dashboard, 'p1_n_churn'), _widget_title(dashboard, 'p1_n_ending')]}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="latest_metric_query_uses_account_scope_aggregation",
            passed=all(
                token in _step_query(dashboard, "s_latest_metrics")
                for token in (
                    'RecordType == "account_year_metric"',
                    "group q by YearLabel",
                    "sum(StartingARR)",
                    "sum(EndingARR)",
                    "q = limit q 1;",
                )
            ),
            detail="Latest metric step should aggregate account-year metric rows before limiting to the most recent scoped year.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="year_filter_uses_year_label",
            passed="YearLabel" in html.unescape(json.dumps(dashboard["state"]["steps"]["f_year"])),
            detail="Retention year filter should operate on YearLabel, not mixed quarter labels.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="manager_filter_present",
            passed="f_manager_w" in dashboard["state"]["widgets"],
            detail="CSM manager surface should support a Manager filter.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="summary_uses_target_bullets",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in ("p1_b_nrr", "p1_b_grr")
            ),
            detail="Retention summary should use bullet charts for NRR and GRR target framing.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="bullet_queries_limit_after_foreach",
            passed=all(
                "q = foreach q generate" in _step_query(dashboard, step)
                and _step_query(dashboard, step).find("q = foreach q generate")
                < _step_query(dashboard, step).find("q = limit q 1;")
                for step in ("s_nrr_bullet", "s_grr_bullet")
            ),
            detail="Retention bullet steps should project fields before applying limit 1.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="cohort_analysis_present",
            passed=any("cohort" in _widget_title(dashboard, name).lower() for name in dashboard["state"]["widgets"]),
            detail="Dashboard should include an explicit cohort view, not just quarterly trends.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="renewal_pipeline_has_risk_fields",
            passed=all(token in _step_query(dashboard, "s_renewal_pipeline") for token in ["RiskLevel", "DaysUntilClose", "ForecastCategory"]),
            detail="Renewal pipeline query includes RiskLevel, DaysUntilClose, and ForecastCategory.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="renewal_save_queue_present",
            passed=all(
                token in _step_query(dashboard, "s_renewal_save_queue")
                for token in ("ForecastPulse", "ManagerAsk", "QuarterLabel", "OwnerName", "OppId")
            ),
            detail="Renewal save queue should include pulse, ask, quarter context, owner context, and record id.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="churn_detail_present",
            passed=all(
                token in _step_query(dashboard, "s_churn_root_causes")
                for token in ("ChurnPulse", "ManagerAsk", "QuarterLabel", "OwnerName", "OppId")
            ),
            detail="Churn Analysis page includes a root-cause queue with pulse, ask, quarter context, owner context, and record id.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="chrome_ratio_reasonable",
            passed=chrome_ratio <= 0.55,
            detail=f"Chrome ratio = {chrome_ratio:.1%}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="renewal_pipeline_uses_compare_table",
            passed=_widget(dashboard, "p3_ch_table").get("parameters", {}).get("visualizationType") == "comparisontable",
            detail=f"Renewal pipeline table viz = {_widget(dashboard, 'p3_ch_table').get('parameters', {}).get('visualizationType')}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="retention_summary_has_bridge_and_trend",
            passed=all(viz_counts.get(viz, 0) >= count for viz, count in {"waterfall": 1, "combo": 2, "bullet": 2}.items()),
            detail=f"Viz counts = {viz_counts}",
        )
    )

    return {
        "live_export_dir": str(live_export_dir),
        "dashboard": dashboard["label"],
        "page_labels": page_labels,
        "widget_count": len(dashboard["state"].get("widgets", {})),
        "step_count": len(dashboard["state"].get("steps", {})),
        "chrome_ratio": chrome_ratio,
        "checks": [asdict(check) for check in checks],
        "pass_count": sum(1 for check in checks if check.passed),
        "fail_count": sum(1 for check in checks if not check.passed),
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Revenue Retention & Health Audit",
        "",
        f"- Live export: `{payload['live_export_dir']}`",
        f"- Dashboard: `{payload['dashboard']}`",
        f"- Pages: `{payload['page_labels']}`",
        f"- Widget count: `{payload['widget_count']}`",
        f"- Step count: `{payload['step_count']}`",
        f"- Chrome ratio: `{payload['chrome_ratio']:.1%}`",
        "",
        "## Checks",
        "",
    ]
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
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = run_audit(live_export_dir)
    check_count = len(payload["checks"])
    audit_json_path = output_dir / "audit.json"
    audit_md_path = output_dir / "audit.md"
    audit_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_markdown(audit_md_path, payload)

    if emit_text:
        print(audit_json_path)
        print(audit_md_path)

    status = "warn" if payload["fail_count"] else "ok"
    messages = [
        _make_message(
            "warn" if payload["fail_count"] else "info",
            "audit_findings" if payload["fail_count"] else "audit_clean",
            (
                f"Audit found {payload['fail_count']} failing check(s) out of "
                f"{check_count}."
            )
            if payload["fail_count"]
            else f"Audit passed {payload['pass_count']} of {check_count} checks.",
        )
    ]
    result = _make_result(
        status=status,
        messages=messages,
        artifacts=[
            _make_artifact("json", audit_json_path),
            _make_artifact("markdown", audit_md_path),
        ],
        summary={
            "dashboard": payload["dashboard"],
            "pass_count": payload["pass_count"],
            "fail_count": payload["fail_count"],
            "check_count": check_count,
            "widget_count": payload["widget_count"],
            "step_count": payload["step_count"],
            "chrome_ratio": payload["chrome_ratio"],
            "output_dir": str(output_dir),
        },
        audit=payload,
    )
    return result, (1 if payload["fail_count"] else 0)


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
