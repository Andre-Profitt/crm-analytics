#!/usr/bin/env python3
"""Audit Lead Management KPIs against manager-surface rules."""

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
        "tool": "audit_lead_management",
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


def _step_query(dashboard: dict[str, Any], name: str) -> str:
    return dashboard["state"]["steps"][name]["query"]


def run_audit(live_export_dir: Path) -> dict[str, Any]:
    dashboard = _load(live_export_dir / "lead_management_kpis" / "dashboard.json")

    checks: list[AuditCheck] = []
    page_labels = _page_labels(dashboard)
    chrome_ratio = _chrome_ratio(dashboard)
    viz_counts = _viz_counts(dashboard)

    checks.append(
        AuditCheck(
            category="persona",
            name="manager_page_count_reasonable",
            passed=len(page_labels) <= 4,
            detail=f"Page count = {len(page_labels)} ({page_labels})",
        )
    )
    checks.append(
        AuditCheck(
            category="persona",
            name="no_analyst_only_pages_on_manager_surface",
            passed=all(label not in {"Advanced Analytics", "Statistical Analysis"} for label in page_labels),
            detail=f"Pages = {page_labels}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="chrome_ratio_reasonable",
            passed=chrome_ratio <= 0.60,
            detail=f"Chrome ratio = {chrome_ratio:.1%}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="funnel_and_conversion_core_present",
            passed=all(name in dashboard["state"]["widgets"] for name in ["p1_funnel", "p1_conv_rate", "p1_avg_days"]),
            detail="Lead funnel, conversion rate, and conversion speed widgets are present.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="conversion_rate_trend_uses_materialized_converted_field",
            passed="sum(ConvertedCount) / count()" in _step_query(dashboard, "s_trend_conv")
            and "sum(case when ConvertedFlag" not in _step_query(dashboard, "s_trend_conv"),
            detail="Conversion-rate KPI should use the materialized ConvertedCount field, not an invalid sum(case ...) expression.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="no_activity_queue_has_record_context",
            passed=all(token in _step_query(dashboard, "s_no_activity") for token in ["Id", "LeadScore", "LeadAgeDays"]),
            detail="No-activity queue query includes Id, LeadScore, and LeadAgeDays.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="hot_lead_owner_table_present",
            passed="p4_ch_hot_owner" in dashboard["state"]["widgets"],
            detail="Hot lead response-by-owner compare table is present.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="activity_page_uses_target_bullets",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in ("p4_bullet_activity", "p4_bullet_source_attrib")
            ),
            detail="Activity page should use bullet charts for activity rate and source attribution targets.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="advanced_visual_sprawl_reasonable",
            passed=sum(viz_counts.get(viz, 0) for viz in ("sankey", "treemap", "bubble")) <= 2,
            detail=f"Advanced viz counts = sankey:{viz_counts.get('sankey',0)} treemap:{viz_counts.get('treemap',0)} bubble:{viz_counts.get('bubble',0)}",
        )
    )
    checks.append(
        AuditCheck(
            category="persona",
            name="widget_count_reasonable",
            passed=len(dashboard["state"].get("widgets", {})) <= 80,
            detail=f"Widget count = {len(dashboard['state'].get('widgets', {}))}",
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
        "# Lead Management Audit",
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
            _make_artifact("json", audit_json_path),
            _make_artifact("markdown", audit_md_path),
        ],
        summary={
            "dashboard": payload["dashboard"],
            "pass_count": payload["pass_count"],
            "fail_count": payload["fail_count"],
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
