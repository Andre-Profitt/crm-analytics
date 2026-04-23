#!/usr/bin/env python3
"""Audit Lead Funnel against manager demand-surface rules."""

import argparse
import html
import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any


CURRENT_YEAR = date.today().year


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
        "tool": "audit_lead_funnel",
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
    return [
        html.unescape(page.get("label", ""))
        for page in dashboard["state"]["gridLayouts"][0].get("pages", [])
    ]


def _widget(dashboard: dict[str, Any], name: str) -> dict[str, Any]:
    return dashboard["state"]["widgets"][name]


def _widget_title(dashboard: dict[str, Any], name: str) -> str:
    title = _widget(dashboard, name).get("parameters", {}).get("title")
    if isinstance(title, dict):
        return html.unescape(title.get("label", ""))
    return html.unescape(title or "")


def _step_query(dashboard: dict[str, Any], name: str) -> str:
    return dashboard["state"]["steps"][name]["query"]


def run_audit(live_export_dir: Path) -> dict[str, Any]:
    dashboard = _load(live_export_dir / "lead_funnel" / "dashboard.json")
    page_labels = _page_labels(dashboard)
    current_year_token = f"{CURRENT_YEAR}-01"
    current_year_end = f"{CURRENT_YEAR}-12"

    checks: list[AuditCheck] = []
    checks.append(
        AuditCheck(
            category="persona",
            name="lead_funnel_page_set",
            passed=page_labels
            == ["Summary", "Trend & Forecast", "Drivers & Segments", "Exceptions & Actions"],
            detail=f"Pages = {page_labels}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="summary_explicitly_current_year",
            passed=str(CURRENT_YEAR) in _widget(dashboard, "p1_hdr")["parameters"]["content"]["richTextContent"][2]["insert"],
            detail=f"Header subtitle = {_widget(dashboard, 'p1_hdr')['parameters']['content']['richTextContent'][2]['insert']!r}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="trend_steps_current_year_scoped",
            passed=all(
                token in _step_query(dashboard, step)
                for step in ("s_monthly_trajectory", "s_conversion_volume")
                for token in (current_year_token, current_year_end)
            ),
            detail=(
                f"s_monthly_trajectory and s_conversion_volume should both filter to {CURRENT_YEAR}. "
                f"Found tokens {current_year_token}..{current_year_end}."
            ),
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="detail_steps_filter_on_monthlabel_not_missing_createdmonth",
            passed="CreatedMonth" not in _step_query(dashboard, "s_source_mix")
            and current_year_token in _step_query(dashboard, "s_source_mix")
            and current_year_end in _step_query(dashboard, "s_source_mix"),
            detail="Detail-level source mix should filter on MonthLabel because CreatedMonth is not part of the live Lead Funnel dataset.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="trend_volume_uses_combo_not_stackcolumn",
            passed=_widget(dashboard, "p2_ch_volume").get("parameters", {}).get("visualizationType")
            == "combo",
            detail=(
                "Trend page throughput widget should be a combo chart so converted output "
                "reads cleanly against intake/qualified volume."
            ),
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="forecast_widget_uses_timeline",
            passed=_widget(dashboard, "p2_ch_forecast").get("parameters", {}).get("visualizationType")
            == "timeline",
            detail=f"p2_ch_forecast viz = {_widget(dashboard, 'p2_ch_forecast').get('parameters', {}).get('visualizationType')}",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="exception_tables_keep_next_action_context",
            passed=all(
                token in _step_query(dashboard, step)
                for step in ("s_top_response_risk", "s_top_stalled")
                for token in ("NextBestAction", "Id")
            ),
            detail="Exception-table steps include NextBestAction and Id for queue/action context.",
        )
    )

    return {
        "live_export_dir": str(live_export_dir),
        "dashboard": dashboard["label"],
        "page_labels": page_labels,
        "widget_count": len(dashboard["state"].get("widgets", {})),
        "step_count": len(dashboard["state"].get("steps", {})),
        "checks": [asdict(check) for check in checks],
        "pass_count": sum(1 for check in checks if check.passed),
        "fail_count": sum(1 for check in checks if not check.passed),
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Lead Funnel Audit",
        "",
        f"- Live export: `{payload['live_export_dir']}`",
        f"- Dashboard: `{payload['dashboard']}`",
        f"- Pages: `{payload['page_labels']}`",
        f"- Widget count: `{payload['widget_count']}`",
        f"- Step count: `{payload['step_count']}`",
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
    audit_json_path.write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
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
