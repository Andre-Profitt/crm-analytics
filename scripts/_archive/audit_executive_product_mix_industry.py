#!/usr/bin/env python3
"""Audit Executive Product Mix & Industry against executive GTM rules."""

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
        "tool": "audit_executive_product_mix_industry",
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
    return dashboard["state"]["steps"][name]["query"]


def run_audit(live_export_dir: Path) -> dict[str, Any]:
    dashboard = _load(live_export_dir / "executive_product_mix_industry" / "dashboard.json")
    page_labels = _page_labels(dashboard)
    chrome_ratio = _chrome_ratio(dashboard)
    viz_counts = _viz_counts(dashboard)

    checks: list[AuditCheck] = []
    checks.append(
        AuditCheck(
            category="persona",
            name="exec_page_count_reasonable",
            passed=page_labels == ["Summary", "Industry Breakdown"],
            detail=f"Pages = {page_labels}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="summary_kpi_strip_present",
            passed=[
                _widget_title(dashboard, "p1_n_accounts"),
                _widget_title(dashboard, "p1_n_installed"),
                _widget_title(dashboard, "p1_n_expansion"),
                _widget_title(dashboard, "p1_n_whitespace"),
                _widget_title(dashboard, "p1_n_saas"),
            ]
            == ["Accounts in Scope", "Installed ARR", "Open Expansion ARR", "Whitespace ARR", "SaaS Mix %"],
            detail="Executive KPI strip includes scoped account count, installed ARR, expansion ARR, whitespace ARR, and SaaS mix.",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="mix_heatmap_and_gap_views_present",
            passed=all(
                name in dashboard["state"]["widgets"]
                for name in ["p1_ch_mix", "p1_ch_heatmap", "p2_ch_gap", "p2_tbl_accounts"]
            ),
            detail="Dashboard includes mix, whitespace heatmap, industry gap view, and top-account whitespace table.",
        )
    )
    checks.append(
        AuditCheck(
            category="action",
            name="top_accounts_query_has_account_context",
            passed=all(token in _step_query(dashboard, "s_top_accounts") for token in ["AccountName", "WhitespaceScore", "ExpansionScore", "AccountId"]),
            detail="Top-account whitespace query includes account identity and whitespace/expansion scores.",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="exec_chrome_ratio_reasonable",
            passed=chrome_ratio <= 0.50,
            detail=f"Chrome ratio = {chrome_ratio:.1%}",
        )
    )
    checks.append(
        AuditCheck(
            category="visual",
            name="heatmap_and_table_balance_present",
            passed=viz_counts.get("heatmap", 0) >= 2 and viz_counts.get("comparisontable", 0) >= 2,
            detail=f"Viz counts = {viz_counts}",
        )
    )
    checks.append(
        AuditCheck(
            category="story",
            name="global_geo_view_present",
            passed=any(viz in viz_counts for viz in ("choropleth", "map")),
            detail="Executive product/GTM surface should eventually include a geographic view of ARR or whitespace.",
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
        "# Executive Product Mix & Industry Audit",
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
