#!/usr/bin/env python3
"""
dashboard_state_dump.py — pristine-state audit for the Sales Director Monthly
and Sales Ops Quarterly dashboards.

Read-only. Walks every widget on both dashboards, cross-references each
widget binding against its source report's shape, and emits a structured
drift report with one flag per detectable defect:

  no-convert:<field>             revenue aggregate missing .CONVERT
  amount-not-arr-on-widget       widget binding uses s!AMOUNT
  fiscal-date-filter:<form>      fiscal framing on report standardDateFilter
  fiscal-grouping:<col>          fiscal framing on groupingsDown

A "pristine" dashboard emits zero flags.

Usage:
  python3 scripts/dashboard_state_dump.py                     # full markdown report to stdout
  python3 scripts/dashboard_state_dump.py --summary-only      # concise startup check
  python3 scripts/dashboard_state_dump.py --format json       # JSON only
  python3 scripts/dashboard_state_dump.py --format markdown   # Markdown only
  python3 scripts/dashboard_state_dump.py --target-org apro@simcorp.com
  python3 scripts/dashboard_state_dump.py --dashboard 01ZTb00000FSP7hMAH
  python3 scripts/dashboard_state_dump.py --out-md /tmp/state.md --out-json /tmp/state.json

Auth: reads sf CLI access token for the target org via
`sf org display --target-org <username> --json`. Defaults to the production
org used by this repo (`apro@simcorp.com`).

CI gate idea: add `python3 scripts/dashboard_state_dump.py --fail-if-drifted`
to a pre-commit or GitHub Action. Exits non-zero when any flag other than the
explicitly-allowed deferred fiscal-grouping set is present.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from typing import Any

API_VERSION = "v66.0"
TARGET_ORG_DEFAULT = "apro@simcorp.com"

# Both production dashboards.
DEFAULT_DASHBOARDS = [
    ("01ZTb00000FSP7hMAH", "Sales Directors Monthly"),
    ("01ZTb00000FSP9JMAX", "Sales Ops Quarterly KPI"),
]

# Deferred defects that are OK to see in the output — they require a schema
# change (bucket field or custom formula field) and were empirically verified
# unfixable via the Reports API PATCH on 2026-04-08.
DEFERRED_FISCAL_GROUPING_REPORTS: set[str] = {
    "00OTb000008ekxBMAQ",  # D1 Renewal ACV by Quarter
    "00OTb000008TZsDMAW",  # D1 Forecast Accuracy
    "00OTb000008eksLMAQ",  # D1 Renewals by Quarter
    "00OTb000008SrmLMAS",  # D2 Overdue Opportunities
}

# Dashboards classified as "executive review surfaces" — the ones used
# in live monthly/quarterly meetings by senior stakeholders. Research
# sources (Stephen Few, Gartner 2023, Tufte) converge on 6-9 widgets as
# the cognitive ceiling for this audience. We treat violations as
# WARNINGS (not active flags) because widget count is a design choice
# that requires stakeholder sign-off to consolidate, not a data defect.
EXECUTIVE_DASHBOARDS: set[str] = {
    "01ZTb00000FSP7hMAH",  # Sales Directors Monthly
}
EXECUTIVE_WIDGET_CEILING_TARGET = 8  # research-recommended target
EXECUTIVE_WIDGET_CEILING_MAX = 12  # absolute upper bound


@dataclass
class WidgetState:
    dashboard_id: str
    dashboard_label: str
    index: int
    header: str
    report_id: str
    report_name: str | None
    report_format: str | None
    widget_aggregates: list[str] = field(default_factory=list)
    widget_groupings: list[str] = field(default_factory=list)
    report_aggregates: list[str] = field(default_factory=list)
    report_detail_columns: list[str] = field(default_factory=list)
    standard_date_filter: str | None = None
    groupings_down: list[str] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)
    flags_deferred: list[str] = field(default_factory=list)


def sh(*args: str, capture: bool = True) -> str:
    """Shell out, raise on nonzero."""
    result = subprocess.run(
        list(args),
        check=True,
        capture_output=capture,
        text=True,
    )
    return result.stdout


def auth(target_org: str) -> tuple[str, str]:
    raw = sh("sf", "org", "display", "--target-org", target_org, "--json")
    data = json.loads(raw)
    inst = data["result"]["instanceUrl"]
    token = data["result"]["accessToken"]
    return inst, token


def http_get(inst: str, token: str, path: str) -> dict[str, Any]:
    """GET via curl — matches the sf-CLI + curl pattern from CLAUDE.md."""
    url = f"{inst}{path}"
    raw = sh(
        "curl",
        "-sS",
        "-H",
        f"Authorization: Bearer {token}",
        url,
    )
    return json.loads(raw)


def fmt_date_filter(sdf: Any) -> str:
    if sdf is None:
        return "CUSTOM"
    if isinstance(sdf, str):
        return sdf
    if isinstance(sdf, dict):
        return sdf.get("durationValue") or "CUSTOM"
    return str(sdf)


def short(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[: n - 1] + "…"


def analyze_widget(
    dashboard_id: str,
    dashboard_label: str,
    idx: int,
    component: dict[str, Any],
    inst: str,
    token: str,
) -> WidgetState:
    header = component.get("header") or component.get("title") or "(untitled)"
    if isinstance(header, dict):
        header = header.get("label") or "(untitled)"
    report_id = component.get("reportId") or "?"
    widget_aggs = [
        a.get("name", "?")
        for a in (component.get("properties", {}).get("aggregates") or [])
    ]
    widget_groupings = [
        g.get("name", "?")
        for g in (component.get("properties", {}).get("groupings") or [])
    ]
    # Pull FlexTable tableColumns aggregate rows into widget_aggs for drift-detection
    for tc in (
        component.get("properties", {})
        .get("visualizationProperties", {})
        .get("tableColumns")
        or []
    ):
        if tc.get("type") == "aggregate" and tc.get("column"):
            if tc["column"] not in widget_aggs:
                widget_aggs.append(tc["column"])

    state = WidgetState(
        dashboard_id=dashboard_id,
        dashboard_label=dashboard_label,
        index=idx,
        header=str(header),
        report_id=report_id,
        report_name=None,
        report_format=None,
        widget_aggregates=widget_aggs,
        widget_groupings=widget_groupings,
    )

    # Fetch the source report's describe payload.
    try:
        desc = http_get(
            inst,
            token,
            f"/services/data/{API_VERSION}/analytics/reports/{report_id}/describe",
        )
        rm = desc.get("reportMetadata") or {}
        state.report_name = rm.get("name")
        state.report_format = rm.get("reportFormat")
        state.report_aggregates = list(rm.get("aggregates") or [])
        state.report_detail_columns = list(rm.get("detailColumns") or [])
        state.standard_date_filter = fmt_date_filter(rm.get("standardDateFilter"))
        state.groupings_down = [
            g.get("name", "?") for g in (rm.get("groupingsDown") or [])
        ]
    except Exception as e:
        state.flags.append(f"report-fetch-failed:{e}")
        return state

    # Flag 1: fiscal date filter
    if re.search(r"FISCAL", state.standard_date_filter or "", re.IGNORECASE):
        state.flags.append(f"fiscal-date-filter:{state.standard_date_filter}")

    # Flag 2: fiscal grouping (potentially deferred)
    for g in state.groupings_down:
        if re.search(r"FISCAL", g, re.IGNORECASE):
            flag = f"fiscal-grouping:{g}"
            if report_id in DEFERRED_FISCAL_GROUPING_REPORTS:
                state.flags_deferred.append(flag)
            else:
                state.flags.append(flag)

    # Flag 3: widget binding uses s!AMOUNT
    for a in widget_aggs:
        if a == "s!AMOUNT":
            state.flags.append("amount-not-arr-on-widget")
            break

    # Flag 4: widget binding has revenue aggregate missing .CONVERT
    for a in widget_aggs:
        if re.search(r"APTS_(Opportunity|Renewal|Forecast)_(ARR|ACV)", a):
            if not a.endswith(".CONVERT"):
                clean = a.replace("s!Opportunity.", "")
                state.flags.append(f"no-convert:{clean}")

    return state


def build_report(
    dashboards: list[tuple[str, str]], inst: str, token: str
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "generated_at": subprocess.run(
            ["date", "-u", "+%Y-%m-%dT%H:%M:%SZ"], capture_output=True, text=True
        ).stdout.strip(),
        "dashboards": [],
        "summary": {},
    }

    for dash_id, dash_label in dashboards:
        raw = http_get(
            inst, token, f"/services/data/{API_VERSION}/analytics/dashboards/{dash_id}"
        )
        meta = raw.get("dashboardMetadata") or {}
        components = meta.get("components") or []
        widgets: list[WidgetState] = []
        for i, c in enumerate(components):
            widgets.append(analyze_widget(dash_id, dash_label, i + 1, c, inst, token))

        dash_dict = {
            "id": dash_id,
            "label": dash_label,
            "dashboardType": meta.get("dashboardType"),
            "canChangeRunningUser": meta.get("canChangeRunningUser"),
            "runningUser": (meta.get("runningUser") or {}).get("displayName"),
            "filters_count": len(meta.get("filters") or []),
            "components_count": len(components),
            "widgets": [asdict(w) for w in widgets],
        }
        # Executive widget count warnings — design choice, not data defect.
        # Per Stephen Few / Tufte / Gartner 2023, executive review surfaces
        # should carry 6-9 widgets. Widgets above that erode decision speed.
        warnings: list[str] = []
        widget_count = len(widgets)
        if dash_id in EXECUTIVE_DASHBOARDS:
            if widget_count > EXECUTIVE_WIDGET_CEILING_MAX:
                warnings.append(
                    f"widget-count-over-max:{widget_count}/{EXECUTIVE_WIDGET_CEILING_MAX}"
                )
            elif widget_count > EXECUTIVE_WIDGET_CEILING_TARGET:
                warnings.append(
                    f"widget-count-over-target:{widget_count}/{EXECUTIVE_WIDGET_CEILING_TARGET}"
                )
        dash_dict["warnings"] = warnings
        out["dashboards"].append(dash_dict)

        # Summary per dashboard
        active_flags = sum(len(w.flags) for w in widgets)
        deferred_flags = sum(len(w.flags_deferred) for w in widgets)
        flagged_widgets = sum(1 for w in widgets if w.flags)
        out["summary"][dash_label] = {
            "widgets": widget_count,
            "flagged_widgets": flagged_widgets,
            "active_flags": active_flags,
            "deferred_flags": deferred_flags,
            "warnings": warnings,
        }

    return out


def to_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Dashboard State Dump")
    lines.append("")
    lines.append(f"Generated: {report['generated_at']}")
    lines.append("")
    lines.append(
        "Pristine-state audit. Cross-references every widget binding with its source "
        "report. Flags prefixed with `⚠️` are active defects; those prefixed with "
        "`🔶` are deferred (schema change required); dashboard-level warnings "
        "prefixed with `💡` are design-choice flags (e.g. executive widget-count ceiling)."
    )
    lines.append("")

    for dash in report["dashboards"]:
        lines.append(f"## {dash['label']} (`{dash['id']}`)")
        lines.append("")
        lines.append(f"- **dashboardType:** {dash['dashboardType']}")
        lines.append(f"- **canChangeRunningUser:** {dash['canChangeRunningUser']}")
        lines.append(f"- **runningUser:** {dash['runningUser']}")
        lines.append(f"- **filters:** {dash['filters_count']}")
        lines.append(f"- **components:** {dash['components_count']} / 20")
        for w in dash.get("warnings", []):
            lines.append(f"- 💡 **warning:** {w}")
        lines.append("")
        lines.append("| # | Widget | Report | Format | Date | Widget Agg | Flags |")
        lines.append("|---|---|---|---|---|---|---|")
        for w in dash["widgets"]:
            flags_parts = []
            for f in w.get("flags", []):
                flags_parts.append(f"⚠️ {f}")
            for f in w.get("flags_deferred", []):
                flags_parts.append(f"🔶 {f}")
            flags_str = " ".join(flags_parts) if flags_parts else "—"
            widget_agg = ",".join(
                a.replace("s!Opportunity.", "").replace("s!", "")
                for a in (w.get("widget_aggregates") or [])
            )
            lines.append(
                f"| {w['index']} | {short(w['header'], 40)} | `{w['report_id']}` | "
                f"{w.get('report_format') or '?'} | {w.get('standard_date_filter') or '?'} | "
                f"{short(widget_agg, 38)} | {flags_str} |"
            )
        lines.append("")

    lines.append("## Summary")
    lines.append("")
    for label, s in report["summary"].items():
        warning_count = len(s.get("warnings") or [])
        warn_str = f", {warning_count} warning(s)" if warning_count else ""
        lines.append(
            f"- **{label}:** {s['widgets']} widgets, "
            f"{s['flagged_widgets']} flagged, "
            f"{s['active_flags']} active flag(s), "
            f"{s['deferred_flags']} deferred flag(s)"
            f"{warn_str}"
        )
    lines.append("")

    total_active = sum(s["active_flags"] for s in report["summary"].values())
    total_deferred = sum(s["deferred_flags"] for s in report["summary"].values())
    total_warnings = sum(
        len(s.get("warnings") or []) for s in report["summary"].values()
    )
    if total_active == 0:
        warn_suffix = f", {total_warnings} design warning(s)" if total_warnings else ""
        lines.append(
            f"✓ **Pristine state: 0 active flags, {total_deferred} deferred flags{warn_suffix}.**"
        )
    else:
        lines.append(
            f"⚠️ **{total_active} active flag(s) still drifted, {total_deferred} deferred.**"
        )
    return "\n".join(lines) + "\n"


def to_summary(report: dict[str, Any]) -> str:
    lines: list[str] = []
    for label, s in report["summary"].items():
        warning_count = len(s.get("warnings") or [])
        warn_str = f", {warning_count} warning(s)" if warning_count else ""
        lines.append(
            f"{label}: {s['widgets']} widgets, "
            f"{s['active_flags']} active, "
            f"{s['deferred_flags']} deferred"
            f"{warn_str}"
        )

    total_active = sum(s["active_flags"] for s in report["summary"].values())
    total_deferred = sum(s["deferred_flags"] for s in report["summary"].values())
    total_warnings = sum(
        len(s.get("warnings") or []) for s in report["summary"].values()
    )
    if total_active == 0:
        warn_suffix = f", {total_warnings} design warning(s)" if total_warnings else ""
        lines.append(
            f"✓ Pristine state: 0 active flags, {total_deferred} deferred flags{warn_suffix}."
        )
    else:
        lines.append(
            f"⚠️ {total_active} active flag(s) still drifted, {total_deferred} deferred."
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Dashboard state dump — pristine-state audit for Sales Director Monthly + Sales Ops Quarterly"
    )
    ap.add_argument(
        "--format",
        choices=["markdown", "json", "both"],
        default="both",
        help="Output format (default: both; stdout is markdown unless --format json is used)",
    )
    ap.add_argument(
        "--summary-only",
        action="store_true",
        help="Print only the per-dashboard counts and final pristine/drift verdict",
    )
    ap.add_argument(
        "--dashboard",
        action="append",
        help="Dashboard id to audit (repeatable). Default: both production dashboards.",
    )
    ap.add_argument(
        "--target-org",
        default=TARGET_ORG_DEFAULT,
        help=f"Salesforce org username/alias to query (default: {TARGET_ORG_DEFAULT})",
    )
    ap.add_argument("--out-md", help="Write markdown to this path instead of stdout")
    ap.add_argument("--out-json", help="Write JSON to this path instead of stdout")
    ap.add_argument(
        "--fail-if-drifted",
        action="store_true",
        help="Exit nonzero if any active (non-deferred) flag is present",
    )
    args = ap.parse_args()

    if args.dashboard:
        dashboards = [(d, d) for d in args.dashboard]
    else:
        dashboards = DEFAULT_DASHBOARDS

    inst, token = auth(args.target_org)
    report = build_report(dashboards, inst, token)

    json_str = json.dumps(report, indent=2)
    md_str = to_markdown(report)
    summary_str = to_summary(report)

    summary_only_stdout = args.summary_only and not args.out_md and not args.out_json
    if summary_only_stdout:
        print(summary_str, end="")

    if not summary_only_stdout and args.format in ("markdown", "both"):
        if args.out_md:
            with open(args.out_md, "w") as f:
                f.write(md_str)
            print(f"wrote {args.out_md}", file=sys.stderr)
        else:
            print(md_str)
    if not summary_only_stdout and args.format in ("json", "both"):
        if args.out_json:
            with open(args.out_json, "w") as f:
                f.write(json_str)
            print(f"wrote {args.out_json}", file=sys.stderr)
        elif args.format == "json":
            print(json_str)

    total_active = sum(s["active_flags"] for s in report["summary"].values())
    if args.fail_if_drifted and total_active > 0:
        print(
            f"FAIL: {total_active} active flag(s) present — dashboard has drifted from pristine state",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
