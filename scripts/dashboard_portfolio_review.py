#!/usr/bin/env python3
"""Generate a page-by-page, widget-by-widget audit of deployed CRM Analytics dashboards."""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm_analytics_helpers import _sf_api, get_auth  # noqa: E402

API_VERSION = "v66.0"

DASHBOARDS = [
    {
        "label": "Account Intelligence KPIs",
        "id": "0FKTb0000000HYjOAM",
        "target_surface": "Customer & Account Health",
        "audience": "manager",
    },
    {
        "label": "Advanced Pipeline Analytics",
        "id": "0FKTb0000000HnFOAU",
        "target_surface": "Pipeline & Opportunity Operations",
        "audience": "manager",
    },
    {
        "label": "Contract Operations KPIs",
        "id": "0FKTb0000000HX7OAM",
        "target_surface": "Contract Operations & Renewals",
        "audience": "manager",
    },
    {
        "label": "Forecast Intelligence",
        "id": "0FKTb0000000HaLOAU",
        "target_surface": "Forecast & Revenue Motions",
        "audience": "manager",
    },
    {
        "label": "Lead Management KPIs",
        "id": "0FKTb0000000HVVOA2",
        "target_surface": "Lead Funnel",
        "audience": "manager",
    },
    {
        "label": "Opp Management",
        "id": "0FKTb0000000HfBOAU",
        "target_surface": "Pipeline & Opportunity Operations",
        "audience": "manager",
    },
    {
        "label": "Pipeline History",
        "id": "0FKTb0000000HbxOAE",
        "target_surface": "Pipeline & Opportunity Operations",
        "audience": "manager",
    },
    {
        "label": "Revenue Motions KPIs",
        "id": "0FKTb0000000HTtOAM",
        "target_surface": "Forecast & Revenue Motions",
        "audience": "manager",
    },
    {
        "label": "Sales Process Compliance KPIs",
        "id": "0FKTb0000000HSHOA2",
        "target_surface": "Pipeline & Opportunity Operations",
        "audience": "manager",
    },
    {
        "label": "Customer Intelligence",
        "id": "0FKTb0000000HdZOAU",
        "target_surface": "Customer & Account Health",
        "audience": "manager",
    },
]

ADVANCED_SAQL_PATTERNS = {
    "timeseries": r"\btimeseries\b",
    "arimax": r"\barimax\b",
    "cogroup": r"\bcogroup\b",
    "union": r"\bunion\b",
    "window": r"\bwindow\b|\brank\s*\(",
    "fill": r"\bfill\b",
    "bindings": r"\{\{",
}

LOW_SIGNAL_VIZ = {"gauge", "donut", "pie"}
TABLE_VIZ = {"comparisontable", "comparisontable"}
ADVANCED_EXPLORATORY_VIZ = {
    "bubble",
    "choropleth",
    "heatmap",
    "sankey",
    "scatter",
    "treemap",
}
TREND_VIZ = {"area", "combo", "line", "stackarea", "timeline"}
BAR_VIZ = {"column", "hbar", "stackcolumn", "stackhbar", "stackvbar", "vbar"}

SUMMARY_LABELS = {
    "active book",
    "data quality",
    "executive overview",
    "executive summary",
    "forecast overview",
    "mql funnel",
    "pipeline history",
    "portfolio",
}
TREND_LABEL_HINTS = (
    "trend",
    "forecast",
    "history",
    "quota",
    "renewal",
    "conversion",
    "churn",
    "time",
)
ACTION_LABEL_HINTS = (
    "activity",
    "bottleneck",
    "contracts",
    "deal push",
    "engagement",
    "fulfillment",
    "health & risk",
    "rep detail",
    "stuck",
    "past-due",
    "win probability",
)
DRIVER_LABEL_HINTS = (
    "channel",
    "cross",
    "geographic",
    "mix",
    "product",
    "segment",
    "segmentation",
    "adoption",
    "expansion",
)
ANALYST_LABEL_HINTS = ("advanced analytics", "quant intelligence", "statistical analysis")


@dataclass
class WidgetReview:
    dashboard_label: str
    dashboard_id: str
    page_index: int
    page_label: str
    page_target: str
    page_grade: int
    widget_name: str
    widget_type: str
    viz: str
    title: str
    step: str
    step_features: str
    action: str
    rationale: str
    row: int
    column: int
    row_span: int
    column_span: int


def _fetch_dashboard(inst: str, tok: str, dashboard_id: str) -> dict[str, Any]:
    return _sf_api(inst, tok, "GET", f"/services/data/{API_VERSION}/wave/dashboards/{dashboard_id}")


def _plain_text(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("label", "text", "title"):
            if key in value and value[key]:
                return _plain_text(value[key])
        return ""
    if not isinstance(value, str):
        return ""
    text = html.unescape(value)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _widget_title(widget: dict[str, Any]) -> str:
    params = widget.get("parameters", {})
    for key in ("title", "text", "label"):
        text = _plain_text(params.get(key))
        if text:
            return text
    return ""


def _normalize_viz(widget: dict[str, Any]) -> str:
    viz = widget.get("parameters", {}).get("visualizationType", "")
    if not viz:
        return ""
    return str(viz).strip().lower()


def _step_features(step: dict[str, Any]) -> list[str]:
    query = step.get("query", "")
    if isinstance(query, dict):
        query = json.dumps(query, sort_keys=True)
    query = html.unescape(query if isinstance(query, str) else "")
    return [name for name, pattern in ADVANCED_SAQL_PATTERNS.items() if re.search(pattern, query, re.IGNORECASE | re.DOTALL)]


def _page_target(page_label: str) -> str:
    label = page_label.lower()
    if any(hint in label for hint in ANALYST_LABEL_HINTS):
        return "Analyst Lab"
    if label in SUMMARY_LABELS:
        return "Summary"
    if any(hint in label for hint in ACTION_LABEL_HINTS):
        return "Exceptions/Actions"
    if any(hint in label for hint in DRIVER_LABEL_HINTS):
        return "Drivers/Segments"
    if any(hint in label for hint in TREND_LABEL_HINTS):
        return "Trend/Forecast"
    return "Summary"


def _is_action_table(title: str) -> bool:
    lower = title.lower()
    return any(
        hint in lower
        for hint in (
            "at-risk",
            "detail",
            "exception",
            "high-score",
            "hot ",
            "lost",
            "no activity",
            "past-due",
            "renewal",
            "risk",
            "stuck",
            "top 25",
            "top 50",
        )
    )


def _widget_action(widget_type: str, viz: str, title: str, page_target: str) -> tuple[str, str]:
    analyst_page = page_target == "Analyst Lab"

    if widget_type == "link":
        return (
            "remove",
            "Repeated page navigation burns layout space; collapse to a compact dashboard header or reduce the page count.",
        )
    if widget_type in {"listselector", "pillbox"}:
        return (
            "consolidate",
            "Keep the filter once as a persistent dashboard bar; duplicated page-level selectors add friction without new insight.",
        )
    if widget_type == "text":
        return (
            "compress",
            "Use fewer structural text widgets; convert repeated section banners into lighter headers or remove them entirely.",
        )
    if widget_type == "number":
        return (
            "keep_upgrade",
            "Keep only as a top-line KPI tile and add target, forecast, variance, and drill-through context.",
        )
    if widget_type != "chart":
        return ("review", "Non-standard widget type; validate whether it still earns dashboard space.")

    if viz in LOW_SIGNAL_VIZ:
        return (
            "replace",
            "Low-information visual. Replace with a bullet, ranked bar, or trend + variance view that supports exact comparison.",
        )
    if viz in TABLE_VIZ:
        if _is_action_table(title):
            return (
                "keep_as_action_table",
                "Useful as an exception/action table. Add conditional formatting, next-best-action fields, and Salesforce record links.",
            )
        return (
            "replace",
            "Summary compare tables are dense and static. Replace with ranked bars, distributions, or compact KPI summaries.",
        )
    if viz == "bullet":
        return (
            "keep_upgrade",
            "Strong target-vs-actual widget. Add dynamic targets, peer banding, or variance labels to make it operational.",
        )
    if viz == "funnel":
        return (
            "keep_upgrade",
            "Useful for stage-dropoff analysis if paired with conversion rates, dwell time, and a clear exception drill path.",
        )
    if viz == "waterfall":
        return (
            "keep_upgrade",
            "Keep only when explaining variance or movement. Add labeled drivers and reference lines for target or prior-period comparison.",
        )
    if viz in TREND_VIZ:
        return (
            "keep_upgrade",
            "Good base for trend/forecast analysis. Add prediction bands, scenario switches, dynamic reference lines, and annotations.",
        )
    if viz in BAR_VIZ:
        return (
            "keep_upgrade",
            "Keep if the chart is sorted, labeled, and tied to an explicit segment, ranking, or operational action question.",
        )
    if viz in ADVANCED_EXPLORATORY_VIZ:
        if analyst_page:
            return (
                "keep_for_analyst",
                "Exploratory advanced visual. Keep only if it answers a precise analytical question and supports drill or parameter changes.",
            )
        return (
            "move_to_analyst",
            "Advanced visual is likely too exploratory for a manager page; move it to an analyst workbench or replace it with a simpler decision view.",
        )
    if viz == "combo":
        return (
            "keep_upgrade",
            "Keep only when mixing bars and lines materially improves interpretation; otherwise split the metrics into cleaner visuals.",
        )
    if viz == "timeline":
        return (
            "keep_upgrade",
            "Use this as the default forecast canvas. Add prediction intervals, targets, pacing overlays, and horizon controls.",
        )
    return ("review", "Validate this visual against the target audience and replace it if it does not answer a concrete decision question.")


def _page_grade(total_widgets: int, nav_widgets: int, selector_widgets: int, low_signal_count: int, action_tables: int, target: str) -> int:
    grade = 5
    if total_widgets > 15:
        grade -= 1
    if total_widgets > 22:
        grade -= 1
    if nav_widgets >= 5:
        grade -= 1
    if selector_widgets >= 3:
        grade -= 1
    if low_signal_count >= 3:
        grade -= 1
    if target == "Exceptions/Actions" and action_tables == 0:
        grade -= 1
    return max(1, grade)


def _dashboard_priority(summary: dict[str, Any]) -> str:
    score = 0.0
    score += max(0, summary["pages"] - 4) * 1.2
    score += summary["nav_ratio"]
    score += summary["low_signal_widgets"] / 10.0
    score += summary["analyst_pages"] * 1.4
    if summary["forecast_pages"] == 0:
        score += 2.0
    if summary["widgets"] >= 150:
        score += 1.0
    if score >= 9:
        return "High"
    if score >= 6:
        return "Medium"
    return "Lower"


def _page_assessment(page_label: str, target: str, counts: Counter[str], low_signal_count: int, grade: int, action_tables: int, features: set[str]) -> str:
    issues: list[str] = []
    total_widgets = sum(counts.values())
    if counts["link"] >= 5:
        issues.append(f"{counts['link']} repeated nav links")
    if counts["listselector"] + counts["pillbox"] >= 2:
        issues.append(f"{counts['listselector'] + counts['pillbox']} duplicated selectors")
    if total_widgets > 15:
        issues.append(f"{total_widgets} widgets exceeds the target density")
    if low_signal_count:
        issues.append(f"{low_signal_count} low-signal charts")
    if target == "Trend/Forecast" and "timeseries" not in features and "arimax" not in features and "timeline" not in features:
        issues.append("no native forecast mechanics in the backing steps")
    if target == "Exceptions/Actions" and action_tables == 0:
        issues.append("no clear action table")
    if not issues:
        issues.append("shape is workable but still needs better analytical framing")
    return (
        f"Target this page as `{target}`. Current grade: {grade}/5. "
        f"Main issues: {', '.join(issues)}."
    )


def _write_inventory(rows: list[WidgetReview], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dashboard_label",
                "dashboard_id",
                "page_index",
                "page_label",
                "page_target",
                "page_grade",
                "widget_name",
                "widget_type",
                "visualization_type",
                "title",
                "step",
                "step_features",
                "action",
                "rationale",
                "row",
                "column",
                "row_span",
                "column_span",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.dashboard_label,
                    row.dashboard_id,
                    row.page_index,
                    row.page_label,
                    row.page_target,
                    row.page_grade,
                    row.widget_name,
                    row.widget_type,
                    row.viz,
                    row.title,
                    row.step,
                    row.step_features,
                    row.action,
                    row.rationale,
                    row.row,
                    row.column,
                    row.row_span,
                    row.column_span,
                ]
            )


def _write_markdown(
    dashboard_summaries: list[dict[str, Any]],
    page_reviews: list[dict[str, Any]],
    widget_rows: list[WidgetReview],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_by_page: dict[tuple[str, int], list[WidgetReview]] = {}
    for row in widget_rows:
        rows_by_page.setdefault((row.dashboard_label, row.page_index), []).append(row)

    lines = [
        "# Deployed CRM Analytics Portfolio Review",
        "",
        f"Generated on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.",
        "",
        "This audit is based on the deployed dashboard JSON in Salesforce, not only on the local builder scripts.",
        "",
        "## Portfolio Summary",
        "",
        "| Dashboard | Target Surface | Pages | Widgets | Nav/Insight Ratio | Low-Signal Widgets | Analyst Pages | Forecast-Capable Pages | Priority |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for summary in dashboard_summaries:
        lines.append(
            "| {label} | {target_surface} | {pages} | {widgets} | {nav_ratio:.2f} | {low_signal_widgets} | {analyst_pages} | {forecast_pages} | {priority} |".format(
                **summary
            )
        )

    lines.extend(
        [
            "",
            "## Global Findings",
            "",
            "- Repeated page navigation and repeated selectors consume a large share of the canvas across nearly every dashboard.",
            "- Gauges, donuts, and summary compare tables are overused relative to trend, variance, and predictive views.",
            "- Advanced/Statistical pages do contain some valuable visuals, but they sit inside manager dashboards where they add sprawl instead of focused decision support.",
            "- Forecast-oriented pages rarely use native timeseries features or explicit prediction intervals, even where the business question is inherently forward-looking.",
        ]
    )

    for summary in dashboard_summaries:
        lines.extend(
            [
                "",
                f"## {summary['label']}",
                "",
                f"- Current target surface: `{summary['target_surface']}`",
                f"- Current shape: {summary['pages']} pages, {summary['widgets']} widgets, nav/insight ratio {summary['nav_ratio']:.2f}",
                f"- Review priority: `{summary['priority']}`",
                f"- Dominant issues: {summary['issue_summary']}",
            ]
        )

        for page in [p for p in page_reviews if p["dashboard_label"] == summary["label"]]:
            lines.extend(
                [
                    "",
                    f"### Page {page['page_index']}: {page['page_label']}",
                    "",
                    f"- {page['assessment']}",
                    f"- Current shape: {page['total_widgets']} widgets ({page['counts_summary']})",
                    f"- Step features detected: {page['feature_summary']}",
                    "",
                    "| Widget | Type | Action | Why |",
                    "|---|---|---|---|",
                ]
            )
            for row in rows_by_page[(summary["label"], page["page_index"])]:
                widget_label = row.title or row.widget_name
                kind = row.widget_type if not row.viz else f"{row.widget_type}/{row.viz}"
                lines.append(
                    f"| {widget_label} | {kind} | {row.action} | {row.rationale} |"
                )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "docs" / "generated"),
        help="Directory for generated review artifacts.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    inst, tok = get_auth()

    widget_rows: list[WidgetReview] = []
    page_reviews: list[dict[str, Any]] = []
    dashboard_summaries: list[dict[str, Any]] = []

    for dashboard_meta in DASHBOARDS:
        dash = _fetch_dashboard(inst, tok, dashboard_meta["id"])
        state = dash.get("state", {})
        steps = state.get("steps", {})
        widgets = state.get("widgets", {})
        pages = state.get("gridLayouts", [{}])[0].get("pages", []) if state.get("gridLayouts") else []

        widget_counter: Counter[str] = Counter()
        low_signal_widgets = 0
        analyst_pages = 0
        forecast_pages = 0

        for page_index, page in enumerate(pages, start=1):
            page_label = _plain_text(page.get("label")) or f"Page {page_index}"
            page_target = _page_target(page_label)
            if page_target == "Analyst Lab":
                analyst_pages += 1

            page_widgets = sorted(
                page.get("widgets", []),
                key=lambda item: (item.get("row", 0), item.get("column", 0), item.get("name", "")),
            )
            page_counts: Counter[str] = Counter()
            page_feature_set: set[str] = set()
            page_low_signal_count = 0
            page_action_tables = 0
            page_has_forecast = False

            for placed_widget in page_widgets:
                widget_name = placed_widget.get("name", "")
                widget = widgets.get(widget_name, {})
                widget_type = widget.get("type", "unknown")
                viz = _normalize_viz(widget)
                title = _widget_title(widget)
                step_name = widget.get("parameters", {}).get("step", "")
                features = _step_features(steps.get(step_name, {})) if step_name else []
                page_feature_set.update(features)

                widget_counter[widget_type] += 1
                page_counts[widget_type] += 1

                if viz in LOW_SIGNAL_VIZ:
                    low_signal_widgets += 1
                    page_low_signal_count += 1
                if viz in TABLE_VIZ and _is_action_table(title):
                    page_action_tables += 1
                if viz == "timeline" or "timeseries" in features or "arimax" in features:
                    page_has_forecast = True

                action, rationale = _widget_action(widget_type, viz, title, page_target)
                widget_rows.append(
                    WidgetReview(
                        dashboard_label=dashboard_meta["label"],
                        dashboard_id=dashboard_meta["id"],
                        page_index=page_index,
                        page_label=page_label,
                        page_target=page_target,
                        page_grade=0,
                        widget_name=widget_name,
                        widget_type=widget_type,
                        viz=viz,
                        title=title,
                        step=step_name,
                        step_features=", ".join(features) if features else "-",
                        action=action,
                        rationale=rationale,
                        row=placed_widget.get("row", 0),
                        column=placed_widget.get("column", 0),
                        row_span=placed_widget.get("rowSpan", placed_widget.get("rowspan", 1)),
                        column_span=placed_widget.get("columnSpan", placed_widget.get("colspan", 1)),
                    )
                )

            page_grade = _page_grade(
                total_widgets=len(page_widgets),
                nav_widgets=page_counts["link"],
                selector_widgets=page_counts["listselector"] + page_counts["pillbox"],
                low_signal_count=page_low_signal_count,
                action_tables=page_action_tables,
                target=page_target,
            )

            for row in widget_rows:
                if (
                    row.dashboard_id == dashboard_meta["id"]
                    and row.page_index == page_index
                    and row.page_grade == 0
                ):
                    row.page_grade = page_grade

            page_reviews.append(
                {
                    "dashboard_label": dashboard_meta["label"],
                    "page_index": page_index,
                    "page_label": page_label,
                    "page_target": page_target,
                    "total_widgets": len(page_widgets),
                    "assessment": _page_assessment(
                        page_label,
                        page_target,
                        page_counts,
                        page_low_signal_count,
                        page_grade,
                        page_action_tables,
                        page_feature_set | ({"timeline"} if any(w.viz == "timeline" for w in widget_rows if w.dashboard_id == dashboard_meta["id"] and w.page_index == page_index) else set()),
                    ),
                    "counts_summary": ", ".join(
                        f"{kind}={count}"
                        for kind, count in (
                            ("link", page_counts["link"]),
                            ("selector", page_counts["listselector"] + page_counts["pillbox"]),
                            ("text", page_counts["text"]),
                            ("number", page_counts["number"]),
                            ("chart", page_counts["chart"]),
                        )
                        if count
                    ),
                    "feature_summary": ", ".join(sorted(page_feature_set)) if page_feature_set else "basic SAQL only",
                }
            )
            if page_has_forecast:
                forecast_pages += 1

        insight_widgets = widget_counter["chart"] + widget_counter["number"]
        nav_widgets = widget_counter["link"] + widget_counter["text"] + widget_counter["listselector"] + widget_counter["pillbox"]
        nav_ratio = round(nav_widgets / insight_widgets, 2) if insight_widgets else 0.0

        issue_parts = []
        if nav_ratio > 2:
            issue_parts.append("navigation and support widgets outweigh insight widgets")
        if low_signal_widgets:
            issue_parts.append(f"{low_signal_widgets} gauges/donuts to reconsider")
        if analyst_pages:
            issue_parts.append(f"{analyst_pages} pages belong in analyst workbenches")
        if not issue_parts:
            issue_parts.append("shape is healthier than the portfolio average")

        dashboard_summary = {
            "label": dashboard_meta["label"],
            "dashboard_id": dashboard_meta["id"],
            "target_surface": dashboard_meta["target_surface"],
            "pages": len(pages),
            "widgets": len(widgets),
            "nav_ratio": nav_ratio,
            "low_signal_widgets": low_signal_widgets,
            "analyst_pages": analyst_pages,
            "forecast_pages": min(forecast_pages, len(pages)),
            "issue_summary": "; ".join(issue_parts),
        }
        dashboard_summary["priority"] = _dashboard_priority(dashboard_summary)
        dashboard_summaries.append(dashboard_summary)

    dashboard_summaries.sort(key=lambda item: ("High", "Medium", "Lower").index(item["priority"]))
    page_reviews.sort(key=lambda item: (item["dashboard_label"], item["page_index"]))
    widget_rows.sort(key=lambda item: (item.dashboard_label, item.page_index, item.row, item.column, item.widget_name))

    _write_inventory(widget_rows, output_dir / "deployed_dashboard_widget_inventory.csv")
    _write_markdown(
        dashboard_summaries=dashboard_summaries,
        page_reviews=page_reviews,
        widget_rows=widget_rows,
        path=output_dir / "deployed_dashboard_review.md",
    )

    summary = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dashboards": len(DASHBOARDS),
        "pages": len(page_reviews),
        "widgets": len(widget_rows),
        "output_dir": str(output_dir),
    }
    (output_dir / "deployed_dashboard_review_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
