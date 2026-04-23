#!/usr/bin/env python3
"""Repair incomplete columnMap objects on legacy Engagement History dashboards."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm_analytics_helpers import deploy_dashboard, get_auth, get_dashboard_id, get_dashboard_state  # noqa: E402


TARGETS: dict[str, list[str]] = {
    "Account Detail - Engagement History": ["chart_1", "chart_3"],
    "Campaign Detail - Engagement History": ["chart_2"],
    "Contact Detail - Engagement History": ["chart_1", "chart_2"],
    "Opportunity Detail - Engagement History": ["chart_3"],
}


def _normalize_column_map(widget: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    params = widget.setdefault("parameters", {})
    original = params.get("columnMap") or {}
    if not isinstance(original, dict):
        original = {}
    dimension_axis = original.get("dimensionAxis")
    if dimension_axis is None:
        dimension_axis = original.get("dimension", [])
    normalized = {
        "dimensionAxis": list(dimension_axis or []),
        "plots": list(original.get("plots") or []),
        "trellis": list(original.get("trellis") or []),
        "split": list(original.get("split") or []),
    }
    return original, normalized


def _strip_url_keys(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_url_keys(item)
            for key, item in value.items()
            if key != "url"
        }
    if isinstance(value, list):
        return [_strip_url_keys(item) for item in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Apply the live fixes.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/legacy_engagement_history_columnmaps.json"),
        help="Path to write the JSON repair report.",
    )
    args = parser.parse_args()

    inst, tok = get_auth()
    report: dict[str, Any] = {
        "target_org": inst,
        "execute": args.execute,
        "dashboards": [],
    }

    for dashboard_label, widget_names in TARGETS.items():
        dashboard_id = get_dashboard_id(inst, tok, dashboard_label)
        if not dashboard_id:
            raise RuntimeError(f"dashboard not found: {dashboard_label}")
        state = get_dashboard_state(inst, tok, dashboard_id)["state"]
        dashboard_changes: list[dict[str, Any]] = []
        changed = False

        for widget_name in widget_names:
            widget = state["widgets"][widget_name]
            before, after = _normalize_column_map(widget)
            needs_change = before != after
            dashboard_changes.append(
                {
                    "widget": widget_name,
                    "visualizationType": widget.get("parameters", {}).get("visualizationType"),
                    "before": before,
                    "after": after,
                    "changed": needs_change,
                }
            )
            if needs_change:
                widget["parameters"]["columnMap"] = after
                changed = True

        if args.execute and changed:
            try:
                state = _strip_url_keys(state)
                deploy_dashboard(inst, tok, dashboard_id, state)
                refreshed = get_dashboard_state(inst, tok, dashboard_id)["state"]
                for item in dashboard_changes:
                    item["live_after"] = refreshed["widgets"][item["widget"]]["parameters"].get("columnMap")
            except Exception as exc:
                for item in dashboard_changes:
                    item["deploy_error"] = str(exc)

        report["dashboards"].append(
            {
                "dashboard": dashboard_label,
                "dashboard_id": dashboard_id,
                "changed": changed,
                "widgets": dashboard_changes,
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
