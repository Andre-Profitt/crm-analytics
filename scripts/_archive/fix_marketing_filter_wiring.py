#!/usr/bin/env python3
"""Review and repair misclassified marketing filter widgets.

The 2026-03-24 dashboard audit flagged six "unbound" filters across four
legacy marketing dashboards. Live inspection showed that five of those six
controls are actually binding-driven selectors, not dead filters. The one real
issue is the Account-Based Marketing account selector: its backing SAQL step
emits `AccountId.Name` but does not broadcast facets, so the selector never
filters the dashboard.

This script:
1. Re-audits the six flagged widgets.
2. Classifies each widget as binding-driven, facet-driven, or dead.
3. Optionally fixes the ABM selector by enabling `broadcastFacet` on its step.
"""

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


FILTER_TARGETS: dict[str, list[str]] = {
    "Account-Based Marketing": ["listselector_3"],
    "Engagement": [
        "EmailGraphButtonGroup",
        "EmailTemplateGraphToggleGroup",
        "FormsGraphMetricToggleGroup",
    ],
    "Multi-Touch Attribution": ["cimta_influence_model_listselector"],
    "Opportunity Detail - Engagement History": ["listselector_3"],
}

ABM_FIX_TARGET = ("Account-Based Marketing", "listselector_3")


def _step_reference_names(state: dict[str, Any], step_name: str) -> list[str]:
    refs: list[str] = []
    for candidate_name, candidate in state.get("steps", {}).items():
        if candidate_name == step_name:
            continue
        blob = json.dumps(candidate, sort_keys=True)
        if step_name in blob:
            refs.append(candidate_name)
    return sorted(refs)


def _classify_widget(state: dict[str, Any], widget_name: str) -> dict[str, Any]:
    widget = state["widgets"][widget_name]
    parameters = widget.get("parameters", {})
    step_name = parameters.get("step")
    step = state.get("steps", {}).get(step_name, {})
    refs = _step_reference_names(state, step_name) if isinstance(step_name, str) else []
    classification = "dead"
    reason = "no downstream bindings and no facet broadcast"
    if refs:
        classification = "binding_driven"
        reason = f"referenced by downstream steps: {', '.join(refs)}"
    elif step.get("broadcastFacet") is True:
        classification = "facet_driven"
        reason = "selector step broadcasts facets"
    return {
        "widget": widget_name,
        "widget_type": widget.get("type"),
        "step": step_name,
        "step_type": step.get("type"),
        "broadcastFacet": step.get("broadcastFacet"),
        "classification": classification,
        "reason": reason,
        "step_refs": refs,
    }


def _review_dashboard(inst: str, tok: str, dashboard_label: str) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    dashboard_id = get_dashboard_id(inst, tok, dashboard_label)
    if not dashboard_id:
        raise RuntimeError(f"dashboard not found: {dashboard_label}")
    dashboard = get_dashboard_state(inst, tok, dashboard_id)
    state = dashboard.get("state", {})
    if not isinstance(state, dict):
        raise RuntimeError(f"dashboard state unavailable: {dashboard_label}")
    widgets = [_classify_widget(state, widget_name) for widget_name in FILTER_TARGETS[dashboard_label]]
    return dashboard_id, state, widgets


def _apply_abm_fix(state: dict[str, Any], widget_name: str) -> bool:
    widget = state["widgets"][widget_name]
    step_name = widget.get("parameters", {}).get("step")
    if not isinstance(step_name, str):
        raise RuntimeError(f"{widget_name}: missing backing step")
    step = state["steps"][step_name]
    if step.get("broadcastFacet") is True:
        return False
    step["broadcastFacet"] = True
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the ABM fix live if the selector is still classified as dead.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/marketing_filter_wiring_review.json"),
        help="Path to write the JSON review report.",
    )
    args = parser.parse_args()

    inst, tok = get_auth()
    report: dict[str, Any] = {
        "target_org": inst,
        "execute": args.execute,
        "dashboards": [],
        "actions": [],
    }

    abm_fix_needed = False
    abm_dashboard_id = None
    abm_state = None

    for dashboard_label in FILTER_TARGETS:
        dashboard_id, state, widgets = _review_dashboard(inst, tok, dashboard_label)
        report["dashboards"].append(
            {
                "dashboard": dashboard_label,
                "dashboard_id": dashboard_id,
                "widgets": widgets,
            }
        )
        if (dashboard_label, ABM_FIX_TARGET[1]) == ABM_FIX_TARGET:
            for item in widgets:
                if item["widget"] == ABM_FIX_TARGET[1] and item["classification"] == "dead":
                    abm_fix_needed = True
                    abm_dashboard_id = dashboard_id
                    abm_state = state

    if args.execute and abm_fix_needed and abm_dashboard_id and isinstance(abm_state, dict):
        changed = _apply_abm_fix(abm_state, ABM_FIX_TARGET[1])
        if changed:
            deploy_dashboard(inst, tok, abm_dashboard_id, abm_state)
            report["actions"].append(
                {
                    "dashboard": ABM_FIX_TARGET[0],
                    "widget": ABM_FIX_TARGET[1],
                    "action": "enabled_broadcastFacet",
                }
            )
        # Re-read the dashboard after patch to capture final state.
        _, _, refreshed = _review_dashboard(inst, tok, ABM_FIX_TARGET[0])
        for dashboard in report["dashboards"]:
            if dashboard["dashboard"] == ABM_FIX_TARGET[0]:
                dashboard["widgets"] = refreshed
                break
    elif args.execute:
        report["actions"].append(
            {
                "dashboard": ABM_FIX_TARGET[0],
                "widget": ABM_FIX_TARGET[1],
                "action": "no_change_needed",
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
