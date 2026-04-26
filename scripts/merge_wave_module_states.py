#!/usr/bin/env python3
"""Merge exact Wave page-module states into one combined dashboard state."""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm_analytics_helpers import (
    find_dashboard_patch_contract_violations,
    normalize_dashboard_state_for_patch,
)


def load_dashboard_state(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    state = payload.get("state")
    if isinstance(state, dict):
        payload = state
    return normalize_dashboard_state_for_patch(payload)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _merge_list_dedup(base: list[Any], incoming: list[Any]) -> list[Any]:
    merged = list(base)
    seen = {_canonical_json(item) for item in base}
    for item in incoming:
        key = _canonical_json(item)
        if key in seen:
            continue
        merged.append(copy.deepcopy(item))
        seen.add(key)
    return merged


def merge_dashboard_states(states: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    if not states:
        raise ValueError("at least one dashboard state is required")

    merged: dict[str, Any] = {
        "filters": [],
        "gridLayouts": [{"pages": []}],
        "widgets": {},
        "steps": {},
        "widgetStyle": {},
    }
    warnings: list[str] = []

    primary_grid = merged["gridLayouts"][0]
    merged_page_names: set[str] = set()

    for index, state in enumerate(states):
        grid_layouts = state.get("gridLayouts") or []
        source_grid = grid_layouts[0] if grid_layouts and isinstance(grid_layouts[0], dict) else {}

        incoming_filters = state.get("filters")
        if isinstance(incoming_filters, list):
            merged["filters"] = _merge_list_dedup(merged.get("filters") or [], incoming_filters)

        incoming_widget_style = state.get("widgetStyle")
        if isinstance(incoming_widget_style, dict) and incoming_widget_style:
            current_style = merged.get("widgetStyle") or {}
            if not current_style:
                merged["widgetStyle"] = copy.deepcopy(incoming_widget_style)
            elif _canonical_json(current_style) != _canonical_json(incoming_widget_style):
                warnings.append(
                    f"state[{index}] widgetStyle differs from the first state; keeping the first widgetStyle contract."
                )

        incoming_grid_name = source_grid.get("name")
        if isinstance(incoming_grid_name, str) and incoming_grid_name and not primary_grid.get("name"):
            primary_grid["name"] = incoming_grid_name

        incoming_grid_style = source_grid.get("style")
        if isinstance(incoming_grid_style, dict) and incoming_grid_style:
            current_style = primary_grid.get("style") or {}
            if not current_style:
                primary_grid["style"] = copy.deepcopy(incoming_grid_style)
            elif _canonical_json(current_style) != _canonical_json(incoming_grid_style):
                warnings.append(
                    f"state[{index}] gridLayouts[0].style differs from the first state; keeping the first grid style."
                )

        for page in source_grid.get("pages") or []:
            if not isinstance(page, dict):
                continue
            page_name = page.get("name")
            if not isinstance(page_name, str) or not page_name:
                raise ValueError(f"state[{index}] contains a page without a stable name")
            if page_name in merged_page_names:
                raise ValueError(f"duplicate page name while merging module states: {page_name}")
            merged_page_names.add(page_name)
            primary_grid["pages"].append(copy.deepcopy(page))

        for collection_name in ("widgets", "steps"):
            merged_collection = merged.setdefault(collection_name, {})
            for item_name, payload in (state.get(collection_name) or {}).items():
                if not isinstance(item_name, str) or not item_name:
                    continue
                if item_name not in merged_collection:
                    merged_collection[item_name] = copy.deepcopy(payload)
                    continue
                if _canonical_json(merged_collection[item_name]) != _canonical_json(payload):
                    raise ValueError(
                        f"conflicting {collection_name[:-1]} definition while merging module states: {item_name}"
                    )

    violations = find_dashboard_patch_contract_violations(merged)
    if violations:
        warnings.append(f"merged state contains {len(violations)} PATCH contract violation(s)")
    return merged, warnings


def write_outputs(
    *,
    output_dir: Path,
    merged_state: dict[str, Any],
    summary: dict[str, Any],
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / "dashboard_state.merged.json"
    summary_path = output_dir / "merge_summary.json"
    readme_path = output_dir / "README.md"

    state_path.write_text(json.dumps(merged_state, indent=2) + "\n", encoding="utf-8")
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    readme_lines = [
        "# Wave Module Merge",
        "",
        f"- Input states: `{summary['input_state_count']}`",
        f"- Pages: `{summary['page_count']}`",
        f"- Widgets: `{summary['widget_count']}`",
        f"- Steps: `{summary['step_count']}`",
        f"- Contract violations: `{summary['contract_violation_count']}`",
        "",
        "## Outputs",
        "",
        "- `dashboard_state.merged.json`",
        "- `merge_summary.json`",
    ]
    readme_path.write_text("\n".join(readme_lines) + "\n", encoding="utf-8")
    return {
        "merged_state": str(state_path),
        "summary": str(summary_path),
        "review": str(readme_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state",
        dest="states",
        action="append",
        required=True,
        help="Path to a normalized/materialized dashboard state JSON. Repeat for multiple page modules.",
    )
    parser.add_argument("--output-dir", required=True, help="Directory for merged outputs")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    args = parser.parse_args()

    state_paths = [Path(item) for item in args.states]
    states = [load_dashboard_state(path) for path in state_paths]
    merged_state, warnings = merge_dashboard_states(states)
    violations = find_dashboard_patch_contract_violations(merged_state)
    summary = {
        "artifact_type": "wave_module_merge_summary",
        "input_state_count": len(state_paths),
        "inputs": [str(path) for path in state_paths],
        "page_count": len((merged_state.get("gridLayouts") or [{}])[0].get("pages", []))
        if merged_state.get("gridLayouts")
        else 0,
        "widget_count": len(merged_state.get("widgets") or {}),
        "step_count": len(merged_state.get("steps") or {}),
        "contract_violation_count": len(violations),
        "warnings": warnings,
    }
    artifacts = write_outputs(output_dir=Path(args.output_dir), merged_state=merged_state, summary=summary)
    result = {
        "status": "ok" if not violations else "warn",
        "tool": "merge_wave_module_states",
        "summary": summary,
        "artifacts": artifacts,
        "contract_violations": violations,
    }
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"pages: {summary['page_count']}")
        print(f"widgets: {summary['widget_count']}")
        print(f"steps: {summary['step_count']}")
        print(f"contract_violations: {summary['contract_violation_count']}")
        print(f"merged_state: {artifacts['merged_state']}")
    return 0 if not violations else 1


if __name__ == "__main__":
    raise SystemExit(main())
