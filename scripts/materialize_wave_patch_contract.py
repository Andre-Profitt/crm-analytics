#!/usr/bin/env python3
"""Apply companion step-contract truth to compiled Wave patch artifacts."""

from __future__ import annotations

import argparse
import copy
import html
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def load_dashboard_state(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    state = payload.get("state")
    if isinstance(state, dict):
        return state
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _deep_merge(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _normalize_step_payload(step_payload: dict[str, Any]) -> dict[str, Any]:
    normalized = copy.deepcopy(step_payload)
    query = normalized.get("query")
    if isinstance(query, str):
        normalized["query"] = html.unescape(query)
    return normalized


def _load_source_exports(step_contract: dict[str, Any]) -> dict[str, dict[str, Any]]:
    exports: dict[str, dict[str, Any]] = {}
    for export_key, export_path in (step_contract.get("baseline_exports") or {}).items():
        if not isinstance(export_path, str) or not export_path:
            continue
        exports[export_key] = load_dashboard_state(ROOT / export_path)
    return exports


def _find_source_step(source_exports: dict[str, dict[str, Any]], step_alias: str) -> dict[str, Any]:
    for export in source_exports.values():
        steps = export.get("steps") or {}
        payload = steps.get(step_alias)
        if isinstance(payload, dict):
            return _normalize_step_payload(payload)
    raise KeyError(f"source step not found for alias {step_alias}")


def _find_source_widget(
    source_exports: dict[str, dict[str, Any]],
    *,
    widget_key: str,
    export_key: str | None = None,
) -> dict[str, Any]:
    if export_key:
        export = source_exports.get(export_key)
        if export is None:
            raise KeyError(f"source export not found for key {export_key}")
        widget = (export.get("widgets") or {}).get(widget_key)
        if isinstance(widget, dict):
            return copy.deepcopy(widget)
        raise KeyError(f"source widget not found for key {widget_key} in export {export_key}")

    for export in source_exports.values():
        widget = (export.get("widgets") or {}).get(widget_key)
        if isinstance(widget, dict):
            return copy.deepcopy(widget)
    raise KeyError(f"source widget not found for key {widget_key}")


def _prune_invalid_table_rules(widget_payload: dict[str, Any]) -> None:
    parameters = widget_payload.get("parameters")
    if not isinstance(parameters, dict):
        return
    if parameters.get("visualizationType") != "comparisontable":
        return
    parameters.pop("columnMap", None)
    columns = parameters.get("columns") or []
    if not isinstance(columns, list):
        columns = []
    rules = parameters.get("formatRules")
    if not isinstance(rules, list):
        return
    filtered_rules = [rule for rule in rules if isinstance(rule, dict) and rule.get("field") in columns]
    if filtered_rules:
        parameters["formatRules"] = filtered_rules
    else:
        parameters.pop("formatRules", None)


def _materialize_widget_from_template(
    *,
    template_widget: dict[str, Any],
    step_alias: str,
    visualization_type: str,
    title_label: str | None,
    columns: list[str] | None,
    parameter_overrides: dict[str, Any] | None,
) -> dict[str, Any]:
    widget_payload = copy.deepcopy(template_widget)
    parameters = widget_payload.setdefault("parameters", {})
    parameters["step"] = step_alias
    parameters["visualizationType"] = visualization_type
    if columns is not None:
        parameters["columns"] = columns
    if title_label is not None and isinstance(parameters.get("title"), dict):
        parameters["title"]["label"] = title_label
    if parameter_overrides:
        parameters = _deep_merge(parameters, parameter_overrides)
        widget_payload["parameters"] = parameters
    if visualization_type in {"funnel", "treemap", "waterfall"}:
        parameters["columnMap"] = None
    _prune_invalid_table_rules(widget_payload)
    return widget_payload


def _build_widget_payload(
    *,
    entry: dict[str, Any],
    source_exports: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    target_step_alias = entry["target_step_alias"]
    visualization_type = entry.get("visualization_type") or ""
    widget_contract = entry.get("widget_contract") or {}
    widget_type = widget_contract.get("widget_type")
    parameter_overrides = (
        widget_contract.get("parameter_overrides")
        if isinstance(widget_contract.get("parameter_overrides"), dict)
        else None
    )

    if entry.get("reuse_mode") == "copy_live_step_and_widget_contract":
        source_widget_key = entry.get("source_widget_key")
        if not isinstance(source_widget_key, str) or not source_widget_key:
            raise KeyError(f"{target_step_alias}: missing source_widget_key for reuse")
        widget_payload = _find_source_widget(source_exports, widget_key=source_widget_key)
        parameters = widget_payload.setdefault("parameters", {})
        parameters["step"] = target_step_alias
        if parameter_overrides:
            parameters = _deep_merge(parameters, parameter_overrides)
            widget_payload["parameters"] = parameters
        _prune_invalid_table_rules(widget_payload)
        return widget_payload

    if widget_type == "number":
        measure_alias = widget_contract.get("measure_alias") or "Value"
        return {
            "type": "number",
            "parameters": {
                "step": target_step_alias,
                "measureField": measure_alias,
            },
        }

    template_widget_key = widget_contract.get("template_widget_key")
    template_export_key = widget_contract.get("template_export_key")
    if isinstance(template_widget_key, str) and template_widget_key:
        template_widget = _find_source_widget(
            source_exports,
            widget_key=template_widget_key,
            export_key=template_export_key if isinstance(template_export_key, str) else None,
        )
        return _materialize_widget_from_template(
            template_widget=template_widget,
            step_alias=target_step_alias,
            visualization_type=visualization_type,
            title_label=widget_contract.get("title_label"),
            columns=widget_contract.get("columns") if isinstance(widget_contract.get("columns"), list) else None,
            parameter_overrides=parameter_overrides,
        )

    payload = {
        "type": "chart",
        "parameters": {
            "step": target_step_alias,
            "visualizationType": visualization_type,
        },
    }
    columns = widget_contract.get("columns")
    if isinstance(columns, list):
        payload["parameters"]["columns"] = columns
    if parameter_overrides:
        payload["parameters"] = _deep_merge(payload["parameters"], parameter_overrides)
    if visualization_type in {"funnel", "treemap", "waterfall"}:
        payload["parameters"]["columnMap"] = None
    return payload


def _build_step_payload(
    *,
    entry: dict[str, Any],
    source_exports: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if entry.get("reuse_mode") == "copy_live_step_and_widget_contract":
        source_step_alias = entry.get("source_step_alias") or entry.get("target_step_alias")
        if not isinstance(source_step_alias, str) or not source_step_alias:
            raise KeyError(f"{entry.get('target_step_alias')}: missing source_step_alias for reuse")
        step_payload = _find_source_step(source_exports, source_step_alias)
        step_payload["query"] = entry["saql"]
        return step_payload

    payload: dict[str, Any] = {
        "type": entry.get("step_type") or "saql",
        "query": entry["saql"],
    }
    if payload["type"] == "saql":
        payload["broadcastFacet"] = True
    return payload


def materialize_contract(
    *,
    patch_set: dict[str, Any],
    candidate_state: dict[str, Any],
    step_contract: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source_exports = _load_source_exports(step_contract)
    materialized_patch_set = copy.deepcopy(patch_set)
    materialized_candidate_state = copy.deepcopy(candidate_state)

    step_fragment_by_alias = {
        fragment["target_path"].split("steps.", 1)[1]: fragment
        for fragment in materialized_patch_set.get("step_fragments", [])
        if isinstance(fragment, dict) and isinstance(fragment.get("target_path"), str) and "steps." in fragment["target_path"]
    }
    widget_fragment_by_component = {
        fragment["target_path"].split("widgets.", 1)[1]: fragment
        for fragment in materialized_patch_set.get("widget_fragments", [])
        if isinstance(fragment, dict) and isinstance(fragment.get("target_path"), str) and "widgets." in fragment["target_path"]
    }

    contract_entries = list(step_contract.get("reuse_steps") or []) + list(step_contract.get("new_steps") or [])
    if not contract_entries:
        raise ValueError("step_contract contains no reuse_steps or new_steps")

    step_updates = 0
    widget_updates = 0
    updated_step_aliases: list[str] = []
    updated_component_keys: list[str] = []

    materialized_candidate_state.setdefault("steps", {})
    materialized_candidate_state.setdefault("widgets", {})

    for entry in contract_entries:
        target_step_alias = entry.get("target_step_alias")
        target_component_key = entry.get("target_component_key")
        saql = entry.get("saql")
        if not isinstance(target_step_alias, str) or not target_step_alias:
            raise ValueError("step_contract entry missing target_step_alias")
        if not isinstance(target_component_key, str) or not target_component_key:
            raise ValueError(f"{target_step_alias}: missing target_component_key")
        if not isinstance(saql, str) or not saql:
            raise ValueError(f"{target_step_alias}: missing saql")

        step_payload = _build_step_payload(entry=entry, source_exports=source_exports)
        widget_payload = _build_widget_payload(entry=entry, source_exports=source_exports)

        step_fragment = step_fragment_by_alias.get(target_step_alias)
        if step_fragment is None:
            raise KeyError(f"patch_set missing step fragment for alias {target_step_alias}")
        widget_fragment = widget_fragment_by_component.get(target_component_key)
        if widget_fragment is None:
            raise KeyError(f"patch_set missing widget fragment for component {target_component_key}")

        step_fragment["payload"] = step_payload
        step_fragment.pop("todo_fields", None)
        widget_fragment["payload"] = widget_payload
        widget_fragment.pop("todo_fields", None)

        materialized_candidate_state["steps"][target_step_alias] = copy.deepcopy(step_payload)
        materialized_candidate_state["widgets"][target_component_key] = copy.deepcopy(widget_payload)

        step_updates += 1
        widget_updates += 1
        updated_step_aliases.append(target_step_alias)
        updated_component_keys.append(target_component_key)

    summary = {
        "artifact_type": "wave_patch_contract_materialization_summary",
        "module_id": step_contract.get("module_id"),
        "target_page_name": (step_contract.get("target_page") or {}).get("page_name"),
        "contract_entry_count": len(contract_entries),
        "step_updates": step_updates,
        "widget_updates": widget_updates,
        "updated_step_aliases": updated_step_aliases,
        "updated_component_keys": updated_component_keys,
    }
    return materialized_patch_set, materialized_candidate_state, summary


def write_materialized_outputs(
    *,
    output_dir: Path,
    materialized_patch_set: dict[str, Any],
    materialized_candidate_state: dict[str, Any],
    summary: dict[str, Any],
    source_paths: dict[str, str],
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    patch_set_path = output_dir / "wave_patch_set.materialized.json"
    candidate_state_path = output_dir / "dashboard_state.patch.materialized.json"
    summary_path = output_dir / "materialization_summary.json"
    review_path = output_dir / "README.md"

    _write_json(patch_set_path, materialized_patch_set)
    _write_json(candidate_state_path, materialized_candidate_state)
    _write_json(summary_path, summary)

    lines = [
        "# Wave PATCH Contract Materialization",
        "",
        f"- Patch set source: `{source_paths['patch_set']}`",
        f"- Candidate state source: `{source_paths['candidate_state']}`",
        f"- Step contract source: `{source_paths['step_contract']}`",
        f"- Contract entries: `{summary['contract_entry_count']}`",
        f"- Step updates: `{summary['step_updates']}`",
        f"- Widget updates: `{summary['widget_updates']}`",
        "",
        "## Outputs",
        "",
        f"- `wave_patch_set.materialized.json`",
        f"- `dashboard_state.patch.materialized.json`",
        f"- `materialization_summary.json`",
    ]
    review_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "patch_set": str(patch_set_path),
        "candidate_state": str(candidate_state_path),
        "summary": str(summary_path),
        "review": str(review_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--patch-set", required=True, help="Path to wave_patch_set.json")
    parser.add_argument("--candidate-state", required=True, help="Path to dashboard_state.patch.json")
    parser.add_argument("--step-contract", required=True, help="Path to companion step_contract.json")
    parser.add_argument("--output-dir", required=True, help="Directory for materialized artifacts")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    args = parser.parse_args()

    patch_set_path = Path(args.patch_set)
    candidate_state_path = Path(args.candidate_state)
    step_contract_path = Path(args.step_contract)
    output_dir = Path(args.output_dir)

    patch_set = load_json(patch_set_path)
    candidate_state = load_json(candidate_state_path)
    step_contract = load_json(step_contract_path)

    materialized_patch_set, materialized_candidate_state, summary = materialize_contract(
        patch_set=patch_set,
        candidate_state=candidate_state,
        step_contract=step_contract,
    )
    artifacts = write_materialized_outputs(
        output_dir=output_dir,
        materialized_patch_set=materialized_patch_set,
        materialized_candidate_state=materialized_candidate_state,
        summary=summary,
        source_paths={
            "patch_set": str(patch_set_path),
            "candidate_state": str(candidate_state_path),
            "step_contract": str(step_contract_path),
        },
    )

    result = {
        "status": "ok",
        "tool": "materialize_wave_patch_contract",
        "summary": summary,
        "artifacts": artifacts,
    }
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"contract_entries: {summary['contract_entry_count']}")
        print(f"step_updates: {summary['step_updates']}")
        print(f"widget_updates: {summary['widget_updates']}")
        print(f"patch_set: {artifacts['patch_set']}")
        print(f"candidate_state: {artifacts['candidate_state']}")
        print(f"summary: {artifacts['summary']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
