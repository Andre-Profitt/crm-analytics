#!/usr/bin/env python3
"""Compile builder-brain CRMA patch payloads into deterministic Wave worklists."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
CONTEXT_REGISTRY_PATH = ROOT / "config" / "context_registry.json"
DEFAULT_TARGET_ORG = "apro@simcorp.com"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import ai_os_browser
import executor_policy
import wave_patch_policy


ALLOWED_COLUMN_MAP_STRATEGIES = {
    "explicit_full",
    "null",
    "special_gauge",
    "special_choropleth",
    "auto_detect",
    "review_required",
}
SAQL_VISUALIZATION_TYPES = {
    "area",
    "bullet",
    "bubble",
    "column",
    "combo",
    "comparisontable",
    "heatmap",
    "hbar",
    "line",
    "number",
    "scatter",
    "stackarea",
    "stackcolumn",
    "stackhbar",
    "stackvbar",
    "timeline",
    "vbar",
}
AGGREGATEFLEX_VISUALIZATION_TYPES = {"donut", "funnel", "gauge", "pie", "treemap", "waterfall"}


def _load_contract_helpers():
    from crm_analytics_helpers import (
        find_dashboard_patch_contract_violations,
        normalize_dashboard_state_for_patch,
    )

    return find_dashboard_patch_contract_violations, normalize_dashboard_state_for_patch


def make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def make_result(
    *,
    status: str,
    command: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    command_class: str = "read_only",
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "wave_patch_executor",
        "lane": "intelligence_control",
        "command_class": command_class,
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return executor_policy.apply_policy_exceptions(payload)


def load_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def load_baseline_state(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("state"), dict):
        return payload["state"]
    if isinstance(payload, dict):
        return payload
    raise ValueError(f"{path}: expected dashboard JSON object or state object")


def load_context_registry() -> dict[str, Any]:
    return json.loads(CONTEXT_REGISTRY_PATH.read_text(encoding="utf-8"))


def load_evaluation_gate(path: Path) -> dict[str, Any]:
    return executor_policy.load_evaluation_gate(path)


def _load_wave_planning_context(state_path: Path) -> dict[str, Any] | None:
    context_path = state_path.with_name("wave_patch_memory_context.json")
    if not context_path.exists():
        return None
    payload = json.loads(context_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _append_evaluation_bypass_artifact(
    *,
    output_dir: Path | None,
    artifacts: list[dict[str, str]],
    command: str,
    target_org: str,
    evaluation_gate: dict[str, Any] | None,
    summary: dict[str, Any],
) -> None:
    executor_policy.append_evaluation_bypass_artifact(
        output_dir=output_dir,
        artifacts=artifacts,
        command=command,
        target_org=target_org,
        evaluation_gate=evaluation_gate,
        summary=summary,
    )


def _attach_memory_record(
    *,
    result: dict[str, Any],
    planning_context: dict[str, Any] | None,
    command: str,
    evaluation_gate: dict[str, Any] | None,
) -> dict[str, Any]:
    return executor_policy.attach_memory_record(
        result=result,
        planning_context=planning_context,
        command=command,
        evaluation_gate=evaluation_gate,
        script_path="scripts/wave_patch_executor.py",
        make_message=make_message,
    )


def _derive_wave_memory_context(
    *,
    planning_context: dict[str, Any] | None,
    payload: dict[str, Any] | None = None,
    evaluation_gate: dict[str, Any] | None = None,
    deploy_target: dict[str, Any] | None = None,
    output_dir: Path | None = None,
    state_path: Path | None = None,
    command: str,
) -> dict[str, Any] | None:
    return wave_patch_policy.derive_wave_memory_context(
        planning_context=planning_context,
        payload=payload,
        evaluation_gate=evaluation_gate,
        deploy_target=deploy_target,
        output_dir=output_dir,
        state_path=state_path,
        command=command,
    )


def load_org_session(target_org: str) -> tuple[str, str]:
    result = subprocess.run(
        ["sf", "org", "display", "--target-org", target_org, "--json"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "sf org display failed"
        raise RuntimeError(detail)
    payload = json.loads(result.stdout)
    org_result = payload.get("result") or {}
    access_token = org_result.get("accessToken")
    instance_url = org_result.get("instanceUrl")
    if not access_token or not instance_url:
        raise RuntimeError("sf org display did not return accessToken and instanceUrl")
    return instance_url, access_token


def _infer_dashboard_target_from_baseline(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        raise FileNotFoundError(path)

    candidate_paths: list[Path] = [path]
    if path.name == "dashboard.json":
        candidate_paths.extend([path.with_name("summary.json"), path.parent.parent / "manifest.json"])
    elif path.name == "summary.json":
        candidate_paths.append(path.parent.parent / "manifest.json")

    for candidate in candidate_paths:
        if not candidate.exists():
            continue
        payload = json.loads(candidate.read_text(encoding="utf-8"))
        if candidate.name == "summary.json":
            dashboard_id = payload.get("id")
            if dashboard_id:
                return {
                    "dashboard_id": dashboard_id,
                    "dashboard_label": payload.get("label"),
                    "source": "summary_json",
                    "path": str(candidate),
                }
        if candidate.name == "manifest.json":
            dashboards = payload.get("dashboards") or []
            if len(dashboards) == 1 and dashboards[0].get("id"):
                return {
                    "dashboard_id": dashboards[0]["id"],
                    "dashboard_label": dashboards[0].get("label"),
                    "source": "manifest_json",
                    "path": str(candidate),
                }
    return None


def resolve_dashboard_target(
    *,
    dashboard_id: str | None,
    baseline_ref: Path | None,
) -> dict[str, Any] | None:
    if dashboard_id:
        return {
            "dashboard_id": dashboard_id,
            "dashboard_label": None,
            "source": "cli_dashboard_id",
            "path": None,
        }
    if baseline_ref is None:
        return None
    return _infer_dashboard_target_from_baseline(baseline_ref)


def patch_dashboard_state(
    *,
    instance_url: str,
    access_token: str,
    dashboard_id: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    body = json.dumps({"state": state}).encode("utf-8")
    request = Request(
        f"{instance_url}/services/data/v66.0/wave/dashboards/{dashboard_id}",
        data=body,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(request) as response:
            raw = response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(exc)) from exc
    except URLError as exc:
        raise RuntimeError(str(exc)) from exc
    return json.loads(raw)


def _page_names(payload: dict[str, Any]) -> list[str]:
    return [item.get("page_name") for item in payload.get("page_mutations", []) if item.get("page_name")]


def validate_payload(payload: dict[str, Any]) -> tuple[list[str], list[str], dict[str, Any]]:
    errors: list[str] = []
    warnings: list[str] = []

    if payload.get("payload_type") != "wave_patch_payload":
        errors.append("payload_type must be wave_patch_payload")

    target_surface = payload.get("target_surface", {})
    if target_surface.get("surface_type") != "crma_dashboard":
        errors.append("target_surface.surface_type must be crma_dashboard")

    baseline_requirements = payload.get("baseline_requirements", {})
    if not baseline_requirements.get("requires_live_export"):
        warnings.append("baseline_requirements.requires_live_export should usually be true")
    if not baseline_requirements.get("normalization_required"):
        warnings.append("baseline_requirements.normalization_required should usually be true")
    if not baseline_requirements.get("guardrails"):
        errors.append("baseline_requirements.guardrails is required")

    page_mutations = payload.get("page_mutations", [])
    if not isinstance(page_mutations, list) or not page_mutations:
        errors.append("page_mutations must be a non-empty list")
        page_mutations = []

    nav_contract = payload.get("navigation_contract", {})
    nav_pages = nav_contract.get("pages", [])
    nav_mode = nav_contract.get("mode")
    page_names = _page_names(payload)

    if nav_mode not in {"single_page", "multi_page"}:
        errors.append("navigation_contract.mode must be single_page or multi_page")
    if nav_mode == "single_page" and len(page_names) != 1:
        errors.append("single_page navigation requires exactly one page mutation")
    if nav_mode == "multi_page" and len(page_names) < 2:
        errors.append("multi_page navigation requires at least two page mutations")

    nav_destination_names: list[str] = []
    for nav_page in nav_pages:
        destination_name = nav_page.get("destination_name")
        if not destination_name:
            errors.append("navigation_contract.pages entries require destination_name")
            continue
        nav_destination_names.append(destination_name)
    if sorted(nav_destination_names) != sorted(page_names):
        errors.append("navigation_contract pages must match page_mutations page_name values")
    if len(set(page_names)) != len(page_names):
        errors.append("page_mutations page_name values must be unique")

    widget_count = 0
    explicit_full_count = 0
    review_required_count = 0
    component_keys: set[str] = set()
    section_count = 0

    for page in page_mutations:
        if not page.get("page"):
            errors.append("each page_mutation requires page")
        if not page.get("page_name"):
            errors.append("each page_mutation requires page_name")
        section_mutations = page.get("section_mutations", [])
        if not isinstance(section_mutations, list) or not section_mutations:
            warnings.append(f"page {page.get('page_name') or page.get('page')} has no section_mutations")
            continue
        seen_section_orders: set[int] = set()
        for section in section_mutations:
            section_count += 1
            section_order = section.get("section_order")
            if not isinstance(section_order, int):
                errors.append(f"page {page.get('page_name')} has section without integer section_order")
            elif section_order in seen_section_orders:
                errors.append(f"page {page.get('page_name')} reuses section_order {section_order}")
            else:
                seen_section_orders.add(section_order)

            widget_mutations = section.get("widget_mutations", [])
            if not isinstance(widget_mutations, list) or not widget_mutations:
                warnings.append(
                    f"page {page.get('page_name')} section {section.get('section')} has no widget_mutations"
                )
                continue
            for widget in widget_mutations:
                widget_count += 1
                component_key = widget.get("component_key")
                if not component_key:
                    errors.append(f"page {page.get('page_name')} has widget without component_key")
                elif component_key in component_keys:
                    errors.append(f"component_key {component_key} is duplicated")
                else:
                    component_keys.add(component_key)

                strategy = widget.get("column_map_strategy")
                if strategy not in ALLOWED_COLUMN_MAP_STRATEGIES:
                    errors.append(
                        f"widget {component_key or widget.get('metric')}: invalid column_map_strategy {strategy}"
                    )
                elif strategy == "explicit_full":
                    explicit_full_count += 1
                elif strategy == "review_required":
                    review_required_count += 1

                if not widget.get("visualization_type"):
                    errors.append(f"widget {component_key or widget.get('metric')} is missing visualization_type")
                if not widget.get("metric"):
                    warnings.append(f"widget {component_key or 'unknown'} is missing metric")
                if not widget.get("contract_checks"):
                    errors.append(f"widget {component_key or widget.get('metric')} is missing contract_checks")

    handoff_link = payload.get("handoff_link")
    if handoff_link is not None and not handoff_link.get("target_surface"):
        errors.append("handoff_link.target_surface is required when handoff_link is present")

    validation_contract = payload.get("validation_contract", {})
    required_checks = validation_contract.get("required_checks", [])
    review_gates = validation_contract.get("review_gates", [])
    if not review_gates:
        warnings.append("validation_contract.review_gates is empty")
    for required_check in ("normalized_contract_lint", "screenshot_review"):
        if required_check not in required_checks:
            errors.append(f"validation_contract.required_checks must include {required_check}")

    summary = {
        "page_count": len(page_mutations),
        "section_count": section_count,
        "widget_count": widget_count,
        "explicit_full_widgets": explicit_full_count,
        "review_required_widgets": review_required_count,
        "has_handoff_link": bool(handoff_link and handoff_link.get("target_surface")),
        "review_gate_count": len(review_gates),
    }
    return errors, warnings, summary


def build_worklist(payload: dict[str, Any]) -> dict[str, Any]:
    sequence = 1
    steps: list[dict[str, Any]] = []

    steps.append(
        {
            "sequence": sequence,
            "phase": "baseline",
            "operation": "load_and_normalize_baseline",
            "purpose": "Export the live dashboard baseline and normalize it before patching.",
            "required_checks": payload["baseline_requirements"]["guardrails"],
        }
    )
    sequence += 1

    navigation_mode = payload["navigation_contract"]["mode"]
    for page in payload.get("page_mutations", []):
        steps.append(
            {
                "sequence": sequence,
                "phase": "patch",
                "operation": "ensure_page_scaffold",
                "page": page.get("page"),
                "page_name": page.get("page_name"),
                "purpose": page.get("purpose"),
                "emphasis_metric": page.get("emphasis_metric"),
                "required_checks": [
                    "page_name remains stable",
                    "page scaffold exists before widget mutation",
                ],
            }
        )
        sequence += 1

        if navigation_mode == "multi_page":
            steps.append(
                {
                    "sequence": sequence,
                    "phase": "patch",
                    "operation": "wire_navigation_destination",
                    "page": page.get("page"),
                    "page_name": page.get("page_name"),
                    "destination_name": page.get("nav_destination_name"),
                    "required_checks": [
                        "gridLayouts[].pages[].name must match destinationLink.name",
                    ],
                }
            )
            sequence += 1

        for section in page.get("section_mutations", []):
            for widget in section.get("widget_mutations", []):
                steps.append(
                    {
                        "sequence": sequence,
                        "phase": "patch",
                        "operation": "upsert_widget",
                        "page": page.get("page"),
                        "page_name": page.get("page_name"),
                        "section": section.get("section"),
                        "section_order": section.get("section_order"),
                        "layout_band": section.get("layout_band"),
                        "component_key": widget.get("component_key"),
                        "recommended_step_alias": widget.get("recommended_step_alias"),
                        "visualization_type": widget.get("visualization_type"),
                        "metric": widget.get("metric"),
                        "column_map_strategy": widget.get("column_map_strategy"),
                        "required_checks": widget.get("contract_checks", []),
                    }
                )
                sequence += 1

    handoff_link = payload.get("handoff_link")
    if handoff_link and handoff_link.get("target_surface"):
        destination_type = handoff_link.get("destination_type") or "dashboard"
        steps.append(
            {
                "sequence": sequence,
                "phase": "patch",
                "operation": (
                    "wire_handoff_link"
                    if destination_type in {"dashboard", "page"}
                    else "record_external_handoff"
                ),
                "target_surface": handoff_link.get("target_surface"),
                "destination_type": destination_type,
                "mode": handoff_link.get("mode"),
                "required_checks": [
                    (
                        "handoff surface name must match the packaged target"
                        if destination_type in {"dashboard", "page"}
                        else "keep the external handoff target aligned to the packaged target"
                    ),
                ],
            }
        )
        sequence += 1

    steps.append(
        {
            "sequence": sequence,
            "phase": "validation",
            "operation": "run_validation_contract",
            "review_gates": payload["validation_contract"]["review_gates"],
            "required_checks": payload["validation_contract"]["required_checks"],
            "design_constraints": payload["validation_contract"]["design_constraints"],
        }
    )

    widget_steps = [item for item in steps if item["operation"] == "upsert_widget"]
    return {
        "worklist_type": "wave_patch_worklist",
        "target_surface": payload["target_surface"],
        "summary": {
            "total_steps": len(steps),
            "widget_steps": len(widget_steps),
            "page_steps": sum(1 for item in steps if item["operation"] == "ensure_page_scaffold"),
            "navigation_steps": sum(1 for item in steps if item["operation"] == "wire_navigation_destination"),
            "has_handoff_link": any(item["operation"] == "wire_handoff_link" for item in steps),
            "has_external_handoff": any(item["operation"] == "record_external_handoff" for item in steps),
        },
        "steps": steps,
    }


def build_patch_bundle(
    *,
    payload: dict[str, Any],
    baseline_path: Path,
    normalized_state: dict[str, Any],
    baseline_violations: list[dict[str, str]],
    worklist: dict[str, Any],
) -> dict[str, Any]:
    baseline_pages = []
    baseline_page_names: set[str] = set()
    for grid in normalized_state.get("gridLayouts", []) or []:
        if not isinstance(grid, dict):
            continue
        for page in grid.get("pages", []) or []:
            if not isinstance(page, dict):
                continue
            name = page.get("name")
            if isinstance(name, str) and name:
                baseline_page_names.add(name)
                baseline_pages.append(name)

    baseline_widget_names = sorted((normalized_state.get("widgets", {}) or {}).keys())
    baseline_step_names = sorted((normalized_state.get("steps", {}) or {}).keys())

    page_scaffolds: list[dict[str, Any]] = []
    navigation_updates: list[dict[str, Any]] = []
    widget_upserts: list[dict[str, Any]] = []
    for page in payload.get("page_mutations", []):
        page_name = page.get("page_name")
        page_scaffolds.append(
            {
                "page": page.get("page"),
                "page_name": page_name,
                "purpose": page.get("purpose"),
                "baseline_page_present": page_name in baseline_page_names,
                "mutation_mode": "reuse_page" if page_name in baseline_page_names else "create_or_relabel_page",
            }
        )
        if payload.get("navigation_contract", {}).get("mode") == "multi_page":
            navigation_updates.append(
                {
                    "page": page.get("page"),
                    "page_name": page_name,
                    "destination_name": page.get("nav_destination_name"),
                    "baseline_page_present": page_name in baseline_page_names,
                }
            )
        for section in page.get("section_mutations", []):
            for widget in section.get("widget_mutations", []):
                widget_upserts.append(
                    {
                        "component_key": widget.get("component_key"),
                        "page_name": page_name,
                        "section": section.get("section"),
                        "section_order": section.get("section_order"),
                        "layout_band": section.get("layout_band"),
                        "metric": widget.get("metric"),
                        "visualization_type": widget.get("visualization_type"),
                        "recommended_step_alias": widget.get("recommended_step_alias"),
                        "column_map_strategy": widget.get("column_map_strategy"),
                        "column_map_patch": {
                            "mode": widget.get("column_map_strategy"),
                            "template": (
                                {
                                    "dimensionAxis": "__TBD__",
                                    "plots": "__TBD__",
                                    "trellis": [],
                                    "split": [],
                                }
                                if widget.get("column_map_strategy") == "explicit_full"
                                else None
                            ),
                        },
                        "required_checks": widget.get("contract_checks", []),
                    }
                )

    patch_set = build_patch_set(
        page_scaffolds=page_scaffolds,
        navigation_updates=navigation_updates,
        widget_upserts=widget_upserts,
        handoff_patch=payload.get("handoff_link"),
        validation_contract=payload.get("validation_contract"),
    )

    return {
        "bundle_type": "wave_patch_bundle",
        "target_surface": payload["target_surface"],
        "baseline": {
            "path": str(baseline_path),
            "pages": baseline_pages,
            "widget_count": len(baseline_widget_names),
            "step_count": len(baseline_step_names),
            "normalized": True,
            "contract_violation_count": len(baseline_violations),
        },
        "fragments": {
            "page_scaffolds": page_scaffolds,
            "navigation_updates": navigation_updates,
            "widget_upserts": widget_upserts,
            "handoff_patch": payload.get("handoff_link"),
            "validation_contract": payload.get("validation_contract"),
        },
        "patch_set": patch_set,
        "worklist_summary": worklist.get("summary", {}),
    }


def _section_height(layout_band: str) -> int:
    if layout_band == "hero_row":
        return 4
    if layout_band == "analysis_row":
        return 7
    if layout_band == "queue_row":
        return 8
    return 6


def _layout_cells(layout_band: str, count: int, start_row: int) -> list[dict[str, int]]:
    if count <= 0:
        return []
    if layout_band == "hero_row":
        if count == 1:
            return [{"row": start_row, "column": 0, "rowspan": 4, "colspan": 12}]
        if count == 2:
            return [
                {"row": start_row, "column": 0, "rowspan": 4, "colspan": 6},
                {"row": start_row, "column": 6, "rowspan": 4, "colspan": 6},
            ]
    if layout_band == "analysis_row":
        if count == 1:
            return [{"row": start_row, "column": 0, "rowspan": 7, "colspan": 12}]
        if count == 2:
            return [
                {"row": start_row, "column": 0, "rowspan": 7, "colspan": 6},
                {"row": start_row, "column": 6, "rowspan": 7, "colspan": 6},
            ]
    if layout_band == "queue_row":
        return [
            {"row": start_row + index * 6, "column": 0, "rowspan": 6, "colspan": 12}
            for index in range(count)
        ]

    cell_width = max(3, 12 // min(count, 4))
    return [
        {
            "row": start_row,
            "column": min(index * cell_width, 12 - cell_width),
            "rowspan": _section_height(layout_band),
            "colspan": cell_width,
        }
        for index in range(count)
    ]


def _widget_payload_fragment(widget: dict[str, Any]) -> dict[str, Any]:
    viz = widget.get("visualization_type")
    step_alias = widget.get("recommended_step_alias")
    if viz == "number":
        return {
            "type": "number",
            "parameters": {
                "step": step_alias,
                "measureField": _metric_field_alias(widget.get("metric")),
            },
        }

    params: dict[str, Any] = {
        "step": step_alias,
        "visualizationType": viz,
    }
    strategy = widget.get("column_map_strategy")
    if strategy == "explicit_full":
        params["columnMap"] = {
            "dimensionAxis": "__TBD__",
            "plots": "__TBD__",
            "trellis": [],
            "split": [],
        }
    elif strategy == "null":
        params["columnMap"] = None
    return {
        "type": "chart",
        "parameters": params,
    }


def build_patch_set(
    *,
    page_scaffolds: list[dict[str, Any]],
    navigation_updates: list[dict[str, Any]],
    widget_upserts: list[dict[str, Any]],
    handoff_patch: dict[str, Any] | None,
    validation_contract: dict[str, Any] | None,
) -> dict[str, Any]:
    page_fragments: list[dict[str, Any]] = []
    navigation_fragments: list[dict[str, Any]] = []
    layout_fragments: list[dict[str, Any]] = []
    widget_fragments: list[dict[str, Any]] = []
    step_fragments: list[dict[str, Any]] = []
    apply_order: list[str] = []

    for scaffold in page_scaffolds:
        fragment_id = f"page_{scaffold['page_name']}"
        page_fragments.append(
            {
                "fragment_id": fragment_id,
                "target_path": "gridLayouts[0].pages",
                "merge_strategy": scaffold["mutation_mode"],
                "payload": {
                    "name": scaffold["page_name"],
                    "widgets": [],
                },
            }
        )
        apply_order.append(fragment_id)

    for nav in navigation_updates:
        fragment_id = f"nav_{nav['page_name']}"
        navigation_fragments.append(
            {
                "fragment_id": fragment_id,
                "target_path": f"gridLayouts[0].pages[name={nav['page_name']}].name",
                "payload": nav["destination_name"],
            }
        )
        apply_order.append(fragment_id)

    page_widget_groups: dict[str, list[dict[str, Any]]] = {}
    for widget in widget_upserts:
        page_widget_groups.setdefault(widget["page_name"], []).append(widget)

    for page_name, widgets in page_widget_groups.items():
        current_row = 0
        section_groups: dict[tuple[str, int, str], list[dict[str, Any]]] = {}
        for widget in widgets:
            key = (widget["section"], widget["section_order"], widget["layout_band"])
            section_groups.setdefault(key, []).append(widget)

        ordered_sections = sorted(section_groups.items(), key=lambda item: item[0][1])
        for (section, section_order, layout_band), section_widgets in ordered_sections:
            cells = _layout_cells(layout_band, len(section_widgets), current_row)
            for widget, cell in zip(section_widgets, cells):
                step_id = f"step_{widget['recommended_step_alias']}"
                step_fragments.append(
                    {
                        "fragment_id": step_id,
                        "target_path": f"steps.{widget['recommended_step_alias']}",
                        "payload": {
                            "type": "__TBD__",
                            "query": "__TBD__",
                        },
                        "source_metric": widget["metric"],
                        "todo_fields": ["type", "query"],
                    }
                )
                apply_order.append(step_id)

                widget_id = f"widget_{widget['component_key']}"
                widget_fragments.append(
                    {
                        "fragment_id": widget_id,
                        "target_path": f"widgets.{widget['component_key']}",
                        "payload": _widget_payload_fragment(widget),
                        "todo_fields": [
                            "parameters.step",
                            *(
                                ["parameters.columnMap.dimensionAxis", "parameters.columnMap.plots"]
                                if widget["column_map_strategy"] == "explicit_full"
                                else []
                            ),
                        ],
                    }
                )
                apply_order.append(widget_id)

                layout_id = f"layout_{widget['component_key']}"
                layout_fragments.append(
                    {
                        "fragment_id": layout_id,
                        "target_path": f"gridLayouts[0].pages[name={page_name}].widgets",
                        "payload": {
                            "name": widget["component_key"],
                            "row": cell["row"],
                            "column": cell["column"],
                            "rowspan": cell["rowspan"],
                            "colspan": cell["colspan"],
                        },
                    }
                )
                apply_order.append(layout_id)
            current_row += _section_height(layout_band)

    handoff_fragment = None
    handoff_layout_fragment = None
    external_handoff = None
    if handoff_patch:
        target_page_name = page_scaffolds[-1]["page_name"] if page_scaffolds else "overview"
        target_rows = [
            item["payload"]["row"] + item["payload"]["rowspan"]
            for item in layout_fragments
            if item["target_path"] == f"gridLayouts[0].pages[name={target_page_name}].widgets"
        ]
        handoff_widget_name = f"handoff_link_{handoff_patch.get('target_surface')}"
        destination_type = handoff_patch.get("destination_type") or "dashboard"
        parameters = {
            "destination": "__TBD__",
            "destinationType": destination_type,
            "text": "Follow-up",
        }
        destination_name = (
            handoff_patch.get("target_destination_name")
            or handoff_patch.get("target_surface_label")
            or handoff_patch.get("target_surface")
        )
        if destination_type == "dashboard" and destination_name:
            parameters["destinationLink"] = {"name": destination_name}
        if destination_type in {"dashboard", "page"}:
            handoff_fragment = {
                "fragment_id": "handoff_link_contract",
                "target_path": f"widgets.{handoff_widget_name}",
                "payload": {
                    "type": "link",
                    "parameters": parameters,
                },
                "todo_fields": ["parameters.destination"],
                "resolution_hint": {
                    "target_surface": handoff_patch.get("target_surface"),
                    "target_surface_id": handoff_patch.get("target_surface_id"),
                    "target_surface_label": handoff_patch.get("target_surface_label"),
                },
            }
            apply_order.append(handoff_fragment["fragment_id"])
            handoff_layout_fragment = {
                "fragment_id": "handoff_link_layout",
                "target_path": f"gridLayouts[0].pages[name={target_page_name}].widgets",
                "payload": {
                    "name": handoff_widget_name,
                    "row": (max(target_rows) if target_rows else 0) + 1,
                    "column": 9,
                    "rowspan": 1,
                    "colspan": 3,
                },
            }
            apply_order.append(handoff_layout_fragment["fragment_id"])
        else:
            external_handoff = {
                "artifact_type": "external_handoff",
                "target_surface": handoff_patch.get("target_surface"),
                "target_surface_id": handoff_patch.get("target_surface_id"),
                "target_surface_label": handoff_patch.get("target_surface_label"),
                "target_destination_name": handoff_patch.get("target_destination_name"),
                "destination_type": destination_type,
                "mode": handoff_patch.get("mode"),
                "implementation": "package_only",
                "reason": (
                    "Wave link widgets support dashboard/page destinations; "
                    "external report handoffs stay in the build package and runbook."
                ),
            }

    return {
        "patch_set_type": "wave_dashboard_patch_set",
        "apply_order": apply_order,
        "page_fragments": page_fragments,
        "navigation_fragments": navigation_fragments,
        "layout_fragments": layout_fragments,
        "widget_fragments": widget_fragments,
        "step_fragments": step_fragments,
        "handoff_fragment": handoff_fragment,
        "handoff_layout_fragment": handoff_layout_fragment,
        "external_handoff": external_handoff,
        "validation_contract": validation_contract,
    }


def _extract_dashboard_id_from_url(live_url: str | None) -> str | None:
    if not live_url:
        return None
    match = re.search(r"/analytics/dashboard/([^/?#]+)", live_url)
    if match:
        return match.group(1)
    return None


def _normalize_lookup(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def _context_dashboard_lookup(context_registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for item in context_registry.get("dashboards", []):
        for key in filter(
            None,
            [
                item.get("id"),
                item.get("name"),
                _extract_dashboard_id_from_url(item.get("live_url")),
            ],
        ):
            lookup[_normalize_lookup(key)] = item
    return lookup


def _metric_field_alias(metric: str | None) -> str:
    if not metric:
        return "Value"
    text = (
        metric.replace("Variance Driver:", "")
        .replace("Action Queue:", "")
        .replace("Risk:", "")
    )
    tokens = [token.capitalize() for token in _normalize_lookup(text).split() if token]
    return "".join(tokens) or "Value"


def _component_context(widget_target_path: str) -> tuple[str, str]:
    widget_name = widget_target_path.split("widgets.", 1)[1]
    for section in ("headline_story", "diagnostic_breakdown", "action_layer"):
        marker = f"_{section}_"
        if marker in widget_name:
            page_name, _, _ = widget_name.partition(marker)
            return page_name, section
    return widget_name.rsplit("_", 1)[0], "supporting"


def _dimension_guess(page_name: str, section: str) -> str:
    if "ownership" in page_name or "handoff" in page_name:
        return "OwnerName"
    if "process" in page_name or "quality" in page_name:
        return "StageName"
    if section == "headline_story":
        return "ManagerName"
    if section == "action_layer":
        return "AccountName"
    return "GroupName"


def _infer_step_type(widget_fragment: dict[str, Any]) -> str:
    payload = widget_fragment.get("payload", {})
    if payload.get("type") == "number":
        return "saql"
    viz = payload.get("parameters", {}).get("visualizationType")
    if viz in AGGREGATEFLEX_VISUALIZATION_TYPES:
        return "aggregateflex"
    if viz in SAQL_VISUALIZATION_TYPES:
        return "saql"
    return "saql"


def _extract_common_baseline_dataset(normalized_state: dict[str, Any]) -> str | None:
    for step in (normalized_state.get("steps") or {}).values():
        if not isinstance(step, dict):
            continue
        if step.get("type") == "aggregateflex":
            datasets = step.get("datasets") or []
            for dataset in datasets:
                if isinstance(dataset, dict) and dataset.get("name"):
                    return dataset["name"]
        query = step.get("query")
        if isinstance(query, str):
            match = re.search(r'load\s+"([^"]+)"', query)
            if match:
                return match.group(1)
    return None


def _extract_selector_filter_clauses(normalized_state: dict[str, Any]) -> list[str]:
    clauses: list[str] = []
    for step_name, step in (normalized_state.get("steps") or {}).items():
        if not isinstance(step, dict) or step.get("type") != "aggregateflex":
            continue
        query = step.get("query")
        if isinstance(query, dict):
            groups = query.get("groups") or []
            if not groups and isinstance(query.get("query"), str):
                try:
                    query_payload = json.loads(query["query"])
                except json.JSONDecodeError:
                    query_payload = {}
                groups = query_payload.get("groups") or []
        else:
            groups = []
        if not groups:
            continue
        group_field = groups[0]
        if not isinstance(group_field, str) or not group_field:
            continue
        clauses.append(
            f'q = filter q by {{{{coalesce(column({step_name}.selection, ["{group_field}"]), '
            f'column({step_name}.result, ["{group_field}"])).asEquality(\'{group_field}\')}}}};'
        )
    return clauses


def _extract_common_record_type_filter(normalized_state: dict[str, Any]) -> str | None:
    record_type_counts: dict[str, int] = {}
    for step in (normalized_state.get("steps") or {}).values():
        if not isinstance(step, dict):
            continue
        query = step.get("query")
        if not isinstance(query, str):
            continue
        for match in re.finditer(r'RecordType\s*==\s*"([^"]+)"', query):
            value = match.group(1)
            record_type_counts[value] = record_type_counts.get(value, 0) + 1
    if not record_type_counts:
        return None
    return max(record_type_counts.items(), key=lambda item: item[1])[0]


def _baseline_query_context(normalized_state: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset": _extract_common_baseline_dataset(normalized_state),
        "selector_filter_clauses": _extract_selector_filter_clauses(normalized_state),
        "record_type_filter": _extract_common_record_type_filter(normalized_state),
    }


def _primary_dimension_and_measure(
    widget_fragment: dict[str, Any], source_metric: str | None
) -> tuple[str, str]:
    params = widget_fragment.get("payload", {}).get("parameters", {})
    column_map = params.get("columnMap")
    dimension = None
    measure = None
    if isinstance(column_map, dict):
        dimension_axis = column_map.get("dimensionAxis") or []
        plots = column_map.get("plots") or []
        if isinstance(dimension_axis, list) and dimension_axis:
            dimension = dimension_axis[0]
        if isinstance(plots, list) and plots:
            measure = plots[0]
        if dimension == "__TBD__":
            dimension = None
        if measure == "__TBD__":
            measure = None
    page_name, section = _component_context(widget_fragment["target_path"])
    if not dimension:
        dimension = _dimension_guess(page_name, section)
    if not measure:
        metric = params.get("measureField") or params.get("text") or source_metric
        measure = _metric_field_alias(metric)
    return dimension, measure


def _query_limit_for_viz(visualization_type: str | None) -> int:
    if visualization_type == "comparisontable":
        return 15
    if visualization_type == "number":
        return 1
    return 10


def _query_sort_direction(metric: str | None) -> str:
    text = (metric or "").lower()
    if "risk" in text and "confidence" in text:
        return "asc"
    return "desc"


def _commercial_rhythm_base_lines(
    query_context: dict[str, Any],
    *,
    record_type: str = "detail",
) -> list[str]:
    lines = ['q = load "Commercial_Rhythm_Control_Tower";']
    lines.append(f'q = filter q by RecordType == "{record_type}";')
    lines.extend(query_context.get("selector_filter_clauses") or [])
    return lines


def _build_commercial_rhythm_query_scaffold(
    *,
    widget_fragment: dict[str, Any],
    source_metric: str | None,
    query_context: dict[str, Any],
) -> tuple[str | None, bool]:
    metric_text = _normalize_lookup(source_metric or widget_fragment.get("payload", {}).get("parameters", {}).get("text") or "")
    page_name, _section = _component_context(widget_fragment["target_path"])
    widget_type = widget_fragment.get("payload", {}).get("type")

    if widget_type == "number" and "ownership alignment" in metric_text:
        lines = _commercial_rhythm_base_lines(query_context)
        lines.append("q = group q by all;")
        lines.append(
            "q = foreach q generate sum(CoveredRenewalOppCount) as CoveredRenewalOppCount, "
            "sum(RenewalOppCount) as RenewalOppCount;"
        )
        lines.append(
            "q = foreach q generate case when RenewalOppCount > 0 then "
            "(CoveredRenewalOppCount / RenewalOppCount) * 100 else 100 end as ActualOwnershipAlignment;"
        )
        lines.append("q = limit q 1;")
        return "\n".join(lines), False

    if "forecast hygiene" in metric_text:
        lines = _commercial_rhythm_base_lines(query_context)
        lines.append('q = filter q by IsClosed == "false";')
        if "ownership" in page_name or "handoff" in page_name:
            lines.append("q = group q by (OppOwnerName, OppOwnerPersona);")
            lines.append(
                "q = foreach q generate OppOwnerName as OwnerName, OppOwnerPersona as Persona, "
                "sum(ReviewCandidateCount) as ForecastHygiene, sum(OpenValue) as OpenValue;"
            )
        else:
            lines.append("q = group q by OppManagerName;")
            lines.append(
                "q = foreach q generate OppManagerName as ManagerName, "
                "sum(ReviewCandidateCount) as ForecastHygiene, sum(OpenValue) as OpenValue;"
            )
        lines.append("q = order q by ForecastHygiene desc;")
        lines.append(f"q = limit q {_query_limit_for_viz(widget_fragment.get('payload', {}).get('parameters', {}).get('visualizationType'))};")
        return "\n".join(lines), False

    if "renewal semantic confidence" in metric_text:
        lines = _commercial_rhythm_base_lines(query_context)
        lines.append('q = filter q by MotionType == "Renewal";')
        lines.append("q = group q by (OppManagerName, OppOwnerName, OppOwnerPersona, OppOwnershipAlignment);")
        lines.append(
            "q = foreach q generate OppManagerName as ManagerName, OppOwnerName as OwnerName, "
            "OppOwnerPersona as Persona, OppOwnershipAlignment as OwnershipAlignment, "
            "sum(RenewalOppCount) as RenewalOppCount, sum(CoveredRenewalOppCount) as CoveredRenewalOppCount, "
            "sum(AtRiskRenewalValue) as AtRiskRenewalValue, sum(ZeroValueRenewalCount) as ZeroValueRenewalCount;"
        )
        lines.append(
            "q = foreach q generate ManagerName, OwnerName, Persona, OwnershipAlignment, "
            "AtRiskRenewalValue, ZeroValueRenewalCount, "
            "case when RenewalOppCount > 0 then (CoveredRenewalOppCount / RenewalOppCount) * 100 else 0 end "
            "as RenewalSemanticConfidence;"
        )
        lines.append("q = order q by RenewalSemanticConfidence asc;")
        lines.append("q = limit q 15;")
        return "\n".join(lines), False

    if "handoff quality" in metric_text:
        lines = _commercial_rhythm_base_lines(query_context)
        lines.append('q = filter q by IsClosed == "false";')
        lines.append("q = filter q by ReviewCandidateCount > 0;")
        lines.append(
            "q = group q by (OpportunityName, AccountName, MotionType, OppOwnerName, OppOwnerPersona, "
            "HandoffState, ReviewPulse, LeadershipAsk, Id, AccountId);"
        )
        lines.append(
            "q = foreach q generate OpportunityName, AccountName, MotionType, "
            "OppOwnerName as OwnerName, OppOwnerPersona as Persona, HandoffState, ReviewPulse, LeadershipAsk, "
            "max(ReviewCandidateCount) as HandoffQuality, max(OpenValue) as OpenValue, Id, AccountId;"
        )
        lines.append("q = order q by HandoffQuality desc;")
        lines.append("q = limit q 15;")
        return "\n".join(lines), False

    return None, False


def _build_query_scaffold(
    *,
    widget_fragment: dict[str, Any],
    source_metric: str | None,
    query_context: dict[str, Any],
) -> tuple[str | None, bool]:
    dataset = query_context.get("dataset")
    if not dataset:
        return None, False

    if dataset == "Commercial_Rhythm_Control_Tower":
        query, requires_review = _build_commercial_rhythm_query_scaffold(
            widget_fragment=widget_fragment,
            source_metric=source_metric,
            query_context=query_context,
        )
        if query:
            return query, requires_review

    params = widget_fragment.get("payload", {}).get("parameters", {})
    visualization_type = params.get("visualizationType")
    metric = params.get("text") or source_metric
    dimension, measure = _primary_dimension_and_measure(widget_fragment, source_metric)

    lines = [f'q = load "{dataset}";']
    record_type_filter = query_context.get("record_type_filter")
    if record_type_filter:
        lines.append(f'q = filter q by RecordType == "{record_type_filter}";')
    lines.extend(query_context.get("selector_filter_clauses") or [])

    if widget_fragment.get("payload", {}).get("type") == "number":
        lines.append(f"q = foreach q generate sum({measure}) as {measure};")
        lines.append("q = limit q 1;")
        return "\n".join(lines), True

    lines.append(f"q = group q by '{dimension}';")
    lines.append(f"q = foreach q generate '{dimension}' as {dimension}, sum({measure}) as {measure};")
    lines.append(f"q = order q by {measure} {_query_sort_direction(metric)};")
    lines.append(f"q = limit q {_query_limit_for_viz(visualization_type)};")
    return "\n".join(lines), True


def apply_repo_truth_defaults(patch_set: dict[str, Any], normalized_state: dict[str, Any]) -> dict[str, Any]:
    context_registry = load_context_registry()
    dashboard_lookup = _context_dashboard_lookup(context_registry)
    query_context = _baseline_query_context(normalized_state)

    widget_by_step: dict[str, dict[str, Any]] = {}
    for fragment in patch_set.get("widget_fragments", []):
        step_name = fragment.get("payload", {}).get("parameters", {}).get("step")
        if isinstance(step_name, str):
            widget_by_step[step_name] = fragment

    autofills: list[dict[str, Any]] = []
    for fragment in patch_set.get("step_fragments", []):
        step_name = fragment["target_path"].split("steps.", 1)[1]
        widget_fragment = widget_by_step.get(step_name)
        if fragment.get("payload", {}).get("type") == "__TBD__":
            inferred_type = _infer_step_type(widget_fragment or {})
            fragment["payload"]["type"] = inferred_type
            if inferred_type == "saql":
                fragment["payload"]["broadcastFacet"] = True
            autofills.append(
                {
                    "category": "step_definition",
                    "fragment_id": fragment["fragment_id"],
                    "field_path": "type",
                    "value": inferred_type,
                    "source": "repo_truth_default",
                }
            )
        if fragment.get("payload", {}).get("query") == "__TBD__" and widget_fragment:
            query_scaffold, requires_review = _build_query_scaffold(
                widget_fragment=widget_fragment,
                source_metric=fragment.get("source_metric"),
                query_context=query_context,
            )
            if query_scaffold:
                fragment["payload"]["query"] = query_scaffold
                autofills.append(
                    {
                        "category": "step_definition",
                        "fragment_id": fragment["fragment_id"],
                        "field_path": "query",
                        "value": query_scaffold,
                        "source": "baseline_query_scaffold",
                        "review_required": requires_review,
                    }
                )

    for fragment in patch_set.get("widget_fragments", []):
        params = fragment.get("payload", {}).get("parameters", {})
        column_map = params.get("columnMap")
        if not isinstance(column_map, dict):
            continue
        page_name, section = _component_context(fragment["target_path"])
        if column_map.get("dimensionAxis") == "__TBD__":
            value = [_dimension_guess(page_name, section)]
            column_map["dimensionAxis"] = value
            autofills.append(
                {
                    "category": "widget_binding",
                    "fragment_id": fragment["fragment_id"],
                    "field_path": "parameters.columnMap.dimensionAxis",
                    "value": value,
                    "source": "heuristic_page_section_guess",
                }
            )
        if column_map.get("plots") == "__TBD__":
            metric = fragment.get("payload", {}).get("parameters", {}).get("text")
            if not metric:
                matching_step = fragment.get("payload", {}).get("parameters", {}).get("step")
                for step_fragment in patch_set.get("step_fragments", []):
                    if step_fragment["target_path"].endswith(matching_step):
                        metric = step_fragment.get("source_metric")
                        break
            value = [_metric_field_alias(metric)]
            column_map["plots"] = value
            autofills.append(
                {
                    "category": "widget_binding",
                    "fragment_id": fragment["fragment_id"],
                    "field_path": "parameters.columnMap.plots",
                    "value": value,
                    "source": "heuristic_metric_alias",
                }
            )

    handoff_fragment = patch_set.get("handoff_fragment")
    if handoff_fragment:
        resolution_hint = handoff_fragment.get("resolution_hint", {})
        parameters = handoff_fragment.get("payload", {}).get("parameters", {})
        destination_type = parameters.get("destinationType") or "dashboard"
        destination_link = parameters.get("destinationLink") or {}
        destination_name = destination_link.get("name")
        dashboard_id = resolution_hint.get("target_surface_id")
        if dashboard_id:
            source = "payload_target_surface_id"
        else:
            match = dashboard_lookup.get(
                _normalize_lookup(
                    resolution_hint.get("target_surface_label")
                    or destination_name
                    or resolution_hint.get("target_surface")
                    or ""
                )
            )
            dashboard_id = _extract_dashboard_id_from_url(match.get("live_url")) if match else None
            source = "context_registry_live_url"
        if dashboard_id and parameters.get("destination") == "__TBD__":
            parameters["destination"] = dashboard_id
            if destination_type == "dashboard" and "destinationLink" in parameters:
                parameters["destinationLink"]["name"] = dashboard_id
            autofills.append(
                {
                    "category": "handoff_binding",
                    "fragment_id": handoff_fragment["fragment_id"],
                    "field_path": "parameters.destination",
                    "value": dashboard_id,
                    "source": source,
                }
            )

    by_category: dict[str, int] = {}
    review_required_count = 0
    review_required_by_category: dict[str, int] = {}
    for item in autofills:
        by_category[item["category"]] = by_category.get(item["category"], 0) + 1
        if item.get("review_required"):
            review_required_count += 1
            review_required_by_category[item["category"]] = review_required_by_category.get(item["category"], 0) + 1
    return {
        "total_autofills": len(autofills),
        "by_category": by_category,
        "review_required_count": review_required_count,
        "review_required_by_category": review_required_by_category,
        "autofills": autofills,
    }


def _iter_placeholder_fields(value: Any, path: str = "") -> list[tuple[str, str]]:
    placeholders: list[tuple[str, str]] = []
    if value == "__TBD__":
        placeholders.append((path or "payload", "__TBD__"))
        return placeholders
    if isinstance(value, dict):
        for key, item in value.items():
            next_path = f"{path}.{key}" if path else key
            placeholders.extend(_iter_placeholder_fields(item, next_path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            next_path = f"{path}[{index}]"
            placeholders.extend(_iter_placeholder_fields(item, next_path))
    return placeholders


def build_fill_requirements(patch_set: dict[str, Any]) -> dict[str, Any]:
    requirements: list[dict[str, Any]] = []

    def add_fragment_requirements(
        *,
        fragment: dict[str, Any] | None,
        category: str,
        guidance: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if not fragment:
            return
        for field_path, current_value in _iter_placeholder_fields(fragment.get("payload")):
            requirements.append(
                {
                    "requirement_id": f"{fragment['fragment_id']}::{field_path}",
                    "category": category,
                    "fragment_id": fragment["fragment_id"],
                    "target_path": fragment["target_path"],
                    "field_path": field_path,
                    "current_value": current_value,
                    "fill_guidance": guidance,
                    **(extra or {}),
                }
            )

    for fragment in patch_set.get("step_fragments", []):
        add_fragment_requirements(
            fragment=fragment,
            category="step_definition",
            guidance="Replace placeholders with a supported Wave step type and a real query body.",
            extra={"source_metric": fragment.get("source_metric")},
        )

    for fragment in patch_set.get("widget_fragments", []):
        add_fragment_requirements(
            fragment=fragment,
            category="widget_binding",
            guidance="Resolve explicit widget bindings before live PATCH, especially columnMap dimensions/plots.",
        )

    add_fragment_requirements(
        fragment=patch_set.get("handoff_fragment"),
        category="handoff_binding",
        guidance="Replace the handoff destination placeholder with the actual target dashboard/report identifier.",
    )

    by_category: dict[str, int] = {}
    for item in requirements:
        by_category[item["category"]] = by_category.get(item["category"], 0) + 1

    return {
        "artifact_type": "wave_patch_fill_requirements",
        "blocking_for_live_patch": bool(requirements),
        "summary": {
            "total_requirements": len(requirements),
            "by_category": by_category,
        },
        "requirements": requirements,
    }


def build_query_review_checklist(
    patch_set: dict[str, Any],
    autofill_summary: dict[str, Any],
) -> dict[str, Any]:
    step_lookup = {
        fragment["fragment_id"]: fragment
        for fragment in patch_set.get("step_fragments", [])
        if isinstance(fragment, dict) and fragment.get("fragment_id")
    }
    items: list[dict[str, Any]] = []
    for autofill in autofill_summary.get("autofills", []):
        if not autofill.get("review_required"):
            continue
        fragment = step_lookup.get(autofill.get("fragment_id"))
        if not fragment:
            continue
        step_name = fragment["target_path"].split("steps.", 1)[1]
        query = fragment.get("payload", {}).get("query", "")
        items.append(
            {
                "review_id": f"{fragment['fragment_id']}::query_review",
                "fragment_id": fragment["fragment_id"],
                "step_name": step_name,
                "source_metric": fragment.get("source_metric"),
                "query_preview": query[:400],
                "review_focus": [
                    "Confirm the baseline dataset matches the intended dashboard grain.",
                    "Confirm selector/filter clauses are the right set for this surface.",
                    "Confirm the grouping field and measure alias match the widget intent and columnMap.",
                    "Confirm the SAQL still follows repo guardrails: group before foreach, single-column order by, no quoted fields inside aggregates.",
                ],
            }
        )
    return {
        "artifact_type": "wave_patch_query_review_checklist",
        "review_required_for_live_patch": bool(items),
        "summary": {
            "total_items": len(items),
            "step_query_reviews": len(items),
        },
        "items": items,
    }


def _extract_page_name(target_path: str) -> str:
    marker = "pages[name="
    start = target_path.find(marker)
    if start == -1:
        raise ValueError(f"Unsupported page target path: {target_path}")
    start += len(marker)
    end = target_path.find("]", start)
    if end == -1:
        raise ValueError(f"Unsupported page target path: {target_path}")
    return target_path[start:end]


def _collect_referenced_step_names(state: dict[str, Any]) -> set[str]:
    step_names: set[str] = set()
    for step in (state.get("steps") or {}).values():
        if not isinstance(step, dict):
            continue
        query = step.get("query")
        if not isinstance(query, str):
            continue
        step_names.update(
            match.group("step")
            for match in re.finditer(r"(?:column|cell)\((?P<step>[A-Za-z0-9_]+)\.(?:selection|result)\s*,", query)
        )
    for widget in (state.get("widgets") or {}).values():
        if not isinstance(widget, dict):
            continue
        params = widget.get("parameters")
        if not isinstance(params, dict):
            continue
        step_name = params.get("step")
        if isinstance(step_name, str) and step_name:
            step_names.add(step_name)
    return step_names


def assemble_candidate_state(
    *,
    normalized_state: dict[str, Any],
    patch_set: dict[str, Any],
    find_dashboard_patch_contract_violations,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, str]]]:
    candidate_state = json.loads(json.dumps(normalized_state))
    baseline_steps = {
        step_name: json.loads(json.dumps(step))
        for step_name, step in (candidate_state.get("steps") or {}).items()
        if isinstance(step_name, str) and step_name and isinstance(step, dict)
    }
    grid_layouts = candidate_state.get("gridLayouts", [])
    if not grid_layouts:
        grid_layouts = [{}]
        candidate_state["gridLayouts"] = grid_layouts
    primary_grid = grid_layouts[0]
    primary_grid.setdefault("pages", [])

    baseline_pages = [json.loads(json.dumps(page)) for page in primary_grid.get("pages", []) if isinstance(page, dict)]
    unused_pages = list(baseline_pages)
    ordered_pages: list[dict[str, Any]] = []
    page_lookup: dict[str, dict[str, Any]] = {}

    def take_page_by_name(name: str) -> dict[str, Any] | None:
        for index, page in enumerate(unused_pages):
            if page.get("name") == name:
                return unused_pages.pop(index)
        return None

    for fragment in patch_set.get("page_fragments", []):
        target_name = fragment["payload"]["name"]
        page = take_page_by_name(target_name)
        if page is None and fragment.get("merge_strategy") == "create_or_relabel_page" and unused_pages:
            page = unused_pages.pop(0)
        if page is None:
            page = {}
        page["name"] = target_name
        page["widgets"] = []
        ordered_pages.append(page)
        page_lookup[target_name] = page

    primary_grid["pages"] = ordered_pages

    for fragment in patch_set.get("navigation_fragments", []):
        page_name = _extract_page_name(fragment["target_path"])
        if page_name in page_lookup:
            page_lookup[page_name]["name"] = fragment["payload"]

    candidate_state["steps"] = {}
    for fragment in patch_set.get("step_fragments", []):
        step_name = fragment["target_path"].split("steps.", 1)[1]
        candidate_state["steps"][step_name] = json.loads(json.dumps(fragment["payload"]))

    candidate_state["widgets"] = {}
    for fragment in patch_set.get("widget_fragments", []):
        widget_name = fragment["target_path"].split("widgets.", 1)[1]
        candidate_state["widgets"][widget_name] = json.loads(json.dumps(fragment["payload"]))
    handoff_fragment = patch_set.get("handoff_fragment")
    if handoff_fragment:
        widget_name = handoff_fragment["target_path"].split("widgets.", 1)[1]
        candidate_state["widgets"][widget_name] = json.loads(json.dumps(handoff_fragment["payload"]))

    for fragment in patch_set.get("layout_fragments", []):
        page_name = _extract_page_name(fragment["target_path"])
        if page_name in page_lookup:
            page_lookup[page_name].setdefault("widgets", []).append(json.loads(json.dumps(fragment["payload"])))
    handoff_layout_fragment = patch_set.get("handoff_layout_fragment")
    if handoff_layout_fragment:
        page_name = _extract_page_name(handoff_layout_fragment["target_path"])
        if page_name in page_lookup:
            page_lookup[page_name].setdefault("widgets", []).append(json.loads(json.dumps(handoff_layout_fragment["payload"])))

    for referenced_step_name in sorted(_collect_referenced_step_names(candidate_state)):
        if referenced_step_name in candidate_state["steps"]:
            continue
        baseline_step = baseline_steps.get(referenced_step_name)
        if baseline_step:
            candidate_state["steps"][referenced_step_name] = json.loads(json.dumps(baseline_step))

    for page in page_lookup.values():
        page["widgets"] = sorted(
            page.get("widgets", []),
            key=lambda item: (item.get("row", 0), item.get("column", 0), item.get("name", "")),
        )

    candidate_violations = find_dashboard_patch_contract_violations(candidate_state)
    candidate_summary = {
        "page_count": len(primary_grid.get("pages", [])),
        "widget_count": len(candidate_state.get("widgets", {})),
        "step_count": len(candidate_state.get("steps", {})),
        "layout_item_count": sum(len(page.get("widgets", [])) for page in primary_grid.get("pages", [])),
        "contract_violation_count": len(candidate_violations),
        "pages": [page.get("name") for page in primary_grid.get("pages", [])],
    }
    return candidate_state, candidate_summary, candidate_violations


def print_text(payload: dict[str, Any]) -> None:
    if payload["command"] == "validate":
        summary = payload["summary"]
        print(f"pages: {summary['page_count']}")
        print(f"sections: {summary['section_count']}")
        print(f"widgets: {summary['widget_count']}")
        print(f"explicit_full_widgets: {summary['explicit_full_widgets']}")
        print(f"review_required_widgets: {summary['review_required_widgets']}")
        return

    if payload["command"] == "bundle":
        summary = payload["summary"]
        patch_bundle = payload["patch_bundle"]
        print(f"pages: {summary['page_count']}")
        print(f"widgets: {summary['widget_count']}")
        print(f"baseline_widget_count: {patch_bundle['baseline']['widget_count']}")
        print(f"fragment_widget_upserts: {len(patch_bundle['fragments']['widget_upserts'])}")
        print(f"patch_set_widget_fragments: {len(patch_bundle['patch_set']['widget_fragments'])}")
        print(f"candidate_contract_violations: {payload['candidate_state_summary']['contract_violation_count']}")
        print(f"autofills: {payload['autofill_summary']['total_autofills']}")
        print(f"autofill_review_required: {payload['autofill_summary']['review_required_count']}")
        print(f"fill_requirements: {payload['fill_requirements']['summary']['total_requirements']}")
        print(f"query_review_items: {payload['query_review_checklist']['summary']['total_items']}")
        return

    if payload["command"] == "deploy":
        deploy_target = payload["deploy_target"]
        deploy_summary = payload["deploy_summary"]
        print(f"dashboard_id: {deploy_target['dashboard_id']}")
        if deploy_target.get("dashboard_label"):
            print(f"dashboard_label: {deploy_target['dashboard_label']}")
        print(f"deploy_mode: {deploy_summary['mode']}")
        print(f"pages: {deploy_summary['page_count']}")
        print(f"widgets: {deploy_summary['widget_count']}")
        print(f"steps: {deploy_summary['step_count']}")
        print(f"body_bytes: {deploy_summary['body_bytes']}")
        print(f"contract_violations: {deploy_summary['contract_violation_count']}")
        return

    worklist = payload.get("worklist")
    if not worklist:
        summary = payload.get("summary", {})
        for message in payload.get("messages", []):
            if message["level"] in {"error", "warn"}:
                print(f"{message['level']}: {message['text']}")
        if summary:
            print(f"pages: {summary.get('page_count', 0)}")
            print(f"widgets: {summary.get('widget_count', 0)}")
        if isinstance(payload.get("review_artifact"), str) and payload["review_artifact"]:
            print(f"review_artifact: {payload['review_artifact']}")
        if isinstance(payload.get("collection_landing_artifact"), str) and payload["collection_landing_artifact"]:
            print(f"wave_patch_collection_landing_artifact: {payload['collection_landing_artifact']}")
        if isinstance(payload.get("browser_landing_artifact"), str) and payload["browser_landing_artifact"]:
            print(f"ai_os_browser_landing_artifact: {payload['browser_landing_artifact']}")
        if isinstance(payload.get("browser_health_landing_artifact"), str) and payload["browser_health_landing_artifact"]:
            print(f"ai_os_health_landing_artifact: {payload['browser_health_landing_artifact']}")
        return
    print(f"steps: {worklist['summary']['total_steps']}")
    print(f"widget_steps: {worklist['summary']['widget_steps']}")
    print(f"page_steps: {worklist['summary']['page_steps']}")
    print(f"navigation_steps: {worklist['summary']['navigation_steps']}")
    for step in worklist["steps"]:
        print(f"- {step['sequence']}: {step['operation']}")
    if isinstance(payload.get("review_artifact"), str) and payload["review_artifact"]:
        print(f"review_artifact: {payload['review_artifact']}")
    if isinstance(payload.get("collection_landing_artifact"), str) and payload["collection_landing_artifact"]:
        print(f"wave_patch_collection_landing_artifact: {payload['collection_landing_artifact']}")
    if isinstance(payload.get("browser_landing_artifact"), str) and payload["browser_landing_artifact"]:
        print(f"ai_os_browser_landing_artifact: {payload['browser_landing_artifact']}")
    if isinstance(payload.get("browser_health_landing_artifact"), str) and payload["browser_health_landing_artifact"]:
        print(f"ai_os_health_landing_artifact: {payload['browser_health_landing_artifact']}")


def _append_message_once(messages: list[dict[str, str]], message: dict[str, str]) -> None:
    if any(
        isinstance(existing, dict)
        and existing.get("code") == message.get("code")
        and existing.get("text") == message.get("text")
        for existing in messages
    ):
        return
    messages.append(message)


def _append_artifact_once(artifacts: list[dict[str, str]], artifact: dict[str, str]) -> None:
    if artifact in artifacts:
        return
    artifacts.append(artifact)


def _stringify_review_value(value: Any) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str) and value:
        return value
    if isinstance(value, list) and value and all(isinstance(item, (str, int, float, bool)) for item in value):
        return ", ".join(_stringify_review_value(item) or "" for item in value)
    return None


def _write_wave_patch_review(*, output_dir: Path, result: dict[str, Any]) -> Path:
    lines = [
        "# Wave PATCH Run",
        "",
        f"- Command: `{result.get('command') or 'unknown'}`",
        f"- Status: `{result.get('status') or 'unknown'}`",
        f"- Command class: `{result.get('command_class') or 'unknown'}`",
        f"- Run dir: `{output_dir}`",
    ]

    deploy_target = result.get("deploy_target")
    if isinstance(deploy_target, dict) and deploy_target:
        lines.extend(["", "## Target"])
        if isinstance(deploy_target.get("dashboard_id"), str) and deploy_target["dashboard_id"]:
            lines.append(f"- Dashboard id: `{deploy_target['dashboard_id']}`")
        if isinstance(deploy_target.get("dashboard_label"), str) and deploy_target["dashboard_label"]:
            lines.append(f"- Dashboard label: `{deploy_target['dashboard_label']}`")
        if isinstance(deploy_target.get("source"), str) and deploy_target["source"]:
            lines.append(f"- Resolution source: `{deploy_target['source']}`")

    summary_payload = result.get("deploy_summary")
    if not isinstance(summary_payload, dict):
        summary_payload = result.get("summary")
    if isinstance(summary_payload, dict) and summary_payload:
        lines.extend(["", "## Summary"])
        for key, value in summary_payload.items():
            rendered = _stringify_review_value(value)
            if rendered is not None:
                lines.append(f"- {key}: `{rendered}`")

    evaluation_gate = result.get("evaluation_gate")
    if isinstance(evaluation_gate, dict) and evaluation_gate:
        lines.extend(["", "## Evaluation Gate"])
        for key in ("verdict", "bypassed", "source", "run_id", "path"):
            rendered = _stringify_review_value(evaluation_gate.get(key))
            if rendered is not None:
                lines.append(f"- {key}: `{rendered}`")

    messages = result.get("messages") or []
    if messages:
        lines.extend(["", "## Messages"])
        for message in messages:
            if not isinstance(message, dict):
                continue
            level = message.get("level") or "info"
            code = message.get("code") or "message"
            text = message.get("text") or ""
            lines.append(f"- [{level}] `{code}`: {text}")

    artifacts = result.get("artifacts") or []
    if artifacts:
        lines.extend(["", "## Artifacts"])
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_type = artifact.get("type") or artifact.get("kind") or "artifact"
            path = artifact.get("path")
            if isinstance(path, str) and path:
                lines.append(f"- {artifact_type}: `{path}`")

    review_path = output_dir / "README.md"
    review_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return review_path


def _render_wave_patch_collection_entry(item: dict[str, Any]) -> list[str]:
    run_label = item.get("label") or Path(str(item.get("run_dir") or "")).name or "run"
    lines = [
        f"### {run_label}",
        f"- Command: `{item.get('command') or 'unknown'}`",
        f"- Status: `{item.get('status') or 'unknown'}`",
        f"- Updated: `{item.get('updated_at') or 'unknown'}`",
    ]
    if isinstance(item.get("mode"), str) and item["mode"]:
        lines.append(f"- Mode: `{item['mode']}`")
    if isinstance(item.get("dashboard_id"), str) and item["dashboard_id"]:
        lines.append(f"- Dashboard id: `{item['dashboard_id']}`")
    if isinstance(item.get("dashboard_label"), str) and item["dashboard_label"]:
        lines.append(f"- Dashboard label: `{item['dashboard_label']}`")
    if isinstance(item.get("evaluation_verdict"), str) and item["evaluation_verdict"]:
        lines.append(f"- Evaluation verdict: `{item['evaluation_verdict']}`")
    if isinstance(item.get("run_dir"), str) and item["run_dir"]:
        lines.append(f"- Run dir: `{item['run_dir']}`")
    if isinstance(item.get("landing_artifact"), str) and item["landing_artifact"]:
        lines.append(f"- Landing page: `{item['landing_artifact']}`")
    return lines


def _write_wave_patch_collection_index(*, collection_root: Path, entry: dict[str, Any]) -> tuple[Path, Path]:
    return ai_os_browser.write_run_collection_index(
        collection_root=collection_root,
        index_filename="wave_patch_run_index.json",
        overview_filename="wave_patch_overview.md",
        title="# Wave PATCH Runs",
        entry=entry,
        render_entry_lines=_render_wave_patch_collection_entry,
    )


def _attach_wave_patch_browser_artifacts(*, result: dict[str, Any], output_dir: Path | None) -> dict[str, Any]:
    if output_dir is None:
        return result

    output_dir.mkdir(parents=True, exist_ok=True)
    review_path = _write_wave_patch_review(output_dir=output_dir, result=result)

    deploy_target = result.get("deploy_target")
    deploy_summary = result.get("deploy_summary")
    evaluation_gate = result.get("evaluation_gate")
    collection_root = output_dir.parent
    collection_index_path, collection_overview_path = _write_wave_patch_collection_index(
        collection_root=collection_root,
        entry={
            "command": result.get("command"),
            "status": result.get("status"),
            "label": (
                (deploy_target or {}).get("dashboard_label")
                or (deploy_target or {}).get("dashboard_id")
                or output_dir.name
            ),
            "run_dir": str(output_dir),
            "landing_artifact": str(review_path),
            "mode": (deploy_summary or {}).get("mode") if isinstance(deploy_summary, dict) else None,
            "dashboard_id": (deploy_target or {}).get("dashboard_id") if isinstance(deploy_target, dict) else None,
            "dashboard_label": (deploy_target or {}).get("dashboard_label") if isinstance(deploy_target, dict) else None,
            "evaluation_verdict": (evaluation_gate or {}).get("verdict") if isinstance(evaluation_gate, dict) else None,
        },
    )
    browser_root = ai_os_browser.resolve_ai_os_browser_root(collection_root=collection_root)
    browser_index_path, browser_overview_path = ai_os_browser.write_ai_os_browser_index(browser_root=browser_root)
    health_summary = ai_os_browser.load_ai_os_browser_health_summary(index_path=browser_index_path)
    health_index_path, health_overview_path = ai_os_browser.resolve_ai_os_health_paths(browser_root=browser_root)

    result["review_artifact"] = str(review_path)
    result["collection_index_artifact"] = str(collection_index_path)
    result["collection_landing_artifact"] = str(collection_overview_path)
    result["browser_index_artifact"] = str(browser_index_path)
    result["browser_landing_artifact"] = str(browser_overview_path)
    result["browser_health_index_artifact"] = str(health_index_path)
    result["browser_health_landing_artifact"] = str(health_overview_path)
    result["browser_health_summary"] = health_summary

    artifacts = result.setdefault("artifacts", [])
    _append_artifact_once(artifacts, {"type": "wave_patch_review", "path": str(review_path)})
    _append_artifact_once(artifacts, {"type": "wave_patch_run_index", "path": str(collection_index_path)})
    _append_artifact_once(artifacts, {"type": "wave_patch_overview", "path": str(collection_overview_path)})
    _append_artifact_once(artifacts, {"type": "ai_os_collections_index", "path": str(browser_index_path)})
    _append_artifact_once(artifacts, {"type": "ai_os_overview", "path": str(browser_overview_path)})
    _append_artifact_once(artifacts, {"type": "ai_os_health", "path": str(health_index_path)})
    _append_artifact_once(artifacts, {"type": "ai_os_health_overview", "path": str(health_overview_path)})

    messages = result.setdefault("messages", [])
    _append_message_once(messages, make_message("info", "wave_patch_review_ready", f"Wave PATCH review: {review_path}"))
    _append_message_once(
        messages,
        make_message("info", "wave_patch_collection_index_ready", f"Wave PATCH collection overview: {collection_overview_path}"),
    )
    _append_message_once(messages, make_message("info", "ai_os_browser_ready", f"AI OS browser: {browser_overview_path}"))
    _append_message_once(messages, make_message("info", "ai_os_health_ready", f"AI OS health: {health_overview_path}"))
    return result


def _emit_result(*, result: dict[str, Any], output_dir: Path | None, json_mode: bool) -> None:
    result = _attach_wave_patch_browser_artifacts(result=result, output_dir=output_dir)
    if json_mode:
        print(json.dumps(result, indent=2))
    else:
        print_text(result)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate a wave_patch_payload contract.")
    validate.add_argument("--payload", required=True, help="Path to wave_patch_payload.json")
    validate.add_argument("--json", action="store_true", help="Print JSON output.")

    worklist = subparsers.add_parser("worklist", help="Compile a wave_patch_payload into an ordered worklist.")
    worklist.add_argument("--payload", required=True, help="Path to wave_patch_payload.json")
    worklist.add_argument("--output", default=None, help="Optional path for wave_patch_worklist.json")
    worklist.add_argument("--json", action="store_true", help="Print JSON output.")

    bundle = subparsers.add_parser(
        "bundle",
        help="Compile a payload plus exported baseline into a normalized patch bundle.",
    )
    bundle.add_argument("--payload", required=True, help="Path to wave_patch_payload.json")
    bundle.add_argument("--baseline", required=True, help="Path to exported dashboard.json or state JSON")
    bundle.add_argument("--evaluation", default=None, help="Optional path to evaluation.json from the plan evaluator.")
    bundle.add_argument("--output-dir", default=None, help="Optional directory for emitted bundle artifacts")
    bundle.add_argument("--json", action="store_true", help="Print JSON output.")

    deploy = subparsers.add_parser(
        "deploy",
        help="Preview or apply a compiled dashboard_state.patch.json to a live Wave dashboard.",
    )
    deploy.add_argument("--state", required=True, help="Path to dashboard_state.patch.json or a state JSON object")
    deploy.add_argument(
        "--dashboard-id",
        default=None,
        help="Explicit live Wave dashboard id. If omitted, infer from --baseline summary/manifest.",
    )
    deploy.add_argument(
        "--baseline",
        default=None,
        help="Path to exported dashboard.json, summary.json, or manifest.json for live dashboard-id inference.",
    )
    deploy.add_argument("--evaluation", default=None, help="Optional path to evaluation.json from the plan evaluator.")
    deploy.add_argument(
        "--allow-missing-evaluation",
        action="store_true",
        help="Allow live PATCH apply to continue without a pass evaluator verdict.",
    )
    deploy.add_argument("--target-org", default=DEFAULT_TARGET_ORG, help="Salesforce org alias or username.")
    deploy.add_argument("--apply", action="store_true", help="PATCH the live dashboard instead of previewing only.")
    deploy.add_argument("--output-dir", default=None, help="Optional directory for emitted preview/apply artifacts.")
    deploy.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    payload: dict[str, Any] | None = None
    errors: list[str] = []
    warnings: list[str] = []
    summary: dict[str, Any] = {}

    if args.command in {"validate", "worklist", "bundle"}:
        payload = load_payload(Path(args.payload))
        errors, warnings, summary = validate_payload(payload)

    if args.command == "validate":
        assert payload is not None
        status = "error" if errors else ("warn" if warnings else "ok")
        result = make_result(
            status=status,
            command="validate",
            messages=[
                *[make_message("error", "invalid_payload", item) for item in errors],
                *[make_message("warn", "payload_warning", item) for item in warnings],
                make_message(
                    "info" if not errors else "error",
                    "validation_complete",
                    "Validated wave patch payload." if not errors else "Wave patch payload validation failed.",
                ),
            ],
            summary=summary,
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_text(result)
        return 1 if errors else 0

    if args.command == "bundle":
        assert payload is not None
        planning_context = payload.get("planning_context") if isinstance(payload.get("planning_context"), dict) else None
        output_dir = Path(args.output_dir) if args.output_dir else None
        evaluation_gate = None
        if args.evaluation:
            try:
                evaluation_gate = load_evaluation_gate(Path(args.evaluation))
            except Exception as exc:
                result = make_result(
                    status="error",
                    command="bundle",
                    messages=[make_message("error", "evaluation_invalid", str(exc))],
                    summary=summary,
                )
                _emit_result(result=result, output_dir=output_dir, json_mode=args.json)
                return 1
        planning_context = _derive_wave_memory_context(
            planning_context=planning_context,
            payload=payload,
            evaluation_gate=evaluation_gate,
            output_dir=output_dir,
            command="bundle",
        )
        baseline_path = Path(args.baseline)
        (
            find_dashboard_patch_contract_violations,
            normalize_dashboard_state_for_patch,
        ) = _load_contract_helpers()
        baseline_state = load_baseline_state(baseline_path)
        normalized_state = normalize_dashboard_state_for_patch(
            baseline_state,
            strip_page_labels=True,
            strip_number_widget_patch_fields=True,
        )
        baseline_violations = find_dashboard_patch_contract_violations(normalized_state)
        worklist = build_worklist(payload) if not errors else None
        if errors or worklist is None:
            result = make_result(
                status="error",
                command="bundle",
                messages=[make_message("error", "invalid_payload", item) for item in errors],
                summary=summary,
            )
            _emit_result(result=result, output_dir=output_dir, json_mode=args.json)
            return 1

        patch_bundle = build_patch_bundle(
            payload=payload,
            baseline_path=baseline_path,
            normalized_state=normalized_state,
            baseline_violations=baseline_violations,
            worklist=worklist,
        )
        autofill_summary = apply_repo_truth_defaults(patch_bundle["patch_set"], normalized_state)
        fill_requirements = build_fill_requirements(patch_bundle["patch_set"])
        query_review_checklist = build_query_review_checklist(patch_bundle["patch_set"], autofill_summary)
        candidate_state, candidate_state_summary, candidate_violations = assemble_candidate_state(
            normalized_state=normalized_state,
            patch_set=patch_bundle["patch_set"],
            find_dashboard_patch_contract_violations=find_dashboard_patch_contract_violations,
        )
        artifacts: list[dict[str, str]] = []
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            normalized_path = output_dir / "normalized_baseline_state.json"
            worklist_path = output_dir / "wave_patch_worklist.json"
            bundle_path = output_dir / "wave_patch_bundle.json"
            patch_set_path = output_dir / "wave_patch_set.json"
            candidate_state_path = output_dir / "dashboard_state.patch.json"
            autofill_summary_path = output_dir / "wave_patch_autofill_summary.json"
            fill_requirements_path = output_dir / "wave_patch_fill_requirements.json"
            query_review_path = output_dir / "wave_patch_query_review_checklist.json"
            planning_context_path = output_dir / "wave_patch_memory_context.json"
            normalized_path.write_text(json.dumps(normalized_state, indent=2), encoding="utf-8")
            worklist_path.write_text(json.dumps(worklist, indent=2), encoding="utf-8")
            bundle_path.write_text(json.dumps(patch_bundle, indent=2), encoding="utf-8")
            patch_set_path.write_text(json.dumps(patch_bundle["patch_set"], indent=2), encoding="utf-8")
            candidate_state_path.write_text(json.dumps(candidate_state, indent=2), encoding="utf-8")
            autofill_summary_path.write_text(json.dumps(autofill_summary, indent=2), encoding="utf-8")
            fill_requirements_path.write_text(json.dumps(fill_requirements, indent=2), encoding="utf-8")
            query_review_path.write_text(json.dumps(query_review_checklist, indent=2), encoding="utf-8")
            if planning_context:
                planning_context_path.write_text(json.dumps(planning_context, indent=2), encoding="utf-8")
            artifacts.extend(
                [
                    {"type": "normalized_baseline_state", "path": str(normalized_path)},
                    {"type": "wave_patch_worklist", "path": str(worklist_path)},
                    {"type": "wave_patch_bundle", "path": str(bundle_path)},
                    {"type": "wave_patch_set", "path": str(patch_set_path)},
                    {"type": "dashboard_state_patch", "path": str(candidate_state_path)},
                    {"type": "wave_patch_autofill_summary", "path": str(autofill_summary_path)},
                    {"type": "wave_patch_fill_requirements", "path": str(fill_requirements_path)},
                    {"type": "wave_patch_query_review_checklist", "path": str(query_review_path)},
                    *(
                        [{"type": "wave_patch_memory_context", "path": str(planning_context_path)}]
                        if planning_context
                        else []
                    ),
                ]
            )

        status = (
            "error"
            if errors
            else (
                "warn"
                if warnings
                or baseline_violations
                or candidate_violations
                or autofill_summary.get("review_required_count")
                else "ok"
            )
        )
        messages = [
            *[make_message("warn", "payload_warning", item) for item in warnings],
            *[
                make_message(
                    "warn",
                    "baseline_contract_violation",
                    f"{item['code']}: {item['path']} :: {item['message']}",
                )
                for item in baseline_violations
            ],
            *[
                make_message(
                    "warn",
                    "candidate_contract_violation",
                    f"{item['code']}: {item['path']} :: {item['message']}",
                )
                for item in candidate_violations
            ],
            *(
                [
                    make_message(
                        "warn",
                        "heuristic_query_review_required",
                        (
                            "Applied heuristic query scaffolds from the baseline context; "
                            "review generated SAQL before any live PATCH."
                        ),
                    )
                ]
                if autofill_summary.get("review_required_count")
                else []
            ),
            make_message("info", "bundle_ready", "Compiled normalized Wave patch bundle."),
        ]
        result = make_result(
            status=status,
            command="bundle",
            messages=messages,
            artifacts=artifacts,
            summary=summary,
            worklist=worklist,
            patch_bundle=patch_bundle,
            candidate_state_summary=candidate_state_summary,
            candidate_contract_violations=candidate_violations,
            autofill_summary=autofill_summary,
            fill_requirements=fill_requirements,
            query_review_checklist=query_review_checklist,
            evaluation_gate=evaluation_gate,
        )
        _emit_result(result=result, output_dir=output_dir, json_mode=args.json)
        return 0 if not errors else 1

    if args.command == "deploy":
        state_path = Path(args.state)
        planning_context = _load_wave_planning_context(state_path)
        output_dir = Path(args.output_dir) if args.output_dir else None
        planning_context = _derive_wave_memory_context(
            planning_context=planning_context,
            evaluation_gate=None,
            output_dir=output_dir,
            state_path=state_path,
            command="deploy",
        )
        baseline_ref = Path(args.baseline) if args.baseline else None
        (
            find_dashboard_patch_contract_violations,
            normalize_dashboard_state_for_patch,
        ) = _load_contract_helpers()
        state = load_baseline_state(state_path)
        normalized_state = normalize_dashboard_state_for_patch(
            state,
            strip_page_labels=True,
            strip_number_widget_patch_fields=True,
        )
        violations = find_dashboard_patch_contract_violations(normalized_state)
        evaluation_gate, gate_messages, gate_error = wave_patch_policy.resolve_wave_deploy_evaluation_gate(
            evaluation_path=Path(args.evaluation) if args.evaluation else None,
            require_pass=args.apply,
            allow_missing=args.allow_missing_evaluation,
            make_message=make_message,
        )
        if gate_error is not None:
            result = make_result(
                status="error",
                command="deploy",
                messages=[make_message("error", gate_error["code"], gate_error["text"])],
                deploy_summary={
                    "mode": "apply" if args.apply else "dry_run",
                },
                evaluation_gate=evaluation_gate,
                command_class="mutating" if args.apply else "read_only",
            )
            if args.apply:
                result = _attach_memory_record(
                    result=result,
                    planning_context=planning_context,
                    command="deploy",
                    evaluation_gate=evaluation_gate,
                )
            _emit_result(result=result, output_dir=output_dir, json_mode=args.json)
            return 1

        planning_context = _derive_wave_memory_context(
            planning_context=planning_context,
            evaluation_gate=evaluation_gate,
            output_dir=output_dir,
            state_path=state_path,
            command="deploy",
        )
        deploy_target = resolve_dashboard_target(
            dashboard_id=args.dashboard_id,
            baseline_ref=baseline_ref,
        )
        planning_context = _derive_wave_memory_context(
            planning_context=planning_context,
            evaluation_gate=evaluation_gate,
            deploy_target=deploy_target,
            output_dir=output_dir,
            state_path=state_path,
            command="deploy",
        )
        deploy_summary = {
            "mode": "apply" if args.apply else "dry_run",
            "page_count": len((normalized_state.get("gridLayouts") or [{}])[0].get("pages", []))
            if normalized_state.get("gridLayouts")
            else 0,
            "widget_count": len(normalized_state.get("widgets") or {}),
            "step_count": len(normalized_state.get("steps") or {}),
            "body_bytes": len(json.dumps({"state": normalized_state}).encode("utf-8")),
            "contract_violation_count": len(violations),
            "evaluation_verdict": (evaluation_gate or {}).get("verdict"),
            "evaluation_bypassed": bool((evaluation_gate or {}).get("bypassed")),
        }
        artifacts: list[dict[str, str]] = []
        request_preview = {
            "artifact_type": "wave_patch_request",
            "mode": deploy_summary["mode"],
            "target_org": args.target_org,
            "dashboard_id": (deploy_target or {}).get("dashboard_id"),
            "dashboard_label": (deploy_target or {}).get("dashboard_label"),
            "request_path": (
                f"/services/data/v66.0/wave/dashboards/{deploy_target['dashboard_id']}"
                if deploy_target and deploy_target.get("dashboard_id")
                else None
            ),
            "state_path": str(state_path),
            "baseline_ref": str(baseline_ref) if baseline_ref else None,
            "summary": deploy_summary,
        }
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            preview_path = output_dir / "wave_patch_request.json"
            preview_path.write_text(json.dumps(request_preview, indent=2), encoding="utf-8")
            artifacts.append({"type": "wave_patch_request", "path": str(preview_path)})
            _append_evaluation_bypass_artifact(
                output_dir=output_dir,
                artifacts=artifacts,
                command="deploy",
                target_org=args.target_org,
                evaluation_gate=evaluation_gate,
                summary=deploy_summary,
            )

        if not deploy_target or not deploy_target.get("dashboard_id"):
            result = make_result(
                status="error",
                command="deploy",
                messages=[
                    make_message(
                        "error",
                        "dashboard_target_missing",
                        "No live dashboard id was provided or inferred from the baseline reference.",
                    )
                ],
                artifacts=artifacts,
                deploy_target=deploy_target,
                deploy_summary=deploy_summary,
                request_preview=request_preview,
                contract_violations=violations,
                evaluation_gate=evaluation_gate,
            )
            if args.apply:
                result = _attach_memory_record(
                    result=result,
                    planning_context=planning_context,
                    command="deploy",
                    evaluation_gate=evaluation_gate,
                )
            _emit_result(result=result, output_dir=output_dir, json_mode=args.json)
            return 1

        if violations:
            result = make_result(
                status="error",
                command="deploy",
                messages=[
                    *[
                        make_message(
                            "error",
                            "candidate_contract_violation",
                            f"{item['code']}: {item['path']} :: {item['message']}",
                        )
                        for item in violations
                    ],
                    make_message(
                        "error",
                        "deploy_blocked",
                        "Refusing to preview/apply a state with PATCH contract violations.",
                    ),
                ],
                artifacts=artifacts,
                deploy_target=deploy_target,
                deploy_summary=deploy_summary,
                request_preview=request_preview,
                contract_violations=violations,
                evaluation_gate=evaluation_gate,
                command_class="mutating" if args.apply else "read_only",
            )
            if args.apply:
                result = _attach_memory_record(
                    result=result,
                    planning_context=planning_context,
                    command="deploy",
                    evaluation_gate=evaluation_gate,
                )
            _emit_result(result=result, output_dir=output_dir, json_mode=args.json)
            return 1

        if not args.apply:
            result = make_result(
                status="ok",
                command="deploy",
                messages=[
                    *gate_messages,
                    make_message(
                        "info",
                        "deploy_preview_ready",
                        f"Wave PATCH preview is ready for dashboard {deploy_target['dashboard_id']}.",
                    )
                ],
                artifacts=artifacts,
                deploy_target=deploy_target,
                deploy_summary=deploy_summary,
                request_preview=request_preview,
                contract_violations=[],
                evaluation_gate=evaluation_gate,
            )
            _emit_result(result=result, output_dir=output_dir, json_mode=args.json)
            return 0

        try:
            instance_url, access_token = load_org_session(args.target_org)
            response_payload = patch_dashboard_state(
                instance_url=instance_url,
                access_token=access_token,
                dashboard_id=deploy_target["dashboard_id"],
                state=normalized_state,
            )
        except RuntimeError as exc:
            result = make_result(
                status="error",
                command="deploy",
                messages=[
                    *gate_messages,
                    make_message(
                        "error",
                        "deploy_failed",
                        f"Wave PATCH failed for dashboard {deploy_target['dashboard_id']}: {str(exc)[:2000]}",
                    )
                ],
                artifacts=artifacts,
                deploy_target=deploy_target,
                deploy_summary=deploy_summary,
                request_preview=request_preview,
                contract_violations=[],
                evaluation_gate=evaluation_gate,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=planning_context,
                command="deploy",
                evaluation_gate=evaluation_gate,
            )
            _emit_result(result=result, output_dir=output_dir, json_mode=args.json)
            return 1

        result = make_result(
            status="ok",
            command="deploy",
            messages=[
                *gate_messages,
                make_message(
                    "info",
                    "deploy_applied",
                    f"Applied Wave PATCH to dashboard {deploy_target['dashboard_id']}.",
                )
            ],
            artifacts=artifacts,
            deploy_target=deploy_target,
            deploy_summary=deploy_summary,
            request_preview=request_preview,
            contract_violations=[],
            evaluation_gate=evaluation_gate,
            applied_dashboard={
                "id": response_payload.get("id") or deploy_target["dashboard_id"],
                "name": response_payload.get("name"),
                "label": response_payload.get("label"),
            },
            command_class="mutating",
        )
        result = _attach_memory_record(
            result=result,
            planning_context=planning_context,
            command="deploy",
            evaluation_gate=evaluation_gate,
        )
        _emit_result(result=result, output_dir=output_dir, json_mode=args.json)
        return 0

    if errors:
        result = make_result(
            status="error",
            command="worklist",
            messages=[make_message("error", "invalid_payload", item) for item in errors],
            summary=summary,
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_text(result)
        return 1

    worklist = build_worklist(payload)
    artifacts: list[dict[str, str]] = []
    output_path = Path(args.output) if args.output else None
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(worklist, indent=2), encoding="utf-8")
        artifacts.append({"type": "wave_patch_worklist", "path": str(output_path)})

    result = make_result(
        status="warn" if warnings else "ok",
        command="worklist",
        messages=[
            *[make_message("warn", "payload_warning", item) for item in warnings],
            make_message("info", "worklist_ready", "Compiled wave patch worklist."),
        ],
        artifacts=artifacts,
        summary=summary,
        worklist=worklist,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print_text(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
