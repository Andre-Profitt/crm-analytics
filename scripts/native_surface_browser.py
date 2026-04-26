#!/usr/bin/env python3
"""Shared browser/index helpers for native dashboard/report executor runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import ai_os_browser


SURFACE_CONFIG = {
    "dashboard": {
        "run_title": "# Salesforce Dashboard Run",
        "collection_title": "# Salesforce Dashboard Runs",
        "index_filename": "salesforce_dashboard_run_index.json",
        "overview_filename": "salesforce_dashboard_overview.md",
        "artifact_prefix": "salesforce_dashboard",
        "review_message": "Salesforce dashboard review",
        "collection_message": "Salesforce dashboard collection overview",
    },
    "report": {
        "run_title": "# Salesforce Report Run",
        "collection_title": "# Salesforce Report Runs",
        "index_filename": "salesforce_report_run_index.json",
        "overview_filename": "salesforce_report_overview.md",
        "artifact_prefix": "salesforce_report",
        "review_message": "Salesforce report review",
        "collection_message": "Salesforce report collection overview",
    },
}


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


def _extract_target_metadata(*, result: dict[str, Any], surface: str) -> dict[str, str | None]:
    if surface == "dashboard":
        target = result.get("applied_dashboard")
        summary = result.get("summary") or {}
        target_id = None
        target_name = None
        if isinstance(target, dict):
            raw_id = target.get("id")
            raw_name = target.get("name")
            target_id = raw_id if isinstance(raw_id, str) and raw_id else None
            target_name = raw_name if isinstance(raw_name, str) and raw_name else None
        if target_id is None and isinstance(summary, dict):
            for key in ("applied_dashboard_id", "deleted_dashboard_id", "target_dashboard_id"):
                raw_value = summary.get(key)
                if isinstance(raw_value, str) and raw_value:
                    target_id = raw_value
                    break
        return {"id": target_id, "name": target_name}

    target = result.get("applied_report")
    summary = result.get("summary") or {}
    target_id = None
    target_name = None
    if isinstance(target, dict):
        raw_id = target.get("id")
        raw_name = target.get("name")
        target_id = raw_id if isinstance(raw_id, str) and raw_id else None
        target_name = raw_name if isinstance(raw_name, str) and raw_name else None
    if target_id is None and isinstance(summary, dict):
        for key in ("applied_report_id", "deleted_report_id", "target_report_id"):
            raw_value = summary.get(key)
            if isinstance(raw_value, str) and raw_value:
                target_id = raw_value
                break
    return {"id": target_id, "name": target_name}


def _review_lines(*, result: dict[str, Any], output_dir: Path, surface: str) -> list[str]:
    config = SURFACE_CONFIG[surface]
    target = _extract_target_metadata(result=result, surface=surface)
    lines = [
        config["run_title"],
        "",
        f"- Command: `{result.get('command') or 'unknown'}`",
        f"- Status: `{result.get('status') or 'unknown'}`",
        f"- Command class: `{result.get('command_class') or 'unknown'}`",
        f"- Run dir: `{output_dir}`",
    ]

    if target.get("id") or target.get("name"):
        lines.extend(["", "## Target"])
        if target.get("id"):
            label = "Dashboard id" if surface == "dashboard" else "Report id"
            lines.append(f"- {label}: `{target['id']}`")
        if target.get("name"):
            label = "Dashboard name" if surface == "dashboard" else "Report name"
            lines.append(f"- {label}: `{target['name']}`")

    summary_payload = result.get("apply_summary")
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
    return lines


def _write_review(*, output_dir: Path, result: dict[str, Any], surface: str) -> Path:
    review_path = output_dir / "README.md"
    review_path.write_text("\n".join(_review_lines(result=result, output_dir=output_dir, surface=surface)) + "\n", encoding="utf-8")
    return review_path


def _render_collection_entry(*, item: dict[str, Any], surface: str) -> list[str]:
    target_label = "Dashboard" if surface == "dashboard" else "Report"
    run_label = item.get("label") or Path(str(item.get("run_dir") or "")).name or "run"
    lines = [
        f"### {run_label}",
        f"- Command: `{item.get('command') or 'unknown'}`",
        f"- Status: `{item.get('status') or 'unknown'}`",
        f"- Updated: `{item.get('updated_at') or 'unknown'}`",
    ]
    if isinstance(item.get("mode"), str) and item["mode"]:
        lines.append(f"- Mode: `{item['mode']}`")
    if isinstance(item.get("target_id"), str) and item["target_id"]:
        lines.append(f"- {target_label} id: `{item['target_id']}`")
    if isinstance(item.get("target_name"), str) and item["target_name"]:
        lines.append(f"- {target_label} name: `{item['target_name']}`")
    if isinstance(item.get("evaluation_verdict"), str) and item["evaluation_verdict"]:
        lines.append(f"- Evaluation verdict: `{item['evaluation_verdict']}`")
    if isinstance(item.get("run_dir"), str) and item["run_dir"]:
        lines.append(f"- Run dir: `{item['run_dir']}`")
    if isinstance(item.get("landing_artifact"), str) and item["landing_artifact"]:
        lines.append(f"- Landing page: `{item['landing_artifact']}`")
    return lines


def attach_native_surface_browser_artifacts(
    *,
    result: dict[str, Any],
    output_dir: Path | None,
    surface: str,
    make_message,
) -> dict[str, Any]:
    if output_dir is None:
        return result

    config = SURFACE_CONFIG[surface]
    output_dir.mkdir(parents=True, exist_ok=True)
    review_path = _write_review(output_dir=output_dir, result=result, surface=surface)
    target = _extract_target_metadata(result=result, surface=surface)
    apply_summary = result.get("apply_summary")
    collection_root = output_dir.parent
    collection_index_path, collection_overview_path = ai_os_browser.write_run_collection_index(
        collection_root=collection_root,
        index_filename=config["index_filename"],
        overview_filename=config["overview_filename"],
        title=config["collection_title"],
        entry={
            "command": result.get("command"),
            "status": result.get("status"),
            "label": target.get("name") or target.get("id") or output_dir.name,
            "run_dir": str(output_dir),
            "landing_artifact": str(review_path),
            "mode": (apply_summary or {}).get("mode") if isinstance(apply_summary, dict) else None,
            "target_id": target.get("id"),
            "target_name": target.get("name"),
            "evaluation_verdict": (result.get("evaluation_gate") or {}).get("verdict")
            if isinstance(result.get("evaluation_gate"), dict)
            else None,
        },
        render_entry_lines=lambda item: _render_collection_entry(item=item, surface=surface),
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
    _append_artifact_once(artifacts, {"type": f"{config['artifact_prefix']}_review", "path": str(review_path)})
    _append_artifact_once(artifacts, {"type": config["index_filename"].removesuffix(".json"), "path": str(collection_index_path)})
    _append_artifact_once(artifacts, {"type": config["overview_filename"].removesuffix(".md"), "path": str(collection_overview_path)})
    _append_artifact_once(artifacts, {"type": "ai_os_collections_index", "path": str(browser_index_path)})
    _append_artifact_once(artifacts, {"type": "ai_os_overview", "path": str(browser_overview_path)})
    _append_artifact_once(artifacts, {"type": "ai_os_health", "path": str(health_index_path)})
    _append_artifact_once(artifacts, {"type": "ai_os_health_overview", "path": str(health_overview_path)})

    messages = result.setdefault("messages", [])
    prefix = config["artifact_prefix"]
    _append_message_once(messages, make_message("info", f"{prefix}_review_ready", f"{config['review_message']}: {review_path}"))
    _append_message_once(
        messages,
        make_message("info", f"{prefix}_collection_index_ready", f"{config['collection_message']}: {collection_overview_path}"),
    )
    _append_message_once(messages, make_message("info", "ai_os_browser_ready", f"AI OS browser: {browser_overview_path}"))
    _append_message_once(messages, make_message("info", "ai_os_health_ready", f"AI OS health: {health_overview_path}"))
    return result
