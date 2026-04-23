#!/usr/bin/env python3
"""Shared browser/index helpers for AI OS run collections."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any


LANDING_FILES_BY_INDEX = {
    "builder_brain_run_index.json": "README.md",
    "intelligence_workflow_run_index.json": "intelligence_workflow_overview.md",
    "salesforce_dashboard_run_index.json": "salesforce_dashboard_overview.md",
    "salesforce_report_run_index.json": "salesforce_report_overview.md",
    "wave_patch_run_index.json": "wave_patch_overview.md",
}

TOOL_FAMILY_BY_INDEX = {
    "builder_brain_run_index.json": "builder_brain",
    "intelligence_workflow_run_index.json": "intelligence_workflows",
    "salesforce_dashboard_run_index.json": "salesforce_dashboard",
    "salesforce_report_run_index.json": "salesforce_report",
    "wave_patch_run_index.json": "wave_patch",
}

RECENCY_24H = timedelta(hours=24)
RECENCY_7D = timedelta(days=7)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parse_updated_at(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _recency_bucket(value: Any, *, now: datetime) -> str:
    parsed = _parse_updated_at(value)
    if parsed is None:
        return "unknown"
    age = now - parsed
    if age <= RECENCY_24H:
        return "last_24h"
    if age <= RECENCY_7D:
        return "last_7d"
    return "older"


def _attention_level(score: int) -> str:
    if score >= 100:
        return "critical"
    if score >= 70:
        return "high"
    if score >= 30:
        return "medium"
    if score > 0:
        return "low"
    return "none"


def annotate_run_attention(entry: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    effective_now = now or datetime.now(timezone.utc)
    annotated = dict(entry)
    reasons: list[str] = []
    score = 0

    recency_bucket = _recency_bucket(annotated.get("updated_at"), now=effective_now)
    annotated["recency_bucket"] = recency_bucket
    annotated["is_stale"] = recency_bucket in {"older", "unknown"}

    if annotated.get("has_evaluation_bypass"):
        score += 100
        reasons.append("evaluation_bypass")

    status = annotated.get("status")
    if status == "error":
        score += 80
        reasons.append("error_status")
    elif status == "warn":
        score += 50
        reasons.append("warn_status")

    verdict = annotated.get("evaluation_verdict")
    if verdict == "fail":
        score += 40
        reasons.append("evaluation_fail")
    elif verdict == "needs_more_evidence":
        score += 20
        reasons.append("needs_more_evidence")

    if recency_bucket == "older":
        score += 15
        reasons.append("stale_run")
    elif recency_bucket == "unknown":
        score += 10
        reasons.append("unknown_run_age")

    annotated["attention_score"] = score
    annotated["attention_level"] = _attention_level(score)
    annotated["attention_reasons"] = reasons
    return annotated


def _recency_counter(entries: list[dict[str, Any]], *, key: str, now: datetime) -> dict[str, int]:
    counts = {
        "last_24h": 0,
        "last_7d": 0,
        "older": 0,
        "unknown": 0,
    }
    for item in entries:
        counts[_recency_bucket(item.get(key), now=now)] += 1
    return counts


def _latest_updated_at(entries: list[dict[str, Any]], *, key: str = "updated_at") -> str | None:
    latest_value: str | None = None
    latest_timestamp: datetime | None = None
    for item in entries:
        raw_value = item.get(key)
        parsed = _parse_updated_at(raw_value)
        if parsed is None:
            continue
        if latest_timestamp is None or parsed > latest_timestamp:
            latest_timestamp = parsed
            latest_value = raw_value
    return latest_value


def write_run_collection_index(
    *,
    collection_root: Path,
    index_filename: str,
    overview_filename: str,
    title: str,
    entry: dict[str, Any],
    render_entry_lines: Callable[[dict[str, Any]], list[str]],
) -> tuple[Path, Path]:
    collection_root.mkdir(parents=True, exist_ok=True)
    index_path = collection_root / index_filename
    existing_entries: list[dict[str, Any]] = []
    if index_path.exists():
        try:
            existing_payload = json.loads(index_path.read_text(encoding="utf-8"))
            if isinstance(existing_payload, dict) and isinstance(existing_payload.get("entries"), list):
                existing_entries = [item for item in existing_payload["entries"] if isinstance(item, dict)]
        except Exception:
            existing_entries = []

    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    normalized_entry = dict(entry)
    normalized_entry["updated_at"] = updated_at
    filtered_entries = [
        item
        for item in existing_entries
        if not (
            item.get("command") == normalized_entry.get("command")
            and item.get("run_dir") == normalized_entry.get("run_dir")
        )
    ]
    filtered_entries.insert(0, normalized_entry)
    filtered_entries.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    filtered_entries = filtered_entries[:25]
    _write_json(
        index_path,
        {
            "updated_at": updated_at,
            "entries": filtered_entries,
        },
    )

    lines = [
        title,
        "",
        f"- Collection root: `{collection_root}`",
        f"- Indexed runs: `{len(filtered_entries)}`",
        "",
        "## Recent Runs",
        "",
    ]
    for item in filtered_entries:
        lines.extend(render_entry_lines(item))
        lines.append("")

    overview_path = collection_root / overview_filename
    overview_path.write_text("\n".join(lines), encoding="utf-8")
    return index_path, overview_path


def write_collection_browser_index(
    *,
    browser_root: Path,
    source_index_filename: str,
    collection_landing_filename: str,
    output_index_filename: str,
    output_overview_filename: str,
    title: str,
    render_collection_lines: Callable[[dict[str, Any]], list[str]],
) -> tuple[Path, Path]:
    browser_root.mkdir(parents=True, exist_ok=True)
    collection_entries: list[dict[str, Any]] = []
    for index_path in browser_root.rglob(source_index_filename):
        if not index_path.is_file():
            continue
        try:
            payload = json.loads(index_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        entries = payload.get("entries")
        if not isinstance(entries, list):
            continue
        normalized_entries = [item for item in entries if isinstance(item, dict)]
        collection_dir = index_path.parent
        collection_entry: dict[str, Any] = {
            "collection_dir": str(collection_dir),
            "collection_index_artifact": str(index_path),
            "collection_landing_artifact": str(collection_dir / collection_landing_filename),
            "run_count": len(normalized_entries),
            "updated_at": payload.get("updated_at"),
        }
        latest_entry = normalized_entries[0] if normalized_entries else {}
        if isinstance(latest_entry, dict):
            collection_entry["latest_label"] = latest_entry.get("label")
            collection_entry["latest_status"] = latest_entry.get("status")
            collection_entry["latest_run_dir"] = latest_entry.get("run_dir")
            collection_entry["latest_landing_artifact"] = latest_entry.get("landing_artifact")
            if not isinstance(collection_entry.get("updated_at"), str) or not collection_entry["updated_at"]:
                collection_entry["updated_at"] = latest_entry.get("updated_at")
        collection_entries.append(collection_entry)

    collection_entries.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    collection_entries = collection_entries[:25]
    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    index_path = browser_root / output_index_filename
    _write_json(
        index_path,
        {
            "updated_at": updated_at,
            "collections": collection_entries,
        },
    )

    lines = [
        title,
        "",
        f"- Browser root: `{browser_root}`",
        f"- Indexed collections: `{len(collection_entries)}`",
        "",
        "## Recent Collections",
        "",
    ]
    for item in collection_entries:
        lines.extend(render_collection_lines(item))
        lines.append("")

    overview_path = browser_root / output_overview_filename
    overview_path.write_text("\n".join(lines), encoding="utf-8")
    return index_path, overview_path


def resolve_ai_os_browser_root(*, collection_root: Path) -> Path:
    resolved_root = collection_root.resolve()
    for candidate in (resolved_root, *resolved_root.parents):
        if candidate.name == "output":
            return candidate
    return collection_root


def resolve_ai_os_health_paths(*, browser_root: Path) -> tuple[Path, Path]:
    resolved_root = browser_root.resolve()
    return resolved_root / "ai_os_health.json", resolved_root / "ai_os_health.md"


def load_ai_os_browser_health_summary(*, index_path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    health_summary = payload.get("health_summary")
    return health_summary if isinstance(health_summary, dict) else {}


def _resolve_evaluation_bypass_artifact(run_dir: str | None) -> str | None:
    if not isinstance(run_dir, str) or not run_dir:
        return None
    bypass_path = Path(run_dir).expanduser().resolve() / "evaluation_bypass_audit.json"
    if bypass_path.exists():
        return str(bypass_path)
    return None


def _enrich_browser_run_entry(entry: dict[str, Any], *, collection: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(entry)
    bypass_path = _resolve_evaluation_bypass_artifact(enriched.get("run_dir"))
    enriched["has_evaluation_bypass"] = bool(bypass_path)
    if bypass_path:
        enriched["evaluation_bypass_artifact"] = bypass_path
        existing_exceptions = enriched.get("policy_exceptions")
        policy_exceptions = [item for item in existing_exceptions if isinstance(item, str)] if isinstance(existing_exceptions, list) else []
        if "evaluation_bypass" not in policy_exceptions:
            policy_exceptions.append("evaluation_bypass")
        enriched["policy_exceptions"] = policy_exceptions
    for key in (
        "tool_family",
        "collection_dir",
        "collection_index_artifact",
        "collection_landing_artifact",
    ):
        value = collection.get(key)
        if isinstance(value, str) and value:
            enriched[key] = value
    return annotate_run_attention(enriched)


def _status_counter(entries: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in entries:
        status = item.get("status")
        if not isinstance(status, str) or not status:
            continue
        counts[status] = counts.get(status, 0) + 1
    return counts


def _evaluation_verdict_counter(entries: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in entries:
        verdict = item.get("evaluation_verdict")
        if not isinstance(verdict, str) or not verdict:
            continue
        counts[verdict] = counts.get(verdict, 0) + 1
    return counts


def _attention_level_counter(entries: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in entries:
        level = item.get("attention_level")
        if not isinstance(level, str) or not level or level == "none":
            continue
        counts[level] = counts.get(level, 0) + 1
    return counts


def _sort_attention_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        entries,
        key=lambda item: (
            int(item.get("attention_score") or 0),
            str(item.get("updated_at") or ""),
        ),
        reverse=True,
    )


def _tool_family_health(entries: list[dict[str, Any]], *, now: datetime) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in entries:
        tool_family = item.get("tool_family")
        key = tool_family if isinstance(tool_family, str) and tool_family else "unknown"
        grouped.setdefault(key, []).append(item)

    summary: list[dict[str, Any]] = []
    for tool_family, grouped_entries in grouped.items():
        latest_updated_at = _latest_updated_at(grouped_entries)
        recency_bucket = _recency_bucket(latest_updated_at, now=now)
        max_attention_score = max((int(item.get("attention_score") or 0) for item in grouped_entries), default=0)
        summary.append(
            {
                "tool_family": tool_family,
                "run_count": len(grouped_entries),
                "status_counts": _status_counter(grouped_entries),
                "evaluation_verdict_counts": _evaluation_verdict_counter(grouped_entries),
                "evaluation_bypass_count": sum(1 for item in grouped_entries if item.get("has_evaluation_bypass")),
                "attention_run_count": sum(1 for item in grouped_entries if int(item.get("attention_score") or 0) > 0),
                "max_attention_score": max_attention_score,
                "max_attention_level": _attention_level(max_attention_score),
                "latest_updated_at": latest_updated_at,
                "recency_bucket": recency_bucket,
                "is_stale": recency_bucket in {"older", "unknown"},
            }
        )
    summary.sort(key=lambda item: str(item.get("latest_updated_at") or ""), reverse=True)
    return summary


def _is_risky_run(entry: dict[str, Any]) -> bool:
    if entry.get("has_evaluation_bypass"):
        return True
    status = entry.get("status")
    return isinstance(status, str) and status in {"warn", "error"}


def summarize_ai_os_health(
    *,
    browser_root: Path,
    collections: list[dict[str, Any]] | None = None,
    tool_family: str | None = None,
    top_k: int = 10,
) -> dict[str, Any]:
    browser_root = browser_root.resolve()
    now = datetime.now(timezone.utc)
    collection_entries: list[dict[str, Any]]
    if collections is None:
        collection_entries = []
        index_path = browser_root / "ai_os_collections_index.json"
        if index_path.exists():
            try:
                payload = json.loads(index_path.read_text(encoding="utf-8"))
                raw_collections = payload.get("collections")
                if isinstance(raw_collections, list):
                    collection_entries = [item for item in raw_collections if isinstance(item, dict)]
            except Exception:
                collection_entries = []
    else:
        collection_entries = [item for item in collections if isinstance(item, dict)]

    if tool_family:
        collection_entries = [item for item in collection_entries if item.get("tool_family") == tool_family]

    runs: list[dict[str, Any]] = []
    for collection_entry in collection_entries:
        raw_index_path = collection_entry.get("collection_index_artifact")
        if not isinstance(raw_index_path, str) or not raw_index_path:
            continue
        index_path = Path(raw_index_path).expanduser().resolve()
        try:
            payload = json.loads(index_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        entries = payload.get("entries")
        if not isinstance(entries, list):
            continue
        runs.extend(
            _enrich_browser_run_entry(item, collection=collection_entry) for item in entries if isinstance(item, dict)
        )

    runs.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    risk_runs = _sort_attention_entries([item for item in runs if _is_risky_run(item)])
    attention_runs = _sort_attention_entries([item for item in runs if int(item.get("attention_score") or 0) > 0])
    tool_family_health = _tool_family_health(runs, now=now)
    run_recency_counts = _recency_counter(runs, key="updated_at", now=now)
    collection_recency_counts = _recency_counter(collection_entries, key="updated_at", now=now)
    stale_tool_families = [
        {
            "tool_family": item.get("tool_family") or "unknown",
            "latest_updated_at": item.get("latest_updated_at"),
            "recency_bucket": item.get("recency_bucket") or "unknown",
        }
        for item in tool_family_health
        if item.get("is_stale")
    ]
    return {
        "browser_root": str(browser_root),
        "filters": {"tool_family": tool_family},
        "collection_count": len(collection_entries),
        "run_count": len(runs),
        "latest_run_updated_at": _latest_updated_at(runs),
        "latest_collection_updated_at": _latest_updated_at(collection_entries),
        "run_recency_counts": run_recency_counts,
        "collection_recency_counts": collection_recency_counts,
        "stale_collection_count": collection_recency_counts.get("older", 0),
        "unknown_collection_count": collection_recency_counts.get("unknown", 0),
        "evaluation_bypass_count": sum(1 for item in runs if item.get("has_evaluation_bypass")),
        "risk_run_count": len(risk_runs),
        "attention_run_count": len(attention_runs),
        "status_counts": _status_counter(runs),
        "evaluation_verdict_counts": _evaluation_verdict_counter(runs),
        "attention_level_counts": _attention_level_counter(runs),
        "tool_family_health": tool_family_health,
        "stale_tool_families": stale_tool_families,
        "risky_runs": risk_runs[: max(1, top_k)],
        "attention_runs": attention_runs[: max(1, top_k)],
    }


def write_ai_os_browser_index(*, browser_root: Path) -> tuple[Path, Path]:
    browser_root.mkdir(parents=True, exist_ok=True)
    collection_entries: list[dict[str, Any]] = []
    for index_path in browser_root.rglob("*_run_index.json"):
        if not index_path.is_file():
            continue
        landing_name = LANDING_FILES_BY_INDEX.get(index_path.name)
        tool_family = TOOL_FAMILY_BY_INDEX.get(index_path.name)
        if landing_name is None or tool_family is None:
            continue
        try:
            payload = json.loads(index_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        entries = payload.get("entries")
        if not isinstance(entries, list):
            continue
        normalized_entries = [item for item in entries if isinstance(item, dict)]
        collection_dir = index_path.parent
        collection_entry: dict[str, Any] = {
            "tool_family": tool_family,
            "collection_dir": str(collection_dir),
            "collection_index_artifact": str(index_path),
            "collection_landing_artifact": str(collection_dir / landing_name),
            "run_count": len(normalized_entries),
            "updated_at": payload.get("updated_at"),
        }
        latest_entry = normalized_entries[0] if normalized_entries else {}
        if isinstance(latest_entry, dict):
            collection_entry["latest_label"] = latest_entry.get("label")
            collection_entry["latest_status"] = latest_entry.get("status")
            collection_entry["latest_run_dir"] = latest_entry.get("run_dir")
            collection_entry["latest_landing_artifact"] = latest_entry.get("landing_artifact")
            if not isinstance(collection_entry.get("updated_at"), str) or not collection_entry["updated_at"]:
                collection_entry["updated_at"] = latest_entry.get("updated_at")
        collection_entries.append(collection_entry)

    collection_entries.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    collection_entries = collection_entries[:25]
    health_summary = summarize_ai_os_health(browser_root=browser_root, collections=collection_entries)
    health_json_path, health_markdown_path = write_ai_os_health_artifacts(
        browser_root=browser_root,
        summary=health_summary,
    )
    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    index_path = browser_root / "ai_os_collections_index.json"
    _write_json(
        index_path,
        {
            "updated_at": updated_at,
            "collections": collection_entries,
            "health_summary": health_summary,
        },
    )

    lines = [
        "# AI OS Collections",
        "",
        f"- Browser root: `{browser_root}`",
        f"- Indexed collections: `{len(collection_entries)}`",
        f"- Indexed runs: `{health_summary.get('run_count', 0)}`",
        f"- Risk runs: `{health_summary.get('risk_run_count', 0)}`",
        f"- Attention runs: `{health_summary.get('attention_run_count', 0)}`",
        f"- Evaluation bypass runs: `{health_summary.get('evaluation_bypass_count', 0)}`",
        f"- Stale collections: `{health_summary.get('stale_collection_count', 0)}`",
        f"- Health JSON: `{health_json_path}`",
        f"- Health overview: `{health_markdown_path}`",
        "",
        "## Health Snapshot",
        "",
    ]
    status_counts = health_summary.get("status_counts") or {}
    if isinstance(status_counts, dict) and status_counts:
        rendered_statuses = " ".join(f"{key}={value}" for key, value in status_counts.items())
        lines.append(f"- Status counts: `{rendered_statuses}`")
    attention_level_counts = health_summary.get("attention_level_counts") or {}
    if isinstance(attention_level_counts, dict) and attention_level_counts:
        rendered_attention = " ".join(f"{key}={value}" for key, value in attention_level_counts.items())
        lines.append(f"- Attention levels: `{rendered_attention}`")
    run_recency_counts = health_summary.get("run_recency_counts") or {}
    if isinstance(run_recency_counts, dict) and run_recency_counts:
        rendered_recency = " ".join(f"{key}={value}" for key, value in run_recency_counts.items())
        lines.append(f"- Run recency: `{rendered_recency}`")
    collection_recency_counts = health_summary.get("collection_recency_counts") or {}
    if isinstance(collection_recency_counts, dict) and collection_recency_counts:
        rendered_collection_recency = " ".join(f"{key}={value}" for key, value in collection_recency_counts.items())
        lines.append(f"- Collection recency: `{rendered_collection_recency}`")
    if isinstance(health_summary.get("latest_run_updated_at"), str) and health_summary["latest_run_updated_at"]:
        lines.append(f"- Latest run update: `{health_summary['latest_run_updated_at']}`")
    tool_family_health = health_summary.get("tool_family_health") or []
    if isinstance(tool_family_health, list) and tool_family_health:
        for item in tool_family_health[:5]:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {item.get('tool_family') or 'unknown'}: "
                f"runs=`{item.get('run_count', 0)}` "
                f"bypass=`{item.get('evaluation_bypass_count', 0)}` "
                f"attention=`{item.get('attention_run_count', 0)}` "
                f"recency=`{item.get('recency_bucket') or 'unknown'}`"
            )
    stale_tool_families = health_summary.get("stale_tool_families") or []
    if isinstance(stale_tool_families, list) and stale_tool_families:
        rendered_stale = ", ".join(
            f"{item.get('tool_family') or 'unknown'}:{item.get('recency_bucket') or 'unknown'}"
            for item in stale_tool_families[:5]
            if isinstance(item, dict)
        )
        if rendered_stale:
            lines.append(f"- Stale tool families: `{rendered_stale}`")
    attention_runs = health_summary.get("attention_runs") or []
    if isinstance(attention_runs, list) and attention_runs:
        lines.extend(["", "### Attention Queue"])
        for item in attention_runs[:5]:
            if not isinstance(item, dict):
                continue
            label = item.get("label") or item.get("run_dir") or "run"
            line = (
                f"- {label}: `{item.get('tool_family') or 'unknown'}` "
                f"`{item.get('status') or 'unknown'}` "
                f"`{item.get('attention_level') or 'none'}`"
            )
            if item.get("has_evaluation_bypass"):
                line += " `evaluation_bypass`"
            lines.append(line)
    lines.extend([
        "",
        "## Recent Collections",
        "",
    ])
    for item in collection_entries:
        collection_label = Path(str(item.get("collection_dir") or "")).name or "collection"
        lines.append(f"### {collection_label}")
        lines.append(f"- Tool family: `{item.get('tool_family') or 'unknown'}`")
        if isinstance(item.get("updated_at"), str) and item["updated_at"]:
            lines.append(f"- Updated: `{item['updated_at']}`")
        if isinstance(item.get("collection_dir"), str) and item["collection_dir"]:
            lines.append(f"- Collection dir: `{item['collection_dir']}`")
        if isinstance(item.get("collection_landing_artifact"), str) and item["collection_landing_artifact"]:
            lines.append(f"- Collection landing page: `{item['collection_landing_artifact']}`")
        if isinstance(item.get("latest_landing_artifact"), str) and item["latest_landing_artifact"]:
            lines.append(f"- Latest landing page: `{item['latest_landing_artifact']}`")
        if isinstance(item.get("latest_run_dir"), str) and item["latest_run_dir"]:
            lines.append(f"- Latest run dir: `{item['latest_run_dir']}`")
        if isinstance(item.get("run_count"), int):
            lines.append(f"- Indexed runs: `{item['run_count']}`")
        lines.append("")

    overview_path = browser_root / "ai_os_overview.md"
    overview_path.write_text("\n".join(lines), encoding="utf-8")
    return index_path, overview_path


def write_ai_os_health_artifacts(*, browser_root: Path, summary: dict[str, Any]) -> tuple[Path, Path]:
    browser_root.mkdir(parents=True, exist_ok=True)
    json_path, markdown_path = resolve_ai_os_health_paths(browser_root=browser_root)
    _write_json(json_path, summary)

    lines = [
        "# AI OS Health",
        "",
        f"- Browser root: `{browser_root}`",
        f"- Indexed collections: `{summary.get('collection_count', 0)}`",
        f"- Indexed runs: `{summary.get('run_count', 0)}`",
        f"- Risk runs: `{summary.get('risk_run_count', 0)}`",
        f"- Attention runs: `{summary.get('attention_run_count', 0)}`",
        f"- Evaluation bypass runs: `{summary.get('evaluation_bypass_count', 0)}`",
        "",
        "## Freshness",
        "",
    ]
    latest_run_updated_at = summary.get("latest_run_updated_at")
    if isinstance(latest_run_updated_at, str) and latest_run_updated_at:
        lines.append(f"- Latest run update: `{latest_run_updated_at}`")
    latest_collection_updated_at = summary.get("latest_collection_updated_at")
    if isinstance(latest_collection_updated_at, str) and latest_collection_updated_at:
        lines.append(f"- Latest collection update: `{latest_collection_updated_at}`")
    run_recency_counts = summary.get("run_recency_counts") or {}
    if isinstance(run_recency_counts, dict) and run_recency_counts:
        rendered_run_recency = " ".join(f"{key}={value}" for key, value in run_recency_counts.items())
        lines.append(f"- Run recency: `{rendered_run_recency}`")
    collection_recency_counts = summary.get("collection_recency_counts") or {}
    if isinstance(collection_recency_counts, dict) and collection_recency_counts:
        rendered_collection_recency = " ".join(f"{key}={value}" for key, value in collection_recency_counts.items())
        lines.append(f"- Collection recency: `{rendered_collection_recency}`")
    lines.extend(
        [
            f"- Stale collections: `{summary.get('stale_collection_count', 0)}`",
            f"- Unknown-age collections: `{summary.get('unknown_collection_count', 0)}`",
            "",
            "## Status Counts",
            "",
        ]
    )
    status_counts = summary.get("status_counts") or {}
    if isinstance(status_counts, dict) and status_counts:
        for key, value in status_counts.items():
            lines.append(f"- {key}: `{value}`")
    else:
        lines.append("- none")

    lines.extend(["", "## Attention Levels", ""])
    attention_level_counts = summary.get("attention_level_counts") or {}
    if isinstance(attention_level_counts, dict) and attention_level_counts:
        for key, value in attention_level_counts.items():
            lines.append(f"- {key}: `{value}`")
    else:
        lines.append("- none")

    lines.extend(["", "## Tool Families", ""])
    tool_family_health = summary.get("tool_family_health") or []
    if isinstance(tool_family_health, list) and tool_family_health:
        for item in tool_family_health:
            if not isinstance(item, dict):
                continue
            lines.append(f"### {item.get('tool_family') or 'unknown'}")
            lines.append(f"- Runs: `{item.get('run_count', 0)}`")
            lines.append(f"- Evaluation bypasses: `{item.get('evaluation_bypass_count', 0)}`")
            lines.append(f"- Attention runs: `{item.get('attention_run_count', 0)}`")
            if isinstance(item.get("latest_updated_at"), str) and item["latest_updated_at"]:
                lines.append(f"- Latest update: `{item['latest_updated_at']}`")
            lines.append(f"- Recency: `{item.get('recency_bucket') or 'unknown'}`")
            lines.append(f"- Stale: `{bool(item.get('is_stale'))}`")
            lines.append(f"- Max attention: `{item.get('max_attention_level') or 'none'}`")
            status_counts = item.get("status_counts") or {}
            if isinstance(status_counts, dict) and status_counts:
                rendered_statuses = " ".join(f"{key}={value}" for key, value in status_counts.items())
                lines.append(f"- Status counts: `{rendered_statuses}`")
            verdict_counts = item.get("evaluation_verdict_counts") or {}
            if isinstance(verdict_counts, dict) and verdict_counts:
                rendered_verdicts = " ".join(f"{key}={value}" for key, value in verdict_counts.items())
                lines.append(f"- Evaluation verdicts: `{rendered_verdicts}`")
            lines.append("")
    else:
        lines.append("- none")
        lines.append("")

    lines.extend(["## Stale Tool Families", ""])
    stale_tool_families = summary.get("stale_tool_families") or []
    if isinstance(stale_tool_families, list) and stale_tool_families:
        for item in stale_tool_families:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {item.get('tool_family') or 'unknown'}: "
                f"`{item.get('recency_bucket') or 'unknown'}` "
                f"latest=`{item.get('latest_updated_at') or 'unknown'}`"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.extend(["## Attention Queue", ""])
    attention_runs = summary.get("attention_runs") or []
    if isinstance(attention_runs, list) and attention_runs:
        for item in attention_runs:
            if not isinstance(item, dict):
                continue
            label = item.get("label") or item.get("run_dir") or "run"
            lines.append(f"### {label}")
            lines.append(f"- Tool family: `{item.get('tool_family') or 'unknown'}`")
            lines.append(f"- Command: `{item.get('command') or 'unknown'}`")
            lines.append(f"- Status: `{item.get('status') or 'unknown'}`")
            lines.append(f"- Attention: `{item.get('attention_level') or 'none'}` score=`{item.get('attention_score') or 0}`")
            if item.get("has_evaluation_bypass"):
                lines.append("- Evaluation bypass: `true`")
            if isinstance(item.get("evaluation_verdict"), str) and item["evaluation_verdict"]:
                lines.append(f"- Evaluation verdict: `{item['evaluation_verdict']}`")
            reasons = item.get("attention_reasons") or []
            if isinstance(reasons, list) and reasons:
                lines.append(f"- Attention reasons: `{' '.join(str(reason) for reason in reasons)}`")
            if isinstance(item.get("run_dir"), str) and item["run_dir"]:
                lines.append(f"- Run dir: `{item['run_dir']}`")
            if isinstance(item.get("landing_artifact"), str) and item["landing_artifact"]:
                lines.append(f"- Landing page: `{item['landing_artifact']}`")
            lines.append("")
    else:
        lines.append("- none")

    markdown_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, markdown_path
