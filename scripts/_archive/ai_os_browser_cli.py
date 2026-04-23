#!/usr/bin/env python3
"""Read-only CLI for the generated AI OS browser and collection indexes."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import ai_os_browser


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BROWSER_ROOT = Path(os.environ.get("CRM_AI_BROWSER_ROOT", ROOT / "output")).expanduser()
INDEX_FILENAME_BY_TOOL_FAMILY = {value: key for key, value in ai_os_browser.TOOL_FAMILY_BY_INDEX.items()}


def make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def make_result(
    *,
    status: str,
    command: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "ai_os_browser_cli",
        "lane": "intelligence_control",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return payload


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _resolve_browser_root(raw_value: str | None) -> Path:
    return Path(raw_value).expanduser().resolve() if raw_value else DEFAULT_BROWSER_ROOT.resolve()


def _load_browser_index(*, browser_root: Path, refresh: bool) -> tuple[Path, Path, dict[str, Any]]:
    index_path = browser_root / "ai_os_collections_index.json"
    overview_path = browser_root / "ai_os_overview.md"
    if refresh or not index_path.exists() or not overview_path.exists():
        index_path, overview_path = ai_os_browser.write_ai_os_browser_index(browser_root=browser_root)
    payload = _load_json_object(index_path)
    if not isinstance(payload.get("collections"), list):
        payload["collections"] = []
    if not isinstance(payload.get("health_summary"), dict):
        payload["health_summary"] = {}
    return index_path, overview_path, payload


def _filter_collections(
    collections: list[dict[str, Any]],
    *,
    tool_family: str | None,
    top_k: int,
) -> list[dict[str, Any]]:
    filtered = [item for item in collections if isinstance(item, dict)]
    if tool_family:
        filtered = [item for item in filtered if item.get("tool_family") == tool_family]
    return filtered[:top_k]


def _resolve_collection_index_path(
    *,
    browser_collections: list[dict[str, Any]],
    tool_family: str | None,
    collection_dir: str | None,
    collection_index: str | None,
) -> tuple[Path, dict[str, Any] | None]:
    if collection_index:
        return Path(collection_index).expanduser().resolve(), None

    if collection_dir:
        collection_root = Path(collection_dir).expanduser().resolve()
        if tool_family:
            index_name = INDEX_FILENAME_BY_TOOL_FAMILY.get(tool_family)
            if index_name is None:
                raise ValueError(f"Unknown tool family: {tool_family}")
            index_path = collection_root / index_name
            if not index_path.exists():
                raise FileNotFoundError(f"No {index_name} under {collection_root}")
            return index_path, None

        matching_paths = [
            collection_root / index_name
            for index_name in ai_os_browser.TOOL_FAMILY_BY_INDEX
            if (collection_root / index_name).exists()
        ]
        if not matching_paths:
            raise FileNotFoundError(f"No known *_run_index.json files under {collection_root}")
        if len(matching_paths) > 1:
            raise ValueError(
                f"Collection dir {collection_root} contains multiple run indexes; specify --tool-family or --collection-index."
            )
        return matching_paths[0], None

    if tool_family:
        matching_entries = [item for item in browser_collections if item.get("tool_family") == tool_family]
        if not matching_entries:
            raise FileNotFoundError(f"No collections indexed for tool family {tool_family}")
        selected_entry = matching_entries[0]
        raw_path = selected_entry.get("collection_index_artifact")
        if not isinstance(raw_path, str) or not raw_path:
            raise ValueError(f"Latest {tool_family} collection is missing collection_index_artifact")
        return Path(raw_path).expanduser().resolve(), selected_entry

    raise ValueError("Provide --tool-family, --collection-dir, or --collection-index.")


def _resolve_evaluation_bypass_artifact(entry: dict[str, Any]) -> str | None:
    run_dir = entry.get("run_dir")
    if not isinstance(run_dir, str) or not run_dir:
        return None
    bypass_path = Path(run_dir).expanduser().resolve() / "evaluation_bypass_audit.json"
    if bypass_path.exists():
        return str(bypass_path)
    return None


def _enrich_run_entry(entry: dict[str, Any], *, collection: dict[str, Any] | None = None) -> dict[str, Any]:
    enriched = dict(entry)
    bypass_path = _resolve_evaluation_bypass_artifact(enriched)
    enriched["has_evaluation_bypass"] = bool(bypass_path)
    if bypass_path:
        enriched["evaluation_bypass_artifact"] = bypass_path
        existing_exceptions = enriched.get("policy_exceptions")
        policy_exceptions = [item for item in existing_exceptions if isinstance(item, str)] if isinstance(existing_exceptions, list) else []
        if "evaluation_bypass" not in policy_exceptions:
            policy_exceptions.append("evaluation_bypass")
        enriched["policy_exceptions"] = policy_exceptions
    if isinstance(collection, dict):
        for key in (
            "tool_family",
            "collection_dir",
            "collection_index_artifact",
            "collection_landing_artifact",
        ):
            value = collection.get(key)
            if isinstance(value, str) and value:
                enriched[key] = value
    return ai_os_browser.annotate_run_attention(enriched)


def _matches_substring(value: Any, needle: str | None) -> bool:
    if not needle:
        return True
    if not isinstance(value, str):
        return False
    return needle.casefold() in value.casefold()


def _filter_run_entries(
    entries: list[dict[str, Any]],
    *,
    status: str | None,
    run_command: str | None,
    label_contains: str | None,
    path_contains: str | None,
    evaluation_verdict: str | None,
    has_evaluation_bypass: bool,
    needs_attention: bool,
    sort_by_attention: bool,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        if status and item.get("status") != status:
            continue
        if run_command and item.get("command") != run_command:
            continue
        if evaluation_verdict and item.get("evaluation_verdict") != evaluation_verdict:
            continue
        if label_contains and not _matches_substring(item.get("label"), label_contains):
            continue
        if path_contains and not (
            _matches_substring(item.get("run_dir"), path_contains)
            or _matches_substring(item.get("landing_artifact"), path_contains)
            or _matches_substring(item.get("collection_dir"), path_contains)
        ):
            continue
        if has_evaluation_bypass and not item.get("has_evaluation_bypass"):
            continue
        if needs_attention and int(item.get("attention_score") or 0) <= 0:
            continue
        filtered.append(item)
    if sort_by_attention:
        filtered.sort(
            key=lambda item: (
                int(item.get("attention_score") or 0),
                str(item.get("updated_at") or ""),
            ),
            reverse=True,
        )
    else:
        filtered.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    return filtered


def _load_browser_runs(
    *,
    browser_collections: list[dict[str, Any]],
    tool_family: str | None = None,
) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for collection_entry in browser_collections:
        if not isinstance(collection_entry, dict):
            continue
        if tool_family and collection_entry.get("tool_family") != tool_family:
            continue
        raw_index_path = collection_entry.get("collection_index_artifact")
        if not isinstance(raw_index_path, str) or not raw_index_path:
            continue
        collection_index_path = Path(raw_index_path).expanduser().resolve()
        try:
            collection_payload = _load_json_object(collection_index_path)
        except Exception:
            continue
        entries = collection_payload.get("entries")
        if not isinstance(entries, list):
            continue
        runs.extend(
            _enrich_run_entry(item, collection=collection_entry) for item in entries if isinstance(item, dict)
        )
    runs.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    return runs


def _collection_metadata(
    *,
    collection_index_path: Path,
    browser_entry: dict[str, Any] | None,
) -> dict[str, Any]:
    tool_family = ai_os_browser.TOOL_FAMILY_BY_INDEX.get(collection_index_path.name)
    landing_name = ai_os_browser.LANDING_FILES_BY_INDEX.get(collection_index_path.name)
    collection_dir = collection_index_path.parent
    payload: dict[str, Any] = {
        "tool_family": tool_family,
        "collection_dir": str(collection_dir),
        "collection_index_artifact": str(collection_index_path),
        "collection_landing_artifact": str(collection_dir / landing_name) if landing_name else None,
    }
    if isinstance(browser_entry, dict):
        for key in (
            "updated_at",
            "latest_label",
            "latest_status",
            "latest_run_dir",
            "latest_landing_artifact",
            "run_count",
        ):
            if key in browser_entry:
                payload[key] = browser_entry[key]
    return payload


def _print_overview_text(payload: dict[str, Any]) -> None:
    print(f"status: {payload['status']}")
    print(f"browser_root: {payload['browser_root']}")
    print(f"browser_index_artifact: {payload['browser_index_artifact']}")
    print(f"browser_landing_artifact: {payload['browser_landing_artifact']}")
    if payload.get("health_index_artifact"):
        print(f"health_index_artifact: {payload['health_index_artifact']}")
    if payload.get("health_landing_artifact"):
        print(f"health_landing_artifact: {payload['health_landing_artifact']}")
    health_summary = payload.get("health_summary") or {}
    if isinstance(health_summary, dict) and health_summary:
        print(f"risk_run_count: {health_summary.get('risk_run_count', 0)}")
        print(f"attention_run_count: {health_summary.get('attention_run_count', 0)}")
        print(f"evaluation_bypass_count: {health_summary.get('evaluation_bypass_count', 0)}")
        print(f"stale_collection_count: {health_summary.get('stale_collection_count', 0)}")
        if health_summary.get("latest_run_updated_at"):
            print(f"latest_run_updated_at: {health_summary['latest_run_updated_at']}")
    for message in payload.get("messages", []):
        print(f"{message['level']}: {message['code']}: {message['text']}")
    for item in payload.get("collections", []):
        print(f"- {item.get('tool_family') or 'unknown'}: {item.get('collection_dir')}")


def _print_runs_text(payload: dict[str, Any]) -> None:
    print(f"status: {payload['status']}")
    print(f"collection_dir: {payload['selected_collection']['collection_dir']}")
    if payload["selected_collection"].get("collection_landing_artifact"):
        print(f"collection_landing_artifact: {payload['selected_collection']['collection_landing_artifact']}")
    if payload.get("health_landing_artifact"):
        print(f"health_landing_artifact: {payload['health_landing_artifact']}")
    for message in payload.get("messages", []):
        print(f"{message['level']}: {message['code']}: {message['text']}")
    for item in payload.get("runs", []):
        print(
            f"- {item.get('command') or 'unknown'} | {item.get('status') or 'unknown'} | "
            f"{item.get('attention_level') or 'none'} | {item.get('run_dir') or 'unknown'}"
        )


def _print_run_text(payload: dict[str, Any]) -> None:
    print(f"status: {payload['status']}")
    print(f"collection_dir: {payload['selected_collection']['collection_dir']}")
    print(f"run_dir: {payload['selected_run'].get('run_dir') or 'unknown'}")
    print(f"attention_level: {payload['selected_run'].get('attention_level') or 'none'}")
    if payload["selected_run"].get("landing_artifact"):
        print(f"landing_artifact: {payload['selected_run']['landing_artifact']}")
    if payload["selected_run"].get("evaluation_bypass_artifact"):
        print(f"evaluation_bypass_artifact: {payload['selected_run']['evaluation_bypass_artifact']}")
    if payload.get("health_landing_artifact"):
        print(f"health_landing_artifact: {payload['health_landing_artifact']}")
    for message in payload.get("messages", []):
        print(f"{message['level']}: {message['code']}: {message['text']}")
    if isinstance(payload.get("landing_markdown"), str) and payload["landing_markdown"]:
        print("")
        print(payload["landing_markdown"].rstrip())


def _print_search_text(payload: dict[str, Any]) -> None:
    print(f"status: {payload['status']}")
    print(f"browser_root: {payload['browser_root']}")
    print(f"matched_runs: {payload['run_count']}")
    if payload.get("health_landing_artifact"):
        print(f"health_landing_artifact: {payload['health_landing_artifact']}")
    for message in payload.get("messages", []):
        print(f"{message['level']}: {message['code']}: {message['text']}")
    for item in payload.get("runs", []):
        line = (
            f"- {item.get('tool_family') or 'unknown'} | {item.get('command') or 'unknown'} | "
            f"{item.get('status') or 'unknown'} | {item.get('attention_level') or 'none'} | {item.get('run_dir') or 'unknown'}"
        )
        if item.get("has_evaluation_bypass"):
            line += " | evaluation_bypass"
        print(line)


def _print_health_text(payload: dict[str, Any]) -> None:
    print(f"status: {payload['status']}")
    print(f"browser_root: {payload['browser_root']}")
    print(f"run_count: {payload['run_count']}")
    print(f"collection_count: {payload['collection_count']}")
    print(f"evaluation_bypass_count: {payload['evaluation_bypass_count']}")
    print(f"risk_run_count: {payload['risk_run_count']}")
    print(f"attention_run_count: {payload.get('attention_run_count', 0)}")
    print(f"stale_collection_count: {payload.get('stale_collection_count', 0)}")
    print(f"unknown_collection_count: {payload.get('unknown_collection_count', 0)}")
    if payload.get("latest_run_updated_at"):
        print(f"latest_run_updated_at: {payload['latest_run_updated_at']}")
    if payload.get("latest_collection_updated_at"):
        print(f"latest_collection_updated_at: {payload['latest_collection_updated_at']}")
    run_recency_counts = payload.get("run_recency_counts") or {}
    if isinstance(run_recency_counts, dict) and run_recency_counts:
        print(f"run_recency_counts: {run_recency_counts}")
    collection_recency_counts = payload.get("collection_recency_counts") or {}
    if isinstance(collection_recency_counts, dict) and collection_recency_counts:
        print(f"collection_recency_counts: {collection_recency_counts}")
    if payload.get("health_index_artifact"):
        print(f"health_index_artifact: {payload['health_index_artifact']}")
    if payload.get("health_landing_artifact"):
        print(f"health_landing_artifact: {payload['health_landing_artifact']}")
    for message in payload.get("messages", []):
        print(f"{message['level']}: {message['code']}: {message['text']}")
    for item in payload.get("tool_family_health", []):
        print(
            f"- {item.get('tool_family') or 'unknown'} | runs={item.get('run_count', 0)} | "
            f"bypass={item.get('evaluation_bypass_count', 0)} | "
            f"attention={item.get('attention_run_count', 0)} | "
            f"recency={item.get('recency_bucket') or 'unknown'} | "
            f"status={item.get('status_counts') or {}}"
        )


def _add_run_filter_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--status", default=None, help="Filter by run status, e.g. ok, warn, error.")
    parser.add_argument("--run-command", default=None, help="Filter by run command, e.g. apply, verify, deploy.")
    parser.add_argument("--label-contains", default=None, help="Case-insensitive substring filter on the run label.")
    parser.add_argument(
        "--path-contains",
        default=None,
        help="Case-insensitive substring filter on run_dir, landing_artifact, or collection_dir.",
    )
    parser.add_argument(
        "--evaluation-verdict",
        default=None,
        help="Filter by evaluation verdict when the run index carries it.",
    )
    parser.add_argument(
        "--has-evaluation-bypass",
        action="store_true",
        help="Only include runs whose run dir contains evaluation_bypass_audit.json.",
    )
    parser.add_argument(
        "--needs-attention",
        action="store_true",
        help="Only include runs with a non-zero attention score.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    overview = subparsers.add_parser("overview", help="Show the top-level AI OS browser index.")
    overview.add_argument("--browser-root", default=None, help="Browser/output root. Defaults to output/ or CRM_AI_BROWSER_ROOT.")
    overview.add_argument("--refresh", action="store_true", help="Refresh ai_os_collections_index.json before reading it.")
    overview.add_argument("--top-k", type=int, default=10, help="Max collections to include.")
    overview.add_argument("--json", action="store_true", help="Print JSON output.")

    collections = subparsers.add_parser("collections", help="List indexed AI OS collections.")
    collections.add_argument("--browser-root", default=None, help="Browser/output root. Defaults to output/ or CRM_AI_BROWSER_ROOT.")
    collections.add_argument("--refresh", action="store_true", help="Refresh ai_os_collections_index.json before reading it.")
    collections.add_argument("--tool-family", default=None, help="Optional tool family filter, e.g. salesforce_dashboard.")
    collections.add_argument("--top-k", type=int, default=10, help="Max collections to include.")
    collections.add_argument("--json", action="store_true", help="Print JSON output.")

    health = subparsers.add_parser("health", help="Summarize AI OS run health across indexed collections.")
    health.add_argument("--browser-root", default=None, help="Browser/output root. Defaults to output/ or CRM_AI_BROWSER_ROOT.")
    health.add_argument("--refresh", action="store_true", help="Refresh ai_os_collections_index.json before summarizing.")
    health.add_argument("--tool-family", default=None, help="Optional tool family filter, e.g. salesforce_dashboard.")
    health.add_argument("--top-k", type=int, default=10, help="Max risky runs to include.")
    health.add_argument("--json", action="store_true", help="Print JSON output.")

    search = subparsers.add_parser("search", help="Search indexed runs across AI OS collections.")
    search.add_argument("--browser-root", default=None, help="Browser/output root. Defaults to output/ or CRM_AI_BROWSER_ROOT.")
    search.add_argument("--refresh", action="store_true", help="Refresh ai_os_collections_index.json before searching.")
    search.add_argument("--tool-family", default=None, help="Optional tool family filter, e.g. salesforce_dashboard.")
    _add_run_filter_arguments(search)
    search.add_argument("--top-k", type=int, default=25, help="Max runs to include.")
    search.add_argument("--json", action="store_true", help="Print JSON output.")

    runs = subparsers.add_parser("runs", help="Inspect recent runs in one indexed collection.")
    runs.add_argument("--browser-root", default=None, help="Browser/output root. Defaults to output/ or CRM_AI_BROWSER_ROOT.")
    runs.add_argument("--refresh", action="store_true", help="Refresh ai_os_collections_index.json before resolving the collection.")
    runs.add_argument("--tool-family", default=None, help="Tool family to inspect. If used alone, selects the latest indexed collection.")
    runs.add_argument("--collection-dir", default=None, help="Explicit collection directory containing a *_run_index.json file.")
    runs.add_argument("--collection-index", default=None, help="Explicit path to a *_run_index.json file.")
    _add_run_filter_arguments(runs)
    runs.add_argument("--top-k", type=int, default=10, help="Max runs to include.")
    runs.add_argument("--json", action="store_true", help="Print JSON output.")

    run = subparsers.add_parser("run", help="Inspect one run from an indexed collection.")
    run.add_argument("--browser-root", default=None, help="Browser/output root. Defaults to output/ or CRM_AI_BROWSER_ROOT.")
    run.add_argument("--refresh", action="store_true", help="Refresh ai_os_collections_index.json before resolving the collection.")
    run.add_argument("--tool-family", default=None, help="Tool family to inspect. If used alone, selects the latest indexed collection.")
    run.add_argument("--collection-dir", default=None, help="Explicit collection directory containing a *_run_index.json file.")
    run.add_argument("--collection-index", default=None, help="Explicit path to a *_run_index.json file.")
    run.add_argument("--run-dir", default=None, help="Explicit run directory to inspect from the selected collection.")
    _add_run_filter_arguments(run)
    run.add_argument("--position", type=int, default=1, help="1-based position within the selected collection when --run-dir is omitted.")
    run.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    browser_root = _resolve_browser_root(getattr(args, "browser_root", None))

    try:
        browser_index_path, browser_overview_path, browser_payload = _load_browser_index(
            browser_root=browser_root,
            refresh=getattr(args, "refresh", False),
        )
    except Exception as exc:
        result = make_result(
            status="error",
            command=args.command,
            messages=[make_message("error", "browser_index_load_failed", str(exc))],
            browser_root=str(browser_root),
        )
        if getattr(args, "json", False):
            print(json.dumps(result, indent=2))
        else:
            _print_overview_text(result)
        return 1

    collections = _filter_collections(
        browser_payload.get("collections", []),
        tool_family=getattr(args, "tool_family", None),
        top_k=max(1, getattr(args, "top_k", 10)),
    )
    shared_artifacts = [
        {"type": "ai_os_collections_index", "path": str(browser_index_path)},
        {"type": "ai_os_overview", "path": str(browser_overview_path)},
    ]
    health_index_path, health_overview_path = ai_os_browser.resolve_ai_os_health_paths(browser_root=browser_root)
    if health_index_path.exists():
        shared_artifacts.append({"type": "ai_os_health", "path": str(health_index_path)})
    if health_overview_path.exists():
        shared_artifacts.append({"type": "ai_os_health_overview", "path": str(health_overview_path)})

    if args.command in {"overview", "collections"}:
        code = "overview_ready" if args.command == "overview" else "collections_ready"
        noun = "collection(s)" if args.command == "overview" else "filtered collection(s)"
        result = make_result(
            status="ok",
            command=args.command,
            messages=[
                make_message(
                    "info",
                    code,
                    f"Loaded {len(collections)} {noun} from the AI OS browser.",
                )
            ],
            artifacts=shared_artifacts,
            browser_root=str(browser_root),
            browser_index_artifact=str(browser_index_path),
            browser_landing_artifact=str(browser_overview_path),
            health_index_artifact=str(health_index_path),
            health_landing_artifact=str(health_overview_path),
            health_summary=browser_payload.get("health_summary") or {},
            collections=collections,
            collection_count=len(collections),
            filters={"tool_family": getattr(args, "tool_family", None)},
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_overview_text(result)
        return 0

    if args.command == "health":
        browser_collections = browser_payload.get("collections", [])
        if not isinstance(browser_collections, list):
            browser_collections = []
        filtered_collections = [item for item in browser_collections if isinstance(item, dict)]
        if args.tool_family:
            filtered_collections = [item for item in filtered_collections if item.get("tool_family") == args.tool_family]
        health_summary = ai_os_browser.summarize_ai_os_health(
            browser_root=browser_root,
            collections=filtered_collections,
            tool_family=args.tool_family,
            top_k=max(1, args.top_k),
        )
        health_index_path, health_markdown_path = ai_os_browser.write_ai_os_health_artifacts(
            browser_root=browser_root,
            summary=health_summary,
        )
        result = make_result(
            status="ok",
            command="health",
            messages=[
                make_message(
                    "info",
                    "health_ready",
                    f"Summarized {health_summary.get('run_count', 0)} run(s) across {len(filtered_collections)} collection(s).",
                )
            ],
            artifacts=shared_artifacts
            + [
                {"type": "ai_os_health", "path": str(health_index_path)},
                {"type": "ai_os_health_overview", "path": str(health_markdown_path)},
            ],
            browser_index_artifact=str(browser_index_path),
            browser_landing_artifact=str(browser_overview_path),
            health_index_artifact=str(health_index_path),
            health_landing_artifact=str(health_markdown_path),
            **health_summary,
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_health_text(result)
        return 0

    if args.command == "search":
        browser_collections = browser_payload.get("collections", [])
        if not isinstance(browser_collections, list):
            browser_collections = []
        matched_runs = _filter_run_entries(
            _load_browser_runs(browser_collections=browser_collections, tool_family=args.tool_family),
            status=args.status,
            run_command=args.run_command,
            label_contains=args.label_contains,
            path_contains=args.path_contains,
            evaluation_verdict=args.evaluation_verdict,
            has_evaluation_bypass=args.has_evaluation_bypass,
            needs_attention=args.needs_attention,
            sort_by_attention=True,
        )
        matched_runs = matched_runs[: max(1, args.top_k)]
        result = make_result(
            status="ok",
            command="search",
            messages=[
                make_message(
                    "info",
                    "search_ready",
                    f"Matched {len(matched_runs)} run(s) across the AI OS browser.",
                )
            ],
            artifacts=shared_artifacts,
            browser_root=str(browser_root),
            browser_index_artifact=str(browser_index_path),
            browser_landing_artifact=str(browser_overview_path),
            health_index_artifact=str(health_index_path),
            health_landing_artifact=str(health_overview_path),
            runs=matched_runs,
            run_count=len(matched_runs),
            filters={
                "tool_family": args.tool_family,
                "status": args.status,
                "run_command": args.run_command,
                "label_contains": args.label_contains,
                "path_contains": args.path_contains,
                "evaluation_verdict": args.evaluation_verdict,
                "has_evaluation_bypass": args.has_evaluation_bypass,
                "needs_attention": args.needs_attention,
            },
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_search_text(result)
        return 0

    try:
        collection_index_path, browser_entry = _resolve_collection_index_path(
            browser_collections=browser_payload.get("collections", []),
            tool_family=args.tool_family,
            collection_dir=args.collection_dir,
            collection_index=args.collection_index,
        )
        collection_payload = _load_json_object(collection_index_path)
    except Exception as exc:
        result = make_result(
            status="error",
            command=args.command,
            messages=[make_message("error", "collection_resolution_failed", str(exc))],
            artifacts=shared_artifacts,
            browser_root=str(browser_root),
            browser_index_artifact=str(browser_index_path),
            browser_landing_artifact=str(browser_overview_path),
            health_index_artifact=str(health_index_path),
            health_landing_artifact=str(health_overview_path),
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_overview_text(result)
        return 1

    entries = collection_payload.get("entries")
    if not isinstance(entries, list):
        entries = []
    selected_collection = _collection_metadata(collection_index_path=collection_index_path, browser_entry=browser_entry)
    enriched_entries = [_enrich_run_entry(item, collection=selected_collection) for item in entries if isinstance(item, dict)]
    filtered_entries = _filter_run_entries(
        enriched_entries,
        status=args.status,
        run_command=args.run_command,
        label_contains=args.label_contains,
        path_contains=args.path_contains,
        evaluation_verdict=args.evaluation_verdict,
        has_evaluation_bypass=args.has_evaluation_bypass,
        needs_attention=args.needs_attention,
        sort_by_attention=args.needs_attention,
    )
    collection_landing_artifact = selected_collection.get("collection_landing_artifact")
    artifacts = list(shared_artifacts)
    artifacts.append({"type": collection_index_path.stem, "path": str(collection_index_path)})
    if isinstance(collection_landing_artifact, str) and collection_landing_artifact:
        artifacts.append({"type": Path(collection_landing_artifact).stem, "path": collection_landing_artifact})

    if args.command == "runs":
        result = make_result(
            status="ok",
            command="runs",
            messages=[
                make_message(
                    "info",
                    "runs_ready",
                    f"Loaded {min(len(filtered_entries), max(1, args.top_k))} run(s) from {selected_collection['collection_dir']}.",
                )
            ],
            artifacts=artifacts,
            browser_root=str(browser_root),
            browser_index_artifact=str(browser_index_path),
            browser_landing_artifact=str(browser_overview_path),
            health_index_artifact=str(health_index_path),
            health_landing_artifact=str(health_overview_path),
            selected_collection=selected_collection,
            runs=filtered_entries[: max(1, args.top_k)],
            run_count=min(len(filtered_entries), max(1, args.top_k)),
            filters={
                "status": args.status,
                "run_command": args.run_command,
                "label_contains": args.label_contains,
                "path_contains": args.path_contains,
                "evaluation_verdict": args.evaluation_verdict,
                "has_evaluation_bypass": args.has_evaluation_bypass,
                "needs_attention": args.needs_attention,
            },
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_runs_text(result)
        return 0

    position = max(1, getattr(args, "position", 1))
    selected_run: dict[str, Any] | None = None
    if args.run_dir:
        target_run_dir = str(Path(args.run_dir).expanduser().resolve())
        for entry in filtered_entries:
            if isinstance(entry, dict) and entry.get("run_dir") == target_run_dir:
                selected_run = entry
                break
        if selected_run is None:
            result = make_result(
                status="error",
                command="run",
                messages=[make_message("error", "run_not_found", f"No run matched {target_run_dir}.")],
                artifacts=artifacts,
                browser_root=str(browser_root),
                browser_index_artifact=str(browser_index_path),
                browser_landing_artifact=str(browser_overview_path),
                health_index_artifact=str(health_index_path),
                health_landing_artifact=str(health_overview_path),
                selected_collection=selected_collection,
            )
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                _print_runs_text(result)
            return 1
    elif filtered_entries:
        selected_run = filtered_entries[position - 1] if position - 1 < len(filtered_entries) else None

    if selected_run is None:
        result = make_result(
            status="error",
            command="run",
            messages=[
                make_message(
                    "error",
                    "run_selection_failed",
                    f"No run available at position {position} in {selected_collection['collection_dir']} after filters.",
                )
            ],
            artifacts=artifacts,
            browser_root=str(browser_root),
            browser_index_artifact=str(browser_index_path),
            browser_landing_artifact=str(browser_overview_path),
            health_index_artifact=str(health_index_path),
            health_landing_artifact=str(health_overview_path),
            selected_collection=selected_collection,
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            _print_runs_text(result)
        return 1

    landing_markdown = None
    landing_artifact = selected_run.get("landing_artifact")
    if isinstance(landing_artifact, str) and landing_artifact:
        landing_path = Path(landing_artifact).expanduser().resolve()
        if landing_path.exists():
            landing_markdown = landing_path.read_text(encoding="utf-8")
            artifacts.append({"type": landing_path.stem, "path": str(landing_path)})

    result = make_result(
        status="ok",
        command="run",
        messages=[
            make_message(
                "info",
                "run_ready",
                f"Loaded run {selected_run.get('run_dir') or position} from {selected_collection['collection_dir']}.",
            )
        ],
        artifacts=artifacts,
        browser_root=str(browser_root),
        browser_index_artifact=str(browser_index_path),
        browser_landing_artifact=str(browser_overview_path),
        health_index_artifact=str(health_index_path),
        health_landing_artifact=str(health_overview_path),
        selected_collection=selected_collection,
        selected_run=selected_run,
        landing_markdown=landing_markdown,
        filters={
            "status": args.status,
            "run_command": args.run_command,
            "label_contains": args.label_contains,
            "path_contains": args.path_contains,
            "evaluation_verdict": args.evaluation_verdict,
            "has_evaluation_bypass": args.has_evaluation_bypass,
        },
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        _print_run_text(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
