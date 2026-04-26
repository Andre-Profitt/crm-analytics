#!/usr/bin/env python3
"""Back up and delete all CRM Analytics dashboards created by Andre Profitt.

This is a destructive reset utility for the live SimCorp org.
It snapshots dashboard JSON first, then deletes the matching Wave dashboards.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm_analytics_helpers import (  # noqa: E402
    _sf_api,
    build_dashboard_state,
    get_auth,
    get_dashboard_state,
    normalize_dashboard_state_for_patch,
    pg,
    section_label,
)

API_VERSION = "v66.0"
TARGET_USER_ID = "005QA000003DUwWYAW"
TARGET_USER_NAME = "Andre Profitt"


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip())
    return cleaned.strip("_").lower() or "asset"


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def list_dashboards(inst: str, tok: str) -> list[dict[str, Any]]:
    dashboards: list[dict[str, Any]] = []
    path = f"/services/data/{API_VERSION}/wave/dashboards?pageSize=200"
    while path:
        payload = _sf_api(inst, tok, "GET", path)
        dashboards.extend(payload.get("dashboards", []))
        path = payload.get("nextPageUrl")
    return dashboards


def matches_target(dashboard: dict[str, Any]) -> bool:
    created_by = dashboard.get("createdBy") or {}
    if created_by.get("id") == TARGET_USER_ID:
        return True
    return created_by.get("name") == TARGET_USER_NAME


def backup_dashboard(inst: str, tok: str, backup_root: Path, dashboard: dict[str, Any]) -> dict[str, Any]:
    dash_id = dashboard["id"]
    label = html.unescape(dashboard.get("label") or dash_id)
    safe_dir = backup_root / f"{slugify(label)}_{dash_id.lower()}"
    full = get_dashboard_state(inst, tok, dash_id)
    write_json(safe_dir / "dashboard.json", full)
    summary = {
        "id": dash_id,
        "label": label,
        "name": dashboard.get("name"),
        "createdBy": dashboard.get("createdBy"),
        "createdDate": dashboard.get("createdDate"),
        "lastModifiedBy": dashboard.get("lastModifiedBy"),
        "lastModifiedDate": dashboard.get("lastModifiedDate"),
        "folder": dashboard.get("folder"),
        "description": html.unescape(dashboard.get("description") or ""),
        "url": full.get("url"),
        "assetSharingUrl": full.get("assetSharingUrl"),
        "widgetCount": len((full.get("state") or {}).get("widgets", {})),
        "stepCount": len((full.get("state") or {}).get("steps", {})),
    }
    write_json(safe_dir / "summary.json", summary)
    return summary


def delete_dashboard(inst: str, tok: str, dashboard_id: str) -> None:
    _sf_api(inst, tok, "DELETE", f"/services/data/{API_VERSION}/wave/dashboards/{dashboard_id}")

def patch_dashboard_state(inst: str, tok: str, dashboard_id: str, state: dict[str, Any]) -> None:
    normalized_state = normalize_dashboard_state_for_patch(
        state,
        strip_page_labels=False,
    )
    body = json.dumps({"state": normalized_state}).encode("utf-8")
    req = urllib.request.Request(
        f"{inst}/services/data/{API_VERSION}/wave/dashboards/{dashboard_id}",
        data=body,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req):
        return


def is_dashboard_link_widget(widget: dict[str, Any]) -> bool:
    params = widget.get("parameters") or {}
    destination_link = params.get("destinationLink") or {}
    if widget.get("type") == "link":
        return True
    if params.get("destinationType") == "dashboard":
        return True
    if params.get("linkType") == "dashboard":
        return True
    if isinstance(destination_link, dict) and (
        destination_link.get("id")
        or destination_link.get("dashboard")
        or destination_link.get("destinationType") == "dashboard"
    ):
        return True
    return False


def strip_dashboard_links(inst: str, tok: str, dashboard_id: str) -> int:
    full = get_dashboard_state(inst, tok, dashboard_id)
    state = full.get("state") or {}
    widgets = state.get("widgets") or {}
    remove_names = {
        name for name, widget in widgets.items() if is_dashboard_link_widget(widget)
    }
    if not remove_names:
        return 0

    state["widgets"] = {
        name: widget for name, widget in widgets.items() if name not in remove_names
    }
    for grid in state.get("gridLayouts", []) or []:
        for page in grid.get("pages", []) or []:
            page["widgets"] = [
                widget_ref
                for widget_ref in page.get("widgets", []) or []
                if widget_ref.get("name") not in remove_names
            ]

    patch_dashboard_state(inst, tok, dashboard_id, state)
    return len(remove_names)


def blank_dashboard_state(label: str) -> dict[str, Any]:
    widgets = {"reset_notice": section_label(f"Reset placeholder for {label}")}
    layout = {
        "name": "reset_layout",
        "numColumns": 12,
        "pages": [
            pg(
                "reset",
                "Reset",
                [
                    {
                        "name": "reset_notice",
                        "row": 0,
                        "column": 0,
                        "colspan": 12,
                        "rowspan": 2,
                    }
                ],
            )
        ],
    }
    return build_dashboard_state({}, widgets, layout)


def overwrite_dashboard_with_blank(inst: str, tok: str, dashboard_id: str, label: str) -> None:
    patch_dashboard_state(inst, tok, dashboard_id, blank_dashboard_state(label))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backup-root",
        default=str(ROOT / "docs" / "generated" / "deleted_dashboard_backups"),
        help="Directory where dashboard backups and manifests are written.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List and back up matches without deleting them.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    inst, tok = get_auth()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    backup_root = Path(args.backup_root).resolve() / f"andre_profitt_reset_{ts}"
    backup_root.mkdir(parents=True, exist_ok=True)

    dashboards = list_dashboards(inst, tok)
    targets = [d for d in dashboards if matches_target(d)]
    print(f"Found {len(targets)} Andre-owned dashboard(s)")

    manifest: list[dict[str, Any]] = []
    for dashboard in sorted(targets, key=lambda d: (d.get("label") or "", d["id"])):
        summary = backup_dashboard(inst, tok, backup_root, dashboard)
        summary["status"] = "backed_up"
        print(f"BACKUP {summary['id']} {summary['label']}")
        manifest.append(summary)

    if not args.dry_run:
        remaining_targets = sorted(targets, key=lambda d: (d.get("label") or "", d["id"]))
        while remaining_targets:
            deleted_this_pass = 0
            for dashboard in remaining_targets:
                summary = next(item for item in manifest if item["id"] == dashboard["id"])
                if summary.get("status") == "deleted":
                    continue
                try:
                    delete_dashboard(inst, tok, summary["id"])
                    summary["status"] = "deleted"
                    deleted_this_pass += 1
                    print(f"DELETE {summary['id']} {summary['label']}")
                except RuntimeError as exc:
                    summary["status"] = "delete_blocked"
                    summary["error"] = str(exc)
                    print(f"BLOCK {summary['id']} {summary['label']} :: {exc}")

            remaining_targets = [d for d in list_dashboards(inst, tok) if matches_target(d)]
            if not remaining_targets:
                break
            if deleted_this_pass:
                continue

            print("No delete progress this pass. Overwriting remaining dashboards with blank state...")
            reset_total = 0
            for dashboard in remaining_targets:
                summary = next(item for item in manifest if item["id"] == dashboard["id"])
                try:
                    overwrite_dashboard_with_blank(
                        inst,
                        tok,
                        dashboard["id"],
                        html.unescape(dashboard.get("label") or dashboard["id"]),
                    )
                    summary["status"] = "overwritten_blank"
                    reset_total += 1
                    print(f"BLANK {dashboard['id']} {html.unescape(dashboard.get('label') or dashboard['id'])}")
                except Exception as exc:
                    summary["status"] = "blank_overwrite_failed"
                    summary["error"] = str(exc)
                    print(f"BLANK_FAIL {dashboard['id']} :: {exc}")
            if not reset_total:
                print("Blank overwrite made no progress; stopping with remaining blockers.")
                break

    remaining = [d for d in list_dashboards(inst, tok) if matches_target(d)]
    result = {
        "targetUserId": TARGET_USER_ID,
        "targetUserName": TARGET_USER_NAME,
        "timestamp": ts,
        "dryRun": args.dry_run,
        "deletedCount": 0 if args.dry_run else len(manifest),
        "remainingCount": len(remaining),
        "remaining": [
            {"id": d["id"], "label": html.unescape(d.get("label") or ""), "name": d.get("name")}
            for d in remaining
        ],
        "dashboards": manifest,
    }
    write_json(backup_root / "manifest.json", result)

    print(f"Remaining Andre-owned dashboards: {len(remaining)}")
    print(f"Backup manifest: {backup_root / 'manifest.json'}")
    return 0 if (args.dry_run or not remaining) else 1


if __name__ == "__main__":
    raise SystemExit(main())
