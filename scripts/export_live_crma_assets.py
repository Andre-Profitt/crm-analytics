#!/usr/bin/env python3
"""Export live CRM Analytics assets through the Wave REST APIs.

Use this as the source-control fallback when Metadata API retrieval is blocked.
It snapshots dashboard JSON plus referenced dataset metadata/XMD into the repo.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm_analytics_helpers import (  # noqa: E402
    _sf_api,
    get_auth,
    get_dashboard_id,
    get_dashboard_state,
    get_dataset_id,
)

API_VERSION = "v66.0"
LOAD_PATTERN = re.compile(r'load\s+"([^"/]+)"')


def slugify(value: str) -> str:
    """Convert labels into stable filesystem-safe directory names."""
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip())
    return cleaned.strip("_").lower() or "asset"


def write_json(path: Path, payload: Any) -> None:
    """Write formatted JSON to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def _make_artifact(kind: str, path: Path) -> dict[str, str]:
    return {"kind": kind, "path": str(path)}


def _make_result(
    *,
    status: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "export_live_crma_assets",
        "lane": "live_inventory",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def extract_dataset_names(dashboard: dict[str, Any]) -> list[str]:
    """Find dataset names referenced by dashboard steps."""
    names: set[str] = set()
    state = dashboard.get("state", {})
    for step in state.get("steps", {}).values():
        query = step.get("query", "")
        if isinstance(query, dict):
            query = json.dumps(query, sort_keys=True)
        if not isinstance(query, str):
            continue
        for match in LOAD_PATTERN.finditer(html.unescape(query)):
            names.add(match.group(1))
    return sorted(names)


def extract_page_labels(dashboard: dict[str, Any]) -> list[str]:
    """Find page labels from the dashboard grid layout."""
    state = dashboard.get("state", {})
    labels: list[str] = []
    for grid_layout in state.get("gridLayouts", []) or []:
        for page in grid_layout.get("pages", []) or []:
            label = page.get("label")
            if label:
                labels.append(html.unescape(label))
    return labels


def fetch_dataset_bundle(inst: str, tok: str, dataset_name: str) -> dict[str, Any]:
    """Fetch dataset metadata plus main XMD."""
    dataset_id = get_dataset_id(inst, tok, dataset_name)
    if not dataset_id:
        return {"name": dataset_name, "error": "dataset not found"}

    dataset = _sf_api(inst, tok, "GET", f"/services/data/{API_VERSION}/wave/datasets/{dataset_id}")
    version_id = dataset.get("currentVersionId")
    result: dict[str, Any] = {
        "name": dataset_name,
        "id": dataset_id,
        "dataset": dataset,
    }
    if version_id:
        xmd = _sf_api(
            inst,
            tok,
            "GET",
            f"/services/data/{API_VERSION}/wave/datasets/{dataset_id}/versions/{version_id}/xmds/main",
        )
        result["xmd"] = xmd
    else:
        result["warning"] = "dataset has no currentVersionId"
    return result


def export_dashboard(inst: str, tok: str, label: str, output_root: Path) -> dict[str, Any]:
    """Export one dashboard and its referenced datasets."""
    dashboard_id = get_dashboard_id(inst, tok, label)
    if not dashboard_id:
        raise RuntimeError(f"dashboard not found for label: {label}")

    dashboard = get_dashboard_state(inst, tok, dashboard_id)
    state = dashboard.get("state", {})
    asset_dir = output_root / slugify(label)
    datasets_dir = asset_dir / "datasets"

    write_json(asset_dir / "dashboard.json", dashboard)

    dataset_names = extract_dataset_names(dashboard)
    dataset_index: list[dict[str, Any]] = []
    for dataset_name in dataset_names:
        bundle = fetch_dataset_bundle(inst, tok, dataset_name)
        dataset_slug = slugify(dataset_name)
        if "dataset" in bundle:
            write_json(datasets_dir / dataset_slug / "dataset.json", bundle["dataset"])
        if "xmd" in bundle:
            write_json(datasets_dir / dataset_slug / "xmd_main.json", bundle["xmd"])
        dataset_index.append(
            {
                "name": dataset_name,
                "id": bundle.get("id"),
                "status": "ok" if "dataset" in bundle else "missing",
                "warning": bundle.get("warning"),
                "error": bundle.get("error"),
            }
        )

    summary = {
        "label": label,
        "id": dashboard_id,
        "name": dashboard.get("name"),
        "pages": extract_page_labels(dashboard),
        "widgetCount": len(state.get("widgets", {})),
        "stepCount": len(state.get("steps", {})),
        "datasets": dataset_index,
        "assetDir": str(asset_dir),
    }
    write_json(asset_dir / "summary.json", summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "labels",
        nargs="+",
        help="One or more dashboard labels to export, for example 'Executive Revenue & Forecast'",
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "docs" / "generated" / "live_asset_exports"),
        help="Directory to write exported dashboard and dataset snapshots",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    return parser


def run_export(
    labels: list[str],
    output_root: Path,
    *,
    emit_text: bool = True,
) -> tuple[dict[str, Any], int]:
    output_root.mkdir(parents=True, exist_ok=True)
    inst, tok = get_auth()

    manifest: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    dataset_warning_count = 0
    artifacts: list[dict[str, str]] = []

    for label in labels:
        if emit_text:
            print(f"Exporting {label}...")
        try:
            summary = export_dashboard(inst, tok, label, output_root)
        except Exception as exc:
            failures.append({"label": label, "error": str(exc)})
            if emit_text:
                print(f"  ERROR: {exc}")
            continue

        manifest.append(summary)
        asset_dir = Path(summary["assetDir"])
        artifacts.append(_make_artifact("directory", asset_dir))
        artifacts.append(_make_artifact("json", asset_dir / "dashboard.json"))
        artifacts.append(_make_artifact("json", asset_dir / "summary.json"))
        dataset_warning_count += sum(
            1
            for dataset in summary["datasets"]
            if dataset.get("warning") or dataset.get("error")
        )

        if emit_text:
            print(
                f"  pages={len(summary['pages'])} widgets={summary['widgetCount']} "
                f"steps={summary['stepCount']} datasets={len(summary['datasets'])}"
            )

    manifest_payload = {"dashboards": manifest, "errors": failures}
    manifest_path = output_root / "manifest.json"
    write_json(manifest_path, manifest_payload)
    artifacts.append(_make_artifact("json", manifest_path))

    if emit_text:
        print(f"\nExported {len(manifest)} dashboard snapshot(s) to {output_root}")

    status = "error" if failures else ("warn" if dataset_warning_count else "ok")
    messages: list[dict[str, str]] = []
    if failures:
        messages.append(
            _make_message(
                "error",
                "export_failed",
                f"Failed to export {len(failures)} dashboard label(s).",
            )
        )
    else:
        messages.append(
            _make_message(
                "info",
                "export_complete",
                f"Exported {len(manifest)} dashboard snapshot(s).",
            )
        )
    if dataset_warning_count:
        messages.append(
            _make_message(
                "warn",
                "dataset_warnings",
                f"Encountered {dataset_warning_count} dataset warning/error entries.",
            )
        )

    payload = _make_result(
        status=status,
        messages=messages,
        artifacts=artifacts,
        summary={
            "dashboards_requested": len(labels),
            "dashboards_exported": len(manifest),
            "dashboard_errors": len(failures),
            "dataset_warning_count": dataset_warning_count,
            "output_dir": str(output_root),
        },
        dashboards=manifest,
        errors=failures,
    )
    return payload, (1 if failures else 0)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    output_root = Path(args.output_dir).resolve()
    payload, exit_code = run_export(
        args.labels,
        output_root,
        emit_text=not args.json,
    )
    if args.json:
        print(json.dumps(payload, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
