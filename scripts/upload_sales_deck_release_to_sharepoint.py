#!/usr/bin/env python3
"""Upload publish-ready Sales Director release assets to SharePoint."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

try:
    from build_sales_deck_release_packet import DEFAULT_OUTPUT_ROOT, quarter_label
except ModuleNotFoundError:  # pragma: no cover
    from scripts.build_sales_deck_release_packet import DEFAULT_OUTPUT_ROOT, quarter_label


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DRIVE_ID = "b!NFdN4U0FoUmZxNNOBNAFO95fRH4lEKdPm1y4Rq_UXNSNhf32U6-8R7dL79jOAP15"
DEFAULT_FOLDER_PREFIX = "General/Book of Business/Sales Director Reporting"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SOURCE_BACKED_SCHEMA_PREFIX = "monthly_platform.source_backed_release_packet"


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def month_year_label(snapshot_date: str) -> str:
    return datetime.strptime(snapshot_date[:10], "%Y-%m-%d").strftime("%B %Y")


def resolve_release_packet(args: argparse.Namespace) -> tuple[Path, dict[str, Any]]:
    if args.source_backed_bundle_manifest_json is not None:
        return (
            args.source_backed_bundle_manifest_json,
            source_backed_packet_from_bundle_manifest(args.source_backed_bundle_manifest_json),
        )
    if args.release_packet_json is not None:
        packet_path = args.release_packet_json
    elif args.release_dir is not None:
        packet_path = args.release_dir / "release-packet.json"
    else:
        packet_path = DEFAULT_OUTPUT_ROOT / args.snapshot_date / "latest.json"
    return packet_path, load_json(packet_path)


def source_backed_packet_from_bundle_manifest(bundle_manifest_path: Path) -> dict[str, Any]:
    manifest = load_json(bundle_manifest_path)
    summary = manifest.get("summary") or {}
    handoff = manifest.get("sharepoint_handoff") or {}
    artifacts = {
        str(row.get("name")): row
        for row in manifest.get("artifacts") or []
        if isinstance(row, dict)
    }

    def artifact_path(source_name: str) -> str | None:
        row = artifacts.get(source_name) or {}
        path_value = row.get("source_path")
        return str(path_value) if path_value else None

    upload_ready = (
        manifest.get("status") == "ok"
        and bool(handoff.get("upload_ready")) is True
        and int(summary.get("missing_required_artifact_count") or 0) == 0
        and Path(str(manifest.get("zip_path") or "")).exists()
    )
    return {
        "schema_version": "monthly_platform.source_backed_release_packet.from_bundle_manifest.v1",
        "status": "ok" if upload_ready else "blocked",
        "publish_recommendation": "publish" if upload_ready else "do_not_publish",
        "snapshot_date": manifest.get("snapshot_date"),
        "run_id": manifest.get("source_run_id"),
        "summary": {
            "release_bundle_artifact_count": summary.get("artifact_count"),
            "release_bundle_copied_artifact_count": summary.get("copied_artifact_count"),
            "release_bundle_required_artifact_count": summary.get("required_artifact_count"),
            "release_bundle_missing_required_artifact_count": summary.get(
                "missing_required_artifact_count"
            ),
            "release_bundle_zip_size_bytes": manifest.get("zip_size_bytes"),
            "release_bundle_upload_ready": bool(handoff.get("upload_ready")),
        },
        "release_checks": [
            {
                "name": "release_bundle_upload_ready",
                "status": "pass" if upload_ready else "fail",
            }
        ],
        "artifacts": {
            "release_bundle_manifest": str(bundle_manifest_path),
            "release_bundle_zip": manifest.get("zip_path"),
            "source_backed_deck": artifact_path("source_backed_monthly_review"),
            "analyst_workbook": artifact_path("analyst_workbook"),
            "thinkcell_workbook": artifact_path("thinkcell_source"),
            "thinkcell_ppttc": artifact_path("thinkcell_ppttc"),
        },
    }


def is_source_backed_packet(packet: dict[str, Any]) -> bool:
    return str(packet.get("schema_version") or "").startswith(SOURCE_BACKED_SCHEMA_PREFIX)


def source_backed_publish_ready(packet: dict[str, Any]) -> bool:
    checks = packet.get("release_checks") or []
    summary = packet.get("summary") or {}
    return (
        packet.get("status") == "ok"
        and packet.get("publish_recommendation") == "publish"
        and all(check.get("status") == "pass" for check in checks)
        and bool(summary.get("release_bundle_upload_ready")) is True
    )


def packet_publish_ready(packet: dict[str, Any]) -> bool:
    if is_source_backed_packet(packet):
        return source_backed_publish_ready(packet)
    return bool(packet.get("publish_ready"))


def source_backed_folder_name(packet: dict[str, Any], bundle_manifest: dict[str, Any] | None) -> str:
    if bundle_manifest:
        handoff = bundle_manifest.get("sharepoint_handoff") or {}
        folder_name = str(handoff.get("folder_name") or "").strip()
        if folder_name:
            return folder_name
    return (
        f"Sales Director Monthly Review - {packet.get('snapshot_date')} - "
        f"{packet.get('run_id')}"
    )


def _asset_from_path(
    *,
    packet: dict[str, Any],
    key: str,
    category: str,
    publish_name: str,
) -> dict[str, Any] | None:
    artifacts = packet.get("artifacts") or {}
    path_value = artifacts.get(key)
    if not path_value:
        return None
    return {
        "category": category,
        "source_artifact": key,
        "publish_name": publish_name,
        "publish_path": str(path_value),
    }


def source_backed_publish_assets(packet: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    artifacts = packet.get("artifacts") or {}
    bundle_manifest_path = artifacts.get("release_bundle_manifest")
    bundle_manifest = (
        load_json(Path(str(bundle_manifest_path)))
        if bundle_manifest_path and Path(str(bundle_manifest_path)).exists()
        else None
    )
    snapshot_date = str(packet.get("snapshot_date") or "")
    month_label = month_year_label(snapshot_date) if snapshot_date else "Unknown Snapshot"
    assets = [
        _asset_from_path(
            packet=packet,
            key="release_bundle_zip",
            category="source_backed_release_bundle",
            publish_name=f"Sales Director Monthly Source-Backed Release Bundle - {month_label}.zip",
        ),
        _asset_from_path(
            packet=packet,
            key="source_backed_deck",
            category="source_backed_deck",
            publish_name=f"Sales Director Monthly Review - {month_label}.pptx",
        ),
        _asset_from_path(
            packet=packet,
            key="analyst_workbook",
            category="source_backed_analyst_workbook",
            publish_name=f"Sales Director Monthly Analyst Workbook - {month_label}.xlsx",
        ),
        _asset_from_path(
            packet=packet,
            key="thinkcell_workbook",
            category="source_backed_thinkcell_workbook",
            publish_name=f"Sales Director Monthly think-cell Source - {month_label}.xlsx",
        ),
        _asset_from_path(
            packet=packet,
            key="thinkcell_ppttc",
            category="source_backed_thinkcell_payload",
            publish_name=f"Sales Director Monthly think-cell Payload - {month_label}.ppttc",
        ),
    ]
    return [asset for asset in assets if asset], bundle_manifest


def resolve_publish_assets(packet: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if is_source_backed_packet(packet):
        return source_backed_publish_assets(packet)
    return (packet.get("publish_assets") or {}).get("assets") or [], None


def acquire_graph_token(explicit_token: str | None) -> str:
    if explicit_token:
        return explicit_token
    proc = subprocess.run(
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            "https://graph.microsoft.com",
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "Failed to acquire Microsoft Graph token via az.")
    token = proc.stdout.strip()
    if not token:
        raise RuntimeError("Microsoft Graph token response was empty.")
    return token


def upload_file(*, token: str, drive_id: str, folder: str, asset_path: Path, publish_name: str) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}"}
    upload_path = f"{folder}/{publish_name}"
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{upload_path}:/content"
    size = asset_path.stat().st_size
    if size < 4 * 1024 * 1024:
        with asset_path.open("rb") as handle:
            response = requests.put(
                url,
                headers={**headers, "Content-Type": "application/octet-stream"},
                data=handle,
                timeout=300,
            )
        response.raise_for_status()
        mode = "single_put"
    else:
        session_url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{upload_path}:/createUploadSession"
        session = requests.post(
            session_url,
            headers=headers,
            json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
            timeout=300,
        )
        session.raise_for_status()
        upload_url = session.json()["uploadUrl"]
        chunk_size = 10 * 1024 * 1024
        with asset_path.open("rb") as handle:
            offset = 0
            while offset < size:
                data = handle.read(chunk_size)
                end = offset + len(data) - 1
                response = requests.put(
                    upload_url,
                    headers={"Content-Range": f"bytes {offset}-{end}/{size}"},
                    data=data,
                    timeout=300,
                )
                response.raise_for_status()
                offset += len(data)
        mode = "chunked"
    return {
        "publish_name": publish_name,
        "source_path": str(asset_path),
        "size_bytes": size,
        "mode": mode,
        "sharepoint_path": upload_path,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date")
    parser.add_argument("--release-dir", type=Path)
    parser.add_argument("--release-packet-json", type=Path)
    parser.add_argument("--source-backed-bundle-manifest-json", type=Path)
    parser.add_argument("--drive-id", default=DEFAULT_DRIVE_ID)
    parser.add_argument("--folder-prefix", default=DEFAULT_FOLDER_PREFIX)
    parser.add_argument("--token", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-path", type=Path)
    args = parser.parse_args()

    packet_path, packet = resolve_release_packet(args)
    snapshot_date = str(packet.get("snapshot_date") or args.snapshot_date or "").strip()
    publish_assets, source_backed_bundle_manifest = resolve_publish_assets(packet)

    payload: dict[str, Any] = {
        "status": "running",
        "snapshot_date": snapshot_date or None,
        "release_packet_path": str(packet_path),
        "schema_version": packet.get("schema_version"),
        "source_backed": is_source_backed_packet(packet),
        "publish_ready": packet_publish_ready(packet),
        "dry_run": args.dry_run,
        "quarter_folder": None,
        "folder": None,
        "planned": [],
        "uploaded": [],
        "skipped": [],
    }

    def finish(exit_code: int) -> int:
        if args.output_path:
            args.output_path.parent.mkdir(parents=True, exist_ok=True)
            args.output_path.write_text(
                json.dumps(payload, indent=2) + "\n",
                encoding="utf-8",
            )
        print(json.dumps(payload, indent=2))
        return exit_code

    if not payload["publish_ready"]:
        payload["status"] = "blocked"
        payload["reason"] = "release_packet_not_publish_ready"
        return finish(2)
    if not snapshot_date:
        payload["status"] = "blocked"
        payload["reason"] = "missing_snapshot_date"
        return finish(2)
    if not publish_assets:
        payload["status"] = "blocked"
        payload["reason"] = "no_publish_assets"
        return finish(2)

    quarter_folder = quarter_label(snapshot_date)
    folder = f"{args.folder_prefix}/{quarter_folder}"
    if payload["source_backed"]:
        folder = f"{folder}/{source_backed_folder_name(packet, source_backed_bundle_manifest)}"
    payload["quarter_folder"] = quarter_folder
    payload["folder"] = folder

    for asset in publish_assets:
        asset_path = Path(str(asset.get("publish_path") or ""))
        publish_name = str(asset.get("publish_name") or asset_path.name)
        payload["planned"].append(
            {
                "publish_name": publish_name,
                "publish_path": str(asset_path),
                "category": asset.get("category"),
                "sharepoint_path": f"{folder}/{publish_name}",
                "exists": asset_path.exists(),
            }
        )

    if args.dry_run:
        payload["planned_count"] = len(payload["planned"])
        payload["missing_count"] = sum(
            1 for planned in payload["planned"] if not planned.get("exists")
        )
        payload["status"] = "planned" if payload["missing_count"] == 0 else "blocked"
        if payload["missing_count"]:
            payload["reason"] = "missing_publish_asset"
        return finish(0 if payload["status"] == "planned" else 2)

    try:
        token = acquire_graph_token(args.token)
        for asset in publish_assets:
            asset_path = Path(str(asset.get("publish_path") or ""))
            publish_name = str(asset.get("publish_name") or asset_path.name)
            if not asset_path.exists():
                payload["skipped"].append(
                    {
                        "publish_name": publish_name,
                        "reason": "missing_publish_asset",
                        "publish_path": str(asset_path),
                    }
                )
                continue
            payload["uploaded"].append(
                upload_file(
                    token=token,
                    drive_id=args.drive_id,
                    folder=folder,
                    asset_path=asset_path,
                    publish_name=publish_name,
                )
            )
    except Exception as exc:  # pragma: no cover - network/auth errors are environment-specific
        payload["status"] = "error"
        payload["error"] = {"type": type(exc).__name__, "message": str(exc)}
        return finish(1)

    payload["status"] = "ok"
    payload["uploaded_count"] = len(payload["uploaded"])
    payload["skipped_count"] = len(payload["skipped"])
    return finish(0)


if __name__ == "__main__":
    raise SystemExit(main())
