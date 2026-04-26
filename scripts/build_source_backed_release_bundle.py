#!/usr/bin/env python3
"""Build a deterministic source-backed monthly release bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "source_backed_release_bundles"
SCHEMA_VERSION = "monthly_platform.source_backed_release_bundle.v1"


def build_release_bundle(
    *,
    snapshot_date: str,
    source_run_id: str,
    artifacts: list[dict[str, Any]],
    output_dir: Path | None = None,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    output_path: Path | None = None,
    zip_path: Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = Path(output_dir or (Path(output_root) / snapshot_date / source_run_id))
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    output_path = Path(output_path or (resolved_output_dir / "source_backed_release_bundle_manifest.json"))
    zip_path = Path(zip_path or (resolved_output_dir / "source_backed_release_bundle.zip"))

    findings: list[dict[str, Any]] = []
    copied_artifacts: list[dict[str, Any]] = []
    for artifact in artifacts:
        copied_artifacts.append(
            _copy_artifact(
                artifact=artifact,
                output_dir=resolved_output_dir,
                findings=findings,
            )
        )

    required_artifacts = [artifact for artifact in copied_artifacts if artifact["required"]]
    missing_required = [
        artifact for artifact in required_artifacts if artifact["status"] != "copied"
    ]
    if missing_required:
        findings.extend(
            _finding(
                "high",
                "required_artifact_missing",
                f"{artifact['name']}: {artifact['source_path']}",
            )
            for artifact in missing_required
        )

    summary = {
        "artifact_count": len(copied_artifacts),
        "copied_artifact_count": sum(
            1 for artifact in copied_artifacts if artifact["status"] == "copied"
        ),
        "required_artifact_count": len(required_artifacts),
        "missing_required_artifact_count": len(missing_required),
        "total_bytes": sum(
            int(artifact.get("size_bytes") or 0)
            for artifact in copied_artifacts
            if artifact["status"] == "copied"
        ),
    }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if missing_required else "ok",
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_date": snapshot_date,
        "source_run_id": source_run_id,
        "output_dir": str(resolved_output_dir),
        "zip_path": str(zip_path),
        "summary": summary,
        "sharepoint_handoff": {
            "folder_name": f"Sales Director Monthly Review - {snapshot_date} - {source_run_id}",
            "upload_ready": not missing_required,
            "source_system": "local_source_backed_monthly_pipeline",
        },
        "artifacts": copied_artifacts,
        "findings": findings,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload["output_path"] = str(output_path)
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _write_summary_markdown(payload, resolved_output_dir / "summary.md")
    _write_zip(
        output_dir=resolved_output_dir,
        zip_path=zip_path,
        include_paths=[Path(artifact["bundle_path"]) for artifact in copied_artifacts if artifact["status"] == "copied"]
        + [output_path, resolved_output_dir / "summary.md"],
    )
    payload["zip_size_bytes"] = zip_path.stat().st_size if zip_path.exists() else 0
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload


def _copy_artifact(
    *,
    artifact: dict[str, Any],
    output_dir: Path,
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    name = str(artifact["name"])
    category = str(artifact["category"])
    source_path = Path(str(artifact["path"]))
    required = bool(artifact.get("required", True))
    bundle_path = output_dir / category / f"{name}{source_path.suffix}"
    result = {
        "name": name,
        "category": category,
        "required": required,
        "source_path": str(source_path),
        "bundle_path": str(bundle_path),
        "status": "missing",
        "size_bytes": 0,
        "sha256": None,
    }
    if not source_path.exists():
        if required:
            findings.append(
                _finding(
                    "high",
                    "artifact_source_missing",
                    f"{name}: {source_path}",
                )
            )
        return result
    if not source_path.is_file():
        findings.append(
            _finding(
                "high" if required else "medium",
                "artifact_source_not_file",
                f"{name}: {source_path}",
            )
        )
        return result
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, bundle_path)
    result.update(
        {
            "status": "copied",
            "size_bytes": bundle_path.stat().st_size,
            "sha256": _sha256(bundle_path),
        }
    )
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_summary_markdown(payload: dict[str, Any], path: Path) -> None:
    summary = payload["summary"]
    lines = [
        f"# Source-Backed Release Bundle — {payload['snapshot_date']}",
        "",
        f"- Status: `{payload['status']}`",
        f"- Run ID: `{payload['source_run_id']}`",
        f"- Upload ready: `{payload['sharepoint_handoff']['upload_ready']}`",
        f"- Copied artifacts: `{summary['copied_artifact_count']}` / `{summary['artifact_count']}`",
        f"- Required missing: `{summary['missing_required_artifact_count']}`",
        f"- Zip: `{payload['zip_path']}`",
        "",
        "## Artifacts",
        "",
    ]
    for artifact in payload["artifacts"]:
        lines.append(
            f"- `{artifact['name']}` ({artifact['category']}): `{artifact['status']}`"
        )
    if payload["findings"]:
        lines.extend(["", "## Findings", ""])
        lines.extend(
            f"- `{finding['severity']}` `{finding['issue']}`: {finding['evidence']}"
            for finding in payload["findings"]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_zip(
    *,
    output_dir: Path,
    zip_path: Path,
    include_paths: list[Path],
) -> None:
    zip_path.unlink(missing_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in include_paths:
            if not path.exists() or path == zip_path:
                continue
            archive.write(path, path.relative_to(output_dir))


def _finding(severity: str, issue: str, evidence: str) -> dict[str, Any]:
    return {"severity": severity, "issue": issue, "evidence": evidence}


def parse_artifact(value: str, *, required: bool) -> dict[str, Any]:
    parts = value.split("=", 2)
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            "artifact must be NAME=CATEGORY=PATH"
        )
    name, category, path = parts
    if not name or not category or not path:
        raise argparse.ArgumentTypeError(
            "artifact must include non-empty NAME, CATEGORY, and PATH"
        )
    return {
        "name": name,
        "category": category,
        "path": path,
        "required": required,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument(
        "--artifact",
        action="append",
        default=[],
        help="Required artifact as NAME=CATEGORY=PATH.",
    )
    parser.add_argument(
        "--optional-artifact",
        action="append",
        default=[],
        help="Optional artifact as NAME=CATEGORY=PATH.",
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--zip-path", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifacts = [
        parse_artifact(value, required=True) for value in args.artifact
    ] + [
        parse_artifact(value, required=False) for value in args.optional_artifact
    ]
    payload = build_release_bundle(
        snapshot_date=args.snapshot_date,
        source_run_id=args.source_run_id,
        artifacts=artifacts,
        output_dir=args.output_dir,
        output_root=args.output_root,
        output_path=args.output_path,
        zip_path=args.zip_path,
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
