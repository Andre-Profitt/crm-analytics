#!/usr/bin/env python3
"""Build territory source bundles from stored Salesforce source extracts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.monthly_platform.source_bundles import build_source_bundles  # noqa: E402


DEFAULT_SOURCE_ROOT = ROOT / "output" / "monthly_salesforce_sources"
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "monthly_source_bundles"


def latest_run_dir(source_root: Path, snapshot_date: str) -> Path:
    dated_root = source_root / snapshot_date
    if not dated_root.exists():
        raise FileNotFoundError(f"No source runs found under {dated_root}")
    candidates = sorted(
        path for path in dated_root.iterdir() if (path / "run_manifest.json").exists()
    )
    if not candidates:
        raise FileNotFoundError(f"No run_manifest.json files found under {dated_root}")
    return candidates[-1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--source-run-dir", type=Path)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    source_run_dir = args.source_run_dir or latest_run_dir(
        args.source_root,
        args.snapshot_date,
    )
    output_dir = (
        args.output_root
        / args.snapshot_date
        / (args.run_id or source_run_dir.name)
    )
    manifest = build_source_bundles(
        source_run_dir=source_run_dir,
        output_dir=output_dir,
        require_complete=args.require_complete,
    )
    result = {
        "status": manifest.status,
        "snapshot_date": manifest.snapshot_date,
        "source_run_id": manifest.source_run_id,
        "source_run_dir": str(source_run_dir),
        "output_dir": manifest.output_dir,
        "manifest_path": str(output_dir / "source_bundle_manifest.json"),
        "territory_count": manifest.territory_count,
        "bundle_count": len(manifest.bundle_paths),
        "source_extract_count": manifest.summary["source_extract_count"],
        "selected_source_count": manifest.summary["selected_source_count"],
        "missing_selected_source_count": manifest.summary[
            "missing_selected_source_count"
        ],
        "forward_fallback_count": manifest.summary["forward_fallback_count"],
        "finding_count": len(manifest.findings),
    }
    print(json.dumps(result, indent=2) if args.json else result)
    if manifest.status == "blocked":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
