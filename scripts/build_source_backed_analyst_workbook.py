#!/usr/bin/env python3
"""Build a deterministic analyst workbook from source-backed DirectorBundles."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform.analyst_workbook import (  # noqa: E402
    build_source_backed_analyst_workbook,
    result_to_json,
)


def latest_director_bundle_manifest(output_root: Path, snapshot_date: str) -> Path:
    snapshot_root = output_root / snapshot_date
    candidates = [
        path
        for path in snapshot_root.glob("*/director_bundle_manifest.json")
        if path.is_file()
    ]
    if not candidates:
        raise FileNotFoundError(
            f"No source-backed DirectorBundle manifests found under {snapshot_root}"
        )
    return max(candidates, key=lambda path: path.stat().st_mtime)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build analyst workbook from source-backed DirectorBundle artifacts."
    )
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--snapshot-date")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("output/monthly_director_bundles_from_sources"),
    )
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args()

    if args.manifest:
        manifest_path = args.manifest
    elif args.snapshot_date:
        manifest_path = latest_director_bundle_manifest(
            args.output_root,
            args.snapshot_date,
        )
    else:
        parser.error("Provide --manifest or --snapshot-date")

    result = build_source_backed_analyst_workbook(
        manifest_path=manifest_path,
        output_path=args.output_path,
    )
    if args.json_output:
        print(result_to_json(result))
    else:
        print(f"Analyst workbook build: {result.status}")
        print(f"Output: {result.workbook_path}")
        print(f"Bundles: {result.bundle_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
