#!/usr/bin/env python3
"""Build legacy DirectorBundle JSON from monthly source-bundle artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform.director_bundle_builder import (  # noqa: E402
    build_director_bundles_from_source_bundles,
    manifest_to_json,
)
from scripts.monthly_platform.director_bundle_contract import (  # noqa: E402
    load_director_bundle_contract,
)


def latest_source_bundle_dir(source_root: Path, snapshot_date: str) -> Path:
    snapshot_root = source_root / snapshot_date
    candidates = [
        path
        for path in snapshot_root.iterdir()
        if path.is_dir() and (path / "source_bundle_manifest.json").exists()
    ]
    if not candidates:
        raise FileNotFoundError(f"No source-bundle runs found under {snapshot_root}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build DirectorBundle JSON from monthly source bundles."
    )
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--source-bundle-dir", type=Path)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("output/monthly_source_bundles"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("output/monthly_director_bundles_from_sources"),
    )
    parser.add_argument("--run-id")
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("config/monthly_director_bundle_contract.json"),
    )
    parser.add_argument("--require-valid", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args()

    bundle_contract = load_director_bundle_contract(args.contract)
    source_bundle_dir = args.source_bundle_dir or latest_source_bundle_dir(
        args.source_root,
        args.snapshot_date,
    )
    run_id = args.run_id or source_bundle_dir.name
    output_dir = args.output_root / args.snapshot_date / run_id
    manifest = build_director_bundles_from_source_bundles(
        source_bundle_dir=source_bundle_dir,
        output_dir=output_dir,
        require_valid=args.require_valid,
        bundle_contract=bundle_contract,
    )
    if args.json_output:
        print(manifest_to_json(manifest))
    else:
        print(f"Director bundle build: {manifest.status}")
        print(f"Output: {output_dir}")
        print(f"Bundles: {len(manifest.bundle_paths)}")
    return 0 if manifest.status != "blocked" else 2


if __name__ == "__main__":
    raise SystemExit(main())
