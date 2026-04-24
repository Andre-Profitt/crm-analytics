#!/usr/bin/env python3
"""Build Excel and .ppttc think-cell source artifacts from DirectorBundle JSON."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform.thinkcell_source import (  # noqa: E402
    DEFAULT_TEMPLATE_PATH,
    build_thinkcell_source_from_manifest,
    default_output_dir,
    latest_manifest_path,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build standard PowerPoint/think-cell source artifacts from a "
            "source-backed DirectorBundle manifest."
        )
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Path to director_bundle_manifest.json. If omitted, use --snapshot-date.",
    )
    parser.add_argument("--snapshot-date")
    parser.add_argument("--run-id")
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("output/monthly_director_bundles_from_sources"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("output/thinkcell_source_from_bundles"),
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--template", default=DEFAULT_TEMPLATE_PATH)
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args()

    if args.manifest:
        manifest_path = args.manifest
    else:
        if not args.snapshot_date:
            parser.error("--snapshot-date is required when --manifest is omitted")
        manifest_path = latest_manifest_path(
            source_root=args.source_root,
            snapshot_date=args.snapshot_date,
            run_id=args.run_id,
        )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    output_dir = args.output_dir or default_output_dir(args.output_root, manifest)
    result = build_thinkcell_source_from_manifest(
        manifest_path=manifest_path,
        output_dir=output_dir,
        template_path=args.template,
    )
    if args.json_output:
        print(json.dumps(result, indent=2))
    else:
        print(f"think-cell source build: {result['status']}")
        print(f"Workbook: {result['workbook_path']}")
        print(f"PPTTC: {result['ppttc_path']}")
        print(f"Metrics: {result['summary']['metric_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
