#!/usr/bin/env python3
"""Audit whether current source extracts can promote a DirectorBundle dataset."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform.dataset_readiness import (  # noqa: E402
    audit_dataset_readiness,
    report_to_json,
)


def latest_source_run_dir(source_root: Path, snapshot_date: str) -> Path:
    snapshot_root = source_root / snapshot_date
    candidates = [
        path
        for path in snapshot_root.iterdir()
        if path.is_dir() and (path / "run_manifest.json").exists()
    ]
    if not candidates:
        raise FileNotFoundError(f"No source runs found under {snapshot_root}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit whether monthly source extracts can promote a dataset."
    )
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--dataset", required=True, choices=["pipeline_open"])
    parser.add_argument("--source-run-dir", type=Path)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("output/monthly_salesforce_sources"),
    )
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args()

    source_run_dir = args.source_run_dir or latest_source_run_dir(
        args.source_root,
        args.snapshot_date,
    )
    report = audit_dataset_readiness(
        source_run_dir=source_run_dir,
        dataset=args.dataset,
    )
    report_json = report_to_json(report)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(report_json + "\n", encoding="utf-8")
    if args.json_output:
        print(report_json)
    else:
        print(f"Dataset readiness: {report.status}")
        print(f"Dataset: {report.dataset}")
        print(
            "Missing required fields: "
            f"{', '.join(report.missing_required_fields) or 'none'}"
        )
    return 0 if report.status == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
