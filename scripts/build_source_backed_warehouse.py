#!/usr/bin/env python3
"""Track H — build the source-backed Parquet warehouse for one monthly run.

Reads the upstream pipeline's existing JSON evidence (extraction plan, quality
audit, contract registry) and materializes a deterministic Parquet layout
under ``output/source_backed_warehouse/<snapshot_date>/<run_id>/``.

Read-only of the source pipeline. Writes only to the warehouse output dir.
Exit code: 0 on parity ``pass``, 1 on parity ``fail``, 2 on missing inputs.

Usage::

    # Pick the latest extract run for a snapshot (no auto-run inference).
    python3 scripts/build_source_backed_warehouse.py \\
        --snapshot-date 2026-04-30 \\
        --run-id live-all-sources-pipeline-open-v16

    # Override the warehouse output root (e.g. for tests).
    python3 scripts/build_source_backed_warehouse.py \\
        --snapshot-date 2026-04-30 --run-id v16 --warehouse-root /tmp/wh
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform.warehouse import (  # noqa: E402
    WarehousePaths,
    build_warehouse,
    compute_parity,
)
from scripts.monthly_platform.warehouse.parity import (  # noqa: E402
    write_parity_report,
)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _print_report(manifest: dict[str, Any], parity: dict[str, Any]) -> None:
    print(
        f"Warehouse: {len(manifest['tables'])} tables -> {manifest['warehouse_root']}"
    )
    for table in manifest["tables"]:
        print(
            f"  {table['table_id']}: {table['row_count']} rows / "
            f"{table['byte_count']} bytes ({table['relative_path']})"
        )
    print(f"Parity: {parity['status']} ({len(parity['checks'])} checks)")
    for check in parity["checks"]:
        marker = "✓" if check["status"] == "pass" else "✗"
        print(
            f"  {marker} {check['name']}: expected={check['expected']} observed={check['observed']}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Track H — build the source-backed Parquet warehouse."
    )
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--warehouse-root",
        type=Path,
        default=None,
        help=(
            "Optional override for the warehouse output root. Default: "
            "<repo>/output/source_backed_warehouse/"
        ),
    )
    parser.add_argument(
        "--requirements",
        type=Path,
        default=REPO_ROOT / "config" / "monthly_source_requirements.json",
    )
    parser.add_argument(
        "--source-plan",
        type=Path,
        default=None,
        help="Override path to source_requirement_plan.json",
    )
    parser.add_argument(
        "--source-quality-audit",
        type=Path,
        default=None,
        help="Override path to source_extract_quality_audit.json",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help="Override repo root (used to resolve default input paths)",
    )
    args = parser.parse_args(argv)

    paths = WarehousePaths(
        repo_root=args.repo_root,
        snapshot_date=args.snapshot_date,
        run_id=args.run_id,
        warehouse_root=args.warehouse_root,
    )
    plan_path = args.source_plan or paths.source_plan_path
    audit_path = args.source_quality_audit or paths.source_quality_audit_path
    registry_path = args.requirements

    try:
        plan = _load_json(plan_path)
        audit = _load_json(audit_path)
        registry = _load_json(registry_path)
    except FileNotFoundError as exc:
        print(f"ERROR: missing input: {exc}", file=sys.stderr)
        return 2

    manifest = build_warehouse(paths=paths, plan=plan, audit=audit, registry=registry)
    report = compute_parity(
        paths=paths,
        plan=plan,
        audit=audit,
        registry=registry,
        manifest=manifest.model_dump(),
    )
    write_parity_report(paths, report)

    _print_report(manifest.model_dump(), report.model_dump())
    return 0 if report.status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
