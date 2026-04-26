#!/usr/bin/env python3
"""Run the canonical source-backed monthly pipeline with safe defaults."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.monthly_platform.period import resolve_period_context  # noqa: E402
from scripts.run_source_backed_monthly_pipeline import (  # noqa: E402
    DEFAULT_BUNDLE_CONTRACT,
    DEFAULT_FIELD_GUARDRAILS,
    DEFAULT_REQUIREMENTS_PATH,
    DEFAULT_TARGET_ORG,
    DEFAULT_TEMPLATE_PATH,
    DEFAULT_TERRITORY_CONFIG,
    default_manifest_path,
)


@dataclass(frozen=True)
class LaunchPlan:
    snapshot_date: str
    run_id: str
    manifest_path: Path
    command: list[str]


def default_run_id(snapshot_date: str, generated_at: datetime | None = None) -> str:
    generated_at = generated_at or datetime.now(UTC)
    return f"source-backed-{snapshot_date}-{generated_at.strftime('%Y%m%dT%H%M%SZ')}"


def build_launch_plan(
    args: argparse.Namespace,
    *,
    generated_at: datetime | None = None,
) -> LaunchPlan:
    snapshot_date = str(args.snapshot_date or resolve_period_context().snapshot_date)[:10]
    run_id = str(args.run_id or default_run_id(snapshot_date, generated_at))
    manifest_path = args.output_path or default_manifest_path(snapshot_date, run_id)
    command = [
        sys.executable,
        "scripts/run_source_backed_monthly_pipeline.py",
        "--snapshot-date",
        snapshot_date,
        "--run-id",
        run_id,
        "--target-org",
        str(args.target_org),
        "--requirements",
        str(args.requirements),
        "--territory-config",
        str(args.territory_config),
        "--bundle-contract",
        str(args.bundle_contract),
        "--field-guardrails",
        str(args.field_guardrails),
        "--template-path",
        str(args.template_path),
        "--output-path",
        str(manifest_path),
    ]
    if args.plan_only:
        command.append("--plan-only")
    if args.keep_going:
        command.append("--keep-going")
    if args.start_at:
        command.extend(["--start-at", str(args.start_at)])
    return LaunchPlan(
        snapshot_date=snapshot_date,
        run_id=run_id,
        manifest_path=Path(manifest_path),
        command=command,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot-date",
        help="Month-end snapshot date. Defaults to previous month-end from the period resolver.",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG)
    parser.add_argument("--requirements", type=Path, default=DEFAULT_REQUIREMENTS_PATH)
    parser.add_argument("--territory-config", type=Path, default=DEFAULT_TERRITORY_CONFIG)
    parser.add_argument("--bundle-contract", type=Path, default=DEFAULT_BUNDLE_CONTRACT)
    parser.add_argument("--field-guardrails", type=Path, default=DEFAULT_FIELD_GUARDRAILS)
    parser.add_argument("--template-path", default=DEFAULT_TEMPLATE_PATH)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--keep-going", action="store_true")
    parser.add_argument("--start-at")
    parser.add_argument(
        "--print-command",
        action="store_true",
        help="Print the resolved launch plan without executing it.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    launch = build_launch_plan(args)
    if args.print_command:
        print(
            json.dumps(
                {
                    "status": "planned",
                    "snapshot_date": launch.snapshot_date,
                    "run_id": launch.run_id,
                    "manifest_path": str(launch.manifest_path),
                    "command": launch.command,
                },
                indent=2,
            )
        )
        return 0
    completed = subprocess.run(launch.command, cwd=ROOT, check=False)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
