#!/usr/bin/env python3
"""Track K — release catalog CLI.

Wraps Track G-Lite's release packet with waiver application from
config/waivers/. Emits a release_catalog.json (and optional .md
summary) with pre/post waiver totals and the final publish_decision.

Usage:
    python3 scripts/build_release_catalog.py \\
        --workbook ~/Downloads/jesper-tyrer-2026-04-20.xlsx \\
        --pptx ~/Downloads/jesper-tyrer-LAND.pptx \\
        --run-id v20d-track-f-final \\
        --out output/track_k/release_catalog.json \\
        --md-out output/track_k/release_catalog.md
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform import release_catalog  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build release catalog (Track K — release packet + waivers)."
    )
    parser.add_argument("--workbook", required=True, type=Path)
    parser.add_argument("--pptx", required=True, type=Path)
    parser.add_argument("--deck-contract", default=None)
    parser.add_argument("--workbook-contract", default=None)
    parser.add_argument("--policy", type=Path, default=None)
    parser.add_argument("--waiver-dir", type=Path, default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--skip-visual", action="store_true")
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--md-out", type=Path, default=None)
    parser.add_argument("--show-applied", action="store_true")
    parser.add_argument(
        "--lineage-dir",
        type=Path,
        default=None,
        help=(
            "When set, emit Track J-Lite OpenLineage events plus "
            "lineage_index.json + slide_to_source_map.json into this dir."
        ),
    )
    args = parser.parse_args(argv)

    if not args.workbook.exists():
        print(f"ERROR: workbook not found: {args.workbook}", file=sys.stderr)
        return 2
    if not args.pptx.exists():
        print(f"ERROR: pptx not found: {args.pptx}", file=sys.stderr)
        return 2

    result = release_catalog.build_release_catalog(
        workbook=args.workbook,
        pptx=args.pptx,
        deck_contract_path=args.deck_contract,
        workbook_contract_path=args.workbook_contract,
        policy_path=args.policy,
        waiver_dir=args.waiver_dir,
        run_id=args.run_id,
        skip_visual=args.skip_visual,
        lineage_dir=args.lineage_dir,
    )

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            json.dumps(result.as_dict(), indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        print(f"catalog: {args.out}")
    if args.md_out:
        args.md_out.parent.mkdir(parents=True, exist_ok=True)
        args.md_out.write_text(
            release_catalog.render_markdown(result), encoding="utf-8"
        )
        print(f"md: {args.md_out}")

    if args.show_applied and result.applied_waivers:
        print("Applied waivers:")
        for a in result.applied_waivers:
            print(
                f"  {a.waiver_id}: {a.gate} "
                f"{a.severity_before} -> {a.severity_after} (owner={a.waiver_owner})"
            )

    print(
        f"release_catalog: {result.publish_decision} "
        f"(pre: {result.pre_waiver_blocker_total} blockers / "
        f"{result.pre_waiver_warning_total} warnings; "
        f"post: {result.post_waiver_blocker_total} blockers / "
        f"{result.post_waiver_warning_total} warnings; "
        f"applied={len(result.applied_waivers)} unused={len(result.unused_waivers)})"
    )
    return 0 if result.publish_decision == "publish_ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
