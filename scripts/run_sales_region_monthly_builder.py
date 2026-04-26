#!/usr/bin/env python3
"""Deterministic regional builder: snapshot -> fact pack -> SimCorp shell."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ARCHIVE_DIR = SCRIPT_DIR / "_archive"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(ARCHIVE_DIR) not in sys.path:
    sys.path.append(str(ARCHIVE_DIR))

try:
    from build_sales_region_monthly_shell import build_shell_deck
    from build_sales_region_snapshot import build_region_snapshot
    from build_validated_sales_region_brief import build_validation_artifacts
    from claude_office_etl import TARGETS, run_skill
except ModuleNotFoundError:  # pragma: no cover
    from scripts.build_sales_region_monthly_shell import build_shell_deck
    from scripts.build_sales_region_snapshot import build_region_snapshot
    from scripts.build_validated_sales_region_brief import build_validation_artifacts
    from scripts.claude_office_etl import TARGETS, run_skill


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIRECTOR_SNAPSHOT_ROOT = REPO_ROOT / "output" / "director_workbook_snapshots"
DEFAULT_REGION_SNAPSHOT_ROOT = REPO_ROOT / "output" / "sales_region_snapshots"
DEFAULT_SHELL_ROOT = REPO_ROOT / "output" / "sales_region_monthly_shells"
DEFAULT_CANONICAL_SHELL_ROOT = REPO_ROOT / "output" / "sales_region_canonical_shells"
DEFAULT_RUN_ROOT = REPO_ROOT / "output" / "sales_region_monthly_builder"


def timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def run_powerpoint_build(
    *,
    shell_path: Path,
    prompt_text: str,
    run_dir: Path,
    timeout: int,
) -> dict[str, object]:
    editable_dir = run_dir / "editable_decks"
    editable_dir.mkdir(parents=True, exist_ok=True)
    run_slug = run_dir.parent.parent.name
    editable_path = editable_dir / f"{shell_path.stem} [build {run_slug}]{shell_path.suffix}"
    shutil.copy2(shell_path, editable_path)
    result = run_skill(
        TARGETS["powerpoint"],
        source_file=editable_path,
        skill_name=None,
        prompt=prompt_text,
        wait_finish_seconds=max(timeout, 900),
        run_dir=run_dir,
        edit_permission_mode="always-allow",
        save_document_on_finish=True,
    )
    return {
        "status": "ok",
        "source_deck_path": str(shell_path),
        "editable_deck_path": str(editable_path),
        **result,
    }


def canonical_shell_candidates(*, shell_root: Path, snapshot_date: str, region_name: str) -> list[Path]:
    slug = f"Sales Region Monthly Shell - {region_name}.pptx"
    return [
        shell_root / snapshot_date / slug,
        shell_root / slug,
    ]


def resolve_shell_path(
    *,
    shell_source: str,
    region_name: str,
    snapshot_date: str,
    generated_shell_root: Path,
    canonical_shell_root: Path,
    allow_generated_fallback: bool,
) -> tuple[Path, dict[str, object]]:
    if shell_source in {"canonical", "auto"}:
        for candidate in canonical_shell_candidates(
            shell_root=canonical_shell_root,
            snapshot_date=snapshot_date,
            region_name=region_name,
        ):
            if candidate.exists():
                return candidate, {
                    "status": "ok",
                    "source": "canonical",
                    "shell_path": str(candidate),
                }
        if shell_source == "canonical" and not allow_generated_fallback:
            raise FileNotFoundError(
                f"No canonical shell deck found for {region_name} under {canonical_shell_root}. "
                "Create and promote the canonical shell first, or rerun with "
                "--allow-generated-shell-fallback for a non-publish-safe scaffold."
            )
    generated_path = generated_shell_root / snapshot_date / f"Sales Region Monthly Shell - {region_name}.pptx"
    return generated_path, {
        "status": "ok",
        "source": "generated",
        "shell_path": str(generated_path),
        "publish_safe": False,
        "fallback_reason": (
            "Canonical shell missing; using generated scaffold only because generated fallback was explicitly allowed."
            if shell_source == "canonical"
            else "Generated shell explicitly requested."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--region-name", choices=("APAC", "EMEA", "North America"), required=True)
    parser.add_argument("--director-snapshot-root", type=Path, default=DEFAULT_DIRECTOR_SNAPSHOT_ROOT)
    parser.add_argument("--region-snapshot-root", type=Path, default=DEFAULT_REGION_SNAPSHOT_ROOT)
    parser.add_argument("--shell-root", type=Path, default=DEFAULT_SHELL_ROOT)
    parser.add_argument("--canonical-shell-root", type=Path, default=DEFAULT_CANONICAL_SHELL_ROOT)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--shell-source", choices=("auto", "canonical", "generated"), default="canonical")
    parser.add_argument(
        "--allow-generated-shell-fallback",
        action="store_true",
        help="Allow a generated shell scaffold if the canonical shell is missing. This path is not publish-safe.",
    )
    parser.add_argument("--powerpoint-mode", choices=("skip", "build"), default="skip")
    parser.add_argument("--powerpoint-timeout", type=int, default=900)
    args = parser.parse_args()

    run_dir = args.run_root / args.snapshot_date / timestamp_slug() / args.region_name.lower().replace(" ", "-")
    run_dir.mkdir(parents=True, exist_ok=True)

    region_snapshot = build_region_snapshot(
        region_name=args.region_name,
        snapshot_date=args.snapshot_date,
        director_snapshot_root=args.director_snapshot_root,
    )
    snapshot_path = args.region_snapshot_root / args.snapshot_date / f"{args.region_name.lower().replace(' ', '-')}.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(region_snapshot, indent=2, ensure_ascii=True), encoding="utf-8")

    artifacts = build_validation_artifacts(region_snapshot)
    fact_pack_dir = run_dir / "validated_bridge"
    fact_pack_dir.mkdir(parents=True, exist_ok=True)
    (fact_pack_dir / "validated-fact-pack.md").write_text(artifacts["validated_brief"], encoding="utf-8")
    (fact_pack_dir / "powerpoint-fill-payload.json").write_text(
        json.dumps(artifacts["structured_fill_payload"], indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    (fact_pack_dir / "powerpoint-build-prompt.txt").write_text(
        artifacts["powerpoint_build_prompt"], encoding="utf-8"
    )

    shell_path, shell_resolution = resolve_shell_path(
        shell_source=args.shell_source,
        region_name=args.region_name,
        snapshot_date=args.snapshot_date,
        generated_shell_root=args.shell_root,
        canonical_shell_root=args.canonical_shell_root,
        allow_generated_fallback=args.allow_generated_shell_fallback,
    )
    if shell_resolution["source"] == "generated":
        shell_build = build_shell_deck(
            region_name=args.region_name,
            snapshot_date=args.snapshot_date,
            output_path=shell_path,
        )
    else:
        shell_build = {
            "deck_path": str(shell_path),
            "slide_count": None,
            "template_version": "canonical",
        }

    powerpoint_stage: dict[str, object]
    if args.powerpoint_mode == "build":
        powerpoint_stage = run_powerpoint_build(
            shell_path=shell_path,
            prompt_text=artifacts["powerpoint_build_prompt"],
            run_dir=run_dir / "powerpoint_build",
            timeout=args.powerpoint_timeout,
        )
    else:
        powerpoint_stage = {"status": "skipped", "reason": "powerpoint_mode=skip"}

    manifest = {
        "snapshot_date": args.snapshot_date,
        "region_name": args.region_name,
        "status": "ok",
        "run_dir": str(run_dir),
        "region_snapshot_path": str(snapshot_path),
        "validated_fact_pack_path": str(fact_pack_dir / "validated-fact-pack.md"),
        "powerpoint_fill_payload_path": str(fact_pack_dir / "powerpoint-fill-payload.json"),
        "powerpoint_build_prompt_path": str(fact_pack_dir / "powerpoint-build-prompt.txt"),
        "shell_deck_path": str(shell_path),
        "shell_resolution": shell_resolution,
        "shell_build": shell_build,
        "powerpoint_build": powerpoint_stage,
        "component_books": region_snapshot.get("component_books"),
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
