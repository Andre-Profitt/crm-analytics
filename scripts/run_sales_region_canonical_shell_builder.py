#!/usr/bin/env python3
"""Author and promote canonical regional shells from the SimCorp PowerPoint template."""

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
    from build_sales_region_shell_author_prompt import build_prompt, load_json
    from claude_office_etl import TARGETS, run_skill
except ModuleNotFoundError:  # pragma: no cover
    from scripts.build_sales_region_shell_author_prompt import build_prompt, load_json
    from scripts.claude_office_etl import TARGETS, run_skill


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MASTER_TEMPLATE_PATH = (
    Path.home()
    / "archive"
    / "simcorp-deck-agent-backup"
    / "reference-decks"
    / "SimCorp_PPT_Template.pptx"
)
DEFAULT_SHELL_CONTRACT_PATH = REPO_ROOT / "config" / "sales_region_monthly_shell.json"
DEFAULT_DESIGN_SYSTEM_PATH = (
    REPO_ROOT / "docs" / "specs" / "2026-04-11-sales-region-shell-design-system.md"
)
DEFAULT_RUN_ROOT = REPO_ROOT / "output" / "sales_region_canonical_shell_builder"
DEFAULT_CANONICAL_ROOT = REPO_ROOT / "output" / "sales_region_canonical_shells"


def timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def canonical_shell_name(region_name: str) -> str:
    return f"Sales Region Monthly Shell - {region_name}.pptx"


def promote_canonical_shell(
    *,
    working_deck_path: Path,
    canonical_root: Path,
    region_name: str,
    snapshot_date: str,
) -> dict[str, str]:
    stable_path = canonical_root / canonical_shell_name(region_name)
    dated_path = canonical_root / snapshot_date / canonical_shell_name(region_name)
    stable_path.parent.mkdir(parents=True, exist_ok=True)
    dated_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(working_deck_path, stable_path)
    shutil.copy2(working_deck_path, dated_path)
    return {
        "stable_path": str(stable_path),
        "dated_path": str(dated_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region-name", choices=("APAC", "EMEA", "North America"), required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--master-template-path", type=Path, default=DEFAULT_MASTER_TEMPLATE_PATH)
    parser.add_argument("--shell-contract-path", type=Path, default=DEFAULT_SHELL_CONTRACT_PATH)
    parser.add_argument("--design-system-path", type=Path, default=DEFAULT_DESIGN_SYSTEM_PATH)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--canonical-root", type=Path, default=DEFAULT_CANONICAL_ROOT)
    parser.add_argument("--powerpoint-mode", choices=("skip", "build"), default="skip")
    parser.add_argument("--powerpoint-timeout", type=int, default=1200)
    parser.add_argument("--promote-on-success", action="store_true")
    args = parser.parse_args()

    run_dir = args.run_root / args.snapshot_date / timestamp_slug() / args.region_name.lower().replace(" ", "-")
    run_dir.mkdir(parents=True, exist_ok=True)

    shell_contract = load_json(args.shell_contract_path)
    prompt_text = build_prompt(
        region_name=args.region_name,
        shell_contract=shell_contract,
        design_system_path=args.design_system_path,
    )
    prompt_path = run_dir / "shell-author-prompt.txt"
    prompt_path.write_text(prompt_text, encoding="utf-8")

    editable_dir = run_dir / "editable_decks"
    editable_dir.mkdir(parents=True, exist_ok=True)
    working_deck_path = editable_dir / canonical_shell_name(args.region_name)
    shutil.copy2(args.master_template_path, working_deck_path)

    powerpoint_stage: dict[str, object]
    promotion: dict[str, str] | dict[str, object]
    if args.powerpoint_mode == "build":
        result = run_skill(
            TARGETS["powerpoint"],
            source_file=working_deck_path,
            skill_name=None,
            prompt=prompt_text,
            wait_finish_seconds=max(args.powerpoint_timeout, 900),
            run_dir=run_dir / "powerpoint_shell_authoring",
            edit_permission_mode="always-allow",
            save_document_on_finish=True,
        )
        powerpoint_stage = {
            "status": "ok",
            "working_deck_path": str(working_deck_path),
            **result,
        }
        if args.promote_on_success:
            promotion = promote_canonical_shell(
                working_deck_path=working_deck_path,
                canonical_root=args.canonical_root,
                region_name=args.region_name,
                snapshot_date=args.snapshot_date,
            )
        else:
            promotion = {"status": "skipped", "reason": "promote_on_success=false"}
    else:
        powerpoint_stage = {"status": "skipped", "reason": "powerpoint_mode=skip"}
        promotion = {"status": "skipped", "reason": "powerpoint_mode=skip"}

    manifest = {
        "snapshot_date": args.snapshot_date,
        "region_name": args.region_name,
        "master_template_path": str(args.master_template_path),
        "shell_contract_path": str(args.shell_contract_path),
        "design_system_path": str(args.design_system_path),
        "shell_author_prompt_path": str(prompt_path),
        "working_deck_path": str(working_deck_path),
        "powerpoint_shell_authoring": powerpoint_stage,
        "canonical_promotion": promotion,
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
