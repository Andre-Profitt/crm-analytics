#!/usr/bin/env python3
"""Author and promote the canonical global summary shell from the SimCorp PowerPoint template."""

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
    from build_sales_global_summary_shell_author_prompt import build_prompt, load_json
    from claude_office_etl import TARGETS, run_skill
except ModuleNotFoundError:  # pragma: no cover
    from scripts.build_sales_global_summary_shell_author_prompt import build_prompt, load_json
    from scripts.claude_office_etl import TARGETS, run_skill


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MASTER_TEMPLATE_PATH = (
    Path.home()
    / "archive"
    / "simcorp-deck-agent-backup"
    / "reference-decks"
    / "SimCorp_PPT_Template.pptx"
)
DEFAULT_SHELL_CONTRACT_PATH = REPO_ROOT / "config" / "sales_global_summary_shell.json"
DEFAULT_SOURCE_MAP_PATH = REPO_ROOT / "docs" / "specs" / "2026-04-11-sales-deck-family-source-map.md"
DEFAULT_EXECUTION_PLAN_PATH = REPO_ROOT / "docs" / "specs" / "2026-04-11-sales-deck-execution-plan.md"
DEFAULT_RUN_ROOT = REPO_ROOT / "output" / "sales_global_canonical_shell_builder"
DEFAULT_CANONICAL_ROOT = REPO_ROOT / "output" / "sales_global_canonical_shells"
GLOBAL_SHELL_FILENAME = "Sales Global Summary Shell.pptx"


def timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def promote_canonical_shell(
    *,
    working_deck_path: Path,
    canonical_root: Path,
    snapshot_date: str,
) -> dict[str, str]:
    stable_path = canonical_root / GLOBAL_SHELL_FILENAME
    dated_path = canonical_root / snapshot_date / GLOBAL_SHELL_FILENAME
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
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--master-template-path", type=Path, default=DEFAULT_MASTER_TEMPLATE_PATH)
    parser.add_argument("--shell-contract-path", type=Path, default=DEFAULT_SHELL_CONTRACT_PATH)
    parser.add_argument("--source-map-path", type=Path, default=DEFAULT_SOURCE_MAP_PATH)
    parser.add_argument("--execution-plan-path", type=Path, default=DEFAULT_EXECUTION_PLAN_PATH)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--canonical-root", type=Path, default=DEFAULT_CANONICAL_ROOT)
    parser.add_argument(
        "--baseline-deck-path",
        type=Path,
        help="Optional validated baseline deck to promote directly as the canonical shell candidate.",
    )
    parser.add_argument("--powerpoint-mode", choices=("skip", "build"), default="skip")
    parser.add_argument("--powerpoint-timeout", type=int, default=1200)
    parser.add_argument("--promote-on-success", action="store_true")
    args = parser.parse_args()

    run_dir = args.run_root / args.snapshot_date / timestamp_slug()
    run_dir.mkdir(parents=True, exist_ok=True)

    shell_contract = load_json(args.shell_contract_path)
    prompt_text = build_prompt(
        shell_contract=shell_contract,
        source_map_path=args.source_map_path,
        execution_plan_path=args.execution_plan_path,
    )
    prompt_path = run_dir / "shell-author-prompt.txt"
    prompt_path.write_text(prompt_text, encoding="utf-8")

    editable_dir = run_dir / "editable_decks"
    editable_dir.mkdir(parents=True, exist_ok=True)
    working_deck_path = editable_dir / GLOBAL_SHELL_FILENAME
    source_deck_path = args.baseline_deck_path or args.master_template_path
    shutil.copy2(source_deck_path, working_deck_path)

    if args.baseline_deck_path and args.powerpoint_mode == "build":
        raise ValueError(
            "Use either --baseline-deck-path for direct promotion or --powerpoint-mode build for live shell authoring, not both."
        )

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
        powerpoint_stage: dict[str, object] = {
            "status": "ok",
            "source_type": "master-template",
            "source_deck_path": str(source_deck_path),
            "working_deck_path": str(working_deck_path),
            **result,
        }
        if args.promote_on_success:
            promotion: dict[str, object] = promote_canonical_shell(
                working_deck_path=working_deck_path,
                canonical_root=args.canonical_root,
                snapshot_date=args.snapshot_date,
            )
        else:
            promotion = {"status": "skipped", "reason": "promote_on_success=false"}
    elif args.baseline_deck_path:
        powerpoint_stage = {
            "status": "skipped",
            "reason": "baseline-deck promotion path",
            "source_type": "validated-baseline",
            "source_deck_path": str(source_deck_path),
            "working_deck_path": str(working_deck_path),
        }
        if args.promote_on_success:
            promotion = promote_canonical_shell(
                working_deck_path=working_deck_path,
                canonical_root=args.canonical_root,
                snapshot_date=args.snapshot_date,
            )
        else:
            promotion = {"status": "skipped", "reason": "promote_on_success=false"}
    else:
        powerpoint_stage = {
            "status": "skipped",
            "reason": "powerpoint_mode=skip",
            "source_type": "master-template",
            "source_deck_path": str(source_deck_path),
            "working_deck_path": str(working_deck_path),
        }
        promotion = {"status": "skipped", "reason": "powerpoint_mode=skip"}

    manifest = {
        "snapshot_date": args.snapshot_date,
        "run_dir": str(run_dir),
        "master_template_path": str(args.master_template_path),
        "shell_contract_path": str(args.shell_contract_path),
        "source_map_path": str(args.source_map_path),
        "execution_plan_path": str(args.execution_plan_path),
        "shell_author_prompt_path": str(prompt_path),
        "source_deck_path": str(source_deck_path),
        "working_deck_path": str(working_deck_path),
        "powerpoint_shell_authoring": powerpoint_stage,
        "canonical_promotion": promotion,
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
