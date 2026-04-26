#!/usr/bin/env python3
"""Deterministic global builder: global snapshot -> fact pack -> baseline deck -> canonical shell."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
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
    from build_sales_global_summary_snapshot import build_global_summary_snapshot
    from build_sales_global_summary_shell import build_shell_deck
    from build_validated_sales_global_summary_brief import build_validation_artifacts
    from audit_sales_global_summary_preview import audit_preview
    from claude_office_etl import TARGETS, run_skill
except ModuleNotFoundError:  # pragma: no cover
    from scripts.build_sales_global_summary_snapshot import build_global_summary_snapshot
    from scripts.build_sales_global_summary_shell import build_shell_deck
    from scripts.build_validated_sales_global_summary_brief import build_validation_artifacts
    from scripts.audit_sales_global_summary_preview import audit_preview
    from scripts.claude_office_etl import TARGETS, run_skill


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIRECTOR_SNAPSHOT_ROOT = REPO_ROOT / "output" / "director_workbook_snapshots"
DEFAULT_REGION_SNAPSHOT_ROOT = REPO_ROOT / "output" / "sales_region_snapshots"
DEFAULT_GLOBAL_SNAPSHOT_ROOT = REPO_ROOT / "output" / "sales_global_summary_snapshots"
DEFAULT_CANONICAL_SHELL_ROOT = REPO_ROOT / "output" / "sales_global_canonical_shells"
DEFAULT_FALLBACK_SHELL_ROOT = REPO_ROOT / "output" / "sales_global_summary_shells"
DEFAULT_RUN_ROOT = REPO_ROOT / "output" / "sales_global_summary_builder"
SLIDES_SKILL_SCRIPTS = Path.home() / ".codex" / "skills" / "slides" / "scripts"
DEFAULT_MONTAGE_SCRIPT = SLIDES_SKILL_SCRIPTS / "create_montage.py"
DEFAULT_DETECT_FONT_SCRIPT = SLIDES_SKILL_SCRIPTS / "detect_font.py"
DEFAULT_TEMPLATE_DECK_PATH = (
    Path.home()
    / "archive"
    / "simcorp-deck-agent-backup"
    / "reference-decks"
    / "SimCorp_PPT_Template.pptx"
)
GLOBAL_SHELL_FILENAME = "Sales Global Summary Shell.pptx"


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
    run_slug = run_dir.parent.name
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


def canonical_shell_candidates(*, shell_root: Path, snapshot_date: str) -> list[Path]:
    return [
        shell_root / snapshot_date / GLOBAL_SHELL_FILENAME,
        shell_root / GLOBAL_SHELL_FILENAME,
    ]


def resolve_shell_path(
    *,
    snapshot_date: str,
    canonical_shell_root: Path,
    fallback_shell_root: Path,
    allow_generated_fallback: bool,
) -> tuple[Path, dict[str, object]]:
    for candidate in canonical_shell_candidates(shell_root=canonical_shell_root, snapshot_date=snapshot_date):
        if candidate.exists():
            return candidate, {
                "status": "ok",
                "source": "canonical",
                "shell_path": str(candidate),
            }
    if not allow_generated_fallback:
        raise FileNotFoundError(
            f"No canonical global shell deck found under {canonical_shell_root}. "
            "Create and promote the canonical shell first, or rerun with "
            "--allow-generated-shell-fallback for a non-publish-safe fallback shell."
        )
    for candidate in canonical_shell_candidates(shell_root=fallback_shell_root, snapshot_date=snapshot_date):
        if candidate.exists():
            return candidate, {
                "status": "ok",
                "source": "generated",
                "shell_path": str(candidate),
                "publish_safe": False,
                "fallback_reason": "Canonical shell missing; using explicit global fallback shell.",
            }
    raise FileNotFoundError(
        f"No fallback global shell deck found under {fallback_shell_root}. "
        "Create the shell first or remove --allow-generated-shell-fallback."
    )


def plan_shell_resolution(
    *,
    snapshot_date: str,
    canonical_shell_root: Path,
    fallback_shell_root: Path,
    allow_generated_fallback: bool,
) -> dict[str, object]:
    try:
        shell_path, resolution = resolve_shell_path(
            snapshot_date=snapshot_date,
            canonical_shell_root=canonical_shell_root,
            fallback_shell_root=fallback_shell_root,
            allow_generated_fallback=allow_generated_fallback,
        )
        return {
            "status": "ok",
            "shell_path": str(shell_path),
            **resolution,
        }
    except FileNotFoundError as exc:
        return {
            "status": "missing",
            "reason": str(exc),
        }


def build_deterministic_preview(
    *,
    snapshot_date: str,
    fill_payload_path: Path,
    output_dir: Path,
    template_deck_path: Path,
) -> dict[str, object]:
    preview_path = output_dir / "Sales Global Summary Validated Baseline.pptx"
    build = build_shell_deck(
        snapshot_date=snapshot_date,
        output_path=preview_path,
        master_template_path=template_deck_path,
        fill_payload_path=fill_payload_path,
    )
    return {
        "status": "ok",
        "deck_path": str(preview_path),
        "fill_payload_path": str(fill_payload_path),
        "build": build,
    }


def render_deterministic_preview(
    *,
    preview_stage: dict[str, object],
    output_dir: Path,
) -> dict[str, object]:
    deck_path = Path(str(preview_stage["deck_path"]))
    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = output_dir / f"{deck_path.stem}.pdf"
    slides_dir = output_dir / "slides"
    slides_dir.mkdir(parents=True, exist_ok=True)
    montage_path = output_dir / "montage.png"
    font_report_path = output_dir / "font-report.json"

    subprocess.run(
        [
            "soffice",
            "--headless",
            "--convert-to",
            "pdf",
            "--outdir",
            str(output_dir),
            str(deck_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            "pdftoppm",
            "-png",
            str(pdf_path),
            str(slides_dir / "slide"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            "python3",
            str(DEFAULT_MONTAGE_SCRIPT),
            "--input_dir",
            str(slides_dir),
            "--output_file",
            str(montage_path),
            "--label_mode",
            "number",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    font_run = subprocess.run(
        [
            "python3",
            str(DEFAULT_DETECT_FONT_SCRIPT),
            "--json",
            str(deck_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    font_report = json.loads(font_run.stdout or "{}")
    font_report_path.write_text(json.dumps(font_report, indent=2, ensure_ascii=True), encoding="utf-8")
    return {
        "status": "ok",
        "pdf_path": str(pdf_path),
        "slides_dir": str(slides_dir),
        "montage_path": str(montage_path),
        "font_report_path": str(font_report_path),
        "font_report": font_report,
    }


def build_deterministic_preview_audit(
    *,
    preview_stage: dict[str, object],
    fill_payload_path: Path,
    output_dir: Path,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "audit-report.json"
    report = audit_preview(Path(str(preview_stage["deck_path"])), fill_payload_path)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=True), encoding="utf-8")
    return {
        "status": "ok",
        "report_path": str(report_path),
        "ok": report["ok"],
        "finding_count": report["finding_count"],
        "findings": report["findings"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--director-snapshot-root", type=Path, default=DEFAULT_DIRECTOR_SNAPSHOT_ROOT)
    parser.add_argument("--region-snapshot-root", type=Path, default=DEFAULT_REGION_SNAPSHOT_ROOT)
    parser.add_argument("--global-snapshot-root", type=Path, default=DEFAULT_GLOBAL_SNAPSHOT_ROOT)
    parser.add_argument("--canonical-shell-root", type=Path, default=DEFAULT_CANONICAL_SHELL_ROOT)
    parser.add_argument("--fallback-shell-root", type=Path, default=DEFAULT_FALLBACK_SHELL_ROOT)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--template-deck-path", type=Path, default=DEFAULT_TEMPLATE_DECK_PATH)
    parser.add_argument(
        "--allow-generated-shell-fallback",
        action="store_true",
        help="Allow a non-publish-safe fallback shell if the canonical global shell is missing.",
    )
    parser.add_argument("--powerpoint-mode", choices=("skip", "build"), default="skip")
    parser.add_argument("--powerpoint-timeout", type=int, default=900)
    args = parser.parse_args()

    run_dir = args.run_root / args.snapshot_date / timestamp_slug()
    run_dir.mkdir(parents=True, exist_ok=True)

    snapshot = build_global_summary_snapshot(
        snapshot_date=args.snapshot_date,
        region_snapshot_root=args.region_snapshot_root,
        director_snapshot_root=args.director_snapshot_root,
    )
    snapshot_path = args.global_snapshot_root / args.snapshot_date / "global-summary.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=True), encoding="utf-8")

    artifacts = build_validation_artifacts(snapshot)
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

    deterministic_preview = build_deterministic_preview(
        snapshot_date=args.snapshot_date,
        fill_payload_path=fact_pack_dir / "powerpoint-fill-payload.json",
        output_dir=run_dir / "deterministic_preview",
        template_deck_path=args.template_deck_path,
    )
    deterministic_preview_render = render_deterministic_preview(
        preview_stage=deterministic_preview,
        output_dir=run_dir / "deterministic_preview_render",
    )
    deterministic_preview_audit = build_deterministic_preview_audit(
        preview_stage=deterministic_preview,
        fill_payload_path=fact_pack_dir / "powerpoint-fill-payload.json",
        output_dir=run_dir / "deterministic_preview_audit",
    )

    shell_resolution = plan_shell_resolution(
        snapshot_date=args.snapshot_date,
        canonical_shell_root=args.canonical_shell_root,
        fallback_shell_root=args.fallback_shell_root,
        allow_generated_fallback=args.allow_generated_shell_fallback,
    )

    if args.powerpoint_mode == "build":
        if shell_resolution["status"] != "ok":
            raise FileNotFoundError(str(shell_resolution["reason"]))
        shell_path = Path(str(shell_resolution["shell_path"]))
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
        "status": "ok",
        "run_dir": str(run_dir),
        "global_snapshot_path": str(snapshot_path),
        "validated_fact_pack_path": str(fact_pack_dir / "validated-fact-pack.md"),
        "powerpoint_fill_payload_path": str(fact_pack_dir / "powerpoint-fill-payload.json"),
        "powerpoint_build_prompt_path": str(fact_pack_dir / "powerpoint-build-prompt.txt"),
        "shell_resolution": shell_resolution,
        "deterministic_preview": deterministic_preview,
        "deterministic_preview_render": deterministic_preview_render,
        "deterministic_preview_audit": deterministic_preview_audit,
        "powerpoint_build": powerpoint_stage,
        "regions": [row["region_name"] for row in snapshot.get("regions", [])],
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
