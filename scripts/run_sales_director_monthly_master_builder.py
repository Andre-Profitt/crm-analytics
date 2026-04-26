#!/usr/bin/env python3
"""Master monthly builder for Sales Director workbook-to-deck runs.

This runner is the monthly control plane for the current Excel -> Claude ->
PowerPoint workflow.

1. Refresh or reuse workbook JSON snapshots.
2. Ask Excel Claude for a draft monthly brief.
3. Validate that draft against the snapshot and emit a fact pack.
4. Optionally build or review a deck with PowerPoint Claude using the validated pack.

Review and preview rendering must stay anchored to native SimCorp template
artifacts. Generated shell scaffolds are allowed only as non-publish-safe
fallbacks while the canonical shell store is being hardened.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import shutil
import sys
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
ARCHIVE_DIR = SCRIPT_DIR / "_archive"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(ARCHIVE_DIR) not in sys.path:
    sys.path.append(str(ARCHIVE_DIR))

from build_validated_director_brief import (  # noqa: E402
    build_validation_artifacts,
    load_external_inputs,
    load_snapshot,
    write_text,
)
from audit_sales_director_preview import audit_preview  # noqa: E402
from audit_director_etl_intelligence import (  # noqa: E402
    build_etl_intelligence_audit,
    markdown_summary as build_etl_intelligence_markdown,
)
from build_sales_director_monthly_shell import build_shell_deck  # noqa: E402
from build_source_backed_deck import (  # noqa: E402
    DEFAULT_OUTPUT_ROOT as DEFAULT_SOURCE_BACKED_DECK_ROOT,
    build_source_backed_deck,
)
from claude_office_etl import TARGETS, run_skill  # noqa: E402
from extract_director_workbook_snapshot import (  # noqa: E402
    DEFAULT_OUTPUT_ROOT as DEFAULT_SNAPSHOT_ROOT,
    DEFAULT_WORKBOOK_ROOT,
    extract_workbook,
    slugify,
    workbook_paths,
)
from run_director_workbook_decks import (  # noqa: E402
    DEFAULT_DECK_ROOT as DEFAULT_WORKBOOK_DECK_ROOT,
    build_deck as build_workbook_deck,
)
from validate_source_backed_deck_visuals import (  # noqa: E402
    DEFAULT_OUTPUT_ROOT as DEFAULT_SOURCE_BACKED_DECK_VISUAL_ROOT,
    validate_deck_visuals,
)

try:
    from monthly_platform.period import PeriodContext, resolve_period_context
    from monthly_platform.workbook_inventory import available_snapshot_dates
except ModuleNotFoundError:  # pragma: no cover
    from scripts.monthly_platform.period import PeriodContext, resolve_period_context
    from scripts.monthly_platform.workbook_inventory import available_snapshot_dates


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REVIEW_DECK_ROOT = REPO_ROOT / "output" / "sales_director_monthly_runs"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "sales_director_monthly_master_builder"
DEFAULT_SHELL_ROOT = REPO_ROOT / "output" / "sales_director_monthly_shells"
DEFAULT_CANONICAL_SHELL_ROOT = REPO_ROOT / "output" / "sales_director_canonical_shells"
DEFAULT_BUNDLE_ROOT = REPO_ROOT / "output" / "director_bundles"
DEFAULT_GOLD_ROOT = REPO_ROOT / "output" / "director_gold_analytics"
DEFAULT_LIVE_WORKBOOK_ROOT = REPO_ROOT / "output" / "director_live_workbooks"
DEFAULT_LAND_DECK_ROOT = REPO_ROOT / "output" / "simcorp_director_decks"
DEFAULT_TIEOUT_ROOT = REPO_ROOT / "output" / "tie_out"
DEFAULT_DECK_TRUTH_PACKET_ROOT = REPO_ROOT / "output" / "deck_truth_packets"
SLIDES_SKILL_SCRIPTS = Path.home() / ".codex" / "skills" / "slides" / "scripts"
DEFAULT_MONTAGE_SCRIPT = SLIDES_SKILL_SCRIPTS / "create_montage.py"
DEFAULT_DETECT_FONT_SCRIPT = SLIDES_SKILL_SCRIPTS / "detect_font.py"
DEFAULT_SLIDES_TEST_SCRIPT = SLIDES_SKILL_SCRIPTS / "slides_test.py"
DEFAULT_TEMPLATE_DECK_PATH = (
    Path.home()
    / "archive"
    / "simcorp-deck-agent-backup"
    / "reference-decks"
    / "SimCorp_PPT_Template.pptx"
)
DEFAULT_MONTHLY_BRIEF_PROMPT = """Use the SD Workbook Fact Pack skill if it is available.

Read this Sales Director workbook and write a strict factual monthly operating brief in markdown.

Use exactly these headings:
## Executive Summary
## Pipeline Overview
## Commercial Approval
## Renewals
## Q1 Promise vs Delivered
## Slipped Deals
## Coverage and Intel
## Open Questions

Requirements:
- Use only facts supported by the workbook.
- Cite the tab name inline when it materially supports a claim.
- Every pipeline metric must specify the horizon and ARR.
- Renewal metrics must use ACV.
- If a metric is global, ambiguous, missing, or still a placeholder, say so plainly.
- Do not invent owner commentary, root causes, or Finance inputs that are not present.
- Keep each section to 2-3 bullets, except Open Questions with at most 2 bullets.
- Do not ask follow-up questions.
"""


@dataclass(frozen=True)
class DirectorTarget:
    director_name: str
    territory: str
    workbook_path: Path
    snapshot_path: Path
    existing_deck_path: Path | None


def timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def save_manifest(
    run_dir: Path, manifest: dict[str, Any], *, status: str | None = None
) -> None:
    manifest["updated_at"] = datetime.now(UTC).isoformat()
    if status is not None:
        manifest["status"] = status
    save_json(run_dir / "manifest.json", manifest)


def stage_error(exc: Exception) -> dict[str, Any]:
    return {
        "status": "error",
        "error": {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        },
    }


def build_preflight_failure_payload(
    *,
    snapshot_date: str,
    director: str | None,
    deck_source: str,
    fallback_workbook_deck: bool,
    workbook_root: Path,
    period: PeriodContext,
    exc: Exception,
) -> dict[str, Any]:
    snapshot_dates = available_snapshot_dates(workbook_root)
    latest_snapshot_date = snapshot_dates[-1] if snapshot_dates else None
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "error",
        "phase": "preflight",
        "snapshot_date": snapshot_date,
        "period_context": period.as_dict(),
        "director_filter": director,
        "target_count": 0,
        "deck_source": deck_source,
        "fallback_workbook_deck": fallback_workbook_deck,
        "run_dir": None,
        "targets": [],
        "error": {
            "type": type(exc).__name__,
            "message": str(exc),
        },
        "preflight": {
            "requested_snapshot_date": snapshot_date,
            "workbook_root": str(workbook_root),
            "available_snapshot_dates": snapshot_dates,
            "latest_available_snapshot_date": latest_snapshot_date,
        },
    }


def latest_dated_dir(root: Path) -> Path | None:
    if not root.exists():
        return None
    dated = sorted(path for path in root.iterdir() if path.is_dir())
    return dated[-1] if dated else None


def director_prompt(
    prompt_template: str, director_name: str, territory: str, workbook_path: Path
) -> str:
    context = [
        f"Director: {director_name}",
        f"Territory: {territory}",
        f"Workbook: {workbook_path.name}",
    ]
    return "\n".join(context) + "\n\n" + prompt_template.strip() + "\n"


def find_matching_file(
    root: Path | None, director_name: str, suffix: str
) -> Path | None:
    if root is None or not root.exists():
        return None
    matches = sorted(
        path
        for path in root.iterdir()
        if path.is_file()
        and path.suffix == suffix
        and director_name.lower() in path.name.lower()
        and not path.name.startswith("~$")
    )
    return matches[0] if matches else None


def sync_snapshot(
    workbook_path: Path, snapshot_date: str, snapshot_root: Path, refresh: bool
) -> Path:
    snapshot_dir = snapshot_root / snapshot_date
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    director_stub = workbook_path.stem.replace("Sales Director Data - ", "")
    director_name = director_stub.split(" (")[0]
    out_path = snapshot_dir / f"{slugify(director_name)}.json"
    if refresh or not out_path.exists():
        snapshot = extract_workbook(workbook_path)
        out_path.write_text(
            json.dumps(snapshot, indent=2, ensure_ascii=True), encoding="utf-8"
        )
    return out_path


def build_targets(
    *,
    snapshot_date: str,
    director: str | None,
    workbook_root: Path,
    snapshot_root: Path,
    deck_root: Path,
    deck_date: str | None,
    refresh_snapshots: bool,
) -> list[DirectorTarget]:
    workbooks = workbook_paths(workbook_root, snapshot_date, director)
    if not workbooks:
        raise FileNotFoundError(
            f"No workbook files found for snapshot date {snapshot_date} under {workbook_root}."
        )
    deck_dir = deck_root / deck_date if deck_date else latest_dated_dir(deck_root)
    targets: list[DirectorTarget] = []
    for workbook_path in workbooks:
        snapshot_path = sync_snapshot(
            workbook_path, snapshot_date, snapshot_root, refresh_snapshots
        )
        snapshot = load_snapshot(snapshot_path)
        targets.append(
            DirectorTarget(
                director_name=snapshot["director_name"],
                territory=snapshot["territory"],
                workbook_path=workbook_path,
                snapshot_path=snapshot_path,
                existing_deck_path=find_matching_file(
                    deck_dir, snapshot["director_name"], ".pptx"
                ),
            )
        )
    return targets


def prepare_review_deck(
    target: DirectorTarget,
    *,
    snapshot_date: str,
    deck_source: str,
    template_deck_path: Path,
    shell_root: Path,
    canonical_shell_root: Path,
    allow_generated_shell_fallback: bool,
    fallback_workbook_deck: bool,
    workbook_deck_root: Path,
    render_workbook_deck: bool,
) -> tuple[Path | None, dict[str, Any]]:
    if deck_source == "skip":
        return None, {
            "status": "skipped",
            "reason": "Deck review disabled for this run.",
        }
    if deck_source == "canonical-shell":
        shell_name = f"Sales Director Monthly Shell - {target.director_name} ({target.territory}).pptx"
        candidates = [
            canonical_shell_root / snapshot_date / shell_name,
            canonical_shell_root / shell_name,
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate, {
                    "status": "ok",
                    "source": "canonical-shell",
                    "deck_path": str(candidate),
                }
        if not allow_generated_shell_fallback:
            raise FileNotFoundError(
                f"No canonical director shell deck found for {target.director_name} under {canonical_shell_root}. "
                "Create and promote the canonical shell first, or rerun with "
                "--allow-generated-shell-fallback for a non-publish-safe native scaffold."
            )
        deck_source = "shell"
    if deck_source == "shell":
        shell_path = (
            shell_root
            / snapshot_date
            / (
                f"Sales Director Monthly Shell - {target.director_name} ({target.territory}).pptx"
            )
        )
        shell_build = build_shell_deck(
            director_name=target.director_name,
            territory=target.territory,
            snapshot_date=snapshot_date,
            output_path=shell_path,
            master_template_path=template_deck_path,
        )
        return shell_path, {
            "status": "ok",
            "source": "generated-shell",
            "deck_path": str(shell_path),
            "shell_build": shell_build,
            "publish_safe": False,
        }
    if deck_source == "template":
        if not template_deck_path.exists():
            raise FileNotFoundError(f"Template deck not found: {template_deck_path}")
        return template_deck_path, {
            "status": "ok",
            "source": "template",
            "deck_path": str(template_deck_path),
        }
    if deck_source == "existing" and target.existing_deck_path:
        return target.existing_deck_path, {
            "status": "ok",
            "source": "existing",
            "deck_path": str(target.existing_deck_path),
        }
    if deck_source == "existing" and not fallback_workbook_deck:
        return None, {
            "status": "skipped",
            "reason": "No existing review deck matched this director.",
        }

    build = build_workbook_deck(
        target.snapshot_path, workbook_deck_root, render_workbook_deck
    )
    built_path = Path(build["deck_path"])
    source = (
        "workbook-native"
        if deck_source == "workbook-native"
        else "workbook-native-fallback"
    )
    return built_path, {
        "status": "ok",
        "source": source,
        "deck_path": str(built_path),
        "workbook_deck_build": build,
    }


def plan_review_deck(
    target: DirectorTarget,
    *,
    snapshot_date: str,
    deck_source: str,
    template_deck_path: Path,
    shell_root: Path,
    canonical_shell_root: Path,
    allow_generated_shell_fallback: bool,
    fallback_workbook_deck: bool,
    workbook_deck_root: Path,
) -> tuple[Path | None, dict[str, Any]]:
    if deck_source == "skip":
        return None, {
            "status": "skipped",
            "reason": "Deck review disabled for this run.",
        }
    if deck_source == "canonical-shell":
        shell_name = f"Sales Director Monthly Shell - {target.director_name} ({target.territory}).pptx"
        candidates = [
            canonical_shell_root / snapshot_date / shell_name,
            canonical_shell_root / shell_name,
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate, {
                    "status": "ok",
                    "source": "canonical-shell",
                    "deck_path": str(candidate),
                }
        if not allow_generated_shell_fallback:
            return None, {
                "status": "missing",
                "source": "canonical-shell",
                "reason": (
                    f"No canonical director shell deck found for {target.director_name} under {canonical_shell_root}. "
                    "Create and promote the canonical shell first, or rerun with "
                    "--allow-generated-shell-fallback for a non-publish-safe native scaffold."
                ),
            }
        deck_source = "shell"
    if deck_source == "shell":
        shell_path = (
            shell_root
            / snapshot_date
            / (
                f"Sales Director Monthly Shell - {target.director_name} ({target.territory}).pptx"
            )
        )
        return shell_path, {
            "status": "planned",
            "source": "generated-shell",
            "deck_path": str(shell_path),
            "publish_safe": False,
        }
    if deck_source == "template":
        return template_deck_path, {
            "status": "ok" if template_deck_path.exists() else "missing",
            "source": "template",
            "deck_path": str(template_deck_path),
        }
    if deck_source == "existing":
        if target.existing_deck_path:
            return target.existing_deck_path, {
                "status": "ok",
                "source": "existing",
                "deck_path": str(target.existing_deck_path),
            }
        if not fallback_workbook_deck:
            return None, {
                "status": "missing",
                "source": "existing",
                "reason": "No existing review deck matched this director.",
            }
    build_dir = workbook_deck_root / snapshot_date / slugify(target.director_name)
    deck_name = f"{slugify(target.director_name)}.pptx"
    deck_path = build_dir / deck_name
    source = (
        "workbook-native"
        if deck_source == "workbook-native"
        else "workbook-native-fallback"
    )
    return deck_path, {
        "status": "planned",
        "source": source,
        "deck_path": str(deck_path),
    }


def run_excel_brief(
    target: DirectorTarget,
    *,
    prompt_template: str,
    timeout: int,
    run_dir: Path,
) -> dict[str, Any]:
    prompt = director_prompt(
        prompt_template, target.director_name, target.territory, target.workbook_path
    )
    attempts: list[dict[str, Any]] = []
    last_exc: Exception | None = None
    for attempt in range(1, 3):
        attempt_dir = run_dir / f"attempt-{attempt}"
        try:
            result = run_skill(
                TARGETS["excel"],
                source_file=target.workbook_path,
                skill_name=None,
                prompt=prompt,
                wait_finish_seconds=timeout,
                run_dir=attempt_dir,
            )
            return {
                "status": "ok",
                **result,
                "attempt_count": attempt,
                "attempts": attempts,
            }
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            attempts.append(
                {
                    "attempt": attempt,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
            )
            recoverable = (
                "Could not find or open the Claude pane in Microsoft Excel." in str(exc)
                and type(exc).__name__ == "AutomationError"
            )
            if attempt == 1 and recoverable:
                _quit_office_app("Microsoft Excel", save=False)
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Excel brief stage ended without a result or exception.")


def _quit_office_app(app_name: str, *, save: bool) -> None:
    save_literal = "yes" if save else "no"
    subprocess.run(
        [
            "osascript",
            "-e",
            f'tell application "{app_name}" to quit saving {save_literal}',
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def build_validated_bridge(
    snapshot_path: Path,
    excel_brief_text: str,
    output_dir: Path,
    external_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = load_snapshot(snapshot_path)
    artifacts = build_validation_artifacts(snapshot, excel_brief_text, external_inputs)
    output_dir.mkdir(parents=True, exist_ok=True)
    validated_fact_pack = output_dir / "validated-fact-pack.md"
    powerpoint_fill_payload = output_dir / "powerpoint-fill-payload.json"
    powerpoint_prompt = output_dir / "powerpoint-validated-prompt.txt"
    powerpoint_build_prompt = output_dir / "powerpoint-build-prompt.txt"
    validation_report = output_dir / "validation-report.json"
    excel_raw_brief = output_dir / "excel-raw-brief.txt"
    write_text(validated_fact_pack, artifacts["validated_brief"])
    write_text(
        powerpoint_fill_payload,
        json.dumps(artifacts["structured_fill_payload"], indent=2, ensure_ascii=True),
    )
    write_text(powerpoint_prompt, artifacts["powerpoint_prompt"])
    write_text(powerpoint_build_prompt, artifacts["powerpoint_build_prompt"])
    write_text(validation_report, json.dumps(artifacts["validation_report"], indent=2))
    write_text(excel_raw_brief, excel_brief_text)
    return {
        "status": "ok",
        "validated_fact_pack": str(validated_fact_pack),
        "powerpoint_fill_payload": str(powerpoint_fill_payload),
        "powerpoint_prompt": str(powerpoint_prompt),
        "powerpoint_build_prompt": str(powerpoint_build_prompt),
        "validation_report": str(validation_report),
        "excel_raw_brief": str(excel_raw_brief),
        "issues": artifacts["validation_report"]["issues"],
    }


def run_etl_intelligence_audit(
    target: DirectorTarget,
    *,
    snapshot_date: str,
    bundle_root: Path,
    output_dir: Path,
) -> dict[str, Any]:
    bundle_path = bundle_root / snapshot_date / f"{slugify(target.director_name)}.json"
    if not bundle_path.exists():
        return {
            "status": "missing",
            "bundle_path": str(bundle_path),
            "workbook_path": str(target.workbook_path),
            "reason": "Matching DirectorBundle JSON was not found.",
        }
    output_dir.mkdir(parents=True, exist_ok=True)
    audit = build_etl_intelligence_audit(
        bundle_path=bundle_path,
        workbook_path=target.workbook_path,
    )
    audit_path = output_dir / "etl_intelligence_audit.json"
    summary_path = output_dir / "summary.md"
    save_json(audit_path, audit)
    write_text(summary_path, build_etl_intelligence_markdown(audit))
    summary = audit["summary"]
    status = "needs_attention" if summary["high_gap_count"] else "ok"
    return {
        "status": status,
        "bundle_path": str(bundle_path),
        "workbook_path": str(target.workbook_path),
        "audit_path": str(audit_path),
        "summary_path": str(summary_path),
        "high_gap_count": summary["high_gap_count"],
        "coverage_gap_count": summary["coverage_gap_count"],
        "deal_risk_rows": summary["deal_risk_rows"],
        "recommendation_count": summary["recommendation_count"],
    }


def build_deterministic_preview(
    target: DirectorTarget,
    *,
    snapshot_date: str,
    bridge: dict[str, Any],
    output_dir: Path,
    template_deck_path: Path,
) -> dict[str, Any]:
    preview_path = output_dir / (
        f"Sales Director Monthly - {target.director_name} ({target.territory}) Validated Baseline.pptx"
    )
    build = build_shell_deck(
        director_name=target.director_name,
        territory=target.territory,
        snapshot_date=snapshot_date,
        output_path=preview_path,
        master_template_path=template_deck_path,
        fill_payload_path=Path(bridge["powerpoint_fill_payload"]),
    )
    return {
        "status": "ok",
        "deck_path": str(preview_path),
        "fill_payload_path": bridge["powerpoint_fill_payload"],
        "build": build,
    }


def render_deterministic_preview(
    *,
    preview_stage: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    deck_path = Path(preview_stage["deck_path"])
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
    save_json(font_report_path, font_report)
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
    preview_stage: dict[str, Any],
    bridge: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    report_path = output_dir / "audit-report.json"
    report = audit_preview(
        Path(preview_stage["deck_path"]),
        Path(bridge["powerpoint_fill_payload"]),
    )
    save_json(report_path, report)
    return {
        "status": "ok",
        "report_path": str(report_path),
        "ok": report["ok"],
        "finding_count": report["finding_count"],
        "findings": report["findings"],
    }


def build_deterministic_preview_layout_audit(
    *,
    preview_stage: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "layout-report.json"
    run = subprocess.run(
        [
            "python3",
            str(DEFAULT_SLIDES_TEST_SCRIPT),
            str(preview_stage["deck_path"]),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    report = {
        "ok": run.returncode == 0,
        "returncode": run.returncode,
        "stdout": run.stdout,
        "stderr": run.stderr,
    }
    save_json(report_path, report)
    if run.returncode != 0:
        raise RuntimeError(
            "slides_test detected overflow or out-of-bounds content.\n"
            f"{(run.stdout or '').strip()}\n{(run.stderr or '').strip()}".strip()
        )
    return {
        "status": "ok",
        "report_path": str(report_path),
        "ok": True,
    }


def _load_json_if_exists(path_str: str | None) -> dict[str, Any] | None:
    if not path_str:
        return None
    path = Path(path_str)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def run_deck_truth_packet_gate(
    *,
    snapshot_date: str,
    gold_root: Path,
    workbook_dir: Path | None,
    bundle_dir: Path | None,
    decks_dir: Path | None,
    tieout_path: Path | None,
    output_root: Path,
    template_path: str,
    require: bool = False,
    analyst_workbook_path: Path | None = None,
    source_backed_publish_gate_path: Path | None = None,
    require_decks_tieout: bool = True,
) -> dict[str, Any]:
    resolved_workbook_dir = workbook_dir or DEFAULT_LIVE_WORKBOOK_ROOT / snapshot_date
    resolved_bundle_dir = bundle_dir or DEFAULT_BUNDLE_ROOT / snapshot_date
    resolved_decks_dir = decks_dir or DEFAULT_LAND_DECK_ROOT / snapshot_date / "land-only"
    resolved_tieout_path = tieout_path or DEFAULT_TIEOUT_ROOT / snapshot_date / "tie_out_audit.json"
    gold_manifest = gold_root / snapshot_date / "manifest.json"

    workbook_evidence_path = analyst_workbook_path or resolved_workbook_dir
    missing_inputs = [
        str(path)
        for path in [
            gold_manifest,
            workbook_evidence_path,
            resolved_bundle_dir,
        ]
        if not path.exists()
    ]
    if require_decks_tieout:
        missing_inputs.extend(
            str(path)
            for path in [resolved_decks_dir, resolved_tieout_path]
            if not path.exists()
        )
    if missing_inputs:
        status = "error" if require else "skipped"
        return {
            "status": status,
            "reason": "deck truth packet inputs are not complete",
            "missing_inputs": missing_inputs,
        }

    cmd = [
        sys.executable,
        "scripts/build_deck_truth_packet.py",
        "--snapshot-date",
        snapshot_date,
        "--gold-root",
        str(gold_root),
        "--workbook-dir",
        str(resolved_workbook_dir),
        "--bundle-dir",
        str(resolved_bundle_dir),
        "--template-path",
        template_path,
        "--output-root",
        str(output_root),
        "--json",
    ]
    if resolved_workbook_dir.exists():
        cmd.extend(["--workbook-dir", str(resolved_workbook_dir)])
    if analyst_workbook_path:
        cmd.extend(["--analyst-workbook", str(analyst_workbook_path)])
    if source_backed_publish_gate_path:
        cmd.extend(["--source-backed-publish-gate", str(source_backed_publish_gate_path)])
    if resolved_decks_dir.exists():
        cmd.extend(["--decks-dir", str(resolved_decks_dir)])
    if resolved_tieout_path.exists():
        cmd.extend(["--tieout-path", str(resolved_tieout_path)])
    run = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    parsed: dict[str, Any] | None = None
    try:
        parsed = json.loads(run.stdout or "{}")
    except json.JSONDecodeError:
        parsed = None
    stage = {
        "status": "ok" if run.returncode == 0 else "error",
        "returncode": run.returncode,
        "command": cmd,
        "stdout": run.stdout,
        "stderr": run.stderr,
    }
    if parsed:
        stage.update(parsed)
    return stage


def run_source_backed_deck_gate(
    *,
    truth_stage: dict[str, Any],
    deck_output_root: Path,
    visual_output_root: Path,
    source_bundle_manifest_path: Path | None = None,
    source_backed_publish_gate_path: Path | None = None,
    require: bool = False,
) -> dict[str, Any]:
    truth_packet_path = truth_stage.get("manifest_path")
    if truth_stage.get("status") != "ok" or not truth_packet_path:
        status = "error" if require else "skipped"
        return {
            "status": status,
            "reason": "deck truth packet is not available",
            "truth_status": truth_stage.get("status"),
        }
    truth_packet = Path(truth_packet_path)
    if not truth_packet.exists():
        status = "error" if require else "skipped"
        return {
            "status": status,
            "reason": "deck truth packet path missing",
            "truth_packet_path": str(truth_packet),
        }
    if (
        source_backed_publish_gate_path is None
        and source_bundle_manifest_path is None
        and not require
    ):
        return {
            "status": "skipped",
            "reason": "source-backed deck inputs were not requested",
            "truth_packet_path": str(truth_packet),
        }

    build = build_source_backed_deck(
        truth_packet_path=truth_packet,
        output_root=deck_output_root,
        source_bundle_manifest_path=source_bundle_manifest_path,
        source_backed_publish_gate_path=source_backed_publish_gate_path,
    )
    audit = validate_deck_visuals(
        deck_path=Path(build["deck_path"]),
        truth_packet_path=truth_packet,
        manifest_path=Path(build["manifest_path"]),
        output_root=visual_output_root,
    )
    status = "ok" if build.get("status") == "ok" and audit.get("status") == "ok" else "blocked"
    return {
        "status": status,
        "deck_build": build,
        "visual_audit": audit,
    }


def _quarterly_pipeline_disclosure_from_payload(
    payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not payload:
        return None
    quarterly_slide = next(
        (
            slide
            for slide in payload.get("slides", [])
            if slide.get("id") == "quarterly-pipeline"
        ),
        None,
    )
    if not quarterly_slide:
        return None
    slots = quarterly_slide.get("slots", {})
    display_reason = slots.get("quarterly_pipeline_display_reason") or "current_quarter"
    display_title = slots.get("quarterly_pipeline_title")
    display_footnote = slots.get("quarterly_pipeline_footnote")
    if not any((display_reason, display_title, display_footnote)):
        return None
    return {
        "display_reason": display_reason,
        "quarterly_pipeline_title": display_title,
        "quarterly_pipeline_footnote": display_footnote,
    }


def build_run_summary_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for target in manifest.get("targets", []):
        stages = target.get("stages", {})
        etl_audit = stages.get("etl_intelligence_audit", {})
        bridge = stages.get("validated_bridge", {})
        render = stages.get("deterministic_preview_render", {})
        audit = stages.get("deterministic_preview_audit", {})
        layout = stages.get("deterministic_preview_layout_audit", {})
        payload = _load_json_if_exists(bridge.get("powerpoint_fill_payload"))
        executive_slide = None
        approval_slide = None
        if payload:
            executive_slide = next(
                (
                    slide
                    for slide in payload.get("slides", [])
                    if slide.get("id") == "executive-summary"
                ),
                None,
            )
            approval_slide = next(
                (
                    slide
                    for slide in payload.get("slides", [])
                    if slide.get("id") == "commercial-approval-overview"
                ),
                None,
            )
        exec_slots = (executive_slide or {}).get("slots", {})
        approval_slots = (approval_slide or {}).get("slots", {})
        quarterly_disclosure = _quarterly_pipeline_disclosure_from_payload(payload)
        font_report = render.get("font_report", {}) if isinstance(render, dict) else {}
        rows.append(
            {
                "director_name": target.get("director_name"),
                "territory": target.get("territory"),
                "status": target.get("status"),
                "q2_active_arr": exec_slots.get("headline_pipeline_arr_q2"),
                "open_renewal_acv": exec_slots.get("headline_renewal_acv"),
                "approval_backlog": exec_slots.get("missing_approval_candidate_count")
                or approval_slots.get("missing_approval_candidate_count"),
                "etl_status": etl_audit.get("status"),
                "etl_high_gap_count": etl_audit.get("high_gap_count"),
                "etl_deal_risk_rows": etl_audit.get("deal_risk_rows"),
                "etl_summary_path": etl_audit.get("summary_path"),
                "audit_ok": audit.get("ok"),
                "audit_findings": audit.get("finding_count"),
                "layout_ok": layout.get("ok"),
                "font_missing_count": len(font_report.get("font_missing_overall", [])),
                "font_substituted_count": len(
                    font_report.get("font_substituted_overall", [])
                ),
                "deck_path": stages.get("deterministic_preview", {}).get("deck_path"),
                "montage_path": render.get("montage_path"),
                "audit_report_path": audit.get("report_path"),
                "layout_report_path": layout.get("report_path"),
                "quarterly_pipeline_display_reason": (
                    quarterly_disclosure or {}
                ).get("display_reason"),
                "quarterly_pipeline_title": (quarterly_disclosure or {}).get(
                    "quarterly_pipeline_title"
                ),
                "quarterly_pipeline_footnote": (quarterly_disclosure or {}).get(
                    "quarterly_pipeline_footnote"
                ),
            }
        )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_date": manifest.get("snapshot_date"),
        "run_dir": manifest.get("run_dir"),
        "status": manifest.get("status"),
        "target_count": len(rows),
        "targets": rows,
    }


def _audit_status_cell(row: dict[str, Any]) -> str:
    audit_ok = row.get("audit_ok")
    if audit_ok is True:
        return "ok"
    if audit_ok is False:
        finding_count = row.get("audit_findings")
        return f"{finding_count} findings" if finding_count is not None else "check"
    return "pending"


def _layout_status_cell(row: dict[str, Any]) -> str:
    layout_ok = row.get("layout_ok")
    if layout_ok is True:
        return "ok"
    if layout_ok is False:
        return "check"
    return "pending"


def write_run_summary(run_dir: Path, manifest: dict[str, Any]) -> None:
    summary = build_run_summary_payload(manifest)
    save_json(run_dir / "summary.json", summary)
    lines = [
        "# Sales Director Batch Summary",
        "",
        f"- Snapshot date: `{summary['snapshot_date']}`",
        f"- Run status: `{summary['status']}`",
        f"- Target count: `{summary['target_count']}`",
        "",
        "| Director | Territory | Status | Q2 Active ARR | Open Renewal ACV | Approval Backlog | ETL | Audit | Layout | Fonts | Montage |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in summary["targets"]:
        audit_cell = _audit_status_cell(row)
        layout_cell = _layout_status_cell(row)
        font_cell = f"m{row['font_missing_count']}/s{row['font_substituted_count']}"
        etl_cell = row.get("etl_status") or "pending"
        if row.get("etl_high_gap_count"):
            etl_cell = f"{etl_cell}/{row['etl_high_gap_count']} high"
        montage_path = row["montage_path"] or ""
        lines.append(
            f"| {row['director_name']} | {row['territory']} | {row['status']} | "
            f"{row['q2_active_arr'] or '—'} | {row['open_renewal_acv'] or '—'} | "
            f"{row['approval_backlog'] or '—'} | {etl_cell} | {audit_cell} | "
            f"{layout_cell} | {font_cell} | {montage_path} |"
        )
    (run_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_latest_status_packet(manifest: dict[str, Any]) -> dict[str, Any]:
    summary = build_run_summary_payload(manifest)
    target_status_counts: dict[str, int] = {}
    for row in summary["targets"]:
        status = row.get("status") or "unknown"
        target_status_counts[status] = target_status_counts.get(status, 0) + 1

    forward_quarter_fallbacks: list[dict[str, Any]] = []
    empty_quarters: list[dict[str, Any]] = []
    for row in summary["targets"]:
        disclosure = {
            "director_name": row.get("director_name"),
            "territory": row.get("territory"),
            "display_reason": row.get("quarterly_pipeline_display_reason"),
            "quarterly_pipeline_title": row.get("quarterly_pipeline_title"),
            "quarterly_pipeline_footnote": row.get("quarterly_pipeline_footnote"),
        }
        if disclosure["display_reason"] == "forward_quarter_fallback":
            forward_quarter_fallbacks.append(disclosure)
        elif disclosure["display_reason"] == "empty_current_and_forward_quarter":
            empty_quarters.append(disclosure)

    run_dir = Path(summary["run_dir"])
    return {
        **summary,
        "manifest_path": str(run_dir / "manifest.json"),
        "summary_json_path": str(run_dir / "summary.json"),
        "summary_markdown_path": str(run_dir / "summary.md"),
        "target_status_counts": target_status_counts,
        "quarterly_pipeline_disclosures": {
            "forward_quarter_fallbacks": forward_quarter_fallbacks,
            "empty_quarters": empty_quarters,
        },
    }


def build_latest_status_markdown(packet: dict[str, Any]) -> str:
    lines = [
        "# Sales Director Monthly Builder Status",
        "",
        f"- Snapshot date: `{packet['snapshot_date']}`",
        f"- Run status: `{packet['status']}`",
        f"- Target count: `{packet['target_count']}`",
        f"- Run dir: `{packet['run_dir']}`",
        "",
        "| Director | Territory | Status | Quarter Display | Active Pipeline ARR | Open Renewal ACV | Approval Backlog | ETL | Audit | Layout |",
        "|---|---|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in packet["targets"]:
        audit_cell = _audit_status_cell(row)
        layout_cell = _layout_status_cell(row)
        etl_cell = row.get("etl_status") or "pending"
        if row.get("etl_high_gap_count"):
            etl_cell = f"{etl_cell}/{row['etl_high_gap_count']} high"
        quarter_display = row.get("quarterly_pipeline_title") or "—"
        lines.append(
            f"| {row['director_name']} | {row['territory']} | {row['status']} | "
            f"{quarter_display} | {row['q2_active_arr'] or '—'} | "
            f"{row['open_renewal_acv'] or '—'} | {row['approval_backlog'] or '—'} | "
            f"{etl_cell} | {audit_cell} | {layout_cell} |"
        )

    lines.extend(
        [
            "",
            "## Quarter Disclosures",
            "",
        ]
    )
    forward_quarter_fallbacks = packet["quarterly_pipeline_disclosures"][
        "forward_quarter_fallbacks"
    ]
    if forward_quarter_fallbacks:
        for row in forward_quarter_fallbacks:
            lines.append(
                f"- Forward-quarter fallback: {row['director_name']} ({row['territory']}) "
                f"showing {row.get('quarterly_pipeline_title') or 'forward quarter'}. "
                f"{row.get('quarterly_pipeline_footnote') or 'No footnote provided.'}"
            )
    else:
        lines.append("- Forward-quarter fallbacks: none.")

    empty_quarters = packet["quarterly_pipeline_disclosures"]["empty_quarters"]
    if empty_quarters:
        for row in empty_quarters:
            lines.append(
                f"- Empty current and forward quarter: {row['director_name']} ({row['territory']}). "
                f"{row.get('quarterly_pipeline_footnote') or 'No explicit empty-state note provided.'}"
            )
    else:
        lines.append("- Empty current and forward-quarter directors: none.")

    lines.extend(
        [
            "",
            f"- Manifest: `{packet['manifest_path']}`",
            f"- Summary JSON: `{packet['summary_json_path']}`",
            f"- Summary markdown: `{packet['summary_markdown_path']}`",
            "",
        ]
    )
    return "\n".join(lines)


def write_latest_aliases(
    *,
    output_root: Path,
    snapshot_date: str,
    packet: dict[str, Any],
    markdown: str,
) -> None:
    snapshot_latest_json = output_root / snapshot_date / "latest.json"
    snapshot_latest_md = output_root / snapshot_date / "latest.md"
    root_latest_json = output_root / "latest.json"
    root_latest_md = output_root / "latest.md"

    save_json(snapshot_latest_json, packet)
    write_text(snapshot_latest_md, markdown + "\n")
    save_json(root_latest_json, packet)
    write_text(root_latest_md, markdown + "\n")


def run_powerpoint_lane(
    review_deck_path: Path,
    prompt_text: str,
    *,
    mode: str,
    skill_name: str | None,
    timeout: int,
    run_dir: Path,
) -> dict[str, Any]:
    editable_dir = run_dir / "editable_decks"
    editable_dir.mkdir(parents=True, exist_ok=True)
    run_slug = run_dir.parent.parent.name
    editable_deck_path = (
        editable_dir
        / f"{review_deck_path.stem} [{mode} {run_slug}]{review_deck_path.suffix}"
    )
    shutil.copy2(review_deck_path, editable_deck_path)
    effective_timeout = max(timeout, 900) if mode == "build" else timeout
    result = run_skill(
        TARGETS["powerpoint"],
        source_file=editable_deck_path,
        skill_name=skill_name,
        prompt=prompt_text,
        wait_finish_seconds=effective_timeout,
        run_dir=run_dir,
        edit_permission_mode="always-allow" if mode == "build" else "ask",
        save_document_on_finish=mode == "build",
    )
    return {
        "status": "ok",
        "mode": mode,
        "wait_finish_seconds": effective_timeout,
        "source_deck_path": str(review_deck_path),
        "editable_deck_path": str(editable_deck_path),
        **result,
    }


def director_record_base(target: DirectorTarget) -> dict[str, Any]:
    return {
        "director_name": target.director_name,
        "territory": target.territory,
        "workbook_path": str(target.workbook_path),
        "snapshot_path": str(target.snapshot_path),
        "existing_deck_path": str(target.existing_deck_path)
        if target.existing_deck_path
        else None,
        "status": "pending",
        "stages": {},
    }


def execute_target(
    target: DirectorTarget,
    *,
    args: argparse.Namespace,
    run_root: Path,
    record: dict[str, Any],
    progress_callback: Callable[[], None] | None = None,
) -> dict[str, Any]:
    director_dir = run_root / slugify(target.director_name)
    director_dir.mkdir(parents=True, exist_ok=True)
    record["run_dir"] = str(director_dir)
    record["status"] = "running"
    record["started_at"] = datetime.now(UTC).isoformat()

    def flush() -> None:
        record["updated_at"] = datetime.now(UTC).isoformat()
        if progress_callback is not None:
            progress_callback()

    if args.skip_etl_intelligence_audit:
        record["stages"]["etl_intelligence_audit"] = {
            "status": "skipped",
            "reason": "ETL intelligence audit disabled for this run.",
        }
        flush()
    else:
        try:
            record["stages"]["etl_intelligence_audit"] = {"status": "running"}
            flush()
            etl_stage = run_etl_intelligence_audit(
                target,
                snapshot_date=args.snapshot_date,
                bundle_root=Path(args.bundle_root),
                output_dir=director_dir / "etl_intelligence_audit",
            )
            record["stages"]["etl_intelligence_audit"] = etl_stage
            if args.fail_on_etl_high and etl_stage.get("high_gap_count", 0):
                raise RuntimeError(
                    "ETL intelligence audit found "
                    f"{etl_stage['high_gap_count']} high-severity gap(s)."
                )
        except Exception as exc:  # noqa: BLE001
            record["stages"]["etl_intelligence_audit"] = stage_error(exc)
            if args.fail_on_etl_high:
                raise
        flush()

    review_deck_path: Path | None = None
    if args.skip_powerpoint_review:
        record["stages"]["review_deck"] = {
            "status": "skipped",
            "reason": "Review deck disabled because PowerPoint Claude review was skipped.",
        }
        flush()
    else:
        try:
            record["stages"]["review_deck"] = {"status": "running"}
            flush()
            review_deck_path, deck_stage = prepare_review_deck(
                target,
                snapshot_date=args.snapshot_date,
                deck_source=args.deck_source,
                template_deck_path=Path(args.template_deck_path),
                shell_root=Path(args.shell_root),
                canonical_shell_root=Path(args.canonical_shell_root),
                allow_generated_shell_fallback=args.allow_generated_shell_fallback,
                fallback_workbook_deck=args.fallback_workbook_deck,
                workbook_deck_root=Path(args.workbook_deck_root),
                render_workbook_deck=args.render_workbook_deck,
            )
            record["stages"]["review_deck"] = deck_stage
        except Exception as exc:  # noqa: BLE001
            record["stages"]["review_deck"] = stage_error(exc)
        flush()

    excel_brief_text = ""
    if args.skip_excel_brief:
        record["stages"]["excel_brief"] = {
            "status": "skipped",
            "reason": "Excel Claude stage skipped for this run.",
        }
        flush()
    else:
        try:
            record["stages"]["excel_brief"] = {"status": "running"}
            flush()
            excel_stage = run_excel_brief(
                target,
                prompt_template=args.excel_brief_prompt,
                timeout=args.excel_timeout,
                run_dir=director_dir / "excel_brief",
            )
            record["stages"]["excel_brief"] = excel_stage
            message_path = excel_stage.get("message_path")
            if message_path:
                excel_brief_text = Path(message_path).read_text(encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            record["stages"]["excel_brief"] = stage_error(exc)
        flush()

    record["stages"]["validated_bridge"] = {"status": "running"}
    flush()
    external_inputs = load_external_inputs(args.snapshot_date)
    bridge = build_validated_bridge(
        target.snapshot_path,
        excel_brief_text,
        director_dir / "validated_bridge",
        external_inputs,
    )
    record["stages"]["validated_bridge"] = bridge
    flush()

    try:
        record["stages"]["deterministic_preview"] = {"status": "running"}
        flush()
        record["stages"]["deterministic_preview"] = build_deterministic_preview(
            target,
            snapshot_date=args.snapshot_date,
            bridge=bridge,
            output_dir=director_dir / "deterministic_preview",
            template_deck_path=Path(args.template_deck_path),
        )
    except Exception as exc:  # noqa: BLE001
        record["stages"]["deterministic_preview"] = stage_error(exc)
    flush()

    if record["stages"]["deterministic_preview"].get("status") == "ok":
        try:
            record["stages"]["deterministic_preview_render"] = {"status": "running"}
            flush()
            record["stages"]["deterministic_preview_render"] = (
                render_deterministic_preview(
                    preview_stage=record["stages"]["deterministic_preview"],
                    output_dir=director_dir / "deterministic_preview_render",
                )
            )
        except Exception as exc:  # noqa: BLE001
            record["stages"]["deterministic_preview_render"] = stage_error(exc)
        flush()

    if record["stages"]["deterministic_preview"].get("status") == "ok":
        try:
            record["stages"]["deterministic_preview_audit"] = {"status": "running"}
            flush()
            record["stages"]["deterministic_preview_audit"] = (
                build_deterministic_preview_audit(
                    preview_stage=record["stages"]["deterministic_preview"],
                    bridge=bridge,
                    output_dir=director_dir / "deterministic_preview_audit",
                )
            )
        except Exception as exc:  # noqa: BLE001
            record["stages"]["deterministic_preview_audit"] = stage_error(exc)
        flush()

    if record["stages"]["deterministic_preview"].get("status") == "ok":
        try:
            record["stages"]["deterministic_preview_layout_audit"] = {
                "status": "running"
            }
            flush()
            record["stages"]["deterministic_preview_layout_audit"] = (
                build_deterministic_preview_layout_audit(
                    preview_stage=record["stages"]["deterministic_preview"],
                    output_dir=director_dir / "deterministic_preview_layout_audit",
                )
            )
        except Exception as exc:  # noqa: BLE001
            record["stages"]["deterministic_preview_layout_audit"] = stage_error(exc)
        flush()

    if args.skip_powerpoint_review:
        record["stages"]["powerpoint_review"] = {
            "status": "skipped",
            "reason": "PowerPoint Claude review skipped for this run.",
        }
        flush()
    elif review_deck_path is None:
        record["stages"]["powerpoint_review"] = {
            "status": "skipped",
            "reason": "No review deck available for this director.",
        }
        flush()
    else:
        try:
            record["stages"]["powerpoint_review"] = {
                "status": "running",
                "mode": args.powerpoint_mode,
            }
            flush()
            prompt_path = Path(
                bridge["powerpoint_build_prompt"]
                if args.powerpoint_mode == "build"
                else bridge["powerpoint_prompt"]
            )
            prompt_text = prompt_path.read_text(encoding="utf-8")
            record["stages"]["powerpoint_review"] = run_powerpoint_lane(
                review_deck_path,
                prompt_text,
                mode=args.powerpoint_mode,
                skill_name=None
                if args.powerpoint_mode == "build"
                else TARGETS["powerpoint"].default_skill,
                timeout=args.powerpoint_timeout,
                run_dir=director_dir / "powerpoint_review",
            )
        except Exception as exc:  # noqa: BLE001
            record["stages"]["powerpoint_review"] = stage_error(exc)
        flush()

    error_count = sum(
        1
        for stage in record["stages"].values()
        if isinstance(stage, dict) and stage.get("status") == "error"
    )
    record["status"] = "partial" if error_count else "ok"
    record["finished_at"] = datetime.now(UTC).isoformat()
    flush()
    return record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date")
    parser.add_argument(
        "--as-of-date",
        help="Resolve the reporting month from this date when --snapshot-date is omitted.",
    )
    parser.add_argument("--director", help="Single director name fragment.")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Explicitly run all workbook files for the snapshot date.",
    )
    parser.add_argument("--workbook-root", type=Path, default=DEFAULT_WORKBOOK_ROOT)
    parser.add_argument("--snapshot-root", type=Path, default=DEFAULT_SNAPSHOT_ROOT)
    parser.add_argument("--bundle-root", type=Path, default=DEFAULT_BUNDLE_ROOT)
    parser.add_argument("--deck-root", type=Path, default=DEFAULT_REVIEW_DECK_ROOT)
    parser.add_argument(
        "--deck-date",
        help="Optional existing deck date directory for PowerPoint review.",
    )
    parser.add_argument(
        "--deck-source",
        choices=(
            "canonical-shell",
            "shell",
            "template",
            "existing",
            "workbook-native",
            "skip",
        ),
        default="canonical-shell",
        help="Deck source for PowerPoint review/build. 'canonical-shell' is the production path; 'shell' is a generated native scaffold fallback only.",
    )
    parser.add_argument(
        "--template-deck-path",
        type=Path,
        default=DEFAULT_TEMPLATE_DECK_PATH,
        help="SimCorp PowerPoint template to use when --deck-source template.",
    )
    parser.add_argument(
        "--shell-root",
        type=Path,
        default=DEFAULT_SHELL_ROOT,
        help="Output root for generated monthly shell decks when --deck-source shell.",
    )
    parser.add_argument(
        "--canonical-shell-root",
        type=Path,
        default=DEFAULT_CANONICAL_SHELL_ROOT,
        help="Canonical shell store for director monthly decks.",
    )
    parser.add_argument(
        "--allow-generated-shell-fallback",
        action="store_true",
        help="Allow generated native shell scaffolding if the canonical shell is missing. This path is not publish-safe.",
    )
    parser.add_argument(
        "--fallback-workbook-deck",
        action="store_true",
        help="If no existing deck is found, build and review the workbook-native fallback deck.",
    )
    parser.add_argument(
        "--workbook-deck-root",
        type=Path,
        default=DEFAULT_WORKBOOK_DECK_ROOT,
        help="Output root for workbook-native fallback decks.",
    )
    parser.add_argument(
        "--render-workbook-deck",
        action="store_true",
        help="When building the workbook-native fallback deck, also render PDF and montage artifacts.",
    )
    parser.add_argument(
        "--refresh-snapshots",
        action="store_true",
        help="Re-extract JSON snapshots from the Excel workbooks even if a JSON file already exists.",
    )
    parser.add_argument("--skip-excel-brief", action="store_true")
    parser.add_argument("--skip-powerpoint-review", action="store_true")
    parser.add_argument("--skip-etl-intelligence-audit", action="store_true")
    parser.add_argument(
        "--skip-deck-truth-packet",
        action="store_true",
        help="Skip the post-gate deck truth packet compiler.",
    )
    parser.add_argument(
        "--require-deck-truth-packet",
        action="store_true",
        help="Mark the run partial if deck truth packet inputs are missing or compilation fails.",
    )
    parser.add_argument("--deck-truth-gold-root", type=Path, default=DEFAULT_GOLD_ROOT)
    parser.add_argument("--deck-truth-workbook-dir", type=Path)
    parser.add_argument("--deck-truth-analyst-workbook", type=Path)
    parser.add_argument("--deck-truth-source-backed-publish-gate", type=Path)
    parser.add_argument("--deck-truth-bundle-dir", type=Path)
    parser.add_argument("--deck-truth-decks-dir", type=Path)
    parser.add_argument("--deck-truth-tieout-path", type=Path)
    parser.add_argument(
        "--deck-truth-optional-decks-tieout",
        action="store_true",
        help=(
            "Allow deck truth packet compilation before deck sidecars and tie-out "
            "exist; used by the source-backed artifact lane."
        ),
    )
    parser.add_argument(
        "--deck-truth-output-root",
        type=Path,
        default=DEFAULT_DECK_TRUTH_PACKET_ROOT,
    )
    parser.add_argument(
        "--deck-truth-template-path",
        default="sales_director_thinkcell_template.pptx",
    )
    parser.add_argument(
        "--skip-source-backed-deck",
        action="store_true",
        help="Skip source-backed PPTX generation and visual/package audit after deck truth.",
    )
    parser.add_argument(
        "--require-source-backed-deck",
        action="store_true",
        help="Mark the run partial if source-backed PPTX generation or visual/package audit fails.",
    )
    parser.add_argument(
        "--source-backed-deck-output-root",
        type=Path,
        default=DEFAULT_SOURCE_BACKED_DECK_ROOT,
    )
    parser.add_argument(
        "--source-backed-deck-visual-output-root",
        type=Path,
        default=DEFAULT_SOURCE_BACKED_DECK_VISUAL_ROOT,
    )
    parser.add_argument("--source-backed-deck-source-bundle-manifest", type=Path)
    parser.add_argument(
        "--fail-on-etl-high",
        action="store_true",
        help="Fail a director run when the ETL intelligence audit finds high-severity gaps.",
    )
    parser.add_argument(
        "--powerpoint-mode",
        choices=("audit", "build"),
        default="audit",
        help="Use audit mode for critique or build mode to rewrite the current deck in place.",
    )
    parser.add_argument("--excel-timeout", type=int, default=300)
    parser.add_argument("--powerpoint-timeout", type=int, default=300)
    parser.add_argument("--excel-brief-prompt", default=DEFAULT_MONTHLY_BRIEF_PROMPT)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Resolve the monthly run manifest without launching Office automation.",
    )
    parser.add_argument(
        "--fail-fast", action="store_true", help="Stop on the first director error."
    )
    parser.add_argument(
        "--json", action="store_true", help="Print the run manifest as JSON."
    )
    args = parser.parse_args()
    if args.all and args.director:
        parser.error("Use either --director or --all, not both.")
    return args


def resolve_runtime_period(args: argparse.Namespace) -> PeriodContext:
    return resolve_period_context(
        as_of_date=getattr(args, "as_of_date", None),
        snapshot_date=getattr(args, "snapshot_date", None),
        deck_date=getattr(args, "deck_date", None),
    )


def main() -> int:
    args = parse_args()
    period = resolve_runtime_period(args)
    args.snapshot_date = period.snapshot_date

    workbook_root = Path(args.workbook_root)
    try:
        targets = build_targets(
            snapshot_date=args.snapshot_date,
            director=args.director,
            workbook_root=workbook_root,
            snapshot_root=Path(args.snapshot_root),
            deck_root=Path(args.deck_root),
            deck_date=args.deck_date,
            refresh_snapshots=args.refresh_snapshots,
        )
    except FileNotFoundError as exc:
        print(
            json.dumps(
                build_preflight_failure_payload(
                    snapshot_date=args.snapshot_date,
                    director=args.director,
                    deck_source=args.deck_source,
                    fallback_workbook_deck=args.fallback_workbook_deck,
                    workbook_root=workbook_root,
                    period=period,
                    exc=exc,
                ),
                indent=2,
            )
        )
        return 2

    run_dir = Path(args.run_root) / args.snapshot_date / timestamp_slug()
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "running",
        "snapshot_date": args.snapshot_date,
        "period_context": period.as_dict(),
        "director_filter": args.director,
        "target_count": len(targets),
        "deck_source": args.deck_source,
        "fallback_workbook_deck": args.fallback_workbook_deck,
        "run_dir": str(run_dir),
        "stages": {},
        "targets": [],
    }
    save_manifest(run_dir, manifest, status="running")

    for target in targets:
        record = director_record_base(target)
        manifest["targets"].append(record)
        if args.plan_only:
            record["status"] = "planned"
            planned_review_deck, planned_stage = plan_review_deck(
                target,
                snapshot_date=args.snapshot_date,
                deck_source=args.deck_source,
                template_deck_path=Path(args.template_deck_path),
                shell_root=Path(args.shell_root),
                canonical_shell_root=Path(args.canonical_shell_root),
                allow_generated_shell_fallback=args.allow_generated_shell_fallback,
                fallback_workbook_deck=args.fallback_workbook_deck,
                workbook_deck_root=Path(args.workbook_deck_root),
            )
            record["planned_review_deck"] = (
                str(planned_review_deck) if planned_review_deck else None
            )
            record["planned_review_deck_stage"] = planned_stage
            save_manifest(run_dir, manifest, status="planned")
            continue
        try:
            execute_target(
                target,
                args=args,
                run_root=run_dir,
                record=record,
                progress_callback=lambda: save_manifest(
                    run_dir, manifest, status="running"
                ),
            )
        except Exception as exc:  # noqa: BLE001
            record["status"] = "error"
            record["error"] = {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }
            record["finished_at"] = datetime.now(UTC).isoformat()
            save_manifest(run_dir, manifest, status="running")
            if args.fail_fast:
                break

    if args.plan_only:
        final_status = "planned"
    else:
        target_statuses = [record.get("status") for record in manifest["targets"]]
        if any(status == "error" for status in target_statuses):
            final_status = "partial"
        elif any(status == "partial" for status in target_statuses):
            final_status = "partial"
        else:
            final_status = "ok"

    if args.plan_only:
        manifest["stages"]["deck_truth_packet"] = {
            "status": "skipped",
            "reason": "plan-only run",
        }
        manifest["stages"]["source_backed_deck"] = {
            "status": "skipped",
            "reason": "plan-only run",
        }
    elif args.skip_deck_truth_packet:
        manifest["stages"]["deck_truth_packet"] = {
            "status": "skipped",
            "reason": "deck truth packet disabled",
        }
        manifest["stages"]["source_backed_deck"] = {
            "status": "skipped",
            "reason": "deck truth packet disabled",
        }
    else:
        manifest["stages"]["deck_truth_packet"] = {"status": "running"}
        save_manifest(run_dir, manifest, status=final_status)
        truth_stage = run_deck_truth_packet_gate(
            snapshot_date=args.snapshot_date,
            gold_root=Path(args.deck_truth_gold_root),
            workbook_dir=args.deck_truth_workbook_dir,
            bundle_dir=args.deck_truth_bundle_dir,
            decks_dir=args.deck_truth_decks_dir,
            tieout_path=args.deck_truth_tieout_path,
            output_root=Path(args.deck_truth_output_root),
            template_path=args.deck_truth_template_path,
            require=args.require_deck_truth_packet,
            analyst_workbook_path=args.deck_truth_analyst_workbook,
            source_backed_publish_gate_path=args.deck_truth_source_backed_publish_gate,
            require_decks_tieout=not args.deck_truth_optional_decks_tieout,
        )
        manifest["stages"]["deck_truth_packet"] = truth_stage
        if args.require_deck_truth_packet and truth_stage.get("status") != "ok":
            final_status = "partial"
        if args.skip_source_backed_deck:
            manifest["stages"]["source_backed_deck"] = {
                "status": "skipped",
                "reason": "source-backed deck disabled",
            }
        else:
            manifest["stages"]["source_backed_deck"] = {"status": "running"}
            save_manifest(run_dir, manifest, status=final_status)
            source_backed_deck_stage = run_source_backed_deck_gate(
                truth_stage=truth_stage,
                deck_output_root=Path(args.source_backed_deck_output_root),
                visual_output_root=Path(args.source_backed_deck_visual_output_root),
                source_bundle_manifest_path=args.source_backed_deck_source_bundle_manifest,
                source_backed_publish_gate_path=args.deck_truth_source_backed_publish_gate,
                require=args.require_source_backed_deck,
            )
            manifest["stages"]["source_backed_deck"] = source_backed_deck_stage
            if (
                args.require_source_backed_deck
                and source_backed_deck_stage.get("status") != "ok"
            ):
                final_status = "partial"
    save_manifest(run_dir, manifest, status=final_status)
    write_run_summary(run_dir, manifest)
    latest_status = build_latest_status_packet(manifest)
    write_latest_aliases(
        output_root=Path(args.run_root),
        snapshot_date=args.snapshot_date,
        packet=latest_status,
        markdown=build_latest_status_markdown(latest_status),
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
